//! Known entities, and the scan that finds them in text.
//!
//! What `--entities` names: subjects with labels. A label found in a chunk
//! is a mention of that subject, under the subject's own IRI, which is the
//! whole point — an occurrence written here and a fact in the ledger the
//! entity came from meet on one node, so a query over both federates with
//! no mapping step.
//!
//! The scan is deterministic: longest whole-word match over every label,
//! case-folded, and a second pass over Snowball stems so an inflected form
//! ("cities", "Acme's") still finds its entry. Nothing here needs a model.

use aho_corasick::{AhoCorasick, AhoCorasickKind, MatchKind};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Predicates whose objects are names for the subject, in the order a
/// display name is picked from them.
pub const LABEL_PREDICATES: &[&str] = &[
    "skos:prefLabel",
    "schema:name",
    "rdfs:label",
    "skos:altLabel",
    "schema:alternateName",
    "skos:hiddenLabel",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub iri: String,
    /// The preferred label, by [`LABEL_PREDICATES`] order.
    pub name: String,
    pub types: Vec<String>,
    /// Every surface form, the name included.
    pub labels: Vec<String>,
}

/// One label found in a text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mention {
    pub entry: usize,
    /// Character offsets into the scanned text.
    pub begin: usize,
    pub end: usize,
    /// The text as it appears.
    pub surface: String,
}

#[derive(Debug, Default)]
pub struct GazetteerBuilder {
    entries: Vec<Entry>,
    at: HashMap<String, usize>,
    /// Which predicate supplied each entry's name, so a better one wins.
    name_rank: HashMap<String, usize>,
}

impl GazetteerBuilder {
    /// Rows are `[iri, label, type | null]`; `predicate` is the label
    /// predicate the query asked for.
    pub fn add_rows(&mut self, rows: &[Value], predicate: &str) {
        let rank = LABEL_PREDICATES
            .iter()
            .position(|p| *p == predicate)
            .unwrap_or(LABEL_PREDICATES.len());
        for row in rows {
            let (Some(iri), Some(label)) = (cell(row, 0), cell(row, 1)) else {
                continue;
            };
            let label = label.trim();
            if iri.starts_with("_:") || label.is_empty() {
                continue;
            }
            let idx = *self.at.entry(iri.to_string()).or_insert_with(|| {
                self.entries.push(Entry {
                    iri: iri.to_string(),
                    name: label.to_string(),
                    types: Vec::new(),
                    labels: Vec::new(),
                });
                self.name_rank.insert(iri.to_string(), rank);
                self.entries.len() - 1
            });
            let e = &mut self.entries[idx];
            if rank < self.name_rank[iri] {
                e.name = label.to_string();
                self.name_rank.insert(iri.to_string(), rank);
            }
            if !e.labels.iter().any(|l| l == label) {
                e.labels.push(label.to_string());
            }
            if let Some(t) = cell(row, 2) {
                if !e.types.iter().any(|x| x == t) {
                    e.types.push(t.to_string());
                }
            }
        }
    }

    /// Fingerprint of the entries added so far; see [`Gazetteer::fingerprint`].
    pub fn fingerprint(&self) -> String {
        fingerprint_entries(&self.entries)
    }

    pub fn build(self, lang: &str) -> Gazetteer {
        Gazetteer::new(self.entries, lang)
    }
}

pub struct Gazetteer {
    entries: Vec<Entry>,
    surface: Scanner,
    /// Over stemmed labels, run against stemmed text. Absent when the
    /// language has no stemmer.
    stemmed: Option<Scanner>,
    stemmer: Option<rust_stemmers::Stemmer>,
    by_name: HashMap<String, usize>,
}

impl std::fmt::Debug for Gazetteer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gazetteer")
            .field("entries", &self.entries.len())
            .field("stemmed", &self.stemmed.is_some())
            .finish()
    }
}

impl Gazetteer {
    pub fn new(entries: Vec<Entry>, lang: &str) -> Self {
        // Pattern i → entry; a label of one character is noise.
        let mut patterns = Vec::new();
        let mut owners = Vec::new();
        for (i, e) in entries.iter().enumerate() {
            for label in &e.labels {
                if label.chars().count() < 2 {
                    continue;
                }
                patterns.push(label.to_lowercase());
                owners.push(i);
            }
        }
        let surface = Scanner::new(patterns.clone(), owners.clone());

        let stemmer = algorithm_for(lang).map(rust_stemmers::Stemmer::create);
        let stemmed = stemmer.as_ref().map(|st| {
            let stems: Vec<String> = patterns.iter().map(|p| stem_projection(st, p).0).collect();
            Scanner::new(stems, owners)
        });
        let by_name = entries
            .iter()
            .enumerate()
            .flat_map(|(i, e)| e.labels.iter().map(move |l| (normalize_name(l), i)))
            .collect();
        Self {
            entries,
            surface,
            stemmed,
            stemmer,
            by_name,
        }
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entry a name refers to, by any of its labels, case-insensitive.
    pub fn lookup(&self, name: &str) -> Option<&Entry> {
        self.by_name
            .get(&normalize_name(name))
            .map(|i| &self.entries[*i])
    }

    /// Changes when any entry, label or type changes.
    pub fn fingerprint(&self) -> String {
        fingerprint_entries(&self.entries)
    }

    /// Every label found in `text`, longest match winning where labels
    /// overlap, in text order.
    pub fn scan(&self, text: &str) -> Vec<Mention> {
        let lowered = fold_case(text);
        let mut raw = self.surface.scan(&lowered);
        if let (Some(scanner), Some(st)) = (&self.stemmed, &self.stemmer) {
            let (projected, spans) = stem_projection(st, &lowered);
            raw.extend(
                scanner
                    .scan(&projected)
                    .into_iter()
                    .map(|(b, e, owner)| (spans[b].0, spans[e - 1].1, owner)),
            );
        }
        raw.sort_by(|a, b| a.0.cmp(&b.0).then((b.1 - b.0).cmp(&(a.1 - a.0))));
        // Drop anything overlapping a longer match already kept, and exact
        // duplicates from the two passes.
        let mut kept: Vec<(usize, usize, usize)> = Vec::new();
        for m in raw {
            if kept.iter().any(|k| m.0 < k.1 && k.0 < m.1) {
                continue;
            }
            kept.push(m);
        }
        let chars: Vec<char> = text.chars().collect();
        kept.into_iter()
            .map(|(begin, end, entry)| Mention {
                entry,
                begin,
                end,
                surface: chars[begin..end].iter().collect(),
            })
            .collect()
    }

    /// The EXISTING ENTITIES block of the user prompt: the entries mentioned
    /// in this chunk, named exactly as the language model must name them.
    pub fn existing_block(&self, mentions: &[Mention]) -> String {
        let mut seen = Vec::new();
        let mut out = String::new();
        for m in mentions {
            if seen.contains(&m.entry) {
                continue;
            }
            seen.push(m.entry);
            let e = &self.entries[m.entry];
            out.push_str("- \"");
            out.push_str(&e.name);
            out.push('"');
            if !e.types.is_empty() {
                out.push_str(" @type=");
                out.push_str(&e.types.join("|"));
            }
            let alts: Vec<&str> = e
                .labels
                .iter()
                .filter(|l| **l != e.name)
                .map(String::as_str)
                .collect();
            if !alts.is_empty() {
                out.push_str(" — also known as: ");
                out.push_str(&alts.join(", "));
            }
            out.push('\n');
        }
        if out.is_empty() {
            "(none)\n".to_string()
        } else {
            out
        }
    }

    /// Queries whose rows [`GazetteerBuilder::add_rows`] takes, one per
    /// label predicate, each paired with that predicate. `class` restricts
    /// the subjects to one type.
    pub fn queries(class: Option<&str>) -> Vec<(Value, &'static str)> {
        LABEL_PREDICATES
            .iter()
            .map(|pred| {
                let mut anchor = json!({ "@id": "?s", *pred: "?label" });
                if let Some(c) = class {
                    anchor["@type"] = json!(c);
                }
                (
                    json!({
                        "@context": crate::model::query_context(),
                        "where": [
                            anchor,
                            ["optional", { "@id": "?s", "@type": "?type" }]
                        ],
                        "select": ["?s", "?label", "?type"]
                    }),
                    *pred,
                )
            })
            .collect()
    }
}

fn fingerprint_entries(entries: &[Entry]) -> String {
    let mut h = Sha256::new();
    for e in entries {
        h.update(e.iri.as_bytes());
        h.update([0]);
        for t in &e.types {
            h.update(t.as_bytes());
            h.update([1]);
        }
        for l in &e.labels {
            h.update(l.as_bytes());
            h.update([2]);
        }
        h.update([3]);
    }
    hex::encode(h.finalize())
}

pub fn normalize_name(name: &str) -> String {
    name.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// Character-preserving lowercase: a character whose lowercase form is
/// longer (`İ`) keeps its place, so offsets in the folded text are offsets
/// in the original.
fn fold_case(text: &str) -> String {
    text.chars()
        .map(|c| c.to_lowercase().next().unwrap_or(c))
        .collect()
}

/// Longest whole-word match over a set of patterns, each owned by an entry.
struct Scanner {
    automaton: AhoCorasick,
    owners: Vec<usize>,
}

impl Scanner {
    fn new(patterns: Vec<String>, owners: Vec<usize>) -> Self {
        let automaton = AhoCorasick::builder()
            .kind(Some(AhoCorasickKind::DFA))
            .match_kind(MatchKind::Standard)
            .build(&patterns)
            .expect("patterns compile");
        Self { automaton, owners }
    }

    /// `(begin, end, entry)` in character offsets, whole words only.
    fn scan(&self, text: &str) -> Vec<(usize, usize, usize)> {
        if self.owners.is_empty() {
            return Vec::new();
        }
        let bytes = text.as_bytes();
        // Byte → char offset table, built once per text.
        let mut char_at = vec![0usize; bytes.len() + 1];
        for (ci, (bi, _)) in text.char_indices().enumerate() {
            char_at[bi] = ci;
        }
        char_at[bytes.len()] = text.chars().count();
        self.automaton
            .find_overlapping_iter(text)
            .filter(|m| is_whole_word(text, m.start(), m.end()))
            .map(|m| {
                (
                    char_at[m.start()],
                    char_at[m.end()],
                    self.owners[m.pattern().as_usize()],
                )
            })
            .collect()
    }
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_whole_word(text: &str, start: usize, end: usize) -> bool {
    let before = text[..start].chars().next_back();
    let after = text[end..].chars().next();
    !before.is_some_and(is_word_char) && !after.is_some_and(is_word_char)
}

fn algorithm_for(lang: &str) -> Option<rust_stemmers::Algorithm> {
    use rust_stemmers::Algorithm as A;
    let lang = lang.split(['-', '_']).next().unwrap_or(lang).to_lowercase();
    Some(match lang.as_str() {
        "ar" => A::Arabic,
        "da" => A::Danish,
        "nl" => A::Dutch,
        "en" => A::English,
        "fi" => A::Finnish,
        "fr" => A::French,
        "de" => A::German,
        "el" => A::Greek,
        "hu" => A::Hungarian,
        "it" => A::Italian,
        "no" | "nb" | "nn" => A::Norwegian,
        "pt" => A::Portuguese,
        "ro" => A::Romanian,
        "ru" => A::Russian,
        "es" => A::Spanish,
        "sv" => A::Swedish,
        "ta" => A::Tamil,
        "tr" => A::Turkish,
        _ => return None,
    })
}

/// `text` with every word replaced by its stem and every run of
/// non-word characters by one space, plus, per character of the result,
/// the span of the original it stands for. A match in the projection maps
/// back to whole words of the original, so "capital cities" is found by
/// the label "capital city" and reported at its real offsets.
fn stem_projection(st: &rust_stemmers::Stemmer, text: &str) -> (String, Vec<(usize, usize)>) {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut spans: Vec<(usize, usize)> = Vec::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        let start = i;
        if !is_word_char(chars[i]) {
            while i < chars.len() && !is_word_char(chars[i]) {
                i += 1;
            }
            out.push(' ');
            spans.push((start, i));
            continue;
        }
        while i < chars.len() && is_word_char(chars[i]) {
            i += 1;
        }
        let word: String = chars[start..i].iter().collect();
        let stem: Vec<char> = st.stem(&word).chars().collect();
        let last = stem.len().saturating_sub(1);
        for (k, c) in stem.iter().enumerate() {
            out.push(*c);
            let begin = start + k.min(i - start - 1);
            let end = if k == last { i } else { (begin + 1).min(i) };
            spans.push((begin, end));
        }
    }
    (out, spans)
}

fn cell(row: &Value, i: usize) -> Option<&str> {
    match row.get(i)? {
        Value::String(s) => Some(s.as_str()).filter(|s| !s.is_empty()),
        Value::Object(o) => o
            .get("@id")
            .or_else(|| o.get("@value"))
            .and_then(Value::as_str),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gaz() -> Gazetteer {
        let mut b = GazetteerBuilder::default();
        b.add_rows(
            &[
                json!(["ex:nyc", "New York City", "schema:City"]),
                json!(["ex:ny", "New York", "schema:State"]),
                json!(["ex:acme", "Acme Corporation", "schema:Organization"]),
            ],
            "skos:prefLabel",
        );
        b.add_rows(
            &[
                json!(["ex:acme", "Acme", null]),
                json!(["ex:acme", "ACME Corp", null]),
            ],
            "skos:altLabel",
        );
        // A later, lower-ranked predicate must not replace the name.
        b.add_rows(&[json!(["ex:acme", "Acme Corp.", null])], "rdfs:label");
        b.build("en")
    }

    #[test]
    fn labels_merge_under_the_preferred_name() {
        let g = gaz();
        let acme = g.lookup("acme corp").unwrap();
        assert_eq!(acme.name, "Acme Corporation");
        assert_eq!(acme.labels.len(), 4);
        assert_eq!(acme.types, vec!["schema:Organization"]);
        assert_eq!(g.lookup("nowhere"), None);
    }

    #[test]
    fn longest_whole_word_match_wins() {
        let g = gaz();
        let m = g.scan("We flew to New York City, then Acme's office.");
        let found: Vec<(&str, &str)> = m
            .iter()
            .map(|m| (g.entries()[m.entry].iri.as_str(), m.surface.as_str()))
            .collect();
        assert_eq!(
            found,
            vec![("ex:nyc", "New York City"), ("ex:acme", "Acme")]
        );
        assert_eq!((m[0].begin, m[0].end), (11, 24));
    }

    #[test]
    fn partial_words_do_not_match() {
        let g = gaz();
        assert!(g.scan("Acmeville is not Acme").len() == 1);
    }

    #[test]
    fn stems_find_inflected_forms() {
        let mut b = GazetteerBuilder::default();
        b.add_rows(
            &[json!(["ex:city", "capital city", null])],
            "skos:prefLabel",
        );
        let g = b.build("en");
        let m = g.scan("Two capital cities were named.");
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].surface, "capital cities");
        assert!(b_none("xx", "capital cities"));
    }

    fn b_none(lang: &str, text: &str) -> bool {
        let mut b = GazetteerBuilder::default();
        b.add_rows(
            &[json!(["ex:city", "capital city", null])],
            "skos:prefLabel",
        );
        b.build(lang).scan(text).is_empty()
    }

    #[test]
    fn offsets_are_characters() {
        let g = gaz();
        let text = "café — New York";
        let m = g.scan(text);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].begin, 7);
        assert_eq!(text.chars().skip(7).collect::<String>(), "New York");
    }

    #[test]
    fn existing_block_names_each_entry_once() {
        let g = gaz();
        let m = g.scan("Acme and Acme again, in New York City.");
        let block = g.existing_block(&m);
        assert_eq!(block.matches("Acme Corporation").count(), 1);
        assert!(block.contains("@type=schema:Organization"));
        assert!(block.contains("also known as: Acme, ACME Corp, Acme Corp."));
        assert_eq!(g.existing_block(&[]), "(none)\n");
    }

    #[test]
    fn queries_cover_every_label_predicate_and_scope_by_class() {
        let q = Gazetteer::queries(Some("schema:City"));
        assert_eq!(q.len(), LABEL_PREDICATES.len());
        assert_eq!(q[0].1, "skos:prefLabel");
        assert_eq!(q[0].0["where"][0]["@type"], "schema:City");
        assert!(Gazetteer::queries(None)[0].0["where"][0]
            .get("@type")
            .is_none());
    }
}
