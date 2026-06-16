//! Query methods over [`DocsIndex`]: `search`, `get`, `examples`. Results are
//! stamped with [`VERSION`].

use crate::index::{tokenize, DocsIndex};
use crate::model::{DocsTree, Example, Page, SearchHit, VERSION};
use crate::parse::parse_summary;
use std::collections::HashSet;

impl DocsIndex {
    /// Ranked, section-level search. Returns up to `limit` hits, best first.
    pub fn search(&self, query: &str, limit: usize) -> Vec<SearchHit> {
        let terms: HashSet<String> = tokenize(query).into_iter().collect();
        self.ranked(&terms)
            .into_iter()
            .take(limit)
            .map(|(idx, score)| {
                let s = &self.sections[idx];
                SearchHit {
                    path: s.path.clone(),
                    anchor: s.anchor.clone(),
                    title: s.title.clone(),
                    heading_path: s.heading_path.clone(),
                    snippet: snippet(&s.body, query),
                    score,
                    version: VERSION,
                }
            })
            .collect()
    }

    /// A whole page (`anchor = None`) or a single heading-scoped slice
    /// (`anchor = Some`), as markdown. `None` if the path/anchor isn't found.
    pub fn get(&self, path: &str, anchor: Option<&str>) -> Option<Page> {
        let norm = normalize_path(path);
        match anchor {
            None => {
                let content = self.pages.get(&norm)?.clone();
                Some(Page {
                    title: self.page_title(&norm),
                    path: norm,
                    anchor: None,
                    content,
                    version: VERSION,
                })
            }
            Some(a) => {
                // Return the heading's whole subtree, not just its intro prose:
                // the target section plus every following section nested under
                // it (level deeper than the target), until the next sibling or
                // ancestor heading. Section bodies are contiguous slices of the
                // page, so concatenating them reconstructs the original markdown.
                let start = self
                    .sections
                    .iter()
                    .position(|s| s.path == norm && s.anchor == a)?;
                let level = self.sections[start].level;
                let mut content = self.sections[start].body.clone();
                for s in &self.sections[start + 1..] {
                    if s.path != norm || s.level <= level {
                        break;
                    }
                    content.push_str(&s.body);
                }
                Some(Page {
                    path: norm,
                    title: self.sections[start].title.clone(),
                    anchor: Some(a.to_string()),
                    content,
                    version: VERSION,
                })
            }
        }
    }

    /// Code examples from the sections most relevant to `query`, optionally
    /// filtered to a single `lang`. Returns up to `limit` examples.
    pub fn examples(&self, query: &str, lang: Option<&str>, limit: usize) -> Vec<Example> {
        let terms: HashSet<String> = tokenize(query).into_iter().collect();
        let mut out = Vec::new();
        for (idx, _) in self.ranked(&terms) {
            let s = &self.sections[idx];
            for cb in &s.code_blocks {
                if lang.is_some_and(|l| !cb.lang.eq_ignore_ascii_case(l)) {
                    continue;
                }
                out.push(Example {
                    path: s.path.clone(),
                    anchor: s.anchor.clone(),
                    title: s.title.clone(),
                    lang: cb.lang.clone(),
                    code: cb.code.clone(),
                    version: VERSION,
                });
                if out.len() >= limit {
                    return out;
                }
            }
        }
        out
    }

    /// Score every section that contains a query term and return them sorted by
    /// descending BM25 score (length-normalized + saturating, over the
    /// field-weighted term frequencies).
    fn ranked(&self, terms: &HashSet<String>) -> Vec<(usize, f32)> {
        const K1: f32 = 1.2;
        const B: f32 = 0.75;

        let mut scores: HashMap<usize, f32> = HashMap::new();
        let n = self.n_docs as f32;
        for term in terms {
            if let Some(plist) = self.postings.get(term) {
                let df = plist.len() as f32;
                // BM25 idf with the standard +0.5 smoothing.
                let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                for &(idx, freq) in plist {
                    let dl = self.doc_len[idx];
                    let denom = freq + K1 * (1.0 - B + B * dl / self.avgdl);
                    *scores.entry(idx).or_insert(0.0) += idf * (freq * (K1 + 1.0)) / denom;
                }
            }
        }
        // Navigation/boilerplate sections are excluded from the index entirely
        // (see `index::is_nav`), so nothing here scores them.
        let mut ranked: Vec<(usize, f32)> = scores.into_iter().collect();
        // Sort by score desc; break ties by section index for determinism.
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        ranked
    }

    /// The documentation table of contents (from the curated `SUMMARY.md`),
    /// for cheap browse/orientation.
    pub fn tree(&self) -> DocsTree {
        let nodes = self
            .summary
            .as_deref()
            .map(parse_summary)
            .unwrap_or_default();
        DocsTree {
            nodes,
            version: VERSION,
        }
    }

    /// First non-empty heading title on a page (its H1), falling back to the path.
    fn page_title(&self, path: &str) -> String {
        self.sections
            .iter()
            .find(|s| s.path == path && !s.title.is_empty())
            .map_or_else(|| path.to_string(), |s| s.title.clone())
    }
}

use std::collections::HashMap;

/// Accept `query/sparql.md`, `/query/sparql.md`, or `query/sparql` and
/// normalize to the corpus key form (`query/sparql.md`).
fn normalize_path(path: &str) -> String {
    let trimmed = path.trim().trim_start_matches('/');
    if trimmed.ends_with(".md") {
        trimmed.to_string()
    } else {
        format!("{trimmed}.md")
    }
}

/// A ~240-char window of `body` around the first matched query word (raw, not
/// stemmed, so the highlight lands on the literal text), whitespace collapsed,
/// with ellipses where it was clipped.
fn snippet(body: &str, query: &str) -> String {
    const WINDOW: usize = 240;
    const LEAD: usize = 60;

    let lower = body.to_lowercase();
    let first = query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .filter_map(|w| lower.find(&w.to_lowercase()))
        .min()
        .unwrap_or(0);

    let start = floor_boundary(body, first.saturating_sub(LEAD));
    let end = ceil_boundary(body, (start + WINDOW).min(body.len()));

    let mut out = body[start..end]
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if start > 0 {
        out.insert(0, '…');
    }
    if end < body.len() {
        out.push('…');
    }
    out
}

fn floor_boundary(s: &str, mut i: usize) -> usize {
    if i >= s.len() {
        return s.len();
    }
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn ceil_boundary(s: &str, mut i: usize) -> usize {
    if i >= s.len() {
        return s.len();
    }
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}
