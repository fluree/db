//! The in-memory docs index: built once from the embedded corpus, then queried
//! by both surfaces. Small enough (~200 pages) for a plain TF-IDF inverted
//! index — no external search engine.

use crate::embed::DocsAssets;
use crate::model::Section;
use crate::parse::parse_sections;
use rust_stemmers::{Algorithm, Stemmer};
use std::collections::HashMap;
use std::sync::OnceLock;

/// term → list of (section index, field-weighted term frequency).
type Postings = HashMap<String, Vec<(usize, f32)>>;

/// Inverted index over heading-level [`Section`]s.
pub struct DocsIndex {
    /// All sections, in corpus order. `SearchHit`/`Example` reference these by index.
    pub(crate) sections: Vec<Section>,
    /// Full original markdown per page, for whole-page `get`.
    pub(crate) pages: HashMap<String, String>,
    /// term → list of (section index, field-weighted term frequency).
    pub(crate) postings: Postings,
    /// Field-weighted length of each section, for BM25 length normalization.
    pub(crate) doc_len: Vec<f32>,
    /// Average `doc_len` across sections.
    pub(crate) avgdl: f32,
    /// Total number of sections (documents) for IDF.
    pub(crate) n_docs: usize,
    /// Raw `SUMMARY.md` (the curated TOC), for `tree`. `None` if absent.
    pub(crate) summary: Option<String>,
}

impl DocsIndex {
    /// Build the index from the embedded `docs/` corpus. Called once, lazily
    /// (see [`crate::index`]). Skips non-markdown, `SUMMARY.md`/`README.md`, and
    /// the `book/` build output.
    pub fn build() -> Self {
        let mut sections: Vec<Section> = Vec::new();
        let mut pages: HashMap<String, String> = HashMap::new();

        for path in DocsAssets::iter() {
            let p = path.as_ref();
            if !p.ends_with(".md") {
                continue;
            }
            // Skip the mdBook build output and any hidden directory (e.g. a
            // `.llms-staging/` scratch tree) — mdBook ignores dot-prefixed
            // entries, so we do too, to avoid indexing duplicate pages.
            if p.starts_with("book/") || p.split('/').any(|seg| seg.starts_with('.')) {
                continue;
            }
            let base = p.rsplit('/').next().unwrap_or(p);
            if base.eq_ignore_ascii_case("SUMMARY.md") || base.eq_ignore_ascii_case("README.md") {
                continue;
            }
            let Some(file) = DocsAssets::get(p) else {
                continue;
            };
            let Ok(md) = std::str::from_utf8(&file.data) else {
                continue;
            };
            pages.insert(p.to_string(), md.to_string());
            sections.extend(parse_sections(p, md));
        }

        let (postings, doc_len) = build_postings(&sections);
        let n_docs = sections.len();
        let avgdl = if n_docs == 0 {
            1.0
        } else {
            (doc_len.iter().sum::<f32>() / n_docs as f32).max(1.0)
        };
        let summary = DocsAssets::get("SUMMARY.md")
            .and_then(|f| std::str::from_utf8(&f.data).ok().map(str::to_string));
        Self {
            sections,
            pages,
            postings,
            doc_len,
            avgdl,
            n_docs,
            summary,
        }
    }
}

/// Build the term → (section, weighted-tf) postings plus each section's
/// field-weighted length. Field boosts (title ×3, heading-path ×2, body ×1) are
/// baked into the term frequency, so a query word in a heading outranks one
/// buried in body text; BM25 length normalization (see `ranked`) then keeps a
/// short, high-signal heading from losing to a long section that merely repeats
/// the term.
fn build_postings(sections: &[Section]) -> (Postings, Vec<f32>) {
    let mut postings: Postings = HashMap::new();
    let mut doc_len: Vec<f32> = Vec::with_capacity(sections.len());

    for (idx, section) in sections.iter().enumerate() {
        // Navigation/boilerplate sections ("Related Documentation", link-list
        // footers) are never a useful search answer — their value is their
        // links, which `tree`/`get` cover. Exclude them from the index so they
        // can't surface as hits, while leaving them in `sections` so `get` and
        // page reconstruction still return them.
        if is_nav(section) {
            doc_len.push(0.0);
            continue;
        }

        let mut tf: HashMap<String, f32> = HashMap::new();
        add_tokens(&mut tf, &section.title, 3.0);
        for h in &section.heading_path {
            add_tokens(&mut tf, h, 2.0);
        }
        add_tokens(&mut tf, &section.body, 1.0);

        doc_len.push(tf.values().sum());
        for (term, weight) in tf {
            postings.entry(term).or_default().push((idx, weight));
        }
    }

    (postings, doc_len)
}

/// Whether a section is navigation/boilerplate that should be kept out of the
/// search index. Conservative by design: an unambiguous nav heading, or a body
/// that is overwhelmingly markdown links (a "see also" link list). A normal
/// prose section trips neither test, so real content is never excluded — and
/// anything misjudged is still reachable via `tree`/`get`.
fn is_nav(section: &Section) -> bool {
    let title = section.title.trim().to_lowercase();
    let nav_title = matches!(
        title.as_str(),
        "related documentation"
            | "related docs"
            | "related"
            | "related topics"
            | "related reading"
            | "see also"
            | "further reading"
            | "next steps"
    );
    nav_title || link_density(&section.body) > 0.25
}

/// Fraction of whitespace-separated tokens that are markdown link openers
/// (`](`). A link-list footer scores high; prose scores near zero.
fn link_density(body: &str) -> f32 {
    let words = body.split_whitespace().count().max(1);
    body.matches("](").count() as f32 / words as f32
}

fn add_tokens(tf: &mut HashMap<String, f32>, text: &str, weight: f32) {
    for token in tokenize(text) {
        *tf.entry(token).or_insert(0.0) += weight;
    }
}

/// Lowercase, split on non-alphanumeric boundaries, then Snowball-stem so query
/// and document terms match across inflections ("path" ⇄ "paths"). IDF handles
/// common words, so no stopword list is needed for this corpus.
pub(crate) fn tokenize(text: &str) -> Vec<String> {
    let stemmer = stemmer();
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| stemmer.stem(&t.to_lowercase()).into_owned())
        .collect()
}

fn stemmer() -> &'static Stemmer {
    static STEMMER: OnceLock<Stemmer> = OnceLock::new();
    STEMMER.get_or_init(|| Stemmer::create(Algorithm::English))
}
