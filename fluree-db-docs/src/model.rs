//! Surface-agnostic data model for the docs corpus.
//!
//! Every result type carries [`VERSION`] so a consumer (CLI or MCP) can trust a
//! hit matches the binary it came from — the docs are embedded from this exact
//! build (see the crate docs).

use serde::Serialize;

/// The docs version == the binary version (`CARGO_PKG_VERSION`, the workspace
/// version). Stamped onto every returned result.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// A heading-scoped slice of a doc page — the unit of indexing and retrieval.
///
/// One `Section` per heading: its `body` is the original markdown from the
/// heading line up to (but not including) the next heading of equal-or-shallower
/// level. A page's leading content before its first heading is captured as a
/// preamble section with an empty `anchor`/`title`.
#[derive(Debug, Clone)]
pub struct Section {
    /// Book-relative path with forward slashes, e.g. `query/sparql.md`.
    pub path: String,
    /// GitHub/mdBook heading slug, e.g. `property-paths`. Empty for a preamble.
    pub anchor: String,
    /// Heading text, e.g. `Property Paths`. Empty for a preamble.
    pub title: String,
    /// Breadcrumb of ancestor headings, e.g. `["SPARQL", "Property Paths"]`.
    pub heading_path: Vec<String>,
    /// Heading level 1..=6 (0 for a preamble).
    pub level: u8,
    /// Original markdown for this section (includes the heading line itself).
    pub body: String,
    /// Fenced code blocks found within this section.
    pub code_blocks: Vec<CodeBlock>,
}

/// A fenced code block extracted from a section.
#[derive(Debug, Clone, Serialize)]
pub struct CodeBlock {
    /// Info-string language (first token), e.g. `sparql`. Empty if unspecified.
    pub lang: String,
    /// The code body.
    pub code: String,
}

/// A ranked search hit at section granularity.
#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub path: String,
    pub anchor: String,
    pub title: String,
    pub heading_path: Vec<String>,
    pub snippet: String,
    pub score: f32,
    pub version: &'static str,
}

/// A page, or a heading-scoped slice of one, returned as markdown.
#[derive(Debug, Clone, Serialize)]
pub struct Page {
    pub path: String,
    pub title: String,
    pub anchor: Option<String>,
    pub content: String,
    pub version: &'static str,
}

/// The documentation table of contents (from the curated `SUMMARY.md`), for
/// cheap browse/orientation.
#[derive(Debug, Clone, Serialize)]
pub struct DocsTree {
    pub nodes: Vec<TreeNode>,
    pub version: &'static str,
}

/// One entry in the docs TOC tree.
#[derive(Debug, Clone, Serialize)]
pub struct TreeNode {
    pub title: String,
    /// Book-relative page path, e.g. `query/sparql.md`.
    pub path: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<TreeNode>,
}

/// A code example tied back to the section it came from.
#[derive(Debug, Clone, Serialize)]
pub struct Example {
    pub path: String,
    pub anchor: String,
    pub title: String,
    pub lang: String,
    pub code: String,
    pub version: &'static str,
}
