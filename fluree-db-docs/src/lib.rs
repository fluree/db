//! Embedded, version-pinned Fluree documentation lookup.
//!
//! The `docs/` mdBook is embedded into the binary at build time and exposed as a
//! small search/get/examples API over heading-level sections. Because the docs
//! ship inside the binary, every result is **version-exact by construction** —
//! it matches the exact build (and, in the standard local-dev workflow, the
//! exact Fluree database) the agent is working against.
//!
//! Two surfaces consume this crate identically:
//! - the `fluree docs` CLI subcommand, and
//! - the standalone `fluree-docs` MCP server ([`mcp::DocsToolService`], behind
//!   the `mcp` feature).

mod embed;
mod index;
mod model;
mod parse;
mod search;

#[cfg(feature = "mcp")]
pub mod mcp;

pub use index::DocsIndex;
pub use model::{CodeBlock, DocsTree, Example, Page, SearchHit, Section, TreeNode, VERSION};

use std::sync::OnceLock;

static INDEX: OnceLock<DocsIndex> = OnceLock::new();

/// The lazily-built, process-global docs index. First call parses + indexes the
/// embedded corpus (single-digit milliseconds); subsequent calls are free.
pub fn index() -> &'static DocsIndex {
    INDEX.get_or_init(DocsIndex::build)
}

/// The docs/binary version stamped onto every result (`CARGO_PKG_VERSION`).
pub fn version() -> &'static str {
    VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_embeds_expected_pages() {
        let n = embed::DocsAssets::iter()
            .filter(|p| p.ends_with(".md") && !p.starts_with("book/"))
            .count();
        assert!(n > 190, "expected the docs corpus, got {n} markdown files");
        assert!(embed::DocsAssets::get("query/sparql.md").is_some());
    }

    #[test]
    fn search_finds_property_paths_first() {
        let hits = index().search("property path", 5);
        assert!(!hits.is_empty());
        let top = &hits[0];
        assert_eq!(top.path, "query/sparql.md");
        assert_eq!(top.anchor, "property-paths");
        assert_eq!(top.version, VERSION);
    }

    #[test]
    fn title_match_outranks_body_only_match() {
        // "property" appears in many pages' bodies, but the SPARQL Property
        // Paths heading should win on the title/heading boost.
        let hits = index().search("property paths", 3);
        assert_eq!(hits[0].path, "query/sparql.md");
    }

    #[test]
    fn get_whole_page_and_heading_slice() {
        let page = index().get("query/sparql.md", None).unwrap();
        assert!(page.content.len() > 1000);
        assert_eq!(page.anchor, None);

        let slice = index()
            .get("query/sparql.md", Some("property-paths"))
            .unwrap();
        assert_eq!(slice.anchor.as_deref(), Some("property-paths"));
        assert!(slice.content.len() < page.content.len());
        assert!(slice.content.to_lowercase().contains("path"));

        // Path normalization: no extension, leading slash both resolve.
        assert!(index().get("/query/sparql", None).is_some());
        // Unknown page → None.
        assert!(index().get("does/not/exist.md", None).is_none());
    }

    #[test]
    fn get_parent_heading_returns_full_subtree() {
        // A container heading must return its child sections, not just its
        // intro prose. "## Query Forms" parents SELECT/CONSTRUCT/ASK/DESCRIBE.
        let page = index().get("query/sparql.md", Some("query-forms")).unwrap();
        let body = page.content.to_lowercase();
        assert!(
            body.contains("select"),
            "subtree should include child sections"
        );
        assert!(body.contains("construct"));
    }

    #[test]
    fn nav_sections_are_excluded_from_search() {
        // A "Related documentation" footer must never be a search hit, even
        // though the page (graph-crawl.md) is full of "property path" mentions.
        let hits = index().search("property path", 10);
        assert!(
            hits.iter()
                .all(|h| h.title.to_lowercase() != "related documentation"),
            "nav sections should be excluded, got: {:?}",
            hits.iter().map(|h| &h.title).collect::<Vec<_>>()
        );
    }

    #[test]
    fn every_tree_path_is_fetchable() {
        // The tree (from SUMMARY.md) advertises page paths for browse/orient;
        // each must resolve via `get`. Regression: README.md section-index
        // pages used to be excluded from the index, so paths the tree exposed
        // (e.g. `cli/README.md`, the root `README.md`) returned None.
        fn collect<'a>(nodes: &'a [TreeNode], out: &mut Vec<&'a str>) {
            for n in nodes {
                out.push(&n.path);
                collect(&n.children, out);
            }
        }
        let tree = index().tree();
        let mut paths = Vec::new();
        collect(&tree.nodes, &mut paths);
        assert!(paths.len() > 100, "expected a populated TOC, got {paths:?}");

        let unfetchable: Vec<&str> = paths
            .iter()
            .copied()
            .filter(|p| index().get(p, None).is_none())
            .collect();
        assert!(
            unfetchable.is_empty(),
            "tree advertises paths that `get` can't resolve: {unfetchable:?}"
        );
    }

    #[test]
    fn readme_section_index_pages_are_indexed() {
        // README.md pages carry real content (e.g. the CLI overview's
        // Installation / Quick Start). They must be both fetchable and
        // searchable, not silently dropped.
        let cli = index().get("cli/README.md", None).expect("cli/README.md");
        assert!(cli.content.to_lowercase().contains("command-line"));
        assert!(index().get("README.md", None).is_some(), "root Introduction");
    }

    #[test]
    fn tree_reflects_summary_toc() {
        let tree = index().tree();
        assert!(!tree.nodes.is_empty());
        assert_eq!(tree.version, VERSION);
        // The CLI group is a top-level node with children (init, query, …).
        let cli = tree
            .nodes
            .iter()
            .find(|n| n.path == "cli/README.md")
            .expect("CLI node present in TOC");
        assert!(cli.children.iter().any(|c| c.path == "cli/init.md"));
    }

    #[test]
    fn examples_extracts_code_with_lang() {
        let ex = index().examples("transaction", None, 5);
        assert!(!ex.is_empty());
        assert!(ex.iter().all(|e| e.version == VERSION));

        // Language filter only returns matching blocks.
        let json_only = index().examples("query", Some("json"), 10);
        assert!(json_only
            .iter()
            .all(|e| e.lang.eq_ignore_ascii_case("json")));
    }
}
