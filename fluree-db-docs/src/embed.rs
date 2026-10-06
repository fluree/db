//! Build-time embedding of the `docs/` mdBook, and the policy for what belongs
//! in it.
//!
//! The corpus is included from `../docs` relative to this crate
//! (`CARGO_MANIFEST_DIR`). In release builds the bytes are baked into the
//! binary — offline, no filesystem dependency, version-exact by construction.
//!
//! # Corpus policy
//!
//! Embedding ships a page to every agent that can reach `fluree docs search` or
//! the `docs_search` MCP tool, so the embedded set is a *publishing* decision,
//! not a side effect of where a file happens to live. Every embedded page must
//! therefore be one of:
//!
//! 1. linked from `SUMMARY.md` — i.e. published in the mdBook too, so the two
//!    surfaces agree on what the documentation is; or
//! 2. listed in [`EMBEDDED_EXTRAS`] with a reason.
//!
//! Anything else — internal engineering records, scratch trees, build inputs —
//! is excluded by the `#[exclude]` attributes below and never reaches a binary.
//! `tests::every_embedded_page_is_published_or_an_extra` enforces this; a new
//! page under `docs/` fails it until someone chooses one of the three outcomes.
//! Only markdown is embedded (`#[include = "*.md"]`): the index skips
//! everything else, so any other file would be dead weight in the binary.
//! `README.md` section-index pages are kept — mdBook treats them as content.

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../docs"]
#[include = "*.md"]
#[exclude = "book/*"]
// Skip hidden directories (e.g. a `.llms-staging/` scratch tree) so they are
// never baked into release binaries; `index::build` filters them too.
#[exclude = ".*/*"]
// Internal engineering records: audit briefs, findings registers, and per-branch
// gate logs. They name branches, worktree paths, benchmark run IDs, and
// contributors — working notes for this repo, not documentation of the product.
// They stay in the tree (source comments cite them by repo-relative path) but
// they are not shipped to users or served to agents.
#[exclude = "audit/*"]
#[exclude = "audit-impl/*"]
pub struct DocsAssets;

/// Pages that are embedded on purpose even though `SUMMARY.md` does not link
/// them. Every entry needs a reason; an entry naming a file that no longer
/// exists fails `tests::embedded_extras_all_exist`, so the list can't rot.
///
/// Declared here rather than in the test module because it is policy, not test
/// data: it belongs next to the `#[exclude]`s it complements.
#[cfg_attr(not(test), expect(dead_code))]
pub const EMBEDDED_EXTRAS: &[&str] = &[
    // The curated TOC itself. Not a content page — `tree` returns it as
    // structure and `index::build` skips it — but it must be embedded for
    // `tree` to have anything to parse.
    "SUMMARY.md",
    // TODO(review): overlaps `operations/README.md`, `cli/README.md`, and
    // `getting-started/rust-api.md`. Kept searchable rather than dropped while
    // someone decides whether to publish it or fold it into those pages.
    "operations/running-fluree.md",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TreeNode;
    use crate::parse::parse_summary;
    use std::collections::BTreeSet;

    /// Every page path the `SUMMARY.md` TOC links, anchors stripped.
    fn published_paths() -> BTreeSet<String> {
        fn walk(nodes: &[TreeNode], out: &mut BTreeSet<String>) {
            for n in nodes {
                let path = n.path.split('#').next().unwrap_or(&n.path);
                if !path.is_empty() && !path.contains("://") {
                    out.insert(path.to_string());
                }
                walk(&n.children, out);
            }
        }
        let summary = DocsAssets::get("SUMMARY.md").expect("SUMMARY.md is embedded");
        let md = std::str::from_utf8(&summary.data).expect("SUMMARY.md is utf-8");
        let mut out = BTreeSet::new();
        walk(&parse_summary(md), &mut out);
        out
    }

    fn embedded_paths() -> Vec<String> {
        DocsAssets::iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn every_embedded_page_is_published_or_an_extra() {
        let published = published_paths();
        let unaccounted: Vec<String> = embedded_paths()
            .into_iter()
            .filter(|p| !published.contains(p) && !EMBEDDED_EXTRAS.contains(&p.as_str()))
            .collect();
        assert!(
            unaccounted.is_empty(),
            "these pages ship inside the binary but nothing published them: {unaccounted:?}\n\
             Pick one: link the page from docs/SUMMARY.md (publish it), add it to \
             EMBEDDED_EXTRAS with a reason, or add an #[exclude] in embed.rs (internal)."
        );
    }

    #[test]
    fn embedded_extras_all_exist() {
        // Catches rot the other way: an extra whose page was renamed or deleted
        // would otherwise sit in the allowlist forever, silently exempting a
        // path that no longer exists.
        let embedded: BTreeSet<String> = embedded_paths().into_iter().collect();
        let stale: Vec<&&str> = EMBEDDED_EXTRAS
            .iter()
            .filter(|p| !embedded.contains(**p))
            .collect();
        assert!(
            stale.is_empty(),
            "EMBEDDED_EXTRAS names pages that are not embedded (renamed, deleted, \
             or newly excluded?): {stale:?}"
        );
    }

    #[test]
    fn internal_audit_records_are_not_embedded() {
        // The specific leak this policy was written for: `docs/audit/` and
        // `docs/audit-impl/` are ~39 working records (branch names, worktree
        // paths, run IDs, attribution) that used to be retrievable from any
        // shipped binary via `fluree docs search`.
        let leaked: Vec<String> = embedded_paths()
            .into_iter()
            .filter(|p| p.starts_with("audit/") || p.starts_with("audit-impl/"))
            .collect();
        assert!(
            leaked.is_empty(),
            "internal engineering records embedded into the binary: {leaked:?}"
        );
        // And they are unreachable through search, not merely absent from iter().
        assert!(crate::index()
            .search("findings register", 10)
            .iter()
            .all(|h| !h.path.starts_with("audit")));
    }

    #[test]
    fn only_markdown_is_embedded() {
        // The index skips every non-`.md` file, so anything else would be bytes
        // in the binary that no surface can return. Book assets (images, the
        // mdBook config, the llms.txt preprocessor) are served from the repo by
        // the published site, not from here.
        let non_md: Vec<String> = embedded_paths()
            .into_iter()
            .filter(|p| !p.ends_with(".md"))
            .collect();
        assert!(non_md.is_empty(), "non-markdown in the corpus: {non_md:?}");
    }
}
