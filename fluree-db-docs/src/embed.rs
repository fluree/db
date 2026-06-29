//! Build-time embedding of the `docs/` mdBook.
//!
//! The corpus is included from `../docs` relative to this crate
//! (`CARGO_MANIFEST_DIR`). In release builds the bytes are baked into the
//! binary — offline, no filesystem dependency, version-exact by construction.
//! The `book/` build output is excluded; non-`.md` files and `SUMMARY.md` (the
//! curated TOC) are filtered out at index-build time, not here. `README.md`
//! section-index pages are kept — mdBook treats them as content.

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../docs"]
#[exclude = "book/*"]
// Skip hidden directories (e.g. a `.llms-staging/` scratch tree) so they are
// never baked into release binaries; `index::build` filters them too.
#[exclude = ".*/*"]
pub struct DocsAssets;
