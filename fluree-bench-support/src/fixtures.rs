//! Vendored and remote fixture loading.
//!
//! ## Status (bench-1 scope)
//!
//! Skeleton only. Vendored fixtures land in `bench-4` (with the BSBM-1K
//! Turtle file). Remote fetching lands in `bench-6` (nightly + S3).
//! Today this module exposes the public surface so benches can refer to
//! it; the bodies are intentionally minimal.
//!
//! ## Eventual API
//!
//! ```rust,ignore
//! use fluree_bench_support::fixtures;
//!
//! // Load a vendored fixture; falls back to inline generation if the
//! // fixture is too small to vendor or if it's not present.
//! let bsbm_tiny = fixtures::load_or_generate("bsbm", BenchScale::Tiny);
//! ```
//!
//! Fixtures live under `fluree-bench-support/fixtures/<name>/` keyed by
//! BenchScale. Format is whatever the fixture wants; usually `.ttl.gz`
//! for Turtle, `.jsonld` for JSON-LD, or a directory of files.

use std::path::PathBuf;

use crate::runtime::BenchScale;

/// Path to the workspace's vendored fixtures directory, if it exists.
///
/// Resolves via [`crate::budget::workspace_root`] + `fluree-bench-support/fixtures`.
/// Returns `None` when called outside the workspace tree (e.g., from a
/// dependent crate's published-on-crates.io copy).
pub fn fixtures_dir() -> Option<PathBuf> {
    let root = crate::budget::workspace_root()?;
    let dir = root.join("fluree-bench-support").join("fixtures");
    if dir.exists() {
        Some(dir)
    } else {
        None
    }
}

/// Path to a per-fixture directory for the given name.
pub fn fixture_path(name: &str) -> Option<PathBuf> {
    fixtures_dir().map(|d| d.join(name))
}

/// Path to a per-(name, scale) fixture directory.
pub fn fixture_path_for_scale(name: &str, scale: BenchScale) -> Option<PathBuf> {
    fixture_path(name).map(|p| p.join(scale.as_str()))
}

/// Whether a vendored fixture is present for `(name, scale)`.
pub fn has_vendored(name: &str, scale: BenchScale) -> bool {
    fixture_path_for_scale(name, scale)
        .map(|p| p.exists())
        .unwrap_or(false)
}

/// Stub for the eventual `load_or_generate` entry point.
///
/// **Not yet usable.** This will be filled in `bench-4` (vendored data) and
/// `bench-6` (remote fetch). Documented here so benches can compile against
/// the public surface today and migrate to live data later.
pub fn load_or_generate(_name: &str, _scale: BenchScale) -> FixtureRef {
    FixtureRef::placeholder()
}

/// Opaque handle to a fixture. Body filled in `bench-4`.
#[derive(Debug, Clone)]
pub struct FixtureRef {
    /// Marker only, until the body is implemented.
    pub(crate) _marker: (),
}

impl FixtureRef {
    pub(crate) fn placeholder() -> Self {
        Self { _marker: () }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_dir_resolves_in_workspace() {
        // Runs from the workspace; the dir exists (bench-1 created it).
        if let Some(dir) = fixtures_dir() {
            assert!(dir.ends_with("fluree-bench-support/fixtures"));
        }
    }

    #[test]
    fn vendored_check_is_conservative() {
        // Without any vendored data, every name/scale should return false.
        assert!(!has_vendored(
            "definitely_not_present_xyz",
            BenchScale::Tiny
        ));
    }
}
