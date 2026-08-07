//! Mapping W3C test URLs onto the shared `rdf-tests` checkout.
//!
//! # Submodule coupling (deliberate, documented)
//!
//! There is exactly ONE `w3c/rdf-tests` checkout in this repo, registered in
//! `.gitmodules` at `testsuite-sparql/rdf-tests`. This harness READS it
//! through the relative path `../testsuite-sparql/rdf-tests` rather than
//! declaring a second submodule, because two checkouts of the same upstream
//! repo could drift to different commits and make "conformance at submodule
//! SHA X" ambiguous — the freshness contract in
//! `riot-analog-bench-strategy.md` §6b needs a single SHA.
//!
//! The cost of that choice: `testsuite-rdf` does not build without
//! `testsuite-sparql`'s submodule initialized. [`rdf_tests_dir`] fails loudly
//! with the exact `git submodule` command when the directory is missing,
//! rather than reporting an empty suite.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// Root directory of the (shared) rdf-tests submodule.
///
/// See the module docs for why this lives under `testsuite-sparql/`.
pub fn rdf_tests_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("testsuite-sparql")
        .join("rdf-tests")
}

/// Verify the shared submodule is present and populated.
///
/// An uninitialized submodule is an empty directory, which would otherwise
/// surface as "manifest not found" — this turns it into the actionable
/// message.
pub fn ensure_rdf_tests_checkout() -> Result<()> {
    let dir = rdf_tests_dir();
    if !dir.join("rdf").is_dir() {
        bail!(
            "rdf-tests submodule is missing or uninitialized at {}\n\
             This harness shares testsuite-sparql's checkout (one submodule, one SHA).\n\
             Run:  git submodule update --init testsuite-sparql/rdf-tests",
            dir.display()
        );
    }
    Ok(())
}

/// The rdf-tests submodule commit, for the conformance report.
///
/// Read from git; `None` when git is unavailable (the report then records the
/// absence rather than a fabricated SHA).
pub fn rdf_tests_submodule_sha() -> Option<String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(rdf_tests_dir())
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let sha = String::from_utf8(out.stdout).ok()?;
    let sha = sha.trim();
    (!sha.is_empty()).then(|| sha.to_string())
}

/// Map a W3C test URL to a local filesystem path.
///
/// The RDF syntax manifests reference resources by their published URL:
///   `https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/IRI_subject.ttl`
/// which the submodule mirrors at:
///   `{rdf_tests_dir}/rdf/rdf11/rdf-turtle/IRI_subject.ttl`
pub fn url_to_path(url: &str) -> Result<PathBuf> {
    // Strip any fragment identifier (e.g. "#IRI_subject") before resolving.
    let url = url.split('#').next().unwrap_or(url);

    if let Some(relative) = url
        .strip_prefix("https://w3c.github.io/rdf-tests/")
        .or_else(|| url.strip_prefix("http://w3c.github.io/rdf-tests/"))
    {
        return Ok(rdf_tests_dir().join(relative));
    }

    bail!("URL does not match the rdf-tests publication pattern: {url}");
}

/// Read a test resource file, resolving from a W3C URL.
pub fn read_file_to_string(url: &str) -> Result<String> {
    let path = url_to_path(url)?;
    fs::read_to_string(&path).with_context(|| format!("Failed to read {}", path.display()))
}

/// Convert a possibly-relative manifest IRI to an absolute URL.
///
/// Manifest entries are already absolutized by the parser (the loader
/// prepends `@base <manifest-url>`); this covers the residual cases and the
/// `mf:include` chain.
///
/// Dot segments are removed (RFC 3986 §5.2.4). That is load-bearing, not
/// cosmetic: the RDF 1.2 manifests include the RDF 1.1 ones via
/// `<../../rdf11/rdf-turtle/manifest.ttl>`, so without normalization the same
/// test would carry two different IRIs depending on which suite reached it,
/// and a register entry written against one would not match the other.
pub fn resolve_relative_iri(base: &str, relative: &str) -> String {
    if relative.starts_with("http://") || relative.starts_with("https://") {
        return relative.to_string();
    }

    let base_no_fragment = base.split('#').next().unwrap_or(base);

    match base_no_fragment.rfind('/') {
        Some(pos) => remove_dot_segments(&format!("{}/{}", &base_no_fragment[..pos], relative)),
        None => relative.to_string(),
    }
}

/// Collapse `.` and `..` segments in the path portion of an absolute URL.
fn remove_dot_segments(url: &str) -> String {
    // Split off scheme://authority, which `..` must never escape.
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after_scheme = scheme_end + 3;
    let path_start = match url[after_scheme..].find('/') {
        Some(pos) => after_scheme + pos,
        None => return url.to_string(),
    };

    let (prefix, path) = url.split_at(path_start);

    let mut out: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "." => {}
            ".." => {
                // Keep the leading empty segment: it is the root slash.
                if out.len() > 1 {
                    out.pop();
                }
            }
            other => out.push(other),
        }
    }

    format!("{prefix}{}", out.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_to_path_maps_the_rdf11_turtle_suite() {
        let url = "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/IRI_subject.ttl";
        let path = url_to_path(url).unwrap();
        assert!(path.ends_with("rdf/rdf11/rdf-turtle/IRI_subject.ttl"));
    }

    #[test]
    fn url_to_path_strips_the_fragment() {
        let url = "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#IRI_subject";
        let path = url_to_path(url).unwrap();
        assert!(!path.to_string_lossy().contains('#'));
        assert!(path.ends_with("rdf/rdf11/rdf-turtle/manifest.ttl"));
    }

    #[test]
    fn url_to_path_rejects_foreign_urls() {
        assert!(url_to_path("http://example.org/unknown/path.ttl").is_err());
    }

    #[test]
    fn resolve_relative_iri_is_a_noop_on_absolute_urls() {
        let base = "https://w3c.github.io/rdf-tests/rdf/rdf11/manifest.ttl";
        let abs = "https://example.org/other.ttl";
        assert_eq!(resolve_relative_iri(base, abs), abs);
    }

    #[test]
    fn resolve_relative_iri_walks_into_subdirectories() {
        let base = "https://w3c.github.io/rdf-tests/rdf/rdf11/manifest.ttl";
        assert_eq!(
            resolve_relative_iri(base, "rdf-turtle/manifest.ttl"),
            "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl"
        );
    }

    /// The RDF 1.2 manifests reach the RDF 1.1 ones through `../../`; the
    /// tests they yield must carry the same IRIs either way.
    #[test]
    fn resolve_relative_iri_collapses_dot_segments() {
        let base = "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/manifest.ttl";
        assert_eq!(
            resolve_relative_iri(base, "../../rdf11/rdf-turtle/manifest.ttl"),
            "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl"
        );
        assert_eq!(
            resolve_relative_iri(base, "./eval/manifest.ttl"),
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/eval/manifest.ttl"
        );
    }

    /// `..` must not eat the authority.
    #[test]
    fn resolve_relative_iri_cannot_escape_the_host() {
        let base = "https://w3c.github.io/a/manifest.ttl";
        assert_eq!(
            resolve_relative_iri(base, "../../../x.ttl"),
            "https://w3c.github.io/x.ttl"
        );
    }

    /// The whole suite is silently empty if this path is wrong, so pin it.
    #[test]
    fn shared_submodule_is_reachable_from_here() {
        ensure_rdf_tests_checkout().unwrap();
    }
}
