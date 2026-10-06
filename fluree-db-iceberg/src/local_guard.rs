//! Opt-in guard for local-filesystem Iceberg table locations.
//!
//! Catalog-less `Direct` tables may live on the local filesystem (`file:///…`
//! or a bare absolute path). That is exactly right for the CLI and local dev,
//! but `table_location` reaches this crate from embedders that forward
//! caller-supplied input, and before local support existed the config gates
//! rejected anything that was not `s3://` — so downstream services inherited a
//! scheme check they never had to write themselves. Relaxing the gate without a
//! replacement would remove that protection silently, on a version bump.
//!
//! So local locations are **fail-closed**: refused unless the operator names
//! the directories they are willing to expose, via [`LOCAL_ROOTS_ENV`]. The
//! allowlist is a deployment-time switch rather than a cargo feature because
//! `iceberg` is a *default* feature of both the server and the CLI — a
//! compile-time flag would be on in precisely the builds that need it off, and
//! could not be turned on for a legitimate local deployment without a rebuild.
//!
//! The same roots do double duty as a containment boundary: every resolved
//! path is normalized and checked to land under one of them, so a manifest
//! reference such as `…/table/../../../etc/passwd` cannot escape. Against S3
//! that reference would merely 404; locally it would resolve.
//!
//! Two gates, one policy — both reached through this module:
//!
//! 1. **Config validation** ([`ensure_local_location_allowed`]) — refuses a
//!    disallowed location when the graph source is *created*, so the operator
//!    sees a clear error instead of a confusing storage failure later.
//! 2. **Path resolution** ([`resolve_local_path`]) — parses the location form
//!    and confines the result, on every read and listing.

use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;

use crate::error::{IcebergError, Result};

/// Allowlist of directories under which local Iceberg tables may be read.
///
/// Colon-separated absolute paths, in the style of `PATH`. **Unset or empty
/// (the default) disables local-filesystem tables entirely** — a `Direct`
/// `table_location` that is a `file://` URI or a bare absolute path is refused
/// at config validation. When set, local locations are permitted *and* every
/// resolved path must land under one of the roots.
///
/// `FLUREE_ICEBERG_LOCAL_ROOTS=/` restores unrestricted local access for a
/// deployment that genuinely wants it — deliberately spelled out rather than
/// implied by a build flag.
pub const LOCAL_ROOTS_ENV: &str = "FLUREE_ICEBERG_LOCAL_ROOTS";

/// The configured roots, or `None` when local tables are disabled.
///
/// Read once and cached: this is consulted on config validation and on the cold
/// metadata/scan-plan path, and the switch is a startup-time deployment
/// decision.
pub fn local_roots() -> Option<&'static [PathBuf]> {
    static ROOTS: OnceLock<Option<Vec<PathBuf>>> = OnceLock::new();
    ROOTS
        .get_or_init(|| parse_roots(std::env::var(LOCAL_ROOTS_ENV).ok().as_deref()))
        .as_deref()
}

/// Parse the allowlist. Split out from [`local_roots`] so it can be unit-tested
/// without mutating the shared process environment.
///
/// Entries must be absolute; relative entries are dropped rather than silently
/// resolved against an ambient working directory. An allowlist that parses to
/// nothing is indistinguishable from unset — local tables stay disabled.
fn parse_roots(raw: Option<&str>) -> Option<Vec<PathBuf>> {
    let roots = expand_roots(
        raw?.split(':')
            .map(str::trim)
            .filter(|s| s.starts_with('/'))
            .map(PathBuf::from),
    );
    (!roots.is_empty()).then_some(roots)
}

/// Normalize each configured root, keeping BOTH its lexical and its canonical
/// form when they differ.
///
/// A root is frequently reached through a symlink — macOS resolves `/var` to
/// `/private/var`, and `/tmp` with it — so a path canonicalized during
/// containment would not sit under the root as the operator spelled it. Holding
/// both forms lets one comparison satisfy either, without a `canonicalize` call
/// per read.
pub(crate) fn expand_roots(entries: impl Iterator<Item = PathBuf>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in entries {
        let lexical = normalize_lexical(&entry);
        match lexical.canonicalize() {
            Ok(real) if real != lexical => out.push(real),
            _ => {} // absent or already canonical
        }
        out.push(lexical);
    }
    out
}

/// Whether `location` addresses the local filesystem: a `file://` URI or a bare
/// absolute path. Pure syntax — this is the **routing** predicate (which storage
/// backend handles this location), not the permission check. Permission is
/// [`ensure_local_location_allowed`].
pub fn is_local_location(location: &str) -> bool {
    location.starts_with("file:/") || location.starts_with('/')
}

/// Refuse a local `table_location` unless the operator has allowlisted a root
/// containing it. Call from config validation so the refusal lands at
/// graph-source-creation time.
///
/// Non-local locations (`s3://`, …) pass through untouched — this guard has no
/// opinion about object stores.
pub fn ensure_local_location_allowed(location: &str) -> Result<()> {
    if !is_local_location(location) {
        return Ok(());
    }
    resolve_local_path(location).map(|_| ())
}

/// Parse a local location (`file:///abs`, `file:/abs`, or a bare `/abs`) into a
/// filesystem path, confined to the configured roots.
///
/// Object-store URIs are rejected by name — the usual cause is a table copied
/// from S3 whose manifests still reference the original bucket.
pub fn resolve_local_path(location: &str) -> Result<PathBuf> {
    resolve_local_path_within(location, local_roots().unwrap_or(&[]))
}

/// [`resolve_local_path`] against an explicit allowlist rather than the process
/// environment. The storage backend captures its roots once at construction and
/// calls this per read, so the hot path touches no global state.
pub fn resolve_local_path_within(location: &str, roots: &[PathBuf]) -> Result<PathBuf> {
    let raw = parse_local_location(location)?;
    confine(&raw, location, roots)
}

/// The location-form parsing, without the allowlist check: `file:///abs` and
/// the `file:/abs` single-slash variant some writers emit, or a bare absolute
/// path.
fn parse_local_location(location: &str) -> Result<PathBuf> {
    if let Some(rest) = location.strip_prefix("file://") {
        // `file:///abs` → `/abs`; `file://host/abs` is not supported (no remote
        // hosts), but `file://` + `/abs` parses as empty host + path.
        if let Some(p) = rest.strip_prefix('/') {
            // Guard the `file:////`-ish degenerate forms down to one root slash.
            return Ok(PathBuf::from(format!("/{}", p.trim_start_matches('/'))));
        }
        return Err(IcebergError::storage(format!(
            "Unsupported file:// URI (expected file:///absolute/path): {location}"
        )));
    }
    if let Some(rest) = location.strip_prefix("file:/") {
        return Ok(PathBuf::from(format!("/{rest}")));
    }
    if location.starts_with('/') {
        return Ok(PathBuf::from(location));
    }
    if location.starts_with("s3://")
        || location.starts_with("s3a://")
        || location.starts_with("gs://")
    {
        return Err(IcebergError::storage(format!(
            "Local file storage cannot read an object-store URI: {location}. This usually \
             means the table was copied from an object store and its manifests still \
             reference the original location; local reads need the table written with \
             local paths"
        )));
    }
    Err(IcebergError::storage(format!(
        "Local file storage requires a file:// URI or an absolute path, got: {location}"
    )))
}

/// Confine a parsed path to the configured roots.
///
/// Containment is checked twice: lexically (which needs no filesystem access
/// and so works for a location whose directory does not exist yet, as at config
/// validation), and — when the path exists — against its canonical form, which
/// is what catches an escape through a symlink planted inside a root.
fn confine(path: &Path, original: &str, roots: &[PathBuf]) -> Result<PathBuf> {
    if roots.is_empty() {
        return Err(IcebergError::Config(format!(
            "Local-filesystem Iceberg table locations are disabled: {original}. Set \
             {LOCAL_ROOTS_ENV} to a colon-separated list of absolute directories that \
             may be read (e.g. {LOCAL_ROOTS_ENV}=/srv/warehouse) to enable them; \
             locations outside those roots stay refused."
        )));
    }

    let lexical = normalize_lexical(path);
    if !within(&lexical, roots) {
        return Err(outside_roots_err(&lexical, original, roots));
    }
    // Only an existing path can be canonicalized; a miss here is not an escape,
    // and letting it through keeps "no such file" as the error the caller sees.
    match lexical.canonicalize() {
        Ok(real) if !within(&real, roots) => Err(outside_roots_err(&real, original, roots)),
        Ok(real) => Ok(real),
        Err(_) => Ok(lexical),
    }
}

fn outside_roots_err(resolved: &Path, original: &str, roots: &[PathBuf]) -> IcebergError {
    IcebergError::Config(format!(
        "Local Iceberg path {} (from {original}) is outside every directory allowed by \
         {LOCAL_ROOTS_ENV} [{}]",
        resolved.display(),
        roots
            .iter()
            .map(|r| r.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// Whether `path` is one of `roots` or lies beneath one. `Path::starts_with` is
/// component-wise, so `/wh` does not match `/wh2`.
fn within(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path.starts_with(root))
}

/// Resolve `.` and `..` textually, without touching the filesystem. `..` at the
/// root is absorbed, so the result can never climb above `/`.
fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_parse_absolute_entries_only() {
        assert_eq!(parse_roots(None), None, "unset disables local tables");
        assert_eq!(parse_roots(Some("")), None, "empty disables local tables");
        assert_eq!(
            parse_roots(Some("relative/path")),
            None,
            "a relative entry is dropped, not resolved against the cwd"
        );
        assert_eq!(
            parse_roots(Some("/srv/wh:/data/lake")),
            Some(vec![PathBuf::from("/srv/wh"), PathBuf::from("/data/lake")])
        );
        assert_eq!(
            parse_roots(Some(" /srv/wh : /data/lake ")),
            Some(vec![PathBuf::from("/srv/wh"), PathBuf::from("/data/lake")]),
            "entries are trimmed"
        );
        assert_eq!(
            parse_roots(Some("/srv/wh/../wh2")),
            Some(vec![PathBuf::from("/srv/wh2")]),
            "roots are themselves normalized"
        );
    }

    #[test]
    fn containment_is_component_wise() {
        let roots = vec![PathBuf::from("/srv/wh")];
        assert!(within(Path::new("/srv/wh"), &roots));
        assert!(within(Path::new("/srv/wh/people/metadata/x.json"), &roots));
        assert!(
            !within(Path::new("/srv/wh2/people"), &roots),
            "a sibling sharing a name prefix must not match"
        );
        assert!(!within(Path::new("/etc"), &roots));
    }

    #[test]
    fn lexical_normalization_absorbs_traversal() {
        assert_eq!(
            normalize_lexical(Path::new("/srv/wh/table/../../../etc/passwd")),
            PathBuf::from("/etc/passwd"),
            "traversal must be resolved so containment sees the real target"
        );
        assert_eq!(
            normalize_lexical(Path::new("/srv/./wh/./people")),
            PathBuf::from("/srv/wh/people")
        );
        assert_eq!(
            normalize_lexical(Path::new("/../../..")),
            PathBuf::from("/"),
            "`..` at the root is absorbed, never climbing above it"
        );
    }

    #[test]
    fn traversal_out_of_root_is_refused() {
        // The escape the guard exists to stop, checked against an explicit root
        // list so the test does not depend on the process environment.
        let roots = vec![PathBuf::from("/srv/wh")];
        let escaped = normalize_lexical(Path::new("/srv/wh/table/../../../etc/passwd"));
        assert!(!within(&escaped, &roots));
        let inside = normalize_lexical(Path::new("/srv/wh/people/../orders/metadata"));
        assert!(
            within(&inside, &roots),
            "traversal that stays inside a root is fine"
        );
    }

    #[test]
    fn location_forms_parse() {
        assert_eq!(
            parse_local_location("file:///srv/wh/people").unwrap(),
            PathBuf::from("/srv/wh/people")
        );
        assert_eq!(
            parse_local_location("file:/srv/wh/people").unwrap(),
            PathBuf::from("/srv/wh/people"),
            "single-slash variant some writers emit"
        );
        assert_eq!(
            parse_local_location("file:////srv/wh").unwrap(),
            PathBuf::from("/srv/wh"),
            "degenerate multi-slash collapses to one root slash"
        );
        assert_eq!(
            parse_local_location("/srv/wh/people").unwrap(),
            PathBuf::from("/srv/wh/people")
        );
    }

    #[test]
    fn object_store_uris_rejected_by_name() {
        let err = parse_local_location("s3://bucket/wh/people")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("object-store URI"),
            "names the actual cause: {err}"
        );
        assert!(parse_local_location("relative/path").is_err());
    }

    #[test]
    fn non_local_locations_bypass_the_guard() {
        // The guard has no opinion about object stores, and must not refuse them
        // when the allowlist is unset (the default in every test process here).
        assert!(ensure_local_location_allowed("s3://bucket/wh/people").is_ok());
        assert!(ensure_local_location_allowed("s3a://bucket/wh/people").is_ok());
    }

    #[test]
    fn disabled_by_default_and_error_names_the_switch() {
        // `local_roots()` reads the process env once; these tests run with it
        // unset, which is the shipped default.
        if local_roots().is_some() {
            return; // a caller exported the allowlist; nothing to assert here
        }
        let err = ensure_local_location_allowed("/srv/wh/people")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(LOCAL_ROOTS_ENV),
            "refusal must name the switch that enables it: {err}"
        );
        assert!(
            ensure_local_location_allowed("file:///srv/wh/people").is_err(),
            "the file:// form is gated identically to the bare path"
        );
    }

    #[test]
    fn is_local_location_covers_both_forms() {
        assert!(is_local_location("/srv/wh"));
        assert!(is_local_location("file:///srv/wh"));
        assert!(is_local_location("file:/srv/wh"));
        assert!(!is_local_location("s3://bucket/wh"));
        assert!(!is_local_location("relative"));
    }
}
