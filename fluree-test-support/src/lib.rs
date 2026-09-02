//! Reachability guard for crates that group their integration tests.
//!
//! A crate with `autotests = false` declares its `[[test]]` targets explicitly,
//! so a `tests/*.rs` becomes a test binary only if it is named in a `[[test]]`
//! block or pulled into a harness via `#[path]`. That buys far fewer link steps
//! and costs one invariant the compiler does not check: a file that is neither
//! declared nor pulled in is never compiled and never run, and `cargo test`
//! still reports success, because from Cargo's point of view there is nothing
//! to build.
//!
//! Call the assert from a test in each adopting crate, passing that crate's own
//! manifest directory:
//!
//! ```ignore
//! #[test]
//! fn every_test_file_is_reachable() {
//!     fluree_test_support::assert_every_test_file_is_reachable(env!("CARGO_MANIFEST_DIR"));
//! }
//! ```
//!
//! `env!` expands at the call site, so each crate is checked against its own
//! `Cargo.toml` and `tests/` directory.
//!
//! The other rules in `docs/contributing/tests.md` — that env-mutating and
//! instrumentation-asserting tests keep their own binary — are deliberately not
//! enforced here. Checking them means scanning Rust source for call shapes,
//! which costs more in false positives and blind spots than review does.

#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// `path` of every `[[test]]` target, relative to the crate root. A target may
/// omit `path`, in which case Cargo infers `tests/<name>.rs`.
fn declared_test_paths(manifest: &str) -> HashSet<String> {
    let parsed: toml::Value = manifest.parse().expect("parse Cargo.toml");
    let Some(targets) = parsed.get("test").and_then(toml::Value::as_array) else {
        return HashSet::new();
    };
    targets
        .iter()
        .filter_map(|target| {
            target
                .get("path")
                .and_then(toml::Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    target
                        .get("name")
                        .and_then(toml::Value::as_str)
                        .map(|name| format!("tests/{name}.rs"))
                })
        })
        .collect()
}

/// Files pulled in via `#[path = "..."]` by a *declared* target, as written in
/// the attribute (i.e. relative to `tests/`).
///
/// Harvesting from every file on disk instead would let an undeclared harness
/// launder its members into the covered set.
fn referenced_by_declared_targets(root: &Path, declared: &HashSet<String>) -> HashSet<String> {
    let mut referenced = HashSet::new();
    for rel in declared {
        let Ok(src) = fs::read_to_string(root.join(rel)) else {
            continue; // a declared path that does not exist is Cargo's error to report
        };
        for line in src.lines() {
            if let Some(file) = line
                .trim()
                .strip_prefix("#[path = \"")
                .and_then(|rest| rest.split('"').next())
            {
                referenced.insert(file.to_string());
            }
        }
    }
    referenced
}

/// Every top-level `tests/*.rs` must be declared as a `[[test]]` target or
/// pulled into one via `#[path]`.
///
/// Reachability is derived from `Cargo.toml` outwards, never from file names:
/// an undeclared `tests/grp_foo.rs` is itself an orphan, and the files it
/// references are not credited as covered on its say-so.
///
/// # Panics
///
/// Panics listing every unreachable file, or if the crate's `Cargo.toml` or
/// `tests/` directory cannot be read.
pub fn assert_every_test_file_is_reachable(manifest_dir: &str) {
    let root = Path::new(manifest_dir);
    let tests_dir = root.join("tests");
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");

    let declared = declared_test_paths(&manifest);
    let referenced = referenced_by_declared_targets(root, &declared);

    // Only top-level files become test binaries; subdirectories such as
    // tests/support are pulled in by path and are not enumerated here.
    let mut orphans: Vec<String> = Vec::new();
    for entry in fs::read_dir(&tests_dir).expect("read tests/") {
        let path = entry.expect("tests/ entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("utf-8 filename")
            .to_string();

        if declared.contains(&format!("tests/{name}")) || referenced.contains(&name) {
            continue;
        }
        orphans.push(name);
    }

    orphans.sort();
    assert!(
        orphans.is_empty(),
        "unreachable under `autotests = false` — every tests/*.rs must be \
         declared as a [[test]] target or pulled into one via `#[path = \
         \"<file>\"] mod <name>;`. Orphaned: {orphans:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the inference this crate adds on top of TOML, not TOML itself:
    /// a `[[test]]` without `path` means `tests/<name>.rs`, and other target
    /// kinds are not test targets.
    #[test]
    fn a_target_without_a_path_infers_tests_name_rs() {
        let manifest = r#"
[[test]]
name = "grp_query"
path = "tests/grp_query.rs"

[[test]]
name = "inferred"

[[bench]]
name = "not_a_test"
path = "benches/not_a_test.rs"
"#;
        let declared = declared_test_paths(manifest);

        assert!(declared.contains("tests/grp_query.rs"));
        assert!(declared.contains("tests/inferred.rs"));
        assert!(!declared.contains("benches/not_a_test.rs"));
        assert_eq!(declared.len(), 2);
    }

    #[test]
    fn a_manifest_with_no_test_targets_declares_nothing() {
        assert!(declared_test_paths("[package]\nname = \"x\"\n").is_empty());
    }
}
