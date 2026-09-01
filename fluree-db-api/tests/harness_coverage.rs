//! Guards `autotests = false`.
//!
//! With auto-discovery off, a `tests/*.rs` that nobody declares as a `[[test]]`
//! target and nobody pulls into a declared harness is never compiled and never
//! run — and `cargo test` still reports success. With ~260 case files behind 60
//! declared targets, that is easy to do by accident. This test fails loudly
//! instead.
//!
//! Reachability is derived from `Cargo.toml` outwards, never from the file
//! names: an undeclared `tests/grp_foo.rs` is itself an orphan, and the files it
//! references do not count as covered just because something on disk mentions
//! them. Trusting a `grp_` prefix to mean "surely declared" is exactly how a
//! whole harness goes missing without the guard noticing.
//!
//! Kept deliberately in sync with `fluree-db-server/tests/harness_coverage.rs`.
//! The logic is identical; duplicating ~100 lines of self-contained test is
//! cheaper than a shared dev-dependency crate existing solely to hold it.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// `path` values of every `[[test]]` target in the manifest, relative to the
/// crate root. A target may omit `path`, in which case Cargo infers
/// `tests/<name>.rs` — this crate declares two targets that way.
fn declared_test_paths(manifest: &str) -> HashSet<String> {
    let mut declared = HashSet::new();
    let mut in_test = false;
    let mut name: Option<String> = None;
    let mut path: Option<String> = None;

    let mut flush = |name: &mut Option<String>, path: &mut Option<String>| {
        if let Some(p) = path.take() {
            declared.insert(p);
        } else if let Some(n) = name.take() {
            declared.insert(format!("tests/{n}.rs"));
        }
        *name = None;
        *path = None;
    };

    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            if in_test {
                flush(&mut name, &mut path);
            }
            in_test = line == "[[test]]";
            continue;
        }
        if !in_test {
            continue;
        }
        if let Some(v) = line
            .strip_prefix("name = \"")
            .and_then(|r| r.split('"').next())
        {
            name = Some(v.to_string());
        } else if let Some(v) = line
            .strip_prefix("path = \"")
            .and_then(|r| r.split('"').next())
        {
            path = Some(v.to_string());
        }
    }
    if in_test {
        flush(&mut name, &mut path);
    }
    declared
}

#[test]
fn every_test_file_is_reachable() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let tests_dir = root.join("tests");
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");

    let declared = declared_test_paths(&manifest);

    // Files pulled in via `#[path = "..."]` by a *declared* target. Harvesting
    // from every file on disk instead would let an undeclared harness launder
    // its members into the covered set.
    let mut referenced: HashSet<String> = HashSet::new();
    for rel in &declared {
        let Ok(src) = fs::read_to_string(root.join(rel)) else {
            continue; // a declared path that does not exist is Cargo's error to report
        };
        for line in src.lines() {
            if let Some(file) = line
                .trim()
                .strip_prefix("#[path = \"")
                .and_then(|r| r.split('"').next())
            {
                referenced.insert(file.to_string());
            }
        }
    }

    // Only top-level files become test binaries; tests/support and
    // tests/fixtures are pulled in by path and are not enumerated here.
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

#[test]
fn declared_paths_cover_both_declaration_forms() {
    let manifest = r#"
[package]
name = "x"

[[test]]
name = "grp_query"
path = "tests/grp_query.rs"

[[test]]
name = "inferred_path_target"

[[test]]
name = "gated"
path = "tests/gated.rs"
required-features = ["iceberg"]

[[bench]]
name = "not_a_test"
path = "benches/not_a_test.rs"

[lints]
workspace = true
"#;
    let declared = declared_test_paths(manifest);

    assert!(declared.contains("tests/grp_query.rs"));
    // `path` omitted — Cargo infers tests/<name>.rs. This crate declares
    // it_absent_subject_scan_narrowing and it_values_object_bounds that way.
    assert!(declared.contains("tests/inferred_path_target.rs"));
    // Keys after `path` must not spill the target into the next block.
    assert!(declared.contains("tests/gated.rs"));
    // Other target kinds are not [[test]] targets.
    assert!(!declared.contains("benches/not_a_test.rs"));
    assert_eq!(declared.len(), 3);
}
