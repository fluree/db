//! Guards `autotests = false`.
//!
//! With auto-discovery off, a new `tests/*.rs` that nobody wires into a
//! `grp_*` harness (or declares as its own `[[test]]`) is never compiled and
//! never run — and `cargo test` still reports success. This test fails loudly
//! in that case instead.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

#[test]
fn every_test_file_is_reachable() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let tests_dir = root.join("tests");
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");

    // Files pulled into a harness via `#[path = "..."]`.
    let mut grouped: HashSet<String> = HashSet::new();
    for entry in fs::read_dir(&tests_dir).expect("read tests/") {
        let path = entry.expect("tests/ entry").path();
        let is_harness = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("grp_") && n.ends_with(".rs"));
        if !is_harness {
            continue;
        }
        let src = fs::read_to_string(&path).expect("read harness");
        for line in src.lines() {
            if let Some(rest) = line.trim().strip_prefix("#[path = \"") {
                if let Some(file) = rest.split('"').next() {
                    grouped.insert(file.to_string());
                }
            }
        }
    }

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

        // Harnesses are `[[test]]` targets in their own right.
        if name.starts_with("grp_") || grouped.contains(&name) {
            continue;
        }
        // Otherwise it must be declared as its own `[[test]]` target.
        if manifest.contains(&format!("path = \"tests/{name}\"")) {
            continue;
        }
        orphans.push(name);
    }

    orphans.sort();
    assert!(
        orphans.is_empty(),
        "unreachable under `autotests = false` — add a `#[path = \"<file>\"] \
         mod <name>;` line to the right grp_* harness, or declare a [[test]] \
         target: {orphans:?}"
    );
}
