//! Guards `autotests = false`.
//!
//! With auto-discovery off, a `tests/*.rs` that nobody declares as a `[[test]]`
//! target and nobody pulls into a declared harness is never compiled and never
//! run — and `cargo test` still reports success. This test fails loudly in that
//! case instead.
//!
//! Reachability is derived from `Cargo.toml` outwards, never from the file
//! names: an undeclared `tests/grp_foo.rs` is itself an orphan, and the files it
//! references do not count as covered just because something on disk mentions
//! them. Trusting a `grp_` prefix to mean "surely declared" is exactly how a
//! whole harness goes missing without the guard noticing.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// `path` values of every `[[test]]` target in the manifest, relative to the
/// crate root. A target may omit `path`, in which case Cargo infers
/// `tests/<name>.rs`.
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

/// Files pulled in via `#[path = "..."]` by a *declared* target, keyed by the
/// harness that pulls them in. Harvesting from every file on disk instead would
/// let an undeclared harness launder its members into the covered set.
///
/// Paths are as written in the attribute, i.e. relative to `tests/`.
fn grouped_members(root: &Path, declared: &HashSet<String>) -> Vec<(String, String)> {
    let mut members = Vec::new();
    for rel in declared {
        let Ok(src) = fs::read_to_string(root.join(rel)) else {
            continue; // a declared path that does not exist is Cargo's error to report
        };
        for line in src.lines() {
            if let Some(file) = line
                .trim()
                .strip_prefix("#[path = \"")
                .and_then(|r| r.split('"').next())
            {
                members.push((rel.clone(), file.to_string()));
            }
        }
    }
    members
}

#[test]
fn every_test_file_is_reachable() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let tests_dir = root.join("tests");
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");

    let declared = declared_test_paths(&manifest);
    let referenced: HashSet<String> = grouped_members(root, &declared)
        .into_iter()
        .map(|(_harness, member)| member)
        .collect();

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

/// Call shapes for process-global env mutation.
///
/// Assembled with `concat!` so this file does not contain the literals it
/// searches for. It is pulled into a harness like any other case file, so a
/// plain literal would make the guard flag itself.
const ENV_MUTATION: [&str; 2] = [concat!("set", "_var("), concat!("remove", "_var(")];

/// True when `body` calls one of the mutators. A match must begin at a word
/// boundary, so an identifier that merely ends in one — `unset_var(`, say —
/// does not count.
fn mutates_process_env(body: &str) -> bool {
    ENV_MUTATION.iter().any(|needle| {
        let mut from = 0;
        while let Some(i) = body[from..].find(needle) {
            let at = from + i;
            let preceded_by_ident = body[..at]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric() || c == '_');
            if !preceded_by_ident {
                return true;
            }
            from = at + needle.len();
        }
        false
    })
}

#[test]
fn grouped_tests_do_not_mutate_process_env() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let tests_dir = root.join("tests");
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");

    let declared = declared_test_paths(&manifest);
    let members = grouped_members(root, &declared);

    // A target is a *harness* only if it pulls in a sibling top-level case file.
    // Standalone targets use #[path] too, but only to reach shared helpers such
    // as support/span_capture.rs; they are still alone in their process, which
    // is precisely what makes mutating env legitimate for them.
    let harnesses: HashSet<&String> = members
        .iter()
        .filter(|(_, member)| !member.contains('/'))
        .map(|(target, _)| target)
        .collect();

    // Everything compiled into a shared binary: each harness and all it pulls in.
    let mut to_scan: Vec<String> = Vec::new();
    for (target, member) in &members {
        if harnesses.contains(target) {
            to_scan.push(member.clone());
            to_scan.push(target.trim_start_matches("tests/").to_string());
        }
    }
    to_scan.sort();
    to_scan.dedup();

    let mut offenders: Vec<String> = Vec::new();
    for rel in to_scan {
        let Ok(body) = fs::read_to_string(tests_dir.join(&rel)) else {
            continue;
        };
        if mutates_process_env(&body) {
            offenders.push(rel);
        }
    }

    assert!(
        offenders.is_empty(),
        "these files are compiled into a shared test binary but mutate \
         process-global env. Under bare `cargo test` a binary runs its tests as \
         threads in one process, so the mutation leaks into whichever siblings \
         happen to be running — nextest hides this by isolating per test. Give \
         each its own [[test]] target; see docs/contributing/tests.md, \"Kept \
         standalone\": {offenders:?}"
    );
}

#[test]
fn declared_paths_cover_both_declaration_forms() {
    let manifest = r#"
[package]
name = "x"

[[test]]
name = "grp_http"
path = "tests/grp_http.rs"

[[test]]
name = "inferred_path_target"

[[test]]
name = "gated"
path = "tests/gated.rs"
required-features = ["f"]

[[bench]]
name = "not_a_test"
path = "benches/not_a_test.rs"

[lints]
workspace = true
"#;
    let declared = declared_test_paths(manifest);

    assert!(declared.contains("tests/grp_http.rs"));
    // `path` omitted — Cargo infers tests/<name>.rs. fluree-db-api declares two
    // targets this way, so the port depends on this case.
    assert!(declared.contains("tests/inferred_path_target.rs"));
    // Keys after `path` must not spill the target into the next block.
    assert!(declared.contains("tests/gated.rs"));
    // Other target kinds are not [[test]] targets.
    assert!(!declared.contains("benches/not_a_test.rs"));
    assert_eq!(declared.len(), 3);
}
