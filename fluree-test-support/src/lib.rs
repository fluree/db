//! Guards for crates that group their integration tests into shared harnesses.
//!
//! A crate with `autotests = false` declares its `[[test]]` targets explicitly,
//! so a `tests/*.rs` becomes a test binary only if it is named in a `[[test]]`
//! block or pulled into a harness via `#[path]`. That buys far fewer link steps
//! and costs two invariants, neither of which the compiler checks:
//!
//! 1. **Reachability** — a file that is neither declared nor pulled in is never
//!    compiled and never run, and `cargo test` still reports success.
//! 2. **Isolation** — a harness runs its tests as threads in one process under
//!    bare `cargo test`, so a file that mutates process-global env leaks into
//!    whichever siblings happen to be running. `cargo nextest` gives every test
//!    its own process and hides this entirely, so CI can be green while
//!    `cargo test` fails.
//!
//! Call the asserts from a test in each adopting crate, passing that crate's
//! own manifest directory:
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

#![forbid(unsafe_code)]

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

/// Files pulled in via `#[path = "..."]` by a *declared* target, paired with the
/// target that pulls them in. Harvesting from every file on disk instead would
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

/// Call shapes for process-global env mutation.
///
/// Assembled with `concat!` so this file does not contain the literals it
/// searches for — adopting crates pull the guard in as an ordinary case file,
/// and a plain literal would make it flag itself.
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
    let referenced: HashSet<String> = grouped_members(root, &declared)
        .into_iter()
        .map(|(_target, member)| member)
        .collect();

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

/// Nothing compiled into a shared harness may mutate process-global env.
///
/// A target counts as a harness only when it pulls in a sibling top-level case
/// file. Standalone targets use `#[path]` too, but only to reach shared helpers
/// such as `support/span_capture.rs`; they are still alone in their process,
/// which is precisely what makes mutating env legitimate for them.
///
/// # Panics
///
/// Panics listing every offending file, or if the crate's `Cargo.toml` cannot
/// be read.
pub fn assert_grouped_tests_do_not_mutate_env(manifest_dir: &str) {
    let root = Path::new(manifest_dir);
    let tests_dir = root.join("tests");
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");

    let declared = declared_test_paths(&manifest);
    let members = grouped_members(root, &declared);

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

#[cfg(test)]
mod tests {
    use super::*;

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
        // `path` omitted — Cargo infers tests/<name>.rs. fluree-db-api declares
        // two targets this way.
        assert!(declared.contains("tests/inferred_path_target.rs"));
        // Keys after `path` must not spill the target into the next block.
        assert!(declared.contains("tests/gated.rs"));
        // Other target kinds are not [[test]] targets.
        assert!(!declared.contains("benches/not_a_test.rs"));
        assert_eq!(declared.len(), 3);
    }

    #[test]
    fn env_mutation_matches_calls_not_identifiers() {
        assert!(mutates_process_env(r#"std::env::set_var("A", "1");"#));
        assert!(mutates_process_env(r#"env::remove_var("A");"#));
        assert!(mutates_process_env("set_var(\"A\", \"1\");"));
        // An identifier that merely ends in the needle is not a call to it.
        assert!(!mutates_process_env(r#"unset_var("A");"#));
        assert!(!mutates_process_env(r#"my_remove_var("A");"#));
        // Prose mentioning the name without calling it does not count.
        assert!(!mutates_process_env("// this file avoids set_var entirely"));
    }
}
