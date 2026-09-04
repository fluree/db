//! Reachability guard for crates that group their integration tests.
//!
//! A crate with `autotests = false` declares its `[[test]]` targets explicitly,
//! so a `tests/*.rs` is compiled only if it is named in a `[[test]]` block or
//! pulled into a declared binary as a module. That buys far fewer link steps
//! and costs one invariant the compiler does not check: a file that is neither
//! declared nor pulled in is never compiled and never run, and `cargo test`
//! still reports success, because from Cargo's point of view there is nothing
//! to build.
//!
//! The guard follows exactly two module forms from the declared targets,
//! transitively: `#[path = "<file>"] mod <name>;` and plain `mod <name>;`,
//! each at the start of a line. A file included any other way — a `#[path]`
//! built by a macro, say — compiles fine but is reported as an orphan, so the
//! guard errs loud rather than silent; wire such a file in with `#[path]`.
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

/// `path` of every `[[test]]` target, relative to the crate root and
/// normalised so `./tests/x.rs` and `tests/x.rs` are the same target. A target
/// may omit `path`, in which case Cargo infers `tests/<name>.rs`.
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
        .map(|path| normalize(Path::new(&path)))
        .collect()
}

/// Files reachable from the *declared* targets, as paths relative to the crate
/// root, following both `#[path = "..."] mod x;` and plain `mod x;` through
/// every file reached, so a harness that pulls in `mid.rs` which in turn
/// pulls in `leaf.rs` credits both.
///
/// Harvesting from every file on disk instead would let an undeclared harness
/// launder its members into the covered set, so the walk starts from the
/// manifest and only ever reads files it has already reached.
fn reachable_from_declared_targets(root: &Path, declared: &HashSet<String>) -> HashSet<String> {
    let mut reached: HashSet<String> = HashSet::new();
    let mut queue: Vec<(String, bool)> = declared.iter().map(|p| (p.clone(), true)).collect();
    while let Some((rel, is_crate_root)) = queue.pop() {
        if !reached.insert(rel.clone()) {
            continue;
        }
        let Ok(src) = fs::read_to_string(root.join(&rel)) else {
            continue; // a declared path that does not exist is Cargo's error to report
        };
        for member in module_files(&rel, is_crate_root, &src) {
            queue.push((member, false));
        }
    }
    reached
}

/// The files that `mod` declarations in `src` (located at `file`, relative to
/// the crate root) resolve to, following rustc's rules:
///
/// - `#[path = "p"] mod x;` is `p` relative to the directory containing `file`;
/// - plain `mod x;` is `x.rs` beside `file` when `file` is a crate root or a
///   `mod.rs`, and `<stem>/x.rs` below it otherwise. (The `x/mod.rs` spelling
///   lives in a subdirectory either way, so it never names a top-level file
///   and is not modelled.)
///
/// Only line-anchored forms are recognised: the attribute at the start of a
/// line, with or without spaces around `=`, followed by the `mod` declaration
/// on the same or a later line, so a `#[path]` inside a comment is never
/// credited.
fn module_files(file: &str, is_crate_root: bool, src: &str) -> Vec<String> {
    let file = Path::new(file);
    let dir = file.parent().unwrap_or_else(|| Path::new(""));
    let flat = is_crate_root || file.file_name().and_then(|n| n.to_str()) == Some("mod.rs");
    let plain_base = if flat {
        dir.to_path_buf()
    } else {
        dir.join(file.file_stem().unwrap_or_default())
    };

    let mut found = Vec::new();
    let mut pending_path: Option<String> = None;
    for line in src.lines() {
        let mut line = line.trim();
        if let Some(rest) = line.strip_prefix("#[path") {
            let value = rest
                .trim_start()
                .strip_prefix('=')
                .map(str::trim_start)
                .and_then(|r| r.strip_prefix('"'))
                .and_then(|r| r.split_once('"'));
            let Some((value, after)) = value else {
                continue;
            };
            pending_path = Some(value.to_string());
            // `#[path = "x.rs"] mod x;` on one line: fall through to the
            // declaration; otherwise the attribute stands alone.
            match after.strip_prefix(']').map(str::trim_start) {
                Some(same_line) if !same_line.is_empty() => line = same_line,
                _ => continue,
            }
        }
        if line.starts_with("#[") || line.starts_with("#!") {
            continue; // other attributes between #[path] and its mod
        }
        let Some(name) = mod_declaration_name(line) else {
            pending_path = None;
            continue;
        };
        let resolved = match pending_path.take() {
            Some(p) => dir.join(p),
            None => plain_base.join(format!("{name}.rs")),
        };
        found.push(normalize(&resolved));
    }
    found
}

/// `x` for a line of the form `[pub[(...)]] mod x;`, else `None`.
fn mod_declaration_name(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("pub").map_or(line, |r| {
        let r = r.trim_start();
        r.strip_prefix('(')
            .and_then(|r| r.split_once(')'))
            .map_or(r, |(_, after)| after.trim_start())
    });
    let name = rest.strip_prefix("mod ")?.trim().strip_suffix(';')?.trim();
    (!name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')).then_some(name)
}

/// Lexically resolve `.` and `..` so `tests/./a/../b.rs` and `tests/b.rs`
/// compare equal. Forward slashes throughout, since that is how paths are
/// written in `Cargo.toml` and `#[path]`.
fn normalize(path: &Path) -> String {
    use std::path::Component;
    let mut parts: Vec<String> = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                parts.pop();
            }
            other => parts.push(other.as_os_str().to_string_lossy().into_owned()),
        }
    }
    parts.join("/")
}

/// Every top-level `tests/*.rs` must be declared as a `[[test]]` target or
/// reachable from one through `#[path = "..."] mod x;` or plain `mod x;`
/// lines (see the crate docs for what is and is not recognised).
///
/// Reachability is derived from `Cargo.toml` outwards, never from file names:
/// an undeclared `tests/grp_foo.rs` is itself an orphan, and the files it
/// references are not credited as covered on its say-so.
///
/// # Panics
///
/// Panics listing every unreachable file, or if the crate's `Cargo.toml`
/// cannot be read. A crate with no `tests/` directory yet has nothing to
/// reach and passes.
pub fn assert_every_test_file_is_reachable(manifest_dir: &str) {
    let root = Path::new(manifest_dir);
    let tests_dir = root.join("tests");
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");

    let declared = declared_test_paths(&manifest);
    let reachable = reachable_from_declared_targets(root, &declared);

    // Only top-level files can become test binaries, so only they are
    // enumerated. Files in subdirectories such as tests/support/ are modules
    // of whichever binary pulls them in — via `#[path = "support/mod.rs"]`
    // from a harness, or a bare `mod support;` from a standalone crate root —
    // and cannot be orphaned in the sense this guard checks.
    let entries = match fs::read_dir(&tests_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => panic!("read {}: {e}", tests_dir.display()),
    };
    let mut orphans: Vec<String> = Vec::new();
    for entry in entries {
        let path = entry.expect("tests/ entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("utf-8 filename")
            .to_string();

        if reachable.contains(&format!("tests/{name}")) {
            continue;
        }
        orphans.push(name);
    }

    orphans.sort();
    assert!(
        orphans.is_empty(),
        "unreachable under `autotests = false`: no [[test]] target declares \
         these files and none reaches them through a `#[path = \"<file>\"] \
         mod <name>;` or plain `mod <name>;` line. Those two forms, at the \
         start of a line, are the only ones the guard follows — a file pulled \
         in some other way is a false orphan; wire it in with `#[path]` \
         instead. Orphaned: {orphans:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A throwaway crate tree under the OS temp dir: `files` are
    /// `(path relative to the crate root, contents)`.
    struct Crate {
        root: PathBuf,
    }

    impl Crate {
        fn new(files: &[(&str, &str)]) -> Self {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "fluree-test-support-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            for (rel, contents) in files {
                let path = root.join(rel);
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(path, contents).unwrap();
            }
            Self { root }
        }

        /// The guard's panic message, or `None` when it passes.
        fn failure(&self) -> Option<String> {
            let dir = self.root.to_str().unwrap().to_owned();
            std::panic::catch_unwind(|| assert_every_test_file_is_reachable(&dir))
                .err()
                .map(|payload| {
                    payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
                        .unwrap_or_default()
                })
        }
    }

    impl Drop for Crate {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    const ONE_HARNESS: &str = "[[test]]\nname = \"grp\"\npath = \"tests/grp.rs\"\n";

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

    /// The manifest shapes the line-scanning predecessor got wrong, or never
    /// pinned, now handled by a real TOML parser. One row per shape.
    #[test]
    fn declared_targets_survive_every_manifest_shape() {
        let cases: &[(&str, &str, &[&str])] = &[
            (
                "a comment on the [[test]] header",
                "[[test]] # standalone: mutates env\nname = \"a\"\npath = \"tests/a.rs\"\n",
                &["tests/a.rs"],
            ),
            (
                "single-quoted values",
                "[[test]]\nname = 'a'\npath = 'tests/a.rs'\n",
                &["tests/a.rs"],
            ),
            (
                "an inline table instead of an array-of-tables header",
                "test = [{ name = \"a\", path = \"tests/a.rs\" }, { name = \"b\" }]\n",
                &["tests/a.rs", "tests/b.rs"],
            ),
            (
                "non-canonical spacing around `=`",
                "[[test]]\nname=\"a\"\npath  =  \"tests/a.rs\"\n",
                &["tests/a.rs"],
            ),
            (
                "keys after `path` do not spill the target into the next block",
                "[[test]]\nname = \"gated\"\npath = \"tests/gated.rs\"\n\
                 required-features = [\"f\"]\n\n\
                 [[test]]\nname = \"next\"\npath = \"tests/next.rs\"\n",
                &["tests/gated.rs", "tests/next.rs"],
            ),
            (
                "keys before `path`",
                "[[test]]\nrequired-features = [\"f\"]\nname = \"a\"\npath = \"tests/a.rs\"\n",
                &["tests/a.rs"],
            ),
            (
                "a [[test]] block among other sections",
                "[package]\nname = \"x\"\n\n[[bin]]\nname = \"x\"\n\n\
                 [[test]]\nname = \"a\"\n\n[[bench]]\nname = \"b\"\n\n[lints]\nworkspace = true\n",
                &["tests/a.rs"],
            ),
        ];
        for (shape, manifest, expected) in cases {
            let expected: HashSet<String> = expected.iter().map(ToString::to_string).collect();
            assert_eq!(declared_test_paths(manifest), expected, "{shape}");
        }
    }

    #[test]
    fn a_declared_path_is_normalised() {
        let declared = declared_test_paths(
            "[[test]]\nname = \"a\"\npath = \"./tests/a.rs\"\n\
             [[test]]\nname = \"b\"\npath = \"tests/sub/../b.rs\"\n",
        );
        assert_eq!(
            declared,
            HashSet::from(["tests/a.rs".to_string(), "tests/b.rs".to_string()])
        );
    }

    #[test]
    fn a_crate_without_a_tests_directory_has_no_orphans() {
        let krate = Crate::new(&[("Cargo.toml", "[package]\nname = \"x\"\n")]);
        assert_eq!(krate.failure(), None);
    }

    #[test]
    fn every_way_a_declared_target_can_pull_a_file_in_is_credited() {
        let krate = Crate::new(&[
            ("Cargo.toml", ONE_HARNESS),
            (
                "tests/grp.rs",
                "#[path = \"spaced.rs\"]\nmod spaced;\n\
                 #[path=\"unspaced.rs\"]\nmod unspaced;\n\
                 #[path = \"same_line.rs\"] mod same_line;\n\
                 #[path = \"./dotted.rs\"]\nmod dotted;\n\
                 #[cfg(feature = \"x\")]\n#[path = \"gated.rs\"]\nmod gated;\n\
                 mod plain;\npub mod public;\npub(crate) mod scoped;\n\
                 #[path = \"support/mod.rs\"]\nmod support;\n",
            ),
            ("tests/spaced.rs", ""),
            ("tests/unspaced.rs", ""),
            ("tests/same_line.rs", ""),
            ("tests/dotted.rs", ""),
            ("tests/gated.rs", ""),
            ("tests/plain.rs", ""),
            ("tests/public.rs", ""),
            ("tests/scoped.rs", ""),
            ("tests/support/mod.rs", "mod helper;\n"),
            ("tests/support/helper.rs", ""),
        ]);
        assert_eq!(krate.failure(), None);
    }

    #[test]
    fn a_member_pulled_in_transitively_is_credited() {
        let krate = Crate::new(&[
            ("Cargo.toml", ONE_HARNESS),
            ("tests/grp.rs", "#[path = \"mid.rs\"]\nmod mid;\n"),
            ("tests/mid.rs", "#[path = \"leaf.rs\"]\nmod leaf;\n"),
            ("tests/leaf.rs", ""),
        ]);
        assert_eq!(krate.failure(), None);
    }

    #[test]
    fn a_plain_mod_in_a_member_resolves_below_it_not_beside_it() {
        // rustc resolves `mod x;` in tests/mid.rs to tests/mid/x.rs, so a
        // top-level tests/x.rs is *not* what it compiles.
        let krate = Crate::new(&[
            ("Cargo.toml", ONE_HARNESS),
            ("tests/grp.rs", "#[path = \"mid.rs\"]\nmod mid;\n"),
            ("tests/mid.rs", "mod x;\n"),
            ("tests/mid/x.rs", ""),
            ("tests/x.rs", ""),
        ]);
        let msg = krate.failure().expect("tests/x.rs is unreachable");
        assert!(msg.contains("[\"x.rs\"]"), "{msg}");
    }

    #[test]
    fn an_undeclared_harness_does_not_launder_its_members() {
        let krate = Crate::new(&[
            ("Cargo.toml", ONE_HARNESS),
            ("tests/grp.rs", ""),
            (
                "tests/grp_rogue.rs",
                "#[path = \"member.rs\"]\nmod member;\n",
            ),
            ("tests/member.rs", ""),
        ]);
        let msg = krate.failure().expect("both files are unreachable");
        assert!(msg.contains("[\"grp_rogue.rs\", \"member.rs\"]"), "{msg}");
    }

    #[test]
    fn a_path_attribute_in_a_comment_is_not_credited() {
        let krate = Crate::new(&[
            ("Cargo.toml", ONE_HARNESS),
            (
                "tests/grp.rs",
                "//! #[path = \"doc.rs\"] mod doc;\n// #[path = \"line.rs\"]\n// mod line;\n",
            ),
            ("tests/doc.rs", ""),
            ("tests/line.rs", ""),
        ]);
        let msg = krate.failure().expect("both files are unreachable");
        assert!(msg.contains("[\"doc.rs\", \"line.rs\"]"), "{msg}");
    }
}
