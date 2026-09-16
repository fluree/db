//! End-to-end coverage for the `--at` / `--from` / `--to` time-spec grammar
//! (fluree/db#1805).
//!
//! Three CLI surfaces took a time spec through a hand-rolled heuristic that
//! recognised a bare integer and a bare ISO-8601 timestamp and swept every
//! other string into a commit-prefix lookup. So `--at t:2` reached the resolver
//! as the literal `"t:2"` and died on "Commit prefix must be at least 6
//! characters, got 3" — and so did `t:latest`, `iso:…`, `recorded:…` and even
//! `commit:…`.
//!
//! A fourth family of surfaces (`branch create --at`, `branch revert`, `show`)
//! parsed the *inverse* grammar: `t:2` worked and a bare `2` was rejected. The
//! tests here assert both spellings on every surface, so closing the inversion
//! on one side cannot silently reopen it on the other.
//!
//! Deliberately kept out of `integration.rs`: that file is large, frequently
//! edited, and this block wants to stay legible as one unit.

use assert_cmd::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

/// A `fluree` invocation pinned to an isolated `HOME` and cwd, so no test sees
/// another's `~/.fluree/`.
fn fluree_cmd(work_dir: &TempDir) -> Command {
    let mut cmd = cargo_bin_cmd!("fluree");
    cmd.current_dir(work_dir.path());
    cmd.env("HOME", work_dir.path());
    cmd.env("NO_COLOR", "1");
    cmd
}

/// A ledger with two commits: `ex:a ex:val "first"` at t=1, `ex:b ex:val
/// "second"` at t=2. Every assertion below distinguishes the two by whether
/// "second" is visible.
fn two_commit_ledger(name: &str) -> TempDir {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", name]).assert().success();
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:a ex:val \"first\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:b ex:val \"second\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));
    tmp
}

const SELECT_VALS: &str = "SELECT ?val WHERE { ?s <http://example.org/val> ?val }";

// ============================================================================
// query --at
// ============================================================================

/// `--at t:1` must return exactly the rows `--at 1` returns. Before the fix the
/// tagged spelling never reached the resolver at all.
#[test]
fn query_at_tagged_t_matches_bare_integer() {
    let tmp = two_commit_ledger("ttq");
    for spec in ["1", "t:1"] {
        fluree_cmd(&tmp)
            .args(["query", "--sparql", "--at", spec, "-e", SELECT_VALS])
            .assert()
            .success()
            .stdout(predicate::str::contains("first"))
            .stdout(predicate::str::contains("second").not());
    }
    // ...and t=2 sees both, so the assertion above is discriminating.
    for spec in ["2", "t:2"] {
        fluree_cmd(&tmp)
            .args(["query", "--sparql", "--at", spec, "-e", SELECT_VALS])
            .assert()
            .success()
            .stdout(predicate::str::contains("first"))
            .stdout(predicate::str::contains("second"));
    }
}

/// `t:latest` and `latest` both mean the ledger head.
#[test]
fn query_at_latest_spellings_resolve_to_head() {
    let tmp = two_commit_ledger("ttlatest");
    for spec in ["latest", "t:latest"] {
        fluree_cmd(&tmp)
            .args(["query", "--sparql", "--at", spec, "-e", SELECT_VALS])
            .assert()
            .success()
            .stdout(predicate::str::contains("first"))
            .stdout(predicate::str::contains("second"));
    }
}

/// `iso:` and `recorded:` must reach the timestamp resolvers rather than the
/// commit-prefix one. A far-past timestamp is used because the resolver's
/// "no data as of" error proves the spec was understood *as a timestamp* —
/// the pre-fix failure was a commit-prefix error instead.
///
/// `recorded:` is the sharper case: the write side already emitted
/// `@recorded:` but no CLI input could produce it.
#[test]
fn query_at_timestamp_axes_reach_the_time_resolver() {
    let tmp = two_commit_ledger("ttiso");
    for spec in [
        "2000-01-01T00:00:00Z",
        "iso:2000-01-01T00:00:00Z",
        "recorded:2000-01-01T00:00:00Z",
    ] {
        fluree_cmd(&tmp)
            .args(["query", "--sparql", "--at", spec, "-e", SELECT_VALS])
            .assert()
            .failure()
            .stderr(predicate::str::contains("no data as of"))
            .stderr(predicate::str::contains("Commit prefix").not());
    }
}

/// A malformed tagged spec is a usage error naming the flag and the accepted
/// spellings — not a commit-prefix lookup for a string containing a colon.
#[test]
fn query_at_malformed_tagged_spec_names_the_accepted_spellings() {
    let tmp = two_commit_ledger("ttbad");
    fluree_cmd(&tmp)
        .args(["query", "--sparql", "--at", "t:abc", "-e", SELECT_VALS])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--at"))
        .stderr(predicate::str::contains("Accepted: t:<N>"))
        .stderr(predicate::str::contains("No commit found with prefix").not());
}

// ============================================================================
// export --at
// ============================================================================

/// The second affected surface. `export` shares `parse_time_spec` with `query`
/// but is reached through its own call site, and needs an indexed ledger.
#[test]
fn export_at_tagged_t_matches_bare_integer() {
    let tmp = two_commit_ledger("ttexp");
    fluree_cmd(&tmp).args(["index", "ttexp"]).assert().success();

    for spec in ["1", "t:1"] {
        fluree_cmd(&tmp)
            .args(["export", "ttexp", "--format", "ntriples", "--at", spec])
            .assert()
            .success()
            .stdout(predicate::str::contains("first"))
            .stdout(predicate::str::contains("second").not());
    }
    for spec in ["2", "t:2"] {
        fluree_cmd(&tmp)
            .args(["export", "ttexp", "--format", "ntriples", "--at", spec])
            .assert()
            .success()
            .stdout(predicate::str::contains("first"))
            .stdout(predicate::str::contains("second"));
    }
}

// ============================================================================
// history --from / --to
// ============================================================================

/// The third affected surface, and the one with its *own* copy of the
/// heuristic — a fix confined to `query.rs` leaves this broken.
#[test]
fn history_range_accepts_tagged_and_bare_spellings() {
    let tmp = two_commit_ledger("tthist");

    // Bare spellings (the pre-fix status quo) and tagged spellings must agree.
    for (from, to) in [("1", "2"), ("t:1", "t:2"), ("1", "t:2"), ("t:1", "latest")] {
        fluree_cmd(&tmp)
            .args([
                "history",
                "http://example.org/b",
                "-l",
                "tthist",
                "--from",
                from,
                "--to",
                to,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("second"));
    }
}

/// `history` names its own flags in the parse error, not `--at`.
#[test]
fn history_malformed_spec_names_its_own_flags() {
    let tmp = two_commit_ledger("tthistbad");
    fluree_cmd(&tmp)
        .args([
            "history",
            "http://example.org/b",
            "-l",
            "tthistbad",
            "--from",
            "t:abc",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--from/--to"))
        .stderr(predicate::str::contains("Accepted: t:<N>"));
}

// ============================================================================
// The inversion: commit-naming surfaces must take the shared spellings too
// ============================================================================

/// `branch create --at 2` and `query --at t:2` in one test, because they are
/// two halves of one defect: before the fix each surface accepted exactly what
/// the other rejected. Fixing only `query` would have moved the inversion
/// rather than closed it.
#[test]
fn branch_create_at_and_query_at_accept_the_same_spellings() {
    let tmp = two_commit_ledger("ttinv");
    fluree_cmd(&tmp).args(["index", "ttinv"]).assert().success();

    // The half that used to be rejected.
    fluree_cmd(&tmp)
        .args(["branch", "create", "b_bare", "--at", "2"])
        .assert()
        .success()
        .stdout(predicate::str::contains("t=2"));
    fluree_cmd(&tmp)
        .args(["query", "--sparql", "--at", "t:2", "-e", SELECT_VALS])
        .assert()
        .success()
        .stdout(predicate::str::contains("second"));

    // The half that always worked, still working.
    fluree_cmd(&tmp)
        .args(["branch", "create", "b_tagged", "--at", "t:2"])
        .assert()
        .success()
        .stdout(predicate::str::contains("t=2"));
    fluree_cmd(&tmp)
        .args(["query", "--sparql", "--at", "2", "-e", SELECT_VALS])
        .assert()
        .success()
        .stdout(predicate::str::contains("second"));

    // Both spellings name the same commit, so both branches sit at t=2.
    for branch in ["b_bare", "b_tagged"] {
        fluree_cmd(&tmp)
            .args(["branch", "create", &format!("{branch}_probe"), "--at", "1"])
            .assert()
            .success()
            .stdout(predicate::str::contains("t=1"));
    }
}

/// `fluree show` named the same commit two ways depending on the spelling, and
/// rejected the bare integer that `query --at` required.
#[test]
fn show_accepts_tagged_and_bare_transaction_numbers() {
    let tmp = two_commit_ledger("ttshow");
    for spec in ["t:2", "2"] {
        fluree_cmd(&tmp)
            .args(["show", spec, "-l", "ttshow"])
            .assert()
            .success()
            .stdout(predicate::str::contains("second"));
    }
}

/// `branch revert` takes commit refs positionally and via `--from`/`--to`; both
/// used to reject bare integers.
#[test]
fn branch_revert_accepts_tagged_and_bare_transaction_numbers() {
    let tmp = two_commit_ledger("ttrev");
    fluree_cmd(&tmp).args(["index", "ttrev"]).assert().success();

    for spec in ["t:2", "2"] {
        fluree_cmd(&tmp)
            .args(["branch", "revert", spec, "--preview"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Would revert"));
    }
    for (from, to) in [("t:1", "t:2"), ("1", "2")] {
        fluree_cmd(&tmp)
            .args(["branch", "revert", "--from", from, "--to", to, "--preview"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Would revert"));
    }
}

// ============================================================================
// history on a non-main branch (#1872)
// ============================================================================

/// `history` used to paste `:main` onto whatever alias it was handed, so a
/// branch-qualified ledger became `hdb:dev:main` — a three-segment id the
/// nameservice rejects. The command failed outright on every branch but
/// `main`, by both routes that can reach one, while `fluree query -l hdb:dev`
/// worked fine on the same ledger.
#[test]
fn history_works_on_a_non_main_branch_by_either_route() {
    let tmp = two_commit_ledger("ttbranch");
    fluree_cmd(&tmp)
        .args(["branch", "create", "dev", "--direct"])
        .assert()
        .success();
    // A commit that exists only on `dev`, so reading `main` by mistake is
    // visible as a missing row rather than passing silently.
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-l",
            "ttbranch:dev",
            "--direct",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:b ex:val \"on-dev-only\" .",
        ])
        .assert()
        .success();

    // Route 1: explicit -l.
    fluree_cmd(&tmp)
        .args([
            "history",
            "http://example.org/b",
            "--direct",
            "-l",
            "ttbranch:dev",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("on-dev-only"));

    // Route 2: the active ledger. `fluree branch` has no checkout subcommand,
    // so `fluree use <name>:<branch>` is the mechanism for selecting a branch.
    fluree_cmd(&tmp)
        .args(["use", "ttbranch:dev"])
        .assert()
        .success();
    fluree_cmd(&tmp)
        .args(["history", "http://example.org/b", "--direct"])
        .assert()
        .success()
        .stdout(predicate::str::contains("on-dev-only"));
}

/// The default case must come out byte-identical — `to_ledger_id` appends
/// `:main` when the alias names no branch, which is what the old hardcoded
/// `:main` was doing for this path.
#[test]
fn history_on_the_default_branch_is_unchanged() {
    let tmp = two_commit_ledger("ttmain");
    for args in [
        vec![
            "history",
            "http://example.org/b",
            "--direct",
            "-l",
            "ttmain",
        ],
        vec![
            "history",
            "http://example.org/b",
            "--direct",
            "-l",
            "ttmain:main",
        ],
    ] {
        fluree_cmd(&tmp)
            .args(&args)
            .assert()
            .success()
            .stdout(predicate::str::contains("second"));
    }
}

// ============================================================================
// Help text — the discoverability half of #1805
// ============================================================================

/// The issue asks for the accepted spellings to be documented where a user
/// looks for them. All four surfaces list the same grammar.
#[test]
fn help_text_lists_the_accepted_spellings() {
    for (args, expect_time_axes) in [
        (vec!["query", "--help"], true),
        (vec!["export", "--help"], true),
        (vec!["history", "--help"], true),
        (vec!["branch", "create", "--help"], false),
    ] {
        let out = cargo_bin_cmd!("fluree")
            .env("NO_COLOR", "1")
            .args(&args)
            .assert()
            .success();
        let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
        assert!(
            stdout.contains("t:<N>"),
            "`fluree {}` must document t:<N>:\n{stdout}",
            args.join(" ")
        );
        assert!(
            stdout.contains("commit:<prefix>"),
            "`fluree {}` must document commit:<prefix>:\n{stdout}",
            args.join(" ")
        );
        if expect_time_axes {
            // `branch create --at` names a commit, so it has no timestamp axes.
            assert!(
                stdout.contains("iso:<ISO-8601>") && stdout.contains("recorded:<ISO-8601>"),
                "`fluree {}` must document the timestamp axes:\n{stdout}",
                args.join(" ")
            );
        }
    }
}
