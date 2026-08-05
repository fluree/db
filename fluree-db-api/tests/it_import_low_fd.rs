//! End-to-end regression: bulk import must succeed — and produce the same
//! ledger — under a low `RLIMIT_NOFILE`.
//!
//! Before FD budgeting, the index build opened 256 class-membership partition
//! writers up front (plus one descriptor per chunk for the SPOT merge and
//! every run file of an order at once for the secondary merges), so any
//! import of typed data died with `EMFILE` at soft limits ≤ 256 — macOS's
//! stock default. This test proves the budgeted build completes under a
//! 96-descriptor limit and matches an unconstrained import bit-for-bit at
//! the query level.
//!
//! Process shape: the test re-executes itself (`current_exe`) in "child"
//! mode, and the child lowers `RLIMIT_NOFILE` — **hard and soft** — to 96
//! before importing. The hard limit must drop too, otherwise the pipeline's
//! own best-effort raise would undo the constraint and the test would prove
//! nothing. A separate process is mandatory: rlimits are process-global and
//! irreversible (hard can only go down), so doing this in the main test
//! process would poison every other test in the target.
//!
//! This is a standalone `[[test]]` target (not bundled into `grp_import`)
//! for exactly that isolation reason.

#![cfg(all(unix, feature = "native"))]

#[path = "support/mod.rs"]
mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use std::io::Write;

const CHILD_ENV: &str = "FLUREE_LOW_FD_CHILD";
const DB_DIR_ENV: &str = "FLUREE_LOW_FD_DB_DIR";
const TTL_ENV: &str = "FLUREE_LOW_FD_TTL";
const RESULT_ENV: &str = "FLUREE_LOW_FD_RESULT";

/// Below the 256 partition writers the unbudgeted scatter opened up front
/// (the test must fail on a build without FD budgeting), with headroom above
/// what the harness + tokio + storage backend themselves need.
const LOW_FD_LIMIT: u64 = 96;

const LEDGER: &str = "test/import-low-fd:main";
const USERS: u64 = 30_000;

/// Typed subjects are required: `rdf:type` + id-stats is what unconditionally
/// triggered the 256-file scatter. Mixed literal/ref properties give the
/// secondary orders non-trivial shape.
fn generate_ttl(path: &std::path::Path) {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).expect("create ttl"));
    writeln!(
        f,
        "@prefix ex: <http://example.org/ns/> .\n@prefix schema: <http://schema.org/> ."
    )
    .expect("write prefix");
    for i in 0..USERS {
        writeln!(
            f,
            "ex:user{i} a ex:User ;\n    schema:name \"User {i}\" ;\n    schema:age {} ;\n    ex:score {} ;\n    ex:friend ex:user{} .",
            i % 90 + 10,
            i * 7 % 1000,
            (i + 1) % USERS,
        )
        .expect("write subject");
    }
    f.flush().expect("flush ttl");
}

fn lower_nofile_limit_hard(limit: u64) {
    let rl = libc::rlimit {
        rlim_cur: limit as libc::rlim_t,
        rlim_max: limit as libc::rlim_t,
    };
    // SAFETY: setrlimit reads the struct we pass; lowering is always allowed.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rl) };
    assert_eq!(
        rc,
        0,
        "setrlimit(NOFILE, {limit}) failed: {}",
        std::io::Error::last_os_error()
    );
}

async fn run_import(db_dir: &std::path::Path, ttl: &std::path::Path) -> (u64, i64) {
    let fluree = FlureeBuilder::file(db_dir.to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let result = fluree
        .create(LEDGER)
        .import(ttl)
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("import should succeed");
    assert!(result.root_id.is_some(), "index should have been built");
    (result.flake_count, result.t)
}

/// Deterministic fingerprint of ledger contents: row count plus the sorted
/// first/last names and the sum of all ages, via a fresh builder on the dir.
async fn fingerprint(db_dir: &std::path::Path) -> (usize, String, String, i64) {
    let fluree = FlureeBuilder::file(db_dir.to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger = fluree.ledger(LEDGER).await.expect("load ledger");
    let query = json!({
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
        "select": ["?name", "?age"],
        "where": { "schema:name": "?name", "schema:age": "?age" }
    });
    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let rows = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let rows = rows.as_array().expect("array result");
    let mut names: Vec<String> = Vec::with_capacity(rows.len());
    let mut age_sum: i64 = 0;
    for row in rows {
        let cols = row.as_array().expect("row array");
        names.push(cols[0].as_str().expect("name string").to_string());
        age_sum += cols[1].as_i64().expect("age i64");
    }
    names.sort();
    (
        names.len(),
        names.first().cloned().unwrap_or_default(),
        names.last().cloned().unwrap_or_default(),
        age_sum,
    )
}

fn child_main() {
    // Constrain BEFORE anything import-related opens files. The tokio::test
    // runtime already holds its descriptors; lowering the limit below the
    // open count only gates *new* opens, which is exactly the contract the
    // import must survive.
    lower_nofile_limit_hard(LOW_FD_LIMIT);

    let db_dir = std::env::var(DB_DIR_ENV).expect("child db dir env");
    let ttl = std::env::var(TTL_ENV).expect("child ttl env");
    let result_path = std::env::var(RESULT_ENV).expect("child result env");

    // Fresh multi-thread runtime built AFTER the constraint, so every
    // descriptor the import path opens is subject to the low limit.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    let (flake_count, t) = runtime.block_on(run_import(
        std::path::Path::new(&db_dir),
        std::path::Path::new(&ttl),
    ));

    // Positive ran-marker: the parent treats a missing/invalid result file
    // as failure, so a child that silently did nothing cannot pass.
    std::fs::write(
        &result_path,
        serde_json::to_vec(&json!({ "flake_count": flake_count, "t": t })).expect("encode"),
    )
    .expect("write result file");
}

/// Same triples as [`generate_ttl`], split across `files` self-contained
/// Turtle files so the directory import produces one chunk per file — enough
/// chunks to push the SPOT merge past a shrunken fan-in cap.
fn generate_ttl_directory(dir: &std::path::Path, files: u64) {
    let per_file = USERS / files;
    for f in 0..files {
        let mut w = std::io::BufWriter::new(
            std::fs::File::create(dir.join(format!("users_{f:03}.ttl"))).expect("create ttl"),
        );
        writeln!(
            w,
            "@prefix ex: <http://example.org/ns/> .\n@prefix schema: <http://schema.org/> ."
        )
        .expect("write prefix");
        for i in (f * per_file)..((f + 1) * per_file) {
            writeln!(
                w,
                "ex:user{i} a ex:User ;\n    schema:name \"User {i}\" ;\n    schema:age {} ;\n    ex:score {} ;\n    ex:friend ex:user{} .",
                i % 90 + 10,
                i * 7 % 1000,
                (i + 1) % USERS,
            )
            .expect("write subject");
        }
        w.flush().expect("flush ttl");
    }
}

/// Spawn this test binary in child mode against `source`, under the real
/// 96-descriptor kernel limit, with scenario-specific extra env. Returns the
/// child's reported flake count (the positive ran-marker: a missing/invalid
/// result file fails the test, so a child that silently did nothing cannot
/// pass).
fn run_child(
    exe: &std::path::Path,
    db_dir: &std::path::Path,
    source: &std::path::Path,
    result_path: &std::path::Path,
    extra_env: &[(&str, &str)],
    scenario: &str,
) -> u64 {
    let mut cmd = std::process::Command::new(exe);
    cmd.args(["import_under_low_fd_limit", "--exact", "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(DB_DIR_ENV, db_dir)
        .env(TTL_ENV, source)
        .env(RESULT_ENV, result_path)
        // Determinism: never inherit budget/compression overrides from the
        // invoking environment; scenarios set what they need explicitly.
        .env_remove("FLUREE_FD_BUDGET")
        .env_remove("FLUREE_RUN_ZSTD")
        .env_remove("FLUREE_RUN_ZSTD_LEVEL")
        .env_remove("FLUREE_IMPORT_COALESCE_THRESHOLD");
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let output = cmd.output().expect("spawn child");
    assert!(
        output.status.success(),
        "constrained child import failed (scenario {scenario}, limit {LOW_FD_LIMIT}).\n\
         --- child stdout ---\n{}\n--- child stderr ---\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let child_result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(result_path).expect("child result file must exist"))
            .expect("child result json");
    child_result["flake_count"]
        .as_u64()
        .expect("flake_count in child result")
}

/// Two constrained scenarios, both under a REAL kernel limit of 96:
///
/// A. Single-file import (one chunk). End-to-end proof that the bounded
///    scatter pool (56 open writers instead of 256) and overall descriptor
///    discipline survive the stock-macOS-sized limit.
/// B. 30-file directory import with `FLUREE_FD_BUDGET=40` shrinking the
///    *plan* (spot_fan_in 24, merge fan-in 12) while the kernel still
///    enforces 96. 30 chunks > 24 forces the hierarchical SPOT merge, and
///    ≥30 run files per order > 12 forces cascaded secondary merges — both
///    fallbacks run against a real limit, not a synthetic plan.
///
/// Equality is asserted via flake counts and a content fingerprint (row
/// count, sorted first/last names, age sum) — not `root_id`, which embeds
/// commit CIDs with timestamps and is not stable across separate imports.
/// Byte-level identity of the cascade/hierarchical outputs is pinned by the
/// leaf-CID-equality unit tests in fluree-db-indexer.
#[tokio::test]
async fn import_under_low_fd_limit() {
    if std::env::var(CHILD_ENV).is_ok() {
        // Already inside the constrained re-exec; tokio::test's runtime is
        // irrelevant here — the child builds its own after the setrlimit.
        tokio::task::spawn_blocking(child_main)
            .await
            .expect("child mode");
        return;
    }

    let data_dir = tempfile::tempdir().expect("data tmpdir");
    let exe = std::env::current_exe().expect("current exe");

    // --- Scenario A: single file ---
    let ttl_path = data_dir.path().join("users.ttl");
    generate_ttl(&ttl_path);

    let baseline_db = tempfile::tempdir().expect("baseline db tmpdir");
    let (baseline_flakes, _t) = run_import(baseline_db.path(), &ttl_path).await;
    let baseline_print = fingerprint(baseline_db.path()).await;
    assert_eq!(baseline_print.0 as u64, USERS);

    let child_db = tempfile::tempdir().expect("child db tmpdir");
    let child_flakes = run_child(
        &exe,
        child_db.path(),
        &ttl_path,
        &data_dir.path().join("child_a.json"),
        &[],
        "A/single-file",
    );
    assert_eq!(child_flakes, baseline_flakes);
    assert_eq!(fingerprint(child_db.path()).await, baseline_print);

    // --- Scenario B: 30-chunk directory, shrunken plan ---
    let ttl_dir = data_dir.path().join("chunks");
    std::fs::create_dir_all(&ttl_dir).expect("chunks dir");
    generate_ttl_directory(&ttl_dir, 30);

    let dir_baseline_db = tempfile::tempdir().expect("dir baseline tmpdir");
    let (dir_baseline_flakes, _t) = run_import(dir_baseline_db.path(), &ttl_dir).await;
    let dir_baseline_print = fingerprint(dir_baseline_db.path()).await;
    assert_eq!(dir_baseline_print.0 as u64, USERS);

    let dir_child_db = tempfile::tempdir().expect("dir child tmpdir");
    let dir_child_flakes = run_child(
        &exe,
        dir_child_db.path(),
        &ttl_dir,
        &data_dir.path().join("child_b.json"),
        &[
            // Shrink the PLAN below the chunk/run counts so the hierarchical
            // SPOT merge and the cascade both fire; the kernel limit stays 96.
            ("FLUREE_FD_BUDGET", "40"),
            // One chunk per file: don't let the producer coalesce the small
            // files back into few chunks.
            ("FLUREE_IMPORT_COALESCE_THRESHOLD", "0"),
        ],
        "B/directory-cascades",
    );
    assert_eq!(dir_child_flakes, dir_baseline_flakes);
    assert_eq!(fingerprint(dir_child_db.path()).await, dir_baseline_print);
}
