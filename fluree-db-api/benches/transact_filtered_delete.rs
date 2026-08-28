//! Filtered-DELETE staging latency over a novelty-heavy ledger.
//!
//! `WHERE { ?s ex:tag ?o } DELETE { ?s ex:tag ?o }` on a bulk-imported
//! ledger that has since accumulated novelty. Nothing else in the suite
//! exercises WHERE/DELETE — `transact_commit.rs` measures insert-shaped
//! commits only — which is how a `retractions × novelty` shape in
//! `hydrate_list_index_meta_for_retractions` went unnoticed until a
//! 40k-subject delete stopped finishing altogether.
//!
//! ## What the two scenarios separate
//!
//! Staging skips list-position hydration entirely when the index root AND
//! novelty both report no `@list` rows (`fluree-db-transact/src/stage.rs`,
//! the `has_list_meta == Some(false) && !novelty.has_list_meta` gate).
//! The two sides of that gate have very different cost profiles — 0.9 s
//! vs 3.1 s on the same 40.5k-retraction delete when the fix landed — so
//! each gets its own scenario:
//!
//! 1. `no_lists` — the import carries no RDF collection, so the root
//!    records `Some(false)` and novelty stays clean: hydration is skipped.
//!    (Bulk-import roots record the flag exactly; before that they were
//!    left untracked and this branch was unreachable without a full
//!    rebuild in the fixture.)
//! 2. `with_lists` — the import carries one collection, so the root
//!    records `Some(true)` and every retraction group pays the one-SPOT
//!    overlay walk plus a base-only seek. The deleted predicate is the
//!    same plain `ex:tag` in both, so the delete itself is identical and
//!    the delta is the hydration path.
//!
//! Both scenarios assert the gate state they mean to measure during
//! setup: a fixture that quietly stops covering its branch fails the
//! bench instead of reporting a number for the wrong path.
//!
//! The two curves separate from Medium up — 69 ms vs 127 ms at
//! 20k groups on the author's box, where at Small both read ~39 ms
//! because the WHERE and the commit dominate. Read a flat Tiny/Small
//! pair as "the fixture is below the separation point", not as "the
//! gate is free". Either scale still catches what this bench exists
//! for: the shape it replaced was `retractions × novelty`, so a
//! regression back to it turns tens of milliseconds into seconds.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → (imported_subjects, novelty_subjects)
//!              (Tiny=500×500, Small=2k×2k, Medium=10k×10k, Large=40k×40k)
//!              Every subject contributes one retraction group, so the
//!              measured delete retracts base+novelty facts. Large mirrors
//!              the 40.5k-retraction shape the original bug was found on.
//!   metric:    ns/delete (criterion default; no Throughput — the guard is
//!              on the superlinear curve, and a per-flake rate would hide
//!              it behind a growing denominator)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench transact_filtered_delete
//!   cargo bench -p fluree-db-api --bench transact_filtered_delete -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench transact_filtered_delete
//!
//! ## Cargo.toml + budget already wired (see fluree-db-api/Cargo.toml,
//! regression-budget.json).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, LedgerState, TxnOpts};
use serde_json::json;
use std::io::Write;

/// Map BenchScale to (imported subjects, novelty subjects).
fn scale_inputs(scale: BenchScale) -> (usize, usize) {
    match scale {
        // Keep tiny tiny so PR-gated runs finish quickly.
        BenchScale::Tiny => (500, 500),
        BenchScale::Small => (2_000, 2_000),
        BenchScale::Medium => (10_000, 10_000),
        // The shape the original >20-min delete was found on.
        BenchScale::Large => (40_000, 40_000),
    }
}

const CTX_PREFIX: &str = "http://example.org/ns/";

/// Novelty must survive the whole fixture: any reindex would fold it into
/// the base and erase the shape this bench exists to measure.
fn index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1 << 40,
        reindex_max_bytes: 1 << 41,
    }
}

/// Turtle for the imported base: `n` subjects each carrying the deleted
/// predicate plus an untouched one. `with_list` appends a single subject
/// with an RDF collection — enough to flip the root's `has_list_meta`
/// without changing what the measured delete retracts.
fn base_turtle(n: usize, with_list: bool) -> String {
    let mut out = String::with_capacity(n * 64 + 128);
    out.push_str("@prefix ex: <");
    out.push_str(CTX_PREFIX);
    out.push_str("> .\n");
    for i in 0..n {
        out.push_str(&format!("ex:b{i} ex:tag 'keep' ; ex:name 'base{i}' .\n"));
    }
    if with_list {
        out.push_str("ex:listed ex:items ( 'a' 'b' 'c' ) .\n");
    }
    out
}

/// One novelty insert batch as JSON-LD: subjects `[from, to)` carrying the
/// same deleted predicate, so the delete's WHERE spans base and novelty.
fn novelty_batch(from: usize, to: usize) -> serde_json::Value {
    let graph: Vec<_> = (from..to)
        .map(|i| json!({"@id": format!("ex:n{i}"), "ex:tag": "keep", "ex:name": format!("nov{i}")}))
        .collect();
    json!({"@context": {"ex": CTX_PREFIX}, "@graph": graph})
}

/// The measured transaction: retract every `ex:tag` fact in the ledger.
fn delete_txn() -> serde_json::Value {
    json!({
        "@context": {"ex": CTX_PREFIX},
        "where": [{"@id": "?s", "ex:tag": "?o"}],
        "delete": [{"@id": "?s", "ex:tag": "?o"}]
    })
}

/// Everything one measured iteration consumes. The tempdirs ride along so
/// the file-backed store outlives the timed block.
struct Fixture {
    _db_dir: tempfile::TempDir,
    _data_dir: tempfile::TempDir,
    fluree: fluree_db_api::Fluree,
    ledger: LedgerState,
}

/// Bulk-import the base, then pile `novelty_subjects` on top without
/// letting the indexer fold them in.
///
/// Asserts the `has_list_meta` gate state the caller expects: this is the
/// only thing separating the two scenarios, and a fixture that stopped
/// producing it would report a plausible number for the wrong code path.
fn build_fixture(
    rt: &tokio::runtime::Runtime,
    base_turtle: &str,
    novelty_subjects: usize,
    expect_list_meta: bool,
) -> Fixture {
    rt.block_on(async {
        let db_dir = tempfile::tempdir().expect("db tmpdir");
        let data_dir = tempfile::tempdir().expect("data tmpdir");
        let ttl_path = data_dir.path().join("base.ttl");
        let mut f = std::fs::File::create(&ttl_path).expect("create ttl");
        f.write_all(base_turtle.as_bytes()).expect("write ttl");
        drop(f);

        let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
            .build()
            .expect("build file-backed Fluree");
        let alias = next_ledger_alias("tfd");
        let result = fluree
            .create(&alias)
            .import(&ttl_path)
            .cleanup(false)
            .execute()
            .await
            .expect("bulk import");
        assert!(result.root_id.is_some(), "fixture needs an indexed base");

        let mut ledger = fluree.ledger(&alias).await.expect("load imported ledger");
        assert_eq!(
            ledger.snapshot.has_list_meta,
            Some(expect_list_meta),
            "fixture must land on the intended side of the hydration-skip gate"
        );

        // Novelty in batches — one commit per batch, all retained.
        let batch = 500usize;
        let mut from = 0usize;
        while from < novelty_subjects {
            let to = (from + batch).min(novelty_subjects);
            let r = fluree
                .insert_with_opts(
                    ledger,
                    &novelty_batch(from, to),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_config(),
                )
                .await
                .expect("novelty insert");
            ledger = r.ledger;
            from = to;
        }
        // Novelty is list-free in BOTH scenarios — the burst carries plain
        // values only — so the root is the whole difference between them.
        // With a dirty novelty bit the gate would fail closed either way and
        // the two scenarios would measure the same path.
        assert!(
            !ledger.novelty.has_list_meta,
            "novelty must stay list-free so the root alone decides the gate"
        );

        Fixture {
            _db_dir: db_dir,
            _data_dir: data_dir,
            fluree,
            ledger,
        }
    })
}

fn bench_transact_filtered_delete(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let (base_subjects, novelty_subjects) = scale_inputs(scale);

    let plain_ttl = base_turtle(base_subjects, false);
    let listed_ttl = base_turtle(base_subjects, true);
    let del = delete_txn();

    eprintln!(
        "  [transact_filtered_delete] scale={} base={} novelty={} groups~={}",
        scale.as_str(),
        base_subjects,
        novelty_subjects,
        base_subjects + novelty_subjects
    );

    let mut group = c.benchmark_group("transact_filtered_delete");
    group.sample_size(profile.sample_size());
    // The fixture (import + novelty burst) is rebuilt per iteration because
    // the measured op is destructive; Flat keeps criterion from assuming a
    // cheap, repeatable routine.
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, ttl, has_lists) in [
        ("no_lists", &plain_ttl, false),
        ("with_lists", &listed_ttl, true),
    ] {
        group.bench_with_input(
            BenchmarkId::new(name, scale.as_str()),
            &(base_subjects, novelty_subjects),
            |b, _| {
                b.iter_batched(
                    // Setup: import + novelty. NOT measured.
                    || build_fixture(&rt, ttl, novelty_subjects, has_lists),
                    // Measured: one filtered DELETE over every group.
                    |fixture| {
                        rt.block_on(async {
                            let result = fixture
                                .fluree
                                .update_with_opts(
                                    fixture.ledger,
                                    &del,
                                    TxnOpts::default(),
                                    CommitOpts::default(),
                                    &index_config(),
                                )
                                .await
                                .expect("filtered delete");
                            black_box(result.ledger);
                        });
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_transact_filtered_delete);
criterion_main!(benches);
