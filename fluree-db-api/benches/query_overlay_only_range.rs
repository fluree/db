//! Read latency on the overlay-only range path.
//!
//! `binary_range_eq_v3` serves a pattern straight from the overlay whenever a
//! bound component has no persisted id — a novelty-only subject, predicate, or
//! object — or the requested order has no branch manifest. That path used to
//! filter its walk to asserts, which silently resurrected any fact whose
//! assert and retraction both lived in the novelty window (#1683). It now keeps
//! both ops and lifecycle-resolves the matched set, which costs a clone per
//! matched flake and a per-run resolution instead of an early exit.
//!
//! This bench measures that path. The subject crawl is the shape #1683 was
//! reported on: `select {"?s": ["ex:tag"]}` binds subject and predicate, the
//! predicate has no persisted id, and the range provider hands the pattern to
//! the overlay-only path.
//!
//! ## Scenarios
//!
//! 1. `narrow_subject` — crawl a subject holding ONE value under the
//!    novelty-only predicate, over a ledger carrying `novelty_subjects` rows
//!    for that predicate. The matched set is one fact; what scales with
//!    novelty is the walk, which this path always paid.
//! 2. `wide_subject` — crawl a subject holding `wide_values` values under the
//!    same predicate with half of them retracted. This is where the change
//!    actually shows: the matched set is what gets cloned and resolved, and
//!    the retracted half is work the old assert-only filter skipped (by
//!    getting the answer wrong).
//!
//! The two together separate "cost that scales with the overlay" from "cost
//! that scales with the matched fact set", which is the distinction that
//! matters for whether the removed `limit` early exit needs restoring.
//!
//! What they said on the author's box (quick profile):
//!
//! ```text
//!   narrow_subject  small (2k novelty)   13.6 µs
//!   narrow_subject  medium (10k novelty) 13.8 µs
//!   wide_subject    small (200 values)   31.8 µs
//!   wide_subject    medium (1k values)   95.9 µs
//! ```
//!
//! `narrow_subject` is flat across a 5× increase in overlay size, while
//! `wide_subject` tracks the matched set at roughly 80 ns per matched row.
//! So the clone-and-resolve cost this path took on is bounded by the matched
//! fact set, not by novelty — which is why the `limit` early exit was not
//! worth restoring at the price of depending on the walk order with no sort
//! to fall back on.
//!
//! ## The shape neither scenario models
//!
//! Both scenarios bind a subject, which is what keeps their matched set
//! small. One caller does not: `probe_timestamp_axis`
//! (`fluree-db-api/src/time_resolve.rs:154`, the `@iso:`/timestamp
//! time-travel probe) issues `RangeMatch::predicate(..)` — predicate only, no
//! subject, no object — with `flake_limit(1)` against the txn-meta graph, and
//! adds `object_bounds` when resolving `after: Some`. Its matched set is one
//! flake per commit, so it scales with commits-in-novelty rather than with a
//! bound subject's fact count, and it is doubly exposed: those object bounds
//! now prune AFTER resolution instead of inside the walk.
//!
//! It stays off this bench deliberately — it only takes the overlay-only path
//! when the txn-meta graph has no POST branch, it reads a system graph, and at
//! ~80 ns per matched row the absolute cost is small. But it is the site to
//! measure first if this path ever needs the bound back, and it is the
//! counter-example to "every limit-setting caller binds a subject", which is
//! what an earlier version of this reasoning claimed.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → (novelty_subjects, wide_values)
//!              (Tiny=500×50, Small=2k×200, Medium=10k×1k, Large=40k×4k)
//!   metric:    ns/query (criterion default; no Throughput — the interesting
//!              comparison is between the two scenarios at a fixed scale)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_overlay_only_range
//!   cargo bench -p fluree-db-api --bench query_overlay_only_range -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_overlay_only_range
//!
//! ## Cargo.toml + budget already wired (see fluree-db-api/Cargo.toml,
//! regression-budget.json).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, ReindexOptions, TxnOpts};
use serde_json::json;

/// Map BenchScale to (novelty subjects, values on the wide subject).
fn scale_inputs(scale: BenchScale) -> (usize, usize) {
    match scale {
        BenchScale::Tiny => (500, 50),
        BenchScale::Small => (2_000, 200),
        BenchScale::Medium => (10_000, 1_000),
        BenchScale::Large => (40_000, 4_000),
    }
}

const CTX_PREFIX: &str = "http://example.org/ns/";

/// Novelty must survive the fixture — a reindex would fold it into the base
/// and route the crawl through the cursor instead of the path under test.
fn index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1 << 40,
        reindex_max_bytes: 1 << 41,
    }
}

fn ctx() -> serde_json::Value {
    json!({"ex": CTX_PREFIX})
}

/// Crawl one subject's novelty-only predicate.
fn crawl(subject: &str) -> serde_json::Value {
    json!({
        "@context": ctx(),
        "select": {"?s": ["ex:tag"]},
        "where": {"@id": "?s"},
        "values": ["?s", [{"@id": subject}]]
    })
}

/// Indexed base with no `ex:tag` anywhere, then a novelty burst that
/// introduces it. `ex:wide` holds `wide_values` values with half retracted;
/// `ex:narrow` holds one.
async fn setup(novelty_subjects: usize, wide_values: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("overlay-only-range");
    let mut ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    // Base: `ex:name` only, so `ex:tag` never reaches the persisted
    // predicate dictionary and every crawl below takes the overlay-only path.
    let base: Vec<_> = (0..200)
        .map(|n| json!({"@id": format!("ex:b{n}"), "ex:name": format!("b{n}")}))
        .collect();
    let r = fluree
        .insert_with_opts(
            ledger,
            &json!({"@context": ctx(), "@graph": base}),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config(),
        )
        .await
        .expect("base insert");
    drop(r.ledger);
    let _ = fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("reindex");
    ledger = fluree.ledger(&alias).await.expect("reload after reindex");
    assert!(
        ledger.snapshot.range_provider.is_some(),
        "fixture needs an indexed base"
    );

    // Novelty burst: `ex:tag` on many subjects, so the overlay the path walks
    // is large.
    let batch = 500usize;
    let mut from = 0usize;
    while from < novelty_subjects {
        let to = (from + batch).min(novelty_subjects);
        let graph: Vec<_> = (from..to)
            .map(|n| json!({"@id": format!("ex:n{n}"), "ex:tag": format!("t{n}")}))
            .collect();
        let r = fluree
            .insert_with_opts(
                ledger,
                &json!({"@context": ctx(), "@graph": graph}),
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config(),
            )
            .await
            .expect("novelty insert");
        ledger = r.ledger;
        from = to;
    }

    // `ex:narrow`: one value.
    let r = fluree
        .insert_with_opts(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:narrow", "ex:tag": "only"}),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config(),
        )
        .await
        .expect("narrow insert");
    ledger = r.ledger;

    // `ex:wide`: many values, then retract the first half so resolution has
    // real lifecycle work on the matched set.
    let values: Vec<String> = (0..wide_values).map(|n| format!("w{n:06}")).collect();
    let r = fluree
        .insert_with_opts(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:wide", "ex:tag": values}),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config(),
        )
        .await
        .expect("wide insert");
    let ledger = r.ledger;

    let doomed: Vec<serde_json::Value> = values[..wide_values / 2]
        .iter()
        .map(|v| json!({"@id": "ex:wide", "ex:tag": v}))
        .collect();
    let r: fluree_db_api::TransactResult = fluree
        .update_with_opts(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [{"@id": "ex:wide", "ex:tag": "?o"}],
                "delete": {"@graph": doomed}
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config(),
        )
        .await
        .expect("wide retract");
    assert_eq!(
        r.receipt.flake_count,
        wide_values / 2,
        "the retract must land, or the resolved half of the fixture is missing"
    );

    // Correctness stamp: the measured path must be returning the RESOLVED
    // set. If this ever reads `wide_values`, the crawl stopped taking the
    // overlay-only path (or stopped resolving) and the numbers below would be
    // measuring something else.
    let snapshot = fluree.graph(&alias).load().await.expect("graph load");
    let q = crawl("ex:wide");
    let rows = snapshot
        .query()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("stamp query");
    let live = rows
        .as_array()
        .and_then(|a| a.first())
        .and_then(|node| node.get("ex:tag"))
        .and_then(|v| v.as_array())
        .map(Vec::len)
        .unwrap_or(0);
    assert_eq!(
        live,
        wide_values - wide_values / 2,
        "overlay-only crawl must return only live values, got {rows}"
    );

    (db_dir, fluree, alias)
}

fn bench_query_overlay_only_range(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let (novelty_subjects, wide_values) = scale_inputs(scale);

    eprintln!(
        "  [query_overlay_only_range] scale={} novelty_subjects={} wide_values={} (half retracted)",
        scale.as_str(),
        novelty_subjects,
        wide_values
    );

    // Read-only scenarios, so the fixture is built once and shared.
    let (_db_dir, fluree, alias) = rt.block_on(setup(novelty_subjects, wide_values));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    let mut group = c.benchmark_group("query_overlay_only_range");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, subject) in [("narrow_subject", "ex:narrow"), ("wide_subject", "ex:wide")] {
        let q = crawl(subject);
        group.bench_with_input(
            BenchmarkId::new(name, scale.as_str()),
            &novelty_subjects,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let rows = snapshot
                            .query()
                            .jsonld(&q)
                            .execute()
                            .await
                            .expect("crawl execute");
                        black_box(rows);
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_query_overlay_only_range);
criterion_main!(benches);
