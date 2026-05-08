// End-to-end fulltext query benchmarks.
//
// Measures throughput of inline `fulltext()` scoring through the full
// Fluree JSON-LD query engine with realistic paragraph-length documents.
//
// ## Scenarios
//
// 1. `fulltext_scan_all` — Score every `@fulltext` document against a query
//    using `fulltext(?content, "query")`. Pure single-property scan + BM25.
//
// 2. `fulltext_scan_filtered` — Filter docs by category first, then score
//    remaining docs. Measures graph-pattern filter + fulltext scoring.
//
// 3. `fulltext_scan_all_indexed` — Same as (1) but after building a binary
//    index (arena BM25 path instead of TF-saturation fallback).
//
// 4. `fulltext_scan_filtered_indexed` — Same as (2) after indexing.
//
// Each scenario runs at 1K, 5K, 10K, and 50K documents with paragraph-sized
// text (~30-60 words per document), targeting sub-1-second query time.
//
// ## Running
//
//   cargo bench -p fluree-db-api --bench fulltext_query
//
// Quick validation (1 iteration each, no stats):
//
//   cargo bench -p fluree-db-api --bench fulltext_query -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_bench_support::gen::corpora::random_paragraph;
use fluree_bench_support::init_tracing_for_bench;
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};
use rand::prelude::*;
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Articles per insert transaction.
const BATCH_SIZE: usize = 200;

/// Dataset sizes to benchmark.
const DATASET_SIZES_FULL: &[usize] = &[1_000, 5_000, 10_000, 50_000];

/// Scale-driven slice of `DATASET_SIZES_FULL`. Tiny only runs the
/// smallest size so the CI bench-gate stays under its wall-clock
/// budget; nightly (`Full` profile, scale=Large) runs the whole curve.
fn dataset_sizes() -> &'static [usize] {
    use fluree_bench_support::BenchScale;
    match fluree_bench_support::current_scale() {
        BenchScale::Tiny => &DATASET_SIZES_FULL[..1],
        BenchScale::Small => &DATASET_SIZES_FULL[..2],
        BenchScale::Medium => &DATASET_SIZES_FULL[..3],
        BenchScale::Large => DATASET_SIZES_FULL,
    }
}

/// Categories for filtered queries — ~25% pass rate per category.
const CATEGORIES: &[&str] = &["science", "technology", "history", "culture"];

// ---------------------------------------------------------------------------
// Paragraph corpus
// ---------------------------------------------------------------------------
//
// `PARAGRAPH_TEMPLATES`, `EXTRA_VOCAB`, and `random_paragraph` were lifted to
// `fluree_bench_support::gen::corpora` so other text-shaped benches can reuse
// the same deterministic corpus. They're re-imported at module top.

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

type BenchFluree = fluree_db_api::Fluree;
type BenchLedger = fluree_db_api::LedgerState;

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------
//
// `random_paragraph` is `fluree_bench_support::gen::corpora::random_paragraph`
// (re-imported at module top).

/// Create Fluree instance, insert `n_docs` @fulltext documents, return
/// everything needed to run benchmark queries.
async fn setup_dataset(n_docs: usize) -> (BenchFluree, BenchLedger, JsonValue, JsonValue, usize) {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = format!("bench/ft-{n_docs}:main");
    let mut ledger = fluree.create_ledger(&alias).await.unwrap();

    let mut rng = StdRng::seed_from_u64(42);

    let ctx = json!({
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "ex": "http://example.org/ns/",
        "f": "https://ns.flur.ee/db#"
    });

    // Large novelty limit to avoid backpressure during bulk setup.
    let index_config = IndexConfig {
        reindex_min_bytes: 500_000_000,
        reindex_max_bytes: 500_000_000,
    };

    let mut n_science = 0usize;
    let mut batch = Vec::with_capacity(BATCH_SIZE);

    for i in 0..n_docs {
        let content = random_paragraph(&mut rng);
        let category = CATEGORIES[rng.gen_range(0..CATEGORIES.len())];
        if category == "science" {
            n_science += 1;
        }

        batch.push(json!({
            "@id": format!("ex:doc-{}", i),
            "@type": "ex:Document",
            "ex:title": format!("Document {}", i),
            "ex:content": {"@value": content, "@type": "@fulltext"},
            "ex:category": category
        }));

        if batch.len() >= BATCH_SIZE || i == n_docs - 1 {
            let txn = json!({
                "@context": ctx,
                "@graph": batch
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_config,
                )
                .await
                .unwrap();
            ledger = result.ledger;
            batch = Vec::with_capacity(BATCH_SIZE);
        }
    }

    // -----------------------------------------------------------------------
    // Query 1: Score ALL @fulltext docs (single-property scan + BM25)
    // -----------------------------------------------------------------------
    let query_all = json!({
        "@context": ctx,
        "select": ["?doc", "?score"],
        "where": [
            {"@id": "?doc", "ex:content": "?content"},
            ["bind", "?score", "(fulltext ?content \"distributed database systems performance\")"],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 10
    });

    // -----------------------------------------------------------------------
    // Query 2: Filter by category FIRST, then score remaining docs
    // -----------------------------------------------------------------------
    let query_filtered = json!({
        "@context": ctx,
        "select": ["?doc", "?score"],
        "where": [
            {"@id": "?doc", "ex:content": "?content", "ex:category": "?cat"},
            ["filter", "(= ?cat \"science\")"],
            ["bind", "?score", "(fulltext ?content \"machine learning neural networks research\")"],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 10
    });

    (fluree, ledger, query_all, query_filtered, n_science)
}

/// Like `setup_dataset` but also builds a binary index (arena BM25 path).
async fn setup_dataset_indexed(
    n_docs: usize,
) -> (
    BenchFluree,
    fluree_db_api::view::GraphDb,
    JsonValue,
    JsonValue,
    usize,
) {
    let (fluree, _ledger, query_all, query_filtered, n_science) = setup_dataset(n_docs).await;

    let ledger_id = format!("bench/ft-{n_docs}:main");

    fluree
        .reindex(&ledger_id, ReindexOptions::default())
        .await
        .unwrap();

    let view = fluree.db(&ledger_id).await.unwrap();

    (fluree, view, query_all, query_filtered, n_science)
}

// ---------------------------------------------------------------------------
// Benchmark: scan all docs (novelty-only, TF-saturation fallback)
// ---------------------------------------------------------------------------

fn bench_fulltext_scan_all(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fulltext_scan_all");
    group.sample_size(10);

    for &n in dataset_sizes() {
        eprintln!("  [setup] Inserting {n} @fulltext docs...");
        let (fluree, ledger, query, _, _) = rt.block_on(setup_dataset(n));
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| rt.block_on(async { black_box(fluree.query(&db, &query).await.unwrap()) }));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: scan all docs (indexed, arena BM25)
// ---------------------------------------------------------------------------

fn bench_fulltext_scan_all_indexed(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fulltext_scan_all_indexed");
    group.sample_size(10);

    for &n in dataset_sizes() {
        eprintln!("  [setup] Inserting {n} @fulltext docs + building index...");
        let (_fluree, snapshot, query, _, _) = rt.block_on(setup_dataset_indexed(n));

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                rt.block_on(async { black_box(_fluree.query(&snapshot, &query).await.unwrap()) })
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: filter by category, then score (novelty-only)
// ---------------------------------------------------------------------------

fn bench_fulltext_scan_filtered(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fulltext_scan_filtered");
    group.sample_size(10);

    for &n in dataset_sizes() {
        eprintln!("  [setup] Inserting {n} @fulltext docs...");
        let (fluree, ledger, _, query, n_science) = rt.block_on(setup_dataset(n));
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        eprintln!(
            "  {} of {} docs pass category filter (~{:.0}%)",
            n_science,
            n,
            n_science as f64 / n as f64 * 100.0
        );

        group.throughput(Throughput::Elements(n_science as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{n}_total"), n_science),
            &n,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async { black_box(fluree.query(&db, &query).await.unwrap()) })
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: filter by category, then score (indexed, arena BM25)
// ---------------------------------------------------------------------------

fn bench_fulltext_scan_filtered_indexed(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fulltext_scan_filtered_indexed");
    group.sample_size(10);

    for &n in dataset_sizes() {
        eprintln!("  [setup] Inserting {n} @fulltext docs + building index...");
        let (_fluree, snapshot, _, query, n_science) = rt.block_on(setup_dataset_indexed(n));
        eprintln!(
            "  {} of {} docs pass category filter (~{:.0}%)",
            n_science,
            n,
            n_science as f64 / n as f64 * 100.0
        );

        group.throughput(Throughput::Elements(n_science as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{n}_total"), n_science),
            &n,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        black_box(_fluree.query(&snapshot, &query).await.unwrap())
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_fulltext_scan_all,
    bench_fulltext_scan_filtered,
    bench_fulltext_scan_all_indexed,
    bench_fulltext_scan_filtered_indexed,
);
criterion_main!(benches);
