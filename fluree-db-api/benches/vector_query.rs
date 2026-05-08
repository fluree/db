// End-to-end vector query benchmarks.
//
// Measures throughput of inline vector similarity functions through the full
// Fluree JSON-LD query engine with realistic article datasets.
//
// ## Scenarios
//
// 1. `vector_scan_all` — Score every `ex:articleSummaryVec` vector against
//    a query vector using `dotProduct`. Pure single-property scan path.
//
// 2. `vector_scan_filtered` — Filter articles to the last 30 days first
//    (`ex:publishedDate >= cutoff`), then score remaining vectors. Measures
//    the overhead of combining a graph-pattern filter with vector scoring.
//
// Each scenario runs at 1 K and 5 K articles with 768-dimensional vectors
// (typical for BERT / sentence-transformer embeddings).  These sizes target
// sub-1-second query response time in novelty-only mode.
//
// ## Running
//
//   cargo bench -p fluree-db-api --bench vector_query
//
// Quick validation (1 iteration each, no stats):
//
//   cargo bench -p fluree-db-api --bench vector_query -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_bench_support::gen::vectors::rng_one as random_vector;
use fluree_bench_support::init_tracing_for_bench;
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};
use rand::prelude::*;
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Common embedding dimension (BERT-like models).
const VECTOR_DIM: usize = 768;

/// Articles per insert transaction.
const BATCH_SIZE: usize = 100;

/// Dataset sizes to benchmark (keep small for sub-1s query target).
const DATASET_SIZES_FULL: &[usize] = &[1_000, 5_000];

/// Scale-driven slice of `DATASET_SIZES_FULL`. Tiny only runs the
/// smallest size so the CI bench-gate stays under its wall-clock
/// budget; nightly runs the whole curve.
fn dataset_sizes() -> &'static [usize] {
    use fluree_bench_support::BenchScale;
    match fluree_bench_support::current_scale() {
        BenchScale::Tiny => &DATASET_SIZES_FULL[..1],
        _ => DATASET_SIZES_FULL,
    }
}

// ---------------------------------------------------------------------------
// Type aliases (matches test support pattern)
// ---------------------------------------------------------------------------

type BenchFluree = fluree_db_api::Fluree;
type BenchLedger = fluree_db_api::LedgerState;

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------
//
// `random_vector` is `fluree_bench_support::gen::vectors::rng_one` (re-imported
// at module top). Same `(rng, dim) -> Vec<f64>` signature, byte-identical
// output for the same RNG seed chain.

/// Create Fluree instance, insert `n_articles` articles with vectors + dates,
/// and return everything needed to run the two benchmark queries.
async fn setup_dataset(
    n_articles: usize,
) -> (BenchFluree, BenchLedger, JsonValue, JsonValue, usize) {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = format!("bench/vec-{n_articles}:main");
    let mut ledger = fluree.create_ledger(&alias).await.unwrap();

    let mut rng = StdRng::seed_from_u64(42);

    // Date range: past 365 days. ~8% fall within last 30 days.
    let today = chrono::Utc::now().date_naive();
    let cutoff = today - chrono::Duration::days(30);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    let ctx = json!({
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "ex": "http://example.org/ns/",
        "f": "https://ns.flur.ee/db#"
    });

    // Use a large novelty limit to avoid backpressure during bulk setup.
    // 768-dim f32 vectors are ~3 KB each; 5K articles ≈ 15 MB of vector data.
    let index_config = IndexConfig {
        reindex_min_bytes: 500_000_000,
        reindex_max_bytes: 500_000_000,
    };

    let mut n_recent = 0usize;
    let mut batch = Vec::with_capacity(BATCH_SIZE);

    for i in 0..n_articles {
        let vec = random_vector(&mut rng, VECTOR_DIM);
        let days_ago = rng.gen_range(0i64..365);
        let date = today - chrono::Duration::days(days_ago);
        if date >= cutoff {
            n_recent += 1;
        }
        let date_str = date.format("%Y-%m-%d").to_string();

        batch.push(json!({
            "@id": format!("ex:article-{}", i),
            "@type": "ex:Article",
            "ex:title": format!("Article {}", i),
            "ex:articleSummaryVec": {"@value": vec, "@type": "@vector"},
            "ex:publishedDate": {"@value": date_str, "@type": "xsd:date"}
        }));

        if batch.len() >= BATCH_SIZE || i == n_articles - 1 {
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

    // Query vector (deterministic, same seed chain).
    let query_vec = random_vector(&mut rng, VECTOR_DIM);

    // -----------------------------------------------------------------------
    // Query 1: Score ALL vectors on the property (single-property scan)
    // -----------------------------------------------------------------------
    let query_all = json!({
        "@context": ctx,
        "select": ["?article", "?score"],
        // Query VALUES clauses require the full IRI for embedding vector
        // typed literals; the `@vector` alias is INSERT-only. See
        // it_vector_flatrank.rs for the canonical pattern.
        "values": [
            ["?queryVec"],
            [{"@value": query_vec.clone(), "@type": "https://ns.flur.ee/db#embeddingVector"}]
        ],
        "where": [
            {"@id": "?article", "ex:articleSummaryVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?queryVec"]]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 10
    });

    // -----------------------------------------------------------------------
    // Query 2: Filter by date FIRST, then score remaining vectors
    // -----------------------------------------------------------------------
    let query_filtered = json!({
        "@context": ctx,
        "select": ["?article", "?score"],
        "values": [
            ["?queryVec"],
            [{"@value": query_vec, "@type": "https://ns.flur.ee/db#embeddingVector"}]
        ],
        "where": [
            {"@id": "?article", "ex:publishedDate": "?date", "ex:articleSummaryVec": "?vec"},
            ["filter", [">=", "?date", cutoff_str.as_str()]],
            ["bind", "?score", ["dotProduct", "?vec", "?queryVec"]]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 10
    });

    (fluree, ledger, query_all, query_filtered, n_recent)
}

/// Like `setup_dataset` but also builds a binary index and returns a reusable snapshot.
///
/// This exercises the production fast path:
/// - `BinaryScanOperator` uses binary cursor path
/// - `PropertyJoinOperator` can batched-probe the non-driver predicate
async fn setup_dataset_indexed(
    n_articles: usize,
) -> (
    BenchFluree,
    fluree_db_api::view::GraphDb,
    JsonValue,
    JsonValue,
    usize,
) {
    let (fluree, _ledger, query_all, query_filtered, n_recent) = setup_dataset(n_articles).await;

    let ledger_id = format!("bench/vec-{n_articles}:main");

    // Offline reindex from commit history (builds binary columnar index + publishes root).
    // Note: this is intentionally done once during setup, not inside the hot loop.
    fluree
        .reindex(&ledger_id, ReindexOptions::default())
        .await
        .unwrap();

    // Reload as a view so queries can use the binary store (no commit replay).
    let view = fluree.db(&ledger_id).await.unwrap();

    (fluree, view, query_all, query_filtered, n_recent)
}

// ---------------------------------------------------------------------------
// Benchmark: scan all vectors on a single property
// ---------------------------------------------------------------------------

fn bench_vector_scan_all(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("vector_scan_all");
    group.sample_size(10);

    for &n in dataset_sizes() {
        eprintln!("  [setup] Inserting {n} articles with {VECTOR_DIM}-dim vectors...");
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
// Benchmark (indexed): scan all vectors using binary store
// ---------------------------------------------------------------------------

fn bench_vector_scan_all_indexed(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("vector_scan_all_indexed");
    group.sample_size(10);

    for &n in dataset_sizes() {
        eprintln!(
            "  [setup] Inserting {n} articles with {VECTOR_DIM}-dim vectors + building index..."
        );
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
// Benchmark: filter by date, then score remaining vectors
// ---------------------------------------------------------------------------

fn bench_vector_scan_filtered(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("vector_scan_filtered");
    group.sample_size(10);

    for &n in dataset_sizes() {
        eprintln!("  [setup] Inserting {n} articles with {VECTOR_DIM}-dim vectors...");
        let (fluree, ledger, _, query, n_recent) = rt.block_on(setup_dataset(n));
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        eprintln!(
            "  {} of {} articles pass date filter (~{:.0}%)",
            n_recent,
            n,
            n_recent as f64 / n as f64 * 100.0
        );

        // Throughput = vectors actually scored (after filter).
        group.throughput(Throughput::Elements(n_recent as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{n}_total"), n_recent),
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
// Benchmark (indexed): filter by date, then score remaining vectors (binary store)
// ---------------------------------------------------------------------------

fn bench_vector_scan_filtered_indexed(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("vector_scan_filtered_indexed");
    group.sample_size(10);

    for &n in dataset_sizes() {
        eprintln!(
            "  [setup] Inserting {n} articles with {VECTOR_DIM}-dim vectors + building index..."
        );
        let (_fluree, snapshot, _, query, n_recent) = rt.block_on(setup_dataset_indexed(n));
        eprintln!(
            "  {} of {} articles pass date filter (~{:.0}%)",
            n_recent,
            n,
            n_recent as f64 / n as f64 * 100.0
        );

        // Throughput = vectors actually scored (after filter).
        group.throughput(Throughput::Elements(n_recent as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{n}_total"), n_recent),
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
    bench_vector_scan_all,
    bench_vector_scan_filtered,
    bench_vector_scan_all_indexed,
    bench_vector_scan_filtered_indexed
);
criterion_main!(benches);
