//! Hot-cache SPARQL query latency on a BSBM-shape graph.
//!
//! Three scenarios drawn from the
//! [Berlin SPARQL Benchmark](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)
//! query catalogue, each exercising a distinct planner / scan pattern:
//!
//! 1. **Q3-shape** — multi-hop join + scalar range filter:
//!    "products of a given type with rating ≥ N".
//! 2. **Q5-shape** — multi-join with price-range filter and ORDER BY:
//!    "top-10 products by price within a range, with vendor + label".
//! 3. **Q9-shape** — group-by + count + HAVING:
//!    "products with at least K reviews".
//!
//! Distinct from `vector_query.rs` (vector-similarity scoring) and
//! `fulltext_query.rs` (BM25 scoring) — those benches stress search
//! pipelines; this bench stresses the canonical join/filter/aggregate
//! pipeline on a moderately-sized dataset.
//!
//! ## Setup discipline
//!
//! Each scale level builds the dataset once, populates a file-backed
//! ledger, runs a full reindex to put the data behind the binary
//! columnar index, and then reuses the resulting `GraphSnapshot` for
//! all `b.iter` calls. So the queries measure **warm-cache binary scan**,
//! not novelty replay or load.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n_products
//!              (Tiny=100, Small=1k, Medium=10k, Large=100k)
//!              other entity counts derive from `n_products`
//!              (`n_vendors = n_products / 50`,
//!               `n_persons = n_products / 10`,
//!               `n_reviews = n_products * 3`).
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_bsbm
//!   cargo bench -p fluree-db-api --bench query_hot_bsbm -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_bsbm

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::gen::bsbm::{bsbm_data_to_turtle, generate_dataset};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};

fn scale_n_products(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 100,
        BenchScale::Small => 1_000,
        BenchScale::Medium => 10_000,
        BenchScale::Large => 100_000,
    }
}

const Q3: &str = r"
PREFIX bsbm: <http://example.org/bsbm/>
SELECT ?product ?label WHERE {
  ?product a bsbm:Product ;
           bsbm:productType ?ptype ;
           bsbm:label ?label .
  FILTER(?ptype = 'Electronics')
  ?review bsbm:reviewFor ?product ;
          bsbm:rating ?rating .
  FILTER(?rating >= 4)
}
LIMIT 50
";

const Q5: &str = r"
PREFIX bsbm: <http://example.org/bsbm/>
SELECT ?product ?label ?vendorLabel ?price WHERE {
  ?product a bsbm:Product ;
           bsbm:label ?label ;
           bsbm:vendor ?vendor ;
           bsbm:price ?price .
  ?vendor bsbm:label ?vendorLabel .
  FILTER(?price >= 5000 && ?price <= 25000)
}
ORDER BY ?price
LIMIT 10
";

const Q9: &str = r"
PREFIX bsbm: <http://example.org/bsbm/>
SELECT ?product (COUNT(?review) AS ?nReviews) WHERE {
  ?product a bsbm:Product .
  ?review bsbm:reviewFor ?product .
}
GROUP BY ?product
HAVING (COUNT(?review) >= 3)
ORDER BY DESC(?nReviews)
LIMIT 25
";

/// Build a populated, indexed file-backed Fluree ready for hot-cache
/// query benchmarking. The caller keeps the returned `(TempDir, Fluree,
/// alias)` alive for the duration of the bench (the snapshot loaded
/// from `fluree` borrows from it; the alias is the ledger ID used for
/// `graph(...)` lookup).
async fn setup_indexed(n_products: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-bsbm");
    let ledger = fluree
        .create_ledger(&alias)
        .await
        .expect("create_ledger");

    let data = generate_dataset(n_products);
    let turtle = bsbm_data_to_turtle(&data);

    // High thresholds during populate so the foreground commit doesn't
    // race with background indexing — we run an explicit reindex below.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            &turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("populate insert");

    // Reindex puts the data behind the binary columnar index so the
    // measured queries traverse the production hot path.
    let _ = fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("reindex");

    (db_dir, fluree, alias)
}

fn bench_query_hot_bsbm(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_products = scale_n_products(scale);

    eprintln!(
        "  [query_hot_bsbm] scale={} n_products={}",
        scale.as_str(),
        n_products
    );

    // Setup once per scale. `_db_dir` and `fluree` are held in scope so
    // `snapshot` (which borrows from `fluree`) stays valid for the
    // duration of the bench group.
    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_products));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    let mut group = c.benchmark_group("query_hot_bsbm");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    // --- Q3-shape ---
    group.bench_with_input(
        BenchmarkId::new("q3", scale.as_str()),
        &n_products,
        |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result = snapshot
                        .query()
                        .sparql(Q3)
                        .execute()
                        .await
                        .expect("Q3 execute");
                    black_box(result);
                });
            });
        },
    );

    // --- Q5-shape ---
    group.bench_with_input(
        BenchmarkId::new("q5", scale.as_str()),
        &n_products,
        |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result = snapshot
                        .query()
                        .sparql(Q5)
                        .execute()
                        .await
                        .expect("Q5 execute");
                    black_box(result);
                });
            });
        },
    );

    // --- Q9-shape ---
    group.bench_with_input(
        BenchmarkId::new("q9", scale.as_str()),
        &n_products,
        |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result = snapshot
                        .query()
                        .sparql(Q9)
                        .execute()
                        .await
                        .expect("Q9 execute");
                    black_box(result);
                });
            });
        },
    );

    group.finish();
    // snapshot's borrow of fluree ends here; explicit drops below keep
    // the order well-defined.
    drop(snapshot);
    drop(fluree);
}

criterion_group!(benches, bench_query_hot_bsbm);
criterion_main!(benches);
