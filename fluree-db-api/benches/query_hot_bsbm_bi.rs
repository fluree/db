//! Hot-cache SPARQL latency for the BSBM Business-Intelligence "bowtie"
//! shape (BI-1's F2 fragment).
//!
//! Distinct from `query_hot_bsbm.rs` (the Explore-use-case Q3/Q5/Q9
//! join/filter/aggregate shapes), this bench targets the join-ordering
//! decision the query-planner change in PR #1356 introduced: when a query
//! has **two equally-selective anchors** that meet through one large
//! predicate, the seed tie-break must drive from the side that keeps the
//! big predicate hash-able rather than forward-joining over a large
//! intermediate.
//!
//! ## The F2 bowtie
//!
//! ```text
//!   ?vendor bsbm:country "US"          ← anchor A (~1/5 selective)
//!   ?product bsbm:vendor ?vendor
//!   ?review  bsbm:reviewFor ?product   ← the large predicate (the knot)
//!   ?review  bsbm:reviewer  ?person
//!   ?person bsbm:country "DE"          ← anchor B (~1/5 selective)
//! ```
//!
//! Both country filters are equally selective, so cardinality ties at the
//! seed and the planner's `unlocked_object_hash_scan` tie-break decides
//! the drive direction. On the original planner this fragment regressed
//! ~46× when the chain was written producer/vendor-first; the fix keeps
//! the `bsbm:reviewFor`/`bsbm:reviewer` predicate (the bowtie knot, the
//! 2.85M-row predicate at full BSBM-BI scale) on the hash-able side. This
//! bench guards that the chosen drive direction does not silently regress.
//!
//! ## Setup discipline
//!
//! Mirrors `query_hot_bsbm.rs`: build the dataset once per scale,
//! populate a file-backed ledger, run an explicit reindex so the measured
//! query traverses the binary columnar index (warm-cache binary scan),
//! then reuse the loaded snapshot across all `b.iter` calls.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n_products
//!              (Tiny=100, Small=1k, Medium=10k, Large=100k)
//!              reviews = n_products * 3 — the bowtie knot grows with this.
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_bsbm_bi
//!   cargo bench -p fluree-db-api --bench query_hot_bsbm_bi -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_bsbm_bi

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

/// BI-F2 bowtie: products whose vendor is in one country and which carry a
/// review by a person in another country, joined through the review
/// predicate. The two `bsbm:country` filters are equally selective, so the
/// planner's seed tie-break — not raw cardinality — picks the drive side.
const F2: &str = r#"
PREFIX bsbm: <http://example.org/bsbm/>
SELECT ?product ?vendor ?person WHERE {
  ?vendor a bsbm:Vendor ;
          bsbm:country "US" .
  ?product bsbm:vendor ?vendor .
  ?review bsbm:reviewFor ?product ;
          bsbm:reviewer ?person .
  ?person a bsbm:Person ;
          bsbm:country "DE" .
}
LIMIT 100
"#;

/// Build a populated, indexed file-backed Fluree ready for hot-cache
/// query benchmarking. The caller keeps the returned `(TempDir, Fluree,
/// alias)` alive for the duration of the bench (the snapshot loaded from
/// `fluree` borrows from it; the alias is the ledger ID used for
/// `graph(...)` lookup).
async fn setup_indexed(n_products: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-bsbm-bi");
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

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
            None,
        )
        .await
        .expect("populate insert");

    let _ = fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("reindex");

    (db_dir, fluree, alias)
}

fn bench_query_hot_bsbm_bi(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_products = scale_n_products(scale);

    eprintln!(
        "  [query_hot_bsbm_bi] scale={} n_products={}",
        scale.as_str(),
        n_products
    );

    // Setup once per scale. `_db_dir` and `fluree` are held in scope so
    // `snapshot` (which borrows from `fluree`) stays valid for the
    // duration of the bench group.
    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_products));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    let mut group = c.benchmark_group("query_hot_bsbm_bi");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    // --- F2 bowtie ---
    group.bench_with_input(
        BenchmarkId::new("f2", scale.as_str()),
        &n_products,
        |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result = snapshot
                        .query()
                        .sparql(F2)
                        .execute()
                        .await
                        .expect("F2 execute");
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

criterion_group!(benches, bench_query_hot_bsbm_bi);
criterion_main!(benches);
