//! Query latency across the base / overlay / novelty condition matrix.
//!
//! Every other hot-path query bench (`query_hot_bsbm`) measures the
//! epoch-0 case: all data behind the binary columnar index, no trailing
//! novelty. But the system's chronic-bug seam — and the target of the
//! audit roadmap's Phases 1–2 (`docs/audit/2026-06-architecture-audit.md`)
//! — is the **overlay merge**: columnar base combined with row-based
//! novelty. This bench runs the same query shapes at three ledger
//! conditions so a refactor of the merge/translation paths cannot regress
//! a lane that has no benchmark coverage:
//!
//! 1. **`base`** — populate, reindex; epoch 0. Strategy-(a) *and* -(b)
//!    fast paths eligible (`fast_path_common.rs` gates).
//! 2. **`overlay`** — populate, reindex, then commit a ~10% data delta
//!    with indexing thresholds set high so it stays in novelty. Exercises
//!    overlay→binary translation and cursor-merge (strategy (b)); bails
//!    strategy-(a) paths.
//! 3. **`novelty`** — populate with indexing disabled; no binary index at
//!    all. Pure novelty/range path. (Skipped at `large` scale — a
//!    multi-million-triple pure-novelty scan is a pathological shape we
//!    don't gate on.)
//!
//! ## Query shapes
//!
//! - **`count`** — `COUNT(?s)` over a bound class: the fast-count operator
//!   family (leaflet-FIRST skipping, overlay count-delta simulation).
//! - **`star`** — Q5-shape same-subject star join + range filter +
//!   ORDER BY + LIMIT: property-join/star-fusion and ordered access.
//! - **`groupby`** — Q9-shape GROUP BY + COUNT + HAVING + ORDER BY DESC:
//!   group-count fast paths.
//!
//! ## Memory metrics
//!
//! This bench installs the `fluree-bench-alloc` tracking allocator and
//! records per-scenario peak / total allocation to
//! `target/fluree-bench-mem/` (see `fluree-bench-support::mem`), which
//! `bench-baseline capture` merges into the baseline. The tracking adds a
//! small constant overhead to every allocation — identical between a
//! baseline run and a comparison run, so deltas stay valid; don't compare
//! this bench's absolute times against a non-tracking bench.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n_products (Tiny=100, Small=1k, Medium=10k,
//!              Large=100k); overlay delta = n_products / 10.
//!   scenarios: {count, star, groupby} × {base, overlay, novelty}
//!   metric:    ns/query (criterion), peak/total alloc bytes (sidecar)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_overlay_matrix
//!   cargo bench -p fluree-db-api --bench query_overlay_matrix -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_overlay_matrix
//!
//! Then capture / compare via `bench-baseline` (BENCHMARKING.md
//! "Baselines: capture & compare").

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_alloc::TrackingAllocator;
use fluree_bench_support::gen::bsbm::{bsbm_data_to_turtle, generate_dataset, BsbmData};
use fluree_bench_support::mem::{record_scenario, MemMetrics};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};

#[global_allocator]
static ALLOC: TrackingAllocator = TrackingAllocator::new();

const GROUP: &str = "query_overlay_matrix";

fn scale_n_products(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 100,
        BenchScale::Small => 1_000,
        BenchScale::Medium => 10_000,
        BenchScale::Large => 100_000,
    }
}

const Q_COUNT: &str = r"
PREFIX bsbm: <http://example.org/bsbm/>
SELECT (COUNT(?s) AS ?c) WHERE { ?s a bsbm:Product }
";

const Q_STAR: &str = r"
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

const Q_GROUPBY: &str = r"
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

/// Indexing thresholds high enough that populate/delta commits never
/// trigger foreground or background indexing on their own.
fn no_auto_index() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    }
}

/// The ~10% data delta committed on top of the indexed base for the
/// `overlay` condition. Generated from the deterministic generator's tail:
/// products `[n, n + n/10)` plus their reviews. Vendors and persons from
/// the larger dataset are included wholesale — re-asserting facts that are
/// already indexed is itself a realistic overlay shape (novelty set
/// semantics dedup them) and keeps this slice robust to generator-internal
/// count derivations.
fn overlay_delta(n_products: usize) -> BsbmData {
    let delta = std::cmp::max(1, n_products / 10);
    let full = generate_dataset(n_products + delta);
    BsbmData {
        vendors: full.vendors.clone(),
        persons: full.persons.clone(),
        products: full.products[n_products..].to_vec(),
        reviews: full.reviews[n_products * 3..].to_vec(),
    }
}

/// Populate `alias` with the base dataset; optionally reindex (binary
/// index attached, epoch 0); optionally commit the overlay delta in two
/// further commits that stay in novelty.
async fn setup(
    n_products: usize,
    index: bool,
    overlay: bool,
) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let mut builder = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string());
    if !index {
        builder = builder.without_indexing();
    }
    let fluree = builder.build().expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-overlay-matrix");
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    let turtle = bsbm_data_to_turtle(&generate_dataset(n_products));
    let r = fluree
        .insert_turtle_with_opts(
            ledger,
            &turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto_index(),
        )
        .await
        .expect("populate insert");
    let mut ledger = r.ledger;

    if index {
        let _ = fluree
            .reindex(&alias, ReindexOptions::default())
            .await
            .expect("reindex");
    }

    if overlay {
        let delta = overlay_delta(n_products);
        // Two commits so the trailing novelty has a multi-commit t-order,
        // like production overlay state: entities first, reviews second.
        let entities = BsbmData {
            reviews: Vec::new(),
            ..delta.clone()
        };
        let reviews = BsbmData {
            vendors: Vec::new(),
            persons: Vec::new(),
            products: Vec::new(),
            reviews: delta.reviews,
        };
        for part in [entities, reviews] {
            // Reload the post-reindex ledger state through a fresh handle:
            // insert returns the updated handle each time.
            let r = fluree
                .insert_turtle_with_opts(
                    ledger,
                    &bsbm_data_to_turtle(&part),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &no_auto_index(),
                )
                .await
                .expect("overlay delta insert");
            ledger = r.ledger;
        }
    }
    let _ = ledger;

    (db_dir, fluree, alias)
}

fn bench_query_overlay_matrix(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_products = scale_n_products(scale);

    eprintln!(
        "  [query_overlay_matrix] scale={} n_products={} overlay_delta={}",
        scale.as_str(),
        n_products,
        std::cmp::max(1, n_products / 10),
    );

    let mut group = c.benchmark_group(GROUP);
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    // One scenario: time the query via criterion, then record the
    // allocation peak observed across its iterations to the mem sidecar
    // under the same `<group>/<fn_id>/<scale>` ID the baseline tool uses.
    macro_rules! scenario {
        ($snapshot:expr, $fn_id:expr, $query:expr) => {{
            fluree_bench_alloc::reset_peak();
            group.bench_with_input(BenchmarkId::new($fn_id, scale.as_str()), &(), |b, ()| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = $snapshot
                            .query()
                            .sparql($query)
                            .execute()
                            .await
                            .expect("query execute");
                        black_box(result);
                    });
                });
            });
            let m = fluree_bench_alloc::snapshot();
            record_scenario(
                GROUP,
                &format!("{GROUP}/{}/{}", $fn_id, scale.as_str()),
                MemMetrics {
                    peak_bytes: m.peak_bytes as u64,
                    total_allocated_bytes: m.total_allocated_bytes as u64,
                },
            );
        }};
    }

    // --- condition: base (indexed, epoch 0) ---
    {
        let (_db_dir, fluree, alias) = rt.block_on(setup(n_products, true, false));
        let snapshot =
            rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });
        scenario!(snapshot, "count_base", Q_COUNT);
        scenario!(snapshot, "star_base", Q_STAR);
        scenario!(snapshot, "groupby_base", Q_GROUPBY);
        drop(snapshot);
        drop(fluree);
    }

    // --- condition: overlay (indexed + trailing novelty) ---
    {
        let (_db_dir, fluree, alias) = rt.block_on(setup(n_products, true, true));
        let snapshot =
            rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });
        scenario!(snapshot, "count_overlay", Q_COUNT);
        scenario!(snapshot, "star_overlay", Q_STAR);
        scenario!(snapshot, "groupby_overlay", Q_GROUPBY);
        drop(snapshot);
        drop(fluree);
    }

    // --- condition: novelty (no index at all) ---
    if scale == BenchScale::Large {
        eprintln!("  [query_overlay_matrix] skipping novelty condition at large scale");
    } else {
        let (_db_dir, fluree, alias) = rt.block_on(setup(n_products, false, false));
        let snapshot =
            rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });
        scenario!(snapshot, "count_novelty", Q_COUNT);
        scenario!(snapshot, "star_novelty", Q_STAR);
        scenario!(snapshot, "groupby_novelty", Q_GROUPBY);
        drop(snapshot);
        drop(fluree);
    }

    group.finish();
}

criterion_group!(benches, bench_query_overlay_matrix);
criterion_main!(benches);
