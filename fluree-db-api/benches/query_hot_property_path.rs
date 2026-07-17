//! Hot-cache SPARQL property-path latency on a synthetic cluster graph.
//!
//! Property paths are a hot operator (`fluree-db-query/src/property_path.rs`)
//! and the burn-down roadmap requires every PR touching it to gate on this
//! bench (docs/audit/burn-down/ROADMAP.md §2 PR-PP, §7 DoD-3). Four
//! scenarios, each pinned to a distinct execution mode of the operator:
//!
//! 1. **`*` closure, both endpoints variable** — `?s ex:link* ?o`:
//!    the full-closure adjacency build plus the zero-length term
//!    universe (SPARQL 1.1 §18.4 — every graph term pairs with itself).
//! 2. **Sequence path, bound subject** — `ex:seqRoot ex:p1/ex:p2+ ?o`:
//!    lowers to a BGP join feeding a correlated `+` traversal
//!    (`traverse_forward` BFS — the distinct-node fast path).
//! 3. **`?` (zero-or-one), both endpoints variable** — `?s ex:link? ?o`:
//!    the ZeroOrOne closure branch (zero-length universe + one hop).
//! 4. **Selective `*`, both endpoints variable** — `?s ex:rare* ?o`:
//!    one `ex:rare` edge, so closure work is trivial but the zero-length
//!    universe still scans every graph term — isolates the full-graph
//!    universe-scan cost that a link-everywhere graph would hide.
//!
//! Distinct from `query_hot_bsbm.rs` (join/filter/aggregate pipeline) —
//! this bench stresses transitive traversal and closure materialization.
//!
//! ## Setup discipline
//!
//! Mirrors `query_hot_bsbm.rs`: build once per scale, populate a
//! file-backed ledger, full reindex behind the binary columnar index,
//! then reuse the `GraphSnapshot` for all `b.iter` calls (warm-cache).
//!
//! ## Matrix
//!
//! The graph is `n_clusters` disjoint 8-node `ex:link`/`ex:p2` chains
//! (cluster-bounded closures keep output linear in scale, so the bench
//! measures per-node traversal cost, not a quadratic pair blow-up).
//! Every node also carries one literal (`ex:name`), so the zero-length
//! term universe includes literals; a single `ex:rare` edge (fixed, not
//! scaling with `n_clusters`) backs the selective-predicate scenario.
//!
//!   inputs:    BenchScale → n_clusters
//!              (Tiny=20, Small=200, Medium=2_000, Large=20_000)
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_property_path
//!   cargo bench -p fluree-db-api --bench query_hot_property_path -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_property_path

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};
use std::fmt::Write as _;

/// Nodes per chain cluster. Small and fixed so closure output stays
/// linear in `n_clusters`.
const CLUSTER_SIZE: usize = 8;

fn scale_n_clusters(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 20,
        BenchScale::Small => 200,
        BenchScale::Medium => 2_000,
        BenchScale::Large => 20_000,
    }
}

/// `*` closure with both endpoints variable: adjacency-map closure plus
/// the zero-length term universe.
const Q_STAR: &str = r"
PREFIX ex: <http://example.org/pp/>
SELECT ?s ?o WHERE { ?s ex:link* ?o }
";

/// Sequence path with a bound subject: BGP join into a correlated `+`
/// traversal (per-row forward BFS).
const Q_SEQ: &str = r"
PREFIX ex: <http://example.org/pp/>
SELECT ?o WHERE { ex:seqRoot ex:p1/ex:p2+ ?o }
";

/// Zero-or-one with both endpoints variable: zero-length universe plus
/// one direct hop, no closure.
const Q_ZERO_OR_ONE: &str = r"
PREFIX ex: <http://example.org/pp/>
SELECT ?s ?o WHERE { ?s ex:link? ?o }
";

/// Selective `*` with both endpoints variable: `ex:rare` has a single edge,
/// so the closure is trivial but the zero-length universe still scans every
/// graph term — isolates the full-graph universe-scan cost (fluree/db#1452).
const Q_RARE: &str = r"
PREFIX ex: <http://example.org/pp/>
SELECT ?s ?o WHERE { ?s ex:rare* ?o }
";

/// Generate the cluster-chain graph as Turtle.
///
/// Per cluster `c` (nodes `n{c}_0 .. n{c}_7`):
/// - `ex:link` chain edges `n{c}_i -> n{c}_{i+1}` (the `*`/`?` closure graph)
/// - `ex:p2` chain edges over the same nodes (the `+` tail of the sequence)
/// - `ex:seqRoot ex:p1 n{c}_0` (the sequence head fan-out)
/// - one literal `ex:name` per node (zero-length universes span literals)
fn cluster_graph_turtle(n_clusters: usize) -> String {
    let mut ttl = String::with_capacity(n_clusters * CLUSTER_SIZE * 96);
    ttl.push_str("@prefix ex: <http://example.org/pp/> .\n");
    for c in 0..n_clusters {
        let _ = writeln!(ttl, "ex:seqRoot ex:p1 ex:n{c}_0 .");
        for i in 0..CLUSTER_SIZE {
            let _ = writeln!(ttl, "ex:n{c}_{i} ex:name \"node-{c}-{i}\" .");
            if i + 1 < CLUSTER_SIZE {
                let next = i + 1;
                let _ = writeln!(ttl, "ex:n{c}_{i} ex:link ex:n{c}_{next} .");
                let _ = writeln!(ttl, "ex:n{c}_{i} ex:p2 ex:n{c}_{next} .");
            }
        }
    }
    // A single selective edge: `ex:rare` has exactly one edge regardless of
    // scale, so `?s ex:rare* ?o` does trivial closure work while its
    // zero-length universe still scans every term of the (scaling) graph —
    // the shape where the full-graph universe scan dominates and that the
    // `ex:link`-everywhere scenarios cannot expose (fluree/db#1452 review).
    let _ = writeln!(ttl, "ex:rareA ex:rare ex:rareB .");
    ttl
}

/// Build a populated, indexed file-backed Fluree ready for hot-cache
/// property-path benchmarking (same discipline as `query_hot_bsbm.rs`).
async fn setup_indexed(n_clusters: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-property-path");
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    let turtle = cluster_graph_turtle(n_clusters);

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

fn bench_query_hot_property_path(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_clusters = scale_n_clusters(scale);

    eprintln!(
        "  [query_hot_property_path] scale={} n_clusters={} (x{} nodes)",
        scale.as_str(),
        n_clusters,
        CLUSTER_SIZE
    );

    // Setup once per scale; `snapshot` borrows from `fluree`, both held
    // in scope for the group's duration.
    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_clusters));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    let mut group = c.benchmark_group("query_hot_property_path");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, query) in [
        ("star_closure", Q_STAR),
        ("seq_plus", Q_SEQ),
        ("zero_or_one", Q_ZERO_OR_ONE),
        ("rare_star", Q_RARE),
    ] {
        group.bench_with_input(
            BenchmarkId::new(name, scale.as_str()),
            &n_clusters,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = snapshot
                            .query()
                            .sparql(query)
                            .execute()
                            .await
                            .unwrap_or_else(|e| panic!("{name} execute: {e}"));
                        black_box(result);
                    });
                });
            },
        );
    }

    group.finish();
    drop(snapshot);
    drop(fluree);
}

criterion_group!(benches, bench_query_hot_property_path);
criterion_main!(benches);
