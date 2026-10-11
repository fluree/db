//! Hot-cache latency of annotation queries over RDF 1.2 links.
//!
//! An annotated edge is a reifier with an `rdf:reifies <<( s p o )>>` link.
//! Annotation syntax joins the link and checks the base edge exists; each
//! scenario pins a lane that keeps that cost proportional to the query rather
//! than to the predicate or the ledger:
//!
//! 1. **`annotated_edges_small_outer`** — 200 annotated edges into one hub's
//!    posts. The base-edge EXISTS seeds its build from the outer keys
//!    (`fluree-db-query/src/semijoin.rs`).
//! 2. **`annotated_edges_past_one_chunk`** — the same over 1,100 edges, past
//!    one seeded build's 1,024 keys: seeded a chunk at a time, not built over
//!    the whole `ex:likes` predicate.
//! 3. **`annotated_edges_var_predicate`** — scenario 2 with the predicate a
//!    variable, where an unseeded build would cover the whole graph.
//! 4. **`reified_pattern_count`** — `COUNT(*)` over every link of one
//!    predicate, `<< ?s ex:likes ?o >> ex:at ?at`.
//! 5. **`wildcard_into_posts`** — `?x ?p ?post` into one hub's posts: the
//!    batched wildcard-predicate join (`fluree-db-query/src/join/wildcard.rs`).
//! 6. **`independent_count_product`** — `COUNT(*)` over two independent
//!    sides, answered as a product (`fluree-db-query/src/join/replay.rs`).
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n_likes annotated `ex:likes` edges, 1,300 of
//!              them into the two hubs' posts
//!              (Tiny=20_000, Small=100_000, Medium=400_000, Large=1_000_000)
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_annotation_links
//!   cargo bench -p fluree-db-api --bench query_hot_annotation_links -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_annotation_links

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};
use std::fmt::Write as _;

/// Annotated edges into each hub's posts, one like per post.
const SMALL_HUB: usize = 200;
const LARGE_HUB: usize = 1_100;

fn scale_n_likes(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 20_000,
        BenchScale::Small => 100_000,
        BenchScale::Medium => 400_000,
        BenchScale::Large => 1_000_000,
    }
}

/// `(name, query, expected count)`.
const SCENARIOS: [(&str, &str, usize); 6] = [
    (
        "annotated_edges_small_outer",
        r"PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?n) WHERE { ?post ex:author ex:hub0 . ?liker ex:likes ?post {| ex:at ?at |} }",
        SMALL_HUB,
    ),
    (
        "annotated_edges_past_one_chunk",
        r"PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?n) WHERE { ?post ex:author ex:hub1 . ?liker ex:likes ?post {| ex:at ?at |} }",
        LARGE_HUB,
    ),
    (
        "annotated_edges_var_predicate",
        r"PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?n) WHERE { ?post ex:author ex:hub1 . ?liker ?rel ?post {| ex:at ?at |} }",
        LARGE_HUB,
    ),
    (
        "reified_pattern_count",
        r"PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?n) WHERE { << ?s ex:likes ?o >> ex:at ?at }",
        0, // n_likes, checked separately
    ),
    (
        "wildcard_into_posts",
        r"PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?n) WHERE { ?post ex:author ex:hub1 . ?x ?p ?post }",
        LARGE_HUB,
    ),
    (
        "independent_count_product",
        r"PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?n) WHERE { ?a ex:author ex:hub0 . ?b ex:author ex:hub1 }",
        SMALL_HUB * LARGE_HUB,
    ),
];

/// Hub posts each liked once; the remaining likes spread over a hundred
/// other posts. Every like is annotated.
fn likes_turtle(n_likes: usize) -> String {
    let mut ttl = String::with_capacity(n_likes * 120);
    ttl.push_str("VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n");
    let like = |ttl: &mut String, i: usize, post: &str| {
        let _ = writeln!(ttl, "ex:fan{i} ex:likes {post} {{| ex:at {i} |}} .");
    };
    for i in 0..SMALL_HUB {
        let _ = writeln!(ttl, "ex:hub0-post{i} ex:author ex:hub0 .");
        like(&mut ttl, i, &format!("ex:hub0-post{i}"));
    }
    for i in 0..LARGE_HUB {
        let _ = writeln!(ttl, "ex:hub1-post{i} ex:author ex:hub1 .");
        like(&mut ttl, SMALL_HUB + i, &format!("ex:hub1-post{i}"));
    }
    for i in (SMALL_HUB + LARGE_HUB)..n_likes {
        like(&mut ttl, i, &format!("ex:other{}", i % 100));
    }
    ttl
}

/// Populated, indexed file-backed Fluree (same discipline as
/// `query_hot_negation_count.rs`).
async fn setup_indexed(n_likes: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-annotation-links");
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    // High thresholds so populate doesn't race background indexing; the
    // explicit reindex below builds the index.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            &likes_turtle(n_likes),
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

fn bench_query_hot_annotation_links(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_likes = scale_n_likes(scale);

    eprintln!(
        "  [query_hot_annotation_links] scale={} n_likes={n_likes}",
        scale.as_str()
    );

    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_likes));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    // A lane that got faster by answering wrong is not a win.
    for (name, query, expected) in SCENARIOS {
        let expected = if expected == 0 { n_likes } else { expected };
        let count = rt.block_on(async {
            let result = snapshot
                .query()
                .sparql(query)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{name} execute: {e}"));
            result["results"]["bindings"][0]["n"]["value"]
                .as_str()
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("{name}: {result}"))
        });
        assert_eq!(count as usize, expected, "{name}");
    }

    let mut group = c.benchmark_group("query_hot_annotation_links");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, query, _) in SCENARIOS {
        group.bench_with_input(BenchmarkId::new(name, scale.as_str()), &n_likes, |b, _| {
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
        });
    }

    group.finish();
    drop(snapshot);
    drop(fluree);
}

criterion_group!(benches, bench_query_hot_annotation_links);
criterion_main!(benches);
