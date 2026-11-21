//! Keying stored vectors: `DISTINCT`, `GROUP BY`, a join and `MINUS` on `?v`.
//!
//! One distinct 768-dimension vector per subject under `ex:v`, and a copy of
//! every tenth under `ex:u`, on an indexed ledger: the embedding shape, where
//! a key that decodes or renders each vector costs per row what a handle
//! costs per ledger. The cells run first without the arenas' identity tables
//! (a key is a slice of its shard), then with them built (a key is a hash and
//! a canonical handle, and reads nothing). A per-predicate `COUNT(DISTINCT)`
//! is what builds them; the whole-graph `COUNT(DISTINCT ?o)` runs only after.
//! Tiny=2k, small=10k, medium=50k, large=100k subjects.
//!
//! cargo bench -p fluree-db-api --bench query_hot_vector_keying
//! FLUREE_BENCH_SCALE=small cargo bench -p fluree-db-api --bench query_hot_vector_keying
//! cargo bench -p fluree-db-api --bench query_hot_vector_keying -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::gen::vectors::hashed_one;
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};
use std::fmt::Write;

const DIMS: usize = 768;
const P: &str = "PREFIX ex: <http://example.org/> ";

fn vector_literal(seed: usize) -> String {
    let mut out = String::from("\"[");
    for (i, x) in hashed_one(DIMS, seed).iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        write!(out, "{x:.6}").unwrap();
    }
    out.push_str("]\"^^<https://ns.flur.ee/db#embeddingVector>");
    out
}

async fn setup(n: usize) -> (tempfile::TempDir, Fluree, String) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = next_ledger_alias("query-hot-vector-keying");
    let ledger = fluree.create_ledger(&alias).await.unwrap();
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..n {
        let v = vector_literal(i * DIMS);
        writeln!(ttl, "ex:s{i} ex:v {v} .").unwrap();
        if i % 10 == 0 {
            writeln!(ttl, "ex:s{i} ex:u {v} .").unwrap();
        }
    }
    fluree
        .insert_turtle_with_opts(
            ledger,
            &ttl,
            TxnOpts::default(),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 5_000_000_000,
                reindex_max_bytes: 5_000_000_000,
            },
            None,
        )
        .await
        .unwrap();
    fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .unwrap();
    (dir, fluree, alias)
}

/// The keying cells, each counted so no vector is rendered.
fn keying_cells() -> [(&'static str, String); 3] {
    [
        (
            "distinct",
            format!(
                "{P}SELECT (COUNT(*) AS ?k) WHERE {{ SELECT DISTINCT ?v WHERE {{ ?s ex:v ?v }} }}"
            ),
        ),
        (
            "group_by",
            format!(
                "{P}SELECT (COUNT(*) AS ?k) WHERE {{ SELECT ?v (COUNT(?s) AS ?n) \
                 WHERE {{ ?s ex:v ?v }} GROUP BY ?v }}"
            ),
        ),
        (
            "join",
            format!("{P}SELECT (COUNT(*) AS ?k) WHERE {{ ?a ex:v ?v . ?b ex:u ?v }}"),
        ),
    ]
}

fn bench_query_hot_vector_keying(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n = match scale {
        BenchScale::Tiny => 2_000,
        BenchScale::Small => 10_000,
        BenchScale::Medium => 50_000,
        BenchScale::Large => 100_000,
    };
    let (_dir, fluree, alias) = rt.block_on(setup(n));
    let graph = rt.block_on(async { fluree.graph(&alias).load().await.unwrap() });
    let mut group = c.benchmark_group("query_hot_vector_keying");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);
    let mut run = |name: String, sparql: &str| {
        group.bench_with_input(BenchmarkId::new(name, scale.as_str()), sparql, |b, q| {
            b.iter(|| {
                rt.block_on(async { black_box(graph.query().sparql(q).execute().await.unwrap()) })
            });
        });
    };

    // Before the identity tables exist: none of these builds one.
    let mut cells = keying_cells().to_vec();
    cells.push((
        "minus",
        format!("{P}SELECT (COUNT(*) AS ?k) WHERE {{ ?a ex:v ?v MINUS {{ ?b ex:u ?v }} }}"),
    ));
    for (name, sparql) in &cells {
        run(format!("{name}_no_table"), sparql);
    }

    // The per-predicate COUNT(DISTINCT) builds each arena's table, once.
    let count_distinct =
        |p: &str| format!("{P}SELECT (COUNT(DISTINCT ?v) AS ?k) WHERE {{ ?s ex:{p} ?v }}");
    rt.block_on(async {
        for p in ["v", "u"] {
            graph
                .query()
                .sparql(&count_distinct(p))
                .execute()
                .await
                .unwrap();
        }
    });
    for (name, sparql) in keying_cells() {
        run(format!("{name}_table_built"), &sparql);
    }
    run("count_distinct".to_string(), &count_distinct("v"));
    // Every `ex:u` vector is also an `ex:v` vector, so each one is a hash the
    // two arenas share and is compared across them. This count would build
    // the tables itself, so it has no cell before them.
    run(
        "count_distinct_whole_graph".to_string(),
        "SELECT (COUNT(DISTINCT ?o) AS ?k) WHERE { ?s ?p ?o }",
    );
    group.finish();
}

criterion_group!(benches, bench_query_hot_vector_keying);
criterion_main!(benches);
