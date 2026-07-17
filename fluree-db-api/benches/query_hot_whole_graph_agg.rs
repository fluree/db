//! Hot-cache Cypher aggregate latency through the metadata-lane folds.
//!
//! Guards the fast paths from `fluree-db-query`'s `fast_whole_graph_agg`:
//! whole-graph and class-anchored scalar aggregates, GROUP-BY histograms,
//! and group-key-filtered histograms are answered from index directories
//! and predicate-scoped scans instead of scanning the graph. The scenarios
//! here should stay roughly **constant in graph size**; the
//! `count/pipeline_baseline` scenario runs the same count through the
//! general pipeline (`WITH n` blocks the fold) as the linear-cost
//! reference, so a fold that silently stops firing shows up as the folded
//! scenarios converging toward the baseline.
//!
//! ## Scenarios
//!
//! 1. **count/whole_graph** — `count(n)` / `count(n.age)` /
//!    `count(DISTINCT n.age)`: directory-only row and lead-group counts.
//! 2. **scalars/whole_graph** — min/max from POST boundary keys, avg/sum
//!    from the predicate-scoped POST fold.
//! 3. **scalars/class** — the class-anchored family (instance count from
//!    class stats + containment proof for property folds).
//! 4. **histogram/class** — `RETURN n.age, COUNT(*)` as a POST run-length
//!    group count.
//! 5. **histogram/class_filtered** — same, with a group-key range filter
//!    evaluated once per group.
//! 6. **count/pipeline_baseline** — fold-blocked `MATCH (n) WITH n RETURN
//!    count(n)`: the general DISTINCT-subject scan, linear in graph size.
//!
//! ## Setup discipline
//!
//! Each scale level builds a Person/Company dataset once, populates a
//! file-backed ledger, runs a full reindex, and reuses the resulting
//! `GraphDb` for all `b.iter` calls — the folds require a HEAD index with
//! no novelty. Bare `MATCH (n)` needs the whole-graph-scan opt-in, so the
//! bench sets `FLUREE_CYPHER_ALLOW_FULL_SCAN=1` before the first query
//! (the flag is read once per process). Only Persons carry `ex:age`, so
//! the class-anchored containment proof holds; ages cycle through 48
//! values, keeping histogram output bounded at every scale.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n_nodes (Tiny=1k, Small=10k, Medium=100k,
//!              Large=1M), inserted in 1k-node transactions
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_whole_graph_agg
//!   cargo bench -p fluree-db-api --bench query_hot_whole_graph_agg -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_whole_graph_agg

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::gen::people::{generate_txn_data, TxnData};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig, TxnOpts};
use serde_json::{json, Value as JsonValue};

const NODES_PER_TXN: usize = 1_000;

/// The scenario queries. Bare identifiers resolve under the Cypher default
/// vocab (`http://example.org/`), so the dataset is serialized with that
/// base rather than `gen::people`'s `http://example.org/ns/` prefix.
const COUNT_WHOLE_GRAPH: &str =
    "MATCH (n) RETURN count(n) AS c, count(n.age) AS ca, count(DISTINCT n.age) AS cd";
const SCALARS_WHOLE_GRAPH: &str =
    "MATCH (n) RETURN min(n.age) AS mn, max(n.age) AS mx, avg(n.age) AS av, sum(n.age) AS s";
const SCALARS_CLASS: &str =
    "MATCH (n:Person) RETURN count(n) AS c, count(DISTINCT n.age) AS d, max(n.age) AS mx, avg(n.age) AS av";
const HISTOGRAM_CLASS: &str = "MATCH (n:Person) RETURN n.age, COUNT(*)";
const HISTOGRAM_CLASS_FILTERED: &str = "MATCH (n:Person) WHERE n.age >= 40 RETURN n.age, COUNT(*)";
const COUNT_PIPELINE_BASELINE: &str = "MATCH (n) WITH n RETURN count(n) AS c";

/// `gen::people` data under the Cypher default vocab: `ex:` maps to
/// `http://example.org/` so `MATCH (n:Person)` / `n.age` hit the inserted
/// IRIs without a ledger default context.
fn txn_data_to_jsonld_bare_vocab(data: &TxnData) -> JsonValue {
    let mut graph = Vec::with_capacity(data.persons.len() + data.companies.len());
    for p in &data.persons {
        graph.push(json!({
            "@id": p.id,
            "@type": "ex:Person",
            "ex:name": p.name,
            "ex:email": p.email,
            "ex:age": {"@value": p.age, "@type": "xsd:integer"}
        }));
    }
    for c in &data.companies {
        let employees: Vec<JsonValue> =
            c.employee_ids.iter().map(|id| json!({"@id": id})).collect();
        let customers: Vec<JsonValue> =
            c.customer_ids.iter().map(|id| json!({"@id": id})).collect();
        graph.push(json!({
            "@id": c.id,
            "@type": "ex:Company",
            "ex:name": c.name,
            "ex:founded": {"@value": c.founded, "@type": "xsd:date"},
            "ex:employees": employees,
            "ex:customers": customers
        }));
    }
    json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": graph
    })
}

/// Build a populated, indexed, novelty-free file-backed ledger and return
/// the `GraphDb` the scenarios query. The caller keeps `(TempDir, Fluree)`
/// alive for the duration of the bench. Also returns the total node count
/// for the setup sanity check.
async fn setup_indexed(n_nodes: usize) -> (tempfile::TempDir, Fluree, GraphDb, usize) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-wga");
    let mut ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    // High thresholds during populate so the foreground commits don't race
    // with background indexing — we run an explicit reindex below.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };

    let n_txns = n_nodes.div_ceil(NODES_PER_TXN);
    let mut total_nodes = 0usize;
    for txn_idx in 0..n_txns {
        let data = generate_txn_data(txn_idx, NODES_PER_TXN);
        total_nodes += data.persons.len() + data.companies.len();
        let doc = txn_data_to_jsonld_bare_vocab(&data);
        let result = fluree
            .insert_with_opts(
                ledger,
                &doc,
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config,
            )
            .await
            .expect("populate insert");
        ledger = result.ledger;
    }

    // Reindex publishes a HEAD index with empty novelty — the strict
    // metadata-lane gate the folds require.
    let _ = fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("reindex");
    let db = fluree.db(&alias).await.expect("indexed GraphDb");

    (db_dir, fluree, db, total_nodes)
}

/// First row of a Cypher result as JSON.
async fn first_row(fluree: &Fluree, db: &GraphDb, cypher: &str) -> JsonValue {
    let cj = fluree
        .query_cypher(db, cypher)
        .await
        .expect("cypher execute")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    cj["results"][0]["data"][0]["row"].clone()
}

/// The folds must actually answer the scenarios — a setup where they
/// silently decline would bake scan-speed numbers into the baseline.
/// Cross-check the folded whole-graph count against the generator's node
/// count (correctness implies the shape lowered as expected; the timing
/// gap vs. `count/pipeline_baseline` in the report confirms the lane).
async fn sanity_check(fluree: &Fluree, db: &GraphDb, total_nodes: usize) {
    let row = first_row(fluree, db, "MATCH (n) RETURN count(n) AS c").await;
    assert_eq!(
        row[0],
        json!(total_nodes),
        "whole-graph count(n) must equal the generated node count"
    );
    let row = first_row(fluree, db, HISTOGRAM_CLASS_FILTERED).await;
    assert!(
        row[0].is_number(),
        "filtered histogram must produce group rows: {row}"
    );
}

fn bench_query_hot_whole_graph_agg(c: &mut Criterion) {
    // Must precede the first Cypher query: the whole-graph-scan opt-in is
    // read once per process.
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_nodes = scale.elements_default() as usize;

    eprintln!(
        "  [query_hot_whole_graph_agg] scale={} n_nodes={}",
        scale.as_str(),
        n_nodes
    );

    let (_db_dir, fluree, db, total_nodes) = rt.block_on(setup_indexed(n_nodes));
    rt.block_on(sanity_check(&fluree, &db, total_nodes));

    let mut group = c.benchmark_group("query_hot_whole_graph_agg");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    let scenarios: &[(&str, &str)] = &[
        ("count/whole_graph", COUNT_WHOLE_GRAPH),
        ("scalars/whole_graph", SCALARS_WHOLE_GRAPH),
        ("scalars/class", SCALARS_CLASS),
        ("histogram/class", HISTOGRAM_CLASS),
        ("histogram/class_filtered", HISTOGRAM_CLASS_FILTERED),
        ("count/pipeline_baseline", COUNT_PIPELINE_BASELINE),
    ];

    for (name, cypher) in scenarios {
        group.bench_with_input(BenchmarkId::new(*name, scale.as_str()), cypher, |b, q| {
            b.iter(|| {
                rt.block_on(async {
                    let result = fluree.query_cypher(&db, q).await.expect("execute");
                    black_box(result);
                });
            });
        });
    }

    group.finish();
    drop(db);
    drop(fluree);
}

criterion_group!(benches, bench_query_hot_whole_graph_agg);
criterion_main!(benches);
