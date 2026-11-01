//! Reads of small named graphs that share the pooled index tree.
//!
//! A user named graph under the size bounds has no index trees of its own: its
//! rows live in one pooled tree with every other small graph, in GSPO (graph
//! first) and the graph-last secondary orders. A read of one pooled graph must
//! cost what that graph holds, not what the pool holds, and a `GRAPH ?g` block
//! runs as one quad scan over the pool.
//!
//! The ledger is document-per-graph: `g` graphs of ten quads each, every one
//! pooled (checked before anything is timed).
//!
//! ## Scenarios
//!
//! 1. **graph/whole_read**: `GRAPH ex:gK { ?s ?p ?o }`, one document whole.
//! 2. **graph/whole_count**: `COUNT(*)` over the same block.
//! 3. **graph/bound_subject**: `GRAPH ex:gK { ex:dK ?p ?o }`.
//! 4. **graphs/quad_block**: mentions per graph, `GROUP BY ?g`, over every
//!    graph through one quad scan.
//! 5. **graphs/value_lookup**: the graphs mentioning one entity.
//!
//! ## Acceptance
//!
//! Scenarios 1–3 read one graph and should stay flat as the pool grows across
//! scales; a whole read that routes every pooled leaf grows with it.
//! Scenarios 4 and 5 read the pool and grow with it.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → g graphs (Tiny=100, Small=1k, Medium=10k,
//!              Large=100k), ten quads each
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_pooled_graphs
//!   cargo bench -p fluree-db-api --bench query_hot_pooled_graphs -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_pooled_graphs

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig, TxnOpts};
use fluree_db_binary_index::BinaryIndexStore;

const PREFIXES: &str = "PREFIX ex: <http://example.org/>\n";

/// Entities a document mentions are drawn from this many.
const ENTITIES: usize = 50;

fn scenarios(k: usize) -> Vec<(&'static str, String)> {
    vec![
        (
            "graph/whole_read",
            format!("SELECT ?s ?p ?o WHERE {{ GRAPH ex:g{k} {{ ?s ?p ?o }} }}"),
        ),
        (
            "graph/whole_count",
            format!("SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH ex:g{k} {{ ?s ?p ?o }} }}"),
        ),
        (
            "graph/bound_subject",
            format!("SELECT ?p ?o WHERE {{ GRAPH ex:g{k} {{ ex:d{k} ?p ?o }} }}"),
        ),
        (
            "graphs/quad_block",
            "SELECT ?g (COUNT(?e) AS ?n) WHERE { GRAPH ?g { ?d ex:mentions ?e } } GROUP BY ?g"
                .to_string(),
        ),
        (
            "graphs/value_lookup",
            "SELECT ?g ?d WHERE { GRAPH ?g { ?d ex:mentions ex:e7 } }".to_string(),
        ),
    ]
}

/// `graphs` documents, each a named graph of ten quads: a type, a title, a
/// length, four mentions, and a section with a heading and an order.
fn documents_trig(graphs: usize) -> String {
    let mut trig = String::from("@prefix ex: <http://example.org/> .\n");
    for g in 0..graphs {
        let mentions: Vec<String> = (0..4)
            .map(|i| format!("ex:e{}", (g * 7 + i * 13) % ENTITIES))
            .collect();
        trig.push_str(&format!(
            "ex:g{g} {{ ex:d{g} a ex:Doc ; ex:title \"Document {g}\" ; ex:length {} ; \
             ex:mentions {} ; ex:section ex:s{g} . \
             ex:s{g} ex:heading \"Section of {g}\" ; ex:order 1 . }}\n",
            100 + g % 900,
            mentions.join(", "),
        ));
    }
    trig
}

/// A file-backed ledger holding `trig`, reindexed so it reads from a HEAD
/// index with no novelty.
async fn setup_indexed(fluree: &Fluree, trig: &str) -> (String, GraphDb) {
    let alias = next_ledger_alias("pooled-graphs");
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");
    // High thresholds so the populate commit does not race background
    // indexing; the explicit reindex below publishes the index.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            trig,
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
    let db = fluree.db(&alias).await.expect("indexed GraphDb");
    (alias, db)
}

/// Every graph is pooled and a whole read of one returns its ten quads: a
/// setup that gave graphs trees of their own would time the wrong layout.
async fn sanity_check(fluree: &Fluree, alias: &str, db: &GraphDb, graphs: usize, k: usize) {
    let state = fluree.ledger(alias).await.expect("load ledger");
    let store = state
        .binary_store
        .as_ref()
        .and_then(|te| te.0.downcast_ref::<BinaryIndexStore>())
        .expect("an indexed ledger");
    assert_eq!(store.pooled_graph_ids().len(), graphs, "every graph pooled");
    let result = db
        .query(fluree)
        .sparql(&format!(
            "{PREFIXES}SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH ex:g{k} {{ ?s ?p ?o }} }}"
        ))
        .execute()
        .await
        .expect("count query");
    assert_eq!(
        result.to_jsonld(&db.snapshot).expect("to_jsonld"),
        serde_json::json!([[10]])
    );
}

fn bench_query_hot_pooled_graphs(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let graphs = (scale.elements_default() / 10) as usize;
    let k = graphs / 2;
    eprintln!(
        "  [query_hot_pooled_graphs] scale={} graphs={graphs}",
        scale.as_str()
    );

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    // `build` spawns the background indexer, which needs the runtime.
    let fluree = rt.block_on(async {
        FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .build()
            .expect("build file-backed Fluree")
    });
    let (alias, db) = rt.block_on(setup_indexed(&fluree, &documents_trig(graphs)));
    rt.block_on(sanity_check(&fluree, &alias, &db, graphs, k));

    let mut group = c.benchmark_group("query_hot_pooled_graphs");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);
    for (name, body) in scenarios(k) {
        let query = format!("{PREFIXES}{body}");
        group.bench_with_input(BenchmarkId::new(name, scale.as_str()), &query, |b, q| {
            b.iter(|| {
                rt.block_on(async {
                    let result = db
                        .query(&fluree)
                        .sparql(q)
                        .execute()
                        .await
                        .unwrap_or_else(|e| panic!("{name}: {e}"));
                    black_box(result);
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_query_hot_pooled_graphs);
criterion_main!(benches);
