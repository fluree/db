//! CONSTRUCT serialization cost per output format.
//!
//! Every graph format runs the same pipeline up to the serializer: execute,
//! instantiate the template into a `Graph`, sort and dedupe, then write. So
//! the scenarios differ only in the writer, and the shared stages show up in
//! all of them. Allocation peak and churn are recorded next to the timings
//! (this bench installs the tracking allocator).
//!
//! ## Scenarios
//!
//! `<format>`: `CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }` over the
//! `gen::people` dataset, formatted as JSON-LD, RDF/XML, Turtle and N-Triples
//! via `execute_formatted_string`. `execute_only` runs the same query without
//! formatting, as the reference the formats add to.
//!
//! `graph_store_*`: the Graph Store `GET` queries, as N-Triples. `plain` is the
//! whole default graph; `union` adds the `UNION` branch that brings back edge
//! annotations, on the unannotated ledger and after one annotation lands;
//! `named_*` are the same over a named graph.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → nodes (Tiny=1k, Small=10k, Medium=100k,
//!              Large=1M), inserted in 1k-node transactions, unindexed
//!   metric:    ns/query (criterion); peak and total bytes (mem sidecar)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_construct_formats
//!   cargo bench -p fluree-db-api --bench query_hot_construct_formats -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_construct_formats

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_alloc::TrackingAllocator;
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::mem::{record_scenario, MemMetrics};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{FlureeBuilder, FormatterConfig, GraphPayload, GraphSel};

#[global_allocator]
static ALLOC: TrackingAllocator = TrackingAllocator::new();

const GROUP: &str = "query_hot_construct_formats";
const NODES_PER_TXN: usize = 1_000;

const QUERY: &str = "PREFIX ex: <http://example.org/ns/>
CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";

fn scale_nodes(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 1_000,
        BenchScale::Small => 10_000,
        BenchScale::Medium => 100_000,
        BenchScale::Large => 1_000_000,
    }
}

fn bench_construct_formats(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let n_nodes = scale_nodes(scale);

    let _rt_guard = rt.enter();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = next_ledger_alias("construct-formats");
    rt.block_on(async {
        let mut ledger = fluree.create_ledger(&alias).await.expect("create_ledger");
        for txn_idx in 0..n_nodes.div_ceil(NODES_PER_TXN) {
            let turtle = txn_data_to_turtle(&generate_txn_data(txn_idx, NODES_PER_TXN));
            ledger = fluree
                .insert_turtle(ledger, &turtle)
                .await
                .expect("populate")
                .ledger;
        }
    });
    let db = rt.block_on(async { fluree.db(&alias).await.expect("db") });

    let formats = [
        ("jsonld", FormatterConfig::jsonld()),
        ("rdf_xml", FormatterConfig::rdf_xml()),
        ("turtle", FormatterConfig::turtle()),
        ("ntriples", FormatterConfig::ntriples()),
    ];

    // Reference: the query alone, unformatted. What the format scenarios
    // spend beyond this is instantiation plus serialization.
    let reset_base = fluree_bench_alloc::reset_peak();
    let rows = rt.block_on(async {
        db.query(&fluree)
            .sparql(QUERY)
            .execute()
            .await
            .expect("construct")
            .row_count()
    });
    let m = fluree_bench_alloc::snapshot();
    eprintln!(
        "  [{GROUP}] execute only: rows={rows} peak={}B allocated={}B",
        m.peak_bytes.saturating_sub(reset_base),
        m.total_allocated_bytes
    );

    let mut group = c.benchmark_group(GROUP);
    group.sample_size(current_profile().sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.bench_with_input(
        BenchmarkId::new("execute_only", scale.as_str()),
        &(),
        |b, ()| {
            b.iter(|| {
                rt.block_on(async {
                    let result = db.query(&fluree).sparql(QUERY).execute().await;
                    black_box(result.expect("construct").row_count());
                });
            });
        },
    );
    for (name, config) in formats {
        // One query on its own, so peak and churn read per query.
        let reset_base = fluree_bench_alloc::reset_peak();
        let bytes = rt.block_on(async {
            db.query(&fluree)
                .sparql(QUERY)
                .format(config.clone())
                .execute_formatted_string()
                .await
                .expect("construct")
                .len()
        });
        let m = fluree_bench_alloc::snapshot();
        eprintln!(
            "  [{GROUP}] {name}: output={bytes}B peak={}B allocated={}B",
            m.peak_bytes.saturating_sub(reset_base),
            m.total_allocated_bytes
        );

        let reset_base = fluree_bench_alloc::reset_peak();
        group.bench_with_input(BenchmarkId::new(name, scale.as_str()), &(), |b, ()| {
            b.iter(|| {
                rt.block_on(async {
                    let out = db
                        .query(&fluree)
                        .sparql(QUERY)
                        .format(config.clone())
                        .execute_formatted_string()
                        .await
                        .expect("construct");
                    black_box(out.len());
                });
            });
        });
        let m = fluree_bench_alloc::snapshot();
        record_scenario(
            GROUP,
            &format!("{GROUP}/{name}/{}", scale.as_str()),
            MemMetrics {
                peak_bytes: (m.peak_bytes as u64).saturating_sub(reset_base as u64),
                total_allocated_bytes: m.total_allocated_bytes as u64,
            },
        );
    }

    // The Graph Store GET queries: plain, and with the `UNION` annotation
    // branch it adds once a ledger holds annotations (on the unannotated
    // ledger, then after one annotation lands).
    let graph_store =
        |db: &fluree_db_api::GraphDb,
         name: &str,
         query: &str,
         group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>| {
            group.bench_with_input(BenchmarkId::new(name, scale.as_str()), &(), |b, ()| {
                b.iter(|| {
                    rt.block_on(async {
                        let out = db
                            .query(&fluree)
                            .sparql(query)
                            .format(FormatterConfig::ntriples())
                            .execute_formatted_string()
                            .await
                            .expect("graph store query");
                        black_box(out.len());
                    });
                });
            });
        };
    graph_store(&db, "graph_store_plain", GRAPH_STORE_PLAIN, &mut group);
    graph_store(&db, "graph_store_union", GRAPH_STORE_UNION, &mut group);
    rt.block_on(async {
        let ledger = fluree.ledger(&alias).await.expect("ledger");
        fluree
            .insert_turtle(
                ledger,
                "@prefix ex: <http://example.org/ns/> .\n\
                 ex:annotated ex:knows ex:other ~ ex:claim {| ex:confidence 0.9 |} .\n",
            )
            .await
            .expect("annotate");
    });
    let annotated_db = rt.block_on(async { fluree.db(&alias).await.expect("db") });
    graph_store(
        &annotated_db,
        "graph_store_union_annotated_ledger",
        GRAPH_STORE_UNION,
        &mut group,
    );

    // The same data in a named graph, on a ledger whose only annotation sits
    // in the default graph: the case where the gate adds the lookup to a
    // graph that has none.
    let named_alias = next_ledger_alias("construct-formats-named");
    rt.block_on(async {
        fluree
            .create_ledger(&named_alias)
            .await
            .expect("create_ledger");
        for txn_idx in 0..n_nodes.div_ceil(NODES_PER_TXN) {
            let turtle = txn_data_to_turtle(&generate_txn_data(txn_idx, NODES_PER_TXN));
            let handle = fluree.ledger_cached(&named_alias).await.expect("handle");
            fluree
                .stage(&handle)
                .insert_graph_payload(
                    GraphSel::Graph(NAMED_GRAPH.to_string()),
                    GraphPayload::Rdf(&turtle),
                )
                .execute()
                .await
                .expect("populate named graph");
        }
        let ledger = fluree.ledger(&named_alias).await.expect("ledger");
        fluree
            .insert_turtle(
                ledger,
                "@prefix ex: <http://example.org/ns/> .\n\
                 ex:annotated ex:knows ex:other ~ ex:claim {| ex:confidence 0.9 |} .\n",
            )
            .await
            .expect("annotate");
    });
    let named_db = rt.block_on(async { fluree.db(&named_alias).await.expect("db") });
    let named_plain =
        format!("CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{NAMED_GRAPH}> {{ ?s ?p ?o }} }}");
    graph_store(
        &named_db,
        "graph_store_named_plain",
        &named_plain,
        &mut group,
    );
    let named_union = format!(
        "CONSTRUCT {{ ?s ?p ?o ~ ?r }} WHERE {{ GRAPH <{NAMED_GRAPH}> {{ {{ ?s ?p ?o }} UNION \
         {{ ?r <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> <<( ?s ?p ?o )>> }} }} }}"
    );
    graph_store(
        &named_db,
        "graph_store_named_union",
        &named_union,
        &mut group,
    );
    group.finish();
}

const NAMED_GRAPH: &str = "http://example.org/graphs/people";

const GRAPH_STORE_PLAIN: &str = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
const GRAPH_STORE_UNION: &str = "CONSTRUCT { ?s ?p ?o ~ ?r } WHERE { { ?s ?p ?o } UNION \
     { ?r <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> <<( ?s ?p ?o )>> } }";

criterion_group!(benches, bench_construct_formats);
criterion_main!(benches);
