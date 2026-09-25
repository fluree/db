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
//! ## Matrix
//!
//!   inputs:    BenchScale → nodes (Tiny=1k, Small=10k, Medium=100k,
//!              Large=1M), inserted in 1k-node transactions, unindexed
//!   metric:    ns/query (criterion); peak and total bytes (mem sidecar)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_construct_formats
//!   cargo bench -p fluree-db-api --bench query_construct_formats -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_construct_formats

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_alloc::TrackingAllocator;
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::mem::{record_scenario, MemMetrics};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{FlureeBuilder, FormatterConfig};

#[global_allocator]
static ALLOC: TrackingAllocator = TrackingAllocator::new();

const GROUP: &str = "query_construct_formats";
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
    group.finish();
}

criterion_group!(benches, bench_construct_formats);
criterion_main!(benches);
