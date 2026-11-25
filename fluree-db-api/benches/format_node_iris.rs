//! The cost of writing node IRIs into a result, timed apart from the query.
//!
//! Each query runs once, untimed; each timed iteration formats the same
//! result. A computed IRI is one an expression built in a namespace the ledger
//! does not store (`IRI(CONCAT(...))`), so it is held as text rather than as a
//! stored node, as a `GRAPH` variable's graph, a `SERVICE` row or a graph
//! source's IRI is.
//!
//! ## Scenarios
//!
//! Results, one column of `n` rows each:
//!
//! 1. **computed_prefixed**: computed IRIs under a declared prefix (`c:x1`).
//! 2. **computed_unprefixed**: the same IRIs with no prefix declared for them.
//! 3. **stored_prefixed**: stored subjects under a declared prefix (`ex:s1`).
//!
//! Formats, for each result:
//!
//! - **jsonld_dom** / **jsonld_stream**: JSON-LD as a value tree, and streamed
//!   to a string.
//! - **typed_dom** / **typed_stream**: typed JSON, the same two ways.
//! - **sparql_json**: W3C SPARQL JSON, which writes every node absolute.
//! - **csv_display**: CSV under the CLI's compacting profile.
//!
//! ## Acceptance
//!
//! An A/B against a baseline, by scenario and format. `sparql_json` writes
//! absolute IRIs and is the control. The other formats compact a computed IRI
//! where the baseline may write it as given, so their `computed_*` cells
//! measure that cost, and `stored_prefixed` measures compaction a stored node
//! pays either way.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n rows (Tiny=1k, Small=10k, Medium=100k)
//!   metric:    ns/format (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench format_node_iris
//!   cargo bench -p fluree-db-api --bench format_node_iris -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench format_node_iris

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{bench_runtime, current_profile, current_scale, init_tracing_for_bench};
use fluree_db_api::format::format_results_string;
use fluree_db_api::{FlureeBuilder, FormatterConfig, QueryResult};
use std::fmt::Write as _;

const LEDGER: &str = "bench/format-node-iris:main";
const EX: &str = "PREFIX ex: <http://example.org/>\n";
const COMPUTED: &str = "IRI(CONCAT(\"http://computed.example/x\", STR(?n)))";

fn scenarios() -> [(&'static str, String); 3] {
    [
        (
            "computed_prefixed",
            format!(
                "{EX}PREFIX c: <http://computed.example/>\n\
                 SELECT ?i WHERE {{ ?s ex:n ?n BIND({COMPUTED} AS ?i) }}"
            ),
        ),
        (
            "computed_unprefixed",
            format!("{EX}SELECT ?i WHERE {{ ?s ex:n ?n BIND({COMPUTED} AS ?i) }}"),
        ),
        (
            "stored_prefixed",
            format!("{EX}SELECT ?i WHERE {{ ?i ex:n ?n }}"),
        ),
    ]
}

/// A result's `n` rows, and each one an absolute IRI in the W3C profile: a
/// setup that bound something else would time the wrong values.
fn sanity_check(
    name: &str,
    result: &QueryResult,
    snapshot: &fluree_db_core::LedgerSnapshot,
    n: usize,
) {
    let json = result.to_sparql_json(snapshot).expect("sparql json");
    let rows = json["results"]["bindings"].as_array().expect("bindings");
    assert_eq!(rows.len(), n, "{name}: rows");
    let want = if name.starts_with("computed") {
        "http://computed.example/x"
    } else {
        "http://example.org/s"
    };
    for row in rows {
        assert_eq!(row["i"]["type"], "uri", "{name}: {row}");
        let iri = row["i"]["value"].as_str().expect("value");
        assert!(iri.starts_with(want), "{name}: {iri}");
    }
}

fn bench_format_node_iris(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n = scale.elements_default() as usize;
    eprintln!("  [format_node_iris] scale={} rows={n}", scale.as_str());

    // `build_memory` spawns the background indexer, which needs the runtime.
    let fluree = rt.block_on(async { FlureeBuilder::memory().build_memory() });
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..n {
        writeln!(ttl, "ex:s{i} ex:n {i} .").expect("write");
    }
    let db = rt.block_on(async {
        fluree.create_ledger(LEDGER).await.expect("create");
        fluree
            .graph(LEDGER)
            .transact()
            .insert_turtle(&ttl)
            .commit()
            .await
            .expect("insert");
        fluree.db(LEDGER).await.expect("db")
    });

    let mut group = c.benchmark_group("format_node_iris");
    group.sample_size(profile.sample_size());
    for (name, query) in scenarios() {
        let result = rt.block_on(async {
            db.query(&fluree)
                .sparql(&query)
                .execute()
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"))
        });
        let snapshot = &db.snapshot;
        sanity_check(name, &result, snapshot, n);

        let id = |format: &str| BenchmarkId::new(format!("{name}/{format}"), scale.as_str());
        group.bench_function(id("jsonld_dom"), |b| {
            b.iter(|| black_box(result.to_jsonld(snapshot).expect("jsonld")));
        });
        group.bench_function(id("jsonld_stream"), |b| {
            let config = FormatterConfig::jsonld();
            b.iter(|| {
                black_box(
                    format_results_string(&result, &result.context, snapshot, &config)
                        .expect("jsonld string"),
                )
            });
        });
        group.bench_function(id("typed_dom"), |b| {
            b.iter(|| black_box(result.to_typed_json(snapshot).expect("typed")));
        });
        group.bench_function(id("typed_stream"), |b| {
            let config = FormatterConfig::typed_json();
            b.iter(|| {
                black_box(
                    format_results_string(&result, &result.context, snapshot, &config)
                        .expect("typed string"),
                )
            });
        });
        group.bench_function(id("sparql_json"), |b| {
            let config = FormatterConfig::sparql_json();
            b.iter(|| {
                black_box(
                    format_results_string(&result, &result.context, snapshot, &config)
                        .expect("sparql json string"),
                )
            });
        });
        group.bench_function(id("csv_display"), |b| {
            let config = FormatterConfig::csv().with_compact_iris();
            b.iter(|| {
                black_box(
                    result
                        .to_delimited_bytes(snapshot, &config)
                        .expect("csv bytes"),
                )
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_format_node_iris);
criterion_main!(benches);
