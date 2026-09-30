//! Dataset resolution and build cost, per member, on a file-backed connection.
//!
//! A dataset clause names its members as text. Each member is resolved (ledger
//! or graph source, which graph, which time) and loaded into a view before the
//! query runs, and on the graph-source-aware path every member is also probed
//! for an R2RML mapping. This bench prices that work as the member count grows,
//! for members that are graphs of one ledger (`L#<graph>`) and for members that
//! are separate ledgers.
//!
//! ## Scenarios
//!
//! - `build/{within,cross}/{n}`: `Fluree::build_dataset_view` alone.
//! - `query/{within,cross}/{n}`: SPARQL `FROM` + `FROM NAMED` × n with a
//!   `GRAPH ?g` pattern through `query_from()` (build + execute).
//! - `query_r2rml/{within,cross}/{n}` (`iceberg` feature): the same with the
//!   graph-source providers attached, as the server's `/query` runs it.
//!
//! n ∈ {1, 4, 16}. The row count of every query is checked before timing.
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench dataset_build
//!   cargo bench -p fluree-db-api --features iceberg --bench dataset_build

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{bench_runtime, current_profile, init_tracing_for_bench};
use fluree_db_api::{DatasetSpec, Fluree, FlureeBuilder, GraphSource};
use std::fmt::Write as _;
use std::hint::black_box;

const LEDGER: &str = "dsb:main";
const MAX_MEMBERS: usize = 16;
const SIZES: [usize; 3] = [1, 4, 16];

fn graph_iri(i: usize) -> String {
    format!("http://ex.org/graphs/g{i}")
}

fn member_ledger(i: usize) -> String {
    format!("dsb-m{i}:main")
}

async fn seed(fluree: &Fluree) {
    let mut trig = String::from("@prefix ex: <http://ex.org/> .\nex:d ex:title \"D\" .\n");
    for i in 0..MAX_MEMBERS {
        let _ = writeln!(
            trig,
            "GRAPH <{}> {{ ex:s{i} ex:title \"G{i}\" . }}",
            graph_iri(i)
        );
    }
    let ledger = fluree.create_ledger(LEDGER).await.unwrap();
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .unwrap();
    for i in 0..MAX_MEMBERS {
        let ledger = fluree.create_ledger(&member_ledger(i)).await.unwrap();
        fluree
            .stage_owned(ledger)
            .upsert_turtle(&format!(
                "@prefix ex: <http://ex.org/> .\nex:m{i} ex:title \"M{i}\" ."
            ))
            .execute()
            .await
            .unwrap();
    }
}

/// The `FROM NAMED` member IRIs of one scenario.
fn members(kind: &str, n: usize) -> Vec<String> {
    (0..n)
        .map(|i| match kind {
            "within" => format!("{LEDGER}#{}", graph_iri(i)),
            _ => member_ledger(i),
        })
        .collect()
}

fn spec(kind: &str, n: usize) -> DatasetSpec {
    members(kind, n).into_iter().fold(
        DatasetSpec::new().with_default(GraphSource::new(LEDGER)),
        |spec, iri| spec.with_named(GraphSource::new(iri)),
    )
}

fn sparql(kind: &str, n: usize) -> String {
    let named: String = members(kind, n)
        .iter()
        .map(|iri| format!("FROM NAMED <{iri}> "))
        .collect();
    format!(
        "PREFIX ex: <http://ex.org/>\n\
         SELECT ?g ?t FROM <{LEDGER}> {named}WHERE {{ GRAPH ?g {{ ?s ex:title ?t }} }}"
    )
}

fn bench_dataset_build(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let dir = tempfile::tempdir().unwrap();
    // The file-backed connection spawns background work at build time.
    let _runtime = rt.enter();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    rt.block_on(seed(&fluree));

    let mut group = c.benchmark_group("dataset_build");
    group.sample_size(current_profile().sample_size());
    for kind in ["within", "cross"] {
        for n in SIZES {
            let spec = spec(kind, n);
            let dataset = rt.block_on(fluree.build_dataset_view(&spec)).unwrap();
            assert_eq!(dataset.named.len(), n, "build/{kind}/{n}");
            group.bench_with_input(
                BenchmarkId::new(format!("build/{kind}"), n),
                &spec,
                |b, spec| {
                    b.iter(|| {
                        rt.block_on(async {
                            black_box(fluree.build_dataset_view(spec).await.unwrap())
                        })
                    });
                },
            );

            let query = sparql(kind, n);
            let rows = rt
                .block_on(fluree.query_from().sparql(&query).execute())
                .unwrap()
                .row_count();
            assert_eq!(rows, n, "query/{kind}/{n}");
            group.bench_with_input(
                BenchmarkId::new(format!("query/{kind}"), n),
                &query,
                |b, q| {
                    b.iter(|| {
                        rt.block_on(async {
                            black_box(fluree.query_from().sparql(q).execute().await.unwrap())
                        })
                    });
                },
            );

            #[cfg(feature = "iceberg")]
            {
                let rows = rt
                    .block_on(fluree.query_from().sparql(&query).with_r2rml().execute())
                    .unwrap()
                    .row_count();
                assert_eq!(rows, n, "query_r2rml/{kind}/{n}");
                group.bench_with_input(
                    BenchmarkId::new(format!("query_r2rml/{kind}"), n),
                    &query,
                    |b, q| {
                        b.iter(|| {
                            rt.block_on(async {
                                black_box(
                                    fluree
                                        .query_from()
                                        .sparql(q)
                                        .with_r2rml()
                                        .execute()
                                        .await
                                        .unwrap(),
                                )
                            })
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_dataset_build);
criterion_main!(benches);
