// GraphQL schema derivation and query benchmarks.
//
// Two questions this answers:
//
//  1. **Are the caches load-bearing?** There are two, with different lifetimes:
//     deriving the model walks every class's property statistics and merges
//     novelty, and registering the executable schema allocates a type, a filter
//     input and two order inputs per class. A GraphiQL session pays both on
//     every keystroke's introspection query if neither is cached.
//
//  2. **What does the GraphQL layer cost over the query it lowers to?** The
//     `graphql` vs `jsonld` pair runs the same read both ways, so the delta is
//     lowering plus reshaping and nothing else.
//
// ## Measured (M-series laptop, 2026-08, dev machine — indicative only)
//
//   derivation   warm       ~256 ns      (both 10 and 100 classes: a map hit)
//   derivation   cold        ~250 µs     at 10 classes
//   registration            ~274 µs      at 10 classes, ~2.5 ms at 100
//   query        graphql     ~84 µs      end to end
//   query        jsonld      ~39 µs      the same read, written by hand
//
// Registering per request was the whole of the GraphQL overhead: caching it
// took the query from ~332 µs to ~84 µs. What remains over the JSON-LD path is
// document parsing, lowering, async-graphql execution, and reshaping.
//
// ## Running
//
//   cargo bench -p fluree-db-api --features graphql --bench graphql_schema
//
// Quick validation (1 iteration each, no stats):
//
//   cargo bench -p fluree-db-api --features graphql --bench graphql_schema -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{bench_runtime, init_tracing_for_bench, next_ledger_alias};
use fluree_db_api::graphql::{derive_schema, GraphQlRequest};
use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime;

/// Class counts to derive a schema from. A ledger with hundreds of classes is
/// where derivation stops being free.
const CLASS_COUNTS: &[usize] = &[10, 100];
/// Properties per class.
const PROPERTIES_PER_CLASS: usize = 12;
/// Instances per class, so the classes have statistics at all.
const INSTANCES_PER_CLASS: usize = 5;

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({ "ex": EX })
}

/// A ledger with `classes` classes, each with properties and a few instances.
///
/// Every class also references the next, so the schema has edges to type rather
/// than only scalars — reference fields are where the target-class lookup runs.
fn seed(rt: &Runtime, fluree: &Fluree, classes: usize) -> LedgerState {
    let alias = next_ledger_alias("gqlbench");
    let mut nodes = Vec::with_capacity(classes * INSTANCES_PER_CLASS);
    for class in 0..classes {
        for instance in 0..INSTANCES_PER_CLASS {
            let mut node = serde_json::Map::new();
            node.insert("@id".to_string(), json!(format!("ex:c{class}i{instance}")));
            node.insert("@type".to_string(), json!(format!("ex:Class{class}")));
            for property in 0..PROPERTIES_PER_CLASS {
                node.insert(
                    format!("ex:p{property}"),
                    json!(format!("value {class}-{instance}-{property}")),
                );
            }
            node.insert(
                "ex:next".to_string(),
                json!({ "@id": format!("ex:c{}i0", (class + 1) % classes.max(1)) }),
            );
            nodes.push(JsonValue::Object(node));
        }
    }

    rt.block_on(async {
        let ledger = LedgerState::new(
            fluree_db_core::LedgerSnapshot::genesis(&alias),
            fluree_db_api::Novelty::new(0),
        );
        fluree
            .insert(ledger, &json!({ "@context": context(), "@graph": nodes }))
            .await
            .expect("seed")
            .ledger
    })
}

fn view(ledger: &LedgerState) -> GraphDb {
    GraphDb::from_ledger_state(ledger).with_default_context(Some(context()))
}

fn bench_schema_derivation(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    // `build_memory()` spawns the ledger-cache event listener, so it must run
    // inside the runtime.
    let fluree = rt.block_on(async { FlureeBuilder::memory().build_memory() });

    let mut group = c.benchmark_group("graphql_schema_derivation");
    for &classes in CLASS_COUNTS {
        let ledger = seed(&rt, &fluree, classes);

        // Warm: the cache hit every request after the first takes.
        group.bench_with_input(BenchmarkId::new("warm", classes), &classes, |b, _| {
            let db = view(&ledger);
            // Prime it once, outside the timed loop.
            rt.block_on(derive_schema(&db));
            b.iter(|| {
                let derived = rt.block_on(derive_schema(black_box(&db)));
                black_box(derived.model.objects.len())
            });
        });

        // Cold: a fresh context string makes a distinct cache key, so each
        // iteration derives from scratch without needing to clear the cache.
        group.bench_with_input(BenchmarkId::new("cold", classes), &classes, |b, _| {
            let mut nonce = 0u64;
            b.iter(|| {
                nonce += 1;
                let db = GraphDb::from_ledger_state(&ledger).with_default_context(Some(
                    json!({ "ex": EX, "_nonce": format!("urn:{nonce}#") }),
                ));
                let derived = rt.block_on(derive_schema(black_box(&db)));
                black_box(derived.model.objects.len())
            });
        });
    }
    group.finish();
}

/// The same read, through GraphQL and through the JSON-LD query it lowers to.
fn bench_query_overhead(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let fluree = rt.block_on(async { FlureeBuilder::memory().build_memory() });
    let ledger = seed(&rt, &fluree, 10);
    let db = view(&ledger);
    // Derivation is cached in the real endpoint too, so prime it: this measures
    // steady-state request cost, not first-request cost.
    rt.block_on(derive_schema(&db));

    let graphql = GraphQlRequest::new("{ class0s { id p0 p1 next(limit: 3) { id p0 } } }");
    let jsonld = json!({
        "@context": {},
        "select": { "?s": [
            "@id",
            format!("{EX}p0"),
            format!("{EX}p1"),
            { format!("{EX}next"): { "select": ["@id", format!("{EX}p0")], "limit": 3 } }
        ]},
        "where": [{ "@id": "?s", "@type": format!("{EX}Class0") }]
    });

    let mut group = c.benchmark_group("graphql_query_overhead");
    group.bench_function("graphql", |b| {
        b.iter(|| {
            let response = rt.block_on(fluree.graphql(black_box(&db), &graphql));
            black_box(response.expect("graphql"))
        });
    });
    group.bench_function("jsonld", |b| {
        b.iter(|| {
            let rows = rt.block_on(async {
                let result = fluree.query(black_box(&db), &jsonld).await.expect("query");
                result
                    .to_jsonld_async(db.as_graph_db_ref())
                    .await
                    .expect("format")
            });
            black_box(rows)
        });
    });
    group.finish();
}

/// Registering the executable schema, separately from deriving the model.
///
/// These are two different caches with two different lifetimes, and the query
/// overhead above is only interpretable once you know which of them a request
/// is paying for.
fn bench_schema_registration(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let fluree = rt.block_on(async { FlureeBuilder::memory().build_memory() });

    let mut group = c.benchmark_group("graphql_schema_registration");
    for &classes in CLASS_COUNTS {
        let ledger = seed(&rt, &fluree, classes);
        let db = view(&ledger);
        let derived = rt.block_on(derive_schema(&db));
        group.bench_with_input(BenchmarkId::from_parameter(classes), &classes, |b, _| {
            b.iter(|| {
                let sdl = fluree_db_graphql::sdl(black_box(&derived.model)).expect("sdl");
                black_box(sdl.len())
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_schema_derivation,
    bench_schema_registration,
    bench_query_overhead
);
criterion_main!(benches);
