// JSON-LD transaction parsing, parse only.
//
// Times `fluree_db_transact::parse_transaction` on its own: context
// handling, edge-annotation lowering, expansion and template construction.
// No ledger, no staging, no commit. Changes to the JSON-LD template parser
// (graph scoping, blank-node labelling, keyword handling) show up here
// without the staging and commit costs that dominate `insert_formats` and
// `transact_commit`.
//
// ## Scenarios
//
// 1. `flat_envelope` — an insert envelope of flat nodes (`@type`, two
//    literals, one reference each).
// 2. `nested_selector` — an insert of depth-3 node trees, each root carrying
//    a node-level `"@graph": "<iri>"` selector; the leaf is anonymous.
// 3. `update_sugar` — an update whose `insert` is an array of
//    `["graph", <iri>, <node>]` items over 16 graphs.
// 4. `annotations` — an insert of edges that each carry an `@annotation`
//    (runs the edge-annotation lowering pass).
//
// ## Matrix
//
//   nodes (flat_envelope, nested_selector): tiny=2k, small=20k, medium=100k, large=200k
//   items (update_sugar):                   tiny=100, small=1k, medium=5k, large=10k
//   edges (annotations):                    tiny=200, small=2k, medium=10k, large=20k
//   metric: elements/sec (nodes, items or edges)
//
// ## Running
//
//   cargo bench -p fluree-db-api --bench jsonld_parse
//
// Quick validation (single iteration, no stats):
//
//   cargo bench -p fluree-db-api --bench jsonld_parse -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_bench_support::{current_profile, current_scale, init_tracing_for_bench, BenchScale};
use fluree_db_transact::{parse_transaction, NamespaceRegistry, TxnOpts, TxnType};
use serde_json::{json, Value};

struct Sizes {
    nodes: usize,
    sugar_items: usize,
    annotated_edges: usize,
}

fn sizes(scale: BenchScale) -> Sizes {
    match scale {
        BenchScale::Tiny => Sizes {
            nodes: 2_000,
            sugar_items: 100,
            annotated_edges: 200,
        },
        BenchScale::Small => Sizes {
            nodes: 20_000,
            sugar_items: 1_000,
            annotated_edges: 2_000,
        },
        BenchScale::Medium => Sizes {
            nodes: 100_000,
            sugar_items: 5_000,
            annotated_edges: 10_000,
        },
        BenchScale::Large => Sizes {
            nodes: 200_000,
            sugar_items: 10_000,
            annotated_edges: 20_000,
        },
    }
}

fn context() -> Value {
    json!({ "ex": "http://example.org/ns/" })
}

/// `n` flat nodes in one envelope: 5 statements per node.
fn flat_envelope(n: usize) -> Value {
    let nodes: Vec<Value> = (0..n)
        .map(|i| {
            json!({
                "@id": format!("ex:n{i}"),
                "@type": "ex:Person",
                "ex:name": format!("Person {i}"),
                "ex:age": i % 100,
                "ex:knows": {"@id": format!("ex:n{}", i.saturating_sub(1))}
            })
        })
        .collect();
    json!({ "@context": context(), "@graph": nodes })
}

/// `n` nodes as `n / 3` depth-3 trees under a node-level graph selector.
fn nested_selector(n: usize) -> Value {
    let roots: Vec<Value> = (0..n / 3)
        .map(|i| {
            json!({
                "@id": format!("ex:a{i}"),
                "@graph": "http://example.org/graphs/g1",
                "@type": "ex:Root",
                "ex:name": format!("a{i}"),
                "ex:child": {
                    "@id": format!("ex:b{i}"),
                    "@type": "ex:Mid",
                    "ex:name": format!("b{i}"),
                    "ex:child": {
                        "@type": "ex:Leaf",
                        "ex:name": format!("c{i}"),
                        "ex:val": i
                    }
                }
            })
        })
        .collect();
    json!({ "@context": context(), "@graph": roots })
}

/// An update whose `insert` holds `n` `["graph", g, node]` items.
fn update_sugar(n: usize) -> Value {
    let items: Vec<Value> = (0..n)
        .map(|i| {
            json!([
                "graph",
                format!("http://example.org/graphs/g{}", i % 16),
                {
                    "@id": format!("ex:s{i}"),
                    "ex:p": i,
                    "ex:q": format!("v{i}")
                }
            ])
        })
        .collect();
    json!({ "@context": context(), "insert": items })
}

/// `n` annotated edges in one envelope.
fn annotations(n: usize) -> Value {
    let nodes: Vec<Value> = (0..n)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "ex:worksFor": {
                    "@id": format!("ex:org{}", i % 50),
                    "@annotation": {
                        "ex:role": format!("r{}", i % 7),
                        "ex:since": 2000 + (i % 20)
                    }
                }
            })
        })
        .collect();
    json!({ "@context": context(), "@graph": nodes })
}

fn parse_once(doc: &Value, txn_type: TxnType) -> usize {
    let mut ns = NamespaceRegistry::new();
    let txn = parse_transaction(doc, txn_type, TxnOpts::default(), &mut ns, "bench:main")
        .expect("bench document parses");
    txn.insert_templates.len()
}

fn bench_jsonld_parse(c: &mut Criterion) {
    init_tracing_for_bench();
    let scale = current_scale();
    let sizes = sizes(scale);

    let scenarios: [(&str, Value, TxnType, usize); 4] = [
        (
            "flat_envelope",
            flat_envelope(sizes.nodes),
            TxnType::Insert,
            sizes.nodes,
        ),
        (
            "nested_selector",
            nested_selector(sizes.nodes),
            TxnType::Insert,
            sizes.nodes / 3 * 3,
        ),
        (
            "update_sugar",
            update_sugar(sizes.sugar_items),
            TxnType::Update,
            sizes.sugar_items,
        ),
        (
            "annotations",
            annotations(sizes.annotated_edges),
            TxnType::Insert,
            sizes.annotated_edges,
        ),
    ];

    let mut group = c.benchmark_group("jsonld_parse");
    group.sample_size(current_profile().sample_size());
    for (name, doc, txn_type, elements) in &scenarios {
        // Sanity: every scenario produces templates, so a regression that
        // silently drops data cannot masquerade as a speedup.
        assert!(
            parse_once(doc, *txn_type) > 0,
            "{name} produced no templates"
        );
        group.throughput(Throughput::Elements(*elements as u64));
        group.bench_with_input(BenchmarkId::new(*name, scale.as_str()), doc, |b, doc| {
            b.iter(|| black_box(parse_once(black_box(doc), *txn_type)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_jsonld_parse);
criterion_main!(benches);
