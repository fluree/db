//! Cost of binding a relationship variable over a **bounded variable-length**
//! Cypher range on a reified ledger.
//!
//! `-[rs:T*m..n]->` expands to a UNION of fixed-length join chains
//! (`fluree-db-cypher/src/lower/pattern.rs`), and binding `rs` (or a path
//! variable) gives every hop of every chain its own edge identity: a
//! `Pattern::Optional([EdgeAnnotation])` probe per hop, coalesced with the
//! synthesized relationship value. The hop count is triangular in the range —
//! `*1..1` is 1 hop, `*1..3` is 1+2+3 = 6, `*1..5` is 15 — so anything paid
//! *per probe operator* is multiplied by that count.
//!
//! Each probe plans to an `AnnotationValueOptionalBuilder`
//! (`fluree-db-query/src/optional.rs`), which answers its rows from sidecar
//! maps drained out of the three `f:reifies*` predicates. The drain is
//! O(#annotations in the ledger) and is **independent of the result size**, so
//! it is the term this bench is built to hold down: the scenarios below bind a
//! relationship variable and never read an edge property, which is the shape
//! that pays the drain for nothing.
//!
//! ## Scenarios
//!
//! All four anchor at one node of a 5-hop chain, so the result is 1, 3 or 5
//! rows and per-row work is negligible next to per-probe work.
//!
//! 1. **`unbound_1_3`** — `-[:KNOWS*1..3]->` with no relationship variable.
//!    Zero probes: the floor, and the control for scenarios 2-4.
//! 2. **`bound_1_1`** — `-[rs:KNOWS*1..1]->`. One probe — one sidecar drain.
//! 3. **`bound_1_3`** — `-[rs:KNOWS*1..3]->`. Six probes.
//! 4. **`bound_1_5`** — `-[rs:KNOWS*1..5]->`. Fifteen probes.
//!
//! With a per-query sidecar cache, 2-4 all cost one drain and the gap between
//! them is join work only. Without one, 3 and 4 are 6x and 15x scenario 2, and
//! every multiple grows with the ledger's annotation count rather than with
//! anything the query asked for. That divergence is what a regression here
//! means.
//!
//! ## Setup discipline
//!
//! Mirrors `query_hot_optional.rs`: build once per scale, populate a
//! file-backed ledger, full reindex behind the binary columnar index, then
//! reuse one `GraphDb` for every `b.iter` call (warm-cache). Indexed matters —
//! the sidecar drain runs as ordinary planned scans, and an unindexed ledger
//! answers them from novelty instead.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale -> n_claims, reified `KNOWS` edges NOT reachable
//!              from the anchor, so they inflate the sidecar without
//!              inflating the result (Tiny=1_000, Small=5_000, Medium=20_000,
//!              Large=50_000), plus a 5-hop reified anchor chain
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench annotation_varlength_probe
//!   cargo bench -p fluree-db-api --bench annotation_varlength_probe -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench annotation_varlength_probe

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig, TxnOpts};
use serde_json::{json, Value as JsonValue};

/// Hops in the anchor chain. Five so `*1..5` unrolls to its full 15 hops
/// without hitting `MAX_BOUNDED_HOPS`.
const CHAIN_HOPS: usize = 5;

/// Reified edges per insert batch. One `@graph` array per commit; batching
/// keeps the populate phase from paying commit overhead per edge.
const BATCH: usize = 2_000;

fn scale_n_claims(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 1_000,
        BenchScale::Small => 5_000,
        BenchScale::Medium => 20_000,
        BenchScale::Large => 50_000,
    }
}

/// Zero probes: the range binds no relationship variable, so the lowering
/// emits plain join chains and never touches the annotation sidecar.
const Q_UNBOUND_1_3: &str = r#"MATCH (a:Person {name: "Anchor"})-[:KNOWS*1..3]->(b) RETURN b"#;

/// One probe. The pre-existing single-hop cost, and the unit scenarios 3 and 4
/// are multiples of.
const Q_BOUND_1_1: &str = r#"MATCH (a:Person {name: "Anchor"})-[rs:KNOWS*1..1]->(b) RETURN b"#;

/// Six probes (1+2+3). No edge property is read — `rs` is bound and dropped.
const Q_BOUND_1_3: &str = r#"MATCH (a:Person {name: "Anchor"})-[rs:KNOWS*1..3]->(b) RETURN b"#;

/// Fifteen probes (1+2+3+4+5).
const Q_BOUND_1_5: &str = r#"MATCH (a:Person {name: "Anchor"})-[rs:KNOWS*1..5]->(b) RETURN b"#;

/// The anchor chain: `c0 -KNOWS-> c1 -> ... -> c5`, every hop reified, `c0`
/// the only node carrying `name: "Anchor"`. Bare names (no `@vocab`) so the
/// Cypher label `Person` and the property `name` are the same namespace-0
/// names the JSON-LD writes.
fn anchor_chain() -> Vec<JsonValue> {
    (0..=CHAIN_HOPS)
        .map(|i| {
            let mut node = json!({
                "@id": format!("c{i}"),
                "@type": "Person",
                "name": if i == 0 { "Anchor".to_string() } else { format!("chain-{i}") },
            });
            if i < CHAIN_HOPS {
                node["KNOWS"] = json!({
                    "@id": format!("c{}", i + 1),
                    "@annotation": { "confidence": 0.9 }
                });
            }
            node
        })
        .collect()
}

/// One batch of reified `KNOWS` edges over disjoint node pairs — unreachable
/// from the anchor, so they enlarge the `f:reifies*` sidecar (and therefore
/// every drain) without enlarging any scenario's result.
fn claim_batch(start: usize, end: usize) -> JsonValue {
    let graph: Vec<JsonValue> = (start..end)
        .map(|i| {
            json!({
                "@id": format!("n{i}"),
                "KNOWS": {
                    "@id": format!("m{i}"),
                    "@annotation": { "confidence": 0.5 }
                }
            })
        })
        .collect();
    json!({ "@graph": graph })
}

/// Build a populated, indexed file-backed Fluree ready for hot-cache probing.
async fn setup_indexed(n_claims: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("annotation-varlength");
    let mut ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    // High thresholds during populate so the foreground commits don't race
    // with background indexing — we run an explicit reindex below.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };

    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({ "@graph": anchor_chain() }),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("insert anchor chain")
        .ledger;

    let mut done = 0;
    while done < n_claims {
        let end = (done + BATCH).min(n_claims);
        ledger = fluree
            .insert_with_opts(
                ledger,
                &claim_batch(done, end),
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config,
            )
            .await
            .expect("insert claim batch")
            .ledger;
        done = end;
    }
    drop(ledger);

    let _ = fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("reindex");

    (db_dir, fluree, alias)
}

fn bench_annotation_varlength_probe(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_claims = scale_n_claims(scale);

    eprintln!(
        "  [annotation_varlength_probe] scale={} n_claims={} chain_hops={}",
        scale.as_str(),
        n_claims,
        CHAIN_HOPS
    );

    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_claims));
    let state = rt.block_on(fluree.ledger(&alias)).expect("load ledger");
    let db = GraphDb::from_ledger_state(&state);

    let scenarios = [
        ("unbound_1_3", Q_UNBOUND_1_3, 3usize),
        ("bound_1_1", Q_BOUND_1_1, 1),
        ("bound_1_3", Q_BOUND_1_3, 3),
        ("bound_1_5", Q_BOUND_1_5, 5),
    ];

    // Ran-marker: a query that silently answers zero rows would benchmark the
    // planner and nothing else. Assert the shape before measuring it.
    for (name, query, want_rows) in scenarios {
        let rows = rt
            .block_on(fluree.query_cypher(&db, query))
            .unwrap_or_else(|e| panic!("{name} validate: {e}"))
            .row_count();
        assert_eq!(rows, want_rows, "{name} must answer {want_rows} rows");
    }

    let mut group = c.benchmark_group("annotation_varlength_probe");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, query, _) in scenarios {
        group.bench_with_input(BenchmarkId::new(name, scale.as_str()), &n_claims, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result = fluree
                        .query_cypher(&db, query)
                        .await
                        .unwrap_or_else(|e| panic!("{name} execute: {e}"));
                    black_box(result);
                });
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_annotation_varlength_probe);
criterion_main!(benches);
