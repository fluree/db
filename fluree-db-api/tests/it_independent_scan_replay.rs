//! Standalone: independent scan routing capture and the process-global fast-path switch.
#![cfg(feature = "native")]
mod support;

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, ReindexOptions};
use fluree_db_ledger::LedgerState;
use serde_json::{json, Value};

const PREFIX: &str = "PREFIX ex: <http://example.org/> ";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const EVENT: &str = "independent scan replay engaged";
const COUNT_EVENT: &str = "independent join count product engaged";

#[derive(Clone, Copy, Debug)]
enum Routing {
    MustFire,
    MustNotFire,
}

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

async fn check(
    fluree: &Fluree,
    ledger: &LedgerState,
    query: &str,
    expected_len: usize,
    routing: Routing,
) -> Value {
    let query = format!("{PREFIX}{query}");
    let (spans, _guard) = support::span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let fast = support::query_sparql(fluree, ledger, &query)
        .await
        .expect(&query)
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let fast_rows = normalized(&fast);
    assert_eq!(fast_rows.len(), expected_len, "{query}: {fast}");
    let fired = spans.has_event(EVENT);
    assert_eq!(
        fired,
        matches!(routing, Routing::MustFire),
        "{routing:?}: {query}"
    );
    drop(_guard);
    let (spans, _guard) = support::span_capture::init_test_tracing();
    set_fast_paths_disabled(true);
    let generic = support::query_sparql(fluree, ledger, &query)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(fast_rows, normalized(&generic), "{query}");
    if query.contains("ORDER BY") {
        assert_eq!(
            fast["results"]["bindings"], generic["results"]["bindings"],
            "ordered output"
        );
    }
    assert!(!spans.has_event(EVENT), "disabled lane fired: {query}");
    set_fast_paths_disabled(false);
    fast
}

async fn check_count(
    fluree: &Fluree,
    ledger: &LedgerState,
    query: &str,
    expected: u64,
    product: bool,
) {
    for disabled in [false, true] {
        set_fast_paths_disabled(disabled);
        let (spans, guard) = support::span_capture::init_test_tracing();
        let value = support::query_sparql(fluree, ledger, &format!("{PREFIX}{query}"))
            .await
            .unwrap()
            .to_sparql_json(&ledger.snapshot)
            .unwrap();
        assert_eq!(
            value["results"]["bindings"][0]["total"]["value"],
            expected.to_string(),
            "{query}"
        );
        assert_eq!(
            spans.has_event(COUNT_EVENT),
            product && !disabled,
            "count routing: {query}"
        );
        drop(guard);
    }
    set_fast_paths_disabled(false);
}

async fn check_count_lifecycle(ledger: &LedgerState) {
    use fluree_db_query::{
        binary_scan::EmitMask,
        binding::Batch,
        context::ExecutionContext,
        ir::{Ref, Term, TriplePattern},
        join::NestedLoopJoinOperator,
        operator::Operator,
        seed::BatchSeedOperator,
        TemporalMode, VarRegistry,
    };
    use std::sync::Arc;
    let mut vars = VarRegistry::new();
    let subject = vars.get_or_insert("?s");
    let object = vars.get_or_insert("?o");
    let store = ledger
        .binary_store
        .as_ref()
        .unwrap()
        .0
        .clone()
        .downcast::<fluree_db_binary_index::BinaryIndexStore>()
        .unwrap();
    let ctx = ExecutionContext::new(&ledger.snapshot, &vars)
        .with_binary_store(store, 0)
        .with_batch_size(1);
    let pattern = TriplePattern::new(
        Ref::Var(subject),
        Ref::Sid(
            ledger
                .snapshot
                .encode_iri("http://example.org/items")
                .unwrap(),
        ),
        Term::Var(object),
    );
    let make = |rows| {
        NestedLoopJoinOperator::new(
            Box::new(BatchSeedOperator::from_batch(Batch::empty_schema_with_len(
                rows,
            ))),
            Arc::from([]),
            pattern.clone(),
            None,
            Vec::new(),
            EmitMask::ALL,
            TemporalMode::Current,
        )
    };
    let mut fresh = make(3);
    fresh.open(&ctx).await.unwrap();
    assert_eq!(fresh.drain_count(&ctx).await.unwrap(), Some(9));
    assert!(fresh.next_batch(&ctx).await.unwrap().is_none());
    fresh.close();
    let mut partial = make(3);
    partial.open(&ctx).await.unwrap();
    let mut rows = partial.next_batch(&ctx).await.unwrap().unwrap().len();
    assert_eq!(rows, 1);
    assert_eq!(
        partial.drain_count(&ctx).await.unwrap(),
        None,
        "must decline after partial output"
    );
    while let Some(batch) = partial.next_batch(&ctx).await.unwrap() {
        rows += batch.len();
    }
    assert_eq!(rows, 9);
    partial.close();
    #[cfg(target_pointer_width = "64")]
    {
        // Empty-schema batches carry multiplicity without allocating the rows.
        let mut overflow = make(usize::MAX);
        overflow.open(&ctx).await.unwrap();
        assert!(overflow
            .drain_count(&ctx)
            .await
            .unwrap_err()
            .to_string()
            .contains("overflow"));
        overflow.close();
    }
}

fn normalized(result: &Value) -> Vec<String> {
    let mut rows: Vec<_> = result["results"]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| serde_json::to_string(r).unwrap())
        .collect();
    rows.sort();
    rows
}

#[tokio::test(flavor = "current_thread")]
async fn independent_scans_preserve_multiplicity_filters_and_fallbacks() {
    let _serial = SERIAL.lock().await;
    assert!(std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none());
    let _reset = Reset;
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = "replay/test:main";
    let ledger = fluree.create_ledger(alias).await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({
                "@context":{"ex":"http://example.org/"},
                "@graph":[
                    {"@id":"ex:a", "ex:driver":1, "ex:items":{"@list":[10,10,20]},
                     "ex:edge":{"@id":"ex:b","@annotation":{"ex:source":"one"}}},
                    {"@id":"ex:b", "ex:driver":2,
                     "ex:edge":{"@id":"ex:c","@annotation":{"ex:source":"two"}}},
                    {"@id":"ex:c", "ex:driver":3, "ex:label":[
                        {"@value":"bonjour","@language":"fr"}, {"@value":"hello","@language":"en"}]}
                ]
            }),
        )
        .await
        .unwrap();
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .unwrap();
    let ledger = fluree.ledger(alias).await.unwrap();
    check_count_lifecycle(&ledger).await;
    use Routing::*;
    let body = "?l ex:driver ?n . ?s ex:items ?o";
    let q = format!("SELECT ?l ?s ?o WHERE {{ {body} }}");
    check_count(
        &fluree,
        &ledger,
        &format!("SELECT (COUNT(*) AS ?total) WHERE {{ {body} }}"),
        9,
        true,
    )
    .await;
    check_count(
        &fluree,
        &ledger,
        &format!("SELECT (COUNT(*) AS ?total) WHERE {{ {body} FILTER(?n + ?o > 21) }}"),
        2,
        false,
    )
    .await;
    check_count(
        &fluree,
        &ledger,
        &format!("SELECT (COUNT(DISTINCT ?l) AS ?total) WHERE {{ {body} }}"),
        3,
        false,
    )
    .await;
    check_count(
        &fluree,
        &ledger,
        "SELECT (COUNT(*) AS ?total) WHERE { ?l ex:driver ?x . ?l ex:items ?o }",
        3,
        false,
    )
    .await;
    check_count(
        &fluree,
        &ledger,
        "SELECT (COUNT(*) AS ?total) WHERE { ?l ex:driver ?x . ?s ex:missing ?o }",
        0,
        true,
    )
    .await;

    let result = check(&fluree, &ledger, &q, 9, MustFire).await;
    assert_eq!(
        result["results"]["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["o"]["value"] == "10")
            .count(),
        6
    );
    check(
        &fluree,
        &ledger,
        &format!("SELECT ?o WHERE {{ {body} }}"),
        9,
        MustFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        &format!("SELECT ?l ?o WHERE {{ {body} FILTER(?n + ?o > 21) }}"),
        2,
        MustFire,
    )
    .await;
    check(&fluree, &ledger, &format!("SELECT ?l ?o ?v WHERE {{ {body} BIND(?n + ?o AS ?v) }} ORDER BY ?l ?o LIMIT 4 OFFSET 2"), 4, MustFire).await;
    check(
        &fluree,
        &ledger,
        "SELECT ?l ?v WHERE { ?l ex:driver ?n . ?s ex:label ?v }",
        6,
        MustFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?l ?o WHERE { ?l ex:driver ?n . ?l ex:items ?o }",
        3,
        MustNotFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?l ?o WHERE { ?l ex:driver ?n . ?s ex:missing ?o }",
        0,
        MustNotFire,
    )
    .await;
    // The two annotation components are independent, as in S21/S22. Their
    // source comparisons still run for every joined pair.
    check(&fluree, &ledger, "SELECT ?x ?y WHERE { << ?a ex:edge ?b >> ex:source ?x . << ?c ex:edge ?d >> ex:source ?y . FILTER(STR(?x) > STR(?y)) }", 1, MustFire).await;

    let json_query = json!({"@context":{"ex":"http://example.org/"},
        "select":["?l","?s","?o"], "where":[
            {"@id":"?l","ex:driver":"?n"}, {"@id":"?s","ex:items":"?o"}]});
    let (spans, guard) = support::span_capture::init_test_tracing();
    let twin = support::query_jsonld(&fluree, &ledger, &json_query)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalized(&twin), normalized(&result));
    assert!(spans.has_event(EVENT));
    drop(guard);

    let mut json_count = json_query.clone();
    json_count["select"] = json!(["(as (count *) ?total)"]);
    let (spans, guard) = support::span_capture::init_test_tracing();
    let count = support::query_jsonld(&fluree, &ledger, &json_count)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(count["results"]["bindings"][0]["total"]["value"], "9");
    assert!(spans.has_event(COUNT_EVENT));
    drop(guard);

    let mut restricted = json_query.clone();
    restricted["from"] = json!(alias);
    restricted["opts"] = json!({"default-allow":true, "policy":[{
        "@id":"ex:hideItems", "f:required":true, "f:action":"f:view",
        "f:onProperty":[{"@id":"http://example.org/items"}], "f:allow":false
    }]});
    let (spans, guard) = support::span_capture::init_test_tracing();
    let hidden = fluree
        .query_connection(&restricted)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert!(normalized(&hidden).is_empty());
    assert!(
        !spans.has_event(EVENT),
        "policy-filtered scans must decline"
    );
    drop(guard);
    // A post-join volatile filter still runs for every pair during replay.
    check(
        &fluree,
        &ledger,
        &format!("SELECT ?l ?o WHERE {{ {body} FILTER(RAND() >= 0) }}"),
        9,
        MustFire,
    )
    .await;

    // More than one output batch, followed by the cache capacity boundary.
    // The planner drives these from the three-row predicate.
    let wide_alias = "replay/wide:main";
    let wide = fluree.create_ledger(wide_alias).await.unwrap();
    fluree
        .insert(
            wide,
            &json!({"@context":{"ex":"http://example.org/"},
        "@graph":[{"@id":"ex:a","ex:driver":1}, {"@id":"ex:b","ex:driver":2},
        {"@id":"ex:c","ex:driver":3}, {"@id":"ex:wide",
        "ex:fits":{"@list":(0..8192).collect::<Vec<_>>()},
        "ex:overflows":{"@list":(0..8193).collect::<Vec<_>>()}}]}),
        )
        .await
        .unwrap();
    fluree
        .reindex(wide_alias, ReindexOptions::default())
        .await
        .unwrap();
    let wide = fluree.ledger(wide_alias).await.unwrap();
    check(
        &fluree,
        &wide,
        "SELECT ?l ?v WHERE { ?l ex:driver ?n . ?s ex:fits ?v }",
        3 * 8192,
        MustFire,
    )
    .await;
    check(
        &fluree,
        &wide,
        "SELECT ?l ?v WHERE { ?l ex:driver ?n . ?s ex:overflows ?v }",
        3 * 8193,
        MustNotFire,
    )
    .await;
    // A small LIMIT must not eagerly drain or fill the right cache.
    check(
        &fluree,
        &wide,
        "SELECT ?l ?v WHERE { ?l ex:driver ?n . ?s ex:fits ?v } LIMIT 1",
        1,
        MustNotFire,
    )
    .await;

    let receipt = fluree
        .insert(
            ledger,
            &json!({"@context":{"ex":"http://example.org/"},
        "@id":"ex:d", "ex:driver":4}),
        )
        .await
        .unwrap();
    check(&fluree, &receipt.ledger, &q, 12, MustNotFire).await;
    check_count(
        &fluree,
        &receipt.ledger,
        &format!("SELECT (COUNT(*) AS ?total) WHERE {{ {body} }}"),
        12,
        false,
    )
    .await;
}

/// Reproducible local A/B probe benchmark. Run separately in dev-fast/release;
/// the fixture and expected counts are independent of StarBench and its ledger.
#[tokio::test(flavor = "current_thread")]
#[ignore = "local performance experiment; run with --profile dev-fast --ignored --nocapture"]
async fn independent_scan_replay_benchmark() {
    let _serial = SERIAL.lock().await;
    use std::fmt::Write;
    use std::time::Instant;
    let _reset = Reset;
    let dir = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    let n: usize = std::env::var("FLUREE_REPLAY_BENCH_KEYS")
        .map(|s| s.parse().expect("positive key count"))
        .unwrap_or(6000);
    assert!(n > 0);
    for i in 0..n {
        writeln!(ttl, "ex:l{i} ex:left {i} .").unwrap();
    }
    for i in 0..619 {
        writeln!(ttl, "ex:r{i} ex:right {i} .").unwrap();
    }
    std::fs::write(data.path().join("data.ttl"), ttl).unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = "replay/bench:main";
    fluree
        .create(alias)
        .import(data.path())
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .unwrap();
    let ledger = fluree.ledger(alias).await.unwrap();
    for (name, body, expected) in [
        ("cross-product", "?l ex:left ?x . ?r ex:right ?y", n * 619),
        (
            "filtered-product",
            "?l ex:left ?x . ?r ex:right ?y . FILTER(?x > ?y)",
            (0..n).map(|i| i.min(619)).sum(),
        ),
    ] {
        let query = format!("{PREFIX} SELECT (COUNT(*) AS ?n) WHERE {{ {body} }}");
        set_fast_paths_disabled(false);
        let (spans, tracing_guard) = support::span_capture::init_test_tracing();
        support::query_sparql(&fluree, &ledger, &query)
            .await
            .unwrap();
        assert!(
            spans.has_event(if name == "cross-product" {
                COUNT_EVENT
            } else {
                EVENT
            }),
            "benchmark must engage: {name}"
        );
        drop(tracing_guard);
        let mut times = [Vec::new(), Vec::new()];
        for round in 0..8 {
            // Alternate mode order; exclude the first warm-up pair.
            for mode in [round % 2, 1 - round % 2] {
                set_fast_paths_disabled(mode == 1);
                let start = Instant::now();
                let result = support::query_sparql(&fluree, &ledger, &query)
                    .await
                    .unwrap()
                    .to_sparql_json(&ledger.snapshot)
                    .unwrap();
                let elapsed = start.elapsed().as_secs_f64();
                assert_eq!(
                    result["results"]["bindings"][0]["n"]["value"],
                    expected.to_string()
                );
                if round != 0 {
                    times[mode].push(elapsed);
                }
            }
        }
        for t in &mut times {
            t.sort_by(f64::total_cmp);
        }
        println!(
            "{name}: optimized={:.6}s generic={:.6}s speedup={:.2}x (median of 7, {n} keys)",
            times[0][3],
            times[1][3],
            times[1][3] / times[0][3]
        );
    }
}
