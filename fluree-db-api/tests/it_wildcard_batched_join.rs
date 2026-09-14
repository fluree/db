//! Standalone: routing capture and the process-global fast-path switch.
#![cfg(feature = "native")]
mod support;

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, ReindexOptions};
use fluree_db_ledger::LedgerState;
use serde_json::{json, Value};

const PREFIX: &str = "PREFIX ex: <http://example.org/> ";
const EVENT: &str = "batched wildcard join engaged";

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
async fn wildcard_joins_preserve_facts_multiplicity_and_fallbacks() {
    assert!(std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none());
    let _reset = Reset;
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = "wildcard/joins:main";
    let ledger = fluree.create_ledger(alias).await.unwrap();
    fluree.insert(ledger, &json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a", "ex:name":"A",
             "ex:edge":{"@id":"ex:b", "@annotation":{"@id":"ex:ann", "ex:source":"paper"}},
             "ex:label":[{"@value":"bonjour", "@language":"fr"}, {"@value":"hello", "@language":"en"}],
             "ex:items":{"@list":[10,10,20]}},
            {"@id":"ex:b", "ex:name":"B", "ex:owner":{"@id":"ex:a"}},
            {"@id":"ex:c", "ex:edge":{"@id":"ex:a"}, "ex:other":{"@id":"ex:a"}}
        ]
    })).await.unwrap();
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .unwrap();
    let ledger = fluree.ledger(alias).await.unwrap();
    use Routing::*;
    let outgoing = "SELECT ?s ?p ?o WHERE { VALUES ?s { ex:a ex:a ex:b } ?s ?p ?o }";
    let rows = check(&fluree, &ledger, outgoing, 16, MustFire).await;
    // Two equal list values at different indices must survive, for each of
    // the two duplicate driving rows. Language tags must survive too.
    let bindings = rows["results"]["bindings"].as_array().unwrap();
    assert_eq!(
        bindings
            .iter()
            .filter(|r| r["p"]["value"] == "http://example.org/items" && r["o"]["value"] == "10")
            .count(),
        4
    );
    assert_eq!(
        bindings
            .iter()
            .filter(|r| r["o"]["xml:lang"] == "fr")
            .count(),
        2
    );
    check(
        &fluree,
        &ledger,
        "SELECT ?s ?p ?o WHERE { VALUES ?o { ex:a ex:a ex:b } ?s ?p ?o }",
        7,
        MustFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?s ?p WHERE { VALUES ?o { ex:c ex:c } ?s ?p ?o }",
        0,
        MustFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { VALUES ?s { ex:a ex:a ex:b } ?s ?p ?o }",
        16,
        MustFire,
    )
    .await;
    check(&fluree, &ledger,
        "SELECT ?p ?o WHERE { VALUES ?s { ex:a ex:b } ?s ?p ?o FILTER(STR(?p) = 'http://example.org/name') }", 2, MustFire).await;
    check(
        &fluree,
        &ledger,
        "SELECT ?p ?o WHERE { VALUES ?s { ex:a ex:b } ?s ?p ?o FILTER(?s = ex:b) }",
        2,
        MustFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?s ?p ?o WHERE { VALUES ?s { ex:a ex:b } ?s ?p ?o } ORDER BY ?s ?p ?o LIMIT 3",
        3,
        MustFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?p ?o WHERE { VALUES ?s { ex:a } ?s ?p ?s }",
        0,
        MustNotFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?p WHERE { VALUES ?s { ex:a } ?s ?p ?p }",
        0,
        MustNotFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?o WHERE { VALUES (?s ?p) { (ex:a ex:name) } ?s ?p ?o }",
        1,
        MustNotFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?s ?p WHERE { VALUES ?o { 'A' } ?s ?p ?o }",
        1,
        MustNotFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?s ?p ?o WHERE { VALUES ?s { ex:a UNDEF ex:b } ?s ?p ?o }",
        21,
        MustFire,
    )
    .await;
    // A single matched fact fans out across several output batches. Resuming
    // must retain the duplicate-driver position as well as the cursor position.
    let many = format!(
        "SELECT ?p ?o WHERE {{ VALUES ?s {{ {} }} ?s ?p ?o }}",
        "ex:a ".repeat(2100)
    );
    check(&fluree, &ledger, &many, 14700, MustFire).await;
    check(
        &fluree,
        &ledger,
        "SELECT ?x WHERE { VALUES ?s { ex:a ex:b } ?s ?p ?o BIND(CONCAT(STR(?s), STR(?p)) AS ?x) }",
        9,
        MustFire,
    )
    .await;
    // The reifier's encoding predicates must not leak into this wildcard.
    let annotation = check(
        &fluree,
        &ledger,
        "SELECT ?d ?e WHERE { VALUES ?s { ex:a ex:a } << ?s ex:edge ex:b >> ?d ?e }",
        2,
        MustFire,
    )
    .await;
    assert_eq!(
        annotation["results"]["bindings"][0]["d"]["value"],
        "http://example.org/source"
    );

    // JSON-LD twin uses the shared IR and must engage the same lane.
    let json_query = json!({"@context":{"ex":"http://example.org/"},
        "select":["?s","?p","?o"], "where":[{"@id":"?s", "?p":"?o"}],
        "values":["?s", [
            {"@value":"ex:a","@type":"@id"},
            {"@value":"ex:a","@type":"@id"},
            {"@value":"ex:b","@type":"@id"}]]});
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let twin = support::query_jsonld(&fluree, &ledger, &json_query)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalized(&rows), normalized(&twin));
    assert!(spans.has_event(EVENT), "JSON-LD twin must fire");
    drop(_guard);

    // Incoming JSON-LD twin, including duplicate driving keys.
    let mut incoming = json_query.clone();
    incoming["values"][0] = json!("?o");
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let incoming_rows = support::query_jsonld(&fluree, &ledger, &incoming)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalized(&incoming_rows).len(), 7);
    assert!(spans.has_event(EVENT), "incoming JSON-LD twin must fire");
    drop(_guard);

    // UNDEF and literals can be interleaved with batchable keys. No pending
    // fallback row may lose its retained left batch when a flush takes ownership.
    check(
        &fluree,
        &ledger,
        "SELECT ?s ?p ?o WHERE { VALUES ?o { ex:a 'A' ex:a } ?s ?p ?o }",
        7,
        MustFire,
    )
    .await;
    check(
        &fluree,
        &ledger,
        "SELECT ?p ?o WHERE { VALUES ?s { ex:ann ex:ann } ?s ?p ?o }",
        2,
        MustFire,
    )
    .await;

    let mut inspection = json_query.clone();
    inspection["values"] = json!(["?s", [
        {"@value":"ex:ann","@type":"@id"}, {"@value":"ex:ann","@type":"@id"}]]);
    inspection["opts"] = json!({"includeSystemFacts":true});
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let exposed = support::query_jsonld(&fluree, &ledger, &inspection)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert!(normalized(&exposed).len() > 2);
    assert!(spans.has_event(EVENT));
    set_fast_paths_disabled(true);
    let generic = support::query_jsonld(&fluree, &ledger, &inspection)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalized(&exposed), normalized(&generic));
    set_fast_paths_disabled(false);
    drop(_guard);

    // A restrictive property policy must use the filtered scan, even at HEAD.
    let mut restricted = json_query.clone();
    restricted["from"] = json!(alias);
    restricted["opts"] = json!({"default-allow":true, "policy":[{
        "@id":"ex:hideName", "f:required":true, "f:action":"f:view",
        "f:onProperty":[{"@id":"http://example.org/name"}], "f:allow":false
    }]});
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let hidden = fluree
        .query_connection(&restricted)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalized(&hidden).len(), 13);
    assert!(!spans.has_event(EVENT), "restricted policy must decline");
    assert!(hidden["results"]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r["p"]["value"] != "http://example.org/name"));
    drop(_guard);
    // Multiple ledgers have independent dictionaries and must decline.
    let other_alias = "wildcard/other:main";
    let other = fluree.create_ledger(other_alias).await.unwrap();
    fluree
        .insert(
            other,
            &json!({"@context":{"ex":"http://example.org/"},
        "@id":"ex:outside", "ex:extra":99}),
        )
        .await
        .unwrap();
    fluree
        .reindex(other_alias, ReindexOptions::default())
        .await
        .unwrap();
    let mut dataset = json_query.clone();
    dataset["from"] = json!([alias, other_alias]);
    dataset["values"] = json!(["?s", [
        {"@value":"ex:b","@type":"@id"}, {"@value":"ex:b","@type":"@id"}]]);
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let union = fluree
        .query_connection(&dataset)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalized(&union).len(), 4);
    assert!(!spans.has_event(EVENT), "multi-ledger dataset must decline");
    drop(_guard);
    let mut reasoning = json_query.clone();
    reasoning["reasoning"] = json!("rdfs");
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let inferred = support::query_jsonld(&fluree, &ledger, &reasoning)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalized(&inferred), normalized(&twin));
    assert!(!spans.has_event(EVENT), "reasoning must decline");
    drop(_guard);
    let base_t = ledger.t();

    // Any live novelty declines before accumulating: both directions see
    // fresh asserts and retracts through the ordinary overlay-aware scan.
    let receipt = fluree
        .update(
            ledger,
            &json!({
                "@context":{"ex":"http://example.org/"},
                "where":{"@id":"ex:a","ex:name":"?old"},
                "delete":[{"@id":"ex:a","ex:name":"?old"}],
                "insert":[{"@id":"ex:a","ex:name":"NEW"},
                          {"@id":"ex:new","ex:edge":{"@id":"ex:a"}}]
            }),
        )
        .await
        .unwrap();
    let rows = check(&fluree, &receipt.ledger, outgoing, 16, MustNotFire).await;
    assert!(rows["results"]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["o"]["value"] == "NEW"));
    check(
        &fluree,
        &receipt.ledger,
        "SELECT ?s ?p WHERE { VALUES ?o { ex:a ex:a } ?s ?p ?o }",
        8,
        MustNotFire,
    )
    .await;
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .unwrap();
    let head = fluree.ledger(alias).await.unwrap();
    // Past snapshots decline after a new index contains the retraction.
    let mut historical = json_query.clone();
    historical["from"] = json!({"@id":alias, "t":base_t});
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let past = fluree
        .query_connection(&historical)
        .await
        .unwrap()
        .to_sparql_json(&head.snapshot)
        .unwrap();
    assert_eq!(normalized(&past), normalized(&twin));
    assert!(!spans.has_event(EVENT), "past snapshot must decline");
    drop(_guard);
    historical["from"] = json!(format!("{alias}@t:{base_t}"));
    historical["to"] = json!(format!("{alias}@t:latest"));
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let history = fluree
        .query_connection(&historical)
        .await
        .unwrap()
        .to_sparql_json(&head.snapshot)
        .unwrap();
    assert!(normalized(&history).len() > 16);
    assert!(!spans.has_event(EVENT), "history range must decline");
}

/// Reproducible local A/B probe benchmark. Run separately in dev-fast/release;
/// the fixture and expected counts are independent of StarBench and its ledger.
#[tokio::test(flavor = "current_thread")]
#[ignore = "local performance experiment; run with --profile dev-fast --ignored --nocapture"]
async fn wildcard_join_benchmark() {
    use std::fmt::Write;
    use std::time::Instant;
    let _reset = Reset;
    let dir = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    let n: usize = std::env::var("FLUREE_WILDCARD_BENCH_KEYS")
        .map(|s| s.parse().expect("positive key count"))
        .unwrap_or(20_000);
    assert!(n > 0);
    for i in 0..n {
        writeln!(ttl, "ex:driver{i} ex:pick ex:node{i} .\nex:node{i} ex:p1 1 ; ex:p2 2 ; ex:p3 ex:value .\nex:missing{i} ex:marker true .").unwrap();
    }
    std::fs::write(data.path().join("data.ttl"), ttl).unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = "wildcard/bench:main";
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
        ("outgoing", "?driver ex:pick ?s . ?s ?p ?o", n * 3),
        ("incoming-misses", "?o ex:marker true . ?s ?p ?o", 0),
        ("incoming-hits", "?o ex:p1 1 . ?s ?p ?o", n),
    ] {
        let query = format!("{PREFIX} SELECT (COUNT(*) AS ?n) WHERE {{ {body} }}");
        set_fast_paths_disabled(false);
        let (spans, tracing_guard) = support::span_capture::init_test_tracing();
        support::query_sparql(&fluree, &ledger, &query)
            .await
            .unwrap();
        assert!(spans.has_event(EVENT), "benchmark must engage: {name}");
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
            "{name}: batched={:.6}s generic={:.6}s speedup={:.2}x (median of 7, {n} keys)",
            times[0][3],
            times[1][3],
            times[1][3] / times[0][3]
        );
    }
}
