//! TI3-style object joins over typed decimals. Dataset setup is outside timings.
use crate::support::{assert_index_defaults, genesis_ledger, rebuild_and_publish_index};
use fluree_db_api::{Fluree, FlureeBuilder};
use serde_json::{json, Value};

const LEDGER: &str = "ti3:main";
const QUERY: &str = "SELECT * WHERE { ?y <http://example.org/p1> ?x . ?z <http://example.org/p2> ?x . ?u <http://example.org/p3> ?x } LIMIT 1000";

fn decimal(n: usize) -> Value {
    json!({"@value": format!("{n}.0"), "@type": "http://www.w3.org/2001/XMLSchema#decimal"})
}

async fn seed(fluree: &Fluree, probe: usize, noise: usize, misses: usize) {
    let ledger = genesis_ledger(fluree, LEDGER);
    let mut graph = Vec::new();
    for i in 0..8 {
        graph.push(json!({"@id":format!("ex:y{i}"), "ex:p1":decimal(230 + i % 2)}));
    }
    if misses > 0 {
        graph.push(json!({"@id":"ex:missY", "ex:p1":decimal(300)}));
    }
    for i in 0..1_000 {
        graph.push(
            json!({"@id":format!("ex:z{i}"), "ex:p2":decimal(if i < 2 {230+i} else if i < misses+2 {300} else {1000+i})}),
        );
    }
    for i in 0..probe {
        graph.push(
            // Keep p3's average fanout above p2's, matching TI3's join order.
            json!({"@id":format!("ex:u{i}"), "ex:p3":decimal(if i < 2 {230+i} else {100000+i % (probe / 20).max(1)})}),
        );
    }
    for i in 0..noise {
        graph.push(json!({"@id":format!("ex:n{i}"), "ex:noise":decimal(i)}));
    }
    fluree
        .insert(
            ledger,
            &json!({"@context":{"ex":"http://example.org/"}, "@graph":graph}),
        )
        .await
        .unwrap();
    rebuild_and_publish_index(fluree, LEDGER).await;
}

fn assert_join(result: &Value) {
    let rows = result["results"]["bindings"].as_array().unwrap();
    assert_eq!(rows.len(), 8, "{result}");
    let mut seen = Vec::new();
    for row in rows {
        let y = row["y"]["value"]
            .as_str()
            .unwrap()
            .strip_prefix("http://example.org/y")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        assert_eq!(row["z"]["value"], format!("http://example.org/z{}", y % 2));
        assert_eq!(row["u"]["value"], format!("http://example.org/u{}", y % 2));
        assert_eq!(
            row["x"]["value"].as_str().unwrap().parse::<f64>().unwrap(),
            (230 + y % 2) as f64
        );
        seen.push(y);
    }
    seen.sort();
    assert_eq!(seen, (0..8).collect::<Vec<_>>());
}

#[tokio::test]
async fn ti3_decimal_join_preserves_multiplicity() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, 50_000, 100, 500).await;
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    let db = crate::support::graphdb_from_ledger(&ledger);
    let result = db
        .query(&fluree)
        .sparql(QUERY)
        .track_all()
        .execute_tracked()
        .await
        .unwrap();
    assert_eq!(result.status, 200);
    assert_join(&result.result);
    // The first join emits 508 rows, 500 of which miss p3. Reopening a broad
    // p3 scan for every row costs >12 fuel; point lookups stay below 3.
    assert!(
        result.fuel.unwrap() < 5.0,
        "repeated predicate scans: {result:?}"
    );
    let from_query = QUERY.replace("WHERE", &format!("FROM <{LEDGER}> WHERE"));
    let from_result = fluree
        .query_from()
        .sparql(&from_query)
        .track_all()
        .execute_tracked()
        .await
        .unwrap();
    assert_eq!(from_result.status, 200);
    assert_join(&from_result.result);
    assert!(from_result.fuel.unwrap() < 5.0);
}

async fn subjects(fluree: &Fluree, sparql: &str) -> Vec<String> {
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    let db = crate::support::graphdb_from_ledger(&ledger);
    let result = db
        .query(fluree)
        .sparql(sparql)
        .track_all()
        .execute_tracked()
        .await
        .unwrap();
    assert_eq!(result.status, 200);
    let mut ids: Vec<_> = result.result["results"]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row["s"]["value"]
                .as_str()
                .unwrap()
                .strip_prefix("http://example.org/")
                .unwrap()
                .to_owned()
        })
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn decimal_seeks_preserve_scales_mixed_types_and_novelty() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, LEDGER);
    fluree
        .insert(
            ledger,
            &json!({
                "@context":{"ex":"http://example.org/", "xsd":"http://www.w3.org/2001/XMLSchema#"},
                "@graph":[
                    {"@id":"ex:a", "ex:p":{"@value":"22.0", "@type":"xsd:decimal"}},
                    {"@id":"ex:b", "ex:p":{"@value":"22.000", "@type":"xsd:decimal"}},
                    {"@id":"ex:unrelated", "ex:other":{"@value":"999.0", "@type":"xsd:decimal"}}
                ]
            }),
        )
        .await
        .unwrap();
    let typed = "PREFIX ex: <http://example.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?s WHERE { ?s ex:p \"22.00\"^^xsd:decimal }";
    let untyped = "PREFIX ex: <http://example.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?s WHERE { VALUES ?v { \"22.00\"^^xsd:decimal \"-1.0\"^^xsd:decimal } ?s ex:p ?v }";
    let absent = typed.replace("22.00", "999.0");
    let original_t = fluree.ledger(LEDGER).await.unwrap().t();
    // Novelty-only and persisted variants; scale-normalized handles must not
    // collapse distinct subject rows. A handle in another predicate is irrelevant.
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, LEDGER).await;
        }
        assert_eq!(subjects(&fluree, typed).await, ["a", "b"]);
        assert_eq!(subjects(&fluree, untyped).await, ["a", "b"]);
        assert!(subjects(&fluree, &absent).await.is_empty());
    }
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({
                "@context":{"ex":"http://example.org/", "xsd":"http://www.w3.org/2001/XMLSchema#"},
                "@graph":[
                    {"@id":"ex:integer", "ex:p":22},
                    {"@id":"ex:double", "ex:p":{"@value":"22.0", "@type":"xsd:double"}},
                    {"@id":"ex:fresh", "ex:p":{"@value":"999.0", "@type":"xsd:decimal"}}
                ]
            }),
        )
        .await
        .unwrap();
    // The mixed observed set must veto untyped decimal narrowing immediately,
    // before indexing. Explicit decimal constraints still match only decimals.
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, LEDGER).await;
        }
        assert_eq!(subjects(&fluree, typed).await, ["a", "b"]);
        assert_eq!(
            subjects(&fluree, untyped).await,
            ["a", "b", "double", "integer"]
        );
        assert_eq!(subjects(&fluree, &absent).await, ["fresh"]);
    }
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    fluree
        .update(
            ledger,
            &json!({
                "@context":{"ex":"http://example.org/", "xsd":"http://www.w3.org/2001/XMLSchema#"},
                "delete":[{"@id":"ex:a", "ex:p":{"@value":"22.000", "@type":"xsd:decimal"}}]
            }),
        )
        .await
        .unwrap();
    assert_eq!(subjects(&fluree, typed).await, ["b"]);
    assert_eq!(subjects(&fluree, untyped).await, ["b", "double", "integer"]);
    rebuild_and_publish_index(&fluree, LEDGER).await;
    let historical = fluree.db(LEDGER).await.unwrap().as_of(original_t);
    for query in [typed, untyped] {
        let result = historical
            .query(&fluree)
            .sparql(query)
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        assert_eq!(result.status, 200);
        let json = result.result;
        let mut ids: Vec<_> = json["results"]["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["s"]["value"].as_str().unwrap())
            .collect();
        ids.sort();
        assert_eq!(ids, ["http://example.org/a", "http://example.org/b"]);
    }
}

#[tokio::test]
async fn decimal_join_keeps_equal_overflow_integers() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, LEDGER);
    fluree.insert(ledger, &json!({
        "@context":{"ex":"http://example.org/", "xsd":"http://www.w3.org/2001/XMLSchema#"},
        "@graph":[
            {"@id":"ex:decimal", "ex:p":{"@value":"100000000000000000000.0", "@type":"xsd:decimal"}},
            {"@id":"ex:integer", "ex:p":{"@value":"100000000000000000000", "@type":"xsd:integer"}}
        ]
    })).await.unwrap();
    let query = "PREFIX ex: <http://example.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?s WHERE { VALUES ?v { \"100000000000000000000.00\"^^xsd:decimal \"-1.0\"^^xsd:decimal } ?s ex:p ?v }";
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, LEDGER).await;
        }
        assert_eq!(subjects(&fluree, query).await, ["decimal", "integer"]);
    }
}

/// TI3_PROBE and TI3_NOISE control predicate sizes. FLUREE_HASH_JOIN is the
/// existing diagnostic override; leave unset for the planner's own decision.
#[tokio::test]
#[ignore = "local TI3 scaling probe"]
async fn ti3_local_scaling() {
    assert_index_defaults();
    let read = |name, default| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(default)
    };
    let probe = read("TI3_PROBE", 50_000);
    let noise = read("TI3_NOISE", 100_000);
    let misses = read("TI3_MISSES", 0);
    assert!(probe >= 2 && misses <= 998);
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, probe, noise, misses).await;
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    let db = crate::support::graphdb_from_ledger(&ledger);
    let mut times = Vec::new();
    let mut fuel = None;
    let shape = std::env::var("TI3_SHAPE").unwrap_or_default();
    let sparql = match shape.as_str() {
        "anchor" => "SELECT * WHERE { ?y <http://example.org/p1> ?x } LIMIT 1000",
        "first_join" => "SELECT * WHERE { ?y <http://example.org/p1> ?x . ?z <http://example.org/p2> ?x } LIMIT 1000",
        "point" => "SELECT * WHERE { ?s <http://example.org/p3> \"230.0\"^^<http://www.w3.org/2001/XMLSchema#decimal> } LIMIT 1000",
        _ => QUERY,
    };
    if std::env::var_os("TI3_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("fluree_db_query=trace")
            .try_init();
    }
    for iteration in 0..6 {
        let start = std::time::Instant::now();
        let result = db
            .query(&fluree)
            .sparql(sparql)
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert_eq!(result.status, 200);
        if sparql == QUERY {
            assert_join(&result.result);
        } else {
            assert_eq!(
                result.result["results"]["bindings"]
                    .as_array()
                    .unwrap()
                    .len(),
                match shape.as_str() {
                    "point" => 1,
                    "anchor" => 8 + usize::from(misses > 0),
                    _ => 8 + misses,
                }
            );
        }
        if iteration > 0 {
            times.push(elapsed);
        }
        fuel = result.fuel;
    }
    times.sort();
    eprintln!(
        "TI3 probe={probe} noise={noise} misses={misses} shape={shape}: median={:?}, fuel={fuel:?}",
        times[2]
    );
}
