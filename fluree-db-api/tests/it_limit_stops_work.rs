//! An outer `LIMIT` stops the batched lanes that gather input before probing.
//!
//! Property join reads its driver a chunk of subjects at a time, so a `LIMIT`
//! stops it after the first chunk, including when a `DISTINCT` between them
//! absorbs the row budget. Chunking must never change the rows or their
//! order: paging through a result with different `LIMIT`/`OFFSET` windows (so
//! different chunk sizes) has to reproduce the full drain exactly. OPTIONAL
//! sizes its first coalesced seed to the budget.

use crate::support::{genesis_ledger_for_fluree, span_capture};
use fluree_db_api::{FlureeBuilder, LedgerState, QueryInput, ReindexOptions};
use serde_json::{json, Value as JsonValue};

/// Enough typed subjects for three chunks (1024, 8192, rest).
const PEOPLE: usize = 12_000;

/// DISTINCT must still let a chain emit before its large probe window fills.
#[tokio::test]
async fn distinct_limit_stops_a_chain_after_one_small_probe() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/chain-stream-limit:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
    let graph: Vec<JsonValue> = (0..PEOPLE)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "ex:knows": {"@id": format!("ex:p{}", (i + 1) % PEOPLE)},
                "ex:bucket": i % 3,
            })
        })
        .collect();
    fluree
        .insert(ledger, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("seed chain");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let view = fluree.db(ledger_id).await.expect("view");
    let query = "PREFIX ex: <http://example.org/ns/> SELECT DISTINCT ?p ?fof \
                 WHERE { ?p ex:knows ?f . ?f ex:knows ?fof } LIMIT 1";
    let plan = fluree.explain_sparql(&view, query).await.expect("explain");
    assert!(
        plan["plan"]["physical"]
            .to_string()
            .contains("NestedLoopJoinOperator"),
        "{plan}"
    );
    let (spans, guard) = span_capture::init_test_tracing();
    assert_eq!(rows(&fluree, &view, query).await.len(), 1);
    drop(guard);
    let events = spans.find_events("join batched probe input");
    let input_rows: usize = events
        .iter()
        .map(|event| {
            event.fields["input_rows"]
                .parse::<usize>()
                .expect("input rows")
        })
        .sum();
    assert_eq!(events.len(), 1, "expected one probe: {events:?}");
    assert!(
        input_rows <= 1024,
        "expected one small probe, consumed {input_rows} rows: {events:?}"
    );

    // The hint is a planning choice: full drains and blocking modifiers keep
    // the hash join, including COUNT whose *output* has just one row.
    let chain = "PREFIX ex: <http://example.org/ns/> SELECT DISTINCT ?p ?fof \
                 WHERE { ?p ex:knows ?f . ?f ex:knows ?fof }";
    for query in [
        chain.to_string(),
        format!("{chain} ORDER BY ?p LIMIT 1"),
        format!("{chain} OFFSET 10000 LIMIT 1"),
        "PREFIX ex: <http://example.org/ns/> SELECT (COUNT(*) AS ?n) \
         WHERE { ?p ex:knows ?f . ?f ex:knows ?fof } LIMIT 1"
            .to_string(),
    ] {
        let plan = fluree.explain_sparql(&view, &query).await.expect("explain");
        assert!(
            plan["plan"]["physical"]
                .to_string()
                .contains("HashJoinOperator"),
            "{plan}"
        );
    }

    // OFFSET slices the streaming result, and each returned pair follows the
    // two edges. No reliance on the unrelated full-drain hash join's order.
    let prefix = rows(&fluree, &view, &format!("{chain} LIMIT 20")).await;
    assert_eq!(
        rows(&fluree, &view, &format!("{chain} OFFSET 5 LIMIT 10")).await,
        prefix[5..15]
    );
    for row in &prefix {
        let p: usize = row[0]
            .as_str()
            .unwrap()
            .strip_prefix("ex:p")
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(row[1], format!("ex:p{}", (p + 2) % PEOPLE));
    }

    // Known low-cardinality projections cannot fill LIMIT: keep throughput
    // planning rather than replacing the hash join for an unattainable prefix.
    let buckets = "PREFIX ex: <http://example.org/ns/> SELECT DISTINCT ?b \
                   WHERE { ?p ex:knows ?f . ?f ex:bucket ?b } LIMIT 4";
    let plan = fluree.explain_sparql(&view, buckets).await.unwrap();
    assert!(
        plan["plan"]["physical"]
            .to_string()
            .contains("HashJoinOperator"),
        "{plan}"
    );
    let mut got = rows(&fluree, &view, buckets).await;
    got.sort_by_key(ToString::to_string);
    assert_eq!(got, vec![json!([0]), json!([1]), json!([2])]);

    let offset_buckets = buckets.replace("LIMIT 4", "OFFSET 3 LIMIT 1");
    let plan = fluree.explain_sparql(&view, &offset_buckets).await.unwrap();
    assert!(
        plan["plan"]["physical"]
            .to_string()
            .contains("HashJoinOperator"),
        "OFFSET must contribute to the requested prefix: {plan}"
    );
    assert!(rows(&fluree, &view, &offset_buckets).await.is_empty());

    // A computed projection has no NDV estimate, so it keeps the startup hint.
    // It must still drain all windows when only three distinct rows survive.
    let computed_buckets = buckets.replace("SELECT DISTINCT ?b", "SELECT DISTINCT (?b + 0 AS ?c)");
    let (spans, guard) = span_capture::init_test_tracing();
    let mut got = rows(&fluree, &view, &computed_buckets).await;
    drop(guard);
    got.sort_by_key(ToString::to_string);
    assert_eq!(got, vec![json!([0]), json!([1]), json!([2])]);
    let inputs: Vec<usize> = spans
        .find_events("join batched probe input")
        .iter()
        .map(|e| e.fields["input_rows"].parse().unwrap())
        .collect();
    assert_eq!(inputs, vec![1024, 8192, PEOPLE - 9216]);

    let sparse = chain.replace(
        "?f ex:knows ?fof }",
        "?f ex:knows ?fof FILTER(STRENDS(STR(?p), \"p11999\")) }",
    );
    assert_eq!(
        rows(&fluree, &view, &format!("{sparse} LIMIT 1")).await,
        vec![json!(["ex:p11999", "ex:p1"])]
    );
}

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/ns/"})
}

fn person(i: usize) -> JsonValue {
    let mut node = json!({
        "@id": format!("ex:p{i}"),
        "@type": "ex:Person",
        "ex:name": format!("Person {i}"),
    });
    if i.is_multiple_of(3) {
        node["ex:nick"] = json!([format!("n{i}a"), format!("n{i}b")]);
    }
    if i.is_multiple_of(2) {
        node["ex:email"] = json!(format!("p{i}@example.org"));
    }
    if i.is_multiple_of(100) {
        node["ex:badge"] = json!(format!("b{i}"));
    }
    node
}

/// People interleaved with untyped subjects that share their predicates, so
/// the typed subjects' ids are not contiguous.
async fn seed_people(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> LedgerState {
    let mut ledger = genesis_ledger_for_fluree(fluree, ledger_id);
    for start in (0..PEOPLE).step_by(4000) {
        let graph: Vec<JsonValue> = (start..start + 4000)
            .flat_map(|i| {
                [
                    person(i),
                    json!({"@id": format!("ex:pet{i}"), "ex:name": format!("Pet {i}")}),
                ]
            })
            .collect();
        ledger = fluree
            .insert(ledger, &json!({"@context": ctx(), "@graph": graph}))
            .await
            .expect("seed")
            .ledger;
    }
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    ledger
}

async fn rows(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    sparql: &str,
) -> Vec<JsonValue> {
    let result = fluree
        .query(view, QueryInput::Sparql(sparql))
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    jsonld.as_array().expect("rows").clone()
}

async fn assert_property_join(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    sparql: &str,
) {
    let plan = fluree.explain_sparql(view, sparql).await.expect("explain");
    let physical = plan["plan"]["physical"].to_string();
    assert!(
        physical.contains("PropertyJoinOperator"),
        "expected a property join: {physical}"
    );
}

/// Subjects the property join read from its driver, and whether it read the
/// driver to the end.
async fn driver_read(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    sparql: &str,
) -> (usize, bool) {
    let (spans, guard) = span_capture::init_test_tracing();
    rows(fluree, view, sparql).await;
    drop(guard);
    let events = spans.find_events("property_join: complete");
    let [event] = events.as_slice() else {
        panic!("expected one property join, got {events:?}");
    };
    let subjects = event.fields["subjects"].parse().expect("subjects");
    (subjects, event.fields["driver_exhausted"] == "true")
}

#[tokio::test]
async fn limit_stops_the_driver_after_one_chunk() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/pj-stream-limit:main";
    seed_people(&fluree, ledger_id).await;
    let view = fluree.db(ledger_id).await.expect("view");

    let star = |select: &str, where_: &str, modifiers: &str| {
        format!("PREFIX ex: <http://example.org/ns/>\n{select} WHERE {{ {where_} }} {modifiers}")
    };
    let dense = "?p a ex:Person ; ex:name ?n";
    let sparse = "?p a ex:Person ; ex:badge ?b";
    assert_property_join(&fluree, &view, &star("SELECT *", dense, "")).await;
    assert_property_join(&fluree, &view, &star("SELECT *", sparse, "")).await;

    assert_eq!(
        driver_read(&fluree, &view, &star("SELECT ?p ?n", dense, "")).await,
        (PEOPLE, true)
    );
    for (modifiers, expected_subjects) in
        [("LIMIT 1", 1), ("LIMIT 10", 10), ("OFFSET 5 LIMIT 10", 15)]
    {
        assert_eq!(
            driver_read(&fluree, &view, &star("SELECT ?p ?n", dense, modifiers)).await,
            (expected_subjects, false),
            "dense star should probe only its requested prefix: {modifiers}"
        );
    }
    // DISTINCT absorbs the row budget, so it relies on the chunk schedule
    // alone. The sparse star's whole result is smaller than one output batch,
    // so it relies on rows being handed over as each chunk finishes.
    for (select, where_, modifiers) in [
        ("SELECT ?p ?n", dense, "LIMIT 5"),
        ("SELECT DISTINCT ?n", dense, "LIMIT 1"),
        ("SELECT ?p ?b", sparse, "LIMIT 2"),
    ] {
        let (subjects, exhausted) =
            driver_read(&fluree, &view, &star(select, where_, modifiers)).await;
        assert!(
            subjects <= 1024 && !exhausted,
            "{select} {{ {where_} }} {modifiers}: read {subjects} driver subjects \
             (exhausted: {exhausted})"
        );
    }
}

#[tokio::test]
async fn chunked_rows_match_the_full_drain() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/pj-stream-pages:main";
    seed_people(&fluree, ledger_id).await;
    let view = fluree.db(ledger_id).await.expect("view");

    let star = "PREFIX ex: <http://example.org/ns/>\n\
                SELECT ?p ?n ?k WHERE { ?p a ex:Person ; ex:name ?n ; ex:nick ?k \
                FILTER(!STRENDS(?n, \"7\")) }";
    assert_property_join(&fluree, &view, star).await;

    let full = rows(&fluree, &view, star).await;
    let mut expected: Vec<JsonValue> = (0..PEOPLE)
        .filter(|i| i % 3 == 0 && i % 10 != 7)
        .flat_map(|i| {
            ["a", "b"].map(|k| {
                json!([
                    format!("ex:p{i}"),
                    format!("Person {i}"),
                    format!("n{i}{k}")
                ])
            })
        })
        .collect();
    let mut sorted = full.clone();
    sorted.sort_by_key(ToString::to_string);
    expected.sort_by_key(ToString::to_string);
    assert_eq!(sorted, expected, "full drain content");

    // Each window starts the join over with a different first chunk; the
    // pages must still line up with the full drain row for row.
    let mut paged = Vec::new();
    for (offset, limit) in [
        (0, 1),
        (1, 9),
        (10, 290),
        (300, 1500),
        (1800, 3000),
        (4800, 100_000),
    ] {
        paged.extend(
            rows(
                &fluree,
                &view,
                &format!("{star} OFFSET {offset} LIMIT {limit}"),
            )
            .await,
        );
    }
    assert_eq!(paged, full, "paged rows differ from the full drain");
}

/// A bound object inside an `@list` indexes once per list position, so the
/// driver revisits subjects after earlier chunks have been emitted. Each
/// subject must still join once.
#[tokio::test]
async fn driver_revisiting_a_subject_joins_it_once() {
    const TAGGED: usize = 3000;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/pj-stream-revisit:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
    let graph: Vec<JsonValue> = (0..TAGGED)
        .map(|i| {
            json!({
                "@id": format!("ex:t{i}"),
                "ex:name": format!("Tagged {i}"),
                "ex:tags": {"@list": [{"@id": "ex:red"}, {"@id": "ex:blue"}, {"@id": "ex:red"}]},
            })
        })
        .collect();
    fluree
        .insert(ledger, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("seed");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let view = fluree.db(ledger_id).await.expect("view");

    let star = "PREFIX ex: <http://example.org/ns/>\n\
                SELECT ?t ?n WHERE { ?t ex:tags ex:red ; ex:name ?n }";
    assert_property_join(&fluree, &view, star).await;
    let mut got = rows(&fluree, &view, star).await;
    got.sort_by_key(ToString::to_string);
    let mut expected: Vec<JsonValue> = (0..TAGGED)
        .map(|i| json!([format!("ex:t{i}"), format!("Tagged {i}")]))
        .collect();
    expected.sort_by_key(ToString::to_string);
    assert_eq!(got, expected);
}

/// Subjects deleted after `t` have the highest ids, so after a reindex their
/// rows live only in the last leaflet's history, past its last live row. The
/// star walk's leaflet skip must still find them at `t`. Two things keep it
/// sound: index writers widen a leaflet's keys over its history, and the walk
/// never skips a leaflet it has to replay. This fails only with both removed.
#[tokio::test]
async fn historical_star_walk_finds_subjects_deleted_since() {
    const RETIRED: usize = 5;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/pj-stream-history:main";
    let ledger = seed_people(&fluree, ledger_id).await;
    let retired: Vec<JsonValue> = (0..RETIRED)
        .map(|i| {
            json!({
                "@id": format!("ex:r{i}"),
                "@type": "ex:Retired",
                "ex:name": format!("Retired {i}"),
            })
        })
        .collect();
    let ledger = fluree
        .insert(ledger, &json!({"@context": ctx(), "@graph": retired}))
        .await
        .expect("insert retired")
        .ledger;
    let before_delete = ledger.t();
    fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": {"@id": "?r", "@type": "ex:Retired", "ex:name": "?n"},
                "delete": {"@id": "?r", "@type": "ex:Retired", "ex:name": "?n"}
            }),
        )
        .await
        .expect("delete retired");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let star = "PREFIX ex: <http://example.org/ns/>\n\
                SELECT ?r ?n WHERE { ?r a ex:Retired ; ex:name ?n }";
    let current = fluree.db(ledger_id).await.expect("current view");
    assert!(rows(&fluree, &current, star).await.is_empty());

    let historical = fluree
        .db_at_t(ledger_id, before_delete)
        .await
        .expect("historical view");
    assert_property_join(&fluree, &historical, star).await;
    let (spans, guard) = span_capture::init_test_tracing();
    let mut got = rows(&fluree, &historical, star).await;
    drop(guard);
    let events = spans.find_events("property_join: complete");
    let [event] = events.as_slice() else {
        panic!("expected one property join, got {events:?}");
    };
    assert_eq!(event.fields["used_spot_star_walk"], "true");

    got.sort_by_key(ToString::to_string);
    let expected: Vec<JsonValue> = (0..RETIRED)
        .map(|i| json!([format!("ex:r{i}"), format!("Retired {i}")]))
        .collect();
    assert_eq!(got, expected);
}

/// Novelty on subjects that land in later chunks: retracts, re-asserted
/// values, and a subject that exists only in novelty.
#[tokio::test]
async fn novelty_in_later_chunks_matches_the_reindexed_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/pj-stream-novelty:main";
    let ledger = seed_people(&fluree, ledger_id).await;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": {"@id": "ex:p5000", "@type": "?t"},
                "delete": {"@id": "ex:p5000", "@type": "?t"}
            }),
        )
        .await
        .expect("untype p5000")
        .ledger;
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:p9000", "ex:name": "Second 9000"},
                    {"@id": "ex:p12000", "@type": "ex:Person", "ex:name": "Person 12000"}
                ]
            }),
        )
        .await
        .expect("novelty inserts")
        .ledger;
    fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": {"@id": "ex:p11990", "ex:name": "?old"},
                "delete": {"@id": "ex:p11990", "ex:name": "?old"},
                "insert": {"@id": "ex:p11990", "ex:name": "Renamed 11990"}
            }),
        )
        .await
        .expect("rename");

    let star = "PREFIX ex: <http://example.org/ns/>\n\
                SELECT ?p ?n WHERE { ?p a ex:Person ; ex:name ?n }";
    let novelty_view = fluree.db(ledger_id).await.expect("novelty view");
    assert_property_join(&fluree, &novelty_view, star).await;
    let under_novelty = rows(&fluree, &novelty_view, star).await;

    let has = |p: &str, n: &str| under_novelty.contains(&json!([p, n]));
    assert_eq!(under_novelty.len(), PEOPLE + 1);
    assert!(!under_novelty.iter().any(|r| r[0] == "ex:p5000"));
    assert!(has("ex:p9000", "Person 9000") && has("ex:p9000", "Second 9000"));
    assert!(has("ex:p11990", "Renamed 11990") && !has("ex:p11990", "Person 11990"));
    assert!(has("ex:p12000", "Person 12000"));

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let indexed_view = fluree.db(ledger_id).await.expect("indexed view");
    let mut indexed = rows(&fluree, &indexed_view, star).await;
    let mut under_novelty = under_novelty;
    indexed.sort_by_key(ToString::to_string);
    under_novelty.sort_by_key(ToString::to_string);
    assert_eq!(under_novelty, indexed);
}

/// Every required row of an OPTIONAL yields an output row, so a `LIMIT`
/// budget sizes the first coalesced seed instead of the whole required side.
#[tokio::test]
async fn optional_limit_coalesces_one_small_seed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/optional-limit-seed:main";
    seed_people(&fluree, ledger_id).await;
    let view = fluree.db(ledger_id).await.expect("view");

    let query = "PREFIX ex: <http://example.org/ns/>\n\
                 SELECT ?p ?e WHERE { ?p ex:name ?n OPTIONAL { ?p ex:email ?e } }";
    for (modifiers, max_seed) in [("", 2 * PEOPLE), ("LIMIT 10", 1024)] {
        let (spans, guard) = span_capture::init_test_tracing();
        rows(&fluree, &view, &format!("{query} {modifiers}")).await;
        drop(guard);
        let seeds: Vec<usize> = spans
            .find_events("optional batched probe complete")
            .iter()
            .map(|e| e.fields["rows"].parse().expect("rows"))
            .collect();
        assert!(
            !seeds.is_empty() && seeds.iter().sum::<usize>() <= max_seed,
            "{modifiers:?}: seeds {seeds:?}"
        );
    }
}
