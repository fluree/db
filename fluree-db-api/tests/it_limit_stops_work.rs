//! An outer `LIMIT` stops the batched lanes that gather input before probing.
//!
//! Property join reads its driver a chunk of subjects at a time, so a `LIMIT`
//! stops it after the first chunk, including when a `DISTINCT` between them
//! absorbs the row budget. Chunking must never change the rows or their
//! order: paging through a result with different `LIMIT`/`OFFSET` windows (so
//! different chunk sizes) has to reproduce the full drain exactly. OPTIONAL
//! sizes its first coalesced seed to the budget.

use crate::support::{genesis_ledger_for_fluree, span_capture};
use fluree_db_api::{FlureeBuilder, IndexerConfig, LedgerState, QueryInput, ReindexOptions};
use serde_json::{json, Value as JsonValue};

/// Enough typed subjects for three chunks (1024, 8192, rest).
const PEOPLE: usize = 12_000;

/// DISTINCT must still let a chain emit before its large probe window fills.
#[tokio::test]
async fn distinct_limit_stops_a_chain_after_one_small_probe() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/chain-stream-limit:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id).await;
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
    assert_eq!(inputs, vec![4, 32, 256, 732, 8192, PEOPLE - 9216]);

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
    let mut ledger = genesis_ledger_for_fluree(fluree, ledger_id).await;
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

/// After its first chunks, a selective star sizes the next from the rows found
/// per subject so far, instead of growing eightfold past what `LIMIT` needs.
#[tokio::test]
async fn a_selective_star_sizes_later_chunks_from_its_yield() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/pj-stream-yield:main";
    seed_people(&fluree, ledger_id).await;
    let view = fluree.db(ledger_id).await.expect("view");

    // One person in 100 has a badge, so 30 rows need about 3,000 subjects.
    // Chunks of 30, 240, 1,920 and then 15,360 would read all 12,000.
    let sparql = "PREFIX ex: <http://example.org/ns/>\n\
                  SELECT ?p ?b WHERE { ?p a ex:Person ; ex:badge ?b } LIMIT 30";
    assert_eq!(rows(&fluree, &view, sparql).await.len(), 30);
    let (subjects, exhausted) = driver_read(&fluree, &view, sparql).await;
    assert!(
        subjects < PEOPLE / 2 && !exhausted,
        "read {subjects} driver subjects (exhausted: {exhausted})"
    );
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
/// subject must still join once. A JSON-LD query: it reads list entries as
/// values, where SPARQL reads a list's head and cells.
#[tokio::test]
async fn driver_revisiting_a_subject_joins_it_once() {
    const TAGGED: usize = 3000;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/pj-stream-revisit:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id).await;
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

    let star = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?t", "?n"],
        "where": {"@id": "?t", "ex:tags": {"@id": "ex:red"}, "ex:name": "?n"}
    });
    let plan = fluree.explain(&view, &star).await.expect("explain");
    let physical = plan["plan"]["physical"].to_string();
    assert!(
        physical.contains("PropertyJoinOperator"),
        "expected a property join: {physical}"
    );
    let result = fluree
        .query(&view, QueryInput::from(&star))
        .await
        .expect("query");
    let mut got = result
        .to_jsonld(&view.snapshot)
        .expect("to_jsonld")
        .as_array()
        .expect("rows")
        .clone();
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

/// Left rows each batched object probe took, in order.
fn object_probe_windows(spans: &span_capture::SpanStore) -> Vec<usize> {
    assert!(
        spans.has_event("join batched object flush complete"),
        "expected the bound-object lane"
    );
    spans
        .find_events("join batched probe input")
        .iter()
        .map(|e| e.fields["input_rows"].parse().expect("input rows"))
        .collect()
}

/// A bound-object probe reads only the leaflets holding its objects, so a
/// `LIMIT` sizes its first window directly. Holding back a 1,024-row window
/// per join would drain far more of the driving scan than the rows asked for;
/// the windows still end where that one did, so none reads past it.
#[tokio::test]
async fn limit_sizes_the_bound_object_lanes_first_window() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/object-lane-limit:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id).await;
    let graph: Vec<JsonValue> = (0..PEOPLE)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "ex:tag": format!("t{i}"),
                "ex:label": format!("t{i}"),
                "ex:likes": {"@id": format!("ex:c{i}")},
                "ex:follows": {"@id": format!("ex:c{i}")},
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

    let strings = "?a ex:tag ?v . ?b ex:label ?v";
    let refs = "?a ex:likes ?v . ?b ex:follows ?v";
    let select = |where_: &str, modifiers: &str| {
        format!("PREFIX ex: <http://example.org/ns/> SELECT ?a ?b WHERE {{ {where_} }} {modifiers}")
    };
    for where_ in [strings, refs] {
        let query = select(where_, "LIMIT 5");
        let plan = fluree.explain_sparql(&view, &query).await.expect("explain");
        assert!(
            plan["plan"]["physical"]
                .to_string()
                .contains("NestedLoopJoinOperator"),
            "{plan}"
        );
        let (spans, guard) = span_capture::init_test_tracing();
        let got = rows(&fluree, &view, &query).await;
        drop(guard);
        assert_eq!(got.len(), 5, "{where_}");
        assert!(got.iter().all(|row| row[0] == row[1]), "{where_}: {got:?}");
        assert_eq!(object_probe_windows(&spans), vec![5], "{where_}");
    }

    // The same join through JSON-LD.
    let jsonld = json!({
        "@context": ctx(),
        "select": ["?a", "?b"],
        "where": [{"@id": "?a", "ex:tag": "?v"}, {"@id": "?b", "ex:label": "?v"}],
        "limit": 5,
    });
    let (spans, guard) = span_capture::init_test_tracing();
    let result = fluree
        .query(&view, QueryInput::JsonLd(&jsonld))
        .await
        .expect("query");
    drop(guard);
    let got = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    assert_eq!(got.as_array().expect("rows").len(), 5);
    assert_eq!(object_probe_windows(&spans), vec![5]);

    // One left row in a hundred survives, so the windows grow until the
    // `LIMIT` is met, within the first 1,024 rows, and the answer is the full
    // drain's.
    let sparse = format!("{strings} FILTER(STRENDS(STR(?b), \"50\"))");
    let (spans, guard) = span_capture::init_test_tracing();
    let got = rows(&fluree, &view, &select(&sparse, "LIMIT 2")).await;
    drop(guard);
    assert_eq!(object_probe_windows(&spans), vec![2, 16, 128, 878]);
    let all = rows(&fluree, &view, &select(&sparse, "")).await;
    assert_eq!(all.len(), PEOPLE / 100);
    assert_eq!(got.len(), 2);
    assert!(got.iter().all(|row| all.contains(row)), "{got:?}");
}

/// Members of each of the two hubs in [`seed_hubs`].
const HUB_MEMBERS: usize = 3000;
/// Pointers in [`seed_hubs`]; every `HUB_POINTER_EVERY`th one names a hub.
const POINTERS: usize = 21_000;
const HUB_POINTER_EVERY: usize = 2100;

/// Ten pointers at two hubs of [`HUB_MEMBERS`] members each, among pointers
/// at nothing and members of nothing pointed at. Sized so both the `LIMIT`
/// and the full drain take the nested-loop join.
async fn seed_hubs(fluree: &fluree_db_api::Fluree, ledger_id: &str) {
    let ledger = genesis_ledger_for_fluree(fluree, ledger_id).await;
    let pointers: Vec<JsonValue> = (0..POINTERS)
        .map(|i| {
            let target = if i.is_multiple_of(HUB_POINTER_EVERY) {
                format!("ex:hub{}", (i / HUB_POINTER_EVERY) % 2)
            } else {
                format!("ex:t{i}")
            };
            json!({"@id": format!("ex:a{i}"), "ex:pointsTo": {"@id": target}})
        })
        .collect();
    let ledger = fluree
        .insert(ledger, &json!({"@context": ctx(), "@graph": pointers}))
        .await
        .expect("seed pointers")
        .ledger;
    let members: Vec<JsonValue> = (0..2 * HUB_MEMBERS)
        .map(|k| json!({"@id": format!("ex:m{k}"), "ex:memberOf": {"@id": format!("ex:hub{}", k % 2)}}))
        .chain((0..19_000).map(|k| {
            json!({"@id": format!("ex:f{k}"), "ex:memberOf": {"@id": format!("ex:lonely{k}")}})
        }))
        .collect();
    fluree
        .insert(ledger, &json!({"@context": ctx(), "@graph": members}))
        .await
        .expect("seed members");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
}

/// Rows the bound-object flushes matched, and rows built from those matches.
fn object_probe_matched_and_built(spans: &span_capture::SpanStore) -> (usize, usize) {
    let sum = |event: &str, field: &str| -> usize {
        spans
            .find_events(event)
            .iter()
            .map(|e| e.fields[field].parse::<usize>().expect("row count"))
            .sum()
    };
    (
        sum("join batched object flush complete", "matched_rows"),
        sum("join batched object rows built", "rows"),
    )
}

/// One bound object can match far more rows than a `LIMIT` reads. The lane
/// builds its rows a batch at a time from the matches, so the consumer stops
/// it after one batch, and the rows and their order stay those of the full
/// drain however the windows and batches fall.
#[tokio::test]
async fn limit_builds_one_batch_of_a_hub_objects_matches() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/object-lane-hub:main";
    seed_hubs(&fluree, ledger_id).await;
    let view = fluree.db(ledger_id).await.expect("view");

    let join = "?a ex:pointsTo ?v . ?m ex:memberOf ?v";
    let select = |where_: &str, modifiers: &str| {
        format!("PREFIX ex: <http://example.org/ns/> SELECT ?a ?m WHERE {{ {where_} }} {modifiers}")
    };
    for modifiers in ["", "LIMIT 5"] {
        let plan = fluree
            .explain_sparql(&view, &select(join, modifiers))
            .await
            .expect("explain");
        assert!(
            plan["plan"]["physical"]
                .to_string()
                .contains("NestedLoopJoinOperator"),
            "{modifiers:?}: {plan}"
        );
    }

    let (spans, guard) = span_capture::init_test_tracing();
    let full = rows(&fluree, &view, &select(join, "")).await;
    drop(guard);
    let hub_rows = (POINTERS / HUB_POINTER_EVERY) * HUB_MEMBERS;
    assert_eq!(
        object_probe_matched_and_built(&spans),
        (hub_rows, hub_rows),
        "a full drain builds every match"
    );
    let mut expected: Vec<JsonValue> = (0..POINTERS)
        .step_by(HUB_POINTER_EVERY)
        .flat_map(|i| {
            let hub = (i / HUB_POINTER_EVERY) % 2;
            (0..2 * HUB_MEMBERS)
                .filter(move |k| k % 2 == hub)
                .map(move |k| json!([format!("ex:a{i}"), format!("ex:m{k}")]))
        })
        .collect();
    let mut sorted = full.clone();
    sorted.sort_by_key(ToString::to_string);
    expected.sort_by_key(ToString::to_string);
    assert_eq!(sorted, expected);

    // A hub pointer's flush matches a whole hub; five rows build one batch.
    let (spans, guard) = span_capture::init_test_tracing();
    let got = rows(&fluree, &view, &select(join, "LIMIT 5")).await;
    drop(guard);
    assert_eq!(got, full[..5]);
    let (matched, built) = object_probe_matched_and_built(&spans);
    assert!(matched >= HUB_MEMBERS, "matched {matched}");
    assert!(built <= 1000, "built {built} of {matched} matched rows");

    // Pages across a batch boundary and across two left rows' matches.
    for (offset, limit) in [(0, 1024), (990, 30), (HUB_MEMBERS - 10, 20), (5995, 10)] {
        assert_eq!(
            rows(
                &fluree,
                &view,
                &select(join, &format!("OFFSET {offset} LIMIT {limit}"))
            )
            .await,
            full[offset..offset + limit],
            "OFFSET {offset} LIMIT {limit}"
        );
    }

    // Filters drop matches as their rows are built: one reads the matched
    // subject alone, one the whole row.
    fn last(value: &JsonValue) -> Option<char> {
        value.as_str().expect("iri").chars().last()
    }
    type Keep = fn(&JsonValue) -> bool;
    let filters: [(&str, Keep); 2] = [
        ("FILTER(STRENDS(STR(?m), \"7\"))", |row| {
            last(&row[1]) == Some('7')
        }),
        (
            "FILTER(STRENDS(STR(?m), SUBSTR(STR(?a), STRLEN(STR(?a)))))",
            |row| last(&row[0]) == last(&row[1]),
        ),
    ];
    for (filter, keep) in filters {
        let filtered = format!("{join} {filter}");
        let kept: Vec<JsonValue> = full.iter().filter(|row| keep(row)).cloned().collect();
        assert_eq!(kept.len(), hub_rows / 10, "{filter}");
        assert_eq!(
            rows(&fluree, &view, &select(&filtered, "")).await,
            kept,
            "{filter}"
        );
        let (spans, guard) = span_capture::init_test_tracing();
        let got = rows(&fluree, &view, &select(&filtered, "LIMIT 5")).await;
        drop(guard);
        assert_eq!(got, kept[..5], "{filter}");
        let (matched, built) = object_probe_matched_and_built(&spans);
        assert!(matched >= HUB_MEMBERS, "{filter}: matched {matched}");
        assert!(built <= 1000, "{filter}: built {built} of {matched}");
    }

    // The same join through JSON-LD.
    let jsonld = json!({
        "@context": ctx(),
        "select": ["?a", "?m"],
        "where": [{"@id": "?a", "ex:pointsTo": "?v"}, {"@id": "?m", "ex:memberOf": "?v"}],
        "limit": 5,
    });
    let (spans, guard) = span_capture::init_test_tracing();
    let result = fluree
        .query(&view, QueryInput::JsonLd(&jsonld))
        .await
        .expect("query");
    drop(guard);
    let got = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    assert_eq!(got.as_array().expect("rows")[..], full[..5]);
    let (matched, built) = object_probe_matched_and_built(&spans);
    assert!(matched >= HUB_MEMBERS, "matched {matched}");
    assert!(built <= 1000, "built {built} of {matched} matched rows");
}

/// A flush is charged for the matches it read, not for the rows a `LIMIT`
/// let it build from them: five rows of a hub cost the hub. Set against the
/// same query on a pointer at nothing, which reads the same leaflets and
/// matches no row.
#[tokio::test]
async fn limit_still_pays_for_the_hub_it_read() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/object-lane-hub-fuel:main";
    seed_hubs(&fluree, ledger_id).await;
    let view = fluree.db(ledger_id).await.expect("view");

    let mut fuel = Vec::new();
    for (pointer, matched) in [("a0", HUB_MEMBERS), ("a1", 0)] {
        let sparql = format!(
            "PREFIX ex: <http://example.org/ns/> SELECT ?m \
             WHERE {{ ex:{pointer} ex:pointsTo ?v . ?m ex:memberOf ?v }} LIMIT 5"
        );
        let (spans, guard) = span_capture::init_test_tracing();
        let tracked = view
            .query(&fluree)
            .sparql(&sparql)
            .track_all()
            .execute_tracked()
            .await
            .expect("tracked query");
        drop(guard);
        assert_eq!(tracked.status, 200);
        let (read, built) = object_probe_matched_and_built(&spans);
        assert_eq!(read, matched, "{pointer}");
        assert!(built <= 1000, "{pointer}: built {built} of {read}");
        fuel.push(tracked.fuel.expect("fuel"));
    }

    // Each match read costs a thousandth of a fuel. Charging the one batch
    // of rows built instead would come to a third of this.
    let read_fuel = HUB_MEMBERS as f64 / 1000.0;
    assert!(
        fuel[0] > fuel[1] + 0.8 * read_fuel,
        "reading a {HUB_MEMBERS}-member hub charged {} fuel, {} with no match",
        fuel[0],
        fuel[1]
    );
}

/// The matches the bound-object lane holds for one string object.
const STRING_MATCH_BOUND: usize = 1 << 16;
/// Subjects labelled `"common"` in the string-hub test: more than that bound.
const STRING_HUB: usize = 76_000;
const STRING_HUB_LEAFLET_ROWS: usize = 2000;
/// Labels retracted to take the hub under that bound, then asserted to take
/// it back over.
const STRING_HUB_RETRACTED: usize = 10_500;
const STRING_HUB_ADDED: usize = 40;

const STRING_HUB_JOIN: &str = "?a ex:tag ?v . ?m ex:label ?v";

fn string_hub_select(where_: &str, modifiers: &str) -> String {
    format!("PREFIX ex: <http://example.org/ns/> SELECT ?a ?v ?m WHERE {{ {where_} }} {modifiers}")
}

fn labelled(prefix: &str, ids: std::ops::Range<usize>, label: &str) -> Vec<JsonValue> {
    ids.map(|k| json!({"@id": format!("ex:{prefix}{k}"), "ex:label": label}))
        .collect()
}

/// Small leaflets and leaves, so a long run of one label has whole ones past
/// the lane's bound.
async fn reindex_in_small_leaves(fluree: &fluree_db_api::Fluree, ledger_id: &str) {
    let config = IndexerConfig::default()
        .with_leaflet_rows(STRING_HUB_LEAFLET_ROWS)
        .with_leaf_rows(2 * STRING_HUB_LEAFLET_ROWS);
    fluree
        .reindex(
            ledger_id,
            ReindexOptions::default().with_indexer_config(config),
        )
        .await
        .expect("reindex");
}

/// Matches the bound-object flushes held to build rows from, and objects they
/// left to the per-row scan.
fn object_probe_held_and_streamed(spans: &span_capture::SpanStore) -> (usize, usize) {
    let events = spans.find_events("join batched object flush complete");
    assert!(!events.is_empty(), "expected the bound-object lane");
    let sum = |field: &str| -> usize {
        events
            .iter()
            .map(|e| e.fields[field].parse::<usize>().expect("count"))
            .sum()
    };
    (sum("matched_rows"), sum("streamed_objects"))
}

fn object_probe_leaflets_read(spans: &span_capture::SpanStore) -> usize {
    spans
        .find_events("join batched object flush complete")
        .iter()
        .map(|e| e.fields["leaflets_read"].parse::<usize>().expect("count"))
        .sum()
}

/// A hub's indexed run is left where it passes the lane's bound: the leaflets
/// wholly past that point are not read.
fn assert_hub_run_left_at_the_bound(spans: &span_capture::SpanStore, surface: &str) {
    let read = object_probe_leaflets_read(spans);
    let to_the_bound = STRING_MATCH_BOUND / STRING_HUB_LEAFLET_ROWS + 2;
    let whole_run = STRING_HUB / STRING_HUB_LEAFLET_ROWS;
    assert!(
        read <= to_the_bound && to_the_bound < whole_run,
        "{surface}: read {read} leaflets of a {whole_run}-leaflet run"
    );
}

/// The subjects carrying each label at one point of the string-hub test, and
/// whether the hub is past the lane's bound there.
struct StringHubPhase {
    name: &'static str,
    aaa: Vec<String>,
    hub: Vec<String>,
    zzz: Vec<String>,
    streamed: bool,
}

/// Drain the string-hub join and check it against `phase`: the rows, the path
/// the hub took, and that pages are slices of the drain. Returns the drain and
/// the trace of a one-row read of the hub.
async fn check_string_hub(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    phase: &StringHubPhase,
) -> (Vec<JsonValue>, span_capture::SpanStore) {
    let StringHubPhase {
        name,
        aaa,
        hub,
        zzz,
        streamed,
    } = phase;
    for modifiers in ["", "LIMIT 5"] {
        let query = string_hub_select(STRING_HUB_JOIN, modifiers);
        let plan = fluree.explain_sparql(view, &query).await.expect("explain");
        assert!(
            plan["plan"]["physical"]
                .to_string()
                .contains("NestedLoopJoinOperator"),
            "{name} {modifiers:?}: {plan}"
        );
    }

    let (spans, guard) = span_capture::init_test_tracing();
    let full = rows(fluree, view, &string_hub_select(STRING_HUB_JOIN, "")).await;
    drop(guard);
    // Two left rows share the hub; a held hub is counted for each.
    let small = aaa.len() + zzz.len();
    let held_and_streamed = if *streamed {
        (small, 1)
    } else {
        (small + 2 * hub.len(), 0)
    };
    assert_eq!(
        object_probe_held_and_streamed(&spans),
        held_and_streamed,
        "{name}"
    );

    let mut expected: Vec<JsonValue> = [
        ("ex:seed1", "aaa-small", aaa),
        ("ex:seed1", "common", hub),
        ("ex:seed1", "zzz-small", zzz),
        ("ex:seed2", "common", hub),
    ]
    .into_iter()
    .flat_map(|(a, v, subjects)| subjects.iter().map(move |m| json!([a, v, m])))
    .collect();
    expected.sort_by_cached_key(ToString::to_string);
    let mut sorted = full.clone();
    sorted.sort_by_cached_key(ToString::to_string);
    assert!(
        sorted == expected,
        "{name}: {} rows, expected {}",
        sorted.len(),
        expected.len()
    );

    // Each left row's matches come out together.
    let left_row = |row: &JsonValue| (row[0].clone(), row[1].clone());
    let mut runs: Vec<std::ops::Range<usize>> = Vec::new();
    for (i, row) in full.iter().enumerate() {
        match runs.last_mut() {
            Some(run) if left_row(&full[run.start]) == left_row(row) => run.end = i + 1,
            _ => runs.push(i..i + 1),
        }
    }
    let mut run_lens: Vec<usize> = runs.iter().map(ExactSizeIterator::len).collect();
    run_lens.sort_unstable();
    let mut left_row_matches = vec![aaa.len(), zzz.len(), hub.len(), hub.len()];
    left_row_matches.sort_unstable();
    assert_eq!(run_lens, left_row_matches, "{name}");

    // Pages across each boundary between two left rows' matches, and across a
    // batch boundary inside the hub's.
    let mut pages = vec![(0, 1), (0, 5)];
    for run in &runs {
        if run.start > 0 {
            pages.push((run.start - 2, 5));
        }
        if run.len() > 2000 {
            pages.push((run.start + 990, 30));
        }
    }
    for (offset, limit) in pages {
        let query = string_hub_select(STRING_HUB_JOIN, &format!("OFFSET {offset} LIMIT {limit}"));
        assert_eq!(
            rows(fluree, view, &query).await,
            full[offset..(offset + limit).min(full.len())],
            "{name}: OFFSET {offset} LIMIT {limit}"
        );
    }

    // Every left row on the hub: one row of it is asked for.
    let hub_only = "PREFIX ex: <http://example.org/ns/> SELECT ?v ?m \
                    WHERE { ex:seed2 ex:tag ?v . ?m ex:label ?v } LIMIT 1";
    let (spans, guard) = span_capture::init_test_tracing();
    let got = rows(fluree, view, hub_only).await;
    drop(guard);
    let first = runs
        .iter()
        .find(|run| full[run.start][0] == "ex:seed2")
        .expect("seed2's matches")
        .start;
    assert_eq!(got, vec![json!([full[first][1], full[first][2]])], "{name}");
    if *streamed {
        assert_eq!(object_probe_held_and_streamed(&spans), (0, 1), "{name}");
        assert_eq!(
            object_probe_matched_and_built(&spans).1,
            0,
            "{name}: the per-row scan builds the hub's rows"
        );
    } else {
        assert_eq!(
            object_probe_held_and_streamed(&spans),
            (hub.len(), 0),
            "{name}"
        );
    }
    (full, spans)
}

/// A string shared by more subjects than the bound-object lane holds for one
/// object is left to the per-row scan, which streams: a `LIMIT` on a common
/// label does not wait for the label's whole run to be collected. Which path
/// an object takes follows from its live match count alone, so pages stay
/// slices of the full drain while novelty takes the label under the bound and
/// back over it.
#[tokio::test]
async fn a_common_string_object_streams_from_the_per_row_scan() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/object-lane-string-hub:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id).await;
    let mut graph = vec![
        json!({"@id": "ex:seed1", "ex:tag": ["aaa-small", "common", "zzz-small"]}),
        json!({"@id": "ex:seed2", "ex:tag": "common"}),
        json!({"@id": "ex:seed3", "ex:tag": "nowhere"}),
    ];
    graph.extend(labelled("a", 0..3, "aaa-small"));
    graph.extend(labelled("m", 0..STRING_HUB, "common"));
    graph.extend(labelled("z", 0..3, "zzz-small"));
    fluree
        .insert(ledger, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("seed");
    reindex_in_small_leaves(&fluree, ledger_id).await;

    let ids = |prefix: &str, ids: std::ops::Range<usize>| -> Vec<String> {
        ids.map(|k| format!("ex:{prefix}{k}")).collect()
    };
    let mut phase = StringHubPhase {
        name: "indexed",
        aaa: ids("a", 0..3),
        hub: ids("m", 0..STRING_HUB),
        zzz: ids("z", 0..3),
        streamed: true,
    };
    let view = fluree.db(ledger_id).await.expect("indexed view");
    let (full, hub_read) = check_string_hub(&fluree, &view, &phase).await;
    assert_hub_run_left_at_the_bound(&hub_read, "sparql");

    // A filter reaches the hub's rows through the per-row scan and the other
    // labels' through the lane.
    let filtered = format!("{STRING_HUB_JOIN} FILTER(STRENDS(STR(?m), \"1\"))");
    let kept: Vec<JsonValue> = full
        .iter()
        .filter(|row| row[2].as_str().expect("iri").ends_with('1'))
        .cloned()
        .collect();
    assert_eq!(kept.len(), 2 * (STRING_HUB / 10) + 2);
    let got = rows(&fluree, &view, &string_hub_select(&filtered, "")).await;
    assert!(got == kept, "filtered: {} rows", got.len());
    assert_eq!(
        rows(&fluree, &view, &string_hub_select(&filtered, "LIMIT 5")).await,
        kept[..5]
    );

    // The same joins through JSON-LD.
    let jsonld = json!({
        "@context": ctx(),
        "select": ["?a", "?v", "?m"],
        "where": [{"@id": "?a", "ex:tag": "?v"}, {"@id": "?m", "ex:label": "?v"}],
        "limit": 5,
    });
    let result = fluree
        .query(&view, QueryInput::JsonLd(&jsonld))
        .await
        .expect("query");
    let got = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    assert_eq!(got.as_array().expect("rows")[..], full[..5]);
    let jsonld = json!({
        "@context": ctx(),
        "select": ["?v", "?m"],
        "where": [{"@id": "ex:seed2", "ex:tag": "?v"}, {"@id": "?m", "ex:label": "?v"}],
        "limit": 1,
    });
    let (spans, guard) = span_capture::init_test_tracing();
    let result = fluree
        .query(&view, QueryInput::JsonLd(&jsonld))
        .await
        .expect("query");
    drop(guard);
    let got = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    assert_eq!(got.as_array().expect("rows").len(), 1);
    assert_eq!(object_probe_held_and_streamed(&spans), (0, 1));
    assert_hub_run_left_at_the_bound(&spans, "json-ld");

    // Retractions take the hub under the bound: the lane holds what is left.
    let ledger = fluree.ledger(ledger_id).await.expect("indexed ledger");
    let receipt = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "delete": labelled("m", 0..STRING_HUB_RETRACTED, "common")
            }),
        )
        .await
        .expect("retract hub labels");
    phase.name = "retracted under the bound";
    phase.hub = ids("m", STRING_HUB_RETRACTED..STRING_HUB);
    phase.streamed = false;
    let view = fluree.db(ledger_id).await.expect("retracted view");
    check_string_hub(&fluree, &view, &phase).await;

    // Asserts take it back over, past the last of its indexed rows.
    let mut inserts = labelled("n", 0..STRING_HUB_ADDED, "common");
    inserts.push(json!({"@id": "ex:fresh", "ex:label": "aaa-small"}));
    fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "delete": {"@id": "ex:z0", "ex:label": "zzz-small"},
                "insert": inserts
            }),
        )
        .await
        .expect("assert hub labels");
    phase.name = "asserted over the bound";
    phase.aaa.push("ex:fresh".to_string());
    phase.hub.extend(ids("n", 0..STRING_HUB_ADDED));
    phase.zzz.remove(0);
    phase.streamed = true;
    let view = fluree.db(ledger_id).await.expect("asserted view");
    check_string_hub(&fluree, &view, &phase).await;

    reindex_in_small_leaves(&fluree, ledger_id).await;
    phase.name = "reindexed";
    let view = fluree.db(ledger_id).await.expect("reindexed view");
    check_string_hub(&fluree, &view, &phase).await;
}
