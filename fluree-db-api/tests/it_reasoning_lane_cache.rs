//! Reasoning lanes whose routing depends on what an earlier query left on
//! the reasoning materialization: the overlay ops a lane translated.
//!
//! Those translations live on the materialization, which lives in the
//! process-wide reasoning cache (16 entries). Among many concurrent reasoning
//! tests, a sibling can evict it between two queries; the rebuild starts with
//! no translations. Its own binary keeps the materializations alive here fewer
//! than the cache holds.
//!
//! What index maintenance leaves in that cache is checked here for the same
//! reason: an entry a sibling evicted would read as one never made.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, QueryInput, ReindexOptions};
use serde_json::{json, Value};
use support::genesis_ledger_for_fluree;
use support::reasoning_lanes::{
    ctx, query_rows, sorted, sparql, OBJECT_LANE, SPARQL_PREFIXES, SUBJECT_LANE,
};
use support::span_capture::{init_test_tracing, SpanStore};

const PER_ROW_EVENT: &str = "join batched probe: too few rows for the overlay extent; per-row";

/// `mentored` derived `ex:knows` edges `s{i} → o{i}`. `ex:board` ranks
/// `s0` and levels the first `wide` subjects; `o0` has an age and the first
/// `wide` objects a tier. The markers bind the probe key from a pattern of
/// their own, so the join is a nested loop, not a star. Stored `ex:knows`
/// edges outnumber the markers in the planner's stats (derived facts aren't
/// in them), so the markers drive.
fn derived_extent_data(mentored: usize, wide: usize) -> Value {
    let mut graph = vec![
        json!({"@id": "ex:mentors", "rdfs:subPropertyOf": {"@id": "ex:knows"}}),
        json!({"@id": "ex:board", "ex:ranks": {"@id": "ex:s0"}}),
        json!({"@id": "ex:o0", "ex:age": 30}),
    ];
    for i in 0..4 * wide {
        graph.push(json!({"@id": format!("ex:k{i}"), "ex:knows": {"@id": format!("ex:j{i}")}}));
    }
    for i in 0..mentored {
        graph.push(json!({"@id": format!("ex:s{i}"), "ex:mentors": {"@id": format!("ex:o{i}")}}));
        if i < wide {
            graph.push(json!({"@id": "ex:board", "ex:levels": {"@id": format!("ex:s{i}")}}));
            graph.push(json!({"@id": format!("ex:o{i}"), "ex:tier": 1}));
        }
    }
    json!({"@context": ctx(), "@graph": graph})
}

/// `[?x ?z]` for a marker pattern joined to `?x ex:knows ?z`.
fn knows_query(marker: Value, sparql_marker: &str) -> (Value, String) {
    (
        json!({
            "@context": ctx(),
            "select": ["?x", "?z"],
            "where": [marker, {"@id": "?x", "ex:knows": "?z"}],
            "reasoning": "owl2rl"
        }),
        sparql(&format!(
            "SELECT ?x ?z WHERE {{ {sparql_marker} . ?x ex:knows ?z }}"
        )),
    )
}

/// A query's rows, whether it flushed `lane`, whether a flush handed its
/// rows to the per-row path, and its span names.
async fn routed_rows(
    fluree: &Fluree,
    view: &GraphDb,
    store: &SpanStore,
    lane: &str,
    input: QueryInput<'_>,
) -> (Vec<Value>, bool, bool, Vec<&'static str>) {
    let flushes = store.find_spans(lane).len();
    let per_row = store.find_events(PER_ROW_EVENT).len();
    let spans = store.span_names().len();
    let rows = query_rows(fluree, view, input).await;
    (
        rows,
        store.find_spans(lane).len() > flushes,
        store.find_events(PER_ROW_EVENT).len() > per_row,
        store.span_names()[spans..].to_vec(),
    )
}

/// A cold lane would translate its predicate's whole derived extent before
/// the first probe. A final flush with too few rows to repay that runs per
/// row; a wide input takes the lane, and once its ops are translated a
/// selective input takes it too.
#[tokio::test(flavor = "current_thread")]
async fn a_cold_lane_leaves_a_selective_input_per_row() {
    const MENTORED: usize = 400;
    const WIDE: usize = 60;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-selective:main";
    let warmup_id = "it/reasoning-selective-warmup:main";
    for id in [warmup_id, ledger_id] {
        let ledger = genesis_ledger_for_fluree(&fluree, id).await;
        fluree
            .insert(ledger, &derived_extent_data(MENTORED, WIDE))
            .await
            .expect("insert");
        fluree
            .reindex(id, ReindexOptions::default())
            .await
            .expect("reindex");
    }
    let selective = [
        (
            OBJECT_LANE,
            knows_query(json!({"@id": "?z", "ex:age": 30}), "?z ex:age 30"),
        ),
        (
            SUBJECT_LANE,
            knows_query(
                json!({"@id": "ex:board", "ex:ranks": "?x"}),
                "ex:board ex:ranks ?x",
            ),
        ),
    ];
    let wide = [
        (
            OBJECT_LANE,
            knows_query(json!({"@id": "?z", "ex:tier": 1}), "?z ex:tier 1"),
        ),
        (
            SUBJECT_LANE,
            knows_query(
                json!({"@id": "ex:board", "ex:levels": "?x"}),
                "ex:board ex:levels ?x",
            ),
        ),
    ];
    let one = vec![json!(["ex:s0", "ex:o0"])];
    let all_wide = sorted(
        (0..WIDE)
            .map(|i| json!([format!("ex:s{i}"), format!("ex:o{i}")]))
            .collect(),
    );

    let (store, guard) = init_test_tracing();
    // Register every callsite on a twin ledger (its cache keys are its own),
    // then recompute interest before measuring.
    let warmup = fluree.db(warmup_id).await.expect("warmup view");
    for (_, (jsonld, _)) in selective.iter().chain(&wide) {
        query_rows(&fluree, &warmup, QueryInput::JsonLd(jsonld)).await;
    }
    tracing::callsite::rebuild_interest_cache();

    let view = fluree.db(ledger_id).await.expect("view");
    for (phase, queries, expected, takes_lane) in [
        ("cold", &selective, &one, false),
        ("wide", &wide, &all_wide, true),
        ("warm", &selective, &one, true),
    ] {
        for (lane, (jsonld, sparql)) in queries {
            for (surface, input) in [
                ("JSON-LD", QueryInput::JsonLd(jsonld)),
                ("SPARQL", QueryInput::Sparql(sparql)),
            ] {
                let label = format!("{phase} {lane} ({surface})");
                let (rows, flushed, per_row, spans) =
                    routed_rows(&fluree, &view, &store, lane, input).await;
                assert_eq!(&rows, expected, "{label}: rows");
                assert_eq!(
                    (flushed, per_row),
                    (takes_lane, !takes_lane),
                    "{label}: (flushed the lane, ran per row); this query's spans: {spans:?}"
                );
            }
        }
    }
    drop(guard);
}

const DECLINE_EVENT: &str = "join batched probe: overlay plan declined; per-row";

const REROUTE_MARKERS: usize = 3000;
const REROUTE_RULE_INPUTS: [usize; 4] = [3, 7, 1500, 2999];

/// `ex:board` levels `REROUTE_MARKERS` subjects. Stored `ex:caseId`s index the
/// predicate and outnumber the markers in the planner's stats, so the markers
/// drive; a datalog rule derives `ex:caseId`s for early and late markers.
fn reroute_data() -> Value {
    let mut graph = Vec::new();
    for i in 0..REROUTE_MARKERS {
        graph.push(json!({"@id": "ex:board", "ex:levels": {"@id": format!("ex:s{i}")}}));
    }
    for i in 0..4000 {
        graph.push(json!({"@id": format!("ex:k{i}"), "ex:caseId": format!("stored-k{i}")}));
    }
    for i in 0..10 {
        graph.push(json!({"@id": format!("ex:s{i}"), "ex:caseId": format!("stored-s{i}")}));
    }
    for i in REROUTE_RULE_INPUTS {
        graph.push(json!({"@id": format!("ex:s{i}"), "ex:parent": {"@id": format!("ex:p{i}")}}));
    }
    json!({"@context": ctx(), "@graph": graph})
}

/// The rule derives `ex:caseId` strings no dictionary holds, so translating
/// the predicate's overlay fails and the deferred plan declines. `LIMIT`
/// starts the flush schedule small, so the first flush is not the last: it
/// hands its rows to the per-row path while left rows remain, and the rest
/// follow per row. Once the failed translation is cached, a rerun declines
/// before accumulating anything.
#[tokio::test(flavor = "current_thread")]
async fn a_deferred_plan_declining_mid_stream_keeps_every_row() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-reroute:main";
    let oracle_id = "it/reasoning-reroute-oracle:main";
    let rule = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": "http://example.org/ns/rule/caseId",
        "f:rule": {"@type": "f:sparql", "@value": format!(
            "{SPARQL_PREFIXES}CONSTRUCT {{ ?x ex:caseId ?id }} \
             WHERE {{ ?x ex:parent ?y BIND(CONCAT(\"derived-\", STR(?y)) AS ?id) }}"
        )}
    });
    for id in [ledger_id, oracle_id] {
        let ledger = genesis_ledger_for_fluree(&fluree, id).await;
        let ledger = fluree
            .insert(ledger, &reroute_data())
            .await
            .expect("seed")
            .ledger;
        fluree.insert(ledger, &rule).await.expect("rule");
    }
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let view = fluree.db(ledger_id).await.expect("view");
    let oracle = fluree.db(oracle_id).await.expect("oracle");

    let jsonld = json!({
        "@context": ctx(),
        "select": ["?x", "?id"],
        "where": [{"@id": "ex:board", "ex:levels": "?x"}, {"@id": "?x", "ex:caseId": "?id"}],
        "reasoning": "datalog",
        "limit": 1024
    });
    let sparql = format!(
        "# PRAGMA reasoning: datalog\n{SPARQL_PREFIXES}\
         SELECT ?x ?id WHERE {{ ex:board ex:levels ?x . ?x ex:caseId ?id }} LIMIT 1024"
    );
    let expected = sorted(
        (0..10)
            .map(|i| json!([format!("ex:s{i}"), format!("stored-s{i}")]))
            .chain(REROUTE_RULE_INPUTS.iter().map(|i| {
                json!([
                    format!("ex:s{i}"),
                    format!("derived-http://example.org/ns/p{i}")
                ])
            }))
            .collect(),
    );

    let (store, guard) = init_test_tracing();
    tracing::callsite::rebuild_interest_cache();
    for (run, surface, input) in [
        ("cold", "JSON-LD", QueryInput::JsonLd(&jsonld)),
        ("cached", "SPARQL", QueryInput::Sparql(&sparql)),
        ("cached", "JSON-LD", QueryInput::JsonLd(&jsonld)),
    ] {
        let label = format!("{run} ({surface})");
        assert_eq!(
            query_rows(&fluree, &oracle, input).await,
            expected,
            "{label}: oracle"
        );
        let declines = store.find_events(DECLINE_EVENT).len();
        assert_eq!(
            query_rows(&fluree, &view, input).await,
            expected,
            "{label}: indexed"
        );
        let mid_stream: Vec<usize> = store.find_events(DECLINE_EVENT)[declines..]
            .iter()
            .filter_map(|e| e.fields.get("input_rows")?.parse().ok())
            .collect();
        if run == "cold" {
            assert!(
                mid_stream.iter().any(|&rows| rows < REROUTE_MARKERS),
                "{label}: a flush before the last should decline; declined flushes: {mid_stream:?}"
            );
        } else {
            assert!(
                mid_stream.is_empty(),
                "{label}: a cached failed translation declines before accumulating; \
                 declined flushes: {mid_stream:?}"
            );
        }
    }
    drop(guard);
}

/// Maintenance computes a closure to write it into a generation. Queries
/// are then answered from the index, so nothing would read that closure
/// from the reasoning cache again, where each entry holds a whole closure
/// in memory: a refresh leaves none there. A query-time run of the same
/// reasoning does.
#[tokio::test]
async fn a_refresh_leaves_no_closure_in_the_reasoning_cache() {
    use fluree_db_api::RefreshOutcome;
    let cache = fluree_db_query::reasoning::global_reasoning_cache();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-cache-refresh:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id).await;
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"}
            ]}),
        )
        .await
        .expect("seed")
        .ledger;
    let people = json!({"@context": ctx(), "select": "?p", "reasoning": "owl2rl",
        "where": {"@id": "?p", "@type": "ex:Person"}});

    for program in [
        json!({"reasoning": "owl2rl"}),
        json!({"reasoning": "datalog", "rules": [{
            "@context": ctx(),
            "where": {"@id": "?s", "@type": "ex:Student"},
            "insert": {"@id": "?s", "@type": "ex:Person"}
        }]}),
    ] {
        let index = fluree
            .register_reasoning_index(ledger_id, &program)
            .await
            .expect("register");
        let outcome = fluree
            .refresh_reasoning_index(ledger_id, &index)
            .await
            .expect("refresh");
        assert!(
            matches!(outcome, RefreshOutcome::Published { .. }),
            "{outcome:?}"
        );
        assert!(
            !cache.holds_ledger(ledger_id),
            "the refresh of {program} left its closure in the reasoning cache"
        );
    }

    // A commit the indexes have not reached: the query reasons for itself.
    fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:bob", "@type": "ex:Student"}),
        )
        .await
        .expect("commit");
    let view = fluree.db(ledger_id).await.expect("view");
    let rows = query_rows(&fluree, &view, QueryInput::JsonLd(&people)).await;
    assert_eq!(rows, vec![json!("ex:alice"), json!("ex:bob")]);
    assert!(cache.holds_ledger(ledger_id));
}
