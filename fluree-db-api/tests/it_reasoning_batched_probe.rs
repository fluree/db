//! Batched join lanes under a reasoning overlay.
//!
//! Reasoning queries run with eager materialization: derived facts are
//! decoded `Sid`s, which an encoded binding never equals. The nested-loop
//! join's subject, exists and object lanes and the property join's walks stay
//! engaged under it and decode what they emit. Each case pins its rows in
//! JSON-LD and SPARQL, the lane that produced them, and the rows of a
//! never-indexed copy of the ledger, where every join runs per row.
//!
//! The derived facts are mostly edges (`ex:mentors ⊑ ex:knows`,
//! `ex:adores ⊑ ex:likes`): sub-property entailments reach the lanes as
//! overlay asserts on fixed-predicate probes.

#![cfg(feature = "native")]

use crate::support::genesis_ledger_for_fluree;
use crate::support::reasoning_lanes::{
    ctx, query_rows, sorted, sparql, SPARQL_PREFIXES, SUBJECT_LANE,
};
use crate::support::span_capture::{init_test_tracing, SpanStore};
use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, QueryInput, ReindexOptions};
use serde_json::{json, Value};

/// `hub ex:mentors carol` entails `hub ex:knows carol`; `carol ex:adores
/// pizza` entails `carol ex:likes pizza`.
fn seed_data() -> Value {
    json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
            {"@id": "ex:mentors", "rdfs:subPropertyOf": {"@id": "ex:knows"}},
            {"@id": "ex:adores",  "rdfs:subPropertyOf": {"@id": "ex:likes"}},
            {"@id": "ex:alice", "@type": "ex:Student", "ex:age": 20,
             "ex:name": "Alice", "ex:likes": {"@id": "ex:pizza"}},
            {"@id": "ex:bob",   "@type": "ex:Person",  "ex:age": 25},
            {"@id": "ex:carol", "@type": "ex:Student", "ex:age": 30,
             "ex:adores": {"@id": "ex:pizza"}},
            {"@id": "ex:dave",  "@type": "ex:Dog",     "ex:age": 5,
             "ex:likes": {"@id": "ex:pizza"}},
            {"@id": "ex:hub",
             "ex:knows": [{"@id": "ex:alice"}, {"@id": "ex:bob"}],
             "ex:mentors": {"@id": "ex:carol"}},
            {"@id": "ex:fan1", "ex:admires": {"@id": "ex:alice"}},
            {"@id": "ex:fan2", "ex:admires": {"@id": "ex:carol"}},
            {"@id": "ex:alice", "ex:bestFriend": {"@id": "ex:carol"}},
            {"@id": "ex:bob",   "ex:bestFriend": {"@id": "ex:alice"}},
            {"@id": "ex:carol", "ex:bestFriend": {"@id": "ex:alice"}}
        ]
    })
}

/// A commit after the index: erin is minted in novelty and reaches the
/// lanes only through entailed edges.
fn novelty_data() -> Value {
    json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:hub", "ex:mentors": {"@id": "ex:erin"}},
            {"@id": "ex:erin", "ex:age": 40, "ex:adores": {"@id": "ex:pizza"},
             "ex:bestFriend": {"@id": "ex:carol"}},
            {"@id": "ex:fan3", "ex:admires": {"@id": "ex:erin"}}
        ]
    })
}

/// The lane a case's rows must come from.
#[derive(Clone, Copy, Debug)]
enum Lane {
    /// A nested-loop join lane, named by its flush span.
    Join(&'static str),
    /// The property join's batched probe or SPOT star walk.
    PropertyJoin,
    /// Not pinned: the rows are checked, the routing is not.
    Unpinned,
}

struct Case {
    name: &'static str,
    jsonld: Value,
    sparql: String,
    lane: Lane,
    /// Rows over the index, then after the novelty commit.
    expected: [Vec<Value>; 2],
}

fn rows(values: &[Value]) -> Vec<Value> {
    values.to_vec()
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            // The left scan merges the entailed hub→carol edge, so carol's
            // probe key is a decoded `Sid`; the age probe decodes its rows.
            name: "subject-lane-over-entailed-edge",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?b", "?v"],
                "where": [
                    {"@id": "ex:hub", "ex:knows": "?b"},
                    {"@id": "?b", "ex:age": "?v"}
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql("SELECT ?b ?v WHERE { ex:hub ex:knows ?b . ?b ex:age ?v }"),
            lane: Lane::Join("join_flush_batched_binary"),
            expected: [
                rows(&[
                    json!(["ex:alice", 20]),
                    json!(["ex:bob", 25]),
                    json!(["ex:carol", 30]),
                ]),
                rows(&[
                    json!(["ex:alice", 20]),
                    json!(["ex:bob", 25]),
                    json!(["ex:carol", 30]),
                    json!(["ex:erin", 40]),
                ]),
            ],
        },
        Case {
            name: "subject-lane-rows-reach-filter",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?b", "?v"],
                "where": [
                    {"@id": "ex:hub", "ex:knows": "?b"},
                    {"@id": "?b", "ex:age": "?v"},
                    ["filter", "(> ?v 21)"]
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql(
                "SELECT ?b ?v WHERE { ex:hub ex:knows ?b . ?b ex:age ?v FILTER(?v > 21) }",
            ),
            lane: Lane::Join("join_flush_batched_binary"),
            expected: [
                rows(&[json!(["ex:bob", 25]), json!(["ex:carol", 30])]),
                rows(&[
                    json!(["ex:bob", 25]),
                    json!(["ex:carol", 30]),
                    json!(["ex:erin", 40]),
                ]),
            ],
        },
        Case {
            // The right pattern's object is an entailed ref: the lane emits
            // it as the `Sid` the next join reads.
            name: "subject-lane-emits-entailed-ref",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?b", "?food"],
                "where": [
                    {"@id": "ex:hub", "ex:knows": "?b"},
                    {"@id": "?b", "ex:likes": "?food"}
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql("SELECT ?b ?food WHERE { ex:hub ex:knows ?b . ?b ex:likes ?food }"),
            lane: Lane::Join("join_flush_batched_binary"),
            expected: [
                rows(&[
                    json!(["ex:alice", "ex:pizza"]),
                    json!(["ex:carol", "ex:pizza"]),
                ]),
                rows(&[
                    json!(["ex:alice", "ex:pizza"]),
                    json!(["ex:carol", "ex:pizza"]),
                    json!(["ex:erin", "ex:pizza"]),
                ]),
            ],
        },
        Case {
            // The planner probes `bestFriend` by its bound object, and the
            // lane's emitted carol must meet the entailed hub→carol edge.
            name: "object-lane-subject-meets-entailed-edge",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?b", "?f"],
                "where": [
                    {"@id": "ex:hub", "ex:knows": "?b"},
                    {"@id": "?b", "ex:bestFriend": "?f"},
                    {"@id": "ex:hub", "ex:knows": "?f"}
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql(
                "SELECT ?b ?f WHERE { ex:hub ex:knows ?b . ?b ex:bestFriend ?f . ex:hub ex:knows ?f }",
            ),
            lane: Lane::Join("join_flush_batched_object_binary"),
            expected: [
                rows(&[
                    json!(["ex:alice", "ex:carol"]),
                    json!(["ex:bob", "ex:alice"]),
                    json!(["ex:carol", "ex:alice"]),
                ]),
                rows(&[
                    json!(["ex:alice", "ex:carol"]),
                    json!(["ex:bob", "ex:alice"]),
                    json!(["ex:carol", "ex:alice"]),
                    json!(["ex:erin", "ex:carol"]),
                ]),
            ],
        },
        Case {
            // The lane's emitted subject must meet an entailed type
            // (Student ⊑ Person).
            name: "object-lane-subject-meets-entailed-type",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?b", "?f"],
                "where": [
                    {"@id": "ex:hub", "ex:knows": "?b"},
                    {"@id": "?b", "ex:bestFriend": "?f"},
                    {"@id": "?f", "@type": "ex:Person"}
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql(
                "SELECT ?b ?f WHERE { ex:hub ex:knows ?b . ?b ex:bestFriend ?f . ?f a ex:Person }",
            ),
            lane: Lane::Join("join_flush_batched_object_binary"),
            expected: [
                rows(&[
                    json!(["ex:alice", "ex:carol"]),
                    json!(["ex:bob", "ex:alice"]),
                    json!(["ex:carol", "ex:alice"]),
                ]),
                rows(&[
                    json!(["ex:alice", "ex:carol"]),
                    json!(["ex:bob", "ex:alice"]),
                    json!(["ex:carol", "ex:alice"]),
                    json!(["ex:erin", "ex:carol"]),
                ]),
            ],
        },
        Case {
            // Bound subject, fixed predicate and object: the exists lane.
            // carol's and erin's `likes pizza` are entailed.
            name: "exists-lane-sees-entailed-edge",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?b"],
                "where": [
                    {"@id": "ex:hub", "ex:knows": "?b"},
                    {"@id": "?b", "ex:likes": {"@id": "ex:pizza"}}
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql("SELECT ?b WHERE { ex:hub ex:knows ?b . ?b ex:likes ex:pizza }"),
            lane: Lane::Join("join_flush_batched_exists_binary"),
            expected: [
                rows(&[json!(["ex:alice"]), json!(["ex:carol"])]),
                rows(&[json!(["ex:alice"]), json!(["ex:carol"]), json!(["ex:erin"])]),
            ],
        },
        Case {
            // Bound object from the left, new subject variable: the object
            // lane, whose emitted subjects decode.
            name: "object-lane-emits-decoded-subjects",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?fan", "?x"],
                "where": [
                    {"@id": "ex:hub", "ex:knows": "?x"},
                    {"@id": "?fan", "ex:admires": "?x"}
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql("SELECT ?fan ?x WHERE { ex:hub ex:knows ?x . ?fan ex:admires ?x }"),
            lane: Lane::Join("join_flush_batched_object_binary"),
            expected: [
                rows(&[
                    json!(["ex:fan1", "ex:alice"]),
                    json!(["ex:fan2", "ex:carol"]),
                ]),
                rows(&[
                    json!(["ex:fan1", "ex:alice"]),
                    json!(["ex:fan2", "ex:carol"]),
                    json!(["ex:fan3", "ex:erin"]),
                ]),
            ],
        },
        Case {
            // A star anchored on an entailed bound object: the property
            // join keys its subjects by id under eager materialization.
            name: "property-join-star-on-entailed-anchor",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?s", "?v"],
                "where": [
                    {"@id": "?s", "ex:likes": {"@id": "ex:pizza"}, "ex:age": "?v"}
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql("SELECT ?s ?v WHERE { ?s ex:likes ex:pizza ; ex:age ?v }"),
            lane: Lane::PropertyJoin,
            expected: [
                rows(&[
                    json!(["ex:alice", 20]),
                    json!(["ex:carol", 30]),
                    json!(["ex:dave", 5]),
                ]),
                rows(&[
                    json!(["ex:alice", 20]),
                    json!(["ex:carol", 30]),
                    json!(["ex:dave", 5]),
                    json!(["ex:erin", 40]),
                ]),
            ],
        },
        Case {
            // OPTIONAL keeps its per-row lanes under eager materialization;
            // its rows over the entailed edge are what is pinned.
            name: "optional-over-entailed-edge",
            jsonld: json!({
                "@context": ctx(),
                "select": ["?b", "?n"],
                "where": [
                    {"@id": "ex:hub", "ex:knows": "?b"},
                    ["optional", {"@id": "?b", "ex:name": "?n"}]
                ],
                "reasoning": "owl2rl"
            }),
            sparql: sparql(
                "SELECT ?b ?n WHERE { ex:hub ex:knows ?b OPTIONAL { ?b ex:name ?n } }",
            ),
            lane: Lane::Unpinned,
            expected: [
                rows(&[
                    json!(["ex:alice", "Alice"]),
                    json!(["ex:bob", null]),
                    json!(["ex:carol", null]),
                ]),
                rows(&[
                    json!(["ex:alice", "Alice"]),
                    json!(["ex:bob", null]),
                    json!(["ex:carol", null]),
                    json!(["ex:erin", null]),
                ]),
            ],
        },
    ]
}

/// Event and span counts before a query, so a check reads only its own.
struct Mark {
    runtime_events: usize,
    property_join_events: usize,
    flush_spans: usize,
    spans: usize,
}

const JOIN_RUNTIME_EVENT: &str = "nested loop join runtime path";
const PROPERTY_JOIN_EVENT: &str = "property_join: complete";

fn mark(store: &SpanStore, lane: Lane) -> Mark {
    Mark {
        runtime_events: store.find_events(JOIN_RUNTIME_EVENT).len(),
        property_join_events: store.find_events(PROPERTY_JOIN_EVENT).len(),
        flush_spans: match lane {
            Lane::Join(span) => store.find_spans(span).len(),
            _ => 0,
        },
        spans: store.span_names().len(),
    }
}

fn assert_lane(store: &SpanStore, before: &Mark, lane: Lane, label: &str) {
    match lane {
        Lane::Join(span) => {
            let routed: Vec<String> = store.find_events(JOIN_RUNTIME_EVENT)
                [before.runtime_events..]
                .iter()
                .filter_map(|e| e.fields.get("use_batched").cloned())
                .collect();
            assert!(
                routed.iter().any(|r| r == "true"),
                "{label}: a join must take a batched lane under reasoning; use_batched: {routed:?}"
            );
            assert!(
                store.find_spans(span).len() > before.flush_spans,
                "{label}: expected the {span} lane; this query's spans: {:?}",
                &store.span_names()[before.spans..]
            );
        }
        Lane::PropertyJoin => {
            let walked = store.find_events(PROPERTY_JOIN_EVENT)[before.property_join_events..]
                .iter()
                .any(|e| {
                    ["used_batched_probe", "used_spot_star_walk"]
                        .iter()
                        .any(|field| e.fields.get(*field).is_some_and(|v| v == "true"))
                });
            assert!(
                walked,
                "{label}: the property join must probe or walk in batches under reasoning"
            );
        }
        Lane::Unpinned => {}
    }
}

async fn indexed_and_oracle(fluree: &Fluree, ledger_id: &str, oracle_id: &str) {
    let ledger = genesis_ledger_for_fluree(fluree, ledger_id).await;
    fluree.insert(ledger, &seed_data()).await.expect("insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let oracle = genesis_ledger_for_fluree(fluree, oracle_id).await;
    fluree
        .insert(oracle, &seed_data())
        .await
        .expect("oracle insert");
}

#[tokio::test(flavor = "current_thread")]
async fn batched_join_lanes_engage_and_join_entailed_facts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-batched:main";
    let oracle_id = "it/reasoning-batched-oracle:main";
    indexed_and_oracle(&fluree, ledger_id, oracle_id).await;

    let (store, guard) = init_test_tracing();
    let cases = cases();

    let indexed = fluree.db(ledger_id).await.expect("indexed view");
    let oracle = fluree.db(oracle_id).await.expect("oracle view");
    // Sibling tests share these callsites and can pin their interest to
    // "never" while the subscriber installs: run every query once, then
    // recompute interest before measuring.
    for case in &cases {
        query_rows(&fluree, &indexed, QueryInput::JsonLd(&case.jsonld)).await;
        query_rows(&fluree, &indexed, QueryInput::Sparql(&case.sparql)).await;
    }
    tracing::callsite::rebuild_interest_cache();
    check_phase(&fluree, &store, &cases, 0, &indexed, &oracle).await;

    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let t = fluree
        .insert(ledger, &novelty_data())
        .await
        .expect("novelty commit")
        .receipt
        .t;
    let oracle_ledger = fluree.ledger(oracle_id).await.expect("oracle ledger");
    let oracle_t = fluree
        .insert(oracle_ledger, &novelty_data())
        .await
        .expect("oracle novelty commit")
        .receipt
        .t;
    let indexed = fluree.db_at_t(ledger_id, t).await.expect("novelty view");
    let oracle = fluree
        .db_at_t(oracle_id, oracle_t)
        .await
        .expect("oracle novelty view");
    check_phase(&fluree, &store, &cases, 1, &indexed, &oracle).await;

    // One state, with and without reasoning: the lane probes `ex:knows`,
    // whose derived ops must not leak into the unreasoned query.
    let mentees_known = |mode: &str| {
        (
            json!({
                "@context": ctx(),
                "select": ["?m", "?k"],
                "where": [
                    {"@id": "?h", "ex:mentors": "?m"},
                    {"@id": "?h", "ex:knows": "?k"}
                ],
                "reasoning": mode
            }),
            format!(
                "# PRAGMA reasoning: {mode}\n{SPARQL_PREFIXES}\
                 SELECT ?m ?k WHERE {{ ?h ex:mentors ?m . ?h ex:knows ?k }}"
            ),
        )
    };
    let pairs = |known: &[&str]| {
        let mut rows = Vec::new();
        for m in ["ex:carol", "ex:erin"] {
            for k in known {
                rows.push(json!([m, k]));
            }
        }
        sorted(rows)
    };
    let lane = Lane::Join("join_flush_batched_binary");
    for (mode, known) in [
        ("owl2rl", &["ex:alice", "ex:bob", "ex:carol", "ex:erin"][..]),
        ("none", &["ex:alice", "ex:bob"][..]),
    ] {
        let (jsonld, sparql) = mentees_known(mode);
        for input in [QueryInput::JsonLd(&jsonld), QueryInput::Sparql(&sparql)] {
            let label = format!("hub's mentees and known (reasoning {mode})");
            let before = mark(&store, lane);
            assert_eq!(
                query_rows(&fluree, &indexed, input).await,
                pairs(known),
                "{label}"
            );
            assert_lane(&store, &before, lane, &label);
        }
    }
    drop(guard);
}

async fn check_phase(
    fluree: &Fluree,
    store: &SpanStore,
    cases: &[Case],
    phase: usize,
    indexed: &GraphDb,
    oracle: &GraphDb,
) {
    let phase_name = ["index", "index + novelty"][phase];
    for case in cases {
        let expected = sorted(case.expected[phase].clone());
        for (surface, input) in [
            ("JSON-LD", QueryInput::JsonLd(&case.jsonld)),
            ("SPARQL", QueryInput::Sparql(&case.sparql)),
        ] {
            let label = format!("{} ({surface}, {phase_name})", case.name);
            let before = mark(store, case.lane);
            let rows = query_rows(fluree, indexed, input).await;
            assert_eq!(rows, expected, "{label}: rows");
            assert_lane(store, &before, case.lane, &label);
            assert_eq!(
                query_rows(fluree, oracle, input).await,
                expected,
                "{label}: per-row oracle rows"
            );
        }
    }
}

/// Facts a rule derives into a named graph reach a batched join inside that
/// graph, which reads the graph's own partition of the derived facts. The
/// rule's own body runs on a batched lane as it materializes.
#[tokio::test(flavor = "current_thread")]
async fn batched_join_reads_facts_derived_into_a_named_graph() {
    const DATA: &str = r"
@prefix ex: <http://example.org/ns/> .
ex:a ex:parent ex:b . ex:b ex:parent ex:c . ex:c ex:parent ex:d .
GRAPH ex:inferred { ex:c ex:age 30 . ex:d ex:age 40 . ex:q ex:grandparent ex:r . }
";
    let rule = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": "http://example.org/ns/grandparent",
        "f:rule": {
            "@type": "f:sparql",
            "@value": format!(
                "{SPARQL_PREFIXES}CONSTRUCT {{ GRAPH ex:inferred {{ ?x ex:grandparent ?z }} }} \
                 WHERE {{ ?x ex:parent ?y . ?y ex:parent ?z }}"
            )
        }
    });
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-batched-graph:main";
    let oracle_id = "it/reasoning-batched-graph-oracle:main";
    // The batched lanes don't read a pooled graph under reasoning, so keep
    // `ex:inferred` in trees of its own, as a large graph would be.
    let dedicated = format!(
        "@prefix f: <https://ns.flur.ee/db#> .
GRAPH <urn:fluree:{ledger_id}#config> {{
  <urn:config:main> a f:LedgerConfig ;
    f:indexingDefaults [ f:graphPlacement f:PlacementDedicated ] .
}}
"
    );
    for id in [ledger_id, oracle_id] {
        let data = if id == ledger_id {
            format!("{DATA}{dedicated}")
        } else {
            DATA.to_string()
        };
        let ledger = fluree
            .stage_owned(genesis_ledger_for_fluree(&fluree, id).await)
            .upsert_turtle(&data)
            .execute()
            .await
            .expect("seed")
            .ledger;
        fluree.insert(ledger, &rule).await.expect("rule");
    }
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    // The stored `ex:q ex:grandparent ex:r` indexes the predicate, which a
    // lane needs before it merges derived facts into it.
    let in_graph = |select: &str, body: &str| {
        format!(
            "# PRAGMA reasoning: datalog\n{SPARQL_PREFIXES}\
             SELECT {select} WHERE {{ GRAPH ex:inferred {{ {body} }} }}"
        )
    };
    let graph_ctx = json!({"ex": "http://example.org/ns/"});
    let cases = [
        (
            "subject-lane",
            in_graph("?x ?z ?a", "?x ex:grandparent ?z . ?z ex:age ?a"),
            json!({
                "@context": graph_ctx,
                "select": ["?x", "?z", "?a"],
                "where": [["graph", "ex:inferred",
                    {"@id": "?x", "ex:grandparent": "?z"},
                    {"@id": "?z", "ex:age": "?a"}]],
                "reasoning": "datalog"
            }),
            Lane::Join("join_flush_batched_binary"),
            sorted(vec![
                json!(["ex:a", "ex:c", 30]),
                json!(["ex:b", "ex:d", 40]),
            ]),
        ),
        (
            "object-lane",
            in_graph("?x ?z", "?z ex:age 30 . ?x ex:grandparent ?z"),
            json!({
                "@context": graph_ctx,
                "select": ["?x", "?z"],
                "where": [["graph", "ex:inferred",
                    {"@id": "?z", "ex:age": 30},
                    {"@id": "?x", "ex:grandparent": "?z"}]],
                "reasoning": "datalog"
            }),
            Lane::Join("join_flush_batched_object_binary"),
            vec![json!(["ex:a", "ex:c"])],
        ),
    ];

    let (store, guard) = init_test_tracing();
    let indexed = fluree.db(ledger_id).await.expect("indexed view");
    let oracle = fluree.db(oracle_id).await.expect("oracle view");
    for (_, sparql, jsonld, _, _) in &cases {
        query_rows(&fluree, &indexed, QueryInput::Sparql(sparql)).await;
        query_rows(&fluree, &indexed, QueryInput::JsonLd(jsonld)).await;
    }
    tracing::callsite::rebuild_interest_cache();

    for (name, sparql, jsonld, lane, expected) in &cases {
        for (surface, input) in [
            ("SPARQL", QueryInput::Sparql(sparql)),
            ("JSON-LD", QueryInput::JsonLd(jsonld)),
        ] {
            let label = format!("named-graph {name} ({surface})");
            let before = mark(&store, *lane);
            let rows = query_rows(&fluree, &indexed, input).await;
            assert_eq!(&rows, expected, "{label}: rows over the index");
            assert_lane(&store, &before, *lane, &label);
            assert_eq!(
                &query_rows(&fluree, &oracle, input).await,
                expected,
                "{label}: per-row oracle rows"
            );
        }
    }
    drop(guard);
}

/// Datalog rule bodies run with eager materialization too, so their joins
/// take the batched lanes, and each head is built from what a lane emitted:
/// a subject from the object lane, a ref or a literal from the subject lane.
#[tokio::test(flavor = "current_thread")]
async fn rule_bodies_on_batched_lanes_derive_whole_facts() {
    const DATA: &str = r#"
@prefix ex: <http://example.org/ns/> .
ex:a ex:parent ex:b . ex:b ex:parent ex:c . ex:c ex:name "Cee" .
"#;
    let rules = [
        (
            "grandparent",
            "CONSTRUCT { ?x ex:grandparent ?z } WHERE { ?x ex:parent ?y . ?y ex:parent ?z }",
        ),
        (
            "descendant",
            "CONSTRUCT { ?z ex:descendantOf ex:a } WHERE { ex:a ex:parent ?y . ?y ex:parent ?z }",
        ),
        (
            "childName",
            "CONSTRUCT { ex:b ex:childName ?n } WHERE { ex:b ex:parent ?z . ?z ex:name ?n }",
        ),
    ];
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-batched-rules:main";
    let oracle_id = "it/reasoning-batched-rules-oracle:main";
    for id in [ledger_id, oracle_id] {
        let mut ledger = fluree
            .stage_owned(genesis_ledger_for_fluree(&fluree, id).await)
            .upsert_turtle(DATA)
            .execute()
            .await
            .expect("seed")
            .ledger;
        for (name, construct) in rules {
            let rule = json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": format!("http://example.org/ns/rule/{name}"),
                "f:rule": {"@type": "f:sparql", "@value": format!("{SPARQL_PREFIXES}{construct}")}
            });
            ledger = fluree.insert(ledger, &rule).await.expect("rule").ledger;
        }
    }
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let query = format!(
        "# PRAGMA reasoning: datalog\n{SPARQL_PREFIXES}\
         SELECT ?s ?p ?o WHERE {{ VALUES ?p {{ ex:grandparent ex:descendantOf ex:childName }} ?s ?p ?o }}"
    );
    let expected = sorted(vec![
        json!(["ex:a", "ex:grandparent", "ex:c"]),
        json!(["ex:b", "ex:childName", "Cee"]),
        json!(["ex:c", "ex:descendantOf", "ex:a"]),
    ]);

    // The cold query materializes, so its spans include the rule bodies'.
    let (store, guard) = init_test_tracing();
    tracing::callsite::rebuild_interest_cache();
    let indexed = fluree.db(ledger_id).await.expect("indexed view");
    let rows = query_rows(&fluree, &indexed, QueryInput::Sparql(&query)).await;
    drop(guard);
    assert_eq!(rows, expected, "facts derived over the index");
    for lane in [
        "join_flush_batched_binary",
        "join_flush_batched_object_binary",
    ] {
        assert!(
            store.has_span(lane),
            "rule bodies should take the {lane} lane; spans: {:?}",
            store.span_names()
        );
    }

    let oracle = fluree.db(oracle_id).await.expect("oracle view");
    assert_eq!(
        query_rows(&fluree, &oracle, QueryInput::Sparql(&query)).await,
        expected,
        "facts derived per row"
    );
}

/// A join step with both ends bound (the edge closing a triangle) has a
/// batched pair lane, which is not among the lanes that decode for an eager
/// caller. Under eager materialization it stays off and the step runs per
/// row: a rule whose body closes a triangle derives from asserted and
/// derived closing edges alike, and a reasoning query over the triangle
/// reads both. The routing is pinned as well as the rows, so admitting the
/// lane under reasoning has to come with a reason to change this test.
#[tokio::test(flavor = "current_thread")]
async fn a_closing_edge_under_reasoning_is_joined_per_row() {
    const PAIR_LANE: &str = "join_flush_batched_pair_binary";
    // Typed ends keep the cyclic-pattern operator off the triangle, so its
    // closing edge is a join step.
    const TRIANGLE: &str = "?x a ex:Student . ?y a ex:Faculty . ?z a ex:Course . \
         ?x ex:advisor ?y . ?y ex:teacherOf ?z . ?x ex:takesCourse ?z";
    let mut data = String::from("@prefix ex: <http://example.org/ns/> .\n");
    for i in 0..40 {
        // Each student takes their advisor's course; odd ones only audit it.
        let takes = if i % 2 == 0 { "takesCourse" } else { "audits" };
        data.push_str(&format!(
            "ex:s{i} a ex:Student ; ex:advisor ex:f{f} ; ex:{takes} ex:c{f} ; ex:takesCourse ex:other{i} .\n",
            f = i % 4
        ));
    }
    for f in 0..4 {
        data.push_str(&format!(
            "ex:f{f} a ex:Faculty ; ex:teacherOf ex:c{f} . ex:c{f} a ex:Course .\n"
        ));
    }
    let rules = [
        (
            "audited",
            "CONSTRUCT { ?x ex:takesCourse ?z } WHERE { ?x ex:audits ?z }",
        ),
        (
            "taughtByAdvisor",
            "CONSTRUCT { ?x ex:taughtByAdvisor ?z } WHERE { ?x a ex:Student . ?y a ex:Faculty . ?z a ex:Course . ?x ex:advisor ?y . ?y ex:teacherOf ?z . ?x ex:takesCourse ?z }",
        ),
    ];
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-batched-pair:main";
    let oracle_id = "it/reasoning-batched-pair-oracle:main";
    for id in [ledger_id, oracle_id] {
        let mut ledger = fluree
            .stage_owned(genesis_ledger_for_fluree(&fluree, id).await)
            .upsert_turtle(&data)
            .execute()
            .await
            .expect("seed")
            .ledger;
        for (name, construct) in rules {
            let rule = json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": format!("http://example.org/ns/rule/{name}"),
                "f:rule": {"@type": "f:sparql", "@value": format!("{SPARQL_PREFIXES}{construct}")}
            });
            ledger = fluree.insert(ledger, &rule).await.expect("rule").ledger;
        }
    }
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let derived = format!(
        "# PRAGMA reasoning: datalog\n{SPARQL_PREFIXES}\
         SELECT ?s ?o WHERE {{ ?s ex:taughtByAdvisor ?o }}"
    );
    let triangle = format!(
        "# PRAGMA reasoning: datalog\n{SPARQL_PREFIXES}\
         SELECT ?x ?z WHERE {{ {TRIANGLE} }}"
    );
    let expected = sorted(
        (0..40)
            .map(|i| json!([format!("ex:s{i}"), format!("ex:c{}", i % 4)]))
            .collect(),
    );

    let (store, guard) = init_test_tracing();
    tracing::callsite::rebuild_interest_cache();
    let indexed = fluree.db(ledger_id).await.expect("indexed view");
    for query in [&derived, &triangle] {
        assert_eq!(
            query_rows(&fluree, &indexed, QueryInput::Sparql(query)).await,
            expected,
            "over the index: {query}"
        );
    }
    drop(guard);
    assert!(
        !store.has_span(PAIR_LANE),
        "the pair lane ran under eager materialization; spans: {:?}",
        store.span_names()
    );

    // The same triangle without reasoning takes the lane: the fixture
    // reaches it, so its absence above is the guard's doing.
    let plain = format!("{SPARQL_PREFIXES}SELECT ?x ?z WHERE {{ {TRIANGLE} }}");
    let (store, guard) = init_test_tracing();
    tracing::callsite::rebuild_interest_cache();
    let asserted = query_rows(&fluree, &indexed, QueryInput::Sparql(&plain)).await;
    drop(guard);
    assert_eq!(asserted.len(), 20, "asserted closing edges only");
    assert!(
        store.has_span(PAIR_LANE),
        "the fixture should reach the pair lane; spans: {:?}",
        store.span_names()
    );

    let oracle = fluree.db(oracle_id).await.expect("oracle view");
    for query in [&derived, &triangle] {
        assert_eq!(
            query_rows(&fluree, &oracle, QueryInput::Sparql(query)).await,
            expected,
            "per row: {query}"
        );
    }
}

/// Two eager overlays over one state derive different `ex:friend` facts
/// (OWL 2 RL symmetry, a datalog rule); a lane's translated ops for one are
/// never served to the other. Stored filler edges outnumber the club in the
/// planner's stats, so the club drives and the lane probes `ex:friend`.
#[tokio::test(flavor = "current_thread")]
async fn translated_ops_are_kept_apart_per_reasoning_mode() {
    const DATA: &str = r"
@prefix ex: <http://example.org/ns/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:friend a owl:SymmetricProperty .
ex:club ex:member ex:a, ex:b, ex:c, ex:d .
ex:a ex:friend ex:b .
ex:c ex:teammate ex:d .
ex:p0 ex:friend ex:q0 . ex:p1 ex:friend ex:q1 . ex:p2 ex:friend ex:q2 .
ex:p3 ex:friend ex:q3 . ex:p4 ex:friend ex:q4 . ex:p5 ex:friend ex:q5 .
";
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-modes-apart:main";
    let ledger = fluree
        .stage_owned(genesis_ledger_for_fluree(&fluree, ledger_id).await)
        .upsert_turtle(DATA)
        .execute()
        .await
        .expect("seed")
        .ledger;
    let rule = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": "http://example.org/ns/rule/teammates",
        "f:rule": {"@type": "f:sparql", "@value": format!(
            "{SPARQL_PREFIXES}CONSTRUCT {{ ?x ex:friend ?y }} WHERE {{ ?x ex:teammate ?y }}"
        )}
    });
    fluree.insert(ledger, &rule).await.expect("rule");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let query = |mode: &str| {
        (
            json!({
                "@context": ctx(),
                "select": ["?x", "?y"],
                "where": [
                    {"@id": "ex:club", "ex:member": "?x"},
                    {"@id": "?x", "ex:friend": "?y"}
                ],
                "reasoning": mode
            }),
            format!(
                "# PRAGMA reasoning: {mode}\n{SPARQL_PREFIXES}\
                 SELECT ?x ?y WHERE {{ ex:club ex:member ?x . ?x ex:friend ?y }}"
            ),
        )
    };
    let symmetric = sorted(vec![json!(["ex:a", "ex:b"]), json!(["ex:b", "ex:a"])]);
    let teammates = sorted(vec![json!(["ex:a", "ex:b"]), json!(["ex:c", "ex:d"])]);

    let (store, guard) = init_test_tracing();
    tracing::callsite::rebuild_interest_cache();
    let view = fluree.db(ledger_id).await.expect("view");
    for (mode, expected) in [
        ("owl2rl", &symmetric),
        ("datalog", &teammates),
        ("owl2rl", &symmetric),
    ] {
        let (jsonld, sparql) = query(mode);
        for (surface, input) in [
            ("JSON-LD", QueryInput::JsonLd(&jsonld)),
            ("SPARQL", QueryInput::Sparql(&sparql)),
        ] {
            let label = format!("{mode} ({surface})");
            let lane = Lane::Join(SUBJECT_LANE);
            let before = mark(&store, lane);
            assert_eq!(
                &query_rows(&fluree, &view, input).await,
                expected,
                "{label}"
            );
            assert_lane(&store, &before, lane, &label);
        }
    }
    drop(guard);
}
