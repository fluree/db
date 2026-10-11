//! Equality between encoded and decoded bindings for values first committed
//! after the last index build.
//!
//! The batched probe lanes emit `EncodedSid`/`EncodedLit` under novelty, with
//! ids from `DictNovelty` for subjects and strings the index has not seen.
//! VALUES, UNION branches and plain scans under novelty emit the same terms
//! decoded. Each equality surface (VALUES compatibility, EXISTS keys, DISTINCT,
//! MINUS, GROUP BY) must see the two as one term; a normalizer that only knew
//! the persisted dictionaries kept them apart, so novelty-only values were
//! dropped, double-counted, or left uneliminated.
//!
//! Every query runs over the index plus a novelty tail, asserts explicit
//! expected rows, then compares against the same ledger fully reindexed.

#![cfg(feature = "native")]

use crate::support::{genesis_ledger_for_fluree, normalize_rows, span_capture};
use fluree_db_api::{FlureeBuilder, QueryInput, ReindexOptions};
use serde_json::{json, Value};

fn ctx() -> Value {
    json!({"ex": "http://example.org/ns/"})
}

const P: &str = "PREFIX ex: <http://example.org/ns/>\n";

/// The lane that must run for the case to pair an encoded term with a decoded
/// one: a batched lane emitting the novelty term encoded, or the hash join,
/// whose build side arrives encoded and probe scan decoded.
#[derive(Clone, Copy)]
enum Lane {
    PropertyJoin,
    SubjectProbe,
    ObjectProbe,
    HashJoin,
}

enum Query {
    Sparql(String),
    JsonLd(Value),
}

struct Case {
    name: &'static str,
    query: Query,
    expected: Value,
    lane: Lane,
}

#[tokio::test]
async fn novelty_only_terms_compare_equal_across_representations() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/novelty-encoded-equality:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    let base = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:m1", "@type": "ex:Decision", "ex:origin": "model"},
            {"@id": "ex:h0", "@type": "ex:Decision", "ex:origin": "human"},
            {"@id": "ex:old", "ex:kind": {"@id": "ex:Receipt"}, "ex:template": {"@id": "ex:T1"}}
        ]
    });
    let receipt = fluree.insert(ledger, &base).await.expect("base insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex base");

    // "novel", ex:n1, ex:new, ex:Fee and ex:T2 exist only in novelty.
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:n1", "@type": "ex:Decision", "ex:origin": "novel"},
                    {"@id": "ex:new", "ex:kind": {"@id": "ex:Fee"}, "ex:template": {"@id": "ex:T2"}}
                ]
            }),
        )
        .await
        .expect("novelty insert");

    let cases = vec![
        Case {
            name: "VALUES literal joined to a typed star",
            query: Query::Sparql(format!(
                r#"{P}SELECT ?d WHERE {{ VALUES ?o {{ "novel" "human" }} ?d a ex:Decision ; ex:origin ?o }}"#
            )),
            expected: json!([["ex:h0"], ["ex:n1"]]),
            lane: Lane::PropertyJoin,
        },
        Case {
            name: "VALUES literal joined to a typed star (JSON-LD)",
            query: Query::JsonLd(json!({
                "@context": ctx(),
                "select": ["?d"],
                "where": [
                    ["values", ["?o", ["novel", "human"]]],
                    {"@id": "?d", "@type": "ex:Decision", "ex:origin": "?o"}
                ]
            })),
            expected: json!([["ex:h0"], ["ex:n1"]]),
            lane: Lane::PropertyJoin,
        },
        Case {
            name: "NOT EXISTS keyed on a novelty-only subject",
            query: Query::Sparql(format!(
                r"{P}SELECT ?r WHERE {{ ?r ex:kind ?k . VALUES ?k {{ ex:Fee ex:Receipt }} FILTER NOT EXISTS {{ ?r ex:template ?a }} }}"
            )),
            expected: json!([]),
            lane: Lane::ObjectProbe,
        },
        Case {
            name: "NOT EXISTS keyed on a novelty-only subject (JSON-LD)",
            query: Query::JsonLd(json!({
                "@context": ctx(),
                "select": ["?r"],
                "where": [
                    {"@id": "?r", "ex:kind": "?k"},
                    ["values", ["?k", [{"@value": "ex:Fee", "@type": "@id"}, {"@value": "ex:Receipt", "@type": "@id"}]]],
                    ["not-exists", {"@id": "?r", "ex:template": "?a"}]
                ]
            })),
            expected: json!([]),
            lane: Lane::ObjectProbe,
        },
        Case {
            name: "EXISTS keyed on a novelty-only subject",
            query: Query::Sparql(format!(
                r"{P}SELECT ?r WHERE {{ ?r ex:kind ?k . VALUES ?k {{ ex:Fee ex:Receipt }} FILTER EXISTS {{ ?r ex:template ?a }} }}"
            )),
            expected: json!([["ex:new"], ["ex:old"]]),
            lane: Lane::ObjectProbe,
        },
        Case {
            name: "OPTIONAL with !BOUND keyed on a novelty-only subject",
            query: Query::Sparql(format!(
                r"{P}SELECT ?r WHERE {{ ?r ex:kind ?k . VALUES ?k {{ ex:Fee ex:Receipt }} OPTIONAL {{ ?r ex:template ?a }} FILTER(!BOUND(?a)) }}"
            )),
            expected: json!([]),
            lane: Lane::ObjectProbe,
        },
        Case {
            name: "sameTerm of a novelty-only subject and a constant",
            query: Query::Sparql(format!(
                r"{P}SELECT ?r WHERE {{ ?r ex:kind ?k . VALUES ?k {{ ex:Fee ex:Receipt }} FILTER(sameTerm(?r, ex:new)) }}"
            )),
            expected: json!([["ex:new"]]),
            lane: Lane::ObjectProbe,
        },
        Case {
            name: "sameTerm of a novelty-only subject and a constant (JSON-LD)",
            query: Query::JsonLd(json!({
                "@context": ctx(),
                "select": ["?r"],
                "where": [
                    {"@id": "?r", "ex:kind": "?k"},
                    ["values", ["?k", [{"@value": "ex:Fee", "@type": "@id"}, {"@value": "ex:Receipt", "@type": "@id"}]]],
                    ["filter", "(sameTerm ?r ex:new)"]
                ]
            })),
            expected: json!([["ex:new"]]),
            lane: Lane::ObjectProbe,
        },
        Case {
            name: "DISTINCT over a novelty-only subject in two representations",
            query: Query::Sparql(format!(
                r"{P}SELECT DISTINCT ?r WHERE {{ {{ ?r ex:kind ?k . VALUES ?k {{ ex:Fee ex:Receipt }} }} UNION {{ ?r ex:template ?a }} }}"
            )),
            expected: json!([["ex:new"], ["ex:old"]]),
            lane: Lane::ObjectProbe,
        },
        Case {
            name: "DISTINCT over a novelty-only string in two representations",
            query: Query::Sparql(format!(
                r#"{P}SELECT DISTINCT ?o WHERE {{ {{ ?d a ex:Decision ; ex:origin ?o }} UNION {{ VALUES ?o {{ "novel" "human" }} }} }}"#
            )),
            expected: json!([["human"], ["model"], ["novel"]]),
            lane: Lane::SubjectProbe,
        },
        Case {
            name: "MINUS eliminates a novelty-only string",
            query: Query::Sparql(format!(
                r#"{P}SELECT ?o WHERE {{ ?d a ex:Decision ; ex:origin ?o MINUS {{ VALUES ?o {{ "novel" "human" }} }} }}"#
            )),
            expected: json!([["model"]]),
            lane: Lane::PropertyJoin,
        },
        Case {
            name: "GROUP BY merges a novelty-only string into one group",
            query: Query::Sparql(format!(
                r#"{P}SELECT ?o (COUNT(*) AS ?n) WHERE {{ {{ ?d a ex:Decision ; ex:origin ?o }} UNION {{ VALUES ?o {{ "novel" "human" }} }} }} GROUP BY ?o"#
            )),
            expected: json!([["human", 2], ["model", 1], ["novel", 2]]),
            lane: Lane::SubjectProbe,
        },
    ];

    assert_cases(&fluree, ledger_id, &cases).await;
}

/// Each case over the index plus novelty — rows and lane — then the same
/// queries over the fully reindexed ledger, which must answer identically.
async fn assert_cases(fluree: &fluree_db_api::Fluree, ledger_id: &str, cases: &[Case]) {
    let view = fluree.db(ledger_id).await.expect("novelty view");
    let mut failures = Vec::new();
    let mut novelty_rows = Vec::new();
    for case in cases {
        let (spans, guard) = span_capture::init_test_tracing();
        let rows = run(fluree, &view, &case.query).await;
        drop(guard);
        if Value::Array(rows.clone()) != case.expected {
            failures.push(format!(
                "{}: expected {}, got {rows:?}",
                case.name, case.expected
            ));
        }
        let engaged = match case.lane {
            Lane::PropertyJoin => spans
                .find_events("property_join: complete")
                .iter()
                .any(|e| {
                    e.fields.get("used_batched_probe").map(String::as_str) == Some("true")
                        || e.fields.get("used_spot_star_walk").map(String::as_str) == Some("true")
                }),
            Lane::SubjectProbe => !spans.find_spans("join_flush_batched_binary").is_empty(),
            Lane::ObjectProbe => !spans
                .find_spans("join_flush_batched_object_binary")
                .is_empty(),
            Lane::HashJoin => explain(fluree, &view, &case.query)
                .await
                .contains("HashJoinOperator"),
        };
        if !engaged {
            failures.push(format!(
                "{}: the expected lane did not run, so the case no longer pairs an \
                 encoded term with a decoded one; spans {:?}",
                case.name,
                spans.span_names()
            ));
        }
        novelty_rows.push(rows);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex ground truth");
    let view = fluree.db(ledger_id).await.expect("indexed view");
    for (case, under_novelty) in cases.iter().zip(&novelty_rows) {
        let indexed = run(fluree, &view, &case.query).await;
        assert_eq!(
            &indexed, under_novelty,
            "{}: novelty answer differs from the reindexed ground truth",
            case.name
        );
    }
}

/// A skolemized blank node is a dictionary subject: lanes reading the index
/// emit it encoded, scans under novelty emit it decoded.
#[tokio::test]
async fn blank_node_subjects_compare_equal_across_representations() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/novelty-encoded-equality-bnode:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    let mut graph = vec![json!({"@id": "_:p", "@type": "ex:Person"})];
    graph.extend((0..1000).map(|i| json!({"@id": format!("ex:a{i}"), "ex:knows": {"@id": "_:p"}})));
    let receipt = fluree
        .insert(ledger, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("base insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex base");
    // Any novelty switches the scans to decoded output.
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({"@context": ctx(), "@id": "ex:zz", "ex:unrelated": "x"}),
        )
        .await
        .expect("novelty insert");

    let cases = vec![
        Case {
            name: "hash join keyed on a blank node",
            query: Query::Sparql(format!(
                r"{P}SELECT (COUNT(*) AS ?n) WHERE {{ VALUES ?k {{ ex:Person ex:Other }} ?x a ?k . ?a ex:knows ?x }}"
            )),
            expected: json!([[1000]]),
            lane: Lane::HashJoin,
        },
        Case {
            name: "hash join keyed on a blank node (JSON-LD)",
            query: Query::JsonLd(json!({
                "@context": ctx(),
                "select": ["(as (count *) ?n)"],
                "where": [
                    ["values", ["?k", [{"@value": "ex:Person", "@type": "@id"}, {"@value": "ex:Other", "@type": "@id"}]]],
                    {"@id": "?x", "@type": "?k"},
                    {"@id": "?a", "ex:knows": "?x"}
                ]
            })),
            expected: json!([[1000]]),
            lane: Lane::HashJoin,
        },
        Case {
            name: "COUNT(DISTINCT) over a blank node in two representations",
            query: Query::Sparql(format!(
                r"{P}SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE {{ {{ VALUES ?k {{ ex:Person ex:Other }} ?x a ?k }} UNION {{ ?a ex:knows ?x }} }}"
            )),
            expected: json!([[1]]),
            lane: Lane::ObjectProbe,
        },
        Case {
            name: "MINUS eliminates a blank node across representations",
            query: Query::Sparql(format!(
                r"{P}SELECT ?x WHERE {{ VALUES ?k {{ ex:Person ex:Other }} ?x a ?k MINUS {{ ?a ex:knows ?x }} }}"
            )),
            expected: json!([]),
            lane: Lane::ObjectProbe,
        },
    ];

    assert_cases(&fluree, ledger_id, &cases).await;
}

async fn explain(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    query: &Query,
) -> String {
    let plan = match query {
        Query::Sparql(q) => fluree.explain_sparql(view, q).await,
        Query::JsonLd(q) => fluree.explain(view, q).await,
    };
    plan.expect("explain")["plan"]["physical"].to_string()
}

async fn run(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    query: &Query,
) -> Vec<Value> {
    let input = match query {
        Query::Sparql(q) => QueryInput::Sparql(q),
        Query::JsonLd(q) => QueryInput::JsonLd(q),
    };
    let result = fluree.query(view, input).await.expect("query");
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    normalize_rows(&jsonld)
}
