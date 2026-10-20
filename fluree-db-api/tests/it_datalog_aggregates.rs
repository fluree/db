//! Grouped aggregates in rule bodies.
//!
//! A rule aggregates in a subquery of its body (SPARQL
//! `{ SELECT ?k (COUNT(?x) AS ?n) WHERE { … } GROUP BY ?k }`, JSON-LD
//! `["query", {…, "groupBy": […]}]`). Like negation, an aggregate reads only
//! facts that are complete before the rule runs. These tests pin the counting
//! semantics: groups, the implicit group, bag versus DISTINCT, duplicates of
//! asserted facts, `owl:sameAs` aliases, and failure when the budget stops
//! first.

use crate::support::{
    genesis_ledger, instances_sharing_storage, normalize_rows, query_jsonld,
    rebuild_and_publish_index, MemoryFluree, MemoryLedger,
};
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug)]
enum Storage {
    NoveltyOnly,
    Indexed,
}

const STORAGE: [Storage; 2] = [Storage::NoveltyOnly, Storage::Indexed];

/// A ledger holding `data`, loaded by a second instance from novelty only or
/// from a published index.
async fn load(ledger_id: &str, data: &Value, storage: Storage) -> (MemoryFluree, MemoryLedger) {
    let (writer, reader) = instances_sharing_storage();
    let ledger = genesis_ledger(&writer, ledger_id);
    writer.insert(ledger, data).await.expect("seed");
    if let Storage::Indexed = storage {
        rebuild_and_publish_index(&writer, ledger_id).await;
    }
    let ledger = reader.ledger(ledger_id).await.expect("reader load");
    (reader, ledger)
}

async fn rows(fluree: &MemoryFluree, ledger: &MemoryLedger, query: &Value) -> Vec<Value> {
    let rows = query_jsonld(fluree, ledger, query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("format");
    let mut rows = normalize_rows(&rows);
    rows.sort_by_key(ToString::to_string);
    rows
}

/// A derived fact that is also asserted is one fact, so a count over the
/// reasoned view sees it once.
#[tokio::test]
async fn a_derived_fact_that_is_also_asserted_counts_once() {
    for storage in STORAGE {
        let ledger_id = format!("datalog/agg-duplicate-{storage:?}:main").to_lowercase();
        let data = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{
                "@id": "ex:a",
                "ex:friend": [{"@id": "ex:b"}, {"@id": "ex:c"}],
                "ex:knows": {"@id": "ex:b"}
            }]
        });
        let (fluree, ledger) = load(&ledger_id, &data, storage).await;
        let query = json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["(as (count ?y) ?n)"],
            "where": {"@id": "ex:a", "ex:knows": "?y"},
            "reasoning": "datalog",
            "rules": [{
                "@context": {"ex": "http://example.org/"},
                "where": {"@id": "?x", "ex:friend": "?y"},
                "insert": {"@id": "?x", "ex:knows": {"@id": "?y"}}
            }]
        });
        assert_eq!(
            rows(&fluree, &ledger, &query).await,
            vec![json!([2])],
            "{storage:?}"
        );
    }
}

fn ctx() -> Value {
    json!({"ex": "http://example.org/"})
}

fn accounts() -> Value {
    json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:a1", "@type": "ex:Account"},
            {"@id": "ex:a2", "@type": "ex:Account"},
            {"@id": "ex:a3", "@type": "ex:Account"},
            {"@id": "ex:t1", "ex:account": {"@id": "ex:a1"}},
            {"@id": "ex:t2", "ex:account": {"@id": "ex:a1"}},
            {"@id": "ex:t3", "ex:account": {"@id": "ex:a1"}},
            {"@id": "ex:t4", "ex:account": {"@id": "ex:a2"}}
        ]
    })
}

fn txn_counts_query(rules: Value) -> Value {
    json!({
        "@context": ctx(),
        "select": ["?a", "?n"],
        "where": {"@id": "?a", "ex:txnCount": "?n"},
        "reasoning": "datalog",
        "rules": rules
    })
}

/// One fact per group. An account with no transactions has no group, so it
/// gets no fact: a missing group is not a zero.
#[tokio::test]
async fn a_grouped_count_derives_one_fact_per_group() {
    for storage in STORAGE {
        let ledger_id = format!("datalog/agg-count-{storage:?}:main").to_lowercase();
        let (fluree, ledger) = load(&ledger_id, &accounts(), storage).await;
        let query = txn_counts_query(json!([{
            "@context": ctx(),
            "where": [["query", {
                "@context": ctx(),
                "select": ["?acct", "(as (count ?t) ?n)"],
                "where": {"@id": "?t", "ex:account": "?acct"},
                "groupBy": ["?acct"]
            }]],
            "insert": {"@id": "?acct", "ex:txnCount": "?n"}
        }]));
        assert_eq!(
            rows(&fluree, &ledger, &query).await,
            vec![json!(["ex:a1", 3]), json!(["ex:a2", 1])],
            "{storage:?}"
        );
    }
}

/// The SPARQL twin, as a stored rule.
#[tokio::test]
async fn a_sparql_aggregate_subquery_counts_per_group() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "datalog/agg-sparql:main");
    let ledger = fluree
        .insert(ledger, &accounts())
        .await
        .expect("seed")
        .ledger;
    let rule = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": "http://example.org/countRule",
        "f:rule": {
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?acct ex:txnCount ?n } WHERE { \
                       { SELECT ?acct (COUNT(?t) AS ?n) WHERE { ?t ex:account ?acct } \
                         GROUP BY ?acct } }"
        }
    });
    let ledger = fluree.insert(ledger, &rule).await.expect("rule").ledger;
    let rows = crate::support::query_sparql(
        &fluree,
        &ledger,
        "# PRAGMA reasoning: datalog\nPREFIX ex: <http://example.org/>\n\
         SELECT ?a ?n WHERE { ?a ex:txnCount ?n } ORDER BY ?a",
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("format");
    assert_eq!(
        normalize_rows(&rows),
        vec![json!(["ex:a1", 3]), json!(["ex:a2", 1])]
    );
}

/// An aggregate over a recursive relation counts the finished closure. The
/// recursion takes many passes; a count taken any earlier would be low.
#[tokio::test]
async fn an_aggregate_counts_a_recursive_relation_only_once_it_is_complete() {
    const LEN: usize = 10;
    let ledger = crate::it_datalog_rule_schedule::ledger_with(
        "datalog/agg-closure:main",
        crate::it_datalog_rule_schedule::chain_data(LEN),
    )
    .await;
    let rules = crate::it_datalog_rule_schedule::sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:descendants ?n } WHERE { \
             { SELECT ?x (COUNT(?y) AS ?n) WHERE { ?y ex:ancestor ?x } GROUP BY ?x } }"
                .to_string(),
            "CONSTRUCT { ?x ex:ancestor ?y } WHERE { ?x ex:parent ?y }".to_string(),
            "CONSTRUCT { ?x ex:ancestor ?z } WHERE { ?x ex:parent ?y . ?y ex:ancestor ?z \
             FILTER(?x != ?z) }"
                .to_string(),
        ],
    );
    let result = crate::it_datalog_rule_schedule::run(
        &ledger,
        &rules,
        fluree_db_query::datalog_rules::RuleSchedule::Dependencies,
    )
    .await;
    let mut counts: Vec<(String, String)> =
        crate::it_datalog_semi_naive::derived(&result, &ledger, "descendants");
    counts.sort();
    // Chain n0 -> n1 -> … -> n9: node n{i} has i descendants.
    let mut expected: Vec<(String, String)> = (1..LEN)
        .map(|i| (format!("n{i}"), format!("Long({i})")))
        .collect();
    expected.sort();
    assert_eq!(counts, expected);
}

/// Subquery solutions are a bag: a transaction reached through two of a
/// person's accounts counts twice unless the count says DISTINCT.
#[tokio::test]
async fn counts_are_over_solutions_unless_distinct() {
    let (fluree, ledger) = load(
        "datalog/agg-distinct:main",
        &json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:a1", "ex:owner": {"@id": "ex:pat"}},
                {"@id": "ex:a2", "ex:owner": {"@id": "ex:pat"}},
                {"@id": "ex:t1", "ex:account": [{"@id": "ex:a1"}, {"@id": "ex:a2"}]},
                {"@id": "ex:t2", "ex:account": {"@id": "ex:a1"}}
            ]
        }),
        Storage::NoveltyOnly,
    )
    .await;
    let rule = |count: &str, predicate: &str| {
        json!({
            "@context": ctx(),
            "where": [["query", {
                "@context": ctx(),
                "select": ["?p", format!("(as ({count} ?t) ?n)")],
                "where": [
                    {"@id": "?acct", "ex:owner": "?p"},
                    {"@id": "?t", "ex:account": "?acct"}
                ],
                "groupBy": ["?p"]
            }]],
            "insert": {"@id": "?p", predicate: "?n"}
        })
    };
    let query = json!({
        "@context": ctx(),
        "select": ["?solutions", "?distinct"],
        "where": {"@id": "ex:pat", "ex:solutions": "?solutions", "ex:distinctTxns": "?distinct"},
        "reasoning": "datalog",
        "rules": [rule("count", "ex:solutions"), rule("count-distinct", "ex:distinctTxns")]
    });
    assert_eq!(rows(&fluree, &ledger, &query).await, vec![json!([3, 2])]);
}

/// Inside a rule too, a derived fact that is also asserted is one fact: the
/// count over the lower stratum's output is not inflated by it.
#[tokio::test]
async fn an_aggregating_rule_counts_a_derived_and_asserted_fact_once() {
    for storage in STORAGE {
        let ledger_id = format!("datalog/agg-rule-duplicate-{storage:?}:main").to_lowercase();
        let data = json!({
            "@context": ctx(),
            "@graph": [{
                "@id": "ex:a",
                "ex:friend": [{"@id": "ex:b"}, {"@id": "ex:c"}],
                "ex:knows": {"@id": "ex:b"}
            }]
        });
        let (fluree, ledger) = load(&ledger_id, &data, storage).await;
        let query = json!({
            "@context": ctx(),
            "select": "?n",
            "where": {"@id": "ex:a", "ex:knowsCount": "?n"},
            "reasoning": "datalog",
            "rules": [
                {
                    "@context": ctx(),
                    "where": {"@id": "?x", "ex:friend": "?y"},
                    "insert": {"@id": "?x", "ex:knows": {"@id": "?y"}}
                },
                {
                    "@context": ctx(),
                    "where": [["query", {
                        "@context": ctx(),
                        "select": ["?x", "(as (count ?y) ?n)"],
                        "where": {"@id": "?x", "ex:knows": "?y"},
                        "groupBy": ["?x"]
                    }]],
                    "insert": {"@id": "?x", "ex:knowsCount": "?n"}
                }
            ]
        });
        assert_eq!(
            rows(&fluree, &ledger, &query).await,
            vec![json!(2)],
            "{storage:?}"
        );
    }
}

/// Without GROUP BY the whole input is one group, so a count over nothing is
/// zero rather than no fact.
#[tokio::test]
async fn an_implicit_group_counts_zero() {
    let (fluree, ledger) = load(
        "datalog/agg-implicit:main",
        &accounts(),
        Storage::NoveltyOnly,
    )
    .await;
    let query = json!({
        "@context": ctx(),
        "select": "?n",
        "where": {"@id": "ex:stats", "ex:flagged": "?n"},
        "reasoning": "datalog",
        "rules": [{
            "@context": ctx(),
            "where": [["query", {
                "@context": ctx(),
                "select": ["(as (count ?t) ?n)"],
                "where": {"@id": "?t", "@type": "ex:Flagged"}
            }]],
            "insert": {"@id": "ex:stats", "ex:flagged": "?n"}
        }]
    });
    assert_eq!(rows(&fluree, &ledger, &query).await, vec![json!(0)]);
}

/// A run whose budget stops before an aggregating rule finishes fails: a
/// count over part of its input would be wrong, not merely missing.
#[tokio::test]
async fn a_budget_that_stops_before_an_aggregating_rule_fails_the_run() {
    let ledger = crate::it_datalog_rule_schedule::ledger_with(
        "datalog/agg-capped:main",
        crate::it_datalog_rule_schedule::chain_data(12),
    )
    .await;
    let rules = crate::it_datalog_rule_schedule::sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:ancestor ?y } WHERE { ?x ex:parent ?y }".to_string(),
            "CONSTRUCT { ?x ex:ancestor ?z } WHERE { ?x ex:parent ?y . ?y ex:ancestor ?z \
             FILTER(?x != ?z) }"
                .to_string(),
            "CONSTRUCT { ?x ex:descendants ?n } WHERE { \
             { SELECT ?x (COUNT(?y) AS ?n) WHERE { ?y ex:ancestor ?x } GROUP BY ?x } }"
                .to_string(),
        ],
    );
    let budget = fluree_db_query::reasoning::ReasoningBudget::new(
        std::time::Duration::from_secs(60),
        20,
        usize::MAX,
    );
    let err = fluree_db_query::datalog_rules::run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        &rules,
        100,
        &budget,
        fluree_db_query::datalog_rules::RuleExecutionEnv::default(),
        fluree_db_query::datalog_rules::RuleSchedule::Dependencies,
    )
    .await
    .expect_err("the count cannot be taken over a complete closure");
    let message = err.to_string();
    assert!(
        message.contains("ex:rule02") && message.contains("an aggregate"),
        "{message}"
    );
}

/// GROUP BY at the top of a body leaves the head only the group keys; the
/// error points at the subquery form.
#[tokio::test]
async fn a_top_level_group_by_points_at_a_subquery() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "datalog/agg-top-level:main");
    let rule = json!({
        "@id": "ex:topLevel",
        "f:rule": {
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?acct ex:active true } WHERE { ?t ex:account ?acct } \
                       GROUP BY ?acct"
        }
    });
    let err = fluree_db_query::datalog_rules::parse_query_time_rule(&rule, &ledger.snapshot, 0)
        .expect_err("top-level GROUP BY is rejected");
    assert!(err.to_string().contains("subquery"), "{err}");
}

/// Aggregation counts IRIs as stated. OWL 2 RL does not rewrite an asserted
/// `owl:sameAs` into its aliases' facts, so each alias counts the
/// transactions stated about it and none is counted twice. If equality
/// handling ever merges aliases, this test is where the counting semantics
/// get decided again.
#[tokio::test]
async fn same_as_aliases_do_not_inflate_counts() {
    let (fluree, ledger) = load(
        "datalog/agg-same-as:main",
        &json!({
            "@context": {"ex": "http://example.org/", "owl": "http://www.w3.org/2002/07/owl#"},
            "@graph": [
                {"@id": "ex:acctA", "owl:sameAs": {"@id": "ex:acctB"}},
                {"@id": "ex:t1", "ex:account": {"@id": "ex:acctA"}},
                {"@id": "ex:t2", "ex:account": {"@id": "ex:acctB"}},
                {"@id": "ex:t3", "ex:account": {"@id": "ex:acctC"}}
            ]
        }),
        Storage::NoveltyOnly,
    )
    .await;
    for reasoning in [json!("datalog"), json!(["owl2rl", "datalog"])] {
        let query = txn_counts_query(json!([{
            "@context": ctx(),
            "where": [["query", {
                "@context": ctx(),
                "select": ["?acct", "(as (count ?t) ?n)"],
                "where": {"@id": "?t", "ex:account": "?acct"},
                "groupBy": ["?acct"]
            }]],
            "insert": {"@id": "?acct", "ex:txnCount": "?n"}
        }]));
        let mut query = query;
        query["reasoning"] = reasoning.clone();
        assert_eq!(
            rows(&fluree, &ledger, &query).await,
            vec![
                json!(["ex:acctA", 1]),
                json!(["ex:acctB", 1]),
                json!(["ex:acctC", 1])
            ],
            "{reasoning}"
        );
    }
}

/// Aggregates other than COUNT, and HAVING, behave as in queries.
#[tokio::test]
async fn sum_and_having_work_in_a_rule_subquery() {
    let ledger = crate::it_datalog_rule_schedule::ledger_with(
        "datalog/agg-sum-having:main",
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:t1", "ex:account": {"@id": "ex:a1"}, "ex:amount": 70},
                {"@id": "ex:t2", "ex:account": {"@id": "ex:a1"}, "ex:amount": 50},
                {"@id": "ex:t3", "ex:account": {"@id": "ex:a2"}, "ex:amount": 90}
            ]
        }),
    )
    .await;
    let rules = crate::it_datalog_rule_schedule::sparql_rules(
        &ledger,
        &["CONSTRUCT { ?acct ex:largeTotal ?total } WHERE { \
           { SELECT ?acct (SUM(?amt) AS ?total) WHERE { ?t ex:account ?acct . ?t ex:amount ?amt } \
             GROUP BY ?acct HAVING (SUM(?amt) > 100) } }"
            .to_string()],
    );
    let result = crate::it_datalog_rule_schedule::run(
        &ledger,
        &rules,
        fluree_db_query::datalog_rules::RuleSchedule::Dependencies,
    )
    .await;
    assert_eq!(
        crate::it_datalog_semi_naive::derived(&result, &ledger, "largeTotal"),
        vec![("a1".to_string(), "Long(120)".to_string())]
    );
}

/// Under `rdfs` without `owl2rl`, a rule body counts only the stored
/// employee while the query's own `COUNT` also counts the manager, so a
/// counting program is refused. With `owl2rl` the rule's count is the query's.
#[tokio::test]
async fn a_count_under_rdfs_without_owl2rl_is_refused() {
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {
                "@id": "ex:Manager",
                "http://www.w3.org/2000/01/rdf-schema#subClassOf": {"@id": "ex:Employee"}
            },
            {"@id": "ex:ann", "@type": "ex:Manager"},
            {"@id": "ex:cy", "@type": "ex:Employee"}
        ]
    });
    let (fluree, ledger) = load(
        "datalog/agg-rewrite-modes:main",
        &data,
        Storage::NoveltyOnly,
    )
    .await;
    let rule = json!({
        "@id": "ex:headcountRule",
        "@context": {"ex": "http://example.org/"},
        "where": [["query", {
            "@context": {"ex": "http://example.org/"},
            "select": ["(as (count ?e) ?n)"],
            "where": {"@id": "?e", "@type": "ex:Employee"}
        }]],
        "insert": {"@id": "ex:acme", "ex:headcount": "?n"}
    });
    let query = |reasoning: &Value, select: Value, where_: Value| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "select": select,
            "where": where_,
            "reasoning": reasoning,
            "rules": [rule.clone()]
        })
    };
    let headcount = |reasoning: &Value| {
        query(
            reasoning,
            json!("?n"),
            json!({"@id": "ex:acme", "ex:headcount": "?n"}),
        )
    };
    let employees = |reasoning: &Value| {
        query(
            reasoning,
            json!(["(as (count ?e) ?m)"]),
            json!({"@id": "?e", "@type": "ex:Employee"}),
        )
    };

    let without = json!(["rdfs", "datalog"]);
    let err = query_jsonld(&fluree, &ledger, &headcount(&without))
        .await
        .expect_err("a count under a query rewrite");
    let message = err.to_string();
    assert!(
        message.contains("ex:headcountRule")
            && message.contains("an aggregate")
            && message.contains("rdfs reasoning is on without owl2rl"),
        "{message}"
    );

    let with = json!(["rdfs", "owl2rl", "datalog"]);
    assert_eq!(
        rows(&fluree, &ledger, &headcount(&with)).await,
        vec![json!(2)]
    );
    assert_eq!(
        rows(&fluree, &ledger, &employees(&with)).await,
        vec![json!([2])]
    );
}
