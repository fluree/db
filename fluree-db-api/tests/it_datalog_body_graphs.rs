//! What a rule body can read: the graphs of the ledger its rules run against,
//! over a default graph of one graph.
//!
//! A body `GRAPH <iri>` naming anything else (a graph the ledger does not
//! have, another ledger, a system graph) used to yield no rows, and under a
//! negation that reads as "absent for everyone", so a wiring mistake derived a
//! full, plausible set of conclusions. Those rule sets are refused now, naming
//! the rule and the graph, as a head naming a missing graph already was. So is
//! a rule set run over a default graph that merges several graphs, of which the
//! rules would read one. (A graph source named in a body is covered in
//! `it_iceberg_local_fs.rs`, which has the fixtures to register one.)

use crate::support::normalize_rows;
use fluree_db_api::{ApiError, Fluree, FlureeBuilder};
use serde_json::{json, Value};

const DATA: &str = "body-graphs/data:main";
const OTHER: &str = "body-graphs/other:main";

/// `data` has three accounts. Carol's exclusion is in its default graph,
/// Alice's in its named graph `ex:exclusions`; `other` excludes Bob, in its
/// default graph.
async fn ledgers() -> Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    let ctx = json!({"ex": "http://example.org/"});
    let data = fluree.create_ledger(DATA).await.expect("data");
    let data = fluree
        .insert(
            data,
            &json!({"@context": ctx, "@graph": [
                {"@id": "ex:alice", "@type": "ex:Account"},
                {"@id": "ex:bob", "@type": "ex:Account"},
                {"@id": "ex:carol", "@type": "ex:Account", "ex:excluded": true}
            ]}),
        )
        .await
        .expect("accounts")
        .ledger;
    fluree
        .insert(
            data,
            &json!({"@context": ctx, "@id": "ex:exclusions",
                    "@graph": [{"@id": "ex:alice", "ex:excluded": true}]}),
        )
        .await
        .expect("exclusions");
    let other = fluree.create_ledger(OTHER).await.expect("other");
    fluree
        .insert(
            other,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:bob", "ex:excluded": true}]}),
        )
        .await
        .expect("other exclusions");
    fluree
}

/// The issue's rule: an account is a candidate unless graph `graph` excludes
/// it.
fn candidate_rule(graph: &str) -> Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:candidate",
        "where": [
            {"@id": "?a", "@type": "ex:Account"},
            ["not-exists", ["graph", graph, {"@id": "?a", "ex:excluded": "?x"}]]
        ],
        "insert": {"@id": "?a", "@type": "ex:Candidate"}
    })
}

fn candidates_query(rules: Value) -> Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?a",
        "where": {"@id": "?a", "@type": "ex:Candidate"},
        "reasoning": "datalog",
        "rules": rules
    })
}

async fn candidates(fluree: &Fluree, rules: Value) -> Result<Vec<Value>, ApiError> {
    let rows = fluree
        .graph(DATA)
        .query()
        .jsonld(&candidates_query(rules))
        .execute_formatted()
        .await?;
    let mut rows = normalize_rows(&rows);
    rows.sort_by_key(ToString::to_string);
    Ok(rows)
}

async fn sparql_candidates(fluree: &Fluree, rule: &str) -> Result<Vec<Value>, ApiError> {
    let rules = json!([{"@context": {"f": "https://ns.flur.ee/db#"},
                        "@id": "http://example.org/candidate",
                        "f:rule": {"@type": "f:sparql",
                                   "@value": format!("PREFIX ex: <http://example.org/> {rule}")}}]);
    candidates(fluree, rules).await
}

/// The refusal: a 400 naming the rule and the graph, saying why.
fn assert_refused(result: Result<Vec<Value>, ApiError>, graph: &str, why: &str) {
    let err = match result {
        Ok(rows) => panic!("<{graph}>: answered {rows:?} instead of refusing"),
        Err(err) => err,
    };
    assert_eq!(err.status_code(), 400, "<{graph}>: {err}");
    let message = err.to_string();
    assert!(
        message.contains("datalog rule") && message.contains("candidate"),
        "<{graph}>: the message names the rule: {message}"
    );
    assert!(
        message.contains(&format!("reads graph <{graph}>")),
        "<{graph}>: the message names the graph: {message}"
    );
    assert!(message.contains(why), "<{graph}>: {message}");
}

#[tokio::test]
async fn a_body_reads_the_graphs_of_its_own_ledger() {
    let fluree = ledgers().await;
    // The named graph that holds Alice's exclusion.
    assert_eq!(
        candidates(&fluree, json!([candidate_rule("ex:exclusions")]))
            .await
            .expect("named graph"),
        vec![json!("ex:bob"), json!("ex:carol")]
    );
    // The ledger's own address is its default graph, which holds Carol's.
    assert_eq!(
        candidates(&fluree, json!([candidate_rule(DATA)]))
            .await
            .expect("own address"),
        vec![json!("ex:alice"), json!("ex:bob")]
    );
    // `GRAPH ?g` ranges over the named graphs, unchanged.
    assert_eq!(
        candidates(&fluree, json!([candidate_rule("?g")]))
            .await
            .expect("graph variable"),
        vec![json!("ex:bob"), json!("ex:carol")]
    );
}

/// The issue's table: a typo'd graph IRI and another ledger each derived every
/// account a candidate.
#[tokio::test]
async fn a_body_naming_a_graph_its_ledger_does_not_have_is_refused() {
    let fluree = ledgers().await;
    assert_refused(
        candidates(&fluree, json!([candidate_rule("ex:exclusionz")])).await,
        "http://example.org/exclusionz",
        &format!("which ledger '{DATA}' does not have as of t="),
    );
    assert_refused(
        candidates(&fluree, json!([candidate_rule(OTHER)])).await,
        OTHER,
        &format!("which names another ledger ('{OTHER}')"),
    );
    // A compact IRI with no prefix reads as an IRI and as an address both.
    assert_refused(
        candidates(&fluree, json!([candidate_rule("other:main")])).await,
        "other:main",
        "and as a ledger address names another ledger ('other:main')",
    );
    let txn_meta = format!("urn:fluree:{DATA}#txn-meta");
    assert_refused(
        candidates(&fluree, json!([candidate_rule(&txn_meta)])).await,
        &txn_meta,
        "which is the txn-meta graph of ledger",
    );
}

/// Wherever the `GRAPH` sits in the body, at any depth, on either surface.
#[tokio::test]
async fn a_body_graph_is_checked_at_any_depth() {
    let fluree = ledgers().await;
    let typo = "http://example.org/exclusionz";
    let missing = "does not have as of t=";

    // JSON-LD: a positive GRAPH, a UNION branch, a subquery.
    for body in [
        json!([["graph", "ex:exclusionz", {"@id": "?a", "@type": "ex:Account"}]]),
        json!([["union",
                [{"@id": "?a", "@type": "ex:Account"}],
                [["graph", "ex:exclusionz", {"@id": "?a", "@type": "ex:Account"}]]]]),
        json!([{"@id": "?a", "@type": "ex:Account"},
               ["query", {"@context": {"ex": "http://example.org/"},
                          "select": ["?a"],
                          "where": [["graph", "ex:exclusionz", {"@id": "?a", "ex:excluded": "?x"}]]}]]),
        // A GRAPH that reads no fact still yields no rows for a missing graph.
        json!([{"@id": "?a", "@type": "ex:Account"},
               ["graph", "ex:exclusionz", ["bind", "?b", "?a"]]]),
    ] {
        let rule = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:candidate",
            "where": body,
            "insert": {"@id": "?a", "@type": "ex:Candidate"}
        });
        assert_refused(candidates(&fluree, json!([rule])).await, typo, missing);
    }

    // SPARQL: FILTER NOT EXISTS and EXISTS, a UNION branch, a subquery.
    for rule in [
        "CONSTRUCT { ?a a ex:Candidate } WHERE { ?a a ex:Account \
         FILTER NOT EXISTS { GRAPH ex:exclusionz { ?a ex:excluded ?x } } }",
        "CONSTRUCT { ?a a ex:Candidate } WHERE { ?a a ex:Account \
         FILTER (!EXISTS { GRAPH ex:exclusionz { ?a ex:excluded ?x } }) }",
        "CONSTRUCT { ?a a ex:Candidate } WHERE { { ?a a ex:Account } UNION \
         { GRAPH ex:exclusionz { ?a a ex:Account } } }",
        "CONSTRUCT { ?a a ex:Candidate } WHERE { ?a a ex:Account \
         { SELECT ?a WHERE { GRAPH ex:exclusionz { ?a ex:excluded ?x } } } }",
    ] {
        assert_refused(sparql_candidates(&fluree, rule).await, typo, missing);
    }
    // The SPARQL spelling of the working rule still works.
    assert_eq!(
        sparql_candidates(
            &fluree,
            "CONSTRUCT { ?a a ex:Candidate } WHERE { ?a a ex:Account \
             FILTER NOT EXISTS { GRAPH ex:exclusions { ?a ex:excluded ?x } } }"
        )
        .await
        .expect("sparql rule"),
        vec![json!("ex:bob"), json!("ex:carol")]
    );
}

/// A stored rule is refused as a query-time one is, on the SPARQL query
/// surface too.
#[tokio::test]
async fn a_stored_rule_naming_a_missing_graph_is_refused() {
    let fluree = ledgers().await;
    let ledger = fluree.ledger(DATA).await.expect("load");
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "http://example.org/candidate",
                "f:rule": {"@type": "@json", "@value": candidate_rule("ex:exclusionz")}
            }),
        )
        .await
        .expect("store rule");
    let sparql = "# PRAGMA reasoning: datalog\nPREFIX ex: <http://example.org/>\n\
                  SELECT ?a WHERE { ?a a ex:Candidate }";
    let err = fluree
        .graph(DATA)
        .query()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect_err("refused");
    assert_eq!(err.status_code(), 400, "{err}");
    assert!(
        err.to_string()
            .contains("reads graph <http://example.org/exclusionz>, which ledger"),
        "{err}"
    );
}

/// A rule set written before its input graph has data fails until the graph
/// exists, as a head's graph does.
#[tokio::test]
async fn a_rule_runs_once_the_graph_it_reads_exists() {
    let fluree = ledgers().await;
    let rules = json!([candidate_rule("ex:later")]);
    assert_refused(
        candidates(&fluree, rules.clone()).await,
        "http://example.org/later",
        "a named graph exists once data is transacted into it",
    );
    let ledger = fluree.ledger(DATA).await.expect("load");
    fluree
        .insert(
            ledger,
            &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:later",
                    "@graph": [{"@id": "ex:bob", "ex:excluded": true}]}),
        )
        .await
        .expect("create graph");
    assert_eq!(
        candidates(&fluree, rules).await.expect("graph exists"),
        vec![json!("ex:alice"), json!("ex:carol")]
    );
}

/// Rows of a `from` query for candidates under `rules`.
async fn dataset_candidates(
    fluree: &Fluree,
    from: Value,
    rules: Option<Value>,
) -> Result<Vec<Value>, ApiError> {
    let mut query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": from,
        "select": "?a",
        "where": {"@id": "?a", "@type": "?type"},
        "reasoning": "datalog"
    });
    if let Some(rules) = rules {
        query["rules"] = rules;
        query["where"] = json!({"@id": "?a", "@type": "ex:Candidate"});
    }
    let rows = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await?;
    let mut rows = normalize_rows(&rows);
    rows.sort_by_key(ToString::to_string);
    rows.dedup();
    Ok(rows)
}

fn assert_several_graphs_refused(result: Result<Vec<Value>, ApiError>, from: &Value) {
    let err = match result {
        Ok(rows) => panic!("{from}: answered {rows:?} instead of refusing"),
        Err(err) => err,
    };
    assert_eq!(err.status_code(), 400, "{from}: {err}");
    let message = err.to_string();
    assert!(
        message.contains("default graph merges several graphs")
            && message.contains("candidate")
            && message.contains("run over one graph"),
        "{from}: {message}"
    );
}

/// A default graph that spans two ledgers: a plain query joins across both,
/// but the rules read only the first, so the rule set is refused in either
/// order. A default graph of two graphs of one ledger is the same shape.
#[tokio::test]
async fn rules_over_a_default_graph_of_several_graphs_are_refused() {
    let fluree = ledgers().await;
    // No `GRAPH`: the rule reads exclusions from the default graph.
    let rule = json!([{
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:candidate",
        "where": [
            {"@id": "?a", "@type": "ex:Account"},
            ["not-exists", {"@id": "?a", "ex:excluded": "?x"}]
        ],
        "insert": {"@id": "?a", "@type": "ex:Candidate"}
    }]);

    // One graph: the rules run, and the answer is cached.
    assert_eq!(
        dataset_candidates(&fluree, json!(DATA), Some(rule.clone()))
            .await
            .expect("one ledger"),
        vec![json!("ex:alice"), json!("ex:bob")]
    );
    // Several: refused before the cache could serve the one-graph answer.
    for from in [
        json!([DATA, OTHER]),
        json!([OTHER, DATA]),
        json!([DATA, {"@id": DATA, "graph": "http://example.org/exclusions"}]),
    ] {
        assert_several_graphs_refused(
            dataset_candidates(&fluree, from.clone(), Some(rule.clone())).await,
            &from,
        );
    }
    // With no rules to run, datalog over several ledgers answers as before.
    let rows = dataset_candidates(&fluree, json!([DATA, OTHER]), None)
        .await
        .expect("no rules");
    assert_eq!(
        rows,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );
}

/// The SPARQL twin: stored rules, `FROM` both ledgers. A dataset query runs
/// the stored rules of its first ledger, so each ledger stores the rule for
/// both orders to have rules to run.
#[tokio::test]
async fn sparql_from_several_ledgers_refuses_stored_rules() {
    let fluree = ledgers().await;
    for id in [DATA, OTHER] {
        let ledger = fluree.ledger(id).await.expect("load");
        fluree
            .insert(
                ledger,
                &json!({
                    "@context": {"f": "https://ns.flur.ee/db#"},
                    "@id": "http://example.org/candidate",
                    "f:rule": {"@type": "f:sparql", "@value":
                        "PREFIX ex: <http://example.org/> CONSTRUCT { ?a a ex:Candidate } \
                         WHERE { ?a a ex:Account FILTER NOT EXISTS { ?a ex:excluded ?x } }"}
                }),
            )
            .await
            .expect("store rule");
    }
    for (first, second) in [(DATA, OTHER), (OTHER, DATA)] {
        let sparql = format!(
            "# PRAGMA reasoning: datalog\nPREFIX ex: <http://example.org/>\n\
             SELECT ?a FROM <{first}> FROM <{second}> WHERE {{ ?a a ex:Candidate }}"
        );
        let result = fluree
            .query_from()
            .sparql(&sparql)
            .execute_formatted()
            .await;
        let from = json!([first, second]);
        assert_several_graphs_refused(result.map(|rows| normalize_rows(&rows)), &from);
    }
}
