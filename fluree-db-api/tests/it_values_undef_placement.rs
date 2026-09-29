//! BIND and FILTER placement over variables a VALUES UNDEF cell, an OPTIONAL or
//! a per-row seed leaves unbound on some rows, while a triple of the same group
//! still binds them.
//!
//! The planner treated "listed in the upstream schema" as "bound", so it ran
//! such a BIND or FILTER before that triple: `BIND(STR(?payer) AS ?name)` came
//! back unbound, and a FILTER reading `?payer` dropped every row. A range FILTER
//! on another triple's object exposed it, by moving that triple to the front of
//! the join chain.

use crate::support::genesis_ledger;
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

const LEDGER_ID: &str = "values-undef-placement:main";
const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n";

/// Five claims; payers alternate between `payer/0` and `payer/1`.
async fn seed(fluree: &fluree_db_api::Fluree, indexed: bool) {
    let graph: Vec<JsonValue> = (0..5)
        .map(|i| {
            json!({
                "@id": format!("ex:claim{i}"),
                "ex:payer": {"@id": format!("ex:payer{}", i % 2)},
                "ex:patientAge": 10 + i,
            })
        })
        .collect();
    fluree
        .insert(
            genesis_ledger(fluree, LEDGER_ID),
            &json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": graph}),
        )
        .await
        .expect("seed insert");
    if indexed {
        crate::support::rebuild_and_publish_index(fluree, LEDGER_ID).await;
    }
}

fn sparql(body: &str) -> String {
    format!(
        "{PREFIX}{}",
        body.replacen("WHERE", &format!("FROM <{LEDGER_ID}> WHERE"), 1)
    )
}

/// Rows of `vars`, each value as its lexical form (`None` when unbound), sorted.
async fn rows(
    fluree: &fluree_db_api::Fluree,
    body: &str,
    vars: &[&str],
) -> Vec<Vec<Option<String>>> {
    let result = fluree
        .query_from()
        .sparql(&sparql(body))
        .execute_tracked()
        .await
        .expect("query should succeed");
    let mut out: Vec<Vec<Option<String>>> = result.result["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .iter()
        .map(|row| {
            vars.iter()
                .map(|v| row[*v]["value"].as_str().map(str::to_string))
                .collect()
        })
        .collect();
    out.sort();
    out
}

async fn plan_ops(fluree: &fluree_db_api::Fluree, body: &str) -> Vec<String> {
    let view = fluree.db(LEDGER_ID).await.expect("view");
    let plan = fluree
        .explain_sparql(&view, &sparql(body))
        .await
        .expect("explain");
    fn walk(node: &JsonValue, out: &mut Vec<String>) {
        if let Some(op) = node["op"].as_str() {
            let right = node["details"]["right"].as_str().unwrap_or_default();
            out.push(format!("{op} {right}").trim_end().to_string());
        }
        for edge in node["children"].as_array().into_iter().flatten() {
            walk(&edge["node"], out);
        }
    }
    let mut ops = Vec::new();
    walk(&plan["plan"]["physical"], &mut ops);
    ops
}

/// `ops` with each `?vN` renamed by order of first appearance, so two plans
/// that differ only in variable registration order compare equal.
fn renumber_vars(ops: &[String]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    ops.iter()
        .map(|op| {
            op.split(' ')
                .map(|word| {
                    if !word.starts_with("?v") {
                        return word.to_string();
                    }
                    let at = seen.iter().position(|w| w == word).unwrap_or_else(|| {
                        seen.push(word.to_string());
                        seen.len() - 1
                    });
                    format!("?v{at}")
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

fn payer(i: usize) -> Option<String> {
    Some(format!("http://example.org/ns/payer{i}"))
}

fn claim(i: usize) -> Option<String> {
    Some(format!("http://example.org/ns/claim{i}"))
}

/// `?payer` and `STR(?payer)` for every claim, plus the claims of `payer1`
/// again for the table's second row.
fn every_claim_then_payer1_again() -> Vec<Vec<Option<String>>> {
    let mut expected: Vec<Vec<Option<String>>> = (0..5)
        .chain([1, 3])
        .map(|i| vec![claim(i), payer(i % 2), payer(i % 2)])
        .collect();
    expected.sort();
    expected
}

const MIXED_TABLE_BIND: &str = "SELECT ?c ?payer ?name WHERE {
      VALUES ?payer { UNDEF ex:payer1 }
      ?c ex:payer ?payer ; ex:patientAge ?a .
      FILTER(?a > 0)
      BIND(STR(?payer) AS ?name)
    }";

#[tokio::test]
async fn bind_reads_the_value_its_triple_binds_after_an_undef_cell() {
    for indexed in [false, true] {
        let fluree = FlureeBuilder::memory().build_memory();
        seed(&fluree, indexed).await;

        // The range bound on `?a` is what moves `ex:patientAge` ahead of
        // `ex:payer`; without that the bug had nowhere to show.
        let ops = plan_ops(&fluree, MIXED_TABLE_BIND).await;
        let join_at = |pred: &str| {
            ops.iter()
                .position(|op| op.contains(pred))
                .unwrap_or_else(|| panic!("no join on {pred}: {ops:?}"))
        };
        assert!(
            join_at("patientAge") > join_at("payer>"),
            "precondition: the range-bounded triple should drive the chain: {ops:?}"
        );

        assert_eq!(
            rows(&fluree, MIXED_TABLE_BIND, &["c", "payer", "name"]).await,
            every_claim_then_payer1_again(),
            "indexed={indexed}"
        );
    }
}

#[tokio::test]
async fn filter_on_an_undef_values_var_waits_for_its_triple() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, false).await;

    for filter in ["BOUND(?payer)", "STRSTARTS(STR(?payer), 'http')"] {
        let body = format!(
            "SELECT ?c ?payer ?name WHERE {{
              VALUES ?payer {{ UNDEF ex:payer1 }}
              ?c ex:payer ?payer ; ex:patientAge ?a .
              FILTER(?a > 0)
              FILTER({filter})
              BIND(STR(?payer) AS ?name)
            }}"
        );
        assert_eq!(
            rows(&fluree, &body, &["c", "payer", "name"]).await,
            every_claim_then_payer1_again(),
            "FILTER({filter}) must not drop the rows its triple binds"
        );
    }
}

#[tokio::test]
async fn bind_after_optional_reads_the_value_a_later_triple_binds() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, false).await;

    // The OPTIONAL matches nothing, so `?payer` reaches the next join unbound.
    let body = "SELECT ?c ?payer ?name WHERE {
      ?c ex:patientAge ?a .
      OPTIONAL { ?c ex:none ?payer }
      ?c ex:patientAge ?a2 ; ex:payer ?payer .
      FILTER(?a2 > 0)
      BIND(STR(?payer) AS ?name)
    }";
    let mut expected: Vec<Vec<Option<String>>> = (0..5)
        .map(|i| vec![claim(i), payer(i % 2), payer(i % 2)])
        .collect();
    expected.sort();
    assert_eq!(rows(&fluree, body, &["c", "payer", "name"]).await, expected);
}

#[tokio::test]
async fn union_branch_seeded_with_an_unbound_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, false).await;

    // Each branch is planned on top of one outer row, `?payer` unbound in it.
    let body = "SELECT ?c ?payer ?name WHERE {
      VALUES ?payer { UNDEF ex:payer1 }
      ?c ex:patientAge ?a .
      { ?c ex:payer ?payer ; ex:patientAge ?a2 . FILTER(?a2 > 0) BIND(STR(?payer) AS ?name) }
      UNION
      { ?c ex:none ?z }
    }";
    assert_eq!(
        rows(&fluree, body, &["c", "payer", "name"]).await,
        every_claim_then_payer1_again()
    );
}

#[tokio::test]
async fn all_undef_values_plans_like_the_query_without_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, true).await;

    let with_undef = "SELECT ?payer ?name WHERE {
      VALUES ?payer { UNDEF }
      ?c ex:payer ?payer ; ex:patientAge ?a .
      FILTER(?a > 0)
      BIND(STR(?payer) AS ?name)
    } LIMIT 1";
    let without = "SELECT ?payer ?name WHERE {
      ?c ex:payer ?payer ; ex:patientAge ?a .
      FILTER(?a > 0)
      BIND(STR(?payer) AS ?name)
    } LIMIT 1";

    assert_eq!(
        renumber_vars(&plan_ops(&fluree, with_undef).await),
        renumber_vars(&plan_ops(&fluree, without).await)
    );
    let found = rows(&fluree, with_undef, &["payer", "name"]).await;
    assert_eq!(found.len(), 1);
    assert!(found[0][0].is_some(), "{found:?}");
    assert_eq!(found[0][0], found[0][1], "{found:?}");
}

#[tokio::test]
async fn jsonld_bind_reads_the_value_its_triple_binds_after_an_undef_cell() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, false).await;

    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "from": LEDGER_ID,
        "select": ["?c", "?payer", "?name"],
        "where": [
            ["values", ["?payer", [null, {"@type": "@id", "@value": "ex:payer1"}]]],
            {"@id": "?c", "ex:payer": "?payer", "ex:patientAge": "?a"},
            ["filter", "(> ?a 0)"],
            ["bind", "?name", "(str ?payer)"]
        ]
    });
    let result = fluree
        .query_from()
        .jsonld(&query)
        .execute_tracked()
        .await
        .expect("query should succeed");
    let mut found: Vec<JsonValue> = result.result.as_array().expect("rows").clone();
    found.sort_by_key(ToString::to_string);

    let mut expected: Vec<JsonValue> = (0..5)
        .chain([1, 3])
        .map(|i| {
            json!([
                format!("ex:claim{i}"),
                format!("ex:payer{}", i % 2),
                format!("http://example.org/ns/payer{}", i % 2)
            ])
        })
        .collect();
    expected.sort_by_key(ToString::to_string);
    assert_eq!(found, expected);
}
