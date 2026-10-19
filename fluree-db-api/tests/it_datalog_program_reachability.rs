//! A recursive ownership program checked against an independent oracle.
//!
//! The program mixes two independent recursive relations with acyclic
//! business rules layered over them:
//!
//! - `ex:controlsTransitively`: nonlinear transitive closure of `ex:controls`
//!   over a long chain that feeds a dense cycle.
//! - `ex:associated`: symmetric, nonlinear transitive closure of
//!   `ex:sharesAddress`, independent of control.
//! - `ex:exposedTo`, `ex:ExposedEntity`, `ex:reviewQueue` and
//!   `ex:associatedWithExposed`: run-once rules over those closures, the last
//!   reading both.
//!
//! Each derived relation is compared with the same relation computed in test
//! code from the asserted edges, so a change to how the rules are scheduled or
//! evaluated cannot pass by agreeing with the engine. The
//! program also runs with its rules under permuted ids, which permutes their
//! evaluation order.

use crate::support::{
    genesis_ledger, instances_sharing_storage, normalize_rows, rebuild_and_publish_index,
    MemoryFluree,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;

const CHAIN_LEN: usize = 40;
const CYCLE_LEN: usize = 6;

fn context() -> Value {
    json!({"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"})
}

pub(crate) fn rules() -> Vec<Value> {
    vec![
        json!({
            "where": {"@id": "?x", "ex:controls": "?y"},
            "insert": {"@id": "?x", "ex:controlsTransitively": {"@id": "?y"}}
        }),
        json!({
            "where": [
                {"@id": "?x", "ex:controlsTransitively": "?y"},
                {"@id": "?y", "ex:controlsTransitively": "?z"}
            ],
            "insert": {"@id": "?x", "ex:controlsTransitively": {"@id": "?z"}}
        }),
        json!({
            "where": {"@id": "?x", "ex:sharesAddress": "?y"},
            "insert": {"@id": "?x", "ex:associated": {"@id": "?y"}}
        }),
        json!({
            "where": {"@id": "?x", "ex:associated": "?y"},
            "insert": {"@id": "?y", "ex:associated": {"@id": "?x"}}
        }),
        json!({
            "where": [
                {"@id": "?x", "ex:associated": "?y"},
                {"@id": "?y", "ex:associated": "?z"}
            ],
            "insert": {"@id": "?x", "ex:associated": {"@id": "?z"}}
        }),
        json!({
            "where": [
                {"@id": "?x", "ex:controlsTransitively": "?y"},
                {"@id": "?y", "@type": "ex:SanctionedEntity"}
            ],
            "insert": {"@id": "?x", "ex:exposedTo": {"@id": "?y"}}
        }),
        json!({
            "where": {"@id": "?x", "ex:exposedTo": "?y"},
            "insert": {"@id": "?x", "@type": "ex:ExposedEntity"}
        }),
        json!({
            "where": {"@id": "?x", "@type": "ex:ExposedEntity"},
            "insert": {"@id": "?x", "ex:reviewQueue": {"@id": "ex:sanctionsQueue"}}
        }),
        json!({
            "where": [
                {"@id": "?x", "ex:associated": "?y"},
                {"@id": "?y", "@type": "ex:ExposedEntity"}
            ],
            "insert": {"@id": "?x", "ex:associatedWithExposed": {"@id": "?y"}}
        }),
    ]
}

type RuleId = fn(usize) -> String;

fn program(rule_id: impl Fn(usize) -> String) -> Value {
    let mut graph: Vec<Value> = rules()
        .into_iter()
        .enumerate()
        .map(|(i, mut rule)| {
            rule["@context"] = json!({"ex": "http://example.org/"});
            json!({"@id": rule_id(i), "f:rule": {"@type": "@json", "@value": rule}})
        })
        .collect();

    for i in 0..CHAIN_LEN - 1 {
        graph.push(
            json!({"@id": format!("ex:c{i}"), "ex:controls": {"@id": format!("ex:c{}", i + 1)}}),
        );
    }
    graph.push(json!({"@id": format!("ex:c{}", CHAIN_LEN - 1), "ex:controls": {"@id": "ex:k0"}}));
    for i in 0..CYCLE_LEN {
        graph.push(json!({"@id": format!("ex:k{i}"), "ex:controls": {"@id": format!("ex:k{}", (i + 1) % CYCLE_LEN)}}));
    }
    graph.push(json!({"@id": "ex:k0", "ex:controls": {"@id": "ex:k3"}}));
    graph.push(json!({"@id": "ex:k2", "ex:controls": {"@id": "ex:k5"}}));
    graph.push(json!({"@id": "ex:k4", "@type": "ex:SanctionedEntity"}));
    graph.push(json!({"@id": "ex:s0", "ex:controls": {"@id": "ex:s1"}}));
    graph.push(json!({"@id": "ex:s1", "@type": "ex:SanctionedEntity"}));

    graph.push(json!({"@id": "ex:a0", "ex:sharesAddress": {"@id": "ex:a1"}}));
    graph.push(json!({"@id": "ex:a1", "ex:sharesAddress": {"@id": "ex:a2"}}));
    graph.push(json!({"@id": "ex:a2", "ex:sharesAddress": {"@id": "ex:c35"}}));
    graph.push(json!({"@id": "ex:a3", "ex:sharesAddress": {"@id": "ex:a4"}}));

    json!({"@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"}, "@graph": graph})
}

async fn seeded(ledger_id: &str, data: &Value, indexed: bool) -> MemoryFluree {
    let (writer, reader) = instances_sharing_storage();
    let ledger = genesis_ledger(&writer, ledger_id);
    let committed = writer.insert(ledger, data).await.expect("seed").ledger;
    if indexed {
        rebuild_and_publish_index(&writer, ledger_id).await;
    }
    let loaded = reader.ledger(ledger_id).await.expect("reader load");
    let expected_index_t = if indexed { committed.t() } else { 0 };
    assert_eq!(loaded.index_t(), expected_index_t);
    reader
}

type Pair = (String, String);

async fn pairs(
    fluree: &MemoryFluree,
    ledger_id: &str,
    where_: Value,
    reasoning: &str,
) -> BTreeSet<Pair> {
    let query = json!({
        "@context": context(),
        "from": ledger_id,
        "selectDistinct": ["?x", "?y"],
        "where": where_,
        "reasoning": reasoning
    });
    let result = fluree
        .query_connection(&query)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{query:#}"));
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    normalize_rows(&result.to_jsonld(&ledger.snapshot).expect("to_jsonld"))
        .into_iter()
        .map(|row| match row.as_array().map(Vec::as_slice) {
            Some([Value::String(x), Value::String(y)]) => (x.clone(), y.clone()),
            _ => panic!("expected an [?x, ?y] row of IRIs, got {row}"),
        })
        .collect()
}

fn transitive_closure(edges: &BTreeSet<Pair>) -> BTreeSet<Pair> {
    let mut closure = edges.clone();
    loop {
        let next: BTreeSet<Pair> = closure
            .iter()
            .flat_map(|(x, y)| {
                closure
                    .iter()
                    .filter(move |(y2, _)| y2 == y)
                    .map(move |(_, z)| (x.clone(), z.clone()))
            })
            .collect();
        let before = closure.len();
        closure.extend(next);
        if closure.len() == before {
            return closure;
        }
    }
}

/// Every derived relation, computed in test code from the asserted edges.
async fn expected_relations(
    fluree: &MemoryFluree,
    ledger_id: &str,
) -> Vec<(&'static str, BTreeSet<Pair>)> {
    let controls = pairs(
        fluree,
        ledger_id,
        json!({"@id": "?x", "ex:controls": "?y"}),
        "none",
    )
    .await;
    let shares = pairs(
        fluree,
        ledger_id,
        json!({"@id": "?x", "ex:sharesAddress": "?y"}),
        "none",
    )
    .await;
    let sanctioned: BTreeSet<String> = pairs(
        fluree,
        ledger_id,
        json!([{"@id": "?x", "@type": "ex:SanctionedEntity"}, ["bind", "?y", "?x"]]),
        "none",
    )
    .await
    .into_iter()
    .map(|(x, _)| x)
    .collect();

    let controls_transitively = transitive_closure(&controls);
    let symmetric_shares = shares
        .iter()
        .flat_map(|(x, y)| [(x.clone(), y.clone()), (y.clone(), x.clone())])
        .collect();
    let associated = transitive_closure(&symmetric_shares);
    let exposed_to: BTreeSet<Pair> = controls_transitively
        .iter()
        .filter(|(_, y)| sanctioned.contains(y))
        .cloned()
        .collect();
    let exposed: BTreeSet<&String> = exposed_to.iter().map(|(x, _)| x).collect();
    let review_queue = exposed
        .iter()
        .map(|x| ((*x).clone(), "ex:sanctionsQueue".to_string()))
        .collect();
    let associated_with_exposed = associated
        .iter()
        .filter(|(_, y)| exposed.contains(y))
        .cloned()
        .collect();

    vec![
        ("controlsTransitively", controls_transitively),
        ("associated", associated),
        ("exposedTo", exposed_to),
        ("reviewQueue", review_queue),
        ("associatedWithExposed", associated_with_exposed),
    ]
}

async fn assert_matches_oracle(fluree: &MemoryFluree, ledger_id: &str, label: &str) {
    for (name, expected) in expected_relations(fluree, ledger_id).await {
        assert!(
            !expected.is_empty(),
            "{label}/{name}: the oracle must not be empty"
        );
        let derived = json!({"@id": "?x", format!("ex:{name}"): "?y"});
        let actual = pairs(fluree, ledger_id, derived, "datalog").await;
        let missing: Vec<_> = expected.difference(&actual).collect();
        let extra: Vec<_> = actual.difference(&expected).collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "{label}/{name}: derived relation differs from the oracle\n  \
             {} missing, first: {:?}\n  {} extra, first: {:?}",
            missing.len(),
            &missing[..missing.len().min(5)],
            extra.len(),
            &extra[..extra.len().min(5)],
        );
    }
}

#[tokio::test]
async fn recursive_program_matches_oracle() {
    let orders: [(&str, RuleId); 2] = [
        ("ascending-ids", |i| format!("ex:rule{i:02}")),
        ("descending-ids", |i| format!("ex:rule{:02}", 99 - i)),
    ];
    for indexed in [false, true] {
        for (order, rule_id) in orders {
            let label = format!("{}/{order}", if indexed { "indexed" } else { "novelty" });
            let ledger_id = format!(
                "datalog-program/reachability-{}-{order}:main",
                indexed as u8
            );
            let fluree = seeded(&ledger_id, &program(rule_id), indexed).await;
            assert_matches_oracle(&fluree, &ledger_id, &label).await;
        }
    }
}

#[tokio::test]
async fn bound_account_sees_its_own_exposure() {
    let ledger_id = "datalog-program/reachability-bound:main";
    let fluree = seeded(ledger_id, &program(|i| format!("ex:rule{i:02}")), false).await;
    let query = json!({
        "@context": context(),
        "from": ledger_id,
        "select": ["?target"],
        "where": {"@id": "ex:c0", "ex:exposedTo": "?target"},
        "reasoning": "datalog"
    });
    let result = fluree.query_connection(&query).await.expect("query");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    assert_eq!(
        normalize_rows(&result.to_jsonld(&ledger.snapshot).expect("to_jsonld")),
        vec![json!(["ex:k4"])],
        "c0 reaches the sanctioned k4 through the chain and cycle, and never s1"
    );
}
