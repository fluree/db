//! Stable Fluree blank-node identifier tests.
//!
//! Fluree skolemizes every blank node into the reserved `_:fdb-...` label
//! space at insert time. These ids are returned by queries and — as pinned
//! here — are *stable*: when a later query or transaction references an
//! `_:fdb-...` label, it denotes the existing stored node instead of minting
//! a fresh one (RDF 1.1 §3.5 skolemization, kept in blank-node syntax). This
//! makes blank-node-rooted structures (e.g. OWL restrictions) editable in
//! place, without retracting and re-asserting the whole subtree.
//!
//! Ordinary client-authored labels (`_:b0`) keep standard semantics: fresh
//! node per transaction on the write side, existential variable in SPARQL
//! WHERE patterns.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "owl": "http://www.w3.org/2002/07/owl#"
    })
}

/// Seed a class with a single OWL-restriction-like structure rooted at an
/// anonymous blank node and return the fluree handle + ledger.
async fn seed_restriction(ledger_id: &str) -> (fluree_db_api::Fluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx(),
                "insert": {
                    "@id": "ex:ClassA",
                    "ex:restriction": {
                        "owl:onProperty": {"@id": "ex:hasPart"},
                        "owl:someValuesFrom": {"@id": "ex:Widget"}
                    }
                }
            }),
        )
        .await
        .expect("seed insert");
    (fluree, seeded.ledger)
}

async fn select_strings(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    query: &JsonValue,
) -> Vec<String> {
    let result = support::query_jsonld(fluree, ledger, query)
        .await
        .expect("query");
    let v = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let mut out: Vec<String> = v
        .as_array()
        .expect("array result")
        .iter()
        .map(|x| x.as_str().expect("string binding").to_string())
        .collect();
    out.sort();
    out
}

/// The `_:fdb-...` id of ex:ClassA's restriction node.
async fn restriction_id(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) -> String {
    let ids = select_strings(
        fluree,
        ledger,
        &json!({
            "@context": ctx(),
            "select": "?r",
            "where": {"@id": "ex:ClassA", "ex:restriction": "?r"}
        }),
    )
    .await;
    assert_eq!(ids.len(), 1, "exactly one restriction node: {ids:?}");
    let id = ids.into_iter().next().unwrap();
    assert!(
        id.starts_with("_:fdb-"),
        "restriction id should be a stable Fluree blank-node id, got {id}"
    );
    id
}

async fn run_sparql_update(
    fluree: &fluree_db_api::Fluree,
    ledger: LedgerState,
    sparql: &str,
) -> fluree_db_api::TransactResult {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(
        !parsed.has_errors(),
        "SPARQL parse errors: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("stage SPARQL UPDATE")
}

// ============================================================================
// JSON-LD transactions
// ============================================================================

/// Inserting with a stable id must extend the existing node, not mint a new
/// one.
#[tokio::test]
async fn jsonld_insert_extends_existing_blank_node() {
    let (fluree, ledger) = seed_restriction("it/stable-bnode:jsonld-insert").await;
    let bnode = restriction_id(&fluree, &ledger).await;

    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "insert": {"@id": bnode, "ex:note": "edited"}
            }),
        )
        .await
        .expect("insert on stable id")
        .ledger;

    // The note is reachable through the parent's ref — proof the triple
    // landed on the same node.
    let notes = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?note",
            "where": {"@id": "ex:ClassA", "ex:restriction": {"ex:note": "?note"}}
        }),
    )
    .await;
    assert_eq!(notes, vec!["edited"]);

    // Still exactly one restriction-shaped node in the ledger.
    let restrictions = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?r",
            "where": {"@id": "?r", "owl:onProperty": {"@id": "ex:hasPart"}}
        }),
    )
    .await;
    assert_eq!(restrictions.len(), 1);
}

/// where/delete/insert against a stable id edits the node in place: the
/// parent's reference is untouched and the node id survives the edit.
#[tokio::test]
async fn jsonld_delete_insert_edits_blank_node_in_place() {
    let (fluree, ledger) = seed_restriction("it/stable-bnode:jsonld-edit").await;
    let bnode = restriction_id(&fluree, &ledger).await;

    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": bnode, "owl:someValuesFrom": "?old"},
                "delete": {"@id": bnode, "owl:someValuesFrom": "?old"},
                "insert": {"@id": bnode, "owl:someValuesFrom": {"@id": "ex:Gadget"}}
            }),
        )
        .await
        .expect("edit restriction in place")
        .ledger;

    let values = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?v",
            "where": {"@id": "ex:ClassA", "ex:restriction": {"owl:someValuesFrom": "?v"}}
        }),
    )
    .await;
    assert_eq!(values, vec!["ex:Gadget"]);

    // Node identity is stable across the edit.
    assert_eq!(restriction_id(&fluree, &ledger).await, bnode);
}

/// Ordinary blank-node labels keep fresh-mint semantics: the same label in
/// two transactions produces two distinct nodes.
#[tokio::test]
async fn jsonld_plain_blank_label_still_mints_fresh() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = LedgerSnapshot::genesis("it/stable-bnode:fresh-mint");
    let mut ledger = LedgerState::new(db0, Novelty::new(0));

    for tag in ["one", "two"] {
        ledger = fluree
            .update(
                ledger,
                &json!({
                    "@context": ctx(),
                    "insert": {"@id": "_:b0", "ex:tag": tag}
                }),
            )
            .await
            .expect("insert")
            .ledger;
    }

    let subjects = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?s",
            "where": {"@id": "?s", "ex:tag": "?t"}
        }),
    )
    .await;
    assert_eq!(
        subjects.len(),
        2,
        "same client label across transactions must mint distinct nodes: {subjects:?}"
    );
}

// ============================================================================
// SPARQL
// ============================================================================

/// A stable id in a SPARQL WHERE pattern is a constant pinned to the stored
/// node, while an ordinary label stays an existential variable.
#[tokio::test]
async fn sparql_select_stable_blank_node_is_constant() {
    let (fluree, ledger) = seed_restriction("it/stable-bnode:sparql-select").await;

    // Add a second restriction so a wildcard match would return two rows.
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "insert": {
                    "@id": "ex:ClassB",
                    "ex:restriction": {
                        "owl:onProperty": {"@id": "ex:hasPart"},
                        "owl:someValuesFrom": {"@id": "ex:Sprocket"}
                    }
                }
            }),
        )
        .await
        .expect("insert ClassB")
        .ledger;
    let bnode = restriction_id(&fluree, &ledger).await;

    // Constant: only the addressed node's value comes back.
    let sparql = format!(
        "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n\
         SELECT ?v WHERE {{ {bnode} owl:someValuesFrom ?v }}"
    );
    let result = support::query_sparql(&fluree, &ledger, &sparql)
        .await
        .expect("sparql select");
    let v = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows = v.as_array().expect("array");
    assert_eq!(rows.len(), 1, "stable id must pin one node: {rows:?}");

    // Ordinary label: existential variable, matches both restrictions.
    let sparql = "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n\
                  SELECT ?v WHERE { _:b0 owl:someValuesFrom ?v }";
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("sparql select wildcard");
    let v = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        v.as_array().expect("array").len(),
        2,
        "plain blank label must stay a variable"
    );
}

/// SPARQL DELETE/INSERT WHERE addressing a stable id edits the node in place.
#[tokio::test]
async fn sparql_delete_insert_edits_blank_node() {
    let (fluree, ledger) = seed_restriction("it/stable-bnode:sparql-edit").await;
    let bnode = restriction_id(&fluree, &ledger).await;

    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         PREFIX owl: <http://www.w3.org/2002/07/owl#>\n\
         DELETE {{ {bnode} owl:someValuesFrom ?old }}\n\
         INSERT {{ {bnode} owl:someValuesFrom ex:Gadget }}\n\
         WHERE  {{ {bnode} owl:someValuesFrom ?old }}"
    );
    let ledger = run_sparql_update(&fluree, ledger, &sparql).await.ledger;

    let values = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?v",
            "where": {"@id": "ex:ClassA", "ex:restriction": {"owl:someValuesFrom": "?v"}}
        }),
    )
    .await;
    assert_eq!(values, vec!["ex:Gadget"]);
    assert_eq!(restriction_id(&fluree, &ledger).await, bnode);
}

/// SPARQL DELETE DATA / INSERT DATA with a stable id retract and assert
/// exact triples on the stored node.
#[tokio::test]
async fn sparql_delete_data_and_insert_data_stable_blank_node() {
    let (fluree, ledger) = seed_restriction("it/stable-bnode:sparql-data").await;
    let bnode = restriction_id(&fluree, &ledger).await;

    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         PREFIX owl: <http://www.w3.org/2002/07/owl#>\n\
         DELETE DATA {{ {bnode} owl:someValuesFrom ex:Widget }}"
    );
    let ledger = run_sparql_update(&fluree, ledger, &sparql).await.ledger;

    let values = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?v",
            "where": {"@id": "ex:ClassA", "ex:restriction": {"owl:someValuesFrom": "?v"}}
        }),
    )
    .await;
    assert!(values.is_empty(), "DELETE DATA must retract: {values:?}");

    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         PREFIX owl: <http://www.w3.org/2002/07/owl#>\n\
         INSERT DATA {{ {bnode} owl:someValuesFrom ex:Gadget }}"
    );
    let ledger = run_sparql_update(&fluree, ledger, &sparql).await.ledger;

    let values = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?v",
            "where": {"@id": "ex:ClassA", "ex:restriction": {"owl:someValuesFrom": "?v"}}
        }),
    )
    .await;
    assert_eq!(
        values,
        vec!["ex:Gadget"],
        "INSERT DATA must extend the existing node"
    );
}

/// DELETE WHERE with a stable-id subject retracts that node's matching
/// triples only.
#[tokio::test]
async fn sparql_delete_where_stable_blank_node() {
    let (fluree, ledger) = seed_restriction("it/stable-bnode:sparql-delete-where").await;
    let bnode = restriction_id(&fluree, &ledger).await;

    let sparql = format!("DELETE WHERE {{ {bnode} ?p ?o }}");
    let ledger = run_sparql_update(&fluree, ledger, &sparql).await.ledger;

    let props = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?p",
            "where": {"@id": bnode, "?p": "?o"}
        }),
    )
    .await;
    assert!(props.is_empty(), "node must be emptied: {props:?}");

    // The parent's ref to the (now-empty) node is a separate triple and
    // survives — retract it explicitly if the whole subtree should go.
    let refs = select_strings(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?r",
            "where": {"@id": "ex:ClassA", "ex:restriction": "?r"}
        }),
    )
    .await;
    assert_eq!(refs, vec![bnode]);
}
