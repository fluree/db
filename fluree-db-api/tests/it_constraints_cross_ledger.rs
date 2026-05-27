//! End-to-end cross-ledger uniqueness enforcement.
//!
//! Data ledger D's `#config` declares `f:constraintsSource` with
//! `f:ledger` pointing at model ledger M, plus a named graph in M
//! that holds `f:enforceUnique true` annotations on properties.
//! A transaction against D that would create a duplicate value on
//! one of those properties must be rejected — M's constraints
//! govern D without any annotation triples ever being written into D.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::genesis_ledger;

fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// M declares `ex:email` as `f:enforceUnique true`. D references M's
/// constraints graph in its `#config`. Inserting ex:alice and then
/// ex:bob with the same email on D must trigger
/// `TransactError::UniqueConstraintViolation`.
#[tokio::test]
async fn data_ledger_tx_enforces_model_ledger_unique_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();

    // --- model ledger M: annotates a property unique in a named graph
    let model_id = "test/cross-ledger-constraints/model:main";
    let model = genesis_ledger(&fluree, model_id);

    let constraints_graph_iri = "http://example.org/governance/constraints";
    let m_trig = format!(
        r"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{constraints_graph_iri}> {{
            ex:email f:enforceUnique true .
        }}
    "
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&m_trig)
        .execute()
        .await
        .expect("seed M constraints graph");

    // --- data ledger D: seed data + cross-ledger constraints config.
    let data_id = "test/cross-ledger-constraints/data:main";
    let data = genesis_ledger(&fluree, data_id);

    // Insert the first record. No config yet (config writes are
    // lagging — they take effect on the next tx), so this insert
    // never enforces anything.
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "ex:email": "alice@example.com"
            }),
        )
        .await
        .expect("seed alice");
    let data = r1.ledger;

    // Now write the config pointing f:constraintsSource cross-ledger
    // at M, with uniqueEnabled. The next transaction will pick this up.
    let config_iri = config_graph_iri(data_id);
    let d_config = format!(
        r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:cfg:main> rdf:type f:LedgerConfig .
            <urn:cfg:main> f:transactDefaults <urn:cfg:transact> .
            <urn:cfg:transact> f:uniqueEnabled true .
            <urn:cfg:transact> f:constraintsSource <urn:cfg:constraints-ref> .
            <urn:cfg:constraints-ref> rdf:type f:GraphRef ;
                                      f:graphSource <urn:cfg:constraints-src> .
            <urn:cfg:constraints-src> f:ledger <{model_id}> ;
                                      f:graphSelector <{constraints_graph_iri}> .
        }}
    "
    );
    let r2 = fluree
        .stage_owned(data)
        .upsert_turtle(&d_config)
        .execute()
        .await
        .expect("seed D cross-ledger constraints config");
    let data = r2.ledger;

    // Insert a second record with the SAME email. M's cross-ledger
    // constraint must apply against D's data and reject the tx.
    let err = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:bob",
                "ex:email": "alice@example.com"
            }),
        )
        .await
        .expect_err("duplicate email under cross-ledger constraint must be rejected");

    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UniqueConstraintViolation { .. }
            )
        ),
        "expected UniqueConstraintViolation from M's cross-ledger constraint, got: {err:?}"
    );
}

/// When `f:constraintsSource` is cross-ledger but the model ledger
/// has been retracted (or never created), the transaction must fail
/// closed with a clear diagnostic naming the missing model ledger —
/// not silently allow the duplicate.
#[tokio::test]
async fn cross_ledger_constraints_missing_model_fails_tx_closed() {
    let fluree = FlureeBuilder::memory().build_memory();

    let data_id = "test/cross-ledger-constraints/no-model:main";
    let model_id = "test/cross-ledger-constraints/never-created:main";
    let data = genesis_ledger(&fluree, data_id);

    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "ex:email": "alice@example.com"
            }),
        )
        .await
        .unwrap();
    let data = r1.ledger;

    let config_iri = config_graph_iri(data_id);
    let r2 = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:transactDefaults <urn:cfg:transact> .
                <urn:cfg:transact> f:uniqueEnabled true .
                <urn:cfg:transact> f:constraintsSource <urn:cfg:cref> .
                <urn:cfg:cref> rdf:type f:GraphRef ;
                               f:graphSource <urn:cfg:csrc> .
                <urn:cfg:csrc> f:ledger <{model_id}> ;
                               f:graphSelector <http://example.org/whatever> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D config (missing model)");
    let data = r2.ledger;

    let err = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:bob",
                "ex:email": "bob@example.com"
            }),
        )
        .await
        .expect_err("tx must fail closed when cross-ledger constraints model is missing");

    let msg = err.to_string();
    assert!(
        msg.contains(model_id) || msg.contains("not present on this instance"),
        "error must name the missing model ledger, got: {msg}"
    );
}
