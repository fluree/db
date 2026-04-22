//! Regression: reindex should populate `LedgerSnapshot.schema` (IndexSchema).

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::genesis_ledger_for_fluree;

#[tokio::test]
async fn reindex_populates_index_schema_from_subclass_and_subproperty_ops() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reindex-schema:main";

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id":"ex:Student", "rdfs:subClassOf": {"@id":"ex:Person"}},
            {"@id":"ex:name", "rdfs:subPropertyOf": {"@id":"ex:label"}}
        ]
    });

    let _ledger1 = fluree
        .insert_with_opts(
            ledger0,
            &tx,
            TxnOpts::default(),
            CommitOpts::default(),
            &fluree_db_api::IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert")
        .ledger;

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let loaded = fluree.ledger(ledger_id).await.expect("load ledger");
    let schema = loaded.snapshot.schema.as_ref().expect("schema present");
    assert!(
        schema.t > 0,
        "expected schema.t > 0 after reindex, got {}",
        schema.t
    );

    let has_student = schema.pred.vals.iter().any(|e| {
        e.id.name.as_ref() == "Student" && e.subclass_of.iter().any(|p| p.name.as_ref() == "Person")
    });
    assert!(has_student, "expected Student subClassOf Person in schema");

    let has_name_parent = schema.pred.vals.iter().any(|e| {
        e.id.name.as_ref() == "name" && e.parent_props.iter().any(|p| p.name.as_ref() == "label")
    });
    assert!(
        has_name_parent,
        "expected name subPropertyOf label in schema"
    );

    let has_label_child = schema.pred.vals.iter().any(|e| {
        e.id.name.as_ref() == "label" && e.child_props.iter().any(|c| c.name.as_ref() == "name")
    });
    assert!(has_label_child, "expected label childProps includes name");
}
