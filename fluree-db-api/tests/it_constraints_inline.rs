//! Inline uniqueness constraints via `opts.uniqueProperties`.
//!
//! Per-transaction property-IRI list that the uniqueness enforcer
//! treats as `f:enforceUnique true` for the duration of the
//! transaction. Unions additively with any `f:constraintsSource`
//! config; the list itself never persists into the ledger.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig};
use fluree_db_transact::ir::TxnOpts;
use serde_json::json;
use support::genesis_ledger;

fn test_index_cfg() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    }
}

#[tokio::test]
async fn inline_unique_property_rejects_duplicate() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/duplicate:main");

    // Seed alice with email.
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id":       "ex:alice",
        "ex:email":  "alice@example.org"
    });
    let r = fluree.insert(ledger, &seed).await.expect("seed alice");

    // Second tx: bob with the same email, marking ex:email inline-unique.
    let opts = TxnOpts {
        unique_properties: Some(vec!["http://example.org/ns/email".to_string()]),
        ..TxnOpts::default()
    };
    let err = fluree
        .insert_with_opts(
            r.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:bob",
                "ex:email":  "alice@example.org"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect_err("duplicate value on inline-unique property must be rejected");

    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected uniqueness violation error, got: {err}"
    );
}

#[tokio::test]
async fn inline_unique_property_does_not_persist() {
    // After a tx that supplies opts.uniqueProperties, a later tx
    // without it must accept a duplicate value — the inline list
    // was transient.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/transient:main");

    // Tx 1: seed alice under the inline-unique constraint.
    let opts = TxnOpts {
        unique_properties: Some(vec!["http://example.org/ns/email".to_string()]),
        ..TxnOpts::default()
    };
    let r1 = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:alice",
                "ex:email":  "alice@example.org"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect("seed alice with inline constraint");

    // Tx 2: bob with the same email, NO opts.uniqueProperties.
    // Without the inline list and no `f:constraintsSource` config,
    // there's no constraint to violate — must be accepted.
    fluree
        .insert(
            r1.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:bob",
                "ex:email":  "alice@example.org"
            }),
        )
        .await
        .expect("without opts.uniqueProperties the prior tx's constraint must not apply");
}

#[tokio::test]
async fn inline_unique_property_accepts_unique_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/distinct:main");

    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id":       "ex:alice",
        "ex:email":  "alice@example.org"
    });
    let r = fluree.insert(ledger, &seed).await.expect("seed alice");

    let opts = TxnOpts {
        unique_properties: Some(vec!["http://example.org/ns/email".to_string()]),
        ..TxnOpts::default()
    };
    fluree
        .insert_with_opts(
            r.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:bob",
                "ex:email":  "bob@example.org"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect("distinct value must satisfy inline-unique constraint");
}

#[tokio::test]
async fn inline_property_unknown_to_ledger_is_dropped_silently() {
    // An inline IRI the ledger has never seen has no instances
    // and cannot be violated; the enforcer drops it rather than
    // failing the transaction. This matches the same-ledger
    // contract where `encode_iri` returning None drops the
    // would-be constraint.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/unknown:main");

    let opts = TxnOpts {
        unique_properties: Some(vec!["http://never-seen.org/ns/whatever".to_string()]),
        ..TxnOpts::default()
    };
    fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:alice",
                "ex:email":  "alice@example.org"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect("inline IRI absent from D's ns map must not fail the tx");
}
