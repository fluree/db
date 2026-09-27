//! Inline uniqueness constraints via `opts.uniqueProperties`.
//!
//! Per-transaction property-IRI list that the uniqueness enforcer
//! treats as `f:enforceUnique true` for the duration of the
//! transaction. Unions additively with any `f:constraintsSource`
//! config; the list itself never persists into the ledger.

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig};
use fluree_db_transact::ir::TxnOpts;
use serde_json::json;

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
async fn inline_property_unknown_to_ledger_fails_loudly() {
    // An inline IRI the ledger has never seen — neither
    // pre-tx nor introduced by this tx — must surface as a
    // parse error rather than silently becoming a no-op.
    // The non-strict `encode_iri` folds unknown IRIs into the
    // EMPTY namespace, which would match nothing and silently
    // disable enforcement; the strict path used here rejects
    // them up front so typos can't quietly weaken governance.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/unknown:main");

    let opts = TxnOpts {
        unique_properties: Some(vec!["http://never-seen.org/ns/whatever".to_string()]),
        ..TxnOpts::default()
    };
    let err = fluree
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
        .expect_err("unknown inline-unique IRI must fail the tx, not silently no-op");

    let msg = err.to_string();
    assert!(
        msg.contains("opts.uniqueProperties") && msg.contains("never-seen.org"),
        "expected explicit unresolved-IRI diagnostic, got: {msg}"
    );
}

/// Two named graphs, registered in this order, with alice's email in the
/// second. A transaction that writes only to the second graph numbers it
/// first among its own graphs, so enforcement must translate to the ledger's
/// graph id to look in the right place.
async fn seed_email_in_second_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id);
    fluree
        .stage_owned(ledger)
        .upsert_turtle(
            r#"
            GRAPH <http://example.org/g/a> {
                <http://example.org/ns/x> <http://example.org/ns/other> "1" .
            }
            GRAPH <http://example.org/g/b> {
                <http://example.org/ns/alice> <http://example.org/ns/email> "alice@example.org" .
            }
        "#,
        )
        .execute()
        .await
        .expect("seed two named graphs")
        .ledger
}

fn email_unique() -> TxnOpts {
    TxnOpts {
        unique_properties: Some(vec!["http://example.org/ns/email".to_string()]),
        ..TxnOpts::default()
    }
}

#[tokio::test]
async fn inline_unique_property_enforced_in_second_named_graph_sparql() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger =
        seed_email_in_second_graph(&fluree, "test/inline-constraints/second-graph-sparql:main")
            .await;
    let parsed = fluree_db_sparql::parse_sparql(
        r#"INSERT DATA { GRAPH <http://example.org/g/b> {
            <http://example.org/ns/bob> <http://example.org/ns/email> "alice@example.org" } }"#,
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, email_unique())
        .expect("lower SPARQL UPDATE");
    let err = fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect_err("duplicate email in the second named graph must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected uniqueness violation error, got: {err}"
    );
}

#[tokio::test]
async fn inline_unique_property_enforced_in_second_named_graph_jsonld() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger =
        seed_email_in_second_graph(&fluree, "test/inline-constraints/second-graph-jsonld:main")
            .await;
    let err = fluree
        .update_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "graph": "http://example.org/g/b",
                "insert": {"@id": "ex:bob", "ex:email": "alice@example.org"}
            }),
            email_unique(),
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect_err("duplicate email in the second named graph must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected uniqueness violation error, got: {err}"
    );
}
