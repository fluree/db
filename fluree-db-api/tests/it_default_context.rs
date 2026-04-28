//! Default-context opt-in regression tests.
//!
//! Locks in the boundary policy: queries get prefix expansion from the
//! ledger's stored default context **only** when the caller explicitly
//! opts in via [`Fluree::db_with_default_context`]. Direct API consumers
//! using [`Fluree::db`] see no auto-injection — they must include
//! `@context` in the query themselves.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Helper: count `?name` rows from a JSON-LD select result.
fn name_count(rows: &serde_json::Value) -> usize {
    rows.as_array().map(std::vec::Vec::len).unwrap_or_default()
}

/// `Fluree::db_with_default_context` injects the stored context into a
/// query that omits `@context`, so prefix-using selects resolve.
#[tokio::test]
async fn db_with_default_context_applies_stored_context() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("ctx-optin").await.unwrap();
    let txn = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice" },
            { "@id": "ex:bob",   "ex:name": "Bob" }
        ]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    fluree
        .set_default_context("ctx-optin", &json!({ "ex": "http://example.org/ns/" }))
        .await
        .expect("set_default_context");

    // Same query body for both paths — the only difference is which
    // loader the caller picks.
    let prefix_only_query = json!({
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });

    // Opt-in path: db_with_default_context attaches the stored context.
    let view = fluree
        .db_with_default_context("ctx-optin")
        .await
        .expect("db_with_default_context");
    let result = fluree
        .query(&view, &prefix_only_query)
        .await
        .expect("opt-in query should succeed");
    let rows = result.to_jsonld(&view.snapshot).expect("format jsonld");
    assert_eq!(
        name_count(&rows),
        2,
        "opt-in path should expand `ex:` prefix and match both inserted subjects; got: {rows}"
    );
}

/// `Fluree::db` returns a view with no default context, so the same
/// prefix-only query does not match any data — the parser doesn't inject
/// the stored context, and `ex:name` either fails to resolve or is
/// treated as a literal IRI that doesn't appear in the data.
#[tokio::test]
async fn db_skips_default_context_auto_injection() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("ctx-no-optin").await.unwrap();
    let txn = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice" }
        ]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    fluree
        .set_default_context("ctx-no-optin", &json!({ "ex": "http://example.org/ns/" }))
        .await
        .expect("set_default_context");

    let prefix_only_query = json!({
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });

    let view = fluree.db("ctx-no-optin").await.expect("db");
    // Should either parse-fail or return zero matches; either is correct
    // — the point is the stored context must NOT be auto-applied.
    let result = fluree.query(&view, &prefix_only_query).await;
    match result {
        Ok(qr) => {
            let rows = qr.to_jsonld(&view.snapshot).expect("format jsonld");
            assert_eq!(
                name_count(&rows),
                0,
                "db() must not auto-inject the stored default context; got rows: {rows}"
            );
        }
        Err(_) => {
            // Parse error is acceptable — the prefix `ex:` is undeclared
            // when the default context isn't injected.
        }
    }
}

/// Even on the opt-in path, an explicit `@context` in the query takes
/// precedence — the stored default is not injected when the caller
/// supplies their own. (Empty `@context: {}` is the documented opt-out
/// for "no prefixes at all".)
#[tokio::test]
async fn db_with_default_context_respects_query_supplied_context() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("ctx-override").await.unwrap();
    let txn = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice" }
        ]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    fluree
        .set_default_context("ctx-override", &json!({ "ex": "http://example.org/ns/" }))
        .await
        .expect("set_default_context");

    // Query supplies an empty @context — the gate in parse_jsonld_query
    // is "@context absent", so an explicit empty context blocks
    // auto-injection even on the opt-in path.
    let opt_out_query = json!({
        "@context": {},
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });

    let view = fluree
        .db_with_default_context("ctx-override")
        .await
        .expect("db_with_default_context");
    let result = fluree.query(&view, &opt_out_query).await;
    match result {
        Ok(qr) => {
            let rows = qr.to_jsonld(&view.snapshot).expect("format jsonld");
            assert_eq!(
                name_count(&rows),
                0,
                "explicit empty @context must block auto-injection; got rows: {rows}"
            );
        }
        Err(_) => {
            // Parse error is also acceptable — point is the default
            // context did not silently win over the user's empty one.
        }
    }
}
