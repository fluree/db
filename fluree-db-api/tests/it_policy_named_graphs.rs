//! Policy + named graphs integration tests
//!
//! Ensures that view-policy enforcement applies correctly when querying a named graph
//! (graph selection via fragment and structured `from` forms).

#![cfg(feature = "native")]

use fluree_db_api::{FlureeBuilder, LedgerManagerConfig};
use serde_json::json;

use std::sync::Arc;
mod support;
use support::{assert_index_defaults, start_background_indexer_local};

#[tokio::test]
async fn policy_applies_to_named_graph_queries() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "policy/named-graphs:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            // Seed a named graph via TriG
            let ledger0 = support::genesis_ledger(&fluree, ledger_id);
            let trig = r#"
                @prefix ex: <http://example.org/ns/> .
                @prefix schema: <http://schema.org/> .

                GRAPH <http://example.org/graphs/private> {
                    ex:alice schema:ssn "111-11-1111" .
                }

                GRAPH <http://example.org/graphs/public> {
                    ex:alice schema:name "Alice" .
                }
            "#;

            let out1 = fluree
                .stage_owned(ledger0)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("seed trig");

            let completion = handle.trigger(ledger_id, out1.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                other => panic!("indexing failed: {other:?}"),
            }

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Policy: deny schema:ssn but allow everything else.
            //
            // We include an explicit allow-all policy so this test
            // doesn't depend on subtle default-allow behavior when policies are present.
            let policy = json!([
                {
                    "@id": "ex:denySsnPolicy",
                    "@type": "f:AccessPolicy",
                    "f:action": "f:view",
                    "f:onProperty": [{"@id": "http://schema.org/ssn"}],
                    "f:allow": false
                },
                {
                    "@id": "ex:allowAllPolicy",
                    "@type": "f:AccessPolicy",
                    "f:action": "f:view",
                    "f:allow": true
                }
            ]);

            // Sanity: each named graph should be queryable without policy.
            let q_private_ssn_no_policy = json!({
                "@context": {"ex": "http://example.org/ns/", "schema":"http://schema.org/"},
                "from": {"@id": ledger_id, "graph": "http://example.org/graphs/private"},
                "select": "?ssn",
                "where": {"@id":"ex:alice", "schema:ssn":"?ssn"}
            });
            let out_private = fluree
                .query_connection(&q_private_ssn_no_policy)
                .await
                .expect("query private ssn (no policy)");
            let jsonld_private = out_private.to_jsonld(&ledger.snapshot).expect("to_jsonld private");
            assert_eq!(jsonld_private, json!(["111-11-1111"]));

            let q_public_name_no_policy = json!({
                "@context": {"ex": "http://example.org/ns/", "schema":"http://schema.org/"},
                "from": {"@id": ledger_id, "graph": "http://example.org/graphs/public"},
                "select": "?name",
                "where": {"@id":"ex:alice", "schema:name":"?name"}
            });
            let out_public = fluree
                .query_connection(&q_public_name_no_policy)
                .await
                .expect("query public name (no policy)");
            let jsonld_public = out_public.to_jsonld(&ledger.snapshot).expect("to_jsonld public");
            assert_eq!(jsonld_public, json!(["Alice"]));

            // 1) Structured from: named graph + denied property
            let q_private_ssn = json!({
                "@context": {"ex": "http://example.org/ns/", "schema":"http://schema.org/", "f":"https://ns.flur.ee/db#"},
                "from": {"@id": ledger_id, "graph": "http://example.org/graphs/private"},
                "opts": {"policy": policy.clone(), "default-allow": true},
                "select": ["?ssn"],
                "where": {"@id":"ex:alice", "schema:ssn":"?ssn"}
            });

            let result = fluree
                .query_connection(&q_private_ssn)
                .await
                .expect("query private ssn");
            let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            assert_eq!(
                jsonld,
                json!([]),
                "policy should deny schema:ssn within named graph query"
            );

            // 2) Fragment from: public graph, allowed property
            let q_public_name = json!({
                "@context": {"ex": "http://example.org/ns/", "schema":"http://schema.org/", "f":"https://ns.flur.ee/db#"},
                "from": format!("{ledger_id}#http://example.org/graphs/public"),
                "opts": {"policy": policy.clone(), "default-allow": true},
                "select": "?name",
                "where": {"@id":"ex:alice", "schema:name":"?name"}
            });

            let result = fluree
                .query_connection(&q_public_name)
                .await
                .expect("query public name");
            let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            assert_eq!(jsonld, json!(["Alice"]));

            // 3) Structured from: public graph still returns name with policy (sanity)
            let q_public_name_structured = json!({
                "@context": {"ex": "http://example.org/ns/", "schema":"http://schema.org/", "f":"https://ns.flur.ee/db#"},
                "from": {"@id": ledger_id, "graph": "http://example.org/graphs/public"},
                "opts": {"policy": policy.clone(), "default-allow": true},
                "select": "?name",
                "where": {"@id":"ex:alice", "schema:name":"?name"}
            });

            let result = fluree
                .query_connection(&q_public_name_structured)
                .await
                .expect("query public structured name");
            let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            assert_eq!(jsonld, json!(["Alice"]));
        })
        .await;
}
