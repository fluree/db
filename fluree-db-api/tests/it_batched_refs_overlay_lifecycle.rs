//! Batched `rdf:type` lookup must resolve overlay lifecycles.
//!
//! `RangeProvider::lookup_subject_predicate_refs_batched` is the latency path
//! policy enforcement uses for `f:onClass` (`fluree-db-policy/src/class_lookup.rs`)
//! and the indexer uses for class/property stats. It has three internal paths,
//! picked by what the persisted index can contribute; the one taken when the
//! target graph has no PSOT branch served the overlay assert-only, with no
//! lifecycle resolution.
//!
//! The overlay is a log, so a type asserted and then retracted inside the same
//! novelty window has both flakes in it. Reporting the class anyway means a
//! class-based policy grant outliving its own revocation until the first index
//! build — which for a named graph written only in novelty is indefinitely.
//!
//! The fixture is exactly that shape: the default graph is indexed (so
//! `rdf:type` has a persisted predicate id and the store exists), while the
//! named graph lives only in novelty and therefore has no branch for its
//! `g_id`.

#![cfg(feature = "native")]

mod support;
use crate::support::{start_background_indexer_local, trigger_index_and_wait_outcome};
use fluree_db_api::FlureeBuilder;
use fluree_db_core::{IndexType, RangeOptions, Sid};

const GRAPH_IRI: &str = "http://example.org/graphs/novelty-only";

#[tokio::test]
async fn retracted_type_in_a_branchless_graph_is_not_reported() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/batched-refs-overlay:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Default graph, indexed. Carries `rdf:type` so the predicate is
            // in the persisted dictionary — without that the batched lookup
            // short-circuits on an unknown predicate and never reaches the
            // overlay path at all.
            let seed = r#"
                @prefix ex: <http://example.org/> .
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                ex:seed rdf:type ex:Person .
                ex:seed ex:name "seed" .
            "#;
            let r = fluree
                .stage_owned(ledger)
                .upsert_turtle(seed)
                .execute()
                .await
                .expect("seed insert");
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(
                ledger.snapshot.range_provider.is_some(),
                "fixture needs an indexed base with a range provider"
            );

            // Named graph, novelty only: grant two classes, then revoke one.
            let grant = format!(
                r"
                @prefix ex: <http://example.org/> .
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                GRAPH <{GRAPH_IRI}> {{
                    ex:alice rdf:type ex:Admin .
                    ex:alice rdf:type ex:Person .
                }}
                ",
            );
            let r = fluree
                .stage_owned(ledger)
                .upsert_turtle(&grant)
                .execute()
                .await
                .expect("grant insert");

            let ledger = fluree.ledger(ledger_id).await.unwrap();
            let revoke = format!(
                r"
                PREFIX ex: <http://example.org/>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                DELETE {{ GRAPH <{GRAPH_IRI}> {{ ex:alice rdf:type ex:Admin }} }}
                WHERE  {{ GRAPH <{GRAPH_IRI}> {{ ex:alice rdf:type ex:Admin }} }}
                ",
            );
            let parsed = fluree_db_sparql::parse_sparql(&revoke);
            assert!(
                !parsed.has_errors(),
                "SPARQL parse: {:?}",
                parsed.diagnostics
            );
            let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
            let txn = fluree_db_transact::lower_sparql_update_ast(
                &parsed.ast.expect("SPARQL AST"),
                &mut ns,
                fluree_db_transact::TxnOpts::default(),
            )
            .expect("lower SPARQL UPDATE");
            let r2 = fluree
                .stage_owned(ledger)
                .txn(txn)
                .execute()
                .await
                .expect("revoke");
            assert_eq!(
                r2.receipt.flake_count, 1,
                "the revoke must retract one flake"
            );
            assert!(r2.receipt.t > r.receipt.t);

            // Ask the provider the same question policy asks.
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            let g_sid = ledger
                .snapshot
                .encode_iri(GRAPH_IRI)
                .expect("named graph IRI is registered");
            let g_id = *ledger
                .snapshot
                .build_reverse_graph()
                .expect("reverse graph")
                .get(&g_sid)
                .expect("named graph has a g_id");
            assert_ne!(g_id, 0, "the fixture must exercise the named graph");

            let provider = ledger.snapshot.range_provider.as_ref().unwrap();
            let rdf_type = Sid::new(
                fluree_vocab::namespaces::RDF,
                fluree_vocab::predicates::RDF_TYPE,
            );
            let alice = ledger
                .snapshot
                .encode_iri("http://example.org/alice")
                .expect("alice is registered");

            let classes = provider
                .lookup_subject_predicate_refs_batched(
                    g_id,
                    IndexType::Psot,
                    &rdf_type,
                    std::slice::from_ref(&alice),
                    &RangeOptions::new().with_to_t(ledger.t()),
                    &*ledger.novelty,
                )
                .expect("batched class lookup");

            let names: Vec<String> = classes
                .get(&alice)
                .map(|cs| cs.iter().map(|c| c.name_str().to_string()).collect())
                .unwrap_or_default();
            assert!(
                !names.iter().any(|n| n == "Admin"),
                "a revoked rdf:type must not be reported — a class-based policy \
                 grant would outlive its revocation; got {names:?}"
            );
            assert!(
                names.iter().any(|n| n == "Person"),
                "the class that was never revoked must still be reported; got {names:?}"
            );
        })
        .await;
}
