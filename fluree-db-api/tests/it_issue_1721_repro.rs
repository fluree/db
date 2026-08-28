//! #1721: a delete that removes nothing must not change query results.
//!
//! The planner merges novelty into the aggregate per-datatype breakdown as a
//! blind ±1 delta log. A retraction of a fact the ledger never held still
//! charges its `-1`, and when that drives a predicate's last literal tag to zero
//! the predicate reads as all-ref — which licenses `filter_fold` to rewrite an
//! unrelated `FILTER(?x = ?y)` into an equijoin that does not mean the same
//! thing. These pin the observable consequence: three shapes of no-op delete,
//! none of which touches the data, none of which may touch the answer.
//!
//! The mechanism itself is pinned closer to the source, and without depending on
//! any particular looseness in the join, by
//! `fluree_db_novelty::runtime_stats::tests::spurious_retraction_keeps_a_literal_datatype_observed`.

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// One predicate carrying a ref (so the surviving breakdown stays non-empty and
/// all-`JSON_LD_ID` once the literal tags are zeroed) and two literals that
/// share a lexical form but not a datatype. `"abc"^^xsd:string` and
/// `"abc"^^ex:custom` are different terms and are not value-equal under SPARQL
/// `=`, so every correct answer to the query below is an identity pair.
fn seed() -> serde_json::Value {
    json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id":"ex:s1","ex:p": "abc"},
            {"@id":"ex:s2","ex:p": {"@value":"abc","@type":"ex:custom"}},
            {"@id":"ex:s3","ex:p": {"@id":"ex:n1"}}
        ]
    })
}

/// A delete of two facts that are not in the ledger, one per literal datatype.
fn no_op_delete() -> serde_json::Value {
    json!({
        "@context": { "ex": "http://example.org/ns/" },
        "delete": [
            {"@id":"ex:s1","ex:p": "nothing-here"},
            {"@id":"ex:s2","ex:p": {"@value":"nothing-here","@type":"ex:custom"}}
        ]
    })
}

const Q: &str = r"
    PREFIX ex: <http://example.org/ns/>
    SELECT ?a ?b WHERE {
      ?a ex:p ?x .
      ?b ex:p ?y .
      FILTER(?x = ?y)
    }
";

fn identity_rows() -> Vec<serde_json::Value> {
    normalize_rows(&json!([
        ["ex:s1", "ex:s1"],
        ["ex:s2", "ex:s2"],
        ["ex:s3", "ex:s3"]
    ]))
}

async fn rows(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    query: &str,
) -> Vec<serde_json::Value> {
    normalize_rows(
        &support::query_sparql(fluree, ledger, query)
            .await
            .expect("query")
            .to_jsonld(&ledger.snapshot)
            .expect("jsonld"),
    )
}

async fn run_sparql_update(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
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
    .expect("lower SPARQL UPDATE to Txn IR");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("stage SPARQL UPDATE")
}

#[tokio::test]
async fn issue_1721_json_ld_no_op_delete_keeps_filter_equality_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "hazard1721-jsonld:main");
    let ledger = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    let before = rows(&fluree, &ledger, Q).await;
    assert_eq!(
        before,
        identity_rows(),
        "baseline: `=` must equate only a term with itself"
    );

    let ledger2 = fluree
        .update(ledger, &no_op_delete())
        .await
        .expect("delete")
        .ledger;

    // The data is provably untouched across the delete.
    let data = rows(
        &fluree,
        &ledger2,
        "PREFIX ex: <http://example.org/ns/> SELECT ?s ?o WHERE { ?s ex:p ?o }",
    )
    .await;
    assert_eq!(data.len(), 3, "the delete removed something: {data:?}");

    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "a delete that removed nothing changed FILTER(?x = ?y) results"
    );
}

/// The same hazard through the SPARQL Update surface: `DELETE DATA` of a triple
/// that is not in the graph is a no-op by SPARQL 1.1 Update.
#[tokio::test]
async fn issue_1721_sparql_delete_data_no_op_keeps_filter_equality_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "hazard1721-sparql:main");
    let ledger = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    let before = rows(&fluree, &ledger, Q).await;
    assert_eq!(before, identity_rows(), "baseline");

    let update = r#"
        PREFIX ex: <http://example.org/ns/>
        DELETE DATA {
          ex:s1 ex:p "nothing-here" .
          ex:s2 ex:p "nothing-here"^^<http://example.org/ns/custom> .
        }
    "#;
    let ledger2 = run_sparql_update(&fluree, ledger, update).await.ledger;

    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "DELETE DATA of absent triples changed FILTER(?x = ?y) results"
    );
}

/// No bogus data needed at all: deleting facts that *did* exist, twice. The
/// second delete is a no-op against the data but charges a second `-1`. This is
/// the operationally likely shape — an at-least-once delivery retry, a replayed
/// migration, an idempotent-by-intent client.
#[tokio::test]
async fn issue_1721_replayed_delete_keeps_filter_equality_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "hazard1721-replay:main");
    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id":"ex:s1","ex:p": "abc"},
            {"@id":"ex:s2","ex:p": {"@value":"abc","@type":"ex:custom"}},
            {"@id":"ex:s3","ex:p": {"@id":"ex:n1"}},
            // Literals that WILL be deleted (once, legitimately) below.
            {"@id":"ex:sx","ex:p": "gone"},
            {"@id":"ex:sy","ex:p": {"@value":"gone","@type":"ex:custom"}}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let del = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "delete": [
            {"@id":"ex:sx","ex:p": "gone"},
            {"@id":"ex:sy","ex:p": {"@value":"gone","@type":"ex:custom"}}
        ]
    });
    let ledger1 = fluree.update(ledger, &del).await.expect("delete 1").ledger;
    let before = rows(&fluree, &ledger1, Q).await;
    assert_eq!(before, identity_rows(), "baseline after the real delete");

    let ledger2 = fluree.update(ledger1, &del).await.expect("delete 2").ledger;
    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "replaying an already-applied delete changed FILTER(?x = ?y) results"
    );
}

/// The fold has to keep working: an all-ref predicate still licenses it after a
/// real index publish, with a literal-valued sibling for contrast.
///
/// Scope, because it is narrower than it looks. `observed_datatypes` is not on
/// the wire — every decoder re-derives it from the breakdown — so a round trip
/// can only ever observe the decoder it happens to route through, and this one
/// routes through `fluree-db-core`'s (the memory backend's), not
/// `fluree-db-binary-index`'s. It cannot see the index-build aggregate at all,
/// since encoding discards whatever that produced. Each producer is therefore
/// pinned where it lives:
/// `fluree_db_binary_index::format::stats_wire::tests::every_stats_decoder_rederives_the_observed_datatype_tags`
/// covers all three decoders, and
/// `fluree_db_indexer::stats::tests::aggregate_carries_the_observed_datatype_tags_across_graphs`
/// covers the build-side aggregate that both the index pipelines and the import
/// now share. That matters more than usual here: the field is fail-closed, so a
/// producer that stopped filling it would cost the optimization silently
/// instead of failing anything.
#[tokio::test]
async fn ref_only_survives_a_published_index_round_trip() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "hazard1721-refonly:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id":"ex:a","ex:knows": {"@id":"ex:b"}, "ex:name": "A"},
            {"@id":"ex:b","ex:knows": {"@id":"ex:a"}, "ex:name": "B"}
        ]
    });
    let _ = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    support::build_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("reload after index");

    let stats = ledger
        .snapshot
        .stats
        .clone()
        .expect("a published index carries stats");
    let view = fluree_db_core::StatsView::from_db_stats_with_namespaces(
        &stats,
        ledger.snapshot.namespaces(),
    );
    let ns = "http://example.org/ns/";
    assert_eq!(
        view.is_property_ref_only_by_iri(&format!("{ns}knows")),
        Some(true),
        "an all-ref predicate lost its ref-only licence across the index round trip"
    );
    assert_eq!(
        view.is_property_ref_only_by_iri(&format!("{ns}name")),
        Some(false)
    );
}

/// Blast radius: the same hazard against a *published base index* rather than
/// novelty-synthesized stats. Here the literal datatype tags come from the
/// index, and the spurious `-1`s have to cancel them out of the merged
/// breakdown before the fold is licensed.
///
/// An unrelated triple is staged after the index publish so that both
/// measurements are taken with novelty non-empty. The encoded-object read lane
/// answers this query differently from the overlay lane (a datatype-flattening
/// defect of its own, unrelated to the fold), and comparing across that lane
/// switch would measure that instead of the drift.
#[tokio::test]
async fn issue_1721_no_op_delete_does_not_drift_a_published_index() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "hazard1721-indexed:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let _ = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    support::build_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("reload after index");
    let unrelated = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [{"@id":"ex:s9","ex:other": "x"}]
    });
    let ledger = fluree
        .insert(ledger, &unrelated)
        .await
        .expect("unrelated insert")
        .ledger;

    let before = rows(&fluree, &ledger, Q).await;
    assert_eq!(before, identity_rows(), "baseline over the published index");

    let ledger2 = fluree
        .update(ledger, &no_op_delete())
        .await
        .expect("delete")
        .ledger;

    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "a no-op delete drifted the stats read off a published index"
    );
}
