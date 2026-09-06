//! #1738: a delete that removes nothing must not narrow a scan to the wrong
//! datatype — the graph-scoped sibling of #1721.
//!
//! `infer_exact_datatype_sid_from_stats` narrows a bound-object scan to one
//! exact datatype when the predicate's datatype set is (effectively) a
//! singleton. That set used to be read off the graph-scoped `datatypes`
//! counts, which the novelty merge maintains as a blind ±1 delta log: a
//! retraction naming a tag the base index carries — even a retraction of a
//! fact that never existed — zeroes the tag out, leaves a singleton, and the
//! scan then returns rows that do not match the query. Unlike #1721, which
//! declined an optimization, this lane *invents* rows.
//!
//! The mechanism is pinned at the consumer by
//! `fluree_db_query::binary_scan::tests::observed_set_vetoes_narrowing_when_counts_dropped_a_tag`;
//! these pin the observable consequence end to end, with the control that
//! makes the mechanism conclusive: only a no-op delete naming a tag the
//! predicate DOES carry could perturb the answer, one naming a foreign tag
//! never could.

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// One predicate carrying the same lexical value under two integer-family
/// datatypes — the shape where narrowing to either one changes the answer.
fn seed() -> serde_json::Value {
    json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id":"ex:a","ex:p": {"@value":"25","@type":"xsd:int"}},
            {"@id":"ex:b","ex:p": {"@value":"25","@type":"xsd:long"}}
        ]
    })
}

/// A no-op delete typed with a tag the predicate DOES carry (`xsd:int`): the
/// spurious `-1` lands on a real tag and can zero it out of the count set.
fn no_op_delete_carried_tag() -> serde_json::Value {
    json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "delete": [
            {"@id":"ex:a","ex:p": {"@value":"999","@type":"xsd:int"}}
        ]
    })
}

/// The control: the same no-op delete typed with a tag the predicate does
/// NOT carry (`xsd:short`). Its `-1` has no base tag to land on, so even the
/// un-fixed merge leaves the set intact — if THIS one moved the answer, the
/// mechanism under test would not be the one moving it.
fn no_op_delete_foreign_tag() -> serde_json::Value {
    json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "delete": [
            {"@id":"ex:a","ex:p": {"@value":"999","@type":"xsd:short"}}
        ]
    })
}

/// A bare numeric object: no datatype constraint reaches the probe, which is
/// exactly the shape the exact-datatype inference exists for.
const Q: &str = r"
    PREFIX ex: <http://example.org/ns/>
    SELECT ?s WHERE { ?s ex:p 25 }
";

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

async fn jsonld_rows(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
) -> Vec<serde_json::Value> {
    let q = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "select": ["?s"],
        "where": { "@id": "?s", "ex:p": 25 }
    });
    normalize_rows(
        &support::query_jsonld(fluree, ledger, &q)
            .await
            .expect("jsonld query")
            .to_jsonld(&ledger.snapshot)
            .expect("jsonld"),
    )
}

/// Publish an index for the seed, stage an unrelated triple so novelty is
/// non-empty (both measurements on the same lane), and return the ledger.
async fn indexed_ledger_with_novelty(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let _ = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;
    support::build_and_publish_index(fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("reload after index");
    let unrelated = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [{"@id":"ex:s9","ex:other": "x"}]
    });
    fluree
        .insert(ledger, &unrelated)
        .await
        .expect("unrelated insert")
        .ledger
}

#[tokio::test]
async fn issue_1738_no_op_delete_does_not_change_a_bound_object_scan() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed_ledger_with_novelty(&fluree, "hazard1738-int:main").await;

    let before = rows(&fluree, &ledger, Q).await;
    let before_jsonld = jsonld_rows(&fluree, &ledger).await;

    let ledger2 = fluree
        .update(ledger, &no_op_delete_carried_tag())
        .await
        .expect("no-op delete")
        .ledger;

    // The data is provably untouched across the delete.
    let data = rows(
        &fluree,
        &ledger2,
        "PREFIX ex: <http://example.org/ns/> SELECT ?s ?o WHERE { ?s ex:p ?o }",
    )
    .await;
    assert_eq!(data.len(), 2, "the delete removed something: {data:?}");

    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "a no-op delete narrowed the scan to the surviving datatype and \
         changed the answer"
    );
    // Surface parity: the JSON-LD query shares the IR and the scan lane.
    assert_eq!(
        jsonld_rows(&fluree, &ledger2).await,
        before_jsonld,
        "the JSON-LD surface drifted where SPARQL is pinned"
    );
}

#[tokio::test]
async fn issue_1738_control_a_foreign_tag_delete_changes_nothing() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed_ledger_with_novelty(&fluree, "hazard1738-ctl:main").await;

    let before = rows(&fluree, &ledger, Q).await;

    let ledger2 = fluree
        .update(ledger, &no_op_delete_foreign_tag())
        .await
        .expect("no-op delete")
        .ledger;

    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "control moved: a tag the predicate never carried perturbed the scan"
    );
}
