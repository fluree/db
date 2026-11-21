//! A new ledger splits IRIs under `NsSplitMode::NamedLocal`: an IRI ending in
//! `/` files under the namespace before it instead of taking one of its own.
//! A ledger whose first commit declared another mode keeps it, whether it is
//! loaded from its commits or from an index.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState};
use fluree_db_core::NsSplitMode;
use serde_json::{json, Value};

const VENDOR: &str = "http://example.com/vendor7/";
const OFFER: &str = "http://example.com/vendor7/Offer12";
const PAGE: &str = "http://example.com/vendor7/Offer12/";
/// A predicate ending in `/` whose own IRI is also the prefix of a subject's:
/// its split must not depend on that prefix having a code.
const TAG: &str = "http://example.org/ns/tag/";

fn offer_with_its_page() -> Value {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": OFFER, "ex:page": {"@id": PAGE}, TAG: "featured"},
            {"@id": PAGE, "ex:title": "the offer's page"},
            {"@id": "http://example.org/ns/tag/sale", "ex:title": "a tag"}
        ]
    })
}

fn has_namespace(ledger: &LedgerState, prefix: &str) -> bool {
    ledger.snapshot.namespaces().values().any(|p| p == prefix)
}

/// The page is found as an object and as a subject, from SPARQL and JSON-LD.
async fn assert_page_found(fluree: &Fluree, ledger: &LedgerState) {
    let by_object = format!(
        "PREFIX ex: <http://example.org/ns/>
         SELECT ?offer ?title WHERE {{ ?offer ex:page <{PAGE}> . <{PAGE}> ex:title ?title }}"
    );
    let rows = support::query_sparql(fluree, ledger, &by_object)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!([[OFFER, "the offer's page"]]));

    let from_offer = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?page", "?title"],
        "where": {"@id": OFFER, "ex:page": {"@id": "?page", "ex:title": "?title"}}
    });
    let rows = support::query_jsonld(fluree, ledger, &from_offer)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!([[PAGE, "the offer's page"]]));

    let tagged = format!("SELECT ?tag WHERE {{ <{OFFER}> <{TAG}> ?tag }}");
    let rows = support::query_sparql(fluree, ledger, &tagged)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!([["featured"]]));
}

/// Write the offer and its page to a new file-backed ledger, its first commit
/// declaring `mode` (or the new-ledger default), then check the mode and the
/// answers after the write, after a reload from the commits alone, and after
/// a reload from an index.
async fn write_reload_and_index(ledger_id: &str, mode: Option<NsSplitMode>) {
    let expected = mode.unwrap_or(NsSplitMode::FOR_NEW_LEDGERS);
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
    let mut ledger = fluree.create_ledger(ledger_id).await.unwrap();
    assert_eq!(
        ledger.snapshot.ns_split_mode(),
        NsSplitMode::FOR_NEW_LEDGERS
    );
    if let Some(mode) = mode {
        std::sync::Arc::make_mut(&mut ledger.snapshot)
            .set_ns_split_mode(mode, 0)
            .unwrap();
    }
    let written = fluree
        .insert(ledger, &offer_with_its_page())
        .await
        .unwrap()
        .ledger;
    assert_eq!(written.snapshot.ns_split_mode(), expected);
    assert!(has_namespace(&written, VENDOR));
    assert_eq!(
        has_namespace(&written, PAGE),
        expected == NsSplitMode::MostGranular
    );
    assert_page_found(&fluree, &written).await;
    drop(fluree);

    let fluree = FlureeBuilder::file(path.clone()).build().expect("reopen");
    let replayed = fluree.ledger(ledger_id).await.unwrap();
    assert!(replayed.snapshot.index_root_id().is_none());
    assert_eq!(replayed.snapshot.ns_split_mode(), expected);
    assert_page_found(&fluree, &replayed).await;
    drop(fluree);

    // A full build, then an incremental one over it, each read back from a
    // fresh process.
    for (t, offer, incremental) in [(2, "Offer13", false), (3, "Offer14", true)] {
        let fluree = FlureeBuilder::file(path.clone()).build().expect("reopen");
        let ledger = fluree.ledger(ledger_id).await.unwrap();
        let more = json!({"@id": format!("{VENDOR}{offer}"), TAG: "new"});
        fluree.insert(ledger, &more).await.unwrap();
        if incremental {
            support::build_incrementally_and_publish_index(&fluree, ledger_id).await;
        } else {
            support::rebuild_and_publish_index(&fluree, ledger_id).await;
        }
        drop(fluree);

        let fluree = FlureeBuilder::file(path.clone()).build().expect("reopen");
        let indexed = fluree.ledger(ledger_id).await.unwrap();
        assert_eq!(indexed.index_t(), t);
        assert_eq!(indexed.snapshot.ns_split_mode(), expected);
        assert_page_found(&fluree, &indexed).await;
        assert_tag_has_statistics(&indexed);
    }
}

/// The planner reads a predicate's statistics by the SID its IRI encodes to.
fn assert_tag_has_statistics(ledger: &LedgerState) {
    let tag = ledger.snapshot.encode_iri(TAG).unwrap();
    let properties = ledger
        .snapshot
        .stats
        .as_ref()
        .unwrap()
        .properties
        .as_deref()
        .unwrap();
    assert!(
        properties
            .iter()
            .any(|p| p.sid == (tag.namespace_code, tag.name.to_string())),
        "no statistics under {tag:?}: {:?}",
        properties.iter().map(|p| &p.sid).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_new_ledger_files_a_page_iri_under_the_namespace_before_it() {
    write_reload_and_index("it/named-local:main", None).await;
}

#[tokio::test]
async fn a_ledger_whose_first_commit_declares_most_granular_keeps_it() {
    write_reload_and_index("it/most-granular:main", Some(NsSplitMode::MostGranular)).await;
}
