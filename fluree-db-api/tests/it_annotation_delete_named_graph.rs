//! Deleting a named-graph annotation by id retracts its whole bundle and
//! its body in that graph.
//!
//! The by-id delete names the bundle's subject, predicate and object slots.
//! The cascade then cleaned up the annotation's body (LPG mode) through a
//! range read whose index-resident rows carry no graph, so the body's
//! retraction landed in the default graph and the body survived in its own
//! graph. And nothing retracted the bundle's `f:reifiesGraph` anchor, which
//! stayed behind describing nothing. Both now come from the stored facts of
//! the annotation, graph stamped.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::{range_with_overlay, FlakeValue, RangeMatch, RangeOptions, RangeTest};
use serde_json::json;

const EX: &str = "http://example.org/";
const G1: &str = "http://example.org/g1";

/// Every current fact of `subject` in `graph` (None = default), as
/// `pred=value`, sorted — `f:reifies*` included.
async fn facts(ledger: &LedgerState, graph: Option<&str>, subject: &str) -> Vec<String> {
    let g_id = match graph {
        None => 0,
        Some(iri) => ledger
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .expect("graph"),
    };
    let s = ledger
        .snapshot
        .encode_iri(&format!("{EX}{subject}"))
        .expect("subject");
    let flakes = range_with_overlay(
        &ledger.snapshot,
        g_id,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::new().with_subject(s),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .expect("range");
    let mut out: Vec<String> = flakes
        .iter()
        .filter(|f| f.op)
        .map(|f| match &f.o {
            FlakeValue::String(v) => format!("{}={v}", f.p.name),
            FlakeValue::Ref(r) => format!("{}=<{}>", f.p.name, r.name),
            other => format!("{}={other:?}", f.p.name),
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn by_id_delete_of_a_named_graph_annotation_retracts_bundle_and_body_in_that_graph() {
    for indexed in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .expect("fluree");
        let id = format!("it/ann-delete-g1-{indexed}:main");
        let ledger = fluree.create_ledger(&id).await.unwrap();
        let mut ledger = fluree
            .insert(
                ledger,
                &json!({
                    "@context": {"ex": EX},
                    "@graph": [{
                        "@id": "ex:alice", "@graph": G1,
                        "ex:worksFor": {
                            "@id": "ex:acme",
                            "@annotation": {"@id": "ex:empA", "ex:role": "Engineer"}
                        }
                    }]
                }),
            )
            .await
            .unwrap()
            .ledger;
        if indexed {
            drop(ledger);
            support::rebuild_and_publish_index(&fluree, &id).await;
            ledger = fluree.ledger(&id).await.unwrap();
        }
        let before = facts(&ledger, Some(G1), "empA").await;
        assert!(
            before.iter().any(|f| f.starts_with("reifiesGraph="))
                && before.iter().any(|f| f == "role=Engineer"),
            "indexed={indexed}: precondition {before:?}"
        );

        let r = fluree
            .update(
                ledger,
                &json!({
                    "@context": {"ex": EX},
                    "graph": G1,
                    "delete": {
                        "@id": "ex:alice",
                        "ex:worksFor": {"@id": "ex:acme", "@annotation": {"@id": "ex:empA"}}
                    },
                    "opts": {"lpgEdgeLifecycle": true}
                }),
            )
            .await
            .expect("by-id annotation delete");
        let mut ledger = r.ledger;
        for phase in ["after the delete", "after the next index build"] {
            assert_eq!(
                facts(&ledger, Some(G1), "empA").await,
                Vec::<String>::new(),
                "indexed={indexed}, {phase}: the bundle (anchor included) and the body are gone"
            );
            assert_eq!(
                facts(&ledger, Some(G1), "alice").await,
                ["worksFor=<acme>"],
                "indexed={indexed}, {phase}: the base edge stays"
            );
            if phase == "after the delete" {
                drop(ledger);
                support::rebuild_and_publish_index(&fluree, &id).await;
                ledger = fluree.ledger(&id).await.unwrap();
            }
        }
        // Nothing of the annotation's was ever in the default graph, so
        // nothing may be retracted there.
        assert_eq!(
            r.receipt.retract_count, 5,
            "indexed={indexed}: three named slots, the anchor, and the body"
        );
    }
}
