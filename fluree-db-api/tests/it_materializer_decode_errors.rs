//! An encoded binding that cannot be decoded must fail materialization rather
//! than yield a placeholder: upsert builds retraction flakes from `to_term`, so
//! a fabricated `_:unknown_<id>` would retract nothing and leave the old value.
use crate::support::{genesis_ledger, query_sparql, rebuild_and_publish_index};
use fluree_db_api::FlureeBuilder;
use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::ObjKind;
use fluree_db_query::{Binding, JoinKeyMode, Materializer, QueryError};
use serde_json::json;

#[tokio::test]
async fn undecodable_encoded_bindings_are_errors() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "materializer-decode-errors:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let subject = "https://example.org/s";
    fluree
        .insert(
            ledger,
            &json!({"@id": subject, "https://example.org/label": "seed"}),
        )
        .await
        .unwrap();
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let result = query_sparql(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { ?s <https://example.org/label> ?l }",
    )
    .await
    .unwrap();
    let store = result
        .binary_graph
        .as_ref()
        .expect("indexed graph view")
        .clone_store();

    let s_id = store
        .find_subject_id(subject)
        .unwrap()
        .expect("indexed subject");
    let missing_s_id = SubjectId::new(SubjectId::from_u64(s_id).ns_code(), 1 << 40).as_u64();
    let missing_p_id = store.predicate_count() + 1000;
    let missing_lit = Binding::EncodedLit {
        o_kind: ObjKind::LEX_ID.as_u8(),
        o_key: u64::from(store.string_count()) + 1000,
        p_id: 0,
        dt_id: 0,
        lang_id: 0,
        i_val: 0,
        t: 1,
    };

    for mode in [JoinKeyMode::SingleLedger, JoinKeyMode::MultiLedger] {
        let mut mat = Materializer::new(BinaryGraphView::new(store.clone(), 0), mode);

        assert!(matches!(
            mat.to_term(&Binding::encoded_sid(s_id)),
            Ok(Binding::Sid { sid, .. }) if store.sid_to_iri(&sid).as_deref() == Some(subject)
        ));

        let missing = [
            Binding::encoded_sid(missing_s_id),
            Binding::EncodedPid { p_id: missing_p_id },
            missing_lit.clone(),
        ];
        for binding in &missing {
            let is_lookup_error =
                |r: Result<(), QueryError>| matches!(r, Err(QueryError::DictionaryLookup(_)));
            assert!(
                is_lookup_error(mat.to_term(binding).map(drop)),
                "to_term {binding:?}"
            );
            assert!(
                is_lookup_error(mat.comparable(binding).map(drop)),
                "comparable {binding:?}"
            );
            assert!(
                is_lookup_error(mat.as_string(binding).map(drop)),
                "as_string {binding:?}"
            );
            if mode == JoinKeyMode::MultiLedger && !matches!(binding, Binding::EncodedLit { .. }) {
                assert!(
                    is_lookup_error(mat.join_key(binding).map(drop)),
                    "join_key {binding:?}"
                );
            }
        }
    }
}
