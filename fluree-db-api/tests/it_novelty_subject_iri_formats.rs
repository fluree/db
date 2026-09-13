//! Exercise encoded novelty subjects directly at the formatter boundary. This
//! isolates the Update Q5 failure without replaying hundreds of update mixes.
use crate::support::{genesis_ledger, query_sparql, rebuild_and_publish_index};
use fluree_db_api::{format::format_results_string, FlureeBuilder, FormatterConfig};
use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_query::binding::{Batch, Binding};
use fluree_vocab::namespaces;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn novelty_full_iri_subjects_format_as_xml_json_csv_and_tsv() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "novelty-iri-formats:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(
            ledger,
            &json!({
                "@id": "https://example.org/seed",
                "http://www.w3.org/2000/01/rdf-schema#label": "seed"
            }),
        )
        .await
        .unwrap();
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let mut result = query_sparql(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label }",
    )
    .await
    .unwrap();
    let store = result
        .binary_graph
        .as_ref()
        .expect("indexed graph view")
        .clone_store();
    let var = result.vars.get_or_insert("?s");
    let iri = "https://unregistered.example/product?x=1&y=2";

    for ns_code in [namespaces::OVERFLOW, namespaces::EMPTY] {
        let mut novelty = DictNovelty::with_watermarks(vec![], 0);
        let s_id = novelty.subjects.assign_or_lookup(ns_code, iri);
        result.binary_graph = Some(BinaryGraphView::with_novelty(
            Arc::clone(&store),
            0,
            Some(Arc::new(novelty)),
        ));
        result.batches = vec![Batch::single_row(
            Arc::from(vec![var].into_boxed_slice()),
            vec![Binding::encoded_sid(s_id)],
        )
        .unwrap()];

        // JSON takes the Sid materialization path; XML/delimited formats take
        // the full-IRI path. Keep their behavior aligned for the same binding.
        let expected = json!({"type":"uri", "value":iri});
        let json = result.to_sparql_json(&ledger.snapshot).unwrap();
        assert_eq!(json["results"]["bindings"][0]["s"], expected);
        let streamed_json = format_results_string(
            &result,
            &result.context,
            &ledger.snapshot,
            &FormatterConfig::sparql_json(),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&streamed_json).unwrap(),
            json
        );
        let xml = format_results_string(
            &result,
            &result.context,
            &ledger.snapshot,
            &FormatterConfig::sparql_xml(),
        )
        .unwrap();
        assert!(
            xml.contains("<uri>https://unregistered.example/product?x=1&amp;y=2</uri>"),
            "{xml}"
        );
        assert_eq!(
            result.to_csv(&ledger.snapshot).unwrap(),
            format!("s\n{iri}\n")
        );
        assert_eq!(
            result.to_tsv(&ledger.snapshot).unwrap(),
            format!("s\n{iri}\n")
        );
    }
}
