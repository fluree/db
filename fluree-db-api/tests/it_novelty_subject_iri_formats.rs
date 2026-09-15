//! Exercise encoded novelty subjects directly at the formatter boundary. This
//! isolates the Update Q5 failure without replaying hundreds of update mixes.
use crate::support::{genesis_ledger, query_jsonld, query_sparql, rebuild_and_publish_index};
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

/// End to end: exhaust the namespace table, index, then commit a subject in a
/// new namespace so it lands in dictionary novelty under `OVERFLOW`. A join
/// shaped like BSBM Explore Q5 keeps the subject encoded until formatting, so
/// XML and delimited output resolve it through `BinaryGraphView` rather than
/// `DictOverlay`; export resolves it through `ExportResolver`. `STR()` must
/// return its IRI rather than the internal `code:name` form.
#[tokio::test]
async fn overflow_subject_in_novelty_formats_and_exports() {
    use fluree_db_api::export::ExportFormat;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "novelty-overflow-e2e:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let label = "http://example.org/label";
    let feat = "http://example.org/feat";
    let num = "http://example.org/num";
    let anchor = "http://anchor.example/s";

    let mut graph: Vec<_> = (0..namespaces::OVERFLOW as usize)
        .map(|i| json!({"@id": format!("http://ns{i}.example/s"), label: "base"}))
        .collect();
    graph.push(
        json!({"@id": anchor, label: "anchor", feat: {"@id": "http://example.org/f1"}, num: 1}),
    );
    fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .unwrap();
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed = fluree.ledger(ledger_id).await.unwrap();

    let iri = "http://novel-overflow.example/s?x=1&y=2";
    fluree
        .insert_with_opts(
            indexed,
            &json!({"@id": iri, label: "novel", feat: {"@id": "http://example.org/f1"}, num: 5}),
            TxnOpts::default(),
            CommitOpts::default(),
            &fluree_db_api::IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .unwrap();
    let ledger = fluree.ledger(ledger_id).await.unwrap();

    let result = query_sparql(
        &fluree,
        &ledger,
        &format!(
            "SELECT DISTINCT ?s ?l WHERE {{
               ?s <{label}> ?l . FILTER(<{anchor}> != ?s)
               <{anchor}> <{feat}> ?f . ?s <{feat}> ?f .
               <{anchor}> <{num}> ?n1 . ?s <{num}> ?n2 .
               FILTER(?n2 < (?n1 + 10) && ?n2 > (?n1 - 10))
             }} ORDER BY DESC(?l) LIMIT 5"
        ),
    )
    .await
    .unwrap();
    let var = result.vars.get("?s").unwrap();
    let rows: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| (0..b.len()).map(move |i| b.get(i, var).unwrap().clone()))
        .collect();
    assert!(
        matches!(rows.as_slice(), [Binding::EncodedSid { .. }]),
        "the subject must reach the formatters encoded: {rows:?}"
    );

    let xml = format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::sparql_xml(),
    )
    .unwrap();
    assert!(
        xml.contains("<uri>http://novel-overflow.example/s?x=1&amp;y=2</uri>"),
        "{xml}"
    );
    assert_eq!(
        result.to_csv(&ledger.snapshot).unwrap(),
        format!("s,l\n{iri},novel\n")
    );
    assert_eq!(
        result.to_tsv(&ledger.snapshot).unwrap(),
        format!("s\tl\n{iri}\tnovel\n")
    );

    // STR() of an overflow subject is its IRI, in novelty and once persisted
    // (the last base subjects overflowed the namespace table).
    let row_count =
        |r: fluree_db_api::QueryResult| -> usize { r.batches.iter().map(Batch::len).sum() };
    for target in [iri, "http://ns65533.example/s"] {
        for filter in [
            format!("STR(?s) = \"{target}\""),
            format!("xsd:string(?s) = \"{target}\""),
        ] {
            let sparql = format!(
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                 SELECT ?s WHERE {{ ?s <{label}> ?l FILTER({filter}) }}"
            );
            let result = query_sparql(&fluree, &ledger, &sparql).await.unwrap();
            assert_eq!(row_count(result), 1, "{sparql}");
        }
        let jsonld = json!({
            "select": ["?s"],
            "where": [
                {"@id": "?s", label: "?l"},
                ["filter", format!("(= (str ?s) \"{target}\")")]
            ]
        });
        let result = query_jsonld(&fluree, &ledger, &jsonld).await.unwrap();
        assert_eq!(row_count(result), 1, "{jsonld}");
    }

    let mut buf: Vec<u8> = Vec::new();
    fluree
        .export(ledger_id)
        .format(ExportFormat::NTriples)
        .write_to(&mut buf)
        .await
        .expect("export ntriples");
    let out = String::from_utf8(buf).unwrap();
    assert!(
        out.contains(&format!("<{iri}> <{label}> \"novel\" .")),
        "{out}"
    );
}
