//! SPARQL Results XML renders a value repeated down a column once and copies
//! its text to the rows that follow. These pin that a copy is only ever taken
//! for the same value: a repeated literal renders identically (escaping
//! included) on every row, and adjacent values that share their lexical form
//! but not their language stay distinct.
use crate::support::{genesis_ledger, query_sparql, rebuild_and_publish_index};
use fluree_db_api::{format::format_results_string, FlureeBuilder, FormatterConfig, QueryResult};
use fluree_db_query::binding::Binding;
use serde_json::json;

const P: &str = "http://example.org/p";

async fn indexed_ledger() -> (fluree_db_api::Fluree, String) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "xml-repeated-values:main".to_string();
    let ledger = genesis_ledger(&fluree, &ledger_id);
    fluree
        .insert(
            ledger,
            &json!({
                "@id": P,
                "http://example.org/label": "a < b & c",
                "http://example.org/feature": [
                    {"@id": "http://example.org/f1"},
                    {"@id": "http://example.org/f2"},
                    {"@id": "http://example.org/f3"}
                ],
                "http://example.org/name": [
                    {"@value": "chat", "@language": "en"},
                    {"@value": "chat", "@language": "fr"}
                ]
            }),
        )
        .await
        .unwrap();
    rebuild_and_publish_index(&fluree, &ledger_id).await;
    (fluree, ledger_id)
}

async fn xml(fluree: &fluree_db_api::Fluree, ledger_id: &str, sparql: &str, var: &str) -> String {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let mut result: QueryResult = query_sparql(fluree, &ledger, sparql).await.unwrap();
    // The reuse applies to encoded values; a materialized column would pass
    // without exercising it.
    let var = result.vars.get_or_insert(var);
    let encoded = result.batches.iter().any(|batch| {
        let col = batch
            .schema()
            .iter()
            .position(|&v| v == var)
            .expect("column");
        (0..batch.len()).any(|row| matches!(batch.get_by_col(row, col), Binding::EncodedLit { .. }))
    });
    assert!(encoded, "expected encoded literals in the result");
    format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::sparql_xml(),
    )
    .unwrap()
}

#[tokio::test]
async fn a_literal_repeated_down_a_column_renders_the_same_on_every_row() {
    let (fluree, ledger_id) = indexed_ledger().await;

    let xml = xml(
        &fluree,
        &ledger_id,
        &format!(
            "SELECT ?f ?label WHERE {{ <{P}> <http://example.org/label> ?label ; \
             <http://example.org/feature> ?f }}"
        ),
        "?label",
    )
    .await;

    assert_eq!(xml.matches("<result>").count(), 3, "{xml}");
    assert_eq!(
        xml.matches(r#"<binding name="label"><literal>a &lt; b &amp; c</literal></binding>"#)
            .count(),
        3,
        "{xml}"
    );
    for f in ["f1", "f2", "f3"] {
        assert!(
            xml.contains(&format!("<uri>http://example.org/{f}</uri>")),
            "{xml}"
        );
    }
}

#[tokio::test]
async fn adjacent_values_differing_only_in_language_stay_distinct() {
    let (fluree, ledger_id) = indexed_ledger().await;

    let xml = xml(
        &fluree,
        &ledger_id,
        &format!("SELECT ?name WHERE {{ <{P}> <http://example.org/name> ?name }}"),
        "?name",
    )
    .await;

    assert_eq!(xml.matches("<result>").count(), 2, "{xml}");
    assert_eq!(
        xml.matches(r#"<literal xml:lang="en">chat</literal>"#)
            .count(),
        1,
        "{xml}"
    );
    assert_eq!(
        xml.matches(r#"<literal xml:lang="fr">chat</literal>"#)
            .count(),
        1,
        "{xml}"
    );
}
