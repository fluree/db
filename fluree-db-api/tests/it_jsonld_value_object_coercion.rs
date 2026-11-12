//! A value object under a term whose context coerces values to IRIs.
//!
//! A context term with `"@type": "@id"` (or `"@vocab"`) turns a string value
//! into an IRI. JSON-LD 1.1 applies that coercion only to strings (Value
//! Expansion); a value object such as `{"@value": "TWD"}` is a literal. The
//! expander used to read it as the relative IRI `<TWD>`, so an insert or
//! upsert of such a document stored a reference where the same data in
//! Turtle or TriG stores the string "TWD".

use crate::support;
use crate::support::normalize_rows;
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value};

const QUERY: &str = r"PREFIX ex: <http://example.org/>
    SELECT ?unit (isLiteral(?unit) AS ?literal) (LANG(?unit) AS ?lang)
    WHERE { ex:price ex:unit ?unit }";

fn doc(coercion: &str, unit: Value) -> Value {
    json!({
        "@context": {
            "ex": "http://example.org/",
            "unit": {"@id": "ex:unit", "@type": coercion}
        },
        "@id": "ex:price",
        "unit": unit
    })
}

async fn unit_rows(fluree: &support::MemoryFluree, ledger: &support::MemoryLedger) -> Vec<Value> {
    let out = support::query_sparql(fluree, ledger, QUERY)
        .await
        .expect("sparql query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    normalize_rows(&out)
}

#[tokio::test]
async fn value_object_under_id_or_vocab_term_stores_a_literal() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (i, coercion) in ["@id", "@vocab"].into_iter().enumerate() {
        let insert_ledger = fluree
            .create_ledger(&format!("tx/value-object-coercion-insert-{i}:main"))
            .await
            .expect("genesis");
        let ledger = fluree
            .insert(insert_ledger, &doc(coercion, json!({"@value": "TWD"})))
            .await
            .expect("insert")
            .ledger;
        assert_eq!(
            unit_rows(&fluree, &ledger).await,
            vec![json!(["TWD", true, ""])],
            "insert under {coercion}"
        );

        // A JSON-LD query written with the same context matches what the
        // insert stored: its `where` already read the value object as a
        // literal.
        let query = json!({
            "@context": doc(coercion, Value::Null)["@context"],
            "select": "?s",
            "where": {"@id": "?s", "unit": {"@value": "TWD"}}
        });
        let rows = support::query_jsonld(&fluree, &ledger, &query)
            .await
            .expect("jsonld query")
            .to_jsonld(&ledger.snapshot)
            .expect("to_jsonld");
        assert_eq!(rows, json!(["ex:price"]), "query under {coercion}");

        let upsert_ledger = fluree
            .create_ledger(&format!("tx/value-object-coercion-upsert-{i}:main"))
            .await
            .expect("genesis");
        let ledger = fluree
            .upsert(
                upsert_ledger,
                &doc(coercion, json!({"@value": "TWD", "@language": "en"})),
            )
            .await
            .expect("upsert")
            .ledger;
        assert_eq!(
            unit_rows(&fluree, &ledger).await,
            vec![json!([{"@value": "TWD", "@language": "en"}, true, "en"])],
            "upsert under {coercion}"
        );
    }
}

/// A value object typed `@id` names an IRI, which only a string can: any
/// other `@value` is refused rather than stored as a reference to nothing.
#[tokio::test]
async fn a_value_object_typed_id_with_a_non_string_value_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (i, value) in [json!(5), json!(true)].into_iter().enumerate() {
        let ledger = fluree
            .create_ledger(&format!("tx/value-object-id-non-string-{i}:main"))
            .await
            .expect("genesis");
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:price",
            "ex:unit": {"@value": value, "@type": "@id"}
        });
        let err = fluree
            .insert(ledger, &doc)
            .await
            .expect_err("a non-string @value typed @id");
        assert_eq!(err.status_code(), 400, "{value}: {err}");
        assert!(
            err.to_string().contains("@value must be a string"),
            "{value}: {err}"
        );
    }
}
