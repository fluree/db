//! A value object under a term whose context coerces values.
//!
//! JSON-LD 1.1 applies a term's `@type` and `@language`, and the default
//! language, only to bare values (Value Expansion); a value object such as
//! `{"@value": "TWD"}` is taken as written. The expander used to read it
//! under an `@id` or `@vocab` term as the relative IRI `<TWD>`, and to give
//! it the default language and its term's datatype.

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

/// The default language and a term's datatype reach a bare value, not a
/// value object, and a `where` written with the document's context matches
/// what it stored.
#[tokio::test]
async fn value_object_takes_neither_the_default_language_nor_its_terms_datatype() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = json!({
        "ex": "http://example.org/",
        "@language": "en",
        "n": {"@id": "ex:n", "@type": "http://www.w3.org/2001/XMLSchema#integer"}
    });
    let ledger = fluree
        .create_ledger("tx/value-object-as-written:main")
        .await
        .expect("genesis");
    let document = json!({
        "@context": context,
        "@id": "ex:s",
        "ex:t": ["plain", {"@value": "obj"}],
        "n": [{"@value": "5"}, "6"]
    });
    let ledger = fluree
        .insert(ledger, &document)
        .await
        .expect("insert")
        .ledger;

    let query = r"PREFIX ex: <http://example.org/>
        SELECT ?v (STR(DATATYPE(?o)) AS ?dt) (LANG(?o) AS ?lang)
        WHERE { VALUES ?p { ex:t ex:n } ex:s ?p ?o BIND(STR(?o) AS ?v) }
        ORDER BY ?v";
    let out = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("sparql query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    let xsd = "http://www.w3.org/2001/XMLSchema#";
    let lang_string = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
    assert_eq!(
        normalize_rows(&out),
        vec![
            json!(["5", format!("{xsd}string"), ""]),
            json!(["6", format!("{xsd}integer"), ""]),
            json!(["obj", format!("{xsd}string"), ""]),
            json!(["plain", lang_string, "en"]),
        ]
    );

    for (property, value) in [
        ("n", json!({"@value": "5"})),
        ("n", json!("6")),
        ("ex:t", json!({"@value": "obj"})),
    ] {
        let query = json!({
            "@context": context,
            "select": "?s",
            "where": {"@id": "?s", property: value}
        });
        let rows = support::query_jsonld(&fluree, &ledger, &query)
            .await
            .expect("jsonld query")
            .to_jsonld(&ledger.snapshot)
            .expect("to_jsonld");
        assert_eq!(rows, json!(["ex:s"]), "{property}: {value}");
    }
}
