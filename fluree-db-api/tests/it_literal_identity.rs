//! Literal term identity in constant-object matching.
//!
//! `"bob"`, `"bob"@en` and `"bob"@fr` are three distinct RDF terms. They
//! share one string-dictionary key, and SPARQL lowering used to attach no
//! datatype/language constraint to ordinary triple objects, so a bound
//! literal matched all three (and `VALUES ?o { "bob"@en }` projected the
//! wrong literal for the false matches). Per-row join/OPTIONAL probes
//! rebuilt the object from a binding the same way, so joins on a string
//! variable equated every tag.
//!
//! Now SPARQL string literals (plain, tagged, explicitly typed) carry their
//! term constraint into the scan, join/OPTIONAL probes carry the binding's
//! string constraint, and novelty-side filters honour the tag. Bare numerics
//! keep their lenient cross-subtype matching, as does a plain JSON string in
//! the JSON-LD query surface (pinned below so a change there is deliberate).

#![cfg(feature = "native")]

mod support;
use crate::support::{
    query_jsonld_formatted, query_sparql, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value};

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/> \
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

async fn sparql(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::LedgerState,
    body: &str,
) -> Value {
    let rows = query_sparql(fluree, view, &format!("{PREFIX}{body}"))
        .await
        .expect("query");
    rows.to_jsonld(&view.snapshot).expect("jsonld")
}

#[tokio::test]
async fn string_literals_match_by_term_identity_in_novelty_and_index() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let id = "it/literal-identity:main";
            let ledger = fluree.create_ledger(id).await.unwrap();
            let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:d4", "ex:authoredBy": {"@value": "bob", "@language": "en"}},
                {"@id": "ex:d5", "ex:authoredBy": {"@value": "bob", "@language": "fr"}},
                {"@id": "ex:d6", "ex:authoredBy": "bob"},
                {"@id": "ex:d7", "ex:editedBy": {"@value": "bob", "@language": "fr"}}
            ]});
            let r = fluree.insert(ledger, &data).await.unwrap();

            // Every shape is checked against novelty-only state, then again
            // once the same data is in the binary index (different scan,
            // probe, and overlay paths).
            for phase in ["novelty", "indexed"] {
                if phase == "indexed" {
                    trigger_index_and_wait_outcome(&handle, id, r.receipt.t).await;
                }
                let view = fluree.ledger(id).await.unwrap();
                assert_eq!(
                    view.snapshot.range_provider.is_some(),
                    phase == "indexed",
                    "{phase}: setup"
                );

                let cases: &[(&str, &str, Value)] = &[
                    (
                        "tagged en",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy "bob"@en }"#,
                        json!([["ex:d4"]]),
                    ),
                    (
                        "tagged fr",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy "bob"@fr }"#,
                        json!([["ex:d5"]]),
                    ),
                    (
                        "plain is xsd:string only",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy "bob" }"#,
                        json!([["ex:d6"]]),
                    ),
                    (
                        "explicit xsd:string",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy "bob"^^xsd:string }"#,
                        json!([["ex:d6"]]),
                    ),
                    (
                        "one-row VALUES keeps its literal",
                        r#"SELECT ?s ?o WHERE { VALUES ?o { "bob"@en } ?s ex:authoredBy ?o }"#,
                        json!([["ex:d4", {"@value": "bob", "@language": "en"}]]),
                    ),
                    (
                        "multi-row VALUES probes per tag",
                        r#"SELECT ?s ?o WHERE { VALUES ?o { "bob"@en "bob"@fr } ?s ex:authoredBy ?o }
                           ORDER BY ?s"#,
                        json!([
                            ["ex:d4", {"@value": "bob", "@language": "en"}],
                            ["ex:d5", {"@value": "bob", "@language": "fr"}]
                        ]),
                    ),
                    (
                        "self-join on a string var is term-exact",
                        "SELECT ?a ?b WHERE { ?a ex:authoredBy ?o . ?b ex:authoredBy ?o FILTER(?a != ?b) }",
                        json!([]),
                    ),
                    (
                        "cross-predicate join matches the same tag only",
                        "SELECT ?a ?b WHERE { ?a ex:authoredBy ?o . ?b ex:editedBy ?o }",
                        json!([["ex:d5", "ex:d7"]]),
                    ),
                    (
                        "OPTIONAL probe is term-exact",
                        "SELECT ?a ?b WHERE { ?a ex:authoredBy ?o .
                                            OPTIONAL { ?b ex:authoredBy ?o FILTER(?a != ?b) } }
                         ORDER BY ?a",
                        json!([["ex:d4", null], ["ex:d5", null], ["ex:d6", null]]),
                    ),
                    (
                        "property-path endpoint keeps its tag",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy|ex:editedBy "bob"@fr } ORDER BY ?s"#,
                        json!([["ex:d5"], ["ex:d7"]]),
                    ),
                    (
                        "FILTER equality (already exact) agrees",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy ?o FILTER(?o = "bob"@en) }"#,
                        json!([["ex:d4"]]),
                    ),
                ];
                for (name, body, expected) in cases {
                    let got = sparql(&fluree, &view, body).await;
                    assert_eq!(&got, expected, "{phase}: {name}");
                }

                // JSON-LD surface: `@language` / `@type` objects are exact;
                // a plain JSON string keeps matching every string datatype
                // and tag (pinned on purpose — changing it is a product
                // decision, not a side effect of this fix).
                let jl = |o: Value| {
                    json!({"@context": {"ex": "http://example.org/ns/"},
                           "select": "?s", "where": {"@id": "?s", "ex:authoredBy": o}})
                };
                let en = query_jsonld_formatted(
                    &fluree,
                    &view,
                    &jl(json!({"@value": "bob", "@language": "en"})),
                )
                .await
                .unwrap();
                assert_eq!(en, json!(["ex:d4"]), "{phase}: json-ld @language");
                let typed = query_jsonld_formatted(
                    &fluree,
                    &view,
                    &jl(json!({"@value": "bob", "@type": "http://www.w3.org/2001/XMLSchema#string"})),
                )
                .await
                .unwrap();
                assert_eq!(typed, json!(["ex:d6"]), "{phase}: json-ld @type");
                let mut plain = query_jsonld_formatted(&fluree, &view, &jl(json!("bob")))
                    .await
                    .unwrap();
                plain.as_array_mut().unwrap().sort_by_key(std::string::ToString::to_string);
                assert_eq!(
                    plain,
                    json!(["ex:d4", "ex:d5", "ex:d6"]),
                    "{phase}: json-ld plain string stays lenient"
                );
            }
        })
        .await;
}

/// Bare numerics are deliberately unconstrained: `25` still matches a value
/// stored under any integer subtype, as before.
#[tokio::test]
async fn bare_numeric_literals_stay_lenient_across_subtypes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let id = "it/literal-identity-num:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
        {"@id": "ex:a", "ex:age": 25},
        {"@id": "ex:b", "ex:age": {"@value": "25", "@type": "http://www.w3.org/2001/XMLSchema#int"}},
        {"@id": "ex:c", "ex:age": {"@value": "25", "@type": "http://www.w3.org/2001/XMLSchema#long"}}
    ]});
    fluree.insert(ledger, &data).await.unwrap();
    let view = fluree.ledger(id).await.unwrap();
    let got = sparql(
        &fluree,
        &view,
        "SELECT ?s WHERE { ?s ex:age 25 } ORDER BY ?s",
    )
    .await;
    assert_eq!(got, json!([["ex:a"], ["ex:b"], ["ex:c"]]));
}

/// BCP 47 tags are case-insensitive: a value written as `@EN` is stored,
/// matched and reported as `en`, whether the query writes `@en` or `@EN`.
#[tokio::test]
async fn language_tags_are_case_insensitive() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let id = "it/literal-identity-case:main";
            let ledger = fluree.create_ledger(id).await.unwrap();
            let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:upper", "ex:name": {"@value": "chat", "@language": "EN"}},
                {"@id": "ex:lower", "ex:name": {"@value": "chat", "@language": "en"}},
                {"@id": "ex:fr", "ex:name": {"@value": "chat", "@language": "fr-CA"}}
            ]});
            let r = fluree.insert(ledger, &data).await.unwrap();

            for phase in ["novelty", "indexed"] {
                if phase == "indexed" {
                    trigger_index_and_wait_outcome(&handle, id, r.receipt.t).await;
                }
                let view = fluree.ledger(id).await.unwrap();
                for q in [
                    r#"SELECT ?s WHERE { ?s ex:name "chat"@en } ORDER BY ?s"#,
                    r#"SELECT ?s WHERE { ?s ex:name "chat"@EN } ORDER BY ?s"#,
                ] {
                    assert_eq!(
                        sparql(&fluree, &view, q).await,
                        json!([["ex:lower"], ["ex:upper"]]),
                        "{phase}: {q}"
                    );
                }
                assert_eq!(
                    sparql(&fluree, &view, r#"SELECT ?s WHERE { ?s ex:name "chat"@FR-ca }"#).await,
                    json!([["ex:fr"]]),
                    "{phase}: region subtag case"
                );
                // LANG() reports the canonical lowercase form for every row.
                assert_eq!(
                    sparql(
                        &fluree,
                        &view,
                        "SELECT ?s (LANG(?n) AS ?l) WHERE { ?s ex:name ?n } ORDER BY ?s"
                    )
                    .await,
                    json!([["ex:fr", "fr-ca"], ["ex:lower", "en"], ["ex:upper", "en"]]),
                    "{phase}: LANG() canonical"
                );
                let jl = json!({"@context": {"ex": "http://example.org/ns/"}, "select": "?s",
                                "where": {"@id": "?s", "ex:name": {"@value": "chat", "@language": "En"}}});
                let mut got = query_jsonld_formatted(&fluree, &view, &jl).await.unwrap();
                got.as_array_mut().unwrap().sort_by_key(std::string::ToString::to_string);
                assert_eq!(got, json!(["ex:lower", "ex:upper"]), "{phase}: json-ld @language case");
            }
        })
        .await;
}
