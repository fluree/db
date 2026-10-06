//! End-to-end cross-ledger `f:schemaSource` reasoning.
//!
//! Data ledger D's `#config` declares `f:reasoningDefaults` →
//! `f:schemaSource` with `f:ledger` pointing at model ledger M's
//! ontology graph. At query time,
//! `view/query.rs::resolve_configured_schema_bundle` dispatches the
//! ref through the cross-ledger resolver (`ArtifactKind::SchemaClosure`),
//! M's whitelisted ontology axioms are projected onto D's snapshot as
//! `SchemaBundleFlakes`, and D's reasoner entails over them — nothing
//! is copied into D.
//!
//! The wire-artifact contract is pinned separately in
//! `it_cross_ledger_resolver.rs`; these tests prove the config-driven
//! path end to end, including the fail-closed rejection of
//! `f:followOwlImports` (the cross-ledger materializer is single-graph
//! and does not walk `owl:imports`).

#![cfg(feature = "native")]

use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::{ApiError, FlureeBuilder};
use serde_json::json;

fn config_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// Seed model ledger M with a subclass axiom, optionally inside a
/// named graph.
async fn seed_model(fluree: &fluree_db_api::Fluree, model_id: &str, graph_iri: Option<&str>) {
    let model = genesis_ledger(fluree, model_id);
    let axiom = "ex:Manager rdfs:subClassOf ex:Employee .";
    let body = match graph_iri {
        Some(iri) => format!("GRAPH <{iri}> {{ {axiom} }}"),
        None => axiom.to_string(),
    };
    let trig = format!(
        r"
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex:   <http://example.org/> .

        {body}
    "
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed M ontology");
}

/// Write D's `#config` wiring `f:schemaSource` at M's graph, then
/// insert `ex:anita a ex:Manager` into D's default graph.
async fn seed_data(
    fluree: &fluree_db_api::Fluree,
    data_id: &str,
    model_id: &str,
    graph_selector: &str,
    extra_reasoning_config: &str,
) {
    let data = genesis_ledger(fluree, data_id);
    let cfg = config_iri(data_id);
    let cfg_trig = format!(
        r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{cfg}> {{
            <urn:cfg:main>       rdf:type            f:LedgerConfig ;
                                 f:reasoningDefaults <urn:cfg:reasoning> .
            <urn:cfg:reasoning>  f:schemaSource      <urn:cfg:schema-ref> {extra_reasoning_config}.
            <urn:cfg:schema-ref> rdf:type            f:GraphRef ;
                                 f:graphSource       <urn:cfg:schema-src> .
            <urn:cfg:schema-src> f:ledger            <{model_id}> ;
                                 f:graphSelector     {graph_selector} .
        }}
    "
    );
    let r = fluree
        .stage_owned(data)
        .upsert_turtle(&cfg_trig)
        .execute()
        .await
        .expect("seed D config with cross-ledger f:schemaSource");

    let instances = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:anita",
        "@type": "ex:Manager"
    });
    fluree
        .insert(r.ledger, &instances)
        .await
        .expect("insert instance data into D");
}

fn employee_query() -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "?x", "@type": "ex:Employee"},
        "reasoning": "rdfs"
    })
}

/// The core scenario: M owns `ex:Manager rdfs:subClassOf ex:Employee`
/// in a named ontology graph; D holds `ex:anita a ex:Manager`. A
/// reasoning query on D for `?x a ex:Employee` must entail anita via
/// M's hierarchy — resolved cross-ledger, nothing copied into D.
#[tokio::test]
async fn data_ledger_reasoning_pulls_schema_from_model_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-schema/model:main";
    let data_id = "test/cross-ledger-schema/data:main";
    let ontology_iri = "http://example.org/ontology/core";

    seed_model(&fluree, model_id, Some(ontology_iri)).await;
    seed_data(&fluree, data_id, model_id, &format!("<{ontology_iri}>"), "").await;

    let view = fluree.db(data_id).await.expect("load D with config");
    let data = fluree.ledger(data_id).await.expect("reload D ledger");
    let rows = fluree
        .query(&view, &employee_query())
        .await
        .expect("query D with cross-ledger schema")
        .to_jsonld(&data.snapshot)
        .expect("to_jsonld");
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:anita")),
        "M's subclass axiom (resolved cross-ledger) must entail anita \
         as an Employee on D; got: {results:?}"
    );
}

/// Same scenario with `f:graphSelector f:defaultGraph` — the axiom
/// lives in M's default graph.
#[tokio::test]
async fn cross_ledger_schema_with_default_graph_selector() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-schema/model-default:main";
    let data_id = "test/cross-ledger-schema/data-default:main";

    seed_model(&fluree, model_id, None).await;
    seed_data(&fluree, data_id, model_id, "f:defaultGraph", "").await;

    let view = fluree.db(data_id).await.expect("load D with config");
    let data = fluree.ledger(data_id).await.expect("reload D ledger");
    let rows = fluree
        .query(&view, &employee_query())
        .await
        .expect("query D with cross-ledger schema (default graph)")
        .to_jsonld(&data.snapshot)
        .expect("to_jsonld");
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:anita")),
        "M's default-graph subclass axiom must entail anita as an \
         Employee on D; got: {results:?}"
    );
}

/// `f:followOwlImports true` combined with a cross-ledger
/// `f:schemaSource` must fail closed: the cross-ledger materializer
/// resolves a single graph and does not walk `owl:imports`, so
/// accepting the flag would silently drop the import closure from the
/// reasoning view.
#[tokio::test]
async fn cross_ledger_schema_with_follow_owl_imports_fails_closed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-schema/model-follow:main";
    let data_id = "test/cross-ledger-schema/data-follow:main";
    let ontology_iri = "http://example.org/ontology/core";

    seed_model(&fluree, model_id, Some(ontology_iri)).await;
    seed_data(
        &fluree,
        data_id,
        model_id,
        &format!("<{ontology_iri}>"),
        ";\n                                 f:followOwlImports  true ",
    )
    .await;

    let view = fluree.db(data_id).await.expect("load D with config");
    let err = fluree
        .query(&view, &employee_query())
        .await
        .expect_err("followOwlImports + cross-ledger schemaSource must be rejected");
    match err {
        ApiError::OntologyImport(msg) => {
            assert!(
                msg.contains("followOwlImports"),
                "error should name the unsupported flag: {msg}"
            );
        }
        other => panic!("expected OntologyImport, got {other:?}"),
    }
}

/// Dataset-path parity: the same cross-ledger entailment must work when
/// the query takes the multi-ledger dataset path (`from` with two
/// sources → `DataSetDb` → `build_executable_for_dataset`), which
/// previously resolved `f:schemaSource` same-ledger-only and errored on
/// a cross-ledger ref. The second source is an unrelated ledger — NOT M
/// — so the hierarchy is reachable only through the cross-ledger schema
/// bundle, not via the dataset union.
#[tokio::test]
async fn dataset_query_pulls_schema_from_model_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-schema/model-ds:main";
    let data_id = "test/cross-ledger-schema/data-ds:main";
    let other_id = "test/cross-ledger-schema/other-ds:main";
    let ontology_iri = "http://example.org/ontology/core";

    seed_model(&fluree, model_id, Some(ontology_iri)).await;
    seed_data(&fluree, data_id, model_id, &format!("<{ontology_iri}>"), "").await;

    let other = genesis_ledger(&fluree, other_id);
    fluree
        .insert(
            other,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:unrelated",
                "ex:note": "no ontology here"
            }),
        )
        .await
        .expect("seed unrelated ledger");

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "from": [data_id, other_id],
        "select": "?x",
        "where": {"@id": "?x", "@type": "ex:Employee"},
        "reasoning": "rdfs"
    });
    let result = fluree
        .query_connection(&q)
        .await
        .expect("dataset query with cross-ledger schema");
    let data = fluree.ledger(data_id).await.expect("reload D ledger");
    let rows = result.to_jsonld(&data.snapshot).expect("to_jsonld");
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:anita")),
        "M's subclass axiom must entail anita on the dataset path; \
         got: {results:?}"
    );
}

/// Dataset-path parity for the fail-closed guard: `f:followOwlImports`
/// + cross-ledger `f:schemaSource` must reject on the dataset path
/// exactly as it does on the single-ledger path (shared choke point).
#[tokio::test]
async fn dataset_follow_owl_imports_fails_closed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-schema/model-ds-follow:main";
    let data_id = "test/cross-ledger-schema/data-ds-follow:main";
    let other_id = "test/cross-ledger-schema/other-ds-follow:main";
    let ontology_iri = "http://example.org/ontology/core";

    seed_model(&fluree, model_id, Some(ontology_iri)).await;
    seed_data(
        &fluree,
        data_id,
        model_id,
        &format!("<{ontology_iri}>"),
        ";\n                                 f:followOwlImports  true ",
    )
    .await;
    let other = genesis_ledger(&fluree, other_id);
    fluree
        .insert(
            other,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:unrelated",
                "ex:note": "no ontology here"
            }),
        )
        .await
        .expect("seed unrelated ledger");

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "from": [data_id, other_id],
        "select": "?x",
        "where": {"@id": "?x", "@type": "ex:Employee"},
        "reasoning": "rdfs"
    });
    let err = fluree
        .query_connection(&q)
        .await
        .expect_err("followOwlImports + cross-ledger schemaSource must reject on dataset path");
    let msg = err.to_string();
    assert!(
        msg.contains("followOwlImports"),
        "error should name the unsupported flag: {msg}"
    );
}
