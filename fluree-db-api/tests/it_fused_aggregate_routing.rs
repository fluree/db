//! Routing canary for the fused R2RML aggregate (#1978) on a catalog-less local
//! Iceberg table (the committed `people` fixture, also read by
//! `it_iceberg_local_fs`).
//!
//! The fused operator folds a key-only grouping from column batches, and must
//! decline the same grouping with a SELECT expression, which it cannot evaluate
//! (the generic pipeline runs the expression once per group). The decline is
//! asserted as the absence of a `fused_r2rml_aggregate` stamp, so this test is
//! its own binary: span capture in a process shared with parallel tests can
//! miss events, which an absence assertion would read as a pass.

#![cfg(all(feature = "iceberg", feature = "native"))]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{FlureeBuilder, R2rmlCreateConfig};

const PEOPLE_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#PeopleMapping>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people" ] ;
        rr:subjectMap [
            rr:template "http://example.org/person/{id}" ;
            rr:class ex:Person
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:name ;
            rr:objectMap [ rr:column "name" ]
        ] .
"#;

/// SPARQL-JSON rows as sorted `var=value` strings.
fn rows_of(v: &serde_json::Value) -> Vec<String> {
    let mut vars: Vec<String> = v["head"]["vars"]
        .as_array()
        .expect("vars")
        .iter()
        .filter_map(|x| x.as_str().map(String::from))
        .collect();
    vars.sort();
    let mut rows: Vec<String> = v["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("not SPARQL JSON: {v}"))
        .iter()
        .map(|b| {
            vars.iter()
                .map(|var| format!("{var}={}", b[var]["value"].as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    rows.sort();
    rows
}

/// The fused aggregate folds the key-only grouping and declines it with a
/// SELECT expression; each answer is checked against the fixture's five names.
#[tokio::test(flavor = "current_thread")]
async fn fused_aggregate_declines_a_grouped_select_expression() {
    const SITE: &str = "fused_r2rml_aggregate";
    let fixtures = format!("{}/tests/fixtures/iceberg", env!("CARGO_MANIFEST_DIR"));
    // Local tables are fail-closed behind an allowlist, captured on first use.
    // SAFETY: set before any storage or scan is built; this binary has one test.
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", &fixtures);
    let location = format!("file://{fixtures}/silver/people");

    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("local-people-agg", &location, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create local-file graph source");

    let (store, _tracing_guard) = span_capture::init_test_tracing();
    for (sparql, expected, must_fire) in [
        (
            "SELECT ?name (COUNT(?s) AS ?n) FROM <local-people-agg:main> \
             WHERE { ?s <http://example.org/name> ?name } GROUP BY ?name",
            ["alice", "bob", "carol", "dave", "erin"]
                .map(|name| format!("n=1 name={name}"))
                .to_vec(),
            true,
        ),
        (
            "SELECT ?name (COUNT(?s) AS ?n) (?n * 10 AS ?m) FROM <local-people-agg:main> \
             WHERE { ?s <http://example.org/name> ?name } GROUP BY ?name",
            ["alice", "bob", "carol", "dave", "erin"]
                .map(|name| format!("m=10 n=1 name={name}"))
                .to_vec(),
            false,
        ),
    ] {
        let before = store.find_events("fast-path outcome").len();
        let result = fluree
            .query_from()
            .sparql(sparql)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("{e}\n{sparql}"));
        let proceeded: Vec<String> = store.find_events("fast-path outcome")[before..]
            .iter()
            .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
            .filter_map(|e| e.fields.get("site").cloned())
            .collect();
        assert_eq!(
            proceeded.iter().any(|s| s == SITE),
            must_fire,
            "`{SITE}` must {}proceed [proceeded: {proceeded:?}]\n{sparql}",
            if must_fire { "" } else { "not " }
        );
        assert_eq!(rows_of(&result), expected, "{sparql}");
    }
}
