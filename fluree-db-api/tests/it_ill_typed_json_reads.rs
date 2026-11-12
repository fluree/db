//! An `rdf:JSON` literal whose text is not JSON reads back as the typed
//! literal it is, on every output.
//!
//! A transaction refuses such a literal, but bulk import keeps it as written,
//! so a ledger can hold one. It reads back the way any other ill-typed literal
//! does: `"not json"^^rdf:JSON` in SPARQL results, CONSTRUCT and export, and
//! `{"@value": "not json", "@type": <rdf:JSON IRI>}` in JSON-LD, typed JSON
//! and agent JSON, select and graph crawl alike. Never `@json`, which would
//! declare it a JSON value. A valid JSON literal reads back exactly as before.

use fluree_db_api::export::ExportFormat;
use fluree_db_api::format::{
    format_results, format_results_async, format_results_string, format_results_string_async,
};
use fluree_db_api::{Fluree, FlureeBuilder, FormatterConfig, LedgerState};
use serde_json::{json, Value};

const EX: &str = "http://example.org/ns/";
const RDF_JSON: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const LEDGER: &str = "it/json-reads:main";

/// A ledger built by bulk import, holding `ex:s ex:doc '<lexical>'^^rdf:JSON`
/// beside `ex:s ex:name "s"`.
async fn imported(lexical: &str) -> (tempfile::TempDir, Fluree, LedgerState) {
    let data = tempfile::tempdir().unwrap();
    let db = tempfile::tempdir().unwrap();
    std::fs::write(
        data.path().join("data.ttl"),
        format!("<{EX}s> <{EX}doc> '{lexical}'^^<{RDF_JSON}> .\n<{EX}s> <{EX}name> \"s\" .\n"),
    )
    .unwrap();
    let fluree = FlureeBuilder::file(db.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    fluree
        .create(LEDGER)
        .import(data.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    (db, fluree, ledger)
}

/// How each output should render the literal.
struct Expected<'a> {
    /// Surfaces whose output must be these bytes exactly.
    exact: &'a [(&'a str, &'a str)],
    /// The stored text.
    text: &'a str,
    /// JSON-LD and agent JSON: the document itself, or the typed literal.
    bare: Value,
    /// Typed JSON: `{"@value": document, "@type": "@json"}`, or the typed literal.
    typed: Value,
    /// A crawl's `{"p": ["@value", "@type"]}` and the JSON-LD export.
    valued: Value,
}

/// Run every read surface over a ledger holding `lexical` and compare it with
/// `expected`. Returns the surfaces that differ, with what they gave.
async fn read_surfaces(lexical: &str, expected: &Expected<'_>) -> Vec<String> {
    let (_db, fluree, ledger) = imported(lexical).await;
    let db = crate::support::graphdb_from_ledger(&ledger);
    let doc = format!("{EX}doc");
    let name = format!("{EX}name");
    let s = format!("{EX}s");
    let text = expected.text;
    let mut differing = Vec::new();
    let mut exact_checked = 0;
    let mut check = |surface: &str, got: Result<String, String>, want: &dyn Fn(&str) -> bool| {
        let exact = expected
            .exact
            .iter()
            .find(|(s, _)| *s == surface)
            .map(|(_, e)| *e);
        exact_checked += usize::from(exact.is_some());
        match (got, exact) {
            (Ok(got), Some(exact)) if got == exact => {}
            (Ok(got), None) if want(&got) => {}
            (other, _) => differing.push(format!("{surface}: {other:?}")),
        }
    };
    let json_eq = |want: Value| {
        move |got: &str| serde_json::from_str::<Value>(got).ok() == Some(want.clone())
    };

    // SELECT, every result format, DOM and streamed.
    let select = fluree
        .query(&db, &format!("SELECT ?o WHERE {{ <{s}> <{doc}> ?o }}"))
        .await
        .expect("select");
    let sparql_json = json!({"head": {"vars": ["o"]}, "results": {"bindings": [
        {"o": {"type": "literal", "value": text, "datatype": RDF_JSON}}]}});
    let agent = json!({"schema": {"?o": RDF_JSON}, "rows": [{"?o": expected.bare}],
                       "rowCount": 1, "t": 1, "hasMore": false});
    for (format, config, want) in [
        (
            "JSON-LD",
            FormatterConfig::jsonld(),
            json!([[expected.bare]]),
        ),
        (
            "typed JSON",
            FormatterConfig::typed_json(),
            json!([{"?o": expected.typed}]),
        ),
        ("agent JSON", FormatterConfig::agent_json(), agent),
        ("SPARQL JSON", FormatterConfig::sparql_json(), sparql_json),
    ] {
        let dom = format_results(&select, &select.context, &ledger.snapshot, &config)
            .map(|v| v.to_string())
            .map_err(|e| e.to_string());
        check(&format!("select {format}"), dom, &json_eq(want.clone()));
        let streamed = format_results_string(&select, &select.context, &ledger.snapshot, &config)
            .map_err(|e| e.to_string());
        check(
            &format!("select {format}, streamed"),
            streamed,
            &json_eq(want),
        );
    }
    let xml_literal = format!("<literal datatype=\"{RDF_JSON}\">{text}</literal>");
    let csv_cell = if text.contains('"') {
        format!("\"{}\"", text.replace('"', "\"\""))
    } else {
        text.to_string()
    };
    for (format, config, want) in [
        ("SPARQL XML", FormatterConfig::sparql_xml(), xml_literal),
        ("CSV", FormatterConfig::csv(), format!("o\n{csv_cell}\n")),
        ("TSV", FormatterConfig::tsv(), format!("o\n{text}\n")),
    ] {
        let got = format_results_string(&select, &select.context, &ledger.snapshot, &config)
            .map_err(|e| e.to_string());
        check(&format!("select {format}"), got, &|g: &str| {
            g.contains(&want)
        });
    }

    // Graph crawl, plain and typed, DOM and streamed, whole node and the
    // literal's own `@value`/`@type`.
    let typed_name = json!({"@value": "s", "@type": XSD_STRING});
    for (crawl, query, plain, typed) in [
        (
            "crawl",
            json!({"select": {&s: ["*"]}}),
            json!([{"@id": &s, &doc: expected.bare, &name: "s"}]),
            json!([{"@id": &s, &doc: expected.typed, &name: typed_name}]),
        ),
        (
            "crawl of @value and @type",
            json!({"select": {&s: [{&doc: ["@value", "@type"]}]}}),
            json!([{&doc: expected.valued}]),
            json!([{&doc: expected.valued}]),
        ),
    ] {
        let result = fluree.query(&db, &query).await.expect("crawl");
        for (format, config, want) in [
            ("JSON-LD", FormatterConfig::jsonld(), plain),
            ("typed JSON", FormatterConfig::typed_json(), typed),
        ] {
            let dom = format_results_async(
                &result,
                &result.context,
                db.as_graph_db_ref(),
                &config,
                None,
                None,
            )
            .await
            .map(|v| v.to_string())
            .map_err(|e| e.to_string());
            check(&format!("{crawl} {format}"), dom, &json_eq(want.clone()));
            let streamed = format_results_string_async(
                &result,
                &result.context,
                db.as_graph_db_ref(),
                &config,
                None,
            )
            .await
            .map_err(|e| e.to_string());
            check(
                &format!("{crawl} {format}, streamed"),
                streamed,
                &json_eq(want),
            );
        }
    }

    // CONSTRUCT, as JSON-LD and as RDF text.
    let construct = fluree
        .query(&db, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
        .await
        .expect("construct");
    let graph = json!({"@context": {}, "@graph": [{"@id": &s,
        &doc: [{"@value": text, "@type": RDF_JSON}], &name: ["s"]}]});
    check(
        "CONSTRUCT JSON-LD",
        construct
            .to_construct(&ledger.snapshot)
            .map(|v| v.to_string())
            .map_err(|e| e.to_string()),
        &json_eq(graph),
    );
    let escaped = text.replace('"', "\\\"");
    let typed_text = format!("\"{escaped}\"^^<{RDF_JSON}>");
    for (format, config) in [
        ("Turtle", FormatterConfig::turtle()),
        ("N-Triples", FormatterConfig::ntriples()),
    ] {
        let got = format_results_string(&construct, &construct.context, &ledger.snapshot, &config)
            .map_err(|e| e.to_string());
        check(&format!("CONSTRUCT {format}"), got, &|g: &str| {
            g.contains(&format!("<{doc}> {typed_text}"))
        });
    }

    // Export.
    for format in [
        ExportFormat::Turtle,
        ExportFormat::NTriples,
        ExportFormat::JsonLd,
    ] {
        let mut out = Vec::new();
        let got = fluree
            .export(LEDGER)
            .format(format)
            .context(&json!({"ex": EX}))
            .write_to(&mut out)
            .await
            .map(|_| String::from_utf8_lossy(&out).to_string())
            .map_err(|e| e.to_string());
        let surface = format!("export {format:?}");
        match format {
            ExportFormat::JsonLd => check(&surface, got, &|g: &str| {
                serde_json::from_str::<Value>(g)
                    .ok()
                    .is_some_and(|v| v["@graph"][0]["ex:doc"] == expected.valued)
            }),
            ExportFormat::Turtle => check(&surface, got, &|g: &str| {
                g.contains(&format!("ex:doc {typed_text}"))
            }),
            _ => check(&surface, got, &|g: &str| {
                g.contains(&format!("<{doc}> {typed_text}"))
            }),
        }
    }
    if exact_checked != expected.exact.len() {
        differing.push(format!(
            "{exact_checked} of {} recorded outputs matched a surface",
            expected.exact.len()
        ));
    }
    differing
}

/// A valid JSON literal an import stored reads back exactly as it always has,
/// byte for byte on every surface.
#[tokio::test]
async fn an_imported_json_literal_reads_back_as_before() {
    assert_eq!(VALID_OUTPUTS.len(), 25, "one recorded output per surface");
    let document = json!({"a": [true], "b": 1});
    let expected = Expected {
        exact: VALID_OUTPUTS,
        text: r#"{"a":[true],"b":1}"#,
        bare: document.clone(),
        typed: json!({"@value": document, "@type": "@json"}),
        valued: json!({"@value": document, "@type": RDF_JSON}),
    };
    let differing = read_surfaces(r#"{"b":1,"a":[true]}"#, &expected).await;
    assert!(differing.is_empty(), "{differing:#?}");
}

/// An `rdf:JSON` literal that is not JSON, which bulk import keeps as written,
/// reads back on every surface as the typed literal it is, where JSON-LD,
/// typed JSON, agent JSON and the crawl used to fail the whole query.
#[tokio::test]
async fn an_imported_invalid_json_literal_reads_back_as_typed_text() {
    let typed_literal = json!({"@value": "not json", "@type": RDF_JSON});
    let expected = Expected {
        exact: &[],
        text: "not json",
        bare: typed_literal.clone(),
        typed: typed_literal.clone(),
        valued: typed_literal,
    };
    let differing = read_surfaces("not json", &expected).await;
    assert!(differing.is_empty(), "{differing:#?}");
}

/// Every surface's output for a valid JSON literal before invalid ones read
/// back as typed text, byte for byte: that change must not touch it.
const VALID_OUTPUTS: &[(&str, &str)] = &[
    ("select JSON-LD", r#"[[{"a":[true],"b":1}]]"#),
    (
        "select typed JSON",
        r#"[{"?o":{"@value":{"a":[true],"b":1},"@type":"@json"}}]"#,
    ),
    (
        "select SPARQL JSON",
        r#"{"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"literal","value":"{\"a\":[true],\"b\":1}","datatype":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}}]}}"#,
    ),
    (
        "select agent JSON",
        r#"{"schema":{"?o":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"},"rows":[{"?o":{"a":[true],"b":1}}],"rowCount":1,"t":1,"hasMore":false}"#,
    ),
    ("select JSON-LD, streamed", r#"[[{"a":[true],"b":1}]]"#),
    (
        "select typed JSON, streamed",
        r#"[{"?o":{"@value":{"a":[true],"b":1},"@type":"@json"}}]"#,
    ),
    (
        "select SPARQL JSON, streamed",
        r#"{"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"literal","value":"{\"a\":[true],\"b\":1}","datatype":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}}]}}"#,
    ),
    (
        "select SPARQL XML",
        r#"<?xml version="1.0" encoding="UTF-8"?><sparql xmlns="http://www.w3.org/2005/sparql-results#"><head><variable name="o"/></head><results><result><binding name="o"><literal datatype="http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON">{"a":[true],"b":1}</literal></binding></result></results></sparql>"#,
    ),
    (
        "select CSV",
        r#"o
"{""a"":[true],""b"":1}"
"#,
    ),
    (
        "select TSV",
        r#"o
{"a":[true],"b":1}
"#,
    ),
    (
        "select agent JSON, streamed",
        r#"{"schema":{"?o":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"},"rows":[{"?o":{"a":[true],"b":1}}],"rowCount":1,"t":1,"hasMore":false}"#,
    ),
    (
        "crawl JSON-LD",
        r#"[{"@id":"http://example.org/ns/s","http://example.org/ns/doc":{"a":[true],"b":1},"http://example.org/ns/name":"s"}]"#,
    ),
    (
        "crawl JSON-LD, streamed",
        r#"[{"@id":"http://example.org/ns/s","http://example.org/ns/doc":{"a":[true],"b":1},"http://example.org/ns/name":"s"}]"#,
    ),
    (
        "crawl typed JSON",
        r#"[{"@id":"http://example.org/ns/s","http://example.org/ns/doc":{"@value":{"a":[true],"b":1},"@type":"@json"},"http://example.org/ns/name":{"@value":"s","@type":"http://www.w3.org/2001/XMLSchema#string"}}]"#,
    ),
    (
        "crawl typed JSON, streamed",
        r#"[{"@id":"http://example.org/ns/s","http://example.org/ns/doc":{"@value":{"a":[true],"b":1},"@type":"@json"},"http://example.org/ns/name":{"@value":"s","@type":"http://www.w3.org/2001/XMLSchema#string"}}]"#,
    ),
    (
        "crawl of @value and @type JSON-LD",
        r#"[{"http://example.org/ns/doc":{"@value":{"a":[true],"b":1},"@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}}]"#,
    ),
    (
        "crawl of @value and @type JSON-LD, streamed",
        r#"[{"http://example.org/ns/doc":{"@value":{"a":[true],"b":1},"@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}}]"#,
    ),
    (
        "crawl of @value and @type typed JSON",
        r#"[{"http://example.org/ns/doc":{"@value":{"a":[true],"b":1},"@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}}]"#,
    ),
    (
        "crawl of @value and @type typed JSON, streamed",
        r#"[{"http://example.org/ns/doc":{"@value":{"a":[true],"b":1},"@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}}]"#,
    ),
    (
        "CONSTRUCT JSON-LD",
        r#"{"@context":{},"@graph":[{"@id":"http://example.org/ns/s","http://example.org/ns/doc":[{"@value":"{\"a\":[true],\"b\":1}","@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}],"http://example.org/ns/name":["s"]}]}"#,
    ),
    (
        "CONSTRUCT Turtle",
        r#"<http://example.org/ns/s> <http://example.org/ns/doc> "{\"a\":[true],\"b\":1}"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> ;
    <http://example.org/ns/name> "s" .
"#,
    ),
    (
        "CONSTRUCT N-Triples",
        r#"<http://example.org/ns/s> <http://example.org/ns/doc> "{\"a\":[true],\"b\":1}"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> .
<http://example.org/ns/s> <http://example.org/ns/name> "s" .
"#,
    ),
    (
        "export Turtle",
        r#"@prefix ex: <http://example.org/ns/> .

ex:s
    ex:doc "{\"a\":[true],\"b\":1}"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> ;
    ex:name "s" .
"#,
    ),
    (
        "export NTriples",
        r#"<http://example.org/ns/s> <http://example.org/ns/doc> "{\"a\":[true],\"b\":1}"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> .
<http://example.org/ns/s> <http://example.org/ns/name> "s" .
"#,
    ),
    (
        "export JsonLd",
        r#"{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "@graph": [
    {"@id": "ex:s", "ex:doc": {"@value":{"a":[true],"b":1},"@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}, "ex:name": "s"}
  ]
}
"#,
    ),
];
