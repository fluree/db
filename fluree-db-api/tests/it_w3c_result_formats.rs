//! Issue #45: the W3C result serializations must emit absolute IRIs.
//!
//! `application/sparql-results+json`, `text/csv` and `text/tab-separated-values`
//! carry no prefix map and no `@base` slot, so a CURIE or a relative reference in
//! one of them is lossy — the consumer cannot recover the term. On main, a query
//! prologue of `PREFIX ex:` produced `ex:s2` in all three, and a `BASE`-only
//! prologue produced the bare relative reference `s2`.
//!
//! These tests drive the real query engine end to end (prologue -> lowered
//! `@context` -> writer), which is what makes them a gate on the whole path
//! rather than on the writer in isolation. The JSON-LD writer is included as the
//! control: it carries an `@context`, so it still compacts (#1466).

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::{
    format, FlureeBuilder, FormatterConfig, IndexConfig, LedgerManagerConfig, QueryInput,
};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::genesis_ledger_for_fluree;

const S2: &str = "http://example.org/s2";

fn seed_data() -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": S2,
             "http://example.org/p": {"@value": "2", "@type": "http://www.w3.org/2001/XMLSchema#integer"}}
        ]
    })
}

async fn seeded_view(ledger_id: &str) -> (fluree_db_api::Fluree, fluree_db_api::GraphDb) {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
    fluree
        .insert_with_opts(
            ledger,
            &seed_data(),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_cfg,
        )
        .await
        .expect("insert");
    let view = fluree.db(ledger_id).await.expect("view");
    (fluree, view)
}

/// The four prologue forms that reach the writer as four different lowered
/// contexts. `BASE` is the worst case: it lowers to `@base`, which shortens
/// `@id`-position IRIs to a bare relative reference.
const PROLOGUES: [(&str, &str); 4] = [
    (
        "PREFIX ex:",
        "PREFIX ex: <http://example.org/> SELECT ?s ?o WHERE { ?s ex:p ?o }",
    ),
    (
        "PREFIX :",
        "PREFIX : <http://example.org/> SELECT ?s ?o WHERE { ?s :p ?o }",
    ),
    (
        "no prologue",
        "SELECT ?s ?o WHERE { ?s <http://example.org/p> ?o }",
    ),
    (
        "BASE only",
        "BASE <http://example.org/> SELECT ?s ?o WHERE { ?s <http://example.org/p> ?o }",
    ),
];

#[tokio::test]
async fn w3c_result_formats_emit_absolute_iris_under_every_prologue() {
    let (fluree, view) = seeded_view("w3cfmt/iris:main").await;

    for (label, query) in PROLOGUES {
        let result = fluree
            .query(&view, QueryInput::Sparql(query))
            .await
            .unwrap_or_else(|e| panic!("{label}: query failed: {e}"));

        // SRJ through the DOM path...
        let srj = format::format_results(
            &result,
            &result.context,
            &view.snapshot,
            &FormatterConfig::sparql_json(),
        )
        .unwrap_or_else(|e| panic!("{label}: sparql_json: {e}"));
        assert_eq!(
            srj["results"]["bindings"][0]["s"],
            json!({"type": "uri", "value": S2}),
            "{label}: SRJ (DOM) must carry the absolute IRI"
        );

        // ...and through the streaming path the server actually uses. These are
        // separate writers; a fix applied to only one ships half-broken.
        let srj_stream = format::format_results_string(
            &result,
            &result.context,
            &view.snapshot,
            &FormatterConfig::sparql_json(),
        )
        .unwrap_or_else(|e| panic!("{label}: sparql_json string: {e}"));
        assert_eq!(
            srj_stream,
            serde_json::to_string(&srj).unwrap(),
            "{label}: streaming SRJ diverged from the DOM"
        );

        // SPARQL Results XML was already correct; it stays correct.
        let srx = format::format_results_string(
            &result,
            &result.context,
            &view.snapshot,
            &FormatterConfig::sparql_xml(),
        )
        .unwrap_or_else(|e| panic!("{label}: sparql_xml: {e}"));
        assert!(
            srx.contains(&format!("<uri>{S2}</uri>")),
            "{label}: SRX must carry the absolute IRI: {srx}"
        );

        // CSV and TSV carry the identical defect on main.
        let csv = result.to_csv(&view.snapshot).expect("csv");
        let tsv = result.to_tsv(&view.snapshot).expect("tsv");
        for (name, text) in [("CSV", &csv), ("TSV", &tsv)] {
            assert!(
                text.contains(S2),
                "{label}: {name} must carry the absolute IRI: {text}"
            );
        }

        // Control: the JSON-LD writer ships the `@context` that expands a CURIE,
        // so it still compacts wherever the prologue gives it one. This is what
        // keeps the assertions above measuring the W3C profile rather than a
        // context that never reached the formatter.
        let jsonld = result.to_jsonld(&view.snapshot).expect("jsonld");
        let jsonld_s = serde_json::to_string(&jsonld).unwrap();
        let expected_compact = match label {
            "PREFIX ex:" => Some("ex:s2"),
            "PREFIX :" => Some(":s2"),
            "BASE only" => Some("s2"),
            _ => None, // no prologue: nothing to compact against
        };
        match expected_compact {
            Some(compact) => assert!(
                jsonld_s.contains(&format!("\"{compact}\"")),
                "{label}: JSON-LD should still compact to {compact}: {jsonld_s}"
            ),
            None => assert!(
                jsonld_s.contains(S2),
                "{label}: JSON-LD without a prologue keeps the full IRI: {jsonld_s}"
            ),
        }
    }
}

/// Issue #45 (b). `is_inferable_datatype` drops the `datatype` tag off any
/// string-backed literal whose type is on its allow-list. Stored integers escape
/// it only because ingest lands them as `FlakeValue::Long`; `STRDT` builds a
/// string-backed literal with an "inferable" type at query time, and on main the
/// tag was dropped — so `STRDT("2", xsd:integer)` serialized as a *different* RDF
/// term (a simple literal, i.e. `xsd:string`).
#[tokio::test]
async fn w3c_sparql_json_keeps_datatype_on_constructed_literals() {
    let (fluree, view) = seeded_view("w3cfmt/strdt:main").await;

    // The first three were dropped on main; the last two are the control group
    // (never on the allow-list, so they were always emitted).
    for dt in [
        "http://www.w3.org/2001/XMLSchema#integer",
        "http://www.w3.org/2001/XMLSchema#boolean",
        "http://www.w3.org/2001/XMLSchema#decimal",
        "http://www.w3.org/2001/XMLSchema#float",
        "http://www.w3.org/2001/XMLSchema#date",
    ] {
        let lex = if dt.ends_with("boolean") {
            "true"
        } else if dt.ends_with("date") {
            "2020-01-01"
        } else {
            "2"
        };
        let q = format!(
            "SELECT (STRDT(\"{lex}\", <{dt}>) AS ?v) WHERE {{ ?s <http://example.org/p> ?o }}"
        );
        let result = fluree
            .query(&view, QueryInput::Sparql(&q))
            .await
            .unwrap_or_else(|e| panic!("STRDT({lex}, {dt}): {e}"));
        let srj = format::format_results(
            &result,
            &result.context,
            &view.snapshot,
            &FormatterConfig::sparql_json(),
        )
        .expect("sparql_json");
        assert_eq!(
            srj["results"]["bindings"][0]["v"],
            json!({"type": "literal", "value": lex, "datatype": dt}),
            "STRDT({lex}, {dt}) must round-trip its datatype"
        );
    }

    // A plain string literal stays bare: no `datatype` and no `xml:lang` IS
    // `xsd:string` per SRJ §3.2.2, so emitting the tag would be noise, not a fix.
    let result = fluree
        .query(
            &view,
            QueryInput::Sparql("SELECT (\"plain\" AS ?v) WHERE { ?s <http://example.org/p> ?o }"),
        )
        .await
        .expect("plain literal query");
    let srj = format::format_results(
        &result,
        &result.context,
        &view.snapshot,
        &FormatterConfig::sparql_json(),
    )
    .expect("sparql_json");
    assert_eq!(
        srj["results"]["bindings"][0]["v"],
        json!({"type": "literal", "value": "plain"}),
        "a simple literal stays bare"
    );
}
