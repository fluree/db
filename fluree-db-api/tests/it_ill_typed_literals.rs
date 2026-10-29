//! Ill-typed literals (`"landed"^^xsd:integer`) on every write path.
//!
//! A literal whose lexical form is not in its datatype's lexical space is
//! refused by every transaction, with the error the JSON-LD insert gives:
//! SPARQL `INSERT DATA`, the JSON-LD insert, an update that builds the
//! literal at run time (`STRDT`, JSON-LD `str-dt`) or copies a stored one,
//! and the RDF-text verbs (Turtle insert and upsert, the TriG insert
//! fallback, graph insert and graph sync). Bulk import is the one lenient
//! path: it keeps the literal as text under its datatype. A valid form is one
//! term whichever path wrote it. A ledger may hold an ill-typed literal; it
//! reads back with its datatype and can be deleted.

use crate::support::{
    self, ledger_with_stored_ill_typed_literals, query_jsonld_formatted, query_sparql_formatted,
};
use fluree_db_api::{FlureeBuilder, GraphPayload};
use fluree_db_transact::GraphSel;
use serde_json::{json, Value};

const EX: &str = "http://example.org/ns/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const PREFIXES: &str =
    "PREFIX ex: <http://example.org/ns/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

/// (datatype local name, an ill-typed lexical form, a valid one).
const CASES: [(&str, &str, &str); 2] = [
    ("integer", "landed", "42"),
    ("dateTime", "landed", "2026-10-28T12:00:00Z"),
];

/// Each write of `"lexical"^^xsd:<dt>` to `ex:<subject> ex:p`, as
/// (path, the write's outcome as `Ok(())` or the error text).
async fn write_on_every_path(
    fluree: &support::MemoryFluree,
    id: &str,
    subject: &str,
    dt: &str,
    lexical: &str,
) -> Vec<(String, Result<(), String>)> {
    let ctx = json!({"ex": EX, "xsd": XSD});
    let sparql = |q: String| async move {
        fluree
            .graph(id)
            .transact()
            .sparql_update(&q)
            .commit()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    let mut out = Vec::new();
    out.push((
        "SPARQL INSERT DATA".to_string(),
        sparql(format!(
            r#"{PREFIXES} INSERT DATA {{ ex:{subject}-data ex:p "{lexical}"^^xsd:{dt} }}"#
        ))
        .await,
    ));
    out.push((
        "SPARQL update, STRDT".to_string(),
        sparql(format!(
            r#"{PREFIXES} INSERT {{ ex:{subject}-strdt ex:p ?v }}
               WHERE {{ ex:seed ex:k ?k BIND(STRDT("{lexical}", xsd:{dt}) AS ?v) }}"#
        ))
        .await,
    ));
    let (head, tail) = lexical.split_at(lexical.len() / 2);
    out.push((
        "SPARQL update, STRDT of CONCAT".to_string(),
        sparql(format!(
            r#"{PREFIXES} INSERT {{ ex:{subject}-concat ex:p ?v }}
               WHERE {{ ex:seed ex:k ?k BIND(STRDT(CONCAT("{head}", "{tail}"), xsd:{dt}) AS ?v) }}"#
        ))
        .await,
    ));
    let ledger = fluree.ledger(id).await.unwrap();
    out.push((
        "JSON-LD insert".to_string(),
        fluree
            .insert(
                ledger,
                &json!({"@context": ctx, "@id": format!("ex:{subject}-insert"),
                        "ex:p": {"@value": lexical, "@type": format!("xsd:{dt}")}}),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    ));
    let ledger = fluree.ledger(id).await.unwrap();
    out.push((
        "JSON-LD update, str-dt".to_string(),
        fluree
            .update(
                ledger,
                &json!({"@context": ctx,
                        "where": [{"@id": "ex:seed", "ex:k": "?k"},
                                  ["bind", "?v", format!("(str-dt \"{lexical}\" \"{XSD}{dt}\")")]],
                        "insert": {"@id": format!("ex:{subject}-strdt-jsonld"), "ex:p": "?v"}}),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    ));
    let ledger = fluree.ledger(id).await.unwrap();
    out.push((
        "Turtle insert".to_string(),
        fluree
            .insert_turtle(
                ledger,
                &format!("<{EX}{subject}-ttl> <{EX}p> \"{lexical}\"^^<{XSD}{dt}> ."),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    ));
    out.push((
        "Turtle upsert".to_string(),
        fluree
            .graph(id)
            .transact()
            .upsert_turtle(&format!(
                "<{EX}{subject}-upsert> <{EX}p> \"{lexical}\"^^<{XSD}{dt}> ."
            ))
            .commit()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    ));
    let ledger = fluree.ledger(id).await.unwrap();
    out.push((
        "TriG insert".to_string(),
        fluree
            .insert_turtle(
                ledger,
                &format!(
                    "GRAPH <{EX}g-trig> {{ <{EX}{subject}-trig> <{EX}p> \"{lexical}\"^^<{XSD}{dt}> . }}"
                ),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    ));
    let triple = format!("<{EX}{subject}-graph> <{EX}p> \"{lexical}\"^^<{XSD}{dt}> .");
    out.push((
        "graph insert".to_string(),
        fluree
            .stage_owned(fluree.ledger(id).await.unwrap())
            .insert_graph_payload(
                GraphSel::Graph(format!("{EX}g-insert")),
                GraphPayload::Rdf(&triple),
            )
            .execute()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    ));
    out.push((
        "graph sync".to_string(),
        fluree
            .stage_owned(fluree.ledger(id).await.unwrap())
            .sync_graph_payload(
                GraphSel::Graph(format!("{EX}g-sync")),
                GraphPayload::Rdf(&triple),
                false,
            )
            .execute()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    ));
    out
}

async fn seeded(fluree: &support::MemoryFluree, id: &str) {
    fluree.create_ledger(id).await.expect("create");
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!(r#"{PREFIXES} INSERT DATA {{ ex:seed ex:k "k" }}"#))
        .commit()
        .await
        .expect("seed");
}

/// Every transaction refuses an ill-typed literal with the insert's error,
/// and commits nothing.
#[tokio::test]
async fn every_transaction_refuses_an_ill_typed_literal() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (dt, bad, _) in CASES {
        let id = format!("it/ill-typed-refused-{dt}:main");
        seeded(&fluree, &id).await;
        let t = fluree.ledger(&id).await.unwrap().t();
        for (path, outcome) in write_on_every_path(&fluree, &id, "s", dt, bad).await {
            let err = outcome.expect_err(&format!("{dt}: {path} refuses \"{bad}\""));
            assert!(
                err.contains(&format!("Cannot parse '{bad}' as")),
                "{dt}: {path}: {err}"
            );
        }
        assert_eq!(
            fluree.ledger(&id).await.unwrap().t(),
            t,
            "{dt}: nothing committed"
        );
    }
}

/// Bulk import is the one lenient write path: it keeps an ill-typed literal
/// as text under its datatype (`LiteralCheck::Lenient`), and stores the
/// valid literal beside it as its value. Pinned, so a change to it is
/// deliberate.
#[tokio::test]
async fn bulk_import_keeps_an_ill_typed_literal() {
    for (dt, bad, good) in CASES {
        let data_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            data_dir.path().join("data.ttl"),
            format!(
                "<{EX}imp> <{EX}ok> \"{good}\"^^<{XSD}{dt}> .\n\
                 <{EX}imp> <{EX}p> \"{bad}\"^^<{XSD}{dt}> .\n"
            ),
        )
        .unwrap();
        let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .build()
            .unwrap();
        let id = "it/ill-typed-import:main";
        fluree
            .create(id)
            .import(data_dir.path())
            .threads(1)
            .memory_budget_mb(256)
            .cleanup(false)
            .execute()
            .await
            .unwrap_or_else(|e| panic!("{dt}: import: {e}"));
        let ledger = fluree.ledger(id).await.unwrap();
        let rows = query_sparql_formatted(
            &fluree,
            &ledger,
            &format!(
                "{PREFIXES} SELECT (STR(?v) AS ?lex) (DATATYPE(?v) AS ?dt) WHERE {{ ex:imp ex:p ?v }}"
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            rows,
            json!([[bad, format!("xsd:{dt}")]]),
            "{dt}: kept as text"
        );
        let valid = query_sparql_formatted(
            &fluree,
            &ledger,
            &format!(
                r#"{PREFIXES} SELECT (sameTerm(?v, "{good}"^^xsd:{dt}) AS ?same) WHERE {{ ex:imp ex:ok ?v }}"#
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            valid,
            json!([[true]]),
            "{dt}: the valid literal, as its value"
        );
    }
}

/// A valid lexical form is stored as its value on every path, one term
/// whichever path wrote it.
#[tokio::test]
async fn a_valid_typed_literal_is_one_term_on_every_write_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (dt, _, good) in CASES {
        let id = format!("it/ill-typed-valid-{dt}:main");
        seeded(&fluree, &id).await;
        let outcomes = write_on_every_path(&fluree, &id, "s", dt, good).await;
        let paths = outcomes.len();
        for (path, outcome) in outcomes {
            outcome.unwrap_or_else(|e| panic!("{dt}: {path}: {e}"));
        }
        let ledger = fluree.ledger(&id).await.unwrap();
        let rows = query_sparql_formatted(
            &fluree,
            &ledger,
            &format!(
                r#"{PREFIXES} SELECT (COUNT(?s) AS ?n) (COUNT(DISTINCT ?v) AS ?terms)
                   WHERE {{ {{ ?s ex:p ?v }} UNION {{ GRAPH ?g {{ ?s ex:p ?v }} }}
                           FILTER(sameTerm(?v, "{good}"^^xsd:{dt})) }}"#
            ),
        )
        .await
        .unwrap();
        assert_eq!(rows, json!([[paths, 1]]), "{dt}: every path, one term");
    }
}

/// An ill-typed literal a ledger already holds reads back with its datatype,
/// in SPARQL and JSON-LD results and in a JSON-LD graph crawl, not as a
/// plain string. An update cannot copy it, and a DELETE still removes it.
#[tokio::test]
async fn a_stored_ill_typed_literal_keeps_its_datatype() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (dt, bad, _) in CASES {
        let id = format!("it/ill-typed-stored-{dt}:main");
        let ledger = ledger_with_stored_ill_typed_literals(
            &fluree,
            &id,
            "",
            &[(&format!("{EX}s"), &format!("{EX}p"), bad, dt)],
        )
        .await;
        let typed = json!({"@value": bad, "@type": format!("{XSD}{dt}")});

        let rows = query_sparql_formatted(
            &fluree,
            &ledger,
            &format!(
                "{PREFIXES} SELECT ?v (STR(?v) AS ?lex) (DATATYPE(?v) AS ?dt) WHERE {{ ex:s ex:p ?v }}"
            ),
        )
        .await
        .unwrap();
        // The query's `PREFIX xsd:` compacts the datatype.
        let compact = json!({"@value": bad, "@type": format!("xsd:{dt}")});
        assert_eq!(
            rows,
            json!([[compact, bad, format!("xsd:{dt}")]]),
            "{dt}: SPARQL"
        );
        let select = query_jsonld_formatted(
            &fluree,
            &ledger,
            &json!({"@context": {"ex": EX}, "select": "?v", "where": {"@id": "ex:s", "ex:p": "?v"}}),
        )
        .await
        .unwrap();
        assert_eq!(select, json!([typed.clone()]), "{dt}: JSON-LD select");
        let crawl = query_jsonld_formatted(
            &fluree,
            &ledger,
            &json!({"@context": {"ex": EX}, "select": {"ex:s": ["ex:p"]}}),
        )
        .await
        .unwrap();
        let crawled: &Value = &crawl[0]["ex:p"];
        assert_eq!(crawled, &typed, "{dt}: JSON-LD graph crawl: {crawl}");

        let copy = fluree
            .graph(&id)
            .transact()
            .sparql_update(&format!(
                "{PREFIXES} INSERT {{ ex:copy ex:p ?v }} WHERE {{ ex:s ex:p ?v }}"
            ))
            .commit()
            .await
            .expect_err(&format!("{dt}: an update cannot copy it"));
        assert!(
            copy.to_string()
                .contains(&format!("Cannot parse '{bad}' as")),
            "{dt}: {copy}"
        );

        fluree
            .graph(&id)
            .transact()
            .sparql_update(&format!(
                "{PREFIXES} DELETE {{ ex:s ex:p ?v }} WHERE {{ ex:s ex:p ?v }}"
            ))
            .commit()
            .await
            .unwrap_or_else(|e| panic!("{dt}: delete: {e}"));
        let ledger = fluree.ledger(&id).await.unwrap();
        let left = query_sparql_formatted(
            &fluree,
            &ledger,
            &format!("{PREFIXES} SELECT ?v WHERE {{ ex:s ex:p ?v }}"),
        )
        .await
        .unwrap();
        assert_eq!(left, json!([]), "{dt}: deleted");
    }
}
