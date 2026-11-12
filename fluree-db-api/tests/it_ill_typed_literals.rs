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
use fluree_db_core::FlakeValue;
use fluree_db_transact::GraphSel;
use serde_json::{json, Value};

const EX: &str = "http://example.org/ns/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const PREFIXES: &str = "PREFIX ex: <http://example.org/ns/> \
     PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ";

/// (datatype IRI, its name under `PREFIXES`, an ill-typed lexical form, a
/// valid one).
const CASES: [(&str, &str, &str, &str); 3] = [
    (
        "http://www.w3.org/2001/XMLSchema#integer",
        "xsd:integer",
        "landed",
        "42",
    ),
    (
        "http://www.w3.org/2001/XMLSchema#dateTime",
        "xsd:dateTime",
        "landed",
        "2026-10-28T12:00:00Z",
    ),
    // A string `@value` under `@json` is a serialized document, so the
    // JSON-LD insert parses it as `INSERT DATA` parses `"…"^^rdf:JSON`.
    (
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON",
        "rdf:JSON",
        "not json",
        "[1,2]",
    ),
];

/// The local name of the datatype IRI `dt`, for ledger ids.
fn local(dt: &str) -> &str {
    dt.rsplit(['#', '/']).next().unwrap_or(dt)
}

/// Each write of `"lexical"^^<dt>` to `ex:<subject> ex:p`, as
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
            r#"{PREFIXES} INSERT DATA {{ ex:{subject}-data ex:p "{lexical}"^^<{dt}> }}"#
        ))
        .await,
    ));
    out.push((
        "SPARQL update, STRDT".to_string(),
        sparql(format!(
            r#"{PREFIXES} INSERT {{ ex:{subject}-strdt ex:p ?v }}
               WHERE {{ ex:seed ex:k ?k BIND(STRDT("{lexical}", <{dt}>) AS ?v) }}"#
        ))
        .await,
    ));
    let (head, tail) = lexical.split_at(lexical.len() / 2);
    out.push((
        "SPARQL update, STRDT of CONCAT".to_string(),
        sparql(format!(
            r#"{PREFIXES} INSERT {{ ex:{subject}-concat ex:p ?v }}
               WHERE {{ ex:seed ex:k ?k BIND(STRDT(CONCAT("{head}", "{tail}"), <{dt}>) AS ?v) }}"#
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
                        "ex:p": {"@value": lexical, "@type": dt}}),
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
                                  ["bind", "?v", format!("(str-dt \"{lexical}\" \"{dt}\")")]],
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
                &format!("<{EX}{subject}-ttl> <{EX}p> \"{lexical}\"^^<{dt}> ."),
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
                "<{EX}{subject}-upsert> <{EX}p> \"{lexical}\"^^<{dt}> ."
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
                    "GRAPH <{EX}g-trig> {{ <{EX}{subject}-trig> <{EX}p> \"{lexical}\"^^<{dt}> . }}"
                ),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    ));
    let triple = format!("<{EX}{subject}-graph> <{EX}p> \"{lexical}\"^^<{dt}> .");
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
    for (dt, name, bad, _) in CASES {
        let id = format!("it/ill-typed-refused-{}:main", local(dt));
        seeded(&fluree, &id).await;
        let t = fluree.ledger(&id).await.unwrap().t();
        for (path, outcome) in write_on_every_path(&fluree, &id, "s", dt, bad).await {
            let err = outcome.expect_err(&format!("{name}: {path} refuses \"{bad}\""));
            assert!(
                err.contains(&format!("Cannot parse '{bad}' as")),
                "{name}: {path}: {err}"
            );
        }
        assert_eq!(
            fluree.ledger(&id).await.unwrap().t(),
            t,
            "{name}: nothing committed"
        );
    }
}

/// Bulk import is the one lenient write path: it keeps an ill-typed literal
/// as text under its datatype (`LiteralCheck::Lenient`), and stores the
/// valid literal beside it as its value. Pinned, so a change to it is
/// deliberate.
#[tokio::test]
async fn bulk_import_keeps_an_ill_typed_literal() {
    for (dt, name, bad, good) in CASES {
        let data_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            data_dir.path().join("data.ttl"),
            format!(
                "<{EX}imp> <{EX}ok> \"{good}\"^^<{dt}> .\n\
                 <{EX}imp> <{EX}p> \"{bad}\"^^<{dt}> .\n"
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
            .unwrap_or_else(|e| panic!("{name}: import: {e}"));
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
        assert_eq!(rows, json!([[bad, name]]), "{name}: kept as text");
        let valid = query_sparql_formatted(
            &fluree,
            &ledger,
            &format!(
                r#"{PREFIXES} SELECT (sameTerm(?v, "{good}"^^<{dt}>) AS ?same) WHERE {{ ex:imp ex:ok ?v }}"#
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            valid,
            json!([[true]]),
            "{name}: the valid literal, as its value"
        );
    }
}

/// A valid lexical form is stored as its value on every path, one term
/// whichever path wrote it.
#[tokio::test]
async fn a_valid_typed_literal_is_one_term_on_every_write_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (dt, name, _, good) in CASES {
        let id = format!("it/ill-typed-valid-{}:main", local(dt));
        seeded(&fluree, &id).await;
        let outcomes = write_on_every_path(&fluree, &id, "s", dt, good).await;
        let paths = outcomes.len();
        for (path, outcome) in outcomes {
            outcome.unwrap_or_else(|e| panic!("{name}: {path}: {e}"));
        }
        let ledger = fluree.ledger(&id).await.unwrap();
        let rows = query_sparql_formatted(
            &fluree,
            &ledger,
            &format!(
                r#"{PREFIXES} SELECT (COUNT(?s) AS ?n) (COUNT(DISTINCT ?v) AS ?terms)
                   WHERE {{ {{ ?s ex:p ?v }} UNION {{ GRAPH ?g {{ ?s ex:p ?v }} }}
                           FILTER(sameTerm(?v, "{good}"^^<{dt}>)) }}"#
            ),
        )
        .await
        .unwrap();
        assert_eq!(rows, json!([[paths, 1]]), "{name}: every path, one term");
    }
}

/// An ill-typed literal a ledger already holds reads back with its datatype,
/// in SPARQL and JSON-LD results and in a JSON-LD graph crawl, not as a
/// plain string. An update cannot copy it, and a DELETE still removes it.
#[tokio::test]
async fn a_stored_ill_typed_literal_keeps_its_datatype() {
    let fluree = FlureeBuilder::memory().build_memory();
    // The helper stores text under an `xsd:` datatype.
    for (dt, _, bad, _) in CASES.into_iter().filter(|c| c.0.starts_with(XSD)) {
        let dt = local(dt);
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

/// Each object stored for `subject predicate`, as (the value, its datatype
/// IRI), read from the ledger as stored: novelty or index.
async fn stored_values(
    ledger: &fluree_db_api::LedgerState,
    subject: &str,
    predicate: &str,
) -> Vec<(fluree_db_core::FlakeValue, String)> {
    use fluree_db_core::comparator::IndexType;
    use fluree_db_core::range::{RangeMatch, RangeTest};
    let snapshot = &ledger.snapshot;
    let (Some(s), Some(p)) = (snapshot.encode_iri(subject), snapshot.encode_iri(predicate)) else {
        return Vec::new();
    };
    let flakes = ledger
        .as_graph_db_ref(0)
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch {
                s: Some(s),
                p: Some(p),
                ..Default::default()
            },
        )
        .await
        .expect("range");
    flakes
        .into_iter()
        .filter(|f| f.op)
        .map(|f| {
            let dt = snapshot.decode_sid(&f.dt).unwrap_or_default();
            (f.o, dt)
        })
        .collect()
}

/// A transaction and an import parse a typed literal with one parser. Each
/// valid form below is stored as the same value, of the kind named, whichever
/// of the two wrote it, the transaction into novelty and the import into the
/// index. A form a transaction refuses is the one an import keeps as text
/// (an `rdf:JSON` one as written).
#[tokio::test]
async fn a_transaction_and_an_import_store_one_value_per_literal() {
    const RDF_JSON: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON";
    const WKT: &str = "http://www.opengis.net/ont/geosparql#wktLiteral";
    const VECTOR: &str = "https://ns.flur.ee/db#embeddingVector";
    let xsd = |local: &str| format!("{XSD}{local}");
    // (datatype IRI, lexical form, the kind of value both store).
    let valid: Vec<(String, &str, &str)> = vec![
        (xsd("boolean"), "1", "Boolean"),
        (xsd("decimal"), "1.5", "Decimal"),
        (xsd("double"), "INF", "Double"),
        (xsd("float"), "-1.5E2", "Double"),
        (xsd("integer"), "123456789012345678901234567890", "BigInt"),
        (xsd("byte"), "12", "Long"),
        (xsd("dateTime"), "2026-10-28T12:00:00Z", "DateTime"),
        (xsd("gYear"), "2024", "GYear"),
        (xsd("duration"), "P1Y2M3DT4H", "Duration"),
        (xsd("dayTimeDuration"), "PT36H", "DayTimeDuration"),
        (WKT.to_string(), "POINT(1 2)", "GeoPoint"),
        (RDF_JSON.to_string(), r#"{"b": 1, "a": [true]}"#, "Json"),
        (VECTOR.to_string(), "[0.5, 1.5]", "Vector"),
        (xsd("anyURI"), "http://example.org/x", "String"),
        (format!("{EX}custom"), "anything at all", "String"),
    ];
    // (datatype IRI, a lexical form a transaction refuses, its error, what
    // import keeps).
    let text = |s: &str| FlakeValue::String(s.to_string());
    let refused: Vec<(String, &str, &str, FlakeValue)> = vec![
        (
            xsd("byte"),
            "300",
            "Value 300 is out of range for byte",
            text("300"),
        ),
        (xsd("boolean"), "yes", "Cannot parse 'yes' as", text("yes")),
        (xsd("duration"), "P1Q", "Cannot parse 'P1Q' as", text("P1Q")),
        (
            RDF_JSON.to_string(),
            "not json",
            "Cannot parse 'not json' as",
            FlakeValue::Json("not json".into()),
        ),
    ];
    let triple =
        |i: usize, dt: &str, lexical: &str| format!("<{EX}s> <{EX}p{i}> '{lexical}'^^<{dt}> .\n");

    let db_dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();

    // By transaction: the valid forms in one, each refused form on its own.
    let txn_id = "it/parity-transaction:main";
    let ledger = fluree.create_ledger(txn_id).await.unwrap();
    let doc: String = valid
        .iter()
        .enumerate()
        .map(|(i, (dt, lexical, _))| triple(i, dt, lexical))
        .collect();
    fluree
        .insert_turtle(ledger, &doc)
        .await
        .expect("valid forms");
    for (i, (dt, lexical, error, _)) in refused.iter().enumerate() {
        let ledger = fluree.ledger(txn_id).await.unwrap();
        let err = fluree
            .insert_turtle(ledger, &triple(valid.len() + i, dt, lexical))
            .await
            .map(|_| ())
            .expect_err(&format!("a transaction refuses '{lexical}'^^<{dt}>"));
        assert!(err.to_string().contains(error), "{dt}: {err}");
    }

    // By import: every form in one file.
    let data_dir = tempfile::tempdir().unwrap();
    let doc: String = valid
        .iter()
        .map(|(dt, lexical, _)| (dt, lexical))
        .chain(refused.iter().map(|(dt, lexical, _, _)| (dt, lexical)))
        .enumerate()
        .map(|(i, (dt, lexical))| triple(i, dt, lexical))
        .collect();
    std::fs::write(data_dir.path().join("data.ttl"), doc).unwrap();
    let import_id = "it/parity-import:main";
    fluree
        .create(import_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");

    let by_txn = fluree.ledger(txn_id).await.unwrap();
    let by_import = fluree.ledger(import_id).await.unwrap();
    let subject = format!("{EX}s");
    let mut differing = Vec::new();
    for (i, (dt, lexical, kind)) in valid.iter().enumerate() {
        let predicate = format!("{EX}p{i}");
        let txn = stored_values(&by_txn, &subject, &predicate).await;
        let import = stored_values(&by_import, &subject, &predicate).await;
        let as_kind = txn.len() == 1 && format!("{:?}", txn[0].0).starts_with(&format!("{kind}("));
        if txn != import || !as_kind || txn[0].1 != *dt {
            differing.push(format!(
                "'{lexical}'^^<{dt}> as {kind}: transaction {txn:?}, import {import:?}"
            ));
        }
    }
    for (i, (dt, lexical, _, kept)) in refused.iter().enumerate() {
        let predicate = format!("{EX}p{}", valid.len() + i);
        let txn = stored_values(&by_txn, &subject, &predicate).await;
        let import = stored_values(&by_import, &subject, &predicate).await;
        if !txn.is_empty() || import != vec![(kept.clone(), dt.clone())] {
            differing.push(format!(
                "'{lexical}'^^<{dt}>: transaction {txn:?}, import {import:?}, expected {kept:?}"
            ));
        }
    }
    assert!(differing.is_empty(), "{differing:#?}");
}
