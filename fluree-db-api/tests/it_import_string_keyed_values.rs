//! Bulk import and `fluree graph import` read back every value kept in the
//! string dictionary as written: strings, language-tagged strings, a custom
//! datatype's text, and also `rdf:JSON` and `xsd:duration` values.
//!
//! An import numbers strings per chunk and remaps them to the ledger's
//! dictionary ids before it builds the index. A value whose id missed the
//! remap reads back as whatever string holds that id in the ledger, which
//! differs from one import to the next. The sources here span several chunks
//! (one per file), and the graph import lands on a ledger that already has a
//! dictionary, so a missed remap cannot land on the right string by chance.

use fluree_db_api::{Fluree, FlureeBuilder, LedgerState, ReindexOptions};
use serde_json::{json, Value};
use std::path::Path;

use crate::support;

const LEDGER: &str = "it/import-string-keyed:main";
const GRAPH: &str = "http://example.org/graph/second";
const EX: &str = "http://example.org/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

fn fluree_at(dir: &Path) -> Fluree {
    FlureeBuilder::file(dir.to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("file fluree")
}

/// One source file: a subject per tag holding a value of each string-keyed
/// kind, with text of its own so each chunk numbers different strings.
/// `n` varies the JSON and duration values. Returns the file and the
/// `(subject, predicate, lexical form, datatype)` rows it should read back.
fn source(tag: &str, n: u32) -> (String, Vec<[String; 4]>) {
    let s = format!("{EX}{tag}");
    let json = format!(r#"{{"k":"{tag}","n":{n}}}"#);
    let duration = format!("P{n}Y2M");
    let day_time = format!("P{n}DT4H");
    let rows = [
        ("name", format!("{tag} name"), format!("{XSD}string")),
        ("label", tag.to_string(), format!("{RDF}langString")),
        ("code", format!("{tag}-code"), format!("{EX}Code")),
        ("doc", json.clone(), format!("{RDF}JSON")),
        ("span", duration.clone(), format!("{XSD}duration")),
        ("wait", day_time.clone(), format!("{XSD}duration")),
    ]
    .into_iter()
    .map(|(p, lex, dt)| [s.clone(), format!("{EX}{p}"), lex, dt])
    .collect();
    let file = format!(
        "<{s}> <{EX}name> \"{tag} name\" ;\n\
         \t<{EX}label> \"{tag}\"@en ;\n\
         \t<{EX}code> \"{tag}-code\"^^<{EX}Code> ;\n\
         \t<{EX}doc> '{json}'^^<{RDF}JSON> ;\n\
         \t<{EX}span> \"{duration}\"^^<{XSD}duration> ;\n\
         \t<{EX}wait> \"{day_time}\"^^<{XSD}duration> .\n"
    );
    (file, rows)
}

/// Write `files` as `<tag>.ttl` into a fresh directory; returns it and the
/// rows they should read back, sorted.
fn source_dir(files: &[(&str, u32)]) -> (tempfile::TempDir, Vec<[String; 4]>) {
    let dir = tempfile::tempdir().unwrap();
    let mut expected = Vec::new();
    for (tag, n) in files {
        let (file, rows) = source(tag, *n);
        std::fs::write(dir.path().join(format!("{tag}.ttl")), file).unwrap();
        expected.extend(rows);
    }
    expected.sort();
    (dir, expected)
}

/// Every literal in `graph` (the default graph when `None`), as
/// `(subject, predicate, lexical form, datatype)`, sorted. The lexical form
/// is read from the formatted value itself (`STR` of a duration is unbound),
/// and a JSON value as its serialization.
async fn literals(fluree: &Fluree, ledger: &LedgerState, graph: Option<&str>) -> Vec<[String; 4]> {
    let pattern = "?s ?p ?o FILTER(isLiteral(?o))";
    let body = match graph {
        Some(g) => format!("GRAPH <{g}> {{ {pattern} }}"),
        None => pattern.to_string(),
    };
    let rows = support::query_sparql_formatted(
        fluree,
        ledger,
        &format!("SELECT ?s ?p ?o (DATATYPE(?o) AS ?dt) WHERE {{ {body} }}"),
    )
    .await
    .expect("query");
    let text = |cell: &Value| match cell {
        Value::String(s) => s.clone(),
        Value::Object(o) if o.contains_key("@value") => match &o["@value"] {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        },
        other => other.to_string(),
    };
    let mut out: Vec<[String; 4]> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| [text(&row[0]), text(&row[1]), text(&row[2]), text(&row[3])])
        .collect();
    out.sort();
    out
}

/// The JSON-LD select of each subject's `rdf:JSON` value in `graph` (the
/// default graph when `None`). It formats the value as JSON, so text that is
/// not the document fails here too.
async fn json_docs(fluree: &Fluree, ledger: &LedgerState, graph: Option<&str>) -> Vec<Value> {
    let from = match graph {
        Some(g) => format!("{LEDGER}#{g}"),
        None => LEDGER.to_string(),
    };
    let query = json!({"from": from, "select": "?d",
                       "where": {"@id": "?s", format!("{EX}doc"): "?d"}});
    let docs = fluree
        .query_connection(&query)
        .await
        .expect("JSON-LD select")
        .to_jsonld(&ledger.snapshot)
        .expect("JSON-LD format");
    let mut docs = docs.as_array().expect("docs").clone();
    docs.sort_by_key(Value::to_string);
    docs
}

fn expected_docs(files: &[(&str, u32)]) -> Vec<Value> {
    let mut docs: Vec<Value> = files
        .iter()
        .map(|(tag, n)| json!({"k": tag, "n": n}))
        .collect();
    docs.sort_by_key(Value::to_string);
    docs
}

/// A bulk import across several chunks, then a graph import into the same
/// ledger, read every string-keyed value back as written, and so does the
/// index a rebuild from the ledger's commits produces.
#[tokio::test]
async fn imports_read_back_json_and_duration_values_as_written() {
    let db = tempfile::tempdir().unwrap();
    let first = [("alpha", 1), ("bravo", 2), ("charlie", 3)];
    let second = [("delta", 4), ("echo", 5)];

    let (src, default_rows) = source_dir(&first);
    fluree_at(db.path())
        .create(LEDGER)
        .import(src.path())
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("bulk import");
    let fluree = fluree_at(db.path());
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    assert_eq!(
        literals(&fluree, &ledger, None).await,
        default_rows,
        "bulk import"
    );
    assert_eq!(
        json_docs(&fluree, &ledger, None).await,
        expected_docs(&first)
    );

    let (src, graph_rows) = source_dir(&second);
    fluree
        .import_into_graph(LEDGER, GRAPH)
        .import(src.path())
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("graph import");
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    assert_eq!(
        literals(&fluree, &ledger, Some(GRAPH)).await,
        graph_rows,
        "graph import"
    );
    assert_eq!(
        json_docs(&fluree, &ledger, Some(GRAPH)).await,
        expected_docs(&second)
    );
    assert_eq!(
        literals(&fluree, &ledger, None).await,
        default_rows,
        "the default graph after the graph import"
    );

    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");
    let fluree = fluree_at(db.path());
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    assert_eq!(
        literals(&fluree, &ledger, None).await,
        default_rows,
        "default graph after a rebuild"
    );
    assert_eq!(
        literals(&fluree, &ledger, Some(GRAPH)).await,
        graph_rows,
        "graph after a rebuild"
    );
}
