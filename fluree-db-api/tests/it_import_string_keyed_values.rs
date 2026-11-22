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
    let fluree = fluree_at(db.path());
    fluree
        .create(LEDGER)
        .import(src.path())
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("bulk import");
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
    // Reopened, so the rebuilt index is read from disk.
    drop(fluree);
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

/// Bulk-imported IDs are lexicographic, while compressed reverse keys may
/// reorder them. Both prefix-count fast paths and range-pushed scans must
/// keep the answers for short prefixes, verified long prefixes, and UTF-8
/// split by the key scheme's 48-byte boundary.
#[tokio::test]
async fn imported_prefix_queries_keep_hash_reordered_ids_and_long_prefix_checks() {
    let db = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let hashed_group = format!("Co{}", "x".repeat(46));
    let raw_group = format!("a{}", "r".repeat(47));
    let unicode_group = format!("{}é", "x".repeat(47));
    let suffix = " title text".repeat(10);
    let mut values: Vec<String> = ["Cat", "Co", "Cow", "alpha", "élan", "😀 graph"]
        .into_iter()
        .map(str::to_string)
        .collect();
    values.extend((0..16).map(|i| format!("{hashed_group}{i:02}{suffix}")));
    // More than the hash cap: this group uses value keys in the same tree.
    values.extend((0..40).map(|i| format!("{raw_group}{i:02}{suffix}")));
    values.extend((0..2).map(|i| format!("{unicode_group}{i}{suffix}")));
    // Same first 48 bytes, a different second byte of the UTF-8 character.
    values.push(format!("{}ê{suffix}", "x".repeat(47)));
    // A short raw key alongside the hashed keys of its own group.
    values.push(format!("{hashed_group}short"));
    // More facts than dictionary IDs: the query must count rows, not IDs.
    values.push(values[6].clone());
    values.push(values[6].clone());
    let mut ttl = String::new();
    for (i, value) in values.iter().enumerate() {
        let lang = if i % 5 == 0 { "@en" } else { "" };
        ttl.push_str(&format!("<{EX}s{i}> <{EX}label> \"{value}\"{lang} .\n"));
    }
    ttl.push_str(&format!(
        "<{EX}other> <{EX}other> \"Co unrelated value\" .\n"
    ));
    std::fs::write(src.path().join("prefix.ttl"), ttl).unwrap();
    let fluree = fluree_at(db.path());
    fluree
        .create(LEDGER)
        .import(src.path())
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("import");
    let view = fluree.db(LEDGER).await.unwrap();
    let store = view.binary_store().expect("imported index");
    assert!(store.lex_sorted_string_ids());
    let rules = store.string_key_rules().unwrap();
    assert!(rules.raw.get(hashed_group.as_bytes()).is_none());
    assert!(rules.raw.get(raw_group.as_bytes()).is_some());
    let ids = store.find_strings_by_prefix(&hashed_group).unwrap();
    assert!(ids.windows(2).any(|w| w[0] > w[1]), "hash-reordered IDs");

    for prefix in [
        "C".to_string(),
        "Co".to_string(),
        "a".to_string(),
        hashed_group.clone(),
        format!("{hashed_group}03"),
        format!("{raw_group}12"),
        unicode_group,
        "é".to_string(),
        "😀".to_string(),
        "no matching prefix".to_string(),
    ] {
        let expected = values.iter().filter(|v| v.starts_with(&prefix)).count();
        let pattern = format!("?s <{EX}label> ?o");
        for body in [
            format!("SELECT (COUNT(*) AS ?n) WHERE {{ {pattern} FILTER REGEX(?o, \"^{prefix}\") }}"),
            format!("SELECT (COUNT(*) AS ?n) WHERE {{ {pattern} FILTER STRSTARTS(?o, \"{prefix}\") }}"),
            format!("SELECT (SUM(<{XSD}integer>(STRSTARTS(?o, \"{prefix}\"))) AS ?n) WHERE {{ {pattern} }}"),
        ] {
            let result = fluree.query(&view, body.as_str()).await.expect("prefix count");
            assert_eq!(
                result.to_jsonld(&view.snapshot).unwrap(),
                json!([[expected]]),
                "{body}"
            );
        }
        let query = format!("SELECT ?s WHERE {{ {pattern} FILTER STRSTARTS(?o, \"{prefix}\") }}");
        let result = fluree
            .query(&view, query.as_str())
            .await
            .expect("prefix scan");
        assert_eq!(result.row_count(), expected, "{query}");
    }
}
