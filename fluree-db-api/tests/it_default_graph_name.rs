//! `urn:default`, the name ledger info gives the default graph, outside a
//! query's dataset: in an update and in a `GRAPH` pattern. Every position that
//! reads a graph or chooses a default graph reads it as the default graph, and
//! a write that names a graph by it is refused, since the default graph is no
//! named graph and one by that name would be hidden from every such read.

use crate::support::{self, genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

const G1: &str = "http://example.org/g1";
const REFUSAL: &str = "<urn:default> names the default graph";

/// `"default"` in the default graph, `"g1"` in `G1`.
async fn seed(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .
        ex:a ex:v "default" .
        GRAPH <{G1}> {{ ex:b ex:v "g1" . }}
        "#
    );
    fluree
        .stage_owned(genesis_ledger(fluree, ledger_id))
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed")
        .ledger
}

/// The `?o` values `query` selects, sorted.
async fn values(fluree: &MemoryFluree, ledger: &MemoryLedger, query: &str) -> Vec<String> {
    let rows = support::query_sparql(fluree, ledger, query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");
    let mut out: Vec<String> = rows
        .as_array()
        .unwrap_or_else(|| panic!("rows: {rows}"))
        .iter()
        .map(|row| {
            let v = row.as_array().and_then(|r| r.first()).unwrap_or(row);
            v.as_str().map_or_else(|| v.to_string(), str::to_string)
        })
        .collect();
    out.sort();
    out
}

/// The `ex:v` values of the default graph and of `G1`.
async fn graphs(fluree: &MemoryFluree, ledger: &MemoryLedger) -> (Vec<String>, Vec<String>) {
    let default = values(
        fluree,
        ledger,
        "PREFIX ex: <http://example.org/> SELECT ?o WHERE { ?s ex:v ?o }",
    )
    .await;
    let g1 = values(
        fluree,
        ledger,
        &format!(
            "PREFIX ex: <http://example.org/> SELECT ?o WHERE {{ GRAPH <{G1}> {{ ?s ex:v ?o }} }}"
        ),
    )
    .await;
    (default, g1)
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_string()).collect()
}

enum Update {
    Sparql(String),
    JsonLd(JsonValue),
}

async fn run(
    fluree: &MemoryFluree,
    ledger: MemoryLedger,
    update: &Update,
) -> Result<MemoryLedger, String> {
    match update {
        Update::Sparql(sparql) => {
            let parsed = fluree_db_sparql::parse_sparql(sparql);
            assert!(!parsed.has_errors(), "{sparql}: {:?}", parsed.diagnostics);
            let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
            let txn = fluree_db_transact::lower_sparql_update_ast(
                &parsed.ast.expect("SPARQL AST"),
                &mut ns,
                fluree_db_transact::TxnOpts::default(),
            )
            .map_err(|e| e.to_string())?;
            fluree.stage_owned(ledger).txn(txn).execute().await
        }
        Update::JsonLd(json) => fluree.stage_owned(ledger).update(json).execute().await,
    }
    .map(|r| r.ledger)
    .map_err(|e| e.to_string())
}

/// `USING`, `USING NAMED`, `WITH`, `GRAPH` in the WHERE, and the JSON-LD
/// `graph` / `from` / `["graph", …]` twins read `urn:default` as the default
/// graph, and `WITH` writes it.
#[tokio::test]
async fn update_positions_read_urn_default_as_the_default_graph() {
    let ex = "PREFIX ex: <http://example.org/> ";
    let v = json!({"@id": "?s", "ex:v": "?o"});
    let ctx = json!({"ex": "http://example.org/"});
    let cases: Vec<(&str, Update, &[&str])> = vec![
        (
            "USING",
            Update::Sparql(format!(
                "{ex}DELETE {{ ?s ex:v ?o }} USING <urn:default> WHERE {{ ?s ex:v ?o }}"
            )),
            &[],
        ),
        (
            "WITH, read",
            Update::Sparql(format!(
                "{ex}WITH <urn:default> DELETE {{ ?s ex:v ?o }} WHERE {{ ?s ex:v ?o }}"
            )),
            &[],
        ),
        (
            "WITH, write",
            Update::Sparql(format!(
                r#"{ex}WITH <urn:default> INSERT {{ ex:z ex:v "zed" }} WHERE {{ }}"#
            )),
            &["default", "zed"],
        ),
        (
            "GRAPH in the WHERE",
            Update::Sparql(format!(
                "{ex}DELETE {{ ?s ex:v ?o }} WHERE {{ GRAPH <urn:default> {{ ?s ex:v ?o }} }}"
            )),
            &[],
        ),
        (
            "USING NAMED",
            Update::Sparql(format!(
                "{ex}DELETE {{ ?s ex:v ?o }} USING NAMED <urn:default> \
                 WHERE {{ GRAPH <urn:default> {{ ?s ex:v ?o }} }}"
            )),
            &[],
        ),
        (
            "JSON-LD graph",
            Update::JsonLd(
                json!({"@context": ctx, "graph": "urn:default", "where": v, "delete": v}),
            ),
            &[],
        ),
        (
            "JSON-LD from",
            Update::JsonLd(
                json!({"@context": ctx, "from": "urn:default", "where": v, "delete": v}),
            ),
            &[],
        ),
        (
            "JSON-LD fromNamed",
            Update::JsonLd(json!({
                "@context": ctx,
                "fromNamed": ["urn:default"],
                "where": [["graph", "urn:default", v]],
                "delete": v
            })),
            &[],
        ),
        (
            "JSON-LD [\"graph\", …] in the where",
            Update::JsonLd(json!({
                "@context": ctx,
                "where": [["graph", "urn:default", v]],
                "delete": v
            })),
            &[],
        ),
    ];

    let fluree = FlureeBuilder::memory().build_memory();
    let mut failures = Vec::new();
    for (i, (name, update, default_after)) in cases.iter().enumerate() {
        let ledger = seed(&fluree, &format!("it/urn-default-read-{i}:main")).await;
        match run(&fluree, ledger, update).await {
            Ok(ledger) => {
                let after = graphs(&fluree, &ledger).await;
                let expected = (strs(default_after), strs(&["g1"]));
                if after != expected {
                    failures.push(format!("{name}: got {after:?}, expected {expected:?}"));
                }
            }
            Err(e) => failures.push(format!("{name}: {e}")),
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// A write that names a graph `urn:default` is refused, in each form that
/// names a write graph, and leaves the ledger as it was.
#[tokio::test]
async fn writes_naming_urn_default_as_a_graph_are_refused() {
    let ex = "PREFIX ex: <http://example.org/> ";
    let updates: Vec<(&str, Update)> = vec![
        (
            "INSERT DATA quad",
            Update::Sparql(format!(
                r#"{ex}INSERT DATA {{ GRAPH <urn:default> {{ ex:z ex:v "zed" }} }}"#
            )),
        ),
        (
            "GRAPH template",
            Update::Sparql(format!(
                r#"{ex}INSERT {{ GRAPH <urn:default> {{ ex:z ex:v "zed" }} }} WHERE {{ }}"#
            )),
        ),
        (
            "GRAPH ?g template",
            Update::Sparql(format!(
                r#"{ex}INSERT {{ GRAPH ?g {{ ex:z ex:v "zed" }} }} WHERE {{ VALUES ?g {{ <urn:default> }} }}"#
            )),
        ),
        (
            "CREATE GRAPH",
            Update::Sparql("CREATE GRAPH <urn:default>".to_string()),
        ),
        (
            "CLEAR GRAPH",
            Update::Sparql("CLEAR GRAPH <urn:default>".to_string()),
        ),
        (
            "COPY destination",
            Update::Sparql(format!("COPY GRAPH <{G1}> TO GRAPH <urn:default>")),
        ),
        (
            "ADD source",
            Update::Sparql(format!("ADD GRAPH <urn:default> TO GRAPH <{G1}>")),
        ),
        (
            "JSON-LD [\"graph\", …] template",
            Update::JsonLd(json!({
                "@context": {"ex": "http://example.org/"},
                "where": {"@id": "?s", "ex:v": "?o"},
                "delete": [["graph", "urn:default", {"@id": "?s", "ex:v": "?o"}]]
            })),
        ),
        (
            "JSON-LD @graph",
            Update::JsonLd(json!({
                "@context": {"ex": "http://example.org/"},
                "insert": {"@id": "ex:z", "@graph": "urn:default", "ex:v": "zed"}
            })),
        ),
    ];

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "it/urn-default-write:main").await;
    let mut failures = Vec::new();
    for (name, update) in &updates {
        match run(&fluree, ledger.clone(), update).await {
            Ok(_) => failures.push(format!("{name}: committed")),
            Err(e) if !e.contains(REFUSAL) => failures.push(format!("{name}: {e}")),
            Err(_) => {}
        }
    }

    let trig = r#"@prefix ex: <http://example.org/> .
                  GRAPH <urn:default> { ex:z ex:v "zed" . }"#;
    match fluree
        .stage_owned(ledger.clone())
        .upsert_turtle(trig)
        .execute()
        .await
    {
        Ok(_) => failures.push("TriG block: committed".to_string()),
        Err(e) if !e.to_string().contains(REFUSAL) => {
            failures.push(format!("TriG block: {e}"));
        }
        Err(_) => {}
    }

    let data = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:z", "ex:v": "zed"});
    match fluree
        .stage_owned(ledger.clone())
        .sync_graph("urn:default", &data)
        .execute()
        .await
    {
        Ok(_) => failures.push("sync target: committed".to_string()),
        Err(e) if !e.to_string().contains(REFUSAL) => {
            failures.push(format!("sync target: {e}"));
        }
        Err(_) => {}
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    assert_eq!(
        graphs(&fluree, &ledger).await,
        (strs(&["default"]), strs(&["g1"]))
    );
}

/// A bulk import refuses a `GRAPH <urn:default>` block, as staged writes do.
#[tokio::test]
async fn bulk_import_refuses_a_urn_default_block() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let path = data_dir.path().join("data.trig");
    std::fs::write(
        &path,
        "@prefix ex: <http://example.org/> .\nGRAPH <urn:default> { ex:z ex:v \"zed\" . }\n",
    )
    .unwrap();

    let err = fluree
        .create("it/urn-default-import:main")
        .import(&path)
        .execute()
        .await
        .map(|_| ())
        .expect_err("import of a urn:default block must be refused")
        .to_string();
    assert!(err.contains(REFUSAL), "{err}");
}

/// In a query, `GRAPH <urn:default>` reads the default graph, as `GRAPH`
/// naming the ledger does, with the union default graph on or off; `GRAPH ?g`
/// never binds it.
#[tokio::test]
async fn graph_pattern_reads_urn_default_as_the_default_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "it/urn-default-query:main").await;
    let ex = "PREFIX ex: <http://example.org/> ";

    let graph_default = format!("{ex}SELECT ?o WHERE {{ GRAPH <urn:default> {{ ?s ex:v ?o }} }}");
    assert_eq!(
        values(&fluree, &ledger, &graph_default).await,
        strs(&["default"])
    );
    let union = format!("# PRAGMA union-default-graph: true\n{graph_default}");
    assert_eq!(values(&fluree, &ledger, &union).await, strs(&["default"]));

    let graph_var = format!("{ex}SELECT ?o WHERE {{ GRAPH ?g {{ ?s ex:v ?o }} }}");
    assert_eq!(values(&fluree, &ledger, &graph_var).await, strs(&["g1"]));

    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?o"],
        "where": [["graph", "urn:default", {"@id": "?s", "ex:v": "?o"}]]
    });
    let rows = support::query_jsonld(&fluree, &ledger, &jsonld)
        .await
        .expect("JSON-LD query")
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");
    assert_eq!(rows, json!([["default"]]), "{rows}");
}
