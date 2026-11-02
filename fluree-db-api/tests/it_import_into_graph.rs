//! Bulk import into a new named graph of an existing ledger.
//!
//! A ledger is built by transactions and indexed, then a Turtle source that
//! shares subjects, strings, predicates, a class and a language tag with it
//! is imported into a graph it does not have. The graph must read as the
//! same source imported into a fresh ledger reads, and the ledger must read
//! the same from the root the import spliced, after a full rebuild from its
//! commits, and after later writes indexed incrementally over that root.

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use serde_json::{json, Value};
use std::path::Path;

use crate::support;

const LEDGER: &str = "it/graph-import:main";
const EX: &str = "http://example.org/";
const GRAPH: &str = "http://example.org/graph/products";
const DB: &str = "https://ns.flur.ee/db#";

fn fluree_at(dir: &Path) -> Fluree {
    FlureeBuilder::file(dir.to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("file fluree")
}

const SOURCE_A: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:widget1 a ex:Product ;
    ex:name "Widget" ;
    ex:maker ex:alice ;
    ex:label "Alice" ;
    ex:price "12345678901234567890.5"^^xsd:decimal ;
    ex:tag "neu"@de , "new"@en .
ex:widget2 a ex:Product ;
    ex:name "Gadget" ;
    ex:parts ( ex:widget1 ex:bolt ) .
ex:alice ex:likes ex:widget2 .
"#;

const SOURCE_B: &str = r#"
@prefix ex: <http://example.org/> .
ex:bolt ex:name "Bolt" ;
    ex:supplier [ ex:name "Acme" ] .
ex:carol a ex:Person .
<http://other.example/thing/1> ex:name "Thing" .
"#;

/// `graph s p o` for every statement in the default graph and in `GRAPH`,
/// sorted, IRIs under `ex:` written `ex:` and blank nodes `_:`.
async fn statements(fluree: &Fluree) -> Vec<String> {
    let ledger = fluree.ledger(LEDGER).await.expect("load");
    let mut out = Vec::new();
    for (label, from) in [
        ("default", LEDGER.to_string()),
        ("g", format!("{LEDGER}#{GRAPH}")),
    ] {
        let query = json!({
            "from": from,
            "select": ["?s", "?p", "?o"],
            "where": {"@id": "?s", "?p": "?o"}
        });
        let rows = match fluree.query_connection(&query).await {
            Ok(rows) => rows.to_jsonld(&ledger.snapshot).expect("jsonld"),
            Err(e) if e.to_string().contains("not found") => continue,
            Err(e) => panic!("{label}: {e}"),
        };
        for row in rows.as_array().expect("rows") {
            out.push(format!("{label}: {}", row_text(row)));
        }
    }
    out.sort();
    out
}

fn row_text(row: &Value) -> String {
    row.as_array()
        .expect("row")
        .iter()
        .map(|cell| {
            let cell = match cell {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            if cell.starts_with("_:") {
                "_:".to_string()
            } else {
                cell.replace(EX, "ex:")
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn source_dir(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (name, body) in files {
        std::fs::write(dir.path().join(name), body).unwrap();
    }
    dir
}

/// A ledger with two commits of default-graph data, indexed to its head.
async fn indexed_ledger(dir: &Path) -> i64 {
    let fluree = fluree_at(dir);
    let ledger = fluree.create_ledger(LEDGER).await.unwrap();
    let ctx = json!({"ex": EX});
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx, "@graph": [
                {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice",
                 "ex:note": {"@value": "hello", "@language": "en"}},
                {"@id": "ex:bob", "ex:name": "Bob"}
            ]}),
        )
        .await
        .unwrap()
        .ledger;
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx, "@id": "ex:carol", "ex:name": "Carol"}),
        )
        .await
        .unwrap()
        .ledger;
    support::rebuild_and_publish_index(&fluree, LEDGER).await;
    ledger.t()
}

async fn import_into_graph(
    fluree: &Fluree,
    files: &[(&str, &str)],
    graph: &str,
) -> Result<fluree_db_api::ImportResult, String> {
    let source = source_dir(files);
    fluree
        .import_into_graph(LEDGER, graph)
        .import(source.path())
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .map_err(|e| e.to_string())
}

/// The statements `files` hold, imported into the default graph of a fresh
/// ledger, under the label the graph import's are read under.
async fn reference(files: &[(&str, &str)]) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let source = source_dir(files);
    fluree_at(dir.path())
        .create(LEDGER)
        .import(source.path())
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("reference import");
    statements(&fluree_at(dir.path()))
        .await
        .into_iter()
        .map(|s| s.replacen("default: ", "g: ", 1))
        .collect()
}

#[tokio::test]
async fn import_fills_a_new_graph_of_an_indexed_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let base_t = indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let before = statements(&fluree).await;
    assert!(before.iter().all(|s| s.starts_with("default: ")));

    // A cached handle is held and swapped by the import, not left behind.
    let handle = fluree.ledger_cached(LEDGER).await.expect("cached handle");

    let files = [("a.ttl", SOURCE_A), ("b.ttl", SOURCE_B)];
    let result = import_into_graph(&fluree, &files, GRAPH)
        .await
        .expect("graph import");
    let graph = result.graph.as_ref().expect("an import into a graph");
    assert_eq!(graph.iri, GRAPH);
    assert_eq!(graph.g_id, 3, "the first user graph");
    assert_eq!(graph.base_t, base_t);
    assert_eq!(result.t, base_t + 2, "one commit per source file");
    assert_eq!(result.index_t, result.t);
    assert_eq!(handle.t().await, result.t);
    assert_eq!(handle.index_t().await, result.t);

    let after = statements(&fluree_at(dir.path())).await;
    let (default_after, imported): (Vec<String>, Vec<String>) =
        after.into_iter().partition(|s| s.starts_with("default: "));
    assert_eq!(default_after, before, "the default graph is untouched");
    assert_eq!(imported, reference(&files).await);
    for fact in [
        "g: ex:widget1 ex:maker ex:alice",
        "g: ex:alice ex:likes ex:widget2",
        "g: ex:widget1 ex:label Alice",
        "g: ex:carol @type ex:Person",
        "g: http://other.example/thing/1 ex:name Thing",
    ] {
        let fact = fact.replace("@type", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        assert!(imported.contains(&fact), "missing {fact}: {imported:#?}");
    }

    // The graph's class stats name a class the import introduced and one
    // the ledger already had.
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    let stats = ledger.snapshot.stats.as_ref().expect("stats");
    let graph_stats = stats
        .graphs
        .as_ref()
        .and_then(|graphs| graphs.iter().find(|g| g.g_id == 3))
        .expect("the graph's stats");
    let classes: Vec<(String, u64)> = graph_stats
        .classes
        .iter()
        .flatten()
        .map(|c| (c.class_sid.name.to_string(), c.count))
        .collect();
    assert!(classes.contains(&("Product".to_string(), 2)), "{classes:?}");
    assert!(classes.contains(&("Person".to_string(), 1)), "{classes:?}");

    // txn-meta holds every commit, the import's chained onto the head it
    // started from.
    let ts = fluree
        .query_connection(&json!({
            "from": format!("{LEDGER}#txn-meta"),
            "select": "?t",
            "where": {"@id": "?c", format!("{DB}t"): "?t"}
        }))
        .await
        .expect("txn-meta")
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let mut ts: Vec<i64> = ts
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t.as_i64().unwrap())
        .collect();
    ts.sort_unstable();
    assert_eq!(ts, (1..=result.t).collect::<Vec<_>>());
    let previous = fluree
        .query_connection(&json!({
            "from": format!("{LEDGER}#txn-meta"),
            "select": "?pt",
            "where": [
                {"@id": "?c", format!("{DB}t"): base_t + 1, format!("{DB}previous"): "?p"},
                {"@id": "?p", format!("{DB}t"): "?pt"}
            ]
        }))
        .await
        .expect("txn-meta chain")
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(previous, json!([base_t]));

    assert_shares_the_ledgers_terms(&fluree, base_t).await;

    // The commits alone rebuild the same ledger.
    let spliced = statements(&fluree).await;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("full rebuild");
    let fluree = fluree_at(dir.path());
    assert_eq!(statements(&fluree).await, spliced);
    assert_shares_the_ledgers_terms(&fluree, base_t).await;
}

async fn sparql_cells(fluree: &Fluree, body: &str) -> Vec<String> {
    let query =
        format!("PREFIX ex: <{EX}> SELECT * FROM <{LEDGER}> FROM NAMED <{LEDGER}#{GRAPH}> {body}");
    let rows = fluree
        .query_from()
        .sparql(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut cells: Vec<String> = support::normalize_sparql_bindings(&rows)
        .iter()
        .map(|row| {
            row.as_object()
                .expect("binding")
                .values()
                .map(|v| v["value"].as_str().unwrap_or_default().replace(EX, "ex:"))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    cells.sort();
    cells
}

/// A subject or string the ledger held is the same term in the new graph:
/// a bound lookup finds it in both graphs, and a join crosses them. Were it
/// numbered anew, one id would be the dictionary's and the other unreachable.
async fn assert_shares_the_ledgers_terms(fluree: &Fluree, base_t: i64) {
    assert_eq!(
        sparql_cells(
            fluree,
            &format!(
                "WHERE {{ GRAPH <{LEDGER}#{GRAPH}> {{ ex:widget1 ex:maker ?m }} ?m ex:name ?n }}"
            )
        )
        .await,
        ["ex:alice Alice"]
    );
    assert_eq!(
        sparql_cells(
            fluree,
            &format!("WHERE {{ GRAPH <{LEDGER}#{GRAPH}> {{ ex:alice ex:likes ?w }} }}")
        )
        .await,
        ["ex:widget2"]
    );
    // `ex:carol` keeps its old id but sorts after the new `ex:bolt` in its
    // chunk: the graph's SPOT order holds only if the chunk was renumbered.
    assert_eq!(
        sparql_cells(
            fluree,
            &format!("WHERE {{ GRAPH <{LEDGER}#{GRAPH}> {{ ex:carol a ?c }} }}")
        )
        .await,
        ["ex:Person"]
    );
    assert_eq!(
        sparql_cells(
            fluree,
            &format!("WHERE {{ GRAPH <{LEDGER}#{GRAPH}> {{ ex:bolt ex:name ?n }} }}")
        )
        .await,
        ["Bolt"]
    );
    assert_eq!(
        sparql_cells(fluree, "WHERE { ?p ex:name \"Alice\" }").await,
        ["ex:alice"]
    );
    assert_eq!(
        sparql_cells(
            fluree,
            &format!("WHERE {{ GRAPH <{LEDGER}#{GRAPH}> {{ ?w ex:label \"Alice\" }} }}")
        )
        .await,
        ["ex:widget1"]
    );
    // The first file's commit holds that file's statements only.
    let first = format!("{LEDGER}@t:{}#{GRAPH}", base_t + 1);
    let named = fluree
        .query_from()
        .sparql(&format!(
            "PREFIX ex: <{EX}> SELECT ?s FROM NAMED <{first}> \
             WHERE {{ GRAPH <{first}> {{ ?s ex:name ?n }} }}"
        ))
        .execute_formatted()
        .await
        .expect("time travel");
    let mut subjects: Vec<String> = support::normalize_sparql_bindings(&named)
        .iter()
        .map(|row| row["s"]["value"].as_str().unwrap().replace(EX, "ex:"))
        .collect();
    subjects.sort();
    assert_eq!(subjects, ["ex:widget1", "ex:widget2"]);
}

#[tokio::test]
async fn later_writes_index_incrementally_over_the_spliced_root() {
    let dir = tempfile::tempdir().unwrap();
    indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    // On a spawned task, as a server's import job runs it: the import's
    // future must be `Send` for every lifetime it borrows.
    let spawned = fluree.clone();
    let result =
        tokio::spawn(
            async move { import_into_graph(&spawned, &[("a.ttl", SOURCE_A)], GRAPH).await },
        )
        .await
        .unwrap()
        .expect("graph import");

    let ledger = fluree.ledger(LEDGER).await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@context": {"ex": EX}, "@id": "ex:widget1", "ex:name": "Widget Pro"}),
        )
        .await
        .unwrap();
    support::build_incrementally_and_publish_index(&fluree, LEDGER).await;

    let ledger = fluree.ledger(LEDGER).await.unwrap();
    assert_eq!(ledger.index_t(), result.t + 1);
    let incremental = statements(&fluree).await;
    assert!(incremental.contains(&"default: ex:widget1 ex:name Widget Pro".to_string()));
    assert!(incremental.contains(&"g: ex:widget1 ex:name Widget".to_string()));

    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("full rebuild");
    assert_eq!(statements(&fluree_at(dir.path())).await, incremental);
}

/// A refused import publishes nothing: the ledger stays at its head.
async fn assert_refused(fluree: &Fluree, files: &[(&str, &str)], graph: &str, says: &str) {
    let before = fluree.ledger(LEDGER).await.unwrap().t();
    let err = import_into_graph(fluree, files, graph)
        .await
        .expect_err("the import is refused");
    assert!(err.contains(says), "{err}");
    assert_eq!(fluree.ledger(LEDGER).await.unwrap().t(), before);
}

#[tokio::test]
async fn an_import_needs_a_new_graph_and_default_graph_data() {
    let dir = tempfile::tempdir().unwrap();
    indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    import_into_graph(&fluree, &[("a.ttl", SOURCE_A)], GRAPH)
        .await
        .expect("graph import");

    assert_refused(
        &fluree,
        &[("b.ttl", SOURCE_B)],
        GRAPH,
        "already has the graph",
    )
    .await;
    assert_refused(
        &fluree,
        &[(
            "a.trig",
            "<http://example.org/g> { <http://example.org/a> <http://example.org/p> 1 . }",
        )],
        "http://example.org/graph/other",
        "TriG",
    )
    .await;
    let named = json!({"@context": {"ex": EX}, "@id": "ex:g2",
        "@graph": [{"@id": "ex:a", "ex:q": "v"}]})
    .to_string();
    assert_refused(
        &fluree,
        &[("a.jsonld", named.as_str())],
        "http://example.org/graph/other",
        "names a graph of its own",
    )
    .await;
    assert_refused(
        &fluree,
        &[("b.ttl", SOURCE_B)],
        "urn:default",
        "names the default graph of",
    )
    .await;
}

/// Without a background indexer, the import indexes the commits after the
/// ledger's index itself before it splices the graph in.
#[tokio::test]
async fn an_unindexed_ledger_is_indexed_before_the_import() {
    let dir = tempfile::tempdir().unwrap();
    let base_t = indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@context": {"ex": EX}, "@id": "ex:dave", "ex:name": "Dave"}),
        )
        .await
        .unwrap();

    let result = import_into_graph(&fluree, &[("a.ttl", SOURCE_A)], GRAPH)
        .await
        .expect("graph import");
    assert_eq!(result.graph.as_ref().map(|g| g.base_t), Some(base_t + 1));
    assert_eq!(result.index_t, result.t);

    let fresh = fluree_at(dir.path());
    let ledger = fresh.ledger(LEDGER).await.unwrap();
    assert_eq!(ledger.index_t(), result.t);
    let read = statements(&fresh).await;
    assert!(
        read.contains(&"default: ex:dave ex:name Dave".to_string()),
        "{read:?}"
    );
    assert!(
        read.contains(&"g: ex:widget1 ex:name Widget".to_string()),
        "{read:?}"
    );
}

const CLAIMS: &str = "@prefix ex: <http://example.org/> .
ex:alice ex:knows ex:bob {| ex:confidence 0.9 |} .
";

const MORE_CLAIMS: &str = "@prefix ex: <http://example.org/> .
ex:erin ex:knows ex:frank {| ex:confidence 0.5 |} .
";

/// `(subject, object, confidence)` for every annotated `ex:knows` edge in
/// the default graph, or in `graph`.
async fn claims(fluree: &Fluree, graph: Option<&str>) -> Vec<String> {
    let pattern = "?s ex:knows ?o {| ex:confidence ?conf |}";
    let (from, body) = match graph {
        Some(g) => (
            format!("FROM NAMED <{LEDGER}#{g}>"),
            format!("GRAPH <{LEDGER}#{g}> {{ {pattern} }}"),
        ),
        None => (String::new(), pattern.to_string()),
    };
    let rows = fluree
        .query_from()
        .sparql(&format!(
            "PREFIX ex: <{EX}> SELECT ?s ?o ?conf FROM <{LEDGER}> {from} WHERE {{ {body} }}"
        ))
        .execute_formatted()
        .await
        .expect("annotation query");
    let mut out: Vec<String> = support::normalize_sparql_bindings(&rows)
        .iter()
        .map(|row| {
            ["s", "o", "conf"]
                .map(|v| row[v]["value"].as_str().unwrap().replace(EX, "ex:"))
                .join(" ")
        })
        .collect();
    out.sort();
    out
}

/// The ledger's annotation arena keeps covering every annotation: kept when
/// the graph adds none, dropped (annotations read by scan, correctly, until
/// a reindex seals it again) when it does.
#[tokio::test]
async fn the_annotation_arena_survives_only_an_unannotated_graph() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = fluree_at(dir.path());
    let source = source_dir(&[("claims.ttl", CLAIMS)]);
    fluree
        .create(LEDGER)
        .import(source.path())
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("annotated ledger");
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("seal the arena");
    let fluree = fluree_at(dir.path());
    let arena = |fluree: Fluree| async move {
        fluree
            .ledger(LEDGER)
            .await
            .unwrap()
            .snapshot
            .annotation_index
            .is_some()
    };
    assert!(
        arena(fluree_at(dir.path())).await,
        "the reindex sealed an arena"
    );
    let base_claims = vec!["ex:alice ex:bob 0.9".to_string()];
    assert_eq!(claims(&fluree, None).await, base_claims);

    let plain = import_into_graph(&fluree, &[("a.ttl", SOURCE_A)], GRAPH)
        .await
        .expect("plain graph import");
    assert!(!plain.has_annotations);
    assert!(
        arena(fluree_at(dir.path())).await,
        "an unannotated graph keeps it"
    );
    assert_eq!(claims(&fluree_at(dir.path()), None).await, base_claims);

    let annotated_graph = "http://example.org/graph/claims";
    let annotated = import_into_graph(&fluree, &[("more.ttl", MORE_CLAIMS)], annotated_graph)
        .await
        .expect("annotated graph import");
    assert!(annotated.has_annotations);
    // Export and JSON-LD hydration read a sealed arena as complete.
    assert!(
        !arena(fluree_at(dir.path())).await,
        "an arena without the graph's annotations would hide them"
    );
    let fluree = fluree_at(dir.path());
    assert_eq!(claims(&fluree, None).await, base_claims);
    assert_eq!(
        claims(&fluree, Some(annotated_graph)).await,
        ["ex:erin ex:frank 0.5"]
    );
}
