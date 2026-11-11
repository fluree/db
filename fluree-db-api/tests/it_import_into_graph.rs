//! Bulk import into a new named graph of an existing ledger.
//!
//! A ledger is built by transactions and indexed, then a Turtle source that
//! shares subjects, strings, predicates, a class and a language tag with it
//! is imported into a graph it does not have. The graph must read as the
//! same source imported into a fresh ledger reads, and the ledger must read
//! the same from the root the import spliced, after a full rebuild from its
//! commits, and after later writes indexed incrementally over that root.

use fluree_db_api::{
    ApiError, Fluree, FlureeBuilder, ImportError, LedgerId, NameServiceMode, NotifyResult,
    NsNotify, ReindexOptions,
};
use fluree_db_core::MemoryStorage;
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::race_nameservice::PausingNameService;
use crate::support;

const LEDGER: &str = "it/graph-import:main";
const EX: &str = "http://example.org/";
const GRAPH: &str = "http://example.org/graph/products";
const DB: &str = "https://ns.flur.ee/db#";

fn fluree_at(dir: &Path) -> Fluree {
    FlureeBuilder::file(dir.to_string_lossy().to_string())
        .without_indexing()
        .without_storage_lock()
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
/// sorted, IRIs under `ex:` written `ex:` and blank nodes `_:`. A ledger
/// without `GRAPH` reads only the default graph; any other error fails.
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
            Err(ApiError::GraphNotFound(_)) if label == "g" => continue,
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
    seed_ledger(&fluree_at(dir)).await
}

/// [`indexed_ledger`]'s ledger, created through `fluree`.
async fn seed_ledger(fluree: &Fluree) -> i64 {
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
    support::rebuild_and_publish_index(fluree, LEDGER).await;
    ledger.t()
}

async fn import_into_graph(
    fluree: &Fluree,
    files: &[(&str, &str)],
    graph: &str,
) -> Result<fluree_db_api::ImportResult, String> {
    import_into(fluree, LEDGER, files, graph).await
}

async fn import_into(
    fluree: &Fluree,
    ledger: &str,
    files: &[(&str, &str)],
    graph: &str,
) -> Result<fluree_db_api::ImportResult, String> {
    let source = source_dir(files);
    fluree
        .import_into_graph(ledger, graph)
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
    sparql_rows(fluree, "*", body).await
}

/// [`sparql_cells`] for the variables `projection` names, in that order.
async fn sparql_rows(fluree: &Fluree, projection: &str, body: &str) -> Vec<String> {
    let query = format!(
        "PREFIX ex: <{EX}> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
         SELECT {projection} FROM <{LEDGER}> FROM NAMED <{LEDGER}#{GRAPH}> {body}"
    );
    let vars: Vec<&str> = projection
        .split_whitespace()
        .filter_map(|v| v.strip_prefix('?'))
        .collect();
    let rows = fluree
        .query_from()
        .sparql(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut cells: Vec<String> = support::normalize_sparql_bindings(&rows)
        .iter()
        .map(|row| {
            let row = row.as_object().expect("binding");
            let cell = |v: &Value| v["value"].as_str().unwrap_or_default().replace(EX, "ex:");
            match vars.as_slice() {
                [] => row.values().map(cell).collect::<Vec<_>>().join(" "),
                vars => vars
                    .iter()
                    .map(|v| row.get(*v).map(cell).unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join(" "),
            }
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

/// While an import runs, queries read the head it started from and a write
/// waits, then lands on top of the import.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_go_on_and_writes_wait_while_an_import_runs() {
    let dir = tempfile::tempdir().unwrap();
    let base_t = indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let handle = fluree.ledger_cached(LEDGER).await.unwrap();

    let (parked, release) = handle.gate_next_writer_exclusion_for_test();
    let importer = fluree.clone();
    let import =
        tokio::spawn(
            async move { import_into_graph(&importer, &[("a.ttl", SOURCE_A)], GRAPH).await },
        );
    parked.await.expect("the import parks holding the ledger");

    let read = tokio::time::timeout(std::time::Duration::from_secs(10), handle.snapshot())
        .await
        .expect("a query is not held up by the import");
    assert_eq!(read.to_ledger_state().t(), base_t);

    let (writer, writer_handle) = (fluree.clone(), handle.clone());
    let write = tokio::spawn(async move {
        writer
            .stage(&writer_handle)
            .sparql_update(
                "PREFIX ex: <http://example.org/> INSERT DATA { ex:dave ex:name \"Dave\" }",
            )
            .execute()
            .await
            .map(|r| r.receipt.t)
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!write.is_finished(), "a write waits for the import");

    release.send(true).unwrap();
    let imported = import.await.unwrap().expect("graph import");
    let written = write.await.unwrap().expect("the write after the import");
    assert_eq!(written, imported.t + 1);
    let read = statements(&fluree_at(dir.path())).await;
    assert!(
        read.contains(&"default: ex:dave ex:name Dave".to_string()),
        "{read:?}"
    );
    assert!(
        read.contains(&"g: ex:widget1 ex:name Widget".to_string()),
        "{read:?}"
    );
}

/// A branch forked from an indexed ledger starts on its parent's index root,
/// whose txn-meta and config graphs are named for the parent. The roots its
/// imports publish name them for the branch, so one import loads another's.
#[tokio::test]
async fn a_branch_takes_one_import_after_another() {
    const BRANCH: &str = "it/graph-import:dev";
    let dir = tempfile::tempdir().unwrap();
    indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    fluree
        .create_branch("it/graph-import", "dev", None, None)
        .await
        .unwrap();

    import_into(&fluree, BRANCH, &[("a.ttl", SOURCE_A)], GRAPH)
        .await
        .expect("first import");
    let other = "http://example.org/graph/parts";
    import_into(&fluree, BRANCH, &[("b.ttl", SOURCE_B)], other)
        .await
        .expect("second import");

    let fresh = fluree_at(dir.path());
    let ledger = fresh.ledger(BRANCH).await.unwrap();
    let registry = &ledger.snapshot.graph_registry;
    assert_eq!(
        (registry.iri_for_graph_id(1), registry.iri_for_graph_id(2)),
        (
            Some(fluree_db_core::txn_meta_graph_iri(BRANCH).as_str()),
            Some(fluree_db_core::config_graph_iri(BRANCH).as_str())
        )
    );
    for (graph, subject) in [(GRAPH, "widget1"), (other, "bolt")] {
        let query = json!({
            "from": format!("{BRANCH}#{graph}"),
            "select": "?name",
            "where": {"@id": format!("{EX}{subject}"), format!("{EX}name"): "?name"}
        });
        let rows = fresh
            .query_connection(&query)
            .await
            .expect("query the branch")
            .to_jsonld(&ledger.snapshot)
            .expect("jsonld");
        assert_eq!(rows.as_array().map(Vec::len), Some(1), "{graph}: {rows}");
    }
    let main = fresh.ledger(LEDGER).await.unwrap();
    assert_eq!(main.snapshot.graph_registry.assigned_graph_id(GRAPH), None);
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

/// A graph the ledger dropped is imported into afresh under its id: it
/// holds only the new import, from the spliced root, after a restart and
/// after a full rebuild from the commits.
#[tokio::test]
async fn an_import_refills_a_dropped_graph() {
    let dir = tempfile::tempdir().unwrap();
    indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let first = import_into_graph(&fluree, &[("a.ttl", SOURCE_A)], GRAPH)
        .await
        .expect("first import");
    fluree
        .drop_named_graph(LEDGER, GRAPH)
        .await
        .expect("drop the graph");

    let g_id = first.graph.as_ref().expect("the graph").g_id;
    // SOURCE_A's decimal has an arena; SOURCE_B has nothing to put in one.
    let arenas = |root: fluree_db_binary_index::format::index_root::IndexRoot| {
        root.graph_arenas.iter().filter(|a| a.g_id == g_id).count()
    };
    assert_eq!(arenas(published_root(&fluree).await), 1);

    let second = import_into_graph(&fluree, &[("b.ttl", SOURCE_B)], GRAPH)
        .await
        .expect("import into the dropped graph");
    assert_eq!(
        second.graph.as_ref().map(|g| g.g_id),
        Some(g_id),
        "the graph keeps its id"
    );
    assert_eq!(
        arenas(published_root(&fluree).await),
        0,
        "nothing of the dropped graph's arenas is left"
    );

    let expected = reference(&[("b.ttl", SOURCE_B)]).await;
    let refilled = |all: Vec<String>| -> Vec<String> {
        all.into_iter().filter(|s| s.starts_with("g: ")).collect()
    };
    assert_eq!(refilled(statements(&fluree).await), expected);
    let reopened = fluree_at(dir.path());
    assert_eq!(refilled(statements(&reopened).await), expected, "reopened");
    reopened
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("full rebuild");
    assert_eq!(
        refilled(statements(&fluree_at(dir.path())).await),
        expected,
        "after a full rebuild"
    );
}

/// The ledger's published index root.
async fn published_root(fluree: &Fluree) -> fluree_db_binary_index::format::index_root::IndexRoot {
    let record = fluree
        .nameservice()
        .lookup(LEDGER)
        .await
        .unwrap()
        .expect("ledger record");
    let bytes = fluree
        .backend()
        .content_store(&record.storage_namespace())
        .get(record.index_head_id.as_ref().expect("an index"))
        .await
        .unwrap();
    fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes).unwrap()
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

/// [`CLAIMS`]'s edge annotated again, and an edge the ledger never reified.
const GRAPH_CLAIMS: &str = "@prefix ex: <http://example.org/> .
ex:alice ex:knows ex:bob {| ex:confidence 0.7 |} .
ex:carol ex:knows ex:dave {| ex:confidence 0.1 |} .
";

/// Triple terms as values: [`CLAIMS`]'s edge, a term nesting it, and one
/// with a language-tagged object.
const GRAPH_TERMS: &str = "@prefix ex: <http://example.org/> .
ex:doc ex:mentions <<( ex:alice ex:knows ex:bob )>> .
ex:doc ex:cites <<( ex:erin ex:says <<( ex:alice ex:knows ex:bob )>> )>> .
ex:doc ex:quotes <<( ex:erin ex:says \"chat\"@fr )>> .
";

/// A ledger whose default graph holds [`CLAIMS`], bulk-imported and indexed.
async fn annotated_ledger(dir: &Path) {
    let source = source_dir(&[("claims.ttl", CLAIMS)]);
    fluree_at(dir)
        .create(LEDGER)
        .import(source.path())
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("annotated ledger");
}

/// Distinct terms in the published index's triple-term dictionary.
async fn term_count(fluree: &Fluree) -> u64 {
    let record = fluree
        .nameservice()
        .lookup(LEDGER)
        .await
        .unwrap()
        .expect("ledger");
    let bytes = fluree
        .content_store(&record.storage_namespace())
        .get(&record.index_head_id.expect("index"))
        .await
        .unwrap();
    fluree_db_binary_index::IndexRoot::decode(&bytes)
        .unwrap()
        .term_dict
        .map_or(0, |terms| terms.term_count)
}

/// The live links on `ex:knows` edges the ledger's stats count.
async fn knows_links(fluree: &Fluree) -> Option<u64> {
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    let links = ledger.snapshot.stats.as_ref()?.links.clone()?;
    Some(
        links
            .iter()
            .filter(|l| l.sid.1 == "knows")
            .map(|l| l.count)
            .sum(),
    )
}

/// A graph's annotations import with it. An edge the ledger already reifies
/// keeps its term, so a join on the term crosses the graphs; an edge new to
/// the ledger adds one. The same holds from the root the import spliced and
/// from a rebuild of the ledger's commits.
#[tokio::test]
async fn annotations_import_into_a_graph_and_keep_the_ledgers_terms() {
    let dir = tempfile::tempdir().unwrap();
    annotated_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let base_claims = vec!["ex:alice ex:bob 0.9".to_string()];
    assert_eq!(claims(&fluree, None).await, base_claims);
    assert_eq!(term_count(&fluree).await, 1);

    let files = [("claims.ttl", GRAPH_CLAIMS)];
    let result = import_into_graph(&fluree, &files, GRAPH)
        .await
        .expect("annotated graph import");
    assert!(result.has_annotations);
    let imported = |statements: Vec<String>| -> Vec<String> {
        statements
            .into_iter()
            .filter(|s| s.starts_with("g: "))
            .collect()
    };
    let reference = reference(&files).await;

    for label in ["spliced", "rebuilt"] {
        if label == "rebuilt" {
            fluree_at(dir.path())
                .reindex(LEDGER, ReindexOptions::default())
                .await
                .expect("full rebuild");
        }
        let fluree = fluree_at(dir.path());
        assert_eq!(term_count(&fluree).await, 2, "[{label}] one new term");
        assert_eq!(
            knows_links(&fluree).await,
            Some(3),
            "[{label}] the ledger's link and the graph's two"
        );
        assert_eq!(claims(&fluree, None).await, base_claims, "[{label}]");
        assert_eq!(
            claims(&fluree, Some(GRAPH)).await,
            ["ex:alice ex:bob 0.7", "ex:carol ex:dave 0.1"],
            "[{label}]"
        );
        assert_eq!(imported(statements(&fluree).await), reference, "[{label}]");
        assert_eq!(
            sparql_rows(
                &fluree,
                "?c1 ?c2",
                &format!(
                    "WHERE {{ ?r1 rdf:reifies ?t ; ex:confidence ?c1 . \
                     GRAPH <{LEDGER}#{GRAPH}> {{ ?r2 rdf:reifies ?t ; ex:confidence ?c2 }} }}"
                ),
            )
            .await,
            ["0.9 0.7"],
            "[{label}] the shared edge is one term"
        );
        assert_eq!(
            sparql_rows(
                &fluree,
                "?c",
                &format!(
                    "WHERE {{ {{ ?r rdf:reifies <<( ex:alice ex:knows ex:bob )>> ; \
                     ex:confidence ?c }} UNION {{ GRAPH <{LEDGER}#{GRAPH}> {{ ?r rdf:reifies \
                     <<( ex:alice ex:knows ex:bob )>> ; ex:confidence ?c }} }} }}"
                ),
            )
            .await,
            ["0.7", "0.9"],
            "[{label}] a constant term finds both graphs' links"
        );
    }
}

/// Triple terms import as values into a graph, nested ones and one the
/// ledger already holds included, and read as they do after a fresh import.
#[tokio::test]
async fn triple_term_values_import_into_a_graph() {
    let dir = tempfile::tempdir().unwrap();
    annotated_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let files = [("terms.ttl", GRAPH_TERMS)];
    import_into_graph(&fluree, &files, GRAPH)
        .await
        .expect("graph import");
    let reference = reference(&files).await;

    for label in ["spliced", "rebuilt"] {
        if label == "rebuilt" {
            fluree_at(dir.path())
                .reindex(LEDGER, ReindexOptions::default())
                .await
                .expect("full rebuild");
        }
        let fluree = fluree_at(dir.path());
        // The ledger's term, the nesting one, and the tagged one.
        assert_eq!(term_count(&fluree).await, 3, "[{label}]");
        let imported: Vec<String> = statements(&fluree)
            .await
            .into_iter()
            .filter(|s| s.starts_with("g: "))
            .collect();
        assert_eq!(imported, reference, "[{label}]");
        let graph = |pattern: &str| format!("WHERE {{ GRAPH <{LEDGER}#{GRAPH}> {{ {pattern} }} }}");
        assert_eq!(
            sparql_rows(
                &fluree,
                "?d",
                &graph("?d ex:mentions <<( ex:alice ex:knows ex:bob )>>")
            )
            .await,
            ["ex:doc"],
            "[{label}] a constant term matches"
        );
        assert_eq!(
            sparql_rows(
                &fluree,
                "?who ?o",
                &graph("ex:doc ex:cites <<( ?who ex:says <<( ex:alice ex:knows ?o )>> )>>")
            )
            .await,
            ["ex:erin ex:bob"],
            "[{label}] a nested term"
        );
        assert_eq!(
            sparql_rows(
                &fluree,
                "?o",
                &graph("ex:doc ex:quotes ?t BIND(OBJECT(?t) AS ?o) FILTER(LANG(?o) = \"fr\")")
            )
            .await,
            ["chat"],
            "[{label}] the term keeps its tag"
        );
        assert_eq!(
            sparql_rows(
                &fluree,
                "?c",
                &format!(
                    "WHERE {{ ?r rdf:reifies ?t ; ex:confidence ?c . \
                     GRAPH <{LEDGER}#{GRAPH}> {{ ex:doc ex:mentions ?t }} }}"
                ),
            )
            .await,
            ["0.9"],
            "[{label}] the mention is the default graph's reified edge"
        );
    }
}

/// Annotations written after a graph import index incrementally over the
/// spliced dictionary: an edge it holds keeps its term, a new one adds one,
/// and a rebuild from the commits agrees.
#[tokio::test]
async fn later_annotations_index_incrementally_over_the_spliced_terms() {
    let dir = tempfile::tempdir().unwrap();
    annotated_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    import_into_graph(&fluree, &[("claims.ttl", GRAPH_CLAIMS)], GRAPH)
        .await
        .expect("graph import");
    fluree
        .graph(LEDGER)
        .transact()
        .sparql_update(&format!(
            "PREFIX ex: <{EX}> INSERT DATA {{ GRAPH <{GRAPH}> {{ \
             ex:alice ex:knows ex:bob {{| ex:confidence 0.5 |}} . \
             ex:erin ex:knows ex:frank {{| ex:confidence 0.3 |}} }} }}"
        ))
        .commit()
        .await
        .expect("annotate");
    support::build_incrementally_and_publish_index(&fluree, LEDGER).await;

    let want = [
        "ex:alice ex:bob 0.5",
        "ex:alice ex:bob 0.7",
        "ex:carol ex:dave 0.1",
        "ex:erin ex:frank 0.3",
    ];
    for label in ["incremental", "rebuilt"] {
        if label == "rebuilt" {
            fluree_at(dir.path())
                .reindex(LEDGER, ReindexOptions::default())
                .await
                .expect("full rebuild");
        }
        let fluree = fluree_at(dir.path());
        assert_eq!(term_count(&fluree).await, 3, "[{label}]");
        assert_eq!(claims(&fluree, Some(GRAPH)).await, want, "[{label}]");
        assert_eq!(
            sparql_rows(
                &fluree,
                "?c",
                &format!(
                    "WHERE {{ GRAPH <{LEDGER}#{GRAPH}> {{ ?r rdf:reifies \
                     <<( ex:alice ex:knows ex:bob )>> ; ex:confidence ?c }} }}"
                ),
            )
            .await,
            ["0.5", "0.7"],
            "[{label}]"
        );
    }
}

/// [`GRAPH_CLAIMS`] as JSON-LD `@annotation`s.
const GRAPH_CLAIMS_JSONLD: &str = r#"{
  "@context": {"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
  "@graph": [
    {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob",
      "@annotation": {"ex:confidence": {"@value": "0.7", "@type": "xsd:decimal"}}}},
    {"@id": "ex:carol", "ex:knows": {"@id": "ex:dave",
      "@annotation": {"ex:confidence": {"@value": "0.1", "@type": "xsd:decimal"}}}}
  ]
}"#;

/// One graph's annotations three ways, each into a ledger annotated alike:
/// a Turtle import, a JSON-LD import, and the Turtle's statements
/// transacted. All three read the same, through the same terms.
#[tokio::test]
async fn a_graph_import_reads_as_its_json_ld_twin_and_its_transaction() {
    let ledger = |label: &'static str| async move {
        let dir = tempfile::tempdir().unwrap();
        annotated_ledger(dir.path()).await;
        let fluree = fluree_at(dir.path());
        match label {
            "turtle" => {
                import_into_graph(&fluree, &[("claims.ttl", GRAPH_CLAIMS)], GRAPH)
                    .await
                    .expect("Turtle import");
            }
            "json-ld" => {
                import_into_graph(&fluree, &[("claims.jsonld", GRAPH_CLAIMS_JSONLD)], GRAPH)
                    .await
                    .expect("JSON-LD import");
            }
            _ => {
                let body = GRAPH_CLAIMS.replace("@prefix ex: <http://example.org/> .", "");
                fluree
                    .graph(LEDGER)
                    .transact()
                    .sparql_update(&format!(
                        "PREFIX ex: <{EX}> INSERT DATA {{ GRAPH <{GRAPH}> {{ {body} }} }}"
                    ))
                    .commit()
                    .await
                    .expect("transaction");
                support::build_incrementally_and_publish_index(&fluree, LEDGER).await;
            }
        }
        let fluree = fluree_at(dir.path());
        let read = (
            statements(&fluree).await,
            claims(&fluree, Some(GRAPH)).await,
            term_count(&fluree).await,
        );
        drop(dir);
        read
    };
    let turtle = ledger("turtle").await;
    assert_eq!(
        turtle.1,
        ["ex:alice ex:bob 0.7", "ex:carol ex:dave 0.1"],
        "the Turtle import"
    );
    assert_eq!(turtle.2, 2);
    assert_eq!(ledger("json-ld").await, turtle, "the JSON-LD import");
    assert_eq!(ledger("transacted").await, turtle, "the transaction");
}

/// A ledger whose index holds annotations from before links has no term
/// dictionary to extend: the import is refused before it reads the source,
/// and goes through once a reindex writes the links.
#[tokio::test]
async fn a_ledger_indexed_before_links_is_reindexed_first() {
    let dir = tempfile::tempdir().unwrap();
    annotated_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let record = fluree
        .nameservice()
        .lookup(LEDGER)
        .await
        .unwrap()
        .expect("ledger");
    let cs = fluree.content_store(&record.storage_namespace());
    let bytes = cs
        .get(record.index_head_id.as_ref().expect("index"))
        .await
        .unwrap();
    let mut root = fluree_db_binary_index::IndexRoot::decode(&bytes).unwrap();
    assert!(root.has_annotations && root.term_dict.is_some());
    root.term_dict = None;
    let cid = cs
        .put(fluree_db_core::ContentKind::IndexRoot, &root.encode())
        .await
        .unwrap();
    fluree
        .publisher()
        .unwrap()
        .publish_index_allow_equal_fenced(LEDGER, record.fence, record.index_t, &cid)
        .await
        .unwrap();

    let fluree = fluree_at(dir.path());
    assert_refused(
        &fluree,
        &[("claims.ttl", GRAPH_CLAIMS)],
        GRAPH,
        "reindex it before importing",
    )
    .await;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");
    import_into_graph(
        &fluree_at(dir.path()),
        &[("claims.ttl", GRAPH_CLAIMS)],
        GRAPH,
    )
    .await
    .expect("graph import after the reindex");
    assert_eq!(
        claims(&fluree_at(dir.path()), Some(GRAPH)).await,
        ["ex:alice ex:bob 0.7", "ex:carol ex:dave 0.1"]
    );
}

/// One reifier with two links, the first to a term new to the ledger and the
/// second to one it holds: the new term's handle is the higher, so the links
/// read in the opposite order to the source's. Each is found by a lookup
/// bound to it, as by a scan.
#[tokio::test]
async fn a_reifier_whose_links_reorder_reads_each_one() {
    let dir = tempfile::tempdir().unwrap();
    annotated_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let source = "@prefix ex: <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
ex:r rdf:reifies <<( ex:carol ex:knows ex:dave )>> , <<( ex:alice ex:knows ex:bob )>> .
";
    import_into_graph(&fluree, &[("links.ttl", source)], GRAPH)
        .await
        .expect("graph import");
    for label in ["spliced", "rebuilt"] {
        if label == "rebuilt" {
            fluree_at(dir.path())
                .reindex(LEDGER, ReindexOptions::default())
                .await
                .expect("full rebuild");
        }
        let fluree = fluree_at(dir.path());
        let graph = |pattern: &str| format!("WHERE {{ GRAPH <{LEDGER}#{GRAPH}> {{ {pattern} }} }}");
        assert_eq!(
            sparql_rows(
                &fluree,
                "?s",
                &graph("ex:r rdf:reifies ?t BIND(SUBJECT(?t) AS ?s)")
            )
            .await,
            ["ex:alice", "ex:carol"],
            "[{label}] a scan"
        );
        for (term, s) in [
            ("<<( ex:alice ex:knows ex:bob )>>", "ex:alice"),
            ("<<( ex:carol ex:knows ex:dave )>>", "ex:carol"),
        ] {
            assert_eq!(
                sparql_rows(&fluree, "?r", &graph(&format!("?r rdf:reifies {term}"))).await,
                ["ex:r"],
                "[{label}] by object, {s}"
            );
            assert_eq!(
                sparql_rows(
                    &fluree,
                    "?n",
                    &graph(&format!("ex:r rdf:reifies {term} BIND(1 AS ?n)"))
                )
                .await,
                ["1"],
                "[{label}] bound, {s}"
            );
        }
    }
}

/// A ledger without a triple-term dictionary gets one from a graph import
/// that brings annotations, and an unannotated ledger is annotated after.
#[tokio::test]
async fn annotations_import_into_a_graph_of_an_unannotated_ledger() {
    let dir = tempfile::tempdir().unwrap();
    indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    assert_eq!(term_count(&fluree).await, 0);
    assert!(
        !fluree
            .ledger(LEDGER)
            .await
            .unwrap()
            .snapshot
            .has_annotations
    );

    let result = import_into_graph(&fluree, &[("claims.ttl", GRAPH_CLAIMS)], GRAPH)
        .await
        .expect("graph import");
    assert!(result.has_annotations);
    let fluree = fluree_at(dir.path());
    assert_eq!(term_count(&fluree).await, 2);
    assert!(
        fluree
            .ledger(LEDGER)
            .await
            .unwrap()
            .snapshot
            .has_annotations
    );
    assert!(claims(&fluree, None).await.is_empty());
    assert_eq!(
        claims(&fluree, Some(GRAPH)).await,
        ["ex:alice ex:bob 0.7", "ex:carol ex:dave 0.1"]
    );
}

/// A memory-backed connection whose nameservice can hold or fail an index
/// publish, over [`seed_ledger`]'s ledger, and the storage and nameservice a
/// second connection to it needs.
struct Pausing {
    fluree: Fluree,
    ns: Arc<PausingNameService>,
    storage: MemoryStorage,
    inner: Arc<MemoryNameService>,
    base_t: i64,
}

impl Pausing {
    async fn new() -> Self {
        let storage = MemoryStorage::new();
        let inner = Arc::new(MemoryNameService::new());
        let ns = Arc::new(PausingNameService::new(inner.clone()));
        let fluree = FlureeBuilder::memory()
            .build_with(storage.clone(), NameServiceMode::ReadWrite(ns.clone()));
        let base_t = seed_ledger(&fluree).await;
        Self {
            fluree,
            ns,
            storage,
            inner,
            base_t,
        }
    }

    /// Another connection to the ledger, through the nameservice unwrapped.
    fn other(&self) -> Fluree {
        FlureeBuilder::memory().build_with(
            self.storage.clone(),
            NameServiceMode::ReadWrite(self.inner.clone()),
        )
    }
}

/// Hold an import of `files` at its index publish, once its commit head has
/// moved, and notify the connection's ledger cache with the record as it
/// stands then, as the cache listener does on the import's commit event.
/// Returns the import's commit count and what the notification did.
async fn notify_while_the_import_publishes(files: Vec<(String, String)>) -> (i64, NotifyResult) {
    let c = Pausing::new().await;
    let handle = c.fluree.ledger_cached(LEDGER).await.unwrap();
    c.ns.pause_next_index_publish();
    let importer = c.fluree.clone();
    let import = tokio::spawn(async move {
        let files: Vec<(&str, &str)> = files
            .iter()
            .map(|(name, body)| (name.as_str(), body.as_str()))
            .collect();
        import_into_graph(&importer, &files, GRAPH).await
    });
    tokio::time::timeout(Duration::from_secs(60), c.ns.index_entered.notified())
        .await
        .expect("the import reaches its index publish");

    let record = c
        .fluree
        .nameservice()
        .lookup(LEDGER)
        .await
        .unwrap()
        .expect("record");
    assert!(
        record.index_t < record.commit_t,
        "the commit head is published and the index not yet"
    );
    let manager = Arc::clone(c.fluree.ledger_manager().expect("a ledger cache"));
    let notify = tokio::spawn(async move {
        manager
            .notify(NsNotify {
                ledger_id: LedgerId::parse(LEDGER).unwrap(),
                record: Some(record),
            })
            .await
    });
    // Time for the notification to plan against that record while the
    // index publish is held.
    tokio::time::sleep(Duration::from_millis(300)).await;
    c.ns.index_resume.notify_one();

    let imported = import.await.unwrap().expect("graph import");
    let notified = notify.await.unwrap().expect("notify");
    assert_eq!(handle.snapshot().await.to_ledger_state().t(), imported.t);
    (imported.t - c.base_t, notified)
}

/// The ledger cache, notified while an import of a few commits publishes,
/// finds it installed at its heads instead of catching up on its commits one
/// by one.
#[tokio::test]
async fn a_notification_while_a_short_import_publishes_finds_it_installed() {
    let few = vec![("a.ttl".to_string(), SOURCE_A.to_string())];
    let (commits, notified) = notify_while_the_import_publishes(few).await;
    assert!(commits <= 5, "the catch-up path ({commits} commits)");
    assert!(
        matches!(notified, NotifyResult::Current),
        "{commits} commits: {notified:?}"
    );
}

/// The ledger cache, notified while an import of more than five commits
/// publishes, finds it installed at its heads instead of reloading the
/// ledger.
#[tokio::test]
async fn a_notification_while_a_long_import_publishes_finds_it_installed() {
    let many = (0..6)
        .map(|i| {
            (
                format!("f{i}.ttl"),
                format!("@prefix ex: <{EX}> .\nex:item{i} ex:name \"Item {i}\" .\n"),
            )
        })
        .collect();
    let (commits, notified) = notify_while_the_import_publishes(many).await;
    assert!(commits > 5, "the reload path ({commits} commits)");
    assert!(
        matches!(notified, NotifyResult::Current),
        "{commits} commits: {notified:?}"
    );
}

/// Upserts replace imported values, in the imported graph and beside it in
/// the default graph: one staged while the import holds the ledger, so
/// staged again on the import, and one through the handle the import
/// installed. The ledger then reads the same through that handle, after a
/// fresh load, over an incremental index and after a reindex.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upserts_replace_imported_values() {
    let dir = tempfile::tempdir().unwrap();
    indexed_ledger(dir.path()).await;
    let fluree = fluree_at(dir.path());
    let handle = fluree.ledger_cached(LEDGER).await.unwrap();

    let (parked, release) = handle.gate_next_writer_exclusion_for_test();
    let importer = fluree.clone();
    let import =
        tokio::spawn(
            async move { import_into_graph(&importer, &[("a.ttl", SOURCE_A)], GRAPH).await },
        );
    parked.await.expect("the import parks holding the ledger");
    let (writer, writer_handle) = (fluree.clone(), handle.clone());
    let early = tokio::spawn(async move {
        let upsert = format!(
            "@prefix ex: <{EX}> .\nGRAPH <{GRAPH}> {{ ex:widget2 ex:name \"Gadget Pro\" . }}\n"
        );
        writer
            .stage(&writer_handle)
            .upsert_turtle(&upsert)
            .execute()
            .await
            .map(|r| r.receipt.t)
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!early.is_finished(), "the upsert waits for the import");
    release.send(true).unwrap();
    let imported = import.await.unwrap().expect("graph import");
    let early_t = early
        .await
        .unwrap()
        .expect("the upsert staged during the import");
    assert_eq!(early_t, imported.t + 1);

    let upsert = format!(
        "@prefix ex: <{EX}> .\n\
         ex:alice ex:name \"Alice B\" .\n\
         ex:dave ex:name \"Dave\" .\n\
         GRAPH <{GRAPH}> {{ ex:widget1 ex:name \"Widget Pro\" . ex:widget9 ex:name \"Widget Nine\" . }}\n"
    );
    fluree
        .stage(&handle)
        .upsert_turtle(&upsert)
        .execute()
        .await
        .expect("the upsert after the import");

    let read = statements(&fluree).await;
    for row in [
        "g: ex:widget1 ex:name Widget Pro",
        "g: ex:widget2 ex:name Gadget Pro",
        "g: ex:widget9 ex:name Widget Nine",
        "g: ex:alice ex:likes ex:widget2",
        "default: ex:alice ex:name Alice B",
        "default: ex:dave ex:name Dave",
    ] {
        assert!(read.contains(&row.to_string()), "{row}: {read:?}");
    }
    for row in [
        "g: ex:widget1 ex:name Widget",
        "g: ex:widget2 ex:name Gadget",
        "default: ex:alice ex:name Alice",
    ] {
        assert!(
            !read.contains(&row.to_string()),
            "{row} is replaced: {read:?}"
        );
    }

    assert_eq!(
        statements(&fluree_at(dir.path())).await,
        read,
        "a fresh load"
    );
    support::build_incrementally_and_publish_index(&fluree, LEDGER).await;
    assert_eq!(
        statements(&fluree_at(dir.path())).await,
        read,
        "over an incremental index"
    );
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("full rebuild");
    assert_eq!(
        statements(&fluree_at(dir.path())).await,
        read,
        "after a reindex"
    );
}

/// An index publish that fails once the commit head has moved leaves the
/// import in the ledger, and the error says so. The ledger reads the graph
/// from the import's commits, a write through the importing connection lands
/// on top of them, and a reindex covers them.
#[tokio::test]
async fn an_index_publish_failure_leaves_the_import_published() {
    let c = Pausing::new().await;
    c.fluree.ledger_cached(LEDGER).await.unwrap();
    c.ns.fail_index_publishes();
    let source = source_dir(&[("a.ttl", SOURCE_A)]);
    let err = c
        .fluree
        .import_into_graph(LEDGER, GRAPH)
        .import(source.path())
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect_err("the index publish fails");
    let ImportError::PublishedWithoutIndex { t, .. } = err else {
        panic!("{err}");
    };
    assert!(t > c.base_t);
    let record = c
        .other()
        .nameservice()
        .lookup(LEDGER)
        .await
        .unwrap()
        .expect("record");
    assert_eq!((record.commit_t, record.index_t), (t, c.base_t));
    let published = statements(&c.other()).await;
    assert!(
        published.contains(&"g: ex:widget1 ex:name Widget".to_string()),
        "{published:?}"
    );

    let written = c
        .fluree
        .graph(LEDGER)
        .transact()
        .insert(&json!({"@context": {"ex": EX}, "@id": "ex:erin", "ex:name": "Erin"}))
        .commit()
        .await
        .expect("a write after the failed publish");
    assert_eq!(written.receipt.t, t + 1);

    let other = c.other();
    other
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");
    let record = other
        .nameservice()
        .lookup(LEDGER)
        .await
        .unwrap()
        .expect("record");
    assert_eq!(record.index_t, t + 1);
    let mut want = published;
    want.push("default: ex:erin ex:name Erin".to_string());
    want.sort();
    assert_eq!(statements(&c.other()).await, want);
}
