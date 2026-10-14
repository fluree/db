//! A fact inserted and retracted between two index builds stays retracted
//! after the second one, in a graph the first build never saw.
//!
//! An incremental build gives a graph with no partition in its base index a
//! fresh branch. That branch used to be written from the window's net
//! records with their ops dropped, so a fact whose last op in the window was
//! a retraction came back as a live row, and its in-window history was
//! discarded. A full rebuild resolves every fact's events to its final state
//! and was never affected.
//!
//! The graph lacks a partition when the base index holds nothing in it: a
//! named graph first written after the build, or the default graph of a
//! ledger whose indexed data all sits in named graphs.
//!
//! Background indexing is off: these tests drive each build themselves, and
//! a background build racing them would make the outcome timing-dependent.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState};
use serde_json::{json, Value as JsonValue};
use std::path::Path;

const EX: &str = "http://example.org/ns/";
const G1: &str = "http://example.org/g1";
const G2: &str = "http://example.org/g2";

#[derive(Clone, Copy, Debug)]
enum Base {
    /// The indexed data is in the default graph only.
    Default,
    /// The indexed data is in `G2` only: the default graph has no partition.
    G2Only,
}

#[derive(Clone, Copy, Debug)]
enum Build {
    Full,
    Incremental,
}

fn fluree_at(dir: &Path) -> Fluree {
    FlureeBuilder::file(dir.to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("file fluree")
}

async fn sparql(fluree: &Fluree, ledger: LedgerState, body: &str) -> LedgerState {
    let text = format!("PREFIX ex: <{EX}>\n{body}");
    let parsed = fluree_db_sparql::parse_sparql(&text);
    assert!(!parsed.has_errors(), "{text}: {:?}", parsed.diagnostics);
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &parsed.ast.expect("ast"),
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"))
        .ledger
}

/// `body` inside `GRAPH <graph> { … }`, or bare for the default graph.
fn in_graph(graph: Option<&str>, body: &str) -> String {
    match graph {
        Some(g) => format!("GRAPH <{g}> {{ {body} }}"),
        None => body.to_string(),
    }
}

/// A file-backed ledger whose published index holds `base`.
async fn indexed_ledger(dir: &Path, id: &str, base: Base) -> (Fluree, LedgerState) {
    let fluree = fluree_at(dir);
    let ledger = fluree.create_ledger(id).await.expect("create");
    let seed = match base {
        Base::Default => "INSERT DATA { ex:a ex:q \"base\" }".to_string(),
        Base::G2Only => format!("INSERT DATA {{ GRAPH <{G2}> {{ ex:a ex:q \"base\" }} }}"),
    };
    let ledger = sparql(&fluree, ledger, &seed).await;
    drop(ledger);
    support::rebuild_and_publish_index(&fluree, id).await;
    let ledger = fluree.ledger(id).await.expect("reload");
    assert!(ledger.snapshot.range_provider.is_some());
    (fluree, ledger)
}

async fn build(fluree: &Fluree, id: &str, build: Build) {
    match build {
        Build::Full => support::rebuild_and_publish_index(fluree, id).await,
        Build::Incremental => support::build_incrementally_and_publish_index(fluree, id).await,
    }
}

/// `ex:s ex:p ?o` over the dataset `from`, sorted.
async fn objects(fluree: &Fluree, ledger_id: &str, from: &str) -> Vec<String> {
    let q = json!({
        "@context": {"ex": EX},
        "from": from,
        "select": ["?o"],
        "where": {"@id": "ex:s", "ex:p": "?o"}
    });
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let result = fluree.query_connection(&q).await.expect("query");
    let json = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    let mut rows: Vec<String> = json
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| match row {
            JsonValue::Array(cells) => cells[0].to_string(),
            cell => cell.to_string(),
        })
        .collect();
    rows.sort();
    rows
}

fn from_alias(ledger_id: &str, graph: Option<&str>, t: Option<i64>) -> String {
    let at = t.map(|t| format!("@t:{t}")).unwrap_or_default();
    match graph {
        Some(g) => format!("{ledger_id}{at}#{g}"),
        None => format!("{ledger_id}{at}"),
    }
}

/// `ex:s ex:p ?o` in `graph`, read through `fluree` and through a fresh
/// instance over the same storage, which must agree.
async fn current(fluree: &Fluree, dir: &Path, id: &str, graph: Option<&str>) -> Vec<String> {
    let from = from_alias(id, graph, None);
    let same = objects(fluree, id, &from).await;
    let fresh = objects(&fluree_at(dir), id, &from).await;
    assert_eq!(same, fresh, "{from}: the building instance vs a fresh one");
    same
}

/// Insert a string, an integer, a language-tagged string and a ref into
/// `target`, delete them all again, then build the index.
async fn insert_delete_build(
    base: Base,
    target: Option<&str>,
    jsonld_delete: bool,
    kind: Build,
) -> Vec<String> {
    let dir = tempfile::tempdir().expect("tempdir");
    let id = format!(
        "it/delete-survives-{base:?}-{}-{}-{kind:?}:main",
        target.map_or("default", |g| &g[g.len() - 2..]),
        if jsonld_delete { "jsonld" } else { "sparql" },
    );
    let (fluree, ledger) = indexed_ledger(dir.path(), &id, base).await;

    let facts = r#"ex:s ex:p "x", 42, "hi"@en, ex:o ."#;
    let ledger = sparql(
        &fluree,
        ledger,
        &format!("INSERT DATA {{ {} }}", in_graph(target, facts)),
    )
    .await;
    assert_eq!(current(&fluree, dir.path(), &id, target).await.len(), 4);

    let ledger = if jsonld_delete {
        let mut txn = json!({
            "@context": {"ex": EX},
            "delete": {
                "@id": "ex:s",
                "ex:p": ["x", 42, {"@value": "hi", "@language": "en"}, {"@id": "ex:o"}]
            }
        });
        if let Some(g) = target {
            txn["graph"] = json!(g);
        }
        fluree
            .update(ledger, &txn)
            .await
            .expect("JSON-LD delete")
            .ledger
    } else {
        let body = format!("DELETE DATA {{ {} }}", in_graph(target, facts));
        sparql(&fluree, ledger, &body).await
    };
    drop(ledger);
    assert_eq!(
        current(&fluree, dir.path(), &id, target).await,
        Vec::<String>::new(),
        "{id}: deleted before the build"
    );

    build(&fluree, &id, kind).await;
    current(&fluree, dir.path(), &id, target).await
}

#[tokio::test]
async fn deleted_facts_stay_deleted_after_index_build() {
    let mut resurrected = Vec::new();
    for base in [Base::Default, Base::G2Only] {
        for target in [None, Some(G1), Some(G2)] {
            for jsonld_delete in [false, true] {
                for kind in [Build::Full, Build::Incremental] {
                    let after = insert_delete_build(base, target, jsonld_delete, kind).await;
                    if !after.is_empty() {
                        resurrected.push(format!(
                            "base={base:?} graph={} delete={} build={kind:?}: {after:?}",
                            target.unwrap_or("default"),
                            if jsonld_delete { "JSON-LD" } else { "SPARQL" },
                        ));
                    }
                }
            }
        }
    }
    assert!(
        resurrected.is_empty(),
        "deleted facts came back after the build:\n{}",
        resurrected.join("\n")
    );
}

/// A value replaced inside the window keeps only its replacement.
#[tokio::test]
async fn replaced_value_stays_replaced_after_incremental_build() {
    for target in [None, Some(G1)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = format!(
            "it/delete-survives-replace-{}:main",
            target.map_or("default", |g| &g[g.len() - 2..])
        );
        let (fluree, ledger) = indexed_ledger(dir.path(), &id, Base::G2Only).await;
        let ledger = sparql(
            &fluree,
            ledger,
            &format!(
                "INSERT DATA {{ {} }}",
                in_graph(target, "ex:s ex:p \"old\"")
            ),
        )
        .await;
        let with = target.map(|g| format!("WITH <{g}> ")).unwrap_or_default();
        let ledger = sparql(
            &fluree,
            ledger,
            &format!(
                "{with}DELETE {{ ex:s ex:p \"old\" }} INSERT {{ ex:s ex:p \"new\" }} \
                 WHERE {{ ex:s ex:p \"old\" }}"
            ),
        )
        .await;
        drop(ledger);
        assert_eq!(current(&fluree, dir.path(), &id, target).await, ["\"new\""]);

        build(&fluree, &id, Build::Incremental).await;
        assert_eq!(
            current(&fluree, dir.path(), &id, target).await,
            ["\"new\""],
            "{id}: after the build"
        );
    }
}

/// The window's history lands in the new branch: the fact reads as present
/// between its insert and its delete.
#[tokio::test]
async fn history_of_new_graph_survives_incremental_build() {
    for target in [None, Some(G1)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = format!(
            "it/delete-survives-history-{}:main",
            target.map_or("default", |g| &g[g.len() - 2..])
        );
        let (fluree, ledger) = indexed_ledger(dir.path(), &id, Base::G2Only).await;
        let fact = in_graph(target, "ex:s ex:p \"x\"");
        let ledger = sparql(&fluree, ledger, &format!("INSERT DATA {{ {fact} }}")).await;
        let t_insert = ledger.t();
        let ledger = sparql(&fluree, ledger, &format!("DELETE DATA {{ {fact} }}")).await;
        let t_delete = ledger.t();
        drop(ledger);

        build(&fluree, &id, Build::Incremental).await;
        let fresh = fluree_at(dir.path());
        let at = |t| from_alias(&id, target, Some(t));
        assert_eq!(
            objects(&fresh, &id, &at(t_insert - 1)).await,
            Vec::<String>::new(),
            "{id}: before the insert"
        );
        assert_eq!(
            objects(&fresh, &id, &at(t_insert)).await,
            ["\"x\""],
            "{id}: between the insert and the delete"
        );
        assert_eq!(
            objects(&fresh, &id, &at(t_delete)).await,
            Vec::<String>::new(),
            "{id}: at the delete"
        );
    }
}
