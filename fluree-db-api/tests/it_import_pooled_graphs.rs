//! Bulk import into the pooled tree: small named graphs share one index tree
//! (each row carrying its graph), large ones keep their own. Queries must see
//! exactly the per-graph contents a tree per graph would give.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::FlureeBuilder;
use fluree_db_binary_index::IndexRoot;
use std::path::Path;

fn find_roots(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "fir6") {
                out.push(path);
            }
        }
    }
    out
}

/// Named graphs with a dedicated tree, excluding the system graphs.
fn dedicated_user_graphs(root: &IndexRoot) -> Vec<u16> {
    root.named_graphs
        .iter()
        .map(|ng| ng.g_id)
        .filter(|&g| g >= fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID)
        .collect()
}

fn latest_root(dir: &Path) -> IndexRoot {
    let roots = find_roots(dir);
    let newest = roots
        .iter()
        .max_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok())
        .expect("an index root");
    IndexRoot::decode(&std::fs::read(newest).unwrap()).expect("decode root")
}

/// Rows of a SELECT as sorted `var=value` strings, one string per row.
async fn select(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    sparql: &str,
) -> Vec<String> {
    let result = support::query_sparql(fluree, ledger, sparql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{sparql}"));
    let json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let mut rows: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| {
            let mut cells: Vec<String> = b
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| format!("{k}={}", v["value"].as_str().unwrap()))
                .collect();
            cells.sort();
            cells.join(" ")
        })
        .collect();
    rows.sort();
    rows
}

const PREFIXES: &str = "PREFIX ex: <http://example.org/>\n";

async fn import_trig(
    db_dir: &Path,
    data_dir: &Path,
    ledger_id: &str,
    trig: &str,
) -> (fluree_db_api::Fluree, fluree_db_api::LedgerState) {
    import_trig_files(db_dir, data_dir, ledger_id, &[trig]).await
}

/// Import TriG files in order, one commit each.
async fn import_trig_files(
    db_dir: &Path,
    data_dir: &Path,
    ledger_id: &str,
    files: &[&str],
) -> (fluree_db_api::Fluree, fluree_db_api::LedgerState) {
    let dir = data_dir.join("import");
    std::fs::create_dir_all(&dir).unwrap();
    for (i, trig) in files.iter().enumerate() {
        std::fs::write(dir.join(format!("part-{i:02}.trig")), trig).unwrap();
    }
    let fluree = FlureeBuilder::file(db_dir.to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree
        .create(ledger_id)
        .import(&dir)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    (fluree, ledger)
}

/// The same triple in three graphs stays three facts, graph-scoped reads see
/// only their graph, and joins inside `GRAPH ?g` never pair facts from
/// different graphs.
#[tokio::test]
async fn pooled_graphs_answer_like_one_tree_per_graph() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let trig = r#"
@prefix ex: <http://example.org/> .
ex:g1 { ex:s ex:p "same" . ex:a ex:val 42 . ex:x a ex:Doc ; ex:label "X1" . ex:y a ex:Doc . }
ex:g2 { ex:s ex:p "same" . ex:b ex:val 7 . ex:x a ex:Doc ; ex:label "X2" . ex:y ex:label "Y2" . }
ex:g3 { ex:s ex:p "same" . ex:c ex:val 42 . }
"#;
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), "test/pooled:main", trig).await;

    let root = latest_root(db_dir.path());
    let pool = root.pool.as_ref().expect("small named graphs are pooled");
    assert_eq!(
        pool.graphs.len(),
        3,
        "all three graphs pooled: {:?}",
        pool.graphs
    );
    assert!(
        dedicated_user_graphs(&root).is_empty(),
        "no dedicated user graph trees"
    );

    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?g WHERE {{ GRAPH ?g {{ ex:s ex:p \"same\" }} }}")
        )
        .await,
        vec![
            "g=http://example.org/g1",
            "g=http://example.org/g2",
            "g=http://example.org/g3"
        ],
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?g ?s WHERE {{ GRAPH ?g {{ ?s ex:val 42 }} }}")
        )
        .await,
        vec![
            "g=http://example.org/g1 s=http://example.org/a",
            "g=http://example.org/g3 s=http://example.org/c"
        ],
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?s ?o WHERE {{ GRAPH ex:g2 {{ ?s ex:label ?o }} }}")
        )
        .await,
        vec!["o=X2 s=http://example.org/x", "o=Y2 s=http://example.org/y"],
    );
    // ex:y is a Doc only in g1 and labelled only in g2: no graph has both.
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!(
                "{PREFIXES}SELECT ?g ?d ?l WHERE {{ GRAPH ?g {{ ?d a ex:Doc ; ex:label ?l }} }}"
            )
        )
        .await,
        vec![
            "d=http://example.org/x g=http://example.org/g1 l=X1",
            "d=http://example.org/x g=http://example.org/g2 l=X2"
        ],
    );
    // OPTIONAL probes stay in the row's graph: ex:y's only label is in g2.
    assert_eq!(
        select(&fluree, &ledger, &format!("{PREFIXES}SELECT ?g ?d ?l WHERE {{ GRAPH ?g {{ ?d a ex:Doc . OPTIONAL {{ ?d ex:label ?l }} }} }}")).await,
        vec![
            "d=http://example.org/x g=http://example.org/g1 l=X1",
            "d=http://example.org/x g=http://example.org/g2 l=X2",
            "d=http://example.org/y g=http://example.org/g1"
        ],
    );
    assert_eq!(
        select(&fluree, &ledger, &format!("{PREFIXES}SELECT ?g (COUNT(*) AS ?n) WHERE {{ GRAPH ?g {{ ?s ?p ?o }} }} GROUP BY ?g")).await,
        vec![
            "g=http://example.org/g1 n=5",
            "g=http://example.org/g2 n=5",
            "g=http://example.org/g3 n=2"
        ],
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH ex:g3 {{ ?s ?p ?o }} }}")
        )
        .await,
        vec!["n=2"],
    );
}

/// A named graph above the pooling threshold keeps its own tree next to the
/// pooled ones, and both answer.
#[tokio::test]
async fn large_named_graph_keeps_its_own_tree() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let mut trig = String::from("@prefix ex: <http://example.org/> .\nex:big {\n");
    for i in 0..5000 {
        trig.push_str(&format!("  ex:n{i} ex:rank {i} .\n"));
    }
    trig.push_str("}\nex:small { ex:n1 ex:rank 1 . ex:solo ex:rank 9999 . }\n");
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), "test/placed:main", &trig).await;

    let root = latest_root(db_dir.path());
    let pool = root.pool.as_ref().expect("the small graph is pooled");
    assert_eq!(pool.graphs.len(), 1);
    let dedicated = dedicated_user_graphs(&root);
    assert_eq!(dedicated.len(), 1, "the large graph has a tree of its own");
    assert!(!pool.contains(dedicated[0]));

    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?g WHERE {{ GRAPH ?g {{ ex:n1 ex:rank 1 }} }}")
        )
        .await,
        vec!["g=http://example.org/big", "g=http://example.org/small"],
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!(
                "{PREFIXES}SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH ex:big {{ ?s ex:rank ?r }} }}"
            )
        )
        .await,
        vec!["n=5000"],
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?s WHERE {{ GRAPH ex:small {{ ?s ex:rank ?r }} }}")
        )
        .await,
        vec!["s=http://example.org/n1", "s=http://example.org/solo"],
    );
}

async fn sparql_update(fluree: &fluree_db_api::Fluree, ledger_id: &str, sparql: &str) {
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(sparql)
        .index_config(fluree_db_api::IndexConfig {
            reindex_min_bytes: usize::MAX,
            reindex_max_bytes: usize::MAX,
        })
        .commit()
        .await
        .expect("sparql update");
}

/// Build the index for the commits since the last one (incremental when it
/// can be), publish it, and return its root and the reloaded ledger.
async fn index_and_reload(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> (IndexRoot, fluree_db_api::LedgerState) {
    let store = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
    let result = fluree_db_indexer::build_index_for_ledger(
        store.clone(),
        fluree.nameservice(),
        ledger_id,
        fluree_db_indexer::IndexerConfig {
            incremental_enabled: true,
            incremental_max_commits: 100,
            ..Default::default()
        },
    )
    .await
    .expect("index");
    fluree
        .publisher()
        .unwrap()
        .publish_index_fenced(
            ledger_id,
            support::fence_of(fluree, ledger_id).await,
            result.index_t,
            &result.root_id,
        )
        .await
        .expect("publish index");
    let root = IndexRoot::decode(&store.get(&result.root_id).await.expect("root bytes"))
        .expect("decode root");
    (root, fluree.ledger(ledger_id).await.expect("reload ledger"))
}

const THREE_GRAPHS: &str = r#"
@prefix ex: <http://example.org/> .
ex:g1 { ex:a ex:val 1 . ex:a ex:label "A" . ex:same ex:p 0 . }
ex:g2 { ex:b ex:val 2 . ex:b ex:label "B" . ex:same ex:p 0 . }
ex:g3 { ex:c ex:val 3 . ex:same ex:p 0 . }
"#;

/// An append-only write to a pooled graph promotes just that graph to its
/// own tree, incrementally; every graph still reads back exactly.
#[tokio::test]
async fn appending_to_a_pooled_graph_promotes_only_that_graph() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-promote:main";
    let (fluree, _) = import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g1 { ex:a ex:val 1 . ex:a2 ex:val 10 . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    let pool = root.pool.as_ref().expect("untouched graphs stay pooled");
    assert_eq!(
        pool.graphs.len(),
        2,
        "g2 and g3 stay pooled: {:?}",
        pool.graphs
    );
    let dedicated = dedicated_user_graphs(&root);
    assert_eq!(dedicated.len(), 1, "g1 has its own tree now");
    assert!(!pool.contains(dedicated[0]));

    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?s ?o WHERE {{ GRAPH ex:g1 {{ ?s ex:val ?o }} }}")
        )
        .await,
        vec!["o=1 s=http://example.org/a", "o=10 s=http://example.org/a2"],
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?g WHERE {{ GRAPH ?g {{ ex:same ex:p 0 }} }}")
        )
        .await,
        vec![
            "g=http://example.org/g1",
            "g=http://example.org/g2",
            "g=http://example.org/g3"
        ],
    );
    assert_eq!(
        select(&fluree, &ledger, &format!("{PREFIXES}SELECT ?g (COUNT(*) AS ?n) WHERE {{ GRAPH ?g {{ ?s ?p ?o }} }} GROUP BY ?g")).await,
        vec![
            "g=http://example.org/g1 n=4",
            "g=http://example.org/g2 n=3",
            "g=http://example.org/g3 n=2"
        ],
    );
}

/// A retraction in a pooled graph has no pooled history to record into, so
/// the build falls back to a full rebuild; the result is exact.
#[tokio::test]
async fn retracting_from_a_pooled_graph_rebuilds() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-retract:main";
    let (fluree, _) = import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g2 { ex:b ex:label \"B\" . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert!(
        root.pool.as_ref().is_none_or(|p| p.graphs.is_empty()),
        "a full rebuild writes dedicated trees"
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?s ?o WHERE {{ GRAPH ex:g2 {{ ?s ?p ?o }} }}")
        )
        .await,
        vec![
            "o=0 s=http://example.org/same",
            "o=2 s=http://example.org/b"
        ],
    );
}

/// An imported pool is append-only: an earlier `t` sees exactly the rows
/// written by then.
#[tokio::test]
async fn pooled_graphs_answer_at_an_earlier_t() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-t:main";
    let first = "@prefix ex: <http://example.org/> .\nex:g1 { ex:a ex:val 1 . }\nex:g2 { ex:b ex:val 2 . }\n";
    let second = "@prefix ex: <http://example.org/> .\nex:g1 { ex:a2 ex:val 10 . }\nex:g3 { ex:c ex:val 3 . }\n";
    let (fluree, ledger) =
        import_trig_files(db_dir.path(), data_dir.path(), ledger_id, &[first, second]).await;
    assert!(latest_root(db_dir.path()).pool.is_some());
    let t = ledger.t();
    assert!(t >= 2, "two commits");

    let earlier = fluree
        .query_connection(&serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "from": {"@id": ledger_id, "t": 1, "graph": "http://example.org/g1"},
            "select": ["?s"],
            "where": {"@id": "?s", "ex:val": "?v"}
        }))
        .await
        .expect("query at t=1")
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");
    let mut subjects: Vec<String> = earlier
        .as_array()
        .expect("rows")
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_string)
                .unwrap_or_else(|| v.to_string())
        })
        .collect();
    subjects.sort();
    assert_eq!(subjects.len(), 1, "g1 at t=1 holds only ex:a: {subjects:?}");
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?s WHERE {{ GRAPH ex:g1 {{ ?s ex:val ?v }} }}")
        )
        .await,
        vec!["s=http://example.org/a", "s=http://example.org/a2"],
    );
}
