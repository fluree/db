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
    let path = data_dir.join("data.trig");
    std::fs::write(&path, trig).unwrap();
    let fluree = FlureeBuilder::file(db_dir.to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree
        .create(ledger_id)
        .import(&path)
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
