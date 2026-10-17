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
fn dedicated_user_graphs(root: &IndexRoot) -> Vec<fluree_db_core::GraphId> {
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

/// Values of `?o` for `s p ?o` in graph `g`, as of `t` (`None`: current).
async fn objects_at(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    ledger_id: &str,
    graph: &str,
    t: Option<i64>,
) -> Vec<String> {
    let mut from =
        serde_json::json!({"@id": ledger_id, "graph": format!("http://example.org/{graph}")});
    if let Some(t) = t {
        from["t"] = serde_json::json!(t);
    }
    let rows = fluree
        .query_connection(&serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": ["?s", "?o"],
            "where": {"@id": "?s", "ex:label": "?o"}
        }))
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");
    let mut out: Vec<String> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| format!("{} {}", r[0].as_str().unwrap(), r[1]))
        .collect();
    out.sort();
    out
}

/// `?s ex:label ?o` events over the whole history of graph `graph`, read by
/// the history scan bound to that graph.
async fn history_events(ledger: &fluree_db_api::LedgerState, graph: &str) -> usize {
    use fluree_db_query::{
        context::ExecutionContext,
        ir::{Ref, Term, TriplePattern},
        operator::Operator,
        BinaryHistoryScanOperator, VarRegistry,
    };
    let store = ledger
        .binary_store
        .as_ref()
        .unwrap()
        .0
        .clone()
        .downcast::<fluree_db_binary_index::BinaryIndexStore>()
        .unwrap();
    let g_id = store
        .graph_id_for_iri(&format!("http://example.org/{graph}"))
        .expect("graph id");
    assert!(store.is_pooled(g_id), "the scan must read the pooled tree");
    let mut vars = VarRegistry::new();
    let pattern = TriplePattern::new(
        Ref::Var(vars.get_or_insert("?s")),
        Ref::Sid(
            ledger
                .snapshot
                .encode_iri("http://example.org/label")
                .expect("predicate sid"),
        ),
        Term::Var(vars.get_or_insert("?o")),
    );
    let mut ctx = ExecutionContext::new(&ledger.snapshot, &vars).with_binary_store(store, g_id);
    ctx.from_t = Some(1);
    let mut scan = BinaryHistoryScanOperator::new(pattern, None, Vec::new());
    scan.open(&ctx).await.expect("open");
    let mut events = 0;
    while let Some(batch) = scan.next_batch(&ctx).await.expect("batch") {
        events += batch.len();
    }
    events
}

/// A retraction in a pooled graph promotes it incrementally, history and
/// all: the pool's trees are untouched, and an earlier `t` still sees the
/// retracted fact.
#[tokio::test]
async fn retracting_from_a_pooled_graph_promotes_it_with_its_history() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-retract:main";
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
    let before = latest_root(db_dir.path()).pool.expect("pooled");
    let t_before = ledger.t();

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g2 { ex:b ex:label \"B\" . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    let pool = root.pool.as_ref().expect("g1 and g3 stay pooled");
    assert_eq!(pool.graphs.len(), 2);
    assert_eq!(
        pool.orders, before.orders,
        "the pooled trees are reused as they were"
    );
    assert_eq!(
        dedicated_user_graphs(&root).len(),
        1,
        "g2 has a tree of its own"
    );

    assert_eq!(
        objects_at(&fluree, &ledger, ledger_id, "g2", None).await,
        Vec::<String>::new()
    );
    assert_eq!(
        objects_at(&fluree, &ledger, ledger_id, "g2", Some(t_before)).await,
        vec!["ex:b \"B\""]
    );
    assert_eq!(
        objects_at(&fluree, &ledger, ledger_id, "g1", None).await,
        vec!["ex:a \"A\""]
    );
}

/// A full rebuild pools small named graphs with their history, and a later
/// retraction promotes one of them carrying that history: every `t` answers
/// as it did when written.
#[tokio::test]
async fn a_rebuild_pools_graphs_with_their_history() {
    let db_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-rebuild:main";
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree.create_ledger(ledger_id).await.expect("create");
    let update = |sparql: &'static str| sparql_update(&fluree, ledger_id, sparql);
    update(
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g1 { ex:a ex:label \"A1\" . }
                       GRAPH ex:g2 { ex:b ex:label \"B1\" . } }",
    )
    .await;
    let t1 = fluree.ledger(ledger_id).await.unwrap().t();
    update(
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g1 { ex:a ex:label \"A1\" . } } ;
         INSERT DATA { GRAPH ex:g1 { ex:a ex:label \"A2\" . } }",
    )
    .await;
    let t2 = fluree.ledger(ledger_id).await.unwrap().t();

    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(
        root.pool.as_ref().map(|p| p.graphs.len()),
        Some(2),
        "the first index is a full rebuild, and it pools both graphs"
    );
    let at = |graph, t| objects_at(&fluree, &ledger, ledger_id, graph, t);
    assert_eq!(at("g1", Some(t1)).await, vec!["ex:a \"A1\""]);
    assert_eq!(at("g1", Some(t2)).await, vec!["ex:a \"A2\""]);
    assert_eq!(at("g2", Some(t1)).await, vec!["ex:b \"B1\""]);
    assert_eq!(
        history_events(&ledger, "g1").await,
        3,
        "A1 asserted, A1 retracted, A2 asserted"
    );
    assert_eq!(
        history_events(&ledger, "g2").await,
        1,
        "only g2's own event"
    );

    update(
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g1 { ex:a ex:label \"A2\" . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(
        root.pool.as_ref().map(|p| p.graphs.len()),
        Some(1),
        "g1 left the pool"
    );
    let at = |graph, t| objects_at(&fluree, &ledger, ledger_id, graph, t);
    assert_eq!(at("g1", None).await, Vec::<String>::new());
    assert_eq!(at("g1", Some(t1)).await, vec!["ex:a \"A1\""]);
    assert_eq!(at("g1", Some(t2)).await, vec!["ex:a \"A2\""]);
    assert_eq!(at("g2", None).await, vec!["ex:b \"B1\""]);
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

/// A pooled graph that also holds an arena (a big integer here) is still one
/// graph: the store's count matches its graph list.
#[tokio::test]
async fn pooled_graph_with_an_arena_counts_once() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let trig = "@prefix ex: <http://example.org/> .\n\
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
        ex:a { ex:x ex:big \"123456789012345678901234567890\"^^xsd:integer . }\n\
        ex:b { ex:y ex:rank 2 . }\n";
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), "test/arena:main", trig).await;
    assert_eq!(
        latest_root(db_dir.path())
            .pool
            .expect("pooled")
            .graphs
            .len(),
        2
    );

    let store = ledger
        .binary_store
        .as_ref()
        .unwrap()
        .0
        .clone()
        .downcast::<fluree_db_binary_index::BinaryIndexStore>()
        .unwrap();
    assert_eq!(store.graph_count(), store.graph_ids().len());
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?v WHERE {{ GRAPH ex:a {{ ex:x ex:big ?v }} }}")
        )
        .await,
        vec!["v=123456789012345678901234567890"],
    );
}

/// The pooled tree is live: a storage sweep plans no orphans and leaves the
/// pooled graphs readable from a fresh process.
#[tokio::test]
async fn sweeping_keeps_the_pooled_tree() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let (fluree, _) = import_trig(
        db_dir.path(),
        data_dir.path(),
        "test/pool-sweep:main",
        THREE_GRAPHS,
    )
    .await;
    assert!(latest_root(db_dir.path()).pool.is_some());

    let plan = fluree
        .plan_index_sweep("test/pool-sweep")
        .await
        .expect("plan sweep");
    assert!(
        plan.orphans.is_empty(),
        "live pooled artifacts: {:?}",
        plan.orphans
    );
    fluree
        .sweep_index_storage("test/pool-sweep")
        .await
        .expect("sweep");
    drop(fluree);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("reopen");
    let ledger = fluree.ledger("test/pool-sweep:main").await.expect("load");
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?s ?o WHERE {{ GRAPH ex:g3 {{ ?s ex:val ?o }} }}")
        )
        .await,
        vec!["o=3 s=http://example.org/c"],
    );
}

/// A vector in a pooled graph keeps its fact-to-handle mapping, so deleting it
/// retracts it: over the pooled base before the next index, and in the tree
/// the graph is promoted to after it.
#[tokio::test]
async fn deleting_a_vector_from_a_pooled_graph_retracts_it() {
    let db_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-vector:main";
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree.create_ledger(ledger_id).await.expect("create");
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#>
         INSERT DATA { GRAPH ex:g1 { ex:doc ex:embedding \"[0.1, 0.2, 0.3]\"^^f:embeddingVector .
                                     ex:doc ex:label \"d\" . }
                       GRAPH ex:g2 { ex:x ex:label \"x\" . } }",
    )
    .await;
    let (root, _) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(root.pool.as_ref().map(|p| p.graphs.len()), Some(2));

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#>
         DELETE DATA { GRAPH ex:g1 { ex:doc ex:embedding \"[0.1, 0.2, 0.3]\"^^f:embeddingVector . } }",
    )
    .await;
    let count = format!(
        "{PREFIXES}SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH ex:g1 {{ ?s ex:embedding ?v }} }}"
    );
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    assert_eq!(
        select(&fluree, &ledger, &count).await,
        vec!["n=0"],
        "over the pooled base"
    );
    let (_, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(
        select(&fluree, &ledger, &count).await,
        vec!["n=0"],
        "after promotion"
    );
}

/// Pruning `GRAPH ?g` by a probe of the pool must not use graph-local value
/// encodings: an arena-backed decimal has handle 1 in the default graph and
/// handle 0 in the pooled graph, and the pooled graph still matches.
#[tokio::test]
async fn graph_pruning_matches_an_arena_decimal_in_its_own_graph() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let trig = "@prefix ex: <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:d1 ex:amount \"111111111111111111111111111111.111111111\"^^xsd:decimal .
        ex:d2 ex:amount \"222222222222222222222222222222.222222222\"^^xsd:decimal .
        ex:g1 { ex:a ex:amount \"222222222222222222222222222222.222222222\"^^xsd:decimal . }
        ex:g2 { ex:b ex:amount 5 . }
    ";
    let (fluree, ledger) = import_trig(
        db_dir.path(),
        data_dir.path(),
        "test/pool-decimal:main",
        trig,
    )
    .await;
    assert_eq!(
        latest_root(db_dir.path()).pool.map(|p| p.graphs.len()),
        Some(2)
    );
    let target = "\"222222222222222222222222222222.222222222\"^^xsd:decimal";
    let prefixes = format!("{PREFIXES}PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n");
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{prefixes}SELECT ?s WHERE {{ GRAPH ex:g1 {{ ?s ex:amount {target} }} }}")
        )
        .await,
        vec!["s=http://example.org/a"],
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{prefixes}SELECT ?g ?s WHERE {{ GRAPH ?g {{ ?s ex:amount {target} }} }}")
        )
        .await,
        vec!["g=http://example.org/g1 s=http://example.org/a"],
    );
    // The default graph's stats see only decimals for ex:amount; g2 holds an
    // integer, which a bare number matches all the same.
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{prefixes}SELECT ?g ?s WHERE {{ GRAPH ?g {{ ?s ex:amount 5 }} }}")
        )
        .await,
        vec!["g=http://example.org/g2 s=http://example.org/b"],
    );
}

/// A graph's only fact, asserted then retracted before the pooled rebuild,
/// lives only in a history-only leaflet; reading at the assert still sees it.
#[tokio::test]
async fn a_retracted_sole_fact_is_visible_at_its_assert() {
    let db_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-history-only:main";
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree.create_ledger(ledger_id).await.expect("create");
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g1 { ex:a ex:label \"A1\" . } }",
    )
    .await;
    let t1 = fluree.ledger(ledger_id).await.unwrap().t();
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g1 { ex:a ex:label \"A1\" . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(root.pool.as_ref().map(|p| p.graphs.len()), Some(1));
    assert_eq!(
        objects_at(&fluree, &ledger, ledger_id, "g1", Some(t1)).await,
        vec!["ex:a \"A1\""]
    );
    assert_eq!(
        objects_at(&fluree, &ledger, ledger_id, "g1", None).await,
        Vec::<String>::new()
    );
}

/// Query shapes over pooled graphs, checked against a ledger that holds the
/// same transactions unindexed. A fast path that reads "no dedicated branch"
/// as "no facts" disagrees here.
const DIFFERENTIAL_QUERIES: &[&str] = &[
    "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o } }",
    "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g1 { ?s ex:val ?o } }",
    "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o } }",
    "SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o } }",
    "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { GRAPH ex:g1 { ?s ex:val ?o } }",
    "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o FILTER(isLiteral(?o)) } }",
    "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o FILTER(isBlank(?s)) } }",
    "SELECT ?p (COUNT(*) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o } } GROUP BY ?p",
    "SELECT ?c (COUNT(?s) AS ?n) WHERE { GRAPH ex:g1 { ?s a ?c } } GROUP BY ?c",
    "SELECT (MIN(?v) AS ?lo) (MAX(?v) AS ?hi) (SUM(?v) AS ?sum) WHERE { GRAPH ex:g1 { ?s ex:val ?v } }",
    "SELECT (MIN(?l) AS ?lo) (MAX(?l) AS ?hi) WHERE { GRAPH ex:g1 { ?s ex:label ?l } }",
    "SELECT ?s WHERE { GRAPH ex:g1 { ?s a ex:C } }",
    "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g1 { ?s a ex:C } }",
    "SELECT ?s ?o WHERE { GRAPH ex:g1 { ?s ex:knows+ ?o } }",
    "SELECT ?s WHERE { GRAPH ex:g1 { ?s ex:val ?v FILTER(?v > 1) } }",
    "SELECT ?s ?l WHERE { GRAPH ex:g1 { ?s ex:val ?v OPTIONAL { ?s ex:label ?l } } }",
    "SELECT ?s WHERE { GRAPH ex:g1 { ?s a ex:C FILTER NOT EXISTS { ?s ex:label \"A\" } } }",
    "SELECT ?s WHERE { GRAPH ex:g1 { ?s a ex:C MINUS { ?s ex:val 2 } } }",
    "SELECT ?s ?o WHERE { GRAPH ex:g1 { VALUES ?o { 1 2 3 } ?s ex:val ?o } }",
    "SELECT ?s ?v WHERE { GRAPH ex:g1 { ?s ex:val ?v } } ORDER BY DESC(?v) LIMIT 2",
    "SELECT ?s ?x WHERE { GRAPH ex:g1 { ?s ex:knows ?o . ?o ex:knows ?x } }",
    "SELECT ?o ?x WHERE { GRAPH ex:g1 { ?s ex:knows ?o . ?o ex:val ?x } }",
    "SELECT ?p ?o WHERE { GRAPH ex:g1 { ex:a ?p ?o } }",
    "SELECT ?s ?p WHERE { GRAPH ex:g1 { ?s ?p ex:b } }",
    "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g1 { ?s ex:knows ex:b } }",
    "SELECT ?s WHERE { GRAPH ex:g1 { ?s ex:label ?l FILTER(LANG(?l) = \"en\") } }",
    "SELECT ?s ?d WHERE { GRAPH ex:g1 { ?s ex:date ?d FILTER(?d > \"2019-01-01\"^^xsd:date) } }",
    "SELECT ?s WHERE { GRAPH ex:g1 { ?s ex:amount 1.5 } }",
    "SELECT ?s ?x WHERE { GRAPH ex:g1 { ?s ex:val ?v ; ex:label ?l ; ex:knows ?x } }",
    "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s a ex:C } }",
    "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
    "SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:val 10 } }",
    "SELECT ?g ?s ?l WHERE { GRAPH ?g { ?s a ex:C OPTIONAL { ?s ex:label ?l } } }",
];

async fn differential_rows(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> Vec<(String, Vec<String>)> {
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let mut out = Vec::new();
    for q in DIFFERENTIAL_QUERIES {
        let sparql = format!("{PREFIXES}PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n{q}");
        let rows = select(fluree, &ledger, &sparql).await;
        out.push((
            q.to_string(),
            rows.iter().map(|r| ledger_neutral(r)).collect(),
        ));
        // The same shape with g1 as the query's default graph, where whole-query
        // fast paths plan against the pooled graph.
        if let Some(at) = q.find("WHERE { GRAPH ex:g1 { ") {
            let close = q.rfind("} }").expect("GRAPH block closes");
            let body = &q[at + "WHERE { GRAPH ex:g1 { ".len()..close];
            let from = format!(
                "{}FROM <http://example.org/g1> WHERE {{ {body}}}{}",
                &q[..at],
                &q[close + 3..]
            );
            let sparql =
                format!("{PREFIXES}PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n{from}");
            let rows = select(fluree, &ledger, &sparql).await;
            out.push((from, rows.iter().map(|r| ledger_neutral(r)).collect()));
        }
    }
    for graph in ["g1", "g2"] {
        let count = fluree
            .query_connection(&serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "from": {"@id": ledger_id, "graph": format!("http://example.org/{graph}")},
                "select": ["(count ?s)"],
                "where": {"@id": "?s", "@type": "ex:C"}
            }))
            .await
            .expect("jsonld count")
            .to_jsonld(&ledger.snapshot)
            .expect("jsonld");
        out.push((
            format!("jsonld count C in {graph}"),
            vec![count.to_string()],
        ));
    }
    out
}

/// A row with skolemized blank-node labels (`fdb-<ledger hex>-<n>-<label>`)
/// stripped of their ledger-specific part.
fn ledger_neutral(row: &str) -> String {
    let mut out = String::new();
    let mut rest = row;
    while let Some(at) = rest.find("fdb-") {
        out.push_str(&rest[..at + 4]);
        rest = &rest[at + 4..];
        let hex = rest.find('-').unwrap_or(0);
        rest = &rest[hex..];
    }
    out.push_str(rest);
    out
}

fn assert_same_rows(stage: &str, got: &[(String, Vec<String>)], want: &[(String, Vec<String>)]) {
    for ((q, g), (_, w)) in got.iter().zip(want) {
        assert_eq!(g, w, "{stage}: {q}");
    }
}

#[tokio::test]
async fn pooled_graphs_answer_every_shape_like_unindexed_data() {
    let db_dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let (pooled, oracle) = ("test/diff-pooled:main", "test/diff-oracle:main");
    let both = |sparql: &'static str| {
        let fluree = &fluree;
        async move {
            sparql_update(fluree, pooled, sparql).await;
            sparql_update(fluree, oracle, sparql).await;
        }
    };
    fluree.create_ledger(pooled).await.expect("create");
    fluree.create_ledger(oracle).await.expect("create");
    both(
        "PREFIX ex: <http://example.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         INSERT DATA {
           ex:a ex:val 100 . ex:q a ex:C .
           GRAPH ex:g1 {
             ex:a a ex:C ; ex:val 1 ; ex:label \"A\" ; ex:knows ex:b ; ex:amount 1.5 ;
                  ex:date \"2020-01-01\"^^xsd:date .
             ex:b a ex:C ; ex:val 2 ; ex:label \"B\"@en ; ex:knows ex:c .
             ex:c a ex:D ; ex:val 3 ; ex:knows ex:a ; ex:date \"2018-06-01\"^^xsd:date .
             _:x ex:val 4 .
           }
           GRAPH ex:g2 { ex:a ex:val 10 . ex:z a ex:C ; ex:label \"Z\" . }
         }",
    )
    .await;
    let (root, _) = index_and_reload(&fluree, pooled).await;
    assert_eq!(root.pool.as_ref().map(|p| p.graphs.len()), Some(2));
    let want = differential_rows(&fluree, oracle).await;
    assert_same_rows(
        "pooled base",
        &differential_rows(&fluree, pooled).await,
        &want,
    );

    both(
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g1 { ex:b ex:label \"B\"@en . ex:c ex:val 3 . } } ;
         INSERT DATA { GRAPH ex:g1 { ex:d a ex:C ; ex:val 5 ; ex:knows ex:a . }
                       GRAPH ex:g2 { ex:y ex:val 10 . } }",
    )
    .await;
    let want = differential_rows(&fluree, oracle).await;
    assert_same_rows(
        "pooled base + novelty",
        &differential_rows(&fluree, pooled).await,
        &want,
    );

    index_and_reload(&fluree, pooled).await;
    assert_same_rows(
        "after promotion",
        &differential_rows(&fluree, pooled).await,
        &want,
    );
}

/// Graph ids past 16 bits, end to end: 70,000 imported graphs pool, answer
/// by id and through `GRAPH ?g`, take a write over the pooled base, promote,
/// and a new graph past the old limit indexes too.
#[tokio::test]
async fn graph_ids_past_16_bits_import_write_and_index() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/wide-graph-ids:main";
    let mut trig = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..70_000 {
        trig.push_str(&format!("ex:g{i} {{ ex:s{i} ex:val {i} . }}\n"));
    }
    let (fluree, ledger) = import_trig(db_dir.path(), data_dir.path(), ledger_id, &trig).await;
    let pool = latest_root(db_dir.path()).pool.expect("pooled");
    assert!(pool.graphs.iter().any(|&g| g > u32::from(u16::MAX)));

    let in_graph =
        |i: u32| format!("{PREFIXES}SELECT ?s ?v WHERE {{ GRAPH ex:g{i} {{ ?s ex:val ?v }} }}");
    assert_eq!(
        select(&fluree, &ledger, &in_graph(69_999)).await,
        vec!["s=http://example.org/s69999 v=69999"]
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?g WHERE {{ GRAPH ?g {{ ?s ex:val 69999 }} }}")
        )
        .await,
        vec!["g=http://example.org/g69999"]
    );

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g69998 { ex:extra ex:val 1 . }
                       GRAPH ex:brand-new { ex:n ex:val 2 . } }",
    )
    .await;
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let both = vec![
        "s=http://example.org/extra v=1",
        "s=http://example.org/s69998 v=69998",
    ];
    assert_eq!(
        select(&fluree, &ledger, &in_graph(69_998)).await,
        both,
        "over the pool"
    );

    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    let dedicated = dedicated_user_graphs(&root);
    assert_eq!(dedicated.len(), 2, "the promoted graph and the new one");
    assert!(dedicated.iter().all(|&g| g > u32::from(u16::MAX)));
    assert_eq!(
        select(&fluree, &ledger, &in_graph(69_998)).await,
        both,
        "promoted"
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?s WHERE {{ GRAPH ex:brand-new {{ ?s ex:val 2 }} }}")
        )
        .await,
        vec!["s=http://example.org/n"]
    );
}
