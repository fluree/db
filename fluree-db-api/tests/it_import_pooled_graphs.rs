//! Bulk import into the pooled tree: small named graphs share one index tree
//! (each row carrying its graph), large ones keep their own. Queries must see
//! exactly the per-graph contents a tree per graph would give.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::FlureeBuilder;
use fluree_db_binary_index::IndexRoot;
use std::path::Path;
use std::sync::Arc;

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

/// `(rows, history)` the pool records for `ex:{graph}`, `None` when the graph
/// is not pooled.
fn pooled_size(root: &IndexRoot, graph: &str) -> Option<(u32, u32)> {
    let iri = format!("http://example.org/{graph}");
    let g_id = root.graph_iris.iter().position(|g| *g == iri)? as u32 + 1;
    root.pool
        .as_ref()?
        .member(g_id)
        .map(|m| (m.rows, m.history))
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
        pool.members.len(),
        3,
        "all three graphs pooled: {:?}",
        pool.members
    );
    assert!(
        dedicated_user_graphs(&root).is_empty(),
        "no dedicated user graph trees"
    );
    assert_eq!(
        ["g1", "g2", "g3"].map(|g| pooled_size(&root, g)),
        [Some((5, 0)), Some((5, 0)), Some((2, 0))],
        "each member's rows, and no history from an import"
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
    assert_eq!(pool.members.len(), 1);
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
        }
        .with_ledger_config_provider(fluree.ledger_config_provider()),
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

/// A write to a pooled graph updates the pool in place: the graph stays
/// pooled with its new size, the others are untouched, and every graph
/// reads back exactly.
#[tokio::test]
async fn appending_to_a_pooled_graph_updates_the_pool() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-append:main";
    let (fluree, _) = import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g1 { ex:a2 ex:val 10 . ex:same ex:p 0 . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert!(dedicated_user_graphs(&root).is_empty(), "nothing promoted");
    assert_eq!(
        ["g1", "g2", "g3"].map(|g| pooled_size(&root, g)),
        [Some((4, 2)), Some((3, 0)), Some((2, 0))],
        "g1 gained one row; re-asserting ex:same, a fact it already held, \
         replaced its row (the old assert and a same-t retract are history)"
    );

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
    history_events_via(ledger, graph, None).await
}

/// [`history_events`] with the planner's index hint, as a history scan
/// grouped by its object (POST) or under a prefix filter (OPST) gets.
async fn history_events_via(
    ledger: &fluree_db_api::LedgerState,
    graph: &str,
    index_hint: Option<fluree_db_core::IndexType>,
) -> usize {
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
    let mut scan = BinaryHistoryScanOperator::new_with_emit_and_index(
        pattern,
        None,
        Vec::new(),
        fluree_db_query::binary_scan::EmitMask::ALL,
        index_hint,
    );
    scan.open(&ctx).await.expect("open");
    let mut events = 0;
    while let Some(batch) = scan.next_batch(&ctx).await.expect("batch") {
        events += batch.len();
    }
    events
}

/// A retraction in a pooled graph stays in the pool, history and all: an
/// earlier `t` still sees the retracted fact, and the history scan bound to
/// the graph reads both events from the pool.
#[tokio::test]
async fn retracting_from_a_pooled_graph_keeps_its_history_in_the_pool() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-retract:main";
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
    let t_before = ledger.t();

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g2 { ex:b ex:label \"B\" . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert!(dedicated_user_graphs(&root).is_empty(), "nothing promoted");
    assert_eq!(
        pooled_size(&root, "g2"),
        Some((2, 2)),
        "B's row left; its assert and retract are history"
    );
    assert_eq!(pooled_size(&root, "g1"), Some((3, 0)));
    assert_eq!(history_events(&ledger, "g2").await, 2);

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

/// A full rebuild pools small named graphs with their history; a later
/// retraction keeps the graph pooled, and a later pin promotes it carrying
/// that history: every `t` answers as it did when written.
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
        root.pool.as_ref().map(|p| p.members.len()),
        Some(2),
        "the first index is a full rebuild, and it pools both graphs"
    );
    assert_eq!(
        pooled_size(&root, "g1"),
        Some((1, 2)),
        "A2's row; A1's assert and retract"
    );
    assert_eq!(pooled_size(&root, "g2"), Some((1, 0)));
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
        pooled_size(&root, "g1"),
        Some((0, 4)),
        "g1 stays pooled, its last row now history"
    );
    let at = |graph, t| objects_at(&fluree, &ledger, ledger_id, graph, t);
    assert_eq!(at("g1", None).await, Vec::<String>::new());
    assert_eq!(at("g1", Some(t1)).await, vec!["ex:a \"A1\""]);
    assert_eq!(at("g1", Some(t2)).await, vec!["ex:a \"A2\""]);
    assert_eq!(history_events(&ledger, "g1").await, 4);

    // Pinning g1 dedicated promotes it at the next index, with no write to
    // it: its whole history moves to trees of its own and leaves the pool.
    set_graph_placement(
        &fluree,
        ledger_id,
        "PlacementAuto",
        &[("g1", "PlacementDedicated")],
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(pooled_size(&root, "g1"), None, "g1 left the pool");
    assert_eq!(
        dedicated_user_graphs(&root),
        vec![user_graph_id(&root, "g1")]
    );
    assert_eq!(pooled_size(&root, "g2"), Some((1, 0)));
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
            .members
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
    assert_eq!(root.pool.as_ref().map(|p| p.members.len()), Some(2));

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
        latest_root(db_dir.path()).pool.map(|p| p.members.len()),
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
    assert_eq!(root.pool.as_ref().map(|p| p.members.len()), Some(1));
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

/// Every graph's triples at every `t` up to `max_t`, for the graphs that
/// exist by then: what time travel must agree on, pooled or not.
async fn triples_by_t(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graphs: &[(&str, i64)],
    max_t: i64,
) -> Vec<(String, Vec<String>)> {
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let mut out = Vec::new();
    for t in 1..=max_t {
        for &(graph, born) in graphs {
            if t < born {
                continue;
            }
            // Every triple, and an object-bound lookup, which reads the pool
            // in POST order — keyed graph-last.
            let shapes = [
                (
                    serde_json::json!(["?s", "?p", "?o"]),
                    serde_json::json!({"@id": "?s", "?p": "?o"}),
                ),
                (
                    serde_json::json!(["?s"]),
                    serde_json::json!({"@id": "?s", "ex:val": 9}),
                ),
            ];
            for (select, pattern) in shapes {
                let rows = fluree
                    .query_connection(&serde_json::json!({
                        "@context": {"ex": "http://example.org/"},
                        "from": {
                            "@id": ledger_id,
                            "graph": format!("http://example.org/{graph}"),
                            "t": t
                        },
                        "select": select,
                        "where": pattern
                    }))
                    .await
                    .unwrap_or_else(|e| panic!("{graph} at t={t}: {e}"))
                    .to_jsonld(&ledger.snapshot)
                    .expect("jsonld");
                let mut rows: Vec<String> = rows
                    .as_array()
                    .expect("rows")
                    .iter()
                    .map(|r| ledger_neutral(&r.to_string()))
                    .collect();
                rows.sort();
                out.push((format!("{graph} at t={t}: {pattern}"), rows));
            }
        }
    }
    out
}

/// The graphs a pooled tree physically holds: in its GSPO rows, and in its
/// history sidecars.
async fn graphs_in_pool(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    ledger_id: &str,
) -> (
    std::collections::BTreeSet<fluree_db_core::GraphId>,
    std::collections::BTreeSet<fluree_db_core::GraphId>,
) {
    use fluree_db_binary_index::format::history_sidecar::decode_sidecar_entries;
    use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
    use fluree_db_binary_index::{BinaryCursor, BinaryFilter, ColumnProjection, RunSortOrder};

    let store = ledger
        .binary_store
        .as_ref()
        .unwrap()
        .0
        .clone()
        .downcast::<fluree_db_binary_index::BinaryIndexStore>()
        .unwrap();
    let branch = store
        .pool_branch(RunSortOrder::Gspo)
        .cloned()
        .expect("a pool");
    let bound = |low: bool| RunRecordV2 {
        s_id: fluree_db_core::subject_id::SubjectId(if low { 0 } else { u64::MAX }),
        o_key: if low { 0 } else { u64::MAX },
        p_id: if low { 0 } else { u32::MAX },
        t: if low { 0 } else { u32::MAX },
        o_i: if low { 0 } else { u32::MAX },
        o_type: if low { 0 } else { u16::MAX },
        g_id: if low { 0 } else { u32::MAX },
    };
    let mut cursor = BinaryCursor::new(
        Arc::clone(&store),
        RunSortOrder::Gspo,
        Arc::clone(&branch),
        &bound(true),
        &bound(false),
        BinaryFilter::default(),
        ColumnProjection::all(),
    );
    let mut rows = std::collections::BTreeSet::new();
    while let Some(batch) = cursor.next_batch().expect("scan pool") {
        for i in 0..batch.row_count {
            rows.insert(batch.g_id.get_or(i, 0));
        }
    }
    let content = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
    let mut history = std::collections::BTreeSet::new();
    for leaf in &branch.leaves {
        if let Some(cid) = &leaf.sidecar_cid {
            let bytes = content.get(cid).await.expect("sidecar");
            history.extend(
                decode_sidecar_entries(&bytes)
                    .expect("decode sidecar")
                    .iter()
                    .map(|e| e.g_id),
            );
        }
    }
    (rows, history)
}

/// Pooled graphs answer exactly like never-indexed data through their whole
/// lifecycle — a pooled base, novelty over it, in-place pool updates, a new
/// graph joining with triples another graph also holds, retractions, a pin
/// promoting a graph out of the pool, writes on both sides afterwards, a
/// storage sweep, and a reopen — in current state and at every earlier `t`.
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
    let check = |stage: &'static str, graphs: &'static [(&'static str, i64)]| {
        let fluree = &fluree;
        async move {
            let t = fluree.ledger(oracle).await.expect("load").t();
            assert_same_rows(
                stage,
                &differential_rows(fluree, pooled).await,
                &differential_rows(fluree, oracle).await,
            );
            assert_same_rows(
                stage,
                &triples_by_t(fluree, pooled, graphs, t).await,
                &triples_by_t(fluree, oracle, graphs, t).await,
            );
        }
    };
    const G12: &[(&str, i64)] = &[("g1", 1), ("g2", 1)];
    const G123: &[(&str, i64)] = &[("g1", 1), ("g2", 1), ("g3", 4)];
    fluree.create_ledger(pooled).await.expect("create");
    fluree.create_ledger(oracle).await.expect("create");
    // t=1
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
    assert_eq!(root.pool.as_ref().map(|p| p.members.len()), Some(2));
    check("pooled base", G12).await;

    // t=2
    both(
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g1 { ex:b ex:label \"B\"@en . ex:c ex:val 3 . } } ;
         INSERT DATA { GRAPH ex:g1 { ex:d a ex:C ; ex:val 5 ; ex:knows ex:a . }
                       GRAPH ex:g2 { ex:y ex:val 10 . } }",
    )
    .await;
    check("pooled base + novelty", G12).await;
    let (root, _) = index_and_reload(&fluree, pooled).await;
    assert!(dedicated_user_graphs(&root).is_empty());
    check("pool updated in place", G12).await;

    // t=3: g2 loses Z and gains triples g1 also holds; t=4: g3 joins with
    // some of the same.
    both(
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g2 { ex:z ex:label \"Z\" . } } ;
         INSERT DATA { GRAPH ex:g2 { ex:a ex:val 1 . ex:a a ex:C . } }",
    )
    .await;
    both(
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g3 { ex:a ex:val 1 ; ex:label \"A\" . ex:e ex:val 7 . } }",
    )
    .await;
    let (root, _) = index_and_reload(&fluree, pooled).await;
    assert_eq!(pooled_size(&root, "g3"), Some((3, 0)), "g3 joined the pool");
    assert!(dedicated_user_graphs(&root).is_empty());
    check("a graph joined", G123).await;

    // t=5: pin g1 dedicated; the next index promotes it and purges it from
    // the pool, rows and history.
    for ledger_id in [pooled, oracle] {
        set_graph_placement(
            &fluree,
            ledger_id,
            "PlacementAuto",
            &[("g1", "PlacementDedicated")],
        )
        .await;
    }
    let (root, ledger) = index_and_reload(&fluree, pooled).await;
    let g1 = user_graph_id(&root, "g1");
    assert_eq!(dedicated_user_graphs(&root), vec![g1]);
    let (rows, history) = graphs_in_pool(&fluree, &ledger, pooled).await;
    assert!(
        !rows.contains(&g1) && !history.contains(&g1),
        "g1 purged from the pool: rows {rows:?}, history {history:?}"
    );
    assert!(rows.contains(&user_graph_id(&root, "g2")));
    assert!(history.contains(&user_graph_id(&root, "g2")));
    check("promoted", G123).await;

    // t=6: writes on both sides of the move.
    both(
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g3 { ex:a ex:val 1 . } GRAPH ex:g1 { ex:a ex:label \"A\" . } } ;
         INSERT DATA { GRAPH ex:g1 { ex:f a ex:C . } GRAPH ex:g2 { ex:f ex:val 10 . } }",
    )
    .await;
    let (root, _) = index_and_reload(&fluree, pooled).await;
    assert_eq!(dedicated_user_graphs(&root), vec![g1]);
    check("written after the move", G123).await;

    // t=7, t=8, one window: the same triple in two pooled graphs, born and
    // deleted in g3 while g2 keeps it — events the graph-last orders must
    // keep apart.
    both(
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g2 { ex:h ex:val 9 . } GRAPH ex:g3 { ex:h ex:val 9 . } }",
    )
    .await;
    both(
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g3 { ex:h ex:val 9 . } }",
    )
    .await;
    index_and_reload(&fluree, pooled).await;
    check("same triple, two graphs, one window", G123).await;

    fluree
        .sweep_index_storage("test/diff-pooled")
        .await
        .expect("sweep");
    drop(fluree);
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("reopen");
    let t = fluree.ledger(oracle).await.expect("load").t();
    assert_same_rows(
        "swept and reopened",
        &differential_rows(&fluree, pooled).await,
        &differential_rows(&fluree, oracle).await,
    );
    assert_same_rows(
        "swept and reopened",
        &triples_by_t(&fluree, pooled, G123, t).await,
        &triples_by_t(&fluree, oracle, G123, t).await,
    );
}

/// Graph ids past 16 bits, end to end: 70,000 imported graphs pool, answer
/// by id and through `GRAPH ?g`, take a write over the pooled base, a new
/// graph past the old limit joins the pool, and a pinned one is promoted.
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
    assert!(pool.graph_ids().any(|g| g > u32::from(u16::MAX)));

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
    assert!(dedicated_user_graphs(&root).is_empty());
    let brand_new = user_graph_id(&root, "brand-new");
    assert!(brand_new > u32::from(u16::MAX));
    assert_eq!(pooled_size(&root, "brand-new"), Some((1, 0)), "joined");
    assert_eq!(pooled_size(&root, "g69998"), Some((2, 0)));
    let new_graph = format!("{PREFIXES}SELECT ?s WHERE {{ GRAPH ex:brand-new {{ ?s ex:val 2 }} }}");
    assert_eq!(select(&fluree, &ledger, &in_graph(69_998)).await, both);
    assert_eq!(
        select(&fluree, &ledger, &new_graph).await,
        vec!["s=http://example.org/n"]
    );

    set_graph_placement(
        &fluree,
        ledger_id,
        "PlacementAuto",
        &[("g69998", "PlacementDedicated")],
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(
        dedicated_user_graphs(&root),
        vec![user_graph_id(&root, "g69998")]
    );
    assert_eq!(
        select(&fluree, &ledger, &in_graph(69_998)).await,
        both,
        "promoted"
    );
    assert_eq!(
        select(&fluree, &ledger, &new_graph).await,
        vec!["s=http://example.org/n"]
    );
}

/// `?s ex:label ?o` in the default graph, as of `t` (`None`: current).
async fn default_labels_at(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    ledger_id: &str,
    t: Option<i64>,
) -> Vec<String> {
    let mut from = serde_json::json!({"@id": ledger_id});
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

/// A fact asserted and retracted within the window that writes a graph's
/// first rows stays retracted, and keeps its lived interval for an earlier
/// `t` — for a new graph joining the pool, a new graph with trees of its
/// own, and a default graph with no leaves (a TriG import with no
/// default-graph triples writes none).
#[tokio::test]
async fn a_fact_retracted_in_a_trees_first_window_stays_retracted() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/first-window-retract:main";
    let (fluree, _) = import_trig(
        db_dir.path(),
        data_dir.path(),
        ledger_id,
        "@prefix ex: <http://example.org/> .\nex:seed { ex:s ex:label \"seed\" . }\n",
    )
    .await;
    // The default graph has no leaves.
    let base = latest_root(db_dir.path());
    assert!(base
        .default_graph_orders
        .iter()
        .all(|o| o.leaves.is_empty()));
    // t=2: ex:own gets trees of its own; ex:fresh joins the pool.
    set_graph_placement(
        &fluree,
        ledger_id,
        "PlacementAuto",
        &[("own", "PlacementDedicated")],
    )
    .await;
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA {
           ex:x ex:label \"X\" . ex:y ex:label \"Y\" .
           GRAPH ex:fresh { ex:x ex:label \"X\" . ex:y ex:label \"Y\" . }
           GRAPH ex:own { ex:x ex:label \"X\" . ex:y ex:label \"Y\" . }
         }",
    )
    .await;
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA {
           ex:x ex:label \"X\" .
           GRAPH ex:fresh { ex:x ex:label \"X\" . }
           GRAPH ex:own { ex:x ex:label \"X\" . }
         }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(
        dedicated_user_graphs(&root),
        vec![user_graph_id(&root, "own")]
    );
    assert_eq!(pooled_size(&root, "fresh"), Some((1, 2)));

    let y = vec![r#"ex:y "Y""#.to_string()];
    let xy = vec![r#"ex:x "X""#.to_string(), r#"ex:y "Y""#.to_string()];
    for graph in ["fresh", "own"] {
        assert_eq!(
            objects_at(&fluree, &ledger, ledger_id, graph, None).await,
            y,
            "{graph}"
        );
        assert_eq!(
            objects_at(&fluree, &ledger, ledger_id, graph, Some(3)).await,
            xy,
            "{graph}"
        );
    }
    assert_eq!(
        default_labels_at(&fluree, &ledger, ledger_id, None).await,
        y
    );
    assert_eq!(
        default_labels_at(&fluree, &ledger, ledger_id, Some(3)).await,
        xy
    );
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT ?p ?o WHERE {{ ex:y ?p ?o }}")
        )
        .await,
        vec!["o=Y p=http://example.org/label"],
    );
}

/// Write `f:graphPlacement` into the ledger's config graph: `default` for
/// every user named graph, and `overrides` as `(graph, placement)` pairs.
async fn set_graph_placement(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    default: &str,
    overrides: &[(&str, &str)],
) {
    let mut body = format!(
        "<urn:config:main> a f:LedgerConfig ;
           f:indexingDefaults [ f:graphPlacement f:{default} ] .\n"
    );
    for (graph, placement) in overrides {
        body.push_str(&format!(
            "<urn:config:main> f:graphOverrides <urn:config:{graph}> .
             <urn:config:{graph}> a f:GraphConfig ;
               f:targetGraph ex:{graph} ;
               f:indexingDefaults [ f:graphPlacement f:{placement} ] .\n"
        ));
    }
    sparql_update(
        fluree,
        ledger_id,
        &format!(
            "PREFIX ex: <http://example.org/>
             PREFIX f: <https://ns.flur.ee/db#>
             INSERT DATA {{ GRAPH <urn:fluree:{ledger_id}#config> {{ {body} }} }}"
        ),
    )
    .await;
}

fn user_graph_id(root: &IndexRoot, graph: &str) -> fluree_db_core::GraphId {
    let iri = format!("http://example.org/{graph}");
    root.graph_iris
        .iter()
        .position(|g| *g == iri)
        .expect("registered graph") as fluree_db_core::GraphId
        + 1
}

/// `f:graphPlacement` places named graphs: a `PlacementDedicated` ledger
/// default keeps every graph in its own trees, and a `PlacementAuto` graph
/// override lets that one pool. Both a ledger's first index and a reindex
/// apply it, and every graph reads back the same.
#[tokio::test]
async fn ledger_config_places_named_graphs() {
    let db_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/placement-config:main";
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree.create_ledger(ledger_id).await.expect("create");
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g1 { ex:a ex:label \"A\" . }
                       GRAPH ex:g2 { ex:b ex:label \"B\" . }
                       GRAPH ex:g3 { ex:c ex:label \"C\" . } }",
    )
    .await;
    set_graph_placement(
        &fluree,
        ledger_id,
        "PlacementDedicated",
        &[("g2", "PlacementAuto")],
    )
    .await;

    let check = |root: &IndexRoot| {
        let pooled: Vec<_> = root
            .pool
            .as_ref()
            .map(|p| p.graph_ids().collect())
            .unwrap_or_default();
        assert_eq!(pooled, vec![user_graph_id(root, "g2")]);
        let mut dedicated = dedicated_user_graphs(root);
        dedicated.sort_unstable();
        assert_eq!(
            dedicated,
            vec![user_graph_id(root, "g1"), user_graph_id(root, "g3")]
        );
    };
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    check(&root);
    let info = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger info");
    let placement = |graph: &str| {
        info["ledger"]["named-graphs"]
            .as_array()
            .expect("named graphs")
            .iter()
            .find(|g| g["iri"] == format!("http://example.org/{graph}"))
            .map(|g| g["placement"].clone())
    };
    assert_eq!(
        ["g1", "g2", "g3"].map(placement),
        ["dedicated", "pooled", "dedicated"].map(|p| Some(serde_json::json!(p))),
        "ledger info reports where the index holds each graph"
    );
    for (graph, label) in [
        ("g1", "ex:a \"A\""),
        ("g2", "ex:b \"B\""),
        ("g3", "ex:c \"C\""),
    ] {
        assert_eq!(
            objects_at(&fluree, &ledger, ledger_id, graph, None).await,
            vec![label.to_string()]
        );
    }

    fluree
        .reindex(ledger_id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");
    check(&latest_root(db_dir.path()));
}

/// A pooled graph that outgrows the pool is promoted at the next index —
/// rows, history and this window's events — and the pool drops it while
/// the other graphs stay pooled.
#[tokio::test]
async fn a_pooled_graph_that_outgrows_the_pool_is_promoted() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-outgrow:main";
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
    let t_import = ledger.t();
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g2 { ex:b ex:label \"B\" . } }",
    )
    .await;
    let rows = fluree_db_indexer::run_index::build::graph_scope::POOL_MAX_GRAPH_ROWS;
    let mut insert = String::from("PREFIX ex: <http://example.org/> INSERT DATA { GRAPH ex:g2 {");
    for i in 0..rows {
        insert.push_str(&format!(" ex:n{i} ex:val {i} ."));
    }
    insert.push_str(" } }");
    sparql_update(&fluree, ledger_id, &insert).await;

    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    let g2 = user_graph_id(&root, "g2");
    assert_eq!(dedicated_user_graphs(&root), vec![g2]);
    assert_eq!(pooled_size(&root, "g1"), Some((3, 0)));
    assert_eq!(pooled_size(&root, "g3"), Some((2, 0)));
    let (in_rows, in_history) = graphs_in_pool(&fluree, &ledger, ledger_id).await;
    assert!(!in_rows.contains(&g2) && !in_history.contains(&g2));
    assert_eq!(
        select(
            &fluree,
            &ledger,
            &format!("{PREFIXES}SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH ex:g2 {{ ?s ?p ?o }} }}")
        )
        .await,
        vec![format!("n={}", rows + 2)]
    );
    assert_eq!(
        objects_at(&fluree, &ledger, ledger_id, "g2", Some(t_import)).await,
        vec!["ex:b \"B\""],
        "the history it carried out of the pool"
    );
}

/// An index without a pool starts one when a small named graph first
/// arrives, keeping that window's history, and later writes update it.
#[tokio::test]
async fn an_index_without_a_pool_starts_one() {
    let db_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-start:main";
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree.create_ledger(ledger_id).await.expect("create");
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/> INSERT DATA { ex:d ex:label \"D\" . }",
    )
    .await;
    let (root, _) = index_and_reload(&fluree, ledger_id).await;
    assert!(root.pool.is_none());

    // t=2, t=3: one window.
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g1 { ex:a ex:label \"A1\" . ex:b ex:label \"B\" . } }",
    )
    .await;
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g1 { ex:a ex:label \"A1\" . } } ;
         INSERT DATA { GRAPH ex:g1 { ex:a ex:label \"A2\" . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(pooled_size(&root, "g1"), Some((2, 2)));
    let at = |t| objects_at(&fluree, &ledger, ledger_id, "g1", t);
    assert_eq!(at(Some(2)).await, vec!["ex:a \"A1\"", "ex:b \"B\""]);
    assert_eq!(at(None).await, vec!["ex:a \"A2\"", "ex:b \"B\""]);

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g2 { ex:c ex:label \"C\" . } GRAPH ex:g1 { ex:e ex:label \"E\" . } }",
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert_eq!(pooled_size(&root, "g1"), Some((3, 2)));
    assert_eq!(pooled_size(&root, "g2"), Some((1, 0)));
    assert_eq!(
        objects_at(&fluree, &ledger, ledger_id, "g1", Some(2)).await,
        vec!["ex:a \"A1\"", "ex:b \"B\""]
    );
    assert_eq!(
        objects_at(&fluree, &ledger, ledger_id, "g2", None).await,
        vec!["ex:c \"C\""]
    );
}

/// Row counts of every leaf of every pooled order.
async fn pooled_leaf_rows(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    root: &IndexRoot,
) -> Vec<u64> {
    let store = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
    let mut rows = Vec::new();
    for (_, cid) in &root.pool.as_ref().expect("a pool").orders {
        let branch = fluree_db_binary_index::format::branch::read_branch_from_bytes(
            &store.get(cid).await.expect("branch"),
        )
        .expect("decode branch");
        rows.extend(branch.leaves.iter().map(|l| l.row_count));
    }
    rows
}

/// A pooled leaf holds about one leaflet's rows, from the import on and
/// through incremental updates, so a write rewrites the few leaflets it
/// lands in rather than most of the pool.
#[tokio::test]
async fn pooled_leaves_hold_one_leaflet() {
    const LEAFLET: usize = 2;
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-leaves:main";
    let dir = data_dir.path().join("import");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("part-00.trig"), THREE_GRAPHS).unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree
        .create(ledger_id)
        .import(&dir)
        .threads(1)
        .leaflet_rows(LEAFLET)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let root = latest_root(db_dir.path());
    let rows = pooled_leaf_rows(&fluree, ledger_id, &root).await;
    // A leaf closes once it reaches a leaflet's rows, so one cut short at a
    // segment boundary can share it with the next.
    assert!(
        rows.iter().all(|&n| n < 2 * LEAFLET as u64) && rows.len() > 10,
        "imported pooled leaves: {rows:?}"
    );

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g1 { ex:a3 ex:val 30 . ex:a4 ex:val 40 . ex:a5 ex:val 50 . } }",
    )
    .await;
    let store = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
    let result = fluree_db_indexer::build_index_for_ledger(
        store.clone(),
        fluree.nameservice(),
        ledger_id,
        fluree_db_indexer::IndexerConfig::default()
            .with_leaflet_rows(LEAFLET)
            .with_incremental_enabled(true),
    )
    .await
    .expect("index");
    let root = IndexRoot::decode(&store.get(&result.root_id).await.expect("root")).unwrap();
    assert!(
        dedicated_user_graphs(&root).is_empty(),
        "an incremental pool update"
    );
    let rows = pooled_leaf_rows(&fluree, ledger_id, &root).await;
    assert!(
        rows.iter().all(|&n| n <= 2 * LEAFLET as u64),
        "a merge keeps pooled leaves within twice a leaflet: {rows:?}"
    );
}

/// A whole read of one pooled graph routes the GSPO leaves holding that
/// graph's rows, not every leaf of the pool: GSPO leads with the graph.
#[tokio::test(flavor = "current_thread")]
async fn a_whole_pooled_graph_read_routes_its_own_leaves() {
    const LEAFLET: usize = 20;
    let trig: String = (0..200)
        .map(|g| {
            let quads: String = (0..5)
                .map(|i| format!("ex:s{g}_{i} ex:val {i} . "))
                .collect();
            format!("ex:g{g} {{ {quads}}}\n")
        })
        .collect();
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-whole-graph:main";
    let dir = data_dir.path().join("import");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("part-00.trig"),
        format!("@prefix ex: <http://example.org/> .\n{trig}"),
    )
    .unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree
        .create(ledger_id)
        .import(&dir)
        .threads(1)
        .leaflet_rows(LEAFLET)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let root = latest_root(db_dir.path());
    assert!(dedicated_user_graphs(&root).is_empty());
    let store = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
    let (_, gspo) = root
        .pool
        .as_ref()
        .expect("a pool")
        .orders
        .iter()
        .find(|(order, _)| *order == fluree_db_binary_index::RunSortOrder::Gspo)
        .expect("a pooled GSPO");
    let pool_leaves = fluree_db_binary_index::format::branch::read_branch_from_bytes(
        &store.get(gspo).await.expect("branch"),
    )
    .expect("decode branch")
    .leaves
    .len();
    assert!(pool_leaves > 10, "pooled GSPO leaves: {pool_leaves}");

    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    for sparql in [
        "SELECT ?s ?p ?o WHERE { GRAPH ex:g100 { ?s ?p ?o } }",
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g100 { ?s ?p ?o } }",
        "SELECT ?s ?p ?o FROM <http://example.org/g100> WHERE { ?s ?p ?o }",
    ] {
        let (spans, guard) = support::span_capture::init_test_tracing();
        let rows = select(&fluree, &ledger, &format!("{PREFIXES}{sparql}")).await;
        drop(guard);
        assert!(rows == ["n=5"] || rows.len() == 5, "{sparql}: {rows:?}");
        let leaves: Vec<usize> = spans
            .find_events("BinaryScanOperator::open")
            .iter()
            .filter_map(|e| e.fields.get("leaves")?.parse().ok())
            .collect();
        assert!(
            !leaves.is_empty() && leaves.iter().all(|&n| n <= 2),
            "{sparql}: leaves routed per scan {leaves:?} of {pool_leaves}"
        );
    }
}

/// A pooled graph's indexed lists read as RDF lists in SPARQL, for a bound
/// owner and inside `GRAPH ?g`, and as members of their predicate in JSON-LD:
/// the list view finds their predicates and owners in the pool.
#[tokio::test]
async fn pooled_lists_read_as_rdf_lists() {
    const LISTS: &str = "
@prefix ex: <http://example.org/> .
ex:g1 { ex:s ex:items ( 1 2 ) . }
ex:g2 { ex:t ex:items ( 3 ) . }
";
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-lists:main";
    let (fluree, _) = import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
    // Import refuses a collection inside a GRAPH block; a transaction stores
    // it as a list.
    fluree
        .graph(ledger_id)
        .transact()
        .insert_turtle(LISTS)
        .index_config(fluree_db_api::IndexConfig {
            reindex_min_bytes: usize::MAX,
            reindex_max_bytes: usize::MAX,
        })
        .commit()
        .await
        .expect("insert lists");
    let (root, ledger) = index_and_reload(&fluree, ledger_id).await;
    assert!(dedicated_user_graphs(&root).is_empty());
    assert!(pooled_size(&root, "g1").is_some() && pooled_size(&root, "g2").is_some());

    let rdf = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";
    let heads = select(
        &fluree,
        &ledger,
        &format!("{PREFIXES}SELECT ?o WHERE {{ GRAPH ex:g1 {{ ex:s ex:items ?o }} }}"),
    )
    .await;
    assert!(
        heads.len() == 1 && !["o=1", "o=2"].contains(&heads[0].as_str()),
        "the owner's edge reaches the list's first cell: {heads:?}"
    );
    let items = select(
        &fluree,
        &ledger,
        &format!(
            "{PREFIXES}{rdf}SELECT ?o WHERE {{ GRAPH ex:g1 {{ ex:s ex:items/rdf:rest*/rdf:first ?o }} }}"
        ),
    )
    .await;
    assert_eq!(items, ["o=1", "o=2"]);
    let firsts = select(
        &fluree,
        &ledger,
        &format!(
            "{PREFIXES}{rdf}SELECT ?g ?o WHERE {{ GRAPH ?g {{ ?s ex:items ?l . ?l rdf:first ?o }} }}"
        ),
    )
    .await;
    assert_eq!(
        firsts,
        ["g=http://example.org/g1 o=1", "g=http://example.org/g2 o=3"]
    );

    let members = support::query_jsonld(
        &fluree,
        &ledger,
        &serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "select": "?o",
            "where": [["graph", "http://example.org/g1", {"@id": "ex:s", "ex:items": "?o"}]]
        }),
    )
    .await
    .expect("JSON-LD query")
    .to_jsonld(&ledger.snapshot)
    .expect("JSON-LD rows");
    let mut members: Vec<i64> = members
        .as_array()
        .expect("rows")
        .iter()
        .map(|v| v.as_i64().expect("an integer member"))
        .collect();
    members.sort_unstable();
    assert_eq!(members, [1, 2]);
}

/// `GRAPH ?g` over pooled graphs: rows of `sparql` with the quad path open,
/// whether it engaged, and the rows with it (and every fast path) off.
async fn quad_and_per_graph(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    sparql: &str,
) -> (Vec<String>, bool, Vec<String>) {
    let sparql = format!("{PREFIXES}{sparql}");
    let (spans, guard) = support::span_capture::init_test_tracing();
    // A callsite first reached under another test's subscriber keeps that
    // interest unless rebuilt.
    tracing::callsite::rebuild_interest_cache();
    fluree_db_api::set_fast_paths_disabled(false);
    let quad = select(fluree, ledger, &sparql).await;
    let fired = spans.has_event("quad block engaged");
    drop(guard);
    fluree_db_api::set_fast_paths_disabled(true);
    let per_graph = select(fluree, ledger, &sparql).await;
    fluree_db_api::set_fast_paths_disabled(false);
    (quad, fired, per_graph)
}

/// A `GRAPH ?g` block of triple patterns and graph-free filters runs as quad
/// scans over the pooled graphs without novelty, and answers as the
/// per-graph path does — with novelty in one graph too, which then runs per
/// graph beside the quad scans. Blocks the quad path cannot answer exactly
/// never engage it.
#[tokio::test(flavor = "current_thread")]
async fn graph_blocks_over_pooled_graphs_run_as_quad_scans() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-quad:main";
    let (fluree, ledger) = import_trig(
        db_dir.path(),
        data_dir.path(),
        ledger_id,
        r#"
@prefix ex: <http://example.org/> .
ex:shared ex:val 99 .
ex:g1 { ex:a ex:val 1 ; ex:label "A" ; ex:knows ex:b . ex:b ex:val 2 . ex:same ex:p 0 . }
ex:g2 { ex:a ex:val 1 ; ex:label "A2" . ex:b ex:val 3 ; ex:knows ex:a . ex:same ex:p 0 . }
ex:g3 { ex:c ex:val 3 ; ex:label "C" . ex:a ex:knows ex:b . }
"#,
    )
    .await;

    let engaged = [
        "SELECT ?g ?s ?o WHERE { GRAPH ?g { ?s ex:val ?o } }",
        "SELECT ?g ?s ?v ?l WHERE { GRAPH ?g { ?s ex:val ?v ; ex:label ?l } }",
        "SELECT ?g WHERE { GRAPH ?g { ex:same ex:p 0 } }",
        "SELECT ?g ?s ?v WHERE { GRAPH ?g { ?s ex:val ?v FILTER(?v > 1) } }",
        "SELECT ?g ?x ?v WHERE { GRAPH ?g { ?s ex:knows ?x . ?x ex:val ?v } }",
        "SELECT ?g (COUNT(?s) AS ?n) WHERE { GRAPH ?g { ?s ex:val ?v } } GROUP BY ?g",
    ];
    let declined = [
        // A filter in the block runs before ?g is bound.
        "SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:val ?v FILTER(?g = ex:g1) } }",
        "SELECT ?g ?s ?l WHERE { GRAPH ?g { ?s ex:val ?v OPTIONAL { ?s ex:label ?l } } }",
        "SELECT ?g ?s WHERE { ?s ex:val 99 . GRAPH ?g { ?s ex:val ?v } }",
        "SELECT ?g ?p WHERE { GRAPH ?g { ex:a ?p ?o } }",
        // The probe rules most graphs out: they run one by one.
        "SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:val 1000 ; ex:label ?l } }",
        // A row goal: the per-graph path stops early.
        "SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:val ?o } } LIMIT 2",
    ];
    for (queries, must_fire) in [(&engaged[..], true), (&declined[..], false)] {
        for q in queries {
            let (quad, fired, per_graph) = quad_and_per_graph(&fluree, &ledger, q).await;
            assert_eq!(fired, must_fire, "{q}");
            assert_eq!(quad, per_graph, "{q}");
        }
    }
    let (rows, _, _) = quad_and_per_graph(&fluree, &ledger, engaged[1]).await;
    assert_eq!(
        rows,
        vec![
            "g=http://example.org/g1 l=A s=http://example.org/a v=1",
            "g=http://example.org/g2 l=A2 s=http://example.org/a v=1",
            "g=http://example.org/g3 l=C s=http://example.org/c v=3",
        ],
        "one row per graph's own pairing: the shared subject never joins across graphs"
    );

    // Novelty in g1: g2 and g3 still run as quad scans, g1 per graph.
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g1 { ex:n ex:val 7 ; ex:label \"N\" . } }",
    )
    .await;
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    for q in &engaged[..2] {
        let (quad, fired, per_graph) = quad_and_per_graph(&fluree, &ledger, q).await;
        assert!(fired, "{q}");
        assert_eq!(quad, per_graph, "{q}");
        assert!(
            quad.iter().any(|r| r.contains("http://example.org/n")),
            "{q}: {quad:?}"
        );
    }
}

/// A filter on transaction time reads flake metadata a quad scan does not
/// carry, so the block runs per graph and keeps every row.
#[tokio::test(flavor = "current_thread")]
async fn a_transaction_time_filter_keeps_its_rows() {
    let db = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let (fluree, ledger) = import_trig(
        db.path(),
        data.path(),
        "test/review-quad-t:main",
        THREE_GRAPHS,
    )
    .await;
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?g", "?s", "?v"],
        "where": [["graph", "?g",
            {"@id": "?s", "ex:val": "?v"},
            ["filter", "(> (t ?v) 0)"]
        ]]
    });
    fluree_db_api::set_fast_paths_disabled(false);
    let quad = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    fluree_db_api::set_fast_paths_disabled(true);
    let per_graph = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    fluree_db_api::set_fast_paths_disabled(false);
    assert_eq!(quad, per_graph);
    assert_eq!(quad.as_array().unwrap().len(), 3);
}

/// The quad path charges what it buffers to the query's memory budget, so
/// an over-budget block fails with `MemoryBudgetExceeded` rather than growing
/// without bound.
#[tokio::test(flavor = "current_thread")]
async fn quad_buffers_count_against_the_memory_budget() {
    use fluree_db_query::{
        binding::Batch,
        context::ExecutionContext,
        ir::{GraphName, Pattern, Ref, Term, TriplePattern},
        operator::Operator,
        quad_block::QuadBlockOperator,
        seed::BatchSeedOperator,
        PlanningContext, VarRegistry,
    };
    let db = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let (_, ledger) = import_trig(
        db.path(),
        data.path(),
        "test/review-quad-memory:main",
        THREE_GRAPHS,
    )
    .await;
    let store = ledger
        .binary_store
        .as_ref()
        .unwrap()
        .0
        .clone()
        .downcast::<fluree_db_binary_index::BinaryIndexStore>()
        .unwrap();
    let mut vars = VarRegistry::new();
    let g = vars.get_or_insert("?g");
    let s = vars.get_or_insert("?s");
    let o = vars.get_or_insert("?o");
    let cancellation = fluree_db_core::QueryCancellation::new();
    // Room for the one parent row, not for a scan's rows.
    cancellation.set_memory_limit(2 * fluree_db_query::context::BINDING_EST_BYTES);
    let ctx = ExecutionContext::new(&ledger.snapshot, &vars)
        .with_binary_store(store, 0)
        .with_cancellation(cancellation);
    let mut op = QuadBlockOperator::new(
        Box::new(BatchSeedOperator::from_batch(Batch::empty_schema_with_len(
            1,
        ))),
        GraphName::Var(g),
        vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(s),
            Ref::Sid(
                ledger
                    .snapshot
                    .encode_iri("http://example.org/val")
                    .unwrap(),
            ),
            Term::Var(o),
        ))],
        PlanningContext::current(),
    );
    let result = op.open(&ctx).await;
    let mut rows = 0;
    if result.is_ok() {
        while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
            rows += batch.len();
        }
    }
    op.close();
    assert!(
        matches!(
            result,
            Err(fluree_db_query::error::QueryError::MemoryBudgetExceeded { .. })
        ),
        "result={result:?}, retained charge={}, rows={rows}",
        ctx.mem_used()
    );
}

/// Writes to pooled graphs merge into GSPO and land in delta runs of the
/// four secondary orders, whose bases stay as they were: every read through
/// those orders — object lookups, quad scans, an earlier `t`, history —
/// still sees them. A graph leaving the pool compacts the runs into the
/// bases.
#[tokio::test]
async fn pooled_secondary_orders_take_writes_as_delta_runs() {
    use fluree_db_binary_index::RunSortOrder;
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-delta:main";
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
    let t_import = ledger.t();
    let imported = latest_root(db_dir.path()).pool.expect("pooled");
    let secondary = [
        RunSortOrder::SpotG,
        RunSortOrder::PsotG,
        RunSortOrder::PostG,
        RunSortOrder::OpstG,
    ];
    let branch = |pool: &fluree_db_binary_index::PoolRouting, order| {
        pool.orders
            .iter()
            .find(|(o, _)| *o == order)
            .map(|(_, cid)| cid.clone())
    };

    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g2 { ex:z ex:label \"Z\" . ex:z ex:val 20 . } }",
    )
    .await;
    let t_added = fluree.ledger(ledger_id).await.unwrap().t();
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g2 { ex:z ex:label \"Z\" . } }",
    )
    .await;
    let (root, _) = index_and_reload(&fluree, ledger_id).await;
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g3 { ex:w ex:val 30 . } }",
    )
    .await;
    let (root2, ledger) = index_and_reload(&fluree, ledger_id).await;
    for pool in [root.pool.as_ref().unwrap(), root2.pool.as_ref().unwrap()] {
        assert_ne!(
            branch(pool, RunSortOrder::Gspo),
            branch(&imported, RunSortOrder::Gspo)
        );
        for order in secondary {
            assert_eq!(
                branch(pool, order),
                branch(&imported, order),
                "{order:?} base"
            );
        }
    }
    for order in secondary {
        assert_eq!(
            root.pool.as_ref().unwrap().delta(order).len(),
            1,
            "{order:?}"
        );
        assert_eq!(
            root2.pool.as_ref().unwrap().delta(order).len(),
            2,
            "{order:?}"
        );
    }
    let reachable = root2.all_cas_ids();
    assert!(root2
        .pool_delta_runs()
        .all(|run| reachable.contains(&run.cid)));

    let check = |ledger: fluree_db_api::LedgerState| {
        let fluree = &fluree;
        async move {
            // A value range reads the pooled POST; an object alone the pooled
            // OPST; GRAPH ?g every pooled order as quad scans.
            assert_eq!(
                select(
                    fluree,
                    &ledger,
                    &format!(
                        "{PREFIXES}SELECT ?s ?v WHERE {{ GRAPH ex:g2 {{ ?s ex:val ?v FILTER(?v > 5) }} }}"
                    )
                )
                .await,
                vec!["s=http://example.org/z v=20"]
            );
            assert_eq!(
                select(
                    fluree,
                    &ledger,
                    &format!("{PREFIXES}SELECT ?s ?p WHERE {{ GRAPH ex:g3 {{ ?s ?p 30 }} }}")
                )
                .await,
                vec!["p=http://example.org/val s=http://example.org/w"]
            );
            assert_eq!(
                select(
                    fluree,
                    &ledger,
                    &format!("{PREFIXES}SELECT ?g ?s WHERE {{ GRAPH ?g {{ ?s ex:val ?v FILTER(?v >= 20) }} }}")
                )
                .await,
                vec![
                    "g=http://example.org/g2 s=http://example.org/z",
                    "g=http://example.org/g3 s=http://example.org/w"
                ]
            );
            assert_eq!(
                objects_at(fluree, &ledger, ledger_id, "g2", Some(t_added)).await,
                vec!["ex:b \"B\"", "ex:z \"Z\""]
            );
            assert_eq!(
                objects_at(fluree, &ledger, ledger_id, "g2", Some(t_import)).await,
                vec!["ex:b \"B\""]
            );
            assert_eq!(
                objects_at(fluree, &ledger, ledger_id, "g2", None).await,
                vec!["ex:b \"B\""]
            );
        }
    };
    check(ledger.clone()).await;
    // Value-ordered history scans read GSPO too: the delta runs hold Z.
    for hint in [
        fluree_db_core::IndexType::Post,
        fluree_db_core::IndexType::Opst,
    ] {
        assert_eq!(
            history_events_via(&ledger, "g2", Some(hint)).await,
            3,
            "{hint:?}: B's assert, Z's assert and retract"
        );
    }

    // g1 leaves the pool: the runs fold into the bases, without g1.
    set_graph_placement(
        &fluree,
        ledger_id,
        "PlacementAuto",
        &[("g1", "PlacementDedicated")],
    )
    .await;
    let (root3, ledger) = index_and_reload(&fluree, ledger_id).await;
    let pool = root3.pool.as_ref().unwrap();
    for order in secondary {
        assert!(pool.delta(order).is_empty(), "{order:?} compacted");
        assert_ne!(
            branch(pool, order),
            branch(&imported, order),
            "{order:?} rebased"
        );
    }
    let content = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
    let garbage: fluree_db_indexer::GarbageRecord = serde_json::from_slice(
        &content
            .get(&root3.garbage.as_ref().expect("garbage").id)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        root2
            .pool_delta_runs()
            .all(|run| garbage.garbage.contains(&run.cid.to_string())),
        "compacted runs are garbage"
    );
    let (in_rows, in_history) = graphs_in_pool(&fluree, &ledger, ledger_id).await;
    let g1 = user_graph_id(&root3, "g1");
    assert!(!in_rows.contains(&g1) && !in_history.contains(&g1));
    check(ledger).await;
}

/// Each pooled order's rows at `t`, with their assertion times.
fn pooled_rows_at(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    order: fluree_db_binary_index::RunSortOrder,
    t: i64,
) -> Vec<(u32, u64, u32, u16, u64, u32, u32)> {
    use fluree_db_binary_index::{BinaryCursor, BinaryFilter, ColumnProjection};
    let branch = Arc::clone(store.pool_branch(order).expect("pooled order"));
    let mut cursor = BinaryCursor::scan_all(
        Arc::clone(store),
        order,
        branch,
        BinaryFilter::default(),
        ColumnProjection::all(),
    );
    cursor.set_to_t(t);
    let mut rows = Vec::new();
    while let Some(b) = cursor.next_batch().unwrap() {
        for i in 0..b.row_count {
            rows.push((
                b.g_id.get_or(i, 0),
                b.s_id.get(i),
                b.p_id.get_or(i, 0),
                b.o_type.get_or(i, 0),
                b.o_key.get(i),
                b.o_i.get_or(i, u32::MAX),
                b.t.get_or(i, 0),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

/// Every pooled secondary order reads what GSPO reads at every `t` from
/// `first_t`, assertion times included.
fn assert_pooled_orders_agree(ledger: &fluree_db_api::LedgerState, first_t: i64, context: &str) {
    use fluree_db_binary_index::RunSortOrder;
    let store = ledger
        .binary_store
        .as_ref()
        .unwrap()
        .0
        .clone()
        .downcast::<fluree_db_binary_index::BinaryIndexStore>()
        .unwrap();
    for t in first_t..=ledger.t() {
        let expected = pooled_rows_at(&store, RunSortOrder::Gspo, t);
        for order in [
            RunSortOrder::SpotG,
            RunSortOrder::PsotG,
            RunSortOrder::PostG,
            RunSortOrder::OpstG,
        ] {
            assert_eq!(
                pooled_rows_at(&store, order, t),
                expected,
                "{context}: t={t} {order:?}"
            );
        }
    }
}

/// Alternating retractions and reassertions across 67 builds, through two
/// run-cap compactions (builds 33 and 66) and a promotion's (build 67): the
/// secondary orders agree with GSPO after every build.
#[tokio::test]
async fn pooled_orders_agree_with_gspo_through_compaction() {
    use fluree_db_binary_index::RunSortOrder;
    let db = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let id = "test/pool-delta-agree:main";
    let (fluree, ledger) = import_trig(db.path(), data.path(), id, THREE_GRAPHS).await;
    let first_t = ledger.t();
    for step in 0..67 {
        let graph = step % 3 + 1;
        let action = if step % 2 == 0 { "DELETE" } else { "INSERT" };
        sparql_update(
            &fluree,
            id,
            &format!(
                "PREFIX ex: <http://example.org/> {action} DATA {{ GRAPH ex:g{graph} {{ ex:same ex:p 0 . }} }}"
            ),
        )
        .await;
        sparql_update(
            &fluree,
            id,
            &format!(
                "PREFIX ex: <http://example.org/> INSERT DATA {{ GRAPH ex:g{graph} {{ ex:n{step} ex:val {} . }} }}",
                100 + step
            ),
        )
        .await;
        if step == 66 {
            set_graph_placement(
                &fluree,
                id,
                "PlacementAuto",
                &[("g3", "PlacementDedicated")],
            )
            .await;
        }
        let (root, ledger) = index_and_reload(&fluree, id).await;
        if matches!(step, 32 | 65 | 66) {
            assert!(
                root.pool
                    .as_ref()
                    .unwrap()
                    .delta(RunSortOrder::PostG)
                    .is_empty(),
                "compaction at step {step}"
            );
        }
        assert_pooled_orders_agree(&ledger, first_t, &format!("step {step}"));
    }
}

/// The ways a fact's assertion time depends on indexing windows, through a
/// compaction folding those windows into the bases: a reassertion alone in
/// its window replaces the row's time (g2), including a fact asserted the
/// window before (g1), while one followed by a retraction within its
/// window changes nothing (g3). The secondary orders keep GSPO's times
/// before and after compaction.
#[tokio::test]
async fn pooled_orders_keep_assertion_times_across_windows() {
    use fluree_db_binary_index::RunSortOrder;
    let db = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let id = "test/pool-delta-times:main";
    let (fluree, ledger) = import_trig(db.path(), data.path(), id, THREE_GRAPHS).await;
    let first_t = ledger.t();
    let update = |body: &'static str| {
        let fluree = &fluree;
        async move {
            sparql_update(
                fluree,
                id,
                &format!("PREFIX ex: <http://example.org/> {body}"),
            )
            .await;
        }
    };

    update("INSERT DATA { GRAPH ex:g2 { ex:same ex:p 0 . } }").await;
    update("INSERT DATA { GRAPH ex:g1 { ex:k ex:val 7 . } GRAPH ex:g4 { ex:d ex:val 4 . } }").await;
    let (_, ledger) = index_and_reload(&fluree, id).await;
    assert_pooled_orders_agree(&ledger, first_t, "window 1");

    update("INSERT DATA { GRAPH ex:g1 { ex:k ex:val 7 . } }").await;
    update("DELETE DATA { GRAPH ex:g2 { ex:same ex:p 0 . } }").await;
    let (_, ledger) = index_and_reload(&fluree, id).await;
    assert_pooled_orders_agree(&ledger, first_t, "window 2");

    update("INSERT DATA { GRAPH ex:g2 { ex:same ex:p 0 . } }").await;
    update("INSERT DATA { GRAPH ex:g3 { ex:same ex:p 0 . } }").await;
    update("DELETE DATA { GRAPH ex:g3 { ex:same ex:p 0 . } }").await;
    update("INSERT DATA { GRAPH ex:g3 { ex:same ex:p 0 . } }").await;
    let (root, ledger) = index_and_reload(&fluree, id).await;
    assert_eq!(
        root.pool.as_ref().unwrap().delta(RunSortOrder::PostG).len(),
        3
    );
    assert_pooled_orders_agree(&ledger, first_t, "window 3");

    set_graph_placement(
        &fluree,
        id,
        "PlacementAuto",
        &[("g4", "PlacementDedicated")],
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, id).await;
    assert!(root
        .pool
        .as_ref()
        .unwrap()
        .delta(RunSortOrder::PostG)
        .is_empty());
    assert_pooled_orders_agree(&ledger, first_t, "compacted");
}

/// A compaction keeps a reasserted fact's assertion time as of each `t`,
/// as GSPO and the query it answers report it.
#[tokio::test]
async fn compaction_keeps_a_reassertion_time() {
    use fluree_db_binary_index::{BinaryCursor, BinaryFilter, ColumnProjection, RunSortOrder};
    let db = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let id = "test/pool-delta-assert-time:main";
    let (fluree, _) = import_trig(db.path(), data.path(), id, THREE_GRAPHS).await;
    sparql_update(
        &fluree,
        id,
        "PREFIX ex: <http://example.org/> INSERT DATA { GRAPH ex:g2 { ex:same ex:p 0 . } }",
    )
    .await;
    let (_, ledger) = index_and_reload(&fluree, id).await;
    let asserted_t = ledger.t();
    for action in ["DELETE", "INSERT"] {
        sparql_update(
            &fluree,
            id,
            &format!(
                "PREFIX ex: <http://example.org/> {action} DATA {{ GRAPH ex:g2 {{ ex:same ex:p 0 . }} }}"
            ),
        )
        .await;
        index_and_reload(&fluree, id).await;
    }
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": {"@id": id, "graph": "http://example.org/g2", "t": asserted_t},
        "select": ["?s", "?v"],
        "where": [
            {"@id": "?s", "ex:p": "?v"},
            ["filter", format!("(and (>= ?v 0) (= (t ?v) {asserted_t}))")]
        ]
    });
    let post_times = |ledger: &fluree_db_api::LedgerState| {
        let store = ledger
            .binary_store
            .as_ref()
            .unwrap()
            .0
            .clone()
            .downcast::<fluree_db_binary_index::BinaryIndexStore>()
            .unwrap();
        let g = user_graph_id(&latest_root(db.path()), "g2");
        let p = store
            .sid_to_p_id(&ledger.snapshot.encode_iri("http://example.org/p").unwrap())
            .unwrap();
        let mut cursor = BinaryCursor::scan_all(
            Arc::clone(&store),
            RunSortOrder::PostG,
            Arc::clone(store.pool_branch(RunSortOrder::PostG).unwrap()),
            BinaryFilter {
                g_id: Some(g),
                p_id: Some(p),
                ..Default::default()
            },
            ColumnProjection::all(),
        );
        cursor.set_to_t(asserted_t);
        let mut times = Vec::new();
        while let Some(b) = cursor.next_batch().unwrap() {
            for i in 0..b.row_count {
                times.push(b.t.get(i));
            }
        }
        times
    };
    let ledger = fluree.ledger(id).await.unwrap();
    assert_eq!(post_times(&ledger), vec![asserted_t as u32]);
    let before = fluree
        .query_connection(&query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(before.as_array().unwrap().len(), 1);

    set_graph_placement(
        &fluree,
        id,
        "PlacementAuto",
        &[("g3", "PlacementDedicated")],
    )
    .await;
    let (root, ledger) = index_and_reload(&fluree, id).await;
    assert!(root
        .pool
        .as_ref()
        .unwrap()
        .delta(RunSortOrder::PostG)
        .is_empty());
    let after = fluree
        .query_connection(&query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(
        post_times(&ledger),
        vec![asserted_t as u32],
        "POST after compaction"
    );
}

/// With unindexed writes, `GRAPH ?g` still prunes by a probe of the
/// persisted pool, and visits every graph those writes touched as well: a
/// match only novelty holds (g3's new label, a new graph g4) is found, a
/// persisted match novelty retracted (g2's) is gone, and an untouched
/// graph's persisted match (g1's) stays — through SPARQL and JSON-LD.
#[tokio::test]
async fn graph_pruning_admits_graphs_with_unindexed_changes() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-probe-novelty:main";
    let (fluree, _) = import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
    for update in [
        r#"INSERT DATA { GRAPH ex:g3 { ex:c ex:label "Z" . } GRAPH ex:g4 { ex:d ex:label "Z" . } }"#,
        r#"DELETE DATA { GRAPH ex:g2 { ex:b ex:label "B" . } }"#,
    ] {
        sparql_update(
            &fluree,
            ledger_id,
            &format!("PREFIX ex: <http://example.org/> {update}"),
        )
        .await;
    }
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let store = ledger
        .binary_store
        .as_ref()
        .unwrap()
        .0
        .clone()
        .downcast::<fluree_db_binary_index::BinaryIndexStore>()
        .unwrap();
    assert!(ledger.t() > store.max_t(), "the writes are unindexed");

    for (pattern, expected) in [
        (
            r#"?s ex:label "Z""#,
            vec![
                "g=http://example.org/g3 s=http://example.org/c",
                "g=http://example.org/g4 s=http://example.org/d",
            ],
        ),
        (r#"?s ex:label "B""#, vec![]),
        (
            r#"?s ex:label "A""#,
            vec!["g=http://example.org/g1 s=http://example.org/a"],
        ),
    ] {
        assert_eq!(
            select(
                &fluree,
                &ledger,
                &format!("{PREFIXES}SELECT ?g ?s WHERE {{ GRAPH ?g {{ {pattern} }} }}")
            )
            .await,
            expected,
            "{pattern}"
        );
    }

    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?g", "?s"],
        "where": [["graph", "?g", {"@id": "?s", "ex:label": "Z"}]],
        "orderBy": ["?g"]
    });
    let rows = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        rows,
        serde_json::json!([
            ["http://example.org/g3", "ex:c"],
            ["http://example.org/g4", "ex:d"]
        ]),
        "JSON-LD"
    );
}

/// A scan merges novelty only for its own graph: with unindexed writes in
/// g1 alone, g1 reads them, while g2 and the default graph read their
/// persisted rows unchanged.
#[tokio::test]
async fn a_scan_merges_only_its_own_graphs_novelty() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-scan-novelty:main";
    let trig = format!("{THREE_GRAPHS}\n@prefix ex: <http://example.org/> .\nex:root ex:val 9 .\n");
    let (fluree, _) = import_trig(db_dir.path(), data_dir.path(), ledger_id, &trig).await;
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         DELETE DATA { GRAPH ex:g1 { ex:a ex:val 1 . } } ;
         INSERT DATA { GRAPH ex:g1 { ex:a ex:val 11 . ex:n ex:val 12 . } }",
    )
    .await;
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    for (graph, expected) in [
        (
            "GRAPH ex:g1",
            vec!["s=http://example.org/a v=11", "s=http://example.org/n v=12"],
        ),
        ("GRAPH ex:g2", vec!["s=http://example.org/b v=2"]),
        ("", vec!["s=http://example.org/root v=9"]),
    ] {
        assert_eq!(
            select(
                &fluree,
                &ledger,
                &format!("{PREFIXES}SELECT ?s ?v WHERE {{ {graph} {{ ?s ex:val ?v }} }}")
            )
            .await,
            expected,
            "{graph}"
        );
    }
}

/// Scans of graphs the pending writes didn't touch read as indexed scans
/// (late-materialized bindings, the independent-scan join lane) while the
/// changed graph merges its novelty, which mints an entity. Every answer
/// with the writes pending equals the answer once they are indexed: GROUP
/// BY, OPTIONAL, a join out of the graph block, DISTINCT and a selective
/// lookup, through SPARQL, and GROUP BY through JSON-LD.
#[tokio::test]
async fn pending_writes_to_one_graph_answer_as_once_indexed() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-pending-mixed:main";
    let trig = r#"
@prefix ex: <http://example.org/> .
ex:e1 ex:name "E1" . ex:e2 ex:name "E2" .
ex:g1 { ex:a ex:refers ex:e1 ; ex:ctx ex:d1 . ex:d1 ex:title "D1" . }
ex:g2 { ex:b ex:refers ex:e1 ; ex:ctx ex:d2 . ex:d2 ex:title "D2" . }
ex:g3 { ex:c ex:refers ex:e2 ; ex:ctx ex:d3 . }
"#;
    let (fluree, _) = import_trig(db_dir.path(), data_dir.path(), ledger_id, trig).await;
    sparql_update(
        &fluree,
        ledger_id,
        r#"PREFIX ex: <http://example.org/>
           INSERT DATA { GRAPH ex:g1 {
               ex:a2 ex:refers ex:e1 ; ex:ctx ex:d1 .
               ex:x ex:refers ex:eNew ; ex:ctx ex:dNew . ex:dNew ex:title "DNew" . } } ;
           DELETE DATA { GRAPH ex:g2 { ex:b ex:refers ex:e1 . } } ;
           INSERT DATA { ex:eNew ex:name "ENew" . }"#,
    )
    .await;
    let queries = [
        "SELECT ?ent ?d (COUNT(?occ) AS ?n) WHERE { GRAPH ?g { ?occ ex:refers ?ent ; ex:ctx ?d } } GROUP BY ?ent ?d",
        "SELECT ?g ?d ?t WHERE { GRAPH ?g { ?occ ex:ctx ?d OPTIONAL { ?d ex:title ?t } } }",
        "SELECT ?occ ?name WHERE { GRAPH ?g { ?occ ex:refers ?ent } ?ent ex:name ?name }",
        "SELECT DISTINCT ?ent WHERE { GRAPH ?g { ?occ ex:refers ?ent } }",
        "SELECT ?g ?occ WHERE { GRAPH ?g { ?occ ex:refers ex:e1 } }",
    ];
    let grouped = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?ent", "?d", "(count ?occ)"],
        "where": [["graph", "?g", {"@id": "?occ", "ex:refers": "?ent", "ex:ctx": "?d"}]],
        "groupBy": ["?ent", "?d"],
        "orderBy": ["?ent", "?d"]
    });
    let answers = |ledger: fluree_db_api::LedgerState| {
        let fluree = &fluree;
        let grouped = &grouped;
        async move {
            let mut out = Vec::new();
            for q in queries {
                out.push(select(fluree, &ledger, &format!("{PREFIXES}{q}")).await);
            }
            let rows = support::query_jsonld(fluree, &ledger, grouped)
                .await
                .unwrap()
                .to_jsonld(&ledger.snapshot)
                .unwrap();
            (out, rows)
        }
    };
    let pending = answers(fluree.ledger(ledger_id).await.unwrap()).await;
    assert_eq!(
        pending.0[0],
        vec![
            "d=http://example.org/d1 ent=http://example.org/e1 n=2",
            "d=http://example.org/d3 ent=http://example.org/e2 n=1",
            "d=http://example.org/dNew ent=http://example.org/eNew n=1",
        ]
    );
    let (_, ledger) = index_and_reload(&fluree, ledger_id).await;
    let indexed = answers(ledger).await;
    for (q, (p, i)) in queries.iter().zip(pending.0.iter().zip(&indexed.0)) {
        assert_eq!(p, i, "{q}");
    }
    assert_eq!(pending.1, indexed.1, "JSON-LD GROUP BY");
}

/// A decimal lookup, a string-prefix filter and a range filter on the
/// default graph answer exactly with writes pending in a pooled graph, where
/// they read the default graph's index as authoritative, and with writes
/// pending in the default graph itself, where they must not.
#[tokio::test]
async fn value_lookups_answer_exactly_beside_another_graphs_pending_writes() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-pending-values:main";
    let trig = r#"
@prefix ex: <http://example.org/> .
ex:a ex:price 1.5 ; ex:name "Alice" .
ex:b ex:price 2.25 ; ex:name "Albert" .
ex:c ex:price 3.5 ; ex:name "Bob" .
ex:a ex:big "111111111111111111111111111111.5"^^<http://www.w3.org/2001/XMLSchema#decimal> .
ex:b ex:big "222222222222222222222222222222.5"^^<http://www.w3.org/2001/XMLSchema#decimal> .
ex:g1 { ex:d ex:price 1.5 ; ex:name "Alma" . }
"#;
    let (fluree, _) = import_trig(db_dir.path(), data_dir.path(), ledger_id, trig).await;
    let queries = [
        "SELECT ?s WHERE { ?s ex:price 1.5 }",
        r#"SELECT ?s WHERE { ?s ex:name ?n FILTER(STRSTARTS(?n, "Al")) }"#,
        "SELECT ?s WHERE { ?s ex:price ?p FILTER(?p > 2) }",
        r#"SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:name ?n FILTER(STRSTARTS(?n, "Al")) } }"#,
        "SELECT ?s WHERE { ?s ex:big 111111111111111111111111111111.5 }",
        "SELECT ?s WHERE { ?s ex:big 2.5 }",
    ];
    let answers = |ledger: fluree_db_api::LedgerState| {
        let fluree = &fluree;
        async move {
            let mut out = Vec::new();
            for q in queries {
                out.push(select(fluree, &ledger, &format!("{PREFIXES}{q}")).await);
            }
            out
        }
    };

    sparql_update(
        &fluree,
        ledger_id,
        r#"PREFIX ex: <http://example.org/>
           INSERT DATA { GRAPH ex:g1 { ex:e ex:price 1.5 ; ex:name "Alfred" . } }"#,
    )
    .await;
    let elsewhere = answers(fluree.ledger(ledger_id).await.unwrap()).await;
    assert_eq!(elsewhere[0], ["s=http://example.org/a"]);
    assert_eq!(
        elsewhere[1],
        ["s=http://example.org/a", "s=http://example.org/b"]
    );
    assert_eq!(
        elsewhere[2],
        ["s=http://example.org/b", "s=http://example.org/c"]
    );
    assert_eq!(elsewhere[4], ["s=http://example.org/a"]);

    sparql_update(
        &fluree,
        ledger_id,
        r#"PREFIX ex: <http://example.org/>
           INSERT DATA { ex:f ex:price 1.5e0 ; ex:name "Alan" ; ex:big 2.5 . ex:h ex:price 4.0 . }"#,
    )
    .await;
    let here = answers(fluree.ledger(ledger_id).await.unwrap()).await;
    let (_, ledger) = index_and_reload(&fluree, ledger_id).await;
    let indexed = answers(ledger).await;
    for (q, (p, i)) in queries.iter().zip(here.iter().zip(&indexed)) {
        assert_eq!(p, i, "{q}");
    }
    assert_eq!(
        indexed[0],
        ["s=http://example.org/a", "s=http://example.org/f"]
    );
    assert_eq!(indexed[5], ["s=http://example.org/f"]);
}

/// What the delta tests read: a value range (pooled POST), an object alone
/// (pooled OPST), `GRAPH ?g` over every pooled order, and g2 at a past `t`.
async fn pooled_answers(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    past_t: i64,
) -> Vec<Vec<String>> {
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    let mut out = Vec::new();
    for q in [
        "SELECT ?s ?v WHERE { GRAPH ex:g2 { ?s ex:val ?v FILTER(?v > 5) } }",
        "SELECT ?s ?p WHERE { GRAPH ex:g3 { ?s ?p 30 } }",
        "SELECT ?g ?s ?v WHERE { GRAPH ?g { ?s ex:val ?v } }",
    ] {
        out.push(select(fluree, &ledger, &format!("{PREFIXES}{q}")).await);
    }
    out.push(objects_at(fluree, &ledger, ledger_id, "g2", Some(past_t)).await);
    out
}

/// An idle ledger's pooled delta folds into its base on request, at the same
/// `t`: answers are unchanged at every `t`, every order still agrees with
/// GSPO, the runs become garbage, and a second call finds nothing to do.
/// With writes pending the call indexes them first. Through the background
/// indexer, and without one as a scheduled function would run it.
#[tokio::test]
async fn maintenance_folds_an_idle_pooled_delta_into_its_base() {
    use fluree_db_api::MaintainIndexOptions;
    use fluree_db_binary_index::RunSortOrder;
    let secondary = [
        RunSortOrder::SpotG,
        RunSortOrder::PsotG,
        RunSortOrder::PostG,
        RunSortOrder::OpstG,
    ];
    for with_indexer in [true, false] {
        let db_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let ledger_id = "test/pool-maintain:main";
        let (fluree, ledger) =
            import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
        let first_t = ledger.t();
        drop(ledger);
        let fluree = if with_indexer {
            fluree
        } else {
            drop(fluree);
            FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                .without_indexing()
                .build()
                .expect("build file-backed Fluree")
        };
        let mode = if with_indexer { "indexer" } else { "direct" };

        sparql_update(
            &fluree,
            ledger_id,
            "PREFIX ex: <http://example.org/>
             INSERT DATA { GRAPH ex:g2 { ex:z ex:label \"Z\" . ex:z ex:val 20 . }
                           GRAPH ex:g3 { ex:w ex:val 30 . } }",
        )
        .await;
        let t_added = fluree.ledger(ledger_id).await.unwrap().t();
        sparql_update(
            &fluree,
            ledger_id,
            "PREFIX ex: <http://example.org/>
             DELETE DATA { GRAPH ex:g2 { ex:z ex:label \"Z\" . } }",
        )
        .await;
        let (root, _) = index_and_reload(&fluree, ledger_id).await;
        let pool = root.pool.as_ref().expect("pooled");
        for order in secondary {
            assert!(
                !pool.delta(order).is_empty(),
                "{mode}: {order:?} has a delta"
            );
        }
        let before = pooled_answers(&fluree, ledger_id, t_added).await;

        let result = fluree
            .maintain_index(ledger_id, MaintainIndexOptions::default())
            .await
            .expect("maintain");
        assert!(!result.built && result.compacted, "{mode}: {result:?}");
        assert_eq!(result.index_t, root.index_t, "{mode}: same t");
        let store = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
        let compacted = IndexRoot::decode(
            &store
                .get(result.root_id.as_ref().expect("root"))
                .await
                .unwrap(),
        )
        .unwrap();
        let compacted_pool = compacted.pool.as_ref().expect("pooled");
        for order in secondary {
            assert!(compacted_pool.delta(order).is_empty(), "{mode}: {order:?}");
        }
        let garbage: fluree_db_indexer::GarbageRecord = serde_json::from_slice(
            &store
                .get(&compacted.garbage.as_ref().expect("garbage").id)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(
            root.pool_delta_runs()
                .all(|run| garbage.garbage.contains(&run.cid.to_string())),
            "{mode}: compacted runs are garbage"
        );
        assert_eq!(
            pooled_answers(&fluree, ledger_id, t_added).await,
            before,
            "{mode}"
        );
        let ledger = fluree.ledger(ledger_id).await.unwrap();
        assert_pooled_orders_agree(&ledger, first_t, mode);

        let again = fluree
            .maintain_index(ledger_id, MaintainIndexOptions::default())
            .await
            .expect("maintain again");
        assert!(!again.built && !again.compacted, "{mode}: {again:?}");

        // Pending writes are indexed, and the run they add folded in.
        sparql_update(
            &fluree,
            ledger_id,
            "PREFIX ex: <http://example.org/>
             INSERT DATA { GRAPH ex:g1 { ex:q ex:val 40 . } }",
        )
        .await;
        let head_t = fluree.ledger(ledger_id).await.unwrap().t();
        let pending = fluree
            .maintain_index(ledger_id, MaintainIndexOptions::default())
            .await
            .expect("maintain pending");
        assert!(pending.built && pending.compacted, "{mode}: {pending:?}");
        assert_eq!(pending.index_t, head_t, "{mode}");
        let indexed = IndexRoot::decode(
            &store
                .get(pending.root_id.as_ref().expect("root"))
                .await
                .unwrap(),
        )
        .unwrap();
        for order in secondary {
            assert!(
                indexed.pool.as_ref().unwrap().delta(order).is_empty(),
                "{mode}: {order:?} after the build"
            );
        }
        let ledger = fluree.ledger(ledger_id).await.unwrap();
        assert_eq!(
            select(
                &fluree,
                &ledger,
                &format!("{PREFIXES}SELECT ?s WHERE {{ GRAPH ex:g1 {{ ?s ex:val 40 }} }}")
            )
            .await,
            vec!["s=http://example.org/q"],
            "{mode}"
        );
        assert_pooled_orders_agree(&ledger, first_t, mode);
    }
}

/// A server's housekeeping folds the pooled delta of a ledger that has not
/// moved since its last tick, so an idle ledger does not keep its delta until
/// the next write.
#[tokio::test]
async fn housekeeping_compacts_an_idle_ledgers_pooled_delta() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-idle:main";
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
    let first_t = ledger.t();
    drop((fluree, ledger));
    let fluree = Arc::new(
        FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .with_indexer_catchup_interval(std::time::Duration::from_millis(50))
            .build()
            .expect("build file-backed Fluree"),
    );
    sparql_update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g2 { ex:z ex:val 20 . } GRAPH ex:g3 { ex:w ex:val 30 . } }",
    )
    .await;
    let t_added = fluree.ledger(ledger_id).await.unwrap().t();
    let (root, _) = index_and_reload(&fluree, ledger_id).await;
    assert!(!root.pool.as_ref().unwrap().deltas.is_empty());
    let before = pooled_answers(&fluree, ledger_id, t_added).await;

    assert!(fluree.start_lifecycle_housekeeping(None));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let head = fluree
            .nameservice()
            .lookup(&fluree_db_core::LedgerId::parse(ledger_id).unwrap())
            .await
            .unwrap()
            .unwrap()
            .index_head_id
            .unwrap();
        let store = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
        let current = IndexRoot::decode(&store.get(&head).await.unwrap()).unwrap();
        if current.pool.as_ref().unwrap().deltas.is_empty() {
            assert_eq!(current.index_t, root.index_t);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the idle ledger's delta was not compacted"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(pooled_answers(&fluree, ledger_id, t_added).await, before);
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    assert_pooled_orders_agree(&ledger, first_t, "idle compaction");
}

async fn cached_state(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("cached ledger");
    handle.snapshot().await.to_ledger_state()
}

/// A root compacted at the same `t` reaches other connections: a reader that
/// cached the ledger before the compaction adopts it on refresh, by itself or
/// with a commit that landed after it, which it keeps; a record read before
/// the compaction does not take it back.
#[tokio::test]
async fn cached_readers_adopt_a_root_compacted_at_the_same_t() {
    use fluree_db_api::{
        FreshnessCheck, MaintainIndexOptions, NotifyResult, NsNotify, RefreshOpts, RemoteWatermark,
    };
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let ledger_id = "test/pool-adopt:main";
    let (fluree, ledger) =
        import_trig(db_dir.path(), data_dir.path(), ledger_id, THREE_GRAPHS).await;
    drop((fluree, ledger));
    let connect = || {
        FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .without_indexing()
            .without_storage_lock()
            .build()
            .expect("build file-backed Fluree")
    };
    let writer = connect();
    sparql_update(
        &writer,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g2 { ex:z ex:val 20 . } GRAPH ex:g3 { ex:w ex:val 30 . } }",
    )
    .await;
    let (root, _) = index_and_reload(&writer, ledger_id).await;
    assert!(!root.pool.as_ref().unwrap().deltas.is_empty());

    let (index_only, catch_up) = (connect(), connect());
    for reader in [&index_only, &catch_up] {
        reader.ledger_cached(ledger_id).await.expect("cache ledger");
    }
    let id = fluree_db_core::LedgerId::parse(ledger_id).unwrap();
    let before = writer.nameservice().lookup(&id).await.unwrap().unwrap();
    let compacted = writer
        .maintain_index(ledger_id, MaintainIndexOptions::default())
        .await
        .expect("maintain");
    assert!(compacted.compacted, "{compacted:?}");
    assert_eq!(compacted.index_t, root.index_t);
    let compacted_root = compacted.root_id.expect("root");

    // A peer's watermark naming the new root marks the cached state stale.
    let watermark = RemoteWatermark {
        commit_t: root.index_t,
        index_t: root.index_t,
        index_head_id: Some(compacted_root.clone()),
        updated_at: std::time::Instant::now(),
    };
    let handle = index_only.ledger_cached(ledger_id).await.unwrap();
    assert_eq!(
        handle.check_freshness(&watermark).await,
        FreshnessCheck::Stale
    );
    let refreshed = index_only
        .refresh(ledger_id, RefreshOpts::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.action, NotifyResult::IndexUpdated);
    assert_eq!(
        handle.check_freshness(&watermark).await,
        FreshnessCheck::Current
    );
    let state = cached_state(&index_only, ledger_id).await;
    assert_eq!(state.head_index_id.as_ref(), Some(&compacted_root));
    assert_eq!(state.index_t(), root.index_t);

    // The record read before the compaction names the root it replaced: the
    // reader does not step back to it, nor a peer watermark mark it stale.
    let stale = index_only
        .ledger_manager()
        .unwrap()
        .notify(NsNotify {
            ledger_id: id.clone(),
            record: Some(before.clone()),
        })
        .await
        .unwrap();
    assert_eq!(stale, NotifyResult::Current);
    let state = cached_state(&index_only, ledger_id).await;
    assert_eq!(state.head_index_id.as_ref(), Some(&compacted_root));
    let stale_watermark = RemoteWatermark {
        index_head_id: before.index_head_id.clone(),
        ..watermark
    };
    assert_eq!(
        handle.check_freshness(&stale_watermark).await,
        FreshnessCheck::Current
    );

    sparql_update(
        &writer,
        ledger_id,
        "PREFIX ex: <http://example.org/>
         INSERT DATA { GRAPH ex:g2 { ex:v ex:val 50 . } }",
    )
    .await;
    let refreshed = catch_up
        .refresh(ledger_id, RefreshOpts::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.action, NotifyResult::CommitsApplied { count: 1 });
    let state = cached_state(&catch_up, ledger_id).await;
    assert_eq!(state.head_index_id.as_ref(), Some(&compacted_root));
    assert_eq!(
        (state.index_t(), state.t()),
        (root.index_t, root.index_t + 1)
    );
    let q = format!("{PREFIXES}SELECT ?s ?v WHERE {{ GRAPH ex:g2 {{ ?s ex:val ?v }} }}");
    let want = select(&writer, &writer.ledger(ledger_id).await.unwrap(), &q).await;
    assert!(want.iter().any(|row| row.contains("v=50")), "{want:?}");
    assert_eq!(select(&catch_up, &state, &q).await, want);
}
