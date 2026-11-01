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
//! A graph the base index holds nothing for can also have an order with no
//! leaves. Bulk import wrote a SPOT with no leaves for a graph it had no quads
//! for, and an incremental build then left that SPOT empty while writing the
//! next window's facts to the other three orders, so reads by subject missed
//! them. Import now writes no such order, and an incremental build treats one
//! as holding no facts.
//!
//! Background indexing is off in all but
//! `background_indexing_keeps_deleted_facts_deleted`: these tests drive each
//! build themselves, and a background build racing them would make the outcome
//! timing-dependent. That one is about the background indexer, and holds it
//! under its soft threshold until one window has both writes.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, IndexConfig, LedgerState, TransactResult};
use fluree_db_binary_index::format::branch::{build_branch_bytes, read_branch_from_bytes};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::IndexRoot;
use fluree_db_core::{ContentId, ContentKind, GraphId};
use serde_json::{json, Value as JsonValue};
use std::path::Path;

const EX: &str = "http://example.org/ns/";
const G1: &str = "http://example.org/g1";
const G2: &str = "http://example.org/g2";
const G3: &str = "http://example.org/g3";

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

/// A second instance beside the live one, reading what is on disk.
fn fresh_at(dir: &Path) -> Fluree {
    FlureeBuilder::file(dir.to_string_lossy().to_string())
        .without_indexing()
        .without_storage_lock()
        .build()
        .expect("fresh file fluree")
}

async fn sparql(fluree: &Fluree, ledger: LedgerState, body: &str) -> LedgerState {
    commit_sparql(fluree, ledger, body, None).await.ledger
}

/// Commit a SPARQL update; `index_config`, when given, decides whether the
/// commit starts a background build (the server default otherwise).
async fn commit_sparql(
    fluree: &Fluree,
    ledger: LedgerState,
    body: &str,
    index_config: Option<IndexConfig>,
) -> TransactResult {
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
    let mut stage = fluree.stage_owned(ledger).txn(txn);
    if let Some(config) = index_config {
        stage = stage.index_config(config);
    }
    stage
        .execute()
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"))
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
    select_o(
        fluree,
        ledger_id,
        from,
        json!({"@id": "ex:s", "ex:p": "?o"}),
    )
    .await
}

/// `?o` for the pattern `pattern` over the dataset `from`, sorted.
async fn select_o(fluree: &Fluree, ledger_id: &str, from: &str, pattern: JsonValue) -> Vec<String> {
    let q = json!({
        "@context": {"ex": EX},
        "from": from,
        "select": ["?o"],
        "where": pattern
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
    let fresh = objects(&fresh_at(dir), id, &from).await;
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
        let fresh = fresh_at(dir.path());
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

/// Wait until the published index reaches `t`.
async fn wait_for_index_t(fluree: &Fluree, id: &str, t: i64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let record = fluree
            .nameservice()
            .lookup(id)
            .await
            .expect("nameservice lookup")
            .expect("ledger record");
        if record.index_t >= t {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "index_t {} never reached {t}",
            record.index_t
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// `count` facts in `graph`, each its own subject, to push novelty past the
/// soft threshold.
fn padding(graph: Option<&str>, tag: &str, count: usize) -> String {
    let body: String = (0..count)
        .map(|i| format!("ex:pad-{tag}-{i} ex:q \"padding value {tag} {i}\" . "))
        .collect();
    in_graph(graph, &body)
}

/// The same window, indexed by the background indexer on its own: no build is
/// driven here. The indexer is held by its soft threshold, which starts a
/// build only from a commit that leaves novelty at or over it (and catch-up
/// sweeps, which ignore it, are off). The insert and the delete stay under it
/// and the next commit crosses it, so the one build after the seed's takes
/// both in one window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_indexing_keeps_deleted_facts_deleted() {
    const SOFT_THRESHOLD: usize = 100_000;
    for target in [None, Some(G1)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = format!(
            "it/delete-survives-background-{}:main",
            target.map_or("default", |g| &g[g.len() - 2..])
        );
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_indexer_config(
                fluree_db_indexer::IndexerConfig::default().with_catchup_sweeps(false),
            )
            .with_indexing_thresholds(SOFT_THRESHOLD, 100 * SOFT_THRESHOLD)
            .build()
            .expect("file fluree");
        let config = fluree.default_index_config();
        let ledger = fluree.create_ledger(&id).await.expect("create");

        // The base index: data in `G2` only.
        let seed = commit_sparql(
            &fluree,
            ledger,
            &format!("INSERT DATA {{ {} }}", padding(Some(G2), "seed", 2_000)),
            Some(config.clone()),
        )
        .await;
        assert!(seed.indexing.needed, "{id}: the seed must start a build");
        wait_for_index_t(&fluree, &id, seed.ledger.t()).await;
        let ledger = fluree.ledger(&id).await.expect("reload");

        let fact = in_graph(target, "ex:s ex:p \"x\"");
        let insert = commit_sparql(
            &fluree,
            ledger,
            &format!("INSERT DATA {{ {fact} }}"),
            Some(config.clone()),
        )
        .await;
        let delete = commit_sparql(
            &fluree,
            insert.ledger,
            &format!("DELETE DATA {{ {fact} }}"),
            Some(config.clone()),
        )
        .await;
        for (write, status) in [("insert", &insert.indexing), ("delete", &delete.indexing)] {
            assert!(
                !status.needed,
                "{id}: the {write} left {} bytes of novelty, over the threshold, \
                 so a build may have started without the other write",
                status.novelty_size
            );
        }
        let cross = commit_sparql(
            &fluree,
            delete.ledger,
            &format!("INSERT DATA {{ {} }}", padding(Some(G2), "cross", 2_000)),
            Some(config),
        )
        .await;
        assert!(
            cross.indexing.needed,
            "{id}: this commit must start the build"
        );
        wait_for_index_t(&fluree, &id, cross.ledger.t()).await;

        let from = from_alias(&id, target, None);
        assert_eq!(
            objects(&fresh_at(dir.path()), &id, &from).await,
            Vec::<String>::new(),
            "{id}: after the background build"
        );
    }
}

/// A ledger bulk-imported from a TriG file whose one quad is in `G2`, with an
/// empty `GRAPH <G3> { }` block: the import has no quads for the default graph
/// or `G3`.
async fn imported_ledger(dir: &Path, id: &str) -> Fluree {
    let data = tempfile::tempdir().expect("tempdir");
    let trig = data.path().join("named.trig");
    std::fs::write(
        &trig,
        format!(
            "@prefix ex: <{EX}> .\nGRAPH <{G2}> {{ ex:a ex:q \"base\" . }}\nGRAPH <{G3}> {{ }}\n"
        ),
    )
    .expect("write TriG");
    let fluree = fluree_at(dir);
    fluree
        .create(id)
        .import(&trig)
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("import");
    fluree
}

/// The published index's `t` and root.
async fn published_root(fluree: &Fluree, id: &str) -> (i64, IndexRoot) {
    let record = fluree
        .nameservice()
        .lookup(id)
        .await
        .expect("nameservice lookup")
        .expect("ledger record");
    let root_id = record.index_head_id.clone().expect("a published index");
    let bytes = fluree
        .content_store(&record.storage_namespace())
        .get(&root_id)
        .await
        .expect("root");
    (
        record.index_t,
        IndexRoot::decode(&bytes).expect("decode root"),
    )
}

/// Every `(g_id, order)` in the published root whose branch has no leaves.
async fn orders_without_leaves(fluree: &Fluree, id: &str) -> Vec<(GraphId, RunSortOrder)> {
    let (_, root) = published_root(fluree, id).await;
    let store = fluree.content_store(
        &fluree
            .storage_namespace(id)
            .await
            .expect("storage namespace"),
    );
    let mut empty: Vec<(GraphId, RunSortOrder)> = root
        .default_graph_orders
        .iter()
        .filter(|o| o.leaves.is_empty())
        .map(|o| (0, o.order))
        .collect();
    for ng in &root.named_graphs {
        for (order, cid) in &ng.orders {
            let bytes = store.get(cid).await.expect("branch");
            if read_branch_from_bytes(&bytes)
                .expect("decode branch")
                .leaves
                .is_empty()
            {
                empty.push((ng.g_id, *order));
            }
        }
    }
    empty
}

/// The `g_id` of `graph`, or 0 for the default graph.
async fn graph_id(fluree: &Fluree, id: &str, graph: Option<&str>) -> GraphId {
    match graph {
        None => 0,
        Some(iri) => fluree
            .ledger(id)
            .await
            .expect("load")
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .expect("a registered graph"),
    }
}

/// Give `g_id` a SPOT branch with no leaves, as every writer writes one.
async fn empty_spot(fluree: &Fluree, id: &str, root: &mut IndexRoot, g_id: GraphId) {
    use fluree_db_binary_index::format::index_root::{DefaultGraphOrder, NamedGraphRouting};
    let spot = RunSortOrder::Spot;
    if g_id == 0 {
        root.default_graph_orders.retain(|o| o.order != spot);
        root.default_graph_orders.push(DefaultGraphOrder {
            order: spot,
            leaves: Vec::new(),
        });
        return;
    }
    let cid = fluree
        .content_store(
            &fluree
                .storage_namespace(id)
                .await
                .expect("storage namespace"),
        )
        .put(
            ContentKind::IndexBranch,
            &build_branch_bytes(spot, g_id, &[]),
        )
        .await
        .expect("put branch");
    // An older import pooled no graph.
    if let Some(pool) = root.pool.as_mut() {
        pool.members.retain(|m| m.g_id != g_id);
    }
    match root.named_graphs.iter_mut().find(|ng| ng.g_id == g_id) {
        Some(ng) => {
            ng.orders.retain(|(order, _)| *order != spot);
            ng.orders.push((spot, cid));
        }
        None => root.named_graphs.push(NamedGraphRouting {
            g_id,
            orders: vec![(spot, cid)],
        }),
    }
}

/// Publish `root` as the index at `t`.
async fn publish_root(fluree: &Fluree, id: &str, t: i64, root: &IndexRoot) {
    let root_id: ContentId = fluree
        .content_store(
            &fluree
                .storage_namespace(id)
                .await
                .expect("storage namespace"),
        )
        .put(ContentKind::IndexRoot, &root.encode())
        .await
        .expect("put root");
    support::publish_built_index(fluree, id, t, &root_id).await;
}

/// An incremental build that keeps every named graph in trees of its own,
/// which the caller then rewrites.
async fn incremental_build(
    fluree: &Fluree,
    id: &str,
) -> Result<fluree_db_indexer::IndexResult, fluree_db_indexer::IndexerError> {
    let record = fluree
        .nameservice()
        .lookup(id)
        .await
        .expect("nameservice lookup")
        .expect("ledger record");
    let config = fluree_db_indexer::IndexerConfig {
        graph_placement: fluree_db_core::ledger_config::GraphPlacementIntent {
            default: fluree_db_core::ledger_config::GraphPlacement::Dedicated,
            overrides: Vec::new(),
        },
        ..Default::default()
    };
    fluree_db_indexer::build_incremental_index_for_record(
        fluree.content_store(&record.storage_namespace()),
        &record,
        config,
    )
    .await
}

/// Bulk import writes no order without leaves, so after an import with no
/// quads for the default graph or `G3`, the next build takes their first facts
/// incrementally. So does a root from an import that still wrote a SPOT with
/// no leaves for each.
#[tokio::test]
async fn graphs_an_import_left_empty_take_facts_incrementally() {
    for older_import in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = format!("it/delete-survives-import-{older_import}:main");
        let fluree = imported_ledger(dir.path(), &id).await;
        assert_eq!(
            orders_without_leaves(&fluree, &id).await,
            [],
            "{id}: the import wrote an order with no leaves"
        );
        if older_import {
            let (t, mut root) = published_root(&fluree, &id).await;
            let mut expected = Vec::new();
            for graph in [None, Some(G3)] {
                let g_id = graph_id(&fluree, &id, graph).await;
                empty_spot(&fluree, &id, &mut root, g_id).await;
                expected.push((g_id, RunSortOrder::Spot));
            }
            publish_root(&fluree, &id, t, &root).await;
            assert_eq!(orders_without_leaves(&fluree, &id).await, expected);
        }

        let ledger = fluree.ledger(&id).await.expect("load");
        let ledger = sparql(
            &fluree,
            ledger,
            &format!("INSERT DATA {{ ex:s ex:p \"x\" . GRAPH <{G3}> {{ ex:s ex:p \"x\" }} }}"),
        )
        .await;
        drop(ledger);
        build(&fluree, &id, Build::Incremental).await;
        for graph in [None, Some(G3)] {
            assert_eq!(
                current(&fluree, dir.path(), &id, graph).await,
                ["\"x\""],
                "{id}: {graph:?} by subject"
            );
        }
    }
}

/// An index in which a graph has facts in PSOT, POST and OPST but none in
/// SPOT, as an incremental build wrote after an import left the graph a SPOT
/// with no leaves: it kept that SPOT empty and wrote the window's facts to the
/// other three orders, so reads by subject missed them. The next build refuses
/// to extend that index and falls back to a full rebuild, which restores SPOT.
#[tokio::test]
async fn facts_missing_from_spot_are_rebuilt() {
    for graph in [None, Some(G3)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = format!(
            "it/delete-survives-spot-{}:main",
            graph.map_or("default", |g| &g[g.len() - 2..])
        );
        let fluree = imported_ledger(dir.path(), &id).await;
        let fact = |o: &str| {
            format!(
                "INSERT DATA {{ {} }}",
                in_graph(graph, &format!("ex:s ex:p \"{o}\""))
            )
        };
        let ledger = fluree.ledger(&id).await.expect("load");
        drop(sparql(&fluree, ledger, &fact("x")).await);

        let built = incremental_build(&fluree, &id)
            .await
            .expect("incremental build");
        let bytes = fluree
            .content_store(
                &fluree
                    .storage_namespace(&id)
                    .await
                    .expect("storage namespace"),
            )
            .get(&built.root_id)
            .await
            .expect("root");
        let mut root = IndexRoot::decode(&bytes).expect("decode root");
        let g_id = graph_id(&fluree, &id, graph).await;
        empty_spot(&fluree, &id, &mut root, g_id).await;
        publish_root(&fluree, &id, built.index_t, &root).await;
        let from = from_alias(&id, graph, None);
        let fresh = fresh_at(dir.path());
        assert_eq!(
            objects(&fresh, &id, &from).await,
            Vec::<String>::new(),
            "{id}: by subject, before"
        );
        assert_eq!(
            select_o(&fresh, &id, &from, json!({"@id": "?s", "ex:p": "?o"})).await,
            ["\"x\""],
            "{id}: by predicate, before"
        );

        let ledger = fluree.ledger(&id).await.expect("load");
        drop(sparql(&fluree, ledger, &fact("y")).await);
        let refused = incremental_build(&fluree, &id).await;
        assert!(
            matches!(
                refused,
                Err(fluree_db_indexer::IndexerError::IncrementalAbort(_))
            ),
            "{id}: {refused:?}"
        );
        support::build_and_publish_index(&fluree, &id).await;
        assert_eq!(
            current(&fluree, dir.path(), &id, graph).await,
            ["\"x\"", "\"y\""],
            "{id}: by subject, after"
        );
    }
}
