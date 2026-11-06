//! SPARQL `DROP`: a dropped graph's facts are no longer queryable, at the head
//! or as of an earlier `t`, while the commits that wrote them keep them.
//! `CLEAR` keeps retracting.

#![cfg(feature = "native")]

use crate::support::{
    genesis_ledger, start_background_indexer_local, trigger_index_and_wait_outcome,
};
use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, TimeSpec};
use serde_json::Value as JsonValue;

const G: &str = "http://example.org/graphs/g";
const H: &str = "http://example.org/graphs/h";

/// Run a SPARQL update against the ledger's head. Background indexing never
/// starts on its own, so a test's index builds cover what it chose.
async fn update(fluree: &Fluree, ledger_id: &str, sparql: &str) -> fluree_db_api::Result<i64> {
    let handle = fluree.ledger_cached(ledger_id).await?;
    let result = fluree
        .stage(&handle)
        .sparql_update(sparql)
        .index_config(fluree_db_api::IndexConfig {
            reindex_min_bytes: 5_000_000_000,
            reindex_max_bytes: 5_000_000_000,
        })
        .execute()
        .await?;
    Ok(result.receipt.t)
}

/// The sorted `?o` values a SPARQL `SELECT ?o` returns.
async fn objects(
    fluree: &Fluree,
    db: &GraphDb,
    sparql: &str,
) -> fluree_db_api::Result<Vec<String>> {
    let result = fluree.query(db, sparql).await?;
    let rows = result.to_jsonld(&db.snapshot)?;
    let mut values: Vec<String> = rows
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| match row {
            JsonValue::Array(mut cells) if cells.len() == 1 => cells.remove(0).to_string(),
            other => other.to_string(),
        })
        .collect();
    values.sort();
    Ok(values)
}

fn in_graph(graph: &str) -> String {
    format!("SELECT ?o WHERE {{ GRAPH <{graph}> {{ ?s ?p ?o }} }}")
}

const DEFAULT_GRAPH: &str = "SELECT ?o WHERE { ?s ?p ?o }";
const GRAPHS: &str = "SELECT DISTINCT ?o WHERE { GRAPH ?o { ?s ?p ?x } }";

async fn head(fluree: &Fluree, ledger_id: &str) -> GraphDb {
    fluree.db(ledger_id).await.expect("head view")
}

async fn at(fluree: &Fluree, ledger_id: &str, t: i64) -> GraphDb {
    fluree
        .db_at(ledger_id, TimeSpec::AtT(t))
        .await
        .expect("historical view")
}

async fn listed_graphs(fluree: &Fluree, ledger_id: &str) -> Vec<String> {
    let info = fluree.ledger_info(ledger_id).execute().await.expect("info");
    info["ledger"]["named-graphs"]
        .as_array()
        .expect("named-graphs")
        .iter()
        .filter_map(|graph| graph["iri"].as_str().map(String::from))
        .collect()
}

async fn seed(fluree: &Fluree, ledger_id: &str) -> i64 {
    let ledger = genesis_ledger(fluree, ledger_id);
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .
        ex:d ex:v "default" .
        GRAPH <{G}> {{ ex:a ex:v "a" . ex:b ex:v "b" . }}
        GRAPH <{H}> {{ ex:z ex:v "z" . }}
        "#
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed")
        .receipt
        .t
}

#[tokio::test]
async fn drop_takes_a_graph_out_of_current_and_past_reads() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/reads:main";
    let seeded = seed(&fluree, ledger_id).await;

    let dropped = update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
        .await
        .expect("drop");
    assert_eq!(
        dropped,
        seeded + 1,
        "a drop retracts nothing and still commits"
    );

    let now = head(&fluree, ledger_id).await;
    assert!(objects(&fluree, &now, &in_graph(G))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        objects(&fluree, &now, &in_graph(H)).await.unwrap(),
        ["\"z\""]
    );
    assert_eq!(
        objects(&fluree, &now, DEFAULT_GRAPH).await.unwrap(),
        ["\"default\""]
    );
    assert_eq!(
        objects(&fluree, &now, GRAPHS).await.unwrap(),
        [format!("\"{H}\"")],
        "GRAPH ?g no longer finds it"
    );
    assert!(!listed_graphs(&fluree, ledger_id)
        .await
        .contains(&G.to_string()));

    // Its history is gone too: naming it as of a `t` before the drop is
    // refused, not answered as an empty graph.
    let before = at(&fluree, ledger_id, seeded).await;
    let err = objects(&fluree, &before, &in_graph(G)).await.unwrap_err();
    assert!(err.to_string().contains("dropped at t="), "{err}");
    assert_eq!(
        objects(&fluree, &before, &in_graph(H)).await.unwrap(),
        ["\"z\""]
    );
    let Err(err) = fluree
        .db_at(&format!("{ledger_id}#{G}"), TimeSpec::AtT(seeded))
        .await
    else {
        panic!("selecting the dropped graph as of before the drop");
    };
    assert!(err.to_string().contains("dropped at t="), "{err}");
    assert_eq!(
        objects(&fluree, &before, GRAPHS).await.unwrap(),
        [format!("\"{H}\"")]
    );
}

#[tokio::test]
async fn a_graph_written_after_its_drop_holds_only_the_new_facts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/recreate:main";
    seed(&fluree, ledger_id).await;

    // One request: the drop applies between the writes. The fact written
    // again is not swallowed as a restatement of the dropped one.
    let t = update(
        &fluree,
        ledger_id,
        &format!(
            "PREFIX ex: <http://example.org/> \
             INSERT DATA {{ GRAPH <{G}> {{ ex:c ex:v \"c\" }} }} ; \
             DROP GRAPH <{G}> ; \
             INSERT DATA {{ GRAPH <{G}> {{ ex:a ex:v \"a\" . ex:e ex:v \"e\" }} }}"
        ),
    )
    .await
    .expect("drop and rewrite");

    let now = head(&fluree, ledger_id).await;
    assert_eq!(
        objects(&fluree, &now, &in_graph(G)).await.unwrap(),
        ["\"a\"", "\"e\""]
    );
    assert!(listed_graphs(&fluree, ledger_id)
        .await
        .contains(&G.to_string()));

    // The facts written before the drop are history no read reaches.
    let err = objects(&fluree, &at(&fluree, ledger_id, t - 1).await, &in_graph(G))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("dropped at t="), "{err}");
}

/// Each operation of a request drops and revives at its own `t` within the
/// request: a graph written again after a drop can be dropped again.
#[tokio::test]
async fn a_request_can_drop_a_graph_it_wrote_again() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/redrop:main";
    seed(&fluree, ledger_id).await;

    update(
        &fluree,
        ledger_id,
        &format!(
            "PREFIX ex: <http://example.org/> \
             INSERT DATA {{ ex:d ex:w \"w\" }} ; \
             DROP GRAPH <{G}> ; \
             INSERT DATA {{ GRAPH <{G}> {{ ex:c ex:v \"c\" }} }} ; \
             DROP GRAPH <{G}>"
        ),
    )
    .await
    .expect("drop, write, drop");

    let now = head(&fluree, ledger_id).await;
    assert!(objects(&fluree, &now, &in_graph(G))
        .await
        .unwrap()
        .is_empty());
    assert!(!listed_graphs(&fluree, ledger_id)
        .await
        .contains(&G.to_string()));
    assert_eq!(
        objects(&fluree, &now, DEFAULT_GRAPH).await.unwrap(),
        ["\"default\"", "\"w\""]
    );
}

/// A delete names a dropped graph without bringing it back: it commits
/// nothing, and the graph stays unlisted.
#[tokio::test]
async fn a_delete_does_not_bring_a_dropped_graph_back() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/delete-after:main";
    seed(&fluree, ledger_id).await;
    let dropped = update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
        .await
        .expect("drop");

    for delete in [
        format!(
            "PREFIX ex: <http://example.org/> \
             DELETE DATA {{ GRAPH <{G}> {{ ex:a ex:v \"a\" }} }}"
        ),
        format!(
            "PREFIX ex: <http://example.org/> \
             DELETE {{ GRAPH <{G}> {{ ex:a ex:v \"a\" }} }} WHERE {{ ex:d ex:v ?any }}"
        ),
    ] {
        if let Ok(t) = update(&fluree, ledger_id, &delete).await {
            assert_eq!(t, dropped, "{delete}: nothing to commit");
        }
        assert!(
            !listed_graphs(&fluree, ledger_id)
                .await
                .contains(&G.to_string()),
            "{delete}"
        );
    }
    assert_eq!(head(&fluree, ledger_id).await.t, dropped);
}

#[tokio::test]
async fn clear_then_drop_still_drops() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/clear-drop:main";
    let seeded = seed(&fluree, ledger_id).await;

    let t = update(
        &fluree,
        ledger_id,
        &format!("CLEAR GRAPH <{G}> ; DROP GRAPH <{G}>"),
    )
    .await
    .expect("clear and drop");
    assert_eq!(t, seeded + 1, "the drop commits though it retracts nothing");
    assert!(!listed_graphs(&fluree, ledger_id)
        .await
        .contains(&G.to_string()));
    let err = objects(&fluree, &at(&fluree, ledger_id, seeded).await, &in_graph(G))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("dropped at t="), "{err}");
}

#[tokio::test]
async fn clear_keeps_the_graph_and_its_history() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/clear:main";
    let seeded = seed(&fluree, ledger_id).await;

    update(&fluree, ledger_id, &format!("CLEAR GRAPH <{G}>"))
        .await
        .expect("clear");
    assert!(
        objects(&fluree, &head(&fluree, ledger_id).await, &in_graph(G))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(listed_graphs(&fluree, ledger_id)
        .await
        .contains(&G.to_string()));
    assert_eq!(
        objects(&fluree, &at(&fluree, ledger_id, seeded).await, &in_graph(G))
            .await
            .unwrap(),
        ["\"a\"", "\"b\""]
    );
}

#[tokio::test]
async fn dropping_a_missing_graph_errors_unless_silent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/missing:main";
    let seeded = seed(&fluree, ledger_id).await;

    let err = update(
        &fluree,
        ledger_id,
        "DROP GRAPH <http://example.org/graphs/nope>",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");

    // SILENT: nothing to drop, nothing committed.
    let t = update(
        &fluree,
        ledger_id,
        "DROP SILENT GRAPH <http://example.org/graphs/nope>",
    )
    .await
    .expect("silent drop");
    assert_eq!(t, seeded);

    // A dropped graph no longer exists either.
    update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
        .await
        .expect("drop");
    let err = update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");
}

#[tokio::test]
async fn drop_default_keeps_the_default_graph_without_its_history() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/default:main";
    let seeded = seed(&fluree, ledger_id).await;

    update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/> \
         DROP DEFAULT ; INSERT DATA { ex:n ex:v \"new\" }",
    )
    .await
    .expect("drop default and write");

    let now = head(&fluree, ledger_id).await;
    assert_eq!(
        objects(&fluree, &now, DEFAULT_GRAPH).await.unwrap(),
        ["\"new\""]
    );
    assert_eq!(
        objects(&fluree, &now, &in_graph(G)).await.unwrap(),
        ["\"a\"", "\"b\""],
        "named graphs are untouched"
    );
    // The default graph as of before the drop reads empty: it always
    // exists, so naming it is never refused.
    assert!(objects(
        &fluree,
        &at(&fluree, ledger_id, seeded).await,
        DEFAULT_GRAPH
    )
    .await
    .unwrap()
    .is_empty());
}

#[tokio::test]
async fn drop_named_and_drop_all() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/named:main";
    seed(&fluree, ledger_id).await;

    update(&fluree, ledger_id, "DROP NAMED")
        .await
        .expect("drop named");
    let now = head(&fluree, ledger_id).await;
    assert!(objects(&fluree, &now, GRAPHS).await.unwrap().is_empty());
    assert_eq!(
        objects(&fluree, &now, DEFAULT_GRAPH).await.unwrap(),
        ["\"default\""]
    );

    update(&fluree, ledger_id, "DROP ALL")
        .await
        .expect("drop all");
    let now = head(&fluree, ledger_id).await;
    assert!(objects(&fluree, &now, DEFAULT_GRAPH)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn move_drops_its_source_and_copy_clears_its_destination() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/move:main";
    let seeded = seed(&fluree, ledger_id).await;

    update(&fluree, ledger_id, &format!("COPY <{G}> TO <{H}>"))
        .await
        .expect("copy");
    let copied = update(&fluree, ledger_id, &format!("MOVE <{H}> TO <{G}>"))
        .await
        .expect("move");

    let now = head(&fluree, ledger_id).await;
    assert_eq!(
        objects(&fluree, &now, &in_graph(G)).await.unwrap(),
        ["\"a\"", "\"b\""]
    );
    assert!(!listed_graphs(&fluree, ledger_id)
        .await
        .contains(&H.to_string()));
    // COPY cleared G's destination, which kept its history; MOVE dropped H,
    // whose history went with it.
    let before = at(&fluree, ledger_id, seeded).await;
    assert_eq!(
        objects(&fluree, &before, &in_graph(G)).await.unwrap(),
        ["\"a\"", "\"b\""]
    );
    let err = objects(
        &fluree,
        &at(&fluree, ledger_id, copied - 1).await,
        &in_graph(H),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("dropped at t="), "{err}");

    // MOVE onto itself stays a no-op.
    let t = update(&fluree, ledger_id, &format!("MOVE <{G}> TO <{G}>"))
        .await
        .expect("self move");
    assert_eq!(t, copied);
}

#[tokio::test]
async fn a_dropped_edge_written_again_has_no_annotations() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/annotations:main";
    fluree.create_ledger(ledger_id).await.expect("create");
    update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/> \
         INSERT DATA { ex:d ex:v \"x\" . ex:alice ex:worksFor ex:acme {| ex:source \"hr\" |} . }",
    )
    .await
    .expect("annotated edge");
    let annotations = "PREFIX ex: <http://example.org/> \
         SELECT ?o WHERE { << ex:alice ex:worksFor ex:acme >> ex:source ?o }";
    assert_eq!(
        objects(&fluree, &head(&fluree, ledger_id).await, annotations)
            .await
            .unwrap(),
        ["\"hr\""]
    );

    update(
        &fluree,
        ledger_id,
        "PREFIX ex: <http://example.org/> \
         DROP DEFAULT ; INSERT DATA { ex:alice ex:worksFor ex:acme . }",
    )
    .await
    .expect("drop and write the edge again");
    let now = head(&fluree, ledger_id).await;
    assert!(objects(&fluree, &now, annotations)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        objects(
            &fluree,
            &now,
            "PREFIX ex: <http://example.org/> SELECT ?o WHERE { ex:alice ex:worksFor ?o }"
        )
        .await
        .unwrap(),
        ["\"ex:acme\""]
    );
}

#[tokio::test]
async fn only_an_unrestricted_identity_drops() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/policy:main";
    seed(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.unwrap();

    // Allows everything, but through a policy.
    let opts = fluree_db_api::GovernanceOptions {
        policy: Some(serde_json::json!([{
            "@id": "ex:everything",
            "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
            "f:allow": true
        }])),
        default_allow: Some(false),
        ..Default::default()
    };
    let policy = fluree_db_api::policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("policy");

    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let drop = format!("DROP GRAPH <{G}>");
    let Err(err) = fluree
        .stage(&handle)
        .sparql_update(&drop)
        .policy(policy.clone())
        .execute()
        .await
    else {
        panic!("a policed drop");
    };
    assert!(err.to_string().contains("restricting policy"), "{err}");
    let mv = format!("MOVE <{G}> TO <{H}>");
    let Err(err) = fluree
        .stage(&handle)
        .sparql_update(&mv)
        .policy(policy.clone())
        .execute()
        .await
    else {
        panic!("a policed move");
    };
    assert!(err.to_string().contains("restricting policy"), "{err}");

    // CLEAR goes through under the same policy.
    let clear = format!("CLEAR GRAPH <{G}>");
    fluree
        .stage(&handle)
        .sparql_update(&clear)
        .policy(policy)
        .execute()
        .await
        .expect("a policed clear");
}

#[tokio::test]
async fn branch_operations_cross_a_drop_only_by_adopting_it() {
    use fluree_db_api::{CommitRef, ConflictStrategy};

    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/graph-drop/branches";
    let main = "it/graph-drop/branches:main";
    let dev = "it/graph-drop/branches:dev";
    fluree.create_ledger(main).await.expect("create");
    update(
        &fluree,
        main,
        "PREFIX ex: <http://example.org/> INSERT DATA { ex:d ex:v \"default\" }",
    )
    .await
    .unwrap();
    let seeded = update(
        &fluree,
        main,
        &format!(
            "PREFIX ex: <http://example.org/> \
             INSERT DATA {{ GRAPH <{G}> {{ ex:a ex:v \"a\" }} GRAPH <{H}> {{ ex:z ex:v \"z\" }} }}"
        ),
    )
    .await
    .unwrap();

    // A fast-forward adopts the drop as it was committed.
    fluree.create_branch(name, "dev", None, None).await.unwrap();
    update(&fluree, dev, &format!("DROP GRAPH <{H}>"))
        .await
        .unwrap();
    fluree
        .merge_branch(name, "dev", Some("main"), ConflictStrategy::default())
        .await
        .expect("fast-forward merge");
    assert_eq!(
        objects(&fluree, &head(&fluree, main).await, GRAPHS)
            .await
            .unwrap(),
        [format!("\"{G}\"")]
    );

    // Once both sides move, nothing adopts the drop: merge, rebase and the
    // merge preview refuse it.
    let dropped = update(&fluree, dev, &format!("DROP GRAPH <{G}>"))
        .await
        .unwrap();
    update(
        &fluree,
        main,
        "PREFIX ex: <http://example.org/> INSERT DATA { ex:m ex:v \"main\" }",
    )
    .await
    .unwrap();
    let err = fluree
        .merge_branch(name, "dev", Some("main"), ConflictStrategy::default())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("graph drop"), "{err}");
    let err = fluree
        .rebase_branch(name, "dev", ConflictStrategy::default())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("graph drop"), "{err}");

    // A revert cannot undo a drop, nor a write to a graph a later drop took,
    // and its preview says so too.
    for (commit, says) in [(dropped, "drops graphs"), (seeded, "dropped at")] {
        let err = fluree
            .revert_commit(
                name,
                "dev",
                CommitRef::T(commit),
                ConflictStrategy::TakeSource,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains(says), "{err}");
        let err = fluree
            .revert_commit_preview(name, "dev", CommitRef::T(commit))
            .await
            .unwrap_err();
        assert!(err.to_string().contains(says), "preview: {err}");
    }

    // Nothing moved.
    assert_eq!(
        objects(&fluree, &head(&fluree, main).await, &in_graph(G))
            .await
            .unwrap(),
        ["\"a\""]
    );
}

/// A transferred commit that writes a graph the ledger dropped brings the
/// graph back, as replaying that commit would.
#[tokio::test]
async fn a_transferred_write_brings_a_dropped_graph_back() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = "it/graph-drop/transfer:main";
    let dev = "it/graph-drop/transfer:dev";
    seed(&fluree, main).await;
    update(&fluree, main, &format!("DROP GRAPH <{G}>"))
        .await
        .expect("drop");
    fluree
        .create_branch("it/graph-drop/transfer", "dev", None, None)
        .await
        .expect("branch");
    update(
        &fluree,
        dev,
        &format!(
            "PREFIX ex: <http://example.org/> INSERT DATA {{ GRAPH <{G}> {{ ex:c ex:v \"c\" }} }}"
        ),
    )
    .await
    .expect("write the graph on dev");

    let page = fluree
        .export_commit_range(
            &fluree.ledger_cached(dev).await.unwrap(),
            &fluree_db_api::ExportCommitsRequest {
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .expect("export dev's write");
    fluree
        .import_commits_incremental(main, page.commits, page.merged_commits, page.blobs)
        .await
        .expect("import it into main");

    assert_eq!(
        objects(&fluree, &head(&fluree, main).await, &in_graph(G))
            .await
            .unwrap(),
        ["\"c\""]
    );
    assert!(listed_graphs(&fluree, main).await.contains(&G.to_string()));
}

/// A restart with nothing indexed replays the whole history, the writes
/// before the drop included: they stay out, and the fact written again after
/// the drop is there.
#[tokio::test]
async fn a_restart_replays_the_drop() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/graph-drop/replay:main";

    let fluree = FlureeBuilder::file(path.clone()).build().unwrap();
    fluree.create_ledger(ledger_id).await.expect("create");
    let ins = |facts: &str| {
        format!("PREFIX ex: <http://example.org/> INSERT DATA {{ GRAPH <{G}> {{ {facts} }} }}")
    };
    let written = update(
        &fluree,
        ledger_id,
        &ins("ex:a ex:v \"a\" . ex:b ex:v \"b\" ."),
    )
    .await
    .unwrap();
    let dropped = update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
        .await
        .unwrap();
    update(&fluree, ledger_id, &ins("ex:a ex:v \"a\" ."))
        .await
        .unwrap();
    drop(fluree);

    let mut fluree = FlureeBuilder::file(path).build().unwrap();
    let check = |fluree: Fluree| async move {
        assert_eq!(
            objects(&fluree, &head(&fluree, ledger_id).await, &in_graph(G))
                .await
                .unwrap(),
            ["\"a\""]
        );
        let err = objects(
            &fluree,
            &at(&fluree, ledger_id, written).await,
            &in_graph(G),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains(&format!("dropped at t={dropped}")),
            "{err}"
        );
    };
    check(fluree.clone()).await;

    // An index build over the same commits leaves out the same writes.
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    local
        .run_until(async {
            let t = fluree.ledger(ledger_id).await.unwrap().t();
            index_through(&fluree, &handle, ledger_id, t).await;
            assert_root_holds_the_drop(&fluree, ledger_id, t, dropped).await;
            check(fluree.clone()).await;
        })
        .await;
}

/// An incremental index build whose commits write a graph, drop it and write
/// it again indexes only the last write.
#[tokio::test]
async fn an_incremental_build_leaves_out_writes_before_a_drop_in_its_window() {
    let tmp = tempfile::TempDir::new().unwrap();
    let ledger_id = "it/graph-drop/window:main";
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    local
        .run_until(async {
            fluree.create_ledger(ledger_id).await.expect("create");
            let base = update(
                &fluree,
                ledger_id,
                "PREFIX ex: <http://example.org/> INSERT DATA { ex:d ex:v \"default\" }",
            )
            .await
            .unwrap();
            index_through(&fluree, &handle, ledger_id, base).await;

            let ins = |facts: &str| {
                format!(
                    "PREFIX ex: <http://example.org/> INSERT DATA {{ GRAPH <{G}> {{ {facts} }} }}"
                )
            };
            update(
                &fluree,
                ledger_id,
                &ins("ex:a ex:v \"a\" . ex:b ex:v \"b\" ."),
            )
            .await
            .unwrap();
            let dropped = update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
                .await
                .unwrap();
            let t = update(&fluree, ledger_id, &ins("ex:a ex:v \"a\" ."))
                .await
                .unwrap();
            index_through(&fluree, &handle, ledger_id, t).await;
            assert_root_holds_the_drop(&fluree, ledger_id, t, dropped).await;
            assert_eq!(
                objects(&fluree, &head(&fluree, ledger_id).await, &in_graph(G))
                    .await
                    .unwrap(),
                ["\"a\""]
            );
            assert_eq!(
                objects(&fluree, &head(&fluree, ledger_id).await, DEFAULT_GRAPH)
                    .await
                    .unwrap(),
                ["\"default\""]
            );
        })
        .await;
}

/// A full reindex merges the previous root's annotation arena into its own,
/// less what a drop took: an edge written again without its annotation has
/// none, in a query or in hydration, which reads the arena.
#[tokio::test]
async fn a_full_reindex_leaves_out_the_annotations_a_drop_took() {
    let ledger_id = "it/graph-drop/reindex-annotations:main";
    let mut fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    // The indexer reads the running ledger's annotations, and seals an arena.
    let (local, handle) = crate::support::start_background_indexer_with_attachments(
        &fluree,
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    let annotations = "PREFIX ex: <http://example.org/> \
         SELECT ?o WHERE { << ex:alice ex:worksFor ex:acme >> ex:source ?o }";
    local
        .run_until(async {
            fluree.create_ledger(ledger_id).await.expect("create");
            let t = update(
                &fluree,
                ledger_id,
                "PREFIX ex: <http://example.org/> \
                 INSERT DATA { ex:d ex:v \"x\" . ex:alice ex:worksFor ex:acme {| ex:source \"hr\" |} . }",
            )
            .await
            .unwrap();
            index_through(&fluree, &handle, ledger_id, t).await;
            assert!(
                published_root(&fluree, ledger_id).await.annotation_index.is_some(),
                "the previous root has an arena to merge"
            );

            update(
                &fluree,
                ledger_id,
                "PREFIX ex: <http://example.org/> \
                 DROP DEFAULT ; INSERT DATA { ex:alice ex:worksFor ex:acme . }",
            )
            .await
            .expect("drop and write the edge again");
            fluree
                .reindex(ledger_id, fluree_db_api::ReindexOptions::default())
                .await
                .expect("reindex");
            fluree
                .refresh(ledger_id, fluree_db_api::RefreshOpts::default())
                .await
                .expect("refresh");
            assert!(objects(&fluree, &head(&fluree, ledger_id).await, annotations)
                .await
                .unwrap()
                .is_empty());
            // Hydration reads the arena: the edge has no annotation, not the
            // reifier the drop took.
            let state = fluree.ledger(ledger_id).await.expect("load");
            let hydrated = crate::support::query_jsonld_formatted(
                &fluree,
                &state,
                &serde_json::json!({
                    "@context": {"ex": "http://example.org/"},
                    "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
                    "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
                }),
            )
            .await
            .expect("hydration");
            assert_eq!(
                hydrated,
                serde_json::json!([{"@id": "ex:alice", "ex:worksFor": {"@id": "ex:acme"}}])
            );
        })
        .await;
}

/// Before an index build covers a drop, the sealed annotation arena still
/// holds what the drop took: an edge written again without its annotation
/// hydrates and exports with none, from a fresh load and from the cached
/// head alike.
#[tokio::test]
async fn an_edge_written_again_after_a_drop_has_no_annotation_before_the_next_build() {
    use fluree_db_api::export::ExportFormat;

    let ledger_id = "it/graph-drop/annotations-before-build:main";
    let mut fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let (local, handle) = crate::support::start_background_indexer_with_attachments(
        &fluree,
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    let hydrate = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
        "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
    });
    local
        .run_until(async {
            fluree.create_ledger(ledger_id).await.expect("create");
            let t = update(
                &fluree,
                ledger_id,
                "PREFIX ex: <http://example.org/> \
                 INSERT DATA { ex:alice ex:worksFor ex:acme {| ex:source \"hr\" |} . }",
            )
            .await
            .unwrap();
            index_through(&fluree, &handle, ledger_id, t).await;
            assert!(published_root(&fluree, ledger_id)
                .await
                .annotation_index
                .is_some());

            update(
                &fluree,
                ledger_id,
                "PREFIX ex: <http://example.org/> \
                 DROP DEFAULT ; INSERT DATA { ex:alice ex:worksFor ex:acme . }",
            )
            .await
            .expect("drop and write the edge again");

            let fresh = fluree.ledger(ledger_id).await.expect("load");
            let cached = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached")
                .snapshot()
                .await
                .to_ledger_state();
            for (load, state) in [("fresh", &fresh), ("cached", &cached)] {
                assert!(state.snapshot.annotation_index.is_some(), "{load}");
                let hydrated = crate::support::query_jsonld_formatted(&fluree, state, &hydrate)
                    .await
                    .expect("hydration");
                assert_eq!(
                    hydrated,
                    serde_json::json!([{"@id": "ex:alice", "ex:worksFor": {"@id": "ex:acme"}}]),
                    "{load}"
                );
            }
            let mut turtle = Vec::new();
            fluree
                .export(ledger_id)
                .format(ExportFormat::Turtle)
                .write_to(&mut turtle)
                .await
                .expect("export");
            let turtle = String::from_utf8(turtle).unwrap();
            assert!(!turtle.contains('~') && !turtle.contains("{|"), "{turtle}");
        })
        .await;
}

/// Shapes that only the index held go with their graph: the compiled
/// shapes a transaction cached before the drop are not reused after it.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shapes_the_index_held_go_with_their_graph() {
    let tmp = tempfile::TempDir::new().unwrap();
    let ledger_id = "it/graph-drop/shapes:main";
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    let nameless =
        |s: &str| format!("PREFIX ex: <http://example.org/> INSERT DATA {{ ex:{s} a ex:User }}");
    local
        .run_until(async {
            let ledger = fluree.create_ledger(ledger_id).await.expect("create");
            fluree
                .insert(
                    ledger,
                    &serde_json::json!({
                        "@context": {"f": "https://ns.flur.ee/db#"},
                        "@id": "urn:config:main",
                        "@type": "f:LedgerConfig",
                        "@graph": "config",
                        "f:shaclDefaults": {"@id": "urn:config:shacl", "f:shaclEnabled": true}
                    }),
                )
                .await
                .expect("enable SHACL");
            let t = update(
                &fluree,
                ledger_id,
                "PREFIX ex: <http://example.org/> PREFIX sh: <http://www.w3.org/ns/shacl#> \
                 INSERT DATA { ex:NameShape a sh:NodeShape ; sh:targetClass ex:User ; \
                   sh:property [ sh:path ex:name ; sh:minCount 1 ] . }",
            )
            .await
            .unwrap();
            index_through(&fluree, &handle, ledger_id, t).await;
            // Refused under the shape, which compiles and caches it.
            update(&fluree, ledger_id, &nameless("u1"))
                .await
                .expect_err("a user needs a name");

            update(&fluree, ledger_id, "DROP DEFAULT")
                .await
                .expect("drop");
            update(&fluree, ledger_id, &nameless("u2"))
                .await
                .expect("no shape is left to refuse it");
        })
        .await;
}

/// A read as of a `t` the index covers learns of the drops after the index
/// from the cached head, reading none of the commits after it: with every
/// commit gone from storage, it still knows the drop. A view loaded with no
/// head at hand reads those commits for their drops.
#[tokio::test]
async fn a_read_below_the_index_takes_later_drops_from_the_head() {
    let tmp = tempfile::TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let ledger_id = "it/graph-drop/below-index:main";
    fluree.create_ledger(ledger_id).await.expect("create");
    let insert = |facts: &str| {
        format!("PREFIX ex: <http://example.org/> INSERT DATA {{ GRAPH <{G}> {{ {facts} }} }}")
    };
    let written = update(&fluree, ledger_id, &insert("ex:a ex:v \"a\" ."))
        .await
        .unwrap();
    crate::support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let dropped = update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
        .await
        .unwrap();
    for i in 0..3 {
        update(
            &fluree,
            ledger_id,
            &format!("PREFIX ex: <http://example.org/> INSERT DATA {{ ex:x ex:n {i} }}"),
        )
        .await
        .unwrap();
    }

    let view = fluree.ledger_view_at(ledger_id, written).await.unwrap();
    let registry = &view.snapshot.graph_registry;
    let g = registry.assigned_graph_id(G).unwrap();
    assert_eq!(registry.drop_after(g, written), Some(dropped));

    remove_commit_blobs(tmp.path());
    assert!(
        fluree.ledger_view_at(ledger_id, written).await.is_err(),
        "a view with no head reads the commits, which are gone"
    );
    let err = objects(
        &fluree,
        &at(&fluree, ledger_id, written).await,
        &in_graph(G),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains(&format!("dropped at t={dropped}")),
        "{err}"
    );
}

/// An export as of a `t` before a drop reads the graph as a query as of that
/// `t` does: naming it is refused, and an export that leaves it out does not
/// count it as a graph it omitted.
#[tokio::test]
async fn an_export_as_of_a_t_before_a_drop_reads_the_graph_as_a_query_does() {
    use fluree_db_api::export::ExportFormat;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-drop/export:main";
    fluree.create_ledger(ledger_id).await.expect("create");
    let insert = |graph: &str, facts: &str| {
        format!("PREFIX ex: <http://example.org/> INSERT DATA {{ GRAPH <{graph}> {{ {facts} }} }}")
    };
    update(&fluree, ledger_id, &insert(H, "ex:h ex:v \"h\" ."))
        .await
        .unwrap();
    let written = update(&fluree, ledger_id, &insert(G, "ex:a ex:v \"a\" ."))
        .await
        .unwrap();
    let dropped = update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
        .await
        .unwrap();
    let recreated = update(&fluree, ledger_id, &insert(G, "ex:b ex:v \"b\" ."))
        .await
        .unwrap();

    let err = fluree
        .export(ledger_id)
        .format(ExportFormat::NQuads)
        .graph(G)
        .as_of(TimeSpec::AtT(written))
        .write_to(&mut Vec::new())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains(&format!("dropped at t={dropped}")),
        "{err}"
    );
    for (t, omitted) in [(written, 1), (dropped, 1), (recreated, 2)] {
        let stats = fluree
            .export(ledger_id)
            .format(ExportFormat::NQuads)
            .as_of(TimeSpec::AtT(t))
            .write_to(&mut Vec::new())
            .await
            .unwrap();
        assert_eq!(stats.named_graphs_omitted, omitted, "t={t}");
    }
}

/// Remove every commit blob under `dir`.
fn remove_commit_blobs(dir: &std::path::Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            remove_commit_blobs(&path);
        } else if path.extension().is_some_and(|ext| ext == "fcv2") {
            std::fs::remove_file(&path).unwrap();
        }
    }
}

/// The acceptance sequence: write, clear, write again, drop, then write the
/// graph afresh — read at the head and as of every `t`, through a restart, an
/// incremental index build, a full reindex, and from a branch taken before
/// the drop, which still has what the drop took.
#[tokio::test]
async fn write_clear_write_drop_recreate_survives_restart_index_and_reindex() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/graph-drop/lifecycle:main";

    let open = |path: String| async move {
        let mut fluree = FlureeBuilder::file(path).build().unwrap();
        let (local, handle) = start_background_indexer_local(
            fluree.backend().clone(),
            fluree.nameservice_mode().publisher_arc().unwrap(),
            fluree_db_indexer::IndexerConfig::small(),
        );
        fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
        (fluree, local, handle)
    };

    let (fluree, local, handle) = open(path.clone()).await;
    let (written, cleared, rewritten, dropped, recreated) = local
        .run_until(async {
            fluree.create_ledger(ledger_id).await.expect("create");
            let ins = |facts: &str| {
                format!(
                    "PREFIX ex: <http://example.org/> INSERT DATA {{ GRAPH <{G}> {{ {facts} }} }}"
                )
            };
            let written = update(
                &fluree,
                ledger_id,
                &ins("ex:a ex:v \"a\" . ex:b ex:v \"b\" ."),
            )
            .await
            .unwrap();
            let cleared = update(&fluree, ledger_id, &format!("CLEAR GRAPH <{G}>"))
                .await
                .unwrap();
            let rewritten = update(
                &fluree,
                ledger_id,
                &ins("ex:a ex:v \"a\" . ex:c ex:v \"c\" ."),
            )
            .await
            .unwrap();
            // The index holds the graph and its history when it is dropped.
            index_through(&fluree, &handle, ledger_id, rewritten).await;
            let dropped = update(&fluree, ledger_id, &format!("DROP GRAPH <{G}>"))
                .await
                .unwrap();
            let recreated = update(
                &fluree,
                ledger_id,
                &ins("ex:a ex:v \"a\" . ex:d ex:v \"d\" ."),
            )
            .await
            .unwrap();
            (written, cleared, rewritten, dropped, recreated)
        })
        .await;

    // What every read must say, however the ledger got its state.
    let check = |fluree: Fluree, stage: &'static str| async move {
        let now = head(&fluree, ledger_id).await;
        assert_eq!(
            objects(&fluree, &now, &in_graph(G)).await.unwrap(),
            ["\"a\"", "\"d\""],
            "{stage}: only what was written after the drop"
        );
        assert!(
            listed_graphs(&fluree, ledger_id)
                .await
                .contains(&G.to_string()),
            "{stage}: the recreated graph is listed"
        );
        for t in [written, cleared, rewritten] {
            let err = objects(&fluree, &at(&fluree, ledger_id, t).await, &in_graph(G))
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains(&format!("dropped at t={dropped}")),
                "{stage}: t={t}: {err}"
            );
        }
        // Between the drop and the rewrite the graph does not exist.
        assert!(
            objects(
                &fluree,
                &at(&fluree, ledger_id, dropped).await,
                &in_graph(G)
            )
            .await
            .unwrap()
            .is_empty(),
            "{stage}: nothing as of the drop"
        );
        assert_eq!(
            objects(
                &fluree,
                &at(&fluree, ledger_id, recreated).await,
                &in_graph(G)
            )
            .await
            .unwrap(),
            ["\"a\"", "\"d\""],
            "{stage}: as of the rewrite"
        );
    };

    local
        .run_until(check(fluree.clone(), "before indexing the drop"))
        .await;

    // Restart: the drop is not in the index yet, so it comes back by replay.
    drop((fluree, handle));
    let (fluree, local, handle) = open(path.clone()).await;
    local
        .run_until(async {
            check(fluree.clone(), "after a restart").await;

            index_through(&fluree, &handle, ledger_id, recreated).await;
            assert_root_holds_the_drop(&fluree, ledger_id, recreated, dropped).await;
            check(fluree.clone(), "after indexing the drop").await;
            fluree.disconnect_ledger(ledger_id).await;
            check(fluree.clone(), "after loading the indexed drop").await;

            fluree
                .reindex(ledger_id, fluree_db_api::ReindexOptions::default())
                .await
                .expect("reindex");
            assert_root_holds_the_drop(&fluree, ledger_id, recreated, dropped).await;
            fluree
                .refresh(ledger_id, fluree_db_api::RefreshOpts::default())
                .await
                .expect("refresh");
            check(fluree.clone(), "after a full reindex").await;
            fluree.disconnect_ledger(ledger_id).await;
            check(fluree.clone(), "after loading the reindexed drop").await;

            // A branch from before the drop replays the commits that wrote
            // what it took: the drop never erased them.
            fluree
                .create_branch(
                    "it/graph-drop/lifecycle",
                    "before-drop",
                    None,
                    Some(TimeSpec::AtT(rewritten)),
                )
                .await
                .expect("branch before the drop");
            let branch = head(&fluree, "it/graph-drop/lifecycle:before-drop").await;
            assert_eq!(
                objects(&fluree, &branch, &in_graph(G)).await.unwrap(),
                ["\"a\"", "\"c\""]
            );
        })
        .await;
}

/// Build the index through `t` and have the cached ledger serve it, so the
/// reads that follow go through the new root rather than novelty.
async fn index_through(
    fluree: &Fluree,
    handle: &fluree_db_indexer::IndexerHandle,
    ledger_id: &str,
    t: i64,
) {
    trigger_index_and_wait_outcome(handle, ledger_id, t).await;
    fluree
        .refresh(ledger_id, fluree_db_api::RefreshOpts::default())
        .await
        .expect("refresh");
    let cached = fluree.ledger_cached(ledger_id).await.expect("cached");
    assert!(
        cached.index_t().await >= t,
        "the cached ledger serves the new index"
    );
}

/// The published index covers `t` and records the drop at `floor`, so a
/// reader of it needs no replay to know of the drop.
async fn assert_root_holds_the_drop(fluree: &Fluree, ledger_id: &str, t: i64, floor: i64) {
    let root = published_root(fluree, ledger_id).await;
    assert!(root.index_t >= t, "indexed through t={t}: {}", root.index_t);
    let g_id = root
        .graph_iris
        .iter()
        .position(|iri| iri == G)
        .map(|i| i as u16 + 1)
        .expect("the graph keeps its registration");
    let drop = root
        .graph_drops
        .iter()
        .find(|(id, _)| *id == g_id)
        .map(|(_, drop)| *drop)
        .expect("the root records the drop");
    assert_eq!(drop.floor, floor);
    assert!(drop.revived.is_some(), "and the rewrite after it");
}

/// The ledger's published index root.
async fn published_root(
    fluree: &Fluree,
    ledger_id: &str,
) -> fluree_db_binary_index::format::index_root::IndexRoot {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .expect("ledger record");
    let root_id = record.index_head_id.expect("an index");
    let bytes = fluree
        .backend()
        .content_store(&record.ledger_id)
        .get(&root_id)
        .await
        .unwrap();
    fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes).unwrap()
}
