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
    let ledger = genesis_ledger(fluree, ledger_id).await;
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
        .import_commits_incremental(
            main,
            fluree_db_api::PushCommitsRequest {
                commits: page.commits,
                merged_commits: page.merged_commits,
                blobs: page.blobs,
                ..Default::default()
            },
        )
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

const N: &str = "http://example.org/graphs/n";
const A: &str = "http://example.org/graphs/a";

fn drop_records(
    registry: &fluree_db_core::GraphRegistry,
) -> std::collections::BTreeMap<String, fluree_db_core::graph_registry::GraphDrop> {
    registry
        .graph_drops()
        .map(|(g_id, drop)| {
            let iri = if g_id == fluree_db_core::DEFAULT_GRAPH_ID {
                fluree_db_core::graph_registry::DEFAULT_GRAPH_IRI
            } else {
                registry.iri_for_graph_id(g_id).unwrap()
            };
            (iri.to_string(), drop)
        })
        .collect()
}

fn encode_commit(commit: &fluree_db_core::Commit) -> fluree_db_api::Base64Bytes {
    fluree_db_api::Base64Bytes(
        fluree_db_core::commit::codec::write_commit(commit, false, None)
            .unwrap()
            .bytes,
    )
}

async fn transfer(
    fluree: &Fluree,
    id: &str,
    push: bool,
    commits: Vec<fluree_db_api::Base64Bytes>,
    blobs: std::collections::HashMap<String, fluree_db_api::Base64Bytes>,
) -> fluree_db_api::Result<()> {
    if push {
        fluree
            .push_commits(
                id,
                fluree_db_api::PushCommitsRequest {
                    commits,
                    blobs,
                    ..Default::default()
                },
                &fluree_db_api::GovernanceOptions::default(),
                &fluree.default_index_config(),
            )
            .await
            .map(|_| ())
    } else {
        fluree
            .import_commits_incremental(
                id,
                fluree_db_api::PushCommitsRequest {
                    commits,
                    blobs,
                    ..Default::default()
                },
            )
            .await
            .map(|_| ())
    }
}

/// Drop records must name user graphs known at that commit. Invalid targets
/// must not turn an otherwise empty commit into an accepted change.
#[tokio::test]
async fn transfers_reject_unknown_and_system_graph_drops() {
    use fluree_db_core::graph_registry::{config_graph_iri, txn_meta_graph_iri};
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/graph-drop/invalid-targets:main";
    let t = seed(&fluree, id).await;
    let base = fluree.ledger(id).await.unwrap().head_commit_id.unwrap();
    for target in [
        N.to_string(),
        config_graph_iri(id),
        config_graph_iri("another:main"),
        txn_meta_graph_iri(id),
    ] {
        for mixed in [false, true] {
            let mut commit =
                fluree_db_core::Commit::new(t + 1, Vec::new()).with_parent(base.clone());
            commit.graph_drops = vec![target.clone()];
            if mixed {
                commit.graph_drops.push(G.to_string());
                commit.graph_drops.sort();
            }
            let bytes = encode_commit(&commit);
            for push in [true, false] {
                let result =
                    transfer(&fluree, id, push, vec![bytes.clone()], Default::default()).await;
                let error =
                    result.expect_err(&format!("push={push}, target={target}, mixed={mixed}"));
                assert_eq!(error.status_code(), 422, "{error}");
                assert!(error.to_string().contains(&target), "{error}");
                let state = fluree.ledger(id).await.unwrap();
                assert_eq!(state.head_commit_id.as_ref(), Some(&base));
                assert_eq!(state.t(), t);
                assert!(drop_records(&state.snapshot.graph_registry).is_empty());
            }
        }
    }
}

/// Registering a graph later in a batch cannot authorize an earlier drop.
/// Batch and one-commit-at-a-time delivery reject the same first commit.
#[tokio::test]
async fn transfers_reject_a_drop_before_its_graph_is_registered() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/graph-drop/future-target";
    let source = "it/graph-drop/future-target:main";
    let t = seed(&fluree, source).await;
    let base = fluree.ledger(source).await.unwrap().head_commit_id.unwrap();
    let receiver = format!("{name}:receiver");
    fluree
        .create_branch(name, "receiver", None, None)
        .await
        .unwrap();
    update(&fluree, source, &format!("DROP GRAPH <{G}>"))
        .await
        .unwrap();
    update(
        &fluree,
        source,
        &format!("INSERT DATA {{ GRAPH <{N}> {{ <urn:s> <urn:p> \"later\" }} }}"),
    )
    .await
    .unwrap();
    let mut page = commits_above(&fluree, source, &base).await;
    assert_eq!(page.commits.len(), 2);
    let mut first = fluree_db_core::commit::codec::read_commit(&page.commits[0].0).unwrap();
    first.graph_drops = vec![N.to_string()];
    page.commits[0] = encode_commit(&first);
    let mut second = fluree_db_core::commit::codec::read_commit(&page.commits[1].0).unwrap();
    second.parents[0] =
        fluree_db_core::ContentId::new(fluree_db_core::ContentKind::Commit, &page.commits[0].0);
    page.commits[1] = encode_commit(&second);
    for push in [true, false] {
        for count in [2, 1] {
            let commits = page.commits[..count].to_vec();
            let result = transfer(&fluree, &receiver, push, commits, page.blobs.clone()).await;
            let error = result.expect_err(&format!("push={push}, count={count}"));
            assert_eq!(error.status_code(), 422, "{error}");
            assert!(error.to_string().contains(N), "{error}");
            let state = fluree.ledger(&receiver).await.unwrap();
            assert_eq!(state.head_commit_id.as_ref(), Some(&base));
            assert_eq!(state.t(), t);
            assert!(drop_records(&state.snapshot.graph_registry).is_empty());
        }
    }
}

/// What a reader of `ledger_id` gets from each graph as of each `t` in `ts`:
/// its sorted objects, or why the read was refused.
async fn reads_as_of(fluree: &Fluree, ledger_id: &str, ts: &[i64]) -> Vec<String> {
    let mut reads = Vec::new();
    for &t in ts {
        let db = at(fluree, ledger_id, t).await;
        for query in [
            DEFAULT_GRAPH.to_string(),
            in_graph(G),
            in_graph(H),
            in_graph(N),
            in_graph(A),
            GRAPHS.to_string(),
        ] {
            let read = match objects(fluree, &db, &query).await {
                Ok(values) => values.join(" "),
                Err(e) => e.to_string(),
            };
            reads.push(format!("t={t} {query} -> {read}"));
        }
    }
    reads
}

/// The commits `ledger_id` holds above `base`, oldest first, as a push or a
/// pull carries them.
async fn commits_above(
    fluree: &Fluree,
    ledger_id: &str,
    base: &fluree_db_api::ContentId,
) -> fluree_db_api::ExportCommitsResponse {
    let mut page = fluree
        .export_commit_range(
            &fluree.ledger_cached(ledger_id).await.unwrap(),
            &fluree_db_api::ExportCommitsRequest {
                lineage: true,
                base_id: Some(base.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("export");
    page.commits.reverse();
    page
}

/// Push and pull carry drops: the receiver applies each commit's drops
/// before its flakes, as the writer did, so it reads what the writer reads,
/// at the head and as of every `t`, and again once a reload replays the
/// commits. The receivers start from an index that holds the graphs the
/// commits drop, and one of them indexes the commits afterward. The commits
/// hold a drop alone, a write of the dropped graph again, a graph they
/// register, drop and write again in one commit, and `DROP DEFAULT`; one
/// pull takes them a commit at a time.
#[tokio::test]
async fn push_and_pull_apply_drops_as_the_writer_did() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    let name = "it/graph-drop/push-pull";
    let main = "it/graph-drop/push-pull:main";
    let receivers = ["pushed", "pulled", "paged"];
    let (seeded, base) = local
        .run_until(async {
            let seeded = seed(&fluree, main).await;
            index_through(&fluree, &handle, main, seeded).await;
            let base = fluree
                .ledger(main)
                .await
                .unwrap()
                .head_commit_id
                .expect("seeded head");
            for branch in receivers {
                fluree
                    .create_branch(name, branch, None, None)
                    .await
                    .unwrap();
            }
            (seeded, base)
        })
        .await;
    // The receivers keep the index they start from until the test builds
    // one: the transfers must not start builds of their own.
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Disabled);

    local
        .run_until(async {
            for branch in receivers {
                let receiver = fluree.ledger(&format!("{name}:{branch}")).await.unwrap();
                assert_eq!(receiver.index_t(), seeded, "{branch} starts from the index");
            }
            let ins = |graph: &str, facts: &str| {
                format!(
                    "PREFIX ex: <http://example.org/> INSERT DATA {{ GRAPH <{graph}> {{ {facts} }} }}"
                )
            };
            let mut ts = vec![seeded];
            for sparql in [
                format!("DROP GRAPH <{G}>"),
                ins(G, "ex:a ex:v \"a\" . ex:c ex:v \"c\" ."),
                ins(N, "ex:n ex:v \"old\" ."),
                format!(
                    "PREFIX ex: <http://example.org/> \
                     DROP GRAPH <{N}> ; INSERT DATA {{ GRAPH <{N}> {{ ex:n ex:v \"new\" }} }}"
                ),
                format!(
                    "{} ; DROP GRAPH <{A}> ; {}",
                    ins(A, "ex:a ex:v \"before\" ."),
                    ins(A, "ex:a ex:v \"after\" ."),
                ),
                "DROP DEFAULT".to_string(),
            ] {
                ts.push(update(&fluree, main, &sparql).await.expect(&sparql));
            }
            let head_t = *ts.last().unwrap();
            let written = reads_as_of(&fluree, main, &ts).await;
            let at_head = |query: &str| {
                written
                    .iter()
                    .find(|read| read.starts_with(&format!("t={head_t} {query} -> ")))
                    .unwrap()
                    .clone()
            };
            assert!(at_head(DEFAULT_GRAPH).ends_with("-> "));
            assert!(at_head(&in_graph(G)).ends_with("-> \"a\" \"c\""));
            assert!(at_head(&in_graph(N)).ends_with("-> \"new\""));
            assert!(at_head(&in_graph(A)).ends_with("-> \"after\""));
            assert!(written.iter().any(|read| {
                read.starts_with(&format!("t={seeded} {} -> ", in_graph(G)))
                    && read.contains(&format!("graph <{G}> was dropped at t={}", ts[1]))
            }));

            let page = commits_above(&fluree, main, &base).await;
            assert_eq!(page.commits.len(), ts.len() - 1);
            let expected_drops = drop_records(&fluree.ledger(main).await.unwrap().snapshot.graph_registry);
            let mut caught_up = fluree.ledger(&format!("{name}:pushed")).await.unwrap();
            for bytes in &page.commits {
                let mut commit = fluree_db_core::commit::codec::read_commit(&bytes.0).unwrap();
                commit.id = Some(fluree_db_core::ContentId::new(
                    fluree_db_core::ContentKind::Commit, &bytes.0,
                ));
                caught_up.apply_single_commit(commit, main).unwrap();
                fluree_db_transact::hide_dropped_graphs(&mut caught_up);
            }
            assert_eq!(drop_records(&caught_up.snapshot.graph_registry), expected_drops, "peer catch-up");
            let pushed = fluree
                .push_commits(
                    &format!("{name}:pushed"),
                    fluree_db_api::PushCommitsRequest {
                        commits: page.commits.clone(),
                        blobs: page.blobs.clone(),
                        ..Default::default()
                    },
                    &fluree_db_api::GovernanceOptions::default(),
                    &fluree.default_index_config(),
                )
                .await
                .expect("push");
            assert_eq!(pushed.head.t, head_t);
            fluree
                .import_commits_incremental(
                    &format!("{name}:pulled"),
                    fluree_db_api::PushCommitsRequest {
                        commits: page.commits.clone(),
                        blobs: page.blobs.clone(),
                        ..Default::default()
                    },
                )
                .await
                .expect("pull");
            for commit in &page.commits {
                fluree
                    .import_commits_incremental(
                        &format!("{name}:paged"),
                        fluree_db_api::PushCommitsRequest {
                            commits: vec![commit.clone()],
                            blobs: page.blobs.clone(),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("pull a commit at a time");
            }

            for branch in receivers {
                let receiver = format!("{name}:{branch}");
                assert_eq!(fluree.ledger(&receiver).await.unwrap().index_t(), seeded);
                assert_eq!(
                    reads_as_of(&fluree, &receiver, &ts).await,
                    written,
                    "{branch}"
                );
                assert_eq!(drop_records(&fluree.ledger(&receiver).await.unwrap().snapshot.graph_registry), expected_drops, "{branch}");
                fluree.disconnect_ledger(&receiver).await;
                assert_eq!(
                    reads_as_of(&fluree, &receiver, &ts).await,
                    written,
                    "{branch}, reloaded"
                );
                assert_eq!(drop_records(&fluree.ledger(&receiver).await.unwrap().snapshot.graph_registry), expected_drops, "{branch}, reloaded");
            }

            // An index the receiver builds over the commits leaves out what
            // their drops took.
            let pushed = format!("{name}:pushed");
            index_through(&fluree, &handle, &pushed, head_t).await;
            let now = |reads: &[String]| -> Vec<String> {
                reads
                    .iter()
                    .filter(|read| read.starts_with(&format!("t={head_t} ")))
                    .cloned()
                    .collect()
            };
            assert_eq!(
                now(&reads_as_of(&fluree, &pushed, &[head_t]).await),
                now(&written),
                "indexed"
            );
            assert_eq!(drop_records(&fluree.ledger(&pushed).await.unwrap().snapshot.graph_registry), expected_drops, "indexed");
        })
        .await;
}

/// A push that drops validates each commit against the state the drops
/// leave, as the writer staged it: a vector written at a new length into a
/// graph whose vectors of the old length the drop took is accepted, not
/// refused as a second length.
#[tokio::test]
async fn a_push_validates_a_commit_after_its_drop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/graph-drop/push-validates";
    let main = "it/graph-drop/push-validates:main";
    fluree.create_ledger(main).await.unwrap();
    let vector = |len: usize| {
        let parts: Vec<String> = (1..=len).map(|i| format!("{i}.0")).collect();
        format!(
            "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#> \
             INSERT DATA {{ GRAPH <{G}> {{ ex:a ex:embedding \"[{}]\"^^f:embeddingVector }} }}",
            parts.join(", ")
        )
    };
    update(&fluree, main, &vector(3)).await.unwrap();
    let base = fluree
        .ledger(main)
        .await
        .unwrap()
        .head_commit_id
        .expect("head");
    fluree
        .create_branch(name, "pushed", None, None)
        .await
        .unwrap();
    update(&fluree, main, &format!("DROP GRAPH <{G}>"))
        .await
        .unwrap();
    let head_t = update(&fluree, main, &vector(4))
        .await
        .expect("a new length after the drop");

    let page = commits_above(&fluree, main, &base).await;
    let pushed = fluree
        .push_commits(
            "it/graph-drop/push-validates:pushed",
            fluree_db_api::PushCommitsRequest {
                commits: page.commits,
                blobs: page.blobs,
                ..Default::default()
            },
            &fluree_db_api::GovernanceOptions::default(),
            &fluree.default_index_config(),
        )
        .await
        .expect("the push validates the write after the drop");
    assert_eq!(pushed.head.t, head_t);
}

/// Only an identity without a restricting policy may push a drop, as only
/// one may drop locally. The policy is the one in force before the commit:
/// a `DROP DEFAULT` that takes the default graph's policies is still refused,
/// though the state after it would hold none to restrict the identity.
#[tokio::test]
async fn a_push_that_drops_needs_an_identity_without_a_restricting_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/graph-drop/push-policy";
    let main = "it/graph-drop/push-policy:main";
    let target = "it/graph-drop/push-policy:target";
    fluree.create_ledger(main).await.unwrap();
    update(
        &fluree,
        main,
        "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#> \
         INSERT DATA { ex:noSsnEdits a ex:Restricted ; f:action f:modify ; \
         f:onProperty ex:ssn ; f:allow false . ex:d ex:v \"default\" }",
    )
    .await
    .unwrap();
    let base = fluree
        .ledger(main)
        .await
        .unwrap()
        .head_commit_id
        .expect("head");
    fluree
        .create_branch(name, "target", None, None)
        .await
        .unwrap();
    fluree
        .create_branch(name, "plain", None, None)
        .await
        .unwrap();
    update(&fluree, main, "DROP DEFAULT").await.unwrap();
    update(
        &fluree,
        &format!("{name}:plain"),
        "PREFIX ex: <http://example.org/> INSERT DATA { ex:p ex:v \"plain\" }",
    )
    .await
    .unwrap();

    let restricted = fluree_db_api::GovernanceOptions {
        policy_class: Some(vec!["http://example.org/Restricted".to_string()]),
        default_allow: Some(true),
        ..Default::default()
    };
    let push = |from: String| {
        let fluree = &fluree;
        let base = &base;
        let restricted = &restricted;
        async move {
            let page = commits_above(fluree, &from, base).await;
            fluree
                .push_commits(
                    target,
                    fluree_db_api::PushCommitsRequest {
                        commits: page.commits,
                        blobs: page.blobs,
                        ..Default::default()
                    },
                    restricted,
                    &fluree.default_index_config(),
                )
                .await
        }
    };
    let err = push(main.to_string())
        .await
        .expect_err("a restricted identity pushing DROP DEFAULT");
    assert_eq!(err.status_code(), 403, "{err}");
    assert!(
        err.to_string()
            .contains("Policy enforcement prevents modification"),
        "{err}"
    );
    assert_eq!(
        objects(&fluree, &head(&fluree, target).await, DEFAULT_GRAPH)
            .await
            .unwrap()
            .len(),
        5,
        "nothing applied"
    );
    push(format!("{name}:plain"))
        .await
        .expect("the same identity pushes a commit that drops nothing");
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

/// A second process over the same storage (a query peer, a raft follower)
/// that has the ledger loaded catches up a drop by applying the commit, as
/// its writer did: the dropped graph's facts leave its novelty before the
/// commit writes the graph again, and as of before the drop the graph is
/// refused there too.
#[tokio::test]
async fn a_peer_catching_up_a_drop_commit_applies_the_drop() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/graph-drop/peer:main";

    let writer = FlureeBuilder::file(path.clone()).build().unwrap();
    writer.create_ledger(ledger_id).await.expect("create");
    let written = update(
        &writer,
        ledger_id,
        &format!(
            "PREFIX ex: <http://example.org/> INSERT DATA {{ ex:d ex:v \"default\" . \
             GRAPH <{G}> {{ ex:a ex:v \"a\" . ex:b ex:v \"b\" . }} }}"
        ),
    )
    .await
    .unwrap();

    let peer = FlureeBuilder::file(path)
        .without_storage_lock()
        .build()
        .unwrap();
    assert_eq!(
        objects(&peer, &head(&peer, ledger_id).await, &in_graph(G))
            .await
            .unwrap(),
        ["\"a\"", "\"b\""]
    );
    let catch_up = |t: i64| {
        let peer = &peer;
        async move {
            let refreshed = peer
                .refresh(ledger_id, fluree_db_api::RefreshOpts::default())
                .await
                .expect("refresh")
                .expect("ledger");
            assert_eq!(
                refreshed.action,
                fluree_db_api::ledger_manager::NotifyResult::CommitsApplied { count: 1 },
                "the peer applies the commit rather than reloading"
            );
            assert_eq!(refreshed.t, t);
        }
    };

    let dropped = update(
        &writer,
        ledger_id,
        &format!(
            "PREFIX ex: <http://example.org/> \
             DROP GRAPH <{G}> ; INSERT DATA {{ GRAPH <{G}> {{ ex:a ex:v \"a\" }} }}"
        ),
    )
    .await
    .unwrap();
    catch_up(dropped).await;
    let now = head(&peer, ledger_id).await;
    assert_eq!(objects(&peer, &now, &in_graph(G)).await.unwrap(), ["\"a\""]);
    assert_eq!(
        objects(&peer, &now, DEFAULT_GRAPH).await.unwrap(),
        ["\"default\""]
    );
    let err = objects(&peer, &at(&peer, ledger_id, written).await, &in_graph(G))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains(&format!("dropped at t={dropped}")),
        "{err}"
    );

    let dropped_default = update(&writer, ledger_id, "DROP DEFAULT").await.unwrap();
    catch_up(dropped_default).await;
    let now = head(&peer, ledger_id).await;
    assert!(objects(&peer, &now, DEFAULT_GRAPH)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(objects(&peer, &now, &in_graph(G)).await.unwrap(), ["\"a\""]);
    assert!(
        objects(&peer, &at(&peer, ledger_id, written).await, DEFAULT_GRAPH)
            .await
            .unwrap()
            .is_empty()
    );
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

/// A full reindex replays the commits less what a drop took, the annotation
/// and its `rdf:reifies` link with it: an edge written again without its
/// annotation has none, in a query or in hydration.
#[tokio::test]
async fn a_full_reindex_leaves_out_the_annotations_a_drop_took() {
    let ledger_id = "it/graph-drop/reindex-annotations:main";
    let mut fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let (local, handle) = crate::support::start_background_indexer_for(
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
            // Hydration reads the links: the edge has no annotation, not the
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

/// Before an index build covers a drop, the index still holds the annotation
/// and link the drop took: an edge written again without its annotation
/// hydrates and exports with none, from a fresh load and from the cached
/// head alike.
#[tokio::test]
async fn an_edge_written_again_after_a_drop_has_no_annotation_before_the_next_build() {
    use fluree_db_api::export::ExportFormat;

    let ledger_id = "it/graph-drop/annotations-before-build:main";
    let mut fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let (local, handle) = crate::support::start_background_indexer_for(
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
            assert!(published_root(&fluree, ledger_id).await.term_dict.is_some());

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
                assert!(state.snapshot.has_annotations, "{load}");
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
        .map(|i| i as fluree_db_core::GraphId + 1)
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
    let root_id = record.index_head_id.clone().expect("an index");
    let bytes = fluree
        .backend()
        .content_store(&record.storage_namespace())
        .get(&root_id)
        .await
        .unwrap();
    fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes).unwrap()
}
