use super::*;

fn assert_shared_cache(state: &LedgerState, ledger: &JournalLedger) {
    let store = state
        .binary_store
        .as_ref()
        .unwrap()
        .0
        .downcast_ref::<fluree_db_binary_index::BinaryIndexStore>()
        .unwrap();
    assert!(Arc::ptr_eq(
        store
            .leaflet_cache()
            .expect("indexed reads need a bounded shared cache"),
        ledger.0.engine.leaflet_cache()
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_import_bootstrap_reads_writes_and_recovers_indexed_state() {
    let source_dir = tempfile::tempdir().unwrap();
    let input_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let input = input_dir.path().join("people.ttl");
    std::fs::write(
        &input,
        r#"
@prefix ex: <http://example.org/wal/> .
ex:one a ex:User ; ex:value 10 ; ex:friend ex:two .
ex:two a ex:User ; ex:value 20 .
"#,
    )
    .unwrap();
    let source = FlureeBuilder::file(source_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let imported = source
        .create("indexed:main")
        .import(&input)
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .unwrap();
    assert!(imported.root_id.is_some());
    let ledger = JournalLedger::bootstrap(
        target_dir.path().into(),
        source_dir.path().into(),
        "indexed:main".into(),
        "test-1".into(),
    )
    .await
    .unwrap();
    let query = json!({"@context":{"ex":"http://example.org/wal/"},"select":["?id","?v"],"where":{"@id":"?id","ex:value":"?v"},"orderBy":"?id"});
    assert_eq!(
        ledger.query(&query).await.unwrap(),
        json!([["ex:one", 10], ["ex:two", 20]])
    );
    {
        let cache = ledger.ready().await.unwrap();
        assert_shared_cache(cache.state.as_ref().unwrap(), &ledger);
        assert_eq!(
            cache
                .state
                .as_ref()
                .unwrap()
                .ns_record
                .as_ref()
                .unwrap()
                .index_t,
            imported.t
        );
    }
    // A second attachment replaces the owner's checkpoint handle; the first
    // adapter's proof remains valid because it binds the exact manifest digest.
    drop(JournalLedger::open(target_dir.path().into()).await.unwrap());
    let accepted = ledger
        .transact(TxnType::Upsert, &json!({"@id":"ex:one","ex:value":11}))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ledger.query(&query).await.unwrap(),
        json!([["ex:one", 11], ["ex:two", 20]])
    );
    let without_context = json!({"select":["?id"],"where":{"@id":"?id","ex:value":11}});
    assert_eq!(
        ledger.query(&without_context).await.unwrap(),
        json!([["ex:one"]])
    );
    let added = ledger.transact(TxnType::Insert, &json!({"@context":{"new":"http://new.example/","ex":"http://example.org/wal/"},"@id":"new:three","new:friend":{"@id":"ex:two"},"new:label":"new string"})).await.unwrap().unwrap();
    let bound = json!({"@context":{"new":"http://new.example/","ex":"http://example.org/wal/"},"select":["?id","?label"],"where":{"@id":"?id","new:friend":{"@id":"ex:two"},"new:label":"?label"}});
    assert_eq!(
        ledger.query(&bound).await.unwrap(),
        json!([["new:three", "new string"]])
    );
    assert_shared_cache(
        ledger.ready().await.unwrap().state.as_ref().unwrap(),
        &ledger,
    );
    let commit_bytes = ledger.content(&accepted.commit.commit_id).await.unwrap();
    drop(ledger);
    drop(source);
    // Remove both the source and all target materialization. Only the checkpoint
    // and journal remain available to reconstruct indexed state plus new novelty.
    source_dir.close().unwrap();
    for _ in 0..2 {
        std::fs::remove_dir_all(target_dir.path().join(".fluree-wal/data")).unwrap();
        std::fs::create_dir(target_dir.path().join(".fluree-wal/data")).unwrap();
        let ledger = JournalLedger::open(target_dir.path().into()).await.unwrap();
        assert_shared_cache(
            ledger.ready().await.unwrap().state.as_ref().unwrap(),
            &ledger,
        );
        assert_eq!(
            ledger.query(&query).await.unwrap(),
            json!([["ex:one", 11], ["ex:two", 20]])
        );
        assert_eq!(
            ledger.query(&bound).await.unwrap(),
            json!([["new:three", "new string"]])
        );
        assert_eq!(
            ledger.content(&accepted.commit.commit_id).await.unwrap(),
            commit_bytes
        );
        assert_eq!(
            ledger.head().await.unwrap().unwrap().id,
            Some(added.commit.commit_id.clone())
        );
    }
}

fn rows(value: &Value) -> Vec<Value> {
    value["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["row"].clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_frozen_cypher_corpus_preserves_annotations_and_external_acknowledgments() {
    use crate::local_journal_ledger::fixtures::{OBSERVE, WRITES};
    use std::io::Write;
    if std::env::var_os("FLUREE_WAL_INDEXED_CORPUS_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "local_journal_ledger::indexed::tests::indexed_frozen_cypher_corpus_preserves_annotations_and_external_acknowledgments", "--nocapture"])
            .env("FLUREE_WAL_INDEXED_CORPUS_CHILD", "1").env("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1").output().unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let source_dir = tempfile::tempdir().unwrap();
    let input_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let controller = tempfile::tempdir().unwrap();
    let source = FlureeBuilder::file(source_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let script = "CREATE (:User {id:1, age:30}); CREATE (:User {id:2, age:20}); MATCH (a:User {id:1}), (b:User {id:2}) CREATE (a)-[:knows {weight:7}]->(b);";
    let objects = crate::cypher_import::cypher_to_jsonld(
        script,
        &crate::cypher_import::CypherImportOptions::default(),
    )
    .unwrap();
    let input = input_dir.path().join("seed.jsonld");
    std::fs::write(
        &input,
        serde_json::to_vec(&json!({"@graph":objects})).unwrap(),
    )
    .unwrap();
    let imported = source
        .create("indexed-cypher:main")
        .import(&input)
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .unwrap();
    assert!(imported.has_annotations);
    // Same follow-up as the CLI: bulk import does not itself seal annotation arenas.
    source
        .reindex("indexed-cypher:main", crate::ReindexOptions::default())
        .await
        .unwrap();
    let source_record = source
        .nameservice()
        .lookup("indexed-cypher:main")
        .await
        .unwrap()
        .unwrap();
    let root_id = source_record.index_head_id.as_ref().unwrap();
    let root = fluree_db_binary_index::IndexRoot::decode(
        &source
            .content_store("indexed-cypher:main")
            .get(root_id)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        root.has_annotations && root.annotation_index.is_some(),
        "fixture must contain actual persisted annotation arenas"
    );
    let historical_root = root
        .prev_index
        .as_ref()
        .expect("reindex fixture must retain previous index root")
        .id
        .clone();
    let annotation = root.annotation_index.as_ref().unwrap();
    let branch_bytes = source
        .content_store("indexed-cypher:main")
        .get(&annotation.forward_branch_cid)
        .await
        .unwrap();
    let branch = fluree_db_binary_index::annotation_arena::format::AnnotationForwardBranch::decode(
        &branch_bytes,
    )
    .unwrap();
    let annotation_leaf = branch.leaves.first().unwrap().leaf_cid.clone();
    let ledger = JournalLedger::bootstrap(
        target_dir.path().into(),
        source_dir.path().into(),
        "indexed-cypher:main".into(),
        "test-1".into(),
    )
    .await
    .unwrap();
    let annotation_query = "MATCH (:User {id:1})-[r:knows]->(:User {id:2}) RETURN r.weight";
    assert_eq!(
        rows(&ledger.query_cypher(annotation_query, None).await.unwrap()),
        vec![json!([7])]
    );
    let mut state = source.ledger("indexed-cypher:main").await.unwrap();
    let mut acks = Vec::new();
    for (i, (name, statement)) in WRITES.iter().enumerate() {
        let params =
            json!({"from":1,"to":2,"id":if *name == "arango__single_vertex_write" {10} else {1}})
                .as_object()
                .unwrap()
                .clone();
        let result = ledger
            .transact_cypher(statement, Some(&params))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let ack = result
            .commit
            .expect("each case must mutate indexed fixture");
        assert_eq!(ack.commit.t, imported.t + i as i64 + 1);
        let (normal, normal_return) = source
            .transact_cypher_returning(state, statement, Some(&params))
            .await
            .unwrap();
        state = normal.ledger;
        assert_eq!(
            result.result.as_ref().map(|v| rows(v).len()),
            normal_return.as_ref().map(|v| rows(v).len()),
            "{name}"
        );
        for query in OBSERVE.iter().copied().chain([annotation_query]) {
            let view = GraphDb::from_ledger_state(&state);
            let expected = source
                .query_cypher(&view, query)
                .await
                .unwrap()
                .to_cypher_json_async(view.as_graph_db_ref())
                .await
                .unwrap();
            assert_eq!(
                rows(&ledger.query_cypher(query, None).await.unwrap()),
                rows(&expected),
                "{name}: {query}"
            );
        }
        acks.push(json!({"id":ack.commit.commit_id.to_string(),"raw_id":ack.raw_txn_id.to_string(),
            "commit":ledger.content(&ack.commit.commit_id).await.unwrap(),"raw":ledger.content(&ack.raw_txn_id).await.unwrap(),
            "text":statement,"params":params,"result":result.result}));
        let mut file = std::fs::File::create(controller.path().join("acks.json")).unwrap();
        file.write_all(&serde_json::to_vec(&acks).unwrap()).unwrap();
        file.sync_all().unwrap();
        std::fs::File::open(controller.path())
            .unwrap()
            .sync_all()
            .unwrap();
    }
    let before = ledger
        .query_cypher("MATCH (n) RETURN n ORDER BY n", None)
        .await
        .unwrap();
    let endpoints = ledger
        .query_cypher("MATCH (a)-[:TempEdge]->(b) RETURN a,b", None)
        .await
        .unwrap();
    for pair in rows(&endpoints) {
        for endpoint in pair.as_array().unwrap() {
            assert!(
                rows(&before).contains(&json!([endpoint])),
                "missing endpoint {endpoint}"
            );
        }
    }
    let view = GraphDb::from_ledger_state(&state);
    let ordinary_nodes = source
        .query_cypher(&view, "MATCH (n) RETURN n ORDER BY n")
        .await
        .unwrap()
        .to_cypher_json_async(view.as_graph_db_ref())
        .await
        .unwrap();
    let ordinary_endpoints = source
        .query_cypher(&view, "MATCH (a)-[:TempEdge]->(b) RETURN a,b")
        .await
        .unwrap()
        .to_cypher_json_async(view.as_graph_db_ref())
        .await
        .unwrap();
    for pair in rows(&ordinary_endpoints) {
        for endpoint in pair.as_array().unwrap() {
            assert!(rows(&ordinary_nodes).contains(&json!([endpoint])));
        }
    }
    drop(view);
    let before_edges = ledger
        .query_cypher("MATCH ()-[r]->() RETURN r ORDER BY r", None)
        .await
        .unwrap();
    assert_eq!(
        rows(
            &ledger
                .query_cypher("MATCH (n:L1) RETURN count(n)", None)
                .await
                .unwrap()
        ),
        vec![json!([101])]
    );
    {
        let cache = ledger.ready().await.unwrap();
        assert_eq!(
            cache
                .state
                .as_ref()
                .unwrap()
                .ns_record
                .as_ref()
                .unwrap()
                .index_head_id
                .as_ref(),
            Some(root_id)
        );
        assert_eq!(cache.state.as_ref().unwrap().snapshot.t, imported.t);
    }
    drop((ledger, state, source));
    // Ordinary file recovery must preserve the same complete node identities.
    let reopened = FlureeBuilder::file(source_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let view = reopened.db("indexed-cypher:main").await.unwrap();
    let recovered_nodes = reopened
        .query_cypher(&view, "MATCH (n) RETURN n ORDER BY n")
        .await
        .unwrap()
        .to_cypher_json_async(view.as_graph_db_ref())
        .await
        .unwrap();
    assert_eq!(recovered_nodes, ordinary_nodes);
    drop((view, reopened));
    source_dir.close().unwrap();
    let acks: Vec<Value> =
        serde_json::from_slice(&std::fs::read(controller.path().join("acks.json")).unwrap())
            .unwrap();
    for _ in 0..2 {
        std::fs::remove_dir_all(target_dir.path().join(".fluree-wal/data")).unwrap();
        std::fs::create_dir(target_dir.path().join(".fluree-wal/data")).unwrap();
        let ledger = JournalLedger::open(target_dir.path().into()).await.unwrap();
        assert_eq!(
            ledger
                .query_cypher("MATCH (n) RETURN n ORDER BY n", None)
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            ledger
                .query_cypher("MATCH ()-[r]->() RETURN r ORDER BY r", None)
                .await
                .unwrap(),
            before_edges
        );
        assert_eq!(
            rows(&ledger.query_cypher(annotation_query, None).await.unwrap()),
            vec![json!([7])]
        );
        let mut parent = imported.commit_head_id.clone();
        for ack in &acks {
            let id: ContentId = ack["id"].as_str().unwrap().parse().unwrap();
            let bytes = ledger.content(&id).await.unwrap();
            assert_eq!(
                bytes,
                serde_json::from_value::<Vec<u8>>(ack["commit"].clone()).unwrap()
            );
            let commit = fluree_db_core::commit::codec::read_commit(&bytes).unwrap();
            assert_eq!(commit.parents, vec![parent]);
            parent = id;
            let raw_id: ContentId = ack["raw_id"].as_str().unwrap().parse().unwrap();
            let raw = ledger.content(&raw_id).await.unwrap();
            assert_eq!(
                raw,
                serde_json::from_value::<Vec<u8>>(ack["raw"].clone()).unwrap()
            );
            assert_eq!(
                serde_json::from_slice::<Value>(&raw).unwrap(),
                json!({"cypher":ack["text"],"params":ack["params"]})
            );
        }
    } // Actual expanded annotation leaves and historical roots must remain
      // prerequisites even when a current-index query could skip their reads.
    for id in [annotation_leaf, historical_root] {
        let path = target_dir
            .path()
            .join(".fluree-wal/checkpoint/objects")
            .join(content_path(
                id.content_kind().unwrap(),
                "indexed-cypher:main",
                &id.digest_hex(),
            ));
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        for _ in 0..2 {
            assert!(JournalLedger::open(target_dir.path().into()).await.is_err());
        }
        std::fs::write(&path, bytes).unwrap();
        std::fs::File::open(&path).unwrap().sync_all().unwrap();
        std::fs::File::open(path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
        let recovered = JournalLedger::open(target_dir.path().into()).await.unwrap();
        assert_eq!(
            rows(
                &recovered
                    .query_cypher(annotation_query, None)
                    .await
                    .unwrap()
            ),
            vec![json!([7])]
        );
    }
}

async fn source_fixture() -> (tempfile::TempDir, Fluree, crate::import::ImportResult) {
    let dir = tempfile::tempdir().unwrap();
    let input_dir = tempfile::tempdir().unwrap();
    let input = input_dir.path().join("seed.ttl");
    std::fs::write(
        &input,
        "@prefix ex: <http://example.org/> . ex:one ex:value 1 .",
    )
    .unwrap();
    let source = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let result = source
        .create("fault:main")
        .import(&input)
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .unwrap();
    (dir, source, result)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_source_after_copy_never_publishes_a_ready_root() {
    let (source_dir, _source, _) = source_fixture().await;
    let target = tempfile::tempdir().unwrap();
    let main = source_dir.path().join("ns@v2/fault/main.json");
    let result = JournalLedger::bootstrap_with_copy_hook(
        target.path().into(),
        source_dir.path().into(),
        "fault:main".into(),
        "test-1".into(),
        move || {
            let mut value: Value = serde_json::from_slice(&std::fs::read(&main)?).unwrap();
            value["f:t"] = json!(2); // injected changed-source image, not a valid new commit
            std::fs::write(main, serde_json::to_vec(&value).unwrap())?;
            Ok(())
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::Journal(JournalError::Invalid(
            "source heads changed during bootstrap"
        )))
    ));
    assert!(target.path().join(".fluree-wal").exists());
    assert!(!target.path().join(".fluree-wal/format.json").exists());
    for _ in 0..2 {
        assert!(JournalLedger::open(target.path().into()).await.is_err());
    }
    assert!(FileStorage::new(target.path())
        .read_bytes("fluree:file://anything")
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_or_corrupt_actual_index_dependencies_reject_bootstrap_before_fence() {
    let (source_dir, source, imported) = source_fixture().await;
    let store = source.content_store("fault:main");
    let artifacts = crate::pack::compute_missing_index_artifacts(
        &store,
        imported.root_id.as_ref().unwrap(),
        None,
    )
    .await
    .unwrap();
    let ids = [
        artifacts
            .iter()
            .find(|id| matches!(id.content_kind(), Some(ContentKind::DictBlob { .. })))
            .unwrap(),
        artifacts
            .iter()
            .find(|id| id.content_kind() == Some(ContentKind::IndexLeaf))
            .unwrap(),
    ];
    for id in ids {
        let path = source_dir.path().join(content_path(
            id.content_kind().unwrap(),
            "fault:main",
            &id.digest_hex(),
        ));
        let original = std::fs::read(&path).unwrap();
        for missing in [true, false] {
            if missing {
                std::fs::remove_file(&path).unwrap();
            } else {
                let mut damaged = original.clone();
                damaged[0] ^= 1;
                std::fs::write(&path, damaged).unwrap();
            }
            let target = tempfile::tempdir().unwrap();
            assert!(JournalLedger::bootstrap(
                target.path().into(),
                source_dir.path().into(),
                "fault:main".into(),
                "test-1".into()
            )
            .await
            .is_err());
            assert!(!target.path().join(".fluree-wal").exists());
            std::fs::write(&path, &original).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_branched_or_mutable_context_sources_are_rejected() {
    let (source_dir, source, _) = source_fixture().await;
    let main = source_dir.path().join("ns@v2/fault/main.json");
    let original = std::fs::read(&main).unwrap();
    for (key, value) in [
        ("f:configV", json!(2)),
        ("f:branches", json!(1)),
        ("f:configMeta", json!({"policy":"required"})),
        ("f:unknown", json!(true)),
    ] {
        let mut head: Value = serde_json::from_slice(&original).unwrap();
        head[key] = value;
        std::fs::write(&main, serde_json::to_vec(&head).unwrap()).unwrap();
        let target = tempfile::tempdir().unwrap();
        assert!(JournalLedger::bootstrap(
            target.path().into(),
            source_dir.path().into(),
            "fault:main".into(),
            "test-1".into()
        )
        .await
        .is_err());
        assert!(!target.path().join(".fluree-wal").exists());
    }
    std::fs::write(&main, &original).unwrap();
    // Correctly content-addressed but unsupported context, rather than corruption.
    let ctx = source
        .content_store("fault:main")
        .put(
            ContentKind::LedgerConfig,
            br#"{"ex":{"@id":"http://example.org/","@context":"https://remote.example/context"}}"#,
        )
        .await
        .unwrap();
    let mut head: Value = serde_json::from_slice(&original).unwrap();
    head["f:defaultContextCid"] = json!(ctx.to_string());
    std::fs::write(main, serde_json::to_vec(&head).unwrap()).unwrap();
    let target = tempfile::tempdir().unwrap();
    assert!(matches!(
        JournalLedger::bootstrap(
            target.path().into(),
            source_dir.path().into(),
            "fault:main".into(),
            "test-1".into()
        )
        .await,
        Err(Error::Journal(JournalError::Invalid(
            "only static imported IRI mappings are supported"
        )))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_install_failure_blocks_cached_reads_and_recovers_new_namespace() {
    let (source_dir, _source, _) = source_fixture().await;
    let target = tempfile::tempdir().unwrap();
    let ledger = JournalLedger::bootstrap(
        target.path().into(),
        source_dir.path().into(),
        "fault:main".into(),
        "test-1".into(),
    )
    .await
    .unwrap();
    let body = json!({"@context":{"new":"http://new.example/"},"@id":"new:one","new:label":"after recovery"});
    let result = ledger
        .transact_with_install_hook(TxnType::Insert, &body, || {
            Err(JournalError::Invalid(
                "injected indexed installation failure",
            ))
        })
        .await;
    assert!(matches!(
        result,
        Err(Error::Journal(JournalError::AcceptanceUnresolved {
            durable: Some(_),
            ..
        }))
    ));
    let query = json!({"@context":{"new":"http://new.example/"},"select":["?id"],"where":{"@id":"?id","new:label":"after recovery"}});
    assert!(matches!(
        ledger.query(&query).await,
        Err(Error::Journal(JournalError::Poisoned))
    ));
    assert!(matches!(
        ledger.head().await,
        Err(Error::Journal(JournalError::Poisoned))
    ));
    ledger.recover().await.unwrap();
    assert_eq!(ledger.query(&query).await.unwrap(), json!([["new:one"]]));
    let head = ledger.head().await.unwrap().unwrap();
    assert_eq!(head.t, 2);
    let commit = fluree_db_core::commit::codec::read_commit(
        &ledger.content(head.id.as_ref().unwrap()).await.unwrap(),
    )
    .unwrap();
    assert_eq!(
        ledger.content(commit.txn.as_ref().unwrap()).await.unwrap(),
        serde_json::to_vec(&body).unwrap()
    );
}
