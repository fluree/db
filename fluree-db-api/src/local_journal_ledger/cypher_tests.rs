use super::*;
use fluree_db_core::commit::codec::read_commit;

async fn initialized() -> (tempfile::TempDir, JournalLedger) {
    let dir = tempfile::tempdir().unwrap();
    let ledger = JournalLedger::initialize(
        dir.path().into(),
        "cypher-wal:main".into(),
        "cypher-test".into(),
    )
    .await
    .unwrap();
    (dir, ledger)
}
fn params(value: Value) -> ParamMap {
    value.as_object().unwrap().clone()
}
fn rows(value: &Value) -> Vec<Value> {
    value["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["row"].clone())
        .collect()
}
async fn count(ledger: &JournalLedger, query: &str) -> i64 {
    rows(&ledger.query_cypher(query, None).await.unwrap())[0][0]
        .as_i64()
        .unwrap()
}

use super::super::fixtures::{OBSERVE, WRITES};

#[tokio::test]
async fn frozen_eight_writes_match_ordinary_semantics_and_external_recovery_oracle() {
    use std::io::Write;
    // Full scans are an existing process-wide opt-in, also used by the rig.
    // Re-exec only this test with that environment; never change the flag in the
    // shared library test process (which has unrelated default-rejection tests).
    if std::env::var_os("FLUREE_WAL_CYPHER_CORPUS_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "local_journal_ledger::cypher::tests::frozen_eight_writes_match_ordinary_semantics_and_external_recovery_oracle", "--nocapture"])
            .env("FLUREE_WAL_CYPHER_CORPUS_CHILD", "1")
            .env("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1")
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let origin: Value = serde_json::from_str(include_str!("fixtures/cypher/ORIGIN.json")).unwrap();
    for (name, statement) in WRITES {
        use sha2::Digest;
        let digest = format!("{:x}", sha2::Sha256::digest(statement.as_bytes()));
        assert_eq!(
            digest,
            origin["sha256"][format!("{name}.cypher")].as_str().unwrap()
        );
    }
    let (dir, ledger) = initialized().await;
    let controller = tempfile::tempdir().unwrap();
    let ordinary = FlureeBuilder::memory().without_indexing().build_memory();
    let mut state = ordinary.create_ledger("cypher-wal:main").await.unwrap();
    let seed = "CREATE (:User {id:1, age:30}), (:User {id:2, age:20})";
    ledger.transact_cypher(seed, None).await.unwrap();
    state = ordinary.transact_cypher(state, seed).await.unwrap().ledger;
    let mut acknowledgments = Vec::new();
    for (i, (name, statement)) in WRITES.iter().enumerate() {
        let p = params(
            json!({"from":1,"to":2,"id":if *name == "arango__single_vertex_write" {10} else {1}}),
        );
        let outcome = ledger
            .transact_cypher(statement, Some(&p))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let ack = outcome.commit.expect("every suite case must really mutate");
        assert_eq!(ack.commit.t, i as i64 + 2, "{name}");
        let (normal, normal_return) = ordinary
            .transact_cypher_returning(state, statement, Some(&p))
            .await
            .unwrap();
        state = normal.ledger;
        assert_eq!(
            outcome.result.as_ref().map(|v| rows(v).len()),
            normal_return.as_ref().map(|v| rows(v).len()),
            "{name}"
        );
        if let Some(result) = &outcome.result {
            assert_eq!(rows(result).len(), 1, "{name}");
        }
        for query in OBSERVE {
            let actual = ledger.query_cypher(query, None).await.unwrap();
            let view = GraphDb::from_ledger_state(&state);
            let expected = ordinary
                .query_cypher(&view, query)
                .await
                .unwrap()
                .to_cypher_json_async(view.as_graph_db_ref())
                .await
                .unwrap();
            assert_eq!(rows(&actual), rows(&expected), "after {name}: {query}");
        }
        let bytes = ledger.content(&ack.commit.commit_id).await.unwrap();
        let commit = read_commit(&bytes).unwrap();
        assert_eq!(commit.txn.as_ref(), Some(&ack.raw_txn_id));
        let raw = ledger.content(&ack.raw_txn_id).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&raw).unwrap(),
            json!({"cypher":statement,"params":p})
        );
        acknowledgments.push(
            json!({"t":ack.commit.t,"id":ack.commit.commit_id.to_string(),"bytes":bytes,
            "raw_id":ack.raw_txn_id.to_string(),"raw":raw,"return":outcome.result}),
        );
        // Controller acknowledgments live outside the recoverable database image.
        let mut file = std::fs::File::create(controller.path().join("acks.json")).unwrap();
        file.write_all(&serde_json::to_vec(&acknowledgments).unwrap())
            .unwrap();
        file.sync_all().unwrap();
        std::fs::File::open(controller.path())
            .unwrap()
            .sync_all()
            .unwrap();
    }
    // Use labeled counts for independent expectations: bare MATCH enumerates
    // subjects and the existing engine omits the object-only unlabeled sink of
    // create__pattern. The differential OBSERVE check above preserves that
    // existing behavior rather than changing Cypher semantics in the WAL patch.
    assert_eq!(count(&ledger, "MATCH (n:L1) RETURN count(n)").await, 101);
    assert_eq!(
        count(&ledger, "MATCH (n:UserTemp) RETURN count(n)").await,
        1
    );
    assert_eq!(count(&ledger, "MATCH ()-[r]->() RETURN count(r)").await, 3);
    let node_ids = ledger
        .query_cypher("MATCH (n) RETURN n ORDER BY n", None)
        .await
        .unwrap();
    let edges = ledger
        .query_cypher("MATCH (a)-[r]->(b) RETURN a, r, b ORDER BY r", None)
        .await
        .unwrap();
    drop(ledger);
    std::fs::remove_dir_all(dir.path().join(".fluree-wal/data")).unwrap();
    std::fs::create_dir(dir.path().join(".fluree-wal/data")).unwrap();
    let external: Vec<Value> =
        serde_json::from_slice(&std::fs::read(controller.path().join("acks.json")).unwrap())
            .unwrap();
    for _ in 0..2 {
        let ledger = JournalLedger::open(dir.path().into()).await.unwrap();
        let head = ledger.head().await.unwrap().unwrap();
        assert_eq!(head.t, 9);
        assert_eq!(
            head.id.unwrap().to_string(),
            external.last().unwrap()["id"].as_str().unwrap()
        );
        for (i, ack) in external.iter().enumerate() {
            let id: ContentId = ack["id"].as_str().unwrap().parse().unwrap();
            let raw_id: ContentId = ack["raw_id"].as_str().unwrap().parse().unwrap();
            let bytes = ledger.content(&id).await.unwrap();
            assert_eq!(json!(bytes), ack["bytes"]);
            let commit = read_commit(&bytes).unwrap();
            assert_eq!(commit.t, ack["t"].as_i64().unwrap());
            if i > 0 {
                assert_eq!(
                    commit.parents[0].to_string(),
                    external[i - 1]["id"].as_str().unwrap()
                );
            }
            assert_eq!(json!(ledger.content(&raw_id).await.unwrap()), ack["raw"]);
        }
        assert_eq!(
            rows(
                &ledger
                    .query_cypher("MATCH (n) RETURN n ORDER BY n", None)
                    .await
                    .unwrap()
            ),
            rows(&node_ids)
        );
        assert_eq!(
            rows(
                &ledger
                    .query_cypher("MATCH (a)-[r]->(b) RETURN a, r, b ORDER BY r", None)
                    .await
                    .unwrap()
            ),
            rows(&edges)
        );
        // The returned generated identities must be the recovered entities.
        let created = rows(
            &ledger
                .query_cypher("MATCH (n:UserTemp) RETURN n", None)
                .await
                .unwrap(),
        );
        assert_eq!(created, rows(&external[1]["return"]));
        let created = rows(
            &ledger
                .query_cypher("MATCH ()-[r:Temp]->() RETURN r", None)
                .await
                .unwrap(),
        );
        assert_eq!(created, rows(&external[0]["return"]));
    }
}

#[tokio::test]
async fn conditional_and_sequential_merge_use_current_state_and_one_commit() {
    let (_dir, ledger) = initialized().await;
    let conditional =
        "MERGE (n:Person {name:$name}) ON CREATE SET n.visits = 1 ON MATCH SET n.visits = 2";
    let p = params(json!({"name":"Alice"}));
    ledger.transact_cypher(conditional, Some(&p)).await.unwrap();
    ledger.transact_cypher(conditional, Some(&p)).await.unwrap();
    assert_eq!(
        rows(
            &ledger
                .query_cypher("MATCH (n:Person) RETURN n.name, n.visits", None)
                .await
                .unwrap()
        ),
        vec![json!(["Alice", 2])]
    );
    let statement = "MERGE (a:Person {name:'Bob'}) MERGE (b:City {name:'Paris'}) MERGE (a)-[:LIVES_IN]->(b) RETURN a.name, b.name";
    let outcome = ledger.transact_cypher(statement, None).await.unwrap();
    assert_eq!(outcome.commit.unwrap().commit.t, 3);
    assert_eq!(
        rows(&outcome.result.unwrap()),
        vec![json!(["Bob", "Paris"])]
    );
    let outcome = ledger.transact_cypher(statement, None).await.unwrap();
    assert!(outcome.commit.is_none());
    assert_eq!(
        rows(&outcome.result.unwrap()),
        vec![json!(["Bob", "Paris"])]
    );
    assert_eq!(ledger.head().await.unwrap().unwrap().t, 3);
    assert_eq!(
        count(&ledger, "MATCH ()-[r:LIVES_IN]->() RETURN count(r)").await,
        1
    );
}

#[tokio::test]
async fn scripts_parse_failures_and_late_staging_errors_leave_no_effects() {
    let (dir, ledger) = initialized().await;
    let before = std::fs::read(dir.path().join(".fluree-wal/journal")).unwrap();
    for statement in [
        "CREATE (:Bad); CREATE (:AlsoBad)",
        "CREATE (",
        "CREATE (:Bad {x:$missing})",
        "RETURN 1",
        "MERGE (a:Bad {name:'would-create'}) MERGE (b:Bad {name:null})",
    ] {
        assert!(
            ledger.transact_cypher(statement, None).await.is_err(),
            "{statement}"
        );
        assert_eq!(
            std::fs::read(dir.path().join(".fluree-wal/journal")).unwrap(),
            before,
            "{statement}"
        );
        assert!(ledger.head().await.unwrap().is_none());
    }
    ledger
        .transact_cypher(
            "CREATE (:Good {text:'semi;colon'}); // trailing comment",
            None,
        )
        .await
        .unwrap();
    assert_eq!(count(&ledger, "MATCH (n:Bad) RETURN count(n)").await, 0);
    assert_eq!(count(&ledger, "MATCH (n:Good) RETURN count(n)").await, 1);
}

#[tokio::test]
async fn failed_install_never_exposes_prepared_return_and_recovery_keeps_generated_id() {
    let (_dir, ledger) = initialized().await;
    let statement = "CREATE (n:LostReply {value:42}) RETURN n";
    let error = ledger
        .transact_cypher_with_install_hook(statement, None, || {
            Err(JournalError::Invalid("injected install failure"))
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Journal(JournalError::AcceptanceUnresolved {
            durable: Some(_),
            ..
        })
    ));
    assert!(ledger
        .query_cypher("MATCH (n) RETURN n", None)
        .await
        .is_err());
    ledger.recover().await.unwrap();
    assert_eq!(
        count(&ledger, "MATCH (n:LostReply) RETURN count(n)").await,
        1
    );
    let head = ledger.head().await.unwrap().unwrap();
    let commit = read_commit(&ledger.content(&head.id.unwrap()).await.unwrap()).unwrap();
    let raw = ledger.content(&commit.txn.unwrap()).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&raw).unwrap(),
        json!({"cypher":statement,"params":{}})
    );
}

#[tokio::test]
async fn concurrent_merge_probes_are_serialized_and_relationship_followup_is_atomic() {
    let (_dir, ledger) = initialized().await;
    let mut jobs = Vec::new();
    for _ in 0..8 {
        let clone = ledger.clone();
        jobs.push(tokio::spawn(async move {
            clone
                .transact_cypher("MERGE (:Once {id:1})", None)
                .await
                .unwrap()
                .commit
                .is_some()
        }));
    }
    let mut accepted = 0;
    for job in jobs {
        accepted += usize::from(job.await.unwrap());
    }
    assert_eq!(accepted, 1);
    assert_eq!(ledger.head().await.unwrap().unwrap().t, 1);
    ledger
        .transact_cypher(
            "CREATE (:User {id:1}), (:User {id:2}), (:User {id:3})",
            None,
        )
        .await
        .unwrap();
    ledger
        .transact_cypher(
            "MATCH (a:User {id:1}), (b:User {id:2}) CREATE (a)-[:KNOWS]->(b)",
            None,
        )
        .await
        .unwrap();
    let mixed = "MATCH (a:User {id:1}), (b:User) WHERE b.id <> a.id MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.created = true ON MATCH SET r.seen = true";
    let outcome = ledger.transact_cypher(mixed, None).await.unwrap();
    assert_eq!(outcome.commit.unwrap().commit.t, 4);
    assert_eq!(rows(&ledger.query_cypher("MATCH (:User {id:1})-[r:KNOWS]->(b:User) RETURN b.id, r.created, r.seen ORDER BY b.id", None).await.unwrap()),
        vec![json!([2,null,true]), json!([3,true,null])]);
}

#[tokio::test]
async fn returning_multiple_created_nodes_and_zero_match_return_are_consistent() {
    let (dir, ledger) = initialized().await;
    ledger
        .transact_cypher(
            "CREATE (:User {id:1}), (:User {id:2}), (:User {id:3})",
            None,
        )
        .await
        .unwrap();
    let statement = "MATCH (u:User) CREATE (n:Returned) RETURN n";
    let outcome = ledger.transact_cypher(statement, None).await.unwrap();
    let mut returned = rows(&outcome.result.unwrap());
    returned.sort_by_key(Value::to_string);
    assert_eq!(returned.len(), 3);
    let before = ledger.head().await.unwrap().unwrap();
    let outcome = ledger
        .transact_cypher(WRITES[0].1, Some(&params(json!({"from":999,"to":998}))))
        .await
        .unwrap();
    assert!(outcome.commit.is_none());
    assert!(rows(&outcome.result.unwrap()).is_empty());
    assert_eq!(ledger.head().await.unwrap().unwrap(), before);
    drop(ledger);
    let ledger = JournalLedger::open(dir.path().into()).await.unwrap();
    let mut actual = rows(
        &ledger
            .query_cypher("MATCH (n:Returned) RETURN n", None)
            .await
            .unwrap(),
    );
    actual.sort_by_key(Value::to_string);
    assert_eq!(actual, returned);
}
