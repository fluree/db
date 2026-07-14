//! End-to-end Bolt coverage: a raw TCP client (built on the fluree-db-bolt
//! codec) drives the real listener — handshake, HELLO/LOGON, autocommit RUN
//! for reads and writes, params, reactive PULL batching, error surfaces,
//! and RESET recovery. Mirrors `cypher_http_integration.rs` on the Bolt
//! transport. Official-driver compatibility is covered separately by the
//! Python smoke script (`fluree-db-server/tests/bolt_driver_smoke.py`).
#![cfg(feature = "bolt")]

use axum::body::Body;
use fluree_db_bolt::chunk::{write_message, ChunkAssembler};
use fluree_db_bolt::handshake::MAGIC;
use fluree_db_bolt::message as msg;
use fluree_db_bolt::packstream;
use fluree_db_bolt::value::{MapValue, Structure, Value};
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tower::ServiceExt;

async fn server_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"));
    (tmp, state)
}

async fn create_ledger(state: &Arc<AppState>, ledger: &str) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "ledger": ledger }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create {ledger}");
}

async fn insert(state: &Arc<AppState>, ledger: &str, body: serde_json::Value) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.status().is_success(), "insert into {ledger}");
}

/// Spin up state + data + the Bolt listener; returns the listener address.
async fn bolt_server(ledger: &str) -> (TempDir, Arc<AppState>, std::net::SocketAddr) {
    let (tmp, state) = server_state().await;
    create_ledger(&state, ledger).await;
    insert(
        &state,
        ledger,
        json!({
            "@context": {"xsd": "http://www.w3.org/2001/XMLSchema#"},
            "@graph": [
                {"@id": "alice", "@type": "Person", "name": "Alice", "age": 30,
                 "birthday": {"@value": "1990-11-23", "@type": "xsd:date"},
                 "knows": {"@id": "bob"}},
                {"@id": "bob", "@type": "Person", "name": "Bob", "age": 45}
            ]
        }),
    )
    .await;
    let (addr, _task) =
        fluree_db_server::bolt::spawn_listener(Arc::clone(&state), "127.0.0.1:0".parse().unwrap())
            .await
            .expect("bolt listener");
    (tmp, state, addr)
}

/// Minimal Bolt client over the same codec crate the server uses.
struct BoltClient {
    socket: TcpStream,
    assembler: ChunkAssembler,
    read_buf: Vec<u8>,
}

/// A decoded server response: signature + metadata/fields.
#[derive(Debug)]
struct Reply {
    signature: u8,
    field: Value,
}

impl Reply {
    fn metadata(&self) -> &MapValue {
        match &self.field {
            Value::Map(m) => m,
            other => panic!("expected metadata map, got {other:?}"),
        }
    }

    fn record(&self) -> &[Value] {
        match &self.field {
            Value::List(l) => l,
            other => panic!("expected record list, got {other:?}"),
        }
    }

    fn assert_success(&self) -> &MapValue {
        assert_eq!(
            self.signature,
            msg::SUCCESS,
            "expected SUCCESS, got 0x{:02X} {:?}",
            self.signature,
            self.field
        );
        self.metadata()
    }

    fn assert_failure(&self) -> &MapValue {
        assert_eq!(
            self.signature,
            msg::FAILURE,
            "expected FAILURE, got 0x{:02X} {:?}",
            self.signature,
            self.field
        );
        self.metadata()
    }
}

impl BoltClient {
    /// Connect and handshake, proposing exactly `proposal` in slot 0.
    async fn connect(addr: std::net::SocketAddr, proposal: [u8; 4]) -> (Self, [u8; 4]) {
        let mut socket = TcpStream::connect(addr).await.expect("connect");
        let mut handshake = Vec::with_capacity(20);
        handshake.extend_from_slice(&MAGIC);
        handshake.extend_from_slice(&proposal);
        handshake.extend_from_slice(&[0; 12]);
        socket.write_all(&handshake).await.expect("handshake write");
        let mut chosen = [0u8; 4];
        socket
            .read_exact(&mut chosen)
            .await
            .expect("handshake read");
        (
            Self {
                socket,
                assembler: ChunkAssembler::new(),
                read_buf: vec![0; 8192],
            },
            chosen,
        )
    }

    async fn send(&mut self, signature: u8, fields: Vec<Value>) {
        let payload = packstream::encode_to_vec(&Value::Structure(Structure { signature, fields }));
        let mut wire = Vec::new();
        write_message(&payload, &mut wire);
        self.socket.write_all(&wire).await.expect("send");
    }

    async fn recv(&mut self) -> Reply {
        loop {
            if let Some(payload) = self.assembler.next_message() {
                let value = packstream::decode_exact(&payload).expect("decode response");
                let Value::Structure(Structure {
                    signature,
                    mut fields,
                }) = value
                else {
                    panic!("response is not a structure");
                };
                let field = if fields.is_empty() {
                    Value::Null
                } else {
                    fields.remove(0)
                };
                return Reply { signature, field };
            }
            let n = self.socket.read(&mut self.read_buf).await.expect("recv");
            assert!(n > 0, "connection closed while awaiting response");
            self.assembler.push(&self.read_buf[..n]).expect("assemble");
        }
    }

    /// HELLO (+ LOGON for 5.x) with no auth; returns HELLO metadata.
    async fn ready(&mut self, logon: bool) -> MapValue {
        self.send(msg::HELLO, vec![Value::empty_map()]).await;
        let hello = self.recv().await;
        let meta = hello.assert_success().clone();
        if logon {
            self.send(msg::LOGON, vec![Value::empty_map()]).await;
            self.recv().await.assert_success();
        }
        meta
    }

    async fn run(&mut self, query: &str, params: MapValue, db: &str) -> Reply {
        let mut extra = MapValue::new();
        extra.insert("db", db);
        self.send(
            msg::RUN,
            vec![Value::from(query), Value::Map(params), Value::Map(extra)],
        )
        .await;
        self.recv().await
    }

    async fn begin(&mut self, db: &str) -> Reply {
        let mut extra = MapValue::new();
        extra.insert("db", db);
        self.send(msg::BEGIN, vec![Value::Map(extra)]).await;
        self.recv().await
    }

    async fn commit(&mut self) -> Reply {
        self.send(msg::COMMIT, vec![]).await;
        self.recv().await
    }

    async fn rollback(&mut self) -> Reply {
        self.send(msg::ROLLBACK, vec![]).await;
        self.recv().await
    }

    /// Autocommit count of :Person nodes.
    async fn person_count(&mut self) -> i64 {
        self.run(
            "MATCH (n:Person) RETURN count(n) AS c",
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
        let (records, _) = self.pull(-1).await;
        match records[0][0] {
            Value::Integer(i) => i,
            ref other => panic!("expected integer count, got {other:?}"),
        }
    }

    /// PULL n, collecting records until the trailing SUCCESS/FAILURE.
    async fn pull(&mut self, n: i64) -> (Vec<Vec<Value>>, Reply) {
        let mut extra = MapValue::new();
        extra.insert("n", n);
        self.send(msg::PULL, vec![Value::Map(extra)]).await;
        let mut records = Vec::new();
        loop {
            let reply = self.recv().await;
            if reply.signature == msg::RECORD {
                records.push(reply.record().to_vec());
            } else {
                return (records, reply);
            }
        }
    }
}

const LEDGER: &str = "boltint";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt5_read_round_trip_with_params() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, chosen) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    assert_eq!(chosen, [0, 0, 4, 5], "server picks Bolt 5.4");

    let hello_meta = client.ready(true).await;
    let server = hello_meta.get_str("server").expect("server agent");
    assert!(
        server.starts_with("Neo4j/"),
        "driver-parsable agent: {server}"
    );

    // Plain read.
    let run = client
        .run(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
            MapValue::new(),
            LEDGER,
        )
        .await;
    let meta = run.assert_success();
    assert_eq!(
        meta.get("fields"),
        Some(&Value::List(vec![Value::from("name")]))
    );
    let (records, summary) = client.pull(-1).await;
    assert_eq!(
        records,
        vec![vec![Value::from("Alice")], vec![Value::from("Bob")]]
    );
    let summary = summary.assert_success();
    assert_eq!(summary.get_str("type"), Some("r"));
    assert_eq!(summary.get("has_more"), Some(&Value::Boolean(false)));

    // Parameterized read.
    let mut params = MapValue::new();
    params.insert("min_age", 40i64);
    let run = client
        .run(
            "MATCH (n:Person) WHERE n.age > $min_age RETURN n.name AS name, n.age AS age",
            params,
            LEDGER,
        )
        .await;
    run.assert_success();
    let (records, _summary) = client.pull(-1).await;
    assert_eq!(records, vec![vec![Value::from("Bob"), Value::Integer(45)]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt44_session_pull_batching() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, chosen) = BoltClient::connect(addr, [0, 0, 4, 4]).await;
    assert_eq!(chosen, [0, 0, 4, 4], "server accepts Bolt 4.4");

    // 4.4: HELLO alone reaches READY (no LOGON).
    client.ready(false).await;

    client
        .run(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();

    // First batch of 1: record + has_more=true.
    let (records, reply) = client.pull(1).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        reply.assert_success().get("has_more"),
        Some(&Value::Boolean(true))
    );

    // Remainder: final summary.
    let (records, reply) = client.pull(-1).await;
    assert_eq!(records.len(), 1);
    assert_eq!(reply.assert_success().get_str("type"), Some("r"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_write_and_read_back() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    // Autocommit write.
    let run = client
        .run(
            r#"CREATE (n:Person {name: "Carol", age: 27})"#,
            MapValue::new(),
            LEDGER,
        )
        .await;
    run.assert_success();
    let (records, summary) = client.pull(-1).await;
    assert!(records.is_empty(), "bare CREATE returns no rows");
    let summary = summary.assert_success();
    assert_eq!(summary.get_str("type"), Some("w"));
    let Some(Value::Map(stats)) = summary.get("stats") else {
        panic!("write summary must carry stats")
    };
    assert_eq!(stats.get("contains-updates"), Some(&Value::Boolean(true)));

    // The write is visible on the same session.
    client
        .run(
            "MATCH (n:Person) RETURN count(n) AS c",
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    assert_eq!(records, vec![vec![Value::Integer(3)]]);

    // Write with RETURN surfaces the created entity as a typed Node.
    let run = client
        .run(
            r#"CREATE (n:Person {name: "Dave"}) RETURN n"#,
            MapValue::new(),
            LEDGER,
        )
        .await;
    let meta = run.assert_success();
    assert_eq!(
        meta.get("fields"),
        Some(&Value::List(vec![Value::from("n")]))
    );
    let (records, summary) = client.pull(-1).await;
    assert_eq!(records.len(), 1, "one created entity row");
    let Value::Structure(node) = &records[0][0] else {
        panic!(
            "write RETURN n must be a Node structure, got {:?}",
            records[0][0]
        )
    };
    assert_eq!(node.signature, SIG_NODE);
    assert_eq!(
        node.fields[1],
        Value::List(vec![Value::String("Person".into())]),
        "created node carries its labels"
    );
    let Value::Map(props) = &node.fields[2] else {
        panic!()
    };
    assert_eq!(props.get_str("name"), Some("Dave"));
    summary.assert_success();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_error_surfaces_and_reset_recovery() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    // Syntax error → FAILURE with a driver-shaped code.
    let run = client
        .run("MATCH (n RETURN n", MapValue::new(), LEDGER)
        .await;
    let failure = run.assert_failure();
    let code = failure.get_str("code").expect("failure code");
    assert!(code.starts_with("Neo.ClientError."), "code: {code}");

    // Everything is IGNORED until RESET.
    let (records, reply) = client.pull(-1).await;
    assert!(records.is_empty());
    assert_eq!(reply.signature, msg::IGNORED);

    client.send(msg::RESET, vec![]).await;
    client.recv().await.assert_success();

    // Session works again.
    client
        .run(
            "MATCH (n:Person) RETURN count(n) AS c",
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    assert_eq!(records, vec![vec![Value::Integer(2)]]);

    // Unknown database → DatabaseNotFound-family failure.
    let run = client
        .run("MATCH (n) RETURN n", MapValue::new(), "nosuch:db")
        .await;
    let failure = run.assert_failure();
    assert!(failure.get_str("code").unwrap().contains("Database"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_begin_without_database_fails_clearly() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    // No `db` in BEGIN extra, none in HELLO, no server default configured.
    client.send(msg::BEGIN, vec![Value::empty_map()]).await;
    let reply = client.recv().await;
    let failure = reply.assert_failure();
    assert!(
        failure.get_str("code").unwrap().contains("Database"),
        "BEGIN without a database must name the problem"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_rejects_unknown_version() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    // Propose Bolt 3.0 only — unsupported.
    let (_client, chosen) = BoltClient::connect(addr, [0, 0, 0, 3]).await;
    assert_eq!(chosen, [0, 0, 0, 0], "no-overlap handshake answers zeros");
}

/// Bolt structure signatures the typed tests assert on.
const SIG_NODE: u8 = 0x4E;
const SIG_RELATIONSHIP: u8 = 0x52;
const SIG_DATE: u8 = 0x44;

fn as_structure(value: &Value) -> &Structure {
    match value {
        Value::Structure(s) => s,
        other => panic!("expected structure, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt5_returns_node_structures() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    client
        .run(
            r#"MATCH (n:Person {name: "Alice"}) RETURN n"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    assert_eq!(records.len(), 1);
    let node = as_structure(&records[0][0]);
    assert_eq!(node.signature, SIG_NODE);
    assert_eq!(
        node.fields.len(),
        4,
        "5.x node: id, labels, properties, element_id"
    );
    assert!(matches!(node.fields[0], Value::Integer(_)));
    assert_eq!(
        node.fields[1],
        Value::List(vec![Value::String("Person".into())])
    );
    let Value::Map(props) = &node.fields[2] else {
        panic!("properties map")
    };
    assert_eq!(props.get_str("name"), Some("Alice"));
    assert_eq!(props.get_int("age"), Some(30));
    // Ref-valued predicates are relationships, not node properties (Neo4j
    // parity): they surface via bound edge/path vars, never inlined here.
    assert!(
        props.get("knows").is_none(),
        "edges must not inline into nodes"
    );
    let Some(Value::Structure(birthday)) = props.get("birthday") else {
        panic!(
            "birthday must be a Date structure, got {:?}",
            props.get("birthday")
        )
    };
    assert_eq!(birthday.signature, SIG_DATE);
    assert_eq!(
        node.fields[3],
        Value::String("alice".into()),
        "element_id is the full IRI"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt44_node_has_no_element_id() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 4]).await;
    client.ready(false).await;

    client
        .run(
            r#"MATCH (n:Person {name: "Bob"}) RETURN n"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    let node = as_structure(&records[0][0]);
    assert_eq!(node.fields.len(), 3, "4.4 node: id, labels, properties");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_returns_date_structure() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    client
        .run(
            r#"MATCH (n:Person {name: "Alice"}) RETURN n.birthday AS b"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    let date = as_structure(&records[0][0]);
    assert_eq!(date.signature, SIG_DATE);
    // 1990-11-23 is 7631 days after 1970-01-01.
    assert_eq!(date.fields, vec![Value::Integer(7631)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_returns_relationship_structure() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    client
        .run(
            r#"MATCH (a:Person {name: "Alice"})-[e:knows]->(b) RETURN e"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    assert_eq!(records.len(), 1);
    let rel = as_structure(&records[0][0]);
    assert_eq!(rel.signature, SIG_RELATIONSHIP);
    assert_eq!(rel.fields.len(), 8, "5.x relationship carries element ids");
    assert_eq!(rel.fields[3], Value::String("knows".into()));
    assert_eq!(
        rel.fields[6],
        Value::String("alice".into()),
        "start_element_id"
    );
    assert_eq!(rel.fields[7], Value::String("bob".into()), "end_element_id");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_explicit_transaction_round_trip() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;
    let (mut observer, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    observer.ready(true).await;

    client.begin(LEDGER).await.assert_success();

    // Write inside the transaction.
    client
        .run(
            r#"CREATE (n:Person {name: "Carol", age: 27})"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (_, summary) = client.pull(-1).await;
    let summary = summary.assert_success();
    let Some(Value::Map(stats)) = summary.get("stats") else {
        panic!("tx write summary must carry stats")
    };
    assert_eq!(stats.get("contains-updates"), Some(&Value::Boolean(true)));

    // Read-your-writes inside the transaction.
    client
        .run(
            "MATCH (n:Person) RETURN count(n) AS c",
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    assert_eq!(
        records,
        vec![vec![Value::Integer(3)]],
        "tx sees its own write"
    );

    // Isolation: another session does NOT see the uncommitted write.
    assert_eq!(
        observer.person_count().await,
        2,
        "uncommitted write must be invisible"
    );

    // COMMIT surfaces a bookmark and the write becomes visible.
    let commit_meta = client.commit().await;
    let commit_meta = commit_meta.assert_success();
    assert!(
        commit_meta
            .get_str("bookmark")
            .unwrap_or("")
            .starts_with("fluree:t:"),
        "commit carries a bookmark"
    );
    assert_eq!(
        observer.person_count().await,
        3,
        "committed write visible everywhere"
    );
    assert_eq!(
        client.person_count().await,
        3,
        "session usable after commit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_multi_statement_transaction_commits_atomically() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    client.begin(LEDGER).await.assert_success();
    for name in ["Carol", "Dave", "Erin"] {
        client
            .run(
                "CREATE (n:Person {name: $name, age: 20})",
                {
                    let mut p = MapValue::new();
                    p.insert("name", name);
                    p
                },
                LEDGER,
            )
            .await
            .assert_success();
        client.pull(-1).await.1.assert_success();
    }
    // Second statement's effect is visible to the third: read count in-tx.
    client
        .run(
            "MATCH (n:Person) RETURN count(n) AS c",
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    assert_eq!(records, vec![vec![Value::Integer(5)]]);

    client.commit().await.assert_success();
    assert_eq!(client.person_count().await, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_rollback_discards_writes() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    client.begin(LEDGER).await.assert_success();
    client
        .run(
            r#"CREATE (n:Person {name: "Ghost"})"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    client.pull(-1).await.1.assert_success();
    client.rollback().await.assert_success();

    assert_eq!(
        client.person_count().await,
        2,
        "rolled-back write must vanish"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_tx_conflict_fails_transient_and_retries() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;
    let (mut rival, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    rival.ready(true).await;

    client.begin(LEDGER).await.assert_success();
    client
        .run(
            r#"CREATE (n:Person {name: "Carol"})"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    client.pull(-1).await.1.assert_success();

    // A rival autocommit write advances the head under the transaction.
    rival
        .run(
            r#"CREATE (n:Person {name: "Rival"})"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    rival.pull(-1).await.1.assert_success();

    // COMMIT must fail with a driver-retryable transient code.
    let failure = client.commit().await;
    let failure = failure.assert_failure();
    let code = failure.get_str("code").expect("failure code");
    assert!(
        code.starts_with("Neo.TransientError."),
        "conflict must be transient (drivers retry); got {code}"
    );
    assert_eq!(
        rival.person_count().await,
        3,
        "only the rival's write landed"
    );

    // The retry (fresh transaction) succeeds — the managed-tx contract.
    client.send(msg::RESET, vec![]).await;
    client.recv().await.assert_success();
    client.begin(LEDGER).await.assert_success();
    client
        .run(
            r#"CREATE (n:Person {name: "Carol"})"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    client.pull(-1).await.1.assert_success();
    client.commit().await.assert_success();
    assert_eq!(client.person_count().await, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_tx_statement_failure_poisons_and_reset_recovers() {
    let (_tmp, _state, addr) = bolt_server(LEDGER).await;
    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    client.begin(LEDGER).await.assert_success();
    client
        .run(
            r#"CREATE (n:Person {name: "Kept"})"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    client.pull(-1).await.1.assert_success();

    // A bad statement poisons the transaction.
    let failure = client.run("MATCH (n RETURN", MapValue::new(), LEDGER).await;
    failure.assert_failure();

    // COMMIT is IGNORED in the failed state.
    client.send(msg::COMMIT, vec![]).await;
    let reply = client.recv().await;
    assert_eq!(reply.signature, msg::IGNORED);

    // RESET drops the transaction; nothing committed.
    client.send(msg::RESET, vec![]).await;
    client.recv().await.assert_success();
    assert_eq!(
        client.person_count().await,
        2,
        "poisoned tx must not commit"
    );
}

/// Same node-structure contract as `bolt5_returns_node_structures`, but on
/// an **indexed** ledger: bindings come off the binary scan as `EncodedSid`
/// and hydration takes the batched sorted-crawl lane (overlay-free gate).
/// Guards the lane's correctness — labels, scalar properties, temporal
/// structures, and the no-adjacency rule must match the novelty path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_indexed_node_structures_via_batched_crawl() {
    let (_tmp, state, addr) = bolt_server(LEDGER).await;
    state
        .fluree
        .reindex(LEDGER, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");

    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;
    client
        .run(
            r#"MATCH (n:Person {name: "Alice"}) RETURN n"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    assert_eq!(records.len(), 1);
    let node = as_structure(&records[0][0]);
    assert_eq!(node.signature, SIG_NODE);
    assert_eq!(
        node.fields[1],
        Value::List(vec![Value::String("Person".into())])
    );
    let Value::Map(props) = &node.fields[2] else {
        panic!("properties map")
    };
    assert_eq!(props.get_str("name"), Some("Alice"));
    assert_eq!(props.get_int("age"), Some(30));
    assert!(props.get("knows").is_none(), "edges must not inline");
    let Some(Value::Structure(birthday)) = props.get("birthday") else {
        panic!(
            "birthday must be a Date structure, got {:?}",
            props.get("birthday")
        )
    };
    assert_eq!(birthday.signature, SIG_DATE);
    assert_eq!(birthday.fields, vec![Value::Integer(7631)]);
    assert_eq!(node.fields[3], Value::String("alice".into()));
}

/// The reified-edge gate's tiers, end to end. On an indexed ledger with no
/// annotations the per-hop probe is skipped (bound edge vars synthesize
/// their value — properties empty). An annotation inserted into novelty
/// afterwards must REOPEN the gate (tier 3, the overlay walk): the bound
/// edge var binds the annotation and exposes its properties. A closed gate
/// that failed to reopen would silently drop annotation bindings.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_reified_edge_gate_reopens_for_novelty_annotations() {
    let (_tmp, state, addr) = bolt_server(LEDGER).await;
    state
        .fluree
        .reindex(LEDGER, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");

    let (mut client, _) = BoltClient::connect(addr, [0, 0, 4, 5]).await;
    client.ready(true).await;

    // Tier 2 closed: no annotations anywhere — bound edge var synthesizes.
    client
        .run(
            r#"MATCH (a:Person {name: "Alice"})-[e:knows]->(b) RETURN e"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    let rel = as_structure(&records[0][0]);
    assert_eq!(rel.signature, SIG_RELATIONSHIP);
    let Value::Map(props) = &rel.fields[4] else {
        panic!("rel props")
    };
    assert!(props.is_empty(), "unannotated edge has no properties");

    // Insert an annotated edge into NOVELTY (post-index): tier 3 must
    // reopen the gate via the overlay walk.
    insert(
        &state,
        LEDGER,
        json!({
            "@context": {},
            "@id": "alice",
            "likes": {"@id": "bob", "@annotation": {"since": 2020}}
        }),
    )
    .await;

    client
        .run(
            r#"MATCH (a:Person {name: "Alice"})-[e:likes]->(b) RETURN e"#,
            MapValue::new(),
            LEDGER,
        )
        .await
        .assert_success();
    let (records, _) = client.pull(-1).await;
    assert_eq!(records.len(), 1, "annotated edge matches");
    let rel = as_structure(&records[0][0]);
    assert_eq!(rel.signature, SIG_RELATIONSHIP);
    assert_eq!(rel.fields[3], Value::String("likes".into()));
    let Value::Map(props) = &rel.fields[4] else {
        panic!("rel props")
    };
    assert_eq!(
        props.get_int("since"),
        Some(2020),
        "novelty annotation must bind through the reopened gate: {props:?}"
    );
}
