//! End-to-end Bolt authentication: HELLO/LOGON credential verification
//! against `data_auth_mode`, ledger-scope gating, per-statement token
//! expiry, LOGOFF re-auth, and identity-derived policy filtering — the
//! Bolt counterpart of `data_auth_integration.rs` + `policy_integration.rs`.
#![cfg(feature = "bolt")]

use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_bolt::chunk::{write_message, ChunkAssembler};
use fluree_db_bolt::handshake::MAGIC;
use fluree_db_bolt::message as msg;
use fluree_db_bolt::packstream;
use fluree_db_bolt::value::{MapValue, Structure, Value};
use fluree_db_credential::did_from_pubkey;
use fluree_db_server::config::DataAuthMode;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tower::ServiceExt;

const BOLT_5_4: [u8; 4] = [0, 0, 4, 5];
const BOLT_4_4: [u8; 4] = [0, 0, 4, 4];

// ---------------------------------------------------------------------
// Server + token helpers
// ---------------------------------------------------------------------

async fn auth_server(mode: DataAuthMode) -> (TempDir, Arc<AppState>, std::net::SocketAddr) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_mode: mode,
        data_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"));
    let (addr, _task) =
        fluree_db_server::bolt::spawn_listener(Arc::clone(&state), "127.0.0.1:0".parse().unwrap())
            .await
            .expect("bolt listener");
    (tmp, state, addr)
}

async fn http_insert(state: &Arc<AppState>, uri: &str, body: serde_json::Value) {
    // Seeding always presents a write-scoped token: required-mode servers
    // demand it, Optional-mode servers verify-and-allow it.
    let token = seed_token(uri);
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::OK || resp.status() == StatusCode::CREATED,
        "{uri}: {:?}",
        resp.status()
    );
}

/// A seeding token whose write scope covers the ledger named in `uri`.
/// No `fluree.identity` claim: seeding must not resolve a policy context.
fn seed_token(uri: &str) -> String {
    let ledger = uri.strip_prefix("/v1/fluree/insert/").unwrap_or("");
    let key = signing_key();
    let claims = serde_json::json!({
        "iss": did_from_pubkey(&key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.ledger.read.all": true,
        "fluree.ledger.write.ledgers": [ledger],
    });
    create_jws(&claims, &key)
}

async fn seed_ledger(state: &Arc<AppState>, ledger: &str) {
    http_insert(
        state,
        "/v1/fluree/create",
        serde_json::json!({"ledger": ledger}),
    )
    .await;
    http_insert(
        state,
        &format!("/v1/fluree/insert/{ledger}"),
        serde_json::json!({
            "@context": {},
            "@graph": [
                {"@id": "alice", "@type": "Person", "name": "Alice"},
                {"@id": "bob", "@type": "Person", "name": "Bob"}
            ]
        }),
    )
    .await;
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn create_jws(claims: &serde_json::Value, signing_key: &SigningKey) -> String {
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes());
    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {"kty": "OKP", "crv": "Ed25519", "x": pubkey_b64}
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let sig_b64 = URL_SAFE_NO_PAD.encode(signing_key.sign(signing_input.as_bytes()).to_bytes());
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

fn signing_key() -> SigningKey {
    SigningKey::from_bytes(&[7u8; 32])
}

/// A read+write token for `ledgers`, expiring at `exp`.
fn scoped_token(read: &[&str], write: &[&str], exp: u64) -> String {
    let key = signing_key();
    let claims = serde_json::json!({
        "iss": did_from_pubkey(&key.verifying_key().to_bytes()),
        "exp": exp,
        "iat": now_secs(),
        "fluree.ledger.read.ledgers": read,
        "fluree.ledger.write.ledgers": write,
    });
    create_jws(&claims, &key)
}

/// A read token carrying a `fluree.identity` for policy resolution.
fn identity_token(identity: &str, ledger: &str) -> String {
    let key = signing_key();
    let claims = serde_json::json!({
        "iss": did_from_pubkey(&key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.identity": identity,
        "fluree.ledger.read.ledgers": [ledger],
    });
    create_jws(&claims, &key)
}

// ---------------------------------------------------------------------
// Minimal Bolt client (same codec crate as the server)
// ---------------------------------------------------------------------

struct BoltClient {
    socket: TcpStream,
    assembler: ChunkAssembler,
    read_buf: Vec<u8>,
}

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

    fn assert_failure_code(&self, code: &str) {
        assert_eq!(
            self.signature,
            msg::FAILURE,
            "expected FAILURE, got 0x{:02X} {:?}",
            self.signature,
            self.field
        );
        assert_eq!(
            self.metadata().get_str("code"),
            Some(code),
            "failure metadata: {:?}",
            self.field
        );
    }
}

fn auth_map(scheme: &str, principal: Option<&str>, credentials: Option<&str>) -> MapValue {
    let mut auth = MapValue::new();
    auth.insert("scheme", scheme);
    if let Some(p) = principal {
        auth.insert("principal", p);
    }
    if let Some(c) = credentials {
        auth.insert("credentials", c);
    }
    auth
}

impl BoltClient {
    async fn connect(addr: std::net::SocketAddr, proposal: [u8; 4]) -> Self {
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
        Self {
            socket,
            assembler: ChunkAssembler::new(),
            read_buf: vec![0; 8192],
        }
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

    /// True when the server closed the connection (after flushing replies).
    async fn assert_closed(&mut self) {
        loop {
            match self.socket.read(&mut self.read_buf).await {
                Ok(0) => return,
                Ok(n) => self.assembler.push(&self.read_buf[..n]).expect("assemble"),
                Err(_) => return,
            }
        }
    }

    async fn hello(&mut self, extra: MapValue) -> Reply {
        self.send(msg::HELLO, vec![Value::Map(extra)]).await;
        self.recv().await
    }

    async fn logon(&mut self, auth: MapValue) -> Reply {
        self.send(msg::LOGON, vec![Value::Map(auth)]).await;
        self.recv().await
    }

    /// 5.4 session: HELLO then LOGON with `auth`.
    async fn ready_54(addr: std::net::SocketAddr, auth: MapValue) -> Self {
        let mut c = Self::connect(addr, BOLT_5_4).await;
        c.hello(MapValue::new()).await.assert_success();
        c.logon(auth).await.assert_success();
        c
    }

    async fn run(&mut self, query: &str, db: &str) -> Reply {
        let mut extra = MapValue::new();
        extra.insert("db", db);
        self.send(
            msg::RUN,
            vec![Value::from(query), Value::empty_map(), Value::Map(extra)],
        )
        .await;
        self.recv().await
    }

    /// PULL everything, returning (records, summary).
    async fn pull(&mut self) -> (Vec<Vec<Value>>, Reply) {
        let mut extra = MapValue::new();
        extra.insert("n", -1i64);
        self.send(msg::PULL, vec![Value::Map(extra)]).await;
        let mut records = Vec::new();
        loop {
            let reply = self.recv().await;
            match reply.signature {
                msg::RECORD => match reply.field {
                    Value::List(l) => records.push(l),
                    other => panic!("record payload: {other:?}"),
                },
                _ => return (records, reply),
            }
        }
    }

    /// RUN + PULL, asserting both succeed; returns the records.
    async fn query_rows(&mut self, query: &str, db: &str) -> Vec<Vec<Value>> {
        self.run(query, db).await.assert_success();
        let (records, summary) = self.pull().await;
        summary.assert_success();
        records
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const LEDGER: &str = "boltauth:main";

#[tokio::test]
async fn required_mode_rejects_anonymous_logon_and_accepts_bearer() {
    let (_tmp, state, addr) = auth_server(DataAuthMode::Required).await;
    seed_ledger(&state, LEDGER).await;

    let mut c = BoltClient::connect(addr, BOLT_5_4).await;
    c.hello(MapValue::new()).await.assert_success();

    // Anonymous LOGON refused; the connection stays open for re-auth.
    c.logon(auth_map("none", None, None))
        .await
        .assert_failure_code("Neo.ClientError.Security.Unauthorized");

    // RESET recovers to the authentication state, and a valid bearer
    // token then authenticates the same connection.
    c.send(msg::RESET, vec![]).await;
    c.recv().await.assert_success();
    let token = scoped_token(&[LEDGER], &[], now_secs() + 3600);
    c.logon(auth_map("bearer", None, Some(&token)))
        .await
        .assert_success();

    let rows = c
        .query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[tokio::test]
async fn required_mode_bolt44_auth_rides_in_hello() {
    let (_tmp, state, addr) = auth_server(DataAuthMode::Required).await;
    seed_ledger(&state, LEDGER).await;

    // Valid bearer credentials inside the HELLO extra map (≤5.0 shape).
    let token = scoped_token(&[LEDGER], &[], now_secs() + 3600);
    let mut c = BoltClient::connect(addr, BOLT_4_4).await;
    let mut extra = auth_map("bearer", Some("neo4j"), Some(&token));
    extra.insert("user_agent", "test/1.0");
    let meta = c.hello(extra).await;
    let meta = meta.assert_success();
    assert!(meta.get_str("server").unwrap().starts_with("Neo4j/"));
    let rows = c
        .query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;
    assert_eq!(rows[0][0], Value::Integer(2));

    // Anonymous HELLO under required auth: FAILURE, then the server
    // closes (no pre-5.1 re-auth path).
    let mut c = BoltClient::connect(addr, BOLT_4_4).await;
    c.hello(MapValue::new())
        .await
        .assert_failure_code("Neo.ClientError.Security.Unauthorized");
    c.assert_closed().await;
}

#[tokio::test]
async fn basic_scheme_carries_token_as_password() {
    let (_tmp, state, addr) = auth_server(DataAuthMode::Required).await;
    seed_ledger(&state, LEDGER).await;

    // Driver code shaped as user/password: the password field carries the
    // token; the principal is ignored (identity comes from the claims).
    let token = scoped_token(&[LEDGER], &[], now_secs() + 3600);
    let mut c =
        BoltClient::ready_54(addr, auth_map("basic", Some("any-user-name"), Some(&token))).await;
    let rows = c
        .query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[tokio::test]
async fn optional_mode_allows_anonymous_but_rejects_bad_tokens() {
    let (_tmp, state, addr) = auth_server(DataAuthMode::Optional).await;
    seed_ledger(&state, LEDGER).await;

    // Anonymous session works in Optional mode.
    let mut c = BoltClient::ready_54(addr, auth_map("none", None, None)).await;
    let rows = c
        .query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;
    assert_eq!(rows[0][0], Value::Integer(2));

    // A presented-but-invalid token is an auth failure, not anonymous.
    let mut c = BoltClient::connect(addr, BOLT_5_4).await;
    c.hello(MapValue::new()).await.assert_success();
    c.logon(auth_map("bearer", None, Some("not-a-jwt")))
        .await
        .assert_failure_code("Neo.ClientError.Security.Unauthorized");
}

#[tokio::test]
async fn scoped_token_gates_ledgers_and_writes() {
    let (_tmp, state, addr) = auth_server(DataAuthMode::Required).await;
    seed_ledger(&state, LEDGER).await;
    seed_ledger(&state, "boltauth:other").await;

    // Read-only scope on LEDGER only.
    let token = scoped_token(&[LEDGER], &[], now_secs() + 3600);
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;

    // In-scope read works.
    let rows = c
        .query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;
    assert_eq!(rows[0][0], Value::Integer(2));

    // Out-of-scope ledger: DatabaseNotFound (existence-hiding, like the
    // HTTP routes' 404).
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    c.run("MATCH (n:Person) RETURN count(n) AS c", "boltauth:other")
        .await
        .assert_failure_code("Neo.ClientError.Database.DatabaseNotFound");

    // No write scope: writes to the readable ledger are refused too.
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    c.run("CREATE (:Person {name: 'Eve'})", LEDGER)
        .await
        .assert_failure_code("Neo.ClientError.Database.DatabaseNotFound");

    // A write-scoped token can write.
    let token = scoped_token(&[LEDGER], &[LEDGER], now_secs() + 3600);
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    c.run("CREATE (:Person {name: 'Eve'})", LEDGER)
        .await
        .assert_success();
    let (_, summary) = c.pull().await;
    summary.assert_success();
    let rows = c
        .query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;
    assert_eq!(rows[0][0], Value::Integer(3));
}

#[tokio::test]
async fn token_expiry_is_rechecked_per_statement() {
    let (_tmp, state, addr) = auth_server(DataAuthMode::Required).await;
    seed_ledger(&state, LEDGER).await;

    // exp 30s in the past: login-time validation passes (60s clock skew),
    // but the per-statement check (no skew — same clock) refuses to
    // execute, answering TokenExpired so 5.x drivers re-authenticate.
    let token = scoped_token(&[LEDGER], &[], now_secs() - 30);
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    c.run("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await
        .assert_failure_code("Neo.ClientError.Security.TokenExpired");
}

#[tokio::test]
async fn logoff_drops_identity_and_requires_reauth() {
    let (_tmp, state, addr) = auth_server(DataAuthMode::Required).await;
    seed_ledger(&state, LEDGER).await;

    let token = scoped_token(&[LEDGER], &[], now_secs() + 3600);
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    c.query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;

    // LOGOFF returns to authentication; anonymous re-LOGON is refused.
    c.send(msg::LOGOFF, vec![]).await;
    c.recv().await.assert_success();
    c.logon(auth_map("none", None, None))
        .await
        .assert_failure_code("Neo.ClientError.Security.Unauthorized");

    // RESET + a fresh bearer LOGON restores the session.
    c.send(msg::RESET, vec![]).await;
    c.recv().await.assert_success();
    c.logon(auth_map("bearer", None, Some(&token)))
        .await
        .assert_success();
    let rows = c
        .query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[tokio::test]
async fn explicit_transactions_enforce_scopes_per_statement() {
    let (_tmp, state, addr) = auth_server(DataAuthMode::Required).await;
    seed_ledger(&state, LEDGER).await;
    seed_ledger(&state, "boltauth:other").await;

    // BEGIN on an out-of-scope ledger is refused (existence-hiding).
    let token = scoped_token(&[LEDGER], &[], now_secs() + 3600);
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    let mut extra = MapValue::new();
    extra.insert("db", "boltauth:other");
    c.send(msg::BEGIN, vec![Value::Map(extra)]).await;
    c.recv()
        .await
        .assert_failure_code("Neo.ClientError.Database.DatabaseNotFound");

    // A read-scoped token can open a transaction and read, but a write
    // statement inside it is refused per-statement.
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    let mut extra = MapValue::new();
    extra.insert("db", LEDGER);
    c.send(msg::BEGIN, vec![Value::Map(extra)]).await;
    c.recv().await.assert_success();
    let rows = c
        .query_rows("MATCH (n:Person) RETURN count(n) AS c", LEDGER)
        .await;
    assert_eq!(rows[0][0], Value::Integer(2));
    c.run("CREATE (:Person {name: 'Eve'})", LEDGER)
        .await
        .assert_failure_code("Neo.ClientError.Database.DatabaseNotFound");
}

#[tokio::test]
async fn identity_derived_policy_filters_bolt_reads() {
    // The full over-Neo4j story: two sessions, two bearer tokens, same
    // Cypher — different graphs. Policy comes from the identity's
    // in-ledger f:policyClass binding; no policy knobs on the transport.
    let (_tmp, state, addr) = auth_server(DataAuthMode::Optional).await;
    let ledger = "boltauth:policy";
    http_insert(
        &state,
        "/v1/fluree/create",
        serde_json::json!({"ledger": ledger}),
    )
    .await;
    http_insert(
        &state,
        &format!("/v1/fluree/insert/{ledger}"),
        serde_json::json!({
            "@context": {"schema": "http://schema.org/"},
            "insert": [
                {"@id": "doc1", "@type": "Document",
                 "schema:name": "Public Post", "classification": "public"},
                {"@id": "doc2", "@type": "Document",
                 "schema:name": "Internal Memo", "classification": "internal"},
                {"@id": "doc3", "@type": "Document",
                 "schema:name": "Executive Salaries", "classification": "confidential"}
            ]
        }),
    )
    .await;
    http_insert(
        &state,
        &format!("/v1/fluree/insert/{ledger}"),
        serde_json::json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "insert": [
                {
                    "@id": "public-policy",
                    "@type": ["f:AccessPolicy", "PublicClass"],
                    "f:action": [{"@id": "f:view"}],
                    "f:query": {
                        "@type": "@json",
                        "@value": {
                            "@context": {},
                            "where": [{"@id": "?$this", "classification": "public"}]
                        }
                    }
                },
                {
                    "@id": "manager-policy",
                    "@type": ["f:AccessPolicy", "ManagerClass"],
                    "f:action": [{"@id": "f:view"}],
                    "f:allow": true
                },
                {
                    "@id": "did:example:public-user",
                    "f:policyClass": [{"@id": "PublicClass"}]
                },
                {
                    "@id": "did:example:manager-user",
                    "f:policyClass": [{"@id": "ManagerClass"}]
                }
            ]
        }),
    )
    .await;

    let q = "MATCH (d:Document) RETURN d ORDER BY d.name";

    // The public identity sees only the public document — including in the
    // hydrated node's properties (the typed-table/Bolt hydration path).
    let token = identity_token("did:example:public-user", ledger);
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    let rows = c.query_rows(q, ledger).await;
    assert_eq!(rows.len(), 1, "public identity sees one document: {rows:?}");
    let node_str = format!("{rows:?}");
    assert!(
        node_str.contains("Public Post") && !node_str.contains("Salaries"),
        "hydrated node must be the public doc only: {node_str}"
    );

    // The manager identity sees all three with the SAME query.
    let token = identity_token("did:example:manager-user", ledger);
    let mut c = BoltClient::ready_54(addr, auth_map("bearer", None, Some(&token))).await;
    let rows = c.query_rows(q, ledger).await;
    assert_eq!(
        rows.len(),
        3,
        "manager identity sees all documents: {rows:?}"
    );

    // An anonymous session (Optional mode, no token → no identity) runs
    // unpoliced, matching the HTTP data plane's semantics.
    let mut c = BoltClient::ready_54(addr, auth_map("none", None, None)).await;
    let rows = c.query_rows(q, ledger).await;
    assert_eq!(rows.len(), 3);
}
