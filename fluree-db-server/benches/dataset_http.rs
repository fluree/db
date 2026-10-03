//! Warm, in-process HTTP requests that carry a dataset clause (SPARQL `FROM` /
//! `FROM NAMED`, JSON-LD `from` / `fromNamed`) or a `GRAPH` scope, on the
//! connection route (`/query`), the ledger route (`/query/{ledger}`) and the
//! streaming ledger route (`/stream/query/{ledger}`). Socket/TLS costs are
//! excluded; everything from request parsing to response serialization is in.
//!
//! These are the request shapes whose dataset references are classified and
//! resolved per request: how many times a SPARQL body is parsed, how each
//! `FROM` / `FROM NAMED` IRI is resolved, and how the dataset is built. The
//! `w*` cases resolve against a ledger with 10k registered graphs, where a
//! per-request cost that grows with the registry shows. Each case asserts its
//! row count once before timing, so a change that alters the answer cannot
//! pass as a speed-up.
//!
//! Run with `cargo bench -p fluree-db-server --bench dataset_http`.

use axum::{body::Body, Router};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use criterion::{criterion_group, criterion_main, Criterion};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_server::{
    config::DataAuthMode, routes::build_router, AppState, ServerConfig, TelemetryConfig,
};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::{
    fmt::Write as _,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

const LEDGER: &str = "dsx:main";
const STREAM_LEDGER: &str = "dsx-stream:main";
/// A ledger with many named graphs, so a request that resolves its dataset in
/// the path's ledger shows what that resolution costs against a large graph
/// registry (per reference, not per registered graph).
const WIDE_LEDGER: &str = "dsx-wide:main";
/// Named graphs in `WIDE_LEDGER`, one triple each.
const WIDE_GRAPHS: usize = 10_000;
/// Graphs `WIDE_LEDGER` registers per seeding commit: one commit registers at
/// most 256 new graphs.
const WIDE_BATCH: usize = 250;
const AUDIENCE: &str = "dataset-bench";
/// Policy class the bearer-token requests are evaluated under: one rule that
/// allows every read, so the bearer case returns the same rows as the others.
const READER_CLASS: &str = "http://ex.org/Reader";
/// Named graphs in `LEDGER`, one triple each.
const NAMED_GRAPHS: usize = 16;
/// Default-graph rows in `STREAM_LEDGER`.
const STREAM_ROWS: usize = 10_000;

fn named_graph(i: usize) -> String {
    format!("http://ex.org/graphs/g{i}")
}

fn seed_trig() -> String {
    let mut trig = format!(
        "@prefix ex: <http://ex.org/> .\n\
         @prefix f: <https://ns.flur.ee/db#> .\n\
         ex:d ex:title \"D\" .\n\
         ex:read a <{READER_CLASS}> ; f:action f:view ; f:allow true .\n\
         GRAPH <urn:ex:doc:1> {{ ex:ep1 ex:title \"E1\" . }}\n"
    );
    for i in 0..NAMED_GRAPHS {
        let _ = writeln!(
            trig,
            "GRAPH <{}> {{ ex:s{i} ex:title \"G{i}\" . }}",
            named_graph(i)
        );
    }
    trig
}

fn token(key: &SigningKey) -> String {
    let public = key.verifying_key().to_bytes();
    let claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&public),
        "aud": AUDIENCE, "sub": "http://example.org/user",
        "exp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600,
        "fluree.ledger.read.ledgers": [LEDGER]
    });
    let header = json!({"alg": "EdDSA", "jwk": {
        "kty": "OKP", "crv": "Ed25519", "x": URL_SAFE_NO_PAD.encode(public)
    }});
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let signature = key.sign(input.as_bytes());
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Vec<u8>) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, bytes.to_vec())
}

async fn app(mode: DataAuthMode, key: &SigningKey) -> (tempfile::TempDir, Router) {
    let directory = tempfile::tempdir().unwrap();
    let config = ServerConfig {
        storage_path: Some(directory.path().to_owned()),
        indexing_enabled: false,
        cors_enabled: false,
        data_auth_mode: mode,
        data_auth_audience: Some(AUDIENCE.into()),
        data_auth_trusted_issuers: vec![fluree_db_credential::did_from_pubkey(
            &key.verifying_key().to_bytes(),
        )],
        data_auth_default_policy_class: Some(READER_CLASS.into()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&config);
    let state = Arc::new(AppState::new(config, telemetry).await.unwrap());

    let ledger = state.fluree.create_ledger(LEDGER).await.unwrap();
    state
        .fluree
        .stage_owned(ledger)
        .upsert_turtle(&seed_trig())
        .execute()
        .await
        .unwrap();

    let ledger = state.fluree.create_ledger(STREAM_LEDGER).await.unwrap();
    let rows: Vec<Value> = (0..STREAM_ROWS)
        .map(|i| json!({"@id": format!("http://ex.org/p{i}"), "http://ex.org/v": i}))
        .collect();
    state
        .fluree
        .insert(ledger, &json!({"@graph": rows}))
        .await
        .unwrap();

    if matches!(mode, DataAuthMode::None) {
        let mut ledger = state.fluree.create_ledger(WIDE_LEDGER).await.unwrap();
        for start in (0..WIDE_GRAPHS).step_by(WIDE_BATCH) {
            let mut trig = String::from("@prefix ex: <http://ex.org/> .\nex:d ex:title \"D\" .\n");
            for i in start..start + WIDE_BATCH {
                let _ = writeln!(
                    trig,
                    "GRAPH <{}> {{ ex:s{i} ex:title \"G{i}\" . }}",
                    named_graph(i)
                );
            }
            ledger = state
                .fluree
                .stage_owned(ledger)
                .upsert_turtle(&trig)
                .execute()
                .await
                .unwrap()
                .ledger;
        }
    }
    (directory, build_router(state))
}

enum QueryText {
    Sparql(String),
    JsonLd(Value),
}

struct Case {
    name: &'static str,
    uri: String,
    body: QueryText,
    headers: Vec<(&'static str, String)>,
    /// Rows the answer must have, checked once before timing.
    rows: usize,
    authenticated: bool,
}

fn request(case: &Case) -> Request<Body> {
    let mut builder = Request::builder().method("POST").uri(&case.uri);
    let body = match &case.body {
        QueryText::Sparql(q) => {
            builder = builder.header("content-type", "application/sparql-query");
            q.clone()
        }
        QueryText::JsonLd(q) => {
            builder = builder.header("content-type", "application/json");
            q.to_string()
        }
    };
    for (name, value) in &case.headers {
        builder = builder.header(*name, value);
    }
    builder.body(Body::from(body)).unwrap()
}

/// Row count of a SPARQL-results, JSON-LD row array, or NDJSON stream body.
fn row_count(uri: &str, bytes: &[u8]) -> usize {
    if uri.contains("/stream/") {
        return std::str::from_utf8(bytes)
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str::<Value>(l).unwrap())
            .filter(|r| r["type"] == "row")
            .count();
    }
    let json: Value = serde_json::from_slice(bytes).unwrap();
    match json.pointer("/results/bindings") {
        Some(bindings) => bindings.as_array().unwrap().len(),
        None => json.as_array().map_or(0, Vec::len),
    }
}

fn cases(bearer: &str) -> Vec<Case> {
    let conn = "/v1/fluree/query".to_string();
    let on_ledger = format!("/v1/fluree/query/{LEDGER}");
    let stream = |ledger: &str| format!("/v1/fluree/stream/query/{ledger}");
    let prefix = "PREFIX ex: <http://ex.org/>\n";
    let sparql = |q: &str| QueryText::Sparql(format!("{prefix}{q}"));
    let values: String = (0..96)
        .map(|i| format!("<http://ex.org/value/{i:04}> "))
        .collect();
    let from_named_16: String = (0..NAMED_GRAPHS)
        .map(|i| format!("FROM NAMED <{}> ", named_graph(i)))
        .collect();
    let default_allow = vec![("fluree-default-allow", "true".to_string())];
    vec![
        Case {
            name: "a_conn_from_default_pattern",
            uri: conn.clone(),
            body: sparql("SELECT ?t FROM <dsx:main> WHERE { ex:d ex:title ?t }"),
            headers: vec![],
            rows: 1,
            authenticated: false,
        },
        Case {
            name: "b_conn_from_named_ledger_graph",
            uri: conn.clone(),
            body: sparql(
                "SELECT ?g ?t FROM <dsx:main> FROM NAMED <dsx:main#urn:ex:doc:1> \
                 WHERE { GRAPH ?g { ?s ex:title ?t } }",
            ),
            headers: vec![],
            rows: 1,
            authenticated: false,
        },
        Case {
            name: "c_ledger_from_named_graph",
            uri: on_ledger.clone(),
            body: sparql(
                "SELECT ?t FROM <dsx:main> FROM NAMED <urn:ex:doc:1> \
                 WHERE { GRAPH <urn:ex:doc:1> { ?s ex:title ?t } }",
            ),
            headers: vec![],
            rows: 1,
            authenticated: false,
        },
        Case {
            name: "d_ledger_no_from_graph",
            uri: on_ledger.clone(),
            body: sparql("SELECT ?t WHERE { GRAPH <urn:ex:doc:1> { ?s ex:title ?t } }"),
            headers: vec![],
            rows: 1,
            authenticated: false,
        },
        Case {
            // The whole-ledger FROM shape (#1975): today a strict one-graph
            // dataset, so the GRAPH pattern matches nothing.
            name: "e_conn_whole_ledger_from_graph",
            uri: conn.clone(),
            body: sparql("SELECT ?t FROM <dsx:main> WHERE { GRAPH <urn:ex:doc:1> { ?s ex:title ?t } }"),
            headers: vec![],
            rows: 0,
            authenticated: false,
        },
        Case {
            name: "f_conn_jsonld_from_graph",
            uri: conn.clone(),
            body: QueryText::JsonLd(json!({
                "from": LEDGER,
                "select": ["?t"],
                "where": [["graph", "urn:ex:doc:1", {"@id": "?s", "http://ex.org/title": "?t"}]]
            })),
            headers: vec![],
            rows: 1,
            authenticated: false,
        },
        Case {
            name: "g_conn_bearer_from_default_pattern",
            uri: conn.clone(),
            body: sparql("SELECT ?t FROM <dsx:main> WHERE { ex:d ex:title ?t }"),
            headers: vec![("authorization", format!("Bearer {bearer}"))],
            rows: 1,
            authenticated: true,
        },
        Case {
            name: "h_conn_values_2kb",
            uri: conn.clone(),
            body: sparql(&format!(
                "SELECT ?t FROM <dsx:main> WHERE {{ VALUES ?v {{ {values} }} ex:d ex:title ?t }} LIMIT 1"
            )),
            headers: vec![],
            rows: 1,
            authenticated: false,
        },
        Case {
            // Resolves one FROM and one FROM NAMED in a ledger that registers
            // WIDE_GRAPHS graphs: the cost is per reference, not per graph.
            name: "w1_ledger_from_named_wide_registry",
            uri: format!("/v1/fluree/query/{WIDE_LEDGER}"),
            body: sparql(&format!(
                "SELECT ?t FROM <{WIDE_LEDGER}> FROM NAMED <{g}> WHERE {{ GRAPH <{g}> {{ ?s ex:title ?t }} }}",
                g = named_graph(5_000)
            )),
            headers: vec![],
            rows: 1,
            authenticated: false,
        },
        Case {
            // The JSON-LD twin, naming the graph through the ledger's address
            // (`L#<g>`), a form every version answers.
            name: "w2_ledger_jsonld_from_named_wide_registry",
            uri: format!("/v1/fluree/query/{WIDE_LEDGER}"),
            body: QueryText::JsonLd(json!({
                "fromNamed": [format!("{WIDE_LEDGER}#{}", named_graph(5_000))],
                "select": ["?t"],
                "where": [[
                    "graph",
                    format!("{WIDE_LEDGER}#{}", named_graph(5_000)),
                    {"@id": "?s", "http://ex.org/title": "?t"}
                ]]
            })),
            headers: vec![],
            rows: 1,
            authenticated: false,
        },
        Case {
            name: "s1_stream_default_rows",
            uri: stream(STREAM_LEDGER),
            body: sparql("SELECT ?s ?v WHERE { ?s ex:v ?v }"),
            headers: vec![],
            rows: STREAM_ROWS,
            authenticated: false,
        },
        Case {
            name: "s2_stream_default_rows_policy",
            uri: stream(STREAM_LEDGER),
            body: sparql("SELECT ?s ?v WHERE { ?s ex:v ?v }"),
            headers: default_allow.clone(),
            rows: STREAM_ROWS,
            authenticated: false,
        },
        Case {
            name: "s5_stream_from_named_16_policy",
            uri: stream(LEDGER),
            body: sparql(&format!(
                "SELECT ?g ?t FROM <dsx:main> {from_named_16} WHERE {{ GRAPH ?g {{ ?s ex:title ?t }} }}"
            )),
            headers: default_allow,
            rows: NAMED_GRAPHS,
            authenticated: false,
        },
    ]
}

fn bench_dataset_http(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let key = SigningKey::from_bytes(&[37; 32]);
    let (_plain_dir, plain) = runtime.block_on(app(DataAuthMode::None, &key));
    let (_auth_dir, authenticated) = runtime.block_on(app(DataAuthMode::Required, &key));
    let bearer = token(&key);

    let mut group = c.benchmark_group("dataset_http");
    for case in cases(&bearer) {
        let app = if case.authenticated {
            &authenticated
        } else {
            &plain
        };
        // Warm caches and pin the answer before timing.
        let (status, bytes) = runtime.block_on(send(app, request(&case)));
        assert_eq!(
            status,
            StatusCode::OK,
            "{}: {}",
            case.name,
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(row_count(&case.uri, &bytes), case.rows, "{}", case.name);
        group.bench_function(case.name, |b| {
            b.to_async(&runtime).iter(|| async {
                let (status, bytes) = send(app, request(&case)).await;
                debug_assert_eq!(status, StatusCode::OK);
                bytes
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_dataset_http);
criterion_main!(benches);
