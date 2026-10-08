//! HTTP coverage for the GraphQL routes.
//!
//! `POST /v1/fluree/graphql/<ledger>` answers queries against the schema derived
//! from the ledger's own data, and `GET /v1/fluree/graphql-schema/<ledger>`
//! returns that schema as SDL. Neither needs any GraphQL configuration on the
//! ledger.

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
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

async fn send(state: &Arc<AppState>, request: Request<Body>) -> (StatusCode, String) {
    let resp = build_router(Arc::clone(state))
        .oneshot(request)
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// A ledger with two people and a default context.
///
/// The context matters to GraphQL: it is what shortens IRIs into the names this
/// ledger's users already write, so it decides both the GraphQL field names and
/// the form `id` values come back in.
async fn seeded(ledger: &str) -> (TempDir, Arc<AppState>) {
    let (tmp, state) = server_state().await;
    let (status, body) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/create")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "ledger": ledger }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");

    let (status, body) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/insert/{ledger}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "@context": { "ex": "http://example.org/" },
                    "@graph": [
                        {
                            "@id": "ex:alice",
                            "@type": "ex:Person",
                            "ex:name": "Alice",
                            "ex:knows": [{ "@id": "ex:bob" }]
                        },
                        { "@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob" }
                    ]
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert!(status.is_success(), "insert: {body}");

    let (status, body) = send(
        &state,
        Request::builder()
            .method("PUT")
            .uri(format!("/v1/fluree/context/{ledger}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "@context": { "ex": "http://example.org/" } }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert!(status.is_success(), "set context: {body}");
    (tmp, state)
}

fn post_json(ledger: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/graphql/{ledger}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn post_json_envelope_answers_a_query() {
    let (_tmp, state) = seeded("gqlhttp").await;
    let (status, body) = send(
        &state,
        post_json(
            "gqlhttp",
            json!({ "query": "{ persons(orderBy: { id: ASC }) { id name knows { id } } }" }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(json.get("errors").is_none(), "{body}");
    assert_eq!(
        json["data"],
        json!({
            "persons": [
                { "id": "ex:alice", "name": ["Alice"], "knows": [{ "id": "ex:bob" }] },
                { "id": "ex:bob", "name": ["Bob"], "knows": [] }
            ]
        })
    );
}

#[tokio::test]
async fn variables_and_operation_name_are_honoured() {
    let (_tmp, state) = seeded("gqlvars").await;
    let (status, body) = send(
        &state,
        post_json(
            "gqlvars",
            json!({
                "query": "query A($n: String!) { persons(where: { name: { EQ: $n } }) { id } }\
                          \nquery B { persons_count }",
                "variables": { "n": "Bob" },
                "operationName": "A"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        json["data"],
        json!({ "persons": [{ "id": "ex:bob" }] }),
        "{body}"
    );
}

#[tokio::test]
async fn application_graphql_bodies_and_get_queries_work() {
    let (_tmp, state) = seeded("gqlforms").await;

    let (status, body) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/graphql/gqlforms")
            .header("content-type", "application/graphql")
            .body(Body::from("{ persons_count }"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["data"], json!({ "persons_count": 2 }), "{body}");

    // The GET form GraphiQL and other browser clients use.
    let (status, body) = send(
        &state,
        Request::builder()
            .method("GET")
            .uri("/v1/fluree/graphql/gqlforms?query=%7B%20persons_count%20%7D")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["data"], json!({ "persons_count": 2 }), "{body}");
}

#[tokio::test]
async fn the_schema_endpoint_returns_sdl() {
    let (_tmp, state) = seeded("gqlsdl").await;
    let (status, body) = send(
        &state,
        Request::builder()
            .method("GET")
            .uri("/v1/fluree/graphql-schema/gqlsdl")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("type Person {"), "{body}");
    assert!(body.contains("persons(where: PersonFilter"), "{body}");
    assert!(body.contains("type Query {"), "{body}");
}

#[tokio::test]
async fn a_graphql_error_is_a_200_with_an_errors_array() {
    let (_tmp, state) = seeded("gqlerr").await;
    // Every standard GraphQL client reads `errors` from the body; a 4xx for an
    // unknown field would break them.
    let (status, body) = send(
        &state,
        post_json("gqlerr", json!({ "query": "{ persons { nope } }" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(json["data"].is_null(), "{body}");
    assert!(
        json["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("nope"),
        "{body}"
    );
}

#[tokio::test]
async fn without_a_default_context_ids_stay_full_iris() {
    // Nothing shortens the IRIs, so `id` round-trips as the absolute form. Type
    // and field names still fall back to the IRI's last segment.
    let (_tmp, state) = server_state().await;
    let (status, _) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/create")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "ledger": "gqlnoctx" }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/insert/gqlnoctx")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "@graph": [{
                        "@id": "http://example.org/alice",
                        "@type": "http://example.org/Person",
                        "http://example.org/name": "Alice"
                    }]
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert!(status.is_success(), "insert: {body}");

    let (status, body) = send(
        &state,
        post_json("gqlnoctx", json!({ "query": "{ persons { id name } }" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        json["data"],
        json!({ "persons": [{ "id": "http://example.org/alice", "name": ["Alice"] }] }),
        "{body}"
    );
}

#[tokio::test]
async fn a_body_without_a_query_is_a_bad_request() {
    let (_tmp, state) = seeded("gqlbad").await;
    let (status, _) = send(&state, post_json("gqlbad", json!({ "variables": {} }))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// =============================================================================
// Mutations
// =============================================================================

/// A ledger whose `graphql:Schema` turns mutations on.
async fn writable(ledger: &str) -> (TempDir, Arc<AppState>) {
    let (tmp, state) = server_state().await;
    let (status, body) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/create")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "ledger": ledger }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");

    let context = json!({
        "ex": "http://example.org/",
        "sh": "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "graphql": "http://datashapes.org/graphql#",
        "f": "https://ns.flur.ee/db#"
    });
    let (status, body) = send(
        &state,
        Request::builder()
            .method("PUT")
            .uri(format!("/v1/fluree/context/{ledger}"))
            .header("content-type", "application/json")
            .body(Body::from(json!({ "@context": context }).to_string()))
            .unwrap(),
    )
    .await;
    assert!(status.is_success(), "set context: {body}");

    let (status, body) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/insert/{ledger}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "@context": context,
                    "@graph": [
                        {
                            "@id": "ex:PersonShape",
                            "@type": "sh:NodeShape",
                            "sh:targetClass": { "@id": "ex:Person" },
                            "sh:property": [{
                                "sh:path": { "@id": "ex:name" },
                                "sh:datatype": { "@id": "xsd:string" },
                                "sh:minCount": 1,
                                "sh:maxCount": 1
                            }]
                        },
                        {
                            "@id": "ex:Api",
                            "@type": "graphql:Schema",
                            "graphql:publicShape": { "@id": "ex:PersonShape" },
                            "f:graphqlEnableMutations": true,
                            "f:graphqlIriBase": "http://example.org/"
                        }
                    ]
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert!(status.is_success(), "insert shapes: {body}");
    (tmp, state)
}

#[tokio::test]
async fn a_mutation_over_http_writes_and_reads_back() {
    let (_tmp, state) = writable("gqlmut").await;

    let (status, body) = send(
        &state,
        post_json(
            "gqlmut",
            json!({
                "query": "mutation { create_Person(input: { id: \"ex:alice\", name: \"Alice\" }) { id name } }"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(json.get("errors").is_none(), "{body}");
    assert_eq!(
        json["data"]["create_Person"],
        json!({ "id": "ex:alice", "name": "Alice" })
    );

    // The write committed: an ordinary query sees it.
    let (status, body) = send(
        &state,
        post_json("gqlmut", json!({ "query": "{ persons { id name } }" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        json["data"],
        json!({ "persons": [{ "id": "ex:alice", "name": "Alice" }] })
    );
}

#[tokio::test]
async fn the_schema_endpoint_shows_the_write_surface() {
    let (_tmp, state) = writable("gqlmutsdl").await;
    let (status, body) = send(
        &state,
        Request::builder()
            .method("GET")
            .uri("/v1/fluree/graphql-schema/gqlmutsdl")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("type Mutation"), "{body}");
    assert!(body.contains("create_Person"), "{body}");
    assert!(body.contains("input PersonInput"), "{body}");
}

#[tokio::test]
async fn a_rejected_mutation_is_a_200_with_errors_and_writes_nothing() {
    let (_tmp, state) = writable("gqlmutreject").await;

    // `ex:name` is `sh:minCount 1`; the same validation any other write faces.
    let (status, body) = send(
        &state,
        post_json(
            "gqlmutreject",
            json!({ "query": "mutation { create_Person(input: { id: \"ex:bob\" }) { id } }" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(json["errors"].is_array(), "{body}");

    let (_, body) = send(
        &state,
        post_json("gqlmutreject", json!({ "query": "{ persons_count }" })),
    )
    .await;
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["data"], json!({ "persons_count": 0 }));
}

#[tokio::test]
async fn a_ledger_without_mutations_enabled_refuses_them_over_http() {
    let (_tmp, state) = seeded("gqlnomut").await;
    let (status, body) = send(
        &state,
        post_json(
            "gqlnomut",
            json!({ "query": "mutation { create_Person(input: { name: \"x\" }) { id } }" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(
        json["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("not configured for mutations"),
        "{body}"
    );
}

#[tokio::test]
async fn explain_is_returned_when_asked_for() {
    let (_tmp, state) = seeded("gqlexplain").await;

    // Both request forms: the query parameter and the envelope's `extensions`.
    for request in [
        post_json(
            "gqlexplain",
            json!({ "query": "{ persons { id } }", "extensions": { "explain": true } }),
        ),
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/graphql/gqlexplain?explain=true")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "query": "{ persons { id } }" }).to_string(),
            ))
            .unwrap(),
    ] {
        let (status, body) = send(&state, request).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let json: Value = serde_json::from_str(&body).unwrap();
        let explain = &json["extensions"]["explain"];
        assert_eq!(explain["tier"], "inferred", "{body}");
        assert_eq!(explain["fields"][0]["field"], "persons", "{body}");
        assert!(explain["fields"][0]["query"].is_object(), "{body}");
    }

    // Absent by default.
    let (_, body) = send(
        &state,
        post_json("gqlexplain", json!({ "query": "{ persons { id } }" })),
    )
    .await;
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(json.get("extensions").is_none(), "{body}");
}

// ── Resource bounds ──────────────────────────────────────────────────────────

/// A server with tight GraphQL limits, over the storage `seeded` just wrote.
///
/// Rebuilding state rather than seeding through the limited server: the seed
/// itself is a transaction, and a depth limit low enough to be interesting
/// would refuse the queries the fixture makes.
async fn seeded_with_limits(
    ledger: &str,
    max_depth: usize,
    max_complexity: usize,
) -> (TempDir, Arc<AppState>) {
    let (tmp, _) = seeded(ledger).await;
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        graphql_max_depth: max_depth,
        graphql_max_complexity: max_complexity,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"));
    (tmp, state)
}

#[tokio::test]
async fn a_document_past_the_configured_depth_is_refused() {
    let (_tmp, state) = seeded_with_limits("gqlhttp-depth", 3, 1000).await;

    let deep = "{ persons { knows { knows { knows { id } } } } }";
    let (status, body) = send(&state, post_json("gqlhttp-depth", json!({ "query": deep }))).await;

    // A GraphQL error is a 200 with `errors`, like every other refusal here.
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(
        json.get("errors").is_some(),
        "a document past graphql_max_depth must be refused: {body}"
    );
}

#[tokio::test]
async fn an_alias_fan_out_past_the_configured_complexity_is_refused() {
    let (_tmp, state) = seeded_with_limits("gqlhttp-complexity", 15, 20).await;

    let document = format!(
        "{{ {} }}",
        (0..50)
            .map(|i| format!("a{i}: persons {{ id name }}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let (status, body) = send(
        &state,
        post_json("gqlhttp-complexity", json!({ "query": document })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(
        json.get("errors").is_some(),
        "a 50-alias document must not run under a 20-field budget: {body}"
    );
    assert!(
        json["data"].get("a0").is_none(),
        "the document must be refused before any field resolves: {body}"
    );
}

/// The limits are a ceiling, not a filter: an ordinary document is unaffected.
#[tokio::test]
async fn the_default_limits_do_not_touch_an_ordinary_document() {
    let (_tmp, state) = seeded("gqlhttp-limits-ok").await;
    let (status, body) = send(
        &state,
        post_json(
            "gqlhttp-limits-ok",
            json!({ "query": "{ persons { id name knows { id name } } }" }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert!(json.get("errors").is_none(), "{body}");
}
