//! W3C SPARQL 1.1 Graph Store HTTP Protocol: `/v1/fluree/data/<ledger...>`.
//!
//! The graph is named by query parameter (the protocol's indirect
//! identification): `?graph=<iri>` for a named graph, a bare `?default` for
//! the default graph.
//!
//! | Method | Does | Built on |
//! |---|---|---|
//! | `GET` / `HEAD` | return the graph | a CONSTRUCT (`HEAD`: an ASK) through the query route, so auth and read policy apply |
//! | `PUT` | replace the graph | graph sync: one commit carrying only the delta |
//! | `POST` | add triples to the graph | graph insert |
//! | `DELETE` | remove the graph | `DROP GRAPH` / `CLEAR DEFAULT` |
//!
//! A named graph exists while it holds a triple (Fluree has no empty named
//! graph), and for a read, while it holds one the caller may see; the default
//! graph always exists. `PUT` and `POST` answer
//! `201 Created` when they bring a graph into existence and `200 OK`
//! otherwise, with the usual transaction response as the body.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{
    negotiate_graph_format, FlureeHeaders, GraphFormat, MaybeCredential, MaybeDataBearer,
};
use crate::routes::query::SparqlParams;
use crate::routes::transact::{
    effective_author, enforce_write_access, execute_transaction, execute_turtle_transaction,
    forward_write_request, graph_target, submit_sparql_update, GraphWrite, TurtleOp,
};
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{FromRequestParts, Path, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use fluree_db_api::{GraphSel, TxnType};
use std::sync::Arc;

/// Body formats `PUT` and `POST` accept.
const JSON_LD_TYPES: [&str; 2] = ["application/ld+json", "application/json"];

/// `GET /data/<ledger...>?graph=<iri>` (or `?default`). `HEAD` shares the
/// handler but answers from an `ASK`, without building the graph.
pub async fn get(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    request: Request,
) -> Response {
    read_graph(state, ledger, request).await.into_response()
}

/// `PUT /data/<ledger...>?graph=<iri>`: replace the graph with the body.
pub async fn put(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    write_graph(state, ledger, bearer, request, GraphWrite::Sync)
        .await
        .into_response()
}

/// `POST /data/<ledger...>?graph=<iri>`: add the body's triples to the graph.
pub async fn post(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    write_graph(state, ledger, bearer, request, GraphWrite::Insert)
        .await
        .into_response()
}

/// `DELETE /data/<ledger...>?graph=<iri>`: remove the graph.
pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    delete_graph(state, ledger, bearer, request)
        .await
        .into_response()
}

/// The graph a request names, validated: a named graph must be an absolute
/// IRI and not one of the ledger's system graphs.
fn request_graph(request: &Request, ledger: &str) -> Result<GraphSel> {
    let pairs = crate::routes::sparql_protocol::decode_pairs(request.uri().query().unwrap_or(""))?;
    let mut graph = None;
    let mut default_graph = false;
    for (key, value) in pairs {
        match key.as_str() {
            "graph" if graph.is_some() => {
                return Err(ServerError::bad_request(
                    "the `graph` parameter may appear only once",
                ))
            }
            "graph" => graph = Some(value),
            "default" if value.is_empty() || value == "true" => default_graph = true,
            "default" => {
                return Err(ServerError::bad_request(
                    "`default` takes no value: `?default` names the default graph",
                ))
            }
            _ => {}
        }
    }
    let graph = graph_target(graph, default_graph, true)?;
    if let GraphSel::Graph(iri) = &graph {
        use fluree_db_core::graph_registry::{
            config_graph_iri, txn_meta_graph_iri, validate_absolute_graph_iri,
        };
        validate_absolute_graph_iri(iri).map_err(ServerError::bad_request)?;
        // An unparseable ledger id fails later as a missing ledger.
        let ledger_id =
            fluree_db_core::normalize_ledger_id(ledger).unwrap_or_else(|_| ledger.to_string());
        if *iri == txn_meta_graph_iri(&ledger_id) || *iri == config_graph_iri(&ledger_id) {
            return Err(ServerError::bad_request(format!(
                "<{iri}> is a system graph; the Graph Store Protocol reads and writes user graphs"
            )));
        }
    }
    Ok(graph)
}

async fn write_graph(
    state: Arc<AppState>,
    ledger: String,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
    write: GraphWrite,
) -> Result<Response> {
    if state.config.server_role == ServerRole::Peer {
        return Ok(forward_write_request(&state, request).await);
    }
    let graph = request_graph(&request, &ledger)?;
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let credential = MaybeCredential::extract(request).await?;
    let headers = crate::routes::policy_auth::bind_authorization(
        &state,
        headers,
        bearer.as_ref(),
        credential.did(),
    )?;
    enforce_write_access(&state, &ledger, bearer.as_ref(), &credential)?;
    let author = effective_author(&credential, bearer.as_ref());

    // An empty body is an empty graph whatever its declared type: `PUT`
    // clears the graph, as the protocol specifies, and `POST` has nothing to
    // add, which graph insert refuses.
    let empty_body = credential.body.iter().all(u8::is_ascii_whitespace);
    let is_rdf = credential.is_turtle_or_trig() || empty_body;
    if !is_rdf && !has_content_type(&credential, &JSON_LD_TYPES) {
        return Err(ServerError::unsupported_media_type(
            "the body must be text/turtle, application/n-triples, application/trig, \
             application/ld+json or application/json",
        ));
    }

    let existed = state.fluree.graph_exists(&ledger, &graph).await?;
    let response = if is_rdf {
        let text = if empty_body {
            String::new()
        } else {
            credential.body_string()?
        };
        let op = TurtleOp::Graph {
            graph: &graph,
            write,
            allow_empty: true,
        };
        execute_turtle_transaction(
            &state,
            &ledger,
            op,
            &text,
            &credential,
            &headers,
            author.as_deref(),
        )
        .await?
    } else {
        let body = credential.body_json()?;
        execute_transaction(
            &state,
            &ledger,
            TxnType::Insert,
            Some((&graph, write)),
            body,
            &credential,
            author.as_deref(),
            &headers,
        )
        .await?
    };

    let created = !existed && state.fluree.graph_exists(&ledger, &graph).await?;
    Ok(with_status(response, created))
}

async fn delete_graph(
    state: Arc<AppState>,
    ledger: String,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
) -> Result<Response> {
    if state.config.server_role == ServerRole::Peer {
        return Ok(forward_write_request(&state, request).await);
    }
    let graph = request_graph(&request, &ledger)?;
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let credential = MaybeCredential::extract(request).await?;
    let headers = crate::routes::policy_auth::bind_authorization(
        &state,
        headers,
        bearer.as_ref(),
        credential.did(),
    )?;
    enforce_write_access(&state, &ledger, bearer.as_ref(), &credential)?;
    if !state.fluree.graph_exists(&ledger, &graph).await? {
        return Err(missing_graph(&graph));
    }
    // The IRI was validated above, so it cannot close the `<…>`.
    let sparql = match &graph {
        GraphSel::Graph(iri) => format!("DROP GRAPH <{iri}>"),
        GraphSel::Default => "CLEAR DEFAULT".to_string(),
    };
    submit_sparql_update(
        &state,
        &ledger,
        sparql,
        &headers,
        &credential,
        &tracing::Span::current(),
    )
    .await
}

async fn read_graph(state: Arc<AppState>, ledger: String, request: Request) -> Result<Response> {
    let graph = request_graph(&request, &ledger)?;
    let accept = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let format =
        negotiate_graph_format(accept.as_deref(), GraphFormat::SINGLE_GRAPH).ok_or_else(|| {
            ServerError::not_acceptable(
                "a graph is available as application/ld+json, text/turtle, \
             application/n-triples or application/rdf+xml",
            )
        })?;
    let head = request.method() == Method::HEAD;
    let (parts, _) = request.into_parts();

    // Existence is asked through the query route, so authentication and read
    // policy answer first: a graph with no triple the caller may see is
    // indistinguishable from an absent one. `HEAD` stops here rather than
    // serializing the graph. A plain `GET` of the default graph needs no probe;
    // the CONSTRUCT is authorized the same way.
    if head || matches!(graph, GraphSel::Graph(_)) {
        let probe = match &graph {
            GraphSel::Graph(iri) => format!("ASK {{ GRAPH <{iri}> {{ ?s ?p ?o }} }}"),
            GraphSel::Default => "ASK { ?s ?p ?o }".to_string(),
        };
        let response = query_as_caller(
            &state,
            &ledger,
            parts.clone(),
            probe,
            "application/sparql-results+json",
        )
        .await?;
        if !response.status().is_success() {
            return Ok(response);
        }
        if !ask_answer(response).await? && matches!(graph, GraphSel::Graph(_)) {
            return Err(missing_graph(&graph));
        }
        if head {
            return Ok(([(header::CONTENT_TYPE, format.media_type())], ()).into_response());
        }
    }

    // Edge annotations come back with their triples, so a GET of an annotated
    // graph PUT back unchanged commits nothing. The lookup costs about a
    // quarter of the query's time, so a ledger that has never held an
    // annotation skips it. That flag only ever turns on: if it is still off
    // after the query ran, the snapshot the query read had no annotations;
    // if a commit or refresh turned it on meanwhile, run again with the
    // lookup. The first check can run before the caller is authenticated, so
    // a failure only drops the lookup; the query route answers for the ledger.
    let annotated = state.fluree.has_annotations(&ledger).await.unwrap_or(false);
    let response =
        construct_graph(&state, &ledger, parts.clone(), &graph, format, annotated).await?;
    if !annotated && response.status().is_success() && state.fluree.has_annotations(&ledger).await?
    {
        return construct_graph(&state, &ledger, parts, &graph, format, true).await;
    }
    Ok(response)
}

/// `CONSTRUCT` the graph through the query route. With `annotated`, each
/// annotated edge also brings its reifier (`?s ?p ?o ~ ?r`). The annotations
/// come from a `UNION` branch rooted at `rdf:reifies`, which costs a lookup
/// per annotation; an `OPTIONAL` probing every triple is several times
/// slower inside a named graph.
async fn construct_graph(
    state: &Arc<AppState>,
    ledger: &str,
    parts: axum::http::request::Parts,
    graph: &GraphSel,
    format: GraphFormat,
    annotated: bool,
) -> Result<Response> {
    let (template, pattern) = if annotated {
        (
            "?s ?p ?o ~ ?r",
            "{ ?s ?p ?o } UNION \
             { ?r <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> <<( ?s ?p ?o )>> }",
        )
    } else {
        ("?s ?p ?o", "?s ?p ?o")
    };
    // The IRI was validated by `request_graph`, so it cannot close the `<…>`.
    let sparql = match graph {
        GraphSel::Graph(iri) => {
            format!("CONSTRUCT {{ {template} }} WHERE {{ GRAPH <{iri}> {{ {pattern} }} }}")
        }
        GraphSel::Default => format!("CONSTRUCT {{ {template} }} WHERE {{ {pattern} }}"),
    };
    query_as_caller(state, ledger, parts, sparql, format.media_type()).await
}

/// Run `sparql` through the query route as the caller would: same auth
/// headers and credential, so the same read policy.
async fn query_as_caller(
    state: &Arc<AppState>,
    ledger: &str,
    mut parts: axum::http::request::Parts,
    sparql: String,
    accept: &'static str,
) -> Result<Response> {
    parts.method = Method::POST;
    parts.uri = axum::http::Uri::from_static("/");
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/sparql-query"),
    );
    parts
        .headers
        .insert(header::ACCEPT, HeaderValue::from_static(accept));
    parts.headers.remove(header::CONTENT_LENGTH);
    let params = SparqlParams::from_request_parts(&mut parts, state).await?;
    let headers = FlureeHeaders::from_request_parts(&mut parts, state).await?;
    let bearer = MaybeDataBearer::from_request_parts(&mut parts, state).await?;
    let credential =
        MaybeCredential::extract(Request::from_parts(parts, Body::from(sparql))).await?;
    crate::routes::query::query_ledger(
        State(state.clone()),
        Path(ledger.to_string()),
        params,
        headers,
        bearer,
        credential,
    )
    .await
}

/// The boolean of a SPARQL JSON `ASK` result.
async fn ask_answer(response: Response) -> Result<bool> {
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .map_err(|e| ServerError::internal(format!("reading ASK result: {e}")))?;
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| v.get("boolean").and_then(serde_json::Value::as_bool))
        .ok_or_else(|| ServerError::internal("ASK result has no boolean"))
}

fn missing_graph(graph: &GraphSel) -> ServerError {
    match graph {
        GraphSel::Graph(iri) => ServerError::not_found(format!("graph <{iri}> does not exist")),
        GraphSel::Default => ServerError::not_found("the default graph does not exist"),
    }
}

fn has_content_type(credential: &MaybeCredential, types: &[&str]) -> bool {
    credential
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .is_some_and(|ct| types.iter().any(|t| ct.trim().eq_ignore_ascii_case(t)))
}

fn with_status(mut response: Response, created: bool) -> Response {
    if created && response.status() == StatusCode::OK {
        *response.status_mut() = StatusCode::CREATED;
    }
    response
}
