//! W3C SPARQL 1.1 Graph Store HTTP Protocol: `/v1/fluree/data/<ledger...>`.
//!
//! The graph is named by query parameter (the protocol's indirect
//! identification): `?graph=<iri>` for a named graph, a bare `?default` for
//! the default graph.
//!
//! | Method | Does | Built on |
//! |---|---|---|
//! | `GET` / `HEAD` | return the graph | a CONSTRUCT through the query route, so read policy applies |
//! | `PUT` | replace the graph | graph sync: one commit carrying only the delta |
//! | `POST` | add triples to the graph | graph insert |
//! | `DELETE` | remove the graph | `DROP GRAPH` / `CLEAR DEFAULT` |
//!
//! A named graph exists while it holds a triple (Fluree has no empty named
//! graph); the default graph always exists. `PUT` and `POST` answer
//! `201 Created` when they bring a graph into existence and `200 OK`
//! otherwise, with the usual transaction response as the body.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeCredential, MaybeDataBearer};
use crate::routes::query::SparqlParams;
use crate::routes::transact::{
    effective_author, enforce_write_access, execute_transaction, execute_turtle_transaction,
    forward_write_request, graph_target, submit_sparql_update, GraphWrite, TurtleOp,
};
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{FromRequestParts, Path, Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use fluree_db_api::{GraphSel, TxnType};
use std::sync::Arc;

/// Body formats `PUT` and `POST` accept.
const JSON_LD_TYPES: [&str; 2] = ["application/ld+json", "application/json"];

/// `GET /data/<ledger...>?graph=<iri>` (or `?default`); `HEAD` is answered
/// from the same handler with the body dropped.
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

/// RDF formats `GET` can return. Turtle and N-Triples are not here yet:
/// CONSTRUCT has no Turtle serializer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphFormat {
    JsonLd,
    RdfXml,
}

impl GraphFormat {
    fn media_type(self) -> &'static str {
        match self {
            GraphFormat::JsonLd => "application/ld+json",
            GraphFormat::RdfXml => "application/rdf+xml",
        }
    }
}

/// The format to answer `GET` in: the highest-`q` media range in `Accept`
/// that a supported format satisfies. No `Accept` means JSON-LD.
fn negotiate(accept: Option<&str>) -> Result<GraphFormat> {
    let Some(accept) = accept.filter(|a| !a.trim().is_empty()) else {
        return Ok(GraphFormat::JsonLd);
    };
    let mut ranges: Vec<(f32, &str)> = accept
        .split(',')
        .filter_map(|range| {
            let mut parts = range.split(';').map(str::trim);
            let media = parts.next().filter(|m| !m.is_empty())?;
            let q = parts
                .find_map(|p| p.strip_prefix("q="))
                .map_or(1.0, |q| q.parse().unwrap_or(0.0));
            Some((q, media))
        })
        .filter(|(q, _)| *q > 0.0)
        .collect();
    // Stable: equal weights keep the client's order.
    ranges.sort_by(|a, b| b.0.total_cmp(&a.0));
    for (_, media) in ranges {
        match media.to_ascii_lowercase().as_str() {
            "application/ld+json" | "application/json" | "*/*" | "application/*" => {
                return Ok(GraphFormat::JsonLd)
            }
            "application/rdf+xml" => return Ok(GraphFormat::RdfXml),
            _ => {}
        }
    }
    Err(ServerError::not_acceptable(
        "a graph is available as application/ld+json or application/rdf+xml; \
         Turtle and N-Triples output is not supported yet",
    ))
}

async fn read_graph(state: Arc<AppState>, ledger: String, request: Request) -> Result<Response> {
    let graph = request_graph(&request, &ledger)?;
    let accept = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let format = negotiate(accept.as_deref())?;
    if !state.fluree.graph_exists(&ledger, &graph).await? {
        return Err(missing_graph(&graph));
    }

    let sparql = match &graph {
        GraphSel::Graph(iri) => {
            format!("CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{iri}> {{ ?s ?p ?o }} }}")
        }
        GraphSel::Default => "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }".to_string(),
    };

    // Run the CONSTRUCT as the query route would a SPARQL request from the
    // same caller: same auth headers and credential, so the same read policy.
    let (mut parts, _) = request.into_parts();
    parts.method = axum::http::Method::POST;
    parts.uri = axum::http::Uri::from_static("/");
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/sparql-query"),
    );
    let query_accept = match format {
        GraphFormat::JsonLd => "application/json",
        GraphFormat::RdfXml => "application/rdf+xml",
    };
    parts
        .headers
        .insert(header::ACCEPT, HeaderValue::from_static(query_accept));
    parts.headers.remove(header::CONTENT_LENGTH);
    let params = SparqlParams::from_request_parts(&mut parts, &state).await?;
    let headers = FlureeHeaders::from_request_parts(&mut parts, &state).await?;
    let bearer = MaybeDataBearer::from_request_parts(&mut parts, &state).await?;
    let credential =
        MaybeCredential::extract(Request::from_parts(parts, Body::from(sparql))).await?;
    let mut response = crate::routes::query::query_ledger(
        State(state),
        Path(ledger),
        params,
        headers,
        bearer,
        credential,
    )
    .await?;
    if response.status().is_success() {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(format.media_type()),
        );
    }
    Ok(response)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiation_follows_q_and_falls_back_to_json_ld() {
        assert_eq!(negotiate(None).unwrap(), GraphFormat::JsonLd);
        assert_eq!(negotiate(Some("*/*")).unwrap(), GraphFormat::JsonLd);
        assert_eq!(
            negotiate(Some("text/turtle, application/rdf+xml;q=0.9, */*;q=0.1")).unwrap(),
            GraphFormat::RdfXml
        );
        assert_eq!(
            negotiate(Some("application/rdf+xml;q=0.5, application/ld+json")).unwrap(),
            GraphFormat::JsonLd
        );
        assert!(negotiate(Some("text/turtle, application/n-triples")).is_err());
        assert!(negotiate(Some("application/rdf+xml;q=0")).is_err());
    }
}
