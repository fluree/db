//! SSE Event Notification Endpoint
//!
//! Publishes nameservice record changes to external query peers.
//! Contract is idempotent (full record) and monotonic (clients react based on `commit_t`/`index_t`).
//!
//! ## Endpoint
//! ```text
//! GET /fluree/events?ledger=books:main&ledger=people:main&graph-source=my-gs:main
//! GET /fluree/events?all=true
//! ```
//!
//! ## Query Parameter Precedence
//! - `all=true` overrides any `ledger=` or `graph-source=` params
//! - Otherwise, filter to explicitly provided aliases
//!
//! ## Event Types
//! - `ns-record` - Record published/updated (ledger or graph source)
//! - `ns-retracted` - Record retracted/deleted

use axum::{
    extract::{RawQuery, State},
    response::sse::{Event, KeepAlive, Sse},
};
use chrono::Utc;
use fluree_db_nameservice::{
    GraphSourceRecord, NameServiceEvent, NameServiceLookup, NsRecord, SubscriptionScope,
};
use fluree_sse::{SSE_KIND_GRAPH_SOURCE, SSE_KIND_LEDGER};
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::broadcast;

use crate::config::{EventsAuthConfig, EventsAuthMode, ServerRole};
use crate::error::ServerError;
use crate::extract::{EventsPrincipal, MaybeBearer};
use crate::state::AppState;

/// Query parameters for the events endpoint
#[derive(Debug, Clone, Default, Deserialize)]
pub struct EventsQuery {
    /// Subscribe to all ledgers and graph sources
    #[serde(default)]
    pub all: bool,

    /// Specific ledger aliases to subscribe to
    #[serde(default, rename = "ledger")]
    pub ledgers: Vec<String>,

    /// Specific graph source aliases to subscribe to
    #[serde(default, rename = "graph-source")]
    pub graph_sources: Vec<String>,
}

impl EventsQuery {
    /// Parse this endpoint's query string.
    ///
    /// Hand-rolled rather than `Query<EventsQuery>`, because the subscription
    /// params are REPEATED keys (`?ledger=a&ledger=b`) and
    /// `serde_urlencoded` — what `axum::extract::Query` deserializes with —
    /// cannot build a `Vec` from repeated keys. It rejects the whole request
    /// with `invalid type: string "…", expected a sequence`, so *every*
    /// `?ledger=` form answered 400, a single alias included; only
    /// `?all=true` ever worked. The `Deserialize` derive stays for the
    /// in-process constructions and tests.
    ///
    /// Unknown keys are ignored (forward compatibility), and so is an empty
    /// `ledger=` / `graph-source=` — one alias among however many were
    /// asked for, and dropping it changes the scope but never erases it.
    ///
    /// `all` is different, and is the one value this parser REJECTS. It is
    /// the whole subscription request, so a value we do not understand can
    /// only be resolved one of two ways: guess `false` and hand back a 200
    /// SSE stream that matches nothing and emits nothing, forever, with no
    /// error for the client to notice — or say so. `serde_urlencoded`, what
    /// this parser replaced, said so (a non-boolean 400d), and the silent
    /// never-emits stream is the exact failure class the repeated-key fix
    /// above exists to remove. `?all` bare is the flag form and means true;
    /// `true`/`1`/`yes`/`on` and `false`/`0`/`no`/`off` are accepted
    /// case-insensitively; anything else, empty included, is a 400.
    pub fn from_query_str(raw: &str) -> Result<Self, ServerError> {
        let mut query = EventsQuery::default();
        for pair in raw.split('&').filter(|p| !p.is_empty()) {
            // `None` distinguishes a bare flag (`?all`) from an explicitly
            // empty value (`?all=`), which are not the same request.
            let (key, raw_value) = match pair.split_once('=') {
                Some((k, v)) => (k, Some(v)),
                None => (pair, None),
            };
            // `+` is a space in application/x-www-form-urlencoded; percent
            // escapes cover everything else (`:` in `books:main` is commonly
            // sent as %3A).
            let value = raw_value.map(|v| {
                urlencoding::decode(&v.replace('+', " "))
                    .map(std::borrow::Cow::into_owned)
                    .unwrap_or_else(|_| v.to_string())
            });
            match key {
                "all" => query.all = parse_all(value.as_deref())?,
                "ledger" => {
                    if let Some(v) = value.filter(|v| !v.is_empty()) {
                        query.ledgers.push(v);
                    }
                }
                "graph-source" => {
                    if let Some(v) = value.filter(|v| !v.is_empty()) {
                        query.graph_sources.push(v);
                    }
                }
                _ => {}
            }
        }
        Ok(query)
    }

    /// Check if this query matches a given resource ID and kind
    #[cfg(test)]
    pub fn matches(&self, resource_id: &str, kind: &str) -> bool {
        if self.all {
            return true;
        }
        match kind {
            SSE_KIND_LEDGER => self.ledgers.iter().any(|l| l == resource_id),
            SSE_KIND_GRAPH_SOURCE => self.graph_sources.iter().any(|v| v == resource_id),
            _ => false,
        }
    }
}

/// `?all` → true (flag form); `?all=<boolean>` → that boolean; anything
/// else → 400 rather than a stream that silently never emits.
fn parse_all(value: Option<&str>) -> Result<bool, ServerError> {
    let Some(value) = value else {
        return Ok(true);
    };
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(ServerError::BadRequest(format!(
            "events: `all` must be a boolean (true/false, 1/0, yes/no, on/off); got {other:?}. \
             Subscribing to specific resources uses `?ledger=` / `?graph-source=` instead."
        ))),
    }
}

/// SSE event data payload for ns-record events
#[derive(Debug, Serialize)]
struct NsRecordData {
    action: &'static str,
    kind: &'static str,
    resource_id: String,
    record: serde_json::Value,
    emitted_at: String,
}

/// SSE event data payload for ns-retracted events
#[derive(Debug, Serialize)]
struct NsRetractedData {
    action: &'static str,
    kind: &'static str,
    resource_id: String,
    emitted_at: String,
}

/// Compute the SSE event ID for a ledger record
fn ledger_event_id(resource_id: &str, record: &NsRecord) -> String {
    format!(
        "ledger:{}:{}:{}",
        resource_id, record.commit_t, record.index_t
    )
}

/// Compute the SSE event ID for a graph source record
fn graph_source_event_id(resource_id: &str, record: &GraphSourceRecord) -> String {
    // Use index_t + 8-char truncated SHA-256 of config
    let config_hash = sha256_short(&record.config);
    format!(
        "graph-source:{}:{}:{}",
        resource_id, record.index_t, config_hash
    )
}

/// Compute the SSE event ID for a retraction
fn retracted_event_id(kind: &str, resource_id: &str) -> String {
    // Include timestamp for ordering across delete/recreate cycles
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{kind}:{resource_id}:retracted:{timestamp_ms}")
}

/// Compute 8-char truncated SHA-256 hash
fn sha256_short(data: &str) -> String {
    let hash = Sha256::digest(data.as_bytes());
    hex::encode(&hash[..4]) // 4 bytes = 8 hex chars
}

/// Get current ISO-8601 timestamp
fn now_iso8601() -> String {
    Utc::now().to_rfc3339()
}

/// Convert a ledger NsRecord to an SSE Event
fn ledger_to_sse_event(record: &NsRecord) -> Event {
    let ledger_id = record.ledger_id.clone();
    let event_id = ledger_event_id(&ledger_id, record);

    let commit_head_id = record
        .commit_head_id
        .as_ref()
        .map(std::string::ToString::to_string);
    let index_head_id = record
        .index_head_id
        .as_ref()
        .map(std::string::ToString::to_string);

    let data = NsRecordData {
        action: "ns-record",
        kind: SSE_KIND_LEDGER,
        resource_id: ledger_id.clone(),
        record: serde_json::json!({
            "ledger_id": ledger_id,
            "branch": record.branch,
            // Identity (storage-agnostic): preferred for sync logic.
            "commit_head_id": commit_head_id,
            "commit_t": record.commit_t,
            "index_head_id": index_head_id,
            "index_t": record.index_t,
            "retracted": record.retracted,
            "source_branch": record.source_branch,
            "branches": record.branches,
        }),
        emitted_at: now_iso8601(),
    };

    Event::default()
        .event("ns-record")
        .id(event_id)
        .json_data(data)
        .unwrap_or_else(|_| Event::default().comment("serialization error"))
}

/// Convert a graph source record to an SSE Event
fn graph_source_to_sse_event(record: &GraphSourceRecord) -> Event {
    let graph_source_id = record.graph_source_id.clone();
    let event_id = graph_source_event_id(&graph_source_id, record);

    let index_id = record
        .index_id
        .as_ref()
        .map(std::string::ToString::to_string);

    let data = NsRecordData {
        action: "ns-record",
        kind: SSE_KIND_GRAPH_SOURCE,
        resource_id: graph_source_id.clone(),
        record: serde_json::json!({
            "graph_source_id": graph_source_id,
            "name": record.name,
            "branch": record.branch,
            "source_type": record.source_type.to_type_string(),
            // Redact any auth/credential leaves: this graph-source record streams
            // to every SSE subscriber, so a literal OAuth2 secret / bearer token
            // in the config must never reach a client (no-op for secret-free
            // configs, which stay byte-identical).
            "config": fluree_db_api::ledger_info::redact_graph_source_config(&record.config),
            "dependencies": record.dependencies,
            "index_id": index_id,
            "index_t": record.index_t,
            "retracted": record.retracted,
        }),
        emitted_at: now_iso8601(),
    };

    Event::default()
        .event("ns-record")
        .id(event_id)
        .json_data(data)
        .unwrap_or_else(|_| Event::default().comment("serialization error"))
}

/// Create a retracted SSE Event
fn retracted_sse_event(kind: &'static str, resource_id: &str) -> Event {
    let event_id = retracted_event_id(kind, resource_id);

    let data = NsRetractedData {
        action: "ns-retracted",
        kind,
        resource_id: resource_id.to_string(),
        emitted_at: now_iso8601(),
    };

    Event::default()
        .event("ns-retracted")
        .id(event_id)
        .json_data(data)
        .unwrap_or_else(|_| Event::default().comment("serialization error"))
}

/// Build the initial snapshot of records on connection
async fn build_initial_snapshot<N>(ns: &N, params: &EventsQuery) -> Vec<Event>
where
    N: NameServiceLookup,
{
    let mut events = Vec::new();

    if params.all {
        // All ledger records (sorted by name)
        if let Ok(mut records) = ns.all_records().await {
            records.sort_by(|a, b| a.ledger_id.cmp(&b.ledger_id));
            for r in records {
                if !r.retracted {
                    events.push(ledger_to_sse_event(&r));
                }
            }
        }
        // All graph source records (sorted by graph_source_id)
        if let Ok(mut records) = ns.all_graph_source_records().await {
            records.sort_by_key(|a| a.graph_source_id.clone());
            for r in records {
                if !r.retracted {
                    events.push(graph_source_to_sse_event(&r));
                }
            }
        }
    } else {
        // Requested ledgers (skip missing, sorted, deduped)
        let mut ledger_aliases: Vec<_> = params.ledgers.iter().collect();
        ledger_aliases.sort();
        ledger_aliases.dedup();
        for alias in ledger_aliases {
            if let Ok(Some(r)) = ns.lookup(alias).await {
                if !r.retracted {
                    events.push(ledger_to_sse_event(&r));
                }
            }
        }

        // Requested graph sources (skip missing, sorted, deduped)
        let mut graph_source_ides: Vec<_> = params.graph_sources.iter().collect();
        graph_source_ides.sort();
        graph_source_ides.dedup();
        for alias in graph_source_ides {
            if let Ok(Some(r)) = ns.lookup_graph_source(alias).await {
                if !r.retracted {
                    events.push(graph_source_to_sse_event(&r));
                }
            }
        }
    }

    events
}

/// Extract the resource ID (ledger alias or graph source alias) from a NameServiceEvent
fn event_resource_id(event: &NameServiceEvent) -> &str {
    match event {
        NameServiceEvent::LedgerCommitPublished { ledger_id, .. } => ledger_id,
        NameServiceEvent::LedgerIndexPublished { ledger_id, .. } => ledger_id,
        NameServiceEvent::LedgerRetracted { ledger_id } => ledger_id,
        NameServiceEvent::GraphSourceConfigPublished {
            graph_source_id, ..
        } => graph_source_id,
        NameServiceEvent::GraphSourceIndexPublished {
            graph_source_id, ..
        } => graph_source_id,
        NameServiceEvent::GraphSourceRetracted { graph_source_id } => graph_source_id,
    }
}

/// Get the kind (ledger/graph-source) from a NameServiceEvent
fn event_kind(event: &NameServiceEvent) -> &'static str {
    match event {
        NameServiceEvent::LedgerCommitPublished { .. }
        | NameServiceEvent::LedgerIndexPublished { .. }
        | NameServiceEvent::LedgerRetracted { .. } => SSE_KIND_LEDGER,
        NameServiceEvent::GraphSourceConfigPublished { .. }
        | NameServiceEvent::GraphSourceIndexPublished { .. }
        | NameServiceEvent::GraphSourceRetracted { .. } => SSE_KIND_GRAPH_SOURCE,
    }
}

/// Transform a nameservice event to an SSE Event, fetching the current record
async fn transform_event<N>(ns: &N, event: NameServiceEvent) -> Option<Event>
where
    N: NameServiceLookup,
{
    let resource_id = event_resource_id(&event).to_string();

    match event {
        NameServiceEvent::LedgerCommitPublished { .. }
        | NameServiceEvent::LedgerIndexPublished { .. } => {
            let record = ns.lookup(&resource_id).await.ok()??;
            Some(ledger_to_sse_event(&record))
        }
        NameServiceEvent::LedgerRetracted { ledger_id } => {
            Some(retracted_sse_event(SSE_KIND_LEDGER, &ledger_id))
        }
        NameServiceEvent::GraphSourceConfigPublished { .. }
        | NameServiceEvent::GraphSourceIndexPublished { .. } => {
            let record = ns.lookup_graph_source(&resource_id).await.ok()??;
            Some(graph_source_to_sse_event(&record))
        }
        NameServiceEvent::GraphSourceRetracted { graph_source_id } => {
            Some(retracted_sse_event(SSE_KIND_GRAPH_SOURCE, &graph_source_id))
        }
    }
}

/// Authorize the request and compute effective scope.
///
/// In None mode, params are passed through unchanged.
/// In Optional/Required mode with a token, params are filtered to allowed scope.
fn authorize_request(
    config: &EventsAuthConfig,
    principal: Option<&EventsPrincipal>,
    params: &EventsQuery,
) -> Result<EventsQuery, ServerError> {
    match config.mode {
        EventsAuthMode::None => {
            // No auth: pass through unchanged
            Ok(params.clone())
        }
        EventsAuthMode::Required if principal.is_none() => {
            // Should not reach here (extractor handles it)
            Err(ServerError::unauthorized("Bearer token required"))
        }
        EventsAuthMode::Optional | EventsAuthMode::Required => {
            match principal {
                Some(p) => Ok(filter_to_allowed(params, p)),
                None => Ok(params.clone()), // Optional mode, no token
            }
        }
    }
}

/// Filter requested scope to what principal is allowed.
/// Returns sorted, deduped lists for deterministic behavior.
/// Silently removes disallowed items (no 403, no existence leak).
fn filter_to_allowed(params: &EventsQuery, principal: &EventsPrincipal) -> EventsQuery {
    // If allowed_all is true, ledgers/graph_sources lists are irrelevant
    if principal.allowed_all {
        // Token grants full access, pass through
        return params.clone();
    }

    // If request.all but token doesn't allow all, expand to allowed lists
    // This is equivalent to all=false with the token's allowed lists
    let mut ledgers: Vec<String> = if params.all {
        principal.allowed_ledgers.iter().cloned().collect()
    } else {
        params
            .ledgers
            .iter()
            .filter(|l| principal.allowed_ledgers.contains(*l))
            .cloned()
            .collect()
    };

    let mut graph_sources: Vec<String> = if params.all {
        principal.allowed_graph_sources.iter().cloned().collect()
    } else {
        params
            .graph_sources
            .iter()
            .filter(|v| principal.allowed_graph_sources.contains(*v))
            .cloned()
            .collect()
    };

    // Sort and dedup for deterministic snapshots
    ledgers.sort();
    ledgers.dedup();
    graph_sources.sort();
    graph_sources.dedup();

    EventsQuery {
        all: false, // Never pass all=true if token restricts
        ledgers,
        graph_sources,
    }
}

/// SSE events endpoint handler
///
/// Streams nameservice record changes to connected clients.
/// On connect, sends an initial snapshot of all matching records,
/// then streams live updates.
///
/// # Authentication
/// When events authentication is enabled:
/// - `Required` mode: Bearer token must be present and valid
/// - `Optional` mode: Token accepted but not required
/// - `None` mode: Token ignored (default)
pub async fn events(
    State(state): State<Arc<AppState>>,
    RawQuery(raw_query): RawQuery,
    MaybeBearer(principal): MaybeBearer,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ServerError> {
    let params = EventsQuery::from_query_str(raw_query.as_deref().unwrap_or_default())?;
    // Peer mode: return 404 - events endpoint not available
    // Peers subscribe to the transaction server's events endpoint instead
    if state.config.server_role == ServerRole::Peer {
        return Err(ServerError::Api(fluree_db_api::ApiError::NotFound(
            "Events endpoint not available in peer mode".to_string(),
        )));
    }

    // Authorize and compute effective scope
    let effective_params =
        authorize_request(&state.config.events_auth(), principal.as_ref(), &params)?;

    // Log connection (issuer only, never token)
    if let Some(p) = &principal {
        tracing::info!(
            issuer = %p.issuer,
            subject = ?p.subject,
            identity = ?p.identity,
            "SSE connection authorized"
        );
    }

    // Clone nameservice mode for use in async closures
    // Events endpoint is only available in transaction mode (checked above)
    let ns = state.fluree.nameservice_mode().clone();

    // 1. SUBSCRIBE FIRST (events during snapshot queue in receiver)
    // This ensures no gap between snapshot and live events
    let subscription = state.fluree.event_bus().subscribe(SubscriptionScope::All);

    // 2. Build initial snapshot using effective params
    let initial_events = build_initial_snapshot(&ns, &effective_params).await;
    let initial_stream = stream::iter(initial_events.into_iter().map(Ok));

    // 3. Create live event stream from broadcast receiver using unfold
    let ns_for_live = ns.clone();
    let live_stream = stream::unfold(
        (subscription.receiver, ns_for_live, effective_params),
        |(mut rx, ns_inner, params)| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        // Filter by effective params
                        let resource_id = event_resource_id(&event);
                        let kind = event_kind(&event);

                        let matches = if params.all {
                            true
                        } else {
                            match kind {
                                SSE_KIND_LEDGER => params.ledgers.iter().any(|l| l == resource_id),
                                SSE_KIND_GRAPH_SOURCE => {
                                    params.graph_sources.iter().any(|v| v == resource_id)
                                }
                                _ => false,
                            }
                        };

                        if !matches {
                            // Skip non-matching events, continue loop
                            continue;
                        }

                        if let Some(sse_event) = transform_event(&ns_inner, event).await {
                            return Some((Ok(sse_event), (rx, ns_inner, params)));
                        }
                        // Event transformed to None (e.g., record not found), continue
                        continue;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            lagged = n,
                            "SSE broadcast lagged, some events may have been missed"
                        );
                        // Continue listening after lag
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed, end the stream
                        return None;
                    }
                }
            }
        },
    );

    // 4. Chain: snapshot first, then live events
    let combined_stream = initial_stream.chain(live_stream);

    Ok(
        Sse::new(combined_stream)
            .keep_alive(KeepAlive::default().interval(Duration::from_secs(30))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_short() {
        let hash = sha256_short("test config");
        assert_eq!(hash.len(), 8);
    }

    #[test]
    fn test_ledger_event_id() {
        let record = NsRecord {
            ledger_id: "test:main".to_string(),
            name: "test".to_string(),
            branch: "main".to_string(),
            commit_head_id: None,
            config_id: None,
            commit_t: 42,
            index_head_id: None,
            index_t: 40,
            default_context: None,
            retracted: false,
            source_branch: None,
            branches: 0,
        };
        let id = ledger_event_id("test:main", &record);
        assert_eq!(id, "ledger:test:main:42:40");
    }

    #[test]
    fn test_retracted_event_id_format() {
        let id = retracted_event_id("ledger", "test:main");
        assert!(id.starts_with("ledger:test:main:retracted:"));
    }

    #[test]
    fn test_events_query_matches() {
        let query = EventsQuery {
            all: false,
            ledgers: vec!["books:main".to_string(), "users:main".to_string()],
            graph_sources: vec!["search:main".to_string()],
        };

        assert!(query.matches("books:main", "ledger"));
        assert!(query.matches("users:main", "ledger"));
        assert!(!query.matches("other:main", "ledger"));
        assert!(query.matches("search:main", "graph-source"));
        assert!(!query.matches("books:main", "graph-source"));
    }

    #[test]
    fn test_events_query_matches_all() {
        let query = EventsQuery {
            all: true,
            ledgers: vec![],
            graph_sources: vec![],
        };

        assert!(query.matches("any:main", "ledger"));
        assert!(query.matches("any:main", "graph-source"));
    }

    /// Parse a query string that this test expects to be valid.
    fn parsed(raw: &str) -> EventsQuery {
        EventsQuery::from_query_str(raw).unwrap_or_else(|e| panic!("{raw} must parse: {e}"))
    }

    /// The wire form, which nothing used to cover: every test above builds
    /// an `EventsQuery` in process, so the fact that no `?ledger=` request
    /// could ever be deserialized went unnoticed. Repeated keys must
    /// accumulate, and a percent-encoded `:` must survive.
    #[test]
    fn test_events_query_from_query_str() {
        let one = parsed("ledger=books%3Amain");
        assert_eq!(one.ledgers, vec!["books:main".to_string()]);
        assert!(!one.all);
        assert!(one.matches("books:main", "ledger"));

        let many = parsed("ledger=books%3Amain&ledger=users%3Amain&graph-source=search%3Amain");
        assert_eq!(
            many.ledgers,
            vec!["books:main".to_string(), "users:main".to_string()]
        );
        assert_eq!(many.graph_sources, vec!["search:main".to_string()]);

        assert!(parsed("all=true").all);
        assert!(parsed("all=1").all);
        assert!(!parsed("all=false").all);
        assert!(!parsed("").all);

        // Unknown keys and empty values are ignored, not fatal.
        let odd = parsed("ledger=&future=1&ledger=a%3Ab");
        assert_eq!(odd.ledgers, vec!["a:b".to_string()]);
    }

    /// `?all=` is the whole subscription request. A value this parser does
    /// not understand must NOT resolve to `false`: that hands the client a
    /// 200 SSE stream matching nothing, emitting nothing, forever, with
    /// nothing to notice — the silent-freeze class this endpoint's parser
    /// exists to remove, and what `serde_urlencoded` used to 400 on.
    #[test]
    fn an_uninterpretable_all_is_rejected_rather_than_silently_false() {
        for raw in ["all=ture", "all=maybe", "all=", "all=2", "all=null"] {
            let err = EventsQuery::from_query_str(raw)
                .err()
                .unwrap_or_else(|| panic!("{raw} must not be accepted"));
            assert_eq!(
                err.status_code(),
                axum::http::StatusCode::BAD_REQUEST,
                "{raw}"
            );
            let message = err.to_string();
            assert!(
                message.contains("`all`"),
                "must name the parameter: {message}"
            );
        }

        // The spellings a human actually types all work, either case.
        for raw in ["all=TRUE", "all=Yes", "all=on", "all=1", "all"] {
            assert!(parsed(raw).all, "{raw} must mean true");
        }
        for raw in ["all=FALSE", "all=no", "all=off", "all=0"] {
            assert!(!parsed(raw).all, "{raw} must mean false");
        }

        // And an unusable `ledger=` stays ignorable: it is one alias among
        // however many, not the entire request.
        assert!(EventsQuery::from_query_str("ledger=&ledger=a%3Ab").is_ok());
    }
}
