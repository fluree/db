//! SSE subscription task for peer mode
//!
//! Maintains an SSE connection to the transaction server's `/fluree/events`
//! endpoint and updates the peer state with remote watermarks.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use crate::config::ServerConfig;
use crate::peer::state::PeerState;

use fluree_db_api::{Fluree, NotifyResult, NsNotify};
use fluree_db_core::ledger_id::split_ledger_id;
use fluree_db_core::ContentId;
use fluree_db_nameservice::NsRecord;
use fluree_db_peer::{GraphSourceRecord, LedgerRecord};
use fluree_sse::{SseEvent, SseParser, SSE_KIND_GRAPH_SOURCE, SSE_KIND_LEDGER};

/// Background task that maintains SSE subscription to transaction server
pub struct PeerSubscriptionTask {
    config: ServerConfig,
    peer_state: Arc<PeerState>,
    fluree: Arc<Fluree>,
    http_client: reqwest::Client,
}

impl PeerSubscriptionTask {
    pub fn new(config: ServerConfig, peer_state: Arc<PeerState>, fluree: Arc<Fluree>) -> Self {
        let http_client = reqwest::Client::builder()
            // No timeout for SSE - it's a long-lived connection
            .connect_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            config,
            peer_state,
            fluree,
            http_client,
        }
    }

    /// Spawn the subscription task
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&self) {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(self.config.peer_reconnect_initial_ms),
            Duration::from_millis(self.config.peer_reconnect_max_ms),
            self.config.peer_reconnect_multiplier,
        );

        loop {
            match self.connect_and_stream().await {
                Ok(()) => {
                    // Clean disconnect (server closed connection) - reconnect after short delay
                    // This handles server restarts, load balancer cycling, etc.
                    backoff.reset();
                    let delay = backoff.next_delay();
                    tracing::info!(
                        reconnect_in_ms = delay.as_millis(),
                        "SSE stream ended cleanly, will reconnect"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    self.peer_state.set_connected(false).await;

                    // Check for fatal errors (401/403, token load failure)
                    if e.is_fatal() {
                        tracing::error!(error = %e, "Fatal peer subscription error, will not retry");
                        break;
                    }

                    let delay = backoff.next_delay();
                    tracing::warn!(
                        error = %e,
                        reconnect_in_ms = delay.as_millis(),
                        "Peer SSE subscription failed, will reconnect"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn connect_and_stream(&self) -> Result<(), PeerSubscriptionError> {
        let url = self.build_events_url();

        tracing::info!(url = %url, "Connecting to transaction server events");

        let mut request = self.http_client.get(&url);

        // Add Bearer token if configured
        if let Some(token) = self
            .config
            .load_peer_events_token()
            .map_err(PeerSubscriptionError::TokenLoad)?
        {
            request = request.header("Authorization", format!("Bearer {token}"));
        }

        request = request.header("Accept", "text/event-stream");

        let response = request.send().await?;

        if !response.status().is_success() {
            return Err(PeerSubscriptionError::HttpStatus(response.status()));
        }

        // Clear state on reconnect (new snapshot coming)
        self.peer_state.clear().await;
        self.peer_state.set_connected(true).await;

        tracing::info!("Connected to transaction server, receiving snapshot");

        // Optional: preload explicitly configured ledgers so the peer starts "warm".
        //
        // We intentionally do NOT preload on subscribe-all to avoid accidentally
        // loading a large number of ledgers.
        self.preload_configured_ledgers().await;

        // Stream and parse SSE events
        let mut stream = response.bytes_stream();
        let mut parser = SseParser::new();

        while let Some(chunk_result) = stream.next().await {
            let bytes = chunk_result?;

            for event in parser.feed(&bytes) {
                if let Err(e) = self.handle_event(&event).await {
                    tracing::warn!(error = %e, "Error handling SSE event");
                }
            }
        }

        // Stream ended cleanly - mark disconnected
        self.peer_state.set_connected(false).await;
        tracing::debug!("SSE stream ended");

        Ok(())
    }

    async fn handle_event(&self, event: &SseEvent) -> Result<(), PeerSubscriptionError> {
        match event.event_type.as_deref() {
            Some("ns-record") => {
                // Parse the ns-record event
                let data: NsRecordData = serde_json::from_str(&event.data)?;

                match data.kind.as_str() {
                    SSE_KIND_LEDGER => {
                        let record: LedgerRecord = serde_json::from_value(data.record)?;
                        let changed = self
                            .peer_state
                            .update_ledger(
                                &record.ledger_id,
                                record.commit_t,
                                record.index_t,
                                record.commit_head_id.clone(),
                                record.index_head_id.clone(),
                            )
                            .await;

                        if changed {
                            tracing::info!(
                                ledger_id = %record.ledger_id,
                                commit_t = record.commit_t,
                                index_t = record.index_t,
                                "Remote ledger watermark updated"
                            );
                        }

                        // Keep hot: if this ledger is already cached locally, apply the
                        // nameservice update to the library-level cache (reload if stale).
                        self.refresh_cached_ledger_from_record(&record).await;
                    }
                    SSE_KIND_GRAPH_SOURCE => {
                        let record: GraphSourceRecord = serde_json::from_value(data.record)?;
                        let changed = self
                            .peer_state
                            .update_graph_source(
                                &record.graph_source_id,
                                record.index_t,
                                record.config_hash(),
                                record.index_id.clone(),
                            )
                            .await;

                        if changed {
                            tracing::info!(
                                graph_source_id = %record.graph_source_id,
                                index_t = record.index_t,
                                "Remote graph source watermark updated"
                            );
                        }
                    }
                    _ => {
                        tracing::debug!(kind = %data.kind, "Unknown ns-record kind");
                    }
                }
            }
            Some("ns-retracted") => {
                let data: NsRetractedData = serde_json::from_str(&event.data)?;

                match data.kind.as_str() {
                    SSE_KIND_LEDGER => {
                        self.peer_state.remove_ledger(&data.resource_id).await;
                        tracing::info!(ledger_id = %data.resource_id, "Ledger retracted from remote");

                        // Evict any cached state for the ledger (no-op if not cached).
                        self.disconnect_cached_ledger(&data.resource_id).await;
                    }
                    SSE_KIND_GRAPH_SOURCE => {
                        self.peer_state.remove_graph_source(&data.resource_id).await;
                        tracing::info!(graph_source_id = %data.resource_id, "Graph source retracted from remote");
                    }
                    _ => {
                        tracing::debug!(kind = %data.kind, "Unknown retraction kind");
                    }
                }
            }
            None | Some("") => {
                // No event type - heartbeat or comment, ignore
            }
            Some(unknown) => {
                tracing::debug!(event_type = %unknown, "Unknown SSE event type, ignoring");
            }
        }

        Ok(())
    }

    async fn preload_configured_ledgers(&self) {
        let sub = self.config.peer_subscription();
        if sub.all || sub.ledgers.is_empty() {
            return;
        }

        for ledger_id in &sub.ledgers {
            // Preload by loading into the connection-level ledger cache.
            let result = self.fluree.ledger_cached(ledger_id).await.map(|_| ());

            match result {
                Ok(()) => {
                    tracing::info!(ledger_id = %ledger_id, "Preloaded ledger into peer cache");
                }
                Err(e) => {
                    tracing::warn!(ledger_id = %ledger_id, error = %e, "Failed to preload ledger");
                }
            }
        }
    }

    async fn disconnect_cached_ledger(&self, ledger_id: &str) {
        self.fluree.disconnect_ledger(ledger_id).await;
    }

    async fn refresh_cached_ledger_from_record(&self, record: &LedgerRecord) {
        let ns_record = match ledger_record_to_ns_record(record) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(ledger_id = %record.ledger_id, error = %e, "Failed to build NsRecord from SSE record");
                return;
            }
        };

        let Some(mgr) = self.fluree.ledger_manager() else {
            return;
        };
        self.notify_mgr(mgr, record, ns_record).await;
    }

    async fn notify_mgr(
        &self,
        mgr: &Arc<fluree_db_api::LedgerManager>,
        record: &LedgerRecord,
        ns_record: NsRecord,
    ) {
        match mgr
            .notify(NsNotify {
                ledger_id: record.ledger_id.clone(),
                record: Some(ns_record),
            })
            .await
        {
            Ok(NotifyResult::NotLoaded) => {
                // Not cached - do not cold-load on events (avoids subscribe-all stampede).
            }
            Ok(NotifyResult::Current) => {
                // Already up to date.
            }
            Ok(
                result @ (NotifyResult::Reloaded
                | NotifyResult::IndexUpdated
                | NotifyResult::CommitsApplied { .. }),
            ) => {
                tracing::info!(ledger_id = %record.ledger_id, ?result, "Refreshed cached ledger from SSE update");
            }
            Err(e) => {
                tracing::warn!(ledger_id = %record.ledger_id, error = %e, "Failed to refresh cached ledger from SSE update");
            }
        }
    }

    fn build_events_url(&self) -> String {
        let mut url = self
            .config
            .peer_events_url()
            .expect("peer_events_url should be set in peer mode");

        let sub = self.config.peer_subscription();
        let mut params = vec![];

        if sub.all {
            params.push("all=true".to_string());
        } else {
            for l in &sub.ledgers {
                params.push(format!("ledger={}", urlencoding::encode(l)));
            }
            for gs in &sub.graph_sources {
                params.push(format!("graph-source={}", urlencoding::encode(gs)));
            }
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        url
    }
}

fn ledger_record_to_ns_record(record: &LedgerRecord) -> Result<NsRecord, String> {
    let (name, branch) = split_ledger_id(&record.ledger_id)
        .map_err(|e| format!("invalid ledger ID '{}': {}", record.ledger_id, e))?;

    let commit_head_id = record
        .commit_head_id
        .as_deref()
        .and_then(|s| s.parse::<ContentId>().ok());
    let index_head_id = record
        .index_head_id
        .as_deref()
        .and_then(|s| s.parse::<ContentId>().ok());

    Ok(NsRecord {
        ledger_id: record.ledger_id.clone(),
        name,
        branch,
        commit_head_id,
        config_id: None,
        commit_t: record.commit_t,
        index_head_id,
        index_t: record.index_t,
        default_context: None,
        retracted: record.retracted,
        source_branch: None,
        branches: 0,
    })
}

/// Parsed ns-record event data
#[derive(Debug, serde::Deserialize)]
#[expect(dead_code)] // Fields used by serde deserialization
struct NsRecordData {
    action: String,
    kind: String,
    resource_id: String,
    record: serde_json::Value,
    emitted_at: String,
}

/// Parsed ns-retracted event data
#[derive(Debug, serde::Deserialize)]
#[expect(dead_code)] // Fields used by serde deserialization
struct NsRetractedData {
    action: String,
    kind: String,
    resource_id: String,
    emitted_at: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PeerSubscriptionError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("HTTP status {0}")]
    HttpStatus(reqwest::StatusCode),

    #[error("Failed to load token: {0}")]
    TokenLoad(std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

impl PeerSubscriptionError {
    pub fn is_fatal(&self) -> bool {
        match self {
            PeerSubscriptionError::HttpStatus(status) => {
                status.as_u16() == 401 || status.as_u16() == 403
            }
            PeerSubscriptionError::TokenLoad(_) => true,
            _ => false,
        }
    }
}

/// Simple exponential backoff with jitter
struct ExponentialBackoff {
    initial: Duration,
    max: Duration,
    multiplier: f64,
    current: Duration,
}

impl ExponentialBackoff {
    fn new(initial: Duration, max: Duration, multiplier: f64) -> Self {
        Self {
            initial,
            max,
            multiplier,
            current: initial,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = std::cmp::min(
            self.max,
            Duration::from_secs_f64(self.current.as_secs_f64() * self.multiplier),
        );
        // Add jitter (±25%)
        let jitter = rand::random::<f64>() * 0.5 - 0.25;
        Duration::from_secs_f64(delay.as_secs_f64() * (1.0 + jitter))
    }

    fn reset(&mut self) {
        self.current = self.initial;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff_increases() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_millis(10000),
            2.0,
        );

        // First delay should be around 100ms (±25% jitter)
        let delay1 = backoff.next_delay();
        assert!(delay1.as_millis() >= 75 && delay1.as_millis() <= 125);

        // Second delay should be around 200ms
        let delay2 = backoff.next_delay();
        assert!(delay2.as_millis() >= 150 && delay2.as_millis() <= 250);
    }

    #[test]
    fn test_exponential_backoff_caps_at_max() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(1000),
            Duration::from_millis(2000),
            10.0,
        );

        // First delay
        let _ = backoff.next_delay();

        // Second delay should be capped at max (with jitter)
        let delay2 = backoff.next_delay();
        // Max is 2000, with +25% jitter = 2500
        assert!(delay2.as_millis() <= 2500);
    }

    #[test]
    fn test_exponential_backoff_reset() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_millis(10000),
            2.0,
        );

        // Advance several times
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();

        // Reset
        backoff.reset();

        // Should be back to initial
        let delay = backoff.next_delay();
        assert!(delay.as_millis() >= 75 && delay.as_millis() <= 125);
    }
}
