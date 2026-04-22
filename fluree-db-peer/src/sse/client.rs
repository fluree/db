//! SSE client with automatic reconnection
//!
//! Connects to the `/fluree/events` endpoint and maintains the connection
//! with exponential backoff reconnection on failure.

use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::config::PeerConfig;
use crate::error::SseError;
use crate::sse::events::{
    GraphSourceRecord, LedgerRecord, NsRecordEvent, NsRetractedEvent, SseClientEvent,
};
use fluree_sse::{SseParser, SSE_KIND_GRAPH_SOURCE, SSE_KIND_LEDGER};

/// SSE client that handles connection, reconnect, and event parsing
pub struct SseClient {
    config: PeerConfig,
    http_client: reqwest::Client,
}

impl SseClient {
    /// Create a new SSE client with the given configuration
    pub fn new(config: PeerConfig) -> Self {
        let http_client = reqwest::Client::builder()
            // No timeout for SSE - it's a long-lived connection
            .connect_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            config,
            http_client,
        }
    }

    /// Start the SSE client, returning a channel of events
    ///
    /// This spawns a background task that:
    /// 1. Connects to the events endpoint
    /// 2. Parses SSE events and sends them to the channel
    /// 3. Reconnects on disconnect with exponential backoff
    pub fn start(self) -> mpsc::Receiver<SseClientEvent> {
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            self.run_loop(tx).await;
        });

        rx
    }

    async fn run_loop(self, tx: mpsc::Sender<SseClientEvent>) {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(self.config.reconnect_initial_ms),
            Duration::from_millis(self.config.reconnect_max_ms),
            self.config.reconnect_multiplier,
        );

        loop {
            match self.connect_and_stream(&tx).await {
                Ok(()) => {
                    // Clean disconnect (channel closed or receiver dropped)
                    tracing::info!("SSE stream ended cleanly");
                    backoff.reset();
                    break;
                }
                Err(e) => {
                    // Check if this is a fatal error (401/403, token load failure)
                    if e.is_fatal() {
                        tracing::error!(error = %e, "Fatal SSE error, will not retry");
                        let _ = tx
                            .send(SseClientEvent::Fatal {
                                error: e.to_string(),
                            })
                            .await;
                        break;
                    }

                    // Transient error - retry with backoff
                    let _ = tx
                        .send(SseClientEvent::Disconnected {
                            reason: e.to_string(),
                        })
                        .await;

                    let delay = backoff.next_delay();
                    tracing::warn!(
                        error = %e,
                        reconnect_in_ms = delay.as_millis(),
                        "SSE connection failed, will reconnect"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn connect_and_stream(&self, tx: &mpsc::Sender<SseClientEvent>) -> Result<(), SseError> {
        let url = self.config.events_url_with_params();

        tracing::info!(url = %url, "Connecting to events endpoint");

        let mut request = self.http_client.get(&url);

        // Add Bearer token if configured
        if let Some(token) = self.config.load_token().map_err(SseError::TokenLoad)? {
            request = request.header("Authorization", format!("Bearer {token}"));
        }

        // Set Accept header for SSE
        request = request.header("Accept", "text/event-stream");

        let response = request.send().await?;

        if !response.status().is_success() {
            return Err(SseError::HttpStatus(response.status()));
        }

        let _ = tx.send(SseClientEvent::Connected).await;

        // Reset backoff on successful connection
        tracing::info!("Connected to events endpoint");

        // Stream SSE events
        let mut stream = response.bytes_stream();
        let mut parser = SseParser::new();

        while let Some(chunk_result) = stream.next().await {
            let bytes = chunk_result?;

            for event in parser.feed(&bytes) {
                match self.parse_event(&event) {
                    Ok(Some(client_event)) => {
                        if tx.send(client_event).await.is_err() {
                            // Receiver dropped, exit cleanly
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        // Unknown event type, skip
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            event_type = ?event.event_type,
                            "Failed to parse SSE event"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_event(
        &self,
        event: &fluree_sse::SseEvent,
    ) -> Result<Option<SseClientEvent>, SseError> {
        match event.event_type.as_deref() {
            Some("ns-record") => {
                let data: NsRecordEvent = serde_json::from_str(&event.data)?;

                match data.kind.as_str() {
                    SSE_KIND_LEDGER => {
                        let record: LedgerRecord = serde_json::from_value(data.record)?;
                        Ok(Some(SseClientEvent::LedgerRecord(record)))
                    }
                    SSE_KIND_GRAPH_SOURCE => {
                        let record: GraphSourceRecord = serde_json::from_value(data.record)?;
                        Ok(Some(SseClientEvent::GraphSourceRecord(record)))
                    }
                    _ => {
                        tracing::debug!(kind = %data.kind, "Unknown ns-record kind");
                        Err(SseError::UnknownRecordType(data.kind))
                    }
                }
            }
            Some("ns-retracted") => {
                let data: NsRetractedEvent = serde_json::from_str(&event.data)?;
                Ok(Some(SseClientEvent::Retracted {
                    kind: data.kind,
                    resource_id: data.resource_id,
                }))
            }
            Some("snapshot-complete") => {
                // Server may send this after initial snapshot
                let data: serde_json::Value = serde_json::from_str(&event.data)?;
                let hash = data
                    .get("hash")
                    .and_then(|h| h.as_str())
                    .unwrap_or("")
                    .to_string();
                Ok(Some(SseClientEvent::SnapshotComplete { hash }))
            }
            None | Some("") => {
                // No event type - could be a heartbeat or comment
                Ok(None)
            }
            Some(unknown) => {
                tracing::debug!(event_type = %unknown, "Unknown SSE event type, ignoring");
                Ok(None)
            }
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
