//! SSE-based remote watch implementation
//!
//! Connects to a remote's `/fluree/events` SSE endpoint, parses incoming events
//! via `fluree_sse::SseParser`, and yields `RemoteEvent`s. Automatically
//! reconnects with exponential backoff on connection loss.

use crate::backoff::Backoff;
use crate::server_sse::parse_server_sse_event;
use crate::watch::RemoteEvent;
use fluree_sse::SseParser;
use futures::{Stream, StreamExt};
use std::fmt::Debug;
use std::pin::Pin;

/// SSE-based remote watch
#[derive(Debug)]
pub struct SseRemoteWatch {
    events_url: String,
    auth_token: Option<String>,
}

impl SseRemoteWatch {
    /// Create a new SSE watch.
    ///
    /// `events_url` should be the full URL to the SSE endpoint,
    /// e.g., `http://localhost:8090/fluree/events?all=true`.
    pub fn new(events_url: impl Into<String>, auth_token: Option<String>) -> Self {
        Self {
            events_url: events_url.into(),
            auth_token,
        }
    }
}

impl crate::watch::RemoteWatch for SseRemoteWatch {
    fn watch(&self) -> Pin<Box<dyn Stream<Item = RemoteEvent> + Send>> {
        let events_url = self.events_url.clone();
        let auth_token = self.auth_token.clone();

        let stream = async_stream::stream! {
            let client = reqwest::Client::new();
            let mut backoff = Backoff::new(500, 30_000);
            let mut consecutive_parse_errors: usize = 0;
            const MAX_CONSECUTIVE_PARSE_ERRORS: usize = 25;

            loop {
                let mut request = client.get(&events_url);
                if let Some(ref token) = auth_token {
                    request = request.bearer_auth(token);
                }
                request = request.header("Accept", "text/event-stream");

                match request.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        backoff.reset();
                        yield RemoteEvent::Connected;

                        let mut parser = SseParser::new();
                        let mut byte_stream = resp.bytes_stream();

                        while let Some(chunk_result) = byte_stream.next().await {
                            match chunk_result {
                                Ok(bytes) => {
                                    for event in parser.feed(&bytes) {
                                        match parse_server_sse_event(&event) {
                                            Ok(Some(remote_event)) => {
                                                consecutive_parse_errors = 0;
                                                yield remote_event;
                                            }
                                            Ok(None) => {
                                                // ignored event (keepalive / unknown type)
                                            }
                                            Err(e) => {
                                                consecutive_parse_errors += 1;
                                                tracing::warn!(
                                                    error = %e,
                                                    consecutive = consecutive_parse_errors,
                                                    "Failed to parse server SSE event"
                                                );

                                                if consecutive_parse_errors >= MAX_CONSECUTIVE_PARSE_ERRORS {
                                                    yield RemoteEvent::Fatal {
                                                        reason: format!(
                                                            "Too many SSE parse errors ({consecutive_parse_errors}): likely schema mismatch"
                                                        ),
                                                    };
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    yield RemoteEvent::Disconnected {
                                        reason: format!("Stream error: {e}"),
                                    };
                                    break;
                                }
                            }
                        }

                        // Stream ended without error (server closed)
                        yield RemoteEvent::Disconnected {
                            reason: "Stream ended".to_string(),
                        };
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        if status.as_u16() == 401 || status.as_u16() == 403 {
                            // Fatal auth error — do not retry
                            yield RemoteEvent::Fatal {
                                reason: format!("HTTP {status}"),
                            };
                            break;
                        }
                        yield RemoteEvent::Disconnected {
                            reason: format!("HTTP {status}"),
                        };
                    }
                    Err(e) => {
                        yield RemoteEvent::Disconnected {
                            reason: format!("Connection failed: {e}"),
                        };
                    }
                }

                // Wait before reconnecting
                let delay = backoff.next_delay();
                tracing::debug!("SSE reconnecting in {:?}", delay);
                tokio::time::sleep(delay).await;
            }
        };

        Box::pin(stream)
    }
}
