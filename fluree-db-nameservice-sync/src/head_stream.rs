//! Shared SSE head-tracking pump: the peer subscription loop, lifted.
//!
//! The transaction server publishes nameservice head changes over
//! `GET /events` as SSE. Every consumer of that stream — the native peer's
//! subscription task, the browser peer's driver — runs the same loop:
//! connect, parse frames with [`fluree_sse::SseParser`], convert payloads
//! with [`crate::server_sse`], dispatch typed [`RemoteEvent`]s, and
//! reconnect with exponential backoff (clean stream end resets the
//! backoff; failures grow it; 401/403 is fatal).
//!
//! [`run_head_stream`] is that loop, parameterized over everything
//! runtime-specific: the connection ([`SseChunkSource`] — reqwest natively,
//! a channel fed by the browser driver's fetch-stream on wasm), the timer
//! ([`Sleeper`]), and the consumer ([`HeadSink`]). The pump itself is
//! target-agnostic and returns a `Send` future, so a wasm host can drive
//! it from any executor while its JS-owning pieces stay behind channels.

use crate::server_sse::parse_server_sse_event;
use crate::watch::RemoteEvent;
use async_trait::async_trait;
use bytes::Bytes;
use fluree_sse::SseParser;
use futures::future::Either;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::watch;

/// A boxed stream of raw SSE body chunks. `Err` is a mid-stream transport
/// failure (the pump reconnects); the stream ending is a clean disconnect.
pub type BoxChunkStream = Pin<Box<dyn Stream<Item = Result<Bytes, String>> + Send>>;

/// Why an SSE connection attempt did not yield a stream.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SseConnectError {
    /// Do not retry: bad credentials (401/403) or an unrecoverable local
    /// failure (token unloadable, driver gone).
    #[error("fatal: {0}")]
    Fatal(String),
    /// Transient: retry with backoff.
    #[error("{0}")]
    Retryable(String),
}

/// Establishes SSE connections. Called once per (re)connect attempt so
/// implementations can re-resolve tokens or rebuild requests each time.
#[async_trait]
pub trait SseChunkSource: Send + Sync {
    async fn connect(&self) -> Result<BoxChunkStream, SseConnectError>;
}

/// Consumes the pump's typed events. `Connected` arrives before the
/// server's snapshot replay on every (re)connect; `Disconnected` after
/// every stream end or failure; `Fatal` exactly once, last, when the pump
/// gives up.
#[async_trait]
pub trait HeadSink: Send + Sync {
    async fn on_event(&self, event: RemoteEvent);
}

/// The pump's timer. Native: tokio. Wasm: whatever owns the JS clock.
#[async_trait]
pub trait Sleeper: Send + Sync {
    async fn sleep(&self, duration: Duration);
}

/// [`Sleeper`] over the tokio timer.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioSleeper;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Sleeper for TokioSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Reconnect policy knobs (the server's `peer_reconnect_*` settings).
#[derive(Debug, Clone, Copy)]
pub struct HeadStreamConfig {
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
    pub reconnect_multiplier: f64,
}

impl Default for HeadStreamConfig {
    fn default() -> Self {
        Self {
            reconnect_initial: Duration::from_millis(1_000),
            reconnect_max: Duration::from_millis(30_000),
            reconnect_multiplier: 2.0,
        }
    }
}

/// Exponential backoff with ±25% jitter, lifted verbatim from the server's
/// peer subscription task so reconnect behavior (including the configurable
/// multiplier) is preserved.
struct ExponentialBackoff {
    initial: Duration,
    max: Duration,
    multiplier: f64,
    current: Duration,
}

impl ExponentialBackoff {
    fn new(config: &HeadStreamConfig) -> Self {
        Self {
            initial: config.reconnect_initial,
            max: config.reconnect_max,
            multiplier: config.reconnect_multiplier,
            current: config.reconnect_initial,
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

/// `true` when the stop signal fired (set to `true`, or sender dropped).
async fn stopped(stop: &mut watch::Receiver<bool>) -> bool {
    match stop.changed().await {
        Ok(()) => *stop.borrow(),
        // Sender gone: the tracker was dropped — stop.
        Err(_) => true,
    }
}

/// Race `fut` against the stop signal; `None` means stop won.
async fn until_stopped<F, T>(fut: F, stop: &mut watch::Receiver<bool>) -> Option<T>
where
    F: std::future::Future<Output = T>,
{
    futures::pin_mut!(fut);
    loop {
        let stop_fut = stopped(stop);
        futures::pin_mut!(stop_fut);
        match futures::future::select(fut, stop_fut).await {
            Either::Left((value, _)) => return Some(value),
            Either::Right((true, _)) => return None,
            Either::Right((false, rest)) => {
                // Spurious change back to false; keep waiting.
                fut = rest;
            }
        }
    }
}

/// Run the head-tracking loop until a fatal error or the stop signal.
///
/// Loop shape (identical to the native peer subscription task):
/// 1. `connect()`; fatal → emit [`RemoteEvent::Fatal`] and return;
///    retryable → emit `Disconnected`, back off, retry.
/// 2. On a stream: emit [`RemoteEvent::Connected`], parse chunks into SSE
///    frames, convert each recognized frame and dispatch it to the sink
///    (malformed frames of recognized types are logged and skipped).
/// 3. Stream end → `Disconnected`; a clean end resets the backoff before
///    the next delay, a mid-stream failure lets it keep growing.
pub async fn run_head_stream(
    source: &dyn SseChunkSource,
    sink: &dyn HeadSink,
    sleeper: &dyn Sleeper,
    config: HeadStreamConfig,
    mut stop: watch::Receiver<bool>,
) {
    let mut backoff = ExponentialBackoff::new(&config);
    loop {
        if *stop.borrow() {
            return;
        }
        let Some(outcome) = until_stopped(source.connect(), &mut stop).await else {
            return;
        };
        match outcome {
            Err(SseConnectError::Fatal(reason)) => {
                sink.on_event(RemoteEvent::Fatal { reason }).await;
                return;
            }
            Err(SseConnectError::Retryable(reason)) => {
                sink.on_event(RemoteEvent::Disconnected { reason }).await;
                let delay = backoff.next_delay();
                if until_stopped(sleeper.sleep(delay), &mut stop)
                    .await
                    .is_none()
                {
                    return;
                }
            }
            Ok(mut stream) => {
                sink.on_event(RemoteEvent::Connected).await;
                let mut parser = SseParser::new();
                let mut failure: Option<String> = None;
                loop {
                    let Some(item) = until_stopped(stream.next(), &mut stop).await else {
                        return;
                    };
                    match item {
                        None => break,
                        Some(Err(reason)) => {
                            failure = Some(reason);
                            break;
                        }
                        Some(Ok(bytes)) => {
                            for frame in parser.feed(&bytes) {
                                match parse_server_sse_event(&frame) {
                                    Ok(Some(event)) => sink.on_event(event).await,
                                    Ok(None) => {}
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "malformed head-stream event; skipping"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                let clean = failure.is_none();
                sink.on_event(RemoteEvent::Disconnected {
                    reason: failure.unwrap_or_else(|| "stream ended".to_string()),
                })
                .await;
                if clean {
                    backoff.reset();
                }
                let delay = backoff.next_delay();
                if until_stopped(sleeper.sleep(delay), &mut stop)
                    .await
                    .is_none()
                {
                    return;
                }
            }
        }
    }
}

/// Native [`SseChunkSource`] over reqwest: `GET {url}` with
/// `Accept: text/event-stream` and an optional bearer resolved per connect
/// (tokens can rotate on disk between reconnects). 401/403 and token-load
/// failures are fatal, matching the peer task's historical taxonomy.
#[cfg(not(target_arch = "wasm32"))]
pub struct ReqwestSseSource {
    client: reqwest::Client,
    url: String,
    token_provider: TokenProvider,
}

/// Resolves the bearer token for a connection attempt.
#[cfg(not(target_arch = "wasm32"))]
pub type TokenProvider = std::sync::Arc<dyn Fn() -> std::io::Result<Option<String>> + Send + Sync>;

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for ReqwestSseSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestSseSource")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ReqwestSseSource {
    /// A source for `url` with a 30 s connect timeout and no read timeout
    /// (SSE is long-lived), matching the native peer task's client.
    pub fn new(url: String, token_provider: TokenProvider) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create SSE client");
        Self {
            client,
            url,
            token_provider,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl SseChunkSource for ReqwestSseSource {
    async fn connect(&self) -> Result<BoxChunkStream, SseConnectError> {
        let token = (self.token_provider)()
            .map_err(|e| SseConnectError::Fatal(format!("Failed to load token: {e}")))?;

        let mut request = self
            .client
            .get(&self.url)
            .header("Accept", "text/event-stream");
        if let Some(token) = token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }

        let response = request
            .send()
            .await
            .map_err(|e| SseConnectError::Retryable(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let reason = format!("HTTP status {status}");
            return Err(if status.as_u16() == 401 || status.as_u16() == 403 {
                SseConnectError::Fatal(reason)
            } else {
                SseConnectError::Retryable(reason)
            });
        }

        Ok(Box::pin(
            response
                .bytes_stream()
                .map(|item| item.map_err(|e| e.to_string())),
        ))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// One scripted connection attempt.
    enum Script {
        Frames(Vec<Vec<u8>>),
        FailMidStream(Vec<Vec<u8>>, String),
        Retryable(String),
        Fatal(String),
    }

    struct ScriptedSource {
        scripts: Mutex<VecDeque<Script>>,
    }

    impl ScriptedSource {
        fn new(scripts: Vec<Script>) -> Self {
            Self {
                scripts: Mutex::new(scripts.into()),
            }
        }
    }

    #[async_trait]
    impl SseChunkSource for ScriptedSource {
        async fn connect(&self) -> Result<BoxChunkStream, SseConnectError> {
            let script = self.scripts.lock().unwrap().pop_front();
            match script {
                None => Err(SseConnectError::Fatal("script exhausted".to_string())),
                Some(Script::Fatal(reason)) => Err(SseConnectError::Fatal(reason)),
                Some(Script::Retryable(reason)) => Err(SseConnectError::Retryable(reason)),
                Some(Script::Frames(chunks)) => Ok(Box::pin(futures::stream::iter(
                    chunks.into_iter().map(|c| Ok(Bytes::from(c))),
                ))),
                Some(Script::FailMidStream(chunks, reason)) => {
                    let items: Vec<Result<Bytes, String>> = chunks
                        .into_iter()
                        .map(|c| Ok(Bytes::from(c)))
                        .chain([Err(reason)])
                        .collect();
                    Ok(Box::pin(futures::stream::iter(items)))
                }
            }
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl HeadSink for RecordingSink {
        async fn on_event(&self, event: RemoteEvent) {
            let tag = match event {
                RemoteEvent::Connected => "connected".to_string(),
                RemoteEvent::Disconnected { .. } => "disconnected".to_string(),
                RemoteEvent::Fatal { reason } => format!("fatal:{reason}"),
                RemoteEvent::LedgerUpdated(r) => format!("ledger:{}@{}", r.ledger_id, r.commit_t),
                RemoteEvent::LedgerRetracted { ledger_id } => format!("retracted:{ledger_id}"),
                RemoteEvent::GraphSourceUpdated(r) => format!("gs:{}", r.graph_source_id),
                RemoteEvent::GraphSourceRetracted { graph_source_id } => {
                    format!("gs-retracted:{graph_source_id}")
                }
            };
            self.events.lock().unwrap().push(tag);
        }
    }

    #[derive(Default)]
    struct RecordingSleeper {
        delays: Mutex<Vec<Duration>>,
    }

    #[async_trait]
    impl Sleeper for RecordingSleeper {
        async fn sleep(&self, duration: Duration) {
            self.delays.lock().unwrap().push(duration);
        }
    }

    fn ledger_frame(ledger: &str, commit_t: i64) -> Vec<u8> {
        format!(
            "event: ns-record\ndata: {{\"action\":\"ns-record\",\"kind\":\"ledger\",\"resource_id\":\"{ledger}\",\"record\":{{\"ledger_id\":\"{ledger}\",\"branch\":\"main\",\"commit_head_id\":null,\"commit_t\":{commit_t},\"index_head_id\":null,\"index_t\":0,\"retracted\":false}},\"emitted_at\":\"now\"}}\n\n"
        )
        .into_bytes()
    }

    fn fast_config() -> HeadStreamConfig {
        HeadStreamConfig {
            reconnect_initial: Duration::from_millis(100),
            reconnect_max: Duration::from_millis(1_000),
            reconnect_multiplier: 2.0,
        }
    }

    fn no_stop() -> (watch::Sender<bool>, watch::Receiver<bool>) {
        watch::channel(false)
    }

    #[tokio::test]
    async fn events_flow_across_chunk_splits_and_clean_end_resets_backoff() {
        // One frame split mid-payload across two chunks, then a second frame.
        let frame = ledger_frame("books:main", 7);
        let (a, b) = frame.split_at(25);
        let source = ScriptedSource::new(vec![
            Script::Frames(vec![a.to_vec(), b.to_vec(), ledger_frame("books:main", 8)]),
            Script::Fatal("end of test".to_string()),
        ]);
        let sink = RecordingSink::default();
        let sleeper = RecordingSleeper::default();
        let (_tx, stop) = no_stop();

        run_head_stream(&source, &sink, &sleeper, fast_config(), stop).await;

        let events = sink.events.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                "connected",
                "ledger:books:main@7",
                "ledger:books:main@8",
                "disconnected",
                "fatal:end of test",
            ]
        );
        // Clean end: backoff reset before the delay → within initial ±25%.
        let delays = sleeper.delays.lock().unwrap().clone();
        assert_eq!(delays.len(), 1);
        assert!(delays[0] <= Duration::from_millis(125), "got {delays:?}");
    }

    #[tokio::test]
    async fn retryable_failures_grow_the_backoff_until_fatal_stops_the_pump() {
        let source = ScriptedSource::new(vec![
            Script::Retryable("refused".to_string()),
            Script::Retryable("refused".to_string()),
            Script::Fatal("credentials rejected".to_string()),
        ]);
        let sink = RecordingSink::default();
        let sleeper = RecordingSleeper::default();
        let (_tx, stop) = no_stop();

        run_head_stream(&source, &sink, &sleeper, fast_config(), stop).await;

        let events = sink.events.lock().unwrap().clone();
        assert_eq!(
            events,
            vec!["disconnected", "disconnected", "fatal:credentials rejected"]
        );
        let delays = sleeper.delays.lock().unwrap().clone();
        assert_eq!(delays.len(), 2);
        // Jitter is ±25%: delay₂ ∈ [150ms, 250ms] strictly exceeds
        // delay₁ ∈ [75ms, 125ms].
        assert!(delays[1] > delays[0], "backoff must grow: {delays:?}");
    }

    #[tokio::test]
    async fn mid_stream_failure_keeps_backoff_growing() {
        let source = ScriptedSource::new(vec![
            Script::FailMidStream(vec![ledger_frame("a:main", 1)], "reset by peer".to_string()),
            Script::FailMidStream(vec![], "reset by peer".to_string()),
            Script::Fatal("end".to_string()),
        ]);
        let sink = RecordingSink::default();
        let sleeper = RecordingSleeper::default();
        let (_tx, stop) = no_stop();

        run_head_stream(&source, &sink, &sleeper, fast_config(), stop).await;

        let delays = sleeper.delays.lock().unwrap().clone();
        assert_eq!(delays.len(), 2);
        assert!(delays[1] > delays[0], "no reset on failure: {delays:?}");
        let events = sink.events.lock().unwrap().clone();
        assert_eq!(events[0], "connected");
        assert_eq!(events[1], "ledger:a:main@1");
    }

    #[tokio::test]
    async fn malformed_recognized_events_are_skipped_not_fatal() {
        let mut bad_then_good = Vec::new();
        bad_then_good
            .extend_from_slice(b"event: ns-record\ndata: {\"kind\":\"ledger\",\"record\":42}\n\n");
        bad_then_good.extend_from_slice(&ledger_frame("ok:main", 3));
        let source = ScriptedSource::new(vec![
            Script::Frames(vec![bad_then_good]),
            Script::Fatal("end".to_string()),
        ]);
        let sink = RecordingSink::default();
        let sleeper = RecordingSleeper::default();
        let (_tx, stop) = no_stop();

        run_head_stream(&source, &sink, &sleeper, fast_config(), stop).await;

        let events = sink.events.lock().unwrap().clone();
        assert!(
            events.contains(&"ledger:ok:main@3".to_string()),
            "{events:?}"
        );
    }

    #[tokio::test]
    async fn stop_signal_ends_the_pump_mid_stream() {
        struct HangingSource;
        #[async_trait]
        impl SseChunkSource for HangingSource {
            async fn connect(&self) -> Result<BoxChunkStream, SseConnectError> {
                Ok(Box::pin(futures::stream::pending()))
            }
        }
        let sink = RecordingSink::default();
        let sleeper = RecordingSleeper::default();
        let (tx, stop) = no_stop();

        let pump = run_head_stream(&HangingSource, &sink, &sleeper, fast_config(), stop);
        let stopper = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            tx.send(true).unwrap();
        };
        tokio::join!(pump, stopper);

        let events = sink.events.lock().unwrap().clone();
        assert_eq!(events, vec!["connected"], "stopped while streaming");
    }

    #[tokio::test]
    async fn dropping_the_stop_sender_also_stops_the_pump() {
        struct HangingSource;
        #[async_trait]
        impl SseChunkSource for HangingSource {
            async fn connect(&self) -> Result<BoxChunkStream, SseConnectError> {
                Ok(Box::pin(futures::stream::pending()))
            }
        }
        let sink = RecordingSink::default();
        let sleeper = RecordingSleeper::default();
        let (tx, stop) = no_stop();

        let pump = run_head_stream(&HangingSource, &sink, &sleeper, fast_config(), stop);
        let dropper = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(tx);
        };
        tokio::join!(pump, dropper);
    }
}
