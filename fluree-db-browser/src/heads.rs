//! Browser head tracking: the shared SSE pump wired to the driver and the
//! engine.
//!
//! The pump ([`fluree_db_nameservice_sync::run_head_stream`]) is
//! target-agnostic; this module supplies its browser-shaped parameters:
//!
//! - [`ChannelSseSource`]: connections are `SseOpen` driver jobs. SSE is
//!   deliberately NOT an [`HttpTransport`](crate::HttpTransport) call —
//!   the transport contract is full-body buffering; streaming gets its own
//!   job type, and the JS `ReadableStream` stays inside the driver.
//! - [`DriverSleeper`]: reconnect delays ride the driver's `Sleep` job
//!   (the engine side has no timer of its own).
//! - [`PeerHeadSink`]: ledger head changes go to `LedgerManager::notify`
//!   — the same incremental refresh the native peer applies, so an open
//!   peer re-opens at the new head on its next `db()`; in-flight queries
//!   keep their frozen views — and then to the registered callbacks.
//!
//! Retracted ledgers are evicted from the cache (`disconnect_ledger`);
//! graph-source events are logged and ignored (no graph sources in the
//! v1 browser peer).

use crate::bridge::{IoHandle, TokenCell};
use crate::protocol::{IoJob, SseHeaders};
use async_trait::async_trait;
use bytes::Bytes;
use fluree_db_api::{Fluree, NotifyResult, NsNotify};
use fluree_db_nameservice_sync::{
    BoxChunkStream, HeadSink, RemoteEvent, Sleeper, SseChunkSource, SseConnectError,
};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// A ledger head change, as delivered to [`on_head_change`] callbacks.
///
/// [`on_head_change`]: crate::BrowserPeer::on_head_change
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadChange {
    pub ledger_id: String,
    pub commit_t: i64,
    pub index_t: i64,
}

type HeadCallback = Box<dyn Fn(&HeadChange) + Send + Sync>;

/// Registered head-change callbacks. Callbacks run on the pump's task and
/// must not block or re-register (invoked under the registry lock).
#[derive(Default)]
pub(crate) struct HeadRegistry {
    callbacks: Mutex<Vec<HeadCallback>>,
}

impl HeadRegistry {
    pub(crate) fn add(&self, callback: HeadCallback) {
        self.callbacks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(callback);
    }

    fn notify(&self, change: &HeadChange) {
        for callback in self
            .callbacks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            callback(change);
        }
    }
}

impl std::fmt::Debug for HeadRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self
            .callbacks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        f.debug_struct("HeadRegistry")
            .field("callbacks", &n)
            .finish()
    }
}

/// Stops a head-tracking pump. Dropping the tracker also stops it (the
/// pump treats a dropped stop sender as stop).
#[derive(Debug)]
pub struct HeadTracker {
    stop: watch::Sender<bool>,
}

impl HeadTracker {
    pub(crate) fn new(stop: watch::Sender<bool>) -> Self {
        Self { stop }
    }

    /// Ask the pump to stop at its next await point.
    pub fn stop(&self) {
        let _ = self.stop.send(true);
    }
}

/// [`SseChunkSource`] over the driver's `SseOpen` job: each connect opens
/// a fetch-streamed SSE request inside the driver and hands the chunks
/// back over a channel (Send futures on the engine side, JS handles on the
/// driver side).
///
/// The bearer is resolved from the [`TokenCell`] PER CONNECT — the same
/// semantics as the native `ReqwestSseSource`'s per-connect token
/// provider — so a reconnect after [`TokenCell::set`] carries the fresh
/// token.
#[derive(Debug, Clone)]
pub struct ChannelSseSource {
    io: IoHandle,
    url: String,
    bearer: Option<TokenCell>,
}

impl ChannelSseSource {
    /// A source for `url`; when a cell is given, its current token is sent
    /// as the `authorization` header on each connect attempt.
    pub fn new(io: IoHandle, url: String, bearer: Option<TokenCell>) -> Self {
        Self { io, url, bearer }
    }

    fn headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![("accept", "text/event-stream".to_string())];
        if let Some(cell) = &self.bearer {
            headers.push(("authorization", cell.bearer_header()));
        }
        headers
    }
}

#[async_trait]
impl SseChunkSource for ChannelSseSource {
    async fn connect(&self) -> Result<BoxChunkStream, SseConnectError> {
        let (ready, ready_rx) = oneshot::channel();
        // Bounded so a server flooding the SSE stream backpressures the
        // driver's read rather than growing memory without limit; head-change
        // events are small and infrequent, so this depth is never reached in
        // normal operation.
        const SSE_CHUNK_CHANNEL_DEPTH: usize = 256;
        let (chunk_tx, chunk_rx) = mpsc::channel::<Result<Bytes, String>>(SSE_CHUNK_CHANNEL_DEPTH);
        self.io
            .send(IoJob::SseOpen {
                url: self.url.clone(),
                headers: SseHeaders(self.headers()),
                ready,
                chunks: chunk_tx,
            })
            .map_err(|_| SseConnectError::Fatal("browser I/O driver is not running".to_string()))?;
        ready_rx.await.map_err(|_| {
            SseConnectError::Retryable("driver dropped the SSE request".to_string())
        })??;
        Ok(Box::pin(futures::stream::unfold(
            chunk_rx,
            |mut rx| async move { rx.recv().await.map(|item| (item, rx)) },
        )))
    }
}

/// [`Sleeper`] over the driver's `Sleep` job.
#[derive(Debug, Clone)]
pub struct DriverSleeper {
    io: IoHandle,
}

impl DriverSleeper {
    pub fn new(io: IoHandle) -> Self {
        Self { io }
    }
}

#[async_trait]
impl Sleeper for DriverSleeper {
    async fn sleep(&self, duration: Duration) {
        let (reply, rx) = oneshot::channel();
        if self.io.send(IoJob::Sleep { duration, reply }).is_err() {
            // No driver, no timer; the pump's next connect will fail fatal.
            return;
        }
        let _ = rx.await;
    }
}

/// Waits between attempts to refresh a cached ledger after a head change.
/// When every attempt fails the ledger is evicted instead.
const REFRESH_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(250),
    Duration::from_secs(1),
    Duration::from_secs(4),
];

/// The browser peer's [`HeadSink`]: ledger updates refresh the cached
/// ledger through `LedgerManager::notify` (between queries — a query in
/// flight keeps its frozen view) and fan out to registered callbacks;
/// retractions evict the cached ledger.
pub(crate) struct PeerHeadSink {
    pub(crate) fluree: Fluree,
    pub(crate) registry: std::sync::Arc<HeadRegistry>,
    pub(crate) sleeper: std::sync::Arc<dyn Sleeper>,
}

impl PeerHeadSink {
    /// Bring the cached ledger to `record`'s head. A refresh that keeps
    /// failing evicts the ledger: callbacks fire after this returns, and a
    /// callback's re-query must load the new head (or fail visibly), not
    /// read the state the failed refresh left behind.
    async fn refresh(&self, record: fluree_db_nameservice::NsRecord) {
        let Some(mgr) = self.fluree.ledger_manager() else {
            return;
        };
        let ledger_id = record.ledger_id.clone();
        let mut delays = REFRESH_RETRY_DELAYS.iter();
        loop {
            let notify = NsNotify {
                ledger_id: ledger_id.clone(),
                record: Some(record.clone()),
            };
            match mgr.notify(notify).await {
                Ok(NotifyResult::NotLoaded | NotifyResult::Current) => return,
                Ok(result) => {
                    tracing::info!(ledger_id = %ledger_id, ?result, "refreshed cached ledger from head stream");
                    return;
                }
                Err(e) => match delays.next() {
                    Some(delay) => {
                        tracing::debug!(ledger_id = %ledger_id, error = %e, ?delay, "retrying cached ledger refresh");
                        self.sleeper.sleep(*delay).await;
                    }
                    None => {
                        tracing::warn!(ledger_id = %ledger_id, error = %e, "could not refresh cached ledger from head stream; evicting it");
                        self.fluree.disconnect_ledger(ledger_id.as_str()).await;
                        return;
                    }
                },
            }
        }
    }
}

#[async_trait]
impl HeadSink for PeerHeadSink {
    async fn on_event(&self, event: RemoteEvent) {
        match event {
            RemoteEvent::Connected => {
                tracing::info!("head tracking connected");
            }
            RemoteEvent::LedgerUpdated(record) => {
                let change = HeadChange {
                    ledger_id: record.ledger_id.to_string(),
                    commit_t: record.commit_t,
                    index_t: record.index_t,
                };
                self.refresh(record).await;
                self.registry.notify(&change);
            }
            RemoteEvent::LedgerRetracted { ledger_id } => {
                tracing::info!(ledger_id = %ledger_id, "ledger retracted on the remote");
                self.fluree.disconnect_ledger(&ledger_id).await;
            }
            RemoteEvent::GraphSourceUpdated(record) => {
                tracing::debug!(graph_source_id = %record.graph_source_id, "graph-source head event ignored (unsupported in the browser peer)");
            }
            RemoteEvent::GraphSourceRetracted { graph_source_id } => {
                tracing::debug!(graph_source_id = %graph_source_id, "graph-source retraction ignored");
            }
            RemoteEvent::Disconnected { reason } => {
                tracing::warn!(reason = %reason, "head tracking disconnected; will reconnect");
            }
            RemoteEvent::Fatal { reason } => {
                tracing::error!(reason = %reason, "head tracking stopped (fatal)");
            }
        }
    }
}

/// Build the events URL for a subscription. An empty ledger list
/// subscribes to everything the token can see.
pub(crate) fn events_url(api_base: &str, ledgers: &[String]) -> String {
    let mut url = format!("{api_base}/events");
    if ledgers.is_empty() {
        url.push_str("?all=true");
    } else {
        let params: Vec<String> = ledgers
            .iter()
            .map(|l| format!("ledger={}", urlencoding::encode(l)))
            .collect();
        url.push('?');
        url.push_str(&params.join("&"));
    }
    url
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// The bearer is read from the cell on EVERY connect (native
    /// `ReqwestSseSource` parity): a reconnect after a token refresh
    /// carries the fresh token.
    #[tokio::test]
    async fn sse_source_resolves_the_token_per_connect() {
        use crate::cas::tests::{spawn_mock_driver, MockState};

        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            s.sse_script.push_back(Vec::new());
            s.sse_script.push_back(Vec::new());
        }
        let (io, rx) = IoHandle::channel();
        let driver = spawn_mock_driver(rx, Arc::clone(&state));

        let cell = TokenCell::new("a");
        let source = ChannelSseSource::new(
            io.clone(),
            "http://origin.example/v1/fluree/events?all=true".to_string(),
            Some(cell.clone()),
        );

        let _first = source.connect().await.expect("first connect");
        cell.set("b");
        let _second = source.connect().await.expect("second connect");

        {
            let s = state.lock().unwrap();
            let bearers: Vec<String> = s
                .sse_log
                .iter()
                .map(|(_, headers)| {
                    headers
                        .iter()
                        .find(|(name, _)| *name == "authorization")
                        .map(|(_, value)| value.clone())
                        .expect("connect must send a bearer")
                })
                .collect();
            assert_eq!(bearers, vec!["Bearer a", "Bearer b"]);
        }

        io.shutdown();
        driver.await.unwrap();
    }

    /// Records each requested delay and returns at once.
    #[derive(Default)]
    struct RecordingSleeper(Mutex<Vec<Duration>>);

    #[async_trait]
    impl Sleeper for RecordingSleeper {
        async fn sleep(&self, duration: Duration) {
            self.0.lock().unwrap().push(duration);
        }
    }

    const LEDGER: &str = "heads/test:main";

    /// A sink over a seeded, cached ledger, and the changes its callbacks saw.
    async fn cached_ledger_sink() -> (
        PeerHeadSink,
        Arc<RecordingSleeper>,
        Arc<Mutex<Vec<HeadChange>>>,
    ) {
        let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
        fluree.create_ledger(LEDGER).await.expect("create");
        let tx = serde_json::json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@id": "ex:alice", "ex:name": "Alice"
        });
        fluree
            .graph(LEDGER)
            .transact()
            .insert(&tx)
            .commit()
            .await
            .expect("seed");
        fluree.ledger(LEDGER).await.expect("cache");

        let seen = Arc::new(Mutex::new(Vec::new()));
        let registry = Arc::new(HeadRegistry::default());
        {
            let seen = Arc::clone(&seen);
            registry.add(Box::new(move |c| seen.lock().unwrap().push(c.clone())));
        }
        let sleeper = Arc::new(RecordingSleeper::default());
        let sink = PeerHeadSink {
            fluree,
            registry,
            sleeper: sleeper.clone(),
        };
        (sink, sleeper, seen)
    }

    async fn head_record(sink: &PeerHeadSink) -> fluree_db_nameservice::NsRecord {
        sink.fluree
            .nameservice()
            .lookup(LEDGER)
            .await
            .expect("lookup")
            .expect("record")
    }

    async fn cached_t(sink: &PeerHeadSink) -> Option<i64> {
        let id = fluree_db_api::LedgerId::parse(LEDGER).expect("ledger id");
        sink.fluree.ledger_manager().unwrap().current_t(&id).await
    }

    /// A head change the cached ledger cannot reach — here a commit that is
    /// not in storage — is retried, then the ledger is evicted BEFORE the
    /// callbacks fire, so a callback's re-query loads the new head instead
    /// of reading the old state as if it were current.
    #[tokio::test]
    async fn an_unreachable_head_evicts_the_ledger_before_callbacks_fire() {
        let (sink, sleeper, seen) = cached_ledger_sink().await;
        let mut record = head_record(&sink).await;
        let t = record.commit_t;
        assert_eq!(cached_t(&sink).await, Some(t), "fixture: ledger cached");

        record.commit_t = t + 1;
        record.commit_head_id = Some(fluree_db_core::ContentId::new(
            fluree_db_core::ContentKind::Commit,
            b"a commit storage never saw",
        ));
        sink.on_event(RemoteEvent::LedgerUpdated(record)).await;

        assert_eq!(*sleeper.0.lock().unwrap(), REFRESH_RETRY_DELAYS.to_vec());
        assert_eq!(cached_t(&sink).await, None, "the stale ledger was evicted");
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "callbacks still fire");
        assert_eq!(seen[0].commit_t, t + 1);
    }

    /// A refresh that succeeds does not wait and keeps the ledger cached.
    #[tokio::test]
    async fn a_reachable_head_keeps_the_ledger_cached() {
        let (sink, sleeper, seen) = cached_ledger_sink().await;
        let record = head_record(&sink).await;
        let t = record.commit_t;
        sink.on_event(RemoteEvent::LedgerUpdated(record)).await;

        assert!(sleeper.0.lock().unwrap().is_empty(), "no retry");
        assert_eq!(cached_t(&sink).await, Some(t));
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn events_url_encodes_subscriptions() {
        assert_eq!(
            events_url("http://x/v1/fluree", &[]),
            "http://x/v1/fluree/events?all=true"
        );
        assert_eq!(
            events_url(
                "http://x/v1/fluree",
                &["books:main".to_string(), "a/b:dev".to_string()]
            ),
            "http://x/v1/fluree/events?ledger=books%3Amain&ledger=a%2Fb%3Adev"
        );
    }
}
