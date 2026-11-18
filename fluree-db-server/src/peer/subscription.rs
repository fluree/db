//! SSE subscription task for peer mode, in either storage access mode
//!
//! Drives the shared head-stream pump
//! ([`fluree_db_nameservice_sync::run_head_stream`]) over the transaction
//! server's `/fluree/events` endpoint and applies its events to the peer
//! state and the library-level ledger cache. A peer writes nothing: on
//! shared storage the nameservice is the transaction server's own, and the
//! peer only reads it. The pump owns the loop shape
//! (connect, parse, dispatch, reconnect with the configured backoff,
//! 401/403 fatal); this module owns what the events *mean* for a peer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::config::ServerConfig;
use crate::peer::state::PeerState;

use async_trait::async_trait;
use fluree_db_api::{Fluree, LedgerId, NotifyResult};
use fluree_db_nameservice::NsRecord;
use fluree_db_nameservice_sync::{
    run_head_stream, HeadSink, HeadStreamConfig, RemoteEvent, ReqwestSseSource, TokioSleeper,
};

/// Background task that maintains SSE subscription to transaction server
pub struct PeerSubscriptionTask {
    config: ServerConfig,
    peer_state: Arc<PeerState>,
    fluree: Arc<Fluree>,
}

impl PeerSubscriptionTask {
    pub fn new(config: ServerConfig, peer_state: Arc<PeerState>, fluree: Arc<Fluree>) -> Self {
        Self {
            config,
            peer_state,
            fluree,
        }
    }

    /// Spawn the subscription task
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&self) {
        let url = self.build_events_url();
        tracing::info!(url = %url, "Connecting to transaction server events");

        // Token resolution happens per connect attempt so a rotated
        // on-disk token is picked up at the next reconnect; a load failure
        // is fatal (matches the historical taxonomy).
        let token_config = self.config.clone();
        let source =
            ReqwestSseSource::new(url, Arc::new(move || token_config.load_peer_events_token()));
        let sink = PeerEventSink::new(
            self.config.clone(),
            Arc::clone(&self.peer_state),
            Arc::clone(&self.fluree),
        );
        let stream_config = HeadStreamConfig {
            reconnect_initial: Duration::from_millis(self.config.peer_reconnect_initial_ms),
            reconnect_max: Duration::from_millis(self.config.peer_reconnect_max_ms),
            reconnect_multiplier: self.config.peer_reconnect_multiplier,
        };
        // The task runs for the server's lifetime; the stop sender is held
        // here so the pump ends when the task future is dropped.
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        run_head_stream(&source, &sink, &TokioSleeper, stream_config, stop_rx).await;
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

/// Most ledgers whose records are applied at once.
const CONCURRENT_LEDGER_UPDATES: usize = 8;

/// Applies head-stream events to peer state, the ledger cache, and logs —
/// the peer-mode meaning of each event.
struct PeerEventSink {
    config: ServerConfig,
    peer_state: Arc<PeerState>,
    fluree: Arc<Fluree>,
    updates: Arc<LedgerUpdates>,
}

/// The ledger updates waiting to be applied. A ledger's updates are applied
/// one at a time, in stream order, by a worker of its own, so one waiting on
/// slow storage holds up no other ledger's. An update that arrives while its
/// ledger's worker is busy replaces any still waiting: the latest supersedes
/// them.
struct LedgerUpdates {
    /// A ledger with an entry has a worker; the entry is the update it
    /// applies next.
    waiting: std::sync::Mutex<HashMap<LedgerId, Option<LedgerUpdate>>>,
    permits: tokio::sync::Semaphore,
}

enum LedgerUpdate {
    /// A record received in connection `epoch` (see [`PeerState::epoch`]).
    Record { epoch: u64, record: Box<NsRecord> },
    /// A retraction received in connection `epoch`.
    Retracted { epoch: u64 },
}

impl PeerEventSink {
    fn new(config: ServerConfig, peer_state: Arc<PeerState>, fluree: Arc<Fluree>) -> Self {
        Self {
            config,
            peer_state,
            fluree,
            updates: Arc::new(LedgerUpdates {
                waiting: std::sync::Mutex::new(HashMap::new()),
                permits: tokio::sync::Semaphore::new(CONCURRENT_LEDGER_UPDATES),
            }),
        }
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

    /// Queue `update` for `ledger_id`'s worker, starting one if it has none.
    fn queue(&self, ledger_id: LedgerId, update: LedgerUpdate) {
        let start = {
            let mut waiting = self.updates.waiting.lock().expect("never poisoned");
            match waiting.get_mut(&ledger_id) {
                Some(next) => {
                    *next = Some(update);
                    false
                }
                None => {
                    waiting.insert(ledger_id.clone(), Some(update));
                    true
                }
            }
        };
        if start {
            tokio::spawn(apply_ledger_updates(
                Arc::clone(&self.updates),
                Arc::clone(&self.fluree),
                Arc::clone(&self.peer_state),
                ledger_id,
            ));
        }
    }
}

/// Whether the nameservice has no ledger `ledger_id`, or has it retracted.
async fn ledger_is_gone(fluree: &Fluree, ledger_id: &LedgerId) -> bool {
    matches!(
        fluree.nameservice().lookup(ledger_id.as_str()).await,
        Ok(None
            | Some(NsRecord {
                retracted: true,
                ..
            }))
    )
}

/// A ledger's worker: applies its waiting updates until none is left.
async fn apply_ledger_updates(
    updates: Arc<LedgerUpdates>,
    fluree: Arc<Fluree>,
    peer_state: Arc<PeerState>,
    ledger_id: LedgerId,
) {
    loop {
        let update = {
            let mut waiting = updates.waiting.lock().expect("never poisoned");
            match waiting.get_mut(&ledger_id).and_then(Option::take) {
                Some(update) => update,
                None => {
                    waiting.remove(&ledger_id);
                    return;
                }
            }
        };
        let _permit = updates.permits.acquire().await.expect("never closed");
        let (epoch, record) = match update {
            LedgerUpdate::Record { epoch, record } => (epoch, record),
            LedgerUpdate::Retracted { epoch } => {
                // Evict any cached state for the ledger (no-op if not cached).
                let evict = fluree.disconnect_ledger(&ledger_id);
                if peer_state.retract_ledger_in(epoch, &ledger_id, evict).await {
                    tracing::info!(ledger_id = %ledger_id, "Ledger retracted from remote");
                } else if ledger_is_gone(&fluree, &ledger_id).await {
                    // From an earlier connection, whose records the new one's
                    // replace. Those cannot say a ledger is gone, so the
                    // nameservice does; a ledger it still has (re-created
                    // since, or loading) stays.
                    fluree.disconnect_ledger(&ledger_id).await;
                    tracing::info!(ledger_id = %ledger_id, "Ledger retracted before a reconnect");
                }
                continue;
            }
        };
        // The cached ledger first: a query that sees the new watermark over a
        // cached ledger behind it reloads it whole.
        refresh_cached_ledger(&fluree, &record).await;
        let changed = peer_state
            .update_ledger_in(
                epoch,
                &record.ledger_id,
                record.commit_t,
                record.index_t,
                record.commit_head_id.as_ref().map(ToString::to_string),
                record.index_head_id.as_ref().map(ToString::to_string),
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
    }
}

/// Bring a ledger this peer has cached up to the record, in place. A ledger
/// it has not cached is left to load on its first query.
async fn refresh_cached_ledger(fluree: &Fluree, record: &NsRecord) {
    let Some(mgr) = fluree.ledger_manager() else {
        return;
    };
    let ledger_id = &record.ledger_id;
    match mgr.follow(record.clone()).await {
        // Not cached - do not cold-load on events (avoids subscribe-all stampede).
        Ok(NotifyResult::NotLoaded | NotifyResult::Current) => {}
        Ok(
            result @ (NotifyResult::Reloaded
            | NotifyResult::IndexUpdated
            | NotifyResult::CommitsApplied { .. }),
        ) => {
            tracing::info!(ledger_id = %ledger_id, ?result, "Refreshed cached ledger from SSE update");
        }
        Err(e) => {
            tracing::warn!(ledger_id = %ledger_id, error = %e, "Failed to refresh cached ledger from SSE update");
        }
    }
}

/// 8-hex-char config fingerprint (matches the server's `sha256_short` used
/// to build graph-source SSE event ids).
fn config_hash(config: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(config.as_bytes());
    hex::encode(&hash[..4])
}

#[async_trait]
impl HeadSink for PeerEventSink {
    async fn on_event(&self, event: RemoteEvent) {
        match event {
            RemoteEvent::Connected => {
                // Clear state on reconnect (new snapshot coming).
                self.peer_state.clear().await;
                self.peer_state.set_connected(true).await;
                tracing::info!("Connected to transaction server, receiving snapshot");

                // Optional: preload explicitly configured ledgers so the
                // peer starts "warm". We intentionally do NOT preload on
                // subscribe-all to avoid accidentally loading a large
                // number of ledgers.
                self.preload_configured_ledgers().await;
            }
            RemoteEvent::LedgerUpdated(record) => {
                let epoch = self.peer_state.epoch();
                self.queue(
                    record.ledger_id.clone(),
                    LedgerUpdate::Record {
                        epoch,
                        record: Box::new(record),
                    },
                );
            }
            RemoteEvent::GraphSourceUpdated(record) => {
                let changed = self
                    .peer_state
                    .update_graph_source(
                        &record.graph_source_id,
                        record.index_t,
                        config_hash(&record.config),
                        record.index_id.as_ref().map(ToString::to_string),
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
            // Applied after any update to the ledger already in flight.
            RemoteEvent::LedgerRetracted { ledger_id } => {
                let epoch = self.peer_state.epoch();
                self.queue(ledger_id, LedgerUpdate::Retracted { epoch });
            }
            RemoteEvent::GraphSourceRetracted { graph_source_id } => {
                self.peer_state.remove_graph_source(&graph_source_id).await;
                tracing::info!(graph_source_id = %graph_source_id, "Graph source retracted from remote");
            }
            RemoteEvent::Disconnected { reason } => {
                self.peer_state.set_connected(false).await;
                tracing::warn!(reason = %reason, "Peer SSE disconnected, will reconnect");
            }
            RemoteEvent::Fatal { reason } => {
                self.peer_state.set_connected(false).await;
                tracing::error!(reason = %reason, "Fatal peer subscription error, will not retry");
            }
        }
    }
}

/// Historical error taxonomy for peer subscriptions. The connection loop
/// now lives in the shared head-stream pump (which classifies 401/403 and
/// token-load failures as fatal exactly as [`is_fatal`](Self::is_fatal)
/// did); the type is kept for API compatibility.
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

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::{FlureeBuilder, LedgerId};
    use serde_json::json;

    /// A transaction server's root and a shared-storage peer reading it.
    struct SharedRoot {
        _dir: tempfile::TempDir,
        writer: Fluree,
        peer: Arc<Fluree>,
        sink: PeerEventSink,
    }

    impl SharedRoot {
        async fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_string_lossy().to_string();
            let writer = FlureeBuilder::file(path.clone())
                .without_indexing()
                .build()
                .unwrap();
            let peer = Arc::new(
                FlureeBuilder::file(path)
                    .read_only()
                    .build_client()
                    .await
                    .unwrap(),
            );
            let sink = PeerEventSink::new(
                ServerConfig::default(),
                Arc::new(PeerState::new()),
                Arc::clone(&peer),
            );
            Self {
                _dir: dir,
                writer,
                peer,
                sink,
            }
        }

        async fn commit(&self, n: usize) -> NsRecord {
            let ledger = match self.writer.ledger("peer:main").await {
                Ok(ledger) => ledger,
                Err(_) => self.writer.create_ledger("peer:main").await.unwrap(),
            };
            self.writer
                .insert(
                    ledger,
                    &json!({"@context": {"ex": "http://example.org/"},
                            "@id": format!("ex:s{n}"), "ex:p": n}),
                )
                .await
                .unwrap();
            self.record().await
        }

        async fn record(&self) -> NsRecord {
            self.writer
                .nameservice()
                .lookup("peer:main")
                .await
                .unwrap()
                .unwrap()
        }

        /// Wait until every ledger's worker has applied its records.
        async fn settled(&self) {
            settled(&self.sink).await;
        }

        async fn cached_t(&self) -> Option<i64> {
            self.peer
                .ledger_manager()
                .unwrap()
                .current_t(&LedgerId::parse("peer:main").unwrap())
                .await
        }
    }

    /// The peer has already read past the record: it keeps its cached
    /// ledger, and the transaction server's record is not moved back.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_record_the_peer_has_passed_changes_nothing() {
        let root = SharedRoot::new().await;
        let behind = root.commit(1).await;
        let head = root.commit(2).await;
        root.peer.ledger_cached("peer:main").await.unwrap();

        root.sink.on_event(RemoteEvent::LedgerUpdated(behind)).await;
        root.settled().await;

        assert_eq!(root.cached_t().await, Some(head.commit_t), "still cached");
        let after = root.record().await;
        assert_eq!(
            (after.commit_head_id, after.commit_t),
            (head.commit_head_id, head.commit_t)
        );
    }

    /// A new commit is applied to the cached ledger in place: it is not
    /// evicted, so no query waits on a cold load.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_new_commit_is_applied_to_the_cached_ledger() {
        let root = SharedRoot::new().await;
        root.commit(1).await;
        root.peer.ledger_cached("peer:main").await.unwrap();
        let next = root.commit(2).await;

        root.sink
            .on_event(RemoteEvent::LedgerUpdated(next.clone()))
            .await;
        root.settled().await;

        assert_eq!(root.cached_t().await, Some(next.commit_t));
        let watermark = root
            .sink
            .peer_state
            .get_remote_ledger(&next.ledger_id)
            .await
            .unwrap();
        assert_eq!(watermark.commit_t, next.commit_t);
    }

    /// Every file under `dir`, with its length and modification time.
    fn files_under(
        dir: &std::path::Path,
    ) -> std::collections::BTreeMap<std::path::PathBuf, (u64, std::time::SystemTime)> {
        let mut files = std::collections::BTreeMap::new();
        let mut pending = vec![dir.to_path_buf()];
        while let Some(next) = pending.pop() {
            for entry in std::fs::read_dir(&next).unwrap() {
                let entry = entry.unwrap();
                let meta = entry.metadata().unwrap();
                if meta.is_dir() {
                    pending.push(entry.path());
                } else {
                    let path = entry.path().strip_prefix(dir).unwrap().to_path_buf();
                    files.insert(path, (meta.len(), meta.modified().unwrap()));
                }
            }
        }
        files
    }

    /// Wait until the writer's commit-index line for `t` is on disk: the
    /// nameservice appends it after the publish returns, without the
    /// committer waiting on it.
    async fn commit_index_lists(dir: &std::path::Path, t: i64) {
        let index = dir.join("ns@v2/peer/main.commits.jsonl");
        let line = format!("\"t\":{t},");
        for _ in 0..500 {
            if std::fs::read_to_string(&index).is_ok_and(|s| s.contains(&line)) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the writer never listed t {t} in its commit index");
    }

    /// A shared-storage peer writes nothing under the root: not as it opens
    /// it, loads a ledger, or applies a new commit to the cached ledger.
    /// Each snapshot is taken once the writer's own commit has settled.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_peer_writes_nothing_under_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let writer = FlureeBuilder::file(path.clone())
            .without_indexing()
            .build()
            .unwrap();
        let commit = |n: usize| {
            let writer = &writer;
            async move {
                let ledger = match writer.ledger("peer:main").await {
                    Ok(ledger) => ledger,
                    Err(_) => writer.create_ledger("peer:main").await.unwrap(),
                };
                writer
                    .insert(
                        ledger,
                        &json!({"@context": {"ex": "http://example.org/"},
                                "@id": format!("ex:s{n}"), "ex:p": n}),
                    )
                    .await
                    .unwrap();
                writer
                    .nameservice()
                    .lookup("peer:main")
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let first = commit(1).await;
        commit_index_lists(dir.path(), first.commit_t).await;

        let before = files_under(dir.path());
        let peer = Arc::new(
            FlureeBuilder::file(path)
                .read_only()
                .build_client()
                .await
                .unwrap(),
        );
        let sink = PeerEventSink::new(
            ServerConfig::default(),
            Arc::new(PeerState::new()),
            Arc::clone(&peer),
        );
        sink.on_event(RemoteEvent::Connected).await;
        peer.ledger_cached("peer:main").await.unwrap();
        assert_eq!(files_under(dir.path()), before, "opening and loading");

        let next = commit(2).await;
        commit_index_lists(dir.path(), next.commit_t).await;
        let before = files_under(dir.path());
        sink.on_event(RemoteEvent::LedgerUpdated(next.clone()))
            .await;
        settled(&sink).await;
        let cached_t = peer
            .ledger_manager()
            .unwrap()
            .current_t(&LedgerId::parse("peer:main").unwrap())
            .await;
        assert_eq!(cached_t, Some(next.commit_t), "the commit was applied");
        assert_eq!(files_under(dir.path()), before, "applying a commit");
    }

    /// A shared-storage peer opens the root as a reader.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_peer_cannot_write_the_nameservice() {
        let root = SharedRoot::new().await;
        assert!(root.peer.nameservice_mode().is_read_only());
        assert!(root.peer.nameservice_mode().publisher().is_none());
    }

    async fn settled(sink: &PeerEventSink) {
        for _ in 0..500 {
            if sink.updates.waiting.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the ledger workers did not finish");
    }

    /// A nameservice whose lookup of one ledger, once armed, waits to be
    /// released: storage that is slow for that ledger alone.
    #[derive(Debug)]
    struct SlowLedger {
        inner: Arc<dyn fluree_db_nameservice::NameServiceLookup>,
        ledger_id: &'static str,
        armed: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    mod slow_ledger {
        use super::SlowLedger;
        use async_trait::async_trait;
        use fluree_db_nameservice::{
            ConfigLookup, ConfigValue, GraphSourceLookup, GraphSourceRecord, LedgerHeads,
            NameServiceLookup, NsLookupResult, NsRecord, RefKind, RefLookup, RefValue, Result,
            StatusLookup, StatusValue,
        };
        use std::sync::atomic::Ordering;

        #[async_trait]
        impl GraphSourceLookup for SlowLedger {
            async fn lookup_graph_source(&self, id: &str) -> Result<Option<GraphSourceRecord>> {
                self.inner.lookup_graph_source(id).await
            }
            async fn lookup_any(&self, id: &str) -> Result<NsLookupResult> {
                self.inner.lookup_any(id).await
            }
            async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
                self.inner.all_graph_source_records().await
            }
        }

        #[async_trait]
        impl RefLookup for SlowLedger {
            async fn get_ref(&self, id: &str, kind: RefKind) -> Result<Option<RefValue>> {
                self.inner.get_ref(id, kind).await
            }
        }

        #[async_trait]
        impl StatusLookup for SlowLedger {
            async fn get_status(&self, id: &str) -> Result<Option<StatusValue>> {
                self.inner.get_status(id).await
            }
        }

        #[async_trait]
        impl ConfigLookup for SlowLedger {
            async fn get_config(&self, id: &str) -> Result<Option<ConfigValue>> {
                self.inner.get_config(id).await
            }
        }

        #[async_trait]
        impl NameServiceLookup for SlowLedger {
            async fn lookup(&self, id: &str) -> Result<Option<NsRecord>> {
                if id == self.ledger_id && self.armed.swap(false, Ordering::SeqCst) {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                self.inner.lookup(id).await
            }
            async fn all_records(&self) -> Result<Vec<NsRecord>> {
                self.inner.all_records().await
            }
            async fn branch_records(&self, name: &str) -> Result<Vec<NsRecord>> {
                self.inner.branch_records(name).await
            }
            async fn heads(&self, id: &str) -> Result<Option<LedgerHeads>> {
                self.inner.heads(id).await
            }
        }
    }

    /// A writer with ledgers `a:main` and `b:main`, and a peer on its root
    /// whose next load of `a:main`, once armed, waits to be released.
    struct TwoLedgers {
        _dir: tempfile::TempDir,
        writer: Fluree,
        peer: Arc<Fluree>,
        slow: Arc<SlowLedger>,
        sink: PeerEventSink,
    }

    impl TwoLedgers {
        async fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_path_buf();
            let writer = FlureeBuilder::file(path.to_string_lossy().to_string())
                .without_indexing()
                .build()
                .unwrap();
            let storage = || {
                fluree_db_core::FileStorage::new(&path)
                    .with_durability(fluree_db_core::Durability::Sync)
                    .without_wal_replay()
            };
            let slow = Arc::new(SlowLedger {
                inner: Arc::new(fluree_db_nameservice::file::FileNameService::with_storage(
                    storage(),
                )),
                ledger_id: "a:main",
                armed: false.into(),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let peer = Arc::new(FlureeBuilder::memory().build_with(
                storage(),
                fluree_db_api::NameServiceMode::ReadOnly(Arc::clone(&slow) as _),
            ));
            let sink = PeerEventSink::new(
                ServerConfig::default(),
                Arc::new(PeerState::new()),
                Arc::clone(&peer),
            );
            let root = Self {
                _dir: dir,
                writer,
                peer,
                slow,
                sink,
            };
            for ledger in ["a:main", "b:main"] {
                root.commit(ledger, 0).await;
            }
            root
        }

        async fn commit(&self, ledger_id: &str, n: usize) -> NsRecord {
            let ledger = match self.writer.ledger(ledger_id).await {
                Ok(ledger) => ledger,
                Err(_) => self.writer.create_ledger(ledger_id).await.unwrap(),
            };
            self.writer
                .insert(
                    ledger,
                    &json!({"@context": {"ex": "http://example.org/"},
                            "@id": format!("ex:s{n}"), "ex:p": n}),
                )
                .await
                .unwrap();
            self.writer
                .nameservice()
                .lookup(ledger_id)
                .await
                .unwrap()
                .unwrap()
        }

        async fn cached_t(&self, ledger_id: &str) -> Option<i64> {
            self.peer
                .ledger_manager()
                .unwrap()
                .current_t(&LedgerId::parse(ledger_id).unwrap())
                .await
        }

        fn waiting_for_a(&self) -> Option<Option<i64>> {
            self.sink
                .updates
                .waiting
                .lock()
                .unwrap()
                .get(&LedgerId::parse("a:main").unwrap())
                .map(|next| match next {
                    Some(LedgerUpdate::Record { record, .. }) => Some(record.commit_t),
                    Some(LedgerUpdate::Retracted { .. }) => Some(-1),
                    None => None,
                })
        }

        /// Start a load of `a:main` that waits until released.
        async fn stall_a(&self) -> tokio::task::JoinHandle<()> {
            self.slow
                .armed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let peer = Arc::clone(&self.peer);
            let load = tokio::spawn(async move {
                peer.ledger_cached("a:main").await.unwrap();
            });
            self.slow.entered.notified().await;
            load
        }
    }

    /// A ledger whose load is stuck on storage holds up no other ledger's
    /// records, and the event stream goes on.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_ledger_waiting_on_storage_holds_up_no_other() {
        let root = TwoLedgers::new().await;
        root.peer.ledger_cached("b:main").await.unwrap();
        let load = root.stall_a().await;

        let a = root.commit("a:main", 1).await;
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            root.sink.on_event(RemoteEvent::LedgerUpdated(a)),
        )
        .await
        .expect("the stream does not wait on a ledger's update");
        let b = root.commit("b:main", 1).await;
        root.sink
            .on_event(RemoteEvent::LedgerUpdated(b.clone()))
            .await;
        for _ in 0..200 {
            if root.cached_t("b:main").await == Some(b.commit_t) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(root.cached_t("b:main").await, Some(b.commit_t));
        assert_eq!(root.waiting_for_a(), Some(None), "a:main is still loading");

        root.slow.release.notify_one();
        load.await.unwrap();
        settled(&root.sink).await;
    }

    /// Records that arrive while their ledger's worker is busy wait as one:
    /// the latest replaces the others, and is the one applied.
    #[tokio::test(flavor = "multi_thread")]
    async fn records_behind_a_busy_worker_collapse_to_the_latest() {
        let root = TwoLedgers::new().await;
        let load = root.stall_a().await;

        let first = root.commit("a:main", 1).await;
        root.sink.on_event(RemoteEvent::LedgerUpdated(first)).await;
        // The worker takes the first record and waits on the load.
        for _ in 0..200 {
            if root.waiting_for_a() == Some(None) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(root.waiting_for_a(), Some(None));
        let second = root.commit("a:main", 2).await;
        let third = root.commit("a:main", 3).await;
        root.sink.on_event(RemoteEvent::LedgerUpdated(second)).await;
        root.sink
            .on_event(RemoteEvent::LedgerUpdated(third.clone()))
            .await;
        assert_eq!(
            root.waiting_for_a(),
            Some(Some(third.commit_t)),
            "only the latest record waits"
        );

        root.slow.release.notify_one();
        load.await.unwrap();
        settled(&root.sink).await;
        assert_eq!(root.cached_t("a:main").await, Some(third.commit_t));
    }

    async fn watermark_t(sink: &PeerEventSink, ledger_id: &str) -> Option<i64> {
        sink.peer_state
            .get_remote_ledger(&LedgerId::parse(ledger_id).unwrap())
            .await
            .map(|w| w.commit_t)
    }

    /// A retraction is applied after the ledger's update in flight, so that
    /// update cannot set the watermark again once the retraction removed it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retraction_follows_the_update_in_flight() {
        let root = TwoLedgers::new().await;
        let load = root.stall_a().await;
        let record = root.commit("a:main", 1).await;
        root.sink.on_event(RemoteEvent::LedgerUpdated(record)).await;
        for _ in 0..200 {
            if root.waiting_for_a() == Some(None) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        root.sink
            .on_event(RemoteEvent::LedgerRetracted {
                ledger_id: LedgerId::parse("a:main").unwrap(),
            })
            .await;

        root.slow.release.notify_one();
        load.await.unwrap();
        settled(&root.sink).await;
        assert_eq!(watermark_t(&root.sink, "a:main").await, None);
        assert_eq!(root.cached_t("a:main").await, None, "evicted");
    }

    /// A record from before a reconnect, applied after it, sets no
    /// watermark: the new connection's records do.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_record_from_before_a_reconnect_sets_no_watermark() {
        let root = TwoLedgers::new().await;
        let load = root.stall_a().await;
        let record = root.commit("a:main", 1).await;
        root.sink.on_event(RemoteEvent::LedgerUpdated(record)).await;
        root.sink.on_event(RemoteEvent::Connected).await;

        root.slow.release.notify_one();
        load.await.unwrap();
        settled(&root.sink).await;
        assert_eq!(watermark_t(&root.sink, "a:main").await, None);
    }

    /// A retraction from before a reconnect, of a ledger the transaction
    /// server did drop, still evicts it: the new connection's records cannot
    /// say the ledger is gone, so the nameservice does.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retraction_from_before_a_reconnect_evicts_a_dropped_ledger() {
        let root = TwoLedgers::new().await;
        root.peer.ledger_cached("a:main").await.unwrap();
        assert!(root.cached_t("a:main").await.is_some());

        let before = root.sink.peer_state.epoch();
        root.sink.on_event(RemoteEvent::Connected).await;
        root.writer
            .drop_ledger("a", fluree_db_api::DropMode::Soft)
            .await
            .unwrap();
        root.sink.queue(
            LedgerId::parse("a:main").unwrap(),
            LedgerUpdate::Retracted { epoch: before },
        );

        settled(&root.sink).await;
        assert_eq!(root.cached_t("a:main").await, None, "evicted");
    }

    /// A retraction from before a reconnect is not applied after it: the new
    /// connection's records say whether the ledger exists, and a load of it
    /// begun since is not evicted.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retraction_from_before_a_reconnect_is_not_applied() {
        let root = TwoLedgers::new().await;
        let load = root.stall_a().await;
        let record = root.commit("a:main", 1).await;
        root.sink.on_event(RemoteEvent::LedgerUpdated(record)).await;
        for _ in 0..200 {
            if root.waiting_for_a() == Some(None) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        root.sink
            .on_event(RemoteEvent::LedgerRetracted {
                ledger_id: LedgerId::parse("a:main").unwrap(),
            })
            .await;
        root.sink.on_event(RemoteEvent::Connected).await;

        root.slow.release.notify_one();
        load.await.unwrap();
        settled(&root.sink).await;
        assert!(root.cached_t("a:main").await.is_some(), "still cached");
    }
}
