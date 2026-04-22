//! Polling-based remote watch implementation
//!
//! Periodically calls `RemoteNameserviceClient::snapshot()`, diffs against
//! the previous state, and synthesizes `RemoteEvent`s for changes.
//! Used as a fallback when SSE is unavailable.

use crate::backoff::Backoff;
use crate::client::RemoteNameserviceClient;
use crate::watch::RemoteEvent;
use fluree_db_nameservice::{GraphSourceRecord, NsRecord};
use futures::Stream;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// Polling-based remote watch
#[derive(Debug)]
pub struct PollRemoteWatch {
    client: Arc<dyn RemoteNameserviceClient>,
    interval: Duration,
}

impl PollRemoteWatch {
    /// Create a new polling watch.
    ///
    /// `interval` is how often to poll the remote for changes.
    pub fn new(client: Arc<dyn RemoteNameserviceClient>, interval: Duration) -> Self {
        Self { client, interval }
    }
}

impl crate::watch::RemoteWatch for PollRemoteWatch {
    fn watch(&self) -> Pin<Box<dyn Stream<Item = RemoteEvent> + Send>> {
        let client = self.client.clone();
        let interval = self.interval;

        let stream = async_stream::stream! {
            let mut prev_ledgers: HashMap<String, NsRecord> = HashMap::new();
            let mut prev_graph_sources: HashMap<String, GraphSourceRecord> = HashMap::new();
            let mut backoff = Backoff::new(1000, 60_000);
            let mut connected = false;

            loop {
                match client.snapshot().await {
                    Ok(snapshot) => {
                        if !connected {
                            yield RemoteEvent::Connected;
                            connected = true;
                            backoff.reset();
                        }

                        // Diff ledgers
                        let mut current_ledgers: HashMap<String, NsRecord> = HashMap::new();
                        for record in snapshot.ledgers {
                            let key = record.ledger_id.clone();
                            if record.retracted {
                                if prev_ledgers.contains_key(&key) {
                                    yield RemoteEvent::LedgerRetracted { ledger_id: key.clone() };
                                }
                            } else if let Some(prev) = prev_ledgers.get(&key) {
                                if prev.commit_t != record.commit_t
                                    || prev.index_t != record.index_t
                                    || prev.commit_head_id != record.commit_head_id
                                    || prev.index_head_id != record.index_head_id
                                {
                                    yield RemoteEvent::LedgerUpdated(record.clone());
                                }
                            } else {
                                // New record
                                yield RemoteEvent::LedgerUpdated(record.clone());
                            }
                            current_ledgers.insert(key, record);
                        }

                        // Check for removed ledgers (present in prev but not in current)
                        for key in prev_ledgers.keys() {
                            if !current_ledgers.contains_key(key) {
                                yield RemoteEvent::LedgerRetracted { ledger_id: key.clone() };
                            }
                        }

                        prev_ledgers = current_ledgers;

                        // Diff graph sources
                        let mut current_graph_sources: HashMap<String, GraphSourceRecord> = HashMap::new();
                        for record in snapshot.graph_sources {
                            let key = record.graph_source_id.clone();
                            if record.retracted {
                                if prev_graph_sources.contains_key(&key) {
                                    yield RemoteEvent::GraphSourceRetracted { graph_source_id: key.clone() };
                                }
                            } else if let Some(prev) = prev_graph_sources.get(&key) {
                                if prev.index_t != record.index_t
                                    || prev.index_id != record.index_id
                                {
                                    yield RemoteEvent::GraphSourceUpdated(record.clone());
                                }
                            } else {
                                yield RemoteEvent::GraphSourceUpdated(record.clone());
                            }
                            current_graph_sources.insert(key, record);
                        }

                        for key in prev_graph_sources.keys() {
                            if !current_graph_sources.contains_key(key) {
                                yield RemoteEvent::GraphSourceRetracted { graph_source_id: key.clone() };
                            }
                        }

                        prev_graph_sources = current_graph_sources;

                        tokio::time::sleep(interval).await;
                    }
                    Err(e) => {
                        if connected {
                            yield RemoteEvent::Disconnected {
                                reason: format!("Poll failed: {e}"),
                            };
                            connected = false;
                        }

                        let delay = backoff.next_delay();
                        tracing::debug!("Poll reconnecting in {:?}", delay);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        };

        Box::pin(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{RemoteNameserviceClient, RemoteSnapshot};
    use crate::error;
    use crate::watch::RemoteWatch;
    use fluree_db_nameservice::{CasResult, RefKind, RefValue};
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock client that returns pre-configured snapshots
    #[derive(Debug)]
    struct MockClient {
        snapshots: parking_lot::Mutex<Vec<RemoteSnapshot>>,
        call_count: AtomicUsize,
    }

    impl MockClient {
        fn new(snapshots: Vec<RemoteSnapshot>) -> Self {
            Self {
                snapshots: parking_lot::Mutex::new(snapshots),
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl RemoteNameserviceClient for MockClient {
        async fn lookup(&self, _address: &str) -> error::Result<Option<NsRecord>> {
            Ok(None)
        }

        async fn snapshot(&self) -> error::Result<RemoteSnapshot> {
            let idx = self.call_count.fetch_add(1, Ordering::SeqCst);
            let snapshots = self.snapshots.lock();
            if idx < snapshots.len() {
                Ok(snapshots[idx].clone())
            } else {
                // Return last snapshot for subsequent calls
                Ok(snapshots.last().cloned().unwrap_or(RemoteSnapshot {
                    ledgers: vec![],
                    graph_sources: vec![],
                }))
            }
        }

        async fn push_ref(
            &self,
            _address: &str,
            _kind: RefKind,
            _expected: Option<&RefValue>,
            _new: &RefValue,
        ) -> error::Result<CasResult> {
            Ok(CasResult::Updated)
        }

        async fn init_ledger(&self, _address: &str) -> error::Result<bool> {
            Ok(true)
        }
    }

    fn make_record(ledger_name: &str, commit_t: i64) -> NsRecord {
        NsRecord {
            ledger_id: format!("{ledger_name}:main"),
            name: ledger_name.to_string(),
            branch: "main".to_string(),
            commit_head_id: None,
            config_id: None,
            commit_t,
            index_head_id: None,
            index_t: 0,
            retracted: false,
            default_context: None,
            source_branch: None,
            branches: 0,
        }
    }

    #[tokio::test]
    async fn test_poll_detects_new_record() {
        let client = Arc::new(MockClient::new(vec![RemoteSnapshot {
            ledgers: vec![make_record("db1", 1)],
            graph_sources: vec![],
        }]));

        let watch = PollRemoteWatch::new(client, Duration::from_millis(10));
        let mut stream = watch.watch();

        // First event should be Connected
        match stream.next().await.unwrap() {
            RemoteEvent::Connected => {}
            other => panic!("expected Connected, got {other:?}"),
        }

        // Second should be the new ledger
        match stream.next().await.unwrap() {
            RemoteEvent::LedgerUpdated(r) => {
                assert_eq!(r.commit_t, 1);
            }
            other => panic!("expected LedgerUpdated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_poll_detects_update() {
        let client = Arc::new(MockClient::new(vec![
            RemoteSnapshot {
                ledgers: vec![make_record("db1", 1)],
                graph_sources: vec![],
            },
            RemoteSnapshot {
                ledgers: vec![make_record("db1", 5)],
                graph_sources: vec![],
            },
        ]));

        let watch = PollRemoteWatch::new(client, Duration::from_millis(1));
        let mut stream = watch.watch();

        // Connected
        stream.next().await;
        // Initial LedgerUpdated (t=1)
        stream.next().await;

        // Wait for next poll cycle - should detect update
        // We need a short sleep to let the poll interval pass
        tokio::time::sleep(Duration::from_millis(10)).await;

        match stream.next().await.unwrap() {
            RemoteEvent::LedgerUpdated(r) => {
                assert_eq!(r.commit_t, 5);
            }
            other => panic!("expected LedgerUpdated, got {other:?}"),
        }
    }
}
