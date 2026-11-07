//! The background worker that keeps reasoning indexes current.
//!
//! It watches commit events and refreshes the indexes of each ledger that
//! committed, one ledger at a time, so concurrent materializations stay
//! bounded to one per worker. Commits that arrive while a ledger is being
//! refreshed coalesce into one more refresh. A periodic catch-up scan finds
//! indexes that events missed: registrations made by another process, indexes
//! whose publication was throttled, and events lost to a lagging receiver.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use fluree_db_core::reasoning_index::RegistrationState;
use fluree_db_nameservice::{NameServiceEvent, SubscriptionScope};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use super::RefreshOutcome;
use crate::Fluree;

/// How often the worker scans every ledger for indexes that need a refresh.
pub const DEFAULT_CATCHUP_INTERVAL: Duration = Duration::from_secs(60);

/// Keeps every registered reasoning index of every ledger current.
pub struct ReasoningIndexWorker {
    fluree: Arc<Fluree>,
    catchup_interval: Duration,
}

impl ReasoningIndexWorker {
    pub fn new(fluree: Arc<Fluree>) -> Self {
        Self {
            fluree,
            catchup_interval: DEFAULT_CATCHUP_INTERVAL,
        }
    }

    pub fn with_catchup_interval(mut self, interval: Duration) -> Self {
        self.catchup_interval = interval;
        self
    }

    /// Run until the event bus closes.
    pub async fn run(self) {
        let mut events = self.fluree.event_bus().subscribe(SubscriptionScope::All);
        let mut has_indexes: HashMap<String, bool> = HashMap::new();
        let mut pending: BTreeSet<String> = self.scan(&mut has_indexes).await;
        let mut tick = tokio::time::interval(self.catchup_interval);
        tick.tick().await;
        loop {
            while let Some(ledger_id) = pending.pop_first() {
                self.refresh(&ledger_id).await;
                loop {
                    match events.receiver.try_recv() {
                        Ok(event) => {
                            if let Some(id) = self.committed(event, &mut has_indexes).await {
                                pending.insert(id);
                            }
                        }
                        Err(TryRecvError::Lagged(_)) => {
                            pending.extend(self.scan(&mut has_indexes).await);
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Closed) => return,
                    }
                }
            }
            tokio::select! {
                event = events.receiver.recv() => match event {
                    Ok(event) => {
                        if let Some(id) = self.committed(event, &mut has_indexes).await {
                            pending.insert(id);
                        }
                    }
                    Err(RecvError::Lagged(_)) => pending.extend(self.scan(&mut has_indexes).await),
                    Err(RecvError::Closed) => return,
                },
                _ = tick.tick() => pending.extend(self.scan(&mut has_indexes).await),
                () = self.fluree.reasoning_index_state.registered.notified() => {
                    for id in self.fluree.reasoning_index_state.take_registered() {
                        has_indexes.insert(id.clone(), true);
                        pending.insert(id);
                    }
                }
            }
        }
    }

    /// The ledger a commit event names, when it has indexes to refresh.
    async fn committed(
        &self,
        event: NameServiceEvent,
        has_indexes: &mut HashMap<String, bool>,
    ) -> Option<String> {
        let NameServiceEvent::LedgerCommitPublished { ledger_id, .. } = event else {
            return None;
        };
        let id = ledger_id.to_string();
        let known = match has_indexes.get(&id) {
            Some(known) => *known,
            None => {
                let known = self.has_active_indexes(&id).await;
                has_indexes.insert(id.clone(), known);
                known
            }
        };
        known.then_some(id)
    }

    async fn has_active_indexes(&self, ledger_id: &str) -> bool {
        match self
            .fluree
            .nameservice()
            .get_reasoning_indexes(ledger_id)
            .await
        {
            Ok(Some(doc)) => doc
                .indexes
                .values()
                .any(|e| matches!(e.registration.state, RegistrationState::Active)),
            Ok(None) => false,
            Err(e) => {
                tracing::debug!(ledger = ledger_id, error = %e, "could not read reasoning indexes");
                false
            }
        }
    }

    /// Every ledger with an active index, refreshing what the worker knows.
    async fn scan(&self, has_indexes: &mut HashMap<String, bool>) -> BTreeSet<String> {
        let records = match self.fluree.nameservice().all_records().await {
            Ok(records) => records,
            Err(e) => {
                tracing::warn!(error = %e, "reasoning-index catch-up could not list ledgers");
                return BTreeSet::new();
            }
        };
        let mut out = BTreeSet::new();
        for record in records.iter().filter(|r| !r.retracted && !r.frozen) {
            let id = record.ledger_id.to_string();
            let doc = match self.fluree.nameservice().get_reasoning_indexes(&id).await {
                Ok(Some(doc)) => doc,
                _ => {
                    has_indexes.insert(id, false);
                    continue;
                }
            };
            let active = doc
                .indexes
                .values()
                .any(|e| matches!(e.registration.state, RegistrationState::Active));
            let retired = doc
                .indexes
                .values()
                .any(|e| matches!(e.registration.state, RegistrationState::Retired { .. }));
            if retired {
                if let Err(e) = self.fluree.clean_retired_reasoning_indexes(&id).await {
                    tracing::warn!(ledger = %id, error = %e, "retired reasoning indexes not cleaned");
                }
            }
            has_indexes.insert(id.clone(), active);
            if active {
                out.insert(id);
            }
        }
        out
    }

    async fn refresh(&self, ledger_id: &str) {
        match self.fluree.refresh_reasoning_indexes(ledger_id).await {
            Ok(outcomes) => {
                for (index_id, outcome) in outcomes {
                    match outcome {
                        RefreshOutcome::Published {
                            generation,
                            kind,
                            basis_t,
                            stats,
                        } => tracing::info!(
                            ledger = ledger_id,
                            index = %index_id,
                            generation,
                            ?kind,
                            basis_t,
                            derived_only = stats.derived_only,
                            added = stats.added,
                            removed = stats.removed,
                            "reasoning index published"
                        ),
                        RefreshOutcome::Failed { reason } => tracing::warn!(
                            ledger = ledger_id,
                            index = %index_id,
                            reason,
                            "reasoning index not refreshed"
                        ),
                        other => tracing::debug!(
                            ledger = ledger_id,
                            index = %index_id,
                            outcome = ?other,
                            "reasoning index refresh"
                        ),
                    }
                }
            }
            Err(e) => {
                tracing::warn!(ledger = ledger_id, error = %e, "reasoning-index refresh failed");
            }
        }
    }
}
