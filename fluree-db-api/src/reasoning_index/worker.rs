//! The background worker that keeps reasoning indexes current.
//!
//! It watches commit events and refreshes the indexes of each ledger that
//! committed, one ledger at a time, so concurrent materializations stay
//! bounded to one per worker. Commits that arrive while a ledger is being
//! refreshed coalesce into one more refresh. A periodic catch-up scan finds
//! indexes that events missed: registrations made by another process, indexes
//! whose publication was throttled, and events lost to a lagging receiver.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use fluree_db_core::reasoning_index::RegistrationState;
use fluree_db_nameservice::{NameServiceEvent, SubscriptionScope};
use tokio::sync::broadcast::error::RecvError;

use super::RefreshOutcome;
use crate::Fluree;

/// How often the worker retries known registered ledgers.
pub const DEFAULT_CATCHUP_INTERVAL: Duration = Duration::from_secs(60);
/// Full discovery is infrequent; local registrations wake the worker immediately.
pub const DEFAULT_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// Coalesce repeated commits without letting a busy ledger jump the queue.
#[derive(Default)]
struct Pending {
    queue: VecDeque<String>,
    queued: HashSet<String>,
}

impl Pending {
    fn insert(&mut self, id: String) {
        if self.queued.insert(id.clone()) {
            self.queue.push_back(id);
        }
    }
    fn extend(&mut self, ids: impl IntoIterator<Item = String>) {
        for id in ids {
            self.insert(id);
        }
    }
    fn pop(&mut self) -> Option<String> {
        let id = self.queue.pop_front()?;
        self.queued.remove(&id);
        Some(id)
    }
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

/// Keeps every registered reasoning index of every ledger current.
pub struct ReasoningIndexWorker {
    fluree: Arc<Fluree>,
    catchup_interval: Duration,
    reconciliation_interval: Duration,
}

impl ReasoningIndexWorker {
    pub fn new(fluree: Arc<Fluree>) -> Self {
        Self {
            fluree,
            catchup_interval: DEFAULT_CATCHUP_INTERVAL,
            reconciliation_interval: DEFAULT_RECONCILIATION_INTERVAL,
        }
    }

    pub fn with_catchup_interval(mut self, interval: Duration) -> Self {
        self.catchup_interval = interval.max(Duration::from_millis(1));
        self
    }

    pub fn with_reconciliation_interval(mut self, interval: Duration) -> Self {
        self.reconciliation_interval = interval.max(Duration::from_millis(1));
        self
    }

    /// Run until the event bus closes.
    pub async fn run(self) {
        let mut events = self.fluree.event_bus().subscribe(SubscriptionScope::All);
        let mut has_indexes: HashMap<String, bool> = HashMap::new();
        let mut pending = Pending::default();
        pending.extend(self.scan(&mut has_indexes).await);
        let mut tick = tokio::time::interval(self.catchup_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        let mut reconcile = tokio::time::interval(self.reconciliation_interval);
        reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        reconcile.tick().await;
        loop {
            // Inspect registrations even while the work queue stays nonempty.
            for id in self.fluree.reasoning_index_state.take_registered() {
                has_indexes.insert(id.clone(), true);
                pending.insert(id);
            }
            tokio::select! {
                event = events.receiver.recv() => match event {
                    Ok(event) => {
                        if let Some(id) = self.committed(event, &mut has_indexes).await {
                            pending.insert(id);
                        }
                    }
                    // Known indexes catch up now; the slow discovery sweep
                    // still finds registrations made by other processes.
                    Err(RecvError::Lagged(_)) => pending.extend(
                        has_indexes.iter().filter(|(_, active)| **active).map(|(id, _)| id.clone())
                    ),
                    Err(RecvError::Closed) => return,
                },
                _ = tick.tick() => pending.extend(
                    has_indexes.iter().filter(|(_, active)| **active).map(|(id, _)| id.clone())
                ),
                _ = reconcile.tick() => pending.extend(self.scan(&mut has_indexes).await),
                () = self.fluree.reasoning_index_state.registered.notified() => {},
                () = std::future::ready(()), if !pending.is_empty() => {
                    let id = pending.pop().expect("nonempty queue");
                    let active = self.refresh(&id).await;
                    has_indexes.insert(id, active);
                },
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
                let known = self.has_active_indexes(&id).await?;
                has_indexes.insert(id.clone(), known);
                known
            }
        };
        known.then_some(id)
    }

    async fn has_active_indexes(&self, ledger_id: &str) -> Option<bool> {
        match self
            .fluree
            .nameservice()
            .get_reasoning_indexes(ledger_id)
            .await
        {
            Ok(Some(doc)) => Some(
                doc.indexes
                    .values()
                    .any(|e| matches!(e.registration.state, RegistrationState::Active)),
            ),
            Ok(None) => Some(false),
            Err(e) => {
                tracing::debug!(ledger = ledger_id, error = %e, "could not read reasoning indexes");
                None
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
        let live: HashSet<_> = records
            .iter()
            .filter(|r| !r.retracted && !r.frozen)
            .map(|r| r.ledger_id.to_string())
            .collect();
        has_indexes.retain(|id, _| live.contains(id));
        for record in records.iter().filter(|r| !r.retracted && !r.frozen) {
            let id = record.ledger_id.to_string();
            let doc = match self.fluree.nameservice().get_reasoning_indexes(&id).await {
                Ok(Some(doc)) => doc,
                Ok(None) => {
                    has_indexes.insert(id, false);
                    continue;
                }
                Err(error) => {
                    tracing::warn!(ledger = %id, %error, "could not reconcile reasoning indexes");
                    if has_indexes.get(&id).copied().unwrap_or(false) {
                        out.insert(id);
                    }
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

    async fn refresh(&self, ledger_id: &str) -> bool {
        match self.fluree.refresh_indexes(ledger_id, true).await {
            Ok(outcomes) => {
                let active = !outcomes.is_empty();
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
                        RefreshOutcome::NewerEngine { published, engine } => tracing::warn!(
                            ledger = ledger_id,
                            index = %index_id,
                            published,
                            engine,
                            "reasoning index published under newer engine semantics; not \
                             refreshed here (after a downgrade, retire and register it again)"
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
                active
            }
            Err(e) => {
                tracing::warn!(ledger = ledger_id, error = %e, "reasoning-index refresh failed");
                !matches!(e, crate::ApiError::NotFound(_))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_busy_ledger_cannot_overtake_waiting_ledgers() {
        let mut pending = Pending::default();
        pending.extend(["a".into(), "b".into(), "c".into(), "a".into()]);
        assert_eq!(pending.pop().as_deref(), Some("a"));
        pending.insert("a".into());
        assert_eq!(pending.pop().as_deref(), Some("b"));
        assert_eq!(pending.pop().as_deref(), Some("c"));
        assert_eq!(pending.pop().as_deref(), Some("a"));
        assert!(pending.is_empty());
    }
}
