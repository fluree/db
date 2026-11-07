//! The background worker that keeps reasoned heads current.
//!
//! It watches commit events and refreshes the heads of each ledger that
//! committed, one ledger at a time, so concurrent materializations stay
//! bounded to one per worker. Commits that arrive while a ledger is being
//! refreshed coalesce into one more refresh. A periodic catch-up scan finds
//! heads that events missed: registrations made by another process, heads
//! whose publication was throttled, and events lost to a lagging receiver.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use fluree_db_core::reasoned_head::RegistrationState;
use fluree_db_nameservice::{NameServiceEvent, SubscriptionScope};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use super::RefreshOutcome;
use crate::Fluree;

/// How often the worker scans every ledger for heads that need a refresh.
pub const DEFAULT_CATCHUP_INTERVAL: Duration = Duration::from_secs(60);

/// Keeps every registered reasoned head of every ledger current.
pub struct ReasonedHeadWorker {
    fluree: Arc<Fluree>,
    catchup_interval: Duration,
}

impl ReasonedHeadWorker {
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
        let mut has_heads: HashMap<String, bool> = HashMap::new();
        let mut pending: BTreeSet<String> = self.scan(&mut has_heads).await;
        let mut tick = tokio::time::interval(self.catchup_interval);
        tick.tick().await;
        loop {
            while let Some(ledger_id) = pending.pop_first() {
                self.refresh(&ledger_id).await;
                loop {
                    match events.receiver.try_recv() {
                        Ok(event) => {
                            if let Some(id) = self.committed(event, &mut has_heads).await {
                                pending.insert(id);
                            }
                        }
                        Err(TryRecvError::Lagged(_)) => {
                            pending.extend(self.scan(&mut has_heads).await);
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Closed) => return,
                    }
                }
            }
            tokio::select! {
                event = events.receiver.recv() => match event {
                    Ok(event) => {
                        if let Some(id) = self.committed(event, &mut has_heads).await {
                            pending.insert(id);
                        }
                    }
                    Err(RecvError::Lagged(_)) => pending.extend(self.scan(&mut has_heads).await),
                    Err(RecvError::Closed) => return,
                },
                _ = tick.tick() => pending.extend(self.scan(&mut has_heads).await),
                () = self.fluree.reasoned.registered.notified() => {
                    for id in self.fluree.reasoned.take_registered() {
                        has_heads.insert(id.clone(), true);
                        pending.insert(id);
                    }
                }
            }
        }
    }

    /// The ledger a commit event names, when it has heads to refresh.
    async fn committed(
        &self,
        event: NameServiceEvent,
        has_heads: &mut HashMap<String, bool>,
    ) -> Option<String> {
        let NameServiceEvent::LedgerCommitPublished { ledger_id, .. } = event else {
            return None;
        };
        let id = ledger_id.to_string();
        let known = match has_heads.get(&id) {
            Some(known) => *known,
            None => {
                let known = self.has_active_heads(&id).await;
                has_heads.insert(id.clone(), known);
                known
            }
        };
        known.then_some(id)
    }

    async fn has_active_heads(&self, ledger_id: &str) -> bool {
        match self
            .fluree
            .nameservice()
            .get_reasoned_heads(ledger_id)
            .await
        {
            Ok(Some(doc)) => doc
                .heads
                .values()
                .any(|e| matches!(e.registration.state, RegistrationState::Active)),
            Ok(None) => false,
            Err(e) => {
                tracing::debug!(ledger = ledger_id, error = %e, "could not read reasoned heads");
                false
            }
        }
    }

    /// Every ledger with an active head, refreshing what the worker knows.
    async fn scan(&self, has_heads: &mut HashMap<String, bool>) -> BTreeSet<String> {
        let records = match self.fluree.nameservice().all_records().await {
            Ok(records) => records,
            Err(e) => {
                tracing::warn!(error = %e, "reasoned-head catch-up could not list ledgers");
                return BTreeSet::new();
            }
        };
        let mut out = BTreeSet::new();
        for record in records.iter().filter(|r| !r.retracted && !r.frozen) {
            let id = record.ledger_id.to_string();
            let active = self.has_active_heads(&id).await;
            has_heads.insert(id.clone(), active);
            if active {
                out.insert(id);
            }
        }
        out
    }

    async fn refresh(&self, ledger_id: &str) {
        match self.fluree.refresh_reasoned_heads(ledger_id).await {
            Ok(outcomes) => {
                for (head_id, outcome) in outcomes {
                    match outcome {
                        RefreshOutcome::Published {
                            generation,
                            kind,
                            basis_t,
                            stats,
                        } => tracing::info!(
                            ledger = ledger_id,
                            head = %head_id,
                            generation,
                            ?kind,
                            basis_t,
                            derived_only = stats.derived_only,
                            added = stats.added,
                            removed = stats.removed,
                            "reasoned head published"
                        ),
                        RefreshOutcome::Failed { reason } => tracing::warn!(
                            ledger = ledger_id,
                            head = %head_id,
                            reason,
                            "reasoned head not refreshed"
                        ),
                        other => tracing::debug!(
                            ledger = ledger_id,
                            head = %head_id,
                            outcome = ?other,
                            "reasoned head refresh"
                        ),
                    }
                }
            }
            Err(e) => {
                tracing::warn!(ledger = ledger_id, error = %e, "reasoned-head refresh failed");
            }
        }
    }
}
