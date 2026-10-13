//! What a remote's snapshot names, so a subscriber can drop what it holds
//! that the remote retracted while the subscriber was disconnected.

use crate::watch::RemoteEvent;
use fluree_db_core::LedgerId;
use std::collections::HashSet;
use std::sync::{Mutex, PoisonError};

/// The ledgers and graph sources a snapshot named.
#[derive(Debug, Default, Clone)]
pub struct SnapshotNames {
    pub ledgers: HashSet<LedgerId>,
    pub graph_sources: HashSet<LedgerId>,
}

/// Follows a remote's events from [`RemoteEvent::Connected`] to
/// [`RemoteEvent::SnapshotComplete`], collecting what the snapshot names.
/// A snapshot cut short by a disconnect names nothing, so nothing is
/// dropped on its account.
#[derive(Debug, Default)]
pub struct SnapshotTracker(Mutex<Option<SnapshotNames>>);

impl SnapshotTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Note `event`. Returns what the snapshot named once it completes.
    pub fn observe(&self, event: &RemoteEvent) -> Option<SnapshotNames> {
        let mut names = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match event {
            RemoteEvent::Connected => *names = Some(SnapshotNames::default()),
            RemoteEvent::LedgerUpdated(record) => {
                if let Some(names) = names.as_mut() {
                    names.ledgers.insert(record.ledger_id.clone());
                }
            }
            RemoteEvent::GraphSourceUpdated(record) => {
                if let Some(names) = names.as_mut() {
                    names.graph_sources.insert(record.graph_source_id.clone());
                }
            }
            RemoteEvent::SnapshotComplete => return names.take(),
            RemoteEvent::Disconnected { .. } | RemoteEvent::Fatal { .. } => *names = None,
            RemoteEvent::LedgerRetracted { .. } | RemoteEvent::GraphSourceRetracted { .. } => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_nameservice::NsRecord;

    fn updated(id: &str) -> RemoteEvent {
        RemoteEvent::LedgerUpdated(NsRecord::new(LedgerId::parse(id).unwrap()))
    }

    #[test]
    fn names_what_the_snapshot_sent_and_nothing_after_a_cut() {
        let tracker = SnapshotTracker::new();
        assert!(tracker.observe(&RemoteEvent::SnapshotComplete).is_none());

        tracker.observe(&RemoteEvent::Connected);
        tracker.observe(&updated("a:main"));
        let names = tracker
            .observe(&RemoteEvent::SnapshotComplete)
            .expect("complete");
        assert_eq!(
            names.ledgers,
            HashSet::from([LedgerId::parse("a:main").unwrap()])
        );
        // Live updates after it name nothing further.
        tracker.observe(&updated("b:main"));
        assert!(tracker.observe(&RemoteEvent::SnapshotComplete).is_none());

        tracker.observe(&RemoteEvent::Connected);
        tracker.observe(&RemoteEvent::Disconnected {
            reason: "cut".into(),
        });
        assert!(tracker.observe(&RemoteEvent::SnapshotComplete).is_none());
    }
}
