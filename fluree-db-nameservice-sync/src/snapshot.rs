//! What a remote's snapshot names, so a subscriber can drop what it holds
//! that the remote retracted while the subscriber was disconnected, and
//! whether it missed a refresh meanwhile.

use crate::watch::RemoteEvent;
use fluree_db_core::LedgerId;
use std::collections::HashSet;
use std::sync::{Mutex, PoisonError};

/// The ledgers and graph sources a snapshot named.
#[derive(Debug, Default, Clone)]
pub struct SnapshotNames {
    pub ledgers: HashSet<LedgerId>,
    pub graph_sources: HashSet<LedgerId>,
    /// The remote's erasure generation changed since last seen: a refresh
    /// was missed, so every cached ledger is stale.
    pub missed_refresh: bool,
}

/// Follows a remote's events from [`RemoteEvent::Connected`] to
/// [`RemoteEvent::SnapshotComplete`], collecting what the snapshot names.
/// A snapshot cut short by a disconnect names nothing, so nothing is
/// dropped on its account. Remembers the erasure generation last seen.
#[derive(Debug, Default)]
pub struct SnapshotTracker(Mutex<Tracked>);

#[derive(Debug, Default)]
struct Tracked {
    names: Option<SnapshotNames>,
    erasures: Option<String>,
}

impl SnapshotTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Note `event`. Returns what the snapshot named once it completes.
    pub fn observe(&self, event: &RemoteEvent) -> Option<SnapshotNames> {
        let mut tracked = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match event {
            RemoteEvent::Connected => tracked.names = Some(SnapshotNames::default()),
            RemoteEvent::LedgerUpdated(record) => {
                if let Some(names) = tracked.names.as_mut() {
                    names.ledgers.insert(record.ledger_id.clone());
                }
            }
            RemoteEvent::GraphSourceUpdated(record) => {
                if let Some(names) = tracked.names.as_mut() {
                    names.graph_sources.insert(record.graph_source_id.clone());
                }
            }
            RemoteEvent::LedgerRefresh { erasures, .. } => {
                if erasures.is_some() {
                    tracked.erasures.clone_from(erasures);
                }
            }
            RemoteEvent::SnapshotComplete { erasures } => {
                let mut names = tracked.names.take()?;
                if let Some(erasures) = erasures {
                    let seen = tracked.erasures.replace(erasures.clone());
                    names.missed_refresh = seen.is_some_and(|seen| seen != *erasures);
                }
                return Some(names);
            }
            RemoteEvent::Disconnected { .. } | RemoteEvent::Fatal { .. } => tracked.names = None,
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
        assert!(tracker.observe(&complete(None)).is_none());

        tracker.observe(&RemoteEvent::Connected);
        tracker.observe(&updated("a:main"));
        let names = tracker.observe(&complete(None)).expect("complete");
        assert_eq!(
            names.ledgers,
            HashSet::from([LedgerId::parse("a:main").unwrap()])
        );
        // Live updates after it name nothing further.
        tracker.observe(&updated("b:main"));
        assert!(tracker.observe(&complete(None)).is_none());

        tracker.observe(&RemoteEvent::Connected);
        tracker.observe(&RemoteEvent::Disconnected {
            reason: "cut".into(),
        });
        assert!(tracker.observe(&complete(None)).is_none());
    }

    fn complete(erasures: Option<&str>) -> RemoteEvent {
        RemoteEvent::SnapshotComplete {
            erasures: erasures.map(str::to_string),
        }
    }

    /// A snapshot whose erasure generation differs from the last one seen,
    /// in a snapshot or a refresh, reports a missed refresh; the first
    /// seen, and one a refresh announced, do not.
    #[test]
    fn a_changed_erasure_generation_is_a_missed_refresh() {
        let tracker = SnapshotTracker::new();
        let snapshot = |erasures| {
            tracker.observe(&RemoteEvent::Connected);
            tracker.observe(&complete(erasures)).expect("complete")
        };
        assert!(!snapshot(Some("g1")).missed_refresh);
        assert!(!snapshot(Some("g1")).missed_refresh);
        assert!(snapshot(Some("g2")).missed_refresh);
        tracker.observe(&RemoteEvent::LedgerRefresh {
            ledger_id: LedgerId::parse("a:main").unwrap(),
            erasures: Some("g3".into()),
        });
        assert!(!snapshot(Some("g3")).missed_refresh);
        // A remote that sends none compares nothing.
        assert!(!snapshot(None).missed_refresh);
        assert!(!snapshot(Some("g3")).missed_refresh);
    }
}
