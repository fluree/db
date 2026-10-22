//! What this instance knows of each branch, without reading the
//! nameservice: which incarnation holds the name, its commit head, and
//! (where this instance makes every write) whether it has a removal journal;
//! and which incarnations this instance has dropped.
//!
//! Kept current by the nameservice event bus, which carries this instance's
//! own writes, a cluster's replicated ones and a peer's synced ones, the
//! same events the ledger cache follows; it does not depend on the cache.
//! A branch it knows nothing of is looked up once, and kept unless an event
//! for it arrived meanwhile.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use fluree_db_nameservice::{Fence, NameServiceEvent, NsRecord};

/// One branch as this instance knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Seen {
    /// The branch exists: its record's fence, its ledger's instance (`0`
    /// for a ledger rooted at its name), its commit head, and whether its
    /// journal holds anything (`None` when not known).
    Live {
        fence: Option<Fence>,
        instance: u128,
        head_t: i64,
        journal: Option<bool>,
    },
    /// The branch was dropped.
    Dropped,
    /// A lookup found no live branch: dropped, or never created.
    Absent,
}

/// What [`BranchWatch::check`] found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Check {
    /// This instance dropped the branch record.
    FenceDropped,
    Known(Seen),
    /// Nothing kept: look it up.
    Unknown,
}

#[derive(Default)]
struct Branch {
    /// Sequence of the last event or write that changed it.
    touched: u64,
    seen: Option<Seen>,
}

#[derive(Default)]
struct State {
    seq: u64,
    branches: HashMap<String, Branch>,
    /// Branch records this instance dropped.
    dropped: HashSet<Fence>,
}

#[derive(Default)]
pub(crate) struct BranchWatch {
    state: RwLock<State>,
    /// The event bus carries every nameservice write this instance makes
    /// or applies; without it nothing is kept.
    tracking: AtomicBool,
    /// This instance makes every nameservice write (file or memory
    /// storage), so a journal it last read or wrote is the journal.
    sole_writer: AtomicBool,
}

impl BranchWatch {
    /// Follow `bus`, which carries every nameservice write this instance
    /// makes or applies.
    pub(crate) fn track(self: &std::sync::Arc<Self>, bus: &fluree_db_nameservice::LedgerEventBus) {
        let watch = std::sync::Arc::clone(self);
        bus.observe(std::sync::Arc::new(move |event| watch.observe(event)));
        self.tracking.store(true, Ordering::Release);
    }

    pub(crate) fn set_sole_writer(&self) {
        self.sole_writer.store(true, Ordering::Release);
    }

    pub(crate) fn clear_sole_writer(&self) {
        self.sole_writer.store(false, Ordering::Release);
    }

    /// What is known of `ledger_id` (as canonically spelled) and of the
    /// branch record `fence` belongs to, in one look; a journal is reported
    /// only where this instance makes every write.
    pub(crate) fn check(&self, ledger_id: &str, fence: Option<Fence>) -> Check {
        let state = self.state.read().expect("watch lock never poisoned");
        if fence.is_some_and(|fence| state.dropped.contains(&fence)) {
            return Check::FenceDropped;
        }
        if !self.tracking.load(Ordering::Acquire) {
            return Check::Unknown;
        }
        let Some(seen) = state.branches.get(ledger_id).and_then(|b| b.seen) else {
            return Check::Unknown;
        };
        Check::Known(match seen {
            Seen::Live {
                fence,
                instance,
                head_t,
                journal,
            } => Seen::Live {
                fence,
                instance,
                head_t,
                journal: journal.filter(|_| self.sole_writer.load(Ordering::Acquire)),
            },
            other => other,
        })
    }

    /// This instance dropped the branch records `fences` belong to.
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn dropped(&self, fences: impl IntoIterator<Item = Fence>) {
        self.state
            .write()
            .expect("watch lock never poisoned")
            .dropped
            .extend(fences);
    }

    /// A token for [`Self::learn`] and [`Self::learn_journal`], taken
    /// before the read whose result they record.
    pub(crate) fn token(&self) -> u64 {
        self.state.read().expect("watch lock never poisoned").seq
    }

    /// Record a branch looked up after `token`, unless something changed it
    /// since.
    pub(crate) fn learn(&self, ledger_id: &str, record: Option<&NsRecord>, token: u64) {
        let mut state = self.state.write().expect("watch lock never poisoned");
        let branch = state.branches.entry(ledger_id.to_string()).or_default();
        if branch.touched > token {
            return;
        }
        branch.seen = Some(match record {
            Some(record) => Seen::live(record),
            None => Seen::Absent,
        });
    }

    /// Record whether a journal read after `token` holds anything, unless
    /// something changed the branch since.
    pub(crate) fn learn_journal(&self, ledger_id: &str, holds: bool, token: u64) {
        let mut state = self.state.write().expect("watch lock never poisoned");
        let Some(branch) = state.branches.get_mut(ledger_id) else {
            return;
        };
        if branch.touched > token {
            return;
        }
        if let Some(Seen::Live { journal, .. }) = &mut branch.seen {
            *journal = Some(holds);
        }
    }

    /// Before this instance writes `ledger_id`'s journal: what was known of
    /// it no longer holds.
    pub(crate) fn journal_changing(&self, ledger_id: &str) {
        self.touch(ledger_id, |seen| {
            if let Some(Seen::Live { journal, .. }) = seen {
                *journal = None;
            }
        });
    }

    /// Apply a nameservice event.
    pub(crate) fn observe(&self, event: &NameServiceEvent) {
        match event {
            NameServiceEvent::LedgerCommitPublished {
                ledger_id,
                commit_t,
                ..
            } => {
                self.touch(ledger_id.as_str(), |seen| {
                    if let Some(Seen::Live { head_t, .. }) = seen {
                        *head_t = (*head_t).max(*commit_t);
                    }
                });
            }
            NameServiceEvent::LedgerRetracted { ledger_id, .. } => {
                self.touch(ledger_id.as_str(), |seen| *seen = Some(Seen::Dropped));
            }
            // Possibly another incarnation under the name: look it up again.
            NameServiceEvent::LedgerCreated { ledger_id, .. } => {
                self.touch(ledger_id.as_str(), |seen| *seen = None);
            }
            _ => {}
        }
    }

    fn touch(&self, ledger_id: &str, update: impl FnOnce(&mut Option<Seen>)) {
        let mut state = self.state.write().expect("watch lock never poisoned");
        state.seq += 1;
        let seq = state.seq;
        let branch = state.branches.entry(ledger_id.to_string()).or_default();
        branch.touched = seq;
        update(&mut branch.seen);
    }
}

impl Seen {
    pub(crate) fn live(record: &NsRecord) -> Self {
        Seen::Live {
            fence: record.fence,
            instance: record
                .storage_root
                .as_ref()
                .and_then(fluree_db_core::StorageRoot::instance_bits)
                .unwrap_or(0),
            head_t: record.commit_t,
            journal: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentId, ContentKind, LedgerId};

    fn id() -> LedgerId {
        LedgerId::parse("w:main").unwrap()
    }

    fn get(watch: &BranchWatch) -> Option<Seen> {
        match watch.check("w:main", None) {
            Check::Known(seen) => Some(seen),
            _ => None,
        }
    }

    fn tracking() -> BranchWatch {
        let watch = BranchWatch::default();
        watch.tracking.store(true, Ordering::Release);
        watch
    }

    fn record(t: i64) -> NsRecord {
        NsRecord {
            commit_t: t,
            ..NsRecord::new(id())
        }
    }

    fn commit(t: i64) -> NameServiceEvent {
        NameServiceEvent::LedgerCommitPublished {
            ledger_id: id(),
            commit_id: ContentId::new(ContentKind::Commit, b"c"),
            commit_t: t,
        }
    }

    #[test]
    fn events_keep_a_learned_branch_current() {
        let watch = tracking();
        watch.set_sole_writer();
        assert_eq!(get(&watch), None);
        watch.learn("w:main", Some(&record(3)), watch.token());
        watch.observe(&commit(5));
        let live = |head_t, journal| Seen::Live {
            fence: None,
            instance: 0,
            head_t,
            journal,
        };
        assert_eq!(get(&watch), Some(live(5, None)));
        watch.learn_journal("w:main", false, watch.token());
        assert_eq!(get(&watch), Some(live(5, Some(false))));
        watch.journal_changing("w:main");
        assert_eq!(get(&watch), Some(live(5, None)));

        watch.observe(&NameServiceEvent::LedgerRetracted {
            ledger_id: id(),
            instance: None,
        });
        assert_eq!(get(&watch), Some(Seen::Dropped));
    }

    #[test]
    fn a_read_overtaken_by_an_event_is_not_kept() {
        let watch = tracking();
        watch.set_sole_writer();
        let token = watch.token();
        watch.observe(&commit(7));
        watch.learn("w:main", Some(&record(3)), token);
        assert_eq!(get(&watch), None);

        watch.learn("w:main", Some(&record(7)), watch.token());
        let token = watch.token();
        watch.journal_changing("w:main");
        watch.learn_journal("w:main", false, token);
        assert!(matches!(
            get(&watch),
            Some(Seen::Live { journal: None, .. })
        ));
    }

    #[test]
    fn a_dropped_record_is_known_without_the_event_bus() {
        let watch = BranchWatch::default();
        let fence = fluree_db_nameservice::Fence::generate();
        watch.dropped([fence]);
        assert_eq!(watch.check("w:main", Some(fence)), Check::FenceDropped);
        assert_eq!(watch.check("w:main", None), Check::Unknown);
    }

    #[test]
    fn nothing_is_kept_without_the_event_bus() {
        let watch = BranchWatch::default();
        watch.learn("w:main", Some(&record(3)), watch.token());
        assert_eq!(get(&watch), None);
    }

    #[test]
    fn a_journal_is_reported_only_to_its_sole_writer() {
        let watch = tracking();
        watch.learn("w:main", Some(&record(3)), watch.token());
        watch.learn_journal("w:main", false, watch.token());
        assert!(matches!(
            get(&watch),
            Some(Seen::Live { journal: None, .. })
        ));
    }
}
