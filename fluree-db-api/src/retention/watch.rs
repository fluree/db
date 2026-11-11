//! What this instance knows of each branch, without reading the
//! nameservice: which incarnation holds the name, its commit head, and
//! its configuration revision and what its removal journal holds; and
//! which incarnations this instance has dropped.
//!
//! Kept current by the nameservice event bus, which carries this instance's
//! own writes, a cluster's replicated ones and a peer's synced ones, the
//! same events the ledger cache follows; it does not depend on the cache.
//! A branch it knows nothing of is looked up once, and kept unless an event
//! for it arrived meanwhile.
//!
//! An instance that shares its storage with other writers (a root opened
//! without its lock, S3) sees none of their changes as events. It serves
//! what it last learned of a branch, like every other read it makes, and
//! learns it again in the background once that is [`REVALIDATE_MS`] old:
//! a change made elsewhere reaches its reads within about that long.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use fluree_db_nameservice::{Fence, NameServiceEvent, NsRecord};

/// How old what an instance sharing its storage learned of a branch may
/// grow before a read has it learned again.
pub(crate) const REVALIDATE_MS: i64 = 1_000;

/// What a branch's removal journal holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Holds {
    Nothing,
    /// `f:noHistory` rules, which an index build applies and no read
    /// refuses for.
    Rules,
    /// A targeted removal: reads that may need what it took out are
    /// refused.
    Removals,
}

impl Holds {
    pub(crate) fn removals(self) -> bool {
        self == Holds::Removals
    }
}

/// One branch as this instance knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Seen {
    /// The branch exists: its record's fence, its ledger's instance (`0`
    /// for a ledger rooted at its name), its commit head, and what its
    /// journal holds (`None` when not known).
    Live {
        fence: Option<Fence>,
        instance: u128,
        head_t: i64,
        journal: Option<Holds>,
        config_version: Option<i64>,
        /// Moves whenever the branch's history may have changed other than
        /// by commits added at its head: a reset, a refresh, another
        /// incarnation. What was worked out from its history, such as
        /// which removals apply to it, holds while this stands.
        history: u64,
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
    replicated: bool,
    /// See [`Seen::Live::history`]; kept here so forgetting the branch
    /// does not start it over.
    history: u64,
    /// When the nameservice last confirmed what is kept (ms).
    validated_ms: i64,
    /// A background read is learning it again.
    revalidating: bool,
    /// The branch it was made from, as its record named it.
    source: Option<String>,
}

#[derive(Default)]
struct State {
    seq: u64,
    branches: HashMap<String, Branch>,
    /// Branch records this instance dropped.
    dropped: HashSet<Fence>,
    /// The ledgers each search graph source indexes, by the id a query
    /// names it with.
    sources: HashMap<String, Source>,
    /// Sequence of the last graph source change.
    sources_changed: u64,
}

struct Source {
    dependencies: std::sync::Arc<[String]>,
    learned_ms: i64,
}

#[derive(Default)]
pub(crate) struct BranchWatch {
    state: RwLock<State>,
    /// The event bus carries every nameservice write this instance makes
    /// or applies; without it nothing is kept.
    tracking: AtomicBool,
    /// Every config change is observed locally, through exclusive storage
    /// ownership or the replicated apply path.
    coherent: AtomicBool,
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
        self.coherent.store(true, Ordering::Release);
    }

    /// A replicated nameservice emits every applied config change locally.
    pub(crate) fn set_replicated(&self, complete: bool) {
        self.coherent.store(complete, Ordering::Release);
    }

    /// A peer's stream and reconnect snapshots carry configuration
    /// revisions even when the commit head stays unchanged.
    pub(crate) fn remote_record(&self, record: &NsRecord) {
        let mut state = self.state.write().expect("watch lock never poisoned");
        state.seq += 1;
        let seq = state.seq;
        let branch = state
            .branches
            .entry(record.ledger_id.to_string())
            .or_default();
        branch.adopt(record);
        branch.touched = seq;
        branch.replicated = true;
    }

    /// What is known of `ledger_id` (as canonically spelled) and of the
    /// branch record `fence` belongs to, in one look.
    pub(crate) fn check(&self, ledger_id: &str, fence: Option<Fence>) -> Check {
        let state = self.state.read().expect("watch lock never poisoned");
        if fence.is_some_and(|fence| state.dropped.contains(&fence)) {
            return Check::FenceDropped;
        }
        if !self.tracking.load(Ordering::Acquire) {
            return Check::Unknown;
        }
        let Some(branch) = state.branches.get(ledger_id) else {
            return Check::Unknown;
        };
        match branch.seen {
            None => Check::Unknown,
            Some(Seen::Live {
                fence,
                instance,
                head_t,
                journal,
                config_version,
                ..
            }) => Check::Known(Seen::Live {
                fence,
                instance,
                head_t,
                journal,
                config_version,
                history: branch.history,
            }),
            Some(other) => Check::Known(other),
        }
    }

    /// The branch `ledger_id` was made from (`Some(None)` for none), when
    /// what is kept of it is current; see [`Self::revalidation_due`].
    pub(crate) fn source_branch(&self, ledger_id: &str) -> Option<Option<String>> {
        if !self.tracking.load(Ordering::Acquire) {
            return None;
        }
        let state = self.state.read().expect("watch lock never poisoned");
        let branch = state.branches.get(ledger_id)?;
        let fresh = self.coherent.load(Ordering::Acquire)
            || super::worker::now_ms() - branch.validated_ms < REVALIDATE_MS;
        match branch.seen {
            Some(Seen::Live { .. }) if fresh => Some(branch.source.clone()),
            _ => None,
        }
    }

    /// Whether what is kept of `ledger_id` is due to be learned again, in
    /// the background: never on an instance that observes every change,
    /// and once per [`REVALIDATE_MS`] on one sharing its storage. A `true`
    /// claims the read; [`Self::revalidated`] ends it.
    pub(crate) fn revalidation_due(&self, ledger_id: &str) -> bool {
        if self.coherent.load(Ordering::Acquire) || !self.tracking.load(Ordering::Acquire) {
            return false;
        }
        let now = super::worker::now_ms();
        let due = |branch: &Branch| {
            branch.seen.is_some()
                && !branch.revalidating
                && (!branch.replicated || branch.version().is_none())
                && now - branch.validated_ms >= REVALIDATE_MS
        };
        {
            let state = self.state.read().expect("watch lock never poisoned");
            if !state.branches.get(ledger_id).is_some_and(due) {
                return false;
            }
        }
        let mut state = self.state.write().expect("watch lock never poisoned");
        match state.branches.get_mut(ledger_id) {
            Some(branch) if due(branch) => {
                branch.revalidating = true;
                true
            }
            _ => false,
        }
    }

    /// End a read [`Self::revalidation_due`] claimed: record what it
    /// found, unless something changed the branch since `token`.
    pub(crate) fn revalidated(
        &self,
        ledger_id: &str,
        found: Option<(Option<&NsRecord>, Option<Holds>)>,
        token: u64,
    ) {
        let mut state = self.state.write().expect("watch lock never poisoned");
        let Some(branch) = state.branches.get_mut(ledger_id) else {
            return;
        };
        branch.revalidating = false;
        let Some((record, holds)) = found else {
            return;
        };
        if branch.touched > token {
            return;
        }
        match record {
            Some(record) => branch.adopt(record),
            None => branch.seen = Some(Seen::Absent),
        }
        if let (Some(holds), Some(Seen::Live { journal, .. })) = (holds, &mut branch.seen) {
            *journal = Some(holds);
        }
        branch.validated_ms = super::worker::now_ms();
    }

    /// The ledgers (canonically spelled) graph source `id` indexes, when
    /// known: until a graph source changes on an instance that observes
    /// every change, for [`REVALIDATE_MS`] on one sharing its storage.
    pub(crate) fn source_dependencies(&self, id: &str) -> Option<std::sync::Arc<[String]>> {
        if !self.tracking.load(Ordering::Acquire) {
            return None;
        }
        let state = self.state.read().expect("watch lock never poisoned");
        let source = state.sources.get(id)?;
        let current = self.coherent.load(Ordering::Acquire)
            || super::worker::now_ms() - source.learned_ms < REVALIDATE_MS;
        current.then(|| std::sync::Arc::clone(&source.dependencies))
    }

    /// Record the ledgers graph source `id` indexes, looked up after
    /// `token`, unless a graph source changed since.
    pub(crate) fn learn_source(&self, id: &str, dependencies: Vec<String>, token: u64) {
        let mut state = self.state.write().expect("watch lock never poisoned");
        if state.sources_changed > token {
            return;
        }
        state.sources.insert(
            id.to_string(),
            Source {
                dependencies: dependencies.into(),
                learned_ms: super::worker::now_ms(),
            },
        );
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
        match record {
            Some(record) => branch.adopt(record),
            None => branch.seen = Some(Seen::Absent),
        }
        branch.validated_ms = super::worker::now_ms();
    }

    /// Record what a journal read after `token` holds, unless something
    /// changed the branch since.
    pub(crate) fn learn_journal(&self, ledger_id: &str, holds: Holds, token: u64) {
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
        self.touch(ledger_id, |branch| {
            if let Some(Seen::Live {
                journal,
                config_version,
                ..
            }) = &mut branch.seen
            {
                *journal = None;
                *config_version = None;
            }
        });
    }

    /// What is kept of `ledger_id` no longer holds, its history included:
    /// it is looked up again.
    pub(crate) fn forget(&self, ledger_id: &str) {
        self.touch(ledger_id, |branch| {
            branch.seen = None;
            branch.history += 1;
        });
    }

    pub(crate) fn forget_all(&self) {
        let mut state = self.state.write().expect("watch lock never poisoned");
        state.seq += 1;
        let seq = state.seq;
        state.sources_changed = seq;
        state.sources.clear();
        for branch in state.branches.values_mut() {
            branch.seen = None;
            branch.history += 1;
            branch.touched = seq;
        }
    }

    /// Apply a nameservice event.
    pub(crate) fn observe(&self, event: &NameServiceEvent) {
        match event {
            NameServiceEvent::LedgerConfigPublished {
                ledger_id,
                config_version,
            } => {
                self.touch(ledger_id.as_str(), |branch| {
                    if let Some(Seen::Live {
                        journal,
                        config_version: version,
                        ..
                    }) = &mut branch.seen
                    {
                        *journal = None;
                        *version =
                            Some(version.map_or(*config_version, |v| v.max(*config_version)));
                    }
                });
            }
            NameServiceEvent::LedgerRefresh { ledger_id } => {
                self.forget(ledger_id.as_str());
            }
            NameServiceEvent::LedgerCommitPublished {
                ledger_id,
                commit_t,
                ..
            } => {
                self.touch(ledger_id.as_str(), |branch| {
                    if let Some(Seen::Live { head_t, .. }) = &mut branch.seen {
                        // A head set below the one known: a reset.
                        if *commit_t < *head_t {
                            branch.history += 1;
                        }
                        *head_t = (*head_t).max(*commit_t);
                    }
                });
            }
            NameServiceEvent::LedgerRetracted { ledger_id, .. } => {
                self.touch(ledger_id.as_str(), |branch| {
                    branch.seen = Some(Seen::Dropped);
                    branch.history += 1;
                });
            }
            // Possibly another incarnation under the name: look it up again.
            NameServiceEvent::LedgerCreated { ledger_id, .. } => {
                self.forget(ledger_id.as_str());
            }
            NameServiceEvent::GraphSourceConfigPublished { .. }
            | NameServiceEvent::GraphSourceRetracted { .. } => {
                let mut state = self.state.write().expect("watch lock never poisoned");
                state.seq += 1;
                state.sources_changed = state.seq;
                state.sources.clear();
            }
            _ => {}
        }
    }

    fn touch(&self, ledger_id: &str, update: impl FnOnce(&mut Branch)) {
        let mut state = self.state.write().expect("watch lock never poisoned");
        state.seq += 1;
        let seq = state.seq;
        let branch = state.branches.entry(ledger_id.to_string()).or_default();
        branch.touched = seq;
        update(branch);
    }
}

impl Branch {
    fn version(&self) -> Option<i64> {
        match self.seen {
            Some(Seen::Live { config_version, .. }) => config_version,
            _ => None,
        }
    }

    /// Take `record` as what the branch is now. What its journal holds is
    /// kept while the record names the same incarnation and configuration
    /// revision; a head below the one known, or another incarnation,
    /// moves its history.
    fn adopt(&mut self, record: &NsRecord) {
        self.source.clone_from(&record.source_branch);
        let mut next = Seen::live(record);
        if let (
            Some(Seen::Live {
                fence: a,
                instance: b,
                config_version: c,
                journal: prior,
                head_t: was,
                ..
            }),
            Seen::Live {
                fence: d,
                instance: e,
                config_version: f,
                journal,
                head_t,
                ..
            },
        ) = (self.seen, &mut next)
        {
            if (a, b) != (*d, *e) || *head_t < was {
                self.history += 1;
            }
            if (a, b, c) == (*d, *e, *f) && journal.is_none() {
                *journal = prior;
            }
        }
        self.seen = Some(next);
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
            journal: (record.config_version == Some(0)).then_some(Holds::Nothing),
            config_version: record.config_version,
            history: 0,
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
            config_version: None,
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
            config_version: None,
            history: 0,
        };
        assert_eq!(get(&watch), Some(live(5, None)));
        watch.learn_journal("w:main", Holds::Nothing, watch.token());
        assert_eq!(get(&watch), Some(live(5, Some(Holds::Nothing))));
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
        watch.learn_journal("w:main", Holds::Nothing, token);
        assert!(matches!(
            get(&watch),
            Some(Seen::Live { journal: None, .. })
        ));
    }

    #[test]
    fn a_peer_learns_config_changes_at_the_same_head_and_on_reconnect() {
        let watch = tracking();
        let mut r = record(3);
        r.config_version = Some(0);
        watch.remote_record(&r);
        assert!(matches!(
            get(&watch),
            Some(Seen::Live {
                journal: Some(Holds::Nothing),
                ..
            })
        ));
        let token = watch.token();
        r.config_version = Some(1);
        watch.remote_record(&r);
        watch.learn_journal("w:main", Holds::Nothing, token);
        assert!(matches!(
            get(&watch),
            Some(Seen::Live {
                journal: None,
                config_version: Some(1),
                ..
            })
        ));
        watch.learn_journal("w:main", Holds::Removals, watch.token());
        watch.remote_record(&r); // unchanged reconnect snapshot
        assert!(matches!(
            get(&watch),
            Some(Seen::Live {
                journal: Some(Holds::Removals),
                ..
            })
        ));
        r.config_version = None; // legacy records are unknown, never zero
        watch.remote_record(&r);
        assert!(matches!(
            get(&watch),
            Some(Seen::Live { journal: None, .. })
        ));
        // Nor is a journal learned for one kept past its next read.
        watch.learn_journal("w:main", Holds::Nothing, watch.token());
        assert!(watch.revalidation_due("w:main"));
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

    /// An instance holding its storage root's lock writes every journal
    /// there; one opened without the lock may share the root with another,
    /// so it learns what it keeps again once that is old enough.
    #[cfg(feature = "native")]
    #[tokio::test]
    async fn only_a_locked_root_makes_its_instance_the_sole_writer() {
        for locked in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let mut builder = crate::FlureeBuilder::file(dir.path().to_string_lossy().to_string())
                .without_indexing();
            if !locked {
                builder = builder.without_storage_lock();
            }
            let fluree = builder.build().unwrap();
            fluree.create_ledger("w").await.unwrap();
            let watch = &fluree.retention.watch;
            fluree.suppression_for("w:main").await.unwrap();
            assert!(matches!(
                get(watch),
                Some(Seen::Live {
                    journal: Some(Holds::Nothing),
                    ..
                })
            ));
            backdate(watch, "w:main");
            assert_eq!(watch.revalidation_due("w:main"), !locked);
        }
    }

    /// What an instance sharing its storage keeps of a branch is served
    /// until it is due, learned again once, and kept across that read when
    /// the record names the same revision.
    #[test]
    fn a_shared_storage_instance_learns_a_branch_again_once_it_is_due() {
        let watch = tracking();
        let mut r = record(3);
        r.config_version = Some(2);
        watch.learn("w:main", Some(&r), watch.token());
        watch.learn_journal("w:main", Holds::Rules, watch.token());
        assert!(!watch.revalidation_due("w:main"));
        backdate(&watch, "w:main");
        assert!(watch.revalidation_due("w:main"));
        // Claimed: one read at a time.
        assert!(!watch.revalidation_due("w:main"));
        let token = watch.token();
        watch.revalidated("w:main", Some((Some(&r), None)), token);
        assert!(matches!(
            get(&watch),
            Some(Seen::Live {
                journal: Some(Holds::Rules),
                ..
            })
        ));
        assert!(!watch.revalidation_due("w:main"));
        // A new revision: what the journal holds is read again.
        backdate(&watch, "w:main");
        assert!(watch.revalidation_due("w:main"));
        r.config_version = Some(3);
        watch.revalidated("w:main", Some((Some(&r), None)), watch.token());
        assert!(matches!(
            get(&watch),
            Some(Seen::Live { journal: None, .. })
        ));
        // A failed read releases the claim and keeps what is known.
        backdate(&watch, "w:main");
        assert!(watch.revalidation_due("w:main"));
        watch.revalidated("w:main", None, watch.token());
        assert!(watch.revalidation_due("w:main"));
    }

    /// A head set below the one known, a refresh or another incarnation
    /// moves a branch's history; commits added at its head do not.
    #[test]
    fn only_a_rewrite_moves_a_branchs_history() {
        let watch = tracking();
        watch.set_sole_writer();
        let history = |watch: &BranchWatch| match get(watch) {
            Some(Seen::Live { history, .. }) => history,
            other => panic!("{other:?}"),
        };
        watch.learn("w:main", Some(&record(3)), watch.token());
        let start = history(&watch);
        watch.observe(&commit(4));
        watch.observe(&commit(4));
        assert_eq!(history(&watch), start);
        watch.observe(&commit(2));
        assert_eq!(history(&watch), start + 1);
        watch.forget("w:main");
        watch.learn("w:main", Some(&record(4)), watch.token());
        assert_eq!(history(&watch), start + 2);
        let mut other = record(4);
        other.fence = Some(Fence::generate());
        watch.learn("w:main", Some(&other), watch.token());
        assert_eq!(history(&watch), start + 3);
    }

    /// A graph source's dependencies are kept until a graph source
    /// changes.
    #[test]
    fn a_graph_source_change_forgets_dependencies() {
        let watch = tracking();
        watch.set_sole_writer();
        let token = watch.token();
        watch.learn_source("gs", vec!["w:main".into()], token);
        assert_eq!(
            watch.source_dependencies("gs").as_deref(),
            Some(&["w:main".to_string()][..])
        );
        watch.observe(&NameServiceEvent::GraphSourceRetracted {
            graph_source_id: LedgerId::parse("gs:main").unwrap(),
        });
        assert!(watch.source_dependencies("gs").is_none());
        // A lookup overtaken by a change is not kept.
        watch.learn_source("gs", vec!["w:main".into()], token);
        assert!(watch.source_dependencies("gs").is_none());
    }

    /// Make what is kept of `ledger_id` old enough to be learned again.
    fn backdate(watch: &BranchWatch, ledger_id: &str) {
        let mut state = watch.state.write().unwrap();
        state.branches.get_mut(ledger_id).unwrap().validated_ms -= REVALIDATE_MS;
    }
}
