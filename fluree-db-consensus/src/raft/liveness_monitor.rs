//! Leader-only worker-eligibility monitor.
//!
//! Samples per-peer replication state from openraft's `RaftMetrics`
//! at a fixed interval. When a voter's match log stops advancing
//! while the leader's own log grows past it, the monitor counts the
//! peer as lagging; sustained lag past
//! [`LivenessConfig::unreachable_after`] triggers a
//! [`Command::SetWorkerEligibility`] propose with `eligible: false`,
//! demoting the voter from
//! [`NameServiceState::worker_eligible_voters`](crate::raft::state_machine::NameServiceState::worker_eligible_voters).
//! Restored advancement past [`LivenessConfig::live_after`] flips
//! the voter back. Idempotent: once a voter's state matches the
//! monitor's last proposed flag, the monitor stops proposing until
//! the state flips again.
//!
//! Per-process scope: trackers live in the [`run`](LivenessMonitor::run)
//! loop and disappear when the leader watcher aborts the task on
//! leader transition. The next leader's monitor starts fresh; the
//! replicated `worker_eligible_voters` survives the gap.

use crate::raft::state_machine::{Command as SmCommand, Response as SmResponse, WorkerEligibility};
use crate::raft::{NodeId, TypeConfig};
use openraft::{LogId, Raft};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, warn};

/// Default interval between metric samples.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Default time a peer's match log can stay stuck (while the
/// leader's last log advances past it) before being proposed as
/// ineligible. Sized to absorb transient slowness — short GC
/// pauses, a brief network jitter — without demoting healthy peers.
pub const DEFAULT_UNREACHABLE_AFTER: Duration = Duration::from_secs(15);

/// Default minimum time a previously-demoted peer must show
/// advancement before being proposed as eligible again. Strictly
/// less than [`DEFAULT_UNREACHABLE_AFTER`] so the live/unreachable
/// pair forms a hysteresis window — a flapping peer can't bounce
/// between states once per tick.
pub const DEFAULT_LIVE_AFTER: Duration = Duration::from_secs(5);

/// Threshold tuning for the liveness monitor.
#[derive(Clone, Debug)]
pub struct LivenessConfig {
    /// Interval between metric samples.
    pub sample_interval: Duration,
    /// How long a peer's match log can stay stuck (while the
    /// leader's log advances past it) before being proposed
    /// ineligible.
    pub unreachable_after: Duration,
    /// How long a previously-demoted peer must show advancement
    /// before being proposed eligible again. Should be strictly
    /// less than `unreachable_after` to keep the hysteresis window
    /// non-degenerate.
    pub live_after: Duration,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            unreachable_after: DEFAULT_UNREACHABLE_AFTER,
            live_after: DEFAULT_LIVE_AFTER,
        }
    }
}

/// The eligibility change the monitor wants to propose for a peer.
/// `Promote` adds the voter to
/// [`NameServiceState::worker_eligible_voters`](crate::raft::state_machine::NameServiceState::worker_eligible_voters);
/// `Demote` removes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EligibilityProposal {
    Promote,
    Demote,
}

/// Per-peer state held in the monitor's run loop.
#[derive(Debug)]
struct PeerTracker {
    /// Last `LogId` observed for this peer in the leader's
    /// replication metrics.
    last_observed_log: Option<LogId<NodeId>>,
    /// Wall-clock when this peer first showed unhealthy lag
    /// (its match log stuck while the leader's log grew past it).
    /// `None` while the peer is currently advancing on schedule.
    unreachable_since: Option<Instant>,
    /// Wall-clock when, after the most recent demotion, the peer
    /// first showed advancement again. `None` while the peer is
    /// currently lagging or has never been demoted by the monitor.
    /// Held separately from log-advance timestamps so subsequent
    /// advances don't reset the recovery window — the promote
    /// check measures elapsed time since the *first* post-demotion
    /// advance, not the most recent one.
    recovering_since: Option<Instant>,
    /// The last [`EligibilityProposal`] this monitor landed for the
    /// peer, or `None` if it's never proposed. Read as hysteresis:
    /// re-propose only when the desired proposal differs from the
    /// last one we landed.
    last_proposed: Option<EligibilityProposal>,
}

impl PeerTracker {
    fn new() -> Self {
        Self {
            last_observed_log: None,
            unreachable_since: None,
            recovering_since: None,
            last_proposed: None,
        }
    }
}

/// Leader-only monitor that proposes [`Command::SetWorkerEligibility`]
/// based on observed per-peer replication state. Cloning is cheap
/// (`Arc` clone of the raft handle); a single instance is driven by
/// the leader watcher.
#[derive(Clone)]
pub struct LivenessMonitor {
    raft: Arc<Raft<TypeConfig>>,
    config: LivenessConfig,
}

impl LivenessMonitor {
    pub fn new(raft: Arc<Raft<TypeConfig>>) -> Self {
        Self {
            raft,
            config: LivenessConfig::default(),
        }
    }

    /// Override the threshold tuning.
    pub fn with_config(mut self, config: LivenessConfig) -> Self {
        self.config = config;
        self
    }

    /// Drive the sample/propose loop until the leader watcher aborts.
    /// Trackers live in this stack frame so leader transition drops
    /// them on abort.
    pub async fn run(self) {
        let mut trackers: HashMap<NodeId, PeerTracker> = HashMap::new();
        loop {
            tokio::time::sleep(self.config.sample_interval).await;
            self.tick(&mut trackers, Instant::now()).await;
        }
    }

    async fn tick(&self, trackers: &mut HashMap<NodeId, PeerTracker>, now: Instant) {
        let metrics = self.raft.metrics().borrow().clone();
        // No replication metrics = not the leader. The leader watcher
        // ordinarily aborts the task before this fires; the guard
        // protects the gap.
        let Some(replication) = metrics.replication.as_ref() else {
            return;
        };
        let leader_id = metrics.id;
        let leader_last_log = metrics.last_log_index;

        for (peer_id, peer_log) in replication {
            if *peer_id == leader_id {
                continue;
            }
            let tracker = trackers.entry(*peer_id).or_insert_with(PeerTracker::new);
            record_replication_progress(tracker, *peer_log, leader_last_log, now);
            match next_eligibility_proposal(tracker, now, &self.config) {
                Some(EligibilityProposal::Promote) => {
                    self.propose_eligible(*peer_id).await;
                    tracker.last_proposed = Some(EligibilityProposal::Promote);
                    tracker.recovering_since = None;
                }
                Some(EligibilityProposal::Demote) => {
                    self.propose_ineligible(*peer_id).await;
                    tracker.last_proposed = Some(EligibilityProposal::Demote);
                }
                None => {}
            }
        }
        // Drop trackers for peers that left the voter set on a
        // membership change. Keeps memory bounded across cluster
        // lifetime.
        let configured: std::collections::HashSet<NodeId> =
            metrics.membership_config.membership().voter_ids().collect();
        trackers.retain(|id, _| configured.contains(id));
    }

    async fn propose_eligible(&self, voter: NodeId) {
        self.propose_eligibility(WorkerEligibility {
            voter,
            eligible: true,
            applied_at_millis: now_millis(),
        })
        .await;
    }

    async fn propose_ineligible(&self, voter: NodeId) {
        self.propose_eligibility(WorkerEligibility {
            voter,
            eligible: false,
            applied_at_millis: now_millis(),
        })
        .await;
    }

    async fn propose_eligibility(&self, args: WorkerEligibility) {
        let voter = args.voter;
        let eligible = args.eligible;
        let cmd = SmCommand::SetWorkerEligibility(args);
        match self.raft.client_write(cmd).await {
            Ok(resp) => match resp.data {
                SmResponse::WorkerEligibilitySet { changed: true, .. } => {
                    debug!(voter, eligible, "worker eligibility flipped");
                }
                SmResponse::WorkerEligibilitySet { changed: false, .. } => {
                    // Idempotent re-apply — state already matched.
                    // Common on monitor restart against a state
                    // machine the prior leader's monitor left
                    // populated.
                }
                SmResponse::WorkerEligibilityRefused { reason, .. } => {
                    warn!(voter, eligible, ?reason, "eligibility propose refused");
                }
                other => {
                    warn!(
                        voter,
                        eligible,
                        ?other,
                        "unexpected eligibility propose response"
                    );
                }
            },
            Err(err) => warn!(voter, eligible, error = %err, "eligibility propose failed"),
        }
    }
}

/// Record the peer's current match log and the leader's last log
/// into the tracker. Updates the log-observation pointer and the
/// in-progress unreachable / recovery timers; doesn't decide
/// whether anything should be proposed — that's
/// [`next_eligibility_proposal`]'s job.
fn record_replication_progress(
    tracker: &mut PeerTracker,
    current_log: Option<LogId<NodeId>>,
    leader_last_log: Option<u64>,
    now: Instant,
) {
    if log_advanced(&tracker.last_observed_log, &current_log) {
        tracker.last_observed_log = current_log;
        tracker.unreachable_since = None;
        // First advance after a demotion starts the recovery
        // window; subsequent advances don't push the start forward
        // — the promote check needs a non-moving timestamp to
        // measure against.
        if tracker.last_proposed == Some(EligibilityProposal::Demote)
            && tracker.recovering_since.is_none()
        {
            tracker.recovering_since = Some(now);
        }
    } else if leader_is_ahead_of_peer(leader_last_log, &tracker.last_observed_log) {
        // No advance from the peer while the leader has new entries
        // it hasn't matched — the peer is lagging. Start the
        // unreachable timer on the first sample that observes the
        // lag; subsequent samples don't reset it until the peer
        // advances.
        tracker.unreachable_since.get_or_insert(now);
        tracker.recovering_since = None;
    }
}

/// Read the tracker's accumulated timers and the hysteresis flag,
/// and return the next [`EligibilityProposal`] to fire if one is
/// warranted. Pure — no mutation. The caller updates
/// [`PeerTracker::last_proposed`] (and clears
/// [`PeerTracker::recovering_since`] on a successful promote) after
/// landing the propose.
fn next_eligibility_proposal(
    tracker: &PeerTracker,
    now: Instant,
    config: &LivenessConfig,
) -> Option<EligibilityProposal> {
    if let Some(since) = tracker.unreachable_since {
        if now.saturating_duration_since(since) >= config.unreachable_after
            && tracker.last_proposed != Some(EligibilityProposal::Demote)
        {
            return Some(EligibilityProposal::Demote);
        }
    }
    if tracker.last_proposed == Some(EligibilityProposal::Demote) {
        if let Some(recovering_since) = tracker.recovering_since {
            if now.saturating_duration_since(recovering_since) >= config.live_after {
                return Some(EligibilityProposal::Promote);
            }
        }
    }
    None
}

fn log_advanced(prev: &Option<LogId<NodeId>>, curr: &Option<LogId<NodeId>>) -> bool {
    match (prev, curr) {
        (None, Some(_)) => true,
        (Some(p), Some(c)) => c.index > p.index,
        _ => false,
    }
}

fn leader_is_ahead_of_peer(leader_last_log: Option<u64>, peer_log: &Option<LogId<NodeId>>) -> bool {
    match (leader_last_log, peer_log) {
        (Some(leader_idx), Some(peer_log)) => leader_idx > peer_log.index,
        (Some(_), None) => true,
        _ => false,
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        }
    }

    fn fast_config() -> LivenessConfig {
        // Sub-second thresholds keep the tests deterministic and
        // fast — the relevant invariant is the ordering of the
        // two thresholds, not their absolute size.
        LivenessConfig {
            sample_interval: Duration::from_millis(1),
            unreachable_after: Duration::from_millis(100),
            live_after: Duration::from_millis(30),
        }
    }

    /// Mirror of the `record_replication_progress` + `next_eligibility_proposal`
    /// pair the monitor runs each tick — plus the post-propose state
    /// updates the caller does at the dispatch site. Tests use this
    /// to advance the tracker one sample at a time.
    fn tick(
        tracker: &mut PeerTracker,
        current_log: Option<LogId<NodeId>>,
        leader_last_log: Option<u64>,
        now: Instant,
        config: &LivenessConfig,
    ) -> Option<EligibilityProposal> {
        record_replication_progress(tracker, current_log, leader_last_log, now);
        let proposal = next_eligibility_proposal(tracker, now, config);
        match proposal {
            Some(EligibilityProposal::Promote) => {
                tracker.last_proposed = Some(EligibilityProposal::Promote);
                tracker.recovering_since = None;
            }
            Some(EligibilityProposal::Demote) => {
                tracker.last_proposed = Some(EligibilityProposal::Demote);
            }
            None => {}
        }
        proposal
    }

    #[test]
    fn first_sample_with_advancing_peer_proposes_nothing() {
        // Fresh tracker observes the peer advancing from None to
        // some log id. No prior proposal to override, no lag to
        // record — the monitor stays silent.
        let now = Instant::now();
        let mut tracker = PeerTracker::new();
        let decision = tick(
            &mut tracker,
            Some(log_id(1, 5)),
            Some(5),
            now,
            &fast_config(),
        );
        assert_eq!(decision, None);
        assert_eq!(tracker.last_observed_log, Some(log_id(1, 5)));
        assert!(tracker.unreachable_since.is_none());
    }

    #[test]
    fn peer_stuck_with_leader_ahead_proposes_demote_after_unreachable_after() {
        // First sample: leader at 5, peer at 5, healthy.
        // Subsequent samples: leader advances to 10, peer stuck at
        // 5. After `unreachable_after` elapses, demote.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::new();
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(5), t0, &cfg);
        // Leader runs ahead, peer doesn't follow.
        let t1 = t0 + cfg.sample_interval;
        let d1 = tick(&mut tracker, Some(log_id(1, 5)), Some(10), t1, &cfg);
        assert_eq!(d1, None, "unreachable_after hasn't elapsed yet");
        assert!(tracker.unreachable_since.is_some());
        // Past the threshold.
        let t2 = t1 + cfg.unreachable_after;
        let d2 = tick(&mut tracker, Some(log_id(1, 5)), Some(10), t2, &cfg);
        assert_eq!(
            d2,
            Some(EligibilityProposal::Demote),
            "monitor demotes after unreachable_after"
        );
    }

    #[test]
    fn demote_proposal_is_not_repeated_while_peer_stays_stuck() {
        // After a demotion, every subsequent stuck-with-leader-
        // ahead sample stays silent — the hysteresis check sees
        // `last_proposed == Some(Demote)` and bails.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::new();
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(5), t0, &cfg);
        let t1 = t0 + cfg.unreachable_after;
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(10), t1, &cfg);
        tracker.last_proposed = Some(EligibilityProposal::Demote);
        // Another sample after a long wait — peer still stuck.
        let t2 = t1 + cfg.unreachable_after;
        let d = tick(&mut tracker, Some(log_id(1, 5)), Some(10), t2, &cfg);
        assert_eq!(d, None, "demote proposal should not repeat");
    }

    #[test]
    fn recovered_peer_proposes_promote_after_live_after() {
        // Tracker is in a post-demote state. First advance starts
        // the recovery window; once `live_after` elapses, promote.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::new();
        tracker.last_observed_log = Some(log_id(1, 5));
        tracker.last_proposed = Some(EligibilityProposal::Demote);
        // First advance after demotion starts recovering_since.
        let t1 = t0 + cfg.sample_interval;
        let d1 = tick(&mut tracker, Some(log_id(1, 6)), Some(10), t1, &cfg);
        assert_eq!(d1, None, "live_after hasn't elapsed yet");
        assert_eq!(tracker.recovering_since, Some(t1));
        // Second advance just before live_after has elapsed.
        let t2 = t1 + cfg.live_after / 2;
        let d2 = tick(&mut tracker, Some(log_id(1, 7)), Some(10), t2, &cfg);
        assert_eq!(d2, None, "still inside live_after window");
        assert_eq!(
            tracker.recovering_since,
            Some(t1),
            "recovery anchor doesn't move on subsequent advances"
        );
        // Past the live_after threshold.
        let t3 = t1 + cfg.live_after;
        let d3 = tick(&mut tracker, Some(log_id(1, 8)), Some(10), t3, &cfg);
        assert_eq!(
            d3,
            Some(EligibilityProposal::Promote),
            "monitor promotes after live_after"
        );
        assert_eq!(
            tracker.recovering_since, None,
            "promote clears the recovery anchor"
        );
    }

    #[test]
    fn recovery_window_resets_if_peer_stalls_again_before_live_after() {
        // Demoted peer advances briefly, then stalls again before
        // live_after elapses. The recovery anchor clears; the
        // monitor stays at `last_proposed: Some(Demote)` without
        // ever firing a promote.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::new();
        tracker.last_observed_log = Some(log_id(1, 5));
        tracker.last_proposed = Some(EligibilityProposal::Demote);
        // Brief advance.
        let t1 = t0 + cfg.sample_interval;
        let _ = tick(&mut tracker, Some(log_id(1, 6)), Some(10), t1, &cfg);
        assert!(tracker.recovering_since.is_some());
        // Stalls again while leader keeps moving.
        let t2 = t1 + cfg.sample_interval;
        let d = tick(&mut tracker, Some(log_id(1, 6)), Some(20), t2, &cfg);
        assert_eq!(d, None);
        assert!(
            tracker.recovering_since.is_none(),
            "stall clears recovery anchor"
        );
    }

    #[test]
    fn idle_leader_and_idle_peer_proposes_nothing() {
        // No log advance from either side. The monitor has no
        // signal — never proposes anything, never tips into
        // unreachable.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::new();
        tracker.last_observed_log = Some(log_id(1, 5));
        // Many ticks pass; nothing advances.
        for i in 1..50 {
            let now = t0 + cfg.sample_interval * i;
            let d = tick(&mut tracker, Some(log_id(1, 5)), Some(5), now, &cfg);
            assert_eq!(d, None);
        }
        assert!(tracker.unreachable_since.is_none());
    }

    #[test]
    fn peer_advancing_clears_an_in_progress_unreachable_window() {
        // Peer goes stuck, accumulates `unreachable_since`, then
        // advances before the demotion threshold. The window
        // resets — no demote.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::new();
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(5), t0, &cfg);
        let t1 = t0 + cfg.sample_interval;
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(10), t1, &cfg);
        assert!(tracker.unreachable_since.is_some());
        // Peer recovers before unreachable_after elapses.
        let t2 = t1 + cfg.unreachable_after / 2;
        let d = tick(&mut tracker, Some(log_id(1, 10)), Some(10), t2, &cfg);
        assert_eq!(d, None, "advance before threshold = no demote");
        assert!(tracker.unreachable_since.is_none());
    }

    #[test]
    fn previously_unproposed_healthy_peer_does_not_propose_promote() {
        // A healthy peer that the monitor never demoted shouldn't
        // get a redundant promote propose just because it crossed
        // `live_after`. The propose only fires to flip a state the
        // monitor itself put in place.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::new();
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(5), t0, &cfg);
        let t1 = t0 + cfg.live_after * 10;
        let d = tick(&mut tracker, Some(log_id(1, 10)), Some(10), t1, &cfg);
        assert_eq!(d, None);
    }
}
