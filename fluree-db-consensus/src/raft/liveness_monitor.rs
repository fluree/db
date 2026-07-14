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
//! leader transition. The next leader's monitor seeds each tracker's
//! hysteresis from the replicated `worker_eligible_voters` set on
//! first observation, so demotions a prior leader landed survive the
//! gap and the Promote path can still fire when the demoted peer
//! demonstrates recovery on the new leader.

use crate::raft::state_machine::{Command as SmCommand, Response as SmResponse, WorkerEligibility};
use crate::raft::state_machine_adapter::SharedState;
use crate::raft::{NodeId, TypeConfig};
use openraft::{LogId, Raft};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Default interval between metric samples.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Default time a peer's match log can stay more than
/// [`DEFAULT_MAX_HEALTHY_LAG`] entries behind the leader before being
/// proposed as ineligible. Sized to absorb transient slowness —
/// short GC pauses, a brief network jitter, a replication burst the
/// peer is catching up on — without demoting healthy peers.
pub const DEFAULT_UNREACHABLE_AFTER: Duration = Duration::from_secs(15);

/// Default maximum number of log entries a peer's match log may trail
/// the leader's last log and still count as healthy. Measures lag as
/// *distance*, not "did the match index move at all": a peer matching
/// one entry per sample while the leader gains thousands is advancing
/// yet falling further behind, and only a distance measure demotes
/// it. Set comfortably above the deepest legitimate in-flight
/// replication window (openraft ships entries in batches, so a
/// keeping-up follower trails by at most a batch or two) so normal
/// pipelining never trips it; a peer that stays beyond it for
/// [`DEFAULT_UNREACHABLE_AFTER`] genuinely can't keep pace.
pub const DEFAULT_MAX_HEALTHY_LAG: u64 = 1000;

/// Default minimum time a previously-demoted peer must show
/// advancement before being proposed as eligible again. Strictly
/// less than [`DEFAULT_UNREACHABLE_AFTER`] so the live/unreachable
/// pair forms a hysteresis window — a flapping peer can't bounce
/// between states once per tick.
pub const DEFAULT_LIVE_AFTER: Duration = Duration::from_secs(5);

/// Default minimum time between consecutive eligibility proposes
/// for the same peer after a refusal. A refused propose (the apply
/// refuses a voter no longer in the configured set —
/// `VoterNotConfigured`) still commits a raft log entry that
/// applies as a no-op; without backoff, the monitor would commit
/// one such entry every [`DEFAULT_SAMPLE_INTERVAL`] until the
/// refusal condition clears. Set well above the sample interval
/// so the cost amortizes; small enough that a transient refusal
/// resolves within an operator's normal failover patience.
pub const DEFAULT_REFUSAL_BACKOFF: Duration = Duration::from_secs(30);

/// Threshold tuning for the liveness monitor.
#[derive(Clone, Debug)]
pub struct LivenessConfig {
    /// Interval between metric samples.
    pub sample_interval: Duration,
    /// How long a peer's match log can stay more than
    /// [`Self::max_healthy_lag`] entries behind the leader before
    /// being proposed ineligible.
    pub unreachable_after: Duration,
    /// Maximum entries a peer may trail the leader's last log and
    /// still count as keeping up. Lag beyond this — sustained past
    /// [`Self::unreachable_after`] — demotes the peer even if its
    /// match index is still inching forward. See
    /// [`DEFAULT_MAX_HEALTHY_LAG`].
    pub max_healthy_lag: u64,
    /// How long a previously-demoted peer must show advancement
    /// before being proposed eligible again. Should be strictly
    /// less than `unreachable_after` to keep the hysteresis window
    /// non-degenerate.
    pub live_after: Duration,
    /// Minimum time between consecutive eligibility proposes for
    /// the same peer after the prior one was refused. Caps the
    /// raft-log commit rate when the refusal condition
    /// (`VoterNotConfigured`) is persistent.
    pub refusal_backoff: Duration,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            unreachable_after: DEFAULT_UNREACHABLE_AFTER,
            live_after: DEFAULT_LIVE_AFTER,
            refusal_backoff: DEFAULT_REFUSAL_BACKOFF,
            max_healthy_lag: DEFAULT_MAX_HEALTHY_LAG,
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
    /// The last [`EligibilityProposal`] this monitor expects the
    /// replicated state to reflect for this peer. Seeded from
    /// [`NameServiceState::worker_eligible_voters`](crate::raft::state_machine::NameServiceState::worker_eligible_voters)
    /// when the tracker is first created — so a peer the prior
    /// leader's monitor demoted carries `Some(Demote)` into the new
    /// leader's tracker — and updated whenever this monitor's own
    /// propose lands. Read as hysteresis: re-propose only when the
    /// desired proposal differs from this.
    last_proposed: Option<EligibilityProposal>,
    /// Wall-clock when the most recent eligibility propose for this
    /// peer was refused (or failed at the raft transport).
    /// [`next_eligibility_proposal`] short-circuits while less than
    /// [`LivenessConfig::refusal_backoff`] has elapsed since this
    /// timestamp, preventing the monitor from committing a raft
    /// log entry every sample tick when the refusal condition
    /// hasn't cleared. Cleared whenever a propose lands
    /// successfully — the underlying refusal (`VoterNotConfigured`)
    /// is by definition no longer in effect.
    last_refused: Option<Instant>,
}

impl PeerTracker {
    /// Tracker for a peer the replicated state shows as eligible.
    /// Used both for monitor startup against a peer that has never
    /// been demoted, and for newly-added voters whose first appearance
    /// in the eligible set is part of the membership change that
    /// added them.
    fn for_eligible_peer() -> Self {
        Self {
            last_observed_log: None,
            unreachable_since: None,
            recovering_since: None,
            last_proposed: None,
            last_refused: None,
        }
    }

    /// Tracker for a peer the replicated state shows as ineligible.
    /// Seeds `last_proposed` as if this monitor itself had landed
    /// the Demote, so the Promote path fires when the peer
    /// demonstrates recovery — covering the case where a prior
    /// leader's monitor proposed the demotion and this leader's
    /// fresh trackers wouldn't otherwise carry that history.
    fn for_ineligible_peer() -> Self {
        Self {
            last_observed_log: None,
            unreachable_since: None,
            recovering_since: None,
            last_proposed: Some(EligibilityProposal::Demote),
            last_refused: None,
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
    shared_state: SharedState,
    config: LivenessConfig,
}

impl LivenessMonitor {
    pub fn new(raft: Arc<Raft<TypeConfig>>, shared_state: SharedState) -> Self {
        Self {
            raft,
            shared_state,
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
        // Read what we need from the metrics ref and drop it
        // before any await. Cloning the whole `RaftMetrics` (the
        // earlier shape) deep-copied the per-peer replication
        // BTreeMap; here we just copy out the small fields we
        // need, leaving the watch ref free for openraft's writer.
        // Holding the ref across `propose_*` awaits would also
        // block metrics updates for the entire tick.
        let (leader_last_log, peers) = {
            let metrics_rx = self.raft.metrics();
            let metrics = metrics_rx.borrow();
            // No replication metrics = not the leader. The leader
            // watcher ordinarily aborts the task before this fires;
            // the guard protects the gap.
            let Some(replication) = metrics.replication.as_ref() else {
                return;
            };
            let leader_id = metrics.id;
            let leader_last_log = metrics.last_log_index;
            let peers: Vec<(NodeId, Option<LogId<NodeId>>)> = replication
                .iter()
                .filter(|(id, _)| **id != leader_id)
                .map(|(id, log)| (*id, *log))
                .collect();
            (leader_last_log, peers)
        };

        // Snapshot the replicated voter + eligibility sets together
        // so every per-peer tracker created in this tick reads a
        // consistent view, and so the per-peer awaits below don't
        // hold the state lock against writers. Membership-apply
        // updates both sets atomically (see `EntryPayload::Membership`
        // in the state-machine adapter), so the snapshot can't see a
        // voter in one set but not the other.
        let (configured_voters, eligible_voters) = {
            let state = self.shared_state.read().await;
            (
                state.configured_voters.clone(),
                state.worker_eligible_voters.clone(),
            )
        };

        for (peer_id, peer_log) in peers {
            // Learners appear in openraft's replication map from the
            // moment `add-learner` starts shipping them log entries,
            // but they aren't workers — `apply_set_worker_eligibility`
            // refuses proposes for any voter outside `configured_voters`.
            // Building a tracker for a learner seeds it as `for_ineligible_peer`
            // (it's not yet in `worker_eligible_voters`), and once
            // `change-membership` promotes the learner to a voter the
            // stale `last_proposed = Some(Demote)` blocks every future
            // Demote on this peer — including the one a subsequent
            // kill ought to fire. Skipping non-voters at the top is
            // what avoids that pre-promotion seeding.
            if !configured_voters.contains(&peer_id) {
                continue;
            }
            let tracker = trackers.entry(peer_id).or_insert_with(|| {
                if eligible_voters.contains(&peer_id) {
                    PeerTracker::for_eligible_peer()
                } else {
                    PeerTracker::for_ineligible_peer()
                }
            });
            record_replication_progress(
                tracker,
                peer_log,
                leader_last_log,
                self.config.max_healthy_lag,
                now,
            );
            let Some(proposal) = next_eligibility_proposal(tracker, now, &self.config) else {
                continue;
            };
            let landed = match proposal {
                EligibilityProposal::Promote => self.propose_eligible(peer_id).await,
                EligibilityProposal::Demote => self.propose_ineligible(peer_id).await,
            };
            if landed {
                tracker.last_refused = None;
                match proposal {
                    EligibilityProposal::Promote => {
                        tracker.last_proposed = Some(EligibilityProposal::Promote);
                        tracker.recovering_since = None;
                    }
                    EligibilityProposal::Demote => {
                        tracker.last_proposed = Some(EligibilityProposal::Demote);
                    }
                }
            } else {
                // Stamp the refusal so [`next_eligibility_proposal`]
                // short-circuits the next `refusal_backoff` worth of
                // ticks for this peer — refused proposes still cost a
                // raft log entry (committed and applied as no-op), so
                // re-firing every sample tick on a persistent refusal
                // is the bug this guards.
                tracker.last_refused = Some(now);
            }
        }
        // Drop trackers for peers that left the voter set on a
        // membership change. Cheap at realistic cluster sizes — a
        // BTreeSet lookup per tracked voter.
        trackers.retain(|id, _| configured_voters.contains(id));
    }

    /// Propose `eligible: true` for `voter`. Returns `true` when the
    /// state machine reports the eligible set now contains the voter
    /// (either flipped this call or already matched), `false` when
    /// the propose failed, was refused, or returned an unexpected
    /// response. A `false` return leaves the caller's hysteresis
    /// flag unchanged so the next tick re-attempts.
    async fn propose_eligible(&self, voter: NodeId) -> bool {
        self.propose_eligibility(WorkerEligibility {
            voter,
            eligible: true,
            applied_at_millis: crate::raft::current_millis(),
        })
        .await
    }

    /// Propose `eligible: false` for `voter`. Same return contract
    /// as [`Self::propose_eligible`].
    async fn propose_ineligible(&self, voter: NodeId) -> bool {
        self.propose_eligibility(WorkerEligibility {
            voter,
            eligible: false,
            applied_at_millis: crate::raft::current_millis(),
        })
        .await
    }

    /// `true` only when the apply landed (or was idempotent against
    /// state that already matched). `WorkerEligibilityRefused`
    /// (`VoterNotConfigured`) returns `false` so the next tick
    /// re-attempts once the refusal condition clears.
    async fn propose_eligibility(&self, args: WorkerEligibility) -> bool {
        let voter = args.voter;
        let eligible = args.eligible;
        let cmd = SmCommand::SetWorkerEligibility(args);
        match self.raft.client_write(cmd).await {
            Ok(resp) => match resp.data {
                SmResponse::WorkerEligibilitySet { changed: true, .. } => {
                    debug!(voter, eligible, "worker eligibility flipped");
                    true
                }
                SmResponse::WorkerEligibilitySet { changed: false, .. } => {
                    // Idempotent re-apply — state already matched.
                    // Common on monitor restart against a state
                    // machine the prior leader's monitor left
                    // populated.
                    true
                }
                SmResponse::WorkerEligibilityRefused { reason, .. } => {
                    warn!(voter, eligible, ?reason, "eligibility propose refused");
                    false
                }
                other => {
                    warn!(
                        voter,
                        eligible,
                        ?other,
                        "unexpected eligibility propose response"
                    );
                    false
                }
            },
            Err(err) => {
                warn!(voter, eligible, error = %err, "eligibility propose failed");
                false
            }
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
    max_healthy_lag: u64,
    now: Instant,
) {
    // Track the peer's furthest observed match position (monotonic).
    if log_advanced(&tracker.last_observed_log, &current_log) {
        tracker.last_observed_log = current_log;
    }

    if peer_is_keeping_up(leader_last_log, &tracker.last_observed_log, max_healthy_lag) {
        // Within the healthy lag window — clear any pending
        // unreachable timer. The first healthy sample after a
        // demotion opens the recovery window; later healthy samples
        // don't push its start forward, so the promote check
        // measures sustained health from a fixed point.
        tracker.unreachable_since = None;
        if tracker.last_proposed == Some(EligibilityProposal::Demote)
            && tracker.recovering_since.is_none()
        {
            tracker.recovering_since = Some(now);
        }
    } else {
        // Trailing the leader by more than `max_healthy_lag` — the
        // peer isn't keeping pace, even if its match index is still
        // inching forward. Start the unreachable timer on the first
        // lagging sample; subsequent samples don't reset it until the
        // peer catches back inside the window.
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
///
/// Returns `None` while the [`LivenessConfig::refusal_backoff`]
/// window after a refused propose is still in effect — without
/// this short-circuit, a persistent refusal (`VoterNotConfigured`)
/// would commit a raft log entry every sample tick until the
/// underlying condition cleared.
fn next_eligibility_proposal(
    tracker: &PeerTracker,
    now: Instant,
    config: &LivenessConfig,
) -> Option<EligibilityProposal> {
    if let Some(refused_at) = tracker.last_refused {
        if now.saturating_duration_since(refused_at) < config.refusal_backoff {
            return None;
        }
    }
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

/// True when the peer has matched at least one entry AND the
/// leader's log is past it. Returns `false` when `peer_log` is
/// `None` — a peer that has yet to match anything (bootstrap,
/// snapshot install) is in an indeterminate state distinct from
/// lag, and treating the absence of a baseline as lag would demote
/// a freshly-added voter before it had any chance to start
/// replicating.
/// True when the peer's match log is within `max_healthy_lag` entries
/// of the leader's last log — i.e. keeping pace. A peer that has yet
/// to match anything (`None`) counts as keeping up: it's in its
/// bootstrap / snapshot-install window, and treating an absent
/// baseline as lag would demote every freshly-added voter. A leader
/// with no log yet (`None`) trivially can't be ahead.
fn peer_is_keeping_up(
    leader_last_log: Option<u64>,
    peer_log: &Option<LogId<NodeId>>,
    max_healthy_lag: u64,
) -> bool {
    match (leader_last_log, peer_log) {
        (Some(leader_idx), Some(peer_log)) => {
            leader_idx.saturating_sub(peer_log.index) <= max_healthy_lag
        }
        _ => true,
    }
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
        // fast — the relevant invariants are the orderings between
        // these durations, not their absolute size.
        LivenessConfig {
            sample_interval: Duration::from_millis(1),
            unreachable_after: Duration::from_millis(100),
            live_after: Duration::from_millis(30),
            refusal_backoff: Duration::from_millis(200),
            // Small window so tests can express "keeping up" vs
            // "falling behind" with modest index gaps.
            max_healthy_lag: 10,
        }
    }

    /// Mirror of the `record_replication_progress` + `next_eligibility_proposal`
    /// pair the monitor runs each tick — plus the post-propose state
    /// updates the caller does at the dispatch site **when the apply
    /// landed**. Tests use this to advance the tracker one sample at
    /// a time under the normal "apply succeeded" path.
    fn tick(
        tracker: &mut PeerTracker,
        current_log: Option<LogId<NodeId>>,
        leader_last_log: Option<u64>,
        now: Instant,
        config: &LivenessConfig,
    ) -> Option<EligibilityProposal> {
        tick_with_outcome(tracker, current_log, leader_last_log, now, config, true)
    }

    /// Same shape as [`tick`] but lets the test simulate the apply
    /// being refused (e.g. `WorkerEligibilityRefused` /
    /// `client_write` error). When `apply_landed` is `false`, the
    /// tracker's `last_proposed` is **not** updated — so the next
    /// tick can re-attempt.
    fn tick_with_outcome(
        tracker: &mut PeerTracker,
        current_log: Option<LogId<NodeId>>,
        leader_last_log: Option<u64>,
        now: Instant,
        config: &LivenessConfig,
        apply_landed: bool,
    ) -> Option<EligibilityProposal> {
        record_replication_progress(
            tracker,
            current_log,
            leader_last_log,
            config.max_healthy_lag,
            now,
        );
        let proposal = next_eligibility_proposal(tracker, now, config);
        match (proposal, apply_landed) {
            (Some(EligibilityProposal::Promote), true) => {
                tracker.last_proposed = Some(EligibilityProposal::Promote);
                tracker.recovering_since = None;
                tracker.last_refused = None;
            }
            (Some(EligibilityProposal::Demote), true) => {
                tracker.last_proposed = Some(EligibilityProposal::Demote);
                tracker.last_refused = None;
            }
            (Some(_), false) => {
                tracker.last_refused = Some(now);
            }
            (None, _) => {}
        }
        proposal
    }

    #[test]
    fn first_sample_with_advancing_peer_proposes_nothing() {
        // Fresh tracker observes the peer advancing from None to
        // some log id. No prior proposal to override, no lag to
        // record — the monitor stays silent.
        let now = Instant::now();
        let mut tracker = PeerTracker::for_eligible_peer();
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
    fn peer_far_behind_leader_proposes_demote_after_unreachable_after() {
        // First sample: leader at 5, peer at 5, healthy.
        // Subsequent samples: leader runs to 50 (> max_healthy_lag
        // ahead), peer stuck at 5. After `unreachable_after`
        // elapses, demote.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::for_eligible_peer();
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(5), t0, &cfg);
        // Leader runs far ahead, peer doesn't follow.
        let t1 = t0 + cfg.sample_interval;
        let d1 = tick(&mut tracker, Some(log_id(1, 5)), Some(50), t1, &cfg);
        assert_eq!(d1, None, "unreachable_after hasn't elapsed yet");
        assert!(tracker.unreachable_since.is_some());
        // Past the threshold.
        let t2 = t1 + cfg.unreachable_after;
        let d2 = tick(&mut tracker, Some(log_id(1, 5)), Some(50), t2, &cfg);
        assert_eq!(
            d2,
            Some(EligibilityProposal::Demote),
            "monitor demotes after unreachable_after"
        );
    }

    #[test]
    fn peer_inching_forward_while_falling_behind_is_still_demoted() {
        // The bug this fixes: a peer whose match index advances every
        // sample — but by far less than the leader gains — was seen
        // as healthy because "it advanced", so its `unreachable_since`
        // reset every tick and it was never demoted while its
        // branches backed up. With lag measured as distance, a peer
        // that keeps falling further behind is demoted.
        let cfg = fast_config(); // max_healthy_lag = 10
        let t0 = Instant::now();
        let mut tracker = PeerTracker::for_eligible_peer();

        // Sample 0: caught up.
        let _ = tick(&mut tracker, Some(log_id(1, 100)), Some(100), t0, &cfg);
        assert!(tracker.unreachable_since.is_none());

        // Each sample the peer gains 1 entry; the leader gains 100,
        // so the gap grows without bound. Across these samples the
        // unreachable timer must stay set — the crux of the fix,
        // since the old "did it advance?" check reset it every tick.
        let mut peer_idx = 100u64;
        let mut leader_idx = 100u64;
        let mut t = t0;
        for _ in 0..3 {
            t += cfg.sample_interval;
            peer_idx += 1;
            leader_idx += 100;
            let d = tick(
                &mut tracker,
                Some(log_id(1, peer_idx)),
                Some(leader_idx),
                t,
                &cfg,
            );
            assert_eq!(d, None, "not demoted before unreachable_after elapses");
            assert!(
                tracker.unreachable_since.is_some(),
                "an inching-but-falling-behind peer must stay flagged"
            );
        }

        // Still inching, still falling behind, now past
        // `unreachable_after` — demote.
        let t_final = t + cfg.unreachable_after;
        let d = tick(
            &mut tracker,
            Some(log_id(1, peer_idx + 1)),
            Some(leader_idx + 100),
            t_final,
            &cfg,
        );
        assert_eq!(
            d,
            Some(EligibilityProposal::Demote),
            "a peer that advances but keeps falling behind must be demoted"
        );
    }

    #[test]
    fn demote_proposal_is_not_repeated_while_peer_stays_stuck() {
        // After a demotion, every subsequent stuck-with-leader-
        // ahead sample stays silent — the hysteresis check sees
        // `last_proposed == Some(Demote)` and bails.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::for_eligible_peer();
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
        let mut tracker = PeerTracker::for_ineligible_peer();
        tracker.last_observed_log = Some(log_id(1, 5));
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
        let mut tracker = PeerTracker::for_ineligible_peer();
        tracker.last_observed_log = Some(log_id(1, 5));
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
        let mut tracker = PeerTracker::for_eligible_peer();
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
    fn peer_catching_up_clears_an_in_progress_unreachable_window() {
        // Peer falls far behind, accumulates `unreachable_since`,
        // then catches back inside the healthy lag window before the
        // demotion threshold. The window resets — no demote.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::for_eligible_peer();
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(5), t0, &cfg);
        let t1 = t0 + cfg.sample_interval;
        // Leader jumps 45 ahead (> max_healthy_lag) — peer is behind.
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(50), t1, &cfg);
        assert!(tracker.unreachable_since.is_some());
        // Peer catches back to within the window before the threshold.
        let t2 = t1 + cfg.unreachable_after / 2;
        let d = tick(&mut tracker, Some(log_id(1, 45)), Some(50), t2, &cfg);
        assert_eq!(d, None, "back inside the lag window = no demote");
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
        let mut tracker = PeerTracker::for_eligible_peer();
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(5), t0, &cfg);
        let t1 = t0 + cfg.live_after * 10;
        let d = tick(&mut tracker, Some(log_id(1, 10)), Some(10), t1, &cfg);
        assert_eq!(d, None);
    }

    #[test]
    fn refused_demote_re_attempts_after_refusal_backoff() {
        // Stale-propose scenario: monitor decides to demote, the
        // state machine refuses (e.g. `VoterNotConfigured` because
        // the target voter was removed between the metrics snapshot
        // and the apply), or `client_write` fails at the transport
        // layer. The tracker's `last_proposed` must stay unset so
        // the next tick can re-propose once the refusal condition
        // clears — but only after `refusal_backoff` elapses, so a
        // persistent refusal doesn't commit a raft log entry every
        // sample interval.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::for_eligible_peer();
        // Build up to a Demote decision: healthy sample, then a
        // stuck sample, then enough time for `unreachable_after`.
        let _ = tick_with_outcome(&mut tracker, Some(log_id(1, 5)), Some(5), t0, &cfg, true);
        let t1 = t0 + cfg.sample_interval;
        let _ = tick_with_outcome(&mut tracker, Some(log_id(1, 5)), Some(50), t1, &cfg, true);
        let t2 = t1 + cfg.unreachable_after;
        let first = tick_with_outcome(&mut tracker, Some(log_id(1, 5)), Some(50), t2, &cfg, false);
        assert_eq!(
            first,
            Some(EligibilityProposal::Demote),
            "monitor wants to demote"
        );
        assert_eq!(
            tracker.last_proposed, None,
            "refused apply must not update last_proposed"
        );

        // Within the backoff window, the refusal must not re-fire
        // — otherwise the monitor would commit a raft log entry
        // every sample tick on a persistent refusal (e.g.
        // `VoterNotConfigured`).
        let t3 = t2 + cfg.sample_interval;
        assert!(t3 < t2 + cfg.refusal_backoff);
        let throttled = tick(&mut tracker, Some(log_id(1, 5)), Some(50), t3, &cfg);
        assert_eq!(
            throttled, None,
            "within refusal_backoff window, monitor must not re-attempt"
        );
        assert_eq!(
            tracker.last_proposed, None,
            "throttled tick still leaves last_proposed unset"
        );

        // After the backoff window elapses, the same condition
        // re-fires.
        let t4 = t2 + cfg.refusal_backoff;
        let second = tick(&mut tracker, Some(log_id(1, 5)), Some(20), t4, &cfg);
        assert_eq!(
            second,
            Some(EligibilityProposal::Demote),
            "after refusal_backoff elapses, monitor re-proposes Demote"
        );
        assert_eq!(
            tracker.last_proposed,
            Some(EligibilityProposal::Demote),
            "landed apply updates last_proposed"
        );
        assert!(
            tracker.last_refused.is_none(),
            "landed apply clears last_refused"
        );
    }

    #[test]
    fn refused_promote_re_attempts_after_refusal_backoff() {
        // Symmetric to the refused-demote case for the promote
        // path: monitor has the peer marked Demote, the peer
        // recovers, monitor wants to promote, the apply doesn't
        // land. The next tick within `refusal_backoff` must NOT
        // re-fire; once the backoff elapses the re-attempt does.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::for_ineligible_peer();
        tracker.last_observed_log = Some(log_id(1, 5));
        // First advance after demotion starts recovering_since.
        let t1 = t0 + cfg.sample_interval;
        let _ = tick_with_outcome(&mut tracker, Some(log_id(1, 6)), Some(10), t1, &cfg, true);
        // Past the live_after threshold — monitor wants Promote;
        // simulate the apply being refused.
        let t2 = t1 + cfg.live_after;
        let first = tick_with_outcome(&mut tracker, Some(log_id(1, 7)), Some(10), t2, &cfg, false);
        assert_eq!(first, Some(EligibilityProposal::Promote));
        assert_eq!(
            tracker.last_proposed,
            Some(EligibilityProposal::Demote),
            "refused promote leaves last_proposed at Demote"
        );
        assert_eq!(
            tracker.last_refused,
            Some(t2),
            "refused promote stamps last_refused"
        );

        // Within the backoff window: throttled. The peer keeps
        // advancing — otherwise `record_replication_progress`
        // would interpret the stall as fresh lag and clear the
        // recovery anchor.
        let t3 = t2 + cfg.sample_interval;
        let throttled = tick(&mut tracker, Some(log_id(1, 8)), Some(10), t3, &cfg);
        assert_eq!(
            throttled, None,
            "within refusal_backoff window, monitor must not re-attempt the Promote"
        );

        // After the backoff window: re-fires. Peer's log advances
        // again so it continues to count as recovering.
        let t4 = t2 + cfg.refusal_backoff;
        let second = tick(&mut tracker, Some(log_id(1, 9)), Some(10), t4, &cfg);
        assert_eq!(
            second,
            Some(EligibilityProposal::Promote),
            "after refusal_backoff elapses, monitor re-proposes Promote"
        );
    }

    #[test]
    fn fresh_tracker_for_replicated_ineligible_peer_promotes_on_recovery() {
        // Leader transition: the prior leader's monitor demoted the
        // peer (replicated `worker_eligible_voters` no longer
        // contains it). The new leader's monitor builds a fresh
        // tracker on first observation. Without the seed from
        // replicated state, `last_proposed` would be `None` and
        // the Promote arm (which fires only against a
        // `Some(Demote)` hysteresis) would never propose — the
        // demotion would stick across every leader transition even
        // after the peer recovers.
        let cfg = fast_config();
        let t0 = Instant::now();
        // The replicated state shows this peer as ineligible.
        let mut tracker = PeerTracker::for_ineligible_peer();
        assert_eq!(
            tracker.last_proposed,
            Some(EligibilityProposal::Demote),
            "tracker for an ineligible peer seeds as if this monitor demoted it"
        );
        // First sample on the new leader: peer advances. Starts
        // recovering_since.
        let t1 = t0 + cfg.sample_interval;
        let d1 = tick(&mut tracker, Some(log_id(1, 6)), Some(10), t1, &cfg);
        assert_eq!(
            d1, None,
            "recovery anchor just started; live_after not elapsed"
        );
        assert_eq!(tracker.recovering_since, Some(t1));
        // Past live_after.
        let t2 = t1 + cfg.live_after;
        let d2 = tick(&mut tracker, Some(log_id(1, 7)), Some(10), t2, &cfg);
        assert_eq!(
            d2,
            Some(EligibilityProposal::Promote),
            "monitor promotes the peer the prior leader demoted, restoring \
             eligibility once it demonstrates recovery"
        );
    }

    #[test]
    fn freshly_added_voter_bootstraps_without_premature_demotion() {
        // A voter just added via membership change has `peer_log:
        // None` until the first successful append-entries — for a
        // sizable state machine this means the entire snapshot
        // install window, well past `unreachable_after`. The monitor
        // must not start the unreachable timer during this window;
        // once the peer matches its first entry, the standard
        // observed-lag detection takes over.
        let cfg = fast_config();
        let t0 = Instant::now();
        let mut tracker = PeerTracker::for_eligible_peer();
        // Bootstrap window: leader's log grows far past anything
        // the peer has matched (peer_log stays None). Far longer
        // than `unreachable_after` elapses — nothing fires.
        let t1 = t0 + cfg.unreachable_after * 10;
        let d1 = tick(&mut tracker, None, Some(20), t1, &cfg);
        assert_eq!(d1, None, "never-matched peer is not lag-classified");
        assert!(
            tracker.unreachable_since.is_none(),
            "no lag timer for a peer that has yet to match anything"
        );
        // Peer matches its first entry — `last_observed_log` is
        // now populated and the bootstrap window ends.
        let t2 = t1 + cfg.sample_interval;
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(20), t2, &cfg);
        assert_eq!(tracker.last_observed_log, Some(log_id(1, 5)));
        // Leader keeps advancing; peer stuck at 5. Standard lag
        // detection now applies from here.
        let t3 = t2 + cfg.sample_interval;
        let _ = tick(&mut tracker, Some(log_id(1, 5)), Some(30), t3, &cfg);
        assert!(
            tracker.unreachable_since.is_some(),
            "lag timer starts once the peer has matched a baseline"
        );
        let t4 = t3 + cfg.unreachable_after;
        let d4 = tick(&mut tracker, Some(log_id(1, 5)), Some(30), t4, &cfg);
        assert_eq!(
            d4,
            Some(EligibilityProposal::Demote),
            "post-bootstrap lag detection still fires"
        );
    }
}
