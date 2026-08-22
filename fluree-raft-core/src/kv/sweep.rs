//! The leader-only driver that keeps expired entries from accumulating.
//!
//! [`KvCommand::Evict`](super::KvCommand::Evict) is deliberately
//! bounded: it examines at most `limit` records so one sweep cannot
//! stall every apply behind it. That bound makes a *driver* necessary —
//! something has to notice `more_expired` and come back — and the retry
//! protocol has two details that are easy to get wrong in a way that
//! only shows up as a backlog that never drains:
//!
//! 1. **Re-propose immediately, with the *same* cutoff.** Waiting a
//!    full interval means a fragment expiring faster than one batch per
//!    interval grows without bound. Re-reading the clock for each round
//!    is worse: the cutoff advances under a steadily-expiring workload,
//!    so the sweep chases its own tail and never reports drained.
//! 2. **Do not propose when nothing has expired.** An idle group whose
//!    ticker still writes to the log forces snapshots and log purges
//!    forever, on every node, for no work.
//!
//! Both live here rather than in each consumer, because flow, resolve,
//! and the nameservice would otherwise each reimplement them.
//!
//! Available with `kv` + `raft`.

use super::KvResponse;
use crate::runtime::run_periodic;
use async_trait::async_trait;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Default gap between sweeps. Eviction is a reclamation task, not a
/// correctness one — an expired entry is already logically absent — so
/// this is deliberately unhurried.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Default bound on immediate re-proposals within one tick. Caps the
/// leader time one backlog can take before other proposers get a turn;
/// whatever is left waits for the next tick.
pub const DEFAULT_MAX_ROUNDS: u32 = 8;

/// Floor on the sweep interval.
///
/// A zero interval is a caller mistake with no useful reading: it turns
/// the ticker into a spin loop, and — because the pre-check is a local
/// read — into a continuous stream of proposals the moment anything
/// expires.
pub const MIN_SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// How an application reaches one fragment: a local read to decide
/// whether a sweep is worth proposing, and a way to put an `Evict`
/// through its own command type.
///
/// One implementation per fragment. A group with several fragments runs
/// several sweeps, which is also what keeps a churny tenant from
/// starving another's.
#[async_trait]
pub trait SweepTarget: Send + Sync + 'static {
    /// Why a proposal could not be made — most often "not the leader
    /// any more". Reported and treated as end-of-tick, never retried
    /// in a tight loop.
    type Error: std::fmt::Display + Send;

    /// Whether this fragment holds anything expired at `cutoff_ms`.
    ///
    /// Answered from local state. It may lag — this node is a replica —
    /// which costs at most one skipped tick and never correctness.
    async fn has_expired(&self, cutoff_ms: u64) -> bool;

    /// Propose one `Evict` and return what the state machine answered.
    async fn propose_evict(&self, cutoff_ms: u64, limit: u32) -> Result<KvResponse, Self::Error>;
}

/// Tuning for one fragment's sweep.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SweepConfig {
    pub interval: Duration,
    /// Records examined per proposal. Capped by the fragment's
    /// [`KvPolicy`](super::KvPolicy) and again by
    /// [`HARD_MAX_EVICT_LIMIT`](super::HARD_MAX_EVICT_LIMIT) at apply
    /// time, so an over-large value here degrades to the maximum rather
    /// than being honored.
    pub batch: u32,
    pub max_rounds: u32,
}

impl Default for SweepConfig {
    /// Hand-written: a derived `Default` would give a zero interval —
    /// a proposal storm — and a zero round budget, which sweeps
    /// nothing.
    fn default() -> Self {
        Self {
            interval: DEFAULT_SWEEP_INTERVAL,
            batch: super::DEFAULT_EVICT_LIMIT,
            max_rounds: DEFAULT_MAX_ROUNDS,
        }
    }
}

impl SweepConfig {
    /// The config actually used, with degenerate values raised to a
    /// usable floor and the substitution logged.
    ///
    /// Clamped rather than rejected, on the same reasoning as
    /// [`KvPolicy::evict_batch`](super::KvPolicy::evict_batch): these
    /// are throughput knobs with no observable semantics, and failing a
    /// leader task at spawn time — the only place a rejection could
    /// land — would disable eviction entirely on the node best placed
    /// to do it. A TTL is the opposite case, and is rejected.
    pub fn sanitized(&self) -> Self {
        let interval = self.interval.max(MIN_SWEEP_INTERVAL);
        let max_rounds = self.max_rounds.max(1);
        if interval != self.interval || max_rounds != self.max_rounds {
            tracing::warn!(
                requested_interval_ms = self.interval.as_millis() as u64,
                requested_max_rounds = self.max_rounds,
                interval_ms = interval.as_millis() as u64,
                max_rounds,
                "kv sweep: unusable configuration raised to the minimum"
            );
        }
        Self {
            interval,
            batch: self.batch,
            max_rounds,
        }
    }
}

/// What one tick did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Proposals made. Zero means nothing had expired.
    pub rounds: u32,
    /// Records reclaimed across those proposals.
    pub removed: u64,
    /// Whether the fragment was left with nothing expired at this
    /// cutoff. False means the round budget ran out, the tick was
    /// cancelled, or a proposal failed — all of which leave work for
    /// the next tick.
    pub drained: bool,
}

/// Drain everything expired at `cutoff_ms`, up to the round budget.
///
/// Separate from [`run_sweep`] so the retry protocol can be tested
/// without a ticker or a clock.
pub async fn sweep_once<T: SweepTarget>(
    target: &T,
    config: &SweepConfig,
    cutoff_ms: u64,
    cancel: &CancellationToken,
) -> SweepOutcome {
    let config = &config.sanitized();
    let mut outcome = SweepOutcome::default();
    if !target.has_expired(cutoff_ms).await {
        // Nothing to reclaim: say drained and, crucially, write nothing
        // to the log.
        outcome.drained = true;
        return outcome;
    }

    while outcome.rounds < config.max_rounds {
        if cancel.is_cancelled() {
            return outcome;
        }
        // The same cutoff every round. A fresh clock read here would
        // let a steadily-expiring fragment outrun the sweep.
        match target.propose_evict(cutoff_ms, config.batch).await {
            Ok(KvResponse::Evicted {
                removed,
                more_expired,
            }) => {
                outcome.rounds += 1;
                outcome.removed += u64::from(removed);
                if !more_expired {
                    outcome.drained = true;
                    return outcome;
                }
            }
            Ok(other) => {
                // Only `Evict` is proposed here, and only `Evicted`
                // answers it — so this is a wiring bug in the
                // application's command routing, not a runtime
                // condition.
                tracing::error!(
                    response = ?other,
                    "kv sweep: Evict answered with an unexpected response; check command routing"
                );
                return outcome;
            }
            Err(error) => {
                // Most often a lost leadership. The watcher will stop
                // this task shortly; until then, do not spin.
                tracing::debug!(%error, "kv sweep: evict proposal failed");
                return outcome;
            }
        }
    }
    outcome
}

/// Run [`sweep_once`] every `config.interval` until cancelled.
///
/// Spawn this from
/// [`spawn_leader_watcher`](crate::runtime::spawn_leader_watcher)'s
/// task factory: eviction is a write, so only the leader should be
/// proposing it, and the watcher already handles the flap and shutdown
/// semantics.
///
/// `clock` supplies `now_ms` at the start of each tick — the proposer's
/// clock, the same one a `Put` stamps with. A parameter rather than a
/// `SystemTime` read so the protocol is testable at chosen instants.
pub async fn run_sweep<T, C>(target: T, config: SweepConfig, clock: C, cancel: CancellationToken)
where
    T: SweepTarget,
    C: Fn() -> u64 + Send + Sync,
{
    let config = config.sanitized();
    let (target, config, clock) = (&target, &config, &clock);
    let inner = cancel.clone();
    let inner = &inner;
    run_periodic(config.interval, cancel.clone(), move || async move {
        let outcome = sweep_once(target, config, clock(), inner).await;
        if outcome.removed > 0 {
            tracing::debug!(
                rounds = outcome.rounds,
                removed = outcome.removed,
                drained = outcome.drained,
                "kv sweep"
            );
        }
    })
    .await;
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::kv::{apply, Expect, KvCommand, KvFragment, KvPolicy};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    const HOUR: u64 = 60 * 60 * 1000;

    /// A real fragment behind the trait, so the responses the driver
    /// reacts to are the ones `kv::apply` actually produces.
    pub(super) struct Fragment {
        state: Mutex<KvFragment>,
        policy: KvPolicy,
        /// `(cutoff_ms, limit)` for every proposal attempted.
        proposals: Mutex<Vec<(u64, u32)>>,
        index: AtomicU64,
        /// Start failing proposals once this many have been made.
        fail_after: Option<usize>,
        /// Cancel this once `cancel_after` proposals have been made.
        cancel_after: Option<(usize, CancellationToken)>,
    }

    impl Fragment {
        pub(super) fn with(expired: usize, live: usize) -> Self {
            let policy = KvPolicy::default();
            let mut state = KvFragment::new();
            for i in 0..expired {
                apply(
                    &mut state,
                    &KvCommand::Put {
                        key: format!("gone{i:03}"),
                        value: b"v".to_vec(),
                        expect: Expect::Absent,
                        ttl_ms: Some(HOUR),
                        now_ms: 0,
                    },
                    &policy,
                    i as u64 + 1,
                );
            }
            for i in 0..live {
                apply(
                    &mut state,
                    &KvCommand::Put {
                        key: format!("live{i:03}"),
                        value: b"v".to_vec(),
                        expect: Expect::Absent,
                        ttl_ms: Some(10 * HOUR),
                        now_ms: 0,
                    },
                    &policy,
                    1_000 + i as u64,
                );
            }
            Self {
                state: Mutex::new(state),
                policy,
                proposals: Mutex::new(Vec::new()),
                index: AtomicU64::new(10_000),
                fail_after: None,
                cancel_after: None,
            }
        }

        fn cutoffs(&self) -> Vec<u64> {
            self.proposals.lock().unwrap().iter().map(|p| p.0).collect()
        }

        fn attempts(&self) -> usize {
            self.proposals.lock().unwrap().len()
        }

        fn physical_len(&self) -> usize {
            self.state.lock().unwrap().physical_len()
        }
    }

    #[async_trait]
    impl SweepTarget for Fragment {
        type Error = &'static str;

        async fn has_expired(&self, cutoff_ms: u64) -> bool {
            self.state.lock().unwrap().has_expired_at(cutoff_ms)
        }

        async fn propose_evict(
            &self,
            cutoff_ms: u64,
            limit: u32,
        ) -> Result<KvResponse, Self::Error> {
            let attempt = {
                let mut proposals = self.proposals.lock().unwrap();
                proposals.push((cutoff_ms, limit));
                proposals.len()
            };
            if let Some((after, token)) = &self.cancel_after {
                if attempt >= *after {
                    token.cancel();
                }
            }
            if self.fail_after.is_some_and(|after| attempt > after) {
                return Err("not the leader");
            }
            let index = self.index.fetch_add(1, Ordering::Relaxed);
            let mut state = self.state.lock().unwrap();
            Ok(apply(
                &mut state,
                &KvCommand::Evict { cutoff_ms, limit },
                &self.policy,
                index,
            ))
        }
    }

    fn config(batch: u32, max_rounds: u32) -> SweepConfig {
        SweepConfig {
            interval: Duration::from_millis(5),
            batch,
            max_rounds,
        }
    }

    /// An idle group whose ticker still proposes grows its log forever,
    /// on every node, for no work — which then forces snapshots and log
    /// purges. The local pre-check is what prevents that.
    #[tokio::test]
    async fn nothing_expired_writes_nothing_to_the_log() {
        let target = Fragment::with(0, 5);
        let outcome =
            sweep_once(&target, &config(16, 8), HOUR - 1, &CancellationToken::new()).await;

        assert_eq!(
            outcome,
            SweepOutcome {
                rounds: 0,
                removed: 0,
                drained: true,
            },
        );
        assert_eq!(target.attempts(), 0, "an idle sweep must not propose");
    }

    /// The retry protocol: keep going immediately while `more_expired`
    /// is set, rather than waiting out the interval — otherwise a
    /// fragment expiring faster than one batch per interval never
    /// drains.
    #[tokio::test]
    async fn a_backlog_drains_within_one_tick() {
        let target = Fragment::with(10, 2);
        let outcome = sweep_once(&target, &config(4, 8), HOUR, &CancellationToken::new()).await;

        assert_eq!(
            outcome,
            SweepOutcome {
                rounds: 3,
                removed: 10,
                drained: true,
            },
        );
        assert_eq!(target.physical_len(), 2, "live entries must survive");
    }

    /// Every round of one tick uses the cutoff the tick started with.
    /// Re-reading the clock per round advances the cutoff under a
    /// steadily-expiring workload, so the sweep chases its own tail and
    /// never reports drained.
    #[tokio::test]
    async fn every_round_of_a_tick_uses_the_same_cutoff() {
        let target = Fragment::with(10, 0);
        sweep_once(&target, &config(3, 8), HOUR, &CancellationToken::new()).await;

        let cutoffs = target.cutoffs();
        assert!(cutoffs.len() > 1, "the batch must force several rounds");
        assert!(
            cutoffs.iter().all(|&c| c == HOUR),
            "a tick must not advance its cutoff mid-drain: {cutoffs:?}",
        );
    }

    /// One backlog must not monopolize the leader. Whatever is left
    /// waits for the next tick, which is what `drained: false` says.
    #[tokio::test]
    async fn the_round_budget_bounds_one_tick() {
        let target = Fragment::with(100, 0);
        let outcome = sweep_once(&target, &config(4, 3), HOUR, &CancellationToken::new()).await;

        assert_eq!(
            outcome,
            SweepOutcome {
                rounds: 3,
                removed: 12,
                drained: false,
            },
        );
        assert_eq!(target.attempts(), 3);
        assert_eq!(target.physical_len(), 88);
    }

    /// Losing leadership mid-drain must stop the tick rather than let
    /// a deposed leader keep proposing until its round budget runs out.
    #[tokio::test]
    async fn cancellation_stops_the_drain_between_rounds() {
        let cancel = CancellationToken::new();
        let mut target = Fragment::with(100, 0);
        target.cancel_after = Some((2, cancel.clone()));

        let outcome = sweep_once(&target, &config(4, 8), HOUR, &cancel).await;

        assert_eq!(outcome.rounds, 2, "the round in flight completes");
        assert!(!outcome.drained);
        assert_eq!(
            target.attempts(),
            2,
            "no further proposal may be attempted after cancellation",
        );
    }

    /// A failed proposal — almost always a lost leadership — ends the
    /// tick. Retrying in a tight loop would hammer a node that has
    /// already been told it is not the leader.
    #[tokio::test]
    async fn a_failed_proposal_ends_the_tick_without_spinning() {
        let mut target = Fragment::with(100, 0);
        target.fail_after = Some(1);

        let outcome = sweep_once(&target, &config(4, 8), HOUR, &CancellationToken::new()).await;

        assert_eq!(outcome.rounds, 1);
        assert!(!outcome.drained);
        assert_eq!(
            target.attempts(),
            2,
            "exactly one failure, then stop — not a retry loop",
        );
    }

    /// The ticker wrapper: it reaches the target on its own, and stops
    /// when the token is cancelled rather than at the end of a sleep.
    #[tokio::test(start_paused = true)]
    async fn run_sweep_ticks_and_then_stops() {
        let target = Fragment::with(6, 1);
        let cancel = CancellationToken::new();
        let sweep = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                run_sweep(target, config(2, 8), || HOUR, cancel).await;
            })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), sweep)
            .await
            .expect("run_sweep must stop promptly on cancellation")
            .expect("sweep task must not panic");
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    /// A zero interval turns the ticker into a spin loop and, once
    /// anything expires, into a continuous stream of proposals. A zero
    /// round budget silently sweeps nothing at all — the worse of the
    /// two, because it looks like it is working.
    #[test]
    fn degenerate_configurations_are_raised_to_a_usable_floor() {
        let sane = SweepConfig {
            interval: Duration::ZERO,
            batch: 8,
            max_rounds: 0,
        }
        .sanitized();
        assert_eq!(sane.interval, MIN_SWEEP_INTERVAL);
        assert_eq!(sane.max_rounds, 1);
        assert_eq!(sane.batch, 8, "the batch has its own clamp at apply time");

        let untouched = SweepConfig::default();
        assert_eq!(untouched.sanitized(), untouched);
    }

    /// `sweep_once` sanitizes too, not just the ticker — the round
    /// budget is enforced there, so a zero budget passed directly must
    /// still make progress rather than return an empty success.
    #[tokio::test]
    async fn a_zero_round_budget_still_makes_progress() {
        let target = Fragment::with(3, 0);
        let outcome = sweep_once(
            &target,
            &SweepConfig {
                interval: Duration::from_millis(5),
                batch: 8,
                max_rounds: 0,
            },
            HOUR,
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(outcome.rounds, 1);
        assert_eq!(outcome.removed, 3);
        assert!(outcome.drained);
    }

    use super::tests::Fragment;
    const HOUR: u64 = 60 * 60 * 1000;
}
