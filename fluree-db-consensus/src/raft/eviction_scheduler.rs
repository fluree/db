//! Leader-only periodic [`Command::EvictIdempotency`] proposer.
//!
//! The state machine carries an idempotency cache that grows on every
//! queue-mediated submission and only shrinks via
//! [`Command::EvictIdempotency`]. This scheduler runs on the current
//! leader (spawned by the same leader watcher as the
//! [`CommitWorker`](super::commit_worker::CommitWorker)), sleeps
//! `eviction_interval`, and proposes the eviction command. The actual
//! CAS releases happen on every node: the state-machine adapter
//! drains `Response::EvictionApplied.released_envelopes` into a
//! per-node release task so followers free their local blobs too.
//!
//! Per-process scope: a leader transition strands the loop on the
//! former leader (the leader watcher aborts the task). The new
//! leader spawns its own scheduler on transition. No state survives
//! across the gap — that's fine because the state machine's
//! idempotency map is the source of truth and the next tick on the
//! new leader will pick up wherever the old one left off.

use crate::raft::state_machine::{Command as SmCommand, Response as SmResponse};
use crate::raft::TypeConfig;
use openraft::Raft;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::warn;

/// Default TTL for idempotency cache entries before eviction. Matches
/// the in-process [`CachingCommitter::DEFAULT_IDEMPOTENCY_TTL`] so
/// client-facing retry semantics don't change between local and Raft
/// deployments.
pub const DEFAULT_IDEMPOTENCY_TTL: Duration = Duration::from_secs(60 * 60);

/// TTL applied to `recently_cleared` admin-clear markers. Bounded
/// well above any realistic worker propose window — the only
/// in-flight workers a marker informs are those that already had a
/// stage in flight when the admin clear landed; they either land
/// within seconds or strand at a leader transition.
pub const DEFAULT_MARKER_TTL: Duration = Duration::from_secs(5 * 60);

/// Default interval between [`Command::EvictIdempotency`] proposals.
pub const DEFAULT_EVICTION_INTERVAL: Duration = Duration::from_secs(60);

/// Periodic eviction proposer. Cloning is cheap (`Arc` clones); a
/// single instance is driven by the leader watcher.
#[derive(Clone)]
pub struct EvictionScheduler {
    raft: Arc<Raft<TypeConfig>>,
    interval: Duration,
    ttl: Duration,
    marker_ttl: Duration,
}

impl EvictionScheduler {
    pub fn new(raft: Arc<Raft<TypeConfig>>) -> Self {
        Self {
            raft,
            interval: DEFAULT_EVICTION_INTERVAL,
            ttl: DEFAULT_IDEMPOTENCY_TTL,
            marker_ttl: DEFAULT_MARKER_TTL,
        }
    }

    /// Override the interval between eviction proposals (default 60s).
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Override the TTL applied to idempotency cache entries (default
    /// 1h). Entries with `recorded_at_millis < now - ttl` get
    /// evicted.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Override the TTL applied to `recently_cleared` markers
    /// (default 5 minutes). See [`DEFAULT_MARKER_TTL`].
    pub fn with_marker_ttl(mut self, ttl: Duration) -> Self {
        self.marker_ttl = ttl;
        self
    }

    /// Drive periodic eviction until the leader watcher aborts the
    /// task. Each tick: sleep for the interval, compute the cutoff,
    /// propose `EvictIdempotency`. The actual CAS release fan-out
    /// happens inside the state-machine adapter on every node.
    pub async fn run(self) {
        loop {
            tokio::time::sleep(self.interval).await;
            self.tick().await;
        }
    }

    async fn tick(&self) {
        let Some(idempotency_cutoff) = cutoff_millis(self.ttl) else {
            return;
        };
        // Markers have a shorter TTL than idempotency entries, so we
        // compute them independently. The state machine evicts each
        // map against its matching cutoff in a single apply.
        let marker_cutoff = cutoff_millis(self.marker_ttl).unwrap_or(0);
        let cmd = SmCommand::EvictIdempotency {
            cutoff_millis: idempotency_cutoff,
            marker_cutoff_millis: marker_cutoff,
        };
        let resp = match self.raft.client_write(cmd).await {
            Ok(resp) => resp,
            Err(err) => {
                warn!(error = %err, "EvictIdempotency propose failed");
                return;
            }
        };
        if !matches!(resp.data, SmResponse::EvictionApplied { .. }) {
            warn!(
                response = ?resp.data,
                "unexpected response from EvictIdempotency apply"
            );
        }
    }
}

/// `now - ttl` as millis-since-epoch, or `None` if the system clock
/// is behind the TTL (in which case there's nothing old enough to
/// evict). Pulled out as a free function so it can be unit-tested
/// without an Arc<Raft>.
fn cutoff_millis(ttl: Duration) -> Option<u64> {
    let now_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;
    let ttl_millis = ttl.as_millis() as u64;
    now_millis.checked_sub(ttl_millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cutoff_is_now_minus_ttl_and_shrinks_with_larger_ttl() {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let one_sec = cutoff_millis(Duration::from_secs(1))
            .expect("system clock is past 1s of epoch");
        let one_hour = cutoff_millis(Duration::from_secs(60 * 60))
            .expect("system clock is past 1h of epoch");

        assert!(one_sec <= now);
        assert!(one_hour <= now);
        assert!(one_hour < one_sec, "longer TTL → earlier cutoff");
    }

    #[test]
    fn cutoff_is_none_when_ttl_exceeds_unix_time() {
        // Pathological case: TTL of one century would put the cutoff
        // before the epoch. Confirms the saturating-arithmetic guard
        // returns None rather than wrapping.
        let huge = Duration::from_secs(60 * 60 * 24 * 365 * 100);
        assert!(cutoff_millis(huge).is_none());
    }
}
