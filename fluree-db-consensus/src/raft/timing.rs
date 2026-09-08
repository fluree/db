//! Opt-in diagnostic durations. Nested/overlapping phases must not be summed.
use std::time::Instant;
pub(super) fn start() -> Option<Instant> {
    tracing::enabled!(target: "fluree_raft_timing", tracing::Level::DEBUG).then(Instant::now)
}
pub(super) fn record(
    start: Option<Instant>,
    phase: &str,
    ledger: &str,
    identity: &str,
    bytes: usize,
    ok: bool,
) {
    if let Some(start) = start {
        tracing::debug!(target: "fluree_raft_timing", phase, ledger, identity, bytes, ok,
            elapsed_us = start.elapsed().as_micros() as u64, "raft_phase");
    }
}
