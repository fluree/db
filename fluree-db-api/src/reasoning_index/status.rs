//! Local maintenance diagnostics and bounded retries. These are not part of
//! the authoritative registration document: a restart retries the work.

use fluree_db_core::clock::Instant;
use std::collections::BTreeMap;
use std::time::Duration;

use fluree_db_core::reasoning_index::ReasoningIndexId;
use fluree_db_core::ContentId;

use super::ReasoningIndexState;
use crate::Fluree;

/// The last failed refresh in this process. Explicit refresh bypasses backoff.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningIndexFailure {
    pub reason: String,
    pub attempts: u32,
    pub failed_at_ms: i64,
    pub retry_at_ms: i64,
}

pub(super) struct Failure {
    status: ReasoningIndexFailure,
    retry_at: Instant,
}

fn retry_delay(attempts: u32) -> Duration {
    Duration::from_secs(
        5u64.saturating_mul(1u64 << attempts.saturating_sub(1).min(6))
            .min(300),
    )
}

impl Fluree {
    /// Last refresh failures observed by this instance, keyed by index id.
    /// Successful refreshes clear them; they do not survive a process restart.
    pub fn reasoning_index_failures(
        &self,
        ledger_id: &str,
    ) -> BTreeMap<ReasoningIndexId, ReasoningIndexFailure> {
        let id = fluree_db_core::LedgerId::parse(ledger_id)
            .map(|id| id.to_string())
            .unwrap_or_else(|_| ledger_id.to_string());
        self.reasoning_index_state
            .failures
            .lock()
            .iter()
            .filter(|((ledger, _), _)| ledger == &id)
            .map(|((_, index), failure)| (index.clone(), failure.status.clone()))
            .collect()
    }

    /// Bound the number of loaded reasoning generations kept by this instance.
    pub fn set_reasoning_index_cache_capacity(&self, capacity: std::num::NonZeroUsize) {
        self.reasoning_index_state.loaded.lock().0.resize(capacity);
    }
}

impl ReasoningIndexState {
    pub(super) fn retry_after(&self, ledger: &str, index: &ReasoningIndexId) -> Option<i64> {
        self.failures
            .lock()
            .get(&(ledger.to_string(), index.clone()))
            .filter(|failure| Instant::now() < failure.retry_at)
            .map(|failure| failure.status.retry_at_ms)
    }

    pub(super) fn record_failure(&self, ledger: &str, index: &ReasoningIndexId, reason: String) {
        let mut failures = self.failures.lock();
        let key = (ledger.to_string(), index.clone());
        let attempts = failures
            .get(&key)
            .map_or(1, |f| f.status.attempts.saturating_add(1));
        let delay = retry_delay(attempts);
        let now = fluree_db_core::clock::SystemTime::now()
            .duration_since(fluree_db_core::clock::SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        failures.insert(
            key,
            Failure {
                status: ReasoningIndexFailure {
                    reason,
                    attempts,
                    failed_at_ms: now,
                    retry_at_ms: now.saturating_add(delay.as_millis() as i64),
                },
                retry_at: Instant::now() + delay,
            },
        );
    }

    pub(super) fn clear_failure(&self, ledger: &str, index: &ReasoningIndexId) {
        self.failures
            .lock()
            .remove(&(ledger.to_string(), index.clone()));
    }

    pub(super) fn needs_repair(
        &self,
        ledger: &str,
        index: &ReasoningIndexId,
        manifest: &ContentId,
    ) -> bool {
        self.repairs
            .lock()
            .get(&(ledger.to_string(), index.clone()))
            == Some(manifest)
    }

    pub(super) fn request_repair(
        &self,
        ledger: &str,
        index: &ReasoningIndexId,
        manifest: &ContentId,
        reason: String,
    ) {
        self.loaded
            .lock()
            .0
            .pop(&(ledger.to_string(), index.clone(), manifest.clone()));
        let mut repairs = self.repairs.lock();
        let key = (ledger.to_string(), index.clone());
        if repairs.get(&key) != Some(manifest) {
            repairs.insert(key, manifest.clone());
            drop(repairs);
            self.record_failure(ledger, index, reason);
            self.note_registered(ledger);
        }
    }

    pub(super) fn warn_serving(&self, ledger: &str, error: &impl std::fmt::Display) {
        let mut warnings = self.serve_warnings.lock();
        let now = Instant::now();
        if warnings
            .get(ledger)
            .is_none_or(|last| now.duration_since(*last) >= Duration::from_secs(60))
        {
            tracing::warn!(ledger, error = %error, "reasoning index unreadable");
            warnings.insert(ledger.to_string(), now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_refreshes_back_off_but_always_retry() {
        assert_eq!(retry_delay(1), Duration::from_secs(5));
        assert_eq!(retry_delay(2), Duration::from_secs(10));
        assert_eq!(retry_delay(u32::MAX), Duration::from_secs(300));
        let state = ReasoningIndexState::default();
        let index = ReasoningIndexId::parse("01JC0000000000000000000000").unwrap();
        state.record_failure("ledger:main", &index, "capped".into());
        assert!(state.retry_after("ledger:main", &index).is_some());
        state.clear_failure("ledger:main", &index);
        assert!(state.retry_after("ledger:main", &index).is_none());
    }
}
