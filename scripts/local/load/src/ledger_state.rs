//! Shared map of ledgers known to exist.
//!
//! `CreateLedger` ops add to this map after their request lands;
//! `Transact` and `Query` ops pick from it when generating a request.
//! Cheap to clone (an `Arc` over a `RwLock`), so every worker task
//! holds its own handle.

use std::sync::{Arc, RwLock};

/// Set of ledgers (by canonical `name:branch` form, or just `name`)
/// the harness has successfully created during this run.
///
/// Wrapped in `Arc<RwLock<...>>` so worker tasks can register newly
/// created ledgers and read for target selection concurrently. The
/// expected access pattern is many reads (one per transact/query
/// dispatch) and few writes (one per landed `CreateLedger`), so the
/// `RwLock` win over `Mutex` is real even at low contention.
#[derive(Clone, Default)]
pub struct LedgerState {
    inner: Arc<RwLock<Vec<String>>>,
}

impl LedgerState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a ledger. Idempotent — re-registering the same name
    /// is a no-op (preserves uniqueness without paying for a `HashSet`
    /// at our scale; the vec is small and reads dominate).
    pub fn register(&self, ledger: &str) {
        let mut guard = self.inner.write().expect("ledger_state lock poisoned");
        if !guard.iter().any(|l| l == ledger) {
            guard.push(ledger.to_string());
        }
    }

    /// Number of registered ledgers. Used by the workload composer
    /// to decide whether a `Transact` op has anything to target.
    pub fn len(&self) -> usize {
        self.inner.read().expect("ledger_state lock poisoned").len()
    }

    /// Pick one ledger by index modulo the current size. Returns
    /// `None` if no ledgers exist yet.
    ///
    /// The index is supplied by the caller so the workload composer
    /// can drive deterministic round-robin, modulo-based pseudo-random,
    /// or whatever distribution it wants without this module taking
    /// an opinion.
    pub fn pick(&self, index: usize) -> Option<String> {
        let guard = self.inner.read().expect("ledger_state lock poisoned");
        if guard.is_empty() {
            return None;
        }
        Some(guard[index % guard.len()].clone())
    }

    /// Snapshot of every registered ledger. Used by the reporter for
    /// per-ledger stats summaries.
    pub fn snapshot(&self) -> Vec<String> {
        self.inner
            .read()
            .expect("ledger_state lock poisoned")
            .clone()
    }
}
