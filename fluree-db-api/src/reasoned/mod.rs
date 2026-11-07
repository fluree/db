//! Reasoned heads: persisted, background-maintained results of registered
//! reasoning programs.
//!
//! A head is a complete merged index of a branch — every graph's asserted
//! facts plus the program's derived ones — pinned to a basis commit and
//! stored under `{branch}/@reasoned/{head_id}/`. A reasoning query whose
//! settled program matches a head published at exactly the query's commit is
//! answered by an ordinary query against the head, with reasoning off;
//! anything else runs reasoning at query time, as before.
//!
//! - [`build`]: registration, and building and publishing generations.
//! - [`serve`]: matching a query to a servable generation.

mod build;
pub(crate) mod derived;
pub(crate) mod program;
pub(crate) mod serve;
mod worker;

pub use build::{RefreshOutcome, MAX_ACTIVE_HEADS};
pub use worker::{ReasonedHeadWorker, DEFAULT_CATCHUP_INTERVAL};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluree_db_core::reasoned_head::{HeadId, ReasonedManifest};
use fluree_db_core::ContentId;
use parking_lot::Mutex;

use crate::LedgerState;

/// A published generation, loaded for serving.
pub(crate) struct LoadedGeneration {
    pub manifest_id: ContentId,
    pub manifest: ReasonedManifest,
    pub state: LedgerState,
}

/// How many loaded generations an instance keeps.
const LOADED_GENERATIONS: usize = 16;

/// How long superseded generations are kept, and how many a head may
/// accumulate inside that window before publication waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeadRetention {
    /// A superseded generation is kept until this long after its successor
    /// was published, so queries already reading it can finish. Nothing
    /// overrides it.
    pub min_age_ms: i64,
    /// Publication waits (refreshes report [`RefreshOutcome::Throttled`])
    /// while this many superseded generations are still inside the window.
    pub max_generations_in_guard: usize,
}

impl Default for HeadRetention {
    fn default() -> Self {
        Self {
            min_age_ms: 15 * 60 * 1000,
            max_generations_in_guard: 32,
        }
    }
}

/// Per-instance reasoned-head state: which heads are being refreshed, the
/// generations loaded for serving, and the retention settings.
#[derive(Default)]
pub(crate) struct HeadState {
    flights: Mutex<HashSet<(String, HeadId)>>,
    loaded: Mutex<HashMap<(String, HeadId), Arc<LoadedGeneration>>>,
    retention: Mutex<HeadRetention>,
    /// Ledgers that registered a head in this process since the worker last
    /// looked, and the signal that wakes it.
    registered_ledgers: Mutex<HashSet<String>>,
    pub(crate) registered: tokio::sync::Notify,
    /// How long a `strict` query waits for a generation at its snapshot.
    strict_wait: Mutex<Option<std::time::Duration>>,
    /// Ledgers whose heads document was empty when last read, and when, so a
    /// reasoning query on a ledger with no heads does not read the
    /// nameservice each time. A registration here clears its ledger at once;
    /// one made elsewhere is seen within [`NO_HEADS_TTL`].
    no_heads: Mutex<HashMap<String, std::time::Instant>>,
}

/// How long an empty heads document is trusted without re-reading.
pub(crate) const NO_HEADS_TTL: std::time::Duration = std::time::Duration::from_secs(30);

impl std::fmt::Debug for HeadState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeadState")
            .field("flights", &self.flights.lock().len())
            .field("loaded", &self.loaded.lock().len())
            .finish()
    }
}

/// Holds a head's refresh slot; one refresh per head at a time.
pub(crate) struct Flight<'a> {
    state: &'a HeadState,
    key: (String, HeadId),
}

impl Drop for Flight<'_> {
    fn drop(&mut self) {
        self.state.flights.lock().remove(&self.key);
    }
}

impl HeadState {
    pub(crate) fn retention(&self) -> HeadRetention {
        *self.retention.lock()
    }

    pub(crate) fn set_retention(&self, retention: HeadRetention) {
        *self.retention.lock() = retention;
    }

    pub(crate) fn strict_wait(&self) -> std::time::Duration {
        self.strict_wait
            .lock()
            .unwrap_or(std::time::Duration::from_secs(10))
    }

    pub(crate) fn set_strict_wait(&self, wait: std::time::Duration) {
        *self.strict_wait.lock() = Some(wait);
    }

    pub(crate) fn note_registered(&self, ledger_id: &str) {
        self.no_heads.lock().remove(ledger_id);
        self.registered_ledgers.lock().insert(ledger_id.to_string());
        self.registered.notify_one();
    }

    pub(crate) fn known_to_have_no_heads(&self, ledger_id: &str) -> bool {
        self.no_heads
            .lock()
            .get(ledger_id)
            .is_some_and(|read_at| read_at.elapsed() < NO_HEADS_TTL)
    }

    pub(crate) fn note_no_heads(&self, ledger_id: &str) {
        let mut no_heads = self.no_heads.lock();
        if no_heads.len() > 4096 {
            no_heads.retain(|_, read_at| read_at.elapsed() < NO_HEADS_TTL);
        }
        no_heads.insert(ledger_id.to_string(), std::time::Instant::now());
    }

    pub(crate) fn take_registered(&self) -> Vec<String> {
        self.registered_ledgers.lock().drain().collect()
    }

    pub(crate) fn begin(&self, ledger_id: &str, head_id: &HeadId) -> Option<Flight<'_>> {
        let key = (ledger_id.to_string(), head_id.clone());
        self.flights
            .lock()
            .insert(key.clone())
            .then_some(Flight { state: self, key })
    }

    pub(crate) fn forget_head(&self, ledger_id: &str, head_id: &HeadId) {
        self.loaded
            .lock()
            .remove(&(ledger_id.to_string(), head_id.clone()));
    }

    fn loaded(
        &self,
        ledger_id: &str,
        head_id: &HeadId,
        manifest_id: &ContentId,
    ) -> Option<Arc<LoadedGeneration>> {
        self.loaded
            .lock()
            .get(&(ledger_id.to_string(), head_id.clone()))
            .filter(|g| &g.manifest_id == manifest_id)
            .cloned()
    }

    fn keep(&self, ledger_id: &str, head_id: &HeadId, generation: Arc<LoadedGeneration>) {
        let mut loaded = self.loaded.lock();
        if loaded.len() >= LOADED_GENERATIONS {
            if let Some(key) = loaded.keys().next().cloned() {
                loaded.remove(&key);
            }
        }
        loaded.insert((ledger_id.to_string(), head_id.clone()), generation);
    }
}
