//! Materialized reasoning: reasoning indexes, the persisted and
//! background-maintained results of registered reasoning programs.
//!
//! - A **reasoning program** is the registered rules and options.
//! - A **reasoning index** is what a branch maintains for one program: the
//!   branch's asserted facts together with the program's derived facts, every
//!   graph, stored under `{branch}/@reasoning/{index_id}/`.
//! - A **generation** is one immutable build of an index, at a basis commit.
//! - The **head** points at an index's currently published generation.
//!
//! A reasoning query whose settled program matches an index whose head is at
//! exactly the query's commit is answered by an ordinary query against that
//! generation, with reasoning off; anything else reasons at query time, as
//! before.
//!
//! - [`build`]: registration, and building and publishing generations.
//! - [`serve`]: matching a query to a servable generation.

mod build;
pub(crate) mod derived;
pub(crate) mod program;
pub(crate) mod serve;
mod worker;

pub use build::{RefreshOutcome, MAX_ACTIVE_INDEXES};
pub use worker::{ReasoningIndexWorker, DEFAULT_CATCHUP_INTERVAL};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluree_db_core::reasoning_index::{ReasoningIndexId, ReasoningManifest};
use fluree_db_core::ContentId;
use parking_lot::Mutex;

use crate::LedgerState;

/// A published generation, loaded for serving.
pub(crate) struct LoadedGeneration {
    pub manifest_id: ContentId,
    pub manifest: ReasoningManifest,
    pub state: LedgerState,
}

/// How many loaded generations an instance keeps.
const LOADED_GENERATIONS: usize = 16;

/// How long superseded generations are kept, and how many an index may
/// accumulate inside that window before publication waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationRetention {
    /// A superseded generation is kept until this long after its successor
    /// was published, so queries already reading it can finish. Nothing
    /// overrides it.
    pub min_age_ms: i64,
    /// Publication waits (refreshes report [`RefreshOutcome::Throttled`])
    /// while this many superseded generations are still inside the window.
    pub max_generations_in_guard: usize,
}

impl Default for GenerationRetention {
    fn default() -> Self {
        Self {
            min_age_ms: 15 * 60 * 1000,
            max_generations_in_guard: 32,
        }
    }
}

/// Per-instance reasoning-index state: which indexes are being refreshed, the
/// generations loaded for serving, and the retention settings.
#[derive(Default)]
pub(crate) struct ReasoningIndexState {
    flights: Mutex<HashSet<(String, ReasoningIndexId)>>,
    loaded: Mutex<HashMap<(String, ReasoningIndexId), Arc<LoadedGeneration>>>,
    retention: Mutex<GenerationRetention>,
    /// Ledgers that registered an index in this process since the worker last
    /// looked, and the signal that wakes it.
    registered_ledgers: Mutex<HashSet<String>>,
    pub(crate) registered: tokio::sync::Notify,
    /// How long a `strict` query waits for a generation at its snapshot.
    strict_wait: Mutex<Option<std::time::Duration>>,
    /// Ledgers whose reasoning-indexes document was empty when last read, and when, so a
    /// reasoning query on a ledger with no indexes does not read the
    /// nameservice each time. A registration here clears its ledger at once;
    /// one made elsewhere is seen within [`NO_INDEXES_TTL`].
    no_indexes: Mutex<HashMap<String, std::time::Instant>>,
    /// Index builds writing files, and sweeps holding them off, by ledger
    /// name. See [`ReasoningIndexState::begin_write`].
    writes: Mutex<IndexWrites>,
    writes_changed: tokio::sync::Notify,
    /// Pauses a refresh after it has written its files and before it
    /// publishes them.
    #[cfg(test)]
    pub(crate) before_publish: Mutex<Option<Arc<PauseSeam>>>,
}

#[derive(Default)]
struct IndexWrites {
    building: HashMap<String, usize>,
    sweeping: HashMap<String, usize>,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct PauseSeam {
    pub reached: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
}

/// An index build writing files under a ledger; the ledger's sweeps wait for
/// it to drop.
pub(crate) struct IndexWrite<'a> {
    state: &'a ReasoningIndexState,
    ledger_name: String,
}

impl Drop for IndexWrite<'_> {
    fn drop(&mut self) {
        let mut writes = self.state.writes.lock();
        if let Some(n) = writes.building.get_mut(&self.ledger_name) {
            *n -= 1;
            if *n == 0 {
                writes.building.remove(&self.ledger_name);
            }
        }
        drop(writes);
        self.state.writes_changed.notify_waiters();
    }
}

/// A sweep of a ledger: no index build of the ledger starts writing until it
/// drops.
pub(crate) struct SweepHold<'a> {
    state: &'a ReasoningIndexState,
    ledger_name: String,
}

impl Drop for SweepHold<'_> {
    fn drop(&mut self) {
        let mut writes = self.state.writes.lock();
        if let Some(n) = writes.sweeping.get_mut(&self.ledger_name) {
            *n -= 1;
            if *n == 0 {
                writes.sweeping.remove(&self.ledger_name);
            }
        }
        drop(writes);
        self.state.writes_changed.notify_waiters();
    }
}

/// How long an empty reasoning-indexes document is trusted without re-reading.
pub(crate) const NO_INDEXES_TTL: std::time::Duration = std::time::Duration::from_secs(30);

impl std::fmt::Debug for ReasoningIndexState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReasoningIndexState")
            .field("flights", &self.flights.lock().len())
            .field("loaded", &self.loaded.lock().len())
            .finish()
    }
}

/// Holds an index's refresh slot; one refresh per index at a time.
pub(crate) struct Flight<'a> {
    state: &'a ReasoningIndexState,
    key: (String, ReasoningIndexId),
}

impl Drop for Flight<'_> {
    fn drop(&mut self) {
        self.state.flights.lock().remove(&self.key);
    }
}

impl ReasoningIndexState {
    pub(crate) fn retention(&self) -> GenerationRetention {
        *self.retention.lock()
    }

    pub(crate) fn set_retention(&self, retention: GenerationRetention) {
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
        self.no_indexes.lock().remove(ledger_id);
        self.registered_ledgers.lock().insert(ledger_id.to_string());
        self.registered.notify_one();
    }

    pub(crate) fn known_to_have_no_indexes(&self, ledger_id: &str) -> bool {
        self.no_indexes
            .lock()
            .get(ledger_id)
            .is_some_and(|read_at| read_at.elapsed() < NO_INDEXES_TTL)
    }

    pub(crate) fn note_no_indexes(&self, ledger_id: &str) {
        let mut no_indexes = self.no_indexes.lock();
        if no_indexes.len() > 4096 {
            no_indexes.retain(|_, read_at| read_at.elapsed() < NO_INDEXES_TTL);
        }
        no_indexes.insert(ledger_id.to_string(), std::time::Instant::now());
    }

    pub(crate) fn take_registered(&self) -> Vec<String> {
        self.registered_ledgers.lock().drain().collect()
    }

    pub(crate) fn begin(&self, ledger_id: &str, index_id: &ReasoningIndexId) -> Option<Flight<'_>> {
        let key = (ledger_id.to_string(), index_id.clone());
        self.flights
            .lock()
            .insert(key.clone())
            .then_some(Flight { state: self, key })
    }

    /// Start writing a generation's files under `ledger_name`, or `None`
    /// while a sweep of the ledger runs. A sweep reclaims every file no
    /// published generation references, so it must not plan while a build
    /// has files written and not yet published.
    pub(crate) fn begin_write(&self, ledger_name: &str) -> Option<IndexWrite<'_>> {
        let mut writes = self.writes.lock();
        if writes.sweeping.contains_key(ledger_name) {
            return None;
        }
        *writes.building.entry(ledger_name.to_string()).or_default() += 1;
        Some(IndexWrite {
            state: self,
            ledger_name: ledger_name.to_string(),
        })
    }

    /// Hold index builds of `ledger_name` off for a sweep, once those already
    /// writing have published or given up.
    pub(crate) async fn hold_for_sweep(&self, ledger_name: &str) -> SweepHold<'_> {
        *self
            .writes
            .lock()
            .sweeping
            .entry(ledger_name.to_string())
            .or_default() += 1;
        let hold = SweepHold {
            state: self,
            ledger_name: ledger_name.to_string(),
        };
        loop {
            // Created before the check, so a drop in between still wakes it.
            let changed = self.writes_changed.notified();
            if !self.writes.lock().building.contains_key(ledger_name) {
                return hold;
            }
            changed.await;
        }
    }

    pub(crate) fn forget_index(&self, ledger_id: &str, index_id: &ReasoningIndexId) {
        self.loaded
            .lock()
            .remove(&(ledger_id.to_string(), index_id.clone()));
    }

    fn loaded(
        &self,
        ledger_id: &str,
        index_id: &ReasoningIndexId,
        manifest_id: &ContentId,
    ) -> Option<Arc<LoadedGeneration>> {
        self.loaded
            .lock()
            .get(&(ledger_id.to_string(), index_id.clone()))
            .filter(|g| &g.manifest_id == manifest_id)
            .cloned()
    }

    fn keep(
        &self,
        ledger_id: &str,
        index_id: &ReasoningIndexId,
        generation: Arc<LoadedGeneration>,
    ) {
        let mut loaded = self.loaded.lock();
        if loaded.len() >= LOADED_GENERATIONS {
            if let Some(key) = loaded.keys().next().cloned() {
                loaded.remove(&key);
            }
        }
        loaded.insert((ledger_id.to_string(), index_id.clone()), generation);
    }
}
