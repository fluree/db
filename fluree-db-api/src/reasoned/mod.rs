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
mod serve;

pub use build::RefreshOutcome;

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

/// Per-instance reasoned-head state: which heads are being refreshed, and
/// the generations loaded for serving.
#[derive(Default)]
pub(crate) struct HeadState {
    flights: Mutex<HashSet<(String, HeadId)>>,
    loaded: Mutex<HashMap<(String, HeadId), Arc<LoadedGeneration>>>,
}

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
