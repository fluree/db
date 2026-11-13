//! Translated overlay ops a reasoning materialization keeps for the batched
//! join lanes.
//!
//! A lane merges a predicate's overlay facts into its probes, translated into
//! the binary index's ids. Under reasoning those facts are the derived extent:
//! translating it costs a dictionary lookup per fact, and query after query
//! reads the same materialization. So the translations live on the
//! materialization ([`DerivedFactsOverlay::attachment`]), not in a cache of
//! their own: they are made lazily, shared by every query reading that
//! materialization, and dropped with it.
//!
//! They count toward the materialization's reasoning memory budget
//! (`ReasoningBudget::max_memory_bytes`), beside its derived facts, once per
//! materialization rather than once per query. A translation that would take
//! the materialization past the budget is not made; the lane declines and its
//! join runs per row.

use std::sync::{Arc, Mutex};

use fluree_db_binary_index::read::types::OverlayOp;
use fluree_db_binary_index::{BinaryIndexStore, RunSortOrder};
use fluree_db_core::{GraphId, Sid};
use fluree_db_reasoner::DerivedFactsOverlay;
use rustc_hash::FxHashMap;

use crate::context::ExecutionContext;
use crate::fast_path_common::SharedOverlayOps;

/// Estimated bytes one translation holds beyond its ops: its key, map slot
/// and shared-slice header.
const ENTRY_OVERHEAD_BYTES: usize = 96;

/// Estimated bytes a translation of `ops` ops holds.
fn translation_bytes(ops: usize) -> usize {
    ops.saturating_mul(std::mem::size_of::<OverlayOp>()) + ENTRY_OVERHEAD_BYTES
}

/// The overlay and store a set of translations was made against. Ids and
/// visibility depend on both, so translations for another binding are not
/// served and are replaced rather than kept beside the new ones.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct OpsBinding {
    content_version: u64,
    store_id: u64,
    snapshot_t: i64,
    to_t: i64,
}

type OpsKey = (GraphId, RunSortOrder, Sid);

/// See the [module docs](self).
pub struct ReasoningOps {
    /// The materialization's reasoning memory budget.
    limit_bytes: usize,
    /// Estimated bytes of the materialization's own facts.
    derived_bytes: usize,
    derived_facts: usize,
    state: Mutex<OpsState>,
}

#[derive(Default)]
struct OpsState {
    binding: Option<OpsBinding>,
    /// `None` is a failed translation: it walks the whole extent and fails
    /// the same way every time. Empty translations aren't kept.
    entries: FxHashMap<OpsKey, Option<SharedOverlayOps>>,
    /// Estimated bytes held by `entries` plus translations under way.
    bytes: usize,
    /// One warning per materialization, not one per query.
    warned: bool,
}

impl OpsState {
    /// Start over when the binding changes: the old translations can't be
    /// served to it.
    fn bind(&mut self, binding: OpsBinding) {
        if self.binding != Some(binding) {
            *self = OpsState {
                binding: Some(binding),
                warned: self.warned,
                ..OpsState::default()
            };
        }
    }
}

impl std::fmt::Debug for ReasoningOps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReasoningOps")
            .field("limit_bytes", &self.limit_bytes)
            .field("derived_bytes", &self.derived_bytes)
            .finish_non_exhaustive()
    }
}

impl ReasoningOps {
    fn new(derived: &DerivedFactsOverlay, limit_bytes: usize) -> Self {
        Self {
            limit_bytes,
            derived_bytes: derived.approx_bytes(),
            derived_facts: derived.len(),
            state: Mutex::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, OpsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The materialization's estimated bytes: its facts and its translations.
    fn retained_bytes(&self, state: &OpsState) -> usize {
        self.derived_bytes.saturating_add(state.bytes)
    }

    fn get(&self, binding: OpsBinding, key: &OpsKey) -> Option<Option<SharedOverlayOps>> {
        let state = self.lock();
        (state.binding == Some(binding))
            .then(|| state.entries.get(key).cloned())
            .flatten()
    }

    /// Reserve `bytes` for a translation about to be made, or refuse when it
    /// would take the materialization past its budget.
    fn reserve(&self, binding: OpsBinding, bytes: usize, pred: &Sid) -> bool {
        let mut state = self.lock();
        state.bind(binding);
        if self.retained_bytes(&state).saturating_add(bytes) > self.limit_bytes {
            self.over_budget(&mut state, bytes, pred);
            return false;
        }
        state.bytes += bytes;
        true
    }

    /// Keep a finished translation, replacing its reservation with its
    /// actual size. A translation made without a reservation (its size is
    /// known only once made) is kept only if it fits.
    fn finish(
        &self,
        binding: OpsBinding,
        key: OpsKey,
        reserved: usize,
        ops: Option<SharedOverlayOps>,
    ) {
        let mut state = self.lock();
        if reserved == 0 {
            state.bind(binding);
        } else if state.binding != Some(binding) {
            // Replaced by another binding meanwhile; its reservation went with it.
            return;
        }
        state.bytes = state.bytes.saturating_sub(reserved);
        if ops.as_ref().is_some_and(|ops| ops.is_empty()) || state.entries.contains_key(&key) {
            return;
        }
        let bytes = translation_bytes(ops.as_ref().map_or(0, |ops| ops.len()));
        if reserved == 0 && self.retained_bytes(&state).saturating_add(bytes) > self.limit_bytes {
            self.over_budget(&mut state, bytes, &key.2);
            return;
        }
        state.bytes += bytes;
        state.entries.insert(key, ops);
    }

    fn release(&self, binding: OpsBinding, reserved: usize) {
        let mut state = self.lock();
        if state.binding == Some(binding) {
            state.bytes = state.bytes.saturating_sub(reserved);
        }
    }

    fn over_budget(&self, state: &mut OpsState, bytes: usize, pred: &Sid) {
        let retained = self.retained_bytes(state);
        tracing::debug!(
            ?pred,
            estimated_bytes = bytes,
            retained_estimated_bytes = retained,
            limit_bytes = self.limit_bytes,
            "batched join lane: overlay translation over the reasoning memory budget; per-row"
        );
        if std::mem::replace(&mut state.warned, true) {
            return;
        }
        tracing::warn!(
            phase = "batched join lane: translating derived facts",
            ?pred,
            translation_estimated_bytes = bytes,
            retained_estimated_bytes = retained,
            limit_bytes = self.limit_bytes,
            derived_facts = self.derived_facts,
            "A reasoning query's batched join would take its reasoning result past the \
             reasoning memory limit (sizes are estimates), so the join runs per row \
             instead: correct, but slower. A cached reasoning result holds its derived \
             facts and these translations, up to the limit, for every query reading it, \
             and up to 16 results are cached at once. If the host has memory for it, \
             raise the limit (f:reasoningMaxMemoryMb in ledger config, \"reasoningBudget\" \
             per query, or FLUREE_REASONING_MAX_MEMORY_MB); if not, run fewer reasoning \
             workloads concurrently, narrow the rules and queries, or register a \
             reasoning index, whose queries read the persisted derived facts and hold \
             no result in memory (building one still computes the closure in memory; \
             \"reasoningFreshness\": \"strict\" keeps a query from reasoning in memory \
             when no generation can answer it). Reported once per reasoning result."
        );
    }
}

/// A query's view of its materialization's translations. Served only to
/// contexts reading the overlay the materialization was attached for, so a
/// dataset member reading other novelty never rebinds it.
#[derive(Clone, Debug)]
pub struct ReasoningOpsHandle {
    ops: Arc<ReasoningOps>,
    content_version: u64,
}

impl ReasoningOpsHandle {
    /// The translations attached to `derived`, for a query whose overlay
    /// (`content_version`) reads it under a reasoning budget of
    /// `limit_bytes`. `None` when the overlay can't vouch for its content.
    pub fn attach(
        derived: &DerivedFactsOverlay,
        limit_bytes: usize,
        content_version: Option<u64>,
    ) -> Option<Self> {
        let content_version = content_version?;
        let ops = derived.attachment(|| ReasoningOps::new(derived, limit_bytes))?;
        Some(Self {
            ops,
            content_version,
        })
    }
}

/// The materialization's translations and this context's binding, when the
/// context reads the overlay they belong to.
fn for_ctx<'c>(
    ctx: &'c ExecutionContext<'_>,
    store: &BinaryIndexStore,
) -> Option<(&'c ReasoningOps, OpsBinding)> {
    let handle = ctx.reasoning_ops.as_ref()?;
    let content_version = ctx.overlay().content_version()?;
    (content_version == handle.content_version).then(|| {
        (
            &*handle.ops,
            OpsBinding {
                content_version,
                store_id: store.store_id(),
                snapshot_t: ctx.active_snapshot.t,
                to_t: ctx.to_t,
            },
        )
    })
}

/// Whether `(g_id, order, pred_sid)` is translated on the materialization.
pub(crate) fn cached(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    order: RunSortOrder,
    pred_sid: &Sid,
) -> bool {
    for_ctx(ctx, store)
        .is_some_and(|(ops, binding)| ops.get(binding, &(g_id, order, pred_sid.clone())).is_some())
}

/// The translation for `(g_id, order, pred_sid)`: the materialization's when
/// it has one, else `compute`'s, kept on the materialization when it fits.
/// `estimate` is the translation's op count when it can be known before
/// making it; a translation that won't fit is not made, and reads as a failed
/// one (`None`), which declines the lane. Outside a reasoning query this is
/// `compute`.
pub(crate) fn get_or_translate(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    order: RunSortOrder,
    pred_sid: &Sid,
    estimate: impl FnOnce() -> Option<usize>,
    compute: impl FnOnce() -> crate::error::Result<Option<SharedOverlayOps>>,
) -> crate::error::Result<Option<SharedOverlayOps>> {
    let Some((ops, binding)) = for_ctx(ctx, store) else {
        return compute();
    };
    let key = (g_id, order, pred_sid.clone());
    if let Some(hit) = ops.get(binding, &key) {
        return Ok(hit);
    }
    let reserved = match estimate() {
        Some(count) => {
            let bytes = translation_bytes(count);
            if !ops.reserve(binding, bytes, pred_sid) {
                return Ok(None);
            }
            bytes
        }
        None => 0,
    };
    match compute() {
        Ok(computed) => {
            ops.finish(binding, key, reserved, computed.clone());
            Ok(computed)
        }
        Err(e) => {
            ops.release(binding, reserved);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(store_id: u64) -> OpsBinding {
        OpsBinding {
            content_version: 1,
            store_id,
            snapshot_t: 1,
            to_t: 1,
        }
    }

    fn ops_of(n: usize) -> SharedOverlayOps {
        (0..n as u64)
            .map(|s_id| OverlayOp {
                s_id,
                p_id: 0,
                o_type: 0,
                o_key: 0,
                o_i: 0,
                t: 1,
                op: true,
                g_id: 0,
            })
            .collect()
    }

    fn ops_with_limit(limit_bytes: usize) -> ReasoningOps {
        ReasoningOps {
            limit_bytes,
            derived_bytes: 1000,
            derived_facts: 10,
            state: Mutex::default(),
        }
    }

    fn key(name: &str) -> OpsKey {
        (0, RunSortOrder::Psot, Sid::new(100, name))
    }

    #[test]
    fn translations_count_against_the_budget_beside_the_derived_facts() {
        let ten = translation_bytes(10);
        let ops = ops_with_limit(1000 + 2 * ten);
        let b = binding(1);
        assert!(ops.reserve(b, ten, &key("a").2));
        ops.finish(b, key("a"), ten, Some(ops_of(10)));
        assert!(ops.reserve(b, ten, &key("b").2));
        ops.finish(b, key("b"), ten, Some(ops_of(10)));
        assert!(
            !ops.reserve(b, ten, &key("c").2),
            "a third would pass the budget"
        );
        assert_eq!(ops.lock().bytes, 2 * ten);
        assert!(ops.get(b, &key("a")).is_some());
        assert!(
            ops.get(b, &key("c")).is_none(),
            "a refused translation isn't kept"
        );
    }

    #[test]
    fn a_new_binding_replaces_the_old_translations() {
        let ops = ops_with_limit(usize::MAX);
        let ten = translation_bytes(10);
        assert!(ops.reserve(binding(1), ten, &key("a").2));
        ops.finish(binding(1), key("a"), ten, Some(ops_of(10)));
        assert!(ops.reserve(binding(2), ten, &key("a").2));
        let state = ops.lock();
        assert_eq!(state.entries.len(), 0, "store 1's translations are gone");
        assert_eq!(state.bytes, ten, "only store 2's reservation is held");
    }

    #[test]
    fn a_translation_finished_under_a_replaced_binding_is_dropped() {
        let ops = ops_with_limit(usize::MAX);
        let ten = translation_bytes(10);
        assert!(ops.reserve(binding(1), ten, &key("a").2));
        assert!(ops.reserve(binding(2), ten, &key("b").2));
        ops.finish(binding(1), key("a"), ten, Some(ops_of(10)));
        assert!(ops.get(binding(2), &key("a")).is_none());
        assert_eq!(ops.lock().bytes, ten);
    }

    #[test]
    fn empty_translations_are_not_kept_and_failed_ones_are() {
        let ops = ops_with_limit(usize::MAX);
        let b = binding(1);
        ops.finish(b, key("empty"), 0, Some(ops_of(0)));
        ops.finish(b, key("failed"), 0, None);
        assert!(ops.get(b, &key("empty")).is_none());
        assert!(matches!(ops.get(b, &key("failed")), Some(None)));
        assert_eq!(ops.lock().bytes, translation_bytes(0));
    }

    #[test]
    fn an_unestimated_translation_is_kept_only_if_it_fits() {
        let ten = translation_bytes(10);
        let ops = ops_with_limit(1000 + ten);
        let b = binding(1);
        ops.finish(b, key("a"), 0, Some(ops_of(10)));
        ops.finish(b, key("b"), 0, Some(ops_of(10)));
        assert!(ops.get(b, &key("a")).is_some());
        assert!(ops.get(b, &key("b")).is_none());
        assert_eq!(ops.lock().bytes, ten);
    }
}
