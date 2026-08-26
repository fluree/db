//! V3 range provider — implements `RangeProvider` for V6 indexes.
//!
//! Plugs into `range_with_overlay()` so all 25+ callers (policy, SHACL,
//! reasoner, property paths, API) transparently query V3 indexes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{
    BinaryCursor, BinaryFilter, BinaryGraphView, BinaryIndexStore, ColumnProjection, RunSortOrder,
};
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{
    flake_matches_range_eq, range_provider::RangeQuery, Flake, FlakeValue, GraphId, IndexType,
    OType, OverlayProvider, RangeMatch, RangeOptions, RangeProvider, RangeTest, RuntimeSmallDicts,
    Sid,
};

use crate::binary_scan::{encode_bound_object_prefilter, index_type_to_sort_order};

/// Result of translating overlay flakes into V3 `OverlayOp`s.
///
/// If `failed=true`, `ops` is incomplete and callers must use `raw` to preserve correctness.
struct OverlayTranslateV3Result {
    ops: Vec<fluree_db_binary_index::OverlayOp>,
    raw: Vec<Flake>,
    ephemeral_p_id_to_sid: HashMap<u32, Sid>,
    failed: bool,
}

/// Translate overlay flakes to V3 `OverlayOp`s, capturing raw flakes on failure.
///
/// This is a correctness helper shared across range-provider entry points.
/// When translation fails (e.g., missing dict novelty), callers must not silently
/// drop overlay flakes — they should either fall back to raw overlay merging or fail.
#[allow(clippy::too_many_arguments)]
fn translate_overlay_ops_v3_with_raw(
    overlay: &dyn OverlayProvider,
    g_id: GraphId,
    index: IndexType,
    to_t: i64,
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    runtime_small_dicts: &Arc<RuntimeSmallDicts>,
    mut include: impl FnMut(&Flake) -> bool,
    warn_ctx: &'static str,
) -> OverlayTranslateV3Result {
    let mut ephemeral_preds: HashMap<Sid, u32> = HashMap::new();
    // Runtime dicts should normally be seeded from the persisted store, but use the
    // store count as a floor so novelty-only predicates can never collide with
    // persisted predicate IDs if a caller hands us an unseeded/runtime-empty dict.
    let mut next_ep = runtime_small_dicts
        .predicate_count()
        .max(store.predicate_count());
    let mut ops: Vec<fluree_db_binary_index::OverlayOp> = Vec::new();
    let mut raw: Vec<Flake> = Vec::new();
    let mut failed = false;
    let mut unsupported_count: u64 = 0;
    let mut error_count: u64 = 0;
    let mut first_error: Option<String> = None;

    overlay.for_each_overlay_flake(g_id, index, None, None, true, to_t, &mut |flake| {
        if !include(flake) {
            return;
        }
        match crate::binary_scan::translate_one_flake_v3_pub(
            flake,
            store,
            Some(dict_novelty),
            Some(runtime_small_dicts),
            &mut ephemeral_preds,
            &mut next_ep,
            g_id,
        ) {
            Ok(op) => ops.push(op),
            Err(e) => {
                failed = true;
                raw.push(flake.clone());
                if e.kind() == std::io::ErrorKind::Unsupported {
                    unsupported_count += 1;
                } else {
                    error_count += 1;
                }
                if first_error.is_none() {
                    first_error = Some(e.to_string());
                }
                tracing::debug!(
                    ctx = warn_ctx,
                    error = %e,
                    s = %flake.s,
                    p = %flake.p,
                    t = flake.t,
                    op = flake.op,
                    "failed to translate overlay flake; will merge as raw flake"
                );
            }
        }
    });

    // One summary per translation call, not one line per flake: a novelty
    // tail full of untranslatable values (e.g. arena-new decimals) used to
    // emit a WARN per flake per query — a log storm at INFO. `Unsupported`
    // is a handled condition (the raw-flake merge is correct), so it stays
    // at debug; unexpected errors keep a warn.
    if failed {
        if error_count > 0 {
            tracing::warn!(
                ctx = warn_ctx,
                unsupported = unsupported_count,
                errors = error_count,
                first_error = first_error.as_deref().unwrap_or(""),
                "some overlay flakes failed V3 translation; merging as raw flakes"
            );
        } else {
            tracing::debug!(
                ctx = warn_ctx,
                unsupported = unsupported_count,
                first_error = first_error.as_deref().unwrap_or(""),
                "some overlay flakes are not V3-translatable; merging as raw flakes"
            );
        }
    }

    let ephemeral_p_id_to_sid: HashMap<u32, Sid> = ephemeral_preds
        .into_iter()
        .map(|(sid, id)| (id, sid))
        .collect();

    OverlayTranslateV3Result {
        ops,
        raw,
        ephemeral_p_id_to_sid,
        failed,
    }
}

/// Identity of a cached range-provider overlay translation.
///
/// Every component that can change the translated product is included:
/// `store_id` is process-unique per `BinaryIndexStore` instance (covering
/// ledger identity and same-`index_t` store rebuilds that re-rank dict ids),
/// `index_t` covers in-place incremental index advances, `content_version`
/// is the overlay's globally-unique content stamp (see
/// [`OverlayProvider::content_version`] — overlays that cannot vouch for one
/// are never cached), and `to_t` bounds which overlay flakes the walk emits.
#[derive(Clone, PartialEq, Eq, Hash)]
struct RangeTranslationKey {
    store_id: u64,
    index_t: i64,
    content_version: u64,
    to_t: i64,
    g_id: GraphId,
    index: IndexType,
}

/// Cached product of an **unfiltered** `translate_overlay_ops_v3_with_raw`
/// call: ops sorted by the key's index order with lifecycles resolved, plus
/// the raw untranslatable flakes (retracts intact — the raw-merge fallback
/// in `binary_range_eq_v3` needs them to cancel base facts) and the
/// ephemeral predicate mapping for decode.
struct CachedRangeTranslation {
    ops: Arc<[fluree_db_binary_index::OverlayOp]>,
    raw: Arc<[Flake]>,
    ephemeral_p_id_to_sid: Arc<HashMap<u32, Sid>>,
}

/// Cross-call LRU of range-provider overlay translations.
///
/// `range_with_overlay` looks like a cheap point lookup at call sites, but
/// each call re-walked the graph's entire novelty, re-translated every flake
/// (dict probes), and re-sorted the op set — so per-flake lookup loops
/// (staging list-meta hydration, policy class lookups, upsert deletions,
/// annotation cascades) cost O(calls × novelty log novelty) on
/// novelty-heavy ledgers. Entries are large (~50 B/op), so capacity stays
/// small: one or two hot database states × a few index orders.
type RangeTranslationLru = lru::LruCache<RangeTranslationKey, Arc<CachedRangeTranslation>>;

fn range_translation_cache() -> &'static std::sync::Mutex<RangeTranslationLru> {
    use once_cell::sync::Lazy;
    static CACHE: Lazy<std::sync::Mutex<RangeTranslationLru>> = Lazy::new(|| {
        std::sync::Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(8).expect("capacity must be > 0"),
        ))
    });
    &CACHE
}

/// Translate the full (unfiltered) overlay for `(g_id, index, to_t)`, served
/// from [`range_translation_cache`] when the overlay reports a
/// [`content_version`](OverlayProvider::content_version). Returns `None`
/// when the overlay opts out of caching — callers fall back to a fresh
/// per-call translation, preserving pre-cache behavior.
#[allow(clippy::too_many_arguments)]
fn cached_overlay_translation(
    overlay: &dyn OverlayProvider,
    g_id: GraphId,
    index: IndexType,
    effective_to_t: i64,
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    runtime_small_dicts: &Arc<RuntimeSmallDicts>,
    warn_ctx: &'static str,
) -> Option<Arc<CachedRangeTranslation>> {
    let content_version = overlay.content_version()?;
    let key = RangeTranslationKey {
        store_id: store.store_id(),
        index_t: store.max_t(),
        content_version,
        to_t: effective_to_t,
        g_id,
        index,
    };

    if let Some(hit) = range_translation_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
    {
        return Some(Arc::clone(hit));
    }

    let OverlayTranslateV3Result {
        mut ops,
        raw,
        ephemeral_p_id_to_sid,
        failed: _,
    } = translate_overlay_ops_v3_with_raw(
        overlay,
        g_id,
        index,
        effective_to_t,
        store,
        dict_novelty,
        runtime_small_dicts,
        |_| true,
        warn_ctx,
    );
    let order = index_type_to_sort_order(index);
    fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, order);
    fluree_db_binary_index::read::types::resolve_overlay_ops(&mut ops);

    let entry = Arc::new(CachedRangeTranslation {
        ops: ops.into(),
        raw: raw.into(),
        ephemeral_p_id_to_sid: Arc::new(ephemeral_p_id_to_sid),
    });
    range_translation_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .put(key, Arc::clone(&entry));
    Some(entry)
}

/// Try persisted lookup first, then DictNovelty. Returns `None` if neither resolves.
fn resolve_or_novelty<T>(
    persisted: Option<T>,
    dict_novelty: &DictNovelty,
    novelty_lookup: impl FnOnce() -> Option<T>,
) -> Option<T> {
    match persisted {
        Some(id) => Some(id),
        None if dict_novelty.is_initialized() => novelty_lookup(),
        None => None,
    }
}

/// V3 range provider: wraps `BinaryIndexStore` to serve `range_with_overlay()` callers.
///
/// Graph ID is passed per-call (not embedded), so one provider serves all graphs.
pub struct BinaryRangeProvider {
    store: Arc<BinaryIndexStore>,
    dict_novelty: Arc<DictNovelty>,
    runtime_small_dicts: Arc<RuntimeSmallDicts>,
    namespace_codes_fallback: Option<Arc<HashMap<u16, String>>>,
}

impl BinaryRangeProvider {
    pub fn new(
        store: Arc<BinaryIndexStore>,
        dict_novelty: Arc<DictNovelty>,
        runtime_small_dicts: Arc<RuntimeSmallDicts>,
        namespace_codes_fallback: Option<Arc<HashMap<u16, String>>>,
    ) -> Self {
        Self {
            store,
            dict_novelty,
            runtime_small_dicts,
            namespace_codes_fallback,
        }
    }

    /// Access the underlying `BinaryIndexStore`.
    pub fn store(&self) -> &Arc<BinaryIndexStore> {
        &self.store
    }

    /// Access the `DictNovelty` used for overlay decoding.
    pub fn dict_novelty(&self) -> &Arc<DictNovelty> {
        &self.dict_novelty
    }

    /// Access the runtime predicate/datatype dictionaries used for overlay translation.
    pub fn runtime_small_dicts(&self) -> &Arc<RuntimeSmallDicts> {
        &self.runtime_small_dicts
    }
}

impl RangeProvider for BinaryRangeProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn range(&self, query: &RangeQuery<'_>) -> std::io::Result<Vec<Flake>> {
        match query.test {
            RangeTest::Eq => binary_range_eq_v3(
                &self.store,
                &self.dict_novelty,
                &self.runtime_small_dicts,
                query.g_id,
                query.index,
                query.match_val,
                query.opts,
                query.overlay,
                query.tracker,
            ),
            test => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("V3 range provider: unsupported RangeTest {test:?}"),
            )),
        }
    }

    fn range_bounded(
        &self,
        g_id: GraphId,
        index: IndexType,
        start_bound: &Flake,
        end_bound: &Flake,
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> std::io::Result<Vec<Flake>> {
        binary_range_bounded_v3(
            &self.store,
            &self.dict_novelty,
            &self.runtime_small_dicts,
            &self.namespace_codes_fallback,
            g_id,
            index,
            start_bound,
            end_bound,
            opts,
            overlay,
        )
    }

    fn lookup_subject_predicate_refs_batched(
        &self,
        g_id: GraphId,
        index: IndexType,
        predicate: &Sid,
        subjects: &[Sid],
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> std::io::Result<HashMap<Sid, Vec<Sid>>> {
        binary_lookup_subject_predicate_refs_batched_v3(
            &self.store,
            &self.dict_novelty,
            &self.runtime_small_dicts,
            g_id,
            index,
            predicate,
            subjects,
            opts,
            overlay,
        )
    }
}

/// V3 equality range query: scan the appropriate index order with filters,
/// decode each row to a `Flake`, apply overlay merge.
#[allow(clippy::too_many_arguments)]
fn binary_range_eq_v3(
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    runtime_small_dicts: &Arc<RuntimeSmallDicts>,
    g_id: GraphId,
    index: IndexType,
    match_val: &RangeMatch,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
    tracker: Option<&fluree_db_core::Tracker>,
) -> std::io::Result<Vec<fluree_db_core::Flake>> {
    let order = index_type_to_sort_order(index);
    let view = {
        let v =
            BinaryGraphView::with_novelty(Arc::clone(store), g_id, Some(Arc::clone(dict_novelty)));
        match tracker {
            Some(t) => v.with_tracker(t.clone()),
            None => v,
        }
    };

    // Build filter from bound match components.
    let mut filter = BinaryFilter::default();

    if let Some(s_sid) = &match_val.s {
        // Prefer persisted reverse dict, then DictNovelty. If neither can map
        // this subject to an s_id, there are no base rows to scan; return
        // overlay-only matches.
        match resolve_or_novelty(
            store.find_subject_id_by_parts(s_sid.namespace_code, &s_sid.name)?,
            dict_novelty,
            || {
                dict_novelty
                    .subjects
                    .find_subject(s_sid.namespace_code, &s_sid.name)
            },
        ) {
            Some(id) => filter.s_id = Some(id),
            None => return overlay_only_flakes(store, g_id, index, match_val, opts, overlay),
        }
    }
    if let Some(p_sid) = &match_val.p {
        match store.sid_to_p_id(p_sid) {
            Some(id) => filter.p_id = Some(id),
            None => {
                // Unknown predicate in persisted dict: base scan cannot match.
                // Overlay may still contain this predicate (novelty), so return overlay-only.
                return overlay_only_flakes(store, g_id, index, match_val, opts, overlay);
            }
        }
    }
    if let Some(o_val) = &match_val.o {
        match o_val {
            fluree_db_core::FlakeValue::Ref(sid) => {
                // Resolve ref object to an s_id (persisted → DictNovelty).
                let o_id = match resolve_or_novelty(
                    store.find_subject_id_by_parts(sid.namespace_code, &sid.name)?,
                    dict_novelty,
                    || {
                        dict_novelty
                            .subjects
                            .find_subject(sid.namespace_code, &sid.name)
                    },
                ) {
                    Some(id) => id,
                    None => {
                        return overlay_only_flakes(store, g_id, index, match_val, opts, overlay)
                    }
                };
                filter.o_type = Some(OType::IRI_REF.as_u16());
                filter.o_key = Some(o_id);
            }
            fluree_db_core::FlakeValue::String(s) => {
                // Resolve string dict id (persisted → DictNovelty).
                let str_id =
                    match resolve_or_novelty(store.find_string_id(s)?, dict_novelty, || {
                        dict_novelty.strings.find_string(s)
                    }) {
                        Some(id) => id,
                        None => {
                            return overlay_only_flakes(
                                store, g_id, index, match_val, opts, overlay,
                            )
                        }
                    };
                filter.o_type = Some(OType::XSD_STRING.as_u16());
                filter.o_key = Some(str_id as u64);
            }
            fluree_db_core::FlakeValue::Json(s) => {
                // JSON values share the string dictionary but use OType::RDF_JSON.
                // Same persisted → DictNovelty resolution as strings.
                let str_id =
                    match resolve_or_novelty(store.find_string_id(s)?, dict_novelty, || {
                        dict_novelty.strings.find_string(s)
                    }) {
                        Some(id) => id,
                        None => {
                            return overlay_only_flakes(
                                store, g_id, index, match_val, opts, overlay,
                            )
                        }
                    };
                filter.o_type = Some(OType::RDF_JSON.as_u16());
                filter.o_key = Some(str_id as u64);
            }
            _ => {
                // Use the same bound-object prefilter semantics as BinaryScanOperator:
                // preserve untyped numeric family matching by not forcing an exact o_type.
                if let Ok(prefilter) = encode_bound_object_prefilter(
                    o_val,
                    match_val.dt.as_ref(),
                    None,
                    store,
                    Some(dict_novelty),
                ) {
                    filter.o_type = prefilter.o_type.map(OType::as_u16);
                    filter.o_key = Some(prefilter.o_key);
                }
            }
        }
    }

    // Get branch manifest.
    let branch = match store.branch_for_order(g_id, order) {
        Some(b) => Arc::clone(b),
        None => {
            // No branch for this order — return overlay-only results if any.
            return overlay_only_flakes(store, g_id, index, match_val, opts, overlay);
        }
    };

    // Resolve the optional projection-predicate allow-list once.
    //
    // `predicate_filter_p_ids` is the row-loop's allow-set of `u32` p_ids.
    // We seed it with persisted-dict resolutions here; novelty-only
    // predicates (no persisted p_id yet) get appended below after overlay
    // translation surfaces their ephemeral p_ids via `ephemeral_p_id_to_sid`.
    // Without that extension, an overlay assert on a novelty-only selected
    // predicate would survive the overlay translator's Sid filter, land in
    // the cursor stream with an ephemeral p_id, and then get silently
    // dropped here.
    let mut predicate_filter_p_ids: Option<Vec<u32>> =
        opts.predicate_filter.as_deref().map(|sids| {
            sids.iter()
                .filter_map(|s| store.sid_to_p_id(s))
                .collect::<Vec<u32>>()
        });

    // Create cursor: use range-narrowed scan when any filter field is bound,
    // matching the pattern in BinaryScanOperator::open. For novelty-only subjects
    // this yields an empty leaf_range, so the cursor drains overlay ops directly
    // with zero leaf I/O.
    let projection = ColumnProjection::all();
    let use_range = filter.s_id.is_some()
        || filter.p_id.is_some()
        || filter.o_type.is_some()
        || filter.o_key.is_some();

    let mut range_keys: Option<(RunRecordV2, RunRecordV2)> = None;
    let mut cursor = if use_range {
        let min_key = RunRecordV2 {
            s_id: SubjectId(filter.s_id.unwrap_or(0)),
            o_key: filter.o_key.unwrap_or(0),
            p_id: filter.p_id.unwrap_or(0),
            t: 0,
            o_i: 0,
            o_type: filter.o_type.unwrap_or(0),
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId(filter.s_id.unwrap_or(u64::MAX)),
            o_key: filter.o_key.unwrap_or(u64::MAX),
            p_id: filter.p_id.unwrap_or(u32::MAX),
            t: u32::MAX,
            o_i: u32::MAX,
            o_type: filter.o_type.unwrap_or(u16::MAX),
            g_id,
        };
        let cursor = BinaryCursor::new(
            Arc::clone(store),
            order,
            branch,
            &min_key,
            &max_key,
            filter,
            projection,
        );
        range_keys = Some((min_key, max_key));
        cursor
    } else {
        BinaryCursor::scan_all(Arc::clone(store), order, branch, filter, projection)
    };

    if let Some(t) = tracker {
        cursor = cursor.with_tracker(t.clone());
    }

    // Apply overlay.
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    cursor.set_to_t(effective_to_t);

    // Overlay translation. Unfiltered translations are served from the
    // cross-call LRU when the overlay reports a content version (raw
    // `Novelty` does) — see `range_translation_cache` for why fresh per-call
    // translation makes point-lookup loops quadratic in novelty size.
    //
    // When the caller supplied a projection-predicate allow-list, translate
    // fresh with the `flake.p` filter as before — the allow-list changes
    // both the translated set and the raw-fallback set (it must not smuggle
    // non-selected predicates back in), so that product is not cacheable
    // under the unfiltered key. Sid match (vs persisted p_id) so novel
    // predicates still pass.
    let cached = if opts.predicate_filter.is_none() {
        cached_overlay_translation(
            overlay,
            g_id,
            index,
            effective_to_t,
            store,
            dict_novelty,
            runtime_small_dicts,
            "V3 range",
        )
    } else {
        None
    };
    let (overlay_ops, untranslated, ephemeral_p_id_to_sid) = match cached {
        Some(entry) => (
            Arc::clone(&entry.ops),
            entry.raw.to_vec(),
            Arc::clone(&entry.ephemeral_p_id_to_sid),
        ),
        None => {
            let predicate_filter_sids = opts.predicate_filter.clone();
            let OverlayTranslateV3Result {
                mut ops,
                raw,
                ephemeral_p_id_to_sid,
                failed: _overlay_failed_translation,
            } = translate_overlay_ops_v3_with_raw(
                overlay,
                g_id,
                index,
                effective_to_t,
                store,
                dict_novelty,
                runtime_small_dicts,
                move |flake| match &predicate_filter_sids {
                    Some(allow) => allow.iter().any(|p| p == &flake.p),
                    None => true,
                },
                "V3 range",
            );
            fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, order);
            fluree_db_binary_index::read::types::resolve_overlay_ops(&mut ops);
            (
                Arc::<[fluree_db_binary_index::OverlayOp]>::from(ops),
                raw,
                Arc::new(ephemeral_p_id_to_sid),
            )
        }
    };

    if !overlay_ops.is_empty() {
        // Range-bounded cursors get only the ops window intersecting
        // [min_key, max_key]: out-of-range ops can never match the filter,
        // and carrying them costs an O(overlay) merge walk per call while
        // defeating leaflet pre-skips (same pattern as
        // `BinaryScanOperator::open`).
        let (start, end) = match &range_keys {
            Some((min_key, max_key)) => fluree_db_binary_index::overlay_window_for_range(
                &overlay_ops,
                min_key,
                max_key,
                order,
            ),
            None => (0, overlay_ops.len()),
        };
        if start < end {
            cursor.set_overlay_ops_window(overlay_ops, start, end);
        }
    }

    // Extend the row-loop allow-set with ephemeral p_ids whose mapped Sid is
    // in the caller's allow-list — these are novelty-only predicates that
    // have no persisted p_id yet, so they were not captured during the
    // initial Sid-to-p_id resolution. The overlay translator already let
    // them through via Sid match; without this step the row loop would
    // drop them under their cursor-side ephemeral p_id.
    if let (Some(allow_sids), Some(allow_ids)) = (
        opts.predicate_filter.as_deref(),
        predicate_filter_p_ids.as_mut(),
    ) {
        for (eph_p_id, sid) in ephemeral_p_id_to_sid.iter() {
            if allow_sids.iter().any(|s| s == sid) {
                allow_ids.push(*eph_p_id);
            }
        }
        allow_ids.sort_unstable();
        allow_ids.dedup();
    }

    // Iterate and decode to Flakes.
    let has_untranslated = !untranslated.is_empty();
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);
    let mut flakes = Vec::new();
    let mut skipped = 0usize;

    while let Some(batch) = cursor.next_batch()? {
        for i in 0..batch.row_count {
            let p_id = batch.p_id.get_or(i, 0);

            // Projection-predicate gate. Skip discarded predicates before any
            // dict touch (subject resolve, predicate IRI, object decode,
            // datatype/lang lookups) — purely an integer probe.
            if let Some(allow) = &predicate_filter_p_ids {
                if allow.binary_search(&p_id).is_err() {
                    continue;
                }
            }

            let s_id = batch.s_id.get(i);
            let o_type = batch.o_type.get_or(i, 0);
            let o_key = batch.o_key.get(i);
            let t = batch.t.get_or(i, 0) as i64;
            let o_i = batch.o_i.get_or(i, u32::MAX);

            // Resolve subject. Reuse the caller-supplied Sid when present —
            // the bound `match_val.s` IS the subject for every base row this
            // scan returns, so the per-row `resolve_subject_sid` (a dict
            // touch on the persisted path) is redundant.
            let s_sid = match &match_val.s {
                Some(sid) => sid.clone(),
                None => resolve_sid(s_id, &view)?,
            };
            // Resolve predicate: persisted table first, then ephemeral overlay map.
            let p_sid = match store.p_sid_table().get(p_id as usize) {
                Some(sid) => sid.clone(),
                None => match ephemeral_p_id_to_sid.get(&p_id) {
                    Some(sid) => sid.clone(),
                    None => continue, // truly unknown — shouldn't happen
                },
            };
            // Decode object.
            let o_val = view.decode_value(o_type, o_key, p_id)?;
            // Resolve datatype (value-aware: NUM_BIG_OVERFLOW names no o_type).
            let dt = store
                .resolve_datatype_sid_for_value(o_type, &o_val)
                .unwrap_or_else(|| Sid::new(0, ""));
            // Language tag.
            let lang = store
                .resolve_lang_tag(o_type)
                .map(std::string::ToString::to_string);
            // List index.
            let meta = if lang.is_some() || o_i != u32::MAX {
                Some(fluree_db_core::FlakeMeta {
                    lang,
                    i: if o_i != u32::MAX {
                        Some(o_i as i32)
                    } else {
                        None
                    },
                })
            } else {
                None
            };

            let flake = fluree_db_core::Flake {
                g: None,
                s: s_sid,
                p: p_sid,
                o: o_val,
                dt,
                t,
                op: true,
                m: meta,
            };

            if has_untranslated {
                flakes.push(flake);
                continue;
            }

            if !flake_matches_range_eq(&flake, match_val) {
                continue;
            }

            // Fast path filters/limits.
            if let Some(bounds) = &opts.object_bounds {
                if !bounds.matches(&flake.o) {
                    continue;
                }
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            flakes.push(flake);
            if flakes.len() >= limit {
                return Ok(flakes);
            }
        }
    }

    if !has_untranslated {
        return Ok(flakes);
    }

    // Correctness fallback: merge untranslated overlay flakes (including retracts),
    // resolve per-fact lifecycles (latest-op-wins), then apply RangeOptions.
    flakes.extend(untranslated);
    let mut resolved = resolve_latest_ops_keep_asserts(flakes, index);
    resolved.retain(|f| flake_matches_range_eq(f, match_val));

    if let Some(bounds) = &opts.object_bounds {
        resolved.retain(|f| bounds.matches(&f.o));
    }
    if offset > 0 && !resolved.is_empty() {
        let n = offset.min(resolved.len());
        resolved.drain(0..n);
    }
    if resolved.len() > limit {
        resolved.truncate(limit);
    }

    Ok(resolved)
}

/// Resolve a subject integer ID to Sid.
///
/// Delegates to `BinaryGraphView::resolve_subject_sid` which handles
/// watermark-based novelty routing internally: novel subjects return
/// `Sid::new(ns_code, suffix)` directly (no IRI string + trie lookup).
#[inline]
fn resolve_sid(s_id: u64, view: &BinaryGraphView) -> std::io::Result<Sid> {
    view.resolve_subject_sid(s_id)
}

/// Resolve fact lifecycles (latest op wins) and drop retracts.
///
/// Used as a correctness fallback when some overlay flakes cannot be translated
/// into V3 `OverlayOp`s (e.g., missing dict novelty), and by the raw-overlay
/// paths that serve a pattern the persisted index cannot contribute to. The
/// input should include both cursor output flakes (asserts) and raw overlay
/// flakes (asserts/retracts).
///
/// ## Why the grouping is two-level
///
/// A fact's identity is `(s, p, o, dt, m)`: two `@list` entries holding the
/// same value at different positions, and one lexical form under two language
/// tags, are DISTINCT facts (the #1273 class). But every comparator orders
/// `… t, op, m` — `t` BEFORE `m` (`fluree-db-core/src/comparator.rs`). So
/// flakes that share `(s, p, o, dt)` and differ only in `m` interleave by `t`,
/// which puts a sibling's assert between a fact's own assert and its
/// retraction. Cutting runs on full identity therefore closes a group early
/// and emits the retracted assert as live — the exact resurrection this
/// helper exists to prevent, on `["a", "b", "a"]` or `"x"@en` + `"x"@fr`.
///
/// So runs are cut on [`same_fact_key`] — the four fields all four comparators
/// DO order before `t`, making those runs contiguous — and the winner is
/// picked per distinct `m` inside the run.
fn resolve_latest_ops_keep_asserts(mut flakes: Vec<Flake>, index: IndexType) -> Vec<Flake> {
    let cmp = index.comparator();
    flakes.sort_by(cmp);

    if flakes.len() < 2 {
        return flakes.into_iter().filter(|f| f.op).collect();
    }

    let mut out: Vec<Flake> = Vec::with_capacity(flakes.len());
    // Scratch index buffer for the multi-flake run path, reused across runs.
    let mut idxs: Vec<usize> = Vec::new();
    let mut start = 0usize;
    while start < flakes.len() {
        let mut end = start + 1;
        while end < flakes.len() && same_fact_key(&flakes[start], &flakes[end]) {
            end += 1;
        }

        // Overwhelmingly the common run: one flake, so no grouping at all.
        if end - start == 1 {
            if flakes[start].op {
                out.push(flakes[start].clone());
            }
            start = end;
            continue;
        }

        // Group the run by `m`. Sorting rather than scanning a winners list
        // keeps this O(k log k): a list that repeats one value across k
        // positions puts all k in ONE run with k distinct `m`, and a linear
        // "have I seen this `m`" probe would make that quadratic — the shape
        // this whole area of the code exists to have stopped doing.
        //
        // The sort keys on `m` ALONE and is stable, so the comparator's
        // `t, op` order survives inside each group and the winner scan below
        // sees candidates in ascending `t`.
        idxs.clear();
        idxs.extend(start..end);
        idxs.sort_by(|&a, &b| flakes[a].m.cmp(&flakes[b].m));

        let mut g = 0usize;
        while g < idxs.len() {
            let mut best = idxs[g];
            let mut h = g + 1;
            while h < idxs.len() && flakes[idxs[h]].m == flakes[best].m {
                let cand = &flakes[idxs[h]];
                let cur = &flakes[best];
                // Newest op wins; at equal `t` a retraction beats an assert,
                // matching `resolve_current_flakes`.
                if cand.t > cur.t || (cand.t == cur.t && !cand.op && cur.op) {
                    best = idxs[h];
                }
                h += 1;
            }
            if flakes[best].op {
                out.push(flakes[best].clone());
            }
            g = h;
        }

        start = end;
    }

    // Survivors of a multi-flake run are emitted in `m` order, which is not
    // the comparator's order for the run (it orders `t` first). Callers take
    // this as a range result and expect index order, which the
    // single-winner-per-group predecessor gave them for free. Cheap to
    // restore: `out` is a subsequence of an already-sorted vec whose only
    // inversions are inside those runs.
    out.sort_by(cmp);
    out
}

/// Run key for [`resolve_latest_ops_keep_asserts`]: the four fields every
/// comparator orders BEFORE `t`, so flakes sharing it land contiguously after
/// the sort. Deliberately NOT full fact identity — `m` is resolved per-fact
/// inside the run, because the comparators interleave differing `m` by `t`.
///
/// `g` is absent because every caller walks one `g_id` at a time, so a
/// mixed-graph vector never reaches here. A caller that ever scopes
/// differently has to add it.
#[inline]
fn same_fact_key(a: &Flake, b: &Flake) -> bool {
    a.s == b.s && a.p == b.p && a.o == b.o && a.dt == b.dt
}

/// Batched lookup for ref-valued predicate objects across many subjects (V3).
///
/// For a fixed predicate, scans PSOT within the `[min_s_id, max_s_id]` range,
/// filters to the requested subject set, and returns only IRI-ref-typed objects.
/// Used by policy (`rdf:type` lookups) and stats refresh.
#[allow(clippy::too_many_arguments)]
fn binary_lookup_subject_predicate_refs_batched_v3(
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    runtime_small_dicts: &Arc<RuntimeSmallDicts>,
    g_id: GraphId,
    index: IndexType,
    predicate: &Sid,
    subjects: &[Sid],
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<HashMap<Sid, Vec<Sid>>> {
    if index != IndexType::Psot {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "V3 batched predicate+subject lookup currently supports PSOT only",
        ));
    }

    if subjects.is_empty() {
        return Ok(HashMap::new());
    }

    let view =
        BinaryGraphView::with_novelty(Arc::clone(store), g_id, Some(Arc::clone(dict_novelty)));

    // Resolve predicate.
    let p_id = match store.sid_to_p_id(predicate) {
        Some(id) => id,
        None => return Ok(HashMap::new()), // unknown predicate → no results
    };

    // Translate subjects to s_id and build s_id → Sid map.
    let mut s_ids: Vec<u64> = Vec::with_capacity(subjects.len());
    let mut s_id_to_sid: HashMap<u64, Sid> = HashMap::with_capacity(subjects.len());
    for sid in subjects {
        if let Ok(Some(s_id)) = store.find_subject_id_by_parts(sid.namespace_code, &sid.name) {
            s_id_to_sid.entry(s_id).or_insert_with(|| sid.clone());
            s_ids.push(s_id);
        } else if dict_novelty.is_initialized() {
            // Try DictNovelty for uncommitted subjects.
            if let Some(s_id) = dict_novelty
                .subjects
                .find_subject(sid.namespace_code, &sid.name)
            {
                s_id_to_sid.entry(s_id).or_insert_with(|| sid.clone());
                s_ids.push(s_id);
            }
        }
    }
    if s_ids.is_empty() {
        return Ok(HashMap::new());
    }
    s_ids.sort_unstable();
    s_ids.dedup();

    let min_s_id = s_ids[0];
    let max_s_id = *s_ids.last().unwrap();

    // PSOT key bounds: restrict to [min_s_id, max_s_id] within this predicate.
    let min_key = RunRecordV2 {
        s_id: SubjectId::from_u64(min_s_id),
        o_key: 0,
        p_id,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId::from_u64(max_s_id),
        o_key: u64::MAX,
        p_id,
        t: 0,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };

    // Get branch manifest.
    let branch = match store.branch_for_order(g_id, RunSortOrder::Psot) {
        Some(b) => Arc::clone(b),
        None => {
            // No PSOT branch — try overlay only.
            return batched_refs_overlay_only(
                store,
                dict_novelty,
                g_id,
                predicate,
                subjects,
                opts,
                overlay,
            );
        }
    };

    let filter = BinaryFilter {
        p_id: Some(p_id),
        ..Default::default()
    };

    let projection = ColumnProjection::all();
    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        RunSortOrder::Psot,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );

    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    cursor.set_to_t(effective_to_t);

    // Overlay merge — pre-filter to avoid translating irrelevant flakes.
    // Only translate flakes that match the target predicate and subject set.
    let subject_sid_set: HashSet<&Sid> = subjects.iter().collect();
    let OverlayTranslateV3Result {
        mut ops,
        raw: raw_overlay,
        ..
    } = translate_overlay_ops_v3_with_raw(
        overlay,
        g_id,
        IndexType::Psot,
        effective_to_t,
        store,
        dict_novelty,
        runtime_small_dicts,
        |flake| flake.p == *predicate && subject_sid_set.contains(&flake.s),
        "V3 batched refs",
    );

    if !ops.is_empty() {
        fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, RunSortOrder::Psot);
        fluree_db_binary_index::read::types::resolve_overlay_ops(&mut ops);
        cursor.set_overlay_ops(ops.into());
    }

    // Membership filter for s_id (fast O(1)).
    let s_id_set: HashSet<u64> = s_ids.into_iter().collect();
    let iri_ref_o_type = OType::IRI_REF.as_u16();

    let mut out: HashMap<Sid, Vec<Sid>> = HashMap::new();

    while let Some(batch) = cursor.next_batch()? {
        for i in 0..batch.row_count {
            let s_id = batch.s_id.get(i);
            if !s_id_set.contains(&s_id) {
                continue;
            }

            let o_type = batch.o_type.get_or(i, 0);
            if o_type != iri_ref_o_type {
                continue;
            }

            let o_key = batch.o_key.get(i);

            // Subject Sid: prefer the original input Sid.
            let subj_sid = match s_id_to_sid.get(&s_id) {
                Some(s) => s.clone(),
                None => resolve_sid(s_id, &view)?,
            };

            // Resolve object (IRI ref) to Sid.
            let class_sid = resolve_sid(o_key, &view)?;

            out.entry(subj_sid).or_default().push(class_sid);
        }
    }

    // Correctness fallback for overlay translation failures: apply raw overlay deltas now.
    if !raw_overlay.is_empty() {
        apply_raw_overlay_deltas_to_batched_refs(&mut out, &raw_overlay, predicate, effective_to_t);
    }

    // Dedup class vectors per subject for stable policy semantics.
    for classes in out.values_mut() {
        classes.sort();
        classes.dedup();
    }

    Ok(out)
}

/// Apply raw overlay deltas to the batched refs output map.
///
/// Ensures correctness when V3 overlay translation fails by applying the latest-op-wins
/// semantics for `(subject, predicate, class)` facts using raw flakes.
fn apply_raw_overlay_deltas_to_batched_refs(
    out: &mut HashMap<Sid, Vec<Sid>>,
    raw_overlay: &[Flake],
    predicate: &Sid,
    to_t: i64,
) {
    use std::collections::HashMap as StdHashMap;

    // Map: subject -> class -> (t, op)
    let mut latest: StdHashMap<&Sid, StdHashMap<&Sid, (i64, bool)>> = StdHashMap::new();

    for flake in raw_overlay {
        if flake.t > to_t {
            continue;
        }
        if flake.p != *predicate {
            continue;
        }
        let FlakeValue::Ref(ref class_sid) = flake.o else {
            continue;
        };

        let subj_entry = latest.entry(&flake.s).or_default();
        match subj_entry.get(class_sid) {
            None => {
                subj_entry.insert(class_sid, (flake.t, flake.op));
            }
            Some(&(t0, _op0)) => {
                if flake.t > t0 {
                    subj_entry.insert(class_sid, (flake.t, flake.op));
                }
            }
        }
    }

    // Apply: latest assert adds, latest retract removes.
    for (subj, classes) in latest {
        let vec = out.entry(subj.clone()).or_default();
        for (class_sid, (_t, op)) in classes {
            if op {
                vec.push(class_sid.clone());
            } else {
                vec.retain(|c| c != class_sid);
            }
        }
    }
}

/// Overlay-only fallback for batched ref lookup when no PSOT branch exists.
///
/// The overlay is a log: an `rdf:type` asserted and then retracted inside the
/// same novelty window has both flakes here. Filtering the walk to asserts
/// would report a class that was revoked — and since this function's caller
/// serves policy `rdf:type` lookups, that means a class-based grant surviving
/// its own revocation until the first index build. So retractions are kept and
/// the merged set goes through
/// [`apply_raw_overlay_deltas_to_batched_refs`] — the same latest-op-wins rule
/// this function's two sibling paths already apply, rather than a third
/// variant of it.
#[allow(clippy::too_many_arguments)]
fn batched_refs_overlay_only(
    store: &Arc<BinaryIndexStore>,
    _dict_novelty: &Arc<DictNovelty>,
    g_id: GraphId,
    predicate: &Sid,
    subjects: &[Sid],
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<HashMap<Sid, Vec<Sid>>> {
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    let subject_set: HashSet<&Sid> = subjects.iter().collect();

    // Gate on the two components that narrow the walk; the delta helper
    // applies the `to_t` and ref-object filters itself.
    let mut raw_overlay: Vec<Flake> = Vec::new();
    overlay.for_each_overlay_flake(
        g_id,
        IndexType::Psot,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            if flake.p == *predicate && subject_set.contains(&flake.s) {
                raw_overlay.push(flake.clone());
            }
        },
    );

    let mut out: HashMap<Sid, Vec<Sid>> = HashMap::new();
    apply_raw_overlay_deltas_to_batched_refs(&mut out, &raw_overlay, predicate, effective_to_t);

    for classes in out.values_mut() {
        classes.sort();
        classes.dedup();
    }

    Ok(out)
}

/// Bounded range query: scan between `start_bound` and `end_bound` in index order.
///
/// Used for subject-range queries (e.g., SHA prefix scans in `time_resolve`).
/// Currently only supports SPOT index order.
///
/// Since subject s_ids are NOT in IRI lexicographic order (they're assigned in
/// first-seen/insertion order), we cannot simply create a bounded SPOT cursor
/// between two s_ids. Instead, we:
/// 1. Use the reverse subject tree to find all persisted subjects whose suffix
///    falls in the [start_name, end_name) range within the namespace.
/// 2. Also collect overlay subjects matching the prefix (so novelty-only subjects
///    are not dropped when persisted matches exist).
/// 3. Build a HashSet of matching s_ids, create a SPOT cursor bounded to
///    [min_s_id, max_s_id] for leaf selection, then post-filter rows.
#[allow(clippy::too_many_arguments)]
fn binary_range_bounded_v3(
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    runtime_small_dicts: &Arc<RuntimeSmallDicts>,
    namespace_codes_fallback: &Option<Arc<HashMap<u16, String>>>,
    g_id: GraphId,
    index: IndexType,
    start_bound: &Flake,
    end_bound: &Flake,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<Vec<Flake>> {
    // Guard: range_bounded is designed for SPOT subject-prefix scans.
    if index != IndexType::Spot {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("V3 range_bounded: only SPOT is supported, got {index:?}"),
        ));
    }

    let order = index_type_to_sort_order(index);
    let ns_code = start_bound.s.namespace_code;
    let start_name: &str = &start_bound.s.name;
    let end_name: &str = &end_bound.s.name;
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());

    // Step 1: Find persisted subjects in the IRI prefix range via reverse tree.
    let matching_s_ids = store.find_subjects_by_prefix(ns_code, start_name)?;
    let mut s_id_set: HashSet<u64> = matching_s_ids.into_iter().collect();

    // Step 2: Translate overlay flakes and collect novelty-only subject s_ids
    // that match the prefix range. This ensures uncommitted subjects aren't
    // dropped when persisted matches also exist.
    let OverlayTranslateV3Result {
        ops: mut overlay_ops,
        raw: raw_overlay,
        ephemeral_p_id_to_sid,
        ..
    } = translate_overlay_ops_v3_with_raw(
        overlay,
        g_id,
        index,
        effective_to_t,
        store,
        dict_novelty,
        runtime_small_dicts,
        |flake| {
            if flake.s.namespace_code != ns_code {
                return false;
            }
            let name: &str = &flake.s.name;
            !(name < start_name || name >= end_name)
        },
        "V3 range_bounded",
    );

    // Add overlay subject s_ids from successfully-translated ops.
    for op in &overlay_ops {
        s_id_set.insert(op.s_id);
    }
    // Add overlay subject s_ids from raw flakes by resolving subject only.
    for flake in &raw_overlay {
        if flake.s.namespace_code != ns_code {
            continue;
        }
        let name: &str = &flake.s.name;
        if name < start_name || name >= end_name {
            continue;
        }
        if let Some(s_id) = resolve_or_novelty(
            store.find_subject_id_by_parts(flake.s.namespace_code, &flake.s.name)?,
            dict_novelty,
            || {
                dict_novelty
                    .subjects
                    .find_subject(flake.s.namespace_code, &flake.s.name)
            },
        ) {
            s_id_set.insert(s_id);
        }
    }

    if s_id_set.is_empty() {
        // No persisted subjects (and we couldn't resolve overlay subjects to s_id).
        // For correctness, fall back to overlay-only bounded collection + lifecycle resolution.
        return overlay_only_flakes_bounded(
            store,
            g_id,
            index,
            start_bound,
            end_bound,
            opts,
            overlay,
        );
    }

    let branch = match store.branch_for_order(g_id, order) {
        Some(b) => Arc::clone(b),
        None => {
            // No SPOT branch — return overlay-only results (already translated above).
            return overlay_only_flakes_bounded(
                store,
                g_id,
                index,
                start_bound,
                end_bound,
                opts,
                overlay,
            );
        }
    };

    // Compute s_id bounds for leaf selection (narrows the leaf range).
    let min_s_id = *s_id_set.iter().min().unwrap();
    let max_s_id = *s_id_set.iter().max().unwrap();

    let min_key = RunRecordV2 {
        s_id: SubjectId::from_u64(min_s_id),
        o_key: 0,
        p_id: 0,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId::from_u64(max_s_id),
        o_key: u64::MAX,
        p_id: u32::MAX,
        t: 0,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };

    let filter = BinaryFilter::default();
    let projection = ColumnProjection::all();
    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        order,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );

    cursor.set_to_t(effective_to_t);

    // Attach pre-translated overlay ops (even if some translation failed).
    if !overlay_ops.is_empty() {
        fluree_db_binary_index::read::types::sort_overlay_ops(&mut overlay_ops, order);
        fluree_db_binary_index::read::types::resolve_overlay_ops(&mut overlay_ops);
        cursor.set_overlay_ops(overlay_ops.into());
    }

    let view =
        BinaryGraphView::with_novelty(Arc::clone(store), g_id, Some(Arc::clone(dict_novelty)))
            .with_namespace_codes_fallback(namespace_codes_fallback.clone());
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);
    let mut flakes = Vec::new();
    let mut skipped = 0usize;

    let has_raw_overlay = !raw_overlay.is_empty();
    while let Some(batch) = cursor.next_batch()? {
        for i in 0..batch.row_count {
            let s_id = batch.s_id.get(i);

            // Post-filter: only accept rows for subjects in our prefix range.
            if !s_id_set.contains(&s_id) {
                continue;
            }

            let p_id = batch.p_id.get_or(i, 0);
            let o_type = batch.o_type.get_or(i, 0);
            let o_key = batch.o_key.get(i);
            let t = batch.t.get_or(i, 0) as i64;
            let o_i = batch.o_i.get_or(i, u32::MAX);

            let s_sid = resolve_sid(s_id, &view)?;

            // Double-check the subject name is in [start_name, end_name).
            if s_sid.namespace_code == ns_code {
                let name: &str = &s_sid.name;
                if name < start_name || name >= end_name {
                    continue;
                }
            }

            // Resolve predicate: persisted table first, then ephemeral overlay map.
            let p_sid = match store.p_sid_table().get(p_id as usize) {
                Some(sid) => sid.clone(),
                None => match ephemeral_p_id_to_sid.get(&p_id) {
                    Some(sid) => sid.clone(),
                    None => continue, // truly unknown — shouldn't happen
                },
            };
            let o_val = view.decode_value(o_type, o_key, p_id)?;
            let dt = store
                .resolve_datatype_sid_for_value(o_type, &o_val)
                .unwrap_or_else(|| Sid::new(0, ""));
            let lang = store
                .resolve_lang_tag(o_type)
                .map(std::string::ToString::to_string);
            let meta = if lang.is_some() || o_i != u32::MAX {
                Some(fluree_db_core::FlakeMeta {
                    lang,
                    i: if o_i != u32::MAX {
                        Some(o_i as i32)
                    } else {
                        None
                    },
                })
            } else {
                None
            };

            let flake = Flake {
                g: None,
                s: s_sid,
                p: p_sid,
                o: o_val,
                dt,
                t,
                op: true,
                m: meta,
            };

            if has_raw_overlay {
                flakes.push(flake);
                continue;
            }

            if let Some(bounds) = &opts.object_bounds {
                if !bounds.matches(&flake.o) {
                    continue;
                }
            }

            if skipped < offset {
                skipped += 1;
                continue;
            }

            flakes.push(flake);
            if flakes.len() >= limit {
                return Ok(flakes);
            }
        }
    }

    if !has_raw_overlay {
        return Ok(flakes);
    }

    // Correctness fallback: merge raw overlay flakes, resolve lifecycles, then apply options.
    flakes.extend(raw_overlay);
    let mut resolved = resolve_latest_ops_keep_asserts(flakes, IndexType::Spot);

    // Re-apply subject bounds: start_bound.s <= s < end_bound.s.
    resolved.retain(|f| f.s >= start_bound.s && f.s < end_bound.s);

    if let Some(bounds) = &opts.object_bounds {
        resolved.retain(|f| bounds.matches(&f.o));
    }
    if offset > 0 && !resolved.is_empty() {
        let n = offset.min(resolved.len());
        resolved.drain(0..n);
    }
    if resolved.len() > limit {
        resolved.truncate(limit);
    }

    Ok(resolved)
}

/// Overlay-only path for range_bounded when no branch exists.
#[allow(clippy::too_many_arguments)]
fn overlay_only_flakes_bounded(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    index: IndexType,
    start_bound: &Flake,
    end_bound: &Flake,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<Vec<Flake>> {
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);

    let mut flakes = Vec::new();

    overlay.for_each_overlay_flake(
        g_id,
        index,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            // Check subject bounds: start_bound.s <= flake.s < end_bound.s.
            if flake.s < start_bound.s || flake.s >= end_bound.s {
                return;
            }

            // Keep both asserts and retracts; resolve lifecycles after collection.
            flakes.push(flake.clone());
        },
    );

    // Resolve lifecycles (latest op wins) and drop retracts.
    let mut resolved = resolve_latest_ops_keep_asserts(flakes, index);

    // Apply options after lifecycle resolution.
    if let Some(ref bounds) = opts.object_bounds {
        resolved.retain(|f| bounds.matches(&f.o));
    }
    if offset > 0 && !resolved.is_empty() {
        let n = offset.min(resolved.len());
        resolved.drain(0..n);
    }
    if resolved.len() > limit {
        resolved.truncate(limit);
    }

    Ok(resolved)
}

/// Overlay-only results when the persisted index cannot contribute a row.
///
/// Reached whenever a bound component of `match_val` has no persisted id
/// (a novelty-only subject, predicate, or object), or no branch manifest
/// exists for the requested order (genesis / pre-first-index).
///
/// The overlay is a **log**, not a current-state view: an assert at `t=2`
/// and its retraction at `t=3` both sit in novelty. So retractions are kept
/// through the walk and the whole matching set is lifecycle-resolved
/// (newest op per fact identity wins, retracted keys drop out) before
/// options are applied — the same rule the cursor path gets from
/// `resolve_overlay_ops` and the bounded twin gets from
/// `resolve_latest_ops_keep_asserts`. Filtering to `op == true` inside the
/// walk instead silently resurrects every fact whose assert AND retract
/// both live in novelty.
///
/// This also rules out an early exit at `limit`: the retraction that
/// cancels an already-collected assert can arrive after the limit is
/// reached. `for_each_overlay_flake` walks the graph's whole overlay
/// regardless, so the exit only ever saved clones of matching flakes.
fn overlay_only_flakes(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    index: IndexType,
    match_val: &RangeMatch,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<Vec<fluree_db_core::Flake>> {
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);

    let mut flakes = Vec::new();

    overlay.for_each_overlay_flake(
        g_id,
        index,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            // Filter by match components. A retraction carries the same
            // subject/predicate/object as the assert it cancels, so these
            // gates keep both halves of a fact together.
            if let Some(ref s_sid) = match_val.s {
                if flake.s != *s_sid {
                    return;
                }
            }
            if let Some(ref p_sid) = match_val.p {
                if flake.p != *p_sid {
                    return;
                }
            }
            if let Some(ref o_val) = match_val.o {
                if flake.o != *o_val {
                    return;
                }
            }

            // Projection-predicate allow-list (parity with the indexed path).
            // Applied here for novelty-only subjects that bypass the cursor
            // loop above (subject/predicate/object Sid unresolvable in the
            // persisted dict, or no branch manifest for this order).
            if let Some(ref allow) = opts.predicate_filter {
                if !allow.iter().any(|p| p == &flake.p) {
                    return;
                }
            }

            // Keep both asserts and retracts; resolve lifecycles below.
            flakes.push(flake.clone());
        },
    );

    let mut resolved = resolve_latest_ops_keep_asserts(flakes, index);

    // Options apply to the resolved current state, not to the log.
    if let Some(ref bounds) = opts.object_bounds {
        resolved.retain(|f| bounds.matches(&f.o));
    }
    if offset > 0 && !resolved.is_empty() {
        let n = offset.min(resolved.len());
        resolved.drain(0..n);
    }
    if resolved.len() > limit {
        resolved.truncate(limit);
    }

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::FlakeMeta;

    fn s(name: &str) -> Sid {
        Sid::new(100, name)
    }

    fn dt_string() -> Sid {
        Sid::new(fluree_vocab::namespaces::XSD, "string")
    }

    /// One flake on `ex:sub ex:pred`, with `m` supplied directly.
    fn f(o: &str, t: i64, op: bool, m: Option<FlakeMeta>) -> Flake {
        Flake::new(
            s("sub"),
            s("pred"),
            FlakeValue::String(o.to_string()),
            dt_string(),
            t,
            op,
            m,
        )
    }

    fn at(i: i32) -> Option<FlakeMeta> {
        FlakeMeta::from_parts(None, Some(i))
    }

    fn tagged(lang: &str) -> Option<FlakeMeta> {
        FlakeMeta::from_parts(Some(lang), None)
    }

    fn rendered(flakes: &[Flake]) -> Vec<(String, Option<FlakeMeta>)> {
        flakes
            .iter()
            .map(|f| {
                let o = match &f.o {
                    FlakeValue::String(v) => v.clone(),
                    other => format!("{other:?}"),
                };
                (o, f.m.clone())
            })
            .collect()
    }

    /// The plain case the helper always handled: one fact, asserted then
    /// retracted, resolves away.
    #[test]
    fn single_valued_retraction_resolves() {
        let out = resolve_latest_ops_keep_asserts(
            vec![f("a", 2, true, None), f("a", 3, false, None)],
            IndexType::Spot,
        );
        assert!(out.is_empty());
    }

    /// A `@list` that repeats a value: `["a", "b", "a"]` with position 0
    /// deleted. The comparators order `t` before `m`, so the sibling `a@2`
    /// assert sits between `a@0`'s assert and its retraction — cutting runs
    /// on full identity closed the group early and emitted the retracted
    /// `a@0` as live. Position 0 is the load-bearing case: deleting position
    /// 2 passed even with the bug, because it only fails when the retracted
    /// entry's metadata sorts below a surviving sibling's.
    #[test]
    fn repeated_list_value_retracts_the_right_position() {
        let out = resolve_latest_ops_keep_asserts(
            vec![
                f("a", 2, true, at(0)),
                f("b", 2, true, at(1)),
                f("a", 2, true, at(2)),
                f("a", 3, false, at(0)),
            ],
            IndexType::Spot,
        );
        assert_eq!(
            rendered(&out),
            vec![("a".to_string(), at(2)), ("b".to_string(), at(1))],
            "only the position-0 entry may go"
        );
    }

    /// The mirror case that used to pass, kept so a future regrouping can't
    /// fix one position by breaking the other.
    #[test]
    fn repeated_list_value_retracts_the_last_position() {
        let out = resolve_latest_ops_keep_asserts(
            vec![
                f("a", 2, true, at(0)),
                f("b", 2, true, at(1)),
                f("a", 2, true, at(2)),
                f("a", 3, false, at(2)),
            ],
            IndexType::Spot,
        );
        assert_eq!(
            rendered(&out),
            vec![("a".to_string(), at(0)), ("b".to_string(), at(1))]
        );
    }

    /// One lexical form under two language tags: distinct facts (#1273), so
    /// retracting the `@en` must leave the `@fr` and take nothing else.
    #[test]
    fn language_tagged_siblings_retract_independently() {
        let out = resolve_latest_ops_keep_asserts(
            vec![
                f("hello", 2, true, tagged("en")),
                f("hello", 2, true, tagged("fr")),
                f("hello", 3, false, tagged("en")),
            ],
            IndexType::Spot,
        );
        assert_eq!(rendered(&out), vec![("hello".to_string(), tagged("fr"))]);
    }

    /// A position retracted and re-asserted must come back, and the winner is
    /// the newest op for that `m` alone — not the newest in the whole run.
    #[test]
    fn reasserted_position_survives_a_sibling_with_a_later_t() {
        let out = resolve_latest_ops_keep_asserts(
            vec![
                f("a", 2, true, at(0)),
                f("a", 3, false, at(0)),
                f("a", 4, true, at(0)),
                f("a", 5, true, at(1)),
            ],
            IndexType::Spot,
        );
        assert_eq!(
            rendered(&out),
            vec![("a".to_string(), at(0)), ("a".to_string(), at(1))]
        );
    }

    /// At equal `t` a retraction beats an assert, per `resolve_current_flakes`.
    #[test]
    fn retraction_wins_at_equal_t() {
        let out = resolve_latest_ops_keep_asserts(
            vec![f("a", 2, true, at(0)), f("a", 2, false, at(0))],
            IndexType::Spot,
        );
        assert!(out.is_empty());
    }

    /// Output must stay in comparator order: callers take it as a range
    /// result. Winners are collected in `t` order inside a run, so a run
    /// whose survivors' `t` order disagrees with their `m` order would emit
    /// unsorted without the final sort.
    #[test]
    fn output_is_comparator_ordered() {
        let out = resolve_latest_ops_keep_asserts(
            vec![
                // `m` ascending but `t` descending across the two survivors.
                f("a", 9, true, at(0)),
                f("a", 3, true, at(1)),
            ],
            IndexType::Spot,
        );
        let cmp = IndexType::Spot.comparator();
        assert!(
            out.windows(2)
                .all(|w| cmp(&w[0], &w[1]) != std::cmp::Ordering::Greater),
            "resolved range results must be index-ordered, got {:?}",
            rendered(&out)
        );
    }

    /// A list holding ONE value at many positions puts every entry in a
    /// single `(s, p, o, dt)` run with a distinct `m` each — the worst case
    /// for the per-`m` grouping, and the shape a "have I seen this `m`"
    /// linear probe would turn quadratic. Sized so a quadratic grouper would
    /// be conspicuous rather than merely slower.
    #[test]
    fn many_positions_of_one_value_resolve_without_a_quadratic() {
        const K: i32 = 4_000;
        let mut flakes: Vec<Flake> = (0..K).map(|i| f("x", 2, true, at(i))).collect();
        // Retract every third position.
        flakes.extend(
            (0..K)
                .filter(|i| i % 3 == 0)
                .map(|i| f("x", 3, false, at(i))),
        );

        let started = std::time::Instant::now();
        let out = resolve_latest_ops_keep_asserts(flakes, IndexType::Spot);
        let elapsed = started.elapsed();

        let expected = (0..K).filter(|i| i % 3 != 0).count();
        assert_eq!(
            out.len(),
            expected,
            "exactly the unretracted positions live"
        );
        let survivors: Vec<i32> = out.iter().filter_map(|f| f.m.as_ref()?.i).collect();
        assert!(
            survivors.iter().all(|i| i % 3 != 0),
            "no retracted position may survive"
        );
        assert!(
            survivors.windows(2).all(|w| w[0] < w[1]),
            "survivors stay in index order"
        );
        // Generous by ~2 orders of magnitude against the real runtime, so it
        // fails on a reintroduced quadratic without flaking on a slow box.
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "resolution took {elapsed:?} for {K} positions — grouping went superlinear"
        );
    }

    /// The latest-op-wins rule `batched_refs_overlay_only` delegates to.
    ///
    /// Its overlay walk is a log, so an `rdf:type` asserted and retracted in
    /// the same novelty window arrives as both flakes. Filtering to asserts
    /// (what that function used to do) reports a revoked class — and its
    /// caller serves policy `rdf:type` lookups, so a class-based grant would
    /// outlive its own revocation until the first index build.
    #[test]
    fn batched_ref_deltas_drop_a_retracted_class() {
        let rdf_type = s("type");
        let type_flake = |subject: &str, class: &str, t: i64, op: bool| {
            Flake::new(
                s(subject),
                rdf_type.clone(),
                FlakeValue::Ref(s(class)),
                Sid::new(0, ""),
                t,
                op,
                None,
            )
        };

        let mut out: HashMap<Sid, Vec<Sid>> = HashMap::new();
        apply_raw_overlay_deltas_to_batched_refs(
            &mut out,
            &[
                // Revoked in the same window.
                type_flake("alice", "Admin", 2, true),
                type_flake("alice", "Admin", 3, false),
                // Still held.
                type_flake("alice", "Person", 2, true),
                // Revoked, then granted again.
                type_flake("bob", "Admin", 2, true),
                type_flake("bob", "Admin", 3, false),
                type_flake("bob", "Admin", 4, true),
            ],
            &rdf_type,
            10,
        );

        let classes = |subject: &str| -> Vec<String> {
            let mut v: Vec<String> = out
                .get(&s(subject))
                .map(|cs| cs.iter().map(|c| c.name_str().to_string()).collect())
                .unwrap_or_default();
            v.sort();
            v
        };
        assert_eq!(
            classes("alice"),
            vec!["Person".to_string()],
            "a revoked class must not be reported"
        );
        assert_eq!(
            classes("bob"),
            vec!["Admin".to_string()],
            "a re-granted class comes back"
        );
    }

    /// Distinct datatypes on the same lexical value are distinct facts, so a
    /// retraction of one must not take the other. Pins that `dt` stays in the
    /// run key rather than being folded in with `m`.
    #[test]
    fn datatype_siblings_retract_independently() {
        let typed = |dt: &str, t: i64, op: bool| {
            Flake::new(
                s("sub"),
                s("pred"),
                FlakeValue::String("1".to_string()),
                Sid::new(fluree_vocab::namespaces::XSD, dt),
                t,
                op,
                None,
            )
        };
        let out = resolve_latest_ops_keep_asserts(
            vec![
                typed("string", 2, true),
                typed("token", 2, true),
                typed("string", 3, false),
            ],
            IndexType::Spot,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].dt.name_str(), "token");
    }
}
