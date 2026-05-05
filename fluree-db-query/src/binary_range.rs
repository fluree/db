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
                tracing::warn!(
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

    // Create cursor: use range-narrowed scan when any filter field is bound,
    // matching the pattern in BinaryScanOperator::open. For novelty-only subjects
    // this yields an empty leaf_range, so the cursor drains overlay ops directly
    // with zero leaf I/O.
    let projection = ColumnProjection::all();
    let use_range = filter.s_id.is_some()
        || filter.p_id.is_some()
        || filter.o_type.is_some()
        || filter.o_key.is_some();

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
        BinaryCursor::new(
            Arc::clone(store),
            order,
            branch,
            &min_key,
            &max_key,
            filter,
            projection,
        )
    } else {
        BinaryCursor::scan_all(Arc::clone(store), order, branch, filter, projection)
    };

    if let Some(t) = tracker {
        cursor = cursor.with_tracker(t.clone());
    }

    // Apply overlay.
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    cursor.set_to_t(effective_to_t);

    // Overlay translation.
    let OverlayTranslateV3Result {
        mut ops,
        raw: untranslated,
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
        |_| true,
        "V3 range",
    );

    if !ops.is_empty() {
        fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, order);
        fluree_db_binary_index::read::types::resolve_overlay_ops(&mut ops);
        let epoch = overlay.epoch();
        cursor.set_overlay_ops(ops);
        cursor.set_epoch(epoch);
    }

    // Iterate and decode to Flakes.
    let has_untranslated = !untranslated.is_empty();
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);
    let mut flakes = Vec::new();
    let mut skipped = 0usize;

    while let Some(batch) = cursor.next_batch()? {
        for i in 0..batch.row_count {
            let s_id = batch.s_id.get(i);
            let p_id = batch.p_id.get_or(i, 0);
            let o_type = batch.o_type.get_or(i, 0);
            let o_key = batch.o_key.get(i);
            let t = batch.t.get_or(i, 0) as i64;
            let o_i = batch.o_i.get_or(i, u32::MAX);

            // Resolve subject.
            let s_sid = resolve_sid(s_id, &view)?;
            // Resolve predicate: persisted dict first, then ephemeral overlay map.
            let p_sid = match store.resolve_predicate_iri(p_id) {
                Some(iri) => store.encode_iri(iri),
                None => match ephemeral_p_id_to_sid.get(&p_id) {
                    Some(sid) => sid.clone(),
                    None => continue, // truly unknown — shouldn't happen
                },
            };
            // Decode object.
            let o_val = view.decode_value(o_type, o_key, p_id)?;
            // Resolve datatype.
            let dt = store
                .resolve_datatype_sid(o_type)
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
/// into V3 `OverlayOp`s (e.g., missing dict novelty). The input should include
/// both cursor output flakes (asserts) and raw overlay flakes (asserts/retracts).
fn resolve_latest_ops_keep_asserts(mut flakes: Vec<Flake>, index: IndexType) -> Vec<Flake> {
    let cmp = index.comparator();
    flakes.sort_by(cmp);

    if flakes.len() < 2 {
        return flakes.into_iter().filter(|f| f.op).collect();
    }

    let mut out: Vec<Flake> = Vec::with_capacity(flakes.len());
    let mut i = 0usize;
    while i < flakes.len() {
        let mut best = i;
        i += 1;

        while i < flakes.len() && same_fact_identity(&flakes[best], &flakes[i]) {
            let cand = &flakes[i];
            let cur = &flakes[best];
            if cand.t > cur.t || (cand.t == cur.t && !cand.op && cur.op) {
                best = i;
            }
            i += 1;
        }

        if flakes[best].op {
            out.push(flakes[best].clone());
        }
    }

    out
}

#[inline]
fn same_fact_identity(a: &Flake, b: &Flake) -> bool {
    a.s == b.s && a.p == b.p && a.o == b.o && a.dt == b.dt && a.m == b.m
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
        cursor.set_overlay_ops(ops);
        cursor.set_epoch(overlay.epoch());
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

    let mut out: HashMap<Sid, Vec<Sid>> = HashMap::new();

    overlay.for_each_overlay_flake(
        g_id,
        IndexType::Psot,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            if !flake.op {
                return;
            }
            if !subject_set.contains(&flake.s) {
                return;
            }
            if flake.p != *predicate {
                return;
            }
            // No translation needed: we can inspect the FlakeValue directly.
            // Only include IRI-ref object values.
            if let FlakeValue::Ref(ref class_sid) = flake.o {
                out.entry(flake.s.clone())
                    .or_default()
                    .push(class_sid.clone());
            }
        },
    );

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
        cursor.set_overlay_ops(overlay_ops);
        cursor.set_epoch(overlay.epoch());
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

            // Resolve predicate: persisted dict first, then ephemeral overlay map.
            let p_sid = match store.resolve_predicate_iri(p_id) {
                Some(iri) => store.encode_iri(iri),
                None => match ephemeral_p_id_to_sid.get(&p_id) {
                    Some(sid) => sid.clone(),
                    None => continue, // truly unknown — shouldn't happen
                },
            };
            let o_val = view.decode_value(o_type, o_key, p_id)?;
            let dt = store
                .resolve_datatype_sid(o_type)
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

/// Overlay-only results when no branch exists for the requested order.
///
/// Collects flakes directly from the overlay provider, applies match filtering
/// and options (offset/limit). Used at genesis or before first indexing when
/// no persisted branch exists for the requested sort order.
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

    // Use Cell for early-exit: once we've collected offset+limit, stop cloning.
    let mut skipped = 0usize;
    let mut collected = 0usize;
    let mut flakes = Vec::new();

    overlay.for_each_overlay_flake(
        g_id,
        index,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            // Early exit: already have enough results.
            if collected >= limit {
                return;
            }

            // Only include asserts (op=true).
            if !flake.op {
                return;
            }

            // Filter by match components.
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

            // Apply object bounds (same as persisted path).
            if let Some(ref bounds) = opts.object_bounds {
                if !bounds.matches(&flake.o) {
                    return;
                }
            }

            // Apply offset.
            if skipped < offset {
                skipped += 1;
                return;
            }

            flakes.push(flake.clone());
            collected += 1;
        },
    );

    Ok(flakes)
}
