//! Fast-path: constant-object star constraints + numeric filter + label ORDER BY + LIMIT.
//!
//! Targets common benchmark shapes like:
//! `SELECT DISTINCT ?s ?label WHERE { ?s p1 o1 . ?s p2 o2 . ?s pN oN . ?s pNum ?v FILTER(?v > k) . ?s pLabel ?label } ORDER BY ?label LIMIT k`
//!
//! Implementation strategy (single-ledger, binary-index, HEAD-only):
//! - Build candidate subjects by intersecting OPST subject lists for each constant ref constraint `(?s p oRef)`.
//! - Apply numeric existence filter `(?s pNum ?v FILTER(?v > k))` by scanning PSOT(pNum) only over the candidate subject ranges.
//! - Fetch labels from PSOT(pLabel) over the surviving subjects, deduplicate `(s,label)` pairs, sort by label, and take LIMIT.
//!
//! This avoids building/ordering large intermediate solution sets in the general pipeline.

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    empty_batch, fast_path_store, intersect_many_sorted, ref_to_p_id, term_to_ref_s_id,
};
use crate::operator::BoxedOperator;
use crate::triple::{Ref, Term};
use crate::var_registry::VarId;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::read::column_types::{BinaryFilter, ColumnProjection, ColumnSet};
use fluree_db_binary_index::{BinaryCursor, BinaryIndexStore};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId};
use fluree_vocab::xsd;
use rustc_hash::FxHashSet;
use std::sync::Arc;

/// Build a fused fast-path operator for the detected star-constraint query.
#[allow(clippy::too_many_arguments)]
pub fn star_const_ordered_limit_operator(
    subject_var: VarId,
    label_var: VarId,
    label_pred: Ref,
    const_ref_constraints: Vec<(Ref, Term)>,
    numeric_pred: Ref,
    numeric_threshold: FlakeValue,
    limit: usize,
    fallback: Option<BoxedOperator>,
) -> BoxedOperator {
    let schema: Arc<[VarId]> = Arc::from(vec![subject_var, label_var].into_boxed_slice());
    Box::new(crate::fast_path_common::FastPathOperator::with_schema(
        Arc::clone(&schema),
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let g_id: GraphId = ctx.binary_g_id;

            // Resolve predicate IDs and object subject IDs via ctx.decode_sid() (stable across dict rebuilds).
            let label_p_id = ref_to_p_id(ctx, store.as_ref(), &label_pred)?;
            let numeric_p_id = ref_to_p_id(ctx, store.as_ref(), &numeric_pred)?;

            let mut constraint_subject_lists: Vec<Vec<u64>> =
                Vec::with_capacity(const_ref_constraints.len());
            for (pred, obj) in &const_ref_constraints {
                let p_id = ref_to_p_id(ctx, store.as_ref(), pred)?;
                let Some(o_s_id) = term_to_ref_s_id(ctx, store.as_ref(), obj)? else {
                    // Object IRI not in dict → empty result set.
                    return Ok(Some(empty_batch(schema.clone())?));
                };
                let subjects =
                    collect_subjects_for_predicate_object_ref_opst(store, g_id, p_id, o_s_id)?;
                constraint_subject_lists.push(subjects);
            }

            let candidates: Vec<u64> = intersect_many_sorted(constraint_subject_lists);
            if candidates.is_empty() {
                return Ok(Some(empty_batch(schema.clone())?));
            }

            // Apply numeric existence filter: keep subjects with any numeric value satisfying the threshold.
            let filtered_subjects = filter_subjects_by_numeric_gt(
                store,
                g_id,
                numeric_p_id,
                &candidates,
                ctx.to_t,
                &numeric_threshold,
            )?;
            if filtered_subjects.is_empty() {
                return Ok(Some(empty_batch(schema.clone())?));
            }

            // Fetch labels for surviving subjects.
            let pairs = collect_label_pairs(
                store,
                g_id,
                label_p_id,
                &filtered_subjects,
                ctx.to_t,
                ctx.dict_novelty.as_ref(),
            )?;
            if pairs.is_empty() {
                return Ok(Some(empty_batch(schema.clone())?));
            }

            // DISTINCT + ORDER BY label + LIMIT.
            let mut seen: FxHashSet<(u64, Arc<str>, Option<Arc<str>>)> = FxHashSet::default();
            let mut rows: Vec<(Arc<str>, Option<Arc<str>>, u64)> = Vec::with_capacity(pairs.len());
            for (s_id, label, lang) in pairs {
                let label_arc: Arc<str> = Arc::from(label);
                let lang_arc = lang.map(Arc::from);
                if seen.insert((s_id, Arc::clone(&label_arc), lang_arc.clone())) {
                    rows.push((label_arc, lang_arc, s_id));
                }
            }
            let cmp = |a: &(Arc<str>, Option<Arc<str>>, u64),
                       b: &(Arc<str>, Option<Arc<str>>, u64)| {
                a.0.cmp(&b.0)
                    .then_with(|| a.1.as_deref().cmp(&b.1.as_deref()))
                    .then(a.2.cmp(&b.2))
            };
            if limit == 0 {
                rows.clear();
            } else if rows.len() > limit {
                // Partial sort: select top-k without sorting the full set.
                let nth = limit - 1;
                rows.select_nth_unstable_by(nth, cmp);
                rows.truncate(limit);
                rows.sort_by(cmp);
            } else {
                rows.sort_by(cmp);
            }

            let dt_sid = store.as_ref().encode_iri(xsd::STRING);
            let mut col_s: Vec<Binding> = Vec::with_capacity(rows.len());
            let mut col_label: Vec<Binding> = Vec::with_capacity(rows.len());
            for (label, lang, s_id) in rows {
                col_s.push(Binding::EncodedSid { s_id });
                let lit = FlakeValue::String(label.to_string());
                col_label.push(match lang {
                    Some(tag) => Binding::lit_lang(lit, tag),
                    None => Binding::lit(lit, dt_sid.clone()),
                });
            }
            Ok(Some(Batch::new(schema.clone(), vec![col_s, col_label])?))
        },
        fallback,
        "star-const-order-topk",
    ))
}

/// Collect subjects for a single bound-ref object constraint `?s <p_id> <o_s_id>`
/// using the OPST index to avoid scanning the full predicate partition.
fn collect_subjects_for_predicate_object_ref_opst(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    o_s_id: u64,
) -> Result<Vec<u64>> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Opst) else {
        return Ok(Vec::new());
    };
    let branch = Arc::clone(branch);

    // Cursor range: fixed (o_type, o_key, p_id), all o_i and s_id.
    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: o_s_id,
        p_id,
        t: 0,
        o_i: 0,
        o_type: OType::IRI_REF.as_u16(),
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: o_s_id,
        p_id,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: OType::IRI_REF.as_u16(),
        g_id,
    };

    // We want SId output, but we must also load filter columns for row-level filtering.
    let mut out_cols = ColumnSet::EMPTY;
    out_cols.insert(ColumnId::SId);
    let mut internal = ColumnSet::EMPTY;
    internal.insert(ColumnId::PId);
    internal.insert(ColumnId::OType);
    internal.insert(ColumnId::OKey);
    internal.insert(ColumnId::OI);
    let projection = ColumnProjection {
        output: out_cols,
        internal,
    };

    let filter = BinaryFilter {
        p_id: Some(p_id),
        o_type: Some(OType::IRI_REF.as_u16()),
        o_key: Some(o_s_id),
        ..Default::default()
    };

    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        RunSortOrder::Opst,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );

    let mut out: Vec<u64> = Vec::new();
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("binary cursor: {e}")))?
    {
        for i in 0..batch.row_count {
            out.push(batch.s_id.get_or(i, 0));
        }
    }

    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn chunk_subjects(sorted: &[u64], max_span: u64, max_chunk: usize) -> Vec<&[u64]> {
    if sorted.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    for i in 1..sorted.len() {
        let span = sorted[i].saturating_sub(sorted[start]);
        let size = i - start;
        if span > max_span || size >= max_chunk {
            chunks.push(&sorted[start..i]);
            start = i;
        }
    }
    chunks.push(&sorted[start..]);
    chunks
}

fn filter_subjects_by_numeric_gt(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    subjects_sorted: &[u64],
    to_t: i64,
    threshold: &FlakeValue,
) -> Result<Vec<u64>> {
    if subjects_sorted.is_empty() {
        return Ok(Vec::new());
    }
    // Only support numeric thresholds used in benchmark filters.
    let (thr_i, thr_d) = match threshold {
        FlakeValue::Long(n) => (*n, *n as f64),
        FlakeValue::Double(d) => (*d as i64, *d),
        _ => return Ok(Vec::new()),
    };
    let thr_i_key = fluree_db_core::value_id::ObjKey::encode_i64(thr_i).as_u64();
    let thr_d_key = fluree_db_core::value_id::ObjKey::encode_f64(thr_d)
        .map_err(|_| QueryError::execution("cannot encode f64 threshold".to_string()))?
        .as_u64();

    // Caller provides sorted, deduplicated subjects (from intersect_many_sorted).
    let s_id_set: FxHashSet<u64> = subjects_sorted.iter().copied().collect();
    let mut keep: FxHashSet<u64> = FxHashSet::default();

    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(Vec::new());
    };
    let branch = Arc::clone(branch);

    let mut needed = ColumnSet::EMPTY;
    needed.insert(ColumnId::SId);
    needed.insert(ColumnId::OType);
    needed.insert(ColumnId::OKey);
    let projection = ColumnProjection {
        output: needed,
        internal: ColumnSet::EMPTY,
    };

    const MAX_SPAN: u64 = 100_000;
    const MAX_CHUNK: usize = 1000;
    let chunks = chunk_subjects(subjects_sorted, MAX_SPAN, MAX_CHUNK);
    for chunk in chunks {
        let min_s = chunk[0];
        let max_s = *chunk.last().unwrap_or(&min_s);
        let min_key = RunRecordV2 {
            s_id: SubjectId::from_u64(min_s),
            o_key: 0,
            p_id,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId::from_u64(max_s),
            o_key: u64::MAX,
            p_id,
            t: 0,
            o_i: u32::MAX,
            o_type: u16::MAX,
            g_id,
        };
        let filter = BinaryFilter {
            p_id: Some(p_id),
            ..Default::default()
        };
        let mut cursor = BinaryCursor::new(
            Arc::clone(store),
            RunSortOrder::Psot,
            Arc::clone(&branch),
            &min_key,
            &max_key,
            filter,
            projection,
        );
        cursor.set_to_t(to_t);

        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("binary cursor: {e}")))?
        {
            for i in 0..batch.row_count {
                let s_id = batch.s_id.get(i);
                if !s_id_set.contains(&s_id) {
                    continue;
                }
                let ot_u16 = batch.o_type.get_or(i, 0);
                let ot = OType::from_u16(ot_u16);
                let ok = match ot {
                    OType::XSD_INTEGER => batch.o_key.get(i) > thr_i_key,
                    OType::XSD_DOUBLE => batch.o_key.get(i) > thr_d_key,
                    _ => false,
                };
                if ok {
                    keep.insert(s_id);
                }
            }
        }
    }

    let mut out: Vec<u64> = keep.into_iter().collect();
    out.sort_unstable();
    Ok(out)
}

fn collect_label_pairs(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    subjects_sorted: &[u64],
    to_t: i64,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
) -> Result<Vec<(u64, String, Option<String>)>> {
    if subjects_sorted.is_empty() {
        return Ok(Vec::new());
    }
    // Caller provides sorted, deduplicated subjects (from intersect_many_sorted).
    let s_id_set: FxHashSet<u64> = subjects_sorted.iter().copied().collect();

    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(Vec::new());
    };
    let branch = Arc::clone(branch);

    let mut needed = ColumnSet::EMPTY;
    needed.insert(ColumnId::SId);
    needed.insert(ColumnId::OType);
    needed.insert(ColumnId::OKey);
    let projection = ColumnProjection {
        output: needed,
        internal: ColumnSet::EMPTY,
    };

    const MAX_SPAN: u64 = 100_000;
    const MAX_CHUNK: usize = 1000;
    let chunks = chunk_subjects(subjects_sorted, MAX_SPAN, MAX_CHUNK);
    let mut out: Vec<(u64, String, Option<String>)> = Vec::new();

    for chunk in chunks {
        let min_s = chunk[0];
        let max_s = *chunk.last().unwrap_or(&min_s);
        let min_key = RunRecordV2 {
            s_id: SubjectId::from_u64(min_s),
            o_key: 0,
            p_id,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId::from_u64(max_s),
            o_key: u64::MAX,
            p_id,
            t: 0,
            o_i: u32::MAX,
            o_type: u16::MAX,
            g_id,
        };
        let filter = BinaryFilter {
            p_id: Some(p_id),
            ..Default::default()
        };
        let mut cursor = BinaryCursor::new(
            Arc::clone(store),
            RunSortOrder::Psot,
            Arc::clone(&branch),
            &min_key,
            &max_key,
            filter,
            projection,
        );
        cursor.set_to_t(to_t);

        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("binary cursor: {e}")))?
        {
            for i in 0..batch.row_count {
                let s_id = batch.s_id.get(i);
                if !s_id_set.contains(&s_id) {
                    continue;
                }
                let ot_u16 = batch.o_type.get_or(i, 0);
                let ot = OType::from_u16(ot_u16);
                if !matches!(ot, OType::XSD_STRING) && !ot.is_lang_string() {
                    return Err(QueryError::execution(
                        "label fast-path encountered non-string object".to_string(),
                    ));
                }
                let str_id = batch.o_key.get(i) as u32;
                // Watermark-based routing: novel IDs (above watermark) resolve
                // from DictNovelty; persisted IDs delegate to the store.
                let s = if let Some(dn) = dict_novelty.filter(|dn| dn.is_initialized()) {
                    if str_id > dn.strings.watermark() {
                        dn.strings
                            .resolve_string(str_id)
                            .map(std::string::ToString::to_string)
                            .ok_or_else(|| {
                                QueryError::Internal(format!(
                                    "resolve_string_value: string id {str_id} not found in DictNovelty"
                                ))
                            })?
                    } else {
                        store.resolve_string_value(str_id).map_err(|e| {
                            QueryError::Internal(format!("resolve_string_value: {e}"))
                        })?
                    }
                } else {
                    store
                        .resolve_string_value(str_id)
                        .map_err(|e| QueryError::Internal(format!("resolve_string_value: {e}")))?
                };
                let lang = if ot.is_lang_string() {
                    store
                        .resolve_lang_tag(ot_u16)
                        .map(std::string::ToString::to_string)
                } else {
                    None
                };
                out.push((s_id, s, lang));
            }
        }
    }
    Ok(out)
}
