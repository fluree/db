//! Fast-path row-scanning scalar aggregates over a single triple `?s <p> ?o`.
//!
//! `SUM(expr(?o))`, `AVG(?o)`, and `COUNT(DISTINCT ?o)` all answer by scanning
//! the predicate's POST range and folding directly from encoded `(o_type, o_key)`
//! without materializing per-row bindings. They share the scan scaffolding —
//! resolve the predicate, enumerate POST leaflets, skip empty/foreign leaflets —
//! so a single [`scan_predicate_scalar_agg`] driver runs them; each variant keeps
//! its own per-leaflet gating, per-row fold, and finalization in [`AggState`].
//!
//! `MIN`/`MAX` are intentionally NOT here: they read only leaflet directory keys
//! (O(leaflets)); routing them through this per-row cursor would regress them to
//! O(rows). See [`crate::fast_min_max_string`].
//!
//! SPARQL lowering desugars expressions like `SUM(DAY(?o))` or `SUM(?o + ?o)`
//! to a pre-aggregation `BIND` of the expression into a synthetic var, then
//! `SUM(?synthetic)`; the [`SumExprI64`] variants encode the supported shapes.

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_i64_singleton_batch, build_post_cursor_for_predicate, count_to_i64,
    cursor_fast_path_for_predicate, cursor_projection_otype_okey, leaf_entries_for_predicate,
    normalize_pred_sid, projection_okey_only, FastPathOperator, PredicateFastPath,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use chrono::Datelike;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryIndexStore, ColumnProjection, ColumnSet};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// SUM(expr(?o)) — supported i64 expressions
// ---------------------------------------------------------------------------

/// Supported datetime component functions for the SUM fast-path.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DateComponentFn {
    Year,
    Month,
    Day,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NumericUnaryFn {
    Abs,
    Ceil,
    Floor,
    Round,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SumExprI64 {
    Identity,
    AddSelf,
    DateComponent(DateComponentFn),
    NumericUnary(NumericUnaryFn),
}

impl SumExprI64 {
    fn constant_for_otype(self, o_type: u16) -> Option<i64> {
        match self {
            Self::Identity | Self::AddSelf => None,
            Self::DateComponent(component) => constant_component_for_otype(o_type, component),
            Self::NumericUnary(_) => None,
        }
    }

    fn eval_i64(self, o_type: u16, o_key: u64) -> Option<i64> {
        match self {
            Self::Identity => {
                let ot = OType::from_u16(o_type);
                if ot.decode_kind() != DecodeKind::I64 {
                    return None;
                }
                Some(ObjKey::from_u64(o_key).decode_i64())
            }
            Self::AddSelf => {
                let ot = OType::from_u16(o_type);
                if ot.decode_kind() != DecodeKind::I64 {
                    return None;
                }
                // Use saturating_mul instead of checked_mul to avoid silently
                // dropping rows (returning None) on overflow. The outer loop
                // already uses saturating_add, so clamping to i64::MAX/MIN is
                // consistent with the overall overflow strategy.
                Some(ObjKey::from_u64(o_key).decode_i64().saturating_mul(2))
            }
            Self::DateComponent(component) => component_from_otype_okey(o_type, o_key, component),
            Self::NumericUnary(func) => {
                let ot = OType::from_u16(o_type);
                if ot.decode_kind() != DecodeKind::I64 {
                    return None;
                }
                let v = ObjKey::from_u64(o_key).decode_i64();
                match func {
                    NumericUnaryFn::Abs => v.checked_abs(),
                    NumericUnaryFn::Ceil | NumericUnaryFn::Floor | NumericUnaryFn::Round => Some(v),
                }
            }
        }
    }

    fn label(self) -> &'static str {
        match self {
            SumExprI64::Identity => "SUM(?o)",
            SumExprI64::AddSelf => "SUM(?o+?o)",
            SumExprI64::DateComponent(c) => match c {
                DateComponentFn::Year => "SUM(YEAR)",
                DateComponentFn::Month => "SUM(MONTH)",
                DateComponentFn::Day => "SUM(DAY)",
            },
            SumExprI64::NumericUnary(n) => match n {
                NumericUnaryFn::Abs => "SUM(ABS)",
                NumericUnaryFn::Ceil => "SUM(CEIL)",
                NumericUnaryFn::Floor => "SUM(FLOOR)",
                NumericUnaryFn::Round => "SUM(ROUND)",
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Unified scalar-aggregate operator
// ---------------------------------------------------------------------------

/// A row-scanning scalar aggregate over a single predicate `?s <p> ?o`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ScalarAggKind {
    /// `SUM(expr(?o))` — folds i64, supports the [`SumExprI64`] expression set.
    Sum(SumExprI64),
    /// `AVG(?o)` over a homogeneous numeric predicate — Kahan-summed f64.
    AvgNumeric,
    /// `COUNT(DISTINCT ?o)` over IRI-ref objects — adjacent-dedup on POST order.
    CountDistinctObject,
}

/// Create a fused operator that outputs a single-row batch with the aggregate result.
pub fn predicate_scalar_agg_operator(
    kind: ScalarAggKind,
    predicate: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    let label = match kind {
        ScalarAggKind::Sum(s) => s.label(),
        ScalarAggKind::AvgNumeric => "AVG numeric",
        ScalarAggKind::CountDistinctObject => "COUNT(DISTINCT)",
    };
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = ctx.binary_store.as_ref() else {
                return Ok(None);
            };
            // O1: keep the cursor fast path when the scanned predicate is provably
            // uncovered by the view policy. Anything else (covered, default-deny,
            // multi-ledger, historical) defers to the fallback, which computes the
            // correct aggregate identity over the policy-filtered input.
            let pred_sid = normalize_pred_sid(store, &predicate)?;
            if !matches!(
                cursor_fast_path_for_predicate(ctx, &pred_sid),
                PredicateFastPath::Allow
            ) {
                return Ok(None);
            }
            // No-overlay HEAD reads take the leaflet-metadata scan (with its
            // constant-folding / directory shortcuts). An uncommitted overlay or
            // `to_t < max_t` would make that scan stale, so fold the same
            // aggregate over a POST overlay cursor instead.
            let has_overlay = ctx
                .overlay
                .map(fluree_db_core::OverlayProvider::epoch)
                .unwrap_or(0)
                != 0;
            let result = if !has_overlay && ctx.to_t == store.max_t() {
                scan_predicate_scalar_agg(store, ctx.binary_g_id, &predicate, kind)?
            } else {
                scan_predicate_scalar_agg_overlay(ctx, store, ctx.binary_g_id, &predicate, kind)?
            };
            match result {
                Some(output) => Ok(Some(output.into_batch(out_var)?)),
                None => Ok(None), // Unsupported at runtime — fall through to planned pipeline.
            }
        },
        fallback,
        label,
    )
}

/// The folded result of a scalar aggregate, before batch construction.
enum AggOutput {
    /// `xsd:integer` value (SUM, COUNT-DISTINCT).
    Integer(i64),
    /// A pre-built binding (AVG: `xsd:double`, or `Unbound` for the empty input).
    Binding(Binding),
}

impl AggOutput {
    fn into_batch(self, out_var: VarId) -> Result<Batch> {
        match self {
            AggOutput::Integer(v) => build_i64_singleton_batch(out_var, v, "scalar-agg"),
            AggOutput::Binding(b) => {
                Batch::single_row(Arc::from(vec![out_var].into_boxed_slice()), vec![b])
                    .map_err(|e| QueryError::execution(format!("scalar-agg batch build: {e}")))
            }
        }
    }
}

/// Per-variant accumulator carried across the shared POST scan.
enum AggState {
    Sum {
        scalar: SumExprI64,
        sum: i64,
    },
    Avg {
        required_otype: Option<u16>,
        // Kahan compensated summation state.
        sum: f64,
        compensation: f64,
        count: u64,
    },
    CountDistinct {
        prev_okey: Option<u64>,
        distinct: u64,
    },
}

impl AggState {
    fn new(kind: ScalarAggKind) -> Self {
        match kind {
            ScalarAggKind::Sum(scalar) => AggState::Sum { scalar, sum: 0 },
            ScalarAggKind::AvgNumeric => AggState::Avg {
                required_otype: None,
                sum: 0.0,
                compensation: 0.0,
                count: 0,
            },
            ScalarAggKind::CountDistinctObject => AggState::CountDistinct {
                prev_okey: None,
                distinct: 0,
            },
        }
    }

    fn finalize(self) -> Result<AggOutput> {
        Ok(match self {
            AggState::Sum { sum, .. } => AggOutput::Integer(sum),
            AggState::Avg { sum, count, .. } => {
                if count == 0 {
                    AggOutput::Binding(Binding::Unbound)
                } else {
                    AggOutput::Binding(Binding::lit(
                        FlakeValue::Double(sum / count as f64),
                        Sid::xsd_double(),
                    ))
                }
            }
            AggState::CountDistinct { distinct, .. } => {
                AggOutput::Integer(count_to_i64(distinct, "COUNT(DISTINCT)")?)
            }
        })
    }

    /// Fold `count` rows that each contribute the same constant `val` (SUM only).
    ///
    /// The metadata lane's constant-folding shortcut: when a scalar is constant
    /// for a homogeneous leaflet's `o_type`, the whole leaflet folds with no
    /// column IO. No-op for non-SUM kinds (which never request a constant fold).
    fn fold_constant_sum(&mut self, val: i64, count: u32) {
        if let AggState::Sum { sum, .. } = self {
            *sum = sum.saturating_add(val.saturating_mul(i64::from(count)));
        }
    }

    /// Fold a single row's object `(o_type, o_key)` into the accumulator.
    ///
    /// The per-row fold shared by both the leaflet-metadata scan and the POST
    /// overlay cursor scan, so the two lanes stay byte-for-byte consistent.
    /// Returns `Ok(false)` when the value is unsupported for this aggregate (a
    /// non-numeric/heterogeneous `o_type`, or a non-IRI object for
    /// COUNT(DISTINCT)) and the caller must fall back to the planned pipeline.
    fn fold_row(&mut self, o_type: u16, o_key: u64) -> Result<bool> {
        match self {
            AggState::Sum { scalar, sum } => {
                let Some(v) = scalar.eval_i64(o_type, o_key) else {
                    return Ok(false);
                };
                *sum = sum.saturating_add(v);
            }
            AggState::Avg {
                required_otype,
                sum,
                compensation,
                count,
            } => {
                if !OType::from_u16(o_type).is_numeric() {
                    return Ok(false);
                }
                match *required_otype {
                    None => *required_otype = Some(o_type),
                    Some(existing) if existing != o_type => return Ok(false),
                    Some(_) => {}
                }
                let val = decode_numeric_as_f64(o_type, o_key)?;
                // Kahan summation: compensate for lost low-order bits.
                let y = val - *compensation;
                let t = *sum + y;
                *compensation = (t - *sum) - y;
                *sum = t;
                *count = count.saturating_add(1);
            }
            AggState::CountDistinct {
                prev_okey,
                distinct,
            } => {
                // Only the IRI-ref object case (avoids dictionary decoding).
                if o_type != OType::IRI_REF.as_u16() {
                    return Ok(false);
                }
                // POST orders by (p_id, o_type, o_key, ..), so o_key is monotonic
                // within the IRI-ref group — adjacent dedup is exact.
                if *prev_okey != Some(o_key) {
                    *distinct += 1;
                    *prev_okey = Some(o_key);
                }
            }
        }
        Ok(true)
    }
}

/// Result for a predicate that is absent from the persisted dictionary
/// (empty input). SUM/COUNT-DISTINCT are 0; AVG of nothing is unbound.
fn empty_result(kind: ScalarAggKind) -> AggOutput {
    match kind {
        ScalarAggKind::Sum(_) | ScalarAggKind::CountDistinctObject => AggOutput::Integer(0),
        ScalarAggKind::AvgNumeric => AggOutput::Binding(Binding::Unbound),
    }
}

/// Shared POST-leaflet scan driver. Returns `Ok(None)` when a variant hits a
/// runtime-unsupported leaflet (mixed/non-matching datatypes) and the caller
/// must fall back to the planned pipeline.
fn scan_predicate_scalar_agg(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &Ref,
    kind: ScalarAggKind,
) -> Result<Option<AggOutput>> {
    let pred_sid = normalize_pred_sid(store, predicate)?;
    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
        return Ok(Some(empty_result(kind)));
    };

    // COUNT(DISTINCT ?o): metadata-only POST lead-group walk (directory
    // reads, every object type). The per-row scan below remains the fallback
    // for leaves that predate `lead_group_count` and handles IRI refs only.
    if matches!(kind, ScalarAggKind::CountDistinctObject) {
        if let Some(distinct) =
            crate::fast_count::count_distinct_objects_for_predicate(store, g_id, p_id)?
        {
            return Ok(Some(AggOutput::Integer(count_to_i64(
                distinct,
                "COUNT(DISTINCT)",
            )?)));
        }
    }

    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    let mut state = AggState::new(kind);

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            // POST is predicate-homogeneous; skip empty / foreign leaflets.
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }

            // Constant-folding shortcut: a SUM scalar that is constant for this
            // homogeneous leaflet's `o_type` folds with no column IO.
            if let ScalarAggKind::Sum(scalar) = kind {
                if let Some(ot) = entry.o_type_const {
                    if let Some(const_val) = scalar.constant_for_otype(ot) {
                        state.fold_constant_sum(const_val, entry.row_count);
                        continue;
                    }
                }
            }

            // Otherwise scan rows: pick the projection and the `o_type` to fold
            // each row under (the leaflet constant, or per-row OType for mixed
            // SUM leaflets). `None` ⇒ the leaflet is unsupported for this kind.
            let Some((projection, const_otype)) = leaflet_scan_plan(kind, entry.o_type_const)
            else {
                return Ok(None);
            };
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let o_key = batch.o_key.get(row);
                let o_type = const_otype.unwrap_or_else(|| batch.o_type.get_or(row, 0));
                if !state.fold_row(o_type, o_key)? {
                    return Ok(None);
                }
            }
        }
    }

    Ok(Some(state.finalize()?))
}

/// Per-leaflet plan for the no-overlay metadata scan: the column projection to
/// load and the constant `o_type` to fold every row under (`None` ⇒ read the
/// per-row OType column). Returns `None` when the leaflet is unsupported for
/// `kind` and the caller must fall back to the planned pipeline.
fn leaflet_scan_plan(
    kind: ScalarAggKind,
    o_type_const: Option<u16>,
) -> Option<(ColumnProjection, Option<u16>)> {
    match kind {
        // SUM reads OKey always, plus OType only for heterogeneous leaflets.
        ScalarAggKind::Sum(_) => {
            let mut needed = ColumnSet::EMPTY;
            needed.insert(ColumnId::OKey);
            if o_type_const.is_none() {
                needed.insert(ColumnId::OType);
            }
            Some((
                ColumnProjection {
                    output: ColumnSet::EMPTY,
                    internal: needed,
                },
                o_type_const,
            ))
        }
        // AVG needs a homogeneous numeric leaflet so it can read OKey only.
        ScalarAggKind::AvgNumeric => {
            let ot = o_type_const?;
            if !OType::from_u16(ot).is_numeric() {
                return None;
            }
            Some((projection_okey_only(), Some(ot)))
        }
        // COUNT(DISTINCT) handles only the IRI-ref object case (no dict decode).
        ScalarAggKind::CountDistinctObject => {
            if o_type_const != Some(OType::IRI_REF.as_u16()) {
                return None;
            }
            Some((projection_okey_only(), o_type_const))
        }
    }
}

/// Overlay-aware counterpart of [`scan_predicate_scalar_agg`]: folds the same
/// [`AggState`] over a POST overlay cursor that merges uncommitted novelty and
/// honors `to_t`. Used when an overlay carries novelty or `to_t < max_t`, where
/// the leaflet-metadata scan would be stale.
fn scan_predicate_scalar_agg_overlay(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    predicate: &Ref,
    kind: ScalarAggKind,
) -> Result<Option<AggOutput>> {
    let pred_sid = normalize_pred_sid(store, predicate)?;
    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
        // Predicate absent from the persisted dictionary. With novelty present it
        // may exist only in the overlay (no `p_id` to range-bound a cursor), so
        // fall back to the planned pipeline; otherwise the input is genuinely empty.
        let overlay_has_rows = ctx
            .overlay
            .map(fluree_db_core::OverlayProvider::epoch)
            .unwrap_or(0)
            != 0;
        return if overlay_has_rows {
            Ok(None)
        } else {
            Ok(Some(empty_result(kind)))
        };
    };

    let Some(mut cursor) = build_post_cursor_for_predicate(
        ctx,
        store,
        g_id,
        pred_sid,
        p_id,
        cursor_projection_otype_okey(),
    )?
    else {
        return Ok(None);
    };

    let mut state = AggState::new(kind);
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("scalar-agg cursor: {e}")))?
    {
        for row in 0..batch.row_count {
            let o_type = batch.o_type.get_or(row, 0);
            let o_key = batch.o_key.get(row);
            if !state.fold_row(o_type, o_key)? {
                return Ok(None);
            }
        }
    }

    Ok(Some(state.finalize()?))
}

// ---------------------------------------------------------------------------
// Decode helpers
// ---------------------------------------------------------------------------

fn decode_numeric_as_f64(o_type: u16, o_key: u64) -> Result<f64> {
    let ot = OType::from_u16(o_type);
    let key = ObjKey::from_u64(o_key);
    match ot.decode_kind() {
        DecodeKind::I64 => Ok(key.decode_i64() as f64),
        DecodeKind::F64 => Ok(key.decode_f64()),
        _ => Err(QueryError::execution(format!(
            "unsupported numeric decode kind for AVG fast-path: {ot:?}"
        ))),
    }
}

fn constant_component_for_otype(o_type: u16, component: DateComponentFn) -> Option<i64> {
    let ot = OType::from_u16(o_type);
    match component {
        DateComponentFn::Year => None,
        DateComponentFn::Month => {
            if ot == OType::XSD_G_YEAR || ot == OType::XSD_G_DAY {
                Some(1)
            } else {
                None
            }
        }
        DateComponentFn::Day => {
            if ot == OType::XSD_G_YEAR || ot == OType::XSD_G_YEAR_MONTH || ot == OType::XSD_G_MONTH
            {
                Some(1)
            } else {
                None
            }
        }
    }
}

fn component_from_otype_okey(o_type: u16, o_key: u64, component: DateComponentFn) -> Option<i64> {
    let ot = OType::from_u16(o_type);
    let key = ObjKey::from_u64(o_key);

    // Defaulting semantics match helpers.rs promotion:
    // - gYear → Jan 1, 00:00:00
    // - gYearMonth → day=1
    // - gMonth/gDay/gMonthDay → year=1970, missing parts default to 1
    const DEFAULT_YEAR: i64 = 1970;
    const DEFAULT_MONTH: i64 = 1;

    let (year, month, day) = if ot == OType::XSD_G_YEAR {
        (key.decode_g_year() as i64, 1, 1)
    } else if ot == OType::XSD_G_YEAR_MONTH {
        let (y, m) = key.decode_g_year_month();
        (y as i64, m as i64, 1)
    } else if ot == OType::XSD_G_MONTH {
        (DEFAULT_YEAR, key.decode_g_month() as i64, 1)
    } else if ot == OType::XSD_G_DAY {
        (DEFAULT_YEAR, DEFAULT_MONTH, key.decode_g_day() as i64)
    } else if ot == OType::XSD_G_MONTH_DAY {
        let (m, d) = key.decode_g_month_day();
        (DEFAULT_YEAR, m as i64, d as i64)
    } else if ot == OType::XSD_DATE {
        // xsd:date: days since Unix epoch (1970-01-01)
        let days = key.decode_date() as i64;
        let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
        let dt = base.checked_add_signed(chrono::Duration::days(days))?;
        (dt.year() as i64, dt.month() as i64, dt.day() as i64)
    } else if ot == OType::XSD_DATE_TIME {
        // xsd:dateTime: epoch micros; interpret in UTC for component extraction.
        let micros = key.decode_datetime();
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)?;
        (dt.year() as i64, dt.month() as i64, dt.day() as i64)
    } else {
        return None;
    };

    match component {
        DateComponentFn::Year => Some(year),
        DateComponentFn::Month => Some(month),
        DateComponentFn::Day => Some(day),
    }
}
