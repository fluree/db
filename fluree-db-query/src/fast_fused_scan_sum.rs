//! Fast-path fused scan + SUM(expr(?o)) for a single predicate.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (SUM(DAY(?o)) AS ?sum)
//! WHERE { ?s <p> ?o }
//! ```
//!
//! SPARQL lowering desugars expressions like `SUM(DAY(?o))` or `SUM(?o + ?o)`
//! to a pre-aggregation `BIND` of the
//! expression into a synthetic var, then `SUM(?synthetic)`. This operator bypasses
//! that pipeline by scanning the predicate's POST index range and aggregating
//! directly from encoded `(o_type/o_kind, o_key)` without materializing per-row
//! bindings.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_i64_singleton_batch, fast_path_store, leaf_entries_for_predicate, normalize_pred_sid,
    FastPathOperator,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use chrono::Datelike;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_binary_index::{ColumnProjection, ColumnSet};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::GraphId;

/// Supported datetime component functions for the fast-path.
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
}

/// Create a fused operator that outputs a single-row batch with the SUM result.
pub fn fused_scan_sum_i64_operator(
    predicate: Ref,
    scalar: SumExprI64,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    let label = match scalar {
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
    };
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match sum_scalar_i64(store, ctx.binary_g_id, &predicate, scalar)? {
                Some(sum) => Ok(Some(build_i64_singleton_batch(out_var, sum, "sum")?)),
                None => Ok(None),
            }
        },
        fallback,
        label,
    )
}

fn sum_scalar_i64(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &Ref,
    scalar: SumExprI64,
) -> Result<Option<i64>> {
    let pred_sid = normalize_pred_sid(store, predicate)?;
    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
        return Ok(Some(0));
    };

    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    let mut sum: i64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;

        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            // POST should always be predicate-homogeneous.
            if entry.p_const != Some(p_id) {
                continue;
            }

            // Constant folding: if scalar is constant for this o_type, avoid any column IO.
            if let Some(ot) = entry.o_type_const {
                if let Some(const_val) = scalar.constant_for_otype(ot) {
                    sum = sum.saturating_add(const_val.saturating_mul(entry.row_count as i64));
                    continue;
                }
            }

            let mut needed = ColumnSet::EMPTY;
            needed.insert(ColumnId::OKey);
            if entry.o_type_const.is_none() {
                needed.insert(ColumnId::OType);
            }
            let projection = ColumnProjection {
                output: ColumnSet::EMPTY,
                internal: needed,
            };

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            for row in 0..batch.row_count {
                let o_key = batch.o_key.get(row);
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                let Some(v) = scalar.eval_i64(ot, o_key) else {
                    // Unsupported datatype mix for this scalar fast-path.
                    return Ok(None);
                };
                sum = sum.saturating_add(v);
            }
        }
    }

    Ok(Some(sum))
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
