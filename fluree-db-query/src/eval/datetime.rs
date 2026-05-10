//! DateTime function implementations
//!
//! Implements SPARQL datetime functions: NOW, YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TZ, TIMEZONE

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Datelike, FixedOffset, SecondsFormat, Timelike, Utc};
use fluree_db_core::temporal::DateTime as FlureeDateTime;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use std::sync::Arc;

use super::helpers::{check_arity, parse_datetime_from_binding};
use super::value::ComparableValue;
use crate::parse::UnresolvedDatatypeConstraint;

#[derive(Copy, Clone, Debug)]
enum DateComponent {
    Year,
    Month,
    Day,
    Hour,
    Minute,
}

pub fn eval_now(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "NOW")?;
    let now = Utc::now();
    let formatted = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let parsed = FlureeDateTime::parse(&formatted)
        .map_err(|e| QueryError::InvalidFilter(format!("now parse error: {e}")))?;
    Ok(Some(ComparableValue::DateTime(parsed)))
}

pub fn eval_year<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "YEAR", DateComponent::Year, |dt| {
        dt.year() as i64
    })
}

pub fn eval_month<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "MONTH", DateComponent::Month, |dt| {
        dt.month() as i64
    })
}

pub fn eval_day<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "DAY", DateComponent::Day, |dt| {
        dt.day() as i64
    })
}

pub fn eval_hours<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "HOURS", DateComponent::Hour, |dt| {
        dt.hour() as i64
    })
}

pub fn eval_minutes<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "MINUTES", DateComponent::Minute, |dt| {
        dt.minute() as i64
    })
}

pub fn eval_seconds<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    // W3C: SECONDS returns xsd:decimal (fractional seconds)
    check_arity(args, 1, "SECONDS")?;
    if let Expression::Var(var) = &args[0] {
        match row.get(*var) {
            Some(binding) => match parse_datetime_from_binding(binding, ctx) {
                Some(dt) => {
                    let secs = dt.second() as i64;
                    let nanos = dt.nanosecond() as i64;
                    let decimal = if nanos == 0 {
                        BigDecimal::from(secs)
                    } else {
                        let total_nanos = secs * 1_000_000_000 + nanos;
                        BigDecimal::new(total_nanos.into(), 9)
                    };
                    Ok(Some(ComparableValue::Decimal(Box::new(decimal))))
                }
                None => Ok(None),
            },
            None => Ok(None),
        }
    } else {
        Err(QueryError::InvalidFilter(
            "SECONDS requires a variable argument".to_string(),
        ))
    }
}

pub fn eval_tz<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "TZ")?;
    if let Expression::Var(var_id) = &args[0] {
        match row.get(*var_id) {
            Some(binding) => {
                let has_tz = has_timezone_info(binding);
                match parse_datetime_from_binding(binding, ctx) {
                    Some(dt) => {
                        if !has_tz {
                            // No timezone info in the original value
                            Ok(Some(ComparableValue::String(Arc::from(""))))
                        } else {
                            let total_secs = dt.offset().local_minus_utc();
                            if total_secs == 0 {
                                Ok(Some(ComparableValue::String(Arc::from("Z"))))
                            } else {
                                let hours = total_secs / 3600;
                                let mins = (total_secs.abs() % 3600) / 60;
                                let sign = if total_secs >= 0 { '+' } else { '-' };
                                let tz_str = format!("{}{:02}:{:02}", sign, hours.abs(), mins);
                                Ok(Some(ComparableValue::String(Arc::from(tz_str))))
                            }
                        }
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    } else {
        Err(QueryError::InvalidFilter(
            "TZ requires a variable argument".to_string(),
        ))
    }
}

pub fn eval_timezone<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "TIMEZONE")?;
    if let Expression::Var(var_id) = &args[0] {
        match row.get(*var_id) {
            Some(binding) => {
                let has_tz = has_timezone_info(binding);
                match parse_datetime_from_binding(binding, ctx) {
                    Some(dt) => {
                        if !has_tz {
                            // No timezone → unbound per W3C
                            return Ok(None);
                        }
                        let total_secs = dt.offset().local_minus_utc();
                        let duration = format_day_time_duration(total_secs);
                        Ok(Some(ComparableValue::TypedLiteral {
                            val: fluree_db_core::FlakeValue::String(duration),
                            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                                "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
                            ))),
                        }))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    } else {
        Err(QueryError::InvalidFilter(
            "TIMEZONE requires a variable argument".to_string(),
        ))
    }
}

/// Check if a datetime binding carries explicit timezone information.
fn has_timezone_info(binding: &crate::binding::Binding) -> bool {
    use crate::binding::Binding;
    match binding {
        Binding::Lit {
            val: fluree_db_core::FlakeValue::DateTime(dt),
            ..
        } => dt.tz_offset().is_some(),
        _ => false,
    }
}

/// Format seconds as xsd:dayTimeDuration: "PT0S", "-PT8H", "PT5H30M", etc.
fn format_day_time_duration(total_secs: i32) -> String {
    if total_secs == 0 {
        return "PT0S".to_string();
    }
    let negative = total_secs < 0;
    let abs_secs = total_secs.unsigned_abs();
    let hours = abs_secs / 3600;
    let minutes = (abs_secs % 3600) / 60;
    let secs = abs_secs % 60;

    let mut result = String::new();
    if negative {
        result.push('-');
    }
    result.push_str("PT");
    if hours > 0 {
        result.push_str(&format!("{hours}H"));
    }
    if minutes > 0 {
        result.push_str(&format!("{minutes}M"));
    }
    if secs > 0 {
        result.push_str(&format!("{secs}S"));
    }
    result
}

/// Extract a datetime component from a binding
fn eval_datetime_component<R: RowAccess, F>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    fn_name: &str,
    component: DateComponent,
    extract: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&DateTime<FixedOffset>) -> i64,
{
    check_arity(args, 1, fn_name)?;
    if let Expression::Var(var) = &args[0] {
        match row.get(*var) {
            Some(binding) => {
                if let Some(v) = fast_datetime_component_from_binding(binding, ctx, component) {
                    return Ok(Some(ComparableValue::Long(v)));
                }
                match parse_datetime_from_binding(binding, ctx) {
                    Some(dt) => Ok(Some(ComparableValue::Long(extract(&dt)))),
                    None => Ok(None),
                }
            }
            None => Ok(None), // unbound variable
        }
    } else {
        Err(QueryError::InvalidFilter(format!(
            "{fn_name} requires a variable argument"
        )))
    }
}

/// Fast-path extraction for YEAR/MONTH/DAY/HOURS/MINUTES without constructing a chrono DateTime.
///
/// This avoids the `parse_datetime_from_binding()` promotion path (gYear→dateTime, etc.) which
/// is expensive when applied per-row to large scans (e.g. sparqloscope DBLP date-* benchmarks).
fn fast_datetime_component_from_binding(
    binding: &crate::binding::Binding,
    _ctx: Option<&ExecutionContext<'_>>,
    component: DateComponent,
) -> Option<i64> {
    use crate::binding::Binding;
    use fluree_db_core::FlakeValue;

    // Calendar fragment defaults match `flake_value_to_datetime()` in helpers.rs:
    // - gYear → Jan 1, 00:00:00
    // - gYearMonth → day=1
    // - gMonth/gDay/gMonthDay → year=1970, month/day defaults as appropriate
    const DEFAULT_YEAR: i64 = 1970;
    const DEFAULT_MONTH: i64 = 1;

    let extract_from_parts =
        |year: i64, month: i64, day: i64, hour: i64, minute: i64| match component {
            DateComponent::Year => Some(year),
            DateComponent::Month => Some(month),
            DateComponent::Day => Some(day),
            DateComponent::Hour => Some(hour),
            DateComponent::Minute => Some(minute),
        };

    match binding {
        Binding::Lit { val, dtc, .. } => match val {
            FlakeValue::GYear(gy) => extract_from_parts(gy.year() as i64, 1, 1, 0, 0),
            FlakeValue::GYearMonth(gym) => {
                extract_from_parts(gym.year() as i64, gym.month() as i64, 1, 0, 0)
            }
            FlakeValue::GMonth(gm) => extract_from_parts(DEFAULT_YEAR, gm.month() as i64, 1, 0, 0),
            FlakeValue::GDay(gd) => {
                extract_from_parts(DEFAULT_YEAR, DEFAULT_MONTH, gd.day() as i64, 0, 0)
            }
            FlakeValue::GMonthDay(gmd) => {
                extract_from_parts(DEFAULT_YEAR, gmd.month() as i64, gmd.day() as i64, 0, 0)
            }
            // Numeric gYear encoding fallback (see helpers.rs::flake_value_to_datetime()).
            FlakeValue::Long(year) => {
                let dts = &*crate::eval::helpers::WELL_KNOWN_DATATYPES;
                if *dtc.datatype() == dts.xsd_g_year {
                    extract_from_parts(*year, 1, 1, 0, 0)
                } else {
                    None
                }
            }
            // Other types fall back to the full chrono parse path.
            _ => None,
        },
        Binding::EncodedLit { o_kind, o_key, .. } => {
            let kind = ObjKind::from_u8(*o_kind);
            let key = ObjKey::from_u64(*o_key);
            match kind.as_u8() {
                x if x == ObjKind::G_YEAR.as_u8() => {
                    extract_from_parts(key.decode_g_year() as i64, 1, 1, 0, 0)
                }
                x if x == ObjKind::G_YEAR_MONTH.as_u8() => {
                    let (y, m) = key.decode_g_year_month();
                    extract_from_parts(y as i64, m as i64, 1, 0, 0)
                }
                x if x == ObjKind::G_MONTH.as_u8() => {
                    extract_from_parts(DEFAULT_YEAR, key.decode_g_month() as i64, 1, 0, 0)
                }
                x if x == ObjKind::G_DAY.as_u8() => {
                    extract_from_parts(DEFAULT_YEAR, DEFAULT_MONTH, key.decode_g_day() as i64, 0, 0)
                }
                x if x == ObjKind::G_MONTH_DAY.as_u8() => {
                    let (m, d) = key.decode_g_month_day();
                    extract_from_parts(DEFAULT_YEAR, m as i64, d as i64, 0, 0)
                }
                _ => None,
            }
        }
        _ => None,
    }
}
