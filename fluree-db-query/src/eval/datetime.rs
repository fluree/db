//! DateTime function implementations
//!
//! Implements SPARQL datetime functions: NOW, YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TZ, TIMEZONE

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Datelike, FixedOffset, SecondsFormat, Timelike, Utc};
use fluree_db_core::temporal::{CalendarField, DateTime as FlureeDateTime, TemporalKind};
use fluree_db_core::value_id::{ObjKey, ObjKind};
use std::sync::Arc;

use super::helpers::{binding_is_temporal, check_arity, parse_datetime_from_binding};
use super::value::ComparableValue;
use crate::parse::UnresolvedDatatypeConstraint;

pub fn eval_now(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "NOW")?;
    let now = Utc::now();
    let formatted = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let parsed = FlureeDateTime::parse(&formatted)
        .map_err(|e| QueryError::InvalidFilter(format!("now parse error: {e}")))?;
    Ok(Some(ComparableValue::DateTime(parsed)))
}

/// Current UTC calendar date — Cypher's zero-arg `date()`.
pub fn eval_today(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "date")?;
    Ok(Some(ComparableValue::Date(
        fluree_db_core::temporal::Date::today_utc(),
    )))
}

pub fn eval_year<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "YEAR", CalendarField::Year, |dt| {
        dt.year() as i64
    })
}

pub fn eval_month<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "MONTH", CalendarField::Month, |dt| {
        dt.month() as i64
    })
}

pub fn eval_day<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "DAY", CalendarField::Day, |dt| {
        dt.day() as i64
    })
}

pub fn eval_hours<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "HOURS", CalendarField::Hour, |dt| {
        dt.hour() as i64
    })
}

pub fn eval_minutes<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, ctx, "MINUTES", CalendarField::Minute, |dt| {
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
            Some(binding) => {
                // A value that carries no seconds (a bare `xsd:gYear`, say) has
                // no answer here; report unbound rather than the promotion's
                // zero. See `eval_datetime_component`.
                if let Some(kind) = temporal_kind_of_binding(binding) {
                    if !kind.carries(CalendarField::Second) {
                        return Ok(None);
                    }
                }
                match parse_datetime_from_binding(binding, ctx) {
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
                }
            }
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
            // Fluree normalizes temporal values to UTC and does not persist the
            // source offset: the binary index stores an instant and nothing
            // else, so by the time a value is indexed its original offset is
            // gone. Reporting the offset would therefore answer "-08:00" while
            // a value sat in novelty and "Z" once a background reindex moved
            // it — the same query returning a different string with no write in
            // between, which a caller cannot predict or control.
            //
            // Answering UTC unconditionally is what makes the two lanes agree.
            // It is a deliberate deviation from SPARQL 1.1 §17.4.5.9, which
            // expects the source offset; the two W3C tests that pin that
            // (functions/tz-01, functions/timezone-01) are registered as
            // not-supported in `testsuite-sparql/tests/registers/mod.rs`.
            Some(binding) if binding_is_temporal(binding, ctx) => {
                Ok(Some(ComparableValue::String(Arc::from("Z"))))
            }
            // Not a temporal value: a type error, which demotes to unbound.
            Some(_) => Ok(None),
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
            // UTC for every stored temporal — see the note in `eval_tz`.
            Some(binding) if binding_is_temporal(binding, ctx) => {
                Ok(Some(ComparableValue::TypedLiteral {
                    val: fluree_db_core::FlakeValue::String(format_day_time_duration(0)),
                    dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                        "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
                    ))),
                }))
            }
            Some(_) => Ok(None),
            None => Ok(None),
        }
    } else {
        Err(QueryError::InvalidFilter(
            "TIMEZONE requires a variable argument".to_string(),
        ))
    }
}

/// Check if a datetime binding carries explicit timezone information.
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
    component: CalendarField,
    extract: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&DateTime<FixedOffset>) -> i64,
{
    check_arity(args, 1, fn_name)?;
    if let Expression::Var(var) = &args[0] {
        match row.get(*var) {
            Some(binding) => {
                if let Some(kind) = temporal_kind_of_binding(binding) {
                    // The value's datatype is known. If its lexical form does
                    // not carry this field there is no answer in the data, so
                    // report unbound — never the filler the promotion below
                    // would supply (day 1, month 1, or the epoch's 1970).
                    // Returning here also keeps such a value away from the
                    // epoch-millis fallback, which would re-fabricate it.
                    if !kind.carries(component) {
                        return Ok(None);
                    }
                    if let Some(v) = fast_datetime_component_from_binding(binding, ctx, component) {
                        return Ok(Some(ComparableValue::Long(v)));
                    }
                    if let Some(dt) = parse_datetime_from_binding(binding, ctx) {
                        return Ok(Some(ComparableValue::Long(extract(&dt))));
                    }
                    return Ok(None);
                }
                // Not a recognized temporal type. A plain integer is treated as
                // Unix epoch milliseconds: LDBC-style datasets store
                // dates/datetimes as epoch-ms longs rather than xsd:dateTime, so
                // `<epochMs>.month` / `.day` must still resolve.
                if let Some(ms) = binding_epoch_millis(binding, ctx) {
                    if let Some(v) = epoch_millis_component(ms, component) {
                        return Ok(Some(ComparableValue::Long(v)));
                    }
                }
                Ok(None)
            }
            None => Ok(None), // unbound variable
        }
    } else {
        Err(QueryError::InvalidFilter(format!(
            "{fn_name} requires a variable argument"
        )))
    }
}

/// Extract an integer value from a binding that holds a plain `Long`
/// (eager or late-materialized), for epoch-millisecond interpretation.
fn binding_epoch_millis(
    binding: &crate::binding::Binding,
    ctx: Option<&ExecutionContext<'_>>,
) -> Option<i64> {
    use crate::binding::Binding;
    use fluree_db_core::FlakeValue;
    match binding {
        Binding::Lit {
            val: FlakeValue::Long(n),
            ..
        } => Some(*n),
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } => match ctx?
            .decode_encoded_value(*o_kind, *o_key, *p_id, *dt_id, *lang_id)?
            .ok()?
        {
            FlakeValue::Long(n) => Some(n),
            _ => None,
        },
        _ => None,
    }
}

/// Component of a Unix epoch-millisecond instant, interpreted in UTC.
fn epoch_millis_component(ms: i64, component: CalendarField) -> Option<i64> {
    let dt = DateTime::<Utc>::from_timestamp_millis(ms)?;
    Some(match component {
        CalendarField::Year => i64::from(dt.year()),
        CalendarField::Month => i64::from(dt.month()),
        CalendarField::Day => i64::from(dt.day()),
        CalendarField::Hour => i64::from(dt.hour()),
        CalendarField::Minute => i64::from(dt.minute()),
        // SECONDS is fractional (xsd:decimal) and has its own evaluator.
        CalendarField::Second => return None,
    })
}

/// The XSD temporal datatype of a binding, or `None` when the value is not a
/// recognized temporal type (a string, a bare integer, an IRI).
///
/// The `FlakeValue::Long` arm is the numeric `xsd:gYear` encoding: the value
/// alone is indistinguishable from any other integer, so the binding's declared
/// datatype settles it.
fn temporal_kind_of_binding(binding: &crate::binding::Binding) -> Option<TemporalKind> {
    use crate::binding::Binding;
    use fluree_db_core::FlakeValue;
    match binding {
        Binding::Lit { val, dtc, .. } => TemporalKind::from_flake_value(val).or_else(|| {
            let dts = &*crate::eval::helpers::WELL_KNOWN_DATATYPES;
            match val {
                FlakeValue::Long(_) if *dtc.datatype() == dts.xsd_g_year => {
                    Some(TemporalKind::GYear)
                }
                _ => None,
            }
        }),
        Binding::EncodedLit { o_kind, .. } => {
            TemporalKind::from_obj_kind(ObjKind::from_u8(*o_kind))
        }
        _ => None,
    }
}

/// Fast-path extraction of a CARRIED calendar field, without constructing a
/// chrono DateTime.
///
/// This avoids the `parse_datetime_from_binding()` promotion path (gYear→dateTime, etc.) which
/// is expensive when applied per-row to large scans (e.g. sparqloscope DBLP date-* benchmarks).
///
/// Callers must have already established that the value's datatype carries the
/// field (`TemporalKind::carries`); this function never substitutes a default
/// for one it does not.
fn fast_datetime_component_from_binding(
    binding: &crate::binding::Binding,
    _ctx: Option<&ExecutionContext<'_>>,
    component: CalendarField,
) -> Option<i64> {
    use crate::binding::Binding;
    use fluree_db_core::FlakeValue;

    // Each arm supplies ONLY the fields its datatype carries. There are no
    // defaults here on purpose: this used to fill the rest from 1970/1/1 and
    // hand them back as answers, a third copy of the promotion's defaults
    // table. `None` now means "not available from this representation", which
    // sends the caller to the general path rather than inventing a value.
    let pick = |year: Option<i64>, month: Option<i64>, day: Option<i64>| match component {
        CalendarField::Year => year,
        CalendarField::Month => month,
        CalendarField::Day => day,
        // A calendar fragment carries no time of day, and SECONDS has its own
        // evaluator; the datatypes that do carry a time go the general path.
        CalendarField::Hour | CalendarField::Minute | CalendarField::Second => None,
    };

    match binding {
        Binding::Lit { val, dtc, .. } => match val {
            FlakeValue::GYear(gy) => pick(Some(i64::from(gy.year())), None, None),
            FlakeValue::GYearMonth(gym) => pick(
                Some(i64::from(gym.year())),
                Some(i64::from(gym.month())),
                None,
            ),
            FlakeValue::GMonth(gm) => pick(None, Some(i64::from(gm.month())), None),
            FlakeValue::GDay(gd) => pick(None, None, Some(i64::from(gd.day()))),
            FlakeValue::GMonthDay(gmd) => pick(
                None,
                Some(i64::from(gmd.month())),
                Some(i64::from(gmd.day())),
            ),
            // Numeric gYear encoding: the datatype, not the value, settles it.
            FlakeValue::Long(year) => {
                let dts = &*crate::eval::helpers::WELL_KNOWN_DATATYPES;
                if *dtc.datatype() == dts.xsd_g_year {
                    pick(Some(*year), None, None)
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
                    pick(Some(i64::from(key.decode_g_year())), None, None)
                }
                x if x == ObjKind::G_YEAR_MONTH.as_u8() => {
                    let (y, m) = key.decode_g_year_month();
                    pick(Some(i64::from(y)), Some(i64::from(m)), None)
                }
                x if x == ObjKind::G_MONTH.as_u8() => {
                    pick(None, Some(i64::from(key.decode_g_month())), None)
                }
                x if x == ObjKind::G_DAY.as_u8() => {
                    pick(None, None, Some(i64::from(key.decode_g_day())))
                }
                x if x == ObjKind::G_MONTH_DAY.as_u8() => {
                    let (m, d) = key.decode_g_month_day();
                    pick(None, Some(i64::from(m)), Some(i64::from(d)))
                }
                _ => None,
            }
        }
        _ => None,
    }
}
