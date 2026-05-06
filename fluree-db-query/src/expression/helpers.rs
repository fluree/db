//! Shared helper functions for filter evaluation
//!
//! Contains arity checks, regex caching, datetime parsing, and other utilities.

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::context::WellKnownDatatypes;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone};
use fluree_db_core::{FlakeValue, ObjKind};
use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::value::ComparableValue;
use crate::var_registry::VarId;

// =============================================================================
// Static WellKnownDatatypes (optimization to avoid repeated construction)
// =============================================================================

/// Lazily initialized well-known datatypes.
///
/// This avoids creating a new WellKnownDatatypes instance on every function call.
pub static WELL_KNOWN_DATATYPES: Lazy<WellKnownDatatypes> = Lazy::new(WellKnownDatatypes::new);

// =============================================================================
// Regex Caching
// =============================================================================

// Thread-local cache for compiled regexes to avoid recompiling on every row.
// SPARQL REGEX patterns are typically constant across a query, so caching
// provides significant speedup for filter-heavy queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EncodedBindingCacheKey {
    Lit {
        o_kind: u8,
        o_key: u64,
        p_id: u32,
        dt_id: u16,
        lang_id: u16,
    },
    Sid {
        s_id: u64,
    },
    Pid {
        p_id: u32,
    },
}

type EncodedBoolPredicateKey = (usize, u16, u64, EncodedBindingCacheKey);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CacheableBoolPredicate {
    input_var: VarId,
    expr_hash: u64,
}

#[derive(Clone, Debug)]
pub struct PreparedBoolExpression {
    expr: Expression,
    cache_spec: Option<CacheableBoolPredicate>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VarUsage {
    None,
    Single(VarId),
    Multiple,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BoolCacheAnalysis {
    expr_hash: u64,
    vars: VarUsage,
    supported: bool,
    returns_bool: bool,
    may_materialize: bool,
}

thread_local! {
    static REGEX_CACHE: RefCell<lru::LruCache<(String, String), Regex>> =
        RefCell::new(lru::LruCache::new(std::num::NonZeroUsize::new(32).unwrap()));
    static ENCODED_BOOL_PREDICATE_CACHE: RefCell<lru::LruCache<EncodedBoolPredicateKey, bool>> =
        RefCell::new(lru::LruCache::new(std::num::NonZeroUsize::new(256).unwrap()));
}

/// Build a regex with optional flags (cached)
///
/// Supported flags: i (case-insensitive), m (multiline), s (dot-all), x (ignore whitespace)
/// Returns an error for unknown flags (not silent ignore).
///
/// Uses a thread-local LRU cache to avoid recompiling the same pattern+flags
/// on every row. Regex::clone is cheap (Arc internally).
pub fn build_regex_with_flags(pattern: &str, flags: &str) -> Result<Regex> {
    // Check cache first
    let cache_key = (pattern.to_string(), flags.to_string());
    let cached = REGEX_CACHE.with(|cache| cache.borrow_mut().get(&cache_key).cloned());

    if let Some(re) = cached {
        return Ok(re);
    }

    // Not in cache - compile and store
    let mut builder = RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            c => {
                return Err(QueryError::InvalidFilter(format!(
                    "Unknown regex flag: '{c}'"
                )));
            }
        }
    }
    let re = builder
        .build()
        .map_err(|e| QueryError::InvalidFilter(format!("Invalid regex: {e}")))?;

    // Cache for future use
    REGEX_CACHE.with(|cache| {
        cache.borrow_mut().put(cache_key, re.clone());
    });

    Ok(re)
}

impl PreparedBoolExpression {
    pub fn new(expr: Expression) -> Self {
        let cache_spec = analyze_cacheable_bool_predicate(&expr);
        Self { expr, cache_spec }
    }

    pub fn expr(&self) -> &Expression {
        &self.expr
    }

    pub fn referenced_vars(&self) -> Vec<VarId> {
        self.expr.referenced_vars()
    }

    pub fn eval_to_bool<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<bool> {
        if let Some(pass) =
            eval_cached_bool_predicate_with_spec(self.cache_spec.as_ref(), row, ctx, || {
                self.expr.eval_to_bool_uncached(row, ctx)
            })?
        {
            return Ok(pass);
        }
        self.expr.eval_to_bool_uncached(row, ctx)
    }

    pub fn eval_to_bool_non_strict<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<bool> {
        match self.eval_to_bool(row, ctx) {
            Ok(pass) => Ok(pass),
            Err(err) if err.can_demote_in_expression() => Ok(false),
            Err(err) => Err(err),
        }
    }
}

pub fn eval_cached_bool_predicate<R: RowAccess>(
    expr: &Expression,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    compute: impl FnOnce() -> Result<bool>,
) -> Result<Option<bool>> {
    let cache_spec = analyze_cacheable_bool_predicate(expr);
    eval_cached_bool_predicate_with_spec(cache_spec.as_ref(), row, ctx, compute)
}

fn eval_cached_bool_predicate_with_spec<R: RowAccess>(
    cache_spec: Option<&CacheableBoolPredicate>,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    compute: impl FnOnce() -> Result<bool>,
) -> Result<Option<bool>> {
    let Some(ctx) = ctx else {
        return Ok(None);
    };
    let Some(store) = ctx.binary_store.as_ref() else {
        return Ok(None);
    };
    let Some(spec) = cache_spec else {
        return Ok(None);
    };
    let Some(binding) = row.get(spec.input_var) else {
        return Ok(None);
    };
    let Some(binding_key) = encoded_binding_cache_key(binding) else {
        return Ok(None);
    };

    let cache_key = (
        Arc::as_ptr(store) as usize,
        ctx.binary_g_id,
        spec.expr_hash,
        binding_key,
    );
    if let Some(hit) =
        ENCODED_BOOL_PREDICATE_CACHE.with(|cache| cache.borrow_mut().get(&cache_key).copied())
    {
        return Ok(Some(hit));
    }

    let pass = compute()?;
    ENCODED_BOOL_PREDICATE_CACHE.with(|cache| {
        cache.borrow_mut().put(cache_key, pass);
    });
    Ok(Some(pass))
}

fn analyze_cacheable_bool_predicate(expr: &Expression) -> Option<CacheableBoolPredicate> {
    let analysis = analyze_bool_cache(expr);
    let VarUsage::Single(input_var) = analysis.vars else {
        return None;
    };
    if !analysis.supported || !analysis.returns_bool || !analysis.may_materialize {
        return None;
    }
    Some(CacheableBoolPredicate {
        input_var,
        expr_hash: analysis.expr_hash,
    })
}

fn encoded_binding_cache_key(binding: &Binding) -> Option<EncodedBindingCacheKey> {
    match binding {
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } if encoded_lit_may_need_dictionary_lookup(*o_kind, *dt_id, *lang_id) => {
            Some(EncodedBindingCacheKey::Lit {
                o_kind: *o_kind,
                o_key: *o_key,
                p_id: *p_id,
                dt_id: *dt_id,
                lang_id: *lang_id,
            })
        }
        Binding::EncodedSid { s_id, .. } => Some(EncodedBindingCacheKey::Sid { s_id: *s_id }),
        Binding::EncodedPid { p_id } => Some(EncodedBindingCacheKey::Pid { p_id: *p_id }),
        _ => None,
    }
}

fn encoded_lit_may_need_dictionary_lookup(o_kind: u8, dt_id: u16, lang_id: u16) -> bool {
    matches!(
        ObjKind::from_u8(o_kind),
        kind if kind == ObjKind::LEX_ID
            || kind == ObjKind::JSON_ID
            || kind == ObjKind::NUM_BIG
            || kind == ObjKind::VECTOR_ID
    ) || dt_id != 0
        || lang_id != 0
}

fn analyze_bool_cache(expr: &Expression) -> BoolCacheAnalysis {
    let mut hasher = DefaultHasher::new();
    analyze_bool_cache_inner(expr, &mut hasher)
}

fn analyze_bool_cache_inner(expr: &Expression, state: &mut impl Hasher) -> BoolCacheAnalysis {
    std::mem::discriminant(expr).hash(state);
    match expr {
        Expression::Var(var) => {
            var.hash(state);
            BoolCacheAnalysis {
                expr_hash: state.finish(),
                vars: VarUsage::Single(*var),
                supported: true,
                returns_bool: false,
                may_materialize: false,
            }
        }
        Expression::Const(value) => {
            hash_filter_value(value, state);
            BoolCacheAnalysis {
                expr_hash: state.finish(),
                vars: VarUsage::None,
                supported: true,
                returns_bool: false,
                may_materialize: false,
            }
        }
        Expression::Exists { .. } => BoolCacheAnalysis {
            expr_hash: state.finish(),
            vars: VarUsage::None,
            supported: false,
            returns_bool: false,
            may_materialize: false,
        },
        Expression::Call { func, args } => {
            hash_function(func, state);
            args.len().hash(state);

            let mut vars = VarUsage::None;
            let mut supported = function_supported_for_bool_cache(func);
            let mut may_materialize = function_may_materialize_encoded_value(func);
            let mut all_children_return_bool = true;

            for arg in args {
                let child = analyze_bool_cache_inner(arg, state);
                vars = merge_var_usage(vars, child.vars);
                supported &= child.supported;
                may_materialize |= child.may_materialize;
                all_children_return_bool &= child.returns_bool;
            }

            BoolCacheAnalysis {
                expr_hash: state.finish(),
                vars,
                supported,
                returns_bool: function_returns_bool(func, all_children_return_bool),
                may_materialize,
            }
        }
    }
}

fn merge_var_usage(left: VarUsage, right: VarUsage) -> VarUsage {
    match (left, right) {
        (VarUsage::None, other) | (other, VarUsage::None) => other,
        (VarUsage::Single(a), VarUsage::Single(b)) if a == b => VarUsage::Single(a),
        _ => VarUsage::Multiple,
    }
}

fn function_supported_for_bool_cache(func: &Function) -> bool {
    !matches!(
        func,
        Function::Rand
            | Function::Now
            | Function::Uuid
            | Function::StrUuid
            | Function::Bnode
            | Function::Fulltext
            | Function::DotProduct
            | Function::CosineSimilarity
            | Function::EuclideanDistance
            | Function::GeofDistance
            | Function::T
            | Function::Op
            | Function::Custom(_)
    )
}

fn function_returns_bool(func: &Function, all_children_return_bool: bool) -> bool {
    match func {
        Function::Eq
        | Function::Ne
        | Function::Lt
        | Function::Le
        | Function::Gt
        | Function::Ge
        | Function::In
        | Function::NotIn
        | Function::Contains
        | Function::StrStarts
        | Function::StrEnds
        | Function::Regex
        | Function::LangMatches
        | Function::SameTerm => true,
        Function::And | Function::Or | Function::Not => all_children_return_bool,
        _ => false,
    }
}

fn function_may_materialize_encoded_value(func: &Function) -> bool {
    matches!(
        func,
        Function::Eq
            | Function::Ne
            | Function::Lt
            | Function::Le
            | Function::Gt
            | Function::Ge
            | Function::In
            | Function::NotIn
            | Function::Contains
            | Function::StrStarts
            | Function::StrEnds
            | Function::Regex
            | Function::Str
            | Function::Lang
            | Function::Lcase
            | Function::Ucase
            | Function::Strlen
            | Function::Concat
            | Function::StrBefore
            | Function::StrAfter
            | Function::Replace
            | Function::Substr
            | Function::EncodeForUri
            | Function::StrDt
            | Function::StrLang
            | Function::Datatype
            | Function::LangMatches
            | Function::SameTerm
            | Function::Iri
            | Function::If
            | Function::Coalesce
            | Function::Md5
            | Function::Sha1
            | Function::Sha256
            | Function::Sha384
            | Function::Sha512
            | Function::XsdBoolean
            | Function::XsdInteger
            | Function::XsdFloat
            | Function::XsdDouble
            | Function::XsdDecimal
            | Function::XsdString
            | Function::Year
            | Function::Month
            | Function::Day
            | Function::Hours
            | Function::Minutes
            | Function::Seconds
            | Function::Tz
            | Function::Timezone
    )
}

fn hash_function(func: &Function, state: &mut impl Hasher) {
    std::mem::discriminant(func).hash(state);
    if let Function::Custom(name) = func {
        name.hash(state);
    }
}

fn hash_filter_value(value: &crate::ir::FilterValue, state: &mut impl Hasher) {
    std::mem::discriminant(value).hash(state);
    match value {
        crate::ir::FilterValue::Long(v) => v.hash(state),
        crate::ir::FilterValue::Double(v) => v.to_bits().hash(state),
        crate::ir::FilterValue::String(v) => v.hash(state),
        crate::ir::FilterValue::Bool(v) => v.hash(state),
        crate::ir::FilterValue::Temporal(v) => hash_flake_value(v, state),
    }
}

fn hash_flake_value(value: &FlakeValue, state: &mut impl Hasher) {
    std::mem::discriminant(value).hash(state);
    match value {
        FlakeValue::Ref(v) => {
            v.namespace_code.hash(state);
            v.name.hash(state);
        }
        FlakeValue::Boolean(v) => v.hash(state),
        FlakeValue::Long(v) => v.hash(state),
        FlakeValue::Double(v) => v.to_bits().hash(state),
        FlakeValue::BigInt(v) => v.to_string().hash(state),
        FlakeValue::Decimal(v) => v.to_string().hash(state),
        FlakeValue::DateTime(v) => v.original().hash(state),
        FlakeValue::Date(v) => v.original().hash(state),
        FlakeValue::Time(v) => v.original().hash(state),
        FlakeValue::GYear(v) => v.original().hash(state),
        FlakeValue::GYearMonth(v) => v.original().hash(state),
        FlakeValue::GMonth(v) => v.original().hash(state),
        FlakeValue::GDay(v) => v.original().hash(state),
        FlakeValue::GMonthDay(v) => v.original().hash(state),
        FlakeValue::YearMonthDuration(v) => v.original().hash(state),
        FlakeValue::DayTimeDuration(v) => v.original().hash(state),
        FlakeValue::Duration(v) => v.original().hash(state),
        FlakeValue::String(v) => v.hash(state),
        FlakeValue::Vector(v) => {
            for item in v {
                item.to_bits().hash(state);
            }
        }
        FlakeValue::Json(v) => v.hash(state),
        FlakeValue::GeoPoint(v) => v.0.hash(state),
        FlakeValue::Null => {}
    }
}

// =============================================================================
// Arity Checking
// =============================================================================

/// Check that a function has the expected number of arguments
#[inline]
pub fn check_arity(args: &[Expression], expected: usize, fn_name: &str) -> Result<()> {
    if args.len() != expected {
        Err(QueryError::InvalidFilter(format!(
            "{} requires exactly {} argument{}",
            fn_name,
            expected,
            if expected == 1 { "" } else { "s" }
        )))
    } else {
        Ok(())
    }
}

/// Check that a function has at least the minimum number of arguments
#[inline]
pub fn check_min_arity(args: &[Expression], min: usize, fn_name: &str) -> Result<()> {
    if args.len() < min {
        Err(QueryError::InvalidFilter(format!(
            "{} requires at least {} argument{}",
            fn_name,
            min,
            if min == 1 { "" } else { "s" }
        )))
    } else {
        Ok(())
    }
}

// =============================================================================
// DateTime Parsing
// =============================================================================

/// Parse a datetime from a binding, respecting datatype
///
/// Returns None if not a datetime type or parse fails.
/// Handles xsd:dateTime, xsd:date, xsd:time, and calendar fragment types
/// (xsd:gYear, xsd:gYearMonth, xsd:gMonth, xsd:gDay, xsd:gMonthDay).
/// Fragment types are promoted to full DateTime with sensible defaults
/// for missing components (e.g., gYear -> Jan 1 at 00:00:00).
pub fn parse_datetime_from_binding(
    binding: &Binding,
    ctx: Option<&ExecutionContext<'_>>,
) -> Option<DateTime<FixedOffset>> {
    let datatypes = &*WELL_KNOWN_DATATYPES;

    match binding {
        Binding::Lit { val, dtc, .. } => {
            let dt = dtc.datatype();
            let is_datetime_type = *dt == datatypes.xsd_datetime
                || *dt == datatypes.xsd_date
                || *dt == datatypes.xsd_time
                || *dt == datatypes.xsd_g_year
                || *dt == datatypes.xsd_g_year_month
                || *dt == datatypes.xsd_g_month
                || *dt == datatypes.xsd_g_day
                || *dt == datatypes.xsd_g_month_day;

            if !is_datetime_type {
                return None;
            }

            flake_value_to_datetime(val, Some(dt), datatypes)
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } => {
            let ctx = ctx?;
            let store = ctx.binary_store.as_deref()?;
            let dt_sid = store.dt_sids().get(*dt_id as usize)?.clone();

            let is_datetime_type = dt_sid == datatypes.xsd_datetime
                || dt_sid == datatypes.xsd_date
                || dt_sid == datatypes.xsd_time
                || dt_sid == datatypes.xsd_g_year
                || dt_sid == datatypes.xsd_g_year_month
                || dt_sid == datatypes.xsd_g_month
                || dt_sid == datatypes.xsd_g_day
                || dt_sid == datatypes.xsd_g_month_day;
            if !is_datetime_type {
                return None;
            }

            let gv = ctx.graph_view()?;
            let val = gv
                .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                .ok()?;

            flake_value_to_datetime(&val, Some(&dt_sid), datatypes)
        }
        _ => None,
    }
}

/// Convert a FlakeValue to a DateTime, handling all XSD temporal types.
///
/// The `dt_sid` parameter is used only for the `FlakeValue::Long` fallback
/// (numeric gYear encoding); all other variants are self-describing.
fn flake_value_to_datetime(
    val: &FlakeValue,
    dt_sid: Option<&fluree_db_core::Sid>,
    datatypes: &WellKnownDatatypes,
) -> Option<DateTime<FixedOffset>> {
    let utc = FixedOffset::east_opt(0).unwrap();

    match val {
        FlakeValue::DateTime(dt) => {
            let offset = dt.tz_offset().unwrap_or(utc);
            Some(dt.instant().with_timezone(&offset))
        }
        FlakeValue::Date(d) => {
            let offset = d.tz_offset().unwrap_or(utc);
            let naive = d.date().and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::Time(t) => {
            let offset = t.tz_offset().unwrap_or(utc);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let naive = NaiveDateTime::new(date, t.time());
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GYear(gy) => {
            let offset = gy.tz_offset().unwrap_or(utc);
            let naive = NaiveDate::from_ymd_opt(gy.year(), 1, 1)?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GYearMonth(gym) => {
            let offset = gym.tz_offset().unwrap_or(utc);
            let naive =
                NaiveDate::from_ymd_opt(gym.year(), gym.month(), 1)?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GMonth(gm) => {
            let offset = gm.tz_offset().unwrap_or(utc);
            let naive = NaiveDate::from_ymd_opt(1970, gm.month(), 1)?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GDay(gd) => {
            let offset = gd.tz_offset().unwrap_or(utc);
            let naive = NaiveDate::from_ymd_opt(1970, 1, gd.day())?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GMonthDay(gmd) => {
            let offset = gmd.tz_offset().unwrap_or(utc);
            let naive =
                NaiveDate::from_ymd_opt(1970, gmd.month(), gmd.day())?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::String(s) => DateTime::parse_from_rfc3339(s).ok().or_else(|| {
            let with_time = format!("{s}T00:00:00+00:00");
            DateTime::parse_from_rfc3339(&with_time).ok()
        }),
        FlakeValue::Long(y) if dt_sid == Some(&datatypes.xsd_g_year) => {
            let year = i32::try_from(*y).ok()?;
            let naive = NaiveDate::from_ymd_opt(year, 1, 1)?.and_hms_opt(0, 0, 0)?;
            Some(
                utc.from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| utc.from_utc_datetime(&naive)),
            )
        }
        _ => None,
    }
}

/// Format a datatype Sid as a ComparableValue
///
/// Returns compact string representations for well-known datatypes.
pub fn format_datatype_sid(dt: &fluree_db_core::Sid) -> ComparableValue {
    let datatypes = &*WELL_KNOWN_DATATYPES;
    if *dt == datatypes.rdf_json {
        ComparableValue::String(Arc::from("@json"))
    } else if *dt == datatypes.id_type {
        ComparableValue::String(Arc::from("@id"))
    } else if dt.namespace_code == datatypes.xsd_string.namespace_code {
        ComparableValue::String(Arc::from(format!("xsd:{}", dt.name_str())))
    } else if dt.namespace_code == datatypes.rdf_json.namespace_code {
        ComparableValue::String(Arc::from(format!("rdf:{}", dt.name_str())))
    } else {
        ComparableValue::Sid(dt.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::FilterValue;

    #[test]
    fn cacheable_bool_predicate_accepts_regex_over_single_var() {
        let expr = Expression::call(
            Function::Regex,
            vec![
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::String("^crm:stage/".to_string())),
            ],
        );

        let spec = analyze_cacheable_bool_predicate(&expr).expect("regex should be cacheable");
        assert_eq!(spec.input_var, VarId(0));
    }

    #[test]
    fn cacheable_bool_predicate_accepts_not_strstarts_shape() {
        let expr = Expression::not(Expression::call(
            Function::StrStarts,
            vec![
                Expression::call(Function::Str, vec![Expression::Var(VarId(3))]),
                Expression::Const(FilterValue::String("Closed".to_string())),
            ],
        ));

        let spec = analyze_cacheable_bool_predicate(&expr).expect("NOT(STRSTARTS(STR(?v), const))");
        assert_eq!(spec.input_var, VarId(3));
    }

    #[test]
    fn cacheable_bool_predicate_rejects_two_var_comparisons() {
        let expr = Expression::eq(Expression::Var(VarId(0)), Expression::Var(VarId(1)));
        assert!(analyze_cacheable_bool_predicate(&expr).is_none());
    }

    #[test]
    fn cacheable_bool_predicate_rejects_low_value_bound_checks() {
        let expr = Expression::call(Function::Bound, vec![Expression::Var(VarId(0))]);
        assert!(analyze_cacheable_bool_predicate(&expr).is_none());
    }

    #[test]
    fn encoded_lit_gate_focuses_on_dictionary_backed_inputs() {
        assert!(encoded_lit_may_need_dictionary_lookup(
            ObjKind::LEX_ID.as_u8(),
            0,
            0
        ));
        assert!(encoded_lit_may_need_dictionary_lookup(
            ObjKind::NUM_INT.as_u8(),
            1,
            0
        ));
        assert!(!encoded_lit_may_need_dictionary_lookup(
            ObjKind::NUM_INT.as_u8(),
            0,
            0
        ));
    }
}
