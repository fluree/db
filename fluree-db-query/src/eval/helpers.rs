//! Shared helper functions for filter evaluation
//!
//! Contains arity checks, regex caching, datetime parsing, and other utilities.

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::context::WellKnownDatatypes;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone};
use fluree_db_core::temporal::CalendarField;
use fluree_db_core::{FlakeValue, ObjKind};
use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
/// Supported flags: i (case-insensitive), m (multiline), s (dot-all),
/// x (ignore whitespace), q (literal pattern — all metacharacters escaped,
/// XPath `fn:matches`). Per XPath F&O §5.6.2, `q` used together with `m`,
/// `s`, or `x` renders those flags no-ops; `q` composes with `i`.
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

    // Validate flags before compiling; `q` escapes the whole pattern.
    let literal = flags.contains('q');
    for flag in flags.chars() {
        match flag {
            'i' | 'm' | 's' | 'x' | 'q' => {}
            c => {
                return Err(QueryError::InvalidFilter(format!(
                    "Unknown regex flag: '{c}'"
                )));
            }
        }
    }

    let escaped;
    let effective_pattern = if literal {
        escaped = regex::escape(pattern);
        &escaped
    } else {
        pattern
    };

    // Not in cache - compile and store
    let mut builder = RegexBuilder::new(effective_pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            // `q` neutralizes m/s/x (they'd have no meaning against an
            // escaped literal anyway — x would strip literal whitespace).
            'm' if !literal => {
                builder.multi_line(true);
            }
            's' if !literal => {
                builder.dot_matches_new_line(true);
            }
            'x' if !literal => {
                builder.ignore_whitespace(true);
            }
            _ => {}
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

/// Cheap, hash-free variable-usage walk used to pre-filter cacheable predicates.
///
/// The encoded-bool-predicate cache only applies to single-variable predicates,
/// but `analyze_bool_cache` computes a SipHash of the whole expression tree as it
/// walks. For the common multi-variable predicate (e.g. `?a < ?b + k`) that hash
/// is pure waste — recomputed per row only to be discarded. This walk decides
/// single-vs-multi without hashing and short-circuits on the second distinct var.
fn bool_predicate_var_usage(expr: &Expression) -> VarUsage {
    match expr {
        Expression::Var(v) => VarUsage::Single(*v),
        Expression::Const(_) | Expression::Exists { .. } => VarUsage::None,
        Expression::Call { args, .. } => {
            let mut vars = VarUsage::None;
            for arg in args {
                vars = merge_var_usage(vars, bool_predicate_var_usage(arg));
                if matches!(vars, VarUsage::Multiple) {
                    return VarUsage::Multiple;
                }
            }
            vars
        }
        Expression::Map(entries) => {
            let mut vars = VarUsage::None;
            for (_, v) in entries {
                vars = merge_var_usage(vars, bool_predicate_var_usage(v));
                if matches!(vars, VarUsage::Multiple) {
                    return VarUsage::Multiple;
                }
            }
            vars
        }
        // Scoped iteration / member access aren't single-var bool predicates;
        // mark Multiple so the single-var bool cache declines them (safe).
        Expression::ListComprehension { .. }
        | Expression::Reduce { .. }
        | Expression::ListPredicate { .. }
        | Expression::Member { .. }
        | Expression::PatternComprehension { .. }
        | Expression::Resolved(_) => VarUsage::Multiple,
    }
}

fn analyze_cacheable_bool_predicate(expr: &Expression) -> Option<CacheableBoolPredicate> {
    // Bail before the (SipHash) structural+hash walk when the predicate isn't a
    // single-variable shape — the only shape the cache can serve.
    if !matches!(bool_predicate_var_usage(expr), VarUsage::Single(_)) {
        return None;
    }
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
            hash_flake_value(value, state);
            BoolCacheAnalysis {
                expr_hash: state.finish(),
                vars: VarUsage::None,
                supported: true,
                returns_bool: false,
                may_materialize: false,
            }
        }
        Expression::Exists { .. }
        | Expression::Map(_)
        | Expression::ListComprehension { .. }
        | Expression::Reduce { .. }
        | Expression::ListPredicate { .. }
        | Expression::Member { .. }
        | Expression::PatternComprehension { .. }
        | Expression::Resolved(_) => BoolCacheAnalysis {
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
            | Function::Today
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
        Function::And | Function::Or | Function::Not | Function::Xor => all_children_return_bool,
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
            | Function::Lang { .. }
            | Function::Lcase
            | Function::Ucase
            | Function::Strlen
            | Function::Concat
            | Function::StrBefore
            | Function::StrAfter
            | Function::Replace
            | Function::ReplaceAll
            | Function::Split
            | Function::Trim
            | Function::LTrim
            | Function::RTrim
            | Function::Left
            | Function::Right
            | Function::Substr
            | Function::EncodeForUri
            | Function::StrDt
            | Function::StrLang
            | Function::Datatype { .. }
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
            | Function::XsdDateTime
            | Function::XsdDate
            | Function::XsdTime
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
        FlakeValue::DateTime(v) => v.hash(state),
        FlakeValue::Date(v) => v.hash(state),
        FlakeValue::Time(v) => v.hash(state),
        FlakeValue::GYear(v) => v.hash(state),
        FlakeValue::GYearMonth(v) => v.hash(state),
        FlakeValue::GMonth(v) => v.hash(state),
        FlakeValue::GDay(v) => v.hash(state),
        FlakeValue::GMonthDay(v) => v.hash(state),
        FlakeValue::YearMonthDuration(v) => v.original().hash(state),
        FlakeValue::DayTimeDuration(v) => v.original().hash(state),
        FlakeValue::Duration(v) => v.original().hash(state),
        FlakeValue::String(v) => v.hash(state),
        FlakeValue::Vector(v) => {
            for item in v.iter() {
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

/// Whether `dt` is one of the datatypes the temporal readers accept.
fn is_temporal_datatype(dt: &fluree_db_core::Sid, datatypes: &WellKnownDatatypes) -> bool {
    *dt == datatypes.xsd_datetime
        || *dt == datatypes.xsd_date
        || *dt == datatypes.xsd_time
        || *dt == datatypes.xsd_g_year
        || *dt == datatypes.xsd_g_year_month
        || *dt == datatypes.xsd_g_month
        || *dt == datatypes.xsd_g_day
        || *dt == datatypes.xsd_g_month_day
}

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
            if !is_temporal_datatype(dt, datatypes) {
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
            if !is_temporal_datatype(&dt_sid, datatypes) {
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

/// Whether this binding holds a temporal value — the only question `TZ` and
/// `TIMEZONE` need to answer.
///
/// Deliberately inspects the datatype alone and never decodes the value: Fluree
/// normalizes temporals to UTC and does not persist the source offset, so the
/// answer cannot depend on the value. Skipping the decode also keeps `TZ` cheap,
/// since it is evaluated per row.
pub fn binding_is_temporal(binding: &Binding, ctx: Option<&ExecutionContext<'_>>) -> bool {
    let datatypes = &*WELL_KNOWN_DATATYPES;
    match binding {
        Binding::Lit { dtc, .. } => is_temporal_datatype(dtc.datatype(), datatypes),
        Binding::EncodedLit { dt_id, .. } => ctx
            .and_then(|c| c.binary_store.as_deref())
            .and_then(|store| store.dt_sids().get(*dt_id as usize))
            .is_some_and(|dt_sid| is_temporal_datatype(dt_sid, datatypes)),
        _ => false,
    }
}

/// Promote a calendar fragment (`xsd:gYear` and friends) to a whole instant,
/// filling the fields it does not carry from [`CalendarField::promotion_default`].
///
/// The filler is for whole-value uses only — comparison, ordering,
/// `TZ`/`TIMEZONE`. Reading a field back off the result is only valid once
/// [`TemporalKind::carries`] says the value carries it; the SPARQL accessors
/// check that first (see `eval::datetime`), because reporting a filled field as
/// data is exactly what made `DAY("2005"^^xsd:gYear)` answer 1.
/// The date a value carrying no date at all promotes to, from the shared
/// defaults: the Unix epoch's 1970-01-01.
fn promotion_default_date() -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(
        i32::try_from(CalendarField::Year.promotion_default()).ok()?,
        u32::try_from(CalendarField::Month.promotion_default()).ok()?,
        u32::try_from(CalendarField::Day.promotion_default()).ok()?,
    )
}

/// Promote a gYear-family fragment to a full instant by filling the missing
/// fields from the shared defaults. Always UTC — these types carry no timezone
/// (see `fluree_db_core::temporal`).
fn promote_calendar_fragment(
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
) -> Option<DateTime<FixedOffset>> {
    let offset = FixedOffset::east_opt(0)?;
    let naive = NaiveDate::from_ymd_opt(
        year.unwrap_or(i32::try_from(CalendarField::Year.promotion_default()).ok()?),
        month.unwrap_or(u32::try_from(CalendarField::Month.promotion_default()).ok()?),
        day.unwrap_or(u32::try_from(CalendarField::Day.promotion_default()).ok()?),
    )?
    .and_hms_opt(
        u32::try_from(CalendarField::Hour.promotion_default()).ok()?,
        u32::try_from(CalendarField::Minute.promotion_default()).ok()?,
        u32::try_from(CalendarField::Second.promotion_default()).ok()?,
    )?;
    Some(
        offset
            .from_local_datetime(&naive)
            .single()
            .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
    )
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
        // Temporal values carry no offset (see fluree_db_core::temporal), so
        // every component is read in UTC on both storage lanes.
        FlakeValue::DateTime(dt) => Some(dt.instant().with_timezone(&utc)),
        FlakeValue::Date(d) => Some(utc.from_utc_datetime(&d.date().and_hms_opt(0, 0, 0)?)),
        FlakeValue::Time(t) => {
            // A time carries no date; fill it from the shared defaults.
            let date = promotion_default_date()?;
            Some(utc.from_utc_datetime(&NaiveDateTime::new(date, t.time())))
        }
        FlakeValue::GYear(gy) => promote_calendar_fragment(Some(gy.year()), None, None),
        FlakeValue::GYearMonth(gym) => {
            promote_calendar_fragment(Some(gym.year()), Some(gym.month()), None)
        }
        FlakeValue::GMonth(gm) => promote_calendar_fragment(None, Some(gm.month()), None),
        FlakeValue::GDay(gd) => promote_calendar_fragment(None, None, Some(gd.day())),
        FlakeValue::GMonthDay(gmd) => {
            promote_calendar_fragment(None, Some(gmd.month()), Some(gmd.day()))
        }
        FlakeValue::String(s) => DateTime::parse_from_rfc3339(s).ok().or_else(|| {
            let with_time = format!("{s}T00:00:00+00:00");
            DateTime::parse_from_rfc3339(&with_time).ok()
        }),
        FlakeValue::Long(y) if dt_sid == Some(&datatypes.xsd_g_year) => {
            promote_calendar_fragment(Some(i32::try_from(*y).ok()?), None, None)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::value::FlakeValue;

    #[test]
    fn cacheable_bool_predicate_accepts_regex_over_single_var() {
        let expr = Expression::call(
            Function::Regex,
            vec![
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::String("^crm:stage/".to_string())),
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
                Expression::Const(FlakeValue::String("Closed".to_string())),
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
