//! Query parameter types shared across query backends.
//!
//! These types define the query interface (what to match, how to compare,
//! query options) and are independent of the underlying index implementation.

use std::sync::Arc;

use crate::o_type::OType;
use crate::sid::Sid;
use crate::temporal;
use crate::value::FlakeValue;

/// Comparison test operators for range queries
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeTest {
    /// Equal to (becomes >= and <=)
    Eq,
    /// Less than
    Lt,
    /// Less than or equal
    Le,
    /// Greater than
    Gt,
    /// Greater than or equal
    Ge,
}

/// Components to match in a range query
///
/// Use the builder methods to construct a match for specific components.
/// Unset components are wildcards (use min/max bounds).
#[derive(Clone, Debug, Default)]
pub struct RangeMatch {
    /// Subject to match
    pub s: Option<Sid>,
    /// Predicate to match
    pub p: Option<Sid>,
    /// Object to match
    pub o: Option<FlakeValue>,
    /// Datatype to match
    pub dt: Option<Sid>,
    /// Transaction time to match
    pub t: Option<i64>,
}

impl RangeMatch {
    /// Create an empty match (matches everything)
    pub fn new() -> Self {
        Self::default()
    }

    /// Match a specific subject
    pub fn subject(s: Sid) -> Self {
        Self {
            s: Some(s),
            ..Default::default()
        }
    }

    /// Match a specific subject and predicate
    pub fn subject_predicate(s: Sid, p: Sid) -> Self {
        Self {
            s: Some(s),
            p: Some(p),
            ..Default::default()
        }
    }

    /// Match a specific predicate
    pub fn predicate(p: Sid) -> Self {
        Self {
            p: Some(p),
            ..Default::default()
        }
    }

    /// Match a specific predicate and object
    pub fn predicate_object(p: Sid, o: FlakeValue) -> Self {
        Self {
            p: Some(p),
            o: Some(o),
            ..Default::default()
        }
    }

    /// Match a specific transaction time
    pub fn at_t(t: i64) -> Self {
        Self {
            t: Some(t),
            ..Default::default()
        }
    }

    /// Set subject
    pub fn with_subject(mut self, s: Sid) -> Self {
        self.s = Some(s);
        self
    }

    /// Set predicate
    pub fn with_predicate(mut self, p: Sid) -> Self {
        self.p = Some(p);
        self
    }

    /// Set object
    pub fn with_object(mut self, o: FlakeValue) -> Self {
        self.o = Some(o);
        self
    }

    /// Set datatype
    pub fn with_datatype(mut self, dt: Sid) -> Self {
        self.dt = Some(dt);
        self
    }

    /// Set transaction time
    pub fn with_t(mut self, t: i64) -> Self {
        self.t = Some(t);
        self
    }
}

/// Object value bounds for range filtering
///
/// Used for filter pushdown to narrow scan results based on object value comparisons.
/// Bounds are applied as a post-filter after the range scan.
///
/// A bound is an RDF term, not just a value, so that the pushed bounds keep
/// the semantics of the FILTER they replace. A string bound is the
/// `xsd:string` term with that lexical form: it admits `xsd:string` objects
/// only, never a language-tagged or otherwise-typed literal that shares the
/// text, as SPARQL's `=` and `<` find those unequal or incomparable. That is
/// why matching takes the stored object's datatype, as a `Sid`
/// ([`Self::matches`]) or a binary-index `o_type`
/// ([`Self::matches_encoded`]).
#[derive(Clone, Debug, Default)]
pub struct ObjectBounds {
    /// Lower bound: (value, inclusive)
    /// For `?x > 10`, use `(10, false)`. For `?x >= 10`, use `(10, true)`.
    pub lower: Option<(FlakeValue, bool)>,
    /// Upper bound: (value, inclusive)
    /// For `?x < 100`, use `(100, false)`. For `?x <= 100`, use `(100, true)`.
    pub upper: Option<(FlakeValue, bool)>,
    /// Whether a string bound orders a temporal object, and a temporal bound
    /// a string object, by parsing the string as that temporal type (a
    /// gYear stored as a string, a date compared with `"2026-01-01"`).
    /// SPARQL's `<`, `<=`, `>` and `>=` compare that way; `=` never equates
    /// a string with a temporal. So bounds that came from an `=` leave it
    /// off.
    pub temporal_string_ordering: bool,
}

/// The temporal `o_type`s a string can be parsed as, for
/// [`ObjectBounds::temporal_string_ordering`]. Durations have no total order
/// and are never coerced.
const STRING_ORDERED_TEMPORALS: [OType; 8] = [
    OType::XSD_DATE,
    OType::XSD_TIME,
    OType::XSD_DATE_TIME,
    OType::XSD_G_YEAR,
    OType::XSD_G_YEAR_MONTH,
    OType::XSD_G_MONTH,
    OType::XSD_G_DAY,
    OType::XSD_G_MONTH_DAY,
];

impl ObjectBounds {
    /// Create empty bounds (no filtering)
    pub fn new() -> Self {
        Self::default()
    }

    /// Set lower bound
    pub fn with_lower(mut self, value: FlakeValue, inclusive: bool) -> Self {
        self.lower = Some((value, inclusive));
        self
    }

    /// Set upper bound
    pub fn with_upper(mut self, value: FlakeValue, inclusive: bool) -> Self {
        self.upper = Some((value, inclusive));
        self
    }

    /// Whether a stored object, `value` with datatype `dt`, satisfies the
    /// bounds.
    ///
    /// Uses **type class comparison**:
    /// - All numeric types (Long, Double, BigInt, Decimal) are comparable to each other
    /// - Temporal types are only comparable within the same kind (Date vs Date, etc.)
    /// - A string bound compares only against an `xsd:string` object
    /// - Other types require exact type match
    #[inline]
    pub fn matches(&self, value: &FlakeValue, dt: &Sid) -> bool {
        self.matches_object(value, || crate::datatypes::is_xsd_string_datatype(dt))
    }

    /// [`Self::matches`] for a binary-index row, whose datatype is its
    /// `o_type`.
    #[inline]
    pub fn matches_encoded(&self, value: &FlakeValue, o_type: OType) -> bool {
        self.matches_object(value, || o_type == OType::XSD_STRING)
    }

    /// [`Self::matches`] for a number, whose datatype the bounds never
    /// consult: a numeric bound orders every numeric type by value, and a
    /// string bound admits no number. Anything else passed here is treated as
    /// a non-`xsd:string` object, so it can only fail a string bound.
    #[inline]
    pub fn matches_number(&self, value: &FlakeValue) -> bool {
        self.matches_object(value, || false)
    }

    #[inline]
    fn matches_object(&self, value: &FlakeValue, is_xsd_string: impl Fn() -> bool) -> bool {
        // Check lower bound
        if let Some((lower, inclusive)) = &self.lower {
            match self.class_cmp(value, lower, &is_xsd_string) {
                None => return false, // Incompatible types
                Some(std::cmp::Ordering::Less) => return false,
                Some(std::cmp::Ordering::Equal) if !inclusive => return false,
                _ => {}
            }
        }

        // Check upper bound
        if let Some((upper, inclusive)) = &self.upper {
            match self.class_cmp(value, upper, &is_xsd_string) {
                None => return false, // Incompatible types
                Some(std::cmp::Ordering::Greater) => return false,
                Some(std::cmp::Ordering::Equal) if !inclusive => return false,
                _ => {}
            }
        }

        true
    }

    /// Compare a stored value with a bound within their type class.
    ///
    /// Returns `Some(Ordering)` if the values are comparable, `None` if incompatible.
    ///
    /// - **Numeric class**: All numeric types are comparable (Long, Double, BigInt, Decimal)
    /// - **Temporal class**: Same temporal type only (Date vs Date, Time vs Time, DateTime vs DateTime)
    /// - **String**: an `xsd:string` value only (`is_xsd_string`)
    /// - **Same type**: Always comparable
    /// - **Different type classes**: Incompatible (returns None)
    #[inline]
    fn class_cmp(
        &self,
        value: &FlakeValue,
        bound: &FlakeValue,
        is_xsd_string: &impl Fn() -> bool,
    ) -> Option<std::cmp::Ordering> {
        // Numeric class: all numeric types are comparable
        if value.is_numeric() && bound.is_numeric() {
            return value.numeric_cmp(bound);
        }

        // String class, ahead of the rest as the common case for a string
        // bound: the bound is the `xsd:string` term, and a tagged or
        // otherwise-typed string shares its value but not its type. No
        // coercion applies between two strings.
        if let (FlakeValue::String(v), FlakeValue::String(b)) = (value, bound) {
            return is_xsd_string().then(|| v.as_str().cmp(b.as_str()));
        }

        // Temporal class: same temporal type only
        if value.is_temporal() && bound.is_temporal() {
            return value.temporal_cmp(bound);
        }

        // Cross-type: String vs temporal — try to parse string as the
        // temporal type. This handles values stored as LEX_ID (string
        // dict) with a temporal datatype annotation (e.g. gYear values
        // in bulk-imported data). Ordering only.
        if self.temporal_string_ordering {
            if let Some(ordering) = Self::try_coerce_temporal_string_cmp(value, bound) {
                return Some(ordering);
            }
        }

        // Same type: use standard comparison
        if std::mem::discriminant(value) == std::mem::discriminant(bound) {
            return Some(value.cmp(bound));
        }

        // Incompatible types
        None
    }

    /// The `o_type`s a stored object needs to satisfy these bounds when a
    /// bound is a string, or `None` when no bound is. Always `xsd:string`;
    /// with [`Self::temporal_string_ordering`], also each temporal type
    /// every string bound parses as. A binary-index scan seeks these slices
    /// rather than reading every type the predicate holds.
    pub fn string_bound_otypes(&self) -> Option<Vec<OType>> {
        let strings: Vec<&str> = [&self.lower, &self.upper]
            .into_iter()
            .flatten()
            .filter_map(|(v, _)| match v {
                FlakeValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        if strings.is_empty() {
            return None;
        }
        let mut otypes = vec![OType::XSD_STRING];
        if self.temporal_string_ordering {
            otypes.extend(
                STRING_ORDERED_TEMPORALS
                    .into_iter()
                    .filter(|ot| strings.iter().all(|s| parses_as_temporal(s, *ot))),
            );
        }
        Some(otypes)
    }

    /// Try to compare a temporal FlakeValue against a String by parsing the
    /// string as the matching temporal type.
    fn try_coerce_temporal_string_cmp(
        a: &FlakeValue,
        b: &FlakeValue,
    ) -> Option<std::cmp::Ordering> {
        match (a, b) {
            (FlakeValue::String(s), FlakeValue::GYear(g)) => {
                temporal::GYear::parse(s).ok().map(|p| p.cmp(g.as_ref()))
            }
            (FlakeValue::GYear(g), FlakeValue::String(s)) => {
                temporal::GYear::parse(s).ok().map(|p| g.as_ref().cmp(&p))
            }
            (FlakeValue::String(s), FlakeValue::GYearMonth(g)) => temporal::GYearMonth::parse(s)
                .ok()
                .map(|p| p.cmp(g.as_ref())),
            (FlakeValue::GYearMonth(g), FlakeValue::String(s)) => temporal::GYearMonth::parse(s)
                .ok()
                .map(|p| g.as_ref().cmp(&p)),
            (FlakeValue::String(s), FlakeValue::GMonth(g)) => {
                temporal::GMonth::parse(s).ok().map(|p| p.cmp(g.as_ref()))
            }
            (FlakeValue::GMonth(g), FlakeValue::String(s)) => {
                temporal::GMonth::parse(s).ok().map(|p| g.as_ref().cmp(&p))
            }
            (FlakeValue::String(s), FlakeValue::GDay(g)) => {
                temporal::GDay::parse(s).ok().map(|p| p.cmp(g.as_ref()))
            }
            (FlakeValue::GDay(g), FlakeValue::String(s)) => {
                temporal::GDay::parse(s).ok().map(|p| g.as_ref().cmp(&p))
            }
            (FlakeValue::String(s), FlakeValue::GMonthDay(g)) => temporal::GMonthDay::parse(s)
                .ok()
                .map(|p| p.cmp(g.as_ref())),
            (FlakeValue::GMonthDay(g), FlakeValue::String(s)) => temporal::GMonthDay::parse(s)
                .ok()
                .map(|p| g.as_ref().cmp(&p)),
            (FlakeValue::String(s), FlakeValue::DateTime(d)) => {
                temporal::DateTime::parse(s).ok().map(|p| p.cmp(d.as_ref()))
            }
            (FlakeValue::DateTime(d), FlakeValue::String(s)) => temporal::DateTime::parse(s)
                .ok()
                .map(|p| d.as_ref().cmp(&p)),
            (FlakeValue::String(s), FlakeValue::Date(d)) => {
                temporal::Date::parse(s).ok().map(|p| p.cmp(d.as_ref()))
            }
            (FlakeValue::Date(d), FlakeValue::String(s)) => {
                temporal::Date::parse(s).ok().map(|p| d.as_ref().cmp(&p))
            }
            (FlakeValue::String(s), FlakeValue::Time(t)) => {
                temporal::Time::parse(s).ok().map(|p| p.cmp(t.as_ref()))
            }
            (FlakeValue::Time(t), FlakeValue::String(s)) => {
                temporal::Time::parse(s).ok().map(|p| t.as_ref().cmp(&p))
            }
            _ => None,
        }
    }

    /// Returns true if no bounds are set
    pub fn is_empty(&self) -> bool {
        self.lower.is_none() && self.upper.is_none()
    }
}

/// Whether `s` parses as the temporal type `ot`, the test
/// [`ObjectBounds::try_coerce_temporal_string_cmp`] applies to a row of it.
fn parses_as_temporal(s: &str, ot: OType) -> bool {
    match ot {
        OType::XSD_DATE => temporal::Date::parse(s).is_ok(),
        OType::XSD_TIME => temporal::Time::parse(s).is_ok(),
        OType::XSD_DATE_TIME => temporal::DateTime::parse(s).is_ok(),
        OType::XSD_G_YEAR => temporal::GYear::parse(s).is_ok(),
        OType::XSD_G_YEAR_MONTH => temporal::GYearMonth::parse(s).is_ok(),
        OType::XSD_G_MONTH => temporal::GMonth::parse(s).is_ok(),
        OType::XSD_G_DAY => temporal::GDay::parse(s).is_ok(),
        OType::XSD_G_MONTH_DAY => temporal::GMonthDay::parse(s).is_ok(),
        _ => false,
    }
}

/// Options for range query execution
#[derive(Clone, Debug, Default)]
pub struct RangeOptions {
    /// Maximum number of subjects to return
    pub limit: Option<usize>,
    /// Number of subjects to skip
    pub offset: Option<usize>,
    /// Maximum number of flakes to return
    pub flake_limit: Option<usize>,
    /// "As-of" time - only include flakes where t <= to_t
    /// If None, uses the database's current t
    pub to_t: Option<i64>,
    /// Start time for history queries - only include flakes where t >= from_t
    /// Used together with to_t for time-range queries
    pub from_t: Option<i64>,
    /// Optional object value bounds (for filter pushdown)
    pub object_bounds: Option<ObjectBounds>,
    /// History mode: when true, skip stale removal to return all flakes
    /// including retractions. Used by history queries to show full history.
    pub history_mode: bool,
    /// Number of leaves to prefetch ahead during traversal.
    /// Set to 0 to disable prefetch. Default is 3.
    /// Prefetch overlaps I/O with processing to reduce cold query latency.
    pub prefetch_n: Option<usize>,
    /// Optional forward-predicate allow-list (by `Sid`).
    ///
    /// When set, the range provider drops rows whose predicate is not in the
    /// list **before** decoding the object value, resolving the subject Sid,
    /// or charging dict-touch fuel — i.e., it converts a single SPOT(s,*,*)
    /// scan into a selective subject crawl without forcing per-predicate
    /// probes. None preserves the previous behavior (decode every row).
    ///
    /// Hint to providers; cursor work is unchanged (one bounded scan over
    /// the subject's range). Used by graph-crawl hydration when the
    /// projection lists explicit forward predicates (non-wildcard).
    /// Use `Arc<[Sid]>` so the same allow-list is shared cheaply across the
    /// N per-subject range calls a single hydration emits.
    pub predicate_filter: Option<Arc<[Sid]>>,
}

impl RangeOptions {
    /// Create default options (no limits)
    pub fn new() -> Self {
        Self::default()
    }

    /// Set subject limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set subject offset
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set flake limit
    pub fn with_flake_limit(mut self, flake_limit: usize) -> Self {
        self.flake_limit = Some(flake_limit);
        self
    }

    /// Set "as-of" time for time travel queries
    ///
    /// Only flakes with t <= to_t will be included in results.
    pub fn with_to_t(mut self, to_t: i64) -> Self {
        self.to_t = Some(to_t);
        self
    }

    /// Set start time for history queries
    ///
    /// Only flakes with t >= from_t will be included.
    /// Use together with `with_to_t` for time-range queries.
    pub fn with_from_t(mut self, from_t: i64) -> Self {
        self.from_t = Some(from_t);
        self
    }

    /// Set both from_t and to_t for a time range query
    pub fn with_time_range(mut self, from_t: i64, to_t: i64) -> Self {
        self.from_t = Some(from_t);
        self.to_t = Some(to_t);
        self
    }

    /// Set object value bounds for filter pushdown
    ///
    /// Bounds are applied as a post-filter after the range scan, retaining only
    /// flakes whose object value falls within the specified range.
    pub fn with_object_bounds(mut self, bounds: ObjectBounds) -> Self {
        self.object_bounds = Some(bounds);
        self
    }

    /// Enable history mode
    ///
    /// When enabled, skips stale removal to return all flakes including
    /// retractions. Used by history queries to show the full history of changes.
    pub fn with_history_mode(mut self) -> Self {
        self.history_mode = true;
        self
    }

    /// Set the number of leaves to prefetch ahead during traversal
    ///
    /// Prefetch overlaps I/O with processing - while one leaf is being processed,
    /// the next N leaves are loaded in parallel. This significantly reduces cold
    /// query latency.
    ///
    /// Set to 0 to disable prefetch. Default is 3 (if None).
    pub fn with_prefetch_n(mut self, n: usize) -> Self {
        self.prefetch_n = Some(n);
        self
    }

    /// Disable prefetch
    pub fn without_prefetch(mut self) -> Self {
        self.prefetch_n = Some(0);
        self
    }

    /// Set the forward-predicate allow-list.
    ///
    /// See [`RangeOptions::predicate_filter`] for semantics. Callers should
    /// keep the slice small (typical hydration projections: <16 entries) and
    /// share the `Arc<[Sid]>` across all the range calls a single subject
    /// crawl emits.
    pub fn with_predicate_filter(mut self, preds: Arc<[Sid]>) -> Self {
        self.predicate_filter = Some(preds);
        self
    }
}
