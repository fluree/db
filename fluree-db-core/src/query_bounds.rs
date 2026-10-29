//! Query parameter types shared across query backends.
//!
//! These types define the query interface (what to match, how to compare,
//! query options) and are independent of the underlying index implementation.

use std::cell::OnceCell;
use std::sync::{Arc, LazyLock};

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
    /// an `xsd:string` object, by reading the string as a value of that
    /// temporal type (a date compared with `"2026-01-01"`), as the FILTER
    /// evaluator's `<`, `<=`, `>` and `>=` do; `=` never equates a string
    /// with a temporal. So bounds that came from an `=` leave it off. A
    /// tagged or otherwise-typed string is never read as a temporal.
    pub temporal_string_ordering: bool,
}

/// A value of each temporal type a string bound can order against, keyed by
/// its `o_type`. [`ObjectBounds::string_bound_otypes`] asks
/// [`temporal::cmp_with_lexical`] itself whether a string reaches rows of
/// each type, so the slices a scan seeks cannot drift from the comparison
/// the rows then get. Durations have no total order and are never coerced.
static TEMPORAL_SAMPLES: LazyLock<Vec<(OType, FlakeValue)>> = LazyLock::new(|| {
    vec![
        (
            OType::XSD_DATE,
            FlakeValue::Date(Box::new(temporal::Date::parse("2000-01-01").unwrap())),
        ),
        (
            OType::XSD_TIME,
            FlakeValue::Time(Box::new(temporal::Time::parse("00:00:00").unwrap())),
        ),
        (
            OType::XSD_DATE_TIME,
            FlakeValue::DateTime(Box::new(
                temporal::DateTime::parse("2000-01-01T00:00:00Z").unwrap(),
            )),
        ),
        (
            OType::XSD_G_YEAR,
            FlakeValue::GYear(Box::new(temporal::GYear::parse("2000").unwrap())),
        ),
        (
            OType::XSD_G_YEAR_MONTH,
            FlakeValue::GYearMonth(Box::new(temporal::GYearMonth::parse("2000-01").unwrap())),
        ),
        (
            OType::XSD_G_MONTH,
            FlakeValue::GMonth(Box::new(temporal::GMonth::parse("--01").unwrap())),
        ),
        (
            OType::XSD_G_DAY,
            FlakeValue::GDay(Box::new(temporal::GDay::parse("---01").unwrap())),
        ),
        (
            OType::XSD_G_MONTH_DAY,
            FlakeValue::GMonthDay(Box::new(temporal::GMonthDay::parse("--01-01").unwrap())),
        ),
    ]
});

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
    ///
    /// To test many objects, [`Self::prepare`] the bounds once.
    #[inline]
    pub fn matches(&self, value: &FlakeValue, dt: &Sid) -> bool {
        self.matches_object(value, || crate::datatypes::is_xsd_string_datatype(dt), None)
    }

    /// [`Self::matches`] for a binary-index row, whose datatype is its
    /// `o_type`.
    #[inline]
    pub fn matches_encoded(&self, value: &FlakeValue, o_type: OType) -> bool {
        self.matches_object(value, || o_type == OType::XSD_STRING, None)
    }

    /// [`Self::matches`] for a number, whose datatype the bounds never
    /// consult: a numeric bound orders every numeric type by value, and a
    /// string bound admits no number. Anything else passed here is treated as
    /// a non-`xsd:string` object, so it can only fail a string bound.
    #[inline]
    pub fn matches_number(&self, value: &FlakeValue) -> bool {
        self.matches_object(value, || false, None)
    }

    /// The bounds set up to test many objects: a string bound that orders a
    /// temporal object is read as each temporal kind once, not once per
    /// object.
    pub fn prepare(&self) -> PreparedBounds<'_> {
        PreparedBounds {
            bounds: self,
            lexicals: Default::default(),
        }
    }

    // Forced: under fat LTO the generic body otherwise stays a call per
    // bound, which cost a numeric range scan ~2% (release A/B).
    #[inline(always)]
    fn matches_object(
        &self,
        value: &FlakeValue,
        is_xsd_string: impl Fn() -> bool,
        lexicals: Option<&BoundLexicals>,
    ) -> bool {
        // Check lower bound
        if let Some((lower, inclusive)) = &self.lower {
            match self.class_cmp(value, lower, &is_xsd_string, lexicals.map(|l| &l[0])) {
                None => return false, // Incompatible types
                Some(std::cmp::Ordering::Less) => return false,
                Some(std::cmp::Ordering::Equal) if !inclusive => return false,
                _ => {}
            }
        }

        // Check upper bound
        if let Some((upper, inclusive)) = &self.upper {
            match self.class_cmp(value, upper, &is_xsd_string, lexicals.map(|l| &l[1])) {
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
    #[inline(always)]
    fn class_cmp(
        &self,
        value: &FlakeValue,
        bound: &FlakeValue,
        is_xsd_string: &impl Fn() -> bool,
        lexicals: Option<&[OnceCell<Option<FlakeValue>>; TEMPORAL_KINDS]>,
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

        // Cross-type: a plain string vs a temporal, the string read as a
        // value of that temporal type. Ordering only. A string bound is
        // always the `xsd:string` term; a stored string must be one too, as
        // a tagged or otherwise-typed literal is a term of its own type.
        let plain = || !matches!(value, FlakeValue::String(_)) || is_xsd_string();
        if self.temporal_string_ordering && plain() {
            let ordering = match (lexicals, bound) {
                // A temporal object against a string bound: the bound read as
                // the object's kind, parsed on the kind's first object.
                (Some(cells), FlakeValue::String(lexical)) => {
                    temporal_kind(value).and_then(|kind| {
                        cells[kind]
                            .get_or_init(|| temporal::lexical_as_kind_of(value, lexical))
                            .as_ref()
                            .and_then(|parsed| value.temporal_cmp(parsed))
                    })
                }
                _ => temporal::cmp_with_lexical(value, bound),
            };
            if let Some(ordering) = ordering {
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
        let strings: Vec<&FlakeValue> = [&self.lower, &self.upper]
            .into_iter()
            .flatten()
            .map(|(v, _)| v)
            .filter(|v| matches!(v, FlakeValue::String(_)))
            .collect();
        if strings.is_empty() {
            return None;
        }
        let mut otypes = vec![OType::XSD_STRING];
        if self.temporal_string_ordering {
            otypes.extend(
                TEMPORAL_SAMPLES
                    .iter()
                    .filter(|(_, sample)| {
                        strings
                            .iter()
                            .all(|s| temporal::cmp_with_lexical(sample, s).is_some())
                    })
                    .map(|(ot, _)| *ot),
            );
        }
        Some(otypes)
    }

    /// Returns true if no bounds are set
    pub fn is_empty(&self) -> bool {
        self.lower.is_none() && self.upper.is_none()
    }
}

/// The temporal kinds a string can be read as: `DateTime`, `Date`, `Time`
/// and the five gregorian ones.
const TEMPORAL_KINDS: usize = 8;

/// A string bound read as each temporal kind, `[lower, upper][kind]`.
type BoundLexicals = [[OnceCell<Option<FlakeValue>>; TEMPORAL_KINDS]; 2];

/// Index of a temporal value's kind in [`BoundLexicals`].
fn temporal_kind(value: &FlakeValue) -> Option<usize> {
    Some(match value {
        FlakeValue::DateTime(_) => 0,
        FlakeValue::Date(_) => 1,
        FlakeValue::Time(_) => 2,
        FlakeValue::GYear(_) => 3,
        FlakeValue::GYearMonth(_) => 4,
        FlakeValue::GMonth(_) => 5,
        FlakeValue::GDay(_) => 6,
        FlakeValue::GMonthDay(_) => 7,
        _ => return None,
    })
}

/// [`ObjectBounds`] ready to test many objects ([`ObjectBounds::prepare`]):
/// it answers exactly as the bounds do, reading a string bound as each
/// temporal kind once ([`temporal::lexical_as_kind_of`]) instead of once per
/// temporal object.
pub struct PreparedBounds<'a> {
    bounds: &'a ObjectBounds,
    lexicals: BoundLexicals,
}

impl PreparedBounds<'_> {
    /// [`ObjectBounds::matches`].
    #[inline]
    pub fn matches(&self, value: &FlakeValue, dt: &Sid) -> bool {
        self.bounds.matches_object(
            value,
            || crate::datatypes::is_xsd_string_datatype(dt),
            Some(&self.lexicals),
        )
    }

    /// [`ObjectBounds::matches_encoded`].
    #[inline]
    pub fn matches_encoded(&self, value: &FlakeValue, o_type: OType) -> bool {
        self.bounds
            .matches_object(value, || o_type == OType::XSD_STRING, Some(&self.lexicals))
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
