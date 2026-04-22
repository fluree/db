//! Query parameter types shared across query backends.
//!
//! These types define the query interface (what to match, how to compare,
//! query options) and are independent of the underlying index implementation.

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
#[derive(Clone, Debug, Default)]
pub struct ObjectBounds {
    /// Lower bound: (value, inclusive)
    /// For `?x > 10`, use `(10, false)`. For `?x >= 10`, use `(10, true)`.
    pub lower: Option<(FlakeValue, bool)>,
    /// Upper bound: (value, inclusive)
    /// For `?x < 100`, use `(100, false)`. For `?x <= 100`, use `(100, true)`.
    pub upper: Option<(FlakeValue, bool)>,
}

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

    /// Check if a value satisfies the bounds
    ///
    /// Uses **type class comparison**:
    /// - All numeric types (Long, Double, BigInt, Decimal) are comparable to each other
    /// - Temporal types are only comparable within the same kind (Date vs Date, etc.)
    /// - Other types require exact type match
    pub fn matches(&self, value: &FlakeValue) -> bool {
        // Check lower bound
        if let Some((lower, inclusive)) = &self.lower {
            match Self::class_cmp(value, lower) {
                None => return false, // Incompatible types
                Some(std::cmp::Ordering::Less) => return false,
                Some(std::cmp::Ordering::Equal) if !inclusive => return false,
                _ => {}
            }
        }

        // Check upper bound
        if let Some((upper, inclusive)) = &self.upper {
            match Self::class_cmp(value, upper) {
                None => return false, // Incompatible types
                Some(std::cmp::Ordering::Greater) => return false,
                Some(std::cmp::Ordering::Equal) if !inclusive => return false,
                _ => {}
            }
        }

        true
    }

    /// Compare values within their type class.
    ///
    /// Returns `Some(Ordering)` if the values are comparable, `None` if incompatible.
    ///
    /// - **Numeric class**: All numeric types are comparable (Long, Double, BigInt, Decimal)
    /// - **Temporal class**: Same temporal type only (Date vs Date, Time vs Time, DateTime vs DateTime)
    /// - **Same type**: Always comparable
    /// - **Different type classes**: Incompatible (returns None)
    fn class_cmp(a: &FlakeValue, b: &FlakeValue) -> Option<std::cmp::Ordering> {
        // Numeric class: all numeric types are comparable
        if a.is_numeric() && b.is_numeric() {
            return a.numeric_cmp(b);
        }

        // Temporal class: same temporal type only
        if a.is_temporal() && b.is_temporal() {
            return a.temporal_cmp(b);
        }

        // Cross-type: String vs temporal — try to parse string as the
        // temporal type. This handles values stored as LEX_ID (string
        // dict) with a temporal datatype annotation (e.g. gYear values
        // in bulk-imported data).
        if let Some(ordering) = Self::try_coerce_temporal_string_cmp(a, b) {
            return Some(ordering);
        }

        // Same type: use standard comparison
        if std::mem::discriminant(a) == std::mem::discriminant(b) {
            return Some(a.cmp(b));
        }

        // Incompatible types
        None
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
}
