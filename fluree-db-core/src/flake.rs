//! Flake - the fundamental data unit in Fluree
//!
//! A Flake represents a single fact (assertion or retraction) with 7 components:
//! - `s`: Subject ID
//! - `p`: Predicate ID
//! - `o`: Object value
//! - `dt`: Datatype ID
//! - `t`: Transaction time
//! - `op`: Operation (true = assert, false = retract)
//! - `m`: Metadata (optional)
//!
//! ## Ordering
//!
//! Flakes don't implement `Ord` directly because ordering depends on the index type.
//! Use the comparator functions in the `comparator` module for index-specific ordering.
//
// ## Sentinels
//
// `Flake::min_for_*` and `Flake::max_for_*` provide bounds for wildcard queries
// on specific index types.

use crate::sid::Sid;
use crate::value::FlakeValue;
use fluree_vocab::namespaces::JSON_LD;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

/// Flake metadata - contains language tags, list indices, etc.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlakeMeta {
    /// Language tag for langString values (e.g., "en", "fr")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lang: Option<String>,
    /// Index position for list items
    #[serde(skip_serializing_if = "Option::is_none")]
    pub i: Option<i32>,
}

/// Canonical form of a BCP 47 language tag: ASCII-lowercased. RDF 1.1 defines
/// the value space of language tags as lowercase and compares them
/// case-insensitively, so `"x"@EN` and `"x"@en` are one term. Every tag that
/// enters a [`FlakeMeta`], a language dictionary, or a query constraint goes
/// through here so a single lexical form is stored and looked up.
pub fn normalize_lang_tag(tag: &str) -> std::borrow::Cow<'_, str> {
    if tag.bytes().any(|b| b.is_ascii_uppercase()) {
        std::borrow::Cow::Owned(tag.to_ascii_lowercase())
    } else {
        std::borrow::Cow::Borrowed(tag)
    }
}

impl FlakeMeta {
    /// Create empty metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Create metadata with language tag
    pub fn with_lang(lang: impl Into<String>) -> Self {
        Self {
            lang: Some(normalize_lang_tag(&lang.into()).into_owned()),
            i: None,
        }
    }

    /// Metadata for a value with an optional language tag and/or list
    /// position; the tag is normalized like [`with_lang`](Self::with_lang).
    pub fn from_parts(lang: Option<&str>, i: Option<i32>) -> Option<Self> {
        match (lang, i) {
            (None, None) => None,
            (lang, i) => Some(Self {
                lang: lang.map(|l| normalize_lang_tag(l).into_owned()),
                i,
            }),
        }
    }

    /// Create metadata with list index
    pub fn with_index(i: i32) -> Self {
        Self {
            lang: None,
            i: Some(i),
        }
    }

    /// Minimum metadata for range bounds
    pub fn min() -> Self {
        Self {
            lang: None,
            i: Some(i32::MIN),
        }
    }

    /// Maximum metadata for range bounds
    ///
    /// This is an upper bound for the metadata a query can reach, **not** a
    /// maximum over the whole type: `{lang: Some(_), i: Some(i32::MAX)}`
    /// sorts strictly above it, because [`Ord`] breaks an `i` tie on `lang`
    /// and `None < Some`. An inclusive upper bound built from this therefore
    /// excludes a language-tagged value sitting at list index `i32::MAX`.
    ///
    /// Unreachable through every bound builder in the tree — [`Flake::max_spot`],
    /// [`Flake::max_for_subject`], [`Flake::max_for_subject_predicate`] and
    /// [`Flake::max_for_predicate`] here, plus `predicate_walk_bounds` and
    /// `overlay_walk_bounds` in `fluree-db-query`. What guards them is `t`: all
    /// six pin `t` to `i64::MAX` and `op` to `true`, both compared before the
    /// metadata tiebreak in all four comparators, and no real flake carries
    /// `t == i64::MAX`. The `o`/`dt` maxima are incidental and are *not*
    /// universal — `overlay_walk_bounds` pins `o` to the pattern's bound object
    /// and `dt` to `Sid::max()` when the pattern has one. A new bound builder
    /// that does not pin `t` to `i64::MAX` would reach the narrowing, so do not
    /// assume this value dominates every [`FlakeMeta`].
    pub fn max() -> Self {
        Self {
            lang: None,
            i: Some(i32::MAX),
        }
    }
}

impl PartialOrd for FlakeMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(std::cmp::Ord::cmp(self, other))
    }
}

impl Ord for FlakeMeta {
    /// Compare metadata for ordering
    ///
    /// List index first — it is the discriminator a `@list` range walks —
    /// then the language tag as a tiebreak. `None` sorts before `Some` on
    /// both fields.
    ///
    /// The tiebreak is what makes this relation agree with the derived
    /// [`Eq`]: `cmp(a, b) == Equal` if and only if `a == b`. Without it,
    /// `{lang: Some("en"), i: Some(0)}` and `{lang: Some("fr"), i: Some(0)}`
    /// compared `Equal` while being unequal, and every caller that expressed
    /// identity through this ordering — an `OrdMap` key, a `cmp(..) == Equal`
    /// test, an exclusive `partition_point` seek — folded the two distinct
    /// facts into one.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.i
            .cmp(&other.i)
            .then_with(|| self.lang.cmp(&other.lang))
    }
}

/// A Flake represents a single fact in the database
///
/// The 7 components are:
/// - `g`: Graph (optional: named graph for this fact, None = default graph)
/// - `s`: Subject (who/what the fact is about)
/// - `p`: Predicate (the property/relationship)
/// - `o`: Object (the value)
/// - `dt`: Datatype (type of the object value)
/// - `t`: Transaction time (when asserted/retracted)
/// - `op`: Operation (true = assert, false = retract)
/// - `m`: Metadata (optional: language tags, list indices)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Flake {
    /// Graph ID (optional). None = default graph.
    /// When Some, the Sid identifies the named graph IRI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub g: Option<Sid>,
    /// Subject ID
    pub s: Sid,
    /// Predicate ID
    pub p: Sid,
    /// Object value
    pub o: FlakeValue,
    /// Datatype ID (e.g., xsd:string, xsd:long, or $id for references)
    pub dt: Sid,
    /// Transaction time
    pub t: i64,
    /// Operation: true = assert, false = retract
    pub op: bool,
    /// Metadata (optional)
    pub m: Option<FlakeMeta>,
}

impl Flake {
    /// Create a new Flake in the default graph
    pub fn new(
        s: Sid,
        p: Sid,
        o: FlakeValue,
        dt: Sid,
        t: i64,
        op: bool,
        m: Option<FlakeMeta>,
    ) -> Self {
        Self {
            g: None,
            s,
            p,
            o,
            dt,
            t,
            op,
            m,
        }
    }

    /// Create a new Flake in a named graph
    #[allow(clippy::too_many_arguments)]
    pub fn new_in_graph(
        g: Sid,
        s: Sid,
        p: Sid,
        o: FlakeValue,
        dt: Sid,
        t: i64,
        op: bool,
        m: Option<FlakeMeta>,
    ) -> Self {
        Self {
            g: Some(g),
            s,
            p,
            o,
            dt,
            t,
            op,
            m,
        }
    }

    /// Create a minimum flake for SPOT index range bounds
    ///
    /// All components set to minimum values.
    pub fn min_spot() -> Self {
        Self {
            g: None,
            s: Sid::min(),
            p: Sid::min(),
            o: FlakeValue::min(),
            dt: Sid::min(),
            t: i64::MIN,
            op: false,
            m: Some(FlakeMeta::min()),
        }
    }

    /// Create a maximum flake for SPOT index range bounds
    ///
    /// All components set to maximum values.
    pub fn max_spot() -> Self {
        Self {
            g: None,
            s: Sid::max(),
            p: Sid::max(),
            o: FlakeValue::max(),
            dt: Sid::max(),
            t: i64::MAX,
            op: true,
            m: Some(FlakeMeta::max()),
        }
    }

    /// Create a minimum flake with a specific subject (for SPOT index)
    ///
    /// Use this to find "all flakes for subject X".
    pub fn min_for_subject(s: Sid) -> Self {
        Self {
            g: None,
            s,
            p: Sid::min(),
            o: FlakeValue::min(),
            dt: Sid::min(),
            t: i64::MIN,
            op: false,
            m: Some(FlakeMeta::min()),
        }
    }

    /// Create a maximum flake with a specific subject (for SPOT index)
    pub fn max_for_subject(s: Sid) -> Self {
        Self {
            g: None,
            s,
            p: Sid::max(),
            o: FlakeValue::max(),
            dt: Sid::max(),
            t: i64::MAX,
            op: true,
            m: Some(FlakeMeta::max()),
        }
    }

    /// Create a minimum flake with specific subject and predicate (for SPOT index)
    pub fn min_for_subject_predicate(s: Sid, p: Sid) -> Self {
        Self {
            g: None,
            s,
            p,
            o: FlakeValue::min(),
            dt: Sid::min(),
            t: i64::MIN,
            op: false,
            m: Some(FlakeMeta::min()),
        }
    }

    /// Create a maximum flake with specific subject and predicate (for SPOT index)
    pub fn max_for_subject_predicate(s: Sid, p: Sid) -> Self {
        Self {
            g: None,
            s,
            p,
            o: FlakeValue::max(),
            dt: Sid::max(),
            t: i64::MAX,
            op: true,
            m: Some(FlakeMeta::max()),
        }
    }

    /// Create a minimum flake for PSOT index (predicate-first)
    pub fn min_psot() -> Self {
        // Same components, but PSOT ordering is p, s, o, t
        Self::min_spot()
    }

    /// Create a maximum flake for PSOT index
    pub fn max_psot() -> Self {
        Self::max_spot()
    }

    /// Create a minimum flake with a specific predicate (for PSOT/POST index)
    pub fn min_for_predicate(p: Sid) -> Self {
        Self {
            g: None,
            s: Sid::min(),
            p,
            o: FlakeValue::min(),
            dt: Sid::min(),
            t: i64::MIN,
            op: false,
            m: Some(FlakeMeta::min()),
        }
    }

    /// Create a maximum flake with a specific predicate (for PSOT/POST index)
    pub fn max_for_predicate(p: Sid) -> Self {
        Self {
            g: None,
            s: Sid::max(),
            p,
            o: FlakeValue::max(),
            dt: Sid::max(),
            t: i64::MAX,
            op: true,
            m: Some(FlakeMeta::max()),
        }
    }

    /// Check if this is a reference flake (object points to another subject)
    ///
    /// Reference flakes have datatype $id (namespace_code JSON_LD, name "id").
    pub fn is_ref(&self) -> bool {
        self.dt.namespace_code == JSON_LD && self.dt.name.as_ref() == "id"
    }

    /// Create a retraction of this flake (flip the operation)
    pub fn retract(&self) -> Self {
        Self {
            op: false,
            ..self.clone()
        }
    }

    /// Create a retraction with a new transaction time
    pub fn retract_at(&self, t: i64) -> Self {
        Self {
            t,
            op: false,
            ..self.clone()
        }
    }

    /// Create the inverse of this flake by flipping `op` (assertion ⇄
    /// retraction) and preserving every other component.
    ///
    /// Use this when you need true symmetric inversion — e.g. when reverting a
    /// commit and a flake's `op` may already be `false`. [`Self::retract`]
    /// only sets `op: false` regardless of the original value, which is the
    /// correct primitive when you specifically want a retraction but does not
    /// invert a retraction back into an assertion.
    pub fn invert(&self) -> Self {
        Self {
            op: !self.op,
            ..self.clone()
        }
    }

    /// Like [`Self::invert`] but also overrides the transaction time. Useful
    /// when staging inverted flakes for a new commit, where the staging layer
    /// will rewrite `t` anyway.
    pub fn invert_at(&self, t: i64) -> Self {
        Self {
            t,
            op: !self.op,
            ..self.clone()
        }
    }

    /// Approximate size of this flake in bytes
    ///
    /// Used for cache size estimation.
    pub fn size_bytes(&self) -> usize {
        // Base: struct overhead + s, p, dt SIDs
        let base =
            16 + (8 + self.s.name.len()) + (8 + self.p.name.len()) + (8 + self.dt.name.len());

        // Object size depends on type
        let o_size = match &self.o {
            FlakeValue::Null => 1,
            FlakeValue::Boolean(_) => 1,
            FlakeValue::Long(_) => 8,
            FlakeValue::Double(_) => 8,
            FlakeValue::BigInt(v) => 16 + v.to_string().len(), // boxed + string representation
            FlakeValue::Decimal(v) => 16 + v.to_string().len(), // boxed + string representation
            FlakeValue::DateTime(v) => 16 + v.original().len(), // boxed + original string
            FlakeValue::Date(v) => 16 + v.original().len(),
            FlakeValue::Time(v) => 16 + v.original().len(),
            FlakeValue::GYear(v) => 16 + v.original().len(),
            FlakeValue::GYearMonth(v) => 16 + v.original().len(),
            FlakeValue::GMonth(v) => 16 + v.original().len(),
            FlakeValue::GDay(v) => 16 + v.original().len(),
            FlakeValue::GMonthDay(v) => 16 + v.original().len(),
            FlakeValue::YearMonthDuration(v) => 16 + v.original().len(),
            FlakeValue::DayTimeDuration(v) => 16 + v.original().len(),
            FlakeValue::Duration(v) => 16 + v.original().len(),
            FlakeValue::String(s) => 8 + s.len(),
            FlakeValue::Json(s) => 8 + s.len(), // JSON stored as string
            FlakeValue::Ref(sid) => 8 + sid.name.len(),
            FlakeValue::Vector(v) => 8 + v.len() * 8, // length prefix + 8 bytes per f64
            FlakeValue::GeoPoint(_) => 8,             // packed u64
        };

        // Metadata size
        let m_size = self.m.as_ref().map_or(0, |m| {
            8 + m.lang.as_ref().map_or(0, std::string::String::len) + 4
        });

        // t (8) + op (1)
        base + o_size + m_size + 9
    }

    /// Fast deterministic size estimate for stats (`IndexStats.size`)
    ///
    /// Mirrors the intent of legacy `flake/size-flake`: **speed over accuracy**.
    /// This is *not* the storage byte size of index nodes; it is an estimate of
    /// total bytes represented by flakes.
    ///
    /// Important: keep this allocation-free and platform-stable (u64).
    pub fn size_estimate_bytes(&self) -> u64 {
        // Uses a fixed base (38 bytes) + object/meta additions.
        const BASE: u64 = 38;

        let o_size: u64 = match &self.o {
            FlakeValue::Null => 0,
            FlakeValue::Boolean(_) => 1,
            FlakeValue::Long(_) => 8,
            FlakeValue::Double(_) => 8,
            FlakeValue::BigInt(v) => 16 + v.to_string().len() as u64,
            FlakeValue::Decimal(v) => 16 + v.to_string().len() as u64,
            FlakeValue::DateTime(v) => 16 + v.original().len() as u64,
            FlakeValue::Date(v) => 16 + v.original().len() as u64,
            FlakeValue::Time(v) => 16 + v.original().len() as u64,
            FlakeValue::GYear(v) => 16 + v.original().len() as u64,
            FlakeValue::GYearMonth(v) => 16 + v.original().len() as u64,
            FlakeValue::GMonth(v) => 16 + v.original().len() as u64,
            FlakeValue::GDay(v) => 16 + v.original().len() as u64,
            FlakeValue::GMonthDay(v) => 16 + v.original().len() as u64,
            FlakeValue::YearMonthDuration(v) => 16 + v.original().len() as u64,
            FlakeValue::DayTimeDuration(v) => 16 + v.original().len() as u64,
            FlakeValue::Duration(v) => 16 + v.original().len() as u64,
            FlakeValue::String(s) => s.len() as u64,
            FlakeValue::Json(s) => s.len() as u64, // JSON stored as string
            FlakeValue::Ref(sid) => 8 + sid.name.len() as u64,
            FlakeValue::Vector(v) => (v.len() * 8) as u64,
            FlakeValue::GeoPoint(_) => 8,
        };

        let m_size: u64 = match &self.m {
            None => 0,
            Some(m) => {
                let lang = m.lang.as_ref().map(|l| l.len() as u64).unwrap_or(0);
                let idx = m.i.map(|_| 4u64).unwrap_or(0);
                // small overhead for metadata presence
                4 + lang + idx
            }
        };

        BASE + o_size + m_size
    }
}

/// Fast deterministic size estimate for a collection of flakes (stats)
pub fn size_flakes_estimate(flakes: &[Flake]) -> u64 {
    flakes.iter().map(Flake::size_estimate_bytes).sum()
}

// === Equality based on fact identity (ignoring t and op) ===

impl PartialEq for Flake {
    fn eq(&self, other: &Self) -> bool {
        self.s == other.s
            && self.p == other.p
            && self.o == other.o
            && self.dt == other.dt
            && self.m == other.m
    }
}

impl Eq for Flake {}

impl Hash for Flake {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.s.hash(state);
        self.p.hash(state);
        self.o.hash(state);
        self.dt.hash(state);
        self.m.hash(state);
    }
}

impl fmt::Display for Flake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op_str = if self.op { "+" } else { "-" };
        write!(
            f,
            "[{} {} {} {} t:{} {}]",
            self.s, self.p, self.o, self.dt, self.t, op_str
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flake_creation() {
        let flake = Flake::new(
            Sid::new(1, "alice"),
            Sid::new(2, "name"),
            FlakeValue::String("Alice".to_string()),
            Sid::new(3, "string"),
            1,
            true,
            None,
        );

        assert_eq!(flake.s.name.as_ref(), "alice");
        assert_eq!(flake.p.name.as_ref(), "name");
        assert!(matches!(flake.o, FlakeValue::String(_)));
        assert_eq!(flake.t, 1);
        assert!(flake.op);
    }

    #[test]
    fn test_min_max_for_subject() {
        let subject = Sid::new(100, "test");
        let min = Flake::min_for_subject(subject.clone());
        let max = Flake::max_for_subject(subject.clone());

        assert_eq!(min.s, subject);
        assert_eq!(max.s, subject);
        assert!(min.p < max.p);
        assert!(min.t < max.t);
    }

    #[test]
    fn test_flake_equality() {
        let f1 = Flake::new(
            Sid::new(1, "s"),
            Sid::new(2, "p"),
            FlakeValue::Long(42),
            Sid::new(3, "long"),
            1,
            true,
            None,
        );

        let f2 = Flake::new(
            Sid::new(1, "s"),
            Sid::new(2, "p"),
            FlakeValue::Long(42),
            Sid::new(3, "long"),
            2,     // Different t
            false, // Different op
            None,
        );

        // Equality ignores t and op
        assert_eq!(f1, f2);
    }

    #[test]
    fn test_retraction() {
        let flake = Flake::new(
            Sid::new(1, "s"),
            Sid::new(2, "p"),
            FlakeValue::Long(42),
            Sid::new(3, "long"),
            1,
            true,
            None,
        );

        let retracted = flake.retract();
        assert!(!retracted.op);
        assert_eq!(retracted.t, flake.t);

        let retracted_at = flake.retract_at(5);
        assert!(!retracted_at.op);
        assert_eq!(retracted_at.t, 5);
    }

    #[test]
    fn test_is_ref() {
        let ref_flake = Flake::new(
            Sid::new(1, "s"),
            Sid::new(2, "p"),
            FlakeValue::Ref(Sid::new(1, "target")),
            Sid::new(1, "id"), // $id datatype
            1,
            true,
            None,
        );

        let string_flake = Flake::new(
            Sid::new(1, "s"),
            Sid::new(2, "p"),
            FlakeValue::String("value".to_string()),
            Sid::new(3, "string"),
            1,
            true,
            None,
        );

        assert!(ref_flake.is_ref());
        assert!(!string_flake.is_ref());
    }

    #[test]
    fn test_flake_meta_ordering() {
        let m1 = FlakeMeta {
            lang: None,
            i: Some(1),
        };
        let m2 = FlakeMeta {
            lang: None,
            i: Some(2),
        };
        let m3 = FlakeMeta {
            lang: None,
            i: None,
        };

        assert!(m1 < m2);
        assert!(m3 < m1); // None < Some for i
    }

    // ===== `FlakeMeta` ordering laws =====
    //
    // `FlakeMeta` derives `PartialEq`/`Eq`/`Hash` over both fields but hand
    // writes `Ord`, so the two can drift apart. They did (#1711): the old
    // `cmp` compared `lang` only when neither side carried a list index, so
    // `{en, 0}` and `{fr, 0}` were `Equal` without being equal. These three
    // tests pin the relation the type now promises, and pin it against the
    // relation it used to have.

    /// Every `(lang, i)` combination that matters, as a domain to quantify
    /// the laws below over. Two `Option` states on `i` plus the extremes the
    /// sentinels use, crossed with absent/present language tags.
    fn meta_domain() -> Vec<FlakeMeta> {
        let langs = [None, Some("de"), Some("en"), Some("fr")];
        let indices = [
            None,
            Some(i32::MIN),
            Some(-1),
            Some(0),
            Some(1),
            Some(i32::MAX),
        ];
        let mut out = Vec::with_capacity(langs.len() * indices.len());
        for lang in &langs {
            for i in &indices {
                out.push(FlakeMeta {
                    lang: lang.map(str::to_string),
                    i: *i,
                });
            }
        }
        out
    }

    /// `FlakeMeta::cmp` as it stood before #1711, held verbatim so the change
    /// can be checked rather than taken on trust.
    fn legacy_cmp(a: &FlakeMeta, b: &FlakeMeta) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (&a.i, &b.i) {
            (Some(x), Some(y)) => x.cmp(y),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => match (&a.lang, &b.lang) {
                (Some(x), Some(y)) => x.cmp(y),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            },
        }
    }

    /// The new ordering is a strict *refinement* of the old one: wherever the
    /// old comparator was decisive it still decides the same way, and the
    /// entire change is pairs it called `Equal` while `Eq` called them
    /// distinct. Nothing is reordered, so nothing that merely sorts by this
    /// relation can observe the change.
    ///
    /// This is the test that makes the one-line comparator edit reviewable.
    #[test]
    fn new_ord_refines_old_ord() {
        use std::cmp::Ordering;
        let domain = meta_domain();
        let mut newly_split = 0usize;

        for a in &domain {
            for b in &domain {
                let old = legacy_cmp(a, b);
                let new = a.cmp(b);
                if old != Ordering::Equal {
                    assert_eq!(
                        new, old,
                        "a decision the old comparator made was reversed: \
                         {a:?} vs {b:?} was {old:?}, is now {new:?}"
                    );
                    continue;
                }
                if new != Ordering::Equal {
                    newly_split += 1;
                    assert_ne!(
                        a, b,
                        "the refinement may only separate values that are \
                         genuinely unequal: {a:?} vs {b:?}"
                    );
                }
            }
        }

        // Non-vacuity: the refinement has to actually refine something, or
        // this test would pass against the very bug it exists to pin.
        assert!(
            newly_split > 0,
            "no pair was newly separated — the ordering fix is not in effect"
        );

        // The one behavioral consequence of the refinement, pinned where the
        // change is described. `FlakeMeta::max()` is a query range bound, not
        // a maximum of the type: a language-tagged value at list index
        // `i32::MAX` now sorts strictly above it, so an inclusive upper bound
        // built from it no longer admits that (unreachable) case. See the doc
        // on `FlakeMeta::max`.
        let sentinel = FlakeMeta::max();
        let tagged_at_max = FlakeMeta {
            lang: Some("en".to_string()),
            i: Some(i32::MAX),
        };
        assert_eq!(legacy_cmp(&tagged_at_max, &sentinel), Ordering::Equal);
        assert_eq!(tagged_at_max.cmp(&sentinel), Ordering::Greater);
    }

    /// The law the type was violating: `cmp` returns `Equal` exactly when
    /// `==` says equal. Callers that express identity through this ordering —
    /// an `OrdMap` key, a `cmp(..) == Equal` test, an exclusive seek — are
    /// only correct while this holds.
    #[test]
    fn ord_agrees_with_eq() {
        use std::cmp::Ordering;
        for a in &meta_domain() {
            for b in &meta_domain() {
                assert_eq!(
                    a.cmp(b) == Ordering::Equal,
                    a == b,
                    "Ord and Eq disagree on {a:?} vs {b:?}"
                );
            }
        }
    }

    /// `Ord`'s own contract: antisymmetric, and transitive on both the
    /// strict relation and equality.
    #[test]
    fn ord_is_a_total_order() {
        use std::cmp::Ordering;
        let domain = meta_domain();

        for a in &domain {
            assert_eq!(a.cmp(a), Ordering::Equal, "not reflexive at {a:?}");
            for b in &domain {
                assert_eq!(
                    a.cmp(b),
                    b.cmp(a).reverse(),
                    "not antisymmetric: {a:?} vs {b:?}"
                );
                // `PartialOrd` must agree with `Ord`, since it delegates.
                assert_eq!(a.partial_cmp(b), Some(a.cmp(b)));
            }
        }

        for a in &domain {
            for b in &domain {
                let ab = a.cmp(b);
                for c in &domain {
                    let bc = b.cmp(c);
                    // Whenever `a?b` and `b?c` force `a?c`, check the forced
                    // answer: equality composes both ways, and two strict
                    // steps in the same direction compose.
                    let forced = match (ab, bc) {
                        (Ordering::Equal, _) => Some(bc),
                        (_, Ordering::Equal) => Some(ab),
                        _ if ab == bc => Some(ab),
                        _ => None,
                    };
                    if let Some(expected) = forced {
                        assert_eq!(
                            a.cmp(c),
                            expected,
                            "not transitive: {a:?} {ab:?} {b:?} {bc:?} {c:?}"
                        );
                    }
                }
            }
        }
    }
}
