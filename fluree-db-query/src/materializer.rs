//! On-demand binding materialization service.
//!
//! The `Materializer` provides lazy decoding of encoded bindings with caching.
//! Each query gets its own `Materializer` instance to avoid cross-query cache pollution.
//!
//! # Design Principles
//!
//! 1. **Return views/keys, not Bindings**: The materializer returns separate key/value
//!    objects (`JoinKey`, `ComparableValue`, etc.) rather than rewriting `Binding` values.
//!    This prevents accidental mutation of hash keys in GROUP BY/DISTINCT/hash joins.
//!
//! 2. **Safe boundaries**: Only `to_term()` produces full `Binding` values, and it should
//!    only be called at terminal boundaries (formatting/projection) after all hashing/grouping
//!    is complete.
//!
//! 3. **Caching**: Decoded values are cached for the query duration to avoid repeated
//!    dictionary lookups.
//!
//! # Single-Ledger vs Multi-Ledger (Dataset) Mode
//!
//! - **Single-ledger**: Join keys use raw `s_id` comparison (no decoding needed).
//! - **Multi-ledger**: Join keys use canonical IRI strings for correct cross-ledger semantics.

use crate::binding::Binding;
use chrono::{Datelike, Timelike};
use fluree_db_binary_index::{BinaryGraphView, BinaryIndexStore};
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{FlakeValue, Sid};
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Join key for hash joins, DISTINCT, and GROUP BY operations.
///
/// This type is designed to be cheap to hash and compare without requiring
/// dictionary lookups in single-ledger mode.
#[derive(Debug, Clone)]
pub enum JoinKey<'a> {
    /// Single-ledger: raw subject/ref ID (no decoding needed for comparison)
    Sid(u64),
    /// Single-ledger: raw predicate ID
    Pid(u32),
    /// Multi-ledger: canonical IRI string for cross-ledger comparison (borrowed)
    Iri(Cow<'a, str>),
    /// Multi-ledger: canonical IRI string (owned via Arc, no allocation on clone)
    /// This is used for cached IRI resolutions to avoid allocating a new String.
    IriOwned(Arc<str>),
    /// Literal value key (excluding transaction metadata)
    Lit {
        o_kind: u8,
        o_key: u64,
        /// `p_id` is only required for NUM_BIG decoding (per-predicate numbig arena).
        /// For all other literal kinds, it must not affect join-key identity.
        p_id_for_numbig: Option<u32>,
        dt_id: u16,
        lang_id: u16,
    },
    /// Already-materialized Sid (namespace_code + name)
    MaterializedSid(u16, Cow<'a, str>),
    /// Already-materialized literal (for decoded Lit bindings)
    ///
    /// IMPORTANT: Must include datatype and language to match SPARQL term identity.
    MaterializedLit(MaterializedLitKey),
    /// Unbound/Poisoned - represents absence
    Absent,
}

/// Hashable wrapper for FlakeValue (excluding Ref which should use Sid)
#[derive(Debug, Clone, PartialEq)]
pub struct FlakeValueKey(pub FlakeValue);

impl Eq for FlakeValueKey {}

impl Hash for FlakeValueKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash by discriminant + value
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            FlakeValue::String(s) => s.hash(state),
            FlakeValue::Long(n) => n.hash(state),
            FlakeValue::Double(d) => d.to_bits().hash(state),
            FlakeValue::Boolean(b) => b.hash(state),
            FlakeValue::Ref(sid) => {
                sid.namespace_code.hash(state);
                sid.name.hash(state);
            }
            FlakeValue::BigInt(n) => n.to_string().hash(state),
            FlakeValue::Decimal(d) => d.to_string().hash(state),
            FlakeValue::DateTime(dt) => dt.hash(state),
            FlakeValue::Date(d) => d.hash(state),
            FlakeValue::Time(t) => t.hash(state),
            FlakeValue::Json(s) => s.hash(state),
            FlakeValue::Vector(v) => {
                for f in v {
                    f.to_bits().hash(state);
                }
            }
            FlakeValue::Null => {}
            FlakeValue::GYear(v) => v.hash(state),
            FlakeValue::GYearMonth(v) => v.hash(state),
            FlakeValue::GMonth(v) => v.hash(state),
            FlakeValue::GDay(v) => v.hash(state),
            FlakeValue::GMonthDay(v) => v.hash(state),
            FlakeValue::YearMonthDuration(v) => v.hash(state),
            FlakeValue::DayTimeDuration(v) => v.hash(state),
            FlakeValue::Duration(v) => v.hash(state),
            FlakeValue::GeoPoint(v) => v.hash(state),
        }
    }
}

/// Hashable key for a fully materialized literal binding.
///
/// This must include datatype and language to match `Binding::Lit` equality/hash semantics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedLitKey {
    pub val: FlakeValueKey,
    pub dtc: DatatypeConstraint,
}

impl PartialEq for JoinKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (JoinKey::Sid(a), JoinKey::Sid(b)) => a == b,
            (JoinKey::Pid(a), JoinKey::Pid(b)) => a == b,
            // Iri and IriOwned are equivalent if they have the same string content
            (JoinKey::Iri(a), JoinKey::Iri(b)) => a == b,
            (JoinKey::IriOwned(a), JoinKey::IriOwned(b)) => a == b,
            (JoinKey::Iri(a), JoinKey::IriOwned(b)) => a.as_ref() == b.as_ref(),
            (JoinKey::IriOwned(a), JoinKey::Iri(b)) => a.as_ref() == b.as_ref(),
            (
                JoinKey::Lit {
                    o_kind: k1,
                    o_key: ok1,
                    p_id_for_numbig: p1,
                    dt_id: d1,
                    lang_id: l1,
                },
                JoinKey::Lit {
                    o_kind: k2,
                    o_key: ok2,
                    p_id_for_numbig: p2,
                    dt_id: d2,
                    lang_id: l2,
                },
            ) => k1 == k2 && ok1 == ok2 && p1 == p2 && d1 == d2 && l1 == l2,
            (JoinKey::MaterializedSid(ns1, n1), JoinKey::MaterializedSid(ns2, n2)) => {
                ns1 == ns2 && n1 == n2
            }
            (JoinKey::MaterializedLit(a), JoinKey::MaterializedLit(b)) => a == b,
            (JoinKey::Absent, JoinKey::Absent) => true,
            _ => false,
        }
    }
}

impl Eq for JoinKey<'_> {}

impl Hash for JoinKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use explicit discriminant values to ensure Iri and IriOwned hash the same
        // (since they are considered equal when they have the same string content)
        let discriminant: u8 = match self {
            JoinKey::Sid(_) => 0,
            JoinKey::Pid(_) => 1,
            JoinKey::Iri(_) | JoinKey::IriOwned(_) => 2, // Same discriminant for equivalence
            JoinKey::Lit { .. } => 3,
            JoinKey::MaterializedSid(_, _) => 4,
            JoinKey::MaterializedLit(_) => 5,
            JoinKey::Absent => 6,
        };
        discriminant.hash(state);
        match self {
            JoinKey::Sid(id) => id.hash(state),
            JoinKey::Pid(id) => id.hash(state),
            JoinKey::Iri(s) => s.hash(state),
            JoinKey::IriOwned(s) => s.hash(state),
            JoinKey::Lit {
                o_kind,
                o_key,
                p_id_for_numbig,
                dt_id,
                lang_id,
            } => {
                o_kind.hash(state);
                o_key.hash(state);
                p_id_for_numbig.hash(state);
                dt_id.hash(state);
                lang_id.hash(state);
            }
            JoinKey::MaterializedSid(ns, name) => {
                ns.hash(state);
                name.hash(state);
            }
            JoinKey::MaterializedLit(v) => v.hash(state),
            JoinKey::Absent => {}
        }
    }
}

/// Comparable value for FILTER expressions and ORDER BY.
///
/// This type supports the SPARQL comparison semantics including
/// cross-type numeric comparisons and proper ordering.
#[derive(Debug, Clone, PartialEq)]
pub enum ComparableValue {
    Iri(Arc<str>),
    String(Arc<str>),
    Long(i64),
    Double(f64),
    Bool(bool),
    DateTime(i64),
    Date(i32),
    Time(i64),
    BigInt(Arc<str>), // String representation for comparison
    Decimal(Arc<str>),
    /// Sid for single-ledger IRI comparisons
    Sid(Sid),
}

impl ComparableValue {
    /// Extract numeric value for arithmetic/comparison operations
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ComparableValue::Long(n) => Some(*n as f64),
            ComparableValue::Double(d) => Some(*d),
            _ => None,
        }
    }

    /// Extract integer value
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ComparableValue::Long(n) => Some(*n),
            ComparableValue::Double(d) if d.fract() == 0.0 => Some(*d as i64),
            _ => None,
        }
    }
}

/// Join key mode for the materializer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JoinKeyMode {
    /// Single-ledger: use raw IDs for comparison (no decoding)
    #[default]
    SingleLedger,
    /// Multi-ledger (dataset): use canonical IRI strings
    MultiLedger,
}

/// Per-query materializer with caching.
///
/// Create one instance per query execution and pass it to operators that need
/// to materialize bindings for comparison/hashing/output.
pub struct Materializer {
    /// Graph-scoped view (store + graph ID) for decoding.
    ///
    /// When constructed with a novelty-aware `BinaryGraphView` (via
    /// `ExecutionContext::graph_view()`), all decode methods automatically
    /// handle watermark routing for novelty-only subject/string IDs.
    graph_view: BinaryGraphView,
    /// Cache: s_id -> Sid (for terminal output)
    sid_cache: HashMap<u64, Sid>,
    /// Cache: s_id -> canonical IRI (for multi-ledger join keys)
    iri_cache: HashMap<u64, Arc<str>>,
    /// Cache: p_id -> Sid
    pid_cache: HashMap<u32, Sid>,
    /// Join key mode
    mode: JoinKeyMode,
    /// Ledger ID (for IriMatch construction in multi-ledger mode)
    ledger_id: Option<Arc<str>>,
}

impl Materializer {
    /// Create a new materializer for a query.
    ///
    /// # Arguments
    /// * `graph_view` - Graph-scoped view (store + graph ID) for decoding
    /// * `mode` - Single-ledger or multi-ledger join key mode
    pub fn new(graph_view: BinaryGraphView, mode: JoinKeyMode) -> Self {
        Self {
            graph_view,
            sid_cache: HashMap::new(),
            iri_cache: HashMap::new(),
            pid_cache: HashMap::new(),
            mode,
            ledger_id: None,
        }
    }

    /// Set ledger ID for IriMatch construction in multi-ledger mode.
    pub fn with_ledger_id(mut self, ledger_id: impl Into<Arc<str>>) -> Self {
        self.ledger_id = Some(ledger_id.into());
        self
    }

    /// Get the join key mode.
    pub fn mode(&self) -> JoinKeyMode {
        self.mode
    }

    /// Get the underlying binary index store.
    pub fn store(&self) -> &BinaryIndexStore {
        self.graph_view.store()
    }

    // -------------------------------------------------------------------------
    // Join Key API (for hash operations: DISTINCT, GROUP BY, hash joins)
    // -------------------------------------------------------------------------

    /// Get a join key for a binding.
    ///
    /// This returns a `JoinKey` that can be hashed and compared for equality
    /// without modifying the original `Binding`. Use this for DISTINCT, GROUP BY,
    /// and hash join operations.
    ///
    /// In single-ledger mode, encoded IDs are used directly (no decoding).
    /// In multi-ledger mode, IRIs are resolved to canonical strings.
    pub fn join_key<'a>(&'a mut self, binding: &'a Binding) -> JoinKey<'a> {
        match binding {
            Binding::Unbound | Binding::Poisoned => JoinKey::Absent,

            Binding::Sid { sid, .. } => {
                match self.mode {
                    JoinKeyMode::SingleLedger => {
                        // In single-ledger mode, (namespace_code, name) is a valid key
                        JoinKey::MaterializedSid(sid.namespace_code, Cow::Borrowed(&sid.name))
                    }
                    JoinKeyMode::MultiLedger => {
                        // In multi-ledger mode, namespace codes may differ across ledgers,
                        // so we must use the full canonical IRI for comparison.
                        // Unknown namespace code → Absent (strict decode).
                        match self.graph_view.store().sid_to_iri(sid) {
                            Some(iri) => JoinKey::IriOwned(Arc::from(iri)),
                            None => {
                                tracing::error!(
                                    ns_code = sid.namespace_code,
                                    suffix = %sid.name,
                                    "sid_to_iri: unknown namespace code in materializer join_key \
                                     — this is a data corruption signal"
                                );
                                JoinKey::Absent
                            }
                        }
                    }
                }
            }

            Binding::IriMatch { iri, .. } => JoinKey::Iri(Cow::Borrowed(iri.as_ref())),

            Binding::Iri(iri) => JoinKey::Iri(Cow::Borrowed(iri.as_ref())),

            Binding::EncodedSid { s_id, .. } => {
                match self.mode {
                    JoinKeyMode::SingleLedger => JoinKey::Sid(*s_id),
                    JoinKeyMode::MultiLedger => {
                        // Resolve to canonical IRI for cross-ledger comparison
                        // Using IriOwned avoids allocation - just clones the Arc
                        let iri = self.resolve_iri(*s_id);
                        JoinKey::IriOwned(iri)
                    }
                }
            }

            Binding::EncodedPid { p_id } => {
                match self.mode {
                    JoinKeyMode::SingleLedger => JoinKey::Pid(*p_id),
                    JoinKeyMode::MultiLedger => {
                        // Resolve to canonical IRI
                        // Using IriOwned avoids allocation - just clones the Arc
                        if let Some(iri) = self.graph_view.store().resolve_predicate_iri(*p_id) {
                            JoinKey::IriOwned(Arc::from(iri))
                        } else {
                            JoinKey::Pid(*p_id) // Fallback
                        }
                    }
                }
            }

            Binding::Lit { val, dtc, .. } => JoinKey::MaterializedLit(MaterializedLitKey {
                val: FlakeValueKey(val.clone()),
                dtc: dtc.clone(),
            }),

            Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id,
                ..
            } => JoinKey::Lit {
                o_kind: *o_kind,
                o_key: *o_key,
                p_id_for_numbig: if *o_kind == fluree_db_core::ObjKind::NUM_BIG.as_u8() {
                    Some(*p_id)
                } else {
                    None
                },
                dt_id: *dt_id,
                lang_id: *lang_id,
            },

            Binding::Grouped(_) => {
                // Grouped bindings shouldn't appear in join key contexts
                debug_assert!(false, "Grouped binding in join_key");
                JoinKey::Absent
            }
        }
    }

    // -------------------------------------------------------------------------
    // Comparable Value API (for FILTER and ORDER BY)
    // -------------------------------------------------------------------------

    /// Get a comparable value for a binding.
    ///
    /// This returns a `ComparableValue` for use in FILTER expressions and
    /// ORDER BY comparisons. Returns `None` for unbound/poisoned bindings.
    pub fn comparable(&mut self, binding: &Binding) -> Option<ComparableValue> {
        match binding {
            Binding::Unbound | Binding::Poisoned => None,

            Binding::Sid { sid, .. } => Some(ComparableValue::Sid(sid.clone())),

            Binding::IriMatch { iri, .. } => Some(ComparableValue::Iri(Arc::clone(iri))),

            Binding::Iri(iri) => Some(ComparableValue::Iri(Arc::clone(iri))),

            Binding::EncodedSid { s_id, .. } => {
                let iri = self.resolve_iri(*s_id);
                Some(ComparableValue::Iri(iri))
            }

            Binding::EncodedPid { p_id } => self
                .graph_view
                .store()
                .resolve_predicate_iri(*p_id)
                .map(|iri| ComparableValue::Iri(Arc::from(iri))),

            Binding::Lit { val, .. } => flake_value_to_comparable(val),

            Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id,
                ..
            } => match self
                .graph_view
                .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
            {
                Ok(val) => flake_value_to_comparable(&val),
                Err(_) => None,
            },

            Binding::Grouped(_) => None,
        }
    }

    // -------------------------------------------------------------------------
    // String API (for STR/REGEX/CONTAINS functions)
    // -------------------------------------------------------------------------

    /// Get a string representation of a binding.
    ///
    /// This is used for SPARQL functions like STR(), REGEX(), CONTAINS().
    /// Returns `None` for unbound/poisoned bindings.
    pub fn as_string(&mut self, binding: &Binding) -> Option<Arc<str>> {
        match binding {
            Binding::Unbound | Binding::Poisoned => None,

            Binding::Sid { sid, .. } => {
                // Decode to full IRI string.
                // IMPORTANT: `namespace_code:name` is an internal representation and is not a full IRI.
                // Unknown namespace code → None (strict decode).
                match self.graph_view.store().sid_to_iri(sid) {
                    Some(iri) => Some(Arc::from(iri)),
                    None => {
                        tracing::error!(
                            ns_code = sid.namespace_code,
                            suffix = %sid.name,
                            "sid_to_iri: unknown namespace code in materializer as_string \
                             — this is a data corruption signal"
                        );
                        None
                    }
                }
            }

            Binding::IriMatch { iri, .. } => Some(Arc::clone(iri)),

            Binding::Iri(iri) => Some(Arc::clone(iri)),

            Binding::EncodedSid { s_id, .. } => Some(self.resolve_iri(*s_id)),

            Binding::EncodedPid { p_id } => self
                .graph_view
                .store()
                .resolve_predicate_iri(*p_id)
                .map(Arc::from),

            Binding::Lit { val, .. } => Some(Arc::from(val.to_string())),

            Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id,
                ..
            } => self
                .graph_view
                .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                .ok()
                .map(|v| Arc::from(v.to_string())),

            Binding::Grouped(_) => None,
        }
    }

    // -------------------------------------------------------------------------
    // Terminal Materialization (ONLY at formatting/projection boundaries)
    // -------------------------------------------------------------------------

    /// Materialize a binding to its full decoded form.
    ///
    /// **IMPORTANT**: This should ONLY be called at terminal boundaries
    /// (formatting, projection) AFTER all hashing/grouping/joining is complete.
    /// Calling this during intermediate processing can corrupt hash-based
    /// operator results if the materialized binding is inserted into a hash
    /// structure that contained the original encoded binding.
    pub fn to_term(&mut self, binding: &Binding) -> Binding {
        match binding {
            // Already materialized
            Binding::Unbound
            | Binding::Poisoned
            | Binding::Sid { .. }
            | Binding::IriMatch { .. }
            | Binding::Iri(_)
            | Binding::Lit { .. }
            | Binding::Grouped(_) => binding.clone(),

            Binding::EncodedSid { s_id, .. } => {
                let sid = self.resolve_sid(*s_id);
                Binding::sid(sid)
            }

            Binding::EncodedPid { p_id } => {
                let sid = self.resolve_pid(*p_id);
                Binding::sid(sid)
            }

            Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id,
                i_val,
                t,
            } => match self
                .graph_view
                .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
            {
                Ok(FlakeValue::Ref(sid)) => Binding::sid(sid),
                Ok(val) => {
                    let dt_sid = self
                        .graph_view
                        .store()
                        .dt_sids()
                        .get(*dt_id as usize)
                        .cloned()
                        .unwrap_or_else(|| Sid::new(0, ""));
                    let meta = self.graph_view.store().decode_meta(*lang_id, *i_val);
                    let dtc = match meta.and_then(|m| m.lang.map(Arc::from)) {
                        Some(lang) => DatatypeConstraint::LangTag(lang),
                        None => DatatypeConstraint::Explicit(dt_sid),
                    };
                    Binding::Lit {
                        val,
                        dtc,
                        t: Some(*t),
                        op: None,
                        p_id: Some(*p_id),
                    }
                }
                Err(_) => Binding::Unbound,
            },
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers with caching
    // -------------------------------------------------------------------------

    /// Resolve s_id to canonical IRI string (cached).
    ///
    /// Novelty-aware: `BinaryGraphView` handles watermark routing internally.
    fn resolve_iri(&mut self, s_id: u64) -> Arc<str> {
        if let Some(cached) = self.iri_cache.get(&s_id) {
            return Arc::clone(cached);
        }

        let iri = match self.graph_view.resolve_subject_iri(s_id) {
            Ok(iri) => Arc::from(iri),
            Err(e) => {
                tracing::warn!(
                    s_id,
                    error = %e,
                    "resolve_subject_iri failed — fabricating placeholder IRI"
                );
                Arc::from(format!("_:unknown_{s_id}"))
            }
        };

        self.iri_cache.insert(s_id, Arc::clone(&iri));
        iri
    }

    /// Resolve s_id to Sid (cached).
    ///
    /// Novelty-aware: uses `resolve_subject_sid` which returns `Sid` directly
    /// for novel subjects (no IRI string allocation or trie lookup).
    fn resolve_sid(&mut self, s_id: u64) -> Sid {
        if let Some(cached) = self.sid_cache.get(&s_id) {
            return cached.clone();
        }

        let sid = match self.graph_view.resolve_subject_sid(s_id) {
            Ok(sid) => sid,
            Err(_) => Sid::new(0, format!("_:unknown_{s_id}")),
        };

        self.sid_cache.insert(s_id, sid.clone());
        sid
    }

    /// Resolve p_id to Sid (cached).
    fn resolve_pid(&mut self, p_id: u32) -> Sid {
        if let Some(cached) = self.pid_cache.get(&p_id) {
            return cached.clone();
        }

        let sid = match self.graph_view.store().resolve_predicate_iri(p_id) {
            Some(iri) => self.graph_view.store().encode_iri(iri),
            None => {
                tracing::warn!(
                    p_id,
                    "resolve_predicate_iri failed — fabricating placeholder predicate"
                );
                Sid::new(0, format!("_:unknown_p_{p_id}"))
            }
        };

        self.pid_cache.insert(p_id, sid.clone());
        sid
    }
}

/// Convert a FlakeValue to a ComparableValue.
fn flake_value_to_comparable(val: &FlakeValue) -> Option<ComparableValue> {
    match val {
        FlakeValue::String(s) => Some(ComparableValue::String(Arc::from(s.as_str()))),
        FlakeValue::Long(n) => Some(ComparableValue::Long(*n)),
        FlakeValue::Double(d) => Some(ComparableValue::Double(*d)),
        FlakeValue::Boolean(b) => Some(ComparableValue::Bool(*b)),
        FlakeValue::DateTime(dt) => Some(ComparableValue::DateTime(dt.epoch_millis())),
        FlakeValue::Date(d) => Some(ComparableValue::Date(d.date().num_days_from_ce())),
        FlakeValue::Time(t) => Some(ComparableValue::Time(
            t.time().num_seconds_from_midnight() as i64
        )),
        FlakeValue::BigInt(n) => Some(ComparableValue::BigInt(Arc::from(n.to_string()))),
        FlakeValue::Decimal(d) => Some(ComparableValue::Decimal(Arc::from(d.to_string()))),
        FlakeValue::Ref(sid) => Some(ComparableValue::Sid(sid.clone())),
        FlakeValue::Json(s) => Some(ComparableValue::String(Arc::from(s.as_str()))),
        FlakeValue::Vector(_) => None, // Vectors aren't directly comparable
        FlakeValue::Null => None,
        // Temporal types - convert to comparable representation
        FlakeValue::GYear(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
        FlakeValue::GYearMonth(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
        FlakeValue::GMonth(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
        FlakeValue::GDay(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
        FlakeValue::GMonthDay(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
        FlakeValue::YearMonthDuration(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
        FlakeValue::DayTimeDuration(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
        FlakeValue::Duration(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
        FlakeValue::GeoPoint(v) => Some(ComparableValue::String(Arc::from(v.to_string()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_key_equality() {
        // Same s_id should be equal
        assert_eq!(JoinKey::Sid(123), JoinKey::Sid(123));
        assert_ne!(JoinKey::Sid(123), JoinKey::Sid(456));

        // Same IRI should be equal
        assert_eq!(
            JoinKey::Iri(Cow::Borrowed("http://example.org/foo")),
            JoinKey::Iri(Cow::Borrowed("http://example.org/foo"))
        );

        // Different types should not be equal
        assert_ne!(JoinKey::Sid(123), JoinKey::Pid(123));
        assert_ne!(JoinKey::Sid(123), JoinKey::Absent);
    }

    #[test]
    fn test_join_key_hashing() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(JoinKey::Sid(123));
        set.insert(JoinKey::Sid(456));
        set.insert(JoinKey::Sid(123)); // Duplicate

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_comparable_value_numeric() {
        let long_val = ComparableValue::Long(42);
        assert_eq!(long_val.as_f64(), Some(42.0));
        assert_eq!(long_val.as_i64(), Some(42));

        let double_val = ComparableValue::Double(3.5);
        assert_eq!(double_val.as_f64(), Some(3.5));
        assert_eq!(double_val.as_i64(), None); // Has fractional part
    }
}
