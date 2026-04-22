//! Binding types for query results
//!
//! This module contains:
//! - `Binding`: A bound value in a solution (cheap to clone)
//! - `Batch`: Columnar batch of solutions
//! - `BatchView`: Zero-copy view of selected columns
//! - `RowView`: Zero-copy view of a single row

use crate::var_registry::VarId;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{FlakeMeta, FlakeValue, Sid};
use std::sync::Arc;

/// A bound value in a solution - cheap to clone (Arc-backed strings)
///
/// # Invariants
///
/// - `Lit` variant NEVER contains `FlakeValue::Ref` - all references use `Sid` variant
/// - Conversion from flake enforces this
/// - `Grouped` variant is only produced by GROUP BY and consumed by aggregate functions
///
/// # Multi-Ledger Support
///
/// For correct cross-ledger joins, IRI-typed bindings must carry canonical IRI strings
/// rather than raw SIDs (which are ledger-local). The `IriMatch` variant provides this:
/// - Stores the canonical IRI for join comparisons
/// - Caches the originating ledger's SID for efficient same-ledger lookups
/// - Tracks ledger provenance for re-encoding when crossing ledger boundaries
#[derive(Clone, Debug)]
pub enum Binding {
    /// Variable is not bound
    Unbound,
    /// Variable is poisoned (from failed OPTIONAL, blocks future binding)
    ///
    /// Poisoned bindings represent variables that could not be matched in an
    /// OPTIONAL clause. Unlike Unbound, a Poisoned binding **blocks** future
    /// pattern matching - any pattern that uses a Poisoned variable yields
    /// no matches (not "match anything", but "match nothing").
    Poisoned,
    /// IRI/node reference (for subject, predicate, or ref-typed object)
    ///
    /// Used in single-ledger mode where SID comparison is sufficient.
    /// For multi-ledger queries, prefer `IriMatch` which carries canonical IRI.
    Sid(Sid),
    /// IRI reference with canonical IRI and per-ledger SID cache
    ///
    /// Used in multi-ledger (dataset) mode to ensure correct cross-ledger joins.
    /// The canonical IRI is the universal join key, while the cached SID enables
    /// efficient lookups in the originating ledger.
    ///
    /// # Design (mirrors the legacy match structure)
    ///
    /// The legacy match maps store both `::iri` and `::sids {alias -> sid}`.
    /// This variant captures the same information for Rust.
    IriMatch {
        /// Canonical IRI string - the universal join key for cross-ledger equality
        iri: Arc<str>,
        /// SID from the originating ledger (for efficient same-ledger lookups)
        primary_sid: Sid,
        /// Ledger alias where this SID came from (for re-encoding decisions)
        ledger_alias: Arc<str>,
    },
    /// Raw IRI string (for graph source results not in namespace table)
    ///
    /// Graph source queries (e.g., R2RML over Iceberg) generate IRIs from
    /// templates that may not exist in the namespace table. Instead of
    /// silently dropping rows, we keep the full IRI string.
    ///
    /// Unlike `Sid`, these IRIs are not encoded and cannot be directly
    /// joined with native Fluree data without IRI string comparison.
    Iri(Arc<str>),
    /// Typed literal value (NEVER contains FlakeValue::Ref)
    Lit {
        /// The value (String, Long, Double, Bool - never Ref)
        val: FlakeValue,
        /// Datatype constraint (explicit datatype or language tag)
        dtc: DatatypeConstraint,
        /// Optional transaction time (when the flake was asserted)
        t: Option<i64>,
        /// Optional operation type for history queries (true = assert, false = retract)
        /// Only populated in history mode when both assertions and retractions are returned.
        op: Option<bool>,
        /// Optional predicate ID from binary index (metadata, excluded from eq/hash).
        /// Set when early-materializing novelty values in BinaryScanOperator.
        /// Used by `fulltext()` to identify the correct per-predicate arena/stats.
        p_id: Option<u32>,
    },
    /// Encoded literal value from the binary index (late materialization).
    ///
    /// This is used to avoid decoding (especially string dictionary lookups)
    /// in the middle of index scans and batched joins when the value is not
    /// needed until projection/formatting time.
    ///
    /// Fields mirror binary index columns:
    /// - `o_kind`/`o_key` are the object value encoding
    /// - `p_id` is required for `BinaryIndexStore::decode_value`
    /// - `dt_id`/`lang_id`/`i_val` provide literal metadata
    /// - `t` is the assertion transaction time (metadata)
    ///
    /// NOTE: EncodedLit represents only literal values. References are still
    /// represented as `Binding::Sid` (resolved via subject dictionaries).
    EncodedLit {
        o_kind: u8,
        o_key: u64,
        p_id: u32,
        dt_id: u16,
        lang_id: u16,
        i_val: i32,
        t: i64,
    },
    /// Encoded subject/predicate/ref-object ID (late materialization).
    ///
    /// Used to defer subject dictionary lookups until join/output time.
    /// The `s_id` is the raw u64 from the binary index.
    ///
    /// # Single-Ledger Only
    ///
    /// `EncodedSid` comparison by `s_id` is only valid within a single ledger.
    /// For cross-ledger (dataset) queries, use `IriMatch` or derive canonical
    /// IRI via the materializer's `JoinKey::Iri`.
    EncodedSid {
        /// Raw subject/ref ID from binary index
        s_id: u64,
    },
    /// Encoded predicate ID (late materialization).
    ///
    /// Used when predicate is a variable in the pattern.
    /// The `p_id` is the raw u32 from the binary index.
    ///
    /// # Single-Ledger Only
    ///
    /// Same constraint as `EncodedSid` - not cross-ledger comparable.
    EncodedPid {
        /// Raw predicate ID from binary index
        p_id: u32,
    },
    /// Grouped values (produced by GROUP BY for non-group-key variables)
    ///
    /// Contains all values for a variable within a single group. This is an
    /// intermediate representation consumed by aggregate functions.
    ///
    /// # Invariants
    ///
    /// - Only produced by GroupByOperator
    /// - Should never appear in join/scan codepaths
    /// - Consumed by aggregate functions (count, sum, avg, etc.)
    Grouped(Vec<Binding>),
}

impl Binding {
    /// Create a new literal binding
    ///
    /// # Panics
    ///
    /// In debug builds, panics if `val` is `FlakeValue::Ref` - use `Binding::Sid` instead.
    /// In release builds, this is enforced by convention; prefer `Binding::from_object`.
    pub fn lit(val: FlakeValue, dt: Sid) -> Self {
        debug_assert!(
            !matches!(val, FlakeValue::Ref(_)),
            "Lit cannot contain Ref - use Binding::Sid"
        );
        Binding::Lit {
            val,
            dtc: DatatypeConstraint::Explicit(dt),
            t: None,
            op: None,
            p_id: None,
        }
    }

    /// Create a new literal binding with language tag
    pub fn lit_lang(val: FlakeValue, lang: impl Into<Arc<str>>) -> Self {
        debug_assert!(
            !matches!(val, FlakeValue::Ref(_)),
            "Lit cannot contain Ref - use Binding::Sid"
        );
        Binding::Lit {
            val,
            dtc: DatatypeConstraint::LangTag(lang.into()),
            t: None,
            op: None,
            p_id: None,
        }
    }

    /// Create a new literal binding with transaction time
    pub fn lit_with_t(val: FlakeValue, dt: Sid, t: i64) -> Self {
        debug_assert!(
            !matches!(val, FlakeValue::Ref(_)),
            "Lit cannot contain Ref - use Binding::Sid"
        );
        Binding::Lit {
            val,
            dtc: DatatypeConstraint::Explicit(dt),
            t: Some(t),
            op: None,
            p_id: None,
        }
    }

    /// Create a binding from a flake's object value
    ///
    /// Automatically routes `FlakeValue::Ref` to `Binding::Sid`.
    pub fn from_object(val: FlakeValue, dt: Sid) -> Self {
        match val {
            FlakeValue::Ref(sid) => Binding::Sid(sid),
            other => Binding::Lit {
                val: other,
                dtc: DatatypeConstraint::Explicit(dt),
                t: None,
                op: None,
                p_id: None,
            },
        }
    }

    /// Create a binding from a flake's object value plus optional metadata.
    ///
    /// Preserves language tags from `FlakeMeta.lang` for langString values.
    pub fn from_object_with_meta(val: FlakeValue, dt: Sid, meta: Option<FlakeMeta>) -> Self {
        match val {
            FlakeValue::Ref(sid) => Binding::Sid(sid),
            other => {
                let dtc = match meta.and_then(|m| m.lang.map(Arc::from)) {
                    Some(lang) => DatatypeConstraint::LangTag(lang),
                    None => DatatypeConstraint::Explicit(dt),
                };
                Binding::Lit {
                    val: other,
                    dtc,
                    t: None,
                    op: None,
                    p_id: None,
                }
            }
        }
    }

    /// Create a binding from a flake's object value with full metadata including transaction time.
    ///
    /// This is the preferred method when building bindings from flake matches,
    /// as it preserves all metadata including the transaction time for `@t` bindings.
    pub fn from_object_with_t(val: FlakeValue, dt: Sid, meta: Option<FlakeMeta>, t: i64) -> Self {
        match val {
            FlakeValue::Ref(sid) => Binding::Sid(sid),
            other => {
                let dtc = match meta.and_then(|m| m.lang.map(Arc::from)) {
                    Some(lang) => DatatypeConstraint::LangTag(lang),
                    None => DatatypeConstraint::Explicit(dt),
                };
                Binding::Lit {
                    val: other,
                    dtc,
                    t: Some(t),
                    op: None,
                    p_id: None,
                }
            }
        }
    }

    /// Create a binding from a flake's object value with full metadata including t and op.
    ///
    /// This is used for history mode queries where both the transaction time and
    /// operation type (assert/retract) need to be captured.
    pub fn from_object_with_t_op(
        val: FlakeValue,
        dt: Sid,
        meta: Option<FlakeMeta>,
        t: i64,
        op: bool,
    ) -> Self {
        match val {
            FlakeValue::Ref(sid) => Binding::Sid(sid),
            other => {
                let dtc = match meta.and_then(|m| m.lang.map(Arc::from)) {
                    Some(lang) => DatatypeConstraint::LangTag(lang),
                    None => DatatypeConstraint::Explicit(dt),
                };
                Binding::Lit {
                    val: other,
                    dtc,
                    t: Some(t),
                    op: Some(op),
                    p_id: None,
                }
            }
        }
    }

    /// Create a binding from a raw IRI string.
    ///
    /// Used for graph source results where IRIs are generated from templates
    /// and may not exist in the namespace table. The IRI is kept as a full
    /// string rather than being encoded to a SID.
    pub fn iri(iri: impl Into<Arc<str>>) -> Self {
        Binding::Iri(iri.into())
    }

    /// Check if this binding is bound (not Unbound)
    ///
    /// Note: A Poisoned binding is considered "bound" in that it has a definite
    /// state (the variable failed to match in an OPTIONAL). For checking whether
    /// a binding can participate in matching, use `is_matchable()`.
    pub fn is_bound(&self) -> bool {
        !matches!(self, Binding::Unbound)
    }

    /// Check if this binding is poisoned (from failed OPTIONAL)
    ///
    /// Poisoned bindings block future pattern matching - any pattern that
    /// references a poisoned variable will yield no matches.
    pub fn is_poisoned(&self) -> bool {
        matches!(self, Binding::Poisoned)
    }

    /// Check if this binding is effectively unbound (Unbound or Poisoned).
    ///
    /// For VALUES compatibility and merge semantics, both Unbound and Poisoned
    /// act as wildcards — they match any value and can be filled in by the
    /// VALUES row.  Poisoned arises from failed OPTIONAL; semantically the
    /// variable has no value.
    pub fn is_unbound_or_poisoned(&self) -> bool {
        matches!(self, Binding::Unbound | Binding::Poisoned)
    }

    /// Check if this binding can participate in pattern matching
    ///
    /// Returns true for Sid, IriMatch, Iri, and Lit bindings - values that can be used to
    /// constrain index lookups or join operations.
    ///
    /// Returns false for:
    /// - Unbound: variable not yet assigned
    /// - Poisoned: variable from failed OPTIONAL (blocks matching)
    pub fn is_matchable(&self) -> bool {
        matches!(
            self,
            Binding::Sid(_) | Binding::IriMatch { .. } | Binding::Iri(_) | Binding::Lit { .. }
        )
    }

    /// Check if this is a reference/Sid binding (not IriMatch)
    pub fn is_sid(&self) -> bool {
        matches!(self, Binding::Sid(_))
    }

    /// Check if this is an IriMatch binding (multi-ledger IRI reference)
    pub fn is_iri_match(&self) -> bool {
        matches!(self, Binding::IriMatch { .. })
    }

    /// Check if this binding represents an IRI reference (Sid, IriMatch, or Iri)
    pub fn is_iri_type(&self) -> bool {
        matches!(
            self,
            Binding::Sid(_)
                | Binding::IriMatch { .. }
                | Binding::Iri(_)
                | Binding::EncodedSid { .. }
                | Binding::EncodedPid { .. }
        )
    }

    /// Check if this is a literal binding
    pub fn is_lit(&self) -> bool {
        matches!(self, Binding::Lit { .. } | Binding::EncodedLit { .. })
    }

    /// Check if this is an encoded (late-materialized) literal binding.
    pub fn is_encoded_lit(&self) -> bool {
        matches!(self, Binding::EncodedLit { .. })
    }

    /// Check if this is an encoded subject ID binding.
    pub fn is_encoded_sid(&self) -> bool {
        matches!(self, Binding::EncodedSid { .. })
    }

    /// Check if this is an encoded predicate ID binding.
    pub fn is_encoded_pid(&self) -> bool {
        matches!(self, Binding::EncodedPid { .. })
    }

    /// Check if this is any encoded (late-materialized) binding.
    ///
    /// Returns true for `EncodedLit`, `EncodedSid`, and `EncodedPid`.
    pub fn is_encoded(&self) -> bool {
        matches!(
            self,
            Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. }
        )
    }

    /// Get the raw s_id from an EncodedSid binding.
    pub fn encoded_s_id(&self) -> Option<u64> {
        match self {
            Binding::EncodedSid { s_id } => Some(*s_id),
            _ => None,
        }
    }

    /// Get the raw p_id from an EncodedPid binding.
    pub fn encoded_p_id(&self) -> Option<u32> {
        match self {
            Binding::EncodedPid { p_id } => Some(*p_id),
            _ => None,
        }
    }

    /// Try to get as Sid (only for Binding::Sid, not IriMatch).
    ///
    /// Returns `None` for `EncodedSid` — if your query may use late
    /// materialization (binary scan with epoch=0), use `GraphDbRef::eager()`
    /// to force resolved bindings.
    pub fn as_sid(&self) -> Option<&Sid> {
        debug_assert!(
            !self.is_encoded_sid(),
            "as_sid() called on EncodedSid — use GraphDbRef::eager() for infrastructure queries"
        );
        match self {
            Binding::Sid(sid) => Some(sid),
            _ => None,
        }
    }

    /// Try to get a SID for use in a specific ledger
    ///
    /// For `Sid`: returns the SID directly
    /// For `IriMatch`: returns the primary_sid (caller must verify ledger match)
    /// For others: returns None
    pub fn get_sid_for_ledger(&self, _ledger_alias: &str) -> Option<&Sid> {
        match self {
            Binding::Sid(sid) => Some(sid),
            Binding::IriMatch { primary_sid, .. } => Some(primary_sid),
            _ => None,
        }
    }

    /// Try to get as IriMatch
    pub fn as_iri_match(&self) -> Option<(&Arc<str>, &Sid, &Arc<str>)> {
        match self {
            Binding::IriMatch {
                iri,
                primary_sid,
                ledger_alias,
            } => Some((iri, primary_sid, ledger_alias)),
            _ => None,
        }
    }

    /// Get the canonical IRI string if this binding represents an IRI reference
    ///
    /// Returns the IRI for:
    /// - `IriMatch`: the canonical IRI
    /// - `Iri`: the raw IRI string
    /// - `Sid`: None (would need decode, use `decode_to_iri` with db instead)
    pub fn get_iri(&self) -> Option<&Arc<str>> {
        match self {
            Binding::IriMatch { iri, .. } => Some(iri),
            Binding::Iri(iri) => Some(iri),
            _ => None,
        }
    }

    /// Get the ledger alias if this is an IriMatch binding
    pub fn get_ledger_alias(&self) -> Option<&Arc<str>> {
        match self {
            Binding::IriMatch { ledger_alias, .. } => Some(ledger_alias),
            _ => None,
        }
    }

    /// Create an IriMatch binding
    ///
    /// Used when scanning in multi-ledger mode to create bindings that
    /// carry both the canonical IRI and the originating ledger's SID.
    pub fn iri_match(
        iri: impl Into<Arc<str>>,
        primary_sid: Sid,
        ledger_alias: impl Into<Arc<str>>,
    ) -> Self {
        Binding::IriMatch {
            iri: iri.into(),
            primary_sid,
            ledger_alias: ledger_alias.into(),
        }
    }

    /// Try to get literal value.
    ///
    /// Returns `None` for `EncodedLit` — if your query may use late
    /// materialization (binary scan with epoch=0), use `GraphDbRef::eager()`
    /// to force resolved bindings.
    pub fn as_lit(&self) -> Option<(&FlakeValue, &DatatypeConstraint)> {
        debug_assert!(
            !self.is_encoded_lit(),
            "as_lit() called on EncodedLit — use GraphDbRef::eager() for infrastructure queries"
        );
        match self {
            Binding::Lit { val, dtc, .. } => Some((val, dtc)),
            _ => None,
        }
    }

    /// Get the operation type if this is a Lit binding with op set
    pub fn op(&self) -> Option<bool> {
        match self {
            Binding::Lit { op, .. } => *op,
            _ => None,
        }
    }

    /// Get the transaction time if this is a Lit binding with t set
    pub fn t(&self) -> Option<i64> {
        match self {
            Binding::Lit { t, .. } => *t,
            _ => None,
        }
    }

    /// Check if this is a grouped binding (produced by GROUP BY)
    pub fn is_grouped(&self) -> bool {
        matches!(self, Binding::Grouped(_))
    }

    /// Try to get grouped values
    pub fn as_grouped(&self) -> Option<&[Binding]> {
        match self {
            Binding::Grouped(values) => Some(values),
            _ => None,
        }
    }

    /// Try to get owned grouped values
    pub fn into_grouped(self) -> Option<Vec<Binding>> {
        match self {
            Binding::Grouped(values) => Some(values),
            _ => None,
        }
    }

    /// Compare for join operations with same-ledger SID optimization.
    ///
    /// Unlike `PartialEq`, this method optimizes for same-ledger comparisons:
    /// - If both `IriMatch` bindings are from the same ledger, compare SIDs (faster)
    /// - If from different ledgers, compare canonical IRIs (correct cross-ledger semantics)
    ///
    /// This is preferred for join operators where bindings carry ledger provenance.
    ///
    /// # Why not use `PartialEq`?
    ///
    /// `PartialEq` must be consistent with `Hash` for use in `HashMap`/`HashSet`.
    /// Since `Hash` uses the IRI, `PartialEq` must also use IRI comparison.
    /// This method allows join operators to bypass IRI comparison when safe.
    pub fn eq_for_join(&self, other: &Self) -> bool {
        match (self, other) {
            // Same-ledger optimization for IriMatch
            (
                Binding::IriMatch {
                    primary_sid: sid_a,
                    ledger_alias: alias_a,
                    iri: iri_a,
                },
                Binding::IriMatch {
                    primary_sid: sid_b,
                    ledger_alias: alias_b,
                    iri: iri_b,
                },
            ) => {
                // Fast path: same ledger -> SID comparison (cheaper than string comparison)
                if Arc::ptr_eq(alias_a, alias_b) || alias_a == alias_b {
                    sid_a == sid_b
                } else {
                    // Cross-ledger: must use IRI comparison
                    iri_a == iri_b
                }
            }
            // All other cases delegate to PartialEq
            _ => self == other,
        }
    }
}

/// Compute the Effective Boolean Value (EBV) of a Binding.
///
/// EBV is used in SPARQL FILTER and conditional expressions.
/// See: <https://www.w3.org/TR/sparql11-query/#ebv>
///
/// - Bound values (Sid, IriMatch, Iri, Lit, Encoded*) are truthy
/// - Lit with Boolean(false) is falsy
/// - Unbound and Poisoned are falsy
/// - Grouped is falsy (should not appear in filter evaluation)
impl From<&Binding> for bool {
    fn from(binding: &Binding) -> bool {
        match binding {
            Binding::Lit {
                val: FlakeValue::Boolean(b),
                ..
            } => *b,
            Binding::Lit { .. } => true,
            Binding::EncodedLit { .. } => true,
            Binding::Sid(_) => true,
            Binding::IriMatch { .. } => true,
            Binding::Iri(_) => true,
            Binding::EncodedSid { .. } => true,
            Binding::EncodedPid { .. } => true,
            Binding::Unbound | Binding::Poisoned => false,
            Binding::Grouped(_) => false,
        }
    }
}

impl PartialEq for Binding {
    /// Equality for bindings - used for joins, DISTINCT, GROUP BY, etc.
    ///
    /// # Multi-Ledger Semantics
    ///
    /// For cross-ledger joins, IRI comparison (not SID) is the correct semantics:
    /// - `IriMatch` vs `IriMatch`: compare canonical IRIs (different SIDs can match)
    /// - `IriMatch` vs `Iri`: compare IRI strings
    /// - `Sid` vs `Sid`: compare SIDs (only valid within single ledger)
    /// - `Sid` vs `IriMatch`: NOT equal (avoid mixing modes; indicates a bug)
    ///
    /// IMPORTANT: The `t` field (transaction time) is intentionally EXCLUDED from equality.
    /// It is metadata accessible via the `t()` function when users explicitly bind it with `@t`,
    /// but it should not affect value unification. Two literals with the same value/datatype/lang
    /// but different assertion times should still unify in joins.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Binding::Unbound, Binding::Unbound) => true,
            (Binding::Poisoned, Binding::Poisoned) => true,

            // Same-variant SID comparison (single-ledger mode)
            (Binding::Sid(a), Binding::Sid(b)) => a == b,

            // IriMatch: compare canonical IRIs (multi-ledger mode)
            // This is the key for correct cross-ledger joins
            (
                Binding::IriMatch { iri: a, .. },
                Binding::IriMatch { iri: b, .. },
            ) => a == b,

            // IriMatch vs Iri: compare IRI strings
            (Binding::IriMatch { iri: a, .. }, Binding::Iri(b)) => a == b,
            (Binding::Iri(a), Binding::IriMatch { iri: b, .. }) => a == b,

            // Plain Iri comparison
            (Binding::Iri(a), Binding::Iri(b)) => a == b,

            // Sid vs IriMatch: These should not be compared directly.
            // If this happens, it indicates mixed single/multi-ledger mode which is a bug.
            // Return false to be conservative (no accidental matches).
            (Binding::Sid(_), Binding::IriMatch { .. }) => false,
            (Binding::IriMatch { .. }, Binding::Sid(_)) => false,

            // Sid vs Iri: Cannot compare without decode context
            (Binding::Sid(_), Binding::Iri(_)) => false,
            (Binding::Iri(_), Binding::Sid(_)) => false,

            (
                Binding::Lit {
                    val: v1,
                    dtc: dtc1,
                    .. // t and op intentionally ignored - they are metadata only
                },
                Binding::Lit {
                    val: v2,
                    dtc: dtc2,
                    .. // t and op intentionally ignored - they are metadata only
                },
            ) => v1 == v2 && dtc1 == dtc2,
            // Encoded literals compare by their encoded identity (excluding t metadata).
            (
                Binding::EncodedLit {
                    o_kind: k1,
                    o_key: ok1,
                    p_id: p1,
                    dt_id: dt1,
                    lang_id: l1,
                    ..
                },
                Binding::EncodedLit {
                    o_kind: k2,
                    o_key: ok2,
                    p_id: p2,
                    dt_id: dt2,
                    lang_id: l2,
                    ..
                },
            ) => {
                // IMPORTANT:
                // - `t` is metadata and intentionally excluded (see docs above).
                // - `i_val` is list index metadata and is intentionally excluded from
                //   term identity (it should not affect DISTINCT/GROUP BY/join unification).
                // - `p_id` is only required for NUM_BIG decoding (per-predicate arena);
                //   for other kinds it must NOT affect term identity.
                if k1 != k2 || ok1 != ok2 || dt1 != dt2 || l1 != l2 {
                    return false;
                }

                let num_big = fluree_db_core::ObjKind::NUM_BIG.as_u8();
                if *k1 == num_big {
                    p1 == p2
                } else {
                    true
                }
            },

            // EncodedSid: compare by s_id directly (single-ledger only)
            (Binding::EncodedSid { s_id: a }, Binding::EncodedSid { s_id: b }) => a == b,

            // EncodedPid: compare by p_id directly (single-ledger only)
            (Binding::EncodedPid { p_id: a }, Binding::EncodedPid { p_id: b }) => a == b,

            // EncodedSid vs Sid: NOT equal (don't mix encoded/decoded modes)
            // This prevents accidental mixing which could corrupt hash structures
            (Binding::EncodedSid { .. }, Binding::Sid(_)) => false,
            (Binding::Sid(_), Binding::EncodedSid { .. }) => false,

            // EncodedPid vs Sid: NOT equal
            (Binding::EncodedPid { .. }, Binding::Sid(_)) => false,
            (Binding::Sid(_), Binding::EncodedPid { .. }) => false,

            // EncodedSid/EncodedPid vs IriMatch/Iri: NOT equal (single vs multi-ledger)
            (Binding::EncodedSid { .. }, Binding::IriMatch { .. } | Binding::Iri(_)) => false,
            (Binding::IriMatch { .. } | Binding::Iri(_), Binding::EncodedSid { .. }) => false,
            (Binding::EncodedPid { .. }, Binding::IriMatch { .. } | Binding::Iri(_)) => false,
            (Binding::IriMatch { .. } | Binding::Iri(_), Binding::EncodedPid { .. }) => false,

            (Binding::Grouped(a), Binding::Grouped(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Binding {}

impl std::hash::Hash for Binding {
    /// Hash for bindings - used for hash joins, DISTINCT, GROUP BY, etc.
    ///
    /// # Multi-Ledger Semantics
    ///
    /// For `IriMatch`, we hash the canonical IRI (not the SID or ledger_alias)
    /// so that the same IRI from different ledgers hashes to the same bucket.
    /// This is critical for hash joins across ledgers to work correctly.
    ///
    /// IMPORTANT: The `t` and `op` fields are intentionally EXCLUDED from hashing
    /// to match the equality semantics. See `PartialEq` impl for rationale.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Note: We use a custom discriminant for IriMatch and Iri so they hash
        // compatibly (both are IRI-typed values). This allows IriMatch and Iri
        // to potentially match in hash-based operations.
        match self {
            Binding::Unbound => {
                0u8.hash(state);
            }
            Binding::Poisoned => {
                1u8.hash(state);
            }
            Binding::Sid(sid) => {
                2u8.hash(state);
                sid.hash(state);
            }
            Binding::IriMatch { iri, .. } => {
                // Use same discriminant as Iri for compatible hashing
                3u8.hash(state);
                iri.hash(state);
            }
            Binding::Iri(iri) => {
                // Same discriminant as IriMatch
                3u8.hash(state);
                iri.hash(state);
            }
            Binding::Lit { val, dtc, .. } => {
                // t and op intentionally excluded - they are metadata only
                4u8.hash(state);
                val.hash(state);
                dtc.hash(state);
            }
            Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id,
                ..
            } => {
                // t intentionally excluded - metadata only
                6u8.hash(state);
                o_kind.hash(state);
                o_key.hash(state);
                dt_id.hash(state);
                lang_id.hash(state);
                // `p_id` is only required for NUM_BIG decoding (per-predicate arena).
                // For all other literal kinds, it must not affect hash identity.
                if *o_kind == fluree_db_core::ObjKind::NUM_BIG.as_u8() {
                    p_id.hash(state);
                }
            }
            Binding::EncodedSid { s_id } => {
                // Distinct discriminant from Sid (2) - they are not interchangeable
                7u8.hash(state);
                s_id.hash(state);
            }
            Binding::EncodedPid { p_id } => {
                // Distinct discriminant - predicates are not subjects
                8u8.hash(state);
                p_id.hash(state);
            }
            Binding::Grouped(values) => {
                5u8.hash(state);
                values.len().hash(state);
                for v in values {
                    v.hash(state);
                }
            }
        }
    }
}

/// Error type for batch operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchError {
    /// Column lengths don't match
    ColumnLengthMismatch {
        expected: usize,
        got: usize,
        column: usize,
    },
    /// Schema has duplicate VarIds
    DuplicateVarId(VarId),
    /// Schema length doesn't match columns length
    SchemaColumnMismatch {
        schema_len: usize,
        columns_len: usize,
    },
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchError::ColumnLengthMismatch {
                expected,
                got,
                column,
            } => {
                write!(f, "Column {column} has {got} rows, expected {expected}")
            }
            BatchError::DuplicateVarId(id) => {
                write!(f, "Duplicate VarId {id:?} in schema")
            }
            BatchError::SchemaColumnMismatch {
                schema_len,
                columns_len,
            } => {
                write!(
                    f,
                    "Schema has {schema_len} vars but {columns_len} columns provided"
                )
            }
        }
    }
}

impl std::error::Error for BatchError {}

/// A batch of solutions - columnar for efficient processing
///
/// # Invariants
///
/// - `columns.len() == schema.len()`
/// - All columns have exactly `len` elements
/// - Schema contains no duplicate VarIds
#[derive(Debug, Clone)]
pub struct Batch {
    /// Number of rows in this batch
    len: usize,
    /// Schema: which variables this batch contains, in column order
    schema: Arc<[VarId]>,
    /// One column per variable in schema order
    columns: Vec<Vec<Binding>>,
}

impl Batch {
    /// Create a new batch with schema, enforcing invariants
    pub fn new(schema: Arc<[VarId]>, columns: Vec<Vec<Binding>>) -> Result<Self, BatchError> {
        // Check schema/columns length match
        if schema.len() != columns.len() {
            return Err(BatchError::SchemaColumnMismatch {
                schema_len: schema.len(),
                columns_len: columns.len(),
            });
        }

        // Check for duplicates (schema length is typically very small, so O(n^2) is fine)
        for (i, &var_id) in schema.iter().enumerate() {
            if schema.iter().take(i).any(|&v| v == var_id) {
                return Err(BatchError::DuplicateVarId(var_id));
            }
        }

        // Determine row count (from first column, or 0 if no columns)
        let len = columns.first().map(std::vec::Vec::len).unwrap_or(0);

        // Check all columns have same length
        for (i, col) in columns.iter().enumerate() {
            if col.len() != len {
                return Err(BatchError::ColumnLengthMismatch {
                    expected: len,
                    got: col.len(),
                    column: i,
                });
            }
        }

        Ok(Self {
            len,
            schema,
            columns,
        })
    }

    /// Create an empty batch with given schema (zero rows)
    pub fn empty(schema: Arc<[VarId]>) -> Result<Self, BatchError> {
        let columns = schema.iter().map(|_| Vec::new()).collect();
        Self::new(schema, columns)
    }

    /// Create a batch with a single row
    pub fn single_row(schema: Arc<[VarId]>, row: Vec<Binding>) -> Result<Self, BatchError> {
        if schema.len() != row.len() {
            return Err(BatchError::SchemaColumnMismatch {
                schema_len: schema.len(),
                columns_len: row.len(),
            });
        }
        let columns = row.into_iter().map(|b| vec![b]).collect();
        Self::new(schema, columns)
    }

    /// Create a batch representing a single empty solution (1 row, 0 columns).
    ///
    /// This is used for queries with an empty WHERE clause. A plain `Batch::new` with
    /// an empty schema would report `len=0` (since there is no first column to infer
    /// row count), which breaks LIMIT/OFFSET/DISTINCT semantics.
    pub fn single_empty() -> Self {
        Self {
            len: 1,
            schema: Arc::from(Vec::new().into_boxed_slice()),
            columns: Vec::new(),
        }
    }

    /// Create a batch with an empty schema (0 columns) and an explicit row count.
    ///
    /// This is needed for operators like JOIN/EXISTS that may produce solutions that
    /// introduce no variables (e.g., all-bound patterns) but still must represent
    /// the existence of one-or-more rows.
    pub fn empty_schema_with_len(len: usize) -> Self {
        Self {
            len,
            schema: Arc::from(Vec::new().into_boxed_slice()),
            columns: Vec::new(),
        }
    }

    /// Number of rows
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the schema (variables in column order)
    pub fn schema(&self) -> &[VarId] {
        &self.schema
    }

    /// Get binding by VarId (linear scan over schema; schema is typically tiny)
    ///
    /// Returns None if VarId is not in schema or row is out of bounds.
    pub fn get(&self, row: usize, var: VarId) -> Option<&Binding> {
        let col_idx = self.schema.iter().position(|&v| v == var)?;
        self.columns.get(col_idx)?.get(row)
    }

    /// Get binding by column index directly (when schema position is known)
    ///
    /// # Panics
    ///
    /// Panics if column or row is out of bounds.
    pub fn get_by_col(&self, row: usize, col: usize) -> &Binding {
        &self.columns[col][row]
    }

    /// Get a column by VarId
    pub fn column(&self, var: VarId) -> Option<&[Binding]> {
        let col_idx = self.schema.iter().position(|&v| v == var)?;
        Some(&self.columns[col_idx])
    }

    /// Get a column by index
    pub fn column_by_idx(&self, col: usize) -> Option<&[Binding]> {
        self.columns.get(col).map(std::vec::Vec::as_slice)
    }

    /// Zero-copy view selecting specific columns
    pub fn project_view(&self, vars: &[VarId]) -> Option<BatchView<'_>> {
        let col_indices: Vec<usize> = vars
            .iter()
            .map(|&v| self.schema.iter().position(|&sv| sv == v))
            .collect::<Option<Vec<_>>>()?;

        Some(BatchView {
            batch: self,
            col_indices,
            projected_schema: Arc::from(vars.to_vec().into_boxed_slice()),
        })
    }

    /// Materializing projection - clones selected columns into new Batch
    pub fn project(&self, vars: &[VarId]) -> Option<Self> {
        let view = self.project_view(vars)?;
        Some(view.to_batch())
    }

    /// Decompose into `(schema, columns, len)` for in-place column transformation
    /// pipelines. Recompose via [`Batch::from_parts`], which preserves `len`
    /// even when `columns` is empty.
    pub fn into_parts(self) -> (Arc<[VarId]>, Vec<Vec<Binding>>, usize) {
        (self.schema, self.columns, self.len)
    }

    /// Reconstruct a `Batch` from explicit `(schema, columns, len)`.
    ///
    /// Unlike [`Batch::new`], which *infers* `len` from the first column and
    /// therefore reports `len = 0` whenever `columns` is empty, this preserves
    /// the caller-supplied row count. That matters for empty-schema batches
    /// (e.g. produced by [`Batch::empty_schema_with_len`] or by projecting
    /// away every variable while preserving the WHERE solution count) — the
    /// downstream flake-generation loop iterates `len` times, so losing `len`
    /// silently changes "fire once per solution row" into "fire once total"
    /// for all-literal templates.
    ///
    /// Validates:
    /// - `schema.len() == columns.len()`
    /// - no duplicate `VarId` in `schema`
    /// - every column has exactly `len` bindings
    pub fn from_parts(
        schema: Arc<[VarId]>,
        columns: Vec<Vec<Binding>>,
        len: usize,
    ) -> Result<Self, BatchError> {
        if schema.len() != columns.len() {
            return Err(BatchError::SchemaColumnMismatch {
                schema_len: schema.len(),
                columns_len: columns.len(),
            });
        }
        for (i, &var_id) in schema.iter().enumerate() {
            if schema.iter().take(i).any(|&v| v == var_id) {
                return Err(BatchError::DuplicateVarId(var_id));
            }
        }
        for (i, col) in columns.iter().enumerate() {
            if col.len() != len {
                return Err(BatchError::ColumnLengthMismatch {
                    expected: len,
                    got: col.len(),
                    column: i,
                });
            }
        }
        Ok(Self {
            len,
            schema,
            columns,
        })
    }

    /// Owning projection: drop columns not in `vars` without cloning the kept ones.
    ///
    /// Vars in `vars` that aren't present in the schema are silently ignored.
    /// Duplicate entries in `vars` are also ignored — each schema column is
    /// kept at most once (otherwise `mem::take` would empty the source column
    /// on the second hit and break the per-column row-count invariant).
    /// If no requested var matches, returns a batch with empty schema and the
    /// original row count preserved (so all-literal templates iterate correctly).
    pub fn project_owned(mut self, vars: &[VarId]) -> Self {
        let mut col_indices: Vec<usize> = Vec::with_capacity(vars.len());
        let mut new_schema: Vec<VarId> = Vec::with_capacity(vars.len());
        for &v in vars {
            // Skip duplicates — schemas are typically tiny so linear search is fine.
            if new_schema.contains(&v) {
                continue;
            }
            if let Some(idx) = self.schema.iter().position(|&sv| sv == v) {
                col_indices.push(idx);
                new_schema.push(v);
            }
        }

        // Already a no-op (kept everything in original order)
        if col_indices.len() == self.schema.len()
            && col_indices.iter().enumerate().all(|(i, &idx)| i == idx)
        {
            return self;
        }

        if new_schema.is_empty() {
            return Self::empty_schema_with_len(self.len);
        }

        let mut moved: Vec<Vec<Binding>> = Vec::with_capacity(new_schema.len());
        for &idx in &col_indices {
            moved.push(std::mem::take(&mut self.columns[idx]));
        }

        Self {
            len: self.len,
            schema: Arc::from(new_schema.into_boxed_slice()),
            columns: moved,
        }
    }

    /// View a single row without allocation
    pub fn row_view(&self, row: usize) -> Option<RowView<'_>> {
        if row < self.len {
            Some(RowView { batch: self, row })
        } else {
            None
        }
    }

    /// Iterate over rows as RowViews
    pub fn rows(&self) -> impl Iterator<Item = RowView<'_>> {
        (0..self.len).map(move |row| RowView { batch: self, row })
    }

    /// Retain only the specified variables, dropping all other columns.
    ///
    /// The output preserves the original schema order (i.e. columns that
    /// appear earlier in `self.schema` will still appear earlier in the
    /// result).  The order of `vars` does NOT affect the output order —
    /// it is treated as a *set* of variables to keep.
    ///
    /// Returns `self` unchanged when `vars` is a superset of (or equal
    /// to) the current schema.  Returns `None` if any variable in `vars`
    /// is not present in the current schema.
    pub fn retain(self, new_schema: Arc<[VarId]>) -> Self {
        // Fast path: nothing to trim.
        if new_schema.len() == self.schema.len() {
            return self;
        }

        // Single pass: filter schema to retained vars in schema order,
        // moving columns instead of cloning since `self` is consumed.
        let len = self.len;
        let new_columns: Vec<Vec<Binding>> = self
            .schema
            .iter()
            .zip(self.columns)
            .filter_map(|(var, col)| new_schema.contains(var).then_some(col))
            .collect();

        debug_assert_eq!(
            new_schema.len(),
            new_columns.len(),
            "Batch::retain: requested variables not present in schema"
        );

        Self {
            len,
            schema: new_schema,
            columns: new_columns,
        }
    }
}

/// Zero-copy view of selected columns in a batch
#[derive(Debug, Clone)]
pub struct BatchView<'a> {
    batch: &'a Batch,
    /// Column indices into batch.columns (not VarIds)
    col_indices: Vec<usize>,
    /// Schema of this view (subset of batch schema)
    projected_schema: Arc<[VarId]>,
}

impl<'a> BatchView<'a> {
    /// Get binding by column index within this view (0..col_indices.len())
    pub fn get(&self, row: usize, col_idx: usize) -> Option<&'a Binding> {
        let batch_col = *self.col_indices.get(col_idx)?;
        self.batch.columns.get(batch_col)?.get(row)
    }

    /// Schema of this view (subset of batch schema)
    pub fn schema(&self) -> &[VarId] {
        &self.projected_schema
    }

    /// Number of rows
    pub fn len(&self) -> usize {
        self.batch.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Number of columns in this view
    pub fn num_columns(&self) -> usize {
        self.col_indices.len()
    }

    /// Materialize into owned Batch
    pub fn to_batch(&self) -> Batch {
        let columns: Vec<Vec<Binding>> = self
            .col_indices
            .iter()
            .map(|&col_idx| self.batch.columns[col_idx].clone())
            .collect();

        // We know this is valid because it came from a valid batch
        Batch {
            len: self.batch.len,
            schema: self.projected_schema.clone(),
            columns,
        }
    }
}

/// Trait for types that provide access to bindings by variable ID.
///
/// This abstraction allows expression evaluation to work with both:
/// - `RowView` - a view into a batch row
/// - `BindingRow` - a lightweight view over a slice of bindings
///
/// Using this trait enables pre-batch filtering where we evaluate filters
/// on bindings before constructing a full batch.
pub trait RowAccess {
    /// Get a binding by variable ID.
    fn get(&self, var: VarId) -> Option<&Binding>;
}

/// Zero-copy view of a single row in a batch
#[derive(Debug, Clone, Copy)]
pub struct RowView<'a> {
    batch: &'a Batch,
    row: usize,
}

impl RowAccess for RowView<'_> {
    fn get(&self, var: VarId) -> Option<&Binding> {
        self.batch.get(self.row, var)
    }
}

impl<'a> RowView<'a> {
    /// Get binding by column index
    pub fn get_by_col(&self, col: usize) -> Option<&'a Binding> {
        self.batch.columns.get(col)?.get(self.row)
    }

    /// Get the row index
    pub fn row_index(&self) -> usize {
        self.row
    }

    /// Convert to owned Vec of bindings
    pub fn to_vec(&self) -> Vec<Binding> {
        self.batch
            .columns
            .iter()
            .map(|col| col[self.row].clone())
            .collect()
    }
}

/// Lightweight view over a slice of bindings for pre-batch filter evaluation.
///
/// This struct enables evaluating filter expressions on bindings before they're
/// added to a batch, avoiding allocations for rows that will be filtered out.
#[derive(Debug, Clone, Copy)]
pub struct BindingRow<'a> {
    schema: &'a [VarId],
    bindings: &'a [Binding],
}

impl<'a> BindingRow<'a> {
    /// Create a new binding row view.
    ///
    /// # Panics
    ///
    /// Panics if `schema.len() != bindings.len()`.
    pub fn new(schema: &'a [VarId], bindings: &'a [Binding]) -> Self {
        debug_assert_eq!(
            schema.len(),
            bindings.len(),
            "schema and bindings must have same length"
        );
        Self { schema, bindings }
    }
}

impl RowAccess for BindingRow<'_> {
    fn get(&self, var: VarId) -> Option<&Binding> {
        self.schema
            .iter()
            .position(|&v| v == var)
            .and_then(|idx| self.bindings.get(idx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sid() -> Sid {
        Sid::new(100, "test")
    }

    fn xsd_long() -> Sid {
        Sid::new(2, "long")
    }

    fn xsd_string() -> Sid {
        Sid::new(2, "string")
    }

    #[test]
    fn test_binding_from_object_ref() {
        let sid = test_sid();
        let binding = Binding::from_object(FlakeValue::Ref(sid.clone()), xsd_long());

        // Should become Sid, not Lit
        assert!(binding.is_sid());
        assert_eq!(binding.as_sid(), Some(&sid));
    }

    #[test]
    fn test_binding_from_object_lit() {
        let binding = Binding::from_object(FlakeValue::Long(42), xsd_long());

        assert!(binding.is_lit());
        let (val, dtc) = binding.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(42));
        assert_eq!(dtc.datatype().name.as_ref(), "long");
        assert!(dtc.lang_tag().is_none());
    }

    // `Binding::lit` uses `debug_assert!` to enforce the "no Ref in Lit" invariant.
    // So this should panic in debug builds, but not in `--release`.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "Lit cannot contain Ref")]
    fn test_binding_lit_rejects_ref() {
        Binding::lit(FlakeValue::Ref(test_sid()), xsd_long());
    }

    #[test]
    fn test_batch_new() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![Binding::Sid(test_sid()), Binding::Unbound],
            vec![
                Binding::lit(FlakeValue::Long(1), xsd_long()),
                Binding::lit(FlakeValue::Long(2), xsd_long()),
            ],
        ];

        let batch = Batch::new(schema, columns).unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.schema().len(), 2);
    }

    #[test]
    fn test_batch_column_length_mismatch() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![Binding::Unbound, Binding::Unbound],
            vec![Binding::Unbound], // Wrong length
        ];

        let result = Batch::new(schema, columns);
        assert!(matches!(
            result,
            Err(BatchError::ColumnLengthMismatch { .. })
        ));
    }

    #[test]
    fn test_batch_duplicate_varid() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(0)].into_boxed_slice()); // Duplicate
        let columns = vec![vec![Binding::Unbound], vec![Binding::Unbound]];

        let result = Batch::new(schema, columns);
        assert!(matches!(result, Err(BatchError::DuplicateVarId(_))));
    }

    #[test]
    fn test_batch_get() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::Sid(Sid::new(1, "a")),
                Binding::Sid(Sid::new(1, "b")),
            ],
            vec![
                Binding::lit(FlakeValue::Long(10), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long()),
            ],
        ];

        let batch = Batch::new(schema, columns).unwrap();

        // Get by VarId
        let b = batch.get(0, VarId(0)).unwrap();
        assert!(matches!(b, Binding::Sid(_)));

        let b = batch.get(1, VarId(1)).unwrap();
        let (val, _) = b.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(20));

        // Invalid VarId
        assert!(batch.get(0, VarId(99)).is_none());

        // Out of bounds row
        assert!(batch.get(99, VarId(0)).is_none());
    }

    #[test]
    fn test_batch_project_owned_dedups_duplicate_input_vars() {
        // Regression: project_owned must tolerate duplicate VarIds in `vars`
        // without producing an invalid batch. Earlier behavior `mem::take`d
        // the same source column twice — the second take produced an empty
        // Vec while `len` was still the original row count, violating the
        // batch invariant that all columns match `len`. The result schema
        // also contained a duplicate VarId, which `Batch::new` rejects.
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::Long(10), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long()),
            ],
            vec![
                Binding::lit(FlakeValue::Long(100), xsd_long()),
                Binding::lit(FlakeValue::Long(200), xsd_long()),
            ],
        ];
        let batch = Batch::new(schema, columns).unwrap();

        // Duplicate VarId(0) in the request must be deduplicated.
        let projected = batch.project_owned(&[VarId(0), VarId(0), VarId(1)]);

        assert_eq!(projected.schema(), &[VarId(0), VarId(1)]);
        assert_eq!(projected.len(), 2);
        // All retained columns must match the row count — no empty placeholder
        // produced by a second mem::take of the same source column.
        for (i, col) in projected.columns.iter().enumerate() {
            assert_eq!(
                col.len(),
                projected.len(),
                "column {i} length must match batch len after dedup"
            );
        }

        // The kept columns must carry the original data, not a swapped-in
        // empty Vec.
        let (v, _) = projected.get(0, VarId(0)).unwrap().as_lit().unwrap();
        assert_eq!(*v, FlakeValue::Long(10));
        let (v, _) = projected.get(1, VarId(1)).unwrap().as_lit().unwrap();
        assert_eq!(*v, FlakeValue::Long(200));
    }

    #[test]
    fn test_batch_project_owned_unknown_vars_ignored() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![Binding::lit(FlakeValue::Long(1), xsd_long())],
            vec![Binding::lit(FlakeValue::Long(2), xsd_long())],
        ];
        let batch = Batch::new(schema, columns).unwrap();

        // VarId(99) is absent — silently skipped, leaving only VarId(0).
        let projected = batch.project_owned(&[VarId(99), VarId(0)]);
        assert_eq!(projected.schema(), &[VarId(0)]);
        assert_eq!(projected.len(), 1);
    }

    #[test]
    fn test_batch_project_owned_empty_vars_preserves_row_count() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let columns = vec![vec![
            Binding::lit(FlakeValue::Long(1), xsd_long()),
            Binding::lit(FlakeValue::Long(2), xsd_long()),
            Binding::lit(FlakeValue::Long(3), xsd_long()),
        ]];
        let batch = Batch::new(schema, columns).unwrap();

        // No vars retained — row count must be preserved so all-literal
        // templates still iterate once per WHERE solution.
        let projected = batch.project_owned(&[]);
        assert_eq!(projected.schema(), &[] as &[VarId]);
        assert_eq!(projected.len(), 3);
        assert!(!projected.is_empty());
    }

    #[test]
    fn test_batch_new_loses_len_for_empty_schema() {
        // Pin the surprising-but-real Batch::new behavior: with no columns,
        // it infers len = 0 regardless of the caller's intent. This is why
        // `from_parts` exists — pipelines that round-trip via
        // `into_parts` -> mutate -> reconstruct must use `from_parts` or
        // they will silently lose the row count for empty-schema batches.
        let schema: Arc<[VarId]> = Arc::from(Vec::<VarId>::new().into_boxed_slice());
        let columns: Vec<Vec<Binding>> = Vec::new();
        let inferred = Batch::new(schema, columns).unwrap();
        assert_eq!(
            inferred.len(),
            0,
            "Batch::new with no columns infers len = 0"
        );
    }

    #[test]
    fn test_batch_from_parts_preserves_len_for_empty_schema() {
        // Counterpart to `test_batch_new_loses_len_for_empty_schema`:
        // `from_parts` is the proper inverse of `into_parts` and preserves
        // the explicit row count even when `columns` is empty.
        let original = Batch::empty_schema_with_len(3);
        let (schema, columns, len) = original.into_parts();
        let reconstructed = Batch::from_parts(schema, columns, len).unwrap();
        assert_eq!(reconstructed.len(), 3);
        assert_eq!(reconstructed.schema(), &[] as &[VarId]);
        assert!(!reconstructed.is_empty());
    }

    #[test]
    fn test_batch_from_parts_round_trips_normal_batch() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![Binding::Sid(test_sid()), Binding::Unbound],
            vec![
                Binding::lit(FlakeValue::Long(1), xsd_long()),
                Binding::lit(FlakeValue::Long(2), xsd_long()),
            ],
        ];
        let original = Batch::new(schema, columns).unwrap();
        let original_len = original.len();
        let (schema, columns, len) = original.into_parts();
        let reconstructed = Batch::from_parts(schema, columns, len).unwrap();
        assert_eq!(reconstructed.len(), original_len);
        assert_eq!(reconstructed.schema().len(), 2);
    }

    #[test]
    fn test_batch_from_parts_rejects_column_length_mismatch() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let columns = vec![vec![Binding::Unbound]]; // len 1
        let result = Batch::from_parts(schema, columns, 2); // claim len 2
        assert!(matches!(
            result,
            Err(BatchError::ColumnLengthMismatch { .. })
        ));
    }

    #[test]
    fn test_batch_project_view() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns = vec![
            vec![Binding::lit(FlakeValue::Long(1), xsd_long())],
            vec![Binding::lit(FlakeValue::Long(2), xsd_long())],
            vec![Binding::lit(FlakeValue::Long(3), xsd_long())],
        ];

        let batch = Batch::new(schema, columns).unwrap();

        // Project to columns 0 and 2
        let view = batch.project_view(&[VarId(0), VarId(2)]).unwrap();

        assert_eq!(view.len(), 1);
        assert_eq!(view.num_columns(), 2);
        assert_eq!(view.schema(), &[VarId(0), VarId(2)]);

        let (val, _) = view.get(0, 0).unwrap().as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(1));

        let (val, _) = view.get(0, 1).unwrap().as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(3));
    }

    #[test]
    fn test_batch_row_view() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::String("a".into()), xsd_string()),
                Binding::lit(FlakeValue::String("b".into()), xsd_string()),
            ],
            vec![
                Binding::lit(FlakeValue::Long(1), xsd_long()),
                Binding::lit(FlakeValue::Long(2), xsd_long()),
            ],
        ];

        let batch = Batch::new(schema, columns).unwrap();

        let row = batch.row_view(0).unwrap();
        let (val, _) = row.get(VarId(0)).unwrap().as_lit().unwrap();
        assert_eq!(*val, FlakeValue::String("a".into()));

        let row = batch.row_view(1).unwrap();
        let (val, _) = row.get(VarId(1)).unwrap().as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(2));
    }

    #[test]
    fn test_batch_empty() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let batch = Batch::empty(schema).unwrap();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        assert_eq!(batch.schema().len(), 2);
    }

    #[test]
    fn test_binding_poisoned() {
        let poisoned = Binding::Poisoned;

        // is_poisoned
        assert!(poisoned.is_poisoned());
        assert!(!Binding::Unbound.is_poisoned());
        assert!(!Binding::Sid(test_sid()).is_poisoned());
        assert!(!Binding::lit(FlakeValue::Long(42), xsd_long()).is_poisoned());
    }

    #[test]
    fn test_binding_is_matchable() {
        // Poisoned is NOT matchable (blocks future pattern matching)
        assert!(!Binding::Poisoned.is_matchable());

        // Unbound is NOT matchable (not yet assigned)
        assert!(!Binding::Unbound.is_matchable());

        // Sid IS matchable
        assert!(Binding::Sid(test_sid()).is_matchable());

        // Lit IS matchable
        assert!(Binding::lit(FlakeValue::Long(42), xsd_long()).is_matchable());
    }

    #[test]
    fn test_binding_poisoned_is_bound() {
        // Poisoned is considered "bound" (has a definite state)
        assert!(Binding::Poisoned.is_bound());

        // Unbound is not bound
        assert!(!Binding::Unbound.is_bound());
    }

    #[test]
    fn test_binding_poisoned_equality() {
        // Two Poisoned bindings are equal
        assert_eq!(Binding::Poisoned, Binding::Poisoned);

        // Poisoned != Unbound
        assert_ne!(Binding::Poisoned, Binding::Unbound);

        // Poisoned != Sid
        assert_ne!(Binding::Poisoned, Binding::Sid(test_sid()));
    }

    #[test]
    fn test_binding_poisoned_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Binding::Poisoned);

        // Can insert Poisoned into a HashSet
        assert!(set.contains(&Binding::Poisoned));

        // Unbound is different
        assert!(!set.contains(&Binding::Unbound));
    }

    #[test]
    fn test_binding_grouped() {
        let values = vec![
            Binding::lit(FlakeValue::Long(1), xsd_long()),
            Binding::lit(FlakeValue::Long(2), xsd_long()),
            Binding::lit(FlakeValue::Long(3), xsd_long()),
        ];
        let grouped = Binding::Grouped(values.clone());

        // is_grouped
        assert!(grouped.is_grouped());
        assert!(!Binding::Unbound.is_grouped());
        assert!(!Binding::Sid(test_sid()).is_grouped());

        // as_grouped
        let inner = grouped.as_grouped().unwrap();
        assert_eq!(inner.len(), 3);

        // Grouped is NOT matchable (only used by aggregates)
        assert!(!grouped.is_matchable());

        // Grouped is NOT bound in the traditional sense
        // (it's an intermediate value, not a concrete binding)
        // Actually, let's define it as bound since it has a value
        // No - Grouped shouldn't participate in normal binding semantics
    }

    #[test]
    fn test_binding_grouped_equality() {
        let g1 = Binding::Grouped(vec![
            Binding::lit(FlakeValue::Long(1), xsd_long()),
            Binding::lit(FlakeValue::Long(2), xsd_long()),
        ]);
        let g2 = Binding::Grouped(vec![
            Binding::lit(FlakeValue::Long(1), xsd_long()),
            Binding::lit(FlakeValue::Long(2), xsd_long()),
        ]);
        let g3 = Binding::Grouped(vec![Binding::lit(FlakeValue::Long(1), xsd_long())]);

        assert_eq!(g1, g2);
        assert_ne!(g1, g3);
        assert_ne!(g1, Binding::Unbound);
    }

    #[test]
    fn test_binding_grouped_hash() {
        use std::collections::HashSet;

        let g1 = Binding::Grouped(vec![Binding::lit(FlakeValue::Long(1), xsd_long())]);
        let g2 = Binding::Grouped(vec![Binding::lit(FlakeValue::Long(1), xsd_long())]);

        let mut set = HashSet::new();
        set.insert(g1.clone());

        // Same contents should match
        assert!(set.contains(&g2));

        // Different Grouped should not match
        let g3 = Binding::Grouped(vec![Binding::lit(FlakeValue::Long(2), xsd_long())]);
        assert!(!set.contains(&g3));
    }

    #[test]
    fn test_binding_grouped_into() {
        let values = vec![
            Binding::lit(FlakeValue::Long(1), xsd_long()),
            Binding::lit(FlakeValue::Long(2), xsd_long()),
        ];
        let grouped = Binding::Grouped(values);

        let inner = grouped.into_grouped().unwrap();
        assert_eq!(inner.len(), 2);

        // Non-grouped returns None
        assert!(Binding::Unbound.into_grouped().is_none());
    }

    // ============================================================================
    // eq_for_join() tests - same-ledger SID optimization
    // ============================================================================

    #[test]
    fn test_eq_for_join_same_ledger_same_sid() {
        // Same ledger alias (Arc pointer equality) and same SID -> should match
        let alias: Arc<str> = Arc::from("test/ledger");
        let iri: Arc<str> = Arc::from("http://example.org/person/1");
        let sid = Sid::new(100, "person1");

        let a = Binding::IriMatch {
            primary_sid: sid.clone(),
            ledger_alias: alias.clone(),
            iri: iri.clone(),
        };
        let b = Binding::IriMatch {
            primary_sid: sid.clone(),
            ledger_alias: alias.clone(), // Same Arc
            iri: iri.clone(),
        };

        assert!(a.eq_for_join(&b));
    }

    #[test]
    fn test_eq_for_join_same_ledger_different_sid() {
        // Same ledger alias but different SIDs -> should NOT match
        let alias: Arc<str> = Arc::from("test/ledger");
        let sid_a = Sid::new(100, "person1");
        let sid_b = Sid::new(100, "person2");

        let a = Binding::IriMatch {
            primary_sid: sid_a,
            ledger_alias: alias.clone(),
            iri: Arc::from("http://example.org/person/1"),
        };
        let b = Binding::IriMatch {
            primary_sid: sid_b,
            ledger_alias: alias.clone(),
            iri: Arc::from("http://example.org/person/2"),
        };

        assert!(!a.eq_for_join(&b));
    }

    #[test]
    fn test_eq_for_join_same_ledger_value_equality() {
        // Same ledger alias (value equality, not pointer) and same SID -> should match
        // Tests that we don't just rely on Arc::ptr_eq
        let alias_a: Arc<str> = Arc::from("test/ledger");
        let alias_b: Arc<str> = Arc::from("test/ledger"); // Different Arc, same value
        let sid = Sid::new(100, "person1");
        let iri: Arc<str> = Arc::from("http://example.org/person/1");

        let a = Binding::IriMatch {
            primary_sid: sid.clone(),
            ledger_alias: alias_a,
            iri: iri.clone(),
        };
        let b = Binding::IriMatch {
            primary_sid: sid.clone(),
            ledger_alias: alias_b,
            iri: iri.clone(),
        };

        // Should use SID comparison since ledger_alias values are equal
        assert!(a.eq_for_join(&b));
    }

    #[test]
    fn test_eq_for_join_different_ledger_same_iri() {
        // Different ledgers but same IRI -> should match (cross-ledger join)
        let alias_a: Arc<str> = Arc::from("ledger_a");
        let alias_b: Arc<str> = Arc::from("ledger_b");
        let iri: Arc<str> = Arc::from("http://example.org/shared/entity");

        // Note: Different SIDs because namespace_codes are per-ledger
        let sid_a = Sid::new(100, "entity"); // namespace 100 in ledger_a
        let sid_b = Sid::new(200, "entity"); // namespace 200 in ledger_b (different!)

        let a = Binding::IriMatch {
            primary_sid: sid_a,
            ledger_alias: alias_a,
            iri: iri.clone(),
        };
        let b = Binding::IriMatch {
            primary_sid: sid_b,
            ledger_alias: alias_b,
            iri: iri.clone(),
        };

        // Should use IRI comparison since ledgers are different
        // IRIs are the same, so should match
        assert!(a.eq_for_join(&b));
    }

    #[test]
    fn test_eq_for_join_different_ledger_different_iri() {
        // Different ledgers and different IRIs -> should NOT match
        let alias_a: Arc<str> = Arc::from("ledger_a");
        let alias_b: Arc<str> = Arc::from("ledger_b");

        let a = Binding::IriMatch {
            primary_sid: Sid::new(100, "entity1"),
            ledger_alias: alias_a,
            iri: Arc::from("http://example.org/entity/1"),
        };
        let b = Binding::IriMatch {
            primary_sid: Sid::new(100, "entity2"),
            ledger_alias: alias_b,
            iri: Arc::from("http://example.org/entity/2"),
        };

        assert!(!a.eq_for_join(&b));
    }

    #[test]
    fn test_eq_for_join_non_iri_match_delegates_to_partial_eq() {
        // Non-IriMatch bindings should delegate to PartialEq
        let sid_a = test_sid();
        let sid_b = test_sid();

        let a = Binding::Sid(sid_a.clone());
        let b = Binding::Sid(sid_b.clone());

        // Should use PartialEq which compares SIDs
        assert!(a.eq_for_join(&b));

        // Different SIDs should not match
        let c = Binding::Sid(Sid::new(999, "other"));
        assert!(!a.eq_for_join(&c));
    }

    #[test]
    fn test_eq_for_join_lit_bindings() {
        // Lit bindings should work via PartialEq delegation
        let a = Binding::lit(FlakeValue::Long(42), xsd_long());
        let b = Binding::lit(FlakeValue::Long(42), xsd_long());
        let c = Binding::lit(FlakeValue::Long(99), xsd_long());

        assert!(a.eq_for_join(&b));
        assert!(!a.eq_for_join(&c));
    }

    #[test]
    fn test_eq_for_join_mixed_types() {
        // IriMatch vs Sid should not match (different variants)
        let iri_match = Binding::IriMatch {
            primary_sid: test_sid(),
            ledger_alias: Arc::from("test/ledger"),
            iri: Arc::from("http://example.org/test"),
        };
        let sid = Binding::Sid(test_sid());

        assert!(!iri_match.eq_for_join(&sid));
    }
}
