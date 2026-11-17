//! Streaming GROUP BY + Aggregate operator for O(groups) memory usage.
//!
//! The standard `GroupByOperator` + `AggregateOperator` pipeline stores O(rows) in memory
//! because it collects all rows before computing aggregates. This is problematic for
//! queries like:
//!
//! ```sparql
//! SELECT ?venue (COUNT(?paper) as ?count)
//! WHERE { ?paper dblp:publishedIn ?venue . }
//! GROUP BY ?venue
//! ```
//!
//! With millions of papers but thousands of venues, storing all papers per venue
//! uses gigabytes of memory when we only need to count them.
//!
//! `GroupAggregateOperator` solves this by:
//! 1. Computing aggregates incrementally as rows arrive (streaming)
//! 2. Storing only `O(groups)` state, not `O(rows)`
//! 3. Using `JoinKey` for group keys (no decoding in single-ledger mode)
//!
//! # Supported Streamable Aggregates
//!
//! - COUNT, COUNT(*) - just increment counter
//! - COUNT(DISTINCT) - maintain HashSet (still O(distinct values) per group)
//! - SUM, AVG - track sum and count
//! - MIN, MAX - track current min/max
//!
//! # Non-Streamable Aggregates (fallback to collect)
//!
//! - GROUP_CONCAT - needs all values for concatenation
//! - MEDIAN - needs all values for sorting
//! - VARIANCE, STDDEV - could be done with Welford's algorithm but not yet
//! - SAMPLE - could pick first, but semantic might expect random
//!
//! When any non-streamable aggregate is present, we fall back to the standard
//! GROUP BY behavior for that column only.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::AggregateFn;
// Note: JoinKey and Materializer would be used for multi-ledger/dataset mode
// but for now we use GroupKeyOwned for single-ledger simplicity
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::TermView;
use fluree_db_core::clock::Instant;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use rustc_hash::{FxBuildHasher, FxHashSet};
use smallvec::SmallVec;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::Instrument;

/// Specification for a streaming aggregate.
///
/// `function` carries every variant-correlated input (the read variable,
/// the `distinct` flag, the GROUP_CONCAT separator); only the columnar
/// projection (`input_col`) and the output binding (`output_var`) are
/// extracted here because they're properties of the operator wiring, not
/// of the aggregate itself.
#[derive(Debug, Clone)]
pub struct StreamingAggSpec {
    /// The aggregate function (carries its input variable + distinct flag).
    pub function: AggregateFn,
    /// Input column index in the upstream batch (None for COUNT(*)).
    pub input_col: Option<usize>,
    /// Output variable ID.
    pub output_var: VarId,
}

/// Per-group streaming aggregate state
#[derive(Debug)]
enum AggState {
    /// COUNT/COUNT(*) - just a counter
    Count { n: u64 },
    /// COUNT(DISTINCT) - HashSet of seen values
    CountDistinct { seen: FxHashSet<GroupKeyOwned> },
    /// COUNT(DISTINCT *) - HashSet of seen whole solutions
    CountDistinctAll { seen: FxHashSet<Vec<GroupKeyOwned>> },
    /// SUM - exact-when-possible numeric accumulator with type promotion
    /// (xsd:integer → xsd:decimal → xsd:double). `poisoned` records a bound
    /// non-numeric member, which makes the whole aggregate a type error.
    Sum {
        acc: crate::aggregate::NumericAcc,
        poisoned: bool,
    },
    /// AVG - same accumulator as SUM, divided by count at finalize.
    Avg {
        acc: crate::aggregate::NumericAcc,
        poisoned: bool,
    },
    /// MIN - current minimum (stores materialized binding for correct comparison)
    Min { min: Option<Binding> },
    /// MAX - current maximum (stores materialized binding for correct comparison)
    Max { max: Option<Binding> },
    /// SAMPLE - arbitrary value (we choose first observed value)
    Sample { sample: Option<Binding> },
    /// Fallback: collect all values (for GROUP_CONCAT, MEDIAN, etc.)
    Collect { values: Vec<Binding> },
}

/// Materialize an encoded binding for MIN/MAX comparison.
///
/// EncodedSid/EncodedPid raw IDs don't have semantic ordering (s_id=100 for "zebra"
/// would incorrectly compare > s_id=50 for "apple"). We must decode to get correct
/// term ordering via namespace/name comparison.
fn compare_for_minmax(
    a: &Binding,
    b: &Binding,
    gv: Option<&TermView>,
) -> std::result::Result<std::cmp::Ordering, Undecodable> {
    let Some(gv) = gv else {
        if a.is_encoded() || b.is_encoded() {
            return Err(undecodable(if a.is_encoded() { a } else { b }, "no view"));
        }
        return Ok(crate::sort::compare_bindings(a, b));
    };
    let store = gv.store();

    // Fast path 1: subject IDs (IRIs) — compare lexicographically without allocation.
    if let (Binding::EncodedSid { s_id: a_id, .. }, Binding::EncodedSid { s_id: b_id, .. }) = (a, b)
    {
        if let Ok(ord) = store.compare_subject_iri_lex(*a_id, *b_id) {
            return Ok(ord);
        }
        // Fall back to materialize+compare if dictionary lookup fails unexpectedly.
    }

    // Fast path 2: string-like encoded literals with identical type identity —
    // compare their string dictionary values without allocating.
    if let (
        Binding::EncodedLit {
            o_kind: a_kind,
            o_key: a_key,
            dt_id: a_dt,
            lang_id: a_lang,
            ..
        },
        Binding::EncodedLit {
            o_kind: b_kind,
            o_key: b_key,
            dt_id: b_dt,
            lang_id: b_lang,
            ..
        },
    ) = (a, b)
    {
        let string_kinds = [
            fluree_db_core::ObjKind::LEX_ID.as_u8(),
            fluree_db_core::ObjKind::JSON_ID.as_u8(),
        ];
        if *a_kind == *b_kind && string_kinds.contains(a_kind) && a_dt == b_dt && a_lang == b_lang {
            if let (Ok(ak), Ok(bk)) = (u32::try_from(*a_key), u32::try_from(*b_key)) {
                if let Ok(ord) = store.compare_string_lex(ak, bk) {
                    return Ok(ord);
                }
            }
        }
    }

    // General path: materialize then use SPARQL ordering comparator.
    // TermView handles novelty watermark routing internally.
    let am = materialize_encoded(a, Some(gv))?;
    let bm = materialize_encoded(b, Some(gv))?;
    Ok(crate::sort::compare_bindings(&am, &bm))
}

/// An encoded binding that cannot be decoded: an id the view's dictionaries
/// do not hold, or no view at all. An encoded binding exists only where a
/// store produced it, so either is a broken invariant, refused rather than
/// passed on encoded to a reader that would read it through some other view
/// or key it by an id.
///
/// A pointer, so the per-row key and decode paths that can return it stay the
/// size of their success value; it becomes a [`QueryError`] where an operator
/// returns it.
///
/// A failed read (`io::Error`) is kept with it, so a read that ran out of
/// fuel or needs a fetch first stays that typed error
/// ([`QueryError::from_io`]) rather than reading as a lookup failure.
// One thin pointer on purpose: `String` (24 B) or `Box<str>` (16 B) would
// not fit beside the key in `Result<GroupKeyOwned, Undecodable>`.
#[derive(Debug)]
pub(crate) struct Undecodable(Box<UndecodableCause>);

#[derive(Debug)]
struct UndecodableCause {
    message: String,
    read: Option<std::io::Error>,
}

impl From<Undecodable> for QueryError {
    #[cold]
    fn from(e: Undecodable) -> Self {
        let UndecodableCause { message, read } = *e.0;
        read.and_then(|err| QueryError::typed_io(&err))
            .unwrap_or_else(|| QueryError::dictionary_lookup(message))
    }
}

/// `collect` into a `Vec` for an iterator of `Result`s, as a plain loop.
///
/// `collect::<Result<Vec<_>, _>>()` goes through an adapter
/// (`GenericShunt`) that the compiler leaves out of line, a call per item on
/// the per-row key paths that build keys this way.
pub(crate) trait TryCollectVec<T, E>:
    Iterator<Item = std::result::Result<T, E>> + Sized
{
    #[inline]
    fn try_collect_vec(self) -> std::result::Result<Vec<T>, E> {
        let mut out = Vec::with_capacity(self.size_hint().0);
        for item in self {
            out.push(item?);
        }
        Ok(out)
    }
}

impl<T, E, I: Iterator<Item = std::result::Result<T, E>>> TryCollectVec<T, E> for I {}

#[cold]
pub(crate) fn undecodable(binding: &Binding, why: impl std::fmt::Display) -> Undecodable {
    Undecodable(Box::new(UndecodableCause {
        message: format!("cannot decode {binding:?}: {why}"),
        read: None,
    }))
}

/// [`undecodable`] for a read that failed, keeping its error.
#[cold]
pub(crate) fn unreadable(binding: &Binding, err: std::io::Error) -> Undecodable {
    Undecodable(Box::new(UndecodableCause {
        message: format!("cannot decode {binding:?}: {err}"),
        read: Some(err),
    }))
}

/// `binding` decoded through `gv`: an encoded literal, subject or predicate
/// to its term, anything else unchanged. Fails closed: an encoded binding
/// with no view, or one the view cannot decode, is an error.
pub(crate) fn materialize_encoded(
    binding: &Binding,
    gv: Option<&TermView>,
) -> std::result::Result<Binding, Undecodable> {
    let Some(gv) = gv else {
        if binding.is_encoded() {
            return Err(undecodable(binding, "no view to decode it through"));
        }
        return Ok(binding.clone());
    };
    let store = gv.store();

    Ok(match binding {
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            i_val,
            t,
            arena_g_id,
        } => {
            // TermView handles novelty watermark routing internally.
            match gv.decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id, *arena_g_id) {
                Ok(fluree_db_core::FlakeValue::Ref(sid)) => Binding::sid(sid),
                Ok(val) => {
                    let dt_sid = store
                        .resolve_dt_id_sid_for_value(*dt_id, &val)
                        .unwrap_or_else(|| Sid::new(0, ""));
                    let meta = store.decode_meta(*lang_id, *i_val);
                    let dtc = meta
                        .as_ref()
                        .and_then(|m| m.lang.as_ref())
                        .map(|s| DatatypeConstraint::LangTag(Arc::<str>::from(s.as_str())))
                        .unwrap_or_else(|| DatatypeConstraint::Explicit(dt_sid));
                    Binding::Lit {
                        val,
                        dtc,
                        t: Some(*t),
                        op: None,
                        p_id: Some(*p_id),
                    }
                }
                Err(e) => return Err(unreadable(binding, e)),
            }
        }
        Binding::EncodedSid { s_id, .. } => match gv.resolve_subject_sid(*s_id) {
            Ok(sid) => Binding::sid(sid),
            Err(e) => return Err(unreadable(binding, e)),
        },
        Binding::EncodedPid { p_id } => match store.resolve_predicate_iri(*p_id) {
            Some(iri) => Binding::sid(store.encode_iri(iri)),
            None => return Err(undecodable(binding, "unknown predicate id")),
        },
        _ => binding.clone(),
    })
}

impl AggState {
    fn new(func: &AggregateFn) -> Self {
        match func {
            AggregateFn::Count(_) | AggregateFn::CountAll => AggState::Count { n: 0 },
            AggregateFn::CountDistinct(_) => AggState::CountDistinct {
                seen: FxHashSet::default(),
            },
            AggregateFn::CountDistinctAll(_) => AggState::CountDistinctAll {
                seen: FxHashSet::default(),
            },
            AggregateFn::Sum { .. } => AggState::Sum {
                acc: crate::aggregate::NumericAcc::new(),
                poisoned: false,
            },
            AggregateFn::Avg { .. } => AggState::Avg {
                acc: crate::aggregate::NumericAcc::new(),
                poisoned: false,
            },
            AggregateFn::Min(_) => AggState::Min { min: None },
            AggregateFn::Max(_) => AggState::Max { max: None },
            // Non-streamable: collect all values
            AggregateFn::Median { .. }
            | AggregateFn::Variance { .. }
            | AggregateFn::Stddev { .. }
            | AggregateFn::GroupConcat { .. }
            | AggregateFn::Collect(..) => AggState::Collect { values: Vec::new() },
            // SAMPLE is explicitly arbitrary in SPARQL; we pick the first observed value.
            AggregateFn::Sample(_) => AggState::Sample { sample: None },
        }
    }

    /// Update state with a new binding value
    ///
    /// # Arguments
    ///
    /// * `binding` - The binding value to incorporate
    /// * `gv` - Optional graph view for materializing encoded bindings (needed for MIN/MAX).
    ///   When the graph view is novelty-aware, watermark routing is handled internally.
    fn update(
        &mut self,
        binding: &Binding,
        dicts: Option<crate::object_binding::TermDicts<'_>>,
        gv: Option<&TermView>,
    ) -> Result<()> {
        match self {
            AggState::Count { n } => {
                // COUNT: count non-Unbound values (COUNT(*) counts all via CountAll variant)
                if !matches!(binding, Binding::Unbound | Binding::Poisoned) {
                    *n += 1;
                }
            }
            // Whole-row state: fed by `update_distinct_row`, never by a single
            // column (COUNT(DISTINCT *) has no input variable).
            AggState::CountDistinctAll { .. } => {}
            AggState::CountDistinct { seen } => {
                if !matches!(binding, Binding::Unbound | Binding::Poisoned) {
                    // Convert binding to owned group key for HashSet,
                    // normalizing decoded bindings so mixed-representation
                    // streams count as one value.
                    let key = binding_to_group_key_normalized(binding, dicts, gv)?;
                    seen.insert(key);
                }
            }
            AggState::Sum { acc, poisoned } => {
                // Numeric hot path first (unchanged); a bound non-numeric member
                // poisons the aggregate to a type error (agg-err-01), while
                // Unbound/Poisoned contribute nothing and do not poison.
                if let Some(num) = extract_numeric_with_gv(binding, gv) {
                    acc.add(num);
                } else if !matches!(binding, Binding::Unbound | Binding::Poisoned) {
                    *poisoned = true;
                }
            }
            AggState::Avg { acc, poisoned } => {
                if let Some(num) = extract_numeric_with_gv(binding, gv) {
                    acc.add(num);
                } else if !matches!(binding, Binding::Unbound | Binding::Poisoned) {
                    *poisoned = true;
                }
            }
            AggState::Min { min } => {
                if !matches!(
                    binding,
                    Binding::Unbound | Binding::Poisoned | Binding::Grouped(_)
                ) {
                    match min {
                        None => *min = Some(binding.clone()),
                        Some(current) => {
                            if compare_for_minmax(binding, current, gv)? == std::cmp::Ordering::Less
                            {
                                *min = Some(binding.clone());
                            }
                        }
                    }
                }
            }
            AggState::Max { max } => {
                if !matches!(
                    binding,
                    Binding::Unbound | Binding::Poisoned | Binding::Grouped(_)
                ) {
                    match max {
                        None => *max = Some(binding.clone()),
                        Some(current) => {
                            if compare_for_minmax(binding, current, gv)?
                                == std::cmp::Ordering::Greater
                            {
                                *max = Some(binding.clone());
                            }
                        }
                    }
                }
            }
            AggState::Sample { sample } => {
                if sample.is_none()
                    && !matches!(
                        binding,
                        Binding::Unbound | Binding::Poisoned | Binding::Grouped(_)
                    )
                {
                    *sample = Some(binding.clone());
                }
            }
            AggState::Collect { values } => {
                // Median/Variance/Stddev/GroupConcat fold over decoded values;
                // encoded bindings would be silently dropped by the value
                // matchers in `aggregate.rs` (e.g. GROUP_CONCAT returned
                // Unbound for every indexed-ledger string).
                values.push(materialize_encoded(binding, gv)?);
            }
        }
        Ok(())
    }

    /// Update for COUNT(*) - increment regardless of binding value
    fn update_count_all(&mut self) {
        if let AggState::Count { n } = self {
            *n += 1;
        }
    }

    /// Update with a whole composed solution (`COUNT(DISTINCT *)`).
    fn update_distinct_row(&mut self, row: &[GroupKeyOwned]) {
        if let AggState::CountDistinctAll { seen } = self {
            if !seen.contains(row) {
                seen.insert(row.to_vec());
            }
        }
    }

    /// Finalize the aggregate state into a result binding
    fn finalize(self, func: &AggregateFn) -> Binding {
        match self {
            AggState::Count { n } => Binding::lit(FlakeValue::Long(n as i64), Sid::xsd_integer()),
            AggState::CountDistinct { seen } => {
                Binding::lit(FlakeValue::Long(seen.len() as i64), Sid::xsd_integer())
            }
            AggState::CountDistinctAll { seen } => {
                Binding::lit(FlakeValue::Long(seen.len() as i64), Sid::xsd_integer())
            }
            AggState::Sum { acc, poisoned } => {
                if poisoned {
                    Binding::Unbound
                } else {
                    acc.finalize_sum()
                }
            }
            AggState::Avg { acc, poisoned } => {
                if poisoned {
                    Binding::Unbound
                } else {
                    acc.finalize_avg()
                }
            }
            AggState::Min { min } => min.unwrap_or(Binding::Unbound),
            AggState::Max { max } => max.unwrap_or(Binding::Unbound),
            AggState::Sample { sample } => sample.unwrap_or(Binding::Unbound),
            AggState::Collect { values } => {
                // Non-streamable aggregates collect all values, then delegate.
                // DISTINCT SUM/AVG never reach this path — the planner routes
                // them to the traditional AggregateOperator instead (via
                // `all_streamable()` returning false). If a future non-streamable
                // aggregate needs DISTINCT, pass the spec's distinct flag here.
                func.apply(&Binding::Grouped(values))
            }
        }
    }
}

/// Owned group key for HashMap storage.
/// Uses the same semantics as JoinKey but owns its data.
/// Also used by SemijoinOperator for EXISTS hash probing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum GroupKeyOwned {
    /// Single-ledger: raw s_id
    Sid(u64),
    /// Single-ledger: raw p_id
    Pid(u32),
    /// Encoded literal key
    Lit {
        o_kind: u8,
        o_key: u64,
        /// An arena handle's predicate and graph (NUM_BIG, VECTOR): the handle
        /// names a value only with both. `None` for every other kind, whose
        /// identity they must not affect.
        arena: Option<(u32, fluree_db_core::GraphId)>,
        dt_id: u16,
        lang_id: u16,
    },
    /// Materialized Sid (namespace_code, name)
    MaterializedSid(u16, Arc<str>),
    /// Materialized literal value
    MaterializedLit(MaterializedLitKey),
    /// Materialized vector literal: its components, compared bitwise as
    /// `FlakeValue`'s vector equality does. Shares the decoded value's
    /// allocation rather than rendering it.
    MaterializedVector(VectorKey, DatatypeConstraint),
    /// Ordered sequence key — a path's node sequence or a list value, so
    /// `WITH path, collect(...)` / GROUP BY a list groups per distinct sequence.
    Seq(Vec<GroupKeyOwned>),
    /// Unbound/Poisoned
    Absent,
}

/// A vector as a hash key, by value: two keys are equal iff the vectors are
/// (bitwise-equal components, as `FlakeValue`'s vector equality has it), in
/// whichever form each arrives. A stored vector keys by its arena's identity
/// table when that is built (hash and canonical handle, no shard held), and
/// otherwise as a zero-copy slice of its arena shard, so keying never decodes
/// or copies it; a decoded one (novelty, `VALUES`, `BIND`) keys by its
/// components. All hash with the one canonical function
/// ([`fluree_db_core::value::vector_value_hash`]), carried in the key so a
/// table that grows doesn't read the components again.
#[derive(Debug, Clone)]
pub(crate) struct VectorKey {
    hash: u64,
    repr: VectorRepr,
}

#[derive(Debug, Clone)]
enum VectorRepr {
    Arena(fluree_db_binary_index::ArenaVector),
    Decoded(Arc<[f64]>),
}

impl VectorKey {
    pub(crate) fn decoded(value: &Arc<[f64]>) -> Self {
        Self {
            hash: fluree_db_core::value::vector_value_hash(value),
            repr: VectorRepr::Decoded(Arc::clone(value)),
        }
    }

    /// The key of an encoded vector, read from the arena of the graph it
    /// carries without decoding. `Ok(None)` for any other binding, or a
    /// handle the arena doesn't hold (which the caller decodes instead); an
    /// error if the arena read fails or the literal carries no graph.
    pub(crate) fn try_of_encoded(
        binding: &Binding,
        gv: &TermView,
    ) -> std::io::Result<Option<Self>> {
        let Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            arena_g_id,
            ..
        } = binding
        else {
            return Ok(None);
        };
        if *o_kind != fluree_db_core::ObjKind::VECTOR_ID.as_u8() {
            return Ok(None);
        }
        Ok(gv
            .arena_vector(*arena_g_id, *p_id, *o_key as u32)?
            .map(|arena| Self {
                hash: arena.hash,
                repr: VectorRepr::Arena(arena),
            }))
    }

    /// The key of any vector binding: encoded through its arena (decoding it
    /// only if the arena doesn't hold the handle), decoded by its
    /// components. `Ok(None)` for a binding that isn't a vector. Fails
    /// closed, as [`materialize_encoded`] does: an encoded vector with no
    /// view, or whose arena read fails, is an error.
    pub(crate) fn of_binding(
        binding: &Binding,
        gv: Option<&TermView>,
    ) -> std::result::Result<Option<Self>, Undecodable> {
        match binding {
            Binding::Lit {
                val: FlakeValue::Vector(v),
                ..
            } => Ok(Some(Self::decoded(v))),
            Binding::EncodedLit { o_kind, .. }
                if *o_kind == fluree_db_core::ObjKind::VECTOR_ID.as_u8() =>
            {
                if let Some(gv) = gv {
                    if let Some(key) =
                        Self::try_of_encoded(binding, gv).map_err(|e| unreadable(binding, e))?
                    {
                        return Ok(Some(key));
                    }
                }
                Ok(match materialize_encoded(binding, gv)? {
                    Binding::Lit {
                        val: FlakeValue::Vector(v),
                        ..
                    } => Some(Self::decoded(&v)),
                    _ => None,
                })
            }
            _ => Ok(None),
        }
    }
}

/// Whether `binding` is a vector, encoded or decoded.
pub(crate) fn is_vector_binding(binding: &Binding) -> bool {
    match binding {
        Binding::Lit { val, .. } => matches!(val, FlakeValue::Vector(_)),
        Binding::EncodedLit { o_kind, .. } => *o_kind == fluree_db_core::ObjKind::VECTOR_ID.as_u8(),
        _ => false,
    }
}

impl VectorKey {
    /// Whether the two keys hold equal vectors. Keys of one arena whose
    /// identity table is built compare by canonical handle and read nothing;
    /// otherwise, on a hash match, components are compared, read from the
    /// arena for a key that holds none.
    pub(crate) fn same_value(&self, other: &Self) -> std::io::Result<bool> {
        if self.hash != other.hash {
            return Ok(false);
        }
        match (&self.repr, &other.repr) {
            (VectorRepr::Arena(a), VectorRepr::Arena(b)) => a.same_value(b),
            (VectorRepr::Arena(a), VectorRepr::Decoded(d))
            | (VectorRepr::Decoded(d), VectorRepr::Arena(a)) => {
                a.same_value_as_decoded(self.hash, d)
            }
            (VectorRepr::Decoded(x), VectorRepr::Decoded(y)) => Ok(Arc::ptr_eq(x, y)
                || x.len() == y.len()
                    && x.iter()
                        .zip(y.iter())
                        .all(|(p, q)| p.to_bits() == q.to_bits())),
        }
    }
}

/// Equality for a hash table, which can't return an error. Only a
/// comparison that needs components a key doesn't hold reads, on a hash
/// match, and only a key the identity table resolved holds none. A failed
/// read is recorded in that key's slot, the query's, and the operator that
/// owns the table fails the query on its next check
/// ([`crate::context::ExecutionContext::checkpoint`]): the pair reads as
/// unequal only until then, and no row built on it is returned.
impl PartialEq for VectorKey {
    fn eq(&self, other: &Self) -> bool {
        self.same_value(other).unwrap_or_else(|err| {
            let slot = [self, other].into_iter().find_map(|key| match &key.repr {
                VectorRepr::Arena(arena) => arena.read_errors(),
                VectorRepr::Decoded(_) => None,
            });
            debug_assert!(slot.is_some(), "a key that holds no components read none");
            if let Some(slot) = slot {
                slot.record(err);
            }
            false
        })
    }
}

impl Eq for VectorKey {}

impl Hash for VectorKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

/// The datatype constraint a decoded vector of `dt_id` carries, as
/// [`materialize_encoded`] gives it, so an encoded vector's key matches a
/// decoded one's.
fn vector_dtc(gv: &TermView, dt_id: u16) -> DatatypeConstraint {
    DatatypeConstraint::Explicit(
        gv.store()
            .dt_sids()
            .get(dt_id as usize)
            .cloned()
            .unwrap_or_else(|| Sid::new(0, "")),
    )
}

/// Hashable key for materialized literals.
/// Includes datatype and language for correct GROUP BY / COUNT(DISTINCT) semantics.
/// Without these, "1"^^xsd:string and "1"^^xsd:integer would incorrectly merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedLitKey {
    discriminant: u8,
    // For strings/json: the string value
    string_val: Option<Arc<str>>,
    // For numbers: bits representation
    number_bits: Option<u64>,
    // For booleans
    bool_val: Option<bool>,
    // Datatype constraint - critical for correct comparison
    dtc: DatatypeConstraint,
}

impl Hash for MaterializedLitKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.discriminant.hash(state);
        self.string_val.hash(state);
        self.number_bits.hash(state);
        self.bool_val.hash(state);
        // Critical: include datatype constraint for correct GROUP BY / COUNT(DISTINCT) semantics
        // Without these, "1"^^xsd:string and "1"^^xsd:integer would incorrectly hash the same
        self.dtc.hash(state);
    }
}

/// [`binding_to_group_key_owned`] with representation normalization, so
/// mixed-representation streams (encoded scan output meeting decoded
/// VALUES/UNION/BIND bindings) key identically. Pass the store whenever the
/// stream may contain late-materialized bindings; pass the view so
/// arena-backed literals (NUM_BIG, VECTOR_ID) key by value: handles are
/// per-(graph, predicate), and the vector arena does not deduplicate, so a
/// handle is not canonical across graphs, predicates, representations or
/// facts.
pub(crate) fn binding_to_group_key_normalized(
    binding: &Binding,
    dicts: Option<crate::object_binding::TermDicts<'_>>,
    gv: Option<&TermView>,
) -> std::result::Result<GroupKeyOwned, Undecodable> {
    // An encoded subject id is its own canonical key.
    if let Binding::EncodedSid { s_id, .. } = binding {
        return Ok(GroupKeyOwned::Sid(*s_id));
    }
    if let Some(canonical) =
        dicts.and_then(|d| crate::object_binding::encoded_iri_canonical(binding, d))
    {
        return Ok(binding_to_group_key_owned(
            canonical.as_ref().unwrap_or(binding),
        ));
    }
    // An arena handle keys by its value, and fails closed when it cannot be
    // read (see `normalize_for_key_cow`). A stored vector keys by its arena
    // entry, undecoded, through the graph the literal carries.
    if crate::object_binding::is_arena_encoded(binding) {
        if let (Some(gv), Binding::EncodedLit { dt_id, .. }) = (gv, binding) {
            if let Some(key) =
                VectorKey::try_of_encoded(binding, gv).map_err(|e| unreadable(binding, e))?
            {
                return Ok(GroupKeyOwned::MaterializedVector(
                    key,
                    vector_dtc(gv, *dt_id),
                ));
            }
        }
        return Ok(binding_to_group_key_owned(&materialize_encoded(
            binding, gv,
        )?));
    }
    if let Some(dicts) = dicts {
        if let Some(normalized) = crate::object_binding::encoded_equivalent(binding, dicts) {
            return Ok(binding_to_group_key_owned(&normalized));
        }
    }
    Ok(binding_to_group_key_owned(binding))
}

/// Convert a binding to an owned group key.
/// Also used by SemijoinOperator for EXISTS hash probing.
pub(crate) fn binding_to_group_key_owned(binding: &Binding) -> GroupKeyOwned {
    match binding {
        Binding::EncodedSid { s_id, .. } => GroupKeyOwned::Sid(*s_id),
        Binding::EncodedPid { p_id } => GroupKeyOwned::Pid(*p_id),
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            arena_g_id,
            ..
        } => GroupKeyOwned::Lit {
            o_kind: *o_kind,
            o_key: *o_key,
            arena: crate::binding::is_arena_kind(*o_kind).then_some((*p_id, *arena_g_id)),
            dt_id: *dt_id,
            lang_id: *lang_id,
        },
        Binding::Sid { sid, .. } => {
            GroupKeyOwned::MaterializedSid(sid.namespace_code, sid.name.clone())
        }
        Binding::Lit {
            val: FlakeValue::Vector(v),
            dtc,
            ..
        } => GroupKeyOwned::MaterializedVector(VectorKey::decoded(v), dtc.clone()),
        Binding::Lit { val, dtc, .. } => {
            GroupKeyOwned::MaterializedLit(flake_value_to_key(val, dtc))
        }
        Binding::Unbound | Binding::Poisoned => GroupKeyOwned::Absent,
        Binding::IriMatch { iri, .. } => {
            // Use full IRI for cross-ledger correctness
            GroupKeyOwned::MaterializedSid(0, iri.clone())
        }
        Binding::Iri(iri) => {
            // Plain IRI string
            GroupKeyOwned::MaterializedSid(0, iri.clone())
        }
        // Unreachable: a per-group list never leaves its grouping stage (the
        // plan-time `Grouping::first_ungrouped_read` check). This function
        // cannot fail, so a list here would merge every group into one.
        Binding::Grouped(_) => {
            debug_assert!(false, "Grouped binding used as a group key");
            GroupKeyOwned::Absent
        }
        // A path groups per distinct (nodes, per-hop edges) identity — needed
        // for `WITH path, collect(...)` over allShortestPaths (IC14). Edges are
        // included to match PartialEq/Hash: two paths over the same node
        // sequence via different (parallel) edges are distinct paths and must
        // not merge under GROUP BY when DISTINCT would keep them apart. (On the
        // RDF/JSON-LD surface `edges` is always empty, so this is a no-op there.)
        Binding::Path { nodes, edges } => {
            let sid_key =
                |sid: &Sid| GroupKeyOwned::MaterializedSid(sid.namespace_code, sid.name.clone());
            let node_keys = GroupKeyOwned::Seq(nodes.iter().map(sid_key).collect());
            let edge_keys = GroupKeyOwned::Seq(
                edges
                    .iter()
                    .map(|(s, p, e)| GroupKeyOwned::Seq(vec![sid_key(s), sid_key(p), sid_key(e)]))
                    .collect(),
            );
            GroupKeyOwned::Seq(vec![node_keys, edge_keys])
        }
        // A relationship groups per (start, predicate, end) identity.
        // Relationship identity (matches PartialEq/Hash): the reifier when
        // present, else (start, predicate, end). The single-vs-triple length
        // keeps the two cases distinct.
        Binding::Rel(rel) => {
            let sids: Vec<&Sid> = match &rel.reifier {
                Some(r) => vec![r],
                None => vec![&rel.start, &rel.predicate, &rel.end],
            };
            GroupKeyOwned::Seq(
                sids.into_iter()
                    .map(|sid| GroupKeyOwned::MaterializedSid(sid.namespace_code, sid.name.clone()))
                    .collect(),
            )
        }
        Binding::List(items) => {
            GroupKeyOwned::Seq(items.iter().map(binding_to_group_key_owned).collect())
        }
        // A map groups per distinct {key→value} set: each entry contributes its
        // key string (sentinel namespace) and its value key. Sorted by key so the
        // group key is order-insensitive, matching map equality/hash.
        Binding::Map(entries) => {
            let mut sorted: Vec<_> = entries.iter().collect();
            sorted.sort_by(|x, y| x.0.cmp(&y.0));
            GroupKeyOwned::Seq(
                sorted
                    .into_iter()
                    .map(|(k, v)| {
                        GroupKeyOwned::Seq(vec![
                            GroupKeyOwned::MaterializedSid(u16::MAX, k.clone()),
                            binding_to_group_key_owned(v),
                        ])
                    })
                    .collect(),
            )
        }
    }
}

/// The pipeline's canonical term identity for a materialized literal.
///
/// Shared with the exact NumBig distinct-object counter in [`crate::fast_count`],
/// which must key arena values exactly as the general pipeline does or the two
/// would disagree about how many distinct objects a graph holds.
pub(crate) fn flake_value_to_key(val: &FlakeValue, dtc: &DatatypeConstraint) -> MaterializedLitKey {
    match val {
        FlakeValue::String(s) => MaterializedLitKey {
            discriminant: 1,
            string_val: Some(Arc::from(s.as_str())),
            number_bits: None,
            bool_val: None,
            dtc: dtc.clone(),
        },
        FlakeValue::Long(n) => MaterializedLitKey {
            discriminant: 2,
            string_val: None,
            number_bits: Some(*n as u64),
            bool_val: None,
            dtc: dtc.clone(),
        },
        // Every NaN is one value, and -0.0 is 0.0, as in the index's keys.
        FlakeValue::Double(d) => MaterializedLitKey {
            discriminant: 3,
            string_val: None,
            number_bits: Some(fluree_db_core::value_id::canonical_f64_bits(*d)),
            bool_val: None,
            dtc: dtc.clone(),
        },
        FlakeValue::Boolean(b) => MaterializedLitKey {
            discriminant: 4,
            string_val: None,
            number_bits: None,
            bool_val: Some(*b),
            dtc: dtc.clone(),
        },
        // Decimals key on the normalized lexical: 1.5 and 1.50 are one
        // xsd:decimal value and must land in one group. (The Debug catch-all
        // below is scale-sensitive and structurally fragile.)
        FlakeValue::Decimal(d) => MaterializedLitKey {
            discriminant: 5,
            string_val: Some(Arc::from(d.normalized().to_string())),
            number_bits: None,
            bool_val: None,
            dtc: dtc.clone(),
        },
        FlakeValue::BigInt(n) => MaterializedLitKey {
            discriminant: 6,
            string_val: Some(Arc::from(n.to_string())),
            number_bits: None,
            bool_val: None,
            dtc: dtc.clone(),
        },
        _ => MaterializedLitKey {
            discriminant: 0,
            string_val: Some(Arc::from(format!("{val:?}"))),
            number_bits: None,
            bool_val: None,
            dtc: dtc.clone(),
        },
    }
}

/// Composite group key (multiple columns).
/// Also used by SemijoinOperator for multi-var EXISTS hash probing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CompositeGroupKey(pub(crate) Vec<GroupKeyOwned>);

impl CompositeGroupKey {
    /// Key over `bindings`, normalized so mixed-representation rows key
    /// identically (see [`binding_to_group_key_normalized`]).
    pub(crate) fn normalized<'a>(
        bindings: impl IntoIterator<Item = &'a Binding>,
        norm: &Option<crate::object_binding::EqualityNorm>,
    ) -> Result<Self> {
        let (dicts, gv) = crate::object_binding::EqualityNorm::parts(norm);
        Ok(Self(
            bindings
                .into_iter()
                .map(|b| binding_to_group_key_normalized(b, dicts, gv))
                .try_collect_vec()?,
        ))
    }
}

/// Per-group state: the key bindings and aggregate states
struct GroupState {
    /// Original bindings for group key columns (for output)
    key_bindings: Vec<Binding>,
    /// Aggregate states (one per aggregate spec)
    agg_states: Vec<AggState>,
}

/// Routing stamp of the partitioned GROUP BY lane ([`GroupAggregateOperator`]
/// aggregating key runs instead of hashing), recorded at open when the plan
/// promised partitioned input. The lane stamps it again with a decline when
/// its key column holds a key whose encoding is not one per value and those
/// rows fold through the hashed groups (see [`folds_by_value`]).
pub(crate) const PARTITIONED_GROUP_BY_SITE: &str = "partitioned_group_by";

/// Streaming GROUP BY + Aggregate operator
///
/// Memory usage: O(groups) instead of O(rows)
pub struct GroupAggregateOperator {
    /// Child operator
    child: BoxedOperator,
    /// Output schema
    in_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Group key column indices
    group_key_indices: Vec<usize>,
    /// Aggregate specifications
    agg_specs: Vec<StreamingAggSpec>,
    /// Accumulated groups: composite_key -> group_state
    groups: hashbrown::HashMap<CompositeGroupKey, GroupState, FxBuildHasher>,
    /// If true, input is already partitioned by the GROUP BY key(s), so we can
    /// aggregate per-run without hashing each row into a map. Set at open:
    /// the plan's hint ([`Self::partition_hint`]), held only when the scope
    /// reads exactly one graph.
    partitioned: bool,
    /// The plan's promise that the input arrives grouped by the key. A
    /// default graph of several graphs drains its members one after another,
    /// one sorted run per member, so the promise holds only for one graph.
    partition_hint: bool,
    /// Accumulated groups in partitioned mode (in input order).
    partitioned_groups: Vec<GroupState>,
    /// Iterator for emitting results.
    emit_iter: Option<GroupEmitIter>,
    /// Graph view for materializing encoded bindings (used for MIN/MAX semantic ordering).
    /// When novelty-aware (via `ExecutionContext::one_graph_view()`), watermark routing
    /// for novelty-only subject/string IDs is handled internally.
    graph_view: Option<TermView>,
    /// Normalization for group keys and `COUNT(DISTINCT)`, so the forms one
    /// term takes key as one (see [`binding_to_group_key_normalized`]).
    norm: Option<crate::object_binding::EqualityNorm>,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
    /// Child columns each `COUNT(DISTINCT *)` spec composes its solution from,
    /// parallel to `agg_specs`; `None` for every other aggregate. Resolved once
    /// from the spec's user-visible variable list, so lowering-internal columns
    /// never split a solution.
    row_distinct_cols: Vec<Option<Vec<usize>>>,
    /// Whether any aggregate is `COUNT(DISTINCT *)`. Gates the per-row solution
    /// composition so ordinary grouping does no extra work.
    has_row_distinct: bool,
}

enum GroupEmitIter {
    Hash(hashbrown::hash_map::IntoIter<CompositeGroupKey, GroupState>),
    Vec(std::vec::IntoIter<GroupState>),
}

impl GroupAggregateOperator {
    /// Create a streaming GROUP BY + Aggregate operator
    ///
    /// # Arguments
    ///
    /// * `child` - Input operator
    /// * `group_vars` - Variables to group by
    /// * `agg_specs` - Aggregate specifications (input col, function, output var)
    /// * `graph_view` - Optional graph view for encoded binding materialization
    pub fn new(
        child: BoxedOperator,
        group_vars: Vec<VarId>,
        agg_specs: Vec<StreamingAggSpec>,
        graph_view: Option<TermView>,
        partitioned: bool,
    ) -> Self {
        let child_schema = child.schema().to_vec();

        // Compute indices for group key columns
        let group_key_indices: Vec<usize> = group_vars
            .iter()
            .map(|v| {
                child_schema
                    .iter()
                    .position(|sv| sv == v)
                    .unwrap_or_else(|| panic!("GROUP BY variable {v:?} not in schema"))
            })
            .collect();

        // Build output schema: group keys + aggregate outputs
        let mut output_vars: Vec<VarId> = group_vars.clone();
        for spec in &agg_specs {
            output_vars.push(spec.output_var);
        }

        let schema: Arc<[VarId]> = Arc::from(output_vars.into_boxed_slice());

        // Resolve each COUNT(DISTINCT *)'s visible variables to child columns.
        // Variables from other scopes, and SELECT aliases, are not in this
        // schema and drop out here.
        let row_distinct_cols: Vec<Option<Vec<usize>>> = agg_specs
            .iter()
            .map(|s| match &s.function {
                AggregateFn::CountDistinctAll(vars) => Some(
                    vars.iter()
                        .filter_map(|v| child_schema.iter().position(|sv| sv == v))
                        .collect(),
                ),
                _ => None,
            })
            .collect();
        let has_row_distinct = row_distinct_cols.iter().any(Option::is_some);

        Self {
            child,
            in_schema: schema,
            state: OperatorState::Created,
            group_key_indices,
            agg_specs,
            groups: hashbrown::HashMap::with_hasher(FxBuildHasher),
            partitioned,
            partition_hint: partitioned,
            partitioned_groups: Vec::new(),
            emit_iter: None,
            graph_view,
            norm: None,
            out_schema: None,
            row_distinct_cols,
            has_row_distinct,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Check if all aggregates are streamable (for planner optimization
    /// decisions); see [`AggregateFn::is_streamable`].
    pub fn all_streamable(specs: &[StreamingAggSpec]) -> bool {
        specs.iter().all(|spec| spec.function.is_streamable())
    }

    /// Write a row's composite group key into `key`, reusing its allocation.
    /// `dicts`/`gv` are this operator's normalization, read once per batch.
    fn fill_group_key(
        &self,
        batch: &Batch,
        row_idx: usize,
        key: &mut CompositeGroupKey,
        dicts: Option<crate::object_binding::TermDicts<'_>>,
        gv: Option<&TermView>,
    ) -> Result<()> {
        key.0.clear();
        for &col_idx in &self.group_key_indices {
            let binding = batch.get_by_col(row_idx, col_idx);
            key.0
                .push(binding_to_group_key_normalized(binding, dicts, gv)?);
        }
        Ok(())
    }

    /// Compose one solution into a hashable key, for `COUNT(DISTINCT *)`.
    ///
    /// Only `cols` participate — the user-visible columns resolved at
    /// construction — normalized the same way group keys are so a mixed
    /// encoded/decoded stream does not double-count.
    fn extract_row_key(
        &self,
        batch: &Batch,
        row_idx: usize,
        cols: &[usize],
        dicts: Option<crate::object_binding::TermDicts<'_>>,
        gv: Option<&TermView>,
    ) -> Result<Vec<GroupKeyOwned>> {
        Ok(cols
            .iter()
            .map(|&col_idx| {
                let binding = batch.get_by_col(row_idx, col_idx);
                binding_to_group_key_normalized(binding, dicts, gv)
            })
            .try_collect_vec()?)
    }

    /// Per-spec composed solutions for this row, empty when no aggregate needs
    /// them. Computed before the group state is borrowed.
    fn extract_row_keys(
        &self,
        batch: &Batch,
        row_idx: usize,
        dicts: Option<crate::object_binding::TermDicts<'_>>,
        gv: Option<&TermView>,
    ) -> Result<Vec<Vec<GroupKeyOwned>>> {
        if !self.has_row_distinct {
            return Ok(Vec::new());
        }
        self.row_distinct_cols
            .iter()
            .map(|cols| match cols {
                Some(cols) => self.extract_row_key(batch, row_idx, cols, dicts, gv),
                None => Ok(Vec::new()),
            })
            .collect()
    }

    /// Extract original bindings for group key columns (for output)
    fn extract_key_bindings(&self, batch: &Batch, row_idx: usize) -> Vec<Binding> {
        self.group_key_indices
            .iter()
            .map(|&col_idx| batch.get_by_col(row_idx, col_idx).clone())
            .collect()
    }
}

/// Why the partitioned lane folds a key by value instead of by run, if it
/// does: the key is a vector (`"vector-key"`), or a big number while
/// `legacy_numbig` holds (`"legacy-numbig-key"`). Both cover the encoded
/// (arena handle) and decoded forms.
#[inline]
fn folds_by_value(binding: &Binding, legacy_numbig: bool) -> Option<&'static str> {
    use fluree_db_core::ObjKind;
    let (vector, big_number) = match binding {
        Binding::EncodedLit { o_kind, .. } => (
            *o_kind == ObjKind::VECTOR_ID.as_u8(),
            *o_kind == ObjKind::NUM_BIG.as_u8(),
        ),
        Binding::Lit { val, .. } => (
            matches!(val, FlakeValue::Vector(_)),
            matches!(val, FlakeValue::Decimal(_) | FlakeValue::BigInt(_)),
        ),
        _ => return None,
    };
    if vector {
        Some("vector-key")
    } else if big_number && legacy_numbig {
        Some("legacy-numbig-key")
    } else {
        None
    }
}

/// Fold one row into the hashed groups under `group_key` (already filled from
/// the row): one hash per row, and the key is cloned out of the buffer only
/// when it opens a new group. Takes the operator's fields rather than `self`
/// so it runs while `dicts` borrows the operator's normalization.
#[allow(clippy::too_many_arguments)]
#[inline]
fn fold_row_hashed(
    groups: &mut hashbrown::HashMap<CompositeGroupKey, GroupState, FxBuildHasher>,
    group_key_indices: &[usize],
    agg_specs: &[StreamingAggSpec],
    gv: Option<&TermView>,
    dicts: Option<crate::object_binding::TermDicts<'_>>,
    batch: &Batch,
    row_idx: usize,
    group_key: &CompositeGroupKey,
    row_keys: &[Vec<GroupKeyOwned>],
) -> Result<()> {
    let (_, group_state) = groups
        .raw_entry_mut()
        .from_key(group_key)
        .or_insert_with(|| {
            let state = GroupState {
                key_bindings: group_key_indices
                    .iter()
                    .map(|&col_idx| batch.get_by_col(row_idx, col_idx).clone())
                    .collect(),
                agg_states: agg_specs
                    .iter()
                    .map(|spec| AggState::new(&spec.function))
                    .collect(),
            };
            (group_key.clone(), state)
        });

    // Update each aggregate with this row's values
    for (agg_idx, spec) in agg_specs.iter().enumerate() {
        match spec.input_col {
            Some(col_idx) => {
                let binding = batch.get_by_col(row_idx, col_idx);
                group_state.agg_states[agg_idx].update(binding, dicts, gv)?;
            }
            // COUNT(DISTINCT *) - count distinct solutions
            None if matches!(spec.function, AggregateFn::CountDistinctAll(_)) => {
                group_state.agg_states[agg_idx].update_distinct_row(&row_keys[agg_idx]);
            }
            // COUNT(*) - count all rows
            None => group_state.agg_states[agg_idx].update_count_all(),
        }
    }
    Ok(())
}

#[async_trait]
impl Operator for GroupAggregateOperator {
    /// Item 11 (F-AUD-7): DECLINE forwarding — a grouped aggregate must consume ALL
    /// input before any group is closed, so a finite budget cannot bound it (the
    /// last input row may open a new group or change an existing one). Explicit
    /// (was a silent trait-default no-op) so the swallow is observable.
    fn set_row_budget(&mut self, budget: usize) {
        tracing::debug!(
            budget,
            "GROUP-AGGREGATE row-budget swallowed (unsound to forward: groups need all input)"
        );
    }

    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.groups.clear();
        self.partitioned_groups.clear();
        self.emit_iter = None;
        if self.partition_hint {
            self.partitioned = ctx.reads_exactly_one_graph();
            crate::fast_path_outcome::stamp_fast_path(
                PARTITIONED_GROUP_BY_SITE,
                if self.partitioned {
                    crate::fast_path_outcome::FastPathOutcome::Proceed
                } else {
                    crate::fast_path_outcome::FastPathOutcome::Fallback(
                        crate::fast_path_outcome::FastPathFallback::GateDeclined,
                    )
                },
            );
        }
        // If the execution context has a binary store, use it to materialize
        // encoded bindings for correct MIN/MAX comparison semantics.
        if self.graph_view.is_none() {
            self.graph_view = ctx.term_view();
        }
        self.norm = crate::object_binding::equality_norm(ctx);
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // COUNT(*) pushdown: if the child supports drain_count and we're
        // doing a simple ungrouped COUNT(*), skip row-by-row accumulation.
        if self.emit_iter.is_none()
            && self.group_key_indices.is_empty()
            && self.agg_specs.len() == 1
            && matches!(self.agg_specs[0].function, AggregateFn::CountAll)
        {
            if let Some(count) = self.child.drain_count(ctx).await? {
                let count_i64 = i64::try_from(count).map_err(|_| {
                    QueryError::execution("COUNT(*) exceeds i64::MAX in drain_count")
                })?;
                let count_binding = Binding::lit(FlakeValue::Long(count_i64), Sid::xsd_integer());
                let out_var = self.agg_specs[0].output_var;
                let schema: Arc<[VarId]> = Arc::from(vec![out_var].into_boxed_slice());
                let batch = Batch::new(schema, vec![vec![count_binding]])?;
                self.state = OperatorState::Exhausted;
                return Ok(Some(batch));
            }
        }

        // A join can count matches before expanding them into output rows when
        // all group keys come from its driving side, and a union of graphs can
        // count one pattern's rows from its members' indexes. Every aggregate
        // must count rows: `COUNT(*)`, or `COUNT(?v)` of a variable the child
        // shows bound in every row. Other aggregates and DISTINCT retain
        // ordinary row consumption.
        if self.emit_iter.is_none()
            && !self.group_key_indices.is_empty()
            && !self.agg_specs.is_empty()
            && self
                .agg_specs
                .iter()
                .all(|spec| matches!(spec.function, AggregateFn::CountAll | AggregateFn::Count(_)))
        {
            let bound_vars: SmallVec<[VarId; 4]> = self
                .agg_specs
                .iter()
                .filter_map(|spec| match spec.function {
                    AggregateFn::Count(v) => Some(v),
                    _ => None,
                })
                .collect();
            let group_vars = &self.in_schema[..self.group_key_indices.len()];
            if let Some(groups) = self
                .child
                .drain_grouped_count(ctx, group_vars, &bound_vars)
                .await?
            {
                let states = groups
                    .into_iter()
                    .map(|group| {
                        i64::try_from(group.count).map_err(|_| {
                            QueryError::execution("grouped COUNT(*) exceeds i64::MAX")
                        })?;
                        Ok(GroupState {
                            key_bindings: group.keys,
                            agg_states: self
                                .agg_specs
                                .iter()
                                .map(|_| AggState::Count { n: group.count })
                                .collect(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.emit_iter = Some(GroupEmitIter::Vec(states.into_iter()));
            }
        }

        // If we haven't consumed all input yet, do so now (streaming aggregation)
        if self.emit_iter.is_none() {
            let span = tracing::debug_span!(
                "group_aggregate_streaming",
                group_key_cols = self.group_key_indices.len(),
                agg_count = self.agg_specs.len(),
                partitioned = self.partitioned,
                input_batches = tracing::field::Empty,
                input_rows = tracing::field::Empty,
                groups = tracing::field::Empty,
                // APPROXIMATE recorded memory (rows x cols x flat per-binding est), not
                // measured bytes — the `_est` suffix flags it so a 507 debugger doesn't
                // read it as exact. Declared here so the record below is not a silent
                // no-op (tracing drops records for undeclared fields).
                mem_used_bytes_est = tracing::field::Empty,
                drain_ms = tracing::field::Empty
            );
            async {
                let span = tracing::Span::current();
                let drain_start = Instant::now();
                let mut input_batches: u64 = 0;
                let mut input_rows: u64 = 0;

                if self.partitioned && self.group_key_indices.len() == 1 {
                    // Partitioned fast path: input is grouped by a single key column.
                    let key_col = self.group_key_indices[0];
                    let mut current_key: Option<GroupKeyOwned> = None;
                    let mut current_state: Option<GroupState> = None;
                    // Rows whose key's encoding is not one per value fold into
                    // the hashed groups (see below); this buffer keys them.
                    let mut value_key = CompositeGroupKey(Vec::with_capacity(1));
                    let mut keys_hashed = false;
                    // Decided once, so a decimal's encoded and decoded forms
                    // take the same path. The lane reads exactly one graph
                    // (`reads_exactly_one_graph` at open), whose arena it is.
                    let legacy_numbig = self.norm.is_some()
                        && ctx.one_graph_view().is_some_and(|gv| {
                            gv.store().graph_numbig_has_duplicate_handles(gv.g_id())
                        });

                    loop {
                        // Per-batch checkpoint (deadline + memory budget; see the
                        // general-path loop). `partitioned_groups` grows with distinct
                        // group cardinality, accounted per finalized group below.
                        ctx.checkpoint()?;
                        let batch = match self.child.next_batch(ctx).await? {
                            Some(b) => b,
                            None => break,
                        };
                        input_batches += 1;
                        if batch.is_empty() {
                            continue;
                        }

                        let (dicts, norm_gv) =
                            crate::object_binding::EqualityNorm::parts(&self.norm);
                        for row_idx in 0..batch.len() {
                            input_rows += 1;
                            let key_binding = batch.get_by_col(row_idx, key_col);
                            // This lane needs one encoded key per value, with
                            // equal values adjacent. A vector breaks that (its
                            // arena gives each stored fact its own handle), and
                            // so does a decimal on a graph whose legacy NumBig
                            // arena holds scale variants (`1.5`, `1.50`) under
                            // separate handles. Those rows fold into the hashed
                            // groups instead, keyed by value. No other term
                            // equals one of them, so the two sets of groups
                            // never overlap.
                            if let Some(reason) = folds_by_value(key_binding, legacy_numbig) {
                                if !keys_hashed {
                                    keys_hashed = true;
                                    tracing::debug!(
                                        reason,
                                        "partitioned GROUP BY: keys fold by value"
                                    );
                                    crate::fast_path_outcome::stamp_fast_path(
                                        PARTITIONED_GROUP_BY_SITE,
                                        crate::fast_path_outcome::FastPathOutcome::Fallback(
                                            crate::fast_path_outcome::FastPathFallback::GateDeclined,
                                        ),
                                    );
                                }
                                self.fill_group_key(
                                    &batch,
                                    row_idx,
                                    &mut value_key,
                                    dicts,
                                    norm_gv,
                                )?;
                                let row_keys = if self.has_row_distinct {
                                    self.extract_row_keys(&batch, row_idx, dicts, norm_gv)?
                                } else {
                                    Vec::new()
                                };
                                let groups_before = self.groups.len();
                                fold_row_hashed(
                                    &mut self.groups,
                                    &self.group_key_indices,
                                    &self.agg_specs,
                                    self.graph_view.as_ref(),
                                    dicts,
                                    &batch,
                                    row_idx,
                                    &value_key,
                                    &row_keys,
                                )?;
                                if self.groups.len() > groups_before {
                                    ctx.record_alloc(crate::context::GROUP_EST_BYTES);
                                }
                                continue;
                            }
                            let key = binding_to_group_key_owned(key_binding);

                            let same_group = current_key.as_ref().is_some_and(|k| k == &key);
                            if !same_group {
                                if let Some(state) = current_state.take() {
                                    self.partitioned_groups.push(state);
                                    ctx.record_alloc(crate::context::GROUP_EST_BYTES);
                                }
                                current_key = Some(key);
                                // First row of new group: capture original key bindings for output.
                                let key_bindings = self.extract_key_bindings(&batch, row_idx);
                                let agg_states = self
                                    .agg_specs
                                    .iter()
                                    .map(|spec| AggState::new(&spec.function))
                                    .collect();
                                current_state = Some(GroupState {
                                    key_bindings,
                                    agg_states,
                                });
                            }

                            // COUNT(DISTINCT *) reads a whole solution, so
                            // compose it before the group state is borrowed.
                            let row_keys = if self.has_row_distinct {
                                self.extract_row_keys(&batch, row_idx, dicts, norm_gv)?
                            } else {
                                Vec::new()
                            };

                            // Update aggregate states for current group.
                            let gv_ref = self.graph_view.as_ref();
                            let group_state = current_state
                                .as_mut()
                                .expect("partitioned aggregation must have current group state");
                            for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
                                match spec.input_col {
                                    Some(col_idx) => {
                                        let binding = batch.get_by_col(row_idx, col_idx);
                                        group_state.agg_states[agg_idx]
                                            .update(binding, dicts, gv_ref)?;
                                    }
                                    // COUNT(DISTINCT *) - count distinct solutions
                                    None if matches!(
                                        spec.function,
                                        AggregateFn::CountDistinctAll(_)
                                    ) =>
                                    {
                                        group_state.agg_states[agg_idx]
                                            .update_distinct_row(&row_keys[agg_idx]);
                                    }
                                    // COUNT(*) - count all rows
                                    None => group_state.agg_states[agg_idx].update_count_all(),
                                }
                            }
                        }
                    }

                    if let Some(state) = current_state.take() {
                        self.partitioned_groups.push(state);
                        ctx.record_alloc(crate::context::GROUP_EST_BYTES);
                    }
                    // Vector-keyed groups, already accounted as they opened.
                    self.partitioned_groups
                        .extend(std::mem::take(&mut self.groups).into_values());

                    span.record("input_batches", input_batches);
                    span.record("input_rows", input_rows);
                    span.record("groups", self.partitioned_groups.len() as u64);
                    span.record("mem_used_bytes_est", ctx.mem_used() as u64);
                    span.record(
                        "drain_ms",
                        (drain_start.elapsed().as_secs_f64() * 1000.0) as u64,
                    );

                    self.emit_iter = Some(GroupEmitIter::Vec(
                        std::mem::take(&mut self.partitioned_groups).into_iter(),
                    ));
                    return Ok::<_, crate::error::QueryError>(());
                }

                // General path: hash-based accumulation.
                let mut group_key =
                    CompositeGroupKey(Vec::with_capacity(self.group_key_indices.len()));
                loop {
                    // The hash-aggregate fold buffers `groups` proportional to distinct
                    // group cardinality and, over a fully-buffered join child, can run
                    // minutes without pulling a polling scan. Checkpoint per batch
                    // (batch granularity; the per-row fold below is bounded by one batch
                    // between polls): a deadline abort surfaces `Cancelled`, and a
                    // high-cardinality GROUP BY that grew `groups` past the budget on a
                    // prior batch surfaces a typed `MemoryBudgetExceeded` before OOM.
                    // (SUM/COUNT rollups have few groups so this never trips for them —
                    // their OOM is the join build, guarded in hash_join.rs.)
                    ctx.checkpoint()?;
                    let batch = match self.child.next_batch(ctx).await? {
                        Some(b) => b,
                        None => break,
                    };
                    input_batches += 1;

                    if batch.is_empty() {
                        continue;
                    }

                    let groups_before = self.groups.len();

                    // Process each row
                    let (dicts, norm_gv) = crate::object_binding::EqualityNorm::parts(&self.norm);
                    for row_idx in 0..batch.len() {
                        input_rows += 1;

                        // Probe with a reused key buffer: a row joining an
                        // existing group allocates nothing.
                        self.fill_group_key(&batch, row_idx, &mut group_key, dicts, norm_gv)?;

                        // COUNT(DISTINCT *) reads a whole solution, so compose
                        // it here too — before the group state is borrowed.
                        let row_keys = if self.has_row_distinct {
                            self.extract_row_keys(&batch, row_idx, dicts, norm_gv)?
                        } else {
                            Vec::new()
                        };

                        fold_row_hashed(
                            &mut self.groups,
                            &self.group_key_indices,
                            &self.agg_specs,
                            self.graph_view.as_ref(),
                            dicts,
                            &batch,
                            row_idx,
                            &group_key,
                            &row_keys,
                        )?;
                    }
                    // Account this batch's group growth into the query-scoped counter
                    // (an O(1) len delta); the next iteration's checkpoint enforces the
                    // budget against the running total, which also folds in any upstream
                    // hash-join build under the same query budget.
                    let grown = self.groups.len() - groups_before;
                    if grown > 0 {
                        ctx.record_alloc(grown * crate::context::GROUP_EST_BYTES);
                    }
                }

                // SPARQL semantics: with no GROUP BY keys, aggregates are computed over a single
                // implicit group. Even if the input has zero rows, we must emit exactly one row
                // of aggregate results (e.g., COUNT(*) = 0).
                if self.group_key_indices.is_empty() && input_rows == 0 {
                    let agg_specs_ref = &self.agg_specs;
                    self.groups
                        .entry(CompositeGroupKey(Vec::new()))
                        .or_insert_with(|| GroupState {
                            key_bindings: Vec::new(),
                            agg_states: agg_specs_ref
                                .iter()
                                .map(|spec| AggState::new(&spec.function))
                                .collect(),
                        });
                }

                span.record("input_batches", input_batches);
                span.record("input_rows", input_rows);
                span.record("groups", self.groups.len() as u64);
                // Growth-curve telemetry: the query's retained post-scan memory as
                // observed at the end of the fold (see `ExecutionContext::checkpoint`).
                span.record("mem_used_bytes_est", ctx.mem_used() as u64);
                span.record(
                    "drain_ms",
                    (drain_start.elapsed().as_secs_f64() * 1000.0) as u64,
                );

                // Prepare iterator for emission
                let groups = std::mem::take(&mut self.groups);
                self.emit_iter = Some(GroupEmitIter::Hash(groups.into_iter()));
                Ok::<_, crate::error::QueryError>(())
            }
            .instrument(span)
            .await?;
            // A key comparison that failed to read split one group in two:
            // the groups are not an answer.
            ctx.check_key_reads()?;
        }

        // Emit batches from accumulated groups
        let batch_size = ctx.batch_size;
        let num_cols = self.in_schema.len();
        let mut output_columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut rows_added = 0;

        if let Some(ref mut iter) = self.emit_iter {
            while rows_added < batch_size {
                let next_state = match iter {
                    GroupEmitIter::Hash(it) => it.next().map(|(_k, s)| s),
                    GroupEmitIter::Vec(it) => it.next(),
                };
                match next_state {
                    Some(group_state) => {
                        // Output group key columns
                        for (col_idx, key_binding) in
                            group_state.key_bindings.into_iter().enumerate()
                        {
                            output_columns[col_idx].push(key_binding);
                        }

                        // Output aggregate results
                        let key_col_count = self.group_key_indices.len();
                        for (agg_idx, agg_state) in group_state.agg_states.into_iter().enumerate() {
                            let result = agg_state.finalize(&self.agg_specs[agg_idx].function);
                            output_columns[key_col_count + agg_idx].push(result);
                        }

                        rows_added += 1;
                    }
                    None => {
                        self.state = OperatorState::Exhausted;
                        break;
                    }
                }
            }
        }

        if rows_added == 0 {
            return Ok(None);
        }

        let batch = Batch::new(self.in_schema.clone(), output_columns)?;
        Ok(trim_batch(&self.out_schema, batch))
    }

    fn close(&mut self) {
        self.child.close();
        self.groups.clear();
        self.partitioned_groups.clear();
        self.emit_iter = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // We don't know group count without running
        None
    }
}

/// Extract numeric value from binding
/// Extract a `NumericValue` from a binding for streaming SUM / AVG.
///
/// Handles inline numeric kinds (Long/Double/Boolean/Decimal/BigInt as
/// `Lit`, NUM_INT/NUM_F64 as `EncodedLit`). For `EncodedLit` with kind
/// `NUM_BIG` (arena-backed BigInt/BigDecimal) we materialize through the
/// graph view's dictionary when one is available, so SUM/AVG see the
/// full numeric class — without this, summing decimals stored via the
/// arena would silently drop the values.
fn extract_numeric_with_gv(
    binding: &Binding,
    gv: Option<&TermView>,
) -> Option<crate::aggregate::NumericValue> {
    use fluree_db_core::ObjKind;

    if let Binding::EncodedLit {
        o_kind,
        o_key,
        p_id,
        dt_id,
        lang_id,
        arena_g_id,
        ..
    } = binding
    {
        if *o_kind == ObjKind::NUM_BIG.as_u8() {
            let gv = gv?;
            let val = gv
                .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id, *arena_g_id)
                .ok()?;
            return crate::aggregate::flake_value_to_numeric(&val);
        }
    }
    crate::aggregate::binding_to_numeric(binding)
}

#[cfg(test)]
mod fail_closed_decode {
    use super::*;
    use fluree_db_binary_index::BinaryIndexStore;

    fn empty_terms() -> TermView {
        let dir = std::env::temp_dir().join(format!("fail-closed-{}", std::process::id()));
        TermView::new(Arc::new(BinaryIndexStore::empty(dir)), None)
    }

    fn arena_lit(o_kind: u8, arena_g_id: u16) -> Binding {
        Binding::EncodedLit {
            o_kind,
            o_key: 0,
            p_id: 1,
            dt_id: 0,
            lang_id: 0,
            i_val: i32::MIN,
            t: 0,
            arena_g_id,
        }
    }

    /// An encoded binding is refused, never passed on encoded, when there is
    /// no view to decode it or the view cannot; a decoded one passes.
    #[test]
    fn materialize_encoded_fails_closed() {
        let sid = Binding::EncodedSid {
            s_id: 7,
            t: None,
            op: None,
        };
        assert!(materialize_encoded(&sid, None).is_err(), "no view");
        assert!(
            materialize_encoded(&sid, Some(&empty_terms())).is_err(),
            "an id the view does not hold"
        );
        let pid = Binding::EncodedPid { p_id: 9 };
        assert!(materialize_encoded(&pid, Some(&empty_terms())).is_err());
        let lit = arena_lit(fluree_db_core::ObjKind::NUM_BIG.as_u8(), 2);
        assert!(materialize_encoded(&lit, Some(&empty_terms())).is_err());
        let decoded = Binding::lit(fluree_db_core::FlakeValue::Long(1), Sid::new(2, "long"));
        assert_eq!(materialize_encoded(&decoded, None).unwrap(), decoded);
    }

    /// An arena literal keys by its value or not at all: with no view, or one
    /// that cannot decode it, every key surface refuses it.
    #[test]
    fn arena_keys_fail_closed() {
        for kind in [
            fluree_db_core::ObjKind::NUM_BIG,
            fluree_db_core::ObjKind::VECTOR_ID,
        ] {
            let lit = arena_lit(kind.as_u8(), 2);
            assert!(crate::object_binding::normalize_for_key_cow(&lit, None, None).is_err());
            assert!(binding_to_group_key_normalized(&lit, None, None).is_err());
            assert!(
                binding_to_group_key_normalized(&lit, None, Some(&empty_terms())).is_err(),
                "{kind:?}: no arena in an empty store"
            );
            let no_graph = arena_lit(kind.as_u8(), fluree_db_core::NO_ARENA_GRAPH);
            assert!(
                binding_to_group_key_normalized(&no_graph, None, Some(&empty_terms())).is_err(),
                "{kind:?}: no graph"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stored vector's key: `handle` of a one-shard arena holding
    /// `vectors`, under `(store_id, p_id)`, without an identity table.
    fn arena_key(store_id: u64, p_id: u32, vectors: &[&[f32]], handle: u32) -> VectorKey {
        use fluree_db_binary_index::arena::vector::{VectorShard, VectorSlice};
        let dims = vectors[0].len();
        let shard = Arc::new(VectorShard {
            dims: dims as u16,
            count: vectors.len() as u32,
            values: vectors.iter().flat_map(|v| v.iter().copied()).collect(),
        });
        let slice = VectorSlice::new(shard, handle).unwrap();
        let hash = fluree_db_core::value::vector_value_hash_f32(slice.as_f32());
        VectorKey {
            hash,
            repr: VectorRepr::Arena(fluree_db_binary_index::ArenaVector {
                hash,
                store_id,
                g_id: 0,
                p_id,
                handle,
                form: fluree_db_binary_index::ArenaVectorForm::Slice(slice),
            }),
        }
    }

    fn hash_of(key: &VectorKey) -> u64 {
        use std::hash::BuildHasher;
        FxBuildHasher.hash_one(key)
    }

    /// One vector, keyed as an arena slice, as decoded novelty and as a
    /// `VALUES` literal, is one key; another vector at the same offset of
    /// another arena is not.
    #[test]
    fn vector_keys_agree_across_forms() {
        let stored = arena_key(1, 7, &[&[0.25, -1.5], &[0.5, 0.5]], 1);
        let novelty = VectorKey::decoded(&Arc::from(vec![0.5, 0.5]));
        let values = VectorKey::of_binding(
            &Binding::lit(
                FlakeValue::Vector(Arc::from(vec![0.5f64, 0.5])),
                Sid::new(0, "v"),
            ),
            None,
        )
        .unwrap()
        .unwrap();
        for other in [&novelty, &values] {
            assert_eq!(&stored, other);
            assert_eq!(hash_of(&stored), hash_of(other));
        }
        assert_eq!(novelty, values);

        let elsewhere = arena_key(1, 8, &[&[9.0, 9.0], &[0.75, 0.5]], 1);
        assert_ne!(stored, elsewhere);
        // The same value at another offset of another arena is equal.
        let moved = arena_key(2, 8, &[&[0.5, 0.5]], 0);
        assert_eq!(stored, moved);
        assert_eq!(hash_of(&stored), hash_of(&moved));
    }

    /// Equality is bitwise on the components, as `FlakeValue`'s is: `-0.0`
    /// and `0.0` differ; a query literal whose component doesn't narrow to
    /// `f32` equals no stored vector; a NaN equals itself, so a table keyed
    /// by it stays consistent.
    #[test]
    fn vector_key_equality_is_bitwise_and_hash_consistent() {
        let zero = arena_key(1, 7, &[&[0.0, 1.0]], 0);
        let neg_zero = VectorKey::decoded(&Arc::from(vec![-0.0, 1.0]));
        assert_ne!(zero, neg_zero);
        assert_eq!(zero, VectorKey::decoded(&Arc::from(vec![0.0, 1.0])));

        let stored = arena_key(1, 7, &[&[0.1f32]], 0);
        let literal = VectorKey::decoded(&Arc::from(vec![0.1f64]));
        let widened = VectorKey::decoded(&Arc::from(vec![f64::from(0.1f32)]));
        assert_ne!(stored, literal, "0.1 as f64 is not the stored f32 0.1");
        assert_eq!(stored, widened);
        assert_eq!(hash_of(&stored), hash_of(&widened));
        assert!(
            FlakeValue::Vector(Arc::from(vec![0.1f64]))
                != FlakeValue::Vector(Arc::from(vec![f64::from(0.1f32)])),
            "the decoded values agree"
        );

        let nan = VectorKey::decoded(&Arc::from(vec![f64::NAN]));
        let nan_again = VectorKey::decoded(&Arc::from(vec![f64::NAN]));
        assert_eq!(nan, nan_again);
        assert_eq!(hash_of(&nan), hash_of(&nan_again));
        let stored_nan = arena_key(1, 7, &[&[f32::NAN]], 0);
        assert_eq!(stored_nan == nan, hash_of(&stored_nan) == hash_of(&nan));
        let mut set = FxHashSet::default();
        set.insert(nan.clone());
        assert!(set.contains(&nan_again));
    }
    use crate::ir::InputSemantics;
    use fluree_db_core::LedgerSnapshot;
    use std::collections::HashMap;

    fn make_test_snapshot() -> LedgerSnapshot {
        LedgerSnapshot::genesis("test:main")
    }

    /// `fast_count`'s exact NumBig branch keys its whole slice with a constant
    /// `xsd:decimal`, where the general pipeline resolves `xsd:integer` for an
    /// overflow BigInt and `xsd:decimal` for a BigDecimal. That is only sound
    /// because the discriminant already separates the two variants, so the
    /// datatype cannot merge anything it would otherwise keep apart.
    ///
    /// Merging these discriminants would make that branch silently undercount a
    /// graph holding a BigInt and a BigDecimal with the same lexical form, and
    /// no fixture over real data would necessarily catch it. Pinned here
    /// because this is where the invariant lives.
    #[test]
    fn big_decimal_and_big_int_key_apart_regardless_of_datatype() {
        use fluree_db_core::{DatatypeConstraint, Sid};
        use std::str::FromStr;

        let same_datatype = DatatypeConstraint::Explicit(Sid::xsd_decimal());
        let dec = FlakeValue::Decimal(Box::new(
            bigdecimal::BigDecimal::from_str("170141183460469231731687303715884105727").unwrap(),
        ));
        let int = FlakeValue::BigInt(Box::new(
            num_bigint::BigInt::from_str("170141183460469231731687303715884105727").unwrap(),
        ));

        let dec_key = flake_value_to_key(&dec, &same_datatype);
        let int_key = flake_value_to_key(&int, &same_datatype);
        assert_ne!(
            dec_key, int_key,
            "a BigDecimal and a BigInt with the same lexical form must not \
             collapse into one group when the datatype is held constant"
        );

        // And the datatype genuinely adds nothing here: resolving it per
        // variant, as the general pipeline does, gives the same partition.
        let per_variant_dec = flake_value_to_key(&dec, &same_datatype);
        let per_variant_int =
            flake_value_to_key(&int, &DatatypeConstraint::Explicit(Sid::xsd_integer()));
        assert_ne!(per_variant_dec, per_variant_int);
        assert_eq!(
            dec_key, per_variant_dec,
            "the decimal side is unaffected by the datatype choice"
        );
    }

    /// A path's group key keys on nodes AND per-hop edges, matching
    /// PartialEq/Hash — so GROUP BY does not merge two paths that DISTINCT keeps
    /// apart (same node sequence reached via different parallel edges). With no
    /// edges (the RDF/JSON-LD surface) two same-node paths still group together.
    #[test]
    fn path_group_key_includes_edges() {
        let sid = |n: &str| Sid::new(1, n);
        let nodes = vec![sid("a"), sid("b")];
        let path = |edges: Vec<(Sid, Sid, Sid)>| Binding::Path {
            nodes: nodes.clone(),
            edges,
        };

        let key = |b: &Binding| binding_to_group_key_owned(b);

        // Same nodes, different edge predicate → distinct group keys (and the
        // bindings are distinct under PartialEq, the DISTINCT contract).
        let p_knows = path(vec![(sid("a"), sid("knows"), sid("b"))]);
        let p_likes = path(vec![(sid("a"), sid("likes"), sid("b"))]);
        assert_ne!(key(&p_knows), key(&p_likes));
        assert_ne!(p_knows, p_likes);

        // Same nodes and edges → identical group key.
        assert_eq!(
            key(&p_knows),
            key(&path(vec![(sid("a"), sid("knows"), sid("b"))]))
        );

        // No edges (RDF surface): two same-node paths group together.
        assert_eq!(key(&path(vec![])), key(&path(vec![])));
    }

    /// R3-A: the hash-aggregate fold runs entirely in the first `next_batch`, over
    /// a possibly fully-buffered join child, with no poll of its own. With a
    /// pre-cancelled deadline it must abort typed `Cancelled` at the fold's
    /// per-batch poll, not run to completion.
    #[tokio::test]
    async fn r3a_group_aggregate_polls_cancellation() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{QueryCancellation, QueryCancellationReason};

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let cancel = QueryCancellation::new();
        cancel.cancel_with(QueryCancellationReason::Timeout);
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancel);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let batch = Batch::new(
            schema.clone(),
            vec![
                vec![Binding::sid(Sid::new(100, "v"))],
                vec![Binding::sid(Sid::new(200, "p"))],
            ],
        )
        .unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });
        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::Count(VarId(1)),
            input_col: Some(1),
            output_var: VarId(2),
        }];
        let mut op = GroupAggregateOperator::new(child, vec![VarId(0)], agg_specs, None, false);

        // Either open() (if it polls) or the first next_batch (where the fold runs)
        // must surface the deadline.
        let result = match op.open(&ctx).await {
            Err(e) => Err(e),
            Ok(()) => op.next_batch(&ctx).await.map(|_| ()),
        };
        assert!(
            matches!(result, Err(crate::error::QueryError::Cancelled { .. })),
            "fold must poll the deadline, got {result:?}"
        );
    }

    /// R3-B: a tiny memory budget makes the GroupAggregate fold abort typed
    /// (`MemoryBudgetExceeded`) as the groups map grows, before OOM.
    #[tokio::test]
    async fn r3b_group_aggregate_budget_aborts_typed() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::QueryCancellation;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        // Pin a 1-byte ceiling → the first group's recorded growth crosses it, so the
        // next checkpoint aborts typed.
        let cancel = QueryCancellation::new();
        cancel.set_memory_limit(1);
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancel);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let batch = Batch::new(
            schema.clone(),
            vec![
                vec![Binding::sid(Sid::new(100, "v"))],
                vec![Binding::sid(Sid::new(200, "p"))],
            ],
        )
        .unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });
        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::Count(VarId(1)),
            input_col: Some(1),
            output_var: VarId(2),
        }];
        let mut op = GroupAggregateOperator::new(child, vec![VarId(0)], agg_specs, None, false);
        op.open(&ctx).await.unwrap();
        let err = op.next_batch(&ctx).await.unwrap_err();
        assert!(
            matches!(err, crate::error::QueryError::MemoryBudgetExceeded { .. }),
            "fold must abort typed on the memory budget, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_streaming_count() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Create input: 5 papers for venue A, 3 papers for venue B
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice()); // ?venue, ?paper
        let columns = vec![
            // ?venue
            vec![
                Binding::sid(Sid::new(100, "venueA")),
                Binding::sid(Sid::new(100, "venueA")),
                Binding::sid(Sid::new(100, "venueA")),
                Binding::sid(Sid::new(100, "venueA")),
                Binding::sid(Sid::new(100, "venueA")),
                Binding::sid(Sid::new(100, "venueB")),
                Binding::sid(Sid::new(100, "venueB")),
                Binding::sid(Sid::new(100, "venueB")),
            ],
            // ?paper
            vec![
                Binding::sid(Sid::new(200, "paper1")),
                Binding::sid(Sid::new(200, "paper2")),
                Binding::sid(Sid::new(200, "paper3")),
                Binding::sid(Sid::new(200, "paper4")),
                Binding::sid(Sid::new(200, "paper5")),
                Binding::sid(Sid::new(200, "paper6")),
                Binding::sid(Sid::new(200, "paper7")),
                Binding::sid(Sid::new(200, "paper8")),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // GROUP BY ?venue, COUNT(?paper) as ?count
        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::Count(VarId(1)), // ?paper
            input_col: Some(1),
            output_var: VarId(2),
        }];

        let mut op = GroupAggregateOperator::new(child, vec![VarId(0)], agg_specs, None, false);
        op.open(&ctx).await.unwrap();

        // Collect results
        let mut results: Vec<(Binding, i64)> = Vec::new();
        while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
            for row_idx in 0..batch.len() {
                let venue = batch.get_by_col(row_idx, 0).clone();
                let count = batch.get_by_col(row_idx, 1);
                if let Binding::Lit {
                    val: FlakeValue::Long(n),
                    ..
                } = count
                {
                    results.push((venue, *n));
                }
            }
        }

        // Verify results
        assert_eq!(results.len(), 2);

        // Find venueA and venueB counts
        let venue_a_count = results
            .iter()
            .find(|(v, _)| {
                if let Binding::Sid { sid, .. } = v {
                    sid.name.as_ref() == "venueA"
                } else {
                    false
                }
            })
            .map(|(_, c)| *c);
        let venue_b_count = results
            .iter()
            .find(|(v, _)| {
                if let Binding::Sid { sid, .. } = v {
                    sid.name.as_ref() == "venueB"
                } else {
                    false
                }
            })
            .map(|(_, c)| *c);

        assert_eq!(venue_a_count, Some(5));
        assert_eq!(venue_b_count, Some(3));

        op.close();
    }

    #[tokio::test]
    async fn test_streaming_sum_avg() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Create input: category A with values 10, 20, 30; category B with values 5, 15
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            // ?category
            vec![
                Binding::sid(Sid::new(100, "catA")),
                Binding::sid(Sid::new(100, "catA")),
                Binding::sid(Sid::new(100, "catA")),
                Binding::sid(Sid::new(100, "catB")),
                Binding::sid(Sid::new(100, "catB")),
            ],
            // ?value
            vec![
                Binding::lit(FlakeValue::Long(10), Sid::xsd_integer()),
                Binding::lit(FlakeValue::Long(20), Sid::xsd_integer()),
                Binding::lit(FlakeValue::Long(30), Sid::xsd_integer()),
                Binding::lit(FlakeValue::Long(5), Sid::xsd_integer()),
                Binding::lit(FlakeValue::Long(15), Sid::xsd_integer()),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // GROUP BY ?category, SUM(?value), AVG(?value)
        let list = InputSemantics::List;
        let agg_specs = vec![
            StreamingAggSpec {
                function: AggregateFn::Sum(VarId(1), list),
                input_col: Some(1),
                output_var: VarId(2),
            },
            StreamingAggSpec {
                function: AggregateFn::Avg(VarId(1), list),
                input_col: Some(1),
                output_var: VarId(3),
            },
        ];

        let mut op = GroupAggregateOperator::new(child, vec![VarId(0)], agg_specs, None, false);
        op.open(&ctx).await.unwrap();

        let mut results: HashMap<String, (i64, String)> = HashMap::new();
        while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
            for row_idx in 0..batch.len() {
                let cat = batch.get_by_col(row_idx, 0);
                let sum_val = batch.get_by_col(row_idx, 1);
                let avg_val = batch.get_by_col(row_idx, 2);

                if let Binding::Sid { sid, .. } = cat {
                    let sum = match sum_val {
                        Binding::Lit {
                            val: FlakeValue::Long(n),
                            ..
                        } => *n,
                        _ => panic!("SUM of integers should yield xsd:integer (Long)"),
                    };
                    // Per W3C XPath numeric promotion, AVG of integer inputs
                    // yields xsd:decimal (not xsd:double).
                    let avg = match avg_val {
                        Binding::Lit {
                            val: FlakeValue::Decimal(d),
                            ..
                        } => d.to_string(),
                        _ => panic!("AVG of integers should yield xsd:decimal"),
                    };
                    results.insert(sid.name.to_string(), (sum, avg));
                }
            }
        }

        assert_eq!(results.get("catA"), Some(&(60, "20".to_string())));
        assert_eq!(results.get("catB"), Some(&(20, "10".to_string())));

        op.close();
    }

    #[test]
    fn test_all_streamable() {
        let v0 = VarId(0);
        let streamable = vec![
            StreamingAggSpec {
                function: AggregateFn::Count(v0),
                input_col: Some(0),
                output_var: VarId(1),
            },
            StreamingAggSpec {
                function: AggregateFn::Sum(v0, InputSemantics::List),
                input_col: Some(0),
                output_var: VarId(2),
            },
        ];
        assert!(GroupAggregateOperator::all_streamable(&streamable));

        let non_streamable = vec![
            StreamingAggSpec {
                function: AggregateFn::Count(v0),
                input_col: Some(0),
                output_var: VarId(1),
            },
            StreamingAggSpec {
                function: AggregateFn::GroupConcat {
                    input: v0,
                    semantics: InputSemantics::List,
                    separator: ",".to_string(),
                },
                input_col: Some(0),
                output_var: VarId(2),
            },
        ];
        assert!(!GroupAggregateOperator::all_streamable(&non_streamable));

        // DISTINCT SUM is not streamable (needs to collect all values for dedup)
        let distinct_sum = vec![StreamingAggSpec {
            function: AggregateFn::Sum(v0, InputSemantics::Set),
            input_col: Some(0),
            output_var: VarId(1),
        }];
        assert!(!GroupAggregateOperator::all_streamable(&distinct_sum));

        // MIN doesn't carry a `distinct` flag — the variant elides it because
        // DISTINCT has no effect on min/max — so this is just a streamable Min.
        let plain_min = vec![StreamingAggSpec {
            function: AggregateFn::Min(v0),
            input_col: Some(0),
            output_var: VarId(1),
        }];
        assert!(GroupAggregateOperator::all_streamable(&plain_min));
    }

    /// GROUP BY and COUNT(DISTINCT) key a double by its value: every NaN is
    /// one key, -0.0 is 0.0, and the infinities stay apart.
    #[test]
    fn group_keys_identify_doubles_by_value() {
        use fluree_db_core::{DatatypeConstraint, Sid};
        let dt = DatatypeConstraint::Explicit(Sid::xsd_double());
        let key = |bits: u64| flake_value_to_key(&FlakeValue::Double(f64::from_bits(bits)), &dt);
        let nan = key(f64::NAN.to_bits());
        for bits in [
            0xFFF8_0000_0000_0000u64,
            0x7FF0_0000_0000_0001,
            0xFFFF_FFFF_FFFF_FFFF,
        ] {
            assert_eq!(key(bits), nan, "{bits:#x}");
        }
        assert_eq!(key((-0.0f64).to_bits()), key(0.0f64.to_bits()));
        assert_ne!(
            key(f64::INFINITY.to_bits()),
            key(f64::NEG_INFINITY.to_bits())
        );
        assert_ne!(key(f64::INFINITY.to_bits()), nan);
    }

    /// A legacy pre-normalization NumBig arena holds `1.5` under two handles.
    /// The partitioned lane groups by run of the encoded key, so it would
    /// count the value as two groups; on a graph with such an arena it folds
    /// big numbers by value, wherever their rows fall in the input.
    #[tokio::test]
    async fn partitioned_group_by_folds_legacy_numbig_scale_variants_by_value() {
        use crate::context::ExecutionContext;
        use crate::seed::BatchSeedOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_binary_index::arena::numbig::read_numbig_arena_from_bytes;
        use fluree_db_binary_index::BinaryIndexStore;
        use fluree_db_core::o_type::OType;
        use num_bigint::BigInt;

        /// One decimal entry of an NBA1 buffer, as a pre-normalization build
        /// wrote it: unscaled value and scale as given.
        fn bigdec_entry(buf: &mut Vec<u8>, unscaled: i64, scale: i64) {
            let unscaled_bytes = BigInt::from(unscaled).to_signed_bytes_le();
            buf.push(1);
            let repr_len = 4 + unscaled_bytes.len() + 8;
            buf.extend_from_slice(&(repr_len as u32).to_le_bytes());
            buf.extend_from_slice(&(unscaled_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&unscaled_bytes);
            buf.extend_from_slice(&scale.to_le_bytes());
        }
        let mut buf = Vec::new();
        buf.extend_from_slice(b"NBA1");
        buf.extend_from_slice(&3u32.to_le_bytes());
        bigdec_entry(&mut buf, 150, 2); // handle 0: 1.50
        bigdec_entry(&mut buf, 15, 1); // handle 1: 1.5, the same value
        bigdec_entry(&mut buf, 1999, 2); // handle 2: 19.99
        let arena = read_numbig_arena_from_bytes(&buf).unwrap();
        assert!(arena.has_duplicate_handles());

        const P_ID: u32 = 7;
        let mut store = BinaryIndexStore::empty(std::env::temp_dir());
        store.set_numbig_arena_for_test(0, P_ID, arena);
        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars).with_binary_store(Arc::new(store), 0);

        let big = |handle: u64| {
            crate::object_binding::late_materialized_object_binding(
                OType::NUM_BIG_OVERFLOW.as_u16(),
                handle,
                P_ID,
                1,
                u32::MAX,
                None,
                0,
            )
            .unwrap()
        };
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = Batch::new(schema, vec![vec![big(0), big(2), big(1)]]).unwrap();
        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::CountAll,
            input_col: None,
            output_var: VarId(1),
        }];
        let mut op = GroupAggregateOperator::new(
            Box::new(BatchSeedOperator::from_batch(batch)),
            vec![VarId(0)],
            agg_specs,
            None,
            true,
        );
        op.open(&ctx).await.unwrap();
        let mut counts = Vec::new();
        while let Some(out) = op.next_batch(&ctx).await.unwrap() {
            for row in 0..out.len() {
                match out.get_by_col(row, 1) {
                    Binding::Lit {
                        val: FlakeValue::Long(n),
                        ..
                    } => counts.push(*n),
                    other => panic!("count: {other:?}"),
                }
            }
        }
        counts.sort_unstable();
        assert_eq!(counts, [1, 2], "1.5 and 1.50 are one group");
    }
}
