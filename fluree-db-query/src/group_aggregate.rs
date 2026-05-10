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
use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

/// Specification for a streaming aggregate
#[derive(Debug, Clone)]
pub struct StreamingAggSpec {
    /// The aggregate function
    pub function: AggregateFn,
    /// Input column index (None for COUNT(*))
    pub input_col: Option<usize>,
    /// Output variable ID
    pub output_var: VarId,
    /// Whether DISTINCT was specified (e.g., SUM(DISTINCT ?x)).
    /// Used by `all_streamable()` to route DISTINCT SUM/AVG to the
    /// traditional AggregateOperator path which handles deduplication.
    pub distinct: bool,
}

/// Per-group streaming aggregate state
#[derive(Debug)]
enum AggState {
    /// COUNT/COUNT(*) - just a counter
    Count { n: u64 },
    /// COUNT(DISTINCT) - HashSet of seen values
    CountDistinct { seen: HashSet<GroupKeyOwned> },
    /// SUM - running total (as f64 for mixed types)
    Sum { total: f64, has_int_only: bool },
    /// AVG - sum and count
    Avg { sum: f64, count: u64 },
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
    gv: Option<&BinaryGraphView>,
) -> std::cmp::Ordering {
    let Some(gv) = gv else {
        return crate::sort::compare_bindings(a, b);
    };
    let store = gv.store();

    // Fast path 1: subject IDs (IRIs) — compare lexicographically without allocation.
    if let (Binding::EncodedSid { s_id: a_id, .. }, Binding::EncodedSid { s_id: b_id, .. }) = (a, b)
    {
        if let Ok(ord) = store.compare_subject_iri_lex(*a_id, *b_id) {
            return ord;
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
                    return ord;
                }
            }
        }
    }

    // General path: materialize then use SPARQL ordering comparator.
    // BinaryGraphView handles novelty watermark routing internally.
    let am = materialize_for_minmax(a, Some(gv));
    let bm = materialize_for_minmax(b, Some(gv));
    crate::sort::compare_bindings(&am, &bm)
}

fn materialize_for_minmax(binding: &Binding, gv: Option<&BinaryGraphView>) -> Binding {
    let Some(gv) = gv else {
        return binding.clone();
    };
    let store = gv.store();

    match binding {
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            i_val,
            t,
        } => {
            // BinaryGraphView handles novelty watermark routing internally.
            match gv.decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id) {
                Ok(fluree_db_core::FlakeValue::Ref(sid)) => Binding::sid(sid),
                Ok(val) => {
                    let dt_sid = store
                        .dt_sids()
                        .get(*dt_id as usize)
                        .cloned()
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
                Err(_) => binding.clone(),
            }
        }
        Binding::EncodedSid { s_id, .. } => match gv.resolve_subject_sid(*s_id) {
            Ok(sid) => Binding::sid(sid),
            Err(_) => binding.clone(),
        },
        Binding::EncodedPid { p_id } => match store.resolve_predicate_iri(*p_id) {
            Some(iri) => Binding::sid(store.encode_iri(iri)),
            None => binding.clone(),
        },
        _ => binding.clone(),
    }
}

impl AggState {
    fn new(func: &AggregateFn) -> Self {
        match func {
            AggregateFn::Count | AggregateFn::CountAll => AggState::Count { n: 0 },
            AggregateFn::CountDistinct => AggState::CountDistinct {
                seen: HashSet::new(),
            },
            AggregateFn::Sum => AggState::Sum {
                total: 0.0,
                has_int_only: true,
            },
            AggregateFn::Avg => AggState::Avg { sum: 0.0, count: 0 },
            AggregateFn::Min => AggState::Min { min: None },
            AggregateFn::Max => AggState::Max { max: None },
            // Non-streamable: collect all values
            AggregateFn::Median
            | AggregateFn::Variance
            | AggregateFn::Stddev
            | AggregateFn::GroupConcat { .. } => AggState::Collect { values: Vec::new() },
            // SAMPLE is explicitly arbitrary in SPARQL; we pick the first observed value.
            AggregateFn::Sample => AggState::Sample { sample: None },
        }
    }

    /// Update state with a new binding value
    ///
    /// # Arguments
    ///
    /// * `binding` - The binding value to incorporate
    /// * `gv` - Optional graph view for materializing encoded bindings (needed for MIN/MAX).
    ///   When the graph view is novelty-aware, watermark routing is handled internally.
    fn update(&mut self, binding: &Binding, gv: Option<&BinaryGraphView>) {
        match self {
            AggState::Count { n } => {
                // COUNT: count non-Unbound values (COUNT(*) counts all via CountAll variant)
                if !matches!(binding, Binding::Unbound | Binding::Poisoned) {
                    *n += 1;
                }
            }
            AggState::CountDistinct { seen } => {
                if !matches!(binding, Binding::Unbound | Binding::Poisoned) {
                    // Convert binding to owned group key for HashSet
                    let key = binding_to_group_key_owned(binding);
                    seen.insert(key);
                }
            }
            AggState::Sum {
                total,
                has_int_only,
            } => {
                if let Some(num) = extract_number(binding) {
                    *total += num;
                    if !is_int_binding(binding) {
                        *has_int_only = false;
                    }
                }
            }
            AggState::Avg { sum, count } => {
                if let Some(num) = extract_number(binding) {
                    *sum += num;
                    *count += 1;
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
                            if compare_for_minmax(binding, current, gv) == std::cmp::Ordering::Less
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
                            if compare_for_minmax(binding, current, gv)
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
                values.push(binding.clone());
            }
        }
    }

    /// Update for COUNT(*) - increment regardless of binding value
    fn update_count_all(&mut self) {
        if let AggState::Count { n } = self {
            *n += 1;
        }
    }

    /// Finalize the aggregate state into a result binding
    fn finalize(self, func: &AggregateFn) -> Binding {
        match self {
            AggState::Count { n } => Binding::lit(FlakeValue::Long(n as i64), Sid::xsd_integer()),
            AggState::CountDistinct { seen } => {
                Binding::lit(FlakeValue::Long(seen.len() as i64), Sid::xsd_integer())
            }
            AggState::Sum {
                total,
                has_int_only,
            } => {
                if has_int_only && total.fract() == 0.0 {
                    Binding::lit(FlakeValue::Long(total as i64), Sid::xsd_integer())
                } else {
                    Binding::lit(FlakeValue::Double(total), Sid::xsd_double())
                }
            }
            AggState::Avg { sum, count } => {
                if count == 0 {
                    Binding::Unbound
                } else {
                    Binding::lit(FlakeValue::Double(sum / count as f64), Sid::xsd_double())
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
                crate::aggregate::apply_aggregate(func, &Binding::Grouped(values), false)
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
        /// `p_id` is only required for NUM_BIG decoding (per-predicate arena).
        /// For all other literal kinds, it must not affect grouping identity.
        p_id_for_numbig: Option<u32>,
        dt_id: u16,
        lang_id: u16,
    },
    /// Materialized Sid (namespace_code, name)
    MaterializedSid(u16, Arc<str>),
    /// Materialized literal value
    MaterializedLit(MaterializedLitKey),
    /// Unbound/Poisoned
    Absent,
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
            ..
        } => GroupKeyOwned::Lit {
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
        Binding::Sid { sid, .. } => {
            GroupKeyOwned::MaterializedSid(sid.namespace_code, sid.name.clone())
        }
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
        Binding::Grouped(_) => GroupKeyOwned::Absent, // Shouldn't happen
    }
}

fn flake_value_to_key(val: &FlakeValue, dtc: &DatatypeConstraint) -> MaterializedLitKey {
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
        FlakeValue::Double(d) => MaterializedLitKey {
            discriminant: 3,
            string_val: None,
            number_bits: Some(d.to_bits()),
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

/// Per-group state: the key bindings and aggregate states
struct GroupState {
    /// Original bindings for group key columns (for output)
    key_bindings: Vec<Binding>,
    /// Aggregate states (one per aggregate spec)
    agg_states: Vec<AggState>,
}

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
    groups: HashMap<CompositeGroupKey, GroupState>,
    /// If true, input is already partitioned by the GROUP BY key(s), so we can
    /// aggregate per-run without hashing each row into a map.
    partitioned: bool,
    /// Accumulated groups in partitioned mode (in input order).
    partitioned_groups: Vec<GroupState>,
    /// Iterator for emitting results.
    emit_iter: Option<GroupEmitIter>,
    /// Graph view for materializing encoded bindings (used for MIN/MAX semantic ordering).
    /// When novelty-aware (via `ExecutionContext::graph_view()`), watermark routing
    /// for novelty-only subject/string IDs is handled internally.
    graph_view: Option<BinaryGraphView>,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

enum GroupEmitIter {
    Hash(std::collections::hash_map::IntoIter<CompositeGroupKey, GroupState>),
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
        graph_view: Option<BinaryGraphView>,
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

        Self {
            child,
            in_schema: schema,
            state: OperatorState::Created,
            group_key_indices,
            agg_specs,
            groups: HashMap::new(),
            partitioned,
            partitioned_groups: Vec::new(),
            emit_iter: None,
            graph_view,
            out_schema: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Check if all aggregates are streamable (for planner optimization decisions)
    ///
    /// DISTINCT SUM/AVG are not streamable because deduplication requires collecting
    /// all values before computing the aggregate. COUNT(DISTINCT) and MIN/MAX(DISTINCT)
    /// remain streamable — COUNT(DISTINCT) already tracks a HashSet, and DISTINCT is
    /// idempotent for MIN/MAX.
    pub fn all_streamable(specs: &[StreamingAggSpec]) -> bool {
        specs.iter().all(|spec| {
            let is_streamable_fn = matches!(
                spec.function,
                AggregateFn::Count
                    | AggregateFn::CountAll
                    | AggregateFn::CountDistinct
                    | AggregateFn::Sum
                    | AggregateFn::Avg
                    | AggregateFn::Min
                    | AggregateFn::Max
                    | AggregateFn::Sample
            );
            // DISTINCT SUM/AVG need to collect all values for dedup — not streamable
            let distinct_blocks =
                spec.distinct && matches!(spec.function, AggregateFn::Sum | AggregateFn::Avg);
            is_streamable_fn && !distinct_blocks
        })
    }

    /// Extract composite group key from a row
    fn extract_group_key(&self, batch: &Batch, row_idx: usize) -> CompositeGroupKey {
        let keys: Vec<GroupKeyOwned> = self
            .group_key_indices
            .iter()
            .map(|&col_idx| {
                let binding = batch.get_by_col(row_idx, col_idx);
                binding_to_group_key_owned(binding)
            })
            .collect();
        CompositeGroupKey(keys)
    }

    /// Extract original bindings for group key columns (for output)
    fn extract_key_bindings(&self, batch: &Batch, row_idx: usize) -> Vec<Binding> {
        self.group_key_indices
            .iter()
            .map(|&col_idx| batch.get_by_col(row_idx, col_idx).clone())
            .collect()
    }
}

#[async_trait]
impl Operator for GroupAggregateOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.groups.clear();
        self.partitioned_groups.clear();
        self.emit_iter = None;
        // If the execution context has a binary store, use it to materialize
        // encoded bindings for correct MIN/MAX comparison semantics.
        if self.graph_view.is_none() {
            self.graph_view = ctx.graph_view();
        }
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
            && !self.agg_specs[0].distinct
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

                    loop {
                        let batch = match self.child.next_batch(ctx).await? {
                            Some(b) => b,
                            None => break,
                        };
                        input_batches += 1;
                        if batch.is_empty() {
                            continue;
                        }

                        for row_idx in 0..batch.len() {
                            input_rows += 1;
                            let key_binding = batch.get_by_col(row_idx, key_col);
                            let key = binding_to_group_key_owned(key_binding);

                            let same_group = current_key.as_ref().is_some_and(|k| k == &key);
                            if !same_group {
                                if let Some(state) = current_state.take() {
                                    self.partitioned_groups.push(state);
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

                            // Update aggregate states for current group.
                            let gv_ref = self.graph_view.as_ref();
                            let group_state = current_state
                                .as_mut()
                                .expect("partitioned aggregation must have current group state");
                            for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
                                match spec.input_col {
                                    Some(col_idx) => {
                                        let binding = batch.get_by_col(row_idx, col_idx);
                                        group_state.agg_states[agg_idx].update(binding, gv_ref);
                                    }
                                    None => {
                                        // COUNT(*) - count all rows
                                        group_state.agg_states[agg_idx].update_count_all();
                                    }
                                }
                            }
                        }
                    }

                    if let Some(state) = current_state.take() {
                        self.partitioned_groups.push(state);
                    }

                    span.record("input_batches", input_batches);
                    span.record("input_rows", input_rows);
                    span.record("groups", self.partitioned_groups.len() as u64);
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
                loop {
                    let batch = match self.child.next_batch(ctx).await? {
                        Some(b) => b,
                        None => break,
                    };
                    input_batches += 1;

                    if batch.is_empty() {
                        continue;
                    }

                    // Process each row
                    for row_idx in 0..batch.len() {
                        input_rows += 1;

                        // Extract composite group key
                        let group_key = self.extract_group_key(&batch, row_idx);

                        // Extract key bindings BEFORE the mutable borrow to avoid borrow conflict
                        let key_bindings = self.extract_key_bindings(&batch, row_idx);

                        // Pre-compute aggregate states initialization
                        let agg_specs_ref = &self.agg_specs;

                        // Get or create group state
                        let group_state =
                            self.groups.entry(group_key).or_insert_with(|| GroupState {
                                key_bindings,
                                agg_states: agg_specs_ref
                                    .iter()
                                    .map(|spec| AggState::new(&spec.function))
                                    .collect(),
                            });

                        // Update each aggregate with this row's values
                        let gv_ref = self.graph_view.as_ref();
                        for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
                            match spec.input_col {
                                Some(col_idx) => {
                                    let binding = batch.get_by_col(row_idx, col_idx);
                                    group_state.agg_states[agg_idx].update(binding, gv_ref);
                                }
                                None => {
                                    // COUNT(*) - count all rows
                                    group_state.agg_states[agg_idx].update_count_all();
                                }
                            }
                        }
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
fn extract_number(binding: &Binding) -> Option<f64> {
    use fluree_db_core::value_id::{ObjKey, ObjKind};

    match binding {
        Binding::Lit { val, .. } => match val {
            FlakeValue::Long(n) => Some(*n as f64),
            FlakeValue::Boolean(b) => Some(i64::from(*b) as f64),
            FlakeValue::Double(d) if !d.is_nan() => Some(*d),
            _ => None,
        },
        Binding::EncodedLit { o_kind, o_key, .. } => {
            // Decode numeric value from o_key based on o_kind
            // i_val is list index metadata, NOT the numeric value!
            if *o_kind == ObjKind::NUM_INT.as_u8() {
                // i64 encoded in o_key via order-preserving XOR transform
                Some(ObjKey::from_u64(*o_key).decode_i64() as f64)
            } else if *o_kind == ObjKind::NUM_F64.as_u8() {
                // f64 encoded in o_key via order-preserving bit transform
                let decoded = ObjKey::from_u64(*o_key).decode_f64();
                if decoded.is_nan() {
                    None
                } else {
                    Some(decoded)
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Check if binding is an integer type
fn is_int_binding(binding: &Binding) -> bool {
    match binding {
        Binding::Lit { val, .. } => matches!(val, FlakeValue::Long(_) | FlakeValue::Boolean(_)),
        Binding::EncodedLit { o_kind, .. } => {
            // ObjKind for Long is typically stored as a specific value
            // This needs to match the actual encoding
            *o_kind == fluree_db_core::ObjKind::NUM_INT.as_u8()
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::LedgerSnapshot;

    fn make_test_snapshot() -> LedgerSnapshot {
        LedgerSnapshot::genesis("test/main")
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
            function: AggregateFn::Count,
            input_col: Some(1), // ?paper
            output_var: VarId(2),
            distinct: false,
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
        let agg_specs = vec![
            StreamingAggSpec {
                function: AggregateFn::Sum,
                input_col: Some(1),
                output_var: VarId(2),
                distinct: false,
            },
            StreamingAggSpec {
                function: AggregateFn::Avg,
                input_col: Some(1),
                output_var: VarId(3),
                distinct: false,
            },
        ];

        let mut op = GroupAggregateOperator::new(child, vec![VarId(0)], agg_specs, None, false);
        op.open(&ctx).await.unwrap();

        let mut results: HashMap<String, (i64, f64)> = HashMap::new();
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
                        _ => panic!("Expected Long"),
                    };
                    let avg = match avg_val {
                        Binding::Lit {
                            val: FlakeValue::Double(d),
                            ..
                        } => *d,
                        _ => panic!("Expected Double"),
                    };
                    results.insert(sid.name.to_string(), (sum, avg));
                }
            }
        }

        assert_eq!(results.get("catA"), Some(&(60, 20.0)));
        assert_eq!(results.get("catB"), Some(&(20, 10.0)));

        op.close();
    }

    #[test]
    fn test_all_streamable() {
        let streamable = vec![
            StreamingAggSpec {
                function: AggregateFn::Count,
                input_col: Some(0),
                output_var: VarId(1),
                distinct: false,
            },
            StreamingAggSpec {
                function: AggregateFn::Sum,
                input_col: Some(0),
                output_var: VarId(2),
                distinct: false,
            },
        ];
        assert!(GroupAggregateOperator::all_streamable(&streamable));

        let non_streamable = vec![
            StreamingAggSpec {
                function: AggregateFn::Count,
                input_col: Some(0),
                output_var: VarId(1),
                distinct: false,
            },
            StreamingAggSpec {
                function: AggregateFn::GroupConcat {
                    separator: ",".to_string(),
                },
                input_col: Some(0),
                output_var: VarId(2),
                distinct: false,
            },
        ];
        assert!(!GroupAggregateOperator::all_streamable(&non_streamable));

        // DISTINCT SUM is not streamable (needs to collect all values for dedup)
        let distinct_sum = vec![StreamingAggSpec {
            function: AggregateFn::Sum,
            input_col: Some(0),
            output_var: VarId(1),
            distinct: true,
        }];
        assert!(!GroupAggregateOperator::all_streamable(&distinct_sum));

        // DISTINCT MIN is streamable (idempotent)
        let distinct_min = vec![StreamingAggSpec {
            function: AggregateFn::Min,
            input_col: Some(0),
            output_var: VarId(1),
            distinct: true,
        }];
        assert!(GroupAggregateOperator::all_streamable(&distinct_min));
    }
}
