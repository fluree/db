//! Sort operator for ordering query results
//!
//! The `SortOperator` is a blocking operator that buffers all input rows,
//! sorts them by specified columns, and then emits results in batches.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{DatatypeConstraint, DatatypeDictId, FlakeValue, Sid};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

/// Keep sort diagnostics cheap by only surfacing non-trivial blocking sorts at
/// debug level during perf captures.
const SORT_DEBUG_MIN_ROWS: u64 = 1_000;
const SORT_DEBUG_MIN_MS: u64 = 25;

/// Materialize an encoded binding to its decoded form for sort comparison.
///
/// This ensures ORDER BY uses correct term ordering (namespace/name for IRIs,
/// value semantics for literals) rather than raw ID ordering.
fn materialize_encoded_for_sort(
    b: &Binding,
    gv: &BinaryGraphView,
    assume_lex_sorted_string_ids: bool,
) -> Option<Binding> {
    match b {
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            i_val,
            t,
        } => {
            // Experiment: if string IDs are lex-order-preserving, keep EncodedLit for
            // simple xsd:string values so ORDER BY can compare by raw o_key.
            if assume_lex_sorted_string_ids
                && *o_kind == ObjKind::LEX_ID.as_u8()
                && *dt_id == DatatypeDictId::STRING.as_u16()
                && *lang_id == 0
            {
                return None;
            }

            // BinaryGraphView handles novelty watermark routing internally.
            let val = gv
                .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                .ok()?;
            match val {
                FlakeValue::Ref(sid) => Some(Binding::Sid(sid)),
                other => {
                    let dt_sid = gv
                        .store()
                        .dt_sids()
                        .get(*dt_id as usize)
                        .cloned()
                        .unwrap_or_else(|| Sid::new(0, ""));
                    let meta = gv.store().decode_meta(*lang_id, *i_val);
                    let dtc = match meta.and_then(|m| m.lang.map(Arc::from)) {
                        Some(lang) => DatatypeConstraint::LangTag(lang),
                        None => DatatypeConstraint::Explicit(dt_sid),
                    };
                    Some(Binding::Lit {
                        val: other,
                        dtc,
                        t: Some(*t),
                        op: None,
                        p_id: Some(*p_id),
                    })
                }
            }
        }
        Binding::EncodedSid { s_id } => {
            // BinaryGraphView::resolve_subject_sid handles novelty routing
            // and returns Sid directly (no IRI string + trie lookup).
            let sid = gv.resolve_subject_sid(*s_id).ok()?;
            Some(Binding::Sid(sid))
        }
        Binding::EncodedPid { p_id } => {
            // Resolve to Sid for correct namespace/name ordering
            let iri = gv.store().resolve_predicate_iri(*p_id)?;
            Some(Binding::Sid(gv.store().encode_iri(iri)))
        }
        _ => None,
    }
}

/// Materialize encoded bindings in sort key columns before sorting.
///
/// This is the safe boundary for materialization: we modify bindings BEFORE
/// they are used in any hash-based operations or sorting comparisons.
fn materialize_sort_keys_in_rows(
    rows: &mut [Vec<Binding>],
    sort_col_indices: &[usize],
    gv: &BinaryGraphView,
    assume_lex_sorted_string_ids: bool,
) {
    for row in rows.iter_mut() {
        for &col_idx in sort_col_indices {
            if let Some(existing) = row.get(col_idx) {
                if let Some(materialized) =
                    materialize_encoded_for_sort(existing, gv, assume_lex_sorted_string_ids)
                {
                    row[col_idx] = materialized;
                }
            }
        }
    }
}

#[inline]
fn materialize_sort_keys_in_row(
    row: &mut Vec<Binding>,
    sort_col_indices: &[usize],
    gv: &BinaryGraphView,
    assume_lex_sorted_string_ids: bool,
) {
    materialize_sort_keys_in_rows(
        std::slice::from_mut(row),
        sort_col_indices,
        gv,
        assume_lex_sorted_string_ids,
    );
}

/// Sort direction for ORDER BY clauses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
    #[default]
    Ascending,
    Descending,
}

/// Sort specification for a single column
#[derive(Debug, Clone)]
pub struct SortSpec {
    /// Variable to sort by
    pub var: VarId,
    /// Sort direction
    pub direction: SortDirection,
}

impl SortSpec {
    /// Create an ascending sort specification
    pub fn asc(var: VarId) -> Self {
        Self {
            var,
            direction: SortDirection::Ascending,
        }
    }

    /// Create a descending sort specification
    pub fn desc(var: VarId) -> Self {
        Self {
            var,
            direction: SortDirection::Descending,
        }
    }
}

/// Compare two bindings for sorting
///
/// Ordering rules:
/// 1. Type class ordering: Unbound < Poisoned < Sid < IriMatch < Iri < Lit < Grouped
/// 2. Within Sid: compare by (namespace_code, name)
/// 3. Within IriMatch: compare by canonical IRI string (for cross-ledger consistency)
/// 4. Within Iri: compare by string value (raw IRIs from graph sources)
/// 5. Within Lit: compare values with type-aware logic
///    - Numeric promotion: Long and Double are comparable
///    - NaN is sorted last within doubles
///    - Cross-type: use type ordering
/// 6. Grouped sorts last (shouldn't appear in normal sort contexts)
pub fn compare_bindings(a: &Binding, b: &Binding) -> Ordering {
    // Type class ordering: Unbound < Poisoned < IRI types < Lit types < Grouped
    // IRI types: Sid, IriMatch, Iri, EncodedSid, EncodedPid (all in same class)
    // Lit types: Lit, EncodedLit (same class)

    match (a, b) {
        // Unbound sorts first
        (Binding::Unbound, Binding::Unbound) => Ordering::Equal,
        (Binding::Unbound, _) => Ordering::Less,
        (_, Binding::Unbound) => Ordering::Greater,

        // Poisoned sorts after Unbound but before bound values
        (Binding::Poisoned, Binding::Poisoned) => Ordering::Equal,
        (Binding::Poisoned, _) => Ordering::Less,
        (_, Binding::Poisoned) => Ordering::Greater,

        // Grouped sorts last (should not appear in normal sort contexts)
        (Binding::Grouped(_), Binding::Grouped(_)) => {
            debug_assert!(
                false,
                "Grouped bindings should not appear in sort comparisons"
            );
            Ordering::Equal
        }
        (_, Binding::Grouped(_)) => Ordering::Less,
        (Binding::Grouped(_), _) => Ordering::Greater,

        // IRI types vs Lit types: IRI sorts before Lit
        (
            Binding::Sid(_)
            | Binding::IriMatch { .. }
            | Binding::Iri(_)
            | Binding::EncodedSid { .. }
            | Binding::EncodedPid { .. },
            Binding::Lit { .. } | Binding::EncodedLit { .. },
        ) => Ordering::Less,
        (
            Binding::Lit { .. } | Binding::EncodedLit { .. },
            Binding::Sid(_)
            | Binding::IriMatch { .. }
            | Binding::Iri(_)
            | Binding::EncodedSid { .. }
            | Binding::EncodedPid { .. },
        ) => Ordering::Greater,

        // Within IRI types: compare by concrete value or ID
        (Binding::Sid(a), Binding::Sid(b)) => compare_sids(a, b),
        (Binding::IriMatch { iri: a, .. }, Binding::IriMatch { iri: b, .. }) => a.cmp(b),
        (Binding::Iri(a), Binding::Iri(b)) => a.cmp(b),
        (Binding::EncodedSid { s_id: a }, Binding::EncodedSid { s_id: b }) => a.cmp(b),
        (Binding::EncodedPid { p_id: a }, Binding::EncodedPid { p_id: b }) => a.cmp(b),
        // Cross-IRI type comparisons: Sid < IriMatch/Iri < EncodedSid/EncodedPid
        // (Prefer materialized over encoded for consistent ordering)
        (Binding::Sid(_), Binding::IriMatch { .. } | Binding::Iri(_)) => Ordering::Less,
        (Binding::IriMatch { .. } | Binding::Iri(_), Binding::Sid(_)) => Ordering::Greater,
        (Binding::IriMatch { iri: a, .. }, Binding::Iri(b)) => a.as_ref().cmp(b.as_ref()),
        (Binding::Iri(a), Binding::IriMatch { iri: b, .. }) => a.as_ref().cmp(b.as_ref()),
        // Encoded IRI types sort after decoded types when mixed
        (
            Binding::Sid(_) | Binding::IriMatch { .. } | Binding::Iri(_),
            Binding::EncodedSid { .. } | Binding::EncodedPid { .. },
        ) => Ordering::Less,
        (
            Binding::EncodedSid { .. } | Binding::EncodedPid { .. },
            Binding::Sid(_) | Binding::IriMatch { .. } | Binding::Iri(_),
        ) => Ordering::Greater,
        // EncodedSid vs EncodedPid: compare by ID (they're in same class)
        (Binding::EncodedSid { s_id }, Binding::EncodedPid { p_id }) => s_id.cmp(&(*p_id as u64)),
        (Binding::EncodedPid { p_id }, Binding::EncodedSid { s_id }) => (*p_id as u64).cmp(s_id),

        // Within Lit types: compare by value
        (Binding::Lit { val: v1, .. }, Binding::Lit { val: v2, .. }) => {
            compare_flake_values(v1, v2)
        }
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
            // - `i_val` is list-index metadata and must not affect value ordering.
            // - `p_id` is only relevant for NUM_BIG (per-predicate arena); for other kinds
            //   it must not affect ordering semantics.
            let base = k1
                .cmp(k2)
                .then(ok1.cmp(ok2))
                .then(dt1.cmp(dt2))
                .then(l1.cmp(l2));

            if base != Ordering::Equal {
                base
            } else if *k1 == fluree_db_core::ObjKind::NUM_BIG.as_u8() {
                p1.cmp(p2)
            } else {
                Ordering::Equal
            }
        }
        // Cross-Lit type: Lit sorts before EncodedLit when mixed
        (Binding::Lit { .. }, Binding::EncodedLit { .. }) => Ordering::Less,
        (Binding::EncodedLit { .. }, Binding::Lit { .. }) => Ordering::Greater,
    }
}

/// Compare two Sids for sorting
fn compare_sids(a: &Sid, b: &Sid) -> Ordering {
    match a.namespace_code.cmp(&b.namespace_code) {
        Ordering::Equal => a.name.cmp(&b.name),
        ord => ord,
    }
}

/// Compare two FlakeValues for sorting
///
/// Delegates to `FlakeValue::cmp` which implements total ordering with:
/// - Numeric class comparison (Long, Double, BigInt, Decimal compared by value)
/// - Temporal class comparison (DateTime, Date, Time compared by instant)
/// - Cross-type ordering by type discriminant
///
/// This ensures sort ordering is consistent with core's canonical ordering.
#[inline]
pub fn compare_flake_values(a: &FlakeValue, b: &FlakeValue) -> Ordering {
    a.cmp(b)
}

/// Sort operator - orders rows by specified columns
///
/// This is a **blocking operator** that must buffer all input before
/// producing any output. Memory usage is proportional to total row count.
///
/// Buffers all input rows, sorts them by the given `SortSpec` columns, then emits in sorted order.
pub struct SortOperator {
    /// Child operator
    child: BoxedOperator,
    /// Sort specifications
    sort_specs: Vec<SortSpec>,
    /// Output schema (same as child)
    in_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Buffered rows (collected during first next_batch call)
    buffer: Option<Vec<Vec<Binding>>>,
    /// Optional top-k optimization: keep only the first k rows after ordering.
    ///
    /// This reduces sort cost from O(n log n) to ~O(n + k log k), but still
    /// requires draining the full child stream (ORDER BY is blocking).
    topk: Option<usize>,
    /// Experiment: assume string IDs are lex-order preserving for ORDER BY.
    assume_lex_sorted_string_ids: bool,
    /// Current emit position in sorted buffer
    emit_idx: usize,
    /// Column indices for sort keys (resolved from schema)
    sort_col_indices: Vec<usize>,
    /// Variables required by downstream operators; if set, emitted output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

#[derive(Clone)]
struct HeapKeyPart {
    val: Binding,
    direction: SortDirection,
}

#[derive(Clone)]
struct HeapRow {
    key: Vec<HeapKeyPart>,
    row: Vec<Binding>,
}

impl HeapRow {
    fn from_row(row: Vec<Binding>, sort_col_indices: &[usize], sort_specs: &[SortSpec]) -> Self {
        let mut key: Vec<HeapKeyPart> = Vec::with_capacity(sort_col_indices.len());
        for (i, &col_idx) in sort_col_indices.iter().enumerate() {
            if let Some(v) = row.get(col_idx) {
                key.push(HeapKeyPart {
                    val: v.clone(),
                    direction: sort_specs
                        .get(i)
                        .map(|s| s.direction)
                        .unwrap_or(SortDirection::Ascending),
                });
            }
        }
        Self { key, row }
    }

    #[inline]
    fn cmp_key(&self, other: &Self) -> Ordering {
        for (a, b) in self.key.iter().zip(other.key.iter()) {
            let ord = compare_bindings(&a.val, &b.val);
            if ord != Ordering::Equal {
                return match a.direction {
                    SortDirection::Ascending => ord,
                    SortDirection::Descending => ord.reverse(),
                };
            }
        }
        Ordering::Equal
    }
}

impl PartialEq for HeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp_key(other) == Ordering::Equal
    }
}
impl Eq for HeapRow {}

impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_key(other)
    }
}

impl SortOperator {
    /// Create a new sort operator
    ///
    /// # Arguments
    ///
    /// * `child` - The child operator to sort
    /// * `sort_specs` - Sort specifications (columns and directions)
    ///
    /// # Note
    ///
    /// Sort variables must exist in the child's schema. Variables not found
    /// in the schema will be ignored during sorting.
    pub fn new(child: BoxedOperator, sort_specs: Vec<SortSpec>) -> Self {
        let schema: Arc<[VarId]> = Arc::from(child.schema().to_vec().into_boxed_slice());

        // Resolve sort column indices from schema
        let sort_col_indices: Vec<usize> = sort_specs
            .iter()
            .filter_map(|spec| schema.iter().position(|&v| v == spec.var))
            .collect();

        Self {
            child,
            sort_specs,
            in_schema: schema,
            state: OperatorState::Created,
            buffer: None,
            emit_idx: 0,
            sort_col_indices,
            out_schema: None,
            topk: None,
            assume_lex_sorted_string_ids: false,
        }
    }

    /// Create a sort operator with a top-k optimization.
    ///
    /// This is safe only when downstream semantics don't depend on rows beyond k
    /// (e.g., ORDER BY ... LIMIT k without DISTINCT/OFFSET applied after sort).
    pub fn new_topk(child: BoxedOperator, sort_specs: Vec<SortSpec>, k: usize) -> Self {
        let mut op = Self::new(child, sort_specs);
        op.topk = Some(k);
        op
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Get the sort specifications
    pub fn sort_specs(&self) -> &[SortSpec] {
        &self.sort_specs
    }

    /// Compare two rows by sort specifications
    fn compare_rows(&self, a: &[Binding], b: &[Binding]) -> Ordering {
        for (i, &col_idx) in self.sort_col_indices.iter().enumerate() {
            let ordering = compare_bindings(&a[col_idx], &b[col_idx]);

            if ordering != Ordering::Equal {
                return match self.sort_specs[i].direction {
                    SortDirection::Ascending => ordering,
                    SortDirection::Descending => ordering.reverse(),
                };
            }
        }
        Ordering::Equal
    }
}

#[async_trait]
impl Operator for SortOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(crate::error::QueryError::OperatorClosed);
            }
            return Err(crate::error::QueryError::OperatorAlreadyOpened);
        }

        self.child.open(ctx).await?;
        self.buffer = None;
        self.emit_idx = 0;
        self.assume_lex_sorted_string_ids = ctx
            .graph_view()
            .map(|gv| gv.store().lex_sorted_string_ids())
            .unwrap_or(false);
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(crate::error::QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        // First call: buffer all input and sort
        if self.buffer.is_none() {
            let span = tracing::debug_span!(
                "sort_blocking",
                sort_keys = self.sort_col_indices.len(),
                schema_cols = self.in_schema.len(),
                topk = self.topk.unwrap_or(0),
                input_batches = tracing::field::Empty,
                input_rows = tracing::field::Empty,
                drain_ms = tracing::field::Empty,
                sort_ms = tracing::field::Empty,
                child_next_ms = tracing::field::Empty,
                build_rows_ms = tracing::field::Empty
            );
            // Use an async block with .instrument() so the span is NOT held
            // across .await via a thread-local guard (which would cause cross-request
            // trace contamination in tokio's multi-threaded runtime).
            let buffer = async {
                let span = tracing::Span::current();
                let use_streaming_topk = self.topk.is_some();
                let mut all_rows: Vec<Vec<Binding>> = Vec::new();
                let mut heap: Option<BinaryHeap<HeapRow>> = self.topk.map(|k| {
                    // ORDER BY ... LIMIT 0 => empty result.
                    if k == 0 {
                        BinaryHeap::new()
                    } else {
                        BinaryHeap::with_capacity(k + 1)
                    }
                });

                // Drain child operator
                let drain_start = Instant::now();
                let mut input_batches: u64 = 0;
                let mut input_rows: u64 = 0;
                let mut child_next_ms: u64 = 0;
                let mut build_rows_ms: u64 = 0;
                let k = self.topk.unwrap_or(0);

                // Build novelty-aware graph view once for the entire drain loop.
                let cached_gv = ctx.graph_view();

                loop {
                    let next_start = Instant::now();
                    let next = self
                        .child
                        .next_batch(ctx)
                        .instrument(tracing::trace_span!("sort_child_next_batch"))
                        .await?;
                    child_next_ms += (next_start.elapsed().as_secs_f64() * 1000.0) as u64;
                    let Some(batch) = next else {
                        break;
                    };

                    input_batches += 1;
                    let build_span =
                        tracing::trace_span!("sort_build_rows_batch", rows = batch.len());
                    let build_start = Instant::now();
                    let _bg = build_span.enter();
                    for row_idx in 0..batch.len() {
                        input_rows += 1;
                        let row: Vec<Binding> = (0..self.in_schema.len())
                            .map(|col| batch.get_by_col(row_idx, col).clone())
                            .collect();
                        if use_streaming_topk {
                            if k == 0 {
                                continue;
                            }
                            let mut row = row;
                            if let Some(ref gv) = cached_gv {
                                materialize_sort_keys_in_row(
                                    &mut row,
                                    &self.sort_col_indices,
                                    gv,
                                    self.assume_lex_sorted_string_ids,
                                );
                            }
                            let candidate =
                                HeapRow::from_row(row, &self.sort_col_indices, &self.sort_specs);
                            let h = heap.as_mut().expect("heap initialized when topk set");
                            if h.len() < k {
                                h.push(candidate);
                            } else if let Some(worst) = h.peek() {
                                // BinaryHeap is a max-heap; peek() is the worst row (last by ordering).
                                if candidate.cmp_key(worst) == Ordering::Less {
                                    let _ = h.pop();
                                    h.push(candidate);
                                }
                            }
                        } else {
                            all_rows.push(row);
                        }
                    }
                    build_rows_ms += (build_start.elapsed().as_secs_f64() * 1000.0) as u64;
                }
                let drain_ms = (drain_start.elapsed().as_secs_f64() * 1000.0) as u64;

                // Sort rows
                // Late materialization hook:
                // If the batched-join path produced EncodedLit bindings, we must
                // materialize ONLY the ORDER BY key columns before sorting.
                let sort_execute_span = tracing::debug_span!(
                    "sort_execute",
                    total_rows = if use_streaming_topk {
                        heap.as_ref()
                            .map(std::collections::BinaryHeap::len)
                            .unwrap_or(0)
                    } else {
                        all_rows.len()
                    },
                    sort_columns = self.sort_col_indices.len(),
                    materialize = ctx.graph_view().is_some(),
                );
                let _sort_exec_guard = sort_execute_span.enter();
                if !use_streaming_topk {
                    if let Some(ref gv) = cached_gv {
                        materialize_sort_keys_in_rows(
                            &mut all_rows,
                            &self.sort_col_indices,
                            gv,
                            self.assume_lex_sorted_string_ids,
                        );
                    }
                    if let Some(k) = self.topk {
                        if k == 0 {
                            all_rows.clear();
                        } else if all_rows.len() > k {
                            // Keep only the first k rows by ordering.
                            let nth = k - 1;
                            all_rows.select_nth_unstable_by(nth, |a, b| self.compare_rows(a, b));
                            all_rows.truncate(k);
                        }
                    }
                }
                let sort_start = Instant::now();
                let out_rows: Vec<Vec<Binding>> = if use_streaming_topk {
                    let mut rows: Vec<Vec<Binding>> = heap
                        .take()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|r| r.row)
                        .collect();
                    rows.sort_by(|a, b| self.compare_rows(a, b));
                    rows
                } else {
                    all_rows.sort_by(|a, b| self.compare_rows(a, b));
                    all_rows
                };
                let sort_ms = (sort_start.elapsed().as_secs_f64() * 1000.0) as u64;
                drop(_sort_exec_guard);

                span.record("input_batches", input_batches);
                span.record("input_rows", input_rows);
                span.record("drain_ms", drain_ms);
                span.record("sort_ms", sort_ms);
                span.record("child_next_ms", child_next_ms);
                span.record("build_rows_ms", build_rows_ms);
                let total_ms = (drain_start.elapsed().as_secs_f64() * 1000.0) as u64;
                if input_rows >= SORT_DEBUG_MIN_ROWS || total_ms >= SORT_DEBUG_MIN_MS {
                    tracing::debug!(
                        input_batches,
                        input_rows,
                        output_rows = out_rows.len(),
                        use_streaming_topk,
                        topk = self.topk.unwrap_or(0),
                        total_ms,
                        drain_ms,
                        sort_ms,
                        child_next_ms,
                        build_rows_ms,
                        "sort blocking summary"
                    );
                } else {
                    tracing::trace!(
                        input_batches,
                        input_rows,
                        output_rows = out_rows.len(),
                        use_streaming_topk,
                        topk = self.topk.unwrap_or(0),
                        total_ms,
                        drain_ms,
                        sort_ms,
                        child_next_ms,
                        build_rows_ms,
                        "sort blocking summary"
                    );
                }

                Ok::<_, crate::error::QueryError>(out_rows)
            }
            .instrument(span)
            .await?;

            self.buffer = Some(buffer);
        }

        let rows = self.buffer.as_ref().unwrap();

        if self.emit_idx >= rows.len() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        // Emit up to batch_size rows
        let batch_size = ctx.batch_size;
        let end_idx = (self.emit_idx + batch_size).min(rows.len());

        let num_cols = self.in_schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(end_idx - self.emit_idx))
            .collect();

        for row in &rows[self.emit_idx..end_idx] {
            for (col_idx, val) in row.iter().enumerate() {
                columns[col_idx].push(val.clone());
            }
        }

        self.emit_idx = end_idx;

        let batch = Batch::new(self.in_schema.clone(), columns)?;
        Ok(trim_batch(&self.out_schema, batch))
    }

    fn close(&mut self) {
        self.child.close();
        self.buffer = None; // Release memory
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Sort doesn't change row count
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::QueryError;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::LedgerSnapshot;

    /// Mock operator that emits predefined batches
    struct MockOperator {
        batches: Vec<Batch>,
        idx: usize,
        schema: Arc<[VarId]>,
        state: OperatorState,
    }

    impl MockOperator {
        fn new(batches: Vec<Batch>) -> Self {
            let schema = batches
                .first()
                .map(|b| Arc::from(b.schema().to_vec().into_boxed_slice()))
                .unwrap_or_else(|| Arc::from(Vec::new().into_boxed_slice()));
            Self {
                batches,
                idx: 0,
                schema,
                state: OperatorState::Created,
            }
        }
    }

    #[async_trait]
    impl Operator for MockOperator {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }

        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            self.idx = 0;
            self.state = OperatorState::Open;
            Ok(())
        }

        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            if self.state != OperatorState::Open {
                return Ok(None);
            }

            if self.idx < self.batches.len() {
                let batch = self.batches[self.idx].clone();
                self.idx += 1;
                Ok(Some(batch))
            } else {
                self.state = OperatorState::Exhausted;
                Ok(None)
            }
        }

        fn close(&mut self) {
            self.state = OperatorState::Closed;
        }

        fn estimated_rows(&self) -> Option<usize> {
            Some(
                self.batches
                    .iter()
                    .map(super::super::binding::Batch::len)
                    .sum(),
            )
        }
    }

    fn make_batch_with_values(schema: Arc<[VarId]>, values: Vec<i64>) -> Batch {
        let columns = vec![values
            .into_iter()
            .map(|v| Binding::lit(FlakeValue::Long(v), Sid::new(1, "long")))
            .collect()];
        Batch::new(schema, columns).unwrap()
    }

    fn make_batch_2col(schema: Arc<[VarId]>, col1: Vec<i64>, col2: Vec<i64>) -> Batch {
        assert_eq!(col1.len(), col2.len());
        let columns = vec![
            col1.into_iter()
                .map(|v| Binding::lit(FlakeValue::Long(v), Sid::new(1, "long")))
                .collect(),
            col2.into_iter()
                .map(|v| Binding::lit(FlakeValue::Long(v), Sid::new(1, "long")))
                .collect(),
        ];
        Batch::new(schema, columns).unwrap()
    }

    fn extract_long_values(batch: &Batch, col: usize) -> Vec<i64> {
        (0..batch.len())
            .filter_map(|i| {
                if let Binding::Lit {
                    val: FlakeValue::Long(v),
                    ..
                } = batch.get_by_col(i, col)
                {
                    Some(*v)
                } else {
                    None
                }
            })
            .collect()
    }

    // ===== compare_bindings tests =====

    #[test]
    fn test_compare_bindings_unbound() {
        assert_eq!(
            compare_bindings(&Binding::Unbound, &Binding::Unbound),
            Ordering::Equal
        );
        assert_eq!(
            compare_bindings(
                &Binding::Unbound,
                &Binding::lit(FlakeValue::Long(1), Sid::new(1, "long"))
            ),
            Ordering::Less
        );
        assert_eq!(
            compare_bindings(
                &Binding::lit(FlakeValue::Long(1), Sid::new(1, "long")),
                &Binding::Unbound
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_bindings_poisoned() {
        assert_eq!(
            compare_bindings(&Binding::Poisoned, &Binding::Poisoned),
            Ordering::Equal
        );
        assert_eq!(
            compare_bindings(&Binding::Poisoned, &Binding::Unbound),
            Ordering::Greater
        );
        assert_eq!(
            compare_bindings(&Binding::Unbound, &Binding::Poisoned),
            Ordering::Less
        );
        assert_eq!(
            compare_bindings(
                &Binding::Poisoned,
                &Binding::lit(FlakeValue::Long(1), Sid::new(1, "long"))
            ),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_bindings_sid() {
        let sid1 = Binding::Sid(Sid::new(1, "apple"));
        let sid2 = Binding::Sid(Sid::new(1, "banana"));
        let sid3 = Binding::Sid(Sid::new(2, "apple"));

        assert_eq!(compare_bindings(&sid1, &sid1), Ordering::Equal);
        assert_eq!(compare_bindings(&sid1, &sid2), Ordering::Less); // apple < banana
        assert_eq!(compare_bindings(&sid1, &sid3), Ordering::Less); // ns 1 < ns 2
    }

    #[test]
    fn test_compare_bindings_long() {
        let a = Binding::lit(FlakeValue::Long(1), Sid::new(1, "long"));
        let b = Binding::lit(FlakeValue::Long(2), Sid::new(1, "long"));

        assert_eq!(compare_bindings(&a, &a), Ordering::Equal);
        assert_eq!(compare_bindings(&a, &b), Ordering::Less);
        assert_eq!(compare_bindings(&b, &a), Ordering::Greater);
    }

    #[test]
    fn test_compare_bindings_double() {
        let a = Binding::lit(FlakeValue::Double(1.5), Sid::new(1, "double"));
        let b = Binding::lit(FlakeValue::Double(2.5), Sid::new(1, "double"));

        assert_eq!(compare_bindings(&a, &a), Ordering::Equal);
        assert_eq!(compare_bindings(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_compare_bindings_nan() {
        let nan = Binding::lit(FlakeValue::Double(f64::NAN), Sid::new(1, "double"));
        let num = Binding::lit(FlakeValue::Double(1.0), Sid::new(1, "double"));

        // NaN sorts last
        assert_eq!(compare_bindings(&nan, &num), Ordering::Greater);
        assert_eq!(compare_bindings(&num, &nan), Ordering::Less);
        assert_eq!(compare_bindings(&nan, &nan), Ordering::Equal);
    }

    #[test]
    fn test_compare_bindings_numeric_promotion() {
        let long_1 = Binding::lit(FlakeValue::Long(1), Sid::new(1, "long"));
        let double_1 = Binding::lit(FlakeValue::Double(1.0), Sid::new(1, "double"));
        let double_2 = Binding::lit(FlakeValue::Double(2.0), Sid::new(1, "double"));

        assert_eq!(compare_bindings(&long_1, &double_1), Ordering::Equal);
        assert_eq!(compare_bindings(&long_1, &double_2), Ordering::Less);
    }

    #[test]
    fn test_compare_bindings_string() {
        let a = Binding::lit(
            FlakeValue::String("apple".to_string()),
            Sid::new(1, "string"),
        );
        let b = Binding::lit(
            FlakeValue::String("banana".to_string()),
            Sid::new(1, "string"),
        );

        assert_eq!(compare_bindings(&a, &a), Ordering::Equal);
        assert_eq!(compare_bindings(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_compare_bindings_cross_type() {
        // Type order: Null < Boolean < Long < Double < String < Ref
        let null = Binding::lit(FlakeValue::Null, Sid::new(1, "null"));
        let bool_val = Binding::lit(FlakeValue::Boolean(true), Sid::new(1, "bool"));
        let long_val = Binding::lit(FlakeValue::Long(1), Sid::new(1, "long"));
        let string_val = Binding::lit(FlakeValue::String("x".to_string()), Sid::new(1, "string"));

        assert_eq!(compare_bindings(&null, &bool_val), Ordering::Less);
        assert_eq!(compare_bindings(&bool_val, &long_val), Ordering::Less);
        assert_eq!(compare_bindings(&long_val, &string_val), Ordering::Less);
    }

    // ===== SortOperator tests =====

    #[tokio::test]
    async fn test_sort_single_column_asc() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_batch_with_values(schema.clone(), vec![3, 1, 4, 1, 5, 9, 2]);
        let mock = MockOperator::new(vec![batch]);

        let mut sort_op = SortOperator::new(Box::new(mock), vec![SortSpec::asc(VarId(0))]);
        sort_op.open(&ctx).await.unwrap();

        let result = sort_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        let batch = result.unwrap();

        let values = extract_long_values(&batch, 0);
        assert_eq!(values, vec![1, 1, 2, 3, 4, 5, 9]);
    }

    #[tokio::test]
    async fn test_sort_single_column_desc() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_batch_with_values(schema.clone(), vec![3, 1, 4, 1, 5]);
        let mock = MockOperator::new(vec![batch]);

        let mut sort_op = SortOperator::new(Box::new(mock), vec![SortSpec::desc(VarId(0))]);
        sort_op.open(&ctx).await.unwrap();

        let result = sort_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        let batch = result.unwrap();

        let values = extract_long_values(&batch, 0);
        assert_eq!(values, vec![5, 4, 3, 1, 1]);
    }

    #[tokio::test]
    async fn test_sort_multi_column() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        // Sort by col0 asc, col1 desc
        // (2, 1), (1, 3), (1, 1), (2, 2) => (1, 3), (1, 1), (2, 2), (2, 1)
        let batch = make_batch_2col(schema.clone(), vec![2, 1, 1, 2], vec![1, 3, 1, 2]);
        let mock = MockOperator::new(vec![batch]);

        let mut sort_op = SortOperator::new(
            Box::new(mock),
            vec![SortSpec::asc(VarId(0)), SortSpec::desc(VarId(1))],
        );
        sort_op.open(&ctx).await.unwrap();

        let result = sort_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        let batch = result.unwrap();

        let col0 = extract_long_values(&batch, 0);
        let col1 = extract_long_values(&batch, 1);
        assert_eq!(col0, vec![1, 1, 2, 2]);
        assert_eq!(col1, vec![3, 1, 2, 1]);
    }

    #[tokio::test]
    async fn test_sort_with_unbound() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let columns = vec![vec![
            Binding::lit(FlakeValue::Long(3), Sid::new(1, "long")),
            Binding::Unbound,
            Binding::lit(FlakeValue::Long(1), Sid::new(1, "long")),
            Binding::Unbound,
        ]];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let mock = MockOperator::new(vec![batch]);

        let mut sort_op = SortOperator::new(Box::new(mock), vec![SortSpec::asc(VarId(0))]);
        sort_op.open(&ctx).await.unwrap();

        let result = sort_op.next_batch(&ctx).await.unwrap().unwrap();

        // Unbound sorts first
        assert!(matches!(result.get_by_col(0, 0), Binding::Unbound));
        assert!(matches!(result.get_by_col(1, 0), Binding::Unbound));
        // Then numbers
        assert!(matches!(
            result.get_by_col(2, 0),
            Binding::Lit {
                val: FlakeValue::Long(1),
                ..
            }
        ));
        assert!(matches!(
            result.get_by_col(3, 0),
            Binding::Lit {
                val: FlakeValue::Long(3),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_sort_across_batches() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch1 = make_batch_with_values(schema.clone(), vec![5, 3, 1]);
        let batch2 = make_batch_with_values(schema.clone(), vec![4, 2]);
        let mock = MockOperator::new(vec![batch1, batch2]);

        let mut sort_op = SortOperator::new(Box::new(mock), vec![SortSpec::asc(VarId(0))]);
        sort_op.open(&ctx).await.unwrap();

        let result = sort_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        let batch = result.unwrap();

        let values = extract_long_values(&batch, 0);
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_sort_emits_in_batches() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        // Use small batch size
        let ctx = ExecutionContext::new(&snapshot, &vars).with_batch_size(3);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_batch_with_values(schema.clone(), vec![5, 4, 3, 2, 1, 0]);
        let mock = MockOperator::new(vec![batch]);

        let mut sort_op = SortOperator::new(Box::new(mock), vec![SortSpec::asc(VarId(0))]);
        sort_op.open(&ctx).await.unwrap();

        // First batch: 3 rows
        let result1 = sort_op.next_batch(&ctx).await.unwrap();
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().len(), 3);

        // Second batch: 3 rows
        let result2 = sort_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().len(), 3);

        // Exhausted
        let result3 = sort_op.next_batch(&ctx).await.unwrap();
        assert!(result3.is_none());
    }

    #[tokio::test]
    async fn test_sort_empty_input() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let mock = MockOperator::new(vec![]);

        let mut sort_op = SortOperator::new(Box::new(mock), vec![SortSpec::asc(VarId(0))]);
        sort_op.open(&ctx).await.unwrap();

        let result = sort_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_sort_preserves_schema() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns: Vec<Vec<Binding>> = (0..3)
            .map(|col| {
                (0..5)
                    .map(|row| {
                        Binding::lit(
                            FlakeValue::Long((col * 10 + row) as i64),
                            Sid::new(1, "long"),
                        )
                    })
                    .collect()
            })
            .collect();
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let mock = MockOperator::new(vec![batch]);

        let mut sort_op = SortOperator::new(Box::new(mock), vec![SortSpec::asc(VarId(0))]);
        assert_eq!(sort_op.schema(), &[VarId(0), VarId(1), VarId(2)]);

        sort_op.open(&ctx).await.unwrap();

        let result = sort_op.next_batch(&ctx).await.unwrap().unwrap();
        assert_eq!(result.schema(), &[VarId(0), VarId(1), VarId(2)]);
    }

    #[tokio::test]
    async fn test_sort_state_transitions() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_batch_with_values(schema.clone(), vec![3, 1, 2]);
        let mock = MockOperator::new(vec![batch]);

        let mut sort_op = SortOperator::new(Box::new(mock), vec![SortSpec::asc(VarId(0))]);

        // Can't call next_batch before open
        let err = sort_op.next_batch(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorNotOpened)));

        // Open successfully
        sort_op.open(&ctx).await.unwrap();

        // Can't open twice
        let err = sort_op.open(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorAlreadyOpened)));

        // Get result
        let _ = sort_op.next_batch(&ctx).await.unwrap();

        // Close
        sort_op.close();

        // Can't open after close
        let err = sort_op.open(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorClosed)));
    }
}
