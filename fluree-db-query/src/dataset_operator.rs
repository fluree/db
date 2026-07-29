//! Dataset operator — fans triple-pattern evaluation across multiple graphs.
//!
//! `DatasetOperator` implements the [`Operator`] trait and wraps one inner
//! operator per active graph (from a SPARQL FROM / FROM NAMED dataset). It
//! drives their lifecycle (`open`/`next_batch`/`close`), merges results, and
//! stamps ledger provenance (`Binding::IriMatch`) when results span multiple
//! ledgers.
//!
//! A [`DatasetBuilder`] trait (factory pattern) separates *how* to build
//! per-graph operators from *when* they are built. The planner constructs a
//! builder at plan time; `DatasetOperator` calls it at execution time during
//! [`Operator::open`].
//!
//! # Nested composition
//!
//! Because `DatasetBuilder::build()` returns [`BoxedOperator`], the inner
//! operator can be anything — including another `DatasetOperator`. Provenance
//! stamping passes `Binding::IriMatch` through unchanged, so nested datasets
//! compose correctly.
//!
//! See `docs/design/query-execution.md` for the pipeline overview.

use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_core::{IndexType, ObjectBounds, Sid};

use crate::binary_history::BinaryHistoryScanOperator;
use crate::binary_scan::{schema_from_pattern_with_emit, BinaryScanOperator, EmitMask};
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::ir::triple::TriplePattern;
use crate::operator::inline::{extend_schema, InlineOperator};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::temporal_mode::TemporalMode;
use crate::var_registry::VarId;

// =============================================================================
// DatasetBuilder trait
// =============================================================================

/// Factory for building per-graph operators at execution time.
///
/// Constructed by the planner at plan time with all the parameters needed to
/// create the inner operator. Stateless — [`build`](DatasetBuilder::build) is
/// called once per active graph during [`DatasetOperator::open`] and each call
/// produces an independent operator.
pub trait DatasetBuilder: Send + Sync {
    /// Build an operator for a single graph.
    ///
    /// The returned operator will be opened with a per-graph
    /// [`ExecutionContext`] (via `ctx.with_graph_ref()`).
    fn build(&self) -> Result<BoxedOperator>;

    /// Output schema. Must be stable across all `build()` calls.
    fn schema(&self) -> &[VarId];

    /// Plan-introspection details for `EXPLAIN` (e.g. predicate, planned index
    /// hint). Default: none. Scan builders override to expose the access path.
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        serde_json::Map::new()
    }
}

// =============================================================================
// ScanDatasetBuilder
// =============================================================================

/// Builder for triple-pattern scans across dataset graphs.
///
/// Produces a mode-specific scan operator for each graph:
/// - [`TemporalMode::Current`] → [`BinaryScanOperator`] directly.
/// - [`TemporalMode::History`] → [`BinaryHistoryScanOperator`].
///
/// Mode is captured at planner-time construction, not read from the runtime
/// `ExecutionContext` — every operator in the tree is single-purpose.
pub struct ScanDatasetBuilder {
    pattern: TriplePattern,
    object_bounds: Option<ObjectBounds>,
    inline_ops: Vec<InlineOperator>,
    emit: EmitMask,
    index_hint: Option<IndexType>,
    mode: TemporalMode,
    schema: Arc<[VarId]>,
}

impl ScanDatasetBuilder {
    pub fn new(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
        emit: EmitMask,
        index_hint: Option<IndexType>,
        mode: TemporalMode,
    ) -> Self {
        let (base_schema, _, _, _) = schema_from_pattern_with_emit(&pattern, emit);
        let schema: Arc<[VarId]> = extend_schema(&base_schema, &inline_ops).into();
        Self {
            pattern,
            object_bounds,
            inline_ops,
            emit,
            index_hint,
            mode,
            schema,
        }
    }

    /// Returns the temporal mode this builder will use when constructing scans.
    #[inline]
    pub fn mode(&self) -> TemporalMode {
        self.mode
    }
}

impl DatasetBuilder for ScanDatasetBuilder {
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert(
            "pattern".into(),
            crate::explain::format_pattern(&self.pattern).into(),
        );
        if let Some(idx) = self.index_hint {
            m.insert("index-hint".into(), format!("{idx:?}").into());
        }
        if self.object_bounds.is_some() {
            m.insert("object-bounds".into(), true.into());
        }
        m
    }

    fn build(&self) -> Result<BoxedOperator> {
        match self.mode {
            TemporalMode::History => Ok(Box::new(
                BinaryHistoryScanOperator::new_with_emit_and_index(
                    self.pattern.clone(),
                    self.object_bounds.clone(),
                    self.inline_ops.clone(),
                    self.emit,
                    self.index_hint,
                ),
            )),
            TemporalMode::Current => Ok(Box::new(BinaryScanOperator::new_with_emit_and_index(
                self.pattern.clone(),
                self.object_bounds.clone(),
                self.inline_ops.clone(),
                self.emit,
                self.index_hint,
            ))),
        }
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }
}

// =============================================================================
// DatasetOperator
// =============================================================================

/// Per-graph inner operator with its provenance metadata.
struct DatasetMember {
    operator: BoxedOperator,
    ledger_id: Arc<str>,
}

/// Operator that fans triple-pattern evaluation across multiple graphs.
///
/// During [`open`](Operator::open), builds one inner operator per active graph
/// (via the [`DatasetBuilder`] factory) and opens each with a per-graph
/// [`ExecutionContext`]. During [`next_batch`](Operator::next_batch), drains
/// members in sequence and stamps ledger provenance when results span multiple
/// ledgers.
pub struct DatasetOperator {
    builder: Box<dyn DatasetBuilder>,
    state: OperatorState,
    /// Per-graph inner operators, indexed in the same order as
    /// `ctx.active_graphs()` returns graphs.
    members: Vec<DatasetMember>,
    /// Index of the member currently being drained.
    current_member: usize,
    /// True when members span multiple distinct ledger IDs, requiring
    /// `Binding::Sid` → `Binding::IriMatch` conversion.
    needs_provenance: bool,
    /// T1.3: a top-of-tree `LIMIT` row budget, recorded by `set_row_budget` and
    /// threaded into each member's inner subtree at build time in `open` (mirrors
    /// `GraphOperator`, which builds its inner subplan the same way). Without this
    /// the dataset wrapper swallowed the budget and a `LIMIT` never reached the
    /// R2RML scan. `None` unless the switch is on AND a budget was pushed.
    row_budget: Option<usize>,
    /// T1.3: an `ORDER BY … LIMIT` top-k directive, applied per member like
    /// `row_budget`. Per-member top-k is sound — the outer sort merges the members'
    /// partial top-k into the global one.
    topk: Option<(VarId, usize)>,
}

impl DatasetOperator {
    /// Create a new dataset operator driven by the given builder.
    pub fn new(builder: Box<dyn DatasetBuilder>) -> Self {
        Self {
            builder,
            state: OperatorState::Created,
            members: Vec::new(),
            current_member: 0,
            needs_provenance: false,
            row_budget: None,
            topk: None,
        }
    }

    /// Thread a recorded top-of-tree `LIMIT` budget / top-k directive into a
    /// freshly built member's inner subtree, before it is opened. No-op unless
    /// `set_row_budget`/`set_topk` recorded one (which only happens when the
    /// `FLUREE_R2RML_DATASET_BUDGET` switch is on). Each member's own operators
    /// forward or absorb the directive exactly as they would below a
    /// `GraphOperator`, so no soundness reasoning is duplicated here.
    fn apply_member_directives(&self, member: &mut dyn Operator) {
        if let Some(budget) = self.row_budget {
            member.set_row_budget(budget);
        }
        if let Some((sort_var, k)) = self.topk {
            member.set_topk(sort_var, k);
        }
    }

    /// Convenience constructor for a triple-pattern scan wrapped in a
    /// dataset operator.
    ///
    /// `mode` must be captured by the caller at planner-time. Late/dynamic
    /// builders (joins, optionals, EXISTS/MINUS, etc.) capture mode at their
    /// own construction time and pass it through here — they do not read it
    /// from the runtime `ExecutionContext`.
    pub fn scan(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
        emit: EmitMask,
        index_hint: Option<IndexType>,
        mode: TemporalMode,
    ) -> Self {
        let builder =
            ScanDatasetBuilder::new(pattern, object_bounds, inline_ops, emit, index_hint, mode);
        Self::new(Box::new(builder))
    }
}

/// Convert `Binding::Sid` values in a batch to `Binding::IriMatch` for
/// cross-ledger provenance tracking.
///
/// - `Binding::Sid` → decoded via the ledger's namespace table, wrapped in
///   `IriMatch` with `ledger_id`. Returns an error if the SID cannot be
///   decoded, since multi-ledger equality requires `IriMatch` and a silent
///   fallback to `Binding::Sid` would break cross-ledger joins.
/// - `Binding::IriMatch` → passed through unchanged (supports nested
///   `DatasetOperator` composition).
/// - All other binding types → unchanged.
pub(crate) fn stamp_provenance(
    batch: Batch,
    ledger_id: &Arc<str>,
    ctx: &ExecutionContext<'_>,
) -> Result<Batch> {
    // Empty-schema batches (e.g. existence checks from count-only scans)
    // carry only a row count with no bindings to stamp. Return unchanged
    // so the row count is preserved.
    if batch.schema().is_empty() {
        return Ok(batch);
    }

    let (schema, columns, _len) = batch.into_parts();

    let stamped_columns: Vec<Vec<Binding>> = columns
        .into_iter()
        .map(|col| {
            col.into_iter()
                .map(|binding| stamp_binding(binding, ledger_id, ctx))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    Batch::new(schema, stamped_columns).map_err(|e| QueryError::Internal(e.to_string()))
}

/// Stamp a single binding with ledger provenance.
///
/// `Binding::Sid` is converted to `IriMatch`; all other variants are moved
/// through unchanged (no cloning).
///
/// # Errors
///
/// - Returns `QueryError::Internal` if a `Binding::Sid` cannot be decoded
///   to an IRI. Multi-ledger equality is defined around `IriMatch`, so a
///   silent fallback to `Binding::Sid` would break cross-ledger joins.
/// - Returns `QueryError::Internal` on `Binding::EncodedSid` or
///   `Binding::EncodedPid` — these late-materialized binary-cursor IDs
///   cannot be decoded without the store, which is disabled for
///   multi-ledger datasets during `open()`.
fn stamp_binding(
    binding: Binding,
    ledger_id: &Arc<str>,
    ctx: &ExecutionContext<'_>,
) -> Result<Binding> {
    match binding {
        Binding::Sid { ref sid, .. } => sid_to_iri_match(sid, ledger_id, ctx),
        Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => Err(QueryError::Internal(
            "EncodedSid/EncodedPid reached stamp_provenance — binary store should have \
                 been disabled for multi-ledger datasets"
                .into(),
        )),
        other => Ok(other),
    }
}

/// Convert a `Sid` to `IriMatch` using the dataset's decoding context.
///
/// # Errors
///
/// Returns `QueryError::Internal` if the SID's namespace code cannot be
/// resolved to an IRI prefix. This indicates either a snapshot that is
/// missing namespace deltas (e.g. from a staged transaction) or data
/// corruption — either way, silently falling back to `Binding::Sid` would
/// break multi-ledger equality semantics.
fn sid_to_iri_match(
    sid: &Sid,
    ledger_id: &Arc<str>,
    ctx: &ExecutionContext<'_>,
) -> Result<Binding> {
    let iri = ctx
        .decode_sid_in_ledger(sid, ledger_id.as_ref())
        .ok_or_else(|| {
            QueryError::Internal(format!(
                "failed to decode SID (ns={}, name={:?}) from ledger {:?}: \
             namespace code not found in snapshot — multi-ledger equality \
             requires IriMatch but the SID cannot be resolved to an IRI",
                sid.namespace_code, sid.name, ledger_id,
            ))
        })?;
    Ok(Binding::iri_match(
        Arc::<str>::from(iri.as_str()),
        sid.clone(),
        Arc::clone(ledger_id),
    ))
}

/// Count a single member to exhaustion, preferring its `drain_count`
/// (count-only, no binding materialization) and falling back to a streaming
/// `next_batch` row count when the member declines count-only mode.
async fn count_member(op: &mut BoxedOperator, ctx: &ExecutionContext<'_>) -> Result<u64> {
    if let Some(n) = op.drain_count(ctx).await? {
        return Ok(n);
    }
    let mut n: u64 = 0;
    while let Some(batch) = op.next_batch(ctx).await? {
        ctx.check_cancelled()?;
        n = n
            .checked_add(batch.len() as u64)
            .ok_or_else(|| QueryError::execution("COUNT(*) overflow in dataset drain_count"))?;
    }
    ctx.check_cancelled()?;
    Ok(n)
}

#[async_trait]
impl Operator for DatasetOperator {
    fn schema(&self) -> &[VarId] {
        self.builder.schema()
    }

    fn set_row_budget(&mut self, budget: usize) {
        // T1.3: record a top-of-tree LIMIT budget; threaded into each member's
        // inner subtree at build time (`open` → `apply_member_directives`), NOT
        // forwarded to a single child (there is none — members are built lazily
        // per active graph). Sound because the consuming LIMIT truncates the
        // member concatenation to `budget`, and each member's own Sort/Distinct
        // still absorb the budget (no-op) where present; this only removes the
        // artificial block the dataset wrapper imposed. Switch-gated for OFF-parity.
        if crate::r2rml::dataset_budget_enabled() {
            self.row_budget = Some(budget);
        }
    }

    fn set_topk(&mut self, sort_var: VarId, k: usize) {
        // T1.3: record ORDER BY … LIMIT top-k; applied per member like the row
        // budget. Per-member top-k is sound — the outer sort merges the members'
        // partial top-k into the global one (same reasoning as `GraphOperator`'s
        // per-partition top-k). Switch-gated.
        if crate::r2rml::dataset_budget_enabled() {
            self.topk = Some((sort_var, k));
        }
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        self.builder.plan_details()
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        match ctx.active_graphs() {
            ActiveGraphs::Single => {
                // Single-graph mode: build one operator, open with parent
                // context directly. No fanout, no provenance stamping.
                let mut inner = self.builder.build()?;
                self.apply_member_directives(inner.as_mut());
                inner.open(ctx).await?;
                self.members.push(DatasetMember {
                    operator: inner,
                    ledger_id: Arc::from(""),
                });
                self.needs_provenance = false;
            }
            ActiveGraphs::Many(graphs) => {
                // Pre-scan: determine whether graphs span multiple ledgers
                // *before* opening any operators so we can consistently
                // disable binary stores for all graphs when provenance
                // stamping is needed.
                // A single active graph can still belong to a multi-ledger
                // dataset (a default graph alongside named graphs from other
                // ledgers); its bindings may cross a boundary and be stamped, so
                // force materialization here too — not only when the active
                // graphs themselves span ledgers.
                let multi_ledger = graphs.windows(2).any(|w| w[0].ledger_id != w[1].ledger_id)
                    || ctx
                        .dataset
                        .as_ref()
                        .is_some_and(|d| d.spans_multiple_ledgers());
                self.needs_provenance = multi_ledger;

                for graph in &graphs {
                    let mut inner = self.builder.build()?;
                    self.apply_member_directives(inner.as_mut());
                    let mut per_graph_ctx = ctx.with_graph_ref(graph);

                    // When provenance stamping is needed (multi-ledger),
                    // force the range fallback path so inner scans produce
                    // `Binding::Sid` rather than `Binding::EncodedSid`.
                    // EncodedSid is a late-materialized binary-cursor ID
                    // that cannot be decoded to an IRI without the store,
                    // which is not available at stamp_provenance time.
                    if multi_ledger {
                        per_graph_ctx.binary_store = None;
                        per_graph_ctx.dict_novelty = None;
                        per_graph_ctx.runtime_small_dicts = None;
                    }

                    inner.open(&per_graph_ctx).await?;

                    self.members.push(DatasetMember {
                        operator: inner,
                        ledger_id: Arc::clone(&graph.ledger_id),
                    });
                }
            }
        }

        self.current_member = 0;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let graphs = ctx.active_graphs();

        debug_assert!(
            match &graphs {
                ActiveGraphs::Many(g) => g.len() == self.members.len(),
                ActiveGraphs::Single => self.members.len() == 1,
            },
            "active_graphs() returned a different number of graphs than open() saw"
        );

        while self.current_member < self.members.len() {
            let batch = {
                let member = &mut self.members[self.current_member];
                match &graphs {
                    ActiveGraphs::Many(g) => {
                        let graph_ctx = ctx.with_graph_ref(g[self.current_member]);
                        member.operator.next_batch(&graph_ctx).await?
                    }
                    ActiveGraphs::Single => member.operator.next_batch(ctx).await?,
                }
            };

            match batch {
                Some(ref b) if b.is_empty() => continue,
                Some(batch) => {
                    let result = if self.needs_provenance {
                        let ledger_id = &self.members[self.current_member].ledger_id;
                        stamp_provenance(batch, ledger_id, ctx)?
                    } else {
                        batch
                    };
                    return Ok(Some(result));
                }
                None => {
                    // This member is exhausted, move to next.
                    self.current_member += 1;
                }
            }
        }

        // All members exhausted.
        self.state = OperatorState::Exhausted;
        Ok(None)
    }

    /// Count-only drain across all member graphs.
    ///
    /// Counts are invariant under provenance stamping (it rewrites binding
    /// values, never row counts), so this skips stamping entirely and sums each
    /// member's count. Each member is counted independently via its own
    /// `drain_count` (or a streaming fallback), so mixed count-only support
    /// across graphs is fine. `COUNT(*)` over a multi-graph dataset is the bag
    /// union of per-graph row counts.
    async fn drain_count(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<u64>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let graphs = ctx.active_graphs();

        debug_assert!(
            match &graphs {
                ActiveGraphs::Many(g) => g.len() == self.members.len(),
                ActiveGraphs::Single => self.members.len() == 1,
            },
            "active_graphs() returned a different number of graphs than open() saw"
        );

        let mut total: u64 = 0;
        while self.current_member < self.members.len() {
            let n = match &graphs {
                ActiveGraphs::Many(g) => {
                    let graph_ctx = ctx.with_graph_ref(g[self.current_member]);
                    count_member(&mut self.members[self.current_member].operator, &graph_ctx)
                        .await?
                }
                ActiveGraphs::Single => {
                    count_member(&mut self.members[self.current_member].operator, ctx).await?
                }
            };
            total = total
                .checked_add(n)
                .ok_or_else(|| QueryError::execution("COUNT(*) overflow in dataset drain_count"))?;
            self.current_member += 1;
        }

        self.state = OperatorState::Exhausted;
        Ok(Some(total))
    }

    fn close(&mut self) {
        for member in &mut self.members {
            member.operator.close();
        }
        self.members.clear();
        self.current_member = 0;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::var_registry::VarId;

    /// Verify ScanDatasetBuilder produces operators with consistent schema.
    #[test]
    fn scan_dataset_builder_consistent_schema() {
        use crate::ir::triple::{Ref, Term};
        use fluree_db_core::Sid;

        let s = VarId(0);
        let o = VarId(1);
        let pattern =
            TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(100, "name")), Term::Var(o));

        let builder = ScanDatasetBuilder::new(
            pattern,
            None,
            Vec::new(),
            EmitMask::ALL,
            None,
            TemporalMode::Current,
        );

        let schema = builder.schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0], s);
        assert_eq!(schema[1], o);

        // Build two operators — schemas must match.
        let op1 = builder.build().unwrap();
        let op2 = builder.build().unwrap();
        assert_eq!(op1.schema(), op2.schema());
        assert_eq!(op1.schema(), builder.schema());
    }

    // ---- T1.3: DatasetOperator LIMIT / top-k forwarding ----

    use std::sync::Mutex;

    /// Inner operator that records the budget / top-k directive its dataset member
    /// receives, and yields no rows. Lets a test read what `open` threaded into a
    /// freshly built member.
    struct DirectiveRecorder {
        budget: Arc<Mutex<Option<usize>>>,
        topk: Arc<Mutex<Option<(VarId, usize)>>>,
        schema: Arc<[VarId]>,
        state: OperatorState,
    }

    #[async_trait]
    impl Operator for DirectiveRecorder {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }
        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            self.state = OperatorState::Open;
            Ok(())
        }
        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            self.state = OperatorState::Exhausted;
            Ok(None)
        }
        fn close(&mut self) {
            self.state = OperatorState::Closed;
        }
        fn set_row_budget(&mut self, budget: usize) {
            *self.budget.lock().unwrap() = Some(budget);
        }
        fn set_topk(&mut self, sort_var: VarId, k: usize) {
            *self.topk.lock().unwrap() = Some((sort_var, k));
        }
    }

    /// `DatasetBuilder` that yields a fresh `DirectiveRecorder` sharing the given
    /// handles, so a test can read what the built member(s) received.
    struct RecorderBuilder {
        budget: Arc<Mutex<Option<usize>>>,
        topk: Arc<Mutex<Option<(VarId, usize)>>>,
        schema: Arc<[VarId]>,
    }

    impl DatasetBuilder for RecorderBuilder {
        fn build(&self) -> Result<BoxedOperator> {
            Ok(Box::new(DirectiveRecorder {
                budget: Arc::clone(&self.budget),
                topk: Arc::clone(&self.topk),
                schema: Arc::clone(&self.schema),
                state: OperatorState::Created,
            }))
        }
        fn schema(&self) -> &[VarId] {
            &self.schema
        }
    }

    type RecorderHandles = (
        Arc<Mutex<Option<usize>>>,
        Arc<Mutex<Option<(VarId, usize)>>>,
    );

    fn recorder_dataset() -> (DatasetOperator, RecorderHandles) {
        let budget = Arc::new(Mutex::new(None));
        let topk = Arc::new(Mutex::new(None));
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let builder = RecorderBuilder {
            budget: Arc::clone(&budget),
            topk: Arc::clone(&topk),
            schema,
        };
        (DatasetOperator::new(Box::new(builder)), (budget, topk))
    }

    /// Open an operator against a trivial single-graph context and drain it, so the
    /// dataset member is built + opened — the point at which directives are applied.
    async fn open_and_drain(op: &mut dyn Operator) {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        op.open(&ctx).await.unwrap();
        while op.next_batch(&ctx).await.unwrap().is_some() {}
    }

    #[tokio::test]
    async fn dataset_forwards_row_budget_to_member() {
        let (mut op, (budget, _topk)) = recorder_dataset();
        op.set_row_budget(20);
        open_and_drain(&mut op).await;
        assert_eq!(
            *budget.lock().unwrap(),
            Some(20),
            "a LIMIT budget on the dataset operator must reach the member scan"
        );
    }

    #[tokio::test]
    async fn dataset_forwards_topk_to_member() {
        let (mut op, (_budget, topk)) = recorder_dataset();
        op.set_topk(VarId(3), 5);
        open_and_drain(&mut op).await;
        assert_eq!(*topk.lock().unwrap(), Some((VarId(3), 5)));
    }

    #[tokio::test]
    async fn dataset_without_limit_leaves_member_unbudgeted() {
        // No set_row_budget / set_topk (also the switch-OFF shape) → the member
        // sees nothing, i.e. byte-identical to the pre-T1.3 no-forward behavior.
        let (mut op, (budget, topk)) = recorder_dataset();
        open_and_drain(&mut op).await;
        assert_eq!(*budget.lock().unwrap(), None);
        assert_eq!(*topk.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn sort_above_dataset_absorbs_budget() {
        // ORDER BY must rank every row, so a LIMIT budget must NOT reach the dataset
        // scan through it — the dataset-path absorb boundary. `SortOperator`
        // inherits the no-op `set_row_budget`, so the dataset below is never
        // budgeted, and the member scan stays full.
        use crate::sort::{SortOperator, SortSpec};
        let (dataset, (budget, _topk)) = recorder_dataset();
        let mut sort = SortOperator::new(Box::new(dataset), vec![SortSpec::asc(VarId(0))]);
        sort.set_row_budget(7);
        open_and_drain(&mut sort).await;
        assert_eq!(
            *budget.lock().unwrap(),
            None,
            "Sort must absorb the budget; the dataset scan below stays unbudgeted"
        );
    }

    #[tokio::test]
    async fn distinct_above_dataset_absorbs_budget() {
        // DISTINCT may need > k raw rows to yield k unique → must absorb; the
        // dataset scan below stays full.
        use crate::distinct::DistinctOperator;
        let (dataset, (budget, _topk)) = recorder_dataset();
        let mut distinct = DistinctOperator::new(Box::new(dataset));
        distinct.set_row_budget(7);
        open_and_drain(&mut distinct).await;
        assert_eq!(*budget.lock().unwrap(), None);
    }
}
