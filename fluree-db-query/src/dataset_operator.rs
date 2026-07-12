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

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_core::{IndexType, ObjectBounds, Sid};
use hashbrown::HashMap;
use rustc_hash::{FxBuildHasher, FxHasher};

use crate::binary_history::BinaryHistoryScanOperator;
use crate::binary_scan::{schema_from_pattern_with_emit, BinaryScanOperator, EmitMask};
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::ir::triple::TriplePattern;
use crate::object_binding::{equality_norm, normalize_for_key, EqualityNorm};
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

    /// Whether this builder's scans emit every *variable* triple position (so
    /// the emitted row carries full triple identity for the dedup key; constant
    /// positions never form a column). The `DatasetOperator` cross-member
    /// set-dedup (SPARQL §13.2) keys on the emitted row, so it is only sound when
    /// no variable column is pruned. Defaults to `true` for builders that never
    /// prune; `ScanDatasetBuilder` reports its actual mask so a dataset path that
    /// arms dedup without setting `PlanningContext::multi_default_graph` (which
    /// forces `EmitMask::ALL`) trips the `debug_assert` in
    /// [`DatasetOperator::open`] instead of silently over-deduping. A ground
    /// (all-constant) pattern legitimately emits zero columns and is `true` here;
    /// its empty-mapping batches are collapsed separately by `BatchDeduper`.
    fn emit_is_full(&self) -> bool {
        true
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

    fn emit_is_full(&self) -> bool {
        // Full triple *identity* only requires that every VARIABLE position is
        // emitted — a constant position never forms a column, so its mask bit is
        // irrelevant to the dedup key. (`emit_mask_for_triple` sets constants to
        // `true`, but the property-join planner leaves them `false`, so consult
        // the pattern rather than assuming `s && p && o`.)
        (!self.pattern.s.is_var() || self.emit.s)
            && (!self.pattern.p.is_var() || self.emit.p)
            && (!self.pattern.o.is_var() || self.emit.o)
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

/// Cross-member set-deduplicator for a `>= 2`-member default union.
///
/// Per SPARQL §13.2 the default graph of a dataset is the RDF *merge* (a set)
/// of its `FROM` graphs, so a triple present in two members must be emitted
/// once. This holds a persistent seen-set across `next_batch` calls (the
/// operator streams one member's batch per call) and drops rows whose full
/// binding tuple has already been emitted.
///
/// Hashing mirrors [`DistinctOperator`](crate::distinct): rows are normalized
/// with [`EqualityNorm`] so a late-materialized `EncodedSid`/`EncodedLit` and
/// its decoded `Sid`/`Lit` twin collapse to one row. `Binding`'s manual
/// `Eq`/`Hash` exclude history metadata, so this is only armed in current mode
/// (see [`DatasetOperator::open`]).
struct BatchDeduper {
    seen: HashMap<Vec<Binding>, (), FxBuildHasher>,
    norm: Option<EqualityNorm>,
    /// Whether the empty solution mapping has already been emitted. A ground
    /// (all-constant) pattern like `{ <s> <p> <o> }` emits a zero-column schema
    /// even under forced `EmitMask::ALL` (only variable positions form columns),
    /// so its matches can't be keyed by the row-tuple set below. The empty
    /// mapping is itself a single set key, so it must survive at most once.
    seen_empty: bool,
}

impl BatchDeduper {
    fn new(ctx: &ExecutionContext<'_>) -> Self {
        Self {
            seen: HashMap::with_hasher(FxBuildHasher),
            norm: equality_norm(ctx),
            seen_empty: false,
        }
    }

    /// Retain only rows whose (normalized) binding tuple has not been emitted
    /// before, returning a batch of the new rows (may be empty).
    fn retain_new(&mut self, batch: Batch) -> Result<Batch> {
        // A ground (all-constant) pattern emits the empty solution mapping with
        // no columns. Under set-merge (SPARQL §13.2) that mapping is a single
        // solution, so a triple present in N default-union members must yield
        // ONE empty solution, not N: emit it on first sight, suppress after.
        // (`next_batch` only forwards non-empty batches, so a first-sight batch
        // always carries at least one empty row; collapse its whole count to 1.)
        if batch.schema().is_empty() {
            if self.seen_empty {
                return Ok(Batch::empty_schema_with_len(0));
            }
            self.seen_empty = true;
            return Ok(Batch::empty_schema_with_len(1));
        }
        let num_cols = batch.schema().len();
        let (store, gv) = EqualityNorm::parts(&self.norm);
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();
        for row_idx in 0..batch.len() {
            let signature: Vec<Binding> = (0..num_cols)
                .map(|col| normalize_for_key(batch.get_by_col(row_idx, col), store, gv))
                .collect();
            let mut h = FxHasher::default();
            signature.hash(&mut h);
            let hash = h.finish();
            let entry = self
                .seen
                .raw_entry_mut()
                .from_hash(hash, |sig| *sig == signature);
            if let hashbrown::hash_map::RawEntryMut::Vacant(v) = entry {
                v.insert_hashed_nocheck(hash, signature, ());
                for (col_idx, col) in columns.iter_mut().enumerate() {
                    col.push(batch.get_by_col(row_idx, col_idx).clone());
                }
            }
        }
        let schema: Arc<[VarId]> = Arc::from(batch.schema().to_vec().into_boxed_slice());
        Batch::new(schema, columns).map_err(|e| QueryError::Internal(e.to_string()))
    }
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
    /// Temporal mode captured at planner-time. Set-deduplication of the default
    /// union is only sound in current mode (history rows carry per-event
    /// assert/retract metadata that the dedup key deliberately ignores).
    mode: TemporalMode,
    /// Cross-member set-deduplicator, armed in [`open`](Operator::open) only for
    /// a current-mode `>= 2`-member default union. `None` for single graphs,
    /// named-graph scopes, and history mode (bag semantics preserved).
    dedup: Option<BatchDeduper>,
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
            mode: TemporalMode::Current,
            dedup: None,
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
        let mut op = Self::new(Box::new(builder));
        op.mode = mode;
        op
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

                // A `>= 2`-member active set is a default union (named scopes
                // always resolve to exactly one member; see `active_graphs`), so
                // enforce set semantics across members. Current mode only — the
                // dedup key ignores the per-event assert/retract metadata that
                // history rows must keep. The planner pairs this with forced
                // `EmitMask::ALL` on the first scan (see
                // `PlanningContext::multi_default_graph`) so that every *variable*
                // position is emitted and the row tuple keys the set. A ground
                // (all-constant) pattern still emits zero columns even so; those
                // zero-column batches are the empty solution mapping, which the
                // deduper collapses to a single solution (`BatchDeduper::retain_new`).
                self.dedup = if graphs.len() >= 2 && self.mode.is_current() {
                    // Soundness coupling: arming dedup on a `>= 2`-member union
                    // requires the plan to have forced `EmitMask::ALL` (via
                    // `PlanningContext::multi_default_graph`) so no variable
                    // column is pruned. A future dataset path that arms a
                    // multi-default scan without setting the flag would key the
                    // dedup on a pruned mask and over-dedup distinct triples —
                    // fail loudly here rather than silently.
                    debug_assert!(
                        self.builder.emit_is_full(),
                        "DatasetOperator armed cross-member set-dedup on a scan \
                         with a pruned variable column; the multi_default_graph \
                         planning flag must force EmitMask::ALL (make_first_scan) \
                         so distinct triples are not collapsed"
                    );
                    Some(BatchDeduper::new(ctx))
                } else {
                    None
                };

                for graph in &graphs {
                    let mut inner = self.builder.build()?;
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
                    // Set-merge the default union (SPARQL §13.2): drop triples an
                    // earlier member already emitted. Deduplicating *after*
                    // stamping keys the cross-ledger path on comparable
                    // IRI-level (`IriMatch`) values. An all-duplicate batch
                    // yields nothing — fetch the member's next batch.
                    let result = match &mut self.dedup {
                        Some(dedup) => {
                            let deduped = dedup.retain_new(result)?;
                            if deduped.is_empty() {
                                continue;
                            }
                            deduped
                        }
                        None => result,
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

        // Set semantics for a `>= 2`-member default union (SPARQL §13.2): the
        // per-member count-only sum below is a *bag* union that over-counts a
        // triple shared across members. `self.dedup` is armed exactly for that
        // case; count the deduplicated `next_batch` stream instead, forgoing the
        // count-only optimization on this cold path.
        if self.dedup.is_some() {
            let mut n: u64 = 0;
            while let Some(batch) = self.next_batch(ctx).await? {
                ctx.check_cancelled()?;
                n = n.checked_add(batch.len() as u64).ok_or_else(|| {
                    QueryError::execution("COUNT(*) overflow in dataset drain_count")
                })?;
            }
            return Ok(Some(n));
        }

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
        self.dedup = None;
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
}
