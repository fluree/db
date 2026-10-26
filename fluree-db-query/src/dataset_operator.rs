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
use fluree_db_core::{DatatypeConstraint, IndexType, ObjectBounds, Sid};
use hashbrown::HashMap;
use rustc_hash::{FxBuildHasher, FxHasher};

use crate::binary_history::BinaryHistoryScanOperator;
use crate::binary_scan::{schema_from_pattern_with_emit, BinaryScanOperator, EmitMask};
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
use crate::index_scope::IndexScope;
use crate::ir::triple::TriplePattern;
use crate::object_binding::{equality_norm, normalize_for_key, EqualityNorm};
use crate::operator::inline::{extend_schema, InlineOperator};
use crate::operator::{BoxedOperator, CountGroup, Operator, OperatorState, RowFact};
use crate::sort::SortSpec;
use crate::temporal_mode::TemporalMode;
use crate::union_group_count::{
    count_groups_over_union, GroupedPosition, UnionGroupCount, PARTITIONED_GROUP_BY_UNION_SITE,
};
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

    /// The grouped count of `group_vars` this builder's rows answer from a
    /// union of graphs' indexes ([`crate::union_group_count`]), counting rows
    /// that bind every one of `bound_vars`. Default: none.
    fn union_group_count(
        &self,
        _group_vars: &[VarId],
        _bound_vars: &[VarId],
    ) -> Option<UnionGroupCount<'_>> {
        None
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
    /// Scan with [`BinaryScanOperator::hiding_list_members`].
    hide_list_members: bool,
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
            hide_list_members: false,
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
            TemporalMode::Current => {
                let scan = BinaryScanOperator::new_with_emit_and_index(
                    self.pattern.clone(),
                    self.object_bounds.clone(),
                    self.inline_ops.clone(),
                    self.emit,
                    self.index_hint,
                );
                Ok(Box::new(if self.hide_list_members {
                    scan.hiding_list_members()
                } else {
                    scan
                }))
            }
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

    /// A current-mode scan of `?s <p> ?o` (two variables, a bound predicate,
    /// no datatype constraint) with no object bounds or inline operators: each
    /// of its rows is one fact of `<p>`, binding both variables. It answers a
    /// grouped count by `?s` or by `?o`.
    fn union_group_count(
        &self,
        group_vars: &[VarId],
        bound_vars: &[VarId],
    ) -> Option<UnionGroupCount<'_>> {
        if !self.mode.is_current() || self.object_bounds.is_some() || !self.inline_ops.is_empty() {
            return None;
        }
        let (s, _, o) = crate::execute::operator_tree::validate_simple_triple(&self.pattern)?;
        let position = match group_vars {
            [v] if *v == s => GroupedPosition::Subject,
            [v] if *v == o => GroupedPosition::Object,
            _ => return None,
        };
        bound_vars
            .iter()
            .all(|v| *v == s || *v == o)
            .then_some(UnionGroupCount {
                pattern: &self.pattern,
                position,
            })
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
/// of its `FROM` graphs, so a fact present in two members is one fact of the
/// union. A fact is storage's own identity (#2109's `FactIdentity`):
/// one fact per value and list position. So the key is a row's bindings plus
/// the fact its members' scans say it matched ([`RowFact`]: a constant
/// object's matched term, and the list position), and only a fact two members
/// both hold collapses: a union with a graph that adds nothing answers as the
/// graph alone. This holds a persistent seen-set across `next_batch` calls
/// (the operator streams one member's batch per call).
///
/// Hashing mirrors [`DistinctOperator`](crate::distinct): bindings are
/// normalized with [`EqualityNorm`] so a late-materialized
/// `EncodedSid`/`EncodedLit` and its decoded `Sid`/`Lit` twin collapse to one
/// row. `Binding`'s manual `Eq`/`Hash` exclude history metadata, so this is
/// only armed in current mode (see [`DatasetOperator::open`]).
///
/// MEMORY CLIFF (documented like `whole_graph_facts`'s): unlike a
/// single-graph scan, a `>= 2`-member default union is no longer
/// bounded-memory streaming — the seen-set grows O(distinct emitted facts)
/// with full normalized `Binding` signatures as keys, exactly like a
/// user-requested `DISTINCT` (which is the price of §13.2 set semantics; a
/// hash-only key was rejected because a collision would silently DROP a
/// distinct row). Each RETAINED row charges one unit of fuel, so a fuel
/// budget bounds the resident set; `drain_count` inherits the same cost when
/// it forgoes the per-member count-only shortcut (surfaced to EXPLAIN via
/// `plan_details.default_union_set_merge`).
struct BatchDeduper {
    seen: HashMap<FactRow, (), FxBuildHasher>,
    norm: Option<EqualityNorm>,
}

/// A union row's dedup key: its normalized bindings and the fact it matched.
#[derive(PartialEq, Eq, Hash)]
struct FactRow {
    bindings: Vec<Binding>,
    fact: Option<RowFact>,
}

impl BatchDeduper {
    fn new(ctx: &ExecutionContext<'_>) -> Self {
        Self {
            seen: HashMap::with_hasher(FxBuildHasher),
            norm: equality_norm(ctx),
        }
    }

    /// Retain only rows whose fact (normalized bindings plus `facts[row]`,
    /// the member scan's [`RowFact`]s for `batch`) has not been emitted
    /// before, returning a batch of the new rows (may be empty).
    fn retain_new(&mut self, batch: Batch, facts: Option<&[RowFact]>) -> Result<Batch> {
        if facts.is_some_and(|f| f.len() != batch.len()) {
            return Err(QueryError::Internal(
                "a union member's row facts do not match its batch".to_string(),
            ));
        }
        let (dicts, gv) = EqualityNorm::parts(&self.norm);
        let normalized_fact = |row: usize| -> Option<RowFact> {
            facts.map(|f| RowFact {
                object: f[row]
                    .object
                    .as_ref()
                    .map(|o| normalize_for_key(o, dicts, gv)),
                list_index: f[row].list_index,
            })
        };
        // A ground (all-constant) pattern emits the empty solution mapping
        // with no columns: one solution per fact it matched.
        if batch.schema().is_empty() {
            let mut kept = 0;
            for row in 0..batch.len() {
                let key = FactRow {
                    bindings: Vec::new(),
                    fact: normalized_fact(row),
                };
                if self.seen.insert(key, ()).is_none() {
                    kept += 1;
                }
            }
            return Ok(Batch::empty_schema_with_len(kept));
        }
        let num_cols = batch.schema().len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();
        for row_idx in 0..batch.len() {
            let key = FactRow {
                bindings: (0..num_cols)
                    .map(|col| normalize_for_key(batch.get_by_col(row_idx, col), dicts, gv))
                    .collect(),
                fact: normalized_fact(row_idx),
            };
            let mut h = FxHasher::default();
            key.hash(&mut h);
            let hash = h.finish();
            let entry = self
                .seen
                .raw_entry_mut()
                .from_hash(hash, |seen| *seen == key);
            if let hashbrown::hash_map::RawEntryMut::Vacant(v) = entry {
                v.insert_hashed_nocheck(hash, key, ());
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
    /// True when members are several graphs of one ledger, whose rows must
    /// leave decoded; see [`Self::member_ctx`].
    decode_members: bool,
    /// Temporal mode captured at planner-time. Set-deduplication of the default
    /// union is only sound in current mode (history rows carry per-event
    /// assert/retract metadata that the dedup key deliberately ignores).
    mode: TemporalMode,
    /// Cross-member set-deduplicator, armed in [`open`](Operator::open) only for
    /// a current-mode `>= 2`-member default union. `None` for single graphs,
    /// named-graph scopes, and history mode (bag semantics preserved).
    dedup: Option<BatchDeduper>,
    /// T1.3: a top-of-tree `LIMIT` row budget, recorded by `set_row_budget` and
    /// threaded into each member's inner subtree at build time in `open` (mirrors
    /// `GraphOperator`, which builds its inner subplan the same way). Without this
    /// the dataset wrapper swallowed the budget and a `LIMIT` never reached the
    /// R2RML scan. `None` unless the switch is on AND a budget was pushed.
    row_budget: Option<usize>,
    /// T1.3: an `ORDER BY … LIMIT` top-k directive, applied per member like
    /// `row_budget`. Per-member top-k is sound — the outer sort merges the members'
    /// partial top-k into the global one.
    topk: Option<(Vec<SortSpec>, usize)>,
    /// Whether a row has been drawn from the members since `open`; a grouped
    /// count drain answers only an undrawn input.
    drawn: bool,
}

impl DatasetOperator {
    /// The context member `graph` runs under.
    ///
    /// Members that are several graphs of one ledger keep the binary cursor,
    /// but their rows leave for a scope of several graphs, which has no single
    /// graph view to decode an encoded binding against (`ctx.graph_view()` is
    /// `None`). A join substituting an undecodable `EncodedSid` would leave the
    /// position unbound and pair every row with every other, and a NUM_BIG
    /// handle decodes only against its own graph's arena. So such members
    /// decode as they scan, where their own view is at hand.
    fn member_ctx<'a>(
        &self,
        ctx: &ExecutionContext<'a>,
        graph: &crate::dataset::GraphRef<'a>,
    ) -> ExecutionContext<'a> {
        Self::member_ctx_for(self.decode_members, ctx, graph)
    }

    fn member_ctx_for<'a>(
        decode_members: bool,
        ctx: &ExecutionContext<'a>,
        graph: &crate::dataset::GraphRef<'a>,
    ) -> ExecutionContext<'a> {
        let mut member = ctx.with_graph_ref(graph);
        if decode_members {
            member.eager_materialization = true;
        }
        member
    }

    /// Create a new dataset operator driven by the given builder.
    pub fn new(builder: Box<dyn DatasetBuilder>) -> Self {
        Self {
            builder,
            state: OperatorState::Created,
            members: Vec::new(),
            current_member: 0,
            needs_provenance: false,
            decode_members: false,
            mode: TemporalMode::Current,
            dedup: None,
            row_budget: None,
            topk: None,
            drawn: false,
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
        if let Some((ordering, k)) = &self.topk {
            member.set_topk(ordering, *k);
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

    /// [`Self::scan`] of `pattern`'s stored rows that are not list members
    /// (see [`BinaryScanOperator::hiding_list_members`]).
    pub fn scan_hiding_list_members(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
        emit: EmitMask,
        index_hint: Option<IndexType>,
        mode: TemporalMode,
    ) -> Self {
        let mut builder =
            ScanDatasetBuilder::new(pattern, object_bounds, inline_ops, emit, index_hint, mode);
        builder.hide_list_members = true;
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

    // A column's literals usually share one datatype, so the batch keeps its
    // last re-coding and re-codes a datatype once, not per row.
    let mut recoded: Option<(Sid, Sid)> = None;
    let stamped_columns: Vec<Vec<Binding>> = columns
        .into_iter()
        .map(|col| {
            col.into_iter()
                .map(|binding| stamp_binding(binding, ledger_id, ctx, &mut recoded))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    Batch::new(schema, stamped_columns).map_err(|e| QueryError::Internal(e.to_string()))
}

/// Stamp `batch` in the ledger a cross-ledger SERVICE body executes against,
/// when the context arms per-scan provenance (`scan_provenance_ledger`); a
/// pass-through everywhere else. For binding producers that are not wrapped
/// by a `DatasetOperator` (property paths) but emit target-encoded `Sid`s.
pub(crate) fn stamp_if_armed(batch: Batch, ctx: &ExecutionContext<'_>) -> Result<Batch> {
    match &ctx.scan_provenance_ledger {
        Some(ledger_id) => stamp_provenance(batch, ledger_id, ctx),
        None => Ok(batch),
    }
}

/// Stamp a parent row's raw `Sid`s in the REQUESTER's ledger (the context's
/// active snapshot) before it seeds a body that executes against another
/// ledger. Inside such a body every reference is namespace-neutral; a
/// requester-encoded `Sid` copied into a body column (`BIND(?parent AS ?x)`)
/// would otherwise be decoded through the target's table at the boundary.
/// An undecodable `Sid` is left as is.
pub(crate) fn stamp_seed_row(row: Vec<Binding>, ctx: &ExecutionContext<'_>) -> Vec<Binding> {
    let requester: Arc<str> = Arc::from(ctx.active_snapshot.ledger_id.as_str());
    row.into_iter()
        .map(|b| match b {
            Binding::Sid { sid, .. } => match ctx.active_snapshot.decode_sid(&sid) {
                Some(iri) => Binding::iri_match(iri, sid, Arc::clone(&requester)),
                None => Binding::sid(sid),
            },
            other => other,
        })
        .collect()
}

/// Stamp a single binding with ledger provenance.
///
/// `Binding::Sid` is converted to `IriMatch`. A literal whose datatype is
/// outside the built-in namespaces has that datatype re-coded into the
/// primary snapshot's namespace space, the one the rest of the query (its
/// patterns, its keys, its output) uses: ledgers code their namespaces
/// independently, so the same datatype IRI is a different `Sid` in another
/// ledger. `recoded` holds the last re-coding, `(member's, primary's)`. All
/// other variants are moved through unchanged (no cloning).
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
    recoded: &mut Option<(Sid, Sid)>,
) -> Result<Binding> {
    match binding {
        Binding::Sid { ref sid, .. } => sid_to_iri_match(sid, ledger_id, ctx),
        Binding::Lit {
            val,
            dtc: DatatypeConstraint::Explicit(dt),
            t,
            op,
            p_id,
        } if !crate::context::namespace_code_is_shared(dt.namespace_code)
            && ledger_id.as_ref() != ctx.original_snapshot.ledger_id.as_str() =>
        {
            let dt = match recoded {
                Some((from, to)) if *from == dt => to.clone(),
                _ => {
                    let to = ctx
                        .decode_sid_in_ledger(&dt, ledger_id)
                        .and_then(|iri| ctx.original_snapshot.encode_iri(&iri))
                        .unwrap_or_else(|| dt.clone());
                    *recoded = Some((dt, to.clone()));
                    to
                }
            };
            Ok(Binding::Lit {
                val,
                dtc: DatatypeConstraint::Explicit(dt),
                t,
                op,
                p_id,
            })
        }
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

    fn set_topk(&mut self, ordering: &[SortSpec], k: usize) {
        // T1.3: record ORDER BY … LIMIT top-k; applied per member like the row
        // budget. Per-member top-k is sound — the outer sort merges the members'
        // partial top-k into the global one (same reasoning as `GraphOperator`'s
        // per-partition top-k). Switch-gated.
        if crate::r2rml::dataset_budget_enabled() {
            self.topk = Some((ordering.to_vec(), k));
        }
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut details = self.builder.plan_details();
        // Post-open, report the armed §13.2 set-merge so EXPLAIN can say why
        // memory grows with distinct rows and why COUNT(*) forgoes the
        // per-member count-only shortcut on a multi-FROM union.
        if self.dedup.is_some() {
            details.insert("default_union_set_merge".to_string(), true.into());
        }
        details
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
                // context directly. No fanout; provenance stamping only when
                // the context demands it — a cross-ledger SERVICE subtree sets
                // `scan_provenance_ledger` so raw target-encoded `Sid`s never
                // escape a scan (they would be re-decoded against the
                // requester's namespace table the moment an intra-body join
                // substitutes them into a pattern; issue #1665). Everywhere
                // else the flag is `None` and this lane is unchanged.
                let mut inner = self.builder.build()?;
                self.apply_member_directives(inner.as_mut());
                inner.open(ctx).await?;
                self.needs_provenance = ctx.scan_provenance_ledger.is_some();
                self.members.push(DatasetMember {
                    operator: inner,
                    ledger_id: ctx
                        .scan_provenance_ledger
                        .clone()
                        .unwrap_or_else(|| Arc::from("")),
                });
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
                    if !self.builder.emit_is_full() {
                        // A pruned variable column under an armed dedup would
                        // COLLAPSE DISTINCT TRIPLES (silent wrong cardinality).
                        // This was a debug_assert, which compiles out in
                        // release — fail loud there too: every known dataset
                        // path forces full emission (make_first_scan under
                        // multi_default_graph; joins/OPTIONAL/property-join
                        // widen or hard-code ALL), so reaching this is a
                        // planner bug, not a user error.
                        return Err(QueryError::Internal(
                            "multi-default-graph set-dedup armed on a scan with a pruned \
                             variable column; the plan must force full variable emission"
                                .to_string(),
                        ));
                    }
                    Some(BatchDeduper::new(ctx))
                } else {
                    None
                };

                self.decode_members = !multi_ledger && graphs.len() >= 2;

                for graph in &graphs {
                    let mut inner = self.builder.build()?;
                    self.apply_member_directives(inner.as_mut());
                    // The dedup keys on the fact each row matched, which only
                    // the member's scan knows.
                    if self.dedup.is_some() && !inner.record_row_facts() {
                        return Err(QueryError::Internal(
                            "multi-default-graph set-dedup armed on a member that cannot \
                             record the facts its rows match"
                                .to_string(),
                        ));
                    }
                    let mut per_graph_ctx = self.member_ctx(ctx, graph);

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
        self.drawn = true;

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
                        let graph_ctx =
                            Self::member_ctx_for(self.decode_members, ctx, g[self.current_member]);
                        member.operator.next_batch(&graph_ctx).await?
                    }
                    ActiveGraphs::Single => member.operator.next_batch(ctx).await?,
                }
            };

            match batch {
                Some(ref b) if b.is_empty() => continue,
                Some(batch) => {
                    let facts = match self.dedup {
                        Some(_) => self.members[self.current_member].operator.take_row_facts(),
                        None => None,
                    };
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
                            let deduped = dedup.retain_new(result, facts.as_deref())?;
                            // Each RETAINED row grows the resident seen-set;
                            // charge fuel so a budget bounds the memory cliff
                            // (see the BatchDeduper doc).
                            ctx.tracker.consume_fuel(deduped.len() as u64)?;
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

        self.drawn = true;
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

    /// A default graph of several graphs of one ledger counts one pattern's
    /// rows per group from its members' indexes (lane A, see
    /// [`crate::union_group_count`]) when every member's index answers for
    /// it, instead of draining and deduplicating the members' decoded rows.
    /// A default graph of none answers no group. Each answer or decline
    /// stamps [`PARTITIONED_GROUP_BY_UNION_SITE`]; the kill switch declines.
    async fn drain_grouped_count(
        &mut self,
        ctx: &ExecutionContext<'_>,
        group_vars: &[VarId],
        bound_vars: &[VarId],
    ) -> Result<Option<Vec<CountGroup>>> {
        if self.state != OperatorState::Open || self.drawn {
            return Ok(None);
        }
        let Some(spec) = self.builder.union_group_count(group_vars, bound_vars) else {
            return Ok(None);
        };
        let scope = ctx.index_scope();
        if !matches!(scope, Some(IndexScope::Union(_) | IndexScope::Empty)) {
            return Ok(None);
        }
        let declined = |reason| {
            stamp_fast_path(
                PARTITIONED_GROUP_BY_UNION_SITE,
                FastPathOutcome::Fallback(reason),
            );
            Ok(None)
        };
        if crate::execute::fast_paths_disabled() {
            return declined(FastPathFallback::KillSwitch);
        }
        let groups = match &scope {
            Some(IndexScope::Union(set)) => match count_groups_over_union(ctx, set, &spec)? {
                Ok(groups) => groups,
                Err(reason) => return declined(reason),
            },
            // A default graph of none holds no row.
            _ => Vec::new(),
        };
        stamp_fast_path(PARTITIONED_GROUP_BY_UNION_SITE, FastPathOutcome::Proceed);
        self.state = OperatorState::Exhausted;
        Ok(Some(groups))
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

    /// A scan builder offers a grouped count over a union of graphs only for
    /// `?s <p> ?o` in current mode with nothing applied to its rows, grouped
    /// by `?s` or `?o`, every counted variable one the pattern binds.
    #[test]
    fn scan_builder_offers_a_union_group_count_for_one_plain_pattern() {
        use crate::ir::triple::{Ref, Term};
        use fluree_db_core::Sid;

        let (s, o, other) = (VarId(0), VarId(1), VarId(2));
        let p = Ref::Sid(Sid::new(100, "n"));
        let plain = TriplePattern::new(Ref::Var(s), p.clone(), Term::Var(o));
        let builder = |pattern: TriplePattern, bounds, inline_ops, mode| {
            ScanDatasetBuilder::new(pattern, bounds, inline_ops, EmitMask::ALL, None, mode)
        };
        let current = builder(plain.clone(), None, Vec::new(), TemporalMode::Current);
        let position = |b: &ScanDatasetBuilder, group: &[VarId], bound: &[VarId]| {
            b.union_group_count(group, bound).map(|spec| spec.position)
        };

        assert_eq!(
            position(&current, &[s], &[]),
            Some(GroupedPosition::Subject)
        );
        assert_eq!(
            position(&current, &[o], &[s, o]),
            Some(GroupedPosition::Object)
        );
        // Two keys, a key the pattern does not bind, a count it cannot show
        // bound in every row.
        assert_eq!(position(&current, &[s, o], &[]), None);
        assert_eq!(position(&current, &[other], &[]), None);
        assert_eq!(position(&current, &[o], &[other]), None);

        let history = builder(plain.clone(), None, Vec::new(), TemporalMode::History);
        assert_eq!(position(&history, &[o], &[]), None);
        let bounded = builder(
            plain.clone(),
            Some(ObjectBounds::new()),
            Vec::new(),
            TemporalMode::Current,
        );
        assert_eq!(position(&bounded, &[o], &[]), None);
        let bind = InlineOperator::Bind {
            var: other,
            expr: crate::ir::Expression::Var(o),
        };
        let inline = builder(plain, None, vec![bind], TemporalMode::Current);
        assert_eq!(position(&inline, &[o], &[]), None);

        // A constant object, a variable predicate, one variable twice.
        let constant = TriplePattern::new(Ref::Var(s), p.clone(), Term::Sid(Sid::new(100, "T1")));
        let wildcard = TriplePattern::new(Ref::Var(s), Ref::Var(other), Term::Var(o));
        let repeated = TriplePattern::new(Ref::Var(s), p, Term::Var(s));
        for pattern in [constant, wildcard, repeated] {
            let b = builder(pattern, None, Vec::new(), TemporalMode::Current);
            assert_eq!(position(&b, &[s], &[]), None);
        }
    }

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
        topk: Arc<Mutex<Option<(VarId, usize, bool)>>>,
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
        fn set_topk(&mut self, ordering: &[SortSpec], k: usize) {
            let primary = &ordering[0];
            *self.topk.lock().unwrap() = Some((
                primary.var,
                k,
                matches!(primary.direction, crate::sort::SortDirection::Ascending),
            ));
        }
    }

    /// `DatasetBuilder` that yields a fresh `DirectiveRecorder` sharing the given
    /// handles, so a test can read what the built member(s) received.
    struct RecorderBuilder {
        budget: Arc<Mutex<Option<usize>>>,
        topk: Arc<Mutex<Option<(VarId, usize, bool)>>>,
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
        Arc<Mutex<Option<(VarId, usize, bool)>>>,
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
        let snapshot = LedgerSnapshot::genesis("test:main");
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
        op.set_topk(&[SortSpec::desc(VarId(3))], 5);
        open_and_drain(&mut op).await;
        assert_eq!(*topk.lock().unwrap(), Some((VarId(3), 5, false)));
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
