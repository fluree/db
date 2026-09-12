//! Subquery operator - executes nested queries and merges results
//!
//! Implements correlated subquery semantics where:
//! - For each parent solution, the subquery is executed
//! - Shared variables between parent and subquery are correlated
//! - Subquery results are merged with the parent solution
//!
//! # Syntax
//!
//! ```json
//! ["query", {
//!   "select": ["?s", "?age"],
//!   "where": {"@id": "?s", "schema:age": "?age"}
//! }]
//! ```
//!
//! # Correlation Semantics
//!
//! Variables shared between parent and subquery are used for correlation:
//! - If `?s` is bound in the parent, the subquery filters to only those `?s` values
//! - Results are merged back to the parent solution

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::execute::build_where_operators_seeded;
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::{Pattern, SubqueryPattern};
use crate::object_binding::{equality_norm, EqualityNorm};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Subquery operator - executes nested queries and merges results
pub struct SubqueryOperator {
    /// Child operator providing parent solutions
    child: BoxedOperator,
    /// The subquery pattern to execute
    subquery: SubqueryPattern,
    /// Output schema (parent schema + new subquery variables)
    in_schema: Arc<[VarId]>,
    /// Variables used for correlation (appear in BOTH parent schema and subquery patterns)
    correlation_vars: Vec<VarId>,
    /// New variables introduced by the subquery select list (not present in parent schema)
    new_vars: Vec<VarId>,
    /// Index of variables in the subquery select row (VarId -> position)
    select_index: HashMap<VarId, usize>,
    /// Operator state
    state: OperatorState,
    /// Current parent and inner positions. Output expansion resumes here instead
    /// of collecting an entire parent batch's join product.
    parent_batch: Option<RetainedBatch>,
    parent_row: usize,
    inner: Option<BoxedOperator>,
    inner_batch: Option<RetainedBatch>,
    inner_row: usize,
    probe: Option<MatchCursor>,
    /// A structural guarantee, never a cardinality estimate: joining to the
    /// identity solution needs neither a reusable materialization nor an index.
    stream_identity: bool,
    /// Optional stats for selectivity-based pattern reordering in subquery
    stats: Option<Arc<StatsView>>,
    /// Planning context captured at planner-time for the subquery subplan.
    planning: PlanningContext,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
    /// The subset of `correlation_vars` used as the hash-join key in join-mode:
    /// the variables the subquery PRODUCES itself (bound by a top-level required
    /// triple / property path). Seeding such a variable per parent row only
    /// filters the subquery's output to that value, so it is equivalent to
    /// evaluating the subquery once and joining on the variable. A correlation
    /// variable that the subquery does NOT bind (e.g. a `GROUP BY` key never
    /// constrained in the body — the BSBM BI-5 quirk) is a pass-through: it is
    /// omitted from the key and flows from the parent, matching SPARQL join
    /// semantics (the subquery's unbound value joins with the parent's bound
    /// value). Empty when there is no correlation (broadcast).
    join_keys: Vec<VarId>,
    /// Correlation vars the subquery binds only via `OPTIONAL`/`UNION`/`BIND`
    /// (`correlation_vars \ join_keys`) — not self-produced, so NOT safe hash
    /// keys, yet present in the subquery's output. They are evaluated UNSEEDED
    /// (per-row mode seeds only `join_keys`; join-mode seeds nothing), so one of
    /// these can take a value that conflicts with the parent's binding; the merge
    /// must then drop the row (SPARQL §18.4 keeps only compatible mappings). The
    /// merge check is therefore LOAD-BEARING in BOTH modes — it is NOT a per-row
    /// no-op, so do not gate it on join-mode. Fixes W3C `var-scope-join-1`
    /// (join-scope-1), where `?X` is bound only by an inner `OPTIONAL` and must
    /// reconcile against the parent `?X`. See the merge in `merge_row`.
    reconcile_vars: Vec<VarId>,
    /// Whether the subquery is evaluated ONCE rather than per parent row.
    /// Reusable results are hash-joined on `join_keys`; an identity seed instead
    /// streams them. Independent SPARQL subqueries with grouping, DISTINCT, or
    /// slicing require independent evaluation for correctness.
    /// Otherwise eligibility and parent cardinality decide whether evaluating
    /// once beats per-row seeding. Bound outer keys use an exact hash probe;
    /// unbound outer keys match all compatible materialized rows.
    join_mode: bool,
    /// Lazily materialized result (built once for non-identity `join_mode`): all
    /// result rows plus a hash index from the correlation-variable values to the
    /// rows carrying them. Reused across every parent row and batch.
    materialized: Option<MaterializedSubquery>,
    /// Store for normalizing decoded bindings to encoded form on both join
    /// sides, so mixed-representation rows key identically. `None` outside
    /// single-ledger binary execution.
    norm: Option<EqualityNorm>,
}

/// A once-evaluated subquery result, indexed for hash-join probing.
struct MaterializedSubquery {
    /// All subquery result rows (projected to the subquery SELECT list).
    rows: Vec<Vec<Binding>>,
    _memory: MemoryCharge,
    /// Correlation-variable values -> indices into `rows`. An empty key vector
    /// (no correlation) maps every row under a single bucket (broadcast).
    index: IndexMap<Vec<GroupKeyOwned>, Vec<usize>>,
}

/// Own only this operator's allocation charges. Dropping a batch or a complete
/// materialization releases its charge; never subtract a context-wide delta
/// across streaming yields, since downstream operators also use that counter.
struct MemoryCharge {
    cancellation: fluree_db_core::QueryCancellation,
    bytes: usize,
}

impl MemoryCharge {
    fn new(ctx: &ExecutionContext<'_>) -> Self {
        Self {
            cancellation: ctx.cancellation.clone(),
            bytes: 0,
        }
    }

    fn add(&mut self, ctx: &ExecutionContext<'_>, bytes: usize) -> Result<()> {
        self.cancellation.record_alloc(bytes);
        self.bytes = self.bytes.saturating_add(bytes);
        ctx.checkpoint()
    }
}

impl Drop for MemoryCharge {
    fn drop(&mut self) {
        self.cancellation.release(self.bytes);
    }
}

struct RetainedBatch {
    batch: Batch,
    _memory: MemoryCharge,
}

impl RetainedBatch {
    fn new(batch: Batch, ctx: &ExecutionContext<'_>) -> Result<Self> {
        let mut memory = MemoryCharge::new(ctx);
        memory.add(
            ctx,
            batch
                .len()
                .saturating_mul(batch.schema().len())
                .saturating_mul(crate::context::BINDING_EST_BYTES),
        )?;
        Ok(Self {
            batch,
            _memory: memory,
        })
    }
}

impl std::ops::Deref for RetainedBatch {
    type Target = Batch;
    fn deref(&self) -> &Batch {
        &self.batch
    }
}

/// Resumable probe without copying matching rows or building a match list.
/// IndexMap lets partial keys visit buckets by position without keeping a
/// self-referential hash iterator across await points.
enum MatchCursor {
    Exact {
        bucket: usize,
        row: usize,
    },
    All {
        row: usize,
    },
    Partial {
        key: Vec<GroupKeyOwned>,
        unbound: Vec<usize>,
        bucket: usize,
        row: usize,
    },
    Done,
}

impl MatchCursor {
    fn next(
        &mut self,
        mat: &MaterializedSubquery,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<usize>> {
        match self {
            Self::Done => Ok(None),
            Self::Exact { bucket, row } => {
                let idx = mat
                    .index
                    .get_index(*bucket)
                    .and_then(|(_, rows)| rows.get(*row))
                    .copied();
                *row += usize::from(idx.is_some());
                Ok(idx)
            }
            Self::All { row } => {
                if *row == mat.rows.len() {
                    return Ok(None);
                }
                let idx = *row;
                *row += 1;
                Ok(Some(idx))
            }
            Self::Partial {
                key,
                unbound,
                bucket,
                row,
            } => {
                while let Some((inner_key, rows)) = mat.index.get_index(*bucket) {
                    if *row == 0 {
                        if *bucket % 1024 == 0 {
                            ctx.check_cancelled()?;
                        }
                        if !key
                            .iter()
                            .zip(inner_key)
                            .enumerate()
                            .all(|(col, (p, s))| p == s || unbound.contains(&col))
                        {
                            *bucket += 1;
                            continue;
                        }
                    }
                    if let Some(&idx) = rows.get(*row) {
                        *row += 1;
                        return Ok(Some(idx));
                    }
                    *bucket += 1;
                    *row = 0;
                }
                Ok(None)
            }
        }
    }
}

/// Bound the join product independently of either input's cardinality.
const SUBQUERY_BATCH_SIZE: usize = 1024;

impl SubqueryOperator {
    /// Create a new subquery operator
    pub fn new(
        child: BoxedOperator,
        subquery: SubqueryPattern,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        let parent_schema: HashSet<VarId> = child.schema().iter().copied().collect();
        let subquery_select_vars: HashSet<VarId> = subquery.select.iter().copied().collect();

        // Correlation vars: variables in BOTH the parent schema AND the subquery
        // SELECT list.  Per SPARQL semantics, the subquery's scope boundary is
        // defined by its SELECT — variables not SELECTed are invisible from the
        // parent, even if referenced internally (e.g., in FILTERs).
        let correlation_vars: Vec<VarId> = child
            .schema()
            .iter()
            .copied()
            .filter(|v| subquery_select_vars.contains(v))
            .collect();

        // New vars are subquery *selected* vars that are not in parent schema, preserving select order.
        let new_vars: Vec<VarId> = subquery
            .select
            .iter()
            .copied()
            .filter(|v| !parent_schema.contains(v))
            .collect();

        // Build select index for row merging
        let select_index: HashMap<VarId, usize> = subquery
            .select
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Output schema = parent schema + new vars from subquery
        let mut schema_vec: Vec<VarId> = child.schema().to_vec();
        schema_vec.extend(&new_vars);
        let schema = Arc::from(schema_vec.into_boxed_slice());

        // Partition correlation vars: a JOIN KEY is one the subquery binds in
        // every solution itself (a top-level required triple / property path);
        // seeding it only filters the output, so it can be a hash key.
        let produced = self_produced_vars(&subquery.patterns);
        // An explicitly-pinned import (Cypher `CALL (p)` — `pinned_vars`) is
        // seeded like a self-produced key even when the body binds it only via
        // OPTIONAL: the import IS a per-row binding by the surface's contract,
        // not an inferred correlation for Family-B reconciliation.
        let pinned: std::collections::HashSet<VarId> =
            subquery.pinned_vars.iter().copied().collect();
        let join_keys: Vec<VarId> = correlation_vars
            .iter()
            .copied()
            .filter(|v| produced.contains(v) || pinned.contains(v))
            .collect();

        // The complementary set: correlation vars the subquery binds only
        // conditionally (OPTIONAL/UNION/BIND), so they are not hash keys but must
        // still be reconciled against the parent at merge time (Family B). Every
        // correlation var is either self-produced (a join key) or here.
        let reconcile_vars: Vec<VarId> = correlation_vars
            .iter()
            .copied()
            .filter(|v| !produced.contains(v) && !pinned.contains(v))
            .collect();

        // Every NON-key correlation var must be either produced by the inner
        // (seeding it only filters — equivalent to the natural join) or
        // unreferenced in the inner body (seeding it is a no-op). When that
        // holds, per-row seeding yields the same multiset as evaluating the
        // subquery independently and joining; otherwise (a var referenced in an
        // inner FILTER/BIND/aggregate) only independent evaluation is correct.
        let body_referenced = referenced_vars_set(&subquery.patterns);
        let pass_through_ok = correlation_vars
            .iter()
            .all(|v| produced.contains(v) || !body_referenced.contains(v));

        // SPARQL 1.1 §18.2: a sub-SELECT is evaluated INDEPENDENTLY and then
        // joined. Independent evaluation is only *required* to honor that when the
        // subquery is sliced, deduplicated, or aggregated (per-row seeding
        // would then change the result — e.g. an inner LIMIT applying per parent
        // row, W3C subquery/sq11), or when a correlation var is referenced but
        // not produced (seeding correlates it). A plain projection sub-SELECT is
        // equivalent either way, so it falls through to the cardinality-guarded
        // choice below and may prune via per-row seeding for a small, selective
        // parent — the legacy optimization we'd otherwise lose for all SPARQL
        // sub-SELECTs.
        let must_evaluate_once = subquery.uncorrelated
            && (subquery.limit.is_some()
                || subquery.offset.is_some()
                || subquery.distinct
                || subquery.grouping.is_some()
                || !pass_through_ok);

        let join_mode = if must_evaluate_once {
            true
        } else {
            // Evaluate-once + hash-join only when there is no inner slice and
            // every non-key correlation var is an unreferenced pass-through;
            // otherwise per-row seed. The cardinality guard prefers per-row
            // seeding for a small parent (its pruning seed can be cheaper); an
            // uncorrelated (no shared key) subquery is always evaluated once
            // (per-row recomputes identically).
            // A pinned import the body does not itself produce requires
            // true per-row (LATERAL) evaluation: evaluate-once + hash-join on
            // the import would drop zero-match parents (e.g. Cypher
            // `CALL (p) { OPTIONAL MATCH … RETURN count(…) }` retaining a
            // friendless `p` as 0).
            let pinned_requires_per_row = subquery
                .pinned_vars
                .iter()
                .any(|v| correlation_vars.contains(v) && !produced.contains(v));
            let eligible = subquery.limit.is_none()
                && subquery.offset.is_none()
                && pass_through_ok
                && !pinned_requires_per_row;
            eligible
                && (correlation_vars.is_empty()
                    || child
                        .estimated_rows()
                        .is_none_or(|n| n >= SUBQUERY_MATERIALIZE_MIN_PARENT_ROWS))
        };

        let stream_identity = join_mode && child.is_identity_seed();

        Self {
            child,
            subquery,
            in_schema: schema,
            correlation_vars,
            new_vars,
            select_index,
            state: OperatorState::Created,
            parent_batch: None,
            parent_row: 0,
            inner: None,
            inner_batch: None,
            inner_row: 0,
            probe: None,
            stream_identity,
            stats,
            planning,
            out_schema: None,
            join_keys,
            reconcile_vars,
            join_mode,
            materialized: None,
            norm: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }
}

#[async_trait]
impl Operator for SubqueryOperator {
    /// Item 11 (F-AUD-7): DECLINE forwarding — a subquery computes its own result
    /// set independently of the outer `LIMIT` (its cardinality, grouping, and
    /// ordering are self-contained), so the outer budget cannot bound its input.
    /// Explicit (was a silent trait-default no-op) so the swallow is observable.
    fn set_row_budget(&mut self, budget: usize) {
        tracing::debug!(
            budget,
            "SUBQUERY row-budget swallowed (unsound to forward: independent inner cardinality)"
        );
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("join-mode".into(), self.join_mode.into());
        m.insert("stream-identity".into(), self.stream_identity.into());
        if !self.correlation_vars.is_empty() {
            m.insert(
                "correlation-vars".into(),
                serde_json::Value::Array(
                    self.correlation_vars
                        .iter()
                        .map(|v| serde_json::Value::String(format!("?v{}", v.0)))
                        .collect(),
                ),
            );
        }
        m
    }

    /// The subquery's inner operator tree is built lazily at runtime (it is not
    /// present before execution), so the default `plan_children` walk can't reach it.
    /// Rebuild it here — build-only, no `open()`/exec — from the stored IR +
    /// stats + planning, and attach it under a `SubqueryBody` node so the inner
    /// joins (where BSBM-BI time lives) are visible. The first child is the outer
    /// input the subquery correlates against.
    fn describe(&self) -> crate::plan_node::PlanNode {
        use crate::plan_node::{PlanEdge, PlanEdgeRel, PlanNode};

        let mut children = vec![PlanEdge {
            rel: PlanEdgeRel::Child,
            node: self.child.describe(),
        }];

        let body = match self.build_inner_plan_for_explain() {
            Ok(inner) => PlanNode {
                op: "SubqueryBody".into(),
                est_rows: None,
                details: serde_json::Map::new(),
                children: vec![PlanEdge {
                    rel: PlanEdgeRel::Child,
                    node: inner.describe(),
                }],
            },
            Err(e) => PlanNode::leaf(format!("SubqueryBody <error: {e}>"), None),
        };
        children.push(PlanEdge {
            rel: PlanEdgeRel::Child,
            node: body,
        });

        PlanNode {
            op: self.op_name(),
            est_rows: self.estimated_rows(),
            details: self.plan_details(),
            children,
        }
    }

    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.materialized = None;
        if self.norm.is_none() {
            self.norm = equality_norm(ctx);
        }
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let result = self.next_bounded_batch(ctx).await;
        if result.is_err() {
            self.close();
        }
        result
    }

    fn close(&mut self) {
        self.child.close();
        if let Some(mut inner) = self.inner.take() {
            inner.close();
        }
        self.parent_batch = None;
        self.inner_batch = None;
        self.materialized = None;
        self.probe = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // The subquery's OWN output estimate seeds the downstream object→subject
        // hash join's driving estimate so a `(message HAS_CREATOR friend)` probe
        // is costed against the ~producer size, not 1 — but ONLY for shapes whose
        // output is reliably bounded (scalar aggregate, anchored `WITH DISTINCT`
        // producer, or explicit LIMIT). For an arbitrary subquery the estimate is
        // just body cardinality, which is fine for join ordering but too
        // unreliable to perturb the hash-join cost model, so we keep the
        // conservative `None` the operator returned before.
        if !crate::planner::subquery_output_estimate_is_bounded(&self.subquery) {
            return None;
        }
        Some(
            crate::planner::estimate_subquery_output(&self.subquery, self.stats.as_deref()).round()
                as usize,
        )
    }
}

impl SubqueryOperator {
    async fn next_bounded_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        ctx.checkpoint()?;
        let mut columns: Vec<Vec<Binding>> =
            (0..self.in_schema.len()).map(|_| Vec::new()).collect();
        // Charge the output columns as they grow, and release on handoff.
        // Tiny results do not reserve a full 1024-row window.
        let mut output_memory = MemoryCharge::new(ctx);
        let mut output_capacity = 0;
        let mut emitted = 0;
        let mut visited = 0usize;
        while emitted < SUBQUERY_BATCH_SIZE {
            if visited.is_multiple_of(SUBQUERY_BATCH_SIZE) {
                ctx.check_cancelled()?;
            }
            visited += 1;
            if self.parent_batch.is_none() {
                match self.child.next_batch(ctx).await? {
                    Some(batch) => {
                        self.parent_batch = Some(RetainedBatch::new(batch, ctx)?);
                        self.parent_row = 0;
                    }
                    None => {
                        self.state = OperatorState::Exhausted;
                        break;
                    }
                }
            }
            let parent = self.parent_batch.as_ref().unwrap();
            if self.parent_row == parent.len() {
                self.parent_batch = None;
                continue;
            }

            let merged = if self.join_mode && !self.stream_identity {
                if self.materialized.is_none() {
                    self.materialized = Some(self.materialize(ctx).await?);
                }
                if self.probe.is_none() {
                    self.probe = Some(self.start_probe());
                }
                let mat = self.materialized.as_ref().unwrap();
                let Some(idx) = self.probe.as_mut().unwrap().next(mat, ctx)? else {
                    self.probe = None;
                    self.parent_row += 1;
                    continue;
                };
                self.merge_row(parent, self.parent_row, |col| mat.rows[idx].get(col))
            } else {
                // Per-row correlations keep their pruning seed. An identity
                // parent uses an independent empty seed and runs the body once.
                if self.inner.is_none() {
                    let seed = self.seed_for_row(parent, self.parent_row);
                    let mut inner = self.build_inner_plan(seed)?;
                    if let Err(error) = inner.open(ctx).await {
                        inner.close();
                        return Err(error);
                    }
                    self.inner = Some(inner);
                }
                if self.inner_batch.is_none() {
                    match self.inner.as_mut().unwrap().next_batch(ctx).await? {
                        Some(batch) => {
                            self.inner_batch = Some(RetainedBatch::new(batch, ctx)?);
                            self.inner_row = 0;
                        }
                        None => {
                            self.inner.take().unwrap().close();
                            self.parent_row += 1;
                            continue;
                        }
                    }
                }
                let batch = self.inner_batch.as_ref().unwrap();
                if self.inner_row == batch.len() {
                    self.inner_batch = None;
                    continue;
                }
                let row = self.inner_row;
                self.inner_row += 1;
                self.merge_row(parent, self.parent_row, |col| {
                    self.subquery
                        .select
                        .get(col)
                        .and_then(|v| batch.get(row, *v))
                })
            };
            if let Some(row) = merged {
                for (col, binding) in columns.iter_mut().zip(row) {
                    col.push(binding);
                }
                let capacity: usize = columns.iter().map(Vec::capacity).sum();
                if capacity > output_capacity {
                    output_memory.add(
                        ctx,
                        (capacity - output_capacity)
                            .saturating_mul(crate::context::BINDING_EST_BYTES),
                    )?;
                    output_capacity = capacity;
                }
                emitted += 1;
            }
        }
        if emitted == 0 {
            return Ok(None);
        }
        if columns.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(emitted)));
        }
        Ok(trim_batch(
            &self.out_schema,
            Batch::new(self.in_schema.clone(), columns)?,
        ))
    }

    fn start_probe(&self) -> MatchCursor {
        let parent = self.parent_batch.as_ref().unwrap();
        // Only actual Unbound values are wildcards. Poisoned also normalizes
        // to Absent but must retain the existing blocking semantics.
        let mut unbound = Vec::new();
        let key: Vec<_> = self
            .join_keys
            .iter()
            .enumerate()
            .map(|(col, v)| {
                let binding = parent.get(self.parent_row, *v);
                if binding.is_none_or(|b| matches!(b, Binding::Unbound)) {
                    unbound.push(col);
                }
                let (store, gv) = EqualityNorm::parts(&self.norm);
                binding
                    .map(|b| binding_to_group_key_normalized(b, store, gv))
                    .unwrap_or(GroupKeyOwned::Absent)
            })
            .collect();
        if unbound.is_empty() {
            self.materialized
                .as_ref()
                .unwrap()
                .index
                .get_index_of(&key)
                .map_or(MatchCursor::Done, |bucket| MatchCursor::Exact {
                    bucket,
                    row: 0,
                })
        } else if unbound.len() == key.len() {
            MatchCursor::All { row: 0 }
        } else {
            MatchCursor::Partial {
                key,
                unbound,
                bucket: 0,
                row: 0,
            }
        }
    }

    fn merge_row<'a>(
        &self,
        parent_batch: &Batch,
        row_idx: usize,
        subquery_row: impl Fn(usize) -> Option<&'a Binding>,
    ) -> Option<Vec<Binding>> {
        // Family B — reconcile OPTIONAL/UNION-produced correlation vars.
        // These are correlation vars the subquery does not self-produce,
        // so they are not hash join keys; the subquery binds them
        // independently (they are never seeded — per-row mode seeds only
        // `join_keys`) and can bind one to a term that conflicts with the
        // parent's. SPARQL's natural join keeps a solution only when every
        // shared variable is compatible (equal, or unbound on either
        // side), so drop a row whose reconcile var is bound on BOTH sides
        // to different terms (normalized like the hash key). This check is
        // load-bearing in BOTH per-row and join mode — it is not a per-row
        // no-op.
        if !self.reconcile_vars.is_empty() {
            let (store, gv) = EqualityNorm::parts(&self.norm);
            let incompatible = self.reconcile_vars.iter().any(|v| {
                let parent = parent_batch.get(row_idx, *v);
                let sub = self.select_index.get(v).and_then(|&i| subquery_row(i));
                match (parent, sub) {
                    (Some(p), Some(s))
                        if !matches!(p, Binding::Unbound | Binding::Poisoned)
                            && !matches!(s, Binding::Unbound | Binding::Poisoned) =>
                    {
                        binding_to_group_key_normalized(p, store, gv)
                            != binding_to_group_key_normalized(s, store, gv)
                    }
                    _ => false,
                }
            });
            if incompatible {
                return None;
            }
        }

        let mut merged_row = Vec::with_capacity(self.in_schema.len());

        // Copy parent bindings
        for var in self.child.schema() {
            let binding = parent_batch
                .get(row_idx, *var)
                .cloned()
                .unwrap_or(Binding::Unbound);
            merged_row.push(binding);
        }

        // Fill in any subquery-selected vars that already exist in the parent schema,
        // but are currently Unbound/Poisoned in the parent row (non-clobbering merge).
        for (parent_idx, var) in self.child.schema().iter().enumerate() {
            if matches!(merged_row[parent_idx], Binding::Unbound | Binding::Poisoned) {
                if let Some(&sel_idx) = self.select_index.get(var) {
                    if let Some(val) = subquery_row(sel_idx) {
                        if !matches!(val, Binding::Unbound | Binding::Poisoned) {
                            merged_row[parent_idx] = val.clone();
                        }
                    }
                }
            }
        }

        // Append new vars introduced by the subquery select list, preserving select order.
        for var in &self.new_vars {
            let binding = self
                .select_index
                .get(var)
                .and_then(|&idx| subquery_row(idx))
                .cloned()
                .unwrap_or(Binding::Unbound);
            merged_row.push(binding);
        }

        Some(merged_row)
    }

    /// Seed only the inputs admitted by the existing correlation rules.
    fn seed_for_row(&self, parent_batch: &Batch, row_idx: usize) -> BoxedOperator {
        // Build seed from parent row (for correlated execution). Seed ONLY the
        // `join_keys` (self-produced correlation vars): seeding such a var merely
        // filters the subquery's own output to that value, which is join-equivalent
        // (SPARQL §18.2 evaluate-independently-then-join). A `reconcile_vars` member
        // is bound only conditionally inside the subquery (OPTIONAL/UNION), so
        // seeding it would PIN it to the parent value and defeat the natural join —
        // it must instead be produced independently and reconciled at merge time
        // (Family B / W3C join-scope-1). When every correlation var is a join key
        // (the common case, incl. BSBM), this is byte-identical to seeding them all.
        let seed_schema: Vec<VarId> = self.join_keys.clone();
        let seed_row: Vec<Binding> = self
            .join_keys
            .iter()
            .map(|var| {
                parent_batch
                    .get(row_idx, *var)
                    .cloned()
                    .unwrap_or(Binding::Unbound)
            })
            .collect();

        let seed: BoxedOperator = if seed_schema.is_empty() {
            Box::new(EmptyOperator::new())
        } else {
            let schema = Arc::from(seed_schema.into_boxed_slice());
            Box::new(SeedOperator::from_row(schema, seed_row))
        };

        seed
    }

    /// Evaluate the subquery ONCE with an empty seed and index its result rows
    /// by their correlation-variable values, for hash-join probing in join-mode.
    /// An empty correlation set produces a single bucket (broadcast).
    async fn materialize(&self, ctx: &ExecutionContext<'_>) -> Result<MaterializedSubquery> {
        let mut operator = self.build_inner_plan(Box::new(EmptyOperator::new()))?;
        let mut mat = MaterializedSubquery {
            rows: Vec::new(),
            index: IndexMap::new(),
            _memory: MemoryCharge::new(ctx),
        };
        let result: Result<()> = async {
            operator.open(ctx).await?;
            while let Some(batch) = operator.next_batch(ctx).await? {
                let batch = RetainedBatch::new(batch, ctx)?;
                // Binding payload plus row slots; allow geometric growth
                // of the outer row vector. Like the
                // other query budget estimates this excludes owned value heaps.
                mat._memory.add(
                    ctx,
                    batch.len().saturating_mul(
                        self.subquery.select.len() * crate::context::BINDING_EST_BYTES
                            + 2 * std::mem::size_of::<Vec<Binding>>(),
                    ),
                )?;
                for i in 0..batch.len() {
                    if i % SUBQUERY_BATCH_SIZE == 0 {
                        ctx.check_cancelled()?;
                    }
                    mat.rows.push(
                        self.subquery
                            .select
                            .iter()
                            .map(|v| batch.get(i, *v).cloned().unwrap_or(Binding::Unbound))
                            .collect(),
                    );
                }
            }
            Ok(())
        }
        .await;
        operator.close();
        drop(operator);
        result?;
        // Keep the original phase separation: inner join/aggregate state is
        // released before allocating the reusable result's index. Building it
        // during the drain would unnecessarily overlap both memory peaks.
        let mut index_bytes = 0usize;
        for (i, row) in mat.rows.iter().enumerate() {
            if i % SUBQUERY_BATCH_SIZE == 0 {
                mat._memory.add(ctx, index_bytes)?;
                index_bytes = 0;
            }
            let (store, gv) = EqualityNorm::parts(&self.norm);
            let key: Vec<_> = self
                .join_keys
                .iter()
                .map(|v| {
                    self.select_index
                        .get(v)
                        .and_then(|&col| row.get(col))
                        .map(|b| binding_to_group_key_normalized(b, store, gv))
                        .unwrap_or(GroupKeyOwned::Absent)
                })
                .collect();
            let indices = mat.index.entry(key).or_insert_with(|| {
                index_bytes += 2
                    * (std::mem::size_of::<Vec<GroupKeyOwned>>()
                        + std::mem::size_of::<Vec<usize>>()
                        + 3 * std::mem::size_of::<usize>())
                    + self.join_keys.len() * std::mem::size_of::<GroupKeyOwned>();
                Vec::new()
            });
            index_bytes += 2 * std::mem::size_of::<usize>();
            indices.push(i);
        }
        mat._memory.add(ctx, index_bytes)?;
        Ok(mat)
    }

    /// Build the subquery's inner operator tree for `EXPLAIN` (build-only — no
    /// `open()`/exec). Mirrors [`build_inner_plan`](Self::build_inner_plan)'s
    /// construction with no execution.
    ///
    /// The seed must match the path that actually runs, because the seed's schema
    /// is the inner `reorder_patterns`' initial bound set AND the child every nested
    /// subquery sees (a `SeedOperator`'s 1-row estimate flips their cardinality-guard
    /// `join_mode` to per-row). `join_mode` evaluates the body ONCE with an empty
    /// seed (`materialize`); per-row seeds the `join_keys` — NOT the reconcile vars,
    /// which are produced independently and reconciled at merge (see
    /// `seed_for_row`). An uncorrelated subquery has no seed either way.
    fn build_inner_plan_for_explain(&self) -> Result<BoxedOperator> {
        let seed: BoxedOperator = if self.join_mode || self.join_keys.is_empty() {
            Box::new(EmptyOperator::new())
        } else {
            let seed_schema: Arc<[VarId]> = Arc::from(self.join_keys.clone().into_boxed_slice());
            let seed_row = vec![Binding::Unbound; self.join_keys.len()];
            Box::new(SeedOperator::from_row(seed_schema, seed_row))
        };

        self.build_inner_plan(seed)
    }

    /// Preserve the scope and the complete modifier tail in every execution
    /// mode. Streaming does not push an outer seed or LIMIT through this tree.
    fn build_inner_plan(&self, seed: BoxedOperator) -> Result<BoxedOperator> {
        let where_op = build_where_operators_seeded(
            Some(seed),
            &self.subquery.patterns,
            self.stats.clone(),
            None,
            &self.planning,
        )?;
        let select_vars =
            (!self.subquery.select.is_empty()).then_some(self.subquery.select.as_slice());
        crate::execute::operator_tree::apply_solution_modifiers(
            where_op,
            self.subquery.grouping.as_ref(),
            &self.subquery.order_binds,
            &self.subquery.ordering,
            select_vars,
            self.subquery.distinct,
            self.subquery.offset,
            self.subquery.limit,
            false,
            None,
            &self.planning,
        )
    }
}

/// Minimum estimated parent (driving) rows above which evaluating a self-keyed
/// subquery once + hash-join beats per-row seeding. Below this the per-row path
/// (with its pruning seed) can be cheaper, so it is kept — the pre-existing
/// behavior, which this never regresses below. An unknown parent size defaults
/// to materialize: the per-row operator-rebuild overhead, paid once per parent
/// row, is the larger risk.
const SUBQUERY_MATERIALIZE_MIN_PARENT_ROWS: usize = 8;

/// Variables bound in *every* solution of `patterns` — produced by a top-level
/// required pattern (triple / property path), or exported by a slice-free nested
/// sub-SELECT whose own body always-binds them. NOT vars bound only inside a
/// `UNION` branch or `OPTIONAL` (conditional), nor a sub-SELECT's pass-throughs.
/// These are the only correlation variables safe to use as evaluate-once
/// hash-join keys. Also used by `GraphOperator` to decide whether seeding the
/// graph variable into its inner subplan is join-equivalent (issue #1442).
pub(crate) fn self_produced_vars(patterns: &[Pattern]) -> HashSet<VarId> {
    let mut produced: HashSet<VarId> = HashSet::new();
    for p in patterns {
        match p {
            Pattern::Triple(_) | Pattern::PropertyPath(_) => {
                produced.extend(p.produced_vars());
            }
            // A nested sub-SELECT binds the SELECT vars its own body always
            // binds, so those are always-bound here too. A slice would make
            // per-row seeding of such a var result-sensitive, so skip it then.
            Pattern::Subquery(sq) if sq.limit.is_none() && sq.offset.is_none() => {
                let inner = self_produced_vars(&sq.patterns);
                produced.extend(sq.select.iter().copied().filter(|v| inner.contains(v)));
            }
            _ => {}
        }
    }
    produced
}

/// All variables referenced anywhere in `patterns` (filters, binds, unions,
/// optionals, nested subquery bodies). A correlation variable that is referenced
/// but not produced is a genuine per-row input, not an omittable pass-through.
fn referenced_vars_set(patterns: &[Pattern]) -> HashSet<VarId> {
    patterns.iter().flat_map(Pattern::referenced_vars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Binding;
    use crate::ir::SubqueryPattern;
    use crate::seed::SeedOperator;
    use crate::var_registry::VarId;

    #[tokio::test]
    async fn materialized_join_matches_partial_keys_without_reviving_poisoned_rows() {
        use crate::group_aggregate::binding_to_group_key_owned;
        use crate::ir::FlakeValue;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{LedgerSnapshot, Sid};

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let lit = |n| Binding::lit(FlakeValue::Long(n), Sid::xsd_integer());
        let keys: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)]);
        let rows = vec![
            vec![lit(1), lit(10), lit(100)],
            vec![lit(1), lit(10), lit(101)],
            vec![lit(2), lit(20), lit(200)],
        ];
        let mut index: IndexMap<Vec<GroupKeyOwned>, Vec<usize>> = IndexMap::new();
        for (i, row) in rows.iter().enumerate() {
            index
                .entry(row[..2].iter().map(binding_to_group_key_owned).collect())
                .or_default()
                .push(i);
        }
        let child = SeedOperator::from_row(keys.clone(), vec![Binding::Unbound; 2]);
        let mut op = SubqueryOperator::new(
            Box::new(child),
            SubqueryPattern::new(vec![VarId(0), VarId(1), VarId(2)], vec![]),
            None,
            PlanningContext::current(),
        );
        // Pin the executor path: planner reordering must not hide this case.
        op.join_mode = true;
        op.join_keys = keys.to_vec();
        op.reconcile_vars.clear();
        op.materialized = Some(MaterializedSubquery {
            rows: rows.clone(),
            _memory: MemoryCharge::new(&ctx),
            index,
        });
        let parent = Batch::new(
            keys.clone(),
            vec![
                vec![
                    Binding::Poisoned,
                    Binding::Unbound,
                    Binding::Unbound,
                    lit(1),
                    lit(1),
                    Binding::Unbound,
                ],
                vec![
                    Binding::Unbound,
                    Binding::Poisoned,
                    lit(10),
                    Binding::Unbound,
                    lit(20),
                    Binding::Unbound,
                ],
            ],
        )
        .unwrap();
        op.parent_batch = Some(RetainedBatch::new(parent, &ctx).unwrap());
        let result = op.next_bounded_batch(&ctx).await.unwrap().unwrap();
        let result_rows: Vec<_> = (0..result.len())
            .map(|i| result.row_view(i).unwrap().to_vec())
            .collect();
        assert_eq!(result_rows.len(), 7);
        for (row, copies) in rows.iter().zip([3, 3, 1]) {
            assert_eq!(result_rows.iter().filter(|r| *r == row).count(), copies);
        }

        // Reuse the same materialization for a subsequent fully bound batch;
        // both inner rows sharing its key must survive the exact lookup.
        let parent = Batch::new(keys, vec![vec![lit(1)], vec![lit(10)]]).unwrap();
        op.parent_batch = Some(RetainedBatch::new(parent, &ctx).unwrap());
        op.parent_row = 0;
        let result = op.next_bounded_batch(&ctx).await.unwrap().unwrap();
        let result_rows: Vec<_> = (0..result.len())
            .map(|i| result.row_view(i).unwrap().to_vec())
            .collect();
        assert_eq!(&result_rows, &rows[..2]);
    }

    fn values_subquery(rows: usize, vars: Vec<VarId>) -> SubqueryPattern {
        use fluree_db_core::{FlakeValue, Sid};
        SubqueryPattern::new(
            vars.clone(),
            vec![Pattern::Values {
                vars: vars.clone(),
                rows: (0..rows)
                    .map(|i| {
                        vars.iter()
                            .map(|_| Binding::lit(FlakeValue::Long(i as i64), Sid::xsd_integer()))
                            .collect()
                    })
                    .collect(),
            }],
        )
        .with_uncorrelated()
    }

    #[tokio::test]
    async fn identity_scope_streams_and_close_discards_pending_input() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let mut op = SubqueryOperator::new(
            Box::new(EmptyOperator::new()),
            values_subquery(3 * SUBQUERY_BATCH_SIZE + 7, vec![VarId(0)]),
            None,
            PlanningContext::current(),
        );
        assert!(op.stream_identity);
        op.open(&ctx).await.unwrap();
        let first = op.next_batch(&ctx).await.unwrap().unwrap();
        assert_eq!(first.len(), SUBQUERY_BATCH_SIZE);
        assert!(op.materialized.is_none());
        assert!(
            op.inner.is_some(),
            "the body must remain resumable, not fully drained"
        );
        let mut count = first.len();
        while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
            assert!(batch.len() <= SUBQUERY_BATCH_SIZE);
            count += batch.len();
        }
        assert_eq!(count, 3 * SUBQUERY_BATCH_SIZE + 7);
        op.close();
        assert!(op.inner.is_none() && op.inner_batch.is_none() && op.parent_batch.is_none());

        let mut op = SubqueryOperator::new(
            Box::new(EmptyOperator::new()),
            values_subquery(3 * SUBQUERY_BATCH_SIZE, vec![VarId(0)]),
            None,
            PlanningContext::current(),
        );
        op.open(&ctx).await.unwrap();
        op.next_batch(&ctx).await.unwrap().unwrap();
        op.close();
        assert!(op.inner.is_none() && op.inner_batch.is_none());
        assert!(op.next_batch(&ctx).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn broadcast_batches_preserve_empty_schema_multiplicity() {
        use crate::seed::BatchSeedOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        for parents in [0, 1, 3] {
            let child = BatchSeedOperator::from_batch(Batch::empty_schema_with_len(parents));
            let mut subquery = values_subquery(SUBQUERY_BATCH_SIZE + 7, vec![VarId(99)]);
            subquery.select.clear(); // project away the inner variable, keeping its multiplicity
            let mut op =
                SubqueryOperator::new(Box::new(child), subquery, None, PlanningContext::current());
            // A zero-column source is not necessarily the identity solution.
            assert!(!op.stream_identity);
            op.open(&ctx).await.unwrap();
            let mut count = 0;
            while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
                assert!(batch.schema().is_empty());
                assert!(batch.len() <= SUBQUERY_BATCH_SIZE);
                count += batch.len();
            }
            assert_eq!(count, parents * (SUBQUERY_BATCH_SIZE + 7));
            op.close();
        }
    }

    #[tokio::test]
    async fn materialized_duplicate_bucket_resumes_without_copying_matches() {
        use crate::group_aggregate::binding_to_group_key_owned;
        use crate::seed::BatchSeedOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{FlakeValue, LedgerSnapshot, Sid};
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let lit = |i| Binding::lit(FlakeValue::Long(i), Sid::xsd_integer());
        let n = 2 * SUBQUERY_BATCH_SIZE + 3;
        for wildcard in [false, true] {
            let parent = Batch::new(
                Arc::from(vec![VarId(0), VarId(1)]),
                vec![
                    vec![lit(1), if wildcard { Binding::Unbound } else { lit(1) }],
                    vec![Binding::Unbound, lit(2)],
                ],
            )
            .unwrap();
            let mut op = SubqueryOperator::new(
                Box::new(BatchSeedOperator::from_batch(parent)),
                values_subquery(0, vec![VarId(0), VarId(1), VarId(2)]),
                None,
                PlanningContext::current(),
            );
            op.join_mode = true;
            op.join_keys = vec![VarId(0), VarId(1)];
            op.reconcile_vars.clear();
            op.open(&ctx).await.unwrap();
            let rows: Vec<_> = (0..n)
                .map(|i| vec![lit(1), lit(2), lit(i as i64)])
                .collect();
            let mut index = IndexMap::new();
            index.insert(
                rows[0][..2]
                    .iter()
                    .map(binding_to_group_key_owned)
                    .collect(),
                (0..n).collect(),
            );
            op.materialized = Some(MaterializedSubquery {
                rows,
                index,
                _memory: MemoryCharge::new(&ctx),
            });
            let mut result = Vec::new();
            while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
                assert!(batch.len() <= SUBQUERY_BATCH_SIZE);
                result.extend((0..batch.len()).map(|i| batch.get_by_col(i, 2).clone()));
            }
            let expected: Vec<_> = (0..n).chain(0..n).map(|i| lit(i as i64)).collect();
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn materialization_and_stream_buffers_enforce_memory_budget() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{LedgerSnapshot, QueryCancellation};
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        for identity in [false, true] {
            let cancel = QueryCancellation::new();
            // Reusable mode admits the input batch but not its retained copy.
            // Identity mode exercises the streaming input-buffer guard itself.
            cancel.set_memory_limit(if identity {
                1
            } else {
                100 * crate::context::BINDING_EST_BYTES + 1
            });
            let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancel);
            let child: BoxedOperator = if identity {
                Box::new(EmptyOperator::new())
            } else {
                Box::new(SeedOperator::from_row(Arc::from(vec![]), vec![]))
            };
            let mut op = SubqueryOperator::new(
                child,
                values_subquery(100, vec![VarId(0)]),
                None,
                PlanningContext::current(),
            );
            op.open(&ctx).await.unwrap();
            let error = op.next_batch(&ctx).await.unwrap_err();
            assert!(
                matches!(error, QueryError::MemoryBudgetExceeded { .. }),
                "{error:?}"
            );
            assert!(op.materialized.is_none() && op.inner.is_none() && op.parent_batch.is_none());
            assert_eq!(ctx.mem_used(), 0);
        }
    }

    #[tokio::test]
    async fn closing_or_cancelling_releases_only_owned_memory() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{LedgerSnapshot, QueryCancellation};
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        for identity in [false, true] {
            let cancellation = QueryCancellation::new();
            let ctx =
                ExecutionContext::new(&snapshot, &vars).with_cancellation(cancellation.clone());
            for cancel in [false, true] {
                let child: BoxedOperator = if identity {
                    Box::new(EmptyOperator::new())
                } else {
                    Box::new(SeedOperator::from_row(Arc::from(vec![]), vec![]))
                };
                let mut op = SubqueryOperator::new(
                    child,
                    values_subquery(3 * SUBQUERY_BATCH_SIZE, vec![VarId(0)]),
                    None,
                    PlanningContext::current(),
                );
                let baseline = ctx.mem_used();
                op.open(&ctx).await.unwrap();
                op.next_batch(&ctx).await.unwrap().unwrap();
                assert!(ctx.mem_used() > baseline);
                // Simulate another operator retaining memory between pulls.
                ctx.record_alloc(123);
                if cancel {
                    cancellation.cancel();
                    assert!(matches!(
                        op.next_batch(&ctx).await,
                        Err(QueryError::Cancelled { .. })
                    ));
                } else {
                    op.close();
                }
                assert_eq!(ctx.mem_used(), baseline + 123);
            }
        }
    }

    /// Verifies that correlation uses SELECT vars, not internal pattern vars.
    ///
    /// Scenario: parent schema has [?s, ?name], subquery SELECT is [?s, ?age],
    /// but subquery patterns also reference ?internal (not SELECTed).
    /// Correlation should be [?s] only — ?internal must NOT appear in
    /// correlation_vars even if it were somehow in the parent schema.
    #[test]
    fn correlation_uses_select_vars_not_pattern_vars() {
        let v_s = VarId(0);
        let v_name = VarId(1);
        let v_age = VarId(2);
        let v_internal = VarId(3);

        // Parent provides [?s, ?name]
        let parent_schema: Arc<[VarId]> = Arc::from(vec![v_s, v_name]);
        let child = SeedOperator::from_row(parent_schema, vec![Binding::Unbound, Binding::Unbound]);

        // Subquery SELECT [?s, ?age]; patterns also reference ?internal
        let subquery = SubqueryPattern::new(
            vec![v_s, v_age],
            vec![], // patterns don't matter for this structural test
        );

        let op = SubqueryOperator::new(
            Box::new(child),
            subquery,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );

        // ?s is in both parent schema and subquery SELECT → correlated
        assert_eq!(op.correlation_vars, vec![v_s]);

        // ?age is new (in subquery SELECT but not parent schema)
        assert_eq!(op.new_vars, vec![v_age]);

        // ?name is NOT in subquery SELECT → not correlated, not new
        assert!(!op.correlation_vars.contains(&v_name));

        // ?internal is NOT in subquery SELECT → never appears
        assert!(!op.correlation_vars.contains(&v_internal));
    }
}
