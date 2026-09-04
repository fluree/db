//! The SQL pushdown lane: one statement per `GRAPH <sql-source> { … }` block.
//!
//! [`SqlBlockOperator`] stands where the planner would put a `GraphOperator`
//! for a block whose shape the lowering could accept. Whether the graph is a
//! SQL-backed source, and whether the mapping and policy admit the block, is
//! only known at `open`, so the operator resolves then and otherwise builds
//! the ordinary `GraphOperator` and streams it — the same fallback discipline
//! as the fused aggregate. When it proceeds, bindings the outer query already
//! holds are sent into the statement as a key set (chunked to the provider's
//! limits), each returned page is joined to the outer rows in memory, and
//! residual filters run in the engine over the returned rows. A `UNION`
//! runs one statement per branch combination, each with its own residuals.

mod aggregate;
mod lower;
mod terms;

pub use aggregate::{
    detect_sql_block_aggregate, SqlAggregateOperator, SQL_AGGREGATE_PUSHDOWN_SITE,
};

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use fluree_db_tabular::plan::{
    ColRef, KeySet, Literal, OrderKey, Pred, RelNode, RelPlan, RelSource,
};
use fluree_db_tabular::BatchSchema;

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::eval::PreparedBoolExpression;
use crate::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::{GraphName, Pattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::r2rml::policy::R2rmlPolicyGate;
use crate::r2rml::{ColumnBatchStream, PushdownCapabilities};
use crate::sort::SortSpec;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;

use lower::{block_is_admissible, lower_block, Decline, LowerInput, Lowered};
use terms::{seed_values, Materializer};

/// Routing stamp site for `MustFire` / `MustNotFire` tests.
pub const SQL_BLOCK_PUSHDOWN_SITE: &str = "sql_block_pushdown";

/// `FLUREE_SQL_PUSHDOWN_LANE=0` keeps every SQL block on the per-scan lane.
pub(crate) fn sql_pushdown_lane_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_SQL_PUSHDOWN_LANE"))
}

/// Whether the planner should route this GRAPH block through the lane.
pub fn admits(name: &GraphName, patterns: &[Pattern]) -> bool {
    matches!(name, GraphName::Iri(_))
        && sql_pushdown_lane_enabled()
        && !crate::execute::operator_tree::fast_paths_disabled()
        && block_is_admissible(patterns)
}

pub struct SqlBlockOperator {
    child: Option<BoxedOperator>,
    graph_iri: Arc<str>,
    inner_patterns: Vec<Pattern>,
    planning: PlanningContext,
    schema: Arc<[VarId]>,
    state: OperatorState,
    row_budget: Option<usize>,
    topk: Option<(Vec<SortSpec>, usize)>,
    /// The variables a `DISTINCT` directly above reads.
    distinct: Option<Vec<VarId>>,
    /// After open: the source, or the fallback.
    chain: Option<BoxedOperator>,
}

impl SqlBlockOperator {
    pub fn new(
        child: BoxedOperator,
        graph_iri: Arc<str>,
        inner_patterns: Vec<Pattern>,
        planning: PlanningContext,
    ) -> Self {
        let mut schema: Vec<VarId> = child.schema().to_vec();
        for p in &inner_patterns {
            for v in p.produced_vars() {
                if !schema.contains(&v) {
                    schema.push(v);
                }
            }
        }
        Self {
            child: Some(child),
            graph_iri,
            inner_patterns,
            planning,
            schema: schema.into(),
            state: OperatorState::Created,
            row_budget: None,
            topk: None,
            distinct: None,
            chain: None,
        }
    }

    fn fallback(&mut self, child: BoxedOperator) -> BoxedOperator {
        let mut op = crate::graph::GraphOperator::new(
            child,
            GraphName::Iri(Arc::clone(&self.graph_iri)),
            self.inner_patterns.clone(),
            self.planning,
        );
        if let Some(b) = self.row_budget {
            op.set_row_budget(b);
        }
        if let Some((ordering, k)) = &self.topk {
            op.set_topk(ordering, *k);
        }
        Box::new(op)
    }

    async fn resolve(
        &self,
        ctx: &ExecutionContext<'_>,
        child_vars: &[VarId],
    ) -> Result<Option<Resolved>> {
        resolve_block(
            ctx,
            &self.graph_iri,
            &self.inner_patterns,
            child_vars,
            self.distinct.as_deref(),
        )
        .await
    }
}

/// Everything that must be true of the graph, the provider, the mapping and
/// the policy before a block is lowered; `None` means fall back.
pub(super) async fn resolve_block(
    ctx: &ExecutionContext<'_>,
    graph_iri: &Arc<str>,
    patterns: &[Pattern],
    child_vars: &[VarId],
    projection: Option<&[VarId]>,
) -> Result<Option<Resolved>> {
    let iri = graph_iri;
    match ctx.dataset {
        Some(ds) => {
            if !ds.has_named_graph(iri) {
                return Ok(None);
            }
        }
        None => {
            // A ledger's own named graph is never a source. The source's
            // own id can equal the view's ledger id (a `query_from` on the
            // source), so that is not excluded; the capability check below
            // decides.
            if ctx.single_db_user_graph_id(iri).is_some() {
                return Ok(None);
            }
        }
    }
    let (Some(provider), Some(table_provider)) = (ctx.r2rml_provider, ctx.r2rml_table_provider)
    else {
        return Ok(None);
    };
    let Some(caps) = table_provider.pushdown_capabilities(iri).await? else {
        return Ok(None);
    };
    let as_of_t = if ctx.dataset.is_some() {
        None
    } else {
        Some(ctx.to_t)
    };
    let mapping = provider.compiled_mapping(iri, as_of_t).await?;
    let graph_ctx = ctx.with_active_graph(Arc::clone(iri));
    let verdicts = match R2rmlPolicyGate::build(&graph_ctx, &mapping, iri) {
        None => None,
        Some(mut gate) => match gate.static_verdicts(&graph_ctx, &mapping).await? {
            Some(v) => Some(v),
            None => {
                tracing::debug!(graph = %iri, "sql pushdown declined: policy is not static");
                return Ok(None);
            }
        },
    };
    let mut verdict =
        |tm: &fluree_db_r2rml::mapping::TriplesMap, pred: &str| -> Result<Option<bool>> {
            Ok(match &verdicts {
                None => Some(true),
                Some(v) => v.get(&(tm.iri.clone(), pred.to_string())).copied(),
            })
        };
    let mut schemas: HashMap<RelSource, Arc<BatchSchema>> = HashMap::new();
    for src in lower::candidate_sources(patterns, ctx.active_snapshot, &mapping) {
        if let Some(schema) = table_provider.source_schema(iri, &src).await? {
            schemas.insert(src, schema);
        }
    }
    let lowered = lower_block(LowerInput {
        patterns,
        mapping: &mapping,
        snapshot: ctx.active_snapshot,
        caps: &caps,
        child_vars,
        policy: verdicts.is_some().then_some(&mut verdict),
        schemas: &schemas,
        projection,
    })?;
    let lowered = match lowered {
        Err(Decline(why)) => {
            tracing::debug!(graph = %iri, why, "sql pushdown declined");
            return Ok(None);
        }
        Ok(l) => l,
    };
    let mut branches = Vec::with_capacity(lowered.len());
    for lowered in lowered {
        tracing::debug!(
            graph = %iri,
            seeds = lowered.seeds.len(),
            child_vars = ?child_vars,
            block_vars = ?lowered.block_vars,
            distinct = lowered.distinct,
            "sql pushdown lowered"
        );
        let materializer = Materializer::new(&lowered, &mapping, ctx.active_snapshot)?;
        let residuals = lowered
            .residual_filters
            .iter()
            .map(|f| PreparedBoolExpression::new(f.clone()))
            .collect();
        branches.push(Branch {
            lowered,
            materializer,
            residuals,
        });
    }
    Ok(Some(Resolved {
        branches,
        caps,
        mapping,
        schemas,
    }))
}

/// One statement of a resolved block: a `UNION` contributes one per branch
/// combination, everything else exactly one.
pub(super) struct Branch {
    pub lowered: Lowered,
    pub materializer: Materializer,
    /// The branch's residual filters, run over its rows in the engine.
    residuals: Vec<PreparedBoolExpression>,
}

pub(super) struct Resolved {
    /// Empty: the block provably yields no rows.
    pub branches: Vec<Branch>,
    pub caps: PushdownCapabilities,
    pub mapping: Arc<CompiledR2rmlMapping>,
    /// Probed schemas of the relations the block can reach.
    pub schemas: HashMap<RelSource, Arc<BatchSchema>>,
}

#[async_trait::async_trait]
impl Operator for SqlBlockOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn set_row_budget(&mut self, budget: usize) {
        self.row_budget = Some(budget);
    }

    fn set_topk(&mut self, ordering: &[SortSpec], k: usize) {
        self.topk = Some((ordering.to_vec(), k));
    }

    fn set_distinct(&mut self, vars: &[VarId]) {
        self.distinct = Some(vars.to_vec());
    }

    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        match (&self.chain, &self.child) {
            (Some(c), _) | (None, Some(c)) => vec![crate::plan_node::PlanChild::child(c.as_ref())],
            _ => Vec::new(),
        }
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("graph".into(), self.graph_iri.to_string().into());
        m.insert("lane".into(), "sql_block_pushdown".into());
        m.insert(
            "note".into(),
            "resolved at open: falls back to GraphOperator unless the source is SQL and the block lowers".into(),
        );
        m
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let child = self
            .child
            .take()
            .ok_or_else(|| QueryError::Internal("SqlBlockOperator opened twice".into()))?;
        let child_vars: Vec<VarId> = child.schema().to_vec();
        let resolved = self.resolve(ctx, &child_vars).await?;
        let mut chain: BoxedOperator = match resolved {
            None => {
                stamp_fast_path(
                    SQL_BLOCK_PUSHDOWN_SITE,
                    FastPathOutcome::Fallback(FastPathFallback::GateDeclined),
                );
                self.fallback(child)
            }
            Some(resolved) => {
                stamp_fast_path(SQL_BLOCK_PUSHDOWN_SITE, FastPathOutcome::Proceed);
                Box::new(SqlBlockSource::new(
                    child,
                    Arc::clone(&self.graph_iri),
                    Arc::clone(&self.schema),
                    resolved,
                    self.row_budget,
                    self.topk.clone(),
                ))
            }
        };
        chain.open(ctx).await?;
        self.chain = Some(chain);
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        let Some(chain) = self.chain.as_mut() else {
            return Ok(None);
        };
        let Some(batch) = chain.next_batch(ctx).await? else {
            return Ok(None);
        };
        if batch.schema() != self.schema.as_ref() {
            // Both the lane and the fallback `GraphOperator` declare their
            // schema in pattern order, so this cannot differ. Permuting here
            // instead would rebuild every batch of every declined block — the
            // common path on a native ledger, where `admits` is shape-only — and
            // the fallback has to be free for the wrap to be acceptable there.
            return Err(QueryError::Internal(format!(
                "sql lane fallback changed the block's schema: declared {:?}, got {:?}",
                self.schema.as_ref(),
                batch.schema()
            )));
        }
        Ok(Some(batch))
    }

    fn close(&mut self) {
        if let Some(c) = self.chain.as_mut() {
            c.close();
        }
        if let Some(c) = self.child.as_mut() {
            c.close();
        }
        self.state = OperatorState::Closed;
    }
}

/// How one child batch joins the rows a statement returns.
enum JoinPlan {
    Cross,
    Hash {
        join_vars: Vec<VarId>,
        full_index: HashMap<Vec<GroupKeyOwned>, Vec<usize>>,
        partial_rows: Vec<(usize, Vec<Option<GroupKeyOwned>>)>,
    },
}

struct InFlight {
    child_batch: Batch,
    /// One join plan per branch (a branch binds its own subset of the
    /// shared variables).
    joins: Vec<JoinPlan>,
    /// `(branch, key-set chunk)` statements still to run.
    chunks: VecDeque<(usize, Option<KeySet>)>,
    stream: Option<(usize, ColumnBatchStream)>,
}

/// The source proper: executes each branch's plan per child batch and
/// merges rows.
struct SqlBlockSource {
    child: BoxedOperator,
    graph_iri: Arc<str>,
    schema: Arc<[VarId]>,
    resolved: Resolved,
    row_budget: Option<usize>,
    topk: Option<(Vec<SortSpec>, usize)>,
    state: OperatorState,
    /// Rows waiting to be batched, tagged with the branch that produced
    /// them so the branch's residual filters run over a homogeneous batch.
    pending: VecDeque<(usize, Vec<Binding>)>,
    inflight: Option<InFlight>,
    child_done: bool,
    out_pos: HashMap<VarId, usize>,
}

impl SqlBlockSource {
    fn new(
        child: BoxedOperator,
        graph_iri: Arc<str>,
        schema: Arc<[VarId]>,
        resolved: Resolved,
        row_budget: Option<usize>,
        topk: Option<(Vec<SortSpec>, usize)>,
    ) -> Self {
        let out_pos = schema.iter().enumerate().map(|(i, v)| (*v, i)).collect();
        Self {
            child,
            graph_iri,
            schema,
            resolved,
            row_budget,
            topk,
            state: OperatorState::Created,
            pending: VecDeque::new(),
            inflight: None,
            child_done: false,
            out_pos,
        }
    }

    fn lowered(&self, branch: usize) -> &Lowered {
        &self.resolved.branches[branch].lowered
    }

    /// The statement for one branch and key-set chunk (or none), with the
    /// modifiers the engine's LIMIT / top-k channels allow.
    fn plan_for(&self, branch: usize, keyset: Option<KeySet>) -> RelPlan {
        let lowered = self.lowered(branch);
        let mut root = lowered.root.clone();
        if let Some(ks) = keyset {
            let on: Vec<Pred> = lowered
                .seeds
                .iter()
                .flat_map(|s| match &s.shape {
                    lower::KeyShape::Template { cols, .. } => cols.clone(),
                    lower::KeyShape::Column { col, .. } => vec![col.clone()],
                })
                .enumerate()
                .map(|(i, col)| Pred::ColEq {
                    left: ColRef::new(&ks.alias, &ks.columns[i].0),
                    right: col,
                })
                .collect();
            root = RelNode::Join {
                left: Box::new(root),
                right: Box::new(RelNode::KeySet(ks)),
                on: Pred::and(on).expect("a key set has columns"),
            };
        }
        let mut order_by = Vec::new();
        let mut limit = None;
        if lowered.limit_is_exact {
            match &self.topk {
                // The statement answers exactly k rows, so the LIMIT goes only
                // with the whole ORDER BY: a prefix of the keys would pick a
                // different k among ties on the primary key.
                Some((ordering, k)) => {
                    let keys: Option<Vec<(OrderKey, bool)>> = ordering
                        .iter()
                        .map(|s| {
                            let (col, _) = lowered.order_columns.get(&s.var)?;
                            Some((OrderKey::Col(col.clone()), s.ascending()))
                        })
                        .collect();
                    if let Some(keys) = keys {
                        order_by = keys;
                        limit = Some(*k as u64);
                    }
                }
                None => {
                    if let Some(b) = self.row_budget {
                        limit = Some(b as u64);
                    }
                }
            }
        }
        RelPlan {
            root,
            output: lowered.outputs.clone(),
            group_by: Vec::new(),
            distinct: lowered.distinct,
            order_by,
            limit,
        }
    }

    /// Key-set chunks for a child batch on one branch: `[None]` when nothing
    /// can be seeded (the statement runs once, unconstrained, and the
    /// in-memory join does the rest), empty when no outer row can match.
    fn keysets_for(
        &self,
        branch: usize,
        child_batch: &Batch,
        ctx: &ExecutionContext<'_>,
    ) -> Vec<Option<KeySet>> {
        let lowered = self.lowered(branch);
        if lowered.seeds.is_empty() {
            return vec![None];
        }
        let gv = ctx.graph_view();
        let child_schema = child_batch.schema();
        let mut rows: Vec<Vec<Literal>> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for row_idx in 0..child_batch.len() {
            let mut key: Vec<Literal> = Vec::new();
            for seed in &lowered.seeds {
                let Some(pos) = child_schema.iter().position(|v| *v == seed.var) else {
                    return vec![None];
                };
                let b = &child_batch.column_by_idx(pos).unwrap()[row_idx];
                if !b.is_bound() {
                    // An unbound outer value joins with everything: no seeding.
                    tracing::debug!(var = ?seed.var, "sql pushdown: unbound outer value, no key set");
                    return vec![None];
                }
                let materialized = if crate::object_binding::is_numbig_encoded(b)
                    || matches!(b, Binding::EncodedLit { .. })
                {
                    crate::group_aggregate::materialize_encoded(b, gv.as_ref())
                } else {
                    b.clone()
                };
                match seed_values(&materialized, &seed.shape, Some(ctx.active_snapshot)) {
                    Some(vals) => key.extend(vals),
                    // This outer row can match nothing; leave it out.
                    None => {
                        key.clear();
                        break;
                    }
                }
            }
            if key.is_empty() {
                continue;
            }
            if seen.insert(format!("{key:?}")) {
                rows.push(key);
            }
        }
        if rows.is_empty() {
            return Vec::new();
        }
        let width = rows[0].len();
        let columns: Vec<(String, Option<fluree_db_tabular::FieldType>)> =
            (0..width).map(|i| (format!("k{i}"), None)).collect();
        let max_rows = self.resolved.caps.keyset_max_rows.max(1);
        let byte_budget = self.resolved.caps.statement_max_bytes / 2;
        let mut chunks = Vec::new();
        let mut current: Vec<Vec<Literal>> = Vec::new();
        let mut bytes = 0usize;
        for row in rows {
            let row_bytes: usize = row.iter().map(|l| literal_len(l) + 4).sum();
            if !current.is_empty() && (current.len() >= max_rows || bytes + row_bytes > byte_budget)
            {
                chunks.push(Some(KeySet {
                    alias: "k".into(),
                    columns: columns.clone(),
                    rows: std::mem::take(&mut current),
                }));
                bytes = 0;
            }
            bytes += row_bytes;
            current.push(row);
        }
        if !current.is_empty() {
            chunks.push(Some(KeySet {
                alias: "k".into(),
                columns,
                rows: current,
            }));
        }
        chunks
    }

    fn build_join_plan(
        &self,
        branch: usize,
        child_batch: &Batch,
        ctx: &ExecutionContext<'_>,
    ) -> JoinPlan {
        let child_schema = child_batch.schema();
        let join_vars: Vec<VarId> = child_schema
            .iter()
            .copied()
            .filter(|v| self.lowered(branch).block_vars.contains(v))
            .collect();
        if join_vars.is_empty() {
            return JoinPlan::Cross;
        }
        let mut full_index: HashMap<Vec<GroupKeyOwned>, Vec<usize>> = HashMap::new();
        let mut partial_rows: Vec<(usize, Vec<Option<GroupKeyOwned>>)> = Vec::new();
        for row_idx in 0..child_batch.len() {
            let mut key: Vec<Option<GroupKeyOwned>> = Vec::with_capacity(join_vars.len());
            let mut all_bound = true;
            let mut poisoned = false;
            for &jv in &join_vars {
                let pos = child_schema.iter().position(|&v| v == jv).unwrap();
                let b = &child_batch.column_by_idx(pos).unwrap()[row_idx];
                if b.is_poisoned() {
                    poisoned = true;
                    break;
                }
                if b.is_bound() {
                    key.push(Some(join_key(b, ctx)));
                } else {
                    all_bound = false;
                    key.push(None);
                }
            }
            if poisoned {
                continue;
            }
            if all_bound {
                full_index
                    .entry(key.into_iter().map(Option::unwrap).collect())
                    .or_default()
                    .push(row_idx);
            } else {
                partial_rows.push((row_idx, key));
            }
        }
        JoinPlan::Hash {
            join_vars,
            full_index,
            partial_rows,
        }
    }

    fn emit_row(
        &mut self,
        branch: usize,
        child_batch: &Batch,
        child_row_idx: usize,
        prod: &[(VarId, Binding)],
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        ctx.tracker.consume_fuel(1)?;
        let mut row: Vec<Binding> = vec![Binding::Unbound; self.schema.len()];
        for (col_idx, var) in child_batch.schema().iter().enumerate() {
            if let Some(&pos) = self.out_pos.get(var) {
                row[pos] = child_batch.column_by_idx(col_idx).unwrap()[child_row_idx].clone();
            }
        }
        for (var, b) in prod {
            if let Some(&pos) = self.out_pos.get(var) {
                row[pos] = b.clone();
            }
        }
        self.pending.push_back((branch, row));
        Ok(())
    }

    fn join_and_emit(
        &mut self,
        branch: usize,
        child_batch: &Batch,
        join: &JoinPlan,
        prod: &[(VarId, Binding)],
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        match join {
            JoinPlan::Cross => {
                for child_row_idx in 0..child_batch.len() {
                    self.emit_row(branch, child_batch, child_row_idx, prod, ctx)?;
                }
            }
            JoinPlan::Hash {
                join_vars,
                full_index,
                partial_rows,
            } => {
                let pkey: Vec<GroupKeyOwned> = join_vars
                    .iter()
                    .filter_map(|jv| {
                        prod.iter()
                            .find(|(v, _)| v == jv)
                            .map(|(_, b)| join_key(b, ctx))
                    })
                    .collect();
                if pkey.len() != join_vars.len() {
                    return Ok(());
                }
                let mut matches: Vec<usize> = full_index.get(&pkey).cloned().unwrap_or_default();
                for (child_row_idx, partial) in partial_rows {
                    let agrees = partial
                        .iter()
                        .zip(pkey.iter())
                        .all(|(c, p)| c.as_ref().is_none_or(|c| c == p));
                    if agrees {
                        matches.push(*child_row_idx);
                    }
                }
                for child_row_idx in matches {
                    self.emit_row(branch, child_batch, child_row_idx, prod, ctx)?;
                }
            }
        }
        Ok(())
    }

    /// The next batch of pending rows: one branch's rows at a time, its
    /// residual filters applied. `all` flushes short runs.
    fn take_batch(&mut self, ctx: &ExecutionContext<'_>, all: bool) -> Result<Option<Batch>> {
        loop {
            let Some(&(branch, _)) = self.pending.front() else {
                return Ok(None);
            };
            if !all && self.pending.len() < ctx.batch_size {
                return Ok(None);
            }
            let n = self
                .pending
                .iter()
                .take(ctx.batch_size)
                .take_while(|(b, _)| *b == branch)
                .count();
            let mut columns: Vec<Vec<Binding>> = (0..self.schema.len())
                .map(|_| Vec::with_capacity(n))
                .collect();
            for _ in 0..n {
                let (_, row) = self.pending.pop_front().unwrap();
                for (i, b) in row.into_iter().enumerate() {
                    columns[i].push(b);
                }
            }
            let mut batch = Batch::new(Arc::clone(&self.schema), columns)
                .map_err(|e| QueryError::Internal(e.to_string()))?;
            let mut dropped = false;
            for f in &self.resolved.branches[branch].residuals {
                match crate::filter::filter_batch(&batch, f, &self.schema, ctx)? {
                    Some(kept) => batch = kept,
                    None => {
                        dropped = true;
                        break;
                    }
                }
            }
            if dropped {
                continue;
            }
            let stamp_ledger_id = match ctx.dataset {
                Some(ds) if ds.spans_multiple_ledgers() => ds
                    .named_graph(self.graph_iri.as_ref())
                    .map(|g| Arc::clone(&g.ledger_id)),
                _ => None,
            };
            return Ok(Some(match stamp_ledger_id {
                Some(ledger_id) => {
                    crate::dataset_operator::stamp_provenance(batch, &ledger_id, ctx)?
                }
                None => batch,
            }));
        }
    }
}

/// A join key that meets across representations: an IRI in any form (a
/// ledger `Sid`, an `IriMatch`, a raw `Iri`) keys by its IRI string, so an
/// outer ledger value joins the block's raw IRI; anything else uses the
/// engine's normalized key.
fn join_key(b: &Binding, ctx: &ExecutionContext<'_>) -> GroupKeyOwned {
    match terms::iri_of_binding(b, Some(ctx.active_snapshot)) {
        Some(iri) => GroupKeyOwned::MaterializedSid(0, iri.into()),
        None => {
            let gv = ctx.graph_view();
            binding_to_group_key_normalized(b, ctx.binary_store.as_deref(), gv.as_ref())
        }
    }
}

fn literal_len(l: &Literal) -> usize {
    match l {
        Literal::Str(s) | Literal::TemplateKey(s) => s.len() + 2,
        Literal::Set(items) => items.iter().map(literal_len).sum(),
        _ => 24,
    }
}

#[async_trait::async_trait]
impl Operator for SqlBlockSource {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("graph".into(), self.graph_iri.to_string().into());
        m.insert("lane".into(), "sql_block_pushdown".into());
        m.insert("statements".into(), self.resolved.branches.len().into());
        m
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state == OperatorState::Exhausted {
            return Ok(None);
        }
        if self.resolved.branches.is_empty() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        let graph_ctx = ctx.with_active_graph(Arc::clone(&self.graph_iri));
        loop {
            graph_ctx.checkpoint()?;
            if let Some(batch) = self.take_batch(&graph_ctx, false)? {
                return Ok(Some(batch));
            }
            if let Some(mut inflight) = self.inflight.take() {
                if let Some((branch, stream)) = inflight.stream.as_mut() {
                    use futures::StreamExt;
                    match stream.next().await {
                        Some(page) => {
                            let page = page?;
                            let branch = *branch;
                            let b = &self.resolved.branches[branch];
                            let batches = b.materializer.split_page(page, &b.lowered.outputs)?;
                            let num_rows = batches.values().next().map(|b| b.num_rows).unwrap_or(0);
                            let mut rows = Vec::with_capacity(num_rows);
                            for i in 0..num_rows {
                                rows.push(b.materializer.row(&batches, i)?);
                            }
                            for prod in &rows {
                                self.join_and_emit(
                                    branch,
                                    &inflight.child_batch,
                                    &inflight.joins[branch],
                                    prod,
                                    &graph_ctx,
                                )?;
                            }
                            self.inflight = Some(inflight);
                            continue;
                        }
                        None => {
                            inflight.stream = None;
                        }
                    }
                }
                if let Some((branch, keyset)) = inflight.chunks.pop_front() {
                    let plan = self.plan_for(branch, keyset);
                    let table_provider = graph_ctx.r2rml_table_provider.ok_or_else(|| {
                        QueryError::InvalidQuery("R2RML table provider not configured".into())
                    })?;
                    let (sql, stream) = table_provider.execute_plan(&self.graph_iri, &plan).await?;
                    graph_ctx.tracker.record_statement(&self.graph_iri, &sql);
                    inflight.stream = Some((branch, stream));
                    self.inflight = Some(inflight);
                }
                continue;
            }
            if self.child_done {
                if let Some(batch) = self.take_batch(&graph_ctx, true)? {
                    return Ok(Some(batch));
                }
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
            match self.child.next_batch(ctx).await? {
                Some(child_batch) => {
                    let mut chunks: VecDeque<(usize, Option<KeySet>)> = VecDeque::new();
                    let mut joins = Vec::with_capacity(self.resolved.branches.len());
                    for branch in 0..self.resolved.branches.len() {
                        // A branch no outer row can match contributes nothing.
                        for ks in self.keysets_for(branch, &child_batch, &graph_ctx) {
                            chunks.push_back((branch, ks));
                        }
                        joins.push(self.build_join_plan(branch, &child_batch, &graph_ctx));
                    }
                    if chunks.is_empty() {
                        continue;
                    }
                    self.inflight = Some(InFlight {
                        child_batch,
                        joins,
                        chunks,
                        stream: None,
                    });
                }
                None => self.child_done = true,
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.inflight = None;
        self.pending.clear();
        self.state = OperatorState::Closed;
    }
}
