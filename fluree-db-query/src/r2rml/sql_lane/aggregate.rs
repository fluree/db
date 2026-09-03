//! GROUP BY and aggregates over one SQL block, as one grouped statement.
//!
//! The block's rows are exactly the SPARQL solutions (the join layer's
//! contract), so grouping them in the database is exact wherever SQL and
//! SPARQL agree on the aggregate; where they differ the plan is patched:
//! `AVG` is pushed as `SUM` + `COUNT` and divided in the engine (databases
//! round a decimal average to the input scale), an empty `SUM` comes back
//! `NULL` and is reported as `0`, and string keys or `MIN`/`MAX` are pushed
//! only when the dialect compares bytes. HAVING, ORDER BY and LIMIT stay
//! with the engine's own operators above; a top-k is offered to the
//! statement when no HAVING could drop rows afterwards.

use std::collections::HashMap;
use std::sync::Arc;

use bigdecimal::BigDecimal;
use fluree_db_core::{FlakeValue, Sid};
use fluree_db_r2rml::mapping::ObjectMap;
use fluree_db_tabular::plan::{ColRef, OrderKey, OutputCol, OutputExpr, RelPlan};
use fluree_db_tabular::{Column, ColumnBatch, FieldType};
use fluree_vocab::xsd;
use num_bigint::BigInt;

use super::lower::{source_of_tm, AccessInfo, KeyShape, Lowered, RdfClass, TermSource};
use super::terms::Materializer;
use super::{resolve_block, Resolved};
use crate::aggregate::NumericAcc;
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
use crate::ir::grouping::{AggregateFn, InputSemantics};
use crate::ir::{GraphName, Pattern, Query};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::sort::SortSpec;
use crate::var_registry::VarId;

/// Routing stamp site for `MustFire` / `MustNotFire` tests.
pub const SQL_AGGREGATE_PUSHDOWN_SITE: &str = "sql_aggregate_pushdown";

/// A grouped query over one `GRAPH <iri>` block, admitted at plan time.
pub struct SqlAggregatePlan {
    graph_iri: Arc<str>,
    inner_patterns: Vec<Pattern>,
    group_by: Vec<VarId>,
    aggregates: Vec<(VarId, AggregateFn)>,
    /// The complete ORDER BY and k when a LIMIT above may be pushed.
    topk: Option<(Vec<SortSpec>, usize)>,
}

/// Structural admission: the fused aggregate's shape (a sole `GRAPH <iri>`
/// block, keys + aggregates projected) over a block the lane could lower.
/// Whether the source is SQL and the mapping admits it is decided at open.
pub fn detect_sql_block_aggregate(query: &Query) -> Option<SqlAggregatePlan> {
    if !super::sql_pushdown_lane_enabled()
        || crate::execute::operator_tree::fast_paths_disabled()
        || !query.order_binds.is_empty()
        || query.post_values.is_some()
    {
        return None;
    }
    let grouping = query.grouping.as_ref()?;
    let group_by: Vec<VarId> = grouping.group_by_vars().collect();
    if grouping.aggregation().is_some_and(|a| !a.binds.is_empty()) {
        return None;
    }
    let aggregates: Vec<(VarId, AggregateFn)> = grouping
        .aggregates()
        .map(|s| (s.output_var, s.function.clone()))
        .collect();
    if !aggregates.iter().all(|(_, f)| {
        matches!(
            f,
            AggregateFn::CountAll
                | AggregateFn::Count(_)
                | AggregateFn::CountDistinct(_)
                | AggregateFn::Sum(..)
                | AggregateFn::Avg(..)
                | AggregateFn::Min(_)
                | AggregateFn::Max(_)
        )
    }) {
        return None;
    }
    let [Pattern::Graph {
        name: GraphName::Iri(iri),
        patterns,
    }] = query.patterns.as_slice()
    else {
        return None;
    };
    if !super::lower::block_is_admissible(patterns) {
        return None;
    }
    let outs: Vec<VarId> = group_by
        .iter()
        .copied()
        .chain(aggregates.iter().map(|(v, _)| *v))
        .collect();
    if let Some(projected) = query.output.projected_vars() {
        if projected.len() != outs.len() || projected.iter().any(|v| !outs.contains(v)) {
            return None;
        }
    }
    if query.ordering.iter().any(|s| !outs.contains(&s.var)) {
        return None;
    }
    let topk = match (query.limit, grouping.having()) {
        (Some(limit), None) if !query.ordering.is_empty() => Some((
            query.ordering.clone(),
            limit.saturating_add(query.offset.unwrap_or(0)),
        )),
        _ => None,
    };
    Some(SqlAggregatePlan {
        graph_iri: Arc::clone(iri),
        inner_patterns: patterns.clone(),
        group_by,
        aggregates,
        topk,
    })
}

/// How one output of the grouped statement becomes a binding.
pub(super) enum Decode {
    /// A key or a `MIN`/`MAX`: the `idx`-th term the materializer builds.
    Term { idx: usize },
    /// `COUNT`: the named integer output.
    Count { name: String },
    /// `SUM` / `AVG`: the named sum and count outputs, typed by `rr:datatype`.
    Numeric {
        sum: String,
        count: String,
        kind: NumKind,
        avg: bool,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum NumKind {
    Integer,
    Decimal,
    Double,
}

struct AggLowered {
    plan: RelPlan,
    materializer: Materializer,
    /// Aligned with the operator's schema (keys, then aggregate outputs).
    decodes: Vec<Decode>,
}

/// A grouped plan over a lowered block: keys, aggregate outputs, the terms
/// and accesses that decode them, and a top-k. What [`lower_aggregate`]
/// runs as a statement and a subquery embeds as a derived table.
pub(super) struct Grouped {
    pub plan: RelPlan,
    /// Aligned with `decodes`' `Term` indices.
    pub terms: Vec<(VarId, TermSource)>,
    /// Mapping columns each access must carry, by alias.
    pub access_columns: HashMap<String, Vec<String>>,
    /// Per-extreme materialization aliases (see `MIN`/`MAX` below).
    pub extremes: Vec<AccessInfo>,
    /// Keys, then aggregates, in `group_by` / `aggregates` order.
    pub decodes: Vec<Decode>,
}

/// The grouped statement for a lowered block, or `None` (a reason) when the
/// aggregates cannot be pushed exactly.
fn lower_aggregate(
    plan: &SqlAggregatePlan,
    resolved: &Resolved,
    lowered: &Lowered,
    snapshot: &fluree_db_core::LedgerSnapshot,
) -> std::result::Result<AggLowered, &'static str> {
    if !lowered.residual_filters.is_empty() {
        return Err("residual filter under an aggregate");
    }
    let mut grouped = group_plan(
        &plan.group_by,
        &plan.aggregates,
        plan.topk.as_ref(),
        lowered,
        &resolved.mapping,
        &resolved.caps,
        &resolved.schemas,
    )?;
    let mut for_terms = lowered.clone();
    for_terms.outputs = grouped.plan.output.clone();
    for_terms.terms = grouped.terms;
    for a in &mut for_terms.accesses {
        a.columns = grouped.access_columns.remove(&a.alias).unwrap_or_default();
    }
    for_terms.accesses.extend(grouped.extremes);
    let materializer =
        Materializer::new(&for_terms, &resolved.mapping, snapshot).map_err(|_| "materializer")?;
    Ok(AggLowered {
        plan: grouped.plan,
        materializer,
        decodes: grouped.decodes,
    })
}

pub(super) fn group_plan(
    group_by: &[VarId],
    aggregates: &[(VarId, AggregateFn)],
    topk: Option<&(Vec<SortSpec>, usize)>,
    lowered: &Lowered,
    mapping: &fluree_db_r2rml::mapping::CompiledR2rmlMapping,
    caps: &fluree_db_tabular::plan::PushdownCapabilities,
    schemas: &HashMap<fluree_db_tabular::plan::RelSource, Arc<fluree_db_tabular::BatchSchema>>,
) -> std::result::Result<Grouped, &'static str> {
    if lowered
        .terms
        .iter()
        .any(|(_, t)| matches!(t, TermSource::Union { .. }))
    {
        return Err("aggregate over a union entity");
    }
    let field_type = |col: &ColRef| -> Option<FieldType> {
        let tm_iri = lowered
            .accesses
            .iter()
            .find(|a| a.alias == col.alias)
            .map(|a| a.tm_iri.as_str())?;
        let tm = mapping.get(tm_iri)?;
        schemas
            .get(&source_of_tm(tm))?
            .field_by_name(&col.column)
            .map(|f| f.field_type)
    };
    let object_map = |term: &TermSource| -> Option<ObjectMap> {
        let TermSource::Object { tm_iri, pom, .. } = term else {
            return None;
        };
        let tm = mapping.get(tm_iri)?;
        Some(tm.predicate_object_maps.get(*pom)?.object_map.clone())
    };
    let var_of =
        |v: VarId| -> std::result::Result<(&super::lower::VarSource, &Vec<ColRef>), &'static str> {
            match (lowered.vars.get(&v), lowered.var_columns.get(&v)) {
                (Some(src), Some(cols)) if !cols.is_empty() => Ok((src, cols)),
                _ => Err("aggregate over a variable without columns"),
            }
        };

    let mut outputs: Vec<OutputCol> = Vec::new();
    let mut group_cols: Vec<ColRef> = Vec::new();
    let mut key_decodes: Vec<Decode> = Vec::new();
    let mut agg_decodes: Vec<Option<Decode>> = (0..aggregates.len()).map(|_| None).collect();
    let mut terms: Vec<(VarId, TermSource)> = Vec::new();
    let mut access_columns: HashMap<String, Vec<String>> = HashMap::new();
    let mut claimed: std::collections::HashSet<(String, String)> = Default::default();
    let name = |outputs: &Vec<OutputCol>| format!("c{}", outputs.len());

    // Keys: every column the key's term reads, grouped and projected.
    for v in group_by {
        let (src, cols) = var_of(*v)?;
        for col in cols {
            if field_type(col) == Some(FieldType::String) && !caps.string_distinct_is_binary {
                return Err("string key under a collating dialect");
            }
            if !claimed.insert((col.alias.clone(), col.column.clone())) {
                return Err("column shared by two keys");
            }
            access_columns
                .entry(col.alias.clone())
                .or_default()
                .push(col.column.clone());
            group_cols.push(col.clone());
            let n = name(&outputs);
            outputs.push(OutputCol::column(col.clone(), n));
        }
        key_decodes.push(Decode::Term { idx: terms.len() });
        terms.push((*v, src.term.clone()));
    }

    // Each extreme reads its column through its own materialization alias,
    // so MIN and MAX of one column, or of a key, are distinct outputs.
    let mut extremes: Vec<AccessInfo> = Vec::new();
    for (pos, (out, f)) in aggregates.iter().enumerate() {
        agg_decodes[pos] = Some(match f {
            AggregateFn::Min(v) | AggregateFn::Max(v) => {
                let (src, cols) = var_of(*v)?;
                let [col] = cols.as_slice() else {
                    return Err("MIN/MAX over a template");
                };
                let TermSource::Object { tm_iri, pom, .. } = &src.term else {
                    return Err("MIN/MAX over a subject");
                };
                let orderable = match &src.key {
                    Some(KeyShape::Column { class, .. }) => match class {
                        RdfClass::Numeric
                        | RdfClass::Date
                        | RdfClass::DateTime
                        | RdfClass::Bool => true,
                        RdfClass::Str => caps.string_order_is_codepoint,
                        _ => false,
                    },
                    _ => false,
                };
                if !orderable {
                    return Err("MIN/MAX over a value the database orders differently");
                }
                let n = name(&outputs);
                let expr = if matches!(f, AggregateFn::Min(_)) {
                    OutputExpr::Min(col.clone())
                } else {
                    OutputExpr::Max(col.clone())
                };
                outputs.push(OutputCol {
                    expr,
                    name: n.clone(),
                });
                let alias = format!("{}.{n}", col.alias);
                let access_tm = lowered
                    .accesses
                    .iter()
                    .find(|a| a.alias == col.alias)
                    .map(|a| a.tm_iri.clone())
                    .ok_or("MIN/MAX column without an access")?;
                extremes.push(AccessInfo {
                    alias: alias.clone(),
                    tm_iri: access_tm,
                    columns: vec![col.column.clone()],
                    output_names: Some(vec![n]),
                });
                let idx = terms.len();
                terms.push((
                    *out,
                    TermSource::Object {
                        alias,
                        tm_iri: tm_iri.clone(),
                        pom: *pom,
                    },
                ));
                Decode::Term { idx }
            }
            AggregateFn::CountAll => {
                let n = name(&outputs);
                outputs.push(OutputCol {
                    expr: OutputExpr::CountRows,
                    name: n.clone(),
                });
                Decode::Count { name: n }
            }
            AggregateFn::Count(v) | AggregateFn::CountDistinct(v) => {
                let distinct = matches!(f, AggregateFn::CountDistinct(_));
                let (_, cols) = var_of(*v)?;
                let col = if distinct {
                    let [col] = cols.as_slice() else {
                        return Err("COUNT DISTINCT over a template");
                    };
                    if field_type(col) == Some(FieldType::String) && !caps.string_distinct_is_binary
                    {
                        return Err("COUNT DISTINCT of strings under a collating dialect");
                    }
                    col
                } else {
                    // A template's columns are null together, so any one
                    // counts the bound rows.
                    &cols[0]
                };
                let n = name(&outputs);
                outputs.push(OutputCol {
                    expr: OutputExpr::Count {
                        col: col.clone(),
                        distinct,
                    },
                    name: n.clone(),
                });
                Decode::Count { name: n }
            }
            AggregateFn::Sum(v, sem) | AggregateFn::Avg(v, sem) => {
                let (src, cols) = var_of(*v)?;
                let [col] = cols.as_slice() else {
                    return Err("SUM/AVG over a template");
                };
                let Some(ObjectMap::Column { datatype, .. }) = object_map(&src.term) else {
                    return Err("SUM/AVG over a non-column value");
                };
                let kind = match datatype.as_deref() {
                    Some(xsd::DECIMAL) => NumKind::Decimal,
                    Some(xsd::DOUBLE | xsd::FLOAT) => NumKind::Double,
                    Some(dt)
                        if super::lower::class_of(
                            Some(dt),
                            None,
                            fluree_db_r2rml::mapping::TermType::Literal,
                        ) == RdfClass::Numeric =>
                    {
                        NumKind::Integer
                    }
                    _ => return Err("SUM/AVG over a non-numeric datatype"),
                };
                let physical_ok = matches!(
                    (kind, field_type(col)),
                    (NumKind::Integer, Some(FieldType::Int32 | FieldType::Int64))
                        | (
                            NumKind::Decimal,
                            Some(FieldType::Decimal { .. } | FieldType::Int32 | FieldType::Int64)
                        )
                        | (
                            NumKind::Double,
                            Some(FieldType::Float32 | FieldType::Float64)
                        )
                );
                if !physical_ok {
                    return Err("SUM/AVG column type differs from its datatype");
                }
                let distinct = matches!(sem, InputSemantics::Set);
                let sum = name(&outputs);
                outputs.push(OutputCol {
                    expr: OutputExpr::Sum {
                        col: col.clone(),
                        distinct,
                    },
                    name: sum.clone(),
                });
                let count = name(&outputs);
                outputs.push(OutputCol {
                    expr: OutputExpr::Count {
                        col: col.clone(),
                        distinct,
                    },
                    name: count.clone(),
                });
                Decode::Numeric {
                    sum,
                    count,
                    kind,
                    avg: matches!(f, AggregateFn::Avg(..)),
                }
            }
            _ => return Err("unsupported aggregate"),
        });
    }
    if outputs.is_empty() {
        return Err("nothing to project");
    }
    let decodes: Vec<Decode> = key_decodes
        .into_iter()
        .chain(
            agg_decodes
                .into_iter()
                .map(|d| d.expect("every aggregate decoded")),
        )
        .collect();

    // A top-k over aggregate outputs and required scalar keys. The statement
    // answers exactly k groups, so the LIMIT goes only with the whole ORDER BY.
    let mut order_by = Vec::new();
    let mut limit = None;
    if let Some((ordering, k)) = topk {
        let keys: Option<Vec<(OrderKey, bool)>> = ordering
            .iter()
            .map(|s| {
                let key = if group_by.contains(&s.var) {
                    let (col, _) = lowered.order_columns.get(&s.var)?;
                    OrderKey::Col(col.clone())
                } else {
                    let i = aggregates.iter().position(|(v, _)| *v == s.var)?;
                    match &decodes[group_by.len() + i] {
                        Decode::Count { name } => OrderKey::Output(name.clone()),
                        Decode::Numeric {
                            sum, avg: false, ..
                        } => OrderKey::Output(sum.clone()),
                        _ => return None,
                    }
                };
                Some((key, s.ascending()))
            })
            .collect();
        if let Some(keys) = keys {
            order_by = keys;
            limit = Some(*k as u64);
        }
    }

    let plan = RelPlan {
        root: lowered.root.clone(),
        output: outputs,
        distinct: aggregates.is_empty(),
        group_by: if aggregates.is_empty() {
            Vec::new()
        } else {
            group_cols
        },
        order_by,
        limit,
    };
    Ok(Grouped {
        plan,
        terms,
        access_columns,
        extremes,
        decodes,
    })
}

/// The grouped-query operator: one statement at open, rows decoded into
/// keys + aggregate outputs; streams the fallback when the lane declines.
pub struct SqlAggregateOperator {
    plan: SqlAggregatePlan,
    schema: Arc<[VarId]>,
    fallback: Option<BoxedOperator>,
    chain: Option<BoxedOperator>,
    state: OperatorState,
}

impl SqlAggregateOperator {
    pub fn new(plan: SqlAggregatePlan, fallback: BoxedOperator) -> Self {
        let schema: Arc<[VarId]> = plan
            .group_by
            .iter()
            .copied()
            .chain(plan.aggregates.iter().map(|(v, _)| *v))
            .collect();
        Self {
            plan,
            schema,
            fallback: Some(fallback),
            chain: None,
            state: OperatorState::Created,
        }
    }
}

#[async_trait::async_trait]
impl Operator for SqlAggregateOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        match (&self.chain, &self.fallback) {
            (Some(c), _) => vec![crate::plan_node::PlanChild::child(c.as_ref())],
            (None, Some(f)) => vec![crate::plan_node::PlanChild::fallback(f.as_ref())],
            _ => Vec::new(),
        }
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("graph".into(), self.plan.graph_iri.to_string().into());
        m.insert("lane".into(), "sql_aggregate_pushdown".into());
        m
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let fallback = self
            .fallback
            .take()
            .ok_or_else(|| QueryError::Internal("SqlAggregateOperator opened twice".into()))?;
        let resolved = resolve_block(
            ctx,
            &self.plan.graph_iri,
            &self.plan.inner_patterns,
            &[],
            None,
        )
        .await?;
        let lowered = resolved.as_ref().and_then(|r| {
            // A UNION is several statements; grouping across them stays with
            // the engine. An empty block has nothing to group either.
            let single = match r.branches.as_slice() {
                [b] => Ok(&b.lowered),
                [] => Err("block yields no rows"),
                _ => Err("UNION under an aggregate"),
            };
            match single.and_then(|l| lower_aggregate(&self.plan, r, l, ctx.active_snapshot)) {
                Ok(al) => Some(al),
                Err(why) => {
                    tracing::debug!(graph = %self.plan.graph_iri, why, "sql aggregate pushdown declined");
                    None
                }
            }
        });
        let mut chain: BoxedOperator = match lowered {
            None => {
                stamp_fast_path(
                    SQL_AGGREGATE_PUSHDOWN_SITE,
                    FastPathOutcome::Fallback(FastPathFallback::GateDeclined),
                );
                fallback
            }
            Some(al) => {
                stamp_fast_path(SQL_AGGREGATE_PUSHDOWN_SITE, FastPathOutcome::Proceed);
                Box::new(GroupedSource {
                    graph_iri: Arc::clone(&self.plan.graph_iri),
                    schema: Arc::clone(&self.schema),
                    lowered: al,
                    stream: None,
                    drained: false,
                    pending: Vec::new(),
                    state: OperatorState::Created,
                })
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
        if batch.schema() == self.schema.as_ref() {
            return Ok(Some(batch));
        }
        let mut columns: Vec<Vec<Binding>> = Vec::with_capacity(self.schema.len());
        for var in self.schema.iter() {
            columns.push(match batch.column(*var) {
                Some(col) => col.to_vec(),
                None => vec![Binding::Unbound; batch.len()],
            });
        }
        Batch::new(Arc::clone(&self.schema), columns)
            .map(Some)
            .map_err(|e| QueryError::Internal(e.to_string()))
    }

    fn close(&mut self) {
        if let Some(c) = self.chain.as_mut() {
            c.close();
        }
        if let Some(f) = self.fallback.as_mut() {
            f.close();
        }
        self.state = OperatorState::Closed;
    }
}

struct GroupedSource {
    graph_iri: Arc<str>,
    schema: Arc<[VarId]>,
    lowered: AggLowered,
    stream: Option<crate::r2rml::ColumnBatchStream>,
    /// The statement ran to completion; only `pending` is left to emit.
    drained: bool,
    pending: Vec<Vec<Binding>>,
    state: OperatorState,
}

impl GroupedSource {
    fn decode_page(
        &self,
        page: ColumnBatch,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Vec<Binding>>> {
        let num_rows = page.num_rows;
        let by_name: HashMap<String, Column> = page
            .schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .zip(page.columns.iter().cloned())
            .collect();
        let batches = self
            .lowered
            .materializer
            .split_page(page, &self.lowered.plan.output)?;
        let mut rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            ctx.tracker.consume_fuel(1)?;
            let terms = self.lowered.materializer.row(&batches, i)?;
            let mut row = Vec::with_capacity(self.schema.len());
            for d in &self.lowered.decodes {
                row.push(match d {
                    Decode::Term { idx } => terms
                        .get(*idx)
                        .map(|(_, b)| b.clone())
                        .unwrap_or(Binding::Unbound),
                    Decode::Count { name } => Binding::lit(
                        FlakeValue::Long(int_at(column(&by_name, name)?, name, i)?),
                        Sid::xsd_integer(),
                    ),
                    Decode::Numeric {
                        sum,
                        count,
                        kind,
                        avg,
                    } => {
                        let count = int_at(column(&by_name, count)?, count, i)? as u64;
                        let acc = match kind {
                            NumKind::Double => NumericAcc::from_double_total(
                                double_at(column(&by_name, sum)?, sum, i)?,
                                count,
                            ),
                            NumKind::Integer | NumKind::Decimal => NumericAcc::from_exact_total(
                                exact_at(column(&by_name, sum)?, sum, i)?,
                                count,
                                matches!(kind, NumKind::Decimal),
                            ),
                        };
                        if *avg {
                            acc.finalize_avg()
                        } else {
                            acc.finalize_sum()
                        }
                    }
                });
            }
            rows.push(row);
        }
        Ok(rows)
    }

    fn take_batch(&mut self, ctx: &ExecutionContext<'_>, all: bool) -> Result<Option<Batch>> {
        if self.pending.is_empty() || (!all && self.pending.len() < ctx.batch_size) {
            return Ok(None);
        }
        let n = self.pending.len().min(ctx.batch_size);
        let mut columns: Vec<Vec<Binding>> = (0..self.schema.len())
            .map(|_| Vec::with_capacity(n))
            .collect();
        for row in self.pending.drain(..n) {
            for (i, b) in row.into_iter().enumerate() {
                columns[i].push(b);
            }
        }
        let batch = Batch::new(Arc::clone(&self.schema), columns)
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        let stamp_ledger_id = match ctx.dataset {
            Some(ds) if ds.spans_multiple_ledgers() => ds
                .named_graph(self.graph_iri.as_ref())
                .map(|g| Arc::clone(&g.ledger_id)),
            _ => None,
        };
        Ok(Some(match stamp_ledger_id {
            Some(ledger_id) => crate::dataset_operator::stamp_provenance(batch, &ledger_id, ctx)?,
            None => batch,
        }))
    }
}

/// An aggregate output of a derived table, read from the batch holding it.
pub(super) fn decode_aggregate(
    batch: &ColumnBatch,
    kind: &super::lower::AggTerm,
    i: usize,
) -> Result<Binding> {
    let col = |name: &str| {
        batch.column_by_name(name).ok_or_else(|| {
            QueryError::Internal(format!("derived table lacks aggregate output '{name}'"))
        })
    };
    Ok(match kind {
        super::lower::AggTerm::Count { column } => Binding::lit(
            FlakeValue::Long(int_at(col(column)?, column, i)?),
            Sid::xsd_integer(),
        ),
        super::lower::AggTerm::Numeric {
            sum,
            count,
            kind,
            avg,
        } => {
            let n = int_at(col(count)?, count, i)? as u64;
            let acc = match kind {
                NumKind::Double => NumericAcc::from_double_total(double_at(col(sum)?, sum, i)?, n),
                NumKind::Integer | NumKind::Decimal => NumericAcc::from_exact_total(
                    exact_at(col(sum)?, sum, i)?,
                    n,
                    matches!(kind, NumKind::Decimal),
                ),
            };
            if *avg {
                acc.finalize_avg()
            } else {
                acc.finalize_sum()
            }
        }
    })
}

fn column<'a>(by_name: &'a HashMap<String, Column>, name: &str) -> Result<&'a Column> {
    by_name
        .get(name)
        .ok_or_else(|| QueryError::Internal(format!("grouped statement lacks output '{name}'")))
}

fn int_at(col: &Column, name: &str, i: usize) -> Result<i64> {
    Ok(match col {
        Column::Int64(v) => v.get(i).copied().flatten().unwrap_or(0),
        Column::Int32(v) => v.get(i).copied().flatten().map(i64::from).unwrap_or(0),
        Column::Decimal { values, scale, .. } if *scale == 0 => values
            .get(i)
            .copied()
            .flatten()
            .and_then(|v| i64::try_from(v).ok())
            .unwrap_or(0),
        other => {
            return Err(QueryError::Internal(format!(
                "count output '{name}' is not an integer column ({:?})",
                other.field_type()
            )))
        }
    })
}

fn double_at(col: &Column, name: &str, i: usize) -> Result<f64> {
    Ok(match col {
        Column::Float64(v) => v.get(i).copied().flatten().unwrap_or(0.0),
        Column::Float32(v) => v.get(i).copied().flatten().map(f64::from).unwrap_or(0.0),
        Column::Int64(v) => v.get(i).copied().flatten().unwrap_or(0) as f64,
        Column::Decimal { values, scale, .. } => values
            .get(i)
            .copied()
            .flatten()
            .map(|v| v as f64 / 10f64.powi(i32::from(*scale)))
            .unwrap_or(0.0),
        other => {
            return Err(QueryError::Internal(format!(
                "sum output '{name}' is not a floating column ({:?})",
                other.field_type()
            )))
        }
    })
}

fn exact_at(col: &Column, name: &str, i: usize) -> Result<BigDecimal> {
    Ok(match col {
        Column::Decimal { values, scale, .. } => values
            .get(i)
            .copied()
            .flatten()
            .map(|v| BigDecimal::new(BigInt::from(v), i64::from(*scale)))
            .unwrap_or_default(),
        Column::Int64(v) => BigDecimal::from(v.get(i).copied().flatten().unwrap_or(0)),
        Column::Int32(v) => BigDecimal::from(v.get(i).copied().flatten().unwrap_or(0)),
        other => {
            return Err(QueryError::Internal(format!(
                "sum output '{name}' is not an exact numeric column ({:?})",
                other.field_type()
            )))
        }
    })
}

#[async_trait::async_trait]
impl Operator for GroupedSource {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("graph".into(), self.graph_iri.to_string().into());
        m.insert("lane".into(), "sql_aggregate_pushdown".into());
        m
    }

    async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state == OperatorState::Exhausted {
            return Ok(None);
        }
        let graph_ctx = ctx.with_active_graph(Arc::clone(&self.graph_iri));
        if self.drained {
            if let Some(batch) = self.take_batch(&graph_ctx, true)? {
                return Ok(Some(batch));
            }
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if self.stream.is_none() {
            let table_provider = graph_ctx.r2rml_table_provider.ok_or_else(|| {
                QueryError::InvalidQuery("R2RML table provider not configured".into())
            })?;
            let (sql, stream) = table_provider
                .execute_plan(&self.graph_iri, &self.lowered.plan)
                .await?;
            graph_ctx.tracker.record_statement(&self.graph_iri, &sql);
            self.stream = Some(stream);
        }
        loop {
            graph_ctx.checkpoint()?;
            if let Some(batch) = self.take_batch(&graph_ctx, false)? {
                return Ok(Some(batch));
            }
            use futures::StreamExt;
            let next = match self.stream.as_mut() {
                Some(s) => s.next().await,
                None => None,
            };
            match next {
                Some(page) => {
                    let rows = self.decode_page(page?, &graph_ctx)?;
                    self.pending.extend(rows);
                }
                None => {
                    self.stream = None;
                    self.drained = true;
                    if let Some(batch) = self.take_batch(&graph_ctx, true)? {
                        return Ok(Some(batch));
                    }
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            }
        }
    }

    fn close(&mut self) {
        self.stream = None;
        self.pending.clear();
        self.state = OperatorState::Closed;
    }
}
