//! Fused R2RML analytical-aggregate operator.
//!
//! When a query is a single R2RML graph-source scan feeding a simple aggregate
//! (no joins), the normal pipeline materializes an RDF `Binding` for every table
//! row — subject IRI strings, per-row vectors, value clones — only for a
//! group-aggregate to fold them away. For analytical shapes that is pure
//! allocation churn. This operator folds the aggregates **directly from the
//! typed `ColumnBatch` values**, never building a subject IRI or a per-row
//! `Binding`, and materializes only the final result row.
//!
//! # Scope (slice 1)
//!
//! `COUNT(?col)` / `COUNT(*)` over one TriplesMap, no GROUP BY, no FILTER, no
//! joins. `SUM`/`AVG`, FILTER pushdown, and GROUP BY keys extend this later.
//!
//! # Soundness
//!
//! Detection is a cheap structural check on the IR. The R2RML rewrite needs the
//! ledger snapshot, so it (and column resolution) is deferred to `open`: the
//! inner triples are rewritten to a `Pattern::R2rml`, and each aggregate variable
//! is resolved to a single scalar table column. If anything fails — the graph is
//! not R2RML, the triples don't collapse to one scan, a predicate is a join or
//! multi-valued — the operator falls back to the exact normal pipeline, so
//! general graph-source semantics are unchanged.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::grouping::{AggregateFn, Grouping};
use crate::ir::{GraphName, Pattern, Query, R2rmlPattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::r2rml::rewrite_patterns_for_r2rml;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{FlakeValue, Sid};
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ObjectMap, TriplesMap};
use fluree_db_tabular::Column;
use futures::StreamExt;
use std::sync::Arc;

/// A detected fused-aggregate plan: the enclosing graph IRI, the inner triple
/// patterns (rewritten to R2RML at `open`), and the per-output aggregate
/// functions in projected-output order.
pub struct FusedAggregatePlan {
    graph_iri: Arc<str>,
    inner_patterns: Vec<Pattern>,
    aggregates: Vec<(VarId, AggregateFn)>,
}

/// Detect the slice-1 fused shape: a single `GRAPH { triples }` block feeding an
/// implicit (no GROUP BY) aggregation of only `COUNT(?col)` / `COUNT(*)`, with no
/// HAVING, post-binds, ordering, or slicing. Whether the graph is actually R2RML
/// (and whether the triples collapse to one scan) is checked at `open`.
pub fn detect_fused_r2rml_aggregate(query: &Query) -> Option<FusedAggregatePlan> {
    if !query.ordering.is_empty()
        || !query.order_binds.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
    {
        return None;
    }

    // Implicit aggregation only (no GROUP BY), no HAVING, no post-aggregate binds.
    let Some(Grouping::Implicit { aggregation, having }) = query.grouping.as_ref() else {
        return None;
    };
    if having.is_some() || !aggregation.binds.is_empty() {
        return None;
    }

    // Sole pattern is `GRAPH <iri> { triples... }` with only triple patterns.
    let (graph_iri, inner) = match query.patterns.as_slice() {
        [Pattern::Graph {
            name: GraphName::Iri(iri),
            patterns,
        }] if !patterns.is_empty()
            && patterns.iter().all(|p| matches!(p, Pattern::Triple(_))) =>
        {
            (Arc::clone(iri), patterns.clone())
        }
        _ => return None,
    };

    // Every aggregate must be a COUNT this operator can fold from a column.
    let mut aggregates = Vec::with_capacity(aggregation.aggregates.len());
    for spec in aggregation.aggregates.iter() {
        if !matches!(spec.function, AggregateFn::CountAll | AggregateFn::Count(_)) {
            return None;
        }
        aggregates.push((spec.output_var, spec.function.clone()));
    }

    // The projection must be exactly the aggregate outputs, so the fused output
    // row is the final result.
    if let Some(projected) = query.output.projected_vars() {
        let outs: std::collections::HashSet<VarId> = aggregates.iter().map(|(v, _)| *v).collect();
        if projected.len() != outs.len() || projected.iter().any(|v| !outs.contains(v)) {
            return None;
        }
    }

    Some(FusedAggregatePlan {
        graph_iri,
        inner_patterns: inner,
        aggregates,
    })
}

/// How one output aggregate folds over the scanned column batches.
enum Fold {
    /// `COUNT(*)` — count rows.
    CountRows,
    /// `COUNT(?col)` — count non-null values of this table column.
    CountColumn(String),
}

/// Resolved fused plan (post-`open`): the rewritten scan pattern, the table to
/// scan, the columns to project, and the per-output fold.
struct Resolved {
    pattern: R2rmlPattern,
    table_name: String,
    projection: Vec<String>,
    folds: Vec<Fold>,
}

/// Fused R2RML aggregate operator. Folds COUNT aggregates straight from column
/// batches; falls back to the normal pipeline when its soundness gates fail.
pub struct FusedR2rmlAggregateOperator {
    graph_iri: Arc<str>,
    inner_patterns: Vec<Pattern>,
    aggregates: Vec<(VarId, AggregateFn)>,
    schema: Arc<[VarId]>,
    fallback: BoxedOperator,
    resolved: Option<Resolved>,
    use_fallback: bool,
    state: OperatorState,
    done: bool,
}

impl FusedR2rmlAggregateOperator {
    /// Build the operator from a detected plan and the normal-pipeline fallback
    /// (built with fast paths disabled).
    pub fn new(plan: FusedAggregatePlan, fallback: BoxedOperator) -> Self {
        let schema: Arc<[VarId]> = plan.aggregates.iter().map(|(v, _)| *v).collect();
        Self {
            graph_iri: plan.graph_iri,
            inner_patterns: plan.inner_patterns,
            aggregates: plan.aggregates,
            schema,
            fallback,
            resolved: None,
            use_fallback: false,
            state: OperatorState::Created,
            done: false,
        }
    }

    /// Resolve the predicate IRI a pattern object variable is bound by.
    fn predicate_for_var(pattern: &R2rmlPattern, var: VarId) -> Option<&str> {
        if Some(var) == pattern.object_var {
            pattern.predicate_filter.as_deref()
        } else {
            pattern
                .star_bindings
                .iter()
                .find(|(_, v)| *v == var)
                .map(|(p, _)| p.as_str())
        }
    }

    /// Resolve the single scalar column a variable's predicate maps to, or `None`
    /// (gate fail) for a RefObjectMap join, a multi-valued predicate, or a
    /// non-column object map.
    fn scalar_column_for_var(pattern: &R2rmlPattern, tm: &TriplesMap, var: VarId) -> Option<String> {
        let pred = Self::predicate_for_var(pattern, var)?;
        let mut poms = tm
            .predicate_object_maps
            .iter()
            .filter(|pom| pom.predicate_map.as_constant() == Some(pred));
        let (Some(pom), None) = (poms.next(), poms.next()) else {
            return None; // missing or multi-valued predicate
        };
        match &pom.object_map {
            ObjectMap::Column { column, .. } => Some(column.clone()),
            _ => None, // RefObjectMap / Template / Constant
        }
    }

    /// Resolve the single TriplesMap for the rewritten pattern, requiring exactly
    /// one (explicit IRI, or an unambiguous class/predicate match).
    fn resolve_triples_map<'m>(
        pattern: &R2rmlPattern,
        mapping: &'m CompiledR2rmlMapping,
    ) -> Option<&'m TriplesMap> {
        if let Some(ref iri) = pattern.triples_map_iri {
            return mapping.triples_maps.get(iri);
        }
        let mut matches = mapping.triples_maps.values().filter(|tm| {
            if let Some(ref class_filter) = pattern.class_filter {
                if !tm.classes().contains(class_filter) {
                    return false;
                }
            }
            if let Some(ref pred) = pattern.predicate_filter {
                if !tm
                    .predicate_object_maps
                    .iter()
                    .any(|pom| pom.predicate_map.as_constant() == Some(pred.as_str()))
                {
                    return false;
                }
            }
            true
        });
        match (matches.next(), matches.next()) {
            (Some(tm), None) => Some(tm),
            _ => None,
        }
    }
}

/// Count non-null values in a column without per-row branching on the enum.
fn non_null_count(col: &Column) -> usize {
    match col {
        Column::Boolean(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::Int32(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::Int64(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::Float32(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::Float64(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::String(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::Bytes(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::Date(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::Timestamp(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::TimestampTz(v) => v.iter().filter(|x| x.is_some()).count(),
        Column::Decimal { values, .. } => values.iter().filter(|x| x.is_some()).count(),
    }
}

#[async_trait]
impl Operator for FusedR2rmlAggregateOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.fallback.as_ref())]
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.resolved = self.resolve_at_open(ctx).await?;
        if self.resolved.is_none() {
            tracing::debug!("fused R2RML aggregate: gates failed, using fallback pipeline");
            self.use_fallback = true;
            self.fallback.open(ctx).await?;
        } else {
            tracing::debug!(
                aggs = self.aggregates.len(),
                "fused R2RML aggregate: folding COUNT from column batches"
            );
        }
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.use_fallback {
            return self.fallback.next_batch(ctx).await;
        }
        if self.done || self.state == OperatorState::Exhausted {
            return Ok(None);
        }
        let resolved = self
            .resolved
            .as_ref()
            .ok_or_else(|| QueryError::Internal("fused aggregate not resolved".to_string()))?;

        let table_provider = ctx.r2rml_table_provider.ok_or_else(|| {
            QueryError::InvalidQuery("R2RML table provider not configured".to_string())
        })?;
        let as_of_t = if ctx.dataset.is_some() {
            None
        } else {
            Some(ctx.to_t)
        };

        let mut counts = vec![0u64; resolved.folds.len()];
        let mut stream = table_provider
            .scan_table(
                &resolved.pattern.graph_source_id,
                &resolved.table_name,
                &resolved.projection,
                &[],
                as_of_t,
            )
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for (i, fold) in resolved.folds.iter().enumerate() {
                match fold {
                    Fold::CountRows => counts[i] += batch.num_rows as u64,
                    Fold::CountColumn(col) => {
                        if let Some(c) = batch.column_by_name(col) {
                            counts[i] += non_null_count(c) as u64;
                        }
                    }
                }
            }
            ctx.tracker.consume_fuel(1)?;
        }

        let columns: Vec<Vec<Binding>> = counts
            .iter()
            .map(|&n| vec![Binding::lit(FlakeValue::Long(n as i64), Sid::xsd_integer())])
            .collect();
        self.done = true;
        self.state = OperatorState::Exhausted;
        Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?))
    }

    fn close(&mut self) {
        self.fallback.close();
        self.resolved = None;
        self.state = OperatorState::Closed;
    }
}

impl FusedR2rmlAggregateOperator {
    /// Rewrite inner triples → R2RML at `open` and resolve column folds.
    async fn resolve_at_open(&self, ctx: &ExecutionContext<'_>) -> Result<Option<Resolved>> {
        // Rewrite the inner triples for this graph using the active snapshot.
        // A non-R2RML graph (or an unconvertible pattern) leaves triples
        // unconverted → fall back.
        let rr = rewrite_patterns_for_r2rml(&self.inner_patterns, &self.graph_iri, ctx.active_snapshot);
        if rr.unconverted_count > 0 {
            return Ok(None);
        }
        let pattern = match rr.patterns.as_slice() {
            [Pattern::R2rml(p)] => p.clone(),
            _ => return Ok(None), // multiple scans / star not handled in slice 1
        };

        let provider = ctx
            .r2rml_provider
            .ok_or_else(|| QueryError::InvalidQuery("R2RML provider not configured".to_string()))?;
        let as_of_t = if ctx.dataset.is_some() {
            None
        } else {
            Some(ctx.to_t)
        };
        let mapping = provider
            .compiled_mapping(&pattern.graph_source_id, as_of_t)
            .await?;

        let Some(tm) = Self::resolve_triples_map(&pattern, &mapping) else {
            return Ok(None);
        };
        let Some(table_name) = tm.table_name().map(str::to_string) else {
            return Ok(None);
        };

        let mut folds = Vec::with_capacity(self.aggregates.len());
        let mut projection: Vec<String> = Vec::new();
        for (_, func) in &self.aggregates {
            match func {
                AggregateFn::CountAll => folds.push(Fold::CountRows),
                AggregateFn::Count(v) => {
                    let Some(col) = Self::scalar_column_for_var(&pattern, tm, *v) else {
                        return Ok(None);
                    };
                    projection.push(col.clone());
                    folds.push(Fold::CountColumn(col));
                }
                _ => return Ok(None),
            }
        }
        projection.sort();
        projection.dedup();
        Ok(Some(Resolved {
            pattern,
            table_name,
            projection,
            folds,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::grouping::AggregateSpec;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use crate::ir::{Query, QueryOutput, ReasoningConfig};
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;

    fn graph_triple(s: VarId, o: VarId) -> Pattern {
        Pattern::Graph {
            name: GraphName::Iri(Arc::from("gs:main")),
            patterns: vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                Ref::Sid(Sid::new(100, "p")),
                Term::Var(o),
            ))],
        }
    }

    fn count_query(group_by: Vec<VarId>, patterns: Vec<Pattern>, out: VarId, counted: VarId) -> Query {
        let agg = AggregateSpec {
            function: AggregateFn::Count(counted),
            output_var: out,
        };
        Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![out]),
            patterns,
            reasoning: ReasoningConfig::default(),
            grouping: Grouping::assemble(group_by, vec![agg], vec![], None),
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
            include_system_facts: false,
        }
    }

    #[test]
    fn detects_graph_count_shape() {
        let (s, o, c) = (VarId(0), VarId(1), VarId(2));
        let q = count_query(vec![], vec![graph_triple(s, o)], c, o);
        assert!(detect_fused_r2rml_aggregate(&q).is_some());
    }

    #[test]
    fn declines_with_group_by() {
        // Slice 1 is implicit aggregation only.
        let (s, o, c) = (VarId(0), VarId(1), VarId(2));
        let q = count_query(vec![s], vec![graph_triple(s, o)], c, o);
        assert!(detect_fused_r2rml_aggregate(&q).is_none());
    }

    #[test]
    fn declines_non_graph_pattern() {
        // A bare triple (native ledger scan) is not the fused shape.
        let (s, o, c) = (VarId(0), VarId(1), VarId(2));
        let triple = Pattern::Triple(TriplePattern::new(
            Ref::Var(s),
            Ref::Sid(Sid::new(100, "p")),
            Term::Var(o),
        ));
        let q = count_query(vec![], vec![triple], c, o);
        assert!(detect_fused_r2rml_aggregate(&q).is_none());
    }

    #[test]
    fn declines_with_limit() {
        let (s, o, c) = (VarId(0), VarId(1), VarId(2));
        let mut q = count_query(vec![], vec![graph_triple(s, o)], c, o);
        q.limit = Some(1);
        assert!(detect_fused_r2rml_aggregate(&q).is_none());
    }
}
