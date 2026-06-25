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

use crate::aggregate::NumericAcc;
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::grouping::{AggregateFn, Grouping};
use crate::ir::{GraphName, Pattern, Query, R2rmlPattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::r2rml::rewrite_patterns_for_r2rml;
use crate::var_registry::VarId;
use async_trait::async_trait;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::BigDecimal;
use fluree_db_core::{FlakeValue, Sid};
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ObjectMap, TriplesMap};
use fluree_db_tabular::Column;
use futures::StreamExt;
use std::sync::Arc;

/// Result numeric kind for a SUM/AVG fold, from the object map's declared
/// datatype (the binding path types the result by datatype, not by the parquet
/// physical type).
#[derive(Clone, Copy, PartialEq, Eq)]
enum NumKind {
    Decimal,
    Integer,
    Double,
}

/// Classify an object map's declared datatype IRI into a fold kind, or `None`
/// (not a foldable numeric → fall back).
fn numeric_kind(datatype: Option<&str>) -> Option<NumKind> {
    use fluree_vocab::xsd;
    let dt = datatype?;
    if dt == xsd::DECIMAL {
        Some(NumKind::Decimal)
    } else if dt == xsd::INTEGER || dt == xsd::LONG || dt == xsd::INT {
        Some(NumKind::Integer)
    } else if dt == xsd::DOUBLE || dt == xsd::FLOAT {
        Some(NumKind::Double)
    } else {
        None
    }
}

/// A detected fused-aggregate plan: the enclosing graph IRI, the inner triple
/// patterns (rewritten to R2RML at `open`), the GROUP BY variables, and the
/// per-output aggregate functions.
pub struct FusedAggregatePlan {
    graph_iri: Arc<str>,
    inner_patterns: Vec<Pattern>,
    group_by: Vec<VarId>,
    aggregates: Vec<(VarId, AggregateFn)>,
}

/// Detect the fused shape: a single `GRAPH { triples }` block feeding an
/// aggregation (implicit, or GROUP BY) of only `COUNT` / `SUM` / `AVG`, with no
/// HAVING, post-binds, FILTER, ordering, or slicing. Whether the graph is
/// actually R2RML (and whether the triples collapse to one scan, and the vars
/// map to columns) is checked at `open`.
pub fn detect_fused_r2rml_aggregate(query: &Query) -> Option<FusedAggregatePlan> {
    // Kill switch (A/B and incident response): force the normal pipeline.
    if matches!(
        std::env::var("FLUREE_FUSED_R2RML_AGG").as_deref(),
        Ok("0" | "false")
    ) {
        return None;
    }
    if !query.ordering.is_empty()
        || !query.order_binds.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
    {
        return None;
    }

    // Implicit aggregation, or GROUP BY with aggregates. No HAVING, no
    // post-aggregate binds.
    let (group_by, aggregation, having): (Vec<VarId>, _, _) = match query.grouping.as_ref()? {
        Grouping::Implicit { aggregation, having } => (Vec::new(), aggregation, having),
        Grouping::Explicit {
            group_by,
            aggregation: Some(aggregation),
            having,
        } => (group_by.iter().copied().collect(), aggregation, having),
        // GROUP BY with no aggregates (DISTINCT-style) is not a fold here.
        Grouping::Explicit { .. } => return None,
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

    // Every aggregate must be a column fold this operator supports.
    let mut aggregates = Vec::with_capacity(aggregation.aggregates.len());
    for spec in aggregation.aggregates.iter() {
        if !matches!(
            spec.function,
            AggregateFn::CountAll
                | AggregateFn::Count(_)
                | AggregateFn::Sum(_, _)
                | AggregateFn::Avg(_, _)
        ) {
            return None;
        }
        aggregates.push((spec.output_var, spec.function.clone()));
    }

    // The projection must be exactly the GROUP BY keys + aggregate outputs, so
    // the fused output rows are the final result.
    if let Some(projected) = query.output.projected_vars() {
        let mut outs: std::collections::HashSet<VarId> =
            aggregates.iter().map(|(v, _)| *v).collect();
        outs.extend(group_by.iter().copied());
        if projected.len() != outs.len() || projected.iter().any(|v| !outs.contains(v)) {
            return None;
        }
    }

    Some(FusedAggregatePlan {
        graph_iri,
        inner_patterns: inner,
        group_by,
        aggregates,
    })
}

/// How one output aggregate folds over the scanned column batches.
enum Fold {
    /// `COUNT(*)` — count rows.
    CountRows,
    /// `COUNT(?col)` — count non-null values of this table column.
    CountColumn(String),
    /// `SUM(?col)` / `AVG(?col)` — reduce the numeric column natively (result
    /// typed by the column's declared datatype, not its physical type).
    Numeric {
        column: String,
        kind: NumKind,
        is_avg: bool,
    },
}

/// Running accumulator for one [`Fold`], mutated per batch in `next_batch`.
enum Acc {
    Count(u64),
    /// Exact (decimal/integer) sum: unscaled i128 total + the decimal scale seen
    /// (0 for integers) + non-null count.
    Exact {
        sum: i128,
        scale: i64,
        decimal: bool,
        count: u64,
        is_avg: bool,
    },
    /// Floating sum.
    Double { sum: f64, count: u64, is_avg: bool },
}

impl Acc {
    fn for_fold(fold: &Fold) -> Self {
        match fold {
            Fold::CountRows | Fold::CountColumn(_) => Acc::Count(0),
            Fold::Numeric {
                kind: NumKind::Double,
                is_avg,
                ..
            } => Acc::Double {
                sum: 0.0,
                count: 0,
                is_avg: *is_avg,
            },
            Fold::Numeric { kind, is_avg, .. } => Acc::Exact {
                sum: 0,
                scale: 0,
                decimal: matches!(kind, NumKind::Decimal),
                count: 0,
                is_avg: *is_avg,
            },
        }
    }

    /// Fold one batch's column into this accumulator.
    fn update(&mut self, fold: &Fold, batch: &fluree_db_tabular::ColumnBatch) {
        match (self, fold) {
            (Acc::Count(n), Fold::CountRows) => *n += batch.num_rows as u64,
            (Acc::Count(n), Fold::CountColumn(col)) => {
                if let Some(c) = batch.column_by_name(col) {
                    *n += non_null_count(c) as u64;
                }
            }
            (
                Acc::Exact {
                    sum, scale, count, ..
                },
                Fold::Numeric { column, .. },
            ) => {
                if let Some(c) = batch.column_by_name(column) {
                    accumulate_exact(c, sum, scale, count);
                }
            }
            (Acc::Double { sum, count, .. }, Fold::Numeric { column, .. }) => {
                if let Some(c) = batch.column_by_name(column) {
                    accumulate_double(c, sum, count);
                }
            }
            _ => {}
        }
    }

    /// Fold a single row's value into this accumulator (grouped path). `col` is
    /// the fold's pre-resolved column for this batch (`None` for `COUNT(*)`).
    fn update_row(&mut self, fold: &Fold, col: Option<&Column>, row: usize) {
        match (self, fold) {
            (Acc::Count(n), Fold::CountRows) => *n += 1,
            (Acc::Count(n), Fold::CountColumn(_)) if col.is_some_and(|c| !c.is_null(row)) => {
                *n += 1;
            }
            (Acc::Count(_), Fold::CountColumn(_)) => {}
            (
                Acc::Exact {
                    sum, scale, count, ..
                },
                Fold::Numeric { .. },
            ) => {
                if let Some(c) = col {
                    accumulate_exact_row(c, row, sum, scale, count);
                }
            }
            (Acc::Double { sum, count, .. }, Fold::Numeric { .. }) => {
                if let Some(c) = col {
                    accumulate_double_row(c, row, sum, count);
                }
            }
            _ => {}
        }
    }

    /// Materialize the final result binding for this accumulator.
    fn finalize(self) -> Binding {
        match self {
            Acc::Count(n) => Binding::lit(FlakeValue::Long(n as i64), Sid::xsd_integer()),
            Acc::Exact {
                sum,
                scale,
                decimal,
                count,
                is_avg,
            } => {
                let big = BigDecimal::new(BigInt::from(sum), scale);
                let acc = NumericAcc::from_exact_total(big, count, decimal);
                if is_avg {
                    acc.finalize_avg()
                } else {
                    acc.finalize_sum()
                }
            }
            Acc::Double {
                sum,
                count,
                is_avg,
            } => {
                let acc = NumericAcc::from_double_total(sum, count);
                if is_avg {
                    acc.finalize_avg()
                } else {
                    acc.finalize_sum()
                }
            }
        }
    }
}

/// Sum a numeric column's values into an exact i128 accumulator, capturing the
/// decimal scale. Decimal columns carry their scale; integer/date columns are
/// scale 0.
fn accumulate_exact(col: &Column, sum: &mut i128, scale: &mut i64, count: &mut u64) {
    match col {
        Column::Decimal { values, scale: s, .. } => {
            *scale = *s as i64;
            for v in values.iter().flatten() {
                *sum += *v;
                *count += 1;
            }
        }
        Column::Int64(values) => {
            for v in values.iter().flatten() {
                *sum += *v as i128;
                *count += 1;
            }
        }
        Column::Int32(values) | Column::Date(values) => {
            for v in values.iter().flatten() {
                *sum += *v as i128;
                *count += 1;
            }
        }
        _ => {}
    }
}

/// Sum a numeric column's values into an f64 accumulator (xsd:double/float).
fn accumulate_double(col: &Column, sum: &mut f64, count: &mut u64) {
    match col {
        Column::Float64(values) => {
            for v in values.iter().flatten() {
                *sum += *v;
                *count += 1;
            }
        }
        Column::Float32(values) => {
            for v in values.iter().flatten() {
                *sum += *v as f64;
                *count += 1;
            }
        }
        _ => {}
    }
}

/// Add one row's exact (decimal/integer) value to the accumulator.
fn accumulate_exact_row(col: &Column, row: usize, sum: &mut i128, scale: &mut i64, count: &mut u64) {
    match col {
        Column::Decimal { values, scale: s, .. } => {
            *scale = *s as i64;
            if let Some(Some(v)) = values.get(row) {
                *sum += *v;
                *count += 1;
            }
        }
        Column::Int64(values) => {
            if let Some(Some(v)) = values.get(row) {
                *sum += *v as i128;
                *count += 1;
            }
        }
        Column::Int32(values) | Column::Date(values) => {
            if let Some(Some(v)) = values.get(row) {
                *sum += *v as i128;
                *count += 1;
            }
        }
        _ => {}
    }
}

/// Add one row's floating value to the accumulator.
fn accumulate_double_row(col: &Column, row: usize, sum: &mut f64, count: &mut u64) {
    match col {
        Column::Float64(values) => {
            if let Some(Some(v)) = values.get(row) {
                *sum += *v;
                *count += 1;
            }
        }
        Column::Float32(values) => {
            if let Some(Some(v)) = values.get(row) {
                *sum += *v as f64;
                *count += 1;
            }
        }
        _ => {}
    }
}

/// A GROUP BY key column: which table column, how to read it, and the encoded
/// datatype Sid for the output key binding.
struct GroupCol {
    column: String,
    kind: GKind,
    dt_sid: Sid,
}

/// Supported GROUP BY key column kinds (slice 3).
#[derive(Clone, Copy)]
enum GKind {
    String,
    Integer,
}

/// Classify a declared datatype into a group-key kind, or `None` (fall back).
fn group_kind(datatype: Option<&str>) -> Option<GKind> {
    use fluree_vocab::xsd;
    let dt = datatype?;
    if dt == xsd::STRING {
        Some(GKind::String)
    } else if dt == xsd::INTEGER || dt == xsd::LONG || dt == xsd::INT {
        Some(GKind::Integer)
    } else {
        None
    }
}

/// One component of a composite group key (hashable / comparable).
#[derive(Clone, PartialEq, Eq, Hash)]
enum GKey {
    Str(String),
    Int(i128),
    Null,
}

impl GroupCol {
    /// Read this column's group-key value at a row.
    fn key_at(&self, col: Option<&Column>, row: usize) -> GKey {
        let Some(c) = col else { return GKey::Null };
        match self.kind {
            GKind::String => match c {
                Column::String(v) => v
                    .get(row)
                    .cloned()
                    .flatten()
                    .map_or(GKey::Null, GKey::Str),
                _ => GKey::Null,
            },
            GKind::Integer => match c {
                Column::Int64(v) => v.get(row).and_then(|o| *o).map_or(GKey::Null, |i| GKey::Int(i as i128)),
                Column::Int32(v) => v.get(row).and_then(|o| *o).map_or(GKey::Null, |i| GKey::Int(i as i128)),
                _ => GKey::Null,
            },
        }
    }

    /// Materialize the output binding for a group key component.
    fn binding(&self, key: &GKey) -> Binding {
        match key {
            GKey::Str(s) => Binding::lit(FlakeValue::String(s.clone()), self.dt_sid.clone()),
            GKey::Int(i) => Binding::lit(FlakeValue::Long(*i as i64), self.dt_sid.clone()),
            GKey::Null => Binding::Unbound,
        }
    }
}

/// Resolved fused plan (post-`open`): the rewritten scan pattern, the table to
/// scan, the columns to project, the GROUP BY key columns, and the per-output
/// fold.
struct Resolved {
    pattern: R2rmlPattern,
    table_name: String,
    projection: Vec<String>,
    group_cols: Vec<GroupCol>,
    folds: Vec<Fold>,
}

/// Fused R2RML aggregate operator. Folds COUNT/SUM/AVG aggregates straight from
/// column batches; falls back to the normal pipeline when its soundness gates
/// fail.
pub struct FusedR2rmlAggregateOperator {
    graph_iri: Arc<str>,
    inner_patterns: Vec<Pattern>,
    group_by: Vec<VarId>,
    aggregates: Vec<(VarId, AggregateFn)>,
    /// Output schema: GROUP BY key vars followed by aggregate output vars.
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
        // Output schema: GROUP BY keys first, then aggregate outputs. Downstream
        // result formatting resolves columns by variable, so this order is safe.
        let schema: Arc<[VarId]> = plan
            .group_by
            .iter()
            .copied()
            .chain(plan.aggregates.iter().map(|(v, _)| *v))
            .collect();
        Self {
            graph_iri: plan.graph_iri,
            inner_patterns: plan.inner_patterns,
            group_by: plan.group_by,
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

    /// Resolve the single scalar column (and its declared datatype) a variable's
    /// predicate maps to, or `None` (gate fail) for a RefObjectMap join, a
    /// multi-valued predicate, or a non-column object map.
    fn scalar_column_for_var(
        pattern: &R2rmlPattern,
        tm: &TriplesMap,
        var: VarId,
    ) -> Option<(String, Option<String>)> {
        let pred = Self::predicate_for_var(pattern, var)?;
        let mut poms = tm
            .predicate_object_maps
            .iter()
            .filter(|pom| pom.predicate_map.as_constant() == Some(pred));
        let (Some(pom), None) = (poms.next(), poms.next()) else {
            return None; // missing or multi-valued predicate
        };
        match &pom.object_map {
            ObjectMap::Column {
                column, datatype, ..
            } => Some((column.clone(), datatype.clone())),
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
                "fused R2RML aggregate: folding from column batches"
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

        let mut stream = table_provider
            .scan_table(
                &resolved.pattern.graph_source_id,
                &resolved.table_name,
                &resolved.projection,
                &[],
                as_of_t,
            )
            .await?;

        let columns = if resolved.group_cols.is_empty() {
            // Implicit aggregation: one group, vectorized per-batch fold.
            let mut accs: Vec<Acc> = resolved.folds.iter().map(Acc::for_fold).collect();
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                for (acc, fold) in accs.iter_mut().zip(resolved.folds.iter()) {
                    acc.update(fold, &batch);
                }
                ctx.tracker.consume_fuel(1)?;
            }
            // One result row: each accumulator's final binding, in schema order.
            accs.into_iter().map(|acc| vec![acc.finalize()]).collect()
        } else {
            // GROUP BY: per-row grouping into per-key accumulators.
            let folds = &resolved.folds;
            let gcols = &resolved.group_cols;
            let mut groups: std::collections::HashMap<Vec<GKey>, Vec<Acc>> =
                std::collections::HashMap::new();
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                // Resolve each fold's / group column's column once per batch.
                let fold_cols: Vec<Option<&Column>> = folds
                    .iter()
                    .map(|f| match f {
                        Fold::CountRows => None,
                        Fold::CountColumn(c) | Fold::Numeric { column: c, .. } => {
                            batch.column_by_name(c)
                        }
                    })
                    .collect();
                let key_cols: Vec<Option<&Column>> =
                    gcols.iter().map(|g| batch.column_by_name(&g.column)).collect();
                for row in 0..batch.num_rows {
                    let key: Vec<GKey> = gcols
                        .iter()
                        .zip(&key_cols)
                        .map(|(g, c)| g.key_at(*c, row))
                        .collect();
                    let accs = groups
                        .entry(key)
                        .or_insert_with(|| folds.iter().map(Acc::for_fold).collect());
                    for (i, fold) in folds.iter().enumerate() {
                        accs[i].update_row(fold, fold_cols[i], row);
                    }
                }
                ctx.tracker.consume_fuel(1)?;
            }

            // One output row per group: key bindings then aggregate bindings.
            let num_cols = gcols.len() + folds.len();
            let mut out: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();
            for (key, accs) in groups {
                for (i, g) in gcols.iter().enumerate() {
                    out[i].push(g.binding(&key[i]));
                }
                for (j, acc) in accs.into_iter().enumerate() {
                    out[gcols.len() + j].push(acc.finalize());
                }
            }
            out
        };

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

        let mut projection: Vec<String> = Vec::new();

        // Resolve GROUP BY key columns (string / integer in slice 3). The output
        // key binding's datatype Sid is encoded from the snapshot so it matches
        // what the normal materialization path produces.
        let mut group_cols = Vec::with_capacity(self.group_by.len());
        for gv in &self.group_by {
            let Some((col, datatype)) = Self::scalar_column_for_var(&pattern, tm, *gv) else {
                return Ok(None);
            };
            let Some(kind) = group_kind(datatype.as_deref()) else {
                return Ok(None);
            };
            let Some(dt_iri) = datatype.as_deref() else {
                return Ok(None);
            };
            let Some(dt_sid) = ctx.active_snapshot.encode_iri(dt_iri) else {
                return Ok(None);
            };
            projection.push(col.clone());
            group_cols.push(GroupCol {
                column: col,
                kind,
                dt_sid,
            });
        }

        let mut folds = Vec::with_capacity(self.aggregates.len());
        for (_, func) in &self.aggregates {
            match func {
                AggregateFn::CountAll => folds.push(Fold::CountRows),
                AggregateFn::Count(v) => {
                    let Some((col, _)) = Self::scalar_column_for_var(&pattern, tm, *v) else {
                        return Ok(None);
                    };
                    projection.push(col.clone());
                    folds.push(Fold::CountColumn(col));
                }
                AggregateFn::Sum(v, _) | AggregateFn::Avg(v, _) => {
                    let Some((col, datatype)) = Self::scalar_column_for_var(&pattern, tm, *v) else {
                        return Ok(None);
                    };
                    // Only numeric declared datatypes fold; anything else (string,
                    // date, untyped) goes to the fallback.
                    let Some(kind) = numeric_kind(datatype.as_deref()) else {
                        return Ok(None);
                    };
                    projection.push(col.clone());
                    folds.push(Fold::Numeric {
                        column: col,
                        kind,
                        is_avg: matches!(func, AggregateFn::Avg(_, _)),
                    });
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
            group_cols,
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

    #[test]
    fn detects_group_by_shape() {
        // GROUP BY ?g over a graph block with a COUNT aggregate.
        let (s, o, g, c) = (VarId(0), VarId(1), VarId(2), VarId(3));
        let agg = AggregateSpec {
            function: AggregateFn::Count(o),
            output_var: c,
        };
        let q = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![g, c]),
            patterns: vec![graph_triple(s, o)],
            reasoning: ReasoningConfig::default(),
            grouping: Grouping::assemble(vec![g], vec![agg], vec![], None),
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
            include_system_facts: false,
        };
        assert!(detect_fused_r2rml_aggregate(&q).is_some());
    }

    #[test]
    fn detects_sum_and_avg_shapes() {
        use crate::ir::grouping::InputSemantics;
        let (s, o, out) = (VarId(0), VarId(1), VarId(2));
        for func in [
            AggregateFn::Sum(o, InputSemantics::List),
            AggregateFn::Avg(o, InputSemantics::List),
        ] {
            let agg = AggregateSpec {
                function: func,
                output_var: out,
            };
            let q = Query {
                context: ParsedContext::default(),
                orig_context: None,
                output: QueryOutput::select_all(vec![out]),
                patterns: vec![graph_triple(s, o)],
                reasoning: ReasoningConfig::default(),
                grouping: Grouping::assemble(vec![], vec![agg], vec![], None),
                ordering: Vec::new(),
                order_binds: Vec::new(),
                limit: None,
                offset: None,
                post_values: None,
                include_system_facts: false,
            };
            assert!(detect_fused_r2rml_aggregate(&q).is_some());
        }
    }
}
