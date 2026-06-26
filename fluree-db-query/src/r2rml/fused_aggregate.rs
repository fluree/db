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
//! # Scope
//!
//! `COUNT` / `SUM` / `AVG` (multiset only — DISTINCT falls back) over one
//! TriplesMap, optionally with GROUP BY keys, a FILTER, and exact decimal
//! arithmetic in the aggregate (`SUM(?a * (1 - ?b))`). Joins, DISTINCT
//! aggregates, and all-integer expression results fall back.
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
use crate::binding::{Batch, Binding, BindingRow};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::eval::PreparedBoolExpression;
use crate::ir::grouping::{AggregateFn, Grouping, InputSemantics};
use crate::ir::{Expression, Function, GraphName, Pattern, Query, R2rmlPattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::r2rml::operator::LiteralEncoder;
use crate::r2rml::rewrite_patterns_for_r2rml;
use crate::var_registry::VarId;
use async_trait::async_trait;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::{BigDecimal, ToPrimitive};
use fluree_db_core::{FlakeValue, Sid};
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ObjectMap, TriplesMap};
use fluree_db_r2rml::materialize::materialize_object_from_batch;
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

/// How to read a numeric column value as an exact decimal during native
/// expression evaluation.
#[derive(Clone, Copy)]
enum DecKind {
    Decimal,
    Integer,
}

/// An exact fixed-point decimal `val * 10^-scale`, mirroring BigDecimal's
/// (unscaled, scale) form so native `+`/`-`/`*` reproduce the engine's exact
/// (no-rounding) decimal arithmetic. `i128` carries ~38 digits — ample for
/// analytical decimal aggregates; an intermediate beyond that yields `None`
/// (the row is skipped) rather than a wrong wraparound.
#[derive(Clone, Copy)]
struct Dec {
    val: i128,
    scale: i64,
}

impl Dec {
    fn mul(self, o: Dec) -> Option<Dec> {
        Some(Dec {
            val: self.val.checked_mul(o.val)?,
            scale: self.scale + o.scale,
        })
    }

    /// Add (or subtract) after aligning to the larger scale, exactly as
    /// BigDecimal does.
    fn add_sub(self, o: Dec, sub: bool) -> Option<Dec> {
        let scale = self.scale.max(o.scale);
        let a = self.val.checked_mul(pow10(scale - self.scale)?)?;
        let b = o.val.checked_mul(pow10(scale - o.scale)?)?;
        let val = if sub {
            a.checked_sub(b)?
        } else {
            a.checked_add(b)?
        };
        Some(Dec { val, scale })
    }
}

fn pow10(n: i64) -> Option<i128> {
    10i128.checked_pow(u32::try_from(n).ok()?)
}

/// Convert a numeric constant to an exact decimal (integers / i128-fitting
/// decimals only).
fn const_to_dec(fv: &FlakeValue) -> Option<Dec> {
    match fv {
        FlakeValue::Long(n) => Some(Dec {
            val: *n as i128,
            scale: 0,
        }),
        FlakeValue::Decimal(bd) => {
            let (bigint, exp) = bd.as_bigint_and_exponent();
            Some(Dec {
                val: bigint.to_i128()?,
                scale: exp,
            })
        }
        _ => None,
    }
}

/// Whether an expression is a native-foldable decimal arithmetic tree
/// (`Var` / numeric `Const` / `+` `-` `*` `negate`). Division is excluded — it
/// rounds, so it stays on the engine path.
fn expr_native_foldable(expr: &Expression) -> bool {
    match expr {
        Expression::Var(_) => true,
        Expression::Const(fv) => const_to_dec(fv).is_some(),
        Expression::Call { func, args } => {
            matches!(
                func,
                Function::Add | Function::Sub | Function::Mul | Function::Negate
            ) && args.iter().all(expr_native_foldable)
        }
        _ => false,
    }
}

/// True if the foldable expression contains a decimal constant, so its result is
/// `xsd:decimal` even when every referenced column is integer.
fn expr_has_decimal_const(expr: &Expression) -> bool {
    match expr {
        Expression::Const(fv) => matches!(fv, FlakeValue::Decimal(_)),
        Expression::Call { args, .. } => args.iter().any(expr_has_decimal_const),
        _ => false,
    }
}

/// Evaluate a native decimal arithmetic expression for one row. `vars` gives the
/// already-read value of each referenced variable (`None` = null → the whole
/// expression is `None`, and the row is skipped, matching SUM's null handling).
fn eval_dec(expr: &Expression, vars: &[(VarId, Option<Dec>)]) -> Option<Dec> {
    match expr {
        Expression::Var(v) => vars.iter().find(|(x, _)| x == v).and_then(|(_, d)| *d),
        Expression::Const(fv) => const_to_dec(fv),
        Expression::Call { func, args } => match (func, args.as_slice()) {
            (Function::Add, [a, b]) => eval_dec(a, vars)?.add_sub(eval_dec(b, vars)?, false),
            (Function::Sub, [a, b]) => eval_dec(a, vars)?.add_sub(eval_dec(b, vars)?, true),
            (Function::Mul, [a, b]) => eval_dec(a, vars)?.mul(eval_dec(b, vars)?),
            (Function::Negate, [a]) => {
                let d = eval_dec(a, vars)?;
                Some(Dec {
                    val: d.val.checked_neg()?,
                    scale: d.scale,
                })
            }
            _ => None,
        },
        _ => None,
    }
}

/// Read a numeric column's value at a row as an exact decimal.
fn read_dec(col: Option<&Column>, kind: DecKind, row: usize) -> Option<Dec> {
    let col = col?;
    match (kind, col) {
        (DecKind::Decimal, Column::Decimal { values, scale, .. }) => {
            values.get(row).copied().flatten().map(|v| Dec {
                val: v,
                scale: *scale as i64,
            })
        }
        (_, Column::Int64(values)) => values.get(row).copied().flatten().map(|v| Dec {
            val: v as i128,
            scale: 0,
        }),
        (_, Column::Int32(values) | Column::Date(values)) => {
            values.get(row).copied().flatten().map(|v| Dec {
                val: v as i128,
                scale: 0,
            })
        }
        _ => None,
    }
}

/// A detected fused-aggregate plan: the enclosing graph IRI, the inner triple
/// patterns (rewritten to R2RML at `open`), the GROUP BY variables, and the
/// per-output aggregate functions.
pub struct FusedAggregatePlan {
    graph_iri: Arc<str>,
    /// The triple patterns (rewritten to R2RML at open); the FILTER, if any, is
    /// held separately and applied per row during the fold.
    inner_patterns: Vec<Pattern>,
    filter: Option<Expression>,
    /// Synthetic aggregate-input variables defined by top-level BINDs (the
    /// desugared `SUM(expr)` / `AVG(expr)` arguments), folded natively per row.
    agg_binds: Vec<(VarId, Expression)>,
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
    // ORDER BY / LIMIT / OFFSET are applied by wrapping the fused operator in the
    // engine's own sort/offset/limit operators (see the operator-tree hook), so
    // they're allowed here. Expression ORDER BY (a synthetic sort var) is not.
    if !query.order_binds.is_empty() {
        return None;
    }

    // Implicit aggregation, or GROUP BY with aggregates. No HAVING, no
    // post-aggregate binds.
    let (group_by, aggregation, having): (Vec<VarId>, _, _) = match query.grouping.as_ref()? {
        Grouping::Implicit {
            aggregation,
            having,
        } => (Vec::new(), aggregation, having),
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

    // `GRAPH <iri> { triples... [FILTER] }` optionally followed by top-level
    // `BIND`s (the desugared aggregate-input expressions, e.g. SUM(?a*?b)).
    let (graph_pat, rest) = query.patterns.split_first()?;
    let (graph_iri, graph_inner) = match graph_pat {
        Pattern::Graph {
            name: GraphName::Iri(iri),
            patterns,
        } => (Arc::clone(iri), patterns),
        _ => return None,
    };
    let mut inner = Vec::with_capacity(graph_inner.len());
    let mut filter: Option<Expression> = None;
    for p in graph_inner {
        match p {
            Pattern::Triple(_) => inner.push(p.clone()),
            Pattern::Filter(e) if filter.is_none() => filter = Some(e.clone()),
            _ => return None,
        }
    }
    if inner.is_empty() {
        return None;
    }
    let mut agg_binds: Vec<(VarId, Expression)> = Vec::new();
    for p in rest {
        match p {
            Pattern::Bind { var, expr } => agg_binds.push((*var, expr.clone())),
            _ => return None,
        }
    }

    // Cost guard: a FILTER is only fused alongside a GROUP BY. There the fused
    // path's win (skipping the subject + the many grouped/aggregated columns)
    // dwarfs the per-row filter eval. For a filtered single aggregate the normal
    // pipeline's file pruning + vectorized filter is faster, so decline.
    if filter.is_some() && group_by.is_empty() {
        return None;
    }

    // Every aggregate must be a column fold this operator supports.
    let mut aggregates = Vec::with_capacity(aggregation.aggregates.len());
    for spec in aggregation.aggregates.iter() {
        // Only multiset (non-DISTINCT) COUNT/SUM/AVG fold from columns; the fused
        // path has no dedup, so DISTINCT (Set) must fall back to the normal
        // pipeline. `CountDistinct` is already a separate, unmatched variant.
        let foldable = match &spec.function {
            AggregateFn::CountAll | AggregateFn::Count(_) => true,
            AggregateFn::Sum(_, sem) | AggregateFn::Avg(_, sem) => {
                matches!(sem, InputSemantics::List)
            }
            _ => false,
        };
        if !foldable {
            return None;
        }
        aggregates.push((spec.output_var, spec.function.clone()));
    }

    // Output variables = GROUP BY keys + aggregate outputs.
    let mut outs: std::collections::HashSet<VarId> = aggregates.iter().map(|(v, _)| *v).collect();
    outs.extend(group_by.iter().copied());

    // The projection must be exactly those, so the fused output rows are the
    // final result; and any ORDER BY must sort by them (the wrapping
    // SortOperator only sees the fused output schema).
    if let Some(projected) = query.output.projected_vars() {
        if projected.len() != outs.len() || projected.iter().any(|v| !outs.contains(v)) {
            return None;
        }
    }
    if query.ordering.iter().any(|s| !outs.contains(&s.var)) {
        return None;
    }

    Some(FusedAggregatePlan {
        graph_iri,
        inner_patterns: inner,
        filter,
        agg_binds,
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
    /// `SUM(expr)` / `AVG(expr)` over a native decimal arithmetic expression;
    /// `index` points into `Resolved::expr_folds`.
    NumericExpr { index: usize, is_avg: bool },
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
    Double {
        sum: f64,
        count: u64,
        is_avg: bool,
    },
    /// Native decimal expression sum: unscaled i128 total + the (constant) result
    /// scale + non-null count.
    Expr {
        sum: i128,
        scale: i64,
        count: u64,
        is_avg: bool,
    },
}

impl Acc {
    fn for_fold(fold: &Fold) -> Self {
        match fold {
            Fold::CountRows | Fold::CountColumn(_) => Acc::Count(0),
            Fold::NumericExpr { is_avg, .. } => Acc::Expr {
                sum: 0,
                scale: 0,
                count: 0,
                is_avg: *is_avg,
            },
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

    /// Fold a single row's value into this accumulator. `col` is the fold's
    /// pre-resolved column for this batch (`None` for `COUNT(*)`). Returns
    /// `false` if an exact i128 sum would overflow (the caller re-runs on the
    /// BigDecimal pipeline); all other folds always return `true`.
    fn update_row(&mut self, fold: &Fold, col: Option<&Column>, row: usize) -> bool {
        match (self, fold) {
            (Acc::Count(n), Fold::CountRows) => {
                *n += 1;
                true
            }
            (Acc::Count(n), Fold::CountColumn(_)) if col.is_some_and(|c| !c.is_null(row)) => {
                *n += 1;
                true
            }
            (Acc::Count(_), Fold::CountColumn(_)) => true,
            (
                Acc::Exact {
                    sum, scale, count, ..
                },
                Fold::Numeric { .. },
            ) => match col {
                Some(c) => accumulate_exact_row(c, row, sum, scale, count),
                None => true,
            },
            (Acc::Double { sum, count, .. }, Fold::Numeric { .. }) => {
                if let Some(c) = col {
                    accumulate_double_row(c, row, sum, count);
                }
                true
            }
            _ => true,
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
            Acc::Double { sum, count, is_avg } => {
                let acc = NumericAcc::from_double_total(sum, count);
                if is_avg {
                    acc.finalize_avg()
                } else {
                    acc.finalize_sum()
                }
            }
            Acc::Expr {
                sum,
                scale,
                count,
                is_avg,
            } => {
                let big = BigDecimal::new(BigInt::from(sum), scale);
                let acc = NumericAcc::from_exact_total(big, count, true);
                if is_avg {
                    acc.finalize_avg()
                } else {
                    acc.finalize_sum()
                }
            }
        }
    }
}

/// Add one row's exact (decimal/integer) value to the accumulator. Returns
/// `false` if the i128 sum would overflow (the caller falls back to BigDecimal).
fn accumulate_exact_row(
    col: &Column,
    row: usize,
    sum: &mut i128,
    scale: &mut i64,
    count: &mut u64,
) -> bool {
    let add = |sum: &mut i128, count: &mut u64, v: i128| match sum.checked_add(v) {
        Some(s) => {
            *sum = s;
            *count += 1;
            true
        }
        None => false,
    };
    match col {
        Column::Decimal {
            values, scale: s, ..
        } => {
            *scale = *s as i64;
            match values.get(row) {
                Some(Some(v)) => add(sum, count, *v),
                _ => true,
            }
        }
        Column::Int64(values) => match values.get(row) {
            Some(Some(v)) => add(sum, count, *v as i128),
            _ => true,
        },
        Column::Int32(values) | Column::Date(values) => match values.get(row) {
            Some(Some(v)) => add(sum, count, *v as i128),
            _ => true,
        },
        _ => true,
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
                Column::String(v) => v.get(row).cloned().flatten().map_or(GKey::Null, GKey::Str),
                _ => GKey::Null,
            },
            GKind::Integer => match c {
                Column::Int64(v) => v
                    .get(row)
                    .and_then(|o| *o)
                    .map_or(GKey::Null, |i| GKey::Int(i as i128)),
                Column::Int32(v) => v
                    .get(row)
                    .and_then(|o| *o)
                    .map_or(GKey::Null, |i| GKey::Int(i as i128)),
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
    filter: Option<FilterPlan>,
    /// Native decimal expression aggregate plans, indexed by `Fold::NumericExpr`.
    expr_folds: Vec<ExprFold>,
    /// Columns that must all be non-null for a row to participate, mirroring the
    /// R2RML star's row-drop: the subject template columns plus every predicate's
    /// object column.
    validity_cols: Vec<String>,
}

/// A native `SUM(expr)` / `AVG(expr)` plan: the arithmetic expression and the
/// (variable, column, read-kind) of each referenced variable.
struct ExprFold {
    expr: Expression,
    var_cols: Vec<(VarId, String, DecKind)>,
}

/// Per-row FILTER evaluation plan. The filter expression is evaluated through the
/// engine's own evaluator (`PreparedBoolExpression`) against a `BindingRow` built
/// from the referenced object columns, so semantics are identical to the normal
/// FILTER operator — only the subject and unreferenced columns are skipped.
struct FilterPlan {
    prepared: PreparedBoolExpression,
    /// Object maps for the referenced variables, aligned with `eval_vars`.
    eval_objmaps: Vec<ObjectMap>,
    /// The referenced variables (the `BindingRow` schema), aligned with the
    /// object maps.
    eval_vars: Arc<[VarId]>,
    encoder: LiteralEncoder,
}

/// Fused R2RML aggregate operator. Folds COUNT/SUM/AVG aggregates straight from
/// column batches; falls back to the normal pipeline when its soundness gates
/// fail.
pub struct FusedR2rmlAggregateOperator {
    graph_iri: Arc<str>,
    inner_patterns: Vec<Pattern>,
    filter: Option<Expression>,
    agg_binds: Vec<(VarId, Expression)>,
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
            filter: plan.filter,
            agg_binds: plan.agg_binds,
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

    /// Resolve the (single, scalar-column) object map a variable's predicate maps
    /// to, for materializing the variable's value during FILTER evaluation.
    fn object_map_for_var(
        pattern: &R2rmlPattern,
        tm: &TriplesMap,
        var: VarId,
    ) -> Option<ObjectMap> {
        let pred = Self::predicate_for_var(pattern, var)?;
        let mut poms = tm
            .predicate_object_maps
            .iter()
            .filter(|pom| pom.predicate_map.as_constant() == Some(pred));
        let (Some(pom), None) = (poms.next(), poms.next()) else {
            return None;
        };
        match &pom.object_map {
            ObjectMap::Column { .. } => Some(pom.object_map.clone()),
            _ => None,
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

        let folds = &resolved.folds;
        let gcols = &resolved.group_cols;

        // Each row is gated by the R2RML star's row-validity (subject + object
        // columns non-null) and the optional FILTER, then folded. Implicit
        // aggregation uses a single flat accumulator set (no per-row hashing);
        // GROUP BY keys one set per group. An exact i128 sum that would overflow
        // sets `overflowed` and the whole query re-runs on the exact pipeline.
        let mut implicit: Vec<Acc> = folds.iter().map(Acc::for_fold).collect();
        let mut groups: std::collections::HashMap<Vec<GKey>, Vec<Acc>> =
            std::collections::HashMap::new();
        let mut overflowed = false;
        'scan: while let Some(batch) = stream.next().await {
            let batch = batch?;
            let fold_cols: Vec<Option<&Column>> = folds
                .iter()
                .map(|f| match f {
                    Fold::CountRows | Fold::NumericExpr { .. } => None,
                    Fold::CountColumn(c) | Fold::Numeric { column: c, .. } => {
                        batch.column_by_name(c)
                    }
                })
                .collect();
            let key_cols: Vec<Option<&Column>> = gcols
                .iter()
                .map(|g| batch.column_by_name(&g.column))
                .collect();
            let validity: Vec<Option<&Column>> = resolved
                .validity_cols
                .iter()
                .map(|c| batch.column_by_name(c))
                .collect();
            // Pre-resolve each expression aggregate's variable columns once.
            let expr_cols: Vec<Vec<Option<&Column>>> = resolved
                .expr_folds
                .iter()
                .map(|ef| {
                    ef.var_cols
                        .iter()
                        .map(|(_, c, _)| batch.column_by_name(c))
                        .collect()
                })
                .collect();
            for row in 0..batch.num_rows {
                // Row-validity (R2RML star row-drop): the subject and every
                // predicate's object column must be non-null.
                if validity
                    .iter()
                    .any(|c| c.is_none_or(|col| col.is_null(row)))
                {
                    continue;
                }
                if let Some(fp) = &resolved.filter {
                    // Materialize only the referenced object columns into a
                    // binding row and evaluate through the engine evaluator.
                    let binds: Vec<Binding> = fp
                        .eval_objmaps
                        .iter()
                        .map(|om| match materialize_object_from_batch(om, &batch, row) {
                            Ok(Some(term)) => fp.encoder.encode(&term),
                            _ => Binding::Unbound,
                        })
                        .collect();
                    let rv = BindingRow::new(&fp.eval_vars, &binds);
                    if !fp.prepared.eval_to_bool_non_strict(&rv, Some(ctx))? {
                        continue;
                    }
                }
                let accs: &mut Vec<Acc> = if gcols.is_empty() {
                    &mut implicit
                } else {
                    let key: Vec<GKey> = gcols
                        .iter()
                        .zip(&key_cols)
                        .map(|(g, c)| g.key_at(*c, row))
                        .collect();
                    groups
                        .entry(key)
                        .or_insert_with(|| folds.iter().map(Acc::for_fold).collect())
                };
                for (i, fold) in folds.iter().enumerate() {
                    let ok = match fold {
                        Fold::NumericExpr { index, .. } => {
                            let ef = &resolved.expr_folds[*index];
                            // Read each referenced variable's value, then evaluate
                            // the arithmetic natively (no allocation).
                            let vars: Vec<(VarId, Option<Dec>)> = ef
                                .var_cols
                                .iter()
                                .enumerate()
                                .map(|(k, (v, _, kind))| {
                                    (*v, read_dec(expr_cols[*index][k], *kind, row))
                                })
                                .collect();
                            match (&mut accs[i], eval_dec(&ef.expr, &vars)) {
                                (
                                    Acc::Expr {
                                        sum, scale, count, ..
                                    },
                                    Some(d),
                                ) => match sum.checked_add(d.val) {
                                    Some(s) => {
                                        *sum = s;
                                        *scale = d.scale;
                                        *count += 1;
                                        true
                                    }
                                    None => false,
                                },
                                _ => true,
                            }
                        }
                        _ => accs[i].update_row(fold, fold_cols[i], row),
                    };
                    if !ok {
                        overflowed = true;
                        break 'scan;
                    }
                }
            }
            ctx.tracker.consume_fuel(1)?;
        }

        // An i128 accumulator overflowed: the exact answer needs BigDecimal, so
        // run the whole query on the normal pipeline instead (nothing has been
        // emitted yet, so this is a clean handoff).
        if overflowed {
            self.use_fallback = true;
            self.fallback.open(ctx).await?;
            return self.fallback.next_batch(ctx).await;
        }

        let columns: Vec<Vec<Binding>> = if gcols.is_empty() {
            implicit.into_iter().map(|a| vec![a.finalize()]).collect()
        } else {
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
        let rr =
            rewrite_patterns_for_r2rml(&self.inner_patterns, &self.graph_iri, ctx.active_snapshot);
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

        // Synthetic aggregate-input expressions (the desugared `SUM(expr)` args).
        let bind_lookup: std::collections::HashMap<VarId, &Expression> =
            self.agg_binds.iter().map(|(v, e)| (*v, e)).collect();

        let mut folds = Vec::with_capacity(self.aggregates.len());
        let mut expr_folds: Vec<ExprFold> = Vec::new();
        for (_, func) in &self.aggregates {
            match func {
                AggregateFn::CountAll => folds.push(Fold::CountRows),
                AggregateFn::Count(v) if *v == pattern.subject_var => {
                    // COUNT of the subject counts the rows that produce a row,
                    // which the row-validity gate already enforces.
                    folds.push(Fold::CountRows);
                }
                AggregateFn::Count(v) => {
                    let Some((col, _)) = Self::scalar_column_for_var(&pattern, tm, *v) else {
                        return Ok(None);
                    };
                    projection.push(col.clone());
                    folds.push(Fold::CountColumn(col));
                }
                AggregateFn::Sum(v, _) | AggregateFn::Avg(v, _) => {
                    let is_avg = matches!(func, AggregateFn::Avg(_, _));
                    if let Some(expr) = bind_lookup.get(v) {
                        // Aggregate over a desugared expression: native decimal fold.
                        if !expr_native_foldable(expr) {
                            return Ok(None);
                        }
                        let mut var_cols = Vec::new();
                        for ev in expr.referenced_vars() {
                            let Some((col, datatype)) =
                                Self::scalar_column_for_var(&pattern, tm, ev)
                            else {
                                return Ok(None);
                            };
                            let deck = match numeric_kind(datatype.as_deref()) {
                                Some(NumKind::Decimal) => DecKind::Decimal,
                                Some(NumKind::Integer) => DecKind::Integer,
                                // floats aren't exact decimals → engine path.
                                _ => return Ok(None),
                            };
                            projection.push(col.clone());
                            var_cols.push((ev, col, deck));
                        }
                        // The native expr fold always finalizes as xsd:decimal.
                        // An all-integer expression (no decimal column or
                        // constant) would be xsd:integer in the normal pipeline,
                        // so fall back to keep the result datatype exact.
                        let any_decimal = var_cols
                            .iter()
                            .any(|(_, _, k)| matches!(k, DecKind::Decimal))
                            || expr_has_decimal_const(expr);
                        if !any_decimal {
                            return Ok(None);
                        }
                        let index = expr_folds.len();
                        expr_folds.push(ExprFold {
                            expr: (*expr).clone(),
                            var_cols,
                        });
                        folds.push(Fold::NumericExpr { index, is_avg });
                    } else {
                        // Aggregate over a bare numeric column: native fold.
                        let Some((col, datatype)) = Self::scalar_column_for_var(&pattern, tm, *v)
                        else {
                            return Ok(None);
                        };
                        let Some(kind) = numeric_kind(datatype.as_deref()) else {
                            return Ok(None);
                        };
                        projection.push(col.clone());
                        folds.push(Fold::Numeric {
                            column: col,
                            kind,
                            is_avg,
                        });
                    }
                }
                _ => return Ok(None),
            }
        }

        // Row-validity columns. A row participates only if the subject template
        // columns and every predicate's object column are non-null — mirroring
        // the R2RML star's row-drop (and a null subject template never
        // materializes a subject). This always applies: even a single-predicate
        // COUNT(*) over a nullable column, or a SUM over a row whose subject key
        // is null, must match the normal pipeline. Because this is always
        // non-empty, the vectorized fast path is gated off and the per-row path
        // (which enforces it) runs — the win is still skipping RDF
        // materialization, not skipping null checks.
        let mut validity_cols: Vec<String> = tm.subject_map.template_columns.clone();
        if let Some(c) = &tm.subject_map.column {
            validity_cols.push(c.clone());
        }
        let mut obj_vars: Vec<VarId> = pattern.object_var.into_iter().collect();
        obj_vars.extend(pattern.star_bindings.iter().map(|(_, v)| *v));
        for v in obj_vars {
            let Some((col, _)) = Self::scalar_column_for_var(&pattern, tm, v) else {
                return Ok(None);
            };
            validity_cols.push(col);
        }
        for c in &validity_cols {
            projection.push(c.clone());
        }
        validity_cols.sort();
        validity_cols.dedup();

        // FILTER: resolve each referenced variable to its object map (for per-row
        // materialization) and prepare the expression for the engine evaluator.
        let filter = if let Some(expr) = &self.filter {
            let eval_vars = expr.referenced_vars();
            let mut eval_objmaps = Vec::with_capacity(eval_vars.len());
            for v in &eval_vars {
                let Some(om) = Self::object_map_for_var(&pattern, tm, *v) else {
                    return Ok(None); // filter references a non-column var → fall back
                };
                for col in om.referenced_columns() {
                    projection.push(col.to_string());
                }
                eval_objmaps.push(om);
            }
            Some(FilterPlan {
                prepared: PreparedBoolExpression::new(expr.clone()),
                eval_objmaps,
                eval_vars: Arc::from(eval_vars),
                encoder: LiteralEncoder::build(tm, ctx.active_snapshot),
            })
        } else {
            None
        };

        projection.sort();
        projection.dedup();
        Ok(Some(Resolved {
            pattern,
            table_name,
            projection,
            group_cols,
            folds,
            filter,
            expr_folds,
            validity_cols,
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

    fn count_query(
        group_by: Vec<VarId>,
        patterns: Vec<Pattern>,
        out: VarId,
        counted: VarId,
    ) -> Query {
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
    fn allows_limit() {
        // LIMIT is applied by wrapping the fused operator, so detection still
        // fires.
        let (s, o, c) = (VarId(0), VarId(1), VarId(2));
        let mut q = count_query(vec![], vec![graph_triple(s, o)], c, o);
        q.limit = Some(1);
        assert!(detect_fused_r2rml_aggregate(&q).is_some());
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
