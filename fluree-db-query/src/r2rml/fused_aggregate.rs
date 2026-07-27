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
use crate::r2rml::operator::{
    decimal_canonical_of, object_column_is_numeric, rdf_term_eq_object_constant_cached,
    LiteralEncoder,
};
use crate::r2rml::rewrite_patterns_for_r2rml;
use crate::r2rml::ObjectConstant;
use crate::var_registry::VarId;
use async_trait::async_trait;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::{BigDecimal, ToPrimitive};
use fluree_db_core::{FlakeValue, Sid};
use fluree_db_r2rml::mapping::{
    extract_template_columns, CompiledR2rmlMapping, ObjectMap, PredicateObjectMap, TermType,
    TriplesMap,
};
use fluree_db_r2rml::materialize::{get_join_key_from_batch, materialize_object_from_batch};
use fluree_db_tabular::{Column, ColumnBatch};
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
/// analytical decimal aggregates; an intermediate beyond that yields `None`, and
/// the caller escalates to the exact BigDecimal pipeline rather than wrap around
/// or drop the row.
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

/// Outcome of evaluating a fused decimal expression for one row. A null operand
/// and an i128 overflow are kept distinct: the former is a legitimate row drop
/// (matching SUM/AVG null handling), the latter must escalate to the exact
/// BigDecimal pipeline rather than silently omit the row.
enum DecEval {
    /// Exact value to fold in.
    Val(Dec),
    /// A referenced operand was null/absent — the row contributes nothing.
    Null,
    /// An i128 intermediate overflowed — the caller must fall back to the exact
    /// pipeline so the row is computed, not dropped.
    Overflow,
}

/// Combine two operand evaluations through an exact decimal op. Overflow takes
/// precedence (always safe — the exact pipeline recomputes the row and still
/// drops it if a null was also present), then null (drop), then the op itself,
/// whose own i128 overflow escalates.
fn combine_dec(a: DecEval, b: DecEval, op: impl FnOnce(Dec, Dec) -> Option<Dec>) -> DecEval {
    match (a, b) {
        (DecEval::Overflow, _) | (_, DecEval::Overflow) => DecEval::Overflow,
        (DecEval::Null, _) | (_, DecEval::Null) => DecEval::Null,
        (DecEval::Val(x), DecEval::Val(y)) => match op(x, y) {
            Some(d) => DecEval::Val(d),
            None => DecEval::Overflow,
        },
    }
}

/// Evaluate a native decimal arithmetic expression for one row. `vars` gives the
/// already-read value of each referenced variable (`None` = null → [`DecEval::Null`]).
fn eval_dec(expr: &Expression, vars: &[(VarId, Option<Dec>)]) -> DecEval {
    match expr {
        Expression::Var(v) => match vars.iter().find(|(x, _)| x == v) {
            Some((_, Some(d))) => DecEval::Val(*d),
            _ => DecEval::Null,
        },
        // Detection (`expr_native_foldable`) rejects constants that don't fit
        // i128, so a `None` here is unexpected — fall back rather than drop.
        Expression::Const(fv) => match const_to_dec(fv) {
            Some(d) => DecEval::Val(d),
            None => DecEval::Overflow,
        },
        Expression::Call { func, args } => match (func, args.as_slice()) {
            (Function::Add, [a, b]) => combine_dec(eval_dec(a, vars), eval_dec(b, vars), |x, y| {
                x.add_sub(y, false)
            }),
            (Function::Sub, [a, b]) => combine_dec(eval_dec(a, vars), eval_dec(b, vars), |x, y| {
                x.add_sub(y, true)
            }),
            (Function::Mul, [a, b]) => combine_dec(eval_dec(a, vars), eval_dec(b, vars), Dec::mul),
            (Function::Negate, [a]) => match eval_dec(a, vars) {
                DecEval::Val(d) => match d.val.checked_neg() {
                    Some(val) => DecEval::Val(Dec {
                        val,
                        scale: d.scale,
                    }),
                    None => DecEval::Overflow,
                },
                other => other,
            },
            // Unsupported call shape (detection rejects these) — fall back.
            _ => DecEval::Overflow,
        },
        _ => DecEval::Overflow,
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

/// PR-6 join sub-switch. `FLUREE_FUSED_R2RML_AGG_JOIN` (the standard R2RML switch
/// spelling via [`super::env_switch_enabled`] — `0`/`false`/`off`/`no` disable
/// it) forces the fact⋈dim fused path off; a multi-pattern (join) shape then
/// falls back to the generic pipeline, while the proven single-table fused path
/// is untouched. On by default. The master switch `FLUREE_FUSED_R2RML_AGG` still
/// gates the whole fused path above this.
fn fused_r2rml_agg_join_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_FUSED_R2RML_AGG_JOIN"))
}

/// Master fused-aggregate kill switch (the whole R2RML fold path). Standard
/// R2RML switch spelling via [`super::env_switch_enabled`] — `0`/`false`/`off`/
/// `no` disable it (this replaces the old bespoke `"0"|"false"` check, aligning
/// it with the rest of the switch family). On by default.
fn fused_r2rml_agg_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_FUSED_R2RML_AGG"))
}

/// Detect the fused shape: a single `GRAPH { triples }` block feeding an
/// aggregation (implicit, or GROUP BY) of only `COUNT` / `SUM` / `AVG`, with no
/// HAVING, post-binds, FILTER, ordering, or slicing. Whether the graph is
/// actually R2RML (and whether the triples collapse to one scan, and the vars
/// map to columns) is checked at `open`.
pub fn detect_fused_r2rml_aggregate(query: &Query) -> Option<FusedAggregatePlan> {
    // Kill switch (A/B and incident response): force the normal pipeline.
    if !fused_r2rml_agg_enabled() {
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
    let (group_by, aggregation): (Vec<VarId>, _) = match query.grouping.as_ref()? {
        Grouping::Implicit { aggregation, .. } => (Vec::new(), aggregation),
        Grouping::Explicit {
            group_by,
            aggregation: Some(aggregation),
            ..
        } => (group_by.iter().copied().collect(), aggregation),
        // GROUP BY with no aggregates (DISTINCT-style) is not a fold here.
        Grouping::Explicit { .. } => return None,
    };
    // PR-6: a HAVING is now allowed — it is applied by a wrapping `HavingOperator`
    // (the operator-tree fused hook), SPARQL-ordered after the fold. The output
    // projection check below still rejects any HAVING that lifts an aggregate not
    // present in the SELECT projection (that query stays on the generic path — the
    // conservative admission line). Post-aggregate BINDs are not foldable.
    if !aggregation.binds.is_empty() {
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

/// Add one row's floating value to the accumulator. NaN is dropped (neither
/// summed nor counted), mirroring the standard aggregate pipeline's numeric
/// coercion, so a NaN value can't poison SUM/AVG or inflate the count.
fn accumulate_double_row(col: &Column, row: usize, sum: &mut f64, count: &mut u64) {
    match col {
        Column::Float64(values) => {
            if let Some(Some(v)) = values.get(row) {
                if !v.is_nan() {
                    *sum += *v;
                    *count += 1;
                }
            }
        }
        Column::Float32(values) => {
            if let Some(Some(v)) = values.get(row) {
                if !v.is_nan() {
                    *sum += *v as f64;
                    *count += 1;
                }
            }
        }
        _ => {}
    }
}

/// A GROUP BY key column: which table column, how to read it, and the encoded
/// datatype Sid for the output key binding.
#[derive(Clone)]
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
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum GKey {
    Str(String),
    Int(i128),
    Null,
}

/// W4-2: the source of one GROUP BY key position in a fused fold, in SPARQL order.
/// A single-table fold is all `Fact`; a pure fact⋈dim fold is all `Dim`; a MIXED
/// rollup interleaves the two (a fact-column key alongside a dim-attribute key,
/// e.g. #7's `shipMethod` and `yearNum`). `Dim(slot)` indexes the dim-subset `GKey`
/// tuple the `GroupKeyResolver.map` stores for a fact FK.
#[derive(Clone, Copy)]
enum KeySource {
    /// Read inline from the scanned fact batch via `group_cols[pos].key_at`.
    Fact,
    /// Read from the FK→GKey map's value at this dim-subset slot.
    Dim(usize),
}

/// Insert a dim join-key → group-keys mapping for the fused-aggregate FK chain,
/// declining (returns `false`) on a CONFLICTING duplicate — the same dim join-key
/// mapping to *different* group-keys. There the generic pipeline (the reference
/// semantics) gives the dim subject two attribute triples, so a joined fact row
/// legitimately lands in two groups, while this single-value probe keeps one and
/// silently under-counts; the caller must fall back (`Ok(None)`). Equal-value
/// duplicates also decline — the fan-out under-counts even when the group-keys
/// agree. Reachable via this stack's own #1450
/// unverified subject-keys / name-based FK inference / hand-written mappings — SF01
/// dims have unique PKs so the corpus can't catch it, hence a checked invariant
/// rather than the old `last-wins` comment.
fn insert_dim_gkeys(
    map: &mut std::collections::HashMap<Vec<String>, Vec<GKey>>,
    key: Vec<String>,
    gkeys: Vec<GKey>,
) -> bool {
    // The FK→GKey map assumes the parent JOIN KEY is UNIQUE — true for a proper
    // star schema (the RefObjectMap's parent columns are the dim's surrogate PK),
    // but NOT guaranteed for a hand-written mapping whose parent columns are a
    // non-key subset. A duplicate parent join key means the materialized inner
    // join FANS OUT (one fact row matches multiple dim rows), which a single-entry
    // per-key map cannot represent: conflicting group attrs would mis-attribute
    // (last-wins), and even EQUAL group attrs would UNDER-COUNT the fan-out. So any
    // duplicate parent key DECLINES the fused plan (caller returns `Ok(None)` →
    // materialize), the conservative posture the whole operator takes when a shape
    // is outside what it can fold soundly. Returns `false` on any duplicate (was
    // previously `true` for an equal-value duplicate — that "harmless" case is a
    // latent fan-out under-count, so it now declines too).
    match map.get(&key) {
        Some(_) => false,
        None => {
            map.insert(key, gkeys);
            true
        }
    }
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
                // A Snowflake `NUMBER(n,0)` integer column arrives as a physical
                // Decimal even when the R2RML datatype is `xsd:integer`; read the
                // exact-integer value so it groups (else every such dim row would
                // be dropped as a null key — the q010 `YEAR_NUM/QUARTER_NUM` case).
                Column::Decimal { values, scale, .. } => match values.get(row).and_then(|o| *o) {
                    Some(unscaled) if *scale == 0 => GKey::Int(unscaled),
                    Some(unscaled) if *scale > 0 => match pow10(i64::from(*scale)) {
                        Some(d) if unscaled % d == 0 => GKey::Int(unscaled / d),
                        _ => GKey::Null,
                    },
                    _ => GKey::Null,
                },
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
    /// The columns that must be non-null for the COUNT(*) manifest shortcut to
    /// equal a full scan — the subject key columns **parsed from the template
    /// string** (not the loader-only `template_columns` field) plus the object
    /// columns. Empty means a constant subject (present on every row → the
    /// shortcut needs no null proof). Consumed only for the bare-COUNT fast path.
    count_non_null_cols: Vec<String>,
    /// PR-6: `None` for the single-table fold (GROUP BY keys read straight from
    /// the scanned fact batch via `group_cols`). `Some` for a fact⋈dim fold: the
    /// GROUP BY keys live on a dimension reached by an FK, so they are resolved
    /// per fact row by probing this dim lookup with the fact's FK columns. A miss
    /// (dangling or null FK, or a dim row with a null group attribute) drops the
    /// fact row — mirroring the R2RML/inner-join row-drop. `group_cols` still
    /// describes the key kinds/datatypes for materializing the output binding.
    group_resolver: Option<GroupKeyResolver>,
    /// W4-2: per GROUP BY position (SPARQL order), whether that key is read inline
    /// from the fact batch (`Fact`) or from the dim FK→GKey map (`Dim(slot)`). All
    /// `Fact` for the single-table fold; all `Dim` for a pure fact⋈dim fold; mixed
    /// for #7-shaped rollups. `group_cols[pos]` still carries the kind/dt_sid for
    /// BOTH the fact `key_at` read and the output `binding`, so the emit path is
    /// position-indexed and source-agnostic.
    group_key_plan: Vec<KeySource>,
    /// E2: fact-side folded constant-object constraints (`star_constraints`)
    /// applied per fact row in `next_batch` — a fact row failing any is dropped
    /// (existence-filter parity with the normal scan). Empty on the single-table
    /// path (which still declines on any star_constraints via the O1 guard) and
    /// for a join with no fact-side flag. Dim-side constraints are applied earlier,
    /// during the FK→GKey map build, so they are not carried here.
    fact_constraints: Vec<ResolvedConstraint>,
}

/// PR-6 fact⋈dim group-key resolver, built once at `open` by scanning the small
/// dimension(s). Maps a fact FK key (the stringified `fact_fk_cols` values, in
/// the RefObjectMap's join order) to the GROUP BY key tuple; an absent key means
/// the fact row has no complete join tuple and drops from the rollup.
struct GroupKeyResolver {
    /// Fact-scan columns forming the probe key — the RefObjectMap child columns
    /// of the first hop, in join-condition order.
    fact_fk_cols: Vec<String>,
    /// FK key → group-key GKey tuple. Only fully-non-null dim rows are inserted,
    /// so a probe miss collapses "dangling FK" and "dim row with null group
    /// attribute" into one drop, exactly as the inner join does.
    map: std::collections::HashMap<Vec<String>, Vec<GKey>>,
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

    /// W4-2 gate Q1: route each GROUP BY var to its SINGLE source pattern in the
    /// `fact → dim1 → … → dimk` chain, preserving SPARQL order. For each var,
    /// `predicate_for_var` must match in EXACTLY ONE participating pattern:
    /// - 0 matches → the var is not bound as a scalar object anywhere on the chain
    ///   → decline (`None`);
    /// - ≥2 matches → the var is an object in two patterns, i.e. a cross-source
    ///   value-equality the single-scan fold cannot enforce (it would produce
    ///   different groups than the materialized inner join) → decline;
    /// - exactly one → that pattern is the source.
    ///
    /// v1 admits a source that is the FACT (chain index 0) or the TERMINAL dim
    /// (last index) only. An INTERIOR-dim source declines: the interior FK→GKey
    /// composition relays only the terminal dim's group attrs, so an interior
    /// group attr is not carried (a sound, decline-only follow-on). Returns the
    /// per-var source chain-index, or `None` to decline the whole fuse.
    fn route_group_key_sources(chain: &[&R2rmlPattern], group_by: &[VarId]) -> Option<Vec<usize>> {
        let last = chain.len().checked_sub(1)?;
        let mut out = Vec::with_capacity(group_by.len());
        for gv in group_by {
            let mut src: Option<usize> = None;
            for (i, p) in chain.iter().enumerate() {
                if Self::predicate_for_var(p, *gv).is_some() {
                    if src.is_some() {
                        return None; // ≥2 sources: cross-source equality
                    }
                    src = Some(i);
                }
            }
            let src = src?; // 0 sources: not a scalar object on the chain
            if src != 0 && src != last {
                return None; // interior-dim group key: v1 declines
            }
            out.push(src);
        }
        Some(out)
    }

    /// W4-2: assemble the composite group key for one fact row, interleaving
    /// fact-inline positions (`key_at` on the scanned fact batch) with dim-resolved
    /// positions (the probed FK→GKey slice) in SPARQL order per `plan`. A NULL in
    /// ANY position — a null fact key column OR a null dim gkey — drops the row
    /// (`None`), the BGP unbound-object semantics, symmetric across both sources and
    /// matching the materialize path's row-drop. `dim_gkeys` is the resolver's
    /// probed value (already existence-checked by the caller); a `Dim` slot with no
    /// resolver value drops defensively.
    fn assemble_group_key(
        plan: &[KeySource],
        group_cols: &[GroupCol],
        key_cols: &[Option<&Column>],
        dim_gkeys: Option<&[GKey]>,
        row: usize,
    ) -> Option<Vec<GKey>> {
        let mut key = Vec::with_capacity(plan.len());
        for (pos, slot) in plan.iter().enumerate() {
            let k = match slot {
                KeySource::Fact => group_cols[pos].key_at(key_cols[pos], row),
                KeySource::Dim(slot) => dim_gkeys?.get(*slot)?.clone(),
            };
            if matches!(k, GKey::Null) {
                return None;
            }
            key.push(k);
        }
        Some(key)
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

    /// Q2 admission gate: a group-key column may be fused only when its object map
    /// produces a PLAIN LITERAL — `TermType::Literal` and no language tag. A
    /// language-tagged (`rdf:langString`) or IRI-/blank-node-typed column
    /// materializes a term whose datatype/lang/term-type the fused fold's
    /// `xsd:string` default would mis-encode, so the grouped key would disagree
    /// with the generic materialize path. `scalar_column_for_var` discards these
    /// two fields (the `..`), so this checks them directly. Decline-only.
    fn group_key_col_is_plain_literal(pattern: &R2rmlPattern, tm: &TriplesMap, var: VarId) -> bool {
        let Some(pred) = Self::predicate_for_var(pattern, var) else {
            return false;
        };
        let mut poms = tm
            .predicate_object_maps
            .iter()
            .filter(|pom| pom.predicate_map.as_constant() == Some(pred));
        let (Some(pom), None) = (poms.next(), poms.next()) else {
            return false;
        };
        matches!(
            &pom.object_map,
            ObjectMap::Column {
                language: None,
                term_type: TermType::Literal,
                ..
            }
        )
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

        // COUNT(*) manifest shortcut: a bare COUNT — exactly one CountRows fold,
        // no GROUP BY, no FILTER — can be answered from the Iceberg manifest
        // record_count sum instead of decoding every data file, WHEN the provider
        // can prove the manifest count equals a full scan: no delete manifests,
        // and every subject/object validity column provably zero-null. Otherwise
        // `table_row_count` returns None and the scan below runs (delete/null-
        // correct). Gated by the same `FLUREE_FUSED_R2RML_AGG` kill switch as the
        // whole fused path (a disabled switch fails detection, so this is never
        // reached). The emitted binding is byte-identical to the scan+fold result
        // (`Acc::Count(n).finalize()`).
        if resolved.filter.is_none()
            && resolved.group_cols.is_empty()
            && resolved.fact_constraints.is_empty()
            && matches!(resolved.folds.as_slice(), [Fold::CountRows])
        {
            let gs = resolved.pattern.graph_source_id.clone();
            let table = resolved.table_name.clone();
            let non_null_cols = resolved.count_non_null_cols.clone();
            if let Some(n) = table_provider
                .table_row_count(&gs, &table, &non_null_cols, as_of_t)
                .await?
            {
                self.done = true;
                self.state = OperatorState::Exhausted;
                let count = Acc::Count(n).finalize();
                return Ok(Some(Batch::new(
                    Arc::clone(&self.schema),
                    vec![vec![count]],
                )?));
            }
        }

        let mut stream = table_provider
            .scan_table(
                &resolved.pattern.graph_source_id,
                &resolved.table_name,
                &resolved.projection,
                &[],
                None,
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
            // Checkpoint per fused-aggregate scan batch (deadline + memory budget): a
            // deadline/abort stops a large fused rollup mid-sweep, and a high-cardinality
            // GROUP BY whose `groups` map crossed the budget on a prior batch aborts
            // typed before OOM.
            ctx.checkpoint()?;
            let batch = batch?;
            let groups_before = groups.len();
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
                // E2: fact-side folded constant-object constraints (a fact flag) —
                // a fact row that fails any is dropped, the existence-filter parity
                // with the normal scan. Empty (a no-op) unless the fused join
                // carried a fact-side constraint.
                if !resolved.fact_constraints.is_empty()
                    && !Self::row_satisfies_constraints(&resolved.fact_constraints, &batch, row)?
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
                    // W4-2: probe the FK→GKey map once (join existence + the
                    // dim-subset group keys) when this is a join fold, then assemble
                    // the composite key by interleaving fact-inline and dim-resolved
                    // positions in SPARQL order. A null/missing FK or a
                    // dim-constraint/existence miss drops the fact row here; a null in
                    // ANY key position drops it in `assemble_group_key` — both the
                    // R2RML/inner-join row-drop. For an all-fact plan over a join
                    // (empty dim subset) the map stores `[]` per FK, so this probe is
                    // a pure existence filter and the key comes wholly from the fact.
                    let dim_gkeys: Option<&[GKey]> = if let Some(resolver) =
                        &resolved.group_resolver
                    {
                        let Some(fk) = get_join_key_from_batch(&resolver.fact_fk_cols, &batch, row)
                        else {
                            continue;
                        };
                        match resolver.map.get(&fk) {
                            Some(gk) => Some(gk.as_slice()),
                            None => continue,
                        }
                    } else {
                        None
                    };
                    let Some(key) = Self::assemble_group_key(
                        &resolved.group_key_plan,
                        gcols,
                        &key_cols,
                        dim_gkeys,
                        row,
                    ) else {
                        continue;
                    };
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
                                    DecEval::Val(d),
                                ) => match sum.checked_add(d.val) {
                                    Some(s) => {
                                        *sum = s;
                                        *scale = d.scale;
                                        *count += 1;
                                        true
                                    }
                                    // Sum-level i128 overflow → exact pipeline.
                                    None => false,
                                },
                                // Intermediate i128 overflow → exact pipeline
                                // (must not be confused with a null row drop).
                                (_, DecEval::Overflow) => false,
                                // Null operand → the row contributes nothing.
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
            // Account this batch's group growth into the query-scoped counter; the next
            // batch's checkpoint enforces the budget against the running total.
            let grown = groups.len() - groups_before;
            if grown > 0 {
                ctx.record_alloc(grown * crate::context::GROUP_EST_BYTES);
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

/// C5 slice-1 MANDATORY soundness guard: whether ANY participating R2RML pattern
/// (the aggregated fact AND every joined dim) carries a `star_constraints` entry —
/// a constant-object member the rewrite folded into the pattern (e.g.
/// `?c ex:isCurrent true` → `star_constraints=[(IS_CURRENT, true)]`, `rewrite.rs:344`).
/// The fused fold reads only the GROUP-BY/aggregate/validity columns + the SPARQL
/// FILTER; it has NO star_constraints handling (`grep star_constraints
/// fused_aggregate.rs` = 0), while the normal scan DOES apply them
/// (`operator.rs:524/1719/…`). So a fused plan over a constrained pattern would
/// silently IGNORE the constraint and OVER-COUNT. Declining here keeps the fold
/// sound; slice 1.5 teaches the fold to apply the constraint and lifts this for
/// that shape. This is also a *current* correctness fix: any COUNT-over-a-flagged-
/// star that reaches the fused path today over-counts.
fn fold_over_star_constraints(pats: &[&R2rmlPattern]) -> bool {
    pats.iter().any(|p| !p.star_constraints.is_empty())
}

/// E2: a folded constant-object constraint (`star_constraints`) resolved to a
/// per-row scalar-column check. The predicate's column `PredicateObjectMap` is
/// materialized per row and compared to `constant` with the normal scan's
/// primitives (parity by construction); `canon` is the constant's precomputed
/// decimal canonical (skips a per-row BigDecimal parse on an exact hit).
struct ResolvedConstraint {
    pom: PredicateObjectMap,
    constant: ObjectConstant,
    canon: Option<String>,
}

impl ResolvedConstraint {
    /// The single scan column this constraint reads. `resolve_star_constraint_checks`
    /// admits only `Column` object maps, so this is always present; it must be
    /// projected into the scan so `row_satisfies_constraints` can read it.
    fn column(&self) -> &str {
        match &self.pom.object_map {
            ObjectMap::Column { column, .. } => column,
            _ => unreachable!("only Column object maps are admitted"),
        }
    }
}

/// C5 O2 core predicate: a dataset resolves to a single data VIEW iff its
/// constituent graphs collapse to EXACTLY ONE distinct `(ledger_id, to_t, policy)`
/// tuple. `ledger_id` alone is not enough (Q1): the SAME ledger at two different
/// `to_t`s, or under two different policy enforcers, is two distinct views the
/// materialize path would union — so keying on the full tuple declines those too.
/// The deployed `FROM <gs>` shape lists the graph source as both a default and a
/// named graph at the same to_t with no policy → one view → admit. A mixed dataset
/// (a native member, a second graph source, or a second view of the same ledger)
/// yields ≥2 and is declined; an empty dataset yields 0 and is declined
/// (materialize — the safe default). Policy identity is by `Arc` pointer (two
/// distinct enforcer instances read as distinct views — conservative/decline-only,
/// never falsely admits). Pure so the guard's arithmetic is hermetic.
fn dataset_views_are_single_source<'s>(
    views: impl IntoIterator<Item = (&'s str, i64, Option<usize>)>,
) -> bool {
    let mut set: std::collections::HashSet<(&str, i64, Option<usize>)> =
        std::collections::HashSet::new();
    set.extend(views);
    set.len() == 1
}

impl FusedR2rmlAggregateOperator {
    /// C5 O2: true when the query's dataset (if any) resolves to a SINGLE data
    /// source — so the fused single-`graph_iri` scan sees every row the
    /// materialized union would. The GRAPH path (`ctx.dataset == None`) is a
    /// single graph and always single-source. In dataset (FROM) mode the deployed
    /// `FROM <gs>` shape registers the graph source as BOTH a default and a named
    /// graph (both the same `ledger_id`), so "exactly one distinct ledger_id"
    /// admits it while any genuinely mixed dataset — a native member, or a second
    /// graph source over the same class — has ≥2 distinct ledger_ids and DECLINES,
    /// because the single-source fold would otherwise UNDER-COUNT vs the
    /// `DatasetOperator` union.
    fn dataset_is_single_source(&self, ctx: &ExecutionContext<'_>) -> bool {
        let Some(ds) = ctx.dataset else {
            return true; // single-graph GRAPH path
        };
        // Key each constituent graph by its full (ledger_id, to_t, policy) view.
        dataset_views_are_single_source(
            ds.default_graphs()
                .iter()
                .chain(ds.named_graphs_iter().map(|(_, g)| g))
                .map(|g| {
                    (
                        g.ledger_id.as_ref(),
                        g.to_t,
                        g.policy_enforcer.as_ref().map(|p| Arc::as_ptr(p) as usize),
                    )
                }),
        )
    }

    /// Rewrite inner triples → R2RML at `open` and resolve column folds.
    async fn resolve_at_open(&self, ctx: &ExecutionContext<'_>) -> Result<Option<Resolved>> {
        // C5 O2 mixed-dataset guard: the fused fold reads ONLY `self.graph_iri`'s
        // single R2RML scan. Placed here it gates BOTH the single-table path and
        // the join path (which is reached by delegation below).
        if !self.dataset_is_single_source(ctx) {
            return Ok(None);
        }
        // Load the compiled mapping first so the rewrite can decide whether a
        // same-subject `rdf:type` is safe to fuse into the star (see
        // `rewrite::class_fusion_is_safe`); it is then reused as the resolved
        // mapping below. A missing provider / load failure leaves `mapping` as
        // `None`, which disables fusion and, for a genuine R2RML scan, falls
        // back to the normal path.
        let as_of_t = if ctx.dataset.is_some() {
            None
        } else {
            Some(ctx.to_t)
        };
        let mapping = match ctx.r2rml_provider {
            Some(provider) => provider
                .compiled_mapping(&self.graph_iri, as_of_t)
                .await
                .ok(),
            None => None,
        };

        // Rewrite the inner triples for this graph using the active snapshot.
        // A non-R2RML graph (or an unconvertible pattern) leaves triples
        // unconverted → fall back.
        let rr = rewrite_patterns_for_r2rml(
            &self.inner_patterns,
            &self.graph_iri,
            ctx.active_snapshot,
            mapping.as_deref(),
            ctx.reasoning_active,
            // Count/aggregate path never merges a projected type-var (it folds
            // scalar columns and materializes no type rows); keep the two-scan
            // rewrite so the browse merge is confined to the crawl projection path.
            false,
        );
        if rr.unconverted_count > 0 {
            return Ok(None);
        }
        // The single-`R2rml` shape gate below also keeps this path decline-safe
        // against non-lowered sub-scopes (`rr.unsupported`): a surviving
        // PropertyPath/Subquery breaks the shape, so we fall back to the normal
        // GRAPH path, which raises the loud `unsupported_subscope_error`.
        let pattern = match rr.patterns.as_slice() {
            [Pattern::R2rml(p)] => p.clone(),
            // PR-6: a fact→dim chain rewrites to multiple R2rml leaf patterns.
            // Admit it as a fused aggregate over one join (gated by the join
            // sub-switch); anything else (non-R2rml pattern present) falls back.
            _ if fused_r2rml_agg_join_enabled() => {
                let mut pats: Vec<&R2rmlPattern> = Vec::with_capacity(rr.patterns.len());
                for p in &rr.patterns {
                    match p {
                        Pattern::R2rml(p) => pats.push(p),
                        _ => return Ok(None),
                    }
                }
                let Some(mapping) = mapping else {
                    return Ok(None);
                };
                return self.resolve_join_at_open(ctx, &pats, &mapping).await;
            }
            _ => return Ok(None), // join sub-switch off, or non-R2rml pattern
        };

        // C5 slice-1 MANDATORY guard: a folded constant-object constraint
        // (star_constraints, e.g. `?c ex:isCurrent true`) on the single-table
        // pattern is NOT applied by the fold and would OVER-COUNT. Decline.
        if fold_over_star_constraints(&[&pattern]) {
            return Ok(None);
        }

        // The graph is genuinely R2RML-backed here; without the mapping fall back
        // to the normal path (which surfaces any real load error).
        let Some(mapping) = mapping else {
            return Ok(None);
        };

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
            // C5 slice-1: default an un-annotated column group key to `xsd:string`
            // — the R2RML natural mapping for an UN-ANNOTATED PLAIN-LITERAL column
            // (the DIM string attributes in this schema, e.g. DimAccount.INDUSTRY /
            // DimStore.CHANNEL, carry no `rr:datatype`; the Q2 gate below rejects
            // lang-/IRI-typed columns for which the "natural string" claim does not
            // hold). Mirrors the join path (`resolve_join_at_open`, the
            // `unwrap_or(xsd::STRING)` there) and admits the family-A single-table
            // rollups (single-DIM COUNT, and single-table COUNT+SUM) the deployed
            // corpus hits.
            //
            // The old byte-for-byte decline this default lifts guarded TWO things,
            // both now retired by mechanisms that make widening sound:
            //   1. OVER-COUNT of a flagged shape (the `q022` isCurrent sentinel):
            //      retired by the `star_constraints` guard above (`:1304`) — any
            //      constant-object flag declines BEFORE this loop, so the default
            //      here only ever widens admission for un-flagged shapes.
            //   2. WRONG group-key TERM vs the materialized answer: retired by
            //      parity-by-construction — the generic materialize path's
            //      `LiteralEncoder` registers a datatype Sid only for annotated
            //      object maps and otherwise falls back to `xsd:string`
            //      (`operator.rs:1637/1643`), the SAME default applied here, so the
            //      fold's group key and the materialized key are byte-identical for a
            //      plain-literal column. (Locked by `default_string_group_key_lexical_parity`
            //      + `key_at_reads_integer_group_key_from_decimal`; live by q060/q007/q023.)
            //
            // Q2 lang/IRI admission gate: that xsd:string default is a PLAIN-LITERAL
            // assumption. A group-key column whose object map is language-tagged
            // (rdf:langString) or IRI-/blank-node-typed materializes a term the
            // fused fold would MIS-ENCODE as an xsd:string literal — a different
            // datatype/lang/term-type than the generic path emits, so the grouped
            // key would disagree with the materialized answer. Decline such a key
            // (decline-only — never over-counts, never widens past the plain case).
            if !Self::group_key_col_is_plain_literal(&pattern, tm, *gv) {
                return Ok(None);
            }
            let dt_iri = datatype.as_deref().unwrap_or(fluree_vocab::xsd::STRING);
            let Some(kind) = group_kind(Some(dt_iri)) else {
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

        // Resolve the aggregate output folds against the (single) scanned TM.
        let (folds, expr_folds) = match self.resolve_agg_folds(&pattern, tm, &mut projection) {
            Some(x) => x,
            None => return Ok(None),
        };

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
        // Trap-safe subject key columns for the COUNT shortcut's null guard: parse
        // the template STRING. `SubjectMap.template_columns` is populated only on
        // the loader path, NOT by `TriplesMap::with_subject_template` (the
        // fluree/db template_columns gotcha), so trusting the field would leave a
        // fixture- or non-loader-built mapping's key columns empty and the null
        // guard would pass vacuously. A constant/column subject has no template
        // placeholders: a constant subject is on every row (empty set is sound —
        // count == record_count); a column subject must itself be non-null.
        let mut count_non_null_cols: Vec<String> = match tm.subject_map.template.as_deref() {
            Some(t) => extract_template_columns(t),
            None => tm.subject_map.column.iter().cloned().collect(),
        };
        let mut obj_vars: Vec<VarId> = pattern.object_var.into_iter().collect();
        obj_vars.extend(pattern.star_bindings.iter().map(|(_, v)| *v));
        for v in obj_vars {
            let Some((col, _)) = Self::scalar_column_for_var(&pattern, tm, v) else {
                return Ok(None);
            };
            validity_cols.push(col.clone());
            count_non_null_cols.push(col);
        }
        count_non_null_cols.sort();
        count_non_null_cols.dedup();
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
            count_non_null_cols,
            group_resolver: None,
            // Single-table fold: every key reads inline from the fact batch.
            group_key_plan: (0..self.group_by.len()).map(|_| KeySource::Fact).collect(),
            fact_constraints: Vec::new(),
        }))
    }

    /// Resolve the aggregate output folds for `self.aggregates` against one
    /// scanned TriplesMap (`pattern`/`tm`), appending each aggregate's value
    /// column(s) to `projection`. `None` = an unsupported aggregate or a
    /// non-scalar/column object map → the caller falls back. Shared by the
    /// single-table `resolve_at_open` and the fact⋈dim `resolve_join_at_open`
    /// (the aggregates always fold from the FACT scan in both).
    fn resolve_agg_folds(
        &self,
        pattern: &R2rmlPattern,
        tm: &TriplesMap,
        projection: &mut Vec<String>,
    ) -> Option<(Vec<Fold>, Vec<ExprFold>)> {
        let bind_lookup: std::collections::HashMap<VarId, &Expression> =
            self.agg_binds.iter().map(|(v, e)| (*v, e)).collect();
        let mut folds = Vec::with_capacity(self.aggregates.len());
        let mut expr_folds: Vec<ExprFold> = Vec::new();
        for (_, func) in &self.aggregates {
            match func {
                AggregateFn::CountAll => folds.push(Fold::CountRows),
                AggregateFn::Count(v) if pattern.subject_var == Some(*v) => {
                    // COUNT of the subject counts the rows that produce a row,
                    // which the row-validity gate already enforces.
                    folds.push(Fold::CountRows);
                }
                AggregateFn::Count(v) => {
                    let (col, _) = Self::scalar_column_for_var(pattern, tm, *v)?;
                    projection.push(col.clone());
                    folds.push(Fold::CountColumn(col));
                }
                AggregateFn::Sum(v, _) | AggregateFn::Avg(v, _) => {
                    let is_avg = matches!(func, AggregateFn::Avg(_, _));
                    if let Some(expr) = bind_lookup.get(v) {
                        // Aggregate over a desugared expression: native decimal fold.
                        if !expr_native_foldable(expr) {
                            return None;
                        }
                        let mut var_cols = Vec::new();
                        for ev in expr.referenced_vars() {
                            let (col, datatype) = Self::scalar_column_for_var(pattern, tm, ev)?;
                            let deck = match numeric_kind(datatype.as_deref()) {
                                Some(NumKind::Decimal) => DecKind::Decimal,
                                Some(NumKind::Integer) => DecKind::Integer,
                                // floats aren't exact decimals → engine path.
                                _ => return None,
                            };
                            projection.push(col.clone());
                            var_cols.push((ev, col, deck));
                        }
                        // The native expr fold always finalizes as xsd:decimal; an
                        // all-integer expression would be xsd:integer in the normal
                        // pipeline, so fall back to keep the result datatype exact.
                        let any_decimal = var_cols
                            .iter()
                            .any(|(_, _, k)| matches!(k, DecKind::Decimal))
                            || expr_has_decimal_const(expr);
                        if !any_decimal {
                            return None;
                        }
                        let index = expr_folds.len();
                        expr_folds.push(ExprFold {
                            expr: (*expr).clone(),
                            var_cols,
                        });
                        folds.push(Fold::NumericExpr { index, is_avg });
                    } else {
                        // Aggregate over a bare numeric column: native fold.
                        let (col, datatype) = Self::scalar_column_for_var(pattern, tm, *v)?;
                        let kind = numeric_kind(datatype.as_deref())?;
                        projection.push(col.clone());
                        folds.push(Fold::Numeric {
                            column: col,
                            kind,
                            is_avg,
                        });
                    }
                }
                _ => return None,
            }
        }
        Some((folds, expr_folds))
    }

    /// If `dim`'s subject variable is bound as an object of `fact` (a RefObjectMap
    /// object among `fact`'s star members / object var), return that join variable
    /// — i.e. `fact` is the child and `dim` the parent of one FK hop.
    fn joins_via(fact: &R2rmlPattern, dim: &R2rmlPattern) -> Option<VarId> {
        let dsv = dim.subject_var?;
        if fact.object_var == Some(dsv) || fact.star_bindings.iter().any(|(_, v)| *v == dsv) {
            Some(dsv)
        } else {
            None
        }
    }

    /// Order the rewritten leaf patterns into a linear `fact → dim1 → … → dimk`
    /// FK chain: each pattern ref-joins to at most one next pattern (no branch),
    /// exactly one pattern is unreferenced (the fact/root), and the walk visits
    /// every pattern exactly once (no cycle, not disconnected). Returns the ordered
    /// chain and, per hop, the join variable (the next pattern's subject var).
    /// `None` declines any non-linear shape (PR-6b constraint: linear chains only).
    fn order_chain<'p>(pats: &[&'p R2rmlPattern]) -> Option<(Vec<&'p R2rmlPattern>, Vec<VarId>)> {
        let n = pats.len();
        if n < 2 {
            return None;
        }
        // `next[i] = (join_var, j)`: `pats[i]` ref-joins to `pats[j]`. At most one
        // per `i` (no branch); each `j` referenced at most once (no merge).
        let mut next: Vec<Option<(VarId, usize)>> = vec![None; n];
        let mut indeg = vec![0usize; n];
        for i in 0..n {
            for j in 0..n {
                if i == j {
                    continue;
                }
                if let Some(jv) = Self::joins_via(pats[i], pats[j]) {
                    if next[i].is_some() {
                        return None; // branch: i joins to >1 dim
                    }
                    next[i] = Some((jv, j));
                    indeg[j] += 1;
                    if indeg[j] > 1 {
                        return None; // merge: j referenced by >1 pattern
                    }
                }
            }
        }
        // Exactly one root (in-degree 0).
        let mut root = None;
        for (i, &d) in indeg.iter().enumerate() {
            if d == 0 {
                if root.is_some() {
                    return None;
                }
                root = Some(i);
            }
        }
        let mut cur = root?;
        let mut order = Vec::with_capacity(n);
        let mut join_vars = Vec::with_capacity(n - 1);
        let mut seen = vec![false; n];
        loop {
            if seen[cur] {
                return None; // cycle
            }
            seen[cur] = true;
            order.push(pats[cur]);
            match next[cur] {
                Some((jv, j)) => {
                    join_vars.push(jv);
                    cur = j;
                }
                None => break,
            }
        }
        if order.len() != n {
            return None; // disconnected
        }
        Some((order, join_vars))
    }

    /// PR-6 (6a): resolve a fused aggregate over a single fact→dim FK hop. `pats`
    /// are the rewritten R2rml leaf patterns — here exactly two: the fact (chain
    /// root, carrying the aggregate measure columns) and one dimension carrying
    /// the GROUP BY attribute(s). Builds a [`GroupKeyResolver`] by scanning the
    /// dim once. Declines (`Ok(None)` → generic pipeline) on any shape outside
    /// the admitted class: != 2 patterns, a FILTER, a composite/multi FK, a
    /// non-scalar group key, an aggregate that is not a fact column, or a
    /// non-linear / cyclic join (see [`Self::order_chain`]).
    /// E2: resolve a pattern's folded constant-object constraints
    /// (`star_constraints`, e.g. `?prod ex:isCurrent true`) to per-row checks
    /// against its TriplesMap. Each `(predicate, constant)` must map to a SCALAR
    /// column PredicateObjectMap so the fold can enforce the equality using the
    /// SAME primitives as the normal scan. Declines (`None`) if any constraint's
    /// predicate is a RefObjectMap object (needs the operator's parent lookups the
    /// fold does not run) or is absent from the map.
    fn resolve_star_constraint_checks(
        pattern: &R2rmlPattern,
        tm: &TriplesMap,
    ) -> Option<Vec<ResolvedConstraint>> {
        let mut checks = Vec::with_capacity(pattern.star_constraints.len());
        for (pred, constant) in &pattern.star_constraints {
            let pom = tm
                .predicate_object_maps
                .iter()
                .find(|p| p.predicate_map.as_constant() == Some(pred.as_str()))?;
            if !matches!(pom.object_map, ObjectMap::Column { .. }) {
                return None; // RefObjectMap / template constraint: keep the materialize path
            }
            checks.push(ResolvedConstraint {
                pom: pom.clone(),
                constant: constant.clone(),
                canon: decimal_canonical_of(constant),
            });
        }
        Some(checks)
    }

    /// E2: whether a row satisfies EVERY resolved constraint — the existence-filter
    /// semantics of the normal scan (`operator.rs:2019`): each constraint predicate
    /// must materialize an object equal to its constant; a null/missing object
    /// fails. Reuses `materialize_object_from_batch` + `object_column_is_numeric` +
    /// `rdf_term_eq_object_constant_cached`, so a fused constraint is byte-parity
    /// with the materialized answer.
    fn row_satisfies_constraints(
        checks: &[ResolvedConstraint],
        batch: &ColumnBatch,
        row: usize,
    ) -> Result<bool> {
        for c in checks {
            let numeric = object_column_is_numeric(&c.pom, batch);
            let matched = materialize_object_from_batch(&c.pom.object_map, batch, row)?
                .is_some_and(|term| {
                    rdf_term_eq_object_constant_cached(
                        &term,
                        &c.constant,
                        numeric,
                        c.canon.as_deref(),
                    )
                });
            if !matched {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn resolve_join_at_open(
        &self,
        ctx: &ExecutionContext<'_>,
        pats: &[&R2rmlPattern],
        mapping: &CompiledR2rmlMapping,
    ) -> Result<Option<Resolved>> {
        // A FILTER over the join is out of scope (none of the target rollups carry
        // one; HAVING is applied by a wrapping operator, not here).
        if self.filter.is_some() {
            return Ok(None);
        }
        // E2 (slice-1.5): a folded constant-object constraint (star_constraints,
        // e.g. a dim `?prod ex:isCurrent true` or a fact-side flag) is no longer a
        // blanket decline — the fold APPLIES it (dim-side while building the FK→GKey
        // map, fact-side in the value fold), resolved below once the TriplesMaps are
        // known. A constraint that does not resolve to a scalar column still
        // declines (in `resolve_star_constraint_checks`).
        // Order the patterns into a linear `fact → dim1 → … → dimk` chain (single
        // ref-join per hop, no branch, no cycle). `join_vars[h]` is dim_{h+1}'s
        // subject var — the object bound by the hop-`h` RefObjectMap.
        let Some((chain, join_vars)) = Self::order_chain(pats) else {
            return Ok(None);
        };
        let fact_p = chain[0];
        let terminal_p = *chain.last().expect("order_chain returns ≥2 patterns");
        let Some(fact_tm) = Self::resolve_triples_map(fact_p, mapping) else {
            return Ok(None);
        };

        // Resolve each hop's single-column FK: `hops[h]` connects `chain[h]` →
        // `chain[h+1]` via a RefObjectMap on `chain[h]`'s TriplesMap. Each entry is
        // (child cols on the source table, parent join cols on the next dim, next
        // dim's TriplesMap). The next dim's TM is the RefObjectMap's authoritative
        // `parent_triples_map` (correct even under a shared group-attr predicate).
        let mut hops: Vec<(Vec<String>, Vec<String>, &TriplesMap)> =
            Vec::with_capacity(join_vars.len());
        let mut src_tm = fact_tm;
        for (h, &jv) in join_vars.iter().enumerate() {
            let Some(join_pred) = Self::predicate_for_var(chain[h], jv) else {
                return Ok(None);
            };
            let rom = src_tm.predicate_object_maps.iter().find_map(|pom| {
                if pom.predicate_map.as_constant() == Some(join_pred) {
                    if let ObjectMap::RefObjectMap(rom) = &pom.object_map {
                        return Some(rom);
                    }
                }
                None
            });
            let Some(rom) = rom else {
                return Ok(None);
            };
            // Single-column FK per hop (the 6b constraint).
            if rom.join_conditions.len() != 1 {
                return Ok(None);
            }
            let Some(parent_tm) = mapping.triples_maps.get(&rom.parent_triples_map) else {
                return Ok(None);
            };
            hops.push((
                rom.child_columns()
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
                rom.parent_columns()
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
                parent_tm,
            ));
            src_tm = parent_tm;
        }
        let terminal_tm = hops.last().expect("≥1 hop for a join").2;

        // E2: resolve each participating pattern's folded constant-object
        // constraints to per-row scalar-column checks against its TriplesMap.
        // `fact_checks` guards the fact fold; `dim_checks[h]` guards `chain[h+1]`
        // (the dim reached by hop `h`). A constraint that does not resolve to a
        // scalar column declines the fuse (falls back to the materialize path), so
        // a shape the fold cannot enforce is never silently ignored (no over-count).
        let Some(fact_checks) = Self::resolve_star_constraint_checks(fact_p, fact_tm) else {
            return Ok(None);
        };
        let mut dim_checks: Vec<Vec<ResolvedConstraint>> = Vec::with_capacity(hops.len());
        for (h, hop) in hops.iter().enumerate() {
            let Some(checks) = Self::resolve_star_constraint_checks(chain[h + 1], hop.2) else {
                return Ok(None);
            };
            dim_checks.push(checks);
        }

        // W4-2: GROUP BY keys may resolve on FACT columns AND/OR the terminal dim's
        // attribute columns (a MIXED rollup like #7's `shipMethod` (fact) + `yearNum`
        // (dim)). Route each key to its single source (gate Q1: exactly one
        // participating pattern, else decline — `route_group_key_sources`), then
        // resolve its column on that pattern's TriplesMap. `group_cols` holds one
        // GroupCol per GROUP BY position (used for BOTH the output binding and a
        // fact-inline `key_at`); `group_key_plan` tags each position Fact/Dim; the
        // dim-subset (`dim_group_cols`/`dim_attr_cols`) feeds the FK→GKey map, and
        // the fact-subset columns (`fact_key_cols`) join the fact scan projection.
        let Some(sources) = Self::route_group_key_sources(&chain, &self.group_by) else {
            return Ok(None);
        };
        let last_idx = chain.len() - 1;
        let mut group_cols = Vec::with_capacity(self.group_by.len());
        let mut group_key_plan = Vec::with_capacity(self.group_by.len());
        let mut dim_group_cols: Vec<GroupCol> = Vec::new();
        let mut dim_attr_cols: Vec<String> = Vec::new();
        let mut fact_key_cols: Vec<String> = Vec::new();
        for (gv, &src) in self.group_by.iter().zip(&sources) {
            // v1: the source is the FACT (chain index 0) or the TERMINAL dim (last).
            let is_fact = src == 0;
            let (src_p, src_tm): (&R2rmlPattern, &TriplesMap) = if is_fact {
                (fact_p, fact_tm)
            } else {
                debug_assert_eq!(src, last_idx, "route admits only fact or terminal");
                (terminal_p, terminal_tm)
            };
            let Some((col, datatype)) = Self::scalar_column_for_var(src_p, src_tm, *gv) else {
                return Ok(None);
            };
            // Q2 lang/IRI admission gate, applied on the KEY'S OWN source pattern
            // (fact OR dim): the `xsd:string` default below is a PLAIN-LITERAL
            // assumption; a language-tagged (rdf:langString) or IRI-/blank-node-typed
            // key would be mis-encoded as an xsd:string literal, disagreeing with the
            // materialized term. Decline (decline-only) — symmetric across sources.
            if !Self::group_key_col_is_plain_literal(src_p, src_tm, *gv) {
                return Ok(None);
            }
            // A column ObjectMap with no `rr:datatype` maps to `xsd:string` (the
            // R2RML natural mapping for an un-annotated plain-literal column); the
            // generic materialize path's `LiteralEncoder` applies the same default,
            // so the group key is byte-identical (parity by construction).
            let dt_iri = datatype.as_deref().unwrap_or(fluree_vocab::xsd::STRING);
            let Some(kind) = group_kind(Some(dt_iri)) else {
                return Ok(None);
            };
            let Some(dt_sid) = ctx.active_snapshot.encode_iri(dt_iri) else {
                return Ok(None);
            };
            let gcol = GroupCol {
                column: col.clone(),
                kind,
                dt_sid,
            };
            if is_fact {
                group_key_plan.push(KeySource::Fact);
                fact_key_cols.push(col);
            } else {
                group_key_plan.push(KeySource::Dim(dim_group_cols.len()));
                dim_attr_cols.push(col);
                dim_group_cols.push(gcol.clone());
            }
            group_cols.push(gcol);
        }
        // A join fold must actually group (an implicit aggregate over a join is
        // not this shape).
        if group_cols.is_empty() {
            return Ok(None);
        }

        // Aggregates fold from the FACT scan.
        let mut projection: Vec<String> = Vec::new();
        let Some((folds, expr_folds)) = self.resolve_agg_folds(fact_p, fact_tm, &mut projection)
        else {
            return Ok(None);
        };

        // Fact-side row-validity: the subject template columns, the first hop's FK
        // child columns (a null FK ⇒ no ref triple ⇒ row drops), and every scalar
        // measure/object column EXCEPT the first join var (a RefObjectMap object,
        // covered by the FK cols).
        let fact_fk_cols = hops[0].0.clone();
        let first_join_var = join_vars[0];
        let mut validity_cols: Vec<String> = fact_tm.subject_map.template_columns.clone();
        if let Some(c) = &fact_tm.subject_map.column {
            validity_cols.push(c.clone());
        }
        validity_cols.extend(fact_fk_cols.iter().cloned());
        let mut fact_obj_vars: Vec<VarId> = fact_p.object_var.into_iter().collect();
        fact_obj_vars.extend(fact_p.star_bindings.iter().map(|(_, v)| *v));
        for v in fact_obj_vars {
            if v == first_join_var {
                continue;
            }
            let Some((col, _)) = Self::scalar_column_for_var(fact_p, fact_tm, v) else {
                return Ok(None);
            };
            validity_cols.push(col);
        }
        for c in &validity_cols {
            projection.push(c.clone());
        }
        validity_cols.sort();
        validity_cols.dedup();
        // E2: scan the fact-side constraint columns so the fold can enforce them.
        for c in &fact_checks {
            projection.push(c.column().to_string());
        }
        // W4-2: scan the fact-side group-key columns so `key_at` reads them inline.
        // (A fact group key is a fact object var, hence already in validity_cols +
        // projection above; pushed explicitly so the fold's key read never relies on
        // that coincidence, and a null fact key still drops via validity_cols.)
        for c in &fact_key_cols {
            projection.push(c.clone());
        }
        projection.sort();
        projection.dedup();

        let Some(fact_table) = fact_tm.table_name().map(str::to_string) else {
            return Ok(None);
        };
        let table_provider = ctx.r2rml_table_provider.ok_or_else(|| {
            QueryError::InvalidQuery("R2RML table provider not configured".to_string())
        })?;
        let as_of_t = if ctx.dataset.is_some() {
            None
        } else {
            Some(ctx.to_t)
        };
        let gs = &fact_p.graph_source_id;

        // PR-8 slice 1: warm the whole chain's catalog contexts CONCURRENTLY
        // before the serial dim + fact scans below, so the per-table `loadTable`
        // GETs overlap instead of summing (measured cold: q008's 3 GETs ~4.99s
        // serial). Best-effort (see `prefetch_tables`); the dim scans here and the
        // fact scan in `next_batch` share `self.session`, so one warm covers every
        // scan in this operator.
        if super::parallel_catalog_resolution_enabled() {
            let mut chain_tables: Vec<String> = Vec::with_capacity(hops.len() + 1);
            chain_tables.push(fact_table.clone());
            for (_, _, dim_tm) in &hops {
                if let Some(t) = dim_tm.table_name() {
                    chain_tables.push(t.to_string());
                }
            }
            table_provider.prefetch_tables(gs, &chain_tables).await;
        }

        // Build the composed group-key resolver, scanning each small dim ONCE from
        // the terminal dim back toward the fact. A dim row is kept only when its
        // join key AND (terminal) its group attrs / (interior) its FK-to-next are
        // all non-null and the next hop resolved — so a fact-row probe miss folds
        // "dangling FK at any hop" and "null group attr" into one drop, exactly as
        // the chained inner join does.
        //
        // Terminal dim: its join key (last hop's parent cols) → group-key GKeys.
        let (_, terminal_parent_cols, _) = hops.last().expect("≥1 hop");
        let terminal_parent_cols = terminal_parent_cols.clone();
        let Some(terminal_table) = terminal_tm.table_name().map(str::to_string) else {
            return Ok(None);
        };
        let mut terminal_proj = terminal_parent_cols.clone();
        terminal_proj.extend(dim_attr_cols.iter().cloned());
        // E2: scan the terminal dim's constraint columns (a dim flag lives here).
        for c in dim_checks.last().expect("≥1 hop") {
            terminal_proj.push(c.column().to_string());
        }
        terminal_proj.sort();
        terminal_proj.dedup();
        let mut map: std::collections::HashMap<Vec<String>, Vec<GKey>> =
            std::collections::HashMap::new();
        {
            let mut s = table_provider
                .scan_table(gs, &terminal_table, &terminal_proj, &[], None, as_of_t)
                .await?;
            while let Some(batch) = s.next().await {
                // Checkpoint the terminal-dim drain (deadline + memory budget): the
                // FK→GKey `map` is fully retained to probe the fact scan, so a large dim
                // aborts typed before OOM instead of buffering unbounded.
                ctx.checkpoint()?;
                let batch = batch?;
                let map_before = map.len();
                // W4-2: read only the DIM-subset group keys (`dim_group_cols`) here;
                // fact-subset keys are read inline from the fact batch at fold time.
                // An all-fact plan leaves this empty, so the map stores `[]` per FK
                // and degenerates to a join-existence set (invariant #1).
                let attr_cols: Vec<Option<&Column>> = dim_group_cols
                    .iter()
                    .map(|g| batch.column_by_name(&g.column))
                    .collect();
                for row in 0..batch.num_rows {
                    let Some(key) = get_join_key_from_batch(&terminal_parent_cols, &batch, row)
                    else {
                        continue;
                    };
                    // E2: skip a terminal dim row that fails its folded constraint
                    // (a dim flag, e.g. `isCurrent true`); its join key never enters
                    // the map, so fact rows probing it drop — the inner-join +
                    // constraint semantics, applied before the group key is read.
                    if !Self::row_satisfies_constraints(
                        dim_checks.last().expect("≥1 hop"),
                        &batch,
                        row,
                    )? {
                        continue;
                    }
                    let mut gkeys = Vec::with_capacity(dim_group_cols.len());
                    let mut any_null = false;
                    for (g, c) in dim_group_cols.iter().zip(&attr_cols) {
                        let k = g.key_at(*c, row);
                        if matches!(k, GKey::Null) {
                            any_null = true;
                            break;
                        }
                        gkeys.push(k);
                    }
                    if any_null {
                        continue;
                    }
                    // Decline the fused plan on ANY duplicate dim join-key (B1: an
                    // equal-value duplicate also under-counts the materialized fan-out,
                    // so it declines too — see `insert_dim_gkeys` for the argument).
                    if !insert_dim_gkeys(&mut map, key, gkeys) {
                        return Ok(None);
                    }
                }
                ctx.record_alloc((map.len() - map_before) * crate::context::GROUP_EST_BYTES);
            }
        }
        // Interior dims: compose from the hop before the terminal back to the fact.
        // `chain[h+1]` is keyed by hop-`h`'s parent cols (its join key) and carries
        // hop-`(h+1)`'s child cols (its FK to the next dim).
        for h in (0..hops.len() - 1).rev() {
            let inter_tm = hops[h].2;
            let Some(inter_table) = inter_tm.table_name().map(str::to_string) else {
                return Ok(None);
            };
            let key_cols = hops[h].1.clone();
            let fk_next_cols = hops[h + 1].0.clone();
            let mut proj = key_cols.clone();
            proj.extend(fk_next_cols.iter().cloned());
            // E2: scan this interior dim's constraint columns.
            for c in &dim_checks[h] {
                proj.push(c.column().to_string());
            }
            proj.sort();
            proj.dedup();
            let mut next_map: std::collections::HashMap<Vec<String>, Vec<GKey>> =
                std::collections::HashMap::new();
            let mut s = table_provider
                .scan_table(gs, &inter_table, &proj, &[], None, as_of_t)
                .await?;
            while let Some(batch) = s.next().await {
                // Checkpoint the interior-dim drain (deadline + memory budget). This is
                // the loop that builds the potentially multi-million-row interior FK→GKey
                // map (`next_map`), so it must be under the same query budget — a large
                // interior dim aborts typed before OOM.
                ctx.checkpoint()?;
                let batch = batch?;
                let next_before = next_map.len();
                for row in 0..batch.num_rows {
                    let Some(pk) = get_join_key_from_batch(&key_cols, &batch, row) else {
                        continue;
                    };
                    let Some(fk_next) = get_join_key_from_batch(&fk_next_cols, &batch, row) else {
                        continue;
                    };
                    // E2: skip an interior dim row that fails its folded constraint.
                    if !Self::row_satisfies_constraints(&dim_checks[h], &batch, row)? {
                        continue;
                    }
                    if let Some(gkeys) = map.get(&fk_next) {
                        // Same any-duplicate soundness as the terminal scan (B1).
                        if !insert_dim_gkeys(&mut next_map, pk, gkeys.clone()) {
                            return Ok(None);
                        }
                    }
                }
                ctx.record_alloc((next_map.len() - next_before) * crate::context::GROUP_EST_BYTES);
            }
            map = next_map;
        }

        Ok(Some(Resolved {
            pattern: fact_p.clone(),
            table_name: fact_table,
            projection,
            group_cols,
            folds,
            filter: None,
            expr_folds,
            validity_cols,
            // The COUNT(*) manifest shortcut is single-table only.
            count_non_null_cols: Vec::new(),
            group_resolver: Some(GroupKeyResolver { fact_fk_cols, map }),
            group_key_plan,
            fact_constraints: fact_checks,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::grouping::AggregateSpec;

    /// C5 slice-1 over-count TRIPWIRE: a participating R2RML pattern carrying a
    /// folded constant-object constraint (`star_constraints`, e.g.
    /// `?c ex:isCurrent true`) must DECLINE the fuse — the fused fold has no
    /// star_constraints handling and would over-count. Red under a hypothetical
    /// unguarded fuse, green with `fold_over_star_constraints`; also the guard
    /// against slice 1.5 silently lifting it.
    #[test]
    fn fold_over_star_constraints_declines_a_constrained_pattern() {
        use crate::r2rml::{ObjectConstant, ScanValue};
        let plain = R2rmlPattern::new("gs:main", VarId(1), Some(VarId(2)))
            .with_predicate("http://ex/gender");
        assert!(
            !fold_over_star_constraints(&[&plain]),
            "no constraint → foldable"
        );

        let mut constrained = plain.clone();
        constrained.star_constraints = vec![(
            "IS_CURRENT".to_string(),
            ObjectConstant::Scalar(ScanValue::Bool(true)),
        )];
        assert!(
            fold_over_star_constraints(&[&constrained]),
            "a folded `isCurrent true` constraint must decline the fuse (else over-count)"
        );
        // Fires if ANY participating map is constrained (the aggregated fact OR a
        // joined dim) — the broadened guard.
        assert!(fold_over_star_constraints(&[&plain, &constrained]));
        assert!(!fold_over_star_constraints(&[&plain, &plain]));
    }

    /// C5 O2 mixed-dataset guard (the `dataset_is_single_source` core): the fused
    /// single-`graph_iri` scan is sound ONLY when every constituent graph resolves
    /// to the SAME source. The deployed `FROM <gs>` shape lists the graph source as
    /// both a default and a named graph (both the same ledger_id) → one distinct
    /// ledger → admit. A genuinely mixed dataset (native + graph-source, or two
    /// graph sources) → ≥2 distinct ledgers → DECLINE (the single-scan fold would
    /// UNDER-COUNT vs the DatasetOperator union). An empty dataset → 0 → decline
    /// (materialize, the safe default).
    #[test]
    fn o2_single_source_admits_deployed_shape_declines_mixed() {
        let gs = "enterprise-sf01-v:main";
        // Deployed FROM <gs>: default + named graph, SAME ledger, same to_t, no policy.
        assert!(
            dataset_views_are_single_source([(gs, 0, None), (gs, 0, None)]),
            "one graph source listed as both default+named must admit"
        );
        // Single member.
        assert!(dataset_views_are_single_source([(gs, 0, None)]));
        // Mixed: graph source unioned with a native ledger → decline.
        assert!(
            !dataset_views_are_single_source([(gs, 0, None), ("native-orders:main", 0, None)]),
            "a native member alongside the graph source must decline (else under-count)"
        );
        // Two distinct graph sources over the same class → decline.
        assert!(!dataset_views_are_single_source([
            ("gs-a:main", 0, None),
            ("gs-b:main", 0, None)
        ]));
        // Q1 residual: the SAME ledger at two different to_t views → decline
        // (materialize would union both snapshots).
        assert!(!dataset_views_are_single_source([
            (gs, 0, None),
            (gs, 5, None)
        ]));
        // Q1 residual: the SAME ledger+to_t under two different policy enforcers →
        // decline (materialize would union both filtered views).
        assert!(!dataset_views_are_single_source([
            (gs, 0, Some(0x1111)),
            (gs, 0, Some(0x2222))
        ]));
        // Empty dataset → decline (materialize).
        assert!(!dataset_views_are_single_source(std::iter::empty()));
    }

    /// C5 slice-1/Q2 parity-by-construction: the None→xsd:string default's group
    /// key encodes to the SAME lexical term the materialize path emits. A String
    /// column reads its raw value as `GKey::Str` (→ an xsd:string literal via the
    /// dt_sid resolved from `encode_iri(xsd::STRING)`, the same fallback the
    /// materialize `LiteralEncoder` uses for an un-annotated column); a NULL value
    /// reads as `GKey::Null` (→ `Binding::Unbound` → validity-drop == BGP
    /// exclusion, so a null-keyed row forms no group, matching materialize). The
    /// int/decimal and null-decimal cases are locked by
    /// `key_at_reads_integer_group_key_from_decimal`; this locks the string default
    /// + string-null + the binding forms.
    #[test]
    fn default_string_group_key_lexical_parity() {
        // The default classification: an un-annotated column carries no kind on its
        // own (`None`), and the applied xsd:string default classifies as String.
        assert!(group_kind(None).is_none());
        assert!(matches!(
            group_kind(Some(fluree_vocab::xsd::STRING)),
            Some(GKind::String)
        ));

        let gc = GroupCol {
            column: "SEGMENT".to_string(),
            kind: GKind::String,
            dt_sid: Sid::new(1, "string"),
        };
        let col = Column::String(vec![Some("SMB".to_string()), None]);
        // Present value → the raw string, the xsd:string lexical form materialize emits.
        assert_eq!(gc.key_at(Some(&col), 0), GKey::Str("SMB".to_string()));
        // NULL value and a missing column both read as Null (row drops == BGP exclusion).
        assert_eq!(gc.key_at(Some(&col), 1), GKey::Null);
        assert_eq!(gc.key_at(None, 0), GKey::Null);
        // Binding forms: a present key binds a literal; Null binds Unbound (drop).
        assert!(!matches!(
            gc.binding(&GKey::Str("SMB".into())),
            Binding::Unbound
        ));
        assert!(matches!(gc.binding(&GKey::Null), Binding::Unbound));
    }

    /// E2: resolve_star_constraint_checks admits a SCALAR-column constraint (the
    /// fold can enforce it) and DECLINES a ref-object or missing-predicate
    /// constraint (which needs the operator's parent lookups the fold does not run
    /// — declining falls back to materialize, never a silent over-count).
    #[test]
    fn e2_resolve_star_constraint_checks_scalar_vs_ref() {
        use crate::r2rml::ScanValue;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap};
        let flag = "http://ex/isCurrent";
        let refp = "http://ex/geography";
        let tm = TriplesMap::new("#Cust", "DIM_CUSTOMER")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(flag),
                object_map: ObjectMap::column("IS_CURRENT"),
            })
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(refp),
                object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                    "#Geo", "GEO_KEY", "GEO_KEY",
                )),
            });
        let constrained = |pred: &str, c: ObjectConstant| {
            let mut p = R2rmlPattern::new("gs", VarId(0), None);
            p.star_constraints = vec![(pred.to_string(), c)];
            p
        };
        // Scalar column flag → resolves to one check.
        let scalar = constrained(flag, ObjectConstant::Scalar(ScanValue::Bool(true)));
        assert_eq!(
            FusedR2rmlAggregateOperator::resolve_star_constraint_checks(&scalar, &tm)
                .map(|v| v.len()),
            Some(1)
        );
        // Ref-object constraint → declines.
        let refc = constrained(refp, ObjectConstant::Iri("http://ex/geo/1".into()));
        assert!(
            FusedR2rmlAggregateOperator::resolve_star_constraint_checks(&refc, &tm).is_none(),
            "a RefObjectMap constraint must decline (fold can't enforce it)"
        );
        // Missing predicate → declines.
        let missing = constrained(
            "http://ex/nope",
            ObjectConstant::Scalar(ScanValue::Bool(true)),
        );
        assert!(
            FusedR2rmlAggregateOperator::resolve_star_constraint_checks(&missing, &tm).is_none()
        );
        // No constraints → admitted with an empty check set.
        let plain = R2rmlPattern::new("gs", VarId(0), None);
        assert_eq!(
            FusedR2rmlAggregateOperator::resolve_star_constraint_checks(&plain, &tm)
                .map(|v| v.len()),
            Some(0)
        );
    }

    /// E2: row_satisfies_constraints enforces a boolean flag (`isCurrent true`) with
    /// the NORMAL scan's equality primitives — a true row passes, a false or null
    /// row fails. So the fused-join constrained fold keeps exactly the flagged rows
    /// (parity with the materialized CONSTRAINED count; no over-count).
    #[test]
    fn e2_row_satisfies_boolean_flag() {
        use crate::r2rml::ScanValue;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};
        let constant = ObjectConstant::Scalar(ScanValue::Bool(true));
        let check = ResolvedConstraint {
            canon: decimal_canonical_of(&constant),
            pom: PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/isCurrent"),
                object_map: ObjectMap::column("IS_CURRENT"),
            },
            constant,
        };
        let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
            name: "IS_CURRENT".to_string(),
            field_type: FieldType::Boolean,
            nullable: true,
            field_id: 1,
        }]));
        let batch = ColumnBatch::new(
            schema,
            vec![Column::Boolean(vec![Some(true), Some(false), None])],
        )
        .unwrap();
        let checks = [check];
        let ok = |row| {
            FusedR2rmlAggregateOperator::row_satisfies_constraints(&checks, &batch, row).unwrap()
        };
        assert!(ok(0), "isCurrent=true row is kept");
        assert!(!ok(1), "isCurrent=false row is dropped");
        assert!(!ok(2), "isCurrent=null row is dropped (existence filter)");
        // No constraints → every row satisfied (a no-op).
        assert!(FusedR2rmlAggregateOperator::row_satisfies_constraints(&[], &batch, 1).unwrap());
    }

    /// Q2 lang/IRI admission gate: a fused group key — the single-table key OR the
    /// join path's terminal-dim key, both of which call this SAME shared predicate
    /// before applying the `xsd:string` default — must be a PLAIN LITERAL. A
    /// language-tagged (`rdf:langString`) or IRI-/blank-node-typed column declines,
    /// because the fold's `xsd:string` default would mis-encode the materialized
    /// term (a different datatype/lang/term-type Sid) and the grouped key would
    /// disagree with the generic materialize path.
    #[test]
    fn q2_group_key_plain_literal_gate() {
        use fluree_db_r2rml::mapping::{PredicateMap, PredicateObjectMap};
        let pred = "http://ex/attr";
        let make = |om: ObjectMap| {
            let tm =
                TriplesMap::new("http://ex/TM", "T").with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant(pred),
                    object_map: om,
                });
            let mut pat = R2rmlPattern::new("gs", VarId(0), None);
            pat.star_bindings = vec![(pred.to_string(), VarId(1))];
            (pat, tm)
        };
        let col = |datatype: Option<&str>, language: Option<&str>, term_type: TermType| {
            ObjectMap::Column {
                column: "C".into(),
                datatype: datatype.map(str::to_string),
                language: language.map(str::to_string),
                term_type,
            }
        };
        let is_plain = |om: ObjectMap| {
            let (p, t) = make(om);
            FusedR2rmlAggregateOperator::group_key_col_is_plain_literal(&p, &t, VarId(1))
        };

        // Plain literal (un-annotated) and a typed literal both admit.
        assert!(is_plain(col(None, None, TermType::Literal)));
        assert!(is_plain(col(
            Some("http://www.w3.org/2001/XMLSchema#integer"),
            None,
            TermType::Literal
        )));
        // Language-tagged, IRI-typed, and blank-node-typed all decline.
        assert!(!is_plain(col(None, Some("en"), TermType::Literal)));
        assert!(!is_plain(col(None, None, TermType::Iri)));
        assert!(!is_plain(col(None, None, TermType::BlankNode)));
    }
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
            cypher_vocab: None,
        }
    }

    #[test]
    fn detects_graph_count_shape() {
        let (s, o, c) = (VarId(0), VarId(1), VarId(2));
        let q = count_query(vec![], vec![graph_triple(s, o)], c, o);
        assert!(detect_fused_r2rml_aggregate(&q).is_some());
    }

    #[test]
    fn dim_dup_join_key_always_declines() {
        // The dim-scan map's soundness gate (#1490 review, HARDENED by the R-1528
        // duplicate-parent-key item): a duplicate parent join key means the
        // materialized join FANS OUT, which the single-entry-per-key map cannot
        // represent — so ANY duplicate (conflicting OR equal group-keys) declines
        // the fused plan (caller returns Ok(None) → generic pipeline). The equal-value
        // duplicate previously kept is a latent fan-out under-count, so it now declines
        // too. Proper star schemas have unique parent keys, so this never fires there.
        use std::collections::HashMap;
        let mut m: HashMap<Vec<String>, Vec<GKey>> = HashMap::new();
        let k = vec!["1".to_string()];
        assert!(insert_dim_gkeys(
            &mut m,
            k.clone(),
            vec![GKey::Str("A".into())]
        ));
        // equal-value duplicate → now DECLINES (was previously kept): the fan-out
        // the map can't represent would under-count.
        assert!(!insert_dim_gkeys(
            &mut m,
            k.clone(),
            vec![GKey::Str("A".into())]
        ));
        // conflicting duplicate (different attrs) → declines (mis-attribution).
        assert!(!insert_dim_gkeys(
            &mut m,
            k.clone(),
            vec![GKey::Str("B".into())]
        ));
        // a distinct key still inserts.
        assert!(insert_dim_gkeys(
            &mut m,
            vec!["2".to_string()],
            vec![GKey::Int(9)]
        ));
    }

    /// D2 (#1514 review): this PR WIDENS fused-aggregate admission into the JOIN fold
    /// (W4-2 mixed fact+dim GROUP BY keys, and dim-side E2), which reaches
    /// `insert_dim_gkeys` via `resolve_join_at_open`'s terminal-dim scan. B1 made
    /// `insert_dim_gkeys` decline ANY duplicate parent join key; `dim_dup_join_key_
    /// always_declines` guards that primitive. THIS test guards the other half — that
    /// the CALLER (`resolve_join_at_open`, reached only by the widened join shapes)
    /// propagates the decline to `Ok(None)` when the dim has a NON-PK (repeating) join
    /// key, so the fuse falls back to the sound materialize path instead of
    /// under-counting the fan-out. A UNIQUE-key control FUSES (`Ok(Some)`), isolating
    /// the duplicate key as the sole cause: a broken fixture would fail the control
    /// rather than pass vacuously. SF01's unique PKs mean the corpus can never raise
    /// this, which is exactly why 0 hash mismatches is not evidence here.
    ///
    /// BOUNDARY: only the JOIN fold reaches `insert_dim_gkeys`. E1 (disjoint-colocated
    /// shared-predicate class fusion), C5 (single-data-view fold), and a fact-only E2
    /// flag all take the SINGLE-scan fold (`resolve_at_open`, `scan_table` n=1) and
    /// never build a FK→GKey map — so the widened-shape concern does not extend to
    /// them. `route_group_key_sources_mixed_and_declines` covers which keys route to a
    /// dim vs the fact.
    #[tokio::test]
    async fn join_fold_declines_on_non_pk_dim_join_key() {
        use crate::r2rml::{ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter};
        use crate::seed::EmptyOperator;
        use crate::var_registry::VarRegistry;
        use async_trait::async_trait;
        use fluree_db_core::LedgerSnapshot;
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
            TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
        use std::sync::Arc;

        // Star mapping: SALES (fact) --custRef(RefObjectMap CUST_FK->CID)--> CUSTOMER
        // (dim). Fact scalar attr `channel` (CHANNEL col), dim scalar attr `region`
        // (REGION col) — the two group keys of a W4-2 MIXED rollup.
        let mapping = Arc::new(CompiledR2rmlMapping::new(vec![
            TriplesMap::new("#Sales", "sales")
                .with_subject_template("http://ex/sale/{SID}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/channel"),
                    object_map: ObjectMap::column("CHANNEL"),
                })
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/custRef"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Customer",
                        "CUST_FK",
                        "CID",
                    )),
                }),
            TriplesMap::new("#Customer", "customer")
                .with_subject_template("http://ex/cust/{CID}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/region"),
                    object_map: ObjectMap::column("REGION"),
                }),
        ]));

        // W4-2 patterns: fact(subj ?0) binds channel=?10 (fact key) and custRef=?1
        // (=dim subject, the join var); dim(subj ?1) binds region=?11 (dim key).
        let (s, cust, channel, region, cnt) = (VarId(0), VarId(1), VarId(10), VarId(11), VarId(20));
        let mut fact = R2rmlPattern::new("gs", s, None);
        fact.triples_map_iri = Some("#Sales".to_string());
        fact.star_bindings = vec![
            ("http://ex/channel".to_string(), channel),
            ("http://ex/custRef".to_string(), cust),
        ];
        let mut dim = R2rmlPattern::new("gs", cust, None);
        dim.triples_map_iri = Some("#Customer".to_string());
        dim.star_bindings = vec![("http://ex/region".to_string(), region)];
        let pats = [&fact, &dim];

        fn customer_batch(cids: Vec<Option<i64>>) -> ColumnBatch {
            let schema = Arc::new(BatchSchema::new(vec![
                FieldInfo {
                    name: "CID".to_string(),
                    field_type: FieldType::Int64,
                    nullable: true,
                    field_id: 1,
                },
                FieldInfo {
                    name: "REGION".to_string(),
                    field_type: FieldType::String,
                    nullable: true,
                    field_id: 2,
                },
            ]));
            let regions: Vec<Option<String>> =
                vec![Some("East".to_string()), Some("West".to_string())];
            ColumnBatch::new(schema, vec![Column::Int64(cids), Column::String(regions)]).unwrap()
        }

        // The terminal-dim provider. `dup` toggles the CUSTOMER join key from a proper
        // PK (10, 20) to a NON-PK repeat (10, 10) — the fan-out the single-entry
        // FK->GKey map cannot represent. (resolve_join_at_open scans ONLY the dim; the
        // fact scan is deferred to next_batch.)
        #[derive(Debug)]
        struct DimProvider {
            dup: bool,
        }
        #[async_trait]
        impl R2rmlTableProvider for DimProvider {
            async fn scan_table(
                &self,
                _gs: &str,
                table: &str,
                _proj: &[String],
                _filters: &[ScanFilter],
                _topk: Option<&crate::r2rml::ScanTopK>,
                _t: Option<i64>,
            ) -> Result<ColumnBatchStream> {
                assert_eq!(
                    table, "customer",
                    "the one-hop join scans only the dim here"
                );
                let cids = if self.dup {
                    vec![Some(10), Some(10)]
                } else {
                    vec![Some(10), Some(20)]
                };
                let b = customer_batch(cids);
                Ok(Box::pin(futures::stream::once(async move { Ok(b) })))
            }
        }

        #[derive(Debug)]
        struct MapProvider(Arc<CompiledR2rmlMapping>);
        #[async_trait]
        impl R2rmlProvider for MapProvider {
            async fn has_r2rml_mapping(&self, _gs: &str) -> bool {
                true
            }
            async fn compiled_mapping(
                &self,
                _gs: &str,
                _t: Option<i64>,
            ) -> Result<Arc<CompiledR2rmlMapping>> {
                Ok(Arc::clone(&self.0))
            }
        }

        let make_op = || {
            let plan = FusedAggregatePlan {
                graph_iri: Arc::from("gs"),
                inner_patterns: vec![],
                filter: None,
                agg_binds: vec![],
                group_by: vec![channel, region], // W4-2 MIXED: fact key + dim key
                aggregates: vec![(cnt, AggregateFn::CountAll)],
            };
            FusedR2rmlAggregateOperator::new(plan, Box::new(EmptyOperator::new()))
        };

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let map_provider = MapProvider(Arc::clone(&mapping));

        // CONTROL: a proper PK dim → the widened W4-2 join fold RESOLVES (fuses). This
        // proves the mapping/patterns/plan are valid, so the decline below is caused by
        // the duplicate key alone.
        let unique = DimProvider { dup: false };
        let ctx_u =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&map_provider, &unique);
        let resolved_u = make_op()
            .resolve_join_at_open(&ctx_u, &pats, mapping.as_ref())
            .await
            .expect("resolve must not error");
        assert!(
            resolved_u.is_some(),
            "a unique dim join key must FUSE (control: the fixture is otherwise valid)"
        );

        // A non-PK (repeating) dim join key must DECLINE the fused join fold — the
        // widened shape reaches insert_dim_gkeys, which now refuses the fan-out.
        let dup = DimProvider { dup: true };
        let ctx_d =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&map_provider, &dup);
        let resolved_d = make_op()
            .resolve_join_at_open(&ctx_d, &pats, mapping.as_ref())
            .await
            .expect("resolve must not error");
        assert!(
            resolved_d.is_none(),
            "a non-PK (duplicate) dim join key must DECLINE the fused join fold \
             (fall back to the materialize path), not silently under-count"
        );
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
            cypher_vocab: None,
        };
        assert!(detect_fused_r2rml_aggregate(&q).is_some());
    }

    // PR-6: HAVING is admitted (applied by a wrapping HavingOperator) when its
    // lifted aggregate is already in the SELECT projection.
    #[test]
    fn admits_having_referencing_projected_aggregate() {
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
            // HAVING references the projected aggregate ?c (no synthetic extra) →
            // outs == projected → fused.
            grouping: Grouping::assemble(vec![g], vec![agg], vec![], Some(Expression::Var(c))),
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
            include_system_facts: false,
            cypher_vocab: None,
        };
        assert!(detect_fused_r2rml_aggregate(&q).is_some());
    }

    // PR-6: the conservative HAVING admission line — a HAVING that lifts an
    // aggregate NOT in the SELECT projection makes outs ⊋ projected, so the
    // projection check declines and the query stays on the generic path.
    #[test]
    fn declines_having_over_unprojected_aggregate() {
        let (s, o, g, c, c2) = (VarId(0), VarId(1), VarId(2), VarId(3), VarId(4));
        let agg = AggregateSpec {
            function: AggregateFn::Count(o),
            output_var: c,
        };
        let agg2 = AggregateSpec {
            function: AggregateFn::Count(s),
            output_var: c2,
        };
        let q = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![g, c]),
            patterns: vec![graph_triple(s, o)],
            reasoning: ReasoningConfig::default(),
            grouping: Grouping::assemble(
                vec![g],
                vec![agg, agg2],
                vec![],
                Some(Expression::Var(c2)),
            ),
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
            include_system_facts: false,
            cypher_vocab: None,
        };
        assert!(detect_fused_r2rml_aggregate(&q).is_none());
    }

    // PR-6: the fact/dim join classifier + cycle-guard used by
    // resolve_join_at_open. A single FK direction resolves one way; an FK loop
    // resolves both ways, which the resolver's `(Some, Some)` arm declines.
    #[test]
    fn joins_via_classifies_direction_and_flags_cycle() {
        let mut fact = R2rmlPattern::new("gs", VarId(0), None);
        fact.star_bindings = vec![("p:fk".to_string(), VarId(1))];
        let dim = R2rmlPattern::new("gs", VarId(1), None);
        assert_eq!(
            FusedR2rmlAggregateOperator::joins_via(&fact, &dim),
            Some(VarId(1)),
            "fact binds dim's subject as an object → join var found"
        );
        assert_eq!(
            FusedR2rmlAggregateOperator::joins_via(&dim, &fact),
            None,
            "dim does not bind fact's subject → no reverse join"
        );

        let mut a = R2rmlPattern::new("gs", VarId(0), None);
        a.star_bindings = vec![("p".to_string(), VarId(1))];
        let mut b = R2rmlPattern::new("gs", VarId(1), None);
        b.star_bindings = vec![("p".to_string(), VarId(0))];
        assert!(FusedR2rmlAggregateOperator::joins_via(&a, &b).is_some());
        assert!(
            FusedR2rmlAggregateOperator::joins_via(&b, &a).is_some(),
            "an FK loop resolves in both directions → the classifier declines it"
        );
    }

    // PR-6b: the linear-chain ordering + its decline guards (cycle, branch).
    #[test]
    fn order_chain_orders_linear_and_declines_nonlinear() {
        let star = |subj: u16, pred: &str, obj: u16| {
            let mut p = R2rmlPattern::new("gs", VarId(subj), None);
            p.star_bindings = vec![(pred.to_string(), VarId(obj))];
            p
        };
        // fact(0)→dim1(1)→dim2(2); dim2's `attr` binds ?3 (a scalar key, no pattern).
        let fact = star(0, "customer", 1);
        let dim1 = star(1, "geography", 2);
        let dim2 = star(2, "region", 3);
        // Pass shuffled; order_chain must recover fact→dim1→dim2.
        let (chain, jvs) =
            FusedR2rmlAggregateOperator::order_chain(&[&dim2, &fact, &dim1]).expect("linear chain");
        assert_eq!(
            chain
                .iter()
                .map(|p| p.subject_var.unwrap())
                .collect::<Vec<_>>(),
            vec![VarId(0), VarId(1), VarId(2)]
        );
        assert_eq!(jvs, vec![VarId(1), VarId(2)], "one join var per hop");

        // Cycle 0→1→2→0: no root, declines (no spin).
        let (a, b, mut c) = (star(0, "p", 1), star(1, "p", 2), star(2, "p", 0));
        c.star_bindings = vec![("p".to_string(), VarId(0))];
        assert!(FusedR2rmlAggregateOperator::order_chain(&[&a, &b, &c]).is_none());

        // Branch: the fact ref-joins to TWO dims → declines (not a linear chain).
        let mut branch_fact = R2rmlPattern::new("gs", VarId(0), None);
        branch_fact.star_bindings =
            vec![("p1".to_string(), VarId(1)), ("p2".to_string(), VarId(2))];
        let d1 = R2rmlPattern::new("gs", VarId(1), None);
        let d2 = R2rmlPattern::new("gs", VarId(2), None);
        assert!(FusedR2rmlAggregateOperator::order_chain(&[&branch_fact, &d1, &d2]).is_none());
    }

    // W4-2 gate Q1: route each GROUP BY var to its single source pattern (fact or
    // terminal dim); decline on 0, ≥2, or interior-dim sources. `predicate_for_var`
    // drives routing off the R2rmlPattern star_bindings, so this is pure.
    #[test]
    fn route_group_key_sources_mixed_and_declines() {
        // fact(0): fact attr `shipMethod`=?10, FK-to-date `dateDim`=?1.
        let mut fact = R2rmlPattern::new("gs", VarId(0), None);
        fact.star_bindings = vec![
            ("shipMethod".into(), VarId(10)),
            ("dateDim".into(), VarId(1)),
        ];
        // terminal dim(1): dim attr `year`=?11.
        let mut dim = R2rmlPattern::new("gs", VarId(1), None);
        dim.star_bindings = vec![("year".into(), VarId(11))];
        let chain = [&fact, &dim];

        // MIXED, both key orders: fact key ?10 → idx 0, dim key ?11 → idx 1.
        assert_eq!(
            FusedR2rmlAggregateOperator::route_group_key_sources(&chain, &[VarId(11), VarId(10)]),
            Some(vec![1, 0])
        );
        assert_eq!(
            FusedR2rmlAggregateOperator::route_group_key_sources(&chain, &[VarId(10), VarId(11)]),
            Some(vec![0, 1])
        );
        // All-fact (invariant #1: dim present for the join but grouping only on fact).
        assert_eq!(
            FusedR2rmlAggregateOperator::route_group_key_sources(&chain, &[VarId(10)]),
            Some(vec![0])
        );
        // Unbound var (0 sources) → decline.
        assert_eq!(
            FusedR2rmlAggregateOperator::route_group_key_sources(&chain, &[VarId(99)]),
            None
        );
        // Cross-source: the same var is an object in BOTH fact and dim (≥2) → decline.
        let mut fact2 = R2rmlPattern::new("gs", VarId(0), None);
        fact2.star_bindings = vec![("x".into(), VarId(7)), ("dateDim".into(), VarId(1))];
        let mut dim2 = R2rmlPattern::new("gs", VarId(1), None);
        dim2.star_bindings = vec![("x".into(), VarId(7))];
        assert_eq!(
            FusedR2rmlAggregateOperator::route_group_key_sources(&[&fact2, &dim2], &[VarId(7)]),
            None
        );
        // Interior-dim source (chain fact→interior→terminal), key on the interior → decline (v1).
        let mut f3 = R2rmlPattern::new("gs", VarId(0), None);
        f3.star_bindings = vec![("h1".into(), VarId(1))];
        let mut mid = R2rmlPattern::new("gs", VarId(1), None);
        mid.star_bindings = vec![("attr".into(), VarId(20)), ("h2".into(), VarId(2))];
        let term = R2rmlPattern::new("gs", VarId(2), None);
        assert_eq!(
            FusedR2rmlAggregateOperator::route_group_key_sources(&[&f3, &mid, &term], &[VarId(20)]),
            None,
            "an interior-dim group key declines in v1"
        );
    }

    // W4-2: assemble the composite key by interleaving fact-inline + dim-resolved
    // positions in SPARQL order; a null in ANY position drops the row.
    #[test]
    fn assemble_group_key_interleaves_and_drops_nulls() {
        let s_col = GroupCol {
            column: "SHIP_METHOD".into(),
            kind: GKind::String,
            dt_sid: Sid::new(1, "string"),
        };
        let i_col = GroupCol {
            column: "YEAR_NUM".into(),
            kind: GKind::Integer,
            dt_sid: Sid::new(2, "integer"),
        };
        let ground = Column::String(vec![Some("Ground".into())]);
        let null_str = Column::String(vec![None]);

        // plan [Fact(shipMethod), Dim(0)=year] → [Str, Int] in order.
        let gc = vec![s_col.clone(), i_col.clone()];
        let kc = vec![Some(&ground), None];
        assert_eq!(
            FusedR2rmlAggregateOperator::assemble_group_key(
                &[KeySource::Fact, KeySource::Dim(0)],
                &gc,
                &kc,
                Some(&[GKey::Int(2024)]),
                0
            ),
            Some(vec![GKey::Str("Ground".into()), GKey::Int(2024)])
        );
        // Swapped order [Dim(0)=year, Fact(shipMethod)] → [Int, Str].
        let gc2 = vec![i_col.clone(), s_col.clone()];
        let kc2 = vec![None, Some(&ground)];
        assert_eq!(
            FusedR2rmlAggregateOperator::assemble_group_key(
                &[KeySource::Dim(0), KeySource::Fact],
                &gc2,
                &kc2,
                Some(&[GKey::Int(2024)]),
                0
            ),
            Some(vec![GKey::Int(2024), GKey::Str("Ground".into())])
        );
        // Null FACT key position → drop.
        let kc_null = vec![Some(&null_str), None];
        assert_eq!(
            FusedR2rmlAggregateOperator::assemble_group_key(
                &[KeySource::Fact, KeySource::Dim(0)],
                &gc,
                &kc_null,
                Some(&[GKey::Int(2024)]),
                0
            ),
            None
        );
        // Null DIM key position → drop.
        assert_eq!(
            FusedR2rmlAggregateOperator::assemble_group_key(
                &[KeySource::Fact, KeySource::Dim(0)],
                &gc,
                &kc,
                Some(&[GKey::Null]),
                0
            ),
            None
        );
        // Dim slot present but no resolver value → drop (defensive).
        assert_eq!(
            FusedR2rmlAggregateOperator::assemble_group_key(
                &[KeySource::Dim(0)],
                std::slice::from_ref(&i_col),
                &[None],
                None,
                0
            ),
            None
        );
        // All-fact plan, no resolver: key wholly inline (invariant #1 fold side).
        assert_eq!(
            FusedR2rmlAggregateOperator::assemble_group_key(
                &[KeySource::Fact],
                std::slice::from_ref(&s_col),
                &[Some(&ground)],
                None,
                0
            ),
            Some(vec![GKey::Str("Ground".into())])
        );
        // W4-2 invariant #1, the DEGENERATE join branch: an all-fact plan over a
        // join yields `Some(&[])` from the FK→GKey map (existence passed, no dim
        // keys). assemble must take the key wholly from the fact and NOT index the
        // empty dim slice — the empty-dim-subset join reduces to an FK-existence
        // filter + inline fact grouping.
        assert_eq!(
            FusedR2rmlAggregateOperator::assemble_group_key(
                &[KeySource::Fact],
                std::slice::from_ref(&s_col),
                &[Some(&ground)],
                Some(&[]),
                0
            ),
            Some(vec![GKey::Str("Ground".into())])
        );
    }

    // PR-6: an `xsd:integer` group key stored as a Snowflake `NUMBER` arrives as
    // a physical `Column::Decimal`; the Integer reader must extract it (the q010
    // `YEAR_NUM`/`QUARTER_NUM` 0-rows regression — a Decimal read as null key
    // dropped every dim row).
    #[test]
    fn key_at_reads_integer_group_key_from_decimal() {
        let gc = GroupCol {
            column: "YEAR_NUM".to_string(),
            kind: GKind::Integer,
            dt_sid: Sid::new(2, "integer"),
        };
        let scale0 = Column::Decimal {
            values: vec![Some(2024), None],
            precision: 38,
            scale: 0,
        };
        assert_eq!(gc.key_at(Some(&scale0), 0), GKey::Int(2024));
        assert_eq!(
            gc.key_at(Some(&scale0), 1),
            GKey::Null,
            "null decimal → null key"
        );
        // Exact-integer decimal with a non-zero scale (202400 · 10^-2 = 2024).
        let scaled = Column::Decimal {
            values: vec![Some(202_400)],
            precision: 38,
            scale: 2,
        };
        assert_eq!(gc.key_at(Some(&scaled), 0), GKey::Int(2024));
        // The native integer columns still read.
        assert_eq!(
            gc.key_at(Some(&Column::Int64(vec![Some(7)])), 0),
            GKey::Int(7)
        );
        assert_eq!(
            gc.key_at(Some(&Column::Int32(vec![Some(3)])), 0),
            GKey::Int(3)
        );
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
                cypher_vocab: None,
            };
            assert!(detect_fused_r2rml_aggregate(&q).is_some());
        }
    }

    #[test]
    fn accumulate_double_row_drops_nan() {
        // A NaN must be dropped (not summed, not counted), matching the standard
        // aggregate pipeline — else SUM/AVG poison to NaN and the count inflates.
        let col = Column::Float64(vec![Some(1.0), Some(f64::NAN), Some(3.0)]);
        let (mut sum, mut count) = (0.0f64, 0u64);
        for row in 0..3 {
            accumulate_double_row(&col, row, &mut sum, &mut count);
        }
        assert_eq!(count, 2, "NaN row is not counted");
        assert_eq!(sum, 4.0, "NaN does not poison the sum");

        let col32 = Column::Float32(vec![Some(2.0f32), Some(f32::NAN)]);
        let (mut sum, mut count) = (0.0f64, 0u64);
        accumulate_double_row(&col32, 0, &mut sum, &mut count);
        accumulate_double_row(&col32, 1, &mut sum, &mut count);
        assert_eq!(count, 1);
        assert_eq!(sum, 2.0);
    }

    #[test]
    fn eval_dec_drops_null_but_escalates_overflow() {
        let (v, w) = (VarId(0), VarId(1));

        // A present operand folds to a value.
        let bound = [(v, Some(Dec { val: 5, scale: 0 }))];
        assert!(matches!(
            eval_dec(&Expression::Var(v), &bound),
            DecEval::Val(_)
        ));

        // A null operand is a legitimate row drop, not an error.
        let nullv = [(v, None)];
        assert!(matches!(
            eval_dec(&Expression::Var(v), &nullv),
            DecEval::Null
        ));

        // Exact arithmetic on bound operands folds to a value.
        let add = Expression::Call {
            func: Function::Add,
            args: vec![Expression::Var(v), Expression::Var(w)],
        };
        let ok = [
            (v, Some(Dec { val: 3, scale: 0 })),
            (w, Some(Dec { val: 4, scale: 0 })),
        ];
        assert!(matches!(eval_dec(&add, &ok), DecEval::Val(d) if d.val == 7));

        // An i128 intermediate overflow escalates (must NOT collapse to a drop).
        let mul = Expression::Call {
            func: Function::Mul,
            args: vec![Expression::Var(v), Expression::Var(w)],
        };
        let big = [
            (
                v,
                Some(Dec {
                    val: i128::MAX,
                    scale: 0,
                }),
            ),
            (w, Some(Dec { val: 2, scale: 0 })),
        ];
        assert!(matches!(eval_dec(&mul, &big), DecEval::Overflow));

        // Overflow takes precedence over a null sibling — still a safe fallback.
        let nested = Expression::Call {
            func: Function::Mul,
            args: vec![
                Expression::Call {
                    func: Function::Mul,
                    args: vec![Expression::Var(v), Expression::Const(FlakeValue::Long(2))],
                },
                Expression::Var(w),
            ],
        };
        let big_and_null = [
            (
                v,
                Some(Dec {
                    val: i128::MAX,
                    scale: 0,
                }),
            ),
            (w, None),
        ];
        assert!(matches!(
            eval_dec(&nested, &big_and_null),
            DecEval::Overflow
        ));
    }
}
