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

use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

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
    extract_template_columns, CompiledR2rmlMapping, ObjectMap, PredicateObjectMap, RefObjectMap,
    TermType, TriplesMap,
};
use fluree_db_r2rml::materialize::{
    canonical_join, get_join_key_from_batch, materialize_object_from_batch,
    materialize_subject_from_batch, subject_sort_key, RdfTerm,
};
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

/// Whether a declared object-map datatype is admissible for a fused MIN/MAX fold:
/// a numeric (`xsd:integer`/`long`/`int`/`decimal`/`double`/`float`) or temporal
/// (`xsd:date`/`dateTime`) type. Everything else — string, boolean, IRI-/langtag-
/// typed, or an **un-annotated** column (which the R2RML natural mapping treats as
/// `xsd:string`, so its MIN/MAX would be lexical) — declines, keeping the generic
/// pipeline's collation/term-type semantics. Fold parity itself comes from
/// materializing the same term + `compare_bindings`; this gate only scopes the
/// mechanism to the types the audit item covers (F-AUD-8).
fn minmax_admissible_datatype(datatype: Option<&str>) -> bool {
    use fluree_vocab::xsd;
    matches!(
        datatype,
        Some(dt) if dt == xsd::INTEGER
            || dt == xsd::LONG
            || dt == xsd::INT
            || dt == xsd::DECIMAL
            || dt == xsd::DOUBLE
            || dt == xsd::FLOAT
            || dt == xsd::DATE
            || dt == xsd::DATE_TIME
    )
}

/// Whether a MIN/MAX fold should replace its running extreme with a candidate
/// whose `compare_bindings(candidate, current)` is `ord`. This mirrors the
/// generic `agg_min`/`agg_max` (`min_by`/`max_by`) tie-breaking EXACTLY:
/// - MIN keeps the FIRST minimum (`min_by` returns the first of equal-minimums),
///   so it replaces only on a strictly-less candidate;
/// - MAX keeps the LAST maximum (`max_by` returns the last of equal-maximums),
///   so it replaces on greater-OR-EQUAL — the later of two equal elements wins.
///
/// The `Equal` case is load-bearing: two values that compare equal can still
/// RENDER differently (double `+0.0` vs `-0.0` → "0" vs "-0"; or a decimal at two
/// scales — `1.50` vs `1.5`), so picking the wrong one breaks byte-parity with the
/// materialized aggregate even though the values are "equal". Candidates are
/// materialized before the compare, so replacing on `Equal` costs no extra work.
fn minmax_should_replace(is_max: bool, ord: std::cmp::Ordering) -> bool {
    if is_max {
        ord != std::cmp::Ordering::Less
    } else {
        ord == std::cmp::Ordering::Less
    }
}

/// Whether the bare-COUNT manifest shortcut is eligible for a resolved fused plan:
/// exactly one `CountRows` fold, no GROUP BY, no FILTER, and no folded
/// constant-object constraints. The Iceberg manifest `record_count` sum cannot see
/// per-row FILTER/constraint matches or per-group partitions, so anything else must
/// fall through to the scan-fold (which applies them). This is the D-c5 soundness
/// line for item 9b in particular: a constraint-bearing COUNT (e.g. `isCurrent
/// true`) MUST decline the delete-blind shortcut and count matching rows in the
/// fold instead. Extracted as a pure predicate so the decline invariant is
/// DIRECTLY unit-tested, not only verified by inspection (R-1522 verified it that
/// way). `filter_present` is passed as a bool because a `FilterPlan` needs a live
/// `LedgerSnapshot` to build — the shortcut only ever cares about its presence.
fn count_shortcut_eligible(
    filter_present: bool,
    group_cols: &[GroupCol],
    fact_constraints: &[ResolvedConstraint],
    folds: &[Fold],
) -> bool {
    !filter_present
        && group_cols.is_empty()
        && fact_constraints.is_empty()
        && matches!(folds, [Fold::CountRows])
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

    // Cost guard: an explicit SPARQL `FILTER` is only fused alongside a GROUP BY.
    // There the fused path's win (skipping the subject + the many grouped/aggregated
    // columns) dwarfs the per-row filter eval. For a filtered single aggregate the
    // normal pipeline's file pruning + vectorized filter is faster, so decline.
    //
    // F1 NOTE (the q038 class): this guard is NOT what declines the ungrouped
    // filtered COUNT `SELECT (COUNT(*)) WHERE { ?s a edw:Customer ; edw:isCurrent
    // true }`. A constant-object triple stays a `Pattern::Triple` (SPARQL lowering
    // never desugars `edw:isCurrent true` to `?v` + `FILTER(?v = true)`), so `filter`
    // is `None` here and this guard does not fire — q038 is ADMITTED. Its decline is
    // downstream, at the single-`[R2rml]` shape gate in `resolve_at_open`, because
    // the rewrite splits its class + const-object members into separate scans;
    // `combine_constrained_class_scan` recombines them. This guard remains for a
    // genuine residual FILTER (`?s edw:score ?v . FILTER(?v > 100)`), where the
    // cost argument above still holds.
    if filter.is_some() && group_by.is_empty() {
        return None;
    }

    // Every aggregate must be a column fold this operator supports.
    let mut aggregates = Vec::with_capacity(aggregation.aggregates.len());
    for spec in aggregation.aggregates.iter() {
        // Only multiset (non-DISTINCT) COUNT/SUM/AVG and MIN/MAX fold from columns;
        // the fused path has no dedup, so DISTINCT (Set) must fall back to the
        // normal pipeline. `CountDistinct` is already a separate, unmatched variant.
        // MIN/MAX carry no DISTINCT flag (dedup is a no-op for them). Whether a
        // MIN/MAX aggregate variable actually resolves to a foldable numeric/temporal
        // column is decided at `open` (`resolve_agg_folds`), which declines string /
        // language- / IRI-typed / un-annotated columns.
        let foldable = match &spec.function {
            AggregateFn::CountAll | AggregateFn::Count(_) => true,
            AggregateFn::Sum(_, sem) | AggregateFn::Avg(_, sem) => {
                matches!(sem, InputSemantics::List)
            }
            AggregateFn::Min(_) | AggregateFn::Max(_) => true,
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
    /// `MIN(?col)` / `MAX(?col)` over a numeric or date/timestamp scalar column;
    /// `index` points into `Resolved::minmax_folds`. Unlike the COUNT/SUM/AVG
    /// folds this one materializes the candidate object term (via the same
    /// `materialize_object_from_batch` + `LiteralEncoder` path the FILTER fold
    /// uses) and keeps the running extreme by `compare_bindings` — byte-parity
    /// with the generic `agg_min`/`agg_max`, but streaming (O(1) memory) instead
    /// of buffering every value and skipping the subject/BindingRow build.
    MinMax { index: usize },
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
    /// Running MIN/MAX extreme, held as the materialized winning `Binding`
    /// (`Unbound` until the first non-null row). The extreme is updated by
    /// `compare_bindings` in the fold loop (the `Fold::MinMax` arm), so this is the
    /// exact `Binding` the generic `agg_min`/`agg_max` would return.
    MinMax {
        best: Binding,
    },
}

impl Acc {
    fn for_fold(fold: &Fold) -> Self {
        match fold {
            Fold::CountRows | Fold::CountColumn(_) => Acc::Count(0),
            Fold::MinMax { .. } => Acc::MinMax {
                best: Binding::Unbound,
            },
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
            // The extreme is already the materialized winning term (or `Unbound`
            // for an empty group), byte-identical to `agg_min`/`agg_max`.
            Acc::MinMax { best } => best,
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
#[derive(Clone, Copy, Debug)]
enum GKind {
    String,
    Integer,
    /// P2a: a RefObjectMap group key — the group value is the referenced parent
    /// SUBJECT IRI, not a table cell. It is always Dim-sourced (resolved through the
    /// FK→IRI map, never read inline from the fact batch), so `key_at`/`key_ref_at`
    /// never produce it; only `binding` acts on it, emitting an IRI term (not a
    /// literal) so the output matches the generic path's `RdfTerm::Iri`.
    Iri,
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

/// N1 kill switch (`FLUREE_FUSED_VECTOR_FOLD`, default **on**). Off (`0`/`false`/
/// `off`/`no`) restores the byte-identical `HashMap<Vec<GKey>, Vec<Acc>>` fold that
/// allocates + clones a fresh owned key every row. Read once per process.
fn vector_fold_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| crate::r2rml::env_switch_enabled("FLUREE_FUSED_VECTOR_FOLD"))
}

/// Rows per bounded output chunk when output-bounding is on (see `pending_rows`). A
/// GROUP BY rollup with more groups than this emits multiple batches instead of one
/// giant one, so a high-cardinality result never fully materializes at once.
const OUTPUT_BOUND_ROWS: usize = 8192;

/// Output-bounding kill switch (`FLUREE_FUSED_R2RML_OUTPUT_BOUND`, default **on**).
/// Off restores the single-batch emission wholesale (byte-identical to pre-bounding:
/// one batch, groups in dict/map iteration order). Read uncached under cfg(test) so
/// the on/off differential is testable without an `OnceLock` caching the first value.
fn output_bound_enabled() -> bool {
    #[cfg(not(test))]
    {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(|| crate::r2rml::env_switch_enabled("FLUREE_FUSED_R2RML_OUTPUT_BOUND"))
    }
    #[cfg(test)]
    {
        crate::r2rml::env_switch_enabled("FLUREE_FUSED_R2RML_OUTPUT_BOUND")
    }
}

/// N1: a BORROWED view of one composite-group-key component, read straight from
/// the scanned batch (or the resolver's owned dim GKeys) with no per-row `String`
/// clone. A null component drops the row before it reaches the key (matching
/// `assemble_group_key`), so `Null` is never a key component — hence only the two
/// value variants. Used to probe the dense group dict; the owned [`GKey`] is
/// materialized only when a NEW group is inserted.
#[derive(Clone, Copy)]
enum GKeyRef<'a> {
    Str(&'a str),
    Int(i128),
}

impl<'a> GKeyRef<'a> {
    /// Borrow an owned dim GKey (from the FK→GKey resolver). `None` for `GKey::Null`
    /// so the row drops, exactly as `assemble_group_key` drops on a null slot.
    fn from_owned(o: &'a GKey) -> Option<GKeyRef<'a>> {
        match o {
            GKey::Str(s) => Some(GKeyRef::Str(s.as_str())),
            GKey::Int(i) => Some(GKeyRef::Int(*i)),
            GKey::Null => None,
        }
    }

    /// Feed this component into a hasher (used by [`hash_key_refs`], the borrowed
    /// probe). Must stay in lockstep with the owned per-component hashing in
    /// [`gkeys_hash`] — both write a `Str`/`Int` tag byte then the value, so no
    /// dependence on `GKey`'s derived `Hash`. The tag keeps the domains disjoint.
    fn hash_into<H: Hasher>(&self, h: &mut H) {
        match self {
            GKeyRef::Str(s) => {
                0u8.hash(h);
                s.hash(h);
            }
            GKeyRef::Int(i) => {
                1u8.hash(h);
                i.hash(h);
            }
        }
    }

    /// Equality against a stored owned key component (the dict's probe predicate).
    fn eq_owned(&self, o: &GKey) -> bool {
        match (self, o) {
            (GKeyRef::Str(a), GKey::Str(b)) => *a == b.as_str(),
            (GKeyRef::Int(a), GKey::Int(b)) => *a == *b,
            _ => false,
        }
    }

    /// Materialize the owned key component (called once per group, on insert).
    fn to_owned_key(self) -> GKey {
        match self {
            GKeyRef::Str(s) => GKey::Str(s.to_string()),
            GKeyRef::Int(i) => GKey::Int(i),
        }
    }
}

/// Hash an OWNED composite key — the dict's resize-rehash function. This MUST be
/// value-identical to [`hash_key_refs`] for the equal borrowed key: a `HashTable`
/// rehashes every entry through this closure when it grows, and the per-row probe
/// hashes through `hash_key_refs`; if the two disagreed, a grow would re-bucket a
/// key away from where the probe looks and split its group. Kept in lockstep by
/// construction (same length prefix, same per-component tag + value).
fn gkeys_hash(k: &[GKey]) -> u64 {
    let mut h = FxHasher::default();
    k.len().hash(&mut h);
    for g in k {
        match g {
            GKey::Str(s) => {
                0u8.hash(&mut h);
                s.as_str().hash(&mut h);
            }
            GKey::Int(i) => {
                1u8.hash(&mut h);
                i.hash(&mut h);
            }
            GKey::Null => 2u8.hash(&mut h),
        }
    }
    h.finish()
}

/// Hash a BORROWED composite key — the dict's per-row probe. Mirror of
/// [`gkeys_hash`] (see its note on why they must agree).
fn hash_key_refs(scratch: &[GKeyRef]) -> u64 {
    let mut h = FxHasher::default();
    scratch.len().hash(&mut h);
    for c in scratch {
        c.hash_into(&mut h);
    }
    h.finish()
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
            // P2a: a ref-IRI key is Dim-sourced (from the FK→IRI map), never read
            // inline from the fact batch, so this is unreachable for it.
            GKind::Iri => GKey::Null,
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

    /// N1: BORROWED read of this column's group-key value at a row — the zero-clone
    /// twin of [`GroupCol::key_at`]. Returns `None` exactly where `key_at` returns
    /// `GKey::Null` (a null/absent/wrong-typed cell), so the vector fold drops the
    /// same rows and groups identically; the value variants match `key_at` bit for
    /// bit (including the `NUMBER(n,0)` physical-Decimal integer coercion).
    fn key_ref_at<'a>(&self, col: Option<&'a Column>, row: usize) -> Option<GKeyRef<'a>> {
        let c = col?;
        match self.kind {
            // P2a: a ref-IRI key is Dim-sourced; never read inline from the fact.
            GKind::Iri => None,
            GKind::String => match c {
                Column::String(v) => v.get(row)?.as_deref().map(GKeyRef::Str),
                _ => None,
            },
            GKind::Integer => match c {
                Column::Int64(v) => v.get(row).and_then(|o| *o).map(|i| GKeyRef::Int(i as i128)),
                Column::Int32(v) => v.get(row).and_then(|o| *o).map(|i| GKeyRef::Int(i as i128)),
                Column::Decimal { values, scale, .. } => match values.get(row).and_then(|o| *o) {
                    Some(unscaled) if *scale == 0 => Some(GKeyRef::Int(unscaled)),
                    Some(unscaled) if *scale > 0 => match pow10(i64::from(*scale)) {
                        Some(d) if unscaled % d == 0 => Some(GKeyRef::Int(unscaled / d)),
                        _ => None,
                    },
                    _ => None,
                },
                _ => None,
            },
        }
    }

    /// Materialize the output binding for a group key component.
    fn binding(&self, key: &GKey) -> Binding {
        match (self.kind, key) {
            // P2a: a ref-IRI group key emits an IRI TERM (byte-identical to the
            // generic path's `RdfTerm::Iri => Binding::iri`), not an xsd:string
            // literal. The GKey holds the parent subject IRI minted at resolver-build.
            (GKind::Iri, GKey::Str(s)) => Binding::iri(s.as_str()),
            (_, GKey::Str(s)) => Binding::lit(FlakeValue::String(s.clone()), self.dt_sid.clone()),
            (_, GKey::Int(i)) => Binding::lit(FlakeValue::Long(*i as i64), self.dt_sid.clone()),
            (_, GKey::Null) => Binding::Unbound,
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
    /// P3 (crt_join_reorder class): `Some` for a branching-star multi-fact join whose
    /// SEMI-JOIN branch resolved to a membership set. Each streamed fact row is probed
    /// against it (drop on miss) BEFORE the group-key resolve/fold — an existence
    /// semi-join, byte-identical to the generic chained inner join
    /// (`build_semi_join_membership`). `None` for the single-table and pure-linear
    /// join folds.
    semi_join: Option<SemiJoinSet>,
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
    /// MIN/MAX fold plans, indexed by `Fold::MinMax.index`. Empty unless the query
    /// carries a MIN/MAX aggregate. The aggregates always fold from the FACT scan
    /// (single-table or join), so these read the fact batch.
    minmax_folds: Vec<MinMaxFold>,
    /// Shared literal encoder for materializing MIN/MAX candidate terms into
    /// `Binding`s (datatype Sids pre-resolved from the fact TriplesMap). `None`
    /// when there are no MIN/MAX folds.
    minmax_encoder: Option<LiteralEncoder>,
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

/// P3 (multi-fact branching-star join, crt_join_reorder class): one FK branch off
/// the fact root — a linear sub-chain from the root's FK target to a terminal.
/// Carried in a typed LIST ([`JoinBranch`]) so K-branch generalization is additive
/// rather than a per-shape rewrite; v1 admission accepts exactly one of each variant.
#[derive(Debug)]
struct Branch<'p> {
    /// The fact root's FK join var TO this branch's head (root → chain[0]).
    root_join_var: VarId,
    /// The branch's linear patterns, head..terminal (EXCLUDES the shared root).
    chain: Vec<&'p R2rmlPattern>,
    /// Within-branch join vars: `chain[i]` ref-joins `chain[i+1]` via `join_vars[i]`.
    join_vars: Vec<VarId>,
}

/// P3: a classified FK branch. GROUP-KEY = the branch whose sub-chain binds a
/// GROUP BY var (resolved via the existing FK→GKey [`GroupKeyResolver`]).
/// SEMI-JOIN = a pure membership/constraint branch (e.g. order→customer=Enterprise),
/// resolved to a keep-min-then-filter membership set the fact fold probes.
#[derive(Debug)]
enum JoinBranch<'p> {
    GroupKey(Branch<'p>),
    SemiJoin(Branch<'p>),
}

/// P3: the decomposition of a branching-star join — the fact root plus its
/// classified branches. v1 admits exactly one GROUP-KEY + one SEMI-JOIN branch
/// (the crt_join_reorder class); the list carries any K for the follow-on.
#[derive(Debug)]
struct BranchingStar<'p> {
    root: &'p R2rmlPattern,
    branches: Vec<JoinBranch<'p>>,
}

/// P3 (crt_join_reorder class): the resolved SEMI-JOIN branch, built once at `open`.
/// A fact row survives iff its `fact_fk_cols` value (the root→branch first-hop FK)
/// is in `membership` — the set of root-FK join keys whose keep-min-resolved branch
/// chain satisfies every branch constraint (`build_semi_join_membership`). Because
/// the set is built KEEP-MIN-THEN-FILTER (each hop resolves to the keep-min parent
/// SUBJECT, byte-identical to the generic `build_parent_lookup`, THEN the kept row is
/// filtered), the probe is byte-identical to the generic chained inner join even on a
/// duplicate intermediate key.
///
/// #1583: semi-join ≡ inner join here rests on each FK being SINGLE-VALUED — one fact
/// row matches at most one branch row per hop. A fan-out `RefObjectMap` (one child →
/// many parents) would make a discarded-dup row a legitimate join partner, so a
/// membership *set* would then under-represent the fan-out; the fan-out follow-on must
/// revisit this site (mirrors the P2a #1583 caveat on the FK→IRI group key).
struct SemiJoinSet {
    /// Root-fact columns forming the probe key — the root→branch first-hop CHILD
    /// columns, in join-condition order (mirrors [`GroupKeyResolver::fact_fk_cols`]).
    fact_fk_cols: Vec<String>,
    /// The admitted root-FK join keys, stringified via `get_join_key_from_batch`
    /// exactly as the fact probe stringifies its own key (same encoding both sides).
    membership: std::collections::HashSet<Vec<String>>,
}

/// P3 keep-min bookkeeping for one join key while building a [`SemiJoinSet`] level:
/// the keep-min parent SUBJECT decides which duplicate row wins (matching the generic
/// `parent_key_insert_keep_min`), and the WINNING row's own-constraint result +
/// next-hop FK value decide membership — the load-bearing keep-min-THEN-filter order.
struct KeptChainRow {
    /// The kept row's parent subject term; the keep-min tie-break key.
    subject: RdfTerm,
    /// Whether the kept row satisfies this chain pattern's own folded constraints.
    passes_own: bool,
    /// The kept row's FK value to the NEXT hop (interior levels only); `None` on the
    /// terminal level or when that FK column is null (a null FK breaks the chain).
    next_fk: Option<Vec<String>>,
}

/// A native `SUM(expr)` / `AVG(expr)` plan: the arithmetic expression and the
/// (variable, column, read-kind) of each referenced variable.
struct ExprFold {
    expr: Expression,
    var_cols: Vec<(VarId, String, DecKind)>,
}

/// A `MIN(?col)` / `MAX(?col)` fold plan. The object map materializes the
/// candidate term per non-null row via the same `materialize_object_from_batch`
/// path the FILTER fold uses (so the term — value + datatype/lang/term-type — is
/// byte-identical to the generic scan's), and the running extreme is kept by
/// `compare_bindings`. The scan column is projected at resolve time; the fold
/// materializes through `object_map`, which carries the column reference
/// internally. Admitted only for numeric/temporal plain-literal columns
/// (`minmax_admissible_datatype` + a `TermType::Literal`, no-lang gate).
struct MinMaxFold {
    object_map: ObjectMap,
    is_max: bool,
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
    /// Output-bounding: after the one-shot scan+fold, a GROUP BY rollup's finalized
    /// rows are emitted in bounded batches across `next_batch` calls rather than as one
    /// giant batch — so a high-cardinality rollup (crt_highcard's 259k groups) never
    /// materializes its whole output at once. Each chunk pops from the BACK (group
    /// order is unspecified — a wrapping Sort applies any ORDER BY), so the drained
    /// key + accumulators are freed incrementally, not held to the last row. `None`
    /// until the fold completes and there is more than one bounded chunk to emit; when
    /// the switch is off (or the result fits one chunk) the single-batch path is taken
    /// and this stays `None`.
    pending_rows: Option<Vec<(Vec<GKey>, Vec<Acc>)>>,
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
            pending_rows: None,
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

    /// P2a: resolve a variable's predicate to a single RefObjectMap object and its
    /// parent TriplesMap, or `None` (not a ref, missing/multi-valued predicate, or
    /// an unresolvable parent). This is the RefObjectMap twin of
    /// `scalar_column_for_var` — a GROUP BY key like `?c` bound as `edw:customer`
    /// (the referenced customer IRI) resolves here, not there. The group-key admission
    /// folds on the parent SUBJECT IRI (minted once per parent row at resolve, exactly
    /// as `build_parent_lookup` does on the generic path), so the fused key is
    /// byte-identical to the materialized `?c` binding.
    fn ref_object_map_for_var<'m>(
        pattern: &R2rmlPattern,
        tm: &'m TriplesMap,
        var: VarId,
        mapping: &'m CompiledR2rmlMapping,
    ) -> Option<(&'m RefObjectMap, &'m TriplesMap)> {
        let pred = Self::predicate_for_var(pattern, var)?;
        let mut poms = tm
            .predicate_object_maps
            .iter()
            .filter(|pom| pom.predicate_map.as_constant() == Some(pred));
        let (Some(pom), None) = (poms.next(), poms.next()) else {
            return None; // missing or multi-valued predicate
        };
        let ObjectMap::RefObjectMap(rom) = &pom.object_map else {
            return None; // a Column / Template / Constant object, not a ref
        };
        let parent = mapping.triples_maps.get(&rom.parent_triples_map)?;
        Some((rom, parent))
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

    /// Build the per-row FILTER evaluation plan for `expr` over ONE pattern /
    /// TriplesMap: resolve every referenced variable to its scalar-column object
    /// map (projecting the column so the scan carries it), prepare the boolean
    /// expression, and build the term encoder from this TriplesMap's datatype
    /// annotations. Returns `None` — i.e. DECLINE the fuse — when any referenced
    /// variable does not resolve to a scalar column on this pattern: a
    /// `RefObjectMap` FK object, a template/constant object, a multi-valued
    /// predicate, or a var bound on a different pattern (`object_map_for_var`
    /// already encodes each of these declines).
    ///
    /// This is the exact construction the single-table `resolve_at_open` used
    /// inline, extracted so the fused JOIN path (FAMILY-C) reuses it VERBATIM. A
    /// fused filter is therefore byte-parity with the materialized `FilterOperator`
    /// by construction: both evaluate the SAME `PreparedBoolExpression` through
    /// `eval_to_bool_non_strict` (`next_batch` for the fact fold,
    /// `row_passes_filter_plan` for a dim fold, `filter.rs` for the materialized
    /// operator), and a demotable expression error yields `false` (row excluded)
    /// in all three.
    fn build_filter_plan(
        pattern: &R2rmlPattern,
        tm: &TriplesMap,
        expr: &Expression,
        ctx: &ExecutionContext<'_>,
        projection: &mut Vec<String>,
    ) -> Option<FilterPlan> {
        let eval_vars = expr.referenced_vars();
        let mut eval_objmaps = Vec::with_capacity(eval_vars.len());
        for v in &eval_vars {
            let om = Self::object_map_for_var(pattern, tm, *v)?;
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

    /// Output-bounding: emit the next bounded chunk of `pending_rows`, finalizing up to
    /// `OUTPUT_BOUND_ROWS` groups per call. Rows are POPPED from the back, so each
    /// group's key + accumulators are freed as it is finalized (peak output-side memory
    /// = one chunk, not the whole result). Group order is unspecified (a wrapping Sort
    /// applies any ORDER BY), so back-to-front drain is sound. Sets `done` once drained.
    fn emit_pending_chunk(&mut self) -> Result<Option<Batch>> {
        let resolved = self
            .resolved
            .as_ref()
            .ok_or_else(|| QueryError::Internal("fused aggregate not resolved".to_string()))?;
        let gcols = &resolved.group_cols;
        let folds = &resolved.folds;
        let num_cols = gcols.len() + folds.len();
        let rows = self
            .pending_rows
            .as_mut()
            .ok_or_else(|| QueryError::Internal("no pending fused output".to_string()))?;
        let take = OUTPUT_BOUND_ROWS.min(rows.len());
        let mut out: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::with_capacity(take)).collect();
        for _ in 0..take {
            let (key, accs) = rows.pop().expect("take <= rows.len()");
            for (i, g) in gcols.iter().enumerate() {
                out[i].push(g.binding(&key[i]));
            }
            for (j, acc) in accs.into_iter().enumerate() {
                out[gcols.len() + j].push(acc.finalize());
            }
            // `key` + `accs` drop here — this group's memory is freed before the next.
        }
        if rows.is_empty() {
            self.pending_rows = None;
            self.done = true;
            self.state = OperatorState::Exhausted;
        }
        Ok(Some(Batch::new(Arc::clone(&self.schema), out)?))
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
        // Output-bounding: a prior call built the full rollup and emitted the first
        // bounded chunk; this call emits the next one (freeing its rows) until drained.
        if self.pending_rows.is_some() {
            return self.emit_pending_chunk();
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
        if count_shortcut_eligible(
            resolved.filter.is_some(),
            &resolved.group_cols,
            &resolved.fact_constraints,
            &resolved.folds,
        ) {
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
        // N1 vector fold (default): `dict` interns the composite group key to a dense
        // id; `group_accs[id]` holds that group's typed accumulators. A `HashTable`
        // (not `HashMap`) so BOTH probe and resize-rehash go through `gkeys_hash` —
        // the borrowed probe hash and the owned rehash hash are the SAME function, so
        // a table grow cannot re-bucket a key away from its probe (the split-group
        // bug a plain `HashMap` + `insert_hashed_nocheck` hits on resize). `groups`
        // is the OFF-path (`FLUREE_FUSED_VECTOR_FOLD=0`) byte-identical owned-key map.
        let vfold = vector_fold_enabled();
        let mut dict: hashbrown::HashTable<(Vec<GKey>, u32)> = hashbrown::HashTable::new();
        let mut group_accs: Vec<Vec<Acc>> = Vec::new();
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
            let groups_before = if vfold {
                group_accs.len()
            } else {
                groups.len()
            };
            let fold_cols: Vec<Option<&Column>> = folds
                .iter()
                .map(|f| match f {
                    // MinMax materializes via its object map (handled inline below),
                    // not a bare column read.
                    Fold::CountRows | Fold::NumericExpr { .. } | Fold::MinMax { .. } => None,
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
            // N1: reused per-batch scratch for the borrowed composite key (borrows
            // this batch's columns + the resolver's dim GKeys); cleared each row so
            // its allocation is paid once per batch, not once per row.
            let mut scratch: Vec<GKeyRef> = Vec::with_capacity(gcols.len());
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
                    // The single-table and FACT-side-join filter, evaluated through the
                    // SAME `row_passes_filter_plan` the dim side uses — one filter-eval
                    // path for all three, so a fact filter is byte-parity with the dim
                    // filter and the materialized operator. A NULL/absent filter-member
                    // column EXCLUDES the row (BGP row-drop). Today `validity_cols`
                    // already null-drops these member columns before the filter runs, so
                    // this null-exclusion is unreachable here; routing through the helper
                    // makes that INVARIANT fail-safe — if a future refactor ever eroded
                    // the validity coverage, a null filter member would still exclude the
                    // row (never counted as, e.g., "not Closed") rather than leak via an
                    // Unbound. (`materialize_object_from_batch` over a scalar-column
                    // ObjectMap — all `build_filter_plan` produces — never returns Err, so
                    // this is behavior-identical to the prior inline block on every
                    // reachable input.)
                    if !Self::row_passes_filter_plan(fp, &batch, row, ctx)? {
                        continue;
                    }
                }
                // P3 (crt_join_reorder class): probe the SEMI-JOIN membership set BEFORE
                // resolving the group key — a fact row whose root→branch FK does not
                // resolve to a branch chain satisfying the branch constraints DROPS, the
                // existence-semantics of the inner join. Byte-identical to the generic
                // chained join because `membership` was built keep-min-THEN-filter (the
                // discarded-dup soundness line). A null FK ⇒ no branch triple ⇒ drop; it
                // is also already null-dropped by `validity_cols` (the FK child columns),
                // so this probe never sees a null-FK row on the reachable path. Mirrors
                // the `group_resolver` FK probe's key stringification
                // (`get_join_key_from_batch`) so both sides encode the join value the
                // same way. #1583: rests on the FK being single-valued (see `SemiJoinSet`).
                if let Some(sj) = &resolved.semi_join {
                    let Some(fk) = get_join_key_from_batch(&sj.fact_fk_cols, &batch, row) else {
                        continue;
                    };
                    if !sj.membership.contains(&fk) {
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
                    if vfold {
                        // N1: assemble the composite key BORROWED into the reused
                        // scratch — no per-row `String` clone / `Vec<GKey>` alloc.
                        // A null in ANY position drops the row (same rule as
                        // `assemble_group_key`). Probe the dense dict by hash; clone
                        // to an owned key only when a NEW group is inserted.
                        scratch.clear();
                        let mut dropped = false;
                        for (pos, slot) in resolved.group_key_plan.iter().enumerate() {
                            let comp = match slot {
                                KeySource::Fact => gcols[pos].key_ref_at(key_cols[pos], row),
                                KeySource::Dim(s) => dim_gkeys
                                    .and_then(|g| g.get(*s))
                                    .and_then(GKeyRef::from_owned),
                            };
                            match comp {
                                Some(c) => scratch.push(c),
                                None => {
                                    dropped = true;
                                    break;
                                }
                            }
                        }
                        if dropped {
                            continue;
                        }
                        let hash = hash_key_refs(&scratch);
                        let id = match dict.entry(
                            hash,
                            |(k, _)| {
                                k.len() == scratch.len()
                                    && scratch.iter().zip(k).all(|(r, o)| r.eq_owned(o))
                            },
                            |(k, _)| gkeys_hash(k),
                        ) {
                            hashbrown::hash_table::Entry::Occupied(o) => o.get().1,
                            hashbrown::hash_table::Entry::Vacant(v) => {
                                let id = group_accs.len() as u32;
                                let owned: Vec<GKey> =
                                    scratch.iter().map(|r| r.to_owned_key()).collect();
                                v.insert((owned, id));
                                group_accs.push(folds.iter().map(Acc::for_fold).collect());
                                id
                            }
                        };
                        &mut group_accs[id as usize]
                    } else {
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
                    }
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
                        Fold::MinMax { index } => {
                            let mmf = &resolved.minmax_folds[*index];
                            // Materialize just the candidate object term (no subject,
                            // no BindingRow) exactly as the generic scan would, then
                            // keep the running extreme by `compare_bindings` — so the
                            // final `Binding` is byte-identical to `agg_min`/`agg_max`.
                            // A null/absent value materializes `None` and contributes
                            // nothing, matching those aggregates dropping `Unbound`.
                            if let (Some(encoder), Ok(Some(term))) = (
                                resolved.minmax_encoder.as_ref(),
                                materialize_object_from_batch(&mmf.object_map, &batch, row),
                            ) {
                                let cand = encoder.encode(&term);
                                if let Acc::MinMax { best } = &mut accs[i] {
                                    let replace = match &*best {
                                        Binding::Unbound => true,
                                        cur => minmax_should_replace(
                                            mmf.is_max,
                                            crate::sort::compare_bindings(&cand, cur),
                                        ),
                                    };
                                    if replace {
                                        *best = cand;
                                    }
                                }
                            }
                            true
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
            let grown = (if vfold {
                group_accs.len()
            } else {
                groups.len()
            }) - groups_before;
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

        // An implicit aggregate is exactly one row — no output-bounding needed.
        if gcols.is_empty() {
            let columns: Vec<Vec<Binding>> =
                implicit.into_iter().map(|a| vec![a.finalize()]).collect();
            self.done = true;
            self.state = OperatorState::Exhausted;
            return Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?));
        }

        // Collect the grouped rows into a drainable Vec (MOVE, no clone — GKeys and
        // accumulators are moved out of the dict/map). Group iteration order is
        // unspecified either way (a wrapping Sort applies any ORDER BY).
        let rows: Vec<(Vec<GKey>, Vec<Acc>)> = if vfold {
            dict.into_iter()
                .map(|(key, id)| (key, std::mem::take(&mut group_accs[id as usize])))
                .collect()
        } else {
            groups.into_iter().collect()
        };

        if !output_bound_enabled() || rows.len() <= OUTPUT_BOUND_ROWS {
            // Switch off, or the whole result fits one bounded chunk: emit a single
            // batch. Byte-identical to the pre-bounding path (same rows, dict/map
            // iteration order — no reordering, since nothing is popped).
            let num_cols = gcols.len() + folds.len();
            let mut out: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();
            for (key, accs) in rows {
                for (i, g) in gcols.iter().enumerate() {
                    out[i].push(g.binding(&key[i]));
                }
                for (j, acc) in accs.into_iter().enumerate() {
                    out[gcols.len() + j].push(acc.finalize());
                }
            }
            self.done = true;
            self.state = OperatorState::Exhausted;
            return Ok(Some(Batch::new(Arc::clone(&self.schema), out)?));
        }

        // Output-bounding: stash the full rollup and emit the first bounded chunk;
        // subsequent next_batch calls drain the rest (the top-of-method fast path).
        self.pending_rows = Some(rows);
        self.emit_pending_chunk()
    }

    fn close(&mut self) {
        self.fallback.close();
        self.resolved = None;
        self.pending_rows = None;
        self.state = OperatorState::Closed;
    }
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
        // F1 (F-AUD-8, the q038 ungrouped/direct-path filtered-COUNT class): the
        // rewrite splits a subject-star with no variable-object member into a
        // standalone class scan + standalone const-object constraint scans.
        // Recombine them into one class-scan-with-`star_constraints` pattern so the
        // single-table fold below applies each constraint per row (see
        // `combine_constrained_class_scan`). Computed before the match so its arm
        // can claim this multi-pattern shape BEFORE the join arm — that arm would
        // otherwise take it and decline (a subject-star is not an FK chain).
        let combined_constrained_class = Self::combine_constrained_class_scan(&rr.patterns);
        let pattern = match rr.patterns.as_slice() {
            [Pattern::R2rml(p)] => p.clone(),
            _ if combined_constrained_class.is_some() => {
                combined_constrained_class.expect("is_some checked in the guard")
            }
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

        // C5 slice-1.5 (item 9b, the q038 filtered-COUNT class): a folded
        // constant-object constraint (`star_constraints`, e.g. `?s edw:isCurrent
        // true`) on the single-table pattern is NO LONGER a blanket decline — the
        // fold now APPLIES it (resolved below, once the TriplesMap is known). Slice-1
        // declined here because the fold ignored the constraint and would OVER-COUNT;
        // that hazard is retired by resolving the constraint to a per-row scalar-column
        // check via the SAME `resolve_star_constraint_checks` / `row_satisfies_constraints`
        // machinery the join path (E2) already uses, so a constrained COUNT excludes
        // the non-matching rows exactly as the materialized answer does. A constraint
        // that does not resolve to a scalar column still declines (below), and the
        // COUNT(*) manifest shortcut stays declined for a constraint-bearing plan (it
        // checks `fact_constraints.is_empty()`), because `record_count` cannot see
        // per-row constraint matches.

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

        // Resolve the fact-side folded constant-object constraints to per-row
        // scalar-column checks. Declines (`None`) if any constraint's predicate is a
        // RefObjectMap object or is absent — those keep the materialize path (no
        // silent over-count). Empty for an unconstrained COUNT/SUM/AVG/MIN/MAX plan.
        let Some(fact_constraints) = Self::resolve_star_constraint_checks(&pattern, tm) else {
            return Ok(None);
        };

        let mut projection: Vec<String> = Vec::new();
        // Scan the constraint columns so the fold can enforce them per row.
        for c in &fact_constraints {
            projection.push(c.column().to_string());
        }

        // Resolve GROUP BY key columns (string / integer in slice 3). The output
        // key binding's datatype Sid is encoded from the snapshot so it matches
        // what the normal materialization path produces.
        let mut group_cols = Vec::with_capacity(self.group_by.len());
        // P2a: per GROUP BY position, whether the key reads inline from the fact
        // (`Fact`) or from the FK→IRI resolver (`Dim(0)`, for the one admitted
        // RefObjectMap key). All-`Fact` keeps the fold byte-identical to before.
        let mut group_key_plan: Vec<KeySource> = Vec::with_capacity(self.group_by.len());
        // P2a: the one RefObjectMap group key's (fact FK child cols, parent join
        // cols, parent TM IRI), captured in SPARQL order; the FK→IRI resolver is
        // built after the scan projection below. At most one (≥2 declines).
        let mut ref_group_key: Option<(Vec<String>, Vec<String>, String)> = None;
        for gv in &self.group_by {
            let Some((col, datatype)) = Self::scalar_column_for_var(&pattern, tm, *gv) else {
                // P2a (#1583 fan-out caveat): admit a RefObjectMap group key — the
                // `GROUP BY ?c` where `?c` is a referenced parent IRI (crt_highcard).
                // The generic path resolves `?c` through `build_parent_lookup`
                // (parent scan → parent-subject IRI, deterministic keep-min on a
                // duplicate parent key, drop on a dangling/NULL FK — NOT a fan-out,
                // since a non-crawl aggregate has `trust_fk_refs=false`). We fold on
                // that same IRI, minted once per parent row at resolver-build (O(dim),
                // the cost `build_parent_lookup` already pays) instead of once per
                // fact row, and emit it as an IRI term at output. WHEN true R2RML
                // RefObjectMap fan-out lands (issue #1583), this admission changes
                // group multiplicity and MUST be revisited in the same change — the
                // fused path and generic path must flip together.
                let Some((rom, parent_tm)) =
                    Self::ref_object_map_for_var(&pattern, tm, *gv, &mapping)
                else {
                    return Ok(None); // neither a scalar column nor a ref object
                };
                if ref_group_key.is_some() {
                    return Ok(None); // ≥2 ref group keys: follow-on scope
                }
                // The parent subject must be a pure IRI (the fold emits an IRI term);
                // a blank-node / literal parent subject declines to the generic path.
                if !parent_tm.subject_map.generates_iri() {
                    return Ok(None);
                }
                // MAJOR-1: canonical_join aligns child/parent columns deterministically,
                // so the resolver's parent-side index and the fact-side probe agree.
                let Some((parent_cols, child_cols)) = canonical_join(rom).ok() else {
                    return Ok(None);
                };
                // dt_sid is unused for an IRI key (the output is `Binding::iri`); a
                // valid placeholder Sid keeps the struct uniform.
                let Some(dt_sid) = ctx.active_snapshot.encode_iri(fluree_vocab::xsd::STRING) else {
                    return Ok(None);
                };
                for c in &child_cols {
                    projection.push(c.clone());
                }
                group_key_plan.push(KeySource::Dim(0));
                group_cols.push(GroupCol {
                    column: child_cols.first().cloned().unwrap_or_default(),
                    kind: GKind::Iri,
                    dt_sid,
                });
                ref_group_key = Some((child_cols, parent_cols, parent_tm.iri.clone()));
                continue;
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
            //      post-E2/F1 a constant-object flag is NO LONGER declined —
            //      `combine_constrained_class_scan` FUSES it onto the class scan as
            //      a `star_constraints` entry that is APPLIED per row
            //      (`resolve_star_constraint_checks` / `row_satisfies_constraints`),
            //      so the fold counts only satisfying rows; the default here widens
            //      admission soundly for flagged shapes too, not just un-flagged ones.
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
            group_key_plan.push(KeySource::Fact);
            group_cols.push(GroupCol {
                column: col,
                kind,
                dt_sid,
            });
        }

        // Resolve the aggregate output folds against the (single) scanned TM.
        let mut minmax_folds: Vec<MinMaxFold> = Vec::new();
        let (folds, expr_folds) =
            match self.resolve_agg_folds(&pattern, tm, &mut projection, &mut minmax_folds) {
                Some(x) => x,
                None => return Ok(None),
            };
        // MIN/MAX materializes candidate terms via this encoder (built once, from
        // the scanned TriplesMap's datatype annotations — the same datatype Sids the
        // generic scan uses).
        let minmax_encoder =
            (!minmax_folds.is_empty()).then(|| LiteralEncoder::build(tm, ctx.active_snapshot));

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
            match Self::scalar_column_for_var(&pattern, tm, v) {
                Some((col, _)) => {
                    validity_cols.push(col.clone());
                    count_non_null_cols.push(col);
                }
                None => {
                    // P2a: a RefObjectMap object var (e.g. the `?c` group key) has no
                    // scalar column. It is admitted ONLY when it is the RefObjectMap
                    // GROUP BY key — the one var for which the `group_resolver` below is
                    // built (a resolver exists ONLY inside `if let Some(..) =
                    // ref_group_key`, and `ref_group_key` is set ONLY by the GROUP BY
                    // loop above). The resolver's per-row parent probe drops a
                    // present-but-dangling FK, matching the generic inner join. A ref
                    // object var that is NOT the group key — any pattern object var, or
                    // an ungrouped `COUNT`/`SUM` over `?o :ref ?c` — has NO resolver, so
                    // admitting it on FK-non-null validity alone would COUNT a
                    // present-but-dangling FK the generic path drops (an over-count;
                    // every prior P2a fixture had a GROUP BY, which is why this slipped).
                    // Decline it to the generic path (never over-count). Declining here,
                    // before `count_shortcut_eligible` (reached only from `next_batch` on
                    // a resolved plan), also closes the secondary manifest `record_count`
                    // over-count for this shape.
                    if !self.group_by.contains(&v) {
                        return Ok(None);
                    }
                    // Its row-validity is "the FK child columns are non-null" — a NULL FK
                    // yields no ref triple, exactly as `materialize_pom_object` returns
                    // None on a null join key, so the fact row drops. (A present-but-
                    // dangling FK is dropped by the `group_resolver` probe.) Any other
                    // unresolvable object declines.
                    let Some((rom, _parent)) =
                        Self::ref_object_map_for_var(&pattern, tm, v, &mapping)
                    else {
                        return Ok(None);
                    };
                    for c in rom.child_columns() {
                        validity_cols.push(c.to_string());
                        count_non_null_cols.push(c.to_string());
                    }
                }
            }
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
        // `build_filter_plan` is shared with the fused JOIN path (FAMILY-C), so a
        // filtered join is byte-parity with this single-table fold.
        let filter = match &self.filter {
            Some(expr) => match Self::build_filter_plan(&pattern, tm, expr, ctx, &mut projection) {
                Some(fp) => Some(fp),
                None => return Ok(None), // a non-column filter var → fall back
            },
            None => None,
        };

        projection.sort();
        projection.dedup();

        // P2a: build the FK→IRI resolver for the one admitted RefObjectMap group key
        // by scanning the parent dimension ONCE — minting the parent-subject IRI per
        // row exactly as `build_parent_lookup` does, with deterministic keep-min on a
        // duplicate parent join key (matching the generic query path post-#1529), so
        // the fused group key is byte-identical to the materialized `?c`. Null parent
        // subject / null join key rows are skipped (they can never satisfy the join);
        // a fact-row probe miss then folds "dangling FK" and "null parent" into one
        // drop, exactly as the generic path does.
        let group_resolver = if let Some((fk_child_cols, parent_cols, parent_tm_iri)) =
            ref_group_key
        {
            let Some(parent_tm) = mapping.triples_maps.get(&parent_tm_iri) else {
                return Ok(None);
            };
            let Some(parent_table) = parent_tm.table_name().map(str::to_string) else {
                return Ok(None);
            };
            let table_provider = ctx.r2rml_table_provider.ok_or_else(|| {
                QueryError::InvalidQuery("R2RML table provider not configured".to_string())
            })?;
            let mut parent_proj = parent_cols.clone();
            if let Some(t) = parent_tm.subject_map.template.as_deref() {
                parent_proj.extend(extract_template_columns(t));
            }
            if let Some(c) = &parent_tm.subject_map.column {
                parent_proj.push(c.clone());
            }
            parent_proj.sort();
            parent_proj.dedup();
            let gs = &pattern.graph_source_id;
            let mut map: std::collections::HashMap<Vec<String>, Vec<GKey>> =
                std::collections::HashMap::new();
            let mut s = table_provider
                .scan_table(gs, &parent_table, &parent_proj, &[], None, as_of_t)
                .await?;
            while let Some(batch) = s.next().await {
                ctx.checkpoint()?;
                let batch = batch?;
                let map_before = map.len();
                for row in 0..batch.num_rows {
                    let Some(key) = get_join_key_from_batch(&parent_cols, &batch, row) else {
                        continue; // null join key → never matches
                    };
                    let iri =
                        match materialize_subject_from_batch(&parent_tm.subject_map, &batch, row) {
                            Ok(Some(RdfTerm::Iri(iri))) => iri,
                            _ => continue, // null / non-IRI subject → skip (blank node declined above)
                        };
                    // Deterministic keep-min on a duplicate parent join key: keep the
                    // lexicographically smaller IRI, byte-identical to
                    // `parent_key_insert_keep_min` on the generic path. The subject is a
                    // pure IRI here (blank node declined above), so this raw-string `<` is
                    // exactly the shared `subject_sort_key` comparator applied to
                    // `RdfTerm::Iri` — the third keep-min copy shares that ordering
                    // (id=3717339907).
                    match map.entry(key) {
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert(vec![GKey::Str(iri)]);
                        }
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            if let Some(GKey::Str(cur)) = e.get().first() {
                                if iri < *cur {
                                    e.insert(vec![GKey::Str(iri)]);
                                }
                            }
                        }
                    }
                }
                ctx.record_alloc((map.len() - map_before) * crate::context::GROUP_EST_BYTES);
            }
            Some(GroupKeyResolver {
                fact_fk_cols: fk_child_cols,
                map,
            })
        } else {
            None
        };

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
            group_resolver,
            semi_join: None,
            group_key_plan,
            fact_constraints,
            minmax_folds,
            minmax_encoder,
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
        minmax_folds: &mut Vec<MinMaxFold>,
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
                AggregateFn::Min(v) | AggregateFn::Max(v) => {
                    // MIN/MAX over a bare scalar column only. A desugared expression
                    // MIN/MAX (an agg_bind) is not folded — decline to the generic
                    // path (the exact term of an arithmetic result is out of scope).
                    if bind_lookup.contains_key(v) {
                        return None;
                    }
                    let is_max = matches!(func, AggregateFn::Max(_));
                    let (col, datatype) = Self::scalar_column_for_var(pattern, tm, *v)?;
                    // Scope gate: numeric / date / timestamp only. String (collation),
                    // boolean, and un-annotated (→ xsd:string) columns decline.
                    if !minmax_admissible_datatype(datatype.as_deref()) {
                        return None;
                    }
                    // Plain-literal term only: a language-tagged or IRI-/blank-typed
                    // object map materializes a term whose lang/term-type the fold's
                    // compare would order differently than the generic path intends —
                    // decline (decline-only, never wrong).
                    let object_map = Self::object_map_for_var(pattern, tm, *v)?;
                    if !matches!(
                        &object_map,
                        ObjectMap::Column {
                            language: None,
                            term_type: TermType::Literal,
                            ..
                        }
                    ) {
                        return None;
                    }
                    projection.push(col);
                    let index = minmax_folds.len();
                    minmax_folds.push(MinMaxFold { object_map, is_max });
                    folds.push(Fold::MinMax { index });
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

    /// P3 kill switch (`FLUREE_FUSED_R2RML_MULTIFACT`, default **on**). Off restores
    /// the pre-P3 behavior wholesale: a branching multi-fact shape simply declines to
    /// the generic pipeline. Read once per process.
    fn multifact_enabled() -> bool {
        // Production: read once (process-wide), like the sibling fused switches.
        #[cfg(not(test))]
        {
            static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
            *ENABLED
                .get_or_init(|| crate::r2rml::env_switch_enabled("FLUREE_FUSED_R2RML_MULTIFACT"))
        }
        // Test: read the env each call so the switch-off decline path is testable
        // without a process-wide `OnceLock` caching the first observed value. Only the
        // gate test toggles this var (under a lock); the resolver tests call
        // `resolve_branching_star_at_open` directly and never read it.
        #[cfg(test)]
        {
            crate::r2rml::env_switch_enabled("FLUREE_FUSED_R2RML_MULTIFACT")
        }
    }

    /// P3 (scaffolding): decompose a BRANCHING-STAR join — a fact ROOT with ≥2 FK
    /// branches, each a linear sub-chain — into its root + classified branches, or
    /// `None` when the shape is not the admitted class. This is the structural half
    /// of P3 (the soundness core — the semi-join keep-min-then-filter build + the
    /// fold probe — is a tracked follow-on); the caller currently DECLINES even a
    /// recognized star, so behavior is byte-identical.
    ///
    /// Classification: a branch is GROUP-KEY if any of its patterns binds a GROUP BY
    /// var (its terminal dim carries the group attribute); otherwise SEMI-JOIN (a
    /// pure membership/constraint branch, e.g. `order → customer[segment=Enterprise]`).
    /// v1 admits EXACTLY one of each (the crt_join_reorder class); the branch LIST is
    /// the K-branch-ready carrier. Declines (`None`) on: <3 patterns, a non-unique
    /// root (disconnected), a non-branching root (out-degree <2 — that is a linear
    /// chain, [`Self::order_chain`]'s job), a merge (a pattern referenced by >1), a
    /// nested branch inside a branch, a cycle, a disconnected pattern, or a
    /// branch-set that is not {one GROUP-KEY, one SEMI-JOIN}.
    fn decompose_branching_star<'p>(
        pats: &[&'p R2rmlPattern],
        group_by: &[VarId],
    ) -> Option<BranchingStar<'p>> {
        let n = pats.len();
        if n < 3 {
            return None; // a branching star needs a root + ≥2 branch heads
        }
        // Directed FK edges (child i → parent j). A pattern may have MULTIPLE
        // out-edges (the branch point); each parent is referenced at most once.
        let mut out: Vec<Vec<(VarId, usize)>> = vec![Vec::new(); n];
        let mut indeg = vec![0usize; n];
        for i in 0..n {
            for j in 0..n {
                if i == j {
                    continue;
                }
                if let Some(jv) = Self::joins_via(pats[i], pats[j]) {
                    out[i].push((jv, j));
                    indeg[j] += 1;
                    if indeg[j] > 1 {
                        return None; // merge: a pattern referenced by >1 parent
                    }
                }
            }
        }
        // Exactly one root (in-degree 0); more than one ⇒ disconnected.
        let mut root_idx = None;
        for (i, &d) in indeg.iter().enumerate() {
            if d == 0 {
                if root_idx.is_some() {
                    return None;
                }
                root_idx = Some(i);
            }
        }
        let root_idx = root_idx?;
        // The root must BRANCH (≥2 FK targets); out-degree 1 is a linear chain and
        // belongs to order_chain, not here.
        if out[root_idx].len() < 2 {
            return None;
        }
        // Walk each root edge into a LINEAR sub-chain; a branch interior with >1
        // out-edge is a nested branch (v1 declines). Every non-root pattern must be
        // visited exactly once (connected, no cycle, branches disjoint).
        let mut visited = vec![false; n];
        visited[root_idx] = true;
        let mut branches: Vec<JoinBranch<'p>> = Vec::with_capacity(out[root_idx].len());
        let root_edges = out[root_idx].clone();
        for (root_jv, head) in root_edges {
            let mut chain: Vec<&'p R2rmlPattern> = Vec::new();
            let mut join_vars: Vec<VarId> = Vec::new();
            let mut cur = head;
            loop {
                if visited[cur] {
                    return None; // cycle, or a node shared between branches
                }
                visited[cur] = true;
                chain.push(pats[cur]);
                match out[cur].as_slice() {
                    [] => break,
                    [(jv, nxt)] => {
                        join_vars.push(*jv);
                        cur = *nxt;
                    }
                    _ => return None, // nested branch within a branch (v1 declines)
                }
            }
            let is_group_key = group_by.iter().any(|gv| {
                chain
                    .iter()
                    .any(|p| Self::predicate_for_var(p, *gv).is_some())
            });
            let branch = Branch {
                root_join_var: root_jv,
                chain,
                join_vars,
            };
            branches.push(if is_group_key {
                JoinBranch::GroupKey(branch)
            } else {
                JoinBranch::SemiJoin(branch)
            });
        }
        if !visited.iter().all(|&v| v) {
            return None; // a pattern is disconnected from the root
        }
        // v1 admission: exactly one GROUP-KEY branch + one SEMI-JOIN branch.
        let group_keys = branches
            .iter()
            .filter(|b| matches!(b, JoinBranch::GroupKey(_)))
            .count();
        let semi_joins = branches
            .iter()
            .filter(|b| matches!(b, JoinBranch::SemiJoin(_)))
            .count();
        if branches.len() != 2 || group_keys != 1 || semi_joins != 1 {
            return None;
        }
        Some(BranchingStar {
            root: pats[root_idx],
            branches,
        })
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

    /// P3: gather ALL of a semi-join chain pattern's folded constant-object
    /// constraints — from `star_constraints` AND from a STANDALONE const-object
    /// member (`predicate_filter` + `object_constant`). The distinction is
    /// load-bearing: the rewrite folds a constant object into `star_constraints` only
    /// when the subject ALSO has a var-object member (the group-key branch's terminal,
    /// e.g. `?p category ?cat ; isCurrent true`); a subject with NO var-object member —
    /// the semi-join terminal `?c segment "Enterprise"` — "stays a standalone scan"
    /// (`rewrite.rs`), so its constraint is `predicate_filter`/`object_constant`, NOT
    /// `star_constraints`. Reading only `star_constraints` there would leave the
    /// membership UNFILTERED and admit every row (a silent SUM over-count). Declines
    /// (`None`) if the standalone constraint's predicate is not a scalar-column
    /// `PredicateObjectMap` — never silently ignored.
    fn resolve_semijoin_pattern_constraints(
        pattern: &R2rmlPattern,
        tm: &TriplesMap,
    ) -> Option<Vec<ResolvedConstraint>> {
        let mut checks = Self::resolve_star_constraint_checks(pattern, tm)?;
        if let (Some(pred), Some(constant)) = (
            pattern.predicate_filter.as_deref(),
            pattern.object_constant.as_ref(),
        ) {
            let pom = tm
                .predicate_object_maps
                .iter()
                .find(|p| p.predicate_map.as_constant() == Some(pred))?;
            if !matches!(pom.object_map, ObjectMap::Column { .. }) {
                return None; // RefObjectMap / template constraint: cannot enforce as a scalar
            }
            checks.push(ResolvedConstraint {
                pom: pom.clone(),
                constant: constant.clone(),
                canon: decimal_canonical_of(constant),
            });
        }
        Some(checks)
    }

    /// F1 (audit F-AUD-8, the q038 ungrouped/direct-path filtered-COUNT class — the
    /// 886×-PARTIAL residual): when a subject-star carries NO variable-object
    /// member, the R2RML rewrite does NOT fold its class + constant-object members
    /// into one scan — it emits a standalone `?s a Class` class scan plus one
    /// standalone `?s pred const` scan per constraint (`rewrite.rs`, the
    /// `var_members.is_empty()` branch, whose only non-standalone route is a
    /// co-located crawl wildcard). That multi-pattern result misses the
    /// single-`[R2rml]` fused gate in `resolve_at_open`, so a bare-but-CONSTRAINED
    /// ungrouped COUNT (`SELECT (COUNT(*)) WHERE { ?s a edw:Customer ; edw:isCurrent
    /// true }`, q038) materializes at ~7k rows/s, while its GROUPED sibling (q022,
    /// which has a var-object `?seg` the rewrite folds the class + `isCurrent`
    /// constraint onto) fuses. This recombines that exact split back into ONE
    /// class-scan-carrying-`star_constraints` pattern — byte-identical to what the
    /// rewrite already produces for the var-object case — so the single-table fold
    /// applies each constraint per row via the SAME 9b machinery
    /// (`resolve_star_constraint_checks` + `row_satisfies_constraints`).
    ///
    /// Soundness (the D-c5 over-count line — the one unacceptable outcome): the
    /// recombination only re-associates patterns the rewrite split; it introduces
    /// no new semantics. It admits ONLY the exact shape — one shared subject VAR,
    /// exactly one pure class scan, every other pattern a scalar const-object
    /// equality with nothing else attached — and returns `None` for anything else
    /// (a bound subject, a var-object member, a second class, a cross-subject FK
    /// join, a RefObjectMap/template object carried on a member, a non-R2RML
    /// pattern), leaving the existing join/decline arms to handle it. Downstream,
    /// `resolve_star_constraint_checks` still DECLINES (→ materialize) any folded
    /// constraint it cannot enforce as a scalar-column check, and the manifest
    /// COUNT shortcut stays declined for the resulting non-empty `fact_constraints`
    /// (`count_shortcut_eligible`), so no constraint is ever silently dropped or
    /// over-counted. This adds NO new constraint source — the const-object members
    /// route through the same `star_constraints` field the var-object star already
    /// fills. Rides `FLUREE_FUSED_R2RML_AGG`: with the fold off,
    /// `detect_fused_r2rml_aggregate` returns `None`, the fused operator is never
    /// built, and q038 reverts to the materialized path (this widening is off).
    fn combine_constrained_class_scan(patterns: &[Pattern]) -> Option<R2rmlPattern> {
        // A lone class scan is already the single-`[R2rml]` arm; a single const
        // scan has no class to resolve its TriplesMap against. The shape is a class
        // + at least one constraint, i.e. ≥ 2 patterns.
        if patterns.len() < 2 {
            return None;
        }
        let mut r2rml: Vec<&R2rmlPattern> = Vec::with_capacity(patterns.len());
        for p in patterns {
            match p {
                Pattern::R2rml(rp) => r2rml.push(rp),
                _ => return None, // a non-R2RML pattern present → not this shape
            }
        }
        // One shared subject VARIABLE (an absent/bound subject → decline: this is a
        // subject-star, not a bound-subject crawl or a cross-subject FK join).
        let subject = r2rml[0].subject_var?;
        if r2rml
            .iter()
            .any(|rp| rp.subject_var != Some(subject) || rp.subject_constant.is_some())
        {
            return None;
        }
        let mut class_base: Option<R2rmlPattern> = None;
        let mut constraints: Vec<(String, ObjectConstant)> = Vec::new();
        for rp in &r2rml {
            // A pure class scan: a class filter and NOTHING else attached.
            let is_pure_class = rp.class_filter.is_some()
                && rp.predicate_filter.is_none()
                && rp.object_var.is_none()
                && rp.object_constant.is_none()
                && rp.predicate_var.is_none()
                && rp.type_var.is_none()
                && rp.triples_map_iri.is_none()
                && rp.class_prune_hint.is_none()
                && rp.star_bindings.is_empty()
                && rp.star_constraints.is_empty();
            // A scalar const-object equality: predicate + object constant only (the
            // standalone shape the rewrite emits for a const member with no
            // var-object base to fold onto).
            let is_const_object = rp.predicate_filter.is_some()
                && rp.object_constant.is_some()
                && rp.class_filter.is_none()
                && rp.object_var.is_none()
                && rp.predicate_var.is_none()
                && rp.type_var.is_none()
                && rp.triples_map_iri.is_none()
                && rp.class_prune_hint.is_none()
                && rp.star_bindings.is_empty()
                && rp.star_constraints.is_empty();
            if is_pure_class {
                if class_base.is_some() {
                    return None; // two class scans → ambiguous base → decline
                }
                class_base = Some((*rp).clone());
            } else if is_const_object {
                constraints.push((rp.predicate_filter.clone()?, rp.object_constant.clone()?));
            } else {
                return None; // a var-object / mixed / RefObjectMap member → not this shape
            }
        }
        let mut base = class_base?; // must be exactly one pure class scan
        if constraints.is_empty() {
            return None; // no constraint to fold (a lone class is the single arm)
        }
        base.star_constraints = constraints;
        Some(base)
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

    /// FAMILY-C: the single chain pattern that owns EVERY variable a FILTER
    /// references — the pattern whose scan phase can evaluate the residual — or
    /// `None` to DECLINE the fuse. Mirrors [`Self::route_group_key_sources`]: for
    /// each filter var, exactly one chain pattern must bind it as a scalar object.
    /// - 0 matches → the var is not a scalar object on the chain (e.g. it is a
    ///   subject, or bound only outside the join) → decline;
    /// - ≥2 matches → the var is an object in two patterns, a cross-source
    ///   value-equality the single-scan fold cannot enforce → decline;
    /// - the filter references vars owned by ≥2 DIFFERENT patterns → the residual
    ///   spans more than one scan phase (fact fold vs a dim map-build) and cannot
    ///   be applied in one pass without materializing the join → decline.
    ///
    /// The caller further restricts the owner to the FACT (index 0) or the
    /// TERMINAL dim (last index); an interior-dim owner declines (symmetric with
    /// the interior-dim group-key decline in `route_group_key_sources`). A var
    /// that routes here but is a `RefObjectMap`/template object still declines
    /// downstream in `build_filter_plan` (`object_map_for_var`).
    fn route_filter_source(chain: &[&R2rmlPattern], filter_vars: &[VarId]) -> Option<usize> {
        let mut owner: Option<usize> = None;
        for v in filter_vars {
            let mut src: Option<usize> = None;
            for (i, p) in chain.iter().enumerate() {
                if Self::predicate_for_var(p, *v).is_some() {
                    if src.is_some() {
                        return None; // var bound as an object on ≥2 patterns
                    }
                    src = Some(i);
                }
            }
            let src = src?; // var bound nowhere on the chain
            match owner {
                None => owner = Some(src),
                Some(o) if o == src => {}
                Some(_) => return None, // filter spans ≥2 patterns
            }
        }
        owner
    }

    /// FAMILY-C: evaluate a resolved [`FilterPlan`] against one row of a scanned
    /// batch, exactly as `next_batch` evaluates `Resolved.filter` over the fact
    /// batch — so a DIM-side filter (applied during the FK→GKey map build) is
    /// byte-parity with the fact-side filter, the single-table filter, and the
    /// materialized `FilterOperator`.
    ///
    /// A referenced column that is NULL/absent materializes no term: the R2RML
    /// star emits no triple for it, so the BGP member is unbound and the row
    /// DROPS (`Ok(false)`) — the same row-drop the single-table path enforces via
    /// `validity_cols` before its filter runs, applied here inline BECAUSE the dim
    /// scan does not otherwise null-check a non-group-key filter column. This
    /// preserves error semantics for `!BOUND`/`COALESCE`-style filters (a naive
    /// "null → Unbound → let the boolean demote" would wrongly KEEP a
    /// `FILTER(!BOUND(?x))` row the materialized BGP drops). Non-null rows evaluate
    /// the prepared expression non-strict: a demotable error ⇒ `false` ⇒ excluded,
    /// identical to the materialized operator and `passes_filters`.
    fn row_passes_filter_plan(
        fp: &FilterPlan,
        batch: &ColumnBatch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<bool> {
        let mut binds: Vec<Binding> = Vec::with_capacity(fp.eval_objmaps.len());
        for om in &fp.eval_objmaps {
            match materialize_object_from_batch(om, batch, row)? {
                Some(term) => binds.push(fp.encoder.encode(&term)),
                None => return Ok(false), // null filter-member column ⇒ BGP row-drop
            }
        }
        let rv = BindingRow::new(&fp.eval_vars, &binds);
        fp.prepared.eval_to_bool_non_strict(&rv, Some(ctx))
    }

    async fn resolve_join_at_open(
        &self,
        ctx: &ExecutionContext<'_>,
        pats: &[&R2rmlPattern],
        mapping: &CompiledR2rmlMapping,
    ) -> Result<Option<Resolved>> {
        // FAMILY-C: a row-level FILTER over the join is NO LONGER a blanket decline.
        // It is routed (below, once the chain + TriplesMaps are known) to the single
        // pattern that owns every filter var and applied in that pattern's scan
        // phase — a FACT-side filter in the fact fold (`next_batch`, via
        // `Resolved.filter`), a TERMINAL-dim filter during the FK→GKey map build.
        // Anything the port cannot resolve soundly still declines (`Ok(None)` →
        // materialize); see `route_filter_source` + `build_filter_plan` and the
        // decline enumeration at the routing site. (HAVING is still applied by a
        // wrapping operator, not here.) This widening rides
        // `FLUREE_FUSED_R2RML_AGG_JOIN` — with the join sub-switch off, this method
        // is never reached and a filtered join reverts to materialize.
        //
        // E2 (slice-1.5): a folded constant-object constraint (star_constraints,
        // e.g. a dim `?prod ex:isCurrent true` or a fact-side flag) is no longer a
        // blanket decline — the fold APPLIES it (dim-side while building the FK→GKey
        // map, fact-side in the value fold), resolved below once the TriplesMaps are
        // known. A constraint that does not resolve to a scalar column still
        // declines (in `resolve_star_constraint_checks`).
        // Order the patterns into a linear `fact → dim1 → … → dimk` chain (single
        // ref-join per hop, no branch, no cycle). `join_vars[h]` is dim_{h+1}'s
        // subject var — the object bound by the hop-`h` RefObjectMap.
        let (chain, join_vars) = match Self::order_chain(pats) {
            Some(c) => c,
            None => {
                // P3: a non-linear shape may be the branching-star multi-fact join
                // (crt_join_reorder class). Gated by the P3 switch, the branching
                // resolver builds the SEMI-JOIN membership + the GROUP-KEY linear
                // resolution and FUSES; it declines (`Ok(None)` → materialize) on any
                // shape outside the admitted class. With the switch OFF this is the
                // pre-P3 decline (byte-identical), so a branching shape reverts to the
                // generic pipeline wholesale.
                if Self::multifact_enabled() {
                    if let Some(star) = Self::decompose_branching_star(pats, &self.group_by) {
                        return self
                            .resolve_branching_star_at_open(ctx, mapping, star)
                            .await;
                    }
                }
                return Ok(None);
            }
        };
        self.resolve_linear_chain_at_open(ctx, mapping, chain, join_vars, &[], None)
            .await
    }

    /// Resolve a fused aggregate over an already-ordered linear `fact → dim1 → … →
    /// dimk` FK chain. Shared by the pure-linear join path
    /// ([`Self::resolve_join_at_open`]) and the P3 branching-star's GROUP-KEY branch
    /// ([`Self::resolve_branching_star_at_open`]).
    ///
    /// `strip_fact_fk_vars` are fact object vars that are FK roots of OTHER branches
    /// (the SEMI-JOIN branch) — they are covered by `semi_join` (a membership probe),
    /// NOT scalar/group objects, so they are excluded from the fact's required-object
    /// validity here (their FK child columns are still null-dropped + projected via
    /// `semi_join.fact_fk_cols`). `semi_join`, when present, is the resolved SEMI-JOIN
    /// membership set threaded into the returned `Resolved` for the fold probe. Both
    /// are empty/`None` on the pure-linear path, making it byte-identical to pre-P3.
    async fn resolve_linear_chain_at_open(
        &self,
        ctx: &ExecutionContext<'_>,
        mapping: &CompiledR2rmlMapping,
        chain: Vec<&R2rmlPattern>,
        join_vars: Vec<VarId>,
        strip_fact_fk_vars: &[VarId],
        semi_join: Option<SemiJoinSet>,
    ) -> Result<Option<Resolved>> {
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

        // FAMILY-C: route the row-level FILTER (if any) to the single participating
        // pattern that owns EVERY referenced variable, and build its FilterPlan
        // there. Two sound placements, one owner:
        //   - the FACT (chain index 0) → applied per fact row in `next_batch` via
        //     the returned `Resolved.filter` (the exact machinery the single-table
        //     path uses; the fact row-validity already null-drops the filter's
        //     member columns before the filter, BGP parity), OR
        //   - the TERMINAL dim (last index) → applied per dim row during the
        //     FK→GKey map build below (`row_passes_filter_plan`), so a fact row
        //     probing a filtered-out dim key drops.
        // DECLINES (Ok(None) → materialize) — each a soundness line, enumerated:
        //   (a) a var-free/constant filter (no per-row residual we model);
        //   (b) a filter var bound as an object on ≥2 chain patterns, or spanning
        //       ≥2 patterns (fact AND dim, or two dims) — `route_filter_source`;
        //   (c) an INTERIOR-dim owner (v1, symmetric with the interior-dim
        //       group-key decline);
        //   (d) a filter var that is a `RefObjectMap` FK / template / constant /
        //       multi-valued object — `build_filter_plan` → `object_map_for_var`;
        //   (e) EXISTS/NOT-EXISTS/subquery filters and any expression the
        //       single-table `PreparedBoolExpression` cannot evaluate never reach
        //       here soundly: `detect_fused_r2rml_aggregate` only captures a single
        //       `Pattern::Filter(expr)` (a bare FILTER expression; a
        //       FILTER EXISTS lowers to a sub-pattern, not a `Pattern::Filter`, so
        //       the GRAPH body carries a non-Triple/Filter pattern and detection
        //       returns None), and language-/IRI-typed comparisons demote exactly
        //       as they do on the single-table path (same evaluator, same encoder).
        let mut fact_filter: Option<FilterPlan> = None;
        let mut fact_filter_cols: Vec<String> = Vec::new();
        let mut terminal_dim_filter: Option<FilterPlan> = None;
        let mut terminal_filter_cols: Vec<String> = Vec::new();
        if let Some(expr) = &self.filter {
            let fvars = expr.referenced_vars();
            if fvars.is_empty() {
                return Ok(None); // (a)
            }
            let Some(src) = Self::route_filter_source(&chain, &fvars) else {
                return Ok(None); // (b)
            };
            if src == 0 {
                let Some(fp) =
                    Self::build_filter_plan(fact_p, fact_tm, expr, ctx, &mut fact_filter_cols)
                else {
                    return Ok(None); // (d) on the fact
                };
                fact_filter = Some(fp);
            } else if src == last_idx {
                let Some(fp) = Self::build_filter_plan(
                    terminal_p,
                    terminal_tm,
                    expr,
                    ctx,
                    &mut terminal_filter_cols,
                ) else {
                    return Ok(None); // (d) on the terminal dim
                };
                terminal_dim_filter = Some(fp);
            } else {
                return Ok(None); // (c) interior-dim filter
            }
        }

        // Aggregates fold from the FACT scan.
        let mut projection: Vec<String> = Vec::new();
        let mut minmax_folds: Vec<MinMaxFold> = Vec::new();
        let Some((folds, expr_folds)) =
            self.resolve_agg_folds(fact_p, fact_tm, &mut projection, &mut minmax_folds)
        else {
            return Ok(None);
        };
        // MIN/MAX candidate terms materialize from the FACT scan, so the encoder is
        // built from the fact TriplesMap (same datatype Sids the generic path uses).
        let minmax_encoder =
            (!minmax_folds.is_empty()).then(|| LiteralEncoder::build(fact_tm, ctx.active_snapshot));

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
            // Skip this chain's own FK to dim1 (a RefObjectMap object, covered by the
            // FK cols) and — P3 — any other branch's FK root (the SEMI-JOIN FK, covered
            // by `semi_join.fact_fk_cols` below). Both are FK objects, not scalars.
            if v == first_join_var || strip_fact_fk_vars.contains(&v) {
                continue;
            }
            let Some((col, _)) = Self::scalar_column_for_var(fact_p, fact_tm, v) else {
                return Ok(None);
            };
            validity_cols.push(col);
        }
        // P3: the SEMI-JOIN branch FK is a fact object (an FK to the branch root),
        // stripped from the scalar/group objects above. Its child columns must still
        // null-drop the fact row (a null FK ⇒ no branch triple ⇒ the inner join drops
        // the row) and be projected so `next_batch` can read them to probe the
        // membership set. Empty (no-op) on the pure-linear path.
        if let Some(sj) = &semi_join {
            validity_cols.extend(sj.fact_fk_cols.iter().cloned());
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
        // FAMILY-C: scan the fact-side FILTER columns so `next_batch` can evaluate
        // the residual per fact row. (A fact filter var is a fact object var, hence
        // already in validity_cols + projection; pushed explicitly for the same
        // reason as the group-key columns above. Empty unless the filter is
        // fact-owned.)
        for c in &fact_filter_cols {
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
        // FAMILY-C: scan the terminal dim's FILTER columns so `row_passes_filter_plan`
        // can evaluate the residual per dim row below. Empty unless the filter is
        // terminal-dim-owned.
        for c in &terminal_filter_cols {
            terminal_proj.push(c.clone());
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
                    // FAMILY-C: skip a terminal dim row that fails the routed
                    // dim-side FILTER (its attributes are functionally determined by
                    // the dim PK, so applying the residual here — before the join key
                    // enters the map — equals applying it post-join; a null filter
                    // member drops the row, BGP parity). No-op unless the filter is
                    // terminal-dim-owned.
                    if let Some(fp) = &terminal_dim_filter {
                        if !Self::row_passes_filter_plan(fp, &batch, row, ctx)? {
                            continue;
                        }
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
            // FAMILY-C: a FACT-side filter is applied per fact row in `next_batch`;
            // `None` when the filter was terminal-dim-owned (already applied during
            // the map build) or absent.
            filter: fact_filter,
            expr_folds,
            validity_cols,
            // The COUNT(*) manifest shortcut is single-table only.
            count_non_null_cols: Vec::new(),
            group_resolver: Some(GroupKeyResolver { fact_fk_cols, map }),
            semi_join,
            group_key_plan,
            fact_constraints: fact_checks,
            minmax_folds,
            minmax_encoder,
        }))
    }

    /// P3 (crt_join_reorder class): resolve a fused aggregate over a decomposed
    /// BRANCHING-STAR join — a fact root with exactly one GROUP-KEY branch and one
    /// SEMI-JOIN branch (`decompose_branching_star`). Builds the SEMI-JOIN membership
    /// (keep-min-then-filter, `build_semi_join_membership`), then resolves the
    /// GROUP-KEY branch as a linear chain `[root, group_branch.chain…]` via the shared
    /// [`Self::resolve_linear_chain_at_open`], with the SEMI-JOIN root FK stripped from
    /// the fact's required objects and the membership set threaded in for the fold
    /// probe. Declines (`Ok(None)` → materialize) on any sub-shape the linear resolver
    /// or the membership build cannot handle soundly.
    async fn resolve_branching_star_at_open(
        &self,
        ctx: &ExecutionContext<'_>,
        mapping: &CompiledR2rmlMapping,
        star: BranchingStar<'_>,
    ) -> Result<Option<Resolved>> {
        let root = star.root;
        // decompose_branching_star's admission guarantees exactly one of each; find
        // both defensively (decline if either is absent).
        let mut group_branch: Option<&Branch> = None;
        let mut semi_branch: Option<&Branch> = None;
        for b in &star.branches {
            match b {
                JoinBranch::GroupKey(br) => group_branch = Some(br),
                JoinBranch::SemiJoin(br) => semi_branch = Some(br),
            }
        }
        let (Some(group_branch), Some(semi_branch)) = (group_branch, semi_branch) else {
            return Ok(None);
        };
        let Some(root_tm) = Self::resolve_triples_map(root, mapping) else {
            return Ok(None);
        };
        // Build the SEMI-JOIN membership set first (keep-min-then-filter). A shape it
        // cannot resolve soundly declines the whole fuse.
        let Some(semi_join) = self
            .build_semi_join_membership(ctx, mapping, root, root_tm, semi_branch)
            .await?
        else {
            return Ok(None);
        };
        // The GROUP-KEY branch resolves as the linear chain [root, group_branch.chain…]
        // (root → group_branch.head via group_branch.root_join_var, then the branch's
        // own within-chain joins). The SEMI-JOIN root FK var is stripped from the
        // fact's required scalar objects — it is covered by the membership probe.
        let mut chain: Vec<&R2rmlPattern> = Vec::with_capacity(group_branch.chain.len() + 1);
        chain.push(root);
        chain.extend(group_branch.chain.iter().copied());
        let mut join_vars: Vec<VarId> = Vec::with_capacity(group_branch.join_vars.len() + 1);
        join_vars.push(group_branch.root_join_var);
        join_vars.extend(group_branch.join_vars.iter().copied());
        let strip = [semi_branch.root_join_var];
        self.resolve_linear_chain_at_open(ctx, mapping, chain, join_vars, &strip, Some(semi_join))
            .await
    }

    /// P3 SEMI-JOIN membership build (crt_join_reorder class) — KEEP-MIN-THEN-FILTER,
    /// the load-bearing soundness invariant.
    ///
    /// The branch is a linear FK sub-chain `root ─root_join_var→ chain[0] ─…→
    /// chain[m-1]`, its terminal carrying the membership constraint (e.g.
    /// `order → customer[segment="Enterprise"]`). Returns the set of ROOT-FK join keys
    /// whose chain — resolved the way the generic pipeline resolves it — satisfies
    /// every constraint. The generic pipeline resolves each FK to the keep-min parent
    /// SUBJECT (`build_parent_lookup` via `parent_key_insert_keep_min`) and then filters
    /// THAT one row, so on a duplicate intermediate key it tests the keep-min row, not
    /// "any duplicate". Building the set the other way — union every key some duplicate
    /// row admits (filter-then-union) — would WRONGLY admit a key whose keep-min row
    /// fails but a discarded duplicate passes. So each level is built keep-min-then-
    /// filter and composed terminal→root: for a key, keep the row with the smallest
    /// parent subject, then admit the key iff THAT row passes its own constraints AND
    /// (interior) its next-hop FK is in the next level's admitted set.
    ///
    /// Bounded: each level's keep-min map is charged via `record_alloc` and the scan
    /// loop `checkpoint`s per batch, so an oversized branch aborts typed
    /// (`MemoryBudgetExceeded`, 507) before OOM — the same fail-loud shape
    /// `build_parent_lookup` uses. Declines (`Ok(None)` → materialize) on any hop that
    /// is not a single-column `RefObjectMap`, a missing parent TM/table, or a
    /// constraint not reducible to a scalar column.
    ///
    /// #1583: the membership *set* models an inner join only while each FK is
    /// single-valued (one fact row → at most one branch row per hop). A fan-out
    /// `RefObjectMap` must revisit this (see [`SemiJoinSet`]). Relatedly, the keep-min
    /// build DECLINES a same-key/same-subject SCD-2 collision whose rows disagree on the
    /// constraint result or next-hop FK (id=3717339904) — keep-min alone would let scan
    /// order decide the answer.
    async fn build_semi_join_membership(
        &self,
        ctx: &ExecutionContext<'_>,
        mapping: &CompiledR2rmlMapping,
        root: &R2rmlPattern,
        root_tm: &TriplesMap,
        branch: &Branch<'_>,
    ) -> Result<Option<SemiJoinSet>> {
        use std::collections::hash_map::Entry;
        let table_provider = ctx.r2rml_table_provider.ok_or_else(|| {
            QueryError::InvalidQuery("R2RML table provider not configured".to_string())
        })?;
        let as_of_t = if ctx.dataset.is_some() {
            None
        } else {
            Some(ctx.to_t)
        };
        let gs = &root.graph_source_id;

        let chain = &branch.chain;
        let m = chain.len();
        if m == 0 {
            return Ok(None); // a branch with no dim is not a semi-join (defensive)
        }
        // A chain pattern's VAR-OBJECT members (`star_bindings`) OTHER than its next-hop
        // FK impose a BGP existence requirement (the object column must be non-null for
        // the triple to match) — and, across two branches sharing the object var, a
        // cross-branch equality — NEITHER of which this keep-min membership build honors:
        // it projects only join / subject / constraint / next-FK columns and never
        // consults `star_bindings`. Silently dropping such a member OVER-ADMITS the root
        // FK (e.g. `?c :segment "Enterprise" ; :region ?r` with a null region admits a
        // customer the generic BGP excludes). The ONLY star_binding a chain pattern may
        // carry is the FK to its next hop — a branch join var, consumed by the hop
        // resolution below. Any other var-object member declines to the generic path
        // (never over-admit); honoring it is a follow-on (project the column + a per-
        // column non-null drop + a cross-branch shared-var equality pass).
        if chain.iter().any(|p| {
            p.star_bindings
                .iter()
                .any(|(_, v)| !branch.join_vars.contains(v))
        }) {
            return Ok(None);
        }
        // Per-hop FK resolution. hop `h` connects source `h` (index 0 = root, then
        // chain[0..m-1]) to chain[h] via join var (root_join_var, then branch.join_vars):
        // (child cols on the source, parent cols on chain[h], chain[h]'s TM). Single-
        // column FK per hop (the 6b constraint). `chain_tms[h]` is chain[h]'s TM.
        let mut hop_join_vars: Vec<VarId> = Vec::with_capacity(m);
        hop_join_vars.push(branch.root_join_var);
        hop_join_vars.extend(branch.join_vars.iter().copied());
        if hop_join_vars.len() != m {
            return Ok(None); // malformed branch (decompose guarantees this shape)
        }
        let mut src_pats: Vec<&R2rmlPattern> = Vec::with_capacity(m);
        src_pats.push(root);
        src_pats.extend(chain.iter().take(m - 1).copied());
        let mut hops: Vec<(Vec<String>, Vec<String>)> = Vec::with_capacity(m);
        let mut chain_tms: Vec<&TriplesMap> = Vec::with_capacity(m);
        let mut src_tm = root_tm;
        for h in 0..m {
            let Some(join_pred) = Self::predicate_for_var(src_pats[h], hop_join_vars[h]) else {
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
            if rom.join_conditions.len() != 1 {
                return Ok(None); // single-column FK per hop (6b)
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
            ));
            chain_tms.push(parent_tm);
            src_tm = parent_tm;
        }
        // Each chain pattern's folded constant-object constraints, resolved to scalar-
        // column checks against ITS TM (a constraint that is not a scalar column
        // declines the fuse — never silently dropped, so no over-admission).
        let mut chain_checks: Vec<Vec<ResolvedConstraint>> = Vec::with_capacity(m);
        for h in 0..m {
            // Gather star_constraints AND the standalone const-object form: the
            // semi-join terminal's constraint (`?c segment "Enterprise"`) is emitted
            // standalone by the rewrite, so reading only star_constraints would admit
            // every row (silent over-count). Declines if a constraint can't resolve to
            // a scalar column.
            let Some(checks) = Self::resolve_semijoin_pattern_constraints(chain[h], chain_tms[h])
            else {
                return Ok(None);
            };
            chain_checks.push(checks);
        }

        // Build the admitted key set per level, terminal (m-1) → root (0). `admitted`
        // after level `i` = the set of hop[i].parent-key values whose keep-min chain
        // from chain[i] onward satisfies every constraint.
        let mut admitted: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
        for level in (0..m).rev() {
            let tm = chain_tms[level];
            let Some(table) = tm.table_name().map(str::to_string) else {
                return Ok(None);
            };
            let is_terminal = level == m - 1;
            // This level's own join key (the columns hop `level` probes it by).
            let parent_cols = hops[level].1.clone();
            // chain[level]'s FK to the NEXT hop (interior levels only).
            let next_fk_cols: Vec<String> = if is_terminal {
                Vec::new()
            } else {
                hops[level + 1].0.clone()
            };
            let checks = &chain_checks[level];

            let mut proj = parent_cols.clone();
            if let Some(t) = tm.subject_map.template.as_deref() {
                proj.extend(extract_template_columns(t));
            }
            if let Some(c) = &tm.subject_map.column {
                proj.push(c.clone());
            }
            for c in checks {
                proj.push(c.column().to_string());
            }
            proj.extend(next_fk_cols.iter().cloned());
            proj.sort();
            proj.dedup();

            // key → keep-min(parent subject) row bookkeeping.
            let mut kept: std::collections::HashMap<Vec<String>, KeptChainRow> =
                std::collections::HashMap::new();
            let mut s = table_provider
                .scan_table(gs, &table, &proj, &[], None, as_of_t)
                .await?;
            while let Some(batch) = s.next().await {
                // Bound the keep-min build: charge each new key + checkpoint per batch,
                // so an oversized branch aborts typed before OOM (build_parent_lookup
                // parity).
                ctx.checkpoint()?;
                let batch = batch?;
                let kept_before = kept.len();
                for row in 0..batch.num_rows {
                    let Some(key) = get_join_key_from_batch(&parent_cols, &batch, row) else {
                        continue; // null join key → never matched (skip, as build_parent_lookup does)
                    };
                    let subject = match materialize_subject_from_batch(&tm.subject_map, &batch, row)
                    {
                        Ok(Some(t)) => t,
                        _ => continue, // null / non-materializable subject → skip
                    };
                    let passes_own = Self::row_satisfies_constraints(checks, &batch, row)?;
                    let next_fk = if is_terminal {
                        None
                    } else {
                        get_join_key_from_batch(&next_fk_cols, &batch, row)
                    };
                    // Deterministic keep-min on the parent SUBJECT, byte-identical to
                    // `parent_key_insert_keep_min`: the lexicographically smaller subject
                    // wins; an equal subject is a benign duplicate (no replace). The
                    // WINNING row's `passes_own` + `next_fk` are what decide membership.
                    match kept.entry(key) {
                        Entry::Vacant(v) => {
                            v.insert(KeptChainRow {
                                subject,
                                passes_own,
                                next_fk,
                            });
                        }
                        Entry::Occupied(mut e) => {
                            // id=3717339904 (SCD-2 same-key/same-subject gap): two dim
                            // rows sharing the join key that mint the SAME subject but
                            // disagree on `passes_own` (the constraint result) or
                            // `next_fk` (the next hop) are the normal SCD-2 case (multiple
                            // versions per key, same subject template, differing on
                            // IS_CURRENT / a versioned FK). keep-min discriminates only by
                            // subject, so it would keep whichever row the scan hit first —
                            // a scan-order-dependent ANSWER (the generic chained join
                            // materializes every version's triples; this fold tests one).
                            // Decline to the generic path. An IDENTICAL duplicate (same
                            // subject, same constraint result, same next FK) is benign.
                            if subject == e.get().subject {
                                if passes_own != e.get().passes_own || next_fk != e.get().next_fk
                                {
                                    return Ok(None);
                                }
                            } else if subject_sort_key(&subject)
                                < subject_sort_key(&e.get().subject)
                            {
                                e.insert(KeptChainRow {
                                    subject,
                                    passes_own,
                                    next_fk,
                                });
                            }
                        }
                    }
                }
                ctx.record_alloc((kept.len() - kept_before) * crate::context::GROUP_EST_BYTES);
            }

            // Reduce the kept (keep-min) rows to this level's admitted set.
            let prev_admitted = std::mem::take(&mut admitted);
            let mut level_admitted: std::collections::HashSet<Vec<String>> =
                std::collections::HashSet::new();
            for (key, k) in kept {
                if !k.passes_own {
                    continue; // kept row fails its own constraint → key excluded
                }
                let ok = if is_terminal {
                    true
                } else {
                    match &k.next_fk {
                        Some(fk) => prev_admitted.contains(fk),
                        None => false, // null next-hop FK → chain breaks → excluded
                    }
                };
                if ok {
                    level_admitted.insert(key);
                }
            }
            admitted = level_admitted;
        }

        Ok(Some(SemiJoinSet {
            // The fact probes with the root→branch first-hop CHILD columns; the level-0
            // build keyed by the matching PARENT columns, same stringified join value.
            fact_fk_cols: hops[0].0.clone(),
            membership: admitted,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::grouping::AggregateSpec;

    /// C5 slice-1.5 (item 9b, the q038 class): a single-table COUNT carrying a
    /// folded constant-object constraint (`star_constraints`, e.g.
    /// `?s edw:isCurrent true`) is NO LONGER a blanket decline — the fold resolves
    /// the constraint to a scalar-column check and APPLIES it per row (the same
    /// `resolve_star_constraint_checks` / `row_satisfies_constraints` machinery the
    /// join path uses), so the constrained count excludes the non-matching rows
    /// exactly as the materialized answer does. The over-count hazard the slice-1
    /// decline guarded is now closed by APPLICATION (this test) + the fused-vs-native
    /// corpus parity (q022/q038/q061 + a multi-constraint member) rather than by
    /// declining. A constraint the fold cannot enforce (a RefObjectMap object) still
    /// declines — see `e2_resolve_star_constraint_checks_scalar_vs_ref`.
    #[test]
    fn slice_1_5_admits_and_applies_a_single_table_flag_constraint() {
        use crate::r2rml::{ObjectConstant, ScanValue};
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
        let flag = "http://ex/isCurrent";
        let tm =
            TriplesMap::new("#Cust", "DIM_CUSTOMER").with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(flag),
                object_map: ObjectMap::column("IS_CURRENT"),
            });
        // The q038 shape: a scalar-column `isCurrent true` flag folded onto the star.
        let mut constrained = R2rmlPattern::new("gs:main", VarId(1), None);
        constrained.star_constraints = vec![(
            flag.to_string(),
            ObjectConstant::Scalar(ScanValue::Bool(true)),
        )];
        // Slice-1.5: it now RESOLVES (admits) with exactly one per-row check applied
        // in the fold — no longer a decline, no longer a silent over-count.
        let checks = FusedR2rmlAggregateOperator::resolve_star_constraint_checks(&constrained, &tm);
        assert_eq!(
            checks.map(|v| v.len()),
            Some(1),
            "the q038 `isCurrent true` flag resolves to one applied scalar check"
        );
        // A non-empty `fact_constraints` set keeps the COUNT(*) manifest shortcut
        // declined in `next_batch` (it requires `fact_constraints.is_empty()`), so a
        // constrained count is never answered from the delete-blind `record_count`.
        let plain = R2rmlPattern::new("gs:main", VarId(1), None);
        assert_eq!(
            FusedR2rmlAggregateOperator::resolve_star_constraint_checks(&plain, &tm)
                .map(|v| v.len()),
            Some(0),
            "an unconstrained star resolves to zero checks (shortcut stays eligible)"
        );
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

    fn gc_str() -> GroupCol {
        GroupCol {
            column: "K".to_string(),
            kind: GKind::String,
            dt_sid: Sid::new(1, "string"),
        }
    }
    fn gc_int() -> GroupCol {
        GroupCol {
            column: "K".to_string(),
            kind: GKind::Integer,
            dt_sid: Sid::new(1, "integer"),
        }
    }

    /// N1 value-identity guard: the BORROWED key read (`key_ref_at`) must agree
    /// with the owned read (`key_at`) for every Column kind, null, out-of-bounds,
    /// wrong-type, and the NUMBER(n,0) physical-Decimal integer coercion (scale 0 /
    /// scale>0 divisible / non-divisible / null). `None` must correspond EXACTLY to
    /// `GKey::Null` (the row-drop). If these agree, the vector fold reads identical
    /// keys and therefore groups identically to the owned-key fold.
    #[test]
    fn key_ref_at_matches_key_at() {
        let cases: Vec<(GroupCol, Column, usize)> = vec![
            (gc_str(), Column::String(vec![Some("A".into()), None]), 0),
            (gc_str(), Column::String(vec![Some("A".into()), None]), 1),
            (gc_str(), Column::String(vec![Some("A".into())]), 5),
            (gc_str(), Column::Int64(vec![Some(7)]), 0), // wrong physical type
            (gc_int(), Column::Int64(vec![Some(42), None]), 0),
            (gc_int(), Column::Int64(vec![Some(42), None]), 1),
            (gc_int(), Column::Int32(vec![Some(-3)]), 0),
            (
                gc_int(),
                Column::Decimal {
                    values: vec![Some(50)],
                    precision: 5,
                    scale: 0,
                },
                0,
            ),
            (
                gc_int(),
                Column::Decimal {
                    values: vec![Some(500)],
                    precision: 5,
                    scale: 2,
                },
                0,
            ), // divisible -> 5
            (
                gc_int(),
                Column::Decimal {
                    values: vec![Some(543)],
                    precision: 5,
                    scale: 2,
                },
                0,
            ), // non-divisible -> Null
            (
                gc_int(),
                Column::Decimal {
                    values: vec![None],
                    precision: 5,
                    scale: 2,
                },
                0,
            ),
        ];
        for (gc, col, row) in &cases {
            let owned = gc.key_at(Some(col), *row);
            let borrowed = gc
                .key_ref_at(Some(col), *row)
                .map(GKeyRef::to_owned_key)
                .unwrap_or(GKey::Null);
            assert_eq!(owned, borrowed, "kind {:?} row {}", gc.kind, row);
            if let Some(r) = gc.key_ref_at(Some(col), *row) {
                assert!(r.eq_owned(&owned), "eq_owned disagrees for {owned:?}");
            }
        }
        assert_eq!(gc_str().key_at(None, 0), GKey::Null);
        assert!(gc_str().key_ref_at(None, 0).is_none());
    }

    /// N1 hash-consistency invariant (the resize-safety guard): the borrowed probe
    /// hash MUST equal the owned rehash hash for the equal key. If this ever drifts,
    /// a `HashTable` grow re-buckets keys away from the probe and splits groups (the
    /// bug the live A/B caught before this test was hardened).
    #[test]
    fn key_hash_borrowed_matches_owned() {
        let keys: Vec<Vec<GKey>> = vec![
            vec![GKey::Str("Mobile".into())],
            vec![GKey::Str(String::new())],
            vec![GKey::Int(-42), GKey::Str("x".into())],
            vec![GKey::Str("web".into()), GKey::Int(9_000_000_000)],
        ];
        for k in &keys {
            let refs: Vec<GKeyRef> = k.iter().map(|g| GKeyRef::from_owned(g).unwrap()).collect();
            assert_eq!(
                hash_key_refs(&refs),
                gkeys_hash(k),
                "borrowed vs owned hash differ for {k:?}"
            );
        }
    }

    /// N1: the dense `HashTable` dict groups a row sequence into EXACTLY the same
    /// partition and per-group counts as the owned-key `HashMap<Vec<GKey>, _>`, using
    /// the SAME production probe/rehash functions. Uses ENOUGH distinct keys (2000+)
    /// to force multiple table grows, so a probe/rehash hash mismatch (the
    /// split-group bug) would surface here, not only in the live A/B.
    #[test]
    fn vector_fold_grouping_matches_owned_map() {
        // Build a long row stream: many distinct composite keys, each repeated a
        // varying number of times, interleaved so grows happen mid-stream.
        let mut rows: Vec<Vec<GKey>> = Vec::new();
        for i in 0..2500u32 {
            let reps = 1 + (i % 4);
            for _ in 0..reps {
                rows.push(vec![
                    GKey::Str(format!("chan{}", i % 37)),
                    GKey::Int(i as i128),
                ]);
            }
        }
        // A few pure-string and pure-int keys too.
        for i in 0..300u32 {
            rows.push(vec![GKey::Str(format!("s{}", i % 11))]);
        }

        let mut owned: std::collections::HashMap<Vec<GKey>, u64> = Default::default();
        for k in &rows {
            *owned.entry(k.clone()).or_insert(0) += 1;
        }

        let mut dict: hashbrown::HashTable<(Vec<GKey>, u32)> = hashbrown::HashTable::new();
        let mut counts: Vec<u64> = Vec::new();
        for k in &rows {
            let refs: Vec<GKeyRef> = k.iter().map(|g| GKeyRef::from_owned(g).unwrap()).collect();
            let hash = hash_key_refs(&refs);
            let id = match dict.entry(
                hash,
                |(kk, _)| kk.len() == refs.len() && refs.iter().zip(kk).all(|(r, o)| r.eq_owned(o)),
                |(kk, _)| gkeys_hash(kk),
            ) {
                hashbrown::hash_table::Entry::Occupied(o) => o.get().1,
                hashbrown::hash_table::Entry::Vacant(v) => {
                    let id = counts.len() as u32;
                    v.insert((k.clone(), id));
                    counts.push(0);
                    id
                }
            };
            counts[id as usize] += 1;
        }
        assert_eq!(dict.len(), owned.len(), "group count differs");
        for (key, id) in &dict {
            assert_eq!(counts[*id as usize], owned[key], "count for {key:?}");
        }
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

    /// Item 9b MULTI-constraint boundary (the D-c5 lesson): with TWO folded
    /// constant-object constraints a row is kept iff it satisfies BOTH (AND
    /// semantics), so a constrained COUNT excludes any row failing EITHER — no
    /// over-count. This is the shape of corpus member q077 (isCurrent true AND
    /// segment=Enterprise); the fused single-table path routes through this same
    /// `row_satisfies_constraints`, so the member's native oracle and this test pin
    /// the same guarantee at two levels.
    #[test]
    fn multi_constraint_requires_all_to_match() {
        use crate::r2rml::ScanValue;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};
        let flag = ObjectConstant::Scalar(ScanValue::Bool(true));
        let seg = ObjectConstant::Scalar(ScanValue::Str("Enterprise".to_string()));
        let checks = [
            ResolvedConstraint {
                canon: decimal_canonical_of(&flag),
                pom: PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/isCurrent"),
                    object_map: ObjectMap::column("IS_CURRENT"),
                },
                constant: flag,
            },
            ResolvedConstraint {
                canon: decimal_canonical_of(&seg),
                pom: PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/segment"),
                    object_map: ObjectMap::column("SEGMENT"),
                },
                constant: seg,
            },
        ];
        let schema = Arc::new(BatchSchema::new(vec![
            FieldInfo {
                name: "IS_CURRENT".to_string(),
                field_type: FieldType::Boolean,
                nullable: true,
                field_id: 1,
            },
            FieldInfo {
                name: "SEGMENT".to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 2,
            },
        ]));
        // rows: (both match), (flag matches, segment wrong), (flag wrong, segment
        // matches), (segment null).
        let batch = ColumnBatch::new(
            schema,
            vec![
                Column::Boolean(vec![Some(true), Some(true), Some(false), Some(true)]),
                Column::String(vec![
                    Some("Enterprise".to_string()),
                    Some("Consumer".to_string()),
                    Some("Enterprise".to_string()),
                    None,
                ]),
            ],
        )
        .unwrap();
        let ok = |row| {
            FusedR2rmlAggregateOperator::row_satisfies_constraints(&checks, &batch, row).unwrap()
        };
        assert!(ok(0), "both constraints satisfied → kept");
        assert!(!ok(1), "segment mismatch → dropped (no over-count)");
        assert!(!ok(2), "flag mismatch → dropped (no over-count)");
        assert!(!ok(3), "null segment → dropped (existence filter)");
    }

    /// FAMILY-C admission matrix: `route_filter_source` routes a FILTER to the
    /// single chain pattern that owns EVERY referenced variable, and declines
    /// (`None`) the shapes the port cannot resolve in one scan phase. Both P4
    /// production shapes route to the FACT (Q1 `?status`; Q2 `?onHand`/`?reorder`);
    /// a dim-attribute filter routes to the dim; a filter spanning fact AND dim,
    /// a variable bound as an object on two patterns, and an unbound variable all
    /// decline. (The FACT/TERMINAL-vs-interior restriction and the
    /// RefObjectMap/non-scalar decline are asserted separately, below.)
    #[test]
    fn family_c_route_filter_source_admits_and_declines() {
        let status = VarId(1);
        let cust = VarId(2);
        let segment = VarId(3);
        let onhand = VarId(4);
        let reorder = VarId(5);
        // fact(SupportTicket/InventorySnapshot merged for the fixture): fact-side
        // scalar members + the FK object var `?cust`.
        let mut fact = R2rmlPattern::new("gs", VarId(0), None);
        fact.star_bindings = vec![
            ("http://ex/status".to_string(), status),
            ("http://ex/onHandQty".to_string(), onhand),
            ("http://ex/reorderPoint".to_string(), reorder),
            ("http://ex/customer".to_string(), cust), // the RefObjectMap FK object
        ];
        // dim(Customer): `?cust` is its SUBJECT (not an object → not routed here);
        // `?segment` is its attribute.
        let mut dim = R2rmlPattern::new("gs", cust, None);
        dim.star_bindings = vec![("http://ex/segment".to_string(), segment)];
        let chain = vec![&fact, &dim];

        // Q1: a fact-side inequality var → the fact (index 0).
        assert_eq!(
            FusedR2rmlAggregateOperator::route_filter_source(&chain, &[status]),
            Some(0)
        );
        // Q2: a fact-side var-to-var pair → the fact (both owned there).
        assert_eq!(
            FusedR2rmlAggregateOperator::route_filter_source(&chain, &[onhand, reorder]),
            Some(0)
        );
        // A dim-attribute filter → the terminal dim (index 1).
        assert_eq!(
            FusedR2rmlAggregateOperator::route_filter_source(&chain, &[segment]),
            Some(1)
        );
        // Spans fact AND dim → declines (two scan phases, cannot apply in one pass).
        assert_eq!(
            FusedR2rmlAggregateOperator::route_filter_source(&chain, &[status, segment]),
            None
        );
        // An unbound variable → declines.
        assert_eq!(
            FusedR2rmlAggregateOperator::route_filter_source(&chain, &[VarId(99)]),
            None
        );

        // A variable bound as an OBJECT on two patterns (cross-source equality) →
        // declines (fresh fixture so the shared member is on both).
        let shared = VarId(7);
        let mut fact2 = R2rmlPattern::new("gs", VarId(0), None);
        fact2.star_bindings = vec![("http://ex/shared".to_string(), shared)];
        let mut dim2 = R2rmlPattern::new("gs", VarId(0), None);
        dim2.star_bindings = vec![("http://ex/shared".to_string(), shared)];
        let chain2 = vec![&fact2, &dim2];
        assert_eq!(
            FusedR2rmlAggregateOperator::route_filter_source(&chain2, &[shared]),
            None
        );
    }

    /// FAMILY-C: `build_filter_plan` (the shared single-table/join construction)
    /// ADMITS a filter over a scalar column — resolving its object map and
    /// projecting the column — and DECLINES (`None`) a filter over a `RefObjectMap`
    /// FK object var (the fold materializes no parent join), exactly as the
    /// single-table path did inline. The decline is the D-c5 line: an un-resolvable
    /// filter var must fall back to materialize, never be silently ignored.
    #[test]
    fn family_c_build_filter_plan_projects_scalar_declines_ref() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap};

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let tm = TriplesMap::new("#Ticket", "FACT_SUPPORT_TICKET")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/status"),
                object_map: ObjectMap::column("STATUS"),
            })
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/customer"),
                object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                    "#Cust", "CUST_KEY", "CUST_KEY",
                )),
            });
        let status = VarId(1);
        let cust = VarId(2);
        let mut pat = R2rmlPattern::new("gs", VarId(0), None);
        pat.star_bindings = vec![
            ("http://ex/status".to_string(), status),
            ("http://ex/customer".to_string(), cust),
        ];

        // Scalar-column filter var → admits, and its column is projected.
        let mut proj = Vec::new();
        let scalar_filter = Expression::ne(
            Expression::Var(status),
            Expression::Const(FlakeValue::String("Closed".to_string())),
        );
        let fp = FusedR2rmlAggregateOperator::build_filter_plan(
            &pat,
            &tm,
            &scalar_filter,
            &ctx,
            &mut proj,
        );
        assert!(fp.is_some(), "a scalar-column filter var admits");
        assert!(
            proj.contains(&"STATUS".to_string()),
            "the filter's column is projected into the scan"
        );

        // A filter over the RefObjectMap FK object var → declines.
        let mut proj_ref = Vec::new();
        let ref_filter = Expression::ne(
            Expression::Var(cust),
            Expression::Const(FlakeValue::String("x".to_string())),
        );
        assert!(
            FusedR2rmlAggregateOperator::build_filter_plan(
                &pat,
                &tm,
                &ref_filter,
                &ctx,
                &mut proj_ref
            )
            .is_none(),
            "a filter over a RefObjectMap FK object var declines (fold has no parent join)"
        );
    }

    /// FAMILY-C fact-side NULL defense (R-1528 hardening item 1): the FACT filter in
    /// `next_batch` routes through the SAME `row_passes_filter_plan` as the dim side,
    /// so a NULL fact filter-member column EXCLUDES the row (Q1's `?status != "Closed"`
    /// with a NULL status is NOT counted as "not Closed"). In production `validity_cols`
    /// already null-drops the member before the filter runs, making this unreachable —
    /// this test bypasses that path (calls the helper directly) to prove the defense is
    /// fail-safe should a future refactor ever erode the validity coverage. Exercises the
    /// exact fact filter arm with Q1's production STATUS shape.
    #[test]
    fn family_c_fact_filter_null_member_excludes_failsafe() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let status = VarId(1);
        let tm = TriplesMap::new("#Ticket", "FACT_SUPPORT_TICKET").with_predicate_object(
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/status"),
                object_map: ObjectMap::column("STATUS"),
            },
        );
        let mut pat = R2rmlPattern::new("gs", VarId(0), None);
        pat.star_bindings = vec![("http://ex/status".to_string(), status)];
        let mut proj = Vec::new();
        let ne_filter = Expression::ne(
            Expression::Var(status),
            Expression::Const(FlakeValue::String("Closed".to_string())),
        );
        let fp =
            FusedR2rmlAggregateOperator::build_filter_plan(&pat, &tm, &ne_filter, &ctx, &mut proj)
                .expect("scalar filter admits");
        let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
            name: "STATUS".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 1,
        }]));
        // rows: Open (!= Closed → kept), Closed (== Closed → dropped), NULL (excluded).
        let batch = ColumnBatch::new(
            schema,
            vec![Column::String(vec![
                Some("Open".to_string()),
                Some("Closed".to_string()),
                None,
            ])],
        )
        .unwrap();
        let pass = |row| {
            FusedR2rmlAggregateOperator::row_passes_filter_plan(&fp, &batch, row, &ctx).unwrap()
        };
        assert!(pass(0), "status=Open → `!=` true → kept");
        assert!(!pass(1), "status=Closed → `!=` false → dropped");
        assert!(
            !pass(2),
            "status=NULL → unbound BGP member → EXCLUDED fail-safe (NOT counted as 'not Closed')"
        );
    }

    /// FAMILY-C null/comparison semantics — THE D-c5 correctness crux. Evaluated
    /// through `row_passes_filter_plan`, the exact per-row eval the fused fold
    /// runs (dim-side directly; fact-side via the same `FilterPlan` in
    /// `next_batch`). Covers all three required shapes with NULL-bearing rows:
    ///
    /// - Q1 `?status != "Closed"` (a dim-side variant here, `?category`): a NULL
    ///   value is an unbound BGP member ⇒ the row is EXCLUDED — it does NOT count
    ///   as "not Closed"/"not Electronics". This is the one wrong-count outcome the
    ///   task calls unacceptable.
    /// - Q2 `?onHand < ?reorder`: var-to-var with a NULL on EACH side excludes the
    ///   row (either operand unbound ⇒ excluded); a genuine `<` decides the rest.
    ///
    /// The exclusion matches the materialized `FilterOperator` two ways at once:
    /// the null-member row-drop (BGP `validity` parity — here the `None`
    /// short-circuit) AND `eval_to_bool_non_strict` demoting a comparison error to
    /// `false`. Both agree; the row is excluded either way.
    #[test]
    fn family_c_row_passes_filter_plan_null_excludes_and_compares() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Q1 shape (dim-side variant): `?category != "Electronics"` over DIM_PRODUCT.
        let category = VarId(1);
        let tm_cat =
            TriplesMap::new("#Prod", "DIM_PRODUCT").with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/category"),
                object_map: ObjectMap::column("CATEGORY"),
            });
        let mut pat_cat = R2rmlPattern::new("gs", VarId(0), None);
        pat_cat.star_bindings = vec![("http://ex/category".to_string(), category)];
        let mut proj = Vec::new();
        let ne_filter = Expression::ne(
            Expression::Var(category),
            Expression::Const(FlakeValue::String("Electronics".to_string())),
        );
        let fp_cat = FusedR2rmlAggregateOperator::build_filter_plan(
            &pat_cat, &tm_cat, &ne_filter, &ctx, &mut proj,
        )
        .expect("scalar filter admits");
        let schema_cat = Arc::new(BatchSchema::new(vec![FieldInfo {
            name: "CATEGORY".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 1,
        }]));
        let batch_cat = ColumnBatch::new(
            schema_cat,
            vec![Column::String(vec![
                Some("Electronics".to_string()),
                Some("Toys".to_string()),
                None,
            ])],
        )
        .unwrap();
        let pass_cat = |row| {
            FusedR2rmlAggregateOperator::row_passes_filter_plan(&fp_cat, &batch_cat, row, &ctx)
                .unwrap()
        };
        assert!(!pass_cat(0), "category=Electronics → `!=` false → excluded");
        assert!(pass_cat(1), "category=Toys → `!=` true → kept");
        assert!(
            !pass_cat(2),
            "category=NULL → unbound BGP member → EXCLUDED (must NOT count as 'not Electronics')"
        );

        // Q2 shape: `?onHand < ?reorder`, both fact-side xsd:integer columns.
        let onhand = VarId(1);
        let reorder = VarId(2);
        let tm_inv = TriplesMap::new("#Snap", "FACT_INVENTORY_SNAPSHOT")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/onHandQty"),
                object_map: ObjectMap::column_typed("ON_HAND", fluree_vocab::xsd::INTEGER),
            })
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/reorderPoint"),
                object_map: ObjectMap::column_typed("REORDER", fluree_vocab::xsd::INTEGER),
            });
        let mut pat_inv = R2rmlPattern::new("gs", VarId(0), None);
        pat_inv.star_bindings = vec![
            ("http://ex/onHandQty".to_string(), onhand),
            ("http://ex/reorderPoint".to_string(), reorder),
        ];
        let mut proj_inv = Vec::new();
        let lt_filter = Expression::lt(Expression::Var(onhand), Expression::Var(reorder));
        let fp_inv = FusedR2rmlAggregateOperator::build_filter_plan(
            &pat_inv,
            &tm_inv,
            &lt_filter,
            &ctx,
            &mut proj_inv,
        )
        .expect("var-to-var filter admits");
        let schema_inv = Arc::new(BatchSchema::new(vec![
            FieldInfo {
                name: "ON_HAND".to_string(),
                field_type: FieldType::Int64,
                nullable: true,
                field_id: 1,
            },
            FieldInfo {
                name: "REORDER".to_string(),
                field_type: FieldType::Int64,
                nullable: true,
                field_id: 2,
            },
        ]));
        // rows: (5<10 keep), (10<5 drop), (NULL on-hand → drop), (NULL reorder → drop).
        let batch_inv = ColumnBatch::new(
            schema_inv,
            vec![
                Column::Int64(vec![Some(5), Some(10), None, Some(10)]),
                Column::Int64(vec![Some(10), Some(5), Some(10), None]),
            ],
        )
        .unwrap();
        let pass_inv = |row| {
            FusedR2rmlAggregateOperator::row_passes_filter_plan(&fp_inv, &batch_inv, row, &ctx)
                .unwrap()
        };
        assert!(pass_inv(0), "5 < 10 → kept");
        assert!(!pass_inv(1), "10 < 5 → excluded");
        assert!(
            !pass_inv(2),
            "NULL onHand → unbound operand → EXCLUDED (not 'less than')"
        );
        assert!(
            !pass_inv(3),
            "NULL reorder → unbound operand → EXCLUDED (not 'less than')"
        );
    }

    /// FAMILY-C multi-constraint + filter combined (the p3+filter shape): a
    /// terminal dim row must pass BOTH its folded constant-object constraint (a
    /// flag, `row_satisfies_constraints`) AND the routed dim-side FILTER
    /// (`row_passes_filter_plan`) to enter the FK→GKey map — the conjunction the
    /// terminal-dim scan loop applies. A row failing EITHER is dropped (no
    /// over-count); this pins the combined gate the loop enforces in sequence.
    #[test]
    fn family_c_constraint_and_filter_are_conjunctive() {
        use crate::context::ExecutionContext;
        use crate::r2rml::ScanValue;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Dim carrying BOTH an `isCurrent true` flag (constraint) and a `?segment`
        // attribute the filter constrains.
        let flag = "http://ex/isCurrent";
        let seg_pred = "http://ex/segment";
        let tm = TriplesMap::new("#Cust", "DIM_CUSTOMER")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(flag),
                object_map: ObjectMap::column("IS_CURRENT"),
            })
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(seg_pred),
                object_map: ObjectMap::column("SEGMENT"),
            });
        let segment = VarId(1);
        let mut pat = R2rmlPattern::new("gs", VarId(0), None);
        pat.star_bindings = vec![(seg_pred.to_string(), segment)];

        let flag_const = ObjectConstant::Scalar(ScanValue::Bool(true));
        let checks = [ResolvedConstraint {
            canon: decimal_canonical_of(&flag_const),
            pom: PredicateObjectMap {
                predicate_map: PredicateMap::constant(flag),
                object_map: ObjectMap::column("IS_CURRENT"),
            },
            constant: flag_const,
        }];
        let mut proj = Vec::new();
        let seg_filter = Expression::ne(
            Expression::Var(segment),
            Expression::Const(FlakeValue::String("SMB".to_string())),
        );
        let fp =
            FusedR2rmlAggregateOperator::build_filter_plan(&pat, &tm, &seg_filter, &ctx, &mut proj)
                .expect("scalar filter admits");

        let schema = Arc::new(BatchSchema::new(vec![
            FieldInfo {
                name: "IS_CURRENT".to_string(),
                field_type: FieldType::Boolean,
                nullable: true,
                field_id: 1,
            },
            FieldInfo {
                name: "SEGMENT".to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 2,
            },
        ]));
        // rows: (current + Enterprise → keep), (current + SMB → filter drops),
        // (not current + Enterprise → constraint drops).
        let batch = ColumnBatch::new(
            schema,
            vec![
                Column::Boolean(vec![Some(true), Some(true), Some(false)]),
                Column::String(vec![
                    Some("Enterprise".to_string()),
                    Some("SMB".to_string()),
                    Some("Enterprise".to_string()),
                ]),
            ],
        )
        .unwrap();
        // The loop keeps a row iff it satisfies the constraint AND the filter.
        let kept = |row| {
            FusedR2rmlAggregateOperator::row_satisfies_constraints(&checks, &batch, row).unwrap()
                && FusedR2rmlAggregateOperator::row_passes_filter_plan(&fp, &batch, row, &ctx)
                    .unwrap()
        };
        assert!(kept(0), "current AND segment≠SMB → kept");
        assert!(!kept(1), "current but segment=SMB → filter drops");
        assert!(!kept(2), "segment≠SMB but not current → constraint drops");
    }

    /// Item 9b decline invariant (D-c5): the bare-COUNT manifest shortcut fires ONLY
    /// for a plain COUNT — it DECLINES for a filtered, grouped, constant-object-
    /// constrained, or non-`CountRows` plan, because the manifest `record_count` sum
    /// cannot see per-row matches or per-group partitions. A false positive here is
    /// exactly the over-count R-1522 named as the unacceptable outcome; this pins the
    /// predicate directly (it was previously only inspection-verified).
    #[test]
    fn count_shortcut_declines_constraints_filter_group_and_non_count() {
        use crate::r2rml::ScanValue;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
        // Eligible: a single CountRows fold, no filter / group / constraints.
        assert!(count_shortcut_eligible(false, &[], &[], &[Fold::CountRows]));
        // Declines: a FILTER is present.
        assert!(!count_shortcut_eligible(true, &[], &[], &[Fold::CountRows]));
        // Declines: a GROUP BY key is present.
        let gcol = GroupCol {
            column: "SEG".into(),
            kind: GKind::String,
            dt_sid: Sid::new(2, "string"),
        };
        assert!(!count_shortcut_eligible(
            false,
            &[gcol],
            &[],
            &[Fold::CountRows]
        ));
        // Declines: a folded constant-object constraint is present (the q038/9b line).
        let constant = ObjectConstant::Scalar(ScanValue::Bool(true));
        let constraint = ResolvedConstraint {
            canon: decimal_canonical_of(&constant),
            pom: PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/isCurrent"),
                object_map: ObjectMap::column("IS_CURRENT"),
            },
            constant,
        };
        assert!(
            !count_shortcut_eligible(false, &[], &[constraint], &[Fold::CountRows]),
            "a constraint-bearing COUNT must decline the delete-blind shortcut"
        );
        // Declines: not a bare CountRows fold (COUNT(col) / SUM / MIN / two folds / none).
        assert!(!count_shortcut_eligible(
            false,
            &[],
            &[],
            &[Fold::CountColumn("C".into())]
        ));
        assert!(!count_shortcut_eligible(
            false,
            &[],
            &[],
            &[Fold::CountRows, Fold::CountRows]
        ));
        assert!(!count_shortcut_eligible(false, &[], &[], &[]));
    }

    /// F1 (F-AUD-8, the q038 ungrouped/direct-path filtered-COUNT class): the
    /// recombination admits ONLY a same-subject `class scan + scalar const-object
    /// constraint scans` split (what the rewrite emits for a subject-star with no
    /// var-object member) and folds the constraints onto the class scan as
    /// `star_constraints`. Every other multi-pattern shape declines (`None`), so the
    /// existing join/decline arms still own it and no over-count is possible.
    #[test]
    fn f1_combine_constrained_class_scan_admission() {
        use crate::r2rml::{ObjectConstant, ScanValue};
        const CUST: &str = "http://ns.fluree.dev/edw#Customer";
        const OTHER: &str = "http://ns.fluree.dev/edw#Order";
        const IS_CURRENT: &str = "http://ns.fluree.dev/edw#isCurrent";
        const SEGMENT: &str = "http://ns.fluree.dev/edw#segment";
        let s = VarId(0);
        let other_s = VarId(9);
        let bool_true = || ObjectConstant::Scalar(ScanValue::Bool(true));
        let seg_ent = || ObjectConstant::Scalar(ScanValue::Str("Enterprise".to_string()));
        let class_scan = |subj: VarId, class: &str| {
            let mut p = R2rmlPattern::new("gs:main", subj, None);
            p.class_filter = Some(class.to_string());
            Pattern::R2rml(p)
        };
        let const_scan = |subj: VarId, pred: &str, c: ObjectConstant| {
            let mut p = R2rmlPattern::new("gs:main", subj, None);
            p.predicate_filter = Some(pred.to_string());
            p.object_constant = Some(c);
            Pattern::R2rml(p)
        };
        let var_member = |subj: VarId, pred: &str, obj: VarId| {
            let mut p = R2rmlPattern::new("gs:main", subj, Some(obj));
            p.predicate_filter = Some(pred.to_string());
            Pattern::R2rml(p)
        };
        let combine = FusedR2rmlAggregateOperator::combine_constrained_class_scan;

        // q038 shape: class + one const-object constraint → folds (either order).
        let folded = combine(&[class_scan(s, CUST), const_scan(s, IS_CURRENT, bool_true())])
            .expect("class + 1 const folds");
        assert_eq!(folded.class_filter.as_deref(), Some(CUST));
        assert_eq!(folded.star_constraints.len(), 1);
        assert_eq!(folded.star_constraints[0].0, IS_CURRENT);
        assert!(combine(&[const_scan(s, IS_CURRENT, bool_true()), class_scan(s, CUST)]).is_some());

        // q078/q077 shape: class + TWO const-object constraints (multi-constraint AND).
        let multi = combine(&[
            class_scan(s, CUST),
            const_scan(s, IS_CURRENT, bool_true()),
            const_scan(s, SEGMENT, seg_ent()),
        ])
        .expect("class + 2 const folds");
        assert_eq!(multi.star_constraints.len(), 2);

        // Declines: a lone class (the single-`[R2rml]` arm owns it).
        assert!(combine(&[class_scan(s, CUST)]).is_none());
        // Declines: a single const scan (no class to resolve a TriplesMap against).
        assert!(combine(&[const_scan(s, IS_CURRENT, bool_true())]).is_none());
        // Declines: two const scans, no class base.
        assert!(combine(&[
            const_scan(s, IS_CURRENT, bool_true()),
            const_scan(s, SEGMENT, seg_ent()),
        ])
        .is_none());
        // Declines: a var-object member present (a real star, not this shape).
        assert!(combine(&[class_scan(s, CUST), var_member(s, SEGMENT, VarId(2))]).is_none());
        // Declines: a cross-subject FK-join shape (the join arm owns it).
        assert!(combine(&[
            class_scan(s, CUST),
            const_scan(other_s, IS_CURRENT, bool_true())
        ])
        .is_none());
        // Declines: two class scans → ambiguous base.
        assert!(combine(&[
            class_scan(s, CUST),
            class_scan(s, OTHER),
            const_scan(s, IS_CURRENT, bool_true()),
        ])
        .is_none());
        // Declines: a bound-subject member (subject_constant set, no subject var).
        let mut bound_class = R2rmlPattern::new("gs:main", s, None);
        bound_class.subject_var = None;
        bound_class.subject_constant = Some("http://ex/cust/1".to_string());
        bound_class.class_filter = Some(CUST.to_string());
        assert!(combine(&[
            Pattern::R2rml(bound_class),
            const_scan(s, IS_CURRENT, bool_true()),
        ])
        .is_none());
        // Declines: a non-R2RML pattern present.
        assert!(combine(&[class_scan(s, CUST), Pattern::Filter(Expression::Var(s))]).is_none());
    }

    /// F1 diagnosis + fix, end to end through the REAL rewrite: the direct-path
    /// ungrouped constrained COUNT `?s a Customer ; isCurrent true` (q038) rewrites
    /// to TWO separate R2rml patterns (a class scan + a standalone `isCurrent=true`
    /// scan) — the split that misses the single-`[R2rml]` fused gate and forces a
    /// materialize. Its GROUPED sibling q022 (which adds a var-object `?seg`)
    /// already rewrites to ONE fused pattern. `combine_constrained_class_scan`
    /// re-folds the q038 split into one class-scan-with-`star_constraints` pattern
    /// whose constraint the single-table 9b fold resolves — and the resulting
    /// non-empty `fact_constraints` keeps the delete-blind manifest COUNT shortcut
    /// declined. The multi-constraint direct-path variant (the new q078 member)
    /// folds two.
    #[test]
    fn f1_rewrite_splits_then_combine_refolds() {
        use crate::ir::triple::{Ref, Term, TriplePattern};
        use fluree_db_core::LedgerSnapshot;
        use fluree_db_r2rml::mapping::{PredicateMap, PredicateObjectMap};
        const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        const CUST: &str = "http://ns.fluree.dev/edw#Customer";
        const IS_CURRENT: &str = "http://ns.fluree.dev/edw#isCurrent";
        const SEGMENT: &str = "http://ns.fluree.dev/edw#segment";

        let customer = TriplesMap::new("#Customer", "DIM_CUSTOMER")
            .with_subject_template("http://ns.fluree.dev/edw/customer/{CUSTOMER_KEY}")
            .with_class(CUST)
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(IS_CURRENT),
                object_map: ObjectMap::column("IS_CURRENT"),
            })
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(SEGMENT),
                object_map: ObjectMap::column("SEGMENT"),
            });
        let mapping = CompiledR2rmlMapping::new(vec![customer]);
        let snapshot = LedgerSnapshot::genesis("test/main");
        let tm = mapping.triples_maps.values().next().unwrap();

        let s = VarId(0);
        let seg = VarId(1);
        let triple = |p: Ref, o: Term| Pattern::Triple(TriplePattern::new(Ref::Var(s), p, o));
        let type_tp = triple(Ref::Iri(Arc::from(TYPE)), Term::Iri(Arc::from(CUST)));
        let flag_tp = triple(
            Ref::Iri(Arc::from(IS_CURRENT)),
            Term::Value(FlakeValue::Boolean(true)),
        );
        let seg_var_tp = triple(Ref::Iri(Arc::from(SEGMENT)), Term::Var(seg));
        let seg_const_tp = triple(
            Ref::Iri(Arc::from(SEGMENT)),
            Term::Value(FlakeValue::String("Enterprise".to_string())),
        );
        let rewrite = |pats: &[Pattern]| {
            rewrite_patterns_for_r2rml(pats, "gs:main", &snapshot, Some(&mapping), false, false)
                .patterns
        };

        // q038: the direct-path ungrouped constrained COUNT splits into 2 patterns.
        let q038 = rewrite(&[type_tp.clone(), flag_tp.clone()]);
        assert_eq!(
            q038.len(),
            2,
            "q038 (no var-object member) rewrites to a split class + const scan — the fusion gap"
        );
        let folded = FusedR2rmlAggregateOperator::combine_constrained_class_scan(&q038)
            .expect("F1 recombines the q038 split");
        assert_eq!(folded.class_filter.as_deref(), Some(CUST));
        assert_eq!(folded.star_constraints.len(), 1);
        // The 9b fold resolves the folded constraint to one scalar-column check ...
        let checks = FusedR2rmlAggregateOperator::resolve_star_constraint_checks(&folded, tm)
            .expect("scalar column constraint resolves");
        assert_eq!(checks.len(), 1);
        // ... and a non-empty constraint set keeps the manifest COUNT shortcut declined.
        assert!(
            !count_shortcut_eligible(false, &[], &checks, &[Fold::CountRows]),
            "the constrained COUNT must not take the delete-blind manifest shortcut"
        );

        // q022 (grouped, adds var-object ?seg) already fuses in the rewrite: ONE
        // pattern carrying the class + the isCurrent star_constraint. combine is a
        // no-op for it (the single-`[R2rml]` arm owns it).
        let q022 = rewrite(&[type_tp.clone(), flag_tp.clone(), seg_var_tp.clone()]);
        assert_eq!(q022.len(), 1, "q022 fuses in the rewrite (var-object base)");
        assert!(FusedR2rmlAggregateOperator::combine_constrained_class_scan(&q022).is_none());

        // Multi-constraint direct path (the new q078 member): class + 2 const → 3
        // split patterns → combine refolds to one pattern with 2 constraints.
        let q078 = rewrite(&[type_tp.clone(), flag_tp.clone(), seg_const_tp.clone()]);
        assert_eq!(q078.len(), 3, "multi-constraint direct path splits into 3");
        let folded_multi = FusedR2rmlAggregateOperator::combine_constrained_class_scan(&q078)
            .expect("F1 recombines the multi-constraint split");
        assert_eq!(folded_multi.star_constraints.len(), 2);
        let checks_multi =
            FusedR2rmlAggregateOperator::resolve_star_constraint_checks(&folded_multi, tm)
                .expect("both scalar constraints resolve");
        assert_eq!(checks_multi.len(), 2);
    }

    /// Item 9 MIN/MAX admission scope: only numeric and date/timestamp datatypes
    /// fold; string (collation), boolean, custom, and un-annotated (→ xsd:string)
    /// columns decline to the generic pipeline. Parity of the fold itself comes from
    /// materializing the same term + `compare_bindings`; this gate only scopes the
    /// mechanism to the audit item's covered types (F-AUD-8).
    #[test]
    fn minmax_admissible_datatype_scope() {
        use fluree_vocab::xsd;
        for dt in [
            xsd::INTEGER,
            xsd::LONG,
            xsd::INT,
            xsd::DECIMAL,
            xsd::DOUBLE,
            xsd::FLOAT,
            xsd::DATE,
            xsd::DATE_TIME,
        ] {
            assert!(minmax_admissible_datatype(Some(dt)), "{dt} should fold");
        }
        for dt in [xsd::STRING, xsd::BOOLEAN, "http://ex/custom"] {
            assert!(
                !minmax_admissible_datatype(Some(dt)),
                "{dt} should decline (collation / non-orderable / out of scope)"
            );
        }
        assert!(
            !minmax_admissible_datatype(None),
            "an un-annotated column (→ xsd:string) declines"
        );
    }

    /// Item 9 MIN/MAX tie-break parity (R-1522): the fold must resolve an
    /// equal-BUT-differently-rendered extreme the SAME way the generic
    /// `agg_min`/`agg_max` do — MIN keeps the FIRST of the ties, MAX keeps the LAST
    /// (`min_by`/`max_by` semantics). The reachable concrete case is a double column
    /// carrying both `+0.0` and `-0.0`: they compare Equal but render "0" vs "-0",
    /// so picking the wrong one on a tie breaks byte-parity. (A single decimal column
    /// has a fixed scale, so it can't hold two renderings of one value — but the same
    /// `Equal`-by-value rule covers `1.50` vs `1.5` if such a pair ever reached the
    /// fold; the pure-logic assertions below pin that rule directly.)
    #[test]
    fn minmax_tie_break_matches_generic_agg() {
        use crate::sort::compare_bindings;
        use std::cmp::Ordering;
        // The replace predicate == min_by (first) / max_by (last) tie-breaking.
        assert!(
            !minmax_should_replace(false, Ordering::Equal),
            "MIN keeps the FIRST on a tie"
        );
        assert!(
            minmax_should_replace(true, Ordering::Equal),
            "MAX keeps the LAST on a tie"
        );
        assert!(minmax_should_replace(false, Ordering::Less));
        assert!(!minmax_should_replace(false, Ordering::Greater));
        assert!(minmax_should_replace(true, Ordering::Greater));
        assert!(!minmax_should_replace(true, Ordering::Less));

        // Concrete reachable case: ±0.0 compare Equal but render differently.
        let pos = Binding::lit(FlakeValue::Double(0.0), Sid::xsd_double());
        let neg = Binding::lit(FlakeValue::Double(-0.0), Sid::xsd_double());
        assert_eq!(
            compare_bindings(&pos, &neg),
            Ordering::Equal,
            "+0.0 and -0.0 compare Equal"
        );
        let bits = |b: &Binding| match b {
            Binding::Lit {
                val: FlakeValue::Double(d),
                ..
            } => d.to_bits(),
            _ => panic!("expected a double lit"),
        };
        // Fold over [+0.0, -0.0] in scan order: MAX must end on -0.0 (the LAST,
        // matching agg_max); MIN must end on +0.0 (the FIRST, matching agg_min).
        // Under the old first-on-ties MAX this would keep +0.0 and diverge.
        for (is_max, generic) in [
            (
                true,
                AggregateFn::Max(VarId(0)).apply(&Binding::Grouped(vec![pos.clone(), neg.clone()])),
            ),
            (
                false,
                AggregateFn::Min(VarId(0)).apply(&Binding::Grouped(vec![pos.clone(), neg.clone()])),
            ),
        ] {
            let mut best = pos.clone();
            if minmax_should_replace(is_max, compare_bindings(&neg, &best)) {
                best = neg.clone();
            }
            assert_eq!(
                bits(&best),
                bits(&generic),
                "fused {} tie-break must match the generic aggregate",
                if is_max { "MAX" } else { "MIN" }
            );
        }
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

    // --- P2a: RefObjectMap (IRI) group-key admission (crt_highcard shape) ---

    /// Run the fused single-table operator over `GROUP BY ?c { ?o <http://ex/custRef>
    /// ?c } COUNT(*)` against `mapping` + per-table `batches` (a mock provider serving
    /// BOTH the parent scan that builds the FK→IRI resolver AND the fact scan). Returns
    /// `(?c IRI, COUNT)` rows sorted by IRI; PANICS if the group key is not emitted as
    /// an IRI term or the count is not a Long — pinning P2a's byte-identity to the
    /// generic path (`RdfTerm::Iri => Binding::iri`, `Acc::Count => xsd:integer Long`).
    async fn run_ref_iri_group_by(
        mapping: Arc<CompiledR2rmlMapping>,
        batches: std::collections::HashMap<String, fluree_db_tabular::ColumnBatch>,
    ) -> Vec<(String, i64)> {
        use crate::r2rml::{ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter};
        use crate::seed::EmptyOperator;
        use crate::var_registry::VarRegistry;
        use async_trait::async_trait;
        use fluree_db_core::LedgerSnapshot;

        #[derive(Debug)]
        struct P {
            m: Arc<CompiledR2rmlMapping>,
            b: std::collections::HashMap<String, fluree_db_tabular::ColumnBatch>,
        }
        #[async_trait]
        impl R2rmlProvider for P {
            async fn has_r2rml_mapping(&self, _gs: &str) -> bool {
                true
            }
            async fn compiled_mapping(
                &self,
                _gs: &str,
                _t: Option<i64>,
            ) -> Result<Arc<CompiledR2rmlMapping>> {
                Ok(Arc::clone(&self.m))
            }
        }
        #[async_trait]
        impl R2rmlTableProvider for P {
            async fn scan_table(
                &self,
                _gs: &str,
                table: &str,
                _p: &[String],
                _f: &[ScanFilter],
                _tk: Option<&crate::r2rml::ScanTopK>,
                _t: Option<i64>,
            ) -> Result<ColumnBatchStream> {
                let b = self
                    .b
                    .get(table)
                    .cloned()
                    .unwrap_or_else(|| panic!("no batch for table {table}"));
                Ok(Box::pin(futures::stream::once(async move { Ok(b) })))
            }
        }

        let (o, c, cnt) = (VarId(0), VarId(1), VarId(20));
        // `?o <custRef> ?c` — the object var `?c` is bound by a RefObjectMap predicate,
        // so the rewrite keeps it as ONE R2rml pattern (single-table path).
        let mut fact = R2rmlPattern::new("gs", o, Some(c));
        fact.triples_map_iri = Some("#Order".to_string());
        fact.predicate_filter = Some("http://ex/custRef".to_string());
        let plan = FusedAggregatePlan {
            graph_iri: Arc::from("gs"),
            inner_patterns: vec![Pattern::R2rml(fact)], // passes through the rewrite as-is
            filter: None,
            agg_binds: vec![],
            group_by: vec![c],
            aggregates: vec![(cnt, AggregateFn::CountAll)],
        };
        let mut op = FusedR2rmlAggregateOperator::new(plan, Box::new(EmptyOperator::new()));
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let provider = P {
            m: mapping,
            b: batches,
        };
        let ctx =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&provider, &provider);
        op.open(&ctx).await.expect("open");
        // Drain ALL output batches: output-bounding emits a high-cardinality rollup in
        // bounded chunks across multiple next_batch calls, so a single-call consumer
        // would see only the first chunk. (This also exercises the multi-batch drain.)
        let mut out = Vec::new();
        let mut got_batch = false;
        while let Some(batch) = op.next_batch(&ctx).await.expect("next_batch") {
            got_batch = true;
            let n = batch.column(c).map(<[Binding]>::len).unwrap_or(0);
            for row in 0..n {
                let iri = match batch.get(row, c).expect("group-key binding") {
                    Binding::Iri(s) => s.to_string(),
                    other => panic!("a RefObjectMap group key MUST be an IRI term, got {other:?}"),
                };
                let count = match batch.get(row, cnt).expect("count binding") {
                    Binding::Lit {
                        val: FlakeValue::Long(n),
                        ..
                    } => *n,
                    other => panic!("COUNT must be an xsd:integer Long, got {other:?}"),
                };
                out.push((iri, count));
            }
        }
        assert!(
            got_batch,
            "the fused ref-IRI GROUP BY must produce output, not decline"
        );
        out.sort();
        out
    }

    /// A `#Order --custRef(RefObjectMap CFK->CID)--> #Customer` mapping. The customer
    /// subject template is caller-chosen so a test can key it on a column OTHER than
    /// the join column (the keep-min case).
    fn order_customer_mapping(customer_subject_template: &str) -> Arc<CompiledR2rmlMapping> {
        use fluree_db_r2rml::mapping::PredicateMap;
        Arc::new(CompiledR2rmlMapping::new(vec![
            TriplesMap::new("#Order", "order")
                .with_subject_template("http://ex/order/{OID}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/custRef"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Customer",
                        "CFK",
                        "CID",
                    )),
                }),
            TriplesMap::new("#Customer", "customer")
                .with_subject_template(customer_subject_template),
        ]))
    }

    fn i64_col(
        name: &str,
        id: i32,
        vals: Vec<Option<i64>>,
    ) -> (fluree_db_tabular::FieldInfo, Column) {
        (
            fluree_db_tabular::FieldInfo {
                name: name.to_string(),
                field_type: fluree_db_tabular::FieldType::Int64,
                nullable: true,
                field_id: id,
            },
            Column::Int64(vals),
        )
    }

    fn str_col(
        name: &str,
        id: i32,
        vals: Vec<Option<String>>,
    ) -> (fluree_db_tabular::FieldInfo, Column) {
        (
            fluree_db_tabular::FieldInfo {
                name: name.to_string(),
                field_type: fluree_db_tabular::FieldType::String,
                nullable: true,
                field_id: id,
            },
            Column::String(vals),
        )
    }

    fn batch_of(
        cols: Vec<(fluree_db_tabular::FieldInfo, Column)>,
    ) -> fluree_db_tabular::ColumnBatch {
        let (fields, columns): (Vec<_>, Vec<_>) = cols.into_iter().unzip();
        let schema = Arc::new(fluree_db_tabular::BatchSchema::new(fields));
        fluree_db_tabular::ColumnBatch::new(schema, columns).unwrap()
    }

    #[tokio::test]
    async fn ref_iri_group_key_folds_to_parent_iris_and_drops_dangling() {
        // GROUP BY the customer IRI. Orders 1,2 → cust/10; order 3 → cust/20; order 4
        // has a DANGLING FK (99, no customer) and must drop — exactly what
        // build_parent_lookup + materialize_pom_object do on the generic path.
        let mapping = order_customer_mapping("http://ex/cust/{CID}");
        let mut batches = std::collections::HashMap::new();
        batches.insert(
            "order".to_string(),
            batch_of(vec![
                i64_col("OID", 1, vec![Some(1), Some(2), Some(3), Some(4)]),
                i64_col("CFK", 2, vec![Some(10), Some(10), Some(20), Some(99)]),
            ]),
        );
        batches.insert(
            "customer".to_string(),
            batch_of(vec![i64_col("CID", 1, vec![Some(10), Some(20)])]),
        );
        let out = run_ref_iri_group_by(mapping, batches).await;
        assert_eq!(
            out,
            vec![
                ("http://ex/cust/10".to_string(), 2),
                ("http://ex/cust/20".to_string(), 1),
            ],
            "fused ref-IRI GROUP BY must equal the generic answer (dangling FK dropped)"
        );
    }

    /// B1 (review id=3717339897): an UNGROUPED aggregate (`group_by:[]`, COUNT(*))
    /// over a RefObjectMap object var (`?o <custRef> ?c`) must DECLINE the fuse. A
    /// `group_resolver` — whose per-row parent probe drops a present-but-dangling FK —
    /// is built ONLY for a RefObjectMap GROUP BY key (inside `if let Some(..) =
    /// ref_group_key`). Without a resolver the single-table fold admits the object var
    /// on FK-non-null validity alone, so a present-but-dangling FK passes and is folded
    /// in — an over-count vs the generic inner join (which drops it). Here orders carry
    /// CFK {10,10,20,99} against customers {10,20}: pre-fix `resolve_at_open` ADMITTED
    /// (would fold COUNT=4), the generic answer is 3. Every prior P2a fixture has a
    /// GROUP BY, which is exactly why this slipped. The fix declines → the generic path
    /// answers 3 (fused == generic by construction). Declining before
    /// `count_shortcut_eligible` (reached only from `next_batch` on a RESOLVED plan)
    /// also closes the secondary manifest `record_count` over-count for this shape.
    #[tokio::test]
    async fn ungrouped_ref_object_var_declines_dangling_fk_over_count() {
        use crate::seed::EmptyOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let (o, c, cnt) = (VarId(0), VarId(1), VarId(20));
        // `?o <custRef> ?c` — the SAME single-table representation the grouped P2a
        // fixtures use (predicate_filter + object_var bound by a RefObjectMap), only
        // UNGROUPED.
        let mut fact = R2rmlPattern::new("gs", o, Some(c));
        fact.triples_map_iri = Some("#Order".to_string());
        fact.predicate_filter = Some("http://ex/custRef".to_string());
        let plan = FusedAggregatePlan {
            graph_iri: Arc::from("gs"),
            inner_patterns: vec![Pattern::R2rml(fact)],
            filter: None,
            agg_binds: vec![],
            group_by: vec![], // UNGROUPED — no ref_group_key, so no dangling-FK resolver
            aggregates: vec![(cnt, AggregateFn::CountAll)],
        };
        let op = FusedR2rmlAggregateOperator::new(plan, Box::new(EmptyOperator::new()));
        let mapping = order_customer_mapping("http://ex/cust/{CID}");
        let mut batches = std::collections::HashMap::new();
        batches.insert(
            "order".to_string(),
            vec![batch_of(vec![
                i64_col("OID", 1, vec![Some(1), Some(2), Some(3), Some(4)]),
                i64_col("CFK", 2, vec![Some(10), Some(10), Some(20), Some(99)]),
            ])],
        );
        batches.insert(
            "customer".to_string(),
            vec![batch_of(vec![i64_col("CID", 1, vec![Some(10), Some(20)])])],
        );
        let provider = CrtProvider { mapping, batches };
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&provider, &provider);
        let resolved = op
            .resolve_at_open(&ctx)
            .await
            .expect("resolve must not error");
        assert!(
            resolved.is_none(),
            "an ungrouped aggregate over a RefObjectMap object var must DECLINE (no \
             resolver drops the present-but-dangling FK); pre-fix it admitted and \
             over-counted 4 vs the generic 3"
        );
    }

    #[tokio::test]
    async fn ref_iri_group_key_keep_min_on_duplicate_parent_key() {
        // Rider (b): a DUPLICATE parent join key with DISTINCT subjects. The customer
        // subject is keyed on SID (not the join column CID), so join key 10 mints two
        // subjects cust/b (row 0) and cust/a (row 1). The generic path
        // (parent_key_insert_keep_min) keeps the lexicographically SMALLER — cust/a —
        // so the fused fold must too (last-wins would pick cust/b). One order joins key
        // 10 → exactly one group cust/a, count 1.
        let mapping = order_customer_mapping("http://ex/cust/{SID}");
        let mut batches = std::collections::HashMap::new();
        batches.insert(
            "order".to_string(),
            batch_of(vec![
                i64_col("OID", 1, vec![Some(1)]),
                i64_col("CFK", 2, vec![Some(10)]),
            ]),
        );
        batches.insert(
            "customer".to_string(),
            batch_of(vec![
                i64_col("CID", 1, vec![Some(10), Some(10)]),
                str_col("SID", 2, vec![Some("b".to_string()), Some("a".to_string())]),
            ]),
        );
        let out = run_ref_iri_group_by(mapping, batches).await;
        assert_eq!(
            out,
            vec![("http://ex/cust/a".to_string(), 1)],
            "keep-min must bind the lexicographically smaller parent subject (cust/a), \
             byte-identical to build_parent_lookup"
        );
    }

    #[tokio::test]
    async fn ref_iri_group_key_high_cardinality() {
        // ~100k distinct customer IRIs exercise the dense-id dict at scale (the
        // crt_highcard shape): one order per customer → 100k groups, each count 1.
        const N: i64 = 100_000;
        let mapping = order_customer_mapping("http://ex/cust/{CID}");
        let mut batches = std::collections::HashMap::new();
        batches.insert(
            "order".to_string(),
            batch_of(vec![
                i64_col("OID", 1, (0..N).map(Some).collect()),
                i64_col("CFK", 2, (0..N).map(Some).collect()),
            ]),
        );
        batches.insert(
            "customer".to_string(),
            batch_of(vec![i64_col("CID", 1, (0..N).map(Some).collect())]),
        );
        let out = run_ref_iri_group_by(mapping, batches).await;
        assert_eq!(out.len(), N as usize, "one group per distinct customer IRI");
        assert!(
            out.iter().all(|(_, count)| *count == 1),
            "each single-order customer counts exactly 1"
        );
        assert_eq!(out[0].0, "http://ex/cust/0");
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

    // P3 (scaffolding): the branching-star detector must DECOMPOSE crt_join_reorder's
    // shape correctly — the fact root, one group-key dim branch, one semi-join fact
    // branch — even though full admission still declines (the soundness core lands
    // separately).
    #[test]
    fn decompose_branching_star_recognizes_crt_join_reorder_shape() {
        // Synthetic crt_join_reorder join graph (predicate strings arbitrary; only
        // the subject-var edges + which branch binds the GROUP BY var matter):
        //   ?ol(0) --order--> ?o(1) --customer--> ?c(3)      (semi-join branch)
        //   ?ol(0) --product--> ?p(2)  [binds ?cat(12)]      (group-key branch)
        // GROUP BY ?cat(12); ?qty(10) is the scalar measure (not a pattern subject).
        let star = |subj: u16, binds: &[(&str, u16)]| {
            let mut p = R2rmlPattern::new("gs", VarId(subj), None);
            p.star_bindings = binds
                .iter()
                .map(|(pr, v)| ((*pr).to_string(), VarId(*v)))
                .collect();
            p
        };
        let ol = star(0, &[("order", 1), ("product", 2), ("quantity", 10)]);
        let o = star(1, &[("customer", 3)]);
        let c = star(3, &[("segment", 11)]);
        let p = star(2, &[("category", 12)]);
        let group_by = [VarId(12)];

        // Pass shuffled; decomposition must recover the star regardless of order.
        let starr =
            FusedR2rmlAggregateOperator::decompose_branching_star(&[&c, &p, &ol, &o], &group_by)
                .expect("crt_join_reorder shape must decompose");
        assert_eq!(
            starr.root.subject_var,
            Some(VarId(0)),
            "OrderLine is the fact root"
        );
        assert_eq!(starr.branches.len(), 2);
        let gk = starr
            .branches
            .iter()
            .find_map(|b| match b {
                JoinBranch::GroupKey(br) => Some(br),
                JoinBranch::SemiJoin(_) => None,
            })
            .expect("exactly one group-key branch");
        let sj = starr
            .branches
            .iter()
            .find_map(|b| match b {
                JoinBranch::SemiJoin(br) => Some(br),
                JoinBranch::GroupKey(_) => None,
            })
            .expect("exactly one semi-join branch");
        // Group-key branch = product→category: chain [?p], root FK var = ?p(2).
        assert_eq!(gk.root_join_var, VarId(2));
        assert_eq!(
            gk.chain
                .iter()
                .map(|p| p.subject_var.unwrap())
                .collect::<Vec<_>>(),
            vec![VarId(2)]
        );
        // Semi-join branch = order→customer: chain [?o, ?c], root FK var = ?o(1),
        // within-branch hop = the customer join (?c=3).
        assert_eq!(sj.root_join_var, VarId(1));
        assert_eq!(
            sj.chain
                .iter()
                .map(|p| p.subject_var.unwrap())
                .collect::<Vec<_>>(),
            vec![VarId(1), VarId(3)]
        );
        assert_eq!(sj.join_vars, vec![VarId(3)]);

        // A LINEAR chain (no branch) is NOT a branching star — declines here (it is
        // order_chain's shape). A single group-key branch with NO semi-join declines.
        let lin_fact = star(0, &[("customer", 1)]);
        let lin_dim = star(1, &[("region", 12)]);
        assert!(FusedR2rmlAggregateOperator::decompose_branching_star(
            &[&lin_fact, &lin_dim],
            &group_by
        )
        .is_none());
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

    // ===================================================================
    // P3: branching-star multi-fact join (crt_join_reorder class) — the
    // soundness core (SEMI-JOIN keep-min-then-filter membership + fold probe).
    // ===================================================================

    use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};

    /// The crt_join_reorder mapping (integer `SUM(quantity)`): OrderLine(fact)
    /// `--order-->` Order `--customer-->` Customer, and OrderLine `--product-->`
    /// Product. The measure's declared datatype is caller-chosen so a decimal variant
    /// exercises the exact i128 → BigDecimal fold. Order's SUBJECT is keyed on a
    /// surrogate `OID`, NOT the join key `ORDER_KEY`, so a duplicate ORDER_KEY can
    /// carry two distinct subjects — the keep-min discriminator.
    fn crt_mapping(qty_dt: &str) -> Arc<CompiledR2rmlMapping> {
        use fluree_db_r2rml::mapping::PredicateMap;
        use fluree_vocab::xsd;
        Arc::new(CompiledR2rmlMapping::new(vec![
            TriplesMap::new("#OrderLine", "order_line")
                .with_subject_template("http://ex/ol/{OLID}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/quantity"),
                    object_map: ObjectMap::column_typed("QTY", qty_dt),
                })
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/order"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Order",
                        "ORDER_KEY",
                        "ORDER_KEY",
                    )),
                })
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/product"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Product",
                        "PRODUCT_KEY",
                        "PRODUCT_KEY",
                    )),
                }),
            TriplesMap::new("#Order", "order_t")
                .with_subject_template("http://ex/order/{OID}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/customer"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Customer",
                        "CUSTOMER_KEY",
                        "CUSTOMER_KEY",
                    )),
                }),
            TriplesMap::new("#Customer", "customer")
                .with_subject_template("http://ex/cust/{CUSTOMER_KEY}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/segment"),
                    object_map: ObjectMap::column("SEGMENT"),
                }),
            TriplesMap::new("#Product", "product")
                .with_subject_template("http://ex/prod/{PRODUCT_KEY}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/category"),
                    object_map: ObjectMap::column("CATEGORY"),
                })
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/isCurrent"),
                    object_map: ObjectMap::column_typed("IS_CURRENT", xsd::BOOLEAN),
                }),
        ]))
    }

    /// The crt_join_reorder BGP as rewritten R2rml leaf patterns (var ids match the
    /// shape-anchor test): fact `?ol(0)` {quantity=?qty(10), order=?o(1),
    /// product=?p(2)}; `?o(1)` {customer=?c(3)}; `?c(3)` [segment="Enterprise"];
    /// `?p(2)` {category=?cat(12)} [isCurrent=true].
    ///
    /// `segment_star` selects how the semi-join terminal's constraint is represented,
    /// so the OR gatherer (`resolve_semijoin_pattern_constraints`) is fixtured on BOTH
    /// forms: `false` = the STANDALONE const-object scan (`predicate_filter` +
    /// `object_constant`) the rewrite actually emits for a subject with no var-object
    /// member (the live shape, and the regression guard for the over-count bug);
    /// `true` = the FOLDED `star_constraints` form (what the rewrite emits when the
    /// subject also has a var-object member).
    fn crt_patterns(segment_star: bool) -> [R2rmlPattern; 4] {
        let (ol, o, c, p) = (VarId(0), VarId(1), VarId(3), VarId(2));
        let (qty, cat) = (VarId(10), VarId(12));
        let mut olp = R2rmlPattern::new("gs", ol, None);
        olp.triples_map_iri = Some("#OrderLine".into());
        olp.star_bindings = vec![
            ("http://ex/quantity".into(), qty),
            ("http://ex/order".into(), o),
            ("http://ex/product".into(), p),
        ];
        let mut op_ = R2rmlPattern::new("gs", o, None);
        op_.triples_map_iri = Some("#Order".into());
        op_.star_bindings = vec![("http://ex/customer".into(), c)];
        let mut cp = R2rmlPattern::new("gs", c, None);
        cp.triples_map_iri = Some("#Customer".into());
        let seg =
            crate::r2rml::ObjectConstant::Scalar(crate::r2rml::ScanValue::Str("Enterprise".into()));
        if segment_star {
            cp.star_constraints = vec![("http://ex/segment".into(), seg)];
        } else {
            cp.predicate_filter = Some("http://ex/segment".into());
            cp.object_constant = Some(seg);
        }
        let mut pp = R2rmlPattern::new("gs", p, None);
        pp.triples_map_iri = Some("#Product".into());
        pp.star_bindings = vec![("http://ex/category".into(), cat)];
        pp.star_constraints = vec![(
            "http://ex/isCurrent".into(),
            crate::r2rml::ObjectConstant::Scalar(crate::r2rml::ScanValue::Bool(true)),
        )];
        [olp, op_, cp, pp]
    }

    fn crt_op() -> FusedR2rmlAggregateOperator {
        use crate::ir::grouping::InputSemantics;
        use crate::seed::EmptyOperator;
        let (qty, cat, u) = (VarId(10), VarId(12), VarId(20));
        let plan = FusedAggregatePlan {
            graph_iri: Arc::from("gs"),
            inner_patterns: vec![],
            filter: None,
            agg_binds: vec![],
            group_by: vec![cat],
            aggregates: vec![(u, AggregateFn::Sum(qty, InputSemantics::List))],
        };
        FusedR2rmlAggregateOperator::new(plan, Box::new(EmptyOperator::new()))
    }

    /// A mock provider serving multiple tables (each 0+ batches) plus the mapping —
    /// the SEMI-JOIN dim builds (customer, order) + the GROUP-KEY dim build (product)
    /// scan here at resolve, and the fact (order_line) scans here in `next_batch`.
    #[derive(Debug)]
    struct CrtProvider {
        mapping: Arc<CompiledR2rmlMapping>,
        batches: std::collections::HashMap<String, Vec<ColumnBatch>>,
    }
    #[async_trait]
    impl crate::r2rml::R2rmlProvider for CrtProvider {
        async fn has_r2rml_mapping(&self, _gs: &str) -> bool {
            true
        }
        async fn compiled_mapping(
            &self,
            _gs: &str,
            _t: Option<i64>,
        ) -> Result<Arc<CompiledR2rmlMapping>> {
            Ok(Arc::clone(&self.mapping))
        }
    }
    #[async_trait]
    impl crate::r2rml::R2rmlTableProvider for CrtProvider {
        async fn scan_table(
            &self,
            _gs: &str,
            table: &str,
            _p: &[String],
            _f: &[crate::r2rml::ScanFilter],
            _tk: Option<&crate::r2rml::ScanTopK>,
            _t: Option<i64>,
        ) -> Result<crate::r2rml::ColumnBatchStream> {
            let bs = self.batches.get(table).cloned().unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(bs.into_iter().map(Ok))))
        }
    }

    fn mk_batch(cols: Vec<(FieldInfo, Column)>) -> ColumnBatch {
        let (fields, columns): (Vec<_>, Vec<_>) = cols.into_iter().unzip();
        let schema = Arc::new(BatchSchema::new(fields));
        ColumnBatch::new(schema, columns).unwrap()
    }
    fn field(name: &str, id: i32, ft: FieldType) -> FieldInfo {
        FieldInfo {
            name: name.to_string(),
            field_type: ft,
            nullable: true,
            field_id: id,
        }
    }
    fn icol(name: &str, id: i32, v: Vec<Option<i64>>) -> (FieldInfo, Column) {
        (field(name, id, FieldType::Int64), Column::Int64(v))
    }
    fn scol(name: &str, id: i32, v: Vec<&str>) -> (FieldInfo, Column) {
        (
            field(name, id, FieldType::String),
            Column::String(v.into_iter().map(|s| Some(s.to_string())).collect()),
        )
    }
    fn bcol(name: &str, id: i32, v: Vec<Option<bool>>) -> (FieldInfo, Column) {
        (field(name, id, FieldType::Boolean), Column::Boolean(v))
    }

    /// Resolve + fold the crt_join_reorder fixture through the P3 branching-star path
    /// (calling `resolve_branching_star_at_open` DIRECTLY — no env dependence — after
    /// `decompose_branching_star`), returning `(category, SUM)` rows sorted by
    /// category. Panics if the shape does not FUSE.
    async fn run_crt(
        mapping: Arc<CompiledR2rmlMapping>,
        batches: std::collections::HashMap<String, Vec<ColumnBatch>>,
        segment_star: bool,
    ) -> Vec<(String, Binding)> {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let pats = crt_patterns(segment_star);
        let refs: Vec<&R2rmlPattern> = pats.iter().collect();
        let provider = CrtProvider {
            mapping: Arc::clone(&mapping),
            batches,
        };
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&provider, &provider);
        let star = FusedR2rmlAggregateOperator::decompose_branching_star(&refs, &[VarId(12)])
            .expect("crt_join_reorder must decompose");
        let mut op = crt_op();
        let resolved = op
            .resolve_branching_star_at_open(&ctx, mapping.as_ref(), star)
            .await
            .expect("resolve must not error")
            .expect("crt_join_reorder must FUSE (not decline)");
        op.resolved = Some(resolved);
        let (cat, u) = (VarId(12), VarId(20));
        // Drain all output batches (output-bounding may chunk the rollup).
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch(&ctx).await.expect("next_batch") {
            let n = batch.column(cat).map(<[Binding]>::len).unwrap_or(0);
            for row in 0..n {
                let c = match batch.get(row, cat).expect("cat binding") {
                    Binding::Lit {
                        val: FlakeValue::String(s),
                        ..
                    } => s.clone(),
                    other => panic!("category key must be an xsd:string literal, got {other:?}"),
                };
                out.push((c, batch.get(row, u).expect("sum binding").clone()));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn long_sum(n: i64) -> Binding {
        Binding::lit(FlakeValue::Long(n), Sid::xsd_integer())
    }

    /// Batches for a clean (unique-key) crt_join_reorder fixture: two Enterprise
    /// orders on `Widgets`, one on `Gadgets`, one order on an SMB customer (dropped by
    /// the semi-join), and one line on a non-current product (dropped by the
    /// group-key dim constraint). Integer quantities.
    fn clean_batches() -> std::collections::HashMap<String, Vec<ColumnBatch>> {
        let mut m = std::collections::HashMap::new();
        // OrderLine: OLID, ORDER_KEY, PRODUCT_KEY, QTY.
        m.insert(
            "order_line".to_string(),
            vec![mk_batch(vec![
                icol("OLID", 1, vec![Some(1), Some(2), Some(3), Some(4), Some(5)]),
                icol(
                    "ORDER_KEY",
                    2,
                    vec![Some(10), Some(11), Some(12), Some(13), Some(10)],
                ),
                icol(
                    "PRODUCT_KEY",
                    3,
                    vec![Some(100), Some(100), Some(200), Some(100), Some(300)],
                ),
                icol("QTY", 4, vec![Some(5), Some(7), Some(3), Some(9), Some(2)]),
            ])],
        );
        // Order: OID (subject surrogate), ORDER_KEY (join key), CUSTOMER_KEY (FK).
        m.insert(
            "order_t".to_string(),
            vec![mk_batch(vec![
                scol("OID", 1, vec!["o10", "o11", "o12", "o13"]),
                icol("ORDER_KEY", 2, vec![Some(10), Some(11), Some(12), Some(13)]),
                icol(
                    "CUSTOMER_KEY",
                    3,
                    vec![Some(1000), Some(1000), Some(1001), Some(1002)],
                ),
            ])],
        );
        // Customer: CUSTOMER_KEY, SEGMENT. 1002 is SMB (drops order 13).
        m.insert(
            "customer".to_string(),
            vec![mk_batch(vec![
                icol("CUSTOMER_KEY", 1, vec![Some(1000), Some(1001), Some(1002)]),
                scol("SEGMENT", 2, vec!["Enterprise", "Enterprise", "SMB"]),
            ])],
        );
        // Product: PRODUCT_KEY, CATEGORY, IS_CURRENT. 300 is not current (drops OL5).
        m.insert(
            "product".to_string(),
            vec![mk_batch(vec![
                icol("PRODUCT_KEY", 1, vec![Some(100), Some(200), Some(300)]),
                scol("CATEGORY", 2, vec!["Widgets", "Gadgets", "Widgets"]),
                bcol("IS_CURRENT", 3, vec![Some(true), Some(true), Some(false)]),
            ])],
        );
        m
    }

    /// (a) VALUE-IDENTITY: the clean fixture folds to the generic inner join's answer,
    /// byte-identical (integer `SUM(quantity)` → xsd:integer Long). Widgets = OL1(5) +
    /// OL2(7) = 12 (OL5 dropped: product not current); Gadgets = OL3(3); OL4 dropped
    /// (SMB customer).
    #[tokio::test]
    async fn p3_crt_join_reorder_value_identity_integer() {
        let out = run_crt(
            crt_mapping(fluree_vocab::xsd::INTEGER),
            clean_batches(),
            false,
        )
        .await;
        assert_eq!(
            out,
            vec![
                ("Gadgets".to_string(), long_sum(3)),
                ("Widgets".to_string(), long_sum(12)),
            ],
            "fused SUM must equal the generic chained-inner-join answer, byte-identical"
        );
    }

    /// (a-star) The OR gatherer's OTHER arm: the same clean fixture with the semi-join
    /// terminal's segment constraint in the FOLDED `star_constraints` form (what the
    /// rewrite emits when the subject also has a var-object member). Same correct answer
    /// as the standalone-const-object form above — so `resolve_semijoin_pattern_constraints`
    /// is fixtured on BOTH forms (the const-object form is the live shape + the
    /// discriminator's regression guard; this pins the star_constraints arm).
    #[tokio::test]
    async fn p3_crt_join_reorder_semijoin_constraint_star_constraints_form() {
        let out = run_crt(
            crt_mapping(fluree_vocab::xsd::INTEGER),
            clean_batches(),
            true,
        )
        .await;
        assert_eq!(
            out,
            vec![
                ("Gadgets".to_string(), long_sum(3)),
                ("Widgets".to_string(), long_sum(12)),
            ],
            "the folded star_constraints form of the semi-join filter must produce the \
             same filtered answer as the standalone const-object form"
        );
    }

    /// B2 (review id=3717398030): a semi-join CHAIN pattern that carries a VAR-OBJECT
    /// member (`star_bindings`) OTHER than its next-hop FK must DECLINE the fuse.
    /// `build_semi_join_membership` projects only join / subject / constraint / next-FK
    /// columns and never consults `star_bindings`, so a leaf var-object member — a BGP
    /// existence requirement (the object column must be non-null for the triple to
    /// match) — would be silently dropped and OVER-ADMIT the root FK (a customer with a
    /// null region is folded in though the generic BGP excludes it). This uses the REAL
    /// two-forms representation the rewrite emits (memory: r2rml-const-object-two-forms):
    /// segment lives in `star_constraints` BECAUSE the customer subject now ALSO carries
    /// the `?r` (region) var-object member — exactly the shape id=3717398038 flagged as
    /// otherwise unemittable. `?r` (VarId 30) is a leaf object var, NOT a branch join
    /// var and NOT a group-by var, so the branch stays a SEMI-JOIN and the join topology
    /// is unchanged (still decomposes). Pre-fix `resolve_branching_star_at_open`
    /// returned `Some(..)` (folded, ignoring the region existence → over-count); the
    /// guard makes it DECLINE (`Ok(None)` → the generic path, which honors the region
    /// BGP). The interior `order` pattern's ONLY star_binding is `(customer, ?c)`, its
    /// next-hop FK (a branch join var) — so the guard must NOT decline the clean shape.
    #[tokio::test]
    async fn p3_semijoin_chain_var_object_member_declines() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let mapping = crt_mapping(fluree_vocab::xsd::INTEGER);
        let mut pats = crt_patterns(true); // segment in star_constraints (the two-forms arm)
        pats[2].star_bindings = vec![("http://ex/region".into(), VarId(30))];
        let refs: Vec<&R2rmlPattern> = pats.iter().collect();
        let provider = CrtProvider {
            mapping: Arc::clone(&mapping),
            batches: clean_batches(),
        };
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&provider, &provider);
        let star = FusedR2rmlAggregateOperator::decompose_branching_star(&refs, &[VarId(12)])
            .expect("the leaf var-object member does not change the join topology");
        let mut op = crt_op();
        let resolved = op
            .resolve_branching_star_at_open(&ctx, mapping.as_ref(), star)
            .await
            .expect("resolve must not error");
        assert!(
            resolved.is_none(),
            "a semi-join chain pattern with a leaf var-object member must DECLINE the \
             fuse (the membership build cannot honor the region existence requirement); \
             pre-fix it admitted and over-counted"
        );
    }

    /// id=3717339904 (SCD-2 same-key/same-subject semi-join gap): when two semi-join
    /// dim rows share the join key AND mint the SAME subject but disagree on the
    /// constraint result (`passes_own`) or the next-hop FK, keep-min — which
    /// discriminates only by subject — would let SCAN ORDER decide membership, a
    /// non-deterministic ANSWER (the generic chained join materializes every version's
    /// triples; the fused keep-min tests ONE). This is exactly the SCD-2 shape P3
    /// targets. Two customers keyed 1000 both minting cust/1000, one Enterprise (passes)
    /// and one SMB (fails segment) → the fuse must DECLINE to the generic path. Pre-fix
    /// it kept whichever row the scan hit first and admitted (or excluded) key 1000
    /// accordingly.
    #[tokio::test]
    async fn p3_semijoin_same_key_same_subject_conflicting_constraint_declines() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let mapping = crt_mapping(fluree_vocab::xsd::INTEGER);
        let mut batches = clean_batches();
        // Customer: TWO rows keyed 1000 (same join key → same subject cust/1000) with
        // DIFFERING segment — the SCD-2 collision. 1001/1002 unchanged.
        batches.insert(
            "customer".to_string(),
            vec![mk_batch(vec![
                icol(
                    "CUSTOMER_KEY",
                    1,
                    vec![Some(1000), Some(1000), Some(1001), Some(1002)],
                ),
                scol("SEGMENT", 2, vec!["Enterprise", "SMB", "Enterprise", "SMB"]),
            ])],
        );
        let pats = crt_patterns(false); // standalone const-object segment form
        let refs: Vec<&R2rmlPattern> = pats.iter().collect();
        let provider = CrtProvider {
            mapping: Arc::clone(&mapping),
            batches,
        };
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&provider, &provider);
        let star = FusedR2rmlAggregateOperator::decompose_branching_star(&refs, &[VarId(12)])
            .expect("crt_join_reorder must decompose");
        let mut op = crt_op();
        let resolved = op
            .resolve_branching_star_at_open(&ctx, mapping.as_ref(), star)
            .await
            .expect("resolve must not error");
        assert!(
            resolved.is_none(),
            "a same-key/same-subject SCD-2 collision with differing constraint results \
             must DECLINE (keep-min would let scan order decide the answer)"
        );
    }

    /// (a′) VALUE-IDENTITY, Dec i128 path: the same shape with an xsd:decimal measure
    /// folds through the exact i128 → BigDecimal accumulator. Quantities 5.25, 7.25,
    /// 3.50 (scale 2). Widgets = 5.25 + 7.25 = 12.50; Gadgets = 3.50. The reference is
    /// the SAME `Acc::Exact` finalize the fold uses, so this pins the exact-decimal
    /// binding bit for bit.
    #[tokio::test]
    async fn p3_crt_join_reorder_value_identity_decimal() {
        let mut b = clean_batches();
        // Replace QTY with a scale-2 decimal column; keep the same rows.
        b.insert(
            "order_line".to_string(),
            vec![mk_batch(vec![
                icol("OLID", 1, vec![Some(1), Some(2), Some(3), Some(4), Some(5)]),
                icol(
                    "ORDER_KEY",
                    2,
                    vec![Some(10), Some(11), Some(12), Some(13), Some(10)],
                ),
                icol(
                    "PRODUCT_KEY",
                    3,
                    vec![Some(100), Some(100), Some(200), Some(100), Some(300)],
                ),
                (
                    field(
                        "QTY",
                        4,
                        FieldType::Decimal {
                            precision: 12,
                            scale: 2,
                        },
                    ),
                    Column::Decimal {
                        values: vec![Some(525), Some(725), Some(350), Some(900), Some(200)],
                        precision: 12,
                        scale: 2,
                    },
                ),
            ])],
        );
        // `finalize_sum` only branches on count==0 (→ integer 0) vs >0; the exact
        // value is independent of count for SUM, so any positive count reproduces the
        // fold's decimal binding bit for bit.
        let dec = |unscaled: i128| {
            Acc::Exact {
                sum: unscaled,
                scale: 2,
                decimal: true,
                count: 1,
                is_avg: false,
            }
            .finalize()
        };
        let out = run_crt(crt_mapping(fluree_vocab::xsd::DECIMAL), b, false).await;
        assert_eq!(
            out,
            vec![
                ("Gadgets".to_string(), dec(350)),  // 3.50
                ("Widgets".to_string(), dec(1250)), // 5.25 + 7.25 = 12.50
            ],
            "fused decimal SUM must be byte-identical to the exact i128 → BigDecimal finalize"
        );
    }

    /// OUTPUT-BOUNDING: a high-cardinality GROUP BY (> OUTPUT_BOUND_ROWS groups) emits
    /// in bounded chunks with the switch ON — multiple batches, each ≤ the bound, whose
    /// union is the full result — and in a SINGLE batch with the switch OFF (the
    /// byte-identical pre-bounding path). Same total either way. Single-table ref-IRI
    /// GROUP BY (one order per customer → one group per customer).
    #[tokio::test]
    async fn p3_output_bounding_chunks_high_card_and_off_single_batch() {
        use crate::seed::EmptyOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let n: i64 = OUTPUT_BOUND_ROWS as i64 + 500; // spills past one chunk
        let mapping = order_customer_mapping("http://ex/cust/{CID}");
        let mut b = std::collections::HashMap::new();
        b.insert(
            "order".to_string(),
            vec![mk_batch(vec![
                icol("OID", 1, (0..n).map(Some).collect()),
                icol("CFK", 2, (0..n).map(Some).collect()),
            ])],
        );
        b.insert(
            "customer".to_string(),
            vec![mk_batch(vec![icol("CID", 1, (0..n).map(Some).collect())])],
        );
        let (o, c, cnt) = (VarId(0), VarId(1), VarId(20));
        let build_op = || {
            let mut fact = R2rmlPattern::new("gs", o, Some(c));
            fact.triples_map_iri = Some("#Order".to_string());
            fact.predicate_filter = Some("http://ex/custRef".to_string());
            let plan = FusedAggregatePlan {
                graph_iri: Arc::from("gs"),
                inner_patterns: vec![Pattern::R2rml(fact)],
                filter: None,
                agg_binds: vec![],
                group_by: vec![c],
                aggregates: vec![(cnt, AggregateFn::CountAll)],
            };
            FusedR2rmlAggregateOperator::new(plan, Box::new(EmptyOperator::new()))
        };
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();

        // ON: multiple bounded chunks, each ≤ the bound, union == full result.
        std::env::set_var("FLUREE_FUSED_R2RML_OUTPUT_BOUND", "1");
        assert!(output_bound_enabled());
        let prov_on = CrtProvider {
            mapping: Arc::clone(&mapping),
            batches: b.clone(),
        };
        let ctx_on =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&prov_on, &prov_on);
        let mut op_on = build_op();
        op_on.open(&ctx_on).await.expect("open");
        let (mut nbatch_on, mut total_on, mut max_chunk) = (0usize, 0usize, 0usize);
        while let Some(batch) = op_on.next_batch(&ctx_on).await.expect("next_batch") {
            let rows = batch.column(c).map(<[Binding]>::len).unwrap_or(0);
            nbatch_on += 1;
            total_on += rows;
            max_chunk = max_chunk.max(rows);
        }
        assert!(
            nbatch_on > 1,
            "ON must chunk a >{OUTPUT_BOUND_ROWS}-group rollup into multiple batches (got {nbatch_on})"
        );
        assert!(
            max_chunk <= OUTPUT_BOUND_ROWS,
            "each output chunk must be ≤ OUTPUT_BOUND_ROWS (got {max_chunk})"
        );
        assert_eq!(
            total_on, n as usize,
            "ON must emit every group exactly once"
        );

        // OFF: exactly one batch (byte-identical pre-bounding emission), same total.
        std::env::set_var("FLUREE_FUSED_R2RML_OUTPUT_BOUND", "0");
        assert!(!output_bound_enabled());
        let prov_off = CrtProvider {
            mapping: Arc::clone(&mapping),
            batches: b,
        };
        let ctx_off =
            ExecutionContext::new(&snapshot, &vars).with_r2rml_providers(&prov_off, &prov_off);
        let mut op_off = build_op();
        op_off.open(&ctx_off).await.expect("open");
        let (mut nbatch_off, mut total_off) = (0usize, 0usize);
        while let Some(batch) = op_off.next_batch(&ctx_off).await.expect("next_batch") {
            nbatch_off += 1;
            total_off += batch.column(c).map(<[Binding]>::len).unwrap_or(0);
        }
        std::env::remove_var("FLUREE_FUSED_R2RML_OUTPUT_BOUND");
        assert_eq!(nbatch_off, 1, "OFF emits a single batch");
        assert_eq!(
            total_off, n as usize,
            "OFF must emit every group exactly once"
        );
    }

    /// (b) KEEP-MIN-THEN-FILTER DISCRIMINATOR: a duplicate ORDER_KEY where the keep-min
    /// Order row (smaller subject) points at a NON-Enterprise customer and the
    /// DISCARDED duplicate points at an Enterprise customer. The generic join resolves
    /// the FK to the keep-min parent subject and tests THAT row, so the order line
    /// DROPS. A naive filter-then-union membership set (admit the key because SOME
    /// duplicate matches) would WRONGLY include it — this fixture is built so that
    /// bug would produce Widgets=50 instead of the correct empty result.
    #[tokio::test]
    async fn p3_crt_join_reorder_keep_min_then_filter_excludes() {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "order_line".to_string(),
            vec![mk_batch(vec![
                icol("OLID", 1, vec![Some(1)]),
                icol("ORDER_KEY", 2, vec![Some(1)]),
                icol("PRODUCT_KEY", 3, vec![Some(100)]),
                icol("QTY", 4, vec![Some(50)]),
            ])],
        );
        // Two Order rows, SAME ORDER_KEY=1, distinct subjects: order/a (keep-min) →
        // customer 200 (SMB); order/b (discarded) → customer 100 (Enterprise).
        m.insert(
            "order_t".to_string(),
            vec![mk_batch(vec![
                scol("OID", 1, vec!["a", "b"]),
                icol("ORDER_KEY", 2, vec![Some(1), Some(1)]),
                icol("CUSTOMER_KEY", 3, vec![Some(200), Some(100)]),
            ])],
        );
        m.insert(
            "customer".to_string(),
            vec![mk_batch(vec![
                icol("CUSTOMER_KEY", 1, vec![Some(100), Some(200)]),
                scol("SEGMENT", 2, vec!["Enterprise", "SMB"]),
            ])],
        );
        m.insert(
            "product".to_string(),
            vec![mk_batch(vec![
                icol("PRODUCT_KEY", 1, vec![Some(100)]),
                scol("CATEGORY", 2, vec!["Widgets"]),
                bcol("IS_CURRENT", 3, vec![Some(true)]),
            ])],
        );
        let out = run_crt(crt_mapping(fluree_vocab::xsd::INTEGER), m, false).await;
        assert!(
            out.is_empty(),
            "keep-min Order row is SMB → the line drops; filter-then-union would wrongly \
             admit it (Widgets=50). Got {out:?}"
        );
    }

    /// (b-control) The SAME duplicate-key fixture with the subjects SWAPPED so the
    /// keep-min row is now the Enterprise one — the order line must be INCLUDED. The
    /// contrast with the test above proves the keep-min tie-break (not "any duplicate
    /// matches") decides membership.
    #[tokio::test]
    async fn p3_crt_join_reorder_keep_min_control_includes() {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "order_line".to_string(),
            vec![mk_batch(vec![
                icol("OLID", 1, vec![Some(1)]),
                icol("ORDER_KEY", 2, vec![Some(1)]),
                icol("PRODUCT_KEY", 3, vec![Some(100)]),
                icol("QTY", 4, vec![Some(50)]),
            ])],
        );
        // order/a (keep-min) → customer 100 (Enterprise); order/b (discarded) → 200 (SMB).
        m.insert(
            "order_t".to_string(),
            vec![mk_batch(vec![
                scol("OID", 1, vec!["a", "b"]),
                icol("ORDER_KEY", 2, vec![Some(1), Some(1)]),
                icol("CUSTOMER_KEY", 3, vec![Some(100), Some(200)]),
            ])],
        );
        m.insert(
            "customer".to_string(),
            vec![mk_batch(vec![
                icol("CUSTOMER_KEY", 1, vec![Some(100), Some(200)]),
                scol("SEGMENT", 2, vec!["Enterprise", "SMB"]),
            ])],
        );
        m.insert(
            "product".to_string(),
            vec![mk_batch(vec![
                icol("PRODUCT_KEY", 1, vec![Some(100)]),
                scol("CATEGORY", 2, vec!["Widgets"]),
                bcol("IS_CURRENT", 3, vec![Some(true)]),
            ])],
        );
        let out = run_crt(crt_mapping(fluree_vocab::xsd::INTEGER), m, false).await;
        assert_eq!(
            out,
            vec![("Widgets".to_string(), long_sum(50))],
            "keep-min Order row is Enterprise → the line is included"
        );
    }

    /// (c) BUDGET: the SEMI-JOIN membership build is charged via `record_alloc` and
    /// checkpoints per batch, so an oversized branch aborts typed
    /// (`MemoryBudgetExceeded`, 507) instead of OOMing. A tiny pinned budget + a
    /// multi-batch terminal dim trips it inside `build_semi_join_membership`.
    #[tokio::test]
    async fn p3_crt_join_reorder_budget_exceeded_fails_typed() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{LedgerSnapshot, QueryCancellation};
        let mapping = crt_mapping(fluree_vocab::xsd::INTEGER);
        let mut m = std::collections::HashMap::new();
        // Terminal Customer dim served as TWO batches: batch 1 charges the running
        // total past the budget, batch 2's checkpoint aborts.
        m.insert(
            "customer".to_string(),
            vec![
                mk_batch(vec![
                    icol("CUSTOMER_KEY", 1, vec![Some(1000), Some(1001)]),
                    scol("SEGMENT", 2, vec!["Enterprise", "Enterprise"]),
                ]),
                mk_batch(vec![
                    icol("CUSTOMER_KEY", 1, vec![Some(1002), Some(1003)]),
                    scol("SEGMENT", 2, vec!["Enterprise", "Enterprise"]),
                ]),
            ],
        );
        let pats = crt_patterns(false);
        let refs: Vec<&R2rmlPattern> = pats.iter().collect();
        let provider = CrtProvider {
            mapping: Arc::clone(&mapping),
            batches: m,
        };
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let cancel = QueryCancellation::new();
        cancel.set_memory_limit(64); // < GROUP_EST_BYTES → the first batch's charge trips
        let ctx = ExecutionContext::new(&snapshot, &vars)
            .with_r2rml_providers(&provider, &provider)
            .with_cancellation(cancel);
        let star = FusedR2rmlAggregateOperator::decompose_branching_star(&refs, &[VarId(12)])
            .expect("decompose");
        match crt_op()
            .resolve_branching_star_at_open(&ctx, mapping.as_ref(), star)
            .await
        {
            Err(QueryError::MemoryBudgetExceeded { .. }) => {}
            Err(other) => panic!("expected MemoryBudgetExceeded, got a different error: {other:?}"),
            Ok(_) => panic!("an over-budget semi-join build must fail typed, not succeed/OOM"),
        }
    }

    /// (d) SWITCH: `FLUREE_FUSED_R2RML_MULTIFACT=0` disables the whole branching path,
    /// so `resolve_join_at_open` DECLINES the star (Ok(None) → the pre-P3 fallback,
    /// byte-identical); unset/on FUSES it. This is the sole reader/writer of the env
    /// var (the resolver tests call `resolve_branching_star_at_open` directly), so no
    /// lock is needed. `multifact_enabled` is read uncached under cfg(test).
    #[tokio::test]
    async fn p3_multifact_switch_gates_the_branching_path() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        let mapping = crt_mapping(fluree_vocab::xsd::INTEGER);
        let pats = crt_patterns(false);
        let refs: Vec<&R2rmlPattern> = pats.iter().collect();
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();

        // OFF → decline (Ok(None)).
        std::env::set_var("FLUREE_FUSED_R2RML_MULTIFACT", "0");
        assert!(!FusedR2rmlAggregateOperator::multifact_enabled());
        let provider_off = CrtProvider {
            mapping: Arc::clone(&mapping),
            batches: std::collections::HashMap::new(),
        };
        let ctx_off = ExecutionContext::new(&snapshot, &vars)
            .with_r2rml_providers(&provider_off, &provider_off);
        let off = crt_op()
            .resolve_join_at_open(&ctx_off, &refs, mapping.as_ref())
            .await
            .expect("resolve must not error");
        assert!(
            off.is_none(),
            "switch OFF must DECLINE the branching star (pre-P3 byte-identical path)"
        );

        // ON (the same shape + a valid fixture) → fuse (Ok(Some)).
        std::env::set_var("FLUREE_FUSED_R2RML_MULTIFACT", "1");
        assert!(FusedR2rmlAggregateOperator::multifact_enabled());
        let provider_on = CrtProvider {
            mapping: Arc::clone(&mapping),
            batches: clean_batches(),
        };
        let ctx_on = ExecutionContext::new(&snapshot, &vars)
            .with_r2rml_providers(&provider_on, &provider_on);
        let on = crt_op()
            .resolve_join_at_open(&ctx_on, &refs, mapping.as_ref())
            .await
            .expect("resolve must not error");
        std::env::remove_var("FLUREE_FUSED_R2RML_MULTIFACT");
        assert!(
            on.is_some(),
            "switch ON must FUSE the branching star (contrast proving the gate controls it)"
        );
    }
}
