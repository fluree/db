//! Expression lowering — Cypher Expr → fluree-db-query Expression.

use std::sync::Arc;

use fluree_db_core::FlakeValue;
use fluree_db_query::ir::expression::ListPredicateKind as IrListPredicateKind;
use fluree_db_query::ir::{Expression, Function, Pattern, Ref, Term, TriplePattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;

use crate::ast::{
    BinOp, CaseExpr, Expr, ListPredicateKind, Literal, MapProjectionSelector, ParamRef, UnaryOp,
};

use super::context::LoweringContext;
use super::pattern::lower_pattern;
use super::{LowerError, Result};

/// Lower a Cypher expression to an `Expression`. Any auxiliary
/// patterns the expression requires (e.g., property-accessor joins)
/// are appended to `aux`. The caller is responsible for splicing
/// `aux` into the enclosing pattern list before the position where
/// the expression is evaluated.
pub fn lower_expr<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    e: &Expr,
    aux: &mut Vec<Pattern>,
) -> Result<Expression> {
    match e {
        Expr::Var(v) => Ok(Expression::Var(ctx.intern_var(&v.name))),
        Expr::Lit(l) => Ok(Expression::Const(lower_literal(l)?)),
        Expr::Param(_) => Err(LowerError::unsupported(
            "parameter substitution is wired at the API layer, not the lowering layer; submit pre-substituted Cypher in v1",
        )),
        Expr::Prop(target, key, _) => {
            // Property access on a loop-local (a comprehension / reduce variable)
            // can't be a graph-join pattern — lower to eval-time member access.
            // Carry both the bare key (map lookup) and the resolved predicate IRI
            // (node-property scan); the IRI needs the vocab, known only here.
            if let Expr::Var(v) = target.as_ref() {
                if ctx.is_local(&v.name) {
                    let target_expr = lower_expr(ctx, target, aux)?;
                    let predicate_iri = ctx.resolve_predicate(key)?;
                    return Ok(Expression::Member {
                        target: Box::new(target_expr),
                        key: Arc::from(key.as_str()),
                        predicate_iri: Arc::from(predicate_iri.as_str()),
                    });
                }
            }
            // Temporal accessor (`<date>.month`, `<datetime>.year`, …): when
            // the target is a *value* expression — e.g. another property
            // access `friend.birthday` — rather than a bare node variable,
            // lower to the matching extraction function. `n.month` where `n`
            // is a node stays an ordinary property accessor.
            if !matches!(target.as_ref(), Expr::Var(_)) {
                if let Some(func) = temporal_field_function(key) {
                    let inner = lower_expr(ctx, target, aux)?;
                    return Ok(Expression::call(func, vec![inner]));
                }
            }
            let prop_var = resolve_property_accessor(ctx, target, key, aux)?;
            Ok(Expression::Var(prop_var))
        }
        Expr::BinOp(op, l, r, _) => {
            let l = lower_expr(ctx, l, aux)?;
            let r = lower_expr(ctx, r, aux)?;
            let f = match op {
                BinOp::Eq => Function::Eq,
                BinOp::Ne => Function::Ne,
                BinOp::Lt => Function::Lt,
                BinOp::Le => Function::Le,
                BinOp::Gt => Function::Gt,
                BinOp::Ge => Function::Ge,
                BinOp::Add => Function::Add,
                BinOp::Sub => Function::Sub,
                BinOp::Mul => Function::Mul,
                BinOp::Div => Function::Div,
                BinOp::Mod => Function::Mod,
                BinOp::Pow => Function::Pow,
                BinOp::And => Function::And,
                BinOp::Or => Function::Or,
            };
            Ok(Expression::binary(f, l, r))
        }
        Expr::UnaryOp(op, inner, _) => {
            let inner = lower_expr(ctx, inner, aux)?;
            let f = match op {
                UnaryOp::Neg => Function::Negate,
                UnaryOp::Not => Function::Not,
            };
            Ok(Expression::call(f, vec![inner]))
        }
        Expr::In(left, list, _) => {
            // Lower to `Function::In(test, candidate1, candidate2, ...)`.
            // The right-hand side must be a list literal in v1; parameter-
            // bound list expressions are deferred.
            let test = lower_expr(ctx, left, aux)?;
            let items = match list.as_ref() {
                Expr::List(items, _) => items,
                _ => {
                    return Err(LowerError::unsupported(
                        "`IN` right-hand side must be an inline list `[a, b, ...]` in v1",
                    ));
                }
            };
            let mut args = Vec::with_capacity(items.len() + 1);
            args.push(test);
            for item in items {
                args.push(lower_expr(ctx, item, aux)?);
            }
            Ok(Expression::call(Function::In, args))
        }
        Expr::IsNull(inner, _) => {
            let inner = lower_expr(ctx, inner, aux)?;
            Ok(Expression::call(Function::Not, vec![Expression::call(
                Function::Bound,
                vec![inner],
            )]))
        }
        Expr::IsNotNull(inner, _) => {
            let inner = lower_expr(ctx, inner, aux)?;
            Ok(Expression::call(Function::Bound, vec![inner]))
        }
        Expr::StartsWith(l, r, _) => {
            let l = lower_expr(ctx, l, aux)?;
            let r = lower_expr(ctx, r, aux)?;
            Ok(Expression::binary(Function::StrStarts, l, r))
        }
        Expr::EndsWith(l, r, _) => {
            let l = lower_expr(ctx, l, aux)?;
            let r = lower_expr(ctx, r, aux)?;
            Ok(Expression::binary(Function::StrEnds, l, r))
        }
        Expr::Contains(l, r, _) => {
            let l = lower_expr(ctx, l, aux)?;
            let r = lower_expr(ctx, r, aux)?;
            Ok(Expression::binary(Function::Contains, l, r))
        }
        Expr::Case(case) => lower_case(ctx, case, aux),
        Expr::Exists(pattern, inner_where, _) => {
            let mut patterns = lower_pattern(ctx, pattern)?;
            // An inner WHERE is ANDed into the existence test. Its own auxiliary
            // patterns (e.g. property-accessor Optionals) must live INSIDE the
            // subquery so `x.id` resolves within the existence scope, not the
            // outer query — so lower it against a local aux, not the caller's.
            if let Some(cond) = inner_where {
                let mut inner_aux = Vec::new();
                let filter = lower_expr(ctx, cond, &mut inner_aux)?;
                patterns.extend(inner_aux);
                patterns.push(Pattern::Filter(filter));
            }
            Ok(Expression::Exists {
                patterns,
                negated: false,
            })
        }
        Expr::List(items, _) => {
            // A list literal `[a, b, …]` builds a `Binding::List` value via the
            // `MakeList` constructor. Enables structured `collect([a, b])`.
            let args = items
                .iter()
                .map(|it| lower_expr(ctx, it, aux))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expression::call(Function::MakeList, args))
        }
        Expr::Map(entries, _) => {
            // A map literal `{k: v, …}` lowers to `Expression::Map`: keys are
            // static (`Arc<str>`), values are sub-expressions evaluated per row.
            let mut lowered = Vec::with_capacity(entries.len());
            for (key, val) in entries {
                lowered.push((
                    std::sync::Arc::from(key.as_str()),
                    lower_expr(ctx, val, aux)?,
                ));
            }
            Ok(Expression::Map(lowered))
        }
        Expr::Index(list, index, _) => {
            // `list[index]` — element access (e.g. `pair[0]`).
            let list = lower_expr(ctx, list, aux)?;
            let index = lower_expr(ctx, index, aux)?;
            Ok(Expression::call(Function::ListIndex, vec![list, index]))
        }
        // List-iteration forms: lower the list (outer scope), then a fresh
        // loop-local scope for the body so the body's `var` resolves to a
        // synthetic id (and `var.prop` becomes member access).
        Expr::ListComprehension(c) => {
            let list_expr = Box::new(lower_expr(ctx, &c.list, aux)?);
            ctx.enter_scope();
            let loop_var = ctx.bind_local(&c.var.name);
            let filter_expr = c
                .filter
                .as_ref()
                .map(|f| lower_expr(ctx, f, aux))
                .transpose()?
                .map(Box::new);
            let map_expr = c
                .map
                .as_ref()
                .map(|m| lower_expr(ctx, m, aux))
                .transpose()?
                .map(Box::new);
            ctx.exit_scope();
            let built = Expression::ListComprehension {
                var: loop_var,
                list: list_expr,
                filter: filter_expr,
                map: map_expr,
            };
            reject_exists_in_iteration(&built)?;
            Ok(built)
        }
        Expr::Reduce(r) => {
            let init_expr = Box::new(lower_expr(ctx, &r.init, aux)?);
            let list_expr = Box::new(lower_expr(ctx, &r.list, aux)?);
            ctx.enter_scope();
            let acc_var = ctx.bind_local(&r.acc.name);
            let loop_var = ctx.bind_local(&r.var.name);
            let body_expr = Box::new(lower_expr(ctx, &r.body, aux)?);
            ctx.exit_scope();
            let built = Expression::Reduce {
                acc: acc_var,
                init: init_expr,
                var: loop_var,
                list: list_expr,
                body: body_expr,
            };
            reject_exists_in_iteration(&built)?;
            Ok(built)
        }
        Expr::ListPredicate(pred) => {
            let list_expr = Box::new(lower_expr(ctx, &pred.list, aux)?);
            ctx.enter_scope();
            let loop_var = ctx.bind_local(&pred.var.name);
            let pred_expr = Box::new(lower_expr(ctx, &pred.predicate, aux)?);
            ctx.exit_scope();
            let built = Expression::ListPredicate {
                kind: lower_list_predicate_kind(pred.kind),
                var: loop_var,
                list: list_expr,
                predicate: pred_expr,
            };
            reject_exists_in_iteration(&built)?;
            Ok(built)
        }
        Expr::MapProjection(mp) => {
            let has_all = mp
                .selectors
                .iter()
                .any(|s| matches!(s, MapProjectionSelector::AllProperties));
            if has_all {
                // `var{.*}` is exactly `properties(var)`. Mixing `.*` with other
                // selectors would need a runtime map merge — deferred.
                if mp.selectors.len() != 1 {
                    return Err(LowerError::unsupported(
                        "map projection mixing `.*` with other selectors is deferred — use \
                         `properties(n)` or list the keys explicitly",
                    ));
                }
                let target = Expression::Var(ctx.intern_var(&mp.var.name));
                return Ok(Expression::call(Function::Properties, vec![target]));
            }
            // `var{.a, k: e}` desugars to `{a: var.a, k: e}`. A `.key` selector
            // reuses the property-accessor lowering (outer-var aux-pattern join,
            // or eval-time member access when `var` is a loop-local).
            let mut entries = Vec::with_capacity(mp.selectors.len());
            for sel in &mp.selectors {
                match sel {
                    MapProjectionSelector::Property(key) => {
                        let accessor =
                            Expr::Prop(Box::new(Expr::Var(mp.var.clone())), key.clone(), mp.span);
                        entries.push((Arc::from(key.as_str()), lower_expr(ctx, &accessor, aux)?));
                    }
                    MapProjectionSelector::Literal(key, expr) => {
                        entries.push((Arc::from(key.as_str()), lower_expr(ctx, expr, aux)?));
                    }
                    MapProjectionSelector::AllProperties => unreachable!("handled above"),
                }
            }
            Ok(Expression::Map(entries))
        }
        Expr::PatternComprehension(pc) => {
            // A correlated subquery (like EXISTS): the inner pattern's existing
            // variables correlate via the shared registry, new ones bind inside.
            let mut patterns = lower_pattern(ctx, &pc.pattern)?;
            if let Some(cond) = &pc.filter {
                let mut filter_aux = Vec::new();
                let filter = lower_expr(ctx, cond, &mut filter_aux)?;
                patterns.extend(filter_aux);
                patterns.push(Pattern::Filter(filter));
            }
            // The projection's auxiliary patterns (e.g. `b.name` property
            // accessors) must run INSIDE the subquery so they resolve per match;
            // the projection expression then reads the resulting vars.
            let mut proj_aux = Vec::new();
            let projection = lower_expr(ctx, &pc.projection, &mut proj_aux)?;
            patterns.extend(proj_aux);
            Ok(Expression::PatternComprehension {
                patterns,
                projection: Box::new(projection),
            })
        }
        Expr::Call(call) => {
            let name = call.name.to_ascii_lowercase();
            let args: std::result::Result<Vec<_>, _> =
                call.args.iter().map(|a| lower_expr(ctx, a, aux)).collect();
            let args = args?;

            // Functions that remap onto an existing IR primitive with adjusted
            // arguments rather than a straight name → Function mapping.
            match name.as_str() {
                // Cypher `substring` is 0-indexed; SPARQL SUBSTR is 1-based.
                // Shift the start by +1 (works for the 2- and 3-arg forms).
                "substring" => return lower_substring(args),
                // A node/relationship's identity. Fluree has no integer id, so
                // `id(x)` returns its IRI string (its stable identity). Differs
                // from Neo4j's integer id; documented in docs/concepts/cypher.md.
                "id" | "elementid" => {
                    if args.len() != 1 {
                        return Err(LowerError::unsupported(
                            "id() takes exactly one argument",
                        ));
                    }
                    return Ok(Expression::call(Function::Str, args));
                }
                _ => {}
            }

            let func = match name.as_str() {
                "coalesce" => Function::Coalesce,
                "abs" => Function::Abs,
                // Cypher `length(p)` is a path's hop count; `size(x)` is the
                // list/string length (Cypher 9 split these).
                "length" => Function::PathLength,
                "size" => Function::Size,
                // List functions over `collect()` lists.
                "head" => Function::Head,
                "last" => Function::Last,
                "tail" => Function::Tail,
                "reverse" => Function::Reverse,
                "tostring" => Function::Str,
                // Cypher numeric casts (LDBC orders string ids numerically via
                // toInteger). Reuse the XSD cast functions.
                "tointeger" => Function::XsdInteger,
                "tofloat" => Function::XsdDouble,
                // Path / list builders.
                "nodes" => Function::Nodes,
                "range" => Function::Range,
                "pathpairs" => Function::PathPairs,
                "labels" => Function::Labels,
                "type" => Function::RelType,
                "startnode" => Function::StartNode,
                "endnode" => Function::EndNode,
                "keys" => Function::Keys,
                "properties" => Function::Properties,
                // Scalar string functions.
                "toupper" => Function::Ucase,
                "tolower" => Function::Lcase,
                "replace" => Function::ReplaceAll, // literal replace-all
                "split" => Function::Split,
                "trim" => Function::Trim,
                "ltrim" => Function::LTrim,
                "rtrim" => Function::RTrim,
                "left" => Function::Left,
                "right" => Function::Right,
                // Scalar math functions.
                "round" => Function::Round,
                "ceil" | "ceiling" => Function::Ceil,
                "floor" => Function::Floor,
                "rand" => Function::Rand,
                "sqrt" => Function::Sqrt,
                "sign" => Function::Sign,
                "log" => Function::Ln, // natural logarithm
                _ => {
                    return Err(LowerError::unsupported(format!(
                        "function `{}` is not in the v1 expression surface",
                        call.name
                    )));
                }
            };
            Ok(Expression::call(func, args))
        }
    }
}

/// Lower Cypher `substring(s, start[, len])` (0-indexed) to the engine's
/// `Substr` (1-based) by shifting the start position by `+1`.
fn lower_substring(args: Vec<Expression>) -> Result<Expression> {
    if args.len() != 2 && args.len() != 3 {
        return Err(LowerError::unsupported(
            "substring() takes 2 or 3 arguments: substring(string, start[, length])",
        ));
    }
    let mut args = args;
    let start = args.remove(1);
    let one_based =
        Expression::binary(Function::Add, start, Expression::Const(FlakeValue::Long(1)));
    args.insert(1, one_based);
    Ok(Expression::call(Function::Substr, args))
}

/// Reject an `EXISTS { … }` anywhere inside a list-iteration expression. An
/// `EXISTS` there typically references the loop-local element and would need
/// row-local **async** subquery evaluation per element — but list iteration runs
/// in the synchronous per-element eval path, and the FilterOperator's EXISTS
/// pre-resolver doesn't (and shouldn't) descend into these scoped bodies. Reject
/// with a clear error rather than silently evaluating the `EXISTS` as false.
fn reject_exists_in_iteration(built: &Expression) -> Result<()> {
    if fluree_db_query::filter::contains_exists(built) {
        return Err(LowerError::unsupported(
            "EXISTS inside a list comprehension / reduce / list predicate is not supported — \
             it would need per-element async subquery evaluation",
        ));
    }
    Ok(())
}

/// Map the Cypher list-predicate kind to the query IR kind.
fn lower_list_predicate_kind(kind: ListPredicateKind) -> IrListPredicateKind {
    match kind {
        ListPredicateKind::All => IrListPredicateKind::All,
        ListPredicateKind::Any => IrListPredicateKind::Any,
        ListPredicateKind::None => IrListPredicateKind::None,
        ListPredicateKind::Single => IrListPredicateKind::Single,
    }
}

/// Map a temporal-component accessor name to its extraction function.
/// `<date>.month` / `<datetime>.year` etc. mirror SPARQL's YEAR/MONTH/DAY.
fn temporal_field_function(key: &str) -> Option<Function> {
    match key.to_ascii_lowercase().as_str() {
        "year" => Some(Function::Year),
        "month" => Some(Function::Month),
        "day" => Some(Function::Day),
        "hour" => Some(Function::Hours),
        "minute" => Some(Function::Minutes),
        "second" => Some(Function::Seconds),
        _ => None,
    }
}

/// Resolve a Cypher `target.key` property accessor to a VarId.
///
/// Emits `Pattern::Optional([Triple(target, <key IRI>, ?#__prop_target_key)])`
/// into `aux`. The **optional** wrap matches Cypher's nullable
/// property-access semantics: when the target has no value for the
/// key, the accessor evaluates to null and the row still flows
/// through the query, not filtered out by a mandatory join.
///
/// This makes the following work as Cypher users expect:
///
/// - `WHERE n.missing IS NULL` returns nodes lacking the property
///   (the property var is unbound; `IS NULL` evaluates true).
/// - `RETURN n.name` for a sparse property returns one row per
///   matched node, with `null` where the property is absent.
/// - `avg(n.age)` averages across nodes that have age, skipping
///   nulls — the aggregate's natural unbound-input behavior.
/// - `RETURN n.dept, count(*)` groups by dept (with a "null"
///   group for nodes without one).
/// - `WHERE n.age > 30` continues to filter to age-bearing nodes
///   above 30: the `>` comparison on an unbound binding yields a
///   filter-context error → effective boolean false → row excluded.
///   Same end result as the previous mandatory-join behavior.
///
/// Why always-emit rather than dedup at lower time: subquery
/// boundaries (`WITH`) can drop the property variable from the
/// outer scope if it isn't in the WITH's select list. A naive
/// "have we already emitted this name?" check would skip the
/// re-emit in the outer scope, leaving the property var unbound.
/// Re-emitting is correct and the planner handles redundant
/// Optionals cheaply.
///
/// v1 only accepts a bare-variable target (`n.prop`); chained
/// accessors (`n.address.city`) and accessors on non-variable
/// expressions (e.g., `(n {p:1}).p`) are rejected.
pub(crate) fn resolve_property_accessor<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    target: &Expr,
    key: &str,
    aux: &mut Vec<Pattern>,
) -> Result<VarId> {
    let target_var = match target {
        Expr::Var(v) => v,
        Expr::Prop(_, _, _) => {
            return Err(LowerError::unsupported(
                "chained property accessors (`n.foo.bar`) are deferred — bind to an intermediate variable via WITH",
            ));
        }
        _ => {
            return Err(LowerError::unsupported(
                "property accessors require a bare-variable target in v1 (e.g., `n.prop`)",
            ));
        }
    };
    let target_id = ctx.intern_var(&target_var.name);
    let pred_iri = ctx.resolve_predicate(key)?;

    let prop_var_name = format!("?#__prop_{}_{}", target_var.name, key);
    let prop_var = ctx.intern_var(&prop_var_name);

    aux.push(Pattern::Optional(vec![Pattern::Triple(
        TriplePattern::new(
            Ref::Var(target_id),
            Ref::Iri(pred_iri.into()),
            Term::Var(prop_var),
        ),
    )]));
    Ok(prop_var)
}

/// Lower a Cypher `CASE` expression to nested `Function::If` calls.
///
/// Cypher has two forms:
///   `CASE WHEN cond THEN val [...] [ELSE val] END`              (simple)
///   `CASE subj WHEN cand THEN val [...] [ELSE val] END`         (subject)
///
/// In the subject form, each `WHEN cand` desugars to `subj = cand`.
/// Both forms then lower to a right-folded `If(c1, v1, If(c2, v2, ... else))`.
fn lower_case<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    case: &CaseExpr,
    aux: &mut Vec<Pattern>,
) -> Result<Expression> {
    if case.branches.is_empty() {
        return Err(LowerError::unsupported(
            "CASE requires at least one WHEN branch",
        ));
    }

    // The final ELSE — Cypher omits it as implicit NULL; we surface that
    // as Bound→false via Function::Coalesce with zero remaining args
    // (an empty Coalesce returns unbound).
    let else_expr = match &case.else_branch {
        Some(e) => lower_expr(ctx, e, aux)?,
        None => Expression::call(Function::Coalesce, Vec::new()),
    };

    // The subject expression, if any, lowered once and reused per branch
    // wrapped in equality.
    let subject = match &case.subject {
        Some(s) => Some(lower_expr(ctx, s, aux)?),
        None => None,
    };

    // Right-fold over branches.
    let mut acc = else_expr;
    for (cond, val) in case.branches.iter().rev() {
        let cond_expr = lower_expr(ctx, cond, aux)?;
        let val_expr = lower_expr(ctx, val, aux)?;
        let test = match &subject {
            Some(subj) => Expression::binary(Function::Eq, subj.clone(), cond_expr),
            None => cond_expr,
        };
        acc = Expression::call(Function::If, vec![test, val_expr, acc]);
    }
    Ok(acc)
}

pub fn lower_literal(lit: &Literal) -> Result<FlakeValue> {
    Ok(match lit {
        Literal::Integer(n, _) => FlakeValue::Long(*n),
        Literal::Float(f, _) => FlakeValue::Double(*f),
        Literal::String(s, _) => FlakeValue::String(s.clone()),
        Literal::Bool(b, _) => FlakeValue::Boolean(*b),
        Literal::Null(_) => {
            return Err(LowerError::unsupported(
                "NULL literals in lowered expressions are deferred",
            ));
        }
    })
}

// Silence unused-import lints for ParamRef which we keep for future
// non-error wiring.
#[allow(dead_code)]
fn _retain_paramref(_p: &ParamRef) {}
