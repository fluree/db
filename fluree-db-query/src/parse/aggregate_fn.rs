//! Aggregate-function name recognition shared across SELECT s-expression,
//! data-form filter (B1), and string-form filter (B2) parsers.
//!
//! Each surface that parses aggregates (`(as (count ?x) ?cnt)` in SELECT,
//! `["min", "?p"]` inside a BIND/FILTER expression, `"(min ?p)"` inside a
//! BIND/FILTER expression) shares the same set of recognized function names
//! and the same arity/input-shape rules. Centralizing the mapping here
//! keeps them in lockstep — adding a new aggregate (e.g. `MEDIAN_DISTINCT`)
//! or tightening a validation rule happens in one place.
//!
//! The two kinds of input args are kept separate so callers can pre-shape
//! their surface form before calling:
//! - `input` is the first positional argument: a variable name (`"?p"`) or
//!   the literal `"*"` (only valid for COUNT). All surfaces extract this
//!   the same way.
//! - `extra_args` carries any further positional arguments. Only
//!   GROUP_CONCAT consumes one (the separator); every other aggregate
//!   rejects non-empty `extra_args` with a clear arity error.
//!
//! Returning `Option` lets `dispatch_filter_op` express "this op-name might
//! be an aggregate" with a single guard before committing to error-path or
//! non-aggregate-fallback branches.

use super::ast::{
    UnresolvedAggregateFn, UnresolvedAggregateSpec, UnresolvedExpression, UnresolvedFilterValue,
};
use super::error::{ParseError, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Returns true if `name` (case-sensitive — caller normalizes if needed) is a
/// recognized SPARQL/JSON-LD aggregate function.
pub(crate) fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "count-distinct"
            | "countdistinct"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "median"
            | "variance"
            | "stddev"
            | "sample"
            | "group-concat"
            | "groupconcat"
    )
}

/// Map a recognized aggregate name + its input arg + extra args to an
/// `UnresolvedAggregateFn` and the input var name (Arc-shared).
///
/// `name` is matched case-sensitively; the caller is responsible for
/// lowercasing if needed (filter_data + filter_sexpr both lowercase op
/// names before calling, matching legacy SELECT-s-expr behavior).
///
/// Returns `Ok(None)` if `name` is not a recognized aggregate — caller
/// should treat as a regular function call. Returns `Err` if `name` is
/// recognized but the arg shape is invalid (wrong arity for non-GC,
/// COUNT(*) misused, etc.).
pub(crate) fn try_parse_aggregate_call(
    name: &str,
    input: &str,
    extra_args: &[&str],
) -> Result<Option<(UnresolvedAggregateFn, Arc<str>)>> {
    if !is_aggregate_name(name) {
        return Ok(None);
    }

    if input != "*" && !input.starts_with('?') {
        return Err(ParseError::InvalidSelect(format!(
            "aggregate '{name}' input must be a variable or '*', got: {input}"
        )));
    }

    let function = match name {
        "group-concat" | "groupconcat" => {
            // GROUP_CONCAT is the only aggregate that accepts a separator.
            // Default is " " per SPARQL spec when omitted.
            if extra_args.len() > 1 {
                return Err(ParseError::InvalidSelect(format!(
                    "aggregate '{name}' accepts at most 2 arguments, got {} (expected: ({name} ?x) or ({name} ?x \", \"))",
                    extra_args.len() + 1
                )));
            }
            let separator = extra_args
                .first()
                .map(|s| (*s).to_string())
                .unwrap_or_else(|| " ".to_string());
            UnresolvedAggregateFn::GroupConcat { separator }
        }
        _ => {
            if !extra_args.is_empty() {
                return Err(ParseError::InvalidSelect(format!(
                    "aggregate '{name}' requires exactly 1 argument, got {} extra (expected: ({name} ?x))",
                    extra_args.len() + 1
                )));
            }
            match name {
                "count" => UnresolvedAggregateFn::Count,
                "count-distinct" | "countdistinct" => UnresolvedAggregateFn::CountDistinct,
                "sum" => UnresolvedAggregateFn::Sum,
                "avg" => UnresolvedAggregateFn::Avg,
                "min" => UnresolvedAggregateFn::Min,
                "max" => UnresolvedAggregateFn::Max,
                "median" => UnresolvedAggregateFn::Median,
                "variance" => UnresolvedAggregateFn::Variance,
                "stddev" => UnresolvedAggregateFn::Stddev,
                "sample" => UnresolvedAggregateFn::Sample,
                _ => unreachable!("name guard handled by is_aggregate_name"),
            }
        }
    };

    if input == "*" && !matches!(function, UnresolvedAggregateFn::Count) {
        return Err(ParseError::InvalidSelect(format!(
            "'*' can only be used with count, not {name}"
        )));
    }

    Ok(Some((function, Arc::from(input))))
}

/// Build the dedup key used by `rewrite_aggregates_in_expr` to identify
/// equivalent aggregate calls across an expression tree.
///
/// Two inline aggregates with the same function, input, distinct flag, and
/// (for GROUP_CONCAT) separator can share a single `UnresolvedAggregateSpec`
/// — the engine computes them once and the synthetic alias is reused.
fn aggregate_dedup_key(function: &UnresolvedAggregateFn, input: &str, distinct: bool) -> String {
    let (tag, separator) = match function {
        UnresolvedAggregateFn::Count => ("count", ""),
        UnresolvedAggregateFn::CountDistinct => ("count-distinct", ""),
        UnresolvedAggregateFn::Sum => ("sum", ""),
        UnresolvedAggregateFn::Avg => ("avg", ""),
        UnresolvedAggregateFn::Min => ("min", ""),
        UnresolvedAggregateFn::Max => ("max", ""),
        UnresolvedAggregateFn::Median => ("median", ""),
        UnresolvedAggregateFn::Variance => ("variance", ""),
        UnresolvedAggregateFn::Stddev => ("stddev", ""),
        UnresolvedAggregateFn::Sample => ("sample", ""),
        UnresolvedAggregateFn::GroupConcat { separator } => ("group-concat", separator.as_str()),
    };
    format!("{tag}|{input}|{distinct}|{separator}")
}

/// Walk an expression tree and hoist every recognized inline aggregate call
/// (e.g. `Call("min", ["?p"])`) into `aggregates`, replacing each one with
/// a `Var(synthetic_alias)` reference. Equivalent calls (same function +
/// input + separator) are deduplicated through `alias_dedup` and share a
/// single output variable.
///
/// Used by both:
/// - HAVING expressions (`?__having_agg{N}` aliases) — preserves the
///   pre-existing JSON-LD HAVING semantics, but expanded from the previous
///   5-aggregate hard-coded set (count/sum/avg/min/max) to the full
///   `is_aggregate_name` set with input-shape validation.
/// - BIND expressions in WHERE (`?__bind_agg{N}` aliases) — JSON-LD's way
///   of expressing what SPARQL writes as a SELECT projection expression
///   (e.g. `((MIN(?p) + MAX(?p)) / 2 AS ?c)`).
///
/// Per W3C SPARQL §18.5: aggregate inputs must be a variable or `"*"`; nested
/// aggregates `MIN(MAX(?p))` are rejected (the inner aggregate's `Call` is
/// rejected as an invalid aggregate input rather than recursively rewritten,
/// matching the existing HAVING behaviour).
pub(crate) fn rewrite_aggregates_in_expr(
    expr: UnresolvedExpression,
    aggregates: &mut Vec<UnresolvedAggregateSpec>,
    alias_dedup: &mut HashMap<String, Arc<str>>,
    counter: &mut usize,
    synthetic_prefix: &str,
    context_label: &str,
) -> Result<UnresolvedExpression> {
    use UnresolvedExpression as E;

    let rewrite_child = |e: UnresolvedExpression,
                         aggregates: &mut Vec<UnresolvedAggregateSpec>,
                         alias_dedup: &mut HashMap<String, Arc<str>>,
                         counter: &mut usize|
     -> Result<UnresolvedExpression> {
        rewrite_aggregates_in_expr(
            e,
            aggregates,
            alias_dedup,
            counter,
            synthetic_prefix,
            context_label,
        )
    };

    match expr {
        E::Var(_) | E::Const(_) => Ok(expr),

        E::And(exprs) => Ok(E::And(
            exprs
                .into_iter()
                .map(|e| rewrite_child(e, aggregates, alias_dedup, counter))
                .collect::<Result<Vec<_>>>()?,
        )),

        E::Or(exprs) => Ok(E::Or(
            exprs
                .into_iter()
                .map(|e| rewrite_child(e, aggregates, alias_dedup, counter))
                .collect::<Result<Vec<_>>>()?,
        )),

        E::Not(inner) => Ok(E::Not(Box::new(rewrite_child(
            *inner,
            aggregates,
            alias_dedup,
            counter,
        )?))),

        E::In {
            expr,
            values,
            negated,
        } => Ok(E::In {
            expr: Box::new(rewrite_child(*expr, aggregates, alias_dedup, counter)?),
            values: values
                .into_iter()
                .map(|e| rewrite_child(e, aggregates, alias_dedup, counter))
                .collect::<Result<Vec<_>>>()?,
            negated,
        }),

        E::Call { func, args } => {
            let name_lc = func.as_ref().to_ascii_lowercase();

            if !is_aggregate_name(&name_lc) {
                // Non-aggregate function: rewrite its arguments only.
                let rewritten_args = args
                    .into_iter()
                    .map(|e| rewrite_child(e, aggregates, alias_dedup, counter))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(E::Call {
                    func,
                    args: rewritten_args,
                });
            }

            // Aggregate call: validate input shape and (optional GROUP_CONCAT)
            // separator before recursing into args. Crucially, we do NOT
            // recurse into args of an aggregate — nested aggregates are
            // forbidden per SPARQL §18.5, and a nested aggregate would
            // surface here as a non-Var arg, triggering the input-shape
            // error below.
            if args.is_empty() {
                return Err(ParseError::InvalidFilter(format!(
                    "{context_label} aggregate '{func}' requires at least one argument"
                )));
            }

            let input_str = match &args[0] {
                E::Var(v) => v.as_ref().to_string(),
                E::Const(UnresolvedFilterValue::String(s)) if s.as_ref() == "*" => "*".to_string(),
                _ => {
                    return Err(ParseError::InvalidFilter(format!(
                        "{context_label} aggregate '{func}' input must be a variable or '*' (nested aggregates and computed inputs are not allowed)"
                    )));
                }
            };

            // GROUP_CONCAT permits an optional second arg as the separator
            // string literal. All other aggregates reject extra args.
            let mut extra_strings: Vec<String> = Vec::new();
            for arg in args.iter().skip(1) {
                match arg {
                    E::Const(UnresolvedFilterValue::String(s)) => {
                        extra_strings.push(s.as_ref().to_string());
                    }
                    _ => {
                        return Err(ParseError::InvalidFilter(format!(
                            "{context_label} aggregate '{func}' extra argument must be a string literal"
                        )));
                    }
                }
            }
            let extra_refs: Vec<&str> = extra_strings.iter().map(String::as_str).collect();

            let (function, input_arc) =
                try_parse_aggregate_call(&name_lc, &input_str, &extra_refs)?
                    .expect("name guarded by is_aggregate_name");

            let key = aggregate_dedup_key(&function, &input_arc, false);
            let output_var = if let Some(existing) = alias_dedup.get(&key) {
                Arc::clone(existing)
            } else {
                let out: Arc<str> = Arc::from(format!("{synthetic_prefix}{}", *counter));
                *counter += 1;
                aggregates.push(UnresolvedAggregateSpec {
                    function,
                    input_var: input_arc,
                    output_var: Arc::clone(&out),
                });
                alias_dedup.insert(key, Arc::clone(&out));
                out
            };
            Ok(E::Var(output_var))
        }

        // EXISTS/NOT EXISTS makes no sense in aggregate-bearing contexts.
        E::Exists { .. } => Err(ParseError::InvalidFilter(format!(
            "EXISTS/NOT EXISTS not supported in {context_label} expressions"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_basic_aggregates() {
        assert!(is_aggregate_name("count"));
        assert!(is_aggregate_name("sum"));
        assert!(is_aggregate_name("avg"));
        assert!(is_aggregate_name("min"));
        assert!(is_aggregate_name("max"));
        assert!(is_aggregate_name("group-concat"));
        assert!(is_aggregate_name("groupconcat"));
        assert!(is_aggregate_name("count-distinct"));
    }

    #[test]
    fn rejects_non_aggregates() {
        assert!(!is_aggregate_name("strlen"));
        assert!(!is_aggregate_name("+"));
        assert!(!is_aggregate_name("if"));
        assert!(!is_aggregate_name(""));
    }

    #[test]
    fn parses_simple_count() {
        let (f, input) = try_parse_aggregate_call("count", "?x", &[])
            .expect("ok")
            .expect("recognized");
        assert!(matches!(f, UnresolvedAggregateFn::Count));
        assert_eq!(input.as_ref(), "?x");
    }

    #[test]
    fn parses_count_star() {
        let (f, input) = try_parse_aggregate_call("count", "*", &[])
            .expect("ok")
            .expect("recognized");
        assert!(matches!(f, UnresolvedAggregateFn::Count));
        assert_eq!(input.as_ref(), "*");
    }

    #[test]
    fn rejects_star_for_non_count() {
        assert!(try_parse_aggregate_call("sum", "*", &[]).is_err());
        assert!(try_parse_aggregate_call("min", "*", &[]).is_err());
    }

    #[test]
    fn parses_groupconcat_with_separator() {
        let (f, input) = try_parse_aggregate_call("group-concat", "?x", &[", "])
            .expect("ok")
            .expect("recognized");
        match f {
            UnresolvedAggregateFn::GroupConcat { separator } => assert_eq!(separator, ", "),
            _ => panic!("expected GroupConcat"),
        }
        assert_eq!(input.as_ref(), "?x");
    }

    #[test]
    fn parses_groupconcat_default_separator() {
        let (f, _) = try_parse_aggregate_call("groupconcat", "?x", &[])
            .expect("ok")
            .expect("recognized");
        match f {
            UnresolvedAggregateFn::GroupConcat { separator } => assert_eq!(separator, " "),
            _ => panic!("expected GroupConcat"),
        }
    }

    #[test]
    fn rejects_groupconcat_too_many_args() {
        assert!(try_parse_aggregate_call("group-concat", "?x", &[", ", "extra"]).is_err());
    }

    #[test]
    fn rejects_extra_args_for_non_groupconcat() {
        assert!(try_parse_aggregate_call("sum", "?x", &["extra"]).is_err());
    }

    #[test]
    fn rejects_invalid_input() {
        assert!(try_parse_aggregate_call("count", "x", &[]).is_err());
        assert!(try_parse_aggregate_call("sum", "literal", &[]).is_err());
    }

    #[test]
    fn unknown_name_returns_none() {
        assert!(try_parse_aggregate_call("strlen", "?x", &[])
            .unwrap()
            .is_none());
        assert!(try_parse_aggregate_call("+", "?x", &[]).unwrap().is_none());
    }
}
