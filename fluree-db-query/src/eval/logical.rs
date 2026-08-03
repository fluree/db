//! Logical operator implementations
//!
//! Implements logical operators: AND, OR, NOT

use super::compare::{rdf_term_equal, EqOutcome};
use super::value::{ComparableValue, ComparisonError};
use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;

/// Evaluate logical AND with SPARQL 1.1 §17.2 three-valued semantics: `false`
/// dominates (`false && error = false`), otherwise a demotable operand error
/// makes the whole conjunction an error rather than aborting on the first one.
pub fn eval_and<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let mut pending_error: Option<QueryError> = None;
    for arg in args {
        match arg.eval_to_bool(row, ctx) {
            Ok(false) => return Ok(Some(ComparableValue::Bool(false))),
            Ok(true) => {}
            Err(e) if e.can_demote_in_expression() => pending_error = Some(e),
            Err(e) => return Err(e),
        }
    }
    match pending_error {
        Some(e) => Err(e),
        None => Ok(Some(ComparableValue::Bool(true))),
    }
}

/// Evaluate logical OR with SPARQL 1.1 §17.2 three-valued semantics: `true`
/// dominates (`true || error = true`), otherwise a demotable operand error
/// makes the whole disjunction an error rather than aborting on the first one
/// (open-cmp-02: `?a < ?b || ?a = ?b || ?a > ?b` on unorderable terms).
pub fn eval_or<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let mut pending_error: Option<QueryError> = None;
    for arg in args {
        match arg.eval_to_bool(row, ctx) {
            Ok(true) => return Ok(Some(ComparableValue::Bool(true))),
            Ok(false) => {}
            Err(e) if e.can_demote_in_expression() => pending_error = Some(e),
            Err(e) => return Err(e),
        }
    }
    match pending_error {
        Some(e) => Err(e),
        None => Ok(Some(ComparableValue::Bool(false))),
    }
}

/// Evaluate logical NOT
///
/// Returns the logical negation of the single argument.
pub fn eval_not<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(true))); // NOT of nothing is true
    }
    let result = args[0].eval_to_bool(row, ctx)?;
    Ok(Some(ComparableValue::Bool(!result)))
}

/// Evaluate logical XOR (Cypher `XOR`).
///
/// Two-valued: `bool(a) ^ bool(b)`, folded left-to-right over all arguments.
/// This reproduces exactly the `(a OR b) AND NOT(a AND b)` truthiness form it
/// replaces — without the exponential AST blow-up of structural desugaring.
pub fn eval_xor<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let mut acc = false;
    for arg in args {
        acc ^= arg.eval_to_bool(row, ctx)?;
    }
    Ok(Some(ComparableValue::Bool(acc)))
}

/// Evaluate IN expression
///
/// First argument is the test value, remaining arguments are the set values.
/// Returns true if test value equals any set value.
pub fn eval_in<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(false)));
    }

    let test_val = args[0].eval_to_comparable(row, ctx)?;
    match test_val {
        Some(tv) => {
            // §17.4.1.9: `E IN (E1..En)` ≡ `(E=E1) || … || (E=En)` under OR
            // three-valued logic — `true` if any element matches by value
            // equality; else an error if any comparison errored; else `false`.
            // A definitive match short-circuits and discards any pending error.
            let mut found = false;
            let mut pending_error: Option<QueryError> = None;
            for v in &args[1..] {
                match v.eval_to_comparable(row, ctx) {
                    // Value equality (rdf_term_equal), so `1 IN (1.0)` matches
                    // like `1 = 1.0` — not the variant-exact derived `==`.
                    Ok(Some(cv)) => match rdf_term_equal(&cv, &tv) {
                        EqOutcome::Eq => {
                            found = true;
                            break;
                        }
                        EqOutcome::Ne => {}
                        // Incomparable datatypes → the same `Comparison` type
                        // error `=`/`!=` raise (demotes to unbound in Extend /
                        // false in FILTER); kept pending so a later match wins.
                        EqOutcome::TypeError => {
                            pending_error = Some(
                                ComparisonError::TypeMismatch {
                                    operator: "IN",
                                    left_type: tv.type_name(),
                                    right_type: cv.type_name(),
                                }
                                .into(),
                            );
                        }
                    },
                    // Unbound element = no match (matches `E = unbound` = false).
                    Ok(None) => {}
                    // A demotable element-eval error (e.g. `1/0`) is kept pending,
                    // not swallowed, so a no-match result becomes an error.
                    Err(err) if err.can_demote_in_expression() => pending_error = Some(err),
                    // Fatal (dict / fuel / cancel) propagates.
                    Err(err) => return Err(err),
                }
            }
            if found {
                Ok(Some(ComparableValue::Bool(true)))
            } else if let Some(err) = pending_error {
                Err(err)
            } else {
                Ok(Some(ComparableValue::Bool(false)))
            }
        }
        None => Ok(Some(ComparableValue::Bool(false))), // Unbound value -> not in list
    }
}

/// Evaluate NOT IN expression
///
/// First argument is the test value, remaining arguments are the set values.
/// Returns true if test value does not equal any set value.
pub fn eval_not_in<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(true)));
    }

    let test_val = args[0].eval_to_comparable(row, ctx)?;
    match test_val {
        Some(tv) => {
            // `E NOT IN L` ≡ `NOT(E IN L)`: `false` if any element matches, else
            // an error if any comparison errored (NOT of error is error), else
            // `true`. A definitive match short-circuits and discards the error.
            let mut found = false;
            let mut pending_error: Option<QueryError> = None;
            for v in &args[1..] {
                match v.eval_to_comparable(row, ctx) {
                    // Value equality (rdf_term_equal), so `1 IN (1.0)` matches
                    // like `1 = 1.0` — not the variant-exact derived `==`.
                    Ok(Some(cv)) => match rdf_term_equal(&cv, &tv) {
                        EqOutcome::Eq => {
                            found = true;
                            break;
                        }
                        EqOutcome::Ne => {}
                        // Incomparable datatypes → the same `Comparison` type
                        // error `=`/`!=` raise (demotes to unbound in Extend /
                        // false in FILTER); kept pending so a later match wins.
                        EqOutcome::TypeError => {
                            pending_error = Some(
                                ComparisonError::TypeMismatch {
                                    operator: "NOT IN",
                                    left_type: tv.type_name(),
                                    right_type: cv.type_name(),
                                }
                                .into(),
                            );
                        }
                    },
                    // Unbound element = no match (matches `E = unbound` = false).
                    Ok(None) => {}
                    // A demotable element-eval error (e.g. `1/0`) is kept pending,
                    // not swallowed, so a no-match result becomes an error.
                    Err(err) if err.can_demote_in_expression() => pending_error = Some(err),
                    // Fatal (dict / fuel / cancel) propagates.
                    Err(err) => return Err(err),
                }
            }
            if found {
                Ok(Some(ComparableValue::Bool(false)))
            } else if let Some(err) = pending_error {
                Err(err)
            } else {
                Ok(Some(ComparableValue::Bool(true)))
            }
        }
        None => Ok(Some(ComparableValue::Bool(true))), // Unbound value -> not in list (vacuously true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::BindingRow;
    use fluree_db_core::value::FlakeValue;
    use std::sync::Arc;

    fn long(v: i64) -> Expression {
        Expression::Const(FlakeValue::Long(v))
    }

    /// `1 / 0` — a demotable Arithmetic (divide-by-zero) element error.
    fn div_by_zero() -> Expression {
        Expression::div(long(1), long(0))
    }

    /// A vector constant: value-incomparable with a number, so `rdf_term_equal`
    /// returns `TypeError` (the primary swallow site) rather than `Ne`.
    fn vector() -> Expression {
        Expression::Const(FlakeValue::Vector(Arc::from(vec![1.0_f64])))
    }

    fn run_in(args: Vec<Expression>) -> Result<Option<ComparableValue>> {
        let row = BindingRow::new(&[], &[]);
        eval_in(&args, &row, None)
    }

    fn run_not_in(args: Vec<Expression>) -> Result<Option<ComparableValue>> {
        let row = BindingRow::new(&[], &[]);
        eval_not_in(&args, &row, None)
    }

    #[test]
    fn in_no_match_no_error_is_false() {
        // 1 IN (2, 3) → false (unchanged).
        assert_eq!(
            run_in(vec![long(1), long(2), long(3)]).unwrap(),
            Some(ComparableValue::Bool(false))
        );
    }

    #[test]
    fn in_match_is_true() {
        // 1 IN (2, 1) → true (unchanged).
        assert_eq!(
            run_in(vec![long(1), long(2), long(1)]).unwrap(),
            Some(ComparableValue::Bool(true))
        );
    }

    #[test]
    fn in_demotable_error_no_match_is_error() {
        // 1 IN (1/0) → error, not false: no match + a demotable eval error. The
        // propagated Arithmetic error also demotes to unbound in Extend.
        let err = run_in(vec![long(1), div_by_zero()]).unwrap_err();
        assert!(err.demotes_to_unbound_in_extend(), "{err:?}");
    }

    #[test]
    fn in_typeerror_no_match_is_comparison_error() {
        // 1 IN (<vector>) → error via EqOutcome::TypeError (the primary swallow
        // site). The synthesized error is `Comparison`, which demotes in Extend.
        let err = run_in(vec![long(1), vector()]).unwrap_err();
        assert!(matches!(err, QueryError::Comparison(_)), "{err:?}");
        assert!(err.demotes_to_unbound_in_extend(), "{err:?}");
    }

    #[test]
    fn in_match_after_error_is_true() {
        // 1 IN (1/0, 1) → true: a definitive match discards the pending error.
        assert_eq!(
            run_in(vec![long(1), div_by_zero(), long(1)]).unwrap(),
            Some(ComparableValue::Bool(true))
        );
    }

    #[test]
    fn not_in_no_match_no_error_is_true() {
        // 1 NOT IN (2, 3) → true (unchanged).
        assert_eq!(
            run_not_in(vec![long(1), long(2), long(3)]).unwrap(),
            Some(ComparableValue::Bool(true))
        );
    }

    #[test]
    fn not_in_match_is_false() {
        // 1 NOT IN (2, 1) → false (unchanged).
        assert_eq!(
            run_not_in(vec![long(1), long(2), long(1)]).unwrap(),
            Some(ComparableValue::Bool(false))
        );
    }

    #[test]
    fn not_in_error_no_match_is_error_not_true() {
        // 1 NOT IN (1/0) → error, not true: NOT of an error is an error.
        let err = run_not_in(vec![long(1), div_by_zero()]).unwrap_err();
        assert!(err.demotes_to_unbound_in_extend(), "{err:?}");
    }

    #[test]
    fn not_in_match_after_error_is_false() {
        // notin02-style: 2 NOT IN (1/0, 2) → false — the `2` match dominates and
        // discards the pending error.
        assert_eq!(
            run_not_in(vec![long(2), div_by_zero(), long(2)]).unwrap(),
            Some(ComparableValue::Bool(false))
        );
    }
}
