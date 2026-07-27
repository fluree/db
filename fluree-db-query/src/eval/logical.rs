//! Logical operator implementations
//!
//! Implements logical operators: AND, OR, NOT

use super::compare::{rdf_term_equal, EqOutcome};
use super::value::ComparableValue;
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
            let mut found = false;
            for v in &args[1..] {
                match v.eval_to_comparable(row, ctx) {
                    // Value equality (rdf_term_equal), so `1 IN (1.0)` matches
                    // like `1 = 1.0` — not the variant-exact derived `==`. The
                    // §17.4.1.9 error-in-IN 3-valued half is deferred (Ne and a
                    // type error both read as "no match" here).
                    Ok(Some(cv)) if matches!(rdf_term_equal(&cv, &tv), EqOutcome::Eq) => {
                        found = true;
                        break;
                    }
                    Ok(Some(_) | None) => {}
                    Err(err) if err.can_demote_in_expression() => {}
                    Err(err) => return Err(err),
                }
            }
            Ok(Some(ComparableValue::Bool(found)))
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
            let mut found = false;
            for v in &args[1..] {
                match v.eval_to_comparable(row, ctx) {
                    // Value equality (rdf_term_equal), so `1 IN (1.0)` matches
                    // like `1 = 1.0` — not the variant-exact derived `==`. The
                    // §17.4.1.9 error-in-IN 3-valued half is deferred (Ne and a
                    // type error both read as "no match" here).
                    Ok(Some(cv)) if matches!(rdf_term_equal(&cv, &tv), EqOutcome::Eq) => {
                        found = true;
                        break;
                    }
                    Ok(Some(_) | None) => {}
                    Err(err) if err.can_demote_in_expression() => {}
                    Err(err) => return Err(err),
                }
            }
            Ok(Some(ComparableValue::Bool(!found)))
        }
        None => Ok(Some(ComparableValue::Bool(true))), // Unbound value -> not in list (vacuously true)
    }
}
