//! Numeric function implementations
//!
//! Implements SPARQL numeric functions: ABS, ROUND, CEIL, FLOOR, RAND

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;
use bigdecimal::{BigDecimal, RoundingMode};
use rand::random;

use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_abs<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "ABS")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n.abs()))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.abs()))),
        Some(ComparableValue::Decimal(d)) => Ok(Some(ComparableValue::Decimal(Box::new(d.abs())))),
        Some(ComparableValue::BigInt(n)) => Ok(Some(ComparableValue::BigInt(Box::new(
            n.magnitude().clone().into(),
        )))),
        None => Ok(None),
        Some(_) => Ok(None),
    }
}

pub fn eval_round<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "ROUND")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => {
            // W3C: round half toward positive infinity (not away from zero).
            // f64::round() rounds half away from zero, which is wrong for
            // negative values (e.g., -2.5 → -3 instead of -2).
            Ok(Some(ComparableValue::Double((d + 0.5).floor())))
        }
        Some(ComparableValue::Decimal(d)) => {
            // W3C: round half toward positive infinity.
            // RoundingMode::HalfUp rounds half away from zero, which is wrong
            // for negative values. Instead: add 0.5 then floor.
            let half = BigDecimal::new(5.into(), 1); // 0.5
            let rounded = (&*d + &half).with_scale_round(0, RoundingMode::Floor);
            Ok(Some(ComparableValue::Decimal(Box::new(rounded))))
        }
        None => Ok(None),
        Some(_) => Ok(None),
    }
}

pub fn eval_ceil<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "CEIL")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.ceil()))),
        Some(ComparableValue::Decimal(d)) => {
            let ceiled = d.with_scale_round(0, RoundingMode::Ceiling);
            Ok(Some(ComparableValue::Decimal(Box::new(ceiled))))
        }
        None => Ok(None),
        Some(_) => Ok(None),
    }
}

pub fn eval_floor<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "FLOOR")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.floor()))),
        Some(ComparableValue::Decimal(d)) => {
            let floored = d.with_scale_round(0, RoundingMode::Floor);
            Ok(Some(ComparableValue::Decimal(Box::new(floored))))
        }
        None => Ok(None),
        Some(_) => Ok(None),
    }
}

pub fn eval_rand(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "RAND")?;
    Ok(Some(ComparableValue::Double(random::<f64>())))
}
