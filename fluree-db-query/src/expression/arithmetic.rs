//! Arithmetic operator implementations
//!
//! Implements arithmetic operators: Add, Sub, Mul, Div, Negate
//!
//! Arithmetic operations are variadic (left-fold):
//! - 1 arg → identity (return the value)
//! - 2+ args → sequential left-fold: `(a + b) + c`

use super::helpers::{check_arity, check_min_arity};
use super::value::ComparableValue;
use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{ArithmeticOp, Expression};

impl ArithmeticOp {
    /// Evaluate this arithmetic operation over variadic arguments (left-fold).
    ///
    /// - 1 arg → identity (return the value)
    /// - 2+ args → fold: `op(op(a, b), c)` etc.
    pub fn eval<R: RowAccess>(
        &self,
        args: &[Expression],
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Option<ComparableValue>> {
        check_min_arity(args, 1, &self.to_string())?;

        let first = match args[0].eval_to_comparable(row, ctx)? {
            Some(v) => v,
            None => return Ok(None),
        };

        args[1..].iter().try_fold(Some(first), |acc, arg| {
            let acc = match acc {
                Some(v) => v,
                None => return Ok(None),
            };
            let val = match arg.eval_to_comparable(row, ctx)? {
                Some(v) => v,
                None => return Ok(None),
            };
            Ok(Some(self.apply(acc, val)?))
        })
    }
}

/// Evaluate unary negation
pub fn eval_negate<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "Negate")?;

    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(-n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(-d))),
        Some(ComparableValue::BigInt(n)) => Ok(Some(ComparableValue::BigInt(Box::new(-(*n))))),
        Some(ComparableValue::Decimal(d)) => Ok(Some(ComparableValue::Decimal(Box::new(-(*d))))),
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "Negate requires a numeric argument".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::BindingRow;
    use crate::ir::FilterValue;

    fn long(v: i64) -> Expression {
        Expression::Const(FilterValue::Long(v))
    }

    fn empty_row() -> BindingRow<'static> {
        BindingRow::new(&[], &[])
    }

    #[test]
    fn test_add_three_args() {
        let row = empty_row();
        let args = vec![long(1), long(2), long(3)];
        let result = ArithmeticOp::Add.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(6)));
    }

    #[test]
    fn test_sub_three_args() {
        let row = empty_row();
        let args = vec![long(10), long(3), long(2)];
        let result = ArithmeticOp::Sub.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(5)));
    }

    #[test]
    fn test_mul_three_args() {
        let row = empty_row();
        let args = vec![long(2), long(3), long(4)];
        let result = ArithmeticOp::Mul.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(24)));
    }

    #[test]
    fn test_div_three_args() {
        let row = empty_row();
        let args = vec![long(24), long(4), long(3)];
        let result = ArithmeticOp::Div.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(2)));
    }

    #[test]
    fn test_add_single_arg_identity() {
        let row = empty_row();
        let args = vec![long(42)];
        let result = ArithmeticOp::Add.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(42)));
    }

    #[test]
    fn test_add_zero_args_error() {
        let row = empty_row();
        let args: Vec<Expression> = vec![];
        assert!(ArithmeticOp::Add.eval(&args, &row, None).is_err());
    }
}
