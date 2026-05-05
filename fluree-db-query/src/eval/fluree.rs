//! Fluree-specific function implementations
//!
//! Implements Fluree-specific functions: T (transaction time), OP (operation type).
//!
//! Both functions delegate to the central `Binding::t()` / `Binding::op()`
//! accessors so they handle every metadata-bearing variant uniformly:
//! `Lit`, `EncodedLit`, `Sid`, and `EncodedSid`. Adding a new variant
//! that carries history metadata only needs the accessor to learn about
//! it — these evaluators stay unchanged.
//!
//! `OP(?v)` returns a boolean (`true` = assert, `false` = retract) —
//! this matches the on-disk `Flake.op` representation and avoids a
//! per-row Arc allocation. Users compare with `true` / `false` rather
//! than `"assert"` / `"retract"` strings.

use crate::binding::RowAccess;
use crate::error::Result;
use crate::ir::Expression;

use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_t<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "T")?;
    if let Expression::Var(var_id) = &args[0] {
        if let Some(binding) = row.get(*var_id) {
            if let Some(t) = binding.t() {
                return Ok(Some(ComparableValue::Long(t)));
            }
        }
    }
    Ok(None)
}

pub fn eval_op<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "OP")?;
    if let Expression::Var(var_id) = &args[0] {
        if let Some(binding) = row.get(*var_id) {
            if let Some(op) = binding.op() {
                return Ok(Some(ComparableValue::Bool(op)));
            }
        }
    }
    Ok(None)
}
