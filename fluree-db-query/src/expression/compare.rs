//! Value comparison logic
//!
//! This module contains functions for comparing ComparableValues,
//! including numeric, temporal, and string comparisons.
//!
//! Comparison operations are variadic (chained pairwise):
//! - 1 arg → vacuously true
//! - 2+ args → every consecutive pair must satisfy the relation:
//!   `(< a b c)` means `a < b AND b < c`
//!
//! Type mismatches (incomparable types):
//! - `=` yields `false` (different types are not equal)
//! - `!=` yields `true` (different types are not equal)
//! - `<`, `<=`, `>`, `>=` yield a `ComparisonError::TypeMismatch` error

use crate::binding::Binding;
use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{CompareOp, Expression};
use fluree_db_core::FlakeValue;
use std::cmp::Ordering;

use super::helpers::check_min_arity;
use super::value::{ComparableValue, ComparisonError};

fn log_fastpath_hit_once(kind: &'static str) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    static HIT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !HIT.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::debug!(
            kind,
            "FILTER: used encoded-id equality fast path (logged once)"
        );
    }
}

fn fast_eq_ne_for_iri_bindings<R: RowAccess>(
    op: CompareOp,
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<bool>> {
    if !matches!(op, CompareOp::Eq | CompareOp::Ne) || args.len() != 2 {
        return Ok(None);
    }
    let Some(ctx) = ctx else {
        return Ok(None);
    };
    let Some(store) = ctx.binary_store.as_deref() else {
        return Ok(None);
    };

    let sid_to_iri = |sid: &fluree_db_core::Sid| {
        store
            .sid_to_iri(sid)
            .or_else(|| ctx.active_snapshot.decode_sid(sid))
    };
    let comparable_to_subject_iri = |value: ComparableValue| -> Option<String> {
        match value {
            ComparableValue::Sid(sid) => sid_to_iri(&sid),
            ComparableValue::Iri(iri) => Some(iri.to_string()),
            _ => None,
        }
    };

    let try_side = |var_expr: &Expression, other_expr: &Expression| -> Result<Option<bool>> {
        let Expression::Var(v) = var_expr else {
            return Ok(None);
        };
        let Some(binding) = row.get(*v) else {
            return Ok(Some(false));
        };

        let Some(other) = other_expr.eval_to_comparable(row, Some(ctx))? else {
            return Ok(Some(false));
        };

        match binding {
            Binding::EncodedSid { s_id, .. } => {
                let Some(lhs_iri) = ctx
                    .resolve_subject_iri(*s_id)
                    .transpose()
                    .map_err(|e| QueryError::Internal(format!("resolve_subject_iri: {e}")))?
                else {
                    return Ok(None);
                };
                let Some(rhs_iri) = comparable_to_subject_iri(other) else {
                    return Ok(None);
                };

                let eq = lhs_iri == rhs_iri;
                let out = match op {
                    CompareOp::Eq => eq,
                    CompareOp::Ne => !eq,
                    _ => unreachable!(),
                };
                log_fastpath_hit_once("EncodedSid");
                Ok(Some(out))
            }
            Binding::Sid { sid, .. } => {
                let eq = match other {
                    ComparableValue::Sid(rhs) => {
                        if sid == &rhs {
                            true
                        } else {
                            let (Some(lhs_iri), Some(rhs_iri)) =
                                (sid_to_iri(sid), sid_to_iri(&rhs))
                            else {
                                return Ok(None);
                            };
                            lhs_iri == rhs_iri
                        }
                    }
                    ComparableValue::Iri(iri) => {
                        let Some(lhs_iri) = sid_to_iri(sid) else {
                            return Ok(None);
                        };
                        lhs_iri == iri.as_ref()
                    }
                    _ => return Ok(None),
                };
                let out = match op {
                    CompareOp::Eq => eq,
                    CompareOp::Ne => !eq,
                    _ => unreachable!(),
                };
                log_fastpath_hit_once("Sid");
                Ok(Some(out))
            }
            Binding::Iri(iri) | Binding::IriMatch { iri, .. } => {
                let eq = match other {
                    ComparableValue::Sid(rhs) => {
                        let Some(rhs_iri) = sid_to_iri(&rhs) else {
                            return Ok(None);
                        };
                        iri.as_ref() == rhs_iri
                    }
                    ComparableValue::Iri(rhs) => iri.as_ref() == rhs.as_ref(),
                    _ => return Ok(None),
                };
                let out = match op {
                    CompareOp::Eq => eq,
                    CompareOp::Ne => !eq,
                    _ => unreachable!(),
                };
                log_fastpath_hit_once("Iri");
                Ok(Some(out))
            }
            Binding::EncodedPid { p_id } => {
                let rhs_p_id_opt = match other {
                    ComparableValue::Sid(sid) => store.sid_to_p_id(&sid),
                    ComparableValue::Iri(iri) => store.find_predicate_id(iri.as_ref()),
                    _ => return Ok(None),
                };

                let eq = rhs_p_id_opt.is_some_and(|rhs| rhs == *p_id);
                let out = match op {
                    CompareOp::Eq => eq,
                    CompareOp::Ne => !eq,
                    _ => unreachable!(),
                };
                log_fastpath_hit_once("EncodedPid");
                Ok(Some(out))
            }
            _ => Ok(None),
        }
    };

    // Try both orientations: (?var EncodedSid) op (const IRI/Sid) OR swapped.
    if let Some(v) = try_side(&args[0], &args[1])? {
        return Ok(Some(v));
    }
    if let Some(v) = try_side(&args[1], &args[0])? {
        return Ok(Some(v));
    }
    Ok(None)
}

impl CompareOp {
    /// Whether the given ordering satisfies this comparison operator.
    fn satisfies(self, ord: Ordering) -> bool {
        match self {
            CompareOp::Eq => ord == Ordering::Equal,
            CompareOp::Ne => ord != Ordering::Equal,
            CompareOp::Lt => ord == Ordering::Less,
            CompareOp::Le => ord != Ordering::Greater,
            CompareOp::Gt => ord == Ordering::Greater,
            CompareOp::Ge => ord != Ordering::Less,
        }
    }

    /// Evaluate this comparison over variadic arguments (chained pairwise).
    ///
    /// - 1 arg → vacuously true
    /// - 2+ args → checks every consecutive pair, short-circuiting on failure
    pub fn eval<R: RowAccess>(
        &self,
        args: &[Expression],
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Option<ComparableValue>> {
        check_min_arity(args, 1, &self.to_string())?;

        if let Some(b) = fast_eq_ne_for_iri_bindings(*self, args, row, ctx)? {
            return Ok(Some(ComparableValue::Bool(b)));
        }

        // 1 arg → vacuously true
        if args.len() == 1 {
            // Still evaluate the arg (may have side effects / unbound check)
            return match args[0].eval_to_comparable(row, ctx)? {
                Some(_) => Ok(Some(ComparableValue::Bool(true))),
                None => Ok(Some(ComparableValue::Bool(false))),
            };
        }

        let mut prev = match args[0].eval_to_comparable(row, ctx)? {
            Some(v) => v,
            None => return Ok(Some(ComparableValue::Bool(false))),
        };

        for arg in &args[1..] {
            let curr = match arg.eval_to_comparable(row, ctx)? {
                Some(v) => v,
                None => return Ok(Some(ComparableValue::Bool(false))),
            };

            let satisfied = match cmp_values(&prev, &curr) {
                Some(ord) => self.satisfies(ord),
                None => match self {
                    CompareOp::Eq => false,
                    CompareOp::Ne => true,
                    _ => {
                        return Err(ComparisonError::TypeMismatch {
                            operator: self.symbol(),
                            left_type: prev.type_name(),
                            right_type: curr.type_name(),
                        }
                        .into())
                    }
                },
            };
            if !satisfied {
                return Ok(Some(ComparableValue::Bool(false)));
            }

            prev = curr;
        }

        Ok(Some(ComparableValue::Bool(true)))
    }
}

/// Compare two values and return their ordering.
///
/// Returns `None` for type mismatches (incomparable types).
/// Delegates to FlakeValue's comparison methods for numeric and temporal types.
fn cmp_values(left: &ComparableValue, right: &ComparableValue) -> Option<Ordering> {
    let left_fv: FlakeValue = left.into();
    let right_fv: FlakeValue = right.into();

    // Try numeric comparison first (handles all numeric cross-type comparisons)
    if let Some(ordering) = left_fv.numeric_cmp(&right_fv) {
        return Some(ordering);
    }

    // Try temporal comparison (same-type temporal only)
    if let Some(ordering) = left_fv.temporal_cmp(&right_fv) {
        return Some(ordering);
    }

    // Cross-type coercion: when one side is a temporal type and the other
    // is a string, try to parse the string as that temporal type.
    if let Some(ordering) = try_coerce_temporal_string_cmp(&left_fv, &right_fv) {
        return Some(ordering);
    }

    // Fall back to same-type comparisons for non-numeric, non-temporal types
    match (left, right) {
        (ComparableValue::String(a), ComparableValue::String(b)) => Some(a.cmp(b)),
        (ComparableValue::Bool(a), ComparableValue::Bool(b)) => Some(a.cmp(b)),
        (ComparableValue::Sid(a), ComparableValue::Sid(b)) => Some(a.cmp(b)),
        (ComparableValue::Iri(a), ComparableValue::Iri(b)) => Some(a.cmp(b)),
        // Custom-datatype string literals: compare lexically only if both
        // sides share the same datatype constraint, per W3C SPARQL §17.4.1.2.
        // Different datatypes (or one TypedLiteral and one plain String)
        // yield `None` (type error / FILTER false) — exercised by W3C
        // `eq-4`, `eq-2-1`, `eq-2-2`.
        (
            ComparableValue::TypedLiteral { val: lv, dtc: ld },
            ComparableValue::TypedLiteral { val: rv, dtc: rd },
        ) if ld == rd => match (lv, rv) {
            (FlakeValue::String(a), FlakeValue::String(b)) => Some(a.cmp(b)),
            _ => None,
        },
        // Type mismatch
        _ => None,
    }
}

/// Try to compare a temporal FlakeValue against a String by parsing the
/// string as the matching temporal type. Returns `None` if neither side is
/// a string or parsing fails.
///
/// This handles values stored as LEX_ID (string dict entry) with a temporal
/// datatype annotation — the index stores the raw string but the FILTER
/// constant is a properly-typed temporal value.
fn try_coerce_temporal_string_cmp(left: &FlakeValue, right: &FlakeValue) -> Option<Ordering> {
    use fluree_db_core::temporal;

    match (left, right) {
        // String on left, temporal on right → parse left as temporal
        (FlakeValue::String(s), FlakeValue::GYear(g)) => temporal::GYear::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::GYearMonth(g)) => temporal::GYearMonth::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::GMonth(g)) => temporal::GMonth::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::GDay(g)) => temporal::GDay::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::GMonthDay(g)) => temporal::GMonthDay::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::DateTime(dt)) => temporal::DateTime::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(dt.as_ref())),
        (FlakeValue::String(s), FlakeValue::Date(d)) => temporal::Date::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(d.as_ref())),
        (FlakeValue::String(s), FlakeValue::Time(t)) => temporal::Time::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(t.as_ref())),
        // Temporal on left, string on right → parse right as temporal
        (FlakeValue::GYear(g), FlakeValue::String(s)) => temporal::GYear::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::GYearMonth(g), FlakeValue::String(s)) => temporal::GYearMonth::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::GMonth(g), FlakeValue::String(s)) => temporal::GMonth::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::GDay(g), FlakeValue::String(s)) => temporal::GDay::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::GMonthDay(g), FlakeValue::String(s)) => temporal::GMonthDay::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::DateTime(dt), FlakeValue::String(s)) => temporal::DateTime::parse(s)
            .ok()
            .map(|parsed| dt.as_ref().cmp(&parsed)),
        (FlakeValue::Date(d), FlakeValue::String(s)) => temporal::Date::parse(s)
            .ok()
            .map(|parsed| d.as_ref().cmp(&parsed)),
        (FlakeValue::Time(t), FlakeValue::String(s)) => temporal::Time::parse(s)
            .ok()
            .map(|parsed| t.as_ref().cmp(&parsed)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_cmp_longs() {
        let a = ComparableValue::Long(10);
        let b = ComparableValue::Long(20);
        assert_eq!(cmp_values(&a, &b), Some(Ordering::Less));
        assert_eq!(cmp_values(&b, &a), Some(Ordering::Greater));
        assert_eq!(cmp_values(&a, &a), Some(Ordering::Equal));
    }

    #[test]
    fn test_cmp_strings() {
        let a = ComparableValue::String(Arc::from("alpha"));
        let b = ComparableValue::String(Arc::from("beta"));
        assert_eq!(cmp_values(&a, &b), Some(Ordering::Less));
        assert_eq!(cmp_values(&a, &a), Some(Ordering::Equal));
    }

    #[test]
    fn test_type_mismatch() {
        let long = ComparableValue::Long(10);
        let string = ComparableValue::String(Arc::from("10"));
        // Type mismatch returns None
        assert_eq!(cmp_values(&long, &string), None);
    }

    // =========================================================================
    // Variadic eval tests
    // =========================================================================

    use crate::binding::BindingRow;
    use crate::ir::FilterValue;

    fn long(v: i64) -> Expression {
        Expression::Const(FilterValue::Long(v))
    }

    fn string(s: &str) -> Expression {
        Expression::Const(FilterValue::String(s.to_string()))
    }

    fn empty_row() -> BindingRow<'static> {
        BindingRow::new(&[], &[])
    }

    #[test]
    fn test_chained_lt_holds() {
        let row = empty_row();
        // 1 < 2 < 3 → true
        let args = vec![long(1), long(2), long(3)];
        let result = CompareOp::Lt.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_chained_lt_breaks() {
        let row = empty_row();
        // 1 < 3 < 2 → false (3 < 2 fails)
        let args = vec![long(1), long(3), long(2)];
        let result = CompareOp::Lt.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(false)));
    }

    #[test]
    fn test_chained_eq_all_same() {
        let row = empty_row();
        // 5 = 5 = 5 → true
        let args = vec![long(5), long(5), long(5)];
        let result = CompareOp::Eq.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_chained_eq_not_all_same() {
        let row = empty_row();
        // 5 = 5 = 6 → false
        let args = vec![long(5), long(5), long(6)];
        let result = CompareOp::Eq.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(false)));
    }

    #[test]
    fn test_single_arg_vacuously_true() {
        let row = empty_row();
        let args = vec![long(42)];
        let result = CompareOp::Lt.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_zero_args_error() {
        let row = empty_row();
        let args: Vec<Expression> = vec![];
        assert!(CompareOp::Lt.eval(&args, &row, None).is_err());
    }

    #[test]
    fn test_chained_ge_holds() {
        let row = empty_row();
        // 5 >= 3 >= 3 → true
        let args = vec![long(5), long(3), long(3)];
        let result = CompareOp::Ge.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_chained_ne() {
        let row = empty_row();
        // 1 != 2 != 3 → true (all consecutive pairs differ)
        let args = vec![long(1), long(2), long(3)];
        let result = CompareOp::Ne.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_ne_type_mismatch_is_true() {
        let row = empty_row();
        // 1 != "hello" → true (incomparable types are not equal)
        let args = vec![long(1), string("hello")];
        let result = CompareOp::Ne.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_eq_type_mismatch_is_false() {
        let row = empty_row();
        // 1 = "hello" → false (incomparable types are not equal)
        let args = vec![long(1), string("hello")];
        let result = CompareOp::Eq.eval(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(false)));
    }

    #[test]
    fn test_lt_type_mismatch_is_err() {
        let row = empty_row();
        // 1 < "hello" → error (incomparable types cannot be ordered)
        let args = vec![long(1), string("hello")];
        assert!(CompareOp::Lt.eval(&args, &row, None).is_err());
    }

    #[test]
    fn test_le_type_mismatch_is_err() {
        let row = empty_row();
        let args = vec![long(1), string("hello")];
        assert!(CompareOp::Le.eval(&args, &row, None).is_err());
    }

    #[test]
    fn test_gt_type_mismatch_is_err() {
        let row = empty_row();
        let args = vec![long(1), string("hello")];
        assert!(CompareOp::Gt.eval(&args, &row, None).is_err());
    }

    #[test]
    fn test_ge_type_mismatch_is_err() {
        let row = empty_row();
        let args = vec![long(1), string("hello")];
        assert!(CompareOp::Ge.eval(&args, &row, None).is_err());
    }
}
