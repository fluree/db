//! XSD datatype constructor (cast) functions
//!
//! Implements W3C SPARQL 1.1 §17.5 XSD casting:
//! xsd:boolean(), xsd:integer(), xsd:float(), xsd:double(), xsd:decimal(), xsd:string()
//!
//! Per the spec, invalid casts produce no binding (Ok(None)), not errors.

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;
use bigdecimal::BigDecimal;
use fluree_db_core::FlakeValue;
use num_traits::ToPrimitive;
use std::sync::{Arc, LazyLock};

use super::helpers::check_arity;
use super::value::ComparableValue;
use crate::parse::UnresolvedDatatypeConstraint;

static XSD_FLOAT_IRI: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from(fluree_vocab::xsd::FLOAT));

/// Unwrap a `TypedLiteral` into the corresponding primitive `ComparableValue`.
///
/// If the inner `FlakeValue` maps to a known primitive variant, returns that
/// variant. Otherwise returns the original value unchanged. This eliminates
/// the need to duplicate match arms for `TypedLiteral` in every cast function.
fn unwrap_typed_literal(v: ComparableValue) -> ComparableValue {
    match v {
        ComparableValue::TypedLiteral { val, .. } => match val {
            FlakeValue::Boolean(b) => ComparableValue::Bool(b),
            FlakeValue::Long(n) => ComparableValue::Long(n),
            FlakeValue::Double(d) => ComparableValue::Double(d),
            FlakeValue::String(s) => ComparableValue::String(Arc::from(s.as_str())),
            _ => ComparableValue::TypedLiteral { val, dtc: None },
        },
        other => other,
    }
}

// ---------------------------------------------------------------------------
// xsd:boolean
// ---------------------------------------------------------------------------

pub fn eval_xsd_boolean<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "xsd:boolean")?;
    let Some(v) = args[0].eval_to_comparable(row, ctx)? else {
        return Ok(None);
    };
    Ok(cast_to_boolean(v))
}

fn cast_to_boolean(v: ComparableValue) -> Option<ComparableValue> {
    let v = unwrap_typed_literal(v);
    let b = match v {
        ComparableValue::Bool(b) => b,
        ComparableValue::Long(n) => n != 0,
        ComparableValue::Double(d) => {
            if d.is_nan() {
                return None;
            }
            d != 0.0
        }
        ComparableValue::BigInt(n) => !num_traits::Zero::is_zero(&*n),
        ComparableValue::Decimal(d) => !num_traits::Zero::is_zero(&*d),
        ComparableValue::String(s) => match s.as_ref() {
            "true" | "1" => true,
            "false" | "0" => false,
            _ => return None,
        },
        _ => return None,
    };
    Some(ComparableValue::Bool(b))
}

// ---------------------------------------------------------------------------
// xsd:integer
// ---------------------------------------------------------------------------

pub fn eval_xsd_integer<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "xsd:integer")?;
    let Some(v) = args[0].eval_to_comparable(row, ctx)? else {
        return Ok(None);
    };
    Ok(cast_to_integer(v))
}

fn cast_to_integer(v: ComparableValue) -> Option<ComparableValue> {
    let v = unwrap_typed_literal(v);
    let n = match v {
        ComparableValue::Long(n) => n,
        ComparableValue::Bool(b) => i64::from(b),
        ComparableValue::Double(d) => {
            if !d.is_finite() || d > i64::MAX as f64 || d < i64::MIN as f64 {
                return None;
            }
            d.trunc() as i64
        }
        ComparableValue::BigInt(bi) => bi.to_i64()?,
        ComparableValue::Decimal(dec) => {
            if let Some(i) = dec.to_i64() {
                i
            } else {
                let f = dec.to_f64()?;
                if !f.is_finite() || f > i64::MAX as f64 || f < i64::MIN as f64 {
                    return None;
                }
                f.trunc() as i64
            }
        }
        ComparableValue::String(s) => s.parse::<i64>().ok()?,
        _ => return None,
    };
    Some(ComparableValue::Long(n))
}

// ---------------------------------------------------------------------------
// xsd:float
// ---------------------------------------------------------------------------

pub fn eval_xsd_float<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "xsd:float")?;
    let Some(v) = args[0].eval_to_comparable(row, ctx)? else {
        return Ok(None);
    };
    Ok(cast_to_float(v))
}

fn cast_to_float(v: ComparableValue) -> Option<ComparableValue> {
    let v = unwrap_typed_literal(v);
    let f: f32 = match v {
        ComparableValue::Bool(b) => {
            if b {
                1.0
            } else {
                0.0
            }
        }
        ComparableValue::Long(n) => n as f32,
        ComparableValue::Double(d) => d as f32,
        ComparableValue::BigInt(n) => n.to_f32()?,
        ComparableValue::Decimal(d) => d.to_f64()? as f32,
        ComparableValue::String(s) => s.parse::<f32>().ok()?,
        _ => return None,
    };
    Some(float_typed_literal(f))
}

/// Construct a TypedLiteral with xsd:float datatype.
///
/// We store the value as a String (not Double) because f32→f64 conversion
/// introduces precision artifacts (e.g., 33.33f32 → 33.33000183105469f64).
/// Using the f32 string representation avoids this.
fn float_typed_literal(f: f32) -> ComparableValue {
    let s = format_f32(f);
    ComparableValue::TypedLiteral {
        val: FlakeValue::String(s),
        dtc: Some(UnresolvedDatatypeConstraint::Explicit(
            XSD_FLOAT_IRI.clone(),
        )),
    }
}

/// Format an f32 value for xsd:float output.
///
/// Rust's default f32 Display uses minimal decimal digits. This is acceptable
/// for the W3C tests which compare float values numerically.
fn format_f32(f: f32) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            "INF".to_string()
        } else {
            "-INF".to_string()
        }
    } else {
        // Use f32 Display which gives minimal-length representation
        f.to_string()
    }
}

// ---------------------------------------------------------------------------
// xsd:double
// ---------------------------------------------------------------------------

pub fn eval_xsd_double<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "xsd:double")?;
    let Some(v) = args[0].eval_to_comparable(row, ctx)? else {
        return Ok(None);
    };
    Ok(cast_to_double(v))
}

fn cast_to_double(v: ComparableValue) -> Option<ComparableValue> {
    let v = unwrap_typed_literal(v);
    let d: f64 = match v {
        ComparableValue::Bool(b) => {
            if b {
                1.0
            } else {
                0.0
            }
        }
        ComparableValue::Long(n) => n as f64,
        ComparableValue::Double(d) => d,
        ComparableValue::BigInt(n) => n.to_f64()?,
        ComparableValue::Decimal(dec) => dec.to_f64()?,
        ComparableValue::String(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    Some(ComparableValue::Double(d))
}

// ---------------------------------------------------------------------------
// xsd:decimal
// ---------------------------------------------------------------------------

pub fn eval_xsd_decimal<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "xsd:decimal")?;
    let Some(v) = args[0].eval_to_comparable(row, ctx)? else {
        return Ok(None);
    };
    Ok(cast_to_decimal(v))
}

fn cast_to_decimal(v: ComparableValue) -> Option<ComparableValue> {
    let v = unwrap_typed_literal(v);
    let d = match v {
        ComparableValue::Decimal(d) => return Some(ComparableValue::Decimal(d)),
        ComparableValue::Bool(b) => BigDecimal::from(i64::from(b)),
        ComparableValue::Long(n) => BigDecimal::from(n),
        ComparableValue::Double(d) => {
            if !d.is_finite() {
                return None;
            }
            BigDecimal::try_from(d).ok()?
        }
        ComparableValue::BigInt(n) => BigDecimal::from((*n).clone()),
        ComparableValue::String(s) => parse_decimal_string(&s)?,
        _ => return None,
    };
    Some(ComparableValue::Decimal(Box::new(d)))
}

/// Parse a string as xsd:decimal. Rejects scientific notation per XSD spec.
fn parse_decimal_string(s: &str) -> Option<BigDecimal> {
    // xsd:decimal does not accept scientific notation (e.g. "1.5E2")
    if s.contains('e') || s.contains('E') {
        return None;
    }
    s.parse::<BigDecimal>().ok()
}

// ---------------------------------------------------------------------------
// xsd:string
// ---------------------------------------------------------------------------

pub fn eval_xsd_string<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "xsd:string")?;
    let Some(v) = args[0].eval_to_comparable(row, ctx)? else {
        return Ok(None);
    };
    Ok(cast_to_string(v, ctx))
}

fn cast_to_string(
    v: ComparableValue,
    ctx: Option<&ExecutionContext<'_>>,
) -> Option<ComparableValue> {
    // Decimal needs special handling: W3C canonical form strips trailing zeros
    // and trailing decimal point (e.g., "1.0" → "1", "2.50" → "2.5").
    if let ComparableValue::Decimal(ref d) = v {
        let s = canonical_decimal_string(d);
        return Some(ComparableValue::String(Arc::from(s)));
    }
    let namespace_codes = ctx.map(|c| c.active_snapshot.namespaces());
    v.into_string_value_with_namespaces(namespace_codes)
}

/// Produce the XSD canonical string form of a decimal value.
///
/// Strips trailing zeros after the decimal point and removes the decimal
/// point if no fractional part remains. E.g., "1.0" → "1", "2.50" → "2.5".
fn canonical_decimal_string(d: &BigDecimal) -> String {
    let s = d.normalized().to_string();
    if s.contains('.') {
        let trimmed = s.trim_end_matches('0');
        trimmed.trim_end_matches('.').to_string()
    } else {
        s
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::BindingRow;
    use fluree_db_core::value::FlakeValue;

    fn long(v: i64) -> Expression {
        Expression::Const(FlakeValue::Long(v))
    }

    fn double(v: f64) -> Expression {
        Expression::Const(FlakeValue::Double(v))
    }

    fn bool_expr(v: bool) -> Expression {
        Expression::Const(FlakeValue::Boolean(v))
    }

    fn string_expr(s: &str) -> Expression {
        Expression::Const(FlakeValue::String(s.to_string()))
    }

    fn empty_row() -> BindingRow<'static> {
        BindingRow::new(&[], &[])
    }

    // === xsd:boolean ===

    #[test]
    fn boolean_from_bool_identity() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_boolean(&[bool_expr(true)], &row, None).unwrap(),
            Some(ComparableValue::Bool(true))
        );
        assert_eq!(
            eval_xsd_boolean(&[bool_expr(false)], &row, None).unwrap(),
            Some(ComparableValue::Bool(false))
        );
    }

    #[test]
    fn boolean_from_numeric() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_boolean(&[long(1)], &row, None).unwrap(),
            Some(ComparableValue::Bool(true))
        );
        assert_eq!(
            eval_xsd_boolean(&[long(0)], &row, None).unwrap(),
            Some(ComparableValue::Bool(false))
        );
        assert_eq!(
            eval_xsd_boolean(&[double(0.0)], &row, None).unwrap(),
            Some(ComparableValue::Bool(false))
        );
        assert_eq!(
            eval_xsd_boolean(&[double(1.5)], &row, None).unwrap(),
            Some(ComparableValue::Bool(true))
        );
    }

    #[test]
    fn boolean_from_string() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_boolean(&[string_expr("true")], &row, None).unwrap(),
            Some(ComparableValue::Bool(true))
        );
        assert_eq!(
            eval_xsd_boolean(&[string_expr("false")], &row, None).unwrap(),
            Some(ComparableValue::Bool(false))
        );
        assert_eq!(
            eval_xsd_boolean(&[string_expr("1")], &row, None).unwrap(),
            Some(ComparableValue::Bool(true))
        );
        assert_eq!(
            eval_xsd_boolean(&[string_expr("0")], &row, None).unwrap(),
            Some(ComparableValue::Bool(false))
        );
    }

    #[test]
    fn boolean_from_invalid_string_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_boolean(&[string_expr("yes")], &row, None).unwrap(),
            None
        );
    }

    #[test]
    fn boolean_from_nan_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_boolean(&[double(f64::NAN)], &row, None).unwrap(),
            None
        );
    }

    // === xsd:integer ===

    #[test]
    fn integer_from_long_identity() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[long(42)], &row, None).unwrap(),
            Some(ComparableValue::Long(42))
        );
    }

    #[test]
    fn integer_from_bool() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[bool_expr(true)], &row, None).unwrap(),
            Some(ComparableValue::Long(1))
        );
        assert_eq!(
            eval_xsd_integer(&[bool_expr(false)], &row, None).unwrap(),
            Some(ComparableValue::Long(0))
        );
    }

    #[test]
    fn integer_from_double_truncates() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[double(3.9)], &row, None).unwrap(),
            Some(ComparableValue::Long(3))
        );
        assert_eq!(
            eval_xsd_integer(&[double(-2.7)], &row, None).unwrap(),
            Some(ComparableValue::Long(-2))
        );
    }

    #[test]
    fn integer_from_nan_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[double(f64::NAN)], &row, None).unwrap(),
            None
        );
    }

    #[test]
    fn integer_from_infinity_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[double(f64::INFINITY)], &row, None).unwrap(),
            None
        );
    }

    #[test]
    fn integer_from_string() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[string_expr("123")], &row, None).unwrap(),
            Some(ComparableValue::Long(123))
        );
    }

    #[test]
    fn integer_from_non_numeric_string_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[string_expr("abc")], &row, None).unwrap(),
            None
        );
    }

    #[test]
    fn integer_from_float_string_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[string_expr("2.75")], &row, None).unwrap(),
            None
        );
    }

    // === xsd:double ===

    #[test]
    fn double_from_double_identity() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_double(&[double(2.75)], &row, None).unwrap(),
            Some(ComparableValue::Double(2.75))
        );
    }

    #[test]
    fn double_from_bool() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_double(&[bool_expr(true)], &row, None).unwrap(),
            Some(ComparableValue::Double(1.0))
        );
        assert_eq!(
            eval_xsd_double(&[bool_expr(false)], &row, None).unwrap(),
            Some(ComparableValue::Double(0.0))
        );
    }

    #[test]
    fn double_from_long() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_double(&[long(42)], &row, None).unwrap(),
            Some(ComparableValue::Double(42.0))
        );
    }

    #[test]
    fn double_from_string() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_double(&[string_expr("2.75")], &row, None).unwrap(),
            Some(ComparableValue::Double(2.75))
        );
    }

    #[test]
    fn double_from_non_numeric_string_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_double(&[string_expr("abc")], &row, None).unwrap(),
            None
        );
    }

    // === xsd:decimal ===

    #[test]
    fn decimal_from_long() {
        let row = empty_row();
        let result = eval_xsd_decimal(&[long(42)], &row, None).unwrap();
        assert_eq!(
            result,
            Some(ComparableValue::Decimal(Box::new(BigDecimal::from(42))))
        );
    }

    #[test]
    fn decimal_from_bool() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_decimal(&[bool_expr(true)], &row, None).unwrap(),
            Some(ComparableValue::Decimal(Box::new(BigDecimal::from(1))))
        );
    }

    #[test]
    fn decimal_from_string() {
        let row = empty_row();
        let result = eval_xsd_decimal(&[string_expr("33.33")], &row, None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn decimal_rejects_scientific_notation_string() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_decimal(&[string_expr("1.5E2")], &row, None).unwrap(),
            None
        );
    }

    #[test]
    fn decimal_from_nan_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_decimal(&[double(f64::NAN)], &row, None).unwrap(),
            None
        );
    }

    // === xsd:string ===

    #[test]
    fn string_from_long() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_string(&[long(42)], &row, None).unwrap(),
            Some(ComparableValue::String(Arc::from("42")))
        );
    }

    #[test]
    fn string_from_bool() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_string(&[bool_expr(true)], &row, None).unwrap(),
            Some(ComparableValue::String(Arc::from("true")))
        );
    }

    #[test]
    fn string_from_double() {
        let row = empty_row();
        let result = eval_xsd_string(&[double(2.75)], &row, None).unwrap();
        assert!(result.is_some());
    }

    // === xsd:float ===

    #[test]
    fn float_from_bool() {
        let row = empty_row();
        let result = eval_xsd_float(&[bool_expr(true)], &row, None).unwrap();
        assert!(result.is_some());
        // Should be a TypedLiteral with xsd:float datatype
        if let Some(ComparableValue::TypedLiteral { dtc, .. }) = &result {
            assert_eq!(
                dtc.as_ref()
                    .map(fluree_vocab::UnresolvedDatatypeConstraint::datatype_iri),
                Some("http://www.w3.org/2001/XMLSchema#float")
            );
        } else {
            panic!("Expected TypedLiteral, got {result:?}");
        }
    }

    #[test]
    fn float_from_long() {
        let row = empty_row();
        let result = eval_xsd_float(&[long(42)], &row, None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn float_from_non_numeric_string_is_none() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_float(&[string_expr("abc")], &row, None).unwrap(),
            None
        );
    }

    // === arity validation ===

    #[test]
    fn cast_wrong_arity_two_args_errors() {
        let row = empty_row();
        assert!(eval_xsd_integer(&[long(1), long(2)], &row, None).is_err());
        assert!(eval_xsd_boolean(&[bool_expr(true), bool_expr(false)], &row, None).is_err());
        assert!(eval_xsd_double(&[double(1.0), double(2.0)], &row, None).is_err());
        assert!(eval_xsd_float(&[double(1.0), double(2.0)], &row, None).is_err());
        assert!(eval_xsd_decimal(&[long(1), long(2)], &row, None).is_err());
        assert!(eval_xsd_string(&[long(1), long(2)], &row, None).is_err());
    }

    #[test]
    fn cast_wrong_arity_zero_args_errors() {
        let row = empty_row();
        assert!(eval_xsd_integer(&[], &row, None).is_err());
        assert!(eval_xsd_boolean(&[], &row, None).is_err());
        assert!(eval_xsd_double(&[], &row, None).is_err());
        assert!(eval_xsd_float(&[], &row, None).is_err());
        assert!(eval_xsd_decimal(&[], &row, None).is_err());
        assert!(eval_xsd_string(&[], &row, None).is_err());
    }

    // === negative values ===

    #[test]
    fn integer_from_negative_long() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_integer(&[long(-7)], &row, None).unwrap(),
            Some(ComparableValue::Long(-7))
        );
    }

    #[test]
    fn double_from_negative() {
        let row = empty_row();
        assert_eq!(
            eval_xsd_double(&[double(-2.75)], &row, None).unwrap(),
            Some(ComparableValue::Double(-2.75))
        );
    }
}
