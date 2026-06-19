//! ComparableValue type and conversions
//!
//! This module contains the intermediate value type used during filter
//! evaluation, along with conversions to/from FlakeValue.

use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::error::QueryError;
use crate::ir::ArithmeticOp;
use bigdecimal::BigDecimal;
use fluree_db_core::temporal::{
    Date as FlureeDate, DateTime as FlureeDateTime, Time as FlureeTime,
};
use fluree_db_core::{coerce_value, FlakeValue, GeoPointBits};
use num_bigint::BigInt;
use num_traits::Zero;
use std::sync::Arc;
use thiserror::Error;

use super::helpers::WELL_KNOWN_DATATYPES;
use crate::parse::UnresolvedDatatypeConstraint;

/// Errors that can occur during arithmetic operations
#[derive(Error, Debug, Clone, PartialEq)]
pub enum ArithmeticError {
    /// Division by zero
    #[error("division by zero")]
    DivideByZero,

    /// Integer overflow
    #[error("integer overflow")]
    Overflow,

    /// Type mismatch or non-numeric operands
    #[error("type mismatch: cannot perform arithmetic on these types")]
    TypeMismatch,
}

/// Errors that can occur during comparison operations
#[derive(Error, Debug, Clone, PartialEq)]
pub enum ComparisonError {
    /// Ordering comparison between incompatible types
    #[error("type mismatch: cannot compare {left_type} with {right_type} using '{operator}'")]
    TypeMismatch {
        operator: &'static str,
        left_type: &'static str,
        right_type: &'static str,
    },
}

/// Error when converting a FlakeValue that has no ComparableValue equivalent
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("cannot convert null value")]
pub struct NullValueError;

/// Significant-digit cap for the `xsd:integer / xsd:integer` → `xsd:decimal`
/// result. `BigDecimal::div` otherwise expands recurring quotients to its
/// default 100-digit precision (e.g. 1 / 3 = 0.3333…). 34 digits matches
/// IEEE-754 decimal128 — well past xsd:double precision but bounded. Mirrors
/// `AVG_DECIMAL_PRECISION` in `aggregate.rs`, which applies the same XPath rule.
const DIVISION_DECIMAL_PRECISION: u64 = 34;

/// Divide two decimals with bounded precision and a canonical (normalized)
/// scale, for the integer-division case where the result is an `xsd:decimal`.
fn integer_divide(a: BigDecimal, b: BigDecimal) -> BigDecimal {
    (a / b).with_prec(DIVISION_DECIMAL_PRECISION).normalized()
}

impl ArithmeticOp {
    /// Apply this arithmetic operation to two ComparableValue operands.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Division by zero is attempted (`ArithmeticError::DivideByZero`)
    /// - Integer overflow occurs (`ArithmeticError::Overflow`)
    /// - Operands are non-numeric or incompatible (`ArithmeticError::TypeMismatch`)
    pub fn apply(
        self,
        left: ComparableValue,
        right: ComparableValue,
    ) -> Result<ComparableValue, ArithmeticError> {
        use num_traits::ToPrimitive;

        // Normalize numeric values carried as a `TypedLiteral` (e.g. xsd:float,
        // whose value is stored as a String) into their primitive numeric
        // variant so the type-promotion arms below apply. Non-numeric typed
        // literals pass through unchanged and fall to the TypeMismatch arm.
        let left = left.coerce_numeric_operand();
        let right = right.coerce_numeric_operand();

        match (left, right) {
            // Long <op> Long
            (ComparableValue::Long(a), ComparableValue::Long(b)) => match self {
                ArithmeticOp::Add => Ok(ComparableValue::Long(
                    a.checked_add(b).ok_or(ArithmeticError::Overflow)?,
                )),
                ArithmeticOp::Sub => Ok(ComparableValue::Long(
                    a.checked_sub(b).ok_or(ArithmeticError::Overflow)?,
                )),
                ArithmeticOp::Mul => Ok(ComparableValue::Long(
                    a.checked_mul(b).ok_or(ArithmeticError::Overflow)?,
                )),
                // Per XPath op:numeric-divide, xsd:integer / xsd:integer yields
                // xsd:decimal (e.g. 10 / 4 = 2.5), not a truncated integer.
                ArithmeticOp::Div => {
                    if b == 0 {
                        return Err(ArithmeticError::DivideByZero);
                    }
                    Ok(ComparableValue::Decimal(Box::new(integer_divide(
                        BigDecimal::from(a),
                        BigDecimal::from(b),
                    ))))
                }
            },
            // Double + Double = Double
            (ComparableValue::Double(a), ComparableValue::Double(b)) => {
                let result = match self {
                    ArithmeticOp::Add => a + b,
                    ArithmeticOp::Sub => a - b,
                    ArithmeticOp::Mul => a * b,
                    ArithmeticOp::Div => {
                        if b == 0.0 {
                            return Err(ArithmeticError::DivideByZero);
                        }
                        a / b
                    }
                };
                Ok(ComparableValue::Double(result))
            }
            // BigInt <op> BigInt
            (ComparableValue::BigInt(a), ComparableValue::BigInt(b)) => match self {
                ArithmeticOp::Add => Ok(ComparableValue::BigInt(Box::new(*a + &*b))),
                ArithmeticOp::Sub => Ok(ComparableValue::BigInt(Box::new(*a - &*b))),
                ArithmeticOp::Mul => Ok(ComparableValue::BigInt(Box::new(*a * &*b))),
                // integer / integer → xsd:decimal (see the Long/Long arm).
                ArithmeticOp::Div => {
                    if b.is_zero() {
                        return Err(ArithmeticError::DivideByZero);
                    }
                    Ok(ComparableValue::Decimal(Box::new(integer_divide(
                        BigDecimal::from(*a),
                        BigDecimal::from(*b),
                    ))))
                }
            },
            // Decimal + Decimal = Decimal
            (ComparableValue::Decimal(a), ComparableValue::Decimal(b)) => {
                let result = match self {
                    ArithmeticOp::Add => &*a + &*b,
                    ArithmeticOp::Sub => &*a - &*b,
                    ArithmeticOp::Mul => &*a * &*b,
                    ArithmeticOp::Div => {
                        if b.is_zero() {
                            return Err(ArithmeticError::DivideByZero);
                        }
                        &*a / &*b
                    }
                };
                Ok(ComparableValue::Decimal(Box::new(result)))
            }
            // Mixed numeric types -> promote to higher precision
            // Long <-> Double -> Double
            (ComparableValue::Long(a), ComparableValue::Double(b)) => self.apply(
                ComparableValue::Double(a as f64),
                ComparableValue::Double(b),
            ),
            (ComparableValue::Double(a), ComparableValue::Long(b)) => self.apply(
                ComparableValue::Double(a),
                ComparableValue::Double(b as f64),
            ),
            // Long <-> BigInt -> BigInt
            (ComparableValue::Long(a), ComparableValue::BigInt(b)) => self.apply(
                ComparableValue::BigInt(Box::new(BigInt::from(a))),
                ComparableValue::BigInt(b),
            ),
            (ComparableValue::BigInt(a), ComparableValue::Long(b)) => self.apply(
                ComparableValue::BigInt(a),
                ComparableValue::BigInt(Box::new(BigInt::from(b))),
            ),
            // Long <-> Decimal -> Decimal
            (ComparableValue::Long(a), ComparableValue::Decimal(b)) => self.apply(
                ComparableValue::Decimal(Box::new(BigDecimal::from(a))),
                ComparableValue::Decimal(b),
            ),
            (ComparableValue::Decimal(a), ComparableValue::Long(b)) => self.apply(
                ComparableValue::Decimal(a),
                ComparableValue::Decimal(Box::new(BigDecimal::from(b))),
            ),
            // BigInt <-> Decimal -> Decimal
            (ComparableValue::BigInt(a), ComparableValue::Decimal(b)) => self.apply(
                ComparableValue::Decimal(Box::new(BigDecimal::from((*a).clone()))),
                ComparableValue::Decimal(b),
            ),
            (ComparableValue::Decimal(a), ComparableValue::BigInt(b)) => self.apply(
                ComparableValue::Decimal(a),
                ComparableValue::Decimal(Box::new(BigDecimal::from((*b).clone()))),
            ),
            // Double <-> BigInt -> Double (lossy)
            (ComparableValue::Double(a), ComparableValue::BigInt(b)) => {
                let bf = b.to_f64().ok_or(ArithmeticError::TypeMismatch)?;
                self.apply(ComparableValue::Double(a), ComparableValue::Double(bf))
            }
            (ComparableValue::BigInt(a), ComparableValue::Double(b)) => {
                let af = a.to_f64().ok_or(ArithmeticError::TypeMismatch)?;
                self.apply(ComparableValue::Double(af), ComparableValue::Double(b))
            }
            // Double <-> Decimal -> Decimal (if possible)
            (ComparableValue::Double(a), ComparableValue::Decimal(b)) => {
                let ad = BigDecimal::try_from(a).map_err(|_| ArithmeticError::TypeMismatch)?;
                self.apply(
                    ComparableValue::Decimal(Box::new(ad)),
                    ComparableValue::Decimal(b),
                )
            }
            (ComparableValue::Decimal(a), ComparableValue::Double(b)) => {
                let bd = BigDecimal::try_from(b).map_err(|_| ArithmeticError::TypeMismatch)?;
                self.apply(
                    ComparableValue::Decimal(a),
                    ComparableValue::Decimal(Box::new(bd)),
                )
            }
            // Non-numeric types can't do arithmetic
            _ => Err(ArithmeticError::TypeMismatch),
        }
    }
}

/// Comparable value extracted from expression evaluation
///
/// This is the internal representation used during filter evaluation.
/// It normalizes different binding types into a common format for comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum ComparableValue {
    Long(i64),
    Double(f64),
    String(Arc<str>),
    Bool(bool),
    Sid(fluree_db_core::Sid),
    Vector(Arc<[f64]>),
    // Extended numeric types
    BigInt(Box<BigInt>),
    Decimal(Box<BigDecimal>),
    // Temporal types
    DateTime(FlureeDateTime),
    Date(FlureeDate),
    Time(FlureeTime),
    // Geo types
    GeoPoint(GeoPointBits),
    // IRI/URI
    Iri(Arc<str>),
    // Typed literal with optional datatype IRI constraint
    TypedLiteral {
        val: FlakeValue,
        dtc: Option<UnresolvedDatatypeConstraint>,
    },
}

/// Compute the Effective Boolean Value (EBV) of a ComparableValue.
///
/// EBV is used in SPARQL FILTER and conditional expressions.
/// See: <https://www.w3.org/TR/sparql11-query/#ebv>
impl From<ComparableValue> for bool {
    fn from(value: ComparableValue) -> bool {
        match value {
            ComparableValue::Bool(b) => b,
            ComparableValue::String(s) => !s.is_empty(),
            ComparableValue::Iri(s) => !s.is_empty(),
            ComparableValue::Long(n) => n != 0,
            ComparableValue::Double(d) => !d.is_nan() && d != 0.0,
            ComparableValue::BigInt(n) => !n.is_zero(),
            ComparableValue::Decimal(d) => !d.is_zero(),
            // Other types: Sid, Vector, DateTime, etc. are truthy if present
            _ => true,
        }
    }
}

impl ComparableValue {
    /// Return a human-readable type name for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            ComparableValue::Long(_) => "long",
            ComparableValue::Double(_) => "double",
            ComparableValue::String(_) => "string",
            ComparableValue::Bool(_) => "boolean",
            ComparableValue::Sid(_) => "sid",
            ComparableValue::Vector(_) => "vector",
            ComparableValue::BigInt(_) => "bigint",
            ComparableValue::Decimal(_) => "decimal",
            ComparableValue::DateTime(_) => "dateTime",
            ComparableValue::Date(_) => "date",
            ComparableValue::Time(_) => "time",
            ComparableValue::GeoPoint(_) => "geoPoint",
            ComparableValue::Iri(_) => "iri",
            ComparableValue::TypedLiteral { .. } => "typedLiteral",
        }
    }

    /// Normalize a numeric value carried as a [`ComparableValue::TypedLiteral`]
    /// into its primitive numeric variant, so arithmetic type promotion can
    /// apply.
    ///
    /// `xsd:float` casts store their value as a String (to avoid f32→f64 display
    /// artifacts — see `eval/cast.rs`), and other numeric XSD literals may arrive
    /// wrapped as a `TypedLiteral`. Arithmetic needs the numeric value, so the
    /// inner value is unwrapped (or parsed, for the string-encoded cases) here.
    /// Non-numeric typed literals — and all other variants — are returned
    /// unchanged, so they still fall to a `TypeMismatch` in [`ArithmeticOp::apply`].
    pub(crate) fn coerce_numeric_operand(self) -> ComparableValue {
        let ComparableValue::TypedLiteral { val, dtc } = self else {
            return self;
        };
        match val {
            FlakeValue::Long(n) => ComparableValue::Long(n),
            FlakeValue::Double(d) => ComparableValue::Double(d),
            FlakeValue::BigInt(n) => ComparableValue::BigInt(n),
            FlakeValue::Decimal(d) => ComparableValue::Decimal(d),
            FlakeValue::String(s) => {
                // Delegate string→numeric parsing to the canonical coercion
                // layer (`fluree_db_core::coerce_value`), which covers the full
                // XSD numeric lattice: the integer family (with BigInt overflow
                // and per-type range validation), xsd:decimal, and
                // xsd:float/double including the INF/-INF/NaN lexicals. Only a
                // numeric result is adopted; non-numeric datatypes and parse
                // failures keep the literal as-is so it still falls to a
                // TypeMismatch in `apply`.
                if let Some(UnresolvedDatatypeConstraint::Explicit(iri)) = &dtc {
                    if let Ok(coerced) = coerce_value(FlakeValue::String(s.clone()), iri.as_ref()) {
                        if let Ok(
                            cv @ (ComparableValue::Long(_)
                            | ComparableValue::Double(_)
                            | ComparableValue::BigInt(_)
                            | ComparableValue::Decimal(_)),
                        ) = ComparableValue::try_from(coerced)
                        {
                            return cv;
                        }
                    }
                }
                ComparableValue::TypedLiteral {
                    val: FlakeValue::String(s),
                    dtc,
                }
            }
            other => ComparableValue::TypedLiteral { val: other, dtc },
        }
    }

    /// Get a string slice if this value is a String, Iri, or TypedLiteral containing a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ComparableValue::String(s) => Some(s.as_ref()),
            ComparableValue::Iri(s) => Some(s.as_ref()),
            ComparableValue::TypedLiteral {
                val: FlakeValue::String(s),
                ..
            } => Some(s.as_str()),
            _ => None,
        }
    }

    /// Convert this value to a string-typed ComparableValue (for STR function).
    ///
    /// Consumes self and returns a `ComparableValue::String` containing the
    /// string representation of the value. Returns `None` for types that
    /// cannot be converted to strings (e.g., vectors).
    ///
    /// **Note**: For SID values, this returns the internal `{code}:{name}` form.
    /// Use [`into_string_value_with_namespaces`] to expand SIDs to full IRIs.
    pub fn into_string_value(self) -> Option<ComparableValue> {
        match self {
            ComparableValue::String(s) => Some(ComparableValue::String(s)),
            ComparableValue::Iri(s) => Some(ComparableValue::String(s)),
            ComparableValue::Sid(sid) => Some(ComparableValue::String(Arc::from(format!(
                "{}:{}",
                sid.namespace_code, sid.name
            )))),
            ComparableValue::Long(n) => Some(ComparableValue::String(Arc::from(n.to_string()))),
            ComparableValue::Double(d) => Some(ComparableValue::String(Arc::from(d.to_string()))),
            ComparableValue::Bool(b) => Some(ComparableValue::String(Arc::from(b.to_string()))),
            ComparableValue::BigInt(n) => Some(ComparableValue::String(Arc::from(n.to_string()))),
            ComparableValue::Decimal(d) => {
                Some(ComparableValue::String(Arc::from(d.to_plain_string())))
            }
            ComparableValue::DateTime(dt) => {
                Some(ComparableValue::String(Arc::from(dt.to_string())))
            }
            ComparableValue::Date(d) => Some(ComparableValue::String(Arc::from(d.to_string()))),
            ComparableValue::Time(t) => Some(ComparableValue::String(Arc::from(t.to_string()))),
            ComparableValue::GeoPoint(bits) => {
                // Convert to WKT format: POINT(lng lat)
                Some(ComparableValue::String(Arc::from(bits.to_string())))
            }
            ComparableValue::Vector(_) => None, // Vectors don't have a string representation
            ComparableValue::TypedLiteral { val, .. } => match val {
                FlakeValue::String(s) => Some(ComparableValue::String(Arc::from(s))),
                FlakeValue::Long(n) => Some(ComparableValue::String(Arc::from(n.to_string()))),
                FlakeValue::Double(d) => Some(ComparableValue::String(Arc::from(d.to_string()))),
                FlakeValue::Boolean(b) => Some(ComparableValue::String(Arc::from(b.to_string()))),
                _ => None,
            },
        }
    }

    /// Convert this value to a string-typed ComparableValue, expanding SIDs
    /// to full IRIs using the provided namespace codes.
    ///
    /// Per W3C SPARQL spec, `STR()` on an IRI must return the full IRI string,
    /// not an internal compact form. This method looks up the namespace prefix
    /// for SID values and reconstructs the full IRI.
    ///
    /// Falls back to `into_string_value()` for non-SID values or when
    /// namespace codes are unavailable.
    pub fn into_string_value_with_namespaces(
        self,
        namespace_codes: Option<&std::collections::HashMap<u16, String>>,
    ) -> Option<ComparableValue> {
        match &self {
            ComparableValue::Sid(sid) => {
                if let Some(prefix) = namespace_codes.and_then(|ns| ns.get(&sid.namespace_code)) {
                    Some(ComparableValue::String(Arc::from(format!(
                        "{}{}",
                        prefix, sid.name
                    ))))
                } else {
                    // Fallback: no namespace codes available or code not found
                    self.into_string_value()
                }
            }
            _ => self.into_string_value(),
        }
    }

    /// Convert this value to a Binding.
    ///
    /// The `ctx` parameter is required for `Iri` and `TypedLiteral` variants
    /// which need database access to resolve IRIs to Sids.
    pub fn to_binding(self, ctx: Option<&ExecutionContext<'_>>) -> crate::error::Result<Binding> {
        let datatypes = &*WELL_KNOWN_DATATYPES;
        match self {
            ComparableValue::Long(n) => Ok(Binding::lit(
                FlakeValue::Long(n),
                datatypes.xsd_integer.clone(),
            )),
            ComparableValue::Double(d) => Ok(Binding::lit(
                FlakeValue::Double(d),
                datatypes.xsd_double.clone(),
            )),
            ComparableValue::String(s) => Ok(Binding::lit(
                FlakeValue::String(s.to_string()),
                datatypes.xsd_string.clone(),
            )),
            ComparableValue::Bool(b) => Ok(Binding::lit(
                FlakeValue::Boolean(b),
                datatypes.xsd_boolean.clone(),
            )),
            ComparableValue::Sid(sid) => Ok(Binding::sid(sid)),
            ComparableValue::Vector(v) => Ok(Binding::lit(
                FlakeValue::Vector(v.to_vec()),
                datatypes.fluree_vector.clone(),
            )),
            ComparableValue::BigInt(n) => Ok(Binding::lit(
                FlakeValue::BigInt(n),
                datatypes.xsd_integer.clone(),
            )),
            ComparableValue::Decimal(d) => Ok(Binding::lit(
                FlakeValue::Decimal(d),
                datatypes.xsd_decimal.clone(),
            )),
            ComparableValue::DateTime(dt) => Ok(Binding::lit(
                FlakeValue::DateTime(Box::new(dt)),
                datatypes.xsd_datetime.clone(),
            )),
            ComparableValue::Date(d) => Ok(Binding::lit(
                FlakeValue::Date(Box::new(d)),
                datatypes.xsd_date.clone(),
            )),
            ComparableValue::Time(t) => Ok(Binding::lit(
                FlakeValue::Time(Box::new(t)),
                datatypes.xsd_time.clone(),
            )),
            ComparableValue::GeoPoint(bits) => Ok(Binding::lit(
                FlakeValue::GeoPoint(bits),
                datatypes.geo_wkt_literal.clone(),
            )),
            ComparableValue::Iri(iri) => {
                // Try to encode to a SID if we have database context,
                // but fall back to Binding::Iri for constructed IRIs
                // (UUID, IRI() function) that don't exist in the database.
                if let Some(ctx) = ctx {
                    if let Some(sid) = ctx.active_snapshot.encode_iri_strict(&iri) {
                        return Ok(Binding::sid(sid));
                    }
                }
                Ok(Binding::Iri(iri))
            }
            ComparableValue::TypedLiteral { val, dtc } => match dtc {
                Some(UnresolvedDatatypeConstraint::LangTag(lang)) => {
                    Ok(Binding::lit_lang(val, lang))
                }
                Some(UnresolvedDatatypeConstraint::Explicit(dt_iri)) => {
                    let ctx = ctx.ok_or_else(|| {
                        QueryError::InvalidFilter(
                            "bind evaluation requires database context for str-dt/str-lang"
                                .to_string(),
                        )
                    })?;
                    let dt = ctx
                        .active_snapshot
                        .encode_iri_strict(&dt_iri)
                        .ok_or_else(|| {
                            QueryError::InvalidFilter(format!("Unknown datatype IRI: {dt_iri}"))
                        })?;
                    Ok(Binding::lit(val, dt))
                }
                None => Ok(Binding::lit(val, datatypes.xsd_string.clone())),
            },
        }
    }
}

// =============================================================================
// From implementations for primitive types
// =============================================================================

impl From<i64> for ComparableValue {
    fn from(n: i64) -> Self {
        ComparableValue::Long(n)
    }
}

impl From<f64> for ComparableValue {
    fn from(d: f64) -> Self {
        ComparableValue::Double(d)
    }
}

impl From<bool> for ComparableValue {
    fn from(b: bool) -> Self {
        ComparableValue::Bool(b)
    }
}

impl From<String> for ComparableValue {
    fn from(s: String) -> Self {
        ComparableValue::String(Arc::from(s))
    }
}

impl From<&str> for ComparableValue {
    fn from(s: &str) -> Self {
        ComparableValue::String(Arc::from(s))
    }
}

impl From<Arc<str>> for ComparableValue {
    fn from(s: Arc<str>) -> Self {
        ComparableValue::String(s)
    }
}

// =============================================================================
// Conversions from FlakeValue
// =============================================================================

impl TryFrom<&FlakeValue> for ComparableValue {
    type Error = NullValueError;

    fn try_from(val: &FlakeValue) -> Result<Self, Self::Error> {
        match val {
            FlakeValue::Long(n) => Ok(ComparableValue::Long(*n)),
            FlakeValue::Double(d) => Ok(ComparableValue::Double(*d)),
            FlakeValue::String(s) => Ok(ComparableValue::String(Arc::from(s.as_str()))),
            FlakeValue::Json(s) => Ok(ComparableValue::String(Arc::from(s.as_str()))),
            FlakeValue::Boolean(b) => Ok(ComparableValue::Bool(*b)),
            FlakeValue::Ref(sid) => Ok(ComparableValue::Sid(sid.clone())),
            FlakeValue::Null => Err(NullValueError),
            FlakeValue::Vector(v) => Ok(ComparableValue::Vector(Arc::from(v.as_slice()))),
            FlakeValue::BigInt(n) => Ok(ComparableValue::BigInt(n.clone())),
            FlakeValue::Decimal(d) => Ok(ComparableValue::Decimal(d.clone())),
            FlakeValue::DateTime(dt) => Ok(ComparableValue::DateTime(dt.as_ref().clone())),
            FlakeValue::Date(d) => Ok(ComparableValue::Date(d.as_ref().clone())),
            FlakeValue::Time(t) => Ok(ComparableValue::Time(t.as_ref().clone())),
            FlakeValue::GeoPoint(bits) => Ok(ComparableValue::GeoPoint(*bits)),
            FlakeValue::GYear(_)
            | FlakeValue::GYearMonth(_)
            | FlakeValue::GMonth(_)
            | FlakeValue::GDay(_)
            | FlakeValue::GMonthDay(_)
            | FlakeValue::YearMonthDuration(_)
            | FlakeValue::DayTimeDuration(_)
            | FlakeValue::Duration(_) => Ok(ComparableValue::TypedLiteral {
                val: val.clone(),
                dtc: None,
            }),
        }
    }
}

impl TryFrom<FlakeValue> for ComparableValue {
    type Error = NullValueError;

    fn try_from(val: FlakeValue) -> Result<Self, Self::Error> {
        match val {
            FlakeValue::Long(n) => Ok(ComparableValue::Long(n)),
            FlakeValue::Double(d) => Ok(ComparableValue::Double(d)),
            FlakeValue::String(s) => Ok(ComparableValue::String(Arc::from(s))),
            FlakeValue::Json(s) => Ok(ComparableValue::String(Arc::from(s))),
            FlakeValue::Boolean(b) => Ok(ComparableValue::Bool(b)),
            FlakeValue::Ref(sid) => Ok(ComparableValue::Sid(sid)),
            FlakeValue::Null => Err(NullValueError),
            FlakeValue::Vector(v) => Ok(ComparableValue::Vector(Arc::from(v))),
            FlakeValue::BigInt(n) => Ok(ComparableValue::BigInt(n)),
            FlakeValue::Decimal(d) => Ok(ComparableValue::Decimal(d)),
            FlakeValue::DateTime(dt) => Ok(ComparableValue::DateTime(*dt)),
            FlakeValue::Date(d) => Ok(ComparableValue::Date(*d)),
            FlakeValue::Time(t) => Ok(ComparableValue::Time(*t)),
            FlakeValue::GeoPoint(bits) => Ok(ComparableValue::GeoPoint(bits)),
            val @ (FlakeValue::GYear(_)
            | FlakeValue::GYearMonth(_)
            | FlakeValue::GMonth(_)
            | FlakeValue::GDay(_)
            | FlakeValue::GMonthDay(_)
            | FlakeValue::YearMonthDuration(_)
            | FlakeValue::DayTimeDuration(_)
            | FlakeValue::Duration(_)) => Ok(ComparableValue::TypedLiteral { val, dtc: None }),
        }
    }
}

// =============================================================================
// Conversions to FlakeValue
// =============================================================================

impl From<ComparableValue> for FlakeValue {
    fn from(val: ComparableValue) -> Self {
        match val {
            ComparableValue::Long(n) => FlakeValue::Long(n),
            ComparableValue::Double(d) => FlakeValue::Double(d),
            ComparableValue::String(s) => FlakeValue::String(s.to_string()),
            ComparableValue::Bool(b) => FlakeValue::Boolean(b),
            ComparableValue::Sid(sid) => FlakeValue::Ref(sid),
            ComparableValue::Vector(v) => FlakeValue::Vector(v.to_vec()),
            ComparableValue::BigInt(n) => FlakeValue::BigInt(n),
            ComparableValue::Decimal(d) => FlakeValue::Decimal(d),
            ComparableValue::DateTime(dt) => FlakeValue::DateTime(Box::new(dt)),
            ComparableValue::Date(d) => FlakeValue::Date(Box::new(d)),
            ComparableValue::Time(t) => FlakeValue::Time(Box::new(t)),
            ComparableValue::GeoPoint(bits) => FlakeValue::GeoPoint(bits),
            ComparableValue::Iri(s) => FlakeValue::String(s.to_string()),
            ComparableValue::TypedLiteral { val, .. } => val,
        }
    }
}

impl From<&ComparableValue> for FlakeValue {
    fn from(val: &ComparableValue) -> Self {
        match val {
            ComparableValue::Long(n) => FlakeValue::Long(*n),
            ComparableValue::Double(d) => FlakeValue::Double(*d),
            ComparableValue::String(s) => FlakeValue::String(s.to_string()),
            ComparableValue::Bool(b) => FlakeValue::Boolean(*b),
            ComparableValue::Sid(sid) => FlakeValue::Ref(sid.clone()),
            ComparableValue::Vector(v) => FlakeValue::Vector(v.to_vec()),
            ComparableValue::BigInt(n) => FlakeValue::BigInt(n.clone()),
            ComparableValue::Decimal(d) => FlakeValue::Decimal(d.clone()),
            ComparableValue::DateTime(dt) => FlakeValue::DateTime(Box::new(dt.clone())),
            ComparableValue::Date(d) => FlakeValue::Date(Box::new(d.clone())),
            ComparableValue::Time(t) => FlakeValue::Time(Box::new(t.clone())),
            ComparableValue::GeoPoint(bits) => FlakeValue::GeoPoint(*bits),
            ComparableValue::Iri(s) => FlakeValue::String(s.to_string()),
            ComparableValue::TypedLiteral { val, .. } => val.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    #[test]
    fn test_ebv_bool() {
        assert!(bool::from(ComparableValue::Bool(true)));
        assert!(!bool::from(ComparableValue::Bool(false)));
    }

    #[test]
    fn test_ebv_numeric() {
        assert!(bool::from(ComparableValue::Long(1)));
        assert!(!bool::from(ComparableValue::Long(0)));
        assert!(bool::from(ComparableValue::Double(0.1)));
        assert!(!bool::from(ComparableValue::Double(0.0)));
        assert!(!bool::from(ComparableValue::Double(f64::NAN)));
    }

    #[test]
    fn test_ebv_string() {
        assert!(bool::from(ComparableValue::String(Arc::from("hello"))));
        assert!(!bool::from(ComparableValue::String(Arc::from(""))));
    }

    #[test]
    fn test_arithmetic_long() {
        let a = ComparableValue::Long(10);
        let b = ComparableValue::Long(3);
        assert_eq!(
            ArithmeticOp::Add.apply(a.clone(), b.clone()),
            Ok(ComparableValue::Long(13))
        );
        assert_eq!(
            ArithmeticOp::Sub.apply(a.clone(), b.clone()),
            Ok(ComparableValue::Long(7))
        );
        assert_eq!(
            ArithmeticOp::Mul.apply(a.clone(), b.clone()),
            Ok(ComparableValue::Long(30))
        );
        // Per XPath op:numeric-divide, integer / integer yields xsd:decimal,
        // not a truncated integer.
        assert_eq!(
            ArithmeticOp::Div.apply(a, b),
            Ok(ComparableValue::Decimal(Box::new(integer_divide(
                BigDecimal::from(10),
                BigDecimal::from(3)
            ))))
        );
    }

    #[test]
    fn test_arithmetic_int_div_is_decimal() {
        // 10 / 4 = 2.5 (xsd:decimal), not 2 (truncated xsd:integer).
        assert_eq!(
            ArithmeticOp::Div.apply(ComparableValue::Long(10), ComparableValue::Long(4)),
            Ok(ComparableValue::Decimal(Box::new("2.5".parse().unwrap())))
        );
        // Exact integer quotient still yields a decimal (canonical form "5").
        assert_eq!(
            ArithmeticOp::Div.apply(ComparableValue::Long(10), ComparableValue::Long(2)),
            Ok(ComparableValue::Decimal(Box::new("5".parse().unwrap())))
        );
    }

    #[test]
    fn test_arithmetic_float_typed_literal_promotes() {
        // Bug repro: xsd:float(?a) is carried as a String-backed TypedLiteral.
        // Dividing it by an integer must promote, not error with TypeMismatch.
        let float_ten = ComparableValue::TypedLiteral {
            val: FlakeValue::String("10".to_string()),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                fluree_vocab::xsd::FLOAT,
            ))),
        };
        assert_eq!(
            ArithmeticOp::Div.apply(float_ten, ComparableValue::Long(4)),
            Ok(ComparableValue::Double(2.5))
        );
    }

    #[test]
    fn test_arithmetic_typed_literal_coercion_covers_full_xsd_lattice() {
        // xsd:short carried as a typed literal coerces for arithmetic.
        let short_five = ComparableValue::TypedLiteral {
            val: FlakeValue::String("5".to_string()),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                fluree_vocab::xsd::SHORT,
            ))),
        };
        assert_eq!(
            ArithmeticOp::Mul.apply(short_five, ComparableValue::Long(2)),
            Ok(ComparableValue::Long(10))
        );

        // A large xsd:integer beyond i64 coerces to BigInt rather than failing.
        let big = "99999999999999999999999999999"; // > i64::MAX
        let big_lit = ComparableValue::TypedLiteral {
            val: FlakeValue::String(big.to_string()),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                fluree_vocab::xsd::INTEGER,
            ))),
        };
        assert_eq!(
            ArithmeticOp::Add.apply(big_lit, ComparableValue::Long(1)),
            Ok(ComparableValue::BigInt(Box::new(
                big.parse::<BigInt>().unwrap() + 1
            )))
        );

        // The xsd:float INF lexical coerces to an infinite double.
        let inf = ComparableValue::TypedLiteral {
            val: FlakeValue::String("INF".to_string()),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                fluree_vocab::xsd::FLOAT,
            ))),
        };
        assert_eq!(
            ArithmeticOp::Mul.apply(inf, ComparableValue::Long(2)),
            Ok(ComparableValue::Double(f64::INFINITY))
        );
    }

    #[test]
    fn test_arithmetic_div_by_zero() {
        assert_eq!(
            ArithmeticOp::Div.apply(ComparableValue::Long(10), ComparableValue::Long(0)),
            Err(ArithmeticError::DivideByZero)
        );
    }

    #[test]
    fn test_try_from_flake_value() {
        // Reference conversion
        let fv = FlakeValue::Long(42);
        let cv = ComparableValue::try_from(&fv);
        assert_eq!(cv, Ok(ComparableValue::Long(42)));

        // Owned conversion
        let fv_owned = FlakeValue::Long(42);
        let cv_owned = ComparableValue::try_from(fv_owned);
        assert_eq!(cv_owned, Ok(ComparableValue::Long(42)));

        // Null returns error
        let fv_null = FlakeValue::Null;
        let cv_null = ComparableValue::try_from(&fv_null);
        assert_eq!(cv_null, Err(NullValueError));
    }

    #[test]
    fn test_into_flake_value() {
        // Reference conversion
        let cv = ComparableValue::Long(42);
        let fv: FlakeValue = (&cv).into();
        assert_eq!(fv, FlakeValue::Long(42));

        // Owned conversion
        let cv_owned = ComparableValue::Long(99);
        let fv_owned: FlakeValue = cv_owned.into();
        assert_eq!(fv_owned, FlakeValue::Long(99));
    }

    #[test]
    fn test_from_primitives() {
        // i64
        let cv: ComparableValue = 42i64.into();
        assert_eq!(cv, ComparableValue::Long(42));

        // f64
        let cv: ComparableValue = 2.5f64.into();
        assert_eq!(cv, ComparableValue::Double(2.5));

        // bool
        let cv: ComparableValue = true.into();
        assert_eq!(cv, ComparableValue::Bool(true));

        // &str
        let cv: ComparableValue = "hello".into();
        assert_eq!(cv, ComparableValue::String(Arc::from("hello")));

        // String
        let cv: ComparableValue = String::from("world").into();
        assert_eq!(cv, ComparableValue::String(Arc::from("world")));

        // Arc<str>
        let arc: Arc<str> = Arc::from("arc");
        let cv: ComparableValue = arc.into();
        assert_eq!(cv, ComparableValue::String(Arc::from("arc")));
    }

    #[test]
    fn test_as_str() {
        let cv = ComparableValue::String(Arc::from("hello"));
        assert_eq!(cv.as_str(), Some("hello"));

        let cv_long = ComparableValue::Long(42);
        assert_eq!(cv_long.as_str(), None);
    }

    #[test]
    fn test_into_string_value() {
        let cv = ComparableValue::Long(42);
        let sv = cv.into_string_value();
        assert_eq!(sv, Some(ComparableValue::String(Arc::from("42"))));
    }

    #[test]
    fn test_into_string_value_sid_internal_form() {
        // Without namespace codes, SID falls back to internal code:name form
        let sid = Sid::new(21, "packageType");
        let cv = ComparableValue::Sid(sid);
        let sv = cv.into_string_value();
        assert_eq!(
            sv,
            Some(ComparableValue::String(Arc::from("21:packageType")))
        );
    }

    #[test]
    fn test_into_string_value_with_namespaces_expands_sid() {
        use std::collections::HashMap;
        let mut ns = HashMap::new();
        ns.insert(21u16, "https://taxo.cbcrc.ca/ns/".to_string());
        ns.insert(2u16, "http://www.w3.org/2001/XMLSchema#".to_string());

        let sid = Sid::new(21, "packageType");
        let cv = ComparableValue::Sid(sid);
        let sv = cv.into_string_value_with_namespaces(Some(&ns));
        assert_eq!(
            sv,
            Some(ComparableValue::String(Arc::from(
                "https://taxo.cbcrc.ca/ns/packageType"
            )))
        );
    }

    #[test]
    fn test_into_string_value_with_namespaces_unknown_code_fallback() {
        use std::collections::HashMap;
        let ns = HashMap::new(); // empty — code 21 not found

        let sid = Sid::new(21, "packageType");
        let cv = ComparableValue::Sid(sid);
        let sv = cv.into_string_value_with_namespaces(Some(&ns));
        // Falls back to internal form
        assert_eq!(
            sv,
            Some(ComparableValue::String(Arc::from("21:packageType")))
        );
    }

    #[test]
    fn test_into_string_value_with_namespaces_none_fallback() {
        let sid = Sid::new(21, "packageType");
        let cv = ComparableValue::Sid(sid);
        let sv = cv.into_string_value_with_namespaces(None);
        // Falls back to internal form
        assert_eq!(
            sv,
            Some(ComparableValue::String(Arc::from("21:packageType")))
        );
    }

    #[test]
    fn test_into_string_value_with_namespaces_non_sid() {
        use std::collections::HashMap;
        let ns = HashMap::new();

        // Non-SID types delegate to into_string_value
        let cv = ComparableValue::Long(42);
        let sv = cv.into_string_value_with_namespaces(Some(&ns));
        assert_eq!(sv, Some(ComparableValue::String(Arc::from("42"))));
    }
}
