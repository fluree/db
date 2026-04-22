//! Filter predicate expressions for scan planning.
//!
//! This module defines the expression language for filter predicates that can
//! be used for:
//! - Partition pruning at the manifest level
//! - File pruning using column statistics
//! - Residual filtering on actual row data
//!
//! # Design
//!
//! - `field_id` is the CANONICAL identifier for columns (matches Iceberg's schema)
//! - `column` name is stored for debug/error messages only - never use it for lookup
//! - Expressions form a tree with boolean combinators (And, Or, Not)

use crate::manifest::TypedValue;

/// Comparison operators for filter expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    /// Equal to (=)
    Eq,
    /// Not equal to (!=)
    NotEq,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    LtEq,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    GtEq,
}

impl ComparisonOp {
    /// Get the negation of this operator.
    pub fn negate(&self) -> Self {
        match self {
            Self::Eq => Self::NotEq,
            Self::NotEq => Self::Eq,
            Self::Lt => Self::GtEq,
            Self::LtEq => Self::Gt,
            Self::Gt => Self::LtEq,
            Self::GtEq => Self::Lt,
        }
    }

    /// Check if this is an equality operator.
    pub fn is_equality(&self) -> bool {
        matches!(self, Self::Eq | Self::NotEq)
    }
}

impl std::fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::LtEq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::GtEq => write!(f, ">="),
        }
    }
}

/// Literal values for comparisons.
///
/// These match Iceberg's type system and can be compared against decoded bounds.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    /// Date: days since 1970-01-01
    Date(i32),
    /// Timestamp: microseconds since epoch
    Timestamp(i64),
    /// Decimal: (unscaled_value, precision, scale)
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
}

impl LiteralValue {
    /// Convert to TypedValue for comparison with decoded bounds.
    pub fn to_typed_value(&self) -> TypedValue {
        match self {
            Self::Boolean(v) => TypedValue::Boolean(*v),
            Self::Int32(v) => TypedValue::Int32(*v),
            Self::Int64(v) => TypedValue::Int64(*v),
            Self::Float32(v) => TypedValue::Float32(*v),
            Self::Float64(v) => TypedValue::Float64(*v),
            Self::String(v) => TypedValue::String(v.clone()),
            Self::Bytes(v) => TypedValue::Bytes(v.clone()),
            Self::Date(v) => TypedValue::Date(*v),
            Self::Timestamp(v) => TypedValue::Timestamp(*v),
            Self::Decimal {
                unscaled,
                precision,
                scale,
            } => TypedValue::Decimal {
                unscaled: *unscaled,
                precision: *precision,
                scale: *scale,
            },
        }
    }

    /// Create from TypedValue.
    pub fn from_typed_value(value: &TypedValue) -> Self {
        match value {
            TypedValue::Boolean(v) => Self::Boolean(*v),
            TypedValue::Int32(v) => Self::Int32(*v),
            TypedValue::Int64(v) => Self::Int64(*v),
            TypedValue::Float32(v) => Self::Float32(*v),
            TypedValue::Float64(v) => Self::Float64(*v),
            TypedValue::String(v) => Self::String(v.clone()),
            TypedValue::Bytes(v) => Self::Bytes(v.clone()),
            TypedValue::Date(v) => Self::Date(*v),
            TypedValue::Timestamp(v) | TypedValue::TimestampTz(v) => Self::Timestamp(*v),
            TypedValue::Uuid(v) => Self::Bytes(v.to_vec()),
            TypedValue::Decimal {
                unscaled,
                precision,
                scale,
            } => Self::Decimal {
                unscaled: *unscaled,
                precision: *precision,
                scale: *scale,
            },
        }
    }
}

impl std::fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Boolean(v) => write!(f, "{v}"),
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}L"),
            Self::Float32(v) => write!(f, "{v}f"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::String(v) => write!(f, "'{v}'"),
            Self::Bytes(v) => write!(f, "bytes[{}]", v.len()),
            Self::Date(v) => write!(f, "date({v})"),
            Self::Timestamp(v) => write!(f, "ts({v})"),
            Self::Decimal {
                unscaled, scale, ..
            } => {
                write!(f, "decimal({unscaled}, {scale})")
            }
        }
    }
}

/// Filter expression for predicate pushdown.
///
/// # Design
///
/// - `field_id` is the CANONICAL identifier (matches Iceberg schema field IDs)
/// - `column` name is for debug/error messages ONLY - never use for lookup
/// - Expressions are immutable trees, combined via boolean operators
#[derive(Debug, Clone)]
pub enum Expression {
    /// Always true (no filtering)
    AlwaysTrue,
    /// Always false (no results)
    AlwaysFalse,
    /// Logical NOT
    Not(Box<Expression>),
    /// Logical AND of multiple expressions
    And(Vec<Expression>),
    /// Logical OR of multiple expressions
    Or(Vec<Expression>),
    /// IS NULL check
    IsNull {
        /// Iceberg field ID (canonical)
        field_id: i32,
        /// Column name (debug/UX only)
        column: String,
    },
    /// IS NOT NULL check
    IsNotNull {
        /// Iceberg field ID (canonical)
        field_id: i32,
        /// Column name (debug/UX only)
        column: String,
    },
    /// Comparison (=, !=, <, <=, >, >=)
    Comparison {
        /// Iceberg field ID (canonical)
        field_id: i32,
        /// Column name (debug/UX only)
        column: String,
        /// Comparison operator
        op: ComparisonOp,
        /// Literal value to compare against
        value: LiteralValue,
    },
    /// IN list check
    In {
        /// Iceberg field ID (canonical)
        field_id: i32,
        /// Column name (debug/UX only)
        column: String,
        /// Values to check membership in
        values: Vec<LiteralValue>,
    },
    /// NOT IN list check
    NotIn {
        /// Iceberg field ID (canonical)
        field_id: i32,
        /// Column name (debug/UX only)
        column: String,
        /// Values to check non-membership in
        values: Vec<LiteralValue>,
    },
}

impl Expression {
    /// Create an equality comparison.
    pub fn eq(field_id: i32, column: impl Into<String>, value: LiteralValue) -> Self {
        Self::Comparison {
            field_id,
            column: column.into(),
            op: ComparisonOp::Eq,
            value,
        }
    }

    /// Create a not-equal comparison.
    pub fn not_eq(field_id: i32, column: impl Into<String>, value: LiteralValue) -> Self {
        Self::Comparison {
            field_id,
            column: column.into(),
            op: ComparisonOp::NotEq,
            value,
        }
    }

    /// Create a less-than comparison.
    pub fn lt(field_id: i32, column: impl Into<String>, value: LiteralValue) -> Self {
        Self::Comparison {
            field_id,
            column: column.into(),
            op: ComparisonOp::Lt,
            value,
        }
    }

    /// Create a less-than-or-equal comparison.
    pub fn lt_eq(field_id: i32, column: impl Into<String>, value: LiteralValue) -> Self {
        Self::Comparison {
            field_id,
            column: column.into(),
            op: ComparisonOp::LtEq,
            value,
        }
    }

    /// Create a greater-than comparison.
    pub fn gt(field_id: i32, column: impl Into<String>, value: LiteralValue) -> Self {
        Self::Comparison {
            field_id,
            column: column.into(),
            op: ComparisonOp::Gt,
            value,
        }
    }

    /// Create a greater-than-or-equal comparison.
    pub fn gt_eq(field_id: i32, column: impl Into<String>, value: LiteralValue) -> Self {
        Self::Comparison {
            field_id,
            column: column.into(),
            op: ComparisonOp::GtEq,
            value,
        }
    }

    /// Create an IS NULL check.
    pub fn is_null(field_id: i32, column: impl Into<String>) -> Self {
        Self::IsNull {
            field_id,
            column: column.into(),
        }
    }

    /// Create an IS NOT NULL check.
    pub fn is_not_null(field_id: i32, column: impl Into<String>) -> Self {
        Self::IsNotNull {
            field_id,
            column: column.into(),
        }
    }

    /// Create an IN list check.
    pub fn in_list(field_id: i32, column: impl Into<String>, values: Vec<LiteralValue>) -> Self {
        Self::In {
            field_id,
            column: column.into(),
            values,
        }
    }

    /// Create a NOT IN list check.
    pub fn not_in_list(
        field_id: i32,
        column: impl Into<String>,
        values: Vec<LiteralValue>,
    ) -> Self {
        Self::NotIn {
            field_id,
            column: column.into(),
            values,
        }
    }

    /// Create a logical AND of expressions.
    pub fn and(exprs: Vec<Expression>) -> Self {
        // Flatten nested ANDs and filter out AlwaysTrue
        let mut flattened = Vec::new();
        for expr in exprs {
            match expr {
                Expression::AlwaysTrue => continue,
                Expression::AlwaysFalse => return Expression::AlwaysFalse,
                Expression::And(inner) => flattened.extend(inner),
                other => flattened.push(other),
            }
        }

        match flattened.len() {
            0 => Expression::AlwaysTrue,
            1 => flattened.into_iter().next().unwrap(),
            _ => Expression::And(flattened),
        }
    }

    /// Create a logical OR of expressions.
    pub fn or(exprs: Vec<Expression>) -> Self {
        // Flatten nested ORs and filter out AlwaysFalse
        let mut flattened = Vec::new();
        for expr in exprs {
            match expr {
                Expression::AlwaysFalse => continue,
                Expression::AlwaysTrue => return Expression::AlwaysTrue,
                Expression::Or(inner) => flattened.extend(inner),
                other => flattened.push(other),
            }
        }

        match flattened.len() {
            0 => Expression::AlwaysFalse,
            1 => flattened.into_iter().next().unwrap(),
            _ => Expression::Or(flattened),
        }
    }

    /// Create a logical NOT.
    pub fn negate(expr: Expression) -> Self {
        match expr {
            Expression::AlwaysTrue => Expression::AlwaysFalse,
            Expression::AlwaysFalse => Expression::AlwaysTrue,
            Expression::Not(inner) => *inner,
            other => Expression::Not(Box::new(other)),
        }
    }

    /// Check if this expression is always true.
    pub fn is_always_true(&self) -> bool {
        matches!(self, Expression::AlwaysTrue)
    }

    /// Check if this expression is always false.
    pub fn is_always_false(&self) -> bool {
        matches!(self, Expression::AlwaysFalse)
    }

    /// Get all field IDs referenced by this expression.
    pub fn referenced_field_ids(&self) -> Vec<i32> {
        let mut ids = Vec::new();
        self.collect_field_ids(&mut ids);
        ids.sort();
        ids.dedup();
        ids
    }

    fn collect_field_ids(&self, ids: &mut Vec<i32>) {
        match self {
            Expression::AlwaysTrue | Expression::AlwaysFalse => {}
            Expression::Not(inner) => inner.collect_field_ids(ids),
            Expression::And(exprs) | Expression::Or(exprs) => {
                for expr in exprs {
                    expr.collect_field_ids(ids);
                }
            }
            Expression::IsNull { field_id, .. }
            | Expression::IsNotNull { field_id, .. }
            | Expression::Comparison { field_id, .. }
            | Expression::In { field_id, .. }
            | Expression::NotIn { field_id, .. } => {
                ids.push(*field_id);
            }
        }
    }
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::AlwaysTrue => write!(f, "TRUE"),
            Expression::AlwaysFalse => write!(f, "FALSE"),
            Expression::Not(inner) => write!(f, "NOT ({inner})"),
            Expression::And(exprs) => {
                let parts: Vec<String> =
                    exprs.iter().map(std::string::ToString::to_string).collect();
                write!(f, "({})", parts.join(" AND "))
            }
            Expression::Or(exprs) => {
                let parts: Vec<String> =
                    exprs.iter().map(std::string::ToString::to_string).collect();
                write!(f, "({})", parts.join(" OR "))
            }
            Expression::IsNull { column, .. } => write!(f, "{column} IS NULL"),
            Expression::IsNotNull { column, .. } => write!(f, "{column} IS NOT NULL"),
            Expression::Comparison {
                column, op, value, ..
            } => {
                write!(f, "{column} {op} {value}")
            }
            Expression::In { column, values, .. } => {
                let vals: Vec<String> = values
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect();
                write!(f, "{} IN ({})", column, vals.join(", "))
            }
            Expression::NotIn { column, values, .. } => {
                let vals: Vec<String> = values
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect();
                write!(f, "{} NOT IN ({})", column, vals.join(", "))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comparison_op_negate() {
        assert_eq!(ComparisonOp::Eq.negate(), ComparisonOp::NotEq);
        assert_eq!(ComparisonOp::Lt.negate(), ComparisonOp::GtEq);
        assert_eq!(ComparisonOp::LtEq.negate(), ComparisonOp::Gt);
        assert_eq!(ComparisonOp::Gt.negate(), ComparisonOp::LtEq);
        assert_eq!(ComparisonOp::GtEq.negate(), ComparisonOp::Lt);
    }

    #[test]
    fn test_expression_builders() {
        let expr = Expression::eq(1, "id", LiteralValue::Int64(42));
        assert!(matches!(
            expr,
            Expression::Comparison {
                field_id: 1,
                op: ComparisonOp::Eq,
                ..
            }
        ));

        let expr = Expression::gt(2, "value", LiteralValue::Float64(3.13));
        assert!(matches!(
            expr,
            Expression::Comparison {
                field_id: 2,
                op: ComparisonOp::Gt,
                ..
            }
        ));
    }

    #[test]
    fn test_expression_and_simplification() {
        // AND with AlwaysTrue is simplified
        let expr = Expression::and(vec![
            Expression::AlwaysTrue,
            Expression::eq(1, "id", LiteralValue::Int32(1)),
        ]);
        assert!(matches!(expr, Expression::Comparison { .. }));

        // AND with AlwaysFalse becomes AlwaysFalse
        let expr = Expression::and(vec![
            Expression::AlwaysFalse,
            Expression::eq(1, "id", LiteralValue::Int32(1)),
        ]);
        assert!(matches!(expr, Expression::AlwaysFalse));

        // Empty AND becomes AlwaysTrue
        let expr = Expression::and(vec![]);
        assert!(matches!(expr, Expression::AlwaysTrue));
    }

    #[test]
    fn test_expression_or_simplification() {
        // OR with AlwaysFalse is simplified
        let expr = Expression::or(vec![
            Expression::AlwaysFalse,
            Expression::eq(1, "id", LiteralValue::Int32(1)),
        ]);
        assert!(matches!(expr, Expression::Comparison { .. }));

        // OR with AlwaysTrue becomes AlwaysTrue
        let expr = Expression::or(vec![
            Expression::AlwaysTrue,
            Expression::eq(1, "id", LiteralValue::Int32(1)),
        ]);
        assert!(matches!(expr, Expression::AlwaysTrue));

        // Empty OR becomes AlwaysFalse
        let expr = Expression::or(vec![]);
        assert!(matches!(expr, Expression::AlwaysFalse));
    }

    #[test]
    fn test_expression_not_simplification() {
        // NOT TRUE = FALSE
        let expr = Expression::negate(Expression::AlwaysTrue);
        assert!(matches!(expr, Expression::AlwaysFalse));

        // NOT FALSE = TRUE
        let expr = Expression::negate(Expression::AlwaysFalse);
        assert!(matches!(expr, Expression::AlwaysTrue));

        // NOT NOT expr = expr
        let inner = Expression::eq(1, "id", LiteralValue::Int32(1));
        let expr = Expression::negate(Expression::negate(inner.clone()));
        // Should be the same comparison
        assert!(matches!(expr, Expression::Comparison { .. }));
    }

    #[test]
    fn test_referenced_field_ids() {
        let expr = Expression::and(vec![
            Expression::eq(1, "id", LiteralValue::Int32(1)),
            Expression::gt(2, "value", LiteralValue::Float64(3.13)),
            Expression::is_not_null(3, "name"),
        ]);

        let ids = expr.referenced_field_ids();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_expression_display() {
        let expr = Expression::and(vec![
            Expression::eq(1, "id", LiteralValue::Int64(42)),
            Expression::gt(2, "value", LiteralValue::Float64(3.13)),
        ]);

        let s = expr.to_string();
        assert!(s.contains("id = 42L"));
        assert!(s.contains("value > 3.13"));
        assert!(s.contains("AND"));
    }

    #[test]
    fn test_literal_value_typed_conversion() {
        let lit = LiteralValue::Int64(42);
        let typed = lit.to_typed_value();
        assert_eq!(typed, TypedValue::Int64(42));

        let back = LiteralValue::from_typed_value(&typed);
        assert_eq!(back, lit);
    }
}
