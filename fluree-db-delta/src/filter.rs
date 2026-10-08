//! Column predicates pushed into a scan.
//!
//! They act twice. Kernel skips a file whose partition values or min/max
//! statistics prove no row can match ([`to_predicate`]), and the rows of the
//! files that are read are filtered before they leave the reader
//! ([`RowFilter`]). Both only ever remove rows the predicate rejects, under one
//! rule: a filter is used only when its value has the column's own physical
//! type, so the comparison made here is the one the caller would make.

use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeStringArray, RecordBatch, Scalar as ArrowScalar, StringArray, StringViewArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::compute::kernels::cmp;
use delta_kernel::arrow::compute::{filter_record_batch, or};
use delta_kernel::arrow::datatypes::{DataType as ArrowType, Schema as ArrowSchema, TimeUnit};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::expressions::{Expression, Predicate, PredicateRef, Scalar};
use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;

use crate::error::{DeltaError, Result};

/// Members beyond this make an `In` cheaper to ignore than to evaluate per file.
const MAX_IN_MEMBERS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    /// Membership; the value is a [`FilterValue::Set`].
    In,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Bool(bool),
    Int(i64),
    Double(f64),
    Str(String),
    /// Days since 1970-01-01.
    Date(i32),
    /// Microseconds since the epoch; `tz` is whether the instant is
    /// zone-anchored (`timestamp`) or wall-clock (`timestamp_ntz`).
    Timestamp {
        micros: i64,
        tz: bool,
    },
    /// A column value in its textual form, to be read as the column's own type.
    Raw(String),
    Set(Vec<FilterValue>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnFilter {
    pub column: String,
    pub op: FilterOp,
    pub value: FilterValue,
}

/// The conjunction of every filter that can be stated exactly against
/// `schema`. A filter on an unknown column, or whose value does not fit the
/// column's type, is left out rather than approximated.
pub(crate) fn to_predicate(schema: &StructType, filters: &[ColumnFilter]) -> Option<PredicateRef> {
    let parts: Vec<Predicate> = filters.iter().filter_map(|f| one(schema, f)).collect();
    match parts.len() {
        0 => None,
        1 => parts.into_iter().next().map(Arc::new),
        _ => Some(Arc::new(Predicate::and_from(parts))),
    }
}

/// One filter against `schema`: the column, the operator, and its value(s) as
/// scalars of the column's type. `None` when it cannot be
/// stated exactly. A set resolves whole or not at all: a dropped member's rows
/// could otherwise be skipped.
fn resolve<'a>(
    schema: &'a StructType,
    filter: &ColumnFilter,
) -> Option<(&'a StructField, FilterOp, Vec<Scalar>)> {
    let field = crate::table::resolve_field(schema, &filter.column)?;
    let DataType::Primitive(kind) = field.data_type() else {
        return None;
    };
    let values = match (filter.op, &filter.value) {
        (FilterOp::In, FilterValue::Set(members)) => {
            if members.is_empty() || members.len() > MAX_IN_MEMBERS {
                return None;
            }
            members
                .iter()
                .map(|m| scalar(kind, m))
                .collect::<Option<Vec<_>>>()?
        }
        (FilterOp::In, _) | (_, FilterValue::Set(_)) => return None,
        (_, value) => vec![scalar(kind, value)?],
    };
    Some((field, filter.op, values))
}

fn one(schema: &StructType, filter: &ColumnFilter) -> Option<Predicate> {
    let (field, op, values) = resolve(schema, filter)?;
    let column = || Expression::column([field.name().as_str()]);
    let mut literals = values.into_iter().map(Expression::literal);
    Some(match op {
        FilterOp::In => Predicate::or_from(literals.map(|l| Predicate::eq(column(), l))),
        FilterOp::Eq => Predicate::eq(column(), literals.next()?),
        FilterOp::NotEq => Predicate::ne(column(), literals.next()?),
        FilterOp::Lt => Predicate::lt(column(), literals.next()?),
        FilterOp::LtEq => Predicate::le(column(), literals.next()?),
        FilterOp::Gt => Predicate::gt(column(), literals.next()?),
        FilterOp::GtEq => Predicate::ge(column(), literals.next()?),
    })
}

/// The filters against columns as data files name them, for pruning inside a
/// file (see [`crate::prune`]).
pub(crate) fn file_terms(
    schema: &StructType,
    mapping: ColumnMappingMode,
    filters: &[ColumnFilter],
) -> Vec<crate::prune::Term> {
    filters
        .iter()
        .filter_map(|filter| {
            let (field, op, values) = resolve(schema, filter)?;
            Some(crate::prune::Term {
                column: field.physical_name(mapping).to_string(),
                op,
                values,
            })
        })
        .collect()
}

/// The filters as a row-level test over a scan's logical batches. A filter on
/// a column the scan does not project is left to file skipping alone.
pub(crate) struct RowFilter {
    terms: Vec<(usize, FilterOp, Vec<Scalar>)>,
}

impl RowFilter {
    pub(crate) fn new(schema: &StructType, batch: &ArrowSchema, filters: &[ColumnFilter]) -> Self {
        let terms = filters
            .iter()
            .filter_map(|filter| {
                let (field, op, values) = resolve(schema, filter)?;
                Some((batch.index_of(field.name()).ok()?, op, values))
            })
            .collect();
        Self { terms }
    }

    /// Rows passing every term. A null compares as no match, as the absent
    /// triple it stands for would.
    pub(crate) fn apply(&self, mut batch: RecordBatch) -> Result<RecordBatch> {
        for (index, op, values) in &self.terms {
            if batch.num_rows() == 0 {
                break;
            }
            let column = batch.column(*index);
            // Typed against the decoded column, which may be a wider or view
            // form of the declared type; an unexpected one is left unfiltered.
            let Some(literals) = values
                .iter()
                .map(|v| literal(v, column.data_type()))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            batch = term_mask(column, *op, &literals)
                .and_then(|mask| filter_record_batch(&batch, &mask))
                .map_err(|e| DeltaError::Internal(format!("row filter: {e}")))?;
        }
        Ok(batch)
    }
}

fn term_mask(
    column: &ArrayRef,
    op: FilterOp,
    literals: &[ArrayRef],
) -> std::result::Result<BooleanArray, ArrowError> {
    let test = |literal: &ArrayRef| {
        let literal = ArrowScalar::new(literal.clone());
        match op {
            FilterOp::Eq | FilterOp::In => cmp::eq(column, &literal),
            FilterOp::NotEq => cmp::neq(column, &literal),
            FilterOp::Lt => cmp::lt(column, &literal),
            FilterOp::LtEq => cmp::lt_eq(column, &literal),
            FilterOp::Gt => cmp::gt(column, &literal),
            FilterOp::GtEq => cmp::gt_eq(column, &literal),
        }
    };
    let mut masks = literals.iter().map(test);
    let first = masks
        .next()
        .ok_or_else(|| ArrowError::ComputeError("filter without a value".into()))??;
    masks.try_fold(first, |any, next| or(&any, &next?))
}

/// `value` as a one-element array of exactly `data_type`, or `None` where the
/// decoded column is not the type the scalar was resolved for.
fn literal(value: &Scalar, data_type: &ArrowType) -> Option<ArrayRef> {
    Some(match (value, data_type) {
        (Scalar::Long(v), ArrowType::Int64) => Arc::new(Int64Array::from(vec![*v])),
        (Scalar::Integer(v), ArrowType::Int32) => Arc::new(Int32Array::from(vec![*v])),
        (Scalar::Short(v), ArrowType::Int16) => Arc::new(Int16Array::from(vec![*v])),
        (Scalar::Byte(v), ArrowType::Int8) => Arc::new(Int8Array::from(vec![*v])),
        (Scalar::Double(v), ArrowType::Float64) => Arc::new(Float64Array::from(vec![*v])),
        (Scalar::String(v), ArrowType::Utf8) => Arc::new(StringArray::from(vec![v.as_str()])),
        (Scalar::String(v), ArrowType::LargeUtf8) => {
            Arc::new(LargeStringArray::from(vec![v.as_str()]))
        }
        (Scalar::String(v), ArrowType::Utf8View) => {
            Arc::new(StringViewArray::from(vec![v.as_str()]))
        }
        (Scalar::Boolean(v), ArrowType::Boolean) => Arc::new(BooleanArray::from(vec![*v])),
        (Scalar::Date(v), ArrowType::Date32) => Arc::new(Date32Array::from(vec![*v])),
        (Scalar::Timestamp(v), ArrowType::Timestamp(TimeUnit::Microsecond, Some(zone))) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v]).with_timezone(zone.clone()))
        }
        (Scalar::TimestampNtz(v), ArrowType::Timestamp(TimeUnit::Microsecond, None)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v]))
        }
        _ => return None,
    })
}

fn scalar(kind: &PrimitiveType, value: &FilterValue) -> Option<Scalar> {
    use PrimitiveType as P;
    Some(match (kind, value) {
        (P::Long, FilterValue::Int(n)) => Scalar::Long(*n),
        (P::Integer, FilterValue::Int(n)) => Scalar::Integer(i32::try_from(*n).ok()?),
        (P::Short, FilterValue::Int(n)) => Scalar::Short(i16::try_from(*n).ok()?),
        (P::Byte, FilterValue::Int(n)) => Scalar::Byte(i8::try_from(*n).ok()?),
        // `float` statistics would need the literal narrowed, which can round
        // it across a file's bound. Zero is left out because -0.0 equals 0.0
        // numerically but sorts below it in Arrow's total order.
        (P::Double, FilterValue::Double(d)) if !d.is_nan() && *d != 0.0 => Scalar::Double(*d),
        (P::String, FilterValue::Str(s)) => Scalar::String(s.clone()),
        (P::Boolean, FilterValue::Bool(b)) => Scalar::Boolean(*b),
        (P::Date, FilterValue::Date(days)) => Scalar::Date(*days),
        (P::Timestamp, FilterValue::Timestamp { micros, tz: true }) => Scalar::Timestamp(*micros),
        (P::TimestampNtz, FilterValue::Timestamp { micros, tz: false }) => {
            Scalar::TimestampNtz(*micros)
        }
        (P::String, FilterValue::Raw(s)) => Scalar::String(s.clone()),
        (P::Long | P::Integer | P::Short | P::Byte, FilterValue::Raw(s)) => {
            // Only the canonical rendering: "07" is a different key than "7".
            let n: i64 = s.parse().ok()?;
            if n.to_string() != *s {
                return None;
            }
            return scalar(kind, &FilterValue::Int(n));
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::schema::StructField;

    fn schema() -> StructType {
        StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("small", DataType::SHORT),
            StructField::nullable("approx", DataType::FLOAT),
            StructField::nullable("label", DataType::STRING),
            StructField::nullable("at", DataType::TIMESTAMP),
            StructField::nullable("wall", DataType::TIMESTAMP_NTZ),
        ])
        .unwrap()
    }

    fn filter(column: &str, op: FilterOp, value: FilterValue) -> ColumnFilter {
        ColumnFilter {
            column: column.into(),
            op,
            value,
        }
    }

    #[test]
    fn a_value_that_does_not_fit_the_column_is_left_out() {
        let s = schema();
        let none = |f: ColumnFilter| assert!(to_predicate(&s, &[f]).is_none());
        none(filter("missing", FilterOp::Eq, FilterValue::Int(1)));
        none(filter("small", FilterOp::Eq, FilterValue::Int(70_000)));
        none(filter("approx", FilterOp::Gt, FilterValue::Double(1.5)));
        none(filter("label", FilterOp::Eq, FilterValue::Int(1)));
        none(filter("id", FilterOp::Eq, FilterValue::Raw("07".into())));
        // A zoned instant does not compare against a wall-clock column.
        let zoned = FilterValue::Timestamp {
            micros: 1,
            tz: true,
        };
        none(filter("wall", FilterOp::Lt, zoned.clone()));
        assert!(to_predicate(&s, &[filter("at", FilterOp::Lt, zoned)]).is_some());
    }

    #[test]
    fn a_set_is_pushed_whole_or_not_at_all() {
        let s = schema();
        let set = |members| filter("small", FilterOp::In, FilterValue::Set(members));
        assert!(to_predicate(&s, &[set(vec![FilterValue::Int(1), FilterValue::Int(2)])]).is_some());
        assert!(to_predicate(
            &s,
            &[set(vec![FilterValue::Int(1), FilterValue::Int(70_000)])]
        )
        .is_none());
        assert!(to_predicate(&s, &[set(vec![])]).is_none());
    }

    #[test]
    fn an_unusable_filter_does_not_discard_the_usable_ones() {
        let s = schema();
        let kept = to_predicate(
            &s,
            &[
                filter("missing", FilterOp::Eq, FilterValue::Int(1)),
                filter("id", FilterOp::Gt, FilterValue::Int(5)),
            ],
        )
        .expect("the id filter survives");
        assert_eq!(
            *kept,
            Predicate::gt(Expression::column(["id"]), Expression::literal(5i64))
        );
    }
}
