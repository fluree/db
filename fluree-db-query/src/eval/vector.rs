//! Vector function implementations
//!
//! Implements vector/embedding functions: dotProduct, cosineSimilarity, euclideanDistance

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;

use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_dot_product<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_vector_fn(args, row, ctx, "dotProduct", |a, b| {
        Some(a.iter().zip(b.iter()).map(|(x, y)| x * y).sum())
    })
}

pub fn eval_cosine_similarity<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_vector_fn(args, row, ctx, "cosineSimilarity", |a, b| {
        let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let mag_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
        let mag_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
        if mag_a == 0.0 || mag_b == 0.0 {
            None // mathematically undefined, not a type error
        } else {
            Some(dot / (mag_a * mag_b))
        }
    })
}

pub fn eval_euclidean_distance<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_vector_fn(args, row, ctx, "euclideanDistance", |a, b| {
        let sum_sq: f64 = a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| {
                let diff = x - y;
                diff * diff
            })
            .sum();
        Some(sum_sq.sqrt())
    })
}

/// Evaluate a binary vector function
fn eval_binary_vector_fn<R: RowAccess, F>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    fn_name: &str,
    compute: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&[f64], &[f64]) -> Option<f64>,
{
    check_arity(args, 2, fn_name)?;
    let v1 = args[0].eval_to_comparable(row, ctx)?;
    let v2 = args[1].eval_to_comparable(row, ctx)?;
    match (v1, v2) {
        (Some(ComparableValue::Vector(a)), Some(ComparableValue::Vector(b))) => {
            if a.len() != b.len() {
                Err(QueryError::InvalidFilter(format!(
                    "{} requires vectors of equal length (got {} and {})",
                    fn_name,
                    a.len(),
                    b.len()
                )))
            } else {
                Ok(compute(&a, &b).map(ComparableValue::Double))
            }
        }
        // Type mismatch or unbound -> return None (SPARQL-style graceful handling)
        _ => Ok(None),
    }
}
