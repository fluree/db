//! Aggregate functions and operator for GROUP BY queries
//!
//! Implements SPARQL aggregate functions that operate on `Grouped(Vec<Binding>)` values:
//! - COUNT, COUNT_DISTINCT - count values (excluding Unbound)
//! - SUM, AVG - numeric sum/average
//! - MIN, MAX - minimum/maximum by comparison
//! - MEDIAN, VARIANCE, STDDEV - statistical functions
//! - GROUP_CONCAT - concatenate strings
//! - SAMPLE - return an arbitrary value from the group
//!
//! # Type Handling
//!
//! - Numeric aggregates (SUM, AVG, etc.) skip non-numeric values
//! - Empty input → Unbound (except COUNT → 0)
//! - Mixed types → promote to double where possible
//! - Unbound values are skipped

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{FlakeValue, Sid};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

/// Aggregate function types.
///
/// DISTINCT handling is split: `COUNT(DISTINCT)` has a dedicated variant
/// (`CountDistinct`) because its streaming state uses a HashSet rather than
/// a simple counter. All other DISTINCT aggregates (SUM, AVG, etc.) use
/// their normal variant with `AggregateSpec::distinct = true`, and dedup
/// is applied at execution time by `apply_aggregate()`.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFn {
    /// COUNT - count non-Unbound values of a variable
    Count,
    /// COUNT(*) - count all rows in a group (regardless of variable values)
    CountAll,
    /// COUNT(DISTINCT) - count distinct non-Unbound values (dedicated variant
    /// for streaming HashSet state; `AggregateSpec::distinct` is false for this)
    CountDistinct,
    /// SUM - numeric sum
    Sum,
    /// AVG - numeric average
    Avg,
    /// MIN - minimum value by comparison
    Min,
    /// MAX - maximum value by comparison
    Max,
    /// MEDIAN - median value
    Median,
    /// VARIANCE - population variance
    Variance,
    /// STDDEV - population standard deviation
    Stddev,
    /// GROUP_CONCAT - concatenate strings with separator
    GroupConcat { separator: String },
    /// SAMPLE - return an arbitrary value
    Sample,
}

/// Specification for a single aggregate operation
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    /// The aggregate function to apply
    pub function: AggregateFn,
    /// Input variable (should contain Grouped values after GROUP BY)
    /// None for COUNT(*) which counts all rows regardless of variable values
    pub input_var: Option<VarId>,
    /// Output variable for the aggregate result
    pub output_var: VarId,
    /// Whether DISTINCT was specified (e.g., SUM(DISTINCT ?x))
    pub distinct: bool,
}

/// Aggregate operator - applies aggregate functions to grouped values
///
/// Expects input from `GroupByOperator` where non-grouped columns contain
/// `Grouped(Vec<Binding>)` values. Replaces those with aggregate results.
pub struct AggregateOperator {
    /// Child operator (typically GroupByOperator)
    child: BoxedOperator,
    /// Aggregate specifications
    aggregates: Vec<AggregateSpec>,
    /// Output schema
    in_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Mapping from input column index to aggregate spec index (in-place aggregates)
    aggregate_map: Vec<Option<usize>>,
    /// Extra aggregates that append new columns (agg_idx, input_col_idx, output_col_idx)
    extra_specs: Vec<(usize, Option<usize>, usize)>,
    /// Column index to use for determining group size (first Grouped column)
    /// Used by CountAll to determine how many rows are in each group
    group_size_col: Option<usize>,
    /// Number of columns from child schema (before extra additions)
    child_col_count: usize,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl AggregateOperator {
    /// Create a new aggregate operator
    ///
    /// # Arguments
    ///
    /// * `child` - Child operator (typically GroupByOperator)
    /// * `aggregates` - List of aggregate specifications
    ///
    /// The output schema includes:
    /// - All columns from child (with aggregate input vars replaced by output vars)
    /// - Additional columns for COUNT(*) aggregates (which have no input var)
    pub fn new(child: BoxedOperator, aggregates: Vec<AggregateSpec>) -> Self {
        let child_schema = child.schema().to_vec();
        let child_col_count = child_schema.len();

        // Build output schema: start with child schema, but replace
        // aggregate input vars with output vars
        let mut output_vars = child_schema.clone();
        let mut aggregate_map: Vec<Option<usize>> = vec![None; child_col_count];
        let mut extra_specs: Vec<(usize, Option<usize>, usize)> = Vec::new();
        let mut group_size_col: Option<usize> = None;

        for (agg_idx, spec) in aggregates.iter().enumerate() {
            match spec.input_var {
                Some(input_var) => {
                    // Regular aggregate with input variable
                    if let Some(col_idx) = child_schema.iter().position(|v| *v == input_var) {
                        // Track this column for group size detection (any Grouped column works)
                        if group_size_col.is_none() {
                            group_size_col = Some(col_idx);
                        }
                        if spec.output_var == input_var {
                            aggregate_map[col_idx] = Some(agg_idx);
                        } else {
                            let output_col_idx = output_vars.len();
                            output_vars.push(spec.output_var);
                            extra_specs.push((agg_idx, Some(col_idx), output_col_idx));
                        }
                    }
                }
                None => {
                    // COUNT(*) - no input variable, add as new column
                    let output_col_idx = output_vars.len();
                    output_vars.push(spec.output_var);
                    extra_specs.push((agg_idx, None, output_col_idx));
                }
            }
        }

        // If no regular aggregates found a Grouped column, find any non-key column
        // (In practice, GROUP BY always produces at least one Grouped column unless
        // all columns are keys, in which case group size is always 1)
        if group_size_col.is_none()
            && extra_specs
                .iter()
                .any(|(_, input_col, _)| input_col.is_none())
        {
            // Use the first column as fallback - it might be a group key (size=1)
            // or the batch might be empty
            group_size_col = Some(0);
        }

        let schema: Arc<[VarId]> = Arc::from(output_vars.into_boxed_slice());

        Self {
            child,
            aggregates,
            in_schema: schema,
            state: OperatorState::Created,
            aggregate_map,
            extra_specs,
            group_size_col,
            child_col_count,
            out_schema: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }
}

#[async_trait]
impl Operator for AggregateOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        let span = tracing::debug_span!(
            "aggregate_batch",
            aggregates = self.aggregates.len(),
            rows_in = tracing::field::Empty,
            ms = tracing::field::Empty
        );
        async {
            let span = tracing::Span::current();
            let start = Instant::now();

            let batch = match self.child.next_batch(ctx).await? {
                Some(b) => b,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            if batch.is_empty() {
                return Ok(Some(Batch::empty(self.in_schema.clone())?));
            }

            span.record("rows_in", batch.len() as u64);

            let num_cols = self.in_schema.len();
            let mut output_columns: Vec<Vec<Binding>> = Vec::with_capacity(num_cols);

            // Process child columns (regular aggregates and pass-through)
            for col_idx in 0..self.child_col_count {
                let mut col_output = Vec::with_capacity(batch.len());

                for row_idx in 0..batch.len() {
                    let input_binding = batch.get_by_col(row_idx, col_idx);

                    let output_binding = match self.aggregate_map.get(col_idx).copied().flatten() {
                        Some(agg_idx) => {
                            // This column needs aggregation
                            let spec = &self.aggregates[agg_idx];
                            apply_aggregate(&spec.function, input_binding, spec.distinct)
                        }
                        None => {
                            // Pass through unchanged
                            input_binding.clone()
                        }
                    };

                    col_output.push(output_binding);
                }

                output_columns.push(col_output);
            }

            if !self.extra_specs.is_empty() {
                let mut group_sizes: Option<Vec<i64>> = None;

                for (agg_idx, input_col, _output_col_idx) in &self.extra_specs {
                    let spec = &self.aggregates[*agg_idx];
                    let col_output: Vec<Binding> = match input_col {
                        Some(col_idx) => (0..batch.len())
                            .map(|row_idx| {
                                let input_binding = batch.get_by_col(row_idx, *col_idx);
                                apply_aggregate(&spec.function, input_binding, spec.distinct)
                            })
                            .collect(),
                        None => {
                            let sizes = group_sizes.get_or_insert_with(|| {
                                (0..batch.len())
                                    .map(|row_idx| {
                                        if let Some(col_idx) = self.group_size_col {
                                            let binding = batch.get_by_col(row_idx, col_idx);
                                            if let Binding::Grouped(values) = binding {
                                                return values.len() as i64;
                                            }
                                        }

                                        for col_idx in 0..self.child_col_count {
                                            let binding = batch.get_by_col(row_idx, col_idx);
                                            if let Binding::Grouped(values) = binding {
                                                return values.len() as i64;
                                            }
                                        }

                                        1
                                    })
                                    .collect()
                            });
                            sizes
                                .iter()
                                .map(|&size| Binding::lit(FlakeValue::Long(size), xsd_integer()))
                                .collect()
                        }
                    };
                    output_columns.push(col_output);
                }
            }

            let out = Batch::new(self.in_schema.clone(), output_columns)?;
            span.record("ms", (start.elapsed().as_secs_f64() * 1000.0) as u64);
            Ok(trim_batch(&self.out_schema, out))
        }
        .instrument(span)
        .await
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.child.estimated_rows()
    }
}

/// Apply an aggregate function to a binding
///
/// If the binding is `Grouped(values)`, compute the aggregate.
/// Otherwise, pass through unchanged (shouldn't happen in normal usage).
/// When `distinct` is true, deduplicates values before aggregation.
pub fn apply_aggregate(func: &AggregateFn, binding: &Binding, distinct: bool) -> Binding {
    match binding {
        Binding::Grouped(values) => {
            if distinct {
                let mut seen = HashSet::with_capacity(values.len());
                let deduped: Vec<Binding> =
                    values.iter().filter(|b| seen.insert(*b)).cloned().collect();
                compute_aggregate(func, &deduped)
            } else {
                compute_aggregate(func, values)
            }
        }
        // Non-grouped values pass through (e.g., group key columns)
        other => other.clone(),
    }
}

/// Compute aggregate over a list of bindings
fn compute_aggregate(func: &AggregateFn, values: &[Binding]) -> Binding {
    match func {
        AggregateFn::Count => agg_count(values),
        AggregateFn::CountAll => agg_count_all(values),
        AggregateFn::CountDistinct => agg_count_distinct(values),
        AggregateFn::Sum => agg_sum(values),
        AggregateFn::Avg => agg_avg(values),
        AggregateFn::Min => agg_min(values),
        AggregateFn::Max => agg_max(values),
        AggregateFn::Median => agg_median(values),
        AggregateFn::Variance => agg_variance(values),
        AggregateFn::Stddev => agg_stddev(values),
        AggregateFn::GroupConcat { separator } => agg_group_concat(values, separator),
        AggregateFn::Sample => agg_sample(values),
    }
}

fn xsd_integer() -> Sid {
    Sid::xsd_integer()
}

fn xsd_double() -> Sid {
    Sid::xsd_double()
}

fn xsd_string() -> Sid {
    Sid::xsd_string()
}

/// COUNT - count non-Unbound values
fn agg_count(values: &[Binding]) -> Binding {
    let count = values
        .iter()
        .filter(|b| !matches!(b, Binding::Unbound | Binding::Poisoned))
        .count();
    Binding::lit(FlakeValue::Long(count as i64), xsd_integer())
}

/// COUNT(*) - count all rows (including Unbound/Poisoned)
fn agg_count_all(values: &[Binding]) -> Binding {
    Binding::lit(FlakeValue::Long(values.len() as i64), xsd_integer())
}

/// COUNT(DISTINCT) - count distinct non-Unbound values
fn agg_count_distinct(values: &[Binding]) -> Binding {
    let distinct: HashSet<_> = values
        .iter()
        .filter(|b| !matches!(b, Binding::Unbound | Binding::Poisoned))
        .collect();
    Binding::lit(FlakeValue::Long(distinct.len() as i64), xsd_integer())
}

/// SUM - numeric sum with W3C SPARQL §17.4.1.7 type promotion.
///
/// Result type is the widest tier observed across the group:
/// - all `xsd:integer` → `xsd:integer` (BigInt-backed when overflowing i64)
/// - any `xsd:decimal` mixed with integers → `xsd:decimal`
/// - any `xsd:float` (no double) → `xsd:float`
/// - any `xsd:double` → `xsd:double`
///
/// Empty input returns `Binding::Unbound`. Non-numeric inputs are skipped
/// silently (matching SPARQL's permissive aggregate semantics).
fn agg_sum(values: &[Binding]) -> Binding {
    let mut accum = crate::numeric_tier::NumericAccum::new();
    for v in values {
        accum.add(v);
    }
    accum.finalize_sum()
}

/// AVG - numeric average with W3C SPARQL §17.4.1.7 type promotion.
///
/// Result tier mirrors SUM with one twist: an all-integer group widens to
/// `xsd:decimal` because SPARQL's `integer ÷ integer` is decimal-typed.
/// An empty group returns `0` `xsd:integer` per W3C test `agg-avg-03`
/// ("AVG with empty group (value defined to be 0)").
fn agg_avg(values: &[Binding]) -> Binding {
    let mut accum = crate::numeric_tier::NumericAccum::new();
    for v in values {
        accum.add(v);
    }
    accum.finalize_avg()
}

/// MIN - minimum value
fn agg_min(values: &[Binding]) -> Binding {
    values
        .iter()
        .filter(|b| {
            !matches!(
                b,
                Binding::Unbound | Binding::Poisoned | Binding::Grouped(_)
            )
        })
        .min_by(|a, b| crate::sort::compare_bindings(a, b))
        .cloned()
        .unwrap_or(Binding::Unbound)
}

/// MAX - maximum value
fn agg_max(values: &[Binding]) -> Binding {
    values
        .iter()
        .filter(|b| {
            !matches!(
                b,
                Binding::Unbound | Binding::Poisoned | Binding::Grouped(_)
            )
        })
        .max_by(|a, b| crate::sort::compare_bindings(a, b))
        .cloned()
        .unwrap_or(Binding::Unbound)
}

/// MEDIAN - median numeric value
fn agg_median(values: &[Binding]) -> Binding {
    let mut numbers = extract_numbers(values);
    if numbers.is_empty() {
        return Binding::Unbound;
    }

    numbers.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let len = numbers.len();
    let median = if len.is_multiple_of(2) {
        f64::midpoint(numbers[len / 2 - 1], numbers[len / 2])
    } else {
        numbers[len / 2]
    };

    Binding::lit(FlakeValue::Double(median), xsd_double())
}

/// VARIANCE - population variance
fn agg_variance(values: &[Binding]) -> Binding {
    let numbers = extract_numbers(values);
    if numbers.is_empty() {
        return Binding::Unbound;
    }

    let n = numbers.len() as f64;
    let mean: f64 = numbers.iter().sum::<f64>() / n;
    let variance: f64 = numbers.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;

    Binding::lit(FlakeValue::Double(variance), xsd_double())
}

/// STDDEV - population standard deviation
fn agg_stddev(values: &[Binding]) -> Binding {
    let numbers = extract_numbers(values);
    if numbers.is_empty() {
        return Binding::Unbound;
    }

    let n = numbers.len() as f64;
    let mean: f64 = numbers.iter().sum::<f64>() / n;
    let variance: f64 = numbers.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    let stddev = variance.sqrt();

    Binding::lit(FlakeValue::Double(stddev), xsd_double())
}

/// GROUP_CONCAT - concatenate string values
fn agg_group_concat(values: &[Binding], separator: &str) -> Binding {
    let strings: Vec<String> = values
        .iter()
        .filter_map(|b| match b {
            Binding::Lit { val, .. } => match val {
                FlakeValue::String(s) => Some(s.clone()),
                FlakeValue::Json(s) => Some(s.clone()), // JSON as string
                FlakeValue::Long(n) => Some(n.to_string()),
                FlakeValue::Double(n) => Some(n.to_string()),
                FlakeValue::Boolean(b) => Some(b.to_string()),
                _ => None,
            },
            _ => None,
        })
        .collect();

    if strings.is_empty() {
        return Binding::Unbound;
    }

    let result = strings.join(separator);
    Binding::lit(FlakeValue::String(result), xsd_string())
}

/// SAMPLE - return an arbitrary value (first non-Unbound)
fn agg_sample(values: &[Binding]) -> Binding {
    values
        .iter()
        .find(|b| !matches!(b, Binding::Unbound | Binding::Poisoned))
        .cloned()
        .unwrap_or(Binding::Unbound)
}

/// Extract numeric values as f64 from bindings
fn extract_numbers(values: &[Binding]) -> Vec<f64> {
    values
        .iter()
        .filter_map(|b| match b {
            Binding::Lit { val, .. } => match val {
                FlakeValue::Long(n) => Some(*n as f64),
                FlakeValue::Boolean(b) => Some(i64::from(*b) as f64),
                FlakeValue::Double(n) => {
                    if n.is_nan() {
                        None
                    } else {
                        Some(*n)
                    }
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agg_count() {
        let values = vec![
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::Unbound,
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::Poisoned,
            Binding::lit(FlakeValue::Long(3), xsd_integer()),
        ];

        let result = agg_count(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(3));
    }

    #[test]
    fn test_agg_count_all() {
        // COUNT(*) counts ALL rows, including Unbound and Poisoned
        let values = vec![
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::Unbound,
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::Poisoned,
            Binding::lit(FlakeValue::Long(3), xsd_integer()),
        ];

        let result = agg_count_all(&values);
        let (val, _) = result.as_lit().unwrap();
        // All 5 rows are counted, not just the 3 bound values
        assert_eq!(*val, FlakeValue::Long(5));
    }

    #[test]
    fn test_agg_count_all_empty() {
        let values: Vec<Binding> = vec![];
        let result = agg_count_all(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(0));
    }

    #[test]
    fn test_agg_count_distinct() {
        let values = vec![
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::Unbound,
        ];

        let result = agg_count_distinct(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(2));
    }

    #[test]
    fn test_agg_sum() {
        let values = vec![
            Binding::lit(FlakeValue::Long(10), xsd_integer()),
            Binding::lit(FlakeValue::Long(20), xsd_integer()),
            Binding::lit(FlakeValue::Long(30), xsd_integer()),
        ];

        let result = agg_sum(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(60));
    }

    #[test]
    fn test_agg_sum_empty() {
        let values: Vec<Binding> = vec![];
        let result = agg_sum(&values);
        assert!(matches!(result, Binding::Unbound));
    }

    #[test]
    fn test_agg_sum_mixed_types() {
        let values = vec![
            Binding::lit(FlakeValue::Long(10), xsd_integer()),
            Binding::lit(FlakeValue::Double(20.5), xsd_double()),
        ];

        let result = agg_sum(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Double(30.5));
    }

    #[test]
    fn test_agg_sum_booleans_as_zero_one() {
        let values = vec![
            Binding::lit(
                FlakeValue::Boolean(true),
                Sid::new(
                    fluree_vocab::namespaces::XSD,
                    fluree_vocab::xsd_names::BOOLEAN,
                ),
            ),
            Binding::lit(
                FlakeValue::Boolean(false),
                Sid::new(
                    fluree_vocab::namespaces::XSD,
                    fluree_vocab::xsd_names::BOOLEAN,
                ),
            ),
            Binding::lit(
                FlakeValue::Boolean(true),
                Sid::new(
                    fluree_vocab::namespaces::XSD,
                    fluree_vocab::xsd_names::BOOLEAN,
                ),
            ),
        ];

        let result = agg_sum(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(2));
    }

    #[test]
    fn test_agg_avg() {
        let values = vec![
            Binding::lit(FlakeValue::Long(10), xsd_integer()),
            Binding::lit(FlakeValue::Long(20), xsd_integer()),
            Binding::lit(FlakeValue::Long(30), xsd_integer()),
        ];

        let result = agg_avg(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Double(20.0));
    }

    #[test]
    fn test_agg_min() {
        let values = vec![
            Binding::lit(FlakeValue::Long(30), xsd_integer()),
            Binding::lit(FlakeValue::Long(10), xsd_integer()),
            Binding::lit(FlakeValue::Long(20), xsd_integer()),
        ];

        let result = agg_min(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(10));
    }

    #[test]
    fn test_agg_max() {
        let values = vec![
            Binding::lit(FlakeValue::Long(30), xsd_integer()),
            Binding::lit(FlakeValue::Long(10), xsd_integer()),
            Binding::lit(FlakeValue::Long(20), xsd_integer()),
        ];

        let result = agg_max(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(30));
    }

    #[test]
    fn test_agg_median_odd() {
        let values = vec![
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::lit(FlakeValue::Long(5), xsd_integer()),
            Binding::lit(FlakeValue::Long(3), xsd_integer()),
        ];

        let result = agg_median(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Double(3.0));
    }

    #[test]
    fn test_agg_median_even() {
        let values = vec![
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::lit(FlakeValue::Long(3), xsd_integer()),
            Binding::lit(FlakeValue::Long(4), xsd_integer()),
        ];

        let result = agg_median(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Double(2.5));
    }

    #[test]
    fn test_agg_variance() {
        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean: 5
        // Variance: ((2-5)^2 + (4-5)^2 + (4-5)^2 + (4-5)^2 + (5-5)^2 + (5-5)^2 + (7-5)^2 + (9-5)^2) / 8
        //         = (9 + 1 + 1 + 1 + 0 + 0 + 4 + 16) / 8 = 32 / 8 = 4
        let values = vec![
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::lit(FlakeValue::Long(4), xsd_integer()),
            Binding::lit(FlakeValue::Long(4), xsd_integer()),
            Binding::lit(FlakeValue::Long(4), xsd_integer()),
            Binding::lit(FlakeValue::Long(5), xsd_integer()),
            Binding::lit(FlakeValue::Long(5), xsd_integer()),
            Binding::lit(FlakeValue::Long(7), xsd_integer()),
            Binding::lit(FlakeValue::Long(9), xsd_integer()),
        ];

        let result = agg_variance(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Double(4.0));
    }

    #[test]
    fn test_agg_stddev() {
        // Same values as variance test, stddev = sqrt(4) = 2
        let values = vec![
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::lit(FlakeValue::Long(4), xsd_integer()),
            Binding::lit(FlakeValue::Long(4), xsd_integer()),
            Binding::lit(FlakeValue::Long(4), xsd_integer()),
            Binding::lit(FlakeValue::Long(5), xsd_integer()),
            Binding::lit(FlakeValue::Long(5), xsd_integer()),
            Binding::lit(FlakeValue::Long(7), xsd_integer()),
            Binding::lit(FlakeValue::Long(9), xsd_integer()),
        ];

        let result = agg_stddev(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Double(2.0));
    }

    #[test]
    fn test_agg_group_concat() {
        let values = vec![
            Binding::lit(FlakeValue::String("a".into()), xsd_string()),
            Binding::lit(FlakeValue::String("b".into()), xsd_string()),
            Binding::lit(FlakeValue::String("c".into()), xsd_string()),
        ];

        let result = agg_group_concat(&values, ", ");
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::String("a, b, c".into()));
    }

    #[test]
    fn test_agg_sample() {
        let values = vec![
            Binding::Unbound,
            Binding::lit(FlakeValue::Long(42), xsd_integer()),
            Binding::lit(FlakeValue::Long(99), xsd_integer()),
        ];

        let result = agg_sample(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(42)); // First non-unbound
    }

    #[test]
    fn test_agg_sample_all_unbound() {
        let values = vec![Binding::Unbound, Binding::Unbound];
        let result = agg_sample(&values);
        assert!(matches!(result, Binding::Unbound));
    }

    #[test]
    fn test_apply_aggregate_non_grouped() {
        // Non-grouped values pass through unchanged
        let binding = Binding::lit(FlakeValue::Long(42), xsd_integer());
        let result = apply_aggregate(&AggregateFn::Sum, &binding, false);
        assert_eq!(result, binding);
    }

    #[test]
    fn test_apply_aggregate_grouped() {
        let grouped = Binding::Grouped(vec![
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::lit(FlakeValue::Long(3), xsd_integer()),
        ]);

        let result = apply_aggregate(&AggregateFn::Sum, &grouped, false);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(6));
    }

    #[test]
    fn test_apply_aggregate_distinct() {
        // SUM(DISTINCT) should deduplicate before summing
        let grouped = Binding::Grouped(vec![
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::lit(FlakeValue::Long(1), xsd_integer()), // duplicate
            Binding::lit(FlakeValue::Long(3), xsd_integer()),
            Binding::lit(FlakeValue::Long(2), xsd_integer()), // duplicate
        ]);

        let result = apply_aggregate(&AggregateFn::Sum, &grouped, true);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(6)); // 1+2+3 = 6, not 1+2+1+3+2 = 9
    }
}
