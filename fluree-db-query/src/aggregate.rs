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
use crate::ir::{AggregateFn, AggregateSpec};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use bigdecimal::{BigDecimal, ToPrimitive};
use fluree_db_core::{FlakeValue, Sid};
use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

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
    /// Graph view for materializing encoded bindings before value-folding
    /// aggregates (GROUP_CONCAT/MEDIAN/VARIANCE/STDDEV) — their matchers
    /// silently drop encoded values.
    graph_view: Option<fluree_db_binary_index::BinaryGraphView>,
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
            match spec.function.input_var() {
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
            graph_view: None,
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
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        if self.graph_view.is_none() {
            self.graph_view = ctx.graph_view();
        }
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
            ctx.check_cancelled()?;

            // Process child columns (regular aggregates and pass-through)
            for col_idx in 0..self.child_col_count {
                ctx.check_cancelled()?;
                let mut col_output = Vec::with_capacity(batch.len());

                for row_idx in 0..batch.len() {
                    let input_binding = batch.get_by_col(row_idx, col_idx);

                    let output_binding = match self.aggregate_map.get(col_idx).copied().flatten() {
                        Some(agg_idx) => {
                            // This column needs aggregation
                            let spec = &self.aggregates[agg_idx];
                            apply_aggregate(&spec.function, input_binding, self.graph_view.as_ref())
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
            ctx.check_cancelled()?;

            if !self.extra_specs.is_empty() {
                let mut group_sizes: Option<Vec<i64>> = None;

                for (agg_idx, input_col, _output_col_idx) in &self.extra_specs {
                    ctx.check_cancelled()?;
                    let spec = &self.aggregates[*agg_idx];
                    let col_output: Vec<Binding> = match input_col {
                        Some(col_idx) => (0..batch.len())
                            .map(|row_idx| {
                                let input_binding = batch.get_by_col(row_idx, *col_idx);
                                apply_aggregate(
                                    &spec.function,
                                    input_binding,
                                    self.graph_view.as_ref(),
                                )
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
            ctx.check_cancelled()?;

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

/// Apply an aggregate, first materializing encoded bindings for the
/// value-folding variants whose matchers in this module silently drop them
/// (GROUP_CONCAT returned Unbound for every indexed-ledger string).
/// Comparison/count variants and the numeric accumulator handle encoded
/// bindings natively, so they skip the decode.
fn apply_aggregate(
    func: &AggregateFn,
    binding: &Binding,
    gv: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> Binding {
    let needs_decoded_values = matches!(
        func,
        AggregateFn::GroupConcat { .. }
            | AggregateFn::Median { .. }
            | AggregateFn::Variance { .. }
            | AggregateFn::Stddev { .. }
    );
    if needs_decoded_values && gv.is_some() {
        if let Binding::Grouped(values) = binding {
            let decoded: Vec<Binding> = values
                .iter()
                .map(|b| crate::group_aggregate::materialize_encoded(b, gv))
                .collect();
            return func.apply(&Binding::Grouped(decoded));
        }
    }
    func.apply(binding)
}

impl AggregateFn {
    /// Apply this aggregate function to a binding.
    ///
    /// If the binding is `Grouped(values)`, compute the aggregate;
    /// non-grouped values pass through (e.g. group-key columns).
    /// Variants that need an upstream dedup pass (see
    /// [`Self::needs_input_dedup`]) get one before being handed to
    /// [`Self::compute`].
    pub fn apply(&self, binding: &Binding) -> Binding {
        let Binding::Grouped(values) = binding else {
            return binding.clone();
        };
        if self.needs_input_dedup() {
            let mut seen = HashSet::with_capacity(values.len());
            let deduped: Vec<Binding> =
                values.iter().filter(|b| seen.insert(*b)).cloned().collect();
            self.compute(&deduped)
        } else {
            self.compute(values)
        }
    }

    /// Whether [`Self::apply`] must deduplicate input values before
    /// reducing. True for variants whose [`InputSemantics`] is
    /// [`InputSemantics::Set`]; false for everything else, including
    /// [`Self::CountDistinct`] — its streaming state is already a
    /// `HashSet`, so an additional dedup pass would be redundant.
    fn needs_input_dedup(&self) -> bool {
        self.is_distinct() && !matches!(self, AggregateFn::CountDistinct(_))
    }

    /// Compute the aggregate result over an already-prepared list of
    /// bindings (deduplicated upstream by [`Self::apply`] if needed).
    fn compute(&self, values: &[Binding]) -> Binding {
        match self {
            Self::Count(_) => agg_count(values),
            Self::CountAll => agg_count_all(values),
            Self::CountDistinct(_) => agg_count_distinct(values),
            Self::Sum { .. } => agg_sum(values),
            Self::Avg { .. } => agg_avg(values),
            Self::Min(_) => agg_min(values),
            Self::Max(_) => agg_max(values),
            Self::Median { .. } => agg_median(values),
            Self::Variance { .. } => agg_variance(values),
            Self::Stddev { .. } => agg_stddev(values),
            Self::GroupConcat { separator, .. } => agg_group_concat(values, separator),
            Self::Sample(_) => agg_sample(values),
        }
    }
}

fn xsd_integer() -> Sid {
    Sid::xsd_integer()
}

fn xsd_double() -> Sid {
    Sid::xsd_double()
}

fn xsd_decimal() -> Sid {
    Sid::xsd_decimal()
}

fn xsd_string() -> Sid {
    Sid::xsd_string()
}

/// Numeric inputs for aggregate accumulators, preserving original XSD class.
pub(crate) enum NumericValue {
    Long(i64),
    BigInt(BigInt),
    Decimal(BigDecimal),
    Double(f64),
}

/// SUM/AVG accumulator with XSD numeric type promotion.
///
/// Promotion lattice (sticky upward):
///   Integer → Decimal → Double
///
/// While in the `Integer`/`Decimal` states we accumulate exactly as `BigDecimal`.
/// Once any `Double` input is seen we collapse to `f64` (per W3C XPath numeric
/// promotion: xsd:double absorbs all other numeric types).
#[derive(Debug)]
pub(crate) struct NumericAcc {
    kind: NumKind,
    big_sum: BigDecimal,
    dbl_sum: f64,
    count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumKind {
    Integer,
    Decimal,
    Double,
}

impl NumericAcc {
    pub(crate) fn new() -> Self {
        Self {
            kind: NumKind::Integer,
            big_sum: BigDecimal::zero(),
            dbl_sum: 0.0,
            count: 0,
        }
    }

    pub(crate) fn add(&mut self, v: NumericValue) {
        self.count += 1;
        match v {
            NumericValue::Long(n) => match self.kind {
                NumKind::Double => self.dbl_sum += n as f64,
                _ => self.big_sum += BigDecimal::from(n),
            },
            NumericValue::BigInt(b) => match self.kind {
                NumKind::Double => self.dbl_sum += bigint_to_f64(&b),
                _ => self.big_sum += BigDecimal::from(b),
            },
            NumericValue::Decimal(d) => match self.kind {
                NumKind::Double => self.dbl_sum += d.to_f64().unwrap_or(0.0),
                NumKind::Integer => {
                    self.kind = NumKind::Decimal;
                    self.big_sum += d;
                }
                NumKind::Decimal => self.big_sum += d,
            },
            NumericValue::Double(f) => {
                if self.kind != NumKind::Double {
                    // Collapse exact accumulator into f64 once a double appears.
                    self.dbl_sum = self.big_sum.to_f64().unwrap_or(0.0);
                    self.big_sum = BigDecimal::zero();
                    self.kind = NumKind::Double;
                }
                self.dbl_sum += f;
            }
        }
    }

    /// Finalize as SPARQL SUM. Empty group returns `Unbound`.
    ///
    /// Result datatype follows the W3C arithmetic promotion lattice:
    ///   all integer  → xsd:integer (Long if it fits, else BigInt)
    ///   any decimal  → xsd:decimal
    ///   any double   → xsd:double
    pub(crate) fn finalize_sum(self) -> Binding {
        if self.count == 0 {
            return Binding::Unbound;
        }
        match self.kind {
            NumKind::Integer => integer_binding_from_bigdecimal(self.big_sum),
            NumKind::Decimal => {
                Binding::lit(FlakeValue::Decimal(Box::new(self.big_sum)), xsd_decimal())
            }
            NumKind::Double => Binding::lit(FlakeValue::Double(self.dbl_sum), xsd_double()),
        }
    }

    /// Finalize as SPARQL AVG. Empty group returns `Unbound`.
    ///
    /// AVG promotes xsd:integer inputs to xsd:decimal output (matches XPath
    /// `op:numeric-divide` of integers, which yields xsd:decimal).
    ///
    /// The division precision is capped at `AVG_DECIMAL_PRECISION` significant
    /// digits to keep output bounded — `BigDecimal::div` would otherwise expand
    /// recurring decimals to its default 100-digit precision, producing values
    /// like `0.33333...` with 100 trailing digits.
    pub(crate) fn finalize_avg(self) -> Binding {
        if self.count == 0 {
            return Binding::Unbound;
        }
        match self.kind {
            NumKind::Integer | NumKind::Decimal => {
                let count_bd = BigDecimal::from(self.count as i64);
                let avg = (self.big_sum / count_bd)
                    .with_prec(AVG_DECIMAL_PRECISION)
                    .normalized();
                Binding::lit(FlakeValue::Decimal(Box::new(avg)), xsd_decimal())
            }
            NumKind::Double => Binding::lit(
                FlakeValue::Double(self.dbl_sum / self.count as f64),
                xsd_double(),
            ),
        }
    }
}

/// Significant-digit cap for the result of AVG over xsd:decimal/xsd:integer
/// inputs. Matches IEEE-754 decimal128 precision (34 digits) — well past
/// xsd:double's ~17 digits of precision but small enough to keep output
/// compact for typical financial / scientific aggregates.
const AVG_DECIMAL_PRECISION: u64 = 34;

/// Best-effort `BigInt → f64`. Saturates at infinity for out-of-range values.
fn bigint_to_f64(b: &BigInt) -> f64 {
    b.to_f64().unwrap_or_else(|| {
        if b.is_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }
    })
}

/// Produce an xsd:integer binding from an integer-valued `BigDecimal`.
/// Returns `Long` when the value fits in `i64`, else `BigInt`.
fn integer_binding_from_bigdecimal(sum: BigDecimal) -> Binding {
    // For NumKind::Integer the accumulator only ever received integer inputs,
    // so the BigDecimal has zero fractional scale; conversion to BigInt is exact.
    let (digits, _scale) = sum.into_bigint_and_exponent();
    if let Some(n) = digits.to_i64() {
        Binding::lit(FlakeValue::Long(n), xsd_integer())
    } else {
        Binding::lit(FlakeValue::BigInt(Box::new(digits)), xsd_integer())
    }
}

/// Extract a numeric value from a binding, recognizing all XSD numeric kinds
/// that Fluree can store: xsd:integer (Long/BigInt), xsd:decimal (BigDecimal),
/// xsd:double, and xsd:boolean (treated as 0/1).
///
/// `EncodedLit` is decoded via `decode_encoded_lit_numeric` (only the inline
/// kinds NUM_INT and NUM_F64; NUM_BIG arena handles require a graph view and
/// are handled by the streaming aggregate path that has access to one).
pub(crate) fn binding_to_numeric(binding: &Binding) -> Option<NumericValue> {
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    match binding {
        Binding::Lit { val, dtc, .. } => {
            flake_value_to_numeric(val).or_else(|| string_lit_to_numeric(val, dtc))
        }
        Binding::EncodedLit { o_kind, o_key, .. } => {
            if *o_kind == ObjKind::NUM_INT.as_u8() {
                Some(NumericValue::Long(ObjKey::from_u64(*o_key).decode_i64()))
            } else if *o_kind == ObjKind::NUM_F64.as_u8() {
                let d = ObjKey::from_u64(*o_key).decode_f64();
                if d.is_nan() {
                    None
                } else {
                    Some(NumericValue::Double(d))
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn flake_value_to_numeric(val: &FlakeValue) -> Option<NumericValue> {
    match val {
        FlakeValue::Long(n) => Some(NumericValue::Long(*n)),
        FlakeValue::Boolean(b) => Some(NumericValue::Long(i64::from(*b))),
        FlakeValue::Double(d) => {
            if d.is_nan() {
                None
            } else {
                Some(NumericValue::Double(*d))
            }
        }
        FlakeValue::BigInt(b) => Some(NumericValue::BigInt((**b).clone())),
        FlakeValue::Decimal(d) => Some(NumericValue::Decimal((**d).clone())),
        _ => None,
    }
}

/// Coerce a numeric value that is stored as a `FlakeValue::String` to a number.
///
/// The query-time `xsd:float(?x)` cast is kept string-backed (to preserve f32
/// precision — see `eval/cast.rs`), so SUM/AVG/etc. would otherwise silently
/// drop it (`flake_value_to_numeric` has no `String` arm) and return unbound.
/// Parse the string when the literal's datatype is a numeric XSD type. Yields
/// `Double` (f64 is exact for f32-sourced values); NaN is dropped like other
/// non-finite aggregate inputs.
fn string_lit_to_numeric(
    val: &FlakeValue,
    dtc: &fluree_db_core::DatatypeConstraint,
) -> Option<NumericValue> {
    let FlakeValue::String(s) = val else {
        return None;
    };
    if !is_numeric_xsd_datatype(dtc.datatype()) {
        return None;
    }
    let d = s.parse::<f64>().ok()?;
    (!d.is_nan()).then_some(NumericValue::Double(d))
}

/// Whether `sid` is a numeric XSD datatype (the family that should accumulate
/// in SUM/AVG even when carried as a string-backed literal).
fn is_numeric_xsd_datatype(sid: &Sid) -> bool {
    use fluree_vocab::{namespaces, xsd_names};
    sid.namespace_code == namespaces::XSD
        && matches!(
            sid.name.as_ref(),
            xsd_names::FLOAT
                | xsd_names::DOUBLE
                | xsd_names::DECIMAL
                | xsd_names::INTEGER
                | xsd_names::LONG
                | xsd_names::INT
                | xsd_names::SHORT
                | xsd_names::BYTE
                | xsd_names::UNSIGNED_LONG
                | xsd_names::UNSIGNED_INT
                | xsd_names::UNSIGNED_SHORT
                | xsd_names::UNSIGNED_BYTE
                | xsd_names::NON_NEGATIVE_INTEGER
                | xsd_names::POSITIVE_INTEGER
                | xsd_names::NON_POSITIVE_INTEGER
                | xsd_names::NEGATIVE_INTEGER
        )
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

/// SUM - numeric sum
///
/// Accumulates exactly via `BigDecimal` for xsd:integer and xsd:decimal inputs
/// (preserving monetary precision), and falls back to `f64` once any xsd:double
/// is encountered. Result datatype follows XPath numeric promotion.
fn agg_sum(values: &[Binding]) -> Binding {
    let mut acc = NumericAcc::new();
    for v in values.iter().filter_map(binding_to_numeric) {
        acc.add(v);
    }
    acc.finalize_sum()
}

/// AVG - numeric average
///
/// Accumulates the sum exactly in `BigDecimal` for integer/decimal inputs and
/// divides by the count at finalize time, yielding an xsd:decimal result.
/// xsd:double inputs collapse the accumulator to f64 and yield xsd:double.
fn agg_avg(values: &[Binding]) -> Binding {
    let mut acc = NumericAcc::new();
    for v in values.iter().filter_map(binding_to_numeric) {
        acc.add(v);
    }
    acc.finalize_avg()
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

/// Extract numeric values as f64 from bindings.
///
/// Used by MEDIAN / VARIANCE / STDDEV which intrinsically operate in f64.
/// Mirrors `binding_to_numeric` but collapses every numeric class to f64
/// (xsd:integer / xsd:decimal / xsd:double / xsd:boolean), losing precision
/// for large BigInt and exact-precision Decimal values — acceptable because
/// these statistical aggregates have no exact-arithmetic semantics in SPARQL.
fn extract_numbers(values: &[Binding]) -> Vec<f64> {
    values
        .iter()
        .filter_map(binding_to_numeric)
        .filter_map(|n| match n {
            NumericValue::Long(v) => Some(v as f64),
            NumericValue::BigInt(b) => Some(bigint_to_f64(&b)),
            NumericValue::Decimal(d) => d.to_f64(),
            NumericValue::Double(d) => {
                if d.is_nan() {
                    None
                } else {
                    Some(d)
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::InputSemantics;

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
        let (val, dtc) = result.as_lit().unwrap();
        // Per W3C: AVG of integers yields xsd:decimal (op:numeric-divide on
        // integers returns xsd:decimal).
        assert_eq!(*val, FlakeValue::Decimal(Box::new(BigDecimal::from(20))));
        assert!(format!("{dtc:?}").contains("decimal"));
    }

    #[test]
    fn test_agg_sum_over_decimal() {
        // Bug repro: SUM over xsd:decimal must return an exact xsd:decimal,
        // not the additive identity 0 nor a lossy xsd:double.
        let values = vec![
            Binding::lit(
                FlakeValue::Decimal(Box::new("12.50".parse().unwrap())),
                Sid::xsd_decimal(),
            ),
            Binding::lit(
                FlakeValue::Decimal(Box::new("7.99".parse().unwrap())),
                Sid::xsd_decimal(),
            ),
        ];

        let result = agg_sum(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(
            *val,
            FlakeValue::Decimal(Box::new("20.49".parse().unwrap()))
        );
    }

    #[test]
    fn test_agg_avg_over_decimal() {
        let values = vec![
            Binding::lit(
                FlakeValue::Decimal(Box::new("12.50".parse().unwrap())),
                Sid::xsd_decimal(),
            ),
            Binding::lit(
                FlakeValue::Decimal(Box::new("7.99".parse().unwrap())),
                Sid::xsd_decimal(),
            ),
        ];

        let result = agg_avg(&values);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(
            *val,
            FlakeValue::Decimal(Box::new("10.245".parse().unwrap()))
        );
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

    fn sum_of(input: VarId, distinct: bool) -> AggregateFn {
        let semantics = if distinct {
            InputSemantics::Set
        } else {
            InputSemantics::List
        };
        AggregateFn::Sum(input, semantics)
    }

    #[test]
    fn test_apply_aggregate_non_grouped() {
        // Non-grouped values pass through unchanged
        let binding = Binding::lit(FlakeValue::Long(42), xsd_integer());
        let result = sum_of(VarId(0), false).apply(&binding);
        assert_eq!(result, binding);
    }

    #[test]
    fn test_apply_aggregate_grouped() {
        let grouped = Binding::Grouped(vec![
            Binding::lit(FlakeValue::Long(1), xsd_integer()),
            Binding::lit(FlakeValue::Long(2), xsd_integer()),
            Binding::lit(FlakeValue::Long(3), xsd_integer()),
        ]);

        let result = sum_of(VarId(0), false).apply(&grouped);
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

        let result = sum_of(VarId(0), true).apply(&grouped);
        let (val, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(6)); // 1+2+3 = 6, not 1+2+1+3+2 = 9
    }
}
