//! Grouping: how a query partitions its solution stream and what aggregate
//! functions it computes per group.

use fluree_db_core::NonEmpty;

use super::expression::Expression;
use crate::var_registry::VarId;

/// Aggregate function kinds.
///
/// `DISTINCT` handling is split: `COUNT(DISTINCT)` has a dedicated variant
/// (`CountDistinct`) because its streaming state uses a `HashSet` rather
/// than a simple counter. All other DISTINCT aggregates (SUM, AVG, …) use
/// their normal variant with `AggregateSpec::distinct = true`, and dedup
/// is applied at execution time.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFn {
    /// COUNT — count non-Unbound values of a variable
    Count,
    /// COUNT(*) — count all rows in a group regardless of variable values
    CountAll,
    /// COUNT(DISTINCT) — count distinct non-Unbound values (dedicated variant
    /// for streaming HashSet state; `AggregateSpec::distinct` is false here)
    CountDistinct,
    /// SUM — numeric sum
    Sum,
    /// AVG — numeric average
    Avg,
    /// MIN — minimum value by comparison
    Min,
    /// MAX — maximum value by comparison
    Max,
    /// MEDIAN — median value
    Median,
    /// VARIANCE — population variance
    Variance,
    /// STDDEV — population standard deviation
    Stddev,
    /// GROUP_CONCAT — concatenate strings with separator
    GroupConcat { separator: String },
    /// SAMPLE — return an arbitrary value
    Sample,
}

/// Specification for a single aggregate operation.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    /// The aggregate function to apply.
    pub function: AggregateFn,
    /// Input variable (contains `Grouped` values after GROUP BY). `None`
    /// for `COUNT(*)`, which counts all rows regardless of values.
    pub input_var: Option<VarId>,
    /// Output variable for the aggregate result.
    pub output_var: VarId,
    /// Whether `DISTINCT` was specified (e.g., `SUM(DISTINCT ?x)`).
    pub distinct: bool,
}

/// The aggregation stage of a grouping phase: aggregate functions computed
/// per group, plus any derived bindings that depend on the aggregate outputs.
///
/// `binds` may reference aggregate output variables; they fire after every
/// aggregate has been computed and before HAVING is evaluated. Empty `binds`
/// means no derived bindings; `aggregates` is `NonEmpty` because an
/// aggregation stage with nothing to compute would be meaningless.
#[derive(Debug, Clone)]
pub struct Aggregation {
    /// Aggregate specs computed per group.
    pub aggregates: NonEmpty<AggregateSpec>,
    /// Derived bindings computed from aggregate outputs.
    pub binds: Vec<(VarId, Expression)>,
}

/// The grouping phase of a query: how solutions partition into groups, what
/// aggregates compute over each group, and whether to filter the resulting
/// groups.
///
/// `Query.grouping` is `Option<Grouping>`; `None` means the query has no
/// grouping phase. The two variants distinguish whether the partition
/// criterion was stated by the user.
///
/// # Invariants
///
/// - `Implicit` always carries an `Aggregation` (a single-group grouping
///   with nothing to compute would be a no-op pass-through).
/// - `Explicit::group_by` is `NonEmpty<VarId>` (an empty key list would
///   semantically be `Implicit`).
/// - `Explicit::aggregation` is `Option<Aggregation>` — `None` represents
///   a deduplicating GROUP BY (`SELECT ?g WHERE { ... } GROUP BY ?g`
///   produces distinct values of `?g` with no per-group computations).
/// - `Aggregation::binds` only exist when an aggregation stage is present,
///   so they cannot accidentally accompany a dedup-only Explicit grouping.
/// - `having` contains no aggregate-function calls. Aggregates that
///   appeared inside the surface HAVING expression have been lifted into
///   `aggregation.aggregates` with synthetic output variables, and the
///   `having` expression has been rewritten to reference those output
///   variables. The post-lift expression evaluates as a regular boolean
///   against rows produced by upstream aggregate operators.
#[derive(Debug, Clone)]
pub enum Grouping {
    /// All solutions form one implicit group; aggregates produce a single
    /// result row. Surface form: aggregates without a `GROUP BY` clause
    /// (e.g. `SELECT (count(*) AS ?n) WHERE { ... }`).
    Implicit {
        aggregation: Aggregation,
        having: Option<Expression>,
    },
    /// Solutions partitioned by the values of `group_by`. Carries an
    /// optional `aggregation` stage; with no aggregation, partitioning
    /// alone deduplicates by group keys.
    Explicit {
        group_by: NonEmpty<VarId>,
        aggregation: Option<Aggregation>,
        having: Option<Expression>,
    },
}

impl Grouping {
    /// Assemble a grouping phase from the loose pieces produced by lowering.
    ///
    /// Returns `None` when there is no grouping phase to build (no `GROUP BY`,
    /// no aggregates, no post-aggregation binds). Otherwise selects the variant
    /// that satisfies the type-level invariants:
    ///   - `Explicit` when `group_by` is non-empty (regardless of whether an
    ///     aggregation stage is present — `GROUP BY` alone deduplicates by key).
    ///   - `Implicit` when there's no `GROUP BY` but at least one aggregate.
    ///
    /// Any leftover `having` or `binds` when no grouping exists is dropped on
    /// the floor — the parser/validator owns rejecting that surface form.
    pub fn assemble(
        group_by: Vec<VarId>,
        aggregates: Vec<AggregateSpec>,
        binds: Vec<(VarId, Expression)>,
        having: Option<Expression>,
    ) -> Option<Self> {
        let aggregation = NonEmpty::try_from_vec(aggregates)
            .map(|aggregates| Aggregation { aggregates, binds });
        if let Some(group_by) = NonEmpty::try_from_vec(group_by) {
            Some(Self::Explicit {
                group_by,
                aggregation,
                having,
            })
        } else {
            aggregation.map(|aggregation| Self::Implicit { aggregation, having })
        }
    }

    /// Borrow the `having` filter, if any, from either variant.
    pub fn having(&self) -> Option<&Expression> {
        match self {
            Self::Implicit { having, .. } | Self::Explicit { having, .. } => having.as_ref(),
        }
    }

    /// Borrow the aggregation stage, if any. Always present for `Implicit`;
    /// optional for `Explicit` (absent in dedup-only `GROUP BY`).
    pub fn aggregation(&self) -> Option<&Aggregation> {
        match self {
            Self::Implicit { aggregation, .. } => Some(aggregation),
            Self::Explicit { aggregation, .. } => aggregation.as_ref(),
        }
    }

    /// Iterate over every aggregate spec computed by this grouping phase,
    /// regardless of variant.
    pub fn aggregates(&self) -> impl Iterator<Item = &AggregateSpec> {
        self.aggregation()
            .into_iter()
            .flat_map(|agg| agg.aggregates.iter())
    }

    /// Iterate over the post-aggregation bind expressions for this grouping
    /// phase (`(VarId, Expression)` pairs). Empty when there's no
    /// aggregation stage.
    pub fn binds(&self) -> impl Iterator<Item = &(VarId, Expression)> {
        self.aggregation()
            .into_iter()
            .flat_map(|agg| agg.binds.iter())
    }
}
