//! Grouping: how a query partitions its solution stream and what aggregate
//! functions it computes per group.

use fluree_db_core::NonEmpty;

use super::expression::Expression;
use crate::var_registry::VarId;

/// How an aggregate function interprets duplicate input values.
///
/// SPARQL aggregates can be written with or without the `DISTINCT`
/// modifier. The modifier doesn't change what the input *is* (the
/// executor always carries a multiset of `Binding`s) — it changes how
/// the aggregate *interprets* that input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputSemantics {
    /// Treat the input as a list — every occurrence counted (default).
    List,
    /// Treat the input as a set — duplicates collapsed (`DISTINCT`).
    Set,
}

/// Aggregate function kinds, with each variant carrying exactly the fields
/// that variant needs:
///
/// - The input variable is part of every variant except [`Self::CountAll`]
///   (which counts rows regardless of values). Variants that take an input
///   carry it inline, so "Sum without an input" or "CountAll with an input"
///   are structurally unrepresentable.
/// - [`InputSemantics`] rides only on variants where SPARQL's `DISTINCT`
///   modifier actually changes the result. `Min`, `Max`, and `Sample` omit
///   it because their values are unchanged by deduplication; `Count` /
///   `CountDistinct` are separate variants for the same reason plus a
///   streaming-state distinction (counter vs. `HashSet`).
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFn {
    /// `COUNT(?x)` — count non-Unbound values of a variable.
    Count(VarId),
    /// `COUNT(*)` — count all rows in a group regardless of values.
    CountAll,
    /// `COUNT(DISTINCT ?x)` — count distinct non-Unbound values. Separate
    /// variant because its streaming state uses a `HashSet` rather than a
    /// counter.
    CountDistinct(VarId),
    /// `SUM(?x)` or `SUM(DISTINCT ?x)`.
    Sum(VarId, InputSemantics),
    /// `AVG(?x)` or `AVG(DISTINCT ?x)`.
    Avg(VarId, InputSemantics),
    /// `MIN(?x)` — DISTINCT is a no-op for min, so no flag.
    Min(VarId),
    /// `MAX(?x)` — DISTINCT is a no-op for max, so no flag.
    Max(VarId),
    /// `MEDIAN(?x)` or `MEDIAN(DISTINCT ?x)`.
    Median(VarId, InputSemantics),
    /// `VARIANCE(?x)` or `VARIANCE(DISTINCT ?x)` — population variance.
    Variance(VarId, InputSemantics),
    /// `STDDEV(?x)` or `STDDEV(DISTINCT ?x)` — population standard deviation.
    Stddev(VarId, InputSemantics),
    /// `GROUP_CONCAT(?x; SEPARATOR=…)` or its DISTINCT form. Stays in
    /// struct form because of the extra `separator` field.
    GroupConcat {
        input: VarId,
        semantics: InputSemantics,
        separator: String,
    },
    /// `SAMPLE(?x)` — an arbitrary value; DISTINCT is a no-op.
    Sample(VarId),
}

impl AggregateFn {
    /// Variable this aggregate reads from each row, if any. Returns `None`
    /// only for [`Self::CountAll`].
    pub fn input_var(&self) -> Option<VarId> {
        match self {
            Self::CountAll => None,
            Self::Count(v)
            | Self::CountDistinct(v)
            | Self::Sum(v, _)
            | Self::Avg(v, _)
            | Self::Min(v)
            | Self::Max(v)
            | Self::Median(v, _)
            | Self::Variance(v, _)
            | Self::Stddev(v, _)
            | Self::Sample(v) => Some(*v),
            Self::GroupConcat { input, .. } => Some(*input),
        }
    }

    /// Whether `DISTINCT` was requested. `true` for [`Self::CountDistinct`]
    /// (its own dedicated variant) and for any variant whose
    /// [`InputSemantics`] is [`InputSemantics::Set`]; always `false` on
    /// `Min`/`Max`/`Sample`/`Count`/`CountAll`, which don't carry the
    /// modifier at all.
    pub fn is_distinct(&self) -> bool {
        matches!(
            self,
            Self::CountDistinct(_)
                | Self::Sum(_, InputSemantics::Set)
                | Self::Avg(_, InputSemantics::Set)
                | Self::Median(_, InputSemantics::Set)
                | Self::Variance(_, InputSemantics::Set)
                | Self::Stddev(_, InputSemantics::Set)
                | Self::GroupConcat {
                    semantics: InputSemantics::Set,
                    ..
                }
        )
    }
}

/// Specification for a single aggregate operation: the function applied to
/// each group plus the variable the result is bound to.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    /// The aggregate function to apply.
    pub function: AggregateFn,
    /// Output variable for the aggregate result.
    pub output_var: VarId,
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
        let aggregation =
            NonEmpty::try_from_vec(aggregates).map(|aggregates| Aggregation { aggregates, binds });
        if let Some(group_by) = NonEmpty::try_from_vec(group_by) {
            Some(Self::Explicit {
                group_by,
                aggregation,
                having,
            })
        } else {
            aggregation.map(|aggregation| Self::Implicit {
                aggregation,
                having,
            })
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

    /// Iterate over the `GROUP BY` key variables. Empty for `Implicit`
    /// grouping (single implicit group); the `Explicit` keys for
    /// `Explicit` grouping.
    pub fn group_by_vars(&self) -> impl Iterator<Item = VarId> + '_ {
        let explicit = match self {
            Self::Explicit { group_by, .. } => Some(group_by.iter().copied()),
            Self::Implicit { .. } => None,
        };
        explicit.into_iter().flatten()
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
