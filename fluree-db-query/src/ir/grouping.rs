//! Grouping: how a query partitions its solution stream and what aggregate
//! functions it computes per group.

use fluree_db_core::NonEmpty;

use super::expression::Expression;
use crate::aggregate::AggregateSpec;
use crate::var_registry::VarId;

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
/// - `Implicit` requires aggregates (a single-group grouping with no
///   aggregates would be a no-op pass-through).
/// - `Explicit::group_by` is `NonEmpty<VarId>` (an empty key list would
///   semantically be `Implicit`).
/// - `Explicit::aggregates` may be empty (a deduplicating GROUP BY:
///   `SELECT ?g WHERE { ... } GROUP BY ?g` produces distinct values of `?g`
///   with no per-group computations).
/// - `having` contains no aggregate-function calls. Aggregates that appeared
///   inside the surface HAVING expression have been lifted into `aggregates`
///   with synthetic output variables, and the `having` expression has been
///   rewritten to reference those output variables. The post-lift expression
///   evaluates as a regular boolean against rows produced by upstream
///   aggregate operators.
#[derive(Debug, Clone)]
pub enum Grouping {
    /// All solutions form one implicit group; aggregates produce a single
    /// result row. Surface form: aggregates without a `GROUP BY` clause
    /// (e.g. `SELECT (count(*) AS ?n) WHERE { ... }`).
    Implicit {
        aggregates: NonEmpty<AggregateSpec>,
        having: Option<Expression>,
    },
    /// Solutions partitioned by the values of `group_by`; aggregates produce
    /// one row per partition.
    Explicit {
        group_by: NonEmpty<VarId>,
        aggregates: Vec<AggregateSpec>,
        having: Option<Expression>,
    },
}

impl Grouping {
    /// Borrow the `having` filter, if any, from either variant.
    pub fn having(&self) -> Option<&Expression> {
        match self {
            Self::Implicit { having, .. } | Self::Explicit { having, .. } => having.as_ref(),
        }
    }

    /// Number of aggregates computed by this grouping phase.
    pub fn aggregate_count(&self) -> usize {
        match self {
            Self::Implicit { aggregates, .. } => aggregates.len(),
            Self::Explicit { aggregates, .. } => aggregates.len(),
        }
    }
}
