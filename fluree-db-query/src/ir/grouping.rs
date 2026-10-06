//! Grouping: how a query partitions its solution stream and what aggregate
//! functions it computes per group.

use std::collections::HashSet;

use fluree_db_core::NonEmpty;

use super::expression::Expression;
use crate::var_registry::{VarId, VarRegistry};

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
    /// `COUNT(DISTINCT *)` — count the *distinct solutions* in a group
    /// (SPARQL 1.1 §18.5.1.1). Unlike every other variant this reads a whole
    /// row rather than one column, so it carries no input variable.
    ///
    /// The payload is the query's **user-visible** variables, in registration
    /// order. `*` denotes the solution mapping, and SPARQL projects
    /// lowering-internal variables out of it — property-path join variables
    /// (`?__ppN`) and non-distinguished blank-node variables (§4.1.4) among
    /// them. Composing distinctness over the raw upstream row would let those
    /// split solutions that the spec considers identical, so the group
    /// operators intersect this list with their input schema and read only
    /// those columns. Variables from other scopes (or SELECT aliases, which
    /// never reach the pre-grouping row) drop out in that intersection.
    CountDistinctAll(Vec<VarId>),
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
    /// `collect(?x)` / `collect(DISTINCT ?x)` (Cypher) — gather every
    /// non-Unbound value of a variable into a list. Produces a
    /// `Binding::List` (a real list value, not the internal per-group
    /// `Binding::Grouped` carrier). No SPARQL surface; lowered only from
    /// Cypher.
    Collect(VarId, InputSemantics),
}

impl AggregateFn {
    /// Whether the streaming `GroupAggregateOperator` can compute this
    /// aggregate, folding one row at a time.
    ///
    /// `DISTINCT SUM`/`AVG` aren't streamable because the dedup pass must collect
    /// every value before reducing. `Median`/`Variance`/`Stddev`/`GroupConcat`
    /// are non-streamable regardless of DISTINCT — they likewise need every
    /// value in hand. `COUNT(DISTINCT)` is streamable because its state machine
    /// is a `HashSet` (the dedup IS the streaming state), and `Min`/`Max`/`Sample`
    /// don't carry a DISTINCT flag at all.
    pub fn is_streamable(&self) -> bool {
        match self {
            Self::Count(_)
            | Self::CountAll
            | Self::CountDistinct(_)
            | Self::CountDistinctAll(_)
            | Self::Min(_)
            | Self::Max(_)
            | Self::Sample(_) => true,
            Self::Sum(_, semantics) | Self::Avg(_, semantics) => {
                matches!(semantics, InputSemantics::List)
            }
            Self::Median { .. }
            | Self::Variance { .. }
            | Self::Stddev { .. }
            | Self::GroupConcat { .. }
            | Self::Collect(..) => false,
        }
    }

    /// Variable this aggregate reads from each row, if any. Returns `None`
    /// only for [`Self::CountAll`].
    pub fn input_var(&self) -> Option<VarId> {
        match self {
            Self::CountAll | Self::CountDistinctAll(_) => None,
            Self::Count(v)
            | Self::CountDistinct(v)
            | Self::Sum(v, _)
            | Self::Avg(v, _)
            | Self::Min(v)
            | Self::Max(v)
            | Self::Median(v, _)
            | Self::Variance(v, _)
            | Self::Stddev(v, _)
            | Self::Sample(v)
            | Self::Collect(v, _) => Some(*v),
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
                | Self::CountDistinctAll(_)
                | Self::Sum(_, InputSemantics::Set)
                | Self::Avg(_, InputSemantics::Set)
                | Self::Median(_, InputSemantics::Set)
                | Self::Variance(_, InputSemantics::Set)
                | Self::Stddev(_, InputSemantics::Set)
                | Self::GroupConcat {
                    semantics: InputSemantics::Set,
                    ..
                }
                | Self::Collect(_, InputSemantics::Set)
        )
    }

    /// Whether the aggregate's result is unchanged when duplicate input ROWS
    /// are collapsed to one — i.e. it observes only the *set* of values, not
    /// their multiplicity. True for every `DISTINCT`-marked variant plus
    /// `Min`/`Max`/`Sample` (order statistics / arbitrary pick).
    ///
    /// This is the soundness condition for WHERE-level early dedup: when
    /// every aggregate in the grouping phase is duplicate-insensitive, the
    /// planner may project away dead variables and collapse duplicate rows
    /// between joins without changing results (see
    /// `where_plan::build_sequential_join_block`).
    /// Matched exhaustively on purpose: this is a soundness-critical
    /// partition, so a new variant must be classified deliberately rather
    /// than defaulting (silently to `false` — safe, but it would lose the
    /// optimization with no signal).
    pub fn duplicate_insensitive(&self) -> bool {
        match self {
            Self::Min(_) | Self::Max(_) | Self::Sample(_) => true,
            Self::CountAll | Self::Count(_) => false,
            // Counting distinct solutions is insensitive to duplicate rows in
            // principle, but the optimization this gates also *projects away
            // dead variables* before deduping — and dropping a column changes
            // which rows are distinct. Classified false so the whole-row read
            // always sees the full solution.
            Self::CountDistinctAll(_) => false,
            Self::CountDistinct(_)
            | Self::Sum(..)
            | Self::Avg(..)
            | Self::Median(..)
            | Self::Variance(..)
            | Self::Stddev(..)
            | Self::Collect(..)
            | Self::GroupConcat { .. } => self.is_distinct(),
        }
    }
}

impl AggregateFn {
    /// Rename the input variable from `old` to `new` (no-op for `CountAll`).
    pub fn substitute_var(&mut self, old: VarId, new: VarId) {
        let rename = |v: &mut VarId| {
            if *v == old {
                *v = new;
            }
        };
        match self {
            Self::CountAll | Self::CountDistinctAll(_) => {}
            Self::Count(v)
            | Self::CountDistinct(v)
            | Self::Sum(v, _)
            | Self::Avg(v, _)
            | Self::Min(v)
            | Self::Max(v)
            | Self::Median(v, _)
            | Self::Variance(v, _)
            | Self::Stddev(v, _)
            | Self::Sample(v)
            | Self::Collect(v, _) => rename(v),
            Self::GroupConcat { input, .. } => rename(input),
        }
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

/// The aggregation stage of a grouping phase: the aggregate functions
/// computed per group. `aggregates` is `NonEmpty` because an aggregation
/// stage with nothing to compute would be meaningless.
#[derive(Debug, Clone)]
pub struct Aggregation {
    /// Aggregate specs computed per group.
    pub aggregates: NonEmpty<AggregateSpec>,
}

/// The grouping phase of a query: how solutions partition into groups, what
/// aggregates compute over each group, which groups survive, and what each
/// surviving group row binds afterwards.
///
/// `Query.grouping` is `Option<Grouping>`; `None` means the query has no
/// grouping phase. The two variants distinguish whether the partition
/// criterion was stated by the user.
///
/// Stage order, as the executor runs it (SPARQL 1.1 §18.2.4): group, compute
/// the aggregates, filter by `having`, then evaluate `binds` in order.
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
/// - `binds` are the per-group `Extend`s (§18.2.4.4): they run after
///   `having` and read only group keys, aggregate outputs and the outputs of
///   earlier binds. They belong to the grouping phase whether or not an
///   aggregation stage exists — a dedup-only `GROUP BY ?a` projecting
///   `(IF(?a …) AS ?seg)` needs one.
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
        /// Per-group `Extend`s, run after `having` (see the type docs).
        binds: Vec<(VarId, Expression)>,
    },
    /// Solutions partitioned by the values of `group_by`. Carries an
    /// optional `aggregation` stage; with no aggregation, partitioning
    /// alone deduplicates by group keys.
    Explicit {
        group_by: NonEmpty<VarId>,
        aggregation: Option<Aggregation>,
        having: Option<Expression>,
        /// Per-group `Extend`s, run after `having` (see the type docs).
        binds: Vec<(VarId, Expression)>,
    },
}

/// A grouping phase that cannot be built: the pieces ask for a post-grouping
/// stage (HAVING, per-group binds) but there is nothing to group by and nothing
/// to aggregate.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GroupingError {
    /// A HAVING with neither a group key nor an aggregate. A lowerer turns it
    /// into a Filter first (SPARQL 1.1 §18.2.4.2); reaching `assemble` means
    /// a caller did not.
    #[error("HAVING without GROUP BY or an aggregate must be lowered as a filter")]
    HavingWithoutGrouping,
    /// Per-group binds with neither a group key nor an aggregate: they belong
    /// in the WHERE as ordinary binds.
    #[error("per-group SELECT expressions without GROUP BY or an aggregate")]
    BindsWithoutGrouping,
}

impl Grouping {
    /// Assemble a grouping phase from the loose pieces produced by lowering.
    ///
    /// Returns `Ok(None)` when there is no grouping phase to build (no `GROUP
    /// BY`, no aggregates, no HAVING, no binds). Otherwise selects the variant
    /// that satisfies the type-level invariants:
    ///   - `Explicit` when `group_by` is non-empty (regardless of whether an
    ///     aggregation stage is present — `GROUP BY` alone deduplicates by key).
    ///   - `Implicit` when there's no `GROUP BY` but at least one aggregate.
    ///
    /// A `having` or `binds` with neither a key nor an aggregate is an error,
    /// not dropped: the SPARQL and JSON-LD lowerers turn such a HAVING into a
    /// filter before calling this, so the error is for any other caller.
    pub fn assemble(
        group_by: Vec<VarId>,
        aggregates: Vec<AggregateSpec>,
        binds: Vec<(VarId, Expression)>,
        having: Option<Expression>,
    ) -> Result<Option<Self>, GroupingError> {
        let aggregation =
            NonEmpty::try_from_vec(aggregates).map(|aggregates| Aggregation { aggregates });
        if let Some(group_by) = NonEmpty::try_from_vec(group_by) {
            return Ok(Some(Self::Explicit {
                group_by,
                aggregation,
                having,
                binds,
            }));
        }
        match aggregation {
            Some(aggregation) => Ok(Some(Self::Implicit {
                aggregation,
                having,
                binds,
            })),
            None if having.is_some() => Err(GroupingError::HavingWithoutGrouping),
            None if !binds.is_empty() => Err(GroupingError::BindsWithoutGrouping),
            None => Ok(None),
        }
    }

    /// The first post-grouping read of a variable that the pre-group pipeline
    /// binds (`where_vars`) but this grouping neither keys, aggregates nor
    /// binds. Such a read would observe a per-group list, which only a JSON-LD
    /// top-level projection may do (`policy`); everything else must be
    /// rejected before the plan runs.
    ///
    /// Stage order is the executor's: the binds HAVING reads
    /// ([`Self::binds_before_having`]), HAVING, the other binds (each bind may
    /// read the ones before it), the ORDER BY binds, ORDER BY, then the
    /// projection (checked only under [`UngroupedProjection::Reject`]). A
    /// variable nothing binds before grouping is not an ungrouped read: it is
    /// unbound there. Neither is a variable that only an `EXISTS` body (or a
    /// pattern comprehension) in the expression mentions: the group row does
    /// not bind it, so it is free in the pattern; the reads checked are
    /// [`Expression::row_reads`].
    ///
    /// The lowerers never produce such a read (they rewrite non-key HAVING /
    /// ORDER BY reads to `SAMPLE`, [`sample_ungrouped_reads`]); this is the
    /// fail-closed check for everything else.
    pub fn first_ungrouped_read(
        &self,
        where_vars: &[VarId],
        order_binds: &[(VarId, Expression)],
        ordering: &[crate::sort::SortSpec],
        projection: Option<&[VarId]>,
        policy: super::query::UngroupedProjection,
    ) -> Option<UngroupedRead> {
        /// The first of `vars` the WHERE binds that is not produced by grouping.
        fn find(
            mut vars: impl Iterator<Item = VarId>,
            where_vars: &[VarId],
            grouped: impl Fn(VarId) -> bool,
        ) -> Option<VarId> {
            vars.find(|v| where_vars.contains(v) && !grouped(*v))
        }
        // What grouping has produced when a stage runs: the keys, the aggregate
        // outputs, the binds run so far (`done`) and the first
        // `order_binds_done` ORDER BY binds. Scanned rather than collected:
        // every grouped plan runs this check, over a few variables.
        let produced = |v: VarId, done: &[VarId], order_binds_done: usize| {
            self.group_by_vars().any(|k| k == v)
                || self.aggregates().any(|spec| spec.output_var == v)
                || done.contains(&v)
                || order_binds[..order_binds_done]
                    .iter()
                    .any(|(out, _)| *out == v)
        };
        let binds = self.bind_list();
        let before_having = self.binds_before_having();
        // The grouping's binds that run before HAVING (`true`) or after it.
        let check_binds = |done: &mut Vec<VarId>, before: bool| -> Option<UngroupedRead> {
            for (i, (out, expr)) in binds.iter().enumerate() {
                if before_having[i] != before {
                    continue;
                }
                if let Some(var) = find(expr.row_reads().into_iter(), where_vars, |v| {
                    produced(v, done, 0)
                }) {
                    return Some(UngroupedRead {
                        var,
                        stage: ReadStage::Bind(*out),
                    });
                }
                done.push(*out);
            }
            None
        };

        let mut done: Vec<VarId> = Vec::new();
        if let Some(read) = check_binds(&mut done, true) {
            return Some(read);
        }
        if let Some(having) = self.having() {
            if let Some(var) = find(having.row_reads().into_iter(), where_vars, |v| {
                produced(v, &done, 0)
            }) {
                return Some(UngroupedRead {
                    var,
                    stage: ReadStage::Having,
                });
            }
        }
        if let Some(read) = check_binds(&mut done, false) {
            return Some(read);
        }
        for (j, (out, expr)) in order_binds.iter().enumerate() {
            if let Some(var) = find(expr.row_reads().into_iter(), where_vars, |v| {
                produced(v, &done, j)
            }) {
                return Some(UngroupedRead {
                    var,
                    stage: ReadStage::OrderBind(*out),
                });
            }
        }
        let after_binds = |v| produced(v, &done, order_binds.len());
        if let Some(var) = find(ordering.iter().map(|s| s.var), where_vars, after_binds) {
            return Some(UngroupedRead {
                var,
                stage: ReadStage::OrderBy,
            });
        }
        if policy == super::query::UngroupedProjection::Reject {
            if let Some(var) =
                projection.and_then(|p| find(p.iter().copied(), where_vars, after_binds))
            {
                return Some(UngroupedRead {
                    var,
                    stage: ReadStage::Projection,
                });
            }
        }
        None
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

    /// Whether this grouping has aggregates and every one is streamable
    /// ([`AggregateFn::is_streamable`]). Unless its projection reads a per-group
    /// list, such a grouping runs on the streaming `GroupAggregateOperator`,
    /// which outputs only the keys and aggregate outputs. Otherwise it runs on
    /// the `GroupByOperator` lane, which also carries the level's other WHERE
    /// columns: as per-group lists under `UngroupedProjection::PerGroupList`,
    /// so a JSON-LD `select *` returns them.
    pub fn aggregates_stream(&self) -> bool {
        let mut aggregates = self.aggregates().peekable();
        aggregates.peek().is_some() && aggregates.all(|spec| spec.function.is_streamable())
    }

    /// The per-group `Extend`s of this grouping phase, in SELECT order
    /// (`(VarId, Expression)` pairs), with or without an aggregation stage.
    /// The ones HAVING reads run before it, the rest after it
    /// ([`Self::binds_before_having`]).
    pub fn binds(&self) -> impl Iterator<Item = &(VarId, Expression)> {
        self.bind_list().iter()
    }

    /// The per-group `Extend`s as a slice (see [`Self::binds`]).
    pub fn bind_list(&self) -> &[(VarId, Expression)] {
        match self {
            Self::Implicit { binds, .. } | Self::Explicit { binds, .. } => binds,
        }
    }

    /// Which of the grouping's binds run before HAVING: the ones HAVING reads,
    /// directly or through another bind (`true` at a bind's index). The others
    /// run after HAVING, and each part keeps SELECT order (a bind reads only
    /// earlier ones).
    ///
    /// So HAVING sees a SELECT alias: an aggregate alias is an aggregate output
    /// and always could, and an expression alias (`(COUNT(?e) + 0 AS ?n)`, or
    /// one over the keys) now can too. SPARQL 1.1 runs every SELECT expression
    /// after HAVING (§18.2.4), so this is a Fluree extension, and so is reading
    /// an aggregate alias; Cypher's `WITH … WHERE` needs it, since its `WHERE`
    /// sees the whole projection. Each bind still runs once per group, so HAVING
    /// tests the value the projection shows. An alias HAVING does not read
    /// keeps the spec's place.
    pub fn binds_before_having(&self) -> Vec<bool> {
        let binds = self.bind_list();
        let mut before = vec![false; binds.len()];
        let Some(having) = self.having() else {
            return before;
        };
        if binds.is_empty() {
            return before;
        }
        // Every variable HAVING mentions, EXISTS correlations included: an alias
        // a correlated pattern reads has to be bound in the row it seeds from.
        let mut read = having.referenced_vars();
        for (i, (out, expr)) in binds.iter().enumerate().rev() {
            if read.contains(out) {
                before[i] = true;
                read.extend(expr.referenced_vars());
            }
        }
        before
    }

    /// Rename every occurrence of variable `old` to `new` across GROUP BY keys,
    /// aggregate input/output vars, post-aggregation binds, and HAVING. Used by
    /// the equijoin-filter fold to unify two provably-equal variables.
    pub fn substitute_var(&mut self, old: VarId, new: VarId) {
        let rename = |v: &mut VarId| {
            if *v == old {
                *v = new;
            }
        };
        let (aggregation, having, binds) = match self {
            Self::Implicit {
                aggregation,
                having,
                binds,
            } => (Some(aggregation), having, binds),
            Self::Explicit {
                group_by,
                aggregation,
                having,
                binds,
            } => {
                for v in group_by.iter_mut() {
                    rename(v);
                }
                (aggregation.as_mut(), having, binds)
            }
        };
        if let Some(agg) = aggregation {
            for spec in agg.aggregates.iter_mut() {
                spec.function.substitute_var(old, new);
                rename(&mut spec.output_var);
            }
        }
        for (var, expr) in binds {
            rename(var);
            expr.substitute_var(old, new);
        }
        if let Some(expr) = having {
            expr.substitute_var(old, new);
        }
    }
}

/// A post-grouping read of a variable the grouping does not produce (see
/// [`Grouping::first_ungrouped_read`]), or another grouping-stage error about
/// one variable: an aggregate reading a variable nothing binds, or an
/// aggregate output that is already bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UngroupedRead {
    /// The variable read.
    pub var: VarId,
    /// Where it is read.
    pub stage: ReadStage,
}

/// The post-grouping stage of an [`UngroupedRead`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStage {
    /// The HAVING expression.
    Having,
    /// The grouping's bind (a SELECT expression) that outputs this variable.
    Bind(VarId),
    /// The ORDER BY bind that outputs this variable.
    OrderBind(VarId),
    /// An ORDER BY key.
    OrderBy,
    /// The projection.
    Projection,
    /// The projection, of a variable nothing in the query binds: not the
    /// WHERE, the grouping or a trailing VALUES.
    UnboundProjection,
    /// An aggregate's input, a variable nothing before grouping binds.
    UnboundAggregateInput,
    /// An aggregate's output, a variable the WHERE pattern already binds.
    BoundAggregateOutput,
    /// An aggregate's output that another aggregate outputs too.
    RepeatedAggregateOutput,
}

impl UngroupedRead {
    /// The user-facing message: which stage reads which variable, and what to
    /// do instead. Variables print as their ids: plan-time code has no
    /// variable names. [`Self::named_message`] names them.
    pub fn message(&self) -> String {
        self.message_with(|var| Some(format!("{var:?}")))
    }

    /// [`Self::message`] with each variable named from `vars`, falling back to
    /// its id for one the registry does not hold. An internal variable (a
    /// synthetic no user wrote, such as a Cypher property access) is not
    /// named: the message says "a variable" instead.
    pub fn named_message(&self, vars: &VarRegistry) -> String {
        self.message_with(|var| match vars.try_name(var) {
            Some(name) if crate::var_registry::is_internal_var_name(name) => None,
            Some(name) => Some(name.to_string()),
            None => Some(format!("{var:?}")),
        })
    }

    fn message_with(&self, name: impl Fn(VarId) -> Option<String>) -> String {
        let neither = "neither a GROUP BY key nor an aggregate result";
        let read = name(self.var);
        match self.stage {
            ReadStage::Having => match read {
                Some(var) => format!("HAVING reads variable {var}, which is {neither}"),
                None => format!("HAVING reads a variable that is {neither}"),
            },
            ReadStage::Bind(out) => {
                let expr = name(out).map_or_else(
                    || "a SELECT expression".to_string(),
                    |out| format!("the SELECT expression for {out}"),
                );
                match read {
                    Some(var) => format!("{expr} reads variable {var}, which is {neither}"),
                    None => format!("{expr} reads a variable that is {neither}"),
                }
            }
            ReadStage::OrderBind(_) => match read {
                Some(var) => {
                    format!("an ORDER BY expression reads variable {var}, which is {neither}")
                }
                None => format!("an ORDER BY expression reads a variable that is {neither}"),
            },
            ReadStage::OrderBy => match read {
                Some(var) => format!("ORDER BY variable {var} is {neither}"),
                None => format!("an ORDER BY key is {neither}"),
            },
            ReadStage::Projection => {
                let var = read.map_or_else(
                    || "a projected variable".to_string(),
                    |var| format!("projected variable {var}"),
                );
                format!(
                    "{var} is {neither}; aggregate it (e.g. with SAMPLE, collect or \
                     group-concat)"
                )
            }
            ReadStage::UnboundProjection => {
                let var = read.map_or_else(
                    || "a projected variable".to_string(),
                    |var| format!("projected variable {var}"),
                );
                format!("{var} is unbound: nothing in the query binds it")
            }
            ReadStage::UnboundAggregateInput => match read {
                Some(var) => format!(
                    "an aggregate reads variable {var}, which is unbound: nothing in the \
                     query binds it"
                ),
                None => "an aggregate reads a variable that nothing in the query binds".to_string(),
            },
            ReadStage::BoundAggregateOutput => match read {
                Some(var) => {
                    format!("aggregate output variable {var} is already bound in the WHERE pattern")
                }
                None => {
                    "an aggregate output variable is already bound in the WHERE pattern".to_string()
                }
            },
            ReadStage::RepeatedAggregateOutput => match read {
                Some(var) => format!("variable {var} is the output of more than one aggregate"),
                None => "a variable is the output of more than one aggregate".to_string(),
            },
        }
    }
}

/// In a grouping level, give every HAVING / ORDER BY read of a non-key
/// variable its SPARQL 1.1 meaning, `SAMPLE(?v)` (§18.2.4.1: "For each … HAVING(X),
/// and each ORDER BY X … For each unaggregated variable V in X / Replace V with
/// Sample(V)").
///
/// A read of `?v` is rewritten when the level's pre-group pipeline binds `?v`
/// (`where_vars`, called at most once, and only when HAVING or ORDER BY reads a
/// non-key variable) and `?v` is not a group key. Everything else is left alone:
/// a key (`SAMPLE(key)` is the key), an aggregate output, a per-group `Extend`
/// output (HAVING reads it unbound, §18.2.4.2; ORDER BY reads the Extend), and a
/// variable nothing binds (unbound either way). The rewrite reuses a `SAMPLE(?v)`
/// the level already computes, and otherwise adds one whose output `mint`
/// names. It reads and renames only what the expressions read from the group
/// row ([`Expression::row_reads`], [`Expression::substitute_row_read`]): a
/// variable inside an `EXISTS` body is a pattern variable, not a variable of
/// the expression (`Sample(?v)` cannot stand in a triple pattern), and the
/// group row does not bind it, so the pattern sees it free. After the rewrite,
/// every such read is a read of an aggregate output, so running it again
/// changes nothing.
///
/// Which value SAMPLE picks is implementation-defined.
pub fn sample_ungrouped_reads(
    keys: &[VarId],
    aggregates: &mut Vec<AggregateSpec>,
    having: Option<&mut Expression>,
    order_binds: &mut [(VarId, Expression)],
    ordering: &mut [crate::sort::SortSpec],
    where_vars: impl FnOnce() -> HashSet<VarId>,
    mint: &mut dyn FnMut(VarId) -> VarId,
) {
    // The non-key reads first: a level without HAVING or ORDER BY has none,
    // and then never collects `where_vars`.
    let mut reads: Vec<VarId> = Vec::new();
    let mut note = |v: VarId| {
        if !keys.contains(&v) && !reads.contains(&v) {
            reads.push(v);
        }
    };
    if let Some(having) = having.as_deref() {
        having.row_reads().into_iter().for_each(&mut note);
    }
    for (_, expr) in order_binds.iter() {
        expr.row_reads().into_iter().for_each(&mut note);
    }
    for spec in ordering.iter() {
        note(spec.var);
    }
    if reads.is_empty() {
        return;
    }
    let where_vars = where_vars();
    reads.retain(|v| where_vars.contains(v));

    let mut having = having;
    for v in reads {
        let sampled = aggregates
            .iter()
            .find(|spec| spec.function == AggregateFn::Sample(v))
            .map(|spec| spec.output_var)
            .unwrap_or_else(|| {
                let output_var = mint(v);
                aggregates.push(AggregateSpec {
                    function: AggregateFn::Sample(v),
                    output_var,
                });
                output_var
            });
        if let Some(having) = having.as_deref_mut() {
            having.substitute_row_read(v, sampled);
        }
        for (_, expr) in order_binds.iter_mut() {
            expr.substitute_row_read(v, sampled);
        }
        for spec in ordering.iter_mut() {
            if spec.var == v {
                spec.var = sampled;
            }
        }
    }
}

/// A HAVING on a level that does not group: SPARQL 1.1 §18.2.4.2 makes it a
/// Filter over the level's solutions ("For each HAVING(E) in Q / P :=
/// Filter(E, P)"), which does not depend on grouping.
///
/// The level's SELECT expressions are evaluated after HAVING, so HAVING cannot
/// see them — but in a level that does not group they are WHERE `BIND`s, which a
/// Filter placed in the WHERE could see. Reads of `select_aliases` are therefore
/// renamed to fresh variables (named by `mint`) that nothing binds.
pub fn having_as_filter(
    mut having: Expression,
    select_aliases: &HashSet<VarId>,
    mint: &mut dyn FnMut(VarId) -> VarId,
) -> super::pattern::Pattern {
    read_as_unbound(&mut having, select_aliases, mint);
    super::pattern::Pattern::Filter(having)
}

/// Rename every read of `vars` in `expr` to a fresh variable, named by
/// `mint`, that nothing binds, so `expr` reads them as unbound wherever it is
/// evaluated. For a HAVING that must not see variables a later stage binds:
/// the level's SELECT expressions, or its trailing VALUES clause (joined after
/// HAVING, SPARQL 1.1 §18.2.4).
pub fn read_as_unbound(
    expr: &mut Expression,
    vars: &HashSet<VarId>,
    mint: &mut dyn FnMut(VarId) -> VarId,
) {
    let mut read: Vec<VarId> = expr
        .referenced_vars()
        .into_iter()
        .filter(|v| vars.contains(v))
        .collect();
    read.sort_unstable();
    read.dedup();
    for var in read {
        let unbound = mint(var);
        expr.substitute_var(var, unbound);
    }
}

/// Where a SELECT-clause expression `(expr AS ?alias)` of one query level is
/// evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectExprPlacement {
    /// A WHERE `BIND`, evaluated once per solution before grouping.
    PreGroup,
    /// A per-group `Extend` in [`Grouping`]'s `binds`, evaluated once per
    /// group after HAVING (SPARQL 1.1 §18.2.4.4).
    PostGroup,
}

/// Places the SELECT expressions of one query level. The one placement rule for
/// every surface (SPARQL and JSON-LD share it).
///
/// In a level that does not group, every expression is a WHERE bind. In a
/// grouping level:
///
/// - An expression that reads an aggregate output, or an alias that does, runs
///   once per group, after HAVING. So does one that reads only group keys,
///   constants and aliases of those (it is constant within its group).
/// - An expression runs before grouping, once per solution, when its alias is a
///   group key (the key has to exist before grouping: the `GROUP BY
///   (LCASE(?a))` + `(LCASE(?a) AS ?k)` shortcut, a JSON-LD `groupBy` naming a
///   computed alias) or an aggregate input (JSON-LD `(as (str ?a) ?s)` +
///   `(count ?s)`), or when it reads a variable the pre-group pipeline binds
///   that is not a group key, and no aggregate output. That is JSON-LD's
///   documented per-group list (`(as (str ?e) ?es)` under `groupBy ?a`);
///   SPARQL's validator rejects the shape.
/// - An alias that a before-grouping expression reads has to exist before
///   grouping. A key-only alias is constant within its group, so it moves
///   before grouping with its reader, and the reader stays per solution
///   (JSON-LD `(as (strlen ?a) ?len)` + `(as (+ ?len (strlen (str ?e))) ?x)`
///   under `groupBy ?a` gives both as per-group lists).
///
/// An expression that reads an aggregate output and a non-key variable runs
/// per group, where the plan-time check rejects the non-key read. A variable
/// nothing binds before grouping (a typo) is unbound either way, and one that
/// only an `EXISTS` body mentions is free in the pattern over a group row
/// ([`Expression::row_reads`]), so neither holds an expression back.
#[derive(Debug)]
pub struct SelectExprPlacer {
    grouped: bool,
    keys: HashSet<VarId>,
    aggregate_outputs: HashSet<VarId>,
    aggregate_inputs: HashSet<VarId>,
    where_vars: HashSet<VarId>,
}

impl SelectExprPlacer {
    /// A placer for a level that does not group: everything is pre-group.
    pub fn ungrouped() -> Self {
        Self {
            grouped: false,
            keys: HashSet::new(),
            aggregate_outputs: HashSet::new(),
            aggregate_inputs: HashSet::new(),
            where_vars: HashSet::new(),
        }
    }

    /// A placer for a grouping level. `where_vars` are the variables the
    /// level's pre-group pipeline binds.
    pub fn grouped<'a>(
        keys: impl IntoIterator<Item = VarId>,
        aggregates: impl IntoIterator<Item = &'a AggregateSpec>,
        where_vars: HashSet<VarId>,
    ) -> Self {
        let mut aggregate_outputs = HashSet::new();
        let mut aggregate_inputs = HashSet::new();
        for spec in aggregates {
            aggregate_outputs.insert(spec.output_var);
            // `COUNT(DISTINCT *)` reads the solution, not a SELECT alias, so
            // `input_var()` (None for it) is the right question.
            aggregate_inputs.extend(spec.function.input_var());
        }
        Self {
            grouped: true,
            keys: keys.into_iter().collect(),
            aggregate_outputs,
            aggregate_inputs,
            where_vars,
        }
    }

    /// Place one level's SELECT expressions. `items` are in SELECT order:
    /// `(alias, expression, contains_aggregate)`, where `contains_aggregate`
    /// marks an expression with an aggregate inside it (it runs per group).
    /// Returns one placement per item.
    pub fn place_all(&self, items: &[(VarId, &Expression, bool)]) -> Vec<SelectExprPlacement> {
        if !self.grouped {
            return vec![SelectExprPlacement::PreGroup; items.len()];
        }
        let refs: Vec<Vec<VarId>> = items.iter().map(|(_, expr, _)| expr.row_reads()).collect();
        // Key-only aliases that a before-grouping expression reads, found by
        // walking back from each such reader until nothing more moves.
        let mut moved: HashSet<VarId> = HashSet::new();
        loop {
            let (placements, per_group) = self.place_forward(items, &refs, &moved);
            let mut grew = false;
            for (i, refs) in refs.iter().enumerate().rev() {
                if placements[i] != SelectExprPlacement::PreGroup {
                    continue;
                }
                for var in refs {
                    let earlier = items[..i].iter().rposition(|(alias, _, _)| alias == var);
                    if let Some(j) = earlier {
                        if placements[j] == SelectExprPlacement::PostGroup
                            && !per_group.contains(var)
                            && moved.insert(*var)
                        {
                            grew = true;
                        }
                    }
                }
            }
            if !grew {
                return placements;
            }
        }
    }

    /// One pass in SELECT order: an alias reads only earlier ones. `moved`
    /// holds key-only aliases that have to run before grouping. Also returns
    /// the aliases that depend on an aggregate.
    fn place_forward(
        &self,
        items: &[(VarId, &Expression, bool)],
        refs: &[Vec<VarId>],
        moved: &HashSet<VarId>,
    ) -> (Vec<SelectExprPlacement>, HashSet<VarId>) {
        let mut where_vars = self.where_vars.clone();
        let mut per_group: HashSet<VarId> = HashSet::new();
        let mut placements = Vec::with_capacity(items.len());
        for ((alias, _, contains_aggregate), refs) in items.iter().zip(refs) {
            let reads_aggregate = *contains_aggregate
                || refs
                    .iter()
                    .any(|v| self.aggregate_outputs.contains(v) || per_group.contains(v));
            let placement = if reads_aggregate {
                per_group.insert(*alias);
                SelectExprPlacement::PostGroup
            } else if moved.contains(alias)
                || self.keys.contains(alias)
                || self.aggregate_inputs.contains(alias)
                || refs
                    .iter()
                    .any(|v| where_vars.contains(v) && !self.keys.contains(v))
            {
                SelectExprPlacement::PreGroup
            } else {
                SelectExprPlacement::PostGroup
            };
            if placement == SelectExprPlacement::PreGroup {
                where_vars.insert(*alias);
            }
            placements.push(placement);
        }
        (placements, per_group)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::var_registry::VarId;
    use fluree_db_core::FlakeValue;

    /// A FILTER pattern reading `v`, for EXISTS bodies.
    fn filter_on(v: VarId) -> crate::ir::Pattern {
        crate::ir::Pattern::Filter(Expression::Var(v))
    }

    #[test]
    fn sample_ungrouped_reads_rewrites_only_non_key_where_reads() {
        use crate::sort::SortSpec;
        // ?a key, ?e non-key WHERE var, ?n aggregate output, ?x an Extend
        // output, ?nosuch bound nowhere, ?es a JSON-LD pre-group alias.
        let (a, e, n, x, nosuch, es) = (VarId(0), VarId(1), VarId(2), VarId(3), VarId(4), VarId(5));
        let where_vars: HashSet<VarId> = [a, e, es].into_iter().collect();
        let mut aggregates = vec![AggregateSpec {
            function: AggregateFn::Count(e),
            output_var: n,
        }];
        let mut having = Expression::and(vec![
            Expression::Var(a),
            Expression::Var(e),
            Expression::Var(n),
            Expression::Var(x),
            Expression::Var(nosuch),
            Expression::Exists {
                patterns: vec![filter_on(e)],
                negated: false,
            },
        ]);
        let mut order_binds = vec![(VarId(20), Expression::Var(es))];
        let mut ordering = vec![SortSpec::asc(e), SortSpec::asc(a), SortSpec::asc(x)];
        let mut next = 100;
        let mut mint = |_| {
            next += 1;
            VarId(next)
        };
        sample_ungrouped_reads(
            &[a],
            &mut aggregates,
            Some(&mut having),
            &mut order_binds,
            &mut ordering,
            || where_vars.clone(),
            &mut mint,
        );

        // ?e and ?es are sampled, once each, in first-read order.
        let samples: Vec<(VarId, VarId)> = aggregates
            .iter()
            .filter_map(|s| match s.function {
                AggregateFn::Sample(v) => Some((v, s.output_var)),
                _ => None,
            })
            .collect();
        assert_eq!(samples, vec![(e, VarId(101)), (es, VarId(102))]);
        let reads = having.row_reads();
        assert!(!reads.contains(&e), "?e renamed where the HAVING reads it");
        assert!(reads.contains(&VarId(101)));
        for kept in [a, n, x, nosuch] {
            assert!(reads.contains(&kept), "{kept:?} left alone");
        }
        // The EXISTS body keeps ?e: the group row does not bind it, so it is
        // free in the pattern, not the sampled value.
        assert!(having.referenced_vars().contains(&e), "EXISTS body renamed");
        assert_eq!(order_binds[0].1.referenced_vars(), vec![VarId(102)]);
        assert_eq!(
            ordering.iter().map(|s| s.var).collect::<Vec<_>>(),
            vec![VarId(101), a, x]
        );

        // Idempotent: every such read is now an aggregate output.
        let (having_before, aggregates_before) = (format!("{having:?}"), aggregates.len());
        sample_ungrouped_reads(
            &[a],
            &mut aggregates,
            Some(&mut having),
            &mut order_binds,
            &mut ordering,
            || where_vars.clone(),
            &mut mint,
        );
        assert_eq!(aggregates.len(), aggregates_before);
        assert_eq!(format!("{having:?}"), having_before);
    }

    #[test]
    fn sample_ungrouped_reads_reuses_an_existing_sample() {
        use crate::sort::SortSpec;
        let (a, e, s) = (VarId(0), VarId(1), VarId(2));
        let mut aggregates = vec![AggregateSpec {
            function: AggregateFn::Sample(e),
            output_var: s,
        }];
        let mut ordering = vec![SortSpec::asc(e)];
        sample_ungrouped_reads(
            &[a],
            &mut aggregates,
            None,
            &mut [],
            &mut ordering,
            || [a, e].into_iter().collect(),
            &mut |_| panic!("an existing SAMPLE(?e) is reused"),
        );
        assert_eq!(aggregates.len(), 1);
        assert_eq!(ordering[0].var, s);
    }

    /// A variable only an EXISTS body mentions is free over the group row:
    /// the SAMPLE rewrite, the plan-time check and the placer all leave it
    /// alone, while an EXISTS correlated on a key keeps its key.
    #[test]
    fn exists_body_variables_are_not_group_row_reads() {
        use crate::ir::UngroupedProjection::Reject;
        let (k, e, n, f) = (VarId(0), VarId(1), VarId(2), VarId(3));
        let exists = |v: VarId| Expression::Exists {
            patterns: vec![filter_on(v)],
            negated: false,
        };
        let count = AggregateSpec {
            function: AggregateFn::Count(e),
            output_var: n,
        };

        // No SAMPLE for ?e: the body is not rewritten, and nothing is collected.
        let mut aggregates = vec![count.clone()];
        let mut having = exists(e);
        sample_ungrouped_reads(
            &[k],
            &mut aggregates,
            Some(&mut having),
            &mut [],
            &mut [],
            || panic!("where_vars collected for an EXISTS-only variable"),
            &mut |_| panic!("an EXISTS-only variable sampled"),
        );
        assert_eq!(aggregates.len(), 1);
        assert_eq!(having.referenced_vars(), vec![e]);

        // Not an ungrouped read, in HAVING or in a bind.
        let where_vars = [k, e];
        let g = Grouping::assemble(
            vec![k],
            vec![count.clone()],
            vec![(f, exists(e))],
            Some(exists(e)),
        )
        .expect("valid")
        .expect("grouping");
        assert_eq!(
            g.first_ungrouped_read(&where_vars, &[], &[], Some(&[k, f, n]), Reject),
            None
        );

        // An EXISTS-only expression is evaluated per group; one that also
        // reads ?e outside the body still runs per solution.
        let placer = SelectExprPlacer::grouped([k], [&count], [k, e].into_iter().collect());
        let both = Expression::and(vec![Expression::Var(e), exists(e)]);
        assert_eq!(
            placer.place_all(&[(f, &exists(e), false), (VarId(4), &both, false)]),
            vec![
                SelectExprPlacement::PostGroup,
                SelectExprPlacement::PreGroup
            ]
        );
    }

    #[test]
    fn sample_ungrouped_reads_collects_where_vars_only_for_a_non_key_read() {
        use crate::sort::SortSpec;
        let (a, n) = (VarId(0), VarId(1));
        let mut aggregates = vec![AggregateSpec {
            function: AggregateFn::CountAll,
            output_var: n,
        }];
        // No HAVING or ORDER BY, or an ORDER BY of a key: nothing to sample,
        // so the level's variables are never collected (every grouped query
        // lowers through here).
        for mut ordering in [vec![], vec![SortSpec::asc(a)]] {
            sample_ungrouped_reads(
                &[a],
                &mut aggregates,
                None,
                &mut [],
                &mut ordering,
                || panic!("where_vars collected without a non-key read"),
                &mut |_| panic!("nothing to sample"),
            );
        }
        assert_eq!(aggregates.len(), 1);
    }

    /// The binds HAVING reads, directly or through another bind, run before it;
    /// the rest after it, each part in SELECT order.
    #[test]
    fn binds_before_having_follow_what_having_reads() {
        use crate::ir::UngroupedProjection::Reject;
        let (k, n, s, t, u, w) = (VarId(0), VarId(1), VarId(2), VarId(3), VarId(4), VarId(5));
        let count = AggregateSpec {
            function: AggregateFn::CountAll,
            output_var: n,
        };
        // ?s = ?n + 0, ?t = ?s * 10, ?u = ?k; HAVING reads ?t.
        let binds = vec![
            (
                s,
                Expression::call(
                    crate::ir::Function::Add,
                    vec![Expression::Var(n), Expression::Const(FlakeValue::Long(0))],
                ),
            ),
            (
                t,
                Expression::call(
                    crate::ir::Function::Mul,
                    vec![Expression::Var(s), Expression::Const(FlakeValue::Long(10))],
                ),
            ),
            (u, Expression::Var(k)),
        ];
        let g = Grouping::assemble(
            vec![k],
            vec![count.clone()],
            binds.clone(),
            Some(Expression::Var(t)),
        )
        .expect("valid")
        .expect("grouping");
        assert_eq!(g.binds_before_having(), vec![true, true, false]);

        // Without HAVING, or when it reads no alias, every bind runs after it.
        let none = Grouping::assemble(
            vec![k],
            vec![count.clone()],
            binds.clone(),
            Some(Expression::Var(n)),
        )
        .expect("valid")
        .expect("grouping");
        assert_eq!(none.binds_before_having(), vec![false, false, false]);

        // The plan check walks the same order: a pre-HAVING bind reading a
        // non-key WHERE variable is reported before HAVING's own read.
        let bad = vec![(s, Expression::Var(w)), (t, Expression::Var(k))];
        let g = Grouping::assemble(
            vec![k],
            vec![count],
            bad,
            Some(Expression::and(vec![
                Expression::Var(s),
                Expression::Var(w),
            ])),
        )
        .expect("valid")
        .expect("grouping");
        assert_eq!(
            g.first_ungrouped_read(&[k, w], &[], &[], None, Reject),
            Some(UngroupedRead {
                var: w,
                stage: ReadStage::Bind(s)
            })
        );
    }

    #[test]
    fn having_as_filter_hides_select_aliases() {
        let (a, alias) = (VarId(0), VarId(1));
        let having = Expression::and(vec![Expression::Var(a), Expression::Var(alias)]);
        let crate::ir::Pattern::Filter(filter) =
            having_as_filter(having, &[alias].into_iter().collect(), &mut |_| VarId(9))
        else {
            panic!("a Filter pattern");
        };
        let refs = filter.referenced_vars();
        assert!(refs.contains(&a) && refs.contains(&VarId(9)));
        assert!(
            !refs.contains(&alias),
            "the alias is read as a fresh, unbound var"
        );
    }

    #[test]
    fn assemble_fails_closed_without_keys_or_aggregates() {
        let (a, x) = (VarId(0), VarId(1));
        assert!(matches!(
            Grouping::assemble(vec![], vec![], vec![], Some(Expression::Var(a))),
            Err(GroupingError::HavingWithoutGrouping)
        ));
        assert!(matches!(
            Grouping::assemble(vec![], vec![], vec![(x, Expression::Var(a))], None),
            Err(GroupingError::BindsWithoutGrouping)
        ));
        assert!(matches!(
            Grouping::assemble(vec![], vec![], vec![], None),
            Ok(None)
        ));
        // A dedup-only GROUP BY keeps its binds.
        let g = Grouping::assemble(vec![a], vec![], vec![(x, Expression::Var(a))], None)
            .expect("valid")
            .expect("explicit");
        assert_eq!(g.bind_list().len(), 1);
        assert!(g.aggregation().is_none());
    }

    #[test]
    fn first_ungrouped_read_by_stage_and_variable_kind() {
        use crate::ir::UngroupedProjection::{PerGroupList, Reject};
        use crate::sort::SortSpec;
        // ?k key, ?n aggregate output, ?b an earlier bind, ?w a non-key WHERE
        // variable, ?u a variable nothing binds before grouping.
        let (k, n, b, w, u, out) = (VarId(0), VarId(1), VarId(2), VarId(3), VarId(4), VarId(5));
        let where_vars = [k, w];
        let count = AggregateSpec {
            function: AggregateFn::Count(w),
            output_var: n,
        };
        let grouping = |having: Option<Expression>, binds: Vec<(VarId, Expression)>| {
            Grouping::assemble(vec![k], vec![count.clone()], binds, having)
                .expect("valid")
                .expect("grouping")
        };
        for (read, ungrouped) in [(k, false), (n, false), (b, false), (w, true), (u, false)] {
            let expect = |stage| ungrouped.then_some(UngroupedRead { var: read, stage });
            let earlier = (b, Expression::Var(k));

            let g = grouping(Some(Expression::Var(read)), vec![]);
            // ?b is not bound here (no bind produces it), and it is not a
            // WHERE variable either: not a grouped read.
            assert_eq!(
                g.first_ungrouped_read(&where_vars, &[], &[], None, Reject),
                expect(ReadStage::Having),
                "HAVING {read:?}"
            );
            let g = grouping(None, vec![earlier.clone(), (out, Expression::Var(read))]);
            assert_eq!(
                g.first_ungrouped_read(&where_vars, &[], &[], None, Reject),
                expect(ReadStage::Bind(out)),
                "bind {read:?}"
            );
            let g = grouping(None, vec![earlier.clone()]);
            assert_eq!(
                g.first_ungrouped_read(
                    &where_vars,
                    &[(out, Expression::Var(read))],
                    &[],
                    None,
                    Reject
                ),
                expect(ReadStage::OrderBind(out)),
                "order bind {read:?}"
            );
            assert_eq!(
                g.first_ungrouped_read(&where_vars, &[], &[SortSpec::asc(read)], None, Reject),
                expect(ReadStage::OrderBy),
                "ORDER BY {read:?}"
            );
            assert_eq!(
                g.first_ungrouped_read(&where_vars, &[], &[], Some(&[read]), Reject),
                expect(ReadStage::Projection),
                "projection {read:?}"
            );
            // A JSON-LD top-level projection may carry the per-group list...
            assert_eq!(
                g.first_ungrouped_read(&where_vars, &[], &[], Some(&[read]), PerGroupList),
                None,
                "per-group-list projection {read:?}"
            );
            // ...but no other stage may read it under that policy either.
            assert_eq!(
                g.first_ungrouped_read(
                    &where_vars,
                    &[],
                    &[SortSpec::asc(read)],
                    Some(&[read]),
                    PerGroupList
                ),
                expect(ReadStage::OrderBy),
                "ORDER BY under per-group-list {read:?}"
            );
        }
    }

    /// The message names variables from the registry when one is at hand, and
    /// falls back to the id for one it does not hold.
    #[test]
    fn ungrouped_read_message_names_variables() {
        let mut vars = VarRegistry::new();
        let x = vars.get_or_insert("?x");
        let t = vars.get_or_insert("?t");
        let read = UngroupedRead {
            var: x,
            stage: ReadStage::Bind(t),
        };
        assert_eq!(
            read.named_message(&vars),
            "the SELECT expression for ?t reads variable ?x, which is neither a GROUP BY key \
             nor an aggregate result"
        );
        assert_eq!(
            read.message(),
            format!(
                "the SELECT expression for {t:?} reads variable {x:?}, which is neither a \
                 GROUP BY key nor an aggregate result"
            )
        );
        let unknown = UngroupedRead {
            var: VarId(99),
            stage: ReadStage::OrderBy,
        };
        assert_eq!(
            unknown.named_message(&vars),
            "ORDER BY variable VarId(99) is neither a GROUP BY key nor an aggregate result"
        );
        let unbound = UngroupedRead {
            var: x,
            stage: ReadStage::UnboundProjection,
        };
        assert_eq!(
            unbound.named_message(&vars),
            "projected variable ?x is unbound: nothing in the query binds it"
        );
        // An internal variable (here a Cypher property access) is not named.
        let prop = vars.get_or_insert("?#__prop_e_area");
        let internal = UngroupedRead {
            var: prop,
            stage: ReadStage::OrderBy,
        };
        assert_eq!(
            internal.named_message(&vars),
            "an ORDER BY key is neither a GROUP BY key nor an aggregate result"
        );
        let synthetic = vars.get_or_insert("?__sample_7");
        let bind = UngroupedRead {
            var: prop,
            stage: ReadStage::Bind(synthetic),
        };
        assert_eq!(
            bind.named_message(&vars),
            "a SELECT expression reads a variable that is neither a GROUP BY key nor an \
             aggregate result"
        );
        let err = crate::error::QueryError::UngroupedRead(read).name_variables(&vars);
        assert!(
            matches!(&err, crate::error::QueryError::InvalidQuery(msg) if msg.contains("?x")),
            "{err:?}"
        );
    }

    /// The lowerers' SAMPLE rewrite and the plan-time predicate compose: after
    /// the rewrite, a HAVING read of a non-key variable is a read of an
    /// aggregate output; without it, the predicate rejects the plan (s04b:
    /// `?a (GROUP_CONCAT(?e) AS ?g) … GROUP BY ?a HAVING (?e = ex:e1)`).
    #[test]
    fn sample_rewrite_then_predicate() {
        use crate::ir::UngroupedProjection::Reject;
        let (a, e, g, iri) = (VarId(0), VarId(1), VarId(2), VarId(3));
        let where_vars = [a, e];
        let concat = AggregateSpec {
            function: AggregateFn::GroupConcat {
                input: e,
                semantics: InputSemantics::List,
                separator: " ".into(),
            },
            output_var: g,
        };
        let having = || Expression::eq(Expression::Var(e), Expression::Var(iri));

        let skipped = Grouping::assemble(vec![a], vec![concat.clone()], vec![], Some(having()))
            .expect("valid")
            .expect("grouping");
        assert_eq!(
            skipped.first_ungrouped_read(&where_vars, &[], &[], Some(&[a, g]), Reject),
            Some(UngroupedRead {
                var: e,
                stage: ReadStage::Having
            })
        );

        let mut aggregates = vec![concat];
        let mut rewritten = having();
        sample_ungrouped_reads(
            &[a],
            &mut aggregates,
            Some(&mut rewritten),
            &mut [],
            &mut [],
            || where_vars.into_iter().collect(),
            &mut |_| VarId(9),
        );
        let sampled = Grouping::assemble(vec![a], aggregates, vec![], Some(rewritten))
            .expect("valid")
            .expect("grouping");
        assert_eq!(
            sampled.first_ungrouped_read(&where_vars, &[], &[], Some(&[a, g]), Reject),
            None
        );
    }

    #[test]
    fn select_expr_placement_rule() {
        use SelectExprPlacement::{PostGroup, PreGroup};
        let (a, e, n, nosuch) = (VarId(0), VarId(1), VarId(2), VarId(3));
        let count_e = AggregateSpec {
            function: AggregateFn::Count(e),
            output_var: n,
        };
        let where_vars: HashSet<VarId> = [a, e].into_iter().collect();
        let var = Expression::Var;
        let placer = SelectExprPlacer::grouped([a], [&count_e], where_vars.clone());
        let place = |items: &[(VarId, Expression, bool)]| {
            let items: Vec<(VarId, &Expression, bool)> = items
                .iter()
                .map(|(alias, expr, aggregate)| (*alias, expr, *aggregate))
                .collect();
            placer.place_all(&items)
        };
        let (k1, k2, k3, k4, k5) = (VarId(10), VarId(11), VarId(12), VarId(13), VarId(14));

        // Key-only and constant expressions run once per group; so does one
        // over a variable nothing binds before grouping, and one reading an
        // aggregate output or an alias that does.
        assert_eq!(
            place(&[
                (k1, var(a), false),
                (k2, Expression::Const(FlakeValue::Long(1)), false),
                (k3, var(nosuch), false),
                (k4, var(n), false),
                (k5, var(k1), false),
            ]),
            vec![PostGroup; 5]
        );
        // A non-key WHERE variable runs before grouping (JSON-LD's per-group
        // list), and so does an expression over that alias.
        assert_eq!(
            place(&[(k1, var(e), false), (k2, var(k1), false)]),
            vec![PreGroup, PreGroup]
        );
        // Reading an aggregate output wins over a non-key read: per group, where
        // the plan-time check rejects the non-key read. An expression with an
        // aggregate inside is per group.
        assert_eq!(
            place(&[
                (k1, Expression::add(var(n), var(e)), false),
                (k2, var(a), true)
            ]),
            vec![PostGroup, PostGroup]
        );
        // A key-only alias that a before-grouping expression reads moves before
        // grouping with it, and so does another key-only expression over it.
        assert_eq!(
            place(&[
                (k1, var(a), false),
                (k2, Expression::add(var(k1), var(k1)), false),
                (k3, Expression::add(var(k1), var(e)), false),
            ]),
            vec![PreGroup, PreGroup, PreGroup]
        );
        // One that only a per-group expression reads stays per group.
        assert_eq!(
            place(&[
                (k1, var(a), false),
                (k2, Expression::add(var(n), var(k1)), false)
            ]),
            vec![PostGroup, PostGroup]
        );

        // The alias is a key: it has to exist before grouping.
        let placer = SelectExprPlacer::grouped([a, VarId(20)], [&count_e], where_vars.clone());
        assert_eq!(
            placer.place_all(&[(VarId(20), &var(a), false)]),
            vec![PreGroup]
        );

        // The alias is an aggregate input: before grouping.
        let count_alias = AggregateSpec {
            function: AggregateFn::Count(VarId(21)),
            output_var: VarId(22),
        };
        let placer = SelectExprPlacer::grouped([a], [&count_alias], where_vars);
        assert_eq!(
            placer.place_all(&[(VarId(21), &var(a), false)]),
            vec![PreGroup]
        );

        // A level that does not group: always a WHERE bind.
        assert_eq!(
            SelectExprPlacer::ungrouped().place_all(&[(VarId(30), &var(n), false)]),
            vec![PreGroup]
        );
    }

    #[test]
    fn duplicate_insensitive_partitions_aggregates() {
        let v = VarId(0);
        // Insensitive: every DISTINCT-marked variant plus MIN/MAX/SAMPLE.
        for f in [
            AggregateFn::CountDistinct(v),
            AggregateFn::Sum(v, InputSemantics::Set),
            AggregateFn::Avg(v, InputSemantics::Set),
            AggregateFn::Median(v, InputSemantics::Set),
            AggregateFn::Variance(v, InputSemantics::Set),
            AggregateFn::Stddev(v, InputSemantics::Set),
            AggregateFn::GroupConcat {
                input: v,
                semantics: InputSemantics::Set,
                separator: ",".into(),
            },
            AggregateFn::Collect(v, InputSemantics::Set),
            AggregateFn::Min(v),
            AggregateFn::Max(v),
            AggregateFn::Sample(v),
        ] {
            assert!(f.duplicate_insensitive(), "{f:?} must be insensitive");
        }
        // Sensitive: multiplicity-observing variants — WHERE-level early dedup
        // must be blocked when any of these is present.
        for f in [
            AggregateFn::Count(v),
            AggregateFn::CountAll,
            AggregateFn::Sum(v, InputSemantics::List),
            AggregateFn::Avg(v, InputSemantics::List),
            AggregateFn::Median(v, InputSemantics::List),
            AggregateFn::Variance(v, InputSemantics::List),
            AggregateFn::Stddev(v, InputSemantics::List),
            AggregateFn::GroupConcat {
                input: v,
                semantics: InputSemantics::List,
                separator: ",".into(),
            },
            AggregateFn::Collect(v, InputSemantics::List),
        ] {
            assert!(!f.duplicate_insensitive(), "{f:?} must be sensitive");
        }
    }
}
