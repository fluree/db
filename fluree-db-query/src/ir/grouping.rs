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
    /// Stage order is the executor's: HAVING, the grouping's binds (each may
    /// read the ones before it), the ORDER BY binds, ORDER BY, then the
    /// projection (checked only under [`UngroupedProjection::Reject`]). A
    /// variable nothing binds before grouping is not an ungrouped read: it is
    /// unbound there (a HAVING reading a SELECT alias, §18.2.4.2).
    /// [`Expression::referenced_vars`] includes `EXISTS` correlation variables,
    /// so an `EXISTS` inside a grouped expression is covered too.
    ///
    /// The lowerers never produce such a read (they rewrite non-key HAVING /
    /// ORDER BY reads to `SAMPLE`, [`sample_ungrouped_reads`]); this is the
    /// fail-closed check for everything else.
    pub fn first_ungrouped_read(
        &self,
        where_vars: &HashSet<VarId>,
        order_binds: &[(VarId, Expression)],
        ordering: &[crate::sort::SortSpec],
        projection: Option<&[VarId]>,
        policy: super::query::UngroupedProjection,
    ) -> Option<UngroupedRead> {
        /// The first of `vars` the WHERE binds that is not produced by grouping.
        fn find(
            where_vars: &HashSet<VarId>,
            grouped: &HashSet<VarId>,
            mut vars: impl Iterator<Item = VarId>,
        ) -> Option<VarId> {
            vars.find(|v| where_vars.contains(v) && !grouped.contains(v))
        }
        let mut grouped: HashSet<VarId> = self.group_by_vars().collect();
        grouped.extend(self.aggregates().map(|spec| spec.output_var));

        if let Some(having) = self.having() {
            if let Some(var) = find(where_vars, &grouped, having.referenced_vars().into_iter()) {
                return Some(UngroupedRead {
                    var,
                    stage: ReadStage::Having,
                });
            }
        }
        for (out, expr) in self.binds() {
            if let Some(var) = find(where_vars, &grouped, expr.referenced_vars().into_iter()) {
                return Some(UngroupedRead {
                    var,
                    stage: ReadStage::Bind(*out),
                });
            }
            grouped.insert(*out);
        }
        for (out, expr) in order_binds {
            if let Some(var) = find(where_vars, &grouped, expr.referenced_vars().into_iter()) {
                return Some(UngroupedRead {
                    var,
                    stage: ReadStage::OrderBind(*out),
                });
            }
            grouped.insert(*out);
        }
        if let Some(var) = find(where_vars, &grouped, ordering.iter().map(|s| s.var)) {
            return Some(UngroupedRead {
                var,
                stage: ReadStage::OrderBy,
            });
        }
        if policy == super::query::UngroupedProjection::Reject {
            if let Some(var) =
                projection.and_then(|p| find(where_vars, &grouped, p.iter().copied()))
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

    /// The per-group `Extend`s of this grouping phase, in evaluation order
    /// (`(VarId, Expression)` pairs). They run after HAVING, with or without
    /// an aggregation stage.
    pub fn binds(&self) -> impl Iterator<Item = &(VarId, Expression)> {
        self.bind_list().iter()
    }

    /// The per-group `Extend`s as a slice (see [`Self::binds`]).
    pub fn bind_list(&self) -> &[(VarId, Expression)] {
        match self {
            Self::Implicit { binds, .. } | Self::Explicit { binds, .. } => binds,
        }
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
/// [`Grouping::first_ungrouped_read`]).
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
}

impl UngroupedRead {
    /// The user-facing message: which stage reads which variable, and what to
    /// do instead. Variables print as their ids: plan-time code has no
    /// variable names. [`Self::named_message`] names them.
    pub fn message(&self) -> String {
        self.message_with(|var| format!("{var:?}"))
    }

    /// [`Self::message`] with each variable named from `vars`, falling back to
    /// its id for one the registry does not hold.
    pub fn named_message(&self, vars: &VarRegistry) -> String {
        self.message_with(|var| {
            vars.try_name(var)
                .map_or_else(|| format!("{var:?}"), str::to_string)
        })
    }

    fn message_with(&self, name: impl Fn(VarId) -> String) -> String {
        let var = name(self.var);
        let neither = "neither a GROUP BY key nor an aggregate result";
        match self.stage {
            ReadStage::Having => format!("HAVING reads variable {var}, which is {neither}"),
            ReadStage::Bind(out) => format!(
                "the SELECT expression for {} reads variable {var}, which is {neither}",
                name(out)
            ),
            ReadStage::OrderBind(_) => {
                format!("an ORDER BY expression reads variable {var}, which is {neither}")
            }
            ReadStage::OrderBy => format!("ORDER BY variable {var} is {neither}"),
            ReadStage::Projection => format!(
                "projected variable {var} is {neither}; aggregate it (e.g. with SAMPLE, \
                 collect or group-concat)"
            ),
        }
    }
}

/// In a grouping level, give every HAVING / ORDER BY read of a non-key
/// variable its SPARQL 1.1 meaning, `SAMPLE(?v)` (§18.2.4.1: "For each … HAVING(X),
/// and each ORDER BY X … For each unaggregated variable V in X / Replace V with
/// Sample(V)").
///
/// A read of `?v` is rewritten when the level's pre-group pipeline binds `?v`
/// (`where_vars`) and `?v` is not a group key. Everything else is left alone:
/// a key (`SAMPLE(key)` is the key), an aggregate output, a per-group `Extend`
/// output (HAVING reads it unbound, §18.2.4.2; ORDER BY reads the Extend), and a
/// variable nothing binds (unbound either way). The rewrite reuses a `SAMPLE(?v)`
/// the level already computes, and otherwise adds one whose output `mint`
/// names. It renames with [`Expression::substitute_var`], which also reaches
/// `EXISTS` patterns. After it, every such read is a read of an aggregate
/// output, so running it again changes nothing.
///
/// Which value SAMPLE picks is implementation-defined.
pub fn sample_ungrouped_reads(
    keys: &[VarId],
    aggregates: &mut Vec<AggregateSpec>,
    having: Option<&mut Expression>,
    order_binds: &mut [(VarId, Expression)],
    ordering: &mut [crate::sort::SortSpec],
    where_vars: &HashSet<VarId>,
    mint: &mut dyn FnMut(VarId) -> VarId,
) {
    let ungrouped = |v: &VarId| where_vars.contains(v) && !keys.contains(v);
    let mut reads: Vec<VarId> = Vec::new();
    let mut note = |v: VarId| {
        if ungrouped(&v) && !reads.contains(&v) {
            reads.push(v);
        }
    };
    if let Some(having) = having.as_deref() {
        having.referenced_vars().into_iter().for_each(&mut note);
    }
    for (_, expr) in order_binds.iter() {
        expr.referenced_vars().into_iter().for_each(&mut note);
    }
    for spec in ordering.iter() {
        note(spec.var);
    }
    if reads.is_empty() {
        return;
    }

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
            having.substitute_var(v, sampled);
        }
        for (_, expr) in order_binds.iter_mut() {
            expr.substitute_var(v, sampled);
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
    let mut read: Vec<VarId> = having
        .referenced_vars()
        .into_iter()
        .filter(|v| select_aliases.contains(v))
        .collect();
    read.sort_unstable();
    read.dedup();
    for alias in read {
        let unbound = mint(alias);
        having.substitute_var(alias, unbound);
    }
    super::pattern::Pattern::Filter(having)
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

/// Places the SELECT expressions of one query level, in SELECT order. The one
/// placement rule for every surface (SPARQL and JSON-LD share it).
///
/// In a level that does not group, every expression is a WHERE bind. In a
/// grouping level an expression runs once per group, after HAVING, unless one
/// of these keeps it before grouping:
///
/// - its alias is a group key (the key has to exist before grouping: the
///   `GROUP BY (LCASE(?a))` + `(LCASE(?a) AS ?k)` shortcut, a JSON-LD
///   `groupBy` naming a computed alias);
/// - an aggregate of the same level reads the alias (JSON-LD
///   `(as (str ?a) ?s)` + `(count ?s)` counts per-solution values; SPARQL
///   rejects the shape before lowering);
/// - it reads a variable the pre-group pipeline binds that is not a group key,
///   and no aggregate output or earlier per-group alias. That is JSON-LD's
///   documented per-group list (`(as (str ?e) ?es)` under `groupBy ?a`).
///   SPARQL's validator rejects the shape.
///
/// An expression that reads an aggregate output or an earlier per-group alias
/// always runs per group. A variable nothing binds before grouping (a typo, or
/// a variable internal to an `EXISTS`) is unbound either way, so it does not
/// hold an expression back.
#[derive(Debug)]
pub struct SelectExprPlacer {
    grouped: bool,
    keys: HashSet<VarId>,
    aggregate_outputs: HashSet<VarId>,
    aggregate_inputs: HashSet<VarId>,
    where_vars: HashSet<VarId>,
    post_aliases: HashSet<VarId>,
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
            post_aliases: HashSet::new(),
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
            post_aliases: HashSet::new(),
        }
    }

    /// Place `(expr AS ?alias)`, the next expression in SELECT order, and
    /// record the decision for the expressions after it.
    pub fn place(&mut self, alias: VarId, expr: &Expression) -> SelectExprPlacement {
        let placement = self.placement(alias, expr);
        match placement {
            SelectExprPlacement::PostGroup => {
                self.post_aliases.insert(alias);
            }
            SelectExprPlacement::PreGroup => {
                self.where_vars.insert(alias);
            }
        }
        placement
    }

    /// Record an expression that runs per group regardless of the rule: an
    /// expression containing an aggregate.
    pub fn record_post_group(&mut self, alias: VarId) {
        self.post_aliases.insert(alias);
    }

    fn placement(&self, alias: VarId, expr: &Expression) -> SelectExprPlacement {
        if !self.grouped {
            return SelectExprPlacement::PreGroup;
        }
        let refs = expr.referenced_vars();
        if refs
            .iter()
            .any(|v| self.aggregate_outputs.contains(v) || self.post_aliases.contains(v))
        {
            return SelectExprPlacement::PostGroup;
        }
        if self.keys.contains(&alias) || self.aggregate_inputs.contains(&alias) {
            return SelectExprPlacement::PreGroup;
        }
        let reads_ungrouped = refs
            .iter()
            .any(|v| self.where_vars.contains(v) && !self.keys.contains(v));
        if reads_ungrouped {
            SelectExprPlacement::PreGroup
        } else {
            SelectExprPlacement::PostGroup
        }
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
            &where_vars,
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
        let refs = having.referenced_vars();
        assert!(!refs.contains(&e), "?e renamed, EXISTS body included");
        assert!(refs.contains(&VarId(101)));
        for kept in [a, n, x, nosuch] {
            assert!(refs.contains(&kept), "{kept:?} left alone");
        }
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
            &where_vars,
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
            &[a, e].into_iter().collect(),
            &mut |_| panic!("an existing SAMPLE(?e) is reused"),
        );
        assert_eq!(aggregates.len(), 1);
        assert_eq!(ordering[0].var, s);
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
        let where_vars: HashSet<VarId> = [k, w].into_iter().collect();
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
            // HAVING runs before the binds, so it reads ?b as unbound: not a
            // grouped read.
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
        let where_vars: HashSet<VarId> = [a, e].into_iter().collect();
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
            &where_vars,
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
        let mut placer = SelectExprPlacer::grouped([a], [&count_e], where_vars.clone());

        // Key-only and constant expressions run once per group.
        assert_eq!(placer.place(VarId(10), &var(a)), PostGroup);
        assert_eq!(
            placer.place(VarId(11), &Expression::Const(FlakeValue::Long(1))),
            PostGroup
        );
        // A variable nothing binds before grouping does not hold it back.
        assert_eq!(placer.place(VarId(12), &var(nosuch)), PostGroup);
        // An aggregate output or an earlier per-group alias: per group.
        assert_eq!(placer.place(VarId(13), &var(n)), PostGroup);
        assert_eq!(placer.place(VarId(14), &var(VarId(10))), PostGroup);
        // A non-key WHERE variable: before grouping (JSON-LD's per-group list).
        assert_eq!(placer.place(VarId(15), &var(e)), PreGroup);
        // ...and so does an expression over that pre-group alias.
        assert_eq!(placer.place(VarId(16), &var(VarId(15))), PreGroup);
        // Reading an aggregate output wins over a non-key read: per group, where
        // the plan-time check rejects the non-key read.
        assert_eq!(
            placer.place(VarId(17), &Expression::add(var(n), var(e))),
            PostGroup
        );

        // The alias is a key: it has to exist before grouping.
        let mut placer = SelectExprPlacer::grouped([a, VarId(20)], [&count_e], where_vars.clone());
        assert_eq!(placer.place(VarId(20), &var(a)), PreGroup);

        // The alias is an aggregate input: before grouping.
        let count_alias = AggregateSpec {
            function: AggregateFn::Count(VarId(21)),
            output_var: VarId(22),
        };
        let mut placer = SelectExprPlacer::grouped([a], [&count_alias], where_vars);
        assert_eq!(placer.place(VarId(21), &var(a)), PreGroup);

        // A level that does not group: always a WHERE bind.
        let mut placer = SelectExprPlacer::ungrouped();
        assert_eq!(placer.place(VarId(30), &var(n)), PreGroup);
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
