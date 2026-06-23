//! Fast-path operators for "stats queries".
//!
//! These operators answer certain aggregate queries directly from `IndexStats`
//! / `StatsView` without scanning the triple store.

use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{FlakeValue, StatsView};
use std::sync::Arc;

/// Emit per-predicate counts using `StatsView` (no triple scan).
///
/// Intended to fast-path queries like:
/// `SELECT ?p (COUNT(?s) AS ?count) WHERE { ?s ?p ?o } GROUP BY ?p ORDER BY DESC(?count)`
///
/// The `StatsView` counts are whole-index totals with no per-flake policy
/// filtering, so under a non-root view policy they would leak restricted
/// predicates' existence and cardinality. When a policy is active this operator
/// delegates to `fallback` — the equivalent generic GROUP BY pipeline, whose
/// scan applies the per-flake filter — instead of emitting stats rows.
pub struct StatsCountByPredicateOperator {
    stats: Arc<StatsView>,
    schema: Arc<[VarId]>,
    state: OperatorState,
    rows: Vec<(Binding, Binding)>,
    pos: usize,
    /// Policy-enforced generic GROUP BY producing the same `[pred, count]`
    /// schema. Used when a view policy is active (the stats counts can't be
    /// trusted then). Built unconditionally so the operator tree stays
    /// policy-agnostic (it is prepared once and reused across policies).
    fallback: Option<BoxedOperator>,
    /// Set in `open()` when a policy is active: subsequent `next_batch` calls
    /// stream from `fallback` rather than the stats rows.
    use_fallback: bool,
}

impl StatsCountByPredicateOperator {
    pub fn new(
        stats: Arc<StatsView>,
        pred_var: VarId,
        count_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        let schema: Arc<[VarId]> = Arc::from(vec![pred_var, count_var].into_boxed_slice());
        Self {
            stats,
            schema,
            state: OperatorState::Created,
            rows: Vec::new(),
            pos: 0,
            fallback,
            use_fallback: false,
        }
    }

    /// True when a non-root view policy is active on the scope this operator
    /// runs against (single-ledger or any graph of a dataset). The stats counts
    /// are only valid when no such policy can hide flakes.
    fn policy_active(ctx: &ExecutionContext<'_>) -> bool {
        match ctx.active_graphs() {
            ActiveGraphs::Single => ctx.has_policy(),
            ActiveGraphs::Many(graphs) => graphs.iter().any(|g| g.has_policy()),
        }
    }

    fn build_rows(&self, ctx: &ExecutionContext<'_>) -> Result<Vec<(Binding, Binding)>> {
        let dt = WellKnownDatatypes::new().xsd_long;
        let store = ctx.binary_store.as_deref();

        // Prefer graph-scoped stats if present (and we can resolve p_id → Sid).
        if let Some(props) = self.stats.get_graph_properties(ctx.binary_g_id) {
            let mut out = Vec::with_capacity(props.len());
            for (&p_id, data) in props {
                let pred_sid = ctx
                    .runtime_small_dicts
                    .and_then(|dicts| dicts.predicate_sid(p_id))
                    .cloned()
                    .or_else(|| {
                        // Safe only for persisted-range IDs: runtime-only predicate IDs are
                        // resolved above through `runtime_small_dicts`, so reaching this
                        // fallback implies `p_id` can be interpreted in persisted store space.
                        store
                            .and_then(|store| store.resolve_predicate_iri(p_id.as_u32()))
                            .map(|iri| store.expect("store already used above").encode_iri(iri))
                    });
                let Some(pred_sid) = pred_sid else {
                    continue;
                };
                let pred = Binding::sid(pred_sid);
                let count = Binding::lit(FlakeValue::Long(data.count as i64), dt.clone());
                out.push((pred, count));
            }
            return Ok(out);
        }

        // Fallback: aggregate SID-keyed stats (across graphs).
        if !self.stats.properties.is_empty() {
            let mut out = Vec::with_capacity(self.stats.properties.len());
            for (sid, data) in &self.stats.properties {
                let pred = Binding::sid(sid.clone());
                let count = Binding::lit(FlakeValue::Long(data.count as i64), dt.clone());
                out.push((pred, count));
            }
            return Ok(out);
        }

        Err(QueryError::InvalidQuery(
            "stats query fast-path requires IndexStats/StatsView".to_string(),
        ))
    }
}

#[async_trait]
impl Operator for StatsCountByPredicateOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            return Err(QueryError::OperatorAlreadyOpened);
        }
        // Under a view policy the whole-index stats counts can't be trusted, so
        // stream from the policy-enforced generic fallback instead. If a policy
        // is active but no fallback was wired, fail closed rather than leak.
        if Self::policy_active(ctx) {
            let fallback = self.fallback.as_mut().ok_or_else(|| {
                QueryError::Policy(
                    "stats count-by-predicate fast path has no policy-enforced fallback"
                        .to_string(),
                )
            })?;
            fallback.open(ctx).await?;
            self.use_fallback = true;
        } else {
            self.rows = self.build_rows(ctx)?;
            self.pos = 0;
        }
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        if self.use_fallback {
            return self
                .fallback
                .as_mut()
                .expect("use_fallback implies fallback is Some")
                .next_batch(ctx)
                .await;
        }
        if self.pos >= self.rows.len() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        let end = (self.pos + ctx.batch_size).min(self.rows.len());
        let slice = &self.rows[self.pos..end];
        self.pos = end;

        let mut pred_col: Vec<Binding> = Vec::with_capacity(slice.len());
        let mut count_col: Vec<Binding> = Vec::with_capacity(slice.len());
        for (p, c) in slice {
            pred_col.push(p.clone());
            count_col.push(c.clone());
        }

        Ok(Some(Batch::new(
            self.schema.clone(),
            vec![pred_col, count_col],
        )?))
    }

    fn close(&mut self) {
        if let Some(fallback) = self.fallback.as_mut() {
            fallback.close();
        }
        self.use_fallback = false;
        self.state = OperatorState::Closed;
        self.rows.clear();
        self.pos = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.rows.len())
    }
}
