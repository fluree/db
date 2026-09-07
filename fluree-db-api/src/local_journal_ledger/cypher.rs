//! Cypher lowering/probes stay inside the adapter gate; this module never applies
//! through the ordinary persistence backend or exposes an uncommitted view.
use super::*;
use crate::cypher_write::{ResolvedConditional, WritePlan};
use fluree_db_cypher::ParamMap;

impl JournalLedger {
    /// Execute exactly one Cypher write statement with the normal local Fluree
    /// lowering/conditional/sequential staging semantics, under root authority.
    /// Semicolon-separated scripts are rejected before effects. Multiple clauses
    /// within ONE statement remain one atomic commit.
    ///
    /// Provenance is serialized as {"cypher": original_text, "params": map};
    /// absent parameters become an empty map. No automatic retry/deduplication.
    pub async fn transact_cypher(
        &self,
        cypher: &str,
        params: Option<&ParamMap>,
    ) -> Result<CypherOutcome> {
        self.transact_cypher_with_install_hook(cypher, params, || Ok(()))
            .await
    }

    // Shares the JSON adapter's deterministic post-flush test seam.
    async fn transact_cypher_with_install_hook(
        &self,
        cypher: &str,
        params: Option<&ParamMap>,
        before_install: impl FnOnce() -> fluree_db_core::local_journal::Result<()> + Send + 'static,
    ) -> Result<CypherOutcome> {
        let statements = crate::cypher_import::split_statements(cypher);
        if statements.len() != 1 {
            return Err(
                JournalError::Invalid("journal Cypher requires exactly one statement").into(),
            );
        }
        let started = std::time::Instant::now();
        let cache = self.ready().await?;
        let state = cache.state.as_ref().expect("ready state");
        let (staged, kind, result) = self.stage_cypher(state, &statements[0], params).await?;
        let staged_at = std::time::Instant::now();
        let raw = json!({"cypher": cypher, "params": params.cloned().unwrap_or_default()});
        let commit = self
            .accept_staged(cache, staged, kind, &raw, before_install)
            .await?;
        tracing::debug!(target: "fluree::journal_probe", t = commit.as_ref().map(|c| c.commit.t), stage_us = staged_at.duration_since(started).as_micros() as u64, accept_total_us = staged_at.elapsed().as_micros() as u64, "journal Cypher phases");
        Ok(CypherOutcome { commit, result })
    }

    async fn stage_cypher(
        &self,
        state: &LedgerState,
        cypher: &str,
        params: Option<&ParamMap>,
    ) -> Result<(StageResult, TxnType, Option<Value>)> {
        let engine = &self.0.engine;
        let config = index_config();
        let ast = crate::query::helpers::substituted_cypher_ast(cypher, params)?;
        if let Some(plan) = crate::cypher_seq::detect_sequential(&ast) {
            let outcome = engine
                .stage_cypher_sequential(
                    state.clone(),
                    &plan,
                    state.ledger_id(),
                    None,
                    Some(&config),
                    None,
                    None,
                    None,
                )
                .await?;
            let result = match outcome.return_result {
                Some(result) => {
                    let view = GraphDb::from_ledger_state(&outcome.final_state);
                    Some(
                        result
                            .to_cypher_json_async(view.as_graph_db_ref())
                            .await
                            .map_err(crate::ApiError::from)?,
                    )
                }
                None => None,
            };
            return Ok((outcome.stage_result, TxnType::Update, result));
        }
        let return_plan = crate::cypher_write::plan_write_return(&ast)
            .map_err(|e| crate::ApiError::cypher(e, Vec::new()))?;
        let skolem = return_plan
            .as_ref()
            .map(|_| fluree_db_transact::generate_txn_id());
        let plan = engine
            .cypher_write_plan_with_skolem(
                cypher,
                params,
                state.ledger_id(),
                &state.snapshot,
                skolem.clone(),
            )
            .await?;
        let resolved = match plan {
            WritePlan::Single(txn) => ResolvedConditional::single(*txn),
            WritePlan::Conditional(plan) => {
                engine
                    .resolve_conditional_cypher(
                        &plan,
                        GraphDb::from_ledger_state(state),
                        state.ledger_id(),
                        &state.snapshot,
                    )
                    .await?
            }
            WritePlan::Sequential(_) => {
                return Err(crate::ApiError::internal("inconsistent Cypher classification").into())
            }
        };
        let kind = resolved.primary.txn_type;
        let staged = match resolved.followup {
            Some(followup) => {
                engine
                    .stage_pair_from_txns(
                        state.clone(),
                        resolved.primary,
                        followup,
                        Some(&config),
                        None,
                        None,
                    )
                    .await?
            }
            None => {
                engine
                    .stage_transaction_from_txn(
                        state.clone(),
                        resolved.primary,
                        Some(&config),
                        None,
                        None,
                    )
                    .await?
            }
        };
        let result = match (return_plan, skolem) {
            (Some(plan), Some(id)) => {
                // The standard transient-state helper prepares RETURN before
                // append. This view never becomes accepted state or escapes.
                // Errors here are definite rejections, with no durable effects.
                let mut preview = staged.view.base().clone();
                preview
                    .apply_staged_flakes_for_sequential_staging(
                        staged.view.staged_flakes().to_vec(),
                        staged.ns_registry.delta(),
                        staged.graph_delta.values(),
                    )
                    .map_err(crate::ApiError::from)?;
                Some(crate::cypher_write::write_return_rows(&plan, &id, &preview).await?)
            }
            _ => None,
        };
        Ok((staged, kind, result))
    }

    /// Execute a Cypher read and format its JSON envelope while the query gate
    /// is held. Parameters follow the ordinary Fluree Cypher read path.
    pub async fn query_cypher(&self, cypher: &str, params: Option<&ParamMap>) -> Result<Value> {
        let cache = self.ready().await?;
        let state = cache.state.as_ref().expect("ready state");
        let view = GraphDb::from_ledger_state(state)
            .with_default_context(cache.proof.as_ref().and_then(|p| p.context.clone()));
        let result = self
            .0
            .engine
            .query_cypher_with_params(&view, cypher, params)
            .await?;
        let value = result
            .to_cypher_json_async(view.as_graph_db_ref())
            .await
            .map_err(crate::ApiError::from)?;
        self.accepted_head().await?;
        Ok(value)
    }
}

#[cfg(test)]
#[path = "cypher_tests.rs"]
mod tests;
