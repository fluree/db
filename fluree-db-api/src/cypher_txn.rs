//! Interactive (multi-statement) Cypher transactions — the engine behind
//! Bolt explicit transactions (`BEGIN`/`COMMIT`/`ROLLBACK`).
//!
//! ## Model: optimistic, one commit per statement, atomic publish
//!
//! `BEGIN` pins the transaction's **base**: the ledger's cached head state.
//! Each write statement lowers, stages, and *builds a real commit* against
//! the transaction's evolving private state (`build_commit` is pure — no
//! blob is written, nothing publishes), then advances that private state
//! via `StagedCommit::finalize_state`. Reads inside the transaction query
//! the private state, so read-your-writes holds. Statement-level errors
//! surface at `RUN` time, like Neo4j's.
//!
//! `COMMIT` takes the ledger write lock and verifies the head is still
//! exactly the base (`t` + head commit id). If it moved, the whole
//! transaction fails with [`TransactError::CommitConflict`] — callers map
//! this to a driver-retryable transient error, matching the managed-
//! transaction contract (transaction functions are retryable closures).
//! If the base holds, the pending commit blobs are written (idempotent,
//! content-addressed) and the head ref advances **once**, from the base
//! to the final commit — intermediate states are never observable, and
//! the chain's parent pointers keep replay/history exact.
//!
//! This is *stronger* than Neo4j's read-committed isolation: a committed
//! transaction is serializable against the base it read. The cost is
//! optimistic-conflict retries under write contention.
//!
//! ## Governance
//!
//! `BEGIN` carries the session's [`GovernanceOptions`]; each write
//! statement builds the transaction's policy context against the private
//! state (same `build_transact_policy_context` choke point the consensus
//! committer uses) and stages under it, so `f:modify` is enforced at
//! staging time — since `COMMIT` re-verifies the base is unchanged,
//! staging-time enforcement is the enforcement. Conditional `MERGE`
//! probes are policy-wrapped like the autocommit path, and the identity
//! lands in each pending commit's `f:identity` for provenance.
//!
//! Scope notes: local-commit deployments only — Raft/peer modes must
//! reject `BEGIN` at the transport layer.

use crate::cypher_write::{self, WritePlan};
use crate::error::ApiError;
use crate::view::GraphDb;
use crate::{Fluree, GovernanceOptions, Result, Tracker};
use fluree_db_core::ContentId;
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{CasResult, RefKind, RefValue};
use fluree_db_transact::{CommitOpts, TransactError};

/// An open interactive Cypher transaction. Create with
/// [`Fluree::begin_cypher_transaction`]; drop to roll back.
pub struct CypherTransaction {
    ledger_id: String,
    /// `t` of the pinned base — the commit-time precondition.
    base_t: i64,
    /// Head ref of the pinned base (`None` on an empty ledger) — the
    /// `expected` side of the publish CAS.
    base_head: Option<RefValue>,
    /// The transaction's private state: base + every staged statement.
    state: LedgerState,
    /// Built-but-unpublished commits, in statement order.
    pending: Vec<PendingCommit>,
    /// Session identity + policy inputs pinned at BEGIN; governs every
    /// statement staged in this transaction.
    governance: GovernanceOptions,
}

struct PendingCommit {
    commit_id: ContentId,
    bytes: Vec<u8>,
    t: i64,
}

/// Per-statement outcome of a write inside a transaction.
pub struct CypherTxnWriteOutcome {
    /// Flakes staged by this statement (0 for a no-effect statement).
    pub flake_count: usize,
    /// Rows for a trailing `RETURN` on the write: hydrated typed cells
    /// (created entities as nodes), same shape reads produce.
    pub return_table: Option<(
        Vec<String>,
        Vec<Vec<crate::format::cypher_typed::CypherCell>>,
    )>,
}

impl CypherTransaction {
    pub fn ledger_id(&self) -> &str {
        &self.ledger_id
    }

    /// `t` of the pinned base state.
    pub fn base_t(&self) -> i64 {
        self.base_t
    }

    /// Whether any statement staged changes (an all-reads transaction
    /// commits without touching consensus).
    pub fn has_writes(&self) -> bool {
        !self.pending.is_empty()
    }
}

impl Fluree {
    /// Open an interactive Cypher transaction pinned to the ledger's
    /// current cached head. `governance` (the session's authenticated
    /// identity and policy inputs) governs every statement staged in it.
    pub async fn begin_cypher_transaction(
        &self,
        ledger_id: &str,
        governance: GovernanceOptions,
    ) -> Result<CypherTransaction> {
        let handle = self.ledger_cached(ledger_id).await?;
        let state = handle.snapshot().await.to_ledger_state();
        let base_head = state.head_commit_id.clone().map(|cid| RefValue {
            id: Some(cid),
            t: state.t(),
        });
        Ok(CypherTransaction {
            ledger_id: ledger_id.to_string(),
            base_t: state.t(),
            base_head,
            state,
            pending: Vec::new(),
            governance,
        })
    }

    /// A queryable view of the transaction's private state (base + staged
    /// statements), with the ledger's default context attached so Cypher
    /// vocab resolution matches the autocommit read path.
    pub async fn cypher_transaction_view(&self, txn: &CypherTransaction) -> Result<GraphDb> {
        let view = GraphDb::from_ledger_state(&txn.state);
        self.resolve_and_attach_config(view).await
    }

    /// Execute one write statement inside the transaction: lower against
    /// the private state (conditional `MERGE` probes run against it too,
    /// so branch choice sees earlier statements), stage, build the commit,
    /// and advance the private state. Nothing publishes until
    /// [`Self::commit_cypher_transaction`].
    pub async fn cypher_transaction_write(
        &self,
        txn: &mut CypherTransaction,
        query: &str,
        params: Option<&crate::CypherParamMap>,
    ) -> Result<CypherTxnWriteOutcome> {
        let return_plan = cypher_write::plan_write_return_source(query, params)?;
        let skolem_txn_id = return_plan
            .as_ref()
            .map(|_| cypher_write::fresh_skolem_txn_id());

        let plan = self
            .cypher_write_plan_with_skolem(
                query,
                params,
                &txn.ledger_id,
                &txn.state.snapshot,
                skolem_txn_id.clone(),
            )
            .await?;
        let resolved = match plan {
            WritePlan::Single(t) => cypher_write::ResolvedConditional::single(*t),
            WritePlan::Conditional(cw) => {
                // Policy-wrap the branch-choosing probe (mirrors the
                // consensus committer's `resolve_cypher_under_lock`): a
                // restricted writer's MERGE picks its branch from
                // policy-visible data only.
                let probe = GraphDb::from_ledger_state(&txn.state);
                let probe = if txn.governance.has_any_policy_inputs() {
                    self.wrap_policy(probe, &txn.governance, None).await?
                } else {
                    probe
                };
                self.resolve_conditional_cypher(&cw, probe, &txn.ledger_id, &txn.state.snapshot)
                    .await?
            }
        };

        // Built against the private state so earlier statements in this
        // transaction are visible to policy evaluation. Staging-time
        // enforcement is the enforcement: COMMIT only re-verifies that the
        // base is unchanged.
        let policy = crate::build_transact_policy_context(
            self,
            &txn.state.snapshot,
            txn.state.novelty.as_ref(),
            Some(txn.state.novelty.as_ref()),
            txn.state.t(),
            &txn.governance,
        )
        .await?;

        let index_config = crate::server_defaults::default_index_config();
        let tracker = Tracker::disabled();
        let stage_result = if let Some(followup) = resolved.followup {
            // Per-row relationship MERGE … ON MATCH SET: stage both branches into
            // this statement's single pending commit.
            self.stage_pair_from_txns(
                txn.state.clone(),
                resolved.primary,
                followup,
                Some(&index_config),
                policy.as_ref(),
                Some(&tracker),
            )
            .await?
        } else {
            self.stage_transaction_from_txn(
                txn.state.clone(),
                resolved.primary,
                Some(&index_config),
                policy.as_ref(),
                Some(&tracker),
            )
            .await?
        };
        let crate::StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = stage_result;

        // Zero-effect statement (`MATCH … SET` matching nothing): Cypher
        // semantics succeed with zero updates; nothing to commit.
        if !view.has_staged() {
            let (base, _flakes) = view.into_parts();
            txn.state = base;
            let return_table = match (&return_plan, &skolem_txn_id) {
                (Some(plan), Some(skolem)) => {
                    Some(cypher_write::write_return_typed_rows(plan, skolem, &txn.state).await?)
                }
                _ => None,
            };
            return Ok(CypherTxnWriteOutcome {
                flake_count: 0,
                return_table,
            });
        }

        let mut commit_opts = CommitOpts::default()
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());
        if let Some(identity) = &txn.governance.identity {
            commit_opts = commit_opts.identity(identity.clone());
        }

        // Head temporal metadata is needed by build_commit's monotonicity
        // guard; resolve it lazily exactly like the autocommit path.
        let mut view = view;
        if view.base().head_temporal.is_none() && view.base().head_commit_id.is_some() {
            let store = self
                .content_store_for_record_or_id(view.base().ns_record.as_ref(), &txn.ledger_id)
                .await?;
            view.base_mut()
                .ensure_head_temporal(store.as_ref())
                .await
                .map_err(TransactError::from)?;
        }

        let expected_head_ref = view.base().head_commit_id.as_ref().map(|cid| RefValue {
            id: Some(cid.clone()),
            t: view.base().t(),
        });
        let staged = fluree_db_transact::build_commit(
            view,
            ns_registry,
            expected_head_ref,
            None,
            &index_config,
            commit_opts,
        )
        .await
        .map_err(ApiError::from)?;

        let commit_id = staged
            .commit
            .id
            .clone()
            .expect("build_commit sets commit.id");
        let bytes = staged.commit_bytes.clone();
        let (receipt, next_state) = staged.finalize_state().map_err(ApiError::from)?;

        txn.pending.push(PendingCommit {
            commit_id,
            bytes,
            t: receipt.t,
        });
        txn.state = next_state;

        let return_table = match (&return_plan, &skolem_txn_id) {
            (Some(plan), Some(skolem)) => {
                Some(cypher_write::write_return_typed_rows(plan, skolem, &txn.state).await?)
            }
            _ => None,
        };
        Ok(CypherTxnWriteOutcome {
            flake_count: receipt.flake_count,
            return_table,
        })
    }

    /// Publish the transaction atomically. Verifies under the ledger write
    /// lock that the head is still the pinned base; a moved head fails the
    /// whole transaction with [`TransactError::CommitConflict`] (map it to
    /// a driver-retryable transient error). On success all pending commit
    /// blobs are written and the head ref advances once, base → final
    /// commit. Returns the final committed `t` (the base `t` for an
    /// all-reads transaction).
    pub async fn commit_cypher_transaction(&self, txn: CypherTransaction) -> Result<i64> {
        if txn.pending.is_empty() {
            return Ok(txn.base_t);
        }
        let handle = self.ledger_cached(&txn.ledger_id).await?;
        let guard = handle.lock_for_write().await;

        let head = guard.state();
        let base_head_id = txn.base_head.as_ref().and_then(|r| r.id.clone());
        if head.t() != txn.base_t || head.head_commit_id != base_head_id {
            return Err(ApiError::Transact(TransactError::CommitConflict {
                expected_t: txn.base_t,
                head_t: head.t(),
            }));
        }

        let content_store = self.content_store(&txn.ledger_id);
        for pending in &txn.pending {
            content_store
                .put_with_id(&pending.commit_id, &pending.bytes)
                .await
                .map_err(|e| {
                    ApiError::internal(format!("transaction commit blob write failed: {e}"))
                })?;
        }

        let last = txn.pending.last().expect("pending is non-empty");
        let new_head = RefValue {
            id: Some(last.commit_id.clone()),
            t: last.t,
        };
        let publisher = self.publisher()?;
        let cas = publisher
            .compare_and_set_ref(
                &txn.ledger_id,
                RefKind::CommitHead,
                txn.base_head.as_ref(),
                &new_head,
            )
            .await
            .map_err(|e| ApiError::internal(format!("transaction head publish failed: {e}")))?;
        match cas {
            CasResult::Updated => {}
            CasResult::Conflict { actual } => {
                // The lock serializes in-process writers, so a CAS conflict
                // means an external writer advanced the durable head.
                return Err(ApiError::Transact(TransactError::CommitConflict {
                    expected_t: txn.base_t,
                    head_t: actual.map(|r| r.t).unwrap_or(0),
                }));
            }
        }

        let index_config = crate::server_defaults::default_index_config();
        let needs_reindex = txn.state.should_reindex(&index_config);
        let final_t = last.t;
        self.finalize_commit(guard, txn.state, final_t, needs_reindex)
            .await?;
        Ok(final_t)
    }
}
