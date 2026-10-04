//! Transactions built one operation at a time and committed as one commit.
//!
//! Each [`Transaction::stage`] applies its operation over the ones before it
//! — validated by the full staging pipeline (policy, SHACL, uniqueness) as it
//! is added — and [`Transaction::db`] reads the result without committing.
//! [`Transaction::commit`] writes every staged operation as a single commit,
//! the operations netted in order exactly as a `;`-separated SPARQL UPDATE
//! nets its parts.
//!
//! The operations stage against the ledger's state when the transaction
//! began. If another commit lands before this one, the commit re-bases the
//! staged result over it when the two touched different subjects, and
//! otherwise stages every operation again against the new head, as a single
//! write does when it loses a race.

use crate::ledger_manager::RefreshOpts;
use crate::tx::{SequentialStager, StageResult, TransactResultRef};
use crate::tx_builder::{is_retryable_commit_conflict, OpPlan, TransactOperation};
use crate::{ApiError, Fluree, GraphDb, PolicyContext, Result};
use fluree_db_core::ContentId;
use fluree_db_ledger::{IndexConfig, LedgerState};
use fluree_db_transact::{CommitOpts, TxnOpts, TxnType};
use serde_json::Value as JsonValue;

/// One write in a [`Transaction`].
#[derive(Clone, Debug)]
pub enum TxnOperation {
    /// JSON-LD insert.
    Insert(JsonValue),
    /// JSON-LD upsert.
    Upsert(JsonValue),
    /// JSON-LD `where` / `delete` / `insert` update.
    Update(JsonValue),
    /// Turtle or TriG insert.
    InsertTurtle(String),
    /// Turtle or TriG upsert.
    UpsertTurtle(String),
    /// SPARQL UPDATE, which may itself hold several `;`-separated operations.
    SparqlUpdate(String),
}

impl TxnOperation {
    fn plan(&self) -> Result<OpPlan<'_>> {
        Ok(match self {
            Self::Insert(json) => OpPlan::from_op(TransactOperation::InsertJson(json))?,
            Self::Upsert(json) => OpPlan::from_op(TransactOperation::UpsertJson(json))?,
            Self::Update(json) => OpPlan::from_op(TransactOperation::UpdateJson(json))?,
            Self::InsertTurtle(turtle) => OpPlan::from_op(TransactOperation::InsertTurtle(turtle))?,
            Self::UpsertTurtle(turtle) => OpPlan::from_op(TransactOperation::UpsertTurtle(turtle))?,
            Self::SparqlUpdate(sparql) => OpPlan::Sparql(sparql),
        })
    }
}

/// A transaction open on one ledger: staged operations, readable through
/// [`Self::db`], committed together by [`Self::commit`].
///
/// Start one with [`Fluree::begin_transaction`]. Dropping it discards the
/// staged operations.
pub struct Transaction {
    fluree: Fluree,
    ledger_id: String,
    base: LedgerState,
    base_head: Option<ContentId>,
    stager: SequentialStager,
    operations: Vec<TxnOperation>,
    policy: Option<PolicyContext>,
    index_config: IndexConfig,
}

impl Fluree {
    /// Open a transaction on `ledger_id` at its current head. Operations
    /// staged on it are checked against `policy` when one is given.
    pub async fn begin_transaction(
        &self,
        ledger_id: &str,
        policy: Option<PolicyContext>,
    ) -> Result<Transaction> {
        let handle = self.ledger_cached(ledger_id).await?;
        let snapshot = handle.snapshot().await;
        let base_head = snapshot.head_commit_id.clone();
        let base = snapshot.to_ledger_state();
        Ok(Transaction {
            fluree: self.clone(),
            ledger_id: handle.id().to_string(),
            stager: SequentialStager::new(base.clone()),
            base,
            base_head,
            operations: Vec::new(),
            policy,
            index_config: crate::server_defaults::default_index_config(),
        })
    }
}

impl Transaction {
    pub fn ledger_id(&self) -> &str {
        &self.ledger_id
    }

    /// The `t` of the state the transaction began on.
    pub fn base_t(&self) -> i64 {
        self.base.t()
    }

    /// The operations staged so far.
    pub fn operations(&self) -> &[TxnOperation] {
        &self.operations
    }

    /// Apply `operation` over those already staged. An operation that fails
    /// to stage — a parse error, a policy denial, a SHACL violation — is
    /// not added, and the transaction stays as it was.
    pub async fn stage(&mut self, operation: TxnOperation) -> Result<()> {
        let staged = stage_operation(
            &self.fluree,
            &mut self.stager,
            &operation,
            self.policy.as_ref(),
            &self.index_config,
        )
        .await;
        match staged {
            Ok(()) => {
                self.operations.push(operation);
                Ok(())
            }
            Err(e) => {
                // A failed stage consumes the stager's state; rebuild it from
                // the operations that did stage.
                self.stager = stage_all(
                    &self.fluree,
                    self.base.clone(),
                    &self.operations,
                    self.policy.as_ref(),
                    &self.index_config,
                )
                .await?;
                Err(e)
            }
        }
    }

    /// The ledger as the staged operations leave it, for queries. Apply
    /// policy to it as to any other view.
    pub async fn db(&self) -> Result<GraphDb> {
        let view = GraphDb::from_ledger_state(self.stager.state());
        self.fluree.resolve_and_attach_config(view).await
    }

    /// Commit every staged operation as one commit. Commits nothing — and
    /// returns a no-op receipt at the current `t` — when they net to no
    /// change.
    pub async fn commit(self, commit_opts: CommitOpts) -> Result<TransactResultRef> {
        let Transaction {
            fluree,
            ledger_id,
            base,
            base_head,
            stager,
            operations,
            policy,
            index_config,
        } = self;
        let handle = fluree.ledger_cached(&ledger_id).await?;
        let base_t = base.t();
        let mut prestaged = Some(stager.finish().await?);

        const MAX_RETRIES: usize = 16;
        for attempt in 0..MAX_RETRIES {
            let guard = handle.lock_for_write().await;
            let unchanged = guard.state().t() == base_t
                && guard.state().head_commit_id.as_ref() == base_head.as_ref();
            let stage = match prestaged.take() {
                Some(stage) if unchanged => stage,
                Some(stage) => {
                    match Fluree::rebase_stage(&guard, stage, base_t, base_head.as_ref()) {
                        Some(rebased) => rebased,
                        None => {
                            restage(
                                &fluree,
                                guard.clone_state(),
                                &operations,
                                &policy,
                                &index_config,
                            )
                            .await?
                        }
                    }
                }
                None => {
                    restage(
                        &fluree,
                        guard.clone_state(),
                        &operations,
                        &policy,
                        &index_config,
                    )
                    .await?
                }
            };
            match fluree
                .commit_and_finalize(
                    guard,
                    stage,
                    TxnType::Update,
                    commit_opts.clone(),
                    &index_config,
                    None,
                    None,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) if attempt + 1 < MAX_RETRIES && is_retryable_commit_conflict(&e) => {
                    // The durable head moved under the cache (another
                    // process); heal the cache and stage again.
                    if let Err(refresh_err) =
                        fluree.refresh(&ledger_id, RefreshOpts::default()).await
                    {
                        tracing::warn!(error = %refresh_err, "refresh after transaction commit conflict failed");
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(ApiError::internal(format!(
            "transaction commit retry limit exceeded ({MAX_RETRIES} attempts)"
        )))
    }
}

async fn stage_operation(
    fluree: &Fluree,
    stager: &mut SequentialStager,
    operation: &TxnOperation,
    policy: Option<&PolicyContext>,
    index_config: &IndexConfig,
) -> Result<()> {
    let plan = operation.plan()?;
    stager
        .stage_with(true, |state| async move {
            let (stage, _, _) = fluree
                .stage_plan(
                    &plan,
                    state,
                    TxnOpts::default(),
                    &CommitOpts::default(),
                    None,
                    index_config,
                    policy,
                )
                .await?;
            Ok(stage)
        })
        .await
        .map(drop)
}

async fn stage_all(
    fluree: &Fluree,
    base: LedgerState,
    operations: &[TxnOperation],
    policy: Option<&PolicyContext>,
    index_config: &IndexConfig,
) -> Result<SequentialStager> {
    let mut stager = SequentialStager::new(base);
    for operation in operations {
        stage_operation(fluree, &mut stager, operation, policy, index_config).await?;
    }
    Ok(stager)
}

async fn restage(
    fluree: &Fluree,
    state: LedgerState,
    operations: &[TxnOperation],
    policy: &Option<PolicyContext>,
    index_config: &IndexConfig,
) -> Result<StageResult> {
    stage_all(fluree, state, operations, policy.as_ref(), index_config)
        .await?
        .finish()
        .await
}
