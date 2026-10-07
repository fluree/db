//! Transactions built one operation at a time and committed as one commit.
//!
//! Each [`Transaction::stage`] (or [`Transaction::stage_cypher`]) applies its
//! operation over the ones before it — validated by the full staging pipeline
//! (policy, SHACL, uniqueness) as it is added — and [`Transaction::db`] reads
//! the result without committing. [`Transaction::commit`] writes every staged
//! operation as a single commit, the operations netted in order exactly as a
//! `;`-separated SPARQL UPDATE nets its parts. Operations in different
//! languages — JSON-LD, Turtle, SPARQL UPDATE, Cypher — mix freely.
//!
//! The operations stage against the ledger's state when the transaction
//! began. If another commit lands before this one, what happens depends on
//! whether the transaction was read:
//!
//! - **Not read:** each operation decides its own effect (an update's
//!   `WHERE`, a Cypher `MATCH` or `MERGE`, is evaluated where it is staged),
//!   so the commit re-bases the staged result over the other commit when the
//!   two touched different subjects, and otherwise stages every operation
//!   again against the new head, under the policy the ledger has then, as a
//!   single write does when it loses a race.
//! - **Read through [`Transaction::db`], or through the `RETURN` rows of a
//!   Cypher write:** the caller may have decided what to write from what it
//!   read, and staging again would replay those decisions against data they
//!   never saw — a lost update. The commit fails with
//!   [`TransactError::CommitConflict`]; run the transaction again.

use crate::cypher_write::{self, ResolvedConditional, WritePlan};
use crate::format::cypher_typed::CypherCell;
use crate::ledger_manager::RefreshOpts;
use crate::tx::{SequentialStager, TransactResultRef};
use crate::tx_builder::{is_retryable_commit_conflict, OpPlan, TransactOperation};
use crate::{
    ApiError, CypherParamMap, Fluree, GovernanceOptions, GraphDb, PolicyContext, Result, Tracker,
    TrackingOptions,
};
use fluree_db_core::ContentId;
use fluree_db_ledger::{IndexConfig, LedgerState};
use fluree_db_transact::{CommitOpts, TransactError, TxnOpts, TxnType};
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicBool, Ordering};

/// One write in a [`Transaction`]. Cypher writes are staged with
/// [`Transaction::stage_cypher`].
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

/// A point in a [`Transaction`]'s staged operations; see
/// [`Transaction::savepoint`].
#[derive(Clone, Copy, Debug)]
pub struct Savepoint(usize);

/// The rows a Cypher write's `RETURN` produced: column names and typed cells.
pub type CypherReturn = (Vec<String>, Vec<Vec<CypherCell>>);

/// How a [`Transaction`] stages and commits.
#[derive(Clone, Debug, Default)]
pub struct TransactionOptions {
    /// The identity and policy inputs every operation is checked against;
    /// the default applies the ledger's configured policy defaults only.
    pub governance: GovernanceOptions,
    /// Fuel, time and policy accounting across the whole transaction,
    /// including any operations staged again; a fuel limit bounds its work.
    /// The tally is reported with the commit.
    pub tracking: TrackingOptions,
    /// The novelty limits the commit is held to; `None` uses the server
    /// defaults.
    pub index_config: Option<IndexConfig>,
}

/// A staged operation, as replayed when the transaction stages again.
enum Staged {
    Operation(TxnOperation),
    /// A Cypher write statement. The skolem id names the nodes and edges it
    /// creates, so staging it again creates the same identities.
    Cypher {
        query: String,
        params: Option<CypherParamMap>,
        skolem_txn_id: String,
    },
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
    operations: Vec<Staged>,
    context: StageContext,
    /// Whether the caller read the staged state: the commit then requires the
    /// head it began on.
    read: AtomicBool,
}

/// What every operation stages under.
struct StageContext {
    ledger_id: String,
    governance: GovernanceOptions,
    policy: Option<PolicyContext>,
    index_config: IndexConfig,
    tracker: Tracker,
}

impl Fluree {
    /// Open a transaction on `ledger_id` at its current head.
    pub async fn begin_transaction(
        &self,
        ledger_id: &str,
        options: TransactionOptions,
    ) -> Result<Transaction> {
        let TransactionOptions {
            governance,
            tracking,
            index_config,
        } = options;
        let handle = self.ledger_cached(ledger_id).await?;
        let snapshot = handle.snapshot().await;
        let base_head = snapshot.head_commit_id.clone();
        let base = snapshot.to_ledger_state();
        let policy = transact_policy(self, &base, &governance).await?;
        let ledger_id = handle.id().to_string();
        Ok(Transaction {
            fluree: self.clone(),
            stager: SequentialStager::new(base.clone()),
            base,
            base_head,
            operations: Vec::new(),
            context: StageContext {
                ledger_id: ledger_id.clone(),
                governance,
                policy,
                index_config: index_config
                    .unwrap_or_else(crate::server_defaults::default_index_config),
                tracker: Tracker::new(tracking),
            },
            ledger_id,
            read: AtomicBool::new(false),
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

    /// How many operations are staged.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Apply `operation` over those already staged. An operation that fails
    /// to stage — a parse error, a policy denial, a SHACL violation — is
    /// not added, and the transaction stays as it was.
    pub async fn stage(&mut self, operation: TxnOperation) -> Result<()> {
        self.push(Staged::Operation(operation)).await.map(drop)
    }

    /// Apply a Cypher write statement (`CREATE`, `MERGE`, `SET`, `DELETE`,
    /// multi-clause writes) over the operations already staged: its `MATCH`
    /// and `MERGE` see them. Returns the rows of its `RETURN`, if it has one;
    /// receiving them counts as reading the transaction (see the module
    /// docs). A statement that fails to stage is not added.
    pub async fn stage_cypher(
        &mut self,
        query: &str,
        params: Option<CypherParamMap>,
    ) -> Result<Option<CypherReturn>> {
        let staged = Staged::Cypher {
            query: query.to_string(),
            params,
            skolem_txn_id: cypher_write::fresh_skolem_txn_id(),
        };
        let rows = self.push(staged).await?;
        if rows.is_some() {
            self.read.store(true, Ordering::Relaxed);
        }
        Ok(rows)
    }

    /// The operations staged so far, to return to with [`Self::rollback_to`].
    pub fn savepoint(&self) -> Savepoint {
        Savepoint(self.operations.len())
    }

    /// Discard every operation staged since `savepoint` — for a group of
    /// operations, such as a `;` Cypher script, that must stage all or
    /// nothing. A read in the discarded group still counts as a read.
    pub async fn rollback_to(&mut self, savepoint: Savepoint) -> Result<()> {
        if savepoint.0 >= self.operations.len() {
            return Ok(());
        }
        self.operations.truncate(savepoint.0);
        self.stager = stage_all(
            &self.fluree,
            self.base.clone(),
            &self.operations,
            &self.context,
        )
        .await?;
        Ok(())
    }

    async fn push(&mut self, staged: Staged) -> Result<Option<CypherReturn>> {
        match stage_one(&self.fluree, &mut self.stager, &staged, &self.context).await {
            Ok(rows) => {
                self.operations.push(staged);
                Ok(rows)
            }
            Err(e) => {
                // A failed stage consumes the stager's state; rebuild it from
                // the operations that did stage.
                self.stager = stage_all(
                    &self.fluree,
                    self.base.clone(),
                    &self.operations,
                    &self.context,
                )
                .await?;
                Err(e)
            }
        }
    }

    /// The ledger as the staged operations leave it, for queries. Apply
    /// policy to it as to any other view.
    ///
    /// Reading makes the commit conditional on the ledger not having moved
    /// since the transaction began; see the module docs.
    pub async fn db(&self) -> Result<GraphDb> {
        self.read.store(true, Ordering::Relaxed);
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
            mut context,
            read,
        } = self;
        let read = read.into_inner();
        let handle = fluree.ledger_cached(&ledger_id).await?;
        let base_t = base.t();
        let mut prestaged = Some(stager.finish().await?);

        const MAX_RETRIES: usize = 16;
        for attempt in 0..MAX_RETRIES {
            let guard = handle.lock_for_write().await;
            let unchanged = guard.state().t() == base_t
                && guard.state().head_commit_id.as_ref() == base_head.as_ref();
            if read && !unchanged {
                return Err(ApiError::Transact(TransactError::CommitConflict {
                    expected_t: base_t,
                    head_t: guard.state().t(),
                }));
            }
            let stage = match prestaged.take() {
                Some(stage) if unchanged => stage,
                prestaged => match prestaged.and_then(|stage| {
                    Fluree::rebase_stage(&guard, stage, base_t, base_head.as_ref())
                }) {
                    Some(stage) => stage,
                    None => {
                        let state = guard.clone_state();
                        // The policy the ledger has now: a commit since the
                        // transaction began may have changed it.
                        context.policy =
                            transact_policy(&fluree, &state, &context.governance).await?;
                        stage_all(&fluree, state, &operations, &context)
                            .await?
                            .finish()
                            .await?
                    }
                },
            };
            match fluree
                .commit_and_finalize(
                    guard,
                    stage,
                    TxnType::Update,
                    commit_opts.clone(),
                    &context.index_config,
                    context.tracker.tally(),
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

/// The policy writes staged on `state` are checked against.
async fn transact_policy(
    fluree: &Fluree,
    state: &LedgerState,
    governance: &GovernanceOptions,
) -> Result<Option<PolicyContext>> {
    crate::build_transact_policy_context(
        fluree,
        &state.snapshot,
        state.novelty.as_ref(),
        Some(state.novelty.as_ref()),
        state.t(),
        governance,
    )
    .await
}

/// Stage one operation over the stager's state; for a Cypher write, also
/// answer its `RETURN`.
async fn stage_one(
    fluree: &Fluree,
    stager: &mut SequentialStager,
    staged: &Staged,
    context: &StageContext,
) -> Result<Option<CypherReturn>> {
    match staged {
        Staged::Operation(operation) => {
            let plan = operation.plan()?;
            stager
                .stage_with(true, |state| async move {
                    let (stage, _, _) = fluree
                        .stage_plan(
                            &plan,
                            state,
                            TxnOpts::default(),
                            &CommitOpts::default(),
                            Some(&context.tracker),
                            &context.index_config,
                            context.policy.as_ref(),
                        )
                        .await?;
                    Ok(stage)
                })
                .await?;
            Ok(None)
        }
        Staged::Cypher {
            query,
            params,
            skolem_txn_id,
        } => {
            stage_cypher(
                fluree,
                stager,
                query,
                params.as_ref(),
                skolem_txn_id,
                context,
            )
            .await
        }
    }
}

/// What a staged Cypher statement's `RETURN` needs once its flakes are part
/// of the stager's state.
enum PendingReturn {
    /// Created entities, read back by their skolemized identities.
    Created(cypher_write::CypherWriteReturnPlan),
    /// A multi-clause statement's rows, already formatted.
    Rows(CypherReturn),
}

async fn stage_cypher(
    fluree: &Fluree,
    stager: &mut SequentialStager,
    query: &str,
    params: Option<&CypherParamMap>,
    skolem_txn_id: &str,
    context: &StageContext,
) -> Result<Option<CypherReturn>> {
    let mut pending = None;
    let pending_ref = &mut pending;
    stager
        .stage_with(true, |state| async move {
            // Multi-clause statements run through the sequential driver, which
            // answers a trailing RETURN from its final row table.
            if let Ok(ast) = crate::query::helpers::substituted_cypher_ast(query, params) {
                if let Some(sq) = crate::cypher_seq::detect_sequential(&ast) {
                    let outcome = fluree
                        .stage_cypher_sequential(
                            state,
                            &sq,
                            &context.ledger_id,
                            Some(&context.governance),
                            Some(&context.index_config),
                            context.policy.as_ref(),
                            Some(&context.tracker),
                            Some(skolem_txn_id.to_string()),
                        )
                        .await?;
                    if let Some(result) = outcome.return_result {
                        let view = fluree
                            .resolve_and_attach_config(GraphDb::from_ledger_state(
                                &outcome.final_state,
                            ))
                            .await?;
                        let table = result.to_cypher_typed_table(&view).await.map_err(|e| {
                            ApiError::internal(format!("Cypher RETURN formatting: {e}"))
                        })?;
                        *pending_ref = Some(PendingReturn::Rows(table));
                    }
                    return Ok(outcome.stage_result);
                }
            }

            if let Some(plan) = cypher_write::plan_write_return_source(query, params)? {
                *pending_ref = Some(PendingReturn::Created(plan));
            }
            let plan = fluree
                .cypher_write_plan_with_skolem(
                    query,
                    params,
                    &context.ledger_id,
                    &state.snapshot,
                    Some(skolem_txn_id.to_string()),
                )
                .await?;
            let resolved = match plan {
                WritePlan::Single(txn) => ResolvedConditional::single(*txn),
                WritePlan::Conditional(cw) => {
                    // The branch-choosing probe sees what the caller's policy
                    // lets it see, as the autocommit path's does.
                    let probe = GraphDb::from_ledger_state(&state);
                    let probe = if context.governance.has_any_policy_inputs() {
                        fluree.wrap_policy(probe, &context.governance).await?
                    } else {
                        probe
                    };
                    fluree
                        .resolve_conditional_cypher(&cw, probe, &context.ledger_id, &state.snapshot)
                        .await?
                }
                WritePlan::Sequential(_) => {
                    return Err(ApiError::internal(
                        "sequential Cypher write reached the single-statement path",
                    ))
                }
            };
            match resolved.followup {
                Some(followup) => {
                    fluree
                        .stage_pair_from_txns(
                            state,
                            resolved.primary,
                            followup,
                            Some(&context.index_config),
                            context.policy.as_ref(),
                            Some(&context.tracker),
                        )
                        .await
                }
                None => {
                    fluree
                        .stage_transaction_from_txn(
                            state,
                            resolved.primary,
                            Some(&context.index_config),
                            context.policy.as_ref(),
                            Some(&context.tracker),
                        )
                        .await
                }
            }
        })
        .await?;
    Ok(match pending {
        None => None,
        Some(PendingReturn::Rows(table)) => Some(table),
        Some(PendingReturn::Created(plan)) => {
            Some(cypher_write::write_return_typed_rows(&plan, skolem_txn_id, stager.state()).await?)
        }
    })
}

async fn stage_all(
    fluree: &Fluree,
    base: LedgerState,
    operations: &[Staged],
    context: &StageContext,
) -> Result<SequentialStager> {
    let mut stager = SequentialStager::new(base);
    for staged in operations {
        stage_one(fluree, &mut stager, staged, context).await?;
    }
    Ok(stager)
}
