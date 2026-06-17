//! Per-leader worker that drains [`NameServiceState`] queues into
//! head advances.
//!
//! The state machine appends `QueueEntry` rows to per-branch FIFOs as
//! [`Command::EnqueueCommand`] applies; the worker (running only on
//! the current Raft leader) pulls each front entry, restages the
//! commit locally, writes the commit blob to shared CAS, and proposes
//! [`Command::ApplyHead`] — at which point the entry pops and the
//! head advances.
//!
//! Scope cuts for v1 (tracked in `docs/design/raft-command-queue.md`):
//! - No retry budget. Any staging failure poisons the entry.
//! - [`BodyKind::Pushed`] handling is deferred; the worker poisons
//!   any pushed entry until that path lands.
//! - Token-bearing fields in [`crate::QueuedRequest::governance`]
//!   travel verbatim; redaction is future work.
//!
//! The worker is owned by the raft integration layer, which spawns
//! it on leadership gain and aborts it on leadership loss (the same
//! lifecycle as the indexer launcher in `fluree-db-server`).

use crate::local::build_policy_context;
use crate::raft::state_machine::{
    BodyKind, Command as SmCommand, PoisonQueueEntryArgs, PoisonReason, QueueEntry, RefKey,
};
use crate::raft::state_machine_adapter::SharedState;
use crate::raft::TypeConfig;
use crate::{QueuedPush, QueuedRequest, QueuedTransact, SubmissionError, TransactionBody};
use fluree_db_api::{ApiError, Base64Bytes, Fluree, PushCommitsRequest, StagedPush};
use fluree_db_core::ContentId;
use fluree_db_ledger::IndexConfig;
use fluree_db_nameservice::CommitPublisher;
use openraft::Raft;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::warn;

/// How often the drain loop polls [`NameServiceState::queues`] when
/// no work was found on the previous tick.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How long the drain loop waits after a Raft propose failure before
/// trying again. Long enough that we don't tight-loop against a lost
/// leader; short enough that recovery feels responsive.
const RAFT_BACKOFF: Duration = Duration::from_millis(250);

/// Per-leader worker that processes the per-branch command queue.
///
/// Cloning is cheap (`Arc` clones); a single worker instance is
/// driven by the leader watcher.
///
/// Head advances flow through `publisher.publish_commit` — a Raft
/// deployment supplies a [`RaftNameService`](crate::raft::nameservice::RaftNameService)
/// here, which proposes `ApplyHead` against the current per-branch
/// queue front. Poisoning still goes direct to Raft because there's
/// no trait surface for "fail this queue entry."
#[derive(Clone)]
pub struct CommitWorker {
    raft: Arc<Raft<TypeConfig>>,
    publisher: Arc<dyn CommitPublisher>,
    fluree: Arc<Fluree>,
    index_config: IndexConfig,
    shared_state: SharedState,
}

impl CommitWorker {
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        publisher: Arc<dyn CommitPublisher>,
        fluree: Arc<Fluree>,
        index_config: IndexConfig,
        shared_state: SharedState,
    ) -> Self {
        Self {
            raft,
            publisher,
            fluree,
            index_config,
            shared_state,
        }
    }

    /// Borrow the shared state. Drain loops use this to peek queue
    /// fronts without going through Raft RPC.
    pub fn shared_state(&self) -> &SharedState {
        &self.shared_state
    }

    /// Process a single queue entry end-to-end.
    ///
    /// Reads the [`QueuedRequest`] envelope from CAS, stages the
    /// commit, writes the commit blob, and publishes the head
    /// advance through the [`CommitPublisher`]. On staging failure,
    /// proposes [`Command::PoisonQueueEntry`] instead. Returns once
    /// the entry has reached a terminal state in the queue (advanced
    /// or poisoned).
    pub async fn process_entry(
        &self,
        ref_key: &RefKey,
        entry: QueueEntry,
    ) -> Result<(), WorkerError> {
        match self.try_advance_head(ref_key, &entry).await {
            Ok(()) => Ok(()),
            Err(WorkerError::Stage(reason)) => {
                self.propose_poison(ref_key, entry.queue_id, reason).await
            }
            Err(other) => Err(other),
        }
    }

    async fn try_advance_head(
        &self,
        ref_key: &RefKey,
        entry: &QueueEntry,
    ) -> Result<(), WorkerError> {
        let envelope = self.load_envelope(ref_key, entry).await?;
        // The state machine doesn't introspect the envelope at enqueue
        // time, so the inline discriminator could disagree with what
        // the leader actually wrote. Bail explicitly rather than
        // process under a kind the queue didn't declare.
        check_envelope_kind(entry.body_kind, &envelope)?;

        let (commit_id, commit_t) = match envelope {
            QueuedRequest::Transact(transact) => self.stage_and_persist(ref_key, transact).await?,
            QueuedRequest::Push(push) => self.process_push(ref_key, push).await?,
        };
        self.publish_head_advance(ref_key, commit_id, commit_t).await
    }

    async fn load_envelope(
        &self,
        ref_key: &RefKey,
        entry: &QueueEntry,
    ) -> Result<QueuedRequest, WorkerError> {
        let ledger_id = format_full_ledger_id(ref_key);
        let bytes = self
            .fluree
            .content_store(&ledger_id)
            .get(&entry.request_cid)
            .await
            .map_err(|e| {
                WorkerError::Stage(PoisonReason::StagingFailed {
                    error: format!("CAS read of request_cid failed: {e}"),
                    attempts: 1,
                })
            })?;
        QueuedRequest::from_bytes(&bytes).map_err(|e| {
            WorkerError::Stage(PoisonReason::BodyMalformed {
                error: format!("QueuedRequest decode failed: {e}"),
            })
        })
    }

    /// Resolve the ledger handle, dispatch on body kind, stage the
    /// commit, and write the commit blob to CAS. Returns the new head
    /// CID + t pair on success; the surrounding caller proposes
    /// [`Command::ApplyHead`].
    async fn stage_and_persist(
        &self,
        ref_key: &RefKey,
        transact: QueuedTransact,
    ) -> Result<(ContentId, i64), WorkerError> {
        let QueuedTransact {
            body,
            txn_opts,
            commit_opts,
            tracking,
            governance,
        } = transact;

        let ledger_id = format_full_ledger_id(ref_key);
        let ledger_manager =
            self.fluree
                .ledger_manager()
                .ok_or_else(|| stage_failure("LedgerManager is not configured on Fluree"))?;
        let ledger_handle = ledger_manager
            .get_or_load(&ledger_id)
            .await
            .map_err(|e| stage_failure(&format!("ledger load failed: {e}")))?;

        let policy_ctx = build_policy_context(&ledger_handle, &governance)
            .await
            .map_err(submission_to_stage)?;

        let staged = self.fluree.stage(&ledger_handle);
        let staged = match &body {
            TransactionBody::JsonLdInsert(json) => staged.insert(json),
            TransactionBody::JsonLdUpsert(json) => staged.upsert(json),
            TransactionBody::JsonLdUpdate(json) => staged.update(json),
            TransactionBody::TurtleInsert(text) => staged.insert_turtle(text.as_str()),
            TransactionBody::TurtleUpsert(text) | TransactionBody::TrigUpsert(text) => {
                staged.upsert_turtle(text.as_str())
            }
            TransactionBody::Sparql(query) => staged.sparql_update(query.as_str()),
        };

        let mut builder = staged
            .txn_opts(txn_opts)
            .commit_opts(commit_opts.into_commit_opts())
            .index_config(self.index_config.clone());
        if let Some(tracking) = tracking {
            builder = builder.tracking(tracking);
        }
        if let Some(policy) = policy_ctx {
            builder = builder.policy(policy);
        }

        let (write_guard, staged_commit) = builder
            .build_commit()
            .await
            .map_err(|e| stage_failure(&format!("build_commit failed: {e}")))?;

        let commit_cid = staged_commit
            .commit
            .id
            .clone()
            .expect("build_commit guarantees commit.id is set");
        let commit_t = staged_commit.commit.t;

        let content_store = self.fluree.content_store(&ledger_id);
        content_store
            .put_with_id(&commit_cid, &staged_commit.commit_bytes)
            .await
            .map_err(|e| stage_failure(&format!("commit blob write failed: {e}")))?;
        for (cid, bytes) in &staged_commit.referenced_bytes {
            content_store
                .put_with_id(cid, bytes)
                .await
                .map_err(|e| stage_failure(&format!("referenced blob write failed: {e}")))?;
        }

        // Local state install. Mirrors `RaftCommitter::transact` on
        // the Applied branch: derives post-commit state from the
        // staged commit and finalizes it through the held write guard
        // so this node's cached view doesn't lag the head we're
        // about to propose.
        let (_receipt, new_state) = staged_commit
            .finalize_state()
            .map_err(|e| stage_failure(&format!("finalize_state failed: {e}")))?;
        let needs_reindex = new_state.should_reindex(&self.index_config);
        self.fluree
            .finalize_commit(write_guard, new_state, commit_t, needs_reindex)
            .await
            .map_err(api_error_to_stage)?;

        Ok((commit_cid, commit_t))
    }

    /// Decode the queued push, hand it to `Fluree::prepare_push` for
    /// validation + CAS persistence + local state derivation, then
    /// finalize through the held write guard so this node's cache
    /// catches up with the head we're about to publish.
    async fn process_push(
        &self,
        ref_key: &RefKey,
        push: QueuedPush,
    ) -> Result<(ContentId, i64), WorkerError> {
        let QueuedPush {
            commits,
            blobs,
            governance,
        } = push;
        let ledger_id = format_full_ledger_id(ref_key);
        let payload = PushCommitsRequest {
            commits: commits.into_iter().map(Base64Bytes).collect(),
            blobs: blobs.into_iter().map(|(k, v)| (k, Base64Bytes(v))).collect(),
        };
        let StagedPush {
            new_head_id,
            new_head_t,
            write_guard,
            final_state,
            ..
        } = self
            .fluree
            .prepare_push(&ledger_id, payload, &governance, &self.index_config)
            .await
            .map_err(|e| stage_failure(&format!("prepare_push failed: {e}")))?;

        let needs_reindex = final_state.should_reindex(&self.index_config);
        self.fluree
            .finalize_commit(write_guard, final_state, new_head_t, needs_reindex)
            .await
            .map_err(api_error_to_stage)?;

        Ok((new_head_id, new_head_t))
    }

    async fn publish_head_advance(
        &self,
        ref_key: &RefKey,
        commit_id: ContentId,
        commit_t: i64,
    ) -> Result<(), WorkerError> {
        let full_ledger_id = format_full_ledger_id(ref_key);
        self.publisher
            .publish_commit(&full_ledger_id, commit_t, &commit_id)
            .await
            .map_err(|e| WorkerError::Raft(format!("publish_commit failed: {e}")))
    }

    async fn propose_poison(
        &self,
        ref_key: &RefKey,
        queue_id: u64,
        reason: PoisonReason,
    ) -> Result<(), WorkerError> {
        let cmd = SmCommand::PoisonQueueEntry(PoisonQueueEntryArgs {
            ledger_id: ref_key.ledger_id.clone(),
            branch: ref_key.branch.clone(),
            queue_id,
            reason,
            applied_at_millis: current_millis(),
        });
        // The state-machine response (`Poisoned` vs `QueueDesync`) is
        // informational once the poison is durably proposed — either
        // way the entry is done from the worker's perspective. Only
        // surface Raft-side failures.
        self.raft
            .client_write(cmd)
            .await
            .map(|_| ())
            .map_err(|e| WorkerError::Raft(format!("PoisonQueueEntry propose failed: {e}")))
    }

    /// Drain per-branch queues until aborted by the caller.
    ///
    /// Each tick snapshots the current front entry for every
    /// non-empty branch and runs them through [`Self::process_entry`]
    /// in turn. Cross-branch ordering inside a single tick is
    /// non-deterministic (driven by the underlying `HashMap`); the
    /// per-branch FIFO ordering the design promises is preserved
    /// because each tick only takes the front of each branch.
    ///
    /// Sequential within a tick keeps the worker simple for v1 — the
    /// design doc notes parallel-across-branches dispatch as a
    /// follow-up if profile data justifies it.
    pub async fn run(self) {
        loop {
            let pending = self.snapshot_pending_fronts().await;
            if pending.is_empty() {
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }

            let mut raft_blocked = false;
            for (ref_key, entry) in pending {
                let queue_id = entry.queue_id;
                // `process_entry` consumes `Stage` failures by
                // proposing `PoisonQueueEntry`, so only `Raft`
                // propagates here.
                match self.process_entry(&ref_key, entry).await {
                    Ok(()) => {}
                    Err(WorkerError::Stage(_)) => {
                        unreachable!("process_entry maps Stage failures to PoisonQueueEntry")
                    }
                    Err(WorkerError::Raft(error)) => {
                        warn!(
                            ledger_id = %ref_key.ledger_id,
                            branch = %ref_key.branch,
                            queue_id,
                            error = %error,
                            "raft publish failed; backing off and re-polling"
                        );
                        raft_blocked = true;
                        break;
                    }
                }
            }

            if raft_blocked {
                tokio::time::sleep(RAFT_BACKOFF).await;
            }
        }
    }

    async fn snapshot_pending_fronts(&self) -> Vec<(RefKey, QueueEntry)> {
        let state = self.shared_state.read().await;
        state
            .queues
            .iter()
            .filter_map(|(ref_key, queue)| {
                queue.front().map(|entry| (ref_key.clone(), entry.clone()))
            })
            .collect()
    }
}

fn format_full_ledger_id(ref_key: &RefKey) -> String {
    format!("{}:{}", ref_key.ledger_id, ref_key.branch)
}

/// Cross-check the queue entry's inline discriminator against the
/// envelope payload variant. A mismatch is a state-machine /
/// committer bug — surface loudly so the entry poisons rather than
/// processing under the wrong path.
fn check_envelope_kind(
    body_kind: BodyKind,
    envelope: &QueuedRequest,
) -> Result<(), WorkerError> {
    let expected = match envelope {
        QueuedRequest::Transact(t) => BodyKind::from(&t.body),
        QueuedRequest::Push(_) => BodyKind::Pushed,
    };
    if expected == body_kind {
        Ok(())
    } else {
        Err(WorkerError::Stage(PoisonReason::BodyMalformed {
            error: format!(
                "queue entry body_kind {body_kind:?} does not match envelope variant {expected:?}"
            ),
        }))
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn stage_failure(message: &str) -> WorkerError {
    WorkerError::Stage(PoisonReason::StagingFailed {
        error: message.into(),
        attempts: 1,
    })
}

fn submission_to_stage(err: SubmissionError) -> WorkerError {
    WorkerError::Stage(PoisonReason::StagingFailed {
        error: err.to_string(),
        attempts: 1,
    })
}

fn api_error_to_stage(err: ApiError) -> WorkerError {
    WorkerError::Stage(PoisonReason::StagingFailed {
        error: err.to_string(),
        attempts: 1,
    })
}

/// Internal classification for worker outcomes.
///
/// `Stage` carries a [`PoisonReason`] the worker turns into a
/// `PoisonQueueEntry` proposal. `Raft` is reserved for propose
/// failures the caller propagates — those mean the cluster
/// fundamentally can't accept commands right now (leader changed,
/// quorum lost) and the drain loop should yield.
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("staging poisoned: {0:?}")]
    Stage(PoisonReason),
    #[error("raft propose: {0}")]
    Raft(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::{NameServiceState, QueueEntry};
    use crate::{IdempotencyCacheKey, IdempotencyKey};
    use fluree_db_api::{CommitOptsRequest, GovernanceOptions, TrackingOptions};
    use fluree_db_core::ContentKind;
    use fluree_db_transact::TxnOpts;
    use serde_json::json;
    use std::collections::VecDeque;
    use tokio::sync::RwLock;

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    fn sample_transact_envelope() -> QueuedRequest {
        QueuedRequest::Transact(QueuedTransact {
            body: TransactionBody::JsonLdInsert(json!({"@id": "ex:s", "ex:p": "ex:o"})),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOptsRequest::default(),
            tracking: Some(TrackingOptions::default()),
            governance: GovernanceOptions::default(),
        })
    }

    fn sample_push_envelope() -> QueuedRequest {
        QueuedRequest::Push(QueuedPush {
            commits: vec![vec![1, 2, 3, 4]],
            blobs: std::collections::HashMap::new(),
            governance: GovernanceOptions::default(),
        })
    }

    #[test]
    fn queued_transact_roundtrips_through_codec() {
        let envelope = sample_transact_envelope();
        let bytes = envelope.to_bytes().expect("encode");
        let decoded = QueuedRequest::from_bytes(&bytes).expect("decode");

        match decoded {
            QueuedRequest::Transact(t) => {
                assert!(matches!(t.body, TransactionBody::JsonLdInsert(_)));
                assert!(t.tracking.is_some());
            }
            other => panic!("expected Transact, got {other:?}"),
        }
    }

    #[test]
    fn queued_push_roundtrips_through_codec() {
        let envelope = sample_push_envelope();
        let bytes = envelope.to_bytes().expect("encode");
        let decoded = QueuedRequest::from_bytes(&bytes).expect("decode");
        match decoded {
            QueuedRequest::Push(p) => {
                assert_eq!(p.commits.len(), 1);
                assert_eq!(p.commits[0], vec![1, 2, 3, 4]);
            }
            other => panic!("expected Push, got {other:?}"),
        }
    }

    #[test]
    fn check_envelope_kind_accepts_matching_pair() {
        assert!(check_envelope_kind(BodyKind::JsonLdInsert, &sample_transact_envelope()).is_ok());
        assert!(check_envelope_kind(BodyKind::Pushed, &sample_push_envelope()).is_ok());
    }

    #[test]
    fn check_envelope_kind_rejects_mismatched_pair() {
        // Transact envelope marked as Pushed
        assert!(check_envelope_kind(BodyKind::Pushed, &sample_transact_envelope()).is_err());
        // Push envelope marked as a JSON-LD insert
        assert!(check_envelope_kind(BodyKind::JsonLdInsert, &sample_push_envelope()).is_err());
    }

    fn enqueued_entry(queue_id: u64, request_cid: ContentId, kind: BodyKind) -> QueueEntry {
        QueueEntry {
            queue_id,
            enqueued_index: 1,
            enqueued_at_millis: 1_000,
            idempotency: Some(IdempotencyCacheKey::new(
                "test/db",
                IdempotencyKey::new("k1"),
            )),
            request_cid,
            body_kind: kind,
        }
    }

    fn install_queue(
        state: &mut NameServiceState,
        ledger_id: &str,
        branch: &str,
        entries: Vec<QueueEntry>,
    ) {
        state
            .queues
            .insert(RefKey::new(ledger_id, branch), VecDeque::from(entries));
    }

    /// Verify the snapshot only takes the front of each branch — the
    /// per-branch FIFO invariant the drain loop relies on.
    #[tokio::test]
    async fn snapshot_pending_fronts_takes_one_per_branch() {
        // Build the worker around a SharedState only — none of the
        // other deps are exercised by the snapshot path. We construct
        // a temporary CommitWorker by hand-poking the fields because
        // the Fluree/Raft handles are not needed for this slice.
        let mut state = NameServiceState::default();
        install_queue(
            &mut state,
            "test/db",
            "main",
            vec![
                enqueued_entry(7, cid(1), BodyKind::JsonLdInsert),
                enqueued_entry(8, cid(2), BodyKind::JsonLdInsert),
            ],
        );
        install_queue(
            &mut state,
            "test/db",
            "feature",
            vec![enqueued_entry(9, cid(3), BodyKind::Sparql)],
        );
        install_queue(&mut state, "test/db", "empty", vec![]);

        let shared = Arc::new(RwLock::new(state));
        let pending = snapshot_pending_fronts_for_test(&shared).await;

        assert_eq!(pending.len(), 2, "empty branches must not appear");
        let queue_ids: Vec<u64> = pending.iter().map(|(_, entry)| entry.queue_id).collect();
        assert!(queue_ids.contains(&7));
        assert!(queue_ids.contains(&9));
        assert!(
            !queue_ids.contains(&8),
            "only the front of each branch is sampled"
        );
    }

    /// Replicates the snapshot logic in `CommitWorker::run` against a
    /// bare `SharedState`. Lets us exercise the queue-traversal
    /// invariants without wiring up Raft + Fluree.
    async fn snapshot_pending_fronts_for_test(state: &SharedState) -> Vec<(RefKey, QueueEntry)> {
        let state = state.read().await;
        state
            .queues
            .iter()
            .filter_map(|(ref_key, queue)| {
                queue.front().map(|entry| (ref_key.clone(), entry.clone()))
            })
            .collect()
    }
}
