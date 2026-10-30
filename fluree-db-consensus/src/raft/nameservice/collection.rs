//! What the leader proposes to collect garbage: see [`Collector`].

use std::collections::HashSet;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use fluree_db_api::collect::Collector;
use fluree_db_api::{ApiError, ContentId, Fluree, Result};
use fluree_db_core::ledger_id::split_ledger_id;

use super::{RaftNameService, SmCommand, SmResponse};
use crate::raft::state_machine::{ApplyOutcome, CollectCommand};
use crate::QueuedRequest;

/// What a lease keeps of the window followers refuse votes for after
/// hearing from the leader: the rest absorbs clock drift and pauses.
const LEASE_SHARE: (u32, u32) = (3, 4);

impl RaftNameService {
    async fn propose_collect(&self, command: CollectCommand) -> Result<SmResponse> {
        self.submit_lifecycle(SmCommand::Collect(command))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))
    }
}

fn unexpected(command: &str, response: SmResponse) -> ApiError {
    ApiError::internal(format!("unexpected response to {command}: {response:?}"))
}

#[async_trait]
impl Collector for RaftNameService {
    async fn condemn(&self, intent: &str) -> Result<u64> {
        match self
            .propose_collect(CollectCommand::Condemn {
                intent: intent.to_string(),
            })
            .await?
        {
            SmResponse::Condemned { number } => Ok(number),
            other => Err(unexpected("Condemn", other)),
        }
    }

    async fn delete(&self, number: u64) -> Result<Option<Vec<String>>> {
        match self
            .propose_collect(CollectCommand::Delete { number })
            .await?
        {
            SmResponse::Deleting { spared } => Ok(Some(spared)),
            SmResponse::IntentClosed { .. } => Ok(None),
            other => Err(unexpected("Delete", other)),
        }
    }

    async fn complete(&self, number: u64) -> Result<()> {
        match self
            .propose_collect(CollectCommand::Complete { number })
            .await?
        {
            SmResponse::IntentClosed { .. } => Ok(()),
            other => Err(unexpected("Complete", other)),
        }
    }

    async fn open(&self) -> Vec<(u64, String, bool)> {
        self.state
            .read()
            .await
            .collect
            .intents
            .iter()
            .map(|(number, intent)| (*number, intent.intent.clone(), intent.deleting))
            .collect()
    }

    async fn queued(&self, fluree: &Fluree, ledger: &str) -> Result<HashSet<ContentId>> {
        let mut named = HashSet::new();
        let queued: Vec<(String, ContentId)> = {
            let state = self.state.read().await;
            for (key, outcome) in &state.idempotency {
                if split_ledger_id(&key.ledger_id).is_ok_and(|(name, _)| name == ledger) {
                    named.insert(match outcome {
                        ApplyOutcome::Applied(record) => record.request_cid.clone(),
                        ApplyOutcome::Failed(record) => record.request_cid.clone(),
                    });
                }
            }
            state
                .queues
                .iter()
                .filter(|(key, _)| key.ledger_name == ledger)
                .flat_map(|(key, queue)| {
                    queue
                        .iter()
                        .map(|entry| (key.ledger_id(), entry.request_cid.clone()))
                })
                .collect()
        };
        for (ledger_id, request_cid) in queued {
            named.insert(request_cid.clone());
            let namespace = fluree.storage_namespace(&ledger_id).await?;
            let bytes = match fluree.content_store(&namespace).get(&request_cid).await {
                Ok(bytes) => bytes,
                // Its worker poisons the entry; nothing it names was stored
                // for it to keep.
                Err(fluree_db_core::Error::NotFound(_)) => continue,
                Err(e) => return Err(e.into()),
            };
            match QueuedRequest::from_bytes(&bytes) {
                Ok(QueuedRequest::Transact(transact)) => {
                    named.extend(transact.commit_opts.raw_txn_id.clone());
                }
                Ok(QueuedRequest::Push(push)) => named.extend(push.commit_cids.iter().cloned()),
                Ok(QueuedRequest::PushWithMerges(push)) => {
                    named.extend(push.push.commit_cids.iter().cloned());
                    named.extend(push.merged_commit_cids.iter().cloned());
                }
                Ok(
                    QueuedRequest::Revert(_) | QueuedRequest::Merge(_) | QueuedRequest::Rebase(_),
                ) => {}
                Err(e) => {
                    return Err(ApiError::internal(format!(
                        "queued request {request_cid} does not decode: {e}"
                    )))
                }
            }
        }
        Ok(named)
    }

    async fn lease(&self) -> Option<Instant> {
        let started = Instant::now();
        self.raft.get_read_log_id().await.ok()?;
        let (share, of) = LEASE_SHARE;
        let window = Duration::from_millis(self.raft.config().election_timeout_max) * share / of;
        Some(started + window)
    }
}
