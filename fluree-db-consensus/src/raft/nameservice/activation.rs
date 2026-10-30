//! What the leader proposes to publish a prepared erasure: see
//! [`ErasureActivator`].

use std::time::Duration;

use fluree_db_api::retention::erasure::ErasureActivator;
use fluree_db_api::{ApiError, Result};
use fluree_db_nameservice::NsRecord;

use super::{ref_key_of, RaftNameService, SmCommand, SmResponse};
use crate::raft::state_machine::{ActivateErasure, ErasureCommand, HolderState, RefKey};

fn key(record: &NsRecord) -> Result<RefKey> {
    ref_key_of(record.ledger_id.as_str()).map_err(|e| ApiError::internal(e.to_string()))
}

fn keys(holders: &[NsRecord]) -> Result<Vec<RefKey>> {
    holders.iter().map(key).collect()
}

/// `record` as the activation is to find it: its record carries the
/// replicated fence and head, and the generation installed here.
fn holder_state(record: &NsRecord) -> Result<HolderState> {
    Ok(HolderState {
        key: key(record)?,
        fence: record.fence.map(fluree_db_nameservice::Fence::as_u64),
        head: record
            .commit_head_id
            .clone()
            .map(|head| (head, record.commit_t)),
        generation: record.erasure_generation,
    })
}

impl RaftNameService {
    async fn propose_erasure(&self, command: ErasureCommand) -> Result<SmResponse> {
        self.submit_lifecycle(SmCommand::Erasure(command))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))
    }
}

fn unexpected(what: &str, response: SmResponse) -> ApiError {
    ApiError::internal(format!("unexpected response to {what}: {response:?}"))
}

#[async_trait::async_trait]
impl ErasureActivator for RaftNameService {
    async fn holder_set(&self) -> Result<Option<u64>> {
        let (tracking, applied) = {
            let state = self.state.read().await;
            (state.erasure.tracking, state.erasure.next_activation)
        };
        if self.installed_through() < applied {
            return Ok(None);
        }
        if !tracking {
            match self.propose_erasure(ErasureCommand::Track).await? {
                SmResponse::ErasureTracking => {}
                other => return Err(unexpected("Track", other)),
            }
        }
        Ok(Some(self.state.read().await.erasure.holder_set))
    }

    async fn hold(&self, holders: &[NsRecord], hold: Duration) -> Result<()> {
        let until_millis = crate::raft::current_millis() + hold.as_millis() as u64;
        match self
            .propose_erasure(ErasureCommand::Hold {
                holders: keys(holders)?,
                until_millis,
            })
            .await?
        {
            SmResponse::ErasureHoldApplied => Ok(()),
            other => Err(unexpected("Hold", other)),
        }
    }

    async fn release(&self, holders: &[NsRecord]) -> Result<()> {
        match self
            .propose_erasure(ErasureCommand::Release {
                holders: keys(holders)?,
            })
            .await?
        {
            SmResponse::ErasureHoldApplied => Ok(()),
            other => Err(unexpected("Release", other)),
        }
    }

    async fn activate(
        &self,
        manifest: &str,
        holder_set: u64,
        holders: &[NsRecord],
    ) -> Result<Option<u64>> {
        let holders = holders.iter().map(holder_state).collect::<Result<_>>()?;
        match self
            .propose_erasure(ErasureCommand::Activate(ActivateErasure {
                manifest: manifest.to_string(),
                holder_set,
                holders,
                applied_at_millis: crate::raft::current_millis(),
            }))
            .await?
        {
            SmResponse::ErasureActivated { activation } => Ok(Some(activation)),
            SmResponse::ErasureActivationRefused { refusal } => {
                tracing::debug!(?refusal, "erasure activation refused; prepared again");
                Ok(None)
            }
            other => Err(unexpected("Activate", other)),
        }
    }

    async fn applied(&self, manifest: &str) -> Option<u64> {
        let state = self.state.read().await;
        state
            .erasure
            .activations
            .iter()
            .find(|(_, a)| a.manifest == manifest)
            .map(|(number, _)| *number)
    }

    async fn installed_everywhere(&self, number: u64) -> bool {
        let state = self.state.read().await;
        !state.configured_voters.is_empty()
            && state.configured_voters.iter().all(|member| {
                state
                    .erasure
                    .installed
                    .get(member)
                    .is_some_and(|installed| installed.through > number)
            })
    }
}
