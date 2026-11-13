//! Proving on a cluster's other members. The leader hands a step to a member
//! with a prover, which proves it in the background and stores the result
//! where the leader, or the next one, finds it; the leader verifies it and
//! records it.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::ProvedStep;
use crate::Fluree;

/// A proving step a cluster's leader hands another member.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProofStep {
    /// The branch whose registration holds the job.
    pub branch: String,
    pub request_id: String,
    pub commit: String,
    /// The branches holding the commit.
    pub branches: Vec<String>,
}

/// How a step ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StepOutcome {
    /// Proved, verified and stored.
    Proved(ProvedStep),
    /// Nothing was left to prove.
    Nothing,
    /// The prover refused the step: it will not prove on retry.
    Refused(String),
    /// To be tried again.
    Retry(String),
    /// The prover ran out of time: tried again, up to a limit.
    TimedOut(String),
}

/// Where a member is with a step its leader handed it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MemberProving {
    /// It has no prover.
    NoProver,
    /// It is proving another step.
    Busy,
    /// It is proving this one.
    Proving,
    Done(StepOutcome),
}

/// A cluster's other members, through which its leader has steps proved.
#[async_trait]
pub trait ProvingMembers: Send + Sync + std::fmt::Debug {
    /// Have `step` proved by another member with a prover: `assigned`, the
    /// member recorded as proving it for an earlier ask, first, then one that
    /// neither leads nor owns a branch holding the commit, then any other. A
    /// member that does not answer for a while is passed over. `None` when
    /// none takes it, or this member was the one assigned, and this member
    /// proves it.
    async fn prove(&self, step: &ProofStep, assigned: Option<u64>) -> Option<StepOutcome>;

    /// Resolves once this member no longer leads in the term it leads now,
    /// at once if it does not lead.
    async fn deposed(&self);
}

/// The step a member proves for its leader, and how it ended once it has.
pub(crate) type ForLeader = (ProofStep, tokio::sync::watch::Receiver<Option<StepOutcome>>);

impl Fluree {
    /// Have this cluster leader's proving steps run on its other members
    /// through `members`.
    pub fn set_proving_members(&self, members: Arc<dyn ProvingMembers>) {
        if let Some(runtime) = self.retention.erasure.get() {
            let _ = runtime.members.set(members);
        }
    }

    /// Where this member, `node`, is with `step`, which its leader hands it:
    /// begun in the background unless another is under way, and its outcome
    /// once it ends. One at a time: a proof can take most of the host's
    /// memory.
    pub fn prove_for_leader(self: &Arc<Self>, step: ProofStep, node: u64) -> MemberProving {
        let Some(runtime) = self.retention.erasure.get() else {
            return MemberProving::NoProver;
        };
        if runtime.config.is_none() {
            return MemberProving::NoProver;
        }
        let mut current = runtime.for_leader.lock();
        if let Some((held, outcome)) = current.as_ref() {
            let ended = outcome.borrow().clone();
            match (held == &step, ended) {
                (true, Some(ended)) => {
                    *current = None;
                    return MemberProving::Done(ended);
                }
                (true, None) => return MemberProving::Proving,
                (false, None) => return MemberProving::Busy,
                // Its leader went; what it proved is stored.
                (false, Some(_)) => {}
            }
        }
        let (tx, rx) = tokio::sync::watch::channel(None);
        *current = Some((step.clone(), rx));
        let fluree = Arc::clone(self);
        tokio::spawn(async move {
            let _ = tx.send(Some(fluree.prove_handed(&step, node).await));
        });
        MemberProving::Proving
    }
}
