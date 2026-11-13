//! A leader's proving steps, run on its other members: see
//! [`ProvingMembers`].

use std::collections::BTreeSet;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use fluree_db_api::retention::erasure::{MemberProving, ProofStep, ProvingMembers, StepOutcome};
use fluree_db_api::Fluree;
use fluree_db_core::ledger_id::split_ledger_id;

use super::{ForwardingConfig, RaftNameService, ERASURE_PROVE_PATH};
use crate::raft::ownership::owner;
use crate::raft::state_machine::RefKey;
use crate::raft::{ClusterNode, NodeId};

/// How often the leader asks a member proving its step how it is going.
const POLL: Duration = Duration::from_secs(1);

/// How long a member asked to prove a step may go unheard before the leader
/// passes it over. One cut off from the leader that long may still be
/// proving it, so another can prove it again.
const SILENCE: Duration = Duration::from_secs(60);

/// What a member answered for a step.
enum Asked {
    Ended(StepOutcome),
    Busy,
    /// It has no prover, runs an earlier release, or went unheard.
    Cannot,
}

/// Why an ask went unanswered.
enum Unanswered {
    /// It did not reach the member's route, or the route refused it: the
    /// member did not take the step.
    Refused,
    /// The answer was lost: the member may have taken the step.
    Lost,
}

impl RaftNameService {
    /// Prove the steps this member's leader hands it with `fluree`.
    pub fn prove_with(&self, fluree: &Arc<Fluree>) {
        let _ = self.prover.set(Arc::downgrade(fluree));
    }

    pub(super) fn prover(&self) -> Option<Arc<Fluree>> {
        self.prover.get().and_then(Weak::upgrade)
    }

    /// The other members to ask to prove `step`, in the order to ask them.
    async fn provers_for(&self, step: &ProofStep) -> Vec<(NodeId, ClusterNode)> {
        let (me, members, voters) = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            let membership = metrics.membership_config.membership();
            (
                metrics.id,
                membership
                    .nodes()
                    .map(|(id, node)| (*id, node.clone()))
                    .collect::<Vec<_>>(),
                membership.voter_ids().collect::<Vec<_>>(),
            )
        };
        let owners: BTreeSet<NodeId> = {
            let state = self.state.read().await;
            let eligible: Vec<NodeId> = match state.worker_eligible_voters.is_empty() {
                true => voters,
                false => state.worker_eligible_voters.iter().copied().collect(),
            };
            step.branches
                .iter()
                .filter_map(|branch| split_ledger_id(branch).ok())
                .filter_map(|(name, branch)| owner(&RefKey::new(&name, &branch), &eligible))
                .collect()
        };
        preference(me, members, &owners)
    }

    /// Ask the member at `node` to prove `step`, and wait for it to end.
    async fn prove_on(
        &self,
        forwarding: &ForwardingConfig,
        node: &ClusterNode,
        loopback: bool,
        step: &ProofStep,
    ) -> Asked {
        if !crate::raft::forward::is_valid_leader_url(&node.raft_addr, loopback) {
            return Asked::Cannot;
        }
        let target = format!(
            "{}{}",
            node.raft_addr.trim_end_matches('/'),
            ERASURE_PROVE_PATH
        );
        let mut heard = Instant::now();
        let mut took = false;
        loop {
            match ask(forwarding, &target, step).await {
                Ok(MemberProving::Done(outcome)) => return Asked::Ended(outcome),
                Ok(MemberProving::Proving) => {
                    took = true;
                    heard = Instant::now();
                }
                Ok(MemberProving::Busy) => return Asked::Busy,
                Ok(MemberProving::NoProver) => return Asked::Cannot,
                Err(Unanswered::Refused) if !took => return Asked::Cannot,
                Err(_) if heard.elapsed() > SILENCE => return Asked::Cannot,
                Err(_) => {}
            }
            tokio::time::sleep(POLL).await;
        }
    }
}

/// One ask of a member at `target` to prove `step`, or how far it got.
async fn ask(
    forwarding: &ForwardingConfig,
    target: &str,
    step: &ProofStep,
) -> Result<MemberProving, Unanswered> {
    let resp = forwarding
        .http_client
        .post(target)
        .timeout(forwarding.request_timeout)
        .json(step)
        .send()
        .await
        .map_err(|e| match e.is_connect() {
            true => Unanswered::Refused,
            false => Unanswered::Lost,
        })?;
    if !resp.status().is_success() {
        return Err(Unanswered::Refused);
    }
    resp.json().await.map_err(|_| Unanswered::Lost)
}

/// The members other than `me`, in the order to ask them to prove: first
/// those that own none of `owners`' branches, whose transactions they
/// stage, then the owners.
fn preference(
    me: NodeId,
    members: impl IntoIterator<Item = (NodeId, ClusterNode)>,
    owners: &BTreeSet<NodeId>,
) -> Vec<(NodeId, ClusterNode)> {
    let mut others: Vec<_> = members.into_iter().filter(|(id, _)| *id != me).collect();
    others.sort_by_key(|(id, _)| (owners.contains(id), *id));
    others
}

#[async_trait]
impl ProvingMembers for RaftNameService {
    async fn prove(&self, step: &ProofStep, assigned: Option<u64>) -> Option<StepOutcome> {
        let forwarding = self.forwarding.as_ref()?;
        let mut candidates = self.provers_for(step).await;
        if let Some(at) = candidates.iter().position(|(id, _)| Some(*id) == assigned) {
            let first = candidates.remove(at);
            candidates.insert(0, first);
        }
        let loopback = {
            let me = self.raft.metrics().borrow().id;
            self.raft
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .get_node(&me)
                .is_some_and(|node| crate::raft::forward::self_addr_is_loopback(&node.raft_addr))
        };
        let mut busy = false;
        for (_, node) in &candidates {
            match self.prove_on(forwarding, node, loopback, step).await {
                Asked::Ended(outcome) => return Some(outcome),
                Asked::Busy => busy = true,
                Asked::Cannot => {}
            }
        }
        busy.then(|| {
            StepOutcome::Retry(
                "every other member with a prover is proving another step".to_string(),
            )
        })
    }

    async fn deposed(&self) {
        let mut metrics = self.raft.metrics();
        let (me, term) = {
            let current = metrics.borrow();
            (current.id, current.current_term)
        };
        loop {
            {
                let current = metrics.borrow_and_update();
                if current.current_leader != Some(me) || current.current_term != term {
                    return;
                }
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
    }
}

/// A step this member's leader hands it: where it is with it.
pub(super) async fn handle_erasure_prove(
    State(ns): State<Arc<RaftNameService>>,
    body: axum::body::Bytes,
) -> Response {
    let step: ProofStep = match serde_json::from_slice(&body) {
        Ok(step) => step,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("not a proving step: {e}")).into_response()
        }
    };
    let answer = match ns.prover() {
        Some(fluree) => fluree.prove_for_leader(step, ns.raft.metrics().borrow().id),
        None => MemberProving::NoProver,
    };
    axum::Json(answer).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: NodeId) -> (NodeId, ClusterNode) {
        (
            id,
            ClusterNode::new(
                format!("http://10.0.0.{id}:9090/raft"),
                format!("http://10.0.0.{id}:8090"),
            ),
        )
    }

    fn order(me: NodeId, members: &[NodeId], owners: &[NodeId]) -> Vec<NodeId> {
        preference(
            me,
            members.iter().map(|id| node(*id)),
            &owners.iter().copied().collect(),
        )
        .into_iter()
        .map(|(id, _)| id)
        .collect()
    }

    #[test]
    fn a_leader_asks_members_owning_none_of_the_branches_first() {
        // A follower owns the branch: the third member is asked first.
        assert_eq!(order(1, &[1, 2, 3], &[2]), vec![3, 2]);
        // The leader owns it: either follower, then none is left.
        assert_eq!(order(1, &[1, 2, 3], &[1]), vec![2, 3]);
        // A learner proves as well as a voter.
        assert_eq!(order(1, &[1, 2, 3, 7], &[2, 3]), vec![7, 2, 3]);
        assert!(order(1, &[1], &[1]).is_empty());
    }
}
