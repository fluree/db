//! Advancing erasure jobs: one proving step at a time in the background,
//! verified and stored as it lands; publishing once a job's commits are
//! proved and indexed.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;
use std::time::Instant;

use fluree_db_core::commit::codec::read_commit_envelope;
use fluree_db_core::{
    collect_first_parent_cids, load_commit_envelope_by_id, ContentId, ContentKind, LedgerId,
};
use fluree_db_erasure::{ProveRequest, ProverError};
use fluree_db_nameservice::NsRecord;
use futures::FutureExt;

use super::activation::{ErasureActivator, HOLD};
use super::index::{Artifact, ErasedCommit, ErasureStep, REFRESH};
use super::{
    ClusterPublication, CommitErasure, ErasureJob, ErasureStage, ProofStep, ProvedStep, StepOutcome,
};
use crate::retention::lineage::span_ends;
use crate::retention::registry;
use crate::retention::removal::OccurrenceRef;
use crate::retention::worker::now_ms;
use crate::{ApiError, Fluree, IndexingMode, Result};

/// Erased sets whose branch heads' reach is kept at once.
const REACHED_SETS: usize = 64;

/// The longest wait before a failed step is tried again (ms).
const MAX_RETRY_MS: i64 = 3_600_000;

/// Clears the in-flight flag however the step ends.
struct InFlight<'a>(&'a AtomicBool);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// A pending job and the branch whose registration holds it.
struct Held {
    branch: String,
    job: ErasureJob,
}

/// The next proving step, and the jobs ready to publish.
#[derive(Default)]
struct Work {
    prove: Option<(String, String, String)>,
    publish: Vec<Held>,
}

/// What a job's commit still needs, given the commit's published erasure.
#[derive(Debug, PartialEq, Eq)]
enum Need {
    /// The published erasure removes every slot the request asks for.
    Covered,
    /// This request's step, proved on the published erasure, to publish.
    Publish,
    /// To prove: the requested slots the published erasure still holds.
    Prove(Vec<u32>),
}

/// What [`Fluree::prepare_publication`] found.
enum Prepared {
    /// Not ready: the job waits, or was given more to prove.
    Waiting,
    /// The published erasures already remove what the job asks for.
    Covered,
    /// Ready to publish `erased` over `holders`, the branches as found.
    Ready {
        holders: Vec<NsRecord>,
        erased: Vec<ErasedCommit>,
    },
}

fn need(task: &CommitErasure, held: Option<&ErasedCommit>) -> Need {
    let done = held.map(ErasedCommit::redacted_slots).unwrap_or_default();
    let slots: Vec<u32> = task
        .slots
        .iter()
        .copied()
        .filter(|s| !done.contains(s))
        .collect();
    if slots.is_empty() {
        return Need::Covered;
    }
    let base = held.and_then(|h| h.steps.last()).map(|s| &s.replacement);
    match &task.proved {
        Some(p) if p.base.as_ref() == base && p.slots == slots => Need::Publish,
        _ => Need::Prove(slots),
    }
}

/// Join `added` to `job`, to be proved before it publishes.
fn add_commits(job: &mut ErasureJob, added: &[CommitErasure]) {
    for new in added {
        for slot in &new.slots {
            job.added.push(OccurrenceRef {
                commit: new.commit.clone(),
                slot: *slot,
            });
        }
        match job.commits.iter_mut().find(|c| c.commit == new.commit) {
            Some(task) => {
                task.slots.extend(&new.slots);
                task.slots.sort_unstable();
                task.slots.dedup();
                for b in &new.branches {
                    if !task.branches.contains(b) {
                        task.branches.push(b.clone());
                    }
                }
            }
            None => job.commits.push(new.clone()),
        }
    }
    job.stage = ErasureStage::Proving;
}

/// How a proving step ended.
enum Stepped {
    Proved,
    /// Nothing was left to prove.
    Nothing,
    /// The job failed: the prover refused the step.
    Refused,
    /// To be tried again.
    Failed,
}

/// What [`Fluree::run_erasure_pass`] did.
#[derive(Clone, Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErasurePassReport {
    /// Steps proved, verified and stored.
    pub proved: usize,
    /// Steps that failed (refused, or to be retried).
    pub failed: usize,
    /// Jobs whose erasures are now published.
    pub published: usize,
    /// Jobs waiting for an index of their commits.
    pub waiting_for_index: usize,
}

impl Fluree {
    /// The pending jobs of `ledgers`, oldest first; one moved to indexing
    /// when its last step has landed.
    async fn erasure_work(&self, ledgers: &[String]) -> Work {
        let mut work = Work::default();
        let Ok(ns) = self.publisher() else {
            return work;
        };
        let mut held = Vec::new();
        for branch in ledgers {
            let Ok((_, Some(stored))) = registry::read_stored(ns, branch).await else {
                continue;
            };
            // Most passes find nothing pending: decided without the jobs'
            // chunks being read.
            if !registry::shell(&stored).is_ok_and(|s| s.erasures.iter().any(ErasureJob::pending)) {
                continue;
            }
            if let Ok(registration) = registry::resolve(&stored, self.registry_objects()).await {
                held.extend(
                    registration
                        .erasures
                        .into_iter()
                        .filter(ErasureJob::pending)
                        .map(|job| Held {
                            branch: branch.clone(),
                            job,
                        }),
                );
            }
        }
        // Oldest first: a commit's later erasure builds on its earlier one,
        // which publishes first.
        held.sort_by_key(|h| h.job.accepted_ms);
        let mut ahead: HashSet<String> = HashSet::new();
        let now = now_ms();
        for Held { branch, mut job } in held {
            let blocked = job.commits.iter().any(|c| ahead.contains(&c.commit));
            ahead.extend(job.commits.iter().map(|c| c.commit.clone()));
            if blocked {
                continue;
            }
            let needs = match self.needs(&job).await {
                Ok(needs) => needs,
                Err(e) => {
                    tracing::warn!(request = %job.request_id, error = %e, "erasure: not advanced");
                    continue;
                }
            };
            let next = job
                .commits
                .iter()
                .zip(&needs)
                .find(|(_, n)| matches!(n, Need::Prove(_)))
                .map(|(c, _)| c.commit.clone());
            let stage = match next {
                Some(_) => ErasureStage::Proving,
                None => ErasureStage::Indexing,
            };
            if job.stage != stage {
                job.stage = stage.clone();
                let _ = self
                    .update_job(&branch, &job.request_id, |j| j.stage = stage.clone())
                    .await;
            }
            match next {
                None => work.publish.push(Held { branch, job }),
                Some(commit) => {
                    if work.prove.is_none() && !job.retry_at_ms.is_some_and(|at| at > now) {
                        work.prove = Some((branch, job.request_id.clone(), commit));
                    }
                }
            }
        }
        work
    }

    /// What each of `job`'s commits still needs.
    async fn needs(&self, job: &ErasureJob) -> Result<Vec<Need>> {
        let runtime = self.erasure_runtime()?;
        let mut needs = Vec::with_capacity(job.commits.len());
        for task in &job.commits {
            let held = runtime.index.erased(&commit_id(&task.commit)?).await?;
            needs.push(need(task, held.as_ref()));
        }
        Ok(needs)
    }

    /// Advance the erasure jobs of `ledgers`: publish what is proved and
    /// indexed, and start the next proving step unless one is running.
    /// Never waits for a proof.
    pub(crate) async fn erasure_sweep(
        &self,
        fluree: &Weak<Fluree>,
        ledgers: &[String],
        purge_grace: std::time::Duration,
    ) {
        let Some(runtime) = self.retention.erasure.get() else {
            return;
        };
        if let Err(e) = self.resume_received_erasures().await {
            tracing::warn!(error = %e, "erasure: received erasures not resumed");
        }
        self.purge_sweep(fluree, ledgers, purge_grace).await;
        let Some(config) = runtime.config.as_ref() else {
            return;
        };
        let work = self.erasure_work(ledgers).await;
        for Held { branch, job } in &work.publish {
            if let Err(e) = self.publish_when_indexed(branch, job).await {
                tracing::warn!(request = %job.request_id, error = %e, "erasure: not published");
            }
        }
        self.record_effective(ledgers).await;
        let Some((branch, request_id, commit)) = work.prove else {
            return;
        };
        if runtime
            .in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let fluree = fluree.clone();
        let retry_ms = config.retry_ms;
        tokio::spawn(async move {
            let Some(fluree) = fluree.upgrade() else {
                return;
            };
            let Some(runtime) = fluree.retention.erasure.get() else {
                return;
            };
            let _clear = InFlight(&runtime.in_flight);
            let step = fluree.step_and_record(&branch, &request_id, &commit, retry_ms);
            match runtime.members.get() {
                // A cluster's step ends with the leadership that began it.
                Some(members) => {
                    tokio::select! {
                        _ = step => {}
                        () = members.deposed() => {}
                    }
                }
                None => {
                    step.await;
                }
            }
            fluree.retention.nudge.notify_one();
        });
    }

    /// Purge, in the background and one pass at a time, the ledgers among
    /// `ledgers` with published erasures to purge, except those whose last
    /// pass was blocked within the hour.
    async fn purge_sweep(
        &self,
        fluree: &Weak<Fluree>,
        ledgers: &[String],
        grace: std::time::Duration,
    ) {
        let (Some(runtime), Ok(ns)) = (self.retention.erasure.get(), self.publisher()) else {
            return;
        };
        // On a cluster a job is purged once every member installed it.
        let clustered = self.collected_by_cluster();
        let now = now_ms();
        let mut due = std::collections::BTreeSet::new();
        for branch in ledgers {
            if let Ok((_, Some(registration))) = registry::read_stored(ns, branch)
                .await
                .and_then(|(c, stored)| Ok((c, stored.map(|s| registry::shell(&s)).transpose()?)))
            {
                let purge = registration
                    .erasures
                    .iter()
                    .any(|j| j.purge_ready(clustered) && j.retry_at_ms.is_none_or(|at| at <= now));
                if let (true, Ok(id)) = (purge, LedgerId::parse(branch)) {
                    due.insert(id.ledger_name().to_string());
                }
            }
        }
        if due.is_empty()
            || runtime
                .purge_in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        let fluree = fluree.clone();
        tokio::spawn(async move {
            let Some(fluree) = fluree.upgrade() else {
                return;
            };
            let Some(runtime) = fluree.retention.erasure.get() else {
                return;
            };
            let _clear = InFlight(&runtime.purge_in_flight);
            let passes = async {
                for name in due {
                    if let Err(e) = fluree.run_purge_pass(&name, grace).await {
                        tracing::warn!(ledger = %name, error = %e, "erasure: purge pass failed");
                    }
                }
            };
            match runtime.members.get() {
                // A cluster's passes end with the leadership that began them;
                // the next leader resumes them.
                Some(members) => {
                    tokio::select! {
                        () = passes => {}
                        () = members.deposed() => {}
                    }
                }
                None => passes.await,
            }
        });
    }

    /// Advance the erasure jobs of every branch of `ledger_id`'s ledger now,
    /// waiting for each proof, until none is ready to prove or publish.
    pub async fn run_erasure_pass(&self, ledger_id: &str) -> Result<ErasurePassReport> {
        let runtime = self.erasure_runtime()?;
        let config = runtime
            .config
            .as_ref()
            .ok_or_else(|| ApiError::http(501, "no prover is configured"))?;
        let name = fluree_db_core::LedgerId::parse(ledger_id)?;
        let ledgers: Vec<String> = self
            .nameservice()
            .list_branches(name.name())
            .await?
            .into_iter()
            .filter(|r| !r.retracted)
            .map(|r| r.ledger_id.to_string())
            .collect();
        let mut report = ErasurePassReport::default();
        // Each round proves one step or publishes; a bounded number of them.
        for _ in 0..1_000 {
            let work = self.erasure_work(&ledgers).await;
            let mut progressed = false;
            for Held { branch, job } in &work.publish {
                self.publish_when_indexed(branch, job).await?;
                match self.job(branch, &job.request_id).await?.stage {
                    ErasureStage::Published => {
                        report.published += 1;
                        progressed = true;
                    }
                    // Joined by occurrences, or a step to prove again.
                    ErasureStage::Proving => progressed = true,
                    _ => report.waiting_for_index += 1,
                }
            }
            if let Some((branch, request_id, commit)) = work.prove {
                if runtime
                    .in_flight
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    break;
                }
                let stepped = self
                    .step_and_record(&branch, &request_id, &commit, config.retry_ms)
                    .await;
                runtime.in_flight.store(false, Ordering::Release);
                match stepped {
                    Stepped::Proved => {
                        report.proved += 1;
                        progressed = true;
                    }
                    Stepped::Nothing => progressed = true,
                    Stepped::Refused => {
                        report.failed += 1;
                        progressed = true;
                    }
                    Stepped::Failed => report.failed += 1,
                }
            }
            if !progressed {
                break;
            }
            report.waiting_for_index = 0;
        }
        self.record_effective(&ledgers).await;
        Ok(report)
    }

    /// Prove one step and record its outcome.
    async fn step_and_record(
        &self,
        branch: &str,
        request_id: &str,
        commit: &str,
        retry_ms: i64,
    ) -> Stepped {
        let outcome = AssertUnwindSafe(self.prove_step(branch, request_id, commit))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(StepError::Retry(ApiError::internal("the step panicked"))));
        let stepped = match &outcome {
            Ok(Some(_)) => Stepped::Proved,
            Ok(None) => Stepped::Nothing,
            Err(StepError::Refused(_)) => Stepped::Refused,
            Err(StepError::Retry(_)) => Stepped::Failed,
        };
        if let Err(e) = self
            .record_step(branch, request_id, commit, outcome, retry_ms)
            .await
        {
            tracing::warn!(request = %request_id, error = %e, "erasure: step not recorded");
        }
        stepped
    }

    /// Prove the step of `request_id` on `commit` over its published
    /// erasure, verify it with every earlier step, and store its artifacts.
    /// `None` when the published erasure leaves nothing to prove. On a
    /// cluster another member proves it when one can, and its result is
    /// verified here.
    async fn prove_step(
        &self,
        branch: &str,
        request_id: &str,
        commit: &str,
    ) -> std::result::Result<Option<ProvedStep>, StepError> {
        let Some(step) = self.prepare_step(branch, request_id, commit).await? else {
            return Ok(None);
        };
        if let Some(proved) = self.stored_proof(&step).await? {
            return Ok(Some(proved));
        }
        let runtime = self.erasure_runtime().map_err(StepError::Retry)?;
        if let Some(members) = runtime.members.get() {
            let handed = ProofStep {
                branch: branch.to_string(),
                request_id: request_id.to_string(),
                commit: commit.to_string(),
                branches: step.branches.clone(),
            };
            // This host proves it for an earlier leader: it ends here.
            let here = runtime
                .for_leader
                .lock()
                .as_ref()
                .is_some_and(|(held, _)| held == &handed);
            if here {
                return self.prove_prepared(&step).await.map(Some);
            }
            let assigned = runtime
                .index
                .proving(&step.key())
                .await
                .map_err(StepError::Retry)?;
            match members.prove(&handed, assigned).await {
                Some(StepOutcome::Proved(proved)) => {
                    self.verify_proved(&step, &proved).await?;
                    return Ok(Some(proved));
                }
                Some(StepOutcome::Nothing) => return Ok(None),
                Some(StepOutcome::Refused(why)) => return Err(StepError::Refused(why)),
                Some(StepOutcome::Retry(why)) => {
                    return Err(StepError::Retry(ApiError::internal(why)))
                }
                None => {}
            }
        }
        self.prove_prepared(&step).await.map(Some)
    }

    /// [`Self::prove_step`] with this member's own prover, for its leader:
    /// over the erasures installed here, once they are the cluster's. The
    /// member, `node`, is recorded as proving it, for a leader asking again.
    pub(super) async fn prove_handed(&self, step: &ProofStep, node: u64) -> StepOutcome {
        if let Err(e) = self.confirm_history_current(&step.branch).await {
            return StepOutcome::Retry(e.to_string());
        }
        AssertUnwindSafe(self.prove_here(step, node))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(StepError::Retry(ApiError::internal("the step panicked"))))
            .into()
    }

    async fn prove_here(
        &self,
        step: &ProofStep,
        node: u64,
    ) -> std::result::Result<Option<ProvedStep>, StepError> {
        let Some(prepared) = self
            .prepare_step(&step.branch, &step.request_id, &step.commit)
            .await?
        else {
            return Ok(None);
        };
        if let Some(proved) = self.stored_proof(&prepared).await? {
            return Ok(Some(proved));
        }
        self.erasure_runtime()
            .map_err(StepError::Retry)?
            .index
            .put_proving(&prepared.key(), node)
            .await
            .map_err(StepError::Retry)?;
        self.prove_prepared(&prepared).await.map(Some)
    }

    /// What the step of `request_id` on `commit` proves over the commit's
    /// published erasure; `None` when that leaves nothing to prove.
    async fn prepare_step(
        &self,
        branch: &str,
        request_id: &str,
        commit: &str,
    ) -> std::result::Result<Option<PreparedStep>, StepError> {
        let runtime = self.erasure_runtime().map_err(StepError::Retry)?;
        let job = self
            .job(branch, request_id)
            .await
            .map_err(StepError::Retry)?;
        let task = job
            .commits
            .iter()
            .find(|c| c.commit == commit)
            .ok_or_else(|| StepError::Retry(ApiError::internal("the step is gone")))?;
        let cid: ContentId = commit
            .parse()
            .map_err(|_| StepError::Refused("not a commit id".into()))?;
        let prior = runtime.index.erased(&cid).await.map_err(StepError::Retry)?;
        let Need::Prove(slots) = need(task, prior.as_ref()) else {
            return Ok(None);
        };
        let base = prior
            .as_ref()
            .and_then(|p| p.steps.last())
            .map(|s| s.replacement.clone());
        Ok(Some(PreparedStep {
            cid,
            slots,
            base,
            prior,
            ledger_id: task.ledger_id.clone(),
            branches: task.branches.clone(),
            signatures: job.signatures,
        }))
    }

    /// A stored result of `step`, proved before by this member or another,
    /// verified here.
    async fn stored_proof(
        &self,
        step: &PreparedStep,
    ) -> std::result::Result<Option<ProvedStep>, StepError> {
        let runtime = self.erasure_runtime().map_err(StepError::Retry)?;
        let Some(proved) = runtime
            .index
            .proved(&step.key())
            .await
            .map_err(StepError::Retry)?
        else {
            return Ok(None);
        };
        self.verify_proved(step, &proved).await?;
        Ok(Some(proved))
    }

    /// Prove `step` with this member's prover, verify it, and store its
    /// artifacts and result: one proof at a time on this host, whether for
    /// itself or for its leader.
    async fn prove_prepared(
        &self,
        step: &PreparedStep,
    ) -> std::result::Result<ProvedStep, StepError> {
        let runtime = self.erasure_runtime().map_err(StepError::Retry)?;
        let config = runtime
            .config
            .as_ref()
            .ok_or_else(|| StepError::Retry(ApiError::internal("no prover")))?;
        let _proving = runtime.proving.lock().await;
        // The proof this one waited for may have been this step's.
        if let Some(proved) = self.stored_proof(step).await? {
            return Ok(proved);
        }
        let input = self
            .commit_bytes(&step.branches[0], &step.cid)
            .await
            .map_err(StepError::Retry)?;
        let request = ProveRequest {
            input,
            removed_slots: step.slots.clone(),
            ledger_id: step.ledger_id.clone(),
            signatures: step.signatures.into(),
        };
        let started = Instant::now();
        let response = config.prover.prove(&request).await.map_err(|e| match e {
            ProverError::Refused(why) => StepError::Refused(why),
            other => StepError::Retry(ApiError::internal(other.to_string())),
        })?;
        let prove_ms = started.elapsed().as_millis() as u64;
        self.verify_chain(step, response.receipt.clone(), response.replacement.clone())
            .await?;

        let replacement_id = ContentId::new(ContentKind::Commit, &response.replacement);
        runtime
            .index
            .put_artifact(Artifact::Replacement, &response.replacement)
            .await
            .map_err(StepError::Retry)?;
        let proof = runtime
            .index
            .put_artifact(Artifact::Proof, &response.receipt)
            .await
            .map_err(StepError::Retry)?;
        let proved = ProvedStep {
            slots: step.slots.clone(),
            base: step.base.clone(),
            replacement: replacement_id.to_string(),
            proof,
            input_bytes: request.input.len() as u64,
            replacement_bytes: response.replacement.len() as u64,
            user_cycles: response.user_cycles,
            segments: response.segments,
            prove_ms,
        };
        runtime
            .index
            .put_proved(&step.key(), &proved)
            .await
            .map_err(StepError::Retry)?;
        Ok(proved)
    }

    /// Verify `proved`, stored by whichever member proved it, as a proof of
    /// `step`.
    async fn verify_proved(
        &self,
        step: &PreparedStep,
        proved: &ProvedStep,
    ) -> std::result::Result<(), StepError> {
        if proved.slots != step.slots || proved.base != step.base {
            return Err(StepError::Retry(ApiError::internal(
                "the step was proved over another erasure",
            )));
        }
        let runtime = self.erasure_runtime().map_err(StepError::Retry)?;
        let replacement: ContentId = proved
            .replacement
            .parse()
            .map_err(|_| StepError::Retry(ApiError::internal("not a replacement id")))?;
        let replacement = runtime
            .index
            .artifact(Artifact::Replacement, &replacement.address_digest())
            .await
            .map_err(StepError::Retry)?;
        let receipt = runtime
            .index
            .artifact(Artifact::Proof, &proved.proof)
            .await
            .map_err(StepError::Retry)?;
        self.verify_chain(step, receipt, replacement).await
    }

    /// Verify `receipt`, proving `replacement` of `step`, with every earlier
    /// step of the commit.
    async fn verify_chain(
        &self,
        step: &PreparedStep,
        receipt: Vec<u8>,
        replacement: Vec<u8>,
    ) -> std::result::Result<(), StepError> {
        let runtime = self.erasure_runtime().map_err(StepError::Retry)?;
        let config = runtime
            .config
            .as_ref()
            .ok_or_else(|| StepError::Retry(ApiError::internal("no prover")))?;
        let mut receipts = Vec::new();
        let mut authorized = Vec::new();
        for earlier in step.prior.iter().flat_map(|p| &p.steps) {
            receipts.push(
                runtime
                    .index
                    .artifact(Artifact::Proof, &earlier.proof)
                    .await
                    .map_err(StepError::Retry)?,
            );
            authorized.push(earlier.slots.clone());
        }
        receipts.push(receipt);
        authorized.push(step.slots.clone());
        let verifier = config.verifier.clone();
        let (original, ledger_id) = (step.cid.to_bytes(), step.ledger_id.clone());
        tokio::task::spawn_blocking(move || {
            verifier.verify_chain(&receipts, &replacement, &original, &ledger_id, &authorized)
        })
        .await
        .map_err(|e| StepError::Retry(ApiError::internal(e.to_string())))?
        .map_err(|e| {
            StepError::Retry(ApiError::internal(format!(
                "the prover's proof does not verify: {e}"
            )))
        })?;
        Ok(())
    }

    /// Record a step's outcome on its job.
    async fn record_step(
        &self,
        branch: &str,
        request_id: &str,
        commit: &str,
        outcome: std::result::Result<Option<ProvedStep>, StepError>,
        retry_ms: i64,
    ) -> Result<()> {
        self.update_job(branch, request_id, |job| match &outcome {
            Ok(step) => {
                if let Some(step) = step {
                    if let Some(task) = job.commits.iter_mut().find(|c| c.commit == commit) {
                        task.proved = Some(step.clone());
                    }
                }
                job.last_error = None;
                job.retry_at_ms = None;
            }
            Err(StepError::Refused(why)) => {
                job.stage = ErasureStage::Failed;
                job.last_error = Some(format!("commit {commit}: {why}"));
            }
            Err(StepError::Retry(e)) => {
                job.retries += 1;
                let wait = retry_ms
                    .saturating_mul(1 << job.retries.saturating_sub(1).min(16))
                    .min(MAX_RETRY_MS);
                job.retry_at_ms = Some(now_ms() + wait);
                job.last_error = Some(format!("commit {commit}: {e}"));
            }
        })
        .await
    }

    /// Publish `job` once every branch holding its commits has indexed them;
    /// until then, ask for the index builds. Complete once the published
    /// erasure of each commit removes every slot the job asks for.
    async fn publish_when_indexed(&self, branch: &str, job: &ErasureJob) -> Result<()> {
        let runtime = self.erasure_runtime()?;
        if let Some(activator) = runtime.activator.get() {
            if !self
                .activate_when_indexed(activator.as_ref(), branch, job)
                .await?
            {
                return Ok(());
            }
        } else {
            // No branch is made between the check and the publication.
            let admission = runtime.admission.lock().await;
            match self.prepare_publication(branch, job).await? {
                Prepared::Waiting => return Ok(()),
                Prepared::Covered => {}
                Prepared::Ready { holders, erased } => {
                    self.store_authorization(branch, job).await?;
                    runtime.index.publish(erased).await?;
                    for holder in &holders {
                        self.announce_refresh(&holder.ledger_id).await;
                    }
                }
            }
            drop(admission);
        }
        if self.needs(job).await?.iter().any(|n| *n != Need::Covered) {
            return Err(ApiError::internal(format!(
                "erasure {} published without removing every requested slot",
                job.request_id
            )));
        }
        // Another instance on this storage reads the replacements once it
        // lists the maps again; it has by the time this reports, whether
        // the maps were written now or before a restart.
        tokio::time::sleep(REFRESH * 2).await;
        self.update_job(branch, &job.request_id, |j| {
            j.stage = ErasureStage::Published;
            j.published_ms = Some(now_ms());
            j.last_error = None;
        })
        .await
    }

    /// [`Self::publish_when_indexed`] on a cluster member: activate a
    /// manifest of the prepared erasures through `activator`. A holder that
    /// moved or was added since preparation refuses the activation; the
    /// holders' heads are then held while it is prepared once more. Whether
    /// the job is published.
    async fn activate_when_indexed(
        &self,
        activator: &dyn ErasureActivator,
        branch: &str,
        job: &ErasureJob,
    ) -> Result<bool> {
        let mut held = None;
        let published = self
            .activation_attempts(activator, branch, job, &mut held)
            .await;
        if let Some(holders) = held {
            if let Err(e) = activator.release(&holders).await {
                tracing::warn!(request = %job.request_id, error = %e, "erasure: hold not released; it lapses");
            }
        }
        published
    }

    async fn activation_attempts(
        &self,
        activator: &dyn ErasureActivator,
        branch: &str,
        job: &ErasureJob,
        held: &mut Option<Vec<NsRecord>>,
    ) -> Result<bool> {
        let runtime = self.erasure_runtime()?;
        loop {
            // Holders are found after the version is read: one added since
            // refuses the activation.
            let Some(holder_set) = activator.holder_set().await? else {
                return Ok(false);
            };
            let admission = runtime.admission.lock().await;
            let (holders, erased) = match self.prepare_publication(branch, job).await? {
                Prepared::Waiting => return Ok(false),
                Prepared::Covered => return self.covered_in_cluster(activator, branch, job).await,
                Prepared::Ready { holders, erased } => (holders, erased),
            };
            self.store_authorization(branch, job).await?;
            let manifest = runtime.index.put_manifest(&erased).await?;
            // Recorded before activating: should the activation apply and
            // its record here be lost, the next pass finds it applied.
            self.record_publication(branch, job, &manifest, None)
                .await?;
            if let Some(activation) = activator.activate(&manifest, holder_set, &holders).await? {
                // For the instances that only list the map objects.
                runtime.index.publish(erased).await?;
                self.record_publication(branch, job, &manifest, Some(activation))
                    .await?;
                return Ok(true);
            }
            drop(admission);
            if held.is_some() {
                return Ok(false);
            }
            activator.hold(&holders, HOLD).await?;
            *held = Some(holders);
        }
    }

    /// A cluster job every commit of which is covered: by its own
    /// activation, or by others'. One whose activation applied before it was
    /// recorded here has its map object written now.
    async fn covered_in_cluster(
        &self,
        activator: &dyn ErasureActivator,
        branch: &str,
        job: &ErasureJob,
    ) -> Result<bool> {
        let Some(ClusterPublication {
            manifest,
            activation: None,
            ..
        }) = &job.cluster
        else {
            return Ok(true);
        };
        if let Some(activation) = activator.applied(manifest).await {
            let runtime = self.erasure_runtime()?;
            runtime
                .index
                .publish(runtime.index.manifest(manifest).await?)
                .await?;
            self.record_publication(branch, job, manifest, Some(activation))
                .await?;
        }
        Ok(true)
    }

    async fn record_publication(
        &self,
        branch: &str,
        job: &ErasureJob,
        manifest: &str,
        activation: Option<u64>,
    ) -> Result<()> {
        self.update_job(branch, &job.request_id, |j| {
            j.cluster = Some(ClusterPublication {
                manifest: manifest.to_string(),
                activation,
                effective_ms: None,
            });
        })
        .await
    }

    /// Record, on the cluster jobs of `ledgers` whose activation every
    /// member has installed, when it became effective for serving.
    async fn record_effective(&self, ledgers: &[String]) {
        let (Some(runtime), Ok(ns)) = (self.retention.erasure.get(), self.publisher()) else {
            return;
        };
        let Some(activator) = runtime.activator.get() else {
            return;
        };
        for branch in ledgers {
            let Ok((_, Some(stored))) = registry::read_stored(ns, branch).await else {
                continue;
            };
            let Ok(registration) = registry::shell(&stored) else {
                continue;
            };
            for job in &registration.erasures {
                let Some(ClusterPublication {
                    activation: Some(activation),
                    effective_ms: None,
                    ..
                }) = &job.cluster
                else {
                    continue;
                };
                if activator.installed_everywhere(*activation).await {
                    let now = now_ms();
                    if let Err(e) = self
                        .update_job(branch, &job.request_id, |j| {
                            if let Some(cluster) = &mut j.cluster {
                                cluster.effective_ms = Some(now);
                            }
                        })
                        .await
                    {
                        tracing::warn!(request = %job.request_id, error = %e, "erasure: effective state not recorded");
                    }
                }
            }
        }
    }

    /// Whether `job` can publish, and over what. Asks for the index builds
    /// its holders lack, and joins the commits ending its spans that it
    /// lacks or proves again those proved on an erasure published since.
    async fn prepare_publication(&self, branch: &str, job: &ErasureJob) -> Result<Prepared> {
        let runtime = self.erasure_runtime()?;
        let Some(holders) = self.holders(branch, job).await? else {
            // Dropped: its branches, and what their indexes hold, can be
            // restored, so the gate waits until they are.
            return Ok(Prepared::Waiting);
        };
        let lagging = self.lagging_branches(&holders, job).await?;
        if !lagging.is_empty() {
            match &self.indexing_mode {
                IndexingMode::Background(handle) => {
                    for (b, t) in &lagging {
                        if let Ok(id) = fluree_db_core::LedgerId::parse(b) {
                            drop(handle.trigger(&id, *t).await);
                        }
                    }
                }
                IndexingMode::Disabled => {
                    self.update_job(branch, &job.request_id, |j| {
                        j.last_error = Some(
                            "waiting for an index of its commits; indexing is disabled".into(),
                        );
                    })
                    .await?;
                }
            }
            return Ok(Prepared::Waiting);
        }
        let added = self.unclaimed_span_ends(branch, &holders, job).await?;
        if !added.is_empty() {
            self.update_job(branch, &job.request_id, |j| add_commits(j, &added))
                .await?;
            return Ok(Prepared::Waiting);
        }
        let mut erased = Vec::with_capacity(job.commits.len());
        let mut stale = Vec::new();
        for task in &job.commits {
            let held = runtime.index.erased(&commit_id(&task.commit)?).await?;
            match (need(task, held.as_ref()), &task.proved) {
                // Published already, by an earlier request or by this one
                // before a crash lost the job's update.
                (Need::Covered, _) => {}
                (Need::Publish, Some(proved)) => {
                    let mut next = held.unwrap_or_else(|| ErasedCommit {
                        commit: task.commit.clone(),
                        ledger_id: task.ledger_id.clone(),
                        txn: None,
                        steps: Vec::new(),
                        replaced_at: Vec::new(),
                    });
                    if next.txn.is_none() {
                        let bytes = self
                            .commit_bytes(&task.branches[0], &commit_id(&task.commit)?)
                            .await?;
                        next.txn = read_commit_envelope(&bytes)
                            .map_err(|e| ApiError::internal(format!("{}: {e}", task.commit)))?
                            .txn
                            .map(|t| t.address_digest());
                    }
                    next.steps.push(ErasureStep {
                        request_id: job.request_id.clone(),
                        slots: proved.slots.clone(),
                        replacement: proved.replacement.clone(),
                        proof: proved.proof.clone(),
                    });
                    erased.push(next);
                }
                // Proved on an erasure published since: prove it again.
                _ => stale.push(task.commit.clone()),
            }
        }
        if !stale.is_empty() {
            self.update_job(branch, &job.request_id, |j| {
                j.stage = ErasureStage::Proving;
                for task in &mut j.commits {
                    if stale.contains(&task.commit) {
                        task.proved = None;
                    }
                }
            })
            .await?;
            return Ok(Prepared::Waiting);
        }
        Ok(match erased.is_empty() {
            true => Prepared::Covered,
            false => Prepared::Ready { holders, erased },
        })
    }

    /// Store what authorized `job`, requested on `branch`, for transfers of
    /// its erasures to carry: its control commit and the occurrences its
    /// manifest hash covers, with those joined since.
    async fn store_authorization(&self, branch: &str, job: &ErasureJob) -> Result<()> {
        let runtime = self.erasure_runtime()?;
        let Some(control) = &job.authorized_commit else {
            tracing::warn!(request = %job.request_id, "erasure: no control commit recorded; its erasures cannot be transferred");
            return Ok(());
        };
        let bytes = self.commit_bytes(branch, &commit_id(control)?).await?;
        let control_bytes = runtime
            .index
            .put_artifact(Artifact::Control, &bytes)
            .await?;
        let added: HashSet<&OccurrenceRef> = job.added.iter().collect();
        let mut manifest: Vec<OccurrenceRef> = job
            .commits
            .iter()
            .flat_map(|c| {
                c.slots.iter().map(|slot| OccurrenceRef {
                    commit: c.commit.clone(),
                    slot: *slot,
                })
            })
            .filter(|o| !added.contains(o))
            .collect();
        manifest.sort_by(|a, b| (&a.commit, a.slot).cmp(&(&b.commit, b.slot)));
        runtime
            .index
            .put_authorization(&super::transfer::Authorization {
                request_id: job.request_id.clone(),
                ledger_id: branch.to_string(),
                control: control.clone(),
                control_bytes,
                manifest,
                added: job.added.clone(),
            })
            .await
    }

    /// Rebuild the index of `record`, a restored branch not yet visible
    /// among `branches`,
    /// when it covers an erased commit: built before the erasure, it holds
    /// the erased values. A rebuild reads the commits as their replacements.
    pub(crate) async fn rebuild_erased_index(
        &self,
        record: &NsRecord,
        branches: &[NsRecord],
    ) -> Result<()> {
        let Some(runtime) = self.retention.erasure.get() else {
            return Ok(());
        };
        let Some(head) = record
            .commit_head_id
            .as_ref()
            .filter(|_| record.index_head_id.is_some())
        else {
            return Ok(());
        };
        if !runtime.index.any_erased().await? {
            return Ok(());
        }
        // Not visible yet, so its parents are found among `branches`.
        let store: std::sync::Arc<dyn fluree_db_core::ContentStore> =
            std::sync::Arc::new(self.store_among(branches, record));
        let mut covers_erased = false;
        for (t, cid) in collect_first_parent_cids(store.as_ref(), head, 0).await? {
            if t <= record.index_t && runtime.index.erased(&cid).await?.is_some() {
                covers_erased = true;
                break;
            }
        }
        if !covers_erased {
            return Ok(());
        }
        self.rebuild_index_from(store, record).await?;
        // The indexes it replaces hold the erased values: released once the
        // ledger is visible, which releasing needs (see `sweep_after_restore`).
        runtime
            .rebuilt_on_restore
            .lock()
            .insert(record.ledger_id.to_string());
        Ok(())
    }

    /// After a restore of `name`, now visible: release the indexes its
    /// rebuilds replaced, which hold erased values, and sweep what no chain
    /// reaches. Nothing has read them: they were never a visible head.
    pub(crate) async fn sweep_after_restore(&self, name: &str) {
        let Some(runtime) = self.retention.erasure.get() else {
            return;
        };
        let rebuilt: Vec<String> = {
            let mut rebuilt = runtime.rebuilt_on_restore.lock();
            let mine: Vec<String> = rebuilt
                .iter()
                .filter(|b| LedgerId::parse(b).is_ok_and(|id| id.name() == name))
                .cloned()
                .collect();
            for b in &mine {
                rebuilt.remove(b);
            }
            mine
        };
        if rebuilt.is_empty() {
            return;
        }
        let released = async {
            for branch in &rebuilt {
                self.collapse_index(branch).await?;
            }
            self.sweep_index_storage(name).await
        };
        if let Err(e) = released.await {
            tracing::warn!(ledger = %name, error = %e, "erasure: a restored ledger's replaced indexes were not released");
        }
    }

    /// The branches holding `job`'s commits, as they are now: those of its
    /// ledger, including any made after the request, and of copies holding
    /// an original, whose history reaches one of them. `None` when
    /// `branch`'s ledger is dropped, which hides its branches.
    pub(super) async fn holders(
        &self,
        branch: &str,
        job: &ErasureJob,
    ) -> Result<Option<Vec<NsRecord>>> {
        let runtime = self.erasure_runtime()?;
        let name = LedgerId::parse(branch)?;
        let mut records: Vec<_> = self
            .nameservice()
            .list_branches(name.name())
            .await?
            .into_iter()
            .filter(|r| !r.retracted)
            .collect();
        if !records.iter().any(|r| r.ledger_id == name) {
            return Ok(None);
        }
        let commits: Vec<&str> = job.commits.iter().map(|c| c.commit.as_str()).collect();
        records.extend(self.copies_holding(&name, &commits).await?);
        let erased: std::collections::BTreeSet<ContentId> =
            commits.iter().filter_map(|c| c.parse().ok()).collect();
        let key = fluree_db_core::sha256_hex(
            erased
                .iter()
                .map(ContentId::to_string)
                .collect::<Vec<_>>()
                .join(",")
                .as_bytes(),
        );
        let mut reached = runtime
            .reached_heads
            .lock()
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let erased: HashSet<ContentId> = erased.into_iter().collect();
        let mut holders = Vec::with_capacity(records.len());
        let mut heads = Vec::with_capacity(records.len());
        for record in records {
            if let Some(head) = &record.commit_head_id {
                heads.push(head.clone());
            }
            if self.reaches(&record, &erased, &mut reached).await? {
                holders.push(record);
            }
        }
        let mut kept = runtime.reached_heads.lock();
        if kept.len() >= REACHED_SETS && !kept.contains_key(&key) {
            kept.clear();
        }
        let kept = kept.entry(key).or_default();
        for head in heads {
            if let Some(reaches) = reached.get(&head) {
                kept.insert(head, *reaches);
            }
        }
        Ok(Some(holders))
    }

    /// Whether `record`'s history reaches one of `erased` through any
    /// parent: a merge's other parent is the source branch's, whose `t` says
    /// nothing of the target's, and a transfer of the target carries it.
    /// What a commit's history reaches is kept in `reached`, for every
    /// commit decided, so history the branches share is read once; one that
    /// cannot be read counts as reaching.
    async fn reaches(
        &self,
        record: &NsRecord,
        erased: &HashSet<ContentId>,
        reached: &mut HashMap<ContentId, bool>,
    ) -> Result<bool> {
        let Some(head) = record.commit_head_id.clone() else {
            return Ok(false);
        };
        let store = self
            .content_store_for_record_or_id(Some(record), &record.storage_namespace())
            .await?;
        // Depth first: a commit is decided once one of its parents reaches,
        // or every parent is decided.
        let mut stack: Vec<(ContentId, Option<Vec<ContentId>>)> = vec![(head.clone(), None)];
        while let Some(top) = stack.len().checked_sub(1) {
            let cid = stack[top].0.clone();
            if reached.contains_key(&cid) {
                stack.pop();
                continue;
            }
            if erased.contains(&cid) {
                reached.insert(cid, true);
                stack.pop();
                continue;
            }
            let Some(parents) = stack[top].1.clone() else {
                match load_commit_envelope_by_id(store.as_ref(), &cid).await {
                    Ok(envelope) => stack[top].1 = Some(envelope.parent_ids().cloned().collect()),
                    Err(e) => {
                        tracing::debug!(commit = %cid, error = %e, "erasure: an unreadable commit counts as holding");
                        reached.insert(cid, true);
                        stack.pop();
                    }
                }
                continue;
            };
            if parents.iter().any(|p| reached.get(p) == Some(&true)) {
                reached.insert(cid, true);
                stack.pop();
                continue;
            }
            match parents.iter().find(|p| !reached.contains_key(*p)) {
                Some(next) => stack.push((next.clone(), None)),
                None => {
                    reached.insert(cid, false);
                    stack.pop();
                }
            }
        }
        Ok(reached.get(&head).copied().unwrap_or(true))
    }

    /// The `holders` whose history holds one of `job`'s commits above their
    /// index, with the `t` each must index to.
    async fn lagging_branches(
        &self,
        holders: &[NsRecord],
        job: &ErasureJob,
    ) -> Result<Vec<(String, i64)>> {
        let mut lagging = Vec::new();
        for record in holders {
            let above: Vec<&CommitErasure> = job
                .commits
                .iter()
                .filter(|c| record.index_t < c.t && c.t <= record.commit_t)
                .collect();
            let (Some(head), Some(lowest)) = (
                record.commit_head_id.as_ref(),
                above.iter().map(|c| c.t).min(),
            ) else {
                continue;
            };
            // Bounded by the branch's unindexed commits.
            let store = self
                .content_store_for_record_or_id(Some(record), &record.storage_namespace())
                .await?;
            let held: HashSet<String> = collect_first_parent_cids(store.as_ref(), head, lowest - 1)
                .await?
                .into_iter()
                .map(|(_, cid)| cid.to_string())
                .collect();
            if let Some(t) = above
                .iter()
                .filter(|c| held.contains(&c.commit))
                .map(|c| c.t)
                .max()
            {
                lagging.push((record.ledger_id.to_string(), t));
            }
        }
        Ok(lagging)
    }

    /// Retractions in the histories of `holders` and of dropped holders that
    /// end the span of one of `job`'s assertions but are not in it, as
    /// commits to erase. A copy restored from an older archive since
    /// acceptance makes one, retracting the value still current in it; a
    /// copy dropped before acceptance can hold its own.
    async fn unclaimed_span_ends(
        &self,
        branch: &str,
        holders: &[NsRecord],
        job: &ErasureJob,
    ) -> Result<Vec<CommitErasure>> {
        let named: HashMap<(&str, u32), &CommitErasure> = job
            .commits
            .iter()
            .flat_map(|task| {
                task.slots
                    .iter()
                    .map(move |s| ((task.commit.as_str(), *s), task))
            })
            .collect();
        let from = job.commits.iter().map(|c| c.t).min().unwrap_or(0);
        let mut stores = Vec::with_capacity(holders.len());
        for holder in holders {
            let store = self
                .content_store_for_record_or_id(Some(holder), &holder.storage_namespace())
                .await?;
            stores.push((holder.clone(), store));
        }
        // Nothing indexes a dropped holder, but its commits can hold the
        // value too.
        let commits: Vec<&str> = job.commits.iter().map(|c| c.commit.as_str()).collect();
        stores.extend(self.dropped_holders(&commits).await?);
        let mut found = super::request::Found::new();
        for (holder, store) in &stores {
            let Some(head) = holder.commit_head_id.as_ref() else {
                continue;
            };
            let (ends, _) = span_ends(store.as_ref(), head, from, &named).await?;
            for end in ends {
                let entry = found
                    .entry((end.commit.to_string(), end.slot))
                    .or_insert((end.t, Vec::new()));
                let id = holder.ledger_id.to_string();
                if !entry.1.contains(&id) {
                    entry.1.push(id);
                }
            }
        }
        if found.is_empty() {
            return Ok(Vec::new());
        }
        let candidates: Vec<String> = stores
            .iter()
            .map(|(r, _)| r.ledger_id.to_string())
            .collect();
        self.commits_to_erase(&LedgerId::parse(branch)?, &found, &candidates)
            .await
    }

    async fn job(&self, branch: &str, request_id: &str) -> Result<ErasureJob> {
        let (_, registration) =
            registry::read(self.publisher()?, self.registry_objects(), branch).await?;
        registration
            .and_then(|r| r.erasures.into_iter().find(|j| j.request_id == request_id))
            .ok_or_else(|| ApiError::internal(format!("erasure {request_id} is gone")))
    }

    pub(super) async fn update_job(
        &self,
        branch: &str,
        request_id: &str,
        change: impl Fn(&mut ErasureJob),
    ) -> Result<()> {
        registry::update(
            self.publisher()?,
            self.registry_objects(),
            branch,
            |current| {
                let mut next = current?.clone();
                let job = next
                    .erasures
                    .iter_mut()
                    .find(|j| j.request_id == request_id)?;
                let before = job.clone();
                change(job);
                (*job != before).then_some(next)
            },
        )
        .await?;
        Ok(())
    }
}

fn commit_id(commit: &str) -> Result<ContentId> {
    commit
        .parse()
        .map_err(|_| ApiError::internal(format!("not a commit id: {commit}")))
}

/// Why a step did not land.
enum StepError {
    /// The prover refused the step: it will not prove on retry.
    Refused(String),
    /// Anything else: tried again later.
    Retry(ApiError),
}

impl From<std::result::Result<Option<ProvedStep>, StepError>> for StepOutcome {
    fn from(outcome: std::result::Result<Option<ProvedStep>, StepError>) -> Self {
        match outcome {
            Ok(Some(proved)) => StepOutcome::Proved(proved),
            Ok(None) => StepOutcome::Nothing,
            Err(StepError::Refused(why)) => StepOutcome::Refused(why),
            Err(StepError::Retry(e)) => StepOutcome::Retry(e.to_string()),
        }
    }
}

/// A step ready to prove: the slots of the commit its published erasure
/// still holds, and the replacement it redacts.
struct PreparedStep {
    cid: ContentId,
    slots: Vec<u32>,
    base: Option<String>,
    prior: Option<ErasedCommit>,
    ledger_id: String,
    /// The branches holding the commit.
    branches: Vec<String>,
    signatures: super::SignatureMode,
}

impl PreparedStep {
    /// Names the step by what it proves from, so a result stored under it
    /// serves whichever member is asked again.
    fn key(&self) -> String {
        let inputs = serde_json::json!([
            self.cid.to_string(),
            self.base,
            self.slots,
            self.ledger_id,
            self.signatures,
        ]);
        fluree_db_core::sha256_hex(inputs.to_string().as_bytes())
    }
}

#[cfg(test)]
mod tests;
