//! Advancing erasure jobs: one proving step at a time in the background,
//! verified and stored as it lands; publishing once a job's commits are
//! proved and indexed.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::time::Instant;

use fluree_db_core::{collect_first_parent_cids, ContentId, ContentKind, LedgerId};
use fluree_db_erasure::{ProveRequest, ProverError};

use super::index::{Artifact, ErasedCommit, ErasureStep, REFRESH};
use super::{CommitErasure, ErasureJob, ErasureStage, ProvedStep};
use crate::retention::registry;
use crate::retention::worker::now_ms;
use crate::{ApiError, Fluree, IndexingMode, Result};

/// The longest wait before a failed step is tried again (ms).
const MAX_RETRY_MS: i64 = 3_600_000;

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
            if let Ok((_, Some(registration))) = registry::read(ns, branch).await {
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
                job.stage = stage;
                let _ = self
                    .update_job(&branch, &job.request_id, |j| j.stage = stage)
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
    pub(crate) async fn erasure_sweep(&self, fluree: &Weak<Fluree>, ledgers: &[String]) {
        let Some(runtime) = self.retention.erasure.get() else {
            return;
        };
        let Some(config) = runtime.config.as_ref() else {
            return;
        };
        let work = self.erasure_work(ledgers).await;
        for Held { branch, job } in &work.publish {
            if let Err(e) = self.publish_when_indexed(branch, job).await {
                tracing::warn!(request = %job.request_id, error = %e, "erasure: not published");
            }
        }
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
            fluree
                .step_and_record(&branch, &request_id, &commit, retry_ms)
                .await;
            if let Some(runtime) = fluree.retention.erasure.get() {
                runtime.in_flight.store(false, Ordering::Release);
            }
            fluree.retention.nudge.notify_one();
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
        let outcome = self.prove_step(branch, request_id, commit).await;
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
    /// `None` when the published erasure leaves nothing to prove.
    async fn prove_step(
        &self,
        branch: &str,
        request_id: &str,
        commit: &str,
    ) -> std::result::Result<Option<ProvedStep>, StepError> {
        let runtime = self.erasure_runtime().map_err(StepError::Retry)?;
        let config = runtime
            .config
            .as_ref()
            .ok_or_else(|| StepError::Retry(ApiError::internal("no prover")))?;
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
        let input = self
            .commit_bytes(&task.branches[0], &cid)
            .await
            .map_err(StepError::Retry)?;
        let request = ProveRequest {
            input,
            removed_slots: slots.clone(),
            ledger_id: task.ledger_id.clone(),
            signatures: job.signatures.into(),
        };
        let started = Instant::now();
        let response = config.prover.prove(&request).await.map_err(|e| match e {
            ProverError::Refused(why) => StepError::Refused(why),
            other => StepError::Retry(ApiError::internal(other.to_string())),
        })?;
        let prove_ms = started.elapsed().as_millis() as u64;

        let mut receipts = Vec::new();
        let mut authorized = Vec::new();
        for step in prior.iter().flat_map(|p| &p.steps) {
            receipts.push(
                runtime
                    .index
                    .artifact(Artifact::Proof, &step.proof)
                    .await
                    .map_err(StepError::Retry)?,
            );
            authorized.push(step.slots.clone());
        }
        receipts.push(response.receipt.clone());
        authorized.push(slots.clone());
        let verifier = config.verifier.clone();
        let (original, ledger_id, replacement) = (
            cid.to_bytes(),
            task.ledger_id.clone(),
            response.replacement.clone(),
        );
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
        Ok(Some(ProvedStep {
            slots,
            base,
            replacement: replacement_id.to_string(),
            proof,
            input_bytes: request.input.len() as u64,
            replacement_bytes: response.replacement.len() as u64,
            user_cycles: response.user_cycles,
            segments: response.segments,
            prove_ms,
        }))
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
        // No branch is made between the check and the publication.
        let admission = runtime.admission.lock().await;
        let Some(lagging) = self.lagging_branches(branch, job).await? else {
            // Dropped: its branches, and what their indexes hold, can be
            // restored, so the gate waits until they are.
            return Ok(());
        };
        if !lagging.is_empty() {
            drop(admission);
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
            return Ok(());
        }
        let mut published = Vec::with_capacity(job.commits.len());
        let mut stale = Vec::new();
        for task in &job.commits {
            let held = runtime.index.erased(&commit_id(&task.commit)?).await?;
            match (need(task, held.as_ref()), &task.proved) {
                // Published already, by an earlier request or by this one
                // before a crash lost the job's update.
                (Need::Covered, _) => {}
                (Need::Publish, Some(proved)) => {
                    let mut erased = held.unwrap_or_else(|| ErasedCommit {
                        commit: task.commit.clone(),
                        ledger_id: task.ledger_id.clone(),
                        steps: Vec::new(),
                    });
                    erased.steps.push(ErasureStep {
                        request_id: job.request_id.clone(),
                        slots: proved.slots.clone(),
                        replacement: proved.replacement.clone(),
                        proof: proved.proof.clone(),
                    });
                    published.push(erased);
                }
                // Proved on an erasure published since: prove it again.
                _ => stale.push(task.commit.clone()),
            }
        }
        if !stale.is_empty() {
            drop(admission);
            return self
                .update_job(branch, &job.request_id, |j| {
                    j.stage = ErasureStage::Proving;
                    for task in &mut j.commits {
                        if stale.contains(&task.commit) {
                            task.proved = None;
                        }
                    }
                })
                .await;
        }
        if !published.is_empty() {
            runtime.index.publish(published).await?;
        }
        drop(admission);
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

    /// Branches, as they are now, whose history holds one of `job`'s
    /// commits above their index, with the `t` each must index to. A branch
    /// made after the request holds them too. `None` when `branch`'s ledger
    /// is dropped, which hides its branches.
    async fn lagging_branches(
        &self,
        branch: &str,
        job: &ErasureJob,
    ) -> Result<Option<Vec<(String, i64)>>> {
        let name = LedgerId::parse(branch)?;
        let records: Vec<_> = self
            .nameservice()
            .list_branches(name.name())
            .await?
            .into_iter()
            .filter(|r| !r.retracted)
            .collect();
        if !records.iter().any(|r| r.ledger_id == name) {
            return Ok(None);
        }
        let mut lagging = Vec::new();
        for record in records {
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
                .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
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
        Ok(Some(lagging))
    }

    async fn job(&self, branch: &str, request_id: &str) -> Result<ErasureJob> {
        let (_, registration) = registry::read(self.publisher()?, branch).await?;
        registration
            .and_then(|r| r.erasures.into_iter().find(|j| j.request_id == request_id))
            .ok_or_else(|| ApiError::internal(format!("erasure {request_id} is gone")))
    }

    async fn update_job(
        &self,
        branch: &str,
        request_id: &str,
        change: impl Fn(&mut ErasureJob),
    ) -> Result<()> {
        registry::update(self.publisher()?, branch, |current| {
            let mut next = current?.clone();
            let job = next
                .erasures
                .iter_mut()
                .find(|j| j.request_id == request_id)?;
            let before = job.clone();
            change(job);
            (*job != before).then_some(next)
        })
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

#[cfg(test)]
mod tests;
