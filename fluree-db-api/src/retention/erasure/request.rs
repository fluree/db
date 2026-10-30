//! Accepting an erasure request: its scope, the removal on every branch
//! that holds an occurrence, the control commit that authorizes it, and the
//! job that tracks it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fluree_db_core::{
    ContentId, ContentStore, HashAlgorithm, LedgerId, StorageNamespace, TxnMetaEntry, TxnMetaValue,
};
use fluree_vocab::{db, namespaces::FLUREE_DB};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use fluree_db_nameservice::{DroppedLedger, DroppedState, NsRecord, Versioned};

use super::{CommitErasure, ErasureJob, ErasureStage, SignatureMode};
use crate::retention::registry;
use crate::retention::removal::{
    FactSelector, OccurrenceRef, RemovalReceipt, RemovalRequest, Widening,
};
use crate::retention::writer::{outcome_of, RetentionBatch, RetentionWriter, SubmitOutcome};
use crate::{ApiError, Fluree, Result};

/// A permanent-erasure request: occurrences named by commit and slot, or
/// facts whose every occurrence is erased.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErasureRequest {
    #[serde(default)]
    pub occurrences: Vec<OccurrenceRef>,
    #[serde(default)]
    pub matching: Vec<FactSelector>,
    /// `expose` proves a signed commit as fast as an unsigned one, but its
    /// proof reveals the signed body's hash.
    #[serde(default)]
    pub signatures: SignatureMode,
}

/// The removal an erasure made on one branch.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchRemoval {
    pub ledger_id: String,
    pub removal: RemovalReceipt,
}

/// What accepting an erasure request did.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErasureReceipt {
    pub request_id: String,
    /// Every occurrence erased, across branches.
    pub manifest: Vec<OccurrenceRef>,
    /// Affected commits.
    pub commits: usize,
    /// The control commit recording the authorization.
    pub authorized_t: i64,
    pub stage: ErasureStage,
    /// The removal on each branch holding an occurrence.
    pub removals: Vec<BranchRemoval>,
}

/// How many times the authorizing commit is retried against a moving head.
const ATTEMPTS: usize = 5;

/// How many times acceptance removes from branches made meanwhile before it
/// gives up.
const APPEARING_ROUNDS: usize = 8;

impl Fluree {
    /// Erase the selected occurrences permanently, committing directly. See
    /// [`Self::erase_occurrences_with`].
    pub async fn erase_occurrences(
        &self,
        ledger_id: &str,
        request: ErasureRequest,
        requester: Option<String>,
    ) -> Result<ErasureReceipt> {
        struct Direct<'a>(&'a Fluree);
        #[async_trait::async_trait]
        impl RetentionWriter for Direct<'_> {
            async fn submit(
                &self,
                ledger_id: &str,
                batch: RetentionBatch,
            ) -> Result<SubmitOutcome> {
                outcome_of(self.0.commit_retention_batch(ledger_id, batch).await)
            }
        }
        self.erase_occurrences_with(&Direct(self), ledger_id, request, requester)
            .await
    }

    /// Accept a permanent-erasure request, committing through `writer`:
    /// remove the occurrences from every branch that holds them, record the
    /// authorization, and queue the proofs. The receipt returns once the
    /// request is durable; proving and publishing follow in the background
    /// (see [`Self::retention_status`]).
    pub async fn erase_occurrences_with(
        &self,
        writer: &dyn RetentionWriter,
        ledger_id: &str,
        request: ErasureRequest,
        requester: Option<String>,
    ) -> Result<ErasureReceipt> {
        // Published, a cluster's erasures could not yet be purged.
        if self.retention.replicas.get().is_some() {
            return Err(unsupported("a cluster cannot erase permanently yet"));
        }
        self.accept_erasure(writer, ledger_id, request, requester)
            .await
    }

    /// [`Self::erase_occurrences_with`] on a cluster member, which refuses
    /// it: for tests of a cluster's publication.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub async fn erase_occurrences_in_cluster(
        &self,
        writer: &dyn RetentionWriter,
        ledger_id: &str,
        request: ErasureRequest,
        requester: Option<String>,
    ) -> Result<ErasureReceipt> {
        self.accept_erasure(writer, ledger_id, request, requester)
            .await
    }

    pub(crate) async fn accept_erasure(
        &self,
        writer: &dyn RetentionWriter,
        ledger_id: &str,
        request: ErasureRequest,
        requester: Option<String>,
    ) -> Result<ErasureReceipt> {
        let runtime = self.erasure_runtime()?;
        if runtime.config.is_none() {
            return Err(unsupported(
                "no prover is available: install one with `fluree prover install`, beside the \
                 server's binary, or name it with `--erasure-prover`",
            ));
        }
        let ledger_id = LedgerId::parse(ledger_id)?;
        let ns = self.publisher()?;
        // No branch is made while a request is accepted: one made before is
        // removed from, one made after inherits the removal.
        let _admission = runtime.admission.lock().await;
        let mut branches: Vec<String> = ns
            .list_branches(ledger_id.name())
            .await?
            .into_iter()
            .filter(|r| !r.retracted && r.ledger_id != ledger_id)
            .map(|r| r.ledger_id.to_string())
            .collect();
        branches.sort();
        branches.insert(0, ledger_id.to_string());

        let removal_request = RemovalRequest {
            occurrences: request.occurrences.clone(),
            matching: request.matching.clone(),
        };
        let mut removed = Accepted::default();
        self.remove_on(writer, &branches, &removal_request, true, &mut removed)
            .await?;
        for round in 0.. {
            // Other ledgers holding the same commits, such as copies restored
            // from an archive, read the replacements too: the occurrences
            // found are removed there as well, with what ends their spans
            // there.
            let commits: Vec<&str> = removed
                .found
                .keys()
                .map(|(commit, _)| commit.as_str())
                .collect();
            let copies: Vec<String> = self
                .copies_holding(&ledger_id, &commits)
                .await?
                .into_iter()
                .map(|r| r.ledger_id.to_string())
                .filter(|id| !branches.contains(id))
                .collect();
            if !copies.is_empty() {
                let named = RemovalRequest {
                    occurrences: removed
                        .found
                        .keys()
                        .map(|(commit, slot)| OccurrenceRef {
                            commit: commit.clone(),
                            slot: *slot,
                        })
                        .collect(),
                    matching: Vec::new(),
                };
                self.remove_on(writer, &copies, &named, false, &mut removed)
                    .await?;
                branches.extend(copies);
            }
            // The admission lock is this process's: on a cluster another
            // member can make a branch meanwhile, copying its source's
            // journal before it held the removal. Once this member has
            // applied what the cluster committed, a branch listed now and
            // not before is removed from too, until none appears; one made
            // later copies the removal.
            let Some(replicas) = self.retention.replicas.get() else {
                break;
            };
            replicas.sync_reads().await.map_err(|e| {
                ApiError::ReplicaNotCurrent(format!(
                    "this member cannot confirm it lists every branch of {ledger_id} ({e}); \
                     retry the request"
                ))
            })?;
            let appeared: Vec<String> = ns
                .list_branches(ledger_id.name())
                .await?
                .into_iter()
                .filter(|r| !r.retracted)
                .map(|r| r.ledger_id.to_string())
                .filter(|id| !branches.contains(id))
                .collect();
            if appeared.is_empty() {
                break;
            }
            if round == APPEARING_ROUNDS {
                return Err(ApiError::http(
                    409,
                    format!(
                        "branches of {ledger_id} kept being made while the request was \
                         accepted; retry it"
                    ),
                ));
            }
            self.remove_on(writer, &appeared, &removal_request, false, &mut removed)
                .await?;
            branches.extend(appeared);
        }
        let Accepted {
            found,
            removals,
            properties,
        } = removed;

        let manifest: Vec<OccurrenceRef> = found
            .keys()
            .map(|(commit, slot)| OccurrenceRef {
                commit: commit.clone(),
                slot: *slot,
            })
            .collect();
        let request_id = request_id(&ledger_id, &manifest);
        let (_, registration) = registry::read(ns, ledger_id.as_str()).await?;
        if let Some(job) = registration
            .as_ref()
            .and_then(|r| r.erasures.iter().find(|j| j.request_id == request_id))
        {
            return Ok(receipt_of(job, manifest, removals));
        }

        let commits = self.commits_to_erase(&ledger_id, &found, &branches).await?;
        let (authorized_t, authorized_commit) = self
            .authorize_erasure(
                writer,
                &ledger_id,
                &request_id,
                requester.as_deref(),
                &manifest,
            )
            .await?;
        let job = ErasureJob {
            request_id: request_id.clone(),
            requester,
            accepted_ms: crate::retention::worker::now_ms(),
            authorized_t,
            authorized_commit: Some(authorized_commit.to_string()),
            signatures: request.signatures,
            stage: ErasureStage::Proving,
            commits,
            properties: match properties.contains("") {
                true => Vec::new(),
                false => properties.into_iter().collect(),
            },
            retries: 0,
            retry_at_ms: None,
            last_error: None,
            published_ms: None,
            added: Vec::new(),
            purge: None,
            cluster: None,
            as_written: None,
        };
        let recorded = job.clone();
        registry::update(ns, ledger_id.as_str(), |current| {
            let mut next = current.cloned().unwrap_or_else(|| registry::Registration {
                active_since_t: authorized_t,
                watermark_t: authorized_t - 1,
                ..Default::default()
            });
            if next
                .erasures
                .iter()
                .any(|j| j.request_id == recorded.request_id)
            {
                return None;
            }
            next.erasures.push(recorded.clone());
            keep_recent_finished(&mut next.erasures);
            Some(next)
        })
        .await?;
        self.retention.mark_active(ledger_id.as_str());
        Ok(receipt_of(&job, manifest, removals))
    }

    /// Remove `request` on each of `branches`, the first of them the
    /// requested one when `requested`, recording what each removed.
    async fn remove_on(
        &self,
        writer: &dyn RetentionWriter,
        branches: &[String],
        request: &RemovalRequest,
        requested: bool,
        into: &mut Accepted,
    ) -> Result<()> {
        let Accepted {
            found,
            removals,
            properties,
        } = into;
        for (i, branch) in branches.iter().enumerate() {
            let requested = requested && i == 0;
            let widening = Widening {
                pair_spans: true,
                skip_foreign: !requested,
                bounded: false,
            };
            let removed = match self.remove_widened(writer, branch, request, widening).await {
                Ok(removed) => removed,
                // Nothing of the request on this other branch.
                Err(e) if !requested && (e.is_not_found() || e.status_code() == 404) => continue,
                Err(e) => return Err(e),
            };
            for (occ, t) in removed.receipt.manifest.iter().zip(&removed.manifest_t) {
                let entry = found
                    .entry((occ.commit.clone(), occ.slot))
                    .or_insert((*t, Vec::new()));
                if !entry.1.contains(branch) {
                    entry.1.push(branch.clone());
                }
            }
            properties.extend(removed.properties);
            removals.push(BranchRemoval {
                ledger_id: branch.clone(),
                removal: removed.receipt,
            });
        }
        Ok(())
    }

    /// The branches of ledgers other than `ledger_id`'s, one of whose
    /// branches holds the original of one of `commits` in its own folder.
    pub(crate) async fn copies_holding(
        &self,
        ledger_id: &LedgerId,
        commits: &[&str],
    ) -> Result<Vec<NsRecord>> {
        let mut cids: Vec<ContentId> = commits.iter().filter_map(|c| c.parse().ok()).collect();
        cids.sort();
        cids.dedup();
        self.ledgers_holding(&cids, Some(ledger_id.name())).await
    }

    /// The live branches of the ledgers, but `except`, one of whose
    /// branches holds the original of one of `cids` in its own folder.
    pub(crate) async fn ledgers_holding(
        &self,
        cids: &[ContentId],
        except: Option<&str>,
    ) -> Result<Vec<NsRecord>> {
        let runtime = self.erasure_runtime()?;
        let mut ledgers: BTreeMap<String, Vec<NsRecord>> = BTreeMap::new();
        for record in self.nameservice().all_records().await? {
            if !record.retracted && Some(record.ledger_id.name()) != except {
                ledgers
                    .entry(record.ledger_id.name().to_string())
                    .or_default()
                    .push(record);
            }
        }
        let mut copies = Vec::new();
        for records in ledgers.into_values() {
            let namespaces: Vec<StorageNamespace> =
                records.iter().map(NsRecord::storage_namespace).collect();
            if any_holds(&runtime.index, &namespaces, cids).await? {
                copies.extend(records);
            }
        }
        copies.sort_by(|a, b| a.ledger_id.cmp(&b.ledger_id));
        Ok(copies)
    }

    /// The branches of soft-dropped ledgers, one of whose branches holds the
    /// original of one of `commits` in its own folder, each with its store.
    /// One being restored holds it as much as one at rest; one being purged
    /// is going.
    pub(crate) async fn dropped_holders(
        &self,
        commits: &[&str],
    ) -> Result<Vec<(NsRecord, Arc<dyn ContentStore>)>> {
        let mut holders = Vec::new();
        let admitted: fn(&DroppedState) -> bool = |state| !matches!(state, DroppedState::Purging);
        for versioned in self.dropped_holding(commits, admitted).await? {
            let entry = versioned.value;
            // An entry keeps its folder beside its records, not in them.
            let branches: Vec<NsRecord> = entry
                .branches
                .iter()
                .map(|r| NsRecord {
                    storage_root: Some(entry.root.clone()),
                    ..r.clone()
                })
                .collect();
            for branch in branches.iter().filter(|r| !r.retracted) {
                let store: Arc<dyn ContentStore> = Arc::new(self.store_among(&branches, branch));
                holders.push((branch.clone(), store));
            }
        }
        Ok(holders)
    }

    /// The soft-dropped ledgers in a state `admitted` admits, one of whose
    /// branches holds the original of one of `commits` in its own folder.
    pub(crate) async fn dropped_holding(
        &self,
        commits: &[&str],
        admitted: fn(&DroppedState) -> bool,
    ) -> Result<Vec<Versioned<DroppedLedger>>> {
        let runtime = self.erasure_runtime()?;
        let mut cids: Vec<ContentId> = commits.iter().filter_map(|c| c.parse().ok()).collect();
        cids.sort();
        cids.dedup();
        let mut holding = Vec::new();
        for versioned in self.publisher()?.list_dropped().await? {
            let entry = &versioned.value;
            let namespaces: Vec<StorageNamespace> = entry
                .branches
                .iter()
                .map(|b| entry.root.namespace(&b.branch))
                .collect();
            if admitted(&entry.state) && any_holds(&runtime.index, &namespaces, &cids).await? {
                holding.push(versioned);
            }
        }
        Ok(holding)
    }

    pub(crate) fn erasure_runtime(&self) -> Result<&std::sync::Arc<super::ErasureRuntime>> {
        self.retention
            .erasure
            .get()
            .ok_or_else(|| unsupported("permanent erasure needs a single node with file storage"))
    }

    /// Announce that `ledger_id`'s stored history changed in place (an
    /// erasure published over its commits, or a purge rewrote its index):
    /// its state cached here goes, and followers evict theirs.
    pub(crate) async fn announce_refresh(&self, ledger_id: &LedgerId) {
        if let Some(manager) = &self.ledger_manager {
            manager.disconnect(ledger_id).await;
        }
        self.event_bus()
            .notify(fluree_db_nameservice::NameServiceEvent::LedgerRefresh {
                ledger_id: ledger_id.clone(),
            });
    }

    /// The erasures published on this storage, as a hash that a follower
    /// compares across reconnects: a change means it may have missed a
    /// refresh. `None` without erasure.
    pub async fn erasure_generation(&self) -> Result<Option<String>> {
        match self.retention.erasure.get() {
            Some(runtime) => Ok(Some(runtime.index.generation().await?)),
            None => Ok(None),
        }
    }

    /// Each affected commit, checked for what the transform supports, with
    /// the ledger id its signatures verify for.
    pub(super) async fn commits_to_erase(
        &self,
        ledger_id: &LedgerId,
        found: &Found,
        branches: &[String],
    ) -> Result<Vec<CommitErasure>> {
        let mut by_commit: BTreeMap<&str, CommitErasure> = BTreeMap::new();
        for ((commit, slot), (t, holders)) in found {
            let task = by_commit.entry(commit).or_insert_with(|| CommitErasure {
                commit: commit.clone(),
                t: *t,
                slots: Vec::new(),
                ledger_id: ledger_id.to_string(),
                branches: Vec::new(),
                proved: None,
            });
            task.slots.push(*slot);
            for b in holders {
                if !task.branches.contains(b) {
                    task.branches.push(b.clone());
                }
            }
        }
        let runtime = self.erasure_runtime()?;
        let mut out = Vec::with_capacity(by_commit.len());
        for (commit, mut task) in by_commit {
            let cid: ContentId = commit
                .parse()
                .map_err(|_| ApiError::http(400, format!("not a commit id: {commit}")))?;
            if cid.hash_algorithm() != Some(HashAlgorithm::Sha2_256) {
                return Err(unsupported_commit(commit, "its CID is not SHA2-256"));
            }
            task.slots.sort_unstable();
            task.slots.dedup();
            if let Some(erased) = runtime.index.erased(&cid).await? {
                // A later step reads the replacement and keeps its ledger.
                task.ledger_id = erased.ledger_id;
                out.push(task);
                continue;
            }
            let bytes = self.commit_bytes(&task.branches[0], &cid).await?;
            task.ledger_id = signing_ledger(&bytes, &task.slots, branches)
                .map_err(|why| unsupported_commit(commit, &why))?;
            out.push(task);
        }
        Ok(out)
    }

    /// A commit's bytes as stored: its original, or its replacement.
    pub(crate) async fn commit_bytes(&self, branch: &str, cid: &ContentId) -> Result<Vec<u8>> {
        let store = match self.nameservice().lookup(branch).await? {
            Some(record) => {
                self.content_store_for_record_or_id(Some(&record), &record.storage_namespace())
                    .await?
            }
            // A soft-dropped holder's branch.
            None => self
                .dropped_holders(&[cid.to_string().as_str()])
                .await?
                .into_iter()
                .find(|(record, _)| record.ledger_id.as_str() == branch)
                .map(|(_, store)| store)
                .ok_or_else(|| ApiError::NotFound(branch.to_string()))?,
        };
        Ok(store.get(cid).await?.into_vec())
    }

    /// Commit the request's authorization on `ledger_id`: its id, its
    /// requester and its manifest's hash, and no value. Returns its `t` and
    /// id.
    async fn authorize_erasure(
        &self,
        writer: &dyn RetentionWriter,
        ledger_id: &LedgerId,
        request_id: &str,
        requester: Option<&str>,
        manifest: &[OccurrenceRef],
    ) -> Result<(i64, ContentId)> {
        let mut control = vec![
            TxnMetaEntry::new(
                FLUREE_DB,
                db::ERASURE_REQUEST,
                TxnMetaValue::String(request_id.to_string()),
            ),
            TxnMetaEntry::new(
                FLUREE_DB,
                db::ERASURE_MANIFEST,
                TxnMetaValue::String(manifest_hash(manifest)),
            ),
        ];
        if let Some(requester) = requester {
            control.push(TxnMetaEntry::new(
                FLUREE_DB,
                db::ERASURE_REQUESTER,
                TxnMetaValue::String(requester.to_string()),
            ));
        }
        for _ in 0..ATTEMPTS {
            let record = self
                .nameservice()
                .lookup(ledger_id)
                .await?
                .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
            let batch = RetentionBatch {
                expected_t: record.commit_t,
                retractions: Vec::new(),
                expired: 0,
                retired: 0,
                control: control.clone(),
            };
            match writer.submit(ledger_id.as_str(), batch).await? {
                SubmitOutcome::Committed { t, commit_id } => return Ok((t, commit_id)),
                SubmitOutcome::HeadMoved => continue,
                SubmitOutcome::Deferred => {
                    return Err(ApiError::Transact(
                        fluree_db_transact::TransactError::NoveltyAtMax,
                    ))
                }
            }
        }
        Err(ApiError::commit_contention(
            ATTEMPTS,
            ApiError::internal("the head kept moving while authorizing the erasure"),
        ))
    }
}

fn unsupported(why: &str) -> ApiError {
    ApiError::http(501, format!("permanent erasure is unsupported here: {why}"))
}

fn unsupported_commit(commit: &str, why: &str) -> ApiError {
    ApiError::http(
        422,
        format!(
            "the occurrences are removed from what the ledger serves, but commit {commit} \
             cannot be erased permanently: {why}"
        ),
    )
}

/// The ledger id among `candidates` that `bytes`' signatures verify for
/// (the first, for an unsigned commit), checked by running the transform
/// as the prover will, verifying signatures whichever mode proves them.
/// Its error is why the commit cannot be erased.
/// (commit, slot) -> (t, the branches holding it)
pub(super) type Found = BTreeMap<(String, u32), (i64, Vec<String>)>;

/// What a request's removals on its branches and copies did.
#[derive(Default)]
struct Accepted {
    found: Found,
    removals: Vec<BranchRemoval>,
    /// Each removed occurrence's property IRI, empty where it did not
    /// decode.
    properties: BTreeSet<String>,
}

fn signing_ledger(
    bytes: &[u8],
    slots: &[u32],
    candidates: &[String],
) -> std::result::Result<String, String> {
    use fluree_db_redaction::{redact_with, Error, HostSha, Signatures};
    let mut last = String::new();
    for candidate in candidates {
        match redact_with::<HostSha, _>(bytes, slots, candidate, Signatures::Verify, &mut ()) {
            Ok(_) => return Ok(candidate.clone()),
            Err(Error::Signature(why)) => last = format!("its signature does not verify ({why})"),
            Err(e) => return Err(e.to_string()),
        }
    }
    Err(last)
}

/// Finished jobs a registration keeps; older ones are dropped (the erasure
/// map, not the job, is what reads rely on).
const KEEP_FINISHED: usize = 100;

fn keep_recent_finished(jobs: &mut Vec<ErasureJob>) {
    let mut drop = jobs
        .iter()
        .filter(|j| j.finished())
        .count()
        .saturating_sub(KEEP_FINISHED);
    jobs.retain(|j| {
        if drop > 0 && j.finished() {
            drop -= 1;
            false
        } else {
            true
        }
    });
}

pub(super) fn manifest_hash(manifest: &[OccurrenceRef]) -> String {
    let mut hasher = Sha256::new();
    for occ in manifest {
        hasher.update(occ.commit.as_bytes());
        hasher.update(occ.slot.to_le_bytes());
    }
    hex::encode(hasher.finalize())
}

/// A deterministic id: the ledger and the manifest, so a retried request
/// finds its job.
fn request_id(ledger_id: &LedgerId, manifest: &[OccurrenceRef]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(ledger_id.as_str().as_bytes());
    hasher.update(manifest_hash(manifest).as_bytes());
    format!("erasure-{}", hex::encode(&hasher.finalize()[..12]))
}

fn receipt_of(
    job: &ErasureJob,
    manifest: Vec<OccurrenceRef>,
    removals: Vec<BranchRemoval>,
) -> ErasureReceipt {
    ErasureReceipt {
        request_id: job.request_id.clone(),
        manifest,
        commits: job.commits.len(),
        authorized_t: job.authorized_t,
        stage: job.stage.clone(),
        removals,
    }
}

/// Whether one of `namespaces` holds the original of one of `cids` in its own
/// folder.
async fn any_holds(
    index: &super::index::ErasureIndex,
    namespaces: &[StorageNamespace],
    cids: &[ContentId],
) -> Result<bool> {
    for namespace in namespaces {
        for cid in cids {
            if index.holds_original(namespace, cid).await? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(n: usize, stage: ErasureStage) -> ErasureJob {
        ErasureJob {
            request_id: format!("erasure-{n}"),
            requester: None,
            accepted_ms: n as i64,
            authorized_t: 1,
            authorized_commit: None,
            signatures: SignatureMode::Verify,
            stage,
            commits: Vec::new(),
            properties: Vec::new(),
            retries: 0,
            retry_at_ms: None,
            last_error: None,
            published_ms: None,
            added: Vec::new(),
            purge: None,
            cluster: None,
            as_written: None,
        }
    }

    #[test]
    fn a_registration_keeps_every_pending_job_and_the_recent_finished_ones() {
        let mut jobs: Vec<ErasureJob> = (0..KEEP_FINISHED + 10)
            .map(|n| job(n, ErasureStage::Purged))
            .collect();
        jobs.insert(3, job(9_999, ErasureStage::Proving));
        // Published, not yet purged: still to do.
        jobs.insert(5, job(9_998, ErasureStage::Published));
        // A later release's stage: never taken for finished.
        jobs.insert(1, job(9_997, ErasureStage::Other("queued".into())));
        keep_recent_finished(&mut jobs);
        assert_eq!(jobs.len(), KEEP_FINISHED + 3);
        assert!(jobs.iter().any(|j| j.request_id == "erasure-9999"));
        assert!(jobs.iter().any(|j| j.request_id == "erasure-9998"));
        assert!(jobs.iter().any(|j| j.request_id == "erasure-9997"));
        assert!(
            jobs.iter().all(|j| j.request_id != "erasure-0"),
            "oldest dropped"
        );
        assert!(jobs
            .iter()
            .any(|j| j.request_id == format!("erasure-{}", KEEP_FINISHED + 9)));
    }

    /// A job in a later release's stage reads, is not acted on, and writes
    /// back as written; this release's stages are written as they always
    /// were.
    #[test]
    fn a_job_in_a_later_release_stage_is_kept_as_written() {
        let mut proving = job(1, ErasureStage::Proving);
        proving.commits.push(super::super::CommitErasure {
            commit: "c".into(),
            t: 1,
            slots: vec![0],
            ledger_id: "l:main".into(),
            branches: vec!["l:main".into()],
            proved: None,
        });
        let registration = registry::Registration {
            erasures: vec![proving],
            ..Default::default()
        };
        let mut written = serde_json::to_value(&registration).unwrap();
        written["erasures"][0]["stage"] = "queued".into();

        let read: registry::Registration = serde_json::from_value(written.clone()).unwrap();
        let held = &read.erasures[0];
        assert_eq!(held.stage, ErasureStage::Other("queued".into()));
        assert!(!held.pending() && !held.purge_pending() && !held.finished());
        assert_eq!(serde_json::to_value(&read).unwrap(), written);

        // Fields of that release, at the top and nested, are kept with it.
        written["erasures"][0]["remoteTask"] = serde_json::json!({"worker": "w1", "lease": 5});
        written["erasures"][0]["commits"][0]["remoteSlot"] = 3.into();
        let read: registry::Registration = serde_json::from_value(written.clone()).unwrap();
        assert_eq!(serde_json::to_value(&read).unwrap(), written);

        // Changed here, it is written as this release knows it.
        let mut changed = read.clone();
        changed.erasures[0].retries = 7;
        let rewritten = serde_json::to_value(&changed).unwrap();
        assert_eq!(rewritten["erasures"][0]["retries"], 7);
        assert!(rewritten["erasures"][0].get("remoteTask").is_none());

        for (stage, name) in [
            (ErasureStage::Proving, "proving"),
            (ErasureStage::Indexing, "indexing"),
            (ErasureStage::Published, "published"),
            (ErasureStage::Purging, "purging"),
            (ErasureStage::Purged, "purged"),
            (ErasureStage::Failed, "failed"),
        ] {
            assert_eq!(serde_json::to_value(&stage).unwrap(), name);
            assert_eq!(
                serde_json::from_value::<ErasureStage>(name.into()).unwrap(),
                stage
            );
        }
    }
}
