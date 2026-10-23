//! Recording the commit and transaction files that no branch references and
//! no record names yet: those a writer left when it crashed before
//! abandoning them, those a history rewrite left, and those from before
//! anything collected.
//!
//! A reconciliation takes the digest space a slice at a time, the number of
//! slices fixed when it starts from how many files it lists. Each step lists
//! the branch folders and checks the slice's copies against every branch's
//! history, streamed once.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use fluree_db_core::{
    ContentId, ContentKind, StorageNamespace, StorageRoot, CODEC_FLUREE_COMMIT, CODEC_FLUREE_TXN,
};
use fluree_db_nameservice::{BindingState, Fence, NameBinding};
use serde::{Deserialize, Serialize};

use super::check::{Holders, Listed};
use super::{dir_address, now_ms, Blob, GarbageReason};
use crate::{Fluree, Result};

/// What reconciling a ledger found.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileReport {
    /// Commit and transaction files read from storage: none for a step that
    /// reuses its pass's listing.
    pub listed: usize,
    /// Files in the slices checked.
    pub checked: usize,
    /// Of those, recorded as garbage.
    pub recorded: usize,
    /// Every slice is checked.
    pub complete: bool,
    pub steps: usize,
    pub timings: ReconcileTimings,
}

/// Where a reconciliation's time went.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileTimings {
    /// Loading the branches to their heads.
    pub load: Duration,
    /// Listing the branch folders and the records, and taking a slice.
    pub list: Duration,
    /// Matching a slice against the branches' histories.
    pub references: Duration,
    /// Checking candidates' commit records, and recording them.
    pub record: Duration,
}

impl ReconcileReport {
    fn add(&mut self, step: &ReconcileReport) {
        self.listed += step.listed;
        self.checked += step.checked;
        self.recorded += step.recorded;
        self.complete = step.complete;
        self.steps += step.steps;
        self.timings.load += step.timings.load;
        self.timings.list += step.timings.list;
        self.timings.references += step.timings.references;
        self.timings.record += step.timings.record;
    }
}

/// What a pass over the ledgers due for reconciliation did.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconcilePass {
    /// What reconciling each ledger did.
    pub ledgers: Vec<(String, ReconcileReport)>,
    /// Ledgers left due when the pass ended.
    pub due: usize,
    /// The earliest start of the last reconciliation of one of those.
    pub oldest_ms: Option<i64>,
}

/// A ledger's last completed reconciliation, beside its records.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reconciled {
    started_ms: i64,
    completed_ms: i64,
}

/// A reconciliation in progress: when it started, its slices of the digest
/// space, and the next to check.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reconciling {
    started_ms: i64,
    slices: u16,
    next: u16,
}

const RECONCILED: &str = "reconciled.json";
const RECONCILING: &str = "reconciling.json";

/// Files a step checks, about: a reconciliation takes the next power of two
/// of the files it lists over this many slices, at most [`MAX_SLICES`].
const SLICE: usize = 50_000;
const MAX_SLICES: usize = 256;

/// How long a purge's call reconciles, and then collects, before it waits
/// for the next.
const RESOLVE_BUDGET: Duration = Duration::from_secs(30);

fn slices_for(listed: usize, slice: usize) -> u16 {
    let slices = listed.div_ceil(slice.max(1)).next_power_of_two();
    u16::try_from(slices.min(MAX_SLICES)).expect("at most 256")
}

/// A ledger's branch folders and records, listed, and then its branches,
/// loaded, once for the steps of a pass: each file by its folder and the
/// first byte of its digest. Listing first leaves only what was being
/// published at the load to look unreferenced; collection then finds it
/// referenced.
pub(super) struct Listing {
    pub(super) holders: Option<Holders>,
    /// The branches listed, with their fences: steps list again once these
    /// change.
    branches: Vec<(String, Fence)>,
    /// When listing began.
    taken_ms: i64,
    folders: Vec<StorageNamespace>,
    buckets: Vec<Vec<(usize, ContentId)>>,
    recorded: HashSet<Blob>,
    files: usize,
}

impl Listing {
    fn lists(&self, binding: &NameBinding) -> bool {
        self.branches.len() == binding.branches.len()
            && self
                .branches
                .iter()
                .zip(&binding.branches)
                .all(|((branch, fence), listed)| *branch == listed.branch && *fence == listed.fence)
    }

    /// The files in slice `slice` of `slices`, a power of two.
    pub(super) fn slice(&self, slices: u16, slice: u16) -> Vec<Blob> {
        let width = MAX_SLICES / usize::from(slices);
        let from = usize::from(slice) * width;
        self.buckets[from..from + width]
            .iter()
            .flatten()
            .map(|(folder, id)| Blob::new(self.folders[*folder].clone(), id.clone()))
            .collect()
    }
}

impl Fluree {
    /// Record the commit and transaction files in ledger `name`'s branch
    /// folders that no branch references. `None` when the ledger is not
    /// settled, or this instance does not collect.
    pub async fn reconcile_garbage(&self, name: &str) -> Result<Option<ReconcileReport>> {
        self.reconcile_run(name, None, None, false).await
    }

    /// Steps of ledger `name`'s reconciliation from one listing, resuming
    /// the one in progress unless it started before `not_before`, until it
    /// completes or `budget` is spent; when `paced`, each step is followed
    /// by a pause as long as it took. `None` when no step could be taken.
    async fn reconcile_run(
        &self,
        name: &str,
        not_before: Option<i64>,
        budget: Option<Duration>,
        paced: bool,
    ) -> Result<Option<ReconcileReport>> {
        let started = Instant::now();
        let mut listing = None;
        let mut total: Option<ReconcileReport> = None;
        loop {
            let clock = Instant::now();
            let Some(step) = self
                .reconcile_step(name, SLICE, not_before, &mut listing)
                .await?
            else {
                return Ok(total);
            };
            let total = total.get_or_insert_with(ReconcileReport::default);
            total.add(&step);
            if paced {
                let left = budget.map_or(Duration::MAX, |b| b.saturating_sub(started.elapsed()));
                tokio::time::sleep(clock.elapsed().min(left)).await;
            }
            if step.complete || budget.is_some_and(|b| started.elapsed() >= b) {
                return Ok(Some(total.clone()));
            }
        }
    }

    /// Check the next slice of ledger `name`'s commit and transaction files,
    /// resuming the reconciliation in progress unless it started before
    /// `not_before`; a new one takes slices of about `slice` files. Lists
    /// the ledger into `listing` unless it holds a listing of its branches.
    pub(super) async fn reconcile_step(
        &self,
        name: &str,
        slice: usize,
        not_before: Option<i64>,
        listing: &mut Option<Listing>,
    ) -> Result<Option<ReconcileReport>> {
        let Some(pins) = self.blob_pins() else {
            return Ok(None);
        };
        let Some(binding) = self.publisher()?.get_binding(name).await? else {
            return Ok(None);
        };
        let binding = binding.value;
        let Some(_pass) = pins.try_collect_root(&binding.root) else {
            return Ok(None);
        };
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        let progress_address = format!("{}{RECONCILING}", dir_address(&method, &binding.root));
        let stored = match storage.read_bytes(&progress_address).await {
            Ok(bytes) => Some(serde_json::from_slice::<Reconciling>(&bytes).ok()),
            Err(fluree_db_core::Error::NotFound(_)) => None,
            Err(e) => return Err(e.into()),
        };
        let progress = stored
            .flatten()
            .filter(|p| not_before.is_none_or(|ms| p.started_ms >= ms));
        let mut timings = ReconcileTimings::default();

        // A run covers what its listings saw, so each must be taken after the
        // run started: a listing kept from before another run restarted this
        // one is taken again.
        let started_ms = progress.map_or_else(now_ms, |p| p.started_ms);
        let mut listed = 0;
        if !listing
            .as_ref()
            .is_some_and(|l| l.lists(&binding) && l.taken_ms >= started_ms)
        {
            let clock = Instant::now();
            let mut fresh = self.list_ledger(&binding).await?;
            timings.list = clock.elapsed();
            let clock = Instant::now();
            fresh.holders = self.load_holders(name, &binding).await?;
            timings.load = clock.elapsed();
            if fresh.holders.is_none() {
                return Ok(None);
            }
            listed = fresh.files;
            *listing = Some(fresh);
        }
        let clock = Instant::now();
        let listing = listing.as_ref().expect("listed");
        let holders = listing.holders.as_ref().expect("loaded");
        let progress = progress.unwrap_or_else(|| Reconciling {
            started_ms,
            slices: slices_for(listing.files, slice),
            next: 0,
        });
        let slice = Listed::new(listing.slice(progress.slices, progress.next));
        timings.list += clock.elapsed();

        let clock = Instant::now();
        let read = holders.read(&slice).await?;
        timings.references = clock.elapsed();

        let clock = Instant::now();
        let mut found = Vec::new();
        for (blob, read) in slice.blobs.iter().zip(read) {
            if read || listing.recorded.contains(blob) {
                continue;
            }
            if blob.id.content_kind() == Some(ContentKind::Commit)
                && holders.lacks_records(self, blob).await?
            {
                continue;
            }
            found.push(blob.clone());
        }
        self.record_garbage(&found, GarbageReason::Reconciled)
            .await?;
        let complete = progress.next + 1 >= progress.slices;
        if complete {
            let done = Reconciled {
                started_ms: progress.started_ms,
                completed_ms: now_ms(),
            };
            storage
                .write_bytes(
                    &reconciled_address(&method, &binding.root),
                    &serde_json::to_vec(&done)?,
                )
                .await?;
            if stored.is_some() {
                storage.delete(&progress_address).await?;
            }
        } else {
            let next = Reconciling {
                next: progress.next + 1,
                ..progress
            };
            storage
                .write_bytes(&progress_address, &serde_json::to_vec(&next)?)
                .await?;
        }
        timings.record = clock.elapsed();
        Ok(Some(ReconcileReport {
            listed,
            checked: slice.blobs.len(),
            recorded: found.len(),
            complete,
            steps: 1,
            timings,
        }))
    }

    /// Every commit and transaction file in `binding`'s branch folders, and
    /// the records under its root.
    pub(super) async fn list_ledger(&self, binding: &NameBinding) -> Result<Listing> {
        let storage = self.garbage_storage()?;
        let method = storage.storage_method();
        let mut listing = Listing {
            holders: None,
            taken_ms: now_ms(),
            branches: binding
                .branches
                .iter()
                .map(|b| (b.branch.clone(), b.fence))
                .collect(),
            folders: Vec::new(),
            buckets: vec![Vec::new(); MAX_SLICES],
            recorded: self.recorded_blobs(&binding.root).await?,
            files: 0,
        };
        for listed in &binding.branches {
            let namespace = StorageNamespace::new(binding.root.clone(), &listed.branch);
            for (dir, codec, suffix) in [
                ("commit", CODEC_FLUREE_COMMIT, ".fcv2"),
                ("txn", CODEC_FLUREE_TXN, ".json"),
            ] {
                let prefix = format!("fluree:{method}://{}/{dir}/", namespace.branch_prefix());
                for address in storage.list_prefix(&prefix).await? {
                    let id = address
                        .strip_prefix(&prefix)
                        .and_then(|f| f.strip_suffix(suffix))
                        .and_then(|digest| ContentId::from_address_digest(codec, digest));
                    if let Some(id) = id {
                        let first = id.as_cid().hash().digest().first().copied();
                        listing.buckets[usize::from(first.unwrap_or(0))]
                            .push((listing.folders.len(), id));
                        listing.files += 1;
                    }
                }
            }
            listing.folders.push(namespace);
        }
        Ok(listing)
    }

    /// Reconcile the ledgers due, least recently reconciled first, until
    /// `budget` is spent: a step at a time, each followed by a pause as long
    /// as it took. A ledger is due when its last reconciliation started more
    /// than `every` ago; one that cannot be reconciled yet is passed over.
    pub async fn reconcile_due(&self, every: Duration, budget: Duration) -> Result<ReconcilePass> {
        let mut pass = ReconcilePass::default();
        if self.blob_pins().is_none() {
            return Ok(pass);
        }
        let started = Instant::now();
        let mut left = Vec::new();
        for (since, name) in self.due_ledgers(every).await? {
            let remaining = budget.saturating_sub(started.elapsed());
            let run = if remaining.is_zero() {
                Ok(None)
            } else {
                self.reconcile_run(&name, None, Some(remaining), true).await
            };
            let complete = match run {
                Ok(Some(report)) => {
                    let complete = report.complete;
                    pass.ledgers.push((name, report));
                    complete
                }
                Ok(None) => false,
                Err(e) => {
                    tracing::warn!(ledger = %name, error = %e, "garbage reconciliation failed");
                    false
                }
            };
            if !complete {
                left.push(since);
            }
        }
        pass.due = left.len();
        pass.oldest_ms = left.into_iter().flatten().min();
        Ok(pass)
    }

    /// The active ledgers whose last reconciliation started more than
    /// `every` ago, or never did, least recently reconciled first.
    async fn due_ledgers(&self, every: Duration) -> Result<Vec<(Option<i64>, String)>> {
        let due_before = now_ms() - i64::try_from(every.as_millis()).unwrap_or(i64::MAX);
        let mut due = Vec::new();
        for (name, binding) in self.publisher()?.list_bindings().await? {
            if !matches!(binding.value.state, BindingState::Active) {
                continue;
            }
            let from = self
                .reconciled(&binding.value.root)
                .await?
                .map(|r| r.started_ms);
            if from.is_none_or(|ms| ms <= due_before) {
                due.push((from, name));
            }
        }
        due.sort();
        Ok(due)
    }

    /// The last completed reconciliation of the ledger at `root`.
    async fn reconciled(&self, root: &StorageRoot) -> Result<Option<Reconciled>> {
        let storage = self.garbage_storage()?;
        let address = reconciled_address(storage.storage_method(), root);
        match storage.read_bytes(&address).await {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            Err(fluree_db_core::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Collect, without a grace period, every file of ledger `name` that no
    /// branch references, as of a reconciliation started at or after
    /// `since_ms`, taking a step of one if there is none. `false` while that
    /// cannot be established: the reconciliation is not complete, the ledger
    /// is unsettled, or a writer pins a file.
    pub(crate) async fn resolve_garbage(&self, name: &str, since_ms: i64) -> Result<bool> {
        let Some(binding) = self.publisher()?.get_binding(name).await? else {
            return Ok(false);
        };
        let root = &binding.value.root;
        let covered = |r: Option<Reconciled>| r.is_some_and(|r| r.started_ms >= since_ms);
        if !covered(self.reconciled(root).await?) {
            let run = self
                .reconcile_run(name, Some(since_ms), Some(RESOLVE_BUDGET), true)
                .await?;
            if !run.is_some_and(|r| r.complete) || !covered(self.reconciled(root).await?) {
                return Ok(false);
            }
        }
        let mut report = super::GarbageReport::default();
        let deadline = Instant::now().checked_add(RESOLVE_BUDGET);
        self.collect_ledger(name, &binding.value, now_ms(), deadline, &mut report)
            .await?;
        Ok(self.unresolved_garbage(root).await?.is_empty())
    }

    /// The files recorded as garbage under `root`, by the last completed
    /// reconciliation or before it, and not yet collected.
    pub(crate) async fn unresolved_garbage(&self, root: &StorageRoot) -> Result<Vec<String>> {
        let Some(reconciled) = self.reconciled(root).await? else {
            return Ok(Vec::new());
        };
        let method = self.garbage_storage()?.storage_method().to_string();
        Ok(self
            .garbage_records(root)
            .await?
            .into_iter()
            .filter(|r| r.record.recorded_ms <= reconciled.completed_ms)
            .filter_map(|r| r.blob.address(&method))
            .collect())
    }

    /// The blobs recorded under `root`, from the records' names alone.
    async fn recorded_blobs(&self, root: &StorageRoot) -> Result<HashSet<Blob>> {
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        Ok(storage
            .list_prefix(&dir_address(&method, root))
            .await?
            .iter()
            .filter_map(|address| super::parse_record_address(&method, root, address))
            .collect())
    }
}

fn reconciled_address(method: &str, root: &StorageRoot) -> String {
    format!("{}{RECONCILED}", dir_address(method, root))
}
