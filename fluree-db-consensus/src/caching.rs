//! In-memory idempotency layer around an inner [`Committer`].
//!
//! Wraps another committer with two pieces of bookkeeping:
//!
//! - A TTL-bounded cache keyed by `(ledger_id, idempotency_key)` so
//!   retries with the same key collapse onto a single execution and so
//!   [`SubmissionLookup::status`] can recover the outcome after a lost
//!   response. Anonymous submissions skip the cache entirely.
//! - An admission semaphore that caps in-flight submissions and refuses
//!   new work with [`SubmissionError::Overloaded`] once the cap is
//!   reached — the bound is what keeps the per-submission body memory
//!   from growing without limit under sustained load.
//!
//! Execution itself is delegated to the inner committer; this layer
//! adds no operation-pipeline work of its own.

use crate::{
    BodyKind, CommittedSubmission, Committer, IdempotencyCacheKey, IdempotencyKey, LocalCommitter,
    MergeReceipt, MergeRequest, OperationReceipt, PushReceipt, PushRequest, RebaseReceipt,
    RebaseRequest, RevertReceipt, RevertRequest, RevertSelection, SubmissionError,
    SubmissionLookup, SubmissionState, TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use dashmap::DashMap;
use fluree_db_api::{CommitId, CommitRef, Fluree, GovernanceOptions, PolicyStats, TrackingTally};
use fluree_db_core::ledger_id::normalize_ledger_id;
use fluree_db_ledger::IndexConfig;
use moka::future::Cache;
use moka::ops::compute::Op;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default TTL for idempotency cache entries (1 hour).
///
/// After this duration, a previously-recorded submission state is forgotten;
/// the same idempotency key may then be reused for a new submission with any
/// body, and status lookups for the expired key return [`SubmissionState::Unknown`].
pub const DEFAULT_IDEMPOTENCY_TTL: Duration = Duration::from_secs(3600);

/// Total byte budget for the idempotency cache across all entries.
///
/// Bounded by bytes rather than entry count because [`CachedSubmission`]
/// size is highly variable: a tracking-enabled `TransactionReceipt` carrying
/// a `TrackingTally` (with its per-policy `HashMap<String, PolicyStats>`)
/// is typically a few KB; a bare `RevertReceipt` is on the order of bytes.
/// A fixed entry count would let resident memory drift far above what the
/// count suggests under sustained tracked traffic. 256 MiB sized to absorb
/// ~100k tracking-heavy entries before evictions start.
pub const DEFAULT_IDEMPOTENCY_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Default cap on in-flight submissions; calls beyond this count are
/// refused with [`SubmissionError::Overloaded`]. Bounding the in-flight
/// count is what keeps the per-request body memory from growing without
/// limit under sustained load.
pub const DEFAULT_PENDING_LIMIT: usize = 1024;

/// Default per-ledger in-flight cap layered under [`DEFAULT_PENDING_LIMIT`].
///
/// A burst of submissions against one ledger fills its own slice of permits
/// before drawing from the global pool, so the global cap is always
/// available to other ledgers. Sized at 1/8 of the global cap: eight
/// equally-saturated ledgers can collectively reach the global limit, but
/// no single ledger can exhaust it on its own.
pub const DEFAULT_PER_LEDGER_PENDING_LIMIT: usize = DEFAULT_PENDING_LIMIT / 8;

/// Cached state for a submission plus the hash of the body it carried.
/// The hash enables detecting the misuse case where the same idempotency
/// key is reused with a different transaction body.
#[derive(Clone)]
struct CachedSubmission {
    state: SubmissionState,
    body_hash: [u8; 32],
}

/// Outcome of [`CachingCommitter::try_claim_slot`]: either an earlier
/// submission's recorded receipt (the caller surfaces it), or a claim
/// guard the caller must hold for the duration of its own execution.
enum ClaimOutcome {
    /// A previous submission with the same key and body already completed.
    /// The caller returns this receipt without running the executor.
    AlreadyDone(OperationReceipt),
    /// This caller won the claim. The guard owns the `InFlight` slot in
    /// the moka cache; dropping it before [`ClaimGuard::commit`] runs
    /// schedules an asynchronous eviction so a cancelled transact future
    /// doesn't leave the slot stuck for the cache TTL.
    Claimed(ClaimGuard),
}

/// Guards the in-flight cache slot a winning [`try_claim_slot`] call
/// wrote. Held by the caller across executor + [`record_outcome`].
///
/// `commit` is the success ack: it disarms the drop-time cleanup so the
/// terminal state `record_outcome` just wrote isn't second-guessed.
/// Without an explicit commit, the drop spawns a best-effort eviction
/// of the slot iff it still shows `InFlight` — covering the
/// cancelled-transact-future case the cache TTL alone would leave stale
/// for an hour.
struct ClaimGuard {
    cache: Cache<IdempotencyCacheKey, CachedSubmission>,
    cache_key: IdempotencyCacheKey,
    committed: bool,
}

impl ClaimGuard {
    /// Mark the claim as committed (terminal state written), disarming
    /// the drop-time cleanup. Call once `record_outcome` has updated
    /// the cache entry to a terminal `Committed` / `Failed` state.
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // The transact future was dropped between `try_claim_slot` and
        // `commit` (HTTP timeout, client disconnect, panic in the
        // executor). Spawn a cleanup that removes the entry iff it is
        // still `InFlight`: any terminal state means a concurrent
        // writer landed a `record_outcome` and must be preserved.
        //
        // `and_compute_with` holds moka's per-key compute lock across
        // the read-decide-write so a concurrent `record_outcome`
        // (which goes through the same compute path) cannot interleave
        // — without that lock the bare `get` + `invalidate` would
        // race and erase the terminal state.
        //
        // Best-effort: if no tokio runtime is in scope (a sync test
        // dropping the guard, runtime shutdown) the cleanup is skipped
        // and the slot expires via TTL — degrading to the prior
        // behavior, not a correctness break.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let cache = self.cache.clone();
        let key = self.cache_key.clone();
        handle.spawn(async move {
            cache
                .entry(key)
                .and_compute_with(|maybe_entry| async move {
                    match maybe_entry {
                        Some(entry) if matches!(entry.value().state, SubmissionState::InFlight) => {
                            Op::Remove
                        }
                        _ => Op::Nop,
                    }
                })
                .await;
        });
    }
}

/// Estimated bytes a cache entry holds, used by moka as the weight for
/// capacity accounting.
///
/// Stack footprint of the key and `CachedSubmission` plus the heap the
/// variant payloads own. The dominant variable cost —
/// `TrackingTally::policy`, a `HashMap<String, PolicyStats>` — is walked
/// explicitly so a heavy entry registers at its true memory cost rather
/// than as a single slot. Fixed-size inline structs (e.g. `CommitId`)
/// contribute through the base `size_of` term, which is exact for them.
fn weigh_cached_submission(key: &IdempotencyCacheKey, value: &CachedSubmission) -> u32 {
    let base = mem::size_of::<IdempotencyCacheKey>() + mem::size_of::<CachedSubmission>();
    let key_heap = key.ledger_id.capacity() + key.key.as_str().len();
    let state_heap = weigh_submission_state(&value.state);
    base.saturating_add(key_heap)
        .saturating_add(state_heap)
        .min(u32::MAX as usize) as u32
}

fn weigh_submission_state(state: &SubmissionState) -> usize {
    match state {
        SubmissionState::Unknown | SubmissionState::InFlight => 0,
        SubmissionState::Committed(committed) => weigh_committed_submission(committed),
        SubmissionState::Failed(err) => weigh_submission_error(err),
    }
}

fn weigh_committed_submission(committed: &CommittedSubmission) -> usize {
    let base = mem::size_of::<CommittedSubmission>();
    let key = weigh_idempotency_key(committed.idempotency_key.as_ref());
    let tally = committed
        .tally
        .as_ref()
        .map(weigh_tracking_tally)
        .unwrap_or(0);
    let receipt = committed
        .receipt
        .as_ref()
        .map(|r| mem::size_of::<OperationReceipt>() + weigh_operation_receipt(r))
        .unwrap_or(0);
    base + key + tally + receipt
}

fn weigh_operation_receipt(receipt: &OperationReceipt) -> usize {
    match receipt {
        OperationReceipt::Transaction(tr) => {
            weigh_idempotency_key(tr.idempotency_key.as_ref())
                + tr.tally.as_ref().map(weigh_tracking_tally).unwrap_or(0)
        }
        OperationReceipt::Revert(rr) => {
            weigh_idempotency_key(rr.idempotency_key.as_ref())
                + rr.branch.capacity()
                + rr.reverted_commits.capacity() * mem::size_of::<CommitId>()
        }
        OperationReceipt::Merge(mr) => {
            weigh_idempotency_key(mr.idempotency_key.as_ref())
                + mr.source.capacity()
                + mr.target.capacity()
        }
        OperationReceipt::Rebase(rr) => {
            weigh_idempotency_key(rr.idempotency_key.as_ref()) + rr.branch.capacity()
        }
        OperationReceipt::Push(pr) => {
            weigh_idempotency_key(pr.idempotency_key.as_ref()) + pr.ledger.capacity()
        }
    }
}

fn weigh_idempotency_key(key: Option<&IdempotencyKey>) -> usize {
    key.map(|k| k.as_str().len()).unwrap_or(0)
}

fn weigh_tracking_tally(tally: &TrackingTally) -> usize {
    let time = tally.time.as_ref().map(String::capacity).unwrap_or(0);
    let policy = tally.policy.as_ref().map(weigh_policy_map).unwrap_or(0);
    let reasoning = tally
        .reasoning
        .as_ref()
        .and_then(|r| r.capped_reason.as_ref())
        .map(String::capacity)
        .unwrap_or(0);
    time + policy + reasoning
}

fn weigh_policy_map(map: &HashMap<String, PolicyStats>) -> usize {
    // Hashbrown's open-addressing layout costs roughly one control byte plus
    // one `(K, V)` slot per entry; capacity (not len) is what's allocated.
    const SLOT: usize = mem::size_of::<(String, PolicyStats)>() + 1;
    map.capacity().saturating_mul(SLOT) + map.keys().map(String::capacity).sum::<usize>()
}

fn weigh_submission_error(err: &SubmissionError) -> usize {
    match err {
        SubmissionError::Execution { message, .. } => message.capacity(),
        SubmissionError::KeyCollision
        | SubmissionError::AlreadyInFlight
        | SubmissionError::Overloaded => 0,
    }
}

/// Idempotency cache + admission control around an inner [`Committer`].
///
/// The cache is in-memory and not persisted across restarts; that is
/// acceptable because a process restart loses any in-flight submissions
/// anyway.
pub struct CachingCommitter<C: Committer = LocalCommitter> {
    executor: C,
    cache: Cache<IdempotencyCacheKey, CachedSubmission>,
    admission: Arc<Semaphore>,
    per_ledger_admission: DashMap<String, Arc<Semaphore>>,
    per_ledger_limit: usize,
}

/// RAII guard holding both an admission permits — the per-ledger slot
/// (acquired first) and the global slot. Dropping it releases both.
/// Fields are private with `_` prefixes because callers only care about
/// the lifetime effect, not direct permit access.
struct AdmissionPermits {
    _global: OwnedSemaphorePermit,
    _per_ledger: OwnedSemaphorePermit,
}

impl CachingCommitter<LocalCommitter> {
    /// Construct with the default 1-hour idempotency TTL.
    pub fn new(fluree: Arc<Fluree>, index_config: IndexConfig) -> Self {
        Self::with_ttl(fluree, index_config, DEFAULT_IDEMPOTENCY_TTL)
    }

    /// Construct with a caller-specified idempotency TTL.
    pub fn with_ttl(fluree: Arc<Fluree>, index_config: IndexConfig, ttl: Duration) -> Self {
        Self::wrapping_with_ttl(LocalCommitter::new(fluree, index_config), ttl)
    }
}

impl<C: Committer> CachingCommitter<C> {
    /// Wrap an arbitrary inner [`Committer`] with this committer's
    /// admission control and idempotency cache. Use when you want to
    /// compose this layer over something other than the default
    /// [`LocalCommitter`] (e.g. on top of the Raft-side
    /// `QueuedTransactor` so keyed retries dedup before they hit the
    /// Raft log).
    pub fn wrapping(executor: C) -> Self {
        Self::wrapping_with_ttl(executor, DEFAULT_IDEMPOTENCY_TTL)
    }

    /// Variant of [`wrapping`](Self::wrapping) with an explicit
    /// idempotency-cache TTL.
    pub fn wrapping_with_ttl(executor: C, ttl: Duration) -> Self {
        let cache = Cache::builder()
            .time_to_live(ttl)
            .max_capacity(DEFAULT_IDEMPOTENCY_CACHE_MAX_BYTES)
            .weigher(weigh_cached_submission)
            .build();
        Self {
            executor,
            cache,
            admission: Arc::new(Semaphore::new(DEFAULT_PENDING_LIMIT)),
            per_ledger_admission: DashMap::new(),
            per_ledger_limit: DEFAULT_PER_LEDGER_PENDING_LIMIT,
        }
    }

    /// Override the global in-flight pending-operation cap (defaults to
    /// [`DEFAULT_PENDING_LIMIT`]). Submissions arriving while `limit`
    /// operations are already in flight across all ledgers are refused
    /// with [`SubmissionError::Overloaded`] rather than queued.
    pub fn with_pending_limit(mut self, limit: usize) -> Self {
        self.admission = Arc::new(Semaphore::new(limit));
        self
    }

    /// Override the per-ledger in-flight cap (defaults to
    /// [`DEFAULT_PER_LEDGER_PENDING_LIMIT`]). Each ledger draws from its
    /// own slice of permits before reaching the global pool, so one
    /// ledger's burst cannot starve other ledgers of admission slots.
    ///
    /// Applies only to per-ledger semaphores created *after* this call;
    /// already-active ledgers keep the cap their semaphore was built with.
    /// Set at construction time before any submissions land.
    pub fn with_per_ledger_pending_limit(mut self, limit: usize) -> Self {
        self.per_ledger_limit = limit;
        self
    }

    /// Override the idempotency cache's byte budget (defaults to
    /// [`DEFAULT_IDEMPOTENCY_CACHE_MAX_BYTES`]). Entry weight is the
    /// per-entry footprint reported by [`weigh_cached_submission`],
    /// not a flat 1 — so a tracking-heavy entry counts against the
    /// budget proportionally to its policy / tally heap, not as a
    /// single slot.
    ///
    /// Rebuilds the cache, so call this before populating any keys.
    pub fn with_cache_capacity_bytes(mut self, max_bytes: u64) -> Self {
        let ttl = self
            .cache
            .policy()
            .time_to_live()
            .unwrap_or(DEFAULT_IDEMPOTENCY_TTL);
        self.cache = Cache::builder()
            .time_to_live(ttl)
            .max_capacity(max_bytes)
            .weigher(weigh_cached_submission)
            .build();
        self
    }

    /// Resolve (or lazily create) the per-ledger admission semaphore
    /// for `ledger_id`. The returned `Arc` is cloned from the map so
    /// the permit acquired against it stays valid even if some future
    /// cleanup pass evicts the map entry.
    fn ledger_semaphore(&self, ledger_id: &str) -> Arc<Semaphore> {
        // Fast path: entry already present. Avoids the `to_string`
        // allocation that `entry(...)` would force on a borrowed key.
        if let Some(slot) = self.per_ledger_admission.get(ledger_id) {
            return Arc::clone(slot.value());
        }
        // Slow path: lazily create. `or_insert_with` is atomic under
        // DashMap's per-bucket lock so concurrent first-touches converge
        // on a single semaphore.
        self.per_ledger_admission
            .entry(ledger_id.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(self.per_ledger_limit)))
            .value()
            .clone()
    }

    /// Try to claim one admission permit at each tier — first the
    /// per-ledger semaphore for `ledger_id`, then the global pool.
    /// Order matters: a flood against one ledger fills its own slice
    /// and is rejected before drawing from the global pool, leaving
    /// global slots available for other ledgers. Either tier hitting
    /// its cap returns [`SubmissionError::Overloaded`]; the per-ledger
    /// permit acquired first drops if the global one fails, releasing
    /// its slot for the next caller.
    ///
    /// `ledger_id` must be the canonical (`name:branch`) form — pass
    /// the result of [`normalize_ledger_id`] or [`format_ledger_id`]
    /// so retries that elide the default branch resolve to the same
    /// semaphore.
    fn try_admit(&self, ledger_id: &str) -> Result<AdmissionPermits, SubmissionError> {
        let per_ledger = self
            .ledger_semaphore(ledger_id)
            .try_acquire_owned()
            .map_err(|_| SubmissionError::Overloaded)?;
        let global = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| SubmissionError::Overloaded)?;
        Ok(AdmissionPermits {
            _global: global,
            _per_ledger: per_ledger,
        })
    }

    fn hash_request_body(request: &TransactionRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(request.body.body_hash());
        hash_governance(&mut hasher, &request.governance);
        hasher.finalize().into()
    }

    /// Atomically claim an idempotency slot in the cache.
    ///
    /// Returns [`ClaimOutcome::Claimed`] with a guard when the caller
    /// wins the claim and must execute the operation — the guard must
    /// be held across the executor + `record_outcome` and then
    /// [`commit`](ClaimGuard::commit)ed on success; dropping it
    /// without committing schedules an asynchronous eviction of the
    /// `InFlight` slot, so a cancelled transact future doesn't pin
    /// the cache key for the full TTL.
    ///
    /// Returns [`ClaimOutcome::AlreadyDone`] when an earlier submission
    /// with the same key and body already completed. Returns
    /// `Err(KeyCollision)` for a mismatched body or
    /// `Err(AlreadyInFlight)` when another caller's execution is still
    /// running.
    async fn try_claim_slot(
        &self,
        cache_key: IdempotencyCacheKey,
        body_hash: [u8; 32],
    ) -> Result<ClaimOutcome, SubmissionError> {
        // `or_insert_with_if` writes a fresh `InFlight` marker when the key
        // is absent, or replaces a prior failed attempt for the same body —
        // failures are re-attemptable. Concurrent submissions for the same
        // key see `is_fresh() == false` and collapse onto the existing
        // submission; only the caller that wins the claim goes on to execute.
        let claim = self
            .cache
            .entry(cache_key.clone())
            .or_insert_with_if(
                std::future::ready(CachedSubmission {
                    state: SubmissionState::InFlight,
                    body_hash,
                }),
                |existing| {
                    matches!(existing.state, SubmissionState::Failed(_))
                        && existing.body_hash == body_hash
                },
            )
            .await;

        if claim.is_fresh() {
            return Ok(ClaimOutcome::Claimed(ClaimGuard {
                cache: self.cache.clone(),
                cache_key,
                committed: false,
            }));
        }

        let existing = claim.into_value();
        if existing.body_hash != body_hash {
            return Err(SubmissionError::KeyCollision);
        }
        match existing.state {
            // Cache entries written by `record_outcome` always carry
            // a `Some(receipt)` — the `None` arm here is defensive,
            // covering a hypothetical post-refactor regression. The
            // `None` case in the wire-level `SubmissionState` is
            // reserved for the replicated-state fallback in
            // `SubmissionLookup` (post-leader-transition reads), which
            // doesn't flow through this map.
            SubmissionState::Committed(committed) => match committed.receipt {
                Some(r) => Ok(ClaimOutcome::AlreadyDone(*r)),
                None => Err(SubmissionError::AlreadyInFlight),
            },
            _ => Err(SubmissionError::AlreadyInFlight),
        }
    }

    /// Record the outcome of a freshly-executed claim back into the cache.
    ///
    /// `project_committed` lifts the per-operation receipt into a
    /// fully-populated [`SubmissionState::Committed`] — the caller
    /// supplies the canonical kit (op kind + commit identity) and
    /// the typed [`OperationReceipt`] together because both come
    /// from the same per-op response shape. Failures bypass the
    /// projection and store directly as [`SubmissionState::Failed`].
    async fn record_outcome<R, F>(
        &self,
        cache_key: IdempotencyCacheKey,
        body_hash: [u8; 32],
        outcome: &Result<R, SubmissionError>,
        project_committed: F,
    ) where
        F: FnOnce(&R) -> SubmissionState,
    {
        let final_state = match outcome {
            Ok(receipt) => project_committed(receipt),
            Err(err) => SubmissionState::Failed(err.clone()),
        };
        let value = CachedSubmission {
            state: final_state,
            body_hash,
        };
        // Go through `and_compute_with` rather than `cache.insert` so
        // this terminal write serializes against `ClaimGuard::drop`'s
        // cleanup on the same key (which uses the same compute path).
        // Otherwise drop's `Op::Remove` decision could race against a
        // bare insert and erase this state.
        self.cache
            .entry(cache_key)
            .and_compute_with(|_| async move { Op::Put(value) })
            .await;
    }

    fn hash_revert_body(request: &RevertRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(request.ledger_name.as_bytes());
        hasher.update([0u8]);
        hasher.update(request.branch.as_bytes());
        hasher.update([0u8]);
        match &request.selection {
            RevertSelection::Commits(commits) => {
                hasher.update([0u8]);
                hasher.update((commits.len() as u64).to_le_bytes());
                for commit in commits.iter() {
                    hash_commit_ref(&mut hasher, commit);
                }
            }
            RevertSelection::Range { from, to } => {
                hasher.update([1u8]);
                hash_commit_ref(&mut hasher, from);
                hash_commit_ref(&mut hasher, to);
            }
        }
        hasher.update([request.strategy as u8]);
        hasher.finalize().into()
    }

    fn hash_merge_body(request: &MergeRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(request.ledger_name.as_bytes());
        hasher.update([0u8]);
        hasher.update(request.source_branch.as_bytes());
        hasher.update([0u8]);
        match &request.target_branch {
            Some(target) => {
                hasher.update([1u8]);
                hasher.update(target.as_bytes());
                hasher.update([0u8]);
            }
            None => hasher.update([0u8]),
        }
        hasher.update([request.strategy as u8]);
        hasher.finalize().into()
    }

    fn hash_rebase_body(request: &RebaseRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(request.ledger_name.as_bytes());
        hasher.update([0u8]);
        hasher.update(request.branch.as_bytes());
        hasher.update([0u8]);
        hasher.update([request.strategy as u8]);
        hasher.finalize().into()
    }

    fn hash_push_body(request: &PushRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(request.ledger_id.as_bytes());
        hasher.update([0u8]);
        hasher.update((request.commits.len() as u64).to_le_bytes());
        for commit in &request.commits {
            hasher.update((commit.len() as u64).to_le_bytes());
            hasher.update(commit);
        }
        // Sort auxiliary blob keys so the hash is order-independent across
        // retries that serialize the map differently.
        let mut blob_keys: Vec<&String> = request.blobs.keys().collect();
        blob_keys.sort();
        hasher.update((blob_keys.len() as u64).to_le_bytes());
        for key in blob_keys {
            hasher.update((key.len() as u64).to_le_bytes());
            hasher.update(key.as_bytes());
            let value = &request.blobs[key];
            hasher.update((value.len() as u64).to_le_bytes());
            hasher.update(value);
        }
        hash_governance(&mut hasher, &request.governance);
        hasher.finalize().into()
    }
}

/// Fold the request's [`GovernanceOptions`] into a per-op body
/// hash so two submissions with identical bodies but different
/// auth/policy contexts don't collapse onto the same cache entry.
/// Without this, a retry that reused another caller's idempotency
/// key + body — but a different `identity` or policy — would
/// silently receive the original caller's receipt back. The
/// state-machine layer covers this via the postcard-encoded
/// envelope CID; this is the matching defense in the in-process
/// moka cache.
///
/// Determinism notes:
/// - Scalar fields (`identity`, `default_allow`) and the
///   length-prefixed string list (`policy_class`) hash to stable
///   bytes for a given input.
/// - `policy_values` keys are sorted before iteration so HashMap
///   iteration order doesn't perturb the digest.
/// - `policy` and individual `policy_values` entries are
///   `JsonValue`s and depend on `serde_json::to_string`, which is
///   only deterministic across calls from the same client that
///   produces the same insertion order (the crate enables
///   `preserve_order`). Same caveat the body itself carries — the
///   contract is "same client, same retry," not "any byte-for-byte
///   reproducer."
fn hash_governance(hasher: &mut Sha256, governance: &GovernanceOptions) {
    hasher.update(b"gov");
    match governance.identity.as_deref() {
        Some(identity) => {
            hasher.update([1u8]);
            hasher.update((identity.len() as u64).to_le_bytes());
            hasher.update(identity.as_bytes());
        }
        None => hasher.update([0u8]),
    }
    match governance.policy_class.as_ref() {
        Some(classes) => {
            hasher.update([1u8]);
            // Sort so policy-class ordering (e.g. `["a", "b"]`
            // vs `["b", "a"]`) doesn't perturb the digest — the
            // set of classes is the dedup-relevant signal, not
            // the order it was supplied in.
            let mut sorted: Vec<&String> = classes.iter().collect();
            sorted.sort();
            hasher.update((sorted.len() as u64).to_le_bytes());
            for class in sorted {
                hasher.update((class.len() as u64).to_le_bytes());
                hasher.update(class.as_bytes());
            }
        }
        None => hasher.update([0u8]),
    }
    match governance.policy.as_ref() {
        Some(policy) => {
            hasher.update([1u8]);
            let bytes = policy.to_string();
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(bytes.as_bytes());
        }
        None => hasher.update([0u8]),
    }
    match governance.policy_values.as_ref() {
        Some(values) => {
            hasher.update([1u8]);
            let mut sorted: Vec<&String> = values.keys().collect();
            sorted.sort();
            hasher.update((sorted.len() as u64).to_le_bytes());
            for key in sorted {
                hasher.update((key.len() as u64).to_le_bytes());
                hasher.update(key.as_bytes());
                let value_bytes = values[key].to_string();
                hasher.update((value_bytes.len() as u64).to_le_bytes());
                hasher.update(value_bytes.as_bytes());
            }
        }
        None => hasher.update([0u8]),
    }
    hasher.update([u8::from(governance.default_allow)]);
}

fn hash_commit_ref(hasher: &mut Sha256, commit: &CommitRef) {
    match commit {
        CommitRef::Exact(id) => {
            hasher.update([0u8]);
            hasher.update(id.to_bytes());
        }
        CommitRef::Prefix(prefix) => {
            hasher.update([1u8]);
            hasher.update(prefix.as_bytes());
            hasher.update([0u8]);
        }
        CommitRef::T(t) => {
            hasher.update([2u8]);
            hasher.update(t.to_le_bytes());
        }
    }
}

#[async_trait]
impl<C: Committer> Committer for CachingCommitter<C> {
    async fn transact(
        &self,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError> {
        // Canonicalize the ledger id once at the top so the moka
        // cache key (here), the downstream executor's idempotency
        // map, the per-ledger admission semaphore, and any CAS
        // keying it drives all agree on the form. Without this, a
        // retry that elides the default branch (`"my-db"` vs
        // `"my-db:main"`) would miss the cached outcome and resolve
        // to a different admission semaphore than the original.
        let mut request = request;
        request.ledger_id =
            normalize_ledger_id(&request.ledger_id).map_err(|e| SubmissionError::Execution {
                status: 400,
                message: format!("invalid ledger_id: {e}"),
            })?;
        let _permit = self.try_admit(&request.ledger_id)?;

        // Anonymous submissions (no idempotency key) skip the cache
        // entirely — no retry-collapse and no later status lookup.
        let Some(idempotency_key) = request.idempotency_key.clone() else {
            return self.executor.transact(request).await;
        };

        let cache_key = IdempotencyCacheKey::new(request.ledger_id.clone(), idempotency_key);
        let body_hash = Self::hash_request_body(&request);

        let guard = match self.try_claim_slot(cache_key.clone(), body_hash).await? {
            ClaimOutcome::AlreadyDone(OperationReceipt::Transaction(r)) => return Ok(r),
            ClaimOutcome::AlreadyDone(_) => return Err(SubmissionError::KeyCollision),
            ClaimOutcome::Claimed(g) => g,
        };

        // Capture the body discriminator before the move; the
        // canonical kit on the cached `Committed` state needs it
        // (`BodyKind` doubles as the public op-kind label on the
        // wire response).
        let body_kind = BodyKind::from(&request.body);
        let outcome = self.executor.transact(request).await;
        self.record_outcome(cache_key, body_hash, &outcome, |r: &TransactionReceipt| {
            SubmissionState::Committed(Box::new(CommittedSubmission {
                idempotency_key: r.idempotency_key.clone(),
                kind: body_kind,
                commit_id: r.commit.commit_id.clone(),
                t: r.commit.t,
                tally: r.tally.clone(),
                receipt: Some(Box::new(OperationReceipt::Transaction(r.clone()))),
            }))
        })
        .await;
        guard.commit();
        outcome
    }

    async fn revert(&self, request: RevertRequest) -> Result<RevertReceipt, SubmissionError> {
        // Cache key uses the same `ledger:branch` form as `transact` so a
        // single status-lookup endpoint works uniformly across op kinds.
        let ledger_id = fluree_db_api::format_ledger_id(&request.ledger_name, &request.branch);
        let _permit = self.try_admit(&ledger_id)?;

        let Some(idempotency_key) = request.idempotency_key.clone() else {
            return self.executor.revert(request).await;
        };

        let cache_key = IdempotencyCacheKey::new(ledger_id, idempotency_key);
        let body_hash = Self::hash_revert_body(&request);

        let guard = match self.try_claim_slot(cache_key.clone(), body_hash).await? {
            ClaimOutcome::AlreadyDone(OperationReceipt::Revert(r)) => return Ok(r),
            ClaimOutcome::AlreadyDone(_) => return Err(SubmissionError::KeyCollision),
            ClaimOutcome::Claimed(g) => g,
        };

        let outcome = self.executor.revert(request).await;
        self.record_outcome(cache_key, body_hash, &outcome, |r: &RevertReceipt| {
            SubmissionState::Committed(Box::new(CommittedSubmission {
                idempotency_key: r.idempotency_key.clone(),
                kind: BodyKind::Revert,
                commit_id: r.new_head_id.clone(),
                t: r.new_head_t,
                tally: None,
                receipt: Some(Box::new(OperationReceipt::Revert(r.clone()))),
            }))
        })
        .await;
        guard.commit();
        outcome
    }

    async fn merge(&self, request: MergeRequest) -> Result<MergeReceipt, SubmissionError> {
        // Namespace by `ledger:source_branch` — uniquely identifies the
        // merge from the client's perspective and is always known up
        // front, no need to pre-resolve the target.
        let ledger_id =
            fluree_db_api::format_ledger_id(&request.ledger_name, &request.source_branch);
        let _permit = self.try_admit(&ledger_id)?;

        let Some(idempotency_key) = request.idempotency_key.clone() else {
            return self.executor.merge(request).await;
        };

        let cache_key = IdempotencyCacheKey::new(ledger_id, idempotency_key);
        let body_hash = Self::hash_merge_body(&request);

        let guard = match self.try_claim_slot(cache_key.clone(), body_hash).await? {
            ClaimOutcome::AlreadyDone(OperationReceipt::Merge(r)) => return Ok(r),
            ClaimOutcome::AlreadyDone(_) => return Err(SubmissionError::KeyCollision),
            ClaimOutcome::Claimed(g) => g,
        };

        let outcome = self.executor.merge(request).await;
        self.record_outcome(cache_key, body_hash, &outcome, |r: &MergeReceipt| {
            SubmissionState::Committed(Box::new(CommittedSubmission {
                idempotency_key: r.idempotency_key.clone(),
                kind: BodyKind::Merge,
                commit_id: r.new_head_id.clone(),
                t: r.new_head_t,
                tally: None,
                receipt: Some(Box::new(OperationReceipt::Merge(r.clone()))),
            }))
        })
        .await;
        guard.commit();
        outcome
    }

    async fn rebase(&self, request: RebaseRequest) -> Result<RebaseReceipt, SubmissionError> {
        // Rebase rewrites `branch` itself, so cache by the branch being
        // rebased — natural client identifier and matches the URL they'd
        // use to check status.
        let ledger_id = fluree_db_api::format_ledger_id(&request.ledger_name, &request.branch);
        let _permit = self.try_admit(&ledger_id)?;

        let Some(idempotency_key) = request.idempotency_key.clone() else {
            return self.executor.rebase(request).await;
        };

        let cache_key = IdempotencyCacheKey::new(ledger_id, idempotency_key);
        let body_hash = Self::hash_rebase_body(&request);

        let guard = match self.try_claim_slot(cache_key.clone(), body_hash).await? {
            ClaimOutcome::AlreadyDone(OperationReceipt::Rebase(r)) => return Ok(r),
            ClaimOutcome::AlreadyDone(_) => return Err(SubmissionError::KeyCollision),
            ClaimOutcome::Claimed(g) => g,
        };

        let outcome = self.executor.rebase(request).await;
        self.record_outcome(cache_key, body_hash, &outcome, |r: &RebaseReceipt| {
            // Rebase's "commit identity" is the source's head — the
            // convergence point the branch was rebased onto. The
            // branch's own new head (replays on top) isn't recorded
            // in the receipt; clients that need it follow up via
            // commit-log.
            SubmissionState::Committed(Box::new(CommittedSubmission {
                idempotency_key: r.idempotency_key.clone(),
                kind: BodyKind::Rebase,
                commit_id: r.source_head_id.clone(),
                t: r.source_head_t,
                tally: None,
                receipt: Some(Box::new(OperationReceipt::Rebase(r.clone()))),
            }))
        })
        .await;
        guard.commit();
        outcome
    }

    async fn push(&self, request: PushRequest) -> Result<PushReceipt, SubmissionError> {
        // See the same comment in `transact` — canonicalize once at
        // the cache boundary so the moka key, the downstream
        // idempotency map, the per-ledger admission semaphore, and
        // the per-ledger CAS path all agree.
        let mut request = request;
        request.ledger_id =
            normalize_ledger_id(&request.ledger_id).map_err(|e| SubmissionError::Execution {
                status: 400,
                message: format!("invalid ledger_id: {e}"),
            })?;
        let _permit = self.try_admit(&request.ledger_id)?;

        let Some(idempotency_key) = request.idempotency_key.clone() else {
            return self.executor.push(request).await;
        };

        // Push targets a fully-qualified `ledger:branch` directly, so the
        // cache key matches `transact` namespacing.
        let cache_key = IdempotencyCacheKey::new(request.ledger_id.clone(), idempotency_key);
        let body_hash = Self::hash_push_body(&request);

        let guard = match self.try_claim_slot(cache_key.clone(), body_hash).await? {
            ClaimOutcome::AlreadyDone(OperationReceipt::Push(r)) => return Ok(r),
            ClaimOutcome::AlreadyDone(_) => return Err(SubmissionError::KeyCollision),
            ClaimOutcome::Claimed(g) => g,
        };

        let outcome = self.executor.push(request).await;
        self.record_outcome(cache_key, body_hash, &outcome, |r: &PushReceipt| {
            SubmissionState::Committed(Box::new(CommittedSubmission {
                idempotency_key: r.idempotency_key.clone(),
                kind: BodyKind::Pushed,
                commit_id: r.head_id.clone(),
                t: r.head_t,
                tally: None,
                receipt: Some(Box::new(OperationReceipt::Push(r.clone()))),
            }))
        })
        .await;
        guard.commit();
        outcome
    }
}

#[async_trait]
impl<C: Committer + SubmissionLookup> SubmissionLookup for CachingCommitter<C> {
    async fn status(&self, ledger_id: &str, key: &IdempotencyKey) -> SubmissionState {
        // Canonicalize so a status lookup with `"my-db"` finds an
        // entry recorded under `"my-db:main"` (and vice versa) —
        // same form as the write-side cache keys above. A bad
        // ledger_id falls through to `Unknown`; the route boundary
        // is where 400s get surfaced.
        let Ok(normalized) = normalize_ledger_id(ledger_id) else {
            return SubmissionState::Unknown;
        };
        let cache_key = IdempotencyCacheKey::new(&normalized, key.clone());
        match self.cache.get(&cache_key).await {
            // Terminal states in the cache are authoritative; return
            // them without paying for an inner round-trip.
            Some(entry) if !matches!(entry.state, SubmissionState::InFlight) => entry.state,
            // Cached `InFlight` may be stale: the originating
            // `try_claim_slot` writes `InFlight` before the executor
            // returns, and a cancelled transact future (HTTP timeout,
            // client disconnect) skips the `record_outcome` that
            // would overwrite it. The executor's idempotency state
            // is the canonical answer in that window — for the Raft
            // path, [`QueuedTransactor::status`] consults the
            // replicated map and returns `Committed` / `Failed` if
            // the propose actually landed.
            Some(entry) => {
                let inner = self.executor.status(&normalized, key).await;
                match &inner {
                    SubmissionState::Committed(_) | SubmissionState::Failed(_) => {
                        // Refresh the cache so subsequent polls hit
                        // fast without another inner round-trip.
                        self.cache
                            .insert(
                                cache_key,
                                CachedSubmission {
                                    state: inner.clone(),
                                    body_hash: entry.body_hash,
                                },
                            )
                            .await;
                        inner
                    }
                    // Inner doesn't know either (LocalCommitter
                    // always returns Unknown; QueuedTransactor's
                    // replicated map may not yet reflect the
                    // entry). Surface the cached `InFlight` rather
                    // than `Unknown` so the client doesn't flip
                    // from "in flight" to "never heard of it".
                    _ => SubmissionState::InFlight,
                }
            }
            // Cache miss — fall through to the inner committer. For
            // [`LocalCommitter`] this is a noop (always `Unknown`);
            // for the Raft path the inner
            // [`QueuedTransactor`](crate::raft::queued_transactor::QueuedTransactor)
            // surfaces a `Committed { receipt: None, ... }` for
            // entries any node committed through consensus —
            // including ones a different leader served before this
            // node took over.
            None => self.executor.status(&normalized, key).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransactionBody;
    use fluree_db_api::{
        CommitId, CommitRef, ConflictStrategy, FlureeBuilder, GovernanceOptions, TrackingOptions,
    };
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use serde_json::{json, Value as JsonValue};

    /// Build a Fluree + `CachingCommitter` + initialized ledger.
    ///
    /// Each test gets its own in-memory Fluree, so tests don't share state
    /// and the same `ledger_id` is safe to reuse across tests.
    async fn setup() -> (Arc<fluree_db_api::Fluree>, CachingCommitter, String) {
        let fluree = Arc::new(FlureeBuilder::memory().build_memory());
        let ledger_id = "test/committer:main".to_string();
        fluree
            .create_ledger(&ledger_id)
            .await
            .expect("create ledger");
        let index_config = fluree_db_ledger::IndexConfig {
            reindex_min_bytes: 1024 * 1024,
            reindex_max_bytes: 1024 * 1024 * 100,
        };
        let committer = CachingCommitter::new(Arc::clone(&fluree), index_config);
        (fluree, committer, ledger_id)
    }

    fn sample_insert(name: &str) -> JsonValue {
        json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{
                "@id": format!("ex:{name}"),
                "ex:name": name
            }]
        })
    }

    fn request(ledger_id: &str, key: Option<&str>, body: JsonValue) -> TransactionRequest {
        TransactionRequest {
            idempotency_key: key.map(|k| IdempotencyKey::new(k).expect("test key fits cap")),
            ledger_id: ledger_id.to_string(),
            body: TransactionBody::JsonLdInsert(body),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        }
    }

    fn cache_key(label: &str) -> IdempotencyCacheKey {
        IdempotencyCacheKey::new(
            "test/db:main",
            IdempotencyKey::new(label).expect("fits cap"),
        )
    }

    fn lightweight_committed(label: &str) -> CachedSubmission {
        CachedSubmission {
            state: SubmissionState::Committed(Box::new(CommittedSubmission {
                idempotency_key: Some(IdempotencyKey::new(label).expect("fits cap")),
                kind: BodyKind::JsonLdInsert,
                commit_id: CommitId::new(fluree_db_api::ContentKind::Commit, &[0u8]),
                t: 1,
                tally: None,
                receipt: None,
            })),
            body_hash: [0u8; 32],
        }
    }

    fn tracking_heavy_committed(label: &str, policy_entries: usize) -> CachedSubmission {
        let mut policy = HashMap::with_capacity(policy_entries);
        for i in 0..policy_entries {
            policy.insert(
                format!("urn:fluree:policy:{i:08}"),
                PolicyStats {
                    executed: i as u64,
                    allowed: i as u64,
                },
            );
        }
        CachedSubmission {
            state: SubmissionState::Committed(Box::new(CommittedSubmission {
                idempotency_key: Some(IdempotencyKey::new(label).expect("fits cap")),
                kind: BodyKind::JsonLdInsert,
                commit_id: CommitId::new(fluree_db_api::ContentKind::Commit, &[0u8]),
                t: 1,
                tally: Some(TrackingTally {
                    time: Some("12.34ms".into()),
                    fuel: Some(0.0),
                    policy: Some(policy),
                    reasoning: None,
                }),
                receipt: None,
            })),
            body_hash: [0u8; 32],
        }
    }

    #[test]
    fn weigh_charges_more_for_tracking_heavy_entries() {
        let key = cache_key("k1");
        let light_w = weigh_cached_submission(&key, &lightweight_committed("k1"));
        let heavy_w = weigh_cached_submission(&key, &tracking_heavy_committed("k1", 100));
        assert!(
            heavy_w as u64 > light_w as u64 * 4,
            "tracking-heavy entry must register substantially more weight than a bare \
             receipt; light={light_w}, heavy={heavy_w}"
        );
    }

    #[test]
    fn weigh_grows_with_policy_map_size() {
        let key = cache_key("k1");
        let small = weigh_cached_submission(&key, &tracking_heavy_committed("k1", 10));
        let large = weigh_cached_submission(&key, &tracking_heavy_committed("k1", 1000));
        assert!(
            large > small,
            "weight must scale with policy map size; small={small}, large={large}"
        );
    }

    /// Minimal Committer + SubmissionLookup stub for status-fallthrough
    /// tests. The committer methods are unreachable in these tests
    /// (the caching layer's status path doesn't call them), and the
    /// stub's `status` returns whatever the caller pre-installs.
    ///
    /// `Clone` so the test can hand one instance to the committer and
    /// keep another for inspection (`calls()`); cloning is cheap because
    /// state lives behind a single `Arc`.
    #[derive(Clone)]
    struct StubExecutor(Arc<StubExecutorInner>);

    struct StubExecutorInner {
        status_response: std::sync::Mutex<SubmissionState>,
        status_calls: std::sync::atomic::AtomicUsize,
    }

    impl StubExecutor {
        fn new(initial: SubmissionState) -> Self {
            Self(Arc::new(StubExecutorInner {
                status_response: std::sync::Mutex::new(initial),
                status_calls: std::sync::atomic::AtomicUsize::new(0),
            }))
        }

        fn calls(&self) -> usize {
            self.0
                .status_calls
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Committer for StubExecutor {
        async fn transact(
            &self,
            _request: TransactionRequest,
        ) -> Result<TransactionReceipt, SubmissionError> {
            unreachable!("status-fallthrough tests never exercise transact")
        }
        async fn revert(&self, _request: RevertRequest) -> Result<RevertReceipt, SubmissionError> {
            unreachable!("status-fallthrough tests never exercise revert")
        }
        async fn merge(&self, _request: MergeRequest) -> Result<MergeReceipt, SubmissionError> {
            unreachable!("status-fallthrough tests never exercise merge")
        }
        async fn rebase(&self, _request: RebaseRequest) -> Result<RebaseReceipt, SubmissionError> {
            unreachable!("status-fallthrough tests never exercise rebase")
        }
        async fn push(&self, _request: PushRequest) -> Result<PushReceipt, SubmissionError> {
            unreachable!("status-fallthrough tests never exercise push")
        }
    }

    #[async_trait::async_trait]
    impl SubmissionLookup for StubExecutor {
        async fn status(&self, _ledger_id: &str, _key: &IdempotencyKey) -> SubmissionState {
            self.0
                .status_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.status_response.lock().unwrap().clone()
        }
    }

    fn committed_state(label: &str) -> SubmissionState {
        SubmissionState::Committed(Box::new(CommittedSubmission {
            idempotency_key: Some(IdempotencyKey::new(label).expect("fits cap")),
            kind: BodyKind::JsonLdInsert,
            commit_id: CommitId::new(fluree_db_api::ContentKind::Commit, &[0u8]),
            t: 1,
            tally: None,
            receipt: None,
        }))
    }

    #[tokio::test]
    async fn status_falls_through_to_inner_when_cache_has_stale_in_flight() {
        let stub = StubExecutor::new(committed_state("k1"));
        // Manual construction so the test keeps a clone of `stub` for
        // post-call assertions on `calls()`.
        let cache = Cache::builder()
            .time_to_live(DEFAULT_IDEMPOTENCY_TTL)
            .max_capacity(DEFAULT_IDEMPOTENCY_CACHE_MAX_BYTES)
            .weigher(weigh_cached_submission)
            .build();
        let committer: CachingCommitter<StubExecutor> = CachingCommitter {
            executor: stub.clone(),
            cache,
            admission: Arc::new(Semaphore::new(DEFAULT_PENDING_LIMIT)),
            per_ledger_admission: DashMap::new(),
            per_ledger_limit: DEFAULT_PER_LEDGER_PENDING_LIMIT,
        };

        // Plant a stale InFlight cache entry like a dropped transact
        // future would leave behind.
        let key = IdempotencyKey::new("k1").expect("fits cap");
        let cache_key = IdempotencyCacheKey::new("tenant-a:main", key.clone());
        committer
            .cache
            .insert(
                cache_key.clone(),
                CachedSubmission {
                    state: SubmissionState::InFlight,
                    body_hash: [7u8; 32],
                },
            )
            .await;

        // Status must surface the inner committer's Committed even
        // though the cache says InFlight.
        let got = committer.status("tenant-a:main", &key).await;
        assert!(matches!(got, SubmissionState::Committed(_)));
        assert_eq!(
            stub.calls(),
            1,
            "inner status must be consulted exactly once"
        );

        // The cache should now be refreshed so a second poll hits
        // fast without a second inner round-trip.
        let got2 = committer.status("tenant-a:main", &key).await;
        assert!(matches!(got2, SubmissionState::Committed(_)));
        assert_eq!(
            stub.calls(),
            1,
            "cache refresh must short-circuit the second poll"
        );

        // Body hash must be preserved across the refresh — otherwise a
        // later retry with the same key + same body would incorrectly
        // hit KeyCollision.
        let entry = committer.cache.get(&cache_key).await.expect("refreshed");
        assert_eq!(entry.body_hash, [7u8; 32]);
    }

    #[tokio::test]
    async fn status_keeps_in_flight_when_inner_does_not_know_either() {
        let stub = StubExecutor::new(SubmissionState::Unknown);
        let cache = Cache::builder()
            .time_to_live(DEFAULT_IDEMPOTENCY_TTL)
            .max_capacity(DEFAULT_IDEMPOTENCY_CACHE_MAX_BYTES)
            .weigher(weigh_cached_submission)
            .build();
        let committer: CachingCommitter<StubExecutor> = CachingCommitter {
            executor: stub.clone(),
            cache,
            admission: Arc::new(Semaphore::new(DEFAULT_PENDING_LIMIT)),
            per_ledger_admission: DashMap::new(),
            per_ledger_limit: DEFAULT_PER_LEDGER_PENDING_LIMIT,
        };

        let key = IdempotencyKey::new("k1").expect("fits cap");
        let cache_key = IdempotencyCacheKey::new("tenant-a:main", key.clone());
        committer
            .cache
            .insert(
                cache_key,
                CachedSubmission {
                    state: SubmissionState::InFlight,
                    body_hash: [7u8; 32],
                },
            )
            .await;

        // Inner returns Unknown — caller still sees InFlight, not
        // Unknown, because the cache's InFlight is a stronger signal
        // than the inner's "haven't heard of it".
        let got = committer.status("tenant-a:main", &key).await;
        assert!(matches!(got, SubmissionState::InFlight));
    }

    #[tokio::test]
    async fn status_returns_cached_committed_without_inner_round_trip() {
        let stub = StubExecutor::new(SubmissionState::Unknown);
        let cache = Cache::builder()
            .time_to_live(DEFAULT_IDEMPOTENCY_TTL)
            .max_capacity(DEFAULT_IDEMPOTENCY_CACHE_MAX_BYTES)
            .weigher(weigh_cached_submission)
            .build();
        let committer: CachingCommitter<StubExecutor> = CachingCommitter {
            executor: stub.clone(),
            cache,
            admission: Arc::new(Semaphore::new(DEFAULT_PENDING_LIMIT)),
            per_ledger_admission: DashMap::new(),
            per_ledger_limit: DEFAULT_PER_LEDGER_PENDING_LIMIT,
        };

        let key = IdempotencyKey::new("k1").expect("fits cap");
        let cache_key = IdempotencyCacheKey::new("tenant-a:main", key.clone());
        committer
            .cache
            .insert(
                cache_key,
                CachedSubmission {
                    state: committed_state("k1"),
                    body_hash: [7u8; 32],
                },
            )
            .await;

        let got = committer.status("tenant-a:main", &key).await;
        assert!(matches!(got, SubmissionState::Committed(_)));
        assert_eq!(
            stub.calls(),
            0,
            "terminal cache entry must not consult the inner"
        );
    }

    #[tokio::test]
    async fn dropped_claim_guard_evicts_in_flight_slot() {
        let (_fluree, committer, _ledger_id) = setup().await;
        let cache_key = IdempotencyCacheKey::new(
            "tenant-a:main",
            IdempotencyKey::new("k1").expect("fits cap"),
        );
        let body_hash = [9u8; 32];

        let outcome = committer
            .try_claim_slot(cache_key.clone(), body_hash)
            .await
            .expect("first claim");
        let guard = match outcome {
            ClaimOutcome::Claimed(g) => g,
            _ => panic!("expected a fresh claim"),
        };

        // Sanity: the cache now holds an InFlight slot.
        let entry = committer
            .cache
            .get(&cache_key)
            .await
            .expect("inflight just claimed");
        assert!(matches!(entry.state, SubmissionState::InFlight));

        // Cancellation: the guard is dropped without commit, mirroring
        // an HTTP-timeout / client-disconnect on the transact future.
        drop(guard);

        // The cleanup runs on a spawned task; yield until it observes
        // and invalidates the slot. The cache TTL alone is 1h, so this
        // assertion fails fast (~100ms cap) if the guard's Drop didn't
        // spawn the cleanup.
        for _ in 0..20 {
            if committer.cache.get(&cache_key).await.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("dropped claim guard never evicted the InFlight slot");
    }

    #[tokio::test]
    async fn cancelled_claim_unblocks_subsequent_retry() {
        let (_fluree, committer, _ledger_id) = setup().await;
        let cache_key = IdempotencyCacheKey::new(
            "tenant-a:main",
            IdempotencyKey::new("k1").expect("fits cap"),
        );
        let body_hash = [9u8; 32];

        // First attempt: claim, drop without commit — simulates a
        // cancelled transact future leaving a stale InFlight slot.
        let outcome1 = committer
            .try_claim_slot(cache_key.clone(), body_hash)
            .await
            .expect("first claim");
        drop(match outcome1 {
            ClaimOutcome::Claimed(g) => g,
            _ => panic!("expected fresh claim"),
        });

        // Wait for the cleanup task to evict the slot.
        for _ in 0..20 {
            if committer.cache.get(&cache_key).await.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Retry must win the claim fresh, NOT see AlreadyInFlight.
        let outcome2 = committer
            .try_claim_slot(cache_key, body_hash)
            .await
            .expect("retry after cancelled claim");
        assert!(
            matches!(outcome2, ClaimOutcome::Claimed(_)),
            "retry after a cancelled claim must win the slot, not surface stale InFlight"
        );
    }

    #[tokio::test]
    async fn committed_claim_guard_skips_cleanup() {
        let (_fluree, committer, _ledger_id) = setup().await;
        let cache_key = IdempotencyCacheKey::new(
            "tenant-a:main",
            IdempotencyKey::new("k1").expect("fits cap"),
        );
        let body_hash = [9u8; 32];

        let outcome = committer
            .try_claim_slot(cache_key.clone(), body_hash)
            .await
            .expect("claim");
        let guard = match outcome {
            ClaimOutcome::Claimed(g) => g,
            _ => panic!("expected fresh claim"),
        };

        // Simulate record_outcome having written terminal state.
        committer
            .cache
            .insert(
                cache_key.clone(),
                CachedSubmission {
                    state: committed_state("k1"),
                    body_hash,
                },
            )
            .await;
        guard.commit();

        // Give any (unwanted) cleanup time to fire — the guard's Drop
        // must skip it because commit was called.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let entry = committer
            .cache
            .get(&cache_key)
            .await
            .expect("terminal state must survive");
        assert!(matches!(entry.state, SubmissionState::Committed(_)));
    }

    #[tokio::test]
    async fn drop_cleanup_preserves_existing_terminal_state() {
        // Regression for the `get` + `invalidate` TOCTOU in
        // `ClaimGuard::drop`: when the cache holds a terminal
        // `Committed` entry at the time the dropped guard's cleanup
        // runs, the cleanup must `Op::Nop`, not erase it. Both the
        // drop cleanup and `record_outcome` now go through moka's
        // compute lock so a concurrent terminal write cannot
        // interleave between the cleanup's read and the conditional
        // remove.
        let (_fluree, committer, _ledger_id) = setup().await;
        let cache_key = IdempotencyCacheKey::new(
            "tenant-a:main",
            IdempotencyKey::new("k1").expect("fits cap"),
        );
        let body_hash = [9u8; 32];

        let outcome = committer
            .try_claim_slot(cache_key.clone(), body_hash)
            .await
            .expect("claim");
        let guard = match outcome {
            ClaimOutcome::Claimed(g) => g,
            _ => panic!("expected fresh claim"),
        };

        // Simulate `record_outcome` landing a terminal Committed
        // entry through the same compute path it now uses.
        committer
            .cache
            .entry(cache_key.clone())
            .and_compute_with(|_| async {
                Op::Put(CachedSubmission {
                    state: committed_state("k1"),
                    body_hash,
                })
            })
            .await;

        // Drop without commit — fires the cleanup spawn against a
        // cache that already holds the terminal state.
        drop(guard);

        // Give the cleanup task time to run.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let entry = committer
            .cache
            .get(&cache_key)
            .await
            .expect("Committed entry must survive drop cleanup");
        assert!(
            matches!(entry.state, SubmissionState::Committed(_)),
            "drop cleanup must not erase a terminal Committed entry"
        );
    }

    #[tokio::test]
    async fn per_ledger_admission_isolates_tenants_under_a_flood() {
        // Global cap 4, per-ledger cap 2 — so ledger A can hold at
        // most 2 in-flight slots, leaving the other 2 globally
        // available for any other ledger.
        let (_fluree, committer, _ledger_id) = setup().await;
        let committer = committer
            .with_pending_limit(4)
            .with_per_ledger_pending_limit(2);

        // Saturate ledger A's per-ledger semaphore.
        let _a1 = committer.try_admit("tenant-a:main").expect("a slot 1");
        let _a2 = committer.try_admit("tenant-a:main").expect("a slot 2");

        // The next claim against ledger A is refused — its per-ledger
        // pool is full even though 2 global slots remain free.
        assert!(matches!(
            committer.try_admit("tenant-a:main"),
            Err(SubmissionError::Overloaded)
        ));

        // Ledger B is unaffected: it has its own per-ledger pool, and
        // the global pool has room.
        let _b1 = committer.try_admit("tenant-b:main").expect("b slot 1");

        // Ledger C also fits (slots used: a1, a2, b1 = 3 of 4).
        let _c1 = committer.try_admit("tenant-c:main").expect("c slot 1");

        // Now ledger D hits the global cap (4 of 4) — refused even
        // though D's per-ledger pool is empty.
        assert!(matches!(
            committer.try_admit("tenant-d:main"),
            Err(SubmissionError::Overloaded)
        ));
    }

    #[test]
    fn weigh_unknown_and_inflight_have_no_state_heap() {
        let key = cache_key("k1");
        let unknown = CachedSubmission {
            state: SubmissionState::Unknown,
            body_hash: [0u8; 32],
        };
        let in_flight = CachedSubmission {
            state: SubmissionState::InFlight,
            body_hash: [0u8; 32],
        };
        let unknown_w = weigh_cached_submission(&key, &unknown);
        let inflight_w = weigh_cached_submission(&key, &in_flight);
        // Both variants contribute zero state-heap, so the weight is
        // exactly the (key + value) base size plus the key heap.
        assert_eq!(unknown_w, inflight_w);
    }

    #[tokio::test]
    async fn anonymous_submission_returns_receipt() {
        let (_fluree, committer, ledger_id) = setup().await;

        let receipt = committer
            .transact(request(&ledger_id, None, sample_insert("alice")))
            .await
            .expect("submission to succeed");

        assert!(receipt.idempotency_key.is_none());
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn keyed_submission_is_visible_via_status_lookup() {
        let (_fluree, committer, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5XAMPLE001").expect("test key fits cap");

        let receipt = committer
            .transact(request(
                &ledger_id,
                Some(key.as_str()),
                sample_insert("alice"),
            ))
            .await
            .expect("submission to succeed");
        assert_eq!(receipt.idempotency_key.as_ref(), Some(&key));

        match committer.status(&ledger_id, &key).await {
            SubmissionState::Committed(committed) => match committed.receipt.map(|b| *b) {
                Some(OperationReceipt::Transaction(stored)) => {
                    assert_eq!(stored.commit.t, receipt.commit.t);
                    assert_eq!(stored.commit.commit_id, receipt.commit.commit_id);
                }
                other => panic!("expected Some(Transaction), got {other:?}"),
            },
            other => panic!("expected Committed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn status_returns_unknown_for_unseen_key() {
        let (_fluree, committer, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5UNKNOWN").expect("test key fits cap");

        assert!(matches!(
            committer.status(&ledger_id, &key).await,
            SubmissionState::Unknown
        ));
    }

    #[tokio::test]
    async fn idempotent_retry_collapses_across_default_branch_elision() {
        // `"test/committer"` (no `:branch`) and `"test/committer:main"`
        // address the same ledger. A retry that drops the default
        // branch on the second attempt must hit the same cache
        // entry, otherwise dedup is purely cosmetic and the same
        // submission re-executes against the canonical ledger.
        let (_fluree, committer, _) = setup().await;
        let key = IdempotencyKey::new("01J5NORMRETRY").expect("test key fits cap");
        let body = sample_insert("alice");

        let first = committer
            .transact(request(
                "test/committer:main",
                Some(key.as_str()),
                body.clone(),
            ))
            .await
            .expect("first submission to succeed");

        let second = committer
            .transact(request("test/committer", Some(key.as_str()), body))
            .await
            .expect("retry with elided default branch should return cached receipt");

        assert_eq!(first.commit.t, second.commit.t);
        assert_eq!(first.commit.commit_id, second.commit.commit_id);

        // Status lookup should also be tolerant to default-branch
        // elision — clients that query under the un-normalized
        // form still find the entry.
        let elided_status = committer.status("test/committer", &key).await;
        let resolved = matches!(
            &elided_status,
            SubmissionState::Committed(committed)
                if matches!(
                    committed.receipt.as_deref(),
                    Some(OperationReceipt::Transaction(_))
                ),
        );
        assert!(
            resolved,
            "status via elided ledger_id should resolve, got {elided_status:?}"
        );
    }

    #[tokio::test]
    async fn idempotent_retry_returns_cached_receipt() {
        let (_fluree, committer, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5RETRY001").expect("test key fits cap");
        let body = sample_insert("alice");

        let first = committer
            .transact(request(&ledger_id, Some(key.as_str()), body.clone()))
            .await
            .expect("first submission to succeed");

        let second = committer
            .transact(request(&ledger_id, Some(key.as_str()), body))
            .await
            .expect("retry with same body should return cached receipt");

        // Same receipt — the second call should NOT have re-executed.
        // If it had, the new transaction would advance `t` past the first.
        assert_eq!(first.commit.t, second.commit.t);
        assert_eq!(first.commit.commit_id, second.commit.commit_id);
    }

    #[tokio::test]
    async fn key_collision_with_different_body_errors() {
        let (_fluree, committer, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5COLLIDE001").expect("test key fits cap");

        committer
            .transact(request(
                &ledger_id,
                Some(key.as_str()),
                sample_insert("alice"),
            ))
            .await
            .expect("first submission to succeed");

        let err = committer
            .transact(request(
                &ledger_id,
                Some(key.as_str()),
                sample_insert("bob"),
            ))
            .await
            .expect_err("second submission with different body should fail");

        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }

    #[tokio::test]
    async fn key_collision_with_different_governance_errors() {
        // A retry that reuses the idempotency key + body but
        // changes the auth context (different `identity`) must not
        // silently receive the original caller's receipt back —
        // governance is part of what makes the submission
        // semantically unique.
        let (_fluree, committer, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5COLLIDE002").expect("test key fits cap");
        let body = sample_insert("alice");

        committer
            .transact(request(&ledger_id, Some(key.as_str()), body.clone()))
            .await
            .expect("first submission to succeed");

        let mut req = request(&ledger_id, Some(key.as_str()), body);
        req.governance.identity = Some("did:example:other".into());
        let err = committer
            .transact(req)
            .await
            .expect_err("retry with same body but different identity must collide");

        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }

    #[tokio::test]
    async fn anonymous_submissions_do_not_populate_cache() {
        let (_fluree, committer, ledger_id) = setup().await;

        committer
            .transact(request(&ledger_id, None, sample_insert("alice")))
            .await
            .expect("anonymous submission");

        // A fresh keyed submission with any body should succeed — no anonymous
        // entry should sit in the cache to clash with it.
        let key = IdempotencyKey::new("01J5FRESH001").expect("test key fits cap");
        committer
            .transact(request(
                &ledger_id,
                Some(key.as_str()),
                sample_insert("bob"),
            ))
            .await
            .expect("fresh keyed submission should succeed after anonymous");
    }

    #[tokio::test]
    async fn upsert_routes_through_consensus() {
        let (_fluree, committer, ledger_id) = setup().await;

        let mut req = request(&ledger_id, None, sample_insert("alice"));
        req.body = TransactionBody::JsonLdUpsert(sample_insert("alice"));

        let receipt = committer.transact(req).await.expect("upsert to succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn tracking_enabled_submission_carries_tally() {
        let (_fluree, committer, ledger_id) = setup().await;

        let mut req = request(&ledger_id, None, sample_insert("alice"));
        req.tracking = Some(TrackingOptions {
            track_time: true,
            track_fuel: true,
            track_policy: false,
            max_fuel: None,
        });

        let receipt = committer
            .transact(req)
            .await
            .expect("tracked submission to succeed");
        assert!(
            receipt.tally.is_some(),
            "a tracking-enabled submission should carry a tally"
        );
    }

    #[tokio::test]
    async fn policy_default_allow_permits_transaction() {
        let (_fluree, committer, ledger_id) = setup().await;

        // `default-allow: true` is a policy input — it triggers policy-context
        // construction inside the consensus layer — and it permits the write.
        let mut req = request(&ledger_id, None, sample_insert("alice"));
        req.governance = GovernanceOptions {
            default_allow: true,
            ..Default::default()
        };

        let receipt = committer
            .transact(req)
            .await
            .expect("policy-permitted transaction to succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn view_only_policy_blocks_transaction() {
        let (_fluree, committer, ledger_id) = setup().await;

        // A view-only policy grants `f:view` but never `f:modify`; with
        // `default-allow: false` the write has no grant, so the consensus
        // layer's policy enforcement must reject it.
        let body = json!({
            "@context": {"ex": "http://example.org/"},
            "insert": {"@id": "ex:john", "ex:name": "John"}
        });
        let mut req = request(&ledger_id, None, body.clone());
        req.body = TransactionBody::JsonLdUpdate(body);
        req.governance = GovernanceOptions {
            policy: Some(json!([{
                "@id": "ex:viewOnly",
                "f:action": [{"@id": "f:view"}],
                "f:allow": true
            }])),
            default_allow: false,
            ..Default::default()
        };

        let err = committer
            .transact(req)
            .await
            .expect_err("view-only policy should block the write");
        // A policy denial is a client error — it must carry a 4xx status.
        match err {
            SubmissionError::Execution { status, .. } => {
                assert!((400..500).contains(&status), "expected 4xx, got {status}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failed_submission_is_recorded_as_failed() {
        let (_fluree, committer, _ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5FAILED001").expect("test key fits cap");
        let missing = "test/missing-ledger:main";

        let err = committer
            .transact(request(missing, Some(key.as_str()), sample_insert("alice")))
            .await
            .expect_err("submission to a missing ledger should fail");
        assert!(
            matches!(err, SubmissionError::Execution { .. }),
            "expected an execution failure, got {err:?}"
        );

        match committer.status(missing, &key).await {
            SubmissionState::Failed(_) => {}
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn turtle_insert_routes_through_consensus() {
        let (_fluree, committer, ledger_id) = setup().await;

        let turtle = r#"@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" ."#;
        let req = TransactionRequest {
            idempotency_key: None,
            ledger_id: ledger_id.clone(),
            body: TransactionBody::TurtleInsert(turtle.to_string()),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        };

        let receipt = committer
            .transact(req)
            .await
            .expect("turtle insert to succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn sparql_update_routes_through_consensus() {
        let (_fluree, committer, ledger_id) = setup().await;

        let req = TransactionRequest {
            idempotency_key: None,
            ledger_id: ledger_id.clone(),
            body: TransactionBody::Sparql(
                r#"INSERT DATA { <http://example.org/alice> <http://example.org/name> "Alice" . }"#
                    .to_string(),
            ),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        };

        let receipt = committer
            .transact(req)
            .await
            .expect("SPARQL UPDATE to succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn sparql_parse_error_is_rejected() {
        let (_fluree, committer, ledger_id) = setup().await;

        let req = TransactionRequest {
            idempotency_key: None,
            ledger_id: ledger_id.clone(),
            body: TransactionBody::Sparql("INSERT DATA { this is not valid sparql".to_string()),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        };

        let err = committer
            .transact(req)
            .await
            .expect_err("malformed SPARQL should be rejected");
        match err {
            SubmissionError::Execution { status, .. } => {
                assert_eq!(status, 400, "SPARQL parse error should be 400");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failed_submission_can_be_retried() {
        let (fluree, committer, _ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5RETRYAFTERFAIL").expect("test key fits cap");
        let ledger = "test/created-later:main";
        let body = sample_insert("alice");

        // First attempt fails — the ledger does not exist yet.
        committer
            .transact(request(ledger, Some(key.as_str()), body.clone()))
            .await
            .expect_err("first attempt should fail before the ledger exists");

        // Create the ledger, then retry with the same key + body. A cached
        // `Failed` entry must not block the retry.
        fluree.create_ledger(ledger).await.expect("create ledger");
        let receipt = committer
            .transact(request(ledger, Some(key.as_str()), body))
            .await
            .expect("retry after the ledger exists should succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    /// Submit two inserts against the test ledger and return the second
    /// commit's ID — the first call seeds the genesis commit (which cannot
    /// be reverted) and the second produces the revertable commit every
    /// revert test needs.
    async fn seed_commit(committer: &CachingCommitter, ledger_id: &str, name: &str) -> CommitId {
        committer
            .transact(request(ledger_id, None, sample_insert("__genesis__")))
            .await
            .expect("genesis transaction to succeed");
        let receipt = committer
            .transact(request(ledger_id, None, sample_insert(name)))
            .await
            .expect("seed transaction to succeed");
        receipt.commit.commit_id
    }

    fn revert_request(
        key: Option<&str>,
        commit: CommitId,
        strategy: ConflictStrategy,
    ) -> RevertRequest {
        RevertRequest {
            idempotency_key: key.map(|k| IdempotencyKey::new(k).expect("test key fits cap")),
            ledger_name: "test/committer".to_string(),
            branch: "main".to_string(),
            selection: RevertSelection::single(CommitRef::Exact(commit)),
            strategy,
        }
    }

    #[tokio::test]
    async fn anonymous_revert_returns_receipt() {
        let (_fluree, committer, ledger_id) = setup().await;
        let commit = seed_commit(&committer, &ledger_id, "alice").await;

        let receipt = committer
            .revert(revert_request(None, commit, ConflictStrategy::Abort))
            .await
            .expect("revert to succeed");

        assert!(receipt.idempotency_key.is_none());
        assert_eq!(receipt.reverted_commits.len(), 1);
    }

    #[tokio::test]
    async fn idempotent_revert_returns_cached_receipt() {
        let (_fluree, committer, ledger_id) = setup().await;
        let commit = seed_commit(&committer, &ledger_id, "alice").await;
        let key = "01J5REVERTRETRY";

        let first = committer
            .revert(revert_request(
                Some(key),
                commit.clone(),
                ConflictStrategy::Abort,
            ))
            .await
            .expect("first revert to succeed");

        let second = committer
            .revert(revert_request(Some(key), commit, ConflictStrategy::Abort))
            .await
            .expect("retry with same body should return cached receipt");

        // Same receipt — the second call must not have re-executed.
        // A second revert would advance `new_head_t` past the first.
        assert_eq!(first.new_head_t, second.new_head_t);
        assert_eq!(first.new_head_id, second.new_head_id);
    }

    #[tokio::test]
    async fn keyed_revert_is_visible_via_status_lookup() {
        let (_fluree, committer, ledger_id) = setup().await;
        let commit = seed_commit(&committer, &ledger_id, "alice").await;
        let key = IdempotencyKey::new("01J5REVERTSTATUS").expect("test key fits cap");

        let receipt = committer
            .revert(revert_request(
                Some(key.as_str()),
                commit,
                ConflictStrategy::Abort,
            ))
            .await
            .expect("revert to succeed");

        match committer.status(&ledger_id, &key).await {
            SubmissionState::Committed(committed) => match committed.receipt.map(|b| *b) {
                Some(OperationReceipt::Revert(stored)) => {
                    assert_eq!(stored.new_head_t, receipt.new_head_t);
                    assert_eq!(stored.new_head_id, receipt.new_head_id);
                }
                other => panic!("expected Some(Revert), got {other:?}"),
            },
            other => panic!("expected Committed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn revert_key_collides_with_transaction_key() {
        let (_fluree, committer, ledger_id) = setup().await;
        let key = "01J5MIXEDKEY";
        // Seed a non-genesis commit so the revert target is valid.
        let commit = seed_commit(&committer, &ledger_id, "alice").await;

        // A keyed transaction claims the key on this ledger:branch.
        committer
            .transact(request(&ledger_id, Some(key), sample_insert("carol")))
            .await
            .expect("keyed transaction to succeed");

        // A revert reusing the same key must collide — the cached entry is
        // a `Transaction` receipt, not a `Revert` one, so the bodies cannot
        // match.
        let err = committer
            .revert(revert_request(Some(key), commit, ConflictStrategy::Abort))
            .await
            .expect_err("revert with a transaction's key should collide");
        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }

    /// Build a Fluree + committer + a parent branch with one commit + a child
    /// `feature` branch with one additional commit — the minimum setup a
    /// merge test needs.
    async fn setup_with_feature_branch() -> (Arc<fluree_db_api::Fluree>, CachingCommitter) {
        let (fluree, committer, parent_id) = setup().await;
        // Genesis commit on `main` so the branch has a head to fork from.
        committer
            .transact(request(&parent_id, None, sample_insert("__genesis__")))
            .await
            .expect("seed commit to succeed");
        fluree
            .create_branch("test/committer", "feature", Some("main"), None)
            .await
            .expect("create feature branch");
        // One commit on `feature` so the merge has something to apply.
        committer
            .transact(request(
                "test/committer:feature",
                None,
                sample_insert("alice"),
            ))
            .await
            .expect("commit on feature to succeed");
        (fluree, committer)
    }

    fn merge_request(key: Option<&str>) -> MergeRequest {
        MergeRequest {
            idempotency_key: key.map(|k| IdempotencyKey::new(k).expect("test key fits cap")),
            ledger_name: "test/committer".to_string(),
            source_branch: "feature".to_string(),
            target_branch: Some("main".to_string()),
            strategy: ConflictStrategy::default(),
        }
    }

    #[tokio::test]
    async fn anonymous_merge_returns_receipt() {
        let (_fluree, committer) = setup_with_feature_branch().await;

        let receipt = committer
            .merge(merge_request(None))
            .await
            .expect("merge to succeed");

        assert!(receipt.idempotency_key.is_none());
        assert_eq!(receipt.source, "feature");
        assert_eq!(receipt.target, "main");
        // `main` hasn't advanced since `feature` branched, so the merge
        // resolves to a fast-forward.
        assert!(receipt.fast_forward);
    }

    #[tokio::test]
    async fn idempotent_merge_returns_cached_receipt() {
        let (_fluree, committer) = setup_with_feature_branch().await;
        let key = "01J5MERGERETRY";

        let first = committer
            .merge(merge_request(Some(key)))
            .await
            .expect("first merge to succeed");

        let second = committer
            .merge(merge_request(Some(key)))
            .await
            .expect("retry with same body should return cached receipt");

        // Same receipt — the second call must not have re-executed.
        // A second merge attempt against the already-merged target would
        // change the head or fail; either way the t/id would differ.
        assert_eq!(first.new_head_t, second.new_head_t);
        assert_eq!(first.new_head_id, second.new_head_id);
    }

    #[tokio::test]
    async fn keyed_merge_is_visible_via_status_lookup() {
        let (_fluree, committer) = setup_with_feature_branch().await;
        let key = IdempotencyKey::new("01J5MERGESTATUS").expect("test key fits cap");

        let receipt = committer
            .merge(merge_request(Some(key.as_str())))
            .await
            .expect("merge to succeed");

        // Status namespacing for merge is `ledger:source_branch`.
        let cache_ledger_id = fluree_db_api::format_ledger_id("test/committer", "feature");
        match committer.status(&cache_ledger_id, &key).await {
            SubmissionState::Committed(committed) => match committed.receipt.map(|b| *b) {
                Some(OperationReceipt::Merge(stored)) => {
                    assert_eq!(stored.new_head_t, receipt.new_head_t);
                    assert_eq!(stored.new_head_id, receipt.new_head_id);
                    assert_eq!(stored.fast_forward, receipt.fast_forward);
                }
                other => panic!("expected Some(Merge), got {other:?}"),
            },
            other => panic!("expected Committed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn merge_key_collides_with_transaction_key() {
        let (_fluree, committer) = setup_with_feature_branch().await;
        let key = "01J5MIXEDMERGEKEY";

        // A keyed transaction on `ledger:feature` claims the cache slot.
        committer
            .transact(request(
                "test/committer:feature",
                Some(key),
                sample_insert("dave"),
            ))
            .await
            .expect("keyed transaction to succeed");

        let err = committer
            .merge(merge_request(Some(key)))
            .await
            .expect_err("merge with a transaction's key should collide");
        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }

    fn rebase_request(key: Option<&str>) -> RebaseRequest {
        RebaseRequest {
            idempotency_key: key.map(|k| IdempotencyKey::new(k).expect("test key fits cap")),
            ledger_name: "test/committer".to_string(),
            branch: "feature".to_string(),
            strategy: ConflictStrategy::default(),
        }
    }

    #[tokio::test]
    async fn anonymous_rebase_returns_receipt() {
        let (_fluree, committer) = setup_with_feature_branch().await;

        let receipt = committer
            .rebase(rebase_request(None))
            .await
            .expect("rebase to succeed");

        assert!(receipt.idempotency_key.is_none());
        assert_eq!(receipt.branch, "feature");
    }

    #[tokio::test]
    async fn idempotent_rebase_returns_cached_receipt() {
        let (_fluree, committer) = setup_with_feature_branch().await;
        let key = "01J5REBASERETRY";

        let first = committer
            .rebase(rebase_request(Some(key)))
            .await
            .expect("first rebase to succeed");

        let second = committer
            .rebase(rebase_request(Some(key)))
            .await
            .expect("retry with same body should return cached receipt");

        assert_eq!(first.source_head_t, second.source_head_t);
        assert_eq!(first.source_head_id, second.source_head_id);
        assert_eq!(first.replayed, second.replayed);
    }

    #[tokio::test]
    async fn keyed_rebase_is_visible_via_status_lookup() {
        let (_fluree, committer) = setup_with_feature_branch().await;
        let key = IdempotencyKey::new("01J5REBASESTATUS").expect("test key fits cap");

        let receipt = committer
            .rebase(rebase_request(Some(key.as_str())))
            .await
            .expect("rebase to succeed");

        // Cache namespace for rebase is `ledger:branch` (the branch being rebased).
        let cache_ledger_id = fluree_db_api::format_ledger_id("test/committer", "feature");
        match committer.status(&cache_ledger_id, &key).await {
            SubmissionState::Committed(committed) => match committed.receipt.map(|b| *b) {
                Some(OperationReceipt::Rebase(stored)) => {
                    assert_eq!(stored.source_head_t, receipt.source_head_t);
                    assert_eq!(stored.source_head_id, receipt.source_head_id);
                    assert_eq!(stored.fast_forward, receipt.fast_forward);
                }
                other => panic!("expected Some(Rebase), got {other:?}"),
            },
            other => panic!("expected Committed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rebase_key_collides_with_transaction_key() {
        let (_fluree, committer) = setup_with_feature_branch().await;
        let key = "01J5MIXEDREBASEKEY";

        // A keyed transaction on `ledger:feature` claims the cache slot.
        committer
            .transact(request(
                "test/committer:feature",
                Some(key),
                sample_insert("eve"),
            ))
            .await
            .expect("keyed transaction to succeed");

        let err = committer
            .rebase(rebase_request(Some(key)))
            .await
            .expect_err("rebase with a transaction's key should collide");
        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }

    fn push_request(key: Option<&str>, commits: Vec<Vec<u8>>) -> PushRequest {
        PushRequest {
            idempotency_key: key.map(|k| IdempotencyKey::new(k).expect("test key fits cap")),
            ledger_id: "test/committer:main".to_string(),
            commits,
            blobs: std::collections::HashMap::new(),
            governance: GovernanceOptions::default(),
        }
    }

    #[tokio::test]
    async fn empty_push_returns_execution_error() {
        let (_fluree, committer, _ledger_id) = setup().await;

        let err = committer
            .push(push_request(None, vec![]))
            .await
            .expect_err("push with no commits should be rejected");
        match err {
            SubmissionError::Execution { status, .. } => {
                assert_eq!(status, 400, "empty push must report a 400");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn submission_rejected_when_pending_cap_reached() {
        // Override the cap to zero so no permit is ever available — every
        // submission must hit `Overloaded` instead of executing.
        let (_fluree, committer, ledger_id) = setup().await;
        let committer = committer.with_pending_limit(0);

        let err = committer
            .transact(request(&ledger_id, None, sample_insert("alice")))
            .await
            .expect_err("limit=0 should refuse every submission");
        assert!(
            matches!(err, SubmissionError::Overloaded),
            "expected Overloaded, got {err:?}"
        );
    }

    #[tokio::test]
    async fn push_key_collides_with_transaction_key() {
        let (_fluree, committer, ledger_id) = setup().await;
        let key = "01J5MIXEDPUSHKEY";

        // A keyed transaction on `ledger:main` claims the cache slot.
        committer
            .transact(request(&ledger_id, Some(key), sample_insert("frank")))
            .await
            .expect("keyed transaction to succeed");

        // The push reuses the same key on the same ledger:main — the cached
        // Transaction receipt body-hash will not match the push body-hash,
        // so the slot-claim returns KeyCollision before any push validation
        // runs. Commits payload is empty for that reason.
        let err = committer
            .push(push_request(Some(key), vec![]))
            .await
            .expect_err("push with a transaction's key should collide");
        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }
}
