//! Commit transfer: push, export, and import of commit v2 blobs.
//!
//! ## Push (client → server)
//!
//! Clients submit locally-written commit v2 blobs. The server validates them
//! against the current ledger state (sequencing, retraction invariants, policy,
//! SHACL), writes the commit bytes to storage, and advances `CommitHead` via CAS.
//!
//! Key invariants:
//! - The first commit's `t` MUST equal server `next_t` (strict sequencing).
//! - Retractions MUST target facts currently asserted at that point in the batch.
//! - List retractions require exact metadata match (no hydration).
//! - Commit bytes are stored verbatim; the server does not rebuild commits.
//!
//! ## Export (server → client)
//!
//! Paginated export of commit blobs using address-cursor pagination.
//! Pages walk backward via `parents` — O(limit) per page regardless of
//! ledger size. Used by pull and clone operations.

use crate::dataset::GovernanceOptions;
use crate::error::{ApiError, Result};
use crate::ledger_manager::LedgerWriteGuard;
use crate::tx::{IndexingMode, IndexingStatus};
use crate::{Fluree, IndexConfig, LedgerHandle};
use base64::Engine as _;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ContentId;
use fluree_db_core::Storage;
use fluree_db_core::{BlobPinner, Unpublished};
use fluree_db_core::{ContentAddressedWrite, ContentKind, Flake, GraphId, Sid, TXN_META_GRAPH_ID};
use fluree_db_core::{LedgerId, StorageNamespace};
use fluree_db_core::{CODEC_FLUREE_COMMIT, CODEC_FLUREE_TXN};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::lifecycle::PendingLedger;
use fluree_db_nameservice::WriteBasis;
use fluree_db_nameservice::{CasResult, NsRecord, NsRecordSnapshot, RefKind, RefValue};
use fluree_db_novelty::{
    drop_forged_commit_flakes, generate_commit_flakes, is_forged_commit_flake,
    stamp_graph_on_commit_flakes, warn_if_forged_commit_flakes_dropped, Novelty,
};
use fluree_db_policy::PolicyContext;
use fluree_db_transact::{datatype_limit, vector_dims};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::error;

/// Base64-encoded bytes for JSON payloads.
///
/// In JSON, we encode as a base64 string to avoid large `[0,1,2,...]` arrays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Base64Bytes(pub Vec<u8>);

impl Base64Bytes {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

/// Per-frame payload ceiling used when decoding a `.flpack` during restore.
///
/// Each data frame carries one whole CAS object (commit, dict pack, index
/// leaf). The indexer packs dictionaries to a 256 MiB *target*
/// (`DEFAULT_RUN_BUDGET_BYTES`), so a pack legitimately lands a little over
/// 256 MiB once the last entry tips it across — which collides exactly with
/// the conservative pull-path default (`DEFAULT_MAX_PAYLOAD`, also 256 MiB) and
/// makes the archive's producer emit frames its own reader would refuse.
///
/// Restore reads a trusted, admin-supplied archive and SHA-256-verifies every
/// frame, and the frame length is a `u32` (the real format bound), so the cap
/// here is only a sanity ceiling against a corrupt length field — not a
/// security boundary. 1 GiB sits 4× above the dict target: it absorbs the
/// last-entry overshoot (and modest operator increases to the run budget)
/// without re-breaking on the next larger ledger, while still bounding how far
/// the streaming reader will buffer for a single malformed frame.
const RESTORE_MAX_FRAME_PAYLOAD: u32 = 1024 * 1024 * 1024;

/// Maximum number of CAS object writes a restore keeps in flight at once.
///
/// A `.flpack` for a large ledger holds tens of thousands of CAS objects
/// (e.g. DBLP: ~74k index artifacts). Writing them one-at-a-time makes the
/// restore wall-clock the *sum* of per-object storage round-trips — minutes of
/// pure latency on a remote backend like S3, enough to blow a serverless time
/// budget. Decoding stays sequential (it is CPU-cheap); the slow per-object
/// writes are dispatched into a bounded pool so they overlap. 32 is a balance
/// between latency-hiding and not exhausting the backend's connection pool.
const RESTORE_MAX_WRITES_IN_FLIGHT: usize = 32;

/// Maximum total bytes of pending write payloads held in memory at once.
///
/// The concurrency pool also bounds by bytes so a burst of large objects (a
/// 256 MiB–1 GiB dictionary pack) can't balloon memory. Small objects (the
/// common case) parallelize up to [`RESTORE_MAX_WRITES_IN_FLIGHT`]; large ones
/// throttle here instead. A single object larger than this cap is still
/// admitted once the pool drains, so it never deadlocks.
const RESTORE_MAX_BYTES_IN_FLIGHT: usize = 512 * 1024 * 1024;

/// Summary of a `.flpack` restore — the import counterpart of
/// [`Fluree::archive_ledger`]'s [`crate::pack::PackStreamResult`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreResult {
    /// Name the ledger was restored under (independent of the source name).
    pub ledger_id: String,
    /// Commit blobs ingested.
    pub commits: usize,
    /// Transaction blobs ingested.
    pub txn_blobs: usize,
    /// Index artifact blobs ingested (0 for a commits-only archive).
    pub index_artifacts: usize,
    /// Permanently erased commits received as their erasures.
    #[serde(default)]
    pub erased_commits: usize,
    /// Branches of other ledgers on this storage that held an original
    /// those erasures erase, and were made to read as the erasures do.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reconciled: Vec<String>,
    /// `t` of the restored commit head: the archive's, or the retraction
    /// of values the lineage has since removed.
    pub commit_t: i64,
    /// `t` the restored index head was set to, when the archive carried one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_t: Option<i64>,
}

/// Verify a single CAS object from a `.flpack` data frame and write it to
/// storage under `ledger_id`.
///
/// Mirrors the integrity rules the pull path uses: a commit-v2 blob hashes
/// over its canonical sub-range (so the digest is derived via
/// `verify_commit_blob`), while every other content kind is a full-bytes
/// SHA-256 verified against the CID. Reimplemented here (rather than reusing
/// `fluree-db-nameservice-sync::ingest_pack_frame`) so the API lib does not
/// pull that crate's `reqwest` tree out of dev-dependencies.
async fn ingest_cas_object<S: ContentAddressedWrite>(
    storage: &S,
    cid: &ContentId,
    bytes: &[u8],
    namespace: &StorageNamespace,
) -> Result<()> {
    let kind = cid
        .content_kind()
        .ok_or_else(|| ApiError::http(400, format!("unknown content kind for CID {cid}")))?;

    let is_commit_v2 = cid.codec() == CODEC_FLUREE_COMMIT && bytes.starts_with(b"FCV2");
    let integrity_ok = if is_commit_v2 {
        fluree_db_core::commit::codec::commit_blob_matches(cid, bytes)
    } else {
        cid.verify(bytes)
    };
    if !integrity_ok {
        return Err(ApiError::http(
            400,
            format!("integrity check failed for {cid}"),
        ));
    }

    // Verified against the CID, so the CID's digest is the object's address.
    storage
        .content_write_bytes_with_hash(kind, namespace, &cid.address_digest(), bytes)
        .await
        .map_err(|e| ApiError::internal(format!("failed to write {cid}: {e}")))?;
    Ok(())
}

impl Serialize for Base64Bytes {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = base64::engine::general_purpose::STANDARD.encode(&self.0);
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for Base64Bytes {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Base64Bytes(bytes))
    }
}

/// Request body for pushing commits to a transactor.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PushCommitsRequest {
    /// Commit v2 blobs, in order (oldest -> newest).
    pub commits: Vec<Base64Bytes>,
    /// Optional additional blobs referenced by commits (e.g. `commit.txn`).
    ///
    /// Map key is a CID string or legacy address string.
    #[serde(default)]
    pub blobs: HashMap<String, Base64Bytes>,
    /// CIDs the sender knows it cannot supply — a provenance gap it is
    /// declaring rather than hiding. Mirrors `missing_blobs` on
    /// [`ExportCommitsResponse`], so the two halves of the replication
    /// protocol describe a gap the same way.
    ///
    /// Additive on the wire. The receiver currently tolerates *undeclared*
    /// gaps too (see [`validate_required_blobs`]) so that a sender predating
    /// this field can still replicate a ledger that has one; declaring is
    /// what lets a later release tighten that back to a 400 without
    /// stranding those ledgers.
    #[serde(default)]
    pub missing_blobs: Vec<String>,
    /// Commit v2 blobs that merges in `commits` brought in, parents before
    /// children. The receiver stores these commits and never replays them.
    /// A merge commit already carries the combined changes of the branch it
    /// merged, so replaying that branch's commits would apply them twice.
    ///
    /// Storing them is still required. They are what a later walk of the
    /// commit DAG reads, and a merge commit whose parent is absent breaks
    /// every such walk.
    #[serde(default)]
    pub merged_commits: Vec<Base64Bytes>,
    /// The CIDs of `commits`, in order. Naming them keeps the identity of a
    /// commit minted with a hash other than SHA2-256, which its bytes alone
    /// do not say; without them each commit's CID is the SHA2-256 of its
    /// bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_ids: Vec<ContentId>,
    /// The CIDs of `merged_commits`, likewise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merged_commit_ids: Vec<ContentId>,
    /// Pack frames carrying permanently erased commits: their chains,
    /// latest replacements, receipts and authorizations. An erased commit
    /// of `commits` or `merged_commits` travels as empty bytes under its
    /// named CID, and its chain here; so does the chain of an erased commit
    /// the receiver holds, when it asked for it (see [`ERASURES_NEEDED`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub erasure_frames: Vec<Base64Bytes>,
    /// Every erased commit of the history the receiver already has, with
    /// its chain's digest: the receiver asks for the chains it lacks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub base_erasures: Vec<BaseErasure>,
    /// The sender's retention section for the ledger (its removal journal),
    /// merged into the receiver's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<serde_json::Value>,
}

impl PushCommitsRequest {
    /// Whether the push carries removals in its retention section: what a
    /// receiver that does not take the section loses.
    pub fn carries_removals(&self) -> bool {
        self.retention.as_ref().is_some_and(|section| {
            crate::retention::registry::ArchivedRetention::parse(section)
                .is_ok_and(|r| !r.journal.removals.is_empty())
        })
    }
}

/// An erased commit a push's receiver already has, as the sender holds its
/// erasure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseErasure {
    pub commit: ContentId,
    /// SHA-256 digest of the chain.
    pub digest: String,
    /// The chain's step count: a receiver holding as many is not behind.
    pub steps: usize,
}

/// What the principal behind a push may do beyond pushing commits.
#[derive(Debug, Clone, Copy, Default)]
pub struct PushAuthority {
    /// It may erase (what `POST /erase` needs): push erasures, and removals
    /// the receiver's journal lacks.
    pub erasures: bool,
}

/// How a push's receiver asks for erasure chains it lacks, in the message
/// of its `409`: this prefix, then a JSON array of the commits' CIDs. A
/// sender pushes their chains, then the push again (see [`erasures_needed`]
/// and `Fluree::erasure_pushes`).
pub const ERASURES_NEEDED: &str = "this server holds erased commits whose erasures it lacks: ";

/// The commits a push's `409` asks erasure chains for, if it does.
pub fn erasures_needed(message: &str) -> Option<Vec<ContentId>> {
    let listed = &message[message.find(ERASURES_NEEDED)? + ERASURES_NEEDED.len()..];
    let end = listed.find(']')?;
    serde_json::from_str(&listed[..=end]).ok()
}

/// Response body for a successful push.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushCommitsResponse {
    pub ledger: String,
    pub accepted: usize,
    pub head: PushedHead,
    /// Indexing status hints for external indexers.
    pub indexing: IndexingStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushedHead {
    pub t: i64,
    /// Content identifier for the new head commit.
    pub commit_id: ContentId,
}

/// Output of [`Fluree::prepare_push`].
///
/// Bundles everything the apply phase needs: receipt metadata
/// (`ledger`, `accepted`, `indexing_status`), the rollback snapshot
/// for local-mode bookkeeping, the ref endpoints the apply will
/// advance between (CAS `expected_prev` = `pre_push_head`; new head
/// = `new_head_*`), the post-push `final_state` to install through
/// the held write guard, and the write guard itself held across the
/// build → apply window.
///
/// `pending_blobs` is intentionally absent — push writes blobs
/// inline during the build phase (they're content-addressed by hash
/// and idempotent), so by the time `apply_push` runs the only thing
/// left is the single ref advance.
pub struct StagedPush {
    /// Target ledger id (`"<name>:<branch>"`).
    pub ledger: LedgerId,
    /// Number of commits accepted in this push.
    pub accepted: usize,
    /// Branch's pre-push head snapshot. The local apply path passes
    /// this to `RefPublisher::reset_head` to roll back a partial
    /// publish on apply failure. The Raft apply path ignores it
    /// (the `AdvanceRef` proposal either applies or doesn't).
    pub rollback_snapshot: NsRecordSnapshot,
    /// Branch's pre-push commit head. `expected_prev` for the CAS
    /// or `AdvanceRef` proposal.
    pub pre_push_head: RefValue,
    /// Head ref the apply step advances the branch to (the last
    /// commit in the push batch).
    pub new_head_id: ContentId,
    /// Companion to [`Self::new_head_id`].
    pub new_head_t: i64,
    /// Ledger write guard held across build → apply so concurrent
    /// transactions stay serialized through the apply step. Always
    /// present (push requires a managed ledger manager).
    pub write_guard: LedgerWriteGuard,
    /// Cumulative post-push state. The apply step installs this
    /// through `write_guard` after the ref advance, so the cache
    /// catches up with the new head atomically.
    pub final_state: LedgerState,
    /// Indexing-needed hint carried through to the receipt and the
    /// background indexer trigger.
    pub indexing_status: IndexingStatus,
    /// Pins on the blobs written, released once the head names them.
    pub pins: Unpublished,
}

/// Push precomputed commits to a ledger (transaction server mode).
///
/// This acquires the ledger write mutex for the duration of validation + publish,
/// so pushes are serialized with normal transactions.
impl Fluree {
    pub async fn push_commits(
        &self,
        ledger_id: &str,
        request: PushCommitsRequest,
        opts: &GovernanceOptions,
        index_config: &IndexConfig,
    ) -> Result<PushCommitsResponse> {
        self.push_commits_with(
            ledger_id,
            request,
            opts,
            index_config,
            PushAuthority::default(),
        )
        .await
    }

    /// [`Self::push_commits`], by a principal with `authority`.
    pub async fn push_commits_with(
        &self,
        ledger_id: &str,
        request: PushCommitsRequest,
        opts: &GovernanceOptions,
        index_config: &IndexConfig,
        authority: PushAuthority,
    ) -> Result<PushCommitsResponse> {
        if request.commits.is_empty()
            && request.merged_commits.is_empty()
            && (!request.erasure_frames.is_empty()
                || !request.base_erasures.is_empty()
                || request.retention.is_some())
        {
            return self
                .push_without_commits(ledger_id, request, index_config, authority)
                .await;
        }
        let staged = self
            .prepare_push_with(ledger_id, request, opts, index_config, authority)
            .await?;
        self.apply_push(staged).await
    }

    /// A push of no commits: erasures and the removal journal of history
    /// `ledger_id` has (see [`Self::receive_pushed_erasures`]).
    async fn push_without_commits(
        &self,
        ledger_id: &str,
        mut request: PushCommitsRequest,
        index_config: &IndexConfig,
        authority: PushAuthority,
    ) -> Result<PushCommitsResponse> {
        let id = LedgerId::parse(ledger_id)?;
        let record = self
            .nameservice()
            .lookup(&id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Ledger not found: {id}")))?;
        // Without history, there is nothing for them to apply to.
        let Some(head) = record.commit_head_id else {
            return Err(ApiError::http(400, "missing required field 'commits'"));
        };
        self.receive_pushed_erasures(&id, &mut request, authority)
            .await?;
        let state = self.ledger(id.as_str()).await?;
        let head = state.head_commit_id.clone().unwrap_or(head);
        Ok(PushCommitsResponse {
            ledger: id.to_string(),
            accepted: 0,
            head: PushedHead {
                t: state.t(),
                commit_id: head,
            },
            indexing: IndexingStatus {
                enabled: self.indexing_mode.is_enabled() && self.defaults_indexing_enabled(),
                needed: state.should_reindex(index_config),
                novelty_size: state.novelty_size(),
                index_t: state.index_t(),
                commit_t: state.t(),
            },
        })
    }

    /// Validate and write the push payload up to (but not including)
    /// the CAS / ref-advance step. Returns a [`StagedPush`] the
    /// caller can then apply via [`Self::apply_push`] (local
    /// single-step apply: CAS + cache install) or by proposing a
    /// single `AdvanceRef` through consensus (Raft path).
    ///
    /// The build phase:
    /// - Acquires the ledger write guard (push requires a managed
    ///   ledger manager).
    /// - Decodes + validates the commit chain (strict sequencing,
    ///   per-commit policy / SHACL / namespace / retraction checks
    ///   against an evolving novelty overlay).
    /// - Writes all referenced blobs and commit bytes to the
    ///   content store. Content-addressed, so writes are idempotent
    ///   and orphan blobs on failure are GC fodder.
    /// - Derives the post-push `final_state` and the indexing
    ///   status hint.
    ///
    /// Returns the `StagedPush` with the head endpoints (`pre_push_head`
    /// as `expected_prev`, `new_head_*` as the target) and the held
    /// write guard. The apply step performs only the single ref
    /// advance + cache install.
    pub async fn prepare_push(
        &self,
        ledger_id: &str,
        request: PushCommitsRequest,
        opts: &GovernanceOptions,
        index_config: &IndexConfig,
    ) -> Result<StagedPush> {
        self.prepare_push_with(
            ledger_id,
            request,
            opts,
            index_config,
            PushAuthority::default(),
        )
        .await
    }

    /// [`Self::prepare_push`], by a principal with `authority`. Erased
    /// history the push carries is taken first, as a pull takes it (see
    /// [`Self::receive_pushed_erasures`]); the erased commits it brings are
    /// then validated as their replacements, stored as nothing.
    pub async fn prepare_push_with(
        &self,
        ledger_id: &str,
        mut request: PushCommitsRequest,
        opts: &GovernanceOptions,
        index_config: &IndexConfig,
        authority: PushAuthority,
    ) -> Result<StagedPush> {
        if request.commits.is_empty() {
            return Err(ApiError::http(400, "missing required field 'commits'"));
        }
        let ledger_id = &LedgerId::parse(ledger_id)?;
        // Before the ledger's write lock: reconciling it takes that lock.
        let erased = self
            .receive_pushed_erasures(ledger_id, &mut request, authority)
            .await?;

        // 0) Lock ledger state for write (serialize with transactions).
        let mut guard = self
            .lock_ledger(ledger_id)
            .await?
            .ok_or_else(|| ApiError::internal("push requires a ledger manager"))?;
        let mut base_state = guard.clone_state_for_staging();

        // 1) Read current head ref (CAS expected). Use the lookup
        //    surface rather than the publisher's `get_ref` so this
        //    works in modes where the publisher trait isn't directly
        //    available (e.g., a Raft state-machine projection where
        //    writes flow through consensus proposals rather than the
        //    `RefPublisher` surface).
        let current_record = self
            .nameservice()
            .lookup(base_state.ledger_id())
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!("Ledger not found: {}", base_state.ledger_id()))
            })?;
        let current_ref = RefValue {
            id: current_record.commit_head_id.clone(),
            t: current_record.commit_t,
        };
        let rollback_snapshot = NsRecordSnapshot::from_record(&current_record);

        // 2) Decode commits and preflight strict sequencing.
        let decoded = decode_and_validate_commit_chain(base_state.ledger_id(), &request, &erased)
            .map_err(PushError::into_api_error)?;

        preflight_strict_next_t_and_prev(&current_ref, &decoded)
            .map_err(PushError::into_api_error)?;

        // 2.1) Decode the commits the chain's merges brought in, and check
        //      that no parent the bundle names is missing here.
        let merged = decode_merged_commits(&request, &erased).map_err(PushError::into_api_error)?;
        admit_bundle_hashes(self, decoded.iter().chain(&merged), &request.blobs)
            .map_err(PushError::into_api_error)?;
        validate_merged_reachable(&decoded, &merged).map_err(PushError::into_api_error)?;
        let content_store = self.branched_content_store(base_state.ledger_id()).await?;
        validate_ancestry_present(content_store.as_ref(), &decoded, &merged)
            .await
            .map_err(PushError::into_api_error)?;

        // 3) Validate referenced blobs are provided (if any) and pre-validate hashes.
        validate_required_blobs(&decoded, &merged, &request.blobs, &request.missing_blobs)
            .map_err(PushError::into_api_error)?;

        // 4) Validate each commit against evolving server view.
        //
        // We maintain an owned Novelty overlay that starts as the current novelty and
        // is extended with each accepted commit (including derived commit-metadata flakes),
        // so later commits validate against the state they would observe when applied.
        let mut evolving_novelty = (*base_state.novelty).clone();

        // Track per-commit "all flakes" so we can update state after CAS.
        let mut accepted_all_flakes: Vec<(i64, Vec<Flake>)> = Vec::with_capacity(decoded.len());

        // 4.0 The txn-meta graph's Sid once the commits apply. It stamps their
        // metadata, screens their blobs for forged commit records and routes
        // to the txn-meta graph, here and at apply.
        let txn_meta_g =
            txn_meta_sid_after(&base_state, &decoded).map_err(PushError::into_api_error)?;

        // 4.0.1 Build ONE graph routing for the whole push: every graph the
        // commits write, with the id the ledger gives it once they apply.
        let routing =
            derive_graph_routing(&base_state, &decoded).map_err(PushError::into_api_error)?;
        let mut reverse_graph = routing.graph_ids.clone();
        reverse_graph
            .entry(txn_meta_g.clone())
            .or_insert(TXN_META_GRAPH_ID);

        // The namespaces these commits introduce, which reach the snapshot only
        // in `apply_pushed_commits_to_state` — after validation. Kept as a plain
        // map so a rejected commit's violation can name the terms it brought in,
        // including ones an earlier commit of the same push introduced.
        let mut pushed_namespaces: HashMap<u16, String> = HashMap::new();

        // Datatypes the ledger holds, and those earlier commits of this push
        // add, so every commit is checked against both.
        let base_datatypes = datatype_limit::known_datatypes(&base_state);
        let mut pushed_datatypes: HashSet<Sid> = HashSet::new();

        for c in &decoded {
            // Current state is base db + evolving novelty.
            let current_t = base_state.snapshot.t.max(evolving_novelty.t);
            pushed_namespaces.extend(
                c.commit
                    .namespace_delta
                    .iter()
                    .map(|(code, prefix)| (*code, prefix.clone())),
            );

            // ns_split_mode immutability: locked once user namespaces are allocated.
            if let Some(mode) = c.commit.ns_split_mode {
                std::sync::Arc::make_mut(&mut base_state.snapshot)
                    .set_ns_split_mode(mode, c.commit.t)
                    .map_err(|e| PushError::Invalid(e.to_string()).into_api_error())?;
            }

            // 4.0.2 Datatype limit, ahead of the costlier validation below. A
            // sender that does not enforce it can push commits the index could
            // never hold. The datatypes are those that reach novelty at 4.5:
            // the blob's flakes other than forged commit records, and the
            // typed txn-meta values. The other generated metadata flakes have
            // reserved datatypes.
            let blob_datatypes = c
                .commit
                .flakes
                .iter()
                .filter(|f| !is_forged_commit_flake(f, &txn_meta_g))
                .map(|f| &f.dt);
            let meta_datatypes = datatype_limit::txn_meta_datatypes(&c.commit.txn_meta);
            let adding: Vec<Sid> = datatype_limit::new_datatypes(
                &base_datatypes,
                blob_datatypes.chain(&meta_datatypes),
            )
            .into_iter()
            .filter(|dt| !pushed_datatypes.contains(*dt))
            .cloned()
            .collect();
            datatype_limit::check_datatype_capacity(
                &base_datatypes,
                pushed_datatypes.len() + adding.len(),
            )
            .map_err(ApiError::Transact)?;
            pushed_datatypes.extend(adding);

            // 4.0.3 Vectors the index can store: checked against the ledger
            // and the earlier commits of this push, as a local write is.
            vector_dims::check_vectors(
                &base_state,
                &evolving_novelty,
                &c.commit.flakes,
                &reverse_graph,
                |sid| {
                    base_state
                        .snapshot
                        .decode_sid(sid)
                        .or_else(|| {
                            pushed_namespaces
                                .get(&sid.namespace_code)
                                .map(|prefix| format!("{prefix}{}", sid.name))
                        })
                        .unwrap_or_else(|| sid.to_string())
                },
            )
            .map_err(ApiError::Transact)?;

            // 4.1 Retraction invariant (strict), read as of `current_t`: the
            // state's `t` is the greater of the index's and novelty's.
            let evolving_state = base_state.clone_with_novelty(Arc::new(evolving_novelty.clone()));
            assert_retractions_exist(&evolving_state, &c.commit.flakes, &reverse_graph)
                .await
                .map_err(PushError::into_api_error)?;

            // 4.2 Policy enforcement: build policy context from opts against current state.
            let policy_ctx =
                build_policy_ctx_for_push(self, &base_state, &evolving_novelty, current_t, opts)
                    .await?;

            // 4.3 Stage flakes (policy/backpressure). No WHERE/cancellation; flakes are prebuilt.
            let staged_view = match stage_commit_flakes(
                evolving_state,
                &c.commit.flakes,
                index_config,
                policy_ctx.as_ref(),
                &routing.graph_ids,
            )
            .await
            {
                Ok(view) => view,
                Err(e) => {
                    // Replication is a write family like any other, and it hits
                    // the same accumulated-novelty gate. Without this, a
                    // follower whose novelty is past `reindex_max_bytes`
                    // rejects every incoming commit and never asks for the
                    // build that would drain it.
                    self.request_index_after_novelty_rejection(
                        base_state.ledger_id(),
                        current_t,
                        &e,
                    )
                    .await;
                    return Err(push_error_for_stage(e).into_api_error());
                }
            };

            // 4.4 SHACL (optional feature).
            //
            // Route through the shared post-stage helper so commit replay
            // honors the ledger's current `f:shaclEnabled` / `f:validationMode`
            // AND per-graph SHACL overrides. A commit created under `Warn` on
            // the leader (ledger-wide or per-graph) must not reject replication
            // on the follower.
            //
            // `graph_delta` is `routing.graph_iris`, which names every graph
            // the push writes, so each is validated under its own config.
            // The SHACL pass attaches the staged dictionaries to the view.
            #[cfg(feature = "shacl")]
            let mut staged_view = staged_view;
            #[cfg(feature = "shacl")]
            {
                // An unreadable config is a server-side read failure (500),
                // not a property of the pushed commits.
                let config = crate::tx::enforcement_config(&staged_view)
                    .await
                    .map_err(ApiError::from)?;
                crate::tx::apply_shacl_policy_to_staged_view(
                    &mut staged_view,
                    crate::tx::StagedShaclContext {
                        graph_delta: Some(&routing.graph_iris),
                        tracker: None,
                        staged_ns: None,
                        // These commits' own namespaces do not reach the
                        // snapshot until after validation, so a violation on a
                        // term they introduce resolves through here or not at
                        // all.
                        uncommitted_namespaces: Some(&pushed_namespaces),
                        // Replay carries no `opts` payload, so there is no
                        // authoring context to compact against — violations
                        // here name full IRIs.
                        txn_context: None,
                        // Inline shapes are an authoring-time
                        // construct; commit replay carries no
                        // `opts` payload.
                        inline_shape_bundle: None,
                        // Replay re-validates under the configured posture:
                        // any authoring-time softening was already applied
                        // (or denied) when the commit was first staged.
                        requested_validation_mode: None,
                        request_identity: None,
                        origin_validated_replay: true,
                    },
                    config.as_deref(),
                    // Commit replay doesn't engage the cross-ledger dispatch
                    // (the leader already validated against M when the
                    // commit was authored); followers re-validate
                    // same-ledger only.
                    None,
                    // Each pushed commit is governed by the state it applies
                    // to, as when it was authored.
                    None,
                )
                .await
                .map_err(|e| ApiError::http(422, e.to_string()))?;
            }
            #[cfg(not(feature = "shacl"))]
            {
                let _ = &staged_view;
            }

            // 4.5 Advance evolving novelty with this commit's flakes + derived metadata flakes.
            //
            // These commits come from a peer, so the blob's flakes are screened
            // before they reach novelty: a flake claiming commit provenance
            // cannot have come from any legitimate writer, since genuine commit
            // records are derived from the envelope just below and never ride
            // the flake stream. Without this an ingested ledger serves forged
            // commit records until it is indexed (#1846).
            let mut all_flakes = c.commit.flakes.clone();
            let mut meta_flakes =
                generate_commit_flakes(&c.commit, base_state.ledger_id(), c.commit.t);
            let dropped = drop_forged_commit_flakes(&mut all_flakes, &txn_meta_g);
            warn_if_forged_commit_flakes_dropped(dropped, base_state.ledger_id(), c.commit.t);
            stamp_graph_on_commit_flakes(&mut meta_flakes, &txn_meta_g);
            all_flakes.extend(meta_flakes);

            // Note: Novelty::apply_commit bumps to max(commit_t) internally.
            evolving_novelty
                .apply_commit(all_flakes.clone(), c.commit.t, &reverse_graph)
                .map_err(ApiError::Novelty)?;

            accepted_all_flakes.push((c.commit.t, all_flakes));
        }

        // 5) Write required blobs and commit bytes to storage. Content
        //    addressing makes these writes idempotent; each stays pinned
        //    until the head names it, and is abandoned to the garbage
        //    collector if it never does.
        let mut pins = Unpublished::new();
        let storage = self
            .backend()
            .admin_storage_cloned()
            .ok_or_else(|| ApiError::config("push_commits requires a managed storage backend"))?;
        write_required_blobs(
            &storage,
            &base_state.storage_namespace(),
            &request.blobs,
            &decoded,
            &merged,
            &mut pins,
        )
        .await
        .map_err(PushError::into_api_error)?;

        // Merged-in commits are written first. They are the parents, and a
        // failure part way through then leaves no stored commit whose parent
        // is absent.
        write_commit_blobs(
            &storage,
            &base_state.storage_namespace(),
            &merged,
            &mut pins,
        )
        .await
        .map_err(PushError::into_api_error)?;

        let stored_commits = write_commit_blobs(
            &storage,
            &base_state.storage_namespace(),
            &decoded,
            &mut pins,
        )
        .await
        .map_err(PushError::into_api_error)?;

        let final_head = stored_commits.last().expect("non-empty stored_commits");
        let new_head_id = final_head.commit_id.clone();
        let new_head_t = final_head.t;
        let accepted = decoded.len();

        // 6) Derive the post-push LedgerState. The apply step
        //    installs it through the held write guard.
        let mut new_state = apply_pushed_commits_to_state(
            base_state,
            &accepted_all_flakes,
            &decoded,
            &stored_commits,
            &txn_meta_g,
        )
        .map_err(PushError::into_api_error)?;
        self.refresh_index(&mut new_state).await?;

        // 7) Compute indexing status from the updated state.
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = new_state.should_reindex(index_config);
        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: new_state.novelty_size(),
            index_t: new_state.index_t(),
            commit_t: new_head_t,
        };

        Ok(StagedPush {
            ledger: ledger_id.clone(),
            accepted,
            rollback_snapshot,
            pre_push_head: current_ref,
            new_head_id,
            new_head_t,
            write_guard: guard,
            final_state: new_state,
            indexing_status,
            pins,
        })
    }

    /// Apply a [`StagedPush`] through the local commit pipeline:
    /// single CAS to advance the branch HEAD, then install the
    /// cumulative state through the held write guard and trigger
    /// background indexing if needed.
    ///
    /// Atomic with the build phase: blobs are already content-
    /// addressed in storage; if the CAS fails, the orphan blobs are
    /// GC fodder and the ref stays at `pre_push_head`.
    async fn apply_push(&self, staged: StagedPush) -> Result<PushCommitsResponse> {
        let StagedPush {
            ledger,
            accepted,
            rollback_snapshot: _,
            pre_push_head,
            new_head_id,
            new_head_t,
            mut write_guard,
            final_state,
            indexing_status,
            pins,
        } = staged;

        let new_ref = RefValue {
            id: Some(new_head_id.clone()),
            t: new_head_t,
        };

        match self
            .publisher()?
            .compare_and_set_ref_fenced(
                &ledger,
                final_state.write_basis(),
                RefKind::CommitHead,
                Some(&pre_push_head),
                &new_ref,
            )
            .await?
        {
            CasResult::Updated => pins.published(),
            CasResult::Conflict { actual } => {
                return Err(ApiError::http(
                    409,
                    format!(
                        "commit head changed during push (expected t={}, actual={:?})",
                        pre_push_head.t, actual
                    ),
                ));
            }
        }

        // Sync binary_store BEFORE replacing state so that concurrent readers
        // (via snapshot()) never see the new state with a stale binary_store.
        write_guard
            .ledger()
            .sync_binary_store_from_state(&final_state)
            .await;
        write_guard.replace(final_state);

        if indexing_status.enabled && indexing_status.needed {
            if let IndexingMode::Background(idx_handle) = &self.indexing_mode {
                idx_handle.trigger(&ledger, new_head_t).await;
            }
        }

        Ok(PushCommitsResponse {
            ledger: ledger.to_string(),
            accepted,
            head: PushedHead {
                t: new_head_t,
                commit_id: new_head_id,
            },
            indexing: indexing_status,
        })
    }
}

// === Internal helpers ===

#[derive(Debug)]
struct PushCommitDecoded {
    commit: fluree_db_novelty::Commit,
    bytes: Vec<u8>,
    id: ContentId,
    /// The commit's address digest (its CID's, algorithm included).
    digest_hex: String,
    /// Permanently erased: `bytes` are its replacement, which a published
    /// erasure stands for, and nothing is stored for it.
    erased: bool,
}

/// The CID of a pushed commit: the one the sender names, checked against the
/// bytes, or else the SHA2-256 of the bytes, all a sender that names none
/// can have minted.
fn pushed_commit_id(
    named: Option<&ContentId>,
    bytes: &[u8],
    what: &str,
) -> std::result::Result<ContentId, PushError> {
    match named {
        Some(id) if fluree_db_core::commit::codec::commit_blob_matches(id, bytes) => Ok(id.clone()),
        Some(id) => Err(PushError::Invalid(format!(
            "{what}: the bytes do not match the named CID {id}"
        ))),
        None => Ok(ContentId::new(ContentKind::Commit, bytes)),
    }
}

/// Refuse a bundle holding content `fluree` does not admit (see
/// [`Fluree::admits_content_id`]): its commits, the parents and
/// transactions they reference, and the blobs riding with them.
fn admit_bundle_hashes<'a>(
    fluree: &Fluree,
    commits: impl IntoIterator<Item = &'a PushCommitDecoded>,
    blobs: &HashMap<String, Base64Bytes>,
) -> std::result::Result<(), PushError> {
    let refuse = |id: &ContentId| {
        if fluree.admits_content_id(id) {
            Ok(())
        } else {
            Err(PushError::Invalid(format!(
                "{id} is not hashed with SHA2-256, the only hash this cluster admits"
            )))
        }
    };
    for decoded in commits {
        refuse(&decoded.id)?;
        for id in decoded.commit.parents.iter().chain(&decoded.commit.txn) {
            refuse(id)?;
        }
    }
    for key in blobs.keys() {
        if let Ok(id) = key.parse::<ContentId>() {
            refuse(&id)?;
        }
    }
    Ok(())
}

/// The CIDs named for `count` pushed commits, if the sender named them.
fn named_ids<'a>(
    ids: &'a [ContentId],
    count: usize,
    what: &str,
) -> std::result::Result<Option<&'a [ContentId]>, PushError> {
    match ids.len() {
        0 => Ok(None),
        n if n == count => Ok(Some(ids)),
        n => Err(PushError::Invalid(format!(
            "{what} names {n} CIDs for {count} commits"
        ))),
    }
}

#[derive(Debug, Clone)]
struct StoredCommit {
    t: i64,
    commit_id: ContentId,
}

#[derive(Debug)]
enum PushError {
    Conflict(String),
    Invalid(String),
    Forbidden(String),
    Internal(String),
}

impl PushError {
    fn into_api_error(self) -> ApiError {
        match self {
            PushError::Conflict(m) => ApiError::http(409, m),
            PushError::Invalid(m) => ApiError::http(422, m),
            PushError::Forbidden(m) => ApiError::http(403, m),
            PushError::Internal(m) => ApiError::internal(m),
        }
    }
}

/// The graph routing of a push: every graph the pushed commits' flakes
/// write, with its ledger graph id.
struct GraphRoutingResult {
    /// Graph Sid → graph id, for staging. Several Sids can name one graph:
    /// any ledger's config graph IRI is the config graph.
    graph_ids: HashMap<Sid, GraphId>,

    /// Graph id → graph IRI, for per-graph SHACL config during replay.
    #[cfg(feature = "shacl")]
    graph_iris: rustc_hash::FxHashMap<GraphId, String>,
}

/// Route every graph the pushed commits write to the id the ledger gives it.
///
/// The ledger's graph registry owns graph ids: a graph it has registered
/// keeps its id, and a graph the commits register (their `graph_delta`)
/// gets the id applying them assigns
/// ([`GraphRegistry::provisional_ids`](fluree_db_core::GraphRegistry::provisional_ids),
/// over the same set `apply_pushed_commits_to_state` registers). Ids do not
/// depend on whether the ledger is indexed.
///
/// A flake in a graph that is neither registered nor registered by the
/// commits is refused: no id is ever given to it, so the ledger could not
/// load it.
fn derive_graph_routing(
    state: &LedgerState,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<GraphRoutingResult, PushError> {
    let mut routing = GraphRoutingResult {
        graph_ids: HashMap::new(),
        #[cfg(feature = "shacl")]
        graph_iris: rustc_hash::FxHashMap::default(),
    };
    let graph_sids: HashSet<&Sid> = decoded
        .iter()
        .flat_map(|c| &c.commit.flakes)
        .filter_map(|f| f.g.as_ref())
        .collect();
    if graph_sids.is_empty() {
        return Ok(routing);
    }

    let registered_by_push: Vec<String> = decoded
        .iter()
        .flat_map(|c| c.commit.graph_delta.values().cloned())
        .collect();
    let ids = state
        .snapshot
        .graph_registry
        .provisional_ids(&registered_by_push);
    // The commits' graph Sids may use namespace codes they introduce.
    let pushed_prefix = |code: u16| {
        decoded
            .iter()
            .find_map(|c| c.commit.namespace_delta.get(&code))
    };

    for g_sid in graph_sids {
        let iri = state
            .snapshot
            .decode_sid(g_sid)
            .or_else(|| pushed_prefix(g_sid.namespace_code).map(|p| format!("{p}{}", g_sid.name)))
            .ok_or_else(|| {
                PushError::Invalid(format!(
                    "a pushed flake names a graph in unknown namespace {}",
                    g_sid.namespace_code
                ))
            })?;
        let g_id = *ids.get(iri.as_str()).ok_or_else(|| {
            PushError::Invalid(format!(
                "pushed flakes write graph <{iri}>, which neither this ledger nor the \
                 pushed commits register"
            ))
        })?;
        routing.graph_ids.insert(g_sid.clone(), g_id);
        // The IRI the ledger registers the graph by, whichever names it here.
        #[cfg(feature = "shacl")]
        routing.graph_iris.insert(
            g_id,
            state
                .snapshot
                .graph_registry
                .iri_for_graph_id(g_id)
                .map_or(iri, str::to_owned),
        );
    }
    Ok(routing)
}

/// The txn-meta graph's Sid once `decoded` applies to `state`, resolved as
/// the loader resolves it: against the namespace table the commits complete.
/// A commit writing the ledger's own config graph registers the namespace of
/// its system graph IRIs, so this can differ from the Sid before they apply.
///
/// Also checks each commit's namespace delta against the ledger's and the
/// earlier commits'.
fn txn_meta_sid_after(
    state: &LedgerState,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<Sid, PushError> {
    let mut namespaces = fluree_db_core::ns_encoding::NamespaceCodes::from_code_to_prefix(
        state.snapshot.namespaces().clone(),
    )
    .map_err(|e| PushError::Invalid(format!("base snapshot namespace corruption: {e}")))?;
    for c in decoded {
        namespaces
            .merge_delta(&c.commit.namespace_delta)
            .map_err(|e| {
                PushError::Invalid(format!(
                    "commit t={}: namespace delta conflict: {}",
                    c.commit.t, e
                ))
            })?;
    }
    // As `apply_pushed_commits_to_state` sets it.
    let mode = decoded
        .iter()
        .rev()
        .find_map(|c| c.commit.ns_split_mode)
        .unwrap_or(state.snapshot.ns_split_mode());
    Ok(fluree_db_core::ns_encoding::encode_registered(
        &fluree_db_core::txn_meta_graph_iri(state.ledger_id()),
        mode,
        |prefix| namespaces.get_code(prefix),
    ))
}

/// Refuse a transferred commit that drops graphs: commit transfer validates
/// each commit against the state the earlier ones leave, which does not yet
/// apply a drop.
fn refuse_graph_drops(
    commit: &fluree_db_core::Commit,
    which: &str,
) -> std::result::Result<(), PushError> {
    if commit.graph_drops.is_empty() {
        return Ok(());
    }
    Err(PushError::Invalid(format!(
        "{which} (t={}) drops graphs, which pushing and pulling commits does not support yet",
        commit.t
    )))
}

fn decode_and_validate_commit_chain(
    _ledger_id: &str,
    request: &PushCommitsRequest,
    erased: &HashSet<ContentId>,
) -> std::result::Result<Vec<PushCommitDecoded>, PushError> {
    let mut out = Vec::with_capacity(request.commits.len());

    let mut prev_t: Option<i64> = None;
    let mut prev_hash: Option<String> = None;
    let ids = named_ids(&request.commit_ids, request.commits.len(), "commit_ids")?;

    for (idx, b64) in request.commits.iter().enumerate() {
        let bytes = b64.0.clone();
        let commit = fluree_db_core::commit::codec::read_commit(&bytes)
            .map_err(|e| PushError::Invalid(format!("invalid commit[{idx}]: {e}")))?;
        refuse_graph_drops(&commit, &format!("commit[{idx}]"))?;

        let named = ids.map(|ids| &ids[idx]);
        let is_erased = named.is_some_and(|id| erased.contains(id));

        // Reject empty commits (no flakes) - keep semantics clear. A merge
        // commit is exempt: it records the merge even when the strategy
        // resolved every flake away, and dropping it would let the same
        // branch be merged again. So is a commit recording only transaction
        // metadata (an erasure's authorization), and an erased commit, all
        // of whose operations an erasure may have removed.
        if commit.flakes.is_empty()
            && commit.txn_meta.is_empty()
            && commit.parents.len() < 2
            && !is_erased
        {
            return Err(PushError::Invalid(format!(
                "invalid commit[{idx}]: empty commit (no flakes)"
            )));
        }

        let id = match named {
            // Its replacement, read from the published erasure.
            Some(id) if is_erased => id.clone(),
            _ => pushed_commit_id(named, &bytes, &format!("commit[{idx}]"))?,
        };
        let digest_hex = id.address_digest();

        // Note: commit blobs are applied to the server-selected `ledger_id` via CAS.
        // We do not currently enforce a ledger identity embedded inside the commit bytes.

        // Chain validation: strict contiguous t (+1).
        if let Some(pt) = prev_t {
            if commit.t != pt + 1 {
                return Err(PushError::Invalid(format!(
                    "commit chain is not contiguous: commit[{}].t={} does not equal prior+1={}",
                    idx,
                    commit.t,
                    pt + 1
                )));
            }
        }

        // Chain validation: the first parent must be the prior commit. A
        // merge commit's remaining parents are the branches it merged, and
        // they arrive in `merged_commits`. Accepting a match on one of those
        // would let a push rewrite the branch's own line of descent, which
        // is the line this receiver replays.
        if let Some(prev_hash_hex) = &prev_hash {
            let ok = commit
                .parents
                .first()
                .is_some_and(|r| r.address_digest() == *prev_hash_hex);
            if !ok {
                return Err(PushError::Invalid(format!(
                    "commit chain previous mismatch at commit[{idx}]: expected previous digest '{prev_hash_hex}'"
                )));
            }
        }

        prev_t = Some(commit.t);
        prev_hash = Some(digest_hex.clone());

        out.push(PushCommitDecoded {
            commit,
            bytes,
            id,
            digest_hex,
            erased: is_erased,
        });
    }

    Ok(out)
}

/// Decode the commits that merges in the bundle brought in.
///
/// These commits carry none of the chain's rules. They belong to other
/// branches, so their `t` values come from other clocks and need not be
/// contiguous with anything. The receiver stores them without replaying
/// them.
fn decode_merged_commits(
    request: &PushCommitsRequest,
    erased: &HashSet<ContentId>,
) -> std::result::Result<Vec<PushCommitDecoded>, PushError> {
    let ids = named_ids(
        &request.merged_commit_ids,
        request.merged_commits.len(),
        "merged_commit_ids",
    )?;
    request
        .merged_commits
        .iter()
        .enumerate()
        .map(|(idx, b64)| {
            let bytes = b64.0.clone();
            let commit = fluree_db_core::commit::codec::read_commit(&bytes)
                .map_err(|e| PushError::Invalid(format!("invalid merged_commits[{idx}]: {e}")))?;
            refuse_graph_drops(&commit, &format!("merged_commits[{idx}]"))?;
            let named = ids.map(|ids| &ids[idx]);
            let is_erased = named.is_some_and(|id| erased.contains(id));
            let id = match named {
                Some(id) if is_erased => id.clone(),
                _ => pushed_commit_id(named, &bytes, &format!("merged_commits[{idx}]"))?,
            };
            let digest_hex = id.address_digest();
            Ok(PushCommitDecoded {
                commit,
                bytes,
                id,
                digest_hex,
                erased: is_erased,
            })
        })
        .collect()
}

/// Check that every merged commit is reachable from a merge in the chain.
///
/// A merged commit is stored without being replayed or validated. One that
/// no merge references would sit in the ledger's storage unreachable. It
/// would also survive `drop`, which finds what to delete by walking the
/// history.
fn validate_merged_reachable(
    chain: &[PushCommitDecoded],
    merged: &[PushCommitDecoded],
) -> std::result::Result<(), PushError> {
    let by_digest: HashMap<&str, &PushCommitDecoded> =
        merged.iter().map(|c| (c.digest_hex.as_str(), c)).collect();
    // Walk from the chain's merge parents through the merged commits.
    let mut reached = HashSet::new();
    let mut stack: Vec<String> = chain
        .iter()
        .flat_map(|c| c.commit.parents.iter().skip(1))
        .map(ContentId::address_digest)
        .collect();
    while let Some(digest) = stack.pop() {
        let Some(c) = by_digest.get(digest.as_str()) else {
            continue;
        };
        if reached.insert(c.digest_hex.as_str()) {
            stack.extend(c.commit.parents.iter().map(ContentId::address_digest));
        }
    }

    match merged
        .iter()
        .find(|c| !reached.contains(c.digest_hex.as_str()))
    {
        Some(c) => Err(PushError::Invalid(format!(
            "merged commit {} is not reachable from any merge in the push",
            c.digest_hex
        ))),
        None => Ok(()),
    }
}

/// Check that every parent the bundle references is either in the bundle or
/// already in storage.
///
/// A commit whose parent is missing breaks every later walk of the DAG:
/// verification, merge, and export all read parents. The push is rejected
/// instead, which leaves the branch where it was.
///
/// `store` is branch-aware. A merge parent of a branch push often lives in
/// the source branch's namespace rather than the pushed branch's own.
async fn validate_ancestry_present<S: fluree_db_core::ContentStore + ?Sized>(
    store: &S,
    chain: &[PushCommitDecoded],
    merged: &[PushCommitDecoded],
) -> std::result::Result<(), PushError> {
    let bundled: HashSet<&str> = chain
        .iter()
        .chain(merged)
        .map(|c| c.digest_hex.as_str())
        .collect();

    for c in chain.iter().chain(merged) {
        for parent in &c.commit.parents {
            if bundled.contains(parent.address_digest().as_str()) {
                continue;
            }
            let present = store
                .has(parent)
                .await
                .map_err(|e| PushError::Internal(format!("failed to read {parent}: {e}")))?;
            if !present {
                return Err(PushError::Invalid(format!(
                    "commit {} references parent {parent}, which is neither in the push nor in storage",
                    c.digest_hex
                )));
            }
        }
    }

    Ok(())
}

fn preflight_strict_next_t_and_prev(
    current: &RefValue,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<(), PushError> {
    let first = decoded.first().expect("non-empty decoded");

    let expected_t = current.t + 1;
    if first.commit.t != expected_t {
        return Err(PushError::Conflict(format!(
            "first commit t mismatch: expected next-t={}, got {}",
            expected_t, first.commit.t
        )));
    }

    // Validate that the first parent is the current head CID. As in
    // `decode_and_validate_commit_chain`, a match on a merge parent is not
    // enough: the branch's line of descent has to continue from the head.
    if let Some(expected_id) = &current.id {
        let ok = first
            .commit
            .parents
            .first()
            .is_some_and(|r| r == expected_id);
        if !ok {
            return Err(PushError::Conflict(format!(
                "first commit previous mismatch: first parent is not the expected head {expected_id:?}"
            )));
        }
    } else if !first.commit.parents.is_empty() {
        return Err(PushError::Conflict(
            "first commit has parent refs but current head has no id".to_string(),
        ));
    }

    Ok(())
}

/// Check the txn blobs a push bundle carries against what its commits
/// reference. A referenced blob the client did not provide is tolerated
/// (logged): the source may itself have a dangling reference, and refusing
/// the push would make that ledger impossible to replicate. Blobs that ARE
/// provided must hash-verify.
///
/// A gap the sender listed in `declared_missing` is expected and logged at
/// debug. An undeclared gap is accepted too — a sender predating
/// `PushCommitsRequest::missing_blobs` cannot declare one — but warns,
/// because it is indistinguishable from a client that simply forgot to
/// attach `blobs`.
fn validate_required_blobs(
    decoded: &[PushCommitDecoded],
    merged: &[PushCommitDecoded],
    provided: &HashMap<String, Base64Bytes>,
    declared_missing: &[String],
) -> std::result::Result<(), PushError> {
    let required = required_txn_cids(decoded, merged);

    let declared: HashSet<&str> = declared_missing.iter().map(String::as_str).collect();
    for addr in &required {
        if provided.contains_key(addr) {
            continue;
        }
        if declared.contains(addr.as_str()) {
            tracing::debug!(
                txn_cid = %addr,
                "pushed commit references a txn blob the sender declared missing; accepting commit without it"
            );
        } else {
            tracing::warn!(
                txn_cid = %addr,
                "pushed commit references a txn blob the client neither provided nor declared missing; accepting commit without it"
            );
        }
    }

    Ok(())
}

/// The txn blob CIDs a bundle's commits reference. An erased commit's
/// travels with nothing: it holds what the erasure removed.
fn required_txn_cids(
    decoded: &[PushCommitDecoded],
    merged: &[PushCommitDecoded],
) -> HashSet<String> {
    decoded
        .iter()
        .chain(merged)
        .filter(|c| !c.erased)
        .filter_map(|c| c.commit.txn.as_ref().map(ToString::to_string))
        .collect()
}

async fn build_policy_ctx_for_push(
    fluree: &Fluree,
    base: &LedgerState,
    evolving: &Novelty,
    current_t: i64,
    opts: &GovernanceOptions,
) -> Result<Option<PolicyContext>> {
    // Build policy context from opts merged with the ledger's #config policy
    // defaults, against current state (db + evolving novelty). Config-aware:
    // honors f:policySource (same-ledger named graphs and cross-ledger model
    // references) instead of assuming policy rules live in the default graph.
    crate::policy_view::build_transact_policy_context(
        fluree,
        &base.snapshot,
        evolving,
        Some(evolving),
        current_t,
        opts,
    )
    .await
}

/// Map a staging failure onto the push surface.
///
/// Split out from `stage_commit_flakes` so the caller sees the typed
/// `TransactError` first: novelty backpressure has to be recognised by variant
/// to ask the indexer for a build, and it is indistinguishable once flattened
/// into `PushError::Invalid(String)`.
fn push_error_for_stage(e: fluree_db_transact::TransactError) -> PushError {
    match e {
        fluree_db_transact::TransactError::PolicyViolation(p) => {
            PushError::Forbidden(p.to_string())
        }
        other => PushError::Invalid(other.to_string()),
    }
}

async fn stage_commit_flakes(
    ledger: LedgerState,
    flakes: &[Flake],
    index_config: &IndexConfig,
    policy_ctx: Option<&PolicyContext>,
    graph_ids: &HashMap<Sid, GraphId>,
) -> std::result::Result<fluree_db_ledger::StagedLedger, fluree_db_transact::TransactError> {
    let mut options = fluree_db_transact::StageOptions::new()
        .with_index_config(index_config)
        .with_graph_ids(graph_ids);
    if let Some(policy_ctx) = policy_ctx.filter(|p| !p.wrapper().is_root()) {
        options = options.with_policy(policy_ctx);
    }
    fluree_db_transact::stage_flakes(ledger, flakes.to_vec(), options).await
}

async fn assert_retractions_exist(
    state: &LedgerState,
    flakes: &[Flake],
    reverse_graph: &HashMap<Sid, GraphId>,
) -> std::result::Result<(), PushError> {
    let mut at = Vec::new();
    let mut items = Vec::new();
    for (idx, f) in flakes.iter().enumerate() {
        if f.op {
            continue;
        }
        at.push(idx);
        items.push((retraction_slot(f, reverse_graph)?, f));
    }
    if items.is_empty() {
        return Ok(());
    }
    // Read through `CurrentFacts`, which stamps the graph on index rows (a
    // range read returns them with `g: None` whatever graph they live in),
    // reads each slot once, and names a fact by the identity storage gives
    // it, so a retraction carrying a term the index keeps less of (a big
    // integer's XSD subtype) names the stored fact.
    let named = fluree_db_transact::CurrentFacts::new(state)
        .named_by(&items)
        .await
        .map_err(|e| PushError::Internal(e.to_string()))?;
    match at.iter().zip(&named).find(|(_, fact)| fact.is_none()) {
        Some((idx, _)) => Err(PushError::Invalid(format!(
            "retraction invariant violated at flake[{idx}]: retract targets non-existent assertion"
        ))),
        None => Ok(()),
    }
}

/// The slot whose stored fact `target` retracts, in the graph the push's
/// routing gives it.
fn retraction_slot(
    target: &Flake,
    reverse_graph: &HashMap<Sid, GraphId>,
) -> std::result::Result<fluree_db_transact::Slot, PushError> {
    // With per-graph novelty, ALL graphs (including overlay-only ones) have proper
    // GraphIds assigned by the cumulative routing.
    let g_id = match &target.g {
        None => 0,
        Some(g_sid) => *reverse_graph.get(g_sid).ok_or_else(|| {
            PushError::Internal(format!(
                "retraction_slot: graph Sid {g_sid:?} not found in reverse_graph map \
                 — derive_graph_routing should have included it",
            ))
        })?,
    };
    Ok(fluree_db_transact::Slot {
        g_id,
        g_sid: target.g.clone(),
        s: target.s.clone(),
        p: target.p.clone(),
    })
}

async fn write_required_blobs<S>(
    storage: &S,
    namespace: &StorageNamespace,
    provided: &HashMap<String, Base64Bytes>,
    decoded: &[PushCommitDecoded],
    merged: &[PushCommitDecoded],
    pins: &mut Unpublished,
) -> std::result::Result<(), PushError>
where
    S: Storage + Send + Sync,
{
    let required = required_txn_cids(decoded, merged);
    let pinner = storage
        .blob_pins()
        .map(|p| BlobPinner::new(p, namespace.clone()));

    for addr in &required {
        let txn_id: ContentId = addr
            .parse()
            .map_err(|e| PushError::Invalid(format!("invalid txn CID reference '{addr}': {e}")))?;
        if txn_id.codec() != CODEC_FLUREE_TXN {
            return Err(PushError::Invalid(format!(
                "referenced txn CID has unexpected codec {}: {}",
                txn_id.codec(),
                addr
            )));
        }

        let Some(bytes) = provided.get(addr).map(|b| b.0.clone()) else {
            // Tolerated — see `validate_required_blobs`.
            continue;
        };

        // Integrity: server MUST re-hash bytes and verify the derived CID.
        if !txn_id.verify(&bytes) {
            return Err(PushError::Invalid(format!(
                "referenced txn CID does not match provided bytes: {addr}"
            )));
        }

        // Write using the CID digest to ensure deterministic placement.
        // (digest hex matches the legacy CAS hash during the transition period)
        let expected_hash = txn_id.address_digest();
        pins.pin(pinner.as_ref(), &txn_id).await;
        let _res = storage
            .content_write_bytes_with_hash(ContentKind::Txn, namespace, &expected_hash, &bytes)
            .await
            .map_err(|e| PushError::Internal(e.to_string()))?;
    }

    Ok(())
}

async fn write_commit_blobs<S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static>(
    storage: &S,
    namespace: &StorageNamespace,
    decoded: &[PushCommitDecoded],
    pins: &mut Unpublished,
) -> std::result::Result<Vec<StoredCommit>, PushError> {
    let pinner = storage
        .blob_pins()
        .map(|p| BlobPinner::new(p, namespace.clone()));
    let mut stored = Vec::with_capacity(decoded.len());
    for (idx, c) in decoded.iter().enumerate() {
        // Its published erasure stands for it.
        if c.erased {
            stored.push(StoredCommit {
                t: c.commit.t,
                commit_id: c.id.clone(),
            });
            continue;
        }
        if let Some(id) = ContentId::from_address_digest(CODEC_FLUREE_COMMIT, &c.digest_hex) {
            pins.pin(pinner.as_ref(), &id).await;
        }
        let res = storage
            .content_write_bytes_with_hash(ContentKind::Commit, namespace, &c.digest_hex, &c.bytes)
            .await
            .map_err(|e| PushError::Internal(e.to_string()))?;
        if res.content_hash != c.digest_hex {
            return Err(PushError::Invalid(format!(
                "commit[{}] hash mismatch after write: expected {}, storage reported {}",
                idx, c.digest_hex, res.content_hash
            )));
        }
        let commit_id = ContentId::from_address_digest(CODEC_FLUREE_COMMIT, &c.digest_hex)
            .ok_or_else(|| {
                PushError::Internal(format!(
                    "commit[{}]: invalid content hash hex '{}'",
                    idx, c.digest_hex
                ))
            })?;
        stored.push(StoredCommit {
            t: c.commit.t,
            commit_id,
        });
    }
    Ok(stored)
}

fn apply_pushed_commits_to_state(
    mut base: LedgerState,
    accepted_all_flakes: &[(i64, Vec<Flake>)],
    decoded: &[PushCommitDecoded],
    stored_commits: &[StoredCommit],
    txn_meta_g: &Sid,
) -> std::result::Result<LedgerState, PushError> {
    // Extract namespace deltas and graph IRIs from the decoded commits,
    // then apply to the snapshot so that build_reverse_graph() has complete data.
    {
        let mut merged_ns_delta: HashMap<u16, String> = HashMap::new();
        let mut all_graph_iris: HashSet<String> = HashSet::new();
        for c in decoded {
            for (code, prefix) in &c.commit.namespace_delta {
                merged_ns_delta
                    .entry(*code)
                    .or_insert_with(|| prefix.clone());
            }
            for iri in c.commit.graph_delta.values() {
                all_graph_iris.insert(iri.clone());
            }
            // Apply ns_split_mode (immutable after user namespace allocation).
            if let Some(mode) = c.commit.ns_split_mode {
                std::sync::Arc::make_mut(&mut base.snapshot)
                    .set_ns_split_mode(mode, c.commit.t)
                    .map_err(|e| PushError::Internal(e.to_string()))?;
            }
        }
        std::sync::Arc::make_mut(&mut base.snapshot)
            .apply_envelope_deltas(&merged_ns_delta, &all_graph_iris)
            .map_err(|e| PushError::Internal(format!("apply_envelope_deltas failed: {e}")))?;
        // A graph the ledger dropped exists again from the commit that writes
        // it, as replay finds.
        for c in decoded {
            base.revive_graphs(
                c.commit.graph_delta.values().map(String::as_str),
                c.commit.t,
            );
        }
    }

    // Build reverse_graph now that namespace_codes and graph_registry are
    // complete. Callers run this before the head moves, and the loader
    // refuses a registry it cannot route, so this does too.
    let mut reverse_graph = base
        .snapshot
        .build_reverse_graph()
        .map_err(|e| PushError::Internal(format!("build_reverse_graph failed: {e}")))?;
    // Ensure txn-meta graph is always routable for commit metadata flakes.
    reverse_graph
        .entry(txn_meta_g.clone())
        .or_insert(TXN_META_GRAPH_ID);

    // Apply all flakes to novelty (we already validated them; re-apply for state).
    let mut novelty = (*base.novelty).clone();
    // Detach the range provider first: it pins `dict_novelty`, so mutating
    // with it attached deep-clones the dictionary and leaves the provider —
    // the copy every read through `base.snapshot` resolves against — without
    // the subjects and strings these commits introduce.
    let provider_store = fluree_db_transact::detach_binary_provider(&mut base);
    let mut dict_novelty = base.dict_novelty.clone();
    // Like the commit path, extend the runtime dictionaries with every
    // predicate and datatype these commits introduce. Queries resolve through
    // them, and the datatype limit counts them.
    let mut runtime_small_dicts = base.runtime_small_dicts.clone();

    let store_opt: Option<&BinaryIndexStore> = base
        .binary_store
        .as_ref()
        .and_then(|te| te.0.as_ref().downcast_ref::<BinaryIndexStore>());

    // The dictionary keeps covering novelty only if it did before the push.
    let covered = dict_novelty.covers(novelty.t);
    for (t, flakes) in accepted_all_flakes {
        // Populate dict novelty similarly to transact commit path.
        fluree_db_binary_index::dict_novelty_safe::populate_dict_novelty_safe(
            Arc::make_mut(&mut dict_novelty),
            store_opt,
            flakes.iter(),
        )
        .map_err(|e| {
            PushError::Internal(format!("populate_dict_novelty_safe failed at t={t}: {e}"))
        })?;
        Arc::make_mut(&mut runtime_small_dicts).populate_from_flakes(flakes);
        // Apply to novelty.
        let links = novelty
            .apply_commit(flakes.clone(), *t, &reverse_graph)
            .map_err(|e| {
                PushError::Internal(format!("novelty apply_commit failed at t={t}: {e}"))
            })?;
        fluree_db_binary_index::dict_novelty_safe::populate_dict_novelty_safe(
            Arc::make_mut(&mut dict_novelty),
            store_opt,
            links.iter(),
        )
        .map_err(|e| {
            PushError::Internal(format!("populate_dict_novelty_safe failed at t={t}: {e}"))
        })?;
    }

    if covered {
        Arc::make_mut(&mut dict_novelty).mark_covered_through(novelty.t);
    }
    base.novelty = Arc::new(novelty);
    base.dict_novelty = dict_novelty;
    base.runtime_small_dicts = runtime_small_dicts;
    if let Some(store) = provider_store {
        fluree_db_transact::attach_binary_provider(&mut base, store);
    }
    base.head_commit_id = stored_commits.last().map(|c| c.commit_id.clone());
    if let Some(ref mut r) = base.ns_record {
        if let Some(last) = stored_commits.last() {
            r.commit_head_id = Some(last.commit_id.clone());
            r.commit_t = last.t;
        }
    }
    Ok(base)
}

trait LedgerStateCloneExt {
    fn clone_with_novelty(&self, novelty: Arc<Novelty>) -> Self;
}

impl LedgerStateCloneExt for LedgerState {
    fn clone_with_novelty(&self, novelty: Arc<Novelty>) -> Self {
        let mut s = self.clone();
        s.novelty = novelty;
        s
    }
}

// ============================================================================
// Export (server → client)
// ============================================================================

/// Maximum commits per export page (server-enforced cap).
const EXPORT_MAX_LIMIT: usize = 500;

/// Default commits per export page when no limit is specified.
const EXPORT_DEFAULT_LIMIT: usize = 100;

/// Query parameters for paginated commit export.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExportCommitsRequest {
    /// Commit cursor to start from.
    ///
    /// Backward-compatible field:
    /// - legacy: commit address (e.g. `fluree:file://.../commit/<hash>.fcv2`)
    /// - new: commit CID string (e.g. `bafy...`)
    ///
    /// When both `cursor` and `cursor_id` are provided, `cursor_id` wins.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Commit CID cursor to start from (storage-agnostic identity).
    ///
    /// Preferred for cross-backend sync. When `None`, starts from the current head.
    #[serde(default)]
    pub cursor_id: Option<ContentId>,
    /// Maximum commits per page. Clamped to server max (500).
    #[serde(default)]
    pub limit: Option<usize>,
    /// Export the branch's first-parent line as `commits`, and the commits
    /// its merges brought in as `merged_commits`.
    ///
    /// Without it, `commits` walks every parent, and a history containing a
    /// merge cannot be imported. The flag is opt-in so that a client
    /// predating it keeps the format it understands. A response that does
    /// not echo `lineage` came from a server predating it.
    #[serde(default)]
    pub lineage: bool,
    /// The client's head, in `lineage` mode. The export stops above it.
    /// When `None`, it runs to genesis.
    #[serde(default)]
    pub base_id: Option<ContentId>,
}

/// Paginated response containing commit blobs (newest → oldest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportCommitsResponse {
    /// Ledger identifier.
    pub ledger: String,
    /// Head commit CID at time of export (storage-agnostic identity).
    pub head_commit_id: ContentId,
    /// Head `t` at time of export (informational).
    pub head_t: i64,
    /// Raw commit v2 blobs, newest → oldest within this page.
    pub commits: Vec<Base64Bytes>,
    /// Referenced blobs (txn blobs) keyed by CID string.
    #[serde(default)]
    pub blobs: HashMap<String, Base64Bytes>,
    /// Txn CIDs referenced by commits in this page that the source could
    /// not read (dangling provenance). The commits themselves are intact
    /// and exported; consumers should carry the reference without the
    /// bytes rather than refuse the chain.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_blobs: Vec<String>,
    /// Highest `t` in this page.
    pub newest_t: i64,
    /// Lowest `t` in this page.
    pub oldest_t: i64,
    /// Cursor for the next page (previous commit CID).
    /// `None` when genesis has been reached.
    pub next_cursor_id: Option<ContentId>,
    /// Number of commits in this page.
    pub count: usize,
    /// Actual limit used (after server clamping).
    pub effective_limit: usize,
    /// Echoes [`ExportCommitsRequest::lineage`]. A server predating that flag
    /// leaves it `false`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lineage: bool,
    /// In `lineage` mode, the commits that merges in `commits` brought in,
    /// parents before children. See
    /// [`PushCommitsRequest::merged_commits`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merged_commits: Vec<Base64Bytes>,
    /// The CIDs of `commits`, in order (see
    /// [`PushCommitsRequest::commit_ids`]). A server predating them leaves
    /// them empty, and each commit's CID is then the SHA2-256 of its bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_ids: Vec<ContentId>,
    /// The CIDs of `merged_commits`, likewise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merged_commit_ids: Vec<ContentId>,
}

impl ExportCommitsResponse {
    /// The CIDs of `commits` and of `merged_commits`, each checked against
    /// its bytes: the ones the page names, or the SHA2-256 of the bytes on a
    /// page from a server that names none.
    pub fn ids(&self) -> Result<(Vec<ContentId>, Vec<ContentId>)> {
        let resolve = |blobs: &[Base64Bytes], ids: &[ContentId], what: &str| {
            let ids = named_ids(ids, blobs.len(), what).map_err(PushError::into_api_error)?;
            blobs
                .iter()
                .enumerate()
                .map(|(idx, b64)| {
                    pushed_commit_id(ids.map(|ids| &ids[idx]), &b64.0, &format!("{what}[{idx}]"))
                        .map_err(PushError::into_api_error)
                })
                .collect::<Result<Vec<_>>>()
        };
        Ok((
            resolve(&self.commits, &self.commit_ids, "commit_ids")?,
            resolve(
                &self.merged_commits,
                &self.merged_commit_ids,
                "merged_commit_ids",
            )?,
        ))
    }
}

/// Export a paginated range of commits from a ledger.
///
/// Uses address-cursor pagination: each page walks backward from the cursor
/// via `parents` for up to `limit` commits. Each page is O(limit)
/// regardless of total ledger size.
///
/// Commits are returned newest → oldest. The client reverses for import.
impl Fluree {
    pub async fn export_commit_range(
        &self,
        handle: &LedgerHandle,
        request: &ExportCommitsRequest,
    ) -> Result<ExportCommitsResponse> {
        use fluree_db_core::commit::codec::envelope::decode_envelope;
        use fluree_db_core::commit::codec::format::{CommitHeader, HEADER_LEN};
        use fluree_db_core::ContentStore;

        let effective_limit = request
            .limit
            .unwrap_or(EXPORT_DEFAULT_LIMIT)
            .min(EXPORT_MAX_LIMIT);

        let ledger_id = handle.id();
        self.ensure_read_ready(ledger_id).await?;

        // Read current head.
        let head_ref = self
            .publisher()?
            .get_ref(ledger_id, RefKind::CommitHead)
            .await?;
        let Some(head_ref) = head_ref else {
            return Err(ApiError::NotFound(format!("Ledger not found: {ledger_id}")));
        };
        let head_commit_id = head_ref
            .id
            .clone()
            .ok_or_else(|| ApiError::NotFound("Ledger has no commits".to_string()))?;
        let head_t = head_ref.t;

        // Branch-aware ContentStore for CID-based reads — pulls walk
        // history backwards, which crosses fork points on a branched
        // ledger and would otherwise 404 on pre-fork ancestors.
        let content_store = self.branched_content_store(ledger_id).await.map_err(|e| {
            ApiError::internal(format!(
                "failed to build branched store for {ledger_id}: {e}"
            ))
        })?;

        // A first page answers for the whole history.
        #[cfg(feature = "native")]
        if request.cursor_id.is_none() && request.cursor.is_none() {
            self.refuse_erased_history(ledger_id, content_store.as_ref(), &head_commit_id)
                .await?;
        }

        // Determine start cursor CID.
        let start_cid: ContentId = if let Some(cid) = &request.cursor_id {
            cid.clone()
        } else if let Some(raw) = request.cursor.as_deref() {
            raw.parse::<ContentId>()
                .map_err(|e| ApiError::http(400, format!("invalid cursor: {e}")))?
        } else {
            head_commit_id.clone()
        };

        if request.lineage {
            return export_lineage_page(
                content_store.as_ref(),
                &start_cid,
                request.base_id.as_ref(),
                ledger_id,
                head_commit_id,
                head_t,
                effective_limit,
            )
            .await;
        }

        let mut commits = Vec::with_capacity(effective_limit);
        let mut commit_ids = Vec::with_capacity(effective_limit);
        let mut blobs: HashMap<String, Base64Bytes> = HashMap::new();
        let mut missing_blobs: Vec<String> = Vec::new();
        let mut newest_t: Option<i64> = None;
        let mut oldest_t: Option<i64> = None;
        let mut frontier = vec![start_cid];
        let mut visited = std::collections::HashSet::new();

        for _ in 0..effective_limit {
            let current_cid = match frontier.pop() {
                Some(cid) => cid,
                None => break,
            };
            if !visited.insert(current_cid.clone()) {
                continue;
            }

            // Read raw commit bytes from ContentStore by CID.
            let raw_bytes = content_store.get(&current_cid).await.map_err(|e| {
                ApiError::internal(format!("failed to read commit {current_cid}: {e}"))
            })?;
            refuse_erased(&current_cid, &raw_bytes)?;

            // Lightweight decode: header + envelope only (skip ops decompression).
            let header = CommitHeader::read_from(&raw_bytes).map_err(|e| {
                ApiError::internal(format!("invalid commit header for {current_cid}: {e}"))
            })?;

            let envelope_start = HEADER_LEN;
            let envelope_end = envelope_start + header.envelope_len as usize;
            if envelope_end > raw_bytes.len() {
                return Err(ApiError::internal(format!(
                    "commit envelope extends past blob for {current_cid}"
                )));
            }
            let env = decode_envelope(&raw_bytes[envelope_start..envelope_end]).map_err(|e| {
                ApiError::internal(format!("failed to decode envelope for {current_cid}: {e}"))
            })?;

            // Track t range.
            let t = header.t;
            if newest_t.is_none() {
                newest_t = Some(t);
            }
            oldest_t = Some(t);

            commits.push(Base64Bytes(raw_bytes.into_vec()));
            commit_ids.push(current_cid.clone());

            // Collect referenced txn blob via ContentStore. A missing blob is
            // a provenance gap, not a chain break: report it and keep going.
            if let Some(ref txn_cid) = env.txn {
                let txn_key = txn_cid.to_string();
                if let std::collections::hash_map::Entry::Vacant(e) = blobs.entry(txn_key.clone()) {
                    match content_store.get(txn_cid).await {
                        Ok(txn_bytes) => {
                            e.insert(Base64Bytes(txn_bytes.into_vec()));
                        }
                        Err(fluree_db_core::Error::NotFound(_)) => {
                            tracing::warn!(
                                commit = %current_cid,
                                t,
                                txn_cid = %txn_key,
                                "commit references a txn blob that is missing from storage; exporting without it"
                            );
                            missing_blobs.push(txn_key);
                        }
                        Err(e) => {
                            return Err(ApiError::internal(format!(
                                "failed to read txn blob {txn_key}: {e}"
                            )));
                        }
                    }
                }
            }

            // Enqueue all parents for traversal.
            for parent in env.parents {
                frontier.push(parent);
            }
        }

        // The next cursor is the next unvisited frontier entry (if any).
        let next_cursor_id = frontier.into_iter().find(|id| !visited.contains(id));

        let count = commits.len();

        Ok(ExportCommitsResponse {
            ledger: ledger_id.to_string(),
            head_commit_id,
            head_t,
            commits,
            blobs,
            missing_blobs,
            newest_t: newest_t.unwrap_or(0),
            oldest_t: oldest_t.unwrap_or(0),
            next_cursor_id,
            count,
            effective_limit,
            lineage: false,
            merged_commits: Vec::new(),
            commit_ids,
            merged_commit_ids: Vec::new(),
        })
    }
}

/// Read commits in order, with their `t`, collecting each one's txn blob
/// into `blobs`. A txn blob missing from storage is listed in
/// `missing_blobs`, as in the default export.
async fn read_export_commits(
    store: &dyn fluree_db_core::ContentStore,
    cids: &[ContentId],
    blobs: &mut HashMap<String, Base64Bytes>,
    missing_blobs: &mut Vec<String>,
) -> Result<Vec<(i64, ContentId, Base64Bytes)>> {
    let mut out = Vec::with_capacity(cids.len());
    for cid in cids {
        let bytes = store
            .get(cid)
            .await
            .map_err(|e| ApiError::internal(format!("failed to read commit {cid}: {e}")))?;
        refuse_erased(cid, &bytes)?;
        let envelope = fluree_db_core::commit::codec::read_commit_envelope(&bytes)
            .map_err(|e| ApiError::internal(format!("invalid commit {cid}: {e}")))?;
        if let Some(txn_cid) = &envelope.txn {
            let key = txn_cid.to_string();
            if let std::collections::hash_map::Entry::Vacant(entry) = blobs.entry(key.clone()) {
                match store.get(txn_cid).await {
                    Ok(txn_bytes) => {
                        entry.insert(Base64Bytes(txn_bytes.into_vec()));
                    }
                    Err(fluree_db_core::Error::NotFound(_)) => {
                        tracing::warn!(
                            commit = %cid,
                            txn_cid = %key,
                            "commit references a txn blob that is missing from storage; exporting without it"
                        );
                        missing_blobs.push(key);
                    }
                    Err(e) => {
                        return Err(ApiError::internal(format!(
                            "failed to read txn blob {key}: {e}"
                        )));
                    }
                }
            }
        }
        out.push((envelope.t, cid.clone(), Base64Bytes(bytes.into_vec())));
    }
    Ok(out)
}

/// One page of a `lineage` export: commits of the first-parent line, plus the
/// commits their merges brought in. `limit` counts both.
async fn export_lineage_page(
    store: &dyn fluree_db_core::ContentStore,
    from: &ContentId,
    base: Option<&ContentId>,
    ledger_id: &str,
    head_commit_id: ContentId,
    head_t: i64,
    limit: usize,
) -> Result<ExportCommitsResponse> {
    let page = fluree_db_core::plan_commit_transfer_page(store, from, base, limit)
        .await
        .map_err(|e| ApiError::internal(format!("failed to plan export from {from}: {e}")))?
        .ok_or_else(|| {
            ApiError::http(
                409,
                "base_id is not on this branch's first-parent line; the histories have diverged",
            )
        })?;

    let mut blobs = HashMap::new();
    let mut missing_blobs = Vec::new();
    // Newest first, as in the default export.
    let lineage: Vec<ContentId> = page.plan.lineage.iter().rev().cloned().collect();
    let commits = read_export_commits(store, &lineage, &mut blobs, &mut missing_blobs).await?;
    let merged_commits =
        read_export_commits(store, &page.plan.merged, &mut blobs, &mut missing_blobs).await?;

    Ok(ExportCommitsResponse {
        ledger: ledger_id.to_string(),
        head_commit_id,
        head_t,
        newest_t: commits.first().map_or(0, |(t, _, _)| *t),
        oldest_t: commits.last().map_or(0, |(t, _, _)| *t),
        count: commits.len(),
        commit_ids: commits.iter().map(|(_, id, _)| id.clone()).collect(),
        commits: commits.into_iter().map(|(_, _, bytes)| bytes).collect(),
        blobs,
        missing_blobs,
        next_cursor_id: page.next,
        effective_limit: limit,
        lineage: true,
        merged_commit_ids: merged_commits.iter().map(|(_, id, _)| id.clone()).collect(),
        merged_commits: merged_commits
            .into_iter()
            .map(|(_, _, bytes)| bytes)
            .collect(),
    })
}

// ============================================================================
// Import (client ← server)
// ============================================================================

/// Result of a bulk import (clone path — CAS writes only, no novelty).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkImportResult {
    /// Number of commit blobs written to local CAS.
    pub stored: usize,
    /// Number of txn/reference blobs written to local CAS.
    pub blobs_stored: usize,
}

/// Result of an incremental import (pull path — validated + novelty applied).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitImportResult {
    /// Number of commits imported.
    pub imported: usize,
    /// New local head `t`.
    pub head_t: i64,
    /// New local head commit CID.
    pub head_commit_id: ContentId,
    /// Indexing status hints for external indexers.
    pub indexing: IndexingStatus,
}

impl Fluree {
    /// Bulk-import commit blobs to local CAS (clone path).
    ///
    /// Writes commit and txn blobs without validation or novelty updates.
    /// Order doesn't matter — CAS writes are idempotent.
    /// Call [`set_commit_head`] after all pages are imported, holding
    /// [`hold_garbage`](Self::hold_garbage) from before the first page until
    /// then: nothing names these blobs before it.
    pub async fn import_commits_bulk(
        &self,
        handle: &LedgerHandle,
        response: &ExportCommitsResponse,
    ) -> Result<BulkImportResult> {
        let namespace = handle.snapshot().await.storage_namespace();
        let storage = self.admin_storage().ok_or_else(|| {
            ApiError::config("import_commits_bulk requires a managed storage backend")
        })?;
        let mut stored = 0usize;
        let mut blobs_stored = 0usize;

        // Write commit blobs to local CAS, each under its CID's address. A
        // `lineage` page carries the commits its merges brought in
        // separately. Clone stores them like any other.
        let (line_ids, merged_ids) = response.ids()?;
        let blob_ids = response
            .blobs
            .keys()
            .filter_map(|cid| cid.parse::<ContentId>().ok())
            .collect::<Vec<_>>();
        for id in line_ids.iter().chain(&merged_ids).chain(&blob_ids) {
            self.admit_content_id(id)?;
        }
        let all = response.commits.iter().chain(&response.merged_commits);
        for (id, b64) in line_ids.iter().chain(&merged_ids).zip(all) {
            let bytes = &b64.0;
            storage
                .content_write_bytes_with_hash(
                    ContentKind::Commit,
                    &namespace,
                    &id.address_digest(),
                    bytes,
                )
                .await
                .map_err(|e| ApiError::internal(format!("failed to write commit blob: {e}")))?;
            stored += 1;
        }

        // Write referenced txn blobs to local CAS.
        for (cid_str, b64) in &response.blobs {
            let bytes = &b64.0;
            let txn_id: ContentId = cid_str
                .parse()
                .map_err(|e| ApiError::http(422, format!("invalid txn CID '{cid_str}': {e}")))?;
            if txn_id.codec() != CODEC_FLUREE_TXN {
                return Err(ApiError::http(
                    422,
                    format!(
                        "referenced txn CID has unexpected codec {}: {}",
                        txn_id.codec(),
                        cid_str
                    ),
                ));
            }
            // Integrity: verify bytes match claimed CID.
            if !txn_id.verify(bytes) {
                return Err(ApiError::http(
                    422,
                    format!("txn CID does not match provided bytes: {cid_str}"),
                ));
            }
            let expected_hash = txn_id.address_digest();
            storage
                .content_write_bytes_with_hash(ContentKind::Txn, &namespace, &expected_hash, bytes)
                .await
                .map_err(|e| ApiError::internal(format!("failed to write txn blob: {e}")))?;
            blobs_stored += 1;
        }

        Ok(BulkImportResult {
            stored,
            blobs_stored,
        })
    }

    /// Set the commit head after bulk import (clone finalization).
    ///
    /// Points `CommitHead` at the given CID/t. Used after all pages of a
    /// bulk clone have been imported to make the ledger loadable.
    pub async fn set_commit_head(
        &self,
        handle: &LedgerHandle,
        head_commit_id: &ContentId,
        head_t: i64,
    ) -> Result<()> {
        self.admit_content_id(head_commit_id)?;
        let basis = handle.snapshot().await.write_basis();
        self.set_ref(
            handle.id(),
            basis,
            RefKind::CommitHead,
            head_commit_id,
            head_t,
        )
        .await
    }

    /// Set the index head after pull/clone with index transfer.
    ///
    /// Points `IndexHead` at the given CID/t so that the next `ledger()` load
    /// picks up the pulled binary index. `IndexHead` uses `t >= current.t`
    /// monotonic guard (allows equal-t overwrites, unlike `CommitHead`).
    ///
    /// Not when this storage holds an erasure of a commit in the ledger's
    /// history: the sender may have built its index from the original, so
    /// the ledger builds its own from the replacements. Returns whether the
    /// index was set.
    pub async fn set_index_head(
        &self,
        handle: &LedgerHandle,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<bool> {
        self.admit_content_id(index_id)?;
        // The head the transfer set, which the handle may not hold yet.
        #[cfg(feature = "native")]
        if let Some(head) = self
            .nameservice()
            .lookup(handle.id())
            .await?
            .and_then(|record| record.commit_head_id)
        {
            let store = self.branched_content_store(handle.id()).await?;
            if let Some(erased) = self
                .erased_in_history(handle.id(), store.as_ref(), &head)
                .await?
            {
                tracing::info!(
                    ledger = %handle.id(),
                    %erased,
                    "a received index is not set: its history holds a commit erased here"
                );
                return Ok(false);
            }
        }
        let basis = handle.snapshot().await.write_basis();
        self.set_ref(handle.id(), basis, RefKind::IndexHead, index_id, index_t)
            .await?;
        Ok(true)
    }

    /// Point `kind` of `ledger_id`'s record at `id`/`t`, by compare-and-swap
    /// against the head read just before.
    async fn set_ref(
        &self,
        ledger_id: &str,
        basis: WriteBasis,
        kind: RefKind,
        id: &ContentId,
        t: i64,
    ) -> Result<()> {
        let new_ref = RefValue {
            id: Some(id.clone()),
            t,
        };
        let current_ref = self.publisher()?.get_ref(ledger_id, kind).await?;
        match self
            .publisher()?
            .compare_and_set_ref_fenced(ledger_id, basis, kind, current_ref.as_ref(), &new_ref)
            .await?
        {
            CasResult::Updated => Ok(()),
            CasResult::Conflict { actual } => Err(ApiError::http(
                409,
                format!(
                    "{kind:?} changed during transfer (expected {current_ref:?}, actual {actual:?})"
                ),
            )),
        }
    }

    /// Restore a ledger from a `.flpack` archive stream, creating a new
    /// ledger under `new_ledger_id`.
    ///
    /// This is the wholesale-restore counterpart of [`Fluree::archive_ledger`]:
    /// it streams pack frames from `reader` — a file, an HTTP body, an S3
    /// `GetObject` body, anything `AsyncRead` — verifies each CAS object's
    /// integrity, writes it to storage, and finalizes the commit (and, when
    /// present, index) head from the embedded `phase: "nameservice"` manifest.
    /// Unlike the push path it does **not** re-validate or replay commits; the
    /// archive is trusted byte-for-byte (every frame is SHA-256 verified) and
    /// the prebuilt index rides along, so the restored ledger is immediately
    /// queryable.
    ///
    /// The new name is independent of the source ledger's name — CAS objects
    /// are content-addressed, so only the nameservice pointer uses the name.
    ///
    /// Returns [`ApiError::LedgerExists`] if the name already holds a live
    /// ledger on any branch.
    ///
    /// The ledger stays invisible until it is fully restored. On any failure
    /// its claim on the name is released and the data it wrote deleted, so
    /// the restore can be retried.
    pub async fn restore_ledger<R>(
        &self,
        new_ledger_id: &str,
        reader: &mut R,
    ) -> Result<RestoreResult>
    where
        R: tokio::io::AsyncRead + Unpin + Send,
    {
        // Boxed: moved by value through the scope, it would sit on the
        // stack once per layer.
        self.write_scope(Box::pin(self.restore_ledger_scoped(new_ledger_id, reader)))
            .await
    }

    /// [`Self::restore_ledger`], in its write scope.
    async fn restore_ledger_scoped<R>(
        &self,
        new_ledger_id: &str,
        reader: &mut R,
    ) -> Result<RestoreResult>
    where
        R: tokio::io::AsyncRead + Unpin + Send,
    {
        // Normalize to canonical `name:branch` form up front so the ingest,
        // content-store namespace, head finalization, and rollback all agree —
        // callers may pass a bare `name` (the CLI does), which `create_ledger`
        // would register as `name:main` while raw-id storage writes would land
        // in the wrong namespace.
        let new_ledger_id = LedgerId::parse(new_ledger_id)?;

        // An erasure accepted or published meanwhile would miss the copy.
        #[cfg(feature = "native")]
        let _admission = match self.retention.erasure.get() {
            Some(erasure) => Some(erasure.admission.write().await),
            None => None,
        };

        // A restore brings in a whole ledger, so it claims the whole name,
        // and nobody sees the ledger until its heads are in place.
        let mut pending = self.claim_name(&new_ledger_id).await?;
        let store = self.publisher()?;
        let record = pending.record.clone();
        let restoring = self.restore_into_created(&record, reader);
        let restored = match crate::ledger::while_claimed(store, &mut pending, restoring).await {
            Some(Ok(result)) => fluree_db_nameservice::lifecycle::activate(store, &pending)
                .await
                .map(|()| result)
                .map_err(ApiError::from),
            Some(Err(e)) => Err(e),
            None => Err(fluree_db_nameservice::lifecycle::create_rolled_back(&record.name).into()),
        };
        if restored.is_err() {
            self.discard_restored(&pending).await;
        }
        #[cfg(feature = "native")]
        if restored.is_ok() {
            self.sweep_after_restore(new_ledger_id.name()).await;
        }
        restored
    }

    /// Undo a restore that failed: release its claim on the name, so the
    /// restore can be retried, then delete what it wrote, which nothing
    /// references once the claim is gone.
    async fn discard_restored(&self, pending: &PendingLedger) {
        let abandoned = match self.publisher() {
            Ok(store) => fluree_db_nameservice::lifecycle::abandon(store, pending)
                .await
                .map_err(ApiError::from),
            Err(e) => Err(e),
        };
        // A claim that could not be released may hold a live ledger: its
        // data stays.
        if let Err(e) = &abandoned {
            error!(
                ledger = %pending.record.ledger_id,
                error = %e,
                "failed to release a failed restore's claim on the name; its data is kept"
            );
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let (Ok(()), Some(storage)) = (&abandoned, self.admin_storage()) {
            let mut report = crate::admin::DropReport::default();
            crate::admin::purge_instance_root(storage, &pending.root, &[], &mut report).await;
            for warning in &report.warnings {
                tracing::warn!(ledger = %pending.record.ledger_id, %warning, "partially-restored data left behind");
            }
        }
    }

    /// Stream-decode a `.flpack` into the pending ledger whose root branch is
    /// `record` and finalize its heads. Split out from
    /// [`Fluree::restore_ledger`] so the caller can roll back on any error
    /// returned here.
    async fn restore_into_created<R>(
        &self,
        record: &NsRecord,
        reader: &mut R,
    ) -> Result<RestoreResult>
    where
        R: tokio::io::AsyncRead + Unpin + Send,
    {
        use fluree_db_core::pack::{
            decode_frame, read_stream_preamble, PackError, PackFrame, PREAMBLE_SIZE,
        };
        use futures::stream::{FuturesUnordered, StreamExt as _};
        use tokio::io::AsyncReadExt as _;

        let storage = self
            .backend()
            .admin_storage_cloned()
            .ok_or_else(|| ApiError::config("restore_ledger requires a managed storage backend"))?;
        let new_ledger_id = record.ledger_id.as_str();
        let namespace = record.storage_namespace();

        // Frames are decoded out of a growing byte buffer; `read` appends and
        // `drain` consumes whole frames as they complete. Decoding stays
        // sequential, but the slow per-object storage writes are dispatched
        // into `in_flight` so they overlap (see `RESTORE_MAX_WRITES_IN_FLIGHT`).
        // Memory is bounded by `in_flight`'s payloads plus the unparsed tail.
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut read_chunk = [0u8; 64 * 1024];
        let mut preamble_consumed = false;
        let mut saw_header = false;
        let mut ended = false;
        let mut commits = 0usize;
        let mut txn_blobs = 0usize;
        let mut index_artifacts = 0usize;
        let mut ns_manifest: Option<serde_json::Value> = None;
        // The genesis commit, which keys the lineage's journal checkpoint.
        let mut lineage: Option<ContentId> = None;
        #[cfg(feature = "native")]
        let mut erasures = crate::retention::erasure::transfer::Received::default();

        // Concurrent CAS writer pool. Each future owns its `(cid, payload)` and
        // returns the byte count it frees on completion; errors propagate (and
        // roll the restore back via the caller). `bytes_in_flight` throttles
        // large payloads independently of the count cap.
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut bytes_in_flight: usize = 0;

        'read: loop {
            // Strip the preamble once enough bytes have arrived.
            if !preamble_consumed && buf.len() >= PREAMBLE_SIZE {
                read_stream_preamble(&buf)
                    .map_err(|e| ApiError::http(400, format!("invalid .flpack preamble: {e}")))?;
                buf.drain(..PREAMBLE_SIZE);
                preamble_consumed = true;
            }

            // Drain every complete frame currently in the buffer.
            if preamble_consumed {
                loop {
                    match decode_frame(&buf, RESTORE_MAX_FRAME_PAYLOAD) {
                        Ok((frame, consumed)) => {
                            match frame {
                                PackFrame::Header(_) => {
                                    if saw_header {
                                        return Err(ApiError::http(
                                            400,
                                            "invalid .flpack: duplicate Header frame",
                                        ));
                                    }
                                    saw_header = true;
                                }
                                PackFrame::Data { cid, payload } => {
                                    if !saw_header {
                                        return Err(ApiError::http(
                                            400,
                                            "invalid .flpack: Data frame before Header",
                                        ));
                                    }
                                    self.admit_content_id(&cid)?;
                                    // Count at dispatch: any write failure aborts the
                                    // whole restore (and rolls it back), so only the
                                    // success path ever returns these counts.
                                    match cid.content_kind() {
                                        Some(ContentKind::Commit) => {
                                            commits += 1;
                                            if lineage.is_none()
                                                && fluree_db_core::commit::codec::format::CommitHeader::read_from(&payload)
                                                    .is_ok_and(|h| h.t == 1)
                                            {
                                                lineage = Some(cid.clone());
                                            }
                                        }
                                        Some(ContentKind::Txn) => txn_blobs += 1,
                                        _ => index_artifacts += 1,
                                    }
                                    // Backpressure: drain completed writes until this one
                                    // fits within both the count and byte budgets. The
                                    // `!in_flight.is_empty()` guard still admits a single
                                    // object larger than the byte budget once the pool
                                    // drains, so an oversized frame never deadlocks.
                                    let len = payload.len();
                                    while in_flight.len() >= RESTORE_MAX_WRITES_IN_FLIGHT
                                        || (bytes_in_flight + len > RESTORE_MAX_BYTES_IN_FLIGHT
                                            && !in_flight.is_empty())
                                    {
                                        match in_flight.next().await {
                                            Some(res) => bytes_in_flight -= res?,
                                            None => break,
                                        }
                                    }
                                    bytes_in_flight += len;
                                    let storage_ref = &storage;
                                    let namespace_ref = &namespace;
                                    in_flight.push(async move {
                                        ingest_cas_object(
                                            storage_ref,
                                            &cid,
                                            &payload,
                                            namespace_ref,
                                        )
                                        .await?;
                                        Ok::<usize, ApiError>(len)
                                    });
                                }
                                PackFrame::Manifest(json) => {
                                    if json.get("phase").and_then(|v| v.as_str())
                                        == Some("nameservice")
                                    {
                                        ns_manifest = Some(json);
                                    }
                                }
                                PackFrame::Error(msg) => {
                                    return Err(ApiError::http(
                                        400,
                                        format!(".flpack contains error frame: {msg}"),
                                    ));
                                }
                                erasure @ (PackFrame::Erasure(_)
                                | PackFrame::Authorization(_)
                                | PackFrame::ErasureArtifact { .. }) => {
                                    if !saw_header {
                                        return Err(ApiError::http(
                                            400,
                                            "invalid .flpack: erasure frame before Header",
                                        ));
                                    }
                                    #[cfg(feature = "native")]
                                    self.take_erasure_frame(erasure, &mut erasures).await?;
                                    #[cfg(not(feature = "native"))]
                                    {
                                        drop(erasure);
                                        return Err(cannot_hold_erasures());
                                    }
                                }
                                PackFrame::End => {
                                    ended = true;
                                    break 'read;
                                }
                            }
                            buf.drain(..consumed);
                        }
                        // Not enough bytes for the next frame yet — go read more.
                        Err(PackError::Incomplete(_)) => break,
                        Err(e) => {
                            return Err(ApiError::http(400, format!("invalid .flpack frame: {e}")));
                        }
                    }
                }
            }

            let n = reader
                .read(&mut read_chunk)
                .await
                .map_err(|e| ApiError::internal(format!("error reading .flpack stream: {e}")))?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&read_chunk[..n]);
        }

        // Flush every write still in flight before finalizing heads — the
        // manifest CID `content.has` checks below require each CAS object to be
        // durable, and a failed write must surface here (rolling the restore
        // back) rather than leaving a head pointing at an unwritten object.
        while let Some(res) = in_flight.next().await {
            res?;
        }

        if !saw_header {
            return Err(ApiError::http(400, "invalid .flpack: no Header frame"));
        }
        if !ended {
            return Err(ApiError::http(
                400,
                "unexpected end of .flpack stream (missing End frame)",
            ));
        }

        let manifest = ns_manifest.ok_or_else(|| {
            ApiError::http(
                400,
                ".flpack is missing its nameservice manifest — cannot determine commit/index heads",
            )
        })?;

        // Resolve the head CIDs from the manifest, then verify they were
        // actually ingested before pointing the nameservice at them — a
        // truncated or mismatched archive must not yield a dangling head.
        // Every part of the manifest is checked before anything acts on it.
        let content = self.content_store(&namespace);

        let commit_head_id = manifest
            .get("commit_head_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ApiError::http(400, ".flpack manifest is missing commit_head_id"))?;
        let commit_cid: ContentId = commit_head_id
            .parse()
            .map_err(|e| ApiError::http(400, format!("invalid commit CID in manifest: {e}")))?;
        #[cfg(feature = "native")]
        let erased_commits = erasures.count();
        #[cfg(not(feature = "native"))]
        let erased_commits = 0;
        #[cfg(feature = "native")]
        let head_erased = erasures.holds(&commit_cid);
        #[cfg(not(feature = "native"))]
        let head_erased = false;
        if !head_erased && !content.has(&commit_cid).await.unwrap_or(false) {
            return Err(ApiError::http(
                400,
                format!(".flpack manifest names commit head {commit_cid} that the archive did not contain"),
            ));
        }
        let commit_t = manifest
            .get("commit_t")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);

        let index_head = match manifest.get("index_head_id").and_then(|v| v.as_str()) {
            Some(index_cid_str) => {
                let index_cid: ContentId = index_cid_str.parse().map_err(|e| {
                    ApiError::http(400, format!("invalid index CID in manifest: {e}"))
                })?;
                if erased_commits > 0 {
                    return Err(ApiError::http(
                        400,
                        ".flpack with erased commits names an index, which may hold an erased value",
                    ));
                }
                if !content.has(&index_cid).await.unwrap_or(false) {
                    return Err(ApiError::http(
                        400,
                        format!(".flpack manifest names index head {index_cid} that the archive did not contain"),
                    ));
                }
                let index_t = manifest
                    .get("index_t")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                Some((index_cid, index_t))
            }
            None => None,
        };

        let default_context = match manifest.get("default_context_id").and_then(|v| v.as_str()) {
            Some(ctx_cid_str) => {
                let ctx_cid: ContentId = ctx_cid_str.parse().map_err(|e| {
                    ApiError::http(400, format!("invalid default_context CID in manifest: {e}"))
                })?;
                if !content.has(&ctx_cid).await.unwrap_or(false) {
                    return Err(ApiError::http(
                        400,
                        format!(".flpack manifest names default-context blob {ctx_cid} that the archive did not contain"),
                    ));
                }
                let bytes = content.get(&ctx_cid).await.map_err(|e| {
                    ApiError::internal(format!(
                        "failed to read default context blob {ctx_cid}: {e}"
                    ))
                })?;
                let ctx_json: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                    ApiError::http(400, format!("default context blob is not valid JSON: {e}"))
                })?;
                Some(ctx_json)
            }
            None => None,
        };

        // The archive's journal, and what the lineage has suppressed since.
        let archived = manifest
            .get("retention")
            .map(crate::retention::registry::ArchivedRetention::parse)
            .transpose()?;

        // Erased commits read as their replacements once their erasures are
        // in, which is before anything reads the history.
        #[cfg(feature = "native")]
        let received = match erased_commits {
            0 => None,
            _ => {
                let journal = archived.as_ref().map(|a| &a.journal);
                Some(
                    self.receive_erasures(
                        erasures,
                        crate::retention::erasure::transfer::Receiver::Restored(new_ledger_id),
                        &namespace,
                        content.as_ref(),
                        &commit_cid,
                        journal,
                    )
                    .await?,
                )
            }
        };
        #[cfg(feature = "native")]
        if let Some(genesis) = received.as_ref().and_then(|r| r.genesis.clone()) {
            lineage.get_or_insert(genesis);
        }

        self.set_ref(
            new_ledger_id,
            record.write_basis(),
            RefKind::CommitHead,
            &commit_cid,
            commit_t,
        )
        .await?;

        let mut index_t_out = None;
        if let Some((index_cid, index_t)) = index_head {
            // The FIR6 index root embeds the *source* ledger_id as an inline
            // identity field. Loading it under a new name trips
            // `apply_loaded_db`'s identity check ("Index ledger_id '<src>' does
            // not match expected '<dst>'"), which only the write path hits —
            // leaving an otherwise-queryable restored ledger silently
            // read-only. Re-stamp that one field and re-write the root under a
            // fresh CID when the names differ. Every other artifact (branches,
            // leaves, dicts) is name-independent and was ingested verbatim;
            // the source ledger name is intentionally preserved in the
            // historical txn-meta/config graph IRIs (those live in the
            // content-addressed dict tree and match clone/pull semantics).
            let head_index_cid = {
                use fluree_db_binary_index::IndexRoot;
                let root_bytes = content.get(&index_cid).await.map_err(|e| {
                    ApiError::internal(format!(
                        "failed to read restored index root {index_cid}: {e}"
                    ))
                })?;
                let mut root = IndexRoot::decode(&root_bytes).map_err(|e| {
                    ApiError::http(
                        400,
                        format!("restored index root {index_cid} is not a valid FIR6 root: {e}"),
                    )
                })?;
                if root.ledger_id == new_ledger_id {
                    index_cid
                } else {
                    root.ledger_id = new_ledger_id.to_string();
                    let restamped = root.encode();
                    let res = storage
                        .content_write_bytes(ContentKind::IndexRoot, &namespace, &restamped)
                        .await
                        .map_err(|e| {
                            ApiError::internal(format!(
                                "failed to write re-stamped index root: {e}"
                            ))
                        })?;
                    ContentId::from_address_digest(
                        ContentKind::IndexRoot.to_codec(),
                        &res.content_hash,
                    )
                    .ok_or_else(|| {
                        ApiError::internal(format!(
                            "re-stamped index root produced an invalid content hash '{}'",
                            res.content_hash
                        ))
                    })?
                }
            };

            self.set_ref(
                new_ledger_id,
                record.write_basis(),
                RefKind::IndexHead,
                &head_index_cid,
                index_t,
            )
            .await?;
            index_t_out = Some(index_t);
        }

        // Restore the ledger's stored default JSON-LD context, if the archive
        // carried one: re-point the new ledger's config at it so queries that
        // omit an inline @context keep working. Re-points config + bumps
        // configV via the standard CAS path; the re-put of the
        // (content-addressed) blob is idempotent.
        if let Some(ctx_json) = default_context {
            self.push_default_context(record, &ctx_json).await?;
        }
        let retracted_t = self
            .restore_retention(
                record,
                archived,
                lineage.as_ref(),
                std::slice::from_ref(record),
                false,
            )
            .await?;
        #[cfg(feature = "native")]
        let reconciled = match received {
            Some(received) => {
                let head_t = retracted_t.unwrap_or(commit_t);
                self.record_received_jobs(record, head_t, received.jobs, received.pending)
                    .await?;
                received.reconciled
            }
            None => Vec::new(),
        };
        #[cfg(not(feature = "native"))]
        let reconciled = Vec::new();

        Ok(RestoreResult {
            ledger_id: new_ledger_id.to_string(),
            commits,
            txn_blobs,
            index_artifacts,
            erased_commits,
            reconciled,
            commit_t: retracted_t.unwrap_or(commit_t),
            index_t: index_t_out,
        })
    }

    /// Incrementally import commits (pull path).
    ///
    /// Validates the commit chain, verifies ancestry against the local head,
    /// writes blobs to CAS, advances `CommitHead`, and updates in-memory novelty.
    ///
    /// `bundle.commits` must be ordered oldest → newest. `merged_commits`
    /// carries the commits that merges in `commits` brought in, parents
    /// before children. See [`PushCommitsRequest::merged_commits`].
    pub async fn import_commits_incremental(
        &self,
        ledger_id: &str,
        bundle: PushCommitsRequest,
    ) -> Result<CommitImportResult> {
        let PushCommitsRequest {
            commits,
            blobs,
            merged_commits,
            commit_ids,
            merged_commit_ids,
            ..
        } = bundle;
        if commits.is_empty() {
            return Err(ApiError::http(400, "no commits to import"));
        }
        let ledger_id = &LedgerId::parse(ledger_id)?;

        let mut guard = self
            .lock_ledger(ledger_id)
            .await?
            .ok_or_else(|| ApiError::internal("import requires a ledger manager"))?;
        let base_state = guard.clone_state_for_staging();

        // 1) Read current head ref.
        let current_ref = self
            .publisher()?
            .get_ref(base_state.ledger_id(), RefKind::CommitHead)
            .await?;
        let Some(current_ref) = current_ref else {
            return Err(ApiError::NotFound(format!(
                "Ledger not found: {}",
                base_state.ledger_id()
            )));
        };

        // 2) Build a PushCommitsRequest-compatible structure for validation reuse.
        let request = PushCommitsRequest {
            commits,
            blobs: blobs.clone(),
            missing_blobs: Vec::new(),
            merged_commits,
            commit_ids,
            merged_commit_ids,
            ..Default::default()
        };

        // 3) Decode and validate chain. The export carries no erased commit.
        let none = HashSet::new();
        let decoded = decode_and_validate_commit_chain(base_state.ledger_id(), &request, &none)
            .map_err(PushError::into_api_error)?;

        // 4) Ancestry preflight: verify first commit's parent matches local head.
        preflight_strict_next_t_and_prev(&current_ref, &decoded)
            .map_err(PushError::into_api_error)?;
        let txn_meta_g =
            txn_meta_sid_after(&base_state, &decoded).map_err(PushError::into_api_error)?;

        // 4.1) Decode the commits the chain's merges brought in, and check
        //      that no parent the bundle names is missing here.
        let merged = decode_merged_commits(&request, &none).map_err(PushError::into_api_error)?;
        admit_bundle_hashes(self, decoded.iter().chain(&merged), &request.blobs)
            .map_err(PushError::into_api_error)?;
        validate_merged_reachable(&decoded, &merged).map_err(PushError::into_api_error)?;
        let content_store = self.branched_content_store(base_state.ledger_id()).await?;
        validate_ancestry_present(content_store.as_ref(), &decoded, &merged)
            .await
            .map_err(PushError::into_api_error)?;

        // 5) Validate referenced blobs are provided.
        validate_required_blobs(&decoded, &merged, &request.blobs, &request.missing_blobs)
            .map_err(PushError::into_api_error)?;

        // 6) Write blobs + commit bytes to local CAS.
        let mut pins = Unpublished::new();
        let storage = self.backend().admin_storage_cloned().ok_or_else(|| {
            ApiError::config("push_commits_strict requires a managed storage backend")
        })?;
        write_required_blobs(
            &storage,
            &base_state.storage_namespace(),
            &request.blobs,
            &decoded,
            &merged,
            &mut pins,
        )
        .await
        .map_err(PushError::into_api_error)?;

        // Parents before children, as in `prepare_push`.
        write_commit_blobs(
            &storage,
            &base_state.storage_namespace(),
            &merged,
            &mut pins,
        )
        .await
        .map_err(PushError::into_api_error)?;

        let stored_commits = write_commit_blobs(
            &storage,
            &base_state.storage_namespace(),
            &decoded,
            &mut pins,
        )
        .await
        .map_err(PushError::into_api_error)?;

        let final_head = stored_commits.last().expect("non-empty stored_commits");
        let new_ref = RefValue {
            id: Some(final_head.commit_id.clone()),
            t: final_head.t,
        };

        // 7) Derive the post-pull state (novelty + dict novelty + namespace
        //    deltas) before the head moves: commits it cannot apply would
        //    leave a head the ledger cannot load.
        let all_flakes: Vec<(i64, Vec<Flake>)> = decoded
            .iter()
            .map(|c| {
                // Pushed commits come from a peer; screen the blob's flakes for
                // impersonated commit provenance before they reach novelty. See
                // the note on the clone path above and #1846.
                let mut flakes = c.commit.flakes.clone();
                let mut meta_flakes =
                    generate_commit_flakes(&c.commit, base_state.ledger_id(), c.commit.t);
                let dropped = drop_forged_commit_flakes(&mut flakes, &txn_meta_g);
                warn_if_forged_commit_flakes_dropped(dropped, base_state.ledger_id(), c.commit.t);
                stamp_graph_on_commit_flakes(&mut meta_flakes, &txn_meta_g);
                flakes.extend(meta_flakes);
                (c.commit.t, flakes)
            })
            .collect();

        let new_state = apply_pushed_commits_to_state(
            base_state,
            &all_flakes,
            &decoded,
            &stored_commits,
            &txn_meta_g,
        )
        .map_err(PushError::into_api_error)?;

        // 8) CAS update CommitHead.
        match self
            .publisher()?
            .compare_and_set_ref_fenced(
                new_state.ledger_id(),
                new_state.write_basis(),
                RefKind::CommitHead,
                Some(&current_ref),
                &new_ref,
            )
            .await?
        {
            CasResult::Updated => pins.published(),
            CasResult::Conflict { actual } => {
                return Err(ApiError::http(
                    409,
                    format!(
                        "commit head changed during import (expected t={}, actual={:?})",
                        current_ref.t, actual
                    ),
                ));
            }
        }

        // 9) Compute indexing status from the updated state.
        let index_config = self.default_index_config();
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = new_state.should_reindex(&index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: new_state.novelty_size(),
            index_t: new_state.index_t(),
            commit_t: final_head.t,
        };

        // Sync binary_store BEFORE replacing state so that concurrent readers
        // (via snapshot()) never see the new state with a stale binary_store.
        guard
            .ledger()
            .sync_binary_store_from_state(&new_state)
            .await;
        guard.replace(new_state);

        // 10) Trigger background indexing if enabled and needed.
        if let IndexingMode::Background(idx_handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                idx_handle.trigger(ledger_id, final_head.t).await;
            }
        }

        Ok(CommitImportResult {
            imported: decoded.len(),
            head_t: final_head.t,
            head_commit_id: final_head.commit_id.clone(),
            indexing: indexing_status,
        })
    }
}

#[cfg(not(feature = "native"))]
impl Fluree {
    /// Without erasure, a push carrying erased commits is refused.
    async fn receive_pushed_erasures(
        &self,
        _ledger_id: &LedgerId,
        request: &mut PushCommitsRequest,
        _authority: PushAuthority,
    ) -> Result<HashSet<ContentId>> {
        let erased_commit = request
            .commits
            .iter()
            .chain(&request.merged_commits)
            .any(|c| c.0.is_empty());
        if erased_commit || !request.erasure_frames.is_empty() {
            return Err(cannot_hold_erasures());
        }
        Ok(HashSet::new())
    }
}

impl Fluree {
    /// Whether a push may carry the ledger's removal journal, with commits
    /// or without: a cluster replicates its own.
    pub fn takes_pushed_retention(&self) -> bool {
        #[cfg(feature = "native")]
        {
            self.retention.replicas.get().is_none()
        }
        #[cfg(not(feature = "native"))]
        {
            false
        }
    }

    /// Whether this instance proves permanent erasures: one with file
    /// storage and a prover, a single node or a cluster's member, not a
    /// query peer. Taking erased commits by a transfer is another matter
    /// ([`Self::can_hold_erasures`]).
    pub fn can_prove_erasures(&self) -> bool {
        #[cfg(feature = "native")]
        {
            !self.nameservice_mode().is_read_only()
                && self
                    .retention
                    .erasure
                    .get()
                    .is_some_and(|runtime| runtime.config.is_some())
        }
        #[cfg(not(feature = "native"))]
        {
            false
        }
    }

    /// Whether a transfer may bring this instance permanently erased
    /// commits: a single node with file storage holds them.
    pub fn can_hold_erasures(&self) -> bool {
        #[cfg(feature = "native")]
        {
            self.retention.erasure.get().is_some() && self.retention.replicas.get().is_none()
        }
        #[cfg(not(feature = "native"))]
        {
            false
        }
    }
}

/// A transfer brought erased commits to an instance that cannot hold them.
pub(crate) fn cannot_hold_erasures() -> ApiError {
    ApiError::http(
        501,
        "the transfer holds permanently erased commits, which this instance cannot store: \
         that needs a single node with file storage",
    )
}

/// Transfers carry commits as stored, and an erased commit is stored as its
/// replacement, which no receiver accepts yet: refuse it by name.
pub(crate) fn refuse_erased(cid: &ContentId, bytes: &[u8]) -> Result<()> {
    if fluree_db_core::commit::codec::replacement::is_replacement(bytes) {
        return Err(ApiError::http(
            501,
            format!(
                "commit {cid} is permanently erased; transferring erased commits is not \
                 supported yet"
            ),
        ));
    }
    Ok(())
}
