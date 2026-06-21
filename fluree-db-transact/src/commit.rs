//! Transaction commit
//!
//! Commit factors into three phases that callers can compose:
//!
//! - [`build_commit`] — stage flakes, build the [`Commit`] record, sign,
//!   and serialize to bytes. Returns a [`StagedCommit`]. The commit
//!   blob is **not** written; the nameservice is **not** published to.
//!   Raw-txn JSON (if attached) **is** written during this phase, so
//!   the staged commit's `txn` ContentId is durable when the function
//!   returns.
//! - [`StagedCommit::apply`] — take a [`StagedCommit`], write the commit
//!   blob to the content store, publish through the nameservice (CAS or
//!   fast-forward), and finalize the new [`LedgerState`].
//! - [`StagedCommit::finalize_state`] — pure logic that builds the new
//!   [`LedgerState`] from a [`StagedCommit`] without touching storage or
//!   nameservice. Used by callers (e.g. Raft) that handle persistence
//!   themselves and need to derive post-apply state.
//!
//! [`commit`] composes [`build_commit`] + [`StagedCommit::apply`] for
//! the existing single-node transactional path.

use crate::error::{Result, TransactError};
use crate::namespace::NamespaceRegistry;
use crate::raw_txn_upload::PendingRawTxnUpload;
use chrono::Utc;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::{ContentId, ContentKind, ContentStore, DictNovelty, Flake, TXN_META_GRAPH_ID};
use fluree_db_ledger::{IndexConfig, LedgerState, StagedLedger};
use fluree_db_nameservice::{CasResult, NameServiceLookup, RefKind, RefPublisher, RefValue};
use fluree_db_novelty::{generate_commit_flakes, stamp_graph_on_commit_flakes};
use fluree_db_novelty::{Commit, SigningKey, TxnMetaEntry, TxnMetaValue, TxnSignature};
use fluree_db_query::BinaryRangeProvider;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::Instrument;

/// Receipt returned after a successful commit
#[derive(Debug, Clone)]
pub struct CommitReceipt {
    /// Content identifier (CIDv1) — primary identity
    pub commit_id: ContentId,
    /// Transaction time of this commit
    pub t: i64,
    /// Number of flakes in the commit
    pub flake_count: usize,
}

/// Output of [`build_commit`].
///
/// Carries everything a caller needs to either drive the commit to
/// completion through the normal [`Self::apply`] path *or* to handle
/// persistence themselves (e.g. a Raft-coordinated committer writes
/// the blob and proposes the ref advancement through consensus, then
/// calls [`Self::finalize_state`] to derive the post-apply state).
///
/// `commit.id` is already populated — it's the SHA-256 of
/// `commit_bytes` tagged with `ContentKind::Commit`. Writing
/// `commit_bytes` via `ContentStore::put_with_id(&commit.id.unwrap(), &commit_bytes)`
/// is idempotent and round-trips to the same CID.
pub struct StagedCommit {
    /// The commit object. `commit.id` is set to its content id.
    pub commit: Commit,
    /// Canonical serialized bytes; `commit.id == ContentId::new(ContentKind::Commit, &commit_bytes)`.
    pub commit_bytes: Vec<u8>,
    /// Head observed during staging — the value the caller passes as
    /// `expected_prev` for the CAS / `AdvanceRef` proposal. `None`
    /// only on the first commit of a branch (genesis).
    pub expected_head_ref: Option<RefValue>,
    /// Bytes for content-addressed payloads the commit references but
    /// that haven't been pre-uploaded by another path. Today the only
    /// such payload (raw-txn JSON) is uploaded during `build_commit`,
    /// so this map is empty; the field is here so future content kinds
    /// can defer writes without changing the struct shape.
    pub referenced_bytes: HashMap<ContentId, Vec<u8>>,
    /// Tracking accounting from staging if `tracking` was set on the
    /// builder.
    pub tally: Option<fluree_db_core::TrackingTally>,
    /// Build-time state needed by [`StagedCommit::apply`] /
    /// [`StagedCommit::finalize_state`] to construct the post-commit
    /// `LedgerState`. Private — callers shouldn't peek inside.
    pub(crate) build_state: BuildState,
}

/// Internal carrier for everything `build_commit` computes that
/// `StagedCommit::apply` / `StagedCommit::finalize_state` needs to
/// construct the post-commit [`LedgerState`].
pub(crate) struct BuildState {
    base: LedgerState,
    new_t: i64,
    flake_count: usize,
    /// CID of raw-txn JSON if it was uploaded during build; carried
    /// so a publish failure can release the orphaned blob.
    txn_id_for_release: Option<ContentId>,
}

/// Options for commit operation
///
/// **Clone behavior:** `CommitOpts` implements `Clone` manually and **omits**
/// the `raw_txn_upload` field (the clone receives `None`). A `PendingRawTxnUpload`
/// owns a Tokio `JoinHandle` plus a Drop-guard release task, so cloning it would
/// either duplicate the release or split ownership — neither is sound. In every
/// current call path the `commit_opts_base.clone()` pattern happens **before**
/// `with_raw_txn_spawned`, so the pending upload is only attached on the final,
/// un-cloned `CommitOpts` that flows into `commit()`.
#[derive(Default)]
pub struct CommitOpts {
    /// Authenticated/impersonated identity acting on the transaction.
    ///
    /// System-controlled: typically the verified DID from a signed credential
    /// or the resolved `opts.identity`. Emitted as `f:identity` on the commit
    /// subject in the txn-meta graph. Overrides any user-supplied `f:identity`
    /// triple in the transaction body.
    ///
    /// `f:message` and `f:author` are **not** fields here — they are user
    /// claims and flow through the transaction body as regular txn-meta.
    pub identity: Option<String>,
    /// In-flight parallel upload of the raw transaction JSON.
    ///
    /// Populated by [`CommitOpts::with_raw_txn_spawned`]. Awaited by the
    /// caller of [`build_commit`] just before staging completes, so the
    /// upload overlaps staging CPU work on the caller's path.
    pub raw_txn_upload: Option<PendingRawTxnUpload>,
    /// Pre-resolved raw-txn ContentId. Set when the upload was already
    /// awaited upstream (e.g. on the Raft leader before enqueueing, so
    /// the CID can travel through the queue envelope and be reattached
    /// worker-side). When both this and `raw_txn_upload` are set,
    /// [`build_commit`] prefers this CID and discards the upload.
    pub raw_txn_id: Option<ContentId>,
    /// Ed25519 signing key for commit signatures (opt-in).
    /// When set, the commit blob includes a trailing signature block.
    pub signing_key: Option<Arc<SigningKey>>,
    /// Transaction signature (audit metadata: who submitted the transaction).
    pub txn_signature: Option<TxnSignature>,
    /// User-provided transaction metadata.
    ///
    /// Stored in the commit envelope and emitted to the txn-meta graph (`g_id=1`)
    /// during indexing. Each entry becomes a triple with the commit as subject.
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Named graph IRI to g_id mappings introduced by this transaction.
    ///
    /// Stored in the commit envelope for replay-safe persistence. The indexer
    /// uses this to resolve graph IRIs to dictionary IDs when building the index.
    pub graph_delta: std::collections::HashMap<u16, String>,
    /// Namespace code delta to carry forward from original commits during rebase.
    ///
    /// When set, this overrides the `NamespaceRegistry::take_delta()` result,
    /// preserving the original commit's namespace allocations during replay.
    pub namespace_delta: Option<std::collections::HashMap<u16, String>>,
    /// Skip backpressure checks (novelty size limits).
    ///
    /// Used during rebase replay where the branch is disconnected and we
    /// control the full commit sequence.
    pub skip_backpressure: bool,
    /// Skip sequencing verification (commit head matching).
    ///
    /// Used during rebase replay where the base state is the source branch
    /// but we commit to the target branch namespace. The sequencing check
    /// would fail because the nameservice head doesn't match the base state.
    pub skip_sequencing: bool,
    /// Additional parent commit IDs for merge commits.
    ///
    /// When non-empty, these are appended as extra `parents` on the
    /// commit record, producing a multi-parent (merge) commit. The primary
    /// parent is still derived from `base.head_commit_id`.
    pub merge_parents: Vec<ContentId>,
    /// ISO 8601 timestamp for the commit.
    ///
    /// When `None`, defaults to `Utc::now().to_rfc3339()`. Provide a fixed
    /// value for deterministic commit hashes (testing, replay).
    pub timestamp: Option<String>,
}

impl std::fmt::Debug for CommitOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitOpts")
            .field("identity", &self.identity)
            .field("raw_txn_upload", &self.raw_txn_upload.is_some())
            .field("raw_txn_id", &self.raw_txn_id)
            .field("signing_key", &self.signing_key.is_some())
            .field(
                "txn_signature",
                &self.txn_signature.as_ref().map(|s| &s.signer),
            )
            .field("txn_meta_count", &self.txn_meta.len())
            .field("graph_delta_count", &self.graph_delta.len())
            .field(
                "namespace_delta",
                &self
                    .namespace_delta
                    .as_ref()
                    .map(std::collections::HashMap::len),
            )
            .field("skip_backpressure", &self.skip_backpressure)
            .field("skip_sequencing", &self.skip_sequencing)
            .field("merge_parents", &self.merge_parents.len())
            .finish()
    }
}

impl Clone for CommitOpts {
    // The `raw_txn_upload` field is intentionally omitted: a PendingRawTxnUpload
    // owns a tokio JoinHandle + Drop-guard release and is not safely cloneable.
    // All current builder paths clone the base CommitOpts *before* attaching the
    // spawned upload, so the clone correctly starts with no upload.
    fn clone(&self) -> Self {
        Self {
            identity: self.identity.clone(),
            raw_txn_upload: None,
            raw_txn_id: self.raw_txn_id.clone(),
            signing_key: self.signing_key.clone(),
            txn_signature: self.txn_signature.clone(),
            txn_meta: self.txn_meta.clone(),
            graph_delta: self.graph_delta.clone(),
            namespace_delta: self.namespace_delta.clone(),
            skip_backpressure: self.skip_backpressure,
            skip_sequencing: self.skip_sequencing,
            merge_parents: self.merge_parents.clone(),
            timestamp: self.timestamp.clone(),
        }
    }
}

impl CommitOpts {
    /// Set the authenticated identity for this commit.
    ///
    /// Recorded as `f:identity` in the txn-meta graph. For signed transactions,
    /// this should be the verified DID.
    pub fn identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = Some(identity.into());
        self
    }

    /// Spawn a parallel upload of the raw transaction JSON to the content
    /// store and attach the handle.
    ///
    /// The upload runs concurrently with staging CPU work. The caller of
    /// [`build_commit`] awaits the handle just before staging completes,
    /// so durability is preserved but the serial latency on the caller's
    /// path is reduced. On error paths that drop `CommitOpts` without
    /// awaiting, the pending upload's Drop guard releases any content
    /// that was stored.
    pub fn with_raw_txn_spawned(
        mut self,
        content_store: Arc<dyn fluree_db_core::ContentStore>,
        txn: serde_json::Value,
    ) -> Self {
        self.raw_txn_upload = Some(PendingRawTxnUpload::spawn(content_store, txn));
        self
    }

    /// Set the signing key for commit signatures
    pub fn with_signing_key(mut self, key: Arc<SigningKey>) -> Self {
        self.signing_key = Some(key);
        self
    }

    /// Set the transaction signature (audit metadata)
    pub fn with_txn_signature(mut self, sig: TxnSignature) -> Self {
        self.txn_signature = Some(sig);
        self
    }

    /// Append entries to the user-provided transaction metadata.
    ///
    /// Appends rather than replaces so any pre-loaded entries (e.g., from a
    /// programmatic embedder constructing `TxnMetaEntry` directly) compose
    /// with body-extracted entries.
    ///
    /// To set `f:message` or `f:author`, include them in the transaction body
    /// (envelope form with `@graph`); they flow through as ordinary user
    /// txn-meta. There is no CommitOpts shortcut by design — they are not
    /// system provenance.
    pub fn with_txn_meta(mut self, mut txn_meta: Vec<TxnMetaEntry>) -> Self {
        self.txn_meta.append(&mut txn_meta);
        self
    }

    /// Set the named graph delta (g_id -> IRI mappings)
    pub fn with_graph_delta(mut self, graph_delta: std::collections::HashMap<u16, String>) -> Self {
        self.graph_delta = graph_delta;
        self
    }

    /// Set a pre-computed namespace delta (for rebase replay).
    pub fn with_namespace_delta(
        mut self,
        ns_delta: std::collections::HashMap<u16, String>,
    ) -> Self {
        self.namespace_delta = Some(ns_delta);
        self
    }

    /// Skip backpressure checks (for rebase replay).
    pub fn with_skip_backpressure(mut self) -> Self {
        self.skip_backpressure = true;
        self
    }

    /// Skip sequencing verification (for rebase replay).
    pub fn with_skip_sequencing(mut self) -> Self {
        self.skip_sequencing = true;
        self
    }

    /// Set additional parent commit IDs for merge commits.
    pub fn with_merge_parents(mut self, parents: Vec<ContentId>) -> Self {
        self.merge_parents = parents;
        self
    }

    /// Set the commit timestamp (ISO 8601). When not set, `Utc::now()` is used.
    pub fn with_timestamp(mut self, ts: impl Into<String>) -> Self {
        self.timestamp = Some(ts.into());
        self
    }
}

/// Serializable subset of [`CommitOpts`] carrying only the fields a
/// transactor client supplies at submission time.
///
/// Used to round-trip a transaction request through content-addressed
/// storage (e.g. the Raft command queue): the leader extracts this
/// projection from the incoming `CommitOpts`, writes the enclosing
/// envelope to CAS, and the worker rehydrates a runtime [`CommitOpts`]
/// via [`Self::into_commit_opts`].
///
/// Fields excluded by design:
/// - `signing_key` — node-level credential resolved worker-side from
///   local config, never transmitted with the request.
/// - `raw_txn_upload` — runtime task; the leader awaits any pending
///   upload before enqueueing and carries the resolved CID via
///   `raw_txn_id`, so the worker doesn't re-do the upload.
/// - `graph_delta` / `namespace_delta` / `skip_backpressure` /
///   `skip_sequencing` / `merge_parents` — populated during staging or
///   reserved for the rebase/merge paths, which carry their own
///   request envelopes when they join the queue.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CommitOptsRequest {
    pub identity: Option<String>,
    pub txn_signature: Option<TxnSignature>,
    pub txn_meta: Vec<TxnMetaEntry>,
    pub timestamp: Option<String>,
    /// Pre-resolved raw-txn CID. Set when the upstream caller's
    /// [`CommitOpts::raw_txn_upload`] was awaited before projection
    /// (e.g. by the Raft leader so the worker can reference the same
    /// raw payload the client submitted). When `None`, the worker's
    /// staging path may re-derive the upload from the parsed body when
    /// `txn_opts.store_raw_txn` is set.
    #[serde(default)]
    pub raw_txn_id: Option<ContentId>,
}

impl CommitOptsRequest {
    /// Lift the request-side projection into a runtime [`CommitOpts`].
    /// Node-side and staging-side fields stay at their defaults; the
    /// worker layers them in (e.g. attaching `signing_key` from local
    /// config) before passing the opts to the staging path. The
    /// pre-resolved `raw_txn_id` is reattached so the build path
    /// references the same raw payload the client uploaded.
    pub fn into_commit_opts(self) -> CommitOpts {
        CommitOpts {
            identity: self.identity,
            raw_txn_upload: None,
            raw_txn_id: self.raw_txn_id,
            signing_key: None,
            txn_signature: self.txn_signature,
            txn_meta: self.txn_meta,
            graph_delta: HashMap::new(),
            namespace_delta: None,
            skip_backpressure: false,
            skip_sequencing: false,
            merge_parents: Vec::new(),
            timestamp: self.timestamp,
        }
    }
}

impl From<CommitOptsRequest> for CommitOpts {
    fn from(req: CommitOptsRequest) -> Self {
        req.into_commit_opts()
    }
}

impl From<&CommitOpts> for CommitOptsRequest {
    /// Extract the serializable client-supplied subset. Node-side and
    /// staging-side fields are dropped. `raw_txn_id` passes through
    /// when already resolved; an unresolved `raw_txn_upload` is not
    /// awaited here — callers that need it materialized (Raft leader)
    /// must `finish()` it explicitly before projecting.
    fn from(opts: &CommitOpts) -> Self {
        Self {
            identity: opts.identity.clone(),
            txn_signature: opts.txn_signature.clone(),
            txn_meta: opts.txn_meta.clone(),
            timestamp: opts.timestamp.clone(),
            raw_txn_id: opts.raw_txn_id.clone(),
        }
    }
}

/// Commit a staged transaction
///
/// This function:
/// 1. Extracts flakes from the view
/// 2. Checks backpressure (novelty not at max)
/// 3. Verifies sequencing (t and previous match expected)
/// 4. Builds and content-addresses the commit record
/// 5. Writes to storage
/// 6. Publishes to nameservice
/// 7. Returns new LedgerState with updated novelty
///
/// # Arguments
///
/// * `view` - The staged ledger view
/// * `ns_registry` - Namespace registry with any new allocations
/// * `content_store` - Content-addressed store for writing commit and txn blobs
/// * `nameservice` - Nameservice for lookup and publishing
/// * `index_config` - Configuration for backpressure limits
/// * `opts` - Commit options (identity, raw_txn, txn_meta, etc.)
///
/// # Returns
///
/// A tuple of (CommitReceipt, new LedgerState)
pub async fn build_commit(
    view: StagedLedger,
    mut ns_registry: NamespaceRegistry,
    expected_head_ref: Option<RefValue>,
    txn_id: Option<ContentId>,
    index_config: &IndexConfig,
    opts: CommitOpts,
) -> Result<StagedCommit> {
    // 1. Extract flakes from view
    let (mut base, flakes) = view.into_parts();

    // Move commit options into locals so we can pass ownership where useful
    // (e.g., txn_meta) without forcing clones, while still using other fields later.
    let CommitOpts {
        identity,
        signing_key,
        txn_signature,
        mut txn_meta,
        graph_delta,
        namespace_delta: override_ns_delta,
        skip_backpressure,
        merge_parents,
        timestamp: opt_timestamp,
        ..
    } = opts;

    // For signed transactions the txn_signature.signer is the cryptographically
    // verified identity — it overrides any caller-supplied CommitOpts.identity
    // or user-supplied f:identity triple in the transaction body.
    let effective_identity = txn_signature
        .as_ref()
        .map(|sig| sig.signer.clone())
        .or(identity);

    if let Some(ref identity_val) = effective_identity {
        // Strip any user-supplied f:identity claim so the system-controlled
        // value is the only one recorded.
        txn_meta.retain(|entry| {
            !(entry.predicate_ns == fluree_vocab::namespaces::FLUREE_DB
                && entry.predicate_name == fluree_vocab::db::IDENTITY)
        });
        txn_meta.push(TxnMetaEntry::new(
            fluree_vocab::namespaces::FLUREE_DB,
            fluree_vocab::db::IDENTITY,
            TxnMetaValue::String(identity_val.clone()),
        ));
    }

    // No wrapper span: the caller's ambient span (e.g. `txn_commit`
    // from `commit()`, or whatever the Raft path opens) carries the
    // build-time fields, so sub-spans below stay direct children of
    // that ambient span.
    let commit_span = tracing::Span::current();

    // 2. Check for empty transaction — merge commits with no data flakes are
    //    valid (e.g., TakeBranch strategy drops all source flakes) because
    //    the commit still records the merge-parent relationship in the DAG.
    if flakes.is_empty() && merge_parents.is_empty() {
        return Err(TransactError::EmptyTransaction);
    }

    // 3. Check backpressure - current novelty at max
    if !skip_backpressure && base.at_max_novelty(index_config) {
        return Err(TransactError::NoveltyAtMax);
    }

    // 4. Predictive sizing - would these flakes reach or exceed max?
    let delta_bytes: usize = flakes.iter().map(fluree_db_core::Flake::size_bytes).sum();
    let current_bytes = base.novelty_size();
    let max_bytes = index_config.reindex_max_bytes;
    commit_span.record("flake_count", flakes.len());
    commit_span.record("delta_bytes", delta_bytes);
    commit_span.record("current_novelty_bytes", current_bytes);
    if !skip_backpressure && current_bytes + delta_bytes >= max_bytes {
        return Err(TransactError::NoveltyWouldExceed {
            current_bytes,
            delta_bytes,
            max_bytes,
        });
    }

    // 5. Build commit record
    //    (sequencing verification + nameservice lookup are the
    //    caller's responsibility; the value to stash in
    //    `expected_head_ref` is passed in.)
    let new_t = base.t() + 1;
    let flake_count = flakes.len();

    // Capture namespace delta once:
    // - write into commit record for persistence
    // - apply to returned in-memory LedgerSnapshot so subsequent operations (e.g., SPARQL/JSON-LD queries)
    //   can encode IRIs without requiring a reload.
    let ns_delta = {
        let span = tracing::debug_span!("commit_namespace_delta");
        let _g = span.enter();
        override_ns_delta.unwrap_or_else(|| ns_registry.take_delta())
    };

    // Apply envelope deltas (namespace + graph) to the in-memory LedgerSnapshot.
    // This must happen before novelty apply so encode_iri() works for graph routing.
    Arc::make_mut(&mut base.snapshot).apply_envelope_deltas(
        &ns_delta,
        graph_delta.values().map(std::string::String::as_str),
    )?;

    // Use caller-provided timestamp or default to wall clock.
    let timestamp = opt_timestamp.unwrap_or_else(|| Utc::now().to_rfc3339());

    // The caller is responsible for uploading the raw-txn JSON
    // (if any) before invoking this function — the result is
    // passed in as `txn_id`. We retain it for both the commit
    // record (`with_txn`) and the release-on-failure path that
    // `StagedCommit::apply` uses if publish fails.
    let txn_id_for_release: Option<ContentId> = txn_id.clone();

    let head_commit_id = base.head_commit_id.clone();
    let ledger_id_for_publish = base.ledger_id().to_string();
    let ns_split_mode_for_genesis = if base.head_commit_id.is_none() {
        Some(ns_registry.split_mode())
    } else {
        None
    };

    // Build the commit record (still under the build phase).
    let mut commit_record = {
        let span = tracing::debug_span!("commit_build_record");
        let _g = span.enter();
        Commit::new(new_t, flakes)
            .with_namespace_delta(ns_delta)
            .with_time(timestamp)
    };

    if let Some(txn_cid) = txn_id {
        commit_record = commit_record.with_txn(txn_cid);
    }
    if let Some(txn_sig) = txn_signature {
        commit_record = commit_record.with_txn_signature(txn_sig);
    }
    if !txn_meta.is_empty() {
        commit_record = commit_record.with_txn_meta(txn_meta);
    }
    if !graph_delta.is_empty() {
        commit_record.graph_delta = graph_delta;
    }
    if let Some(split_mode) = ns_split_mode_for_genesis {
        commit_record.ns_split_mode = Some(split_mode);
    }
    if let Some(cid) = head_commit_id.clone() {
        commit_record = commit_record.with_parent(cid);
    }
    for merge_parent in &merge_parents {
        commit_record = commit_record.with_parent(merge_parent.clone());
    }

    // 7. Sign + serialize. Computes the commit blob bytes and
    // (locally) the CID, without writing the blob to the content
    // store. StagedCommit::apply / RaftCommitter will write the bytes;
    // the CID is deterministic from the bytes.
    let signing = signing_key
        .as_ref()
        .map(|key| (key.as_ref(), ledger_id_for_publish.as_str()));
    let signed = {
        let span = tracing::debug_span!("commit_serialize_commit_blob");
        let _g = span.enter();
        crate::commit_v2::write_commit(&commit_record, true, signing)?
    };
    let commit_cid = ContentId::new(ContentKind::Commit, &signed.bytes);
    commit_record.id = Some(commit_cid.clone());

    Ok(StagedCommit {
        commit: commit_record,
        commit_bytes: signed.bytes,
        expected_head_ref,
        referenced_bytes: HashMap::new(),
        tally: None,
        build_state: BuildState {
            base,
            new_t,
            flake_count,
            txn_id_for_release,
        },
    })
}

impl StagedCommit {
    /// Apply phase of [`commit`]. Consumes the [`StagedCommit`] from
    /// [`build_commit`], writes the commit blob via content-addressable
    /// `put_with_id` (idempotent), publishes the new head ref to the
    /// nameservice, and finalizes the resulting [`LedgerState`].
    ///
    /// On publish failure, the raw-txn blob uploaded during
    /// [`build_commit`] is released — that CID is no longer referenced
    /// by any durable commit record.
    pub async fn apply<C, N>(
        self,
        content_store: &C,
        nameservice: &N,
        skip_sequencing: bool,
    ) -> Result<(CommitReceipt, LedgerState)>
    where
        C: ContentStore + ?Sized,
        N: RefPublisher + ?Sized,
    {
        apply_commit_inner(self, content_store, nameservice, skip_sequencing).await
    }

    /// Pure post-publish/post-apply state finalization. Builds the
    /// [`LedgerState`] and [`CommitReceipt`] from a [`StagedCommit`]
    /// whose commit has been made durable through some external
    /// mechanism (the standard nameservice publish in [`Self::apply`],
    /// or a Raft `AdvanceRef` apply for the consensus path). No I/O —
    /// caller is responsible for ensuring the commit is durable before
    /// invoking.
    pub fn finalize_state(self) -> Result<(CommitReceipt, LedgerState)> {
        let StagedCommit {
            commit,
            build_state,
            ..
        } = self;
        let BuildState {
            base,
            new_t,
            flake_count,
            ..
        } = build_state;
        let commit_cid = commit.id.clone().expect("build_commit sets commit.id");
        finalize_state_with_base(commit, commit_cid, new_t, flake_count, base)
    }
}

async fn apply_commit_inner<C, N>(
    staged: StagedCommit,
    content_store: &C,
    nameservice: &N,
    skip_sequencing: bool,
) -> Result<(CommitReceipt, LedgerState)>
where
    C: ContentStore + ?Sized,
    N: RefPublisher + ?Sized,
{
    let StagedCommit {
        commit: commit_record,
        commit_bytes,
        expected_head_ref,
        referenced_bytes,
        tally: _,
        build_state,
    } = staged;
    let BuildState {
        base,
        new_t,
        flake_count,
        txn_id_for_release,
    } = build_state;

    let commit_cid = commit_record
        .id
        .clone()
        .expect("build_commit sets commit.id");
    let ledger_id_for_publish = base.ledger_id().to_string();

    // 8. Write referenced blobs the build phase deferred (today: none),
    //    then write the commit blob via put_with_id (idempotent).
    let write_and_publish = async {
        for (cid, bytes) in &referenced_bytes {
            content_store
                .put_with_id(cid, bytes)
                .instrument(tracing::debug_span!("commit_write_referenced_blob"))
                .await?;
        }
        {
            let span = tracing::debug_span!("commit_write_commit_blob");
            let _g = span.enter();
            content_store
                .put_with_id(&commit_cid, &commit_bytes)
                .await?;
            tracing::info!(commit_bytes = commit_bytes.len(), "commit blob stored");
        }

        // 9. Publish to nameservice.
        let new_head_ref = RefValue {
            id: Some(commit_cid.clone()),
            t: new_t,
        };
        let publish_result = if skip_sequencing {
            nameservice
                .fast_forward_commit(ledger_id_for_publish.as_str(), &new_head_ref, 3)
                .instrument(tracing::debug_span!("commit_publish_nameservice"))
                .await?
        } else {
            nameservice
                .compare_and_set_ref(
                    ledger_id_for_publish.as_str(),
                    RefKind::CommitHead,
                    expected_head_ref.as_ref(),
                    &new_head_ref,
                )
                .instrument(tracing::debug_span!("commit_publish_nameservice"))
                .await?
        };
        match publish_result {
            CasResult::Updated => {}
            CasResult::Conflict { actual } if skip_sequencing => {
                let head_ahead = actual.as_ref().map(|r| r.t >= new_t).unwrap_or(false);
                if !head_ahead {
                    return Err(TransactError::PublishLostRace {
                        ledger_id: ledger_id_for_publish.clone(),
                        attempted_t: new_t,
                        attempted_commit_id: commit_cid.to_string(),
                        published_t: actual.as_ref().map(|r| r.t).unwrap_or(0),
                        published_commit_id: actual
                            .and_then(|r| r.id)
                            .map(|cid| cid.to_string())
                            .unwrap_or_else(|| "None".to_string()),
                    });
                }
            }
            CasResult::Conflict { actual } => {
                return Err(TransactError::PublishLostRace {
                    ledger_id: ledger_id_for_publish.clone(),
                    attempted_t: new_t,
                    attempted_commit_id: commit_cid.to_string(),
                    published_t: actual.as_ref().map(|r| r.t).unwrap_or(0),
                    published_commit_id: actual
                        .and_then(|r| r.id)
                        .map(|cid| cid.to_string())
                        .unwrap_or_else(|| "None".to_string()),
                });
            }
        }
        Ok::<_, TransactError>(())
    };

    if let Err(e) = write_and_publish.await {
        // Release raw-txn if its upload preceded a failed publish.
        if let Some(cid) = &txn_id_for_release {
            if let Err(release_err) = content_store.release(cid).await {
                tracing::warn!(
                    error = %release_err,
                    raw_txn_cid = %cid,
                    "failed to release raw txn after commit failure"
                );
            }
        }
        return Err(e);
    }
    // Commit published — raw_txn is durably referenced. The
    // `txn_id_for_release` value falls out of scope.
    let _ = txn_id_for_release;

    finalize_state_with_base(commit_record, commit_cid, new_t, flake_count, base)
}

fn finalize_state_with_base(
    mut commit_record: Commit,
    commit_cid: ContentId,
    new_t: i64,
    flake_count: usize,
    base: LedgerState,
) -> Result<(CommitReceipt, LedgerState)> {
    // 10. Generate commit metadata flakes
    let commit_metadata_flakes = {
        let span = tracing::debug_span!("commit_generate_metadata_flakes");
        let _g = span.enter();
        let mut flakes = generate_commit_flakes(&commit_record, base.ledger_id(), new_t);
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(base.ledger_id());
        if let Some(g_sid) = base.snapshot.encode_iri(&txn_meta_iri) {
            stamp_graph_on_commit_flakes(&mut flakes, &g_sid);
        }
        flakes
    };
    tracing::info!(
        metadata_flakes = commit_metadata_flakes.len(),
        "commit metadata flakes generated"
    );

    let mut all_flakes = std::mem::take(&mut commit_record.flakes);
    all_flakes.extend(commit_metadata_flakes);

    let mut dict_novelty = base.dict_novelty.clone();
    {
        let span = tracing::debug_span!("commit_populate_dict_novelty");
        let _g = span.enter();
        let store = base
            .snapshot
            .range_provider
            .as_ref()
            .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>())
            .map(|brp| Arc::clone(brp.store()))
            .or_else(|| {
                base.binary_store
                    .as_ref()
                    .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok())
            });
        populate_dict_novelty(
            Arc::make_mut(&mut dict_novelty),
            store.as_deref(),
            &all_flakes,
        )?;
    }

    let mut runtime_small_dicts = Arc::clone(&base.runtime_small_dicts);
    Arc::make_mut(&mut runtime_small_dicts).populate_from_flakes(&all_flakes);

    let mut new_novelty = Arc::clone(&base.novelty);
    {
        let span = tracing::debug_span!("commit_apply_to_novelty");
        let _g = span.enter();
        let mut reverse_graph = base.snapshot.build_reverse_graph()?;
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(base.ledger_id());
        if let Some(g_sid) = base.snapshot.encode_iri(&txn_meta_iri) {
            reverse_graph.entry(g_sid).or_insert(TXN_META_GRAPH_ID);
        }
        Arc::make_mut(&mut new_novelty).apply_commit(all_flakes, new_t, &reverse_graph)?;
    }

    let mut snapshot = base.snapshot;
    // Build the re-attached provider first so the read-borrow of `snapshot`
    // ends before the `Arc::make_mut` (copy-on-write) assignment below.
    let new_range_provider: Option<Arc<dyn fluree_db_core::range_provider::RangeProvider>> =
        snapshot.range_provider.as_ref().and_then(|provider| {
            provider
                .as_any()
                .downcast_ref::<BinaryRangeProvider>()
                .map(|brp| {
                    let ns_fallback = Some(snapshot.shared_namespaces());
                    Arc::new(BinaryRangeProvider::new(
                        Arc::clone(brp.store()),
                        Arc::clone(&dict_novelty),
                        Arc::clone(&runtime_small_dicts),
                        ns_fallback,
                    )) as Arc<dyn fluree_db_core::range_provider::RangeProvider>
                })
        });
    if let Some(rp) = new_range_provider {
        Arc::make_mut(&mut snapshot).range_provider = Some(rp);
    }

    let new_state = LedgerState {
        snapshot,
        novelty: new_novelty,
        dict_novelty,
        runtime_small_dicts,
        head_commit_id: Some(commit_cid.clone()),
        head_index_id: base.head_index_id,
        ns_record: base.ns_record,
        binary_store: base.binary_store,
        spatial_indexes: base.spatial_indexes,
    };

    let receipt = CommitReceipt {
        commit_id: commit_cid,
        t: new_t,
        flake_count,
    };
    Ok((receipt, new_state))
}

/// Existing single-node transactional path — composes
/// [`build_commit`] + [`StagedCommit::apply`].
pub async fn commit<C, N>(
    view: StagedLedger,
    ns_registry: NamespaceRegistry,
    content_store: &C,
    nameservice: &N,
    index_config: &IndexConfig,
    mut opts: CommitOpts,
) -> Result<(CommitReceipt, LedgerState)>
where
    C: ContentStore + ?Sized,
    N: NameServiceLookup + RefPublisher + ?Sized,
{
    let skip_sequencing = opts.skip_sequencing;

    let commit_span = tracing::debug_span!(
        "txn_commit",
        alias = view.base().ledger_id(),
        base_t = view.base().t(),
        flake_count = tracing::field::Empty,
        delta_bytes = tracing::field::Empty,
        current_novelty_bytes = tracing::field::Empty,
        max_novelty_bytes = index_config.reindex_max_bytes,
        has_raw_txn = opts.raw_txn_upload.is_some(),
    );

    async move {
        // Run the cheap pre-checks before awaiting the raw-txn upload.
        // If lookup or sequencing fails, the still-pending upload is
        // dropped here; `PendingRawTxnUpload`'s Drop guard releases any
        // blob the upload already landed in CAS. Awaiting `finish()`
        // first would have promoted the blob to a referenced CID with
        // no caller obligated to release it.
        let current = if skip_sequencing {
            None
        } else {
            nameservice
                .lookup(view.base().ledger_id())
                .instrument(tracing::debug_span!("commit_nameservice_lookup"))
                .await?
        };
        if !skip_sequencing {
            let span = tracing::debug_span!("commit_verify_sequencing");
            let _g = span.enter();
            verify_sequencing(view.base(), current.as_ref())?;
        }
        let expected_head_ref = current.as_ref().map(commit_head_ref);

        let txn_id = if let Some(pending) = opts.raw_txn_upload.take() {
            Some(
                pending
                    .finish()
                    .instrument(tracing::debug_span!("commit_write_raw_txn"))
                    .await?,
            )
        } else {
            opts.raw_txn_id.take()
        };

        // `build_commit` consumes `txn_id` but its early-return paths
        // (EmptyTransaction, NoveltyAtMax/WouldExceed, envelope-delta
        // failure, serialize failure) all return before installing
        // `txn_id_for_release` on the staged commit's `BuildState`.
        // Release here so an early build error doesn't orphan the
        // blob we just resolved.
        let txn_id_for_cleanup = txn_id.clone();
        let staged = match build_commit(
            view,
            ns_registry,
            expected_head_ref,
            txn_id,
            index_config,
            opts,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                release_raw_txn_after_build_err(content_store, txn_id_for_cleanup.as_ref()).await;
                return Err(e);
            }
        };
        staged
            .apply(content_store, nameservice, skip_sequencing)
            .await
    }
    .instrument(commit_span)
    .await
}

/// Release a raw-txn blob whose CID was resolved before a failed
/// [`build_commit`]. A failed release is logged but not propagated —
/// the caller is already returning an error and shouldn't have its
/// failure mode replaced.
pub async fn release_raw_txn_after_build_err<C>(content_store: &C, txn_id: Option<&ContentId>)
where
    C: ContentStore + ?Sized,
{
    let Some(cid) = txn_id else { return };
    if let Err(release_err) = content_store.release(cid).await {
        tracing::warn!(
            error = %release_err,
            raw_txn_cid = %cid,
            "failed to release raw txn after build_commit failure"
        );
    }
}

fn commit_head_ref(record: &fluree_db_nameservice::NsRecord) -> RefValue {
    RefValue {
        id: record.commit_head_id.clone(),
        t: record.commit_t,
    }
}

/// Populate DictNovelty with subjects and strings from committed flakes.
///
/// Scans each flake for:
/// - Subject IDs (`flake.s`) — registered as novel subjects
/// - Object references (`FlakeValue::Ref`) — registered as novel subjects
/// - String values (`FlakeValue::String`, `FlakeValue::Json`) — registered as novel strings
///
/// Does NOT check the persisted tree — some entries may shadow persisted subjects.
/// This is safe because `DictOverlay` checks the persisted tree first for reverse
/// lookups (canonical ID wins).
fn populate_dict_novelty(
    dict_novelty: &mut DictNovelty,
    store: Option<&BinaryIndexStore>,
    flakes: &[Flake],
) -> Result<()> {
    fluree_db_binary_index::dict_novelty_safe::populate_dict_novelty_safe(
        dict_novelty,
        store,
        flakes.iter(),
    )
    .map_err(|e| TransactError::FlakeGeneration(format!("populate_dict_novelty_safe: {e}")))
}

fn verify_sequencing(
    base: &LedgerState,
    current: Option<&fluree_db_nameservice::NsRecord>,
) -> Result<()> {
    match current {
        None => {
            // Genesis case: no record exists yet
            // Base should have no head_commit_id and t=0
            if base.head_commit_id.is_some() {
                return Err(TransactError::CommitConflict {
                    expected_t: 0,
                    head_t: base.t(),
                });
            }
            if base.t() != 0 {
                return Err(TransactError::CommitConflict {
                    expected_t: 0,
                    head_t: base.t(),
                });
            }
            Ok(())
        }
        Some(record) => {
            if record.retracted {
                return Err(TransactError::Retracted(base.ledger_id().to_string()));
            }

            // Normal case: verify both t and previous
            if base.t() != record.commit_t {
                return Err(TransactError::CommitConflict {
                    expected_t: record.commit_t,
                    head_t: base.t(),
                });
            }

            // Verify previous commit identity matches via CID comparison.
            match (&base.head_commit_id, &record.commit_head_id) {
                // Both have CIDs: compare directly
                (Some(base_cid), Some(record_cid)) => {
                    if base_cid != record_cid {
                        return Err(TransactError::CommitIdMismatch {
                            expected: record_cid.to_string(),
                            found: base_cid.to_string(),
                        });
                    }
                }
                // Neither has CID: genesis edge case, both are at t=0 with no commits
                (None, None) => {}
                // Mixed state: one side has CID, the other doesn't
                (Some(base_cid), None) => {
                    return Err(TransactError::CommitIdMismatch {
                        expected: "None".to_string(),
                        found: base_cid.to_string(),
                    });
                }
                (None, Some(record_cid)) => {
                    return Err(TransactError::CommitIdMismatch {
                        expected: record_cid.to_string(),
                        found: "None".to_string(),
                    });
                }
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{TemplateTerm, TripleTemplate, Txn};
    use crate::stage::{stage, StageOptions};
    use fluree_db_core::{
        content_store_for, FlakeValue, LedgerSnapshot, MemoryStorage, Sid, CODEC_FLUREE_COMMIT,
    };
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_nameservice::{
        BranchLifecycle, GraphSourceLookup, GraphSourceRecord, NameServiceLookup, NsLookupResult,
        NsRecord, NsRecordSnapshot, RefPublisher,
    };
    use fluree_db_novelty::Novelty;
    use std::fmt;

    #[derive(Clone)]
    struct LosePublishRaceNameService {
        inner: MemoryNameService,
        winner_commit_id: ContentId,
    }

    impl fmt::Debug for LosePublishRaceNameService {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("LosePublishRaceNameService").finish()
        }
    }

    #[async_trait::async_trait]
    impl GraphSourceLookup for LosePublishRaceNameService {
        async fn lookup_graph_source(
            &self,
            graph_source_id: &str,
        ) -> fluree_db_nameservice::Result<Option<GraphSourceRecord>> {
            self.inner.lookup_graph_source(graph_source_id).await
        }

        async fn lookup_any(
            &self,
            resource_id: &str,
        ) -> fluree_db_nameservice::Result<NsLookupResult> {
            self.inner.lookup_any(resource_id).await
        }

        async fn all_graph_source_records(
            &self,
        ) -> fluree_db_nameservice::Result<Vec<GraphSourceRecord>> {
            self.inner.all_graph_source_records().await
        }
    }

    #[async_trait::async_trait]
    impl NameServiceLookup for LosePublishRaceNameService {
        async fn lookup(&self, ledger_id: &str) -> fluree_db_nameservice::Result<Option<NsRecord>> {
            self.inner.lookup(ledger_id).await
        }

        async fn all_records(&self) -> fluree_db_nameservice::Result<Vec<NsRecord>> {
            self.inner.all_records().await
        }
    }

    #[async_trait::async_trait]
    impl BranchLifecycle for LosePublishRaceNameService {
        async fn create_branch(
            &self,
            ledger_name: &str,
            new_branch: &str,
            source_branch: &str,
            at_commit: Option<(fluree_db_core::ContentId, i64)>,
        ) -> fluree_db_nameservice::Result<()> {
            self.inner
                .create_branch(ledger_name, new_branch, source_branch, at_commit)
                .await
        }

        async fn drop_branch(&self, ledger_id: &str) -> fluree_db_nameservice::Result<Option<u32>> {
            self.inner.drop_branch(ledger_id).await
        }

        async fn reset_head(
            &self,
            ledger_id: &str,
            snapshot: NsRecordSnapshot,
        ) -> fluree_db_nameservice::Result<()> {
            self.inner.reset_head(ledger_id, snapshot).await
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_nameservice::RefLookup for LosePublishRaceNameService {
        async fn get_ref(
            &self,
            ledger_id: &str,
            kind: RefKind,
        ) -> fluree_db_nameservice::Result<Option<RefValue>> {
            self.inner.get_ref(ledger_id, kind).await
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_nameservice::StatusLookup for LosePublishRaceNameService {
        async fn get_status(
            &self,
            ledger_id: &str,
        ) -> fluree_db_nameservice::Result<Option<fluree_db_nameservice::StatusValue>> {
            self.inner.get_status(ledger_id).await
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_nameservice::ConfigLookup for LosePublishRaceNameService {
        async fn get_config(
            &self,
            ledger_id: &str,
        ) -> fluree_db_nameservice::Result<Option<fluree_db_nameservice::ConfigValue>> {
            self.inner.get_config(ledger_id).await
        }
    }

    #[async_trait::async_trait]
    impl RefPublisher for LosePublishRaceNameService {
        async fn compare_and_set_ref(
            &self,
            ledger_id: &str,
            kind: RefKind,
            _expected: Option<&RefValue>,
            new: &RefValue,
        ) -> fluree_db_nameservice::Result<CasResult> {
            match kind {
                RefKind::CommitHead => Ok(CasResult::Conflict {
                    actual: Some(RefValue {
                        id: Some(self.winner_commit_id.clone()),
                        t: new.t,
                    }),
                }),
                RefKind::IndexHead => {
                    self.inner
                        .compare_and_set_ref(ledger_id, kind, None, new)
                        .await
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_nameservice::LedgerLifecycle for LosePublishRaceNameService {
        async fn init(&self, ledger_id: &str) -> fluree_db_nameservice::Result<()> {
            self.inner.init(ledger_id).await
        }

        async fn retract(&self, ledger_id: &str) -> fluree_db_nameservice::Result<()> {
            self.inner.retract(ledger_id).await
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_nameservice::CommitPublisher for LosePublishRaceNameService {
        async fn publish_commit(
            &self,
            ledger_id: &str,
            commit_t: i64,
            commit_id: &ContentId,
        ) -> fluree_db_nameservice::Result<()> {
            self.inner
                .publish_commit(ledger_id, commit_t, commit_id)
                .await
        }

        fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
            self.inner.publishing_ledger_id(ledger_id)
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_nameservice::IndexPublisher for LosePublishRaceNameService {
        async fn publish_index(
            &self,
            ledger_id: &str,
            index_t: i64,
            index_id: &ContentId,
        ) -> fluree_db_nameservice::Result<()> {
            self.inner.publish_index(ledger_id, index_t, index_id).await
        }
    }

    #[tokio::test]
    async fn test_commit_simple_insert() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();

        // Stage an insert
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Commit
        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let cs = content_store_for(storage.clone(), "test:main");
        let (receipt, new_state) = commit(
            view,
            ns_registry,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        assert_eq!(receipt.t, 1);
        assert_eq!(receipt.flake_count, 1);
        // commit_id is now a ContentId with CODEC_FLUREE_COMMIT
        assert_eq!(receipt.commit_id.codec(), CODEC_FLUREE_COMMIT);
        assert_eq!(new_state.t(), 1);
        assert!(new_state.head_commit_id.is_some());
    }

    #[tokio::test]
    async fn test_commit_empty_transaction() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();

        // Stage an empty transaction (no inserts)
        let txn = Txn::insert();
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Commit should fail
        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let cs = content_store_for(storage.clone(), "test:main");
        let result = commit(
            view,
            ns_registry,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await;

        assert!(matches!(result, Err(TransactError::EmptyTransaction)));
    }

    #[tokio::test]
    async fn test_commit_sequence() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let cs = content_store_for(storage.clone(), "test:main");

        // First commit
        let txn1 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view1, ns_registry1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (receipt1, state1) = commit(
            view1,
            ns_registry1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        assert_eq!(receipt1.t, 1);

        // Second commit
        let txn2 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:bob")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Bob".to_string())),
        ));

        let ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let (view2, ns_registry2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();
        let (receipt2, state2) = commit(
            view2,
            ns_registry2,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        assert_eq!(receipt2.t, 2);
        assert_eq!(state2.t(), 2);
        // Novelty includes transaction flakes + commit metadata flakes
        // 2 txn flakes + 8 metadata (commit 1, no previous) + 9 metadata (commit 2, has previous) = 19
        assert!(
            state2.novelty.len() >= 2,
            "novelty should include at least 2 transaction flakes"
        );
    }

    #[tokio::test]
    async fn test_commit_predictive_sizing() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();

        // Create a transaction with a large string value
        let big_value = "x".repeat(1000);
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:bio")),
            TemplateTerm::Value(FlakeValue::String(big_value)),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Use a very small max to trigger predictive sizing error
        let config = IndexConfig {
            reindex_min_bytes: 50,
            reindex_max_bytes: 100, // Smaller than the big flake
        };

        let cs = content_store_for(storage.clone(), "test:main");
        let result = commit(
            view,
            ns_registry,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await;

        // Should fail with NoveltyWouldExceed
        assert!(matches!(
            result,
            Err(TransactError::NoveltyWouldExceed { .. })
        ));
    }

    #[tokio::test]
    async fn test_commit_reports_publish_lost_race() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let winner_commit_id = ContentId::new(ContentKind::Commit, b"winner");
        let nameservice = LosePublishRaceNameService {
            inner: MemoryNameService::new(),
            winner_commit_id: winner_commit_id.clone(),
        };

        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let cs = content_store_for(storage.clone(), "test:main");
        let result = commit(
            view,
            ns_registry,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await;

        match result {
            Err(TransactError::PublishLostRace {
                ledger_id,
                attempted_t,
                published_t,
                published_commit_id,
                ..
            }) => {
                assert_eq!(ledger_id, "test:main");
                assert_eq!(attempted_t, 1);
                assert_eq!(published_t, 1);
                assert_eq!(published_commit_id, winner_commit_id.to_string());
            }
            other => panic!("expected PublishLostRace, got {other:?}"),
        }
    }

    /// Regression: `build_commit`'s early-return paths
    /// (`EmptyTransaction`, novelty caps, envelope-delta failure,
    /// serialize failure) all return before installing the
    /// `txn_id_for_release` machinery, so a leftover raw-txn upload
    /// would orphan its blob in CAS. The fix moves the upload await
    /// below the pre-checks AND releases on `build_commit` err.
    #[tokio::test]
    async fn empty_transaction_does_not_orphan_raw_txn() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let txn = Txn::insert();
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        let cs = content_store_for(storage.clone(), "test:main");
        let upload_cs: Arc<dyn ContentStore> =
            Arc::new(content_store_for(storage.clone(), "test:main"));
        let raw_txn = serde_json::json!({ "raw": "payload" });
        let raw_txn_bytes = serde_json::to_vec(&raw_txn).expect("serialize raw txn");
        let expected_cid = ContentId::new(ContentKind::Txn, &raw_txn_bytes);
        let opts = CommitOpts::default().with_raw_txn_spawned(upload_cs, raw_txn);

        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };

        let result = commit(view, ns_registry, &cs, &nameservice, &config, opts).await;
        assert!(
            matches!(result, Err(TransactError::EmptyTransaction)),
            "expected EmptyTransaction, got {result:?}"
        );

        // The blob the upload landed in CAS must be released.
        assert!(
            cs.get(&expected_cid).await.is_err(),
            "raw_txn blob must be released after EmptyTransaction error"
        );
    }
}
