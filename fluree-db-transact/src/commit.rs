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
use fluree_db_ledger::{HeadTemporal, IndexConfig, LedgerState, StagedLedger};
use fluree_db_nameservice::{CasResult, NameServiceLookup, RefKind, RefPublisher, RefValue};
use fluree_db_novelty::{
    generate_commit_flakes, iso_to_epoch_ms_opt, stamp_graph_on_commit_flakes,
};
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
    /// Additional parent commit IDs for merge commits.
    ///
    /// When non-empty, these are appended as extra `parents` on the
    /// commit record, producing a multi-parent (merge) commit. The primary
    /// parent is still derived from `base.head_commit_id`.
    pub merge_parents: Vec<ContentId>,
    /// ISO 8601 timestamp for the commit.
    ///
    /// When `None`, defaults to `Utc::now().to_rfc3339()` (clamped to the
    /// head commit's event time so the chain stays monotonic under clock
    /// skew). Provide a fixed value for deterministic commit hashes
    /// (testing, replay). This is the commit's *event time* — the axis
    /// `@iso:` time travel resolves against. Supplied values are validated:
    /// monotonically non-decreasing along the chain, and at most a small
    /// skew into the future.
    pub timestamp: Option<String>,
    /// ISO 8601 wall-clock time the commit was recorded (audit axis).
    ///
    /// Setting this (or committing onto a ledger whose head already carries
    /// `db:receivedAt`) puts the ledger in sticky dual-stamp mode: this and
    /// every subsequent commit records a system-controlled `db:receivedAt`
    /// txn-meta entry, and `@recorded:` time travel resolves against it.
    /// When `None` on a dual-stamp ledger, defaults to wall clock (clamped
    /// monotonic). Normal ledgers (no event-time use) never emit it.
    pub received_at: Option<String>,
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
            .field("merge_parents", &self.merge_parents.len())
            .field("timestamp", &self.timestamp)
            .field("received_at", &self.received_at)
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
            merge_parents: self.merge_parents.clone(),
            timestamp: self.timestamp.clone(),
            received_at: self.received_at.clone(),
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

    /// Set the recorded-at wall-clock timestamp (ISO 8601, audit axis).
    ///
    /// Flips the ledger into sticky dual-stamp mode. Normally left unset:
    /// the build path stamps wall clock automatically on dual-stamp
    /// ledgers. Provide a fixed value only for deterministic commit hashes
    /// (testing, replay).
    pub fn with_received_at(mut self, ts: impl Into<String>) -> Self {
        self.received_at = Some(ts.into());
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
///   `merge_parents` — populated during staging or reserved for the
///   rebase/merge paths, which carry their own request envelopes
///   when they join the queue.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CommitOptsRequest {
    pub identity: Option<String>,
    pub txn_signature: Option<TxnSignature>,
    pub txn_meta: Vec<TxnMetaEntry>,
    pub timestamp: Option<String>,
    /// Recorded-at wall-clock timestamp (audit axis, dual-stamp mode).
    #[serde(default)]
    pub received_at: Option<String>,
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
            merge_parents: Vec::new(),
            timestamp: self.timestamp,
            received_at: self.received_at,
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
            received_at: opts.received_at.clone(),
            raw_txn_id: opts.raw_txn_id.clone(),
        }
    }
}

/// Maximum allowed forward skew for a caller-supplied event time.
///
/// Commits are immutable and event time is monotonically non-decreasing, so
/// a single future-dated event time would permanently pin the ledger's
/// timeline ahead of reality. Small allowance for client/server clock drift.
const MAX_EVENT_TIME_FUTURE_SKEW_MS: i64 = 5 * 60 * 1000;

fn epoch_ms_to_rfc3339(ms: i64) -> Result<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| dt.to_rfc3339())
        .ok_or_else(|| {
            TransactError::InvalidEventTime(format!("epoch milliseconds {ms} out of range"))
        })
}

/// Resolve the commit's event time (`Commit.time`) and optional audit-axis
/// receivedAt stamp (epoch ms) from caller opts + head temporal metadata.
///
/// Event time: caller-supplied values must be valid RFC 3339, at most
/// [`MAX_EVENT_TIME_FUTURE_SKEW_MS`] in the future, and not earlier than the
/// head commit's event time. Defaults to wall clock, clamped to the head's
/// event time so the chain stays monotonic under clock skew — `@iso:`
/// resolution silently mis-resolves on a non-monotonic chain.
///
/// receivedAt: `Some` when the caller supplied one or the ledger is already
/// in dual-stamp mode (sticky — every post-flip commit must carry it for
/// `@recorded:` resolution to stay exact). Clamped monotonic likewise.
fn resolve_commit_times(
    event: Option<String>,
    received: Option<String>,
    head: Option<HeadTemporal>,
) -> Result<(String, Option<i64>)> {
    let now = Utc::now();
    let now_ms = now.timestamp_millis();
    let head_event_ms = head.map(|h| h.event_time_ms);

    let timestamp = match event {
        Some(ts) => {
            let ms = iso_to_epoch_ms_opt(&ts).ok_or_else(|| {
                TransactError::InvalidEventTime(format!("'{ts}' is not a valid RFC 3339 timestamp"))
            })?;
            if ms > now_ms + MAX_EVENT_TIME_FUTURE_SKEW_MS {
                return Err(TransactError::InvalidEventTime(format!(
                    "event time '{ts}' is in the future; commits are immutable and event \
                     time is monotonic, so a future event time would permanently pin the \
                     ledger's timeline ahead of reality"
                )));
            }
            if let Some(head_ms) = head_event_ms {
                if ms < head_ms {
                    return Err(TransactError::InvalidEventTime(format!(
                        "event time '{ts}' is earlier than the head commit's event time \
                         ({}); event time must be monotonically non-decreasing",
                        epoch_ms_to_rfc3339(head_ms)?
                    )));
                }
            }
            ts
        }
        None => match head_event_ms {
            // Clock went backwards (or the head is event-timed at/after our
            // present): reuse the head's event time so the chain stays
            // monotonic and `@iso:` resolution stays correct.
            Some(head_ms) if now_ms < head_ms => epoch_ms_to_rfc3339(head_ms)?,
            _ => now.to_rfc3339(),
        },
    };

    let dual_stamp = received.is_some() || head.is_some_and(|h| h.dual_stamp());
    let received_at_ms = if dual_stamp {
        let ms = match received {
            Some(ts) => iso_to_epoch_ms_opt(&ts).ok_or_else(|| {
                TransactError::InvalidEventTime(format!(
                    "receivedAt '{ts}' is not a valid RFC 3339 timestamp"
                ))
            })?,
            None => now_ms,
        };
        // Symmetric with the event axis: a caller-supplied receivedAt (reachable
        // from the Rust API / CLI — the HTTP route always passes `now`) must not
        // be in the future. The audit axis is immutable and monotonic, so a
        // future stamp would permanently pin the ledger's timeline ahead of
        // reality.
        if ms > now_ms + MAX_EVENT_TIME_FUTURE_SKEW_MS {
            return Err(TransactError::InvalidEventTime(format!(
                "receivedAt '{}' is in the future; the audit axis is immutable and \
                 monotonic, so a future receivedAt would permanently pin the ledger's \
                 timeline ahead of reality",
                epoch_ms_to_rfc3339(ms)?
            )));
        }
        let prev = head.and_then(|h| h.received_time_ms);
        Some(prev.map_or(ms, |p| ms.max(p)))
    } else {
        None
    };

    Ok((timestamp, received_at_ms))
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
        received_at: opt_received_at,
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

    // db:receivedAt is system-controlled like f:identity: strip any
    // caller-supplied txn-meta claim unconditionally; the resolved value
    // (if the ledger dual-stamps) is injected below.
    txn_meta.retain(|entry| {
        !(entry.predicate_ns == fluree_vocab::namespaces::FLUREE_DB
            && entry.predicate_name == fluree_vocab::db::RECEIVED_AT)
    });

    // No wrapper span: the caller's ambient span (e.g. `txn_commit`
    // from `commit()`, or whatever the Raft path opens) carries the
    // build-time fields, so sub-spans below stay direct children of
    // that ambient span.
    let commit_span = tracing::Span::current();

    // 2. Check for empty transaction — merge commits with no data flakes are
    //    valid (e.g., TakeBranch strategy drops all source flakes) because
    //    the commit still records the merge-parent relationship in the DAG.
    //    A REGISTRATION-ONLY commit is likewise valid: `CREATE GRAPH <g>`
    //    stages no flakes (D-6: an empty graph has no representation) but
    //    must persist its graph_delta so the registry learns the IRI — O3's
    //    transfer source-existence check consults exactly that registration.
    let registers_new_graph = graph_delta
        .values()
        .any(|iri| base.snapshot.graph_registry.graph_id_for_iri(iri).is_none());
    if flakes.is_empty() && merge_parents.is_empty() && !registers_new_graph {
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

    // Resolve the commit's event time (`Commit.time`, the `@iso:` axis) and
    // the optional audit-axis receivedAt stamp. Validates monotonicity and
    // future bounds against the in-memory head temporal metadata.
    let (timestamp, received_at_ms) =
        resolve_commit_times(opt_timestamp, opt_received_at, base.head_temporal)?;
    if let Some(recv_ms) = received_at_ms {
        txn_meta.push(TxnMetaEntry::new(
            fluree_vocab::namespaces::FLUREE_DB,
            fluree_vocab::db::RECEIVED_AT,
            TxnMetaValue::Long(recv_ms),
        ));
    }

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
    /// The publish is a compare-and-set against the exact head this
    /// commit was built on (`expected_head_ref`, baked in by
    /// [`build_commit`]); a head that moved between build and apply
    /// surfaces as [`TransactError::PublishLostRace`]. Callers that
    /// make the commit durable through another mechanism (the Raft
    /// `ApplyHead` path, rebase's batched replay publish) use
    /// [`Self::finalize_state`] instead.
    ///
    /// On publish failure, the raw-txn blob uploaded during
    /// [`build_commit`] is released — that CID is no longer referenced
    /// by any durable commit record.
    pub async fn apply<C, N>(
        self,
        content_store: &C,
        nameservice: &N,
    ) -> Result<(CommitReceipt, LedgerState)>
    where
        C: ContentStore + ?Sized,
        N: RefPublisher + ?Sized,
    {
        apply_commit_inner(self, content_store, nameservice).await
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
        let publish_result = nameservice
            .compare_and_set_ref(
                ledger_id_for_publish.as_str(),
                RefKind::CommitHead,
                expected_head_ref.as_ref(),
                &new_head_ref,
            )
            .instrument(tracing::debug_span!("commit_publish_nameservice"))
            .await?;
        match publish_result {
            CasResult::Updated => {}
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
    // Capture before `commit_record.flakes` is taken below; keeps the head
    // temporal metadata current so the next commit's event-time guard and
    // dual-stamp decision stay in-memory integer checks.
    let head_temporal = HeadTemporal::from_commit(&commit_record).or(base.head_temporal);

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

    // Capture the ledger id before moving novelty fields out of `base`: after a
    // partial move we can no longer call `&self` methods on `base`.
    let base_ledger_id = base.ledger_id().to_string();

    // Move the snapshot out of `base` up front so we can DETACH its range
    // provider *before* mutating the dictionaries below. A `BinaryRangeProvider`
    // owns `Arc::clone`s of `dict_novelty` / `runtime_small_dicts`; if it stays
    // attached, the per-commit `Arc::make_mut` on those dicts sees
    // `strong_count >= 2` and deep-clones the (growing) dictionaries on every
    // commit once an index has been published — an O(accumulated-novelty)
    // per-commit cost that returns right after the first reindex. Detaching
    // first restores unique ownership so `make_mut` mutates in place; the
    // provider is rebuilt with the updated dicts after mutation.
    let mut snapshot = base.snapshot;

    // Extract the BinaryIndexStore (needed both to dedup new dict entries against
    // the persisted tree and to rebuild the provider) before dropping the
    // provider; fall back to the type-erased `binary_store` on `base`.
    let had_binary_provider = snapshot
        .range_provider
        .as_ref()
        .is_some_and(|rp| rp.as_any().is::<BinaryRangeProvider>());
    let store: Option<Arc<BinaryIndexStore>> = snapshot
        .range_provider
        .as_ref()
        .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>())
        .map(|brp| Arc::clone(brp.store()))
        .or_else(|| {
            base.binary_store
                .as_ref()
                .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok())
        });

    // Detach the provider so its Arc clones of the dicts are released before the
    // `make_mut` calls (rebuilt + reattached after mutation, below).
    if snapshot.range_provider.is_some() {
        Arc::make_mut(&mut snapshot).range_provider = None;
    }

    // Move the dict Arcs out of `base` (not `Arc::clone`) so `make_mut` mutates
    // in place when this commit uniquely owns them.
    let mut dict_novelty = base.dict_novelty;
    let mut runtime_small_dicts = base.runtime_small_dicts;

    // Ownership probe: after detaching the provider these should be uniquely
    // owned (`strong_count == 1`) so the `make_mut` below mutates in place. A
    // count > 1 means another live holder (e.g. a concurrent reader's snapshot)
    // still pins the dict and `make_mut` will deep-clone it — the O(novelty)
    // post-reindex regression. Debug-level + dedicated target so it stays silent
    // unless `RUST_LOG=fluree::cow_probe=debug` is set.
    tracing::debug!(
        target: "fluree::cow_probe",
        dict_novelty_strong = Arc::strong_count(&dict_novelty),
        runtime_small_dicts_strong = Arc::strong_count(&runtime_small_dicts),
        had_binary_provider,
        "commit_txn dict ownership before make_mut"
    );

    // 10.1 Populate DictNovelty with subjects/strings from this commit.
    {
        let span = tracing::debug_span!("commit_populate_dict_novelty");
        let _g = span.enter();
        populate_dict_novelty(
            Arc::make_mut(&mut dict_novelty),
            store.as_deref(),
            &all_flakes,
        )?;
    }

    Arc::make_mut(&mut runtime_small_dicts).populate_from_flakes(&all_flakes);

    let mut new_novelty = base.novelty;
    {
        let span = tracing::debug_span!("commit_apply_to_novelty");
        let _g = span.enter();
        let mut reverse_graph = snapshot.build_reverse_graph()?;
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(&base_ledger_id);
        if let Some(g_sid) = snapshot.encode_iri(&txn_meta_iri) {
            reverse_graph.entry(g_sid).or_insert(TXN_META_GRAPH_ID);
        }
        Arc::make_mut(&mut new_novelty).apply_commit(all_flakes, new_t, &reverse_graph)?;
    }

    // Rebuild + reattach the provider with the UPDATED dicts so reads through the
    // returned state resolve newly-introduced subject/string IDs. Only when a
    // BinaryRangeProvider was attached before — preserves prior behavior that
    // genesis / no-index states keep `range_provider == None`.
    if had_binary_provider {
        if let Some(store) = store {
            let ns_fallback = Some(snapshot.shared_namespaces());
            let provider = Arc::new(BinaryRangeProvider::new(
                store,
                Arc::clone(&dict_novelty),
                Arc::clone(&runtime_small_dicts),
                ns_fallback,
            )) as Arc<dyn fluree_db_core::range_provider::RangeProvider>;
            Arc::make_mut(&mut snapshot).range_provider = Some(provider);
        }
    }

    let new_state = LedgerState {
        snapshot,
        novelty: new_novelty,
        dict_novelty,
        // Carry the hierarchy cache across commits — the schema epoch check
        // invalidates it only when a commit touched subClassOf/subPropertyOf.
        schema_hierarchy_cache: base.schema_hierarchy_cache,
        shacl_compile_cache: base.shacl_compile_cache,
        runtime_small_dicts,
        head_commit_id: Some(commit_cid.clone()),
        head_index_id: base.head_index_id,
        ns_record: base.ns_record,
        binary_store: base.binary_store,
        spatial_indexes: base.spatial_indexes,
        head_temporal,
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
        let current = nameservice
            .lookup(view.base().ledger_id())
            .instrument(tracing::debug_span!("commit_nameservice_lookup"))
            .await?;
        {
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
        staged.apply(content_store, nameservice).await
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

    // =========================================================================
    // resolve_commit_times: event-time validation + dual-stamp resolution
    // =========================================================================

    fn head(event_ms: i64, received_ms: Option<i64>) -> Option<HeadTemporal> {
        Some(HeadTemporal {
            event_time_ms: event_ms,
            received_time_ms: received_ms,
        })
    }

    #[test]
    fn resolve_times_default_is_wall_clock_no_dual_stamp() {
        let (ts, recv) = resolve_commit_times(None, None, None).unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(&ts).is_ok());
        assert!(recv.is_none(), "plain commit must not dual-stamp");
    }

    #[test]
    fn resolve_times_clamps_clock_backwards_to_head() {
        // Head event time is one hour in the future (clock skew / backdated
        // present): the default stamp must reuse it, not go backwards.
        let future_ms = Utc::now().timestamp_millis() + 3_600_000;
        let (ts, _) = resolve_commit_times(None, None, head(future_ms, None)).unwrap();
        let ms = iso_to_epoch_ms_opt(&ts).unwrap();
        assert_eq!(ms, future_ms, "default stamp must clamp to head event time");
    }

    #[test]
    fn resolve_times_rejects_event_time_before_head() {
        let now_ms = Utc::now().timestamp_millis();
        let err = resolve_commit_times(
            Some("2020-01-01T00:00:00Z".into()),
            None,
            head(now_ms, None),
        )
        .unwrap_err();
        assert!(err.to_string().contains("monotonically non-decreasing"));
    }

    #[test]
    fn resolve_times_rejects_future_event_time() {
        let tomorrow = (Utc::now() + chrono::Duration::days(1)).to_rfc3339();
        let err = resolve_commit_times(Some(tomorrow), None, None).unwrap_err();
        assert!(err.to_string().contains("future"));
    }

    #[test]
    fn resolve_times_rejects_unparseable_event_time() {
        let err = resolve_commit_times(Some("garbage".into()), None, None).unwrap_err();
        assert!(err.to_string().contains("RFC 3339"));
    }

    #[test]
    fn resolve_times_supplied_past_event_time_ok_on_fresh_ledger() {
        let (ts, recv) =
            resolve_commit_times(Some("1969-07-20T20:17:00Z".into()), None, None).unwrap();
        // Pre-1970 (negative epoch ms) is fine — historical modeling.
        assert_eq!(ts, "1969-07-20T20:17:00Z");
        assert!(recv.is_none(), "timestamp alone must not flip dual-stamp");
    }

    #[test]
    fn resolve_times_explicit_received_flips_dual_stamp() {
        let (_, recv) = resolve_commit_times(
            Some("2020-01-01T00:00:00Z".into()),
            Some("2026-01-01T00:00:00Z".into()),
            None,
        )
        .unwrap();
        assert_eq!(recv, iso_to_epoch_ms_opt("2026-01-01T00:00:00Z"));
    }

    #[test]
    fn resolve_times_sticky_dual_stamp_continues_without_opts() {
        let now_ms = Utc::now().timestamp_millis();
        let (_, recv) =
            resolve_commit_times(None, None, head(now_ms - 1000, Some(now_ms - 1000))).unwrap();
        let recv = recv.expect("dual-stamp ledger must keep stamping receivedAt");
        assert!(
            recv >= now_ms - 1000,
            "receivedAt must be clamped monotonic"
        );
    }

    #[test]
    fn resolve_times_rejects_future_received_at() {
        // Symmetric with the event axis: a caller-supplied receivedAt in the
        // future is rejected so the immutable audit axis can't be pinned ahead.
        let tomorrow = (Utc::now() + chrono::Duration::days(1)).to_rfc3339();
        let err = resolve_commit_times(Some("2020-01-01T00:00:00Z".into()), Some(tomorrow), None)
            .unwrap_err();
        assert!(err.to_string().contains("future"));
    }

    #[test]
    fn resolve_times_received_clamped_to_head_received() {
        // Head recorded one hour ahead (clock skew): the new receivedAt must
        // not go backwards, or @recorded: resolution breaks.
        let future_ms = Utc::now().timestamp_millis() + 3_600_000;
        let (_, recv) =
            resolve_commit_times(None, None, head(future_ms - 1, Some(future_ms))).unwrap();
        assert_eq!(recv, Some(future_ms));
    }
}
