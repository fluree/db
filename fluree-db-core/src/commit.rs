//! Commit type and DAG-aware commit traversal.
//!
//! This module provides the [`Commit`] type representing a single transaction,
//! [`CommitEnvelope`] for lightweight metadata-only access, and streaming
//! utilities for walking commit history backwards from a HEAD.
//!
//! Commits form a directed acyclic graph (DAG): normal commits have one
//! parent, merge commits have two or more. The traversal functions use
//! BFS with a visited set to handle the DAG correctly, yielding each
//! commit exactly once in reverse-topological order (highest `t` first).
//!
//! The `stop_at_t` parameter stops traversal when all remaining commits
//! have `t <= stop_at_t`, which is typically the index `t` — commits at
//! or below that point are already captured in the index.

pub mod codec;

use crate::error::{Error, Result};
use crate::{CommitId, ContentId, ContentStore, Flake};
use codec::format::CommitSignature;
use futures::stream::{self, Stream};
use serde::Serialize;
use std::collections::HashMap;

/// Transaction signature — audit record of who submitted a transaction.
///
/// The raw signed transaction (JWS/VC) is stored separately via content-addressed
/// storage. The `txn_id` provides a content-addressed reference to retrieve and
/// re-verify the original signed transaction.
#[derive(Clone, Debug)]
pub struct TxnSignature {
    /// Verified signer identity (did:key:z6Mk...)
    pub signer: String,
    /// Content-addressed transaction ID.
    /// References the original signed transaction stored in CAS.
    pub txn_id: Option<String>,
}

// =============================================================================
// Transaction Metadata Types
// =============================================================================

/// Maximum number of txn-meta entries per transaction.
pub const MAX_TXN_META_ENTRIES: usize = 256;

/// Maximum encoded size of txn-meta in bytes (64KB).
pub const MAX_TXN_META_BYTES: usize = 65536;

/// A predicate/object pair for user-provided transaction metadata.
///
/// Uses ns_code + name (like Sid) for compact encoding and resolver compatibility.
/// The subject is implicit — always the commit itself.
///
/// Stored in the commit envelope for replay-safe persistence, then emitted to
/// the txn-meta graph (`g_id=1`) during indexing.
#[derive(Clone, Debug, PartialEq)]
pub struct TxnMetaEntry {
    /// Predicate namespace code
    pub predicate_ns: u16,
    /// Predicate local name
    pub predicate_name: String,
    /// Object value
    pub value: TxnMetaValue,
}

impl TxnMetaEntry {
    /// Create a new txn-meta entry
    pub fn new(predicate_ns: u16, predicate_name: impl Into<String>, value: TxnMetaValue) -> Self {
        Self {
            predicate_ns,
            predicate_name: predicate_name.into(),
            value,
        }
    }
}

/// Object value for a transaction metadata entry.
///
/// Supports the same value types as normal RDF literals and references,
/// using ns_code + name for compact encoding.
#[derive(Clone, Debug, PartialEq)]
pub enum TxnMetaValue {
    /// Plain string literal (xsd:string)
    String(String),

    /// Typed literal with explicit datatype (ns_code + name)
    TypedLiteral {
        value: String,
        dt_ns: u16,
        dt_name: String,
    },

    /// Language-tagged string (rdf:langString)
    LangString { value: String, lang: String },

    /// IRI reference (ns_code + name)
    Ref { ns: u16, name: String },

    /// Integer value (xsd:long)
    Long(i64),

    /// Double value (xsd:double)
    ///
    /// Must be finite — NaN, +Inf, -Inf are rejected at parse time.
    Double(f64),

    /// Boolean value (xsd:boolean)
    Boolean(bool),
}

impl TxnMetaValue {
    /// Create a string value
    pub fn string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }

    /// Create an integer value
    pub fn long(n: i64) -> Self {
        Self::Long(n)
    }

    /// Create a boolean value
    pub fn boolean(b: bool) -> Self {
        Self::Boolean(b)
    }

    /// Create an IRI reference value
    pub fn reference(ns: u16, name: impl Into<String>) -> Self {
        Self::Ref {
            ns,
            name: name.into(),
        }
    }

    /// Create a language-tagged string
    pub fn lang_string(value: impl Into<String>, lang: impl Into<String>) -> Self {
        Self::LangString {
            value: value.into(),
            lang: lang.into(),
        }
    }

    /// Create a typed literal
    pub fn typed_literal(value: impl Into<String>, dt_ns: u16, dt_name: impl Into<String>) -> Self {
        Self::TypedLiteral {
            value: value.into(),
            dt_ns,
            dt_name: dt_name.into(),
        }
    }

    /// Create a double value, returning None if not finite
    pub fn double(n: f64) -> Option<Self> {
        if n.is_finite() {
            Some(Self::Double(n))
        } else {
            None
        }
    }
}

/// A commit represents a single transaction in the ledger
#[derive(Clone, Debug)]
pub struct Commit {
    /// Content identifier (CIDv1). `None` before the commit is serialized
    /// and hashed; `Some(cid)` after hashing or when loaded from storage.
    pub id: Option<ContentId>,

    /// Transaction time (monotonically increasing)
    pub t: i64,

    /// ISO 8601 timestamp of when the commit was created
    pub time: Option<String>,

    /// Flakes in this commit (assertions and retractions)
    pub flakes: Vec<Flake>,

    /// Parent commit references (CID-based).
    /// Empty for genesis, one element for normal commits, two+ for merge commits.
    pub parents: Vec<CommitId>,

    /// Transaction blob CID (content-addressed reference to original txn JSON).
    /// When present, the raw transaction JSON can be loaded from this CID.
    pub txn: Option<ContentId>,

    /// New namespace codes introduced by this commit (code → prefix)
    ///
    /// When transactions introduce new IRIs with prefixes not yet in the
    /// database's namespace table, new codes are allocated and recorded here.
    /// This allows ledger loading to apply namespace updates from commit history.
    pub namespace_delta: HashMap<u16, String>,

    /// Transaction signature (audit metadata: who submitted the transaction)
    pub txn_signature: Option<TxnSignature>,

    /// Commit signatures (cryptographic proof of which node(s) wrote this commit)
    pub commit_signatures: Vec<CommitSignature>,

    /// User-provided transaction metadata (replay-safe).
    ///
    /// Stored in the commit envelope and emitted to the txn-meta graph (`g_id=1`)
    /// during indexing. Each entry becomes a triple with the commit subject.
    pub txn_meta: Vec<TxnMetaEntry>,

    /// Named graph IRI to g_id mappings introduced by this commit.
    ///
    /// When a transaction references named graphs (via TriG GRAPH blocks or
    /// JSON-LD with graph IRIs), this map stores the g_id assignment for each
    /// graph IRI introduced in this commit. This is necessary for:
    ///
    /// 1. **Replay safety**: Commits must be self-contained so that replaying
    ///    the commit chain produces the same g_id assignments.
    /// 2. **Index independence**: g_id assignments in the commit do not depend
    ///    on the current index state, so re-indexing is deterministic.
    ///
    /// Reserved g_ids:
    /// - `0`: default graph
    /// - `1`: txn-meta graph (`#txn-meta`)
    /// - `2+`: user-defined named graphs
    pub graph_delta: HashMap<u16, String>,

    /// Ledger-fixed split mode for canonical IRI encoding.
    /// Set in the genesis commit; absent in subsequent commits.
    pub ns_split_mode: Option<crate::ns_encoding::NsSplitMode>,
}

impl Commit {
    /// Create a new commit (id is set to `None` until serialized and hashed)
    pub fn new(t: i64, flakes: Vec<Flake>) -> Self {
        Self {
            id: None,
            t,
            time: None,
            flakes,
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::new(),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        }
    }

    /// Set the content identifier
    pub fn with_id(mut self, id: ContentId) -> Self {
        self.id = Some(id);
        self
    }

    /// Set the commit timestamp (ISO 8601)
    pub fn with_time(mut self, time: impl Into<String>) -> Self {
        self.time = Some(time.into());
        self
    }

    /// Add a parent commit reference.
    ///
    /// For normal commits, call once. For merge commits, call multiple times
    /// or use [`with_merge_parents`](Self::with_merge_parents).
    pub fn with_parent(mut self, parent: CommitId) -> Self {
        self.parents.push(parent);
        self
    }

    /// Set all parent commit references at once (for merge commits).
    pub fn with_merge_parents(mut self, refs: Vec<CommitId>) -> Self {
        self.parents = refs;
        self
    }

    /// Set the transaction CID
    pub fn with_txn(mut self, txn_id: ContentId) -> Self {
        self.txn = Some(txn_id);
        self
    }

    /// Set the namespace delta (new namespace codes introduced by this commit)
    pub fn with_namespace_delta(mut self, delta: HashMap<u16, String>) -> Self {
        self.namespace_delta = delta;
        self
    }

    /// Set the transaction signature (audit metadata)
    pub fn with_txn_signature(mut self, sig: TxnSignature) -> Self {
        self.txn_signature = Some(sig);
        self
    }

    /// Set the user-provided transaction metadata
    pub fn with_txn_meta(mut self, txn_meta: Vec<TxnMetaEntry>) -> Self {
        self.txn_meta = txn_meta;
        self
    }

    /// Iterate over all parent commit CIDs.
    pub fn parent_ids(&self) -> impl Iterator<Item = &ContentId> {
        self.parents.iter()
    }
}

// =============================================================================
// Loading
// =============================================================================

/// Load a single commit from a content store by CID.
pub async fn load_commit_by_id<C: ContentStore + ?Sized>(
    store: &C,
    id: &ContentId,
) -> Result<Commit> {
    let data = store
        .get(id)
        .await
        .map_err(|e| Error::storage(format!("Failed to read commit {id}: {e}")))?;

    let _span = tracing::debug_span!("load_commit", blob_bytes = data.len()).entered();
    let mut commit = codec::read_commit(&data).map_err(|e| Error::invalid_commit(e.to_string()))?;

    // Set the commit's id from the CID we used to load it
    commit.id = Some(id.clone());

    Ok(commit)
}

// =============================================================================
// CommitEnvelope - Lightweight commit metadata without flakes
// =============================================================================

/// Lightweight commit metadata without flakes
///
/// Used for scanning commit history without loading all flake data into memory.
/// This enables memory-bounded batched reindex by allowing a metadata-only
/// backwards scan before forward flake processing.
///
/// Decoded from the binary envelope section of a commit blob (v2 or v3).
#[derive(Clone, Debug)]
pub struct CommitEnvelope {
    /// Transaction time (monotonically increasing)
    pub t: i64,

    /// Parent commit references (CID-based).
    /// Empty for genesis, one element for normal commits, two+ for merge commits.
    pub parents: Vec<CommitId>,

    /// Transaction blob CID (content-addressed reference to original txn JSON)
    pub txn: Option<ContentId>,

    /// New namespace codes introduced by this commit (code → prefix)
    pub namespace_delta: HashMap<u16, String>,

    /// User-provided transaction metadata (replay-safe)
    pub txn_meta: Vec<TxnMetaEntry>,

    /// Ledger-fixed split mode for canonical IRI encoding.
    /// Set once in the genesis commit; absent in subsequent commits.
    pub ns_split_mode: Option<crate::ns_encoding::NsSplitMode>,
}

impl CommitEnvelope {
    /// Iterate over all parent commit CIDs.
    pub fn parent_ids(&self) -> impl Iterator<Item = &ContentId> {
        self.parents.iter()
    }
}

/// Range-fetch probe size for [`load_commit_envelope_by_id`].
///
/// Sized to comfortably hold a commit envelope including the maximum
/// [`MAX_TXN_META_BYTES`] (64 KiB) plus framing, so envelope decode almost
/// always completes in a single byte-range request. On backends without a
/// native `get_range` implementation, the probe fetches the full blob via the
/// trait-default fallback — still a single round trip.
const ENVELOPE_PROBE_LEN: u64 = 128 * 1024;

/// Load a commit envelope (metadata only, no flakes) from a content store by CID.
///
/// Uses [`ContentStore::get_range`] to fetch an envelope-sized window instead
/// of the full commit blob. On remote storage (S3) this drops per-envelope
/// bytes from multi-MB (whole blob: header + envelope + ops + dicts + footer)
/// to ~128 KiB, which is typically one TCP RTT on range-capable backends.
///
/// On backends without native range support, the trait-default falls back to
/// `get` + slice — correct but no bandwidth win.
///
/// Falls back to a full `get` in the rare case that the envelope length
/// declared in the header exceeds [`ENVELOPE_PROBE_LEN`].
///
/// More memory-efficient than [`load_commit_by_id`] when you only need
/// metadata for scanning.
pub async fn load_commit_envelope_by_id<C: ContentStore + ?Sized>(
    store: &C,
    id: &ContentId,
) -> Result<CommitEnvelope> {
    use codec::format::{CommitHeader, HEADER_LEN};

    let probe = store
        .get_range(id, 0..ENVELOPE_PROBE_LEN)
        .await
        .map_err(|e| Error::storage(format!("Failed to read commit envelope {id}: {e}")))?;

    if probe.len() < HEADER_LEN {
        return Err(Error::invalid_commit(format!(
            "commit envelope {}: truncated header ({} bytes)",
            id,
            probe.len()
        )));
    }

    let header =
        CommitHeader::read_from(&probe).map_err(|e| Error::invalid_commit(e.to_string()))?;
    let needed = HEADER_LEN + header.envelope_len as usize;

    let data = if needed <= probe.len() {
        probe
    } else {
        // Oversized envelope — rare path. Fetch the full blob.
        store.get(id).await.map_err(|e| {
            Error::storage(format!(
                "Failed to read oversized commit envelope {id}: {e}"
            ))
        })?
    };

    // Sync decode — span guard lives only for the non-await block so callers
    // remain Send-clean (EnteredSpan is !Send).
    let envelope = {
        let _span =
            tracing::debug_span!("load_commit_envelope_by_id", blob_bytes = data.len()).entered();
        codec::read_commit_envelope(&data).map_err(|e| Error::invalid_commit(e.to_string()))?
    };
    Ok(envelope)
}

/// Walk a commit DAG from a head CID, collecting `(t, ContentId)` pairs for
/// all commits with `t > stop_at_t`, sorted by `t` descending.
///
/// Each commit is visited exactly once. This is the building block for
/// [`trace_commit_envelopes_by_id`] and [`trace_commits_by_id`].
pub async fn collect_dag_cids<C: ContentStore + ?Sized>(
    store: &C,
    head_id: &ContentId,
    stop_at_t: i64,
) -> Result<Vec<(i64, ContentId)>> {
    let (cids, _split_mode) = walk_dag(store, head_id, stop_at_t, false).await?;
    Ok(cids)
}

/// Walk a commit DAG like [`collect_dag_cids`], and also return the
/// authoritative `NsSplitMode` for the chain.
///
/// `NsSplitMode` is encoded only on the genesis commit; the returned value
/// is taken from the genesis-most envelope observed during the walk. This
/// lets the rebuild pipeline capture split-mode in the same pass that
/// discovers parents, avoiding a second fetch-per-commit over the chain.
///
/// Callers that don't need `NsSplitMode` should use [`collect_dag_cids`].
pub async fn collect_dag_cids_with_split_mode<C: ContentStore + ?Sized>(
    store: &C,
    head_id: &ContentId,
    stop_at_t: i64,
) -> Result<(Vec<(i64, ContentId)>, crate::ns_encoding::NsSplitMode)> {
    walk_dag(store, head_id, stop_at_t, true).await
}

/// Shared DAG walk implementation backing [`collect_dag_cids`] and
/// [`collect_dag_cids_with_split_mode`]. Envelope fetches use
/// [`load_commit_envelope_by_id`] which issues byte-range requests.
///
/// `capture_split_mode=false` skips the NsSplitMode accumulator — the caller
/// only wants the CID list.
async fn walk_dag<C: ContentStore + ?Sized>(
    store: &C,
    head_id: &ContentId,
    stop_at_t: i64,
    capture_split_mode: bool,
) -> Result<(Vec<(i64, ContentId)>, crate::ns_encoding::NsSplitMode)> {
    let mut result = Vec::new();
    let mut frontier = vec![head_id.clone()];
    let mut visited = std::collections::HashSet::new();
    let mut split_mode = crate::ns_encoding::NsSplitMode::default();

    while let Some(cid) = frontier.pop() {
        if !visited.insert(cid.clone()) {
            continue;
        }
        let envelope = load_commit_envelope_by_id(store, &cid).await?;
        if envelope.t <= stop_at_t {
            continue;
        }
        if capture_split_mode {
            if let Some(mode) = envelope.ns_split_mode {
                split_mode = mode;
            }
        }
        for parent_id in envelope.parent_ids() {
            frontier.push(parent_id.clone());
        }
        result.push((envelope.t, cid));
    }

    // Sort by t descending (highest first = reverse-topological order).
    result.sort_by_key(|b| std::cmp::Reverse(b.0));
    Ok((result, split_mode))
}

/// Stream commit envelopes from head backwards in reverse-topological order.
///
/// Walks the commit DAG, yielding `(ContentId, CommitEnvelope)` pairs ordered
/// by descending `t`. Each commit is yielded exactly once. Handles merge
/// commits with multiple parents.
///
/// Note: each envelope is loaded twice — once during `collect_dag_cids` to
/// discover parents and `t` for ordering, and once when yielded. Both loads
/// go through [`load_commit_envelope_by_id`] which issues byte-range requests
/// (typically ~128 KiB per envelope on range-capable backends), so the
/// bandwidth cost is small relative to a full-blob fetch, but two round trips
/// per commit remain. Acceptable for trace/debug callers; hot paths should
/// prefer a single-pass design.
pub fn trace_commit_envelopes_by_id<C: ContentStore + Clone + 'static>(
    store: C,
    head_id: ContentId,
    stop_at_t: i64,
) -> impl Stream<Item = Result<(ContentId, CommitEnvelope)>> {
    // Collect all CIDs first, then stream them.
    stream::unfold(
        None::<std::result::Result<std::vec::IntoIter<(i64, ContentId)>, ()>>,
        move |state| {
            let store = store.clone();
            let head_id = head_id.clone();
            async move {
                let mut iter = match state {
                    Some(Ok(iter)) => iter,
                    Some(Err(())) => return None,
                    None => {
                        // First call: walk the DAG and collect all CIDs.
                        match collect_dag_cids(&store, &head_id, stop_at_t).await {
                            Ok(cids) => cids.into_iter(),
                            Err(e) => return Some((Err(e), Some(Err(())))),
                        }
                    }
                };

                // Yield next envelope.
                let (_t, cid) = iter.next()?;
                match load_commit_envelope_by_id(&store, &cid).await {
                    Ok(env) => Some((Ok((cid, env)), Some(Ok(iter)))),
                    Err(e) => Some((Err(e), Some(Err(())))),
                }
            }
        },
    )
}

/// Stream commits from head backwards in reverse-topological order.
///
/// Walks the commit DAG, yielding full [`Commit`] values ordered by
/// descending `t`. Each commit is yielded exactly once. Handles merge
/// commits with multiple parents.
pub fn trace_commits_by_id<C: ContentStore + Clone + 'static>(
    store: C,
    head_id: ContentId,
    stop_at_t: i64,
) -> impl Stream<Item = Result<Commit>> {
    stream::unfold(
        None::<std::result::Result<std::vec::IntoIter<(i64, ContentId)>, ()>>,
        move |state| {
            let store = store.clone();
            let head_id = head_id.clone();
            async move {
                let mut iter = match state {
                    Some(Ok(iter)) => iter,
                    Some(Err(())) => return None,
                    None => match collect_dag_cids(&store, &head_id, stop_at_t).await {
                        Ok(cids) => cids.into_iter(),
                        Err(e) => return Some((Err(e), Some(Err(())))),
                    },
                };

                let (_t, cid) = iter.next()?;
                match load_commit_by_id(&store, &cid).await {
                    Ok(commit) => Some((Ok(commit), Some(Ok(iter)))),
                    Err(e) => Some((Err(e), Some(Err(())))),
                }
            }
        },
    )
}

// =============================================================================
// Common Ancestor
// =============================================================================

/// The most recent common ancestor between two commit chains.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommonAncestor {
    /// CID of the common commit.
    pub commit_id: ContentId,
    /// Transaction time of the common commit.
    pub t: i64,
}

/// Find the most recent common ancestor of two commit DAGs.
///
/// Uses dual-frontier BFS: expands both frontiers by following all parents,
/// and stops when a CID visited from one side appears in the other's visited
/// set. Handles merge commits with multiple parents correctly.
///
/// Returns an error if the DAGs share no common ancestor (should not happen
/// for branches within the same ledger, since they share a genesis commit).
pub async fn find_common_ancestor<C: ContentStore>(
    store: &C,
    head_a: &ContentId,
    head_b: &ContentId,
) -> Result<CommonAncestor> {
    if head_a == head_b {
        let envelope = load_commit_envelope_by_id(store, head_a).await?;
        return Ok(CommonAncestor {
            commit_id: head_a.clone(),
            t: envelope.t,
        });
    }

    let mut visited_a = std::collections::HashSet::new();
    let mut visited_b = std::collections::HashSet::new();
    // Frontier entries: (t, cid). We advance whichever frontier has the higher max-t.
    let mut frontier_a = Vec::new();
    let mut frontier_b = Vec::new();

    // Seed both frontiers.
    let env_a = load_commit_envelope_by_id(store, head_a).await?;
    visited_a.insert(head_a.clone());
    frontier_a.push((env_a.t, head_a.clone()));

    let env_b = load_commit_envelope_by_id(store, head_b).await?;
    visited_b.insert(head_b.clone());
    frontier_b.push((env_b.t, head_b.clone()));

    // Check initial overlap.
    if visited_a.contains(head_b) {
        return Ok(CommonAncestor {
            commit_id: head_b.clone(),
            t: env_b.t,
        });
    }
    if visited_b.contains(head_a) {
        return Ok(CommonAncestor {
            commit_id: head_a.clone(),
            t: env_a.t,
        });
    }

    loop {
        let max_a = frontier_a.iter().map(|(t, _)| *t).max();
        let max_b = frontier_b.iter().map(|(t, _)| *t).max();

        match (max_a, max_b) {
            (None, None) => {
                return Err(Error::invalid_commit(
                    "commit chains have no common ancestor".to_string(),
                ));
            }
            (Some(ta), Some(tb)) if ta >= tb => {
                // Advance frontier A: pop highest-t entry, expand its parents.
                if let Some(ancestor) =
                    advance_frontier(store, &mut frontier_a, &mut visited_a, &visited_b).await?
                {
                    return Ok(ancestor);
                }
            }
            _ => {
                // Advance frontier B.
                if let Some(ancestor) =
                    advance_frontier(store, &mut frontier_b, &mut visited_b, &visited_a).await?
                {
                    return Ok(ancestor);
                }
            }
        }
    }
}

/// Pop the highest-t entry from a frontier, load its parents, and check if
/// any newly-visited CID appears in the other side's visited set.
async fn advance_frontier<C: ContentStore>(
    store: &C,
    frontier: &mut Vec<(i64, ContentId)>,
    visited: &mut std::collections::HashSet<ContentId>,
    other_visited: &std::collections::HashSet<ContentId>,
) -> Result<Option<CommonAncestor>> {
    if frontier.is_empty() {
        return Ok(None);
    }
    // Find and remove the highest-t entry.
    let max_idx = frontier
        .iter()
        .enumerate()
        .max_by_key(|(_, (t, _))| *t)
        .map(|(i, _)| i)
        .unwrap();
    let (_t, cid) = frontier.swap_remove(max_idx);

    let envelope = load_commit_envelope_by_id(store, &cid).await?;
    for parent_id in envelope.parent_ids() {
        if visited.insert(parent_id.clone()) {
            let parent_env = load_commit_envelope_by_id(store, parent_id).await?;
            if other_visited.contains(parent_id) {
                return Ok(Some(CommonAncestor {
                    commit_id: parent_id.clone(),
                    t: parent_env.t,
                }));
            }
            frontier.push((parent_env.t, parent_id.clone()));
        }
    }
    Ok(None)
}

// =============================================================================
// CommitSummary - lightweight per-commit info for diff/log views
// =============================================================================

/// Per-commit summary suitable for diff/log views.
///
/// Built by counting [`Flake`] ops on a loaded [`Commit`]. The optional
/// `message` is extracted from `txn_meta` when an entry with predicate
/// `f:message` (namespace `FLUREE_DB`, local name `"message"`) is present
/// and its value is a plain string. Other conventions are not recognized
/// today.
#[derive(Clone, Debug, Serialize)]
pub struct CommitSummary {
    pub t: i64,
    pub commit_id: ContentId,
    /// ISO 8601 from [`Commit::time`]. `None` for legacy commits without a timestamp.
    pub time: Option<String>,
    pub asserts: usize,
    pub retracts: usize,
    pub flake_count: usize,
    /// Extracted from [`Commit::txn_meta`] when an `f:message` entry with a
    /// string value is present. Often `None`.
    pub message: Option<String>,
}

/// Build a [`CommitSummary`] from a fully-loaded [`Commit`].
///
/// Pure function — no I/O. The `commit.id` must be `Some` (it always is for
/// commits loaded via [`load_commit_by_id`]).
pub fn commit_to_summary(commit: &Commit) -> CommitSummary {
    let commit_id = commit
        .id
        .clone()
        .expect("commit_to_summary requires a Commit with id set (loaded via load_commit_by_id)");

    let mut asserts = 0usize;
    let mut retracts = 0usize;
    for f in &commit.flakes {
        if f.op {
            asserts += 1;
        } else {
            retracts += 1;
        }
    }

    let message = commit.txn_meta.iter().find_map(|entry| {
        if entry.predicate_ns == fluree_vocab::namespaces::FLUREE_DB
            && entry.predicate_name == "message"
        {
            if let TxnMetaValue::String(s) = &entry.value {
                return Some(s.clone());
            }
        }
        None
    });

    CommitSummary {
        t: commit.t,
        commit_id,
        time: commit.time.clone(),
        asserts,
        retracts,
        flake_count: commit.flakes.len(),
        message,
    }
}

/// Walk commits from `head` back to `stop_at_t` (exclusive), summarising each.
///
/// Returns `(summaries, total_count)`. `summaries` is newest-first (descending
/// `t`) and capped by `max`. `total_count` always reflects the full divergence
/// regardless of cap; truncation is implied by `summaries.len() < total_count`.
///
/// Reuses [`collect_dag_cids`] (one byte-range envelope read per commit in
/// the divergence) plus one full [`load_commit_by_id`] per summary returned.
pub async fn walk_commit_summaries<C: ContentStore>(
    store: &C,
    head: &ContentId,
    stop_at_t: i64,
    max: Option<usize>,
) -> Result<(Vec<CommitSummary>, usize)> {
    let dag = collect_dag_cids(store, head, stop_at_t).await?;
    let total = dag.len();
    let take_n = max.map_or(total, |cap| cap.min(total));
    let mut summaries = Vec::with_capacity(take_n);
    for (_t, cid) in dag.iter().take(take_n) {
        let commit = load_commit_by_id(store, cid).await?;
        summaries.push(commit_to_summary(&commit));
    }
    Ok((summaries, total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContentKind, Flake, FlakeValue, MemoryContentStore, Sid};

    fn make_test_content_id(kind: ContentKind, label: &str) -> ContentId {
        ContentId::new(kind, label.as_bytes())
    }

    fn make_test_flake(s: i64, p: i64, o: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s as u16, format!("s{s}")),
            Sid::new(p as u16, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[tokio::test]
    async fn test_commit_creation() {
        let flakes = vec![make_test_flake(1, 2, 42, 1)];
        let commit = Commit::new(1, flakes);

        assert_eq!(commit.t, 1);
        assert_eq!(commit.flakes.len(), 1);
        assert!(commit.parents.is_empty());
        assert!(commit.id.is_none());
    }

    #[tokio::test]
    async fn test_commit_chain() {
        let id1 = make_test_content_id(ContentKind::Commit, "commit-1");
        let id2 = make_test_content_id(ContentKind::Commit, "commit-2");

        let commit1 = Commit::new(1, vec![]);
        let commit2 = Commit::new(2, vec![]).with_parent(id1.clone());
        let commit3 = Commit::new(3, vec![]).with_parent(id2.clone());

        assert_eq!(commit1.parent_ids().next(), None);
        assert_eq!(commit2.parent_ids().next(), Some(&id1));
        assert_eq!(commit3.parent_ids().next(), Some(&id2));
    }

    // =========================================================================
    // CommitEnvelope tests
    // =========================================================================

    #[test]
    fn test_commit_envelope_fields() {
        let prev_id = make_test_content_id(ContentKind::Commit, "commit-0");
        let envelope = CommitEnvelope {
            t: 5,
            parents: vec![prev_id.clone()],
            txn: None,
            namespace_delta: HashMap::from([(100, "ex:".to_string())]),
            txn_meta: Vec::new(),
            ns_split_mode: None,
        };

        assert_eq!(envelope.t, 5);
        assert_eq!(envelope.parent_ids().next(), Some(&prev_id));
        assert_eq!(envelope.namespace_delta.get(&100), Some(&"ex:".to_string()));
    }

    // =========================================================================
    // TxnMetaEntry / TxnMetaValue tests
    // =========================================================================

    #[test]
    fn test_txn_meta_entry_creation() {
        let entry = TxnMetaEntry::new(100, "machine", TxnMetaValue::string("10.2.3.4"));
        assert_eq!(entry.predicate_ns, 100);
        assert_eq!(entry.predicate_name, "machine");
        assert_eq!(entry.value, TxnMetaValue::String("10.2.3.4".to_string()));
    }

    #[test]
    fn test_txn_meta_value_constructors() {
        assert_eq!(
            TxnMetaValue::string("hello"),
            TxnMetaValue::String("hello".to_string())
        );
        assert_eq!(TxnMetaValue::long(42), TxnMetaValue::Long(42));
        assert_eq!(TxnMetaValue::boolean(true), TxnMetaValue::Boolean(true));
        assert_eq!(
            TxnMetaValue::reference(50, "Alice"),
            TxnMetaValue::Ref {
                ns: 50,
                name: "Alice".to_string()
            }
        );
        assert_eq!(
            TxnMetaValue::lang_string("hello", "en"),
            TxnMetaValue::LangString {
                value: "hello".to_string(),
                lang: "en".to_string()
            }
        );
        assert_eq!(
            TxnMetaValue::typed_literal("2025-01-01", 2, "date"),
            TxnMetaValue::TypedLiteral {
                value: "2025-01-01".to_string(),
                dt_ns: 2,
                dt_name: "date".to_string()
            }
        );
    }

    #[test]
    fn test_txn_meta_value_double_finite() {
        assert_eq!(TxnMetaValue::double(2.72), Some(TxnMetaValue::Double(2.72)));
        assert_eq!(TxnMetaValue::double(-0.5), Some(TxnMetaValue::Double(-0.5)));
    }

    #[test]
    fn test_txn_meta_value_double_non_finite() {
        assert_eq!(TxnMetaValue::double(f64::NAN), None);
        assert_eq!(TxnMetaValue::double(f64::INFINITY), None);
        assert_eq!(TxnMetaValue::double(f64::NEG_INFINITY), None);
    }

    // =========================================================================
    // trace_commits_by_id tests
    // =========================================================================

    /// Helper: serialize a commit to binary, store in a MemoryContentStore,
    /// and return the CID.
    #[cfg(feature = "credential")]
    async fn store_commit(store: &MemoryContentStore, commit: &Commit) -> ContentId {
        let result = codec::write_commit(commit, false, None).unwrap();
        store.put(ContentKind::Commit, &result.bytes).await.unwrap()
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_trace_commits_by_id_single_commit() {
        use futures::StreamExt;

        let store = MemoryContentStore::new();
        let commit = Commit::new(1, vec![make_test_flake(1, 2, 42, 1)]);
        let cid = store_commit(&store, &commit).await;

        let commits: Vec<_> = trace_commits_by_id(store, cid, 0)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].t, 1);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_trace_commits_by_id_chain() {
        use futures::StreamExt;

        let store = MemoryContentStore::new();

        // Build a 3-commit chain: c1 <- c2 <- c3
        let c1 = Commit::new(1, vec![make_test_flake(1, 2, 10, 1)]);
        let c1_id = store_commit(&store, &c1).await;

        let c2 = Commit::new(2, vec![make_test_flake(2, 3, 20, 2)]).with_parent(c1_id.clone());
        let c2_id = store_commit(&store, &c2).await;

        let c3 = Commit::new(3, vec![make_test_flake(3, 4, 30, 3)]).with_parent(c2_id.clone());
        let c3_id = store_commit(&store, &c3).await;

        // Trace from head (c3), stop_at_t=0 → all 3 commits
        let commits: Vec<_> = trace_commits_by_id(store.clone(), c3_id.clone(), 0)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(commits.len(), 3);
        assert_eq!(commits[0].t, 3);
        assert_eq!(commits[1].t, 2);
        assert_eq!(commits[2].t, 1);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_trace_commits_by_id_stop_at_t() {
        use futures::StreamExt;

        let store = MemoryContentStore::new();

        let c1 = Commit::new(1, vec![]);
        let c1_id = store_commit(&store, &c1).await;

        let c2 = Commit::new(2, vec![]).with_parent(c1_id.clone());
        let c2_id = store_commit(&store, &c2).await;

        let c3 = Commit::new(3, vec![]).with_parent(c2_id.clone());
        let c3_id = store_commit(&store, &c3).await;

        // stop_at_t=1 → only c3 and c2 (c1 has t=1, excluded by t <= stop_at_t)
        let commits: Vec<_> = trace_commits_by_id(store, c3_id, 1)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].t, 3);
        assert_eq!(commits[1].t, 2);
    }

    // =========================================================================
    // find_common_ancestor tests
    // =========================================================================

    /// Helper: build a linear commit chain of length `n`.
    ///
    /// Each commit gets a unique flake derived from `branch_tag` so that
    /// independent chains produce distinct CIDs even at the same `t`.
    /// Returns the CIDs in order [t=start_t, t=start_t+1, ...].
    #[cfg(feature = "credential")]
    async fn store_chain(
        store: &MemoryContentStore,
        start_t: i64,
        count: usize,
        parent: Option<ContentId>,
        branch_tag: i64,
    ) -> Vec<ContentId> {
        let mut ids = Vec::with_capacity(count);
        let mut prev = parent;
        for i in 0..count {
            let t = start_t + i as i64;
            let flake = make_test_flake(branch_tag, 1, t, t);
            let mut commit = Commit::new(t, vec![flake]);
            if let Some(ref p) = prev {
                commit = commit.with_parent(p.clone());
            }
            let cid = store_commit(store, &commit).await;
            prev = Some(cid.clone());
            ids.push(cid);
        }
        ids
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_identical_heads() {
        let store = MemoryContentStore::new();
        let chain = store_chain(&store, 1, 3, None, 1).await;
        let head = chain.last().unwrap();

        let ancestor = find_common_ancestor(&store, head, head).await.unwrap();

        assert_eq!(ancestor.commit_id, *head);
        assert_eq!(ancestor.t, 3);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_one_ahead() {
        // chain: c1 <- c2 <- c3
        // head_a = c3, head_b = c2 → ancestor = c2
        let store = MemoryContentStore::new();
        let chain = store_chain(&store, 1, 3, None, 1).await;

        let ancestor = find_common_ancestor(&store, &chain[2], &chain[1])
            .await
            .unwrap();

        assert_eq!(ancestor.commit_id, chain[1]);
        assert_eq!(ancestor.t, 2);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_symmetric() {
        // Result should be the same regardless of argument order.
        let store = MemoryContentStore::new();
        let chain = store_chain(&store, 1, 3, None, 1).await;

        let ab = find_common_ancestor(&store, &chain[2], &chain[0])
            .await
            .unwrap();
        let ba = find_common_ancestor(&store, &chain[0], &chain[2])
            .await
            .unwrap();

        assert_eq!(ab.commit_id, ba.commit_id);
        assert_eq!(ab.t, ba.t);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_diverged_branches() {
        // shared: c1 <- c2 <- c3
        // branch_a:            c3 <- a4 <- a5
        // branch_b:            c3 <- b4
        let store = MemoryContentStore::new();
        let shared = store_chain(&store, 1, 3, None, 1).await;
        let branch_a = store_chain(&store, 4, 2, Some(shared[2].clone()), 100).await;
        let branch_b = store_chain(&store, 4, 1, Some(shared[2].clone()), 200).await;

        let ancestor =
            find_common_ancestor(&store, branch_a.last().unwrap(), branch_b.last().unwrap())
                .await
                .unwrap();

        assert_eq!(ancestor.commit_id, shared[2]);
        assert_eq!(ancestor.t, 3);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_deep_divergence() {
        // shared: c1
        // branch_a: c1 <- a2 <- a3 <- a4 <- a5
        // branch_b: c1 <- b2 <- b3
        let store = MemoryContentStore::new();
        let shared = store_chain(&store, 1, 1, None, 1).await;
        let branch_a = store_chain(&store, 2, 4, Some(shared[0].clone()), 100).await;
        let branch_b = store_chain(&store, 2, 2, Some(shared[0].clone()), 200).await;

        let ancestor =
            find_common_ancestor(&store, branch_a.last().unwrap(), branch_b.last().unwrap())
                .await
                .unwrap();

        assert_eq!(ancestor.commit_id, shared[0]);
        assert_eq!(ancestor.t, 1);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_same_t_different_cids() {
        // Both branches diverge at c1 and advance to the same t.
        // shared: c1
        // branch_a: c1 <- a2
        // branch_b: c1 <- b2
        // Both heads are at t=2 but different CIDs.
        let store = MemoryContentStore::new();
        let shared = store_chain(&store, 1, 1, None, 1).await;
        let branch_a = store_chain(&store, 2, 1, Some(shared[0].clone()), 100).await;
        let branch_b = store_chain(&store, 2, 1, Some(shared[0].clone()), 200).await;

        assert_ne!(branch_a[0], branch_b[0], "CIDs should differ");

        let ancestor = find_common_ancestor(&store, &branch_a[0], &branch_b[0])
            .await
            .unwrap();

        assert_eq!(ancestor.commit_id, shared[0]);
        assert_eq!(ancestor.t, 1);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_genesis_is_ancestor() {
        // shared: c1 (genesis)
        // branch_a: c1 <- a2 <- a3
        // branch_b: c1 <- b2 <- b3 <- b4
        let store = MemoryContentStore::new();
        let genesis = store_chain(&store, 1, 1, None, 1).await;
        let branch_a = store_chain(&store, 2, 2, Some(genesis[0].clone()), 100).await;
        let branch_b = store_chain(&store, 2, 3, Some(genesis[0].clone()), 200).await;

        let ancestor =
            find_common_ancestor(&store, branch_a.last().unwrap(), branch_b.last().unwrap())
                .await
                .unwrap();

        assert_eq!(ancestor.commit_id, genesis[0]);
        assert_eq!(ancestor.t, 1);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_no_common_ancestor_errors() {
        // Two independent chains with no shared genesis.
        let store = MemoryContentStore::new();
        let chain_a = store_chain(&store, 1, 2, None, 100).await;
        let chain_b = store_chain(&store, 1, 2, None, 200).await;

        let result =
            find_common_ancestor(&store, chain_a.last().unwrap(), chain_b.last().unwrap()).await;

        assert!(result.is_err());
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_common_ancestor_one_is_ancestor_of_other() {
        // chain: c1 <- c2 <- c3 <- c4
        // head_a = c4, head_b = c1 → ancestor = c1
        let store = MemoryContentStore::new();
        let chain = store_chain(&store, 1, 4, None, 1).await;

        let ancestor = find_common_ancestor(&store, &chain[3], &chain[0])
            .await
            .unwrap();

        assert_eq!(ancestor.commit_id, chain[0]);
        assert_eq!(ancestor.t, 1);
    }

    // =========================================================================
    // CommitSummary / commit_to_summary / walk_commit_summaries tests
    // =========================================================================

    fn make_retract_flake(s: i64, p: i64, o: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s as u16, format!("s{s}")),
            Sid::new(p as u16, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            false,
            None,
        )
    }

    #[test]
    fn test_commit_to_summary_counts_asserts_and_retracts() {
        let cid = make_test_content_id(ContentKind::Commit, "summary-1");
        let mut commit = Commit::new(
            7,
            vec![
                make_test_flake(1, 2, 10, 7),
                make_test_flake(1, 3, 11, 7),
                make_retract_flake(2, 2, 20, 7),
            ],
        );
        commit.id = Some(cid.clone());
        commit.time = Some("2026-01-01T00:00:00Z".to_string());

        let summary = commit_to_summary(&commit);
        assert_eq!(summary.t, 7);
        assert_eq!(summary.commit_id, cid);
        assert_eq!(summary.asserts, 2);
        assert_eq!(summary.retracts, 1);
        assert_eq!(summary.flake_count, 3);
        assert_eq!(summary.time.as_deref(), Some("2026-01-01T00:00:00Z"));
        assert!(summary.message.is_none());
    }

    #[test]
    fn test_commit_to_summary_extracts_f_message() {
        let cid = make_test_content_id(ContentKind::Commit, "summary-msg");
        let mut commit = Commit::new(3, vec![]);
        commit.id = Some(cid);
        commit.txn_meta = vec![TxnMetaEntry::new(
            fluree_vocab::namespaces::FLUREE_DB,
            "message",
            TxnMetaValue::string("initial commit"),
        )];

        let summary = commit_to_summary(&commit);
        assert_eq!(summary.message.as_deref(), Some("initial commit"));
    }

    #[test]
    fn test_commit_to_summary_ignores_non_string_message() {
        let cid = make_test_content_id(ContentKind::Commit, "summary-msg-int");
        let mut commit = Commit::new(3, vec![]);
        commit.id = Some(cid);
        commit.txn_meta = vec![TxnMetaEntry::new(
            fluree_vocab::namespaces::FLUREE_DB,
            "message",
            TxnMetaValue::long(42),
        )];

        let summary = commit_to_summary(&commit);
        assert!(summary.message.is_none());
    }

    #[test]
    fn test_commit_to_summary_ignores_other_namespace_message() {
        let cid = make_test_content_id(ContentKind::Commit, "summary-msg-otherns");
        let mut commit = Commit::new(3, vec![]);
        commit.id = Some(cid);
        // Same local name "message" but different namespace — should be ignored.
        commit.txn_meta = vec![TxnMetaEntry::new(
            999,
            "message",
            TxnMetaValue::string("not-a-commit-message"),
        )];

        let summary = commit_to_summary(&commit);
        assert!(summary.message.is_none());
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_walk_commit_summaries_orders_newest_first() {
        let store = MemoryContentStore::new();
        let chain = store_chain(&store, 1, 4, None, 1).await;

        let (summaries, total) = walk_commit_summaries(&store, chain.last().unwrap(), 0, None)
            .await
            .unwrap();

        assert_eq!(total, 4);
        assert_eq!(summaries.len(), 4);
        // Newest-first (descending t).
        assert_eq!(summaries[0].t, 4);
        assert_eq!(summaries[1].t, 3);
        assert_eq!(summaries[2].t, 2);
        assert_eq!(summaries[3].t, 1);
        // Each chain commit has one assert flake.
        assert!(summaries.iter().all(|s| s.asserts == 1 && s.retracts == 0));
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_walk_commit_summaries_respects_stop_at_t() {
        let store = MemoryContentStore::new();
        let chain = store_chain(&store, 1, 4, None, 1).await;

        // stop_at_t = 2 → only commits with t > 2 (t=3 and t=4).
        let (summaries, total) = walk_commit_summaries(&store, chain.last().unwrap(), 2, None)
            .await
            .unwrap();

        assert_eq!(total, 2);
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].t, 4);
        assert_eq!(summaries[1].t, 3);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_walk_commit_summaries_caps_with_max() {
        let store = MemoryContentStore::new();
        let chain = store_chain(&store, 1, 5, None, 1).await;

        // 5 commits in the divergence; cap to 2.
        let (summaries, total) = walk_commit_summaries(&store, chain.last().unwrap(), 0, Some(2))
            .await
            .unwrap();

        assert_eq!(total, 5, "total should reflect the full divergence");
        assert_eq!(summaries.len(), 2, "summaries should be capped");
        // Newest first.
        assert_eq!(summaries[0].t, 5);
        assert_eq!(summaries[1].t, 4);
    }

    #[cfg(feature = "credential")]
    #[tokio::test]
    async fn test_walk_commit_summaries_handles_merge_commit() {
        // Build:
        //   shared: c1
        //   branch_a: c1 <- a2 <- a3
        //   branch_b: c1 <- b2
        //   merge:   m4 with parents [a3, b2]
        // walk_commit_summaries from m4 with stop_at_t = 0 should visit each of
        // {m4, a3, a2, b2, c1} exactly once → total = 5.
        let store = MemoryContentStore::new();
        let shared = store_chain(&store, 1, 1, None, 1).await;
        let branch_a = store_chain(&store, 2, 2, Some(shared[0].clone()), 100).await;
        let branch_b = store_chain(&store, 2, 1, Some(shared[0].clone()), 200).await;

        let merge_commit = Commit::new(4, vec![])
            .with_parent(branch_a.last().unwrap().clone())
            .with_parent(branch_b[0].clone());
        let merge_id = store_commit(&store, &merge_commit).await;

        let (summaries, total) = walk_commit_summaries(&store, &merge_id, 0, None)
            .await
            .unwrap();

        assert_eq!(total, 5);
        assert_eq!(summaries.len(), 5);
        // Strictly t-descending.
        for pair in summaries.windows(2) {
            assert!(
                pair[0].t >= pair[1].t,
                "expected newest-first ordering: {} >= {}",
                pair[0].t,
                pair[1].t
            );
        }
    }
}
