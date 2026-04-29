//! Server-side pack generation for `fluree-pack-v1`.
//!
//! Computes the set of missing commits (and optionally index artifacts) needed
//! by a client, then streams them as binary pack frames via an `mpsc` channel.
//!
//! ## Commit ordering
//!
//! Missing commits are returned oldest-first (parents before children) so the
//! client can write objects in causal order. Each commit is followed by its txn
//! blob (if any), deduplicated across the stream.
//!
//! ## Streaming
//!
//! `stream_pack` writes frames into a `tokio::sync::mpsc::Sender<PackChunk>`.
//! The HTTP handler wraps the receiver as `Body::from_stream()` for natural
//! backpressure: if the client is slow the channel fills and the producer awaits.

use crate::error::{ApiError, Result};
use crate::LedgerHandle;
use fluree_db_binary_index::IndexRoot;
use fluree_db_core::pack::{
    encode_data_frame, encode_end_frame, encode_error_frame, encode_header_frame,
    encode_manifest_frame, estimate_pack_bytes, write_stream_preamble, PackHeader, PACK_PROTOCOL,
};

// Re-export types that appear in our public API signatures so consumers
// don't need fluree-db-core as a direct dependency.
use fluree_db_core::commit::codec::envelope::decode_envelope;
use fluree_db_core::commit::codec::format::{CommitHeader, HEADER_LEN};
pub use fluree_db_core::pack::PackRequest;
use fluree_db_core::{ContentId, ContentStore};
use std::collections::HashSet;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// A chunk of pack stream bytes ready for the HTTP response body.
pub type PackChunk = std::result::Result<Vec<u8>, PackStreamError>;

/// Error type for pack stream generation.
#[derive(Debug, thiserror::Error)]
pub enum PackStreamError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("invalid commit: {0}")]
    InvalidCommit(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Summary of a pack stream generation.
#[derive(Debug, Clone)]
pub struct PackStreamResult {
    /// Number of commit data frames sent.
    pub commits_sent: usize,
    /// Number of txn blob data frames sent.
    pub txn_blobs_sent: usize,
    /// Number of index artifact data frames sent.
    pub index_artifacts_sent: usize,
}

// ============================================================================
// Missing commit computation
// ============================================================================

/// Walk backward from each `want` CID until hitting a `have` CID or genesis.
///
/// Returns commit CIDs in oldest-first (topo) order, deduplicated. When
/// multiple `want` heads are provided, they are sorted by `digest_hex()`
/// before walking for deterministic output.
pub async fn compute_missing_commits<C: ContentStore>(
    store: &C,
    want: &[ContentId],
    have: &HashSet<ContentId>,
) -> Result<Vec<ContentId>> {
    // Sort want heads by digest for deterministic ordering.
    let mut sorted_wants: Vec<&ContentId> = want.iter().collect();
    sorted_wants.sort_by_key(|a| a.digest_hex());

    let mut result = Vec::new();
    let mut seen = HashSet::new();

    for want_head in sorted_wants {
        // BFS walk from want_head, collecting all missing commits.
        let mut frontier = vec![want_head.clone()];
        let mut chain = Vec::new();

        while let Some(current_id) = frontier.pop() {
            if have.contains(&current_id) || seen.contains(&current_id) {
                continue;
            }
            seen.insert(current_id.clone());

            // Load envelope metadata only (no flake decompression).
            let raw_bytes = store.get(&current_id).await.map_err(|e| {
                ApiError::internal(format!("failed to read commit {current_id}: {e}"))
            })?;

            let header = CommitHeader::read_from(&raw_bytes).map_err(|e| {
                ApiError::internal(format!("invalid commit header for {current_id}: {e}"))
            })?;

            let envelope_start = HEADER_LEN;
            let envelope_end = envelope_start + header.envelope_len as usize;
            if envelope_end > raw_bytes.len() {
                return Err(ApiError::internal(format!(
                    "commit envelope extends past blob for {current_id}"
                )));
            }
            let env = decode_envelope(&raw_bytes[envelope_start..envelope_end]).map_err(|e| {
                ApiError::internal(format!("failed to decode envelope for {current_id}: {e}"))
            })?;

            chain.push(current_id);

            for parent in env.parents {
                frontier.push(parent);
            }
        }

        // Reverse: oldest first (parents before children).
        chain.reverse();
        result.extend(chain);
    }

    Ok(result)
}

// ============================================================================
// Missing index artifact computation
// ============================================================================

/// Decode an index root blob (FIR6) and return all CAS artifact CIDs.
fn decode_root_cas_ids(bytes: &[u8]) -> std::result::Result<Vec<ContentId>, String> {
    let root = IndexRoot::decode(bytes).map_err(|e| e.to_string())?;
    Ok(root.all_cas_ids())
}

/// Extract named-graph branch CIDs from a decoded root.
///
/// These branch manifests must be loaded from CAS to discover the
/// leaf/sidecar CIDs they contain.
fn extract_branch_cids(bytes: &[u8]) -> Vec<ContentId> {
    if let Ok(root) = IndexRoot::decode(bytes) {
        return root
            .named_graphs
            .iter()
            .flat_map(|ng| ng.orders.iter().map(|(_, cid)| cid.clone()))
            .collect();
    }
    Vec::new()
}

/// Load branch manifests and collect their leaf + sidecar CIDs.
///
/// Named-graph branches (FBR3) are separate CAS objects whose leaf/sidecar
/// CIDs are not inline in the root. This function expands them so that
/// pack/sync transfers a self-contained index snapshot.
async fn expand_branch_leaf_cids<C: ContentStore>(
    store: &C,
    branch_cids: &[ContentId],
) -> Vec<ContentId> {
    let mut ids = Vec::new();
    for branch_cid in branch_cids {
        let bytes = match store.get(branch_cid).await {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(
                    %branch_cid,
                    error = %e,
                    "skipping branch manifest (not loadable)"
                );
                continue;
            }
        };
        let manifest = match fluree_db_binary_index::format::branch::read_branch_from_bytes(&bytes)
        {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!(
                    %branch_cid,
                    error = %e,
                    "skipping branch manifest (decode failed)"
                );
                continue;
            }
        };
        for leaf in &manifest.leaves {
            ids.push(leaf.leaf_cid.clone());
            if let Some(ref sc) = leaf.sidecar_cid {
                ids.push(sc.clone());
            }
        }
    }
    ids
}

/// Compute the set of index artifact CIDs that the client is missing.
///
/// Loads the `want` index root, collects all its CAS artifact CIDs via
/// `all_cas_ids()`, then subtracts the `have` root's artifact set (if provided).
/// The root blob itself is included in the result so the client gets a complete
/// index snapshot.
///
/// Returns artifact CIDs in sorted order (dict/branch/leaf CIDs), deduplicated.
pub async fn compute_missing_index_artifacts<C: ContentStore>(
    store: &C,
    want_root_id: &ContentId,
    have_root_id: Option<&ContentId>,
) -> Result<Vec<ContentId>> {
    // Load and parse the want root (FIR6).
    let want_bytes = store.get(want_root_id).await.map_err(|e| {
        ApiError::internal(format!("failed to read index root {want_root_id}: {e}"))
    })?;
    let want_cas_ids = decode_root_cas_ids(&want_bytes).map_err(|e| {
        ApiError::internal(format!("failed to parse index root {want_root_id}: {e}"))
    })?;

    // Expand named-graph branch manifests to include their leaf/sidecar CIDs.
    let want_branch_cids = extract_branch_cids(&want_bytes);
    let want_branch_leaf_ids = expand_branch_leaf_cids(store, &want_branch_cids).await;

    let mut want_set: HashSet<ContentId> = want_cas_ids.into_iter().collect();
    want_set.extend(want_branch_leaf_ids);
    // Include the root blob itself.
    want_set.insert(want_root_id.clone());

    // If the client has an existing index root, subtract its artifacts.
    if let Some(have_id) = have_root_id {
        if let Ok(have_bytes) = store.get(have_id).await {
            if let Ok(have_ids) = decode_root_cas_ids(&have_bytes) {
                for cid in have_ids {
                    want_set.remove(&cid);
                }
                // Expand the have root's branches too, so shared leaves are subtracted.
                let have_branch_cids = extract_branch_cids(&have_bytes);
                let have_branch_leaf_ids = expand_branch_leaf_cids(store, &have_branch_cids).await;
                for cid in have_branch_leaf_ids {
                    want_set.remove(&cid);
                }
                // Don't remove have_root_id itself — client already has it.
            }
        }
        // If have root can't be loaded, treat as fresh clone (send everything).
    }

    let mut result: Vec<ContentId> = want_set.into_iter().collect();
    result.sort();
    Ok(result)
}

// ============================================================================
// Pack stream generation
// ============================================================================

/// Validate the pack request protocol field.
pub fn validate_pack_request(request: &PackRequest) -> std::result::Result<(), String> {
    if request.protocol != PACK_PROTOCOL {
        return Err(format!(
            "unsupported protocol: expected {}, got {}",
            PACK_PROTOCOL, request.protocol
        ));
    }
    if request.want.is_empty() {
        return Err("want list must not be empty".to_string());
    }
    Ok(())
}

/// Build a `PackRequest` that captures the entire current state of a ledger.
///
/// Reads the handle's current snapshot and sets `want` to the head commit
/// (and, when `include_indexes` is true and the ledger has a populated
/// index, `want_index_root_id` to the current index root). `have` is left
/// empty so the full commit chain is packed.
///
/// Returns an error if the ledger has no head commit — there is nothing
/// to pack, and we prefer an explicit failure over a silent empty archive.
/// If `include_indexes` is requested but the ledger has no index root yet,
/// the returned request falls back to commits-only rather than failing.
pub async fn full_ledger_pack_request(
    handle: &LedgerHandle,
    include_indexes: bool,
) -> Result<PackRequest> {
    let snapshot = handle.snapshot().await;
    let head_commit_id = snapshot.head_commit_id.clone().ok_or_else(|| {
        ApiError::internal(format!(
            "ledger {} has no head commit to pack",
            handle.ledger_id()
        ))
    })?;

    let request = match (include_indexes, snapshot.head_index_id.clone()) {
        (true, Some(index_root)) => {
            PackRequest::with_indexes(vec![head_commit_id], vec![], index_root, None)
        }
        _ => PackRequest::commits(vec![head_commit_id], vec![]),
    };
    Ok(request)
}

/// Generate a pack stream for the given request.
///
/// Writes frame bytes into `frame_tx`. The caller should wrap the receiver
/// as an HTTP streaming response body. On error — including an invalid
/// request (empty `want`, wrong `protocol`) — sends an error frame + end
/// frame and returns zero-valued stats. Callers that need a Rust-side
/// contract should inspect `PackStreamResult` or decode the frame stream
/// for a `PackFrame::Error`.
///
/// For archiving a whole ledger, build the request with
/// [`full_ledger_pack_request`] rather than hand-rolling one — empty
/// `want` is always rejected.
///
/// This function is meant to be `tokio::spawn`ed by the HTTP handler.
pub async fn stream_pack(
    fluree: &crate::Fluree,
    handle: &LedgerHandle,
    request: &PackRequest,
    frame_tx: mpsc::Sender<PackChunk>,
) -> PackStreamResult {
    let result = stream_pack_inner(fluree, handle, request, &frame_tx).await;

    match result {
        Ok(stats) => {
            // Send End frame.
            let mut end_buf = Vec::new();
            encode_end_frame(&mut end_buf);
            let _ = frame_tx.send(Ok(end_buf)).await;
            stats
        }
        Err(err_msg) => {
            // Send Error frame + End frame.
            warn!(error = %err_msg, "pack stream error");
            let mut buf = Vec::new();
            encode_error_frame(&err_msg, &mut buf);
            encode_end_frame(&mut buf);
            let _ = frame_tx.send(Ok(buf)).await;
            PackStreamResult {
                commits_sent: 0,
                txn_blobs_sent: 0,
                index_artifacts_sent: 0,
            }
        }
    }
}

async fn stream_pack_inner(
    fluree: &crate::Fluree,
    handle: &LedgerHandle,
    request: &PackRequest,
    frame_tx: &mpsc::Sender<PackChunk>,
) -> std::result::Result<PackStreamResult, String> {
    // Guard against silent empty packs and protocol mismatches from
    // non-HTTP callers that bypass the route-layer check.
    validate_pack_request(request)?;

    let ledger_id = handle.ledger_id();
    // Branch-aware store: packing a branched ledger requires reading
    // pre-fork ancestor commits that live under the source branch's
    // namespace.
    let content_store = fluree
        .branched_content_store(ledger_id)
        .await
        .map_err(|e| format!("failed to build branched store for {ledger_id}: {e}"))?;

    // --- Early validation: verify all want CIDs exist ---
    for want_cid in &request.want {
        if !content_store
            .has(want_cid)
            .await
            .map_err(|e| format!("storage error checking want CID {want_cid}: {e}"))?
        {
            return Err(format!("requested commit not found: {want_cid}"));
        }
    }

    // --- Compute missing commits ---
    let have_set: HashSet<ContentId> = request.have.iter().cloned().collect();
    let missing_commits = compute_missing_commits(&content_store, &request.want, &have_set)
        .await
        .map_err(|e| format!("failed to compute missing commits: {e}"))?;

    debug!(
        ledger = %ledger_id,
        commit_count = missing_commits.len(),
        "pack: computed missing commits"
    );

    // --- Compute missing index artifacts (if requested) ---
    let include_indexes = request.include_indexes && request.want_index_root_id.is_some();
    let missing_artifacts = if include_indexes {
        let want_root_id = request.want_index_root_id.as_ref().unwrap();
        let artifacts = compute_missing_index_artifacts(
            &content_store,
            want_root_id,
            request.have_index_root_id.as_ref(),
        )
        .await
        .map_err(|e| format!("failed to compute missing index artifacts: {e}"))?;

        debug!(
            ledger = %ledger_id,
            artifact_count = artifacts.len(),
            root_id = %want_root_id,
            "pack: computed missing index artifacts"
        );
        Some(artifacts)
    } else {
        None
    };

    // --- Build and send header ---
    let header = if let Some(ref artifacts) = missing_artifacts {
        let estimated = estimate_pack_bytes(missing_commits.len() as u32);
        PackHeader::with_indexes(
            Some(missing_commits.len() as u32),
            Some(artifacts.len() as u32),
            estimated,
            request.include_txns,
        )
    } else {
        PackHeader::commits_only(Some(missing_commits.len() as u32), request.include_txns)
    };

    let mut preamble_buf = Vec::with_capacity(256);
    write_stream_preamble(&mut preamble_buf);
    encode_header_frame(&header, &mut preamble_buf);
    frame_tx
        .send(Ok(preamble_buf))
        .await
        .map_err(|_| "client disconnected".to_string())?;

    // --- Stream commit + txn data frames ---
    let mut commits_sent = 0;
    let mut txn_blobs_sent = 0;
    let mut txn_cids_sent = HashSet::new();

    for commit_cid in &missing_commits {
        // Read raw commit bytes.
        let raw_bytes = content_store
            .get(commit_cid)
            .await
            .map_err(|e| format!("failed to read commit {commit_cid}: {e}"))?;

        // Encode and send commit data frame.
        let mut buf = Vec::with_capacity(raw_bytes.len() + 64);
        encode_data_frame(commit_cid, &raw_bytes, &mut buf);
        frame_tx
            .send(Ok(buf))
            .await
            .map_err(|_| "client disconnected".to_string())?;
        commits_sent += 1;

        if !request.include_txns {
            continue;
        }

        // Decode envelope to find txn blob CID.
        let header = CommitHeader::read_from(&raw_bytes)
            .map_err(|e| format!("invalid commit header for {commit_cid}: {e}"))?;

        let envelope_start = HEADER_LEN;
        let envelope_end = envelope_start + header.envelope_len as usize;
        if envelope_end <= raw_bytes.len() {
            if let Ok(env) = decode_envelope(&raw_bytes[envelope_start..envelope_end]) {
                // Send txn blob after its referencing commit (causal order), deduped.
                if let Some(ref txn_cid) = env.txn {
                    if txn_cids_sent.insert(txn_cid.clone()) {
                        let txn_bytes = content_store
                            .get(txn_cid)
                            .await
                            .map_err(|e| format!("failed to read txn blob {txn_cid}: {e}"))?;

                        let mut txn_buf = Vec::with_capacity(txn_bytes.len() + 64);
                        encode_data_frame(txn_cid, &txn_bytes, &mut txn_buf);
                        frame_tx
                            .send(Ok(txn_buf))
                            .await
                            .map_err(|_| "client disconnected".to_string())?;
                        txn_blobs_sent += 1;
                    }
                }
            }
        }
    }

    // --- Stream index artifacts (if requested) ---
    let mut index_artifacts_sent = 0;

    if let Some(ref artifacts) = missing_artifacts {
        let want_root_id = request.want_index_root_id.as_ref().unwrap();

        // Send manifest frame announcing the index phase.
        let manifest = serde_json::json!({
            "phase": "indexes",
            "root_id": want_root_id.to_string(),
            "artifact_count": artifacts.len(),
        });
        let mut manifest_buf = Vec::with_capacity(256);
        encode_manifest_frame(&manifest, &mut manifest_buf);
        frame_tx
            .send(Ok(manifest_buf))
            .await
            .map_err(|_| "client disconnected".to_string())?;

        // Stream each missing artifact as a data frame.
        for artifact_cid in artifacts {
            let artifact_bytes = content_store
                .get(artifact_cid)
                .await
                .map_err(|e| format!("failed to read index artifact {artifact_cid}: {e}"))?;

            let mut buf = Vec::with_capacity(artifact_bytes.len() + 64);
            encode_data_frame(artifact_cid, &artifact_bytes, &mut buf);
            frame_tx
                .send(Ok(buf))
                .await
                .map_err(|_| "client disconnected".to_string())?;
            index_artifacts_sent += 1;
        }
    }

    Ok(PackStreamResult {
        commits_sent,
        txn_blobs_sent,
        index_artifacts_sent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::pack::{
        decode_frame, read_stream_preamble, PackFrame, DEFAULT_MAX_PAYLOAD,
    };
    use fluree_db_core::ContentKind;

    // Kept for: integration tests that need to decode a full pack stream.
    // Use when: writing pack round-trip integration tests (Phase 2 tests).
    #[expect(dead_code)]
    fn decode_all_frames(data: &[u8]) -> Vec<PackFrame> {
        let mut pos = read_stream_preamble(data).expect("valid preamble");
        let mut frames = Vec::new();
        loop {
            let (frame, consumed) =
                decode_frame(&data[pos..], DEFAULT_MAX_PAYLOAD).expect("valid frame");
            pos += consumed;
            let is_end = matches!(frame, PackFrame::End);
            frames.push(frame);
            if is_end {
                break;
            }
        }
        frames
    }

    #[test]
    fn test_validate_pack_request() {
        let good = PackRequest::commits(vec![ContentId::new(ContentKind::Commit, b"head")], vec![]);
        assert!(validate_pack_request(&good).is_ok());

        let bad_proto = PackRequest {
            protocol: "unknown".to_string(),
            ..good.clone()
        };
        assert!(validate_pack_request(&bad_proto).is_err());

        let empty_want = PackRequest::commits(vec![], vec![]);
        assert!(validate_pack_request(&empty_want).is_err());
    }
}
