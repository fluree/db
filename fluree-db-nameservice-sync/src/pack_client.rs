//! Client-side pack consumption for `fluree-pack-v1`.
//!
//! Fetches a binary pack stream from a remote server, decodes frames, verifies
//! each object's integrity, and writes it to local CAS storage.
//!
//! ## Verification
//!
//! Commit blobs (v4) use full-blob SHA-256 via `verify_commit_blob`. All other
//! content kinds use full-bytes SHA-256 via `ContentId::verify()`.
//!
//! ## Write path
//!
//! Objects are written via `content_write_bytes_with_hash()`, which lets storage
//! control layout while the client controls hash derivation.

use crate::error::{Result, SyncError};
use crate::origin::verify_object_integrity;
use bytes::BytesMut;
use fluree_db_core::pack::{
    decode_frame, read_stream_preamble, PackFrame, PackHeader, PackRequest, DEFAULT_MAX_PAYLOAD,
    PACK_PROTOCOL, PREAMBLE_SIZE,
};
use fluree_db_core::{ContentAddressedWrite, ContentId, ContentKind};
use futures::StreamExt;
use tracing::debug;

/// Result of ingesting a pack stream into local storage.
#[derive(Debug, Clone, Default)]
pub struct PackIngestResult {
    /// Number of commit blobs stored.
    pub commits_stored: usize,
    /// Number of txn blobs stored.
    pub txn_blobs_stored: usize,
    /// Number of index artifact blobs stored.
    pub index_artifacts_stored: usize,
    /// Total bytes received.
    pub total_bytes: u64,
}

/// Fetch a pack stream from a remote server and ingest all objects into local storage.
///
/// Makes `POST {origin_base_url}/pack/{ledger}` with the given request body.
/// `origin_base_url` should be the **normalized** base URL ending with `/fluree`
/// (matching the `HttpOriginFetcher.base_url()` convention).
///
/// If the server returns 404/405/406/501 (pack not supported), returns
/// `Err(SyncError::PackNotSupported)` for fallback to paginated export.
pub async fn fetch_and_ingest_pack<S: ContentAddressedWrite>(
    http: &reqwest::Client,
    origin_base_url: &str,
    ledger_id: &str,
    request: &PackRequest,
    storage: &S,
    auth_token: Option<&str>,
) -> Result<PackIngestResult> {
    let url = format!(
        "{}/pack/{}",
        origin_base_url.trim_end_matches('/'),
        ledger_id
    );

    let mut req_builder = http
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/x-fluree-pack");

    if let Some(token) = auth_token {
        req_builder = req_builder.bearer_auth(token);
    }

    let body = serde_json::to_vec(request)
        .map_err(|e| SyncError::PackProtocol(format!("failed to serialize pack request: {e}")))?;

    let response = req_builder
        .body(body)
        .send()
        .await
        .map_err(|e| SyncError::Remote(format!("pack request failed: {e}")))?;

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND
        || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
        || status == reqwest::StatusCode::NOT_ACCEPTABLE
        || status == reqwest::StatusCode::NOT_IMPLEMENTED
    {
        return Err(SyncError::PackNotSupported);
    }
    if !status.is_success() {
        return Err(SyncError::Remote(format!("pack request returned {status}")));
    }

    // Stream the response body and decode frames.
    ingest_pack_stream(response, storage, ledger_id).await
}

/// Decode and ingest all frames from an HTTP response stream.
///
/// Incrementally decodes binary pack frames from the response body, verifies
/// each object's integrity, and writes it to local CAS storage.
///
/// Typically called after obtaining a `reqwest::Response` from
/// [`HttpOriginFetcher::fetch_pack_response`](crate::HttpOriginFetcher) or
/// [`MultiOriginFetcher::fetch_pack_response`](crate::MultiOriginFetcher).
pub async fn ingest_pack_stream<S: ContentAddressedWrite>(
    response: reqwest::Response,
    storage: &S,
    ledger_id: &str,
) -> Result<PackIngestResult> {
    let mut stream = response.bytes_stream();
    let mut buf = BytesMut::new();
    let mut result = PackIngestResult::default();
    let mut preamble_consumed = false;
    let mut saw_header = false;
    let mut saw_any_frame = false;

    // Fill the buffer from the stream until we have enough for decoding.
    loop {
        // Try to decode from what we have.
        if !preamble_consumed {
            if buf.len() >= PREAMBLE_SIZE {
                read_stream_preamble(&buf)
                    .map_err(|e| SyncError::PackProtocol(format!("invalid pack preamble: {e}")))?;
                let _ = buf.split_to(PREAMBLE_SIZE);
                preamble_consumed = true;
                continue;
            }
        } else {
            // Try to decode a frame.
            match decode_frame(&buf, DEFAULT_MAX_PAYLOAD) {
                Ok((frame, consumed)) => {
                    let _ = buf.split_to(consumed);

                    // Protocol rule: the Header frame is mandatory and must be the
                    // first frame after the stream preamble.
                    if !saw_any_frame && !matches!(frame, PackFrame::Header(_)) {
                        return Err(SyncError::PackProtocol(
                            "pack stream must start with Header frame".to_string(),
                        ));
                    }
                    saw_any_frame = true;

                    match frame {
                        PackFrame::Header(header) => {
                            if saw_header {
                                return Err(SyncError::PackProtocol(
                                    "pack stream contains multiple Header frames".to_string(),
                                ));
                            }
                            if header.protocol != PACK_PROTOCOL {
                                return Err(SyncError::PackProtocol(format!(
                                    "unsupported pack protocol: expected {}, got {}",
                                    PACK_PROTOCOL, header.protocol
                                )));
                            }
                            saw_header = true;
                            debug!(
                                protocol = %header.protocol,
                                capabilities = ?header.capabilities,
                                commit_count = ?header.commit_count,
                                "pack: received header"
                            );
                        }
                        PackFrame::Data { cid, payload } => {
                            if !saw_header {
                                return Err(SyncError::PackProtocol(
                                    "pack stream missing Header frame".to_string(),
                                ));
                            }
                            result.total_bytes += payload.len() as u64;
                            ingest_pack_frame(&cid, &payload, storage, ledger_id).await?;

                            // Categorize by content kind.
                            match cid.content_kind() {
                                Some(ContentKind::Commit) => result.commits_stored += 1,
                                Some(ContentKind::Txn) => result.txn_blobs_stored += 1,
                                _ => result.index_artifacts_stored += 1,
                            }
                        }
                        PackFrame::Error(msg) => {
                            return Err(SyncError::PackProtocol(format!(
                                "server error in pack stream: {msg}"
                            )));
                        }
                        PackFrame::Manifest(manifest) => {
                            if !saw_header {
                                return Err(SyncError::PackProtocol(
                                    "pack stream missing Header frame".to_string(),
                                ));
                            }
                            debug!(manifest = %manifest, "pack: received manifest");
                        }
                        PackFrame::End => {
                            if !saw_header {
                                return Err(SyncError::PackProtocol(
                                    "pack stream missing Header frame".to_string(),
                                ));
                            }
                            debug!(
                                commits = result.commits_stored,
                                txns = result.txn_blobs_stored,
                                index_artifacts = result.index_artifacts_stored,
                                total_bytes = result.total_bytes,
                                "pack: stream complete"
                            );
                            return Ok(result);
                        }
                    }
                    continue;
                }
                Err(fluree_db_core::pack::PackError::Incomplete(_)) => {
                    // Need more bytes — fall through to read from stream.
                }
                Err(e) => {
                    return Err(SyncError::PackProtocol(format!(
                        "pack frame decode error: {e}"
                    )));
                }
            }
        }

        // Read more bytes from the HTTP stream.
        match stream.next().await {
            Some(Ok(chunk)) => {
                buf.extend_from_slice(&chunk);
            }
            Some(Err(e)) => {
                return Err(SyncError::Remote(format!("error reading pack stream: {e}")));
            }
            None => {
                // Stream ended without End frame.
                if buf.is_empty() && !preamble_consumed {
                    return Err(SyncError::PackProtocol("empty pack stream".to_string()));
                }
                return Err(SyncError::PackProtocol(
                    "pack stream ended without End frame".to_string(),
                ));
            }
        }
    }
}

/// Read just the preamble and header frame from a pack stream.
///
/// Consumes the body stream until the preamble + header frame are parsed.
/// Returns the parsed `PackHeader` and a `BytesMut` containing any leftover
/// bytes already read past the header frame (start of the next frame).
///
/// The caller retains the body stream (advanced past the header). To continue
/// ingestion, pass the header, buffer tail, and stream to
/// [`ingest_pack_stream_with_header`].
pub async fn peek_pack_header(
    stream: &mut (impl futures::Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>>
              + Unpin),
) -> Result<(PackHeader, BytesMut)> {
    let mut buf = BytesMut::new();
    let mut preamble_consumed = false;

    loop {
        if !preamble_consumed {
            if buf.len() >= PREAMBLE_SIZE {
                read_stream_preamble(&buf)
                    .map_err(|e| SyncError::PackProtocol(format!("invalid pack preamble: {e}")))?;
                let _ = buf.split_to(PREAMBLE_SIZE);
                preamble_consumed = true;
                continue;
            }
        } else {
            // Try to decode the header frame.
            match decode_frame(&buf, DEFAULT_MAX_PAYLOAD) {
                Ok((frame, consumed)) => {
                    let _ = buf.split_to(consumed);
                    match frame {
                        PackFrame::Header(header) => {
                            if header.protocol != PACK_PROTOCOL {
                                return Err(SyncError::PackProtocol(format!(
                                    "unsupported pack protocol: expected {}, got {}",
                                    PACK_PROTOCOL, header.protocol
                                )));
                            }
                            return Ok((header, buf));
                        }
                        PackFrame::Error(msg) => {
                            return Err(SyncError::PackProtocol(format!(
                                "server error in pack stream: {msg}"
                            )));
                        }
                        _ => {
                            return Err(SyncError::PackProtocol(
                                "pack stream must start with Header frame".to_string(),
                            ));
                        }
                    }
                }
                Err(fluree_db_core::pack::PackError::Incomplete(_)) => {
                    // Need more bytes.
                }
                Err(e) => {
                    return Err(SyncError::PackProtocol(format!(
                        "pack frame decode error: {e}"
                    )));
                }
            }
        }

        // Read more bytes from the stream.
        match stream.next().await {
            Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
            Some(Err(e)) => {
                return Err(SyncError::Remote(format!("error reading pack stream: {e}")));
            }
            None => {
                return Err(SyncError::PackProtocol(
                    "pack stream ended before header frame".to_string(),
                ));
            }
        }
    }
}

/// Ingest a pack stream with a pre-parsed header.
///
/// Like [`ingest_pack_stream`], but skips preamble and header parsing — uses
/// the header already obtained via [`peek_pack_header`]. The `initial_buf`
/// contains any leftover bytes from the header peek, which are consumed before
/// reading from `stream`.
pub async fn ingest_pack_stream_with_header<S: ContentAddressedWrite>(
    header: &PackHeader,
    initial_buf: BytesMut,
    stream: &mut (impl futures::Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>>
              + Unpin),
    storage: &S,
    ledger_id: &str,
) -> Result<PackIngestResult> {
    let mut buf = initial_buf;
    let mut result = PackIngestResult::default();

    debug!(
        protocol = %header.protocol,
        capabilities = ?header.capabilities,
        commit_count = ?header.commit_count,
        "pack: received header (pre-parsed)"
    );

    // Decode remaining frames (data, manifest, end).
    loop {
        match decode_frame(&buf, DEFAULT_MAX_PAYLOAD) {
            Ok((frame, consumed)) => {
                let _ = buf.split_to(consumed);

                match frame {
                    PackFrame::Header(_) => {
                        return Err(SyncError::PackProtocol(
                            "pack stream contains multiple Header frames".to_string(),
                        ));
                    }
                    PackFrame::Data { cid, payload } => {
                        result.total_bytes += payload.len() as u64;
                        ingest_pack_frame(&cid, &payload, storage, ledger_id).await?;

                        match cid.content_kind() {
                            Some(ContentKind::Commit) => result.commits_stored += 1,
                            Some(ContentKind::Txn) => result.txn_blobs_stored += 1,
                            _ => result.index_artifacts_stored += 1,
                        }
                    }
                    PackFrame::Error(msg) => {
                        return Err(SyncError::PackProtocol(format!(
                            "server error in pack stream: {msg}"
                        )));
                    }
                    PackFrame::Manifest(manifest) => {
                        debug!(manifest = %manifest, "pack: received manifest");
                    }
                    PackFrame::End => {
                        debug!(
                            commits = result.commits_stored,
                            txns = result.txn_blobs_stored,
                            index_artifacts = result.index_artifacts_stored,
                            total_bytes = result.total_bytes,
                            "pack: stream complete"
                        );
                        return Ok(result);
                    }
                }
                continue;
            }
            Err(fluree_db_core::pack::PackError::Incomplete(_)) => {
                // Need more bytes.
            }
            Err(e) => {
                return Err(SyncError::PackProtocol(format!(
                    "pack frame decode error: {e}"
                )));
            }
        }

        // Read more bytes from the stream.
        match stream.next().await {
            Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
            Some(Err(e)) => {
                return Err(SyncError::Remote(format!("error reading pack stream: {e}")));
            }
            None => {
                if buf.is_empty() {
                    return Err(SyncError::PackProtocol(
                        "pack stream ended without End frame".to_string(),
                    ));
                }
                return Err(SyncError::PackProtocol(
                    "pack stream ended without End frame".to_string(),
                ));
            }
        }
    }
}

/// Verify and write a single CAS object from a pack data frame.
pub async fn ingest_pack_frame<S: ContentAddressedWrite>(
    cid: &ContentId,
    bytes: &[u8],
    storage: &S,
    ledger_id: &str,
) -> Result<()> {
    // Reject unknown content kinds early.
    let kind = cid.content_kind().ok_or_else(|| {
        SyncError::PackProtocol(format!(
            "unknown content kind for CID {} (codec 0x{:x})",
            cid,
            cid.codec()
        ))
    })?;

    // Verify integrity (format-sniffing for commit-v2).
    if !verify_object_integrity(cid, bytes) {
        return Err(SyncError::PackProtocol(format!(
            "integrity check failed for {cid}"
        )));
    }

    // For commit-v2 blobs, derive the hash from the canonical sub-range.
    // For everything else, the CID digest is the full-bytes hash.
    let digest_hex = if kind == ContentKind::Commit {
        // Commit CID digest was already verified by verify_object_integrity.
        // Re-derive it via the same path for the write call.
        derive_commit_digest_hex(bytes)?
    } else {
        cid.digest_hex()
    };

    storage
        .content_write_bytes_with_hash(kind, ledger_id, &digest_hex, bytes)
        .await
        .map_err(|e| SyncError::PackProtocol(format!("failed to write {cid}: {e}")))?;

    Ok(())
}

/// Derive the canonical SHA-256 hex digest for a commit blob.
///
/// This must match the hash derivation in `verify_commit_blob`.
fn derive_commit_digest_hex(bytes: &[u8]) -> Result<String> {
    match fluree_db_core::commit::codec::verify_commit_blob(bytes) {
        Ok(derived_id) => Ok(derived_id.digest_hex()),
        Err(e) => Err(SyncError::PackProtocol(format!(
            "failed to derive commit digest: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::pack::{
        encode_end_frame, encode_header_frame, write_stream_preamble, PackHeader,
    };
    use fluree_db_core::ContentKind;

    #[test]
    fn test_pack_ingest_result_defaults() {
        let result = PackIngestResult::default();
        assert_eq!(result.commits_stored, 0);
        assert_eq!(result.txn_blobs_stored, 0);
        assert_eq!(result.index_artifacts_stored, 0);
        assert_eq!(result.total_bytes, 0);
    }

    #[test]
    fn test_verify_non_commit_object() {
        let data = b"test txn data";
        let cid = ContentId::new(ContentKind::Txn, data);
        assert!(verify_object_integrity(&cid, data));
        assert!(!verify_object_integrity(&cid, b"wrong data"));
    }

    #[test]
    fn test_encode_minimal_pack_stream() {
        // Build a minimal pack stream (header + end only) to validate
        // our frame encoding matches the expected format.
        let mut buf = Vec::new();
        write_stream_preamble(&mut buf);
        encode_header_frame(&PackHeader::commits_only(Some(0), true), &mut buf);
        encode_end_frame(&mut buf);

        // Should be parseable: preamble + at least 2 frames
        assert!(buf.len() > PREAMBLE_SIZE + 2);

        // Verify preamble
        assert_eq!(&buf[..4], b"FPK1");
        assert_eq!(buf[4], 1); // version
    }

    #[test]
    fn test_derive_commit_digest_fails_on_non_commit() {
        // Non-commit bytes should fail verification.
        let result = derive_commit_digest_hex(b"not a commit");
        assert!(result.is_err());
    }
}
