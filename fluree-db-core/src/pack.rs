//! Binary pack stream format (`fluree-pack-v1`).
//!
//! Wire format for efficient bulk transfer of CAS objects (commits, txn blobs,
//! index artifacts) between Fluree instances. Replaces paginated JSON export
//! with a single binary stream.
//!
//! ## Stream Layout
//!
//! ```text
//! [Preamble: FPK1 + version(1)] [Header frame] [Data|Manifest|Error]* [End frame]
//! ```
//!
//! ## Frame Format
//!
//! | Frame    | Type byte | Layout after type byte                                    |
//! |----------|-----------|-----------------------------------------------------------|
//! | Header   | `0x00`    | `payload_len:u32LE` + JSON bytes (`PackHeader`)           |
//! | Data     | `0x01`    | `cid_len:u16LE` + CID binary + `payload_len:u32LE` + raw  |
//! | Error    | `0x02`    | `msg_len:u32LE` + UTF-8 message                           |
//! | Manifest | `0x03`    | `payload_len:u32LE` + JSON bytes                          |
//! | End      | `0xFF`    | (no payload)                                              |
//!
//! ## Design
//!
//! Core encode/decode operates on `&[u8]` / `Vec<u8>` (sync, no IO traits).
//! Server and client buffer frame-sized chunks from their respective async
//! streams, then call these sync decoders.

use crate::content_id::ContentId;
use serde::{Deserialize, Serialize};

// ============================================================================
// Constants
// ============================================================================

/// Magic bytes identifying a fluree-pack-v1 stream.
pub const PACK_MAGIC: [u8; 4] = *b"FPK1";

/// Current pack protocol version.
pub const PACK_VERSION: u8 = 1;

/// Protocol identifier string for request/response metadata.
pub const PACK_PROTOCOL: &str = "fluree-pack-v1";

/// Default maximum frame payload size (256 MiB).
pub const DEFAULT_MAX_PAYLOAD: u32 = 256 * 1024 * 1024;

/// Maximum CID binary length in a Data frame.
pub const MAX_CID_LEN: u16 = 128;

/// Size of the stream preamble (magic + version).
pub const PREAMBLE_SIZE: usize = PACK_MAGIC.len() + 1;

// Frame type discriminators
const FRAME_HEADER: u8 = 0x00;
const FRAME_DATA: u8 = 0x01;
const FRAME_ERROR: u8 = 0x02;
const FRAME_MANIFEST: u8 = 0x03;
const FRAME_END: u8 = 0xFF;

// ============================================================================
// Error types
// ============================================================================

/// Errors specific to pack stream encoding/decoding.
#[derive(Debug, thiserror::Error)]
pub enum PackError {
    /// Not enough bytes in the buffer; need at least this many total.
    ///
    /// This is not a protocol error — the caller should buffer more data
    /// and retry decoding.
    #[error("incomplete: need at least {0} bytes")]
    Incomplete(usize),

    /// Stream preamble has wrong magic bytes.
    #[error("invalid magic bytes")]
    InvalidMagic,

    /// Pack version not supported by this decoder.
    #[error("unsupported pack version: {0}")]
    UnsupportedVersion(u8),

    /// Unknown frame type byte.
    #[error("invalid frame type: 0x{0:02x}")]
    InvalidFrameType(u8),

    /// Frame payload exceeds configured maximum.
    #[error("payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: u32, max: u32 },

    /// CID binary exceeds [`MAX_CID_LEN`] bytes.
    #[error("CID too large: {0} bytes (max {MAX_CID_LEN})")]
    CidTooLarge(u16),

    /// CID bytes failed to parse.
    #[error("invalid CID: {0}")]
    InvalidCid(String),

    /// Error frame message is not valid UTF-8.
    #[error("invalid UTF-8 in error frame")]
    InvalidUtf8,

    /// JSON parsing failed for Header or Manifest frame.
    #[error("invalid JSON in frame: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

/// Result type for pack operations.
pub type PackResult<T> = std::result::Result<T, PackError>;

// ============================================================================
// Request / Response metadata (JSON, used in HTTP body and Header frame)
// ============================================================================

/// Client request for a pack stream.
///
/// Sent as JSON in the `POST /v1/fluree/pack/{ledger}` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackRequest {
    /// Protocol identifier — must be `"fluree-pack-v1"`.
    pub protocol: String,

    /// Commit head CIDs the client wants (typically one: the remote head).
    pub want: Vec<ContentId>,

    /// Commit head CIDs the client already has (roots of known history).
    pub have: Vec<ContentId>,

    /// Explicit index root CID to transfer (from remote nameservice `index_head_id`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub want_index_root_id: Option<ContentId>,

    /// Client's current index root CID (for diff computation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub have_index_root_id: Option<ContentId>,

    /// Whether to include index artifacts in the pack.
    pub include_indexes: bool,

    /// Whether to include original transaction blobs in the pack.
    ///
    /// When false, commits are streamed without their referenced `txn` blobs.
    /// The commit chain remains valid and verifiable, but original transaction
    /// payloads (e.g., JSON-LD insert/update requests) will not be available
    /// on the client. Use this to dramatically shrink clones when only the
    /// materialized ledger state matters.
    pub include_txns: bool,
}

/// Server response header, the mandatory first frame in the pack stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackHeader {
    /// Protocol identifier — echoes `"fluree-pack-v1"`.
    pub protocol: String,

    /// Object types the stream will contain.
    ///
    /// Default: `["commits", "txns"]`.
    /// With indexes: `["commits", "txns", "indexes"]`.
    pub capabilities: Vec<String>,

    /// Server's maximum frame payload size in bytes (informational).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_max_frame_bytes: Option<u32>,

    /// Number of commit objects that will be streamed (if known up front).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_count: Option<u32>,

    /// Number of index artifact objects (if known up front).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_artifact_count: Option<u32>,

    /// Estimated total transfer size in bytes (commits + indexes).
    ///
    /// This is a rough estimate based on object counts and average sizes,
    /// used by clients to warn before large downloads. Not an exact measurement.
    /// Zero when not computed (e.g. commits-only transfers).
    pub estimated_total_bytes: u64,
}

// ============================================================================
// Frame types
// ============================================================================

/// A decoded pack frame.
#[derive(Debug)]
pub enum PackFrame {
    /// Stream header with protocol metadata. Must be the first frame.
    Header(PackHeader),

    /// A CAS object: content ID + raw bytes.
    Data { cid: ContentId, payload: Vec<u8> },

    /// Server-side error. Stream should end after this frame.
    Error(String),

    /// Phase manifest (e.g., index phase metadata).
    Manifest(serde_json::Value),

    /// End-of-stream marker.
    End,
}

// ============================================================================
// Encoding
// ============================================================================

/// Write the stream preamble (`FPK1` + version byte).
pub fn write_stream_preamble(out: &mut Vec<u8>) {
    out.extend_from_slice(&PACK_MAGIC);
    out.push(PACK_VERSION);
}

/// Encode a Header frame.
pub fn encode_header_frame(header: &PackHeader, out: &mut Vec<u8>) {
    let json = serde_json::to_vec(header).expect("PackHeader serialization cannot fail");
    out.push(FRAME_HEADER);
    out.extend_from_slice(&(json.len() as u32).to_le_bytes());
    out.extend_from_slice(&json);
}

/// Encode a Data frame (CID + payload).
pub fn encode_data_frame(cid: &ContentId, payload: &[u8], out: &mut Vec<u8>) {
    let cid_bytes = cid.to_bytes();
    debug_assert!(
        cid_bytes.len() <= MAX_CID_LEN as usize,
        "CID binary exceeds {MAX_CID_LEN} bytes"
    );
    out.push(FRAME_DATA);
    out.extend_from_slice(&(cid_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(&cid_bytes);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Encode an Error frame.
pub fn encode_error_frame(message: &str, out: &mut Vec<u8>) {
    let msg_bytes = message.as_bytes();
    out.push(FRAME_ERROR);
    out.extend_from_slice(&(msg_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(msg_bytes);
}

/// Encode a Manifest frame (arbitrary JSON metadata).
pub fn encode_manifest_frame(manifest: &serde_json::Value, out: &mut Vec<u8>) {
    let json = serde_json::to_vec(manifest).expect("Manifest serialization cannot fail");
    out.push(FRAME_MANIFEST);
    out.extend_from_slice(&(json.len() as u32).to_le_bytes());
    out.extend_from_slice(&json);
}

/// Encode an End frame.
pub fn encode_end_frame(out: &mut Vec<u8>) {
    out.push(FRAME_END);
}

// ============================================================================
// Decoding
// ============================================================================

/// Validate and consume the stream preamble.
///
/// Returns the number of bytes consumed (always [`PREAMBLE_SIZE`]) on success.
pub fn read_stream_preamble(data: &[u8]) -> PackResult<usize> {
    if data.len() < PREAMBLE_SIZE {
        return Err(PackError::Incomplete(PREAMBLE_SIZE));
    }
    if data[..4] != PACK_MAGIC {
        return Err(PackError::InvalidMagic);
    }
    let version = data[4];
    if version != PACK_VERSION {
        return Err(PackError::UnsupportedVersion(version));
    }
    Ok(PREAMBLE_SIZE)
}

/// Decode one frame from the buffer.
///
/// Returns the decoded frame and the number of bytes consumed. The caller
/// should advance its buffer by the consumed count.
///
/// `max_payload` caps the maximum payload size per frame. Frames declaring
/// a payload larger than this are rejected **before** allocation.
pub fn decode_frame(data: &[u8], max_payload: u32) -> PackResult<(PackFrame, usize)> {
    if data.is_empty() {
        return Err(PackError::Incomplete(1));
    }
    match data[0] {
        FRAME_HEADER => decode_json_frame(data, max_payload, true),
        FRAME_DATA => decode_data_frame(data, max_payload),
        FRAME_ERROR => decode_error_frame(data, max_payload),
        FRAME_MANIFEST => decode_json_frame(data, max_payload, false),
        FRAME_END => Ok((PackFrame::End, 1)),
        other => Err(PackError::InvalidFrameType(other)),
    }
}

/// Decode a JSON-payload frame (Header or Manifest).
fn decode_json_frame(
    data: &[u8],
    max_payload: u32,
    is_header: bool,
) -> PackResult<(PackFrame, usize)> {
    // type(1) + payload_len(4)
    if data.len() < 5 {
        return Err(PackError::Incomplete(5));
    }
    let payload_len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
    check_payload_size(payload_len, max_payload)?;

    let total = 5 + payload_len as usize;
    if data.len() < total {
        return Err(PackError::Incomplete(total));
    }
    let json_bytes = &data[5..total];
    if is_header {
        let header: PackHeader = serde_json::from_slice(json_bytes)?;
        Ok((PackFrame::Header(header), total))
    } else {
        let value: serde_json::Value = serde_json::from_slice(json_bytes)?;
        Ok((PackFrame::Manifest(value), total))
    }
}

/// Decode a Data frame.
fn decode_data_frame(data: &[u8], max_payload: u32) -> PackResult<(PackFrame, usize)> {
    // type(1) + cid_len(2)
    if data.len() < 3 {
        return Err(PackError::Incomplete(3));
    }
    let cid_len = u16::from_le_bytes([data[1], data[2]]);
    if cid_len > MAX_CID_LEN {
        return Err(PackError::CidTooLarge(cid_len));
    }

    // type(1) + cid_len(2) + cid(N) + payload_len(4)
    let pre_payload = 3 + cid_len as usize + 4;
    if data.len() < pre_payload {
        return Err(PackError::Incomplete(pre_payload));
    }

    let cid_end = 3 + cid_len as usize;
    let cid = ContentId::from_bytes(&data[3..cid_end])
        .map_err(|e| PackError::InvalidCid(e.to_string()))?;

    let payload_len = u32::from_le_bytes([
        data[cid_end],
        data[cid_end + 1],
        data[cid_end + 2],
        data[cid_end + 3],
    ]);
    check_payload_size(payload_len, max_payload)?;

    let total = pre_payload + payload_len as usize;
    if data.len() < total {
        return Err(PackError::Incomplete(total));
    }

    let payload = data[pre_payload..total].to_vec();
    Ok((PackFrame::Data { cid, payload }, total))
}

/// Decode an Error frame.
fn decode_error_frame(data: &[u8], max_payload: u32) -> PackResult<(PackFrame, usize)> {
    // type(1) + msg_len(4)
    if data.len() < 5 {
        return Err(PackError::Incomplete(5));
    }
    let msg_len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
    check_payload_size(msg_len, max_payload)?;

    let total = 5 + msg_len as usize;
    if data.len() < total {
        return Err(PackError::Incomplete(total));
    }
    let message = std::str::from_utf8(&data[5..total]).map_err(|_| PackError::InvalidUtf8)?;
    Ok((PackFrame::Error(message.to_string()), total))
}

fn check_payload_size(size: u32, max: u32) -> PackResult<()> {
    if size > max {
        Err(PackError::PayloadTooLarge { size, max })
    } else {
        Ok(())
    }
}

// ============================================================================
// Size estimation
// ============================================================================

/// Average bytes per commit object (commit blob + txn blob).
pub const AVG_COMMIT_BYTES: u64 = 50_000;

/// Ratio of index artifact bytes to commit bytes.
/// Based on observed data: index artifacts are roughly 1.1× the size of commit data.
pub const INDEX_TO_COMMIT_SIZE_RATIO: f64 = 1.1;

/// Threshold above which clients should prompt for confirmation (1 GiB).
pub const LARGE_TRANSFER_THRESHOLD: u64 = 1_073_741_824;

/// Estimate total pack transfer bytes from commit count.
///
/// Uses `commit_count` × average commit size × index ratio to produce a rough
/// byte estimate for the combined commit + index transfer.
pub fn estimate_pack_bytes(commit_count: u32) -> u64 {
    let commit_est = commit_count as u64 * AVG_COMMIT_BYTES;
    let index_est = (commit_est as f64 * INDEX_TO_COMMIT_SIZE_RATIO) as u64;
    commit_est + index_est
}

// ============================================================================
// Helpers
// ============================================================================

impl PackRequest {
    /// Create a pack request for commits + txn blobs only.
    pub fn commits(want: Vec<ContentId>, have: Vec<ContentId>) -> Self {
        Self {
            protocol: PACK_PROTOCOL.to_string(),
            want,
            have,
            want_index_root_id: None,
            have_index_root_id: None,
            include_indexes: false,
            include_txns: true,
        }
    }

    /// Create a pack request for commits only (no txn blobs, no indexes).
    pub fn commits_no_txns(want: Vec<ContentId>, have: Vec<ContentId>) -> Self {
        Self {
            protocol: PACK_PROTOCOL.to_string(),
            want,
            have,
            want_index_root_id: None,
            have_index_root_id: None,
            include_indexes: false,
            include_txns: false,
        }
    }

    /// Create a pack request that includes index artifacts.
    pub fn with_indexes(
        want: Vec<ContentId>,
        have: Vec<ContentId>,
        want_index_root_id: ContentId,
        have_index_root_id: Option<ContentId>,
    ) -> Self {
        Self {
            protocol: PACK_PROTOCOL.to_string(),
            want,
            have,
            want_index_root_id: Some(want_index_root_id),
            have_index_root_id,
            include_indexes: true,
            include_txns: true,
        }
    }

    /// Disable txn-blob transfer on an existing request. Returns `self` for chaining.
    pub fn without_txns(mut self) -> Self {
        self.include_txns = false;
        self
    }
}

impl PackHeader {
    /// Create a commits-only header.
    ///
    /// `include_txns` controls whether the stream will carry referenced
    /// transaction blobs; it only affects the `capabilities` list on the
    /// header (purely informational for the client).
    pub fn commits_only(commit_count: Option<u32>, include_txns: bool) -> Self {
        let capabilities = if include_txns {
            vec!["commits".to_string(), "txns".to_string()]
        } else {
            vec!["commits".to_string()]
        };
        Self {
            protocol: PACK_PROTOCOL.to_string(),
            capabilities,
            server_max_frame_bytes: Some(DEFAULT_MAX_PAYLOAD),
            commit_count,
            index_artifact_count: None,
            estimated_total_bytes: 0,
        }
    }

    /// Create a header that includes index artifacts.
    pub fn with_indexes(
        commit_count: Option<u32>,
        index_artifact_count: Option<u32>,
        estimated_total_bytes: u64,
        include_txns: bool,
    ) -> Self {
        let mut capabilities = vec!["commits".to_string()];
        if include_txns {
            capabilities.push("txns".to_string());
        }
        capabilities.push("indexes".to_string());
        Self {
            protocol: PACK_PROTOCOL.to_string(),
            capabilities,
            server_max_frame_bytes: Some(DEFAULT_MAX_PAYLOAD),
            commit_count,
            index_artifact_count,
            estimated_total_bytes,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content_kind::ContentKind;

    fn sample_cid(kind: ContentKind, data: &[u8]) -> ContentId {
        ContentId::new(kind, data)
    }

    // --- Preamble ---

    #[test]
    fn test_preamble_roundtrip() {
        let mut buf = Vec::new();
        write_stream_preamble(&mut buf);
        assert_eq!(buf.len(), PREAMBLE_SIZE);
        let consumed = read_stream_preamble(&buf).unwrap();
        assert_eq!(consumed, PREAMBLE_SIZE);
    }

    #[test]
    fn test_preamble_invalid_magic() {
        let err = read_stream_preamble(b"BADM\x01").unwrap_err();
        assert!(matches!(err, PackError::InvalidMagic));
    }

    #[test]
    fn test_preamble_unsupported_version() {
        let err = read_stream_preamble(b"FPK1\x02").unwrap_err();
        assert!(matches!(err, PackError::UnsupportedVersion(2)));
    }

    #[test]
    fn test_preamble_incomplete() {
        let err = read_stream_preamble(b"FPK").unwrap_err();
        assert!(matches!(err, PackError::Incomplete(PREAMBLE_SIZE)));
    }

    // --- Header frame ---

    #[test]
    fn test_header_frame_roundtrip() {
        let header = PackHeader::commits_only(Some(5), true);
        let mut buf = Vec::new();
        encode_header_frame(&header, &mut buf);

        let (frame, consumed) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(consumed, buf.len());
        match frame {
            PackFrame::Header(h) => {
                assert_eq!(h.protocol, PACK_PROTOCOL);
                assert_eq!(h.capabilities, vec!["commits", "txns"]);
                assert_eq!(h.commit_count, Some(5));
                assert_eq!(h.index_artifact_count, None);
            }
            _ => panic!("expected Header frame"),
        }
    }

    #[test]
    fn test_header_frame_no_txns() {
        let header = PackHeader::commits_only(Some(5), false);
        let mut buf = Vec::new();
        encode_header_frame(&header, &mut buf);

        let (frame, _) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        match frame {
            PackFrame::Header(h) => {
                assert_eq!(h.capabilities, vec!["commits"]);
            }
            _ => panic!("expected Header frame"),
        }
    }

    #[test]
    fn test_header_with_indexes_roundtrip() {
        let header = PackHeader::with_indexes(Some(3), Some(100), 0, true);
        let mut buf = Vec::new();
        encode_header_frame(&header, &mut buf);

        let (frame, _) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        match frame {
            PackFrame::Header(h) => {
                assert_eq!(h.capabilities, vec!["commits", "txns", "indexes"]);
                assert_eq!(h.commit_count, Some(3));
                assert_eq!(h.index_artifact_count, Some(100));
            }
            _ => panic!("expected Header frame"),
        }
    }

    // --- Data frame ---

    #[test]
    fn test_data_frame_roundtrip() {
        let cid = sample_cid(ContentKind::Commit, b"commit data");
        let payload = b"raw commit bytes here";
        let mut buf = Vec::new();
        encode_data_frame(&cid, payload, &mut buf);

        let (frame, consumed) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(consumed, buf.len());
        match frame {
            PackFrame::Data {
                cid: decoded_cid,
                payload: decoded_payload,
            } => {
                assert_eq!(decoded_cid, cid);
                assert_eq!(decoded_payload, payload);
            }
            _ => panic!("expected Data frame"),
        }
    }

    #[test]
    fn test_data_frame_various_content_kinds() {
        for (kind, data) in [
            (ContentKind::Commit, &b"commit"[..]),
            (ContentKind::Txn, &b"txn"[..]),
            (ContentKind::IndexRoot, &b"index root"[..]),
            (ContentKind::IndexBranch, &b"branch"[..]),
            (ContentKind::IndexLeaf, &b"leaf"[..]),
            (
                ContentKind::DictBlob {
                    dict: crate::content_kind::DictKind::Graphs,
                },
                &b"dict blob"[..],
            ),
        ] {
            let cid = sample_cid(kind, data);
            let mut buf = Vec::new();
            encode_data_frame(&cid, b"payload bytes", &mut buf);

            let (frame, consumed) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
            assert_eq!(consumed, buf.len());
            match frame {
                PackFrame::Data {
                    cid: decoded_cid, ..
                } => assert_eq!(decoded_cid, cid),
                _ => panic!("expected Data frame for kind {kind:?}"),
            }
        }
    }

    #[test]
    fn test_data_frame_empty_payload() {
        let cid = sample_cid(ContentKind::Txn, b"txn");
        let mut buf = Vec::new();
        encode_data_frame(&cid, &[], &mut buf);

        let (frame, consumed) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(consumed, buf.len());
        match frame {
            PackFrame::Data { payload, .. } => assert!(payload.is_empty()),
            _ => panic!("expected Data frame"),
        }
    }

    // --- Error frame ---

    #[test]
    fn test_error_frame_roundtrip() {
        let message = "something went wrong: ledger not found";
        let mut buf = Vec::new();
        encode_error_frame(message, &mut buf);

        let (frame, consumed) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(consumed, buf.len());
        match frame {
            PackFrame::Error(msg) => assert_eq!(msg, message),
            _ => panic!("expected Error frame"),
        }
    }

    #[test]
    fn test_error_frame_empty_message() {
        let mut buf = Vec::new();
        encode_error_frame("", &mut buf);

        let (frame, _) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        match frame {
            PackFrame::Error(msg) => assert!(msg.is_empty()),
            _ => panic!("expected Error frame"),
        }
    }

    // --- Manifest frame ---

    #[test]
    fn test_manifest_frame_roundtrip() {
        let manifest = serde_json::json!({
            "phase": "indexes",
            "root_id": "bafytest123",
            "artifact_count": 42
        });
        let mut buf = Vec::new();
        encode_manifest_frame(&manifest, &mut buf);

        let (frame, consumed) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(consumed, buf.len());
        match frame {
            PackFrame::Manifest(v) => {
                assert_eq!(v["phase"], "indexes");
                assert_eq!(v["artifact_count"], 42);
            }
            _ => panic!("expected Manifest frame"),
        }
    }

    // --- End frame ---

    #[test]
    fn test_end_frame_roundtrip() {
        let mut buf = Vec::new();
        encode_end_frame(&mut buf);
        assert_eq!(buf.len(), 1);

        let (frame, consumed) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(consumed, 1);
        assert!(matches!(frame, PackFrame::End));
    }

    // --- Full stream sequence ---

    #[test]
    fn test_full_stream_sequence() {
        let mut buf = Vec::new();

        write_stream_preamble(&mut buf);
        encode_header_frame(
            &PackHeader::with_indexes(Some(2), Some(1), 0, true),
            &mut buf,
        );
        encode_data_frame(
            &sample_cid(ContentKind::Commit, b"c1"),
            b"commit bytes",
            &mut buf,
        );
        encode_data_frame(&sample_cid(ContentKind::Txn, b"t1"), b"txn bytes", &mut buf);
        encode_manifest_frame(
            &serde_json::json!({"phase": "indexes", "artifact_count": 1}),
            &mut buf,
        );
        encode_data_frame(
            &sample_cid(ContentKind::IndexLeaf, b"l1"),
            b"leaf bytes",
            &mut buf,
        );
        encode_end_frame(&mut buf);

        // Decode
        let mut pos = read_stream_preamble(&buf).unwrap();
        let mut frames = Vec::new();
        loop {
            let (frame, consumed) = decode_frame(&buf[pos..], DEFAULT_MAX_PAYLOAD).unwrap();
            pos += consumed;
            let is_end = matches!(frame, PackFrame::End);
            frames.push(frame);
            if is_end {
                break;
            }
        }

        assert_eq!(pos, buf.len());
        assert_eq!(frames.len(), 6);
        assert!(matches!(frames[0], PackFrame::Header(_)));
        assert!(matches!(frames[1], PackFrame::Data { .. }));
        assert!(matches!(frames[2], PackFrame::Data { .. }));
        assert!(matches!(frames[3], PackFrame::Manifest(_)));
        assert!(matches!(frames[4], PackFrame::Data { .. }));
        assert!(matches!(frames[5], PackFrame::End));
    }

    // --- Error conditions ---

    #[test]
    fn test_decode_incomplete_header() {
        let header = PackHeader::commits_only(None, true);
        let mut buf = Vec::new();
        encode_header_frame(&header, &mut buf);

        // Only type + partial length bytes
        let err = decode_frame(&buf[..3], DEFAULT_MAX_PAYLOAD).unwrap_err();
        assert!(matches!(err, PackError::Incomplete(5)));
    }

    #[test]
    fn test_decode_incomplete_data_payload() {
        let cid = sample_cid(ContentKind::Commit, b"test");
        let mut buf = Vec::new();
        encode_data_frame(&cid, b"hello world", &mut buf);

        // Truncate partway through the payload
        let err = decode_frame(&buf[..buf.len() - 3], DEFAULT_MAX_PAYLOAD).unwrap_err();
        assert!(matches!(err, PackError::Incomplete(_)));
    }

    #[test]
    fn test_decode_empty_buffer() {
        let err = decode_frame(&[], DEFAULT_MAX_PAYLOAD).unwrap_err();
        assert!(matches!(err, PackError::Incomplete(1)));
    }

    #[test]
    fn test_decode_invalid_frame_type() {
        let err = decode_frame(&[0x42], DEFAULT_MAX_PAYLOAD).unwrap_err();
        assert!(matches!(err, PackError::InvalidFrameType(0x42)));
    }

    #[test]
    fn test_payload_too_large() {
        let mut buf = vec![FRAME_HEADER];
        let huge_len: u32 = 1024;
        buf.extend_from_slice(&huge_len.to_le_bytes());
        buf.extend(vec![0u8; huge_len as usize]);

        let err = decode_frame(&buf, 512).unwrap_err();
        assert!(matches!(
            err,
            PackError::PayloadTooLarge {
                size: 1024,
                max: 512
            }
        ));
    }

    #[test]
    fn test_cid_too_large() {
        let mut buf = vec![FRAME_DATA];
        buf.extend_from_slice(&200u16.to_le_bytes());

        let err = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap_err();
        assert!(matches!(err, PackError::CidTooLarge(200)));
    }

    #[test]
    fn test_invalid_cid_bytes() {
        let mut buf = vec![FRAME_DATA];
        buf.extend_from_slice(&5u16.to_le_bytes());
        buf.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]); // garbage
        buf.extend_from_slice(&0u32.to_le_bytes());

        let err = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap_err();
        assert!(matches!(err, PackError::InvalidCid(_)));
    }

    // --- Serde for request/header ---

    #[test]
    fn test_pack_request_serde_commits_only() {
        let cid1 = sample_cid(ContentKind::Commit, b"head");
        let cid2 = sample_cid(ContentKind::Commit, b"base");
        let req = PackRequest::commits(vec![cid1.clone()], vec![cid2.clone()]);

        let json = serde_json::to_string(&req).unwrap();
        let parsed: PackRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.protocol, PACK_PROTOCOL);
        assert_eq!(parsed.want, vec![cid1]);
        assert_eq!(parsed.have, vec![cid2]);
        assert!(!parsed.include_indexes);
        assert!(parsed.include_txns);
        assert!(parsed.want_index_root_id.is_none());
    }

    #[test]
    fn test_pack_request_serde_with_indexes() {
        let want_head = sample_cid(ContentKind::Commit, b"head");
        let want_idx = sample_cid(ContentKind::IndexRoot, b"idx-want");
        let have_idx = sample_cid(ContentKind::IndexRoot, b"idx-have");
        let req = PackRequest::with_indexes(
            vec![want_head],
            vec![],
            want_idx.clone(),
            Some(have_idx.clone()),
        );

        let json = serde_json::to_string(&req).unwrap();
        let parsed: PackRequest = serde_json::from_str(&json).unwrap();
        assert!(parsed.include_indexes);
        assert_eq!(parsed.want_index_root_id, Some(want_idx));
        assert_eq!(parsed.have_index_root_id, Some(have_idx));
    }

    #[test]
    fn test_pack_header_serde() {
        let header = PackHeader::commits_only(Some(10), true);
        let json = serde_json::to_string(&header).unwrap();
        let parsed: PackHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.protocol, PACK_PROTOCOL);
        assert_eq!(parsed.commit_count, Some(10));
        assert!(parsed.server_max_frame_bytes.is_some());
    }

    #[test]
    fn test_pack_request_serde_no_txns() {
        let cid = sample_cid(ContentKind::Commit, b"head");
        let req = PackRequest::commits_no_txns(vec![cid.clone()], vec![]);

        let json = serde_json::to_string(&req).unwrap();
        let parsed: PackRequest = serde_json::from_str(&json).unwrap();
        assert!(!parsed.include_indexes);
        assert!(!parsed.include_txns);
        assert_eq!(parsed.want, vec![cid]);
    }

    #[test]
    fn test_pack_request_without_txns_builder() {
        let cid = sample_cid(ContentKind::IndexRoot, b"idx");
        let req = PackRequest::with_indexes(vec![], vec![], cid, None).without_txns();
        assert!(req.include_indexes);
        assert!(!req.include_txns);
    }

    // --- Trailing / consecutive frames ---

    #[test]
    fn test_decode_with_trailing_bytes() {
        let mut buf = Vec::new();
        encode_end_frame(&mut buf);
        buf.extend_from_slice(b"extra trailing bytes");

        let (frame, consumed) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(consumed, 1);
        assert!(matches!(frame, PackFrame::End));
        assert_eq!(&buf[consumed..], b"extra trailing bytes");
    }

    #[test]
    fn test_decode_two_consecutive_data_frames() {
        let cid1 = sample_cid(ContentKind::Commit, b"c1");
        let cid2 = sample_cid(ContentKind::Txn, b"t1");
        let mut buf = Vec::new();
        encode_data_frame(&cid1, b"commit bytes", &mut buf);
        encode_data_frame(&cid2, b"txn bytes", &mut buf);

        let (frame1, n1) = decode_frame(&buf, DEFAULT_MAX_PAYLOAD).unwrap();
        match &frame1 {
            PackFrame::Data { cid, .. } => assert_eq!(*cid, cid1),
            _ => panic!("expected Data frame"),
        }

        let (frame2, n2) = decode_frame(&buf[n1..], DEFAULT_MAX_PAYLOAD).unwrap();
        match &frame2 {
            PackFrame::Data { cid, .. } => assert_eq!(*cid, cid2),
            _ => panic!("expected Data frame"),
        }

        assert_eq!(n1 + n2, buf.len());
    }
}
