//! Content identifier (CIDv1) for storage-agnostic object identity.
//!
//! `ContentId` wraps a CIDv1 from the multiformats ecosystem, using
//! Fluree-specific multicodec values (private-use range) to encode
//! the content kind (commit, txn, index root, etc.) directly in the ID.
//!
//! ## String form
//!
//! The canonical string representation is **base32-lower** (multibase),
//! producing the familiar `bafy...` / `bafk...` prefixes. This is what
//! appears in JSON, logs, and APIs.
//!
//! ## Binary form
//!
//! The compact binary form is the standard CID binary encoding
//! (varint version + varint codec + multihash bytes), used in
//! commit format v3, pack streams, and binary indexes.

use crate::content_kind::ContentKind;
use crate::error::{Error, Result};
use cid::Cid;
use multihash::Multihash;
use sha2::Digest;
use std::fmt;
use std::str::FromStr;

/// SHA2-256 multihash code (standard).
const SHA2_256: u64 = 0x12;

// ============================================================================
// ContentId
// ============================================================================

/// Content identifier wrapping CIDv1 (multiformats).
///
/// The canonical identity for all immutable objects in Fluree: commits,
/// transactions, index roots, leaves, dictionary blobs, etc.
///
/// # String form
///
/// Base32-lower multibase (e.g., `"bafybeig..."`). Used in JSON, logs, APIs.
///
/// # Binary form
///
/// Standard CID binary: varint(version=1) + varint(codec) + multihash.
/// Used in commit format v3, pack streams, and binary indexes.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ContentId(Cid);

/// Type alias: commit content ID.
pub type CommitId = ContentId;

/// Type alias: transaction blob content ID.
pub type TxnId = ContentId;

/// Type alias: index root content ID.
pub type IndexRootId = ContentId;

impl ContentId {
    /// Create a new `ContentId` by hashing `bytes` with SHA2-256 and
    /// tagging with the multicodec for `kind`.
    pub fn new(kind: ContentKind, bytes: &[u8]) -> Self {
        let digest = sha2::Sha256::digest(bytes);
        // SHA2-256 produces 32 bytes, MH_SIZE=64 — wrap always succeeds.
        let mh = Multihash::<64>::wrap(SHA2_256, &digest)
            .expect("SHA2-256 digest fits in Multihash<64>");
        let cid = Cid::new_v1(kind.to_codec(), mh);
        Self(cid)
    }

    /// Wrap an existing `Cid` as a `ContentId`.
    pub fn from_cid(cid: Cid) -> Self {
        Self(cid)
    }

    /// Borrow the inner `Cid`.
    pub fn as_cid(&self) -> &Cid {
        &self.0
    }

    /// The multicodec value stored in this CID.
    pub fn codec(&self) -> u64 {
        self.0.codec()
    }

    /// Attempt to map the codec back to a `ContentKind`.
    ///
    /// Returns `None` for unknown codecs. For dict blobs, returns a
    /// placeholder `DictKind` — callers needing the exact sub-kind must
    /// inspect the bytes.
    pub fn content_kind(&self) -> Option<ContentKind> {
        ContentKind::from_codec(self.0.codec())
    }

    /// Serialize to the standard CID binary form.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    /// Parse from CID binary bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let cid = Cid::try_from(bytes).map_err(|e| Error::storage(format!("invalid CID: {e}")))?;
        Ok(Self(cid))
    }

    /// Re-hash `bytes` and check that the result matches this CID's
    /// multihash digest.
    ///
    /// Use this when accepting objects from untrusted sources (push,
    /// replication, `put_with_id`).
    ///
    /// Computes `SHA-256(full_bytes)`, which is correct for all content
    /// kinds including commit blobs (v4 format).
    pub fn verify(&self, bytes: &[u8]) -> bool {
        let mh = self.0.hash();
        if mh.code() != SHA2_256 {
            // We only support SHA2-256 verification currently.
            return false;
        }
        let digest = sha2::Sha256::digest(bytes);
        mh.digest() == digest.as_slice()
    }

    /// The hex-encoded multihash digest (without the code/length prefix).
    ///
    /// Useful for filesystem layout where the path includes the digest.
    pub fn digest_hex(&self) -> String {
        hex::encode(self.0.hash().digest())
    }

    /// Construct a `ContentId` from a raw SHA-256 digest and multicodec.
    ///
    /// Used for backward compatibility with v2 commit format, where the
    /// SHA-256 hash was computed over a subset of the blob and stored in
    /// the address string.
    pub fn from_sha256_digest(codec: u64, digest: &[u8; 32]) -> Self {
        let mh =
            Multihash::<64>::wrap(SHA2_256, digest).expect("SHA2-256 digest fits in Multihash<64>");
        Self(Cid::new_v1(codec, mh))
    }

    /// Construct a `ContentId` from a hex-encoded SHA-256 digest and multicodec.
    ///
    /// Returns `None` if the hex string is not exactly 64 hex characters.
    pub fn from_hex_digest(codec: u64, hex_digest: &str) -> Option<Self> {
        let bytes = hex::decode(hex_digest).ok()?;
        let digest: [u8; 32] = bytes.try_into().ok()?;
        Some(Self::from_sha256_digest(codec, &digest))
    }
}

// ============================================================================
// Display / FromStr / Debug
// ============================================================================

impl fmt::Display for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // CIDv1 Display uses base32-lower by default.
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentId({})", self.0)
    }
}

impl FromStr for ContentId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let cid =
            Cid::try_from(s).map_err(|e| Error::storage(format!("invalid CID string: {e}")))?;
        Ok(Self(cid))
    }
}

// ============================================================================
// Ord / PartialOrd (canonical byte ordering)
// ============================================================================

impl PartialOrd for ContentId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ContentId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_bytes().cmp(&other.to_bytes())
    }
}

// ============================================================================
// Serde (human-readable: string, binary: CID bytes)
// ============================================================================

impl serde::Serialize for ContentId {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            // JSON / human-readable: base32-lower string
            serializer.serialize_str(&self.to_string())
        } else {
            // postcard / CBOR / binary: raw CID bytes
            serializer.serialize_bytes(&self.to_bytes())
        }
    }
}

impl<'de> serde::Deserialize<'de> for ContentId {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            ContentId::from_str(&s).map_err(serde::de::Error::custom)
        } else {
            let bytes = <Vec<u8>>::deserialize(deserializer)?;
            ContentId::from_bytes(&bytes).map_err(serde::de::Error::custom)
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content_kind::*;

    #[test]
    fn test_new_and_verify() {
        let data = b"hello world";
        let id = ContentId::new(ContentKind::Commit, data);

        assert!(id.verify(data));
        assert!(!id.verify(b"wrong data"));
    }

    #[test]
    fn test_bytes_roundtrip() {
        let id = ContentId::new(ContentKind::Commit, b"test payload");
        let bytes = id.to_bytes();
        let parsed = ContentId::from_bytes(&bytes).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_string_roundtrip() {
        let id = ContentId::new(ContentKind::Txn, b"txn payload");
        let s = id.to_string();
        let parsed: ContentId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_different_kinds_different_cids() {
        let data = b"same data";
        let commit_id = ContentId::new(ContentKind::Commit, data);
        let txn_id = ContentId::new(ContentKind::Txn, data);
        let index_id = ContentId::new(ContentKind::IndexRoot, data);

        // Same bytes, different codecs → different CIDs
        assert_ne!(commit_id, txn_id);
        assert_ne!(commit_id, index_id);
        assert_ne!(txn_id, index_id);
    }

    #[test]
    fn test_codec_roundtrip() {
        assert_eq!(ContentKind::Commit.to_codec(), CODEC_FLUREE_COMMIT);
        assert_eq!(
            ContentKind::from_codec(CODEC_FLUREE_COMMIT),
            Some(ContentKind::Commit)
        );

        assert_eq!(ContentKind::Txn.to_codec(), CODEC_FLUREE_TXN);
        assert_eq!(
            ContentKind::from_codec(CODEC_FLUREE_TXN),
            Some(ContentKind::Txn)
        );

        assert_eq!(ContentKind::IndexRoot.to_codec(), CODEC_FLUREE_INDEX_ROOT);
        assert_eq!(
            ContentKind::from_codec(CODEC_FLUREE_INDEX_ROOT),
            Some(ContentKind::IndexRoot)
        );

        assert_eq!(
            ContentKind::IndexBranch.to_codec(),
            CODEC_FLUREE_INDEX_BRANCH
        );
        assert_eq!(
            ContentKind::from_codec(CODEC_FLUREE_INDEX_BRANCH),
            Some(ContentKind::IndexBranch)
        );

        assert_eq!(ContentKind::IndexLeaf.to_codec(), CODEC_FLUREE_INDEX_LEAF);
        assert_eq!(
            ContentKind::from_codec(CODEC_FLUREE_INDEX_LEAF),
            Some(ContentKind::IndexLeaf)
        );

        // DictBlob maps to single codec regardless of sub-kind
        let dict_kind = ContentKind::DictBlob {
            dict: crate::content_kind::DictKind::Graphs,
        };
        assert_eq!(dict_kind.to_codec(), CODEC_FLUREE_DICT_BLOB);
        let dict_kind2 = ContentKind::DictBlob {
            dict: crate::content_kind::DictKind::SubjectForward,
        };
        assert_eq!(dict_kind2.to_codec(), CODEC_FLUREE_DICT_BLOB);

        assert_eq!(ContentKind::GarbageRecord.to_codec(), CODEC_FLUREE_GARBAGE);
        assert_eq!(
            ContentKind::from_codec(CODEC_FLUREE_GARBAGE),
            Some(ContentKind::GarbageRecord)
        );

        assert_eq!(
            ContentKind::LedgerConfig.to_codec(),
            CODEC_FLUREE_LEDGER_CONFIG
        );
        assert_eq!(
            ContentKind::from_codec(CODEC_FLUREE_LEDGER_CONFIG),
            Some(ContentKind::LedgerConfig)
        );

        // Unknown codec
        assert_eq!(ContentKind::from_codec(0x0099_9999), None);
    }

    #[test]
    fn test_content_kind_from_cid() {
        let id = ContentId::new(ContentKind::Commit, b"data");
        assert_eq!(id.content_kind(), Some(ContentKind::Commit));

        let id = ContentId::new(ContentKind::IndexLeaf, b"data");
        assert_eq!(id.content_kind(), Some(ContentKind::IndexLeaf));
    }

    #[test]
    fn test_serde_json_roundtrip() {
        let id = ContentId::new(ContentKind::Commit, b"json test");
        let json = serde_json::to_string(&id).unwrap();
        // Should be a quoted string (base32-lower)
        assert!(json.starts_with('"'));
        assert!(json.ends_with('"'));

        let parsed: ContentId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_serde_postcard_roundtrip() {
        let id = ContentId::new(ContentKind::Commit, b"postcard test");
        let bytes = postcard::to_allocvec(&id).unwrap();
        let parsed: ContentId = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_ord_matches_byte_ordering() {
        let a = ContentId::new(ContentKind::Commit, b"aaa");
        let b = ContentId::new(ContentKind::Commit, b"bbb");
        let c = ContentId::new(ContentKind::Commit, b"ccc");

        // Ordering should be consistent and deterministic
        let mut ids = [c.clone(), a.clone(), b.clone()];
        ids.sort();

        // Verify it matches byte ordering
        let mut byte_sorted = [c.to_bytes(), a.to_bytes(), b.to_bytes()];
        byte_sorted.sort();

        let ids_bytes: Vec<Vec<u8>> = ids.iter().map(super::ContentId::to_bytes).collect();
        assert_eq!(ids_bytes, byte_sorted);
    }

    #[test]
    fn test_display_starts_with_b() {
        // CIDv1 base32-lower strings start with 'b'
        let id = ContentId::new(ContentKind::Commit, b"display test");
        let s = id.to_string();
        assert!(
            s.starts_with('b'),
            "CIDv1 base32 should start with 'b', got: {s}"
        );
    }

    #[test]
    fn test_debug_format() {
        let id = ContentId::new(ContentKind::Commit, b"debug test");
        let debug = format!("{id:?}");
        assert!(debug.starts_with("ContentId("));
        assert!(debug.ends_with(')'));
    }

    #[test]
    fn test_digest_hex() {
        let data = b"digest hex test";
        let id = ContentId::new(ContentKind::Commit, data);
        let hex_str = id.digest_hex();

        // SHA2-256 produces 32 bytes → 64 hex chars
        assert_eq!(hex_str.len(), 64);

        // Should match direct sha256
        let expected = hex::encode(sha2::Sha256::digest(data));
        assert_eq!(hex_str, expected);
    }

    #[test]
    fn test_hash_map_key() {
        use std::collections::HashMap;
        let id1 = ContentId::new(ContentKind::Commit, b"key1");
        let id2 = ContentId::new(ContentKind::Commit, b"key2");

        let mut map = HashMap::new();
        map.insert(id1.clone(), "value1");
        map.insert(id2.clone(), "value2");

        assert_eq!(map.get(&id1), Some(&"value1"));
        assert_eq!(map.get(&id2), Some(&"value2"));
    }
}
