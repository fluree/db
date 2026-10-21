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
/// SHA2-512 multihash code (standard).
const SHA2_512: u64 = 0x13;

// ============================================================================
// HashAlgorithm
// ============================================================================

/// The content hash a [`ContentId`] is minted with.
///
/// The CID's multihash names its algorithm, so verification follows the
/// CID being checked, never the writer's current choice. SHA2-256 is the
/// default; SHA2-512 is the one alternative. A new variant needs its code,
/// digest length, multicodec name and hash; a circuit hash (Poseidon) also
/// needs a specified encoding of bytes into field elements.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum HashAlgorithm {
    #[default]
    Sha2_256,
    Sha2_512,
}

impl HashAlgorithm {
    /// Every algorithm this build reads and writes.
    pub const SUPPORTED: [Self; 2] = [Self::Sha2_256, Self::Sha2_512];

    /// The multihash code.
    pub const fn code(self) -> u64 {
        match self {
            Self::Sha2_256 => SHA2_256,
            Self::Sha2_512 => SHA2_512,
        }
    }

    /// The digest length in bytes.
    pub const fn digest_len(self) -> usize {
        match self {
            Self::Sha2_256 => 32,
            Self::Sha2_512 => 64,
        }
    }

    /// The multicodec table name (`sha2-256`, `sha2-512`).
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sha2_256 => "sha2-256",
            Self::Sha2_512 => "sha2-512",
        }
    }

    /// The algorithm for a multihash code and digest length; `None` for an
    /// unsupported code or a digest of the wrong length.
    pub fn from_multihash(code: u64, digest_len: usize) -> Option<Self> {
        let alg = match code {
            SHA2_256 => Self::Sha2_256,
            SHA2_512 => Self::Sha2_512,
            _ => return None,
        };
        (alg.digest_len() == digest_len).then_some(alg)
    }

    fn multihash(self, bytes: &[u8]) -> Multihash<64> {
        let wrapped = match self {
            Self::Sha2_256 => Multihash::wrap(SHA2_256, &sha2::Sha256::digest(bytes)),
            Self::Sha2_512 => Multihash::wrap(SHA2_512, &sha2::Sha512::digest(bytes)),
        };
        // Both digests fit in Multihash<64>.
        wrapped.expect("digest fits in Multihash<64>")
    }
}

impl fmt::Display for HashAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl serde::Serialize for HashAlgorithm {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> serde::Deserialize<'de> for HashAlgorithm {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl FromStr for HashAlgorithm {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "sha2-256" | "sha256" => Ok(Self::Sha2_256),
            "sha2-512" | "sha512" => Ok(Self::Sha2_512),
            other => Err(Error::storage(format!(
                "unsupported content hash algorithm {other:?} (supported: sha2-256, sha2-512)"
            ))),
        }
    }
}

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
        Self::new_with(HashAlgorithm::Sha2_256, kind, bytes)
    }

    /// Create a `ContentId` by hashing `bytes` with `alg`.
    pub fn new_with(alg: HashAlgorithm, kind: ContentKind, bytes: &[u8]) -> Self {
        Self(Cid::new_v1(kind.to_codec(), alg.multihash(bytes)))
    }

    /// The CID of `bytes` minted the way this one was: the same algorithm
    /// and codec. `None` when this CID's hash is unsupported.
    pub fn rehash(&self, bytes: &[u8]) -> Option<Self> {
        let alg = self.hash_algorithm()?;
        Some(Self(Cid::new_v1(self.codec(), alg.multihash(bytes))))
    }

    /// The algorithm this CID was minted with; `None` for an unsupported
    /// multihash code or a digest of the wrong length.
    pub fn hash_algorithm(&self) -> Option<HashAlgorithm> {
        let mh = self.0.hash();
        HashAlgorithm::from_multihash(mh.code(), mh.digest().len())
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
    /// Hashes the full bytes with this CID's own algorithm, which is correct
    /// for all content kinds including commit blobs (v4 format). An
    /// unsupported algorithm or digest length never verifies.
    pub fn verify(&self, bytes: &[u8]) -> bool {
        match self.hash_algorithm() {
            Some(alg) => alg.multihash(bytes) == *self.0.hash(),
            None => false,
        }
    }

    /// The digest as storage addresses spell it: bare hex for SHA2-256 (the
    /// legacy layout), and `<algorithm>-<hex>` otherwise, so the algorithm
    /// survives a round trip through a path or key.
    /// [`Self::from_address_digest`] parses it back.
    pub fn address_digest(&self) -> String {
        let mh = self.0.hash();
        let hex = hex::encode(mh.digest());
        match self.hash_algorithm() {
            Some(HashAlgorithm::Sha2_256) => hex,
            Some(alg) => format!("{}-{hex}", alg.name()),
            None => format!("mh{:x}-{hex}", mh.code()),
        }
    }

    /// Parse a string as a CID, but only in the canonical spelling
    /// [`Display`](std::fmt::Display) produces.
    ///
    /// Use this, not [`FromStr`], wherever the input might instead be a hex
    /// digest — which is how commits and index nodes are keyed on disk and in
    /// the indexes, so it is most places a user types one in.
    ///
    /// `FromStr` goes through multibase, where a leading `f` means base16. A
    /// bare hex digest of even length after that `f` therefore decodes, and if
    /// the bytes happen to form a valid CIDv1 it parses — `f01550003000102` is
    /// all hex and yields `bafkqaayaaeba`, because the identity multihash makes
    /// a valid CID out of very few bytes. Taking that as a CID silently
    /// addresses content nobody named. Re-encoding and comparing rejects it,
    /// because `Display` always emits base32-lower.
    ///
    /// ```
    /// use fluree_db_core::{ContentId, ContentKind};
    /// use std::str::FromStr;
    ///
    /// let id = ContentId::new(ContentKind::Commit, b"payload");
    /// assert_eq!(ContentId::parse_canonical(&id.to_string()), Some(id));
    ///
    /// // All hex, and FromStr accepts it — but it is not a CID anyone wrote.
    /// assert!(ContentId::from_str("f01550003000102").is_ok());
    /// assert_eq!(ContentId::parse_canonical("f01550003000102"), None);
    /// ```
    pub fn parse_canonical(s: &str) -> Option<Self> {
        let cid: Self = s.parse().ok()?;
        (cid.to_string() == s).then_some(cid)
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

    /// Construct a `ContentId` from an [address digest](Self::address_digest)
    /// and multicodec: 64 hex characters for SHA2-256, or
    /// `<algorithm>-<hex>` for another supported algorithm.
    ///
    /// Returns `None` for an unsupported algorithm, malformed hex, or a
    /// digest of the wrong length for its algorithm.
    pub fn from_address_digest(codec: u64, address_digest: &str) -> Option<Self> {
        let (alg, hex_digest) = match address_digest.rsplit_once('-') {
            Some((name, hex_digest)) => (name.parse::<HashAlgorithm>().ok()?, hex_digest),
            None => (HashAlgorithm::Sha2_256, address_digest),
        };
        if alg == HashAlgorithm::Sha2_256 && address_digest.contains('-') {
            // SHA2-256 addresses are bare: a tagged spelling names no object.
            return None;
        }
        let digest = hex::decode(hex_digest).ok()?;
        if digest.len() != alg.digest_len() {
            return None;
        }
        let mh = Multihash::<64>::wrap(alg.code(), &digest).ok()?;
        Some(Self(Cid::new_v1(codec, mh)))
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
    fn test_address_digest() {
        let data = b"digest hex test";
        let id = ContentId::new(ContentKind::Commit, data);
        let hex_str = id.address_digest();

        // SHA2-256 addresses stay bare hex: 32 bytes → 64 hex chars.
        assert_eq!(hex_str.len(), 64);
        let expected = hex::encode(sha2::Sha256::digest(data));
        assert_eq!(hex_str, expected);
        assert_eq!(
            ContentId::from_address_digest(id.codec(), &hex_str),
            Some(id)
        );
    }

    #[test]
    fn the_default_is_byte_identical_to_sha2_256() {
        let data = b"default";
        let id = ContentId::new(ContentKind::Commit, data);
        assert_eq!(
            id,
            ContentId::new_with(HashAlgorithm::Sha2_256, ContentKind::Commit, data)
        );
        let mh = Multihash::<64>::wrap(SHA2_256, &sha2::Sha256::digest(data)).unwrap();
        assert_eq!(
            id.to_bytes(),
            Cid::new_v1(ContentKind::Commit.to_codec(), mh).to_bytes()
        );
        assert_eq!(id.hash_algorithm(), Some(HashAlgorithm::Sha2_256));
    }

    #[test]
    fn sha2_512_mints_verifies_and_round_trips() {
        let data = b"sha2-512 payload";
        let id = ContentId::new_with(HashAlgorithm::Sha2_512, ContentKind::Commit, data);
        assert_eq!(id.hash_algorithm(), Some(HashAlgorithm::Sha2_512));
        assert_eq!(id.as_cid().hash().digest().len(), 64);
        assert!(id.verify(data));
        assert!(!id.verify(b"other"));
        // Differs from the SHA2-256 CID of the same bytes.
        assert_ne!(id, ContentId::new(ContentKind::Commit, data));
        // Binary, string and serde forms keep the algorithm.
        assert_eq!(ContentId::from_bytes(&id.to_bytes()).unwrap(), id);
        assert_eq!(id.to_string().parse::<ContentId>().unwrap(), id);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(serde_json::from_str::<ContentId>(&json).unwrap(), id);
        let bin = postcard::to_allocvec(&id).unwrap();
        assert_eq!(postcard::from_bytes::<ContentId>(&bin).unwrap(), id);
        // The address digest is tagged and parses back to the same CID.
        let address = id.address_digest();
        assert!(address.starts_with("sha2-512-"), "{address}");
        assert_eq!(address.len(), "sha2-512-".len() + 128);
        assert_eq!(
            ContentId::from_address_digest(id.codec(), &address),
            Some(id.clone())
        );
        // Rehashing follows the CID's own algorithm.
        assert_eq!(id.rehash(data), Some(id));
    }

    #[test]
    fn unsupported_algorithms_and_lengths_are_rejected() {
        let codec = ContentKind::Commit.to_codec();
        // An unknown multihash code (identity) never verifies.
        let identity = Multihash::<64>::wrap(0x00, b"abc").unwrap();
        let id = ContentId::from_cid(Cid::new_v1(codec, identity));
        assert_eq!(id.hash_algorithm(), None);
        assert!(!id.verify(b"abc"));
        assert_eq!(id.rehash(b"abc"), None);
        // A SHA2-256 code with a 64-byte digest is not SHA2-256.
        let long = Multihash::<64>::wrap(SHA2_256, &[7u8; 64]).unwrap();
        let id = ContentId::from_cid(Cid::new_v1(codec, long));
        assert_eq!(id.hash_algorithm(), None);
        assert!(!id.verify(&[7u8; 64]));
        // Address digests of the wrong length or algorithm do not parse.
        let short = "ab".repeat(31);
        assert_eq!(ContentId::from_address_digest(codec, &short), None);
        let sha512_as_bare = "ab".repeat(64);
        assert_eq!(ContentId::from_address_digest(codec, &sha512_as_bare), None);
        let sha256_tagged = format!("sha2-256-{}", "ab".repeat(32));
        assert_eq!(ContentId::from_address_digest(codec, &sha256_tagged), None);
        let truncated = format!("sha2-512-{}", "ab".repeat(32));
        assert_eq!(ContentId::from_address_digest(codec, &truncated), None);
        let unknown = format!("blake3-{}", "ab".repeat(32));
        assert_eq!(ContentId::from_address_digest(codec, &unknown), None);
        assert!("md5".parse::<HashAlgorithm>().is_err());
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
