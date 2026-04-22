//! Address parsing for IPFS storage.
//!
//! Fluree storage addresses have the form:
//!   `fluree:ipfs://{ledger_path}/{kind_dir}/{hash_hex}.{ext}`
//!
//! For example:
//!   `fluree:ipfs://mydb/main/commit/abcdef0123456789...64hex.fcv2`
//!   `fluree:ipfs://mydb/main/index/roots/abcdef0123456789...64hex.fir6`
//!
//! To retrieve from IPFS, we extract the SHA-256 hash hex from the filename
//! stem and construct a CID for `block_get`. Since Kubo resolves by multihash
//! (not full CID), we use the `raw` codec for retrieval — the codec in the
//! CID doesn't affect block lookup.

use crate::error::{IpfsStorageError, Result};
use cid::Cid;
use multihash::Multihash;

/// SHA2-256 multihash code.
const SHA2_256: u64 = 0x12;

/// `raw` multicodec (0x55) — universally supported by IPFS.
const RAW_CODEC: u64 = 0x55;

/// Extract the SHA-256 hash hex from a Fluree address.
///
/// Parses both `fluree:ipfs://path/{hash}.ext` and bare `path/{hash}.ext` forms.
/// Returns the 64-character hex string.
pub fn extract_hash_hex(address: &str) -> Result<&str> {
    // Strip the fluree:ipfs:// prefix if present
    let path = if let Some(rest) = address.strip_prefix("fluree:") {
        // fluree:ipfs://path or fluree:{id}:ipfs://path
        if let Some(pos) = rest.find("://") {
            &rest[pos + 3..]
        } else {
            return Err(IpfsStorageError::Other(format!(
                "invalid address format: {address}"
            )));
        }
    } else {
        address
    };

    // The hash is the filename stem (last path component without extension)
    let filename = path
        .rsplit('/')
        .next()
        .ok_or_else(|| IpfsStorageError::Other(format!("no filename in address: {address}")))?;

    // Strip extension
    let stem = filename
        .rsplit_once('.')
        .map(|(stem, _ext)| stem)
        .unwrap_or(filename);

    // Validate: SHA-256 hex is exactly 64 characters
    if stem.len() != 64 || !stem.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(IpfsStorageError::Other(format!(
            "expected 64-char hex hash in address, got '{}' (len={}) from: {}",
            stem,
            stem.len(),
            address
        )));
    }

    Ok(stem)
}

/// Construct a CID string from a SHA-256 hex digest for IPFS retrieval.
///
/// Uses `raw` codec (0x55) which is universally recognized by IPFS nodes.
/// Since Kubo resolves by multihash (not full CID), the codec doesn't matter
/// for block lookup — but using `raw` avoids any potential issues with
/// unknown codec names in tooling.
pub fn hash_hex_to_cid_string(hash_hex: &str) -> Result<String> {
    let digest = hex::decode(hash_hex)
        .map_err(|e| IpfsStorageError::Other(format!("invalid hex in hash: {e}")))?;

    let mh = Multihash::<64>::wrap(SHA2_256, &digest)
        .map_err(|e| IpfsStorageError::Other(format!("multihash wrap failed: {e}")))?;

    let cid = Cid::new_v1(RAW_CODEC, mh);
    Ok(cid.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_hash_from_fluree_address() {
        let hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let address = format!("fluree:ipfs://mydb/main/commit/{hash}.fcv2");
        assert_eq!(extract_hash_hex(&address).unwrap(), hash);
    }

    #[test]
    fn test_extract_hash_from_index_address() {
        let hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let address = format!("fluree:ipfs://mydb/main/index/roots/{hash}.fir6");
        assert_eq!(extract_hash_hex(&address).unwrap(), hash);
    }

    #[test]
    fn test_extract_hash_from_bare_path() {
        let hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let address = format!("mydb/main/commit/{hash}.fcv2");
        assert_eq!(extract_hash_hex(&address).unwrap(), hash);
    }

    #[test]
    fn test_reject_non_hex() {
        let address = "fluree:ipfs://mydb/main/commit/not-a-hash.fcv2";
        assert!(extract_hash_hex(address).is_err());
    }

    #[test]
    fn test_reject_wrong_length() {
        let address = "fluree:ipfs://mydb/main/commit/abcdef.fcv2";
        assert!(extract_hash_hex(address).is_err());
    }

    #[test]
    fn test_hash_to_cid_roundtrip() {
        let hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let cid_str = hash_hex_to_cid_string(hash).unwrap();
        // Should be a valid CIDv1 base32 string
        assert!(
            cid_str.starts_with('b'),
            "CIDv1 base32 starts with 'b': {cid_str}"
        );

        // Parse it back and verify the digest
        let cid = Cid::try_from(cid_str.as_str()).unwrap();
        assert_eq!(hex::encode(cid.hash().digest()), hash);
        assert_eq!(cid.codec(), RAW_CODEC);
    }
}
