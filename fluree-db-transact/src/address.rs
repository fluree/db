//! Address helpers
//!
//! Writes are storage-owned (see `ContentAddressedWrite`), but we still need to
//! parse commit IDs from commit addresses for linking previous refs.

/// Parse a commit ID from a storage address
///
/// Extracts the hash from an address like `fluree:{method}://ledger/commit/<hash>.fcv2`
/// and returns `sha256:<hash>`.
pub fn parse_commit_id(address: &str) -> Option<String> {
    // Extract path portion after :// if present (supports `fluree:*://...` and raw `*://...`)
    let path = if let Some(rest) = address.strip_prefix("fluree:") {
        let pos = rest.find("://")?;
        &rest[pos + 3..]
    } else if let Some(pos) = address.find("://") {
        &address[pos + 3..]
    } else {
        // Not a Fluree address, cannot reliably parse
        return None;
    };

    // Strip any file extension from the last path segment
    let commit_part = path.rsplit('/').next()?;
    let dot = commit_part.rfind('.')?;
    if dot == 0 {
        return None;
    }
    let hash = &commit_part[..dot];

    // Validate it looks like a hex hash
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(format!("sha256:{hash}"))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_commit_id() {
        let address = "fluree:file://test/main/commit/abc123def456abc123def456abc123def456abc123def456abc123def456abcd.fcv2";
        let id = parse_commit_id(address);

        assert_eq!(
            id,
            Some(
                "sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abcd"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_parse_commit_id_legacy_json() {
        // Legacy .json extension should still work
        let address = "fluree:file://ledger/commit/abc123def456abc123def456abc123def456abc123def456abc123def456abcd.json".to_string();
        let parsed = parse_commit_id(&address);

        assert_eq!(
            parsed,
            Some(
                "sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abcd"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_parse_commit_id_s3_method() {
        let address = "fluree:s3://test/main/commit/abc123def456abc123def456abc123def456abc123def456abc123def456abcd.fcv2";
        let id = parse_commit_id(address);

        assert_eq!(
            id,
            Some(
                "sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abcd"
                    .to_string()
            )
        );
    }
}
