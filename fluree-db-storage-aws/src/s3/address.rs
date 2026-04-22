//! Fluree address to S3 key conversion
//!
//! Handles the mapping between Fluree addresses (e.g., `fluree:s3://path/to/file.json`)
//! and S3 keys (e.g., `prefix/path/to/file.json`).

use crate::error::{AwsStorageError, Result};

/// Parse a Fluree address and extract the path component
///
/// Supported formats:
/// - `fluree:s3://path/to/file.json`
/// - `fluree:{identifier}:s3://path/to/file.json`
/// - `fluree:file://path/to/file.json` (extracts path, ignores method)
/// - `path/to/file.json` (raw path, used as-is)
pub fn parse_fluree_address(address: &str) -> Result<&str> {
    // Handle fluree: prefix formats
    if let Some(rest) = address.strip_prefix("fluree:") {
        // Find :// which marks the start of the path
        if let Some(path_start) = rest.find("://") {
            let path = &rest[path_start + 3..];
            if path.is_empty() {
                return Err(AwsStorageError::invalid_config(format!(
                    "Empty path in address: {address}"
                )));
            }
            return Ok(path);
        }
        // No :// found - might be just "fluree:path" which is invalid
        return Err(AwsStorageError::invalid_config(format!(
            "Invalid Fluree address format: {address}"
        )));
    }

    // Raw path - use as-is
    if address.is_empty() {
        return Err(AwsStorageError::invalid_config("Empty address"));
    }

    Ok(address)
}

/// Convert a Fluree address to an S3 key
///
/// If a prefix is configured, it's prepended to the path.
pub fn address_to_key(address: &str, prefix: Option<&str>) -> Result<String> {
    let path = parse_fluree_address(address)?;

    match prefix {
        Some(p) => {
            let p = p.trim_end_matches('/');
            Ok(format!("{p}/{path}"))
        }
        None => Ok(path.to_string()),
    }
}

/// Convert an S3 key back to a relative address
///
/// Strips the prefix if present.
pub fn key_to_address(key: &str, prefix: Option<&str>) -> String {
    match prefix {
        Some(p) => {
            let p = p.trim_end_matches('/');
            let prefix_with_slash = format!("{p}/");
            key.strip_prefix(&prefix_with_slash)
                .unwrap_or(key)
                .to_string()
        }
        None => key.to_string(),
    }
}

/// Normalize an ETag value
///
/// S3 ETags are often quoted (e.g., `"abc123"`).
/// Weak ETags have a `W/` prefix (e.g., `W/"abc123"`).
/// This function strips quotes and weak prefixes for comparison.
pub fn normalize_etag(etag: &str) -> String {
    let etag = etag.trim();

    // Strip weak ETag prefix
    let etag = etag.strip_prefix("W/").unwrap_or(etag);

    // Strip surrounding quotes
    let etag = etag.trim_matches('"');

    etag.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fluree_address_s3() {
        assert_eq!(
            parse_fluree_address("fluree:s3://mydb/main/index/root.json").unwrap(),
            "mydb/main/index/root.json"
        );
    }

    #[test]
    fn test_parse_fluree_address_with_identifier() {
        assert_eq!(
            parse_fluree_address("fluree:myid:s3://path/to/file.json").unwrap(),
            "path/to/file.json"
        );
    }

    #[test]
    fn test_parse_fluree_address_file() {
        assert_eq!(
            parse_fluree_address("fluree:file://data/ledger.json").unwrap(),
            "data/ledger.json"
        );
    }

    #[test]
    fn test_parse_fluree_address_raw() {
        assert_eq!(
            parse_fluree_address("some/raw/path.json").unwrap(),
            "some/raw/path.json"
        );
    }

    #[test]
    fn test_parse_fluree_address_empty_path() {
        assert!(parse_fluree_address("fluree:s3://").is_err());
    }

    #[test]
    fn test_address_to_key_with_prefix() {
        assert_eq!(
            address_to_key("fluree:s3://mydb/file.json", Some("ledgers")).unwrap(),
            "ledgers/mydb/file.json"
        );
    }

    #[test]
    fn test_address_to_key_prefix_trailing_slash() {
        assert_eq!(
            address_to_key("fluree:s3://mydb/file.json", Some("ledgers/")).unwrap(),
            "ledgers/mydb/file.json"
        );
    }

    #[test]
    fn test_address_to_key_no_prefix() {
        assert_eq!(
            address_to_key("fluree:s3://mydb/file.json", None).unwrap(),
            "mydb/file.json"
        );
    }

    #[test]
    fn test_key_to_address_with_prefix() {
        assert_eq!(
            key_to_address("ledgers/mydb/file.json", Some("ledgers")),
            "mydb/file.json"
        );
    }

    #[test]
    fn test_key_to_address_no_prefix() {
        assert_eq!(key_to_address("mydb/file.json", None), "mydb/file.json");
    }

    #[test]
    fn test_normalize_etag_quoted() {
        assert_eq!(normalize_etag("\"abc123\""), "abc123");
    }

    #[test]
    fn test_normalize_etag_weak() {
        assert_eq!(normalize_etag("W/\"abc123\""), "abc123");
    }

    #[test]
    fn test_normalize_etag_plain() {
        assert_eq!(normalize_etag("abc123"), "abc123");
    }

    #[test]
    fn test_normalize_etag_whitespace() {
        assert_eq!(normalize_etag("  \"abc123\"  "), "abc123");
    }
}
