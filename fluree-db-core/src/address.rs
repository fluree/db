//! Fluree address parsing utilities.
//!
//! Parses `fluree:` scheme addresses into their components:
//! - `fluree:<method>://<path>` - standard address
//! - `fluree:<identifier>:<method>://<path>` - address with identifier for routing
//!
//! # Examples
//!
//! ```
//! use fluree_db_core::address::{parse_fluree_address, extract_identifier};
//!
//! // Standard address (no identifier)
//! let parsed = parse_fluree_address("fluree:s3://mydb/main/commit/abc.fcv2").unwrap();
//! assert_eq!(parsed.identifier, None);
//! assert_eq!(parsed.method, "s3");
//! assert_eq!(parsed.path, "mydb/main/commit/abc.fcv2");
//!
//! // Address with identifier
//! let parsed = parse_fluree_address("fluree:commit-store:s3://mydb/main/commit/abc.fcv2").unwrap();
//! assert_eq!(parsed.identifier, Some("commit-store"));
//! assert_eq!(parsed.method, "s3");
//! assert_eq!(parsed.path, "mydb/main/commit/abc.fcv2");
//!
//! // Extract just the identifier for routing
//! assert_eq!(extract_identifier("fluree:myid:s3://path"), Some("myid"));
//! assert_eq!(extract_identifier("fluree:s3://path"), None);
//! ```

/// Parsed components of a Fluree address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedFlureeAddress<'a> {
    /// Optional identifier for address-based routing (e.g., "commit-store").
    /// Present when address is `fluree:<identifier>:<method>://...`.
    pub identifier: Option<&'a str>,
    /// Storage method (e.g., "s3", "file", "memory").
    pub method: &'a str,
    /// Path portion after `://`.
    pub path: &'a str,
}

/// Parse a Fluree address into its components.
///
/// Supports the following formats:
/// - `fluree:<method>://<path>` -> identifier=None, method, path
/// - `fluree:<identifier>:<method>://<path>` -> identifier=Some, method, path
///
/// Returns `None` for addresses that don't start with `fluree:` or are malformed.
///
/// # Arguments
///
/// * `address` - The address string to parse
///
/// # Returns
///
/// `Some(ParsedFlureeAddress)` if the address is a valid Fluree address,
/// `None` otherwise.
pub fn parse_fluree_address(address: &str) -> Option<ParsedFlureeAddress<'_>> {
    // Must start with fluree:
    let rest = address.strip_prefix("fluree:")?;

    // Find :// which marks the start of path
    let scheme_end = rest.find("://")?;
    let scheme_part = &rest[..scheme_end];
    let path = &rest[scheme_end + 3..];

    // Empty path is invalid
    if path.is_empty() {
        return None;
    }

    // Check if scheme_part contains a colon (identifier:method)
    if let Some(colon_pos) = scheme_part.find(':') {
        let identifier = &scheme_part[..colon_pos];
        let method = &scheme_part[colon_pos + 1..];

        // Both identifier and method must be non-empty
        if identifier.is_empty() || method.is_empty() {
            return None;
        }

        Some(ParsedFlureeAddress {
            identifier: Some(identifier),
            method,
            path,
        })
    } else {
        // Just method, no identifier
        if scheme_part.is_empty() {
            return None;
        }

        Some(ParsedFlureeAddress {
            identifier: None,
            method: scheme_part,
            path,
        })
    }
}

/// Extract just the identifier from a Fluree address.
///
/// This is a convenience function for routing decisions that only need to check
/// if an identifier is present and what it is.
///
/// # Arguments
///
/// * `address` - The address string to parse
///
/// # Returns
///
/// `Some(&str)` containing the identifier if present, `None` otherwise.
pub fn extract_identifier(address: &str) -> Option<&str> {
    parse_fluree_address(address)?.identifier
}

/// Extract the ledger path prefix from a Fluree content-addressed address.
///
/// For an address like `fluree:memory://mydb/main/commit/abc123.fcv2`,
/// returns `Some("mydb/main")` — the portion before the content-type directory.
///
/// This is useful for constructing a `StorageContentStore` bridge that uses the
/// same path layout as the original writes. The returned prefix is already in
/// path form (no `:` colon), so it can be passed directly to `bridge_content_store`.
pub fn extract_ledger_prefix(address: &str) -> Option<String> {
    let parsed = parse_fluree_address(address)?;
    // Content-type directory markers used by content_path()
    for marker in ["/commit/", "/txn/", "/index/", "/blob/"] {
        if let Some(pos) = parsed.path.find(marker) {
            return Some(parsed.path[..pos].to_string());
        }
    }
    None
}

/// Extract the path portion from a Fluree address.
///
/// This returns the path after the `://` delimiter, regardless of whether
/// an identifier is present.
///
/// # Arguments
///
/// * `address` - The address string to parse
///
/// # Returns
///
/// The path portion if the address is valid, or the original address
/// if it doesn't match the Fluree address format.
pub fn extract_path(address: &str) -> &str {
    if let Some(parsed) = parse_fluree_address(address) {
        parsed.path
    } else {
        // For non-fluree addresses, return as-is (raw path)
        address
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_s3_address() {
        let parsed = parse_fluree_address("fluree:s3://mydb/main/commit/abc.fcv2").unwrap();
        assert_eq!(parsed.identifier, None);
        assert_eq!(parsed.method, "s3");
        assert_eq!(parsed.path, "mydb/main/commit/abc.fcv2");
    }

    #[test]
    fn test_parse_simple_file_address() {
        let parsed = parse_fluree_address("fluree:file://data/ledger.json").unwrap();
        assert_eq!(parsed.identifier, None);
        assert_eq!(parsed.method, "file");
        assert_eq!(parsed.path, "data/ledger.json");
    }

    #[test]
    fn test_parse_simple_memory_address() {
        let parsed = parse_fluree_address("fluree:memory://db/index/root.json").unwrap();
        assert_eq!(parsed.identifier, None);
        assert_eq!(parsed.method, "memory");
        assert_eq!(parsed.path, "db/index/root.json");
    }

    #[test]
    fn test_parse_address_with_identifier() {
        let parsed =
            parse_fluree_address("fluree:commit-storage:s3://mydb/commit/abc.fcv2").unwrap();
        assert_eq!(parsed.identifier, Some("commit-storage"));
        assert_eq!(parsed.method, "s3");
        assert_eq!(parsed.path, "mydb/commit/abc.fcv2");
    }

    #[test]
    fn test_parse_address_with_identifier_file() {
        let parsed =
            parse_fluree_address("fluree:index-store:file://data/index/root.json").unwrap();
        assert_eq!(parsed.identifier, Some("index-store"));
        assert_eq!(parsed.method, "file");
        assert_eq!(parsed.path, "data/index/root.json");
    }

    #[test]
    fn test_parse_address_with_identifier_memory() {
        let parsed = parse_fluree_address("fluree:cache:memory://db/index/root.json").unwrap();
        assert_eq!(parsed.identifier, Some("cache"));
        assert_eq!(parsed.method, "memory");
        assert_eq!(parsed.path, "db/index/root.json");
    }

    #[test]
    fn test_parse_address_with_hyphenated_identifier() {
        let parsed =
            parse_fluree_address("fluree:my-commit-storage:s3://bucket/path/file.json").unwrap();
        assert_eq!(parsed.identifier, Some("my-commit-storage"));
        assert_eq!(parsed.method, "s3");
        assert_eq!(parsed.path, "bucket/path/file.json");
    }

    #[test]
    fn test_non_fluree_address_returns_none() {
        assert!(parse_fluree_address("s3://bucket/path").is_none());
        assert!(parse_fluree_address("file://path/to/file").is_none());
        assert!(parse_fluree_address("https://example.com").is_none());
        assert!(parse_fluree_address("some/raw/path.json").is_none());
    }

    #[test]
    fn test_empty_path_returns_none() {
        assert!(parse_fluree_address("fluree:s3://").is_none());
        assert!(parse_fluree_address("fluree:myid:s3://").is_none());
    }

    #[test]
    fn test_empty_method_returns_none() {
        assert!(parse_fluree_address("fluree:://path").is_none());
        assert!(parse_fluree_address("fluree:myid:://path").is_none());
    }

    #[test]
    fn test_empty_identifier_returns_none() {
        // Empty identifier with method should fail
        assert!(parse_fluree_address("fluree::s3://path").is_none());
    }

    #[test]
    fn test_extract_identifier() {
        assert_eq!(extract_identifier("fluree:myid:s3://path"), Some("myid"));
        assert_eq!(
            extract_identifier("fluree:commit-store:file://path"),
            Some("commit-store")
        );
        assert_eq!(extract_identifier("fluree:s3://path"), None);
        assert_eq!(extract_identifier("s3://path"), None);
        assert_eq!(extract_identifier("raw/path"), None);
    }

    #[test]
    fn test_extract_path() {
        assert_eq!(
            extract_path("fluree:s3://mydb/main/file.json"),
            "mydb/main/file.json"
        );
        assert_eq!(
            extract_path("fluree:myid:s3://mydb/main/file.json"),
            "mydb/main/file.json"
        );
        // Non-fluree addresses return as-is
        assert_eq!(extract_path("raw/path.json"), "raw/path.json");
        assert_eq!(extract_path("s3://bucket/path"), "s3://bucket/path");
    }

    #[test]
    fn test_path_with_special_characters() {
        let parsed = parse_fluree_address("fluree:s3://bucket/path/with spaces/file.json").unwrap();
        assert_eq!(parsed.path, "bucket/path/with spaces/file.json");

        let parsed = parse_fluree_address("fluree:s3://bucket/path%20encoded/file.json").unwrap();
        assert_eq!(parsed.path, "bucket/path%20encoded/file.json");
    }

    #[test]
    fn test_identifier_with_numbers() {
        let parsed = parse_fluree_address("fluree:store123:s3://path/file.json").unwrap();
        assert_eq!(parsed.identifier, Some("store123"));
        assert_eq!(parsed.method, "s3");
    }

    #[test]
    fn test_identifier_with_underscores() {
        let parsed = parse_fluree_address("fluree:my_store:s3://path/file.json").unwrap();
        assert_eq!(parsed.identifier, Some("my_store"));
        assert_eq!(parsed.method, "s3");
    }
}
