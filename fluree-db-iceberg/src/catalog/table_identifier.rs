//! Table identifier parsing and formatting utilities.

use crate::error::{IcebergError, Result};

/// Canonical Iceberg table identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdentifier {
    /// Namespace (e.g., "openflights" or "db.schema")
    pub namespace: String,
    /// Table name
    pub table: String,
}

impl TableIdentifier {
    /// Create a new table identifier.
    pub fn new(namespace: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            table: table.into(),
        }
    }

    /// Get the canonical string representation (namespace.table).
    pub fn to_canonical(&self) -> String {
        format!("{}.{}", self.namespace, self.table)
    }
}

impl std::fmt::Display for TableIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.table)
    }
}

/// Parse a table identifier from various formats.
///
/// Supports:
/// - Canonical (dot): "namespace.table" or "ns1.ns2.table"
/// - Path (slash): "namespace/table" or "ns1/ns2/table"
///
/// Table identifiers are normalized to dot notation (matching `../db` behavior).
///
/// # Examples
///
/// ```
/// use fluree_db_iceberg::catalog::parse_table_identifier;
///
/// let id = parse_table_identifier("openflights/airlines").unwrap();
/// assert_eq!(id.namespace, "openflights");
/// assert_eq!(id.table, "airlines");
/// assert_eq!(id.to_canonical(), "openflights.airlines");
///
/// let multi = parse_table_identifier("db.schema.events").unwrap();
/// assert_eq!(multi.namespace, "db.schema");
/// assert_eq!(multi.table, "events");
/// ```
pub fn parse_table_identifier(table_id: &str) -> Result<TableIdentifier> {
    // Handle slash-separated (path format) - convert to dot notation
    if table_id.contains('/') {
        let parts: Vec<&str> = table_id.split('/').collect();
        if parts.len() < 2 {
            return Err(IcebergError::Config(format!(
                "Invalid table identifier '{table_id}': expected namespace/table"
            )));
        }
        // Validate no empty parts
        if parts.iter().any(|p| p.is_empty()) {
            return Err(IcebergError::Config(format!(
                "Invalid table identifier '{table_id}': empty component"
            )));
        }
        let namespace = parts[..parts.len() - 1].join(".");
        let table = parts[parts.len() - 1].to_string();
        return Ok(TableIdentifier { namespace, table });
    }

    // Handle dot-separated (canonical format)
    if let Some(last_dot) = table_id.rfind('.') {
        if last_dot == 0 || last_dot == table_id.len() - 1 {
            return Err(IcebergError::Config(format!(
                "Invalid table identifier '{table_id}': namespace and table required"
            )));
        }
        let namespace = table_id[..last_dot].to_string();
        let table = table_id[last_dot + 1..].to_string();
        return Ok(TableIdentifier { namespace, table });
    }

    Err(IcebergError::Config(format!(
        "Invalid table identifier '{table_id}': must contain namespace separator (. or /)"
    )))
}

/// Encode a namespace for use in Iceberg REST API paths.
///
/// Multi-level namespaces are encoded using the unit separator character (U+001F)
/// as per the Iceberg REST spec used by Polaris.
///
/// # Examples
///
/// ```
/// use fluree_db_iceberg::catalog::encode_namespace_for_rest;
///
/// // Single-level namespace
/// assert_eq!(encode_namespace_for_rest("openflights"), "openflights");
///
/// // Multi-level namespace
/// assert_eq!(encode_namespace_for_rest("db.schema"), "db%1Fschema");
/// ```
pub fn encode_namespace_for_rest(namespace: &str) -> String {
    // Replace dots with unit separator, then URL-encode
    let with_separator = namespace.replace('.', "\u{001F}");
    urlencoding::encode(&with_separator).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_canonical() {
        let id = parse_table_identifier("openflights.airlines").unwrap();
        assert_eq!(id.namespace, "openflights");
        assert_eq!(id.table, "airlines");
        assert_eq!(id.to_canonical(), "openflights.airlines");
    }

    #[test]
    fn test_parse_multi_level_namespace() {
        let id = parse_table_identifier("db.schema.table").unwrap();
        assert_eq!(id.namespace, "db.schema");
        assert_eq!(id.table, "table");
    }

    #[test]
    fn test_parse_path_format() {
        let id = parse_table_identifier("openflights/airlines").unwrap();
        assert_eq!(id.namespace, "openflights");
        assert_eq!(id.table, "airlines");
        // Path format is normalized to dot notation
        assert_eq!(id.to_canonical(), "openflights.airlines");
    }

    #[test]
    fn test_parse_multi_level_path() {
        let id = parse_table_identifier("db/schema/table").unwrap();
        assert_eq!(id.namespace, "db.schema");
        assert_eq!(id.table, "table");
    }

    #[test]
    fn test_parse_invalid_no_separator() {
        assert!(parse_table_identifier("just_a_table").is_err());
    }

    #[test]
    fn test_parse_invalid_empty_parts() {
        assert!(parse_table_identifier(".table").is_err());
        assert!(parse_table_identifier("ns.").is_err());
        assert!(parse_table_identifier("a//b").is_err());
    }

    #[test]
    fn test_encode_single_level_namespace() {
        assert_eq!(encode_namespace_for_rest("openflights"), "openflights");
    }

    #[test]
    fn test_encode_multi_level_namespace() {
        // db.schema should become db<US>schema, then URL-encoded
        let encoded = encode_namespace_for_rest("db.schema");
        assert_eq!(encoded, "db%1Fschema");
    }

    #[test]
    fn test_encode_three_level_namespace() {
        let encoded = encode_namespace_for_rest("catalog.db.schema");
        assert_eq!(encoded, "catalog%1Fdb%1Fschema");
    }

    #[test]
    fn test_encode_with_special_chars() {
        // Namespace with characters that need URL encoding
        let encoded = encode_namespace_for_rest("my namespace");
        assert!(encoded.contains("%20")); // space encoded
    }

    #[test]
    fn test_table_identifier_display() {
        let id = TableIdentifier::new("ns", "table");
        assert_eq!(format!("{id}"), "ns.table");
    }
}
