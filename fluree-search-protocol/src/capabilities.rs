//! Capabilities response types.

use serde::{Deserialize, Serialize};

use crate::{BM25_ANALYZER_VERSION, MAX_LIMIT, MAX_TIMEOUT_MS, PROTOCOL_VERSION};

/// Service capabilities response.
///
/// Returned by the `/v1/capabilities` endpoint to describe what
/// the search service supports. Clients can use this to:
///
/// - Verify protocol version compatibility
/// - Check analyzer version for score parity verification
/// - Discover supported query types
/// - Learn service limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    /// Protocol version supported by the service.
    pub protocol_version: String,

    /// BM25 analyzer version for parity verification.
    ///
    /// Both embedded and remote search must use identical analyzer
    /// configuration. Clients can compare this against their embedded
    /// analyzer version to ensure score parity.
    pub bm25_analyzer_version: String,

    /// List of supported query kinds (e.g., "bm25", "vector").
    pub supported_query_kinds: Vec<String>,

    /// Maximum allowed limit for search requests.
    pub max_limit: usize,

    /// Maximum allowed timeout in milliseconds.
    pub max_timeout_ms: u64,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self::new()
    }
}

impl Capabilities {
    /// Create capabilities with default values.
    pub fn new() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION.to_string(),
            bm25_analyzer_version: BM25_ANALYZER_VERSION.to_string(),
            supported_query_kinds: vec!["bm25".to_string()],
            max_limit: MAX_LIMIT,
            max_timeout_ms: MAX_TIMEOUT_MS,
        }
    }

    /// Create capabilities with vector search support.
    pub fn with_vector_support(mut self) -> Self {
        if !self.supported_query_kinds.contains(&"vector".to_string()) {
            self.supported_query_kinds.push("vector".to_string());
        }
        self
    }

    /// Check if a query kind is supported.
    pub fn supports(&self, kind: &str) -> bool {
        self.supported_query_kinds.iter().any(|k| k == kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities_default() {
        let caps = Capabilities::new();

        assert_eq!(caps.protocol_version, PROTOCOL_VERSION);
        assert_eq!(caps.bm25_analyzer_version, BM25_ANALYZER_VERSION);
        assert!(caps.supports("bm25"));
        assert!(!caps.supports("vector"));
        assert_eq!(caps.max_limit, MAX_LIMIT);
    }

    #[test]
    fn test_capabilities_with_vector() {
        let caps = Capabilities::new().with_vector_support();

        assert!(caps.supports("bm25"));
        assert!(caps.supports("vector"));
    }

    #[test]
    fn test_capabilities_serialization() {
        let caps = Capabilities::new().with_vector_support();

        let json = serde_json::to_string_pretty(&caps).unwrap();
        assert!(json.contains("bm25"));
        assert!(json.contains("vector"));
        assert!(json.contains(BM25_ANALYZER_VERSION));

        let parsed: Capabilities = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.supported_query_kinds.len(), 2);
    }
}
