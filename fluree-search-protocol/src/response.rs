//! Search response types.

use serde::{Deserialize, Serialize};

/// Search response envelope.
///
/// Returned by the `/v1/search` endpoint for successful queries.
/// Contains the search hits along with metadata about the index state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    /// Protocol version (echoed from request).
    pub protocol_version: String,

    /// Request ID (echoed from request if provided).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    /// Watermark of the index snapshot that was searched.
    ///
    /// This is the transaction time (`t`) of the indexed data. Clients can
    /// use this to measure staleness vs. ledger head.
    pub index_t: i64,

    /// Search hits in descending score order.
    pub hits: Vec<SearchHit>,

    /// Non-fatal warnings (e.g., truncated results, partial index).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,

    /// Time taken to execute the search in milliseconds.
    pub took_ms: u64,
}

/// A single search hit.
///
/// This type is shared across protocol, query engine, and service layers
/// to ensure consistent representation of search results.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    /// The IRI of the matched entity.
    pub iri: String,

    /// The ledger alias where this entity resides.
    ///
    /// For single-ledger graph sources, this is always the source ledger.
    /// For multi-ledger graph sources (datasets), this identifies which ledger
    /// the entity came from.
    pub ledger_alias: String,

    /// The relevance score.
    ///
    /// For BM25, this is the BM25 score (higher is more relevant).
    /// For vector search, this is typically 1 - distance or raw similarity.
    pub score: f64,
}

impl SearchResponse {
    /// Create a new search response.
    pub fn new(
        protocol_version: String,
        request_id: Option<String>,
        index_t: i64,
        hits: Vec<SearchHit>,
        took_ms: u64,
    ) -> Self {
        Self {
            protocol_version,
            request_id,
            index_t,
            hits,
            warnings: Vec::new(),
            took_ms,
        }
    }

    /// Add a warning to the response.
    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}

impl SearchHit {
    /// Create a new search hit.
    pub fn new(iri: impl Into<String>, ledger_alias: impl Into<String>, score: f64) -> Self {
        Self {
            iri: iri.into(),
            ledger_alias: ledger_alias.into(),
            score,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_serialization() {
        let response = SearchResponse {
            protocol_version: "1.0".to_string(),
            request_id: Some("test-123".to_string()),
            index_t: 150,
            hits: vec![
                SearchHit::new("ex:product-1", "mydb:main", 0.95),
                SearchHit::new("ex:product-2", "mydb:main", 0.87),
            ],
            warnings: vec![],
            took_ms: 12,
        };

        let json = serde_json::to_string_pretty(&response).unwrap();
        let parsed: SearchResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.protocol_version, "1.0");
        assert_eq!(parsed.index_t, 150);
        assert_eq!(parsed.hits.len(), 2);
        assert_eq!(parsed.hits[0].iri, "ex:product-1");
        assert_eq!(parsed.hits[0].score, 0.95);
        assert_eq!(parsed.took_ms, 12);
    }

    #[test]
    fn test_response_with_warnings() {
        let response = SearchResponse::new(
            "1.0".to_string(),
            None,
            100,
            vec![SearchHit::new("ex:item", "db:main", 0.5)],
            5,
        )
        .with_warning("Results truncated to limit");

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("Results truncated"));

        let parsed: SearchResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.warnings.len(), 1);
    }

    #[test]
    fn test_empty_warnings_not_serialized() {
        let response = SearchResponse::new("1.0".to_string(), None, 100, vec![], 1);

        let json = serde_json::to_string(&response).unwrap();
        // warnings field should be omitted when empty
        assert!(!json.contains("warnings"));
    }

    #[test]
    fn test_hit_equality() {
        let hit1 = SearchHit::new("ex:a", "db:main", 0.5);
        let hit2 = SearchHit::new("ex:a", "db:main", 0.5);
        let hit3 = SearchHit::new("ex:b", "db:main", 0.5);

        assert_eq!(hit1, hit2);
        assert_ne!(hit1, hit3);
    }
}
