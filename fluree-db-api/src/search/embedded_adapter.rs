//! Embedded BM25 search provider adapter.
//!
//! This module provides [`EmbeddedBm25SearchProvider`], an adapter that implements
//! [`Bm25SearchProvider`] by wrapping a [`Bm25IndexProvider`] and performing local
//! BM25 scoring.

use async_trait::async_trait;
use fluree_db_query::bm25::{
    Analyzer, Bm25IndexProvider, Bm25Scorer, Bm25SearchProvider, Bm25SearchResult, SearchHit,
};
use fluree_db_query::error::Result;
use std::fmt;

/// Embedded BM25 search provider that wraps a [`Bm25IndexProvider`].
///
/// This adapter implements [`Bm25SearchProvider`] by:
/// 1. Loading the BM25 index via the underlying [`Bm25IndexProvider`]
/// 2. Analyzing the query text with the default analyzer
/// 3. Scoring documents using [`Bm25Scorer`]
/// 4. Returning results as [`Bm25SearchResult`]
///
/// Use this adapter for embedded search mode where the index is loaded locally
/// and scoring happens in-process.
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::search::EmbeddedBm25SearchProvider;
/// use fluree_db_api::FlureeIndexProvider;
///
/// let index_provider = FlureeIndexProvider::new(&fluree);
/// let search_provider = EmbeddedBm25SearchProvider::new(&index_provider);
///
/// let result = search_provider.search_bm25(
///     "products:main",
///     "wireless headphones",
///     10,
///     Some(100),
///     false,
///     Some(5000),
/// ).await?;
/// ```
pub struct EmbeddedBm25SearchProvider<'a, P: Bm25IndexProvider> {
    /// The underlying index provider that loads BM25 indexes.
    index_provider: &'a P,
    /// Text analyzer for query processing.
    analyzer: Analyzer,
}

impl<'a, P: Bm25IndexProvider> EmbeddedBm25SearchProvider<'a, P> {
    /// Create a new embedded search provider wrapping an index provider.
    pub fn new(index_provider: &'a P) -> Self {
        Self {
            index_provider,
            analyzer: Analyzer::english_default(),
        }
    }
}

impl<P: Bm25IndexProvider> fmt::Debug for EmbeddedBm25SearchProvider<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbeddedBm25SearchProvider")
            .field("index_provider", &self.index_provider)
            .finish()
    }
}

#[async_trait]
impl<P: Bm25IndexProvider> Bm25SearchProvider for EmbeddedBm25SearchProvider<'_, P> {
    async fn search_bm25(
        &self,
        graph_source_id: &str,
        query_text: &str,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Bm25SearchResult> {
        // Load the index via the underlying provider
        let index = self
            .index_provider
            .bm25_index(graph_source_id, as_of_t, sync, timeout_ms)
            .await?;

        // Get the effective watermark from the loaded index
        let index_t = index.watermark.effective_t();

        // Analyze the query text
        let terms = self.analyzer.analyze_to_strings(query_text);

        // Handle empty query (no terms after analysis)
        if terms.is_empty() {
            return Ok(Bm25SearchResult::empty(index_t));
        }

        // Score documents using top_k for consistent behavior with service backend
        let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
        let scorer = Bm25Scorer::new(&index, &term_refs);
        let scored = scorer.top_k(limit);

        // Convert to SearchHit
        let hits: Vec<SearchHit> = scored
            .into_iter()
            .map(|(doc_key, score)| {
                SearchHit::new(
                    doc_key.subject_iri.to_string(),
                    doc_key.ledger_alias.to_string(),
                    score,
                )
            })
            .collect();

        Ok(Bm25SearchResult::new(index_t, hits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_query::bm25::{Bm25Index, DocKey};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Debug, Default)]
    struct MockIndexProvider {
        indexes: HashMap<String, Arc<Bm25Index>>,
    }

    #[async_trait]
    impl Bm25IndexProvider for MockIndexProvider {
        async fn bm25_index(
            &self,
            graph_source_id: &str,
            _as_of_t: Option<i64>,
            _sync: bool,
            _timeout_ms: Option<u64>,
        ) -> Result<Arc<Bm25Index>> {
            self.indexes.get(graph_source_id).cloned().ok_or_else(|| {
                fluree_db_query::error::QueryError::InvalidQuery(format!(
                    "No index for {graph_source_id}"
                ))
            })
        }
    }

    #[tokio::test]
    async fn test_embedded_search_basic() {
        let mut index = Bm25Index::new();
        index.upsert_document(
            DocKey::new("db:main", "http://example.org/product-1"),
            [("wireless", 2), ("headphones", 1)].into_iter().collect(),
        );
        index.upsert_document(
            DocKey::new("db:main", "http://example.org/product-2"),
            [("wired", 1), ("headphones", 1)].into_iter().collect(),
        );

        let mut mock_provider = MockIndexProvider::default();
        mock_provider
            .indexes
            .insert("products:main".to_string(), Arc::new(index));

        let search_provider = EmbeddedBm25SearchProvider::new(&mock_provider);

        let result = search_provider
            .search_bm25("products:main", "wireless", 10, None, false, None)
            .await
            .unwrap();

        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].iri, "http://example.org/product-1");
        assert_eq!(result.hits[0].ledger_alias, "db:main");
        assert!(result.hits[0].score > 0.0);
    }

    #[tokio::test]
    async fn test_embedded_search_empty_query() {
        let index = Bm25Index::new();

        let mut mock_provider = MockIndexProvider::default();
        mock_provider
            .indexes
            .insert("test:main".to_string(), Arc::new(index));

        let search_provider = EmbeddedBm25SearchProvider::new(&mock_provider);

        // Query with only stopwords should return empty results
        let result = search_provider
            .search_bm25("test:main", "the a an", 10, None, false, None)
            .await
            .unwrap();

        assert!(result.hits.is_empty());
    }

    #[tokio::test]
    async fn test_embedded_search_limit() {
        let mut index = Bm25Index::new();
        // Use "quantum" which is not a stopword (unlike "test" which is filtered)
        for i in 0..10 {
            index.upsert_document(
                DocKey::new("db:main", format!("http://example.org/doc-{i}")),
                [("quantum", 1)].into_iter().collect(),
            );
        }

        let mut mock_provider = MockIndexProvider::default();
        mock_provider
            .indexes
            .insert("search:main".to_string(), Arc::new(index));

        let search_provider = EmbeddedBm25SearchProvider::new(&mock_provider);

        let result = search_provider
            .search_bm25("search:main", "quantum", 3, None, false, None)
            .await
            .unwrap();

        assert_eq!(result.hits.len(), 3);
    }

    #[tokio::test]
    async fn test_embedded_search_no_matches() {
        let mut index = Bm25Index::new();
        index.upsert_document(
            DocKey::new("db:main", "http://example.org/doc-1"),
            [("apple", 1)].into_iter().collect(),
        );

        let mut mock_provider = MockIndexProvider::default();
        mock_provider
            .indexes
            .insert("test:main".to_string(), Arc::new(index));

        let search_provider = EmbeddedBm25SearchProvider::new(&mock_provider);

        let result = search_provider
            .search_bm25("test:main", "banana", 10, None, false, None)
            .await
            .unwrap();

        assert!(result.hits.is_empty());
    }
}
