//! Composite search backend that dispatches to multiple backends.
//!
//! This module provides [`CompositeBackend`], which wraps multiple
//! [`SearchBackend`] implementations and routes queries to the first
//! backend that supports the query type.

use super::SearchBackend;
use crate::error::{Result, ServiceError};
use async_trait::async_trait;
use fluree_search_protocol::{QueryVariant, SearchHit};

/// Composite backend that dispatches to the first matching backend.
///
/// When a search request arrives, the composite iterates through its
/// backends and delegates to the first one whose `supports()` method
/// returns `true` for the query variant.
///
/// # Example
///
/// ```ignore
/// use fluree_search_service::backend::{CompositeBackend, Bm25Backend, VectorBackend};
///
/// let composite = CompositeBackend::new(vec![
///     Box::new(bm25_backend),
///     Box::new(vector_backend),
/// ]);
/// ```
pub struct CompositeBackend {
    backends: Vec<Box<dyn SearchBackend>>,
}

impl CompositeBackend {
    /// Create a new composite backend from a list of backends.
    pub fn new(backends: Vec<Box<dyn SearchBackend>>) -> Self {
        Self { backends }
    }
}

impl std::fmt::Debug for CompositeBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeBackend")
            .field("backends", &self.backends)
            .finish()
    }
}

#[async_trait]
impl SearchBackend for CompositeBackend {
    async fn search(
        &self,
        graph_source_id: &str,
        query: &QueryVariant,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<(i64, Vec<SearchHit>)> {
        for backend in &self.backends {
            if backend.supports(query) {
                return backend
                    .search(graph_source_id, query, limit, as_of_t, sync, timeout_ms)
                    .await;
            }
        }
        Err(ServiceError::InvalidRequest {
            message: format!(
                "No backend supports query type: {:?}",
                std::mem::discriminant(query)
            ),
        })
    }

    fn supports(&self, query: &QueryVariant) -> bool {
        self.backends.iter().any(|b| b.supports(query))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test backend that supports a specific query variant and returns fixed results.
    #[derive(Debug)]
    struct FixedBackend {
        name: &'static str,
        supports_bm25: bool,
        supports_vector: bool,
    }

    #[async_trait]
    impl SearchBackend for FixedBackend {
        async fn search(
            &self,
            _graph_source_id: &str,
            _query: &QueryVariant,
            _limit: usize,
            _as_of_t: Option<i64>,
            _sync: bool,
            _timeout_ms: Option<u64>,
        ) -> Result<(i64, Vec<SearchHit>)> {
            Ok((
                100,
                vec![SearchHit::new(
                    format!("http://example.org/{}", self.name),
                    "db:main",
                    0.9,
                )],
            ))
        }

        fn supports(&self, query: &QueryVariant) -> bool {
            match query {
                QueryVariant::Bm25 { .. } => self.supports_bm25,
                QueryVariant::Vector { .. } | QueryVariant::VectorSimilarTo { .. } => {
                    self.supports_vector
                }
            }
        }
    }

    #[tokio::test]
    async fn test_composite_routes_bm25() {
        let composite = CompositeBackend::new(vec![
            Box::new(FixedBackend {
                name: "bm25",
                supports_bm25: true,
                supports_vector: false,
            }),
            Box::new(FixedBackend {
                name: "vector",
                supports_bm25: false,
                supports_vector: true,
            }),
        ]);

        let (_, hits) = composite
            .search(
                "search:main",
                &QueryVariant::Bm25 {
                    text: "test".to_string(),
                },
                10,
                None,
                false,
                None,
            )
            .await
            .unwrap();

        assert_eq!(hits[0].iri, "http://example.org/bm25");
    }

    #[tokio::test]
    async fn test_composite_routes_vector() {
        let composite = CompositeBackend::new(vec![
            Box::new(FixedBackend {
                name: "bm25",
                supports_bm25: true,
                supports_vector: false,
            }),
            Box::new(FixedBackend {
                name: "vector",
                supports_bm25: false,
                supports_vector: true,
            }),
        ]);

        let (_, hits) = composite
            .search(
                "search:main",
                &QueryVariant::Vector {
                    vector: vec![0.5],
                    metric: None,
                },
                10,
                None,
                false,
                None,
            )
            .await
            .unwrap();

        assert_eq!(hits[0].iri, "http://example.org/vector");
    }

    #[tokio::test]
    async fn test_composite_no_matching_backend() {
        let composite = CompositeBackend::new(vec![Box::new(FixedBackend {
            name: "bm25-only",
            supports_bm25: true,
            supports_vector: false,
        })]);

        let result = composite
            .search(
                "search:main",
                &QueryVariant::Vector {
                    vector: vec![0.5],
                    metric: None,
                },
                10,
                None,
                false,
                None,
            )
            .await;

        assert!(matches!(result, Err(ServiceError::InvalidRequest { .. })));
    }

    #[tokio::test]
    async fn test_composite_supports() {
        let composite = CompositeBackend::new(vec![
            Box::new(FixedBackend {
                name: "bm25",
                supports_bm25: true,
                supports_vector: false,
            }),
            Box::new(FixedBackend {
                name: "vector",
                supports_bm25: false,
                supports_vector: true,
            }),
        ]);

        assert!(composite.supports(&QueryVariant::Bm25 {
            text: "test".to_string()
        }));
        assert!(composite.supports(&QueryVariant::Vector {
            vector: vec![0.5],
            metric: None
        }));
    }

    #[tokio::test]
    async fn test_composite_empty_backends() {
        let composite = CompositeBackend::new(vec![]);

        assert!(!composite.supports(&QueryVariant::Bm25 {
            text: "test".to_string()
        }));

        let result = composite
            .search(
                "search:main",
                &QueryVariant::Bm25 {
                    text: "test".to_string(),
                },
                10,
                None,
                false,
                None,
            )
            .await;

        assert!(matches!(result, Err(ServiceError::InvalidRequest { .. })));
    }
}
