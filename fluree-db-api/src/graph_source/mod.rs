//! Graph Source Operations
//!
//! This module provides APIs for creating, managing, and querying graph sources.
//! Graph sources are derived indexes built from ledger data, such as BM25 full-text
//! search indexes.
//!
//! # Key Concepts
//!
//! - **Graph Source**: A derived index built from one or more source ledgers
//! - **BM25 Index**: A full-text search index using the BM25 scoring algorithm
//! - **Watermark**: The transaction time (`t`) up to which the index has been synced
//! - **Property Dependencies**: IRIs of properties that trigger reindexing when changed
//!
//! # BM25 Full-Text Search
//!
//! ## Creating an Index
//!
//! ```ignore
//! use fluree_db_api::{Fluree, Bm25CreateConfig};
//! use serde_json::json;
//!
//! let config = Bm25CreateConfig::new(
//!     "my-search",  // Graph source name
//!     "docs:main",  // Source ledger
//!     json!({
//!         "@context": {"ex": "http://example.org/"},
//!         "where": [{"@id": "?x", "@type": "ex:Article"}],
//!         "select": {"?x": ["@id", "ex:title", "ex:content"]}
//!     }),
//! )
//! .with_k1(1.2)   // Optional: term frequency saturation
//! .with_b(0.75);  // Optional: document length normalization
//!
//! // Validate before creating
//! config.validate()?;
//!
//! let result = fluree.create_full_text_index(config).await?;
//! println!("Created index with {} documents at t={}", result.doc_count, result.index_t);
//! ```
//!
//! ## Querying
//!
//! Use `f:*` properties (with `"f": "https://ns.flur.ee/db#"` in `@context`) in your query's where clause:
//!
//! ```json
//! {
//!   "where": [{
//!     "f:graphSource": "my-search:main",
//!     "f:searchText": "rust programming",
//!     "f:searchResult": {
//!       "f:resultId": "?doc",
//!       "f:resultScore": "?score",
//!       "f:resultLedger": "?source"
//!     }
//!   }],
//!   "select": ["?doc", "?score"],
//!   "orderBy": [{"var": "?score", "order": "desc"}]
//! }
//! ```
//!
//! ## Syncing (Maintenance)
//!
//! Keep indexes up to date with ledger changes:
//!
//! ```ignore
//! // Manual sync to catch up with ledger head
//! let result = fluree.sync_bm25_index("my-search:main").await?;
//! println!("Synced {} documents", result.upserted);
//!
//! // Check staleness without syncing
//! let check = fluree.check_bm25_staleness("my-search:main").await?;
//! if check.is_stale {
//!     println!("Index is {} commits behind", check.lag);
//! }
//!
//! // Load with automatic sync (on-query catch-up)
//! let (index, sync_result) = fluree.load_bm25_index_with_sync("my-search:main", true).await?;
//! ```
//!
//! ## Time-Travel Queries
//!
//! Query at a specific historical time:
//!
//! ```ignore
//! // Sync to a specific t (for time-travel queries)
//! let result = fluree.sync_bm25_index_to("my-search:main", target_t, Some(5000)).await?;
//!
//! // Use FlureeIndexProvider for query execution
//! let provider = FlureeIndexProvider::new(&fluree);
//! let mut ctx = ExecutionContext::new(&db, &vars);
//! ctx.to_t = target_t;  // Time-travel target
//! ctx.bm25_provider = Some(&provider);
//! ```
//!
//! ## Multi-Ledger Support
//!
//! BM25 indexes support multiple source ledgers with per-ledger watermarks:
//!
//! - Same IRI in different ledgers = distinct documents (keyed by ledger alias + IRI)
//! - `effective_t()` = minimum watermark across all source ledgers
//! - Use `f:resultLedger` binding to disambiguate results in joins

// Internal modules
mod bm25;
mod cache;
mod config;
mod helpers;
mod provider;
mod result;

#[cfg(feature = "vector")]
mod vector;

#[cfg(feature = "iceberg")]
mod r2rml;

// Re-export configuration types
pub use config::Bm25CreateConfig;

#[cfg(feature = "vector")]
pub use config::VectorCreateConfig;

#[cfg(feature = "iceberg")]
pub use config::{CatalogMode, IcebergCreateConfig, RestCatalogMode};

#[cfg(feature = "iceberg")]
pub use config::{R2rmlCreateConfig, R2rmlMappingInput};

// Re-export result types
pub use result::Bm25CreateResult;
pub use result::Bm25DropResult;
pub use result::Bm25StalenessCheck;
pub use result::Bm25SyncResult;
pub use result::SnapshotSelection;

#[cfg(feature = "vector")]
pub use result::VectorCreateResult;

#[cfg(feature = "vector")]
pub use result::VectorDropResult;

#[cfg(feature = "vector")]
pub use result::VectorStalenessCheck;

#[cfg(feature = "vector")]
pub use result::VectorSyncResult;

#[cfg(feature = "iceberg")]
pub use result::IcebergCreateResult;

#[cfg(feature = "iceberg")]
pub use result::R2rmlCreateResult;

// Re-export cache types
pub use cache::R2rmlCache;
pub use cache::R2rmlCacheStats;

// Re-export providers
pub use provider::FlureeIndexProvider;

#[cfg(feature = "iceberg")]
pub use r2rml::FlureeR2rmlProvider;

// Helper functions are used internally by bm25.rs via direct module path
