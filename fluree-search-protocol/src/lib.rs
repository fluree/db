//! Search Service Protocol types for Fluree DB.
//!
//! This crate defines the wire contract for the Fluree search service protocol,
//! supporting both BM25 full-text search and vector similarity search. These types
//! are used by:
//!
//! - Remote search services (HTTP servers, Lambda handlers)
//! - Remote search clients (HTTP clients)
//! - Query engine integration (shared hit types)
//!
//! # Protocol Overview
//!
//! The search service protocol provides:
//!
//! - **Unified request/response envelope** for BM25 and vector search
//! - **Time-travel semantics** via `as_of_t` parameter (BM25 only; vector is head-only)
//! - **Sync semantics** via `sync` parameter
//! - **Watermark tracking** via `index_t` in responses
//!
//! # Example
//!
//! ```rust
//! use fluree_search_protocol::{SearchRequest, QueryVariant, PROTOCOL_VERSION};
//!
//! let request = SearchRequest {
//!     protocol_version: PROTOCOL_VERSION.to_string(),
//!     request_id: Some("req-123".to_string()),
//!     graph_source_id: "products-search:main".to_string(),
//!     limit: 10,
//!     as_of_t: None,
//!     sync: false,
//!     timeout_ms: Some(5000),
//!     query: QueryVariant::Bm25 {
//!         text: "wireless headphones".to_string(),
//!     },
//! };
//! ```

mod capabilities;
mod error;
mod request;
mod response;

pub use capabilities::Capabilities;
pub use error::{ErrorCode, ErrorDetail, SearchError};
pub use request::{QueryVariant, SearchRequest};
pub use response::{SearchHit, SearchResponse};

/// Protocol version string included in all requests and responses.
pub const PROTOCOL_VERSION: &str = "1.0";

/// BM25 analyzer version for compatibility verification between embedded and remote.
///
/// Both embedded and remote search must use identical analyzer configuration
/// to ensure comparable scoring. This version string is included in the
/// capabilities response so clients can verify compatibility.
pub const BM25_ANALYZER_VERSION: &str = "english_default_v1";

/// Default limit for search requests if not specified.
pub const DEFAULT_LIMIT: usize = 10;

/// Maximum allowed limit for search requests.
pub const MAX_LIMIT: usize = 1000;

/// Default timeout in milliseconds for search requests.
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Maximum allowed timeout in milliseconds.
pub const MAX_TIMEOUT_MS: u64 = 300_000;
