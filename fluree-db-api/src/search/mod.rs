//! Search service integration for Fluree DB.
//!
//! This module provides search provider implementations for the BM25 search
//! service protocol. It includes:
//!
//! - [`EmbeddedBm25SearchProvider`]: Adapter that wraps a [`Bm25IndexProvider`] and
//!   performs local scoring to implement [`Bm25SearchProvider`].
//! - [`RemoteBm25SearchProvider`]: HTTP client that delegates to a remote search service
//!   (requires `search-remote-client` feature).
//!
//! # Architecture
//!
//! The search service protocol defines two provider traits:
//!
//! - [`Bm25IndexProvider`]: Returns the raw BM25 index for local scoring (legacy)
//! - [`Bm25SearchProvider`]: Returns search results directly (preferred)
//!
//! For embedded mode, use [`EmbeddedBm25SearchProvider`] to wrap an existing
//! [`Bm25IndexProvider`]. For remote mode (when enabled with the `search-remote-client`
//! feature), use [`RemoteBm25SearchProvider`] which makes HTTP calls to a search service.
//!
//! # Configuration
//!
//! Use [`SearchDeploymentConfig`] to configure the search deployment mode for a
//! graph source. This determines whether search is performed locally (embedded)
//! or delegated to a remote service.
//!
//! [`Bm25IndexProvider`]: fluree_db_query::bm25::Bm25IndexProvider
//! [`Bm25SearchProvider`]: fluree_db_query::bm25::Bm25SearchProvider

mod config;
mod embedded_adapter;

#[cfg(feature = "search-remote-client")]
mod remote_provider;

#[cfg(feature = "search-remote-client")]
mod remote_vector_provider;

pub use config::{DeploymentMode, SearchDeploymentConfig};
pub use embedded_adapter::EmbeddedBm25SearchProvider;

#[cfg(feature = "search-remote-client")]
pub use remote_provider::RemoteBm25SearchProvider;

#[cfg(feature = "search-remote-client")]
pub use remote_vector_provider::RemoteVectorSearchProvider;
