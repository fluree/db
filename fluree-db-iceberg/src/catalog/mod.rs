//! Iceberg catalog client module.
//!
//! This module provides the [`CatalogClient`] trait and implementations for
//! interacting with Iceberg REST catalogs (primarily Polaris).

pub mod direct;
mod rest;
mod table_identifier;

pub use direct::DirectCatalogClient;
pub use rest::{RestCatalogClient, RestCatalogConfig};
pub use table_identifier::{encode_namespace_for_rest, parse_table_identifier, TableIdentifier};

#[cfg(feature = "aws")]
pub use direct::SendDirectCatalogClient;

use crate::credential::VendedCredentials;
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// Response from loading a table, including optional vended credentials.
#[derive(Debug)]
pub struct LoadTableResponse {
    /// S3/file path to the table metadata JSON file
    pub metadata_location: String,
    /// Full config map from REST response (for debugging/extension)
    pub config: HashMap<String, serde_json::Value>,
    /// Vended storage credentials (if catalog supports credential delegation)
    pub credentials: Option<VendedCredentials>,
}

/// Iceberg catalog client trait.
///
/// Provides table discovery and metadata loading from a catalog.
///
/// Note: `Send + Sync` bounds are intentionally NOT required at the trait level
/// to keep the core runtime/WASM-friendly. Apply bounds at integration points
/// as needed.
#[async_trait(?Send)]
pub trait CatalogClient: std::fmt::Debug {
    /// List all namespaces in the catalog.
    async fn list_namespaces(&self) -> Result<Vec<String>>;

    /// List all tables in a namespace.
    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>>;

    /// Load table information including metadata location and optional credentials.
    ///
    /// If `request_credentials` is true, includes the X-Iceberg-Access-Delegation header
    /// to request vended credentials from the catalog.
    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        request_credentials: bool,
    ) -> Result<LoadTableResponse>;
}

/// Send-safe catalog client trait.
///
/// This trait mirrors [`CatalogClient`] but requires `Send + Sync` and produces
/// `Send` futures. Use this for server-side code that needs to spawn tasks.
#[async_trait]
pub trait SendCatalogClient: std::fmt::Debug + Send + Sync {
    /// List all namespaces in the catalog.
    async fn list_namespaces(&self) -> Result<Vec<String>>;

    /// List all tables in a namespace.
    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>>;

    /// Load table information including metadata location and optional credentials.
    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        request_credentials: bool,
    ) -> Result<LoadTableResponse>;
}
