//! Vended credentials management for Iceberg storage access.
//!
//! This module provides structures and traits for handling temporary
//! credentials vended by Iceberg REST catalogs (like Polaris).

mod vended;

#[cfg(feature = "aws")]
mod aws_provider;

pub use vended::{CredentialCacheKey, OperationScope, VendedCredentialCache, VendedCredentials};

#[cfg(feature = "aws")]
pub use aws_provider::VendedAwsCredentialProvider;

use crate::error::Result;
use async_trait::async_trait;
use std::fmt::Debug;

/// Provider for storage credentials obtained from a catalog.
///
/// This trait allows downstream IO layers to request credentials
/// for accessing table data files. Implementations may cache credentials
/// and handle refresh on expiration.
///
/// Note: `Send + Sync` bounds are intentionally NOT required at the trait level.
/// Apply bounds at integration points as needed.
#[async_trait(?Send)]
pub trait CredentialResolver: Debug {
    /// Get credentials for accessing files from a table.
    ///
    /// Returns `None` if vending is disabled or credentials are unavailable.
    /// In that case, the IO layer should fall back to ambient AWS configuration.
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key identifying the catalog, table, and operation scope
    async fn resolve(&self, key: &CredentialCacheKey) -> Result<Option<VendedCredentials>>;

    /// Invalidate cached credentials (e.g., after a 403 error).
    ///
    /// This should be called when credentials are rejected to force a refresh
    /// on the next `resolve()` call.
    fn invalidate(&self, key: &CredentialCacheKey);
}

/// Send-safe credential resolver for AWS SDK integration.
///
/// This trait is identical to [`CredentialResolver`] but requires `Send` bounds
/// on the future. It is used specifically with [`VendedAwsCredentialProvider`]
/// because the AWS SDK requires `Send` futures.
///
/// Note: This trait is only available with the `aws` feature.
#[cfg(feature = "aws")]
#[async_trait]
pub trait SendCredentialResolver: Debug + Send + Sync {
    /// Get credentials for accessing files from a table.
    ///
    /// Returns `None` if vending is disabled or credentials are unavailable.
    async fn resolve(&self, key: &CredentialCacheKey) -> Result<Option<VendedCredentials>>;

    /// Invalidate cached credentials (e.g., after a 403 error).
    fn invalidate(&self, key: &CredentialCacheKey);
}
