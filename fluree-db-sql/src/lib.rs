//! SQL graph sources for Fluree DB.
//!
//! An R2RML mapping over tables served by any engine that speaks the Trino
//! client protocol over HTTP: Trino / Starburst / PrestoDB directly, or a
//! `fluree-sql-bridge` sidecar in front of Postgres, MySQL or SQLite. The
//! query engine pushes one single-table scan at a time (projection + typed
//! filters), rendered here as SQL; joins and everything else stay in-engine.
//!
//! - [`config::SqlGsConfig`] — the persisted graph-source record.
//! - [`dialect`] — SQL rendering of a scan against a probed schema.
//! - [`trino::TrinoClient`] — the statement/page protocol, streaming batches.
//! - [`types`] — Trino type names and JSON page values → column batches.

pub mod config;
pub mod dialect;
pub mod error;
pub mod net;
pub mod trino;
pub mod types;

pub use config::{SqlGsConfig, WireProtocol};
pub use dialect::{
    CmpOp, Literal, LogicalSource, Predicate, RenderedScan, ScanRequest, SqlDialect,
};
pub use error::{Result, SqlError};
pub use net::validate_endpoint as validate_sql_endpoint;
pub use trino::{SqlBatchStream, TrinoClient};

// Re-exported so callers wire auth/secret resolution with one import.
pub use fluree_db_iceberg::auth::{AuthConfig, SendCatalogAuth};
pub use fluree_db_iceberg::config::MappingSource;
pub use fluree_db_iceberg::{ConfigValue, SecretResolver};
