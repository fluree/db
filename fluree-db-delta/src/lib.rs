//! Delta Lake tables as Fluree graph sources.
//!
//! [Delta Kernel](https://docs.delta.io/kernel/rust/) owns the table protocol:
//! log replay and checkpoints, reader-feature checks, column mapping, partition
//! values, and deletion vectors. This crate selects a table version, asks
//! Kernel for that version's logical rows, and hands them to the R2RML layer as
//! [`fluree_db_tabular::ColumnBatch`]es. Tables are never written.

pub mod bridge;
pub mod config;
mod datafile;
pub mod error;
pub mod filter;
mod listing;
mod prune;
mod store;
pub mod table;
mod unity;

pub use config::{AzureAuth, DeltaGsConfig, DeltaIoConfig, Placement, UnityConfig};
pub use datafile::rows_decoded;
pub use error::{DeltaError, Result};
pub use filter::{ColumnFilter, FilterOp, FilterValue};
pub use table::{DeltaBatchStream, DeltaSnapshot, DeltaTable, VersionSelector};
