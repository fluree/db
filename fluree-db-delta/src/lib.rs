//! Delta Lake tables as Fluree graph sources.
//!
//! [Delta Kernel](https://docs.delta.io/kernel/rust/) owns the table protocol:
//! log replay and checkpoints, reader-feature checks, column mapping, partition
//! values, and deletion vectors. This crate selects a table version, asks
//! Kernel for that version's logical rows, and hands them to the R2RML layer as
//! [`fluree_db_tabular::ColumnBatch`]es. Tables are never written.

pub mod bridge;
pub mod config;
pub mod error;
mod store;
pub mod table;

pub use config::{DeltaGsConfig, DeltaIoConfig};
pub use error::{DeltaError, Result};
pub use table::{DeltaBatchStream, DeltaSnapshot, DeltaTable, VersionSelector};
