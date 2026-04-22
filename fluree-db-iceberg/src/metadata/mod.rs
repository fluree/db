//! Iceberg table metadata parsing.
//!
//! This module provides structures for parsing and working with Iceberg
//! table metadata, including schemas, snapshots, partition specs, and
//! time travel snapshot selection.

mod snapshot;
mod table;

pub use snapshot::{select_snapshot, Snapshot, SnapshotSelection};
pub use table::{
    PartitionField, PartitionSpec, Schema, SchemaField, SnapshotLogEntry, SortField, SortOrder,
    TableMetadata,
};
