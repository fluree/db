//! Manifest handling for Iceberg tables.
//!
//! This module provides types and functions for parsing Iceberg manifest lists
//! and manifest files, which are stored in Avro format.
//!
//! # Overview
//!
//! - **Manifest list**: Lists all manifest files for a snapshot, with partition summaries
//! - **Manifest file**: Lists data files with their column statistics (bounds, counts)
//! - **Value codec**: Decodes Iceberg's binary encoding for bounds comparison
//! - **Reader**: Reads a snapshot's manifests concurrently, in manifest-list order

pub mod data_file;
pub mod manifest_list;
pub mod reader;
pub mod value_codec;

pub use data_file::{
    parse_manifest, parse_manifest_with_deleted, DataFile, FileFormat, ManifestEntry,
    ManifestEntryStatus, PartitionData,
};
pub use manifest_list::{
    parse_manifest_list, parse_manifest_list_with_deletes, ManifestContent, ManifestListEntry,
    PartitionFieldSummary,
};
pub use reader::{read_manifests, send_read_manifests, DEFAULT_MANIFEST_READ_CONCURRENCY};
pub use value_codec::{decode_bound, decode_by_type_string, encode_value, TypedValue};
