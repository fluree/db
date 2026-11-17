//! Disk cache for binary-index artifacts.
//!
//! The cache machinery now lives in [`fluree_db_core::disk_cache`] so it can be
//! shared (one global byte budget) with other on-disk caches such as Iceberg
//! data files. This module re-exports it for existing call sites.

pub use fluree_db_core::disk_cache::{
    fetch_cached_bytes, fetch_cached_bytes_cid, needs_disk_copy, seed_disk_cache, uses_disk_cache,
    DiskArtifactCache,
};
