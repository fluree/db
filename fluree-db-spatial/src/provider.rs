//! Spatial index provider trait.
//!
//! Abstracts over embedded (in-process) and remote (RPC) spatial index access.
//! Query operators use this trait so they work identically with either backend.
//!
//! # Design
//!
//! The provider trait mirrors the snapshot query methods but is async to
//! support remote RPC. The embedded implementation just wraps a snapshot,
//! while the remote implementation makes RPC calls.
//!
//! # Novelty Overlay
//!
//! The provider supports novelty overlay for uncommitted changes via interior
//! mutability. This allows `Arc<dyn SpatialIndexProvider>` to be shared across
//! query execution while still supporting novelty updates.
//!
//! Novelty geometry handles use the high bit (0x80000000) to distinguish from
//! snapshot handles. When looking up metadata/geometry, the provider checks
//! this bit and dispatches to the appropriate arena.

use crate::cell_index::CellEntry;
use crate::config::S2CoveringConfig;
use crate::error::Result;
use crate::geometry::{GeometryArena, GeometryMetadata};
use crate::snapshot::{min_distance_to_bbox, min_distance_to_geometry, SpatialIndexSnapshot};
use async_trait::async_trait;
use geo_types::Geometry;
use std::sync::RwLock;

/// High bit flag for novelty geometry handles.
///
/// Novelty entries have this bit set in their geo_handle to distinguish
/// them from snapshot entries. When looking up metadata/geometry, check
/// this bit and dispatch to the appropriate arena.
pub const NOVELTY_HANDLE_FLAG: u32 = 0x8000_0000;

/// Novelty state for uncommitted spatial changes.
///
/// Stored with interior mutability in the provider to support
/// `Arc<dyn SpatialIndexProvider>` while allowing novelty updates.
///
/// Uses `Arc<Vec<CellEntry>>` internally to allow efficient slicing
/// without cloning entries on every range scan.
pub struct NoveltyState {
    /// Novelty cell entries (sorted by cmp_index).
    /// Wrapped in Arc for efficient slicing without per-range cloning.
    pub entries: std::sync::Arc<Vec<CellEntry>>,

    /// Arena for novelty geometries.
    /// Handles in entries have NOVELTY_HANDLE_FLAG set.
    pub arena: std::sync::Arc<GeometryArena>,

    /// Epoch counter for cache invalidation.
    pub epoch: u64,
}

impl Default for NoveltyState {
    fn default() -> Self {
        Self {
            entries: std::sync::Arc::new(Vec::new()),
            arena: std::sync::Arc::new(GeometryArena::new()),
            epoch: 0,
        }
    }
}

impl NoveltyState {
    /// Create a new novelty state.
    pub fn new(entries: Vec<CellEntry>, arena: GeometryArena, epoch: u64) -> Self {
        Self {
            entries: std::sync::Arc::new(entries),
            arena: std::sync::Arc::new(arena),
            epoch,
        }
    }

    /// Check if this state is empty (no novelty entries).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Result of a proximity query.
#[derive(Debug, Clone)]
pub struct ProximityResult {
    /// Subject ID.
    pub subject_id: u64,

    /// Distance in meters.
    pub distance: f64,

    /// Optional geometry handle (for retrieving metadata).
    pub geo_handle: Option<u32>,
}

/// Result of a spatial predicate query (within/contains/intersects).
#[derive(Debug, Clone)]
pub struct SpatialResult {
    /// Subject ID.
    pub subject_id: u64,

    /// Optional geometry handle.
    pub geo_handle: Option<u32>,
}

/// Spatial index provider trait.
///
/// Abstracts over embedded and remote spatial index access.
#[async_trait]
pub trait SpatialIndexProvider: Send + Sync {
    /// Get the base transaction time of the index.
    fn base_t(&self) -> i64;

    /// Get the index transaction time.
    fn index_t(&self) -> i64;

    /// Query subjects within a radius of a point.
    ///
    /// Returns results sorted by distance (nearest first).
    async fn query_radius(
        &self,
        center_lat: f64,
        center_lng: f64,
        radius_meters: f64,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<ProximityResult>>;

    /// Query subjects whose geometry is within the query geometry.
    async fn query_within(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<SpatialResult>>;

    /// Query subjects whose geometry contains the query geometry.
    async fn query_contains(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<SpatialResult>>;

    /// Query subjects whose geometry intersects the query geometry.
    async fn query_intersects(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<SpatialResult>>;

    /// Get geometry metadata by handle (if available).
    async fn get_metadata(&self, geo_handle: u32) -> Result<Option<GeometryMetadata>>;

    /// Set novelty overlay for uncommitted changes.
    ///
    /// Uses interior mutability so this works with `Arc<dyn SpatialIndexProvider>`.
    /// The entries should have NOVELTY_HANDLE_FLAG set in their geo_handle fields.
    fn set_novelty(&self, entries: Vec<CellEntry>, arena: GeometryArena, epoch: u64);

    /// Clear the novelty overlay.
    fn clear_novelty(&self);

    /// Get the S2 covering config used by this index.
    ///
    /// Needed for computing novelty coverings that match the index configuration.
    fn s2_config(&self) -> &S2CoveringConfig;

    /// Get the predicate IRI that this index covers.
    fn predicate(&self) -> &str;
}

/// Embedded spatial index provider.
///
/// Wraps a snapshot for in-process queries with interior mutability for novelty.
pub struct EmbeddedSpatialProvider {
    /// The base spatial index snapshot.
    snapshot: SpatialIndexSnapshot,

    /// Novelty state with interior mutability.
    novelty_state: RwLock<NoveltyState>,
}

impl EmbeddedSpatialProvider {
    /// Create a new embedded provider from a snapshot.
    pub fn new(snapshot: SpatialIndexSnapshot) -> Self {
        Self {
            snapshot,
            novelty_state: RwLock::new(NoveltyState::default()),
        }
    }

    /// Get the underlying snapshot.
    pub fn snapshot(&self) -> &SpatialIndexSnapshot {
        &self.snapshot
    }

    /// Get metadata for an entry, checking both snapshot and novelty arenas.
    fn get_metadata_for_handle(&self, geo_handle: u32) -> Option<GeometryMetadata> {
        if geo_handle & NOVELTY_HANDLE_FLAG != 0 {
            // Novelty handle - look up in novelty arena
            let actual_handle = geo_handle & !NOVELTY_HANDLE_FLAG;
            let (_entries, arena) = self.novelty_snapshot();
            arena.get(actual_handle).map(|e| e.metadata.clone())
        } else {
            // Snapshot handle
            self.snapshot.get_metadata(geo_handle).cloned()
        }
    }

    /// Get metadata using a pre-fetched novelty arena (avoids repeated lock acquisition).
    fn get_metadata_with_novelty_arena(
        &self,
        geo_handle: u32,
        novelty_arena: &GeometryArena,
    ) -> Option<GeometryMetadata> {
        if geo_handle & NOVELTY_HANDLE_FLAG != 0 {
            let actual_handle = geo_handle & !NOVELTY_HANDLE_FLAG;
            novelty_arena.get(actual_handle).map(|e| e.metadata.clone())
        } else {
            self.snapshot.get_metadata(geo_handle).cloned()
        }
    }

    /// Parse geometry using a pre-fetched novelty arena (avoids repeated lock acquisition).
    fn parse_geometry_with_novelty_arena(
        &self,
        geo_handle: u32,
        novelty_arena: &GeometryArena,
    ) -> Result<Geometry<f64>> {
        if geo_handle & NOVELTY_HANDLE_FLAG != 0 {
            let actual_handle = geo_handle & !NOVELTY_HANDLE_FLAG;
            novelty_arena.parse_geometry(actual_handle)
        } else {
            self.snapshot.arena().parse_geometry(geo_handle)
        }
    }

    /// Get a snapshot of the novelty state (entries + arena) without holding lock.
    ///
    /// Returns `(Arc<Vec<CellEntry>>, Arc<GeometryArena>)` for efficient access
    /// across multiple range scans without repeated lock acquisition or cloning.
    fn novelty_snapshot(
        &self,
    ) -> (
        std::sync::Arc<Vec<CellEntry>>,
        std::sync::Arc<GeometryArena>,
    ) {
        let guard = match self.novelty_state.read() {
            Ok(g) => g,
            Err(_) => {
                return (
                    std::sync::Arc::new(Vec::new()),
                    std::sync::Arc::new(GeometryArena::new()),
                )
            }
        };
        (
            std::sync::Arc::clone(&guard.entries),
            std::sync::Arc::clone(&guard.arena),
        )
    }

    /// Slice novelty entries by cell_id range using binary search.
    ///
    /// Takes a pre-fetched entries Arc to avoid repeated lock acquisition.
    fn slice_novelty_range(entries: &[CellEntry], min_cell: u64, max_cell: u64) -> &[CellEntry] {
        if entries.is_empty() {
            return &[];
        }

        // Find first entry with cell_id >= min_cell
        let start = entries.partition_point(|e| e.cell_id < min_cell);
        // Find first entry with cell_id > max_cell
        let end = entries[start..].partition_point(|e| e.cell_id <= max_cell) + start;

        &entries[start..end]
    }
}

#[async_trait]
impl SpatialIndexProvider for EmbeddedSpatialProvider {
    fn base_t(&self) -> i64 {
        self.snapshot.base_t()
    }

    fn index_t(&self) -> i64 {
        self.snapshot.index_t()
    }

    async fn query_radius(
        &self,
        center_lat: f64,
        center_lng: f64,
        radius_meters: f64,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<ProximityResult>> {
        use crate::covering::{cells_to_ranges, covering_for_circle};
        use crate::dedup::{DedupStrategy, StreamingDedup};
        use crate::error::SpatialError;
        use crate::replay::{MergeSorted, ReplayResolver};

        // Check time-travel coverage
        if to_t < self.snapshot.base_t() {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.snapshot.base_t(),
            });
        }

        // Generate covering for the query circle
        let cells = covering_for_circle(center_lat, center_lng, radius_meters, self.s2_config())?;

        let ranges = cells_to_ranges(&cells);

        // Snapshot novelty once for all ranges (avoids repeated lock acquisition)
        let (novelty_entries, novelty_arena) = self.novelty_snapshot();

        // Scan all ranges and collect entries
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in ranges {
            let snapshot_entries = self.snapshot.cell_index().scan_range(min_cell, max_cell)?;

            // Get novelty entries in this range (slice from pre-fetched Arc)
            let novelty_slice = Self::slice_novelty_range(&novelty_entries, min_cell, max_cell);

            // Merge snapshot + novelty (novelty wins on exact ties by merge contract,
            // ensuring uncommitted overlay changes take precedence over persisted state)
            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            // Replay at to_t
            let replayed = ReplayResolver::new(merged, to_t);

            all_entries.extend(replayed);
        }

        // Dedup by subject_id, keeping min distance
        let mut dedup = StreamingDedup::new(DedupStrategy::KeepMinDistance);

        for entry in all_entries {
            // Get geometry metadata for bbox prefilter (uses pre-fetched arena)
            if let Some(meta) =
                self.get_metadata_with_novelty_arena(entry.geo_handle, &novelty_arena)
            {
                // Conservative bbox prefilter
                if let Some(bbox) = &meta.bbox {
                    let min_bbox_distance = min_distance_to_bbox(
                        center_lat,
                        center_lng,
                        bbox.min_lat,
                        bbox.max_lat,
                        bbox.min_lng,
                        bbox.max_lng,
                    );
                    if min_bbox_distance > radius_meters {
                        continue;
                    }
                }

                // Exact distance: parse geometry and compute min distance
                if let Ok(geom) =
                    self.parse_geometry_with_novelty_arena(entry.geo_handle, &novelty_arena)
                {
                    let distance = min_distance_to_geometry(center_lat, center_lng, &geom);
                    if distance <= radius_meters {
                        dedup.push(entry, distance);
                    }
                }
            }
        }

        let mut results = dedup.finish_sorted_by_distance();

        // Apply limit
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        Ok(results
            .into_iter()
            .map(|e| ProximityResult {
                subject_id: e.entry.subject_id,
                distance: e.distance,
                geo_handle: Some(e.entry.geo_handle),
            })
            .collect())
    }

    async fn query_within(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<SpatialResult>> {
        use crate::covering::{cells_to_ranges, covering_for_geometry};
        use crate::dedup::dedup_keep_first;
        use crate::error::SpatialError;
        use crate::geometry::BBox;
        use crate::replay::{MergeSorted, ReplayResolver};
        use geo::Within;

        // Check time-travel coverage
        if to_t < self.snapshot.base_t() {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.snapshot.base_t(),
            });
        }

        // Generate covering for the query geometry
        let cells = covering_for_geometry(query_geom, self.s2_config())?;
        let ranges = cells_to_ranges(&cells);

        // Get query bbox for prefiltering
        let query_bbox = BBox::from_geometry(query_geom);

        // Snapshot novelty once for all ranges
        let (novelty_entries, novelty_arena) = self.novelty_snapshot();

        // Scan all ranges
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in ranges {
            let snapshot_entries = self.snapshot.cell_index().scan_range(min_cell, max_cell)?;
            let novelty_slice = Self::slice_novelty_range(&novelty_entries, min_cell, max_cell);

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }

        // Dedup by subject_id
        let deduped = dedup_keep_first(all_entries);

        // Filter with bbox prefilter + exact predicate
        let mut results = Vec::new();

        for entry in deduped {
            if let Some(meta) =
                self.get_metadata_with_novelty_arena(entry.geo_handle, &novelty_arena)
            {
                // Bbox prefilter
                if let (Some(query_bbox), Some(entry_bbox)) = (&query_bbox, &meta.bbox) {
                    if !query_bbox.intersects(entry_bbox) {
                        continue;
                    }
                }

                // Exact predicate: entry geometry must be within query geometry
                if let Ok(entry_geom) =
                    self.parse_geometry_with_novelty_arena(entry.geo_handle, &novelty_arena)
                {
                    if entry_geom.is_within(query_geom) {
                        results.push(SpatialResult {
                            subject_id: entry.subject_id,
                            geo_handle: Some(entry.geo_handle),
                        });

                        if let Some(lim) = limit {
                            if results.len() >= lim {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    async fn query_contains(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<SpatialResult>> {
        use crate::covering::{cells_to_ranges, covering_for_geometry};
        use crate::dedup::dedup_keep_first;
        use crate::error::SpatialError;
        use crate::geometry::BBox;
        use crate::replay::{MergeSorted, ReplayResolver};
        use geo::Contains;

        // Check time-travel coverage
        if to_t < self.snapshot.base_t() {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.snapshot.base_t(),
            });
        }

        // Generate covering for the query geometry
        let cells = covering_for_geometry(query_geom, self.s2_config())?;
        let ranges = cells_to_ranges(&cells);

        // Get query bbox for prefiltering
        let query_bbox = BBox::from_geometry(query_geom);

        // Snapshot novelty once for all ranges
        let (novelty_entries, novelty_arena) = self.novelty_snapshot();

        // Scan all ranges
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in ranges {
            let snapshot_entries = self.snapshot.cell_index().scan_range(min_cell, max_cell)?;
            let novelty_slice = Self::slice_novelty_range(&novelty_entries, min_cell, max_cell);

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }

        // Dedup by subject_id
        let deduped = dedup_keep_first(all_entries);

        // Filter with bbox prefilter + exact predicate
        let mut results = Vec::new();

        for entry in deduped {
            if let Some(meta) =
                self.get_metadata_with_novelty_arena(entry.geo_handle, &novelty_arena)
            {
                // Bbox prefilter: entry bbox must contain query bbox
                if let (Some(query_bbox), Some(entry_bbox)) = (&query_bbox, &meta.bbox) {
                    if !entry_bbox.contains_bbox(query_bbox) {
                        continue;
                    }
                }

                // Exact predicate: entry geometry must contain query geometry
                if let Ok(entry_geom) =
                    self.parse_geometry_with_novelty_arena(entry.geo_handle, &novelty_arena)
                {
                    if entry_geom.contains(query_geom) {
                        results.push(SpatialResult {
                            subject_id: entry.subject_id,
                            geo_handle: Some(entry.geo_handle),
                        });

                        if let Some(lim) = limit {
                            if results.len() >= lim {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    async fn query_intersects(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<SpatialResult>> {
        use crate::covering::{cells_to_ranges, covering_for_geometry};
        use crate::dedup::dedup_keep_first;
        use crate::error::SpatialError;
        use crate::geometry::BBox;
        use crate::replay::{MergeSorted, ReplayResolver};
        use geo::Intersects;

        // Check time-travel coverage
        if to_t < self.snapshot.base_t() {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.snapshot.base_t(),
            });
        }

        // Generate covering for the query geometry
        let cells = covering_for_geometry(query_geom, self.s2_config())?;
        let ranges = cells_to_ranges(&cells);

        // Get query bbox for prefiltering
        let query_bbox = BBox::from_geometry(query_geom);

        // Snapshot novelty once for all ranges
        let (novelty_entries, novelty_arena) = self.novelty_snapshot();

        // Scan all ranges
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in ranges {
            let snapshot_entries = self.snapshot.cell_index().scan_range(min_cell, max_cell)?;
            let novelty_slice = Self::slice_novelty_range(&novelty_entries, min_cell, max_cell);

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }

        // Dedup by subject_id
        let deduped = dedup_keep_first(all_entries);

        // Filter with bbox prefilter + exact predicate
        let mut results = Vec::new();

        for entry in deduped {
            if let Some(meta) =
                self.get_metadata_with_novelty_arena(entry.geo_handle, &novelty_arena)
            {
                // Bbox prefilter
                if let (Some(query_bbox), Some(entry_bbox)) = (&query_bbox, &meta.bbox) {
                    if !query_bbox.intersects(entry_bbox) {
                        continue;
                    }
                }

                // Exact predicate: entry geometry must intersect query geometry
                if let Ok(entry_geom) =
                    self.parse_geometry_with_novelty_arena(entry.geo_handle, &novelty_arena)
                {
                    if entry_geom.intersects(query_geom) {
                        results.push(SpatialResult {
                            subject_id: entry.subject_id,
                            geo_handle: Some(entry.geo_handle),
                        });

                        if let Some(lim) = limit {
                            if results.len() >= lim {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    async fn get_metadata(&self, geo_handle: u32) -> Result<Option<GeometryMetadata>> {
        Ok(self.get_metadata_for_handle(geo_handle))
    }

    fn set_novelty(&self, mut entries: Vec<CellEntry>, arena: GeometryArena, epoch: u64) {
        // Check if epoch matches - no-op to avoid unnecessary Arc swaps
        if let Ok(guard) = self.novelty_state.read() {
            if guard.epoch == epoch {
                return;
            }
        }

        // Sort entries by index order before storing
        entries.sort_by(super::cell_index::CellEntry::cmp_index);

        if let Ok(mut guard) = self.novelty_state.write() {
            // Double-check epoch after acquiring write lock (another thread may have updated)
            if guard.epoch == epoch {
                return;
            }
            guard.entries = std::sync::Arc::new(entries);
            guard.arena = std::sync::Arc::new(arena);
            guard.epoch = epoch;
        }
    }

    fn clear_novelty(&self) {
        if let Ok(mut guard) = self.novelty_state.write() {
            guard.entries = std::sync::Arc::new(Vec::new());
            guard.arena = std::sync::Arc::new(GeometryArena::new());
            guard.epoch = 0;
        }
    }

    fn s2_config(&self) -> &S2CoveringConfig {
        &self.snapshot.root().config.s2_config
    }

    fn predicate(&self) -> &str {
        &self.snapshot.root().config.predicate
    }
}

// Remote provider would be implemented here with the `remote` feature flag
// It would use gRPC to call a spatial index server that has the same API

#[cfg(feature = "remote")]
pub mod remote {
    /// Remote spatial index provider.
    ///
    /// Connects to a spatial index server via gRPC.
    #[allow(dead_code)]
    pub struct RemoteSpatialProvider {
        // client: SpatialIndexClient,
        base_t: i64,
        index_t: i64,
    }

    // Implementation would go here
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::SpatialIndexBuilder;
    use crate::config::{MetadataConfig, S2CoveringConfig, SpatialCreateConfig};
    use crate::covering::covering_for_geometry;
    use crate::geometry::parse_wkt;
    use std::sync::atomic::{AtomicU64, Ordering};

    // Simple counter for unique hashes in tests
    static HASH_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn simple_hash(bytes: &[u8]) -> String {
        let count = HASH_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Use a simple checksum + counter for unique hashes
        let sum: u64 = bytes.iter().map(|&b| b as u64).sum();
        format!("sha256:test{sum:08x}{count:08x}")
    }

    /// Build a minimal snapshot with a single polygon for testing.
    fn build_test_snapshot(wkt: &str, subject_id: u64, t: i64) -> SpatialIndexSnapshot {
        use std::cell::RefCell;
        use std::sync::Arc;

        let config = SpatialCreateConfig::new("test:vg", "test:ledger", "test:geom");
        let mut builder = SpatialIndexBuilder::new(config);

        builder
            .add_geometry(subject_id, wkt, t, true)
            .expect("add geometry");

        let result = builder.build().expect("build");

        // Write to memory and load back
        // Use RefCell for interior mutability during write, then convert to Arc for read
        let cas = RefCell::new(std::collections::HashMap::<String, Vec<u8>>::new());

        let write_result = result
            .write_to_cas(|bytes| {
                let hash = simple_hash(bytes);
                cas.borrow_mut().insert(hash.clone(), bytes.to_vec());
                Ok(hash)
            })
            .expect("write to cas");

        // Convert to Arc for the read closure
        let cas_arc = Arc::new(cas.into_inner());

        // Use the root from WriteResult directly
        SpatialIndexSnapshot::load_from_cas(write_result.root, move |hash| {
            cas_arc
                .get(hash)
                .cloned()
                .ok_or_else(|| crate::error::SpatialError::ChunkNotFound(hash.to_string()))
        })
        .expect("load snapshot")
    }

    /// Create novelty entries for a polygon.
    fn create_novelty_entries(
        wkt: &str,
        subject_id: u64,
        t: i64,
        op: u8,
        s2_config: &S2CoveringConfig,
    ) -> (Vec<CellEntry>, GeometryArena) {
        let mut arena = GeometryArena::new();
        let metadata_config = MetadataConfig::default();

        let handle = arena.add(wkt, &metadata_config).expect("add to arena");
        let flagged_handle = handle | NOVELTY_HANDLE_FLAG;

        let geom = parse_wkt(wkt).expect("parse wkt");
        let cells = covering_for_geometry(&geom, s2_config).expect("covering");

        let entries: Vec<CellEntry> = cells
            .into_iter()
            .map(|cell_id| CellEntry::new(cell_id, subject_id, flagged_handle, t, op))
            .collect();

        (entries, arena)
    }

    #[tokio::test]
    async fn test_query_finds_snapshot_polygon() {
        // Build a snapshot with one polygon
        let snapshot = build_test_snapshot("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 100, 1);

        let provider = EmbeddedSpatialProvider::new(snapshot);

        // Query that intersects the polygon
        let query_geom = parse_wkt("POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))").unwrap();
        let results = provider
            .query_intersects(&query_geom, 1, None)
            .await
            .unwrap();

        // Should find the snapshot polygon
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject_id, 100);
    }

    #[tokio::test]
    async fn test_novelty_polygon_appears_in_query() {
        // Build a snapshot with one polygon
        let snapshot = build_test_snapshot("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 100, 1);

        let provider = EmbeddedSpatialProvider::new(snapshot);
        let s2_config = provider.s2_config().clone();

        // Add novelty polygon (different location)
        let (entries, arena) = create_novelty_entries(
            "POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))",
            200,
            2, // newer t
            1, // assert
            &s2_config,
        );

        provider.set_novelty(entries, arena, 1);

        // Query that intersects ONLY the novelty polygon (not snapshot)
        let query_geom = parse_wkt("POLYGON((25 25, 35 25, 35 35, 25 35, 25 25))").unwrap();
        let results = provider
            .query_intersects(&query_geom, 2, None)
            .await
            .unwrap();

        // Should find ONLY the novelty polygon
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject_id, 200);

        // Verify the handle has NOVELTY_HANDLE_FLAG set
        assert!(results[0].geo_handle.unwrap() & NOVELTY_HANDLE_FLAG != 0);
    }

    #[tokio::test]
    async fn test_query_finds_both_snapshot_and_novelty() {
        // Build a snapshot with one polygon
        let snapshot = build_test_snapshot("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 100, 1);

        let provider = EmbeddedSpatialProvider::new(snapshot);
        let s2_config = provider.s2_config().clone();

        // Add novelty polygon at a different location
        let (entries, arena) = create_novelty_entries(
            "POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))",
            200,
            2,
            1, // assert
            &s2_config,
        );

        provider.set_novelty(entries, arena, 1);

        // Query that intersects BOTH polygons (large query region)
        let query_geom = parse_wkt("POLYGON((-5 -5, 35 -5, 35 35, -5 35, -5 -5))").unwrap();
        let results = provider
            .query_intersects(&query_geom, 2, None)
            .await
            .unwrap();

        // Should find both polygons
        let subject_ids: std::collections::HashSet<_> =
            results.iter().map(|r| r.subject_id).collect();
        assert_eq!(subject_ids.len(), 2);
        assert!(subject_ids.contains(&100));
        assert!(subject_ids.contains(&200));
    }

    #[tokio::test]
    async fn test_novelty_retraction_hides_polygon() {
        // Build a snapshot with one polygon at subject 100
        let snapshot = build_test_snapshot("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 100, 1);

        let provider = EmbeddedSpatialProvider::new(snapshot);
        let s2_config = provider.s2_config().clone();

        // Create novelty RETRACTION for the same subject (op=0)
        let (entries, arena) = create_novelty_entries(
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",
            100,
            2, // newer t
            0, // RETRACT
            &s2_config,
        );

        provider.set_novelty(entries, arena, 1);

        // Query that would intersect the polygon
        let query_geom = parse_wkt("POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))").unwrap();
        let results = provider
            .query_intersects(&query_geom, 2, None)
            .await
            .unwrap();

        // Should NOT find the polygon (retracted in novelty)
        assert!(
            results.is_empty(),
            "Retracted polygon should not appear in results"
        );
    }

    #[tokio::test]
    async fn test_clear_novelty() {
        // Build a snapshot with one polygon
        let snapshot = build_test_snapshot("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 100, 1);

        let provider = EmbeddedSpatialProvider::new(snapshot);
        let s2_config = provider.s2_config().clone();

        // Add novelty polygon
        let (entries, arena) = create_novelty_entries(
            "POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))",
            200,
            2,
            1,
            &s2_config,
        );

        provider.set_novelty(entries, arena, 1);

        // Verify novelty polygon is found
        let query_geom = parse_wkt("POLYGON((25 25, 35 25, 35 35, 25 35, 25 25))").unwrap();
        let results = provider
            .query_intersects(&query_geom, 2, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        // Clear novelty
        provider.clear_novelty();

        // Now the novelty polygon should NOT be found
        let results = provider
            .query_intersects(&query_geom, 2, None)
            .await
            .unwrap();
        assert!(
            results.is_empty(),
            "After clear_novelty, novelty polygon should not appear"
        );
    }

    #[tokio::test]
    async fn test_radius_query_with_novelty() {
        // Build a snapshot with a polygon at (0-10, 0-10)
        let snapshot = build_test_snapshot("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 100, 1);

        let provider = EmbeddedSpatialProvider::new(snapshot);
        let s2_config = provider.s2_config().clone();

        // Add novelty polygon at (20-30, 20-30)
        let (entries, arena) = create_novelty_entries(
            "POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))",
            200,
            2,
            1,
            &s2_config,
        );

        provider.set_novelty(entries, arena, 1);

        // Radius query centered at (25, 25) should find novelty polygon
        // 500km radius should cover (20-30, 20-30) but not (0-10, 0-10)
        let results = provider
            .query_radius(25.0, 25.0, 1_500_000.0, 2, None)
            .await
            .unwrap();

        // Both polygons might be in this large radius
        let subject_ids: std::collections::HashSet<_> =
            results.iter().map(|r| r.subject_id).collect();

        // Should include novelty polygon 200
        assert!(
            subject_ids.contains(&200),
            "Radius query should find novelty polygon"
        );
    }
}
