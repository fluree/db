//! Spatial index snapshot.
//!
//! Combines cell index, geometry arena, and novelty into a queryable snapshot.
//!
//! # Root Manifest
//!
//! The snapshot is identified by a root manifest stored in CAS:
//!
//! ```json
//! {
//!   "version": 1,
//!   "config": { ... },
//!   "cell_index_hash": "sha256:...",
//!   "arena_hash": "sha256:...",
//!   "base_t": 1000,
//!   "index_t": 1000,
//!   "entry_count": 50000,
//!   "geometry_count": 10000
//! }
//! ```

use crate::cell_index::{CellEntry, CellIndexManifest, CellIndexReader};
use crate::config::SpatialConfig;
use crate::covering::{cells_to_ranges, covering_for_circle, covering_for_geometry};
use crate::dedup::{DedupStrategy, StreamingDedup};
use crate::error::{Result, SpatialError};
use crate::geometry::{GeometryArena, GeometryMetadata};
use crate::replay::{MergeSorted, ReplayResolver};
use geo::{Contains, Intersects, Within};
use geo_types::Geometry;
use serde::{Deserialize, Serialize};

/// Statistics from a spatial query execution.
///
/// Use these to understand query selectivity and identify optimization opportunities.
#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    /// Number of S2 cells in the query covering.
    pub covering_cells: usize,

    /// Number of merged cell ranges to scan.
    pub ranges_scanned: usize,

    /// Number of cell entries from snapshot scan.
    pub snapshot_entries: usize,

    /// Number of cell entries from novelty overlay.
    pub novelty_entries: usize,

    /// Number of entries after replay (time-travel filtering).
    pub after_replay: usize,

    /// Number of entries after dedup (unique subjects).
    pub after_dedup: usize,

    /// Number of entries that passed bbox prefilter.
    pub passed_bbox: usize,

    /// Number of exact predicate checks performed.
    pub exact_checks: usize,

    /// Number of results returned.
    pub result_count: usize,
}

impl QueryStats {
    /// Compute the selectivity ratio: result_count / after_replay.
    ///
    /// Lower is better (more candidates were filtered out).
    pub fn selectivity(&self) -> f64 {
        if self.after_replay == 0 {
            0.0
        } else {
            self.result_count as f64 / self.after_replay as f64
        }
    }

    /// Compute the bbox prefilter efficiency: passed_bbox / after_dedup.
    ///
    /// Lower is better (bbox rejected more candidates).
    pub fn bbox_efficiency(&self) -> f64 {
        if self.after_dedup == 0 {
            0.0
        } else {
            self.passed_bbox as f64 / self.after_dedup as f64
        }
    }

    /// Compute the exact check efficiency: result_count / exact_checks.
    ///
    /// Higher is better (more exact checks resulted in matches).
    pub fn exact_check_efficiency(&self) -> f64 {
        if self.exact_checks == 0 {
            0.0
        } else {
            self.result_count as f64 / self.exact_checks as f64
        }
    }
}

/// Root manifest for a spatial index snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialIndexRoot {
    /// Format version.
    pub version: u32,

    /// Configuration used to build this index.
    pub config: SpatialConfig,

    /// Content hash of the cell index manifest.
    pub cell_index_hash: String,

    /// Content hash of the geometry arena.
    pub arena_hash: String,

    /// Base transaction time (minimum t in the index).
    pub base_t: i64,

    /// Index transaction time (what t this snapshot is current as of).
    pub index_t: i64,

    /// Total number of cell entries.
    pub entry_count: u64,

    /// Total number of distinct geometries.
    pub geometry_count: u64,
}

impl SpatialIndexRoot {
    /// Current format version.
    pub const CURRENT_VERSION: u32 = 1;
}

/// A queryable spatial index snapshot.
pub struct SpatialIndexSnapshot {
    /// The root manifest.
    root: SpatialIndexRoot,

    /// Cell index reader.
    cell_index: CellIndexReader,

    /// Geometry arena (loaded into memory for now).
    arena: GeometryArena,

    /// Novelty entries (uncommitted changes).
    novelty: Vec<CellEntry>,

    /// Novelty epoch for cache keying.
    novelty_epoch: u64,
}

impl SpatialIndexSnapshot {
    /// Create a new snapshot from components.
    pub fn new(root: SpatialIndexRoot, cell_index: CellIndexReader, arena: GeometryArena) -> Self {
        Self {
            root,
            cell_index,
            arena,
            novelty: Vec::new(),
            novelty_epoch: 0,
        }
    }

    /// Load a snapshot from CAS using a fetch callback.
    ///
    /// The `fetch_bytes` callback fetches bytes by content hash (e.g., "sha256:...").
    /// This loads the manifest, arena, and creates a reader for on-demand leaflet access.
    ///
    /// # Arguments
    ///
    /// * `root` - The root manifest (usually loaded from nameservice or known location)
    /// * `fetch_bytes` - Callback to fetch bytes by content hash
    ///
    /// # Example
    ///
    /// ```ignore
    /// let root: SpatialIndexRoot = load_root_from_nameservice()?;
    /// let snapshot = SpatialIndexSnapshot::load_from_cas(root, |hash| {
    ///     cas_store.fetch(hash)
    /// })?;
    /// ```
    pub fn load_from_cas<F>(root: SpatialIndexRoot, fetch_bytes: F) -> Result<Self>
    where
        F: Fn(&str) -> Result<Vec<u8>> + Send + Sync + Clone + 'static,
    {
        // Handle empty index
        if root.cell_index_hash.is_empty() {
            return Ok(Self {
                root,
                cell_index: CellIndexReader::new(
                    CellIndexManifest {
                        total_entries: 0,
                        leaflets: Vec::new(),
                    },
                    move |_| Err(SpatialError::FormatError("empty index".into())),
                ),
                arena: GeometryArena::new(),
                novelty: Vec::new(),
                novelty_epoch: 0,
            });
        }

        // 1. Fetch and deserialize the cell index manifest
        let manifest_bytes = fetch_bytes(&root.cell_index_hash)?;
        let manifest: CellIndexManifest = serde_json::from_slice(&manifest_bytes).map_err(|e| {
            SpatialError::FormatError(format!("failed to deserialize cell index manifest: {e}"))
        })?;

        // 2. Fetch and deserialize the geometry arena
        let arena_bytes = fetch_bytes(&root.arena_hash)?;
        let arena = GeometryArena::from_bytes(&arena_bytes)?;

        // 3. Create cell index reader with the manifest and chunk fetcher
        let fetch_clone = fetch_bytes.clone();
        let cell_index = CellIndexReader::new(manifest, move |hash| fetch_clone(hash));

        Ok(Self {
            root,
            cell_index,
            arena,
            novelty: Vec::new(),
            novelty_epoch: 0,
        })
    }

    /// Set novelty entries for overlay.
    ///
    /// Entries are sorted by `cmp_index()` to ensure correct merge behavior.
    /// This is required for `MergeSorted` to function correctly.
    pub fn set_novelty(&mut self, mut novelty: Vec<CellEntry>, epoch: u64) {
        // Sort by index order (cell_id, subject_id, t DESC, op ASC)
        novelty.sort_by(super::cell_index::CellEntry::cmp_index);
        self.novelty = novelty;
        self.novelty_epoch = epoch;
    }

    /// Get the root manifest.
    pub fn root(&self) -> &SpatialIndexRoot {
        &self.root
    }

    /// Get the base transaction time.
    pub fn base_t(&self) -> i64 {
        self.root.base_t
    }

    /// Get the index transaction time.
    pub fn index_t(&self) -> i64 {
        self.root.index_t
    }

    /// Get geometry metadata by handle.
    pub fn get_metadata(&self, handle: u32) -> Option<&GeometryMetadata> {
        self.arena.get(handle).map(|e| &e.metadata)
    }

    /// Get a reference to the cell index reader.
    pub fn cell_index(&self) -> &CellIndexReader {
        &self.cell_index
    }

    /// Get a reference to the geometry arena.
    pub fn arena(&self) -> &GeometryArena {
        &self.arena
    }

    /// Slice novelty entries by cell_id range using binary search.
    ///
    /// Since novelty is sorted by `(cell_id, subject_id, t DESC, op ASC)`,
    /// we can binary search for the start and end of the cell_id range.
    fn novelty_in_range(&self, min_cell: u64, max_cell: u64) -> &[CellEntry] {
        if self.novelty.is_empty() {
            return &[];
        }

        // Find first entry with cell_id >= min_cell
        let start = self.novelty.partition_point(|e| e.cell_id < min_cell);

        // Find first entry with cell_id > max_cell
        let end = self.novelty[start..].partition_point(|e| e.cell_id <= max_cell) + start;

        &self.novelty[start..end]
    }

    /// Query subjects within a radius of a point.
    ///
    /// Returns subject IDs with their distances, sorted by distance.
    pub fn query_radius(
        &self,
        center_lat: f64,
        center_lng: f64,
        radius_meters: f64,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, f64)>> {
        // Check time-travel coverage
        if to_t < self.root.base_t {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.root.base_t,
            });
        }

        // Generate covering for the query circle
        let cells = covering_for_circle(
            center_lat,
            center_lng,
            radius_meters,
            &self.root.config.s2_config,
        )?;

        let ranges = cells_to_ranges(&cells);

        // Scan all ranges and collect entries
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in ranges {
            let snapshot_entries = self.cell_index.scan_range(min_cell, max_cell)?;

            // Slice novelty using binary search (O(log n) vs O(n))
            let novelty_slice = self.novelty_in_range(min_cell, max_cell);

            // Merge snapshot + novelty (novelty wins on ties)
            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            // Replay at to_t
            let replayed = ReplayResolver::new(merged, to_t);

            all_entries.extend(replayed);
        }

        // Dedup by subject_id, keeping min distance
        let mut dedup = StreamingDedup::new(DedupStrategy::KeepMinDistance);

        for entry in all_entries {
            // Get geometry metadata for bbox prefilter
            if let Some(meta) = self.get_metadata(entry.geo_handle) {
                // Conservative bbox prefilter: reject only if bbox is definitely outside circle
                // This prevents false negatives while reducing exact distance computations
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
                        // Bbox is definitely outside circle - safe to reject
                        continue;
                    }
                }

                // Exact distance: parse geometry and compute min distance
                if let Ok(geom) = self.arena.parse_geometry(entry.geo_handle) {
                    let distance = min_distance_to_geometry(center_lat, center_lng, &geom);
                    if distance <= radius_meters {
                        dedup.push(entry, distance);
                    }
                }
                // If parsing fails, skip (conservative: don't include uncertain results)
            }
        }

        let mut results = dedup.finish_sorted_by_distance();

        // Apply limit
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        Ok(results
            .into_iter()
            .map(|e| (e.entry.subject_id, e.distance))
            .collect())
    }

    /// Query subjects whose geometry is within a query geometry.
    ///
    /// Returns subject IDs that pass the "within" spatial predicate.
    pub fn query_within(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<u64>> {
        // Check time-travel coverage
        if to_t < self.root.base_t {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.root.base_t,
            });
        }

        // Generate covering for the query geometry
        let cells = covering_for_geometry(query_geom, &self.root.config.s2_config)?;
        let ranges = cells_to_ranges(&cells);

        // Get query bbox for prefiltering
        let query_bbox = crate::geometry::BBox::from_geometry(query_geom);

        // Scan all ranges
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in ranges {
            let snapshot_entries = self.cell_index.scan_range(min_cell, max_cell)?;

            // Slice novelty using binary search (O(log n) vs O(n))
            let novelty_slice = self.novelty_in_range(min_cell, max_cell);

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }

        // Dedup by subject_id
        let deduped = crate::dedup::dedup_keep_first(all_entries);

        // Filter with bbox prefilter + exact predicate
        let mut results = Vec::new();

        for entry in deduped {
            if let Some(meta) = self.get_metadata(entry.geo_handle) {
                // Bbox prefilter
                if let (Some(query_bbox), Some(entry_bbox)) = (&query_bbox, &meta.bbox) {
                    if !query_bbox.intersects(entry_bbox) {
                        continue;
                    }
                }

                // Exact predicate: entry geometry must be within query geometry
                if let Ok(entry_geom) = self.arena.parse_geometry(entry.geo_handle) {
                    if entry_geom.is_within(query_geom) {
                        results.push(entry.subject_id);

                        if let Some(lim) = limit {
                            if results.len() >= lim {
                                break;
                            }
                        }
                    }
                }
                // If parsing fails, skip this entry (conservative approach)
            }
        }

        Ok(results)
    }

    /// Query subjects whose geometry contains the query geometry.
    ///
    /// Returns subject IDs that pass the "contains" spatial predicate.
    pub fn query_contains(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<u64>> {
        // Check time-travel coverage
        if to_t < self.root.base_t {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.root.base_t,
            });
        }

        // Generate covering for the query geometry
        let cells = covering_for_geometry(query_geom, &self.root.config.s2_config)?;
        let ranges = cells_to_ranges(&cells);

        // Get query bbox for prefiltering
        let query_bbox = crate::geometry::BBox::from_geometry(query_geom);

        // Scan all ranges
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in ranges {
            let snapshot_entries = self.cell_index.scan_range(min_cell, max_cell)?;
            let novelty_slice = self.novelty_in_range(min_cell, max_cell);

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }

        // Dedup by subject_id
        let deduped = crate::dedup::dedup_keep_first(all_entries);

        // Filter with bbox prefilter + exact predicate
        let mut results = Vec::new();

        for entry in deduped {
            if let Some(meta) = self.get_metadata(entry.geo_handle) {
                // Bbox prefilter: entry bbox must contain query bbox
                if let (Some(query_bbox), Some(entry_bbox)) = (&query_bbox, &meta.bbox) {
                    if !entry_bbox.contains_bbox(query_bbox) {
                        continue;
                    }
                }

                // Exact predicate: entry geometry must contain query geometry
                if let Ok(entry_geom) = self.arena.parse_geometry(entry.geo_handle) {
                    if entry_geom.contains(query_geom) {
                        results.push(entry.subject_id);

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

    /// Query subjects whose geometry intersects the query geometry.
    ///
    /// Returns subject IDs that pass the "intersects" spatial predicate.
    pub fn query_intersects(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<Vec<u64>> {
        // Check time-travel coverage
        if to_t < self.root.base_t {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.root.base_t,
            });
        }

        // Generate covering for the query geometry
        let cells = covering_for_geometry(query_geom, &self.root.config.s2_config)?;
        let ranges = cells_to_ranges(&cells);

        // Get query bbox for prefiltering
        let query_bbox = crate::geometry::BBox::from_geometry(query_geom);

        // Scan all ranges
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in ranges {
            let snapshot_entries = self.cell_index.scan_range(min_cell, max_cell)?;
            let novelty_slice = self.novelty_in_range(min_cell, max_cell);

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }

        // Dedup by subject_id
        let deduped = crate::dedup::dedup_keep_first(all_entries);

        // Filter with bbox prefilter + exact predicate
        let mut results = Vec::new();

        for entry in deduped {
            if let Some(meta) = self.get_metadata(entry.geo_handle) {
                // Bbox prefilter
                if let (Some(query_bbox), Some(entry_bbox)) = (&query_bbox, &meta.bbox) {
                    if !query_bbox.intersects(entry_bbox) {
                        continue;
                    }
                }

                // Exact predicate: entry geometry must intersect query geometry
                if let Ok(entry_geom) = self.arena.parse_geometry(entry.geo_handle) {
                    if entry_geom.intersects(query_geom) {
                        results.push(entry.subject_id);

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

    // ========================================================================
    // Instrumented Query Methods (with stats)
    // ========================================================================

    /// Query within with detailed statistics for benchmarking.
    pub fn query_within_with_stats(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<(Vec<u64>, QueryStats)> {
        let mut stats = QueryStats::default();

        // Check time-travel coverage
        if to_t < self.root.base_t {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.root.base_t,
            });
        }

        // Generate covering
        let cells = covering_for_geometry(query_geom, &self.root.config.s2_config)?;
        stats.covering_cells = cells.len();

        let ranges = cells_to_ranges(&cells);
        stats.ranges_scanned = ranges.len();

        let query_bbox = crate::geometry::BBox::from_geometry(query_geom);

        // Scan all ranges
        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in &ranges {
            let snapshot_entries = self.cell_index.scan_range(*min_cell, *max_cell)?;
            stats.snapshot_entries += snapshot_entries.len();

            let novelty_slice = self.novelty_in_range(*min_cell, *max_cell);
            stats.novelty_entries += novelty_slice.len();

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }
        stats.after_replay = all_entries.len();

        // Dedup
        let deduped = crate::dedup::dedup_keep_first(all_entries);
        stats.after_dedup = deduped.len();

        // Filter
        let mut results = Vec::new();
        for entry in deduped {
            if let Some(meta) = self.get_metadata(entry.geo_handle) {
                // Bbox prefilter
                if let (Some(query_bbox), Some(entry_bbox)) = (&query_bbox, &meta.bbox) {
                    if !query_bbox.intersects(entry_bbox) {
                        continue;
                    }
                }
                stats.passed_bbox += 1;

                // Exact predicate
                if let Ok(entry_geom) = self.arena.parse_geometry(entry.geo_handle) {
                    stats.exact_checks += 1;
                    if entry_geom.is_within(query_geom) {
                        results.push(entry.subject_id);
                        if let Some(lim) = limit {
                            if results.len() >= lim {
                                break;
                            }
                        }
                    }
                }
            }
        }
        stats.result_count = results.len();

        Ok((results, stats))
    }

    /// Query intersects with detailed statistics for benchmarking.
    pub fn query_intersects_with_stats(
        &self,
        query_geom: &Geometry<f64>,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<(Vec<u64>, QueryStats)> {
        let mut stats = QueryStats::default();

        if to_t < self.root.base_t {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.root.base_t,
            });
        }

        let cells = covering_for_geometry(query_geom, &self.root.config.s2_config)?;
        stats.covering_cells = cells.len();

        let ranges = cells_to_ranges(&cells);
        stats.ranges_scanned = ranges.len();

        let query_bbox = crate::geometry::BBox::from_geometry(query_geom);

        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in &ranges {
            let snapshot_entries = self.cell_index.scan_range(*min_cell, *max_cell)?;
            stats.snapshot_entries += snapshot_entries.len();

            let novelty_slice = self.novelty_in_range(*min_cell, *max_cell);
            stats.novelty_entries += novelty_slice.len();

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }
        stats.after_replay = all_entries.len();

        let deduped = crate::dedup::dedup_keep_first(all_entries);
        stats.after_dedup = deduped.len();

        let mut results = Vec::new();
        for entry in deduped {
            if let Some(meta) = self.get_metadata(entry.geo_handle) {
                if let (Some(query_bbox), Some(entry_bbox)) = (&query_bbox, &meta.bbox) {
                    if !query_bbox.intersects(entry_bbox) {
                        continue;
                    }
                }
                stats.passed_bbox += 1;

                if let Ok(entry_geom) = self.arena.parse_geometry(entry.geo_handle) {
                    stats.exact_checks += 1;
                    if entry_geom.intersects(query_geom) {
                        results.push(entry.subject_id);
                        if let Some(lim) = limit {
                            if results.len() >= lim {
                                break;
                            }
                        }
                    }
                }
            }
        }
        stats.result_count = results.len();

        Ok((results, stats))
    }

    /// Query radius with detailed statistics for benchmarking.
    pub fn query_radius_with_stats(
        &self,
        center_lat: f64,
        center_lng: f64,
        radius_meters: f64,
        to_t: i64,
        limit: Option<usize>,
    ) -> Result<(Vec<(u64, f64)>, QueryStats)> {
        let mut stats = QueryStats::default();

        if to_t < self.root.base_t {
            return Err(SpatialError::TimeRangeNotCovered {
                requested_t: to_t,
                base_t: self.root.base_t,
            });
        }

        let cells = covering_for_circle(
            center_lat,
            center_lng,
            radius_meters,
            &self.root.config.s2_config,
        )?;
        stats.covering_cells = cells.len();

        let ranges = cells_to_ranges(&cells);
        stats.ranges_scanned = ranges.len();

        let mut all_entries = Vec::new();
        for (min_cell, max_cell) in &ranges {
            let snapshot_entries = self.cell_index.scan_range(*min_cell, *max_cell)?;
            stats.snapshot_entries += snapshot_entries.len();

            let novelty_slice = self.novelty_in_range(*min_cell, *max_cell);
            stats.novelty_entries += novelty_slice.len();

            let merged =
                MergeSorted::new(snapshot_entries.into_iter(), novelty_slice.iter().copied());

            let replayed = ReplayResolver::new(merged, to_t);
            all_entries.extend(replayed);
        }
        stats.after_replay = all_entries.len();

        let mut dedup = StreamingDedup::new(DedupStrategy::KeepMinDistance);

        for entry in all_entries {
            if let Some(meta) = self.get_metadata(entry.geo_handle) {
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
                stats.passed_bbox += 1;

                if let Ok(geom) = self.arena.parse_geometry(entry.geo_handle) {
                    stats.exact_checks += 1;
                    let distance = min_distance_to_geometry(center_lat, center_lng, &geom);
                    if distance <= radius_meters {
                        dedup.push(entry, distance);
                    }
                }
            }
        }

        let mut results = dedup.finish_sorted_by_distance();
        stats.after_dedup = results.len(); // For radius, dedup happens after

        if let Some(lim) = limit {
            results.truncate(lim);
        }
        stats.result_count = results.len();

        Ok((
            results
                .into_iter()
                .map(|e| (e.entry.subject_id, e.distance))
                .collect(),
            stats,
        ))
    }
}

/// Haversine distance between two points in meters.
pub(crate) fn haversine_distance(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lng = (lng2 - lng1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_METERS * c
}

/// Minimum distance from a point to a bounding box (in meters).
///
/// Returns 0 if the point is inside the bbox.
/// This is a conservative lower bound - the actual distance to any
/// geometry within this bbox is >= this value.
pub(crate) fn min_distance_to_bbox(
    lat: f64,
    lng: f64,
    min_lat: f64,
    max_lat: f64,
    min_lng: f64,
    max_lng: f64,
) -> f64 {
    // Clamp the point to the bbox to find the closest point on the bbox boundary
    let closest_lat = lat.clamp(min_lat, max_lat);
    let closest_lng = lng.clamp(min_lng, max_lng);

    // If the point is inside the bbox, distance is 0
    if closest_lat == lat && closest_lng == lng {
        return 0.0;
    }

    // Otherwise compute haversine distance to the closest bbox corner/edge point
    haversine_distance(lat, lng, closest_lat, closest_lng)
}

/// Minimum distance from a point to a geometry (in meters).
///
/// Computes the minimum haversine distance from the query point to any
/// point on the geometry.
///
/// # Correctness
///
/// This function is designed to be **conservative** for query filtering:
/// - The returned distance is always <= the true minimum distance
/// - This means we may include geometries that are slightly farther (false positives)
/// - But we never exclude geometries that should be included (no false negatives)
///
/// For most geometry types, we compute the exact minimum distance. For
/// unhandled types, we fall back to the minimum distance to the bounding box.
pub(crate) fn min_distance_to_geometry(lat: f64, lng: f64, geom: &Geometry<f64>) -> f64 {
    match geom {
        Geometry::Point(p) => haversine_distance(lat, lng, p.y(), p.x()),
        Geometry::MultiPoint(mp) => mp
            .iter()
            .map(|p| haversine_distance(lat, lng, p.y(), p.x()))
            .fold(f64::INFINITY, f64::min),
        Geometry::Line(line) => {
            // Line is a single segment from start to end
            min_distance_to_segment(lat, lng, line.start.y, line.start.x, line.end.y, line.end.x)
        }
        Geometry::LineString(ls) => min_distance_to_linestring(lat, lng, ls),
        Geometry::MultiLineString(mls) => mls
            .iter()
            .map(|ls| min_distance_to_linestring(lat, lng, ls))
            .fold(f64::INFINITY, f64::min),
        Geometry::Triangle(tri) => {
            // Triangle: check containment first, then distance to edges
            use geo::Contains;
            let point = geo_types::Point::new(lng, lat);
            if tri.contains(&point) {
                return 0.0;
            }
            // Distance to the three edges
            let d1 = min_distance_to_segment(lat, lng, tri.0.y, tri.0.x, tri.1.y, tri.1.x);
            let d2 = min_distance_to_segment(lat, lng, tri.1.y, tri.1.x, tri.2.y, tri.2.x);
            let d3 = min_distance_to_segment(lat, lng, tri.2.y, tri.2.x, tri.0.y, tri.0.x);
            d1.min(d2).min(d3)
        }
        Geometry::Rect(rect) => {
            // Rect: check containment first, then distance to edges
            use geo::Contains;
            let point = geo_types::Point::new(lng, lat);
            if rect.contains(&point) {
                return 0.0;
            }
            // The rect has min/max coords
            let min = rect.min();
            let max = rect.max();
            // Distance to the four edges
            let d1 = min_distance_to_segment(lat, lng, min.y, min.x, min.y, max.x); // bottom
            let d2 = min_distance_to_segment(lat, lng, min.y, max.x, max.y, max.x); // right
            let d3 = min_distance_to_segment(lat, lng, max.y, max.x, max.y, min.x); // top
            let d4 = min_distance_to_segment(lat, lng, max.y, min.x, min.y, min.x); // left
            d1.min(d2).min(d3).min(d4)
        }
        Geometry::Polygon(poly) => {
            // Check if point is inside polygon first (handles holes correctly)
            use geo::Contains;
            let point = geo_types::Point::new(lng, lat);
            if poly.contains(&point) {
                return 0.0;
            }
            // Point is outside polygon OR inside a hole.
            // Compute min distance to ALL rings (exterior + interior holes).
            // This is critical: if point is in a hole, distance to hole boundary
            // is the true min distance, not distance to exterior.
            let mut min_dist = min_distance_to_linestring(lat, lng, poly.exterior());
            for interior in poly.interiors() {
                min_dist = min_dist.min(min_distance_to_linestring(lat, lng, interior));
            }
            min_dist
        }
        Geometry::MultiPolygon(mp) => {
            mp.iter()
                .map(|poly| {
                    use geo::Contains;
                    let point = geo_types::Point::new(lng, lat);
                    if poly.contains(&point) {
                        0.0
                    } else {
                        // Same logic: min distance to all rings
                        let mut min_dist = min_distance_to_linestring(lat, lng, poly.exterior());
                        for interior in poly.interiors() {
                            min_dist = min_dist.min(min_distance_to_linestring(lat, lng, interior));
                        }
                        min_dist
                    }
                })
                .fold(f64::INFINITY, f64::min)
        }
        Geometry::GeometryCollection(gc) => gc
            .iter()
            .map(|g| min_distance_to_geometry(lat, lng, g))
            .fold(f64::INFINITY, f64::min),
        // Future-proofing: if geo_types adds new variants, use conservative bbox distance.
        // min_distance_to_bbox is always <= actual distance, so no false negatives.
        #[allow(unreachable_patterns)]
        _ => {
            if let Some(bbox) = crate::geometry::BBox::from_geometry(geom) {
                min_distance_to_bbox(
                    lat,
                    lng,
                    bbox.min_lat,
                    bbox.max_lat,
                    bbox.min_lng,
                    bbox.max_lng,
                )
            } else {
                // No bbox and unknown type - return 0 to be maximally conservative
                0.0
            }
        }
    }
}

/// Minimum distance from a point to a linestring (in meters).
fn min_distance_to_linestring(lat: f64, lng: f64, ls: &geo_types::LineString<f64>) -> f64 {
    if ls.0.is_empty() {
        return f64::INFINITY;
    }

    let mut min_dist = f64::INFINITY;

    // Check distance to each segment
    for window in ls.0.windows(2) {
        let (p1, p2) = (&window[0], &window[1]);
        let dist = min_distance_to_segment(lat, lng, p1.y, p1.x, p2.y, p2.x);
        min_dist = min_dist.min(dist);
    }

    min_dist
}

/// Minimum distance from a point to a line segment (in meters).
///
/// Uses spherical interpolation approximation for short segments.
fn min_distance_to_segment(lat: f64, lng: f64, lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    // For short segments, we can use a planar approximation with haversine verification
    // This finds the closest point on the segment and computes the distance

    // Vector from p1 to p2
    let dx = lng2 - lng1;
    let dy = lat2 - lat1;

    // If segment is a point
    if dx == 0.0 && dy == 0.0 {
        return haversine_distance(lat, lng, lat1, lng1);
    }

    // Project point onto the line, clamped to segment
    let t = ((lng - lng1) * dx + (lat - lat1) * dy) / (dx * dx + dy * dy);
    let t = t.clamp(0.0, 1.0);

    // Closest point on segment
    let closest_lng = lng1 + t * dx;
    let closest_lat = lat1 + t * dy;

    haversine_distance(lat, lng, closest_lat, closest_lng)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{coord, Line, Rect, Triangle};

    #[test]
    fn test_haversine_distance() {
        // Paris to London: ~343 km
        let paris = (48.8566, 2.3522);
        let london = (51.5074, -0.1278);

        let distance = haversine_distance(paris.0, paris.1, london.0, london.1);

        // Should be approximately 343.5 km
        assert!((distance - 343_500.0).abs() < 5000.0);
    }

    #[test]
    fn test_min_distance_to_line() {
        // Line from (0, 0) to (0, 10) (vertical line along lat=0)
        let line = Geometry::Line(Line::new(
            coord! { x: 0.0, y: 0.0 },  // lng=0, lat=0
            coord! { x: 10.0, y: 0.0 }, // lng=10, lat=0
        ));

        // Point at (5, 1) - should be ~111km from the line (1 degree of latitude)
        let dist = min_distance_to_geometry(1.0, 5.0, &line);
        // 1 degree latitude ≈ 111km
        assert!((dist - 111_000.0).abs() < 5000.0);

        // Point exactly on the line at (5, 0)
        let dist_on_line = min_distance_to_geometry(0.0, 5.0, &line);
        assert!(dist_on_line < 1.0); // Should be essentially 0
    }

    #[test]
    fn test_min_distance_to_rect_outside() {
        // Rectangle from (0, 0) to (10, 10)
        let rect = Geometry::Rect(Rect::new(
            coord! { x: 0.0, y: 0.0 },   // min corner
            coord! { x: 10.0, y: 10.0 }, // max corner
        ));

        // Point at (5, 12) - 2 degrees north of the rect
        let dist = min_distance_to_geometry(12.0, 5.0, &rect);
        // Should be ~222km (2 degrees of latitude)
        assert!((dist - 222_000.0).abs() < 10000.0);
    }

    #[test]
    fn test_min_distance_to_rect_inside() {
        // Rectangle from (0, 0) to (10, 10)
        let rect = Geometry::Rect(Rect::new(
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 10.0, y: 10.0 },
        ));

        // Point at (5, 5) - inside the rect
        let dist = min_distance_to_geometry(5.0, 5.0, &rect);
        assert_eq!(dist, 0.0);
    }

    #[test]
    fn test_min_distance_to_triangle_outside() {
        // Triangle with vertices at (0,0), (10,0), (5,10)
        let tri = Geometry::Triangle(Triangle::new(
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 10.0, y: 0.0 },
            coord! { x: 5.0, y: 10.0 },
        ));

        // Point at (5, -2) - 2 degrees south of the base
        let dist = min_distance_to_geometry(-2.0, 5.0, &tri);
        // Should be ~222km (2 degrees of latitude)
        assert!((dist - 222_000.0).abs() < 10000.0);
    }

    #[test]
    fn test_min_distance_to_triangle_inside() {
        // Triangle with vertices at (0,0), (10,0), (5,10)
        let tri = Geometry::Triangle(Triangle::new(
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 10.0, y: 0.0 },
            coord! { x: 5.0, y: 10.0 },
        ));

        // Point at (5, 3) - inside the triangle
        let dist = min_distance_to_geometry(3.0, 5.0, &tri);
        assert_eq!(dist, 0.0);
    }

    #[test]
    fn test_min_distance_to_bbox_conservative() {
        // Verify that min_distance_to_bbox is always <= true geometry distance
        // This is critical for preventing false negatives

        // A small polygon inside a larger bbox
        let wkt = "POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))";
        let geom = crate::geometry::parse_wkt(wkt).unwrap();
        let bbox = crate::geometry::BBox::from_geometry(&geom).unwrap();

        // Point at (0, 0) - far from both bbox and geometry
        let bbox_dist = min_distance_to_bbox(
            0.0,
            0.0,
            bbox.min_lat,
            bbox.max_lat,
            bbox.min_lng,
            bbox.max_lng,
        );
        let geom_dist = min_distance_to_geometry(0.0, 0.0, &geom);

        // bbox distance should be <= geometry distance (conservative)
        assert!(
            bbox_dist <= geom_dist,
            "bbox_dist ({bbox_dist}) should be <= geom_dist ({geom_dist})"
        );
    }

    #[test]
    fn test_min_distance_polygon_near_edge() {
        // Test that we correctly compute distance to polygon edge, not center
        // This is the key fix - using center would give ~785km, edge gives ~555km

        // Square from (0,0) to (10,10)
        let wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
        let geom = crate::geometry::parse_wkt(wkt).unwrap();

        // Point at (5, -5) - 5 degrees south of the bottom edge
        let dist = min_distance_to_geometry(-5.0, 5.0, &geom);

        // Should be ~555km (5 degrees of latitude)
        // NOT ~785km which would be distance to center (5, 5)
        assert!(
            (dist - 555_000.0).abs() < 30000.0,
            "Expected ~555km to edge, got {dist} meters"
        );
    }

    #[test]
    fn test_no_false_negatives_for_radius_query() {
        // Simulate a radius query scenario where using center distance would cause
        // a false negative

        // Polygon spanning (0,0) to (20,20), center at (10, 10)
        let wkt = "POLYGON((0 0, 20 0, 20 20, 0 20, 0 0))";
        let geom = crate::geometry::parse_wkt(wkt).unwrap();

        // Query point at (10, -2) with radius 300km
        // Distance to center (10, 10) ≈ 1334km - would FAIL if using center
        // Distance to edge (10, 0) ≈ 222km - should PASS
        let query_lat = -2.0;
        let query_lng = 10.0;
        let radius_meters = 300_000.0;

        let dist = min_distance_to_geometry(query_lat, query_lng, &geom);

        // The polygon should be within the radius (edge is ~222km away)
        assert!(
            dist <= radius_meters,
            "Polygon edge at ~222km should be within {radius_meters}m radius, but got {dist}m"
        );
    }

    #[test]
    fn test_polygon_with_hole_distance() {
        // Polygon with a hole: outer ring (0,0)-(10,10), hole (3,3)-(7,7)
        // Point inside the hole should have distance to hole boundary, not exterior
        let wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))";
        let geom = crate::geometry::parse_wkt(wkt).unwrap();

        // Point at (5, 5) - center of the hole
        // Distance to hole boundary (closest is 2 degrees at (5,3) or (5,7) or (3,5) or (7,5))
        // Distance to exterior would be 5 degrees
        let dist = min_distance_to_geometry(5.0, 5.0, &geom);

        // Should be ~222km (2 degrees to hole boundary), NOT ~555km (5 degrees to exterior)
        assert!(
            dist < 300_000.0,
            "Point in hole should be ~222km from hole boundary, got {dist}m"
        );
        assert!(
            (dist - 222_000.0).abs() < 30000.0,
            "Expected ~222km to hole boundary, got {dist}m"
        );
    }

    #[test]
    fn test_polygon_with_hole_no_false_negative() {
        // Regression test: radius query that would false-negative if we only
        // checked distance to exterior ring

        // Polygon with hole
        let wkt = "POLYGON((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))";
        let geom = crate::geometry::parse_wkt(wkt).unwrap();

        // Point at (10, 10) - center of the 10x10 hole
        // Distance to exterior: 10 degrees (~1111km)
        // Distance to hole boundary: 5 degrees (~555km)
        let query_lat = 10.0;
        let query_lng = 10.0;
        let radius_meters = 600_000.0; // 600km

        let dist = min_distance_to_geometry(query_lat, query_lng, &geom);

        // Should find the polygon within radius (hole boundary is ~555km away)
        // Would FAIL if only checking exterior (1111km > 600km)
        assert!(
            dist <= radius_meters,
            "Polygon hole boundary at ~555km should be within {radius_meters}m radius, but got {dist}m"
        );
    }
}
