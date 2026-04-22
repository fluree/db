//! Spatial index builder.
//!
//! Builds a spatial index from geometry data. The builder:
//! 1. Accepts (subject_id, wkt, t, op) records for geometries
//! 2. Parses WKT and computes metadata (bbox, centroid)
//! 3. Generates S2 cell coverings
//! 4. Produces CellEntry records sorted by (cell_id, subject_id, t DESC, op ASC)
//! 5. Chunks into leaflets and writes to CAS
//!
//! # Usage
//!
//! ```ignore
//! let config = SpatialCreateConfig::new("geo:main", "ledger:mydata", "geo:asWKT");
//! let mut builder = SpatialIndexBuilder::new(config);
//!
//! // Add geometry records from commits
//! builder.add_geometry(subject_id, "POLYGON((...))", t, true)?;  // assert
//! builder.add_geometry(subject_id, "POLYGON((...))", t, false)?; // retract
//!
//! // Finalize and write to CAS
//! let root = builder.finalize(cas_store).await?;
//! ```

use crate::cell_index::{CellEntry, CellIndexBuilder};
use crate::config::{SpatialConfig, SpatialCreateConfig};
use crate::covering::covering_for_geometry;
use crate::error::{Result, SpatialError};
use crate::geometry::{parse_wkt, GeometryArena, GeometryType};
use crate::snapshot::SpatialIndexRoot;

/// Result of writing a spatial index to CAS.
///
/// Contains the root manifest and all CAS addresses written.
/// Used by the indexer to populate `SpatialIndexRef` for GC tracking.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// The root manifest.
    pub root: SpatialIndexRoot,
    /// CAS address of the cell index manifest.
    pub manifest_address: String,
    /// CAS address of the geometry arena.
    pub arena_address: String,
    /// CAS addresses of all leaflet chunks.
    pub leaflet_addresses: Vec<String>,
}

/// Statistics collected during index building.
#[derive(Debug, Clone, Default)]
pub struct BuildStats {
    /// Number of geometry records processed.
    pub records_processed: u64,

    /// Number of valid geometries added.
    pub geometries_added: u64,

    /// Number of records skipped (parse errors, etc.).
    pub records_skipped: u64,

    /// Total cell entries generated.
    pub cell_entries: u64,

    /// Number of distinct subjects.
    pub distinct_subjects: u64,

    /// Number of points (if tracked).
    pub point_count: u64,

    /// Number of polygons (if tracked).
    pub polygon_count: u64,

    /// Number of other geometry types.
    pub other_count: u64,
}

/// Builder for spatial indexes.
///
/// Accumulates geometry records and produces a spatial index
/// that can be persisted to CAS.
pub struct SpatialIndexBuilder {
    /// Configuration used for building.
    config: SpatialCreateConfig,

    /// Geometry arena (deduplicates WKT strings).
    arena: GeometryArena,

    /// Accumulated cell entries.
    entries: Vec<CellEntry>,

    /// Min transaction time seen.
    min_t: i64,

    /// Max transaction time seen.
    max_t: i64,

    /// Build statistics.
    stats: BuildStats,

    /// Subject IDs seen (for distinct count).
    seen_subjects: rustc_hash::FxHashSet<u64>,
}

impl SpatialIndexBuilder {
    /// Create a new builder with the given configuration.
    pub fn new(config: SpatialCreateConfig) -> Self {
        Self {
            config,
            arena: GeometryArena::new(),
            entries: Vec::new(),
            min_t: i64::MAX,
            max_t: i64::MIN,
            stats: BuildStats::default(),
            seen_subjects: rustc_hash::FxHashSet::default(),
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &SpatialCreateConfig {
        &self.config
    }

    /// Get current build statistics.
    pub fn stats(&self) -> &BuildStats {
        &self.stats
    }

    /// Add a geometry record to the index.
    ///
    /// # Arguments
    ///
    /// * `subject_id` - Subject ID that owns this geometry
    /// * `wkt` - WKT string for the geometry
    /// * `t` - Transaction time
    /// * `is_assert` - true for assertion, false for retraction
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if the geometry was successfully added
    /// - `Ok(false)` if skipped (WKT parse error, POINT when `index_points=false`)
    ///
    /// Parse errors are logged and counted in `stats.records_skipped`, not propagated
    /// as errors. This allows batch processing to continue on malformed input.
    pub fn add_geometry(
        &mut self,
        subject_id: u64,
        wkt: &str,
        t: i64,
        is_assert: bool,
    ) -> Result<bool> {
        self.stats.records_processed += 1;

        // Parse WKT
        let geom = match parse_wkt(wkt) {
            Ok(g) => g,
            Err(e) => {
                self.stats.records_skipped += 1;
                tracing::debug!(
                    subject_id = subject_id,
                    error = %e,
                    "Failed to parse WKT"
                );
                return Ok(false);
            }
        };

        // Check if this is a point and if we should skip it
        let geom_type = crate::geometry::GeometryType::from_geometry(&geom);
        if geom_type.is_point() && !self.config.index_points {
            self.stats.records_skipped += 1;
            tracing::trace!(
                subject_id = subject_id,
                "Skipping POINT geometry (index_points=false)"
            );
            return Ok(false);
        }

        // Update geometry type stats
        match geom_type {
            GeometryType::Point | GeometryType::MultiPoint => {
                self.stats.point_count += 1;
            }
            GeometryType::Polygon | GeometryType::MultiPolygon => {
                self.stats.polygon_count += 1;
            }
            _ => {
                self.stats.other_count += 1;
            }
        }

        // Add to arena (deduplicates by WKT hash)
        let geo_handle = self.arena.add(wkt, &self.config.metadata_config)?;

        // Generate S2 covering
        let cells = covering_for_geometry(&geom, &self.config.s2_config)?;
        let cell_count = cells.len();

        // Create cell entries for each covering cell
        let op = u8::from(is_assert);
        for cell_id in cells {
            self.entries
                .push(CellEntry::new(cell_id, subject_id, geo_handle, t, op));
        }

        // Update statistics
        self.stats.geometries_added += 1;
        self.stats.cell_entries += cell_count as u64;
        self.seen_subjects.insert(subject_id);

        // Update time bounds
        self.min_t = self.min_t.min(t);
        self.max_t = self.max_t.max(t);

        Ok(true)
    }

    /// Add a retraction by subject_id and geo_handle.
    ///
    /// This is used when you know the geo_handle from a previous assertion
    /// and want to retract it without re-parsing the WKT.
    pub fn add_retraction(&mut self, subject_id: u64, geo_handle: u32, t: i64) -> Result<()> {
        // Get the geometry from the arena to generate matching covering
        let entry = self.arena.get(geo_handle).ok_or_else(|| {
            SpatialError::InvalidGeometry(format!("Unknown geo_handle: {geo_handle}"))
        })?;

        let wkt_str = std::str::from_utf8(&entry.wkt)
            .map_err(|e| SpatialError::WktParse(format!("Invalid UTF-8: {e}")))?;
        let geom = parse_wkt(wkt_str)?;

        // Generate the same S2 covering
        let cells = covering_for_geometry(&geom, &self.config.s2_config)?;
        let cell_count = cells.len();

        // Create retraction entries
        for cell_id in cells {
            self.entries.push(CellEntry::new(
                cell_id, subject_id, geo_handle, t, 0, // retract
            ));
        }

        self.stats.records_processed += 1;
        self.stats.cell_entries += cell_count as u64;
        self.min_t = self.min_t.min(t);
        self.max_t = self.max_t.max(t);

        Ok(())
    }

    /// Get the number of cell entries accumulated.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Get the number of distinct geometries in the arena.
    pub fn geometry_count(&self) -> usize {
        self.arena.len()
    }

    /// Finalize the builder, sorting entries and preparing for serialization.
    ///
    /// Returns the sorted entries and arena. Use `build_index` for full
    /// CAS persistence.
    pub fn finalize(mut self) -> (Vec<CellEntry>, GeometryArena, BuildStats) {
        // Update distinct subjects count
        self.stats.distinct_subjects = self.seen_subjects.len() as u64;

        // Sort entries by index order: (cell_id, subject_id, t DESC, op ASC)
        self.entries
            .sort_by(super::cell_index::CellEntry::cmp_index);

        (self.entries, self.arena, self.stats)
    }

    /// Finalize and build the complete index structure.
    ///
    /// This creates the cell index builder with entries ready for CAS upload.
    pub fn build(self) -> Result<BuildResult> {
        let config = self.config.clone();
        let min_t = self.min_t;
        let max_t = self.max_t;

        let (entries, arena, stats) = self.finalize();

        // Handle empty index
        if entries.is_empty() {
            return Ok(BuildResult {
                config,
                entries: Vec::new(),
                arena,
                stats,
                min_t: 0,
                max_t: 0,
            });
        }

        Ok(BuildResult {
            config,
            entries,
            arena,
            stats,
            min_t,
            max_t,
        })
    }
}

/// Result of building a spatial index.
pub struct BuildResult {
    /// Configuration used for building.
    pub config: SpatialCreateConfig,

    /// Sorted cell entries.
    pub entries: Vec<CellEntry>,

    /// Geometry arena.
    pub arena: GeometryArena,

    /// Build statistics.
    pub stats: BuildStats,

    /// Min transaction time.
    pub min_t: i64,

    /// Max transaction time.
    pub max_t: i64,
}

impl BuildResult {
    /// Build the cell index and write all components to CAS.
    ///
    /// The `write_bytes` callback is called for each chunk (leaflets, manifest,
    /// arena) with its bytes; it should persist to CAS and return the content hash.
    ///
    /// Returns a `WriteResult` containing the root manifest and all CAS addresses.
    pub fn write_to_cas<F>(self, mut write_bytes: F) -> Result<WriteResult>
    where
        F: FnMut(&[u8]) -> Result<String>,
    {
        // Handle empty index
        if self.entries.is_empty() {
            return Ok(WriteResult {
                root: SpatialIndexRoot {
                    version: SpatialIndexRoot::CURRENT_VERSION,
                    config: SpatialConfig::from_create_config(&self.config),
                    cell_index_hash: String::new(),
                    arena_hash: String::new(),
                    base_t: 0,
                    index_t: 0,
                    entry_count: 0,
                    geometry_count: 0,
                },
                manifest_address: String::new(),
                arena_address: String::new(),
                leaflet_addresses: Vec::new(),
            });
        }

        // 1. Build cell index (writes leaflets to CAS via callback)
        let mut cell_builder = CellIndexBuilder::new(self.config.chunk_target_bytes);
        cell_builder.extend(self.entries);
        let manifest = cell_builder.build(&mut write_bytes)?;

        // Collect leaflet addresses from manifest
        let leaflet_addresses: Vec<String> = manifest
            .leaflets
            .iter()
            .map(|l| l.content_hash.clone())
            .collect();

        // 2. Serialize and write the cell index manifest
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(|e| {
            SpatialError::Io(std::io::Error::other(format!(
                "manifest serialization failed: {e}"
            )))
        })?;
        let cell_index_hash = write_bytes(&manifest_bytes)?;

        // 3. Serialize and write the geometry arena
        let arena_bytes = self.arena.to_bytes()?;
        let arena_hash = write_bytes(&arena_bytes)?;

        Ok(WriteResult {
            root: SpatialIndexRoot {
                version: SpatialIndexRoot::CURRENT_VERSION,
                config: SpatialConfig::from_create_config(&self.config),
                cell_index_hash: cell_index_hash.clone(),
                arena_hash: arena_hash.clone(),
                base_t: self.min_t,
                index_t: self.max_t,
                entry_count: self.stats.cell_entries,
                geometry_count: self.arena.len() as u64,
            },
            manifest_address: cell_index_hash,
            arena_address: arena_hash,
            leaflet_addresses,
        })
    }

    /// Get the sorted entries (for testing or custom processing).
    pub fn entries(&self) -> &[CellEntry] {
        &self.entries
    }

    /// Get the geometry arena.
    pub fn arena(&self) -> &GeometryArena {
        &self.arena
    }

    /// Consume and return the arena (for custom processing).
    pub fn into_arena(self) -> GeometryArena {
        self.arena
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SpatialCreateConfig;

    #[test]
    fn test_builder_add_polygon() {
        let config = SpatialCreateConfig::new("geo:test", "ledger:test", "geo:asWKT");
        let mut builder = SpatialIndexBuilder::new(config);

        let wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
        let result = builder.add_geometry(1001, wkt, 100, true).unwrap();

        assert!(result);
        assert_eq!(builder.stats().geometries_added, 1);
        assert!(builder.entry_count() > 0); // Should have at least one cell entry
        assert_eq!(builder.geometry_count(), 1);
    }

    #[test]
    fn test_builder_skip_points_by_default() {
        let config = SpatialCreateConfig::new("geo:test", "ledger:test", "geo:asWKT");
        let mut builder = SpatialIndexBuilder::new(config);

        let wkt = "POINT(0 0)";
        let result = builder.add_geometry(1001, wkt, 100, true).unwrap();

        assert!(!result); // Should be skipped
        assert_eq!(builder.stats().records_skipped, 1);
        assert_eq!(builder.entry_count(), 0);
    }

    #[test]
    fn test_builder_include_points_when_enabled() {
        let config = SpatialCreateConfig::new("geo:test", "ledger:test", "geo:asWKT")
            .with_index_points(true);
        let mut builder = SpatialIndexBuilder::new(config);

        let wkt = "POINT(0 0)";
        let result = builder.add_geometry(1001, wkt, 100, true).unwrap();

        assert!(result);
        assert_eq!(builder.stats().geometries_added, 1);
        assert!(builder.entry_count() > 0);
    }

    #[test]
    fn test_builder_dedup_same_wkt() {
        let config = SpatialCreateConfig::new("geo:test", "ledger:test", "geo:asWKT");
        let mut builder = SpatialIndexBuilder::new(config);

        let wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
        builder.add_geometry(1001, wkt, 100, true).unwrap();
        builder.add_geometry(1002, wkt, 101, true).unwrap(); // Same WKT, different subject

        // Should have 2 subjects but only 1 geometry in arena
        assert_eq!(builder.stats().geometries_added, 2);
        assert_eq!(builder.geometry_count(), 1); // Deduped in arena
    }

    #[test]
    fn test_builder_finalize_sorts_entries() {
        let config = SpatialCreateConfig::new("geo:test", "ledger:test", "geo:asWKT");
        let mut builder = SpatialIndexBuilder::new(config);

        // Add entries out of order
        let wkt1 = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
        let wkt2 = "POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))";

        builder.add_geometry(2000, wkt2, 200, true).unwrap();
        builder.add_geometry(1000, wkt1, 100, true).unwrap();
        builder.add_geometry(1000, wkt1, 150, true).unwrap(); // Same subject, later t

        let (entries, _, _) = builder.finalize();

        // Verify sorted order
        for i in 1..entries.len() {
            assert!(
                entries[i - 1].cmp_index(&entries[i]) != std::cmp::Ordering::Greater,
                "Entries should be sorted"
            );
        }
    }

    #[test]
    fn test_builder_build_result() {
        let config = SpatialCreateConfig::new("geo:test", "ledger:test", "geo:asWKT");
        let mut builder = SpatialIndexBuilder::new(config);

        let wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
        builder.add_geometry(1001, wkt, 100, true).unwrap();

        let result = builder.build().unwrap();

        assert_eq!(result.min_t, 100);
        assert_eq!(result.max_t, 100);
        assert_eq!(result.arena().len(), 1);
        assert!(result.stats.cell_entries > 0);
    }

    #[test]
    fn test_builder_write_to_cas() {
        let config = SpatialCreateConfig::new("geo:test", "ledger:test", "geo:asWKT");
        let mut builder = SpatialIndexBuilder::new(config);

        let wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
        builder.add_geometry(1001, wkt, 100, true).unwrap();

        let result = builder.build().unwrap();

        // Mock CAS write - track what gets written
        let mut writes: Vec<(String, Vec<u8>)> = Vec::new();
        let mut hash_counter = 0u32;

        let write_result = result
            .write_to_cas(|bytes| {
                hash_counter += 1;
                let hash = format!("sha256:{hash_counter:08x}");
                writes.push((hash.clone(), bytes.to_vec()));
                Ok(hash)
            })
            .unwrap();

        assert_eq!(write_result.root.base_t, 100);
        assert_eq!(write_result.root.index_t, 100);
        assert_eq!(write_result.root.geometry_count, 1);

        // Should have written: leaflet(s), manifest, arena
        // At minimum 3 writes (1 leaflet + manifest + arena)
        assert!(
            writes.len() >= 3,
            "expected at least 3 CAS writes, got {}",
            writes.len()
        );

        // Verify addresses are tracked
        assert_eq!(
            write_result.manifest_address,
            write_result.root.cell_index_hash
        );
        assert_eq!(write_result.arena_address, write_result.root.arena_hash);
        assert!(!write_result.leaflet_addresses.is_empty());

        // Verify hashes are real content addresses
        assert!(write_result.root.cell_index_hash.starts_with("sha256:"));
        assert!(write_result.root.arena_hash.starts_with("sha256:"));

        // Verify manifest is valid JSON
        let manifest_write = &writes[writes.len() - 2]; // Second to last is manifest
        let _manifest: crate::cell_index::CellIndexManifest =
            serde_json::from_slice(&manifest_write.1).expect("manifest should be valid JSON");

        // Verify arena can be deserialized
        let arena_write = &writes[writes.len() - 1]; // Last is arena
        let arena = crate::geometry::GeometryArena::from_bytes(&arena_write.1)
            .expect("arena should deserialize");
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn test_arena_roundtrip() {
        use crate::config::MetadataConfig;

        let mut arena = crate::geometry::GeometryArena::new();
        let config = MetadataConfig::default();

        arena
            .add("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", &config)
            .unwrap();
        arena.add("LINESTRING(0 0, 1 1, 2 0)", &config).unwrap();

        let bytes = arena.to_bytes().unwrap();
        let restored = crate::geometry::GeometryArena::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(restored.get(0).unwrap().wkt, arena.get(0).unwrap().wkt);
        assert_eq!(restored.get(1).unwrap().wkt, arena.get(1).unwrap().wkt);
    }

    #[test]
    fn test_full_roundtrip_build_load_query() {
        use crate::snapshot::SpatialIndexSnapshot;
        use std::collections::HashMap;
        use std::sync::{Arc, RwLock};

        // Build an index with some polygons
        let config = SpatialCreateConfig::new("geo:test", "ledger:test", "geo:asWKT");
        let mut builder = SpatialIndexBuilder::new(config);

        // Add a polygon near (0.5, 0.5)
        builder
            .add_geometry(1001, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", 100, true)
            .unwrap();
        // Add a polygon near (10.5, 10.5)
        builder
            .add_geometry(
                1002,
                "POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))",
                100,
                true,
            )
            .unwrap();

        let result = builder.build().unwrap();

        // Mock CAS store
        let cas: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let cas_write = cas.clone();

        // Write to CAS
        let mut counter = 0u32;
        let write_result = result
            .write_to_cas(|bytes| {
                counter += 1;
                let hash = format!("sha256:{counter:08x}");
                cas_write
                    .write()
                    .unwrap()
                    .insert(hash.clone(), bytes.to_vec());
                Ok(hash)
            })
            .unwrap();

        assert_eq!(write_result.root.geometry_count, 2);

        // Load from CAS
        let cas_read = cas.clone();
        let snapshot =
            SpatialIndexSnapshot::load_from_cas(write_result.root.clone(), move |hash| {
                cas_read.read().unwrap().get(hash).cloned().ok_or_else(|| {
                    crate::error::SpatialError::FormatError(format!("hash not found: {hash}"))
                })
            })
            .unwrap();

        assert_eq!(snapshot.base_t(), 100);
        assert_eq!(snapshot.index_t(), 100);

        // Query: find geometries within a larger polygon that contains (0.5, 0.5)
        let query_poly =
            crate::geometry::parse_wkt("POLYGON((-1 -1, 2 -1, 2 2, -1 2, -1 -1))").unwrap();
        let results = snapshot.query_within(&query_poly, 100, None).unwrap();

        // Should find subject 1001 (the polygon at 0,0 to 1,1)
        assert!(results.contains(&1001), "expected to find subject 1001");
        // Should NOT find subject 1002 (the polygon at 10,10 to 11,11)
        assert!(!results.contains(&1002), "should not find subject 1002");
    }
}
