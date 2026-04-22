//! Spatial novelty derivation.
//!
//! Derives spatial index entries from uncommitted overlay operations (novelty).
//! This allows spatial queries to see uncommitted changes before they're persisted.
//!
//! # Design
//!
//! The derivation function:
//! 1. Takes overlay operations from the binary index pipeline
//! 2. Filters for the target predicate and graph
//! 3. Extracts WKT from geo:wktLiteral values
//! 4. Computes S2 coverings matching the index configuration
//! 5. Builds CellEntry records with NOVELTY_HANDLE_FLAG set
//! 6. Returns (Vec<CellEntry>, GeometryArena) for provider injection

use crate::cell_index::CellEntry;
use crate::config::{MetadataConfig, S2CoveringConfig};
use crate::covering::covering_for_geometry;
use crate::error::Result;
use crate::geometry::{GeometryArena, GeometryType};
use crate::provider::NOVELTY_HANDLE_FLAG;
use fluree_db_core::GraphId;

/// Input for novelty derivation - represents a single overlay operation.
///
/// This mirrors the information available from translated overlay ops
/// in the binary index pipeline.
#[derive(Debug, Clone)]
pub struct SpatialNoveltyOp {
    /// Graph ID (0 for default graph).
    pub g_id: GraphId,

    /// Predicate ID.
    pub p_id: u32,

    /// Subject ID.
    pub s_id: u64,

    /// Object value (the WKT string for geo:wktLiteral).
    pub wkt: String,

    /// Transaction time.
    pub t: i64,

    /// Operation: 1 = assert, 0 = retract.
    pub op: u8,
}

/// Result of novelty derivation.
pub struct DerivedNovelty {
    /// Cell entries with NOVELTY_HANDLE_FLAG set.
    pub entries: Vec<CellEntry>,

    /// Arena containing novelty geometries.
    pub arena: GeometryArena,
}

impl DerivedNovelty {
    /// Check if this derivation produced any entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Derive spatial novelty entries from overlay operations.
///
/// Filters operations by predicate and graph, then computes S2 coverings
/// for non-POINT geometries to produce CellEntry records.
///
/// # Arguments
///
/// * `ops` - Iterator of overlay operations with WKT values
/// * `target_p_id` - Predicate ID to filter for
/// * `target_g_id` - Graph ID to filter for
/// * `s2_config` - S2 covering configuration (from loaded index)
/// * `index_points` - Whether to include POINT geometries
///
/// # Returns
///
/// `DerivedNovelty` containing entries with NOVELTY_HANDLE_FLAG set and
/// a GeometryArena for the novelty geometries.
pub fn derive_spatial_novelty(
    ops: impl Iterator<Item = SpatialNoveltyOp>,
    target_p_id: u32,
    target_g_id: GraphId,
    s2_config: &S2CoveringConfig,
    index_points: bool,
) -> Result<DerivedNovelty> {
    let mut arena = GeometryArena::new();
    let mut entries = Vec::new();
    let metadata_config = MetadataConfig::default();

    for op in ops {
        // Filter by predicate and graph
        if op.p_id != target_p_id || op.g_id != target_g_id {
            continue;
        }

        // Parse WKT
        let geom = match crate::geometry::parse_wkt(&op.wkt) {
            Ok(g) => g,
            Err(_) => continue, // Skip invalid WKT
        };

        // Skip points unless configured to index them
        let geom_type = GeometryType::from_geometry(&geom);
        if geom_type.is_point() && !index_points {
            continue;
        }

        // Add to arena (with dedup)
        let handle = arena.add(&op.wkt, &metadata_config)?;

        // Guard: novelty handles must fit in lower 31 bits (< 2^31)
        // to avoid collision with NOVELTY_HANDLE_FLAG
        if handle & NOVELTY_HANDLE_FLAG != 0 {
            return Err(crate::error::SpatialError::Internal(format!(
                "novelty arena handle {handle} exceeds maximum (2^31-1)"
            )));
        }

        // Set NOVELTY_HANDLE_FLAG so lookups dispatch to novelty arena
        let flagged_handle = handle | NOVELTY_HANDLE_FLAG;

        // Compute S2 covering
        let cells = covering_for_geometry(&geom, s2_config)?;

        // Create CellEntry for each cell
        for cell_id in cells {
            entries.push(CellEntry::new(
                cell_id,
                op.s_id,
                flagged_handle,
                op.t,
                op.op,
            ));
        }
    }

    // Sort entries by index order
    entries.sort_by(super::cell_index::CellEntry::cmp_index);

    Ok(DerivedNovelty { entries, arena })
}

/// Derive spatial novelty from raw WKT strings (simpler API for testing).
///
/// Useful when you have direct WKT values rather than overlay operations.
pub fn derive_from_wkt_values(
    values: impl Iterator<Item = (u64, String, i64, u8)>, // (subject_id, wkt, t, op)
    s2_config: &S2CoveringConfig,
    index_points: bool,
) -> Result<DerivedNovelty> {
    let ops = values.map(|(s_id, wkt, t, op)| SpatialNoveltyOp {
        g_id: 0,
        p_id: 0,
        s_id,
        wkt,
        t,
        op,
    });

    derive_spatial_novelty(ops, 0, 0, s2_config, index_points)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_polygon_novelty() {
        let ops = vec![SpatialNoveltyOp {
            g_id: 0,
            p_id: 1,
            s_id: 100,
            wkt: "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))".to_string(),
            t: 1,
            op: 1, // assert
        }];

        let result = derive_spatial_novelty(
            ops.into_iter(),
            1, // target_p_id
            0, // target_g_id
            &S2CoveringConfig::default(),
            false, // don't index points
        )
        .unwrap();

        // Should have entries with NOVELTY_HANDLE_FLAG set
        assert!(!result.is_empty());
        for entry in &result.entries {
            assert!(entry.geo_handle & NOVELTY_HANDLE_FLAG != 0);
            assert_eq!(entry.subject_id, 100);
            assert_eq!(entry.t, 1);
            assert_eq!(entry.op, 1);
        }

        // Arena should have the geometry
        let actual_handle = result.entries[0].geo_handle & !NOVELTY_HANDLE_FLAG;
        assert!(result.arena.get(actual_handle).is_some());
    }

    #[test]
    fn test_skip_points_by_default() {
        let ops = vec![SpatialNoveltyOp {
            g_id: 0,
            p_id: 1,
            s_id: 100,
            wkt: "POINT(5 5)".to_string(),
            t: 1,
            op: 1,
        }];

        let result = derive_spatial_novelty(
            ops.into_iter(),
            1,
            0,
            &S2CoveringConfig::default(),
            false, // don't index points
        )
        .unwrap();

        // Should be empty since points are skipped
        assert!(result.is_empty());
    }

    #[test]
    fn test_filter_by_predicate() {
        let ops = vec![
            SpatialNoveltyOp {
                g_id: 0,
                p_id: 1, // matches
                s_id: 100,
                wkt: "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))".to_string(),
                t: 1,
                op: 1,
            },
            SpatialNoveltyOp {
                g_id: 0,
                p_id: 2, // doesn't match
                s_id: 200,
                wkt: "POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))".to_string(),
                t: 1,
                op: 1,
            },
        ];

        let result = derive_spatial_novelty(
            ops.into_iter(),
            1, // target_p_id = 1
            0,
            &S2CoveringConfig::default(),
            false,
        )
        .unwrap();

        // Should only have entries for subject 100
        for entry in &result.entries {
            assert_eq!(entry.subject_id, 100);
        }
    }

    #[test]
    fn test_retraction_handling() {
        let ops = vec![SpatialNoveltyOp {
            g_id: 0,
            p_id: 1,
            s_id: 100,
            wkt: "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))".to_string(),
            t: 2,
            op: 0, // retract
        }];

        let result =
            derive_spatial_novelty(ops.into_iter(), 1, 0, &S2CoveringConfig::default(), false)
                .unwrap();

        // Should have entries with op=0 for retraction
        assert!(!result.is_empty());
        for entry in &result.entries {
            assert_eq!(entry.op, 0); // retract
        }
    }
}
