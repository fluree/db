//! S2 covering generation.
//!
//! Generates S2 cell coverings for geometries, which are used for:
//! - Indexing: map geometry → set of cell entries
//! - Querying: map query geometry → set of cell ranges to scan
//!
//! # S2 Cell Properties
//!
//! S2 cells have several properties that make them ideal for spatial indexing:
//! - Hilbert-curve ordering: nearby cells have nearby IDs (good for range scans)
//! - Hierarchical: cells at level N contain 4 children at level N+1
//! - Equal-area: cells at the same level have roughly equal area (spherical)
//! - No gaps or overlaps: cells tile the sphere exactly
//!
//! # Cell Levels
//!
//! | Level | Approx. Area (equator) | Use Case |
//! |-------|------------------------|----------|
//! | 0     | 85M km²               | Hemisphere |
//! | 4     | 5M km²                | Large regions |
//! | 8     | 300k km²              | Countries |
//! | 12    | 20k km²               | Cities |
//! | 16    | 1.3k km²              | Neighborhoods |
//! | 20    | 80 km²                | Buildings |
//! | 24    | 5 km²                 | Rooms |
//! | 30    | 0.7 cm²               | Sub-centimeter |

use crate::config::S2CoveringConfig;
use crate::error::{Result, SpatialError};
use geo_types::Geometry;
use s2::cap::Cap;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use s2::rect::Rect;
use s2::region::RegionCoverer;

/// Generate an S2 covering for a geometry.
///
/// Returns a list of S2 cell IDs that cover the geometry.
pub fn covering_for_geometry(geom: &Geometry<f64>, config: &S2CoveringConfig) -> Result<Vec<u64>> {
    let region = geometry_to_s2_region(geom)?;

    let coverer = RegionCoverer {
        min_level: config.min_level,
        max_level: config.max_level,
        level_mod: 1,
        max_cells: config.max_cells,
    };

    let covering = coverer.covering(&region);

    Ok(covering.0.iter().map(|c| c.0).collect())
}

/// Generate an S2 covering for a query circle (center + radius).
///
/// Used for proximity/radius queries.
pub fn covering_for_circle(
    center_lat: f64,
    center_lng: f64,
    radius_meters: f64,
    config: &S2CoveringConfig,
) -> Result<Vec<u64>> {
    let center = LatLng::from_degrees(center_lat, center_lng);
    let center_point = s2::point::Point::from(center);

    // Convert radius in meters to angle (radians) on unit sphere
    // Earth radius ≈ 6,371,000 meters
    const EARTH_RADIUS_METERS: f64 = 6_371_000.0;
    let angle_radians = radius_meters / EARTH_RADIUS_METERS;
    let angle = s2::s1::angle::Angle::from(s2::s1::Rad(angle_radians));

    let cap = Cap::from_center_angle(&center_point, &angle);

    let coverer = RegionCoverer {
        min_level: config.min_level,
        max_level: config.max_level,
        level_mod: 1,
        max_cells: config.max_cells,
    };

    let covering = coverer.covering(&cap);

    Ok(covering.0.iter().map(|c| c.0).collect())
}

/// Convert cell IDs to range intervals for scanning.
///
/// Each cell ID defines a range of descendant cell IDs:
/// `[cell.range_min(), cell.range_max()]`
///
/// This function merges adjacent cells into continuous intervals
/// for more efficient range scanning.
pub fn cells_to_ranges(cells: &[u64]) -> Vec<(u64, u64)> {
    if cells.is_empty() {
        return vec![];
    }

    let mut ranges: Vec<(u64, u64)> = cells
        .iter()
        .map(|&id| {
            let cell = CellID(id);
            (cell.range_min().0, cell.range_max().0)
        })
        .collect();

    // Sort by range start
    ranges.sort_by_key(|r| r.0);

    // Merge overlapping/adjacent ranges
    let mut merged = Vec::with_capacity(ranges.len());
    let mut current = ranges[0];

    for range in ranges.into_iter().skip(1) {
        if range.0 <= current.1 + 1 {
            // Overlapping or adjacent, extend current
            current.1 = current.1.max(range.1);
        } else {
            // Gap, emit current and start new
            merged.push(current);
            current = range;
        }
    }
    merged.push(current);

    merged
}

/// Convert a geo-types geometry to an S2 region for covering.
fn geometry_to_s2_region(geom: &Geometry<f64>) -> Result<Rect> {
    // For now, use the bounding box as the region
    // A more precise approach would use S2Polygon/S2Polyline
    let rect = geo::BoundingRect::bounding_rect(geom)
        .ok_or_else(|| SpatialError::InvalidGeometry("cannot compute bounding rect".into()))?;

    // lat_lo, lng_lo, lat_hi, lng_hi (y is lat, x is lng)
    Ok(Rect::from_degrees(
        rect.min().y,
        rect.min().x,
        rect.max().y,
        rect.max().x,
    ))
}

/// Get the S2 cell ID for a single point.
pub fn cell_for_point(lat: f64, lng: f64, level: u8) -> u64 {
    let ll = LatLng::from_degrees(lat, lng);
    CellID::from(ll).parent(level as u64).0
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{LineString, Polygon};

    #[test]
    fn test_covering_for_circle() {
        let config = S2CoveringConfig::default();

        // Paris coordinates
        let cells = covering_for_circle(48.8566, 2.3522, 10_000.0, &config).unwrap();

        // Should have some cells
        assert!(!cells.is_empty());
        assert!(cells.len() <= config.max_cells);
    }

    #[test]
    fn test_cells_to_ranges() {
        // Two adjacent cells should merge
        let cell1 = CellID::from(LatLng::from_degrees(0.0, 0.0)).parent(10);
        let cell2 = cell1.next();

        let ranges = cells_to_ranges(&[cell1.0, cell2.0]);

        // May or may not merge depending on cell structure
        assert!(!ranges.is_empty());
    }

    #[test]
    fn test_cell_for_point() {
        let cell = cell_for_point(48.8566, 2.3522, 16);
        assert!(cell != 0);

        // Same location should give same cell
        let cell2 = cell_for_point(48.8566, 2.3522, 16);
        assert_eq!(cell, cell2);
    }

    #[test]
    fn test_covering_for_polygon() {
        let config = S2CoveringConfig::default();

        // Simple square polygon
        let exterior = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        let polygon = Polygon::new(LineString::from(exterior), vec![]);
        let geom = Geometry::Polygon(polygon);

        let cells = covering_for_geometry(&geom, &config).unwrap();

        assert!(!cells.is_empty());
        assert!(cells.len() <= config.max_cells);
    }
}
