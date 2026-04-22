//! Geographic utilities for GeoSPARQL POINT handling.
//!
//! This module provides:
//! - WKT POINT extraction for inline storage as GeoPoint
//! - Haversine distance calculation
//! - Proximity bounding for latitude-band scans

/// Detect POINT geometry and extract coordinates without full WKT parsing.
///
/// Returns `Some((lat, lng))` for valid 2D POINT literals, `None` otherwise.
///
/// # Parsing Decisions
///
/// - Case-sensitive: only "POINT" (most common in real data)
/// - No SRID prefix support (v1)
/// - Rejects POINT EMPTY, POINT Z/M/ZM (3D/4D variants)
///
/// # Coordinate Order
///
/// WKT uses (lng, lat) order. This function returns (lat, lng) to match
/// the encoding order in [`GeoPointBits`](crate::value::GeoPointBits).
///
/// # Examples
///
/// ```
/// use fluree_db_core::geo::try_extract_point;
///
/// // Valid POINT
/// assert_eq!(try_extract_point("POINT(2.3522 48.8566)"), Some((48.8566, 2.3522)));
///
/// // Not a point
/// assert_eq!(try_extract_point("LINESTRING(0 0, 1 1)"), None);
///
/// // Out of range latitude
/// assert_eq!(try_extract_point("POINT(0 91)"), None);
/// ```
pub fn try_extract_point(wkt: &str) -> Option<(f64, f64)> {
    let wkt = wkt.trim();

    // Must start with "POINT" (case-sensitive)
    if !wkt.starts_with("POINT") {
        return None;
    }

    // Reject POINT EMPTY
    if wkt.contains("EMPTY") {
        return None;
    }

    // Find the parentheses
    let start = wkt.find('(')?;
    let end = wkt.rfind(')')?;
    if start >= end {
        return None;
    }

    // Extract and parse coordinates
    let coords = wkt[start + 1..end].trim();
    let mut parts = coords.split_whitespace();

    // WKT order is (lng, lat) - swap for our (lat, lng) convention
    let lng: f64 = parts.next()?.parse().ok()?;
    let lat: f64 = parts.next()?.parse().ok()?;

    // Reject 3D/4D (Z, M, ZM)
    if parts.next().is_some() {
        return None;
    }

    // Validate coordinate ranges
    if !lat.is_finite() || !lng.is_finite() {
        return None;
    }
    if !(-90.0..=90.0).contains(&lat) {
        return None;
    }
    if !(-180.0..=180.0).contains(&lng) {
        return None;
    }

    Some((lat, lng))
}

/// Haversine distance between two points in meters.
///
/// Uses spherical Earth approximation (mean radius 6,371,000m).
/// Accuracy is ~0.3% for most practical distances.
///
/// # Arguments
///
/// * `lat1`, `lng1` - First point coordinates in degrees
/// * `lat2`, `lng2` - Second point coordinates in degrees
///
/// # Returns
///
/// Distance in meters.
///
/// # Example
///
/// ```
/// use fluree_db_core::geo::haversine_distance;
///
/// // Paris to London: ~343 km
/// let d = haversine_distance(48.8566, 2.3522, 51.5074, -0.1278);
/// assert!((d - 343_000.0).abs() < 10_000.0);
/// ```
pub fn haversine_distance(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;

    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();

    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlng / 2.0).sin().powi(2);

    EARTH_RADIUS_M * 2.0 * a.sqrt().asin()
}

/// Compute POST scan bounds for proximity query.
///
/// Returns 1-2 ranges of `(min_o_key, max_o_key)` pairs for the latitude-band scan.
/// Two ranges are returned when the search area crosses the antimeridian (+/-180°).
///
/// # Important
///
/// The concatenated 30/30 encoding produces a **latitude-band scan**: all points
/// in `[lat_min, lat_max]` are returned regardless of longitude. The haversine
/// post-filter is **REQUIRED** for correctness.
///
/// # Arguments
///
/// * `center_lat`, `center_lng` - Center point coordinates in degrees
/// * `radius_m` - Search radius in meters
///
/// # Returns
///
/// Vector of `(min_key, max_key)` ranges for POST scan.
pub fn geo_proximity_bounds(center_lat: f64, center_lng: f64, radius_m: f64) -> Vec<(u64, u64)> {
    use crate::value_id::ObjKey;

    // Approximate meters per degree at equator
    const M_PER_DEG: f64 = 111_320.0;

    // Calculate lat/lng delta from radius
    let lat_delta = radius_m / M_PER_DEG;
    let cos_lat = center_lat.to_radians().cos().abs().max(0.001);
    let lng_delta = radius_m / (M_PER_DEG * cos_lat);

    // Compute bounding box
    let lat_min = (center_lat - lat_delta).max(-90.0);
    let lat_max = (center_lat + lat_delta).min(90.0);
    let lng_min = center_lng - lng_delta;
    let lng_max = center_lng + lng_delta;

    // Check for antimeridian crossing
    if lng_min < -180.0 || lng_max > 180.0 {
        // Two ranges needed
        let mut ranges = Vec::with_capacity(2);

        // Range 1: from lng_min (wrapped) to 180
        if lng_min < -180.0 {
            let wrapped = lng_min + 360.0;
            if let (Ok(min_key), Ok(max_key)) = (
                ObjKey::encode_geo_point(lat_min, wrapped),
                ObjKey::encode_geo_point(lat_max, 180.0),
            ) {
                ranges.push((min_key.as_u64(), max_key.as_u64()));
            }
        }

        // Range 2: from -180 to lng_max (wrapped)
        if lng_max > 180.0 {
            let wrapped = lng_max - 360.0;
            if let (Ok(min_key), Ok(max_key)) = (
                ObjKey::encode_geo_point(lat_min, -180.0),
                ObjKey::encode_geo_point(lat_max, wrapped),
            ) {
                ranges.push((min_key.as_u64(), max_key.as_u64()));
            }
        }

        // Also include the non-wrapped portion
        let clamped_min = lng_min.max(-180.0);
        let clamped_max = lng_max.min(180.0);
        if clamped_min < clamped_max {
            if let (Ok(min_key), Ok(max_key)) = (
                ObjKey::encode_geo_point(lat_min, clamped_min),
                ObjKey::encode_geo_point(lat_max, clamped_max),
            ) {
                ranges.push((min_key.as_u64(), max_key.as_u64()));
            }
        }

        ranges
    } else {
        // Single range
        if let (Ok(min_key), Ok(max_key)) = (
            ObjKey::encode_geo_point(lat_min, lng_min),
            ObjKey::encode_geo_point(lat_max, lng_max),
        ) {
            vec![(min_key.as_u64(), max_key.as_u64())]
        } else {
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_extract_point_valid() {
        // Standard WKT POINT
        assert_eq!(
            try_extract_point("POINT(2.3522 48.8566)"),
            Some((48.8566, 2.3522))
        );

        // With extra whitespace
        assert_eq!(
            try_extract_point("  POINT( 2.3522  48.8566 )  "),
            Some((48.8566, 2.3522))
        );

        // Negative coordinates
        assert_eq!(
            try_extract_point("POINT(-0.1278 51.5074)"),
            Some((51.5074, -0.1278))
        );

        // Boundary values
        assert_eq!(try_extract_point("POINT(180 90)"), Some((90.0, 180.0)));
        assert_eq!(try_extract_point("POINT(-180 -90)"), Some((-90.0, -180.0)));
    }

    #[test]
    fn test_try_extract_point_invalid() {
        // Not a POINT
        assert_eq!(try_extract_point("LINESTRING(0 0, 1 1)"), None);
        assert_eq!(try_extract_point("POLYGON((0 0, 1 0, 1 1, 0 0))"), None);

        // Case sensitive
        assert_eq!(try_extract_point("point(0 0)"), None);
        assert_eq!(try_extract_point("Point(0 0)"), None);

        // POINT EMPTY
        assert_eq!(try_extract_point("POINT EMPTY"), None);

        // 3D/4D
        assert_eq!(try_extract_point("POINT(0 0 0)"), None);
        assert_eq!(try_extract_point("POINT(0 0 0 0)"), None);

        // Out of range
        assert_eq!(try_extract_point("POINT(0 91)"), None);
        assert_eq!(try_extract_point("POINT(0 -91)"), None);
        assert_eq!(try_extract_point("POINT(181 0)"), None);
        assert_eq!(try_extract_point("POINT(-181 0)"), None);

        // Invalid format
        assert_eq!(try_extract_point("POINT()"), None);
        assert_eq!(try_extract_point("POINT(0)"), None);
        assert_eq!(try_extract_point("POINT(abc def)"), None);
    }

    #[test]
    fn test_haversine_distance() {
        // Same point
        assert!((haversine_distance(0.0, 0.0, 0.0, 0.0)).abs() < 0.001);

        // Paris to London (~343 km)
        let d = haversine_distance(48.8566, 2.3522, 51.5074, -0.1278);
        assert!((d - 343_500.0).abs() < 5_000.0);

        // Equator, 1 degree of longitude (~111 km)
        let d = haversine_distance(0.0, 0.0, 0.0, 1.0);
        assert!((d - 111_195.0).abs() < 500.0);

        // Poles (antipodal, ~20,000 km)
        let d = haversine_distance(90.0, 0.0, -90.0, 0.0);
        assert!((d - 20_015_086.0).abs() < 1000.0);
    }

    #[test]
    fn test_geo_proximity_bounds_simple() {
        // Non-crossing case
        let bounds = geo_proximity_bounds(48.0, 2.0, 100_000.0);
        assert_eq!(bounds.len(), 1);

        let (min_key, max_key) = bounds[0];
        assert!(min_key < max_key);
    }

    #[test]
    fn test_geo_proximity_bounds_antimeridian() {
        // Crossing antimeridian near Japan
        let bounds = geo_proximity_bounds(35.0, 179.0, 500_000.0);
        // Should produce 2 ranges
        assert!(!bounds.is_empty());
    }
}
