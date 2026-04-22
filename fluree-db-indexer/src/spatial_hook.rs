//! Spatial index collection hook for commit resolution.
//!
//! Collects WKT geometry strings during commit resolution to build a spatial
//! index alongside the binary index.
//!
//! # Usage
//!
//! ```ignore
//! let mut hook = SpatialHook::new();
//!
//! // During resolution:
//! hook.on_op(&raw_op, g_id, subject_id, p_id, t);
//!
//! // After all commits resolved:
//! let entries = hook.into_entries();
//! ```

use fluree_db_core::commit::codec::raw_reader::{RawObject, RawOp};
use fluree_db_core::GraphId;
use fluree_vocab::{geo_names, namespaces};

/// Entry collected for spatial indexing.
#[derive(Debug, Clone)]
pub struct SpatialEntry {
    /// Graph ID (0 = default graph).
    pub g_id: GraphId,
    /// Subject ID (sid64) that owns this geometry.
    pub subject_id: u64,
    /// Predicate ID in binary index space.
    pub p_id: u32,
    /// WKT geometry string.
    pub wkt: String,
    /// Transaction time.
    pub t: i64,
    /// true = assertion, false = retraction.
    pub is_assert: bool,
}

/// Hook for collecting WKT geometries during commit resolution.
///
/// Only collects non-POINT geometries (POINT is handled inline via GeoPoint).
/// Filters by predicate ID if specified, otherwise collects all geo:wktLiteral values.
#[derive(Debug)]
pub struct SpatialHook {
    /// Collected spatial entries.
    entries: Vec<SpatialEntry>,
    /// Optional predicate ID filter. If set, only geometries with this predicate are collected.
    /// If None, all geo:wktLiteral values are collected.
    predicate_filter: Option<u32>,
}

impl SpatialHook {
    /// Create a new spatial hook that collects all geo:wktLiteral values.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            predicate_filter: None,
        }
    }

    /// Create a new spatial hook that only collects geometries with the given predicate ID.
    pub fn with_predicate_filter(p_id: u32) -> Self {
        Self {
            entries: Vec::new(),
            predicate_filter: Some(p_id),
        }
    }

    /// Process a single raw op during commit resolution.
    ///
    /// Collects the WKT string if:
    /// 1. Datatype is geo:wktLiteral (both namespace code AND local name)
    /// 2. Object is a string (non-POINT WKT)
    /// 3. Predicate matches filter (if set)
    ///
    /// # Arguments
    /// * `raw_op` - The raw operation from commit resolution
    /// * `g_id` - Graph ID (0 = default graph)
    /// * `subject_id` - Resolved subject ID (sid64)
    /// * `p_id` - Resolved predicate ID in binary index space
    /// * `t` - Transaction time
    pub fn on_op(&mut self, raw_op: &RawOp<'_>, g_id: GraphId, subject_id: u64, p_id: u32, t: i64) {
        // Check if datatype is geo:wktLiteral - must match BOTH namespace code AND local name
        // to avoid false positives from user-defined datatypes with same local name
        if raw_op.dt_ns_code != namespaces::OGC_GEO || raw_op.dt_name != geo_names::WKT_LITERAL {
            return;
        }

        // Only collect string objects (non-POINT WKT)
        // GeoPoint is handled inline by the binary index
        let wkt = match &raw_op.o {
            RawObject::Str(s) => *s,
            _ => return, // GeoPoint or other type - skip
        };

        // Skip POINT geometries (handled inline) - case-insensitive without allocation
        let trimmed = wkt.trim();
        if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("POINT") {
            return;
        }

        // Check predicate filter
        if let Some(filter_p_id) = self.predicate_filter {
            if p_id != filter_p_id {
                return;
            }
        }

        self.entries.push(SpatialEntry {
            g_id,
            subject_id,
            p_id,
            wkt: wkt.to_string(),
            t,
            is_assert: raw_op.op,
        });
    }

    /// Get the number of collected entries.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Check if any entries were collected.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Consume the hook and return collected entries.
    pub fn into_entries(self) -> Vec<SpatialEntry> {
        self.entries
    }

    /// Get a reference to collected entries.
    pub fn entries(&self) -> &[SpatialEntry] {
        &self.entries
    }
}

impl Default for SpatialHook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spatial_hook_collects_polygon() {
        let hook = SpatialHook::new();

        // Simulate a POLYGON WKT with geo:wktLiteral datatype
        // We can't easily construct RawOp in tests, so this test is a placeholder
        // The actual testing happens in integration tests

        assert!(hook.is_empty());
    }

    #[test]
    fn test_spatial_hook_skips_point() {
        // POINT geometries should be skipped (handled inline)
        // This test is a placeholder - actual testing in integration tests
    }

    #[test]
    fn test_predicate_filter() {
        let hook = SpatialHook::with_predicate_filter(42);
        assert!(hook.is_empty());
        // Filter functionality tested in integration tests
    }
}
