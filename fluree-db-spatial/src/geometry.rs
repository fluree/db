//! Geometry storage and metadata computation.
//!
//! This module provides:
//! - WKT parsing (non-POINT geometries)
//! - Geometry arena for efficient storage
//! - Precomputed metadata (bbox, centroid, type) for fast filtering
//!
//! # Design
//!
//! The geometry arena stores WKT bytes as the source of truth, avoiding
//! normalization during ingestion. This keeps hashing/dedup simple (hash
//! the WKT string directly) and allows normalization to be added later.
//!
//! Precomputed metadata is stored alongside each geometry to enable fast
//! bbox/centroid filtering without reparsing WKT at query time.

use crate::config::MetadataConfig;
use crate::error::{Result, SpatialError};
use geo::{Area, BoundingRect, Centroid, Euclidean, Length};
use geo_types::Geometry;
use serde::{Deserialize, Serialize};

/// Geometry type discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum GeometryType {
    Point = 0,
    LineString = 1,
    Polygon = 2,
    MultiPoint = 3,
    MultiLineString = 4,
    MultiPolygon = 5,
    GeometryCollection = 6,
}

impl GeometryType {
    /// Classify a geo-types Geometry.
    pub fn from_geometry(geom: &Geometry<f64>) -> Self {
        match geom {
            Geometry::Point(_) => GeometryType::Point,
            Geometry::LineString(_) => GeometryType::LineString,
            Geometry::Polygon(_) => GeometryType::Polygon,
            Geometry::MultiPoint(_) => GeometryType::MultiPoint,
            Geometry::MultiLineString(_) => GeometryType::MultiLineString,
            Geometry::MultiPolygon(_) => GeometryType::MultiPolygon,
            Geometry::GeometryCollection(_) => GeometryType::GeometryCollection,
            _ => GeometryType::GeometryCollection, // Fallback for other types
        }
    }

    /// Check if this is a point type.
    pub fn is_point(&self) -> bool {
        matches!(self, GeometryType::Point | GeometryType::MultiPoint)
    }
}

/// Axis-aligned bounding box.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BBox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lng: f64,
    pub max_lng: f64,
}

impl BBox {
    /// Create a new bounding box.
    pub fn new(min_lat: f64, max_lat: f64, min_lng: f64, max_lng: f64) -> Self {
        Self {
            min_lat,
            max_lat,
            min_lng,
            max_lng,
        }
    }

    /// Check if this bbox intersects another.
    pub fn intersects(&self, other: &BBox) -> bool {
        self.min_lat <= other.max_lat
            && self.max_lat >= other.min_lat
            && self.min_lng <= other.max_lng
            && self.max_lng >= other.min_lng
    }

    /// Check if this bbox contains a point.
    pub fn contains_point(&self, lat: f64, lng: f64) -> bool {
        lat >= self.min_lat && lat <= self.max_lat && lng >= self.min_lng && lng <= self.max_lng
    }

    /// Check if this bbox fully contains another bbox.
    pub fn contains_bbox(&self, other: &BBox) -> bool {
        self.min_lat <= other.min_lat
            && self.max_lat >= other.max_lat
            && self.min_lng <= other.min_lng
            && self.max_lng >= other.max_lng
    }

    /// Compute from a geo-types Geometry.
    pub fn from_geometry(geom: &Geometry<f64>) -> Option<Self> {
        let rect = geom.bounding_rect()?;
        Some(Self {
            min_lat: rect.min().y,
            max_lat: rect.max().y,
            min_lng: rect.min().x,
            max_lng: rect.max().x,
        })
    }
}

/// Precomputed geometry metadata.
///
/// Stored alongside each geometry in the arena for fast filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeometryMetadata {
    /// Geometry type.
    pub geom_type: GeometryType,

    /// Bounding box (if computed).
    pub bbox: Option<BBox>,

    /// Centroid lat/lng (if computed).
    pub centroid: Option<(f64, f64)>,

    /// Area in square meters (for polygons, if computed).
    pub area: Option<f64>,

    /// Length in meters (for linestrings, if computed).
    pub length: Option<f64>,
}

impl GeometryMetadata {
    /// Compute metadata from a parsed geometry.
    pub fn compute(geom: &Geometry<f64>, config: &MetadataConfig) -> Self {
        let geom_type = GeometryType::from_geometry(geom);

        let bbox = if config.compute_bbox {
            BBox::from_geometry(geom)
        } else {
            None
        };

        let centroid = if config.compute_centroid {
            geom.centroid().map(|c| (c.y(), c.x()))
        } else {
            None
        };

        let area = if config.compute_area {
            // Note: geo crate computes unsigned area; for spherical, you'd use geodesic
            Some(geom.unsigned_area())
        } else {
            None
        };

        let length = if config.compute_length {
            // Note: This is Euclidean length; for spherical, you'd use geodesic
            // Only LineString and MultiLineString have meaningful length
            match geom {
                Geometry::LineString(ls) => Some(ls.length::<Euclidean>()),
                Geometry::MultiLineString(mls) => Some(mls.length::<Euclidean>()),
                _ => None,
            }
        } else {
            None
        };

        Self {
            geom_type,
            bbox,
            centroid,
            area,
            length,
        }
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        // Simple binary format: type (1 byte) + optional fields
        let mut buf = Vec::with_capacity(64);
        buf.push(self.geom_type as u8);

        // Flags for which optional fields are present
        let mut flags: u8 = 0;
        if self.bbox.is_some() {
            flags |= 0x01;
        }
        if self.centroid.is_some() {
            flags |= 0x02;
        }
        if self.area.is_some() {
            flags |= 0x04;
        }
        if self.length.is_some() {
            flags |= 0x08;
        }
        buf.push(flags);

        if let Some(bbox) = &self.bbox {
            buf.extend_from_slice(&bbox.min_lat.to_le_bytes());
            buf.extend_from_slice(&bbox.max_lat.to_le_bytes());
            buf.extend_from_slice(&bbox.min_lng.to_le_bytes());
            buf.extend_from_slice(&bbox.max_lng.to_le_bytes());
        }

        if let Some((lat, lng)) = self.centroid {
            buf.extend_from_slice(&lat.to_le_bytes());
            buf.extend_from_slice(&lng.to_le_bytes());
        }

        if let Some(area) = self.area {
            buf.extend_from_slice(&area.to_le_bytes());
        }

        if let Some(length) = self.length {
            buf.extend_from_slice(&length.to_le_bytes());
        }

        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 2 {
            return Err(SpatialError::FormatError("metadata too short".into()));
        }

        let geom_type = match data[0] {
            0 => GeometryType::Point,
            1 => GeometryType::LineString,
            2 => GeometryType::Polygon,
            3 => GeometryType::MultiPoint,
            4 => GeometryType::MultiLineString,
            5 => GeometryType::MultiPolygon,
            6 => GeometryType::GeometryCollection,
            _ => return Err(SpatialError::FormatError("invalid geometry type".into())),
        };

        let flags = data[1];
        let mut pos = 2;

        let bbox = if flags & 0x01 != 0 {
            if pos + 32 > data.len() {
                return Err(SpatialError::FormatError("truncated bbox".into()));
            }
            let min_lat = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let max_lat = f64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
            let min_lng = f64::from_le_bytes(data[pos + 16..pos + 24].try_into().unwrap());
            let max_lng = f64::from_le_bytes(data[pos + 24..pos + 32].try_into().unwrap());
            pos += 32;
            Some(BBox {
                min_lat,
                max_lat,
                min_lng,
                max_lng,
            })
        } else {
            None
        };

        let centroid = if flags & 0x02 != 0 {
            if pos + 16 > data.len() {
                return Err(SpatialError::FormatError("truncated centroid".into()));
            }
            let lat = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let lng = f64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
            pos += 16;
            Some((lat, lng))
        } else {
            None
        };

        let area = if flags & 0x04 != 0 {
            if pos + 8 > data.len() {
                return Err(SpatialError::FormatError("truncated area".into()));
            }
            let a = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            Some(a)
        } else {
            None
        };

        let length = if flags & 0x08 != 0 {
            if pos + 8 > data.len() {
                return Err(SpatialError::FormatError("truncated length".into()));
            }
            let l = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            Some(l)
        } else {
            None
        };

        Ok(Self {
            geom_type,
            bbox,
            centroid,
            area,
            length,
        })
    }
}

/// Entry in the geometry arena.
#[derive(Debug, Clone)]
pub struct ArenaEntry {
    /// Handle (index) into the arena.
    pub handle: u32,

    /// Original WKT bytes (source of truth).
    pub wkt: Vec<u8>,

    /// Precomputed metadata.
    pub metadata: GeometryMetadata,
}

/// Geometry arena: stores WKT + metadata for all indexed geometries.
///
/// The arena is append-only during build and immutable after snapshot.
/// WKT bytes are stored as the source of truth; parsing happens at build
/// time for metadata computation and at query time for exact predicates.
pub struct GeometryArena {
    /// All entries in handle order.
    entries: Vec<ArenaEntry>,

    /// WKT hash → handle for deduplication.
    wkt_index: rustc_hash::FxHashMap<u64, u32>,
}

impl GeometryArena {
    /// Create an empty arena.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            wkt_index: rustc_hash::FxHashMap::default(),
        }
    }

    /// Add a geometry to the arena, returning its handle.
    ///
    /// Deduplicates by WKT hash; if the same WKT already exists, returns
    /// the existing handle.
    pub fn add(&mut self, wkt: &str, config: &MetadataConfig) -> Result<u32> {
        // Hash the WKT for dedup
        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        wkt.hash(&mut hasher);
        let hash = hasher.finish();

        // Check for existing entry
        if let Some(&handle) = self.wkt_index.get(&hash) {
            // Verify it's actually the same WKT (hash collision check)
            if self.entries[handle as usize].wkt == wkt.as_bytes() {
                return Ok(handle);
            }
        }

        // Parse WKT to compute metadata
        let geom = parse_wkt(wkt)?;
        let metadata = GeometryMetadata::compute(&geom, config);

        let handle = self.entries.len() as u32;
        self.entries.push(ArenaEntry {
            handle,
            wkt: wkt.as_bytes().to_vec(),
            metadata,
        });
        self.wkt_index.insert(hash, handle);

        Ok(handle)
    }

    /// Get an entry by handle.
    pub fn get(&self, handle: u32) -> Option<&ArenaEntry> {
        self.entries.get(handle as usize)
    }

    /// Number of entries in the arena.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the arena is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = &ArenaEntry> {
        self.entries.iter()
    }

    /// Parse stored WKT to geo-types Geometry.
    ///
    /// This is used for exact predicate testing at query time.
    pub fn parse_geometry(&self, handle: u32) -> Result<Geometry<f64>> {
        let entry = self.get(handle).ok_or_else(|| {
            SpatialError::InvalidGeometry(format!("no geometry at handle {handle}"))
        })?;
        let wkt_str = std::str::from_utf8(&entry.wkt)
            .map_err(|e| SpatialError::WktParse(format!("invalid UTF-8: {e}")))?;
        parse_wkt(wkt_str)
    }
}

impl Default for GeometryArena {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Arena Serialization
// ============================================================================

/// Magic bytes for arena files.
pub const ARENA_MAGIC: &[u8; 4] = b"FSA1";

/// Current arena format version.
pub const ARENA_VERSION: u8 = 1;

impl GeometryArena {
    /// Serialize the arena to bytes.
    ///
    /// Format (zstd compressed):
    /// ```text
    /// Header (8 bytes):
    ///   magic: "FSA1" (4B)
    ///   version: u8
    ///   flags: u8
    ///   _reserved: u16
    ///
    /// Body:
    ///   entry_count: u32 (LE)
    ///   entries: [
    ///     wkt_len: u32 (LE)
    ///     wkt_bytes: [u8; wkt_len]
    ///     metadata_len: u16 (LE)
    ///     metadata_bytes: [u8; metadata_len]
    ///   ]
    /// ```
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        // Build uncompressed body
        let mut body = Vec::new();
        body.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        for entry in &self.entries {
            // WKT
            body.extend_from_slice(&(entry.wkt.len() as u32).to_le_bytes());
            body.extend_from_slice(&entry.wkt);

            // Metadata
            let meta_bytes = entry.metadata.to_bytes();
            body.extend_from_slice(&(meta_bytes.len() as u16).to_le_bytes());
            body.extend_from_slice(&meta_bytes);
        }

        // Compress body
        let compressed = zstd::encode_all(&body[..], 3)
            .map_err(|e| SpatialError::Io(std::io::Error::other(e)))?;

        // Build final buffer with header
        let mut buf = Vec::with_capacity(8 + compressed.len());
        buf.extend_from_slice(ARENA_MAGIC);
        buf.push(ARENA_VERSION);
        buf.push(0); // flags
        buf.extend_from_slice(&[0u8; 2]); // reserved
        buf.extend_from_slice(&compressed);

        Ok(buf)
    }

    /// Deserialize an arena from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(SpatialError::FormatError("arena too short".into()));
        }

        if &data[0..4] != ARENA_MAGIC {
            return Err(SpatialError::FormatError("invalid arena magic".into()));
        }

        let version = data[4];
        if version != ARENA_VERSION {
            return Err(SpatialError::FormatError(format!(
                "unsupported arena version: {version}"
            )));
        }

        // Decompress body
        let decompressed =
            zstd::decode_all(&data[8..]).map_err(|e| SpatialError::Io(std::io::Error::other(e)))?;

        if decompressed.len() < 4 {
            return Err(SpatialError::FormatError("truncated arena body".into()));
        }

        let entry_count = u32::from_le_bytes(decompressed[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;

        let mut entries = Vec::with_capacity(entry_count);
        let mut wkt_index = rustc_hash::FxHashMap::default();

        for handle in 0..entry_count {
            // Read WKT
            if pos + 4 > decompressed.len() {
                return Err(SpatialError::FormatError("truncated wkt length".into()));
            }
            let wkt_len =
                u32::from_le_bytes(decompressed[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + wkt_len > decompressed.len() {
                return Err(SpatialError::FormatError("truncated wkt bytes".into()));
            }
            let wkt = decompressed[pos..pos + wkt_len].to_vec();
            pos += wkt_len;

            // Read metadata
            if pos + 2 > decompressed.len() {
                return Err(SpatialError::FormatError(
                    "truncated metadata length".into(),
                ));
            }
            let meta_len =
                u16::from_le_bytes(decompressed[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            if pos + meta_len > decompressed.len() {
                return Err(SpatialError::FormatError("truncated metadata bytes".into()));
            }
            let metadata = GeometryMetadata::from_bytes(&decompressed[pos..pos + meta_len])?;
            pos += meta_len;

            // Build hash for dedup index
            // Note: Same semantics as add() - stores last handle on hash collision.
            // Hash collisions are extremely rare with 64-bit FxHash.
            use std::hash::{Hash, Hasher};
            let mut hasher = rustc_hash::FxHasher::default();
            wkt.hash(&mut hasher);
            let hash = hasher.finish();
            wkt_index.insert(hash, handle as u32);

            entries.push(ArenaEntry {
                handle: handle as u32,
                wkt,
                metadata,
            });
        }

        Ok(Self { entries, wkt_index })
    }
}

/// Parse WKT string to geo-types Geometry.
pub fn parse_wkt(wkt: &str) -> Result<Geometry<f64>> {
    use std::str::FromStr;
    wkt::Wkt::from_str(wkt)
        .map_err(|e| SpatialError::WktParse(format!("{e:?}")))
        .and_then(|w| {
            w.try_into()
                .map_err(|e: wkt::conversion::Error| SpatialError::WktParse(format!("{e:?}")))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_polygon() {
        let wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
        let geom = parse_wkt(wkt).unwrap();
        assert!(matches!(geom, Geometry::Polygon(_)));
    }

    #[test]
    fn test_bbox_computation() {
        let wkt = "POLYGON((0 0, 10 0, 10 20, 0 20, 0 0))";
        let geom = parse_wkt(wkt).unwrap();
        let bbox = BBox::from_geometry(&geom).unwrap();
        assert_eq!(bbox.min_lng, 0.0);
        assert_eq!(bbox.max_lng, 10.0);
        assert_eq!(bbox.min_lat, 0.0);
        assert_eq!(bbox.max_lat, 20.0);
    }

    #[test]
    fn test_metadata_roundtrip() {
        let wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
        let geom = parse_wkt(wkt).unwrap();
        let config = MetadataConfig::default();
        let meta = GeometryMetadata::compute(&geom, &config);

        let bytes = meta.to_bytes();
        let recovered = GeometryMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(recovered.geom_type, meta.geom_type);
        assert!(recovered.bbox.is_some());
        assert!(recovered.centroid.is_some());
    }

    #[test]
    fn test_arena_dedup() {
        let mut arena = GeometryArena::new();
        let config = MetadataConfig::default();

        let wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
        let h1 = arena.add(wkt, &config).unwrap();
        let h2 = arena.add(wkt, &config).unwrap();

        assert_eq!(h1, h2);
        assert_eq!(arena.len(), 1);
    }
}
