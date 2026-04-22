//! Vector Index Serialization
//!
//! Provides snapshot serialization and deserialization for the VectorIndex
//! using a custom binary format with FVEC magic bytes.
//!
//! # Snapshot Format
//!
//! ```text
//! Magic bytes: "FVEC" (4 bytes)
//! Format version: 1 (1 byte)
//!
//! Metadata length: 4 bytes (little-endian u32)
//! Metadata JSON: { format_version, index_kind, dimensions, metric, ... }
//!
//! IRI mapping length: 4 bytes (little-endian u32)
//! IRI mapping: postcard-serialized PointIdAssigner
//!
//! Watermark length: 4 bytes (little-endian u32)
//! Watermark: postcard-serialized GraphSourceWatermark
//!
//! Property deps length: 4 bytes (little-endian u32)
//! Property deps: postcard-serialized VectorPropertyDeps
//!
//! usearch index length: 4 bytes (little-endian u32)
//! usearch index binary: from Index::save_to_buffer()
//! ```

use std::io::{Read, Write};

use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use crate::bm25::index::GraphSourceWatermark;

use super::super::DistanceMetric;
use super::error::{Result, VectorError};
use super::index::{PointIdAssigner, VectorIndex, VectorIndexMetadata, VectorPropertyDeps};

/// Magic bytes for vector index snapshot files.
const SNAPSHOT_MAGIC: &[u8; 4] = b"FVEC";

/// Current snapshot format version.
const SNAPSHOT_VERSION: u8 = 1;

/// Serialize a VectorIndex to bytes.
pub fn serialize(index: &VectorIndex) -> Result<Vec<u8>> {
    let mut data = Vec::new();

    // Write header
    data.extend_from_slice(SNAPSHOT_MAGIC);
    data.push(SNAPSHOT_VERSION);

    // Serialize metadata as JSON
    let metadata_json = serde_json::to_vec(&index.metadata)
        .map_err(|e| VectorError::SerializeError(format!("metadata JSON: {e}")))?;
    write_length_prefixed(&mut data, &metadata_json);

    // Serialize IRI mapping with postcard
    let iri_bytes = postcard::to_allocvec(index.id_assigner())?;
    write_length_prefixed(&mut data, &iri_bytes);

    // Serialize watermark with postcard
    let watermark_bytes = postcard::to_allocvec(&index.watermark)?;
    write_length_prefixed(&mut data, &watermark_bytes);

    // Serialize property deps with postcard
    let deps_bytes = postcard::to_allocvec(&index.property_deps)?;
    write_length_prefixed(&mut data, &deps_bytes);

    // Serialize usearch index
    // First get the serialized length to allocate the buffer
    let usearch_size = index.serialized_length();
    let mut usearch_bytes = vec![0u8; usearch_size];
    index
        .inner()
        .save_to_buffer(&mut usearch_bytes)
        .map_err(|e| VectorError::Usearch(format!("save_to_buffer: {e}")))?;
    write_length_prefixed(&mut data, &usearch_bytes);

    Ok(data)
}

/// Write a length-prefixed byte slice (little-endian u32 length).
fn write_length_prefixed(data: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len() as u32;
    data.extend_from_slice(&len.to_le_bytes());
    data.extend_from_slice(bytes);
}

/// Deserialize a VectorIndex from bytes.
pub fn deserialize(data: &[u8]) -> Result<VectorIndex> {
    let mut cursor = 0;

    // Check minimum header size
    if data.len() < 5 {
        return Err(VectorError::InvalidFormat(
            "Data too short for header".to_string(),
        ));
    }

    // Check magic bytes
    if &data[0..4] != SNAPSHOT_MAGIC {
        return Err(VectorError::InvalidFormat(
            "Invalid magic bytes (expected FVEC)".to_string(),
        ));
    }
    cursor += 4;

    // Check version
    let version = data[cursor];
    if version > SNAPSHOT_VERSION {
        return Err(VectorError::UnsupportedVersion {
            version,
            max_supported: SNAPSHOT_VERSION,
        });
    }
    cursor += 1;

    // Read metadata JSON
    let (metadata_bytes, new_cursor) = read_length_prefixed(data, cursor)?;
    cursor = new_cursor;
    let metadata: VectorIndexMetadata = serde_json::from_slice(metadata_bytes)
        .map_err(|e| VectorError::SerializeError(format!("metadata JSON parse: {e}")))?;

    // Validate metadata
    check_compatibility(&metadata)?;

    // Read IRI mapping
    let (iri_bytes, new_cursor) = read_length_prefixed(data, cursor)?;
    cursor = new_cursor;
    let id_assigner: PointIdAssigner = postcard::from_bytes(iri_bytes)?;

    // Read watermark
    let (watermark_bytes, new_cursor) = read_length_prefixed(data, cursor)?;
    cursor = new_cursor;
    let watermark: GraphSourceWatermark = postcard::from_bytes(watermark_bytes)?;

    // Read property deps
    let (deps_bytes, new_cursor) = read_length_prefixed(data, cursor)?;
    cursor = new_cursor;
    let property_deps: VectorPropertyDeps = postcard::from_bytes(deps_bytes)?;

    // Read usearch index
    let (usearch_bytes, _new_cursor) = read_length_prefixed(data, cursor)?;

    // Recreate usearch index with matching options
    let usearch_metric = match metadata.metric {
        DistanceMetric::Cosine => MetricKind::Cos,
        DistanceMetric::Dot => MetricKind::IP,
        DistanceMetric::Euclidean => MetricKind::L2sq,
    };

    let quantization = match metadata.quantization.as_str() {
        "f32" => ScalarKind::F32,
        "f16" => ScalarKind::F16,
        "i8" => ScalarKind::I8,
        _ => ScalarKind::F32,
    };

    let index_options = IndexOptions {
        dimensions: metadata.dimensions,
        metric: usearch_metric,
        quantization,
        connectivity: 16, // Default, will be overwritten by loaded index
        expansion_add: 128,
        expansion_search: 64,
        multi: false,
    };

    let inner = Index::new(&index_options)
        .map_err(|e| VectorError::Usearch(format!("create index: {e}")))?;

    // Load the index data from buffer
    inner
        .load_from_buffer(usearch_bytes)
        .map_err(|e| VectorError::Usearch(format!("load_from_buffer: {e}")))?;

    Ok(VectorIndex::from_parts(
        inner,
        id_assigner,
        metadata,
        watermark,
        property_deps,
    ))
}

/// Read a length-prefixed byte slice. Returns (slice, new_cursor_position).
fn read_length_prefixed(data: &[u8], cursor: usize) -> Result<(&[u8], usize)> {
    if cursor + 4 > data.len() {
        return Err(VectorError::InvalidFormat(
            "Data truncated (length prefix)".to_string(),
        ));
    }

    let len_bytes: [u8; 4] = data[cursor..cursor + 4]
        .try_into()
        .expect("slice length checked");
    let len = u32::from_le_bytes(len_bytes) as usize;

    let start = cursor + 4;
    let end = start + len;

    if end > data.len() {
        return Err(VectorError::InvalidFormat(format!(
            "Data truncated (expected {} bytes at offset {}, have {})",
            len,
            start,
            data.len() - start
        )));
    }

    Ok((&data[start..end], end))
}

/// Check metadata compatibility on load.
fn check_compatibility(metadata: &VectorIndexMetadata) -> Result<()> {
    if metadata.format_version > SNAPSHOT_VERSION {
        return Err(VectorError::UnsupportedVersion {
            version: metadata.format_version,
            max_supported: SNAPSHOT_VERSION,
        });
    }

    // Check index kind
    if metadata.index_kind != "usearch_hnsw" {
        return Err(VectorError::InvalidFormat(format!(
            "Unsupported index kind: {} (expected usearch_hnsw)",
            metadata.index_kind
        )));
    }

    // Check encoding
    if metadata.encoding != "postcard" {
        return Err(VectorError::InvalidFormat(format!(
            "Unsupported encoding: {} (expected postcard)",
            metadata.encoding
        )));
    }

    Ok(())
}

/// Write a VectorIndex snapshot to a writer.
pub fn write_snapshot<W: Write>(index: &VectorIndex, mut writer: W) -> Result<()> {
    let data = serialize(index)?;
    writer.write_all(&data)?;
    Ok(())
}

/// Read a VectorIndex snapshot from a reader.
pub fn read_snapshot<R: Read>(mut reader: R) -> Result<VectorIndex> {
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    deserialize(&data)
}

/// Compute a checksum of the index for verification.
///
/// Uses key metadata fields to generate a deterministic hash.
pub fn compute_checksum(index: &VectorIndex) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    // Hash key components
    index.metadata.dimensions.hash(&mut hasher);
    index.metadata.vector_count.hash(&mut hasher);
    format!("{:?}", index.metadata.metric).hash(&mut hasher);
    index.watermark.effective_t().hash(&mut hasher);

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_test_index() -> VectorIndex {
        let mut index = VectorIndex::new(3, DistanceMetric::Cosine).unwrap();

        // Add some vectors
        index
            .add("test:main", "http://example.org/doc1", &[1.0, 0.0, 0.0])
            .unwrap();
        index
            .add("test:main", "http://example.org/doc2", &[0.0, 1.0, 0.0])
            .unwrap();
        index
            .add("test:main", "http://example.org/doc3", &[0.0, 0.0, 1.0])
            .unwrap();

        // Set watermarks
        index.watermark.update("test:main", 42);

        // Set property deps
        index.property_deps = VectorPropertyDeps::new("http://example.org/embedding");

        index
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let original = build_test_index();

        let data = serialize(&original).expect("serialize failed");
        let restored = deserialize(&data).expect("deserialize failed");

        // Verify metadata
        assert_eq!(restored.metadata.dimensions, original.metadata.dimensions);
        assert_eq!(restored.metadata.metric, original.metadata.metric);
        assert_eq!(
            restored.metadata.vector_count,
            original.metadata.vector_count
        );

        // Verify watermarks
        assert_eq!(
            restored.watermark.get("test:main"),
            original.watermark.get("test:main")
        );

        // Verify property deps
        assert_eq!(
            restored.property_deps.embedding_property.as_ref(),
            original.property_deps.embedding_property.as_ref()
        );

        // Verify vectors exist
        assert!(restored.contains("test:main", "http://example.org/doc1"));
        assert!(restored.contains("test:main", "http://example.org/doc2"));
        assert!(restored.contains("test:main", "http://example.org/doc3"));
        assert_eq!(restored.len(), 3);

        // Verify search works
        let results = restored.search(&[1.0, 0.0, 0.0], 3).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].iri.as_ref(), "http://example.org/doc1");
    }

    #[test]
    fn test_serialize_empty_index() {
        let original = VectorIndex::new(768, DistanceMetric::Cosine).unwrap();

        let data = serialize(&original).expect("serialize failed");
        let restored = deserialize(&data).expect("deserialize failed");

        assert_eq!(restored.len(), 0);
        assert_eq!(restored.dimensions(), 768);
    }

    #[test]
    fn test_invalid_magic_bytes() {
        let data = b"XXXXsome data here";
        let result = deserialize(data);
        assert!(matches!(result, Err(VectorError::InvalidFormat(_))));
    }

    #[test]
    fn test_invalid_version() {
        let mut data = Vec::new();
        data.extend_from_slice(SNAPSHOT_MAGIC);
        data.push(99); // Invalid version
        data.extend_from_slice(&[0, 0, 0, 0]); // Zero length

        let result = deserialize(&data);
        assert!(matches!(
            result,
            Err(VectorError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn test_truncated_data() {
        let original = build_test_index();
        let data = serialize(&original).expect("serialize failed");

        // Truncate the data
        let truncated = &data[0..data.len() / 2];
        let result = deserialize(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_read_snapshot() {
        let original = build_test_index();

        let mut buffer = Vec::new();
        write_snapshot(&original, &mut buffer).expect("write failed");

        let cursor = std::io::Cursor::new(buffer);
        let restored = read_snapshot(cursor).expect("read failed");

        assert_eq!(restored.len(), original.len());
        assert_eq!(restored.dimensions(), original.dimensions());
    }

    #[test]
    fn test_compute_checksum() {
        let index1 = build_test_index();
        let index2 = build_test_index();

        // Same index should have same checksum
        let checksum1 = compute_checksum(&index1);
        let checksum2 = compute_checksum(&index2);
        assert_eq!(checksum1, checksum2);

        // Different index should have different checksum
        let mut index3 = build_test_index();
        index3
            .add("test:main", "http://example.org/doc4", &[0.5, 0.5, 0.0])
            .unwrap();

        let checksum3 = compute_checksum(&index3);
        assert_ne!(checksum1, checksum3);
    }

    #[test]
    fn test_length_prefixed_little_endian() {
        let mut data = Vec::new();
        write_length_prefixed(&mut data, b"hello");

        // Length should be little-endian
        assert_eq!(data[0..4], [5, 0, 0, 0]); // 5 in little-endian
        assert_eq!(&data[4..9], b"hello");
    }

    #[test]
    fn test_deterministic_snapshot_bytes() {
        // This test verifies that serializing the same logical index
        // produces identical bytes every time. This is critical for
        // snapshot integrity and cache keying.

        // Build two indexes with the SAME data added in the SAME order
        fn build_index() -> VectorIndex {
            let mut index = VectorIndex::new(3, DistanceMetric::Cosine).unwrap();

            // Add vectors in a specific order
            index
                .add("ledger:main", "http://example.org/aaa", &[1.0, 0.0, 0.0])
                .unwrap();
            index
                .add("ledger:main", "http://example.org/bbb", &[0.0, 1.0, 0.0])
                .unwrap();
            index
                .add("ledger:main", "http://example.org/ccc", &[0.0, 0.0, 1.0])
                .unwrap();

            // Set watermark
            index.watermark.update("ledger:main", 100);

            // Set property deps
            index.property_deps = VectorPropertyDeps::new("http://example.org/embedding");

            index
        }

        let index1 = build_index();
        let index2 = build_index();

        let bytes1 = serialize(&index1).expect("serialize index1");
        let bytes2 = serialize(&index2).expect("serialize index2");

        // The serialized bytes should be IDENTICAL
        assert_eq!(
            bytes1.len(),
            bytes2.len(),
            "Serialized lengths differ: {} vs {}",
            bytes1.len(),
            bytes2.len()
        );

        // Compare bytes (excluding usearch portion which may have internal non-determinism)
        // First let's verify the header + metadata + IRI mapping + watermark + deps are identical
        // The usearch binary portion might have internal non-determinism depending on version

        // Parse out the sections
        fn get_sections(data: &[u8]) -> (usize, usize, usize, usize) {
            // Skip magic (4) + version (1)
            let mut offset = 5;

            // Metadata section
            let meta_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4 + meta_len;

            // IRI section
            let iri_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4 + iri_len;

            // Watermark section
            let wm_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4 + wm_len;

            // Deps section
            let deps_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4 + deps_len;

            (offset, meta_len, iri_len, deps_len)
        }

        let (pre_usearch_len1, _, _, _) = get_sections(&bytes1);
        let (pre_usearch_len2, _, _, _) = get_sections(&bytes2);

        // The pre-usearch portions should be byte-for-byte identical
        assert_eq!(
            &bytes1[..pre_usearch_len1],
            &bytes2[..pre_usearch_len2],
            "Pre-usearch bytes should be identical (deterministic serialization)"
        );

        // Verify the index can be deserialized and produces the same results
        let restored1 = deserialize(&bytes1).expect("deserialize index1");
        let restored2 = deserialize(&bytes2).expect("deserialize index2");

        assert_eq!(restored1.len(), restored2.len());
        assert_eq!(restored1.dimensions(), restored2.dimensions());

        // Both should produce the same search results
        let results1 = restored1.search(&[1.0, 0.0, 0.0], 3).unwrap();
        let results2 = restored2.search(&[1.0, 0.0, 0.0], 3).unwrap();

        assert_eq!(results1.len(), results2.len());
        for (r1, r2) in results1.iter().zip(results2.iter()) {
            assert_eq!(r1.iri, r2.iri);
            assert!((r1.score - r2.score).abs() < 0.0001);
        }
    }
}
