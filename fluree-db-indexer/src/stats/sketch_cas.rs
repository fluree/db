//! CID-based HLL sketch blob persistence.
//!
//! Per-property HLL sketches are serialized into a single [`HllSketchBlob`] and
//! stored in content-addressed storage (CAS) via `ContentKind::StatsSketch`.
//! The blob's `ContentId` is stored in `IndexRoot.sketch_ref`.
//!
//! For incremental refresh, use [`load_sketch_blob`] +
//! [`super::IdStatsHook::with_prior_properties`].

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{IndexerError, Result};
use crate::hll::HllSketch256;
use fluree_db_core::GraphId;

use super::id_hook::{GraphPropertyKey, IdPropertyHll};

/// CAS-persisted HLL sketch blob.
///
/// Contains all per-(graph, property) HLL sketches produced by `IdStatsHook`.
/// Written to CAS as a single JSON blob; its `ContentId` is stored in
/// `IndexRoot.sketch_ref`. Counts are clamped to ≥ 0 (snapshot state,
/// not raw signed deltas).
///
/// Entries are sorted by `(g_id, p_id)` for deterministic serialization and
/// thus deterministic CID computation.
#[derive(Debug, Serialize, Deserialize)]
pub struct HllSketchBlob {
    /// Schema version for forward compatibility.
    pub version: u32,
    /// The maximum transaction time covered (equals `index_t`).
    pub index_t: i64,
    /// Per-(graph, property) HLL entries, sorted by `(g_id, p_id)`.
    pub entries: Vec<HllPropertyEntry>,
}

/// A single property's HLL state within an [`HllSketchBlob`].
#[derive(Debug, Serialize, Deserialize)]
pub struct HllPropertyEntry {
    /// Graph dictionary ID (0 = default graph).
    pub g_id: GraphId,
    /// Predicate dictionary ID.
    pub p_id: u32,
    /// Flake count (clamped to ≥ 0; snapshot state, not raw delta).
    pub count: u64,
    /// Hex-encoded 256-byte values HLL register array.
    pub values_hll: String,
    /// Hex-encoded 256-byte subjects HLL register array.
    pub subjects_hll: String,
    /// Most recent transaction time for this property.
    pub last_modified_t: i64,
    /// Per-datatype flake counts, sorted by tag, clamped to ≥ 0.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datatypes: Vec<(u8, u64)>,
}

impl HllSketchBlob {
    const CURRENT_VERSION: u32 = 1;

    /// Serialize from the `IdStatsHook`'s properties map.
    ///
    /// Must be called BEFORE `finalize_with_aggregate_properties()` consumes the
    /// hook, by borrowing `hook.properties()`. Counts and per-datatype deltas are
    /// clamped to ≥ 0 (the blob represents snapshot state, not raw signed deltas).
    pub fn from_properties(
        index_t: i64,
        properties: &HashMap<GraphPropertyKey, IdPropertyHll>,
    ) -> Self {
        let mut entries: Vec<HllPropertyEntry> = properties
            .iter()
            .map(|(key, hll)| {
                let mut dt_vec: Vec<(u8, u64)> = hll
                    .datatypes
                    .iter()
                    .filter(|(_, &v)| v > 0)
                    .map(|(&k, &v)| (k, v.max(0) as u64))
                    .collect();
                dt_vec.sort_by_key(|&(tag, _)| tag);

                HllPropertyEntry {
                    g_id: key.g_id,
                    p_id: key.p_id,
                    count: hll.count.max(0) as u64,
                    values_hll: hex::encode(hll.values_hll.to_bytes()),
                    subjects_hll: hex::encode(hll.subjects_hll.to_bytes()),
                    last_modified_t: hll.last_modified_t,
                    datatypes: dt_vec,
                }
            })
            .collect();
        entries.sort_by_key(|a| (a.g_id, a.p_id));

        Self {
            version: Self::CURRENT_VERSION,
            index_t,
            entries,
        }
    }

    /// Serialize to canonical JSON bytes (deterministic via sorted `entries`).
    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    ///
    /// Returns an error if the version is not supported.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        let blob: Self = serde_json::from_slice(bytes)
            .map_err(|e| IndexerError::Serialization(e.to_string()))?;
        if blob.version != 1 {
            return Err(IndexerError::Serialization(format!(
                "unsupported sketch blob version {} (expected 1)",
                blob.version
            )));
        }
        Ok(blob)
    }

    /// Reconstruct the `HashMap<GraphPropertyKey, IdPropertyHll>` from the blob.
    ///
    /// Used to load prior sketches for incremental refresh.
    pub fn into_properties(self) -> Result<HashMap<GraphPropertyKey, IdPropertyHll>> {
        let mut map = HashMap::with_capacity(self.entries.len());
        for entry in self.entries {
            let values_bytes = hex::decode(&entry.values_hll).map_err(|e| {
                IndexerError::Serialization(format!(
                    "bad hex values_hll for g{}:p{}: {}",
                    entry.g_id, entry.p_id, e
                ))
            })?;
            let subjects_bytes = hex::decode(&entry.subjects_hll).map_err(|e| {
                IndexerError::Serialization(format!(
                    "bad hex subjects_hll for g{}:p{}: {}",
                    entry.g_id, entry.p_id, e
                ))
            })?;

            let values_hll = decode_hll_registers(&values_bytes, entry.g_id, entry.p_id)?;
            let subjects_hll = decode_hll_registers(&subjects_bytes, entry.g_id, entry.p_id)?;
            let datatypes: HashMap<u8, i64> = entry
                .datatypes
                .into_iter()
                .map(|(k, v)| (k, v as i64))
                .collect();

            map.insert(
                GraphPropertyKey {
                    g_id: entry.g_id,
                    p_id: entry.p_id,
                },
                IdPropertyHll::from_sketches(
                    entry.count as i64,
                    values_hll,
                    subjects_hll,
                    entry.last_modified_t,
                    datatypes,
                ),
            );
        }
        Ok(map)
    }
}

/// Decode hex-decoded bytes into an `HllSketch256`, validating 256-byte length.
fn decode_hll_registers(bytes: &[u8], g_id: GraphId, p_id: u32) -> Result<HllSketch256> {
    if bytes.len() != 256 {
        return Err(IndexerError::Serialization(format!(
            "HLL registers must be 256 bytes for g{}:p{}, got {}",
            g_id,
            p_id,
            bytes.len()
        )));
    }
    let mut registers = [0u8; 256];
    registers.copy_from_slice(bytes);
    Ok(HllSketch256::from_bytes(&registers))
}

/// Load an HLL sketch blob from CAS by its `ContentId`.
///
/// Returns `None` if the blob is not found (first build, or storage migration).
/// Propagates real I/O errors.
pub async fn load_sketch_blob(
    content_store: &dyn fluree_db_core::ContentStore,
    sketch_id: &fluree_db_core::ContentId,
) -> Result<Option<HllSketchBlob>> {
    match content_store.get(sketch_id).await {
        Ok(bytes) => {
            let blob = HllSketchBlob::from_json_bytes(&bytes)?;
            Ok(Some(blob))
        }
        Err(fluree_db_core::Error::NotFound(_)) => Ok(None),
        Err(e) => Err(IndexerError::StorageRead(format!(
            "sketch blob {sketch_id}: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_test_properties() -> HashMap<GraphPropertyKey, IdPropertyHll> {
        let mut map = HashMap::new();

        let mut hll1 = IdPropertyHll::new();
        hll1.values_hll.insert_hash(100);
        hll1.values_hll.insert_hash(200);
        hll1.subjects_hll.insert_hash(1000);
        hll1.count = 5;
        hll1.last_modified_t = 10;
        *hll1.datatypes.entry(3).or_insert(0) += 3; // 3 string values
        *hll1.datatypes.entry(5).or_insert(0) += 2; // 2 ref values
        map.insert(GraphPropertyKey { g_id: 0, p_id: 1 }, hll1);

        let mut hll2 = IdPropertyHll::new();
        hll2.values_hll.insert_hash(300);
        hll2.subjects_hll.insert_hash(2000);
        hll2.count = 3;
        hll2.last_modified_t = 8;
        map.insert(GraphPropertyKey { g_id: 0, p_id: 2 }, hll2);

        map
    }

    #[test]
    fn test_sketch_blob_round_trip() {
        let props = make_test_properties();
        let blob = HllSketchBlob::from_properties(10, &props);

        let bytes = blob.to_json_bytes().unwrap();
        let parsed = HllSketchBlob::from_json_bytes(&bytes).unwrap();

        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.index_t, 10);
        assert_eq!(parsed.entries.len(), 2);

        // Reconstruct back to properties
        let restored = parsed.into_properties().unwrap();
        assert_eq!(restored.len(), 2);

        let key1 = GraphPropertyKey { g_id: 0, p_id: 1 };
        let key2 = GraphPropertyKey { g_id: 0, p_id: 2 };
        assert!(restored.contains_key(&key1));
        assert!(restored.contains_key(&key2));
        assert_eq!(restored[&key1].count, 5);
        assert_eq!(restored[&key2].count, 3);
        assert_eq!(restored[&key1].last_modified_t, 10);
        assert_eq!(restored[&key2].last_modified_t, 8);
    }

    #[test]
    fn test_sketch_blob_empty() {
        let empty: HashMap<GraphPropertyKey, IdPropertyHll> = HashMap::new();
        let blob = HllSketchBlob::from_properties(5, &empty);

        assert_eq!(blob.entries.len(), 0);
        let bytes = blob.to_json_bytes().unwrap();
        let parsed = HllSketchBlob::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.entries.len(), 0);

        let restored = parsed.into_properties().unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn test_sketch_blob_deterministic() {
        let props = make_test_properties();
        let bytes1 = HllSketchBlob::from_properties(10, &props)
            .to_json_bytes()
            .unwrap();
        let bytes2 = HllSketchBlob::from_properties(10, &props)
            .to_json_bytes()
            .unwrap();
        assert_eq!(bytes1, bytes2, "same input must produce identical bytes");
    }

    #[test]
    fn test_sketch_blob_hll_fidelity() {
        let props = make_test_properties();
        let blob = HllSketchBlob::from_properties(10, &props);
        let bytes = blob.to_json_bytes().unwrap();
        let restored = HllSketchBlob::from_json_bytes(&bytes)
            .unwrap()
            .into_properties()
            .unwrap();

        let key = GraphPropertyKey { g_id: 0, p_id: 1 };
        let original = &props[&key];
        let round_tripped = &restored[&key];

        // HLL registers should be identical after round-trip
        assert_eq!(
            original.values_hll.registers(),
            round_tripped.values_hll.registers(),
            "values HLL registers must survive round-trip"
        );
        assert_eq!(
            original.subjects_hll.registers(),
            round_tripped.subjects_hll.registers(),
            "subjects HLL registers must survive round-trip"
        );
    }

    #[test]
    fn test_sketch_blob_clamps_negatives() {
        let mut map = HashMap::new();
        let mut hll = IdPropertyHll::new();
        hll.count = -3; // negative from retractions
        *hll.datatypes.entry(3).or_insert(0) = -2; // negative dt
        hll.last_modified_t = 5;
        map.insert(GraphPropertyKey { g_id: 0, p_id: 1 }, hll);

        let blob = HllSketchBlob::from_properties(5, &map);
        assert_eq!(blob.entries[0].count, 0, "negative count clamped to 0");
        assert!(
            blob.entries[0].datatypes.is_empty()
                || blob.entries[0].datatypes.iter().all(|(_, v)| *v == 0),
            "negative datatypes clamped to 0"
        );
    }

    #[test]
    fn test_sketch_blob_sorted_by_g_id_p_id() {
        let mut map = HashMap::new();
        // Insert in random order
        for &(g, p) in &[(1, 10), (0, 5), (0, 1), (1, 2), (0, 10)] {
            let mut hll = IdPropertyHll::new();
            hll.count = 1;
            hll.last_modified_t = 1;
            map.insert(GraphPropertyKey { g_id: g, p_id: p }, hll);
        }

        let blob = HllSketchBlob::from_properties(1, &map);
        let keys: Vec<(GraphId, u32)> = blob.entries.iter().map(|e| (e.g_id, e.p_id)).collect();
        assert_eq!(
            keys,
            vec![(0, 1), (0, 5), (0, 10), (1, 2), (1, 10)],
            "entries must be sorted by (g_id, p_id)"
        );
    }

    #[test]
    fn test_sketch_blob_rejects_unknown_version() {
        let json = r#"{"version":99,"index_t":1,"entries":[]}"#;
        let err = HllSketchBlob::from_json_bytes(json.as_bytes());
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("unsupported sketch blob version 99"),
            "unexpected error: {msg}"
        );
    }
}
