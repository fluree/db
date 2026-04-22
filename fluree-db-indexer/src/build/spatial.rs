//! Spatial index building (S2 complex geometries).
//!
//! Groups geometry entries by `(g_id, p_id)`, builds one spatial index per
//! group using `fluree_db_spatial`, and uploads cell index leaflets, manifests,
//! and geometry arenas to CAS.

use fluree_db_binary_index::SpatialArenaRef;
use fluree_db_core::{ContentId, ContentKind, GraphId, Storage};

use crate::error::{IndexerError, Result};
use crate::run_index;

/// Build spatial indexes from collected geometry entries and upload to CAS.
///
/// Groups entries by `(g_id, p_id)`, builds one spatial index per group,
/// and uploads cell index leaflets, manifests, and geometry arenas to CAS.
///
/// Two-phase approach:
/// 1. Build the index and collect all serialized blobs with locally-computed
///    SHA-256 hashes (synchronous — no async needed).
/// 2. Upload all blobs to CAS using `content_write_bytes` (async).
///
/// This avoids re-entering `block_on` from within a sync closure, which would
/// deadlock inside the `spawn_blocking` + `handle.block_on()` pattern.
///
/// Returns per-graph spatial arena refs for inclusion in `IndexRoot`.
// Kept for: full-rebuild spatial upload pipeline (rebuild.rs collects spatial
// entries but does not yet call this; wiring deferred to rebuild-spatial milestone).
// Use when: rebuild.rs is extended to upload spatial indexes after Phase C remap.
#[expect(dead_code)]
pub(crate) async fn build_and_upload_spatial_indexes<S: Storage>(
    entries: &[crate::spatial_hook::SpatialEntry],
    predicates: &run_index::PredicateDict,
    ledger_id: &str,
    storage: &S,
) -> Result<Vec<(GraphId, Vec<SpatialArenaRef>)>> {
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;

    // Group entries by (g_id, p_id).
    let mut grouped: BTreeMap<(GraphId, u32), Vec<&crate::spatial_hook::SpatialEntry>> =
        BTreeMap::new();
    for entry in entries {
        grouped
            .entry((entry.g_id, entry.p_id))
            .or_default()
            .push(entry);
    }

    let mut per_graph: BTreeMap<GraphId, Vec<SpatialArenaRef>> = BTreeMap::new();

    for ((g_id, p_id), group_entries) in grouped {
        // Resolve predicate IRI for SpatialCreateConfig.
        let pred_iri = predicates.resolve(p_id).unwrap_or("unknown").to_string();

        let config = fluree_db_spatial::SpatialCreateConfig::new(
            format!("spatial:g{g_id}p{p_id}"),
            ledger_id.to_string(),
            pred_iri.clone(),
        );
        let mut builder = fluree_db_spatial::SpatialIndexBuilder::new(config);

        for entry in &group_entries {
            // add_geometry returns Ok(false) for skipped entries (e.g., parse errors).
            // We log but do not fail the entire build for individual geometry errors.
            if let Err(e) =
                builder.add_geometry(entry.subject_id, &entry.wkt, entry.t, entry.is_assert)
            {
                tracing::warn!(
                    subject_id = entry.subject_id,
                    p_id = p_id,
                    error = %e,
                    "spatial: failed to add geometry, skipping"
                );
            }
        }

        let build_result = builder
            .build()
            .map_err(|e| IndexerError::Other(format!("spatial build error: {e}")))?;

        if build_result.entries.is_empty() {
            continue;
        }

        // Phase 1: Build the index and collect all serialized blobs.
        // We compute SHA-256 locally so write_to_cas gets the correct hashes
        // without needing async CAS writes during the build.
        let mut pending_blobs: Vec<(String, Vec<u8>)> = Vec::new();
        let write_result = build_result
            .write_to_cas(|bytes| {
                let hash_hex = hex::encode(Sha256::digest(bytes));
                pending_blobs.push((hash_hex.clone(), bytes.to_vec()));
                Ok(hash_hex)
            })
            .map_err(|e| IndexerError::Other(format!("spatial build: {e}")))?;

        // Phase 2: Upload all collected blobs to CAS.
        for (expected_hash, blob_bytes) in &pending_blobs {
            let cas_result = storage
                .content_write_bytes(ContentKind::SpatialIndex, ledger_id, blob_bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(format!("spatial CAS write: {e}")))?;
            debug_assert_eq!(
                &cas_result.content_hash, expected_hash,
                "CAS content_hash mismatch for spatial blob"
            );
        }

        // Construct ContentIds from the content hashes.
        let spatial_codec = ContentKind::SpatialIndex.to_codec();
        let manifest_cid =
            ContentId::from_hex_digest(spatial_codec, &write_result.manifest_address).ok_or_else(
                || {
                    IndexerError::Other(format!(
                        "invalid spatial manifest hash: {}",
                        write_result.manifest_address
                    ))
                },
            )?;
        let arena_cid = ContentId::from_hex_digest(spatial_codec, &write_result.arena_address)
            .ok_or_else(|| {
                IndexerError::Other(format!(
                    "invalid spatial arena hash: {}",
                    write_result.arena_address
                ))
            })?;
        let leaflet_cids: Vec<ContentId> = write_result
            .leaflet_addresses
            .iter()
            .map(|hash| {
                ContentId::from_hex_digest(spatial_codec, hash).ok_or_else(|| {
                    IndexerError::Other(format!("invalid spatial leaflet hash: {hash}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Serialize SpatialIndexRoot as JSON and write to CAS.
        let root_json = serde_json::to_vec(&write_result.root)
            .map_err(|e| IndexerError::Other(format!("spatial root serialize: {e}")))?;
        let root_cas = storage
            .content_write_bytes(ContentKind::SpatialIndex, ledger_id, &root_json)
            .await
            .map_err(|e| IndexerError::StorageWrite(format!("spatial root CAS write: {e}")))?;
        let root_cid = ContentId::from_hex_digest(spatial_codec, &root_cas.content_hash)
            .ok_or_else(|| {
                IndexerError::Other(format!(
                    "invalid spatial root hash: {}",
                    root_cas.content_hash
                ))
            })?;

        per_graph.entry(g_id).or_default().push(SpatialArenaRef {
            p_id,
            root_cid,
            manifest: manifest_cid,
            arena: arena_cid,
            leaflets: leaflet_cids,
        });

        tracing::info!(
            g_id,
            p_id,
            predicate = %pred_iri,
            geometries = write_result.root.geometry_count,
            cell_entries = write_result.root.entry_count,
            blobs_uploaded = pending_blobs.len(),
            "spatial index built for (graph, predicate)"
        );
    }

    Ok(per_graph.into_iter().collect())
}
