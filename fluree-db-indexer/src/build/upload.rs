//! CAS upload primitives and index artifact upload.
//!
//! Contains low-level helpers for writing content to a `ContentStore`
//! (`upload_dict_blob`, `upload_dict_file`, `upload_stats_sketch`) and the bounded-parallelism
//! `upload_indexes_to_cas` function for uploading index branches and leaves.

use fluree_db_binary_index::RunSortOrder;
use fluree_db_core::tracking::Tracker;
use fluree_db_core::{ContentId, ContentKind, ContentStore, GraphId};

use std::collections::HashMap;

use crate::error::{IndexerError, Result};
use crate::fuel::charge_extra_leaflets;
use crate::stats::sketch_cas::MAX_PAYLOAD_BYTES;
use crate::stats::{GraphPropertyKey, HllSketchBlob, IdPropertyHll};

use super::types::UploadedIndexes;

/// Upload a single dict blob (already in memory) to the content store and return its CID.
///
/// The base per-write fuel is charged by [`crate::fuel::MeteredContentStore`]
/// when `cs` is the metered wrapper; this helper does not need a tracker.
pub(crate) async fn upload_dict_blob(
    cs: &dyn ContentStore,
    dict: fluree_db_core::DictKind,
    bytes: &[u8],
    msg: &'static str,
) -> Result<ContentId> {
    let kind = ContentKind::DictBlob { dict };
    let cid = cs
        .put(kind, bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    tracing::debug!(cid = %cid, bytes = bytes.len(), "{msg}");
    Ok(cid)
}

/// Encode and upload the stats sketch. `None` when there is nothing to
/// persist or the sketch cannot be encoded; otherwise the CID and the bytes
/// written, for cache seeding.
pub(crate) async fn upload_stats_sketch(
    cs: &dyn ContentStore,
    index_t: i64,
    properties: &HashMap<GraphPropertyKey, IdPropertyHll>,
) -> Result<Option<(ContentId, Vec<u8>)>> {
    upload_stats_sketch_within(cs, index_t, properties, MAX_PAYLOAD_BYTES).await
}

async fn upload_stats_sketch_within(
    cs: &dyn ContentStore,
    index_t: i64,
    properties: &HashMap<GraphPropertyKey, IdPropertyHll>,
    max_payload: usize,
) -> Result<Option<(ContentId, Vec<u8>)>> {
    if properties.is_empty() {
        return Ok(None);
    }
    let bytes =
        match HllSketchBlob::from_properties(index_t, properties).to_bytes_within(max_payload) {
            Ok(bytes) => bytes,
            // Index without a sketch rather than not at all: failing here would
            // fail every build of this ledger, rebuilds included. The next build
            // reseeds NDV from the root's stats.
            Err(e) => {
                tracing::error!(error = %e, entries = properties.len(), "stats sketch not written");
                return Ok(None);
            }
        };
    let cid = cs
        .put(ContentKind::StatsSketch, &bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    tracing::debug!(%cid, bytes = bytes.len(), entries = properties.len(), "stats sketch uploaded");
    Ok(Some((cid, bytes)))
}

/// Read a dict artifact file from disk and upload it to the content store.
pub(crate) async fn upload_dict_file(
    cs: &dyn ContentStore,
    path: &std::path::Path,
    dict: fluree_db_core::DictKind,
    msg: &'static str,
) -> Result<ContentId> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", path.display(), e)))?;
    let cid = upload_dict_blob(cs, dict, &bytes, msg).await?;
    tracing::debug!(path = %path.display(), "dict artifact source path");
    Ok(cid)
}

/// Upload index artifacts (FLI3 leaves, FHS1 sidecars, FBR3 branches) to the content store.
///
/// Default graph (g_id=0) collects inline `LeafEntry` for root embedding.
/// Named graphs upload branch manifests and return branch CIDs.
///
/// The base per-write fuel for every CAS write is charged by
/// [`crate::fuel::MeteredContentStore`] when `cs` is the metered wrapper.
/// The `tracker` param is used here only for the additional per-leaflet
/// charge on FLI3 leaf writes (passthrough leaflets are not charged because
/// no zstd encoding work was performed).
pub(crate) async fn upload_indexes_to_cas(
    cs: &dyn ContentStore,
    tracker: &Tracker,
    build_result: &crate::BuildResult,
) -> Result<UploadedIndexes> {
    use fluree_db_binary_index::format::branch::LeafEntry;
    use fluree_db_binary_index::format::index_root::NamedGraphRouting;
    use std::collections::BTreeMap;

    let mut default_orders: Vec<(RunSortOrder, Vec<LeafEntry>)> = Vec::new();
    let mut named_map: BTreeMap<GraphId, Vec<(RunSortOrder, ContentId)>> = BTreeMap::new();

    for (order, order_result) in &build_result.order_results {
        for graph in &order_result.graphs {
            let g_id = graph.g_id;
            let is_default_graph = g_id == 0;

            // Upload leaf blobs + sidecar blobs.
            for leaf_info in &graph.leaf_infos {
                // Guard: sidecar_cid and sidecar_path must agree.
                match (&leaf_info.sidecar_cid, &leaf_info.sidecar_path) {
                    (Some(_), None) => {
                        return Err(IndexerError::StorageWrite(
                            "leaf has sidecar_cid but no sidecar_path".into(),
                        ));
                    }
                    (None, Some(_)) => {
                        return Err(IndexerError::StorageWrite(
                            "leaf has sidecar_path but no sidecar_cid".into(),
                        ));
                    }
                    _ => {}
                }

                // Sidecar first (CAS ordering: sidecar must exist before leaf references it).
                if let (Some(sc_cid), Some(sc_path)) =
                    (&leaf_info.sidecar_cid, &leaf_info.sidecar_path)
                {
                    let sc_bytes = tokio::fs::read(sc_path).await.map_err(|e| {
                        IndexerError::StorageRead(format!("read {}: {}", sc_path.display(), e))
                    })?;
                    cs.put_with_id(sc_cid, &sc_bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }

                // Leaf blob.
                let leaf_bytes = tokio::fs::read(&leaf_info.leaf_path).await.map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "read {}: {}",
                        leaf_info.leaf_path.display(),
                        e
                    ))
                })?;
                cs.put_with_id(&leaf_info.leaf_cid, &leaf_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                charge_extra_leaflets(tracker, leaf_info.re_encoded_leaflet_count)?;
            }

            // Upload branch manifest for named graphs.
            let uploaded_branch_cid = if !is_default_graph {
                let branch_bytes = tokio::fs::read(&graph.branch_path).await.map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "read {}: {}",
                        graph.branch_path.display(),
                        e
                    ))
                })?;
                cs.put_with_id(&graph.branch_cid, &branch_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                Some(graph.branch_cid.clone())
            } else {
                None
            };

            if is_default_graph {
                default_orders.push((*order, graph.leaf_entries.clone()));
            } else {
                let branch_cid = uploaded_branch_cid.expect("named graph must have branch CID");
                named_map
                    .entry(g_id)
                    .or_default()
                    .push((*order, branch_cid));
            }
        }
    }

    let named_graphs: Vec<NamedGraphRouting> = named_map
        .into_iter()
        .map(|(g_id, orders)| NamedGraphRouting { g_id, orders })
        .collect();

    Ok(UploadedIndexes {
        default_graph_orders: default_orders,
        named_graphs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hll::HllSketch256;
    use fluree_db_core::MemoryContentStore;

    #[tokio::test]
    async fn stats_sketch_past_the_ceiling_is_skipped_not_failed() {
        let properties = HashMap::from([(
            GraphPropertyKey { g_id: 0, p_id: 1 },
            IdPropertyHll::from_sketches(
                3,
                HllSketch256::new(),
                HllSketch256::new(),
                1,
                HashMap::from([(3u8, 3i64)]),
            ),
        )]);
        let cs = MemoryContentStore::new();
        let bytes = HllSketchBlob::from_properties(1, &properties)
            .to_bytes()
            .unwrap();
        let cid = ContentId::new(ContentKind::StatsSketch, &bytes);

        let skipped = upload_stats_sketch_within(&cs, 1, &properties, 8)
            .await
            .unwrap();
        assert!(skipped.is_none());
        assert!(!cs.has(&cid).await.unwrap());

        let (written, _) = upload_stats_sketch(&cs, 1, &properties)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(written, cid);
        assert!(cs.has(&cid).await.unwrap());
    }
}
