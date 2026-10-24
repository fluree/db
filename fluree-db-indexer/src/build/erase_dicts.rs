//! The dictionary half of erasing from an index: a string only erased
//! events used is emptied from the forward packs and removed from the
//! reverse tree, once no row or history entry left refers to it. Values
//! embedded in the key need none of this.

use std::collections::BTreeSet;

use fluree_db_binary_index::dict::forward_pack::{blank_forward_entries, ForwardPack};
use fluree_db_binary_index::dict::pack_builder::DEFAULT_TARGET_PAGE_BYTES;
use fluree_db_binary_index::format::branch::{read_branch_from_bytes, LeafEntry};
use fluree_db_binary_index::format::history_sidecar::{decode_history_segment, HistorySegmentRef};
use fluree_db_binary_index::format::leaf::{decode_leaf_dir_v3_with_base, decode_leaf_header_v3};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::format::wire_helpers::DictRefs;
use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
use fluree_db_binary_index::read::column_types::ColumnProjection;
use fluree_db_binary_index::IndexRoot;
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::{ContentId, ContentKind, ContentStore, DictKind, SubjectId};

use crate::error::{IndexerError, Result};

/// Whether objects of type `o_type` are string dictionary ids.
pub(crate) fn string_typed(o_type: u16) -> bool {
    matches!(
        OType::from_u16(o_type).decode_kind(),
        DecodeKind::StringDict | DecodeKind::JsonArena
    )
}

/// Of the string ids `ids`, those no row or history entry of `root` refers
/// to, in any graph and under any string type.
pub(crate) async fn unused_strings(
    content_store: &dyn ContentStore,
    root: &IndexRoot,
    ids: &BTreeSet<u64>,
) -> Result<BTreeSet<u64>> {
    let mut used = BTreeSet::new();
    for leaves in opst_leaves(content_store, root).await? {
        for (i, leaf) in leaves.iter().enumerate() {
            let next = leaves.get(i + 1).map(|l| &l.first_key);
            let lower = (i > 0).then_some(&leaf.first_key);
            if !ids.iter().any(|id| routes_here(lower, next, *id)) {
                continue;
            }
            let bytes = fetch(content_store, &leaf.leaf_cid).await?;
            let sidecar = match &leaf.sidecar_cid {
                Some(cid) => Some(fetch(content_store, cid).await?),
                None => None,
            };
            let header = decode_leaf_header_v3(&bytes).map_err(read_err)?;
            let dir = decode_leaf_dir_v3_with_base(&bytes, &header).map_err(read_err)?;
            for entry in &dir.entries {
                if entry.row_count > 0 {
                    let batch = load_leaflet_columns(
                        &bytes,
                        entry,
                        dir.payload_base,
                        &ColumnProjection::all(),
                        RunSortOrder::Opst,
                    )
                    .map_err(read_err)?;
                    for row in 0..batch.row_count {
                        let o_key = batch.o_key.get(row);
                        if string_typed(batch.o_type.get(row)) && ids.contains(&o_key) {
                            used.insert(o_key);
                        }
                    }
                }
                if let (Some(sidecar), true) = (&sidecar, entry.history_len > 0) {
                    let segment = HistorySegmentRef {
                        offset: entry.history_offset,
                        len: entry.history_len,
                        min_t: entry.history_min_t,
                        max_t: entry.history_max_t,
                    };
                    for h in decode_history_segment(sidecar, &segment).map_err(read_err)? {
                        if string_typed(h.o_type) && ids.contains(&h.o_key) {
                            used.insert(h.o_key);
                        }
                    }
                }
            }
        }
    }
    Ok(ids.difference(&used).copied().collect())
}

/// Empty `ids` from `refs`' string forward packs and remove their values
/// from its reverse tree. Returns the artifacts replaced.
pub(crate) async fn remove_strings(
    content_store: &dyn ContentStore,
    refs: &mut DictRefs,
    ids: &BTreeSet<u64>,
) -> Result<Vec<ContentId>> {
    let mut replaced = Vec::new();
    let mut keys = Vec::new();
    for entry in &mut refs.forward_packs.string_fwd_packs {
        let mine: Vec<u64> = ids
            .iter()
            .copied()
            .filter(|id| entry.first_id <= *id && *id <= entry.last_id)
            .collect();
        if mine.is_empty() {
            continue;
        }
        let bytes = fetch(content_store, &entry.pack_cid).await?;
        let pack = ForwardPack::from_bytes(&bytes).map_err(read_err)?;
        // `ids` were found through the reverse tree, which no blanked id
        // is in: an empty value is the empty string's.
        keys.extend(
            mine.iter()
                .filter_map(|id| pack.lookup(*id))
                .map(<[u8]>::to_vec),
        );
        let blanked =
            blank_forward_entries(&bytes, &mine, DEFAULT_TARGET_PAGE_BYTES).map_err(read_err)?;
        let cid = content_store
            .put(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &blanked,
            )
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        replaced.push(std::mem::replace(&mut entry.pack_cid, cid));
    }
    keys.sort();
    keys.dedup();
    if !keys.is_empty() {
        let updated = super::dicts::remove_from_reverse_tree_async(
            content_store,
            DictKind::StringReverse,
            &refs.string_reverse,
            keys,
        )
        .await?;
        replaced.extend(updated.replaced_cids);
        refs.string_reverse = updated.tree_refs;
    }
    Ok(replaced)
}

/// Every graph's OPST leaves.
async fn opst_leaves(
    content_store: &dyn ContentStore,
    root: &IndexRoot,
) -> Result<Vec<Vec<LeafEntry>>> {
    let mut lists: Vec<Vec<LeafEntry>> = root
        .default_graph_orders
        .iter()
        .filter(|o| o.order == RunSortOrder::Opst)
        .map(|o| o.leaves.clone())
        .collect();
    for graph in &root.named_graphs {
        for (order, branch_cid) in &graph.orders {
            if *order == RunSortOrder::Opst {
                let bytes = fetch(content_store, branch_cid).await?;
                lists.push(read_branch_from_bytes(&bytes).map_err(read_err)?.leaves);
            }
        }
    }
    Ok(lists)
}

/// Whether an OPST leaf routed keys from `lower` (inclusive; the first leaf
/// takes everything below) to `upper` (exclusive) can hold string `id`
/// under some string type. Routing bounds, not the leaf's own extremes:
/// history entries route by them too.
fn routes_here(lower: Option<&RunRecordV2>, upper: Option<&RunRecordV2>, id: u64) -> bool {
    let cmp = cmp_v2_for_order(RunSortOrder::Opst);
    let from = lower.map_or(0, |k| k.o_type);
    let to = upper.map_or(u16::MAX, |k| k.o_type);
    (from..=to).filter(|ot| string_typed(*ot)).any(|ot| {
        let least = RunRecordV2 {
            s_id: SubjectId(0),
            o_key: id,
            p_id: 0,
            t: 0,
            o_i: 0,
            o_type: ot,
            g_id: 0,
        };
        let most = RunRecordV2 {
            s_id: SubjectId(u64::MAX),
            p_id: u32::MAX,
            o_i: u32::MAX,
            ..least
        };
        lower.is_none_or(|k| cmp(&most, k).is_ge()) && upper.is_none_or(|k| cmp(&least, k).is_lt())
    })
}

async fn fetch(content_store: &dyn ContentStore, cid: &ContentId) -> Result<Vec<u8>> {
    Ok(content_store
        .get(cid)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("{cid}: {e}")))?
        .into_vec())
}

fn read_err(e: std::io::Error) -> IndexerError {
    IndexerError::StorageRead(e.to_string())
}
