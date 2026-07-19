//! End-to-end regression tests for the history-sidecar transition-log
//! rework (`history-transition-log-plan.md`): drives the real pipeline
//! (post-dedup winner + superseded streams → `update_branch` merge →
//! sidecar encode → `replay_leaflet`) and asserts the true historical
//! state, so a failure here is a live wrong `AS OF` answer served from
//! the index.

use std::collections::HashMap;

use fluree_db_binary_index::format::branch::{build_branch_bytes, LeafEntry};
use fluree_db_binary_index::format::leaf::LeafWriter;
use fluree_db_binary_index::format::run_record::{RunSortOrder, LIST_INDEX_NONE};
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::read::leaf_access::{FullBlobLeafHandle, LeafHandle};
use fluree_db_binary_index::replay_leaflet;
use fluree_db_binary_index::ColumnProjection;
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::ContentId;
use fluree_db_indexer::run_index::incremental_branch::{update_branch, BranchUpdateConfig};

fn int_rec(s_id: u64, v: i64, t: u32) -> RunRecordV2 {
    RunRecordV2 {
        s_id: SubjectId::from_u64(s_id),
        o_key: ObjKey::encode_i64(v).as_u64(),
        p_id: 1,
        t,
        o_i: LIST_INDEX_NONE,
        o_type: OType::XSD_INTEGER.as_u16(),
        g_id: 0,
    }
}

/// Build a single-leaf SPOT branch from base records, apply the novelty
/// winner stream through the real branch/leaf merge, and return the merged
/// leaf bytes + sidecar bytes.
fn apply_window(
    base: &[RunRecordV2],
    novelty: &[RunRecordV2],
    ops: &[u8],
    superseded: &[RunRecordV2],
    superseded_ops: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    let mut writer = LeafWriter::new(RunSortOrder::Spot, 100, 1000, 1);
    for r in base {
        writer.push_record(*r).expect("push base record");
    }
    let infos = writer.finish().expect("finish base leaf");
    assert_eq!(infos.len(), 1, "base fits one leaf");

    let mut leaf_store: HashMap<ContentId, Vec<u8>> = HashMap::new();
    let mut entries: Vec<LeafEntry> = Vec::new();
    for info in infos {
        leaf_store.insert(info.leaf_cid.clone(), info.leaf_bytes);
        entries.push(LeafEntry {
            first_key: info.first_key,
            last_key: info.last_key,
            row_count: info.total_rows,
            leaf_cid: info.leaf_cid,
            sidecar_cid: info.sidecar_cid,
        });
    }
    let branch_bytes = build_branch_bytes(RunSortOrder::Spot, 0, &entries);

    let config = BranchUpdateConfig {
        order: RunSortOrder::Spot,
        g_id: 0,
        zstd_level: 1,
        leaflet_target_rows: 100,
        leaf_target_rows: 1000,
        collect_matched: false,
    };
    let result = update_branch(
        &branch_bytes,
        novelty,
        ops,
        superseded,
        superseded_ops,
        &config,
        &|cid| Ok(leaf_store.get(cid).expect("leaf bytes").clone()),
        &|_cid| Ok(None),
    )
    .expect("update branch");

    assert_eq!(
        result.new_leaf_blobs.len(),
        1,
        "merged output fits one leaf"
    );
    let info = &result.new_leaf_blobs[0].info;
    let sidecar = info.sidecar_bytes.clone().unwrap_or_default();
    (info.leaf_bytes.clone(), sidecar)
}

/// Reconstruct the leaflet state `AS OF t_target` via the real replay path
/// and report whether subject `s_id` is present.
fn present_at(leaf_bytes: &[u8], sidecar_bytes: &[u8], t_target: i64, s_id: u64) -> bool {
    let sidecar = (!sidecar_bytes.is_empty()).then(|| sidecar_bytes.to_vec());
    let handle = FullBlobLeafHandle::new(leaf_bytes.to_vec(), sidecar, 0).expect("leaf handle");
    assert_eq!(handle.dir().entries.len(), 1, "single leaflet expected");
    let batch = handle
        .load_columns(0, &ColumnProjection::all(), RunSortOrder::Spot)
        .expect("load columns");
    let history = handle.load_sidecar_segment(0).expect("decode history");

    let replayed = replay_leaflet(&batch, &history, t_target, RunSortOrder::Spot);
    let view = replayed.as_ref().unwrap_or(&batch);
    (0..view.row_count).any(|i| view.s_id.get(i) == s_id)
}

/// Fact present since t=1, deleted at t=4 and re-added at t=6 within one
/// indexing window. The superseded retract and the winning assert are both
/// real transitions, so the sidecar records them (and no retract+assert pair
/// is synthesized): the fact is absent during [4, 6).
#[test]
fn deleted_then_readded_fact_is_absent_during_the_gap() {
    let base = vec![int_rec(10, 100, 1)];
    // Post-dedup winner stream for window events [retract@4, assert@6]:
    // the assert wins; the retract flows in the superseded stream.
    let novelty = vec![int_rec(10, 100, 6)];
    let ops = vec![1u8];
    let superseded = vec![int_rec(10, 100, 4)];
    let superseded_ops = vec![0u8];
    let (leaf, sidecar) = apply_window(&base, &novelty, &ops, &superseded, &superseded_ops);

    assert!(present_at(&leaf, &sidecar, 7, 10), "present after re-add");
    assert!(present_at(&leaf, &sidecar, 2, 10), "present before delete");
    assert!(
        !present_at(&leaf, &sidecar, 5, 10),
        "fact was deleted at t=4 and not re-added until t=6; AS OF t=5 must be absent"
    );
}

/// Fact created at t=5 and deleted at t=6 within one indexing window. No
/// row survives, but both events are real transitions recorded in the
/// sidecar, so the fact's brief life stays visible: present during [5, 6).
#[test]
fn fact_born_and_deleted_in_one_window_is_present_during_its_life() {
    let base = vec![int_rec(5, 50, 1)];
    // Post-dedup winner stream for window events [assert@5, retract@6]:
    // the retract wins; the assert flows in the superseded stream.
    let novelty = vec![int_rec(10, 100, 6)];
    let ops = vec![0u8];
    let superseded = vec![int_rec(10, 100, 5)];
    let superseded_ops = vec![1u8];
    let (leaf, sidecar) = apply_window(&base, &novelty, &ops, &superseded, &superseded_ops);

    assert!(
        !present_at(&leaf, &sidecar, 2, 10),
        "absent before creation"
    );
    assert!(!present_at(&leaf, &sidecar, 7, 10), "absent after deletion");
    assert!(
        present_at(&leaf, &sidecar, 5, 10),
        "fact lived during [5, 6); AS OF t=5 must show it present"
    );
}

/// Fact present since t=1, deleted at t=5, and (redundantly) deleted again
/// at t=6 within one window. The transition into absence is the FIRST
/// retract, so the sidecar records the deletion at t=5 and the repeat is a
/// no-op: the fact is absent from t=5 on.
#[test]
fn double_deleted_fact_is_absent_from_the_first_delete() {
    let base = vec![int_rec(5, 50, 1), int_rec(10, 100, 1)];
    // Post-dedup winner stream for window events [retract@5, retract@6]:
    // the later retract wins; the first retract flows in the superseded stream.
    let novelty = vec![int_rec(10, 100, 6)];
    let ops = vec![0u8];
    let superseded = vec![int_rec(10, 100, 5)];
    let superseded_ops = vec![0u8];
    let (leaf, sidecar) = apply_window(&base, &novelty, &ops, &superseded, &superseded_ops);

    assert!(present_at(&leaf, &sidecar, 4, 10), "present before deletes");
    assert!(!present_at(&leaf, &sidecar, 7, 10), "absent after deletes");
    assert!(
        !present_at(&leaf, &sidecar, 5, 10),
        "fact was deleted at t=5; AS OF t=5 must be absent"
    );
}
