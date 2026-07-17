//! End-to-end reachability check for the history-sidecar fidelity gaps
//! described in `history-transition-log-plan.md`: drives the real pipeline
//! (post-dedup winner stream → `update_branch` merge → sidecar encode →
//! `replay_leaflet`) and asserts the TRUE historical state, so a failure
//! here is a live wrong `AS OF` answer served from the index.

use std::collections::HashMap;

use fluree_db_binary_index::format::branch::{build_branch_bytes, LeafEntry};
use fluree_db_binary_index::format::history_sidecar::{decode_history_segment, HistorySegmentRef};
use fluree_db_binary_index::format::leaf::{
    decode_leaf_dir_v3_with_base, decode_leaf_header_v3, LeafWriter,
};
use fluree_db_binary_index::format::run_record::{RunSortOrder, LIST_INDEX_NONE};
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
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
    candidates: &[RunRecordV2],
    candidate_ops: &[u8],
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
        candidates,
        candidate_ops,
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
    let header = decode_leaf_header_v3(leaf_bytes).expect("leaf header");
    let dir = decode_leaf_dir_v3_with_base(leaf_bytes, &header).expect("leaf dir");
    assert_eq!(dir.entries.len(), 1, "single leaflet expected");
    let entry = &dir.entries[0];

    let batch = load_leaflet_columns(
        leaf_bytes,
        entry,
        dir.payload_base,
        &ColumnProjection::all(),
        RunSortOrder::Spot,
    )
    .expect("load columns");

    let history = if entry.history_len > 0 {
        decode_history_segment(
            sidecar_bytes,
            &HistorySegmentRef {
                offset: entry.history_offset,
                len: entry.history_len,
                min_t: entry.history_min_t,
                max_t: entry.history_max_t,
            },
        )
        .expect("decode history")
    } else {
        Vec::new()
    };

    let replayed = replay_leaflet(&batch, &history, t_target, RunSortOrder::Spot);
    let view = replayed.as_ref().unwrap_or(&batch);
    (0..view.row_count).any(|i| view.s_id.get(i) == s_id)
}

/// Gap 2: fact present since t=1, deleted at t=4 and re-added at t=6 within
/// one indexing window. The dedup resolves the window to the lone re-assert
/// (assert@6), and the merge writes a synthesized retract+assert pair at t=6.
/// Truth: the fact was ABSENT during [4, 6).
#[test]
fn deleted_then_readded_fact_is_absent_during_the_gap() {
    let base = vec![int_rec(10, 100, 1)];
    // Post-dedup winner stream for window events [retract@4, assert@6]:
    // the assert wins; the superseded retract flows as a candidate.
    let novelty = vec![int_rec(10, 100, 6)];
    let ops = vec![1u8];
    let candidates = vec![int_rec(10, 100, 4)];
    let candidate_ops = vec![0u8];
    let (leaf, sidecar) = apply_window(&base, &novelty, &ops, &candidates, &candidate_ops);

    assert!(present_at(&leaf, &sidecar, 7, 10), "present after re-add");
    assert!(present_at(&leaf, &sidecar, 2, 10), "present before delete");
    assert!(
        !present_at(&leaf, &sidecar, 5, 10),
        "fact was deleted at t=4 and not re-added until t=6; AS OF t=5 must be absent"
    );
}

/// Gap 1: fact created at t=5 and deleted at t=6 within one indexing window.
/// The dedup resolves the window to the lone retract, which (by design after
/// the phantom-retract fix) writes no row and no history. Truth: the fact
/// was PRESENT during [5, 6).
#[test]
fn fact_born_and_deleted_in_one_window_is_present_during_its_life() {
    let base = vec![int_rec(5, 50, 1)];
    // Post-dedup winner stream for window events [assert@5, retract@6]:
    // the retract wins; the superseded assert flows as a candidate.
    let novelty = vec![int_rec(10, 100, 6)];
    let ops = vec![0u8];
    let candidates = vec![int_rec(10, 100, 5)];
    let candidate_ops = vec![1u8];
    let (leaf, sidecar) = apply_window(&base, &novelty, &ops, &candidates, &candidate_ops);

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

/// Gap 3: fact present since t=1, deleted at t=5, and (redundantly) deleted
/// again at t=6 within one window. The dedup keeps the highest-t retract
/// (novelty parity for current state), so the sidecar records the deletion
/// at t=6. Truth: the fact was ABSENT from t=5 on.
#[test]
fn double_deleted_fact_is_absent_from_the_first_delete() {
    let base = vec![int_rec(5, 50, 1), int_rec(10, 100, 1)];
    // Post-dedup winner stream for window events [retract@5, retract@6]:
    // the later retract wins; the superseded first retract is a candidate.
    let novelty = vec![int_rec(10, 100, 6)];
    let ops = vec![0u8];
    let candidates = vec![int_rec(10, 100, 5)];
    let candidate_ops = vec![0u8];
    let (leaf, sidecar) = apply_window(&base, &novelty, &ops, &candidates, &candidate_ops);

    assert!(present_at(&leaf, &sidecar, 4, 10), "present before deletes");
    assert!(!present_at(&leaf, &sidecar, 7, 10), "absent after deletes");
    assert!(
        !present_at(&leaf, &sidecar, 5, 10),
        "fact was deleted at t=5; AS OF t=5 must be absent"
    );
}
