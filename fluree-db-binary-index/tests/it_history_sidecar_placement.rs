//! Regression: history entries for a fact whose row opens a new PSOT predicate
//! segment must land in that row's leaflet, not the previous one. Mirrors
//! index_build's push order — history entries pushed immediately before their
//! row's push_record — and replays the segment to confirm the fact is still
//! visible AS OF a time before its final assertion.

use fluree_db_binary_index::format::history_sidecar::HistEntryV2;
use fluree_db_binary_index::format::leaf::LeafWriter;
use fluree_db_binary_index::format::run_record::{RunSortOrder, LIST_INDEX_NONE};
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::read::column_types::ColumnProjection;
use fluree_db_binary_index::read::leaf_access::{FullBlobLeafHandle, LeafHandle};
use fluree_db_binary_index::replay_leaflet;
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKey;

fn rec(p_id: u32, s_id: u64, v: i64, t: u32) -> RunRecordV2 {
    RunRecordV2 {
        s_id: SubjectId::from_u64(s_id),
        o_key: ObjKey::encode_i64(v).as_u64(),
        p_id,
        t,
        o_i: LIST_INDEX_NONE,
        o_type: OType::XSD_INTEGER.as_u16(),
        g_id: 0,
    }
}

fn hist(p_id: u32, s_id: u64, v: i64, t: u32, op: u8) -> HistEntryV2 {
    HistEntryV2 {
        s_id: SubjectId::from_u64(s_id),
        p_id,
        o_type: OType::XSD_INTEGER.as_u16(),
        o_key: ObjKey::encode_i64(v).as_u64(),
        o_i: LIST_INDEX_NONE,
        t,
        op,
    }
}

#[test]
fn history_entries_straddle_psot_segment_boundary() {
    let mut writer = LeafWriter::new(RunSortOrder::Psot, 100, 1000, 1);

    // Predicate 1: one plain row.
    writer.push_record(rec(1, 1, 10, 1)).unwrap();

    // Predicate 2, fact X with lifecycle a@1, r@4, a@6 — index_build pushes
    // the transition entries, then the row.
    writer.push_history_entry(hist(2, 2, 20, 1, 1));
    writer.push_history_entry(hist(2, 2, 20, 4, 0));
    writer.push_record(rec(2, 2, 20, 6)).unwrap();

    let infos = writer.finish().unwrap();
    assert_eq!(infos.len(), 1, "one leaf");
    let info = &infos[0];

    let handle =
        FullBlobLeafHandle::new(info.leaf_bytes.clone(), info.sidecar_bytes.clone(), 0).unwrap();
    let n_leaflets = handle.dir().entries.len();
    eprintln!("leaflets: {n_leaflets}");
    for i in 0..n_leaflets {
        let seg = handle.load_sidecar_segment(i).unwrap();
        let entries: Vec<(u32, u64, u32, u8)> = seg
            .iter()
            .map(|e| (e.p_id, e.s_id.as_u64(), e.t, e.op))
            .collect();
        eprintln!("leaflet {i} history: {entries:?}");
    }

    // Replay the p=2 leaflet to t=2: fact X was present (a@1, not yet
    // retracted). If its entries landed in leaflet 0, this replay misses them.
    let p2_leaflet = n_leaflets - 1;
    let batch = handle
        .load_columns(p2_leaflet, &ColumnProjection::all(), RunSortOrder::Psot)
        .unwrap();
    let history = handle.load_sidecar_segment(p2_leaflet).unwrap();
    let replayed = replay_leaflet(&batch, &history, 2, RunSortOrder::Psot);
    let view = replayed.as_ref().unwrap_or(&batch);
    let x_present = (0..view.row_count).any(|i| view.s_id.get(i) == 2);
    eprintln!("fact X present at t=2 via p=2 leaflet replay: {x_present}");
    assert!(
        x_present,
        "fact X (asserted t=1, retracted t=4) must be present at t=2"
    );
}
