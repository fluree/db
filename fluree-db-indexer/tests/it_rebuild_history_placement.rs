//! Regression: end-to-end check that a full rebuild (`build_index`, PSOT order)
//! files a fact's transition entries into the leaflet holding its row when that
//! row is the first record of a new predicate segment — so time-travel replay
//! of the segment still sees the fact AS OF a time before its final assertion.

use fluree_db_binary_index::format::run_record::{RunSortOrder, LIST_INDEX_NONE};
use fluree_db_binary_index::format::run_record_v2::{RunRecordV2, RECORD_V2_WITH_OP_WIRE_SIZE};
use fluree_db_binary_index::read::column_types::ColumnProjection;
use fluree_db_binary_index::read::leaf_access::{FullBlobLeafHandle, LeafHandle};
use fluree_db_binary_index::replay_leaflet;
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKey;
use fluree_db_indexer::run_index::build::index_build::{build_index, IndexBuildConfig};
use fluree_db_indexer::run_index::run_file::{
    RunFileHeader, RUN_V2_HEADER_LEN, RUN_V2_VERSION_WITH_OP,
};

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

/// Uncompressed with-op run file (version 2, flags = 0).
fn write_run_with_op(path: &std::path::Path, records: &[RunRecordV2], ops: &[u8]) {
    let header = RunFileHeader {
        version: RUN_V2_VERSION_WITH_OP,
        sort_order: RunSortOrder::Psot,
        flags: 0,
        record_count: records.len() as u64,
        records_offset: RUN_V2_HEADER_LEN as u64,
        min_t: 1,
        max_t: 6,
    };
    let mut out = vec![0u8; RUN_V2_HEADER_LEN];
    header.write_to(&mut out);
    let mut buf = [0u8; RECORD_V2_WITH_OP_WIRE_SIZE];
    for (r, &op) in records.iter().zip(ops) {
        r.write_run_le_with_op(op, &mut buf);
        out.extend_from_slice(&buf);
    }
    std::fs::write(path, out).unwrap();
}

#[test]
fn rebuild_psot_first_fact_of_predicate_loses_replay_history() {
    let dir = std::env::temp_dir().join("fluree_rebuild_history_placement");
    let _ = std::fs::remove_dir_all(&dir);
    let run_dir = dir.join("runs");
    let index_dir = dir.join("index");
    std::fs::create_dir_all(&run_dir).unwrap();

    // PSOT order: p=1 plain row, then p=2 fact X with lifecycle a@1, r@4, a@6.
    let records = vec![
        rec(1, 1, 10, 1),
        rec(2, 2, 20, 1),
        rec(2, 2, 20, 4),
        rec(2, 2, 20, 6),
    ];
    let ops = vec![1u8, 1, 0, 1];
    write_run_with_op(&run_dir.join("run_00000.frn"), &records, &ops);

    let config = IndexBuildConfig {
        run_dir,
        index_dir,
        sort_order: RunSortOrder::Psot,
        leaflet_target_rows: 100,
        leaf_target_rows: 1000,
        zstd_level: 1,
        skip_dedup: false,
        skip_history: false,
        g_id: 0,
        progress: None,
    };
    let result = build_index(&config).unwrap();
    let leaf = &result.graphs[0].leaf_infos[0];
    let leaf_bytes = std::fs::read(&leaf.leaf_path).unwrap();
    let sidecar_bytes = leaf
        .sidecar_path
        .as_ref()
        .map(|p| std::fs::read(p).unwrap());

    let handle = FullBlobLeafHandle::new(leaf_bytes, sidecar_bytes, 0).unwrap();
    let n = handle.dir().entries.len();
    eprintln!("leaflets: {n}");
    for i in 0..n {
        let seg = handle.load_sidecar_segment(i).unwrap();
        let entries: Vec<(u32, u64, u32, u8)> = seg
            .iter()
            .map(|e| (e.p_id, e.s_id.as_u64(), e.t, e.op))
            .collect();
        eprintln!("leaflet {i} history: {entries:?}");
    }

    // Replay the p=2 leaflet to t=2: X (asserted@1, retracted@4) must show.
    let p2 = n - 1;
    let batch = handle
        .load_columns(p2, &ColumnProjection::all(), RunSortOrder::Psot)
        .unwrap();
    let history = handle.load_sidecar_segment(p2).unwrap();
    let replayed = replay_leaflet(&batch, &history, 2, RunSortOrder::Psot);
    let view = replayed.as_ref().unwrap_or(&batch);
    let x_present = (0..view.row_count).any(|i| view.s_id.get(i) == 2);
    eprintln!("fact X present at t=2 via p=2 leaflet replay: {x_present}");

    let _ = std::fs::remove_dir_all(&dir);
    assert!(x_present, "fact X must be present AS OF t=2");
}
