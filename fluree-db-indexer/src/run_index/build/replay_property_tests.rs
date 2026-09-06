//! Exhaustive replay-equivalence tests for the transition-log pipeline.
//!
//! A naive reference computes a fact's presence (and the `t` at which its
//! current presence run began) directly from the raw event list. The tests
//! enumerate every op-chain up to length 4 over distinct `t`s, for both base
//! states, and check `replay_leaflet` over the REAL pipeline output against
//! the reference at every interval boundary:
//!
//! - **Incremental path**: events → `dedup_fact_lifecycles` → V1→V2
//!   conversion → `update_branch` (leaf merge + sidecar encode) → decode →
//!   replay, on a base leaf built by the rebuild pipeline (so base facts
//!   carry their assert transitions, as any post-rework leaf does).
//!   Presence must match everywhere. Row `t` must match everywhere too,
//!   with one engine-semantics adjustment: a no-op re-assert of a fact that
//!   crossed the window boundary already present REPLACES the row at the
//!   re-assert's `t` (matching the live overlay view), so from that `t` on
//!   the visible `t` is the winner's, not the run start's. Events within
//!   one window keep the run-start `t` — visible `t` for re-asserts
//!   legitimately depends on window slicing.
//! - **Rebuild path**: events → run file → `build_index` → decode → replay.
//!   The rebuild sees the full log as one window, so both presence and
//!   row `t` must match the reference everywhere.

use std::collections::HashMap;

use fluree_db_binary_index::format::branch::{build_branch_bytes, LeafEntry};
use fluree_db_binary_index::format::history_sidecar::HistEntryV2;
use fluree_db_binary_index::format::run_record::{RunRecord, RunSortOrder, LIST_INDEX_NONE};
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::read::column_types::{ColumnBatch, ColumnProjection};
use fluree_db_binary_index::read::leaf_access::{FullBlobLeafHandle, LeafHandle};
use fluree_db_binary_index::replay_leaflet;
use fluree_db_core::o_type::OType;
use fluree_db_core::o_type_registry::OTypeRegistry;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::ContentId;
use fluree_db_core::DatatypeDictId;

use crate::run_index::build::incremental_branch::{update_branch, BranchUpdateConfig};
use crate::run_index::build::incremental_resolve::dedup_fact_lifecycles;
use crate::run_index::build::index_build::{build_index, IndexBuildConfig};
use crate::run_index::runs::run_file::write_run_file_with_op;

/// Subject under test and its fixed integer value.
const FACT_S: u64 = 10;
const FACT_V: i64 = 100;
/// Padding subject keeping leaves non-empty in every case.
const PAD_S: u64 = 5;
/// The padding fact's assert `t`.
const PAD_T: u32 = 1;
/// The base fact's assert `t` when the base state is "present". Later than
/// the padding fact's, so the base fact's entry is deterministically the
/// newest carried-forward history entry (the `assemble_history` seam sees
/// it first).
const BASE_T: u32 = 2;
/// Distinct in-window event `t`s (odd, so probe targets land between them).
const EVENT_TS: [u32; 4] = [3, 5, 7, 9];
/// Probe targets: before, between, at, and after every event boundary.
const TARGETS: [u32; 11] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

/// One case: base state and the window's op sequence (ops assigned to
/// `EVENT_TS[..len]` in order).
#[derive(Clone, Copy)]
struct Case {
    base_present: bool,
    ops: [u8; 4],
    len: usize,
}

impl Case {
    fn events(&self) -> Vec<(u32, u8)> {
        (0..self.len).map(|i| (EVENT_TS[i], self.ops[i])).collect()
    }

    fn describe(&self) -> String {
        let events: Vec<String> = self
            .events()
            .iter()
            .map(|(t, op)| format!("{}{}", if *op == 1 { 'a' } else { 'r' }, t))
            .collect();
        format!(
            "base={} events=[{}]",
            if self.base_present {
                "present@2"
            } else {
                "absent"
            },
            events.join(",")
        )
    }
}

/// Every non-empty op-chain up to length 4, for both base states.
fn all_cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for base_present in [false, true] {
        for len in 1..=4usize {
            for bits in 0..(1u8 << len) {
                let mut ops = [0u8; 4];
                for (i, op) in ops.iter_mut().enumerate().take(len) {
                    *op = (bits >> i) & 1;
                }
                cases.push(Case {
                    base_present,
                    ops,
                    len,
                });
            }
        }
    }
    cases
}

/// Reference: fold the full event list (base modeled as an assert at
/// `BASE_T`) chronologically up to `target`. Returns presence and the `t`
/// at which the current presence run began.
fn reference(case: &Case, target: u32) -> (bool, Option<u32>) {
    let mut all: Vec<(u32, u8)> = Vec::new();
    if case.base_present {
        all.push((BASE_T, 1));
    }
    all.extend(case.events());

    let mut present = false;
    let mut run_start = None;
    for (t, op) in all {
        if t > target {
            break;
        }
        let asserts = op == 1;
        if asserts != present {
            present = asserts;
            run_start = asserts.then_some(t);
        }
    }
    (present, if present { run_start } else { None })
}

fn fact_v2(t: u32) -> RunRecordV2 {
    RunRecordV2 {
        s_id: SubjectId::from_u64(FACT_S),
        o_key: ObjKey::encode_i64(FACT_V).as_u64(),
        p_id: 1,
        t,
        o_i: LIST_INDEX_NONE,
        o_type: OType::XSD_INTEGER.as_u16(),
        g_id: 0,
    }
}

fn pad_v2() -> RunRecordV2 {
    RunRecordV2 {
        s_id: SubjectId::from_u64(PAD_S),
        o_key: ObjKey::encode_i64(50).as_u64(),
        p_id: 1,
        t: PAD_T,
        o_i: LIST_INDEX_NONE,
        o_type: OType::XSD_INTEGER.as_u16(),
        g_id: 0,
    }
}

fn fact_v1(t: u32, op: u8) -> RunRecord {
    RunRecord {
        s_id: SubjectId::from_u64(FACT_S),
        o_key: ObjKey::encode_i64(FACT_V).as_u64(),
        p_id: 1,
        t,
        i: LIST_INDEX_NONE,
        g_id: 0,
        dt: DatatypeDictId::INTEGER.as_u16(),
        lang_id: 0,
        o_kind: ObjKind::NUM_INT.as_u8(),
        op,
    }
}

/// Decoded view of one leaf: latest-state columns + full sidecar history.
struct LeafView {
    batch: ColumnBatch,
    history: Vec<HistEntryV2>,
}

impl LeafView {
    fn decode(leaf_bytes: &[u8], sidecar_bytes: Option<&[u8]>) -> Self {
        let handle =
            FullBlobLeafHandle::new(leaf_bytes.to_vec(), sidecar_bytes.map(<[u8]>::to_vec), 0)
                .expect("leaf handle");
        assert_eq!(handle.dir().entries.len(), 1, "cases fit one leaflet");
        let batch = handle
            .load_columns(0, &ColumnProjection::all(), RunSortOrder::Spot)
            .expect("load columns");
        let history = handle.load_sidecar_segment(0).expect("decode history");
        LeafView { batch, history }
    }

    /// Replay to `target` and report the fact's presence and row `t`.
    fn fact_at(&self, target: u32) -> (bool, Option<u32>) {
        let replayed = replay_leaflet(
            &self.batch,
            &self.history,
            target as i64,
            RunSortOrder::Spot,
        );
        let view = replayed.as_ref().unwrap_or(&self.batch);
        for i in 0..view.row_count {
            if view.s_id.get(i) == FACT_S {
                return (true, Some(view.t.get_or(i, 0)));
            }
        }
        (false, None)
    }
}

/// Build a leaf (+ sidecar) from a full event log via the rebuild pipeline.
/// Used directly by the rebuild property and as the base for the incremental
/// property, so bases carry proper transition history for their own facts.
fn build_leaf_via_rebuild(
    records: &[RunRecordV2],
    ops: &[u8],
    scratch_tag: &str,
) -> (Vec<u8>, Option<Vec<u8>>, LeafEntry) {
    let dir = std::env::temp_dir().join(format!("fluree_replay_prop_{scratch_tag}"));
    let _ = std::fs::remove_dir_all(&dir);
    let run_dir = dir.join("runs");
    let index_dir = dir.join("index");
    std::fs::create_dir_all(&run_dir).unwrap();

    write_run_file_with_op(
        &run_dir.join("run_00000.frn"),
        records,
        ops,
        RunSortOrder::Spot,
        PAD_T,
        *EVENT_TS.last().expect("event ts"),
    )
    .unwrap();

    let config = IndexBuildConfig {
        run_dir,
        index_dir,
        sort_order: RunSortOrder::Spot,
        leaflet_target_rows: 100,
        leaf_target_rows: 1000,
        zstd_level: 1,
        import_unique_asserts: false,
        skip_history: false,
        g_id: 0,
        progress: None,
        fan_in_cap: usize::MAX,
    };
    let result = build_index(&config).unwrap();
    let leaf = &result.graphs[0].leaf_infos[0];
    let leaf_bytes = std::fs::read(&leaf.leaf_path).unwrap();
    let sidecar_bytes = leaf
        .sidecar_path
        .as_ref()
        .map(|p| std::fs::read(p).unwrap());
    let entry = LeafEntry {
        first_key: leaf.first_key,
        last_key: leaf.last_key,
        row_count: leaf.total_rows,
        leaf_cid: leaf.leaf_cid.clone(),
        sidecar_cid: leaf.sidecar_cid.clone(),
    };
    let _ = std::fs::remove_dir_all(&dir);
    (leaf_bytes, sidecar_bytes, entry)
}

/// Drive one case through the real incremental pipeline: dedup → convert →
/// branch/leaf merge → sidecar encode. The base leaf is built by the rebuild
/// pipeline, so a base-present fact carries its assert transition in the
/// base sidecar (as any post-rework leaf would).
fn run_incremental(case: &Case, case_id: usize) -> (LeafView, Option<(u32, u8)>) {
    let mut base_records = vec![pad_v2()];
    let mut base_ops = vec![1u8];
    if case.base_present {
        base_records.push(fact_v2(BASE_T));
        base_ops.push(1);
    }
    let (base_leaf, base_sidecar, entry) =
        build_leaf_via_rebuild(&base_records, &base_ops, &format!("incr_base_{case_id}"));

    let mut leaf_store: HashMap<ContentId, Vec<u8>> = HashMap::new();
    let mut sidecar_store: HashMap<ContentId, Vec<u8>> = HashMap::new();
    leaf_store.insert(entry.leaf_cid.clone(), base_leaf);
    if let (Some(cid), Some(bytes)) = (entry.sidecar_cid.clone(), base_sidecar) {
        sidecar_store.insert(cid, bytes);
    }
    let branch_bytes = build_branch_bytes(RunSortOrder::Spot, 0, std::slice::from_ref(&entry));

    // The real dedup resolves the window's events to winners + superseded.
    let v1_events: Vec<RunRecord> = case
        .events()
        .iter()
        .map(|&(t, op)| fact_v1(t, op))
        .collect();
    let registry = OTypeRegistry::builtin_only();
    let (winners, superseded) = dedup_fact_lifecycles(v1_events, &registry);

    let to_v2 = |records: &[RunRecord]| -> (Vec<RunRecordV2>, Vec<u8>) {
        records
            .iter()
            .map(|v1| (RunRecordV2::from_v1(v1, &registry), v1.op))
            .unzip()
    };
    let (novelty, ops) = to_v2(&winners);
    let (sup, sup_ops) = to_v2(&superseded);
    let winner = winners.first().map(|w| (w.t, w.op));

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
        &novelty,
        &ops,
        &sup,
        &sup_ops,
        &config,
        &|cid| Ok(leaf_store.get(cid).expect("leaf bytes").clone()),
        &|cid| Ok(sidecar_store.get(cid).cloned()),
    )
    .expect("update branch");
    assert_eq!(result.new_leaf_blobs.len(), 1, "one merged leaf");
    let info = &result.new_leaf_blobs[0].info;
    let view = LeafView::decode(&info.leaf_bytes, info.sidecar_bytes.as_deref());
    (view, winner)
}

/// Drive one case through the real rebuild pipeline: run file → k-way merge
/// with lifecycle resolution → leaf + sidecar. The rebuild sees the full
/// log, so base-present is just an early assert.
fn run_rebuild(case: &Case, case_id: usize) -> LeafView {
    let mut records = vec![pad_v2()];
    let mut ops = vec![1u8];
    if case.base_present {
        records.push(fact_v2(BASE_T));
        ops.push(1);
    }
    for &(t, op) in &case.events() {
        records.push(fact_v2(t));
        ops.push(op);
    }
    let (leaf_bytes, sidecar_bytes, _entry) =
        build_leaf_via_rebuild(&records, &ops, &format!("rebuild_{case_id}"));
    LeafView::decode(&leaf_bytes, sidecar_bytes.as_deref())
}

/// Expected row `t` on the incremental path: the reference run-start, except
/// that an assert winner which was a no-op against a continuously-present
/// fact (a cross-boundary re-assert) replaces the row, so its own `t` is
/// visible from that point on. This window-granular visible-`t` semantics
/// is specified in `fluree_db_binary_index::format::transitions` ("Visible
/// `t` is window-granular"); this helper mirrors it for the exhaustive
/// check.
fn expected_incremental_t(case: &Case, target: u32, winner: Option<(u32, u8)>) -> Option<u32> {
    let (present, run_start) = reference(case, target);
    if !present {
        return None;
    }
    if let Some((winner_t, 1)) = winner {
        let replacement = reference(case, winner_t - 1).0;
        if replacement && target >= winner_t {
            return Some(winner_t);
        }
    }
    run_start
}

#[test]
fn incremental_replay_matches_reference() {
    for (case_id, case) in all_cases().into_iter().enumerate() {
        let (view, winner) = run_incremental(&case, case_id);
        for &target in &TARGETS {
            let (expect_present, _) = reference(&case, target);
            let (present, row_t) = view.fact_at(target);
            assert_eq!(
                present,
                expect_present,
                "presence mismatch: {} target={target} (got t={row_t:?})",
                case.describe()
            );
            assert_eq!(
                row_t,
                expected_incremental_t(&case, target, winner),
                "row t mismatch: {} target={target}",
                case.describe()
            );
        }
    }
}

#[test]
fn rebuild_replay_matches_reference() {
    for (case_id, case) in all_cases().into_iter().enumerate() {
        let view = run_rebuild(&case, case_id);
        for &target in &TARGETS {
            let (expect_present, expect_run_start) = reference(&case, target);
            let (present, row_t) = view.fact_at(target);
            assert_eq!(
                present,
                expect_present,
                "presence mismatch: {} target={target} (got t={row_t:?})",
                case.describe()
            );
            if expect_present {
                assert_eq!(
                    row_t,
                    expect_run_start,
                    "row t mismatch: {} target={target}",
                    case.describe()
                );
            }
        }
    }
}
