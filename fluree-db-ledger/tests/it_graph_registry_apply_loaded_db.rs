//! Regression test: advancing an index must not lose named-graph
//! registrations introduced by commits still in novelty.
//!
//! If an index advances while there are still commits in novelty
//! (t > index_t), `LedgerState::apply_loaded_db` replaces the snapshot with
//! one built from the new index root — whose graph registry only knows
//! graphs up to index_t. Graphs allocated by the remaining novelty commits
//! must be carried forward, and at their ORIGINAL `g_id`s: the remaining
//! novelty flakes are stored under those numeric ids, so a re-registration
//! that permuted ids would silently re-route every named-graph fact.
//!
//! Companion to `it_dict_novelty_apply_loaded_db.rs`, which pins the same
//! lifecycle moment for subject/string dictionary state. (Flagged by the
//! 2026-06 architecture audit as a suspected gap; the preservation logic
//! exists in `apply_loaded_db` — this test pins it.)

use fluree_db_core::{Flake, FlakeValue, IndexType, LedgerSnapshot, Sid};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::Novelty;
use std::sync::Arc;

const G2_IRI: &str = "http://example.org/graphs/g2";
const G3_IRI: &str = "http://example.org/graphs/g3";

#[test]
fn apply_loaded_db_preserves_novelty_graph_registrations_and_ids() {
    // Scenario:
    // - Index at t=1; registry knows only the ledger-default graphs.
    // - Commit t=2: default-graph flake (will be cleared by the index apply).
    // - Commit t=3: registers TWO named graphs (mirroring the commit
    //   envelope's graph_delta application) and writes a flake into each.
    // - Apply a newer index snapshot at t=2 whose root does NOT know either
    //   graph. The t=3 commit stays in novelty; both graphs must survive at
    //   their original g_ids.

    let mut snapshot = LedgerSnapshot::genesis("test:main");
    snapshot.t = 1;
    let mut state = LedgerState::new(snapshot, Novelty::new(1));

    let s = Sid::new(0, "ex:s");
    let p = Sid::new(0, "ex:p");
    let dt = Sid::new(2, "string");

    // t=2: ordinary default-graph commit.
    let reverse_graph = state.snapshot.build_reverse_graph().unwrap_or_default();
    let flakes_t2 = vec![Flake::new(
        s.clone(),
        p.clone(),
        FlakeValue::String("default-graph".to_string()),
        dt.clone(),
        2,
        true,
        None,
    )];
    Arc::make_mut(&mut state.novelty)
        .apply_commit(flakes_t2, 2, &reverse_graph)
        .unwrap();

    // t=3: register the named graphs (as apply_single_commit does via the
    // commit envelope's graph_delta) and commit one flake into each.
    let assigned = Arc::make_mut(&mut state.snapshot)
        .graph_registry
        .apply_delta([G2_IRI, G3_IRI]);
    assert_eq!(assigned.len(), 2, "both graphs newly registered");
    let g2_id = state
        .snapshot
        .graph_registry
        .graph_id_for_iri(G2_IRI)
        .expect("g2 registered");
    let g3_id = state
        .snapshot
        .graph_registry
        .graph_id_for_iri(G3_IRI)
        .expect("g3 registered");
    assert_ne!(g2_id, g3_id);

    let reverse_graph = state.snapshot.build_reverse_graph().expect("reverse graph");
    let g2_sid = state.snapshot.encode_iri(G2_IRI).expect("encode g2");
    let g3_sid = state.snapshot.encode_iri(G3_IRI).expect("encode g3");

    let mut f2 = Flake::new(
        s.clone(),
        p.clone(),
        FlakeValue::String("in-g2".to_string()),
        dt.clone(),
        3,
        true,
        None,
    );
    f2.g = Some(g2_sid);
    let mut f3 = Flake::new(
        s,
        p,
        FlakeValue::String("in-g3".to_string()),
        dt,
        3,
        true,
        None,
    );
    f3.g = Some(g3_sid);
    Arc::make_mut(&mut state.novelty)
        .apply_commit(vec![f2, f3], 3, &reverse_graph)
        .unwrap();

    // Apply a newer index snapshot at t=2. Its root-derived registry knows
    // only the ledger defaults — exactly what a real FIR6 root built before
    // the t=3 commit would carry.
    let mut new_snapshot = LedgerSnapshot::genesis("test:main");
    new_snapshot.t = 2;
    state.apply_loaded_db(new_snapshot, None).unwrap();

    // t<=2 novelty cleared; the two t=3 named-graph flakes remain.
    assert_eq!(
        state.novelty.iter_index(IndexType::Spot).count(),
        2,
        "t=3 named-graph flakes must remain in novelty"
    );

    // Both graphs survive the snapshot replacement AT THEIR ORIGINAL IDS.
    assert_eq!(
        state.snapshot.graph_registry.graph_id_for_iri(G2_IRI),
        Some(g2_id),
        "g2 must keep its pre-apply g_id (novelty rows are stored under it)"
    );
    assert_eq!(
        state.snapshot.graph_registry.graph_id_for_iri(G3_IRI),
        Some(g3_id),
        "g3 must keep its pre-apply g_id (novelty rows are stored under it)"
    );

    // And the routing map can still be rebuilt (every registered IRI encodes).
    let reverse_after = state.snapshot.build_reverse_graph().expect("reverse graph");
    assert!(reverse_after.values().any(|gid| *gid == g2_id));
    assert!(reverse_after.values().any(|gid| *gid == g3_id));
}
