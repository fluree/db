//! Regression test: advancing an index must not break novelty dictionary decode.
//!
//! If an index advances while there are still commits in novelty (t > index_t),
//! `LedgerState::apply_loaded_db` resets watermarks from the new root. It must
//! then re-populate `DictNovelty` from the remaining novelty flakes; otherwise
//! novelty-only subject/string IDs can’t be decoded later (e.g. "string id N not found").

use fluree_db_core::{Flake, FlakeValue, LedgerSnapshot, Sid};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::Novelty;

#[test]
fn apply_loaded_db_repopulates_dict_novelty_for_remaining_overlay_strings() {
    use fluree_db_core::IndexType;

    // Scenario:
    // - Index at t=1 with string watermark=1
    // - Commit t=2 adds string "a" => id=2
    // - Commit t=3 adds string "b" => id=3
    // - Apply a newer index snapshot at t=2 (watermark=2), leaving t=3 in novelty
    // - Must still resolve string id 3 from DictNovelty after applying the index.

    let mut snapshot = LedgerSnapshot::genesis("test:main");
    snapshot.t = 1;
    snapshot.string_watermark = 1;

    let mut state = LedgerState::new(snapshot, Novelty::new(1));
    let reverse_graph = state.snapshot.build_reverse_graph().unwrap_or_default();

    let s = Sid::new(0, "ex:s");
    let p = Sid::new(0, "ex:p");
    let dt = Sid::new(2, "string");

    let flakes_t2 = vec![Flake::new(
        s.clone(),
        p.clone(),
        FlakeValue::String("a".to_string()),
        dt.clone(),
        2,
        true,
        None,
    )];
    std::sync::Arc::make_mut(&mut state.dict_novelty).populate_from_flakes(&flakes_t2);
    std::sync::Arc::make_mut(&mut state.novelty)
        .apply_commit(flakes_t2, 2, &reverse_graph)
        .unwrap();

    let flakes_t3 = vec![Flake::new(
        s,
        p,
        FlakeValue::String("b".to_string()),
        dt,
        3,
        true,
        None,
    )];
    std::sync::Arc::make_mut(&mut state.dict_novelty).populate_from_flakes(&flakes_t3);
    std::sync::Arc::make_mut(&mut state.novelty)
        .apply_commit(flakes_t3, 3, &reverse_graph)
        .unwrap();

    assert_eq!(state.dict_novelty.strings.find_string("a"), Some(2));
    assert_eq!(state.dict_novelty.strings.find_string("b"), Some(3));
    assert_eq!(state.dict_novelty.strings.resolve_string(3), Some("b"));

    // Apply a newer index snapshot at t=2 (string watermark=2).
    let mut new_snapshot = LedgerSnapshot::genesis("test:main");
    new_snapshot.t = 2;
    new_snapshot.string_watermark = 2;
    state.apply_loaded_db(new_snapshot, None).unwrap();

    // Novelty at t<=2 cleared; t=3 remains active.
    assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 1);

    // With watermark=2, remaining novelty string must still resolve at id=3.
    assert_eq!(state.dict_novelty.strings.watermark(), 2);
    assert_eq!(state.dict_novelty.strings.resolve_string(3), Some("b"));
}

/// Installing an index trims the dictionary novelty in place when nothing
/// else holds it: the field is the only reference, so the retire mutates
/// that allocation rather than copying the whole dictionary under the
/// write lock. Observed by allocation identity.
#[test]
fn apply_loaded_db_trims_an_unshared_dict_novelty_in_place() {
    let mut snapshot = LedgerSnapshot::genesis("test:main");
    snapshot.t = 1;
    snapshot.string_watermark = 1;
    let mut state = LedgerState::new(snapshot, Novelty::new(1));
    let reverse_graph = state.snapshot.build_reverse_graph().unwrap_or_default();
    let (s, p, dt) = (
        Sid::new(0, "ex:s"),
        Sid::new(0, "ex:p"),
        Sid::new(2, "string"),
    );
    for (t, value) in [(2, "a"), (3, "b")] {
        let flakes = vec![Flake::new(
            s.clone(),
            p.clone(),
            FlakeValue::String(value.to_string()),
            dt.clone(),
            t,
            true,
            None,
        )];
        std::sync::Arc::make_mut(&mut state.dict_novelty).populate_from_flakes(&flakes);
        std::sync::Arc::make_mut(&mut state.novelty)
            .apply_commit(flakes, t, &reverse_graph)
            .unwrap();
    }
    assert_eq!(
        std::sync::Arc::strong_count(&state.dict_novelty),
        1,
        "nothing but the state holds the dictionary"
    );
    let before = std::sync::Arc::as_ptr(&state.dict_novelty);

    let mut new_snapshot = LedgerSnapshot::genesis("test:main");
    new_snapshot.t = 2;
    new_snapshot.string_watermark = 2;
    state.apply_loaded_db(new_snapshot, None).unwrap();

    assert_eq!(
        std::sync::Arc::as_ptr(&state.dict_novelty),
        before,
        "the dictionary was copied rather than trimmed in place"
    );
    assert_eq!(state.dict_novelty.strings.resolve_string(3), Some("b"));
}
