//! State-transition resolution for history sidecars.
//!
//! The history sidecar (FHS1) is a **transition log**: `replay_leaflet`'s
//! undo-walk reads an assert entry as "absent immediately before this `t`"
//! and a retract entry as "present immediately before this `t`". Writers
//! must therefore record exactly the ops that change a fact's presence —
//! never no-op events (a re-assert of a present fact, a retract of an
//! absent one), which would invert replayed presence.
//!
//! The companion invariant is **row-assert conservation**: a fact's
//! materialized assert lives either as its base row or as a sidecar entry,
//! never both and never neither. Replay synthesizes an assert event from
//! the base row and history queries read rows directly, so recording the
//! row's own assert in the sidecar would duplicate the event — while
//! losing it when the row is later removed or replaced would erase the
//! fact's birth. [`resolve_transitions`] enforces the "never both" half by
//! returning the final assert as the row instead of keeping it as an
//! entry; writers that consume an existing row enforce the "never neither"
//! half by migrating its assert into the sidecar first.
//!
//! ## What the index promises about history
//!
//! Index-served history is **transition-grade**: it reports the state
//! changes a fact went through, not every commit event. A re-assert of an
//! already-present fact changes no state and leaves no trace here; the
//! commit log is the authoritative event record, and event-grade audit
//! ("show every user assertion") is served by replaying commits, not
//! sidecars.
//!
//! ## Visible `t` is window-granular
//!
//! A row's `t` is the time the fact most recently became present, as
//! observed at the granularity of indexing windows. A re-assert that
//! crosses an indexing boundary replaces the row at the re-assert's `t`
//! (matching what the live novelty overlay showed before indexing), while
//! events within one window — and a full rebuild, which sees the entire
//! log as one window — resolve to the earliest assert of the fact's final
//! presence run. A reindex may therefore change the reported `t` of a
//! re-asserted fact from the re-assert time to the birth time; presence at
//! every `t` is unaffected. This is by design: there is no window-free
//! answer, and the alternative alignments would either diverge from the
//! live view on every indexing cycle or re-open the rebuild/novelty
//! disagreement this module exists to close.

use super::run_record_v2::RunRecordV2;

/// Resolve one fact identity's chronological event sequence to its sidecar
/// transitions, in place, and return its materialized row.
///
/// `events` holds the identity's `(record, op)` events in any order (`op`:
/// 1 = assert, 0 = retract). They are sorted by `(t, op)` — same-`t` ties
/// walk retract-first, matching the same-`t` retract-wins resolution used
/// by lifecycle dedup and the query overlay — and folded from
/// `base_present`: each op that changes presence is a transition, ops that
/// repeat the current state are no-ops and vanish.
///
/// On return, `events` holds exactly the transitions that belong in the
/// sidecar. When the final state is asserted, the transition into it is
/// removed from `events` and returned instead: it is the materialized row,
/// which carries its own assert. `None` means no row (final state absent).
pub fn resolve_transitions(
    events: &mut Vec<(RunRecordV2, u8)>,
    base_present: bool,
) -> Option<RunRecordV2> {
    events.sort_unstable_by(|a, b| a.0.t.cmp(&b.0.t).then(a.1.cmp(&b.1)));
    let mut present = base_present;
    let mut write = 0;
    for read in 0..events.len() {
        let (rec, op) = events[read];
        let asserts = op == 1;
        if asserts == present {
            continue;
        }
        present = asserts;
        events[write] = (rec, op);
        write += 1;
    }
    events.truncate(write);
    match events.last() {
        Some(&(_, 1)) => {
            let (row, _) = events.pop().expect("just observed a final assert");
            Some(row)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::subject_id::SubjectId;

    fn ev(t: u32, op: u8) -> (RunRecordV2, u8) {
        (
            RunRecordV2 {
                s_id: SubjectId(1),
                o_key: 10,
                p_id: 1,
                t,
                o_i: u32::MAX,
                o_type: 3,
                g_id: 0,
            },
            op,
        )
    }

    fn ts(events: &[(RunRecordV2, u8)]) -> Vec<(u32, u8)> {
        events.iter().map(|&(r, op)| (r.t, op)).collect()
    }

    #[test]
    fn lone_assert_is_the_row() {
        let mut events = vec![ev(5, 1)];
        let row = resolve_transitions(&mut events, false);
        assert_eq!(row.map(|r| r.t), Some(5));
        assert!(events.is_empty());
    }

    #[test]
    fn full_lifecycle_keeps_prior_transitions() {
        let mut events = vec![ev(1, 1), ev(4, 0), ev(6, 1)];
        let row = resolve_transitions(&mut events, false);
        assert_eq!(row.map(|r| r.t), Some(6));
        assert_eq!(ts(&events), vec![(1, 1), (4, 0)]);
    }

    #[test]
    fn noop_reassert_vanishes() {
        let mut events = vec![ev(1, 1), ev(6, 1)];
        let row = resolve_transitions(&mut events, false);
        assert_eq!(row.map(|r| r.t), Some(1), "first assert is the row");
        assert!(events.is_empty());
    }

    #[test]
    fn retract_of_present_base_is_a_transition() {
        let mut events = vec![ev(5, 0)];
        let row = resolve_transitions(&mut events, true);
        assert!(row.is_none());
        assert_eq!(ts(&events), vec![(5, 0)]);
    }

    #[test]
    fn retract_of_absent_base_is_a_noop() {
        let mut events = vec![ev(5, 0)];
        let row = resolve_transitions(&mut events, false);
        assert!(row.is_none());
        assert!(events.is_empty());
    }

    #[test]
    fn repeat_retract_keeps_first() {
        let mut events = vec![ev(5, 0), ev(6, 0)];
        let row = resolve_transitions(&mut events, true);
        assert!(row.is_none());
        assert_eq!(ts(&events), vec![(5, 0)]);
    }

    #[test]
    fn plain_reassert_of_present_base_changes_nothing() {
        let mut events = vec![ev(6, 1)];
        let row = resolve_transitions(&mut events, true);
        assert!(row.is_none(), "no transition: the base row stands");
        assert!(events.is_empty());
    }

    #[test]
    fn out_of_order_events_resolve_chronologically() {
        let mut events = vec![ev(6, 0), ev(5, 1)];
        let row = resolve_transitions(&mut events, false);
        assert!(row.is_none());
        assert_eq!(ts(&events), vec![(5, 1), (6, 0)]);
    }

    /// Same-`t` pairs walk deterministically (retract first, so the assert
    /// lands final). One identity never actually reaches this layer with a
    /// same-`t` pair: lifecycle dedup resolves the tie retract-wins before
    /// the merge, and commits cannot emit both ops for one fact at one `t`.
    #[test]
    fn same_t_pair_walks_retract_first() {
        let mut events = vec![ev(5, 1), ev(5, 0)];
        let row = resolve_transitions(&mut events, true);
        assert_eq!(row.map(|r| r.t), Some(5));
        assert_eq!(ts(&events), vec![(5, 0)]);
    }
}
