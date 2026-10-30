//! Which op holds when one fact is retracted and asserted at one `t`.
//!
//! A commit can hold a retraction and an assertion of one fact at one `t`.
//! Restating writes before the commit-staging fix wrote them for values whose
//! index key holds less than the term (a big integer's XSD subtype, a
//! `dateTime` past the microsecond), and merges and reverts written before
//! the net-change fold concatenated a range's flakes into one commit. The
//! commit records no order between the two ops, so the rule has to be fixed
//! somewhere, and it has to be one rule: every lane that resolves a fact's
//! ops (the full rebuild, the incremental merge, the query overlay, novelty's
//! fact state, the stats fold, the annotation arena) must agree, or an
//! answer changes with the path that served it.
//!
//! **The assertion holds.** Every pair written by a restating write meant to
//! keep the value, and the persisted orders already put the assertion last:
//! run records, the history sidecar's replay, and the annotation arena sort
//! `op` ascending within a `t`, so a walk in that order ends on the
//! assertion. [`SAME_T_WINNER`] names the rule; it is not a setting, since
//! the persisted orders agree with it.
//!
//! Lanes use one of two forms:
//! - a sort by [`op_order`], walked in ascending order, ends on the op that
//!   holds;
//! - a streaming fold keeps the current op and replaces it when a later op
//!   [`supersedes`] it.
//!
//! A commit built locally never holds such a pair (the transactor refuses
//! one); commits that arrive from elsewhere (push, clone, restore, import)
//! may, and resolve by this rule.

/// The op that holds when one fact is both retracted and asserted at one
/// `t`: the assertion (`op == true`).
pub const SAME_T_WINNER: bool = true;

/// Sort key for one fact's ops: by `t`, then the same-`t` winner last, so an
/// ascending walk ends on the op that holds.
#[inline]
pub fn op_order<T: Ord>(t: T, op: bool) -> (T, bool) {
    (t, op == SAME_T_WINNER)
}

/// Whether op `(t, op)` replaces `(cur_t, cur_op)` as one fact's current op:
/// a later `t`, or the same `t` and the winning op. Order-independent: a fold
/// over a fact's ops in any order lands on the same winner.
#[inline]
pub fn supersedes<T: Ord>((t, op): (T, bool), (cur_t, cur_op): (T, bool)) -> bool {
    op_order(t, op) > op_order(cur_t, cur_op)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_assertion_holds_at_one_t() {
        assert!(supersedes((5, true), (5, false)));
        assert!(!supersedes((5, false), (5, true)));
        assert!(op_order(5, false) < op_order(5, true));
    }

    #[test]
    fn a_later_t_holds_whatever_its_op() {
        assert!(supersedes((6, false), (5, true)));
        assert!(supersedes((6, true), (5, false)));
        assert!(!supersedes((4, true), (5, false)));
        assert!(!supersedes((4, false), (5, true)));
    }

    #[test]
    fn a_repeat_does_not_supersede() {
        assert!(!supersedes((5, true), (5, true)));
        assert!(!supersedes((5, false), (5, false)));
    }

    /// A fold in any order and a sort walked to its end agree.
    #[test]
    fn fold_and_sort_agree_in_every_order() {
        let ops = [(3u32, true), (5, false), (5, true), (4, false)];
        let mut sorted = ops;
        sorted.sort_by_key(|&(t, op)| op_order(t, op));
        let last = *sorted.last().unwrap();
        // Every rotation of the input folds to the same winner.
        for start in 0..ops.len() {
            let mut cur = ops[start];
            for i in 1..ops.len() {
                let next = ops[(start + i) % ops.len()];
                if supersedes(next, cur) {
                    cur = next;
                }
            }
            assert_eq!(cur, last);
        }
        assert_eq!(last, (5, true));
    }
}
