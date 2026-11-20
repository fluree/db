//! What a full rebuild's retractions of big integers retract.
//!
//! A commit staged by value (an earlier release's, or one written against
//! such a release's index) retracts an integer too large for 64 bits under
//! every XSD subtype stored at its slot just before it, whatever subtype it
//! names: that index held them as one fact, read back as `xsd:integer`
//! ([`fluree_db_core::Commit::num_big_typing`]). A rebuild replays every
//! commit in order, so it keeps the terms of each big integer stored at each
//! slot as it goes, and adds a retraction of each other term to such a
//! commit's own. Novelty applies the same retractions alongside the commit
//! (`fluree_db_transact::by_value_retractions`).
//!
//! Only big integers are kept, so the table stays small.

use fluree_db_core::commit::codec::raw_reader::is_big_integer_lexical;
use fluree_db_core::commit::codec::{RawObject, RawOp};
use rustc_hash::FxHashMap;

/// A big integer's slot: graph, subject, predicate, list place, and the
/// value in canonical lexical form.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Slot {
    g: (u16, String),
    s: (u16, String),
    p: (u16, String),
    place: Option<(u32, i32)>,
    value: String,
}

/// A datatype, as a commit names it.
type Datatype = (u16, String);

/// A retraction of a big integer, owned, to emit again under another
/// datatype once its commit has been read.
#[derive(Clone, Debug)]
pub(crate) struct Retraction {
    slot: Slot,
    lexical: String,
}

impl Retraction {
    /// The op that retracts this retraction's value under `dt`.
    pub(crate) fn under<'a>(&'a self, dt: &'a Datatype) -> RawOp<'a> {
        RawOp {
            g_ns_code: self.slot.g.0,
            g_name: &self.slot.g.1,
            s_ns_code: self.slot.s.0,
            s_name: &self.slot.s.1,
            p_ns_code: self.slot.p.0,
            p_name: &self.slot.p.1,
            dt_ns_code: dt.0,
            dt_name: &dt.1,
            o: RawObject::BigIntStr(&self.lexical),
            op: false,
            lang: None,
            place: self.slot.place,
        }
    }
}

/// The terms of each big integer stored at each slot, through the last
/// commit read, and what the commit being read does to them.
#[derive(Default)]
pub(crate) struct BigIntegerTerms {
    stored: FxHashMap<Slot, Vec<Datatype>>,
    /// The current commit's ops on big integers, in order: retractions and
    /// assertions, each with its datatype.
    ops: Vec<(Slot, Datatype, bool)>,
    /// The current commit's retractions of big integers, when it was staged
    /// by value.
    by_value: Vec<Retraction>,
}

impl BigIntegerTerms {
    /// Note `op` of the commit being read, staged by value when `by_value`.
    /// Ops on anything but an integer too large for 64 bits are passed over.
    pub(crate) fn note(&mut self, op: &RawOp<'_>, by_value: bool) {
        let RawObject::BigIntStr(lexical) = op.o else {
            return;
        };
        if !is_big_integer_lexical(lexical) {
            return;
        }
        let value = match lexical.parse::<num_bigint::BigInt>() {
            Ok(v) => v.to_string(),
            Err(_) => return,
        };
        let slot = Slot {
            g: (op.g_ns_code, op.g_name.to_string()),
            s: (op.s_ns_code, op.s_name.to_string()),
            p: (op.p_ns_code, op.p_name.to_string()),
            place: op.place,
            value,
        };
        let dt = (op.dt_ns_code, op.dt_name.to_string());
        if by_value && !op.op {
            self.by_value.push(Retraction {
                slot: slot.clone(),
                lexical: lexical.to_string(),
            });
        }
        self.ops.push((slot, dt, op.op));
    }

    /// End the commit being read: the retractions it adds to its own, each
    /// with the datatype to retract its value under, then the terms stored
    /// after it. Each of its retractions staged by value retracts every term
    /// of its value stored before the commit, but one the commit names
    /// itself: one it retracts is not retracted twice, and on one `t` an
    /// assertion holds (`fluree_db_core::same_t`), so a term it asserts is
    /// not retracted at all. Assertions apply after retractions.
    pub(crate) fn end_commit(&mut self) -> Vec<(Retraction, Datatype)> {
        let mut added: Vec<(Retraction, Datatype)> = Vec::new();
        for r in std::mem::take(&mut self.by_value) {
            let Some(stored) = self.stored.get(&r.slot) else {
                continue;
            };
            for dt in stored.clone() {
                let named = |slot: &Slot, d: &Datatype| *slot == r.slot && *d == dt;
                if self.ops.iter().any(|(s, d, _)| named(s, d))
                    || added.iter().any(|(a, d)| named(&a.slot, d))
                {
                    continue;
                }
                added.push((r.clone(), dt));
            }
        }
        let ops = std::mem::take(&mut self.ops);
        let retractions = ops
            .iter()
            .filter(|(_, _, op)| !op)
            .map(|(s, d, _)| (s, d))
            .chain(added.iter().map(|(r, d)| (&r.slot, d)));
        for (slot, dt) in retractions {
            if let Some(stored) = self.stored.get_mut(slot) {
                stored.retain(|d| d != dt);
                if stored.is_empty() {
                    self.stored.remove(slot);
                }
            }
        }
        for (slot, dt, _) in ops.into_iter().filter(|(_, _, op)| *op) {
            let stored = self.stored.entry(slot).or_default();
            if !stored.contains(&dt) {
                stored.push(dt);
            }
        }
        added
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIG: &str = "18446744073709551615";

    fn op<'a>(dt: &'a str, op: bool, lexical: &'a str) -> RawOp<'a> {
        RawOp {
            g_ns_code: 0,
            g_name: "",
            s_ns_code: 100,
            s_name: "s",
            p_ns_code: 100,
            p_name: "p",
            dt_ns_code: 2,
            dt_name: dt,
            o: RawObject::BigIntStr(lexical),
            op,
            lang: None,
            place: None,
        }
    }

    fn names(added: &[(Retraction, Datatype)]) -> Vec<&str> {
        let mut n: Vec<&str> = added.iter().map(|(_, d)| d.1.as_str()).collect();
        n.sort_unstable();
        n
    }

    /// A retraction staged by value retracts every subtype of its value
    /// stored before its commit, the one it names or not; one staged
    /// declared retracts its own term alone.
    #[test]
    fn a_retraction_by_value_retracts_every_stored_subtype() {
        let mut terms = BigIntegerTerms::default();
        terms.note(&op("unsignedLong", true, BIG), false);
        terms.note(&op("nonNegativeInteger", true, BIG), false);
        assert!(terms.end_commit().is_empty());

        terms.note(&op("integer", false, BIG), true);
        let added = terms.end_commit();
        assert_eq!(names(&added), ["nonNegativeInteger", "unsignedLong"]);
        let (r, dt) = &added[0];
        let emitted = r.under(dt);
        assert!(!emitted.op);
        assert!(matches!(emitted.o, RawObject::BigIntStr(BIG)));

        // Nothing is stored now, so retracting it again adds nothing.
        terms.note(&op("integer", false, BIG), true);
        assert!(terms.end_commit().is_empty());

        terms.note(&op("unsignedLong", true, BIG), false);
        terms.note(&op("nonNegativeInteger", true, BIG), false);
        terms.end_commit();
        terms.note(&op("unsignedLong", false, BIG), false);
        assert!(
            terms.end_commit().is_empty(),
            "a declared retraction names one term"
        );
        terms.note(&op("unsignedLong", false, BIG), true);
        assert_eq!(names(&terms.end_commit()), ["nonNegativeInteger"]);
    }

    /// A term the commit retracts itself takes no second retraction, and one
    /// it asserts none: the assertion holds on the commit's `t` whatever
    /// order the commit stores its ops in.
    #[test]
    fn a_commit_keeps_what_it_asserts_and_retracts_each_term_once() {
        for assert_first in [false, true] {
            let mut terms = BigIntegerTerms::default();
            terms.note(&op("nonNegativeInteger", true, BIG), false);
            terms.note(&op("positiveInteger", true, BIG), false);
            terms.end_commit();
            let pair = [
                op("integer", false, BIG),
                op("positiveInteger", false, BIG),
                op("nonNegativeInteger", true, BIG),
            ];
            let mut ops: Vec<&RawOp<'_>> = pair.iter().collect();
            if assert_first {
                ops.reverse();
            }
            for o in ops {
                terms.note(o, true);
            }
            assert!(terms.end_commit().is_empty());
            // The assertion held: the next by-value retraction meets it.
            terms.note(&op("integer", false, BIG), true);
            assert_eq!(names(&terms.end_commit()), ["nonNegativeInteger"]);
        }
    }

    /// Only integers too large for 64 bits are kept, per value and slot.
    #[test]
    fn only_big_integers_are_kept() {
        let mut terms = BigIntegerTerms::default();
        terms.note(&op("unsignedLong", true, "7"), false);
        terms.note(&op("unsignedLong", true, BIG), false);
        terms.end_commit();
        terms.note(&op("integer", false, "7"), true);
        terms.note(&op("integer", false, "18446744073709551616"), true);
        assert!(terms.end_commit().is_empty());
    }
}
