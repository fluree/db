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
//! The rebuild keeps the table only through the last commit staged by value
//! ([`fluree_db_core::FirstParentWalk::last_by_value_t`]). It grows with the
//! big integers live at each slot, so a slot is kept as a digest and its
//! subtypes as ids: a few dozen bytes each.

use fluree_db_core::commit::codec::raw_reader::is_big_integer_lexical;
use fluree_db_core::commit::codec::{RawObject, RawOp};
use rustc_hash::{FxHashMap, FxHashSet};
use sha2::{Digest, Sha256};

/// A big integer's slot: graph, subject, predicate, list place, and the
/// value in canonical lexical form, as the first 16 bytes of their SHA-256.
type SlotKey = [u8; 16];

fn slot_key(op: &RawOp<'_>, value: &str) -> SlotKey {
    let mut h = Sha256::new();
    for (ns_code, name) in [
        (op.g_ns_code, op.g_name),
        (op.s_ns_code, op.s_name),
        (op.p_ns_code, op.p_name),
    ] {
        h.update(ns_code.to_le_bytes());
        h.update((name.len() as u64).to_le_bytes());
        h.update(name.as_bytes());
    }
    match op.place {
        None => h.update([0]),
        Some((list, index)) => {
            h.update([1]);
            h.update(list.to_le_bytes());
            h.update(index.to_le_bytes());
        }
    }
    h.update(value.as_bytes());
    let digest = h.finalize();
    let mut key = [0u8; 16];
    key.copy_from_slice(&digest[..16]);
    key
}

/// A datatype, as a commit names it.
type Datatype = (u16, String);

/// The datatypes a value is stored under at one slot, as indexes into
/// [`BigIntegerTerms::datatypes`]. Nearly every slot holds one.
#[derive(Clone, Debug)]
enum Datatypes {
    One(u32),
    Many(Vec<u32>),
}

impl Datatypes {
    fn ids(&self) -> &[u32] {
        match self {
            Self::One(dt) => std::slice::from_ref(dt),
            Self::Many(dts) => dts,
        }
    }

    fn insert(&mut self, dt: u32) {
        match self {
            Self::One(held) if *held != dt => *self = Self::Many(vec![*held, dt]),
            Self::Many(dts) if !dts.contains(&dt) => dts.push(dt),
            _ => {}
        }
    }

    /// Remove `dt`; whether any datatype is left.
    fn remove(&mut self, dt: u32) -> bool {
        match self {
            Self::One(held) => *held != dt,
            Self::Many(dts) => {
                dts.retain(|d| *d != dt);
                !dts.is_empty()
            }
        }
    }
}

/// A retraction of a big integer, owned, to emit again under another
/// datatype once its commit has been read.
#[derive(Clone, Debug)]
pub(crate) struct Retraction {
    key: SlotKey,
    g: (u16, String),
    s: (u16, String),
    p: (u16, String),
    place: Option<(u32, i32)>,
    lexical: String,
}

impl Retraction {
    /// The op that retracts this retraction's value under `dt`.
    pub(crate) fn under<'a>(&'a self, dt: &'a Datatype) -> RawOp<'a> {
        RawOp {
            g_ns_code: self.g.0,
            g_name: &self.g.1,
            s_ns_code: self.s.0,
            s_name: &self.s.1,
            p_ns_code: self.p.0,
            p_name: &self.p.1,
            dt_ns_code: dt.0,
            dt_name: &dt.1,
            o: RawObject::BigIntStr(&self.lexical),
            op: false,
            lang: None,
            place: self.place,
        }
    }
}

/// The terms of each big integer stored at each slot, through the last
/// commit read, and what the commit being read does to them.
#[derive(Default)]
pub(crate) struct BigIntegerTerms {
    /// Every datatype a big integer has been noted under, in order first
    /// seen.
    datatypes: Vec<Datatype>,
    stored: FxHashMap<SlotKey, Datatypes>,
    /// The current commit's ops on big integers, in order: retractions and
    /// assertions, each with its datatype.
    ops: Vec<(SlotKey, u32, bool)>,
    /// The current commit's retractions of big integers, when it was staged
    /// by value.
    by_value: Vec<Retraction>,
}

impl BigIntegerTerms {
    fn datatype_id(&mut self, ns_code: u16, name: &str) -> u32 {
        if let Some(at) = self
            .datatypes
            .iter()
            .position(|(n, d)| *n == ns_code && d == name)
        {
            return at as u32;
        }
        self.datatypes.push((ns_code, name.to_string()));
        (self.datatypes.len() - 1) as u32
    }

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
        let key = slot_key(op, &value);
        let dt = self.datatype_id(op.dt_ns_code, op.dt_name);
        if by_value && !op.op {
            self.by_value.push(Retraction {
                key,
                g: (op.g_ns_code, op.g_name.to_string()),
                s: (op.s_ns_code, op.s_name.to_string()),
                p: (op.p_ns_code, op.p_name.to_string()),
                place: op.place,
                lexical: lexical.to_string(),
            });
        }
        self.ops.push((key, dt, op.op));
    }

    /// End the commit being read: the retractions it adds to its own, each
    /// with the datatype to retract its value under, then the terms stored
    /// after it. Each of its retractions staged by value retracts every term
    /// of its value stored before the commit, but one the commit names
    /// itself: one it retracts is not retracted twice, and on one `t` an
    /// assertion holds (`fluree_db_core::same_t`), so a term it asserts is
    /// not retracted at all. Assertions apply after retractions.
    pub(crate) fn end_commit(&mut self) -> Vec<(Retraction, Datatype)> {
        let mut added: Vec<(Retraction, u32)> = Vec::new();
        let by_value = std::mem::take(&mut self.by_value);
        if !by_value.is_empty() {
            // Each term the commit names, and each added so far, once.
            let mut named: FxHashSet<(SlotKey, u32)> =
                self.ops.iter().map(|(key, dt, _)| (*key, *dt)).collect();
            for r in by_value {
                let Some(stored) = self.stored.get(&r.key) else {
                    continue;
                };
                for &dt in stored.ids() {
                    if named.insert((r.key, dt)) {
                        added.push((r.clone(), dt));
                    }
                }
            }
        }
        let ops = std::mem::take(&mut self.ops);
        let retractions = ops
            .iter()
            .filter(|(_, _, op)| !op)
            .map(|(k, d, _)| (*k, *d))
            .chain(added.iter().map(|(r, d)| (r.key, *d)));
        for (key, dt) in retractions {
            if let Some(stored) = self.stored.get_mut(&key) {
                if !stored.remove(dt) {
                    self.stored.remove(&key);
                }
            }
        }
        for (key, dt, _) in ops.into_iter().filter(|(_, _, op)| *op) {
            self.stored
                .entry(key)
                .and_modify(|stored| stored.insert(dt))
                .or_insert(Datatypes::One(dt));
        }
        added
            .into_iter()
            .map(|(r, dt)| (r, self.datatypes[dt as usize].clone()))
            .collect()
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

    /// A retraction by value retracts only its own slot's terms: another
    /// subject, predicate, list place or value keeps its own.
    #[test]
    fn a_retraction_by_value_keeps_to_its_slot() {
        let mut terms = BigIntegerTerms::default();
        let others = [
            RawOp {
                s_name: "t",
                ..op("unsignedLong", true, BIG)
            },
            RawOp {
                p_name: "q",
                ..op("unsignedLong", true, BIG)
            },
            RawOp {
                place: Some((0, 1)),
                ..op("unsignedLong", true, BIG)
            },
            op("unsignedLong", true, "18446744073709551616"),
        ];
        for other in &others {
            terms.note(other, false);
        }
        terms.end_commit();
        terms.note(&op("integer", false, BIG), true);
        assert!(terms.end_commit().is_empty());
    }

    /// A stored slot is a digest and its datatype ids, so the table holds a
    /// few dozen bytes per live big integer.
    #[test]
    fn a_stored_slot_is_a_digest_and_datatype_ids() {
        assert!(std::mem::size_of::<(SlotKey, Datatypes)>() <= 40);
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
