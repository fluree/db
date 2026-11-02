//! The facts of a default graph of several graphs, as a set.
//!
//! A fact is storage's identity: a row's bindings, normalized so a
//! late-materialized term and its decoded twin are one, plus the fact the row's
//! scan says it matched ([`RowFact`]: a constant object's matched term, and the
//! list position). Every reader that collapses a union's rows keys on
//! [`UnionFactSet::insert`], so they cannot disagree about what one fact is:
//! the general plan's dedup (`DatasetOperator`) and the nested-loop join's union
//! probes.
//!
//! The set keeps every fact's key in one buffer of 16-byte [`KeyTerm`]s, so a
//! kept row costs no allocation of its own: an encoded term is its ids, and a
//! term with no encoded form (a decoded value, an arena literal keyed by value)
//! is kept whole beside the buffer and named by its index there.

use crate::binding::Binding;
use crate::group_aggregate::Undecodable;
use crate::object_binding::{normalize_for_key_cow, TermDicts};
use crate::operator::RowFact;
use fluree_db_binary_index::TermView;
use hashbrown::HashTable;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

/// One term of a fact's key: an encoded store-wide term by its ids, the two
/// states with no term, or the index of a binding kept whole. Two key terms
/// are one term exactly when the bindings they stand for are equal: the ids
/// are the fields `Binding`'s equality reads.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum KeyTerm {
    /// The row's scan recorded no fact.
    NoFact,
    /// The fact has no constant object.
    NoObject,
    Unbound,
    Poisoned,
    Sid(u64),
    Pid(u32),
    /// A store-wide encoded literal: its kind, key, datatype and language.
    Lit {
        kind: u8,
        dt: u16,
        lang: u16,
        key: u64,
    },
    /// A binding kept whole, by its index among the set's kept bindings.
    Kept(u32),
}

const _: () = assert!(std::mem::size_of::<KeyTerm>() == 16);

/// What a row's scan recorded ([`RowFact`]), borrowed.
#[derive(Clone, Copy)]
pub(crate) struct FactRef<'a> {
    pub(crate) object: Option<&'a Binding>,
    pub(crate) list_index: Option<u32>,
}

impl<'a> From<&'a RowFact> for FactRef<'a> {
    fn from(f: &'a RowFact) -> Self {
        Self {
            object: f.object.as_ref(),
            list_index: f.list_index,
        }
    }
}

/// A fact in the set: its index (its key starts at `index * width` in the
/// buffer), its list position (`u32::MAX` for none) and the hash it was
/// stored under.
#[derive(Clone, Copy)]
struct FactEntry {
    hash: u64,
    index: u32,
    list: u32,
}

/// A set of a union's facts, each `width` bindings wide plus its fact.
pub(crate) struct UnionFactSet {
    terms: Vec<KeyTerm>,
    kept: Vec<Binding>,
    table: HashTable<FactEntry>,
    /// Key terms per fact: the bindings, then the fact's object.
    width: usize,
}

impl UnionFactSet {
    /// A set of facts of `bindings` bindings each.
    pub(crate) fn new(bindings: usize) -> Self {
        Self {
            terms: Vec::new(),
            kept: Vec::new(),
            table: HashTable::new(),
            width: bindings + 1,
        }
    }

    /// Insert the fact a row with `bindings` matched, `fact` being what its
    /// scan recorded, normalized through `dicts` and `gv`. `true` when the set
    /// did not hold it.
    #[inline]
    pub(crate) fn insert<'b>(
        &mut self,
        bindings: impl IntoIterator<Item = &'b Binding>,
        fact: Option<FactRef<'_>>,
        dicts: Option<TermDicts<'_>>,
        gv: Option<&TermView>,
    ) -> Result<bool, Undecodable> {
        let start = self.terms.len();
        let kept_start = self.kept.len();
        for binding in bindings {
            let term = self.term_of(binding, dicts, gv);
            self.push_term(term, start, kept_start)?;
        }
        let (object, list) = match fact {
            None => (Ok(KeyTerm::NoFact), u32::MAX),
            Some(f) => (
                match f.object {
                    None => Ok(KeyTerm::NoObject),
                    Some(o) => self.term_of(o, dicts, gv),
                },
                f.list_index.unwrap_or(u32::MAX),
            ),
        };
        self.push_term(object, start, kept_start)?;
        debug_assert_eq!(
            self.terms.len() - start,
            self.width,
            "a union fact of another width"
        );

        let hash = self.hash_of(start, list);
        let (terms, kept, width) = (&self.terms, &self.kept, self.width);
        let found = self.table.find(hash, |e| {
            e.list == list && same_key(terms, kept, e.index as usize * width, start, width)
        });
        if found.is_some() {
            self.truncate(start, kept_start);
            return Ok(false);
        }
        let index = u32::try_from(self.table.len()).expect("a union of more than u32::MAX facts");
        self.table
            .insert_unique(hash, FactEntry { hash, index, list }, |e| e.hash);
        Ok(true)
    }

    /// How many facts the set holds.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.table.len()
    }

    /// The bindings each fact of the set has.
    pub(crate) fn bindings(&self) -> usize {
        self.width - 1
    }

    #[inline]
    fn push_term(
        &mut self,
        term: Result<KeyTerm, Undecodable>,
        start: usize,
        kept_start: usize,
    ) -> Result<(), Undecodable> {
        match term {
            Ok(t) => {
                self.terms.push(t);
                Ok(())
            }
            Err(e) => {
                self.truncate(start, kept_start);
                Err(e)
            }
        }
    }

    fn truncate(&mut self, start: usize, kept_start: usize) {
        self.terms.truncate(start);
        self.kept.truncate(kept_start);
    }

    /// `binding`'s key term, keeping it whole when it has no encoded form.
    #[inline]
    fn term_of(
        &mut self,
        binding: &Binding,
        dicts: Option<TermDicts<'_>>,
        gv: Option<&TermView>,
    ) -> Result<KeyTerm, Undecodable> {
        let key = normalize_for_key_cow(binding, dicts, gv)?;
        Ok(match key.as_ref() {
            Binding::Unbound => KeyTerm::Unbound,
            Binding::Poisoned => KeyTerm::Poisoned,
            Binding::EncodedSid { s_id, .. } => KeyTerm::Sid(*s_id),
            Binding::EncodedPid { p_id } => KeyTerm::Pid(*p_id),
            // Normalization keys an arena literal by its value, so an
            // encoded literal here is a store-wide one.
            Binding::EncodedLit {
                o_kind,
                o_key,
                dt_id,
                lang_id,
                ..
            } if !crate::binding::is_arena_kind(*o_kind) => KeyTerm::Lit {
                kind: *o_kind,
                dt: *dt_id,
                lang: *lang_id,
                key: *o_key,
            },
            _ => {
                let index = u32::try_from(self.kept.len())
                    .expect("a union keeping more than u32::MAX terms");
                self.kept.push(key.into_owned());
                KeyTerm::Kept(index)
            }
        })
    }

    fn hash_of(&self, start: usize, list: u32) -> u64 {
        let mut h = FxHasher::default();
        list.hash(&mut h);
        for term in &self.terms[start..] {
            match term {
                KeyTerm::Kept(i) => {
                    u8::MAX.hash(&mut h);
                    self.kept[*i as usize].hash(&mut h);
                }
                other => other.hash(&mut h),
            }
        }
        h.finish()
    }
}

/// Whether the keys at `a` and `b` are one fact's: term by term, kept bindings
/// by `Binding`'s equality.
#[inline]
fn same_key(terms: &[KeyTerm], kept: &[Binding], a: usize, b: usize, width: usize) -> bool {
    terms[a..a + width]
        .iter()
        .zip(&terms[b..b + width])
        .all(|(x, y)| match (x, y) {
            (KeyTerm::Kept(i), KeyTerm::Kept(j)) => kept[*i as usize] == kept[*j as usize],
            _ => x == y,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn lit(o_key: u64) -> Binding {
        Binding::EncodedLit {
            o_kind: fluree_db_core::ObjKind::NUM_INT.as_u8(),
            o_key,
            p_id: 1,
            dt_id: 3,
            lang_id: 0,
            i_val: i32::MIN,
            t: 0,
            arena_g_id: fluree_db_core::NO_ARENA_GRAPH,
        }
    }

    fn fact(list: Option<u32>) -> FactRef<'static> {
        FactRef {
            object: None,
            list_index: list,
        }
    }

    #[test]
    fn one_fact_is_kept_once() {
        let mut set = UnionFactSet::new(2);
        let s = Binding::EncodedSid {
            s_id: 7,
            t: None,
            op: None,
        };
        assert!(set
            .insert([&s, &lit(1)], Some(fact(None)), None, None)
            .unwrap());
        assert!(!set
            .insert([&s, &lit(1)], Some(fact(None)), None, None)
            .unwrap());
        // A list position, another object, or no recorded fact is another fact.
        assert!(set
            .insert([&s, &lit(1)], Some(fact(Some(0))), None, None)
            .unwrap());
        assert!(set
            .insert([&s, &lit(2)], Some(fact(None)), None, None)
            .unwrap());
        assert!(set.insert([&s, &lit(1)], None, None, None).unwrap());
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn every_fact_is_found_where_its_key_was_written() {
        let mut set = UnionFactSet::new(2);
        let facts: Vec<[Binding; 2]> = (0..100u64)
            .map(|i| {
                let s = Binding::EncodedSid {
                    s_id: i,
                    t: None,
                    op: None,
                };
                [s, lit(i % 7)]
            })
            .collect();
        for [s, o] in &facts {
            assert!(set.insert([s, o], Some(fact(None)), None, None).unwrap());
        }
        for [s, o] in &facts {
            assert!(!set.insert([s, o], Some(fact(None)), None, None).unwrap());
        }
        assert_eq!(set.len(), 100);
    }

    #[test]
    fn decoded_terms_compare_as_bindings() {
        let mut set = UnionFactSet::new(1);
        let a = Binding::lit(FlakeValue::Long(5), Sid::new(2, "long"));
        let b = Binding::lit(FlakeValue::Long(5), Sid::new(2, "long"));
        let c = Binding::lit(FlakeValue::Long(6), Sid::new(2, "long"));
        assert!(set.insert([&a], None, None, None).unwrap());
        assert!(!set.insert([&b], None, None, None).unwrap());
        assert!(set.insert([&c], None, None, None).unwrap());
        // A duplicate's kept binding is released: two kept for two facts.
        assert_eq!(set.kept.len(), 2);
    }

    #[test]
    fn an_arena_literal_with_no_view_is_refused_and_leaves_nothing() {
        let mut set = UnionFactSet::new(2);
        let big = Binding::EncodedLit {
            o_kind: fluree_db_core::ObjKind::NUM_BIG.as_u8(),
            o_key: 0,
            p_id: 1,
            dt_id: 0,
            lang_id: 0,
            i_val: i32::MIN,
            t: 0,
            arena_g_id: 2,
        };
        assert!(set.insert([&lit(1), &big], None, None, None).is_err());
        assert!(set.terms.is_empty() && set.kept.is_empty());
    }
}
