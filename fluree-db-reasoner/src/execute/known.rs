//! What rules join against: every fact the run knows.
//!
//! A full run holds them all in its [`DerivedSet`]. A run maintaining a
//! closed state holds only what it derives; the closed state is read on
//! demand ([`OnDemand`]). A lookup there answers from the reads already
//! fetched, combined with the live derivations. A lookup whose read is not
//! fetched yet records a miss: the work that made it is discarded and run
//! again once the read is in, so no answer is ever partial.

use std::cell::RefCell;

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;
use hashbrown::HashMap;

use super::derived::{DerivedSet, Indexed};

/// A read of the closed state: its facts with a predicate and subject, or
/// with a predicate and IRI object.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ReadKey {
    /// (predicate, subject)
    Ps(Sid, Sid),
    /// (predicate, object)
    Po(Sid, Sid),
}

impl ReadKey {
    /// Approximate heap and inline bytes of a pending read.
    pub fn approx_bytes(&self) -> usize {
        let (a, b) = match self {
            ReadKey::Ps(a, b) | ReadKey::Po(a, b) => (a, b),
        };
        std::mem::size_of::<Self>() + a.name.len() + b.name.len()
    }
}

struct Entry {
    facts: Vec<Flake>,
    bytes: usize,
    /// The round that fetched it; older entries are evicted first.
    round: usize,
}

/// Answers read from one closed state. It holds no derived fact: those
/// change every round, and a lookup combines them with these live.
#[derive(Default)]
pub struct ReadCache {
    entries: HashMap<ReadKey, Entry>,
    bytes: usize,
}

/// Bytes a cached answer is charged beyond its facts.
const ENTRY_OVERHEAD: usize = 96;

impl ReadCache {
    pub fn get(&self, key: &ReadKey) -> Option<&[Flake]> {
        self.entries.get(key).map(|e| e.facts.as_slice())
    }

    pub fn contains_key(&self, key: &ReadKey) -> bool {
        self.entries.contains_key(key)
    }

    pub fn insert(&mut self, key: ReadKey, facts: Vec<Flake>, round: usize) {
        let bytes = ENTRY_OVERHEAD
            + facts
                .iter()
                .map(crate::cache::approx_flake_bytes)
                .sum::<usize>();
        self.bytes += bytes;
        if let Some(old) = self.entries.insert(
            key,
            Entry {
                facts,
                bytes,
                round,
            },
        ) {
            self.bytes -= old.bytes;
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Evict the oldest answers until at most `capacity` bytes remain.
    pub fn evict_to(&mut self, capacity: usize) {
        if self.bytes <= capacity {
            return;
        }
        let mut by_age: Vec<(usize, ReadKey)> = self
            .entries
            .iter()
            .map(|(k, e)| (e.round, k.clone()))
            .collect();
        by_age.sort();
        for (_, key) in by_age {
            if self.bytes <= capacity {
                break;
            }
            if let Some(e) = self.entries.remove(&key) {
                self.bytes -= e.bytes;
            }
        }
    }
}

/// The closed state's fetched reads together with this run's derivations,
/// recording every read a lookup needed that is not fetched.
pub struct OnDemand<'a> {
    derived: &'a DerivedSet,
    reads: &'a ReadCache,
    misses: RefCell<Vec<ReadKey>>,
}

impl<'a> OnDemand<'a> {
    pub fn new(derived: &'a DerivedSet, reads: &'a ReadCache) -> Self {
        Self {
            derived,
            reads,
            misses: RefCell::new(Vec::new()),
        }
    }

    /// Whether a lookup missed: what was computed through this view is
    /// incomplete.
    pub fn missed(&self) -> bool {
        !self.misses.borrow().is_empty()
    }

    pub fn into_misses(self) -> Vec<ReadKey> {
        self.misses.into_inner()
    }

    fn read(&self, key: ReadKey) -> &'a [Flake] {
        match self.reads.get(&key) {
            Some(facts) => facts,
            None => {
                self.misses.borrow_mut().push(key);
                &[]
            }
        }
    }

    fn contains(&self, s: &Sid, p: &Sid, o: &FlakeValue) -> bool {
        if self.derived.contains(s, p, o) {
            return true;
        }
        let o = DerivedSet::object_hash(o);
        self.read(ReadKey::Ps(p.clone(), s.clone()))
            .iter()
            .any(|f| DerivedSet::object_hash(&f.o) == o)
    }
}

/// Every fact the run knows, as rules look it up.
#[derive(Clone, Copy)]
pub enum Known<'a> {
    /// All of it in memory.
    Resident(&'a DerivedSet),
    /// The closed state read on demand, with this run's derivations.
    OnDemand(&'a OnDemand<'a>),
}

impl<'a> Known<'a> {
    /// Whether the fact is known: derived this run, or already held.
    pub fn contains(&self, s: &Sid, p: &Sid, o: &FlakeValue) -> bool {
        match self {
            Known::Resident(derived) => derived.contains(s, p, o),
            Known::OnDemand(view) => view.contains(s, p, o),
        }
    }

    /// Known facts with predicate `p` and subject `s`.
    pub fn get_by_ps(&self, p: &Sid, s: &Sid) -> KnownIter<'a> {
        match self {
            Known::Resident(derived) => KnownIter {
                read: [].iter(),
                derived: derived.get_by_ps(p, s),
            },
            Known::OnDemand(view) => KnownIter {
                read: view.read(ReadKey::Ps(p.clone(), s.clone())).iter(),
                derived: view.derived.get_by_ps(p, s),
            },
        }
    }

    /// Known facts with predicate `p` and IRI object `o`.
    pub fn get_by_po(&self, p: &Sid, o: &Sid) -> KnownIter<'a> {
        match self {
            Known::Resident(derived) => KnownIter {
                read: [].iter(),
                derived: derived.get_by_po(p, o),
            },
            Known::OnDemand(view) => KnownIter {
                read: view.read(ReadKey::Po(p.clone(), o.clone())).iter(),
                derived: view.derived.get_by_po(p, o),
            },
        }
    }

    /// The run's in-memory facts, for budget checks.
    pub fn derived(&self) -> &'a DerivedSet {
        match self {
            Known::Resident(derived) => derived,
            Known::OnDemand(view) => view.derived,
        }
    }
}

/// A lookup's answer: facts read from the closed state, then facts derived
/// this run.
#[derive(Clone)]
pub struct KnownIter<'a> {
    read: std::slice::Iter<'a, Flake>,
    derived: Indexed<'a>,
}

impl<'a> Iterator for KnownIter<'a> {
    type Item = &'a Flake;

    fn next(&mut self) -> Option<&'a Flake> {
        self.read.next().or_else(|| self.derived.next())
    }
}
