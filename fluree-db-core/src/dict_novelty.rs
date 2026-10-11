//! Dictionary novelty overlay for subjects, strings and triple terms.
//!
//! `DictNovelty` is a LedgerState-scoped layer that tracks novel dictionary
//! entries (subjects and strings) introduced by commits since the last index
//! build. It persists across queries within a single `LedgerState`, eliminating
//! per-query re-discovery and enabling watermark-based forward lookups.
//!
//! # Lifecycle
//!
//! 1. **Index load** → create with `DictNovelty::with_watermarks(...)` from the
//!    persisted root's `subject_watermarks` / `string_watermark`.
//! 2. **Commit** → `Arc::make_mut` + `populate()` to register novel subjects/strings.
//! 3. **Query** → read-only: `find_subject`, `resolve_subject`, watermark routing.
//! 4. **Next index build** → discard and recreate with new watermarks.
//!
//! # Layers
//!
//! A read over uncommitted state (SHACL validation, a post-state policy
//! condition, a staged preview) needs the committed dictionary plus the
//! subjects and strings that transaction introduces. [`DictNovelty::layered_over`]
//! builds that as an empty delta over a shared `Arc` of the committed
//! dictionary: lookups probe the delta then fall through to the parent, the
//! delta allocates from the parent's frontier so its ids never collide with
//! the parent's, and the persisted watermarks are the parent's. Nothing is
//! copied, whatever the parent's size, and the parent is never mutated. The
//! layer's ids are view-local: commit re-derives its own from the canonical
//! dictionary.
//!
//! # Key invariants
//!
//! - Reverse lookup keys use the same compressed encoding as the persisted
//!   subject reverse tree: `[ns_code BE 2 bytes][suffix UTF-8 bytes]`.
//! - Watermark vector covers `0..max_ns_code+1`. `watermark_for_ns(code)`
//!   returns 0 for any code beyond the vector length.
//! - `NS_OVERFLOW` (0xFFFF) was meant to use dedicated scalar fields, but the
//!   real overflow namespace is `namespaces::OVERFLOW` (0xFFFE), which takes
//!   the ordinary per-namespace path, as in the indexer (#1843).
//! - `initialized` must be true before any commit on a non-genesis ledger.
//!   `ensure_initialized()` panics unconditionally (debug and release).

use std::sync::Arc;

use crate::ns_vec_bi_dict::{lookup_key, NsVecBiDict};
use crate::vec_bi_dict::VecBiDict;
use crate::{Flake, FlakeValue, TripleTermValue};
use std::collections::HashMap;

/// Does not match `namespaces::OVERFLOW` (0xFFFE); no production path assigns this
/// code, so the special case below is never taken (#1843).
const NS_OVERFLOW: u16 = 0xFFFF;

// ---------------------------------------------------------------------------
// Key encoding (shared with dict_tree reverse leaf format)
// ---------------------------------------------------------------------------

/// Encode a subject reverse key: `[ns_code BE 2 bytes][suffix UTF-8 bytes]`.
///
/// This matches the persisted subject reverse tree key format.
/// Returns `Box<[u8]>` for compact storage in `HashMap` keys.
#[inline]
pub fn subject_reverse_key(ns_code: u16, suffix: &str) -> Box<[u8]> {
    let mut key = Vec::with_capacity(2 + suffix.len());
    key.extend_from_slice(&ns_code.to_be_bytes());
    key.extend_from_slice(suffix.as_bytes());
    key.into_boxed_slice()
}

// ---------------------------------------------------------------------------
// DictNovelty
// ---------------------------------------------------------------------------

/// Persistent dictionary novelty layer for subjects and strings.
///
/// Populated during commit, read during queries, discarded at index build.
/// Uses watermark routing to partition persisted vs novel entries.
#[derive(Clone, Debug)]
pub struct DictNovelty {
    pub subjects: SubjectDictNovelty,
    pub strings: StringDictNovelty,
    pub terms: TermDictNovelty,
    initialized: bool,
}

impl DictNovelty {
    /// Create for a genesis ledger (no persisted index yet).
    ///
    /// All watermarks are 0 and `initialized` is true, meaning every
    /// subject/string encountered will be treated as novel.
    pub fn new_genesis() -> Self {
        Self {
            subjects: SubjectDictNovelty::default(),
            strings: StringDictNovelty::default(),
            terms: TermDictNovelty::default(),
            initialized: true,
        }
    }

    /// Create an uninitialized placeholder.
    ///
    /// Used when loading a ledger before the `BinaryIndexStore` is available.
    /// Watermarks must be set via [`with_watermarks`] before any commit.
    /// Query-path treats this as "novel layer empty" (safe fallthrough).
    pub fn new_uninitialized() -> Self {
        Self {
            subjects: SubjectDictNovelty::default(),
            strings: StringDictNovelty::default(),
            terms: TermDictNovelty::default(),
            initialized: false,
        }
    }

    /// Create with watermarks from a persisted index root.
    ///
    /// `subject_wm[i]` = max persisted `local_id` for namespace code `i`.
    /// `string_wm` = max persisted `string_id`.
    ///
    /// The `NS_OVERFLOW` extraction below is unreachable: the index root
    /// stores the watermark count as a `u16`, so index 0xFFFF never exists
    /// (#1843).
    pub fn with_watermarks(subject_wm: Vec<u64>, string_wm: u32) -> Self {
        // Extract overflow watermark if present, and trim vec.
        let overflow_idx = NS_OVERFLOW as usize;
        let (trimmed_wm, overflow_wm) = if subject_wm.len() > overflow_idx {
            let owm = subject_wm[overflow_idx];
            let mut v = subject_wm;
            v.truncate(overflow_idx);
            (v, owm)
        } else {
            (subject_wm, 0)
        };
        Self {
            subjects: SubjectDictNovelty {
                inner: NsVecBiDict::with_watermarks(trimmed_wm, overflow_wm),
                parent: None,
            },
            strings: StringDictNovelty {
                inner: VecBiDict::new(string_wm + 1),
                watermark: string_wm,
                parent: None,
            },
            terms: TermDictNovelty::default(),
            initialized: true,
        }
    }

    /// Drop every entry introduced at or before commit `t` and move the
    /// persisted watermarks up to a newer index root's — what an index
    /// publish covering `t` does to the dictionary novelty instead of
    /// rebuilding it from the remaining flakes.
    ///
    /// Sound because a flake that remains (t > index `t`) can only name an
    /// entry introduced at or before `t` if the index persisted it, and an
    /// entry introduced after `t` is not in the index. The kept entries are
    /// renumbered contiguously above the new watermarks, which is the only
    /// invariant their ids carry. `None` (and no change) on an
    /// uninitialized or layered dictionary. Returns `(subjects, strings)`
    /// dropped.
    pub fn retire_seen_through(
        &mut self,
        t: i64,
        subject_wm: &[u64],
        string_wm: u32,
    ) -> Option<(usize, usize)> {
        if !self.initialized || self.subjects.parent.is_some() || self.strings.parent.is_some() {
            return None;
        }
        let overflow_idx = NS_OVERFLOW as usize;
        let (trimmed_wm, overflow_wm) = if subject_wm.len() > overflow_idx {
            (&subject_wm[..overflow_idx], subject_wm[overflow_idx])
        } else {
            (subject_wm, 0)
        };
        let subjects = self
            .subjects
            .inner
            .retire_seen_through(t, trimmed_wm, overflow_wm);
        let strings = self.strings.inner.retire_seen_through(t, string_wm + 1);
        self.strings.watermark = string_wm;
        self.terms.retire_seen_through(t);
        Some((subjects, strings))
    }

    /// An empty delta over `parent` (see the module doc on layers).
    pub fn layered_over(parent: Arc<DictNovelty>) -> Self {
        Self {
            subjects: SubjectDictNovelty {
                inner: parent.subjects.inner.layer_above(),
                parent: Some(Arc::clone(&parent)),
            },
            strings: StringDictNovelty {
                inner: VecBiDict::new(parent.strings.inner.next_id()),
                watermark: parent.strings.watermark,
                parent: Some(Arc::clone(&parent)),
            },
            terms: TermDictNovelty {
                base: parent.terms.next_index(),
                parent: Some(Arc::clone(&parent)),
                ..TermDictNovelty::default()
            },
            initialized: parent.initialized,
        }
    }

    /// The dictionary this one is layered over, if any.
    pub fn parent(&self) -> Option<&Arc<DictNovelty>> {
        self.subjects.parent.as_ref()
    }

    /// Returns true if watermarks have been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Assert that watermarks are initialized.
    ///
    /// Called at the start of commit-path population. Panics unconditionally
    /// (debug and release) if watermarks have not been set from the index
    /// root, because committing with uninitialized watermarks can allocate
    /// novelty IDs that collide with persisted IDs.
    pub fn ensure_initialized(&self) {
        assert!(
            self.initialized,
            "DictNovelty: watermarks not initialized — set from index root before committing"
        );
    }

    /// Populate the novelty dictionaries from an iterator of flakes.
    ///
    /// Registers:
    /// - subjects (`flake.s`)
    /// - object refs (`FlakeValue::Ref`)
    /// - string-ish literals (`FlakeValue::String`, `FlakeValue::Json`)
    ///
    /// Panics if the dict is uninitialized (same as `ensure_initialized()`).
    pub fn populate_from_flakes_iter<'a, I>(&mut self, flakes: I)
    where
        I: IntoIterator<Item = &'a Flake>,
    {
        self.ensure_initialized();

        for flake in flakes {
            // Subject
            self.subjects
                .assign_or_lookup_at(flake.s.namespace_code, &flake.s.name, flake.t);

            // Object references
            if let FlakeValue::Ref(ref sid) = flake.o {
                self.subjects
                    .assign_or_lookup_at(sid.namespace_code, &sid.name, flake.t);
            }

            // String values
            match &flake.o {
                FlakeValue::String(s) | FlakeValue::Json(s) => {
                    self.strings.assign_or_lookup_at(s, flake.t);
                }
                FlakeValue::TripleTerm(term) => self.register_term(term, flake.t),
                _ => {}
            }
        }
    }

    /// Register a term's subject, object and the term itself; a nested term
    /// first, since the outer term's key names its handle.
    fn register_term(&mut self, term: &TripleTermValue, t: i64) {
        self.subjects
            .assign_or_lookup_at(term.s.namespace_code, &term.s.name, t);
        match &term.o {
            FlakeValue::Ref(sid) => {
                self.subjects
                    .assign_or_lookup_at(sid.namespace_code, &sid.name, t);
            }
            FlakeValue::String(s) | FlakeValue::Json(s) => {
                self.strings.assign_or_lookup_at(s, t);
            }
            FlakeValue::TripleTerm(inner) => self.register_term(inner, t),
            _ => {}
        }
        self.terms.assign_or_lookup_at(term, t);
    }

    /// Populate the novelty dictionaries from a slice of flakes.
    pub fn populate_from_flakes(&mut self, flakes: &[Flake]) {
        self.populate_from_flakes_iter(flakes);
    }
}

impl Default for DictNovelty {
    /// Default is uninitialized (same as `new_uninitialized()`).
    fn default() -> Self {
        Self::new_uninitialized()
    }
}

// ---------------------------------------------------------------------------
// SubjectDictNovelty
// ---------------------------------------------------------------------------

/// Subject dictionary novelty: `(ns_code, suffix)` ↔ `sid64`.
///
/// Backed by [`NsVecBiDict`]: Vec-indexed forward lookups (zero hashing),
/// single-HashMap reverse lookups. Arc-shared string storage. A layered
/// dictionary (see [`DictNovelty::layered_over`]) probes its own entries
/// first and falls through to `parent`.
#[derive(Clone, Debug, Default)]
pub struct SubjectDictNovelty {
    inner: NsVecBiDict,
    parent: Option<Arc<DictNovelty>>,
}

impl SubjectDictNovelty {
    /// Look up or assign a sid64 for `(ns_code, suffix)`.
    ///
    /// If already present here or in a parent, returns the existing sid64.
    /// Otherwise allocates a new sid64 with the next local_id for this
    /// namespace.
    pub fn assign_or_lookup(&mut self, ns_code: u16, suffix: &str) -> u64 {
        self.assign_or_lookup_at(ns_code, suffix, i64::MAX)
    }

    /// [`Self::assign_or_lookup`] recording the commit `t` that introduces
    /// a new entry, so an index publish covering `t` can retire it.
    pub fn assign_or_lookup_at(&mut self, ns_code: u16, suffix: &str, t: i64) -> u64 {
        let key = lookup_key(ns_code, suffix);
        if let Some(id) = self.find_by_key(&key) {
            // An entry of this layer keeps the earliest t it is seen at; a
            // parent's entry is the parent's to date.
            if self.inner.find_by_key(&key).is_some() {
                self.inner.note_seen_at(id, t);
            }
            return id;
        }
        self.inner.insert_new_at(ns_code, suffix, key, t)
    }

    /// Reverse lookup: find sid64 by `(ns_code, suffix)`.
    pub fn find_subject(&self, ns_code: u16, suffix: &str) -> Option<u64> {
        self.find_by_key(&lookup_key(ns_code, suffix))
    }

    /// Reverse lookup through the layer chain with the key encoded once.
    fn find_by_key(&self, key: &[u8]) -> Option<u64> {
        let mut dict = self;
        loop {
            if let Some(id) = dict.inner.find_by_key(key) {
                return Some(id);
            }
            dict = &dict.parent.as_ref()?.subjects;
        }
    }

    /// Forward lookup: resolve sid64 → `(ns_code, &suffix)`.
    pub fn resolve_subject(&self, sid64: u64) -> Option<(u16, &str)> {
        let mut dict = self;
        loop {
            if let Some(hit) = dict.inner.resolve_subject(sid64) {
                return Some(hit);
            }
            dict = &dict.parent.as_ref()?.subjects;
        }
    }

    /// Get the watermark (max persisted local_id) for a namespace code.
    ///
    /// Returns 0 for unknown/out-of-range namespace codes. A layer answers
    /// from its root: its own floor is the parent's allocation frontier, not
    /// a persisted boundary.
    pub fn watermark_for_ns(&self, ns_code: u16) -> u64 {
        self.root().inner.watermark_for_ns(ns_code)
    }

    fn root(&self) -> &SubjectDictNovelty {
        let mut dict = self;
        while let Some(parent) = dict.parent.as_ref() {
            dict = &parent.subjects;
        }
        dict
    }

    /// Number of entries in the novelty layer, parents included.
    pub fn len(&self) -> usize {
        self.inner.len() + self.parent.as_ref().map_or(0, |p| p.subjects.len())
    }

    /// True if no novel subjects have been registered here or in a parent.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate every novel `(ns_code, suffix)` entry, parents first.
    ///
    /// Query-time overlay translation reverse-looks-up exactly these entries
    /// against the persisted subject dictionary; residency-mode loads
    /// prefetch the reverse-tree leaves they will touch.
    pub fn iter_entries(&self) -> impl Iterator<Item = (u16, &str)> + '_ {
        self.chain_root_first()
            .into_iter()
            .flat_map(|dict| dict.inner.iter_entries())
    }

    /// This dictionary and its parents, root first.
    fn chain_root_first(&self) -> Vec<&SubjectDictNovelty> {
        let mut chain = vec![self];
        let mut dict = self;
        while let Some(parent) = dict.parent.as_ref() {
            dict = &parent.subjects;
            chain.push(dict);
        }
        chain.reverse();
        chain
    }
}

// ---------------------------------------------------------------------------
// StringDictNovelty
// ---------------------------------------------------------------------------

/// String dictionary novelty: value ↔ string_id (u32).
///
/// Backed by [`VecBiDict<u32>`]: Vec-indexed forward lookups (zero hashing),
/// single-HashMap reverse lookups. Arc-shared string storage.
#[derive(Clone, Debug)]
pub struct StringDictNovelty {
    inner: VecBiDict<u32>,
    /// Max persisted string_id from the last index build (a layer copies
    /// its parent's).
    watermark: u32,
    parent: Option<Arc<DictNovelty>>,
}

impl Default for StringDictNovelty {
    fn default() -> Self {
        Self {
            inner: VecBiDict::new(1),
            watermark: 0,
            parent: None,
        }
    }
}

impl StringDictNovelty {
    /// Look up or assign a string_id for `value`, here or in a parent.
    pub fn assign_or_lookup(&mut self, value: &str) -> u32 {
        self.assign_or_lookup_at(value, i64::MAX)
    }

    /// [`Self::assign_or_lookup`] recording the commit `t` that introduces
    /// a new entry, so an index publish covering `t` can retire it.
    pub fn assign_or_lookup_at(&mut self, value: &str, t: i64) -> u32 {
        if let Some(id) = self.find_string(value) {
            if self.inner.find(value).is_some() {
                // Keeps the earliest t this layer has seen the entry at.
                self.inner.assign_or_lookup_at(value, t);
            }
            return id;
        }
        self.inner.assign_or_lookup_at(value, t)
    }

    /// Reverse lookup: find string_id by value.
    pub fn find_string(&self, value: &str) -> Option<u32> {
        let mut dict = self;
        loop {
            if let Some(id) = dict.inner.find(value) {
                return Some(id);
            }
            dict = &dict.parent.as_ref()?.strings;
        }
    }

    /// Forward lookup: resolve string_id → value.
    pub fn resolve_string(&self, id: u32) -> Option<&str> {
        let mut dict = self;
        loop {
            if let Some(value) = dict.inner.resolve(id) {
                return Some(value);
            }
            dict = &dict.parent.as_ref()?.strings;
        }
    }

    /// Get the watermark (max persisted string_id).
    pub fn watermark(&self) -> u32 {
        self.watermark
    }

    /// Iterate every novel string value, parents first. Mirror of
    /// [`SubjectDictNovelty::iter_entries`] for the string dictionary.
    pub fn iter_values(&self) -> impl Iterator<Item = &str> + '_ {
        let mut chain = vec![self];
        let mut dict = self;
        while let Some(parent) = dict.parent.as_ref() {
            dict = &parent.strings;
            chain.push(dict);
        }
        chain.reverse();
        chain
            .into_iter()
            .flat_map(|dict| dict.inner.iter().map(|(_, s)| s))
    }

    /// Number of entries in the novelty layer, parents included.
    pub fn len(&self) -> usize {
        self.inner.len() + self.parent.as_ref().map_or(0, |p| p.strings.len())
    }

    /// True if no novel strings have been registered here or in a parent.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ===========================================================================
// Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// TermDictNovelty
// ---------------------------------------------------------------------------

/// Triple-term dictionary novelty: the terms novelty's links name, each with
/// an index that, under the term's inner predicate id, forms a provisional
/// handle (`triple_term::novelty_term_handle`). Readers try the persisted
/// dictionary first, so a term the index interned keeps its handle.
#[derive(Clone, Debug, Default)]
pub struct TermDictNovelty {
    /// `(term, earliest t seen)`, by index minus `base`.
    entries: Vec<(Arc<TripleTermValue>, i64)>,
    by_term: HashMap<Arc<TripleTermValue>, u32>,
    /// Index of this layer's first entry: a layer allocates above its parent.
    base: u32,
    parent: Option<Arc<DictNovelty>>,
}

impl TermDictNovelty {
    /// Look up or assign the index of `term`, here or in a parent.
    pub fn assign_or_lookup_at(&mut self, term: &TripleTermValue, t: i64) -> u32 {
        if let Some(&local) = self.by_term.get(term) {
            let entry = &mut self.entries[(local - self.base) as usize];
            entry.1 = entry.1.min(t);
            return local;
        }
        if let Some(index) = self.parent.as_ref().and_then(|p| p.terms.find(term)) {
            return index;
        }
        let index = self.next_index();
        let term = Arc::new(term.clone());
        self.by_term.insert(Arc::clone(&term), index);
        self.entries.push((term, t));
        index
    }

    /// The index of `term`, here or in a parent.
    pub fn find(&self, term: &TripleTermValue) -> Option<u32> {
        let mut dict = self;
        loop {
            if let Some(&index) = dict.by_term.get(term) {
                return Some(index);
            }
            dict = &dict.parent.as_ref()?.terms;
        }
    }

    /// The term at `index`, here or in a parent.
    pub fn resolve(&self, index: u32) -> Option<&TripleTermValue> {
        let mut dict = self;
        loop {
            if index >= dict.base {
                return dict
                    .entries
                    .get((index - dict.base) as usize)
                    .map(|(term, _)| term.as_ref());
            }
            dict = &dict.parent.as_ref()?.terms;
        }
    }

    /// Every term, parents first, with its index.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &TripleTermValue)> + '_ {
        let mut chain = vec![self];
        let mut dict = self;
        while let Some(parent) = dict.parent.as_ref() {
            dict = &parent.terms;
            chain.push(dict);
        }
        chain.reverse();
        chain.into_iter().flat_map(|dict| {
            dict.entries
                .iter()
                .enumerate()
                .map(move |(i, (term, _))| (dict.base + i as u32, term.as_ref()))
        })
    }

    fn next_index(&self) -> u32 {
        self.base + self.entries.len() as u32
    }

    /// Drop terms first seen at or before `t` (the index now interns them)
    /// and renumber the rest from zero. Returns the number dropped.
    fn retire_seen_through(&mut self, t: i64) -> usize {
        let before = self.entries.len();
        self.entries.retain(|(_, seen)| *seen > t);
        self.by_term = self
            .entries
            .iter()
            .enumerate()
            .map(|(i, (term, _))| (Arc::clone(term), i as u32))
            .collect();
        self.base = 0;
        before - self.entries.len()
    }

    /// Number of terms, parents included.
    pub fn len(&self) -> usize {
        self.entries.len() + self.parent.as_ref().map_or(0, |p| p.terms.len())
    }

    /// True when no term has been registered here or in a parent.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subject_id::SubjectId;

    // -----------------------------------------------------------------------
    // Key encoding
    // -----------------------------------------------------------------------

    #[test]
    fn test_subject_reverse_key_encoding() {
        let key = subject_reverse_key(2, "Alice");
        // ns_code 2 big-endian = [0x00, 0x02], then "Alice" bytes
        assert_eq!(&key[..2], &[0x00, 0x02]);
        assert_eq!(&key[2..], b"Alice");
    }

    #[test]
    fn test_subject_reverse_key_ordering() {
        let k1 = subject_reverse_key(2, "aaa");
        let k2 = subject_reverse_key(2, "bbb");
        let k3 = subject_reverse_key(3, "aaa");

        assert!(k1 < k2, "same ns, suffix sorts lexicographically");
        assert!(k2 < k3, "higher ns_code sorts after");
    }

    // -----------------------------------------------------------------------
    // DictNovelty constructors
    // -----------------------------------------------------------------------

    #[test]
    fn test_genesis() {
        let dn = DictNovelty::new_genesis();
        assert!(dn.is_initialized());
        assert!(dn.subjects.is_empty());
        assert!(dn.strings.is_empty());
    }

    #[test]
    fn test_uninitialized() {
        let dn = DictNovelty::new_uninitialized();
        assert!(!dn.is_initialized());
    }

    #[test]
    fn test_with_watermarks() {
        let dn = DictNovelty::with_watermarks(vec![10, 20, 30], 100);
        assert!(dn.is_initialized());
        assert_eq!(dn.subjects.watermark_for_ns(0), 10);
        assert_eq!(dn.subjects.watermark_for_ns(1), 20);
        assert_eq!(dn.subjects.watermark_for_ns(2), 30);
        assert_eq!(dn.subjects.watermark_for_ns(3), 0); // out of range
        assert_eq!(dn.subjects.watermark_for_ns(NS_OVERFLOW), 0); // always 0
        assert_eq!(dn.strings.watermark(), 100);
    }

    #[test]
    #[should_panic(expected = "watermarks not initialized")]
    fn test_ensure_initialized_panics() {
        let dn = DictNovelty::new_uninitialized();
        dn.ensure_initialized();
    }

    // -----------------------------------------------------------------------
    // SubjectDictNovelty
    // -----------------------------------------------------------------------

    #[test]
    fn test_subject_assign_and_lookup() {
        let mut dn = DictNovelty::new_genesis();

        let id1 = dn.subjects.assign_or_lookup(2, "Alice");
        let id2 = dn.subjects.assign_or_lookup(2, "Bob");
        let id3 = dn.subjects.assign_or_lookup(3, "Alice");

        // Same call returns same id
        assert_eq!(dn.subjects.assign_or_lookup(2, "Alice"), id1);

        // Different entries get different ids
        assert_ne!(id1, id2);
        assert_ne!(id1, id3);
        assert_ne!(id2, id3);

        // Verify namespace structure
        let s1 = SubjectId::from_u64(id1);
        let s2 = SubjectId::from_u64(id2);
        let s3 = SubjectId::from_u64(id3);

        assert_eq!(s1.ns_code(), 2);
        assert_eq!(s2.ns_code(), 2);
        assert_eq!(s3.ns_code(), 3);

        // local_ids within same namespace are sequential (starting at 1 for genesis)
        assert_eq!(s1.local_id(), 1);
        assert_eq!(s2.local_id(), 2);
        assert_eq!(s3.local_id(), 1);
    }

    #[test]
    fn test_subject_find() {
        let mut dn = DictNovelty::new_genesis();
        let id = dn.subjects.assign_or_lookup(5, "foo");

        assert_eq!(dn.subjects.find_subject(5, "foo"), Some(id));
        assert_eq!(dn.subjects.find_subject(5, "bar"), None);
        assert_eq!(dn.subjects.find_subject(6, "foo"), None);
    }

    #[test]
    fn test_subject_resolve() {
        let mut dn = DictNovelty::new_genesis();
        let id = dn.subjects.assign_or_lookup(2, "Alice");

        let (ns, suffix) = dn.subjects.resolve_subject(id).unwrap();
        assert_eq!(ns, 2);
        assert_eq!(suffix, "Alice");

        assert!(dn.subjects.resolve_subject(999).is_none());
    }

    #[test]
    fn test_subject_watermark_allocation() {
        // With watermarks, new IDs start above the watermark
        let mut dn = DictNovelty::with_watermarks(vec![0, 0, 100], 0);

        let id = dn.subjects.assign_or_lookup(2, "new_subject");
        let sid = SubjectId::from_u64(id);

        assert_eq!(sid.ns_code(), 2);
        assert_eq!(sid.local_id(), 101); // starts at watermark + 1
    }

    #[test]
    fn test_subject_novel_classification() {
        let dn = DictNovelty::with_watermarks(vec![0, 0, 100], 0);

        // local_id <= watermark → persisted
        let persisted = SubjectId::new(2, 50).as_u64();
        assert!(SubjectId::from_u64(persisted).local_id() <= dn.subjects.watermark_for_ns(2));

        // local_id > watermark → novel
        let novel = SubjectId::new(2, 101).as_u64();
        assert!(SubjectId::from_u64(novel).local_id() > dn.subjects.watermark_for_ns(2));
    }

    // -----------------------------------------------------------------------
    // StringDictNovelty
    // -----------------------------------------------------------------------

    #[test]
    fn test_string_assign_and_lookup() {
        let mut dn = DictNovelty::new_genesis();

        let id1 = dn.strings.assign_or_lookup("hello");
        let id2 = dn.strings.assign_or_lookup("world");

        // Same call returns same id
        assert_eq!(dn.strings.assign_or_lookup("hello"), id1);

        // Different values get different ids
        assert_ne!(id1, id2);

        // Sequential from watermark + 1
        assert_eq!(id1, 1); // genesis watermark = 0, starts at 1
        assert_eq!(id2, 2);
    }

    #[test]
    fn test_string_find() {
        let mut dn = DictNovelty::new_genesis();
        dn.strings.assign_or_lookup("hello");

        assert_eq!(dn.strings.find_string("hello"), Some(1));
        assert_eq!(dn.strings.find_string("missing"), None);
    }

    #[test]
    fn test_string_resolve() {
        let mut dn = DictNovelty::new_genesis();
        let id = dn.strings.assign_or_lookup("hello");

        assert_eq!(dn.strings.resolve_string(id), Some("hello"));
        assert_eq!(dn.strings.resolve_string(999), None);
    }

    #[test]
    fn test_string_watermark_allocation() {
        let mut dn = DictNovelty::with_watermarks(vec![], 500);

        let id = dn.strings.assign_or_lookup("new_value");
        assert_eq!(id, 501); // starts at watermark + 1
    }

    // -----------------------------------------------------------------------
    // NS_OVERFLOW handling
    // -----------------------------------------------------------------------

    #[test]
    fn test_overflow_assign_does_not_resize_vectors() {
        let mut dn = DictNovelty::new_genesis();

        // Assigning NS_OVERFLOW subjects must NOT resize watermarks/next_local_ids
        // to 65536 entries.
        let id = dn
            .subjects
            .assign_or_lookup(NS_OVERFLOW, "http://example.com/full-iri");
        let sid = SubjectId::from_u64(id);
        assert_eq!(sid.ns_code(), NS_OVERFLOW);
        assert_eq!(sid.local_id(), 1);

        // Regular namespace watermarks remain at 0 (overflow is separate)
        assert_eq!(dn.subjects.watermark_for_ns(0), 0);

        // Second overflow subject gets next local_id
        let id2 = dn
            .subjects
            .assign_or_lookup(NS_OVERFLOW, "http://other.com/iri");
        assert_eq!(SubjectId::from_u64(id2).local_id(), 2);

        // Dedup works
        assert_eq!(
            dn.subjects
                .assign_or_lookup(NS_OVERFLOW, "http://example.com/full-iri"),
            id
        );

        // find/resolve work
        assert_eq!(
            dn.subjects
                .find_subject(NS_OVERFLOW, "http://example.com/full-iri"),
            Some(id)
        );
        let (ns, suffix) = dn.subjects.resolve_subject(id).unwrap();
        assert_eq!(ns, NS_OVERFLOW);
        assert_eq!(suffix, "http://example.com/full-iri");
    }

    #[test]
    fn test_overflow_watermark_routing() {
        // With a persisted overflow watermark, new IDs start above it
        let subject_wm = vec![10, 20]; // ns 0 and 1
                                       // Simulate an overflow watermark being passed through the root
                                       // (in practice this would be a separate field, but with_watermarks
                                       // handles the extraction if the vec happens to be long enough)
        let dn = DictNovelty::with_watermarks(subject_wm.clone(), 0);
        assert_eq!(dn.subjects.watermark_for_ns(0), 10);
        assert_eq!(dn.subjects.watermark_for_ns(1), 20);
        assert_eq!(dn.subjects.watermark_for_ns(NS_OVERFLOW), 0); // no overflow wm set
    }

    // -----------------------------------------------------------------------
    // Layers
    // -----------------------------------------------------------------------

    fn committed_parent() -> Arc<DictNovelty> {
        let mut dn = DictNovelty::with_watermarks(vec![0, 0, 100], 500);
        dn.subjects.assign_or_lookup(2, "alice"); // 2:101
        dn.subjects.assign_or_lookup(2, "bob"); // 2:102
        dn.subjects.assign_or_lookup(NS_OVERFLOW, "http://full/iri"); // ovf:1
        dn.strings.assign_or_lookup("hello"); // 501
        Arc::new(dn)
    }

    #[test]
    fn layer_copies_nothing_and_falls_through_to_its_parent() {
        let parent = committed_parent();
        let layer = DictNovelty::layered_over(Arc::clone(&parent));
        assert!(Arc::ptr_eq(layer.parent().unwrap(), &parent));
        // One `Arc` per sub-dictionary; nothing is copied.
        assert_eq!(Arc::strong_count(&parent), 4);
        assert!(layer.is_initialized());

        let alice = parent.subjects.find_subject(2, "alice").unwrap();
        assert_eq!(layer.subjects.find_subject(2, "alice"), Some(alice));
        assert_eq!(layer.subjects.resolve_subject(alice), Some((2, "alice")));
        assert_eq!(
            layer.subjects.find_subject(NS_OVERFLOW, "http://full/iri"),
            parent.subjects.find_subject(NS_OVERFLOW, "http://full/iri")
        );
        assert_eq!(layer.strings.find_string("hello"), Some(501));
        assert_eq!(layer.strings.resolve_string(501), Some("hello"));
        assert_eq!(layer.subjects.len(), parent.subjects.len());
        assert_eq!(layer.strings.len(), parent.strings.len());
        assert!(!layer.subjects.is_empty());
    }

    #[test]
    fn layer_allocates_above_the_parent_and_never_re_mints_a_parent_entry() {
        let parent = committed_parent();
        let mut layer = DictNovelty::layered_over(Arc::clone(&parent));

        // Known to the parent: same id, nothing minted.
        let alice = parent.subjects.find_subject(2, "alice").unwrap();
        assert_eq!(layer.subjects.assign_or_lookup(2, "alice"), alice);
        assert_eq!(layer.strings.assign_or_lookup("hello"), 501);

        // Novel: allocated from the parent's frontier.
        let carol = layer.subjects.assign_or_lookup(2, "carol");
        assert_eq!(SubjectId::from_u64(carol).local_id(), 103);
        let ovf = layer
            .subjects
            .assign_or_lookup(NS_OVERFLOW, "http://other/iri");
        assert_eq!(SubjectId::from_u64(ovf).local_id(), 2);
        assert_eq!(layer.strings.assign_or_lookup("world"), 502);

        // Resolvable through the layer, absent from the parent.
        assert_eq!(layer.subjects.resolve_subject(carol), Some((2, "carol")));
        assert_eq!(layer.strings.resolve_string(502), Some("world"));
        assert_eq!(parent.subjects.find_subject(2, "carol"), None);
        assert_eq!(parent.subjects.resolve_subject(carol), None);
        assert_eq!(parent.strings.find_string("world"), None);
        assert_eq!(parent.subjects.len(), 3);
        assert_eq!(parent.strings.len(), 1);
        assert_eq!(layer.subjects.len(), 5);
        assert_eq!(layer.strings.len(), 2);
    }

    #[test]
    fn layer_reports_persisted_watermarks_not_the_parent_frontier() {
        let parent = committed_parent();
        let mut layer = DictNovelty::layered_over(Arc::clone(&parent));
        let carol = layer.subjects.assign_or_lookup(2, "carol");

        // Persisted boundary, unchanged by either dictionary's allocations.
        assert_eq!(layer.subjects.watermark_for_ns(2), 100);
        assert_eq!(layer.subjects.watermark_for_ns(NS_OVERFLOW), 0);
        assert_eq!(layer.strings.watermark(), 500);
        // Everything above it, in either layer, classifies as novel.
        assert!(SubjectId::from_u64(carol).local_id() > layer.subjects.watermark_for_ns(2));
        let alice = parent.subjects.find_subject(2, "alice").unwrap();
        assert!(SubjectId::from_u64(alice).local_id() > layer.subjects.watermark_for_ns(2));
    }

    #[test]
    fn sibling_layers_allocate_independently_and_do_not_see_each_other() {
        let parent = committed_parent();
        let mut a = DictNovelty::layered_over(Arc::clone(&parent));
        let mut b = DictNovelty::layered_over(Arc::clone(&parent));

        let a_id = a.subjects.assign_or_lookup(2, "from-a");
        let b_id = b.subjects.assign_or_lookup(2, "from-b");
        assert_eq!(
            a_id, b_id,
            "view-local ids may coincide; they never reach a committed state"
        );
        assert_eq!(a.subjects.resolve_subject(a_id), Some((2, "from-a")));
        assert_eq!(b.subjects.resolve_subject(b_id), Some((2, "from-b")));
        assert_eq!(a.subjects.find_subject(2, "from-b"), None);
        assert_eq!(b.subjects.find_subject(2, "from-a"), None);

        // Dropping a layer (an aborted staging) leaves the parent as it was.
        drop(a);
        assert_eq!(
            Arc::strong_count(&parent),
            4,
            "only b's three references remain"
        );
        assert_eq!(parent.subjects.len(), 3);
        assert_eq!(parent.subjects.find_subject(2, "from-a"), None);
    }

    #[test]
    fn layer_iteration_covers_parent_then_own_entries() {
        let parent = committed_parent();
        let mut layer = DictNovelty::layered_over(Arc::clone(&parent));
        layer.subjects.assign_or_lookup(3, "new");
        layer.strings.assign_or_lookup("world");

        let subjects: Vec<(u16, &str)> = layer.subjects.iter_entries().collect();
        assert_eq!(
            subjects,
            vec![
                (2, "alice"),
                (2, "bob"),
                (NS_OVERFLOW, "http://full/iri"),
                (3, "new")
            ]
        );
        let strings: Vec<&str> = layer.strings.iter_values().collect();
        assert_eq!(strings, vec!["hello", "world"]);
    }

    // -----------------------------------------------------------------------
    // Len / empty
    // -----------------------------------------------------------------------

    #[test]
    fn test_len_tracking() {
        let mut dn = DictNovelty::new_genesis();

        assert_eq!(dn.subjects.len(), 0);
        assert_eq!(dn.strings.len(), 0);
        assert!(dn.subjects.is_empty());
        assert!(dn.strings.is_empty());

        dn.subjects.assign_or_lookup(1, "a");
        dn.subjects.assign_or_lookup(1, "b");
        dn.strings.assign_or_lookup("x");

        assert_eq!(dn.subjects.len(), 2);
        assert_eq!(dn.strings.len(), 1);
        assert!(!dn.subjects.is_empty());
        assert!(!dn.strings.is_empty());
    }

    /// A publish covering `t` drops what commits through `t` introduced,
    /// renumbers the rest above the new watermarks, and refuses a layer.
    #[test]
    fn retire_seen_through_drops_by_commit() {
        let mut d = DictNovelty::with_watermarks(vec![0, 3], 2);
        d.subjects.assign_or_lookup_at(1, "t1-a", 1);
        d.subjects.assign_or_lookup_at(1, "t2-b", 2);
        d.strings.assign_or_lookup_at("t1-x", 1);
        d.strings.assign_or_lookup_at("t2-y", 2);

        let (subjects, strings) = d.retire_seen_through(1, &[0, 9], 7).unwrap();
        assert_eq!((subjects, strings), (1, 1));
        assert_eq!(d.subjects.find_subject(1, "t1-a"), None);
        let b = d.subjects.find_subject(1, "t2-b").unwrap();
        assert_eq!(SubjectId::from_u64(b).local_id(), 10);
        assert_eq!(d.subjects.resolve_subject(b), Some((1, "t2-b")));
        assert_eq!(d.strings.find_string("t1-x"), None);
        assert_eq!(d.strings.find_string("t2-y"), Some(8));
        assert_eq!(d.strings.resolve_string(8), Some("t2-y"));
        assert_eq!(d.strings.watermark(), 7);
        assert_eq!(d.subjects.watermark_for_ns(1), 9);

        let mut layer = DictNovelty::layered_over(Arc::new(d));
        assert!(
            layer.retire_seen_through(5, &[0, 9], 7).is_none(),
            "a layer never retires"
        );
    }

    fn term(s: &str, o: i64) -> TripleTermValue {
        TripleTermValue {
            s: crate::Sid::new(9, s),
            p: crate::Sid::new(9, "p"),
            o: FlakeValue::Long(o),
            dt: crate::Sid::new(2, "integer"),
            lang: None,
        }
    }

    /// Link flakes register their term and its components; a layer numbers
    /// above its parent and reads through it; a publish drops the terms it
    /// covered and renumbers the rest.
    #[test]
    fn terms_register_layer_and_retire() {
        let mut d = DictNovelty::with_watermarks(vec![], 0);
        let link = |t: i64, term: TripleTermValue| {
            Flake::new(
                crate::Sid::new(9, "r"),
                crate::namespaces::rdf_reifies_sid().clone(),
                FlakeValue::TripleTerm(Box::new(term)),
                crate::namespaces::triple_term_datatype_sid().clone(),
                t,
                true,
                None,
            )
        };
        d.populate_from_flakes(&[link(1, term("a", 1)), link(2, term("b", 2))]);
        assert_eq!(d.terms.find(&term("a", 1)), Some(0));
        assert_eq!(d.terms.find(&term("b", 2)), Some(1));
        assert!(d.subjects.find_subject(9, "a").is_some());

        let parent = Arc::new(d.clone());
        let mut layer = DictNovelty::layered_over(Arc::clone(&parent));
        assert_eq!(layer.terms.assign_or_lookup_at(&term("a", 1), 3), 0);
        assert_eq!(layer.terms.assign_or_lookup_at(&term("c", 3), 3), 2);
        assert_eq!(layer.terms.resolve(1), Some(&term("b", 2)));
        assert_eq!(layer.terms.resolve(2), Some(&term("c", 3)));
        assert_eq!(parent.terms.find(&term("c", 3)), None);
        assert_eq!(layer.terms.len(), 3);

        d.retire_seen_through(1, &[], 0).unwrap();
        assert_eq!(d.terms.find(&term("a", 1)), None);
        assert_eq!(d.terms.find(&term("b", 2)), Some(0));
        assert_eq!(d.terms.resolve(0), Some(&term("b", 2)));
    }

    /// A nested term registers before the term holding it, with its subject.
    #[test]
    fn nested_terms_register_inner_first() {
        let mut d = DictNovelty::with_watermarks(vec![], 0);
        let inner = term("inner", 1);
        let outer = TripleTermValue {
            s: crate::Sid::new(9, "outer"),
            p: crate::Sid::new(9, "p"),
            o: FlakeValue::TripleTerm(Box::new(inner.clone())),
            dt: crate::namespaces::triple_term_datatype_sid().clone(),
            lang: None,
        };
        d.populate_from_flakes(&[Flake::new(
            crate::Sid::new(9, "doc"),
            crate::Sid::new(9, "mentions"),
            FlakeValue::TripleTerm(Box::new(outer.clone())),
            crate::namespaces::triple_term_datatype_sid().clone(),
            1,
            true,
            None,
        )]);
        assert_eq!(d.terms.find(&inner), Some(0));
        assert_eq!(d.terms.find(&outer), Some(1));
        assert!(d.subjects.find_subject(9, "inner").is_some());
    }
}
