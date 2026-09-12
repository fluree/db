//! Dictionary novelty overlay for subjects and strings.
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
//! - `NS_OVERFLOW (0xFFFF)` uses dedicated scalar fields to avoid resizing
//!   per-namespace vectors to 65536 entries.
//! - `initialized` must be true before any commit on a non-genesis ledger.
//!   `ensure_initialized()` panics unconditionally (debug and release).

use std::sync::Arc;

use crate::ns_vec_bi_dict::{lookup_key, NsVecBiDict};
use crate::vec_bi_dict::VecBiDict;
use crate::{Flake, FlakeValue};

/// Namespace code reserved for overflow subjects (full IRI as suffix).
/// Never stored in watermark vectors; always treated as novel.
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
            initialized: false,
        }
    }

    /// Create with watermarks from a persisted index root.
    ///
    /// `subject_wm[i]` = max persisted `local_id` for namespace code `i`.
    /// `string_wm` = max persisted `string_id`.
    ///
    /// If the watermarks vector is long enough to include `NS_OVERFLOW`
    /// (index 0xFFFF), the overflow entry is extracted to a dedicated scalar
    /// and the vector is truncated.  In practice watermarks vectors are
    /// short (only non-zero namespace codes up to the max assigned code),
    /// so this branch is rarely taken.
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
            initialized: true,
        }
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
                .assign_or_lookup(flake.s.namespace_code, &flake.s.name);

            // Object references
            if let FlakeValue::Ref(ref sid) = flake.o {
                self.subjects
                    .assign_or_lookup(sid.namespace_code, &sid.name);
            }

            // String values
            match &flake.o {
                FlakeValue::String(s) | FlakeValue::Json(s) => {
                    self.strings.assign_or_lookup(s);
                }
                _ => {}
            }
        }
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
        let key = lookup_key(ns_code, suffix);
        if let Some(id) = self.find_by_key(&key) {
            return id;
        }
        self.inner.insert_new(ns_code, suffix, key)
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
        if let Some(id) = self.find_string(value) {
            return id;
        }
        self.inner.assign_or_lookup(value)
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
        assert_eq!(Arc::strong_count(&parent), 3);
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
            3,
            "only b's two references remain"
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
}
