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

use crate::ns_vec_bi_dict::NsVecBiDict;
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
            },
            strings: StringDictNovelty {
                inner: VecBiDict::new(string_wm + 1),
                watermark: string_wm,
            },
            initialized: true,
        }
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
/// single-HashMap reverse lookups. Arc-shared string storage.
#[derive(Clone, Debug, Default)]
pub struct SubjectDictNovelty {
    inner: NsVecBiDict,
}

impl SubjectDictNovelty {
    /// Look up or assign a sid64 for `(ns_code, suffix)`.
    ///
    /// If already present, returns the existing sid64.
    /// Otherwise allocates a new sid64 with the next local_id for this
    /// namespace.
    pub fn assign_or_lookup(&mut self, ns_code: u16, suffix: &str) -> u64 {
        self.inner.assign_or_lookup(ns_code, suffix)
    }

    /// Reverse lookup: find sid64 by `(ns_code, suffix)`.
    pub fn find_subject(&self, ns_code: u16, suffix: &str) -> Option<u64> {
        self.inner.find_subject(ns_code, suffix)
    }

    /// Forward lookup: resolve sid64 → `(ns_code, &suffix)`.
    pub fn resolve_subject(&self, sid64: u64) -> Option<(u16, &str)> {
        self.inner.resolve_subject(sid64)
    }

    /// Get the watermark (max persisted local_id) for a namespace code.
    ///
    /// Returns 0 for unknown/out-of-range namespace codes.
    pub fn watermark_for_ns(&self, ns_code: u16) -> u64 {
        self.inner.watermark_for_ns(ns_code)
    }

    /// Number of entries in the novelty layer.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True if no novel subjects have been registered.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
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
    /// Max persisted string_id from the last index build.
    watermark: u32,
}

impl Default for StringDictNovelty {
    fn default() -> Self {
        Self {
            inner: VecBiDict::new(1),
            watermark: 0,
        }
    }
}

impl StringDictNovelty {
    /// Look up or assign a string_id for `value`.
    pub fn assign_or_lookup(&mut self, value: &str) -> u32 {
        self.inner.assign_or_lookup(value)
    }

    /// Reverse lookup: find string_id by value.
    pub fn find_string(&self, value: &str) -> Option<u32> {
        self.inner.find(value)
    }

    /// Forward lookup: resolve string_id → value.
    pub fn resolve_string(&self, id: u32) -> Option<&str> {
        self.inner.resolve(id)
    }

    /// Get the watermark (max persisted string_id).
    pub fn watermark(&self) -> u32 {
        self.watermark
    }

    /// Number of entries in the novelty layer.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True if no novel strings have been registered.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
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
