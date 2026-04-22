//! Vec-backed bidirectional dictionary for dense sequential IDs.
//!
//! [`VecBiDict`] stores forward mappings in a `Vec<Arc<str>>` (O(1) index
//! lookup, zero hashing) and reverse mappings in a `HashMap<Arc<str>, Id>`
//! (single hash lookup). The `Arc<str>` is shared between both structures —
//! each string is allocated once and reference-counted.
//!
//! This replaces paired `HashMap<K,V>` + `HashMap<V,K>` patterns that
//! duplicate storage and require manual synchronization.

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use hashbrown::HashMap;

// ---------------------------------------------------------------------------
// DictId trait
// ---------------------------------------------------------------------------

/// Trait for dense sequential dictionary ID types.
///
/// Implementors must be cheaply convertible to/from `usize` for Vec indexing.
/// Implemented for `u16`, `u32`, and `u64`.
pub trait DictId: Copy + Eq + Hash + fmt::Debug + 'static {
    fn from_usize(v: usize) -> Self;
    fn to_usize(self) -> usize;
    fn checked_add(self, rhs: Self) -> Option<Self>;
    fn one() -> Self;
}

impl DictId for u16 {
    #[inline]
    fn from_usize(v: usize) -> Self {
        v as u16
    }
    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }
    #[inline]
    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }
    #[inline]
    fn one() -> Self {
        1
    }
}

impl DictId for u32 {
    #[inline]
    fn from_usize(v: usize) -> Self {
        v as u32
    }
    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }
    #[inline]
    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }
    #[inline]
    fn one() -> Self {
        1
    }
}

impl DictId for u64 {
    #[inline]
    fn from_usize(v: usize) -> Self {
        v as u64
    }
    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }
    #[inline]
    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }
    #[inline]
    fn one() -> Self {
        1
    }
}

// ---------------------------------------------------------------------------
// VecBiDict
// ---------------------------------------------------------------------------

/// Vec-backed bidirectional dictionary for dense sequential IDs.
///
/// - **Forward:** `Vec<Arc<str>>` indexed by `(id - base_id)`. O(1), no hashing.
/// - **Reverse:** `HashMap<Arc<str>, Id>` sharing `Arc` with the Vec. One hash lookup.
/// - **Insert-only** (no remove). IDs are monotonically assigned from `base_id`.
#[derive(Clone, Debug)]
pub struct VecBiDict<Id: DictId> {
    entries: Vec<Arc<str>>,
    reverse: HashMap<Arc<str>, Id>,
    base_id: Id,
    next_id: Id,
}

impl<Id: DictId> VecBiDict<Id> {
    /// Create an empty dictionary with IDs starting at `base_id`.
    pub fn new(base_id: Id) -> Self {
        Self {
            entries: Vec::new(),
            reverse: HashMap::new(),
            base_id,
            next_id: base_id,
        }
    }

    /// Reconstruct a dictionary from an ordered list of entries.
    ///
    /// Entry at index `i` gets ID = `base_id + i`. Directly populates the
    /// entries Vec and reverse HashMap without any path-dependent lookup logic.
    ///
    /// This is the safe way to seed a `VecBiDict` from persisted data (e.g.,
    /// an `IndexRoot`'s inline dictionary vectors) — it guarantees ID
    /// stability regardless of insertion order.
    pub fn from_ordered_vec(base_id: Id, entries: Vec<Arc<str>>) -> Self {
        let mut reverse = HashMap::with_capacity(entries.len());
        let mut next_id = base_id;
        for entry in &entries {
            reverse.insert(Arc::clone(entry), next_id);
            next_id = next_id
                .checked_add(Id::one())
                .expect("DictId overflow in from_ordered_vec");
        }
        Self {
            entries,
            reverse,
            base_id,
            next_id,
        }
    }

    /// Look up or assign an ID for `value`.
    ///
    /// If `value` is already present, returns the existing ID.
    /// Otherwise allocates the next sequential ID and inserts into both
    /// forward and reverse structures.
    pub fn assign_or_lookup(&mut self, value: &str) -> Id {
        if let Some(&id) = self.reverse.get(value) {
            return id;
        }

        let id = self.next_id;
        self.next_id = id
            .checked_add(Id::one())
            .expect("DictId overflow: too many entries");

        let interned: Arc<str> = Arc::from(value);
        self.entries.push(Arc::clone(&interned));
        self.reverse.insert(interned, id);

        id
    }

    /// Reverse lookup: find ID by value.
    #[inline]
    pub fn find(&self, value: &str) -> Option<Id> {
        self.reverse.get(value).copied()
    }

    /// Forward lookup: resolve ID to value. O(1) Vec index.
    #[inline]
    pub fn resolve(&self, id: Id) -> Option<&str> {
        let idx = id.to_usize().checked_sub(self.base_id.to_usize())?;
        self.entries.get(idx).map(|s| &**s)
    }

    /// The base offset (watermark + 1, or 1 for genesis).
    #[inline]
    pub fn base_id(&self) -> Id {
        self.base_id
    }

    /// Number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterator over `(Id, &str)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (Id, &str)> {
        let base = self.base_id.to_usize();
        self.entries
            .iter()
            .enumerate()
            .map(move |(i, s)| (Id::from_usize(base + i), &**s))
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_or_lookup_dedup() {
        let mut d = VecBiDict::<u32>::new(1);
        let id1 = d.assign_or_lookup("hello");
        let id2 = d.assign_or_lookup("hello");
        assert_eq!(id1, id2);
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn test_assign_or_lookup_sequential() {
        let mut d = VecBiDict::<u32>::new(1);
        let id1 = d.assign_or_lookup("aaa");
        let id2 = d.assign_or_lookup("bbb");
        let id3 = d.assign_or_lookup("ccc");
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_find_returns_none_for_missing() {
        let d = VecBiDict::<u32>::new(1);
        assert_eq!(d.find("missing"), None);
    }

    #[test]
    fn test_resolve_returns_none_for_out_of_range() {
        let d = VecBiDict::<u32>::new(1);
        assert_eq!(d.resolve(0), None);
        assert_eq!(d.resolve(1), None);
        assert_eq!(d.resolve(999), None);
    }

    #[test]
    fn test_resolve_forward_lookup() {
        let mut d = VecBiDict::<u32>::new(1);
        let id = d.assign_or_lookup("hello");
        assert_eq!(d.resolve(id), Some("hello"));
    }

    #[test]
    fn test_base_id_offset() {
        let mut d = VecBiDict::<u32>::new(501);
        let id1 = d.assign_or_lookup("first");
        let id2 = d.assign_or_lookup("second");
        assert_eq!(id1, 501);
        assert_eq!(id2, 502);
        assert_eq!(d.resolve(501), Some("first"));
        assert_eq!(d.resolve(502), Some("second"));
        assert_eq!(d.resolve(500), None); // below base
    }

    #[test]
    fn test_clone_preserves_entries() {
        let mut d = VecBiDict::<u32>::new(1);
        d.assign_or_lookup("a");
        d.assign_or_lookup("b");

        let d2 = d.clone();
        assert_eq!(d2.len(), 2);
        assert_eq!(d2.find("a"), Some(1));
        assert_eq!(d2.find("b"), Some(2));
        assert_eq!(d2.resolve(1), Some("a"));
        assert_eq!(d2.resolve(2), Some("b"));
    }

    #[test]
    fn test_iter() {
        let mut d = VecBiDict::<u32>::new(10);
        d.assign_or_lookup("x");
        d.assign_or_lookup("y");
        let pairs: Vec<_> = d.iter().collect();
        assert_eq!(pairs, vec![(10, "x"), (11, "y")]);
    }

    #[test]
    fn test_u16_id_type() {
        let mut d = VecBiDict::<u16>::new(1);
        let id = d.assign_or_lookup("lang-en");
        assert_eq!(id, 1u16);
        assert_eq!(d.resolve(1), Some("lang-en"));
    }

    #[test]
    fn test_u64_id_type() {
        let mut d = VecBiDict::<u64>::new(1);
        let id = d.assign_or_lookup("subject");
        assert_eq!(id, 1u64);
        assert_eq!(d.resolve(1), Some("subject"));
    }

    #[test]
    fn test_empty() {
        let d = VecBiDict::<u32>::new(1);
        assert!(d.is_empty());
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn test_from_ordered_vec() {
        let entries: Vec<Arc<str>> =
            vec![Arc::from("alpha"), Arc::from("beta"), Arc::from("gamma")];
        let d = VecBiDict::<u32>::from_ordered_vec(0, entries);

        assert_eq!(d.len(), 3);
        assert_eq!(d.base_id(), 0);

        // Forward lookups
        assert_eq!(d.resolve(0), Some("alpha"));
        assert_eq!(d.resolve(1), Some("beta"));
        assert_eq!(d.resolve(2), Some("gamma"));
        assert_eq!(d.resolve(3), None);

        // Reverse lookups
        assert_eq!(d.find("alpha"), Some(0));
        assert_eq!(d.find("beta"), Some(1));
        assert_eq!(d.find("gamma"), Some(2));
        assert_eq!(d.find("delta"), None);
    }

    #[test]
    fn test_from_ordered_vec_with_base_id() {
        let entries: Vec<Arc<str>> = vec![Arc::from("en"), Arc::from("fr")];
        let d = VecBiDict::<u16>::from_ordered_vec(1, entries);

        assert_eq!(d.resolve(1), Some("en"));
        assert_eq!(d.resolve(2), Some("fr"));
        assert_eq!(d.resolve(0), None); // below base_id
        assert_eq!(d.find("en"), Some(1));
        assert_eq!(d.find("fr"), Some(2));
    }

    #[test]
    fn test_from_ordered_vec_then_insert() {
        let entries: Vec<Arc<str>> = vec![Arc::from("a"), Arc::from("b")];
        let mut d = VecBiDict::<u32>::from_ordered_vec(0, entries);

        // Existing entry returns existing ID
        assert_eq!(d.assign_or_lookup("a"), 0);
        assert_eq!(d.assign_or_lookup("b"), 1);

        // New entry gets next sequential ID
        let c_id = d.assign_or_lookup("c");
        assert_eq!(c_id, 2);
        assert_eq!(d.len(), 3);
        assert_eq!(d.resolve(2), Some("c"));
    }

    #[test]
    fn test_from_ordered_vec_empty() {
        let d = VecBiDict::<u32>::from_ordered_vec(5, Vec::new());
        assert!(d.is_empty());
        assert_eq!(d.base_id(), 5);
    }
}
