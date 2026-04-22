//! Read-side global dictionary types: PredicateDict and LanguageTagDict.
//!
//! These are lightweight bidirectional dictionary wrappers for predicate IRIs,
//! graph IRIs, datatype IRIs, and language tags. Used by the binary index
//! store to map between numeric IDs and string values.
//!
//! Backed by `VecBiDict` from `fluree-db-core` for O(1) forward lookups
//! and HashMap-backed reverse lookups.

use fluree_db_core::vec_bi_dict::VecBiDict;
use std::sync::Arc;

// ============================================================================
// PredicateDict (also used for graph dicts, datatype dicts)
// ============================================================================

/// Bidirectional string ↔ u32 dictionary for predicates, graphs, and datatypes.
///
/// Forward (id → string): O(1) Vec index. Reverse (string → id): HashMap lookup.
/// Arc<str> shared between both — no string duplication.
/// Appropriate for small cardinality (< 10K).
pub struct PredicateDict {
    inner: VecBiDict<u32>,
}

impl PredicateDict {
    pub fn new() -> Self {
        Self {
            inner: VecBiDict::new(0),
        }
    }

    /// Look up or insert a string, returning its sequential u32 ID.
    pub fn get_or_insert(&mut self, s: &str) -> u32 {
        self.inner.assign_or_lookup(s)
    }

    /// Look up or insert by prefix + name parts, avoiding heap allocation on hits.
    pub fn get_or_insert_parts(&mut self, prefix: &str, name: &str) -> u32 {
        let total_len = prefix.len() + name.len();

        // Stack-based lookup for short IRIs (avoids heap allocation on hits)
        if total_len <= 256 {
            let mut buf = [0u8; 256];
            buf[..prefix.len()].copy_from_slice(prefix.as_bytes());
            buf[prefix.len()..total_len].copy_from_slice(name.as_bytes());
            // SAFETY: buf[..total_len] is copied from two valid UTF-8 &str slices.
            let iri = unsafe { std::str::from_utf8_unchecked(&buf[..total_len]) };

            if let Some(id) = self.inner.find(iri) {
                return id;
            }
        }

        // Miss (or rare long IRI): heap allocate for insertion
        let mut full_iri = String::with_capacity(total_len);
        full_iri.push_str(prefix);
        full_iri.push_str(name);
        self.inner.assign_or_lookup(&full_iri)
    }

    /// Look up a string without inserting.
    pub fn get(&self, s: &str) -> Option<u32> {
        self.inner.find(s)
    }

    /// Get the string for a given ID.
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.inner.resolve(id)
    }

    pub fn len(&self) -> u32 {
        self.inner.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return all entries as (id, value_bytes) pairs. Already in-memory.
    pub fn all_entries(&self) -> Vec<(u64, Vec<u8>)> {
        self.inner
            .iter()
            .map(|(id, s)| (id as u64, s.as_bytes().to_vec()))
            .collect()
    }

    /// Reconstruct from an ordered list of IRIs (e.g., from `IndexRoot`).
    ///
    /// Entry at index `i` gets ID `i`. This is the safe way to seed a dict
    /// from persisted data -- it guarantees ID stability.
    pub fn from_ordered_iris(iris: Vec<Arc<str>>) -> Self {
        Self {
            inner: VecBiDict::from_ordered_vec(0, iris),
        }
    }

    /// Iterator over `(id, &str)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &str)> {
        self.inner.iter()
    }
}

impl Default for PredicateDict {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// LanguageTagDict
// ============================================================================

/// Per-run language tag dictionary.
///
/// Maps language tags (e.g., "en", "fr") to u16 IDs. ID 0 means "no language
/// tag". Rebuilt at each run flush -- downstream merge renumbers.
///
/// Backed by `VecBiDict<u16>` with base_id=1 (1-based IDs; 0 = "no tag").
#[derive(Clone)]
pub struct LanguageTagDict {
    inner: VecBiDict<u16>,
}

impl LanguageTagDict {
    pub fn new() -> Self {
        Self {
            inner: VecBiDict::new(1),
        }
    }

    /// Look up or insert a language tag, returning its u16 ID (>= 1).
    /// Returns 0 if `tag` is None.
    pub fn get_or_insert(&mut self, tag: Option<&str>) -> u16 {
        match tag {
            Some(t) => self.inner.assign_or_lookup(t),
            None => 0,
        }
    }

    /// Get the tag string for a given ID.
    pub fn resolve(&self, id: u16) -> Option<&str> {
        if id == 0 {
            return None;
        }
        self.inner.resolve(id)
    }

    /// Reverse lookup: find the u16 ID for a given language tag string.
    /// Returns None if the tag is not in the dictionary.
    pub fn find(&self, tag: &str) -> Option<u16> {
        self.inner.find(tag)
    }

    /// Number of distinct language tags (excluding the "none" sentinel).
    pub fn len(&self) -> u16 {
        self.inner.len() as u16
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Find the ID for a language tag (reverse lookup).
    ///
    /// Returns `None` if the tag is not in the dictionary.
    pub fn find_id(&self, tag: &str) -> Option<u16> {
        self.inner.find(tag)
    }

    /// Iterator over (id, tag) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u16, &str)> {
        self.inner.iter()
    }

    /// Clear and reset the dictionary (for per-run reuse).
    pub fn clear(&mut self) {
        self.inner = VecBiDict::new(1);
    }

    /// Reconstruct from an ordered list of tags (e.g., from `IndexRoot`).
    ///
    /// Tag at index `i` gets ID `i + 1` (base_id=1; 0 = "no tag").
    pub fn from_ordered_tags(tags: Vec<Arc<str>>) -> Self {
        Self {
            inner: VecBiDict::from_ordered_vec(1, tags),
        }
    }
}

impl Default for LanguageTagDict {
    fn default() -> Self {
        Self::new()
    }
}
