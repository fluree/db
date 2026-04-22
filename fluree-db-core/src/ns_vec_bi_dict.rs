//! Namespace-partitioned Vec-backed bidirectional dictionary.
//!
//! [`NsVecBiDict`] handles the sid64 encoding where IDs are structured as
//! `(ns_code << 48) | local_id`. Local IDs are dense sequential *within each
//! namespace*, but the overall ID space is sparse (huge gaps between namespaces).
//!
//! - **Forward:** `Vec<Vec<Arc<str>>>` indexed by `[ns_code][local_id - watermark - 1]`.
//!   Two Vec index ops, zero hashing.
//! - **Reverse:** `HashMap<Box<[u8]>, u64>` with key = `[ns_code BE 2B][suffix bytes]`.
//!   One hash lookup.
//!
//! NS_OVERFLOW (`0xFFFF`) subjects use dedicated scalar fields and a separate
//! Vec to avoid resizing per-namespace vectors to 65536 entries.

use std::sync::Arc;

use hashbrown::HashMap;

use crate::subject_id::SubjectId;

/// Namespace code reserved for overflow subjects (full IRI as suffix).
const NS_OVERFLOW: u16 = 0xFFFF;

// ---------------------------------------------------------------------------
// NsVecBiDict
// ---------------------------------------------------------------------------

/// Namespace-partitioned Vec-backed bidirectional dictionary.
#[derive(Clone, Debug)]
pub struct NsVecBiDict {
    /// Forward: `entries[ns_code][local_id - watermark - 1]`.
    entries: Vec<Vec<Arc<str>>>,
    /// Reverse: `[ns_code BE 2B][suffix bytes]` → sid64.
    reverse: HashMap<Box<[u8]>, u64>,
    /// Default local-id base for namespaces not present in `watermarks`.
    ///
    /// This exists so callers can build overlay-only dictionaries that allocate
    /// `sid64` values in a high local-id range (to sort after persisted IDs)
    /// without needing to pre-size watermark vectors for all namespace codes.
    ///
    /// For normal novelty dictionaries this is `1` (allocate from local_id=1),
    /// but query-time ephemeral subject dictionaries may choose a much larger
    /// base (e.g., `0x0000_8000_0000_0000`).
    local_base: u64,
    /// Per-namespace watermarks: `watermarks[ns_code]` = max persisted local_id.
    watermarks: Vec<u64>,
    /// Per-namespace next allocation counter.
    next_local_ids: Vec<u64>,
    /// Separate watermark for NS_OVERFLOW (0xFFFF).
    overflow_watermark: u64,
    /// Next local_id for NS_OVERFLOW subjects.
    overflow_next_local_id: u64,
    /// Separate Vec for NS_OVERFLOW entries.
    overflow_entries: Vec<Arc<str>>,
}

impl NsVecBiDict {
    /// Create for genesis (all watermarks 0).
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            reverse: HashMap::new(),
            local_base: 1,
            watermarks: Vec::new(),
            next_local_ids: Vec::new(),
            overflow_watermark: 0,
            overflow_next_local_id: 0,
            overflow_entries: Vec::new(),
        }
    }

    /// Create an empty dict that allocates local IDs starting at `local_base`.
    ///
    /// This is useful for query-time ephemeral subject dictionaries: by choosing
    /// a high `local_base`, IDs are guaranteed to sort after persisted IDs
    /// within the same namespace (while still using proper `SubjectId` encoding).
    pub fn with_local_base(local_base: u64) -> Self {
        let local_base = local_base.max(1);
        Self {
            entries: Vec::new(),
            reverse: HashMap::new(),
            local_base,
            watermarks: Vec::new(),
            next_local_ids: Vec::new(),
            overflow_watermark: local_base - 1,
            overflow_next_local_id: local_base,
            overflow_entries: Vec::new(),
        }
    }

    /// Create with watermarks from a persisted index root.
    ///
    /// `watermarks[i]` = max persisted local_id for namespace code `i`.
    /// `overflow_wm` = max persisted local_id for NS_OVERFLOW.
    pub fn with_watermarks(watermarks: Vec<u64>, overflow_wm: u64) -> Self {
        let next_local_ids: Vec<u64> = watermarks.iter().map(|&wm| wm + 1).collect();
        let entries = vec![Vec::new(); watermarks.len()];
        Self {
            entries,
            reverse: HashMap::new(),
            local_base: 1,
            watermarks,
            next_local_ids,
            overflow_watermark: overflow_wm,
            overflow_next_local_id: overflow_wm + 1,
            overflow_entries: Vec::new(),
        }
    }

    /// Look up or assign a sid64 for `(ns_code, suffix)`.
    ///
    /// If already present in the reverse map, returns the existing sid64.
    /// Otherwise allocates a new sid64 with the next local_id for this
    /// namespace and inserts into both forward and reverse structures.
    pub fn assign_or_lookup(&mut self, ns_code: u16, suffix: &str) -> u64 {
        let key = lookup_key(ns_code, suffix);
        if let Some(&id) = self.reverse.get(key.as_slice()) {
            return id;
        }

        let local_id = if ns_code == NS_OVERFLOW {
            if self.overflow_next_local_id <= self.overflow_watermark {
                self.overflow_next_local_id = self.overflow_watermark + 1;
            }
            let id = self.overflow_next_local_id;
            self.overflow_next_local_id = id + 1;
            id
        } else {
            let ns_idx = ns_code as usize;
            if ns_idx >= self.next_local_ids.len() {
                self.next_local_ids.resize(ns_idx + 1, self.local_base);
            }
            if ns_idx >= self.watermarks.len() {
                self.watermarks.resize(ns_idx + 1, self.local_base - 1);
            }
            if ns_idx >= self.entries.len() {
                self.entries.resize_with(ns_idx + 1, Vec::new);
            }
            // For newly-created namespaces (or callers that seeded with 0),
            // ensure allocation begins above the namespace watermark and respects
            // `local_base`.
            if self.watermarks[ns_idx] == 0 && self.local_base > 1 {
                self.watermarks[ns_idx] = self.local_base - 1;
            }
            if self.next_local_ids[ns_idx] == 0 && self.local_base > 1 {
                self.next_local_ids[ns_idx] = self.local_base;
            }
            if self.next_local_ids[ns_idx] <= self.watermarks[ns_idx] {
                self.next_local_ids[ns_idx] = self.watermarks[ns_idx] + 1;
            }
            let id = self.next_local_ids[ns_idx];
            self.next_local_ids[ns_idx] = id + 1;
            id
        };

        let sid64 = SubjectId::new(ns_code, local_id).as_u64();
        let interned: Arc<str> = Arc::from(suffix);

        if ns_code == NS_OVERFLOW {
            self.overflow_entries.push(Arc::clone(&interned));
        } else {
            self.entries[ns_code as usize].push(Arc::clone(&interned));
        }

        self.reverse.insert(key.into_boxed_slice(), sid64);

        sid64
    }

    /// Reverse lookup: find sid64 by `(ns_code, suffix)`.
    pub fn find_subject(&self, ns_code: u16, suffix: &str) -> Option<u64> {
        let key = lookup_key(ns_code, suffix);
        self.reverse.get(key.as_slice()).copied()
    }

    /// Forward lookup: resolve sid64 → `(ns_code, &suffix)`.
    ///
    /// Decomposes the sid64 into namespace and local_id, then indexes
    /// into the appropriate Vec. Zero hashing.
    pub fn resolve_subject(&self, sid64: u64) -> Option<(u16, &str)> {
        let sid = SubjectId::from_u64(sid64);
        let ns_code = sid.ns_code();
        let local_id = sid.local_id();

        if ns_code == NS_OVERFLOW {
            let wm = self.overflow_watermark;
            if local_id == 0 || local_id <= wm {
                return None;
            }
            let idx = (local_id - wm - 1) as usize;
            self.overflow_entries.get(idx).map(|s| (ns_code, &**s))
        } else {
            let ns_idx = ns_code as usize;
            if ns_idx >= self.entries.len() {
                return None;
            }
            let wm = self.watermarks.get(ns_idx).copied().unwrap_or(0);
            if local_id == 0 || local_id <= wm {
                return None;
            }
            let idx = (local_id - wm - 1) as usize;
            self.entries[ns_idx].get(idx).map(|s| (ns_code, &**s))
        }
    }

    /// Get the watermark (max persisted local_id) for a namespace code.
    ///
    /// Returns 0 for unknown/out-of-range namespace codes.
    pub fn watermark_for_ns(&self, ns_code: u16) -> u64 {
        if ns_code == NS_OVERFLOW {
            return self.overflow_watermark;
        }
        self.watermarks.get(ns_code as usize).copied().unwrap_or(0)
    }

    /// Total entries across all namespaces.
    pub fn len(&self) -> usize {
        self.reverse.len()
    }

    /// True if no novel subjects have been registered.
    pub fn is_empty(&self) -> bool {
        self.reverse.is_empty()
    }
}

impl Default for NsVecBiDict {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Key encoding (matches dict_novelty::subject_reverse_key format)
// ---------------------------------------------------------------------------

/// Build a lookup key as `Vec<u8>`: `[ns_code BE 2 bytes][suffix UTF-8 bytes]`.
#[inline]
fn lookup_key(ns_code: u16, suffix: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + suffix.len());
    key.extend_from_slice(&ns_code.to_be_bytes());
    key.extend_from_slice(suffix.as_bytes());
    key
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_namespace_assignment() {
        let mut d = NsVecBiDict::new();
        let id1 = d.assign_or_lookup(2, "Alice");
        let id2 = d.assign_or_lookup(3, "Alice");
        let id3 = d.assign_or_lookup(2, "Bob");

        // Same suffix in different namespaces → different sid64
        assert_ne!(id1, id2);
        // Different suffix in same namespace → different sid64
        assert_ne!(id1, id3);

        // Dedup: same (ns, suffix) → same sid64
        assert_eq!(d.assign_or_lookup(2, "Alice"), id1);

        // Verify namespace structure
        assert_eq!(SubjectId::from_u64(id1).ns_code(), 2);
        assert_eq!(SubjectId::from_u64(id2).ns_code(), 3);
        assert_eq!(SubjectId::from_u64(id3).ns_code(), 2);

        // Sequential local IDs within namespace
        assert_eq!(SubjectId::from_u64(id1).local_id(), 1);
        assert_eq!(SubjectId::from_u64(id3).local_id(), 2);
        assert_eq!(SubjectId::from_u64(id2).local_id(), 1);
    }

    #[test]
    fn test_watermark_routing() {
        let mut d = NsVecBiDict::with_watermarks(vec![0, 0, 100], 0);

        let id = d.assign_or_lookup(2, "new_subject");
        let sid = SubjectId::from_u64(id);
        assert_eq!(sid.ns_code(), 2);
        assert_eq!(sid.local_id(), 101); // starts at watermark + 1
    }

    #[test]
    fn test_overflow_handling() {
        let mut d = NsVecBiDict::new();

        let id1 = d.assign_or_lookup(NS_OVERFLOW, "http://example.com/full-iri");
        let sid1 = SubjectId::from_u64(id1);
        assert_eq!(sid1.ns_code(), NS_OVERFLOW);
        assert_eq!(sid1.local_id(), 1);

        let id2 = d.assign_or_lookup(NS_OVERFLOW, "http://other.com/iri");
        assert_eq!(SubjectId::from_u64(id2).local_id(), 2);

        // Dedup
        assert_eq!(
            d.assign_or_lookup(NS_OVERFLOW, "http://example.com/full-iri"),
            id1
        );

        // find/resolve
        assert_eq!(
            d.find_subject(NS_OVERFLOW, "http://example.com/full-iri"),
            Some(id1)
        );
        let (ns, suffix) = d.resolve_subject(id1).unwrap();
        assert_eq!(ns, NS_OVERFLOW);
        assert_eq!(suffix, "http://example.com/full-iri");

        // NS_OVERFLOW does NOT resize per-namespace vectors
        assert!(d.entries.is_empty());
    }

    #[test]
    fn test_forward_lookup_decomposition() {
        let mut d = NsVecBiDict::new();
        let id = d.assign_or_lookup(5, "foo");

        let (ns, suffix) = d.resolve_subject(id).unwrap();
        assert_eq!(ns, 5);
        assert_eq!(suffix, "foo");

        // Missing sid64 returns None
        assert!(d.resolve_subject(999).is_none());
    }

    #[test]
    fn test_find_missing() {
        let d = NsVecBiDict::new();
        assert_eq!(d.find_subject(5, "bar"), None);
    }

    #[test]
    fn test_len_tracking() {
        let mut d = NsVecBiDict::new();
        assert!(d.is_empty());
        assert_eq!(d.len(), 0);

        d.assign_or_lookup(1, "a");
        d.assign_or_lookup(1, "b");
        d.assign_or_lookup(2, "a");

        assert_eq!(d.len(), 3);
        assert!(!d.is_empty());
    }

    #[test]
    fn test_watermark_for_ns() {
        let d = NsVecBiDict::with_watermarks(vec![10, 20, 30], 50);
        assert_eq!(d.watermark_for_ns(0), 10);
        assert_eq!(d.watermark_for_ns(1), 20);
        assert_eq!(d.watermark_for_ns(2), 30);
        assert_eq!(d.watermark_for_ns(3), 0); // out of range
        assert_eq!(d.watermark_for_ns(NS_OVERFLOW), 50);
    }

    #[test]
    fn test_reverse_key_encoding_matches() {
        // Verify our lookup_key matches the format from dict_novelty::subject_reverse_key
        use crate::dict_novelty::subject_reverse_key;

        let boxed = subject_reverse_key(2, "Alice");
        let vec_key = lookup_key(2, "Alice");
        assert_eq!(&*boxed, vec_key.as_slice());
    }
}
