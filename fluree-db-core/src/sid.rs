//! Subject ID (SID) - compact IRI representation
//!
//! A SID is composed of:
//! - `namespace_code`: u16 mapping to a namespace prefix (e.g., 2 = "http://www.w3.org/2001/XMLSchema#")
//! - `name`: Arc<str> local part after the namespace prefix (cheap clones)
//!
//! ## Ordering
//!
//! SIDs use strict total ordering: namespace_code first, then name.
//! This enables efficient binary search in sorted collections.
//!
//! ## Sentinels
//!
//! `Sid::min()` and `Sid::max()` provide bounds for wildcard queries.
//! `Sid::max()` uses `u16::MAX` (0xFFFF) as namespace_code, which is strictly
//! above all data namespace codes including OVERFLOW (0xFFFE).
//!
//! ## Interning
//!
//! The `SidInterner` provides deduplication of SID names, reducing memory
//! usage when many flakes share the same subjects/predicates. Use per-Db
//! for best memory efficiency.

use fluree_vocab::namespaces::{self, EMPTY};
use fluree_vocab::xsd_names;
use hashbrown::HashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Subject ID - compact IRI representation
///
/// Uses `Arc<str>` for the name to enable cheap clones and interning.
/// Serializes as `[namespace_code, name]` tuple in JSON.
#[derive(Clone, Debug)]
pub struct Sid {
    pub namespace_code: u16,
    pub name: Arc<str>,
}

impl Sid {
    /// Create a new SID
    pub fn new(namespace_code: u16, name: impl AsRef<str>) -> Self {
        Self {
            namespace_code,
            name: Arc::from(name.as_ref()),
        }
    }

    /// Create a new SID with a pre-interned name
    ///
    /// Use this when you already have an `Arc<str>` from an interner.
    pub fn with_arc(namespace_code: u16, name: Arc<str>) -> Self {
        Self {
            namespace_code,
            name,
        }
    }

    /// Minimum possible SID (for range query lower bounds)
    ///
    /// Uses namespace_code 0 and empty name, which sorts before
    /// any valid SID.
    pub fn min() -> Self {
        Self {
            namespace_code: 0,
            name: Arc::from(""),
        }
    }

    /// Maximum possible SID (for range query upper bounds)
    ///
    /// Uses u16::MAX namespace_code, which sorts after any valid SID.
    /// The name is empty because we only need to exceed the namespace_code.
    pub fn max() -> Self {
        Self {
            namespace_code: u16::MAX,
            name: Arc::from(""),
        }
    }

    /// Check if this is the minimum sentinel
    pub fn is_min(&self) -> bool {
        self.namespace_code == EMPTY && self.name.is_empty()
    }

    /// Check if this is the maximum sentinel
    pub fn is_max(&self) -> bool {
        self.namespace_code == u16::MAX
    }

    /// Get the name as a string slice
    pub fn name_str(&self) -> &str {
        &self.name
    }

    /// XSD `xsd:integer` SID.
    ///
    /// Cached via `LazyLock` — the `Arc<str>` is allocated once and reused.
    pub fn xsd_integer() -> Sid {
        use std::sync::LazyLock;
        static SID: LazyLock<Sid> = LazyLock::new(|| Sid::new(namespaces::XSD, xsd_names::INTEGER));
        SID.clone()
    }

    /// XSD `xsd:double` SID.
    ///
    /// Cached via `LazyLock` — the `Arc<str>` is allocated once and reused.
    pub fn xsd_double() -> Sid {
        use std::sync::LazyLock;
        static SID: LazyLock<Sid> = LazyLock::new(|| Sid::new(namespaces::XSD, xsd_names::DOUBLE));
        SID.clone()
    }

    /// XSD `xsd:decimal` SID.
    ///
    /// Cached via `LazyLock` — the `Arc<str>` is allocated once and reused.
    /// Used by aggregate output typing: `SUM` of integers stays `xsd:integer`,
    /// but `SUM` involving any decimal value promotes to `xsd:decimal`, and
    /// `AVG` of integers/decimals returns `xsd:decimal` per W3C SPARQL §17.4.1.7.
    pub fn xsd_decimal() -> Sid {
        use std::sync::LazyLock;
        static SID: LazyLock<Sid> = LazyLock::new(|| Sid::new(namespaces::XSD, xsd_names::DECIMAL));
        SID.clone()
    }

    /// XSD `xsd:float` SID.
    ///
    /// Cached via `LazyLock` — the `Arc<str>` is allocated once and reused.
    /// `xsd:float` and `xsd:double` are stored alike (`FlakeValue::Double(f64)`),
    /// but the SPARQL numeric promotion lattice distinguishes them: `xsd:float`
    /// promotes to `xsd:double` when mixed with one.
    pub fn xsd_float() -> Sid {
        use std::sync::LazyLock;
        static SID: LazyLock<Sid> = LazyLock::new(|| Sid::new(namespaces::XSD, xsd_names::FLOAT));
        SID.clone()
    }

    /// XSD `xsd:string` SID.
    ///
    /// Cached via `LazyLock` — the `Arc<str>` is allocated once and reused.
    pub fn xsd_string() -> Sid {
        use std::sync::LazyLock;
        static SID: LazyLock<Sid> = LazyLock::new(|| Sid::new(namespaces::XSD, xsd_names::STRING));
        SID.clone()
    }

    /// Canonical hash for HLL statistics.
    ///
    /// Hashes the namespace code and name together for a unique identifier.
    /// Uses xxHash64 for fast, high-quality hashing.
    pub fn canonical_hash(&self) -> u64 {
        use xxhash_rust::xxh64::Xxh64;
        let mut hasher = Xxh64::new(0);
        hasher.update(&self.namespace_code.to_le_bytes());
        hasher.update(self.name.as_bytes());
        hasher.digest()
    }
}

// === Strict Total Ordering ===
// No Option handling, no wildcards - pure comparison

impl PartialEq for Sid {
    fn eq(&self, other: &Self) -> bool {
        self.namespace_code == other.namespace_code && self.name == other.name
    }
}

impl Eq for Sid {}

impl Ord for Sid {
    fn cmp(&self, other: &Self) -> Ordering {
        self.namespace_code
            .cmp(&other.namespace_code)
            .then_with(|| self.name.cmp(&other.name))
    }
}

impl PartialOrd for Sid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Sid {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace_code.hash(state);
        self.name.hash(state);
    }
}

impl fmt::Display for Sid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}:{}]", self.namespace_code, self.name)
    }
}

// === Serde: Serialize as [namespace_code, name] tuple ===

impl Serialize for Sid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut tuple = serializer.serialize_tuple(2)?;
        tuple.serialize_element(&self.namespace_code)?;
        tuple.serialize_element(self.name.as_ref())?;
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for Sid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize as (u16, String) tuple, then convert to Arc<str>
        let (namespace_code, name): (u16, String) = Deserialize::deserialize(deserializer)?;
        Ok(Sid {
            namespace_code,
            name: Arc::from(name),
        })
    }
}

// === SID Interner ===

/// SID interner for deduplicating names across flakes
///
/// Uses `RefCell<HashMap>` for WASM compatibility. For native multi-threaded
/// use cases, wrap in `Mutex` or use `DashMap`.
///
/// # Example
///
/// ```
/// use fluree_db_core::sid::{Sid, SidInterner};
///
/// let interner = SidInterner::new();
/// let sid1 = interner.intern(100, "Person");
/// let sid2 = interner.intern(100, "Person");
///
/// // Same Arc pointer - no extra allocation
/// assert!(std::sync::Arc::ptr_eq(&sid1.name, &sid2.name));
/// ```
#[derive(Debug)]
pub struct SidInterner {
    /// Map from (namespace_code, name) to interned Arc<str>
    /// Uses Arc<str> as key to avoid allocation on cache hits via raw_entry API
    names: RwLock<HashMap<(u16, Arc<str>), ()>>,
}

impl Default for SidInterner {
    fn default() -> Self {
        Self::new()
    }
}

impl SidInterner {
    /// Create a new empty interner
    pub fn new() -> Self {
        Self {
            names: RwLock::new(HashMap::new()),
        }
    }

    /// Create an interner with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            names: RwLock::new(HashMap::with_capacity(capacity)),
        }
    }

    /// Compute hash using the map's hasher
    fn hash_key(map: &HashMap<(u16, Arc<str>), ()>, namespace_code: u16, name: &str) -> u64 {
        use std::hash::BuildHasher;
        let mut hasher = map.hasher().build_hasher();
        namespace_code.hash(&mut hasher);
        name.hash(&mut hasher);
        hasher.finish()
    }

    /// Intern a SID, returning a SID with a shared name Arc
    ///
    /// If the (namespace_code, name) pair has been seen before, returns
    /// a SID pointing to the same Arc<str>. Otherwise, creates a new
    /// Arc<str> and caches it.
    ///
    /// Uses raw_entry API to avoid allocating on cache hits.
    pub fn intern(&self, namespace_code: u16, name: &str) -> Sid {
        let mut names = self.names.write();
        let hash = Self::hash_key(&names, namespace_code, name);

        // Use raw_entry to look up with borrowed key, only allocate on miss
        let entry = names
            .raw_entry_mut()
            .from_hash(hash, |k| k.0 == namespace_code && k.1.as_ref() == name);

        let arc_name = match entry {
            hashbrown::hash_map::RawEntryMut::Occupied(e) => e.key().1.clone(),
            hashbrown::hash_map::RawEntryMut::Vacant(e) => {
                let arc = Arc::<str>::from(name);
                e.insert_hashed_nocheck(hash, (namespace_code, arc.clone()), ());
                arc
            }
        };

        Sid::with_arc(namespace_code, arc_name)
    }

    /// Intern an existing SID, potentially deduplicating its name
    ///
    /// If another SID with the same (namespace_code, name) was previously
    /// interned, returns a new SID sharing that Arc. Otherwise, the
    /// original SID's Arc is cached and returned.
    ///
    /// Uses raw_entry API to avoid allocating on cache hits.
    pub fn intern_sid(&self, sid: &Sid) -> Sid {
        let mut names = self.names.write();
        let hash = Self::hash_key(&names, sid.namespace_code, sid.name.as_ref());

        let entry = names.raw_entry_mut().from_hash(hash, |k| {
            k.0 == sid.namespace_code && k.1.as_ref() == sid.name.as_ref()
        });

        let arc_name = match entry {
            hashbrown::hash_map::RawEntryMut::Occupied(e) => e.key().1.clone(),
            hashbrown::hash_map::RawEntryMut::Vacant(e) => {
                // Reuse the existing Arc from the SID
                e.insert_hashed_nocheck(hash, (sid.namespace_code, sid.name.clone()), ());
                sid.name.clone()
            }
        };

        Sid::with_arc(sid.namespace_code, arc_name)
    }

    /// Get the number of unique interned names
    pub fn len(&self) -> usize {
        self.names.read().len()
    }

    /// Check if the interner is empty
    pub fn is_empty(&self) -> bool {
        self.names.read().is_empty()
    }

    /// Clear all interned names
    pub fn clear(&self) {
        self.names.write().clear();
    }

    /// Get approximate memory usage in bytes
    pub fn estimated_size_bytes(&self) -> usize {
        let names = self.names.read();
        let mut size = 0;
        for ((_, key), value) in names.iter() {
            // Key tuple + Arc<str> overhead + string bytes (shared)
            size += std::mem::size_of::<(u16, Box<str>)>()
                + std::mem::size_of::<Arc<str>>()
                + key.len();
            // The Arc<str> content is shared, so don't double count
            // But add Arc overhead
            size += std::mem::size_of::<usize>() * 2; // Arc refcount + ptr
            let () = value; // silence unused warning
        }
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sid_ordering() {
        let a = Sid::new(1, "foo");
        let b = Sid::new(1, "bar");
        let c = Sid::new(2, "foo");

        // Same namespace: compare by name
        assert!(b < a); // "bar" < "foo"

        // Different namespace: compare by namespace first
        assert!(a < c); // namespace 1 < 2
    }

    #[test]
    fn test_sid_min_max() {
        let min = Sid::min();
        let max = Sid::max();
        let regular = Sid::new(100, "test");

        assert!(min < regular);
        assert!(regular < max);
        assert!(min < max);
    }

    #[test]
    fn test_sid_serde_roundtrip() {
        let sid = Sid::new(42, "example");
        let json = serde_json::to_string(&sid).unwrap();
        assert_eq!(json, "[42,\"example\"]");

        let parsed: Sid = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, sid);
    }

    #[test]
    fn test_sid_equality() {
        let a = Sid::new(1, "test");
        let b = Sid::new(1, "test");
        let c = Sid::new(1, "other");

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_sid_clone_is_cheap() {
        let sid = Sid::new(100, "this_is_a_longer_name_to_demonstrate_arc_sharing");
        let cloned = sid.clone();

        // Both point to the same Arc<str>
        assert!(Arc::ptr_eq(&sid.name, &cloned.name));
    }

    // === SidInterner tests ===

    #[test]
    fn test_interner_deduplicates() {
        let interner = SidInterner::new();

        let sid1 = interner.intern(100, "Person");
        let sid2 = interner.intern(100, "Person");
        let sid3 = interner.intern(100, "Company"); // Different name
        let sid4 = interner.intern(200, "Person"); // Different namespace

        // Same (namespace, name) should share Arc
        assert!(Arc::ptr_eq(&sid1.name, &sid2.name));

        // Different name should NOT share Arc
        assert!(!Arc::ptr_eq(&sid1.name, &sid3.name));

        // Different namespace (same name) should NOT share Arc
        // (they're keyed by (namespace_code, name))
        assert!(!Arc::ptr_eq(&sid1.name, &sid4.name));

        // But values are still equal
        assert_eq!(sid1, sid2);
        assert_eq!(sid1.name.as_ref(), "Person");
        assert_eq!(sid4.name.as_ref(), "Person");
    }

    #[test]
    fn test_interner_intern_sid() {
        let interner = SidInterner::new();

        // First, intern via string
        let sid1 = interner.intern(100, "Test");

        // Now intern an existing SID
        let external_sid = Sid::new(100, "Test");
        let sid2 = interner.intern_sid(&external_sid);

        // Should share the same Arc from sid1
        assert!(Arc::ptr_eq(&sid1.name, &sid2.name));

        // External SID has its own Arc (not shared)
        assert!(!Arc::ptr_eq(&external_sid.name, &sid1.name));
    }

    #[test]
    fn test_interner_len() {
        let interner = SidInterner::new();

        assert_eq!(interner.len(), 0);
        assert!(interner.is_empty());

        interner.intern(1, "a");
        interner.intern(1, "b");
        interner.intern(2, "a");

        assert_eq!(interner.len(), 3);
        assert!(!interner.is_empty());

        // Duplicate doesn't increase count
        interner.intern(1, "a");
        assert_eq!(interner.len(), 3);

        interner.clear();
        assert_eq!(interner.len(), 0);
    }

    // === HLL Canonical Hash Tests ===

    mod hll_hash_tests {
        use super::*;

        #[test]
        fn test_sid_canonical_hash_deterministic() {
            // Same SID should always produce same hash
            let sid1 = Sid::new(100, "Person");
            let sid2 = Sid::new(100, "Person");
            assert_eq!(sid1.canonical_hash(), sid2.canonical_hash());
        }

        #[test]
        fn test_sid_canonical_hash_different_namespace() {
            // Same name, different namespace should produce different hash
            let sid1 = Sid::new(100, "Person");
            let sid2 = Sid::new(200, "Person");
            assert_ne!(sid1.canonical_hash(), sid2.canonical_hash());
        }

        #[test]
        fn test_sid_canonical_hash_different_name() {
            // Same namespace, different name should produce different hash
            let sid1 = Sid::new(100, "Person");
            let sid2 = Sid::new(100, "Company");
            assert_ne!(sid1.canonical_hash(), sid2.canonical_hash());
        }

        #[test]
        fn test_sid_canonical_hash_uniqueness() {
            // Various SIDs should produce unique hashes
            let sids = [
                Sid::new(0, ""),
                Sid::new(0, "a"),
                Sid::new(1, ""),
                Sid::new(1, "a"),
                Sid::new(100, "Person"),
                Sid::new(100, "Company"),
                Sid::new(200, "Person"),
            ];

            let hashes: Vec<u64> = sids.iter().map(super::super::Sid::canonical_hash).collect();

            // Check for uniqueness
            let unique_count = {
                let mut set = std::collections::HashSet::new();
                for h in &hashes {
                    set.insert(*h);
                }
                set.len()
            };
            assert_eq!(
                unique_count,
                hashes.len(),
                "Hash collision detected in test SIDs"
            );
        }
    }
}
