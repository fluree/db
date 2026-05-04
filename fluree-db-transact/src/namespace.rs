//! Namespace registry for IRI prefix code allocation
//!
//! This module provides `NamespaceRegistry` for managing namespace codes
//! during transaction processing. New IRIs with unknown prefixes get new
//! codes allocated, and these allocations are tracked for persistence
//! in the commit record.
//!
//! For parallel import, [`SharedNamespaceAllocator`] provides thread-safe
//! allocation with [`WorkerCache`] for lock-free per-worker lookups.
//! [`NsAllocator`] abstracts over both single-threaded and parallel modes.
//!
//! ## Encoding Invariant
//!
//! All IRI-to-SID encoding uses [`canonical_split`] to deterministically
//! choose `(prefix, suffix)`, then exact-prefix lookup/allocation. No
//! longest-prefix-match or heuristic splitting is used.
//!
//! ## Predefined Namespace Codes
//!
//! Fluree uses predefined codes for common namespaces to ensure compatibility
//! with existing databases. User-supplied namespaces start at `USER_START`.

use fluree_db_core::ns_encoding::{canonical_split, NamespaceCodes, NsAllocError, NsSplitMode};
use fluree_db_core::{LedgerSnapshot, Sid};
use fluree_vocab::namespaces::{BLANK_NODE, OVERFLOW, USER_START};
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

/// First code available for user-defined namespaces.
/// Re-exported from `fluree_vocab::namespaces::USER_START`.
pub const USER_NS_START: u16 = USER_START;

/// Blank node prefix (standard RDF blank node syntax)
pub const BLANK_NODE_PREFIX: &str = "_:";

/// Fluree blank node ID prefix (used in generated blank node names)
pub const BLANK_NODE_ID_PREFIX: &str = "fdb";

// ============================================================================
// NamespaceRegistry (single-threaded, used by serial import + transact paths)
// ============================================================================

/// Registry for namespace prefix codes
///
/// During transaction processing, new IRIs may introduce prefixes not yet
/// in the database's namespace table. This registry:
/// 1. Starts with predefined codes for common namespaces
/// 2. Loads existing codes from `snapshot.namespace_codes`
/// 3. Allocates new codes as needed (starting at USER_NS_START)
/// 4. Tracks new allocations in `delta` for commit persistence
///
/// All IRI encoding uses [`canonical_split`] for deterministic prefix
/// selection, followed by exact-prefix lookup/allocation.
#[derive(Debug, Clone)]
pub struct NamespaceRegistry {
    /// Two-way namespace map with allocation tracking.
    codes: NamespaceCodes,

    /// Ledger-fixed split mode for canonical IRI encoding.
    split_mode: NsSplitMode,
}

impl NamespaceRegistry {
    /// Create a new registry with default namespaces
    pub fn new() -> Self {
        Self {
            codes: NamespaceCodes::new(),
            split_mode: NsSplitMode::default(),
        }
    }

    /// Create a registry seeded from a database's namespace codes.
    ///
    /// Merges snapshot codes into predefined defaults with bimap conflict
    /// validation.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot's namespace codes conflict with the built-in
    /// defaults. This indicates corrupt persisted data — no legacy data
    /// exists to recover from, so failing fast is the correct behavior.
    pub fn from_db(snapshot: &LedgerSnapshot) -> Self {
        let mut codes = NamespaceCodes::new(); // seeded with defaults
        let snapshot_ns: HashMap<u16, String> = snapshot
            .namespaces()
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        codes.merge_delta(&snapshot_ns).unwrap_or_else(|e| {
            panic!(
                "namespace conflict merging snapshot into defaults — \
                 snapshot namespace codes are corrupt: {e}"
            );
        });

        Self {
            codes,
            split_mode: snapshot.ns_split_mode(),
        }
    }

    /// Set the split mode for canonical IRI encoding.
    #[inline]
    pub fn set_split_mode(&mut self, mode: NsSplitMode) {
        self.split_mode = mode;
    }

    /// Get the current split mode.
    #[inline]
    pub fn split_mode(&self) -> NsSplitMode {
        self.split_mode
    }

    /// Get the code for a prefix, allocating a new one if needed.
    ///
    /// If the prefix is not yet registered, a new code is allocated
    /// and recorded in the delta for persistence. If all codes in
    /// `USER_START..OVERFLOW` are exhausted, returns `OVERFLOW` as a
    /// pure sentinel — OVERFLOW is never inserted into codes/names/delta.
    /// The caller (`sid_for_iri`) handles overflow by using the full IRI as
    /// the SID name.
    ///
    /// Returns the code for an existing prefix or allocates a new one.
    ///
    /// On overflow, returns the `OVERFLOW` sentinel. On a bimap conflict
    /// (corrupted namespace state), logs an error and returns `OVERFLOW`
    /// so the server stays up — the affected IRI will encode as
    /// `Sid(OVERFLOW, full_iri)` which is lossy but not a crash.
    pub fn get_or_allocate(&mut self, prefix: &str) -> u16 {
        match self.codes.allocate_prefix(prefix) {
            Ok(code) => code,
            Err(NsAllocError::Overflow) => OVERFLOW,
            Err(e @ (NsAllocError::CodeConflict { .. } | NsAllocError::PrefixConflict { .. })) => {
                tracing::error!(
                    error = %e,
                    "namespace bimap conflict — corrupted namespace state or invalid \
                     commit history; encoding IRI as OVERFLOW to avoid crash"
                );
                OVERFLOW
            }
        }
    }

    /// Get the namespace code for blank nodes.
    pub fn blank_node_code(&self) -> u16 {
        BLANK_NODE
    }

    /// Look up a code without allocating
    pub fn get_code(&self, prefix: &str) -> Option<u16> {
        self.codes.get_code(prefix)
    }

    /// Look up a prefix by code
    pub fn get_prefix(&self, code: u16) -> Option<&str> {
        self.codes.get_prefix(code)
    }

    /// Check if a prefix is registered
    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.codes.get_code(prefix).is_some()
    }

    /// Number of registered namespace codes (predefined + user-allocated).
    pub fn code_count(&self) -> usize {
        self.codes.len()
    }

    /// Returns the set of all registered namespace codes (numeric values).
    pub fn all_codes(&self) -> FxHashSet<u16> {
        self.codes.iter().map(|(code, _)| code).collect()
    }

    /// Take the delta (new allocations) and reset it
    ///
    /// Returns the map of new allocations (code → prefix) for
    /// inclusion in the commit record.
    pub fn take_delta(&mut self) -> HashMap<u16, String> {
        self.codes.take_delta()
    }

    /// Get a reference to the delta without consuming it
    pub fn delta(&self) -> &HashMap<u16, String> {
        self.codes.delta()
    }

    /// Check if there are any new allocations
    pub fn has_delta(&self) -> bool {
        self.codes.has_delta()
    }

    /// Create a Sid for an IRI, allocating namespace code if needed.
    ///
    /// Uses [`canonical_split`] to deterministically choose `(prefix, suffix)`,
    /// then exact-prefix lookup or allocation.
    ///
    /// When namespace codes are exhausted (`allocate_prefix` returns `Overflow`),
    /// the full IRI is used as the SID name — no prefix splitting.
    pub fn sid_for_iri(&mut self, iri: &str) -> Sid {
        let (prefix, suffix) = canonical_split(iri, self.split_mode);

        let code = self.get_or_allocate(prefix);
        if code == OVERFLOW {
            // Overflow: store the full IRI as name, no prefix splitting
            Sid::new(OVERFLOW, iri)
        } else {
            Sid::new(code, suffix)
        }
    }

    /// Register a namespace code if not already present.
    ///
    /// Used to merge allocations from the shared allocator back into the
    /// serial registry (e.g., after commit-order publication). If the prefix
    /// is already registered (under any code), this is a no-op. OVERFLOW codes
    /// are ignored since they are pure sentinels and should never be registered.
    /// Returns `Err` on a namespace bimap conflict.
    pub fn ensure_code(&mut self, code: u16, prefix: &str) -> Result<(), NsAllocError> {
        if code >= OVERFLOW {
            return Ok(()); // OVERFLOW is a sentinel, never register it
        }
        let mut delta = HashMap::new();
        delta.insert(code, prefix.to_string());
        self.codes.merge_delta(&delta)
    }

    /// Adopt namespace allocations made elsewhere AND record them in the
    /// persistence delta so the next commit includes the mappings.
    ///
    /// Distinct from [`ensure_code`](Self::ensure_code), which only updates
    /// the in-memory lookup tables. Use this when staging a `Txn` whose
    /// templates reference namespace codes allocated by an upstream registry
    /// (e.g. `lower_sparql_update`'s caller-owned registry) — without
    /// persisting those mappings, post-commit reads can't resolve the
    /// committed flakes' namespace_codes back to IRIs.
    pub fn adopt_delta_for_persistence(
        &mut self,
        delta: &HashMap<u16, String>,
    ) -> Result<(), NsAllocError> {
        self.codes.adopt_delta_for_persistence(delta)
    }

    /// Create a Sid for a blank node.
    ///
    /// Blank nodes use the predefined `BLANK_NODE` namespace code and generate
    /// a unique local name in the format: `fdb-{unique_id}`
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        let local = format!("{BLANK_NODE_ID_PREFIX}-{unique_id}");
        Sid::new(BLANK_NODE, local)
    }
}

impl Default for NamespaceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// SharedNamespaceAllocator (thread-safe, for parallel import)
// ============================================================================

/// Thread-safe namespace allocator shared across parallel parse workers.
///
/// Allocation is concurrent via `RwLock`. Publication (which codes go into
/// which commit's `namespace_delta`) is handled separately in commit-order
/// by the serial finalizer.
///
/// ## Invariant
///
/// For each finalized commit at t, every `ns_code` referenced by its ops
/// must be resolvable from the cumulative `namespace_delta`s up to and
/// including commit t. This is enforced by the commit-order publication
/// logic in the import orchestrator, NOT by this allocator.
pub struct SharedNamespaceAllocator {
    inner: RwLock<NamespaceCodes>,
    /// Atomic split mode — can be set once via `set_split_mode` before
    /// workers start parsing.
    split_mode: AtomicU8,
}

impl SharedNamespaceAllocator {
    /// Create a shared allocator seeded from an existing `NamespaceRegistry`.
    ///
    /// Used after chunk 0 has been parsed serially to establish the initial
    /// namespace mappings.
    pub fn from_registry(reg: &NamespaceRegistry) -> Self {
        Self {
            inner: RwLock::new(reg.codes.clone()),
            split_mode: AtomicU8::new(
                reg.split_mode
                    .to_byte()
                    .expect("split_mode must be persistable"),
            ),
        }
    }

    /// Set the split mode for canonical IRI encoding.
    ///
    /// Safe to call from any thread (atomic). Must be called before workers
    /// start parsing.
    pub fn set_split_mode(&self, mode: NsSplitMode) {
        self.split_mode.store(
            mode.to_byte().expect("split_mode must be persistable"),
            Ordering::Relaxed,
        );
    }

    /// Get the current split mode.
    #[inline]
    pub fn split_mode(&self) -> NsSplitMode {
        NsSplitMode::from_byte(self.split_mode.load(Ordering::Relaxed))
    }

    /// Thread-safe get-or-allocate with read-lock fast path.
    ///
    /// Returns `OVERFLOW` sentinel when codes are exhausted or on a bimap
    /// conflict (logged as error). Does not panic.
    pub fn get_or_allocate(&self, prefix: &str) -> u16 {
        // Fast path: read lock
        {
            let inner = self.inner.read();
            if let Some(code) = inner.get_code(prefix) {
                return code;
            }
        }
        // Slow path: write lock with double-check
        let mut inner = self.inner.write();
        if let Some(code) = inner.get_code(prefix) {
            return code;
        }
        match inner.allocate_prefix(prefix) {
            Ok(code) => code,
            Err(NsAllocError::Overflow) => OVERFLOW,
            Err(e @ (NsAllocError::CodeConflict { .. } | NsAllocError::PrefixConflict { .. })) => {
                tracing::error!(
                    error = %e,
                    "namespace bimap conflict — encoding IRI as OVERFLOW to avoid crash"
                );
                OVERFLOW
            }
        }
    }

    /// Thread-safe SID resolution using canonical splitting.
    pub fn sid_for_iri(&self, iri: &str) -> Sid {
        let (prefix, suffix) = canonical_split(iri, self.split_mode());
        let code = self.get_or_allocate(prefix);
        if code == OVERFLOW {
            Sid::new(OVERFLOW, iri)
        } else {
            Sid::new(code, suffix)
        }
    }

    /// Create a Sid for a blank node (no lock needed — BLANK_NODE is predefined).
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        let local = format!("{BLANK_NODE_ID_PREFIX}-{unique_id}");
        Sid::new(BLANK_NODE, local)
    }

    /// Batch lookup of code→prefix mappings for publication.
    ///
    /// Returns mappings for all requested codes. Debug-asserts that every
    /// requested code is found (a missing code indicates a bug). Never
    /// returns OVERFLOW.
    pub fn lookup_codes(&self, codes: &FxHashSet<u16>) -> HashMap<u16, String> {
        let inner = self.inner.read();
        let mut result = HashMap::with_capacity(codes.len());
        for &code in codes {
            debug_assert!(code < OVERFLOW, "OVERFLOW must never be published");
            if let Some(prefix) = inner.get_prefix(code) {
                result.insert(code, prefix.to_string());
            } else {
                // Should never happen — every code in the set was allocated
                // through this allocator. Log and skip in release; panic in debug.
                #[cfg(debug_assertions)]
                panic!("code {code} not found in shared allocator");
                #[cfg(not(debug_assertions))]
                tracing::warn!(code, "namespace code not found in shared allocator");
            }
        }
        result
    }

    /// Look up the prefix string for a namespace code.
    pub fn get_prefix(&self, code: u16) -> Option<String> {
        self.inner
            .read()
            .get_prefix(code)
            .map(std::string::ToString::to_string)
    }

    /// Synchronize codes from a `NamespaceRegistry` into this allocator.
    ///
    /// Used after serial import paths where namespace codes were allocated
    /// in the registry but not in the shared allocator. Preserves exact code
    /// assignments from the registry.
    /// Returns `Err` on a namespace bimap conflict.
    pub fn sync_from_registry(&self, reg: &NamespaceRegistry) -> Result<(), NsAllocError> {
        let delta = reg.codes.code_to_prefix_map().clone();
        let mut inner = self.inner.write();
        inner.merge_delta(&delta)
    }

    /// Take a snapshot of the current state for worker initialization.
    ///
    /// Returns `(prefix_to_code, next_code)`. Workers use the map for local
    /// lookups and `next_code` to identify which codes were allocated after
    /// the snapshot.
    pub fn snapshot(&self) -> (FxHashMap<String, u16>, u16) {
        let inner = self.inner.read();
        let codes: FxHashMap<String, u16> = inner
            .prefix_to_code_map()
            .iter()
            .map(|(k, &v)| (k.clone(), v))
            .collect();
        (codes, inner.next_code())
    }
}

// ============================================================================
// WorkerCache (per-worker, lock-free lookups with local snapshot)
// ============================================================================

/// Per-worker namespace cache with a local snapshot.
///
/// Created at worker spawn time from a snapshot of [`SharedNamespaceAllocator`].
/// All `sid_for_iri()` calls use the local map (no lock). Only genuinely
/// new prefix allocations touch the shared allocator.
///
/// Tracks `new_codes`: codes first observed by this worker that were not in
/// the initial snapshot (`code >= snapshot_next_code`). This includes codes
/// allocated by OTHER workers if this worker uses them — because if this
/// chunk's ops reference a code, the commit must publish it if not already
/// published by a prior commit.
pub struct WorkerCache {
    alloc: Arc<SharedNamespaceAllocator>,
    /// Local copy of prefix→code map (for exact lookups).
    local_codes: FxHashMap<String, u16>,
    /// Split mode (copied from allocator, immutable).
    split_mode: NsSplitMode,
    /// The allocator's `next_code` at snapshot time. Any code >= this value
    /// was allocated after the snapshot and might need publishing.
    snapshot_next_code: u16,
    /// Codes first observed after snapshot (code >= snapshot_next_code, < OVERFLOW).
    new_codes: FxHashSet<u16>,
}

impl WorkerCache {
    /// Create a new worker cache from a snapshot of the shared allocator.
    pub fn new(alloc: Arc<SharedNamespaceAllocator>) -> Self {
        let (local_codes, snapshot_next_code) = alloc.snapshot();
        let split_mode = alloc.split_mode();
        Self {
            alloc,
            local_codes,
            split_mode,
            snapshot_next_code,
            new_codes: FxHashSet::default(),
        }
    }

    /// Resolve an IRI to a SID using canonical splitting.
    ///
    /// Falls back to the shared allocator only when a genuinely new prefix
    /// must be allocated.
    pub fn sid_for_iri(&mut self, iri: &str) -> Sid {
        let (prefix, suffix) = canonical_split(iri, self.split_mode);

        let code = self.get_or_allocate(prefix);
        if code == OVERFLOW {
            Sid::new(OVERFLOW, iri)
        } else {
            Sid::new(code, suffix)
        }
    }

    /// Get or allocate a namespace code. Checks local cache first (no lock).
    pub fn get_or_allocate(&mut self, prefix: &str) -> u16 {
        // Local fast path
        if let Some(&code) = self.local_codes.get(prefix) {
            self.track_code(code);
            return code;
        }

        // Shared allocator (may lock)
        let code = self.alloc.get_or_allocate(prefix);

        // Update local state
        if code < OVERFLOW {
            self.local_codes.insert(prefix.to_string(), code);
            self.track_code(code);
        }

        code
    }

    /// Create a Sid for a blank node (no lock needed).
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        let local = format!("{BLANK_NODE_ID_PREFIX}-{unique_id}");
        Sid::new(BLANK_NODE, local)
    }

    /// Consume the cache and return the set of codes first observed after
    /// the snapshot. The serial finalizer uses these for commit-order publication.
    pub fn into_new_codes(self) -> FxHashSet<u16> {
        self.new_codes
    }

    /// Track a code as potentially needing publication if it was allocated
    /// after our snapshot.
    #[inline]
    fn track_code(&mut self, code: u16) {
        if code >= self.snapshot_next_code && code < OVERFLOW {
            self.new_codes.insert(code);
        }
    }
}

// ============================================================================
// NsAllocator (enum wrapper abstracting over single-thread / parallel modes)
// ============================================================================

/// Abstraction over namespace allocation for both serial and parallel paths.
///
/// - `Exclusive`: wraps `&mut NamespaceRegistry` for serial paths (transact,
///   chunk 0, TriG).
/// - `Cached`: wraps `&mut WorkerCache` for parallel import workers.
pub enum NsAllocator<'a> {
    Exclusive(&'a mut NamespaceRegistry),
    Cached(&'a mut WorkerCache),
}

impl NsAllocator<'_> {
    /// Resolve an IRI to a SID.
    pub fn sid_for_iri(&mut self, iri: &str) -> Sid {
        match self {
            NsAllocator::Exclusive(reg) => reg.sid_for_iri(iri),
            NsAllocator::Cached(cache) => cache.sid_for_iri(iri),
        }
    }

    /// Get or allocate a namespace code for a prefix.
    pub fn get_or_allocate(&mut self, prefix: &str) -> u16 {
        match self {
            NsAllocator::Exclusive(reg) => reg.get_or_allocate(prefix),
            NsAllocator::Cached(cache) => cache.get_or_allocate(prefix),
        }
    }

    /// Create a Sid for a blank node.
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        match self {
            NsAllocator::Exclusive(reg) => reg.blank_node_sid(unique_id),
            NsAllocator::Cached(cache) => cache.blank_node_sid(unique_id),
        }
    }
}

// ============================================================================
// Free functions
// ============================================================================

/// Generate a unique blank node ID using ULID
pub fn generate_blank_node_id() -> String {
    ulid::Ulid::new().to_string()
}

/// Check if an IRI is a Fluree-generated blank node ID
pub fn is_blank_node_id(iri: &str) -> bool {
    iri.starts_with("_:fdb")
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::LedgerSnapshot;
    use fluree_vocab::namespaces::{DID_KEY, EMPTY, JSON_LD, XSD};

    #[test]
    fn test_predefined_codes() {
        let registry = NamespaceRegistry::new();

        // Check predefined codes are present
        assert_eq!(registry.get_code(""), Some(EMPTY));
        assert_eq!(registry.get_code("@"), Some(JSON_LD));
        assert_eq!(
            registry.get_code("http://www.w3.org/2001/XMLSchema#"),
            Some(XSD)
        );
        assert_eq!(registry.get_code("_:"), Some(BLANK_NODE));

        // Check reverse lookup
        assert_eq!(registry.get_prefix(BLANK_NODE), Some("_:"));
        assert_eq!(
            registry.get_prefix(XSD),
            Some("http://www.w3.org/2001/XMLSchema#")
        );
    }

    #[test]
    fn test_blank_node_code_is_fixed() {
        let registry = NamespaceRegistry::new();
        assert_eq!(registry.blank_node_code(), BLANK_NODE);
    }

    #[test]
    fn test_allocate_new_code() {
        let mut registry = NamespaceRegistry::new();

        let code1 = registry.get_or_allocate("http://example.org/");
        let code2 = registry.get_or_allocate("http://other.org/");
        let code1_again = registry.get_or_allocate("http://example.org/");

        assert_eq!(code1, code1_again);
        assert_ne!(code1, code2);
        assert!(code1 >= USER_NS_START);
        assert!(code2 >= USER_NS_START);
        assert_eq!(registry.codes.delta().len(), 2);
    }

    #[test]
    fn test_blank_node_sid() {
        let registry = NamespaceRegistry::new();

        // Test with a ULID-style ID
        let sid = registry.blank_node_sid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(sid.namespace_code, BLANK_NODE);
        assert_eq!(sid.name.as_ref(), "fdb-01ARZ3NDEKTSV4RRFFQ69G5FAV");
    }

    #[test]
    fn test_is_blank_node_id() {
        assert!(is_blank_node_id("_:fdb-01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(is_blank_node_id("_:fdb"));
        assert!(!is_blank_node_id("_:b1"));
        assert!(!is_blank_node_id("http://example.org/thing"));
    }

    #[test]
    fn test_sid_for_iri_canonical_split() {
        let mut registry = NamespaceRegistry::new();

        // MostGranular: splits at last '/' → prefix = "http://example.org/"
        let sid1 = registry.sid_for_iri("http://example.org/Person");
        let sid2 = registry.sid_for_iri("http://example.org/name");

        // Same prefix, should have same code
        assert_eq!(sid1.namespace_code, sid2.namespace_code);
        assert_eq!(sid1.name.as_ref(), "Person");
        assert_eq!(sid2.name.as_ref(), "name");
    }

    #[test]
    fn test_sid_for_iri_uses_builtin_prefixes() {
        let mut registry = NamespaceRegistry::new();

        // did:key: is a predefined prefix; canonical_split for opaque IRI
        // with MostGranular splits at last ':', which is "did:key:" → matches built-in
        let sid = registry.sid_for_iri("did:key:z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR");
        assert_eq!(sid.namespace_code, DID_KEY);
        assert_eq!(
            sid.name.as_ref(),
            "z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR"
        );

        // XSD namespace ends with #, canonical split finds it
        let sid3 = registry.sid_for_iri("http://www.w3.org/2001/XMLSchema#string");
        assert_eq!(sid3.namespace_code, XSD);
        assert_eq!(sid3.name.as_ref(), "string");

        // No delta should be created since all prefixes are predefined
        assert!(registry.codes.delta().is_empty());
    }

    #[test]
    fn test_canonical_split_allocates_exact_prefix() {
        let mut registry = NamespaceRegistry::new();

        // Under canonical split (MostGranular), "http://ex.org/foo/bar"
        // splits as prefix="http://ex.org/foo/" suffix="bar"
        let sid = registry.sid_for_iri("http://ex.org/foo/bar");
        assert!(registry.has_prefix("http://ex.org/foo/"));
        assert_eq!(sid.name.as_ref(), "bar");

        // "http://ex.org/bar" splits as prefix="http://ex.org/" suffix="bar"
        let sid2 = registry.sid_for_iri("http://ex.org/bar");
        assert!(registry.has_prefix("http://ex.org/"));
        assert_eq!(sid2.name.as_ref(), "bar");

        // Different prefixes → different namespace codes
        assert_ne!(sid.namespace_code, sid2.namespace_code);
    }

    #[test]
    fn test_registry_from_db() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut registry = NamespaceRegistry::from_db(&snapshot);

        // Default split mode
        assert_eq!(registry.split_mode(), NsSplitMode::MostGranular);

        let iri = "http://some-unseen-host/blah/123/456";
        let sid = registry.sid_for_iri(iri);

        // MostGranular: split at last '/' → prefix="http://some-unseen-host/blah/123/"
        assert!(registry.has_prefix("http://some-unseen-host/blah/123/"));
        assert_eq!(sid.name.as_ref(), "456");
    }

    #[test]
    fn test_host_plus_n_mode() {
        let mut registry = NamespaceRegistry::new();
        registry.set_split_mode(NsSplitMode::HostPlusN(0));

        let iri = "http://some-host.example/blah/123/456";
        let sid = registry.sid_for_iri(iri);

        // HostPlusN(0): split at host root → prefix="http://some-host.example/"
        assert!(registry.has_prefix("http://some-host.example/"));
        assert_eq!(sid.name.as_ref(), "blah/123/456");
    }

    #[test]
    fn test_host_plus_1_mode() {
        let mut registry = NamespaceRegistry::new();
        registry.set_split_mode(NsSplitMode::HostPlusN(1));

        let iri = "http://example.com/api/v1/users";
        let sid = registry.sid_for_iri(iri);

        // HostPlusN(1): prefix="http://example.com/api/"
        assert!(registry.has_prefix("http://example.com/api/"));
        assert_eq!(sid.name.as_ref(), "v1/users");
    }

    #[test]
    fn test_shared_alloc_concurrent_no_collisions() {
        use std::sync::Arc;

        let registry = NamespaceRegistry::new();
        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&registry));

        let handles: Vec<_> = (0..8)
            .map(|thread_id| {
                let alloc = Arc::clone(&alloc);
                std::thread::spawn(move || {
                    let mut cache = WorkerCache::new(alloc);
                    let mut sids = Vec::new();
                    for i in 0..100 {
                        let iri = format!("http://thread{thread_id}.example.org/item{i}");
                        sids.push(cache.sid_for_iri(&iri));
                    }
                    sids
                })
            })
            .collect();

        let all_sids: Vec<Vec<Sid>> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Each thread's IRIs within the same prefix should get the same namespace code.
        for thread_sids in &all_sids {
            let first_code = thread_sids[0].namespace_code;
            for sid in thread_sids {
                assert_eq!(
                    sid.namespace_code, first_code,
                    "all IRIs from the same thread-specific host should share a prefix code"
                );
            }
        }

        // Different threads have different host prefixes → different codes.
        let codes: std::collections::HashSet<u16> =
            all_sids.iter().map(|sids| sids[0].namespace_code).collect();
        assert_eq!(
            codes.len(),
            8,
            "8 threads should allocate 8 distinct prefix codes"
        );
    }
}
