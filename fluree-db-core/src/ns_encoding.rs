//! Canonical namespace encoding invariants.
//!
//! This module provides the single source of truth for IRI ↔ SID encoding
//! and decoding, guaranteeing deterministic, ledger-consistent behavior
//! across all components (transact, import, query, binary index).
//!
//! ## Key types
//!
//! - [`NsSplitMode`]: Ledger-fixed configuration for how IRIs are split into
//!   `(prefix, suffix)`.
//! - [`canonical_split`]: Pure, deterministic function that splits an IRI into
//!   `(prefix, suffix)` based on the split mode alone — independent of which
//!   prefixes are currently registered.
//! - [`NamespaceCodes`]: Concrete two-way map (`prefix ↔ code`) with allocation,
//!   merge, and conflict checking.
//! - [`NsLookup`]: Read-only trait for encoding/decoding using registered
//!   namespace codes.
//!
//! ## Invariants
//!
//! 1. **Canonical split**: every full IRI maps to exactly one `(prefix, suffix)`
//!    determined by a ledger-stable split mode.
//! 2. **Exact-prefix encoding**: the `ns_code` is chosen only by looking up (or
//!    allocating) the canonical prefix — not by "best match" against other prefixes.
//! 3. **Immutable mappings**: `prefix → code` and `code → prefix` are unique and
//!    immutable once established.
//! 4. **Strict decode**: decoding a SID must fail if the namespace code is not
//!    registered (except EMPTY and OVERFLOW, which store the full IRI as the name).

use crate::namespaces::default_namespace_codes;
use crate::prefix_trie::PrefixTrie;
use crate::sid::Sid;
use fluree_vocab::namespaces::{EMPTY, OVERFLOW, USER_START};
use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

// ============================================================================
// NsSplitMode
// ============================================================================

/// Ledger-fixed configuration for how IRIs are split into `(prefix, suffix)`.
///
/// Once a ledger allocates its first user namespace code, the split mode is
/// fixed forever. All encoding paths must use the same mode to guarantee
/// deterministic SID assignment.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NsSplitMode {
    /// Split at the last `/` or `#` (for hierarchical IRIs) or last
    /// `/ | # | :` (for opaque IRIs). This is the finest-grained mode
    /// and the default for new ledgers.
    #[default]
    MostGranular,

    /// Split at `scheme://host/` plus up to **n** additional non-empty path
    /// segments. For opaque IRIs, split at `scheme:` plus the first segment
    /// plus up to **n** additional colon-delimited segments.
    ///
    /// - `HostPlusN(0)` ≈ host-only splitting
    /// - `HostPlusN(1)` ≈ host + one path segment
    HostPlusN(u8),
}

/// Maximum `n` value for `HostPlusN(n)` that can be persisted without
/// wrapping. `HostPlusN(254)` encodes as byte `0xFF`; values above 254
/// would wrap to `0x00` (MostGranular) on round-trip.
pub const HOST_PLUS_N_MAX: u8 = 254;

impl NsSplitMode {
    /// Encode to a single byte for binary persistence.
    ///
    /// Layout: `0x00` = MostGranular, `0x01..=0xFF` = HostPlusN(n-1).
    ///
    /// # Panics
    ///
    /// Returns `Err` if `n > 254` (would wrap to `MostGranular` on decode).
    pub fn to_byte(self) -> Result<u8, NsAllocError> {
        match self {
            Self::MostGranular => Ok(0),
            Self::HostPlusN(n) => {
                if n > HOST_PLUS_N_MAX {
                    return Err(NsAllocError::Overflow);
                }
                Ok(n + 1)
            }
        }
    }

    /// Decode from a single byte.
    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => Self::MostGranular,
            n => Self::HostPlusN(n - 1),
        }
    }
}

// ============================================================================
// Built-in prefix trie (cached, for HostPlusN mode only)
// ============================================================================

/// Cached prefix trie built from `default_namespace_codes()`.
///
/// Only consulted by `canonical_split` in `HostPlusN` mode to prevent
/// coarse host-based splitting from overriding well-known RDF/XSD prefixes.
/// In `MostGranular` mode, the natural `/`/`#` split already produces
/// correct boundaries for built-in namespaces.
static BUILTIN_TRIE: LazyLock<PrefixTrie> =
    LazyLock::new(|| PrefixTrie::from_namespace_codes(&default_namespace_codes()));

/// Access the cached built-in prefix trie.
pub fn builtin_prefix_trie() -> &'static PrefixTrie {
    &BUILTIN_TRIE
}

// ============================================================================
// canonical_split
// ============================================================================

/// Deterministically split an IRI into `(prefix, suffix)` based on the
/// ledger's split mode.
///
/// This function is **pure**: given the same `(iri, mode)` it always returns
/// the same result, independent of which prefixes are currently registered.
/// The result satisfies `prefix.to_owned() + suffix == iri`.
///
/// ## Built-in prefix handling
///
/// In `HostPlusN` mode, built-in prefixes (from `default_namespace_codes()`)
/// are checked first via a cached trie. If a built-in prefix matches, it wins
/// regardless of what `HostPlusN` segment counting would produce. This
/// prevents coarse splitting from decomposing well-known RDF IRIs at
/// host/path boundaries.
///
/// In `MostGranular` mode, the built-in check is skipped for normal IRIs
/// (the natural `/`/`#` split already produces correct boundaries for
/// hash-namespace IRIs like `http://www.w3.org/1999/02/22-rdf-syntax-ns#type`).
/// However, `@`-prefixed strings (JSON-LD keywords like `@type`, `@id`) always
/// check built-ins because they have no standard IRI delimiters.
pub fn canonical_split(iri: &str, mode: NsSplitMode) -> (&str, &str) {
    // Always check built-in prefixes for @-prefixed strings (JSON-LD keywords).
    // These have no `/`, `#`, or `:` delimiters, so normal splitting would
    // produce ("", "@type") → EMPTY fallback, losing the JSON_LD (code 1) prefix.
    if iri.starts_with('@') {
        if let Some((_code, prefix_len)) = BUILTIN_TRIE.longest_match(iri) {
            return (&iri[..prefix_len], &iri[prefix_len..]);
        }
    }

    // HostPlusN: check all built-in prefixes to prevent coarse splitting
    // from overriding well-known RDF/XSD prefixes.
    if let NsSplitMode::HostPlusN(_) = mode {
        if let Some((_code, prefix_len)) = BUILTIN_TRIE.longest_match(iri) {
            return (&iri[..prefix_len], &iri[prefix_len..]);
        }
    }

    // Determine if hierarchical (has "://") or opaque
    if let Some(authority_start) = find_authority_start(iri) {
        split_hierarchical(iri, mode, authority_start)
    } else {
        split_opaque(iri, mode)
    }
}

/// Find the byte offset just past `://` if present, else `None`.
#[inline]
fn find_authority_start(iri: &str) -> Option<usize> {
    let bytes = iri.as_bytes();
    // Find "://" — the authority delimiter for hierarchical IRIs
    for i in 0..bytes.len().saturating_sub(2) {
        if bytes[i] == b':' && bytes[i + 1] == b'/' && bytes[i + 2] == b'/' {
            return Some(i + 3);
        }
    }
    None
}

/// Compute query_cut: index of first `?` that appears before any `#`.
/// Returns `None` if no such `?` exists.
#[inline]
fn query_cut(iri: &str) -> Option<usize> {
    let bytes = iri.as_bytes();
    let hash_idx = bytes.iter().position(|&b| b == b'#');
    let q_idx = bytes.iter().position(|&b| b == b'?');
    match (q_idx, hash_idx) {
        (Some(q), Some(h)) if q < h => Some(q),
        (Some(q), None) => Some(q),
        _ => None,
    }
}

/// Split a hierarchical IRI (`scheme://authority/path...`).
fn split_hierarchical(iri: &str, mode: NsSplitMode, authority_start: usize) -> (&str, &str) {
    let bytes = iri.as_bytes();
    let qcut = query_cut(iri);
    let no_query_end = qcut.unwrap_or(iri.len());

    // Find end of authority (first `/` after `://`)
    let mut host_end = authority_start;
    while host_end < no_query_end && bytes[host_end] != b'/' {
        host_end += 1;
    }

    // host_root: scheme://authority (with trailing `/` if present in no_query)
    let host_root_end = if host_end < no_query_end && bytes[host_end] == b'/' {
        host_end + 1
    } else {
        host_end
    };

    match mode {
        NsSplitMode::MostGranular => {
            // Find last `/` or `#` in no_query portion
            let no_query = &bytes[..no_query_end];
            let split_pos = no_query.iter().rposition(|&b| b == b'/' || b == b'#');
            match split_pos {
                Some(pos) => (&iri[..=pos], &iri[pos + 1..]),
                None => ("", iri),
            }
        }
        NsSplitMode::HostPlusN(n) => {
            // For HostPlusN, strip fragment from head too.
            // Segment counting operates on path only (before `#` and before `?`).
            let hash_in_noquery = bytes[..no_query_end].iter().position(|&b| b == b'#');
            let head_end = hash_in_noquery.unwrap_or(no_query_end);

            if host_root_end >= head_end || !bytes[host_root_end - 1..host_root_end].contains(&b'/')
            {
                // No path after authority (or authority has no trailing `/`)
                // Prefix is just the host root
                return (&iri[..host_root_end], &iri[host_root_end..]);
            }

            // Count n non-empty path segments after host_root
            let prefix_end = extend_by_n_segments(bytes, host_root_end, head_end, n as usize);
            (&iri[..prefix_end], &iri[prefix_end..])
        }
    }
}

/// Extend a prefix from `start` by up to `n` non-empty path segments,
/// where segments are delimited by `/`.
///
/// Returns the byte offset where the prefix ends. If the k-th segment
/// is followed by a `/`, the prefix ends after that `/`. Empty segments
/// created by `//` are not counted toward the budget but their bytes are
/// included in the prefix.
fn extend_by_n_segments(bytes: &[u8], start: usize, end: usize, n: usize) -> usize {
    if n == 0 {
        return start;
    }

    let mut pos = start;
    let mut counted = 0usize;

    while pos < end && counted < n {
        // Skip runs of `/` (empty segments — not counted)
        while pos < end && bytes[pos] == b'/' {
            pos += 1;
        }
        if pos >= end {
            break;
        }
        // Consume a non-empty segment
        while pos < end && bytes[pos] != b'/' {
            pos += 1;
        }
        counted += 1;
        // If followed by `/`, include it in the prefix
        if pos < end && bytes[pos] == b'/' {
            pos += 1;
        }
    }

    pos
}

/// Split an opaque (non-hierarchical) IRI (`scheme:rest` with no `://`).
fn split_opaque(iri: &str, mode: NsSplitMode) -> (&str, &str) {
    let bytes = iri.as_bytes();
    let qcut = query_cut(iri);
    let no_query_end = qcut.unwrap_or(iri.len());

    // Find the scheme (first `:`)
    let scheme_end = match bytes.iter().position(|&b| b == b':') {
        Some(pos) => pos,
        None => {
            // No colon at all — no valid scheme. Treat entire IRI as suffix.
            return ("", iri);
        }
    };

    match mode {
        NsSplitMode::MostGranular => {
            // Find last `/ | # | :` in no_query
            let no_query = &bytes[..no_query_end];
            let split_pos = no_query
                .iter()
                .rposition(|&b| b == b'/' || b == b'#' || b == b':');
            match split_pos {
                Some(pos) => (&iri[..=pos], &iri[pos + 1..]),
                None => ("", iri),
            }
        }
        NsSplitMode::HostPlusN(n) => {
            // Split rest into colon segments: scheme:seg1:seg2:...
            // Prefix = scheme: + first segment + up to n additional colon segments + trailing ':'
            let rest_start = scheme_end + 1;
            let rest = &bytes[rest_start..no_query_end];

            // Walk colon-delimited segments, taking the first 1 + n.
            // Each segment ends at the next `:` or end of `rest`.
            let take = 1 + n as usize;
            let mut pos = 0;
            let mut counted = 0;
            let mut last_seg_end = 0;

            while counted < take {
                let seg_end = rest[pos..]
                    .iter()
                    .position(|&b| b == b':')
                    .map(|i| pos + i)
                    .unwrap_or(rest.len());
                last_seg_end = seg_end;
                counted += 1;
                if seg_end >= rest.len() {
                    break;
                }
                pos = seg_end + 1; // skip the `:`
            }

            if counted == 0 {
                // No segments after scheme — prefix is just "scheme:"
                return (&iri[..=scheme_end], &iri[scheme_end + 1..]);
            }

            // The prefix ends after the last taken segment's trailing `:`
            let abs_seg_end = rest_start + last_seg_end;
            let prefix_end = if abs_seg_end < no_query_end && bytes[abs_seg_end] == b':' {
                abs_seg_end + 1
            } else {
                abs_seg_end
            };

            (&iri[..prefix_end], &iri[prefix_end..])
        }
    }
}

// ============================================================================
// NsAllocError
// ============================================================================

/// Error returned by namespace code allocation or merge operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NsAllocError {
    /// No more allocatable namespace codes (all codes in `USER_START..OVERFLOW`
    /// are exhausted).
    Overflow,

    /// A code already maps to a different prefix than requested.
    CodeConflict {
        code: u16,
        new_prefix: String,
        existing_prefix: String,
    },

    /// A prefix already maps to a different code than requested.
    PrefixConflict {
        prefix: String,
        new_code: u16,
        existing_code: u16,
    },
}

impl fmt::Display for NsAllocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => write!(
                f,
                "namespace code overflow: all codes in {USER_START}..{OVERFLOW} are exhausted"
            ),
            Self::CodeConflict {
                code,
                new_prefix,
                existing_prefix,
            } => write!(
                f,
                "namespace conflict: code {code} maps to {existing_prefix:?} but {new_prefix:?} was requested"
            ),
            Self::PrefixConflict {
                prefix,
                new_code,
                existing_code,
            } => write!(
                f,
                "namespace conflict: prefix {prefix:?} maps to code {existing_code} but code {new_code} was requested"
            ),
        }
    }
}

impl std::error::Error for NsAllocError {}

// ============================================================================
// NamespaceCodes
// ============================================================================

/// Concrete two-way namespace map with allocation and conflict checking.
///
/// This is the single authoritative implementation of namespace code management.
/// All allocation (transact, import) and all merge (commit delta, index augment)
/// operations go through this type to enforce uniqueness invariants.
#[derive(Debug, Clone)]
pub struct NamespaceCodes {
    /// Prefix → code
    prefix_to_code: HashMap<String, u16>,
    /// Code → prefix
    code_to_prefix: HashMap<u16, String>,
    /// Next allocatable code (≥ USER_START)
    next_code: u16,
    /// New allocations since last `take_delta()` (code → prefix)
    delta: HashMap<u16, String>,
}

impl NamespaceCodes {
    /// Create a new instance seeded with the default (built-in) namespace codes.
    pub fn new() -> Self {
        // Built-in defaults are hardcoded and known to be valid.
        Self::from_code_to_prefix(default_namespace_codes())
            .expect("built-in namespace codes must be a valid bimap")
    }

    /// Create from an existing `code → prefix` map (e.g., from an index root
    /// or commit chain reconstruction).
    ///
    /// Returns `Err` if two different codes map to the same prefix (bimap
    /// violation). This indicates corrupted persisted data.
    ///
    /// The built-in defaults are NOT automatically included — the caller is
    /// expected to have merged them already if desired.
    pub fn from_code_to_prefix(code_to_prefix: HashMap<u16, String>) -> Result<Self, NsAllocError> {
        let prefix_to_code: HashMap<String, u16> = code_to_prefix
            .iter()
            .map(|(&code, prefix)| (prefix.clone(), code))
            .collect();

        // Bimap uniqueness: if two codes mapped to the same prefix, the reverse
        // map will be shorter. This indicates corrupted input data.
        if prefix_to_code.len() != code_to_prefix.len() {
            // Find a representative conflict for the error message.
            let mut seen: HashMap<&str, u16> = HashMap::new();
            for (&code, prefix) in &code_to_prefix {
                if let Some(&prev_code) = seen.get(prefix.as_str()) {
                    return Err(NsAllocError::PrefixConflict {
                        prefix: prefix.clone(),
                        new_code: code,
                        existing_code: prev_code,
                    });
                }
                seen.insert(prefix.as_str(), code);
            }
        }

        let max_code = code_to_prefix
            .keys()
            .filter(|&&c| c < OVERFLOW)
            .max()
            .copied()
            .unwrap_or(0);
        let next_code = (max_code + 1).max(USER_START);

        Ok(Self {
            prefix_to_code,
            code_to_prefix,
            next_code,
            delta: HashMap::new(),
        })
    }

    /// Look up the code for a prefix.
    #[inline]
    pub fn get_code(&self, prefix: &str) -> Option<u16> {
        self.prefix_to_code.get(prefix).copied()
    }

    /// Look up the prefix for a code.
    #[inline]
    pub fn get_prefix(&self, code: u16) -> Option<&str> {
        self.code_to_prefix
            .get(&code)
            .map(std::string::String::as_str)
    }

    /// Allocate a new code for a prefix, or return the existing code if
    /// the prefix is already registered.
    ///
    /// Returns:
    /// - `Ok(code)` — the existing or newly allocated code
    /// - `Err(NsAllocError::Overflow)` — no more allocatable codes
    /// - `Err(NsAllocError::CodeConflict)` — namespace bimap conflict (should not
    ///   happen in well-formed usage since we check prefix first)
    pub fn allocate_prefix(&mut self, prefix: &str) -> Result<u16, NsAllocError> {
        // Fast path: prefix already registered
        if let Some(&code) = self.prefix_to_code.get(prefix) {
            return Ok(code);
        }

        // Check for overflow
        if self.next_code >= OVERFLOW {
            return Err(NsAllocError::Overflow);
        }

        let code = self.next_code;
        self.next_code += 1;

        self.prefix_to_code.insert(prefix.to_string(), code);
        self.code_to_prefix.insert(code, prefix.to_string());
        self.delta.insert(code, prefix.to_string());

        Ok(code)
    }

    /// Encode an IRI to a SID, allocating a new namespace code if needed.
    ///
    /// Uses `canonical_split` to determine the prefix, then looks up or allocates
    /// the code. On overflow (all codes exhausted), returns `Sid(OVERFLOW, iri)`
    /// per the overflow rule.
    ///
    /// Returns `Err` only on a bimap conflict (corrupted state). Normal operation
    /// always returns `Ok`.
    pub fn encode_iri_or_allocate(
        &mut self,
        iri: &str,
        mode: NsSplitMode,
    ) -> Result<Sid, NsAllocError> {
        let (prefix, suffix) = canonical_split(iri, mode);
        match self.allocate_prefix(prefix) {
            Ok(code) => Ok(Sid::new(code, suffix)),
            Err(NsAllocError::Overflow) => Ok(Sid::new(OVERFLOW, iri)),
            Err(e) => Err(e),
        }
    }

    /// Merge a namespace delta (code → prefix entries) with conflict checking.
    ///
    /// Each entry must satisfy:
    /// - If `code` exists, its prefix must match
    /// - If `prefix` exists, its code must match
    /// - Otherwise the mapping is a valid extension
    ///
    /// Returns `Err(NsAllocError::CodeConflict | PrefixConflict)` on the first violation.
    pub fn merge_delta(&mut self, delta: &HashMap<u16, String>) -> Result<(), NsAllocError> {
        for (&code, prefix) in delta {
            // Check code → prefix direction
            if let Some(existing) = self.code_to_prefix.get(&code) {
                if existing != prefix {
                    return Err(NsAllocError::CodeConflict {
                        code,
                        new_prefix: prefix.clone(),
                        existing_prefix: existing.clone(),
                    });
                }
                // Already registered with matching prefix — skip
                continue;
            }

            // Check prefix → code direction
            if let Some(&existing_code) = self.prefix_to_code.get(prefix.as_str()) {
                if existing_code != code {
                    return Err(NsAllocError::PrefixConflict {
                        prefix: prefix.clone(),
                        new_code: code,
                        existing_code,
                    });
                }
                // Already registered — skip
                continue;
            }

            // New mapping — insert
            self.prefix_to_code.insert(prefix.clone(), code);
            self.code_to_prefix.insert(code, prefix.clone());
            if code >= self.next_code && code < OVERFLOW {
                self.next_code = code + 1;
            }
        }
        Ok(())
    }

    /// Like [`merge_delta`](Self::merge_delta), but also records the new
    /// entries in the persistence delta (`self.delta`) so the next
    /// `take_delta()` includes them in the commit record.
    ///
    /// Use this when adopting allocations made by *another* registry that
    /// must end up persisted in this registry's commit (e.g. SPARQL
    /// `lower_sparql_update` builds template Sids against a caller-owned
    /// `NamespaceRegistry`; the staging path adopts those allocations so the
    /// committed snapshot maps them back to IRIs for query-time lookup).
    pub fn adopt_delta_for_persistence(
        &mut self,
        delta: &HashMap<u16, String>,
    ) -> Result<(), NsAllocError> {
        for (&code, prefix) in delta {
            // Same conflict checks as merge_delta
            if let Some(existing) = self.code_to_prefix.get(&code) {
                if existing != prefix {
                    return Err(NsAllocError::CodeConflict {
                        code,
                        new_prefix: prefix.clone(),
                        existing_prefix: existing.clone(),
                    });
                }
                // Already registered with matching prefix — record in delta only
                // if this registry hasn't already persisted it.
                self.delta.entry(code).or_insert_with(|| prefix.clone());
                continue;
            }

            if let Some(&existing_code) = self.prefix_to_code.get(prefix.as_str()) {
                if existing_code != code {
                    return Err(NsAllocError::PrefixConflict {
                        prefix: prefix.clone(),
                        new_code: code,
                        existing_code,
                    });
                }
                self.delta.entry(code).or_insert_with(|| prefix.clone());
                continue;
            }

            // New mapping — insert into both lookup tables AND persistence delta
            self.prefix_to_code.insert(prefix.clone(), code);
            self.code_to_prefix.insert(code, prefix.clone());
            self.delta.insert(code, prefix.clone());
            if code >= self.next_code && code < OVERFLOW {
                self.next_code = code + 1;
            }
        }
        Ok(())
    }

    /// Take the accumulated delta (new allocations) and reset it.
    ///
    /// Returns the map of new allocations (`code → prefix`) for inclusion
    /// in the commit record.
    pub fn take_delta(&mut self) -> HashMap<u16, String> {
        std::mem::take(&mut self.delta)
    }

    /// Get a reference to the delta without consuming it.
    pub fn delta(&self) -> &HashMap<u16, String> {
        &self.delta
    }

    /// Check if there are any new allocations since the last `take_delta`.
    pub fn has_delta(&self) -> bool {
        !self.delta.is_empty()
    }

    /// Access the code → prefix map (for backwards compatibility with
    /// code that expects `HashMap<u16, String>`).
    pub fn code_to_prefix_map(&self) -> &HashMap<u16, String> {
        &self.code_to_prefix
    }

    /// Access the prefix → code map.
    pub fn prefix_to_code_map(&self) -> &HashMap<String, u16> {
        &self.prefix_to_code
    }

    /// Number of registered namespace codes.
    pub fn len(&self) -> usize {
        self.code_to_prefix.len()
    }

    /// Whether no codes are registered.
    pub fn is_empty(&self) -> bool {
        self.code_to_prefix.is_empty()
    }

    /// The next code that will be allocated.
    pub fn next_code(&self) -> u16 {
        self.next_code
    }

    /// Iterate over all registered `(code, prefix)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u16, &str)> {
        self.code_to_prefix
            .iter()
            .map(|(&code, prefix)| (code, prefix.as_str()))
    }
}

impl Default for NamespaceCodes {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// NsLookup trait
// ============================================================================

/// Read-only view of namespace code mappings.
///
/// Provides ergonomic encode/decode methods that use [`canonical_split`] for
/// deterministic IRI encoding.
pub trait NsLookup {
    /// Look up the code for a prefix.
    fn code_for_prefix(&self, prefix: &str) -> Option<u16>;

    /// Look up the prefix for a code.
    fn prefix_for_code(&self, code: u16) -> Option<&str>;

    /// Encode an IRI to a SID using canonical splitting and exact-prefix lookup.
    ///
    /// Returns `None` if the canonical prefix is not registered (callers that
    /// need to mint codes must go through [`NamespaceCodes::allocate_prefix`]).
    fn encode_iri(&self, iri: &str, mode: NsSplitMode) -> Option<Sid> {
        let (prefix, suffix) = canonical_split(iri, mode);
        let code = self.code_for_prefix(prefix)?;
        Some(Sid::new(code, suffix))
    }

    /// Decode a SID to a full IRI string.
    ///
    /// **Special codes:**
    /// - `EMPTY (0)`: returns `Some(sid.name)` — name is the full IRI
    /// - `OVERFLOW (0xFFFE)`: returns `Some(sid.name)` — full IRI stored as name
    /// - Any other unregistered code: returns `None` (corruption/bug)
    fn decode_sid_strict(&self, sid: &Sid) -> Option<String> {
        if sid.namespace_code == EMPTY || sid.namespace_code == OVERFLOW {
            return Some(sid.name.to_string());
        }
        let prefix = self.prefix_for_code(sid.namespace_code)?;
        Some(format!("{}{}", prefix, sid.name))
    }
}

impl NsLookup for NamespaceCodes {
    #[inline]
    fn code_for_prefix(&self, prefix: &str) -> Option<u16> {
        self.get_code(prefix)
    }

    #[inline]
    fn prefix_for_code(&self, code: u16) -> Option<&str> {
        self.get_prefix(code)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- NsSplitMode persistence ----

    #[test]
    fn test_split_mode_byte_roundtrip() {
        assert_eq!(
            NsSplitMode::from_byte(NsSplitMode::MostGranular.to_byte().unwrap()),
            NsSplitMode::MostGranular
        );
        assert_eq!(
            NsSplitMode::from_byte(NsSplitMode::HostPlusN(0).to_byte().unwrap()),
            NsSplitMode::HostPlusN(0)
        );
        assert_eq!(
            NsSplitMode::from_byte(NsSplitMode::HostPlusN(1).to_byte().unwrap()),
            NsSplitMode::HostPlusN(1)
        );
        assert_eq!(
            NsSplitMode::from_byte(NsSplitMode::HostPlusN(HOST_PLUS_N_MAX).to_byte().unwrap()),
            NsSplitMode::HostPlusN(HOST_PLUS_N_MAX)
        );
    }

    #[test]
    fn test_split_mode_host_plus_255_returns_err() {
        assert!(NsSplitMode::HostPlusN(255).to_byte().is_err());
    }

    // ---- canonical_split: normative test table ----

    #[test]
    fn case_01_host_only_no_path() {
        let (p, s) = canonical_split("https://example.com", NsSplitMode::HostPlusN(0));
        assert_eq!(p, "https://example.com");
        assert_eq!(s, "");
    }

    #[test]
    fn case_02_host_only_trailing_slash() {
        let (p, s) = canonical_split("https://example.com/", NsSplitMode::HostPlusN(0));
        assert_eq!(p, "https://example.com/");
        assert_eq!(s, "");
    }

    #[test]
    fn case_03_host_plus_1() {
        let (p, s) = canonical_split(
            "https://example.com/api/v1/users",
            NsSplitMode::HostPlusN(1),
        );
        assert_eq!(p, "https://example.com/api/");
        assert_eq!(s, "v1/users");
    }

    #[test]
    fn case_04_host_plus_1_trailing_slash() {
        let (p, s) = canonical_split("https://example.com/api/", NsSplitMode::HostPlusN(1));
        assert_eq!(p, "https://example.com/api/");
        assert_eq!(s, "");
    }

    #[test]
    fn case_05_double_slash_host_plus_1() {
        let (p, s) = canonical_split("https://example.com//foo/bar", NsSplitMode::HostPlusN(1));
        assert_eq!(p, "https://example.com//foo/");
        assert_eq!(s, "bar");
    }

    #[test]
    fn case_06_double_slash_with_query_fragment() {
        let (p, s) = canonical_split(
            "https://example.com//foo/bar?x=1#y",
            NsSplitMode::HostPlusN(1),
        );
        assert_eq!(p, "https://example.com//foo/");
        assert_eq!(s, "bar?x=1#y");
    }

    #[test]
    fn case_07_most_granular_with_query_fragment() {
        let (p, s) = canonical_split(
            "https://example.com/path?foo=bar#frag",
            NsSplitMode::MostGranular,
        );
        assert_eq!(p, "https://example.com/");
        assert_eq!(s, "path?foo=bar#frag");
    }

    #[test]
    fn case_08_most_granular_multi_path_with_query() {
        let (p, s) = canonical_split(
            "https://example.com/a/path?foo=bar",
            NsSplitMode::MostGranular,
        );
        assert_eq!(p, "https://example.com/a/");
        assert_eq!(s, "path?foo=bar");
    }

    #[test]
    fn case_09_most_granular_hash_namespace() {
        let (p, s) = canonical_split("https://example.com/ns#type", NsSplitMode::MostGranular);
        assert_eq!(p, "https://example.com/ns#");
        assert_eq!(s, "type");
    }

    #[test]
    fn case_10_most_granular_hash_then_query() {
        let (p, s) = canonical_split(
            "https://example.com/ns#type?unlikely=but-valid",
            NsSplitMode::MostGranular,
        );
        assert_eq!(p, "https://example.com/ns#");
        assert_eq!(s, "type?unlikely=but-valid");
    }

    #[test]
    fn case_11_opaque_urn_host_plus_0() {
        let (p, s) = canonical_split("urn:aws:iam:policy/ReadOnly", NsSplitMode::HostPlusN(0));
        assert_eq!(p, "urn:aws:");
        assert_eq!(s, "iam:policy/ReadOnly");
    }

    #[test]
    fn case_12_opaque_arn_host_plus_1() {
        let (p, s) = canonical_split(
            "arn:aws:iam::123456789012:role/Admin",
            NsSplitMode::HostPlusN(1),
        );
        assert_eq!(p, "arn:aws:iam:");
        assert_eq!(s, ":123456789012:role/Admin");
    }

    #[test]
    fn case_13_builtin_rdf_host_plus_0() {
        // Built-in prefix must win over HostPlusN(0) host-only splitting
        let (p, s) = canonical_split(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            NsSplitMode::HostPlusN(0),
        );
        assert_eq!(p, "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        assert_eq!(s, "type");
    }

    #[test]
    fn case_14_builtin_xsd_host_plus_0() {
        // Built-in prefix must win over HostPlusN(0) host-only splitting
        let (p, s) = canonical_split(
            "http://www.w3.org/2001/XMLSchema#string",
            NsSplitMode::HostPlusN(0),
        );
        assert_eq!(p, "http://www.w3.org/2001/XMLSchema#");
        assert_eq!(s, "string");
    }

    // ---- canonical_split: additional edge cases ----

    #[test]
    fn empty_iri() {
        let (p, s) = canonical_split("", NsSplitMode::MostGranular);
        assert_eq!(p, "");
        assert_eq!(s, "");
    }

    #[test]
    fn bare_scheme_only() {
        let (p, s) = canonical_split("mailto:", NsSplitMode::MostGranular);
        assert_eq!(p, "mailto:");
        assert_eq!(s, "");
    }

    #[test]
    fn no_colon_at_all() {
        let (p, s) = canonical_split("just-a-string", NsSplitMode::MostGranular);
        assert_eq!(p, "");
        assert_eq!(s, "just-a-string");
    }

    #[test]
    fn did_key_opaque_most_granular() {
        let (p, s) = canonical_split("did:key:z6Mk123", NsSplitMode::MostGranular);
        assert_eq!(p, "did:key:");
        assert_eq!(s, "z6Mk123");
    }

    #[test]
    fn did_key_opaque_host_plus_0() {
        let (p, s) = canonical_split("did:key:z6Mk123", NsSplitMode::HostPlusN(0));
        assert_eq!(p, "did:key:");
        assert_eq!(s, "z6Mk123");
    }

    #[test]
    fn blank_node() {
        let (p, s) = canonical_split("_:fdb-abc123", NsSplitMode::MostGranular);
        assert_eq!(p, "_:");
        assert_eq!(s, "fdb-abc123");
    }

    #[test]
    fn host_plus_n_fragment_not_counted_as_segment() {
        // Fragment must not affect segment counting.
        // head = "https://example.com/a" (before '#')
        // host_root = "https://example.com/"
        // Extend by 1 segment: "a" → no trailing '/' → prefix ends at head_end
        // suffix = iri[prefix_end..] = "#fragment"
        let (p, s) = canonical_split("https://example.com/a#fragment", NsSplitMode::HostPlusN(1));
        assert_eq!(p, "https://example.com/a");
        assert_eq!(s, "#fragment");
    }

    #[test]
    fn host_plus_0_with_query() {
        let (p, s) = canonical_split("https://example.com?x=1", NsSplitMode::HostPlusN(0));
        assert_eq!(p, "https://example.com");
        assert_eq!(s, "?x=1");
    }

    #[test]
    fn most_granular_urn_with_slashes() {
        // URN with mixed delimiters — MostGranular uses last / or # or :
        let (p, s) = canonical_split("urn:example:foo/bar/baz", NsSplitMode::MostGranular);
        assert_eq!(p, "urn:example:foo/bar/");
        assert_eq!(s, "baz");
    }

    // ---- JSON-LD keywords (@-prefixed built-in prefix) ----

    #[test]
    fn json_ld_at_type_most_granular() {
        // "@type" has no IRI delimiters; built-in prefix "@" (JSON_LD=1) must win
        let (p, s) = canonical_split("@type", NsSplitMode::MostGranular);
        assert_eq!(p, "@");
        assert_eq!(s, "type");
    }

    #[test]
    fn json_ld_at_id_most_granular() {
        let (p, s) = canonical_split("@id", NsSplitMode::MostGranular);
        assert_eq!(p, "@");
        assert_eq!(s, "id");
    }

    #[test]
    fn json_ld_at_value_host_plus_0() {
        let (p, s) = canonical_split("@value", NsSplitMode::HostPlusN(0));
        assert_eq!(p, "@");
        assert_eq!(s, "value");
    }

    // ---- NamespaceCodes ----

    #[test]
    fn test_namespace_codes_new_has_defaults() {
        let codes = NamespaceCodes::new();
        assert!(codes.get_code("").is_some()); // EMPTY
        assert!(codes.get_code("@").is_some()); // JSON_LD
        assert_eq!(codes.get_code(""), Some(EMPTY));
    }

    #[test]
    fn test_allocate_prefix_basic() {
        let mut codes = NamespaceCodes::new();
        let code = codes.allocate_prefix("https://example.org/").unwrap();
        assert!(code >= USER_START);
        assert_eq!(codes.get_prefix(code), Some("https://example.org/"));

        // Second allocation of same prefix returns same code
        let code2 = codes.allocate_prefix("https://example.org/").unwrap();
        assert_eq!(code, code2);

        // Delta tracks the new allocation
        assert!(codes.delta().contains_key(&code));
    }

    #[test]
    fn test_merge_delta_valid() {
        let mut codes = NamespaceCodes::new();
        let mut delta = HashMap::new();
        delta.insert(100u16, "https://new.example.org/".to_string());
        delta.insert(101u16, "https://other.example.org/".to_string());

        codes.merge_delta(&delta).unwrap();
        assert_eq!(codes.get_code("https://new.example.org/"), Some(100));
        assert_eq!(codes.get_code("https://other.example.org/"), Some(101));
    }

    #[test]
    fn test_merge_delta_conflict_code() {
        let mut codes = NamespaceCodes::new();
        let mut delta1 = HashMap::new();
        delta1.insert(100u16, "https://a.example.org/".to_string());
        codes.merge_delta(&delta1).unwrap();

        // Same code, different prefix → conflict
        let mut delta2 = HashMap::new();
        delta2.insert(100u16, "https://b.example.org/".to_string());
        assert!(codes.merge_delta(&delta2).is_err());
    }

    #[test]
    fn test_merge_delta_conflict_prefix() {
        let mut codes = NamespaceCodes::new();
        let mut delta1 = HashMap::new();
        delta1.insert(100u16, "https://a.example.org/".to_string());
        codes.merge_delta(&delta1).unwrap();

        // Same prefix, different code → conflict
        let mut delta2 = HashMap::new();
        delta2.insert(200u16, "https://a.example.org/".to_string());
        assert!(codes.merge_delta(&delta2).is_err());
    }

    #[test]
    fn test_merge_delta_idempotent() {
        let mut codes = NamespaceCodes::new();
        let mut delta = HashMap::new();
        delta.insert(100u16, "https://a.example.org/".to_string());
        codes.merge_delta(&delta).unwrap();

        // Same mapping again — should be fine
        codes.merge_delta(&delta).unwrap();
        assert_eq!(codes.get_code("https://a.example.org/"), Some(100));
    }

    #[test]
    fn test_take_delta() {
        let mut codes = NamespaceCodes::new();
        codes.allocate_prefix("https://new.example.org/").unwrap();
        assert!(codes.has_delta());

        let delta = codes.take_delta();
        assert!(!delta.is_empty());
        assert!(!codes.has_delta());
    }

    // ---- adopt_delta_for_persistence ----
    //
    // Distinct from merge_delta: also records adopted entries in
    // `self.delta` so the next take_delta()/commit captures them.
    // Used by `stage_transaction_from_txn` to persist namespace
    // allocations made by an upstream registry (e.g. SPARQL lowering).

    #[test]
    fn test_adopt_delta_records_new_mapping_in_delta() {
        let mut codes = NamespaceCodes::new();
        let next_code_before = codes.next_code;

        let mut delta = HashMap::new();
        delta.insert(next_code_before, "https://new.example.org/".to_string());
        codes.adopt_delta_for_persistence(&delta).unwrap();

        // Lookup tables updated
        assert_eq!(
            codes.get_code("https://new.example.org/"),
            Some(next_code_before)
        );
        assert_eq!(
            codes.get_prefix(next_code_before),
            Some("https://new.example.org/")
        );

        // Persistence delta captured the new entry — this is the contract
        // that distinguishes adopt_delta_for_persistence from merge_delta.
        assert!(codes.has_delta());
        let taken = codes.take_delta();
        assert_eq!(
            taken.get(&next_code_before).map(String::as_str),
            Some("https://new.example.org/")
        );
    }

    #[test]
    fn test_adopt_delta_advances_next_code() {
        let mut codes = NamespaceCodes::new();
        let mut delta = HashMap::new();
        // Adopt a code well above next_code; next_code must jump past it
        // so subsequent allocations don't collide.
        delta.insert(200u16, "https://gap.example.org/".to_string());
        codes.adopt_delta_for_persistence(&delta).unwrap();

        assert_eq!(codes.next_code, 201);

        // The next allocate_prefix should land at 201.
        let new_code = codes.allocate_prefix("https://after.example.org/").unwrap();
        assert_eq!(new_code, 201);
    }

    #[test]
    fn test_adopt_delta_idempotent_for_same_mapping() {
        let mut codes = NamespaceCodes::new();
        let mut delta = HashMap::new();
        delta.insert(150u16, "https://idem.example.org/".to_string());

        codes.adopt_delta_for_persistence(&delta).unwrap();
        // Second adopt of the same mapping must succeed and not duplicate
        // entries or change next_code.
        let next_code_after_first = codes.next_code;
        codes.adopt_delta_for_persistence(&delta).unwrap();
        assert_eq!(codes.next_code, next_code_after_first);
        assert_eq!(codes.get_code("https://idem.example.org/"), Some(150));
    }

    #[test]
    fn test_adopt_delta_rejects_code_conflict() {
        let mut codes = NamespaceCodes::new();
        let mut first = HashMap::new();
        first.insert(170u16, "https://first.example.org/".to_string());
        codes.adopt_delta_for_persistence(&first).unwrap();

        // Same code, different prefix → CodeConflict.
        let mut conflicting = HashMap::new();
        conflicting.insert(170u16, "https://second.example.org/".to_string());
        let err = codes
            .adopt_delta_for_persistence(&conflicting)
            .expect_err("must reject");
        match err {
            NsAllocError::CodeConflict {
                code,
                new_prefix,
                existing_prefix,
            } => {
                assert_eq!(code, 170);
                assert_eq!(new_prefix, "https://second.example.org/");
                assert_eq!(existing_prefix, "https://first.example.org/");
            }
            other => panic!("expected CodeConflict, got {other:?}"),
        }
    }

    #[test]
    fn test_adopt_delta_rejects_prefix_conflict() {
        let mut codes = NamespaceCodes::new();
        let mut first = HashMap::new();
        first.insert(180u16, "https://shared.example.org/".to_string());
        codes.adopt_delta_for_persistence(&first).unwrap();

        // Same prefix, different code → PrefixConflict.
        let mut conflicting = HashMap::new();
        conflicting.insert(181u16, "https://shared.example.org/".to_string());
        let err = codes
            .adopt_delta_for_persistence(&conflicting)
            .expect_err("must reject");
        match err {
            NsAllocError::PrefixConflict {
                prefix,
                new_code,
                existing_code,
            } => {
                assert_eq!(prefix, "https://shared.example.org/");
                assert_eq!(new_code, 181);
                assert_eq!(existing_code, 180);
            }
            other => panic!("expected PrefixConflict, got {other:?}"),
        }
    }

    #[test]
    fn test_adopt_delta_records_existing_mapping_in_delta_when_absent() {
        // Pre-existing mapping (e.g. a default that won't otherwise be in
        // delta): adopt_delta_for_persistence should still record it so the
        // commit captures the use site. Avoids losing the binding when the
        // staging registry inherits from a snapshot but the lowering
        // registry independently learned of the same mapping.
        let mut codes = NamespaceCodes::new();
        let prefix = "https://shared.example.org/";
        let code = codes.allocate_prefix(prefix).unwrap();
        // Drain the delta so we can observe whether adopt re-records it.
        codes.take_delta();
        assert!(!codes.has_delta());

        let mut delta = HashMap::new();
        delta.insert(code, prefix.to_string());
        codes.adopt_delta_for_persistence(&delta).unwrap();

        let taken = codes.take_delta();
        assert_eq!(taken.get(&code).map(String::as_str), Some(prefix));
    }

    // ---- NsLookup trait ----

    #[test]
    fn test_ns_lookup_encode_decode() {
        let mut codes = NamespaceCodes::new();
        codes.allocate_prefix("https://example.org/").unwrap();

        let sid = codes
            .encode_iri("https://example.org/Alice", NsSplitMode::MostGranular)
            .unwrap();
        assert_eq!(sid.name.as_ref(), "Alice");

        let decoded = codes.decode_sid_strict(&sid).unwrap();
        assert_eq!(decoded, "https://example.org/Alice");
    }

    #[test]
    fn test_ns_lookup_decode_empty() {
        let codes = NamespaceCodes::new();
        let sid = Sid::new(EMPTY, "some-bare-string");
        let decoded = codes.decode_sid_strict(&sid).unwrap();
        assert_eq!(decoded, "some-bare-string");
    }

    #[test]
    fn test_ns_lookup_decode_overflow() {
        let codes = NamespaceCodes::new();
        let sid = Sid::new(OVERFLOW, "https://full-iri.example.org/thing");
        let decoded = codes.decode_sid_strict(&sid).unwrap();
        assert_eq!(decoded, "https://full-iri.example.org/thing");
    }

    #[test]
    fn test_ns_lookup_decode_unknown_code() {
        let codes = NamespaceCodes::new();
        let sid = Sid::new(9999, "suffix");
        assert!(codes.decode_sid_strict(&sid).is_none());
    }

    #[test]
    fn test_ns_lookup_encode_unregistered() {
        let codes = NamespaceCodes::new();
        // This prefix is not registered
        let result = codes.encode_iri(
            "https://unknown.example.org/thing",
            NsSplitMode::MostGranular,
        );
        assert!(result.is_none());
    }

    // ---- Round-trip: canonical_split → encode → decode ----

    #[test]
    fn test_roundtrip_most_granular() {
        let mut codes = NamespaceCodes::new();
        let iris = &[
            "https://example.com/api/v1/users",
            "https://example.com/ns#type",
            "urn:aws:iam:policy/ReadOnly",
            "did:key:z6Mk123",
            "_:fdb-abc123",
        ];
        for &iri in iris {
            let (prefix, suffix) = canonical_split(iri, NsSplitMode::MostGranular);
            assert_eq!(
                format!("{prefix}{suffix}"),
                iri,
                "canonical_split round-trip failed for {:?}",
                iri
            );

            // Allocate and encode
            codes.allocate_prefix(prefix).unwrap();
            let sid = codes
                .encode_iri(iri, NsSplitMode::MostGranular)
                .expect("encode must succeed after allocate");
            let decoded = codes.decode_sid_strict(&sid).expect("decode must succeed");
            assert_eq!(decoded, iri, "encode/decode round-trip failed for {iri:?}");
        }
    }

    #[test]
    fn test_roundtrip_host_plus_0() {
        let mut codes = NamespaceCodes::new();
        let iris = &[
            "https://example.com",
            "https://example.com/",
            "https://example.com/api/v1/users",
            "https://example.com?x=1",
            "urn:aws:iam:policy/ReadOnly",
        ];
        for &iri in iris {
            let (prefix, suffix) = canonical_split(iri, NsSplitMode::HostPlusN(0));
            assert_eq!(
                format!("{prefix}{suffix}"),
                iri,
                "canonical_split round-trip failed for {:?}",
                iri
            );

            let _ = codes.allocate_prefix(prefix);
            let sid = codes
                .encode_iri(iri, NsSplitMode::HostPlusN(0))
                .expect("encode must succeed after allocate");
            let decoded = codes.decode_sid_strict(&sid).expect("decode must succeed");
            assert_eq!(decoded, iri, "encode/decode round-trip failed for {iri:?}");
        }
    }

    #[test]
    fn test_roundtrip_host_plus_1() {
        let mut codes = NamespaceCodes::new();
        let iris = &[
            "https://example.com/api/v1/users",
            "https://example.com/api/",
            "https://example.com//foo/bar",
            "https://example.com//foo/bar?x=1#y",
            "arn:aws:iam::123456789012:role/Admin",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", // built-in
            "http://www.w3.org/2001/XMLSchema#string",         // built-in
            "@type",                                           // JSON-LD keyword
        ];
        for &iri in iris {
            let (prefix, suffix) = canonical_split(iri, NsSplitMode::HostPlusN(1));
            assert_eq!(
                format!("{prefix}{suffix}"),
                iri,
                "canonical_split round-trip failed for {:?}",
                iri
            );

            let _ = codes.allocate_prefix(prefix);
            let sid = codes
                .encode_iri(iri, NsSplitMode::HostPlusN(1))
                .expect("encode must succeed after allocate");
            let decoded = codes.decode_sid_strict(&sid).expect("decode must succeed");
            assert_eq!(decoded, iri, "encode/decode round-trip failed for {iri:?}");
        }
    }
}
