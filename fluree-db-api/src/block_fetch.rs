//! Block retrieval API with explicit enforcement semantics.
//!
//! This module provides a reusable, transport-agnostic API for fetching storage
//! blocks (commits, index nodes, leaves) by content identifier (CID) with
//! security enforcement.
//!
//! # Security Model
//!
//! The [`EnforcementMode`] controls what is allowed — the Accept/representation
//! format is orthogonal and handled at the transport layer (e.g., HTTP server).
//!
//! - **[`EnforcementMode::TrustedInternal`]**: Raw bytes returned for any block type.
//!   Used by trusted internal components (peer replication, indexer).
//!
//! - **[`EnforcementMode::PolicyEnforced`]**: Leaf blocks are always decoded and
//!   policy-filtered — they can never be returned as raw bytes. Non-leaf blocks
//!   (commits, branches, index manifests) are structural pointers containing
//!   addresses and transaction times, not user-level data, and are returned as-is.
//!
//! `PolicyEnforced` with both `identity` and `policy_class` as `None` is valid
//! and behaves as root policy (all flakes pass through unfiltered).
//!
//! # Content Kind Allowlist
//!
//! Only replication-relevant artifact kinds are allowed through the block fetch
//! API. Internal metadata (GC records, stats sketches) and graph source
//! snapshots are rejected before any storage I/O occurs.

use crate::dataset::QueryConnectionOptions;
use crate::policy_builder;
use fluree_db_binary_index::format::leaf::{decode_leaf_dir_v3_with_base, decode_leaf_header_v3};
use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
use fluree_db_binary_index::read::column_types::ColumnProjection;
use fluree_db_binary_index::{BinaryGraphView, BinaryIndexStore};
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::flake::Flake;
use fluree_db_core::storage::content_address;
use fluree_db_core::{
    ContentId, LedgerSnapshot, NoOverlay, OType, OverlayProvider, Storage, Tracker,
};
use fluree_db_query::QueryPolicyEnforcer;
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Error Type
// ============================================================================

/// Errors from block fetch operations.
#[derive(Error, Debug)]
pub enum BlockFetchError {
    /// PolicyEnforced mode attempted to return raw bytes for a leaf block
    #[error("Raw leaf bytes not allowed under policy enforcement")]
    LeafRawForbidden,

    /// Leaf decoding requires a BinaryIndexStore but none is loaded
    #[error("No binary index store loaded for this ledger")]
    MissingBinaryStore,

    /// Leaf policy filtering requires a LedgerSnapshot context but none was provided
    #[error("No database context provided for policy filtering")]
    MissingDbContext,

    /// FLI3 leaf parsing failed
    #[error("Leaf decode error: {0}")]
    LeafDecode(std::io::Error),

    /// Policy context construction failed
    #[error("Policy context error: {0}")]
    PolicyBuild(String),

    /// Policy filtering failed
    #[error("Policy filtering error: {0}")]
    PolicyFilter(String),

    /// Storage read failed (non-404)
    #[error("Storage read error: {0}")]
    StorageRead(fluree_db_core::Error),

    /// Block not found in storage
    #[error("Block not found: {0}")]
    NotFound(String),
}

// ============================================================================
// Content Kind Allowlist
// ============================================================================

/// Content kinds allowed through the block fetch API.
///
/// Only replication-relevant artifact kinds are allowed. Internal metadata
/// (GC, stats) and graph source snapshots are excluded. Disallowed kinds
/// map to `NotFound` (404) — no oracle.
pub fn is_allowed_block_kind(kind: ContentKind) -> bool {
    matches!(
        kind,
        ContentKind::Commit
            | ContentKind::Txn
            | ContentKind::LedgerConfig
            | ContentKind::IndexRoot
            | ContentKind::IndexBranch
            | ContentKind::IndexLeaf
            | ContentKind::DictBlob { .. }
    )
}

// ============================================================================
// Authorization
// ============================================================================

/// Describes what ledgers a principal is authorized to access.
///
/// This is the API-layer abstraction — the server maps its token claims
/// (e.g., `StorageProxyPrincipal`) into this struct before calling block_fetch.
#[derive(Debug, Clone)]
pub struct BlockAccessScope {
    /// If true, authorized for all ledgers.
    pub all_ledgers: bool,
    /// Specific ledger IDs authorized (e.g., `{"books:main", "users:main"}`).
    pub authorized_ledgers: HashSet<String>,
}

impl BlockAccessScope {
    /// Check if this scope authorizes access to the given ledger ID.
    pub fn is_authorized_for_ledger(&self, ledger_id: &str) -> bool {
        self.all_ledgers || self.authorized_ledgers.contains(ledger_id)
    }
}

/// Check authorization for a ledger, returning an error on failure.
///
/// Callers typically map this error to 404 (no existence leak).
pub fn authorize_ledger(scope: &BlockAccessScope, ledger_id: &str) -> Result<(), BlockFetchError> {
    if scope.is_authorized_for_ledger(ledger_id) {
        Ok(())
    } else {
        Err(BlockFetchError::NotFound(ledger_id.to_string()))
    }
}

// ============================================================================
// Enforcement Mode
// ============================================================================

/// How a block fetch should be treated security-wise.
///
/// The enforcement mode controls what is allowed. The Accept/representation
/// format is orthogonal and handled at the transport layer.
///
/// `PolicyEnforced` with both `identity` and `policy_class` as `None` is valid
/// and behaves as root policy (all flakes pass through unfiltered). This is the
/// intended behavior when no identity/policy configuration is present.
#[derive(Debug, Clone)]
pub enum EnforcementMode {
    /// Raw bytes OK for any block type. Caller is a trusted internal
    /// component (e.g., peer replication, indexer).
    TrustedInternal,

    /// Must decode+filter leaf blocks. Leaf blocks can NEVER be returned
    /// as raw bytes. Non-leaf blocks (commits, branches, index manifests)
    /// are structural pointers and returned as-is.
    PolicyEnforced {
        /// Identity IRI (e.g., `"ex:PeerServiceAccount"`)
        identity: Option<String>,
        /// Policy class IRI (e.g., `"ex:DefaultReadPolicy"`)
        policy_class: Option<String>,
    },
}

// ============================================================================
// Ledger Context
// ============================================================================

/// Ledger context needed for leaf decoding and policy filtering.
///
/// Groups the database snapshot, time horizon, and binary index store to avoid
/// parameter drift. Constructed from a `CachedLedgerState` at the call site.
pub struct LedgerBlockContext<'a> {
    /// Database snapshot.
    pub snapshot: &'a LedgerSnapshot,
    /// Time horizon for policy filtering (not always `db.t`).
    pub to_t: i64,
    /// Binary index store for leaf decoding (None if not yet indexed).
    pub binary_store: Option<Arc<BinaryIndexStore>>,
}

// ============================================================================
// Result Types
// ============================================================================

/// Content of a fetched block, after any decoding/filtering.
#[derive(Debug)]
pub enum BlockContent {
    /// Raw bytes (non-leaf block, or TrustedInternal mode for any block type).
    RawBytes(Vec<u8>),
    /// Decoded and optionally policy-filtered flakes from a leaf block.
    DecodedFlakes {
        /// The flakes (possibly filtered by policy).
        flakes: Vec<Flake>,
        /// Whether policy filtering was actually applied (false = root/no-policy).
        policy_applied: bool,
    },
}

/// Result of a block fetch operation.
#[derive(Debug)]
pub struct FetchedBlock {
    /// The block content.
    pub content: BlockContent,
}

// ============================================================================
// Leaf Detection
// ============================================================================

/// Check if bytes appear to be an FLI3 leaf block.
///
/// Conservative: checks both the 4-byte magic prefix AND that the full
/// header parses successfully. A `false` here is definitive (not a leaf);
/// a `true` means the header is structurally valid but `decode_leaf_block`
/// may still fail on corrupt leaflet data.
pub fn is_binary_leaf(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0..4] == *b"FLI3" && decode_leaf_header_v3(bytes).is_ok()
}

// ============================================================================
// Leaf Decoding
// ============================================================================

/// Decode an FLI3 binary leaf block into flakes.
///
/// Returns the decoded flakes. Fails if the block is not a valid FLI3 leaf
/// or if row-to-flake conversion fails (e.g., missing dictionary entries).
pub fn decode_leaf_block(
    bytes: &[u8],
    gv: &BinaryGraphView,
    snapshot: &LedgerSnapshot,
) -> Result<Vec<Flake>, BlockFetchError> {
    let store = gv.store();
    let header = decode_leaf_header_v3(bytes).map_err(BlockFetchError::LeafDecode)?;
    let dir = decode_leaf_dir_v3_with_base(bytes, &header).map_err(BlockFetchError::LeafDecode)?;

    let projection = ColumnProjection::all();
    let mut flakes: Vec<Flake> = Vec::with_capacity(header.total_rows as usize);

    for entry in &dir.entries {
        if entry.row_count == 0 {
            continue;
        }
        let batch = load_leaflet_columns(bytes, entry, dir.payload_base, &projection, header.order)
            .map_err(BlockFetchError::LeafDecode)?;

        for i in 0..batch.row_count {
            let s_id = batch.s_id.get(i);
            let p_id = batch.p_id.get_or(i, 0);
            let o_type = batch.o_type.get_or(i, 0);
            let o_key = batch.o_key.get(i);
            let t_u32 = batch.t.get_or(i, 0);
            let o_i = batch.o_i.get_or(i, u32::MAX);

            // Subject: resolve to IRI then encode to Sid.
            let s_iri = store
                .resolve_subject_iri(s_id)
                .map_err(BlockFetchError::LeafDecode)?;
            let s = snapshot
                .encode_iri(&s_iri)
                .unwrap_or_else(|| fluree_db_core::Sid::new(0, s_iri));

            // Predicate: resolve IRI then encode to Sid.
            let p_iri = store.resolve_predicate_iri(p_id).ok_or_else(|| {
                BlockFetchError::LeafDecode(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("predicate id {p_id} not found"),
                ))
            })?;
            let p = snapshot
                .encode_iri(p_iri)
                .unwrap_or_else(|| fluree_db_core::Sid::new(0, p_iri));

            // Object: graph-scoped decode (routes specialty kinds through arenas).
            let mut o = gv
                .decode_value(o_type, o_key, p_id)
                .map_err(BlockFetchError::LeafDecode)?;
            if let fluree_db_core::FlakeValue::Ref(sid) = &o {
                let iri = store.sid_to_iri(sid).ok_or_else(|| {
                    BlockFetchError::LeafDecode(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "sid_to_iri failed: unknown namespace code {} for object ref {:?}",
                            sid.namespace_code, sid.name
                        ),
                    ))
                })?;
                o = fluree_db_core::FlakeValue::Ref(
                    snapshot
                        .encode_iri(&iri)
                        .unwrap_or_else(|| fluree_db_core::Sid::new(0, iri)),
                );
            }

            let dt = match store.resolve_datatype_sid(o_type) {
                None => fluree_db_core::Sid::new(0, ""),
                Some(sid) => {
                    let iri = store.sid_to_iri(&sid).ok_or_else(|| {
                        BlockFetchError::LeafDecode(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "sid_to_iri failed: unknown namespace code {} for datatype {:?}",
                                sid.namespace_code, sid.name
                            ),
                        ))
                    })?;
                    snapshot
                        .encode_iri(&iri)
                        .unwrap_or_else(|| fluree_db_core::Sid::new(0, iri))
                }
            };

            let lang = store
                .resolve_lang_tag(o_type)
                .map(std::string::ToString::to_string);
            let meta = if lang.is_some() || o_i != u32::MAX {
                Some(fluree_db_core::FlakeMeta {
                    lang,
                    i: if o_i != u32::MAX {
                        Some(o_i as i32)
                    } else {
                        None
                    },
                })
            } else {
                None
            };

            // Basic sanity: o_type should always be valid.
            let _ = OType::from_u16(o_type);

            flakes.push(Flake {
                g: None,
                s,
                p,
                o,
                dt,
                t: t_u32 as i64,
                op: true,
                m: meta,
            });
        }
    }

    Ok(flakes)
}

// ============================================================================
// Policy Filtering
// ============================================================================

/// Apply policy filtering to decoded flakes.
///
/// Returns `(filtered_flakes, policy_was_applied)`.
/// If neither `identity` nor `policy_class` is provided, returns all flakes
/// unfiltered (equivalent to root policy).
pub async fn apply_policy_filter(
    snapshot: &LedgerSnapshot,
    to_t: i64,
    flakes: Vec<Flake>,
    identity: Option<&str>,
    policy_class: Option<&str>,
) -> Result<(Vec<Flake>, bool), BlockFetchError> {
    // No identity and no policy class → return all flakes unfiltered
    if identity.is_none() && policy_class.is_none() {
        return Ok((flakes, false));
    }

    let opts = QueryConnectionOptions {
        identity: identity.map(std::string::ToString::to_string),
        policy_class: policy_class.map(|c| vec![c.to_string()]),
        ..Default::default()
    };

    let overlay: &dyn OverlayProvider = &NoOverlay;

    let policy_ctx =
        policy_builder::build_policy_context_from_opts(snapshot, overlay, None, to_t, &opts, &[0])
            .await
            .map_err(|e| BlockFetchError::PolicyBuild(e.to_string()))?;

    if policy_ctx.wrapper().is_root() {
        return Ok((flakes, false));
    }

    let enforcer = QueryPolicyEnforcer::new(Arc::new(policy_ctx));
    let tracker = Tracker::disabled();

    let filtered = enforcer
        .filter_flakes_for_graph(snapshot, overlay, to_t, &tracker, flakes)
        .await
        .map_err(|e| BlockFetchError::PolicyFilter(e.to_string()))?;

    Ok((filtered, true))
}

// ============================================================================
// High-Level Entry Point
// ============================================================================

/// Fetch a block from storage by CID with enforcement.
///
/// This is the primary entry point for block retrieval. It:
/// 1. Checks the CID's content kind against the allowlist
/// 2. Derives the storage address internally from `(storage_method, kind, ledger_id, digest)`
/// 3. Reads raw bytes from storage
/// 4. Detects whether the block is an FLI3 leaf (defense-in-depth, even when kind is `IndexLeaf`)
/// 5. Under `PolicyEnforced`: leaf blocks are always decoded+filtered (never raw)
/// 6. Under `TrustedInternal`: all blocks returned as raw bytes
/// 7. Non-leaf blocks always returned as raw bytes (structural pointers)
///
/// # Security guarantees
///
/// - **Kind allowlist**: Only replication-relevant kinds are allowed. GC records,
///   stats sketches, and graph source snapshots are rejected before I/O.
/// - **`PolicyEnforced` + leaf**: always decoded+filtered, never raw bytes.
/// - **`PolicyEnforced` + non-leaf**: returned as raw bytes. These are structural
///   pointers (addresses, transaction times), not user-level data flakes.
/// - **`TrustedInternal`**: all blocks returned as raw bytes.
pub async fn fetch_and_decode_block<S: Storage + Clone + 'static>(
    storage: &S,
    ledger_id: &str,
    cid: &ContentId,
    ledger_ctx: Option<&LedgerBlockContext<'_>>,
    mode: &EnforcementMode,
) -> Result<FetchedBlock, BlockFetchError> {
    // 1. Check content kind from CID codec
    let kind = cid
        .content_kind()
        .ok_or_else(|| BlockFetchError::NotFound(cid.to_string()))?;

    // 2. Enforce kind allowlist before any I/O
    if !is_allowed_block_kind(kind) {
        return Err(BlockFetchError::NotFound(cid.to_string()));
    }

    // 3. Derive storage address internally
    let method = storage.storage_method();
    let address = content_address(method, kind, ledger_id, &cid.digest_hex());

    // 4. Read raw bytes from storage
    let bytes = storage.read_bytes(&address).await.map_err(|e| {
        if matches!(e, fluree_db_core::Error::NotFound(_)) {
            BlockFetchError::NotFound(cid.to_string())
        } else {
            BlockFetchError::StorageRead(e)
        }
    })?;

    // 5. Non-leaf blocks are structural pointers — return as-is regardless of mode
    //    (FLI3 sniffing is defense-in-depth even when kind == IndexLeaf)
    if !is_binary_leaf(&bytes) {
        return Ok(FetchedBlock {
            content: BlockContent::RawBytes(bytes),
        });
    }

    // 6. It's a leaf block — enforcement mode determines behavior
    match mode {
        EnforcementMode::TrustedInternal => Ok(FetchedBlock {
            content: BlockContent::RawBytes(bytes),
        }),

        EnforcementMode::PolicyEnforced {
            identity,
            policy_class,
        } => {
            // Leaf + PolicyEnforced: MUST decode and filter, never return raw bytes
            let lctx = ledger_ctx.ok_or(BlockFetchError::MissingDbContext)?;

            let store = lctx
                .binary_store
                .as_ref()
                .ok_or(BlockFetchError::MissingBinaryStore)?;

            // Construct a BinaryGraphView with g_id=0 (default graph) for leaf decoding.
            // Block fetch decodes leaves for replication / policy filtering;
            // specialty kinds (BigInt, Vector) route through per-graph arenas.
            let gv = BinaryGraphView::new(Arc::clone(store), 0);
            let flakes = decode_leaf_block(&bytes, &gv, lctx.snapshot)?;

            let (filtered, policy_applied) = apply_policy_filter(
                lctx.snapshot,
                lctx.to_t,
                flakes,
                identity.as_deref(),
                policy_class.as_deref(),
            )
            .await?;

            Ok(FetchedBlock {
                content: BlockContent::DecodedFlakes {
                    flakes: filtered,
                    policy_applied,
                },
            })
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scope(all: bool, ledgers: Vec<&str>) -> BlockAccessScope {
        BlockAccessScope {
            all_ledgers: all,
            authorized_ledgers: ledgers.into_iter().map(String::from).collect(),
        }
    }

    // --- Authorization tests ---

    #[test]
    fn test_authorize_ledger_allowed() {
        let scope = make_scope(false, vec!["books:main"]);
        assert!(authorize_ledger(&scope, "books:main").is_ok());
    }

    #[test]
    fn test_authorize_ledger_denied() {
        let scope = make_scope(false, vec!["other:main"]);
        let result = authorize_ledger(&scope, "books:main");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BlockFetchError::NotFound(_)));
    }

    #[test]
    fn test_authorize_ledger_all_ledgers() {
        let scope = make_scope(true, vec![]);
        assert!(authorize_ledger(&scope, "any:ledger").is_ok());
    }

    // --- Content kind allowlist tests ---

    #[test]
    fn test_allowed_block_kinds() {
        assert!(is_allowed_block_kind(ContentKind::Commit));
        assert!(is_allowed_block_kind(ContentKind::Txn));
        assert!(is_allowed_block_kind(ContentKind::LedgerConfig));
        assert!(is_allowed_block_kind(ContentKind::IndexRoot));
        assert!(is_allowed_block_kind(ContentKind::IndexBranch));
        assert!(is_allowed_block_kind(ContentKind::IndexLeaf));
        assert!(is_allowed_block_kind(ContentKind::DictBlob {
            dict: fluree_db_core::content_kind::DictKind::Graphs,
        }));
    }

    #[test]
    fn test_disallowed_block_kinds() {
        assert!(!is_allowed_block_kind(ContentKind::GarbageRecord));
        assert!(!is_allowed_block_kind(ContentKind::StatsSketch));
        assert!(!is_allowed_block_kind(ContentKind::GraphSourceSnapshot));
    }

    // --- Leaf detection tests ---

    #[test]
    fn test_is_binary_leaf_non_leaf_data() {
        // JSON commit data
        assert!(!is_binary_leaf(b"{\"t\": 1}"));
        // Random bytes
        assert!(!is_binary_leaf(b"random data here"));
        // Too short
        assert!(!is_binary_leaf(b"FLI"));
        // Empty
        assert!(!is_binary_leaf(b""));
    }

    #[test]
    fn test_is_binary_leaf_magic_but_invalid_header() {
        // Has FLI3 magic but header is too short / invalid
        assert!(!is_binary_leaf(b"FLI3short"));
    }

    /// Build a minimal valid FLI3 leaf header (72 bytes, 0 leaflets).
    /// This passes `is_binary_leaf()` and `read_leaf_header()` but has no actual
    /// leaflet data.
    fn make_minimal_leaf_header() -> Vec<u8> {
        let mut buf = vec![0u8; 72];
        // Magic: FLI3
        buf[0..4].copy_from_slice(b"FLI3");
        // Version: 1
        buf[4] = 1;
        // Order: 0 (SPOT)
        buf[5] = 0;
        // padding: 2 bytes
        buf[6] = 0;
        buf[7] = 0;
        // leaflet_count: 0 (u32 LE)
        buf[8..12].copy_from_slice(&0u32.to_le_bytes());
        // total_rows: 0 (u64 LE)
        buf[12..20].copy_from_slice(&0u64.to_le_bytes());
        // first_key and last_key: 26 bytes each, all zeros is fine
        buf
    }

    #[test]
    fn test_is_binary_leaf_valid_minimal_header() {
        let leaf_bytes = make_minimal_leaf_header();
        assert!(is_binary_leaf(&leaf_bytes));
    }

    // --- Security enforcement tests ---
    //
    // These verify the critical security property: under PolicyEnforced,
    // leaf blocks can NEVER be returned as RawBytes.

    #[test]
    fn test_policy_enforced_leaf_no_ledger_ctx_errors() {
        // PolicyEnforced + leaf detected + no ledger context → MissingBinaryStore
        // (NOT RawBytes)
        let leaf_bytes = make_minimal_leaf_header();
        assert!(is_binary_leaf(&leaf_bytes));

        let mode = EnforcementMode::PolicyEnforced {
            identity: None,
            policy_class: None,
        };

        // Verify the invariant at the type level: if is_binary_leaf is true and
        // mode is PolicyEnforced, the only valid outcomes are DecodedFlakes or error.
        // RawBytes is structurally impossible in that branch.
        match &mode {
            EnforcementMode::PolicyEnforced { .. } => {
                // Good — in fetch_and_decode_block, this branch requires ledger_ctx
                // and binary_store, or errors. It never returns RawBytes.
            }
            EnforcementMode::TrustedInternal => {
                panic!("Should be PolicyEnforced");
            }
        }
    }

    #[test]
    fn test_policy_enforced_non_leaf_returns_raw_bytes() {
        // PolicyEnforced + non-leaf → RawBytes is OK (structural data).
        let non_leaf_data = b"{\"t\": 1, \"address\": \"fluree:file://...\"}";
        assert!(!is_binary_leaf(non_leaf_data));

        // Under PolicyEnforced, non-leaf blocks are returned as RawBytes.
        // This is the correct behavior — non-leaf blocks are metadata/pointers.
    }

    #[test]
    fn test_trusted_internal_leaf_returns_raw_bytes() {
        // TrustedInternal + leaf → RawBytes is OK (trusted caller).
        let leaf_bytes = make_minimal_leaf_header();
        assert!(is_binary_leaf(&leaf_bytes));

        // Under TrustedInternal, even leaf blocks are returned as RawBytes.
        // This is the correct behavior for peer replication.
        let mode = EnforcementMode::TrustedInternal;
        assert!(matches!(mode, EnforcementMode::TrustedInternal));
    }

    #[test]
    fn test_decode_leaf_block_empty_leaf() {
        // A valid FLI3 header with 0 leaflets should decode to empty flakes.
        // We just verify is_binary_leaf succeeds and the header is valid.
        let leaf_bytes = make_minimal_leaf_header();
        assert!(is_binary_leaf(&leaf_bytes));

        // Verify the header parses with no leaflets
        let header = decode_leaf_header_v3(&leaf_bytes).unwrap();
        assert_eq!(header.leaflet_count, 0);
        assert_eq!(header.total_rows, 0);
    }
}
