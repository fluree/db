//! Commit transfer: push, export, and import of commit v2 blobs.
//!
//! ## Push (client → server)
//!
//! Clients submit locally-written commit v2 blobs. The server validates them
//! against the current ledger state (sequencing, retraction invariants, policy,
//! SHACL), writes the commit bytes to storage, and advances `CommitHead` via CAS.
//!
//! Key invariants:
//! - The first commit's `t` MUST equal server `next_t` (strict sequencing).
//! - Retractions MUST target facts currently asserted at that point in the batch.
//! - List retractions require exact metadata match (no hydration).
//! - Commit bytes are stored verbatim; the server does not rebuild commits.
//!
//! ## Export (server → client)
//!
//! Paginated export of commit blobs using address-cursor pagination.
//! Pages walk backward via `parents` — O(limit) per page regardless of
//! ledger size. Used by pull and clone operations.

use crate::dataset::QueryConnectionOptions;
use crate::error::{ApiError, Result};
use crate::policy_builder::build_policy_context_from_opts;
use crate::tx::{IndexingMode, IndexingStatus};
use crate::{Fluree, IndexConfig, LedgerHandle};
use base64::Engine as _;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ContentId;
use fluree_db_core::{
    range_with_overlay, ContentAddressedWrite, ContentKind, Flake, GraphId, IndexType, Sid,
    TXN_META_GRAPH_ID,
};
use fluree_db_core::{RangeMatch, RangeOptions, RangeTest, Storage};
use fluree_db_core::{CODEC_FLUREE_COMMIT, CODEC_FLUREE_TXN};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{CasResult, RefKind, RefValue};
use fluree_db_novelty::{generate_commit_flakes, stamp_graph_on_commit_flakes, Novelty};
use fluree_db_policy::PolicyContext;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::error;

/// Base64-encoded bytes for JSON payloads.
///
/// In JSON, we encode as a base64 string to avoid large `[0,1,2,...]` arrays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Base64Bytes(pub Vec<u8>);

impl Base64Bytes {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl Serialize for Base64Bytes {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = base64::engine::general_purpose::STANDARD.encode(&self.0);
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for Base64Bytes {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Base64Bytes(bytes))
    }
}

/// Request body for pushing commits to a transactor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushCommitsRequest {
    /// Commit v2 blobs, in order (oldest -> newest).
    pub commits: Vec<Base64Bytes>,
    /// Optional additional blobs referenced by commits (e.g. `commit.txn`).
    ///
    /// Map key is a CID string or legacy address string.
    #[serde(default)]
    pub blobs: HashMap<String, Base64Bytes>,
}

/// Response body for a successful push.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushCommitsResponse {
    pub ledger: String,
    pub accepted: usize,
    pub head: PushedHead,
    /// Indexing status hints for external indexers.
    pub indexing: IndexingStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushedHead {
    pub t: i64,
    /// Content identifier for the new head commit.
    pub commit_id: ContentId,
}

/// Push precomputed commits to a ledger handle (transaction server mode).
///
/// This acquires the ledger write mutex for the duration of validation + publish,
/// so pushes are serialized with normal transactions.
impl Fluree {
    pub async fn push_commits_with_handle(
        &self,
        handle: &LedgerHandle,
        request: PushCommitsRequest,
        opts: &QueryConnectionOptions,
        index_config: &IndexConfig,
    ) -> Result<PushCommitsResponse> {
        if request.commits.is_empty() {
            return Err(ApiError::http(400, "missing required field 'commits'"));
        }

        // 0) Lock ledger state for write (serialize with transactions).
        let mut guard = handle.lock_for_write().await;
        let mut base_state = guard.clone_state();

        // 1) Read current head ref (CAS expected).
        let current_ref = self
            .publisher()?
            .get_ref(base_state.ledger_id(), RefKind::CommitHead)
            .await?;
        let Some(current_ref) = current_ref else {
            return Err(ApiError::NotFound(format!(
                "Ledger not found: {}",
                base_state.ledger_id()
            )));
        };

        // 2) Decode commits and preflight strict sequencing.
        let decoded = decode_and_validate_commit_chain(base_state.ledger_id(), &request)
            .map_err(PushError::into_api_error)?;

        preflight_strict_next_t_and_prev(&current_ref, &decoded)
            .map_err(PushError::into_api_error)?;

        // 3) Validate referenced blobs are provided (if any) and pre-validate hashes.
        validate_required_blobs(&decoded, &request.blobs).map_err(PushError::into_api_error)?;

        // 4) Validate each commit against evolving server view.
        //
        // We maintain an owned Novelty overlay that starts as the current novelty and
        // is extended with each accepted commit (including derived commit-metadata flakes),
        // so later commits validate against the state they would observe when applied.
        let mut evolving_novelty = (*base_state.novelty).clone();

        // Track per-commit "all flakes" so we can update state after CAS.
        let mut accepted_all_flakes: Vec<(i64, Vec<Flake>)> = Vec::with_capacity(decoded.len());

        // 4.0 Build ONE cumulative graph routing from ALL commits' flakes.
        //
        // This ensures consistent GraphId assignments across the entire push batch.
        // Per-commit routing would assign different GraphIds for the same graph
        // when processing separate commits independently (each starts max_g_id=1).
        let all_push_flakes: Vec<&Flake> = decoded.iter().flat_map(|c| &c.commit.flakes).collect();
        let routing = derive_graph_routing(&base_state, &all_push_flakes);
        let mut reverse_graph = reverse_graph_lookup(&routing.graph_sids);
        // Ensure txn-meta graph is always routable — commit metadata flakes
        // are stamped with this graph SID after routing is derived.
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(base_state.ledger_id());
        if let Some(g_sid) = base_state.snapshot.encode_iri(&txn_meta_iri) {
            reverse_graph.entry(g_sid).or_insert(TXN_META_GRAPH_ID);
        }

        // Accumulate namespace table for cross-commit namespace conflict validation.
        // Uses NamespaceCodes (validated bimap) rather than raw HashMap to enforce
        // bimap uniqueness/immutability through the canonical type.
        let mut accumulated_ns = fluree_db_core::ns_encoding::NamespaceCodes::from_code_to_prefix(
            base_state.snapshot.namespaces().clone(),
        )
        .map_err(|e| {
            PushError::Invalid(format!("base snapshot namespace corruption: {e}")).into_api_error()
        })?;

        for c in &decoded {
            // Current state is base db + evolving novelty.
            let current_t = base_state.snapshot.t.max(evolving_novelty.t);

            // 4.0.1 Cross-commit namespace validation: namespace delta must be
            // conflict-free against the accumulated namespace table from parent + prior commits.
            accumulated_ns
                .merge_delta(&c.commit.namespace_delta)
                .map_err(|e| {
                    PushError::Invalid(format!(
                        "commit t={}: namespace delta conflict: {}",
                        c.commit.t, e
                    ))
                    .into_api_error()
                })?;

            // ns_split_mode immutability: locked once user namespaces are allocated.
            if let Some(mode) = c.commit.ns_split_mode {
                base_state
                    .snapshot
                    .set_ns_split_mode(mode, c.commit.t)
                    .map_err(|e| PushError::Invalid(e.to_string()).into_api_error())?;
            }

            // 4.1 Retraction invariant (strict).
            assert_retractions_exist(
                &base_state.snapshot,
                &evolving_novelty,
                current_t,
                &c.commit.flakes,
                &reverse_graph,
            )
            .await
            .map_err(PushError::into_api_error)?;

            // 4.2 Policy enforcement: build policy context from opts against current state.
            let policy_ctx =
                build_policy_ctx_for_push(&base_state, &evolving_novelty, current_t, opts).await?;

            // 4.3 Stage flakes (policy/backpressure). No WHERE/cancellation; flakes are prebuilt.
            let evolving_state = base_state.clone_with_novelty(Arc::new(evolving_novelty.clone()));
            let staged_view = stage_commit_flakes(
                evolving_state,
                &c.commit.flakes,
                index_config,
                &policy_ctx,
                &routing.graph_sids,
            )
            .await
            .map_err(PushError::into_api_error)?;

            // 4.4 SHACL (optional feature).
            //
            // Route through the shared post-stage helper so commit replay
            // honors the ledger's current `f:shaclEnabled` / `f:validationMode`
            // AND per-graph SHACL overrides. A commit created under `Warn` on
            // the leader (ledger-wide or per-graph) must not reject replication
            // on the follower.
            //
            // `graph_delta` is built from `routing.graph_iris` — IRIs already
            // resolved during graph routing. Overlay-only graphs (not yet in
            // the binary store) are intentionally absent from `graph_iris` and
            // therefore fall back to the ledger-wide baseline, which is the
            // correct behavior: config cannot exist for a graph not yet known.
            #[cfg(feature = "shacl")]
            {
                crate::tx::apply_shacl_policy_to_staged_view(
                    &staged_view,
                    crate::tx::StagedShaclContext {
                        graph_delta: Some(&routing.graph_iris),
                        graph_sids: Some(&routing.graph_sids),
                        tracker: None,
                    },
                )
                .await
                .map_err(|e| ApiError::http(422, e.to_string()))?;
            }
            #[cfg(not(feature = "shacl"))]
            {
                let _ = &staged_view;
            }

            // 4.5 Advance evolving novelty with this commit's flakes + derived metadata flakes.
            let mut all_flakes = c.commit.flakes.clone();
            let mut meta_flakes =
                generate_commit_flakes(&c.commit, base_state.ledger_id(), c.commit.t);
            let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(base_state.ledger_id());
            if let Some(g_sid) = base_state.snapshot.encode_iri(&txn_meta_iri) {
                stamp_graph_on_commit_flakes(&mut meta_flakes, &g_sid);
            }
            all_flakes.extend(meta_flakes);

            // Note: Novelty::apply_commit bumps to max(commit_t) internally.
            evolving_novelty
                .apply_commit(all_flakes.clone(), c.commit.t, &reverse_graph)
                .map_err(ApiError::Novelty)?;

            accepted_all_flakes.push((c.commit.t, all_flakes));
        }

        // 5) Write required blobs and commit bytes to storage (safe before CAS).
        let storage = self
            .backend()
            .admin_storage_cloned()
            .ok_or_else(|| ApiError::config("push_commits requires a managed storage backend"))?;
        write_required_blobs(&storage, base_state.ledger_id(), &request.blobs, &decoded)
            .await
            .map_err(PushError::into_api_error)?;

        let stored_commits = write_commit_blobs(&storage, base_state.ledger_id(), &decoded)
            .await
            .map_err(PushError::into_api_error)?;

        let final_head = stored_commits.last().expect("non-empty stored_commits");
        let new_ref = RefValue {
            id: Some(final_head.commit_id.clone()),
            t: final_head.t,
        };

        // 6) CAS update CommitHead (single CAS, strict).
        match self
            .publisher()?
            .compare_and_set_ref(
                base_state.ledger_id(),
                RefKind::CommitHead,
                Some(&current_ref),
                &new_ref,
            )
            .await?
        {
            CasResult::Updated => {}
            CasResult::Conflict { actual } => {
                return Err(ApiError::http(
                    409,
                    format!(
                        "commit head changed during push (expected t={}, actual={:?})",
                        current_ref.t, actual
                    ),
                ));
            }
        }

        // 7) Update in-memory ledger state (now committed).
        let new_state = apply_pushed_commits_to_state(
            base_state,
            &accepted_all_flakes,
            &decoded,
            &stored_commits,
        )
        .map_err(PushError::into_api_error)?;

        let mut new_state = new_state;
        if crate::ns_helpers::binary_store_missing_snapshot_namespaces(&new_state) {
            let cache_dir = self.binary_store_cache_dir();
            // Result unused: load_and_attach mutates new_state in-place
            let _store = crate::ledger_manager::load_and_attach_binary_store(
                self.backend(),
                &mut new_state,
                &cache_dir,
                Some(std::sync::Arc::clone(self.leaflet_cache())),
            )
            .await?;
        }

        // 8) Compute indexing status from the updated state.
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = new_state.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: new_state.novelty_size(),
            index_t: new_state.index_t(),
            commit_t: final_head.t,
        };

        // Sync binary_store BEFORE replacing state so that concurrent readers
        // (via snapshot()) never see the new state with a stale binary_store.
        handle.sync_binary_store_from_state(&new_state).await;
        guard.replace(new_state);

        // 9) Trigger background indexing if enabled and needed.
        if let IndexingMode::Background(idx_handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                idx_handle.trigger(handle.ledger_id(), final_head.t).await;
            }
        }

        Ok(PushCommitsResponse {
            ledger: handle.ledger_id().to_string(),
            accepted: decoded.len(),
            head: PushedHead {
                t: final_head.t,
                commit_id: final_head.commit_id.clone(),
            },
            indexing: indexing_status,
        })
    }
}

// === Internal helpers ===

#[derive(Debug)]
struct PushCommitDecoded {
    commit: fluree_db_novelty::Commit,
    bytes: Vec<u8>,
    /// SHA-256 hex digest used for content addressing. For v4 blobs this is
    /// the hash of the full blob; for v3 it is the embedded trailing hash.
    digest_hex: String,
}

#[derive(Debug, Clone)]
struct StoredCommit {
    t: i64,
    commit_id: ContentId,
}

#[derive(Debug)]
enum PushError {
    Conflict(String),
    Invalid(String),
    Forbidden(String),
    Internal(String),
}

impl PushError {
    fn into_api_error(self) -> ApiError {
        match self {
            PushError::Conflict(m) => ApiError::http(409, m),
            PushError::Invalid(m) => ApiError::http(422, m),
            PushError::Forbidden(m) => ApiError::http(403, m),
            PushError::Internal(m) => ApiError::internal(m),
        }
    }
}

/// Result of graph routing derivation.
///
/// Maps each unique graph Sid to a GraphId. Resolved graphs (known to the
/// binary store) use the store's existing GraphId; unresolved graphs get
/// fresh sequential IDs. Used by `stage_flakes` and `is_currently_asserted`.
struct GraphRoutingResult {
    /// All graph ID → Sid mappings (resolved + fabricated).
    graph_sids: HashMap<GraphId, Sid>,

    /// `GraphId → graph IRI` for graphs resolved against the binary store.
    ///
    /// Used for per-graph SHACL config lookup during replay. Overlay-only
    /// (unresolved) graphs are intentionally omitted — no per-graph config can
    /// exist for a graph that isn't yet known to the store, so those fall back
    /// to the ledger-wide SHACL baseline.
    graph_iris: rustc_hash::FxHashMap<GraphId, String>,
}

/// Derive a `GraphId → Sid` routing map from flakes + binary store.
///
/// For each unique `Flake.g = Some(sid)`, resolves the graph IRI via the
/// binary store's namespace dictionary, then looks up the `GraphId` in the
/// binary store's graph dictionary.
///
/// Graphs not found in the binary store are "overlay-only" — they exist in
/// novelty but haven't been indexed yet. They still get fabricated sequential
/// `GraphId`s (needed by `stage_flakes`), but range queries for them must use
/// g_id=0 and post-filter by `flake.g` to avoid scanning non-existent binary
/// index partitions.
///
/// Returns an empty routing when no named-graph flakes are present.
fn derive_graph_routing(state: &LedgerState, flakes: &[&Flake]) -> GraphRoutingResult {
    // Collect unique graph Sids from flakes.
    let graph_sids_set: HashSet<Sid> = flakes.iter().filter_map(|f| f.g.clone()).collect();

    if graph_sids_set.is_empty() {
        return GraphRoutingResult {
            graph_sids: HashMap::new(),
            graph_iris: rustc_hash::FxHashMap::default(),
        };
    }

    let binary_store: Option<Arc<BinaryIndexStore>> = state
        .binary_store
        .as_ref()
        .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok());

    let mut result: HashMap<GraphId, Sid> = HashMap::new();
    let mut graph_iris: rustc_hash::FxHashMap<GraphId, String> = rustc_hash::FxHashMap::default();
    let mut max_g_id: GraphId = 1; // 0=default, 1=txn-meta
    let mut unresolved: Vec<Sid> = Vec::new();

    for g_sid in &graph_sids_set {
        let resolved = if let Some(store) = &binary_store {
            let iri = match store.sid_to_iri(g_sid) {
                Some(iri) => iri,
                None => {
                    tracing::error!(
                        ns_code = g_sid.namespace_code,
                        suffix = %g_sid.name,
                        "sid_to_iri failed for graph SID — namespace code not in binary store"
                    );
                    continue; // skip unresolvable graph SID
                }
            };
            store.graph_id_for_iri(&iri).map(|g_id| (g_id, iri))
        } else {
            None
        };

        if let Some((g_id, iri)) = resolved {
            max_g_id = max_g_id.max(g_id);
            result.insert(g_id, g_sid.clone());
            graph_iris.insert(g_id, iri);
        } else {
            unresolved.push(g_sid.clone());
        }
    }

    // Assign fabricated sequential GraphIds for unresolved graphs.
    // Sort to ensure deterministic assignment across calls.
    unresolved.sort();
    for (next_id, g_sid) in (max_g_id + 1..).zip(unresolved) {
        result.insert(next_id, g_sid);
    }

    GraphRoutingResult {
        graph_sids: result,
        graph_iris,
    }
}

/// Build a reverse lookup from graph Sid → GraphId.
fn reverse_graph_lookup(graph_sids: &HashMap<GraphId, Sid>) -> HashMap<Sid, GraphId> {
    graph_sids
        .iter()
        .map(|(&g_id, sid)| (sid.clone(), g_id))
        .collect()
}

fn decode_and_validate_commit_chain(
    _ledger_id: &str,
    request: &PushCommitsRequest,
) -> std::result::Result<Vec<PushCommitDecoded>, PushError> {
    let mut out = Vec::with_capacity(request.commits.len());

    let mut prev_t: Option<i64> = None;
    let mut prev_hash: Option<String> = None;

    for (idx, b64) in request.commits.iter().enumerate() {
        let bytes = b64.0.clone();
        let commit = fluree_db_core::commit::codec::read_commit(&bytes)
            .map_err(|e| PushError::Invalid(format!("invalid commit[{idx}]: {e}")))?;

        // Reject empty commits (no flakes) - keep semantics clear.
        if commit.flakes.is_empty() {
            return Err(PushError::Invalid(format!(
                "invalid commit[{idx}]: empty commit (no flakes)"
            )));
        }

        // Derive the content digest hex for addressing.
        // V4: CID = SHA-256(full blob). No embedded hash to verify.
        let digest_hex = fluree_db_core::sha256_hex(&bytes);

        // Note: commit blobs are applied to the server-selected `ledger_id` via CAS.
        // We do not currently enforce a ledger identity embedded inside the commit bytes.

        // Chain validation: strict contiguous t (+1).
        if let Some(pt) = prev_t {
            if commit.t != pt + 1 {
                return Err(PushError::Invalid(format!(
                    "commit chain is not contiguous: commit[{}].t={} does not equal prior+1={}",
                    idx,
                    commit.t,
                    pt + 1
                )));
            }
        }

        // Chain validation: at least one parent reference must match the prior
        // commit's hash. For normal commits this is the single parent; for merge
        // commits one parent is the prior commit and others are pre-existing.
        if let Some(prev_hash_hex) = &prev_hash {
            let ok = commit
                .parents
                .iter()
                .any(|r| r.digest_hex() == *prev_hash_hex);
            if !ok {
                return Err(PushError::Invalid(format!(
                    "commit chain previous mismatch at commit[{idx}]: expected previous digest '{prev_hash_hex}'"
                )));
            }
        }

        prev_t = Some(commit.t);
        prev_hash = Some(digest_hex.clone());

        out.push(PushCommitDecoded {
            commit,
            bytes,
            digest_hex,
        });
    }

    Ok(out)
}

fn preflight_strict_next_t_and_prev(
    current: &RefValue,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<(), PushError> {
    let first = decoded.first().expect("non-empty decoded");

    let expected_t = current.t + 1;
    if first.commit.t != expected_t {
        return Err(PushError::Conflict(format!(
            "first commit t mismatch: expected next-t={}, got {}",
            expected_t, first.commit.t
        )));
    }

    // Validate that at least one parent reference matches the current head CID.
    if let Some(expected_id) = &current.id {
        let ok = first
            .commit
            .parents
            .iter()
            .any(|r| r == expected_id);
        if !ok {
            return Err(PushError::Conflict(format!(
                "first commit previous mismatch: no parent matches expected head {expected_id:?}"
            )));
        }
    } else if !first.commit.parents.is_empty() {
        return Err(PushError::Conflict(
            "first commit has parent refs but current head has no id".to_string(),
        ));
    }

    Ok(())
}

fn validate_required_blobs(
    decoded: &[PushCommitDecoded],
    provided: &HashMap<String, Base64Bytes>,
) -> std::result::Result<(), PushError> {
    let mut required: HashSet<String> = HashSet::new();
    for c in decoded {
        if let Some(txn_cid) = &c.commit.txn {
            required.insert(txn_cid.to_string());
        }
    }

    for addr in &required {
        if !provided.contains_key(addr) {
            return Err(PushError::Invalid(format!(
                "missing required blob for referenced address: {addr}"
            )));
        }
    }

    Ok(())
}

async fn build_policy_ctx_for_push(
    base: &LedgerState,
    evolving: &Novelty,
    current_t: i64,
    opts: &QueryConnectionOptions,
) -> Result<PolicyContext> {
    // Build policy context from opts against current state (db + evolving novelty).
    build_policy_context_from_opts(
        &base.snapshot,
        evolving,
        Some(evolving),
        current_t,
        opts,
        &[0],
    )
    .await
}

async fn stage_commit_flakes(
    ledger: LedgerState,
    flakes: &[Flake],
    index_config: &IndexConfig,
    policy_ctx: &PolicyContext,
    graph_sids: &HashMap<GraphId, Sid>,
) -> std::result::Result<fluree_db_ledger::StagedLedger, PushError> {
    let mut options = fluree_db_transact::StageOptions::new()
        .with_index_config(index_config)
        .with_graph_sids(graph_sids);
    if !policy_ctx.wrapper().is_root() {
        options = options.with_policy(policy_ctx);
    }
    fluree_db_transact::stage_flakes(ledger, flakes.to_vec(), options)
        .await
        .map_err(|e| match e {
            fluree_db_transact::TransactError::PolicyViolation(p) => {
                PushError::Forbidden(p.to_string())
            }
            other => PushError::Invalid(other.to_string()),
        })
}

async fn assert_retractions_exist(
    snapshot: &fluree_db_core::LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    flakes: &[Flake],
    reverse_graph: &HashMap<Sid, GraphId>,
) -> std::result::Result<(), PushError> {
    for (idx, f) in flakes.iter().enumerate() {
        if f.op {
            continue;
        }

        if !is_currently_asserted(snapshot, overlay, to_t, f, reverse_graph).await? {
            return Err(PushError::Invalid(format!(
                "retraction invariant violated at flake[{idx}]: retract targets non-existent assertion"
            )));
        }
    }
    Ok(())
}

async fn is_currently_asserted(
    snapshot: &fluree_db_core::LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    target: &Flake,
    reverse_graph: &HashMap<Sid, GraphId>,
) -> std::result::Result<bool, PushError> {
    // Resolve the correct graph for this flake's range query.
    //
    // With per-graph novelty, ALL graphs (including overlay-only ones) have proper
    // GraphIds assigned by the cumulative routing. The overlay returns only that
    // graph's flakes, so no post-filtering by flake.g is needed for graph isolation.
    let g_id = match &target.g {
        None => 0,
        Some(g_sid) => *reverse_graph.get(g_sid).ok_or_else(|| {
            PushError::Internal(format!(
                "is_currently_asserted: graph Sid {g_sid:?} not found in reverse_graph map \
                 — derive_graph_routing should have included it",
            ))
        })?,
    };

    let rm = RangeMatch::new()
        .with_subject(target.s.clone())
        .with_predicate(target.p.clone())
        .with_object(target.o.clone())
        .with_datatype(target.dt.clone());

    let found = range_with_overlay(
        snapshot,
        g_id,
        overlay,
        IndexType::Spot,
        RangeTest::Eq,
        rm,
        RangeOptions::new().with_to_t(to_t),
    )
    .await
    .map_err(|e| PushError::Internal(e.to_string()))?;

    let mut last_t: Option<i64> = None;
    let mut last_op: bool = false;

    for f in found {
        // Exact match on graph + metadata. (RangeMatch does not include these fields.)
        if f.g != target.g || f.m != target.m {
            continue;
        }
        // Current truth is the op of the latest t <= to_t.
        if last_t.map(|t| f.t > t).unwrap_or(true) {
            last_t = Some(f.t);
            last_op = f.op;
        }
    }

    Ok(last_t.is_some() && last_op)
}

async fn write_required_blobs<S>(
    storage: &S,
    ledger_id: &str,
    provided: &HashMap<String, Base64Bytes>,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<(), PushError>
where
    S: Storage + Send + Sync,
{
    // Build required set (txn CID strings, for now).
    let mut required: HashSet<String> = HashSet::new();
    for c in decoded {
        if let Some(txn_cid) = &c.commit.txn {
            required.insert(txn_cid.to_string());
        }
    }

    for addr in &required {
        let txn_id: ContentId = addr
            .parse()
            .map_err(|e| PushError::Invalid(format!("invalid txn CID reference '{addr}': {e}")))?;
        if txn_id.codec() != CODEC_FLUREE_TXN {
            return Err(PushError::Invalid(format!(
                "referenced txn CID has unexpected codec {}: {}",
                txn_id.codec(),
                addr
            )));
        }

        let bytes = provided
            .get(addr)
            .ok_or_else(|| PushError::Invalid(format!("missing required blob: {addr}")))?
            .0
            .clone();

        // Integrity: server MUST re-hash bytes and verify the derived CID.
        if !txn_id.verify(&bytes) {
            return Err(PushError::Invalid(format!(
                "referenced txn CID does not match provided bytes: {addr}"
            )));
        }

        // Write using the CID digest to ensure deterministic placement.
        // (digest hex matches the legacy CAS hash during the transition period)
        let expected_hash = txn_id.digest_hex();
        let _res = storage
            .content_write_bytes_with_hash(ContentKind::Txn, ledger_id, &expected_hash, &bytes)
            .await
            .map_err(|e| PushError::Internal(e.to_string()))?;
    }

    Ok(())
}

async fn write_commit_blobs<S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static>(
    storage: &S,
    ledger_id: &str,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<Vec<StoredCommit>, PushError> {
    let mut stored = Vec::with_capacity(decoded.len());
    for (idx, c) in decoded.iter().enumerate() {
        let res = storage
            .content_write_bytes(ContentKind::Commit, ledger_id, &c.bytes)
            .await
            .map_err(|e| PushError::Internal(e.to_string()))?;
        if res.content_hash != c.digest_hex {
            return Err(PushError::Invalid(format!(
                "commit[{}] hash mismatch after write: expected {}, storage reported {}",
                idx, c.digest_hex, res.content_hash
            )));
        }
        let commit_id =
            ContentId::from_hex_digest(CODEC_FLUREE_COMMIT, &c.digest_hex).ok_or_else(|| {
                PushError::Internal(format!(
                    "commit[{}]: invalid content hash hex '{}'",
                    idx, c.digest_hex
                ))
            })?;
        stored.push(StoredCommit {
            t: c.commit.t,
            commit_id,
        });
    }
    Ok(stored)
}

fn apply_pushed_commits_to_state(
    mut base: LedgerState,
    accepted_all_flakes: &[(i64, Vec<Flake>)],
    decoded: &[PushCommitDecoded],
    stored_commits: &[StoredCommit],
) -> std::result::Result<LedgerState, PushError> {
    // Extract namespace deltas and graph IRIs from the decoded commits,
    // then apply to the snapshot so that build_reverse_graph() has complete data.
    {
        let mut merged_ns_delta: HashMap<u16, String> = HashMap::new();
        let mut all_graph_iris: HashSet<String> = HashSet::new();
        for c in decoded {
            for (code, prefix) in &c.commit.namespace_delta {
                merged_ns_delta
                    .entry(*code)
                    .or_insert_with(|| prefix.clone());
            }
            for iri in c.commit.graph_delta.values() {
                all_graph_iris.insert(iri.clone());
            }
            // Apply ns_split_mode (immutable after user namespace allocation).
            if let Some(mode) = c.commit.ns_split_mode {
                base.snapshot
                    .set_ns_split_mode(mode, c.commit.t)
                    .map_err(|e| PushError::Internal(e.to_string()))?;
            }
        }
        base.snapshot
            .apply_envelope_deltas(&merged_ns_delta, &all_graph_iris)
            .map_err(|e| PushError::Internal(format!("apply_envelope_deltas failed: {e}")))?;
    }

    // Build reverse_graph now that namespace_codes and graph_registry are complete.
    // NOTE: We fallback to an empty map on failure because commits are already
    // persisted at this point.  An empty reverse_graph means graph-scoped flake
    // routing in novelty will be incomplete until the server restarts and rebuilds
    // state from the stored commits.  Propagating an error here would mislead the
    // caller into thinking the commit failed when it was actually stored.
    let mut reverse_graph = base.snapshot.build_reverse_graph().unwrap_or_else(|e| {
        error!(
            error = ?e,
            "post-CAS build_reverse_graph failed; in-memory graph routing will be \
             incomplete until server restart — commits are already persisted and will \
             be correct after reload"
        );
        HashMap::new()
    });
    // Ensure txn-meta graph is always routable for commit metadata flakes.
    let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(base.ledger_id());
    if let Some(g_sid) = base.snapshot.encode_iri(&txn_meta_iri) {
        reverse_graph.entry(g_sid).or_insert(TXN_META_GRAPH_ID);
    }

    // Apply all flakes to novelty (we already validated them; re-apply for state).
    let mut novelty = (*base.novelty).clone();
    let mut dict_novelty = base.dict_novelty.clone();

    let store_opt: Option<&BinaryIndexStore> = base
        .binary_store
        .as_ref()
        .and_then(|te| te.0.as_ref().downcast_ref::<BinaryIndexStore>());

    for (t, flakes) in accepted_all_flakes {
        // Populate dict novelty similarly to transact commit path.
        fluree_db_binary_index::dict_novelty_safe::populate_dict_novelty_safe(
            Arc::make_mut(&mut dict_novelty),
            store_opt,
            flakes.iter(),
        )
        .map_err(|e| {
            PushError::Internal(format!("populate_dict_novelty_safe failed at t={t}: {e}"))
        })?;
        // Apply to novelty.
        novelty
            .apply_commit(flakes.clone(), *t, &reverse_graph)
            .map_err(|e| {
                PushError::Internal(format!("novelty apply_commit failed at t={t}: {e}"))
            })?;
    }

    base.novelty = Arc::new(novelty);
    base.dict_novelty = dict_novelty;
    base.head_commit_id = stored_commits.last().map(|c| c.commit_id.clone());
    if let Some(ref mut r) = base.ns_record {
        if let Some(last) = stored_commits.last() {
            r.commit_head_id = Some(last.commit_id.clone());
            r.commit_t = last.t;
        }
    }
    Ok(base)
}

trait LedgerStateCloneExt {
    fn clone_with_novelty(&self, novelty: Arc<Novelty>) -> Self;
}

impl LedgerStateCloneExt for LedgerState {
    fn clone_with_novelty(&self, novelty: Arc<Novelty>) -> Self {
        let mut s = self.clone();
        s.novelty = novelty;
        s
    }
}

// ============================================================================
// Export (server → client)
// ============================================================================

/// Maximum commits per export page (server-enforced cap).
const EXPORT_MAX_LIMIT: usize = 500;

/// Default commits per export page when no limit is specified.
const EXPORT_DEFAULT_LIMIT: usize = 100;

/// Query parameters for paginated commit export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportCommitsRequest {
    /// Commit cursor to start from.
    ///
    /// Backward-compatible field:
    /// - legacy: commit address (e.g. `fluree:file://.../commit/<hash>.fcv2`)
    /// - new: commit CID string (e.g. `bafy...`)
    ///
    /// When both `cursor` and `cursor_id` are provided, `cursor_id` wins.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Commit CID cursor to start from (storage-agnostic identity).
    ///
    /// Preferred for cross-backend sync. When `None`, starts from the current head.
    #[serde(default)]
    pub cursor_id: Option<ContentId>,
    /// Maximum commits per page. Clamped to server max (500).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Paginated response containing commit blobs (newest → oldest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportCommitsResponse {
    /// Ledger identifier.
    pub ledger: String,
    /// Head commit CID at time of export (storage-agnostic identity).
    pub head_commit_id: ContentId,
    /// Head `t` at time of export (informational).
    pub head_t: i64,
    /// Raw commit v2 blobs, newest → oldest within this page.
    pub commits: Vec<Base64Bytes>,
    /// Referenced blobs (txn blobs) keyed by CID string.
    #[serde(default)]
    pub blobs: HashMap<String, Base64Bytes>,
    /// Highest `t` in this page.
    pub newest_t: i64,
    /// Lowest `t` in this page.
    pub oldest_t: i64,
    /// Cursor for the next page (previous commit CID).
    /// `None` when genesis has been reached.
    pub next_cursor_id: Option<ContentId>,
    /// Number of commits in this page.
    pub count: usize,
    /// Actual limit used (after server clamping).
    pub effective_limit: usize,
}

/// Export a paginated range of commits from a ledger.
///
/// Uses address-cursor pagination: each page walks backward from the cursor
/// via `parents` for up to `limit` commits. Each page is O(limit)
/// regardless of total ledger size.
///
/// Commits are returned newest → oldest. The client reverses for import.
impl Fluree {
    pub async fn export_commit_range(
        &self,
        handle: &LedgerHandle,
        request: &ExportCommitsRequest,
    ) -> Result<ExportCommitsResponse> {
        use fluree_db_core::commit::codec::envelope::decode_envelope;
        use fluree_db_core::commit::codec::format::{CommitHeader, HEADER_LEN};
        use fluree_db_core::ContentStore;

        let effective_limit = request
            .limit
            .unwrap_or(EXPORT_DEFAULT_LIMIT)
            .min(EXPORT_MAX_LIMIT);

        let ledger_id = handle.ledger_id();

        // Read current head.
        let head_ref = self
            .publisher()?
            .get_ref(ledger_id, RefKind::CommitHead)
            .await?;
        let Some(head_ref) = head_ref else {
            return Err(ApiError::NotFound(format!("Ledger not found: {ledger_id}")));
        };
        let head_commit_id = head_ref
            .id
            .clone()
            .ok_or_else(|| ApiError::NotFound("Ledger has no commits".to_string()))?;
        let head_t = head_ref.t;

        // Build a ContentStore bridge for CID-based reads.
        let content_store = self.content_store(ledger_id);

        // Determine start cursor CID.
        let start_cid: ContentId = if let Some(cid) = &request.cursor_id {
            cid.clone()
        } else if let Some(raw) = request.cursor.as_deref() {
            raw.parse::<ContentId>()
                .map_err(|e| ApiError::http(400, format!("invalid cursor: {e}")))?
        } else {
            head_commit_id.clone()
        };

        let mut commits = Vec::with_capacity(effective_limit);
        let mut blobs: HashMap<String, Base64Bytes> = HashMap::new();
        let mut newest_t: Option<i64> = None;
        let mut oldest_t: Option<i64> = None;
        let mut frontier = vec![start_cid];
        let mut visited = std::collections::HashSet::new();

        for _ in 0..effective_limit {
            let current_cid = match frontier.pop() {
                Some(cid) => cid,
                None => break,
            };
            if !visited.insert(current_cid.clone()) {
                continue;
            }

            // Read raw commit bytes from ContentStore by CID.
            let raw_bytes = content_store.get(&current_cid).await.map_err(|e| {
                ApiError::internal(format!("failed to read commit {current_cid}: {e}"))
            })?;

            // Lightweight decode: header + envelope only (skip ops decompression).
            let header = CommitHeader::read_from(&raw_bytes).map_err(|e| {
                ApiError::internal(format!("invalid commit header for {current_cid}: {e}"))
            })?;

            let envelope_start = HEADER_LEN;
            let envelope_end = envelope_start + header.envelope_len as usize;
            if envelope_end > raw_bytes.len() {
                return Err(ApiError::internal(format!(
                    "commit envelope extends past blob for {current_cid}"
                )));
            }
            let env = decode_envelope(&raw_bytes[envelope_start..envelope_end]).map_err(|e| {
                ApiError::internal(format!("failed to decode envelope for {current_cid}: {e}"))
            })?;

            // Track t range.
            let t = header.t;
            if newest_t.is_none() {
                newest_t = Some(t);
            }
            oldest_t = Some(t);

            commits.push(Base64Bytes(raw_bytes));

            // Collect referenced txn blob via ContentStore.
            if let Some(ref txn_cid) = env.txn {
                let txn_key = txn_cid.to_string();
                if let std::collections::hash_map::Entry::Vacant(e) = blobs.entry(txn_key.clone()) {
                    let txn_bytes = content_store.get(txn_cid).await.map_err(|e| {
                        ApiError::internal(format!("failed to read txn blob {txn_key}: {e}"))
                    })?;
                    e.insert(Base64Bytes(txn_bytes));
                }
            }

            // Enqueue all parents for traversal.
            for parent in env.parents {
                frontier.push(parent);
            }
        }

        // The next cursor is the next unvisited frontier entry (if any).
        let next_cursor_id = frontier.into_iter().find(|id| !visited.contains(id));

        let count = commits.len();

        Ok(ExportCommitsResponse {
            ledger: ledger_id.to_string(),
            head_commit_id,
            head_t,
            commits,
            blobs,
            newest_t: newest_t.unwrap_or(0),
            oldest_t: oldest_t.unwrap_or(0),
            next_cursor_id,
            count,
            effective_limit,
        })
    }
}

// ============================================================================
// Import (client ← server)
// ============================================================================

/// Result of a bulk import (clone path — CAS writes only, no novelty).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkImportResult {
    /// Number of commit blobs written to local CAS.
    pub stored: usize,
    /// Number of txn/reference blobs written to local CAS.
    pub blobs_stored: usize,
}

/// Result of an incremental import (pull path — validated + novelty applied).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitImportResult {
    /// Number of commits imported.
    pub imported: usize,
    /// New local head `t`.
    pub head_t: i64,
    /// New local head commit CID.
    pub head_commit_id: ContentId,
    /// Indexing status hints for external indexers.
    pub indexing: IndexingStatus,
}

impl Fluree {
    /// Bulk-import commit blobs to local CAS (clone path).
    ///
    /// Writes commit and txn blobs without validation or novelty updates.
    /// Order doesn't matter — CAS writes are idempotent.
    /// Call [`set_commit_head`] after all pages are imported.
    pub async fn import_commits_bulk(
        &self,
        handle: &LedgerHandle,
        response: &ExportCommitsResponse,
    ) -> Result<BulkImportResult> {
        let ledger_id = handle.ledger_id();
        let storage = self.admin_storage().ok_or_else(|| {
            ApiError::config("import_commits_bulk requires a managed storage backend")
        })?;
        let mut stored = 0usize;
        let mut blobs_stored = 0usize;

        // Write commit blobs to local CAS (v4: CID = SHA-256 of full blob).
        for b64 in &response.commits {
            let bytes = &b64.0;
            storage
                .content_write_bytes(ContentKind::Commit, ledger_id, bytes)
                .await
                .map_err(|e| ApiError::internal(format!("failed to write commit blob: {e}")))?;
            stored += 1;
        }

        // Write referenced txn blobs to local CAS.
        for (cid_str, b64) in &response.blobs {
            let bytes = &b64.0;
            let txn_id: ContentId = cid_str
                .parse()
                .map_err(|e| ApiError::http(422, format!("invalid txn CID '{cid_str}': {e}")))?;
            if txn_id.codec() != CODEC_FLUREE_TXN {
                return Err(ApiError::http(
                    422,
                    format!(
                        "referenced txn CID has unexpected codec {}: {}",
                        txn_id.codec(),
                        cid_str
                    ),
                ));
            }
            // Integrity: verify bytes match claimed CID.
            if !txn_id.verify(bytes) {
                return Err(ApiError::http(
                    422,
                    format!("txn CID does not match provided bytes: {cid_str}"),
                ));
            }
            let expected_hash = txn_id.digest_hex();
            storage
                .content_write_bytes_with_hash(ContentKind::Txn, ledger_id, &expected_hash, bytes)
                .await
                .map_err(|e| ApiError::internal(format!("failed to write txn blob: {e}")))?;
            blobs_stored += 1;
        }

        Ok(BulkImportResult {
            stored,
            blobs_stored,
        })
    }

    /// Set the commit head after bulk import (clone finalization).
    ///
    /// Points `CommitHead` at the given CID/t. Used after all pages of a
    /// bulk clone have been imported to make the ledger loadable.
    pub async fn set_commit_head(
        &self,
        handle: &LedgerHandle,
        head_commit_id: &ContentId,
        head_t: i64,
    ) -> Result<()> {
        let ledger_id = handle.ledger_id();
        let new_ref = RefValue {
            id: Some(head_commit_id.clone()),
            t: head_t,
        };

        // Read current head for CAS.
        let current_ref = self
            .publisher()?
            .get_ref(ledger_id, RefKind::CommitHead)
            .await?;

        match self
            .publisher()?
            .compare_and_set_ref(
                ledger_id,
                RefKind::CommitHead,
                current_ref.as_ref(),
                &new_ref,
            )
            .await?
        {
            CasResult::Updated => Ok(()),
            CasResult::Conflict { actual } => Err(ApiError::http(
                409,
                format!(
                    "commit head changed during clone finalization (expected {current_ref:?}, actual {actual:?})"
                ),
            )),
        }
    }

    /// Set the index head after pull/clone with index transfer.
    ///
    /// Points `IndexHead` at the given CID/t so that the next `ledger()` load
    /// picks up the pulled binary index. `IndexHead` uses `t >= current.t`
    /// monotonic guard (allows equal-t overwrites, unlike `CommitHead`).
    pub async fn set_index_head(
        &self,
        handle: &LedgerHandle,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()> {
        let ledger_id = handle.ledger_id();
        let new_ref = RefValue {
            id: Some(index_id.clone()),
            t: index_t,
        };

        let current_ref = self
            .publisher()?
            .get_ref(ledger_id, RefKind::IndexHead)
            .await?;

        match self
            .publisher()?
            .compare_and_set_ref(
                ledger_id,
                RefKind::IndexHead,
                current_ref.as_ref(),
                &new_ref,
            )
            .await?
        {
            CasResult::Updated => Ok(()),
            CasResult::Conflict { actual } => Err(ApiError::http(
                409,
                format!(
                    "index head changed during transfer (expected {current_ref:?}, actual {actual:?})"
                ),
            )),
        }
    }

    /// Incrementally import commits (pull path).
    ///
    /// Validates the commit chain, verifies ancestry against the local head,
    /// writes blobs to CAS, advances `CommitHead`, and updates in-memory novelty.
    ///
    /// `commits` must be ordered oldest → newest.
    pub async fn import_commits_incremental(
        &self,
        handle: &LedgerHandle,
        commits: Vec<Base64Bytes>,
        blobs: HashMap<String, Base64Bytes>,
    ) -> Result<CommitImportResult> {
        if commits.is_empty() {
            return Err(ApiError::http(400, "no commits to import"));
        }

        let mut guard = handle.lock_for_write().await;
        let base_state = guard.clone_state();

        // 1) Read current head ref.
        let current_ref = self
            .publisher()?
            .get_ref(base_state.ledger_id(), RefKind::CommitHead)
            .await?;
        let Some(current_ref) = current_ref else {
            return Err(ApiError::NotFound(format!(
                "Ledger not found: {}",
                base_state.ledger_id()
            )));
        };

        // 2) Build a PushCommitsRequest-compatible structure for validation reuse.
        let request = PushCommitsRequest {
            commits,
            blobs: blobs.clone(),
        };

        // 3) Decode and validate chain.
        let decoded = decode_and_validate_commit_chain(base_state.ledger_id(), &request)
            .map_err(PushError::into_api_error)?;

        // 4) Ancestry preflight: verify first commit's parent matches local head.
        preflight_strict_next_t_and_prev(&current_ref, &decoded)
            .map_err(PushError::into_api_error)?;

        // 5) Validate referenced blobs are provided.
        validate_required_blobs(&decoded, &request.blobs).map_err(PushError::into_api_error)?;

        // 6) Write blobs + commit bytes to local CAS.
        let storage = self.backend().admin_storage_cloned().ok_or_else(|| {
            ApiError::config("push_commits_strict requires a managed storage backend")
        })?;
        write_required_blobs(&storage, base_state.ledger_id(), &request.blobs, &decoded)
            .await
            .map_err(PushError::into_api_error)?;

        let stored_commits = write_commit_blobs(&storage, base_state.ledger_id(), &decoded)
            .await
            .map_err(PushError::into_api_error)?;

        let final_head = stored_commits.last().expect("non-empty stored_commits");
        let new_ref = RefValue {
            id: Some(final_head.commit_id.clone()),
            t: final_head.t,
        };

        // 7) CAS update CommitHead.
        match self
            .publisher()?
            .compare_and_set_ref(
                base_state.ledger_id(),
                RefKind::CommitHead,
                Some(&current_ref),
                &new_ref,
            )
            .await?
        {
            CasResult::Updated => {}
            CasResult::Conflict { actual } => {
                return Err(ApiError::http(
                    409,
                    format!(
                        "commit head changed during import (expected t={}, actual={:?})",
                        current_ref.t, actual
                    ),
                ));
            }
        }

        // 8) Update in-memory state (novelty + dict novelty + namespace deltas).
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(base_state.ledger_id());
        let txn_meta_g_sid = base_state.snapshot.encode_iri(&txn_meta_iri);
        let all_flakes: Vec<(i64, Vec<Flake>)> = decoded
            .iter()
            .map(|c| {
                let mut flakes = c.commit.flakes.clone();
                let mut meta_flakes =
                    generate_commit_flakes(&c.commit, base_state.ledger_id(), c.commit.t);
                if let Some(ref g_sid) = txn_meta_g_sid {
                    stamp_graph_on_commit_flakes(&mut meta_flakes, g_sid);
                }
                flakes.extend(meta_flakes);
                (c.commit.t, flakes)
            })
            .collect();

        let new_state =
            apply_pushed_commits_to_state(base_state, &all_flakes, &decoded, &stored_commits)
                .map_err(PushError::into_api_error)?;

        // 9) Compute indexing status from the updated state.
        let index_config = self.default_index_config();
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = new_state.should_reindex(&index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: new_state.novelty_size(),
            index_t: new_state.index_t(),
            commit_t: final_head.t,
        };

        // Sync binary_store BEFORE replacing state so that concurrent readers
        // (via snapshot()) never see the new state with a stale binary_store.
        handle.sync_binary_store_from_state(&new_state).await;
        guard.replace(new_state);

        // 10) Trigger background indexing if enabled and needed.
        if let IndexingMode::Background(idx_handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                idx_handle.trigger(handle.ledger_id(), final_head.t).await;
            }
        }

        Ok(CommitImportResult {
            imported: decoded.len(),
            head_t: final_head.t,
            head_commit_id: final_head.commit_id.clone(),
            indexing: indexing_status,
        })
    }
}
