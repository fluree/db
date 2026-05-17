//! Transaction APIs (stage + commit) for Fluree DB
//!
//! This module wires `fluree-db-transact` + nameservice publishing + optional
//! indexing triggers into the high-level `fluree-db-api` surface.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config_resolver;
use crate::{ApiError, Result};
use crate::{TrackedErrorResponse, Tracker, TrackingOptions, TrackingTally};
use fluree_db_core::ledger_config::LedgerConfig;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{
    range_with_overlay, ContentId, ContentKind, FlakeValue, GraphId, IndexType, RangeMatch,
    RangeOptions, RangeTest, Sid,
};
use fluree_db_indexer::IndexerHandle;
use fluree_db_ledger::{IndexConfig, LedgerState, StagedLedger};
use fluree_db_novelty::TxnMetaEntry;
#[cfg(feature = "shacl")]
use fluree_db_shacl::ShaclEngine;
use fluree_db_transact::stage as stage_txn;
#[cfg(feature = "shacl")]
use fluree_db_transact::validate_view_with_shacl;
use fluree_db_transact::{
    commit as commit_txn, parse_transaction, resolve_trig_meta, CommitOpts, CommitReceipt,
    NamedGraphBlock, NamespaceRegistry, RawTrigMeta, StageOptions, TemplateTerm, TripleTemplate,
    Txn, TxnOpts, TxnType,
};
use fluree_vocab::config_iris;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

fn ledger_id_from_txn(txn_json: &JsonValue) -> Result<&str> {
    let obj = txn_json
        .as_object()
        .ok_or_else(|| ApiError::config("Invalid transaction, missing required key: ledger."))?;
    obj.get("ledger")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::config("Invalid transaction, missing required key: ledger."))
}

/// Input parameters for tracked transactions with policy enforcement.
///
/// Bundles the common transaction parameters to reduce argument count.
pub struct TrackedTransactionInput<'a> {
    /// Transaction type (insert, delete, etc.)
    pub txn_type: TxnType,
    /// Transaction JSON body
    pub txn_json: &'a JsonValue,
    /// Transaction options
    pub txn_opts: TxnOpts,
    /// Policy context for access control
    pub policy: &'a crate::PolicyContext,
}

impl<'a> TrackedTransactionInput<'a> {
    /// Create new tracked transaction input.
    pub fn new(
        txn_type: TxnType,
        txn_json: &'a JsonValue,
        txn_opts: TxnOpts,
        policy: &'a crate::PolicyContext,
    ) -> Self {
        Self {
            txn_type,
            txn_json,
            txn_opts,
            policy,
        }
    }
}

/// Create a tracker for fuel limits only (no time/policy tracking).
///
/// This mirrors query behavior: even non-tracked transactions respect max-fuel.
fn tracker_for_limits(txn_json: &JsonValue) -> Tracker {
    let opts = txn_json.as_object().and_then(|o| o.get("opts"));
    let tracking = TrackingOptions::from_opts_value(opts);
    match tracking.max_fuel.filter(|limit| *limit > 0) {
        Some(limit) => Tracker::new(TrackingOptions {
            track_time: false,
            track_fuel: true,
            track_policy: false,
            max_fuel: Some(limit),
        }),
        None => Tracker::disabled(),
    }
}

/// Check if a JSON-LD document represents an empty default graph.
///
/// This is the case when:
/// - The document is null
/// - The document is an empty array
/// - The document is an object with only JSON-LD keywords (@context, @id, etc.)
///   AND the @graph key is missing or empty
///
/// This correctly handles envelope-form JSON-LD where data is in @graph.
fn is_empty_default_graph(json: &JsonValue) -> bool {
    match json {
        JsonValue::Null => true,
        JsonValue::Array(arr) => arr.is_empty(),
        JsonValue::Object(obj) => {
            // Check if there are any non-@ keys (actual data predicates at top level)
            let has_data_keys = obj.keys().any(|k| !k.starts_with('@'));
            if has_data_keys {
                return false;
            }
            // No data keys at top level, check if @graph has content
            match obj.get("@graph") {
                Some(JsonValue::Array(arr)) => arr.is_empty(),
                Some(JsonValue::Object(inner_obj)) => inner_obj.is_empty(),
                Some(_) => false, // @graph has some other non-empty value
                None => true,     // No @graph key, and no data keys = empty
            }
        }
        _ => false,
    }
}

// =============================================================================
// Config-driven transaction helpers (shacl feature only)
// =============================================================================

/// Load config from the pre-transaction ledger state.
///
/// INVARIANT: Reads config as-of the input `LedgerState` only (head t
/// before staging). Config mutations inside the staged transaction
/// CANNOT relax constraints for that same transaction.
///
/// Returns `None` if config graph is empty or unreadable (best-effort).
async fn load_transaction_config(ledger: &LedgerState) -> Option<Arc<LedgerConfig>> {
    match config_resolver::resolve_ledger_config(&ledger.snapshot, &*ledger.novelty, ledger.t())
        .await
    {
        Ok(Some(config)) => Some(Arc::new(config)),
        Ok(None) => None,
        Err(e) => {
            tracing::debug!(error = %e, "Config graph read failed during staging — using defaults");
            None
        }
    }
}

/// Resolve SHACL config across all graphs affected by a transaction.
///
/// Starts from the ledger-wide baseline (`resolve_effective_config(config, None)`)
/// and overlays per-graph config for each named graph in `graph_delta`.
/// Returns the strictest combination:
/// - `enabled`: true if ANY graph has SHACL enabled
/// - `validation_mode`: `Reject` if ANY graph is `Reject`
///
/// The ledger-wide baseline is always included because SHACL shapes live in the
/// default/schema graph (g_id=0) and target instances in any graph. Even if a
/// transaction only touches named graphs, the ledger-wide SHACL posture applies.
///
/// Note: `graph_delta` for normal JSON-LD transactions (non-import) contains ALL
/// named graphs referenced by the transaction, not just newly-created ones.
/// The `GraphIdAssigner` is created fresh per transaction during JSON-LD parsing.
/// Build the per-graph SHACL policy map for a transaction.
///
/// For each graph referenced by the transaction (via `graph_delta`), resolve
/// its effective SHACL config — honoring three-tier precedence (query-time,
/// per-graph overlay, ledger-wide baseline) and override-control rules — and
/// include it in the returned map **iff SHACL is enabled for that graph**.
///
/// The returned map is keyed by `GraphId` (the transaction's internal
/// numeric graph id). Graphs absent from the map are treated as disabled by
/// the validator. The default graph (g_id=0) is always included when SHACL
/// is enabled ledger-wide — shapes live there by default, and it's the
/// implicit focus-graph for Turtle inserts and any flake without an explicit
/// `g` component.
///
/// Returns `None` when no graph in the transaction has SHACL enabled (so
/// `apply_shacl_policy_to_staged_view` can skip validation entirely).
#[cfg(feature = "shacl")]
fn build_per_graph_shacl_policy(
    config: &LedgerConfig,
    graph_delta: &FxHashMap<u16, String>,
) -> Option<HashMap<GraphId, fluree_db_transact::ShaclGraphPolicy>> {
    let mut map: HashMap<GraphId, fluree_db_transact::ShaclGraphPolicy> = HashMap::new();

    // Ledger-wide baseline — used for the default graph (g_id=0) and for any
    // graph without an explicit per-graph override. The config resolver
    // returns the full three-tier merge with `graph_iri = None`.
    let ledger_wide = config_resolver::merge_shacl_opts(
        &config_resolver::resolve_effective_config(config, None),
        None,
    );

    // Default graph always gets the ledger-wide policy when SHACL is enabled.
    if let Some(cfg) = &ledger_wide {
        if cfg.enabled {
            map.insert(
                0,
                fluree_db_transact::ShaclGraphPolicy {
                    mode: cfg.validation_mode,
                },
            );
        }
    }

    // Per-graph resolution for every named graph touched by the transaction.
    // The resolver applies per-graph overrides on top of the ledger-wide
    // baseline, so `shacl.enabled = false` at the graph level correctly
    // disables that graph independently of other graphs.
    for (g_id, graph_iri) in graph_delta {
        if *g_id == 0 {
            continue; // already handled above
        }
        let resolved = config_resolver::resolve_effective_config(config, Some(graph_iri));
        if let Some(per_graph) = config_resolver::merge_shacl_opts(&resolved, None) {
            if per_graph.enabled {
                map.insert(
                    *g_id,
                    fluree_db_transact::ShaclGraphPolicy {
                        mode: per_graph.validation_mode,
                    },
                );
            }
        }
    }

    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

/// Context for applying SHACL policy to an already-staged [`StagedLedger`].
///
/// All fields are optional: callers without full transaction context (Turtle
/// insert, commit replay) pass `None` and get sensible default behavior:
/// - no `graph_delta` → ledger-wide config only (no per-graph overrides)
/// - no `graph_sids`  → flakes validated against the default graph (g_id=0)
/// - no `tracker`     → SHACL range scans are not fuel-accounted
#[cfg(feature = "shacl")]
pub(crate) struct StagedShaclContext<'a> {
    /// Per-`GraphId` IRI map from the transaction. Used only for resolving
    /// per-graph SHACL config overlays. Pass `None` when txn graph metadata
    /// is unavailable (Turtle insert, commit replay).
    pub graph_delta: Option<&'a FxHashMap<u16, String>>,

    /// `GraphId → Sid` mapping used by [`validate_view_with_shacl`] to route
    /// each staged flake to the correct per-graph validator. Pass `None` to
    /// fall back to default-graph validation (see `validate_staged_nodes`).
    pub graph_sids: Option<&'a HashMap<GraphId, Sid>>,

    /// Optional tracker for SHACL fuel accounting during validation.
    pub tracker: Option<&'a fluree_db_core::Tracker>,

    /// Cross-ledger shapes artifact, pre-resolved at the API
    /// boundary and threaded through staging as an internal
    /// governance input. When `Some`, this artifact is the shape
    /// source rather than `f:shapesSource`'s local graph selector.
    /// The wire is compiled against `staged_ns` (below) so IRIs
    /// the in-flight transaction introduced are encodable; M-only
    /// IRIs are dropped (their shapes can't apply to data D
    /// doesn't have).
    pub cross_ledger_shapes: Option<&'a crate::cross_ledger::ShapesArtifactWire>,

    /// Staged `NamespaceRegistry` — D's snapshot namespaces plus
    /// any IRIs the in-flight transaction has registered. Required
    /// when `cross_ledger_shapes` is `Some`; consulted as the term
    /// context for compiling M's wire-form shapes against D.
    pub staged_ns: Option<&'a fluree_db_transact::namespace::NamespaceRegistry>,

    /// Inline shape bundle parsed from `txn.opts.shapes` against the
    /// staged namespace registry. When `Some`, the bundle attaches
    /// as an additional shape source alongside any same-ledger
    /// `f:shapesSource` or cross-ledger wire — they enforce
    /// additively. Inline shapes do not persist into the ledger.
    pub inline_shape_bundle:
        Option<std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>>,
}

/// Inspect the data ledger's resolved config and, when
/// `f:shapesSource` carries a cross-ledger `f:ledger` reference,
/// resolve the model ledger's shapes graph into a wire artifact
/// at transaction entry — before staging starts.
///
/// Returns `None` when no cross-ledger shapes are configured (so
/// the staging path uses the unchanged same-ledger flow). Returns
/// `Some(ResolvedGraph)` when a wire artifact has been
/// materialized; the caller threads the artifact into the staging
/// context so SHACL compilation at validation time can use the
/// pre-resolved wire instead of querying the model ledger again.
///
/// All cross-ledger failure modes (missing model, reserved graph,
/// unsupported phase fields) surface here as `TransactError::Parse`
/// formatting the underlying `CrossLedgerError`. The HTTP layer
/// maps these to 400 (TransactError → BAD_REQUEST), matching the
/// constraints path's HTTP class. A future refactor that surfaces
/// `ApiError::CrossLedger` (HTTP 502) through the staging error
/// type would preserve the variant.
#[cfg(feature = "shacl")]
async fn resolve_cross_ledger_shapes_for_tx(
    ledger: &LedgerState,
    ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
) -> std::result::Result<
    Option<std::sync::Arc<crate::cross_ledger::ResolvedGraph>>,
    fluree_db_transact::TransactError,
> {
    // Load the same config the same-ledger SHACL path loads (from
    // pre-tx state). resolve_ledger_config returns None on a fresh
    // ledger with no #config — in that case there's no cross-ledger
    // to dispatch.
    let config = match crate::config_resolver::resolve_ledger_config(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        ledger.t(),
    )
    .await
    {
        Ok(Some(c)) => c,
        Ok(None) => return Ok(None),
        Err(e) => {
            return Err(fluree_db_transact::TransactError::Parse(format!(
                "failed to load ledger config while resolving cross-ledger f:shapesSource: {e}"
            )));
        }
    };
    let shapes_source = config.shacl.as_ref().and_then(|s| s.shapes_source.as_ref());
    let Some(source) = shapes_source else {
        return Ok(None);
    };
    if source.ledger.is_none() {
        return Ok(None);
    }
    let resolved = crate::cross_ledger::resolve_graph_ref(
        source,
        crate::cross_ledger::ArtifactKind::Shapes,
        ctx,
    )
    .await
    .map_err(|e| {
        fluree_db_transact::TransactError::Parse(format!(
            "f:shapesSource cross-ledger resolution failed: {e}"
        ))
    })?;
    Ok(Some(resolved))
}

/// Resolve `f:shapesSource` from a loaded `LedgerConfig` into concrete graph
/// IDs, against the current snapshot's graph registry.
///
/// Returns `[0]` (default graph) when:
/// - the config has no `f:shaclDefaults` section, or
/// - `f:shapesSource` is unset, or
/// - `f:shapesSource.graphSelector` is `f:defaultGraph`.
///
/// Mirrors `policy_builder::resolve_policy_source_g_ids` so that shapes,
/// policies, and (eventually) rules all resolve graph references through the
/// same mechanism — schema, policy, and SHACL shapes can live in any graph
/// the ledger knows about, including the config graph itself.
#[cfg(feature = "shacl")]
fn resolve_shapes_source_g_ids(
    config: Option<&LedgerConfig>,
    snapshot: &fluree_db_core::LedgerSnapshot,
) -> std::result::Result<Vec<GraphId>, fluree_db_transact::TransactError> {
    let source = config
        .and_then(|c| c.shacl.as_ref())
        .and_then(|s| s.shapes_source.as_ref());

    let Some(source) = source else {
        return Ok(vec![0]);
    };

    // Temporal / trust / rollback / cross-ledger dimensions of GraphSourceRef
    // aren't yet supported for SHACL — reject early rather than silently
    // ignoring them. Matches the policy resolver's shape.
    if source.ledger.is_some() {
        return Err(fluree_db_transact::TransactError::Parse(
            "f:shapesSource with cross-ledger f:ledger reference is not yet supported".into(),
        ));
    }
    if source.at_t.is_some() {
        return Err(fluree_db_transact::TransactError::Parse(
            "f:shapesSource with f:atT (temporal pinning) is not yet supported".into(),
        ));
    }
    if source.trust_policy.is_some() {
        return Err(fluree_db_transact::TransactError::Parse(
            "f:shapesSource with f:trustPolicy is not yet supported".into(),
        ));
    }
    if source.rollback_guard.is_some() {
        return Err(fluree_db_transact::TransactError::Parse(
            "f:shapesSource with f:rollbackGuard is not yet supported".into(),
        ));
    }

    let g_id = match source.graph_selector.as_deref() {
        Some(iri) if iri == config_iris::DEFAULT_GRAPH => Some(0u16),
        Some(iri) => snapshot.graph_registry.graph_id_for_iri(iri),
        None => Some(0u16),
    };

    match g_id {
        Some(id) => Ok(vec![id]),
        None => Err(fluree_db_transact::TransactError::Parse(format!(
            "f:shapesSource graph '{}' not found in this ledger's graph registry",
            source.graph_selector.as_deref().unwrap_or("<none>"),
        ))),
    }
}

/// Apply SHACL policy to an already-staged [`StagedLedger`].
///
/// This is the single canonical post-stage SHACL entry point shared by every
/// write surface (JSON-LD txn staging, Turtle insert, commit replay). It:
///
/// 1. Loads config from the view's pre-staging state (`view.base()`)
/// 2. Resolves effective SHACL config (per-graph strictest-wins when
///    `ctx.graph_delta` is `Some`; ledger-wide only otherwise)
/// 3. Short-circuits when SHACL is disabled or no shapes exist
/// 4. Validates staged flakes against compiled shapes
/// 5. Under `Warn`: logs `ShaclViolation` and returns `Ok`; propagates every
///    other error so a broken validation pipeline never silently admits writes
/// 6. Under `Reject`: propagates `ShaclViolation` normally
///
/// Kept in the API layer (not `fluree-db-transact`) because config resolution
/// is API-layer policy, not a staging primitive.
#[cfg(feature = "shacl")]
pub(crate) async fn apply_shacl_policy_to_staged_view(
    view: &StagedLedger,
    ctx: StagedShaclContext<'_>,
) -> std::result::Result<(), fluree_db_transact::TransactError> {
    let base = view.base();

    // 1. Load config from pre-transaction state.
    let config = load_transaction_config(base).await;

    // 2. Build per-graph policy from the config (if any). Each graph has its
    //    own enabled/mode. Graphs absent from the policy map are disabled.
    let per_graph_policy = match (&config, ctx.graph_delta) {
        (Some(c), Some(gd)) => build_per_graph_shacl_policy(c, gd),
        (Some(c), None) => {
            // No graph context — apply ledger-wide posture to the default
            // graph only. Shapes for the default graph are where turtle
            // inserts and commit replay land unless the ledger has more
            // specific graph routing.
            let ledger_wide = config_resolver::merge_shacl_opts(
                &config_resolver::resolve_effective_config(c, None),
                None,
            );
            match ledger_wide {
                Some(cfg) if cfg.enabled => {
                    let mut m = HashMap::new();
                    m.insert(
                        0u16,
                        fluree_db_transact::ShaclGraphPolicy {
                            mode: cfg.validation_mode,
                        },
                    );
                    Some(m)
                }
                _ => None,
            }
        }
        (None, _) => None,
    };

    // 3. Shapes-exist heuristic (only when no config is present).
    //    `None` here means: no config → validate every graph in reject mode
    //    *if* any shapes exist in the chosen sources. We'll check the cache
    //    below after building the engine.
    let has_config = config.is_some();
    if has_config && per_graph_policy.is_none() {
        // Config exists but every graph is disabled → nothing to do.
        return Ok(());
    }

    // 4a. Cross-ledger shapes: when a `ShapesArtifactWire` is
    //     threaded through `ctx`, compile it against the staged
    //     `NamespaceRegistry` (which has D's snapshot namespaces
    //     PLUS any IRIs the in-flight transaction registered).
    //     This sidesteps the pre-staging-snapshot bug: IRIs that
    //     the tx is introducing (e.g., the very `ex:Person`
    //     instance being validated) are encodable here, where
    //     they wouldn't be against `base.snapshot`. M-only IRIs
    //     that D has never seen drop their triples — the shape
    //     can't apply to data D doesn't have, and allocating a
    //     fresh ns_code for every M-only term would churn D's
    //     namespace map for no benefit.
    //
    //     When this branch is taken, the same-ledger
    //     `f:shapesSource` resolution is skipped — the wire is
    //     the authoritative shape source for this transaction.
    // Overlay holders keep `SchemaBundleOverlay` alive for the
    // lifetime of `shape_dbs`'s borrow. Cross-ledger shapes and
    // inline-opts shapes live in separate holders so both can
    // contribute additively when both are configured.
    #[allow(unused_assignments)]
    let mut cl_overlay_holder = None;
    #[allow(unused_assignments)]
    let mut inline_overlay_holder = None;
    let mut shape_dbs: Vec<fluree_db_core::GraphDbRef<'_>> =
        if let (Some(wire), Some(staged_ns)) = (ctx.cross_ledger_shapes, ctx.staged_ns) {
            let bundle = wire
                .translate_to_schema_bundle_flakes(staged_ns)
                .map_err(|e| {
                    fluree_db_transact::TransactError::Parse(format!(
                        "cross-ledger shapes wire translation failed: {e}"
                    ))
                })?;
            cl_overlay_holder = Some(fluree_db_query::schema_bundle::SchemaBundleOverlay::new(
                base.novelty.as_ref(),
                bundle,
            ));
            vec![fluree_db_core::GraphDbRef::new(
                &base.snapshot,
                0u16,
                cl_overlay_holder.as_ref().expect("just set above"),
                base.t(),
            )]
        } else {
            // 4b. Same-ledger path. Resolve `f:shapesSource` into
            //     concrete graph IDs; default to `[0]` when unset.
            let shapes_g_ids = resolve_shapes_source_g_ids(config.as_deref(), &base.snapshot)?;
            shapes_g_ids
                .iter()
                .map(|g_id| base.as_graph_db_ref(*g_id))
                .collect()
        };

    // 4c. Inline `opts.shapes`: attach the per-transaction shape
    //     bundle alongside whichever non-inline source ran above.
    //     Inline shapes never replace configured shapes; they layer
    //     additively (the SHACL engine treats multiple shape DBs as
    //     a union). The bundle was already constructed against the
    //     staged namespace registry at `stage_with_config_shacl`
    //     entry, so encoding is consistent with the live tx.
    if let Some(bundle) = ctx.inline_shape_bundle.clone() {
        inline_overlay_holder = Some(fluree_db_query::schema_bundle::SchemaBundleOverlay::new(
            base.novelty.as_ref(),
            bundle,
        ));
        shape_dbs.push(fluree_db_core::GraphDbRef::new(
            &base.snapshot,
            0u16,
            inline_overlay_holder.as_ref().expect("just set above"),
            base.t(),
        ));
    }

    let engine = ShaclEngine::from_dbs_with_overlay(&shape_dbs, base.ledger_id())
        .await
        .map_err(fluree_db_transact::TransactError::from)?;
    let shacl_cache = engine.cache();

    // No config + no shapes → skip (backward compat: shapes-exist heuristic).
    if !has_config && shacl_cache.is_empty() {
        return Ok(());
    }

    // 5. Validate. `per_graph_policy` drives which graphs participate and
    //    what mode their violations carry. `None` = shapes-exist heuristic
    //    path → every graph validated in reject mode (the transact helper's
    //    default when policy is absent).
    let outcome = validate_view_with_shacl(
        view,
        shacl_cache,
        ctx.graph_sids,
        ctx.tracker,
        per_graph_policy.as_ref(),
    )
    .await?;

    // 6. Apply per-graph mode: warn violations log, reject violations fail.
    if !outcome.warn_violations.is_empty() {
        tracing::warn!(
            count = outcome.warn_violations.len(),
            report = %format_violations(&outcome.warn_violations),
            "SHACL violations (warn-mode graph, continuing)"
        );
    }
    if !outcome.reject_violations.is_empty() {
        return Err(fluree_db_transact::TransactError::ShaclViolation(
            format_violations(&outcome.reject_violations),
        ));
    }
    Ok(())
}

/// Format SHACL violations as a human-readable string, matching the shape of
/// the prior `TransactError::ShaclViolation` payload so test assertions and
/// log readers that look for familiar phrasing keep working.
#[cfg(feature = "shacl")]
fn format_violations(violations: &[fluree_db_shacl::ValidationResult]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "SHACL validation failed with {} violation(s):",
        violations.len()
    );
    for (i, v) in violations.iter().enumerate() {
        let _ = writeln!(out, "  {}. {}", i + 1, v.message);
        let _ = writeln!(
            out,
            "     Focus node: {}{}",
            v.focus_node.namespace_code, v.focus_node.name
        );
        if let Some(path) = &v.result_path {
            let _ = writeln!(out, "     Path: {}{}", path.namespace_code, path.name);
        }
    }
    out
}

/// Perform staging followed by config-aware SHACL validation.
///
/// Splits cleanly into two phases:
/// 1. plain `stage_txn(...)`
/// 2. `apply_shacl_policy_to_staged_view(...)` on the resulting view
///
/// The helper handles config resolution, warn vs reject, and the shapes-exist
/// heuristic — this function just wires the context.
#[cfg(feature = "shacl")]
async fn stage_with_config_shacl(
    ledger: LedgerState,
    txn: Txn,
    ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
    resolve_ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
) -> std::result::Result<(StagedLedger, NamespaceRegistry), fluree_db_transact::TransactError> {
    // Capture graph_delta + tracker before stage_txn consumes the options/txn.
    // graph_delta is used both for per-graph config lookup and for rebuilding
    // graph_sids after staging (IRIs are already interned in ns_registry, so
    // sid_for_iri hits the trie cache — no new allocations).
    let graph_delta = txn.graph_delta.clone();
    let tracker = options.tracker;
    // Capture inline shapes JSON now — stage_txn consumes `txn`.
    // Parsing happens *after* staging so the FlakeSink encodes
    // against the post-staging `ns_registry`, matching the term
    // context cross-ledger shapes compile against.
    let inline_shapes_json = txn.opts.shapes.clone();
    let inline_shapes_ledger_id = ledger.snapshot.ledger_id.to_string();

    // Detect cross-ledger SHACL config at the API boundary BEFORE
    // staging starts: read D's resolved config and, if
    // f:shapesSource carries f:ledger, resolve the wire artifact
    // from M now so the per-tx ResolveCtx benefits from memo +
    // governance cache. The wire is then threaded through
    // staging as an internal governance input and compiled
    // against the staged namespace registry at validation time.
    let cross_ledger_shapes = resolve_cross_ledger_shapes_for_tx(&ledger, resolve_ctx).await?;

    let (view, mut ns_registry) = stage_txn(ledger, txn, ns_registry, options).await?;

    // Parse inline shapes (if any) against the staged namespace
    // registry. The bundle becomes an additional shape DB in
    // `apply_shacl_policy_to_staged_view` alongside any same- or
    // cross-ledger sources — enforcement is additive.
    let inline_shape_bundle = if let Some(shapes_json) = inline_shapes_json.as_ref() {
        crate::inline_shapes::parse_inline_shapes_to_bundle(
            shapes_json,
            &mut ns_registry,
            view.base().t(),
            &inline_shapes_ledger_id,
        )?
    } else {
        None
    };

    let graph_sids: HashMap<GraphId, Sid> = graph_delta
        .iter()
        .map(|(&g_id, iri)| (g_id, ns_registry.sid_for_iri(iri)))
        .collect();

    apply_shacl_policy_to_staged_view(
        &view,
        StagedShaclContext {
            graph_delta: Some(&graph_delta),
            graph_sids: Some(&graph_sids),
            tracker,
            cross_ledger_shapes: cross_ledger_shapes
                .as_deref()
                .and_then(|r| match &r.artifact {
                    crate::cross_ledger::GovernanceArtifact::Shapes(wire) => Some(wire),
                    _ => None,
                }),
            staged_ns: cross_ledger_shapes.as_deref().map(|_| &ns_registry),
            inline_shape_bundle,
        },
    )
    .await?;

    Ok((view, ns_registry))
}

// =============================================================================
// Config-driven unique constraint enforcement
// =============================================================================

/// Run uniqueness enforcement after staging if configured.
///
/// Loads config from the pre-txn state (via `view.base()`) and checks
/// staged flakes against `f:enforceUnique` annotations. Zero-cost when
/// no `f:transactDefaults` / `f:uniqueEnabled` is configured.
///
/// `fluree` is required so cross-ledger `f:constraintsSource` references
/// can be resolved against the model ledger. Pass the parent `Fluree`
/// instance — the staging path always has it on `&self`.
async fn enforce_unique_after_staging(
    view: &StagedLedger,
    graph_delta: &FxHashMap<u16, String>,
    resolve_ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
    inline_unique_properties: Option<&[String]>,
) -> std::result::Result<(), fluree_db_transact::TransactError> {
    let config = load_transaction_config(view.base()).await;

    // Start with config-resolved per-graph SIDs (same/cross ledger).
    // No config → empty; inline properties below can still drive
    // enforcement when set.
    let mut per_graph_unique: HashMap<GraphId, FxHashSet<Sid>> = match &config {
        Some(cfg) => resolve_per_graph_unique_sids(view, cfg, graph_delta, resolve_ctx).await?,
        None => HashMap::new(),
    };

    // Layer inline `opts.uniqueProperties` additively. Apply to
    // every affected graph so the constraint behaves as
    // "for this tx, these properties must be unique wherever they
    // appear" — matching how `f:enforceUnique` annotations work
    // when configured in the default graph against a per-graph
    // tx.
    if let Some(iris) = inline_unique_properties.filter(|v| !v.is_empty()) {
        let snapshot = view.db();
        let inline_sids: FxHashSet<Sid> = iris
            .iter()
            .filter_map(|iri| snapshot.encode_iri(iri))
            .collect();
        if !inline_sids.is_empty() {
            for g_id in affected_graph_ids(view, graph_delta) {
                per_graph_unique
                    .entry(g_id)
                    .or_default()
                    .extend(inline_sids.iter().cloned());
            }
        }
    }

    if per_graph_unique.is_empty() {
        return Ok(());
    }
    enforce_unique_constraints(view, &per_graph_unique, graph_delta).await?;
    Ok(())
}

/// Derive the set of graph IDs touched by staged flakes. Used by
/// both the config-resolved constraints path and the inline
/// `opts.uniqueProperties` path so they enforce against the same
/// set of graphs.
fn affected_graph_ids(
    view: &StagedLedger,
    graph_delta: &FxHashMap<u16, String>,
) -> FxHashSet<GraphId> {
    let snapshot = view.db();
    let mut sid_to_gid: HashMap<Sid, GraphId> = HashMap::new();
    for (&g_id, iri) in graph_delta {
        if let Some(sid) = snapshot.encode_iri(iri) {
            sid_to_gid.insert(sid, g_id);
        }
    }
    for (g_id, iri) in snapshot.graph_registry.iter_entries() {
        if let Some(sid) = snapshot.encode_iri(iri) {
            sid_to_gid.entry(sid).or_insert(g_id);
        }
    }

    let mut out: FxHashSet<GraphId> = FxHashSet::default();
    for flake in view.staged_flakes() {
        if !flake.op {
            continue;
        }
        let g_id = match &flake.g {
            None => 0u16,
            Some(g_sid) => sid_to_gid.get(g_sid).copied().unwrap_or(0),
        };
        out.insert(g_id);
    }
    out
}

/// Resolve per-graph unique property SIDs from `f:enforceUnique` annotations.
///
/// For each graph affected by staged flakes, resolves the effective transact
/// config, loads constraint annotations from the configured source graphs,
/// and returns a map of graph_id → set of property SIDs that must be unique.
///
/// Returns an empty map when no uniqueness constraints are configured (fast path).
async fn resolve_per_graph_unique_sids(
    view: &StagedLedger,
    config: &LedgerConfig,
    graph_delta: &FxHashMap<u16, String>,
    resolve_ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
) -> std::result::Result<HashMap<GraphId, FxHashSet<Sid>>, fluree_db_transact::TransactError> {
    let snapshot = view.db();
    let affected_g_ids = affected_graph_ids(view, graph_delta);
    let mut per_graph: HashMap<GraphId, FxHashSet<Sid>> = HashMap::new();

    for &g_id in &affected_g_ids {
        // Resolve graph IRI for per-graph config lookup
        let graph_iri = if g_id == 0 {
            None
        } else {
            graph_delta
                .get(&g_id)
                .map(std::string::String::as_str)
                .or_else(|| snapshot.graph_registry.iri_for_graph_id(g_id))
        };

        let resolved = config_resolver::resolve_effective_config(config, graph_iri);
        let transact_config = match config_resolver::merge_transact_opts(&resolved) {
            Some(tc) => tc,
            None => continue,
        };

        // Split constraint sources by locality. Local sources read
        // annotations from a graph on the data ledger; cross-ledger
        // sources resolve via the shared cross-ledger resolver against
        // a model ledger and translate back into D's Sid space.
        let mut unique_sids = FxHashSet::default();

        if transact_config.constraints_sources.is_empty() {
            // Default: annotations in the default graph (g_id=0)
            let annotations = read_enforce_unique_from_graph(view, 0u16).await?;
            unique_sids.extend(annotations);
        } else {
            let mut local_sources: Vec<&fluree_db_core::ledger_config::GraphSourceRef> = Vec::new();
            let mut cross_sources: Vec<&fluree_db_core::ledger_config::GraphSourceRef> = Vec::new();
            for source in &transact_config.constraints_sources {
                if source.ledger.is_some() {
                    cross_sources.push(source);
                } else {
                    local_sources.push(source);
                }
            }

            // Local: resolve to g_ids and scan.
            if !local_sources.is_empty() {
                let local_g_ids = resolve_constraint_source_g_ids_for(&local_sources, snapshot)?;
                for source_g_id in local_g_ids {
                    let annotations = read_enforce_unique_from_graph(view, source_g_id).await?;
                    unique_sids.extend(annotations);
                }
            }

            // Cross-ledger: materialize via the resolver, translate
            // each property IRI back to a Sid on D.
            for source in cross_sources {
                let resolved = crate::cross_ledger::resolve_graph_ref(
                    source,
                    crate::cross_ledger::ArtifactKind::Constraints,
                    resolve_ctx,
                )
                .await
                .map_err(|e| {
                    // Resolver errors are operator-facing and need to
                    // fail the transaction clearly. Wrap in
                    // TransactError::Parse so the staging pipeline can
                    // propagate; the API layer (ApiError::Transact) is
                    // the resulting HTTP class. The detail string
                    // preserves the underlying CrossLedgerError display
                    // so operators see model_ledger_id / graph_iri /
                    // failure variant in the body.
                    fluree_db_transact::TransactError::Parse(format!(
                        "f:constraintsSource cross-ledger resolution failed: {e}"
                    ))
                })?;
                let crate::cross_ledger::GovernanceArtifact::Constraints(wire) = &resolved.artifact
                else {
                    return Err(fluree_db_transact::TransactError::Parse(
                        "cross-ledger resolver returned a non-Constraints \
                         artifact for ArtifactKind::Constraints (bug in \
                         resolver dispatch)"
                            .into(),
                    ));
                };
                for sid in wire.translate_to_sids(snapshot) {
                    unique_sids.insert(sid);
                }
            }
        }

        if !unique_sids.is_empty() {
            per_graph.insert(g_id, unique_sids);
        }
    }

    Ok(per_graph)
}

/// Resolve a `GraphSourceRef` list to graph IDs on the local ledger.
///
/// Maps each `f:graphSelector` IRI to a concrete graph ID:
/// - `f:defaultGraph` → 0
/// - Named graph IRI → lookup in `GraphRegistry`
///
/// Fails closed: dropping a constraint source silently would weaken
/// uniqueness enforcement under a misconfiguration, so unknown
/// selectors and unsupported `GraphSourceRef` fields (`f:atT`,
/// `f:trustPolicy`, `f:rollbackGuard`) return a parse error.
///
/// `f:ledger` (cross-ledger) is supported via
/// `cross_ledger::resolve_graph_ref` and is dispatched at
/// [`resolve_per_graph_unique_sids`]; this function sees only
/// already-partitioned local sources and rejects `f:ledger` as a
/// defensive guard against bypassing the partition.
fn resolve_constraint_source_g_ids_for(
    sources: &[&fluree_db_core::ledger_config::GraphSourceRef],
    snapshot: &fluree_db_core::LedgerSnapshot,
) -> std::result::Result<Vec<GraphId>, fluree_db_transact::TransactError> {
    let mut g_ids = Vec::new();
    for source in sources {
        let source = *source;
        if source.ledger.is_some() {
            return Err(fluree_db_transact::TransactError::Parse(
                "f:constraintsSource with cross-ledger f:ledger reference is not yet supported"
                    .into(),
            ));
        }
        if source.at_t.is_some() {
            return Err(fluree_db_transact::TransactError::Parse(
                "f:constraintsSource with f:atT (temporal pinning) is not yet supported".into(),
            ));
        }
        if source.trust_policy.is_some() {
            return Err(fluree_db_transact::TransactError::Parse(
                "f:constraintsSource with f:trustPolicy is not yet supported".into(),
            ));
        }
        if source.rollback_guard.is_some() {
            return Err(fluree_db_transact::TransactError::Parse(
                "f:constraintsSource with f:rollbackGuard is not yet supported".into(),
            ));
        }

        let g_id = match source.graph_selector.as_deref() {
            Some(iri) if iri == config_iris::DEFAULT_GRAPH => Some(0u16),
            Some(iri) => snapshot.graph_registry.graph_id_for_iri(iri),
            None => Some(0u16), // no selector → default graph
        };
        match g_id {
            Some(id) => g_ids.push(id),
            None => {
                return Err(fluree_db_transact::TransactError::Parse(format!(
                    "f:constraintsSource graph '{}' not found in this ledger's graph registry",
                    source.graph_selector.as_deref().unwrap_or("<none>"),
                )));
            }
        }
    }
    Ok(g_ids)
}

/// Read `f:enforceUnique true` annotations from a single graph.
///
/// Queries the POST index at the pre-transaction state for all subjects
/// where `?prop f:enforceUnique true`. Returns the set of property SIDs.
async fn read_enforce_unique_from_graph(
    view: &StagedLedger,
    source_g_id: GraphId,
) -> std::result::Result<Vec<Sid>, fluree_db_transact::TransactError> {
    let snapshot = view.db();

    let enforce_unique_sid = match snapshot.encode_iri(config_iris::ENFORCE_UNIQUE) {
        Some(sid) => sid,
        None => return Ok(Vec::new()),
    };

    let xsd_boolean_sid = match snapshot.encode_iri("http://www.w3.org/2001/XMLSchema#boolean") {
        Some(sid) => sid,
        None => return Ok(Vec::new()),
    };

    // Query: all flakes where p=f:enforceUnique, o=true, dt=xsd:boolean
    // Uses pre-txn state (base novelty) for lagging annotation semantics.
    let base = view.base();
    let match_val = RangeMatch::predicate_object(enforce_unique_sid, FlakeValue::Boolean(true))
        .with_datatype(xsd_boolean_sid);

    let flakes = range_with_overlay(
        &base.snapshot,
        source_g_id,
        &*base.novelty,
        IndexType::Post,
        RangeTest::Eq,
        match_val,
        RangeOptions::new().with_to_t(base.t()),
    )
    .await
    .map_err(fluree_db_transact::TransactError::from)?;

    // Each matching flake's subject is a property IRI that has f:enforceUnique true
    let props: Vec<Sid> = flakes.iter().map(|f| f.s.clone()).collect();
    Ok(props)
}

/// Enforce unique constraints on staged flakes.
///
/// For each affected graph, checks that no two subjects hold the same value
/// for any property marked `f:enforceUnique`. Uses the POST index with
/// datatype-aware matching and stale-removal (last-op-wins).
///
/// Returns `Ok(())` if no violations, or a `UniqueConstraintViolation` error.
async fn enforce_unique_constraints(
    view: &StagedLedger,
    per_graph_unique: &HashMap<GraphId, FxHashSet<Sid>>,
    graph_delta: &FxHashMap<u16, String>,
) -> std::result::Result<(), fluree_db_transact::TransactError> {
    // Fast path: nothing configured
    if per_graph_unique.is_empty() {
        return Ok(());
    }

    let snapshot = view.db();

    // Build reverse map for flake graph resolution
    let mut sid_to_gid: HashMap<Sid, GraphId> = HashMap::new();
    for (&g_id, iri) in graph_delta {
        if let Some(sid) = snapshot.encode_iri(iri) {
            sid_to_gid.insert(sid, g_id);
        }
    }
    for (g_id, iri) in snapshot.graph_registry.iter_entries() {
        if let Some(sid) = snapshot.encode_iri(iri) {
            sid_to_gid.entry(sid).or_insert(g_id);
        }
    }

    // Collect distinct (g_id, p, o) keys from staged asserts on unique properties.
    // Uniqueness ignores datatype and language tag — the key is the storage-layer
    // value identity (FlakeValue), not the RDF datatype IRI.
    let mut keys_to_check: FxHashSet<(GraphId, Sid, FlakeValue)> = FxHashSet::default();
    for flake in view.staged_flakes() {
        if !flake.op {
            continue;
        }
        let g_id = match &flake.g {
            None => 0u16,
            Some(g_sid) => sid_to_gid.get(g_sid).copied().unwrap_or(0),
        };
        if let Some(unique_set) = per_graph_unique.get(&g_id) {
            if unique_set.contains(&flake.p) {
                keys_to_check.insert((g_id, flake.p.clone(), flake.o.clone()));
            }
        }
    }

    // For each unique key, query POST index to check for multiple active subjects.
    // No .with_datatype() — matches all datatypes for the same (p, o) value.
    for (g_id, p, o) in &keys_to_check {
        let match_val = RangeMatch::predicate_object(p.clone(), o.clone());

        // Query with post-staging overlay: sees committed + staged data.
        // Stale removal ensures only currently-active assertions are returned.
        let flakes = range_with_overlay(
            snapshot,
            *g_id,
            view,
            IndexType::Post,
            RangeTest::Eq,
            match_val,
            RangeOptions::new().with_to_t(view.staged_t()),
        )
        .await
        .map_err(fluree_db_transact::TransactError::from)?;

        // Count distinct subjects with active assertions
        let mut seen_subjects: FxHashSet<&Sid> = FxHashSet::default();
        for f in &flakes {
            seen_subjects.insert(&f.s);
        }

        if seen_subjects.len() > 1 {
            // Build a descriptive error with decoded IRIs
            let property_iri = snapshot.decode_sid(p).unwrap_or_else(|| format!("{p:?}"));
            let graph_label = if *g_id == 0 {
                "default".to_string()
            } else {
                graph_delta
                    .get(g_id)
                    .cloned()
                    .or_else(|| {
                        snapshot
                            .graph_registry
                            .iri_for_graph_id(*g_id)
                            .map(std::string::ToString::to_string)
                    })
                    .unwrap_or_else(|| format!("g_id={g_id}"))
            };
            let value_str = format!("{o:?}");

            // Pick two subjects for the error message
            let mut subj_iter = seen_subjects.iter();
            let existing = subj_iter.next().unwrap();
            let conflicting = subj_iter.next().unwrap();
            let existing_iri = snapshot
                .decode_sid(existing)
                .unwrap_or_else(|| format!("{existing:?}"));
            let new_iri = snapshot
                .decode_sid(conflicting)
                .unwrap_or_else(|| format!("{conflicting:?}"));

            return Err(
                fluree_db_transact::TransactError::UniqueConstraintViolation {
                    property: property_iri,
                    value: value_str,
                    graph: graph_label,
                    existing_subject: existing_iri,
                    new_subject: new_iri,
                },
            );
        }
    }

    Ok(())
}

// =============================================================================
// Indexing Mode Configuration
// =============================================================================

/// Indexing mode configuration (server-level setting)
///
/// Controls whether transactions trigger background indexing or return hints
/// for an external indexer (e.g., Lambda).
#[derive(Debug, Clone, Default)]
pub enum IndexingMode {
    /// Disabled mode (Lambda/external indexer)
    ///
    /// Transactions complete without triggering indexing. The `IndexingStatus`
    /// in the result provides hints for external indexers.
    #[default]
    Disabled,
    /// Background mode with coalescing handle
    ///
    /// Triggers background indexing when `indexing_needed` is true.
    /// Uses a depth-1 coalescing queue (latest wins per ledger).
    Background(IndexerHandle),
}

impl IndexingMode {
    /// Returns true if background indexing is enabled
    pub fn is_enabled(&self) -> bool {
        matches!(self, IndexingMode::Background(_))
    }

    /// Returns the indexer handle if in background mode
    pub fn handle(&self) -> Option<&IndexerHandle> {
        match self {
            IndexingMode::Disabled => None,
            IndexingMode::Background(h) => Some(h),
        }
    }
}

/// Indexing status after a transaction
///
/// Provides hints for external indexers (in disabled mode) and confirms
/// indexing was triggered (in background mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingStatus {
    /// True if indexing is enabled (background mode)
    pub enabled: bool,
    /// True if novelty is above `reindex_min_bytes` after commit
    pub needed: bool,
    /// Current novelty size in bytes
    pub novelty_size: usize,
    /// Transaction time of the indexed state
    pub index_t: i64,
    /// Transaction time after this commit
    pub commit_t: i64,
}

/// Result of a committed transaction
pub struct TransactResult {
    pub receipt: CommitReceipt,
    pub ledger: LedgerState,
    /// Indexing status and hints
    pub indexing: IndexingStatus,
}

impl std::fmt::Debug for TransactResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactResult")
            .field("receipt", &self.receipt)
            .field("indexing", &self.indexing)
            .finish_non_exhaustive()
    }
}

/// Result of a committed transaction via reference (cache already updated)
///
/// Unlike `TransactResult`, this does not contain the ledger state because
/// the LedgerHandle's internal state has already been updated in place.
#[derive(Debug)]
pub struct TransactResultRef {
    pub receipt: CommitReceipt,
    /// Indexing status and hints
    pub indexing: IndexingStatus,
    /// Tracking tally (fuel, time, policy) when tracking was requested
    pub tally: Option<TrackingTally>,
}

/// Result of staging a transaction
pub struct StageResult {
    pub view: StagedLedger,
    pub ns_registry: NamespaceRegistry,
    /// User-provided transaction metadata (extracted from envelope-form JSON-LD)
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Named graph IRI to g_id mappings introduced by this transaction
    pub graph_delta: rustc_hash::FxHashMap<u16, String>,
}

/// Convert named graph blocks to TripleTemplates with proper graph_id assignments.
///
/// Returns a tuple of (templates, graph_delta) where:
/// - templates: Vec<TripleTemplate> with graph_id set for each template
/// - graph_delta: HashMap<u16, String> mapping g_id to graph IRI
///
/// Graph IDs are assigned starting at 2 (0=default, 1=txn-meta).
fn convert_named_graphs_to_templates(
    named_graphs: &[NamedGraphBlock],
    ns_registry: &mut NamespaceRegistry,
) -> Result<(Vec<TripleTemplate>, rustc_hash::FxHashMap<u16, String>)> {
    use fluree_db_transact::{RawObject, RawTerm};

    let mut templates = Vec::new();
    let mut graph_delta: rustc_hash::FxHashMap<u16, String> = rustc_hash::FxHashMap::default();
    let mut iri_to_id: std::collections::HashMap<String, u16> = std::collections::HashMap::new();
    let mut next_graph_id: u16 = 3; // 0=default, 1=txn-meta, 2=config

    // Helper to expand prefixed name to full IRI
    fn expand_prefixed_name(
        prefix: &str,
        local: &str,
        prefixes: &rustc_hash::FxHashMap<String, String>,
    ) -> Result<String> {
        prefixes
            .get(prefix)
            .map(|ns| format!("{ns}{local}"))
            .ok_or_else(|| ApiError::query(format!("undefined prefix: {prefix}")))
    }

    // Helper to convert RawTerm to TemplateTerm
    fn convert_term(
        term: &RawTerm,
        prefixes: &rustc_hash::FxHashMap<String, String>,
        ns_registry: &mut NamespaceRegistry,
    ) -> Result<TemplateTerm> {
        match term {
            RawTerm::Iri(iri) => {
                if let Some(local) = iri.strip_prefix("_:") {
                    Ok(TemplateTerm::BlankNode(local.to_string()))
                } else {
                    Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(iri)))
                }
            }
            RawTerm::PrefixedName { prefix, local } => {
                let iri = expand_prefixed_name(prefix, local, prefixes)?;
                Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(&iri)))
            }
        }
    }

    // Helper to convert RawObject to TemplateTerm and optional datatype/language
    fn convert_object(
        obj: &RawObject,
        prefixes: &rustc_hash::FxHashMap<String, String>,
        ns_registry: &mut NamespaceRegistry,
    ) -> Result<(TemplateTerm, Option<DatatypeConstraint>)> {
        use fluree_db_core::FlakeValue;
        match obj {
            RawObject::Iri(iri) => {
                if let Some(local) = iri.strip_prefix("_:") {
                    Ok((TemplateTerm::BlankNode(local.to_string()), None))
                } else {
                    Ok((TemplateTerm::Sid(ns_registry.sid_for_iri(iri)), None))
                }
            }
            RawObject::PrefixedName { prefix, local } => {
                let iri = expand_prefixed_name(prefix, local, prefixes)?;
                Ok((TemplateTerm::Sid(ns_registry.sid_for_iri(&iri)), None))
            }
            RawObject::String(s) => Ok((TemplateTerm::Value(FlakeValue::String(s.clone())), None)),
            RawObject::Integer(n) => Ok((TemplateTerm::Value(FlakeValue::Long(*n)), None)),
            RawObject::Double(n) => Ok((TemplateTerm::Value(FlakeValue::Double(*n)), None)),
            RawObject::Boolean(b) => Ok((TemplateTerm::Value(FlakeValue::Boolean(*b)), None)),
            RawObject::LangString { value, lang } => Ok((
                TemplateTerm::Value(FlakeValue::String(value.clone())),
                Some(DatatypeConstraint::LangTag(Arc::from(lang.as_str()))),
            )),
            RawObject::TypedLiteral { value, datatype } => {
                let dt_sid = ns_registry.sid_for_iri(datatype);
                Ok((
                    TemplateTerm::Value(FlakeValue::String(value.clone())),
                    Some(DatatypeConstraint::Explicit(dt_sid)),
                ))
            }
        }
    }

    for block in named_graphs {
        // Assign a graph_id to this graph IRI (or reuse existing)
        let g_id = *iri_to_id.entry(block.iri.clone()).or_insert_with(|| {
            let id = next_graph_id;
            graph_delta.insert(id, block.iri.clone());
            next_graph_id += 1;
            id
        });

        // Convert each triple in this graph block
        for triple in &block.triples {
            let subject = triple
                .subject
                .as_ref()
                .ok_or_else(|| ApiError::query("named graph triple missing subject"))?;
            let subject_term = convert_term(subject, &block.prefixes, ns_registry)?;
            let predicate_term = convert_term(&triple.predicate, &block.prefixes, ns_registry)?;

            for obj in &triple.objects {
                let (object_term, dtc) = convert_object(obj, &block.prefixes, ns_registry)?;
                let mut template =
                    TripleTemplate::new(subject_term.clone(), predicate_term.clone(), object_term);
                template = template.with_graph_id(g_id);
                if let Some(dtc) = dtc {
                    template = template.with_dtc(dtc);
                }
                templates.push(template);
            }
        }
    }

    Ok((templates, graph_delta))
}

impl crate::Fluree {
    /// Stage a transaction against a ledger (no persistence).
    ///
    /// Respects `opts.max-fuel` in the transaction JSON for fuel limits (consistent with query behavior).
    pub async fn stage_transaction(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        index_config: Option<&IndexConfig>,
    ) -> Result<StageResult> {
        self.stage_transaction_with_trig_meta(
            ledger,
            txn_type,
            txn_json,
            txn_opts,
            index_config,
            None,
        )
        .await
    }

    /// Stage a transaction with optional TriG transaction metadata.
    ///
    /// This is the internal implementation that handles both JSON-LD and TriG inputs.
    /// For TriG inputs, the `trig_meta` parameter contains pre-parsed metadata that
    /// will be resolved and merged into the transaction's txn_meta.
    pub async fn stage_transaction_with_trig_meta(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        index_config: Option<&IndexConfig>,
        trig_meta: Option<&RawTrigMeta>,
    ) -> Result<StageResult> {
        self.stage_transaction_with_named_graphs(
            ledger,
            txn_type,
            txn_json,
            txn_opts,
            index_config,
            trig_meta,
            &[],
        )
        .await
    }

    /// Stage a transaction with optional TriG transaction metadata and named graphs.
    ///
    /// This is the full implementation that handles:
    /// - JSON-LD transactions (default graph)
    /// - TriG txn-meta (commit metadata)
    /// - TriG named graphs (user-defined graphs with separate g_id)
    #[allow(clippy::too_many_arguments)]
    pub async fn stage_transaction_with_named_graphs(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        index_config: Option<&IndexConfig>,
        trig_meta: Option<&RawTrigMeta>,
        named_graphs: &[NamedGraphBlock],
    ) -> Result<StageResult> {
        self.stage_transaction_with_named_graphs_tracked(
            ledger,
            txn_type,
            txn_json,
            txn_opts,
            index_config,
            trig_meta,
            named_graphs,
            None,
            None,
        )
        .await
    }

    /// Stage a transaction with optional TriG metadata, named graphs, external tracker,
    /// and policy context.
    ///
    /// When `external_tracker` is provided, it is used for fuel accounting instead of
    /// the default limits-only tracker derived from the transaction body opts.
    /// When `policy` is provided, modify policies are enforced on staged flakes
    /// (unless the wrapper is root).
    #[allow(clippy::too_many_arguments)]
    pub async fn stage_transaction_with_named_graphs_tracked(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        index_config: Option<&IndexConfig>,
        trig_meta: Option<&RawTrigMeta>,
        named_graphs: &[NamedGraphBlock],
        external_tracker: Option<&Tracker>,
        policy: Option<&crate::PolicyContext>,
    ) -> Result<StageResult> {
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);

        // Handle case where default graph is empty but named graphs are present
        // (e.g., TriG with only GRAPH blocks and no default graph triples)
        let mut txn = if is_empty_default_graph(txn_json) && !named_graphs.is_empty() {
            // Create empty transaction of the appropriate type
            match txn_type {
                TxnType::Insert => Txn::insert().with_opts(txn_opts),
                TxnType::Upsert => Txn::upsert().with_opts(txn_opts),
                TxnType::Update => Txn::update().with_opts(txn_opts),
            }
        } else {
            let parse_span = tracing::debug_span!("txn_parse", txn_type = ?txn_type);
            let _guard = parse_span.enter();
            parse_transaction(txn_json, txn_type, txn_opts, &mut ns_registry)?
        };

        // If TriG metadata was extracted, resolve it and merge into txn_meta
        if let Some(raw_meta) = trig_meta {
            let resolved = resolve_trig_meta(raw_meta, &mut ns_registry)?;
            txn.txn_meta.extend(resolved);
        }

        // Convert named graph blocks to TripleTemplates and merge into the transaction
        if !named_graphs.is_empty() {
            let (named_graph_templates, named_graph_delta) =
                convert_named_graphs_to_templates(named_graphs, &mut ns_registry)?;
            txn.insert_templates.extend(named_graph_templates);
            txn.graph_delta.extend(named_graph_delta);
        }

        // Extract txn_meta, graph_delta, and any inline uniqueness
        // properties before staging consumes the Txn.
        let txn_meta = txn.txn_meta.clone();
        let graph_delta = txn.graph_delta.clone();
        let inline_unique_properties = txn.opts.unique_properties.clone();

        // Use external tracker if provided, otherwise fall back to limits-only tracker
        let limits_tracker;
        let tracker = match external_tracker {
            Some(t) => t,
            None => {
                limits_tracker = tracker_for_limits(txn_json);
                &limits_tracker
            }
        };

        let mut options = match index_config {
            Some(cfg) => StageOptions::new().with_index_config(cfg),
            None => StageOptions::default(),
        };
        if tracker.is_enabled() {
            options = options.with_tracker(tracker);
        }
        if let Some(p) = policy {
            options = options.with_policy(p);
        }

        // Single per-tx ResolveCtx shared by every cross-ledger
        // governance lookup (SHACL shapes, constraints, …) so the
        // tx observes a coherent `resolved_t` per model ledger
        // across all subsystems. `ledger_id_owned` keeps a string
        // alive past the `ledger` move into `stage_with_config_shacl`.
        let ledger_id_owned: String = ledger.snapshot.ledger_id.to_string();
        let mut resolve_ctx = crate::cross_ledger::ResolveCtx::new(&ledger_id_owned, self);

        #[cfg(feature = "shacl")]
        let (view, ns_registry) =
            stage_with_config_shacl(ledger, txn, ns_registry, options, &mut resolve_ctx).await?;
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = stage_txn(ledger, txn, ns_registry, options).await?;

        // Enforce uniqueness constraints (independent of shacl feature)
        enforce_unique_after_staging(
            &view,
            &graph_delta,
            &mut resolve_ctx,
            inline_unique_properties.as_deref(),
        )
        .await?;

        Ok(StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        })
    }

    /// Stage a pre-built transaction IR (bypasses JSON/Turtle parsing).
    ///
    /// This is used for SPARQL UPDATE where the transaction has already been
    /// lowered to the IR representation. When `policy` is `Some`, modify policies
    /// are enforced on staged flakes (unless the wrapper is root).
    pub async fn stage_transaction_from_txn(
        &self,
        ledger: LedgerState,
        txn: fluree_db_transact::Txn,
        index_config: Option<&IndexConfig>,
        policy: Option<&crate::PolicyContext>,
        tracker: Option<&Tracker>,
    ) -> Result<StageResult> {
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);

        // Adopt any namespace allocations the lowering step already made
        // (e.g. `lower_sparql_update` allocates IRIs against a caller-owned
        // registry to build the templates' Sids). The staging registry must
        // both (a) know about the codes for in-session lookups and (b)
        // record them in its persistence delta so the commit envelope
        // captures them — otherwise the committed snapshot omits the
        // mapping and post-commit SELECT can't resolve the predicate IRI
        // back to the same Sid the flake was stored under.
        //
        // Conflicts here are retry-safe: they happen when two concurrent
        // SPARQL UPDATEs both lower against the same pre-commit snapshot
        // and pick the same first-time namespace code for different
        // prefixes. The second writer should re-lower against the latest
        // snapshot (which now sees the first writer's namespaces) — surface
        // as `NamespaceConflict` so callers in the same family as
        // `CommitConflict` / `PublishLostRace` can treat it uniformly.
        if !txn.namespace_delta.is_empty() {
            ns_registry
                .adopt_delta_for_persistence(&txn.namespace_delta)
                .map_err(|e| {
                    ApiError::Transact(fluree_db_transact::TransactError::NamespaceConflict(
                        e.to_string(),
                    ))
                })?;
        }

        // Extract txn_meta, graph_delta, and any inline uniqueness
        // properties before staging consumes the Txn.
        let txn_meta = txn.txn_meta.clone();
        let graph_delta = txn.graph_delta.clone();
        let inline_unique_properties = txn.opts.unique_properties.clone();

        let mut options = match index_config {
            Some(cfg) => StageOptions::new().with_index_config(cfg),
            None => StageOptions::default(),
        };
        if let Some(p) = policy {
            options = options.with_policy(p);
        }
        if let Some(t) = tracker {
            if t.is_enabled() {
                options = options.with_tracker(t);
            }
        }

        // Single per-tx ResolveCtx; see comment on the matching
        // block above for the consistency rationale.
        let ledger_id_owned: String = ledger.snapshot.ledger_id.to_string();
        let mut resolve_ctx = crate::cross_ledger::ResolveCtx::new(&ledger_id_owned, self);

        #[cfg(feature = "shacl")]
        let (view, ns_registry) =
            stage_with_config_shacl(ledger, txn, ns_registry, options, &mut resolve_ctx).await?;
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = stage_txn(ledger, txn, ns_registry, options).await?;

        // Enforce uniqueness constraints (independent of shacl feature)
        enforce_unique_after_staging(
            &view,
            &graph_delta,
            &mut resolve_ctx,
            inline_unique_properties.as_deref(),
        )
        .await?;

        Ok(StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        })
    }

    /// Stage a transaction with policy enforcement + tracking (opts.meta / opts.max-fuel).
    ///
    /// This is the transaction-side equivalent of `query_connection_tracked_with_policy`.
    pub(crate) async fn stage_transaction_tracked_with_policy(
        &self,
        ledger: LedgerState,
        input: TrackedTransactionInput<'_>,
        index_config: Option<&IndexConfig>,
        tracker: &Tracker,
    ) -> std::result::Result<StageResult, TrackedErrorResponse> {
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let txn = {
            let parse_span = tracing::debug_span!("txn_parse", txn_type = ?input.txn_type);
            let _guard = parse_span.enter();
            parse_transaction(
                input.txn_json,
                input.txn_type,
                input.txn_opts,
                &mut ns_registry,
            )
            .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?
        };

        // Extract txn_meta, graph_delta, and any inline uniqueness
        // properties before staging consumes the Txn.
        let txn_meta = txn.txn_meta.clone();
        let graph_delta = txn.graph_delta.clone();
        let inline_unique_properties = txn.opts.unique_properties.clone();

        // Build stage options with policy and tracker
        let mut options = StageOptions::new()
            .with_policy(input.policy)
            .with_tracker(tracker);
        if let Some(cfg) = index_config {
            options = options.with_index_config(cfg);
        }

        // Single per-tx ResolveCtx; see comment on the matching
        // block above for the consistency rationale.
        let ledger_id_owned: String = ledger.snapshot.ledger_id.to_string();
        let mut resolve_ctx = crate::cross_ledger::ResolveCtx::new(&ledger_id_owned, self);

        #[cfg(feature = "shacl")]
        let (view, ns_registry) =
            stage_with_config_shacl(ledger, txn, ns_registry, options, &mut resolve_ctx)
                .await
                .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?;
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = stage_txn(ledger, txn, ns_registry, options)
            .await
            .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?;

        // Enforce uniqueness constraints (independent of shacl feature)
        enforce_unique_after_staging(
            &view,
            &graph_delta,
            &mut resolve_ctx,
            inline_unique_properties.as_deref(),
        )
        .await
        .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?;

        Ok(StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        })
    }

    /// Convenience: stage + commit + tracking + policy.
    ///
    /// Returns `(TransactResult, TrackingTally?)` on success, or `TrackedErrorResponse` on error.
    pub async fn transact_tracked_with_policy(
        &self,
        ledger: LedgerState,
        input: TrackedTransactionInput<'_>,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> std::result::Result<(TransactResult, Option<TrackingTally>), TrackedErrorResponse> {
        let store_raw_txn = input.txn_opts.store_raw_txn.unwrap_or(false);
        let txn_json_for_commit = input.txn_json.clone();

        let opts = input.txn_json.as_object().and_then(|o| o.get("opts"));
        let tracker = Tracker::new(TrackingOptions::from_opts_value(opts));

        // Spawn raw transaction upload in parallel with staging when opted in.
        // Upload overlaps with parse/policy/flake-generation CPU work; commit()
        // awaits the handle just before writing the commit blob, so durability
        // is preserved but serial latency is eliminated on fast paths.
        let commit_opts = if commit_opts.raw_txn.is_none()
            && commit_opts.raw_txn_upload.is_none()
            && store_raw_txn
        {
            let content_store = self.content_store(ledger.ledger_id());
            commit_opts.with_raw_txn_spawned(content_store, txn_json_for_commit)
        } else {
            commit_opts
        };

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = self
            .stage_transaction_tracked_with_policy(ledger, input, Some(index_config), &tracker)
            .await?;

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // Commit (no-op updates handled by existing transact; for the tracked path we just mirror it).
        let (receipt, ledger) = self
            .commit_staged(view, ns_registry, index_config, commit_opts)
            .await
            .map_err(|e| TrackedErrorResponse::new(500, e.to_string(), tracker.tally()))?;

        // Compute indexing status AFTER publish_commit succeeds
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);
        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok((
            TransactResult {
                receipt,
                ledger,
                indexing: indexing_status,
            },
            tracker.tally(),
        ))
    }

    /// Commit a staged transaction (persists commit record + publishes nameservice head).
    pub async fn commit_staged(
        &self,
        view: StagedLedger,
        ns_registry: NamespaceRegistry,
        index_config: &IndexConfig,
        commit_opts: CommitOpts,
    ) -> Result<(CommitReceipt, LedgerState)> {
        let content_store = self.content_store(view.db().ledger_id.as_str());
        let publisher = self.publisher()?;
        let (receipt, ledger) = commit_txn(
            view,
            ns_registry,
            &content_store,
            publisher,
            index_config,
            commit_opts,
        )
        .await?;
        Ok((receipt, ledger))
    }

    /// Convenience: stage + commit.
    ///
    /// After a successful commit (including nameservice publish), this method:
    /// 1. Computes `IndexingStatus` with hints for external indexers
    /// 2. If `indexing_mode` is `Background` and `indexing_needed`, triggers indexing
    pub async fn transact(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        let store_raw_txn = txn_opts.store_raw_txn.unwrap_or(false);

        // Spawn raw_txn upload in parallel with staging when opted in.
        let commit_opts = if commit_opts.raw_txn.is_none()
            && commit_opts.raw_txn_upload.is_none()
            && store_raw_txn
        {
            let content_store = self.content_store(ledger.ledger_id());
            commit_opts.with_raw_txn_spawned(content_store, txn_json.clone())
        } else {
            commit_opts
        };

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = self
            .stage_transaction(ledger, txn_type, txn_json, txn_opts, Some(index_config))
            .await?;

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // No-op updates: if WHERE matches nothing (or templates produce no flakes),
        // return success without committing.
        //
        // This allows patterns like "delete if exists, then insert" to execute safely when
        // there are no matches, and supports conditional updates.
        let (receipt, ledger) =
            if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
                let (base, flakes) = view.into_parts();
                debug_assert!(
                    flakes.is_empty(),
                    "no-op transaction path requires zero staged flakes"
                );
                (
                    CommitReceipt {
                        commit_id: ContentId::new(ContentKind::Commit, &[]),
                        t: base.t(),
                        flake_count: 0,
                    },
                    base,
                )
            } else {
                self.commit_staged(view, ns_registry, index_config, commit_opts)
                    .await?
            };

        // Compute indexing status AFTER publish_commit succeeds
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        // Trigger indexing AFTER publish_commit succeeds (fast operation)
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok(TransactResult {
            receipt,
            ledger,
            indexing: indexing_status,
        })
    }

    /// Execute a transaction with optional TriG metadata.
    ///
    /// This is similar to `transact` but accepts pre-extracted TriG metadata
    /// from Turtle inputs that had GRAPH blocks.
    #[allow(clippy::too_many_arguments)]
    pub async fn transact_with_trig_meta(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
        trig_meta: Option<&RawTrigMeta>,
    ) -> Result<TransactResult> {
        let store_raw_txn = txn_opts.store_raw_txn.unwrap_or(false);

        // Spawn raw_txn upload in parallel with staging when opted in.
        let commit_opts = if commit_opts.raw_txn.is_none()
            && commit_opts.raw_txn_upload.is_none()
            && store_raw_txn
        {
            let content_store = self.content_store(ledger.ledger_id());
            commit_opts.with_raw_txn_spawned(content_store, txn_json.clone())
        } else {
            commit_opts
        };

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = self
            .stage_transaction_with_trig_meta(
                ledger,
                txn_type,
                txn_json,
                txn_opts,
                Some(index_config),
                trig_meta,
            )
            .await?;

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // No-op updates: if WHERE matches nothing (or templates produce no flakes),
        // return success without committing.
        let (receipt, ledger) =
            if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
                let (base, flakes) = view.into_parts();
                debug_assert!(
                    flakes.is_empty(),
                    "no-op transaction path requires zero staged flakes"
                );
                (
                    CommitReceipt {
                        commit_id: ContentId::new(ContentKind::Commit, &[]),
                        t: base.t(),
                        flake_count: 0,
                    },
                    base,
                )
            } else {
                self.commit_staged(view, ns_registry, index_config, commit_opts)
                    .await?
            };

        // Compute indexing status AFTER publish_commit succeeds
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        // Trigger indexing AFTER publish_commit succeeds (fast operation)
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok(TransactResult {
            receipt,
            ledger,
            indexing: indexing_status,
        })
    }

    /// Execute a transaction with optional TriG metadata and named graphs.
    ///
    /// This is the full implementation that handles:
    /// - JSON-LD transactions (default graph)
    /// - TriG txn-meta (commit metadata)
    /// - TriG named graphs (user-defined graphs with separate g_id)
    #[allow(clippy::too_many_arguments)]
    pub async fn transact_with_named_graphs(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
        trig_meta: Option<&RawTrigMeta>,
        named_graphs: &[NamedGraphBlock],
    ) -> Result<TransactResult> {
        let store_raw_txn = txn_opts.store_raw_txn.unwrap_or(false);

        // Spawn raw_txn upload in parallel with staging when opted in.
        let commit_opts = if commit_opts.raw_txn.is_none()
            && commit_opts.raw_txn_upload.is_none()
            && store_raw_txn
        {
            let content_store = self.content_store(ledger.ledger_id());
            commit_opts.with_raw_txn_spawned(content_store, txn_json.clone())
        } else {
            commit_opts
        };

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = self
            .stage_transaction_with_named_graphs(
                ledger,
                txn_type,
                txn_json,
                txn_opts,
                Some(index_config),
                trig_meta,
                named_graphs,
            )
            .await?;

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // No-op updates: if WHERE matches nothing (or templates produce no flakes),
        // return success without committing.
        let (receipt, ledger) =
            if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
                let (base, flakes) = view.into_parts();
                debug_assert!(
                    flakes.is_empty(),
                    "no-op transaction path requires zero staged flakes"
                );
                (
                    CommitReceipt {
                        commit_id: ContentId::new(ContentKind::Commit, &[]),
                        t: base.t(),
                        flake_count: 0,
                    },
                    base,
                )
            } else {
                self.commit_staged(view, ns_registry, index_config, commit_opts)
                    .await?
            };

        // Compute indexing status AFTER publish_commit succeeds
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        // Trigger indexing AFTER publish_commit succeeds (fast operation)
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok(TransactResult {
            receipt,
            ledger,
            indexing: indexing_status,
        })
    }

    /// Insert new data into the ledger
    ///
    /// Fails if any subject with a concrete `@id` already has triples in the ledger.
    /// Blank nodes are always allowed (they generate fresh IDs).
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `data` - JSON-LD data to insert
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.insert(ledger, json!({
    ///     "@context": {"ex": "http://example.org/"},
    ///     "@id": "ex:alice",
    ///     "ex:name": "Alice",
    ///     "ex:age": 30
    /// })).await?;
    /// ```
    pub async fn insert(&self, ledger: LedgerState, data: &JsonValue) -> Result<TransactResult> {
        let index_config = self.default_index_config();
        self.transact(
            ledger,
            TxnType::Insert,
            data,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
    }

    /// Insert new data from Turtle format (direct flake path).
    ///
    /// Parses Turtle directly into assertion flakes, bypassing the
    /// JSON-LD / Txn IR intermediate representations.
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `turtle` - Turtle (TTL) format data
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.insert_turtle(ledger, r#"
    ///     @prefix ex: <http://example.org/> .
    ///     ex:alice ex:name "Alice" ;
    ///              ex:age 30 .
    /// "#).await?;
    /// ```
    pub async fn insert_turtle(&self, ledger: LedgerState, turtle: &str) -> Result<TransactResult> {
        let index_config = self.default_index_config();
        self.insert_turtle_with_opts(
            ledger,
            turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
    }

    /// Insert new data from Turtle format with options (direct flake path).
    ///
    /// Same as `insert_turtle` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).insert_turtle(ttl).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn insert_turtle_with_opts(
        &self,
        ledger: LedgerState,
        turtle: &str,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        let store_raw_txn = txn_opts.store_raw_txn.unwrap_or(false);

        // Spawn raw Turtle upload in parallel with staging when opted in.
        let commit_opts = if commit_opts.raw_txn.is_none()
            && commit_opts.raw_txn_upload.is_none()
            && store_raw_txn
        {
            let content_store = self.content_store(ledger.ledger_id());
            commit_opts.with_raw_txn_spawned(content_store, JsonValue::String(turtle.to_string()))
        } else {
            commit_opts
        };

        let stage_result = self
            .stage_turtle_insert(ledger, turtle, Some(index_config))
            .await?;

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = stage_result;

        // Add transaction metadata and graph delta (graph_delta typically empty for Turtle)
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        let (receipt, ledger) = self
            .commit_staged(view, ns_registry, index_config, commit_opts)
            .await?;

        // Compute indexing status (same logic as transact())
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        // Trigger indexing AFTER publish_commit succeeds
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok(TransactResult {
            receipt,
            ledger,
            indexing: indexing_status,
        })
    }

    /// Stage a Turtle INSERT by parsing directly to flakes (bypass JSON-LD / IR).
    ///
    /// This is the fast path for Turtle ingestion. The Turtle is parsed using
    /// `FlakeSink` which converts parser events directly to flakes.
    pub async fn stage_turtle_insert(
        &self,
        ledger: LedgerState,
        turtle: &str,
        index_config: Option<&IndexConfig>,
    ) -> Result<StageResult> {
        use fluree_db_transact::{generate_txn_id, stage_flakes, FlakeSink};

        let span = tracing::debug_span!(
            "stage_turtle_insert",
            ledger_t = ledger.t(),
            new_t = ledger.t() + 1,
            turtle_bytes = turtle.len()
        );
        let _guard = span.enter();

        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let new_t = ledger.t() + 1;
        let txn_id = generate_txn_id();

        // Parse Turtle directly to flakes
        let parse_span =
            tracing::debug_span!("turtle_parse_to_flakes", turtle_bytes = turtle.len());
        let flakes = {
            let _g = parse_span.enter();
            let mut sink = FlakeSink::new(&mut ns_registry, new_t, txn_id);
            fluree_graph_turtle::parse(turtle, &mut sink)?;
            sink.finish().map_err(ApiError::from)?
        };
        tracing::info!(flake_count = flakes.len(), "turtle parsed to flakes");

        // Stage the flakes (backpressure + optional policy)
        let options = match index_config {
            Some(cfg) => StageOptions::new().with_index_config(cfg),
            None => StageOptions::default(),
        };
        let view = stage_flakes(ledger, flakes, options).await?;

        // Apply SHACL policy to the staged view. Plain Turtle has no named-graph
        // metadata (that's TriG), so we pass `None` for graph_delta/graph_sids —
        // validation falls back to default-graph (g_id=0), matching how flakes
        // are produced by `FlakeSink`.
        #[cfg(feature = "shacl")]
        apply_shacl_policy_to_staged_view(
            &view,
            StagedShaclContext {
                graph_delta: None,
                graph_sids: None,
                tracker: None,
                // Turtle insert doesn't go through the
                // cross-ledger dispatch path today; cross-ledger
                // SHACL on Turtle inserts can be added by calling
                // resolve_cross_ledger_shapes_for_tx here when
                // the use case lands.
                cross_ledger_shapes: None,
                staged_ns: None,
                // Turtle insert API has no `opts.shapes` surface
                // today — inline SHACL flows in over the JSON
                // transaction path. Wireable later if needed.
                inline_shape_bundle: None,
            },
        )
        .await
        .map_err(ApiError::from)?;

        // Plain Turtle doesn't support named graphs or txn-meta extraction (TriG support handles these)
        Ok(StageResult {
            view,
            ns_registry,
            txn_meta: Vec::new(),
            graph_delta: rustc_hash::FxHashMap::default(),
        })
    }

    /// Insert new data with options
    ///
    /// Same as `insert` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).insert(data).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn insert_with_opts(
        &self,
        ledger: LedgerState,
        data: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        self.transact(
            ledger,
            TxnType::Insert,
            data,
            txn_opts,
            commit_opts,
            index_config,
        )
        .await
    }

    /// Upsert data into the ledger
    ///
    /// For each (subject, predicate) pair in the data, any existing values are
    /// retracted before the new values are asserted. This implements "replace mode"
    /// semantics.
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `data` - JSON-LD data to upsert
    ///
    /// # Example
    ///
    /// ```ignore
    /// // If ex:alice already has an age, it will be replaced
    /// let result = fluree.upsert(ledger, json!({
    ///     "@context": {"ex": "http://example.org/"},
    ///     "@id": "ex:alice",
    ///     "ex:age": 31
    /// })).await?;
    /// ```
    pub async fn upsert(&self, ledger: LedgerState, data: &JsonValue) -> Result<TransactResult> {
        let index_config = self.default_index_config();
        self.transact(
            ledger,
            TxnType::Upsert,
            data,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
    }

    /// Upsert data from Turtle format
    ///
    /// Parses the Turtle input and upserts it into the ledger.
    /// For each (subject, predicate) pair, existing values are retracted
    /// before new values are asserted.
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `turtle` - Turtle (TTL) format data
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.upsert_turtle(ledger, r#"
    ///     @prefix ex: <http://example.org/> .
    ///     ex:alice ex:age 31 .
    /// "#).await?;
    /// ```
    pub async fn upsert_turtle(&self, ledger: LedgerState, turtle: &str) -> Result<TransactResult> {
        let data = fluree_graph_turtle::parse_to_json(turtle)?;
        self.upsert(ledger, &data).await
    }

    /// Upsert data from Turtle format with options
    ///
    /// Same as `upsert_turtle` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).upsert_turtle(ttl).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn upsert_turtle_with_opts(
        &self,
        ledger: LedgerState,
        turtle: &str,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        let data = fluree_graph_turtle::parse_to_json(turtle)?;
        self.upsert_with_opts(ledger, &data, txn_opts, commit_opts, index_config)
            .await
    }

    /// Upsert data with options
    ///
    /// Same as `upsert` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).upsert(data).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn upsert_with_opts(
        &self,
        ledger: LedgerState,
        data: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        self.transact(
            ledger,
            TxnType::Upsert,
            data,
            txn_opts,
            commit_opts,
            index_config,
        )
        .await
    }

    /// Update data with WHERE/DELETE/INSERT semantics
    ///
    /// Provides SPARQL UPDATE-style modifications where DELETE and INSERT
    /// templates reference variables bound by WHERE patterns.
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `update_json` - Transaction with `where`, `delete`, and `insert` clauses
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Increment everyone's age by 1
    /// let result = fluree.update(ledger, json!({
    ///     "@context": {"ex": "http://example.org/"},
    ///     "where": { "@id": "?s", "ex:age": "?age" },
    ///     "delete": { "@id": "?s", "ex:age": "?age" },
    ///     "insert": { "@id": "?s", "ex:age": { "@value": "?newAge", "@type": "xsd:integer" } }
    /// })).await?;
    /// ```
    pub async fn update(
        &self,
        ledger: LedgerState,
        update_json: &JsonValue,
    ) -> Result<TransactResult> {
        let index_config = self.default_index_config();
        self.transact(
            ledger,
            TxnType::Update,
            update_json,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
    }

    /// Update data with options
    ///
    /// Same as `update` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).update(data).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn update_with_opts(
        &self,
        ledger: LedgerState,
        update_json: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        self.transact(
            ledger,
            TxnType::Update,
            update_json,
            txn_opts,
            commit_opts,
            index_config,
        )
        .await
    }

    // ========================================================================
    // CREDENTIALED TRANSACTION METHODS
    // ========================================================================

    /// Execute a credentialed transaction
    ///
    /// Verifies the signed credential, extracts the identity (DID), and executes
    /// the transaction with policy enforcement based on the verified identity.
    ///
    /// The original signed envelope is stored as `raw_txn` for provenance.
    ///
    /// # Arguments
    /// * `ledger` - The ledger state (consumed)
    /// * `credential` - JWS string or JSON object containing the signed transaction
    ///
    /// # Returns
    /// Transaction result with policy enforcement applied
    ///
    /// # Errors
    /// - Credential verification errors (400/401)
    /// - Transaction execution errors
    #[cfg(feature = "credential")]
    pub async fn credential_transact(
        &self,
        ledger: LedgerState,
        credential: crate::credential::Input<'_>,
    ) -> Result<TransactResult> {
        use fluree_db_credential::CredentialInput;

        // Convert credential to JsonValue for raw_txn storage
        // - JWS string -> JsonValue::String
        // - VC object -> JsonValue object
        let raw_credential: JsonValue = match &credential {
            CredentialInput::Jws(jws) => JsonValue::String(jws.to_string()),
            CredentialInput::Json(json) => (*json).clone(),
        };

        let verified = crate::credential::verify_credential(credential)?;

        // Build policy context with verified identity
        let opts = crate::QueryConnectionOptions {
            identity: Some(verified.did.clone()),
            ..Default::default()
        };
        let policy_ctx = crate::policy_builder::build_policy_context_from_opts(
            &ledger.snapshot,
            ledger.novelty.as_ref(),
            Some(ledger.novelty.as_ref()),
            ledger.t(),
            &opts,
            &[0],
        )
        .await?;

        // Context propagation: inject parent context if subject doesn't have one
        let mut txn_json = verified.subject.clone();
        if let (Some(parent_ctx), Some(obj)) = (&verified.parent_context, txn_json.as_object_mut())
        {
            if !obj.contains_key("@context") {
                obj.insert("@context".to_string(), parent_ctx.clone());
            }
        }

        // TxnOpts: context for IRI expansion. The verified DID flows into the
        // commit as `f:identity` via CommitOpts.identity / txn_signature below.
        let txn_opts = TxnOpts {
            context: verified.parent_context,
            ..Default::default()
        };

        // Compute content-addressed txn_id from the raw credential bytes
        let txn_id = {
            use fluree_db_core::sha256_hex;
            let raw_bytes = serde_json::to_vec(&raw_credential).unwrap_or_default();
            let hash_hex = sha256_hex(&raw_bytes);
            format!("fluree:tx:sha256:{hash_hex}")
        };

        // CommitOpts: identity for provenance (emitted as f:identity), raw_txn
        // upload (spawned in parallel with staging), txn_signature for audit.
        // Spawn happens here — after credential verification succeeds — so a
        // failed verification never uploads.
        let content_store = self.content_store(ledger.ledger_id());
        let commit_opts = CommitOpts::default()
            .identity(verified.did.clone())
            .with_raw_txn_spawned(content_store, raw_credential)
            .with_txn_signature(fluree_db_novelty::TxnSignature {
                signer: verified.did.clone(),
                txn_id: Some(txn_id),
            });

        // Use transact_tracked_with_policy and extract result
        let index_config = self.default_index_config();
        let input = TrackedTransactionInput::new(
            TxnType::Update, // credential-transact! uses update! internally
            &txn_json,
            txn_opts,
            &policy_ctx,
        );
        let (result, _tally) = self
            .transact_tracked_with_policy(ledger, input, commit_opts, &index_config)
            .await
            .map_err(|e: TrackedErrorResponse| {
                // Map TrackedErrorResponse to ApiError, preserving HTTP status
                ApiError::http(e.status, e.error)
            })?;

        Ok(result)
    }
}

impl crate::Fluree {
    /// Update data using a transaction that specifies the ledger ID.
    ///
    /// Transaction update helper where the transaction payload includes
    /// a `ledger` field. The ledger is loaded by alias before executing the update.
    pub async fn update_with_ledger(&self, update_json: &JsonValue) -> Result<TransactResult> {
        let ledger_id = ledger_id_from_txn(update_json)?;
        let ledger = self.ledger(ledger_id).await?;
        self.update(ledger, update_json).await
    }

    /// Update data using a ledger-specified transaction with tracking enabled.
    ///
    /// Returns the transaction result plus tracking tally (if requested by opts).
    pub async fn update_with_ledger_tracked(
        &self,
        update_json: &JsonValue,
    ) -> Result<(TransactResult, Option<TrackingTally>)> {
        let ledger_id = ledger_id_from_txn(update_json)?;
        let ledger = self.ledger(ledger_id).await?;
        let policy_ctx = crate::PolicyContext::new(fluree_db_policy::PolicyWrapper::root(), None);
        let index_config = self.default_index_config();
        let input = TrackedTransactionInput::new(
            TxnType::Update,
            update_json,
            TxnOpts::default(),
            &policy_ctx,
        );
        let (result, tally) = self
            .transact_tracked_with_policy(ledger, input, CommitOpts::default(), &index_config)
            .await
            .map_err(|e| ApiError::http(e.status, e.error))?;
        Ok((result, tally))
    }
}

// Keep ApiError used (avoid unused import warnings if features change)
#[allow(dead_code)]
fn _ensure_error_used(e: ApiError) -> ApiError {
    e
}
