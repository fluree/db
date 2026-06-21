//! HTTP route handlers and router configuration

mod admin;
pub(crate) mod admin_auth;
mod commits;
mod context;
mod events;
mod export;
#[cfg(feature = "iceberg")]
mod iceberg;
mod import;
mod ledger;
mod log;
mod nameservice_refs;
mod pack;
mod policy_auth;
mod push;
pub(crate) mod query;
mod show;
mod storage_proxy;
mod stubs;
mod submissions;
mod transact;

use crate::state::AppState;
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

/// Apply the Raft follower-forward middleware to a leader-only
/// subrouter when the server was constructed with a Raft handle.
/// Single-node and peer-mode deployments pass through unchanged.
///
/// Generic over the router's state type so it composes both before
/// and after a downstream `.with_state(...)` call.
#[cfg(feature = "raft")]
fn apply_leader_forward<S>(router: Router<S>, state: &Arc<AppState>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    if let Some(integration) = &state.raft {
        router.layer(middleware::from_fn_with_state(
            Arc::clone(&integration.forwarder),
            fluree_db_consensus::raft::forward::forward_to_leader,
        ))
    } else {
        router
    }
}

#[cfg(not(feature = "raft"))]
fn apply_leader_forward<S>(router: Router<S>, _state: &Arc<AppState>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
}

/// Build the main application router
pub fn build_router(state: Arc<AppState>) -> Router {
    // v1 API (versioned base path).
    //
    // Ledger names may contain `/`, so ledger-scoped routes use `*ledger`
    // (greedy tail) with the operation first (e.g. `/v1/fluree/query/<ledger...>`).

    // Admin-protected leader-coordinated writes — need to run on the
    // current Raft leader so the state-machine proposal originates
    // there. Followers transparently HTTP-forward via the layer
    // applied below.
    let v1_admin_protected_writes = Router::new()
        .route("/create", post(ledger::create))
        .route("/drop", post(ledger::drop))
        .route("/reindex", post(ledger::reindex))
        .route("/branch", post(ledger::create_branch))
        .route("/drop-branch", post(ledger::drop_branch))
        .route("/drop-graph", post(ledger::drop_named_graph))
        .route("/rebase", post(ledger::rebase))
        .route("/merge", post(ledger::merge))
        .route("/revert", post(ledger::revert))
        // Wholesale .flpack restore: creates a new ledger from a trusted
        // archive. Writes prebuilt index artifacts, so admin-gated.
        .route("/import/*ledger", post(import::import_ledger_tail))
        // Negotiated upload flow for size-capped clients (mint/complete
        // are admin-gated; the blob PUT below is token-authorized;
        // status is read-only and sits in the reads block).
        .route("/import-upload", post(import::mint_upload))
        .route(
            "/import-upload/:import_id/complete",
            post(import::complete_upload),
        );

    #[cfg(feature = "iceberg")]
    let v1_admin_protected_writes =
        v1_admin_protected_writes.route("/iceberg/map", post(iceberg::iceberg_map));

    // Forward to the Raft leader (when running in Raft mode) before
    // admin-token auth: an out-of-date follower with stale credentials
    // shouldn't reject a request the leader would accept.
    let v1_admin_protected_writes = apply_leader_forward(v1_admin_protected_writes, &state);

    let v1_admin_protected_writes = v1_admin_protected_writes
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admin_auth::require_admin_token,
        ))
        .with_state(state.clone());

    // Admin-protected reads — same admin-token bracket but no
    // leader-forward. These either read ledger data (any node has it
    // via replicated commit log + storage) or per-node ephemeral
    // state. Leader-forwarding them would either burn an extra hop
    // for no functional gain (export) or actively misroute the
    // request away from the node that owns the state (import status,
    // which lives in this node's `import_jobs` map).
    let v1_admin_protected_reads = Router::new()
        // RDF export — pure read; bypasses per-flake policy filtering
        // today, hence admin-gated.
        .route("/export/*ledger", post(export::export_ledger_tail))
        // Status of a negotiated upload — reads this node's
        // `state.import_jobs` map (each node owns the jobs it minted).
        .route("/import-upload/:import_id", get(import::import_status));

    let v1_admin_protected_reads = v1_admin_protected_reads
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admin_auth::require_admin_token,
        ))
        .with_state(state.clone());

    // Leader-only writes outside the admin-token bracket: transactions,
    // precomputed-commit push, and nameservice ref advances. Under
    // Raft these all propose state-machine commands and must run on
    // the current leader; followers transparently HTTP-forward via
    // the layer below.
    let v1_leader_only_routes = Router::new()
        // Transaction endpoints
        .route("/update", post(transact::update))
        .route("/update/*ledger", post(transact::update_ledger_tail))
        .route("/insert", post(transact::insert))
        .route("/insert/*ledger", post(transact::insert_ledger_tail))
        .route("/upsert", post(transact::upsert))
        .route("/upsert/*ledger", post(transact::upsert_ledger_tail))
        // Commit-push endpoint (precomputed commits)
        .route("/push/*ledger", post(push::push_ledger_tail))
        // Nameservice ref endpoints (for remote sync)
        .route(
            "/nameservice/refs/:alias/commit",
            post(nameservice_refs::push_commit_head),
        )
        .route(
            "/nameservice/refs/:alias/index",
            post(nameservice_refs::push_index_head),
        )
        .route(
            "/nameservice/refs/:alias/init",
            post(nameservice_refs::init_ledger),
        );
    let v1_leader_only_routes = apply_leader_forward(v1_leader_only_routes, &state);

    // Read-only routes that nonetheless need leader-forward because
    // their backing state lives in the leader's per-process caches.
    // Today: submission status, which reads from the
    // [`CachingCommitter`]'s in-process idempotency cache. A follower
    // serving this locally returns `Unknown` for a key the leader
    // accepted (the cache is per-process) — leader-forward closes
    // that hole. Note that this still has a known gap across leader
    // transitions: the new leader's cache won't carry entries the
    // old leader served; clients hitting that gap need to re-issue
    // (idempotency dedups the actual write) or verify via the commit
    // log endpoint. Serving from replicated idempotency state would
    // close the gap entirely but requires adding op-type metadata to
    // the persisted `ApplyRecord` — tracked as a follow-up.
    let v1_leader_forwarded_reads = Router::new().route(
        "/submissions/:key/*ledger",
        get(submissions::submission_status),
    );
    let v1_leader_forwarded_reads = apply_leader_forward(v1_leader_forwarded_reads, &state);

    let v1 = Router::new()
        // Admin endpoints (stats and whoami are read-only, no auth required)
        .route("/stats", get(admin::stats))
        .route("/whoami", get(admin::whoami))
        // Ledger management (read-only)
        .route("/ledgers", get(ledger::list_ledgers))
        .route("/info/*ledger", get(ledger::info_ledger_tail))
        .route("/exists/*ledger", get(ledger::exists_ledger_tail))
        .route("/branch/*ledger", get(ledger::list_branches))
        .route("/merge-preview/*ledger", get(ledger::merge_preview))
        .route("/revert-preview/*ledger", get(ledger::revert_preview))
        // Merge admin-protected leader-coordinated writes
        .merge(v1_admin_protected_writes)
        // Merge admin-protected local reads
        .merge(v1_admin_protected_reads)
        // Merge leader-only routes (Raft-forwarded when applicable)
        .merge(v1_leader_only_routes)
        // Query endpoints
        .route("/query", get(query::query).post(query::query))
        .route(
            "/query/*ledger",
            get(query::query_ledger_tail).post(query::query_ledger_tail),
        )
        .route("/multi-query", post(query::multi_query))
        .route("/explain", get(query::explain).post(query::explain))
        .route(
            "/explain/*ledger",
            get(query::explain_ledger_tail).post(query::explain_ledger_tail),
        )
        // Submission status lookup (by idempotency key). Mounted as
        // a leader-forwarded sub-router above so a follower doesn't
        // answer from its empty per-process cache.
        .merge(v1_leader_forwarded_reads)
        // Default context management
        .route(
            "/context/*ledger",
            get(context::get_context).put(context::set_context),
        )
        // Commit show endpoint (decoded commit with resolved IRIs)
        .route("/show/*ledger", get(show::show_ledger_tail))
        // Commit log endpoint (lightweight per-commit summaries)
        .route("/log/*ledger", get(log::log_ledger_tail))
        // Commit export endpoint (paginated, replication-grade auth)
        .route("/commits/*ledger", get(commits::commits_ledger_tail))
        // Binary pack stream endpoint (efficient clone/pull)
        .route("/pack/*ledger", post(pack::pack_ledger_tail))
        // Negotiated-upload blob sink (token-authorized via the minted URL, so
        // it sits outside the admin bracket — the URL is the capability).
        .route(
            "/import-upload/:import_id/blob",
            axum::routing::put(import::put_blob),
        )
        // Negotiated-upload multipart part sink (also token-authorized via the
        // minted URL, so it sits outside the admin bracket).
        .route(
            "/import-upload/:import_id/part/:part_number",
            axum::routing::put(import::put_part),
        )
        // SSE event streaming
        .route("/events", get(events::events))
        // Storage proxy endpoints (for peer mode)
        .route("/storage/ns/:alias", get(storage_proxy::get_ns_record))
        .route("/storage/block", post(storage_proxy::get_block))
        .route(
            "/storage/objects/:cid",
            get(storage_proxy::get_object_by_cid),
        )
        .route("/nameservice/snapshot", get(nameservice_refs::snapshot))
        // Stub endpoints (not yet implemented)
        .route("/subscribe", get(stubs::subscribe))
        .route("/remote/:path", get(stubs::remote).post(stubs::remote));

    let mut router = Router::new()
        // Health check
        .route("/health", get(admin::health))
        // Diagnostic: binary-scan leaflet-loop counters (GET, ?reset=true to zero)
        // Auth discovery (CLI auto-configuration)
        .route("/.well-known/fluree.json", get(admin::discovery))
        // Versioned API
        .nest("/v1/fluree", v1)
        // OpenAPI spec
        .route("/swagger.json", get(admin::openapi_spec));

    // Add MCP router if enabled
    if state.config.mcp_enabled {
        let mcp_router = crate::mcp::build_mcp_router(state.clone());
        router = router.nest("/mcp", mcp_router);
    }

    // Add state
    let mut router = router.with_state(state.clone());

    // Add CORS if enabled
    if state.config.cors_enabled {
        router = router.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
    }

    router
}
