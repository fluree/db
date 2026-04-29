//! HTTP route handlers and router configuration

mod admin;
mod admin_auth;
mod commits;
mod context;
mod events;
#[cfg(feature = "iceberg")]
mod iceberg;
mod ledger;
mod nameservice_refs;
mod pack;
mod policy_auth;
mod push;
mod query;
mod show;
mod storage_proxy;
mod stubs;
mod transact;

use crate::state::AppState;
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

/// Build the main application router
pub fn build_router(state: Arc<AppState>) -> Router {
    // v1 API (versioned base path).
    //
    // Ledger names may contain `/`, so ledger-scoped routes use `*ledger`
    // (greedy tail) with the operation first (e.g. `/v1/fluree/query/<ledger...>`).

    // Admin-protected routes (create, drop) - require admin token when configured
    let v1_admin_protected_routes = Router::new()
        .route("/create", post(ledger::create))
        .route("/drop", post(ledger::drop))
        .route("/reindex", post(ledger::reindex))
        .route("/branch", post(ledger::create_branch))
        .route("/drop-branch", post(ledger::drop_branch))
        .route("/rebase", post(ledger::rebase))
        .route("/merge", post(ledger::merge));

    #[cfg(feature = "iceberg")]
    let v1_admin_protected_routes =
        v1_admin_protected_routes.route("/iceberg/map", post(iceberg::iceberg_map));

    let v1_admin_protected_routes = v1_admin_protected_routes
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admin_auth::require_admin_token,
        ))
        .with_state(state.clone());

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
        // Merge admin-protected routes
        .merge(v1_admin_protected_routes)
        // Query endpoints
        .route("/query", get(query::query).post(query::query))
        .route(
            "/query/*ledger",
            get(query::query_ledger_tail).post(query::query_ledger_tail),
        )
        .route("/explain", get(query::explain).post(query::explain))
        .route(
            "/explain/*ledger",
            get(query::explain_ledger_tail).post(query::explain_ledger_tail),
        )
        // Transaction endpoints
        .route("/update", post(transact::update))
        .route("/update/*ledger", post(transact::update_ledger_tail))
        .route("/insert", post(transact::insert))
        .route("/insert/*ledger", post(transact::insert_ledger_tail))
        .route("/upsert", post(transact::upsert))
        .route("/upsert/*ledger", post(transact::upsert_ledger_tail))
        // Default context management
        .route(
            "/context/*ledger",
            get(context::get_context).put(context::set_context),
        )
        // Commit-push endpoint (precomputed commits)
        .route("/push/*ledger", post(push::push_ledger_tail))
        // Commit show endpoint (decoded commit with resolved IRIs)
        .route("/show/*ledger", get(show::show_ledger_tail))
        // Commit export endpoint (paginated, replication-grade auth)
        .route("/commits/*ledger", get(commits::commits_ledger_tail))
        // Binary pack stream endpoint (efficient clone/pull)
        .route("/pack/*ledger", post(pack::pack_ledger_tail))
        // SSE event streaming
        .route("/events", get(events::events))
        // Storage proxy endpoints (for peer mode)
        .route("/storage/ns/:alias", get(storage_proxy::get_ns_record))
        .route("/storage/block", post(storage_proxy::get_block))
        .route(
            "/storage/objects/:cid",
            get(storage_proxy::get_object_by_cid),
        )
        // Nameservice ref endpoints (for remote sync)
        .route(
            "/nameservice/refs/:alias/commit",
            post(nameservice_refs::push_commit_ref),
        )
        .route(
            "/nameservice/refs/:alias/index",
            post(nameservice_refs::push_index_ref),
        )
        .route(
            "/nameservice/refs/:alias/init",
            post(nameservice_refs::init_ledger),
        )
        .route("/nameservice/snapshot", get(nameservice_refs::snapshot))
        // Stub endpoints (not yet implemented)
        .route("/subscribe", get(stubs::subscribe))
        .route("/remote/:path", get(stubs::remote).post(stubs::remote));

    let mut router = Router::new()
        // Health check
        .route("/health", get(admin::health))
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
