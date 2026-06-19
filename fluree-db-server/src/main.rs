//! Fluree DB Server CLI
//!
//! Run with: `cargo run -p fluree-db-server -- --help`

use clap::{CommandFactory, FromArgMatches};
use fluree_db_server::{
    config_file::{config_error_is_fatal, load_and_merge_config},
    telemetry::{init_logging, shutdown_tracer, TelemetryConfig},
    FlureeServerBuilder, ServerConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Parse CLI + env via clap (get both typed config and raw matches)
    let matches = ServerConfig::command().get_matches();
    let mut config = ServerConfig::from_arg_matches(&matches)?;

    // 2. Load config file and merge (file values apply only where CLI/env didn't set a value)
    //
    // Errors are fatal when the user explicitly specified --config or --profile,
    // since ignoring those silently would cause hard-to-diagnose production issues.
    // For auto-discovered configs, errors are downgraded to warnings.
    if let Err(e) = load_and_merge_config(&mut config, &matches) {
        if config_error_is_fatal(&config) {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        eprintln!("Warning: {e}");
    }

    // Initialize telemetry (logging + optional tracing)
    let telemetry_config = TelemetryConfig::with_server_config(&config);
    init_logging(&telemetry_config);

    // Log startup info
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        storage = config.storage_type_str(),
        addr = %config.listen_addr,
        log_level = %config.log_level,
        cors = config.cors_enabled,
        indexing = config.indexing_enabled,
        query_min_t_timeout_ms = config.query_min_t_timeout_ms,
        query_refresh_enabled = config.query_refresh_enabled,
        query_refresh_ttl_ms = config.query_refresh_ttl_ms,
        reindex_min_bytes = config.reindex_min_bytes,
        reindex_max_bytes = config.reindex_max_bytes,
        events_auth_mode = ?config.events_auth_mode,
        log_format = ?telemetry_config.log_format,
        sensitive_data = ?telemetry_config.sensitive_data,
        query_text_logging = ?telemetry_config.query_text_logging,
        otel_enabled = telemetry_config.is_otel_enabled(),
        "Starting Fluree server"
    );

    // Bootstrap the Raft integration when raft mode is enabled in
    // config. Validation guarantees node_id / storage_path /
    // listen_addr are all set if we get here with raft_enabled.
    #[allow(unused_mut)]
    let mut builder = FlureeServerBuilder::for_config(config.clone());
    #[cfg(feature = "raft")]
    if config.raft_enabled {
        use fluree_db_server::raft::{RaftBootstrapConfig, RaftIntegration};
        use std::sync::Arc;

        let bootstrap = RaftBootstrapConfig::new(
            config.raft_node_id.expect("validated"),
            config.raft_storage_path.clone().expect("validated"),
        );
        let raft_listen = config.raft_listen_addr.expect("validated");
        tracing::info!(
            node_id = config.raft_node_id.unwrap(),
            raft_listen = %raft_listen,
            storage = %bootstrap.storage_path.display(),
            "Bootstrapping Raft integration"
        );
        let integration = Arc::new(RaftIntegration::bootstrap(bootstrap).await?);
        builder = builder.with_raft(integration, raft_listen);
    }

    let server = builder.build().await?;
    let result = server.run().await;

    // Graceful shutdown of telemetry
    shutdown_tracer().await;

    result.map_err(Into::into)
}
