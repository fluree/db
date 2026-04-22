//! Fluree DB Server CLI
//!
//! Run with: `cargo run -p fluree-db-server -- --help`

use clap::{CommandFactory, FromArgMatches};
use fluree_db_server::{
    config_file::{config_error_is_fatal, load_and_merge_config},
    telemetry::{init_logging, shutdown_tracer, TelemetryConfig},
    FlureeServer, ServerConfig,
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
        reindex_min_bytes = config.reindex_min_bytes,
        reindex_max_bytes = config.reindex_max_bytes,
        events_auth_mode = ?config.events_auth_mode,
        log_format = ?telemetry_config.log_format,
        sensitive_data = ?telemetry_config.sensitive_data,
        query_text_logging = ?telemetry_config.query_text_logging,
        otel_enabled = telemetry_config.is_otel_enabled(),
        "Starting Fluree server"
    );

    // Create and run server
    let server = FlureeServer::new(config).await?;
    let result = server.run().await;

    // Graceful shutdown of telemetry
    shutdown_tracer().await;

    result.map_err(Into::into)
}
