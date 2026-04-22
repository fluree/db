//! Standalone Fluree Query Peer CLI (example)
//!
//! This example shows how to run the peer as a standalone process.
//! The main binary was removed when fluree-db-peer became a library-only crate,
//! but this example is preserved for reference and testing.
//!
//! Run with: `cargo run -p fluree-db-peer --example peer_cli -- --help`

use clap::Parser;
use fluree_db_peer::{LoggingCallbacks, PeerConfig, PeerRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let config = PeerConfig::parse();

    tracing::info!(
        events_url = %config.events_url,
        read_mode = ?config.read_mode,
        storage_path = ?config.storage_path,
        all = config.all,
        ledgers = ?config.ledgers,
        graph_sources = ?config.graph_sources,
        "Starting Fluree query peer"
    );

    let runtime = PeerRuntime::new(config, LoggingCallbacks);
    runtime.run().await?;

    Ok(())
}
