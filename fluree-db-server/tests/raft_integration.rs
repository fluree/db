//! End-to-end smoke tests for the Raft-enabled server construction
//! path. Exercises [`FlureeServer::new_with_raft`] all the way through
//! to a router build, without actually binding listeners.
//!
//! Multi-node cluster behavior (initialize → leader → forwarded
//! transaction) is covered separately in the multi-node integration
//! test that spins up real Raft instances over the HTTP network
//! layer.

#![cfg(feature = "raft")]

use std::net::SocketAddr;
use std::sync::Arc;

use fluree_db_server::raft::{RaftBootstrapConfig, RaftIntegration};
use fluree_db_server::FlureeServerBuilder;
use tempfile::TempDir;

#[tokio::test]
async fn builder_with_raft_attaches_integration_to_app_state() {
    let server_tmp = TempDir::new().expect("server tempdir");
    let raft_tmp = TempDir::new().expect("raft tempdir");

    let integration = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(
            1,
            raft_tmp.path().to_path_buf(),
        ))
        .await
        .expect("raft bootstrap"),
    );

    let raft_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = FlureeServerBuilder::file(server_tmp.path())
        .cors_enabled(false)
        .indexing_enabled(false)
        .with_raft(integration, raft_addr)
        .build()
        .await
        .expect("server construction with raft");

    // The integration is attached to AppState — the forward middleware
    // is therefore mounted over leader-only routes.
    assert!(
        server.state().raft.is_some(),
        "raft integration should be reachable from AppState"
    );

    // Router builds without panicking; concrete route behavior is
    // covered by the public-side integration tests.
    let _ = server.router();
}
