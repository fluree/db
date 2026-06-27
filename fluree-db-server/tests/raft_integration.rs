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
use fluree_db_server::{FlureeServerBuilder, ServerConfig};
use tempfile::TempDir;

#[tokio::test]
async fn builder_with_raft_attaches_integration_to_app_state() {
    let server_tmp = TempDir::new().expect("server tempdir");
    let raft_tmp = TempDir::new().expect("raft tempdir");

    let integration = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(1, raft_tmp.path().to_path_buf()))
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

#[test]
fn raft_enabled_requires_node_id() {
    let config = ServerConfig {
        raft_enabled: true,
        raft_node_id: None,
        raft_storage_path: Some(std::path::PathBuf::from("/tmp/raft")),
        raft_listen_addr: Some("127.0.0.1:9090".parse().unwrap()),
        ..Default::default()
    };
    let err = config.validate().expect_err("missing node_id should error");
    assert!(err.contains("raft-node-id"), "got: {err}");
}

#[test]
fn raft_enabled_requires_storage_path() {
    let config = ServerConfig {
        raft_enabled: true,
        raft_node_id: Some(1),
        raft_storage_path: None,
        raft_listen_addr: Some("127.0.0.1:9090".parse().unwrap()),
        ..Default::default()
    };
    let err = config
        .validate()
        .expect_err("missing storage_path should error");
    assert!(err.contains("raft-storage-path"), "got: {err}");
}

#[test]
fn raft_enabled_requires_listen_addr() {
    let config = ServerConfig {
        raft_enabled: true,
        raft_node_id: Some(1),
        raft_storage_path: Some(std::path::PathBuf::from("/tmp/raft")),
        raft_listen_addr: None,
        ..Default::default()
    };
    let err = config
        .validate()
        .expect_err("missing listen_addr should error");
    assert!(err.contains("raft-listen-addr"), "got: {err}");
}

#[test]
fn raft_enabled_rejects_proxy_storage() {
    use fluree_db_server::config::{ServerRole, StorageAccessMode};
    let config = ServerConfig {
        raft_enabled: true,
        raft_node_id: Some(1),
        raft_storage_path: Some(std::path::PathBuf::from("/tmp/raft")),
        raft_listen_addr: Some("127.0.0.1:9090".parse().unwrap()),
        server_role: ServerRole::Peer,
        storage_access_mode: StorageAccessMode::Proxy,
        tx_server_url: Some("http://tx.example".into()),
        storage_proxy_token: Some("dummy".into()),
        ..Default::default()
    };
    let err = config
        .validate()
        .expect_err("raft + proxy storage should error");
    assert!(err.contains("proxy"), "got: {err}");
}
