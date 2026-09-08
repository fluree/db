//! Offline benchmark setup: seed a NEW single-voter Raft root from a file registry.
//! Stop all servers first. Other nodes join this seed using the normal admin API.
//! This is fixture preparation, never part of a timed transaction or server startup.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use fluree_db_consensus::raft::integration::{RaftBootstrapConfig, RaftIntegration};
use fluree_db_consensus::raft::ClusterNode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        return Err(
            "usage: raft_fixture_adopt NEW_RAFT_ROOT FILE_STORE_ROOT RAFT_URL CLIENT_URL".into(),
        );
    }
    let raft_root = PathBuf::from(&args[0]);
    let store_root = PathBuf::from(&args[1]);
    if raft_root.exists() || !store_root.join("ns@v2").is_dir() {
        return Err("requires a nonexistent Raft root and an existing file registry".into());
    }
    let integration = RaftIntegration::bootstrap(
        RaftBootstrapConfig::new(1, &raft_root).with_file_registry_adoption(&store_root),
    )
    .await?;
    integration
        .raft
        .initialize(BTreeMap::from([(
            1,
            ClusterNode::new(args[2].clone(), args[3].clone()),
        )]))
        .await?;
    tokio::time::timeout(Duration::from_secs(30), async {
        while integration.raft.current_leader().await != Some(1) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let result = integration.adopt_file_registry().await;
    integration.raft.shutdown().await?;
    println!("adopted {} ledger records into node 1", result?);
    Ok(())
}
