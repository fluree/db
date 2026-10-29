//! What this node has installed of the cluster's erasure activations.
//!
//! Applying an activation sets what every node must install; installing it
//! (the erasure map reading it, the holders' cached state reloaded) runs
//! here, in the background, in activation order. Until a node has installed
//! an activation, its records report each holder at the generation it has
//! installed, so a write made from one of its views is refused, and its
//! history reads of a holder wait. Once installed, the node reports how far
//! it has installed through the leader, under the run it reports from: a
//! restarted node installs again and reports again.

use crate::raft::nameservice::RaftNameService;
use crate::raft::state_machine::{generation, view_generation, Activation, Incarnation, RefKey};
use crate::raft::state_machine_adapter::SharedState;
use crate::raft::NodeId;
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// How often the installer looks for activations applied since its last
/// pass, and retries one that failed.
const POLL: Duration = Duration::from_millis(250);

/// Where a node's storage incarnation is kept, in its Raft storage root.
const STORAGE_ID: &str = "erasure-storage-id";

/// A random `u64`, from the OS-seeded keys std hashing uses.
fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

/// Install one activation on this node. The engine provides it.
#[async_trait]
pub trait ErasureInstall: Send + Sync {
    /// Make this node read with `activation`: its erasure map installed and
    /// what the holders' cached state was loaded with replaced. An error is
    /// retried.
    async fn install(&self, number: u64, activation: &Activation) -> Result<(), String>;
}

/// This node's erasure installation state.
pub struct ErasureNode {
    id: NodeId,
    incarnation: Incarnation,
    /// Every activation numbered below this is installed.
    installed: watch::Sender<u64>,
}

impl ErasureNode {
    /// The node's state for this run. The storage incarnation lives in
    /// `storage_root`, created there on first use, so a node brought back
    /// on other storage reports as another.
    pub fn open(id: NodeId, storage_root: &Path) -> std::io::Result<Self> {
        let path = storage_root.join(STORAGE_ID);
        let storage = match std::fs::read_to_string(&path) {
            Ok(text) => u64::from_str_radix(text.trim(), 16).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{}: {e}", path.display()),
                )
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let id = random_u64();
                let tmp = path.with_extension("tmp");
                std::fs::write(&tmp, format!("{id:016x}"))?;
                std::fs::rename(&tmp, &path)?;
                id
            }
            Err(e) => return Err(e),
        };
        Ok(Self::new(id, storage))
    }

    pub fn new(id: NodeId, storage: u64) -> Self {
        Self {
            id,
            incarnation: Incarnation {
                process: random_u64(),
                storage,
            },
            installed: watch::Sender::new(0),
        }
    }

    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Every activation numbered below this is installed here.
    pub fn installed_through(&self) -> u64 {
        *self.installed.borrow()
    }

    pub(crate) fn mark_installed(&self, through: u64) {
        self.installed.send_if_modified(|current| {
            let advanced = through > *current;
            if advanced {
                *current = through;
            }
            advanced
        });
    }

    /// Wait up to `timeout` until this node has installed every activation
    /// applied here that reaches `key`.
    pub async fn wait_installed(
        &self,
        state: &SharedState,
        key: &RefKey,
        timeout: Duration,
    ) -> bool {
        let mut rx = self.installed.subscribe();
        let wait = async {
            loop {
                let through = *rx.borrow_and_update();
                let pending = {
                    let state = state.read().await;
                    view_generation(&state, key, through) < generation(&state, key)
                };
                if !pending {
                    return true;
                }
                if rx.changed().await.is_err() {
                    return false;
                }
            }
        };
        tokio::time::timeout(timeout, wait).await.unwrap_or(false)
    }
}

/// Install every applied activation in order, then report how far this
/// node has installed. Runs until `cancel`.
pub async fn run_installer(
    node: Arc<ErasureNode>,
    state: SharedState,
    install: Arc<dyn ErasureInstall>,
    nameservice: Arc<RaftNameService>,
    cancel: CancellationToken,
) {
    loop {
        let pending: Vec<(u64, Activation)> = {
            let state = state.read().await;
            state
                .erasure
                .activations
                .range(node.installed_through()..)
                .map(|(n, a)| (*n, a.clone()))
                .collect()
        };
        for (number, activation) in pending {
            match install.install(number, &activation).await {
                Ok(()) => node.mark_installed(number + 1),
                Err(error) => {
                    tracing::warn!(
                        activation = number,
                        %error,
                        "installing an erasure activation failed; retrying"
                    );
                    break;
                }
            }
        }
        report(&node, &state, &nameservice).await;
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(POLL) => {}
        }
    }
}

/// Report how far this node has installed, unless the replicated state
/// already holds that report from this run. A cluster no erasure has
/// reached reports nothing.
async fn report(node: &ErasureNode, state: &SharedState, nameservice: &RaftNameService) {
    let through = node.installed_through();
    let reported = {
        let state = state.read().await;
        state.erasure.next_activation == 0
            || state
                .erasure
                .installed
                .get(&node.id)
                .is_some_and(|r| r.through == through && r.incarnation == node.incarnation)
    };
    if reported {
        return;
    }
    if let Err(error) = nameservice
        .report_erasure_ready(through, node.id, node.incarnation)
        .await
    {
        tracing::debug!(%error, through, "erasure readiness report deferred");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node keeps its storage incarnation across runs on the same
    /// storage, and reports each run as its own.
    #[test]
    fn a_node_keeps_its_storage_incarnation_across_runs() {
        let dir = tempfile::tempdir().unwrap();
        let first = ErasureNode::open(1, dir.path()).unwrap().incarnation();
        let second = ErasureNode::open(1, dir.path()).unwrap().incarnation();
        assert_eq!(first.storage, second.storage);
        assert_ne!(first.process, second.process);
        let other = tempfile::tempdir().unwrap();
        assert_ne!(
            ErasureNode::open(1, other.path())
                .unwrap()
                .incarnation()
                .storage,
            first.storage
        );
    }
}
