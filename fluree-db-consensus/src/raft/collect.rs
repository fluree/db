//! What this node loads of the cluster's collection: each intent the leader
//! condemned, in order, into the set its writers check
//! ([`fluree_db_api::collect::Condemned`]), and each intent's close.

use crate::raft::state_machine_adapter::SharedState;
use fluree_db_api::collect::Condemned;
use fluree_db_api::Fluree;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// How often the loader looks for intents condemned or closed since its
/// last pass, and retries one it could not read.
const POLL: Duration = Duration::from_millis(250);

/// Load and close intents as the replicated state condemns and completes
/// them. Runs until `cancel`.
pub async fn run_loader(
    fluree: Arc<Fluree>,
    condemned: Arc<Condemned>,
    state: SharedState,
    cancel: CancellationToken,
) {
    loop {
        load_pass(&fluree, &condemned, &state).await;
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(POLL) => {}
        }
    }
}

/// Load whatever the replicated state condemned that this member has not
/// loaded yet, before a write begins: one that began before an intent's
/// load is refused once the intent deletes.
pub async fn catch_up(fluree: &Fluree, condemned: &Condemned, state: &SharedState) {
    if condemned.loaded() < state.read().await.collect.next_intent {
        load_pass(fluree, condemned, state).await;
    }
}

/// Load what was condemned since the last pass, in order (an intent closed
/// before it was loaded is skipped), then close what completed.
pub async fn load_pass(fluree: &Fluree, condemned: &Condemned, state: &SharedState) {
    let _loading = condemned.loading().await;
    let (pending, next) = {
        let state = state.read().await;
        let pending: Vec<(u64, String)> = state
            .collect
            .intents
            .range(condemned.loaded()..)
            .map(|(number, intent)| (*number, intent.intent.clone()))
            .collect();
        (pending, state.collect.next_intent)
    };
    for number in condemned.loaded()..next {
        let Some((_, hash)) = pending.iter().find(|(n, _)| *n == number) else {
            condemned.skip(number);
            continue;
        };
        let loaded = match fluree.intent(hash).await {
            Ok(intent) => {
                let addresses = intent.entries.into_iter().map(|e| e.address).collect();
                condemned.load(number, addresses).await
            }
            Err(e) => Err(e),
        };
        if let Err(error) = loaded {
            tracing::warn!(intent = number, %error, "loading a collection intent failed; retrying");
            break;
        }
    }
    let open: BTreeSet<u64> = state.read().await.collect.intents.keys().copied().collect();
    for number in condemned.open() {
        if !open.contains(&number) {
            condemned.close(number);
        }
    }
}
