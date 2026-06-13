//! Raft-backed [`IndexPublisher`].
//!
//! Indexer-facing surface. When the leader's local indexer finishes
//! a build it calls [`RaftIndexPublisher::publish_index`], which
//! proposes a [`Command::AdvanceIndexHead`] through Raft and waits
//! for quorum. Every node's state machine then updates its
//! [`RefEntry::index`](crate::raft::state_machine::RefEntry::index)
//! under apply, so follower reads through
//! [`RaftNameService`](crate::raft::nameservice::RaftNameService)
//! observe the new index head as soon as the entry commits.
//!
//! # Scope
//!
//! Only the leader has a meaningful `RaftIndexPublisher`. Followers
//! shouldn't drive the indexer (v1's choice), but if a stepped-down
//! leader's in-flight build finishes a tick after the transition,
//! its `publish_index` call hits openraft's
//! [`ClientWriteError::ForwardToLeader`] — we map that to `Ok(())`
//! because the new leader will run its own build against its own
//! state, and rejecting the call would just surface noise in the
//! logs.
//!
//! Equivalent stale-builder handling for the apply outcome:
//! [`Response::IndexStale`] also maps to `Ok(())` — another indexer
//! got there first; not an error.

use crate::raft::state_machine::{AdvanceIndexHeadArgs, Command as SmCommand, Response as SmResponse};
use crate::raft::TypeConfig;
use async_trait::async_trait;
use fluree_db_core::ContentId;
use fluree_db_core::ledger_id::split_ledger_id;
use fluree_db_nameservice::{IndexPublisher, NameServiceError, Result};
use openraft::error::{ClientWriteError, RaftError};
use openraft::Raft;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

/// [`IndexPublisher`] that proposes `AdvanceIndexHead` through Raft.
///
/// Cheap to clone (`Arc`). Construct from the same
/// `Arc<Raft<TypeConfig>>` the `RaftCommitter` and `RaftAdmin` hold.
#[derive(Clone)]
pub struct RaftIndexPublisher {
    raft: Arc<Raft<TypeConfig>>,
}

impl RaftIndexPublisher {
    pub fn new(raft: Arc<Raft<TypeConfig>>) -> Self {
        Self { raft }
    }

    /// Borrow the underlying Raft handle. Exposed so callers that
    /// already hold a publisher don't need to thread the raw handle
    /// alongside.
    pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
        &self.raft
    }
}

impl fmt::Debug for RaftIndexPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftIndexPublisher").finish()
    }
}

/// Build the state-machine command an `IndexPublisher::publish_index`
/// call translates into. Extracted so the construction is testable
/// without spinning up a Raft instance.
fn build_command(
    ledger_id: &str,
    index_t: i64,
    index_id: &ContentId,
) -> std::result::Result<SmCommand, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    let applied_at_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(SmCommand::AdvanceIndexHead(AdvanceIndexHeadArgs {
        ledger_id: ledger_name,
        branch,
        new_index_head: index_id.clone(),
        t: index_t,
        applied_at_millis,
    }))
}

/// Translate the apply outcome into the `IndexPublisher::publish_index`
/// result.
///
/// - [`Response::IndexAdvanced`] / [`Response::IndexStale`] → `Ok(())`.
///   Stale is the racing-indexer case: another publisher landed at a
///   `t` ≥ ours. The cluster's view of the latest index is already
///   at-least-as-fresh as the one we tried to publish; nothing to
///   surface.
/// - [`Response::IndexAhead`] → `Err(Storage)`. The proposer's view of
///   `commit_t` was wrong (almost always: a leadership transition
///   where the new leader had reset to an older state). The caller's
///   indexer should re-stage against the current commit head.
/// - [`Response::LedgerNotFound`] → `Err(not_found)`. Ledger gone
///   mid-build (drop / membership change).
/// - Anything else → `Err(Storage)` with an "unexpected variant"
///   message. None of the other variants are reachable for this
///   command; if one appears it's a state-machine bug worth
///   surfacing rather than swallowing.
fn map_response(resp: SmResponse) -> Result<()> {
    match resp {
        SmResponse::IndexAdvanced { .. } => Ok(()),
        // Stale = concurrent indexer published a t >= ours. The
        // cluster's view of the latest index is already at-least-as
        // fresh; nothing to surface.
        SmResponse::IndexStale { .. } => Ok(()),
        SmResponse::IndexAhead {
            commit_t,
            proposed_t,
        } => Err(NameServiceError::storage(format!(
            "raft AdvanceIndexHead rejected: index_t={proposed_t} > commit_t={commit_t} \
             (proposer ran ahead of applied state; re-stage from current commit head)"
        ))),
        SmResponse::LedgerNotFound { ledger_id } => Err(NameServiceError::not_found(ledger_id)),
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for AdvanceIndexHead: {other:?}"
        ))),
    }
}

#[async_trait]
impl IndexPublisher for RaftIndexPublisher {
    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let cmd = build_command(ledger_id, index_t, index_id)?;

        match self.raft.client_write(cmd).await {
            Ok(resp) => map_response(resp.data),
            // A stepped-down leader's straggling publish call. The
            // new leader will run its own build; nothing for us to
            // do except not propagate the error.
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => Ok(()),
            // ChangeMembershipError can't surface here — this
            // command isn't a membership change. Treat as
            // unreachable but report rather than panic.
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(NameServiceError::storage(format!(
                    "unexpected ChangeMembershipError on AdvanceIndexHead: {e}"
                )))
            }
            Err(RaftError::Fatal(f)) => Err(NameServiceError::storage(format!(
                "raft fatal during AdvanceIndexHead: {f}"
            ))),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    //! Unit coverage for the two pure helpers
    //! ([`build_command`], [`map_response`]). End-to-end coverage
    //! through `raft.client_write` lives in the
    //! `tests/single_node_round_trip.rs` integration test alongside
    //! the existing `CreateLedger` / `AdvanceRef` roundtrips.

    use super::*;
    use fluree_db_api::{ContentId, ContentKind};

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    // -- build_command -------------------------------------------------------

    #[test]
    fn build_command_splits_ledger_id_into_name_and_branch() {
        let cmd = build_command("test/db:main", 7, &cid(42)).expect("build");
        let SmCommand::AdvanceIndexHead(args) = cmd else {
            panic!("expected AdvanceIndexHead");
        };
        assert_eq!(args.ledger_id, "test/db");
        assert_eq!(args.branch, "main");
        assert_eq!(args.new_index_head, cid(42));
        assert_eq!(args.t, 7);
        assert!(args.applied_at_millis > 0);
    }

    #[test]
    fn build_command_defaults_branch_when_omitted() {
        // `split_ledger_id` accepts a bare name and assigns the
        // default branch; the publisher should pick that up too.
        let cmd = build_command("test/db", 7, &cid(42)).expect("build");
        let SmCommand::AdvanceIndexHead(args) = cmd else {
            panic!("expected AdvanceIndexHead");
        };
        assert_eq!(args.ledger_id, "test/db");
        assert_eq!(args.branch, "main");
    }

    #[test]
    fn build_command_rejects_empty_ledger_id() {
        // Genuinely malformed — `split_ledger_id` surfaces the
        // error and we propagate it.
        assert!(build_command("", 7, &cid(42)).is_err());
    }

    // -- map_response --------------------------------------------------------

    #[test]
    fn map_response_advanced_is_ok() {
        let r = map_response(SmResponse::IndexAdvanced {
            index_t: 5,
            index_head: cid(1),
        });
        assert!(r.is_ok());
    }

    #[test]
    fn map_response_stale_is_ok() {
        // Stale is not an error — a concurrent indexer landed a
        // newer t and the cluster's view is already past ours.
        let r = map_response(SmResponse::IndexStale { current_t: 9 });
        assert!(r.is_ok());
    }

    #[test]
    fn map_response_ahead_is_err_with_commit_t_in_message() {
        let r = map_response(SmResponse::IndexAhead {
            commit_t: 3,
            proposed_t: 9,
        });
        let msg = r.expect_err("ahead is error").to_string();
        assert!(msg.contains("commit_t=3"), "got: {msg}");
        assert!(msg.contains("index_t=9"), "got: {msg}");
    }

    #[test]
    fn map_response_ledger_not_found_is_err_not_found() {
        let r = map_response(SmResponse::LedgerNotFound {
            ledger_id: "gone/db".into(),
        });
        let msg = r.expect_err("ledger not found is error").to_string();
        assert!(msg.contains("gone/db"), "got: {msg}");
    }

    #[test]
    fn map_response_unexpected_variant_is_err() {
        // NoOp can't be the state-machine reply to AdvanceIndexHead,
        // but if it ever were, the publisher should surface rather
        // than swallow.
        let r = map_response(SmResponse::NoOp);
        assert!(r.is_err());
    }
}
