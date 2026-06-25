//! Postcard wire shapes for the cross-node `apply_staged_commit` RPC.
//!
//! Postcard is a positional binary codec — `#[serde(skip_serializing_if = ...)]`
//! on a field causes the serializer to emit zero bytes while the
//! deserializer still expects a value at that offset, silently
//! corrupting every subsequent field.
//! [`fluree_db_api::TrackingTally`] and its nested
//! [`fluree_db_core::tracking::ReasoningTally`] both use that
//! attribute (they're shaped for the public JSON response), so the
//! in-memory [`InMemoryArgs`] graph can't ride the RPC wire
//! directly. The types here mirror the same shape but with skip-free
//! option encoding throughout (using
//! [`RecordedTally`](super::super::state_machine::RecordedTally) in
//! place of `TrackingTally`); the apply_staged_commit handler and
//! the follower-side publisher convert to/from these shapes at the
//! postcard boundary.
//!
//! Non-`Transact` receipt variants don't embed any skip-fielded
//! types, so their wire form is byte-identical to the in-memory
//! form and they pass straight through.
//!
//! These types intentionally share the unqualified names of their
//! in-memory counterparts so external readers see the clean
//! `wire::ApplyStagedCommitArgs` form. To avoid `From<X> for X`
//! ambiguity inside this file the in-memory types are imported
//! under `InMemory*` aliases below.

use super::ApplyStagedCommitArgs as InMemoryArgs;
use crate::raft::staged_receipt::{
    AppliedReceipt as InMemoryReceipt, MergeApplied, PushApplied, RebaseApplied, RevertApplied,
    TransactApplied as InMemoryTransactApplied,
};
use crate::raft::state_machine::{RecordedTally, RefKey};
use fluree_db_core::ContentId;
use serde::{Deserialize, Serialize};

/// Postcard wire shape for
/// [`ApplyStagedCommitArgs`](super::ApplyStagedCommitArgs).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ApplyStagedCommitArgs {
    pub ref_key: RefKey,
    pub queue_id: u64,
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub receipt: AppliedReceipt,
}

/// Skip-free mirror of
/// [`AppliedReceipt`](crate::raft::staged_receipt::AppliedReceipt).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum AppliedReceipt {
    Transact(TransactApplied),
    Push(PushApplied),
    Revert(RevertApplied),
    Merge(MergeApplied),
    Rebase(RebaseApplied),
    Minimal { commit_id: ContentId, commit_t: i64 },
}

/// Skip-free mirror of
/// [`TransactApplied`](crate::raft::staged_receipt::TransactApplied):
/// holds the tally as [`RecordedTally`] (postcard-friendly) instead
/// of [`fluree_db_api::TrackingTally`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct TransactApplied {
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub flake_count: usize,
    pub tally: Option<RecordedTally>,
}

impl From<InMemoryArgs> for ApplyStagedCommitArgs {
    fn from(args: InMemoryArgs) -> Self {
        Self {
            ref_key: args.ref_key,
            queue_id: args.queue_id,
            commit_id: args.commit_id,
            commit_t: args.commit_t,
            receipt: args.receipt.into(),
        }
    }
}

impl From<ApplyStagedCommitArgs> for InMemoryArgs {
    fn from(args: ApplyStagedCommitArgs) -> Self {
        Self {
            ref_key: args.ref_key,
            queue_id: args.queue_id,
            commit_id: args.commit_id,
            commit_t: args.commit_t,
            receipt: args.receipt.into(),
        }
    }
}

impl From<InMemoryReceipt> for AppliedReceipt {
    fn from(r: InMemoryReceipt) -> Self {
        match r {
            InMemoryReceipt::Transact(t) => Self::Transact(TransactApplied {
                commit_id: t.commit_id,
                commit_t: t.commit_t,
                flake_count: t.flake_count,
                tally: t.tally.as_ref().map(RecordedTally::from),
            }),
            InMemoryReceipt::Push(p) => Self::Push(p),
            InMemoryReceipt::Revert(r) => Self::Revert(r),
            InMemoryReceipt::Merge(m) => Self::Merge(m),
            InMemoryReceipt::Rebase(r) => Self::Rebase(r),
            InMemoryReceipt::Minimal {
                commit_id,
                commit_t,
            } => Self::Minimal {
                commit_id,
                commit_t,
            },
        }
    }
}

impl From<AppliedReceipt> for InMemoryReceipt {
    fn from(r: AppliedReceipt) -> Self {
        match r {
            AppliedReceipt::Transact(t) => Self::Transact(InMemoryTransactApplied {
                commit_id: t.commit_id,
                commit_t: t.commit_t,
                flake_count: t.flake_count,
                tally: t.tally.map(Into::into),
            }),
            AppliedReceipt::Push(p) => Self::Push(p),
            AppliedReceipt::Revert(r) => Self::Revert(r),
            AppliedReceipt::Merge(m) => Self::Merge(m),
            AppliedReceipt::Rebase(r) => Self::Rebase(r),
            AppliedReceipt::Minimal {
                commit_id,
                commit_t,
            } => Self::Minimal {
                commit_id,
                commit_t,
            },
        }
    }
}
