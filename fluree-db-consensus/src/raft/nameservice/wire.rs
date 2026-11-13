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
//! `wire::StagedCommit` form. To avoid `From<X> for X`
//! ambiguity inside this file the in-memory types are imported
//! under `InMemory*` aliases below.

use super::StagedCommit as InMemoryArgs;
use crate::raft::staged_receipt::{
    AppliedReceipt as InMemoryReceipt, MergeApplied, PushApplied, RebaseApplied, RevertApplied,
    TransactApplied as InMemoryTransactApplied,
};
use crate::raft::state_machine::{RecordedTally, RefKey};
use fluree_db_core::ContentId;
use serde::{Deserialize, Serialize};

/// Postcard wire shape for
/// [`StagedCommit`](super::StagedCommit).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct StagedCommit {
    pub ref_key: RefKey,
    pub queue_id: u64,
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub receipt: AppliedReceipt,
    /// The erasure fields come last, so a leader a release behind reads
    /// the rest and ignores them.
    pub erasure_generation: u64,
    pub erasure_source: Option<(RefKey, u64)>,
    pub seen: Option<u64>,
}

/// [`StagedCommit`] as a follower a release behind sends it, before
/// erasure generations.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct StagedCommitV1 {
    ref_key: RefKey,
    queue_id: u64,
    commit_id: ContentId,
    commit_t: i64,
    receipt: AppliedReceipt,
}

impl StagedCommit {
    /// Decode a ferried commit. A follower a release behind presents no
    /// erasure generation, as its writes did before them, and is fenced as
    /// having loaded no collection intent, since it checks none; its shape
    /// is accepted only when it is the whole body, so a damaged current one
    /// is not read as it.
    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes).or_else(|e| match postcard::take_from_bytes(bytes) {
            Ok((v1, [])) => {
                let StagedCommitV1 {
                    ref_key,
                    queue_id,
                    commit_id,
                    commit_t,
                    receipt,
                } = v1;
                Ok(Self {
                    ref_key,
                    queue_id,
                    commit_id,
                    commit_t,
                    receipt,
                    erasure_generation: 0,
                    erasure_source: None,
                    seen: Some(0),
                })
            }
            _ => Err(e),
        })
    }
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

impl From<InMemoryArgs> for StagedCommit {
    fn from(args: InMemoryArgs) -> Self {
        Self {
            ref_key: args.ref_key,
            queue_id: args.queue_id,
            commit_id: args.commit_id,
            commit_t: args.commit_t,
            receipt: args.receipt.into(),
            erasure_generation: args.erasure_generation,
            erasure_source: args.erasure_source,
            seen: args.seen,
        }
    }
}

impl From<StagedCommit> for InMemoryArgs {
    fn from(args: StagedCommit) -> Self {
        Self {
            ref_key: args.ref_key,
            queue_id: args.queue_id,
            commit_id: args.commit_id,
            commit_t: args.commit_t,
            receipt: args.receipt.into(),
            erasure_generation: args.erasure_generation,
            erasure_source: args.erasure_source,
            seen: args.seen,
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

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    /// A follower a release behind ferries its commit without erasure
    /// generations or the intents it saw, and a damaged current body is not
    /// read as one.
    #[test]
    fn a_ferried_commit_decodes_from_a_follower_a_release_behind() {
        let v1 = StagedCommitV1 {
            ref_key: RefKey::new("test/db", "main"),
            queue_id: 9,
            commit_id: cid(1),
            commit_t: 17,
            receipt: AppliedReceipt::Minimal {
                commit_id: cid(1),
                commit_t: 17,
            },
        };
        let decoded = StagedCommit::decode(&postcard::to_allocvec(&v1).unwrap()).unwrap();
        assert_eq!(decoded.queue_id, 9);
        assert_eq!(decoded.erasure_generation, 0);
        assert_eq!(decoded.erasure_source, None);
        assert_eq!(decoded.seen, Some(0), "fenced: it checks no intent");

        let current = StagedCommit {
            ref_key: v1.ref_key,
            queue_id: 9,
            commit_id: cid(1),
            commit_t: 17,
            receipt: v1.receipt,
            erasure_generation: 2,
            erasure_source: Some((RefKey::new("test/db", "dev"), 1)),
            seen: Some(4),
        };
        let bytes = postcard::to_allocvec(&current).unwrap();
        let decoded = StagedCommit::decode(&bytes).unwrap();
        assert_eq!(decoded.erasure_generation, 2);
        assert_eq!(decoded.erasure_source, current.erasure_source);
        assert_eq!(decoded.seen, Some(4));
        assert!(StagedCommit::decode(&bytes[..bytes.len() - 1]).is_err());
    }
}
