//! Deterministic per-branch worker assignment.
//!
//! Maps a [`RefKey`] to the [`NodeId`] that should run its worker via
//! rendezvous hashing. The hashing itself lives in
//! [`fluree_raft_core::ownership`]; this module only decides how a
//! `RefKey` becomes a digest.
//!
//! Total: any non-empty voter set yields exactly one owner per
//! [`RefKey`], so cluster-wide at-most-one ownership is structural.
//!
//! **The digest is wire format.** Nodes compute ownership locally and
//! independently, so two nodes that disagree about a key's digest can
//! both claim it. `digest_matches_the_pre_extraction_algorithm` pins
//! the exact value the pre-`fluree-raft-core` implementation produced.

use crate::raft::state_machine::RefKey;
use crate::raft::NodeId;
use fluree_raft_core::ownership::{digest_parts, owner_for_digest};

/// Resolve the owner of `ref_key` from a non-empty voter set.
///
/// Returns `None` when `voters` is empty; callers should treat that
/// as "cluster not yet bootstrapped, defer staging." All other inputs
/// yield exactly one owner; ties break by the higher `NodeId` so the
/// result is fully deterministic.
///
/// Accepts any borrow that yields `&NodeId` — `&[NodeId]`,
/// `&Vec<NodeId>`, `&BTreeSet<NodeId>`, etc. — so the per-supervisor
/// tick can iterate `state.worker_eligible_voters` directly without
/// allocating a `Vec` per call.
pub fn owner<'a, I>(ref_key: &RefKey, voters: I) -> Option<NodeId>
where
    I: IntoIterator<Item = &'a NodeId>,
{
    owner_for_digest(key_digest(ref_key), voters)
}

/// Digest of a branch key: the ledger name and branch folded with a
/// `:` separator between them, so `("ab", "c")` and `("a", "bc")`
/// cannot collide.
fn key_digest(ref_key: &RefKey) -> u64 {
    digest_parts([
        ref_key.ledger_name.as_bytes(),
        b":".as_slice(),
        ref_key.branch.as_bytes(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str, branch: &str) -> RefKey {
        RefKey::new(name, branch)
    }

    /// Golden value, carried over from the implementation that lived
    /// here before the `fluree-raft-core` extraction:
    ///
    /// ```text
    /// let hash = xxh64(ref_key.ledger_name.as_bytes(), RENDEZVOUS_SEED);
    /// let hash = xxh64(b":", hash);
    /// xxh64(ref_key.branch.as_bytes(), hash)
    /// ```
    ///
    /// A mixed-version cluster splits branch ownership if this ever
    /// changes, so the constant is the test, not an implementation
    /// detail of one.
    #[test]
    fn digest_matches_the_pre_extraction_algorithm() {
        assert_eq!(key_digest(&key("db", "main")), 0x446a_dd93_2b74_321e);
        assert_eq!(key_digest(&key("db", "feature")), 0xd9a0_ac6c_9b5e_6a4d);
        assert_eq!(key_digest(&key("other", "main")), 0x4329_aa64_1b2b_db53);
    }

    #[test]
    fn owner_matches_the_pre_extraction_assignment() {
        let voters: Vec<NodeId> = (1..=5).collect();
        assert_eq!(owner(&key("db", "main"), &voters), Some(3));
        assert_eq!(owner(&key("db", "feature"), &voters), Some(4));
        assert_eq!(owner(&key("other", "main"), &voters), Some(3));
    }

    #[test]
    fn empty_voter_set_yields_no_owner() {
        assert_eq!(owner(&key("db", "main"), &[]), None);
    }

    #[test]
    fn single_voter_owns_everything() {
        let voters = &[7u64];
        assert_eq!(owner(&key("db", "main"), voters), Some(7));
        assert_eq!(owner(&key("db", "feature"), voters), Some(7));
        assert_eq!(owner(&key("other", "main"), voters), Some(7));
    }

    #[test]
    fn owner_is_independent_of_voter_order() {
        let k = key("db", "main");
        assert_eq!(
            owner(&k, &[1, 2, 3, 4]),
            owner(&k, &[4, 3, 2, 1]),
            "owner must not depend on input ordering",
        );
    }

    #[test]
    fn removing_a_voter_only_reassigns_its_branches() {
        let before: Vec<NodeId> = (1..=4).collect();
        let after: Vec<NodeId> = vec![1, 2, 3]; // dropped node 4
        let keys: Vec<RefKey> = (0..1000)
            .map(|i| key("db", &format!("branch-{i}")))
            .collect();

        // Branches that pointed to a surviving node before must still
        // point to the same surviving node after — only the dropped
        // node's branches reassign.
        let kept: Vec<_> = keys
            .iter()
            .filter(|k| owner(k, &before).unwrap() != 4)
            .collect();
        for k in &kept {
            assert_eq!(
                owner(k, &before),
                owner(k, &after),
                "branch owned by a surviving node must keep its owner",
            );
        }
    }
}
