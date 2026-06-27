//! Deterministic per-branch worker assignment.
//!
//! Every node independently maps a [`RefKey`] to the [`NodeId`] that
//! should run its worker, using rendezvous hashing (Highest Random
//! Weight): score each `(ref_key, node)` pair, highest score wins.
//! Reassignment fraction on a membership change is `~1/(N+1)`.
//!
//! [`xxh64`] is fixed-seeded so every node computes identical scores
//! for the same `(ref_key, node)` pair. `std`'s `DefaultHasher` is
//! randomly seeded per process and would not.
//!
//! Total: any non-empty voter set yields exactly one owner per
//! [`RefKey`], so cluster-wide at-most-one ownership is structural.

use crate::raft::state_machine::RefKey;
use crate::raft::NodeId;
use xxhash_rust::xxh64::xxh64;

const RENDEZVOUS_SEED: u64 = 0x6661_6566_5246_4252;

/// Resolve the owner of `ref_key` from a non-empty voter set.
///
/// Returns `None` when `voters` is empty; callers should treat that
/// as "cluster not yet bootstrapped, defer staging." All other inputs
/// yield exactly one owner; ties (which would require a hash
/// collision across two `NodeId`s and the same `ref_key`) break by
/// the higher `NodeId` so the result is fully deterministic.
///
/// Accepts any borrow that yields `&NodeId` — `&[NodeId]`,
/// `&Vec<NodeId>`, `&BTreeSet<NodeId>`, etc. — so the per-supervisor
/// tick can iterate `state.worker_eligible_voters` directly without
/// allocating a `Vec` per call.
pub fn owner<'a, I>(ref_key: &RefKey, voters: I) -> Option<NodeId>
where
    I: IntoIterator<Item = &'a NodeId>,
{
    let key_digest = key_digest(ref_key);
    voters
        .into_iter()
        .copied()
        .map(|node| (rendezvous_score(node, key_digest), node))
        .max_by_key(|&(score, node)| (score, node))
        .map(|(_, node)| node)
}

fn key_digest(ref_key: &RefKey) -> u64 {
    let hash = xxh64(ref_key.ledger_name.as_bytes(), RENDEZVOUS_SEED);
    let hash = xxh64(b":", hash);
    xxh64(ref_key.branch.as_bytes(), hash)
}

fn rendezvous_score(node: NodeId, key_digest: u64) -> u64 {
    xxh64(&node.to_le_bytes(), key_digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str, branch: &str) -> RefKey {
        RefKey::new(name, branch)
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
    fn owner_is_deterministic_across_invocations() {
        let voters = &[1u64, 2, 3, 4];
        let k = key("db", "main");
        let first = owner(&k, voters);
        for _ in 0..10 {
            assert_eq!(owner(&k, voters), first);
        }
    }

    #[test]
    fn owner_is_independent_of_voter_order() {
        let k = key("db", "main");
        assert_eq!(
            owner(&k, &[1, 2, 3, 4]),
            owner(&k, &[4, 3, 2, 1]),
            "owner must not depend on input ordering",
        );
        assert_eq!(
            owner(&k, &[1, 2, 3, 4]),
            owner(&k, &[3, 1, 4, 2]),
            "owner must not depend on input ordering",
        );
    }

    #[test]
    fn distribution_across_many_branches_is_balanced() {
        let voters: Vec<NodeId> = (1..=4).collect();
        let mut counts = [0usize; 5]; // index by NodeId 1..=4
        for i in 0..1000 {
            let k = key("db", &format!("branch-{i}"));
            let owner = owner(&k, &voters).unwrap();
            counts[owner as usize] += 1;
        }
        // Expected per node: 250. Allow ±50% slack — rendezvous is
        // balanced but not perfectly uniform on small samples.
        for node in 1..=4 {
            let count = counts[node as usize];
            assert!(
                (125..=375).contains(&count),
                "node {node} owns {count} of 1000 branches; expected ~250",
            );
        }
    }

    #[test]
    fn adding_a_voter_moves_only_a_small_fraction() {
        let before: Vec<NodeId> = (1..=4).collect();
        let after: Vec<NodeId> = (1..=5).collect();
        let keys: Vec<RefKey> = (0..1000)
            .map(|i| key("db", &format!("branch-{i}")))
            .collect();

        let moved = keys
            .iter()
            .filter(|k| owner(k, &before) != owner(k, &after))
            .count();

        // Rendezvous moves ~1/(N+1) = 1/5 = 20% on average. Plain
        // modulo would move ~67% in the same scenario. Allow generous
        // bounds (10%-35%) since this is a probabilistic property
        // not an exact one.
        assert!(
            (100..=350).contains(&moved),
            "rendezvous should move ~20% on 4→5; moved {moved}/1000",
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
