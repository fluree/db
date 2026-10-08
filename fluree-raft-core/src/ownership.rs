//! Deterministic work assignment across cluster members.
//!
//! Every node independently maps a key to the [`NodeId`] that should
//! own it, using rendezvous hashing (Highest Random Weight): score each
//! `(key, node)` pair, highest score wins. Reassignment fraction on a
//! membership change is `~1/(N+1)`.
//!
//! The key is reduced to a `u64` digest by [`digest_parts`] before it
//! reaches [`owner_for_digest`], so this module stays free of any
//! application's key type while every application still gets the same
//! hash function.
//!
//! ## Cross-version agreement is a safety property
//!
//! Ownership is computed locally and independently on every node; no
//! consensus round confirms it. If two nodes disagree about a key's
//! digest — say, during a rolling upgrade where one binary changed the
//! hash — both can believe they own it, and the at-most-one-owner
//! invariant that makes rendezvous hashing usable for exclusive work
//! silently breaks.
//!
//! So: [`RENDEZVOUS_SEED`], the xxh64 choice, the fold order in
//! [`digest_parts`], and the tie-break in [`owner_for_digest`] are all
//! wire format. Changing any of them is a breaking change requiring a
//! full cluster stop, not a rolling restart. `digest_is_stable` and
//! `owner_for_digest_is_stable` pin them with golden values.

use crate::node::NodeId;
use xxhash_rust::xxh64::xxh64;

/// Fixed seed for the rendezvous hash.
///
/// Fixed rather than random because every node must compute identical
/// scores for the same `(key, node)` pair. `std`'s `DefaultHasher` is
/// randomly seeded per process and would not.
pub const RENDEZVOUS_SEED: u64 = 0x6661_6566_5246_4252;

/// Reduce a composite key to the digest [`owner_for_digest`] consumes.
///
/// Chains xxh64 over `parts` in order, starting from
/// [`RENDEZVOUS_SEED`]. Callers with a multi-field key should
/// interleave an unambiguous separator so distinct keys can't collide
/// through concatenation — `["a", ":", "bc"]` and `["ab", ":", "c"]`
/// digest differently, but `["a", "bc"]` and `["ab", "c"]` would not.
pub fn digest_parts<'a, I>(parts: I) -> u64
where
    I: IntoIterator<Item = &'a [u8]>,
{
    parts
        .into_iter()
        .fold(RENDEZVOUS_SEED, |hash, part| xxh64(part, hash))
}

/// Resolve the owner of `key_digest` from a non-empty voter set.
///
/// Returns `None` when `voters` is empty; callers should treat that as
/// "cluster not yet bootstrapped, defer." All other inputs yield
/// exactly one owner; ties (which would require a hash collision across
/// two `NodeId`s and the same digest) break by the higher `NodeId` so
/// the result is fully deterministic.
///
/// Accepts any borrow that yields `&NodeId` — `&[NodeId]`,
/// `&Vec<NodeId>`, `&BTreeSet<NodeId>`, etc. — so a per-tick supervisor
/// can iterate replicated state directly without allocating a `Vec` per
/// call.
pub fn owner_for_digest<'a, I>(key_digest: u64, voters: I) -> Option<NodeId>
where
    I: IntoIterator<Item = &'a NodeId>,
{
    voters
        .into_iter()
        .copied()
        .map(|node| (rendezvous_score(node, key_digest), node))
        .max_by_key(|&(score, node)| (score, node))
        .map(|(_, node)| node)
}

fn rendezvous_score(node: NodeId, key_digest: u64) -> u64 {
    xxh64(&node.to_le_bytes(), key_digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Digest of the two-part key `("db", "main")` joined by `:` — the
    /// shape the nameservice's branch keys use.
    fn key(name: &str, branch: &str) -> u64 {
        digest_parts([name.as_bytes(), b":".as_slice(), branch.as_bytes()])
    }

    /// Golden values. See the module docs: these are wire format, and a
    /// change here breaks rolling upgrades. `fluree-db-consensus` pins
    /// the same constant through its `RefKey` wrapper, so a divergence
    /// between the generic function and the nameservice's use of it
    /// fails there too.
    #[test]
    fn digest_is_stable() {
        assert_eq!(key("db", "main"), 0x446a_dd93_2b74_321e);
        assert_eq!(digest_parts([] as [&[u8]; 0]), RENDEZVOUS_SEED);
    }

    #[test]
    fn owner_for_digest_is_stable() {
        let voters: Vec<NodeId> = (1..=5).collect();
        assert_eq!(owner_for_digest(key("db", "main"), &voters), Some(3));
        assert_eq!(owner_for_digest(key("db", "feature"), &voters), Some(4));
        assert_eq!(owner_for_digest(key("other", "main"), &voters), Some(3));
    }

    #[test]
    fn separator_disambiguates_composite_keys() {
        assert_ne!(
            key("ab", "c"),
            key("a", "bc"),
            "a separated fold must not let field boundaries slide",
        );
    }

    #[test]
    fn empty_voter_set_yields_no_owner() {
        assert_eq!(owner_for_digest(key("db", "main"), &[]), None);
    }

    #[test]
    fn single_voter_owns_everything() {
        let voters = &[7u64];
        assert_eq!(owner_for_digest(key("db", "main"), voters), Some(7));
        assert_eq!(owner_for_digest(key("db", "feature"), voters), Some(7));
        assert_eq!(owner_for_digest(key("other", "main"), voters), Some(7));
    }

    #[test]
    fn owner_is_deterministic_across_invocations() {
        let voters = &[1u64, 2, 3, 4];
        let k = key("db", "main");
        let first = owner_for_digest(k, voters);
        for _ in 0..10 {
            assert_eq!(owner_for_digest(k, voters), first);
        }
    }

    #[test]
    fn owner_is_independent_of_voter_order() {
        let k = key("db", "main");
        assert_eq!(
            owner_for_digest(k, &[1, 2, 3, 4]),
            owner_for_digest(k, &[4, 3, 2, 1]),
            "owner must not depend on input ordering",
        );
        assert_eq!(
            owner_for_digest(k, &[1, 2, 3, 4]),
            owner_for_digest(k, &[3, 1, 4, 2]),
            "owner must not depend on input ordering",
        );
    }

    #[test]
    fn distribution_across_many_branches_is_balanced() {
        let voters: Vec<NodeId> = (1..=4).collect();
        let mut counts = [0usize; 5]; // index by NodeId 1..=4
        for i in 0..1000 {
            let k = key("db", &format!("branch-{i}"));
            let owner = owner_for_digest(k, &voters).unwrap();
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
        let keys: Vec<u64> = (0..1000)
            .map(|i| key("db", &format!("branch-{i}")))
            .collect();

        let moved = keys
            .iter()
            .filter(|k| owner_for_digest(**k, &before) != owner_for_digest(**k, &after))
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
        let keys: Vec<u64> = (0..1000)
            .map(|i| key("db", &format!("branch-{i}")))
            .collect();

        // Branches that pointed to a surviving node before must still
        // point to the same surviving node after — only the dropped
        // node's branches reassign.
        let kept: Vec<_> = keys
            .iter()
            .filter(|k| owner_for_digest(**k, &before).unwrap() != 4)
            .collect();
        for k in &kept {
            assert_eq!(
                owner_for_digest(**k, &before),
                owner_for_digest(**k, &after),
                "branch owned by a surviving node must keep its owner",
            );
        }
    }
}
