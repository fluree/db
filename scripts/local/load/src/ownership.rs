//! Local mirror of `fluree-db-consensus/src/raft/ownership.rs`.
//!
//! Same xxh64 seed, same key-digest construction, same tie-break
//! (higher NodeId wins on score tie), so this tool can compute the
//! expected owner of any `(ledger, branch)` for a given voter set
//! without an RPC. Used by the reporter to surface
//! "23/100 branches reassigned at t=15.2s" events during chaos.
//!
//! Keep this file in sync with the consensus crate if anything about
//! the rendezvous algorithm changes there.

use xxhash_rust::xxh64::xxh64;

/// Fixed xxh64 seed. **Must** match
/// `fluree_db_consensus::raft::ownership::RENDEZVOUS_SEED` for the
/// local computation to agree with the cluster's actual assignments.
const RENDEZVOUS_SEED: u64 = 0x6661_6566_5246_4252;

/// Compute the owner of `(ledger, branch)` given a voter set.
///
/// Returns `None` when `voters` is empty. The chosen node is the
/// element of `voters` whose `rendezvous_score(node, key_digest)`
/// is highest; ties (vanishingly unlikely with xxh64) break by
/// higher `NodeId` so the result is fully deterministic.
pub fn owner(ledger: &str, branch: &str, voters: &[u64]) -> Option<u64> {
    if voters.is_empty() {
        return None;
    }
    let digest = key_digest(ledger, branch);
    voters
        .iter()
        .copied()
        .map(|node| (rendezvous_score(node, digest), node))
        .max_by_key(|&(score, node)| (score, node))
        .map(|(_, node)| node)
}

fn key_digest(ledger: &str, branch: &str) -> u64 {
    let hash = xxh64(ledger.as_bytes(), RENDEZVOUS_SEED);
    let hash = xxh64(b":", hash);
    xxh64(branch.as_bytes(), hash)
}

fn rendezvous_score(node: u64, key_digest: u64) -> u64 {
    xxh64(&node.to_le_bytes(), key_digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_voters_returns_none() {
        assert!(owner("ledger", "main", &[]).is_none());
    }

    #[test]
    fn single_voter_always_owns() {
        assert_eq!(owner("ledger", "main", &[7]), Some(7));
        assert_eq!(owner("any", "branch", &[42]), Some(42));
    }

    #[test]
    fn order_independent() {
        let a = owner("ledger", "main", &[1, 2, 3, 4]);
        let b = owner("ledger", "main", &[4, 3, 2, 1]);
        let c = owner("ledger", "main", &[3, 1, 4, 2]);
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn assignment_is_balanced_at_scale() {
        let voters = [1u64, 2, 3, 4];
        let mut counts = [0usize; 5];
        for i in 0..1000 {
            let branch = format!("branch-{i}");
            let o = owner("ledger", &branch, &voters).unwrap();
            counts[o as usize] += 1;
        }
        for &count in &counts[1..5] {
            assert!((125..=375).contains(&count), "imbalanced: {counts:?}");
        }
    }

    #[test]
    fn membership_shrink_only_moves_dropped_node_keys() {
        let before = [1u64, 2, 3, 4];
        let after = [1u64, 2, 3];
        for i in 0..200 {
            let branch = format!("branch-{i}");
            let pre = owner("ledger", &branch, &before).unwrap();
            let post = owner("ledger", &branch, &after).unwrap();
            if pre != 4 {
                assert_eq!(pre, post, "stable for branch {branch}");
            }
        }
    }
}
