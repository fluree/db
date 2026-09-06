//! Pins rendezvous ownership to the algorithm that shipped before the
//! `fluree-raft-core` extraction.
//!
//! Ownership is computed locally and independently on every node — no
//! consensus round confirms it. Two nodes that disagree about a key's
//! digest can therefore both believe they own it, breaking the
//! at-most-one-owner invariant that makes rendezvous hashing usable for
//! exclusive work. That makes the hash a wire format, and a mixed-version
//! cluster during a rolling upgrade the scenario that breaks.
//!
//! The reference implementation below is a frozen verbatim copy of
//! `fluree-db-consensus/src/raft/ownership.rs` as of the extraction. It
//! is *supposed* to be duplicated and never refactored: its whole job is
//! to fail if the live implementation moves.

use fluree_raft_core::node::NodeId;
use fluree_raft_core::ownership::{digest_parts, owner_for_digest, RENDEZVOUS_SEED};
use xxhash_rust::xxh64::xxh64;

/// The seed as it shipped, written out rather than imported.
///
/// Importing `RENDEZVOUS_SEED` here would make the reference track the
/// implementation: changing the production seed would change most of
/// what this file expects, and the comparison tests would keep passing
/// while every existing cluster's ownership map silently moved.
const REFERENCE_SEED: u64 = 0x6661_6566_5246_4252;

/// Guards the constant the rest of this file is written against. If this
/// fails, the seed changed — which is a full-cluster-stop change, not a
/// rolling one.
#[test]
fn published_seed_matches_the_frozen_reference() {
    assert_eq!(
        RENDEZVOUS_SEED, REFERENCE_SEED,
        "RENDEZVOUS_SEED changed; ownership moves for every existing \
         cluster and a rolling upgrade will split branch ownership",
    );
}

fn reference_key_digest(ledger_name: &str, branch: &str) -> u64 {
    let hash = xxh64(ledger_name.as_bytes(), REFERENCE_SEED);
    let hash = xxh64(b":", hash);
    xxh64(branch.as_bytes(), hash)
}

fn reference_score(node: NodeId, key_digest: u64) -> u64 {
    xxh64(&node.to_le_bytes(), key_digest)
}

fn reference_owner(ledger: &str, branch: &str, voters: &[NodeId]) -> Option<NodeId> {
    let digest = reference_key_digest(ledger, branch);
    voters
        .iter()
        .copied()
        .map(|node| (reference_score(node, digest), node))
        .max_by_key(|&(score, node)| (score, node))
        .map(|(_, node)| node)
}

/// The generic `digest_parts` fold must reproduce the hand-chained
/// original exactly — not merely for typical names, but for the cases
/// where a fold could plausibly differ: a separator inside a field, an
/// empty field, and multi-byte UTF-8.
#[test]
fn digest_and_owner_match_the_reference_implementation() {
    let voters: Vec<NodeId> = (1..=7).collect();

    for l in 0..60 {
        for b in 0..60 {
            let (ledger, branch) = (format!("ledger-{l}"), format!("branch-{b}"));
            let digest = digest_parts([ledger.as_bytes(), b":".as_slice(), branch.as_bytes()]);
            assert_eq!(
                digest,
                reference_key_digest(&ledger, &branch),
                "digest drifted for {ledger}:{branch}",
            );
            assert_eq!(
                owner_for_digest(digest, &voters),
                reference_owner(&ledger, &branch, &voters),
                "owner drifted for {ledger}:{branch}",
            );
        }
    }

    for (ledger, branch) in [
        ("a:b", "c"),
        ("a", "b:c"),
        ("", "main"),
        ("db", ""),
        ("", ""),
        ("café", "brä:nch"),
    ] {
        let digest = digest_parts([ledger.as_bytes(), b":".as_slice(), branch.as_bytes()]);
        assert_eq!(
            digest,
            reference_key_digest(ledger, branch),
            "digest drifted for {ledger:?}:{branch:?}",
        );
        assert_eq!(
            owner_for_digest(digest, &voters),
            reference_owner(ledger, branch, &voters),
            "owner drifted for {ledger:?}:{branch:?}",
        );
    }
}

/// A voter set spanning the id range an operator might actually use,
/// including ids far apart in magnitude, since the score folds the id's
/// little-endian bytes.
#[test]
fn owner_matches_the_reference_across_voter_set_shapes() {
    let sets: &[&[NodeId]] = &[
        &[1],
        &[1, 2],
        &[1, 2, 3],
        &[1, 2, 3, 4, 5],
        &[7, 42, 1000, u64::MAX],
        &[0, 1],
    ];
    for voters in sets {
        for i in 0..200 {
            let branch = format!("branch-{i}");
            let digest = digest_parts([b"db".as_slice(), b":".as_slice(), branch.as_bytes()]);
            assert_eq!(
                owner_for_digest(digest, *voters),
                reference_owner("db", &branch, voters),
                "owner drifted for db:{branch} over {voters:?}",
            );
        }
    }
}
