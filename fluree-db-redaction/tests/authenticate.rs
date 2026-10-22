//! Commit authentication against real commits from Fluree's writer: the
//! claim names the commit's CID and parents as Fluree reads them, and any
//! operation's leaf is shown to be in it by its salt and a Merkle path.

use fluree_db_core::commit::codec::write_commit;
use fluree_db_core::{
    Commit, ContentId, ContentKind, Flake, FlakeValue, Sid, TxnMetaEntry, TxnMetaValue,
};
use fluree_db_redaction::{
    audit_path, authenticate, commit_cid, leaf_hash, leaf_salt, leaf_salts, leaves, merkle_root,
    root_from_path, CommitClaim, HostSha, Sha256, Signatures, SALT_LEN,
};

const LEDGER: &str = "acme/people:main";
const SEED: [u8; 32] = [9; 32];

fn claim_of(bytes: &[u8], seed: &[u8; 32]) -> CommitClaim {
    let n = leaves(bytes).unwrap().len();
    let salts = leaf_salts::<HostSha>(seed, n);
    authenticate::<HostSha, _>(bytes, LEDGER, Signatures::Verify, &salts, &mut ()).unwrap()
}

fn salted_hashes(leaves: &[Vec<u8>], seed: &[u8; 32]) -> Vec<[u8; 32]> {
    leaves
        .iter()
        .enumerate()
        .map(|(i, l)| leaf_hash::<HostSha>(&leaf_salt::<HostSha>(seed, i as u32), l))
        .collect()
}

fn flakes(n: usize) -> Vec<Flake> {
    (0..n)
        .map(|i| {
            Flake::new(
                Sid::new(13, format!("person-{}", i / 2)),
                Sid::new(14, if i % 2 == 0 { "email" } else { "name" }),
                FlakeValue::String(format!("value-{i}")),
                Sid::new(2, "string"),
                7,
                true,
                None,
            )
        })
        .collect()
}

fn cid(label: &[u8]) -> ContentId {
    ContentId::new(ContentKind::Commit, label)
}

#[test]
fn the_claim_names_the_commit_and_its_parents_as_fluree_reads_them() {
    let meta = vec![
        TxnMetaEntry {
            predicate_ns: 14,
            predicate_name: "source".into(),
            value: TxnMetaValue::String("import".into()),
        },
        TxnMetaEntry {
            predicate_ns: 14,
            predicate_name: "batch".into(),
            value: TxnMetaValue::Long(42),
        },
    ];
    let commit = Commit::new(7, flakes(10))
        .with_merge_parents(vec![cid(b"p1"), cid(b"p2")])
        .with_txn_meta(meta);
    let bytes = write_commit(&commit, true, None).unwrap().bytes;
    let claim = claim_of(&bytes, &SEED);
    assert_eq!(claim.cid, commit_cid(&HostSha::sha256(&bytes)));
    assert_eq!(claim.t, 7);
    assert_eq!(claim.op_count, 10);
    assert_eq!(
        claim.parents,
        vec![cid(b"p1").to_bytes(), cid(b"p2").to_bytes()]
    );

    let single = Commit::new(8, flakes(2)).with_parent(cid(b"p"));
    let bytes = write_commit(&single, false, None).unwrap().bytes;
    assert_eq!(claim_of(&bytes, &SEED).parents, vec![cid(b"p").to_bytes()]);
}

#[test]
fn any_operation_is_disclosed_by_its_leaf_salt_and_path() {
    let commit = Commit::new(7, flakes(37)).with_parent(cid(b"p"));
    let bytes = write_commit(&commit, true, None).unwrap().bytes;
    let claim = claim_of(&bytes, &SEED);
    let leaves = leaves(&bytes).unwrap();
    assert_eq!(leaves.len(), 37);
    let hashes = salted_hashes(&leaves, &SEED);
    let root = claim.ops_root.as_slice();
    let salt = |slot: usize| leaf_salt::<HostSha>(&SEED, slot as u32);
    for (slot, leaf) in leaves.iter().enumerate() {
        let path = audit_path::<HostSha>(slot, &hashes);
        let found =
            root_from_path::<HostSha>(slot, 37, leaf_hash::<HostSha>(&salt(slot), leaf), &path);
        assert_eq!(found.as_ref().map(|r| r.as_slice()), Some(root));
        // The leaf carries the value it discloses.
        let value = format!("value-{slot}");
        assert!(leaf.windows(value.len()).any(|w| w == value.as_bytes()));
    }
    // A changed leaf, another position, another leaf's path or another salt
    // leads elsewhere.
    let path = audit_path::<HostSha>(5, &hashes);
    let mut changed = leaves[5].clone();
    *changed.last_mut().unwrap() ^= 1;
    let elsewhere = |m, salt: [u8; SALT_LEN], leaf: &[u8], path: &[[u8; 32]]| {
        root_from_path::<HostSha>(m, 37, leaf_hash::<HostSha>(&salt, leaf), path)
            .is_none_or(|r| r.as_slice() != root)
    };
    assert!(elsewhere(5, salt(5), &changed, &path));
    assert!(elsewhere(6, salt(5), &leaves[5], &path));
    assert!(elsewhere(
        5,
        salt(5),
        &leaves[5],
        &audit_path::<HostSha>(6, &hashes)
    ));
    assert!(elsewhere(5, salt(6), &leaves[5], &path));
}

#[test]
fn a_path_reveals_no_leaf_it_does_not_disclose() {
    let commit = Commit::new(7, flakes(8)).with_parent(cid(b"p"));
    let bytes = write_commit(&commit, true, None).unwrap().bytes;
    let leaves = leaves(&bytes).unwrap();
    let hashes = salted_hashes(&leaves, &SEED);
    // Each leaf has its own salt: a disclosed salt opens no other leaf, and
    // the leaf without it matches nothing.
    for (m, leaf) in leaves.iter().enumerate() {
        for k in 0..leaves.len() {
            if k != m {
                let disclosed = leaf_salt::<HostSha>(&SEED, k as u32);
                assert_ne!(leaf_hash::<HostSha>(&disclosed, leaf), hashes[m]);
            }
        }
        assert_ne!(leaf_hash::<HostSha>(&[0; SALT_LEN], leaf), hashes[m]);
    }
    // The root commits to the salts as well as the leaves.
    assert_ne!(
        claim_of(&bytes, &SEED).ops_root,
        claim_of(&bytes, &[8; 32]).ops_root
    );
    assert_eq!(
        claim_of(&bytes, &SEED).ops_root,
        merkle_root::<HostSha>(&hashes).to_vec()
    );
    // One salt per operation, no more and no fewer.
    for n in [7, 9] {
        let salts = leaf_salts::<HostSha>(&SEED, n);
        assert!(
            authenticate::<HostSha, _>(&bytes, LEDGER, Signatures::Verify, &salts, &mut ())
                .is_err()
        );
    }
}

#[test]
fn every_position_of_every_tree_size_has_a_path_to_the_root() {
    for n in 1..=33usize {
        let hashes: Vec<[u8; 32]> = (0..n)
            .map(|i| leaf_hash::<HostSha>(&[i as u8; SALT_LEN], &(i as u32).to_le_bytes()))
            .collect();
        let root = merkle_root::<HostSha>(&hashes);
        for m in 0..n {
            let path = audit_path::<HostSha>(m, &hashes);
            assert_eq!(
                root_from_path::<HostSha>(m, n, hashes[m], &path),
                Some(root),
                "n={n} m={m}"
            );
        }
        assert_eq!(root_from_path::<HostSha>(n, n, hashes[0], &[]), None);
    }
}

#[test]
fn a_chain_link_follows_only_its_previous_head() {
    use fluree_db_redaction::chain_link;
    let image = [7u32; 8];
    let first = write_commit(&Commit::new(1, flakes(2)), false, None)
        .unwrap()
        .bytes;
    let first_cid = ContentId::new(ContentKind::Commit, &first);
    let second = write_commit(
        &Commit::new(2, flakes(2)).with_parent(first_cid.clone()),
        true,
        None,
    )
    .unwrap()
    .bytes;
    let start = chain_link::<HostSha>(&first, None, image).unwrap();
    assert_eq!(start.length, 1);
    assert_eq!(start.head_cid, first_cid.to_bytes());
    let next = chain_link::<HostSha>(&second, Some(&start), image).unwrap();
    assert_eq!((next.length, next.head_t), (2, 2));
    assert_eq!(next.base_cid, first_cid.to_bytes());
    // Not its parent, or another program's link.
    assert!(chain_link::<HostSha>(&second, Some(&next), image).is_err());
    assert!(chain_link::<HostSha>(&second, Some(&start), [8u32; 8]).is_err());
}
