//! A commit's redaction proofs verify only as what they prove, and a fake
//! receipt never passes a production verifier.

#![cfg(feature = "testing")]

use fluree_db_core::commit::codec::write_commit;
use fluree_db_core::{Commit, ContentId, ContentKind, Flake, FlakeValue, Sid};
use fluree_db_erasure::testing::{fake_receipt, TEST_IMAGE_ID};
use fluree_db_erasure::Verifier;
use fluree_db_redaction::{commit_cid, redact, redact_with, HostSha, Sha256, Signatures};

const LEDGER: &str = "acme/people:main";

/// Two steps, each asked to verify the original's signatures.
const VERIFY: &[Signatures] = &[Signatures::Verify, Signatures::Verify];

fn original() -> Vec<u8> {
    let flakes = (0..12)
        .map(|i| {
            Flake::new(
                Sid::new(13, format!("person-{i}")),
                Sid::new(14, "email"),
                FlakeValue::String(format!("p{i}@example.org")),
                Sid::new(2, "string"),
                3,
                true,
                None,
            )
        })
        .collect();
    let commit = Commit::new(3, flakes).with_parent(ContentId::new(ContentKind::Commit, b"parent"));
    write_commit(&commit, true, None).unwrap().bytes
}

/// Two erasures of one commit: receipts, final replacement, original CID.
fn two_steps() -> (Vec<Vec<u8>>, Vec<u8>, Vec<u8>) {
    let bytes = original();
    let (first, s1) = redact::<HostSha, _>(&bytes, &[2], LEDGER, &mut ()).unwrap();
    let (second, s2) = redact::<HostSha, _>(&first, &[5, 9], LEDGER, &mut ()).unwrap();
    let cid = commit_cid(&HostSha::sha256(&bytes));
    (vec![fake_receipt(&s1), fake_receipt(&s2)], second, cid)
}

#[test]
fn a_chain_verifies_back_to_its_original_and_nothing_else() {
    let (receipts, replacement, cid) = two_steps();
    let authorized = vec![vec![2], vec![5, 9]];
    let v = Verifier::accepting_dev_receipts();
    let statements = v
        .verify_chain(&receipts, &replacement, &cid, LEDGER, &authorized, VERIFY)
        .unwrap();
    assert_eq!(statements[1].redacted_slots, vec![2, 5, 9]);

    let check =
        |receipts: &[Vec<u8>], replacement: &[u8], cid: &[u8], ledger, slots: &[Vec<u32>]| {
            v.verify_chain(
                receipts,
                replacement,
                cid,
                ledger,
                slots,
                &VERIFY[..receipts.len()],
            )
        };
    // Another original, ledger, slots, order, a step missing, another
    // replacement.
    let other = commit_cid(&HostSha::sha256(b"another"));
    assert!(check(&receipts, &replacement, &other, LEDGER, &authorized).is_err());
    assert!(check(&receipts, &replacement, &cid, "other:main", &authorized).is_err());
    assert!(check(&receipts, &replacement, &cid, LEDGER, &[vec![2], vec![5]]).is_err());
    let reordered = vec![receipts[1].clone(), receipts[0].clone()];
    assert!(check(&reordered, &replacement, &cid, LEDGER, &authorized).is_err());
    assert!(check(&receipts[1..], &replacement, &cid, LEDGER, &authorized[1..]).is_err());
    let mut changed = replacement.clone();
    *changed.last_mut().unwrap() ^= 1;
    assert!(check(&receipts, &changed, &cid, LEDGER, &authorized).is_err());
}

#[test]
fn a_fake_receipt_never_passes_a_production_verifier() {
    let (receipts, replacement, cid) = two_steps();
    let authorized = vec![vec![2], vec![5, 9]];
    // Even for the image it claims, and even with dev mode in the
    // environment.
    std::env::set_var("RISC0_DEV_MODE", "1");
    let production = Verifier::with_image_ids(vec![TEST_IMAGE_ID]);
    let outcome =
        production.verify_chain(&receipts, &replacement, &cid, LEDGER, &authorized, VERIFY);
    std::env::remove_var("RISC0_DEV_MODE");
    assert!(outcome.is_err());
    assert!(Verifier::default()
        .verify_chain(&receipts, &replacement, &cid, LEDGER, &authorized, VERIFY)
        .is_err());
}

#[test]
fn a_receipt_of_another_program_is_refused() {
    let (receipts, replacement, cid) = two_steps();
    let authorized = vec![vec![2], vec![5, 9]];
    let elsewhere = Verifier::accepting_dev_receipts_for(vec![[1; 8]]);
    assert!(elsewhere
        .verify_chain(&receipts, &replacement, &cid, LEDGER, &authorized, VERIFY)
        .is_err());
}

/// A step asked to verify the original's signatures inside the proof does
/// not take a proof that exposed them instead, wherever it is in the chain.
#[test]
fn a_verify_step_refuses_a_proof_that_exposed_the_signatures() {
    use Signatures::{Expose, Verify};
    let key = fluree_db_credential::SigningKey::from_bytes(&[7u8; 32]);
    let commit = fluree_db_core::commit::codec::read_commit(&original()).unwrap();
    let bytes = write_commit(&commit, true, Some((&key, LEDGER)))
        .unwrap()
        .bytes;
    let cid = commit_cid(&HostSha::sha256(&bytes));
    let (first, exposed) =
        redact_with::<HostSha, _>(&bytes, &[2], LEDGER, Expose, &mut ()).unwrap();
    assert!(!exposed.signatures.is_empty());
    let (second, verified) = redact::<HostSha, _>(&first, &[5], LEDGER, &mut ()).unwrap();
    let receipts = vec![fake_receipt(&exposed), fake_receipt(&verified)];
    let v = Verifier::accepting_dev_receipts();
    let chain = |modes: &[Signatures]| {
        v.verify_chain(&receipts, &second, &cid, LEDGER, &[vec![2], vec![5]], modes)
    };
    assert!(chain(&[Expose, Verify]).is_ok());
    assert!(chain(&[Verify, Verify]).is_err());
    assert!(chain(&[Expose]).is_err(), "a mode for each step");
}
