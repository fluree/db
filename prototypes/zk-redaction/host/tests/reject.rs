//! A proof is accepted only for its program, input, original and authorized
//! slots, and only with the replacement it proved; the guest refuses inputs
//! it cannot vouch for.

use host::{commit_blob, execute, image_id, prove, verify_chain, verify_step, Expectation, LEDGER};
use zk_redaction_core::{commit_cid, redact, HostSha, Sha256};

#[test]
fn the_guest_refuses_a_tampered_or_misattributed_signed_commit() {
    let signed = commit_blob(16, true, true, false);
    assert!(execute(&signed, &[1], LEDGER).is_ok());
    let mut tampered = signed.clone();
    tampered[40] ^= 1;
    assert!(execute(&tampered, &[1], LEDGER).is_err());
    assert!(execute(&signed, &[1], "other:main").is_err());
    // Slots the commit does not have.
    assert!(execute(&signed, &[999], LEDGER).is_err());
}

#[test]
fn a_proof_verifies_only_as_what_it_proves() {
    let input = commit_blob(16, false, false, false);
    let (replacement, _) = redact::<HostSha, _>(&input, &[1], LEDGER, &mut ()).unwrap();
    let proof = prove(&input, &[1], LEDGER).expect("prove");
    let cid = commit_cid(&HostSha::sha256(&input));
    let expect = Expectation {
        input_cid: cid.clone(),
        original_cid: cid.clone(),
        removed_slots: vec![1],
    };
    verify_step(&proof.receipt, image_id(), &replacement, &expect).expect("verifies");

    // Another program.
    assert!(verify_step(&proof.receipt, [7u32; 8], &replacement, &expect).is_err());
    // Another original or input.
    let other = commit_cid(&HostSha::sha256(b"another commit"));
    let mut wrong = expect.clone();
    wrong.original_cid = other.clone();
    assert!(verify_step(&proof.receipt, image_id(), &replacement, &wrong).is_err());
    let mut wrong = expect.clone();
    wrong.input_cid = other;
    assert!(verify_step(&proof.receipt, image_id(), &replacement, &wrong).is_err());
    // Slots other than authorized.
    let mut wrong = expect.clone();
    wrong.removed_slots = vec![2];
    assert!(verify_step(&proof.receipt, image_id(), &replacement, &wrong).is_err());
    // A replacement other than the proven one (a changed survivor).
    let mut changed = replacement.clone();
    let last = changed.len() - 1;
    changed[last] ^= 1;
    assert!(verify_step(&proof.receipt, image_id(), &changed, &expect).is_err());
}

#[test]
fn a_chain_verifies_only_back_to_its_anchored_original() {
    let original = commit_blob(16, false, false, false);
    let original_cid = commit_cid(&HostSha::sha256(&original));
    let (first, _) = redact::<HostSha, _>(&original, &[1], LEDGER, &mut ()).unwrap();
    let (second, _) = redact::<HostSha, _>(&first, &[2], LEDGER, &mut ()).unwrap();
    let step1 = prove(&original, &[1], LEDGER)
        .expect("prove step 1")
        .receipt;
    let step2 = prove(&first, &[2], LEDGER).expect("prove step 2").receipt;
    let slots = vec![vec![1], vec![2]];
    let chain = |receipts: &[risc0_zkvm::Receipt], slots: &[Vec<u32>]| {
        verify_chain(receipts, image_id(), &second, &original_cid, slots)
    };
    let both = [step1.clone(), step2.clone()];
    assert_eq!(chain(&both, &slots).expect("verifies").len(), 2);

    // The first proof missing, as a chain of one or of two.
    assert!(chain(&[step2.clone()], &[vec![2]]).is_err());
    assert!(chain(&[step2.clone()], &slots).is_err());
    // The steps reordered.
    assert!(chain(&[step2.clone(), step1.clone()], &slots).is_err());
    // A first proof of another original, or of other slots.
    let other = commit_blob(16, true, false, false);
    let other_first = prove(&other, &[1], LEDGER).expect("prove").receipt;
    assert!(chain(&[other_first, step2.clone()], &slots).is_err());
    let other_slots = prove(&original, &[3], LEDGER).expect("prove").receipt;
    assert!(chain(&[other_slots, step2.clone()], &slots).is_err());
    // Authorization for other slots than were removed.
    assert!(chain(&both, &[vec![1], vec![3]]).is_err());
    // Another final replacement.
    assert!(verify_chain(&both, image_id(), &first, &original_cid, &slots).is_err());
}
