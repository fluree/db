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
    assert!(chain(std::slice::from_ref(&step2), &[vec![2]]).is_err());
    assert!(chain(std::slice::from_ref(&step2), &slots).is_err());
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

#[test]
fn a_disclosure_verifies_only_against_its_commit() {
    use host::{authenticate_image_id, disclose, new_seed, prove_authenticate, verify_disclosure};
    use zk_redaction_core::Signatures;
    let input = commit_blob(16, true, false, false);
    let seed = new_seed();
    let (receipt, claim, ..) =
        prove_authenticate(&input, &seed, LEDGER, Signatures::Verify).expect("prove");
    let anchored = commit_cid(&HostSha::sha256(&input));
    assert_eq!(claim.cid, anchored);
    let disclosure = disclose(&input, &seed, 3).unwrap();
    let check = |receipt, image, cid: &[u8], d: &host::Disclosure| {
        verify_disclosure(receipt, image, cid, d)
    };
    check(&receipt, authenticate_image_id(), &anchored, &disclosure).expect("verifies");

    // Another program, or another commit.
    assert!(check(&receipt, image_id(), &anchored, &disclosure).is_err());
    let other = commit_cid(&HostSha::sha256(b"another commit"));
    assert!(check(&receipt, authenticate_image_id(), &other, &disclosure).is_err());
    // A changed operation.
    let mut changed = disclosure.clone();
    *changed.leaf.last_mut().unwrap() ^= 1;
    assert!(check(&receipt, authenticate_image_id(), &anchored, &changed).is_err());
    // The operation claimed for another slot, or another slot's path.
    let mut moved = disclosure.clone();
    moved.slot = 4;
    assert!(check(&receipt, authenticate_image_id(), &anchored, &moved).is_err());
    let mut wrong_path = disclosure.clone();
    wrong_path.path = disclose(&input, &seed, 4).unwrap().path;
    assert!(check(&receipt, authenticate_image_id(), &anchored, &wrong_path).is_err());
    // Another salt: another slot's, or the same leaf under another seed.
    let mut resalted = disclosure.clone();
    resalted.salt = disclose(&input, &seed, 4).unwrap().salt;
    assert!(check(&receipt, authenticate_image_id(), &anchored, &resalted).is_err());
    let other_seed = disclose(&input, &new_seed(), 3).unwrap();
    assert!(check(&receipt, authenticate_image_id(), &anchored, &other_seed).is_err());
}

#[test]
fn a_chain_proof_covers_its_whole_ancestry_and_nothing_else() {
    use host::{chain_image_id, commit_chain, prove_chain, verify_chain_claim};
    let commits = commit_chain(3, 8);
    let cid = |c: &[u8]| commit_cid(&HostSha::sha256(c));
    let links = prove_chain(&commits).expect("prove");
    let last = &links[2].receipt;
    let (base, head) = (cid(&commits[0]), cid(&commits[2]));
    let claim = verify_chain_claim(last, chain_image_id(), &base, &head).expect("verifies");
    assert_eq!(claim.length, 3);
    // Another program, or other ends.
    assert!(verify_chain_claim(last, image_id(), &base, &head).is_err());
    assert!(verify_chain_claim(last, chain_image_id(), &cid(&commits[1]), &head).is_err());
    // A commit that does not follow the chain cannot be linked onto it.
    let stray = commit_chain(1, 8);
    assert!(prove_chain(&[commits[0].clone(), stray[0].clone()]).is_err());
}
