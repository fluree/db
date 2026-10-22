//! The transform against real commits from Fluree's writer: a replacement
//! reads, through Fluree's own reader, as the original commit less exactly
//! the removed slots.

use fluree_db_core::commit::codec::{
    commit_blob_matches, load_commit_ops, read_commit, read_commit_envelope, verify_commit_blob,
    write_commit,
};
use fluree_db_core::{Commit, ContentId, ContentKind, Flake, FlakeValue, Sid};
use fluree_db_redaction::{redact, Error, HostSha};

const LEDGER: &str = "acme/people:main";

fn flake(s: &str, p: &str, o: FlakeValue, t: i64, assert: bool) -> Flake {
    Flake::new(
        Sid::new(13, s),
        Sid::new(14, p),
        o,
        Sid::new(2, "string"),
        t,
        assert,
        None,
    )
}

fn people(n: usize, t: i64) -> Vec<Flake> {
    let mut flakes = Vec::new();
    for i in 0..n {
        flakes.push(flake(
            &format!("person-{i}"),
            "email",
            FlakeValue::String(format!("p{i}@example.org")),
            t,
            true,
        ));
        flakes.push(flake(
            &format!("person-{i}"),
            "name",
            FlakeValue::String(format!("Person {i}")),
            t,
            true,
        ));
    }
    flakes
}

fn parent() -> ContentId {
    ContentId::new(ContentKind::Commit, b"parent")
}

fn survivors(original: &Commit, removed: &[u32]) -> Vec<Flake> {
    original
        .flakes
        .iter()
        .enumerate()
        .filter(|(i, _)| !removed.contains(&(*i as u32)))
        .map(|(_, f)| f.clone())
        .collect()
}

/// `replacement` read as Fluree reads it: the original's identity and
/// envelope, its survivors under their original slots, nothing else.
fn read_as_original(replacement: &[u8], bytes: &[u8], redacted: &[u32]) {
    let original = read_commit(bytes).unwrap();
    let decoded = read_commit(replacement).unwrap();
    assert_eq!(decoded.id, original.id);
    assert_eq!(decoded.flakes, survivors(&original, redacted));
    let kept: Vec<u32> = (0..original.flakes.len() as u32)
        .filter(|s| !redacted.contains(s))
        .collect();
    let slots: Vec<u32> = decoded.occurrences().map(|(slot, _)| slot).collect();
    assert_eq!(slots, kept);
    for (slot, flake) in decoded.occurrences() {
        assert_eq!(Some(flake), original.flakes.get(slot as usize));
        assert_eq!(decoded.at_slot(slot), Some(flake));
    }
    assert!(redacted.iter().all(|s| decoded.at_slot(*s).is_none()));
    assert_eq!(
        decoded.redaction.as_ref().unwrap().op_count as usize,
        original.flakes.len()
    );
    assert_eq!(
        (decoded.t, &decoded.parents, &decoded.time, &decoded.txn),
        (original.t, &original.parents, &original.time, &original.txn)
    );
    assert_eq!(decoded.txn_meta, original.txn_meta);
    assert_eq!(decoded.namespace_delta, original.namespace_delta);
    assert!(
        decoded.commit_signatures.is_empty(),
        "no signature signs a replacement"
    );

    let envelope = read_commit_envelope(replacement).unwrap();
    assert_eq!(
        (envelope.t, envelope.parents),
        (original.t, original.parents)
    );
    let ops = load_commit_ops(replacement).unwrap();
    let mut count = 0;
    ops.for_each_op(|_| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, kept.len());
    // A replacement is never accepted as a commit blob.
    assert!(!commit_blob_matches(
        original.id.as_ref().unwrap(),
        replacement
    ));
    assert!(!commit_blob_matches(
        &ContentId::new(ContentKind::Commit, replacement),
        replacement
    ));
    assert!(verify_commit_blob(replacement).is_err());
}

fn check(bytes: &[u8], removed: &[u32]) -> (Vec<u8>, fluree_db_redaction::Statement) {
    let (replacement, statement) = redact::<HostSha, _>(bytes, removed, LEDGER, &mut ()).unwrap();
    assert_eq!(
        statement.input_cid,
        ContentId::new(ContentKind::Commit, bytes).to_bytes()
    );
    assert_eq!(
        statement.output_cid,
        ContentId::new(ContentKind::Commit, &replacement).to_bytes()
    );
    read_as_original(&replacement, bytes, removed);
    (replacement, statement)
}

#[test]
fn uncompressed_and_compressed_commits_keep_their_survivors() {
    for compress in [false, true] {
        let commit = Commit::new(7, people(50, 7)).with_parent(parent());
        let bytes = write_commit(&commit, compress, None).unwrap().bytes;
        assert_eq!(bytes[5] & 1 != 0, compress, "fixture compression");
        let (_, statement) = check(&bytes, &[0, 41]);
        assert_eq!(statement.redacted_slots, vec![0, 41]);
        assert_eq!(statement.op_count, 100);
        assert_eq!(statement.t, 7);
        assert!(statement.signers.is_empty());
    }
}

#[test]
fn a_string_only_the_removed_operation_used_is_gone_and_a_shared_one_stays() {
    let mut flakes = people(3, 2);
    // A subject used only by the operation removed below.
    flakes.push(flake(
        "secret-subject",
        "email",
        FlakeValue::String("hidden@example.org".into()),
        2,
        true,
    ));
    let commit = Commit::new(2, flakes);
    let bytes = write_commit(&commit, false, None).unwrap().bytes;
    // Slot 6 is secret-subject's; slot 0 shares "person-0" with slot 1.
    let (replacement, statement) = check(&bytes, &[0, 6]);
    assert_eq!(statement.removed_terms, 1, "only secret-subject");
    let has = |needle: &[u8]| replacement.windows(needle.len()).any(|w| w == needle);
    assert!(!has(b"secret-subject"));
    assert!(!has(b"hidden@example.org"));
    assert!(!has(b"p0@example.org"));
    assert!(has(b"person-0"), "still used by slot 1");
}

#[test]
fn retractions_and_merge_parents_are_preserved() {
    let mut flakes = people(4, 9);
    flakes.push(flake(
        "person-0",
        "email",
        FlakeValue::String("old@example.org".into()),
        9,
        false,
    ));
    let commit = Commit::new(9, flakes)
        .with_parent(parent())
        .with_merge_parents(vec![ContentId::new(ContentKind::Commit, b"merged")]);
    let bytes = write_commit(&commit, true, None).unwrap().bytes;
    check(&bytes, &[8]);
}

#[test]
fn a_signed_commit_is_verified_and_its_signer_named() {
    let key = fluree_db_credential::SigningKey::from_bytes(&[7u8; 32]);
    let commit = Commit::new(3, people(5, 3)).with_parent(parent());
    let bytes = write_commit(&commit, true, Some((&key, LEDGER)))
        .unwrap()
        .bytes;
    let (replacement, statement) = check(&bytes, &[2]);
    assert_eq!(
        statement.signers,
        vec![fluree_db_credential::did_from_pubkey(
            &key.verifying_key().to_bytes()
        )]
    );
    // Read back, the replacement names the signers the proof verified.
    let read = read_commit(&replacement).unwrap();
    assert_eq!(read.redaction.unwrap().signers, statement.signers);
    // The signature binds the ledger: another ledger id does not verify.
    assert!(matches!(
        redact::<HostSha, _>(&bytes, &[2], "other:main", &mut ()),
        Err(Error::Signature(_))
    ));
    // Nor does a tampered body.
    let mut tampered = bytes.clone();
    tampered[40] ^= 1;
    assert!(redact::<HostSha, _>(&tampered, &[2], LEDGER, &mut ()).is_err());
}

#[test]
fn a_second_erasure_works_from_the_replacement_alone() {
    let commit = Commit::new(5, people(10, 5)).with_parent(parent());
    let bytes = write_commit(&commit, true, None).unwrap().bytes;
    let original = read_commit(&bytes).unwrap();
    let (first, s1) = redact::<HostSha, _>(&bytes, &[3], LEDGER, &mut ()).unwrap();
    let (second, s2) = redact::<HostSha, _>(&first, &[1, 12], LEDGER, &mut ()).unwrap();
    assert_eq!(s2.input_cid, s1.output_cid);
    assert_eq!(s2.original_cid, s1.input_cid);
    assert_eq!(s2.removed_slots, vec![1, 12]);
    assert_eq!(s2.redacted_slots, vec![1, 3, 12]);
    read_as_original(&second, &bytes, &[1, 3, 12]);
    assert_eq!(original.flakes.len(), 20);
}

#[test]
fn invalid_slot_requests_are_refused() {
    let bytes = write_commit(&Commit::new(1, people(2, 1)), false, None)
        .unwrap()
        .bytes;
    for slots in [&[][..], &[2, 1], &[1, 1], &[4]] {
        assert!(matches!(
            redact::<HostSha, _>(&bytes, slots, LEDGER, &mut ()),
            Err(Error::Slots(_))
        ));
    }
    let (first, _) = redact::<HostSha, _>(&bytes, &[1], LEDGER, &mut ()).unwrap();
    assert!(matches!(
        redact::<HostSha, _>(&first, &[1], LEDGER, &mut ()),
        Err(Error::Slots("already removed"))
    ));
}

#[test]
fn the_replacement_is_deterministic() {
    let bytes = write_commit(&Commit::new(4, people(20, 4)), true, None)
        .unwrap()
        .bytes;
    let a = redact::<HostSha, _>(&bytes, &[5, 6], LEDGER, &mut ()).unwrap();
    let b = redact::<HostSha, _>(&bytes, &[5, 6], LEDGER, &mut ()).unwrap();
    assert_eq!(a, b);
}

#[test]
fn exposed_signatures_verify_outside_the_transform_and_bind_their_body() {
    use fluree_db_redaction::{redact_with, verify_exposed_signatures, Signatures};
    let key = fluree_db_credential::SigningKey::from_bytes(&[7u8; 32]);
    let commit = Commit::new(3, people(5, 3)).with_parent(parent());
    let bytes = write_commit(&commit, true, Some((&key, LEDGER)))
        .unwrap()
        .bytes;
    let (replacement, statement) =
        redact_with::<HostSha, _>(&bytes, &[2], LEDGER, Signatures::Expose, &mut ()).unwrap();
    // The same replacement as when the transform verifies them.
    assert_eq!(
        replacement,
        redact::<HostSha, _>(&bytes, &[2], LEDGER, &mut ())
            .unwrap()
            .0
    );
    assert!(statement.signers.is_empty());
    let did = fluree_db_credential::did_from_pubkey(&key.verifying_key().to_bytes());
    assert_eq!(
        verify_exposed_signatures::<HostSha>(&statement).unwrap(),
        vec![did]
    );

    let mut forged = statement.clone();
    forged.signatures[0].signature[0] ^= 1;
    assert!(verify_exposed_signatures::<HostSha>(&forged).is_err());
    let mut rebound = statement.clone();
    rebound.body_hash[0] ^= 1;
    assert!(verify_exposed_signatures::<HostSha>(&rebound).is_err());
    let mut misattributed = statement;
    misattributed.ledger_id = "other:main".into();
    assert!(verify_exposed_signatures::<HostSha>(&misattributed).is_err());
}
