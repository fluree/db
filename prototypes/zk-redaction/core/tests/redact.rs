//! The transform against real commits from Fluree's writer: the survivors a
//! replacement keeps decode, through Fluree's own reader, to exactly the
//! original's flakes minus the removed slots.

use fluree_db_core::commit::codec::{read_commit, write_commit};
use fluree_db_core::{Commit, ContentId, ContentKind, Flake, FlakeValue, Sid};
use zk_redaction_core::{redact, view_replacement, Error, HostSha, ReplacementView};

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

/// A V4 blob holding a replacement's survivors, with removed dictionary
/// entries left as empty strings so ids keep their positions.
fn reconstruct(view: &ReplacementView, original: &[u8]) -> Vec<u8> {
    let mut ops = Vec::new();
    let mut count = 0u32;
    for op in view.slots.iter().flatten() {
        ops.extend_from_slice(op);
        count += 1;
    }
    let mut out = Vec::new();
    let mut header = original[..32].to_vec();
    header[5] = 0; // no compression, no signature
    header[10..14].copy_from_slice(&count.to_le_bytes());
    header[18..20].copy_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&view.envelope);
    out.extend_from_slice(&ops);
    let mut locations = Vec::new();
    for dict in &view.dicts {
        let start = out.len();
        let mut buf = Vec::new();
        varint(dict.len() as u64, &mut buf);
        for entry in dict {
            let s: &[u8] = entry.as_deref().unwrap_or(b"");
            varint(s.len() as u64, &mut buf);
            buf.extend_from_slice(s);
        }
        out.extend_from_slice(&buf);
        locations.push((start as u64, buf.len() as u32));
    }
    for (offset, len) in locations {
        out.extend_from_slice(&offset.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    out.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    out
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
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

fn check(bytes: &[u8], removed: &[u32]) -> (Vec<u8>, zk_redaction_core::Statement) {
    let original = read_commit(bytes).unwrap();
    let (replacement, statement) = redact::<HostSha, _>(bytes, removed, LEDGER, &mut ()).unwrap();
    assert_eq!(
        statement.input_cid,
        ContentId::new(ContentKind::Commit, bytes).to_bytes()
    );
    assert_eq!(
        statement.output_cid,
        ContentId::new(ContentKind::Commit, &replacement).to_bytes()
    );
    let view = view_replacement(&replacement).unwrap();
    let decoded = read_commit(&reconstruct(&view, bytes)).unwrap();
    assert_eq!(decoded.flakes, survivors(&original, removed));
    assert_eq!(decoded.parents, original.parents);
    assert_eq!(decoded.t, original.t);
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
    let (_, statement) = check(&bytes, &[2]);
    assert_eq!(
        statement.signers,
        vec![fluree_db_credential::did_from_pubkey(
            &key.verifying_key().to_bytes()
        )]
    );
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
    let view = view_replacement(&second).unwrap();
    let decoded = read_commit(&reconstruct(&view, &bytes)).unwrap();
    assert_eq!(decoded.flakes, survivors(&original, &[1, 3, 12]));
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
