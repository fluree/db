//! A receipt the real prover made, checked by the production verifier.
//!
//! Every other test here uses fake receipts, and the prover workflow only
//! checks the image id its build records. This one holds a real succinct
//! receipt, so a verifier that no longer accepts what the shipped prover
//! makes (a `risc0` upgrade on one side only) fails here, in milliseconds,
//! instead of at a ledger's first erasure.
//!
//! The fixture is one step erasing one slot of a signed 16-operation
//! commit, asked to verify its signature inside the proof. A new guest
//! image (a new entry in [`REDACT_IMAGE_IDS`]) makes it again: run the
//! ignored [`write_the_fixture`] with `FLUREE_TEST_PROVER` naming a
//! `fluree-prover` built from this tree.

use std::path::PathBuf;

use fluree_db_erasure::{Signatures, Verifier, REDACT_IMAGE_IDS};
use fluree_db_redaction::{commit_cid, HostSha, Sha256};

const LEDGER: &str = "acme/people:main";
const SLOTS: &[u32] = &[3];

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/real-receipt")
        .join(name)
}

fn read(name: &str) -> Vec<u8> {
    std::fs::read(fixture(name)).unwrap_or_else(|e| panic!("fixture {name}: {e}"))
}

/// The fixture's original, replacement and receipt.
fn proved() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    (read("original"), read("replacement"), read("receipt"))
}

fn verify(
    replacement: &[u8],
    original: &[u8],
    ledger: &str,
    slots: &[u32],
    receipt: &[u8],
) -> Result<Vec<fluree_db_erasure::Statement>, fluree_db_erasure::VerifyError> {
    let cid = commit_cid(&HostSha::sha256(original));
    Verifier::default().verify_chain(
        &[receipt.to_vec()],
        replacement,
        &cid,
        ledger,
        &[slots.to_vec()],
        &[Signatures::Verify],
    )
}

#[test]
fn a_real_receipt_verifies_with_the_production_verifier() {
    let (original, replacement, receipt) = proved();
    let statements = verify(&replacement, &original, LEDGER, SLOTS, &receipt)
        .unwrap_or_else(|e| panic!("the shipped prover's receipt is refused: {e}"));
    assert_eq!(statements[0].redacted_slots, SLOTS);
    assert!(
        statements[0].signatures.is_empty(),
        "verified inside the proof, not exposed"
    );
}

/// The real receipt proves exactly its step: not another ledger's, not
/// other slots, not a changed replacement, not another original.
#[test]
fn a_real_receipt_proves_only_its_own_step() {
    let (original, replacement, receipt) = proved();
    assert!(verify(&replacement, &original, "acme/other:main", SLOTS, &receipt).is_err());
    assert!(verify(&replacement, &original, LEDGER, &[4], &receipt).is_err());
    let mut changed = replacement.clone();
    *changed.last_mut().unwrap() ^= 1;
    assert!(verify(&changed, &original, LEDGER, SLOTS, &receipt).is_err());
    let mut other = original.clone();
    *other.last_mut().unwrap() ^= 1;
    assert!(verify(&replacement, &other, LEDGER, SLOTS, &receipt).is_err());
    // Nor under an image this build does not pin.
    assert!(Verifier::with_image_ids(vec![[1; 8]])
        .verify_chain(
            &[receipt],
            &replacement,
            &commit_cid(&HostSha::sha256(&original)),
            LEDGER,
            &[SLOTS.to_vec()],
            &[Signatures::Verify],
        )
        .is_err());
    assert!(!REDACT_IMAGE_IDS.is_empty());
}

/// Make the fixture with the real prover.
#[cfg(unix)]
#[tokio::test]
#[ignore = "needs a built fluree-prover (FLUREE_TEST_PROVER); writes the fixture"]
async fn write_the_fixture() {
    use fluree_db_core::commit::codec::write_commit;
    use fluree_db_core::{Commit, ContentId, ContentKind, Flake, FlakeValue, Sid};
    use fluree_db_erasure::{ProveRequest, Prover, ProverConfig, SubprocessProver};

    let command = std::env::var_os("FLUREE_TEST_PROVER").expect("FLUREE_TEST_PROVER");
    let flakes = (0..16)
        .map(|i| {
            Flake::new(
                Sid::new(13, format!("person-{i}")),
                Sid::new(14, "email"),
                FlakeValue::String(format!("p{i}@secret.example")),
                Sid::new(2, "string"),
                3,
                true,
                None,
            )
        })
        .collect();
    let commit = Commit::new(3, flakes).with_parent(ContentId::new(ContentKind::Commit, b"parent"));
    let key = fluree_db_credential::SigningKey::from_bytes(&[7u8; 32]);
    let original = write_commit(&commit, true, Some((&key, LEDGER)))
        .unwrap()
        .bytes;
    let scratch = tempfile::tempdir().unwrap();
    let prover = SubprocessProver::new(ProverConfig {
        command: command.into(),
        scratch_dir: scratch.path().to_path_buf(),
        threads: None,
        niceness: 0,
        timeout: std::time::Duration::from_secs(3_600),
    });
    let response = prover
        .prove(&ProveRequest {
            input: original.clone(),
            removed_slots: SLOTS.to_vec(),
            ledger_id: LEDGER.into(),
            signatures: Signatures::Verify,
        })
        .await
        .expect("prove");
    std::fs::create_dir_all(fixture("")).unwrap();
    std::fs::write(fixture("original"), &original).unwrap();
    std::fs::write(fixture("replacement"), &response.replacement).unwrap();
    std::fs::write(fixture("receipt"), &response.receipt).unwrap();
    verify(
        &response.replacement,
        &original,
        LEDGER,
        SLOTS,
        &response.receipt,
    )
    .expect("the fresh receipt verifies");
}
