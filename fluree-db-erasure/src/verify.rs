//! Verifying a commit's redaction proofs, with none of its original bytes.

use fluree_db_redaction::{
    commit_cid, verify_exposed_signatures, HostSha, Sha256, Statement, TRANSFORM_VERSION,
};
use risc0_zkvm::{InnerReceipt, Receipt, VerifierContext};

/// Image ids of the redaction guest whose proofs this build accepts, built
/// reproducibly from `fluree-prover/`. A new guest adds its id here; an id
/// is removed only when no stored proof uses it.
pub const REDACT_IMAGE_IDS: &[[u32; 8]] = &[];

/// Why a proof was not accepted.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct VerifyError(String);

impl VerifyError {
    fn at(step: usize, e: VerifyError) -> Self {
        VerifyError(format!("step {}: {}", step + 1, e.0))
    }
}

fn reject(why: impl Into<String>) -> VerifyError {
    VerifyError(why.into())
}

/// Checks receipts against the pinned guest images.
pub struct Verifier {
    image_ids: Vec<[u32; 8]>,
    dev_receipts: bool,
}

impl Default for Verifier {
    fn default() -> Self {
        Verifier {
            image_ids: REDACT_IMAGE_IDS.to_vec(),
            dev_receipts: false,
        }
    }
}

impl Verifier {
    /// A verifier of receipts from these guest images only.
    pub fn with_image_ids(image_ids: Vec<[u32; 8]>) -> Self {
        Verifier {
            image_ids,
            dev_receipts: false,
        }
    }

    /// A verifier that accepts the fake receipts of [`crate::testing`]
    /// (and only for its test image).
    #[cfg(feature = "testing")]
    pub fn accepting_dev_receipts() -> Self {
        Self::accepting_dev_receipts_for(vec![crate::testing::TEST_IMAGE_ID])
    }

    /// [`Self::accepting_dev_receipts`], for other images.
    #[cfg(feature = "testing")]
    pub fn accepting_dev_receipts_for(image_ids: Vec<[u32; 8]>) -> Self {
        Verifier {
            image_ids,
            dev_receipts: true,
        }
    }

    /// Verify the redactions of one commit, in order, from the original
    /// the ledger's history anchors to the stored `replacement`: each step
    /// reads the previous step's output, removes the slots its authorization
    /// names, and the last produced `replacement`.
    pub fn verify_chain(
        &self,
        receipts: &[Vec<u8>],
        replacement: &[u8],
        original_cid: &[u8],
        ledger_id: &str,
        authorized: &[Vec<u32>],
    ) -> Result<Vec<Statement>, VerifyError> {
        if receipts.is_empty() || receipts.len() != authorized.len() {
            return Err(reject(format!(
                "{} proofs for {} authorized steps",
                receipts.len(),
                authorized.len()
            )));
        }
        let mut input = original_cid.to_vec();
        let mut redacted: Vec<u32> = Vec::new();
        let mut statements = Vec::with_capacity(receipts.len());
        for (step, (receipt, slots)) in receipts.iter().zip(authorized).enumerate() {
            let statement = self
                .step(receipt, &input, original_cid, ledger_id, slots)
                .map_err(|e| VerifyError::at(step, e))?;
            redacted.extend(slots);
            redacted.sort_unstable();
            if statement.redacted_slots != redacted {
                return Err(VerifyError::at(step, reject("other slots are redacted")));
            }
            input = statement.output_cid.clone();
            statements.push(statement);
        }
        if input != commit_cid(&HostSha::sha256(replacement)) {
            return Err(reject("the stored replacement is not the proven one"));
        }
        Ok(statements)
    }

    /// One step's statement, from a receipt of a pinned image, checked
    /// against what the step must have read and removed.
    fn step(
        &self,
        receipt: &[u8],
        input_cid: &[u8],
        original_cid: &[u8],
        ledger_id: &str,
        removed_slots: &[u32],
    ) -> Result<Statement, VerifyError> {
        let receipt: Receipt =
            bincode::deserialize(receipt).map_err(|e| reject(format!("not a receipt: {e}")))?;
        match &receipt.inner {
            InnerReceipt::Succinct(_) => {}
            InnerReceipt::Fake(_) if self.dev_receipts => {}
            _ => return Err(reject("not a succinct receipt")),
        }
        // Explicitly, whatever RISC0_DEV_MODE says.
        let ctx = VerifierContext::default().with_dev_mode(self.dev_receipts);
        if !self
            .image_ids
            .iter()
            .any(|id| receipt.verify_with_context(&ctx, *id).is_ok())
        {
            return Err(reject("the receipt does not verify for a known program"));
        }
        let statement: Statement = receipt
            .journal
            .decode()
            .map_err(|e| reject(format!("the journal is not a statement: {e}")))?;
        if statement.transform_version != TRANSFORM_VERSION {
            return Err(reject(format!(
                "transform version {}",
                statement.transform_version
            )));
        }
        if statement.input_cid != input_cid {
            return Err(reject("the proof is for another input"));
        }
        if statement.original_cid != original_cid {
            return Err(reject("the proof is for another original commit"));
        }
        if statement.removed_slots != removed_slots {
            return Err(reject("the proof removed other slots than authorized"));
        }
        if statement.ledger_id != ledger_id {
            return Err(reject("the proof is for another ledger"));
        }
        verify_exposed_signatures::<HostSha>(&statement)
            .map_err(|e| reject(format!("an exposed signature: {e}")))?;
        Ok(statement)
    }
}
