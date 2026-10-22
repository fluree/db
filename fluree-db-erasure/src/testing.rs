//! A native prover for tests of the erasure workflow, where no zkVM runs:
//! it runs the same transform and wraps the statement in a fake receipt,
//! which only [`Verifier::accepting_dev_receipts`] accepts.
//!
//! [`Verifier::accepting_dev_receipts`]: crate::Verifier::accepting_dev_receipts

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use fluree_db_redaction::protocol::{ProveRequest, ProveResponse};
use fluree_db_redaction::{redact_with, HostSha};
use risc0_zkvm::{FakeReceipt, InnerReceipt, MaybePruned, Receipt, ReceiptClaim};

use crate::{Prover, ProverError};

/// The image id fake receipts claim; no real guest has it.
pub const TEST_IMAGE_ID: [u32; 8] = [0x7e57_0001; 8];

/// Proves natively. Can be told to fail or refuse its next steps, to
/// exercise retries and failed jobs.
#[derive(Debug, Default)]
pub struct NativeProver {
    failures: AtomicUsize,
    refusals: AtomicUsize,
    proved: Mutex<Vec<ProveRequest>>,
}

impl NativeProver {
    /// Fail the next `n` steps as an unavailable prover would.
    pub fn fail_next(&self, n: usize) {
        self.failures.store(n, Ordering::SeqCst);
    }

    /// Refuse the next `n` steps as a prover refuses a commit it cannot
    /// prove.
    pub fn refuse_next(&self, n: usize) {
        self.refusals.store(n, Ordering::SeqCst);
    }

    /// Every request proved so far, in order.
    pub fn proved(&self) -> Vec<ProveRequest> {
        self.proved.lock().unwrap().clone()
    }
}

/// A fake receipt for `statement`, claiming [`TEST_IMAGE_ID`].
pub fn fake_receipt(statement: &fluree_db_redaction::Statement) -> Vec<u8> {
    let words = risc0_zkvm::serde::to_vec(statement).expect("statement encodes");
    let journal: Vec<u8> = words.iter().flat_map(|w| w.to_le_bytes()).collect();
    let claim = ReceiptClaim::ok(TEST_IMAGE_ID, MaybePruned::Value(journal.clone()));
    let receipt = Receipt::new(InnerReceipt::Fake(FakeReceipt::new(claim)), journal);
    bincode::serialize(&receipt).expect("receipt encodes")
}

#[async_trait::async_trait]
impl Prover for NativeProver {
    async fn prove(&self, request: &ProveRequest) -> Result<ProveResponse, ProverError> {
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(ProverError::Unavailable("told to fail".into()));
        }
        if self
            .refusals
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(ProverError::Refused("told to refuse".into()));
        }
        let (replacement, statement) = redact_with::<HostSha, _>(
            &request.input,
            &request.removed_slots,
            &request.ledger_id,
            request.signatures,
            &mut (),
        )
        .map_err(|e| ProverError::Refused(format!("redaction: {e}")))?;
        self.proved.lock().unwrap().push(request.clone());
        Ok(ProveResponse {
            replacement,
            receipt: fake_receipt(&statement),
            user_cycles: 0,
            segments: 0,
        })
    }
}
