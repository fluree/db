//! Proof-verified commit erasure: the verifier for redaction proofs and the
//! runner for the prover that makes them.
//!
//! The prover (`fluree-prover/`, outside this workspace) runs the redaction
//! transform of `fluree-db-redaction` in the RISC Zero zkVM and returns a
//! replacement with a succinct receipt. Fluree verifies receipts here, with
//! RISC Zero's verifier only, against the guest images it pins.

mod prover;
#[cfg(feature = "testing")]
pub mod testing;
mod verify;

pub use fluree_db_redaction::protocol::{ProveRequest, ProveResponse};
pub use fluree_db_redaction::{Signatures, Statement};
pub use prover::{Prover, ProverConfig, ProverError, SubprocessProver};
pub use verify::{Verifier, VerifyError, REDACT_IMAGE_IDS};
