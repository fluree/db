//! Proves one redaction step: reads the input commit (original or
//! replacement) and the slots to remove, runs the transform, and commits its
//! statement.

mod common;

use common::{read_frame, Cycles, ZkSha};
use risc0_zkvm::guest::env;
use zk_redaction_core::{redact_with, Signatures};

fn main() {
    let input = read_frame();
    let (removed_slots, ledger_id, signatures): (Vec<u32>, String, Signatures) = env::read();
    let mut probe = Cycles::start();
    let (_, statement) =
        redact_with::<ZkSha, _>(&input, &removed_slots, &ledger_id, signatures, &mut probe)
            .unwrap_or_else(|e| panic!("redaction refused: {e}"));
    env::commit(&statement);
    probe.report();
}
