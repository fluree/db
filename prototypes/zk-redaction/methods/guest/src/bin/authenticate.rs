//! Proves a commit's claim: reads an original commit and one salt per
//! operation, authenticates it, and commits its CID, parents, signers and
//! the Merkle root over its salted operations.

#[path = "../common.rs"]
mod common;

use common::{read_frame, Cycles, ZkSha};
use risc0_zkvm::guest::env;
use fluree_db_redaction::{authenticate, Signatures};

fn main() {
    let input = read_frame();
    let salts = read_frame();
    let (ledger_id, signatures): (String, Signatures) = env::read();
    let mut probe = Cycles::start();
    let claim = authenticate::<ZkSha, _>(&input, &ledger_id, signatures, &salts, &mut probe)
        .unwrap_or_else(|e| panic!("authentication refused: {e}"));
    env::commit(&claim);
    probe.report();
}
