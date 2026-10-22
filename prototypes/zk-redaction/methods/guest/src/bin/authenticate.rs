//! Proves a commit's claim: reads an original commit, authenticates it, and
//! commits its CID, parents, signers and the Merkle root over its
//! operations.

#[path = "../common.rs"]
mod common;

use common::{read_frame, Cycles, ZkSha};
use risc0_zkvm::guest::env;
use zk_redaction_core::{authenticate, Signatures};

fn main() {
    let input = read_frame();
    let (ledger_id, signatures): (String, Signatures) = env::read();
    let mut probe = Cycles::start();
    let claim = authenticate::<ZkSha, _>(&input, &ledger_id, signatures, &mut probe)
        .unwrap_or_else(|e| panic!("authentication refused: {e}"));
    env::commit(&claim);
    probe.report();
}
