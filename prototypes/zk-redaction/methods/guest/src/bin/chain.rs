//! Proves one link of a commit's ancestry: hashes the commit, reads its
//! first parent, and verifies the proof of the link before it (a proof by
//! this same program) as an assumption the prover resolves.

#[path = "../common.rs"]
mod common;

use common::{read_frame, ZkSha};
use risc0_zkvm::guest::env;
use zk_redaction_core::{chain_link, ChainClaim};

fn main() {
    let commit = read_frame();
    let (prev, image_id): (Option<ChainClaim>, [u32; 8]) = env::read();
    if let Some(prev) = &prev {
        env::verify(image_id, &risc0_zkvm::serde::to_vec(prev).unwrap())
            .expect("the previous link verifies");
    }
    let claim = chain_link::<ZkSha>(&commit, prev.as_ref(), image_id)
        .unwrap_or_else(|e| panic!("link refused: {e}"));
    env::commit(&claim);
}
