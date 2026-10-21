//! Proves one redaction step: reads the input commit (original or
//! replacement) and the slots to remove, runs the transform, and commits its
//! statement. Phase cycle counts go to stdout for the host's cost breakdown;
//! they are not part of the statement.

use risc0_zkvm::guest::env;
use risc0_zkvm::sha::{Impl, Sha256 as _};
use zk_redaction_core::{redact, Probe, Sha256};

struct ZkSha;

impl Sha256 for ZkSha {
    fn sha256(data: &[u8]) -> [u8; 32] {
        Impl::hash_bytes(data).as_bytes().try_into().unwrap()
    }
}

struct Cycles(Vec<(&'static str, u64)>);

impl Probe for Cycles {
    fn mark(&mut self, phase: &'static str) {
        self.0.push((phase, env::cycle_count()));
    }
}

/// The host's `write_frame`: a `u32` length, then the bytes. (The guest's
/// own `read_frame` is unstable API.)
fn read_frame() -> Vec<u8> {
    let mut len: u32 = 0;
    env::read_slice(core::slice::from_mut(&mut len));
    let mut bytes = vec![0u8; len as usize];
    env::read_slice(&mut bytes);
    bytes
}

fn main() {
    let input = read_frame();
    let (removed_slots, ledger_id): (Vec<u32>, String) = env::read();
    let mut probe = Cycles(vec![("start", env::cycle_count())]);
    let (_, statement) = redact::<ZkSha, _>(&input, &removed_slots, &ledger_id, &mut probe)
        .unwrap_or_else(|e| panic!("redaction refused: {e}"));
    env::commit(&statement);
    for (phase, cycles) in probe.0 {
        println!("phase {phase} {cycles}");
    }
}
