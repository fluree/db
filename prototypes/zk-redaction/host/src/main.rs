//! Benchmarks the redaction proof.
//!
//! ```text
//! host execute            # every scenario, executor only: cycles per phase
//! host prove <scenario>   # one scenario, proved: time, memory, proof size
//! ```
//!
//! `prove` writes `<scenario>.receipt`, `<scenario>.replacement` and
//! `<scenario>.expect.json` (plus `<scenario>.step1.receipt` for a second
//! erasure) for `verify`, which checks them with no original bytes. Run each
//! `prove` in its own process so peak memory is its own.

use std::time::Instant;

use host::{commit_blob, execute, peak_rss_kib, prove, verify_chain, LEDGER};
use serde_json::json;
use zk_redaction_core::{commit_cid, redact, HostSha, Sha256};

struct Scenario {
    name: &'static str,
    input: Vec<u8>,
    removed: Vec<u32>,
    /// For a second erasure: the original and the slots the first removed.
    first: Option<(Vec<u8>, Vec<u32>)>,
    original_cid: Vec<u8>,
    original_bytes: usize,
    op_count: usize,
}

fn scenario(name: &str) -> Scenario {
    let (input, removed, op_count): (Vec<u8>, Vec<u32>, usize) = match name {
        "small" => (commit_blob(16, false, false, false), vec![1], 16),
        "small_signed" => (commit_blob(16, true, true, false), vec![1], 16),
        "merge" => (commit_blob(64, true, false, true), vec![5], 64),
        "medium" => (commit_blob(1_000, true, false, false), vec![10], 1_000),
        "medium_signed" => (commit_blob(1_000, true, true, false), vec![10], 1_000),
        "medium_10pct" => (
            commit_blob(1_000, true, false, false),
            (0..1_000).step_by(10).collect(),
            1_000,
        ),
        "large_10k" => (commit_blob(10_000, true, false, false), vec![123], 10_000),
        "large_import" => (commit_blob(50_000, true, false, false), vec![123], 50_000),
        "second" => {
            // A second erasure, from the medium scenario's replacement alone.
            let original = commit_blob(1_000, true, false, false);
            let (first, _) = redact::<HostSha, _>(&original, &[10], LEDGER, &mut ()).unwrap();
            let original_cid = commit_cid(&HostSha::sha256(&original));
            return Scenario {
                name: "second",
                original_bytes: original.len(),
                input: first,
                removed: vec![11, 500],
                first: Some((original, vec![10])),
                original_cid,
                op_count: 1_000,
            };
        }
        other => panic!("unknown scenario {other}"),
    };
    Scenario {
        name: Box::leak(name.to_string().into_boxed_str()),
        original_cid: commit_cid(&HostSha::sha256(&input)),
        original_bytes: input.len(),
        input,
        removed,
        first: None,
        op_count,
    }
}

const ALL: [&str; 9] = [
    "small",
    "small_signed",
    "merge",
    "medium",
    "medium_signed",
    "medium_10pct",
    "second",
    "large_10k",
    "large_import",
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("execute") => {
            for name in ALL {
                let s = scenario(name);
                let run = execute(&s.input, &s.removed, LEDGER).expect("execute");
                println!(
                    "{}",
                    json!({
                        "scenario": s.name,
                        "mode": "execute",
                        "input_bytes": s.input.len(),
                        "original_bytes": s.original_bytes,
                        "op_count": s.op_count,
                        "removed": s.removed.len(),
                        "erased_fraction": s.removed.len() as f64 / s.op_count as f64,
                        "user_cycles": run.user_cycles,
                        "total_cycles": run.total_cycles,
                        "segments": run.segments,
                        "phases": run.phases,
                        "execute_ms": run.execute.as_secs_f64() * 1e3,
                        "removed_terms": run.statement.removed_terms,
                        "signers": run.statement.signers.len(),
                    })
                );
            }
        }
        Some("prove") => {
            let name = args.get(2).expect("scenario");
            let s = scenario(name);
            // A second erasure proves the first as well: the chain is
            // verified back to the original.
            let mut receipts = Vec::new();
            let mut authorized = Vec::new();
            let mut first_prove_s = None;
            if let Some((original, slots)) = &s.first {
                let first = prove(original, slots, LEDGER).expect("prove the first step");
                assert_eq!(
                    first.statement.output_cid,
                    commit_cid(&HostSha::sha256(&s.input)),
                    "the first step did not produce this step's input"
                );
                first_prove_s = Some(first.prove.as_secs_f64());
                std::fs::write(
                    format!("{name}.step1.receipt"),
                    bincode::serialize(&first.receipt).unwrap(),
                )
                .unwrap();
                receipts.push(first.receipt);
                authorized.push(slots.clone());
            }
            let (replacement, predicted) =
                redact::<HostSha, _>(&s.input, &s.removed, LEDGER, &mut ()).unwrap();
            let proof = prove(&s.input, &s.removed, LEDGER).expect("prove");
            assert_eq!(proof.statement, predicted, "the guest and host disagree");
            let receipt_bytes = bincode::serialize(&proof.receipt).unwrap();
            receipts.push(proof.receipt);
            authorized.push(s.removed.clone());
            let started = Instant::now();
            verify_chain(
                &receipts,
                host::image_id(),
                &replacement,
                &s.original_cid,
                &authorized,
            )
            .expect("verify");
            let verify_ms = started.elapsed().as_secs_f64() * 1e3;
            std::fs::write(format!("{name}.receipt"), &receipt_bytes).unwrap();
            std::fs::write(format!("{name}.replacement"), &replacement).unwrap();
            std::fs::write(
                format!("{name}.expect.json"),
                json!({
                    "original_cid": hex::encode(&s.original_cid),
                    "authorized": authorized,
                })
                .to_string(),
            )
            .unwrap();
            let (rss_self, rss_children) = peak_rss_kib();
            println!(
                "{}",
                json!({
                    "scenario": s.name,
                    "mode": "prove",
                    "input_bytes": s.input.len(),
                    "op_count": s.op_count,
                    "removed": s.removed.len(),
                    "replacement_bytes": replacement.len(),
                    "total_cycles": proof.total_cycles,
                    "segments": proof.segments,
                    "prove_s": proof.prove.as_secs_f64(),
                    "first_step_prove_s": first_prove_s,
                    "receipt_bytes": receipt_bytes.len(),
                    "verify_ms": verify_ms,
                    "peak_rss_self_kib": rss_self,
                    "peak_rss_children_kib": rss_children,
                    "risc0_zkvm": risc0_zkvm::VERSION,
                })
            );
        }
        _ => {
            eprintln!("usage: host execute | host prove <scenario>");
            std::process::exit(2);
        }
    }
}
