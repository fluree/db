//! `fluree-prover`: proves one redaction step for Fluree.
//!
//! Reads a request frame on stdin (see `fluree_db_redaction::protocol`),
//! proves the redaction guest on it, and writes the response frame on stdout:
//! the replacement and a succinct receipt. Exit status 0 on success, 2 when
//! the request is refused (malformed, invalid slots, a signature that does
//! not verify), 1 when proving fails.
//!
//! ```text
//! fluree-prover              # one step, stdin to stdout
//! fluree-prover --image-id   # the guest image id this build proves, as hex words
//! ```

use std::io::{Read, Write};
use std::process::exit;

use fluree_db_redaction::protocol::{ProveRequest, ProveResponse};
use fluree_db_redaction::{commit_cid, redact_with, HostSha, Sha256, Statement};
use fluree_prover_methods::{REDACT_ELF, REDACT_ID};
use risc0_zkvm::{ExecutorEnv, LocalProver, Prover, ProverOpts};

fn refuse(message: impl std::fmt::Display) -> ! {
    eprintln!("refused: {message}");
    exit(2)
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("failed: {message}");
    exit(1)
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        None => {}
        Some("--image-id") => {
            let words: Vec<String> = REDACT_ID.iter().map(|w| format!("{w:#010x}")).collect();
            println!("{}", words.join(" "));
            return;
        }
        Some(_) => {
            eprintln!("usage: fluree-prover [--image-id]");
            exit(2)
        }
    }
    if std::env::var_os("RISC0_DEV_MODE").is_some() {
        refuse("RISC0_DEV_MODE is set; a dev-mode receipt proves nothing");
    }
    let mut frame = Vec::new();
    if let Err(e) = std::io::stdin().read_to_end(&mut frame) {
        fail(format!("reading the request: {e}"));
    }
    let request = ProveRequest::decode(&frame).unwrap_or_else(|e| refuse(e));
    // The replacement, computed here as the guest computes it; the proof's
    // statement must name it.
    let (replacement, expected) = redact_with::<HostSha, _>(
        &request.input,
        &request.removed_slots,
        &request.ledger_id,
        request.signatures,
        &mut (),
    )
    .unwrap_or_else(|e| refuse(format!("redaction: {e}")));

    // The guest's phase markers go here, not into the response.
    let mut guest_output = Vec::new();
    let env = {
        let mut builder = ExecutorEnv::builder();
        builder.write_frame(&request.input);
        builder
            .write(&(
                request.removed_slots.clone(),
                request.ledger_id.clone(),
                request.signatures,
            ))
            .unwrap_or_else(|e| fail(e));
        builder.stdout(&mut guest_output);
        builder.build().unwrap_or_else(|e| fail(e))
    };
    let info = LocalProver::new("local")
        .prove_with_opts(env, REDACT_ELF, &ProverOpts::succinct())
        .unwrap_or_else(|e| fail(format!("proving: {e}")));
    info.receipt
        .verify(REDACT_ID)
        .unwrap_or_else(|e| fail(format!("own receipt does not verify: {e}")));
    let statement: Statement = info
        .receipt
        .journal
        .decode()
        .unwrap_or_else(|e| fail(format!("journal: {e}")));
    if statement != expected || statement.output_cid != commit_cid(&HostSha::sha256(&replacement))
    {
        fail("the proven statement is not the predicted one");
    }
    let response = ProveResponse {
        replacement,
        receipt: bincode::serialize(&info.receipt).unwrap_or_else(|e| fail(e)),
        user_cycles: info.stats.user_cycles,
        segments: info.stats.segments as u32,
    };
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = stdout
        .write_all(&response.encode())
        .and_then(|()| stdout.flush())
    {
        fail(format!("writing the response: {e}"));
    }
}
