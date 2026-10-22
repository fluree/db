//! Verify a stored redaction proof with no original bytes: the receipts,
//! the stored replacement, and what the ledger's history and the
//! authorizations say (the original CID, the slots each step removes).
//!
//! ```text
//! verify <scenario>     # reads <scenario>.receipt (and .step<n>.receipt
//!                       # for earlier steps), .replacement, .expect.json
//! ```

use std::time::Instant;

use host::{image_id, verify_chain};

fn receipt(path: &str) -> risc0_zkvm::Receipt {
    bincode::deserialize(&std::fs::read(path).unwrap()).unwrap()
}

/// `verify disclose <scenario>`: one disclosed operation, against the
/// authentication receipt and the anchored commit CID.
fn verify_disclosed(name: &str) {
    let receipt = receipt(&format!("{name}.auth.receipt"));
    let file: serde_json::Value =
        serde_json::from_slice(&std::fs::read(format!("{name}.disclosure.json")).unwrap()).unwrap();
    let anchored = hex::decode(file["anchored_cid"].as_str().unwrap()).unwrap();
    let disclosure: host::Disclosure = serde_json::from_value(file["disclosure"].clone()).unwrap();
    let started = Instant::now();
    match host::verify_disclosure(&receipt, host::authenticate_image_id(), &anchored, &disclosure) {
        Ok((claim, signers)) => println!(
            "{{\"scenario\":\"{name}\",\"disclosed\":true,\"slot\":{},\"verify_ms\":{},\"parents\":{},\"signers\":{}}}",
            disclosure.slot,
            started.elapsed().as_secs_f64() * 1e3,
            claim.parents.len(),
            signers.len()
        ),
        Err(e) => {
            println!("{{\"scenario\":\"{name}\",\"disclosed\":false,\"error\":{e:?}}}");
            std::process::exit(1);
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next().expect("scenario");
    if first == "disclose" {
        return verify_disclosed(&args.next().expect("scenario"));
    }
    let name = first;
    let replacement = std::fs::read(format!("{name}.replacement")).unwrap();
    let expect: serde_json::Value =
        serde_json::from_slice(&std::fs::read(format!("{name}.expect.json")).unwrap()).unwrap();
    let original_cid = hex::decode(expect["original_cid"].as_str().unwrap()).unwrap();
    let authorized: Vec<Vec<u32>> = serde_json::from_value(expect["authorized"].clone()).unwrap();
    let mut receipts: Vec<_> = (1..authorized.len())
        .map(|step| receipt(&format!("{name}.step{step}.receipt")))
        .collect();
    receipts.push(receipt(&format!("{name}.receipt")));
    let started = Instant::now();
    match verify_chain(
        &receipts,
        image_id(),
        &replacement,
        &original_cid,
        &authorized,
    ) {
        Ok(statements) => {
            let last = statements.last().unwrap();
            println!(
                "{{\"scenario\":\"{name}\",\"verified\":true,\"steps\":{},\"verify_ms\":{},\"redacted_slots\":{},\"signers\":{}}}",
                statements.len(),
                started.elapsed().as_secs_f64() * 1e3,
                last.redacted_slots.len(),
                // Verified in the guest, or exposed and verified here.
                statements[0].signers.len().max(statements[0].signatures.len())
            )
        }
        Err(e) => {
            println!(
                "{{\"scenario\":\"{name}\",\"verified\":false,\"error\":{:?}}}",
                e
            );
            std::process::exit(1);
        }
    }
}
