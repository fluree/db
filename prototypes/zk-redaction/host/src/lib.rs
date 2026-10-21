//! Host side of the redaction prototype: fixtures built with Fluree's own
//! commit writer, execution and proving of the guest, and the verifier.

use std::time::{Duration, Instant};

use fluree_db_core::commit::codec::write_commit;
use fluree_db_core::{Commit, ContentId, ContentKind, Flake, FlakeMeta, FlakeValue, Sid};
use methods::{REDACT_ELF, REDACT_ID};
use risc0_zkvm::{default_executor, default_prover, ExecutorEnv, ProverOpts, Receipt};
use serde::{Deserialize, Serialize};
use zk_redaction_core::{commit_cid, HostSha, Sha256, Statement, TRANSFORM_VERSION};

pub const LEDGER: &str = "acme/people:main";

/// A mix of the operation shapes commits carry: plain and language-tagged
/// strings, numbers, references, list members, a named graph.
pub fn flakes(n: usize, t: i64) -> Vec<Flake> {
    let mut out = Vec::with_capacity(n);
    let graph = Sid::new(15, "audit");
    for i in 0..n {
        let s = Sid::new(13, &format!("person-{}", i / 4));
        let flake = match i % 4 {
            0 => Flake::new(
                s,
                Sid::new(14, "email"),
                FlakeValue::String(format!("person{i}@example.org")),
                Sid::new(2, "string"),
                t,
                true,
                None,
            ),
            1 => Flake::new(
                s,
                Sid::new(14, "label"),
                FlakeValue::String(format!("Personne {i}")),
                Sid::new(3, "langString"),
                t,
                true,
                Some(FlakeMeta::with_lang("fr")),
            ),
            2 => Flake::new(
                s,
                Sid::new(14, "knows"),
                FlakeValue::Ref(Sid::new(13, &format!("person-{}", (i * 7) % (n / 4 + 1)))),
                Sid::new(1, "id"),
                t,
                true,
                None,
            ),
            _ => Flake::new_in_graph(
                graph.clone(),
                s,
                Sid::new(14, "score"),
                FlakeValue::Long(i as i64),
                Sid::new(2, "long"),
                t,
                i % 8 != 3,
                Some(FlakeMeta::with_index((i % 5) as i32)),
            ),
        };
        out.push(flake);
    }
    out
}

/// A commit blob: `n` operations, optionally compressed and signed, with a
/// parent and optionally a merge parent.
pub fn commit_blob(n: usize, compress: bool, signed: bool, merge: bool) -> Vec<u8> {
    let mut commit =
        Commit::new(42, flakes(n, 42)).with_parent(ContentId::new(ContentKind::Commit, b"p"));
    if merge {
        commit = commit.with_merge_parents(vec![ContentId::new(ContentKind::Commit, b"m")]);
    }
    let key = fluree_db_credential::SigningKey::from_bytes(&[9u8; 32]);
    let signing = signed.then_some((&key, LEDGER));
    write_commit(&commit, compress, signing).unwrap().bytes
}

fn env<'a>(
    input: &[u8],
    removed: &[u32],
    ledger: &str,
    stdout: &'a mut Vec<u8>,
) -> anyhow::Result<ExecutorEnv<'a>> {
    let mut builder = ExecutorEnv::builder();
    builder.write_frame(input);
    builder.write(&(removed.to_vec(), ledger.to_string()))?;
    builder.stdout(stdout);
    builder.build()
}

/// Cycles per phase, from the guest's markers.
fn phases(stdout: &[u8]) -> Vec<(String, u64)> {
    let mut marks: Vec<(String, u64)> = String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|l| {
            let mut parts = l.strip_prefix("phase ")?.split(' ');
            Some((parts.next()?.to_string(), parts.next()?.parse().ok()?))
        })
        .collect();
    let mut out = Vec::new();
    for w in marks.windows(2) {
        out.push((w[1].0.clone(), w[1].1 - w[0].1));
    }
    marks.clear();
    out
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Execution {
    pub statement: Statement,
    pub total_cycles: u64,
    pub user_cycles: u64,
    pub segments: usize,
    pub phases: Vec<(String, u64)>,
    pub execute: Duration,
}

/// Run the guest without proving: cycle counts and the statement it would
/// commit.
pub fn execute(input: &[u8], removed: &[u32], ledger: &str) -> Result<Execution, String> {
    let mut stdout = Vec::new();
    let env = env(input, removed, ledger, &mut stdout).map_err(|e| e.to_string())?;
    let started = Instant::now();
    let session = default_executor()
        .execute(env, REDACT_ELF)
        .map_err(|e| e.to_string())?;
    let execute = started.elapsed();
    let statement: Statement = session.journal.decode().map_err(|e| e.to_string())?;
    Ok(Execution {
        statement,
        total_cycles: session.segments.iter().map(|s| 1u64 << s.po2).sum(),
        user_cycles: session.cycles(),
        segments: session.segments.len(),
        phases: phases(&stdout),
        execute,
    })
}

pub struct Proof {
    pub receipt: Receipt,
    pub statement: Statement,
    pub prove: Duration,
    pub total_cycles: u64,
    pub segments: usize,
}

/// Prove the guest, compressed to a succinct receipt.
pub fn prove(input: &[u8], removed: &[u32], ledger: &str) -> Result<Proof, String> {
    let mut stdout = Vec::new();
    let env = env(input, removed, ledger, &mut stdout).map_err(|e| e.to_string())?;
    let started = Instant::now();
    let info = default_prover()
        .prove_with_opts(env, REDACT_ELF, &ProverOpts::succinct())
        .map_err(|e| e.to_string())?;
    let prove = started.elapsed();
    let statement: Statement = info.receipt.journal.decode().map_err(|e| e.to_string())?;
    Ok(Proof {
        statement,
        prove,
        total_cycles: info.stats.total_cycles,
        segments: info.stats.segments,
        receipt: info.receipt,
    })
}

/// What a verifier knows without the original bytes: the CIDs the ledger's
/// history and the redaction's authorization name, and the replacement
/// stored under the output CID.
#[derive(Clone, Debug)]
pub struct Expectation {
    /// The step's input: the original commit's CID (anchored in the ledger's
    /// history), or the previous replacement's.
    pub input_cid: Vec<u8>,
    pub original_cid: Vec<u8>,
    /// The slots the signed authorization names for this step.
    pub removed_slots: Vec<u32>,
}

/// Verify one step's receipt against the guest image and the expectation,
/// and the stored replacement against the statement's output CID.
pub fn verify_step(
    receipt: &Receipt,
    image_id: [u32; 8],
    replacement: &[u8],
    expect: &Expectation,
) -> Result<Statement, String> {
    let statement = checked_statement(
        receipt,
        image_id,
        &expect.input_cid,
        &expect.original_cid,
        &expect.removed_slots,
    )?;
    if statement.output_cid != commit_cid(&HostSha::sha256(replacement)) {
        return Err("the stored replacement is not the proven one".into());
    }
    Ok(statement)
}

/// Verify the redactions of one commit, in order, from its original to the
/// stored replacement: the first step reads the original the ledger's history
/// anchors, each later step reads the replacement the step before it
/// proved, each removes the slots its authorization names, and the last
/// produced `replacement`.
pub fn verify_chain(
    receipts: &[Receipt],
    image_id: [u32; 8],
    replacement: &[u8],
    original_cid: &[u8],
    authorized: &[Vec<u32>],
) -> Result<Vec<Statement>, String> {
    if receipts.is_empty() || receipts.len() != authorized.len() {
        return Err(format!(
            "{} proofs for {} authorized steps",
            receipts.len(),
            authorized.len()
        ));
    }
    let mut input = original_cid.to_vec();
    let mut redacted: Vec<u32> = Vec::new();
    let mut statements = Vec::with_capacity(receipts.len());
    for (step, (receipt, slots)) in receipts.iter().zip(authorized).enumerate() {
        let statement = checked_statement(receipt, image_id, &input, original_cid, slots)
            .map_err(|e| format!("step {}: {e}", step + 1))?;
        redacted.extend(slots);
        redacted.sort_unstable();
        if statement.redacted_slots != redacted {
            return Err(format!("step {}: other slots are redacted", step + 1));
        }
        input = statement.output_cid.clone();
        statements.push(statement);
    }
    if input != commit_cid(&HostSha::sha256(replacement)) {
        return Err("the stored replacement is not the proven one".into());
    }
    Ok(statements)
}

/// A receipt's statement, checked against the program and what one step
/// must have read and removed.
fn checked_statement(
    receipt: &Receipt,
    image_id: [u32; 8],
    input_cid: &[u8],
    original_cid: &[u8],
    removed_slots: &[u32],
) -> Result<Statement, String> {
    receipt
        .verify(image_id)
        .map_err(|e| format!("receipt does not verify for this program: {e}"))?;
    let statement: Statement = receipt
        .journal
        .decode()
        .map_err(|e| format!("journal is not a statement: {e}"))?;
    if statement.transform_version != TRANSFORM_VERSION {
        return Err(format!(
            "transform version {} is not {TRANSFORM_VERSION}",
            statement.transform_version
        ));
    }
    if statement.input_cid != input_cid {
        return Err("the proof is for another input".into());
    }
    if statement.original_cid != original_cid {
        return Err("the proof is for another original commit".into());
    }
    if statement.removed_slots != removed_slots {
        return Err("the proof removed other slots than authorized".into());
    }
    Ok(statement)
}

/// The guest image id this build proves with.
pub fn image_id() -> [u32; 8] {
    REDACT_ID
}

/// Peak resident memory of this process and of its finished children, KiB.
pub fn peak_rss_kib() -> (i64, i64) {
    let mut own: libc::rusage = unsafe { std::mem::zeroed() };
    let mut children: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut own);
        libc::getrusage(libc::RUSAGE_CHILDREN, &mut children);
    }
    (own.ru_maxrss, children.ru_maxrss)
}
