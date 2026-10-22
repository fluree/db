//! Host side of the redaction prototype: fixtures built with Fluree's own
//! commit writer, execution and proving of the guest, and the verifier.

use std::time::{Duration, Instant};

use fluree_db_core::commit::codec::write_commit;
use fluree_db_core::{Commit, ContentId, ContentKind, Flake, FlakeMeta, FlakeValue, Sid};
use methods::{AUTHENTICATE_ELF, AUTHENTICATE_ID, CHAIN_ELF, CHAIN_ID, REDACT_ELF, REDACT_ID};
use risc0_zkvm::{default_executor, default_prover, ExecutorEnv, ProverOpts, Receipt};
use serde::{Deserialize, Serialize};
use zk_redaction_core::{
    audit_path, check_exposed, commit_cid, leaf_hash, leaf_salt, leaf_salts, leaves,
    root_from_path, verify_exposed_signatures, ChainClaim, CommitClaim, HostSha, Sha256,
    Signatures, Statement, CLAIM_VERSION, SALT_LEN, TRANSFORM_VERSION,
};

pub const LEDGER: &str = "acme/people:main";

/// A mix of the operation shapes commits carry: plain and language-tagged
/// strings, numbers, references, list members, a named graph.
pub fn flakes(n: usize, t: i64) -> Vec<Flake> {
    let mut out = Vec::with_capacity(n);
    let graph = Sid::new(15, "audit");
    for i in 0..n {
        let s = Sid::new(13, format!("person-{}", i / 4));
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
                FlakeValue::Ref(Sid::new(13, format!("person-{}", (i * 7) % (n / 4 + 1)))),
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
    signatures: Signatures,
    stdout: &'a mut Vec<u8>,
) -> anyhow::Result<ExecutorEnv<'a>> {
    let mut builder = ExecutorEnv::builder();
    builder.write_frame(input);
    builder.write(&(removed.to_vec(), ledger.to_string(), signatures))?;
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
    execute_with(input, removed, ledger, Signatures::Verify)
}

/// [`execute`], with the original's signatures verified or exposed.
pub fn execute_with(
    input: &[u8],
    removed: &[u32],
    ledger: &str,
    signatures: Signatures,
) -> Result<Execution, String> {
    let mut stdout = Vec::new();
    let env = env(input, removed, ledger, signatures, &mut stdout).map_err(|e| e.to_string())?;
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
    prove_with(input, removed, ledger, Signatures::Verify)
}

/// [`prove`], with the original's signatures verified or exposed.
pub fn prove_with(
    input: &[u8],
    removed: &[u32],
    ledger: &str,
    signatures: Signatures,
) -> Result<Proof, String> {
    let mut stdout = Vec::new();
    let env = env(input, removed, ledger, signatures, &mut stdout).map_err(|e| e.to_string())?;
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
    // Signatures the proof exposed instead of verifying are verified here.
    verify_exposed_signatures::<HostSha>(&statement)
        .map_err(|e| format!("an exposed signature: {e}"))?;
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

// ---------------------------------------------------------------------------
// Commit authentication and selective disclosure
// ---------------------------------------------------------------------------

/// A fresh secret seed for a commit's leaf salts. Its holder keeps it to
/// disclose operations later; it is never handed over.
pub fn new_seed() -> [u8; 32] {
    let mut seed = [0u8; 32];
    getrandom::getrandom(&mut seed).expect("system randomness");
    seed
}

fn auth_env<'a>(
    input: &[u8],
    seed: &[u8; 32],
    ledger: &str,
    signatures: Signatures,
    stdout: &'a mut Vec<u8>,
) -> anyhow::Result<ExecutorEnv<'a>> {
    let n = leaves(input).map_err(|e| anyhow::anyhow!("{e}"))?.len();
    let mut builder = ExecutorEnv::builder();
    builder.write_frame(input);
    builder.write_frame(&leaf_salts::<HostSha>(seed, n));
    builder.write(&(ledger.to_string(), signatures))?;
    builder.stdout(stdout);
    builder.build()
}

/// An authentication run's claim and cycle counts.
pub struct AuthExecution {
    pub claim: CommitClaim,
    pub user_cycles: u64,
    pub total_cycles: u64,
    pub segments: usize,
    pub phases: Vec<(String, u64)>,
}

/// Run the authentication guest without proving: cycle counts by phase.
pub fn execute_authenticate(
    input: &[u8],
    seed: &[u8; 32],
    ledger: &str,
    signatures: Signatures,
) -> Result<AuthExecution, String> {
    let mut stdout = Vec::new();
    let env = auth_env(input, seed, ledger, signatures, &mut stdout).map_err(|e| e.to_string())?;
    let session = default_executor()
        .execute(env, AUTHENTICATE_ELF)
        .map_err(|e| e.to_string())?;
    let claim: CommitClaim = session.journal.decode().map_err(|e| e.to_string())?;
    Ok(AuthExecution {
        claim,
        user_cycles: session.cycles(),
        total_cycles: session.segments.iter().map(|s| 1u64 << s.po2).sum(),
        segments: session.segments.len(),
        phases: phases(&stdout),
    })
}

/// Prove a commit's claim, its leaves salted from `seed`, compressed to a
/// succinct receipt.
pub fn prove_authenticate(
    input: &[u8],
    seed: &[u8; 32],
    ledger: &str,
    signatures: Signatures,
) -> Result<(Receipt, CommitClaim, Duration, u64, usize), String> {
    let mut stdout = Vec::new();
    let env = auth_env(input, seed, ledger, signatures, &mut stdout).map_err(|e| e.to_string())?;
    let started = Instant::now();
    let info = default_prover()
        .prove_with_opts(env, AUTHENTICATE_ELF, &ProverOpts::succinct())
        .map_err(|e| e.to_string())?;
    let prove = started.elapsed();
    let claim: CommitClaim = info.receipt.journal.decode().map_err(|e| e.to_string())?;
    Ok((
        info.receipt,
        claim,
        prove,
        info.stats.total_cycles,
        info.stats.segments,
    ))
}

/// One operation of an authenticated commit, disclosed: its leaf, its salt
/// and the path of hashes to the claim's root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Disclosure {
    pub slot: u32,
    pub salt: [u8; SALT_LEN],
    pub leaf: Vec<u8>,
    pub path: Vec<[u8; 32]>,
}

/// Disclose operation `slot` of `original`, authenticated with `seed`: what
/// its holder hands over.
pub fn disclose(original: &[u8], seed: &[u8; 32], slot: u32) -> Result<Disclosure, String> {
    let leaves = leaves(original).map_err(|e| e.to_string())?;
    let hashes: Vec<[u8; 32]> = leaves
        .iter()
        .enumerate()
        .map(|(i, l)| leaf_hash::<HostSha>(&leaf_salt::<HostSha>(seed, i as u32), l))
        .collect();
    let leaf = leaves
        .get(slot as usize)
        .ok_or("no such operation")?
        .clone();
    Ok(Disclosure {
        slot,
        salt: leaf_salt::<HostSha>(seed, slot),
        leaf,
        path: audit_path::<HostSha>(slot as usize, &hashes),
    })
}

/// Verify a disclosed operation against an authentication receipt and the
/// commit CID the ledger's history anchors: the receipt is this program's,
/// its claim names that commit, the leaf is for the slot it says, and its
/// path leads to the claim's root. Returns the claim and the signers,
/// verified in the proof or here.
pub fn verify_disclosure(
    receipt: &Receipt,
    image_id: [u32; 8],
    anchored_cid: &[u8],
    disclosure: &Disclosure,
) -> Result<(CommitClaim, Vec<String>), String> {
    receipt
        .verify(image_id)
        .map_err(|e| format!("receipt does not verify for this program: {e}"))?;
    let claim: CommitClaim = receipt
        .journal
        .decode()
        .map_err(|e| format!("journal is not a claim: {e}"))?;
    if claim.claim_version != CLAIM_VERSION {
        return Err(format!("claim version {}", claim.claim_version));
    }
    if claim.cid != anchored_cid {
        return Err("the claim is for another commit".into());
    }
    if disclosure.leaf.get(..4) != Some(&disclosure.slot.to_le_bytes()[..]) {
        return Err("the leaf is for another slot".into());
    }
    let root = root_from_path::<HostSha>(
        disclosure.slot as usize,
        claim.op_count as usize,
        leaf_hash::<HostSha>(&disclosure.salt, &disclosure.leaf),
        &disclosure.path,
    );
    if root.as_ref().map(|r| r.as_slice()) != Some(claim.ops_root.as_slice()) {
        return Err("the operation is not in the commit".into());
    }
    let mut signers = claim.signers.clone();
    signers.extend(
        check_exposed::<HostSha>(&claim.ledger_id, &claim.body_hash, &claim.signatures)
            .map_err(|e| format!("an exposed signature: {e}"))?,
    );
    Ok((claim, signers))
}

/// The authentication guest's image id.
pub fn authenticate_image_id() -> [u32; 8] {
    AUTHENTICATE_ID
}

// ---------------------------------------------------------------------------
// Ancestry
// ---------------------------------------------------------------------------

/// `count` commits, each the first parent of the next, of `ops` operations.
pub fn commit_chain(count: usize, ops: usize) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(count);
    for i in 0..count {
        let t = i as i64 + 1;
        let mut commit = Commit::new(t, flakes(ops, t));
        if let Some(prev) = out.last() {
            commit = commit.with_parent(ContentId::new(ContentKind::Commit, prev));
        }
        out.push(write_commit(&commit, true, None).unwrap().bytes);
    }
    out
}

/// Run a chain's first link without proving: user cycles, padded cycles and
/// segments. A later link adds only the previous receipt's verification.
pub fn execute_chain_link(commit: &[u8]) -> Result<(u64, u64, usize), String> {
    let mut builder = ExecutorEnv::builder();
    builder.write_frame(commit);
    builder
        .write(&(None::<ChainClaim>, CHAIN_ID))
        .map_err(|e| e.to_string())?;
    let env = builder.build().map_err(|e| e.to_string())?;
    let session = default_executor()
        .execute(env, CHAIN_ELF)
        .map_err(|e| e.to_string())?;
    Ok((
        session.cycles(),
        session.segments.iter().map(|s| 1u64 << s.po2).sum(),
        session.segments.len(),
    ))
}

/// One proved link: its receipt, claim and proving time.
pub struct Link {
    pub receipt: Receipt,
    pub claim: ChainClaim,
    pub prove: Duration,
}

/// Prove `commits`' ancestry link by link, oldest first: each link proves
/// its commit and resolves the previous link's receipt as an assumption, so
/// the last receipt alone covers the whole chain.
pub fn prove_chain(commits: &[Vec<u8>]) -> Result<Vec<Link>, String> {
    let mut links: Vec<Link> = Vec::with_capacity(commits.len());
    for commit in commits {
        let prev = links.last();
        let mut builder = ExecutorEnv::builder();
        builder.write_frame(commit);
        builder
            .write(&(prev.map(|l| l.claim.clone()), CHAIN_ID))
            .map_err(|e| e.to_string())?;
        if let Some(prev) = prev {
            builder.add_assumption(prev.receipt.clone());
        }
        let env = builder.build().map_err(|e| e.to_string())?;
        let started = Instant::now();
        let info = default_prover()
            .prove_with_opts(env, CHAIN_ELF, &ProverOpts::succinct())
            .map_err(|e| e.to_string())?;
        let prove = started.elapsed();
        let claim: ChainClaim = info.receipt.journal.decode().map_err(|e| e.to_string())?;
        links.push(Link {
            receipt: info.receipt,
            claim,
            prove,
        });
    }
    Ok(links)
}

/// Verify a chain receipt: this program's, from `base` to `head` (the CIDs
/// the verifier holds), `length` commits.
pub fn verify_chain_claim(
    receipt: &Receipt,
    image_id: [u32; 8],
    base: &[u8],
    head: &[u8],
) -> Result<ChainClaim, String> {
    receipt
        .verify(image_id)
        .map_err(|e| format!("receipt does not verify for this program: {e}"))?;
    let claim: ChainClaim = receipt
        .journal
        .decode()
        .map_err(|e| format!("journal is not a chain claim: {e}"))?;
    if claim.image_id != image_id {
        return Err("the chain was linked by another program".into());
    }
    if claim.base_cid != base || claim.head_cid != head {
        return Err("the chain runs between other commits".into());
    }
    Ok(claim)
}

/// The chain guest's image id.
pub fn chain_image_id() -> [u32; 8] {
    CHAIN_ID
}
