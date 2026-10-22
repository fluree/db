# ZK redaction prototype

RISC Zero proofs about Fluree V4 commit blobs, built for PR 4 of `DATA_ERASURE.md` as an isolated
feasibility experiment. The prototype is excluded from the main workspace, so RISC Zero never
enters a Fluree build.

It proves four separate claims, each one a separate guest program or mode:

| Claim | Program | Public result |
|---|---|---|
| A commit was redacted correctly | `methods/guest` (`main.rs`) | `Statement` |
| A commit is what its CID names, with a Merkle root over its salted operations | `authenticate` | `CommitClaim` |
| One operation is in a commit | none; checked against a `CommitClaim` | a leaf, its salt and its Merkle path |
| A commit descends from another along first parents | `chain` | `ChainClaim` |

A claim that a fact held at a given `t` is not built. It would need a proof that no commit
between the assertion and `t` retracted the fact. A wall-clock claim would also need a
trustworthy time anchor: a timestamp inside a commit proves only what was recorded.

## What is proven

### Redaction

The guest reads a commit blob, either an original V4 commit or a previous replacement, and the
slots to remove. It runs the transform in `core` and commits a `Statement`:

| Field | Meaning |
|---|---|
| `transform_version` | Version of the transform and of the replacement encoding (`FCR1`) |
| `input_cid`, `output_cid` | CIDs (SHA2-256, Fluree commit codec) of the blob read and the replacement written |
| `original_cid` | The original commit's CID, carried through every later redaction |
| `removed_slots`, `redacted_slots` | Slots removed by this step, and by every step so far |
| `op_count`, `t`, `ledger_id` | Original operation count, commit `t`, and the ledger the signatures are for |
| `signers` | `did:key` of every signature over the original body that the guest verified |
| `signatures`, `body_hash` | Instead of `signers`, the signatures and the body hash they sign, for the verifier to check (see below) |
| `removed_terms` | Dictionary entries no surviving operation uses, and so dropped |

The transform keeps every surviving operation's bytes, order and slot number. It copies the
envelope (parents, merge parents, `t`, txn, metadata) byte for byte, and drops a dictionary
string only when no surviving operation refers to it. The replacement is deterministic: the same
input and slots always give the same bytes.

Ed25519 signatures over the original sign the domain digest
`SHA256("fluree/commit/v1" ‖ varint(len) ‖ ledger ‖ SHA256(body))`. The guest handles them in
one of two modes:

- **`Signatures::Verify`**: the guest verifies them, and a signature that does not verify fails
  the proof.
- **`Signatures::Expose`**: the guest commits the signatures and `SHA256(body)`. The proof binds
  both to the original CID, and the verifier checks the signatures natively, in microseconds.

The two modes differ in what they reveal, not only in cost:

- **Signed commits.** The CID covers the signatures as well as the body. A verifier without the
  signatures cannot reproduce the CID, so it cannot test guesses at the removed values against
  it. `Expose` hands over the signatures and the body hash. Anyone who can then rebuild every
  other byte of the body can test candidate values for what was removed. That matters when a
  removed value has few possible values, such as a flag or a small enumeration.
- **Rebuilding the body.** It means reproducing the compressed operations byte for byte, which
  takes the same encoder. That makes guessing harder but does not prevent it.
- **Choosing the mode.** `Expose` is a choice made per proof, trading this for cost, and nothing
  selects it by default.
- **Unsigned commits.** The two modes coincide. The original CID, which every verifier holds,
  already allows the same test. `DATA_ERASURE.md` records this as a limit of the approach: ZK
  "does not remove information already exposed by an original digest".

A verifier (`host/src/bin/verify.rs`) needs none of the original bytes. It needs:

- the receipts of every erasure of the commit, in order;
- the guest image id;
- the stored replacement;
- what the ledger's history and the signed authorizations name: the original CID and each
  step's slots.

It checks each receipt against the image id and each statement against those expectations. The
first step must read the original CID, each later step the previous step's output, and the last
step's output must be the replacement's hash.

### Commit authentication and selective disclosure

`authenticate` reads an original commit and commits a `CommitClaim`:

- its CID, `t` and parent CIDs, read from the envelope;
- its signers, verified or exposed as above;
- an RFC 9162 Merkle tree hash over one salted leaf per operation, in slot order.

A leaf is the slot number, the operation's encoded bytes and every dictionary entry it refers
to, so a disclosed leaf reads on its own; resolving namespace codes to IRIs needs the ledger's
namespace table.

How a leaf is hashed:

- The hash is RFC 9162's over the salt and the leaf: `SHA256(0x00 ‖ salt ‖ leaf)`, with 32 bytes
  of salt per leaf.
- The holder derives the salts from a secret seed it keeps, `SHA256("fluree/leaf-salt" ‖ seed ‖
  slot)`, and passes them to the guest.
- The guest does not check how the salts were made. They only hide the leaves; the hash is what
  binds the root to the commit's operations.

One proof per commit then serves any number of disclosures. The holder of the commit hands over
the receipt, one leaf, its salt and its audit path. The verifier (`verify_disclosure`, or
`verify disclose`) checks:

- the receipt is the `authenticate` program's;
- the claim names the CID the verifier anchors;
- the leaf is for the slot it says;
- the salted leaf's path leads to the claim's root.

What a disclosure reveals:

- **Other leaves stay hidden.** The path's hashes cover leaves whose salts the recipient does not
  hold, so it cannot test guesses at them. Without salts, a recipient could hash each candidate
  for a predictable neighbor and find the match on the path. Salts serve the same purpose in
  SD-JWT (RFC 9901).
- **What is still revealed.** The leaf's position, and, through the claim, the operation count,
  `t`, parents and signers.
- **The CID is not salted.** It is a hash over the whole commit, and so is the body hash under
  `Expose`. Anyone who knows every other byte of the commit can test guesses against it, as
  under redaction.
- **The seed must stay secret.** A leaked seed makes the leaves as guessable as unsalted ones.
- **Keeping the seed is production work.** `host authenticate` draws a fresh seed, writes one
  disclosure and discards the seed. A reusable proof needs the seed, or the salts, stored
  securely for as long as disclosures are wanted.
- **Erasure reaches the salts.** An erased operation's leaf hash can sit on paths already handed
  out, and its salt would make it testable again. Salts stored one per slot can be deleted with
  their operation. A seed can only be deleted whole, which ends new disclosures from that proof.

A disclosure, once handed over, remains evidence after a later erasure of that operation:
erasure cannot revoke a proof package someone already holds.

### Ancestry

`chain` proves one link at a time. Each link hashes a commit, reads its first parent from the
envelope (no operation is decoded), and verifies the previous link's receipt as an assumption
the prover resolves (RISC Zero composition). Its `ChainClaim` names the base and head CIDs, the
length, and the program's own image id, which every link must share. The last receipt alone
covers the whole chain.

## Reproducing

```sh
# Core tests, against Fluree's own commit writer and reader (any machine):
cargo test -p zk-redaction-core --features host-sha
cargo test -p zk-redaction-core --features host-sha,c-zstd

# Guests, host and proofs (Linux x86-64; installs risc0 3.0.6):
curl -L https://risczero.com/install | bash && rzup install
cargo build --release -p host --bins
cargo test --release -p host              # rejection tests, proving included
target/release/host execute               # redaction cycle counts, every scenario
target/release/host execute-auth          # authentication cycle counts
target/release/host prove medium          # writes medium.{receipt,replacement,expect.json}
target/release/verify medium              # fresh process, no original bytes
target/release/host prove second          # both erasures of one commit
target/release/verify second              # the chain, back to the original CID
target/release/host authenticate medium 1 # claim, and a disclosure of slot 1 (seed not kept)
target/release/verify disclose medium
target/release/host execute-chain         # a chain link's cycle counts, by commit size
target/release/host chain 4 1000          # four linked 1,000-operation commits
```

`RISC0_SKIP_BUILD=1 cargo check -p host` checks the host without building the guests.

The guests decompress with the reference C zstd decoder (the core's `c-zstd` feature, on by
default in `methods/guest`), linked against a C allocator over the guest's own. The host
predicts every statement with `ruzstd`, and `host prove` checks that the two agree.

Scenarios (`host/src/main.rs`) build commits with Fluree's `write_commit`. Operations mix plain
and language-tagged strings, numbers, references, list indexes, a named graph, and retractions.

| Scenario | Operations | Compressed | Signed | Other |
|---|---:|---|---|---|
| `small` | 16 | no | no | |
| `small_signed`, `small_signed_exposed` | 16 | yes | yes | signatures verified, or exposed |
| `merge` | 64 | yes | no | merge parent |
| `medium` | 1,000 | yes | no | |
| `medium_signed`, `medium_signed_exposed` | 1,000 | yes | yes | signatures verified, or exposed |
| `medium_10pct` | 1,000 | yes | no | 100 slots removed |
| `second` | 1,000 | n/a | no | a second erasure: proves `medium`'s first step, then redacts its replacement without the original |
| `large_10k` | 10,000 | yes | no | |
| `large_import` | 50,000 | yes | no | one slot removed |

## Coverage

`core/tests/redact.rs` (8 tests) decodes every replacement back through `fluree-db-core`'s
`read_commit` and compares it with the original minus the removed slots. The tests cover:

- compressed and uncompressed commits;
- a dictionary string used only by a removed operation is dropped, while a shared one is kept;
- retractions and merge parents are preserved;
- a signed commit is verified and its signer named;
- exposed signatures verify outside the transform, and fail when forged, rebound to another body
  or attributed to another ledger;
- a second erasure from the replacement alone;
- invalid slots are refused;
- the replacement is deterministic.

`core/tests/authenticate.rs` (5 tests):

- a claim names the commit's CID and parents as Fluree reads them, including merge parents and
  transaction metadata;
- every operation's salted leaf reaches the root by its path, and a changed leaf, another
  position, another leaf's path or another salt does not;
- each leaf has its own salt: a disclosed salt opens no other leaf, a leaf without its salt
  matches nothing, the root changes with the seed, and anything but one salt per operation is
  refused;
- every position of every tree size from 1 to 33 has a path to the root;
- a chain link follows only its previous head and its own program.

`host/tests/reject.rs` proves and checks rejections:

- The guest refuses a tampered signed commit, a signature checked against another ledger, and
  slots the commit does not have.
- A receipt does not verify against another program, another input or original CID, slots
  other than those authorized, or a changed replacement (a changed survivor).
- A chain of two erasures verifies only back to the anchored original: not with the first proof
  missing, the steps reordered, a first proof of another original or of other slots, other
  authorized slots, or another final replacement.
- A disclosure verifies only against its commit: not against another program or CID, with a
  changed operation, an operation claimed for another slot, another slot's path, another slot's
  salt, or a salt from another seed.
- A chain proof covers its whole ancestry and nothing else: not another program or other end
  commits, and a commit that does not follow the chain cannot be linked onto it.

All five pass on the benchmark host.

## Results

Hardware: AWS m7a.4xlarge (16 vCPU AMD EPYC 9R14, 61 GiB), Ubuntu 24.04, CPU proving only.
Software: risc0-zkvm 3.0.6 (rzup), Rust 1.97, succinct receipts (`ProverOpts::succinct()`).

Proving cost follows the padded segments, at 104–116 s per full segment (2^20 cycles) on this
machine, linear from 2 to 42 segments. A one-segment proof takes 20–60 s, by its padded size.
Peak memory is about
9.2 GiB per full segment and does not grow with commit size. Every receipt is about 224 KB and
verifies in about 15 ms in a fresh process.

### Redaction, current build

User cycles by phase, from markers the guest writes. These are execution counts, not proof costs.

| Scenario | Input bytes | User cycles | Segments | Signatures | Decompress | Parse | Hash in | Redact | Hash out |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `small` | 587 | 48,666 | 1 | 747 | 105 | 6,886 | 1,750 | 5,968 | 1,736 |
| `small_signed` | 554 | 1,005,480 | 2 | 924,806 | 31,816 | 6,886 | 1,692 | 6,067 | 1,793 |
| `small_signed_exposed` | 554 | 92,917 | 1 | 2,568 | 31,816 | 6,886 | 1,692 | 6,067 | 1,793 |
| `merge` | 815 | 167,978 | 1 | 747 | 92,562 | 21,154 | 1,989 | 16,298 | 3,233 |
| `medium` | 8,997 | 1,718,304 | 2 | 747 | 1,118,299 | 306,333 | 10,886 | 208,694 | 32,943 |
| `medium_signed` | 9,124 | 2,637,453 | 3 | 918,749 | 1,118,299 | 306,333 | 11,035 | 208,855 | 33,025 |
| `medium_signed_exposed` | 9,124 | 1,740,537 | 2 | 12,015 | 1,118,299 | 306,333 | 11,035 | 208,855 | 33,025 |
| `medium_10pct` | 8,997 | 1,746,856 | 2 | 747 | 1,118,299 | 307,620 | 10,886 | 201,119 | 29,820 |
| `second` | 29,257 | 683,355 | 1 | 0 | 0 | 360,289 | 32,973 | 217,780 | 32,895 |
| `large_10k` | 95,343 | 16,569,293 | 19 | 747 | 10,768,297 | 3,099,043 | 104,960 | 2,139,960 | 335,794 |
| `large_import` | 499,656 | 82,123,710 | 90 | 747 | 53,262,173 | 15,559,043 | 545,500 | 10,553,688 | 1,725,022 |

`medium_signed`, with the guest verifying the signature, proved in 5.7 minutes (3 segments),
against 3.8 minutes (2 segments) for `medium`; `medium_signed_exposed` has `medium`'s 2 segments.

`large_import` proved in 2.9 hours: 90 segments at 117 s each, 9.2 GiB peak, and 25 MB of disk
writes, its replacement included. Its receipt verified in 16 ms in a fresh process. The
replacement is 1.6 MB against the original's 500 KB, since replacements store operations
uncompressed.

### What changed since the first measurement

Each step was measured on top of the ones before it, with segments and, where proved, proof
time:

| Step | `small_signed` | `medium` | `medium_signed` | `large_import` |
|---|---|---|---|---|
| First measurement | 3.73 M, 4 seg, 7.6 min | 3.91 M, 5 seg, 8.7 min | 7.45 M, 8 seg, 15.3 min | 194.6 M, 216 seg |
| Accelerated curve25519 | 1.07 M, 2 seg, 2.5 min | unchanged | 4.83 M, 6 seg, 10.7 min | unchanged |
| Signatures exposed | 0.18 M, 1 seg, 33 s | unchanged | 3.95 M, 5 seg, 8.7 min | unchanged |
| One parsing pass, no copies | 0.15 M, 1 seg, 33 s (exposed) | 2.16 M, 3 seg, 4.8 min | 2.19 M, 3 seg, 4.8 min (exposed) | 107.3 M, 122 seg |
| C zstd decoder | 0.09 M, 1 seg (exposed) | 1.72 M, 2 seg, 3.8 min | 1.74 M, 2 seg (exposed) | 82.1 M, 90 seg |

- **Accelerated curve25519:** RISC Zero's patched `curve25519-dalek` and `crypto-bigint`. One
  signature check drops from 3.58 M to 0.92 M cycles.
- **Signatures exposed:** the check leaves the proof entirely; what remains is reading the
  signature block and hashing the body, about 12,000 cycles for 1,000 operations.
- **One parsing pass:** dictionaries are read first. Each operation is parsed once, marking the
  dictionary entries survivors and removed operations use. Slots borrow the input instead of
  copying it. Parsing drops from 1.12 M to 0.31 M cycles, and the transform from 1.13 M to
  0.21 M.
- **C zstd decoder:** decompression drops from 1.56 M to 1.12 M cycles.

`large_10k` was also proved after the parsing pass, at 24 segments: 45 minutes, from 81.

### Authentication, disclosure and ancestry, current build

| Scenario | User cycles | Segments | Decompress | Parse | Leaf hashes | Tree |
|---|---:|---:|---:|---:|---:|---:|
| `small` | 82,659 | 1 | 480 | 6,768 | 25,847 | 13,431 |
| `merge` | 310,172 | 1 | 92,933 | 21,037 | 103,692 | 55,863 |
| `medium` | 3,988,907 | 5 | 1,118,670 | 306,216 | 1,631,513 | 883,287 |
| `large_10k` | 39,346,626 | 43 | 10,768,668 | 3,098,928 | 16,462,043 | 8,839,287 |
| `large_import` | 195,957,562 | 215 | 53,262,544 | 15,558,928 | 82,163,950 | 44,199,287 |

Salting the leaves added 180 cycles per leaf: `medium` rose from 3.81 M to 3.99 M cycles and
stayed at 5 segments; `large_10k` went from 42 segments to 43, `large_import` from 206 to 215.

- **Authentication**, with salted leaves: proving `medium` took 8.7 minutes (5 segments), and
  the 16-operation commit 33 s.
- **Disclosure:** a 1,000-operation commit's disclosure is a 58-byte leaf, its 32-byte salt and
  a 10-hash path. It verifies, with the receipt, in 15 ms.
- **Ancestry:** a link decodes no operations, but it hashes its whole commit. Its cost is about
  22,000 cycles plus 1.2 per commit byte:

| Commit | Bytes | User cycles | Padded cycles |
|---|---:|---:|---:|
| 16 operations | 387 | 22,416 | 65,536 |
| 1,000 operations | 8,957 | 32,555 | 131,072 |
| 10,000 operations | 95,303 | 134,541 | 262,144 |
| 50,000 operations | 499,616 | 612,081 | 1,048,576 |

Proved link times:

- **16 and 1,000 operations:** 15–21 s for the first commit and about 30 s for each later one,
  resolving the previous link's receipt included. Four links took under 2 minutes.
- **50,000 operations (500 KB):** two links took 109 s and 118 s, a full segment each. A larger
  commit needs more segments.

Every final chain receipt is 224 KB and verifies in 15 ms.

### Concurrent proving

Commit latency of ordinary single-commit transactions on a file-backed ledger (the
`retention_perf` config probe, 2,000 commits per mode) on the same machine, alone and while a
`medium_10pct` proof runs with no resource limits (first-measurement build):

| Mode | p50 alone | p99 alone | p50 during proof | p99 during proof |
|---|---:|---:|---:|---:|
| no config graph | 1.31 ms | 4.5 ms | 2.45 ms | 34.0 ms |
| config graph | 1.72 ms | 5.1 ms | 3.18 ms | 24.1 ms |
| no config graph, again | 1.33 ms | 4.5 ms | 10.51 ms | 35.0 ms |

An unrestricted prover takes nearly every core and degrades transactions 2–8× at p50 and 5–8×
at p99. Production proving needs its own workers, or at least CPU limits that leave the
transaction path its cores. Neither was measured here.

### First measurement

The first build verified signatures in plain guest code, parsed each operation twice with
per-operation copies, and decompressed with `ruzstd`.

| Scenario | User cycles | Proved cycles | Segments | Prove time | Peak RSS |
|---|---:|---:|---:|---:|---:|
| `small` | 77,091 | 131,072 | 1 | 21 s | 1.4 GiB |
| `small_signed` | 3,731,075 | 4,194,304 | 4 | 7.6 min | 9.2 GiB |
| `merge` | 333,253 | 524,288 | 1 | 58 s | 4.6 GiB |
| `medium` | 3,913,595 | 4,718,592 | 5 | 8.7 min | 9.2 GiB |
| `medium_signed` | 7,447,760 | 8,388,608 | 8 | 15.3 min | 9.2 GiB |
| `medium_10pct` | 3,934,635 | 4,718,592 | 5 | 8.7 min | 9.2 GiB |
| `second` | 1,552,957 | 2,097,152 | 2 | 3.7 min | 9.2 GiB |
| `large_10k` | 38,267,810 | 44,040,192 | 42 | 80.9 min | 9.2 GiB |

## Findings

- **Cost follows commit size, not the share removed.** Removing 100 of 1,000 operations
  (`medium_10pct`) costs the same as removing one, so removals from one commit should be
  batched into one proof.
- **The codec still dominates.** For `medium`, decompression is now 65% of user cycles, parsing
  18% and the transform 12%. SHA-256 of input and output is under 3%. These are guest user
  cycles, which exclude the padding and continuation overhead in proved cycles. They are not a
  measured comparison with a Poseidon design, but they say that in this implementation codec
  work, not the hash, is where the cost is.
- **Signatures need not be in the proof, at a price in privacy.** Exposing them leaves a signed
  1,000-operation commit at an unsigned one's 2 segments, against 3 segments (5.7 minutes) with
  the guest verifying them. The statement then carries the signatures and the body hash, which
  let anyone able to rebuild the rest of the body test guesses at removed values (see
  Redaction).
- **A second erasure is cheaper.** A replacement is stored uncompressed, so redacting it again
  skips decompression. The price is size: 29 KB against the original's 9 KB.
- **Authentication costs more than redaction** for the same commit, because of the Merkle tree:
  about 1,630 cycles per salted leaf and 880 per tree node, mostly the SHA-256 precompile's
  per-call cost. Leaves of several operations, or a wider tree, would cut the calls at the cost of
  larger disclosures; not measured.
- **Shared work:** authentication and redaction both decompress and parse the commit. Proving
  them in one run, or making a redaction proof start from an authenticated commit, would share
  that work; not built.
- **A chain link's cost grows with its commit's size.** A link hashes the whole commit, at about
  1.2 cycles per byte over a fixed 22,000. A link took about 30 s for commits up to 9 KB and
  just under 2 minutes for 500 KB. Long histories would want periodic checkpoints rather than
  one chain from genesis; not built.
- **Not measured:** GPU proving, resource-limited proving next to a transactor.

## Gate assessment

The plan's gate is a capacity assessment against recorded target commit sizes, arrival rate,
resource budget and completion target. None of these is recorded by the product yet. So that
the numbers can be checked against any target, these are the service times of one isolated
16-vCPU prover like this one, proving one commit at a time, with the current build:

| Proof | Service time | Per prover-day |
|---|---:|---:|
| Redaction, 1,000 operations, unsigned, or signed with signatures exposed | 3.8 min | about 380 |
| Redaction, 1,000 operations, signed, signatures verified in the proof | 5.7 min | about 250 |
| Redaction, 10,000 operations | about 35 min (19 segments; not proved in this build) | about 40 |
| Redaction, 50,000 operations | 2.9 h | about 8 |
| Authentication, 1,000 operations, salted leaves | 8.7 min | about 165 |
| Ancestry, per link, commits up to 9 KB | about 30 s | about 2,900 |
| Ancestry, per link, 500 KB commit | about 2 min | about 750 |

These are service times, not completion guarantees. A removal needs one proof per commit
holding an occurrence; a fact asserted once and retracted once is two. Proofs of different
commits are independent, so capacity scales with provers.

With removals in 1,000-operation commits at two commits each, one prover's theoretical ceiling
is about 190 removals a day, at which it is fully busy. It is about 125 when the commits are
signed and the proof verifies their signatures. Both figures count redaction proofs only:
authentication and ancestry proofs are separate and optional. How long a removal waits then depends on bursts, backlog, retries and how
many commits each request touches, none of which this prototype models.

Verdict: proving per erasure request with SHA-256, batching each commit's removals into one
proof, is feasible as a background operation, provided proving runs apart from transactions.
The gate is not passed. It needs recorded targets — the commit-size and signature mix, commits
touched per request, arrival bursts, a completion percentile and a resource budget — and a
measurement of the isolated deployment against them. PR 5 is not started.
