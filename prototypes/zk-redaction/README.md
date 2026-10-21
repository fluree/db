# ZK redaction prototype

A RISC Zero proof that a Fluree V4 commit blob was redacted correctly: selected operation slots
are removed and everything else is preserved. This is PR 4 of `DATA_ERASURE.md`, an isolated
feasibility experiment. It is excluded from the main workspace, so RISC Zero never enters a
Fluree build.

## What is proven

The guest (`methods/guest`) reads a commit blob, either an original V4 commit or a previous
replacement, and the slots to remove. It runs the transform in `core` and commits a `Statement`:

| Field | Meaning |
|---|---|
| `transform_version` | Version of the transform and of the replacement encoding (`FCR1`) |
| `input_cid`, `output_cid` | CIDs (SHA2-256, Fluree commit codec) of the blob read and the replacement written |
| `original_cid` | The original commit's CID; carried through every later redaction |
| `removed_slots`, `redacted_slots` | Slots removed by this step, and by every step so far |
| `op_count`, `t`, `ledger_id` | Original operation count, commit `t`, and the ledger the signatures were checked for |
| `signers` | `did:key` of every signature over the original body that the guest verified |
| `removed_terms` | Dictionary entries no surviving operation uses, and so dropped |

The transform keeps every surviving operation's bytes, order and slot number. It copies the
envelope (parents, merge parents, `t`, txn, metadata) byte for byte. It drops a dictionary
string only when no surviving operation refers to it. Ed25519 signatures over the original are
verified in the guest against the domain digest
`SHA256("fluree/commit/v1" ‖ varint(len) ‖ ledger ‖ SHA256(body))`; a signature that does not
verify fails the proof. The replacement is deterministic: the same input and slots always give
the same bytes.

A verifier (`host/src/bin/verify.rs`) needs none of the original bytes. It needs the receipts of
every erasure of the commit, in order; the guest image id; the stored replacement; and what the
ledger's history and the signed authorizations name: the original CID and each step's slots. It
checks each receipt against the image id and each statement against those expectations. The
first step must read the original CID, each later step the previous step's output, and the last
step's output must be the replacement's hash.

## Reproducing

```sh
# Transform tests, against Fluree's own commit writer and reader (any machine):
cargo test -p zk-redaction-core --features host-sha

# Guest, host and proofs (Linux x86-64; installs risc0 3.0.6):
curl -L https://risczero.com/install | bash && rzup install
cargo build --release -p host --bins
cargo test --release -p host            # rejection tests, proving included
target/release/host execute             # cycle counts for every scenario, no proofs
target/release/host prove medium        # writes medium.{receipt,replacement,expect.json}
target/release/verify medium            # fresh process, no original bytes
target/release/host prove second        # both erasures of one commit
target/release/verify second            # the chain, back to the original CID
```

`RISC0_SKIP_BUILD=1 cargo check -p host` checks the host without building the guest.

Scenarios (`host/src/main.rs`) build commits with Fluree's `write_commit`. Operations mix plain
and language-tagged strings, numbers, references, list indexes, a named graph, and retractions.

| Scenario | Operations | Compressed | Signed | Other |
|---|---:|---|---|---|
| `small` | 16 | no | no | |
| `small_signed` | 16 | yes | yes | |
| `merge` | 64 | yes | no | merge parent |
| `medium` | 1,000 | yes | no | |
| `medium_signed` | 1,000 | yes | yes | |
| `medium_10pct` | 1,000 | yes | no | 100 slots removed |
| `second` | 1,000 | n/a | no | a second erasure: proves `medium`'s first step, then redacts its replacement without the original |
| `large_10k` | 10,000 | yes | no | |
| `large_import` | 50,000 | yes | no | one slot removed |

## Coverage

`core/tests/redact.rs` (7 tests) decodes every replacement back through `fluree-db-core`'s
`read_commit` and compares it with the original minus the removed slots. The tests cover:

- compressed and uncompressed commits;
- a dictionary string used only by a removed operation is dropped, while a shared one is kept;
- retractions and merge parents are preserved;
- a signed commit is verified and its signer named;
- a second erasure from the replacement alone;
- invalid slots are refused;
- the replacement is deterministic.

`host/tests/reject.rs` proves and checks rejections:

- The guest refuses a tampered signed commit, a signature checked against another ledger, and
  slots the commit does not have.
- A receipt does not verify against another program, another input or original CID, slots
  other than those authorized, or a changed replacement (a changed survivor).
- A chain of two erasures verifies only back to the anchored original: not with the first proof
  missing, the steps reordered, a first proof of another original or of other slots, other
  authorized slots, or another final replacement.

All three pass on the benchmark host.

## Results

Hardware: AWS m7a.4xlarge (16 vCPU AMD EPYC 9R14, 61 GiB), Ubuntu 24.04, CPU proving only.
Software: risc0-zkvm 3.0.6 (rzup), Rust 1.97, succinct receipts (`ProverOpts::succinct()`).

### Cycles by phase (executor)

User cycles, from markers the guest writes. These are execution counts, not proof costs. Proof
cost follows the padded segments.

| Scenario | Input bytes | Ops | Removed | User cycles | Signatures | Decompress | Parse | Hash input | Redact | Hash output |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `small` | 587 | 16 | 1 | 77,091 | 468 | 690 | 19,460 | 1,741 | 20,911 | 1,731 |
| `small_signed` | 554 | 16 | 1 | 3,731,075 | 3,580,490 | 73,465 | 19,460 | 1,683 | 21,015 | 1,788 |
| `merge` | 815 | 64 | 1 | 333,253 | 468 | 146,793 | 73,154 | 1,980 | 74,203 | 3,228 |
| `medium` | 8,997 | 1,000 | 1 | 3,913,595 | 468 | 1,559,361 | 1,121,984 | 10,877 | 1,130,726 | 32,938 |
| `medium_signed` | 9,124 | 1,000 | 1 | 7,447,760 | 3,533,434 | 1,559,365 | 1,121,984 | 11,026 | 1,130,884 | 33,020 |
| `medium_10pct` | 8,997 | 1,000 | 100 | 3,934,635 | 468 | 1,559,361 | 1,121,984 | 10,877 | 1,117,329 | 29,815 |
| `second` | 29,257 | 1,000 | 2 | 1,552,957 | 0 | 0 | 304,655 | 32,964 | 1,126,078 | 32,890 |
| `large_10k` | 95,343 | 10,000 | 1 | 38,267,810 | 468 | 15,058,030 | 11,284,660 | 104,951 | 11,198,074 | 335,789 |
| `large_import` | 499,656 | 50,000 | 1 | 194,576,923 | 468 | 78,448,891 | 56,536,378 | 545,491 | 56,017,705 | 1,725,017 |

### Proofs

Prove time is wall clock for one proof on all 16 vCPUs; peak RSS is from `/usr/bin/time -v`.

| Scenario | Proved cycles | Segments | Prove time | Per segment | CPU | Peak RSS | Receipt | Replacement | Verified |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `small` | 131,072 | 1 | 21 s | 21 s | 1422% | 1.4 GiB | 224 KB | 606 B | yes, 15 ms |
| `small_signed` | 4,194,304 | 4 | 7.6 min | 114 s | 1471% | 9.2 GiB | 224 KB | 664 B | yes, 15 ms |
| `merge` | 524,288 | 1 | 58 s | 58 s | 1461% | 4.6 GiB | 224 KB | 1,915 B | yes, 15 ms |
| `medium` | 4,718,592 | 5 | 8.7 min | 105 s | 1470% | 9.2 GiB | 224 KB | 29,257 B | yes, 15 ms |
| `medium_signed` | 8,388,608 | 8 | 15.3 min | 115 s | 1471% | 9.2 GiB | 224 KB | 29,315 B | yes, 15 ms |
| `medium_10pct` | 4,718,592 | 5 | 8.7 min | 105 s | 1468% | 9.2 GiB | 226 KB | 26,384 B | yes, 15 ms |
| `second` | 2,097,152 | 2 | 3.7 min | 112 s | 1469% | 9.2 GiB | 224 KB | 29,211 B | yes, 30 ms |
| `large_10k` | 44,040,192 | 42 | 80.9 min | 116 s | 1470% | 9.2 GiB | 224 KB | 307,218 B | yes, 16 ms |

Every receipt verified in a fresh process with no original bytes, in about 15 ms per step.
`second` proves both erasures: its first step is `medium`'s (8.7 min), and its row is the
second step. Its two receipts verify together as a chain in 30 ms. Prover disk writes are
small: 1.4 MB for a 1,000-operation proof and 10.9 MB for the 10,000-operation one, receipt and
replacement included. The prover keeps segments in memory.

`large_import` was executed but not proved. Proving cost is linear in segments: 104–116 s
per full segment from 4 to 42 segments, with `large_10k`'s 42 segments at 116 s. At that rate
its 216 segments come to about 7 hours on this machine; this is an extrapolation, not a
measurement. Peak memory does not grow with commit size: segments are proved one at a time,
at about 9.2 GiB each.

### Concurrent proving

Commit latency of ordinary single-commit transactions on a file-backed ledger (the
`retention_perf` config probe, 2,000 commits per mode) on the same machine, alone and while a
`medium_10pct` proof runs with no resource limits:

| Mode | p50 alone | p99 alone | p50 during proof | p99 during proof |
|---|---:|---:|---:|---:|
| no config graph | 1.31 ms | 4.5 ms | 2.45 ms | 34.0 ms |
| config graph | 1.72 ms | 5.1 ms | 3.18 ms | 24.1 ms |
| no config graph, again | 1.33 ms | 4.5 ms | 10.51 ms | 35.0 ms |

An unrestricted prover takes nearly every core and degrades transactions 2–8× at p50 and 5–8×
at p99. Production proving needs its own workers, or at least CPU limits that leave the
transaction path its cores. Neither was measured here.

## Findings

- **Cost follows commit size, not the share removed.** About 3,900 user cycles per operation
  for a compressed commit from 1,000 to 50,000 operations; fixed costs raise the rate for the
  smallest commits. Removing 100 of 1,000 operations (`medium_10pct`) costs the same as
  removing one, so removals from one commit should be batched into one proof.
- **The codec dominates.** For `medium`, zstd decompression is 40% of user cycles, parsing 29%
  and the transform 29%. SHA-256 of input and output, with the accelerated circuit, is about 1%.
  These are guest user cycles, which exclude the padding and continuation overhead in proved
  cycles, and they are not a measured comparison with a Poseidon design. They do say that in
  this implementation, codec work, not the hash, is where proving cost can be cut.
- **Signatures are expensive.** Ed25519 verification in plain guest code costs about 3.5 M
  cycles per signature. That nearly doubles a signed 1,000-operation proof (15.3 vs 8.7 min).
  RISC Zero's accelerated curve25519 crates were not tried.
- **A second erasure is cheaper.** A replacement is stored uncompressed, so redacting it again
  skips decompression (3.7 min vs 8.7). The price is size: 29 KB against the original's 9 KB.
- **Verification is cheap and constant:** about 15 ms and a 224 KB receipt, whatever the
  commit size.
- **Not measured:** GPU proving, accelerated Ed25519, resource-limited proving next to a
  transactor, and the 50,000-operation proof itself.

## Gate assessment

The plan's gate is a capacity assessment against recorded target commit sizes, arrival rate,
resource budget and completion target. None of these is recorded by the product yet. So that
the numbers can be checked against any target, here is the arithmetic for one isolated
16-vCPU prover like this one, proving one commit at a time:

| Commit | Prove time | Proofs per prover-day |
|---|---:|---:|
| 1,000 operations, unsigned | 8.7 min | 165 |
| 1,000 operations, signed | 15.3 min | 94 |
| 10,000 operations, unsigned | 81 min | 17 |
| 50,000 operations, unsigned (extrapolated) | ~7 h | ~3 |

A removal needs one proof per commit holding an occurrence; a fact asserted once and retracted
once is two. Proofs of different commits are independent, so capacity scales with provers.
Assume a 30-day completion target, removals of facts in signed 1,000-operation commits at two
commits each, and one prover. That handles about 47 removals a day; below that rate a removal
completes in hours, not days. A removal touching a 50,000-operation import commit costs about
7 prover-hours by itself.

Verdict: proving per erasure request with SHA-256, as the plan proposes, is feasible at low
request rates (tens a day per prover), provided proving runs apart from transactions. The
gate is not passed: it needs recorded targets, and a transaction-isolation measurement under
those targets. PR 5 is not started.
