# Initial durable journal PR handoff

Proposed title: **Add an experimental durable local journal and harden filesystem Raft recovery**

The ordinary filesystem transaction path performs several durable file publications
per commit. This branch adds a bounded, opt-in journal acceptance path for a single
local ledger and preserves asynchronous indexing. Filesystem Raft keeps its existing
replicated authority and durable shared payloads; separate fixes address restart log
boundaries, idle worker loss, and concurrent submission identity/ownership.

## Scope and review order

1. **Durability foundation:** `fluree-db-core/src/local_journal/`, guarded root
   ownership, framed records and flush receipts, recovery fences, immutable
   checkpoints, and exclusive offline checkpoint retirement. Inspect the failure
   tests alongside each implementation, including interrupted and failed flushes.
2. **Database/server integration:** `fluree-db-api/src/local_journal_ledger/` and
   `local_journal_acceptance.rs`, `FlureeBuilder::build_local_journal`, and the
   feature-gated server bridge. Source dependencies become durable before ACK;
   index building and adoption remain background work and can use separate storage.
3. **Shared correctness fixes:** generated node identity reservation, incremental
   cardinalities, namespace joins, Cypher relationship decoding, and cached
   nameservice commit metadata. These were needed for exact before/after query and
   commit validation and also apply outside journal mode.
4. **Raft hardening:** `fluree-raft-core/src/storage/fs*`, peer activity/liveness,
   nonce-bearing queued envelopes, identity-aware waiter fanout, and envelope
   ownership through internal retries. No Raft command/snapshot encoding replacement
   or extra WAL is introduced. See [filesystem Raft design](../design/filesystem-raft-durability.md).
5. **Diagnostics and regression coverage:** opt-in phase timings, bounded benchmark
   fixtures, exact receipt/raw-content recovery oracles, and deployment-mode checks.

The branch preserves its incremental commits, including explicit reversions of the
index-publication experiment. Review the final diff: transaction acknowledgment does
not wait for index construction, publication, or adoption. No history rewrite is
required to understand or retain the current implementation.

## Supported boundary

| Mode | Behavior in this PR | Qualification limit |
| --- | --- | --- |
| Ordinary standalone filesystem | Existing durable path plus shared correctness fixes | No default switch to journal mode |
| Experimental local journal | One unsigned default-graph ledger; JSON-LD/Cypher; ordinary HTTP bridge; background indexing | Explicit feature/runtime opt-in; bounded 64 MiB journal; exclusive offline checkpointing |
| Filesystem Raft | Shared durable envelope/commit/raw payloads; separate local voter logs; quorum-ordered acceptance | Existing file-log backend retained; diagnostic NFS deployment tested |
| Raft plus active local journal | Rejected at startup | Compiling both features does not authorize two acceptance authorities |
| S3/external storage with local journal | Unsupported and rejected through configuration/backend guards | No S3 performance claim |

The journal does not support general lifecycle/configuration operations, credentials,
policy context, or multi-ledger/cluster acceptance. Transaction preparation overlap,
group flush, a new Raft log backend, and shared payload packing are separate work.

## Evidence and measurements

Companion evidence lives in `benchmark-db/runs/durable-cypher-20260907/`:

- `journal-phase3-http-20260908/REPORT.md`: matched standalone eight-write comparison,
  **21.503 → 5.441 ms** aggregate with fsync and raw recording enabled. This is the
  measured earlier source, not a fresh timing of the final cleanup commits.
- `journal-phase4-medium-checkpoint/REPORT.md`: repeated offline rotation and restart
  checks, with increasing history/recovery cost. Peak recovery memory of about
  **4.59 GB** missed the proposed 4 GB screen.
- `journal-phase4-raft-filesystem-qualification/REPORT.md`: source `4c81ebc82`,
  **288/288** timed requests at concurrency 1/4/16, exact receipt correlation,
  worker loss, leader failover, and full restart. The six-write diagnostic mix
  yielded **7.787 / 18.119 / 17.572 requests/s** on the tested NFSv4.2 rig.
  Initial failures are retained with the fixes and rerun evidence.

These are separate experiments. The standalone speedup is not a Raft speedup, the
six-write Raft diagnostic is not the official Cypher suite, and historical
competitor or pre-v4.2 results are not a matched comparison for this candidate.

Final PR regression results and source hashes are recorded in the companion
`journal-pr-wrap/REPORT.md`. The Linux checks use Rust 1.97.0 and the repository's
CI environment (`CARGO_INCREMENTAL=0`, dev/test debug info disabled, mold linker):

```sh
cargo fmt --all -- --check
cargo clippy --locked --all --all-features --all-targets -- -D warnings
cargo nextest run --locked --workspace --all-features --no-fail-fast
cargo check --locked --workspace --all-targets
```

Docker is required for the all-feature LocalStack tests. The main workspace checks
do not include the separately managed SPARQL/SHACL compliance and SQL bridge
workspaces. Normal PR CI remains the merge gate for those separate jobs.

## Remaining qualification, not implemented optimization

- Online journal rotation, sustained retention/queue behavior, and bounded recovery
  memory remain open. Keep the journal experimental and disabled by default.
- Deterministic I/O-loss models, process interruption, and restart tests do not
  establish physical power-loss durability. Qualify the actual Linux/block/local
  device and shared filesystem, including ENOSPC, failed flushes and storage outage;
  macOS requires its own storage qualification.
- Raft protects replicated decisions, not the loss of the only shared payload
  volume. Production share availability/durability remains a deployment obligation.
- Cancelled or fatally interrupted proposals may have committed. Preserve their
  envelopes; possible orphans require a future safe collector. Failed cleanup calls
  can also retain objects. Do not promise bounded orphan reclamation in this PR.
- Transport activity detects an unreachable worker node; it does not detect a hung
  application worker whose Raft transport still answers. Custom integrations need
  to wire the documented `PeerActivity` observer.
- Production-share multi-ledger throughput and sustained snapshot/catch-up behavior
  need a matched deployment run. Local multi-ledger ownership tests are correctness
  coverage, not a substitute for that performance qualification.
