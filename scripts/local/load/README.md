# fluree-load

HTTP load harness for Fluree single-node and Raft cluster deployments.

Same tool against either backend — the only thing that changes is the
URL list. Designed to exercise the parts of the Raft path the in-process
bench suite doesn't touch: real wire-level latency, per-branch work
queues, idempotency-cache behavior, and rendezvous-hash ownership
recalculation under chaos.

## Running

Built and dispatched by `./stack load …` against a live local
deployment. The wrapper auto-populates `--addrs` from the running
compose file:

```bash
cd scripts/local
./stack up                              # bring up monolithic deployment
./stack load --workload single-pound --duration 30s

# Or against a raft cluster
./stack up --mode raft
./stack load --workload wide-fanout --duration 60s --concurrency 64
./stack load --tool-help                # full --help from the binary itself
```

You can also build and run it standalone — useful for pointing at
remote deployments or anything fronted by an LB:

```bash
cd scripts/local/load
cargo build --release
./target/release/fluree-load \
    --addrs http://prod-lb.example.com \
    --workload multitenant \
    --duration 5m \
    --concurrency 128
```

## Workloads

Each workload composes operations differently. See `--tool-help` for
the per-workload tuning knobs.

| Workload | What it does | What it exercises |
|---|---|---|
| `single-pound` | One `CreateLedger` at t=0, then transact-only | Baseline single-queue ceiling |
| `create-only` | Pure `CreateLedger` stream | `Command::CreateLedger` apply throughput in isolation |
| `transact-only` | Transact against `--seeded-ledger` names; fails if missing | Pre-seeded steady-state, no creates competing |
| `query-only` | Query against `--seeded-ledger` names; fails if missing | Local read path (no consensus), snapshot / cache-refresh behavior, read availability during chaos |
| `wide-fanout` | Creates N ledgers over the run; transacts to whichever have landed | Per-branch work queues, ownership recalc under failure, state machine growth |
| `multitenant` | Continuous mix: 1 in N ops is a `CreateLedger`, rest transact | Multi-tenant onboarding behavior, ledger-count scaling |

### query-only body shape

Every `query-only` request sends the same bounded triple-scan against
the picked ledger:

```json
{
  "select": ["?s"],
  "where": {"@id": "?s", "http://load.fluree/idx": "?idx"},
  "limit": 10
}
```

It targets the predicate the transact workloads write, so ledgers that
were populated by a prior `transact-only` / `single-pound` /
`wide-fanout` / `multitenant` run return real bindings; fresh ledgers
return an empty result set (still 200 OK, still exercises the query
path). Query is held stable across the run so cache warmth is honest;
per-request cursor variation is a follow-up once we know whether
tail-latency measurement wants it.

Typical two-run pattern:

```bash
# 1. Seed a ledger with data
./stack load --workload single-pound --duration 30s

# 2. Query it (use the ledger name single-pound printed in the top-ledgers
#    section of the summary — the "load-<ULID>-0" one)
./stack load --workload query-only --seeded-ledger load-<ULID>-0 --duration 30s
```

## Per-op outcome classes

The reporter buckets every response into one of these. The first two
count as the request having landed durably; the rest are failure modes
worth distinguishing during chaos:

| Outcome | Meaning |
|---|---|
| `success` | 2xx, no idempotency-cache marker |
| `idempotency-hit` | 2xx, replicated cache short-circuited the propose |
| `leader-change` | 503 with a leader hint; harness retried against the next URL |
| `overloaded` | 503/429 from the in-flight admission cap |
| `timeout` | HTTP-level timeout (connect + send + read) |
| `network-error` | Connection refused/reset/aborted before a response |
| `client-error` | Other 4xx (malformed request, missing ledger, key collision) |
| `server-error` | Other 5xx |

## Live progress + final summary

The progress line shows total ops, last-tick TPS, the "landed" rate
(success + idempotency-hit), and aggregate p50/p99. The final summary
breaks the same percentiles out per op kind and lists the outcome
counts so you can see why p99 spiked: leader churn vs admission cap
vs timeout looks very different.

```
     t       total         tps         ok%         p50         p99
   1.0s         312       311.7      100.00%       8.4ms      18.3ms
   2.0s         625       313.2      100.00%       8.7ms      19.1ms
   ...

─── Summary ───
elapsed: 30.00s   total: 9384   ledgers landed: 1   overall tps: 312

op             count  landed%        p50        p95        p99      p99.9        max
create-ledger      1  100.00%     12.4ms     12.4ms     12.4ms     12.4ms     12.4ms
transact        9383  100.00%      8.6ms     14.2ms     19.0ms     31.7ms     48.2ms

─── Outcomes ───
  create-ledger
    success                  1
  transact
    success               9383
```

## `--watch-cluster`

Optionally polls `/cluster/status` on a target URL (typically a raft
listener at the `9091`+ port) and prints annotation lines when the
cluster's leader, term, or voter set changes. On voter-set changes it
also reports — locally via a rendezvous-hash mirror of
`fluree-db-consensus/src/raft/ownership.rs` — how many of the
currently-known ledger main-branch owners would reassign.

```
[watch-cluster t= 12.3s] leader change: Some(1) → Some(3) (term 2 → 3)
[watch-cluster t= 18.7s] voter set change: [1, 2, 3] → [2, 3] — 14/47 known ledger main-branch owners reassigned
```

Pair with `stack kill <N>` mid-run to see the chaos-correlated latency
spikes in the progress stream and the consensus events inline.

## Targeting either backend

```bash
# Single-node Fluree
fluree-load --addrs http://localhost:8090 --workload single-pound

# Raft cluster (any URL works — leader-aware routing handles the rest)
fluree-load --addrs http://localhost:8091,http://localhost:8092,http://localhost:8093

# Behind a load balancer
fluree-load --addrs https://fluree.internal --workload multitenant
```

## What's intentionally not here

Things that came up during design and were deliberately left out of
this first cut:

- **Open-loop (rate-paced) dispatch.** Closed-loop with N concurrent
  workers measures the saturation curve. Open-loop is a different
  control regime (rate scheduling, coordinated omission handling) and
  adds enough complexity to warrant its own follow-up.
- **Per-branch metrics.** Branches are the queue dimension, but at
  high N branch counts the per-branch view is mostly noise. Per-ledger
  is the right first granularity; the summary surfaces top-10 ledgers
  by op count.
- **Workload migration mid-run.** "Shift hot ledger from A to B at
  t=30s." Useful for cache-tail tests but real engineering.
- **Per-request idempotency keys.** The harness intentionally issues
  anonymous submissions so each request goes through propose → apply
  rather than collapsing on the in-process moka cache. Adding a
  toggle to test the idempotency-collapse path is a sensible follow-up.

## Source layout

```
src/
├── main.rs              CLI + entry point
├── client.rs            HTTP dispatch + cluster-aware routing
├── cluster_watch.rs     `--watch-cluster` poller + ownership-recalc annotation
├── ledger_state.rs      Tracks ledgers known to exist this run
├── metrics.rs           HDR histograms + per-class counters
├── ops.rs               Operation kinds + outcome taxonomy
├── ownership.rs         Local mirror of consensus rendezvous hash
├── reporter.rs          Live progress + final summary
├── runner.rs            Worker pool + stop condition
└── workload.rs          Workload composition (op selection, body generation)
```
