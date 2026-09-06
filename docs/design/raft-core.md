# `fluree-raft-core` — the generic Raft substrate

`fluree-raft-core` is the application-agnostic half of what used to live in
`fluree-db-consensus::raft`: durable log and snapshot storage, node and group
identity, rendezvous ownership, the HTTP transport, membership admin,
follower→leader forwarding, a state-machine seam, group bootstrap, a leader-task
lifecycle, and an optional replicated key/value fragment.

It has **no `fluree-db-*` dependencies**, by design. The nameservice group is one
consumer; others instantiate the same substrate against their own state machines
rather than forking it.

Operator-facing recipes for the nameservice cluster live in
[Raft clusters (replicated writes)](../operations/raft-clusters.md). The
nameservice's own command queue is described in
[Raft command queue](raft-command-queue.md). This page is about the substrate.

## Feature gating

| Feature | Enables | Pulls in openraft |
|---|---|---|
| *(none)* | storage traits, `node`, `group`, `ownership`, `http` | no |
| `raft` | log adapter, transport, admin, forward, state-machine seam, runtime | yes |
| `testing` | the adapter conformance fixture | yes (implies `raft`) |
| `kv` | the key/value fragment | no |
| `kv` + `raft` | `kv::sweep`, the eviction driver | yes |

Default-off `raft` is what keeps monolithic Fluree builds — which reach this
crate through `fluree-db-consensus` for `http::is_hop_by_hop` — from compiling or
linking openraft. `kv` is independent of `raft` because a fragment is pure state
plus a pure reduction; a consumer can depend on the semantics without the
runtime.

## The constrained openraft profile

`openraft::RaftTypeConfig` leaves eight associated types open. `FlureeRaftConfig`
pins six, leaving only `D` and `R` — the application's command and response.
Applications still write their own `declare_raft_types!`; the trait is a bound
with a blanket impl, not a replacement.

| Pin | Why |
|---|---|
| `NodeId = u64` | the id is a bare integer in log file names, metric labels, rendezvous scoring |
| `Node = ClusterNode` | the address *pair* must travel through membership, because forwarding resolves a peer's client URL from replicated membership rather than local config. This is the pin that makes `forward` possible at all. |
| `Entry = openraft::Entry<C>` | the log adapter round-trips entries through postcard as opaque bytes |
| `SnapshotData = Cursor<Vec<u8>>` | snapshots are whole-state encodes handed to storage as bytes |
| `Responder = OneshotResponder<C>` | openraft gates `add_learner`, `change_membership`, and `client_write` on this exact type. Without the pin the membership admin surface does not exist on `Raft<C>`. |
| `AsyncRuntime = TokioRuntime` | everything supplied to openraft here is Tokio-bound (`reqwest`, `tokio::fs`, `tokio::spawn`). Leaving it open lets a config compile and then panic on first IO. |

## The state-machine seam

An adapter has to do two different things, and mixing them is where the bugs are:
reduce the log deterministically, and cause effects. The seam splits them.

- **`AppStateMachine`** — pure reduction. `apply(&mut State, &Command, log_index)`,
  `apply_membership`, `initial_state`, `noop_response`, and the snapshot codec.
  No clocks, no RNG, no IO. Every replica reducing the same log must reach
  byte-identical state.
- **`StateMachineObserver`** — effects. Its hooks run **while the state write lock
  is held** and push owned `Effect` values into a buffer; `publish` runs **after
  the lock drops**. That ordering is the point: a subscriber that reads state back
  cannot re-enter `apply` and deadlock, and it never observes a half-applied batch.

`ReadOnlyState<A>` is the local read model handed to consumers — `read()` /
`try_read()` only. It is advisory: it reflects what this node has applied, which
on a follower may lag the leader.

Snapshot encoding is the application's: the adapter stores whatever
`encode_snapshot` returns, **verbatim**. `state_machine::codec` (`b"FRCS"` + a
`u16` version + a postcard body) is offered for applications that want a version
to migrate from, not imposed.

`RaftGroup::bootstrap` assembles storage, the log adapter, the transport, the
adapter, and `Raft` into one running group, and returns routers the host nests
wherever it likes — at bare `/raft` and `/cluster` for a single group, or under a
`GroupId` when several share a process.

## Rendezvous ownership is a wire format

`ownership` scores `(node, key_digest)` pairs with a fixed-seed xxh64 so every
node computes the same owner locally, without coordinating. That is exactly what
makes it a wire format: two nodes that disagree can both claim the same key.

**Changing the seed, the hash, the fold order, or the tie-break requires a full
cluster stop, not a rolling restart.** A frozen copy of the pre-extraction
algorithm is kept in `tests/ownership_algorithm_stability.rs` and checked against
the live one over thousands of key pairs.

## Catch-up batch sizing

openraft batches up to `max_payload_entries` (300 by default) into one
append-entries RPC, and 0.9 does **not** shrink a batch the peer refuses. So if
`max_payload_entries × max_command_bytes` exceeds the append-entries body cap
(64 MiB by default), a follower that has fallen far enough behind receives a 413,
the transport reports it as a network error, and openraft retries the same
oversized batch forever. That follower never rejoins.

The failure is silent, total, and only reachable from a node that is already
degraded — so `RaftGroupConfig::max_command_bytes` is a declaration, and
`bootstrap` **caps** `max_payload_entries` to fit rather than warning about it.

Set it from the largest command the application can propose. An application whose
commands are uniformly small (the nameservice) is safe leaving it `None`; one
embedding `kv` should set it from its policies' `max_key_bytes + max_value_bytes`
plus the command envelope.

## Leader-only tasks

`spawn_leader_watcher` runs a task factory only while this node leads. On
leadership loss it cancels, allows a shared grace deadline, aborts only the
stragglers, and **waits for all of them** before starting a new generation. That
wait is the point: without it a rapid leader flap runs two generations of
"leader-only" tasks at once, both proposing.

## The `kv` fragment

A replicated key/value fragment an application embeds in its own state machine.
Not a service, not its own group — a lease fences the work it guards only if both
are ordered by the *same* log. Put the lease in a second group and you are back to
reasoning about two independent clocks. Same-group also lets one application
command touch app state and KV state atomically.

### The fence

**An entry's version is the Raft log index of the write that created it.** Not a
per-key counter: those reset on delete or expiry, so a token can repeat, and a
repeating fencing token is not a fence. A log index is globally monotonic and
comparable across keys, which is what lets a holder embed it in external effects
(persisted run records, pre-commit re-checks).

Every successful `Put` gets a new version, renewals included. One key mutation per
command, so `log_index` identifies the write unambiguously — batching would force
`(index, ordinal)` versions and is deliberately unsupported.

### Expiry, and the logical-time floor

An entry is expired when `expires_at_ms <= now_ms`, and an expired entry is absent
to every observer: reads miss it, `Expect::Absent` succeeds against it,
`Expect::Version(v)` fails against it, `TakeOnce` does not take it.

Reclamation destroys information, though. Once the record expiring at 100 is gone,
nothing distinguishes "expired" from "never existed" — so a later command carrying
a rolled-back `now_ms` would see the reclaimed record as absent and a survivor with
the *same* expiry as live. Because `Evict` is bounded, partial sweeps are the
normal case, so that split is reachable whenever clocks disagree: which records a
caller can see would start depending on how far the last sweep got.

So `KvFragment` carries a **logical-time floor**. Every reclamation raises it to
the instant it acted at, and every later decision — reads included — is made at
`max(now_ms, time_floor)`. Time within a fragment cannot run backwards: a record
already treated as expired can never be treated as live again, and a lapsed lease
cannot be renewed back into existence by a slow clock. The floor moves only on
reclamation, so one far-future clock reading cannot mass-expire a fragment that
had nothing to reclaim.

What the floor does **not** do is make a rolled-back clock harmless in wall-clock
terms. Once a leader has asserted that time reached 100, a lease advertised to
expire at 100 is expired, and takeover at wall-clock 50 is allowed. No state
machine can do better without a trusted clock — which is the same reason TTL is
documented as a liveness knob and the fence as the safety one.

### CAS failures return the current record

Every conflict answers with what is there now. Two independent reasons: without it
each consumer bolts a read onto every failed propose, racing the state it just
observed; and it is the recovery path for a **lost response** — a renewal can
commit and lose its reply, after which retrying with the old version conflicts, and
a holder whose value carries a unique holder id recognizes its own successful
renewal in the record it gets back.

### TTL: reject, do not clamp

Silently shortening a TTL turns "why did my entry vanish" into an incident, and
clamping `ttl_ms` would not bound `now_ms + ttl` anyway. So: `Some(0)` rejected
(use `Delete`), above the policy maximum rejected, `now_ms + ttl` overflow
rejected, and `None` rejected unless the policy allows entries with no expiry.

`now_ms` comes from the proposer and `apply` has nothing to check it against. That
is deliberate: **TTL is a liveness knob, not a safety knob.** The proposer is a
cluster node, the same trust assumption a stamped `applied_at_millis` already
makes.

### Delete algebra

`Any` is idempotent and reports whether a *live* record was removed. `Absent`
succeeds only when logically absent. `Version(v)` removes only a matching live
version. Delete creates no new version.

### Bounded eviction

`Evict { cutoff_ms, limit }` ranges a secondary expiry index rather than scanning,
examines at most `limit` records — capped by policy and again by a hard maximum,
because a sweep runs inside `apply` and blocks every apply behind it — and reports
`more_expired` when work remains.

That bound makes a driver necessary. `kv::sweep` is it, and it owns two details
that are easy to get wrong in a way that only shows up later as a backlog that
never drains:

1. **Re-propose immediately, with the *same* cutoff.** Waiting a full interval
   means a fragment expiring faster than one batch per interval grows without
   bound. Re-reading the clock per round is worse: the cutoff advances under a
   steadily-expiring workload, so the sweep chases its own tail.
2. **Propose nothing when nothing has expired.** An idle group whose ticker still
   writes forces snapshots and log purges forever, on every node, for no work.

Spawn `run_sweep` from `spawn_leader_watcher`'s task factory — eviction is a write,
so only the leader should propose it.

### Size limits, at three layers

Apply-time checks are the last line, not the only one: by the time `apply` runs the
command has already been allocated, serialized, replicated, and buffered, so it
cannot stop a huge command from OOMing the process first. Enforce at all three:

1. Transport body caps (`RaftTransportConfig`).
2. Client-side before proposing, via `KvPolicy::check_put` / `check_key`.
3. In `apply` — required for determinism, since a proposer that skipped (2) must
   not be able to diverge the cluster.

Defaults are 1 KiB keys and 1 MiB values, as *policy* defaults rather than global
constants, plus per-fragment entry and byte quotas. Note the interaction with
[catch-up batch sizing](#catch-up-batch-sizing) above.

### Snapshots carry records, not derived state

A fragment's expiry index and byte total are **not serialized**. A snapshot carries
the records and the time floor, and both denormalizations are rebuilt on decode.
Validating them instead would leave a corrupt or wrongly-migrated snapshot able to
underflow the byte total or arm a stale index entry that deletes a live record;
rebuilding makes that unrepresentable, and costs one pass per install.

### Tenancy is the application's composition

`kv` has no tenant concept. An application that wants isolated namespaces keys a
map by its own enum:

```rust
#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
enum Tenant { Lease, Config, OAuth }   // APPEND ONLY

struct AppState { kv: BTreeMap<Tenant, KvFragment>, /* ... */ }
enum AppCommand { Kv(Tenant, KvCommand), /* ... */ }
```

Not separate struct fields (`kv_config`, `kv_oauth`): postcard is positional and
non-self-describing, so appending a struct field breaks decode of every deployed
snapshot and each new tenant becomes a format migration. An enum key costs one
variant plus one policy-registry entry, and old snapshots simply lack the new key.

**The sharp edge:** reordering or removing a variant does *not* error. It silently
decodes one tenant's data as another's. Pin the discriminants with a golden-bytes
test — `tests/kv_group.rs::tenant_discriminants_are_pinned` is the pattern. The
same rule governs `KvCommand`, `Expect`, and `KvResponse`, pinned by
`kv::tests::wire_format_is_pinned`.

The per-tenant `KvPolicy` registry lives **in code**, matched exhaustively, because
a policy that differs between nodes diverges the cluster. Per-tenant policy also
means a churny tenant cannot starve another's sweep.

There is deliberately **no increment/counter command**: read-compute-propose loses
races under contention, and nobody has designed an atomic increment yet.

## Conformance

`testing::run_all` runs any openraft `RaftStateMachine` through the contract every
adapter owes openraft: `applied_state` tracks the last entry, boot restore resumes
after the snapshot, a snapshot is point-in-time, install persists before it swaps,
an installed snapshot is durable, a failed install leaves state untouched,
membership survives a restart, blank and membership entries still answer, and
generated snapshot ids are path-safe.

It takes *any* openraft adapter rather than only this crate's, which is what let
the nameservice's bespoke adapter be held to the same contract while it still
existed. It is now gone — the nameservice reduces through the generic seam like
any other consumer — but the bound is still the right one: a future consumer that
needs its own adapter can be checked against the same nine properties. Both
consumers run the fixture today, in `fluree-raft-core/tests/state_machine_seam.rs`
and `fluree-db-consensus/tests/it_adapter_conformance.rs`.

The persist-before-swap check needs a storage backend whose snapshot writes can be
made to fail; without one a swap-first implementation is indistinguishable from a
persist-first one, and the check skips.

## Worked examples

Both are `fluree-raft-core`-only — no `fluree-db-*` crate — so they double as the
proof that a second consumer can stand up a group without forking anything.

- `tests/multi_node_group.rs` — three real HTTP nodes of a counter group:
  single-voter bootstrap growing to three, replication, membership mirroring, the
  live `LeaderView` the forwarder depends on, and the leader-task lifecycle.
- `tests/kv_group.rs` — three nodes whose state machine embeds tenanted `kv`
  fragments: lease acquire, renew, lapse, and takeover through `client_write`, with
  followers answering from their own replicated copy, plus the sweep driver
  reclaiming a backlog through the log.
