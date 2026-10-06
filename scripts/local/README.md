# local

A single `stack` command that runs a local Fluree deployment via Docker
— monolithic single-node or Raft cluster — for ad-hoc queries,
transactions, benchmarks, and failure-mode exploration. Not for
production; use the workspace-root `Dockerfile` for that.

## Why docker (vs bare processes)

- `docker kill` / `docker stop` / `docker pause` / `docker network disconnect`
  give first-class failure injection without root or `iptables`/`tc`.
- In Raft mode, peers have real non-loopback addresses on the container
  network, which exercises `forward.rs`'s SSRF + loopback-detection
  paths honestly (bare processes on 127.0.0.1 dodge that code).
- Works the same on Linux, macOS, and Windows.

## Prerequisites

- Docker Engine 20.10+ with the `compose` plugin (`docker compose version`
  should print v2.x). Compose v1 is detected but deprecated.
- Rust toolchain — `up` runs `cargo build --release` against the workspace.
- `curl` for raft bootstrap calls.
- `jq` (optional) — `status` prints richer raft output when present.

## Quick start

```bash
cd scripts/local
./stack up                              # default: one-node monolithic
./stack status                          # container state + /health
./stack load --workload single-pound --duration 30s
./stack down                            # tear down

./stack up --mode raft                  # 3-node raft cluster
./stack up --mode raft --nodes 5        # 5-node raft cluster
./stack kill 2                          # kill node 2; quorum (2/3 → 1/2) breaks
./stack restart 2                       # rejoins, catches up from leader
./stack down                            # tear down
```

## Deployment modes

| Mode | Nodes | Consensus | Use for |
|---|---|---|---|
| `monolithic` (default) | 1 | none | local dev, ad-hoc query/transact, throughput baselines |
| `raft` | N (default 3) | Raft via `fluree-db-consensus` | exercising the consensus path, failure-mode exploration, ownership-recalc tests |

Both modes optionally add `--peers M` query peers. Each peer is a
read-only Fluree server (`server_role=peer`) that subscribes to a
tx-server's `/v1/fluree/events` for watermarks and forwards writes
to that node's tx-server URL. Peer target depends on mode:

- Monolithic: all peers subscribe to fluree-1 (the only tx-server).
  This is currently the only end-to-end-working peer topology —
  runtime SSE events reach the peer's watermarks correctly.
- Raft with N nodes: peer P subscribes to
  `fluree-((P-1) % (N-1) + 2)`, cycling through followers. Peers see
  the initial snapshot correctly, but runtime events don't
  propagate to watermarks — a raft-side event-bus wiring gap
  (see the raft peer notes below).

Peers use shared storage — the same data volume the tx-server uses,
mounted at `/var/lib/fluree/data`. Proxy-storage mode (peer reads
via HTTP with a JWT) is a planned follow-up.

The `up` command remembers the mode + node/peer counts in
`compose.generated.yml`; every later command reads them and adapts
(e.g. `status` skips the raft `/cluster/status` view in monolithic
mode; `partition` / `heal` error in monolithic mode; peer commands
require raft mode).

## Commands

### Lifecycle

| Command | Purpose |
|---|---|
| `stack up [--mode MODE] [--nodes N] [--no-build] [--storage MODE]` | Build, start, and (if raft) bootstrap a deployment |
| `stack down [--keep-data]` | Stop containers, network, and (by default) volumes |
| `stack status` | Per-node container/health (+ `/cluster/status` in raft mode) |
| `stack logs [N] [-f] [--tail LINES]` | Tail one node's logs, or all interleaved if N omitted |
| `stack load [args...]` | Run the HTTP load harness against this deployment (see `load/README.md`) |

### Failure injection

All targeting commands accept a bare raft node id (`2`) or a peer
id (`peer-1`).

| Command | Modes | Models | Recover with |
|---|---|---|---|
| `stack kill <target>` | both | Process crash / hard failure (SIGKILL) | `stack restart <target>` |
| `stack restart <target>` | both | Node recovery | — |
| `stack pause <target>` | both | Unresponsive but alive (SIGSTOP — heartbeat timeout in raft, hang in monolithic) | `stack unpause <target>` |
| `stack unpause <target>` | both | Resume a paused target | — |
| `stack partition <target>` | raft only | Network split — target + peers can't reach each other | `stack heal <target>` |
| `stack heal <target>` | raft only | Reattach partitioned target | — |
| `stack repoint <peer-N> <fluree-M>` | raft + peers | Point a peer at a new raft node — models "SSE target dies, peer opens a new subscription elsewhere" | — |

`stack help [command]` shows top-level or per-command usage. Run
`./stack help <command>` for full per-command details.

### Failure-mode cheat sheet (raft)

| Test what happens when… | Sequence |
|---|---|
| The leader crashes | `stack status` → find leader N → `stack kill N` → `stack status` |
| A follower lags then catches up | `stack pause 3` → issue writes against node 1 → `stack unpause 3` → tail logs |
| A node thinks it's still leader (split-brain) | `stack partition 1` (assuming 1 is leader) → quorum elects new leader → `stack heal 1` → old leader steps down |
| Cluster loses quorum | In a 3-node cluster: `stack kill 2 && stack kill 3` → writes refuse, reads still work → `stack restart 2` → quorum returns |
| A slow node delays consensus | `stack pause 2` for a few seconds during a write storm |
| Peer's subscription target dies | `stack kill 2` (fluree-2, peer-1's target) → peer-1 keeps serving stale reads locally → `stack repoint peer-1 fluree-3` → peer reconnects, snapshot catches up freshness |

### Query peers

`stack up --mode raft --nodes 3 --peers 2` adds two read-only query
peers to the deployment. Each peer:

- Runs `fluree-server` with `server_role=peer`.
- Subscribes to a specific raft follower's `/v1/fluree/events` for
  ledger + graph-source watermarks.
- Serves queries locally against the shared `shared-data` volume —
  reads never touch the raft cluster once the peer's nameservice
  knows about a ledger.
- Forwards writes to the tx-server URL (same raft node it subscribes
  to). The follower's leader-forward middleware transparently
  redirects to the current leader.

Peer commands share the same target vocabulary as raft nodes:
`kill peer-1`, `restart peer-1`, `pause peer-1`, `logs peer-1`, etc.
`partition peer-1` isolates the peer from the deployment network —
useful for testing what a client sees when their peer becomes
unreachable.

`stack repoint peer-N fluree-M` reconfigures peer N to a new
subscription target and recreates its container. Models the "peer's
SSE target goes down, peer opens a new subscription against a
surviving cluster member" scenario. The peer's local storage view
is unaffected (still shared-data); only the tx-server URL for
writes and the SSE URL for watermarks change.

**Known Fluree behavior worth naming (not a stack bug):** on the
current build, peer runtime SSE events don't propagate ledger
watermarks reliably against a raft-backed transaction server —
freshness advances *only* on reconnect (when the peer receives a
fresh snapshot). Ledgers that existed at the peer's last
connection-time snapshot serve up-to-date-at-snapshot-time data;
new writes after the snapshot aren't visible to the peer until it
reconnects. Workarounds during development:

- `stack kill peer-1 && stack restart peer-1` — cheapest way to
  force a fresh snapshot.
- `stack repoint peer-1 fluree-M` — same effect but explicitly
  routes to a new target.

Reads against the peer stay *available* even when the subscription
target dies (peer's local nameservice + shared storage keep serving
the last-snapshot view), so this affects freshness, not availability.

## Configuration

Knobs live as environment variables. Most can also be overridden via
`up` flags.

| Variable | Default | Effect |
|---|---|---|
| `DEFAULT_RAFT_NODES` | `3` | Node count for `up --mode raft` when `--nodes` omitted |
| `DEFAULT_PEERS` | `0` | Peer count for `up --mode raft` when `--peers` omitted |
| `PUBLIC_PORT_BASE` | `8090` | Node N publishes its public HTTP on `BASE+N` |
| `RAFT_PORT_BASE` | `9090` | Node N publishes its raft RPC on `BASE+N` (raft mode only) |
| `PEER_PORT_BASE` | `7090` | Peer P publishes its public HTTP on `BASE+P` (raft + peers) |
| `FLUREE_LOCAL_STORAGE` | `ephemeral` | `ephemeral` (named volumes) or `persistent` (bind-mount `./data/`) |

Examples:

```bash
./stack up                                                # monolithic on 8091
./stack up --mode raft --nodes 5                          # 5 nodes on 8091..8095 / 9091..9095
PUBLIC_PORT_BASE=18090 RAFT_PORT_BASE=19090 ./stack up    # avoid port conflicts
./stack up --storage persistent                           # ./data/ survives down --keep-data
```

## Working against the deployment

Every node's public port is the regular Fluree HTTP API. In raft mode,
writes that land on a follower are auto-forwarded to the leader, so
any node URL accepts any request.

```bash
# Create a ledger via curl
curl -X POST http://localhost:8091/v1/fluree/create \
    -H 'Content-Type: application/json' \
    -d '{"ledger":"my-db"}'

# Or via the workspace CLI
cargo run --release -p fluree-db-cli -- query \
    --addr http://localhost:8091 --ledger my-db \
    '{"select": "?s", "where": {"@id": "?s"}, "limit": 5}'
```

## Watching state during chaos

Two terminals work well: one running a `watch`'d `status`, one
issuing the failure-injection commands.

```bash
# Terminal 1 — live view, refreshing twice a second, with color
watch -n 0.5 -c ./stack status

# Terminal 2 — fault primitives + log tailing
./stack pause 1
./stack logs -f 2          # raft mode: see node 2 trigger an election
./stack unpause 1
```

## Cleanup

```bash
./stack down                       # default: wipe volumes + network
./stack down --keep-data           # keep volumes for the next `up`
docker image rm fluree-local:dev # only if you want the image gone too
```

## How it works

`./stack up` does:

1. `cargo build --release -p fluree-db-server --features raft` and
   copies the binary into `.build/fluree-server` (same binary serves
   both modes; raft is opted-in via env vars on the container, not
   feature-flagged at build time).
2. `docker build` against `Dockerfile` (this dir — not the
   workspace-root one), tagging `fluree-local:dev`.
3. Generates `compose.generated.yml` with one service per node. In
   raft mode each service pins its `FLUREE_RAFT_NODE_ID` and exposes
   both the public + raft ports; in monolithic mode the raft env vars
   are absent and only the public port is mapped.
4. `docker compose up -d` and waits for each `/health` to respond.
5. In raft mode, bootstraps via the cluster admin API:
   - `POST /cluster/initialize` to node 1 with `members={1}`
   - For each remaining node: `POST /cluster/add-learner` (blocking
     so the leader replicates existing log before returning)
   - `POST /cluster/change-membership` to promote all to voters

Peers (raft mode) find each other via container hostnames (`fluree-N`)
on the `fluree-net` bridge network — that's why kill / restart /
partition / heal work cleanly per-node.

## Files

```
stack                      Single entry point (executable)
Dockerfile               Dev-only image; uses target/release/fluree-server
README.md                This file
load/                    HTTP load harness (Rust crate; see load/README.md)
.gitignore               Ignores .build/, compose.generated.yml, data/, load/target/
.build/fluree-server     (generated) the copied release binary
.build/fluree-load       (generated) the copied load harness binary
compose.generated.yml    (generated) the compose file `up` writes
data/                    (generated, persistent mode only) bind-mounted volumes
```
