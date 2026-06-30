# local-cluster

A single `cluster` command that runs a real multi-node Fluree raft
cluster locally via Docker — for ad-hoc queries, transactions,
benchmarks, and failure-mode exploration. Not for production; use the
workspace-root `Dockerfile` for that.

## Why docker (vs bare processes)

- `docker kill` / `docker stop` / `docker pause` / `docker network disconnect`
  give first-class failure injection without root or `iptables`/`tc`.
- Peers have real non-loopback addresses on the container network, which
  exercises `forward.rs`'s SSRF + loopback-detection paths honestly
  (bare processes on 127.0.0.1 dodge that code).
- Works the same on Linux, macOS, and Windows.

## Prerequisites

- Docker Engine 20.10+ with the `compose` plugin (`docker compose version`
  should print v2.x). Compose v1 is detected but deprecated.
- Rust toolchain — `up` runs `cargo build --release` against the workspace.
- `curl` for bootstrap calls.
- `jq` (optional) — `status` prints richer output when present.

## Quick start

```bash
cd scripts/local-cluster
./cluster up              # default 3-node cluster
./cluster status          # leader, voters, log indices
./cluster kill 2          # kill node 2; quorum (2/3 -> 1/2) breaks
./cluster status          # node 2 reports "missing"
./cluster restart 2       # rejoins, catches up from leader
./cluster down            # tear it all down
```

## Commands

### Lifecycle

| Command | Purpose |
|---|---|
| `cluster up [N] [--no-build] [--storage MODE]` | Build, start, bootstrap N nodes (default 3) |
| `cluster down [--keep-data]` | Stop containers, network, and (by default) volumes |
| `cluster status` | Per-node container/health + `/cluster/status` from each |
| `cluster logs [N] [-f] [--tail LINES]` | Tail one node's logs, or all interleaved if N omitted |

### Failure injection

| Command | Models | Recover with |
|---|---|---|
| `cluster kill <N>` | Process crash / hard failure (SIGKILL) | `cluster restart <N>` |
| `cluster restart <N>` | Node recovery | — |
| `cluster pause <N>` | Unresponsive but alive (SIGSTOP — heartbeat timeout, not exit) | `cluster unpause <N>` |
| `cluster unpause <N>` | Resume a paused node | — |
| `cluster partition <N>` | Network split — node + peers can't reach each other | `cluster heal <N>` |
| `cluster heal <N>` | Reattach partitioned node | — |

`cluster help [command]` shows top-level or per-command usage. Run
`./cluster help <command>` for full per-command details.

### Failure-mode cheat sheet

| Test what happens when… | Sequence |
|---|---|
| The leader crashes | `cluster status` → find leader N → `cluster kill N` → `cluster status` |
| A follower lags then catches up | `cluster pause 3` → issue writes against node 1 → `cluster unpause 3` → tail logs |
| A node thinks it's still leader (split-brain) | `cluster partition 1` (assuming 1 is leader) → quorum elects new leader → `cluster heal 1` → old leader steps down |
| Cluster loses quorum | In a 3-node cluster: `cluster kill 2 && cluster kill 3` → writes refuse, reads still work → `cluster restart 2` → quorum returns |
| A slow node delays consensus | `cluster pause 2` for a few seconds during a write storm |

## Configuration

Knobs live as environment variables. Most can also be overridden via
`up` flags.

| Variable | Default | Effect |
|---|---|---|
| `DEFAULT_CLUSTER_SIZE` | `3` | Size when `up` is called without an arg |
| `PUBLIC_PORT_BASE` | `8090` | Node N publishes its public HTTP on `BASE+N` |
| `RAFT_PORT_BASE` | `9090` | Node N publishes its raft RPC on `BASE+N` |
| `FLUREE_LOCAL_CLUSTER_STORAGE` | `ephemeral` | `ephemeral` (named volumes) or `persistent` (bind-mount `./data/`) |

Examples:

```bash
./cluster up 5                                          # 5 nodes on 8091..8095 / 9091..9095
PUBLIC_PORT_BASE=18090 RAFT_PORT_BASE=19090 ./cluster up # avoid port conflicts
./cluster up --storage persistent                       # ./data/ survives down --keep-data
```

## Working against the cluster

Each node's public port is the regular Fluree HTTP API. Anything you
can do against a single-node server works against any cluster node;
writes that land on a follower are auto-forwarded to the leader.

```bash
# Create a ledger via curl
curl -X POST http://localhost:8091/fluree/create \
    -H 'Content-Type: application/json' \
    -d '{"ledger":"my-db"}'

# Or via the workspace CLI
cargo run --release -p fluree-db-cli -- query \
    --addr http://localhost:8091 --ledger my-db \
    '{"select": "?s", "where": {"@id": "?s"}, "limit": 5}'
```

## Watching cluster state during chaos

Two terminals work well: one running a `watch`'d `status`, one
issuing the failure-injection commands.

```bash
# Terminal 1 — live cluster view, refreshing twice a second, with color
watch -n 0.5 -c ./cluster status

# Terminal 2 — fault primitives + log tailing
./cluster pause 1
./cluster logs -f 2          # see node 2 trigger an election
./cluster unpause 1
```

## Cleanup

```bash
./cluster down                       # default: wipe volumes + network
./cluster down --keep-data           # keep volumes for the next `up`
docker image rm fluree-local-cluster:dev   # only if you want the image gone too
```

## How it works

`./cluster up` does:

1. `cargo build --release -p fluree-db-server --features raft` and
   copies the binary into `.build/fluree-server`.
2. `docker build` against `Dockerfile` (this dir — not the
   workspace-root one), tagging `fluree-local-cluster:dev`.
3. Generates `compose.generated.yml` with one service per node, each
   pinning its `FLUREE_RAFT_NODE_ID` and ports.
4. `docker compose up -d` and waits for each `/health` to respond.
5. Bootstraps via the cluster admin API:
   - `POST /cluster/initialize` to node 1 with `members={1}`
   - For each remaining node: `POST /cluster/add-learner` (blocking
     so the leader replicates existing log before returning)
   - `POST /cluster/change-membership` to promote all to voters

Peers find each other via container hostnames (`fluree-N`) on the
`fluree-cluster` bridge network — that's why kill / restart work
cleanly per-node.

## Files

```
cluster                  Single entry point (executable)
Dockerfile               Dev-only image; uses target/release/fluree-server
README.md                This file
.gitignore               Ignores .build/, compose.generated.yml, data/
.build/fluree-server     (generated) the copied release binary
compose.generated.yml    (generated) the compose file `up` writes
data/                    (generated, persistent mode only) bind-mounted volumes
```
