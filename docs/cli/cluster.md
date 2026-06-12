# fluree cluster

Bootstrap and manage a Raft cluster (replicated transaction servers).

Wraps the Fluree server's private `/cluster/*` admin endpoints over HTTP. Run this from your operator workstation against each node's `--raft-listen-addr` (the VPC-internal admin port — not the client-facing port). The admin endpoints carry no built-in authentication; the CLI assumes reachability over a private network or SSH tunnel.

For the full deployment recipe and architectural context, see [Raft clusters (replicated writes)](../operations/raft-clusters.md).

## Subcommands

### fluree cluster init

Bootstrap a fresh single-node cluster on the seed node. Run **once**, against one node. That node becomes a single-voter cluster and auto-elects itself leader on the next election tick. Subsequent peers are added with `cluster add` and `cluster promote`.

**Usage:**

```bash
fluree cluster init [OPTIONS]
```

**Options:**

| Option                  | Description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `--addr <URL>`          | Admin URL of the seed node (e.g. `http://node-1-private:9090`).             |
| `--node-id <U64>`       | This node's id. Must be unique in the cluster and stable across restarts.   |
| `--raft-url <URL>`      | This node's inter-node Raft RPC URL (e.g. `http://node-1-private:9090/raft`). |
| `--client-url <URL>`    | This node's client-facing URL — used by peers when forwarding writes (e.g. `http://node-1:8080`). |

**Example:**

```bash
fluree cluster init \
  --addr        http://node-1-private:9090 \
  --node-id     1 \
  --raft-url    http://node-1-private:9090/raft \
  --client-url  http://node-1:8080
```

Returns immediately on success. Fails with `409 Conflict` if the cluster has already been initialized.

### fluree cluster add

Add a non-voting peer (learner) to an existing cluster. The new node replicates the log from the leader; once caught up it can be promoted to a voter with `cluster promote`. Issue against the current **leader**.

**Usage:**

```bash
fluree cluster add [OPTIONS]
```

**Options:**

| Option                  | Description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `--leader <URL>`        | Admin URL of the cluster leader.                                            |
| `--node-id <U64>`       | Id for the new peer.                                                        |
| `--raft-url <URL>`      | New peer's inter-node Raft RPC URL.                                         |
| `--client-url <URL>`    | New peer's client-facing URL.                                               |
| `--blocking <BOOL>`     | Wait for the learner to catch up before returning. Default: `true`.         |

**Example:**

```bash
fluree cluster add \
  --leader      http://node-1-private:9090 \
  --node-id     2 \
  --raft-url    http://node-2-private:9090/raft \
  --client-url  http://node-2:8080
```

`--blocking true` (default) is the right choice for orchestration scripts that immediately follow up with `cluster promote` — it removes the race between replication and the promote call.

### fluree cluster promote

Change the cluster's voting membership. Promotes learners to voters or demotes / removes existing voters. Issue against the **leader**.

**Usage:**

```bash
fluree cluster promote [OPTIONS]
```

**Options:**

| Option                  | Description                                                                  |
|-------------------------|------------------------------------------------------------------------------|
| `--leader <URL>`        | Admin URL of the cluster leader.                                             |
| `--members <U64>,...`   | New voter set, comma-separated (e.g. `1,2,3`).                               |
| `--retain`              | Keep voters dropped from `--members` as learners. Default: removed entirely. |

**Examples:**

```bash
# Promote learners 2 and 3 into the voting set.
fluree cluster promote \
  --leader  http://node-1-private:9090 \
  --members 1,2,3

# Drain node 3 — drop it from the voter set but keep it replicating
# as a learner so reads stay current while you debug.
fluree cluster promote \
  --leader  http://node-1-private:9090 \
  --members 1,2 \
  --retain
```

### fluree cluster status

Snapshot cluster state (current leader, term, voters, learners, last-applied log index). Any node's admin URL works — followers serve their last-known view, which is good enough for health checks and operator scripts.

**Usage:**

```bash
fluree cluster status --addr <URL>
```

**Example:**

```bash
fluree cluster status --addr http://node-1-private:9090
```

Output:

```
Cluster status
  leader:        1
  term:          4
  last_applied:  1283
  voters:        1, 2, 3
  learners:      (none)
```

## See also

- [Raft clusters (replicated writes)](../operations/raft-clusters.md) — deployment recipe, architecture, security notes.
