# Raft clusters (replicated transaction servers)

This guide is for operators who want to run `fluree-server` as a Raft-replicated cluster instead of a single-node transaction server. In this mode every node accepts client traffic, but writes are committed through a Raft log so the cluster survives the loss of any minority of nodes.

## When to use Raft mode

Use it when:

- you need high availability for writes (single-node loss must not stop transactions),
- you can put all nodes behind one shared, content-addressed storage backend (typically S3 with a shared DynamoDB nameservice in the cloud profile, or a shared file mount in a homogeneous cluster),
- you can give each node a stable identity (node id) and a stable VPC-internal address for inter-node RPC.

Do **not** use Raft mode for:

- single-node deployments — the overhead of running a 1-node cluster is needless complexity. Use the default `transaction` server role instead.
- query-only replicas — `--server-role peer` already solves that, with lighter operational overhead.
- proxy-storage peers — Raft mode replicates writes through the log, which is the opposite of proxy mode (which forwards writes to a remote transaction server). Validation rejects the combination at startup.

## Architecture in one paragraph

Every node runs an openraft state machine over a **separate, on-disk Raft log + snapshots** (per node) and shares the same content-addressed storage for ledger blobs (per cluster). Reads on every node observe the replicated state machine via `RaftNameService`, so a follower's reads see anything the leader has applied as soon as the entry commits. Writes that land on a follower are transparently HTTP-forwarded to the leader by middleware; the leader stages the operation, writes the commit blob to shared storage, and proposes an `AdvanceRef` through the log. Once a quorum acknowledges, every node's state machine applies the new head and the leader's response is returned to the client.

Each node listens on **two ports**:

- **Client port** (`--listen-addr`, default `0.0.0.0:8080`) — the public, load-balancer-facing API.
- **Raft port** (`--raft-listen-addr`) — VPC-internal: inter-node Raft RPC under `/raft/*` plus cluster admin under `/cluster/*`. **No authentication is enforced on this port.** Operators are expected to bind it to a private interface (security group, VPC ACL, firewall rule).

## Per-node configuration

Raft mode is built behind a `raft` Cargo feature. A build without the feature has no `--raft-*` flags at all, so single-node binaries don't pull in openraft.

When the feature is enabled, four CLI/env switches govern Raft mode:

| CLI flag                | Env var                        | Required when raft is on |
| ----------------------- | ------------------------------ | ------------------------ |
| `--raft-enabled`        | `FLUREE_RAFT_ENABLED`          | toggle                   |
| `--raft-node-id`        | `FLUREE_RAFT_NODE_ID`          | yes (stable u64)         |
| `--raft-storage-path`   | `FLUREE_RAFT_STORAGE_PATH`     | yes                      |
| `--raft-listen-addr`    | `FLUREE_RAFT_LISTEN_ADDR`      | yes                      |

`--raft-storage-path` must point at a directory on **durable** local storage (not tmpfs). Losing this directory loses committed transactions for this node — though as long as a quorum of nodes still has its log, openraft can re-deliver the lost state when the node is re-added with the same id.

The same options are also accepted in `.fluree/config.toml`:

```toml
[server.raft]
enabled     = true
node_id     = 1
storage_path = "/var/lib/fluree/raft"
listen_addr = "0.0.0.0:9090"
```

CLI / env override file values, file overrides defaults.

### Storage backend choice

Raft mode is orthogonal to the content-addressed storage choice. In production we recommend shared cloud storage so that the **commit blobs the leader writes are immediately visible to every follower's reads** through the same backend. Concretely:

- **AWS** — point all nodes at the same S3 bucket(s) via `--connection-config /etc/fluree/s3.jsonld`. Use a shared DynamoDB nameservice; that nameservice's writes are **not used** in Raft mode (Raft's state machine is the authoritative nameservice), but the storage half stays shared.
- **On-prem** — point all nodes at the same NFS / object-store mount via `--storage-path /shared/fluree`.

The `RaftNameService` on each node sees committed log state; the storage backend just has to be reachable from every node.

### A complete per-node startup

```bash
fluree-server \
  --listen-addr 0.0.0.0:8080 \
  --connection-config /etc/fluree/s3.jsonld \
  --raft-enabled \
  --raft-node-id 1 \
  --raft-storage-path /var/lib/fluree/raft \
  --raft-listen-addr 0.0.0.0:9090
```

After this the node is **idle** — it has joined no cluster yet. Subsequent admin calls form the cluster.

## Bootstrapping a 3-node cluster

The general shape is:

1. Start `fluree-server` on every node with its per-node config.
2. On **one** node — call it node 1 — initialize the cluster as a single-voter cluster (node 1 auto-elects itself).
3. Add nodes 2 and 3 as **learners**; the leader replicates the existing log to each new peer.
4. Promote the learners to **voters** so they participate in quorum.

The cluster is now a 3-node Raft group. Future membership changes reuse steps 3–4.

The cleanest way to drive these is the `fluree cluster` CLI subcommand, which wraps the private `/cluster/*` HTTP endpoints. The CLI does not need to be installed on the cluster nodes themselves — it just needs network reachability to each node's admin URL. Run it from your bastion / operator workstation over SSH tunnels or via VPC peering.

```bash
# 1. Start every node first (see "A complete per-node startup" above).

# 2. Initialize node 1 as the seed (single-voter cluster).
fluree cluster init \
  --addr      http://node-1-private:9090 \
  --node-id   1 \
  --raft-url  http://node-1-private:9090/raft \
  --client-url http://node-1:8080

# 3. Add nodes 2 and 3 as learners, blocking on catch-up so the
#    follow-up promote can't race replication.
fluree cluster add \
  --leader     http://node-1-private:9090 \
  --node-id    2 \
  --raft-url   http://node-2-private:9090/raft \
  --client-url http://node-2:8080

fluree cluster add \
  --leader     http://node-1-private:9090 \
  --node-id    3 \
  --raft-url   http://node-3-private:9090/raft \
  --client-url http://node-3:8080

# 4. Promote both learners into the voting set.
fluree cluster promote \
  --leader  http://node-1-private:9090 \
  --members 1,2,3

# 5. Confirm.
fluree cluster status --addr http://node-1-private:9090
```

After step 5 you should see `voters: 1, 2, 3`, a non-empty `leader`, and a non-zero `term`.

If you'd rather script the bootstrap directly against the HTTP surface, each CLI command is a single POST or GET — see the [Admin HTTP surface](#admin-http-surface) section below.

## Day-2 operations

### Adding a node

Same recipe as steps 3–4 of the bootstrap: start the new node, `fluree cluster add` against the current leader (blocking), then `promote` with the new voter set.

### Removing a node

`promote --members 1,2 --retain` keeps the dropped voter as a learner (useful for graceful drain); `promote --members 1,2` (no `--retain`) removes it entirely.

### Restarting a node

Restart in place. openraft re-reads its log and snapshots from `--raft-storage-path` and rejoins automatically once it can reach the leader. The node's HTTP listener is up immediately but its `RaftNameService` returns stale data until the log catches up — clients targeting that node will see slightly older reads during the gap.

### Recovering a failed node

If a node's Raft storage is lost or corrupted, delete the contents of `--raft-storage-path` on that node, restart it, then on the current leader call `fluree cluster add` with the **same** `--node-id`. The leader will deliver the full log (or a snapshot if the log has been truncated past the join point) to bring the node current. `promote` to re-add it to the voter set when it's caught up.

### Configuration changes

Changes to client-facing config (auth, CORS, indexing thresholds) take effect on next restart and don't need cluster coordination — restart nodes one at a time, observing `fluree cluster status` between restarts to make sure quorum is preserved.

`--raft-node-id` and `--raft-listen-addr` **must not change** for a running node. If you really need to change them, treat it as remove + re-add.

## Admin HTTP surface

All four `fluree cluster` actions map 1:1 to a single HTTP call. Useful when scripting in environments where the CLI isn't available.

### `POST /cluster/initialize`

Once, on the seed node, to bootstrap a single-voter cluster:

```bash
curl -X POST http://node-1-private:9090/cluster/initialize \
  -H 'Content-Type: application/json' \
  -d '{
        "members": {
          "1": {
            "raft_addr":   "http://node-1-private:9090/raft",
            "client_addr": "http://node-1:8080"
          }
        }
      }'
```

Returns `204 No Content` on success. `409 Conflict` if the cluster has already been initialized.

### `POST /cluster/add-learner`

Against the leader, for each new peer:

```bash
curl -X POST http://node-1-private:9090/cluster/add-learner \
  -H 'Content-Type: application/json' \
  -d '{
        "node_id":     2,
        "raft_addr":   "http://node-2-private:9090/raft",
        "client_addr": "http://node-2:8080",
        "blocking":    true
      }'
```

Returns `204 No Content` when the learner has caught up (with `blocking: true`). `421 Misdirected Request` if you hit a follower — the response body names the current leader.

### `POST /cluster/change-membership`

Against the leader:

```bash
curl -X POST http://node-1-private:9090/cluster/change-membership \
  -H 'Content-Type: application/json' \
  -d '{ "members": [1, 2, 3], "retain": false }'
```

`retain: true` keeps dropped voters as learners; `false` (default) removes them.

### `GET /cluster/status`

Against any node:

```bash
curl http://node-1-private:9090/cluster/status
```

Returns JSON:

```json
{
  "current_leader": 1,
  "current_term": 4,
  "last_applied_index": 1283,
  "voters": [1, 2, 3],
  "learners": []
}
```

## Client traffic and follower forwarding

Clients can hit **any** node on its client port. Reads always serve locally. Writes (transact, push, branch admin) are inspected by the follower-forward middleware: if the receiving node is the current leader, the request is handled in place; otherwise it's re-issued against the leader's `client_addr` (looked up from the membership the cluster replicates) and the leader's response is relayed back to the client verbatim. From the client's perspective it's a single round-trip — the extra hop lives inside the cluster.

If no leader is currently elected (mid-election), follower nodes return `503 Service Unavailable` with body `"no leader currently elected; retry shortly"`. Standard client retry/backoff handles this.

## Security notes

- The `--raft-listen-addr` listener has **no built-in authentication**. Operators are expected to enforce trust at the network layer (security groups, VPC ACLs, host firewall rules). Never expose this port to the public internet.
- The `--listen-addr` (client) port is governed by the same auth knobs as the single-node transaction server (`--events-auth-*`, `--data-auth-*`, `--admin-auth-*`). Forwarded requests preserve client auth headers across the follower→leader hop — the leader re-evaluates auth against the same trusted-issuer set, so the auth boundary is unchanged.
- The CLI does not authenticate to admin endpoints. If you need an audit trail of admin calls, front the admin listener with a proxy that adds it (and update the CLI / curl calls to go through that proxy).
