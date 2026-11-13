# Running the erasure prover

[Permanent erasure](../transactions/permanent-erasure.md) replaces a commit with a redacted copy and proves, in zero knowledge, that the copy is the original less exactly the erased operations. The proofs are made by `fluree-prover`, a separate binary. This page covers installing it, pointing a server at it, sizing and isolating it, and keeping it working across upgrades.

For the erasure workflow itself, see the [permanent erasure cookbook](../guides/cookbook-erasure.md).

## What it is, and who needs one

`fluree-prover` holds RISC Zero's prover. It is about 83 MB and no other feature uses it, so it ships apart from `fluree`. A server runs it as a child process, once per affected commit: the commit and the operations to remove go in, the replacement commit and a succinct proof come out. The server then verifies the proof itself, against the program images its release pins, before it stores anything. It never takes the prover's word.

| Needs a prover | Does not |
|---|---|
| A server that accepts erasure requests (`POST /erase`) | Receiving erasures by clone, pull, push or archive restore (proofs are checked, not made) |
| Every member of a Raft cluster that erases | Query peers |
| A Rust application that calls `Fluree::erase_occurrences` | [Targeted removal](../transactions/history-retention.md#targeted-removal), [`f:noHistory`](../transactions/history-retention.md#fnohistory) and [TTL](../transactions/ttl.md) |

A server without a prover runs normally and answers erasure requests with `501`.

## Install

### With the CLI

```bash
fluree prover install
```

This downloads the prover built for this release and platform, checks its checksum, checks that it proves an image this release accepts, and puts it beside the `fluree` binary. A server started from that binary finds it there.

```bash
# Somewhere on the PATH instead
fluree prover install --dir /usr/local/bin

# Replace a prover that is already there
fluree prover install --force
```

Builds exist for Linux (x86-64 and arm64) and macOS on Apple silicon. The Linux builds need glibc 2.39 or later: Ubuntu 24.04, Debian 13, RHEL 10, or newer. On an older system use the Docker image, or build from source.

### With Docker

Every release publishes a second image with the prover included, tagged with the `-erasure` suffix:

```bash
docker run -d --name fluree \
  -p 8090:8090 \
  -v fluree-data:/var/lib/fluree \
  fluree/server:latest-erasure
```

`fluree/server:<version>-erasure` pins a release. The image is otherwise the same as the standard one; see [Running with Docker](docker.md).

### Without access to GitHub

Mirror the release's prover assets (`fluree-prover-<target>.zst` and its `.sha256`) to a directory you serve, and name it:

```bash
FLUREE_PROVER_DOWNLOAD_URL=https://mirror.internal/fluree/v4.3.0 fluree prover install
```

The checksum and image checks run the same way.

### From source

Building needs the RISC Zero toolchain and Docker; see [`fluree-prover/README.md`](https://github.com/fluree/db/blob/main/fluree-prover/README.md). The prover's program builds in a pinned container, so a prover built from a release's sources proves the image that release accepts.

## Check it

```bash
fluree prover status
```

| Output | Meaning |
|---|---|
| `✓ /usr/local/bin/fluree-prover: proves an image this release accepts` | A server started here can erase |
| `No fluree-prover beside this binary or on the PATH…` | Nothing is installed where a server would look |
| `✗ … proves image …, which this release does not accept: install this release's prover` | The prover is from another release. Run `fluree prover install --force` |
| `✗ … --image-id did not answer within 10s` | The binary cannot run here: wrong platform, an old glibc, or a broken file |

Then check the running server. Its discovery document reports whether it will take requests:

```bash
curl -s http://localhost:8090/.well-known/fluree.json | jq .erasure
```

```json
{ "proving": true }
```

When it is `false`, the startup log has the reason. Look for one of these messages:

```
permanent erasure available prover=/usr/local/bin/fluree-prover
permanent erasure unavailable: no fluree-prover beside this binary or on the PATH (install it with `fluree prover install`)
permanent erasure disabled (--disable-erasure)
```

A server looks for the prover once, when it starts. Restart it after installing or replacing one.

## Server settings

The server looks for the prover in this order: the path or command name given by `--erasure-prover`, then beside its own binary, then on the `PATH`.

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `--erasure-prover <path>` | `FLUREE_ERASURE_PROVER` | beside the server's binary, else on the `PATH` | The prover: a path, or a command name looked up on the `PATH`. A server on memory or S3 storage, or a query peer, refuses to start with it |
| `--disable-erasure` | `FLUREE_DISABLE_ERASURE` | off | Refuse erasure requests even where a prover is installed |
| `--erasure-scratch-dir <dir>` | `FLUREE_ERASURE_SCRATCH_DIR` | `<temp>/fluree-erasure` | The prover's temporary files |
| `--erasure-prover-threads <n>` | `FLUREE_ERASURE_PROVER_THREADS` | every core | Proving threads |
| `--erasure-prover-timeout-secs <n>` | `FLUREE_ERASURE_PROVER_TIMEOUT_SECS` | `14400` (4 hours) | A step still running after this long is killed and retried. A request whose steps time out three times fails |

With `fluree server run`, server flags go after `--`:

```bash
fluree server run --storage-path /var/lib/fluree -- \
  --erasure-prover-threads 8 \
  --erasure-scratch-dir /var/lib/fluree-scratch
```

**The scratch directory holds plaintext.** While a step runs, the commit being erased is written there unencrypted, including the values you are erasing. Each step's directory is removed when the step ends, and a purge removes any that a crashed step left. Put it on a volume you would trust with the data: not a shared `/tmp`, and on encrypted disk if the ledger's storage is encrypted.

## Memory and CPU

Proving is the most expensive thing a Fluree server does. Measured on a 16-vCPU host:

| Commit | Proving time |
|---|---|
| 1,000 operations, unsigned or `"signatures": "expose"` | about 4 minutes |
| 1,000 operations, signed (signatures verified in the proof) | about 6 minutes |
| 50,000 operations (the largest that can be erased) | about 3 hours |

- **Memory.** Allow about 10 GB for the prover, on top of what the server uses. Peak memory does not grow with the commit's size.
- **CPU.** The prover uses every core unless `--erasure-prover-threads` caps it. Time scales with the cores it gets.
- **Concurrency.** A host proves one step at a time. A typical request is two or three commits per branch, proved one after another.
- **Throughput.** One 16-vCPU prover makes roughly 250 to 380 proofs of 1,000-operation commits a day.

The [ZK redaction feasibility study](../design/zk-redaction-feasibility.md) has the full measurements.

### Keeping the prover from hurting the server

Each step runs as its own process at low priority (nice 10), so transactions get the CPU first when both want it. Capping threads leaves cores free outright:

```bash
fluree server run -- --erasure-prover-threads 8
```

Memory is the harder problem. On Linux the prover marks itself as the process the kernel should kill first when memory runs out (`oom_score_adj` 1000). If that happens the step is retried later and the server keeps running. That is a preference, not isolation, and two common setups defeat it:

- **Kubernetes** on cgroup v2 kills all of a container's processes together, the server with the prover, unless the kubelet sets `singleProcessOOMKill`.
- **systemd** stops a whole service when the kernel kills any of its processes, unless the unit sets `OOMPolicy=continue`.

For a server that also takes production traffic, size the host or container so the prover's 10 GB fits beside the server's cache (`--cache-max-mb`), or move proving to another host.

### A dedicated proving node

On a [Raft cluster](raft-clusters.md#garbage-collection-and-permanent-erasure), the leader does not prove unless it has to. It hands each step to another member with a prover, preferring one that neither leads nor stages the transactions of the affected ledger. A learner started with a prover therefore makes a proving node that serves no writes:

1. Install the prover on **every** member. The leader accepts erasure requests only with a prover of its own, and any member may become the leader.
2. Add a learner on a host sized for proving, sharing the cluster's file storage.
3. Leave `--erasure-prover-threads` at the default on the learner. Cap it on the voters, where a proof may still land if the learner is down.

A single node has no one to hand proofs to. Size it for both jobs, or accept slower transactions while a proof runs.

## Upgrades

A server accepts proofs only from the program images its release pins, and checks its prover against them at startup. A prover from an earlier release may or may not still be accepted. After upgrading `fluree`:

```bash
fluree prover status
# If it reports an image this release does not accept:
fluree prover install --force
```

Then restart the server. With Docker, moving to the new release's `-erasure` tag does both at once.

Erasures already published keep working across upgrades: their proofs are verified against every image the release still accepts, and no prover is needed to read them.

A job that was `proving` when the server stopped resumes when it starts again.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `POST /erase` answers `501` | No prover found, a prover of another release, `--disable-erasure`, memory or S3 storage, or a query peer | `fluree prover status`, then the startup log |
| Server refuses to start: `--erasure-prover needs file storage named by --storage-path` | `--erasure-prover` given with memory storage, a connection config, or proxy storage | Erasure needs file storage. Remove the flag or change storage |
| `fluree prover install` fails with `no prover is built for …` | Unsupported platform | Use the `-erasure` Docker image, or build from source |
| `prover install failed: … 404` | No release assets for that version (a development build, for example) | Install from a release whose prover this binary accepts (`--version <VERSION>`; the image check still applies), or build from source |
| Job stays `proving`, `retries` climbing, `lastError` mentions the prover failing or being killed | Out of memory | Give it about 10 GB of headroom; check the Kubernetes and systemd notes above |
| Job stays `proving`, no retries | A large commit is still being proved | Wait. Check the prover process is using CPU |
| Job `failed` after timeouts | Steps passed `--erasure-prover-timeout-secs` three times | Add cores or raise the timeout, then send the same request again |
| Transactions slow while a job proves | The prover and the server share cores | Cap `--erasure-prover-threads`, or prove on another host |

Job fields such as `retries` and `lastError` come from `GET /v1/fluree/retention/{ledger}`; see [Status](../transactions/permanent-erasure.md#status). `lastError` is audit data: it is returned only to a token with a `fluree.ledger.audit.*` claim for the ledger.

## Related documentation

- [`fluree prover`](../cli/prover.md): the CLI command
- [Permanent erasure](../transactions/permanent-erasure.md): the reference
- [Cookbook: permanent erasure](../guides/cookbook-erasure.md): an erasure from start to finish
- [Raft clusters](raft-clusters.md#garbage-collection-and-permanent-erasure): erasure and garbage collection on a cluster
- [Configuration](configuration.md): every server flag
