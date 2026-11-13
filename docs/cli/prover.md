# fluree prover

Install and check the prover that [permanent erasure](../transactions/permanent-erasure.md) needs.

The prover, `fluree-prover`, is a separate binary released with each version of Fluree. A server
runs it to prove each erasure: it holds RISC Zero's prover, about 83 MB, which no other feature
needs. Only a server that takes erasure requests (`POST /erase`) needs it. Receiving erasures (by
clone, pull, push or restore), query peers, removals and `f:noHistory` check proofs without it.

A server finds the prover beside its own binary, else on the `PATH`, unless `--erasure-prover`
names it: a path, or a bare command name looked up on the `PATH`. At startup it checks that the prover proves an image the release accepts; a prover of
another release makes proofs every check refuses. Without a prover of its release, the server runs
and refuses erasure requests (`501`), and its discovery document reports `erasure.proving: false`.

Builds exist for Linux (x86-64 and arm64) and macOS on Apple silicon. The Linux builds need glibc 2.39 or
later (Ubuntu 24.04, Debian 13 or RHEL 10, or newer). On an older system, run the server's `-erasure`
Docker image (`fluree/server:latest-erasure`), which ships the prover, or build the prover from source.

## Typical use

```bash
# 1. Install the prover beside fluree
fluree prover install

# 2. Check that this release accepts it
fluree prover status

# 3. Start (or restart) the server: it looks for the prover at startup
fluree server run

# 4. Confirm the server takes erasure requests
curl -s http://localhost:8090/.well-known/fluree.json | jq .erasure
# { "proving": true }
```

After upgrading `fluree`, run `fluree prover status` again. If the installed prover is from a release
this one does not accept, replace it with `fluree prover install --force` and restart the server.

[Running the erasure prover](../operations/erasure-prover.md) covers sizing, memory isolation, the
scratch directory, clusters and troubleshooting. The
[permanent erasure cookbook](../guides/cookbook-erasure.md) walks an erasure from start to finish.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `install` | Download this release's prover and install it beside `fluree` |
| `status` | Show the prover a server started here would use, and whether it matches |

---

## fluree prover install

Download this release's prover for this platform, check it, and install it beside `fluree`, where a
server started with `fluree server run` finds it.

### Usage

```bash
fluree prover install [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--version <VERSION>` | Release to install from (default: this binary's) |
| `--dir <DIR>` | Directory to install into (default: beside this binary) |
| `--force` | Replace a prover already there |

It downloads `fluree-prover-<target>.zst` and its `.sha256` from the release, and installs the
prover only when the checksum matches and the prover proves an image this release accepts. A prover
already installed is kept unless `--force` is given; an installed prover of another release is
reported, not replaced.

`FLUREE_PROVER_DOWNLOAD_URL` names a directory holding a release's prover assets in place of the
release on GitHub: a mirror, for a host without access to GitHub.

### Examples

```bash
# Beside fluree
fluree prover install

# Into a directory on the PATH
fluree prover install --dir /usr/local/bin
```

---

## fluree prover status

Show the prover a server started here would use, and whether it proves an image this release
accepts.

### Usage

```bash
fluree prover status
```

### Examples

```bash
$ fluree prover status
✓ /usr/local/bin/fluree-prover: proves an image this release accepts
```

| Output | Meaning |
|--------|---------|
| `✓ <path>: proves an image this release accepts` | A server started here can prove erasures |
| `No fluree-prover beside this binary or on the PATH: permanent erasure is unavailable…` | No prover is installed where a server would look. Run `fluree prover install` |
| `✗ <path> proves image …, which this release does not accept: install this release's prover` | The prover is from another release. Run `fluree prover install --force` |
| `✗ <path> --image-id did not answer within 10s` | The binary did not run: wrong platform, a glibc older than 2.39, or a damaged file |

`status` looks beside this binary and on the `PATH`. It does not read a server's `--erasure-prover`
setting, so for a server that names its prover, check the server's startup log or its discovery
document (`erasure.proving`).

## See also

- [Running the erasure prover](../operations/erasure-prover.md): installing, sizing and isolating
  the prover, and the `--erasure-prover*` server options
- [Cookbook: permanent erasure](../guides/cookbook-erasure.md): an erasure from start to finish
- [Permanent erasure](../transactions/permanent-erasure.md): what the prover does
