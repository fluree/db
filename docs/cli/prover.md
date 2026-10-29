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

Builds exist for Linux (x86-64 and arm64) and macOS on Apple silicon.

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

## See also

- [Permanent erasure](../transactions/permanent-erasure.md): what the prover does, and the
  `--erasure-prover*` server options
