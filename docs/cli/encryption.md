# fluree encryption

Encryption at rest: the keys a deployment holds and key rotation.

## Usage

```bash
fluree encryption status  [--remote <NAME> | --connection-config <PATH>] [--json]
fluree encryption rotate  --retire <ID> [--dry-run] [--ledger <LEDGER>] [--rate <BYTES/S>] [--wait] [--remote <NAME> | --connection-config <PATH>] [--json]
fluree encryption resume  [--wait] [--remote <NAME> | --connection-config <PATH>] [--json]
fluree encryption pause   [--remote <NAME> | --connection-config <PATH>] [--json]
fluree encryption cancel  [--remote <NAME> | --connection-config <PATH>] [--json]
fluree encryption verify  --retire <ID> [--remote <NAME> | --connection-config <PATH>] [--json]
fluree encryption generate-key
```

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `encryption status` | Show the held key ids, the current key, and the rotation record if one exists |
| `encryption rotate` | Start, or resume, a rotation that re-envelopes every blob on `--retire` under the current key |
| `encryption resume` | Continue the rotation the record describes |
| `encryption pause` | Stop the running sweep after its next blob; it resumes from its cursor |
| `encryption cancel` | Stop the running sweep; the next `rotate` starts over |
| `encryption verify` | Count blobs still on `--retire` across the whole store and stamp the record |
| `encryption generate-key` | Print a fresh base64 AES-256 key for `AES256Key` or an `AES256Keys` entry |

## Options

| Option | Description |
|--------|-------------|
| `--remote <NAME>` | Execute against a remote server (by remote name, e.g. `origin`) |
| `--connection-config <PATH>` | Run directly against the storage a connection config (JSON-LD) describes; the config must list every key involved |
| `--json` | Print the raw JSON response |
| `--retire <ID>` | The key being retired (`rotate`, `verify`) |
| `--dry-run` | Count what a rotation would rewrite without writing anything |
| `--ledger <LEDGER>` | Limit the sweep to one ledger, by name or branch-qualified id |
| `--rate <BYTES/S>` | Throttle rewrites, e.g. `50mb` per second |
| `--wait` | Poll status until the sweep stops, printing one line per change |

Every subcommand except `generate-key` needs `--remote` or `--connection-config`. The CLI's own local store under `.fluree/` is never encrypted, so there is no third mode.

## Description

Every encrypted blob carries the id of the key that encrypted it. A storage configured with a key set (`AES256Keys` plus `AES256CurrentKey`) reads any held key and writes with the current one, so a rotation is a background sweep that rewrites, in place, every blob still on the retiring key. Addresses are hashes of plaintext, so nothing but the bytes at rest changes.

The sweep is resumable. It walks ledger by ledger in a fixed order and checkpoints its position and counters to a record in the same storage every thousand blobs or thirty seconds. That record is a cache of where the sweep stood; the truth is in the blob headers, so resuming from a stale record only re-reads a few of them. A server resumes a running record after a restart, and under Raft the leader runs the sweep and hands it to the next leader on a change of leadership.

Nothing removes a key. `encryption verify` reads every header in the store and stamps the record `completed` when none remain on the retiring key. That stamp is the signal to drop the key from configuration.

The rollout order across nodes matters more than the commands: add the new key everywhere as a decrypt-only entry and restart, make it current everywhere and restart, then rotate and verify. See [Key Rotation](../security/encryption.md#key-rotation) for the full sequence.

A local run (`--connection-config`) always waits for the sweep, since exiting would abandon it. A remote run returns at once unless `--wait` is given.

## Examples

```bash
# Generate a key for the new AES256Keys entry
fluree encryption generate-key

# What does the server hold, and is a rotation in progress?
fluree encryption status --remote origin

# Count the work first
fluree encryption rotate --retire 1 --dry-run --remote origin --wait

# Rotate, throttled, and watch it
fluree encryption rotate --retire 1 --rate 50mb --wait --remote origin

# After a restart or a pause
fluree encryption resume --wait --remote origin

# Confirm nothing remains on key 1 before removing it from config
fluree encryption verify --retire 1 --remote origin

# Directly against storage, without a server
fluree encryption rotate --retire 1 --connection-config /etc/fluree/connection.jsonld
```

## Output

```
Keys held: [2,1] (current: 2)
Rotation: completed — retiring key 1 onto key 2
  holder node-1  last checkpoint 4s ago
  units 5/5
  scanned 1834  rewritten 1790 (61234567 bytes)  already current 12  other keys 0  not enveloped 32  failed 0
  verified: 0 blob(s) remain on the retiring key
```

`STALLED` on a running record means no checkpoint has landed for ten minutes: nobody is advancing it, and the next `rotate` or a leader election takes it over.
