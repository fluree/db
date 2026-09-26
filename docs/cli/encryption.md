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
| `encryption resume` | Re-issue `rotate` for the record's retiring key and ledger scope. A `running` (stale or released), `paused`, `failed` or `swept` record continues; a `cancelled` or `completed` one starts a new sweep |
| `encryption pause` | Stop the running sweep once the blob in progress is done; it resumes from its cursor. Server only (`--remote`) |
| `encryption cancel` | Stop the running sweep; the next `rotate` starts over. Server only (`--remote`) |
| `encryption verify` | Count blobs still on `--retire` across the whole store and stamp the record |
| `encryption generate-key` | Print a fresh base64 AES-256 key for `AES256Key` or an `AES256Keys` entry |

## Options

| Option | Description |
|--------|-------------|
| `--remote <NAME>` | Execute against a remote server (by remote name, e.g. `origin`) |
| `--connection-config <PATH>` | Run directly against the storage a connection config (JSON-LD) describes; the config must list every key involved |
| `--json` | Print the raw JSON response |
| `--retire <ID>` | The key being retired (`rotate`, `verify`) |
| `--dry-run` | Count what a rotation would rewrite without writing anything. The count is `progress.on_retired`, shown only with `--json` |
| `--ledger <LEDGER>` | Limit the sweep to one ledger: a name covers every branch, a branch-qualified id only that branch. The ledger's shared dictionaries are included, graph sources are not |
| `--rate <BYTES/S>` | Cap rewritten plaintext bytes per second, as a number with an optional `kb`, `mb` or `gb` suffix. Suffixes are binary (`1mb` = 1 MiB); a bare number is bytes. Not stored: a resumed sweep runs unthrottled unless restarted with `rotate --rate` |
| `--wait` | Poll status every two seconds until the record is no longer `running`, printing progress to stderr. A stalled or released record is still `running`, so `--wait` keeps waiting |

Every subcommand except `generate-key` needs `--remote` or `--connection-config`. The CLI's own local store under `.fluree/` is never encrypted, so there is no third mode.

## Description

Every encrypted blob carries the id of the key that encrypted it. A storage configured with a key set (`AES256Keys` plus `AES256CurrentKey`) reads any held key and writes with the current one, so a rotation is a background sweep that rewrites, in place, every blob still on the retiring key. Addresses are hashes of plaintext, so nothing but the bytes at rest changes.

The sweep is resumable. It walks in a fixed order (each ledger branch, then each ledger's shared dictionaries, then graph sources) and checkpoints its position and counters to a record in the same storage every thousand blobs or thirty seconds. That record is a cache of where the sweep stood; the truth is in the blob headers, so resuming from a stale record only re-reads a few of them.

Under Raft the leader runs the sweep. A sweep does not always continue by itself after a restart or a change of leader; when status shows it `stalled` or `released`, run `fluree encryption resume`. See [Resuming after a restart or leader change](../security/encryption.md#resuming-after-a-restart-or-leader-change).

Nothing removes a key. `encryption verify` reads every header in the store and stamps the record `completed` when none remain on the retiring key. That stamp, `completed` with `completion.remaining_on_retired` of `0`, is the signal to drop the key from configuration. A dry run also finishes as `completed (dry run)`, without that stamp; it is not a verification. Run `verify` after the sweep stops: verifying a different key id than the record's replaces the record.

A store configured with a single `AES256Key` has every blob on key id `0`. To rotate it, list that key as `{"keyId": 0, "AES256Key": …}` in `AES256Keys`, add the new key, and retire `0`.

The rollout order across nodes matters more than the commands: add the new key everywhere as a decrypt-only entry and restart, make it current everywhere and restart, then rotate and verify. See [Key Rotation](../security/encryption.md#key-rotation) for the full sequence.

A local run (`--connection-config`) runs the sweep inside the CLI process and always waits for it, since exiting would abandon it. `pause` and `cancel` act on the process that runs the sweep, so they cannot reach a local sweep from another shell; interrupt it instead. The record stays `running`, and `resume` takes it over once it shows `stalled` (ten minutes). A remote run returns at once unless `--wait` is given.

`--ledger` narrows the sweep, but the verification at its end counts the whole store, so a scoped run ends `swept` while other ledgers or graph sources remain on the key.

## Exit status and JSON

The command exits 0 whenever the request succeeds, including when `verify` finds blobs remaining or a sweep ends `failed` or `swept` ([#1954](https://github.com/fluree/db/issues/1954)). Scripts should check `--json` output: a rotation is finished when the record's `state` is `completed`, `dry_run` is `false`, and `completion.remaining_on_retired` is `0`. In a status object the record is under `progress`; `verify --json` prints the record itself.

With `--json`:

- `status` prints the status object (`key_ids`, `current_key_id`, `progress`, `active_here`, `seconds_since_update`, `stalled`, `released`), or `{"encrypted": false, "key_ids": [], "current_key_id": null}` for a store that is not encrypted.
- `rotate` and `resume` print the progress record as the sweep starts. With `--wait` they print the final status object instead. A local run without `--wait` prints both, one after the other.
- `pause` and `cancel` print `{"ok": true}`.
- `verify` prints the progress record.

## Examples

```bash
# Generate a key for the new AES256Keys entry
fluree encryption generate-key

# What does the server hold, and is a rotation in progress?
fluree encryption status --remote origin

# Count the work first: the count is .progress.on_retired. Under Raft, point
# --remote at the leader, which holds the dry run's result in memory.
fluree encryption rotate --retire 1 --dry-run --wait --json --remote origin

# Rotate, throttled, and watch it
fluree encryption rotate --retire 1 --rate 50mb --wait --remote origin

# After a restart or a pause
fluree encryption resume --wait --remote origin

# Confirm nothing remains on key 1 before removing it from config
fluree encryption verify --retire 1 --remote origin

# Directly against storage, without a server
fluree encryption rotate --retire 1 --connection-config /etc/fluree/connection.jsonld

# First rotation of a store that used a single AES256Key (key id 0)
fluree encryption rotate --retire 0 --wait --remote origin
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

`STALLED` on a running record means no checkpoint has landed for ten minutes. Usually nobody is advancing it, but listing a unit and the closing verification write no checkpoint, so on a large store check that the holder does not report `active_here: true` before resuming. `RELEASED` means the last holder let go on a change of leader and the new leader has not taken it over. In both cases `fluree encryption resume` continues it.
