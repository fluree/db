# fluree show

Show the decoded contents of a commit — assertions and retractions with resolved IRIs.

## Usage

```bash
fluree show <COMMIT> [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<COMMIT>` | Commit identifier: `t:<N>` transaction number, hex-digest prefix (min 6 chars), or full CID |

## Options

| Option | Description |
|--------|-------------|
| `--ledger <NAME>` | Ledger name (defaults to active ledger) |
| `--remote <NAME>` | Execute against a remote server (by remote name, e.g., "origin") |

## Description

Displays the full decoded contents of a single commit, similar to `git show`. Each flake (assertion or retraction) is rendered with IRIs compacted using the ledger's namespace prefix table.

The commit identifier can be:
- A **transaction number** prefixed with `t:` (e.g., `t:5`) as shown in `fluree log` output
- An **abbreviated hex digest** (minimum 6 characters) as shown in the storage directory or obtained from the txn-meta graph
- A **full CID string** (e.g., `bagaybqabciq...`)

## Policy Filtering

When executed against a remote server (`--remote`), the returned flakes are filtered by the server's data-auth policy. The identity is derived from the Bearer token and the policy class from the server's `default_policy_class` configuration. Flakes the caller is not permitted to read are silently omitted, and the `asserts`/`retracts` counts reflect only the visible flakes.

Unlike the query endpoints, show does not support per-request policy overrides via headers or request body — it uses only the Bearer token identity and server-configured default policy class.

When executed locally (no `--remote`, or with `--direct`), `fluree show` operates with full local-admin access and no policy filtering is applied. This is consistent with other local CLI operations that read directly from storage.

## Output Format

The output is a JSON object containing:

| Field | Description |
|-------|-------------|
| `id` | Full CID of the commit |
| `t` | Transaction number |
| `time` | ISO 8601 timestamp |
| `size` | Commit blob size in bytes |
| `previous` | Previous commit CID |
| `signer` | Transaction signer (if signed) |
| `asserts` | Number of assertion flakes |
| `retracts` | Number of retraction flakes |
| `@context` | Namespace prefix table (prefix → IRI) |
| `flakes` | Array of flake tuples in SPOT order |

Each flake is a tuple: `[subject, predicate, object, datatype, operation]`

- `operation`: `true` = assert (added), `false` = retract (removed)
- Ref objects use `"@id"` as the datatype
- When metadata is present (language tag, list index, or named graph), a 6th element is appended: `{"lang": "en", "i": 0, "graph": "ex:myGraph"}`

## Examples

```bash
# Show a commit by transaction number
fluree show t:5

# Show a commit by hex prefix
fluree show 3dd028

# Show a commit from a specific ledger
fluree show 0303b7 --ledger _system

# Show a commit on a remote server
fluree show t:5 --remote origin

# Show by hex prefix on remote with explicit ledger
fluree show 3dd028 --remote origin --ledger mydb

# Pipe to jq for filtering
fluree show 3dd028 | jq '.flakes[] | select(.[4] == true)'
```

### Example Output

```json
{
  "id": "bagaybqabciqd3ubikmk2zh6gjxngpgjja3vi5myleidf46htiybpswyy2665zra",
  "t": 40,
  "time": "2026-03-12T16:58:18.395474217+00:00",
  "size": 327,
  "previous": "bagaybqabciqc64dbbv46vrueddgqfrafgmo27u4fibkrvwdmr2g6ze4cbaeg23a",
  "asserts": 1,
  "retracts": 1,
  "@context": {
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "schema": "http://schema.org/",
    "f": "https://ns.flur.ee/db#"
  },
  "flakes": [
    ["urn:fsys:dataset:zoho3", "schema:dateModified", "2026-03-12T14:15:30Z", "xsd:string", false],
    ["urn:fsys:dataset:zoho3", "schema:dateModified", "2026-03-12T16:58:16Z", "xsd:string", true]
  ]
}
```

In this example, one property (`dateModified`) was updated: the old value was retracted (`false`) and the new value asserted (`true`).

## See Also

- [log](log.md) - Show commit log (list of commits)
- [history](history.md) - Show change history for a specific entity
- [info](info.md) - Show ledger details
