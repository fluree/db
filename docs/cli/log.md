# fluree log

Show commit log for a ledger.

## Usage

```bash
fluree log [LEDGER] [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--oneline` | Show one-line summary per commit |
| `-n, --count <N>` | Maximum number of commits to show (default: 100) |
| `--all` | Show the whole chain, with no limit. Conflicts with `-n` |
| `--remote <NAME>` | Read the log from a configured remote |
| `--direct` | Execute in-process, bypassing auto-routing through a local server |

## Description

Displays the commit history for a ledger, newest first. Each entry shows the
commit's id, its transaction number `t`, the timestamp, and how many flakes the
commit carries.

Commit messages are **not** shown. The commit format does not persist one
today, so the one-line form shows the timestamp in that column instead.

### Commit ids are hex digests

The id printed here is the first twelve characters of the commit's SHA-256
digest, in hex. That is the spelling the commit resolvers accept, so anything
`fluree log` prints can be pasted directly into [`show`](show.md), into
`history --from` / `--to`, or into a `@commit:` time-travel specifier.

A commit also has a base32 CID spelling — that is what the JSON API returns as
`commit_id`, and what `fluree show` reports as `"id"`. The two are the same
commit; `log` prints hex because a CID cannot usefully be abbreviated. A CIDv1
opens with seven header bytes identifying the multibase, version, codec and hash
function, so the first twelve characters of *every* commit CID are the constant
`bagaybqabciq` and a thirteenth character adds only four bits. Pasting an
abbreviated CID into `show` gets you a diagnostic saying so; pass the hex digest
or the full CID instead.

### How many commits are walked

`-n` and `--all` bound how many commits are *loaded and shown*, not how far the
walk reaches: the chain's shape has to be read in full before commits can be
ordered by `t`. What the limit bounds is the number of full commit blobs
fetched and the amount held in memory at once.

The default of 100 matches the server's, so `fluree log` answers the same way
whether or not it routes through a local server.

## Examples

```bash
# Show the most recent 100 commits
fluree log

# Show last 5 commits
fluree log -n 5

# The whole chain
fluree log --all

# One-line format
fluree log --oneline

# Specific ledger
fluree log production --oneline -n 10
```

## Output

### Full Format (default)

```
$ fluree log logdemo
commit ddca65d84c08
Date:    2026-09-15T02:40:25.527303+00:00
t:       6
Flakes:  1

commit 0ffb1bb2678a
Date:    2026-09-15T02:40:25.479820+00:00
t:       5
Flakes:  1

commit 879416d9f4a4
Date:    2026-09-15T02:40:25.415656+00:00
t:       4
Flakes:  1
```

### One-line Format

```
$ fluree log logdemo --oneline
t=6     ddca65d84c08  2026-09-15T02:40:25.527303+00:00
t=5     0ffb1bb2678a  2026-09-15T02:40:25.479820+00:00
t=4     879416d9f4a4  2026-09-15T02:40:25.415656+00:00
t=3     9c23b9c65161  2026-09-15T02:40:25.365666+00:00
t=2     d016e150b86c  2026-09-15T02:40:25.305330+00:00
t=1     cf96b59f4fa9  2026-09-15T02:40:25.191433+00:00
```

### Truncation

When the chain is longer than the limit, a note goes to stderr so it does not
contaminate a piped log:

```
$ fluree log logdemo --oneline -n 2
t=6     ddca65d84c08  2026-09-15T02:40:25.527303+00:00
t=5     0ffb1bb2678a  2026-09-15T02:40:25.479820+00:00
(showing 2 of 6 commits — pass -n to widen, or --all)
```

### Feeding an id to another command

```
$ fluree show --ledger logdemo ddca65d84c08
{
  "id": "bagaybqabciqn3stf3bgar34nucpfp7z34eupir7ku4w2wzxd6yx2elvthu2mdga",
  "t": 6,
  "time": "2026-09-15T02:40:25.527303+00:00",
  "size": 212,
  "parents": [
    "bagaybqabciqa76y3wjtyvhjxcpohcnygpbiarxnyolzjwtkgh42swikogjinkdi"
  ],
  "asserts": 1,
  "retracts": 0,
  ...
}
```

## See Also

- [show](show.md) - Show decoded contents of a specific commit
- [info](info.md) - Show ledger details
- [history](history.md) - Show entity change history
