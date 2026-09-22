# fluree push

Push local ledger changes to upstream remote, similar to `git push`.

## Usage

```bash
fluree push [LEDGER]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Description

Pushes **local commits** to the configured upstream remote by uploading the commit v2 bytes to the server.

The ledger must have an upstream configured (see `fluree upstream set`).

The push uses strict sequencing + CAS semantics:

- The server rejects the push if the remote head is not in your local history (diverged) or if the remote is ahead.
- The server also rejects the push if the first commit’s `t` does not match the server’s next-t.

Unlike `fetch`/`pull`, this is **not** a storage-proxy replication operation. It requires **write** permissions for the ledger (Bearer token with `fluree.ledger.write.*` claims) and the server validates the pushed commits like normal transactions.

If a pushed commit contains **retractions**, the server enforces a strict invariant: each retraction must target a fact that is currently asserted at that point in the push batch. (List retractions require exact list-index metadata match.)

### Pushing a merge

Push sends the branch's first-parent line: the commits the server replays. A merge commit on that line already carries the combined changes of the branch it merged. The commits that branch made still travel with the push. The server stores them without replaying them, so the history stays complete on the remote. Commits the remote already has are left out, so merging the same branch twice pushes only what it gained in between.

The server stores those commits as history without replaying them. What the merge brings into the branch's state is in the merge commit, which the server validates like any other commit.

A push containing a merge needs a server that supports it. The CLI checks the server's discovery document first. If the server does not support it, the push is refused before anything is sent. The server then needs upgrading.

The remote head must be on your branch's first-parent line. If it is only reachable through a merge, the histories have diverged. Push then asks you to pull first.

## Examples

```bash
# Push active ledger
fluree push

# Push specific ledger
fluree push mydb
```

## Output

Successful push:
```
Pushing 'mydb:main' to 'origin'...
✓ 'mydb:main' pushed 3 commit(s) (new head t=42)
```

Push rejected (remote is ahead):
```
Pushing 'mydb:main' to 'origin'...
error: push rejected; remote is ahead (local t=10, remote t=42). Pull first.
```

No upstream configured:
```
error: no upstream configured for 'mydb:main'
  hint: fluree upstream set mydb:main <remote>
```

## Errors

| Error | Description |
|-------|-------------|
| No upstream configured | Run `fluree upstream set <ledger> <remote>` first |
| Push rejected (409) | Remote head changed, histories diverged, or first commit `t` does not match next-t |
| Push rejected (422) | Invalid commit bytes, missing required referenced blob, or retraction invariant violation |
| History contains a merge | The remote does not support pushing merges. Upgrade the remote server. |
| 404 for a push containing a merge | The remote advertises support, but the node that handled the request does not have it. A node may still be on an earlier release. |

## Workflow

Typical sync workflow:

```bash
# Configure remote and upstream (one time)
fluree remote add origin https://api.example.com --token @~/.fluree/token
fluree upstream set mydb origin

# Daily workflow
fluree pull mydb        # Get latest changes
# ... make local changes ...
fluree push mydb        # Push your changes
```

## See Also

- [clone](clone.md) - Clone a ledger from a remote
- [upstream](upstream.md) - Configure upstream tracking
- [pull](pull.md) - Pull changes from upstream
- [fetch](fetch.md) - Fetch refs without modifying local ledger
