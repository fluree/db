# fluree branch

Manage branches for a ledger.

## Subcommands

### fluree branch create

Create a new branch.

**Usage:**

```bash
fluree branch create <NAME> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<NAME>` | Name for the new branch (e.g., "dev", "feature-x") |

**Options:**

| Option | Description |
|--------|-------------|
| `--ledger <LEDGER>` | Ledger name (defaults to active ledger) |
| `--from <BRANCH>` | Source branch to create from (defaults to "main") |
| `--remote <REMOTE>` | Execute against a remote server |

**Description:**

Creates a new branch for a ledger. The branch starts at the same transaction time as the source branch and is fully isolated -- subsequent transactions on either branch are invisible to the other.

Branches can be nested: you can create a branch from any existing branch, not just "main".

**Examples:**

```bash
# Create a branch from main (default)
fluree branch create dev

# Create a branch for a specific ledger
fluree branch create dev --ledger mydb

# Create a branch from another branch
fluree branch create feature-x --from dev

# Create a branch on a remote server
fluree branch create staging --ledger mydb --remote origin
```

**Output:**

```
Created branch 'dev' from 'main' at t=5
Ledger ID: mydb:dev
```

### fluree branch list

List all branches for a ledger.

**Usage:**

```bash
fluree branch list [LEDGER] [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

**Options:**

| Option | Description |
|--------|-------------|
| `--remote <REMOTE>` | List branches on a remote server |

**Examples:**

```bash
# List branches for the active ledger
fluree branch list

# List branches for a specific ledger
fluree branch list mydb

# List branches on a remote server
fluree branch list mydb --remote origin
```

**Output:**

```
 BRANCH     T   SOURCE
 main       5   -
 dev        7   main
 feature-x  8   dev
```

### fluree branch drop

Drop a branch from a ledger.

**Usage:**

```bash
fluree branch drop <NAME> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<NAME>` | Branch name to drop (e.g., "dev", "feature-x") |

**Options:**

| Option | Description |
|--------|-------------|
| `--ledger <LEDGER>` | Ledger name (defaults to active ledger) |
| `--remote <REMOTE>` | Execute against a remote server |

**Description:**

Drops a branch from a ledger. The `main` branch cannot be dropped.

- **Leaf branches** (no children) are fully deleted — storage artifacts are removed and the NsRecord is purged.
- **Branches with children** are retracted (hidden from listings, reject new transactions) but storage is preserved so that child branches continue to work. When the last child is eventually dropped, the retracted parent is automatically cascade-purged.

**Examples:**

```bash
# Drop a branch
fluree branch drop dev

# Drop a branch for a specific ledger
fluree branch drop dev --ledger mydb

# Drop a branch on a remote server
fluree branch drop staging --ledger mydb --remote origin
```

**Output (leaf branch):**

```
Dropped branch 'dev'.
  Artifacts deleted: 5
```

**Output (branch with children):**

```
Branch 'dev' retracted (has children, storage preserved).
```

**Output (cascade):**

```
Dropped branch 'feature'.
  Artifacts deleted: 3
  Cascaded drops: mydb:dev
```

### fluree branch rebase

Rebase a branch onto its source branch's current HEAD.

**Usage:**

```bash
fluree branch rebase <NAME> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<NAME>` | Branch name to rebase (e.g., "dev", "feature-x") |

**Options:**

| Option | Description |
|--------|-------------|
| `--ledger <LEDGER>` | Ledger name (defaults to active ledger) |
| `--strategy <STRATEGY>` | Conflict resolution strategy (default: "take-both"). Options: `take-both`, `abort`, `take-source`, `take-branch`, `skip` |
| `--remote <REMOTE>` | Execute against a remote server |

**Description:**

Replays a branch's unique commits on top of the source branch's current HEAD. This brings the branch up to date with upstream changes. The `main` branch cannot be rebased.

If the branch has no unique commits, a fast-forward rebase is performed — the branch point is simply updated to the source's current HEAD.

Conflicts occur when both the branch and source have modified the same (subject, predicate, graph) tuples. See [conflict strategies](../concepts/ledgers-and-nameservice.md#rebasing-a-branch) for details.

**Examples:**

```bash
# Rebase with default strategy
fluree branch rebase dev

# Rebase with abort-on-conflict strategy
fluree branch rebase dev --strategy abort

# Rebase for a specific ledger
fluree branch rebase feature-x --ledger mydb --strategy take-source

# Rebase on a remote server
fluree branch rebase dev --ledger mydb --remote origin
```

**Output (fast-forward):**

```
Fast-forward rebase of 'dev' to t=5.
```

**Output (with replay):**

```
Rebased 'dev': 3 commits replayed, 0 skipped, 1 conflicts, 0 failures.
  New branch point: t=8
```

### fluree branch diff

Show a read-only merge preview between two branches.

**Usage:**

```bash
fluree branch diff <SOURCE> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<SOURCE>` | Source branch name to preview merging from (e.g., "dev", "feature-x") |

**Options:**

| Option | Description |
|--------|-------------|
| `--target <BRANCH>` | Target branch to preview merging into (defaults to source's parent branch) |
| `--max-commits <N>` | Cap on per-side commit summaries shown (default: 50; pass 0 for unbounded in local mode) |
| `--max-conflict-keys <N>` | Cap on conflict keys shown (default: 50; pass 0 for unbounded in local mode) |
| `--no-conflicts` | Skip conflict computation for a cheaper preview |
| `--conflict-details` | Include source/target flake values for returned conflict keys |
| `--strategy <STRATEGY>` | Strategy used for conflict detail labels (default: `take-both`). Options: `take-both`, `abort`, `take-source`, `take-branch` |
| `--json` | Emit the raw JSON preview |
| `--ledger <LEDGER>` | Ledger name (defaults to active ledger) |
| `--remote <REMOTE>` | Execute against a remote server |

**Description:**

`branch diff` reports ahead/behind commits, fast-forward eligibility, and conflicting `(subject, predicate, graph)` keys without mutating state. With `--conflict-details`, the preview also shows the source and target values for the returned conflict keys and annotates what the selected strategy would do.

**Examples:**

```bash
# Preview merging dev into its parent
fluree branch diff dev

# Preview a specific target
fluree branch diff dev --target main

# Show value details and source-winning labels
fluree branch diff dev --target main --conflict-details --strategy take-source

# Emit raw JSON for UI tooling
fluree branch diff dev --conflict-details --json
```

### fluree branch merge

Merge a source branch into a target branch.

**Usage:**

```bash
fluree branch merge <SOURCE> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<SOURCE>` | Source branch name to merge from (e.g., "dev", "feature-x") |

**Options:**

| Option | Description |
|--------|-------------|
| `--ledger <LEDGER>` | Ledger name (defaults to active ledger) |
| `--target <BRANCH>` | Target branch to merge into (defaults to source's parent branch) |
| `--strategy <STRATEGY>` | Conflict resolution strategy (default: `take-both`). Options: `take-both`, `abort`, `take-source`, `take-branch`. |
| `--remote <REMOTE>` | Execute against a remote server |

**Description:**

Merges a source branch into a target branch. When the target hasn't advanced since the source branched, this is a fast-forward; otherwise `--strategy` controls how conflicting edits are resolved (mirroring `branch rebase`).

When `--target` is omitted, the merge target is inferred from the source branch's parent (the branch it was created from).

After a successful merge, the source branch remains intact and can continue to receive new transactions and be merged again. Only the new commits since the last merge (or branch creation) are copied.

**Examples:**

```bash
# Merge dev into main (inferred from branch point)
fluree branch merge dev

# Merge feature-x into dev (explicit target)
fluree branch merge feature-x --target dev

# Merge for a specific ledger
fluree branch merge dev --ledger mydb

# Merge with source-winning conflict resolution
fluree branch merge dev --target main --strategy take-source

# Merge on a remote server
fluree branch merge dev --ledger mydb --remote origin
```

**Output:**

```
Merged 'dev' into 'main' (fast-forward to t=8, 3 commits copied).
```

**Output (non-fast-forward):**

```
Merged 'dev' into 'main' (t=9, 3 commits copied, 1 conflicts).
```

## See Also

- [create](create.md) - Create a new ledger
- [list](list.md) - List all ledgers
- [info](info.md) - Show ledger details
- [use](use.md) - Switch active ledger
