# Cookbook: Branching and Merging

Fluree lets you fork a ledger into independent branches, each with its own commit history. Experiment freely, then merge changes back when ready. Think of it like `git branch` for your data.

## Quick start

```bash
# Create a branch from main
fluree branch create experiment

# Switch to the branch
fluree use mydb:experiment

# Make changes (only on the branch)
fluree insert '...'
fluree update '...'

# See both branches
fluree branch list

# Merge back into main
fluree branch merge experiment

# Clean up
fluree branch drop experiment
```

## Core concepts

- **Branches are isolated** — Transactions on one branch are invisible to others
- **Branches are cheap** — Creating a branch doesn't copy data; it creates a new commit pointer
- **Merge is fast-forward** — The target branch must not have diverged. If it has, rebase first
- **Source branch survives merge** — After merging, the branch can continue receiving transactions

## Patterns

### Safe experimentation

Try a risky change without affecting production:

```bash
fluree branch create try-new-schema

fluree use mydb:try-new-schema

# Restructure data
fluree update 'PREFIX ex: <http://example.org/>
DELETE { ?doc ex:category ?cat }
INSERT { ?doc ex:tags ?cat }
WHERE  { ?doc ex:category ?cat }'

# Verify the change looks right
fluree query 'SELECT ?doc ?tag WHERE { ?doc ex:tags ?tag }'

# If it works, merge back
fluree branch merge try-new-schema
fluree branch drop try-new-schema

# If it doesn't work, just drop the branch — main is untouched
fluree branch drop try-new-schema
```

### Review before merge

Use branches as a staging area for data changes:

```bash
# Data engineer creates a branch for the weekly import
fluree branch create weekly-import

fluree use mydb:weekly-import

# Import new data
fluree insert -f new-data.ttl

# Verify: count new entities
fluree query 'SELECT (COUNT(?s) AS ?count) WHERE { ?s a ex:NewRecord }'

# Verify: no duplicates
fluree query 'SELECT ?id (COUNT(?s) AS ?count) WHERE {
  ?s ex:externalId ?id
} GROUP BY ?id HAVING(?count > 1)'

# Looks good — merge into main
fluree branch merge weekly-import
```

### Multi-environment workflow

Use branches to model dev/staging/prod environments within a single ledger:

```bash
# Create environment branches
fluree branch create staging
fluree branch create dev --from staging

# Developers work on dev
fluree use mydb:dev
fluree insert '...'

# Promote to staging via merge
fluree branch merge dev --target staging

# Promote to main (production) after testing
fluree use mydb:staging
# ... run validation queries ...
fluree branch merge staging
```

### Feature branches

Multiple people can work on different features simultaneously:

```bash
# Team member A: add product categories
fluree branch create feature-categories

# Team member B: update pricing
fluree branch create feature-pricing

# Each works independently on their branch
# ...

# Merge sequentially — first one is a fast-forward
fluree branch merge feature-categories

# Second one may need rebase if main advanced
fluree branch rebase feature-pricing
fluree branch merge feature-pricing
```

### Rebase to catch up with upstream

When main has advanced since you branched:

```bash
# Main has new commits that your branch doesn't have
fluree branch rebase my-branch
```

This replays your branch's commits on top of main's current HEAD. Conflict strategies:

| Strategy | Behavior |
|---|---|
| `take-both` (default) | Keep both the source and branch changes |
| `abort` | Stop if any conflicts — let you inspect |
| `take-source` | Source (main) wins on conflict |
| `take-branch` | Branch wins on conflict |
| `skip` | Skip conflicting commits entirely |

```bash
# Rebase with abort on conflict for manual review
fluree branch rebase my-branch --strategy abort

# Rebase where main always wins
fluree branch rebase my-branch --strategy take-source
```

### Compare branches

See what's different between two branches:

```bash
# Query branch for entities not in main
fluree query --ledger mydb:my-branch 'SELECT ?s ?p ?o WHERE {
  ?s ?p ?o .
  FILTER NOT EXISTS {
    SERVICE <fluree:ledger:mydb:main> { ?s ?p ?o }
  }
}'
```

### Time travel across branches

Each branch has its own transaction history. Query any branch at any point in time:

```bash
# Branch state after its 3rd transaction
fluree query --ledger mydb:experiment --at 3 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'
```

## Branch lifecycle

```
create ──→ transact ──→ rebase (if needed) ──→ merge ──→ drop
              ↑                                  │
              └──────── continue working ←───────┘
```

After merging, the branch is still alive. You can:
- Continue transacting on it (for ongoing work)
- Merge again later (only new commits since last merge are copied)
- Drop it when done

## HTTP API

```bash
# Create a branch
curl -X POST 'http://localhost:8090/v1/fluree/branch' \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "dev", "source": "main"}'

# Query a specific branch
curl -X POST 'http://localhost:8090/v1/fluree/query?ledger=mydb:dev' \
  -H "Content-Type: application/sparql-query" \
  -d 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'

# Merge
curl -X POST 'http://localhost:8090/v1/fluree/merge' \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "source": "dev"}'
```

## Best practices

1. **Name branches descriptively** — `weekly-import-2025-04`, `feature-product-tags`, not `test1`
2. **Keep branches short-lived** — Long-lived branches diverge more, making rebase harder
3. **Merge frequently** — Small, frequent merges are easier than large, infrequent ones
4. **Test before merging** — Run validation queries on the branch before promoting
5. **Drop after merging** — Clean up branches you're done with

## Related documentation

- [CLI: branch](../cli/branch.md) — Full command reference
- [Ledgers and Nameservice](../concepts/ledgers-and-nameservice.md) — Branch architecture
- [Time Travel](../concepts/time-travel.md) — Temporal queries on branches
