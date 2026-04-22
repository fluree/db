# fluree memory forget

Retract a memory permanently. Unlike [`update`](update.md), `forget` removes the memory entirely — it stops existing.

```bash
fluree memory forget <ID>
```

Output:

```
Forgotten: mem:fact-01JDXYZ...
```

## When to forget vs. update

| You think… | Use |
|---|---|
| "This was wrong from the start" | `forget` |
| "This was right but the world changed" | [`update`](update.md) |
| "I never want anyone to see this again" | `forget` |

See [Updates and forgetting](../concepts/supersession.md) for more detail.

## Forgetting accidentally-committed secrets

Forgetting removes the memory from the ledger and the next `repo.ttl` export. If a secret value also ended up in **git history**, you need to scrub the history separately — see [Secrets and sensitivity](../concepts/secrets-and-sensitivity.md#what-if-i-slip).
