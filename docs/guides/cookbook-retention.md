# Cookbook: Expiring data and trimming history

Fluree keeps every value and every change by default. This guide has working patterns for the three features that shorten a value's life without destroying it:

- **[TTL](#time-to-live)**: a value retracts itself when its lifetime ends.
- **[`f:noHistory`](#history-free-predicates)**: a predicate keeps only its current values in the index.
- **[Targeted removal](#targeted-removal)**: selected values stop being served, now and in history.

None of them is erasure: the commit log still holds the values. To destroy data, see the [permanent erasure cookbook](cookbook-erasure.md). To choose between the four, see [Data lifecycle](../concepts/data-lifecycle.md).

The examples use a ledger named `mydb:main` on a server at `http://localhost:8090`, started with `fluree server run`. Expiry and `f:noHistory` are applied by the server's background retention worker, so they need a running server (or, when embedding Fluree, [your own scheduled passes](../transactions/ttl.md#how-expiration-runs)).

## Time to live

### A session that ends on its own

Put `@ttl` on the value. The lifetime is in seconds, or a duration string such as `"PT30M"`:

```bash
curl -X POST 'http://localhost:8090/v1/fluree/update?ledger=mydb:main' \
  -H 'Content-Type: application/json' \
  -d '{
    "@context": { "ex": "https://example.org/" },
    "insert": {
      "@id": "ex:session-41c7",
      "ex:user": { "@id": "ex:user-8f3a", "@ttl": "PT30M" },
      "ex:token": { "@value": "tok_2b9e", "@ttl": "PT30M" }
    }
  }'
```

Thirty minutes later the retention worker retracts both values in a commit of its own. A current query no longer returns them:

```bash
fluree query mydb:main 'PREFIX ex: <https://example.org/>
SELECT ?token WHERE { ex:session-41c7 ex:token ?token }'
```

The lifetime belongs to each value, not to the subject. Give every value that should expire its own `@ttl`, or use a [transaction default](#a-lifetime-for-everything-in-a-transaction).

History still has the value. This is what an expiry looks like afterwards:

```bash
fluree history https://example.org/session-41c7 -l mydb:main
```

```
┌───┬────┬───────────────────────────┬───────────────────────────────┐
│ t │ op │ predicate                 │ value                         │
├───┼────┼───────────────────────────┼───────────────────────────────┤
│ 1 │ +  │ https://example.org/user  │ https://example.org/user-8f3a │
│ 1 │ +  │ https://example.org/token │ tok_2b9e                      │
│ 2 │ -  │ https://example.org/user  │ https://example.org/user-8f3a │
│ 2 │ -  │ https://example.org/token │ tok_2b9e                      │
└───┴────┴───────────────────────────┴───────────────────────────────┘
```

The retractions at `t` 2 are the expiry: one commit, made by the retention worker.

If the token itself is sensitive, an expiry is not enough: add [`f:noHistory`](#expire-a-value-and-keep-no-history-of-it) to stop serving its history, or [erase](cookbook-erasure.md) it.

### A lease that must be renewed

A lock or a presence flag should disappear unless its holder keeps renewing it. Write the same value again with an explicit lifetime. The deadline restarts from the renewing commit:

```json
{
  "@context": { "ex": "https://example.org/" },
  "insert": {
    "@id": "ex:job-17",
    "ex:lockedBy": { "@id": "ex:worker-3", "@ttl": 60 }
  }
}
```

Send it every 20 seconds, say. If the worker dies, the lock is gone within about a minute and a quarter (the lifetime plus one retention pass).

Two rules to rely on:

- Writing the unchanged value again **without** a lifetime does not renew it. The deadline stays where it was.
- Writing it with `"@ttl": 0` cancels the expiry. The value becomes permanent.

To release the lock early, delete the value as usual. Its schedule ends with it.

### A deadline on the calendar

When the end is a moment, not a duration, use `@expiresAt`:

```json
{
  "@context": { "ex": "https://example.org/" },
  "insert": {
    "@id": "ex:offer-spring",
    "ex:discount": { "@value": 15, "@expiresAt": "2027-03-31T23:59:59Z" }
  }
}
```

Write the offset (`Z` or `+02:00`). A date with no time, or a time with no offset, is refused in this form.

### A lifetime for everything in a transaction

Set `f:ttl` (or `f:expiresAt`) at the top level of the transaction. It applies to every value the transaction newly asserts:

```json
{
  "@context": { "ex": "https://example.org/", "f": "https://ns.flur.ee/db#" },
  "@graph": [
    { "@id": "ex:import-batch-9", "ex:status": "staged", "ex:rows": 1204 },
    { "@id": "ex:import-batch-9-preview", "ex:sample": "first 20 rows" }
  ],
  "f:ttl": "P7D"
}
```

A value in that transaction can opt out with `"@ttl": 0`, or set its own lifetime: the value's own instruction wins over the transaction's.

### A lifetime for every value of a predicate

To make "readings live 30 days" a rule of the ledger, declare it in the config graph. Every later assertion of the predicate gets the lifetime with no change to the writers:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#", "ex": "https://example.org/" },
  "@id": "urn:fluree:mydb:main:config:ledger",
  "@type": "f:LedgerConfig",
  "@graph": "config",
  "f:retentionDefaults": {
    "@id": "urn:fluree:mydb:main:config:retention",
    "@type": "f:RetentionDefaults",
    "f:property": [
      {
        "@id": "urn:fluree:mydb:main:config:retention:reading",
        "@type": "f:RetentionProperty",
        "f:target": { "@id": "ex:reading" },
        "f:ttl": "P30D"
      }
    ]
  }
}
```

Save it as `retention.json` and write it like any other data: `fluree insert -l mydb:main -f retention.json`.

- The rule covers values asserted **after** the config commit. Values already in the ledger are not rescheduled.
- A writer can still override it for one value (`"@ttl": 0` keeps a reading forever) or one transaction.
- To exempt one named graph, add a graph override with `f:ttl 0`; see [retention defaults](../ledger-config/setting-groups.md#retention-defaults).
- If the ledger already has an `f:LedgerConfig` node, add `f:retentionDefaults` to it instead of creating a second one.

### The same thing in SPARQL, Turtle and Cypher

SPARQL and Turtle use an annotation block on the triple:

```sparql
PREFIX ex: <https://example.org/>
PREFIX f:  <https://ns.flur.ee/db#>

INSERT DATA {
  ex:session-41c7 ex:token "tok_2b9e" {| f:ttl 1800 |} .
}
```

Cypher wraps the property value:

```cypher
MATCH (s:Session {id: "41c7"})
SET s.token = fluree.ttl("tok_2b9e", 1800)
```

See [TTL](../transactions/ttl.md#giving-a-value-a-lifetime) for durations, deadlines and bindings on each surface.

### Check that expiry is running

`GET /v1/fluree/retention/{ledger}` shows what the worker knows about a branch:

```bash
curl http://localhost:8090/v1/fluree/retention/mydb:main
```

```json
{
  "ledgerId": "mydb:main",
  "active": true,
  "registration": {
    "activeSinceT": 2,
    "watermarkT": 9,
    "nextDueMs": 1792584000000,
    "pending": 3,
    "lastPassMs": 1792580412000,
    "expiredTotal": 2
  },
  "maintenanceRunning": true,
  "overdue": 0
}
```

| You see | It means |
|---|---|
| `"active": false` | The ledger is not registered for expiry. Either it never wrote a lifetime, or its first one was written moments ago: registration follows the next index build, usually within seconds |
| `pending` above zero, `overdue` zero | Values are scheduled and none is late |
| `overdue` above zero and growing | The worker is behind, or cannot commit. Check `problems` and the server's write load |
| `"maintenanceRunning": false` | This node does not run the worker. In a cluster, ask the leader |
| `problems` present | Something blocks expiry, such as an invalid retention config. Nothing expires until it is fixed |

Expiry is eventual. Expect a value to disappear within its deadline plus one pass (`--retention-interval-secs`, 15 seconds by default) plus the time to commit. Do not use TTL where a value must be unreadable at an exact instant; check the deadline in your query or [policy](cookbook-policies.md) for that.

### TTL under a policy

Putting a lifetime on a value schedules its deletion, so the writer needs permission to *retract* that value, not only to insert it. A role that can add `ex:status` values but not modify them cannot give one a TTL. Expirations themselves are committed by the system identity `urn:fluree:system:retention`.

## History-free predicates

### A counter that keeps no history

A view counter that changes hundreds of times a day adds a superseded value to the index on every change. Mark the predicate `f:noHistory` and the index keeps only its current value:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#", "ex": "https://example.org/" },
  "@id": "urn:fluree:mydb:main:config:ledger",
  "@type": "f:LedgerConfig",
  "@graph": "config",
  "f:retentionDefaults": {
    "@id": "urn:fluree:mydb:main:config:retention",
    "@type": "f:RetentionDefaults",
    "f:property": [
      {
        "@id": "urn:fluree:mydb:main:config:retention:views",
        "@type": "f:RetentionProperty",
        "f:target": { "@id": "ex:views" },
        "f:noHistory": true
      }
    ]
  }
}
```

What changes for readers, once the changes are indexed:

- A current query is unaffected.
- A query as of an earlier point sees the *current* `ex:views` value if it had been asserted by then, and no earlier value. Every other predicate reads as it was at that point.
- A history query over `ex:views` lists only the current value's assertion.
- Nothing is refused, and no permission changes.

Good candidates are values you would never ask "what was this last Tuesday?" about: counters, last-seen timestamps, heartbeats, sync cursors, cached scores. Leave it off anything you audit.

The rule covers the predicate's existing history as well as future changes. It is recorded at the retention worker's next pass and applied as index files are rewritten, so older values fade out over the following index builds. To drop them at once, [reindex](../indexing-and-search/reindex.md): `fluree reindex mydb:main`.

### Expire a value and keep no history of it

One entry can carry both settings. The value expires, and once that is indexed, a historical read no longer returns it:

```json
{
  "@id": "urn:fluree:mydb:main:config:retention:token",
  "@type": "f:RetentionProperty",
  "f:target": { "@id": "ex:token" },
  "f:ttl": "PT30M",
  "f:noHistory": true
}
```

This suits short-lived credentials and one-time codes. The commit log still holds them; that is audit data on disk, not something a query can reach.

### Keep history in one graph

A ledger-wide rule applies in every data graph. To keep the predicate's history in one named graph, override it there:

```trig
@prefix f:  <https://ns.flur.ee/db#> .
@prefix ex: <https://example.org/> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> f:graphOverrides [
    a f:GraphConfig ;
    f:targetGraph ex:audited ;
    f:retentionDefaults [
      f:property [ a f:RetentionProperty ; f:target ex:views ; f:noHistory false ]
    ]
  ] .
}
```

### Turn it off

Set `f:noHistory false` on the entry, or remove the entry. History is kept from that config commit on. The history that was left out while the rule was on does not come back.

## Targeted removal

A targeted removal is for a value that should not have been written, or should no longer be readable: a password pasted into a notes field, another customer's record, a value a court ordered withdrawn. It takes effect when the request returns, on any storage backend, and leaves the commit log intact for audit.

The endpoint is admin-protected: where the server runs with `--admin-auth-mode required`, send an admin token.

### Remove one value

```bash
curl -X POST http://localhost:8090/v1/fluree/remove/mydb:main \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <admin token>' \
  -d '{
    "matching": [
      {
        "subject": "https://example.org/ticket-204",
        "predicate": "https://example.org/notes",
        "object": "temp password is hunter2"
      }
    ]
  }'
```

Selectors take full IRIs; there is no `@context`. The receipt lists every occurrence that was removed:

```json
{
  "requestId": "removal-6dd64149…",
  "scopeT": 41,
  "manifest": [
    { "commit": "bagaybqabciqa3fk…", "slot": 0 },
    { "commit": "bagaybqabciqjszt…", "slot": 0 }
  ],
  "retractedT": 42,
  "generation": 2
}
```

Here the value was still current, so the removal retracted it (`retractedT`). The manifest names both operations that carry the value: the original assertion and that retraction.

From then on:

- A current query does not return the value.
- A historical read that could return it fails with `410` `err:db/HistoryUnavailable`: a query as of an earlier point, a history range, or an export.

A removal selects by exact value. If the same secret was also written with different text, remove that too. There is no dry run: query the fact first, and check your selectors against what comes back.

### What still works in history afterwards

A removal also changes which *other* historical reads the ledger answers. Once a ledger has one, a read of the past is answered when its patterns show it cannot reach a removed value: they name their predicates, or one constant subject.

| Historical read | After a removal of `ex:notes` on `ex:ticket-204` |
|---|---|
| `ex:ticket-204 ex:notes ?v` at an earlier point | Refused (`410`) |
| `?s ex:notes ?v` at an earlier point | Refused: it could return the removed value |
| `ex:ticket-9 ex:notes ?v` at an earlier point | Answered: a different, constant subject |
| `ex:ticket-204 ex:subject ?v` at an earlier point | Answered: a predicate the removal does not cover |
| `ex:ticket-204 ?p ?o` at an earlier point | Refused: every property of the subject includes the removed one |
| `ex:ticket-9 ?p ?o` at an earlier point | Answered: every property, but of a different, constant subject |
| `?s ?p ?o` at an earlier point | Refused: with no subject and no predicate named, it can read anything |
| A graph crawl (`select {"?s": ["*"]}` or `select {"ex:ticket-9": ["*"]}`), a Cypher query, or a query under reasoning, at an earlier point | Refused, for every subject: these read properties their patterns do not name |
| `fluree history ex:ticket-204` over a range the removal covers | Refused. Add `-p <predicate>` to read another of its properties |
| `fluree history ex:ticket-9` over the same range | Answered |
| Any query as of the head | Answered |
| Any query at a point before the removed value was first written | Answered |

"An earlier point" here means one inside the span the removed value was held: from the commit that first asserted it up to the commit that retracted it. A history range is refused when it overlaps that span. Points and ranges after the retraction are answered like any other. Plan for this before the first removal on a ledger whose applications browse history with wildcard queries.

### Remove every value a property ever had

Leave `object` out to select every value of the property on that subject, across its whole history:

```json
{
  "matching": [
    { "subject": "https://example.org/user-8f3a", "predicate": "https://example.org/email" },
    { "subject": "https://example.org/user-8f3a", "predicate": "https://example.org/phone" }
  ]
}
```

`object` must be absent, not `null`: a `null` or a misspelled field is refused, so a typo cannot widen a removal.

### Select a typed value, a reference or a named graph

```json
{
  "matching": [
    { "subject": "https://example.org/user-8f3a",
      "predicate": "https://example.org/birthDate",
      "object": { "@value": "1984-02-11", "@type": "xsd:date" } },

    { "subject": "https://example.org/user-8f3a",
      "predicate": "https://example.org/bio",
      "object": { "@value": "Née Dupont", "@language": "fr" } },

    { "subject": "https://example.org/user-8f3a",
      "predicate": "https://example.org/manager",
      "object": { "@id": "https://example.org/user-11b0" } },

    { "subject": "https://example.org/user-8f3a",
      "predicate": "https://example.org/salary",
      "graph": "https://example.org/hr" }
  ]
}
```

A plain JSON string selects an `xsd:string`, not the same text under another datatype or with a language tag. A JSON number matches the number under any numeric datatype.

### Who can still read the removed value

The commits still hold it. Once a ledger has a removal, its raw content is audit data: commit show, commit export, pack, clone and pull, and storage-proxy reads need the `fluree.ledger.audit.all` or `fluree.ledger.audit.ledgers` [token claim](../security/authentication.md#scope-claims). Ordinary read, write and storage scopes do not include it.

Two things to plan for:

- **Query peers that read through the storage proxy** need the audit claim on their token once a ledger they serve has a removal.
- **Clones and pulls** of the ledger need audit access, and the copy refuses the same history.

### Branches and search indexes

- **Branches.** A removal is made on one branch. A branch created *after* it inherits it. A branch that already existed keeps serving the value in its own history: send the same request to each such branch.
- **Full-text and vector indexes** hold copies of the text they index. A search of an index that reads the removed property is refused (`410`) until the index syncs past the removal, then served again.

### What a removal is not

It is not undone by re-inserting the value, by resetting a branch, or by any transaction. It is also not erasure: backups, clones and the commit log keep the value. When the value has to be destroyed, follow up with a [permanent erasure](cookbook-erasure.md) of the same selectors.

## Gotchas

- **"My value did not expire."** Check [retention status](#check-that-expiry-is-running). The usual causes are a ledger not yet registered (its first lifetime has not been indexed), a node that is not the maintenance owner, or an invalid retention config under `problems`.
- **Expiry on a local CLI ledger.** The CLI runs no background worker. A value written with a lifetime by the CLI alone stays until a server runs over that storage, which then expires whatever is due.
- **Wildcard history queries after a removal.** A removal makes the ledger refuse historical reads that name neither their predicates nor one subject, and every graph crawl of the past; see [What still works in history afterwards](#what-still-works-in-history-afterwards).
- **Bulk import refuses lifetimes.** Import the data without `@ttl` or `f:ttl`, then write the values that expire with a transaction.
- **A TTL on a shared relationship is refused.** If several annotations or parallel Cypher relationships share one triple, expiring it would end all of them.
- **`f:noHistory` is not privacy.** It changes what the index holds. The commit log, and anyone with raw access to it, still has every value.
- **Time-travel results change under `f:noHistory`.** A dashboard that compares "now" with "last week" will read the current value for both. Do not put the rule on a predicate you compare over time.

## Related documentation

- [Data lifecycle](../concepts/data-lifecycle.md): choosing between expiry, history rules, removal and erasure
- [Time to live (TTL)](../transactions/ttl.md): the full reference
- [History retention](../transactions/history-retention.md): `f:noHistory` and targeted removal in detail
- [Retention defaults](../ledger-config/setting-groups.md#retention-defaults): the config schema
- [Cookbook: permanent erasure](cookbook-erasure.md)
- [Cookbook: time travel](cookbook-time-travel.md)
