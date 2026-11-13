# Time to live (TTL)

A value can carry a lifetime. When the lifetime ends, a background retention worker retracts the value with an ordinary commit, exactly as if a client had deleted it. Current queries stop seeing it at that commit. History and the commit log keep it: a query at an earlier `t` still returns it, and the commit that asserted it is unchanged.

TTL is opt-in per ledger. A ledger that never writes a lifetime and never configures one takes the same write and query paths it always has: nothing checks expiry while you transact or query.

## Giving a value a lifetime

A lifetime is named `ttl` and a deadline `expiresAt` everywhere: `@ttl` and `@expiresAt` in JSON-LD shorthand, `f:ttl` and `f:expiresAt` in annotations, transaction defaults and config, and `fluree.ttl` and `fluree.expiresAt` in Cypher. A lifetime is whole seconds or a fixed duration such as `"PT1H"` on every surface.

### JSON-LD: `@ttl` and `@expiresAt`

On a value object (or a node reference), `@ttl` sets a relative lifetime and `@expiresAt` an absolute deadline:

```json
{
  "@context": { "ex": "https://example.org/" },
  "insert": {
    "@id": "ex:device1",
    "ex:status": { "@value": "online", "@ttl": 3600 },
    "ex:lease": { "@id": "ex:slot7", "@ttl": "PT15M" },
    "ex:offer": { "@value": 10, "@expiresAt": "2026-10-21T12:00:00Z" }
  }
}
```

`@ttl` and `@expiresAt` are Fluree extensions, not JSON-LD 1.1 keywords. They are shorthand for a TTL instruction on the value's [annotation](../concepts/edge-annotations.md): the example's status is stored as `"online"` plus an annotation carrying `f:ttl 3600`. You can write that annotation yourself:

```json
{ "@value": "online", "@annotation": { "f:ttl": 3600 } }
```

Use one form per value, not both. The shorthand works in `insert`, in an update's `insert` clause, and in upsert; it is refused in a `delete`.

### SPARQL and Turtle: annotation blocks

TTL uses the existing SPARQL 1.2 / RDF 1.2 annotation syntax with the `f:ttl` or `f:expiresAt` predicate:

```sparql
PREFIX ex: <https://example.org/>
PREFIX f: <https://ns.flur.ee/db#>

INSERT DATA {
  ex:device1 ex:status "online" {| f:ttl 3600 |} .
}
```

In an update the lifetime can come from a binding, like the value itself:

```sparql
PREFIX ex: <https://example.org/>
PREFIX f: <https://ns.flur.ee/db#>

DELETE { ex:device1 ex:status ?old . }
INSERT { ex:device1 ex:status ?new {| f:ttl ?seconds |} . }
WHERE {
  ex:device1 ex:status ?old .
  VALUES (?new ?seconds) { ("online" 3600) }
}
```

A lifetime can also be a duration, `{| f:ttl "PT1H"^^xsd:dayTimeDuration |}`. For a deadline, write `f:expiresAt "2026-10-21T12:00:00Z"^^xsd:dateTime`. Turtle and TriG transactions accept the same annotation blocks. Bulk import does not apply lifetimes: an import holding `@ttl`, `@expiresAt`, `f:ttl` or `f:expiresAt`, in an annotation or a TriG `GRAPH <#txn-meta>` block, is refused. Import the data without them, then write the values that expire with a transaction.

### Cypher: `fluree.ttl` and `fluree.expiresAt`

Wrap a scalar property value:

```cypher
MATCH (d:Device {id: "device1"})
SET d.status = fluree.ttl("online", $seconds)
```

```cypher
CREATE (:Device {id: "device2", mode: fluree.expiresAt("eco", datetime("2026-10-21T12:00:00Z"))})
```

The stored value is the ordinary `"online"` or `"eco"`; the wrapper becomes its TTL annotation. The wrappers are recognized only as the outermost value of a property in `CREATE`, `MERGE ... ON CREATE SET` and `SET`. They are not functions you can call in `RETURN` or `WHERE`.

### A transaction default

A transaction can set a lifetime for every value it newly asserts, in its [txn-meta](overview.md):

```json
{
  "@context": { "ex": "https://example.org/", "f": "https://ns.flur.ee/db#" },
  "@graph": [ { "@id": "ex:reading42", "ex:temp": 21.5 } ],
  "f:ttl": 86400
}
```

A TriG `GRAPH <#txn-meta>` block can set `f:ttl` or `f:expiresAt` the same way. The default applies to new data assertions in that commit only: not to retractions, not to config or txn-meta, and not to values that were already current.

### A property default

Declare a duration for every future assertion of a predicate in the ledger's config graph, with [`f:retentionDefaults`](../ledger-config/setting-groups.md#retention-defaults):

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
        "f:ttl": 86400
      }
    ]
  }
}
```

The policy applies to `ex:reading` values asserted in commits after the config commit. It does not reschedule values asserted earlier. A per-graph override (`f:graphOverrides` / `f:GraphConfig`) can change the duration for one graph; `f:ttl 0` there turns the inherited duration off for that graph.

## Which lifetime applies

For each newly asserted value, the first of these that is present decides:

1. A TTL instruction on the value's own annotation.
2. The transaction's default.
3. The property's effective `f:ttl` for the value's graph.
4. Otherwise, no expiry.

An explicit lifetime of `0` (or `PT0S`) means "no automatic expiry" and overrides the lower levels.

A relative lifetime starts when the write arrived: the commit's receive time (`db:receivedAt`) where it records one, or its commit time when it has none. The server records a receive time on every write that carries an event time (`opts.eventTime`) and on every later write to that ledger, so a backdated event time does not move the start, and a backfill of old data keeps its full lifetime from when it was loaded. A Rust embedder that backdates a commit with `CommitOpts::with_timestamp` must pass `with_received_at` too: without a receive time the lifetime counts from the backdated time, and a value can be due the moment it is written. It never starts at indexing time, and replay or retries do not restart it. A merge or rebase that brings a value onto another branch does not restart it either: the value keeps the lifetime it was written with, including a transaction default, counted from the commit that first wrote it. Deadlines are to the commit, not the instant, and expiry is best-effort, so a deadline can land a few seconds either side. A commit with no recorded time cannot start a relative lifetime: retention reports it in its status instead of guessing.

## Renewing and cancelling

- **Reasserting an unchanged value does not renew it.** Inserting `ex:status "online"` again with no lifetime leaves its deadline where it was.
- **An explicit lifetime on an unchanged value renews it.** The new deadline is measured from the renewing commit. The superseded TTL annotation is retracted in a later maintenance commit.
- **A lifetime of `0` on a current value cancels its schedule.** Retracting the value's TTL annotation does too.
- **Retracting a value ends its schedule.** If the same value is asserted again later, that is a new occurrence with its own lifetime (or none).
- Turning a property default off, or removing it, affects future assertions only. Values already scheduled still expire.

## Accepted values

| Form | Meaning | Rules |
|---|---|---|
| `@ttl: 3600`, `f:ttl 3600`, `fluree.ttl(v, 3600)` | Lifetime in seconds | A non-negative whole number |
| `@ttl: "PT1H"`, `f:ttl "PT1H"^^xsd:dayTimeDuration`, `fluree.ttl(v, "PT15M")` | A fixed day/time duration | Days, hours, minutes and whole seconds. Days are 86,400 seconds. A string such as `"P1DT2H"` is accepted too |
| `@expiresAt: "2026-10-21T12:00:00Z"` | Absolute deadline | RFC 3339 with `Z` or a numeric offset, normalized to UTC, at most microsecond precision |
| `f:expiresAt` typed `xsd:dateTime`, `fluree.expiresAt(v, datetime(...))` | Absolute deadline | The same rules |

These are refused, not guessed at: negative or fractional lifetimes; calendar durations such as `P1M` or `P1Y`; weeks; numeric strings such as `"3600"`; booleans and `null`; date-only values; date-times with no offset or with `-00:00`; and both a lifetime and a deadline on one value. One exception: an `f:expiresAt` written as a typed `xsd:dateTime` literal in SPARQL, Turtle or an `@annotation` is read as any `xsd:dateTime` is, so one with no offset is taken as UTC, not refused. Write the offset. A deadline already in the past is valid: the value expires at the next pass.

A TTL instruction must sit on an annotation of exactly one triple. `f:ttl` written on an ordinary node is refused, as are two TTL instructions for the same value in one transaction.

## Permissions

Setting or renewing a value's lifetime schedules its retraction. So under a policy, a TTL instruction needs permission to *retract the value*, not only to write the annotation. A transaction that may write annotations but not modify `ex:status` cannot put a lifetime on an `ex:status` value.

Expiration commits are made by the system (`f:identity` is `urn:fluree:system:retention` in their txn-meta).

## How expiration runs

The retention worker runs on the node that owns background maintenance: the server's background indexer, or the Raft leader in a cluster. It:

1. Notices a ledger's first TTL input when an index build first reads the commit that carries it, and registers the ledger before that index is published. Until a build reaches it, the worker does not act on the ledger, so a value's expiry can be delayed by up to one indexing cycle. A ledger that never uses TTL adds no work to its index builds.
2. Derives an expiry schedule from the ledger's commits, and persists it with the commit it covers, so a restart resumes where it left off.
3. On each pass (every `--retention-interval-secs`, default 15 seconds), retracts what is due in one commit. The commit is made only if the ledger's head is still the one the batch was checked against. A concurrent write, such as a renewal, sends the batch back to be recomputed.

Expiration is **best-effort and eventual**. A value disappears after its deadline plus up to one interval plus processing time. Expiration commits use transaction capacity like any write, and they wait when the ledger's novelty is near its limit so that ordinary writes come first. If no maintenance owner is running, nothing expires and scheduled values become overdue.

The server always runs one. A Rust application embedding Fluree runs one by calling `Fluree::start_maintenance(housekeeping, Some(retention_options))` on an instance with a background indexer. Without a background indexer, run passes yourself: `Fluree::run_retention(ledger_id)` registers the ledger if its commits carry retention input, then retracts what is due, and is meant to be called on a schedule, such as every 15 seconds (the server's default interval).

### Hosts without a background process

A host that runs only while handling a request, such as a function invoked per event, has no worker between requests. It runs retention in two parts:

1. **Its index builds register ledgers.** Call `Fluree::install_retention_build_hook()` on the instance whose background indexer builds the ledgers' indexes. A build then registers a ledger before its index covers the first commit carrying TTL input, and applies the ledger's `f:noHistory` rules. A ledger that never used retention adds no reads to its builds.
2. **A schedule runs the passes.** Invoke a function on a timer (every minute, for example) that calls `Fluree::run_registered_retention(ledger_id)` for each ledger. A ledger that is not registered costs one configuration read and is skipped. Values expire within about one interval of their deadline.

Do not call `run_retention` on every ledger from a schedule: on a ledger that is not registered, it reads every commit back to the first, looking for retention input, each time it runs. Call it once for a ledger whose TTL input was indexed before the build hook was installed.

Each invocation starts without the schedule in memory: it reads the persisted schedule, catches up from the commits after it, and saves it again when it changed. Two invocations racing are safe: expirations commit only against the head they were checked against, and the registration is updated with a compare-and-swap.

Commits and annotations written before a ledger's first TTL-aware write are not treated as instructions, even if they happen to use `f:ttl`.

### Status

`GET /v1/fluree/retention/{ledger}` reports a branch's retention state:

```json
{
  "ledgerId": "mydb:main",
  "active": true,
  "registration": {
    "activeSinceT": 12,
    "watermarkT": 40,
    "nextDueMs": 1792584000000,
    "pending": 18,
    "lastPassMs": 1792580412000,
    "expiredTotal": 351,
    "retries": 2
  },
  "maintenanceRunning": true,
  "overdue": 0
}
```

`problems` lists anything that blocks or degrades processing, such as an invalid retention config or a commit with no time. While the retention config is invalid, nothing expires. `overdue` is reported by the node that runs the worker.

## Limits

- **Shared relationships.** A TTL on a triple that several annotations or relationships share (parallel Cypher relationships, for example) is refused, because expiring the triple would end all of them. If sharing appears after a value was scheduled, the value is left in place when it comes due, and status reports it.
- **List occurrences** cannot carry annotations, so they cannot carry a TTL.
- **Not erasure.** Expiration retracts current state. History, the commit log and backups still hold the value. To stop serving a value's history, see [History retention](history-retention.md).

See [Setting groups: retention defaults](../ledger-config/setting-groups.md#retention-defaults) for the config schema.
