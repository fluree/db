# Typed values wrong after indexing

Dates, times and some numbers read back as different, implausible values once
the ledger is indexed — `xsd:date` values become `1970-01-01`, `xsd:long`
values become numbers near `-9223372036854775808` — while every other value
in the ledger is correct.

This happens when the values were written by a version predating the fix for
[fluree/db#1987](https://github.com/fluree/db/issues/1987). The commit history
holds the correct values; only the index misreads them. Reindexing with a
fixed version rebuilds it.

## Affected data

- **Values written through SPARQL UPDATE** (`INSERT DATA`, `DELETE`/`INSERT`
  templates) typed `xsd:date`, `xsd:dateTime`, `xsd:time`, `xsd:gYear` and the
  other `xsd:g*` types, `xsd:dayTimeDuration`, `xsd:yearMonthDuration`,
  `xsd:float`, or an integer subtype other than `xsd:integer` (`xsd:long`,
  `xsd:int`, `xsd:short`, `xsd:unsignedInt`, …).
- **`geo:wktLiteral` POINTs written through SPARQL UPDATE.** These read back
  with the right text, but are stored as text rather than as points, so they
  do not match the same point written through JSON-LD or Turtle.
- **Turtle literals whose text is not a value of their datatype**, such as
  `"1990-00-00"^^xsd:date` or `"abc"^^xsd:integer`, whether transacted or
  bulk-imported.

Values written as JSON-LD, well-formed Turtle literals, and SPARQL literals
typed `xsd:integer`, `xsd:decimal`, `xsd:double`, `xsd:boolean`, or a string
type are not affected.

## Symptoms

After an index build, the affected values read back as small offsets from a
type's zero point:

| Datatype | Written | Read after indexing |
|---|---|---|
| `xsd:date` | `2026-09-08` | `1970-01-01` |
| `xsd:dateTime` | `2026-09-08T12:34:56Z` | `<dateTime -9223372036854775807>` |
| `xsd:time` | `12:34:56` | `00:00:00.000005` |
| `xsd:gYear` | `2026` | `0003` |
| `xsd:long` | `20705` | `-9223372036854775804` |
| `xsd:float` | `1.5` | `NaN` |

Before any index build, the values read correctly, but SPARQL-written values
do not match the same literal written through JSON-LD: a query constant such
as `"2026-09-08"^^xsd:date` misses them, and a `DELETE DATA` sent through the
other surface commits without removing anything.

## Remediation

Upgrade, then reindex each affected ledger:

```bash
# CLI
fluree reindex mydb:main

# Or via the admin API
curl -X POST https://<fluree-server>/v1/fluree/reindex \
  -H 'Content-Type: application/json' \
  -d '{"ledger": "mydb:main"}'
```

An application that embeds `fluree-db-api` instead of running the server or
CLI calls `Fluree::reindex` for each ledger after upgrading:

```rust
fluree.reindex("mydb:main", ReindexOptions::default()).await?;
```

An index that already misreads the values keeps misreading them until it is
rebuilt; incremental index builds do not rewrite it. A fixed version reads the
committed SPARQL values as the typed values they denote, so the reindex
restores them, and afterwards they match query constants and retract through
either surface like values written as JSON-LD. Literals that are not values of
their datatype keep their text and datatype. SPARQL-written
`geo:wktLiteral` points read back as points, as they do from every other
surface.

A ledger that has never been indexed needs no reindex: once upgraded, its
values read correctly.

## What changes on upgrade

Two effects appear as soon as a fixed version loads the ledger:

- **Until the reindex, a delete of an affected value that is already indexed
  does not take effect.** The delete names the typed value, and the old index
  holds the value under a different key. The commit succeeds and the reindex
  applies it.
- **A delete that removed nothing before the upgrade can take effect.** Before
  the fix, a `DELETE DATA` sent through one surface for a value written through
  the other committed without removing it. A fixed version reads that commit as
  naming the same value, so once the ledger is loaded from its commits, or
  reindexed, the value is gone with no new write.

## Related documentation

- [Datatypes and typed values](../concepts/datatypes.md)
- [Background indexing](../indexing-and-search/background-indexing.md) —
  novelty and reindex thresholds
