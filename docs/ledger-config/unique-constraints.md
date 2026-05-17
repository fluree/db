# Unique Constraints (`f:enforceUnique`)

Fluree supports transaction-time enforcement of property value uniqueness via `f:enforceUnique`. This is complementary to SHACL — it runs independently.

## How it works

Unique constraint enforcement has two parts:

1. **Annotation**: Mark properties as unique by asserting `f:enforceUnique true` on their IRIs in any graph
2. **Activation**: Enable enforcement in the config graph via `f:transactDefaults`

This separation follows the same pattern as SHACL (shapes + config activation) and reasoning (schema + config activation). Annotations alone do nothing — enforcement must be explicitly enabled.

## Step 1: Define unique properties

Assert `f:enforceUnique true` on any property IRI that should enforce uniqueness. These annotations can live in the default graph or any named graph:

```trig
@prefix f: <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/ns/> .

# In the default graph
ex:email f:enforceUnique true .
ex:ssn   f:enforceUnique true .
```

## Step 2: Enable enforcement

Enable unique constraint checking in the config graph:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:transactDefaults [
      f:uniqueEnabled true
    ] .
}
```

When `f:constraintsSource` is omitted, the default graph is used as the annotation source.

### Explicit constraint source

To read annotations from a specific graph:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:transactDefaults [
      f:uniqueEnabled true ;
      f:constraintsSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ]
    ] .
}
```

### Multiple constraint sources

Multiple sources can be specified — all are checked:

```trig
f:transactDefaults [
  f:uniqueEnabled true ;
  f:constraintsSource [
    a f:GraphRef ;
    f:graphSource [ f:graphSelector f:defaultGraph ]
  ] , [
    a f:GraphRef ;
    f:graphSource [ f:graphSelector <http://example.org/schema> ]
  ]
] .
```

### Cross-ledger constraint source

`f:constraintsSource` also supports **cross-ledger references** —
set `f:ledger` on the inner `f:graphSource` to load
`f:enforceUnique` annotations from another ledger at transaction
time. See
[Cross-ledger governance — Cross-ledger constraints](../security/cross-ledger-policy.md#cross-ledger-uniqueness-constraints)
for the end-to-end pattern and failure modes.

## What gets enforced

Once enabled, any transaction that would result in **two or more distinct subjects** holding the same value for a unique property **within the same graph** is rejected.

### Scoping: per-graph

Uniqueness is enforced **per graph**. The same value on the same property is allowed across different named graphs:

```
# Graph A: ex:alice ex:email "alice@example.com" — OK
# Graph B: ex:bob   ex:email "alice@example.com" — OK (different graph)
# Graph A: ex:carol ex:email "alice@example.com" — REJECTED (same graph as alice)
```

### Value identity

Uniqueness is determined by the **storage-layer value representation**, not by RDF strict equality. The uniqueness key is:

```
(graph, predicate, value)
```

where "value" is the internal storage representation (type discriminant + payload).

The enforcement query matches on `(predicate, object)` in the POST index without constraining by datatype or language tag. This means:

- Two values with different datatype IRIs but the **same internal representation** are treated as the same value. For example, `"hello"^^xsd:string` and `"hello"^^ex:customType` both store as the same string value internally, so they conflict.
- Two values with different language tags but the **same string content** conflict, because the language tag is metadata, not part of the value key.
- Two values with **different internal representations** are naturally distinct. For example, `"42"` (stored as a string) and `42` (stored as an integer) do not conflict because they are different value types at the storage layer.

This design matches how humans think about value identity and prevents circumventing uniqueness by attaching a different datatype annotation or language tag.

### Intra-transaction enforcement

Uniqueness is checked after staging, so conflicts within a single transaction are caught:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "@graph": [
    { "@id": "ex:alice", "ex:email": "same@example.com" },
    { "@id": "ex:bob",   "ex:email": "same@example.com" }
  ]
}
```

This transaction is rejected because two subjects assert the same value for a unique property.

### Upsert safety

Upserts that change a value are handled correctly. When an upsert retracts the old value and asserts a new one in the same transaction, the old value is no longer active — no false positive.

### Idempotent re-insert

Re-asserting the same `(subject, property, value)` triple that already exists is allowed. One subject still holds the value — no violation.

## Inline `opts.uniqueProperties` per transaction

In addition to constraints stored in the ledger, a transaction can
supply **inline unique-property declarations** via the
`opts.uniqueProperties` field. The properties are enforced only for
that one transaction; the list itself never persists.

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "@id":      "ex:bob",
  "ex:email": "alice@example.org",
  "opts": {
    "uniqueProperties": [
      "http://example.org/ns/email"
    ]
  }
}
```

Each entry must be a **full IRI** (not a compact prefix form). IRIs
that the ledger's namespace map has never seen are dropped silently
— no instance of the property exists, so the constraint cannot be
violated either way; this matches the same-ledger contract.

Semantics:

- **Additive, not replacing.** Inline properties union with whatever
  `f:constraintsSource` already resolves to (same-ledger or
  cross-ledger). A property is enforced if either source declares it.
- **Transient.** The list is never written into the ledger. The next
  transaction without `opts.uniqueProperties` runs without it.
- **No-config enforcement.** Inline properties drive enforcement
  even on a ledger with no `f:transactDefaults` block — the inline
  list is itself the configuration for this transaction.
- **No audit trail.** Without persistence it's not reconstructable
  which constraints validated which commit. If auditability matters,
  declare the constraint in a `f:constraintsSource` graph instead.

Use cases that fit well: per-tenant constraints layered on top of
operator-set baselines; one-off bulk loads that need extra hygiene
without polluting `#config`; testing a candidate constraint before
committing the annotation.

## Error message

When a uniqueness violation is detected, the transaction fails with an error like:

```
Unique constraint violation: property <http://example.org/ns/email>
  value "alice@example.com" already exists for subject
  <http://example.org/ns/alice> in graph default
  (conflicting subject: <http://example.org/ns/bob>)
```

## Lagging config

Config is read from the **pre-transaction state**. This means:

- Enabling `f:uniqueEnabled` and inserting duplicate values in the **same transaction** will **not** reject the duplicates
- The **next** transaction will enforce the constraint

This is intentional and consistent with all other config graph features.

## Per-graph overrides

Transact defaults use **additive** merge semantics:

- `f:uniqueEnabled` uses monotonic OR — once enabled at the ledger level, per-graph configs cannot disable it
- `f:constraintsSource` is additive — per-graph sources are added to (not replace) ledger-wide sources

Note: additive merge is still subject to override control. If the ledger-wide `f:overrideControl` for `f:transactDefaults` is `f:OverrideNone`, per-graph additions are blocked entirely.

This means a per-graph override can add additional constraint sources but cannot remove ledger-wide ones:

```trig
GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:transactDefaults [
      f:uniqueEnabled true ;
      f:constraintsSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ]
    ] ;
    f:graphOverrides (
      [ a f:GraphConfig ;
        f:targetGraph <http://example.org/graphX> ;
        f:transactDefaults [
          f:constraintsSource [
            a f:GraphRef ;
            f:graphSource [ f:graphSelector <http://example.org/schema> ]
          ]
        ]
      ]
    ) .
}
```

In this example, `graphX` checks unique annotations from **both** the default graph (ledger-wide) and `http://example.org/schema` (per-graph addition).

## Zero cost when not configured

When `f:uniqueEnabled` is not set or is `false`, uniqueness checking is completely skipped — no property scan, no index queries, no overhead. The enforcement code fast-paths out immediately.

## Complete example

```trig
@prefix f:  <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/ns/> .

# 1. Define unique annotations in the default graph
ex:email f:enforceUnique true .

# 2. Enable enforcement in the config graph
GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:transactDefaults [
      f:uniqueEnabled true
    ] .
}
```

After this transaction, the **next** transaction that attempts to give two subjects the same `ex:email` value (within the same graph) will be rejected.
