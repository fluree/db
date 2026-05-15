# Conditional Updates (Atomic / Compare-and-Swap Patterns)

Fluree's WHERE/DELETE/INSERT transaction model supports powerful conditional update patterns that depend on the current database state. Every operation runs atomically within a single transaction — the WHERE clause reads current state, and the DELETE/INSERT templates modify it, all as one unit.

This guide covers common patterns for state-dependent updates with both **JSON-LD** and **SPARQL UPDATE** syntax.

## Key Concept: How Conditional Updates Work

```
┌──────────────────────────────────────────────────────┐
│  1. WHERE   — query current state, bind variables    │
│  2. FILTER  — guard: eliminate rows that don't pass  │
│  3. BIND    — compute new values from bound vars     │
│  4. DELETE  — retract matched triples                │
│  5. INSERT  — assert new triples                     │
│                                                      │
│  All steps execute atomically in one transaction.    │
│  If WHERE returns zero rows, nothing happens (no-op).│
└──────────────────────────────────────────────────────┘
```

The WHERE clause runs against the **current** database state. If it matches, the bound variables flow into DELETE (to retract old values) and INSERT (to assert new ones). If WHERE returns zero rows — because a FILTER eliminated them or a pattern didn't match — DELETE is skipped entirely (nothing to retract) and INSERT templates with unbound variables produce zero flakes.

### Two INSERT behaviors

- **INSERT with variables from WHERE** (e.g., `"@id": "?s"`) — conditional. When WHERE returns zero rows, the variable is unbound and the INSERT produces nothing. Use this for CAS, state machines, and guards.
- **All-literal INSERT** (e.g., `"@id": "ex:alice"`) — unconditional. Fires even when WHERE returns zero rows. Use this for "delete-if-exists, always insert" patterns.

---

## 1. Atomic Increment / Decrement

Read the current value, compute a new one, write it back — all in one transaction. Classic use cases: counters, inventory quantities, vote tallies, loyalty points.

### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where": [
    { "@id": "ex:counter", "ex:count": "?old" },
    ["bind", "?new", "(+ ?old 1)"]
  ],
  "delete": { "@id": "ex:counter", "ex:count": "?old" },
  "insert": { "@id": "ex:counter", "ex:count": "?new" }
}
```

### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE { ex:counter ex:count ?old }
INSERT { ex:counter ex:count ?new }
WHERE {
  ex:counter ex:count ?old .
  BIND (?old + 1 AS ?new)
}
```

### Variations

- **Decrement**: `["bind", "?new", "(- ?old 1)"]`
- **Increment by N**: `["bind", "?new", "(+ ?old 50)"]`
- **Multiply**: `["bind", "?new", "(* ?old 2)"]`

---

## 2. Compare-and-Swap (Optimistic Concurrency)

Only update if the current value matches what the client last read. If another transaction changed the data since the read, the WHERE won't match and the update is a no-op. This is the foundation of **optimistic concurrency control**.

### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where":  { "@id": "?s", "ex:version": 1, "ex:price": "?oldPrice" },
  "delete": { "@id": "?s", "ex:version": 1, "ex:price": "?oldPrice" },
  "insert": { "@id": "?s", "ex:version": 2, "ex:price": 24.99 }
}
```

**How it works:**

1. Client reads `ex:item` and sees `version: 1, price: 19.99`
2. Client submits update pinning `version: 1` in WHERE
3. If version is still 1 → match → update succeeds, version bumps to 2
4. If another client already changed version to 2 → no match → no-op

### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE { ?s ex:version 1 . ?s ex:price ?oldPrice }
INSERT { ?s ex:version 2 . ?s ex:price 24.99 }
WHERE {
  ?s ex:version 1 ;
     ex:price ?oldPrice .
}
```

### Application-Level Handling

When a CAS update is a no-op (stale read), the client can detect this by checking whether `t` advanced:

```
response.t == request.t_before  →  stale read, retry with fresh data
response.t  > request.t_before  →  update succeeded
```

---

## 3. State Machine Transitions

Only allow transitions from a valid source state. Invalid transitions (e.g., trying `shipped → delivered` when the current state is `pending`) are silently rejected as no-ops.

### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where":  { "@id": "?order", "ex:status": "pending" },
  "delete": { "@id": "?order", "ex:status": "pending" },
  "insert": { "@id": "?order", "ex:status": "approved" }
}
```

This only fires when the order's current status is exactly `"pending"`. If the status is anything else, the WHERE returns zero rows and nothing changes.

### Multi-Step Chain

Chain transitions across sequential transactions:

```
pending  →  approved  →  shipped  →  delivered
```

Each step is its own transaction, each guarded by the expected source state. If any step finds the state has already moved (e.g., another process approved it), that step is a no-op.

### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE { ?order ex:status "pending" }
INSERT { ?order ex:status "approved" }
WHERE  { ?order ex:status "pending" }
```

---

## 4. Guarded Update (Threshold / Precondition)

Only apply a change when a numeric (or other) precondition is met. Classic use case: prevent overdrafts by checking balance before deducting.

### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where": [
    { "@id": "ex:account", "ex:balance": "?bal" },
    ["filter", "(>= ?bal 100)"],
    ["bind", "?newBal", "(- ?bal 100)"]
  ],
  "delete": { "@id": "ex:account", "ex:balance": "?bal" },
  "insert": { "@id": "ex:account", "ex:balance": "?newBal" }
}
```

**How it works:**

- If `balance >= 100` → FILTER passes → deduction applied
- If `balance < 100` → FILTER eliminates the row → no-op (overdraft prevented)

### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE { ex:account ex:balance ?bal }
INSERT { ex:account ex:balance ?newBal }
WHERE {
  ex:account ex:balance ?bal .
  FILTER (?bal >= 100)
  BIND (?bal - 100 AS ?newBal)
}
```

---

## 5. Atomic Transfer (Double-Entry)

Move a value between two entities atomically in a single transaction. Both the debit and credit happen together — if the guard fails, neither side is modified. Classic use cases: balance transfers, inventory moves between warehouses.

### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where": [
    { "@id": "ex:alice-acct", "ex:balance": "?aliceBal" },
    { "@id": "ex:bob-acct",   "ex:balance": "?bobBal" },
    ["filter", "(>= ?aliceBal 150)"],
    ["bind", "?newAlice", "(- ?aliceBal 150)",
             "?newBob",   "(+ ?bobBal 150)"]
  ],
  "delete": [
    { "@id": "ex:alice-acct", "ex:balance": "?aliceBal" },
    { "@id": "ex:bob-acct",   "ex:balance": "?bobBal" }
  ],
  "insert": [
    { "@id": "ex:alice-acct", "ex:balance": "?newAlice" },
    { "@id": "ex:bob-acct",   "ex:balance": "?newBob" }
  ]
}
```

### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE {
  ex:alice-acct ex:balance ?aliceBal .
  ex:bob-acct   ex:balance ?bobBal .
}
INSERT {
  ex:alice-acct ex:balance ?newAlice .
  ex:bob-acct   ex:balance ?newBob .
}
WHERE {
  ex:alice-acct ex:balance ?aliceBal .
  ex:bob-acct   ex:balance ?bobBal .
  FILTER (?aliceBal >= 150)
  BIND (?aliceBal - 150 AS ?newAlice)
  BIND (?bobBal + 150 AS ?newBob)
}
```

---

## 6. Insert-If-Not-Exists (Conditional Create)

Create an entity only if it doesn't already exist. Useful for preventing duplicate records.

This pattern uses OPTIONAL + FILTER to check for absence. For query-only
absence tests, `["not-exists", { ... }]` is the canonical form (see
[Negation Patterns](../query/jsonld-query.md#negation-patterns)). The
OPTIONAL/BOUND form below is shown here because the example also keeps
`?existing` available to subsequent BIND/DELETE templates when bob does
exist — `not-exists` filters rows out and exposes nothing.

### JSON-LD

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "where": [
    ["optional", { "@id": "ex:bob", "schema:name": "?existing" }],
    ["filter", "(not (bound ?existing))"]
  ],
  "insert": {
    "@id": "ex:bob",
    "schema:name": "Bob",
    "schema:age": 25
  }
}
```

**How it works:**

- If `ex:bob` **does not** exist: OPTIONAL leaves `?existing` unbound → `(not (bound ?existing))` is true → INSERT fires
- If `ex:bob` **exists**: OPTIONAL binds `?existing` → `(not (bound ?existing))` is false → FILTER eliminates the row → INSERT is skipped (zero solution rows = zero template instantiations)

### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX schema: <http://schema.org/>

INSERT { ex:bob schema:name "Bob" ; schema:age 25 }
WHERE {
  OPTIONAL { ex:bob schema:name ?existing }
  FILTER (!BOUND(?existing))
}
```

---

## 7. Capped Accumulator (Increment with Ceiling)

Increment a value but never exceed a maximum. Useful for loyalty points, rate limits, or any bounded counter.

### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where": [
    { "@id": "ex:user", "ex:points": "?pts" },
    ["filter", "(< ?pts 1000)"],
    ["bind", "?new", "(if (> (+ ?pts 150) 1000) 1000 (+ ?pts 150))"]
  ],
  "delete": { "@id": "ex:user", "ex:points": "?pts" },
  "insert": { "@id": "ex:user", "ex:points": "?new" }
}
```

**How it works:**

- If `pts < 1000` → FILTER passes → BIND computes `min(pts + 150, 1000)` → update applied
- If `pts >= 1000` → FILTER eliminates → no-op (already at cap)

### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE { ex:user ex:points ?pts }
INSERT { ex:user ex:points ?new }
WHERE {
  ex:user ex:points ?pts .
  FILTER (?pts < 1000)
  BIND (IF(?pts + 150 > 1000, 1000, ?pts + 150) AS ?new)
}
```

---

## 8. Cascading / Dependent Update (Graph Traversal)

Update one entity based on values from a related entity. The WHERE clause traverses the graph to gather data from multiple nodes.

### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where": [
    { "@id": "ex:order1", "ex:customer": "?cust", "ex:total": "?orderTotal" },
    { "@id": "?cust", "ex:lifetimeSpend": "?ls" },
    ["bind", "?newLs", "(+ ?ls ?orderTotal)"]
  ],
  "delete": { "@id": "?cust", "ex:lifetimeSpend": "?ls" },
  "insert": { "@id": "?cust", "ex:lifetimeSpend": "?newLs" }
}
```

This traverses `order → customer` and accumulates the order total into the customer's lifetime spend — all atomically.

### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE { ?cust ex:lifetimeSpend ?ls }
INSERT { ?cust ex:lifetimeSpend ?newLs }
WHERE {
  ex:order1 ex:customer ?cust ;
            ex:total ?orderTotal .
  ?cust ex:lifetimeSpend ?ls .
  BIND (?ls + ?orderTotal AS ?newLs)
}
```

---

## 9. Batch Conditional Update (Multi-Entity)

Apply the same transformation to every entity matching a pattern. The WHERE clause acts as a filter across the dataset.

### Give All Engineers a 10% Raise

#### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where": [
    { "@id": "?emp", "ex:dept": "engineering", "ex:salary": "?sal" },
    ["bind", "?newSal", "(+ ?sal (/ ?sal 10))"]
  ],
  "delete": { "@id": "?emp", "ex:salary": "?sal" },
  "insert": { "@id": "?emp", "ex:salary": "?newSal" }
}
```

#### SPARQL UPDATE

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE { ?emp ex:salary ?sal }
INSERT { ?emp ex:salary ?newSal }
WHERE {
  ?emp ex:dept "engineering" ;
       ex:salary ?sal .
  BIND (?sal + ?sal / 10 AS ?newSal)
}
```

### Batch Status Change

Approve all pending tasks in one transaction:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where":  { "@id": "?task", "ex:status": "pending" },
  "delete": { "@id": "?task", "ex:status": "pending" },
  "insert": { "@id": "?task", "ex:status": "approved" }
}
```

Only entities with `status: "pending"` are affected; all others remain untouched.

---

## 10. Update with Audit Trail

Change a value and simultaneously record the old value for auditing — in a single atomic transaction.

### JSON-LD

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "where": [
    { "@id": "ex:product", "ex:price": "?oldPrice" },
    ["bind", "?newPrice", "(- ?oldPrice 10)"]
  ],
  "delete": { "@id": "ex:product", "ex:price": "?oldPrice" },
  "insert": {
    "@id": "ex:product",
    "ex:price": "?newPrice",
    "ex:previousPrice": "?oldPrice"
  }
}
```

After the update, the product has both its new price and a record of the previous price.

> **Note:** Fluree's immutable ledger also preserves full history via [time travel](../concepts/time-travel.md), so you can always query any prior state. This pattern is useful when you want the previous value accessible without time-travel queries.

---

## Pattern Summary

| Pattern | WHERE Matches | FILTER | BIND | Effect on No-Match |
|---------|:---:|:---:|:---:|---|
| Atomic increment | Current value | — | Compute new value | No-op |
| Compare-and-swap | Expected value | — | — | No-op (stale read) |
| State machine | Expected state | — | — | No-op (invalid transition) |
| Guarded update | Current value | Threshold check | Compute new value | No-op (guard failed) |
| Atomic transfer | Both accounts | Sender balance | Both new balances | No-op (insufficient) |
| Insert-if-not-exists | OPTIONAL probe | `not bound` | — | No-op (already exists) |
| Capped accumulator | Current value | Below cap | Min(new, cap) | No-op (at cap) |
| Cascading update | Graph traversal | — | Derived value | No-op (path broken) |
| Batch update | All matching | — | Per-entity transform | Only matching entities |
| Audit trail | Current value | — | New value | No-op |

## Best Practices

1. **Prefer pattern matching over FILTER for equality.** Pinning a value in the WHERE pattern (e.g., `"ex:status": "pending"`) is simpler and more efficient than `["filter", "(= ?st \"pending\")"]`.

2. **Check `t` to detect no-ops.** When your application needs to distinguish between "update succeeded" and "condition not met," compare `t` before and after the transaction.

3. **Use BIND for all computed values.** The `["bind", "?var", "(expression)"]` form keeps computation inside the transaction, ensuring atomicity.

4. **Test for absence with the form that fits the update.** Two idioms are
   supported. `["optional", ...]` followed by `["filter", "(not (bound ?var))"]`
   leaves the optional variables in scope for downstream BIND/DELETE/INSERT
   templates — pick this when the update needs the absent variable. For pure
   "does this exist?" guards where no downstream template needs the variable,
   `["not-exists", { ... }]` is the canonical query form (see
   [Negation Patterns](../query/jsonld-query.md#negation-patterns)). Both
   lower to the same planner strategy chooser.

5. **Leverage Fluree's immutability.** Every transaction creates an immutable commit. Even without explicit audit trail patterns, you can always query previous states using time travel. Use the audit trail pattern when you want the old value readily accessible in the current state.

## Related Documentation

- [Update (WHERE/DELETE/INSERT)](update-where-delete-insert.md) — Core syntax reference
- [Insert](insert.md) — Adding new data
- [Upsert](upsert.md) — Replace mode
- [Time travel](../concepts/time-travel.md) — Querying historical states
