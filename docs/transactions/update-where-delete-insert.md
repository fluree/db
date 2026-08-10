# Update (WHERE/DELETE/INSERT)

The WHERE/DELETE/INSERT pattern enables targeted updates to existing data in Fluree. This is the most flexible update mechanism, allowing conditional modifications, partial updates, and complex transformations.

## Basic Pattern

The WHERE/DELETE/INSERT pattern has three clauses:

1. **WHERE**: Pattern to match existing data
2. **DELETE**: Triples to retract (using variables from WHERE)
3. **INSERT**: Triples to assert (using variables from WHERE)

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "where": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

This:
1. Finds the current age of ex:alice
2. Deletes that age value
3. Inserts the new age value

## WHERE clause capabilities

The update transaction `where` clause uses the **same pattern grammar as JSON-LD queries**, so you can use rich patterns like OPTIONAL, UNION, FILTER, VALUES, and subqueries.

Two common forms:

- **Node-map**: a single object (simple triple patterns)
- **Array**: a sequence of node-maps plus special forms (recommended for anything beyond basic matching)

Supported special forms inside the `where` array:

- `["filter", <expr>]`
- `["bind", "?var", <expr>]` (may include multiple var/expr pairs)
- `["optional", <pattern>]`
- `["union", <pattern>, <pattern>, ...]`
- `["minus", <pattern>]`
- `["exists", <pattern>]` / `["not-exists", <pattern>]`
- `["values", <values-clause>]`
- `["query", <subquery>]` (subquery can use `select`, `groupBy`, aggregates like `(max ?x)`, etc.)
- `["graph", <graph-name>, <pattern>]`

Expression format for `filter`/`bind` supports either:

- **Data expressions** like `["+", "?x", 1]`, `["and", [">=", "?age", 18], ["=", "?status", "pending"]]`
- **S-expressions** like `"(+ ?x 1)"`

## Graph scoping (named graphs)

JSON-LD update supports writing into **user-defined named graphs** (ingested via TriG or JSON-LD `@graph`) and scoping the update to a named graph.

### Default graph for WHERE/DELETE/INSERT

Use a top-level `graph` key to scope the update to a named graph **as the default graph**:

```json
{
  "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
  "graph": "http://example.org/graphs/audit",
  "where":  { "@id": "ex:event1", "schema:description": "?old" },
  "delete": { "@id": "ex:event1", "schema:description": "?old" },
  "insert": { "@id": "ex:event1", "schema:description": "new" }
}
```

This is the JSON-LD UPDATE analog of SPARQL UPDATE `WITH <iri>`:
- WHERE patterns are evaluated against the named graph
- DELETE/INSERT templates without an explicit graph are written to that named graph

### Writing templates to specific graphs

There are two ways to target graphs in `insert` / `delete` templates:

- **Per-node `@graph`**: attach a graph IRI to a node object (overrides the transaction-level `graph`)

```json
{
  "insert": [
    { "@id": "ex:event1", "@graph": "http://example.org/graphs/audit", "schema:description": "v" }
  ]
}
```

- **Template sugar**: inside `insert` / `delete` arrays, use `["graph", "<graph IRI>", <pattern>]`

```json
{
  "insert": [
    ["graph", "http://example.org/graphs/audit", { "@id": "ex:event1", "schema:description": "v" }]
  ]
}
```

Notes:
- `graph` is a **graph IRI** (a string like `"http://example.org/graphs/audit"`)
- Named-graph reads are available after indexing completes (see `docs/query/datasets.md`)

## Dataset scoping for WHERE (`from` / `fromNamed`)

JSON-LD update reuses the **same dataset keys as JSON-LD query** to control where the `where` clause reads from:

- **`from`**: scopes the default graph used for `where` evaluation (equivalent to SPARQL UPDATE `USING <iri>`)
- **`fromNamed`**: restricts which named graphs are visible to `where` `["graph", ...]` patterns (equivalent to SPARQL UPDATE `USING NAMED <iri>`)

This is why JSON-LD update uses `from` rather than introducing new keywords: it matches the existing JSON-LD query language vocabulary and keeps dataset configuration consistent across read-only queries and updates.

### `from` (WHERE default graph)

When `from` is present, it scopes the `where` clause evaluation without changing where templates write:

- `graph` (if present) controls the default graph for DELETE/INSERT templates (SPARQL UPDATE `WITH`)
- `from` controls the default graph(s) for `where` evaluation (SPARQL UPDATE `USING`)

Notes:
- `from` can be:
  - a string graph IRI (shorthand for `{"graph": "<iri>"}`)
  - an object with `{"graph": "<iri>"}` (or `{"graph": ["<iri1>", "<iri2>"]}`)
  - an array of graph IRIs/selectors (multiple graphs are evaluated as a merged default graph)
- If your `insert` / `delete` templates write into the same graph as the top-level `graph`, you can omit per-template graph selection. The top-level `graph` becomes the default target for templates that don't specify `@graph` (or `["graph", ...]` sugar).
- If you want to write to **multiple** graphs in one update, keep a top-level `graph` as the default (optional) and use per-template `["graph", ...]` for the exceptions.

```json
{
  "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
  "graph": "http://example.org/g2",
  "from": { "graph": "http://example.org/g1" },
  "where": { "@id": "ex:s", "schema:description": "?d" },
  "insert": [{ "@id": "ex:s", "schema:copyFromG1": "?d" }]
}
```

Example: read from one graph, write to two graphs

```json
{
  "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
  "graph": "http://example.org/g2",
  "from": { "graph": "http://example.org/g1" },
  "where": { "@id": "ex:s", "schema:description": "?d" },
  "insert": [
    { "@id": "ex:s", "schema:copyFromG1": "?d" },
    ["graph", "http://example.org/audit", { "@id": "ex:event1", "schema:description": "copied description" }]
  ]
}
```

### `fromNamed` (WHERE named graphs allowlist)

Use `fromNamed` to allow (and optionally alias) named graphs for `where` `["graph", ...]` patterns:

Notes:
- In `where` GRAPH patterns, you can reference the graph by **alias** (e.g. `"g2"`) or by the **graph IRI** (e.g. `"http://example.org/g2"`). Aliases are just convenience names for matching.
- In `insert` / `delete` templates, graph selection is a **write target**. You can use:
  - the full graph IRI (`"http://example.org/g2"`)
  - a compact IRI/term that expands via `@context` (e.g. `"ex:g2"`)
  - the `fromNamed` **alias** (e.g. `"g2"`) for consistency within the same update transaction

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "fromNamed": [
    { "alias": "g2", "graph": "http://example.org/g2" }
  ],
  "where": [
    ["graph", "g2", { "@id": "ex:s", "ex:p": "?o" }]
  ],
  "insert": [["graph", "g2", { "@id": "ex:s", "ex:q": "touched" }]]
}
```

Same example, but with a compacted graph IRI via `@context`:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "fromNamed": [{ "alias": "g2", "graph": "ex:g2" }],
  "where": [["graph", "g2", { "@id": "ex:s", "ex:p": "?o" }]],
  "insert": [["graph", "ex:g2", { "@id": "ex:s", "ex:q": "touched" }]]
}
```

Same idea without an explicit alias (the `fromNamed` string acts as its own identifier):

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "fromNamed": ["ex:g2"],
  "where": [["graph", "ex:g2", { "@id": "ex:s", "ex:p": "?o" }]],
  "insert": [["graph", "ex:g2", { "@id": "ex:s", "ex:q": "touched" }]]
}
```

## Simple Property Update

Update a single property value:

```bash
curl -X POST "http://localhost:8090/v1/fluree/update?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "where": [
      { "@id": "ex:alice", "schema:email": "?oldEmail" }
    ],
    "delete": [
      { "@id": "ex:alice", "schema:email": "?oldEmail" }
    ],
    "insert": [
      { "@id": "ex:alice", "schema:email": "alice.new@example.org" }
    ]
  }'
```

## Multiple Property Updates

Update several properties at once:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:name": "?oldName" },
    { "@id": "ex:alice", "schema:email": "?oldEmail" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:name": "?oldName" },
    { "@id": "ex:alice", "schema:email": "?oldEmail" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:name": "Alice Johnson" },
    { "@id": "ex:alice", "schema:email": "alice.j@example.org" }
  ]
}
```

## Conditional Updates

Only update if condition is met:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:age": "?age" },
    { "@id": "ex:alice", "ex:status": "?status" },
    ["filter", ["and", [">=", "?age", 18], ["=", "?status", "pending"]]]
  ],
  "delete": [
    { "@id": "ex:alice", "ex:status": "?status" }
  ],
  "insert": [
    { "@id": "ex:alice", "ex:status": "approved" }
  ]
}
```

The update only happens if Alice is 18+ and status is "pending".

## Pattern Matching

### Find and Update

Find entities matching a pattern and update them:

```json
{
  "where": [
    { "@id": "?person", "@type": "schema:Person" },
    { "@id": "?person", "ex:status": "pending" }
  ],
  "delete": [
    { "@id": "?person", "ex:status": "pending" }
  ],
  "insert": [
    { "@id": "?person", "ex:status": "active" }
  ]
}
```

This updates ALL people with status="pending" to status="active".

### Relationship-Based Updates

Update based on relationships:

```json
{
  "where": [
    { "@id": "?employee", "schema:worksFor": "ex:company-a" },
    { "@id": "?employee", "ex:salary": "?oldSalary" },
    ["bind", "?newSalary", ["*", "?oldSalary", 1.1]]
  ],
  "delete": [
    { "@id": "?employee", "ex:salary": "?oldSalary" }
  ],
  "insert": [
    { "@id": "?employee", "ex:salary": "?newSalary" }
  ]
}
```

Gives all company-a employees a 10% raise.

## Variable Transformation

Use variables from WHERE in INSERT with transformations:

```json
{
  "where": [
    { "@id": "ex:product-123", "ex:price": "?currentPrice" },
    ["bind", "?newPrice", ["*", "?currentPrice", 0.9]]
  ],
  "delete": [
    { "@id": "ex:product-123", "ex:price": "?currentPrice" }
  ],
  "insert": [
    { "@id": "ex:product-123", "ex:price": "?newPrice" },
    { "@id": "ex:product-123", "ex:previousPrice": "?currentPrice" }
  ]
}
```

Applies 10% discount and saves previous price.

## Partial Updates

Update only specific properties, leaving others unchanged:

**Current State:**
```text
ex:alice schema:name "Alice"
ex:alice schema:email "alice@example.org"
ex:alice schema:age 30
ex:alice schema:telephone "+1-555-0100"
```

**Update Only Age:**
```json
{
  "where": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

**Result:**
```text
ex:alice schema:name "Alice"              (unchanged)
ex:alice schema:email "alice@example.org" (unchanged)
ex:alice schema:age 31                     (updated)
ex:alice schema:telephone "+1-555-0100"   (unchanged)
```

## Adding Properties

Add a property without WHERE (when it might not exist):

```json
{
  "insert": [
    { "@id": "ex:alice", "schema:telephone": "+1-555-0100" }
  ]
}
```

Or conditionally add if missing:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:name": "?name" },
    ["optional", { "@id": "ex:alice", "schema:telephone": "?existingPhone" }],
    ["filter", ["not", ["bound", "?existingPhone"]]]
  ],
  "insert": [
    { "@id": "ex:alice", "schema:telephone": "+1-555-0100" }
  ]
}
```

## Removing Properties

Remove a property entirely:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:telephone": "?phone" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:telephone": "?phone" }
  ]
}
```

No INSERT clause—just deletes.

## Multi-Value Properties

### Replace One Value

```json
{
  "where": [
    { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:email": "alice.new@example.org" }
  ]
}
```

### Add Value

```json
{
  "insert": [
    { "@id": "ex:alice", "schema:email": "alice.work@example.org" }
  ]
}
```

### Remove One Value

```json
{
  "where": [
    { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
  ]
}
```

### Remove All Values

```json
{
  "where": [
    { "@id": "ex:alice", "schema:email": "?email" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:email": "?email" }
  ]
}
```

## Relationship Updates

### Change Relationship

```json
{
  "where": [
    { "@id": "ex:alice", "schema:worksFor": "?oldCompany" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:worksFor": "?oldCompany" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:worksFor": "ex:company-b" }
  ]
}
```

### Add Relationship

```json
{
  "insert": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" }
  ]
}
```

### Remove Relationship

```json
{
  "where": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" }
  ]
}
```

## Complex Updates

### Cascading Updates

Update related entities:

```json
{
  "where": [
    { "@id": "ex:order-123", "ex:status": "?oldStatus" },
    { "@id": "ex:order-123", "ex:items": "?item" },
    { "@id": "?item", "ex:status": "?itemStatus" }
  ],
  "delete": [
    { "@id": "ex:order-123", "ex:status": "?oldStatus" },
    { "@id": "?item", "ex:status": "?itemStatus" }
  ],
  "insert": [
    { "@id": "ex:order-123", "ex:status": "shipped" },
    { "@id": "?item", "ex:status": "shipped" }
  ]
}
```

### Computed Values

Calculate new values based on old:

```json
{
  "where": [
    { "@id": "ex:product-123", "ex:inventory": "?current" },
    { "@id": "ex:product-123", "ex:sold": "?sold" },
    ["bind", "?newInventory", ["-", "?current", "?sold"]]
  ],
  "delete": [
    { "@id": "ex:product-123", "ex:inventory": "?current" }
  ],
  "insert": [
    { "@id": "ex:product-123", "ex:inventory": "?newInventory" }
  ]
}
```

## Editing Blank-Node Structures (Stable `_:fdb-` Ids)

Fluree skolemizes every blank node at insert time into the reserved
`_:fdb-...` label space, and queries return those labels as the node's `@id`.
These ids are **stable**: referencing an `_:fdb-...` id in a later query or
transaction denotes the existing stored node rather than minting a fresh one
(the blank-node-syntax equivalent of RDF 1.1 §3.5 skolem IRIs). This makes
blank-node-rooted structures — OWL restrictions, address objects, RDF lists —
editable in place, without retracting and re-asserting the whole subtree.

Workflow: query for the node's id, then use it as an ordinary `@id`:

```json
{
  "select": "?r",
  "where": { "@id": "ex:ClassA", "ex:restriction": "?r" }
}
```

returns e.g. `"_:fdb-1751612345678901234-0-b0"`, which can then be edited
directly:

```json
{
  "where":  { "@id": "_:fdb-1751612345678901234-0-b0", "owl:someValuesFrom": "?old" },
  "delete": { "@id": "_:fdb-1751612345678901234-0-b0", "owl:someValuesFrom": "?old" },
  "insert": { "@id": "_:fdb-1751612345678901234-0-b0", "owl:someValuesFrom": { "@id": "ex:Gadget" } }
}
```

The parent's reference to the node is untouched and the node keeps its
identity across the edit.

The same ids work in SPARQL, in all of SELECT patterns, `DELETE`/`INSERT`
templates, `DELETE DATA`, `INSERT DATA`, and `DELETE WHERE`:

```sparql
DELETE { _:fdb-1751612345678901234-0-b0 owl:someValuesFrom ?old }
INSERT { _:fdb-1751612345678901234-0-b0 owl:someValuesFrom ex:Gadget }
WHERE  { _:fdb-1751612345678901234-0-b0 owl:someValuesFrom ?old }
```

Notes:

- Only labels beginning with the reserved `_:fdb-` prefix behave this way.
  Ordinary client-authored labels (`_:b0`) keep standard RDF semantics: a
  fresh node per transaction on the write side, and an existential variable
  in SPARQL WHERE patterns. Clients cannot accidentally collide with the
  reserved space — transaction skolemization wraps client labels with a
  transaction id before prefixing `fdb-`.
- Strictly per spec, SPARQL forbids blank nodes in `DELETE` templates and
  treats WHERE-pattern labels as variables; accepting `_:fdb-` ids as
  constants is a deliberate Fluree extension (the same one Virtuoso's
  `nodeID://` refs and Jena's `<_:label>` syntax provide).
- Bulk import mints the same way, into the same reserved space, with the same
  editability. Its ids carry a document scope — `_:fdb-d<14 chars>-<label>`,
  where the scope identifies the source file and `<label>` is the label as
  written in it (`_:fdb-d1t3k9x0abcdef-genid10`). They are `[0-9a-z-]` only,
  so they are writable in SPARQL, Turtle and JSON-LD without escaping.
- Ledgers imported by Fluree **4.1.4 or earlier** hold the older import format, which
  embedded the ledger id and therefore `:` and `/` (`_:fdb-lubm:main-1-genid10`).
  Those ids are still addressable from JSON-LD, but the SPARQL grammar does
  not allow `:` inside a blank-node label, so they cannot be written in SPARQL
  syntax. Re-importing the source produces ids in the current format. There is
  no mixed-format ledger: import requires a fresh ledger, so a ledger's import
  ids are all of one format.
- To find out which document an import id came from, look it up in the
  ledger's `txn-meta` graph. Each source document contributes one triple whose
  subject is its scope — the first 19 characters of the id, `_:fdb-` plus the
  14-character scope:

  ```sparql
  SELECT ?source WHERE {
    GRAPH <urn:fluree:my/ledger:main#txn-meta> {
      _:fdb-d1t3k9x0abcdef <https://ns.flur.ee/db#importSource> ?source
    }
  }
  ```

  `--skolem-namespace` on `fluree create --from` controls the other half of
  the id: by default the ledger id salts the mint, so two ledgers loaded from
  one source tree hold different blank nodes; passing the same namespace to
  both makes them mint identical ids instead. See
  [Blank nodes](../cli/create.md#blank-nodes) for what counts as a document —
  in particular for `.jsonl`/`.ndjson`, where a `@context` switch ends one
  document and starts the next.

## Error Handling

### No Match

If WHERE doesn't match, nothing happens (not an error):

```json
{
  "where": [
    { "@id": "ex:nonexistent", "schema:name": "?name" }
  ],
  "delete": [...],
  "insert": [...]
}
```

Result: No changes, no error.

### Multiple Matches

If WHERE matches multiple entities, all are updated:

```json
{
  "where": [
    { "@id": "?person", "ex:status": "pending" }
  ],
  "delete": [
    { "@id": "?person", "ex:status": "pending" }
  ],
  "insert": [
    { "@id": "?person", "ex:status": "approved" }
  ]
}
```

Updates ALL entities with status="pending".

## Comparison: WHERE/DELETE/INSERT vs Replace Mode

| Feature | WHERE/DELETE/INSERT | Replace Mode |
|---------|---------------------|--------------|
| **Granularity** | Property-level | Entity-level |
| **Other properties** | Preserved | Removed |
| **Conditional** | Yes (with filters) | No |
| **Pattern matching** | Yes | No |
| **Idempotent** | Depends on logic | Yes |
| **Use case** | Partial updates | Complete replacement |

## Best Practices

### 1. Be Specific in WHERE

Good (specific):
```json
{
  "where": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ]
}
```

Risky (might match many):
```json
{
  "where": [
    { "@id": "?person", "schema:age": "?age" }
  ]
}
```

### 2. Always Use Variables

Use variables from WHERE in DELETE:

Good:
```json
{
  "where": [{ "@id": "ex:alice", "schema:age": "?oldAge" }],
  "delete": [{ "@id": "ex:alice", "schema:age": "?oldAge" }]
}
```

Bad (deletes all ages):
```json
{
  "where": [{ "@id": "ex:alice", "schema:age": "?oldAge" }],
  "delete": [{ "@id": "ex:alice", "schema:age": "?age" }]
}
```

### 3. Test Updates

Test on development data first:

```javascript
// Test update logic
const result = await transact(updateQuery);
console.log(`Updated ${result.flakes_retracted} values`);
```

### 4. Use Filters for Safety

Add filters to prevent unintended updates:

```json
{
  "where": [
    "...",
    ["filter", ["and", [">=", "?age", 0], ["<=", "?age", 150]]]
  ],
  "delete": [...],
  "insert": [...]
}
```

### 5. Handle No Matches

Decide if no matches should be an error in your application:

```javascript
const result = await transact(updateQuery);
if (result.flakes_retracted === 0) {
  console.warn('Update matched no entities');
}
```

### 6. Document Complex Updates

Comment complex update logic:

```javascript
// Update inventory after sale completion
// - Decrement stock by sold quantity
// - Update last-sold timestamp
// - Mark as low-stock if below threshold
const updateInventory = { ... };
```

## Performance Considerations

### Index Usage

WHERE clauses use indexes:
- Subject-based: Fast
- Predicate-based: Fast
- Pattern-based: May be slower

### Batch Updates

For many updates, consider batching:

```javascript
const updates = entities.map(e => createUpdateQuery(e));
for (const update of updates) {
  await transact(update);
}
```

## Related Documentation

- [Conditional updates (atomic / CAS patterns)](conditional-updates.md) - Increment, compare-and-swap, state machines, transfers
- [Insert](insert.md) - Adding new data
- [Upsert](upsert.md) - Replace mode
- [Retractions](retractions.md) - Removing data
- [Overview](overview.md) - Transaction overview
- [Query WHERE Clauses](../query/jsonld-query.md) - WHERE pattern syntax
