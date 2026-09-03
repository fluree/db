# GraphQL

A GraphQL endpoint on every ledger, with the schema derived from what the ledger
already contains. There is nothing to register: no `.graphqls` upload, no
resolver code, no build step.

```bash
POST /v1/fluree/graphql/<ledger>
GET  /v1/fluree/graphql/<ledger>?query=...
GET  /v1/fluree/graphql-schema/<ledger>
```

The CLI equivalents are [`fluree graphql`](../cli/graphql.md).

## Three tiers

The schema sharpens as the ledger says more about itself. Each tier is a strict
superset of the one below, and which one applies is decided by what the ledger
contains — never by a config file.

| Tier | Trigger | What it adds |
|------|---------|--------------|
| **Inferred** | nothing | Every class a type, every observed property a nullable list field |
| **Shaped** | a `sh:NodeShape` with `sh:targetClass` | Cardinality, datatypes, enums, reverse fields, documentation, closed types |
| **Curated** | a `graphql:Schema` instance | Which types are published, their names, interfaces, and — opt-in — mutations |

Because the customization layer is **SHACL in the ledger**, one artifact drives
both validation and the API contract. It is versioned with the data and time
travels with `t`.

## What the inferred schema claims, and what it does not

Statistics can say a property has never been seen twice on one subject; they
cannot say it never will be. So tier 1 is deliberately weak:

- **Every field is a nullable list.** Promoting an observation to a single value
  would break clients the first time someone writes a second one. A subject with
  no value for a selected field gets `[]`.
- **`orderBy` accepts only `id`.** Ordering the query's solutions by a
  multi-valued key would repeat subjects rather than order them.
- **No interfaces and no reverse fields.** Both need someone to say which classes
  are abstract and what the other direction is called; tiers 2 and 3 are where
  that is said.
- **Integers map to `Long`, not `Int`.** Fluree stores a plain JSON integer as
  the unbounded `xsd:integer`; GraphQL's `Int` is 32-bit and would fail at read
  time on a large value. `xsd:decimal` maps to a custom `Decimal` rather than
  rounding into `Float`.
- **A property holding both references and literals becomes `String`**, with a
  warning in [`explain`](#explain) — no GraphQL type covers both, and an IRI at
  least renders as its own lexical form.

Policy applies by **pruning**: a class or property your identity cannot read is
absent from introspection, not present-but-empty.

## Naming

The ledger's [default context](../concepts/iri-and-context.md) decides every
name. `ex:name` becomes `name` because `ex:` is a declared prefix. Without a
default context, names fall back to the IRI's last segment and `id` values come
back as absolute IRIs.

1. `graphql:name`, then `sh:name`, then the IRI's local part.
2. `-` and `.` become `_`; a leading digit gets an `_` prefix.
3. On collision, the context prefix qualifies it: `foaf_name`.
4. Declared names are allocated before derived ones, so a name someone wrote
   down cannot be taken by one merely inferred.

Root fields are `person` (one by IRI), `persons`, and `persons_count`.
Pluralisation is naive; `f:graphqlPluralName` overrides it.

## Querying

```graphql
{
  persons(
    where: { name: { RE: "^A" }, age: { GTE: 18 }, knows: { name: { EQ: "Bob" } } }
    orderBy: { id: ASC }
    limit: 20
    offset: 40
  ) {
    id
    name
    knows(orderBy: { name: ASC }, limit: 5) { id name }
  }
}
```

**Filter operators**: `EQ`, `NEQ`, `IN`, `NIN`, `LT`, `LTE`, `GT`, `GTE`, `RE`,
`IRE` (case-insensitive), `NRE`, `NIRE`, `EXISTS`; combinators `AND`, `OR`,
`NOT`. A filter on a multi-valued field holds when **any** value satisfies it.
`EXISTS: false` selects subjects with no value for the field.

On `id` and on reference fields only `EQ`, `IN` and `EXISTS` apply: the
underlying filter language has no IRI comparison, so those lower to `values`
patterns instead.

**Nested arguments** bound how many values *each subject* shows — a different
question from how many rows the query returns, and one the top-level arguments
cannot express. Nested `orderBy` takes a more permissive input type
(`PersonNestedOrder`) than the root one, because it sorts values already fetched
rather than the query's solutions.

**Language selection.** A field whose values carry language tags takes a `lang`
argument: `label(lang: "en,fr")` yields the English values if there are any,
else the French ones — a preference list, not a filter. `"*"` means every value
whatever its tag, which is also what omitting the argument does.

## Shapes

See [`fluree graphql`](../cli/graphql.md#shapes-sharpen-the-schema) for the full
SHACL → GraphQL table. In brief: `sh:maxCount 1` gives a single value,
`sh:minCount ≥ 1` gives `!`, `sh:in` gives an `enum`, `sh:inversePath` gives a
reverse field, and `sh:closed true` drops observed-but-undeclared properties.

`fluree graphql --bootstrap <ledger>` emits the shapes your current schema
implies, as a starting point to edit.

## Curated schemas and mutations

A `graphql:Schema` instance makes the endpoint a deliberate contract: only the
shapes it lists are published, so it does not grow a type the moment someone
writes an instance. It is also the only place mutations can be turned on — see
[`fluree graphql`](../cli/graphql.md#publishing-a-curated-schema).

Every mutation is an ordinary Fluree transaction, so SHACL validation and policy
apply unchanged and a rejected write comes back as a GraphQL error having
written nothing.

## Explain

Pass `?explain=true`, or `extensions: {"explain": true}` in the request body, to
get `extensions.explain`:

```json
{
  "data": { "persons": [ ... ] },
  "extensions": {
    "explain": {
      "tier": "shaped",
      "warnings": [],
      "fields": [
        { "field": "persons", "provenance": "inferred", "query": { "select": ..., "where": ... } }
      ]
    }
  }
}
```

`query` is the JSON-LD query the field lowered to — the one you could have
written by hand, and which [`fluree query`](../cli/query.md) will run. For a
mutation, `transaction` is the transaction that was committed.

**`explain` reports what ran; it is not a dry run.** A mutation still writes.
Silently not writing when the caller asked to see the plan would be the more
surprising behaviour.

## Errors

A GraphQL error is part of the response body, not a transport failure: the
endpoint returns **200** with an `errors` array, because that is what every
standard client reads. Only a malformed request envelope is a 4xx.

```json
{ "data": null, "errors": [{ "message": "...", "extensions": { "code": "UNSUPPORTED_QUERY" } }] }
```

Codes: `GRAPHQL_PARSE_FAILED`, `SCHEMA_ERROR`, `UNSUPPORTED_QUERY`,
`EXECUTION_ERROR`, `EMPTY_SCHEMA`.

## Limits

The endpoint runs inside the server's ordinary query timeout and
client-disconnect cancellation, and all of one document's root fields share a
single handle — they resolve concurrently, so cancelling one has to cancel all
of them.

Two limits bound the document itself, because a derived schema is cyclic
wherever one class references another and so the caller, not the schema, picks
the nesting depth:

| Limit | Default | Setting |
| --- | --- | --- |
| Nesting depth | 15 | `graphql_max_depth` |
| Fields per document | 1000 | `graphql_max_complexity` |

Depth counts field levels — `{ persons { knows { name } } }` is 3 — and
fragments do not add a level. The field budget spans every alias and fragment,
which is what bounds an aliased fan-out. Exceeding either is a `200` with
`errors` and nothing executed. See
[Configuration](../operations/configuration.md#graphql-document-limits).

## Not yet supported

- `where` on a nested field — filtering a nested level means evaluating a
  predicate over values already fetched, a different engine from the one that
  answers the top-level filter. Filter at a root field and traverse instead.
- Nested arguments on a reverse field.
- Several `graphql:Schema` instances in one ledger: the endpoint falls back to
  the shaped schema rather than guessing which to serve.
- `::n` intra-mutation references; link by `id` across two mutations.
- Subscriptions and federation.

## Related

- [`fluree graphql`](../cli/graphql.md) — the CLI, with the full mapping tables
- [Fluree system vocabulary](../reference/vocabulary.md#graphql-schema-vocabulary)
- [JSON-LD Query](jsonld-query.md) — what GraphQL lowers to
