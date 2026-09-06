# fluree graphql

Query a ledger through GraphQL.

## Usage

```bash
fluree graphql [LEDGER] [DOCUMENT]
fluree graphql --schema [LEDGER]
```

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to the active ledger) |
| `[DOCUMENT]` | Inline GraphQL document |

| Option | Description |
|--------|-------------|
| `-l, --ledger <NAME>` | Ledger name; explicit alternative to the positional argument |
| `-e, --expr <DOC>` | Inline GraphQL document |
| `-f, --file <FILE>` | Read the document from a file |
| `--variables <JSON>` | Query variables, as a JSON object |
| `--operation <NAME>` | Which operation to run, when the document defines several |
| `--schema` | Print the derived schema as SDL instead of running a query |
| `--bootstrap` | Print SHACL shapes derived from the schema, as a starting point for refining it |

With one positional argument, a value starting with `{`, `query`, `mutation` or
`fragment` is treated as the document; anything else is a ledger name. With no
document argument, the document is read from `-e`, `-f`, or stdin.

## Description

There is nothing to register. The schema is **derived from the ledger's own
data**: every class with instances becomes a type, every property observed on
those instances becomes a field, and each type gets three root fields.

For a ledger holding `ex:Person` subjects with `ex:name`, `ex:age` and
`ex:knows`:

```graphql
type Person {
  id: ID!
  name: [String!]
  age: [Long!]
  knows(where: PersonFilter, limit: Int, offset: Int, orderBy: PersonOrder): [Person!]
}

type Query {
  person(id: ID!): Person
  persons(where: PersonFilter, limit: Int, offset: Int, orderBy: PersonOrder): [Person!]
  persons_count(where: PersonFilter): Int!
}
```

The ledger's [default context](context.md) decides the names: `ex:name` becomes
`name` because `ex:` is a declared prefix. Without a default context, names fall
back to the IRI's last segment and `id` values come back as full IRIs. Set one
with `fluree context set` for readable output.

### Shapes sharpen the schema

Write SHACL shapes to the ledger and the schema follows them — there is still
nothing GraphQL-specific to write:

```json
{
  "@id": "ex:PersonShape",
  "@type": "sh:NodeShape",
  "sh:targetClass": { "@id": "ex:Person" },
  "sh:description": "A person we know about.",
  "sh:property": [
    { "sh:path": { "@id": "ex:name" }, "sh:datatype": { "@id": "xsd:string" },
      "sh:minCount": 1, "sh:maxCount": 1, "sh:order": 1,
      "sh:description": "The person's full name." },
    { "sh:path": { "@id": "ex:status" }, "sh:maxCount": 1,
      "sh:in": { "@list": [{ "@id": "ex:Active" }, { "@id": "ex:Retired" }] } },
    { "sh:path": { "sh:inversePath": { "@id": "ex:employer" } },
      "sh:class": { "@id": "ex:Person" }, "sh:name": "colleagues" }
  ]
}
```

| SHACL | GraphQL |
|-------|---------|
| `sh:maxCount 1` | a single value instead of a list |
| `sh:minCount ≥ 1` | `!` |
| `sh:datatype` | the scalar |
| `sh:class` / `sh:node` | the object type (`Node` if that class is not exposed) |
| `sh:in` | an `enum`, when every member yields a usable name |
| `sh:inversePath` | a reverse field |
| `sh:name` | the type's or field's name |
| `sh:description` | its documentation |
| `sh:order` | field order in the SDL |
| `sh:closed true` | observed-but-undeclared properties are dropped from the type |
| `sh:deactivated true` | the shape is ignored |

A shape describes what the ledger is *meant* to hold, so a shaped class gets a
type even before its first instance — a client has to be able to see the schema
it is about to write against. An open shape keeps observed properties it did not
declare, as inferred nullable lists, so a partial shape stays partial. A property
shape that declares a path but nothing about its values takes the cardinality
from the shape and the type from the data.

`sh:in` only becomes an enum when every member is usable: all IRIs or all
literals, each yielding a distinct GraphQL name. Otherwise the field keeps its
datatype — a silently shrunken domain would be worse than no enum. Enum values
come back as member names and filter by them; the underlying IRI travels with
the member.

Property shapes whose `sh:path` is a sequence or alternative path are skipped:
they name no single predicate to read or write. Reverse fields accept no nested
arguments.

### Getting started with shapes

`--bootstrap` writes the shapes the current schema implies, so you do not have
to start from a blank file:

```bash
fluree graphql --bootstrap mydb > shapes.json
# edit shapes.json: add sh:minCount / sh:maxCount / sh:in / sh:name …
fluree insert mydb -f shapes.json
```

The output is deliberately weak: paths and value types, and nothing else. No
cardinalities, no `sh:closed`, no `sh:in` — those are the claims statistics
cannot justify, and guessing at them would put words in your mouth. It is also
lossy in one direction: several XSD types map to `String`, so a `rdf:langString`
property comes back as `xsd:string`.

Nothing is written. **Shapes activate SHACL validation for their class**, so
applying them is a decision to make deliberately, after reading what you are
about to apply.

### What the derived schema does and does not claim

Statistics can say a property has never been seen twice on one subject; they
cannot say it never will be. So every field is a **nullable list** — promoting
that observation to a single value would break clients the first time someone
writes a second value. A subject with no value for a selected field gets `[]`.

For the same reason `orderBy` accepts only `id`: ordering by a multi-valued
property would multiply subjects rather than order them.

Integers map to a custom `Long` scalar, not GraphQL's `Int`. Fluree stores a
plain JSON integer as `xsd:integer`, which is unbounded, and `Int` is 32-bit.

Policy applies by **pruning**: a class or property your identity cannot read is
absent from introspection, not present-but-empty.

### Filtering

`where` follows the conventions GraphQL-over-RDF clients already know:

```graphql
{
  persons(
    where: {
      name: { RE: "^A" }
      age: { GTE: 18 }
      knows: { name: { EQ: "Bob" } }
      OR: [{ age: { LT: 18 } }, { age: { GT: 65 } }]
    }
    orderBy: { id: ASC }
    limit: 20
    offset: 40
  ) {
    id
    name
  }
}
```

Operators: `EQ`, `NEQ`, `IN`, `NIN`, `LT`, `LTE`, `GT`, `GTE`, `RE`, `IRE`,
`NRE`, `NIRE`, `EXISTS`; combinators `AND`, `OR`, `NOT`. A filter on a
multi-valued field holds when **any** value satisfies it. `EXISTS: false` on a
field selects subjects that have no value for it.

On `id` and on reference fields, only `EQ`, `IN` and `EXISTS` apply — the
underlying filter language has no IRI comparison.

## Examples

```bash
# Print the derived schema
fluree graphql --schema mydb

# Run a query
fluree graphql mydb '{ persons { id name knows { id name } } }'

# From a file, with variables
fluree graphql mydb -f query.graphql --variables '{"minAge": 21}'

# From stdin, against the active ledger
echo '{ persons_count }' | fluree graphql
```

Output is the GraphQL response envelope:

```json
{
  "data": {
    "persons": [
      { "id": "ex:alice", "name": ["Alice"], "knows": [{ "id": "ex:bob", "name": ["Bob"] }] }
    ]
  }
}
```

A document that fails to parse, validate, or lower produces an `errors` array in
the same envelope and a non-zero exit code.

### Paging a nested collection

A list-valued object field takes its own `limit`, `offset` and `orderBy`:

```graphql
{
  person(id: "ex:alice") {
    name
    knows(orderBy: { name: ASC }, limit: 5) { id name }
  }
}
```

These bound how many values **that subject** shows, which the root arguments
cannot express — the root `limit` bounds people, this one bounds each person's
friends.

Nested `orderBy` takes a different input type from the root one
(`PersonNestedOrder` rather than `PersonOrder`) and accepts more keys: it sorts
values already fetched for one subject, so a multi-valued field is fine there
and sorts by its first value. At the root, a multi-valued sort key would repeat
subjects rather than order them, so only single-valued fields are offered.

### Publishing a curated schema

A `graphql:Schema` instance turns the derived schema into a deliberate API
contract: only the shapes it lists are published, so the endpoint does not grow
a type the moment someone writes an instance.

```json
{
  "@id": "ex:PublicApi",
  "@type": "graphql:Schema",
  "graphql:publicShape": { "@id": "ex:PersonShape" },
  "graphql:protectedShape": { "@id": "ex:CompanyShape" },
  "graphql:privateShape": { "@id": "ex:AuditShape" }
}
```

| Exposure | Becomes a type | Root query fields | A reference to it |
|----------|----------------|-------------------|-------------------|
| `graphql:publicShape` | yes | yes | the type |
| `graphql:protectedShape` | yes | no | the type |
| `graphql:privateShape`, or unlisted | no | no | `Node` — the IRI, without a type |

Other terms on a schema or a shape:

| Term | Effect |
|------|--------|
| `graphql:name` | the type's name; beats `sh:name` |
| `graphql:isInterface` | the class is abstract: an `interface`, implemented by the classes beneath it in the RDFS hierarchy |
| `f:graphqlPluralName` | the root list/count field name |
| `f:graphqlEnableMutations` | opt in to the write surface |
| `f:graphqlIriBase` | the namespace new subjects are minted under |

A ledger with several `graphql:Schema` instances falls back to the shaped
schema: serving one of them arbitrarily would be worse than a defined answer.

### Mutations

Off unless a `graphql:Schema` says `f:graphqlEnableMutations: true` — a schema
derived from whatever a ledger happens to contain should never become a write
surface by accident. Each published type then gets three fields:

```graphql
mutation {
  create_Person(input: { name: "Alice", employer: "ex:acme" }) { id name }
  update_Person(ids: ["ex:alice"], set: { nickname: null }) {
    affected_count
    affected_objects { id nickname }
  }
  delete_Person(ids: ["ex:bob"]) { affected_count }
}
```

- `create_` mints an IRI under `f:graphqlIriBase` unless you pass an explicit
  `id`. There is no default base: a wrong guess writes identifiers that cannot
  be un-minted.
- `update_` replaces exactly the properties `set` lists and leaves the rest.
  A `null` clears one.
- `delete_` retracts every fact about each subject, scoped to the type the field
  names — `delete_Person` on a Company's IRI is a no-op, not a wipe.
- `id` cannot be changed: renaming a subject is a create and a delete.
- A reference is written as the target's `id`. Creating one as a side effect
  would write an object you never named.

Every mutation is an ordinary Fluree transaction, so SHACL validation and policy
apply unchanged; a rejected write comes back as a GraphQL error having written
nothing. Mutations run serially, so a later field in the same document sees an
earlier one's write.

## Not yet supported

- **`where` on a nested field.** Filtering a nested level means evaluating a
  predicate over values already fetched, which is a different engine from the
  one that answers the top-level filter. Filter at a root field and traverse to
  it instead.
- **Mutations.** Read-only for now.
- **Nested arguments on a reverse field.** The hydration IR carries per-value
  modifiers on forward properties only.
- **Several `graphql:Schema` instances in one ledger.** The endpoint falls back
  to the shaped schema; per-schema routing is a follow-up.
- **`::n` intra-mutation references.** Link by `id` across two mutations
  instead.

## Related

- [`query`](query.md) — JSON-LD, SPARQL, and Cypher queries
- [`context`](context.md) — the default context that decides GraphQL names
- HTTP: `POST /v1/fluree/graphql/<ledger>` and `GET /v1/fluree/graphql-schema/<ledger>`
