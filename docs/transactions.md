# Fluree Transactions Guide

## Overview

Fluree supports JSON-LD based transactions for adding, updating, and removing data. This guide explains how to use `insert`, `upsert`, and `update` (and their `!` variants) with clear examples and best practices. It also summarizes common transaction `opts` you can use to control behavior. A short SPARQL Update section is included; for a deeper dive into SPARQL and other Semantic Web standards, see the Semantic Web Developer Guide.

## Staging vs Committing

- Staging APIs return a new staged database value without committing:
  - `insert`, `upsert`, `update`
- Committing APIs perform the operation and persist changes in one step:
  - `insert!`, `upsert!`, `update!`

Use staging when you want to build up a set of changes and commit later. Use the bang variants for a single atomic change.

## JSON-LD Transactions

All operations accept JSON-LD. Keys may be provided in camelCase, kebab-case, or as keywords; they are normalized automatically.

### Insert

Insert adds new nodes. You provide a JSON-LD graph to add.

```clojure
@(fluree/insert db
  {"@context" {"ex" "http://example.org/ns/"}
   "@graph"   [{"@id" "ex:alice"
                "@type" "ex:User"
                "ex:name" "Alice"}]})
```

To commit immediately:

```clojure
@(fluree/insert! conn ledger-id
  {"@context" {"ex" "http://example.org/ns/"}
   "@graph"   [{"@id" "ex:alice" "ex:prop" "?not-a-var"}]})
```

### Upsert

Upsert updates existing nodes by `@id` or inserts if they do not exist. It is equivalent to a long-form `update` with auto-generated `where`/`delete` for each `insert` triple.

```clojure
@(fluree/upsert db
  {"@context" {"ex" "http://example.org/ns/"}
   "@graph"   [{"@id" "ex:alice"
                "ex:nums" [4 5 6]
                "ex:name" "Alice2"}]})
```

`upsert!` commits atomically.

### Update (long-form)

`update` supports three clauses within one JSON-LD request:
- `where` – patterns to match
- `delete` – triples to retract (for matches)
- `insert` – triples to add (for matches)

```clojure
@(fluree/update db
  {"@context" {"ex" "http://example.org/ns/"}
   "where"   [{"@id" "ex:s" "ex:prop" "?not-a-var"}]
   "insert"  [{"@id" "ex:s" "ex:newProp" "new"}]})
```

`update!` commits atomically.

#### Values

Use `values` to provide a table of variable bindings.

```json
"values": [
  ["?name", ["Alice", "Bob"]]
]
```

## Update with variables (in depth)

Updates are powerful because `where`, `delete`, and `insert` can share variables. Typical workflow:
1) Match data with `where` and bind variables
2) Optionally `delete` matched triples (often referencing the same variables)
3) `insert` new triples that reuse bound variables or constants

### Basic variable flow

Rename a person by binding their current name and replacing it:

```json
{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/ns/"},
  "where":   {"@id": "ex:bob", "schema:name": "?name"},
  "delete":  {"@id": "ex:bob", "schema:name": "?name"},
  "insert":  {"@id": "ex:bob", "schema:name": "Robert"}
}
```

- `?name` is bound in `where`, used in `delete`, and replaced in `insert`.

### Subject/predicate/object variables

You can bind any position:
- `@id`: `"@id": "?s"`
- predicate: `"?p": "?o"`
- object: `"ex:prop": "?val"` (or `{"@variable": "?val"}` when needed)

Example: Move (copy) a value between properties for matched subjects:

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "where":  {"@id": "?s", "ex:old": "?val"},
  "insert": {"@id": "?s", "ex:new": "?val"}
}
```

### Multi-pattern where

`where` accepts a sequence of node patterns and higher-order patterns (e.g., optional/union). Each pattern can bind more variables that are available to `delete` and `insert`.

```json
{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/ns/"},
  "where": [
    {"@id": "?s", "@type": "ex:User"},
    {"@id": "?s", "schema:name": "?name"}
  ],
  "insert": {"@id": "?s", "ex:seen": true}
}
```

### Using VALUES to bind variables

`values` let you supply explicit bindings. Two common shapes are supported.

Matrix form (header row + rows of values):

```json
{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/ns/"},
  "values": [["?s", "?p"], ["ex:alice", "schema:age"], ["ex:bob", "schema:age"]],
  "where":  {"@id": "?s", "?p": "?o"},
  "delete": {"@id": "?s", "?p": "?o"},
  "insert": {"@id": "?s", "?p": 23}
}
```

Columnar form (per-variable lists):

```json
{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/ns/"},
  "values": [["?s", ["ex:alice", "ex:bob"]], ["?p", ["schema:age", "schema:age"]]],
  "where":  {"@id": "?s", "?p": "?o"},
  "delete": {"@id": "?s", "?p": "?o"},
  "insert": {"@id": "?s", "?p": 23}
}
```

Notes:
- All rows (or columns) must align by arity; each binding provides values for all listed variables.
- You can also bind typed values with JSON-LD value objects (e.g., `{ "@value": "ex:foo", "@type": "@id" }`).

### Explicit variables in object values

When object values might look like variables, use the explicit form to ensure variable binding:

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "where":  {"@id": "ex:s", "ex:date": {"@variable": "?d"}},
  "insert": {"@id": "ex:s", "ex:log": {"@variable": "?d"}}
}
```

This works regardless of the `objectVarParsing` option.

## Options (opts)

Transaction calls accept an `opts` map (or JSON object) to control behavior. Below are commonly used options.

### Common options

- `identity` / `did`: Identity used for policy evaluation. Typically a DID string.
- `context`: Override or supplement JSON-LD context resolution at transaction time.
- `meta`: Attach arbitrary metadata to the transaction. Either `true` (include all) or a map/selection.
- `message` (commit only): Commit message.
- `tag` (commit only): Commit tag or label.
- `author` (commit only): Author identity.
- `private` (commit only): Marks commit as private (string identifier).
- `policy-values`: Data made available to policy evaluation.
- `max-fuel`: Limit on computational resources available to the operation.
- `format`: For `insert`/`upsert`, can be `:turtle` to accept Turtle input (default is JSON-LD). For `update`, use `{:format :sparql}` to parse SPARQL Update.

Example with commit metadata:

```clojure
@(fluree/update! conn ledger
  {"@context" {"ex" "http://example.org/"}
   "where"   {"@id" "ex:s"}
   "insert"  {"@id" "ex:s" "ex:status" "active"}}
  {:message "Activate subject" :tag "release-1" :author "did:example:123"})
```

Example with identity and policy values:

```clojure
@(fluree/insert! conn ledger
  {"@context" {"ex" "http://example.org/"}
   "@graph"   [{"@id" "ex:s" "ex:role" "user"}]}
  {:identity "did:key:z6M..." :policy-values {:env "prod"}})
```

### Object variable parsing (advanced)

Fluree supports an option to control how bare strings that look like variables are parsed in object positions.

- Key: `objectVarParsing` (JSON) or `:object-var-parsing` (Clojure)
- Behavior:
  - When `true`: a bare string like `"?x"` is parsed as a variable in object position.
  - When `false`: a bare string like `"?x"` is treated as a literal string.
  - Explicit `{"@variable": "?x"}` is always honored as a variable regardless of this flag.
  - Variable parsing for `@id` and predicate keys is unaffected (keys are always parsed as variables when they begin with `?`).
- Defaults:
  - Insert / Upsert: `false`
  - Update: `true`

Literal string match in WHERE when disabled:

```json
{
  "@context": {"ex": "http://example.org/"},
  "opts": {"objectVarParsing": false},
  "select": ["?s"],
  "where": [
    {"@id": "?s", "ex:prop": "?not-a-var"}
  ]
}
```

## SPARQL Update (brief)

Fluree supports SPARQL 1.1 Update. You can pass SPARQL strings to `update`/`update!` using `{:format :sparql}`.

```clojure
@(fluree/update db
  "PREFIX ex: <http://example.org/>
   INSERT DATA { ex:alice ex:status \"active\" . }"
  {:format :sparql})
```

For a detailed, standards-focused guide (Turtle, SPARQL, SHACL, OWL), see: `docs/semantic_web_guide.md`.

## Best Practices

- Prefer JSON-LD transactions for Fluree-native ergonomics and consistent context handling.
- Use `insert!`/`upsert!`/`update!` for atomic commits; use staging variants when batching multiple changes.
- Be explicit with variables when object values can resemble variables; use `{"@variable": "?x"}` or configure `objectVarParsing` when needed.
- Define `@context` in transactions and queries for readable IRIs.
- When using Upsert, ensure your `@id` is present to match the intended subject.
