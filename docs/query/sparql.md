# SPARQL

Fluree provides full support for SPARQL 1.1, the W3C standard query language for RDF. SPARQL enables compatibility with existing RDF tools, knowledge graphs, and semantic web applications.

## Overview

SPARQL (SPARQL Protocol and RDF Query Language) is the industry standard for querying RDF data. Fluree implements SPARQL 1.1, providing full compatibility with SPARQL endpoints and tools.

### Basic SPARQL Query

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
```

### Default Prefixes

When querying via the **CLI**, a ledger's [default context](../concepts/iri-and-context.md#default-context) prefix mappings are injected into SPARQL queries that have no explicit `PREFIX` declarations. The HTTP API defaults this behavior off; pass `?default-context=true` on ledger-scoped query requests to opt in. For example, if the default context includes `{"ex": "http://example.org/ns/"}`, this query works without a `PREFIX` line when default-context injection is enabled:

```sparql
SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
```

If a query includes any `PREFIX` declarations, the default context is not used — you must declare every prefix you need. To explicitly opt out of the default context without defining any real prefix, use `PREFIX : <>`. See [opting out of the default context](../concepts/iri-and-context.md#opting-out-of-the-default-context) for details.

> **Note:** When using `fluree-db-api` directly (embedded), queries must declare their own `PREFIX` declarations. The default context is not injected automatically by the core API. Use `db_with_default_context()` or `GraphDb::with_default_context()` to opt in. See [Default Context](../concepts/iri-and-context.md#default-context) for details.

You can view and manage the default context with `fluree context get/set` or `GET/PUT /v1/fluree/context/{ledger...}`.

## Query Forms

### SELECT Queries

Return variable bindings:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?email
WHERE {
  ?person ex:name ?name .
  ?person ex:email ?email .
}
```

**DISTINCT Results:**

```sparql
SELECT DISTINCT ?name
WHERE {
  ?person ex:name ?name .
}
```

**Reduced Results:**

```sparql
SELECT REDUCED ?name
WHERE {
  ?person ex:name ?name .
}
```

### CONSTRUCT Queries

Generate RDF graphs from query results:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?person ex:displayName ?name .
}
WHERE {
  ?person ex:name ?name .
}
```

See [CONSTRUCT Queries](construct.md) for details.

### ASK Queries

Return boolean indicating if query matches:

```sparql
PREFIX ex: <http://example.org/ns/>

ASK {
  ?person ex:name "Alice" .
}
```

### DESCRIBE Queries

Return RDF description of resources:

```sparql
PREFIX ex: <http://example.org/ns/>

DESCRIBE ex:alice
```

Fluree’s DESCRIBE returns **outgoing triples** for each described resource (equivalent to `CONSTRUCT { ?r ?p ?o } WHERE { VALUES ?r { ... } . ?r ?p ?o }`).

## Basic Graph Patterns

### Triple Patterns

Match RDF triples:

```sparql
?person ex:name ?name .
```

### Multiple Patterns

Combine patterns with AND semantics:

```sparql
?person ex:name ?name .
?person ex:age ?age .
?person ex:email ?email .
```

### Property Paths

SPARQL property paths allow complex traversal patterns in the predicate position of a triple pattern.

#### Supported Operators

| Syntax | Name | Description |
|--------|------|-------------|
| `p+` | One or more | Transitive closure (follows `p` one or more hops) |
| `p*` | Zero or more | Reflexive transitive closure (includes self) |
| `^p` | Inverse | Traverses `p` in reverse direction |
| `p\|q` | Alternative | Matches either `p` or `q` (UNION semantics) |
| `p/q` | Sequence | Follows `p` then `q` (property chain) |

**One or More (`+`):**

```sparql
?person ex:parent+ ?ancestor .
```

**Zero or More (`*`):**

```sparql
?person ex:parent* ?ancestorOrSelf .
```

**Inverse (`^`):**

```sparql
?child ^ex:parent ?parent .
```

This is equivalent to `?parent ex:parent ?child` — it reverses the traversal direction.

Inverse can also be applied to complex paths (sequences and alternatives):

```sparql
?s ^(ex:friend/ex:name) ?o .   -- inverse of a sequence
?s ^(ex:name|ex:nick) ?o .     -- inverse of an alternative
```

- `^(ex:friend/ex:name)` reverses the step order and inverts each step: `(^ex:name)/(^ex:friend)`
- `^(ex:name|ex:nick)` distributes inverse into each branch: `(^ex:name)|(^ex:nick)`
- Double inverse cancels: `^(^ex:p)` simplifies to `ex:p`

**Alternative (`|`):**

```sparql
?person ex:friend|ex:colleague ?related .
```

This produces UNION semantics: results from both `ex:friend` and `ex:colleague` are combined (bag semantics, so duplicates are preserved).

Three-way and inverse alternatives are supported:

```sparql
?s ex:a|ex:b|ex:c ?o .
?s ex:friend|^ex:colleague ?related .
```

Alternative branches can also be sequence chains. For example, to get the name via either the friend or colleague path:

```sparql
?s (ex:friend/ex:name)|(ex:colleague/ex:name) ?name .
```

Branches can freely mix simple predicates, inverse predicates, and sequence chains:

```sparql
?s ex:name|(ex:friend/ex:name)|^ex:colleague ?val .
```

**Sequence (`/`) — Property Chains:**

```sparql
?person ex:friend/ex:name ?friendName .
```

This follows `ex:friend` then `ex:name`, expanding into a chain of triple patterns joined by internal variables. Multi-step chains are supported:

```sparql
?person ex:friend/ex:friend/ex:name ?fofName .
```

Sequence steps can include inverse predicates:

```sparql
?person ^ex:friend/ex:name ?name .
```

This traverses `ex:friend` backwards (finding who links to `?person`), then follows `ex:name` forward.

Sequence steps can also be alternatives. For example, `ex:friend/(ex:name|ex:nick)` distributes the alternative into a union of chains (`ex:friend/ex:name` and `ex:friend/ex:nick`):

```sparql
?person ex:friend/(ex:name|ex:nick) ?label .
```

Multiple alternative steps are supported: `(ex:a|ex:b)/(ex:c|ex:d)` expands to 4 chains. A safety limit of 64 expanded chains is enforced to prevent combinatorial explosion.

**Rules:**

- Transitive paths (`+`, `*`) require at least one variable (both subject and object cannot be constants).
- Sequence (`/`) steps must be simple predicates (`ex:p`), inverse simple predicates (`^ex:p`), or alternatives of simple predicates (`(ex:a|ex:b)`). Transitive (`+`/`*`) and nested sequence modifiers are not allowed inside sequence steps.
- Variable names starting with `?__` are reserved for internal use and will not appear in `SELECT *` (wildcard) output.

#### Not Yet Supported

The following operators are parsed but not yet supported for execution:

| Syntax | Name |
|--------|------|
| `p?` | Zero or one (optional step) |
| `!p` or `!(p\|q)` | Negated property set |

## Query Modifiers

### FILTER

Filter results with conditions:

```sparql
SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
  FILTER (?age > 18)
}
```

**Multiple Filters:**

```sparql
FILTER (?age > 18 && ?age < 65)
FILTER (regex(?name, "^A"))
```

### OPTIONAL

Match optional patterns:

```sparql
SELECT ?name ?email
WHERE {
  ?person ex:name ?name .
  OPTIONAL { ?person ex:email ?email . }
}
```

**Multiple Optionals:**

```sparql
SELECT ?name ?email ?phone
WHERE {
  ?person ex:name ?name .
  OPTIONAL { ?person ex:email ?email . }
  OPTIONAL { ?person ex:phone ?phone . }
}
```

### UNION

Match alternative patterns:

```sparql
SELECT ?name
WHERE {
  { ?person ex:name ?name . }
  UNION
  { ?person ex:alias ?name . }
}
```

### MINUS

Exclude matching patterns:

```sparql
SELECT ?person
WHERE {
  ?person ex:type ex:User .
  MINUS { ?person ex:status ex:Inactive . }
}
```

### BIND

Compute values:

```sparql
SELECT ?name ?nextAge
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
  BIND (?age + 1 AS ?nextAge)
}
```

### VALUES

Provide initial bindings:

```sparql
SELECT ?person ?name
WHERE {
  VALUES ?name { "Alice" "Bob" "Carol" }
  ?person ex:name ?name .
}
```

## Aggregation

### GROUP BY

Group results by variable:

```sparql
SELECT ?category (COUNT(?product) AS ?count)
WHERE {
  ?product ex:category ?category .
}
GROUP BY ?category
```

**Expression-based GROUP BY:**

Group by a computed expression using `(expr AS ?alias)` syntax:

```sparql
SELECT ?initial (COUNT(?name) AS ?count)
WHERE {
  ?person ex:name ?name .
}
GROUP BY (SUBSTR(?name, 1, 1) AS ?initial)
```

The expression is evaluated per row and bound to the alias variable before grouping. Any SPARQL expression is supported, including function calls, arithmetic, and type casts.

### HAVING

Filter grouped results:

```sparql
SELECT ?category (COUNT(?product) AS ?count)
WHERE {
  ?product ex:category ?category .
}
GROUP BY ?category
HAVING (COUNT(?product) > 10)
```

### Aggregation Functions

- `COUNT(?var)` - Count non-null values
- `SUM(?var)` - Sum numeric values
- `AVG(?var)` - Average numeric values
- `MIN(?var)` - Minimum value
- `MAX(?var)` - Maximum value
- `SAMPLE(?var)` - Arbitrary value from group
- `GROUP_CONCAT(?var; separator=",")` - Concatenate values

All aggregate functions support the `DISTINCT` modifier, which eliminates duplicate values before aggregation:

```sparql
SELECT ?category (COUNT(DISTINCT ?customer) AS ?unique_buyers)
                 (SUM(DISTINCT ?price) AS ?unique_price_total)
WHERE {
  ?order ex:category ?category .
  ?order ex:customer ?customer .
  ?order ex:price ?price .
}
GROUP BY ?category
```

**Aggregate result types:** COUNT and SUM of integers return `xsd:integer`. SUM of mixed numeric types and AVG return `xsd:double`.

## Sorting and Limiting

### ORDER BY

Sort results:

```sparql
SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
ORDER BY ?name
```

**Descending:**

```sparql
ORDER BY DESC(?age)
```

**Multiple Sort Keys:**

```sparql
ORDER BY ?last ASC(?first) DESC(?age)
```

### LIMIT

Limit number of results:

```sparql
SELECT ?name
WHERE {
  ?person ex:name ?name .
}
LIMIT 10
```

### OFFSET

Skip results:

```sparql
SELECT ?name
WHERE {
  ?person ex:name ?name .
}
OFFSET 20
LIMIT 10
```

## Datasets

### FROM

Specify default graph:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
FROM <mydb:main>
WHERE {
  ?person ex:name ?name .
}
```

**Multiple Default Graphs:**

```sparql
SELECT ?name
FROM <mydb:main>
FROM <otherdb:main>
WHERE {
  ?person ex:name ?name .
}
```

### FROM NAMED

Specify named graphs:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?graph ?name
FROM NAMED <mydb:main>
FROM NAMED <otherdb:main>
WHERE {
  GRAPH ?graph {
    ?person ex:name ?name .
  }
}
```

Fluree also exposes a built-in named graph inside each ledger for transaction / commit metadata:
- `FROM <mydb:main#txn-meta>` (txn-meta as the default graph), or
- `FROM NAMED <mydb:main#txn-meta>` and `GRAPH <mydb:main#txn-meta> { ... }`

See [Datasets](datasets.md) for details.

## SPARQL Functions

### String Functions

- `STR(?x)` - String value
- `LANG(?x)` - Language tag
- `LANGMATCHES(?lang, ?pattern)` - Language match
- `REGEX(?str, ?pattern)` - Regular expression
- `REPLACE(?str, ?pattern, ?replacement)` - Replace
- `SUBSTR(?str, ?start, ?length)` - Substring
- `STRLEN(?str)` - String length
- `UCASE(?str)` - Uppercase
- `LCASE(?str)` - Lowercase
- `ENCODE_FOR_URI(?str)` - URI encode
- `CONCAT(?str1, ?str2, ...)` - Concatenate

### Numeric Functions

- `ABS(?x)` - Absolute value
- `ROUND(?x)` - Round
- `CEIL(?x)` - Ceiling
- `FLOOR(?x)` - Floor
- `RAND()` - Random number

### Date/Time Functions

- `NOW()` - Current timestamp
- `YEAR(?date)` - Year
- `MONTH(?date)` - Month
- `DAY(?date)` - Day
- `HOURS(?time)` - Hours
- `MINUTES(?time)` - Minutes
- `SECONDS(?time)` - Seconds

### Type Conversion

- `STRDT(?str, ?datatype)` - String to typed literal
- `STRLANG(?str, ?lang)` - String with language
- `DATATYPE(?literal)` - Datatype
- `IRI(?str)` - IRI from string
- `URI(?str)` - URI from string
- `BNODE(?str)` - Blank node

### XSD Datatype Constructors (Casts)

Per W3C SPARQL 1.1 §17.5, XSD constructor functions cast values between datatypes. Invalid casts produce unbound (no binding), not errors.

- `xsd:boolean(?x)` - Cast to boolean (`"true"`, `"1"` → true; `"false"`, `"0"` → false; numeric 0 → false, non-zero → true)
- `xsd:integer(?x)` - Cast to integer (truncates doubles, parses strings)
- `xsd:float(?x)` - Cast to single-precision float
- `xsd:double(?x)` - Cast to double-precision float
- `xsd:decimal(?x)` - Cast to decimal (rejects scientific notation strings)
- `xsd:string(?x)` - Cast to string (canonical form for decimals)

### Logical Functions

- `BOUND(?var)` - Variable is bound
- `IF(?condition, ?then, ?else)` - Conditional
- `COALESCE(?x, ?y, ...)` - First non-null value
- `ISIRI(?x)` - Is IRI
- `ISURI(?x)` - Is URI
- `ISBLANK(?x)` - Is blank node
- `ISLITERAL(?x)` - Is literal
- `ISNUMERIC(?x)` - Is numeric

## Subqueries

Nest queries:

```sparql
SELECT ?person ?name
WHERE {
  ?person ex:name ?name .
  {
    SELECT ?person
    WHERE {
      ?person ex:age ?age .
      FILTER (?age > 18)
    }
  }
}
```

## Service Queries

SERVICE enables cross-ledger queries within Fluree. You can execute patterns against different ledgers within the same query using the `fluree:ledger:` URI scheme.

### Basic Cross-Ledger Query

Query data from another ledger in your dataset:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customer ?name ?total
FROM <customers:main>
FROM NAMED <orders:main>
WHERE {
  ?customer ex:name ?name .
  SERVICE <fluree:ledger:orders:main> {
    ?order ex:customer ?customer ;
           ex:total ?total .
  }
}
```

### Endpoint URI Format

For local Fluree ledger queries, use the `fluree:ledger:` scheme:

| Format | Description | Matches dataset ledger ID |
|--------|-------------|----------------------|
| `fluree:ledger:<name>` | Query ledger with default branch (main) | `<name>:main` |
| `fluree:ledger:<name>:<branch>` | Query specific branch | `<name>:<branch>` |

Where:
- `<name>` is the ledger name **without** the branch (e.g., `orders`, `acme/people`)
- `<branch>` is the branch name (e.g., `main`, `dev`)
- The full dataset ledger ID is always `<name>:<branch>` (e.g., `orders:main`, `acme/people:dev`)

The endpoint is resolved by matching against the full `ledger_id` in the dataset.

**Examples:**

```sparql
SERVICE <fluree:ledger:orders> { ... }         -- matches orders:main
SERVICE <fluree:ledger:orders:main> { ... }    -- matches orders:main (explicit)
SERVICE <fluree:ledger:orders:dev> { ... }     -- matches orders:dev
```

### SERVICE SILENT

Use `SERVICE SILENT` to return empty results instead of failing if the service errors or is unavailable:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?order
WHERE {
  ?person ex:name ?name .
  SERVICE SILENT <fluree:ledger:orders:main> {
    ?order ex:customer ?person .
  }
}
```

If the `orders` ledger is not in the dataset or encounters an error, the query returns results with unbound `?order` values instead of failing.

### Variable Endpoints

SERVICE supports variable endpoints that iterate over available ledgers:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?ledger ?person ?name
FROM NAMED <db1:main>
FROM NAMED <db2:main>
WHERE {
  SERVICE ?ledger {
    ?person ex:name ?name .
  }
}
```

This queries all named ledgers in the dataset.

### Cross-Ledger Join Example

Join customer data from one ledger with their orders from another:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customerName ?productName ?quantity
FROM <customers:main>
FROM NAMED <orders:main>
FROM NAMED <products:main>
WHERE {
  # Get customer from default graph (customers ledger)
  ?customer ex:name ?customerName .

  # Get orders for this customer from orders ledger
  SERVICE <fluree:ledger:orders:main> {
    ?order ex:customer ?customer ;
           ex:product ?product ;
           ex:quantity ?quantity .
  }

  # Get product details from products ledger
  SERVICE <fluree:ledger:products:main> {
    ?product ex:name ?productName .
  }
}
```

### Requirements

- The target ledger must be included in the dataset (via `FROM` or `FROM NAMED` clauses)
- Results are joined with the outer query on shared variables
- SERVICE patterns are executed as correlated subqueries (like EXISTS)

### Remote Fluree Federation

SERVICE supports querying ledgers on **remote Fluree instances** using the `fluree:remote:` scheme. This enables cross-server federation — a single SPARQL query can join data from local ledgers with data from ledgers on other Fluree servers.

#### Remote Endpoint Format

| Format | Description |
|--------|-------------|
| `fluree:remote:<connection>/<ledger>` | Query a ledger on a registered remote server |

Where:
- `<connection>` is a named remote connection registered at build time (maps to a server URL + bearer token)
- `<ledger>` is the ledger ID on the remote server (e.g., `customers:main`, `acme/people:main`)

#### Example: Cross-Server Join

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?localName ?remoteEmail
WHERE {
  ?person ex:name ?localName .
  SERVICE <fluree:remote:acme/customers:main> {
    ?person ex:email ?remoteEmail .
  }
}
```

This queries `?person ex:name` from the local ledger and joins with `?person ex:email` from the `customers:main` ledger on the remote server named `acme`.

#### Multiple Ledgers on the Same Remote Server

A single remote connection gives access to any ledger the bearer token is authorized for:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customer ?orderId ?productName
WHERE {
  SERVICE <fluree:remote:acme/customers:main> {
    ?customer ex:name ?name .
    ?customer ex:id ?customerId .
  }
  SERVICE <fluree:remote:acme/orders:main> {
    ?order ex:customerId ?customerId .
    ?order ex:orderId ?orderId .
    ?order ex:product ?product .
  }
  SERVICE <fluree:remote:acme/products:main> {
    ?product ex:name ?productName .
  }
}
```

#### SILENT with Remote Endpoints

`SERVICE SILENT` works with remote endpoints. If the remote server is unreachable, the connection is not registered, or the bearer token is rejected, the SERVICE block returns empty results instead of failing the query:

```sparql
SERVICE SILENT <fluree:remote:partner/inventory:main> {
  ?item ex:sku ?sku .
}
```

#### Registering Remote Connections

Remote connections are registered at connection build time via the Rust API or server configuration. See [Configuration: Remote connections](../operations/configuration.md#remote-connections) and [Rust API: Remote federation](../getting-started/rust-api.md#remote-federation) for setup details.

#### Datatype Handling

Remote query results preserve their original datatypes. Values returned from a remote server are parsed into the same rich type system used for local data — `xsd:dateTime`, `xsd:date`, `xsd:decimal`, `xsd:integer`, etc. are all stored with their proper typed representations. Custom datatypes (e.g., `http://example.org/myType`) are also preserved: the value is kept as a string with the original datatype IRI retained, so round-tripping and downstream FILTER comparisons on shared custom types work correctly.

#### Limitations (v1)

- **Uncorrelated execution only.** The SERVICE body is sent to the remote server as a standalone query. Parent-row bindings are not injected as VALUES (bound-join). This means a SERVICE block that references variables bound in the outer query will not push those constraints to the remote server — the remote returns all matching rows, and the join happens locally.
- **SPARQL queries only.** Remote SERVICE is available in SPARQL queries. JSON-LD queries do not currently support the `fluree:remote:` scheme.
- **No query cancellation propagation.** If the local query is cancelled, in-flight remote HTTP requests are not aborted.
- **Policy is local only.** The remote server enforces its own policy based on the bearer token. The local server's policy engine does not filter rows returned from a remote SERVICE.

### External SPARQL Endpoints

Federated queries to non-Fluree SPARQL endpoints (e.g., Wikidata, DBpedia) are not yet supported. Only the `fluree:ledger:` (local) and `fluree:remote:` (remote Fluree) schemes are currently available.

## Time Travel

### Point-in-Time Queries

Query data as it existed at a specific time using time specifiers in the `FROM` clause:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?age
FROM <ledger:main@t:100>
WHERE {
  ?person ex:name ?name ;
          ex:age ?age .
}
```

Time specifiers:
- `@t:100` - Transaction number
- `@iso:2024-01-15T10:30:00Z` - ISO 8601 datetime
- `@commit:bafybeig...` - Commit ContentId
- `@t:latest` - Current/latest state

### History Queries

Query all changes (assertions and retractions) within a time range using `FROM...TO` with RDF-star syntax:

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?age ?t ?op
FROM <ledger:main@t:1>
TO <ledger:main@t:latest>
WHERE {
  << ex:alice ex:age ?age >> f:t ?t .
  << ex:alice ex:age ?age >> f:op ?op .
}
ORDER BY ?t
```

The `<< subject predicate object >>` syntax (RDF-star) treats the triple as an entity that can have metadata:
- `f:t` - Transaction time (integer) when the fact was asserted or retracted.
- `f:op` - Operation type as a boolean: `true` for assertions, `false` for retractions. Mirrors `Flake.op` on disk.

**Filter by operation type:**

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?age ?t
FROM <ledger:main@t:1>
TO <ledger:main@t:latest>
WHERE {
  << ex:alice ex:age ?age >> f:t ?t .
  << ex:alice ex:age ?age >> f:op ?op .
  FILTER(?op = false)
}
```

**History with ISO datetime range:**

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?name ?t ?op
FROM <ledger:main@iso:2024-01-01T00:00:00Z>
TO <ledger:main@iso:2024-12-31T23:59:59Z>
WHERE {
  << ex:alice ex:name ?name >> f:t ?t .
  << ex:alice ex:name ?name >> f:op ?op .
}
```

## SPARQL UPDATE

Fluree supports SPARQL 1.1 Update for modifying data using standard SPARQL syntax. SPARQL UPDATE requests use the `application/sparql-update` content type and are sent to the update endpoints.

### INSERT DATA

Insert ground triples (no variables):

```sparql
PREFIX ex: <http://example.org/ns/>

INSERT DATA {
  ex:alice ex:name "Alice" .
  ex:alice ex:age 30 .
  ex:alice ex:email "alice@example.org" .
}
```

**HTTP Request:**

```bash
curl -X POST http://localhost:8090/v1/fluree/update/mydb:main \
  -H "Content-Type: application/sparql-update" \
  -d 'PREFIX ex: <http://example.org/ns/>
      INSERT DATA { ex:alice ex:name "Alice" }'
```

### DELETE DATA

Delete specific ground triples:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE DATA {
  ex:alice ex:email "alice@example.org" .
}
```

### DELETE WHERE

Delete triples matching a pattern:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE WHERE {
  ex:alice ex:age ?age .
}
```

This finds all `ex:age` values for `ex:alice` and deletes them.

### DELETE/INSERT (Modify)

The most powerful form combines WHERE, DELETE, and INSERT clauses:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE {
  ?person ex:age ?oldAge .
}
INSERT {
  ?person ex:age ?newAge .
}
WHERE {
  ?person ex:name "Alice" .
  ?person ex:age ?oldAge .
  BIND(?oldAge + 1 AS ?newAge)
}
```

**Update multiple properties:**

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE {
  ?person ex:name ?oldName .
  ?person ex:status ?oldStatus .
}
INSERT {
  ?person ex:name "Alicia" .
  ?person ex:status ex:Active .
}
WHERE {
  ?person ex:name "Alice" .
  OPTIONAL { ?person ex:name ?oldName }
  OPTIONAL { ?person ex:status ?oldStatus }
}
```

### Dataset scoping for MODIFY (`WITH` / `USING` / `USING NAMED`)

SPARQL UPDATE `MODIFY` supports dataset scoping for named graphs:

- **`WITH <iri>`**: sets the default graph for INSERT/DELETE templates that don’t use an explicit `GRAPH <iri> { ... }` block.
- **`USING <iri>`**: scopes the default graph(s) for `WHERE` evaluation. Repeated `USING` clauses are evaluated as a **merged default graph**.
- **`USING NAMED <iri>`**: scopes which named graphs are visible to `WHERE` `GRAPH <iri> { ... }` patterns. Repeated `USING NAMED` clauses allow multiple named graphs.

### Blank Nodes in INSERT

Blank nodes can be used in INSERT templates to create new entities:

```sparql
PREFIX ex: <http://example.org/ns/>

INSERT DATA {
  _:newPerson ex:name "Bob" .
  _:newPerson ex:age 25 .
}
```

### Typed Literals

Specify datatypes explicitly:

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

INSERT DATA {
  ex:alice ex:birthDate "1990-05-15"^^xsd:date .
  ex:alice ex:salary "75000.00"^^xsd:decimal .
  ex:alice ex:active "true"^^xsd:boolean .
}
```

### Language-Tagged Strings

Insert strings with language tags:

```sparql
PREFIX ex: <http://example.org/ns/>

INSERT DATA {
  ex:alice ex:name "Alice"@en .
  ex:alice ex:name "Alicia"@es .
  ex:alice ex:name "アリス"@ja .
}
```

### SPARQL UPDATE Restrictions

Current restrictions / boundaries:

- **Graph management operations**: `LOAD`, `CLEAR`, `DROP`, `CREATE`, `ADD`, `MOVE`, `COPY` are not yet supported.
- **Template graph variables**: INSERT/DELETE templates support `GRAPH <iri> { ... }` blocks, but `GRAPH ?g { ... }` is not yet supported.
- **DELETE WHERE + GRAPH blocks**: `GRAPH <iri> { ... }` blocks are not yet supported inside `DELETE WHERE { ... }`.
- **SERVICE**: Only local-ledger endpoints of the form `fluree:ledger:<name>[:<branch>]` are supported; arbitrary remote HTTP `SERVICE` endpoints are not supported.
- **Property paths**: Supported in `WHERE` (subject to Fluree capability settings).

### Endpoint Usage

SPARQL UPDATE uses the update endpoints with `Content-Type: application/sparql-update`:

| Endpoint | Description |
|----------|-------------|
| `POST /v1/fluree/update` | Connection-scoped, requires `Fluree-Ledger` header |
| `POST /v1/fluree/update/<ledger...>` | Ledger-scoped, ledger from URL path |

**Examples:**

```bash
# Ledger-scoped (recommended)
curl -X POST http://localhost:8090/v1/fluree/update/mydb:main \
  -H "Content-Type: application/sparql-update" \
  -d 'PREFIX ex: <http://example.org/ns/>
      INSERT DATA { ex:alice ex:name "Alice" }'

# Connection-scoped with header
curl -X POST http://localhost:8090/v1/fluree/update \
  -H "Content-Type: application/sparql-update" \
  -H "Fluree-Ledger: mydb:main" \
  -d 'PREFIX ex: <http://example.org/ns/>
      INSERT DATA { ex:alice ex:name "Alice" }'
```

## Best Practices

1. **Use PREFIX Declarations**: Makes queries readable
2. **Automatic Pattern Optimization**: The query planner automatically reorders patterns for efficient execution using statistics-driven cardinality estimates
3. **Flexible FILTER Placement**: Filters can be placed anywhere in the WHERE clause — the query engine automatically applies each filter as soon as all its required variables are bound
4. **Limit Results**: Use LIMIT for large result sets
5. **Avoid Cartesian Products**: Structure queries to avoid large joins

## Related Documentation

- [JSON-LD Query](jsonld-query.md): Fluree's native query language
- [CONSTRUCT Queries](construct.md): Generating RDF graphs
- [Datasets](datasets.md): Multi-graph queries
- [Output Formats](output-formats.md): Query result formats
- [Transactions](../transactions/overview.md): JSON-LD transaction format
