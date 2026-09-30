# Datasets and Named Graphs

Fluree supports **SPARQL datasets**, allowing queries to span multiple graphs simultaneously. This enables complex data integration scenarios where data from different sources or time periods needs to be queried together.

## SPARQL Datasets

A **dataset** in SPARQL is a collection of graphs used for query execution:

- **Default Graph**: The primary graph for triple patterns without GRAPH clauses
- **Named Graphs**: Additional graphs identified by IRIs, accessible via GRAPH clauses

### Dataset Structure

```sparql
# Dataset with one default graph and two named graphs
FROM <ledger:main>           # Default graph
FROM NAMED <ledger:archive>  # Named graph
FROM NAMED <ledger:staging>  # Another named graph
```

## Named Graphs

In SPARQL, **named graphs** are additional graphs (identified by IRIs) that participate in query execution and are accessed via `GRAPH <iri> { ... }`.

In Fluree, named graphs are used in several ways:

- **Multi-graph execution (datasets)**: `FROM NAMED <...>` identifies additional **graph sources** (often other ledgers or non-ledger graph sources) that you can reference with `GRAPH <...> { ... }`.
- **System named graphs**: Fluree provides two built-in named graphs:
  - **`txn-meta`** (`#txn-meta`): commit/transaction metadata, queryable via the `#txn-meta` fragment (e.g., `<mydb:main#txn-meta>`)
  - **`config`** (`#config`): ledger-level configuration (policy, SHACL, reasoning, uniqueness constraints). See [Ledger configuration](../ledger-config/README.md).
- **User-defined named graphs**: Fluree supports ingesting data into user-defined named graphs using TriG format. These graphs are identified by their IRI and can be queried using the structured `from` object syntax with a `graph` field.

### HTTP endpoints and default graph behavior

Fluree exposes two query styles over HTTP:

- **Connection-scoped** (`POST /query`): the ledger(s) and graphs are identified by `from` / `fromNamed` (JSON-LD) or `FROM` / `FROM NAMED` (SPARQL). This is the dataset path and supports multi-ledger datasets. There is no target ledger, so a graph is named together with its ledger (`<mydb:main#http://example.org/ns/archive>`). A keyword, or an IRI that cannot be a ledger address (`http://example.org/g`), names no ledger and is refused with a 400; an IRI that could be one (`urn:g1`, read as ledger `urn`, branch `g1`) is looked up as a ledger.
- **Ledger-scoped** (`POST /query/{ledger}`): the ledger is fixed by the URL. The request may still select a **named graph inside that ledger**:
  - JSON-LD: `"from": "default"`, `"from": "txn-meta"`, `"from": "config"`, or `"from": "<graph IRI>"`
  - SPARQL: `FROM <default>`, `FROM <txn-meta>`, `FROM <config>`, `FROM <graph IRI>`, and `FROM NAMED <graph IRI>`
  - The ledger's own address, in any spelling (`mydb`, `mydb:main`, `urn:fluree:mydb:main`), names its default graph, and with a graph (`mydb:main#config`, `mydb:main#<graph IRI>`) that graph. A graph the ledger does not have is a 404 (`err:db/GraphNotFound`).
  - `GRAPH <iri> { ... }` and `GRAPH ?g { ... }` resolve the ledger's registered user named graphs **without** an explicit `FROM NAMED` (the reserved `#txn-meta` / `#config` graphs stay private). Supplying `FROM NAMED` still narrows resolution to exactly the graphs listed.

If the request body tries to target a different ledger than the one in the URL, the server rejects it with a "Ledger mismatch" error.

### The ledger's own address in a graph position

Wherever a query or an update names a graph of the ledger it reads or writes, one table decides which graph the name is. That covers `GRAPH <iri>` in a query or in an update's `WHERE`, an update template's `GRAPH <iri>`, an `INSERT DATA` / `DELETE DATA` quad, a TriG block (in a transaction or a bulk import), `WITH`, and the JSON-LD forms (top-level `graph`, a node's `@graph`, `["graph", …]`). A dataset clause on a ledger-scoped surface (`FROM`, `FROM NAMED`, `USING`, `USING NAMED`, JSON-LD `from` / `fromNamed`) reads the ledger's own address and `mydb:main#<graph IRI>` the same way, and differs only as the list above says: another ledger's address is refused there, and a query's `FROM` may carry a time.

| The name | reads | writes |
|---|---|---|
| the ledger's own address, in any spelling: `mydb`, `mydb:main`, `urn:fluree:mydb:main` | the default graph | the default graph |
| the address with a graph IRI: `mydb:main#http://example.org/g` | the graph `http://example.org/g` | the graph `http://example.org/g` |
| a registered graph IRI | that graph | that graph |
| any other IRI | nothing | a new graph by that IRI |

- No write registers a graph under the ledger's own address, and the graph-management verbs (`CREATE`, `COPY`, `MOVE`, `ADD`) do not create one there. The address with a time (`mydb:main@t:5`) names no graph in these positions.
- A `GRAPH ?g` template writes the graph its binding names in this table, which is the graph the `WHERE` read: `DELETE { GRAPH ?g { ?s ?p ?o } } USING NAMED <mydb:main> WHERE { GRAPH ?g { ?s ?p ?o } }` deletes from the default graph.
- The reserved graphs keep their own IRIs, such as `urn:fluree:mydb:main#config`.

A graph registered under the ledger's address by an earlier version (for instance by a TriG block `GRAPH <mydb:main> { … }`) keeps its data and is reached as `<mydb:main#mydb:main>`: the address, `#`, and the IRI it is registered under. `GRAPH ?g` lists it under that name, in queries and updates alike, so a `?g` binding reads it back. The graph-management verbs name registered graphs exactly, so they still reach it by the address itself; to move its data into the default graph:

```sparql
ADD GRAPH <mydb:main> TO DEFAULT ;
DROP GRAPH <mydb:main>
```

#### Named graphs with no default graph (changed in 4.1.4)

A dataset clause defines the query's dataset exhaustively (SPARQL 1.1 §13.2): the default graph is the union of the `FROM` clauses, so `FROM NAMED` alone leaves it **empty** and patterns written outside `GRAPH { ... }` match nothing. The embedded Rust API has always behaved this way; before 4.1.4 the HTTP endpoints instead substituted a ledger's default graph, so the same query returned different answers depending on which surface you used. The HTTP endpoints now follow §13.2 as well.

This applies equally to the JSON-LD form: `fromNamed` with no `from` leaves the default graph empty, and patterns outside `["graph", ...]` match nothing. The two spellings below are equivalent, and return the same result on every ledger-scoped endpoint (on the connection endpoint, name the graph with its ledger: `FROM NAMED <mydb:main#http://example.org/ns/archive>`):

```sparql
SELECT ?name
FROM NAMED <http://example.org/ns/archive>
WHERE { ?person ex:name ?name }          # matches nothing — empty default graph
```

```json
{
  "fromNamed": { "archive": { "@id": "mydb:main", "@graph": "http://example.org/ns/archive" } },
  "select": ["?name"],
  "where": { "@id": "?person", "ex:name": "?name" }
}
```

To read a default graph alongside a named graph, name it explicitly with `FROM` / `from`:

```sparql
SELECT ?name ?archived
FROM <default>
FROM NAMED <http://example.org/ns/archive>
WHERE {
  ?person ex:name ?name .
  GRAPH <http://example.org/ns/archive> { ?person ex:archived ?archived }
}
```

A query with **no** dataset clause at all is unaffected — it reads the endpoint's ledger default graph as before. When a request does combine named-graph-only with a pattern outside `GRAPH { ... }` / `["graph", ...]`, the response carries an `x-fdb-warning` header explaining why those patterns matched nothing; the status is still `200` and the body is the (correct, possibly empty) result. This holds on the ledger-scoped and connection-scoped `/query` routes and on both streaming routes.

On the **connection-scoped** route this also removes a sharper edge: previously a `fromNamed`-only body had one of its entries chosen as the default graph, so a pattern outside `["graph", ...]` silently read one arbitrarily-selected graph's triples and returned them with a `200`.

### Txn metadata named graph (`#txn-meta`)

The `txn-meta` graph contains per-commit metadata stored as triples. This is useful for auditing and operational metadata (machine address, internal user id, job id, etc.).

**Querying txn-meta via SPARQL:**

```sparql
PREFIX f: <https://ns.flur.ee/db#>
PREFIX ex: <http://example.org/ns/>

SELECT ?commit ?t ?machine
FROM <mydb:main#txn-meta>
WHERE {
  ?commit f:t ?t .
  OPTIONAL { ?commit ex:machine ?machine }
}
```

Notes:
- Using `FROM <mydb:main#txn-meta>` makes txn-meta the **default graph** for the query.
- You can also use dataset syntax (`FROM NAMED` + `GRAPH`) if you need to mix default graph and txn-meta in one query.

### User-Defined Named Graphs

Fluree supports ingesting data into user-defined named graphs using **TriG format**. TriG extends Turtle by adding `GRAPH` blocks that assign triples to specific named graphs.

**Creating named graphs via TriG:**

```trig
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

# Default graph triples
ex:company a schema:Organization ;
    schema:name "Acme Corp" .

# Named graph for product data
GRAPH <http://example.org/graphs/products> {
    ex:widget a schema:Product ;
        schema:name "Widget" ;
        schema:price "29.99"^^xsd:decimal .
}

# Named graph for inventory
GRAPH <http://example.org/graphs/inventory> {
    ex:widget schema:inventory 42 ;
        schema:warehouse "main" .
}
```

Submit TriG data via HTTP API:

```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/trig" \
  --data-binary '@data.trig'
```

To replace a named graph's contents wholesale, for example when reloading an export, use [sync](../transactions/sync.md) instead: it takes the graph's new contents as JSON-LD, Turtle, N-Triples or TriG and commits only what changed. The [Graph Store Protocol](../api/graph-store.md) offers the same replace as a standard `PUT`, plus `GET`, `POST` and `DELETE` on one graph.

**Querying user-defined named graphs (JSON-LD):**

Use the structured `from` object with a `graph` field:

```json
{
  "@context": { "schema": "http://schema.org/" },
  "from": {
    "@id": "mydb:main",
    "graph": "http://example.org/graphs/products"
  },
  "select": ["?name", "?price"],
  "where": [
    { "@id": "?product", "schema:name": "?name" },
    { "@id": "?product", "schema:price": "?price" }
  ]
}
```

**System and user graphs:**
- **Default graph** (implicit): User data without GRAPH blocks
- **`urn:fluree:{ledger_id}#txn-meta`**: Commit metadata
- **`urn:fluree:{ledger_id}#config`**: Ledger configuration (see [Ledger configuration](../ledger-config/README.md))
- **User-defined named graphs**: Identified by their IRI, allocated in order of first use

**Notes:**
- Named graph IRIs are stored in the commit's `graph_delta` field for replay
- Queries against named graphs are scoped to the indexed data (post-indexing)
- Maximum 256 named graphs can be introduced per transaction
- Maximum IRI length is 8KB per graph IRI

### Querying Named Graphs

A graph IRI on its own names a graph of the ledger the query targets: a ledger-scoped endpoint, the CLI with a ledger, or an embedded view. On the connection endpoint, write it with its ledger (`FROM NAMED <mydb:main#http://example.org/ns/graph1>`, and the same IRI in `GRAPH`).

```sparql
# Query specific named graphs
SELECT ?name
FROM NAMED <http://example.org/ns/graph1>
WHERE {
  GRAPH <http://example.org/ns/graph1> {
    ?person ex:name ?name
  }
}

# Query across multiple graphs
SELECT ?graph ?name
FROM NAMED <http://example.org/ns/graph1>
FROM NAMED <http://example.org/ns/graph2>
WHERE {
  GRAPH ?graph {
    ?person ex:name ?name
  }
}
```

## Default Graph Semantics

The **default graph** contains triples that are not in any named graph:

```sparql
# Query only the default graph
SELECT ?name
FROM <ledger:main>
WHERE {
  ?person ex:name ?name
  # This matches triples in the default graph only
}
```

### Union Default Graph

Some SPARQL implementations create a "union default graph" containing triples from all graphs. Fluree keeps them separate by default, but you can achieve union semantics:

```sparql
# Manual union across graphs
SELECT ?name
FROM NAMED <ledger:main>
FROM NAMED <ledger:archive>
WHERE {
  { GRAPH <ledger:main> { ?person ex:name ?name } }
  UNION
  { GRAPH <ledger:archive> { ?person ex:name ?name } }
}
```

## Multi-Ledger Datasets

Datasets can span multiple ledgers:

```sparql
# Dataset across different ledgers
SELECT ?product ?price
FROM <inventory:main>        # Default graph from inventory ledger
FROM NAMED <pricing:main>    # Named graph from pricing ledger
WHERE {
  ?product ex:name "Widget" .
  GRAPH <pricing:main> {
    ?product ex:price ?price
  }
}
```

This enables **federated queries** across different data sources.

## Time-Aware Datasets

Named graphs can represent different time periods:

```sparql
# Query current and historical data
SELECT ?version ?name
FROM NAMED <ledger:main>      # Current data
FROM NAMED <ledger:archive>   # Historical data
WHERE {
  { GRAPH <ledger:main> {
      ?person ex:name ?name .
      BIND("current" AS ?version)
    }
  }
  UNION
  { GRAPH <ledger:archive> {
      ?person ex:name ?name .
      BIND("archive" AS ?version)
    }
  }
}
```

## Graph Management

### Graph Operations

Fluree supports graph-level operations:

```sparql
# Insert into a specific graph
INSERT DATA {
  GRAPH <http://example.org/ns/metadata> {
    <http://example.org/data/doc1> ex:created "2024-01-15T10:00:00Z"^^xsd:dateTime .
  }
}

# Delete from a specific graph
DELETE {
  GRAPH <http://example.org/ns/temp> {
    ?s ?p ?o
  }
}
WHERE {
  GRAPH <http://example.org/ns/temp> {
    ?s ?p ?o
  }
}
```

### Creation is implicit

There is no "create graph" operation. The first transaction that targets a new graph IRI — via TriG, JSON-LD with `@graph`, or SPARQL `INSERT … GRAPH <iri> …` — registers it and assigns a stable `g_id` (3+ for user graphs). Subsequent inserts into the same IRI land in the same registered slot.

### Listing and dropping graphs

Two CLI commands cover the rest of the lifecycle:

- **[`fluree graph list`](../cli/graph.md#fluree-graph-list)** — lists user graphs registered on a branch (with `--include-system` to also show the default and system graphs). Reads the `named-graphs` section of the standard `/info` response.
- **[`fluree graph drop`](../cli/graph.md#fluree-graph-drop)** — transactionally retracts every triple currently asserted under a named graph. Produces one new commit at `t + 1` whose flakes are all retractions; history at older `t` values is preserved, and the graph IRI keeps its `g_id` so future inserts land in the same slot. Drops are per-branch.

The default graph, `urn:fluree:{ledger_id}#txn-meta`, and `urn:fluree:{ledger_id}#config` cannot be dropped. The Rust API entry point is `Fluree::drop_named_graph(ledger_id, graph_iri)`; over HTTP it is `POST /v1/fluree/drop-graph` (admin-protected). See the [server-integration contract](../cli/server-integration.md#drop-named-graph-contract) for the wire details.

### Graph Metadata

For transaction-scoped metadata, Fluree uses the **`txn-meta`** named graph (see above). Transaction metadata is stored as properties on commit subjects in `txn-meta`, and can be queried independently of user data.

## Use Cases

### Data Partitioning

Separate different types of data:

```sparql
SELECT ?customer ?product
FROM NAMED <urn:customers>
FROM NAMED <urn:products>
FROM NAMED <urn:orders>
WHERE {
  GRAPH <urn:customers> { ?customer foaf:name ?name }
  GRAPH <urn:orders> {
    ?order ex:customer ?customer ;
           ex:product ?product .
  }
}
```

### Access Control

Different graphs can have different permissions:

- Public graph: Open access
- Private graph: Restricted access
- Admin graph: Administrative data

### Data Provenance

Track data sources and quality:

```sparql
SELECT ?sensor ?reading ?quality
FROM NAMED <urn:sensor1>
FROM NAMED <urn:sensor2>
WHERE {
  GRAPH ?sensor {
    ?obs ex:reading ?reading ;
         ex:quality ?quality .
  }
  FILTER(?quality > 0.8)  # Only high-quality readings
}
```

### Version Management

Maintain different versions of data:

```sparql
SELECT ?feature ?version
FROM NAMED <urn:v1.0>
FROM NAMED <urn:v2.0>
WHERE {
  GRAPH ?version {
    ?feature ex:status "active"
  }
}
```

## Performance Considerations

### Index Optimization

Named graphs affect indexing strategy:

- **Graph-aware indexes**: Indexes can be partitioned by graph
- **Cross-graph joins**: May require special optimization
- **Graph statistics**: Maintain statistics per graph for query planning

### Query Planning

The query planner considers:

- **Graph selectivity**: Which graphs contain relevant data
- **Join patterns**: How graphs are connected in the query
- **Graph size**: Larger graphs may need different strategies

### Best Practices

1. **Logical Partitioning**: Use graphs for logical data separation
2. **Size Considerations**: Very large graphs may impact query performance
3. **Naming Conventions**: Use consistent IRI patterns for graph names
4. **Documentation**: Document the purpose and schema of each graph

## Standards Compliance

Fluree's dataset implementation follows:

- **SPARQL 1.1 Query**: FROM and FROM NAMED clauses
- **SPARQL 1.1 Update**: GRAPH clauses in updates
- **RDF 1.1 Datasets**: Named graph semantics
- **JSON-LD 1.1**: @graph syntax for named graphs

This enables seamless integration with other RDF tools and SPARQL endpoints while providing Fluree's unique temporal and ledger capabilities.