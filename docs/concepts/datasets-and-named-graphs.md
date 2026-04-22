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

- **Connection-scoped** (`POST /query`): the ledger(s) and graphs are identified by `from` / `fromNamed` (JSON-LD) or `FROM` / `FROM NAMED` (SPARQL). This is the dataset path and supports multi-ledger datasets.
- **Ledger-scoped** (`POST /query/{ledger}`): the ledger is fixed by the URL. The request may still select a **named graph inside that ledger**:
  - JSON-LD: `"from": "default"`, `"from": "txn-meta"`, or `"from": "<graph IRI>"`
  - SPARQL: `FROM <default>`, `FROM <txn-meta>`, `FROM <graph IRI>`, and `FROM NAMED <graph IRI>`

If the request body tries to target a different ledger than the one in the URL, the server rejects it with a "Ledger mismatch" error.

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

### Graph Metadata

For transaction-scoped metadata, Fluree uses the **`txn-meta`** named graph (see above). Transaction metadata is stored as properties on commit subjects in `txn-meta`, and can be queried independently of user data.

## Use Cases

### Data Partitioning

Separate different types of data:

```sparql
FROM NAMED <urn:customers>
FROM NAMED <urn:products>
FROM NAMED <urn:orders>

SELECT ?customer ?product
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
FROM NAMED <urn:sensor1>
FROM NAMED <urn:sensor2>

SELECT ?sensor ?reading ?quality
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
FROM NAMED <urn:v1.0>
FROM NAMED <urn:v2.0>

SELECT ?feature ?version
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