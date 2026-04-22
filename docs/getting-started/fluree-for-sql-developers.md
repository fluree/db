# Fluree for SQL Developers

If you've spent years with PostgreSQL, MySQL, or SQL Server and are encountering a graph database for the first time, this guide bridges the gap. It maps SQL concepts you already know to their Fluree equivalents, shows you the same operations in both languages, and highlights where Fluree gives you capabilities that relational databases simply don't have.

## The mental model shift

In SQL, you design tables with fixed columns, then insert rows. In Fluree, you make statements about things — and those statements can describe anything, with any properties, at any time.

| SQL Concept | Fluree Equivalent | Key Difference |
|---|---|---|
| Database | Ledger | Immutable — every change is preserved |
| Table | Type (via `rdf:type`) | No fixed schema required; types are just labels |
| Row | Entity (identified by IRI) | An entity can have any properties, not just those in a "table" |
| Column | Predicate (property) | Not tied to a single type; any entity can use any property |
| Foreign key | Reference (IRI link) | Relationships are first-class, bidirectional, and traversable |
| Value | Object (literal or reference) | Typed values (string, integer, date, etc.) |
| Row (one fact) | Flake | A triple + provenance (graph, transaction time, assert/retract) |
| `NULL` | Absence | Properties simply don't exist if not set — no nulls |

### The flake: Fluree's atomic unit

Every fact in Fluree is stored as a **flake** — an extended triple that adds provenance. At its core, a flake is a statement: `subject → predicate → object`, plus metadata about *when* it was asserted, *which graph* it belongs to, and *whether it's an assertion or retraction*.

```
ex:alice  schema:name  "Alice"       (graph: default, t: 1, op: assert)
ex:alice  schema:age   30            (graph: default, t: 1, op: assert)
ex:alice  schema:knows ex:bob        (graph: default, t: 1, op: assert)
```

Think of it as: "Alice's name is Alice (added in transaction 1)." The provenance is what makes time travel and immutability possible — every change is a new flake, and retractions are recorded alongside assertions.

In SQL terms, imagine a universal table with columns `entity_id`, `attribute`, `value`, `graph`, `transaction`, `operation` — that can represent any data structure without DDL and preserves complete history.

> **Terminology note:** In RDF standards, the core unit is called a "triple" (subject-predicate-object). Fluree's "flake" extends the triple with temporal and provenance metadata. You'll see both terms in the documentation — "triple" when discussing the RDF data model, "flake" when discussing Fluree's storage and history.

## Side by side: common operations

### Creating structure

**SQL — Define a table:**
```sql
CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE,
  department VARCHAR(100),
  salary DECIMAL(10,2),
  manager_id INTEGER REFERENCES employees(id)
);
```

**Fluree — Just insert data:**
```bash
fluree insert '
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .

ex:alice  a schema:Person ;
  schema:name        "Alice Smith" ;
  schema:email       "alice@example.com" ;
  ex:department      "Engineering" ;
  ex:salary          125000 ;
  ex:reportsTo       ex:bob .

ex:bob  a schema:Person ;
  schema:name        "Bob Jones" ;
  schema:email       "bob@example.com" ;
  ex:department      "Engineering" .
'
```

There's no `CREATE TABLE`. Types and properties emerge from the data itself. You can add new properties to any entity at any time without migrations.

### Inserting data

**SQL:**
```sql
INSERT INTO employees (name, email, department, salary)
VALUES ('Carol Davis', 'carol@example.com', 'Marketing', 95000);
```

**Fluree (CLI):**
```bash
fluree insert '
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .

ex:carol  a schema:Person ;
  schema:name        "Carol Davis" ;
  schema:email       "carol@example.com" ;
  ex:department      "Marketing" ;
  ex:salary          95000 .
'
```

**Fluree (HTTP API):**
```bash
curl -X POST http://localhost:8090/v1/fluree/insert?ledger=mydb:main \
  -H "Content-Type: application/ld+json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "@id": "ex:carol",
    "@type": "schema:Person",
    "schema:name": "Carol Davis",
    "schema:email": "carol@example.com",
    "ex:department": "Marketing",
    "ex:salary": 95000
  }'
```

### Basic queries

**SQL:**
```sql
SELECT name, email FROM employees WHERE department = 'Engineering';
```

**Fluree (SPARQL):**
```sparql
PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?name ?email
WHERE {
  ?person a schema:Person ;
          schema:name ?name ;
          schema:email ?email ;
          ex:department "Engineering" .
}
```

**Fluree (JSON-LD Query):**
```json
{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "select": ["?name", "?email"],
  "where": [
    {
      "@id": "?person", "@type": "schema:Person",
      "schema:name": "?name",
      "schema:email": "?email",
      "ex:department": "Engineering"
    }
  ]
}
```

### Joins

In SQL, joins are explicit operations. In Fluree, relationships are just triples — "joining" is following a link.

**SQL — Find employees and their managers:**
```sql
SELECT e.name AS employee, m.name AS manager
FROM employees e
JOIN employees m ON e.manager_id = m.id;
```

**Fluree (SPARQL):**
```sparql
PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?employee ?manager
WHERE {
  ?e schema:name ?employee ;
     ex:reportsTo ?m .
  ?m schema:name ?manager .
}
```

No `JOIN` keyword — you just follow the `ex:reportsTo` link from one entity to another. The database traverses relationships natively.

### Multi-hop relationships

This is where graphs shine. "Find everyone in Alice's reporting chain" requires recursive CTEs in SQL but is natural in a graph.

**SQL (recursive CTE):**
```sql
WITH RECURSIVE chain AS (
  SELECT id, name, manager_id FROM employees WHERE name = 'Alice Smith'
  UNION ALL
  SELECT e.id, e.name, e.manager_id
  FROM employees e JOIN chain c ON e.id = c.manager_id
)
SELECT name FROM chain;
```

**Fluree (SPARQL — property path):**
```sparql
PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?name
WHERE {
  ex:alice ex:reportsTo+ ?manager .
  ?manager schema:name ?name .
}
```

The `+` after `ex:reportsTo` means "follow this relationship one or more times." No recursion needed.

### Aggregation

**SQL:**
```sql
SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC;
```

**Fluree (SPARQL):**
```sparql
PREFIX ex: <http://example.org/>

SELECT ?dept (COUNT(?person) AS ?count) (AVG(?salary) AS ?avg_salary)
WHERE {
  ?person ex:department ?dept ;
          ex:salary ?salary .
}
GROUP BY ?dept
ORDER BY DESC(?avg_salary)
```

### Updates

**SQL:**
```sql
UPDATE employees SET salary = 130000 WHERE name = 'Alice Smith';
```

**Fluree (SPARQL UPDATE):**
```sparql
PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

DELETE { ?person ex:salary ?oldSalary }
INSERT { ?person ex:salary 130000 }
WHERE  { ?person schema:name "Alice Smith" ; ex:salary ?oldSalary }
```

The `WHERE` finds Alice, `DELETE` removes the old salary, and `INSERT` adds the new one. This is atomic.

**Fluree (CLI — upsert for simpler cases):**
```bash
fluree upsert '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "@id": "ex:alice",
  "ex:salary": 130000
}'
```

Upsert replaces the salary value if Alice already exists, or creates the entity if she doesn't.

### Deletes

**SQL:**
```sql
DELETE FROM employees WHERE name = 'Carol Davis';
```

**Fluree (SPARQL UPDATE):**
```sparql
PREFIX schema: <http://schema.org/>

DELETE { ?person ?p ?o }
WHERE  { ?person schema:name "Carol Davis" ; ?p ?o }
```

But here's the key difference: in SQL, the row is gone. In Fluree, the retraction is recorded — you can still query Carol's data at any previous point in time.

## What SQL can't do

These features have no relational equivalent:

### Time travel

Query data as it existed at any point in the past:

```bash
# What was Alice's salary before the raise?
fluree query --at 1 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?salary WHERE {
  ?person schema:name "Alice Smith" ; ex:salary ?salary .
}'
```

```bash
# Show the full history of salary changes
fluree history 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?salary ?t ?op WHERE {
  ?person schema:name "Alice Smith" ; ex:salary ?salary .
}'
```

In SQL, you'd need audit tables, temporal extensions, or trigger-based logging. In Fluree, every change is automatically preserved.

### Schema flexibility

Add new properties to any entity without `ALTER TABLE`:

```bash
# Alice now has a phone number — no migration needed
fluree insert '
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .

ex:alice schema:telephone "+1-555-0100" .
'
```

Different entities of the same "type" can have different properties. There's no fixed set of columns.

### Branching

Fork your data to experiment without affecting production:

```bash
fluree branch create experiment
fluree use mydb:experiment

# Try risky changes on the branch
fluree update 'PREFIX ex: <http://example.org/>
DELETE { ?p ex:salary ?s }
INSERT { ?p ex:salary 200000 }
WHERE  { ?p ex:salary ?s }'

# Main branch is untouched
fluree query --ledger mydb:main 'SELECT ?name ?salary WHERE {
  ?p <http://schema.org/name> ?name ; <http://example.org/salary> ?salary
}'
```

### Triple-level access control

SQL databases give you table-level or row-level security. Fluree policies control access to individual facts:

```json
{
  "@id": "ex:hide-salary",
  "f:action": "query",
  "f:resource": { "f:predicate": "ex:salary" },
  "f:allow": false
}
```

This hides salary data from everyone unless another policy explicitly grants access. The same query returns different results for different users, automatically.

### Integrated full-text search

No need for Elasticsearch or Solr alongside your database:

```bash
fluree insert '{
  "@context": {"ex": "http://example.org/"},
  "@id": "ex:doc1",
  "ex:content": {
    "@value": "Fluree is a graph database with time travel and integrated search",
    "@type": "@fulltext"
  }
}'

fluree query '{
  "@context": {"ex": "http://example.org/"},
  "select": ["?id", "?score"],
  "where": [
    {"@id": "?id", "ex:content": "?text"},
    ["bind", "?score", "(fulltext ?text \"graph database search\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]]
}'
```

## Common "but in SQL I would..." questions

**"How do I enforce NOT NULL?"**
Use [SHACL shapes](../guides/cookbook-shacl.md) to define constraints like required properties, value types, and cardinality.

**"How do I enforce UNIQUE?"**
Fluree supports [unique constraints](../ledger-config/unique-constraints.md) in the ledger configuration.

**"How do I do transactions?"**
Every Fluree transaction is atomic. Multiple operations in a single request either all succeed or all fail.

**"How do I create indexes?"**
Fluree automatically maintains four indexes (SPOT, POST, OPST, PSOT) that cover all query patterns. You don't need to create indexes manually.

**"How do I paginate?"**
Use `LIMIT` and `OFFSET`, just like SQL:
```sparql
SELECT ?name WHERE { ?p schema:name ?name }
ORDER BY ?name LIMIT 20 OFFSET 40
```

**"How do I do subqueries?"**
SPARQL supports subqueries natively:
```sparql
SELECT ?name ?avgSalary WHERE {
  ?person schema:name ?name ; ex:department ?dept .
  { SELECT ?dept (AVG(?s) AS ?avgSalary) WHERE { ?p ex:department ?dept ; ex:salary ?s } GROUP BY ?dept }
}
```

## Next steps

- [Quickstart: Write Data](quickstart-write.md) — Start writing data with the HTTP API
- [SPARQL Reference](../query/sparql.md) — Full SPARQL 1.1 query reference
- [JSON-LD Query](../query/jsonld-query.md) — Fluree's JSON-native query language
- [Concepts](../concepts/README.md) — Deeper understanding of Fluree's architecture
- [Time Travel](../concepts/time-travel.md) — Full guide to temporal queries
