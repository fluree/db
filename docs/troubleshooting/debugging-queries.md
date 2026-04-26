# Debugging Queries

This guide provides tools and techniques for debugging query performance and correctness issues in Fluree.

## Query Explain Plans

### Enable Explain

Get query execution plan:

```bash
curl -X POST http://localhost:8090/v1/fluree/explain \
  -H "Content-Type: application/json" \
  -d '{
    "from": "mydb:main",
    "select": ["?name", "?age"],
    "where": [
      { "@id": "?person", "schema:name": "?name" },
      { "@id": "?person", "schema:age": "?age" }
    ],
    "filter": "?age > 25"
  }'
```

**Response:**
```json
{
  "plan": {
    "type": "join",
    "left": {
      "type": "scan",
      "index": "POST",
      "predicate": "schema:name",
      "estimated_rows": 1000
    },
    "right": {
      "type": "scan",
      "index": "POST",
      "predicate": "schema:age",
      "estimated_rows": 1000
    },
    "join_variable": "?person",
    "filter": {
      "expression": "?age > 25",
      "selectivity": 0.6
    },
    "estimated_result_rows": 600
  },
  "execution": {
    "duration_ms": 45,
    "rows_scanned": 2000,
    "rows_returned": 573,
    "index_hits": 2000,
    "filter_applications": 1000
  }
}
```

### Understanding Explain Output

**Scan Operations:**
- Which index used (SPOT, POST, OPST, PSOT)
- Estimated rows
- Actual rows scanned

**Join Operations:**
- Join type (hash, merge, nested loop)
- Join variable
- Join order

**Filter Operations:**
- Filter expression
- Estimated selectivity
- Rows filtered

**Execution Stats:**
- Total duration
- Rows scanned vs returned
- Index efficiency

## Query Tracing

### Enable Tracing

Get detailed execution trace:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Trace: true" \
  -d '{...}'
```

**Response:**
```json
{
  "results": [...],
  "trace": {
    "total_duration_ms": 45,
    "phases": [
      {
        "phase": "parse",
        "duration_ms": 2
      },
      {
        "phase": "plan",
        "duration_ms": 3
      },
      {
        "phase": "execute",
        "duration_ms": 38,
        "steps": [
          {
            "step": "scan_POST_schema:name",
            "duration_ms": 12,
            "rows": 1000
          },
          {
            "step": "scan_POST_schema:age",
            "duration_ms": 15,
            "rows": 1000
          },
          {
            "step": "join",
            "duration_ms": 8,
            "rows": 1000
          },
          {
            "step": "filter",
            "duration_ms": 3,
            "rows_in": 1000,
            "rows_out": 573
          }
        ]
      },
      {
        "phase": "serialize",
        "duration_ms": 2
      }
    ]
  }
}
```

### Trace Analysis

Look for:
- **Slow phases:** Which phase takes longest?
- **Excessive scans:** Too many rows scanned?
- **Inefficient joins:** Large intermediate results?
- **Ineffective filters:** Filters applied too late?

## Common Query Issues

### No Results

**Symptom:** Query returns empty array

**Debugging Steps:**

1. **Check data exists:**
   ```sparql
   SELECT (COUNT(*) as ?count)
   WHERE { ?s ?p ?o }
   ```

2. **Test each pattern separately:**
   ```json
   // Test pattern 1
   {"select": ["?person"], "where": [{"@id": "?person", "schema:name": "?name"}]}
   
   // Test pattern 2
   {"select": ["?person"], "where": [{"@id": "?person", "schema:age": "?age"}]}
   ```

3. **Check IRI matching:**
   ```json
   // Query with full IRI
   {"@id": "http://example.org/ns/alice"}
   
   // Or with prefix
   {"@id": "ex:alice"}
   ```

4. **Verify time specifier:**
   ```bash
   # Current data
   "from": "mydb:main"
   
   # Historical might be empty
   "from": "mydb:main@t:1"
   ```

### Unexpected Results

**Symptom:** Results don't match expectations

**Debugging Steps:**

1. **Check each variable:**
   ```json
   {
     "select": ["?person", "?name", "?age"],  // See all bindings
     "where": [...]
   }
   ```

2. **Verify types:**
   ```sparql
   SELECT ?person ?name (DATATYPE(?name) as ?nameType)
   WHERE {
     ?person schema:name ?name
   }
   ```

3. **Check for duplicates:**
   ```sparql
   SELECT ?person (COUNT(?name) as ?count)
   WHERE {
     ?person schema:name ?name
   }
   GROUP BY ?person
   HAVING (?count > 1)
   ```

4. **Test without filters:**
   ```json
   // Remove filter temporarily
   {"where": [...] }  // No filter
   ```

### Slow Queries

**Symptom:** Query takes too long

**Debugging Steps:**

1. **Check explain plan:**
   ```bash
   curl -X POST http://localhost:8090/v1/fluree/explain -d '{...}'
   ```

2. **Check indexing lag:**
   ```bash
   curl http://localhost:8090/v1/fluree/info/mydb:main
   # High indexing lag (commit_t - index_t) can slow queries
   ```

3. **Add LIMIT:**
   ```json
   {"where": [...], "limit": 100}
   ```

4. **Check pattern specificity:**
   ```json
   // Specific (fast)
   {"@id": "ex:alice", "schema:name": "?name"}
   
   // General (slow)
   {"@id": "?entity", "?pred": "?value"}
   ```

5. **Verify index usage:**
   - Subject-based patterns use SPOT (fast)
   - Broad patterns may scan many triples (slow)

## Query Optimization

### Automatic Pattern Reordering

The query planner automatically reorders WHERE-clause patterns for optimal
join order. You do not need to manually order patterns from most to least
selective — the planner does this for you using a greedy algorithm that
considers cardinality estimates and which variables are already bound at each
step.

When database statistics are available (after at least one indexing cycle),
estimates use HLL-derived property counts and distinct-value counts.
Without statistics, the planner falls back to heuristic constants. You can
verify the planner's decisions using explain plans (see
[Explain Plans](../query/explain.md)).

Both of these queries produce the same execution plan:

```json
{
  "where": [
    {"@id": "?company", "schema:name": "?companyName"},
    {"@id": "?person", "schema:worksFor": "?company"},
    {"@id": "ex:alice", "schema:name": "?name"}
  ]
}
```

```json
{
  "where": [
    {"@id": "ex:alice", "schema:name": "?name"},
    {"@id": "ex:alice", "schema:worksFor": "?company"},
    {"@id": "?company", "schema:name": "?companyName"}
  ]
}
```

The planner recognizes that `ex:alice` patterns are highly selective (bound
subject), and that `?company` becomes bound after those patterns execute,
making the final pattern a cheap per-subject lookup rather than a full scan.

### Filter and BIND Placement

Filters and BINDs are placed during the greedy reordering loop, as soon as all
their input variables are bound. You do not need to manually position them for
efficiency. For BIND patterns, only the expression's input variables must be
bound — the target variable is an output that feeds back into the bound set,
enabling cascading placement of dependent patterns.

When a filter or BIND becomes ready immediately after a compound pattern
(UNION, Graph, or Service), the planner pushes it *into* the compound
pattern's inner lists rather than placing it after. For UNION, the filter is
cloned into every branch. This means filters execute within each branch,
benefiting from optimal placement, range pushdown, and inline evaluation — the
same optimizations available to top-level filters.

Additionally, filters whose variables are all bound by a join operator are
evaluated inline during the join itself, avoiding the overhead of a separate
filter pass. Filters that depend on a BIND's output variable are fused into
the BindOperator and evaluated inline after computing each row's BIND value,
similarly eliminating a separate filter pass. Range-safe filters (comparisons
like `>`, `<` on indexed properties) are pushed down to the index scan.

### Use LIMIT

Always limit large result sets:

```json
{
  "where": [...],
  "orderBy": ["?name"],
  "limit": 100,
  "offset": 0
}
```

Implement pagination for UI.

### Avoid Cartesian Products

Ensure patterns are connected:

**Bad (Cartesian product):**
```json
{
  "where": [
    {"@id": "?person", "schema:name": "?name"},
    {"@id": "?company", "schema:name": "?companyName"}
    // Not connected! Returns person × company combinations
  ]
}
```

**Good (connected):**
```json
{
  "where": [
    {"@id": "?person", "schema:name": "?name"},
    {"@id": "?person", "schema:worksFor": "?company"},
    {"@id": "?company", "schema:name": "?companyName"}
  ]
}
```

## Policy Debugging

### Enable Policy Trace

See which policies apply:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

Response:
```json
{
  "results": [...],
  "policy_trace": [
    {
      "policy": "ex:department-policy",
      "matched": true,
      "condition_met": true,
      "decision": "allow",
      "patterns_added": [
        {"@id": "?person", "ex:department": "engineering"}
      ]
    },
    {
      "policy": "ex:role-policy",
      "matched": false,
      "reason": "subject_mismatch"
    }
  ],
  "final_decision": "allow"
}
```

### Policy Impact on Query

Compare query with and without policies:

```javascript
// With policies (authenticated)
const authResult = await queryWithAuth(query);

// Without policies (admin override)
const fullResult = await queryAsAdmin(query);

console.log(`Auth sees ${authResult.length} rows`);
console.log(`Admin sees ${fullResult.length} rows`);
console.log(`Policy filtered ${fullResult.length - authResult.length} rows`);
```

## Testing Queries

### Isolate Components

Test query components separately:

```javascript
// Test each WHERE pattern
for (const pattern of wherePatterns) {
  const result = await query({
    select: ["?s"],
    where: [pattern]
  });
  console.log(`Pattern ${JSON.stringify(pattern)}: ${result.length} results`);
}
```

### Use Smaller Datasets

Test on small dataset first:

```bash
# Create test ledger
curl -X POST "http://localhost:8090/v1/fluree/insert?ledger=test:main" \
  -d '{"@graph": [small test data]}'

# Test query
curl -X POST http://localhost:8090/v1/fluree/query \
  -d '{"from": "test:main", ...}'
```

### Compare with Expected Results

```javascript
const expected = [
  { name: "Alice", age: 30 },
  { name: "Bob", age: 25 }
];

const actual = await query({...});

assert.deepEqual(actual, expected);
```

## Diagnostic Queries

### Check Index Usage

```sparql
# Count triples per index
SELECT (COUNT(*) as ?count)
WHERE { ?s ?p ?o }
```

### Find Large Entities

```sparql
SELECT ?entity (COUNT(?triple) as ?tripleCount)
WHERE {
  ?entity ?p ?o .
  BIND(?entity AS ?triple)
}
GROUP BY ?entity
ORDER BY DESC(?tripleCount)
LIMIT 10
```

### Find Common Predicates

```sparql
SELECT ?predicate (COUNT(*) as ?count)
WHERE {
  ?s ?predicate ?o
}
GROUP BY ?predicate
ORDER BY DESC(?count)
```

### Check Data Types

```sparql
SELECT ?type (COUNT(*) as ?count)
WHERE {
  ?entity a ?type
}
GROUP BY ?type
ORDER BY DESC(?count)
```

## Performance Profiling

### Measure Query Time

```javascript
const start = Date.now();
const result = await query({...});
const duration = Date.now() - start;

console.log(`Query returned ${result.length} rows in ${duration}ms`);
```

### Identify Bottlenecks

Use trace to find slow operations:

```javascript
const response = await queryWithTrace({...});
const trace = response.trace;

const slowSteps = trace.phases
  .flatMap(p => p.steps || [])
  .filter(s => s.duration_ms > 100)
  .sort((a, b) => b.duration_ms - a.duration_ms);

console.log('Slow steps:', slowSteps);
```

### Compare Approaches

Test different query formulations:

```javascript
// Approach 1
const start1 = Date.now();
const result1 = await query(approach1);
const time1 = Date.now() - start1;

// Approach 2
const start2 = Date.now();
const result2 = await query(approach2);
const time2 = Date.now() - start2;

console.log(`Approach 1: ${time1}ms, Approach 2: ${time2}ms`);
```

## Best Practices

### 1. Use Explain Early

Run explain on new queries:

```bash
curl -X POST http://localhost:8090/v1/fluree/explain -d '{...}'
```

### 2. Test with Representative Data

Test queries with production-like data volume:

```javascript
// Load realistic test data
await loadTestData(10000);  // Similar to production size

// Test query performance
const result = await query({...});
```

### 3. Monitor Query Patterns

Track slow queries:

```javascript
if (duration > 1000) {
  logger.warn(`Slow query: ${duration}ms`, {
    query: queryText,
    resultCount: result.length
  });
}
```

### 4. Profile Before Optimizing

Measure before optimizing:

```javascript
console.time('query');
const result = await query({...});
console.timeEnd('query');
```

### 5. Use Query Logs

Enable query logging:

```toml
[logging]
level = "debug"
log_queries = true
```

## Common Query Antipatterns

### Antipattern 1: Overly Broad Patterns

Bad:
```json
{"@id": "?entity", "?predicate": "?value"}
```

Good:
```json
{"@id": "?person", "@type": "schema:Person"},
{"@id": "?person", "schema:name": "?name"}
```

### Antipattern 2: Disconnected Patterns (Cartesian Products)

Ensure all patterns share at least one variable with the rest of the query.
Disconnected patterns produce a Cartesian product:

Bad:
```json
{
  "where": [
    {"@id": "?person", "schema:name": "?name"},
    {"@id": "?dept", "schema:budget": "?budget"}
  ]
}
```

Good:
```json
{
  "where": [
    {"@id": "?person", "schema:name": "?name"},
    {"@id": "?person", "schema:department": "?dept"},
    {"@id": "?dept", "schema:budget": "?budget"}
  ]
}
```

Note: filter placement is handled automatically by the planner. Filters are
applied as soon as all their referenced variables are bound, regardless of
where they appear in the query.

### Antipattern 3: Missing LIMIT

Bad:
```json
{
  "select": ["?name"],
  "where": [...]  // Could return millions
}
```

Good:
```json
{
  "select": ["?name"],
  "where": [...],
  "limit": 1000  // Always limit
}
```

### Antipattern 4: Redundant Patterns

Bad:
```json
{
  "where": [
    {"@id": "ex:alice", "schema:name": "?name"},
    {"@id": "ex:alice", "schema:name": "Alice"}  // Redundant
  ]
}
```

Good:
```json
{
  "where": [
    {"@id": "ex:alice", "schema:name": "Alice"}
  ]
}
```

## Tools

### Query Validation

Validate before sending:

```javascript
function validateQuery(query) {
  if (!query.select) {
    throw new Error('Missing select clause');
  }
  if (!query.where || query.where.length === 0) {
    throw new Error('Missing where clause');
  }
  if (!query.limit && estimateResultSize(query) > 1000) {
    console.warn('Query missing LIMIT clause');
  }
}
```

### Query Builder

Use query builder for complex queries:

```javascript
const query = new QueryBuilder()
  .from('mydb:main')
  .select('?name', '?age')
  .where('?person', 'schema:name', '?name')
  .where('?person', 'schema:age', '?age')
  .filter('?age > 25')
  .limit(100)
  .build();
```

### Query Templates

Create reusable templates:

```javascript
function findPersonByName(name) {
  return {
    from: 'mydb:main',
    select: ['?person', '?email'],
    where: [
      { '@id': '?person', 'schema:name': name },
      { '@id': '?person', 'schema:email': '?email' }
    ]
  };
}
```

## Related Documentation

- [Common Errors](common-errors.md) - Error reference
- [Explain Plans](../query/explain.md) - Query optimization
- [JSON-LD Query](../query/jsonld-query.md) - Query syntax
- [SPARQL](../query/sparql.md) - SPARQL syntax
- [Telemetry](../operations/telemetry.md) - Logging and monitoring
