# Guides

Practical, task-oriented cookbooks for Fluree's key features. Each guide shows working patterns you can adapt to your use case.

If you're new to Fluree, start with the [Getting Started](../getting-started/README.md) section first.

## Cookbooks

### [Query Patterns](cookbook-query-patterns.md)

Recipes for the list-value and path operators: dense / gap-filled series with `unwind` + `range`, collecting values into lists, the collect→unwind round-trip, working with list values, and shortest-path queries.

### [Cypher](cookbook-cypher.md)

Querying and writing with openCypher: modeling a property graph, querying relationships, `MERGE` find-or-create, updates and deletes, paths and shortest path, aggregation, and cross-surface round-trips with JSON-LD/SPARQL.

### [Connecting with Neo4j drivers (Bolt)](bolt.md)

Enable and configure the Bolt listener, connect with official Neo4j
drivers (Python/JavaScript examples), transaction retry semantics, and
troubleshooting.

### [SPARQL](cookbook-sparql.md)

The Fluree-specific SPARQL surface (the 1.1 basics assumed): time travel with `FROM @t:`, fact history via `<< s p o >> f:t/f:op`, RDF 1.2 edge annotations (`{| |}`, `~`, `rdf:reifies`), cross-ledger `FROM`/`GRAPH` queries, and cross-surface round-trips.

### [Full-Text and Vector Search](cookbook-search.md)

Set up BM25 full-text search and vector similarity. Insert searchable data, write relevance-ranked queries, combine search with graph patterns, and build hybrid text+vector search.

### [Time Travel](cookbook-time-travel.md)

Practical patterns for temporal queries: audit trails, point-in-time comparison, compliance snapshots, recovering deleted data, and transaction metadata.

### [Branching and Merging](cookbook-branching.md)

Git-like workflows for data: safe experimentation, review-before-merge, multi-environment setups, feature branches, and rebase strategies.

### [Access Control Policies](cookbook-policies.md)

Set up fine-grained access control: department isolation, role-based access, property redaction, multi-tenant isolation, and default-deny patterns.

### [Sharing Data with Downstream Consumers](sharing-data.md)

Serve your ledgers to other teams and organizations: choosing between query serving (your compute, row-level policy) and peer/block serving (their compute, whole-ledger), minting scoped tokens, declaring per-ledger participation with `f:servingDefaults`, identity-bound permissioning end to end, and the consumer-side CLI workflow.

### [SHACL Validation](cookbook-shacl.md)

Define data quality constraints: required properties, datatype validation, value ranges, string patterns, cardinality, and allowed values.

### [Edge Annotations](cookbook-edge-annotations.md)

Attach properties to a relationship: model property-graph edges, record statement-level provenance, represent parallel relationships, query inline or annotation-rooted, and understand the retract cascade — in JSON-LD and SPARQL 1.2.
