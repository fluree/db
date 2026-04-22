# Concepts

Fluree is a graph database that stores and queries data using RDF (Resource Description Framework) semantics. This section explains the core concepts that make Fluree unique and powerful, with special emphasis on the features that differentiate Fluree from other graph databases.

## Recommended Reading Order

These concepts build on each other. If you're new to Fluree, read them in this order:

**Foundations** (read these first):

1. [IRIs, Namespaces, and JSON-LD @context](iri-and-context.md) — How Fluree identifies everything
2. [Datatypes and Typed Values](datatypes.md) — Fluree's type system
3. [Ledgers and the Nameservice](ledgers-and-nameservice.md) — The core unit of data storage

**Core capabilities** (read next):

4. [Time Travel](time-travel.md) — Query any point in history
5. [Branching](ledgers-and-nameservice.md#branching) — Git-like branch, merge, and rebase for your data
6. [Datasets and Named Graphs](datasets-and-named-graphs.md) — Partition and query across graphs

**Differentiating features** (read as needed):

7. [Graph Sources](graph-sources.md) — Integrated search and external data
8. [Policy Enforcement](policy-enforcement.md) — Fine-grained access control
9. [Verifiable Data](verifiable-data.md) — Cryptographic signatures and trust
10. [Reasoning and Inference](reasoning.md) — Derive facts from ontology rules

If you're coming from a SQL/relational background, start with [Fluree for SQL Developers](../getting-started/fluree-for-sql-developers.md) before diving into the concepts above.

## Core Concepts

### [IRIs, Namespaces, and JSON-LD @context](iri-and-context.md)

Understand how Fluree uses Internationalized Resource Identifiers (IRIs) for all data identifiers, how namespaces provide convenient shorthand notation, and how JSON-LD @context enables compact, readable data exchange.

### [Datatypes and Typed Values](datatypes.md)

Explore Fluree's type system, including support for XSD datatypes (strings, numbers, dates, booleans), RDF datatypes, and how all literal values are strongly typed.

### [Ledgers and the Nameservice](ledgers-and-nameservice.md)

Learn about ledgers (Fluree's equivalent of databases), how they're organized with aliases like `mydb:main`, and how the nameservice provides discovery and metadata management across distributed deployments.

### [Time Travel](time-travel.md)

**Differentiator**: Discover Fluree's temporal database capabilities, including transaction-time versioning, historical queries, and the ability to query data "as of" any previous transaction. Every change is preserved, enabling complete audit trails and historical analysis.

### [Datasets and Named Graphs](datasets-and-named-graphs.md)

Learn about SPARQL datasets, named graphs, and how Fluree supports multi-graph queries across different data sources and time periods.

### [Graph Sources](graph-sources.md)

**Differentiator**: Fluree's graph source system enables seamless integration of specialized indexes and external data sources. Built-in BM25 full-text search, vector similarity search (ANN), Apache Iceberg integration, and R2RML relational mappings extend Fluree's query capabilities beyond traditional graph queries.

### [Policy Enforcement](policy-enforcement.md)

**Differentiator**: Fluree's policy system provides fine-grained, data-level access control. Policies are enforced at query time, ensuring users only see data they're authorized to access. This enables secure multi-tenant deployments and compliance with data privacy regulations.

### [Verifiable Data](verifiable-data.md)

**Differentiator**: Fluree supports cryptographically signed transactions using JWS (JSON Web Signatures) and Verifiable Credentials. Every transaction can be cryptographically verified, providing tamper-proof audit trails and enabling trustless data exchange.

### [Reasoning and Inference](reasoning.md)

Fluree's built-in reasoning engine derives new facts from ontology declarations (RDFS, OWL) and user-defined Datalog rules. Query for a superclass and get all subclass instances automatically.

## Architecture Overview

Fluree combines several architectural concepts:

- **Triple Store**: All data is stored as RDF triples (subject-predicate-object)
- **Temporal Database**: Every transaction is timestamped, enabling complete historical access
- **Multi-Graph Support**: Data can be partitioned across named graphs
- **JSON-LD Integration**: Native support for JSON-LD with full IRI expansion/compaction
- **SPARQL & JSON-LD Query**: Support for both SPARQL and Fluree's native JSON-LD Query language

## Key Differentiators

What makes Fluree unique:

1. **Built-in Full-Text Search**: BM25 indexing is integrated directly into the database, not a separate system
2. **Vector Similarity Search**: Native support for approximate nearest neighbor (ANN) queries via embedded HNSW indexes or remote search service
3. **Apache Iceberg Integration**: Query data lake formats directly as graph sources
4. **Complete Time Travel**: Every transaction is preserved with full historical query capabilities
5. **Data-Level Policy Enforcement**: Fine-grained access control enforced at query time, not application level
6. **Cryptographically Verifiable**: Transactions can be signed and verified using industry-standard formats (JWS/VC)

These concepts work together to provide a powerful, standards-compliant graph database with temporal capabilities, integrated search, and enterprise-grade security features.
