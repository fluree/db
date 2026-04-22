# Getting Started

Welcome to Fluree! This section will guide you through the essential steps to start using Fluree for your graph database needs.

## Quick Navigation

### [Fluree for SQL Developers](fluree-for-sql-developers.md)

Coming from PostgreSQL, MySQL, or SQL Server? This guide maps SQL concepts to Fluree equivalents, shows the same operations in both languages, and highlights where Fluree gives you capabilities that relational databases don't have.

### [Quickstart: Run the Server](quickstart-server.md)

Get Fluree up and running in minutes. Learn how to:
- Install and run the Fluree server
- Configure basic settings
- Verify the server is running
- Access the HTTP API

### [Quickstart: Create a Ledger](quickstart-ledger.md)

Create your first ledger to store data. Learn how to:
- Create a new ledger using the API
- Understand ledger IDs and branching
- Set up initial configuration
- Verify ledger creation

### [Quickstart: Write Data](quickstart-write.md)

Start writing data to your ledger. Learn how to:
- Insert new entities (basic inserts)
- Upsert data (idempotent transactions; predicate-level replacement for supplied predicates)
- Update existing data (WHERE/DELETE/INSERT pattern)
- Understand JSON-LD transaction format

### [Quickstart: Query Data](quickstart-query.md)

Query your data using Fluree's powerful query languages. Learn how to:
- Write basic JSON-LD queries
- Write basic SPARQL queries
- Filter and select data
- Understand query results

### [Tutorial: End-to-End](tutorial-end-to-end.md)

Build a knowledge base that combines Fluree's differentiating features in one workflow:
- Full-text search with BM25 relevance ranking
- Time travel to compare current and historical state
- Branching to experiment safely
- Policies for role-based access control

### [Using Fluree as a Rust Library](rust-api.md)

Embed Fluree directly in your Rust applications. Learn how to:
- Add Fluree as a dependency in Cargo.toml
- Use the Rust API programmatically
- Implement common patterns (insert, query, update)
- Integrate BM25 and vector search
- Handle errors and configuration
- Write tests with Fluree

## What is Fluree?

Fluree is a temporal graph database that stores data as RDF triples with built-in support for:

- **Time Travel**: Query data as it existed at any point in time
- **Full-Text Search**: Integrated BM25 indexing for powerful text search
- **Vector Search**: Approximate nearest neighbor (ANN) queries
- **Policy Enforcement**: Fine-grained, data-level access control
- **Verifiable Data**: Cryptographically signed transactions
- **Graph Sources**: Integration with external data sources (Iceberg, R2RML)

## Learning Path

**For HTTP API users (server-based):**

1. **Bridge the gap**: [Fluree for SQL Developers](fluree-for-sql-developers.md) if coming from relational databases
2. **Start with the Server**: [Run the Server](quickstart-server.md) to get Fluree running
3. **Create Your First Ledger**: [Create a Ledger](quickstart-ledger.md) to set up your database
4. **Add Data**: [Write Data](quickstart-write.md) to insert your first entities
5. **Query Your Data**: [Query Data](quickstart-query.md) to retrieve and explore
6. **See it all together**: [End-to-End Tutorial](tutorial-end-to-end.md) — search, time travel, branching, and policies in one workflow
7. **Core Concepts**: Read [Concepts](../concepts/README.md) to understand Fluree's architecture
8. **Practical Guides**: Explore [Cookbooks](../guides/README.md) for search, time travel, branching, policies, and SHACL validation
9. **Deep Dive**: Explore [Query](../query/README.md), [Transactions](../transactions/README.md), and [Security](../security/README.md)
10. **Production Ready**: Review [Operations](../operations/README.md) for deployment guidance

**For Rust developers (embedded library):**

1. **Rust API Guide**: [Using Fluree as a Rust Library](rust-api.md) for embedding Fluree in your application
2. **Core Concepts**: [Concepts](../concepts/README.md) to understand how Fluree works
3. **Practical Guides**: [Cookbooks](../guides/README.md) for search, time travel, branching, policies, and validation
4. **Advanced Queries**: [Query](../query/README.md) for complex query patterns
5. **Transactions**: [Transactions](../transactions/README.md) for data modification patterns
6. **Production Ready**: [Operations](../operations/README.md) and [Dev Setup](../contributing/dev-setup.md)

## Prerequisites

- Familiarity with JSON format
- HTTP client (curl, Postman, or your programming language's HTTP library)
- No graph database or RDF experience required — [Fluree for SQL Developers](fluree-for-sql-developers.md) bridges the gap from relational databases

## Support and Resources

- **Documentation**: This documentation provides comprehensive coverage
- **API Reference**: See [HTTP API](../api/README.md) for endpoint details
- **Troubleshooting**: Check [Troubleshooting](../troubleshooting/README.md) for common issues

Let's get started!
