# Reference

Reference materials for Fluree developers and operators.

## Reference Guides

### [Glossary](glossary.md)

Definitions of key terms and concepts:
- RDF terminology
- Fluree-specific terms
- Database concepts
- Query terminology
- Index terminology

### [Fluree System Vocabulary](vocabulary.md)

Complete reference for Fluree's system vocabulary under `https://ns.flur.ee/db#`:
- Commit metadata predicates (`f:t`, `f:address`, `f:time`, `f:previous`, etc.)
- Search query vocabulary (BM25 and vector search patterns)
- Nameservice record fields and type taxonomy
- Policy vocabulary
- Namespace codes

### [Standards and Feature Flags](compatibility.md)

Standards and feature-flag reference:
- SPARQL 1.1 compliance
- JSON-LD specifications
- W3C standards support
- Feature flags
- Experimental features
- Deprecated features

### [Graph Identities and Naming](graph-identities.md)

Naming conventions for graphs, ledgers, and identifiers:
- User-facing terminology (ledger, graph IRI, graph source, graph snapshot)
- Time pinning syntax (`@t:`, `@iso:`, `@commit:`)
- Named graphs within a ledger
- Base resolution for graph references

### [Crate Map](crate-map.md)

Overview of Fluree's Rust crate architecture:
- Core crates
- API crates
- Query engine crates
- Storage crates
- Dependency relationships

## Quick Reference

### Common Namespaces

```turtle
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix schema: <http://schema.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix dc: <http://purl.org/dc/terms/> .
```

### Fluree Namespaces

```turtle
@prefix f: <https://ns.flur.ee/db#> .
```

### Time Specifiers

```text
ledger:branch@t:123             # Transaction number
ledger:branch@iso:2024-01-22    # ISO timestamp
ledger:branch@commit:bafybeig...  # Commit ContentId
```

### HTTP Status Codes

| Code | Meaning | Common Cause |
|------|---------|--------------|
| 200 | OK | Success |
| 400 | Bad Request | Invalid syntax |
| 401 | Unauthorized | Missing auth |
| 403 | Forbidden | Policy denied |
| 404 | Not Found | Ledger not found |
| 408 | Timeout | Query too slow |
| 413 | Payload Too Large | Request too big |
| 429 | Too Many Requests | Rate limited |
| 500 | Internal Error | Server error |
| 503 | Unavailable | Overloaded |

### Index Types

| Index | Order | Optimized For |
|-------|-------|---------------|
| SPOT | Subject-Predicate-Object-Time | Entity properties |
| POST | Predicate-Object-Subject-Time | Property values |
| OPST | Object-Predicate-Subject-Time | Value lookups |
| PSOT | Predicate-Subject-Object-Time | Predicate scans |

## Standards Compliance

### RDF Standards

- **RDF 1.1:** Full compliance
- **Turtle:** Full support
- **JSON-LD 1.1:** Full compliance
- **N-Triples:** Support (future)

### Query Standards

- **SPARQL 1.1 Query:** Full compliance
- **SPARQL 1.1 Update:** Partial support
- **GeoSPARQL:** Planned

### Security Standards

- **JWS (RFC 7515):** Full support
- **JWT (RFC 7519):** Full support
- **Verifiable Credentials:** W3C compliant
- **DIDs:** did:key, did:web support

## Performance Benchmarks

Typical performance characteristics:

### Query Performance

| Query Type | Small DB | Medium DB | Large DB |
|------------|----------|-----------|----------|
| Simple lookup | < 1ms | < 5ms | < 10ms |
| Pattern match | < 10ms | < 50ms | < 100ms |
| Complex join | < 50ms | < 200ms | < 500ms |
| Aggregation | < 100ms | < 500ms | < 2s |

### Transaction Performance

| Operation | Typical Time |
|-----------|--------------|
| Small insert (< 10 triples) | < 10ms |
| Medium insert (< 100 triples) | < 50ms |
| Large insert (< 1000 triples) | < 200ms |
| Update | < 20ms |
| Upsert | < 30ms |

### Indexing Performance

| Workload | Rate |
|----------|------|
| Light | 1,000 flakes/sec |
| Medium | 5,000 flakes/sec |
| Heavy | 10,000 flakes/sec |

## Related Documentation

- [Glossary](glossary.md) - Term definitions
- [Compatibility](compatibility.md) - Standards compliance
- [Crate Map](crate-map.md) - Code architecture
