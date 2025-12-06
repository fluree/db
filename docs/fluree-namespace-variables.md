# Fluree Namespace Variables Reference

This document provides a comprehensive list of all Fluree namespace variables
used in the codebase.

## Namespace Definitions

The primary Fluree namespaces are defined in `/src/fluree/db/json_ld/iri.cljc`:

- **`f`**: `"https://ns.flur.ee/ledger#"` (namespace code 8)
- **`fidx`**: `"https://ns.flur.ee/index#"` (namespace code 25)
- **`f-did`**: `"did:fluree:"` (namespace code 11)
- **`f-commit-256`**: `"fluree:commit:sha256:"` (namespace code 12)
- **`fdb-256`**: `"fluree:db:sha256:"` (namespace code 10)
- **`f-mem`**: `"fluree:memory://"` (namespace code 13)
- **`f-file`**: `"fluree:file://"` (namespace code 14)
- **`f-ipfs`**: `"fluree:ipfs://"` (namespace code 15)
- **`f-s3`**: `"fluree:s3://"` (namespace code 16)

## Core System Variables

### Transaction & Time Variables
| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `f:t` / `@t` | Transaction time/sequence number | ✅ `iri-t` |
| `f:assert` | Assertions in a transaction | ✅ `iri-assert` |
| `f:retract` | Retractions in a transaction | ✅ `iri-retract` |
| `f:data` | Transaction data container | ✅ `iri-data` |
| `f:flakes` | Individual fact changes | ✅ `iri-flakes` |
| `f:size` | Size information | ✅ `iri-size` |
| `f:v` | Version number | ✅ `iri-v` |
| `f:address` | Address/location reference | ✅ `iri-address` |

### Commit & Ledger Variables
| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `f:commit` | Commit reference | ✅ `iri-commit` |
| `f:previous` | Previous commit reference | ✅ `iri-previous` |
| `f:alias` | Ledger alias | ✅ `iri-alias` |
| `f:ledger` | Ledger reference | ✅ `iri-ledger` |
| `f:branch` | Branch reference | ✅ `iri-branch` |
| `f:namespaces` | Namespace definitions | ✅ `iri-namespaces` |
| `f:index` | Index reference | ✅ `iri-index` |
| `f:ns` | Namespace reference | ✅ `iri-ns` |
| `f:time` | Time reference | ✅ `iri-time` |
| `f:author` | Transaction author | ✅ `iri-author` |
| `f:txn` | Transaction reference | ✅ `iri-txn` |
| `f:annotation` | Transaction annotation | ✅ `iri-annotation` |
| `f:message` | Commit message | ✅ `iri-message` |
| `f:tag` | Tag reference | ✅ `iri-tag` |
| `f:updates` | Updates in commit | ✅ `iri-updates` |

### Core Types
| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `f:CommitProof` | Commit proof type | ✅ `iri-commit-proof` |
| `f:Commit` | Commit type | ✅ `iri-commit-type` |
| `f:DB` | Database type | ✅ `iri-db` |

## Policy & Security Variables

### Policy Framework
| Variable | Description | Defined in constants.cljc | Notes |
|----------|-------------|---------------------------|-------|
| `f:AccessPolicy` | Access control policy type | ✅ `iri-access-policy` |
| `f:policyClass` | Links identity to policy classes | ✅ `iri-policy-class` |
| `f:action` | Policy action specification | ✅ `iri-action` |
| `f:required` | Required constraint flag | ✅ `iri-required` |
| `f:exMessage` | Exception/error message | ✅ `iri-ex-message` |

### Policy Actions
| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `f:view` | View/read permission | ✅ `iri-view` |
| `f:modify` | Modify/write permission | ✅ `iri-modify` |

### Policy Targeting
| Variable | Description | Defined in constants.cljc | Notes |
|----------|-------------|---------------------------|-------|
| `f:targetRole` | Target role for policy | ✅ `iri-target-role` |
| `f:targetClass` | Target class for policy | ✅ `iri-target-class` |
| `f:targetNode` | Target node for policy | ✅ `iri-target-node` |
| `f:targetSubject` | Target subject for policy | ✅ `iri-target-subject` |
| `f:targetProperty` | Target property for policy | ✅ `iri-target-property` |
| `f:targetObjectsOf` | Target objects of property | ✅ `iri-target-objects-of` |
| `f:onProperty` | Property constraint (legacy) | ✅ `iri-onProperty` | ⚠️
**LEGACY**: Use `f:targetProperty` instead |
| `f:onSubject` | Subject constraint | ✅ `iri-onSubject` | ⚠️ **LEGACY**: Use
`f:targetSubject` instead |

### Policy Logic
| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `f:allow` | Unconditional allow/deny (boolean) | ✅ `iri-allow` |
| `f:query` | Policy query/condition | ✅ `iri-query` |
| `f:where` | Where clause in policy | ✅ `iri-where` |
| `f:values` | Values specification | ✅ `iri-values` |
| `f:equals` | Equality constraint | ✅ `iri-equals` |
| `f:contains` | Contains constraint | ✅ `iri-contains` |
| `f:property` | Property specification | ✅ `iri-property` |
| `f:path` | Property path | ✅ `iri-path` |
| `f:onClass` | Class constraint | ✅ `iri-onClass` |

## Identity & Role Variables

| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `f:$identity` | Special identity variable | ✅ `iri-identity` |
| `f:role` | Role assignment | ✅ `iri-role` |

## Schema & Type Variables

### Data Types
| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `f:VirtualGraph` | Virtual graph type | ✅ `iri-virtual-graph-type` |
| `f:virtualGraph` | Virtual graph name/reference | ✅ `iri-virtual-graph` |
| `f:vector` | Dense vector/embedding type | ✅ `iri-vector` |
| `f:sparseVector` | Sparse vector type | ✅ `iri-sparse-vector` |

### SHACL Integration
| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `f:opts` | Options specification | ✅ `iri-opts` |
| `f:rule` | Rule specification | ✅ `iri-rule` |
| `f:insert` | Insert operation | ✅ `iri-insert` |
| `f:delete` | Delete operation | ✅ `iri-delete` |
| `f:all-nodes` | All nodes specification | ✅ `iri-all-nodes` |

## Index & Search Variables (fidx namespace)

### BM25 Text Search
| Variable | Description | Defined in constants.cljc |
|----------|-------------|---------------------------|
| `fidx:BM25` | BM25 index type | ✅ `iri-bm25-index` |
| `fidx:b` | BM25 b parameter | ✅ `iri-bm25-b` |
| `fidx:k1` | BM25 k1 parameter | ✅ `iri-bm25-k1` |
| `fidx:target` | Index target | ✅ `iri-idx-target` |
| `fidx:property` | Index property | ✅ `iri-idx-property` |
| `fidx:limit` | Index limit | ✅ `iri-idx-limit` |
| `fidx:id` | Index ID | ✅ `iri-idx-id` |
| `fidx:score` | Index score | ✅ `iri-idx-score` |
| `fidx:result` | Index result | ✅ `iri-idx-result` |
| `fidx:vector` | Index vector | ✅ `iri-idx-vector` |
| `fidx:sync` | Index sync | ✅ `iri-idx-sync` |
| `fidx:timeout` | Index timeout | ✅ `iri-idx-timeout` |

## Key Implementation Files

- **`/src/fluree/db/constants.cljc`** - Central IRI constant definitions
- **`/src/fluree/db/json_ld/iri.cljc`** - Namespace and IRI management
- **`/src/fluree/db/json_ld/policy.cljc`** - Policy implementation
