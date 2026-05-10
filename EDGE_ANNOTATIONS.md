# Edge Annotations Design Plan

## Purpose

This document sketches a plan for native edge annotations in Fluree: an ergonomic way to attach properties to a relationship while preserving Fluree's RDF/JSON-LD foundation, immutable history model, and binary index efficiency.

The goal is to support the common LPG shape:

```text
(:Person {id: "alice"})
  -[:worksFor {role: "Engineer", since: "2024-01-01", confidence: 0.97}]->
(:Organization {id: "acme"})
```

without forcing users to model an intermediate relationship node by hand, and without making ordinary RDF triples pay a storage or query cost when annotations are not used.

The plan also aims to provide a compatibility path for:

- JSON-LD-star-style `@annotation` syntax.
- RDF-star / SPARQL-star annotations on asserted triples.
- A useful subset of Cypher / LPG import and query semantics.

## Executive Summary

Keep the existing fact indexes tuple-shaped. Do not embed annotation property bags inside `Flake` rows or binary leaflets.

Add an optional annotation attachment sidecar, referenced from `IndexRoot`, that maps between:

```text
edge key -> annotation/reifier subject(s)
annotation/reifier subject -> edge key
```

Annotation properties are stored as normal Fluree facts about the annotation subject:

```text
ex:alice ex:worksFor ex:acme .

_:ann1 ex:role "Engineer" .
_:ann1 ex:since "2024-01-01" .
_:ann1 ex:confidence 0.97 .
```

The attachment sidecar records that `_:ann1` belongs to the edge:

```text
(g, ex:alice, ex:worksFor, ex:acme, dt) -> _:ann1
```

This gives us:

- Zero impact on ordinary non-annotation queries.
- Fast edge-to-annotation lookups.
- Fast annotation-to-edge lookups.
- Native support for multiple parallel LPG edges.
- RDF-star / JSON-LD-star compatibility for asserted triple annotations.
- Transaction-time cascade semantics for retracting edge metadata when the parent edge is retracted.

## Current Constraints

### Flake Shape

Today a `Flake` is a single fact with fixed fields:

```text
g, s, p, o, dt, t, op, m
```

`m` is currently small metadata such as language and list index:

```rust
pub struct FlakeMeta {
    pub lang: Option<String>,
    pub i: Option<i32>,
}
```

The binary indexes are designed around fixed tuple rows and four sort orders:

```text
SPOT: (g, s, p, o, dt, t, op)
PSOT: (g, p, s, o, dt, t, op)
POST: (g, p, o, dt, s, t, op)
OPST: (g, o, dt, p, s, t, op)
```

Leaflets split row data into independently compressed regions. Region 1 stores order-dependent core columns; Region 2 stores metadata needed to reconstruct full flakes, including datatype, transaction time, sparse language tags, and sparse list indexes; Region 3 stores history journal entries.

### Why Not Store Annotation Bags in `Flake::m`

It is tempting to put `annotation_sid` or a list of annotation ids into `m`, but that has several problems:

- A variable-length list of annotations would make fixed-row binary leaflets harder to encode, route, cache, and decode.
- The fact indexes would need to carry annotation data even for query paths that do not ask for it.
- It would still not provide the inverse lookup needed for "find all edges where edge.role = Engineer".
- It would blur existing metadata semantics. `m.i` means list position; it should not also become relationship identity.

The better approach is to keep `Flake` fixed and add a sidecar only when edge annotations exist.

## Terminology

### Base Edge

The ordinary RDF triple:

```text
s p o
```

In index form:

```text
(g, s, p, o, dt, object_meta)
```

For most relationship edges, `o` is an IRI/reference and `dt` is the ref datatype. Literal-valued triples can also be annotated for RDF-star compatibility, so the edge key must be able to distinguish literal datatype and language.

### Annotation Subject / Reifier

An RDF subject that owns metadata about a base edge. It may be:

- A blank node minted by Fluree.
- An explicit user-provided `@id`.
- An internally generated stable relationship id for Cypher/LPG imports.

Example:

```text
_:ann1 ex:role "Engineer" .
_:ann1 ex:confidence 0.97 .
```

In RDF 1.2 terms, this is close to a reifier. In LPG terms, this is the relationship occurrence id.

### Edge Occurrence

The pair:

```text
(edge key, annotation_sid)
```

This is what allows multiple parallel edges:

```text
(alice, worksFor, acme) -> ann1  role=Engineer
(alice, worksFor, acme) -> ann2  role=Advisor
```

This mirrors the existing list concept: `m.i` distinguishes repeated list occurrences; `annotation_sid` distinguishes repeated relationship occurrences.

## User-Facing Syntax

### Preferred JSON-LD Shape

Use `@annotation` as the standards-aligned keyword, following the JSON-LD-star proposal.

```json
{
  "@context": {
    "ex": "http://example.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  },
  "insert": {
    "@id": "ex:alice",
    "ex:worksFor": {
      "@id": "ex:acme",
      "@annotation": {
        "ex:role": "Engineer",
        "ex:since": { "@value": "2024-01-01", "@type": "xsd:date" },
        "ex:confidence": 0.97
      }
    }
  }
}
```

Fluree may also support `@edge` as an alias for LPG-oriented users:

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@edge": {
      "ex:role": "Engineer",
      "ex:confidence": 0.97
    }
  }
}
```

Internally, `@edge` should normalize to `@annotation`.

### Explicit Annotation Identity

Users should not normally need to name an edge annotation, but it should be possible:

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@annotation": {
      "@id": "ex:employment/alice-acme-2024",
      "ex:role": "Engineer",
      "ex:confidence": 0.97
    }
  }
}
```

Use cases:

- Stable edge identity for updates and deletes.
- External references to an edge occurrence.
- Verifiable credentials / signatures over a relationship.
- Cypher imports that need relationship ids.
- Multiple parallel edges between the same subject and object.

### Inline Query Shape

The query shape should mirror the insert shape:

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?person", "?org", "?role", "?since", "?confidence"],
  "where": {
    "@id": "?person",
    "ex:worksFor": {
      "@id": "?org",
      "@annotation": {
        "ex:role": "?role",
        "ex:since": "?since",
        "ex:confidence": "?confidence"
      }
    }
  }
}
```

This should lower to:

1. Match base edge `?person ex:worksFor ?org`.
2. Lookup attached annotation subject(s) for each matched edge.
3. Match normal facts about those annotation subjects.

### Annotation-Rooted Query Shape

Users must also be able to start from annotation metadata and find matching edges:

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?person", "?org", "?since"],
  "where": {
    "ex:role": "Engineer",
    "ex:since": "?since",
    "@reifies": {
      "@id": "?person",
      "ex:worksFor": { "@id": "?org" }
    }
  }
}
```

This should lower to:

1. Match annotation subjects with `ex:role "Engineer"` and optional other metadata constraints.
2. Use reverse attachment lookup to bind the reified edge key.
3. Optionally verify the base edge is currently asserted.

`@reifies` is aligned with RDF 1.2 discussions, but Fluree may use it primarily as query syntax for annotation-rooted edge patterns.

### Projection of Annotated Edges

Subject expansion should support edge annotations:

```json
{
  "select": {
    "?person": [
      "@id",
      {
        "ex:worksFor": [
          "@id",
          {
            "@annotation": ["ex:role", "ex:since", "ex:confidence"]
          }
        ]
      }
    ]
  },
  "where": {
    "@id": "?person",
    "ex:worksFor": {
      "@id": "?org",
      "@annotation": {
        "ex:role": "Engineer"
      }
    }
  }
}
```

Output shape should preserve the same graph shape:

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@annotation": {
      "ex:role": "Engineer",
      "ex:since": "2024-01-01",
      "ex:confidence": 0.97
    }
  }
}
```

## Storage Model

### Core Rule

Annotation metadata is ordinary RDF data. The only special storage structure is the attachment relation between an edge key and an annotation subject.

For this insert:

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@annotation": {
      "ex:role": "Engineer",
      "ex:confidence": 0.97
    }
  }
}
```

Store:

```text
Base fact:
  ex:alice ex:worksFor ex:acme

Attachment sidecar:
  (default, ex:alice, ex:worksFor, ex:acme, @id) -> _:ann1

Annotation facts:
  _:ann1 ex:role "Engineer"
  _:ann1 ex:confidence 0.97
```

### Edge Key

The edge key should be stable, dictionary-encoded, and comparable:

```text
EdgeKey {
  g_id: Option<Sid>,
  s_id: Sid,
  p_id: Sid,
  o_kind: ObjectKind,
  o_key: u64,
  dt_id: Sid,
  lang_id: u16,
  list_i: Option<i32>,
}
```

Notes:

- `g_id` is required to distinguish named graph scope.
- `dt_id` is required to distinguish IRI refs, strings, dates, numbers, custom datatypes, etc.
- `lang_id` is required for language-tagged literal triples.
- `list_i` should be included if an annotated fact is a list occurrence. This keeps list occurrence identity distinct from non-list fact identity.
- For the common relationship case, `dt_id` is ref and both `lang_id` and `list_i` are empty.

We may optimize the physical wire encoding for the common case, but the logical key must be unambiguous.

### Annotation Subject

The annotation subject is a `Sid`.

If the user supplies `@annotation.@id`, use it.

If the user does not supply an id:

- For JSON-LD/RDF-star inserts, mint a blank node subject.
- For Cypher/LPG imports, mint a stable relationship id if the relationship must be addressable later.

The generated id must be stable within the transaction and persisted through the dictionaries like any other subject.

### Sidecar Artifacts

Add optional CAS artifacts to `IndexRoot`, similar in spirit to dictionary trees:

```text
IndexRoot {
  ...
  annotation_index: Option<AnnotationIndexRoot>
}

AnnotationIndexRoot {
  version: u8,
  max_t: i64,
  forward_cid: ContentId,
  reverse_cid: ContentId,
  stats: AnnotationStats,
}
```

The hard "zero attachments" guarantee for the indexed portion requires both `IndexRoot.annotation_index = None` **and** `IndexRoot.has_annotations = false`. The M2b builder ships, and the encoder coerces `has_annotations = true` whenever an arena is present so the two signals always agree on the wire. The `(true, None)` cell remains valid for two transitional cases: (a) ledgers indexed before slice 3 (no arena ever sealed), and (b) snapshots that hit the indexer's defensive-drop path (caller couldn't supply an `Authoritative` event set this pass). Both fall back to the M2a scan path; the next reindex with provider coverage re-seals an arena.

If any uncertainty exists, set it present with empty artifacts or force the transaction layer to check novelty.

### Forward Attachment Index

Purpose:

```text
edge key -> annotation subject(s)
```

Logical rows:

```text
ForwardAttachment {
  edge_key: EdgeKey,
  ann_sid: Sid,
  t: i64,
  op: bool,
}
```

For current lookup, query the latest visible state and return current `ann_sid`s.

Sort order:

```text
(g, s, p, o_kind, o_key, dt, lang, list_i, ann_sid, t, op)
```

This supports:

- Base edge matched first, then annotations.
- Plain edge retraction cascade.
- Expansion/projection of annotated edges.

### Reverse Attachment Index

Purpose:

```text
annotation subject -> edge key
```

Logical rows:

```text
ReverseAttachment {
  ann_sid: Sid,
  edge_key: EdgeKey,
  t: i64,
  op: bool,
}
```

Sort order:

```text
(ann_sid, g, s, p, o_kind, o_key, dt, lang, list_i, t, op)
```

This supports:

- Annotation-rooted queries.
- `@reifies` query syntax.
- SPARQL-star `?ann rdf:reifies <<...>>` compatibility.
- Efficient deletes by annotation id.

### Direct vs Many Optimization

The simplest implementation is one sidecar row per edge occurrence:

```text
edge_key -> ann_sid
edge_key -> ann_sid2
```

That is probably good enough initially and keeps the format simple.

If storage density becomes a concern, introduce a compressed grouped representation later:

```text
edge_key -> Direct(ann_sid)
edge_key -> Many(set_id)
set_id -> [ann_sid...]
```

Do not start with RDF list (`rdf:first` / `rdf:rest`) semantics for multiple edge occurrences. Parallel LPG relationships are a bag/set of relationship occurrences, not an ordered list.

### Artifact Format

Use dictionary-like CAS artifacts rather than extending fact leaflets:

```text
Annotation branch:
  magic: "EAB1" or similar
  leaf_count
  first_key / last_key / row_count / leaf_cid

Annotation leaf:
  magic: "EAL1" or similar
  row_count
  compressed fixed-width columns
```

The exact magic/version names can be chosen during implementation. The important properties:

- Content addressed.
- Immutable.
- Sorted.
- Range-routable by branch manifest.
- Cheap to omit when no annotations exist.
- Similar operational model to dictionary forward/reverse trees.

### History

Attachment rows need history just like facts.

If an annotation attachment is asserted at `t=10` and retracted at `t=20`, history queries should be able to show both events.

There are two viable approaches:

1. Include `t/op` in attachment rows and query them with the same snapshot rules used by fact scans.
2. Store current attachment state in forward/reverse trees and a separate attachment history journal.

Start with option 1 unless it conflicts with existing index builder internals. It is simpler and matches the append-only flake mental model.

## Transaction Semantics

### Insert With Annotation

For:

```json
{
  "insert": {
    "@id": "ex:alice",
    "ex:worksFor": {
      "@id": "ex:acme",
      "@annotation": {
        "ex:role": "Engineer"
      }
    }
  }
}
```

Lower to:

1. Assert base fact `ex:alice ex:worksFor ex:acme`.
2. Mint or resolve `ann_sid`.
3. Assert attachment `(edge_key, ann_sid)`.
4. Assert annotation facts about `ann_sid`.

All four steps are atomic within the transaction.

### Plain Edge Retraction

For:

```json
{
  "delete": {
    "@id": "ex:alice",
    "ex:worksFor": { "@id": "ex:acme" }
  }
}
```

Native edge-metadata semantics should be LPG-like:

- Delete all current edge occurrences for that edge key.
- Retract all owned annotation facts for those occurrences.
- Retract the base fact.

This avoids leaving current annotations pointing at a non-current edge assertion.

History still preserves the old annotation facts and attachment events.

### Annotated Edge Retraction

For:

```json
{
  "delete": {
    "@id": "ex:alice",
    "ex:worksFor": {
      "@id": "ex:acme",
      "@annotation": {
        "ex:role": "Engineer"
      }
    }
  }
}
```

Interpret annotation properties as a selector, not as full equality:

1. Find current annotations attached to `(alice, worksFor, acme)`.
2. Filter to annotations matching `ex:role "Engineer"`.
3. Retract those attachment rows.
4. Retract owned annotation facts for those annotation subjects.
5. If no annotation occurrence remains for the edge key, decide whether to retract the base fact.

Recommended default for step 5:

- In LPG mode, retract the base fact when the last edge occurrence is removed.
- In RDF/JSON-LD mode, preserve base fact unless the transaction explicitly deletes it.

This is a policy decision and should be made explicit in transaction options.

### Delete by Annotation Id

For:

```json
{
  "delete": {
    "@id": "ex:alice",
    "ex:worksFor": {
      "@id": "ex:acme",
      "@annotation": {
        "@id": "ex:employment/alice-acme-2024"
      }
    }
  }
}
```

Retract exactly that occurrence if it is currently attached to the edge key.

This is the most precise and least surprising update/delete path.

### Metadata Property Updates

Updating edge metadata should be normal RDF update against the annotation subject once the occurrence is bound:

```json
{
  "where": {
    "@id": "ex:alice",
    "ex:worksFor": {
      "@id": "ex:acme",
      "@annotation": {
        "@id": "?edge",
        "ex:role": "Engineer"
      }
    }
  },
  "delete": {
    "@id": "?edge",
    "ex:confidence": "?old"
  },
  "insert": {
    "@id": "?edge",
    "ex:confidence": 0.99
  }
}
```

The parser may provide nicer sugar, but the lowering should remain query-shaped.

### Do Not Require Full Metadata Equality

Never require users to specify all metadata fields to retract an annotated edge. That is brittle and unlike LPG systems.

Supported selectors should be:

- Exact annotation id.
- Partial metadata match.
- Whole base edge.

## Retraction Cascade and Efficiency

### Indexed Snapshot Flag

`IndexRoot.annotation_index.is_some()` tells whether the indexed snapshot may contain annotation attachments.

This is not primarily for normal query planning. Query planning can know syntactically whether a query asks for annotations.

It is primarily for transaction retractions:

```text
if indexed_has_annotations == false and novelty_has_annotations == false:
  plain triple delete can skip attachment lookup
else:
  lookup attachments and cascade as required
```

The flag must be conservative:

- `false` means no current indexed annotation attachments exist.
- `true` means lookup may be needed.

### Novelty Layer

Recent unindexed writes can contain annotation attachments even when the last index root has no annotation arena.

Maintain novelty-level annotation state:

```text
novelty_has_annotations: bool
novelty_forward: edge_key -> ann_sid(s)
novelty_reverse: ann_sid -> edge_key
```

Transaction cascade checks must use:

```text
annotations_possible =
  index_root.annotation_index.is_some() || novelty_has_annotations
```

Lookups must merge indexed sidecar state and novelty state under the same snapshot/transaction visibility rules.

### Retraction Algorithm

Plain triple retract:

```text
fn retract_edge(edge_key):
    if !annotations_possible:
        emit_retract_base_fact(edge_key)
        return

    anns = lookup_current_annotations(edge_key)
    for ann in anns:
        emit_retract_attachment(edge_key, ann)
        for fact in lookup_owned_annotation_facts(ann):
            emit_retract_fact(fact)

    emit_retract_base_fact(edge_key)
```

Annotated occurrence retract:

```text
fn retract_edge_occurrence(edge_key, selector):
    anns = lookup_current_annotations(edge_key)
    matching = filter_annotations(anns, selector)

    for ann in matching:
        emit_retract_attachment(edge_key, ann)
        for fact in lookup_owned_annotation_facts(ann):
            emit_retract_fact(fact)

    if lpg_mode && no_current_annotations(edge_key):
        emit_retract_base_fact(edge_key)
```

### Owned Annotation Facts

Define which facts are "owned" by an annotation subject.

Recommended initial rule:

- All current outbound facts where subject is `ann_sid`, except system-managed attachment/reification predicates, are owned metadata.

Potential refinements:

- Preserve non-owned facts if the annotation id is an explicit IRI and external data references it.
- Require an internal marker such as `f:EdgeAnnotation` for owned anonymous annotations.
- Let transaction options choose cascade behavior for explicit annotation ids.

For LPG ergonomics, anonymous annotation subjects should cascade by default.

## Query Planning

### Zero Impact for Non-Annotation Queries

If a query has no `@annotation`, `@edge`, `@reifies`, RDF-star quoted triple, or relationship variable construct, the planner should emit the same plan it does today.

Do not check the annotation sidecar merely because the ledger has annotations.

### Inline Annotation Query

Input:

```json
{
  "where": {
    "@id": "?person",
    "ex:worksFor": {
      "@id": "?org",
      "@annotation": {
        "ex:role": "Engineer"
      }
    }
  }
}
```

Lower to IR roughly equivalent to:

```text
Triple(?person, ex:worksFor, ?org)
EdgeAnnotations(?person, ex:worksFor, ?org -> ?ann)
Triple(?ann, ex:role, "Engineer")
```

`EdgeAnnotations` is a new logical operator backed by the forward sidecar.

Planner choices:

- If the base triple pattern is selective, scan base facts first and lookup annotations.
- If annotation constraints are more selective, start from annotation facts and use reverse sidecar.

### Annotation-Rooted Query

Input:

```json
{
  "where": {
    "ex:role": "Engineer",
    "@reifies": {
      "@id": "?person",
      "ex:worksFor": { "@id": "?org" }
    }
  }
}
```

Lower to:

```text
Triple(?ann, ex:role, "Engineer")
AnnotationTarget(?ann -> ?person, ex:worksFor, ?org)
```

`AnnotationTarget` is backed by the reverse sidecar.

### Costing

Add annotation stats to `AnnotationIndexRoot`:

```text
AnnotationStats {
  attachment_count: u64,
  annotated_edge_count: u64,
  annotation_subject_count: u64,
  max_annotations_per_edge: u32,
  maybe predicate_histogram for annotation subjects
}
```

Initial planner heuristics:

- If annotation metadata contains a constant predicate/object filter, start from normal fact index over annotation facts.
- If the base edge has bound subject/predicate/object, start from base triple and forward sidecar.
- If only `@annotation` exists with no metadata filter, forward sidecar may be cheaper after base edge matching.

### Multiple Edge Occurrences

When a query binds edge metadata or an edge variable, the result cardinality is per edge occurrence, not per `(s,p,o)`.

Example:

```text
alice worksFor acme -> ann1 role=Engineer
alice worksFor acme -> ann2 role=Advisor
```

Query:

```json
{
  "select": ["?person", "?org", "?role"],
  "where": {
    "@id": "?person",
    "ex:worksFor": {
      "@id": "?org",
      "@annotation": {
        "ex:role": "?role"
      }
    }
  }
}
```

Returns two rows.

A query that does not ask for annotations should continue to see ordinary RDF set semantics:

```json
{
  "selectDistinct": ["?person", "?org"],
  "where": {
    "@id": "?person",
    "ex:worksFor": "?org"
  }
}
```

### Policy

Annotation facts are ordinary facts and should pass through normal policy checks.

Open question:

- If a user can see the base edge but not its annotation metadata, should the edge appear without annotations or be hidden entirely for annotation queries?

Recommended default:

- Base edge visibility and annotation metadata visibility are checked independently.
- Annotation query rows require visibility of both the base edge and the matched annotation facts.

## RDF-star and JSON-LD-star Compatibility

### Supported Target

Support asserted-triple annotations:

```sparql
<< ex:alice ex:worksFor ex:acme >> ex:role "Engineer" .
```

JSON-LD-star:

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@annotation": {
      "ex:role": "Engineer"
    }
  }
}
```

RDF 1.2-style reifier:

```json
{
  "@id": "ex:ann1",
  "@reifies": {
    "@id": "ex:alice",
    "ex:worksFor": { "@id": "ex:acme" }
  },
  "ex:role": "Engineer"
}
```

All should lower to the same sidecar + annotation facts model.

### Virtual `rdf:reifies`

Expose `rdf:reifies` / `@reifies` as virtual syntax over the reverse sidecar.

This query:

```sparql
?ann rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> .
?ann ex:role ?role .
```

should lower to:

```text
ReverseAttachment(?ann -> ex:alice, ex:worksFor, ex:acme)
Triple(?ann, ex:role, ?role)
```

### Boundaries / Non-Goals for Initial Support

Do not initially support full arbitrary RDF 1.2 triple terms as first-class object values.

Defer or reject:

- Triple terms used as arbitrary object values, e.g. `ex:doc ex:mentions <<(s p o)>>`.
- Nested triple terms.
- Reifiers for unasserted triples.
- Reifiers that reify multiple unrelated triples.
- Generic proposition logic independent of edge lifecycle.

These can be added later by introducing a first-class triple-term value/dictionary encoding, but they are not required for native LPG-style edge metadata.

### Assertion Lifecycle

For Fluree native edge annotations, the annotation is current only while the parent edge occurrence is current.

RDF 1.2 allows reifiers to describe unasserted propositions. That is useful in logic/provenance systems but conflicts with LPG edge-property expectations.

Recommended boundary:

- Native `@annotation` / LPG edge metadata is lifecycle-coupled to asserted edge occurrences.
- Advanced RDF 1.2 proposition reification may be a future separate mode.

## Cypher / LPG Compatibility

### What This Unlocks

This design can faithfully import common Cypher/LPG relationships:

```cypher
CREATE
  (:Person {id: "alice"})
    -[:WORKS_FOR {role: "Engineer", confidence: 0.97}]->
  (:Org {id: "acme"})
```

Lower to:

```text
ex:alice ex:worksFor ex:acme
(ex:alice, ex:worksFor, ex:acme) -> _:rel1
_:rel1 ex:role "Engineer"
_:rel1 ex:confidence 0.97
```

Cypher query:

```cypher
MATCH (a)-[r:WORKS_FOR {role: "Engineer"}]->(b)
RETURN a, b, r.confidence
```

Can lower to:

```text
Triple(a, ex:worksFor, b)
EdgeAnnotations(a, ex:worksFor, b -> r)
Triple(r, ex:role, "Engineer")
Triple(r, ex:confidence, ?confidence)
```

### Relationship Identity

Cypher relationships have identity. RDF triples are set-like.

The annotation subject provides the relationship occurrence id:

```text
Cypher relationship variable r == annotation_sid
```

This supports:

- `MATCH ()-[r]->() RETURN r`
- Relationship property updates.
- Relationship deletes.
- Parallel relationships between the same two nodes.

### Parallel Edges

Support multiple relationships between the same nodes:

```cypher
CREATE
  (alice)-[:WORKS_FOR {role: "Engineer"}]->(acme),
  (alice)-[:WORKS_FOR {role: "Advisor"}]->(acme)
```

Store:

```text
(alice, worksFor, acme) -> ann1
(alice, worksFor, acme) -> ann2

ann1 role Engineer
ann2 role Advisor
```

When a query binds `r`, return one row per occurrence.

When a query does not bind relationship identity or metadata, preserve RDF set-like semantics unless the query language explicitly requests LPG cardinality.

### Property-Less Cypher Relationships

Open design choice:

1. RDF mode: no annotation id when no properties exist. Cheap and set-like.
2. LPG mode: mint annotation ids for every relationship, even property-less ones. Full Cypher relationship identity.
3. Hybrid: JSON-LD/RDF inserts remain set-like; Cypher imports mint relationship ids.

Recommended initial approach: hybrid.

This lets Fluree stay RDF-native by default while Cypher imports preserve relationship identity.

### Cypher Features In Scope

Likely feasible over this model:

- Node label matching via `rdf:type`.
- Relationship type matching via predicate IRI.
- Relationship property filters via annotation facts.
- Relationship variables via annotation subjects.
- Directed relationship patterns.
- Variable-length path traversal using existing graph crawl/property path machinery.
- `CREATE` imports for nodes and relationships.
- `MATCH ... RETURN` over node/edge patterns.
- `SET` / `REMOVE` relationship properties as updates to annotation facts.
- `DELETE r` as attachment + owned metadata cascade.

### Cypher Features Not Solved by Storage Alone

Full Cypher requires substantial language/runtime work:

- Path value semantics.
- Shortest path variants.
- `MERGE` semantics.
- Null/list/map expression semantics.
- Pattern comprehensions.
- Aggregation and grouping behavior.
- Transactional uniqueness constraints.
- Full relationship id exposure/stability guarantees.
- Multi-database and schema/index DDL semantics.

This design provides the storage primitive, not a full Cypher implementation by itself.

## Index Root and CAS Management

### Root Fields

Add an optional annotation section:

```text
IndexRoot {
  ...
  annotation_index: Option<AnnotationIndexRoot>
}
```

If absent:

- Indexed data has no current annotation attachments.
- Query plans without annotation syntax are unchanged.
- Retraction cascade can skip indexed sidecar lookup, subject to novelty state.

If present:

- Forward/reverse attachment artifacts are loaded lazily as needed.
- Query plans with annotation syntax can route to the sidecar.
- Transaction retractions must check it for cascades.

### CAS Layout

Treat annotation arenas like dictionary trees:

```text
annotation/
  forward/
    <branch-cid>
    <leaf-cid>...
  reverse/
    <branch-cid>
    <leaf-cid>...
```

The exact physical directory is less important than preserving the existing CAS principles:

- Root points to content ids.
- Branches route ranges.
- Leaves store sorted rows.
- Artifacts are immutable and content-addressed.
- Missing annotation section means no fetches.

### Garbage Collection

Annotation artifacts participate in the same root-chain reachability model as other index artifacts.

When a new index root supersedes an old one:

- New annotation CIDs are reachable from the new root.
- Old annotation CIDs are garbage only when no retained root references them.
- Attachment rows and annotation facts are immutable history; current-state compaction follows normal index rebuild rules.

## Import and Index Build

### Bulk Import

During import parsing:

1. Parse `@annotation` / `@edge` on value objects.
2. Emit base fact run records.
3. Emit annotation property run records.
4. Emit attachment run records.
5. Sort/build normal fact indexes as today.
6. Sort/build annotation forward/reverse arenas if attachment run records exist.
7. Set `IndexRoot.annotation_index`.

If no attachment records exist, omit annotation artifacts.

### Incremental Transactions

Novelty must track:

- Base facts.
- Annotation facts.
- Attachment assertions/retractions.

Before an index rebuild, queries and transactions must merge indexed attachment state with novelty attachment state.

After rebuild, novelty attachment state covered by the new root can be cleared.

## Query and Transaction IR Additions

### New Logical Patterns

Add query IR patterns similar to:

```rust
Pattern::EdgeAnnotation {
    edge: EdgePattern,
    annotation: RefOrVar,
}

Pattern::AnnotationTarget {
    annotation: RefOrVar,
    edge: EdgePattern,
}
```

Where `EdgePattern` can bind or constrain:

```text
g, s, p, o, dt, lang, list_i
```

The parser lowers:

- Inline `@annotation` to `EdgeAnnotation`.
- `@reifies` to `AnnotationTarget`.
- SPARQL-star quoted triple annotations to one of the above.
- Cypher relationship variable binding to `EdgeAnnotation`.

### Transaction IR

Add transaction operations:

```text
AssertAttachment(edge_key, ann_sid)
RetractAttachment(edge_key, ann_sid)
CascadeRetractEdge(edge_key)
```

These can lower to novelty attachment records and normal annotation fact asserts/retracts.

## Design Decisions (v1)

The following are the locked-in decisions for the v1 implementation.
Anything outside this list is deferred (see *Deferred / Out of v1 scope*
below). The companion document `EDGE_ANNOTATIONS_IMPL_PLAN.md` references
this section as its frozen contract.

### Mode default

- **RDF default.** Retracting a base edge removes the attachment bundle
  and anonymous annotation body metadata, but preserves explicit-IRI
  annotation subjects as ordinary RDF. Triple-store users keep their
  existing semantics and user-named resources are not deleted
  surprisingly.
- **LPG mode is opt-in per transaction.** A transaction option (e.g.
  `opts.lpgEdgeLifecycle: true`) extends base-edge retract cascade to
  explicit-IRI annotation body metadata, matching Cypher's relationship
  lifecycle where deleting a relationship deletes its properties.
- **Occurrence-level lifecycle is deferred.** Targeted retracts such as
  "delete the annotation with `ex:role` = `Engineer` but keep the
  `Manager` annotation" still require occurrence-by-selector / by-id IR
  work. When that lands, LPG mode can also define the "last occurrence
  removed may retract the base fact" behavior for those targeted shapes.
- Cypher / LPG imports default to LPG mode without requiring the user to
  pass the option.

### Empty `@annotation: {}`

- In RDF mode, an empty annotation block is a **no-op**. No annotation
  subject is minted, no attachment row is written. Inserts remain
  idempotent at the (s, p, o) level.
- In LPG mode, an empty annotation block **does** mint a fresh
  annotation subject (a property-less relationship still has identity).

### Multiple annotations per edge — required

Multiple parallel annotations on the same `(g, s, p, o, dt, lang, list_i)`
key are a hard requirement and exercised by the v1 storage layer.

- The forward attachment index is a multimap on `edge_key`.
- Two `@annotation: { ... }` blocks against the same base edge with
  anonymous subjects mint two distinct annotation SIDs and two
  attachment rows. They are not deduped.
- Two annotation blocks with the **same explicit `@id`** target one
  annotation subject; the attachment is idempotent.
- Cypher `(a)-[:T {p:1}]->(b), (a)-[:T {p:2}]->(b)` round-trips as two
  parallel attachments and `MATCH (a)-[r:T]->(b) RETURN r` returns two
  rows.

### Multiplicity contract by query layer

These are the rules the planner / executor enforces so RDF and LPG users
both get what they expect:

- A bare `Triple(?s, p, ?o)` returns **one row per distinct `(s, p, o)`**
  regardless of how many annotation occurrences attach. RDF set
  semantics are unchanged; `selectDistinct` users see no behavior
  change.
- `EdgeAnnotation { edge, annotation: ?ann, body }` returns **one row
  per `(edge_key, ann_sid)` currently asserted**. Binding an annotation
  variable (or matching `body`) is what introduces per-occurrence
  cardinality.
- `select: "*"` follows the same rules: it does not multiply by
  occurrence count unless the WHERE binds an annotation variable.

### Annotation subject visibility

- Anonymous (generated) annotation SIDs are **hidden** from
  `select: "*"` and from JSON-LD subject-expansion output unless
  explicitly bound through `@annotation` or projected by
  `@reifies`.
- Explicit annotation IRIs are ordinary subjects and visible like any
  other.

### Explicit annotation id cascade

- **Anonymous annotation subjects cascade** on edge retraction:
  attachment removed, all owned annotation facts retracted.
- **Explicit annotation IRIs do NOT cascade their non-attachment
  facts** by default — only the attachment row is retracted, not facts
  about the user-named resource. This avoids surprising deletion of
  data referenced from outside the edge.
- LPG mode (per above) overrides this: explicit ids cascade their
  facts in LPG mode, matching Cypher relationship-delete semantics.

### EdgeKey object representation

- Reuse the existing `FlakeValue` encoding for the object position
  rather than introducing a separate `o_kind: ObjectKind, o_key: u64`
  pair. The current encoding already canonicalizes IRIs, literals,
  refs, and datatype.
- `EdgeKey` shape:

  ```text
  EdgeKey {
      g_id: Option<Sid>,
      s_id: Sid,
      p_id: Sid,
      o: FlakeValue,
      dt_id: Sid,
      lang_id: u16,        // 0 == none
      list_i: Option<i32>, // see below
  }
  ```

### `list_i` in `EdgeKey`

- The field is present in the v1 key shape so the on-disk format never
  changes when list-occurrence annotations are added.
- v1 always writes `None` for `list_i`. The parser rejects
  `@annotation` on list-occurrence triples with a clear error until a
  real use case arrives.

### `@annotation` on literal-valued objects

- v1 accepts `@annotation` only on `@id`-valued objects (asserted
  relationship triples) and on the parent of an asserted IRI subject
  via `@reifies`.
- Annotations on literal-valued triples are deferred. RDF-star
  asserted-triple annotations against literals are rare in practice
  and lock in `dt_id` / `lang_id` semantics that v1 should not have to
  commit to.

### Reifiers for unasserted triples

- v1 rejects reifiers that point at unasserted propositions. Native
  edge annotations are lifecycle-coupled to asserted edge occurrences.
- A future proposition-reifier store can be added without disturbing
  the LPG edge model.

### Multiple reified triples per reifier

- v1 rejects an annotation subject reifying more than one triple term.
  One annotation subject corresponds to one edge occurrence.
- Allowing multiple reverse attachment rows per annotation SID would
  ambiguate Cypher relationship identity and isn't worth the
  complexity for v1.

## Deferred / Out of v1 scope

The following are explicitly out of scope for v1 and may be revisited
in later phases:

- Triple-term object values as first-class `FlakeValue` (e.g.
  `ex:doc ex:mentions <<(s p o)>>`).
- Nested triple terms.
- Reifiers for unasserted triples.
- Reifiers that reify multiple unrelated triples.
- General proposition logic independent of edge lifecycle.
- Annotations on literal-valued triples.
- Annotations on list-occurrence triples (`list_i.is_some()`).
- Phase 6 Cypher import (separate workstream; the storage primitive
  is delivered by v1 but the `MATCH/CREATE/MERGE/...` language work
  is its own track).
- Non-JSON-LD output of annotation metadata. v1 emits `@annotation`
  / `@reifies` in JSON-LD output. Turtle/TriG, N-Quads, and SPARQL
  CONSTRUCT need a separate surface-form decision
  (Turtle-star vs RDF 1.2 reifier vs other). Until that decision,
  CONSTRUCT against a non-JSON-LD target that projects annotation
  metadata returns a clear `UnsupportedFeature` error.

## Phased Implementation Plan

### Phase 1: Data Model and Parser Surface

- Add JSON-LD parser support for `@annotation`.
- Add `@edge` as optional alias if desired.
- Lower annotated value objects into:
  - base fact asserts,
  - annotation subject allocation,
  - annotation fact asserts,
  - attachment asserts.
- Add query parser support for inline annotation patterns.
- Add transaction validation for unsupported cases.

Deliverable:

- JSON-LD insert/query shape works in novelty/in-memory paths.

### Phase 2: Attachment Sidecar in Novelty

- Add novelty forward/reverse attachment maps.
- Add cascade logic for plain edge retractions.
- Add cascade logic for annotation occurrence retractions.
- Add history behavior for attachment events.

Deliverable:

- Correct transaction semantics before binary index persistence.

### Phase 3: Binary Annotation Arena

- Define annotation forward/reverse artifact formats.
- Extend index build to emit annotation artifacts.
- Extend `IndexRoot` with optional `AnnotationIndexRoot`.
- Load sidecar lazily.
- Merge indexed and novelty attachment state.

Deliverable:

- Annotated edges survive indexing and remote CAS storage.

### Phase 4: Query Planner and Execution

- Add `EdgeAnnotation` and `AnnotationTarget` IR operators.
- Implement forward sidecar lookup.
- Implement reverse sidecar lookup.
- Add planner heuristics/costing.
- Ensure no sidecar access for non-annotation queries.

Deliverable:

- Efficient inline and annotation-rooted queries.

### Phase 5: RDF-star / JSON-LD-star Compatibility

- Lower SPARQL-star `<< s p o >> annP annO` to annotation patterns.
- Support virtual `rdf:reifies` for asserted triples.
- Support JSON-LD-star `@reifies` where it maps to one asserted triple.
- Reject nested/arbitrary triple terms with clear errors.

Deliverable:

- Useful RDF-star compatibility without first-class triple-term values.

### Phase 6: Cypher / LPG Import Subset

- Map node labels to `rdf:type`.
- Map relationship types to predicates.
- Mint relationship ids as annotation subjects.
- Store relationship properties as annotation facts.
- Support parallel relationships.
- Support basic `MATCH (a)-[r:T {props}]->(b)` patterns.
- Support `CREATE`, `SET`, and `DELETE r` for relationships.

Deliverable:

- Native import/query for common LPG data without lossy edge-property conversion.

## Success Criteria

- Ordinary RDF/JSON-LD queries have unchanged plans and no sidecar reads.
- Ledgers with no annotations do not create annotation artifacts.
- Plain triple retractions can skip cascade checks when both indexed root and novelty say no annotations exist.
- Annotated edge queries are efficient in both directions:
  - edge first,
  - metadata first.
- Multiple parallel edges are representable without widening core fact rows.
- Annotation properties are normal facts, so history, policy, export, and query behavior reuse existing machinery.
- RDF-star support covers asserted-triple annotations.
- Cypher imports preserve relationship properties and parallel relationship identity.

