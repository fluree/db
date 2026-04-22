# Schema (mem: vocabulary)

Every memory is a set of RDF triples. The `mem:` vocabulary defines the classes and predicates.

## Namespace

```
@prefix mem: <https://ns.flur.ee/memory#> .
```

## Classes

A memory's kind is expressed via `rdf:type` (`a` in Turtle) — there is no `mem:kind` predicate.

| Class | Kind |
|---|---|
| `mem:Fact` | fact |
| `mem:Decision` | decision |
| `mem:Constraint` | constraint |

`mem:repo` and `mem:user` are additional IRIs used as the range of `mem:scope` (see below).

## Core predicates

| Predicate | Range | Required | Meaning |
|---|---|---|---|
| `mem:content` | `xsd:string` (indexed as `@fulltext`) | ✅ | The textual content; BM25-searchable |
| `mem:scope` | IRI — `mem:repo` or `mem:user` | ✅ | Which TTL file it lives in |
| `mem:createdAt` | `xsd:dateTime` | ✅ | Insertion timestamp |
| `mem:tag` | `xsd:string` (multi-valued) | optional | Free-form tags |
| `mem:artifactRef` | `xsd:string` (multi-valued) | optional | File / symbol / URL references |
| `mem:branch` | `xsd:string` | optional | Git branch captured at write time |

## Optional predicates (any kind)

These predicates can appear on any memory kind. All values are stored as plain string literals (not IRIs).

| Predicate | Range | Meaning |
|---|---|---|
| `mem:rationale` | `xsd:string` (indexed as `@fulltext`) | Why — the reasoning behind this memory |
| `mem:alternatives` | `xsd:string` | What else was considered |
| `mem:severity` | `xsd:string` — `"must"`, `"should"`, or `"prefer"` | How hard a constraint is (constraints only) |

## ID format

Memory IRIs take the shape:

```
mem:<kind>-<ULID>
```

Examples:

```
mem:fact-01JDXYZ5A2B3C4D5E6F7G8H9J0
mem:decision-01JDABC6D7E8F9G0H1I2J3K4L5
mem:constraint-01JDLMN7O8P9Q0R1S2T3U4V5W6
```

ULIDs are sortable by creation time, which is why memories display nicely in chronological order without an explicit index.

## Full example

```ttl
@prefix mem: <https://ns.flur.ee/memory#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

mem:decision-01JDABC a mem:Decision ;
    mem:content "Use postcard for compact index encoding" ;
    mem:tag "encoding" ;
    mem:tag "indexer" ;
    mem:scope mem:repo ;
    mem:artifactRef "fluree-db-indexer/" ;
    mem:createdAt "2026-02-22T14:00:00Z"^^xsd:dateTime ;
    mem:rationale "no_std compatible, smaller output than bincode" ;
    mem:alternatives "bincode, CBOR, MessagePack" .
```

See also: [TTL file format](ttl-format.md) for how this shows up on disk.
