# Cookbook: SHACL across named graphs

Named graphs are a natural way to divide a ledger. Each organization, team or
source system writes its own graph, and reference data that everyone shares (a
product catalog, a list of commodities, a price list) lives once in a graph of
its own.

SHACL checks a record against the graph it is written to. A shape whose rules
depend on the shared data then fails even when the data is right: a listing in
`<org-a>` that references `ex:coffee` cannot be shown to reference an
`ex:Commodity` when only `<catalog>` says what coffee is.

`f:additionalDataSource` names graphs to read with the graph being written. The
record is checked against them as one graph, so every constraint can use the
shared data (`sh:class`, a property path, `sh:node`, a count, `sh:sparql`),
while the record stays in its own graph and the catalog stays in its own.

This walkthrough builds a small marketplace ledger:

| Graph IRI                               | Role                                         |
|-----------------------------------------|----------------------------------------------|
| `<http://example.org/shapes>`           | Shapes                                       |
| `<http://example.org/catalog>`          | Shared catalog: commodities and their status |
| `<http://example.org/org-a>`, `<…/org-b>` | Each organization's listings               |
| `<http://example.org/org-b-catalog>`    | Commodities only org-b trades                |
| `<urn:fluree:market:main#config>`       | Ledger config                                |

> The reference for the setting is
> [SHACL validation → Validating against other graphs](cookbook-shacl.md#validating-against-other-graphs);
> the config schema is in [Setting groups](../ledger-config/setting-groups.md#shacl-defaults).

## When to use it

| Situation | Use |
|-----------|-----|
| A small value-set owned by whoever writes the shapes | Keep it with the shapes ([shared value-sets](cookbook-shacl.md#shared-value-sets-with-shclass)). Only `sh:class` reads it. |
| Reference data maintained apart from the shapes, or needed by constraints other than `sh:class` (a path into it, a nested shape, a count, a SPARQL rule) | `f:additionalDataSource` |
| Data that belongs to the record itself | Write it to the record's graph |
| Querying across graphs | A [dataset or the union default graph](../query/datasets.md). These change what a query reads, not what validation reads. |

---

## 1. Create the ledger

```bash
fluree init
fluree create market
```

The ledger's full ID is `market:main`, so its config graph is
`urn:fluree:market:main#config`.

---

## 2. Shapes, catalog and config

A listing needs exactly one commodity, which must be an `ex:Commodity` and
whose status must be `"active"`. The catalog declares two commodities. It types
coffee as `ex:Arabica`, a subclass of `ex:Commodity` declared in the catalog
itself.

Save as `01-model.trig`:

```turtle
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix f:    <https://ns.flur.ee/db#> .
@prefix ex:   <http://example.org/> .

# ---- Shapes --------------------------------------------------------------
GRAPH <http://example.org/shapes> {
  ex:ListingShape a sh:NodeShape ;
    sh:targetClass ex:Listing ;
    sh:property [
      sh:path ex:commodity ;
      sh:minCount 1 ;
      sh:maxCount 1 ;
      sh:class ex:Commodity
    ] , [
      sh:path ex:commodity ;
      sh:node ex:ActiveCommodity ;
      sh:message "the commodity is not active in the catalog"
    ] .

  ex:ActiveCommodity a sh:NodeShape ;
    sh:property [ sh:path ex:status ; sh:hasValue "active" ] .
}

# ---- The shared catalog --------------------------------------------------
GRAPH <http://example.org/catalog> {
  ex:Arabica rdfs:subClassOf ex:Commodity .

  ex:coffee a ex:Arabica ;   ex:status "active" .
  ex:cocoa  a ex:Commodity ; ex:status "retired" .
}

# ---- Config: SHACL on, shapes from <shapes> --------------------------------
GRAPH <urn:fluree:market:main#config> {
  <urn:fluree:market:main:config:ledger> a f:LedgerConfig ;
    f:shaclDefaults <urn:fluree:market:main:config:shacl> .

  <urn:fluree:market:main:config:shacl>
    f:shaclEnabled true ;
    f:shapesSource [ a f:GraphRef ; f:graphSource [ f:graphSelector <http://example.org/shapes> ] ] .
}
```

```bash
fluree upsert -f 01-model.trig --format turtle
# → Committed t=1, 27 flakes
```

`--format turtle` is needed because `.trig` is not on the auto-detect list. The
config governs the transactions after the one that writes it.

The SHACL settings get an IRI of their own (`…:config:shacl`) rather than a
blank node, so a later step can add to them.

---

## 3. The problem: the catalog is not read

Save as `02-listing.trig`:

```turtle
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex:  <http://example.org/> .

GRAPH <http://example.org/org-a> {
  ex:listing-1 a ex:Listing ; ex:commodity ex:coffee .
}
```

```bash
fluree upsert -f 02-listing.trig --format turtle
```

```text
error: Transaction error: SHACL validation failed with 2 violation(s):
  1. Value coffee is not an instance of class Commodity
     Focus node: http://example.org/listing-1
     Path: http://example.org/commodity
     Constraint: http://www.w3.org/ns/shacl#ClassConstraintComponent
  2. the commodity is not active in the catalog
     Focus node: http://example.org/listing-1
     Path: http://example.org/commodity
     Constraint: http://www.w3.org/ns/shacl#NodeConstraintComponent
```

The listing is right; the check only reads `<org-a>`, which says nothing about
coffee.

---

## 4. Read the catalog with every write

Save as `03-sources.trig`:

```turtle
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:market:main#config> {
  <urn:fluree:market:main:config:shacl>
    f:additionalDataSource [ a f:GraphRef ; f:graphSource [ f:graphSelector <http://example.org/catalog> ] ] .
}
```

```bash
fluree upsert -f 03-sources.trig --format turtle
# → Committed t=2, 4 flakes
fluree upsert -f 02-listing.trig --format turtle
# → Committed t=3, 2 flakes
```

The same listing now passes. Both constraints read the catalog: `sh:class`
finds coffee's type and the `ex:Arabica` subclass edge there, and the nested
shape reads coffee's status there. A value-set kept with the shapes could have
satisfied `sh:class`, but not the status check.

The setting is on the ledger-wide `f:shaclDefaults`, so a write to any graph
reads `<catalog>`. `f:additionalDataSource` is repeatable: list as many graphs
as the shapes need.

---

## 5. What still fails

Save as `04-bad-listings.trig`:

```turtle
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex:  <http://example.org/> .

GRAPH <http://example.org/org-a> {
  ex:listing-2 a ex:Listing ; ex:commodity ex:cocoa .
  ex:listing-3 a ex:Listing ; ex:commodity ex:tea .
}
```

```bash
fluree upsert -f 04-bad-listings.trig --format turtle
```

```text
error: Transaction error: SHACL validation failed with 3 violation(s):
  1. Value tea is not an instance of class Commodity
     Focus node: http://example.org/listing-3
     ...
  2. the commodity is not active in the catalog
     Focus node: http://example.org/listing-3
     ...
  3. the commodity is not active in the catalog
     Focus node: http://example.org/listing-2
     ...
```

Cocoa is in the catalog but retired. Tea is not in the catalog at all.

---

## 6. Change the catalog and its records together

Save as `05-new-commodity.trig`:

```turtle
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex:  <http://example.org/> .

GRAPH <http://example.org/catalog> {
  ex:tea a ex:Commodity ; ex:status "active" .
}
GRAPH <http://example.org/org-a> {
  ex:listing-3 a ex:Listing ; ex:commodity ex:tea .
}
```

```bash
fluree upsert -f 05-new-commodity.trig --format turtle
# → Committed t=4, 4 flakes
```

Checks read the transaction's own changes, to the catalog as well as to the
record. Removing a catalog entry and writing a record that uses it, in one
transaction, fails.

---

## 7. Re-check records after the catalog changes

Save as `06-retire-coffee.trig`:

```turtle
@prefix ex: <http://example.org/> .

GRAPH <http://example.org/catalog> {
  ex:coffee ex:status "retired" .
}
```

```bash
fluree upsert -f 06-retire-coffee.trig --format turtle
# → Committed t=5, 2 flakes
```

This commits even though `ex:listing-1` now references a retired commodity.
A record is checked when it is written; an edit to the catalog does not
re-check the records that read it. To find the records a catalog change left
non-conforming, validate their graph:

```bash
fluree validate market --graph http://example.org/org-a
```

```text
Violation: http://example.org/listing-1
    path:      http://example.org/commodity
    component: NodeConstraintComponent
    message:   the commodity is not active in the catalog
    value:     {"@id":"http://example.org/coffee"}

Conforms: false — 1 violation(s), 0 warning(s), 0 info (3 shape(s) checked at t=5)
```

Validation reads the same graphs a transaction does, and exits non-zero when it
finds a violation, so it fits in CI or a scheduled job. The HTTP
[`/validate`](../api/endpoints.md#getpost-validateledger) endpoint behaves the
same. See [`fluree validate`](../cli/validate.md) for its options.

---

## 8. A list for one organization

Org-b also trades saffron, which the shared catalog does not carry. It keeps it
in a catalog of its own, and a per-graph override gives `<org-b>` its own list.

Save as `07-org-b.trig`:

```turtle
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix f:   <https://ns.flur.ee/db#> .
@prefix ex:  <http://example.org/> .

GRAPH <http://example.org/org-b-catalog> {
  ex:saffron a ex:Commodity ; ex:status "active" .
}

GRAPH <urn:fluree:market:main#config> {
  <urn:fluree:market:main:config:ledger> f:graphOverrides <urn:fluree:market:main:config:org-b> .

  <urn:fluree:market:main:config:org-b> a f:GraphConfig ;
    f:targetGraph <http://example.org/org-b> ;
    f:shaclDefaults [
      f:additionalDataSource
        [ a f:GraphRef ; f:graphSource [ f:graphSelector <http://example.org/catalog> ] ] ,
        [ a f:GraphRef ; f:graphSource [ f:graphSelector <http://example.org/org-b-catalog> ] ]
    ] .
}
```

```bash
fluree upsert -f 07-org-b.trig --format turtle
# → Committed t=6, 14 flakes
```

A saffron listing written to `<org-b>` now commits, and the same listing
written to `<org-a>` is rejected (`Value saffron is not an instance of class
Commodity`). A per-graph list replaces the ledger-wide list for that graph, so
org-b's list names `<catalog>` again.

---

## How it behaves

- **Which records are checked does not change.** A transaction checks the
  records it writes, and which shapes apply to a record is decided from the
  graph being written. A record typed `ex:Listing` only in the catalog is not
  checked as a listing when it is written to `<org-a>`, and the catalog's own
  nodes are not checked because `<org-a>` reads them.
- **A listed graph can make a record fail, not only pass.** The graphs are read
  as one, so a fact counts once wherever it is stored, and a fact only the
  catalog holds counts too: `sh:maxCount` counts the catalog's values for the
  record, and `sh:closed` sees the catalog's properties on it.
- **Checks read listed graphs in full**, whatever the writer's policy lets them
  read. A violation can show a writer a value from a graph their policy hides.
- **Only the config sets the list.** No request can change it, and only graphs
  of this ledger can be listed. Shapes can still come from another ledger
  through a cross-ledger `f:shapesSource`.

The [reference](cookbook-shacl.md#validating-against-other-graphs) lists the
full semantics.

## Troubleshooting

| Symptom | Cause |
|---------|-------|
| A data write fails with `f:additionalDataSource graph '…' not found in this ledger's graph registry` | The config names a graph the ledger does not have, usually a typo. The config write itself commits; data writes then fail until it is fixed, rather than silently reading nothing. A graph that the same transaction creates is found. |
| `f:additionalDataSource cannot name the config graph` (or `txn-meta`) | The system graphs cannot be listed. |
| `f:additionalDataSource with … is not yet supported` | `f:ledger`, `f:atT`, `f:trustPolicy` and `f:rollbackGuard` are not supported on this setting yet. |
| A record that should fail still commits after a catalog edit | Records are checked when written. Run `fluree validate --graph` on the graphs that read the catalog. |
| A shape does not apply to a record typed in the catalog | Shapes apply by what the record's own graph says. Type the record in its own graph. |
| A graph-specific list ignores the ledger-wide sources | A per-graph list replaces the ledger-wide one. List those graphs again in the override. |

## Related documentation

- [SHACL validation](cookbook-shacl.md) — shapes, constraints, modes and per-graph configuration
- [Setting groups — SHACL](../ledger-config/setting-groups.md#shacl-defaults) — `f:shaclDefaults` reference
- [Override control](../ledger-config/override-control.md) — how per-graph overrides merge
- [Datasets and named graphs](../concepts/datasets-and-named-graphs.md) — named graphs in Fluree
