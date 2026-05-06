# Cookbook: `owl:imports` across named graphs

This walkthrough builds a small two-file ontology, links it together with
`owl:imports`, applies it to instance data, and shows OWL 2 QL and OWL 2 RL
inference firing through the import.

In Fluree, an `owl:imports` target must resolve to **another named graph in the
same ledger** (or to a local graph via `f:ontologyImportMap`). Cross-ledger
imports are not supported. This tutorial uses three named graphs in one ledger:

| Graph IRI                                  | Role                                |
|--------------------------------------------|-------------------------------------|
| *(default graph)*                          | Instance data                       |
| `<http://example.org/onto/core>`           | Core ontology — class hierarchy + `owl:imports` hub |
| `<http://example.org/onto/behaviors>`      | Imported ontology — property characteristics |
| `<urn:fluree:demo:main#config>`            | Ledger config — wires up reasoning  |

A runnable end-to-end script lives at
[`cookbook-owl-imports.sh`](cookbook-owl-imports.sh) — it executes every
command in this tutorial and asserts the expected rows.

> Reasoning is currently **experimental**. See
> [Reasoning and inference](../concepts/reasoning.md) for background and
> [Setting groups → reasoningDefaults](../ledger-config/setting-groups.md)
> for the full config schema.

---

## 1. Create the ledger

```bash
fluree init
fluree create demo
```

`demo` becomes the active ledger. Its full ID is `demo:main`, which means the
config named graph IRI is `urn:fluree:demo:main#config` (the `#config`
fragment is a Fluree convention).

---

## 2. Insert instance data into the default graph

Save as `01-data.ttl`:

```turtle
@prefix ex: <http://example.org/> .

# People (typed directly, will be classified further by reasoning)
ex:alice  a ex:GradStudent .
ex:bob    a ex:Person .
ex:carol  a ex:Professor .

# Ancestor chain — exercises owl:TransitiveProperty (declared in the import)
ex:alice  ex:hasAncestor ex:eve .
ex:eve    ex:hasAncestor ex:frank .

# Living arrangement — exercises owl:SymmetricProperty
ex:alice  ex:livesWith   ex:bob .

# Parent/child — exercises owl:inverseOf
ex:carol  ex:parentOf    ex:alice .

# Teaching — exercises rdfs:domain / rdfs:range
ex:professor1 ex:teaches ex:cs101 .
```

Insert it:

```bash
fluree upsert -f 01-data.ttl
# → Committed t=1, 8 flakes
```

> Use `upsert` (not `insert`) for any TriG document that contains `GRAPH`
> blocks. `fluree insert` takes a fast path that only understands plain
> Turtle. `upsert` routes Turtle/TriG through the parser that recognises
> `GRAPH` blocks. Plain Turtle works through either command.

---

## 3. Stage the ontology and reasoning config (TriG)

Save as `02-ontology.trig`:

```turtle
@prefix f:    <https://ns.flur.ee/db#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex:   <http://example.org/> .

# ---- Core ontology: class hierarchy + owl:imports hub -----------------
GRAPH <http://example.org/onto/core> {
  <http://example.org/onto/core>
      a owl:Ontology ;
      owl:imports <http://example.org/onto/behaviors> .

  ex:Student      rdfs:subClassOf  ex:Person .
  ex:GradStudent  rdfs:subClassOf  ex:Student .
  ex:Professor    rdfs:subClassOf  ex:Person .
}

# ---- Imported ontology: property characteristics + domain/range -------
GRAPH <http://example.org/onto/behaviors> {
  ex:hasAncestor  a              owl:TransitiveProperty .
  ex:livesWith    a              owl:SymmetricProperty .
  ex:parentOf     owl:inverseOf  ex:childOf .
  ex:teaches      rdfs:domain    ex:Professor ;
                  rdfs:range     ex:Course .
}

# ---- Reasoning configuration ------------------------------------------
# schemaSource = <onto/core>, followOwlImports = true
# → reasoner walks the import closure and projects schema triples from
#   BOTH graphs onto the default graph for inference.
GRAPH <urn:fluree:demo:main#config> {
  <urn:demo:cfg>
      a f:LedgerConfig ;
      f:reasoningDefaults <urn:demo:cfg:reasoning> .

  <urn:demo:cfg:reasoning>
      f:schemaSource      <urn:demo:cfg:schemaref> ;
      f:followOwlImports  true .

  <urn:demo:cfg:schemaref>
      a f:GraphRef ;
      f:graphSource <urn:demo:cfg:schemasrc> .

  <urn:demo:cfg:schemasrc>
      f:graphSelector <http://example.org/onto/core> .
}
```

Submit it:

```bash
fluree upsert -f 02-ontology.trig --format turtle
# → Committed t=2, 17 flakes
```

`--format turtle` is needed because the file extension `.trig` is not on the
auto-detect list; the parser treats the contents as Turtle/TriG.

---

## 4. Verify base data

Without reasoning, only asserted facts are returned:

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?s",
  "where":{"@id":"?s","@type":"ex:Person"},
  "reasoning":"none"
}'
# → ["ex:bob"]
```

Only `bob` is *directly* typed `Person`. The schema and the rest of the
classifications are still hidden behind reasoning.

---

## 5. RDFS subclass expansion

`rdfs:subClassOf` is declared in `<onto/core>` (the schemaSource).
With RDFS reasoning, querying for `Person` returns every subclass instance:

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?s",
  "where":{"@id":"?s","@type":"ex:Person"},
  "reasoning":"rdfs"
}'
# → ["ex:bob", "ex:carol", "ex:alice"]
```

`alice` (GradStudent → Student → Person) and `carol` (Professor → Person)
are now classified through the hierarchy.

---

## 6. OWL 2 RL inference *through* the import

Everything below uses axioms declared **in the imported `<onto/behaviors>`
graph** — they reach the reasoner only because `owl:imports` resolved
correctly.

### 6.1 `owl:TransitiveProperty` — `ex:hasAncestor`

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?a",
  "where":{"@id":"ex:alice","ex:hasAncestor":"?a"},
  "reasoning":"owl2rl"
}'
# → ["ex:eve", "ex:frank"]
```

Asserted: `alice → eve`, `eve → frank`. Inferred via the
TransitiveProperty axiom in the imported graph: `alice → frank`.

### 6.2 `owl:SymmetricProperty` — `ex:livesWith`

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?p",
  "where":{"@id":"ex:bob","ex:livesWith":"?p"},
  "reasoning":"owl2rl"
}'
# → ["ex:alice"]
```

Only `alice livesWith bob` was asserted; the symmetric pair is inferred.

### 6.3 `owl:inverseOf` — `parentOf` / `childOf`

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?p",
  "where":{"@id":"ex:alice","ex:childOf":"?p"},
  "reasoning":"owl2rl"
}'
# → ["ex:carol"]
```

Asserted: `carol parentOf alice`. Inferred: `alice childOf carol`.

### 6.4 `rdfs:domain` / `rdfs:range`

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?p",
  "where":{"@id":"?p","@type":"ex:Professor"},
  "reasoning":"owl2rl"
}'
# → ["ex:carol", "ex:professor1"]
```

`professor1` was never typed. The reasoner infers it from
`teaches rdfs:domain Professor` (declared in the imported graph) plus the
asserted `professor1 teaches cs101`.

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?c",
  "where":{"@id":"?c","@type":"ex:Course"},
  "reasoning":"owl2rl"
}'
# → ["ex:cs101"]
```

Same idea on the range side: `cs101` is classified as a Course because of
`teaches rdfs:range Course` in the import.

---

## 7. OWL 2 QL — query rewriting only

OWL 2 QL handles the same constructs as RDFS plus `owl:inverseOf` and
`rdfs:domain`/`range`, but at **query rewrite time** rather than via fact
materialisation. For the patterns above where you query the *inferred*
direction directly, OWL 2 RL is the simpler choice. OWL 2 QL is best when
you want zero materialisation and your queries already align with the
rewriting (e.g., asking for any superclass type).

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?s",
  "where":{"@id":"?s","@type":"ex:Person"},
  "reasoning":"owl2ql"
}'
# → ["ex:bob", "ex:carol", "ex:alice"]
```

Same answer as RDFS for this pattern, with no materialisation step.

---

## 8. Full chain: combining modes

Combining `rdfs` + `owl2rl` lets schema hierarchy and forward-chained facts
work together. `professor1` appears as a `Person` via:

1. `teaches rdfs:domain Professor`           (imported axiom, OWL 2 RL)
2. `professor1 teaches cs101`                (asserted)
3. ⇒ `professor1 a Professor`                (derived)
4. `Professor rdfs:subClassOf Person`        (core ontology, RDFS)
5. ⇒ `professor1 a Person`                   (derived)

```bash
fluree query --format json '{
  "@context":{"ex":"http://example.org/"},
  "select":"?s",
  "where":{"@id":"?s","@type":"ex:Person"},
  "reasoning":["rdfs","owl2rl"]
}'
# → ["ex:bob", "ex:carol", "ex:professor1", "ex:alice"]
```

---

## Submitting TriG over the HTTP API

The CLI's `upsert` command is one way to load TriG. Against a running
`fluree-db-server`, the same payload goes through the HTTP API. Both
endpoints below accept Turtle/TriG when sent with `Content-Type:
application/trig` (or `text/turtle`):

```bash
# Connection-scoped (specify ledger via query string)
curl -X POST 'http://localhost:8090/fluree/upsert?ledger=demo:main' \
     -H 'Content-Type: application/trig' \
     --data-binary @02-ontology.trig

# Ledger-scoped path form
curl -X POST 'http://localhost:8090/fluree/upsert/demo:main' \
     -H 'Content-Type: application/trig' \
     --data-binary @02-ontology.trig
```

The same TriG `GRAPH` blocks land in the same named graphs as via the CLI;
nothing else changes about the reasoning wiring.

See [HTTP endpoints](../api/endpoints.md) for the full surface area and
[Datasets and named graphs](../concepts/datasets-and-named-graphs.md) for
how named graphs participate in queries.

---

## What was actually proved

Each query above is a load-bearing test that the import closure is being
walked correctly:

| Query                              | Axiom location                | Without `owl:imports` resolution it would… |
|------------------------------------|-------------------------------|--------------------------------------------|
| §6.1 transitive ancestors          | imported graph (`behaviors`)  | …only return `ex:eve` (no transitive closure) |
| §6.2 symmetric `livesWith`         | imported graph                | …return empty (`bob livesWith alice` not asserted) |
| §6.3 `childOf` via inverse         | imported graph                | …return empty (`childOf` is never asserted) |
| §6.4 domain/range classification   | imported graph                | …not classify `professor1` / `cs101` |

If you change `f:followOwlImports` to `false` in the config graph, every
query in §6 except `bob livesWith` collapses back to base data — a useful
toggle for confirming the closure walk is what's doing the work.

## Related references

- [Concepts: Reasoning and inference](../concepts/reasoning.md)
- [Query-time reasoning syntax](../query/reasoning.md)
- [Setting groups → reasoningDefaults](../ledger-config/setting-groups.md)
- [Design: ontology imports](../design/ontology-imports.md)
- [Concepts: Datasets and named graphs](../concepts/datasets-and-named-graphs.md)
