# Edge annotations — storage internals

User-facing surface and contract live in [Edge annotations (concept doc)](../concepts/edge-annotations.md). This page is for contributors: how an annotation is stored, how the index resolves it, and what the transaction path maintains.

## The link is the record

An annotation is stored as RDF 1.2 defines it: the reifier's link to the triple it reifies, plus the reifier's own properties.

```text
ex:alice  ex:knows     ex:bob .                          # the base edge
ex:claim1 rdf:reifies  <<( ex:alice ex:knows ex:bob )>> .  # the link
ex:claim1 ex:confidence 0.8 .                            # the body
```

The link is an ordinary flake whose object is a triple term (`FlakeValue::TripleTerm`, datatype `f:tripleTerm`). It rides the same pipeline as every other flake — commits, replay, history, policy, time travel — and lives in the graph of the edge it names. Nothing about it is annotation-specific below the write surfaces and the term dictionary.

A triple term carries its object's datatype and language tag, so `"chat"@fr` and `"chat"@en` are different terms; the term is the edge identity that cascade, hydration and export compare on.

## Writers

Every write surface produces the link directly. As in RDF 1.2, only the annotation syntax (`s p o ~ r`, `s p o {| … |}`) asserts the triple it reifies; a reified triple (`<< s p o >>`, `r rdf:reifies <<( s p o )>>`, JSON-LD `@reifies`) does not.

- **Turtle / TriG / N-Quads** (`FlakeSink`, `ImportSink`, TriG import): `reified_triple_link` builds the link flake for each `~ r`, `{| … |}`, `<< s p o ~ r >>` and `r rdf:reifies <<( s p o )>>`; the parser emits the base triple itself for the annotation syntax. The Turtle→JSON-LD conversion behind upsert and graph sync writes a reification whose triple the document does not assert as a `@reifies` node.
- **SPARQL UPDATE**: `expand_annotated_triples` desugars an annotation tail as the spec does, to the base triple plus `r rdf:reifies <<( s p o )>>`. Template lowering turns the triple term into `TemplateTerm::TripleTerm`, whose positions resolve per solution; WHERE lowering turns it into the reifier pattern queries use.
- **Cypher**: `CREATE (a)-[r:T]->(b)` writes `a T b` and `r rdf:reifies <<( a T b )>>` with `r` a fresh reifier.
- **JSON-LD**: JSON-LD has no triple-term syntax, so `@annotation` / `@edge` lower (before expansion) to `f:reifies*` slot keys on a sibling node, and a node's `@reifies` (`{"@id": s, p: o}`, or an array of them) to slot keys on the node itself. After parsing, `fold_slots_into_links` turns each reifier's slots into its link template, and bulk import's `ImportSink` does the same with the slot triples it receives. The slots are an intermediate form and are never stored. A delete-by-selector matches the existing link with the query-side `@reifies` form.

The `f:reifies*` predicates are reserved: the write surfaces reject user-authored ones.

## Reading a link: the term dictionary

The index stores a link's object as a triple-term handle, `(inner p_id << 32) | seq`, interned in the term dictionary with its components. The dictionary keeps two reverse trees, subject-first and object-first, so a pattern with a bound subject or object finds its terms without reading every link. Live link counts per inner predicate go in `IndexStats.links` for the planner.

A decimal, big-integer or vector object is keyed in a term by the string id of its canonical form (`lexical_term_object`), not by an arena handle, which names a value only within one graph and predicate.

Query lowering of annotation patterns is described in [Annotation patterns read the link](#annotation-patterns-read-the-link).

## Transaction-time rules

`cascade_attachment_retracts` keeps links pointing at live edges:

1. A retracted triple retracts every link naming it (a POST probe on `rdf:reifies` with the term as the object).
2. A transaction that retracts all of a reifier's body retracts its links.
3. A reifier left with no link loses its body when it is a blank node, or in LPG mode (`opts.lpgEdgeLifecycle`, which Cypher `DELETE` sets); an IRI reifier's body otherwise stays as ordinary RDF.

The cascade is a Fluree rule, not an entailment: RDF 1.2 does not delete a reifier's statements when the triple it reifies is deleted. Annotation-syntax reads rely on it (see below). A ledger that has never held an annotation pays nothing: `snapshot.has_annotations` and `Novelty::has_annotations` gate the pass.

A reifier may reify several triples. Re-pointing one is a retract of the old link and an assert of the new; a JSON-LD upsert does that for a reifier it names. In LPG mode, an empty `@annotation: {}` mints a fresh property-less reifier so the relationship keeps an identity; in RDF mode it writes nothing.

## Annotation patterns read the link

Every annotation pattern reads the link. Reified-triple patterns (`<< s p o ~ ?r >>`, `?r rdf:reifies <<( s p o )>>`, JSON-LD `@reifies`) lower to it directly: `lower_reified_link` (`fluree-db-query/src/ir/term_components.rs`), shared by the SPARQL and JSON-LD lowerings, emits the reifier's `rdf:reifies` link, `?r rdf:reifies ?t`, plus `TermComponents(?t, s, p, o)` relating the term to its components and a `sameTerm` filter per constant component, which the planner turns into the scan's handle interval. A fully constant edge composes to a constant term instead.

Annotation syntax (`s p o ~ ?r {| ... |}`, JSON-LD `@annotation`, Cypher relationship properties) keeps its `Pattern::EdgeAnnotation` through lowering, and `expand_edge_annotation_patterns_for` (`where_plan.rs`) expands it at planning into the body, the link with its term components (`link_patterns`), and the base edge: the annotation syntax asserts its triple, so a reifier of an unasserted triple must not match it. The base edge comes last in the chain, so where estimates tie it is a bound existence probe. The chain is wrapped in `Pattern::DefaultGraphSource` only when the default graph is a union of two or more graphs (`PlanningContext::default_graph_union`).

A reified-triple pattern names its triple without joining it, so visibility is checked on the term: `QueryPolicyEnforcer` lets a flake whose object is a triple term through only when the triple that term names would be visible, recursively for nested terms. A policy hiding `ex:worksFor` therefore hides the links to `ex:worksFor` edges on every route.

A link is ordinary data: wildcard scans (`?s ?p ?o`) and wildcard hydration return it like any triple, and hydration renders its triple term as an embedded node. An `@annotation` body leaves its reifier's link out, since the body hangs from it, and so do Cypher property maps, where the link is the relationship itself.

Hydration (`@annotation` in subject expansion) and export read links the same way: hydration probes `rdf:reifies` with the rendered edge's term; export reads the ledger's live links once and writes a `~ <r>` marker (`@annotation` in JSON-LD) on each base edge it reaches. A link whose triple its graph does not assert has no edge to carry a marker, so export writes it as the link itself: `r rdf:reifies <<( s p o )>>`, or `@reifies` in JSON-LD. CONSTRUCT does the same: `?r rdf:reifies <<( … )>>` in a template writes the reification without the triple (`ConstructTemplate::push_reified_pattern`).

## Ledgers written before links

Earlier releases stored an annotation as an `f:reifies*` bundle (`f:reifiesSubject`, `f:reifiesPredicate`, `f:reifiesObject`, …) on the reifier. Those bundles stay in commits, hidden from wildcard scans and hydration unless `opts.includeSystemFacts` asks for them; the scan finds their predicate ids once per index store (`scan_hidden_p_ids`), so a ledger without bundles checks no row. The link is derived from them wherever it is needed: an index build derives it from each commit's bundle ops (`link_synth`), and novelty derives it for commits the index has not covered (`fluree-db-novelty/src/links.rs`). Readers see only links. A write that retracts a derived link leaves the bundle in place; the retract cancels the derived assert in novelty and at the next build alike.

Index roots from those releases may also carry an annotation-arena section. Readers skip it, keeping only the arena's two branch CIDs, and the next index build releases the arena's blobs as garbage.

An annotated index built before links has no term dictionary. Link reads on it fail asking for a rebuild (`fluree reindex`) rather than answering without its annotations. An incremental build over it declines once its window holds a triple term — a term dictionary covering the window alone would lift that refusal — and the index build falls back to a full rebuild, which links every annotation in history.

## See also

- [Edge annotations (concept doc)](../concepts/edge-annotations.md) — the user-facing surface.
- [Index format](index-format.md) — fact indexes, dictionary trees, root layout.
