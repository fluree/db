# Entities and relations

`fluree doc ingest` can go past structure and chunks and say what a document is *about*: which known things it mentions, where, and what it states about them. Two inputs ground that, and both are graphs you already have or can write in a few lines of Turtle.

```bash
fluree doc ingest ./memos -l memos --model ./ontology.ttl --entities people --entities orgs.ttl#schema:Organization
```

- `--model` is an ontology: the classes an entity may have and the properties a relation may use. A ledger or a `.ttl` / `.jsonld` file.
- `--entities` is a gazetteer: subjects with labels, to be found in the text by name. A ledger or a file, repeatable, optionally scoped to one class with `#Class`.

Either can be given alone. `--entities` without `--model` is a deterministic scan and needs no model at all. `--model` needs a language model in the `llm` slot, or a Fluree AI account.

## The one rule that matters: a known entity keeps its IRI

When a label from an `--entities` source is found in a document, the mention is written against **that subject's own IRI**. Nothing is copied, renamed or re-minted. A ledger of people and a ledger of documents about them then meet on the same nodes, and a query across both is a plain join:

```sparql
PREFIX doc: <https://ns.flur.ee/doc#>
PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>
PREFIX schema: <https://schema.org/>

SELECT ?person ?email ?file ?excerpt
FROM <people:main>
FROM <memos:main>
WHERE {
  ?person a schema:Person ; schema:email ?email .          # from the people ledger
  ?m nif:entity ?person ; nif:anchorOf ?excerpt ;          # from the memos ledger
     doc:sourceDocument ?d .
  ?d doc:relativePath ?file .
}
```

The language model is shown the entities the scan found in each chunk and told to use exactly those names, so its output reconciles to the same IRIs. An entity it names that no source knows is minted once, under an IRI derived from the name, so the same new name in two documents is one node. Those minted entities are typed `doc:Entity` and, on the next run, are themselves part of the gazetteer, so a corpus converges on one node per name. Entities flagged off-model are left out of that: a name the ontology has no class for is kept for review, not scanned for.

## What the scan does

Every label of every entity — `skos:prefLabel`, `skos:altLabel`, `skos:hiddenLabel`, `rdfs:label`, `schema:name`, `schema:legalName`, `schema:alternateName` — goes into one automaton. Each chunk is scanned for the longest whole-word match, case-folded, and scanned again over Snowball stems (`--lang`, default `en`) so "cities" finds an entry labelled "city". A match is a `doc:Mention` on the chunk: `nif:beginIndex` and `nif:endIndex` into the chunk's text, `nif:anchorOf` with the text as written, `nif:entity` pointing at the entity, and `doc:sourceElement` naming the paragraph or cell it sits in, which for a PDF carries page and box.

This is the deterministic half. It is exact, it is cheap, and with `--entities` alone it is the whole job.

## What the language model does

Per chunk, one call to the `llm` slot with the rendered ontology as the system prompt and, as the user turn, the entities already found in that chunk and the chunk's text. It answers with JSON: entities (name, class, coarse label, an exact excerpt, alternate names, literal attributes) and relations (subject, predicate, object, an exact excerpt). The prompt is the same one Fluree AI's hosted extraction uses, so an account and a local run make the same ask.

Everything it says is checked before it is written:

| The model said | What happens |
|---|---|
| An entity the gazetteer knows, under any of its labels | Resolved to the gazetteer's IRI. |
| An entity nobody knows, with an excerpt found in the chunk | Minted as `doc:Entity`, typed by the model's class if it is in the ontology, with a mention at the excerpt narrowed to the name. |
| An entity whose excerpt is not in the chunk | A hallucination. Dropped, along with any relation naming it. Counted in the output. |
| A new entity whose class is not in the ontology (`schema:Thing`, or anything else) | Kept as a bare `doc:Entity` flagged `doc:offModel true`, so a reviewer sees it and a query can leave it out. "March" and "platform engineering" are what a model returns when asked for more than the text holds; `--drop-off-model` (or `doc.extraction.drop_off_model`) drops them instead, for a project with no review step. An entity the gazetteer knows is kept whatever the model typed it. |
| A relation whose object names no entity in the chunk | Written reified with the object as a literal, never as an edge. The statement is evidence either way; an edge needs a node at both ends. |
| An attribute property not in the ontology, or one that ranges over a class | The attribute is dropped. |
| A relation whose predicate is an ontology property, as spelled | **valid**: written reified and, in `--relations direct` mode, as an edge. |
| A predicate spelled as a label, a local name, a compact form, or a class the property ranges over | **repaired** to the one property it can only mean, written the same way, with the original spelling kept on the node. Two candidates is a coin flip, so it is not repaired. |
| Anything else | **rejected**: written reified with the reason, never as an edge. |

Reified means a `doc:Relation` node with `rdf:subject`, `rdf:predicate`, `rdf:object`, the excerpt, the verdict, the chunk and the document. Every relation gets one, so a rejected predicate is evidence you can review and a repaired one shows what was repaired. `--relations reified` stops there and writes no edges at all; `--relations off` skips relations.

### Direct edges are claims, with evidence

Read a `doc:` ledger's direct edges as *what a document stated*, not as what is true. The gate above decides which predicates may be written, never which statements are correct, so any document you ingest can attach an admitted-predicate edge to any gazetteer entity it names by label — that is the feature, and it is why a memo naming a person joins to that person's node.

Every edge has a `doc:Relation` beside it carrying the excerpt, the verdict, the chunk and the source document, so the provenance of any claim is one hop away. A consumer that needs assertions rather than claims should read the reified nodes and apply its own trust rule — by source document, by verdict, or by corroboration across documents. `--relations reified` writes the evidence without the edges, which is the right mode when the corpus is not trusted.

## Deciding what to trust

Two signals are already on every relation node, both grounded in something checkable rather than in a model's opinion of itself, and both queryable with no extra ingest work.

**Corroboration** — how many independent documents state the same triple. The relation node carries `rdf:subject`, `rdf:predicate`, `rdf:object` and `doc:sourceDocument`, so counting sources per statement is one group-by.

**`doc:verdict`** — whether the predicate is one your ontology actually declares, as spelled (`valid`) or after an unambiguous repair (`repaired`), or not at all (`rejected`).

Composed, they give a traversal filter that admits only statements more than one source makes with a predicate the ontology has:

```sparql
PREFIX doc: <https://ns.flur.ee/doc#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?s ?p ?o (COUNT(DISTINCT ?src) AS ?sources) WHERE {
  ?s ?p ?o .
  ?rel a doc:Relation ;
       rdf:subject ?s ; rdf:predicate ?p ; rdf:object ?o ;
       doc:asserted true ; doc:verdict "valid" ;
       doc:sourceDocument ?src .
}
GROUP BY ?s ?p ?o
HAVING (COUNT(DISTINCT ?src) >= 2)
```

Keep the `GROUP BY`. An ungrouped aggregate over the same shape is a different query plan and is not what this was measured against.

### Source trust goes on the document node

There is no `doc:` term for "how much do I trust this source", because it is your judgement and not a fact the pipeline can observe. Write it yourself, on the document node, in your own vocabulary:

```sparql
PREFIX ex: <https://example.org/>
INSERT DATA {
  <urn:fluree:doc:policy-manual.pdf> ex:trust 1.0 .
  <urn:fluree:doc:forum-thread.html> ex:trust 0.3
}
```

and compose it with the evidence at query time, where the relation node's `doc:sourceDocument` is the join:

```sparql
PREFIX doc: <https://ns.flur.ee/doc#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ex:  <https://example.org/>
SELECT ?s ?p ?o (SUM(?t) AS ?score) WHERE {
  ?rel a doc:Relation ;
       rdf:subject ?s ; rdf:predicate ?p ; rdf:object ?o ;
       doc:verdict "valid" ;
       doc:sourceDocument ?src .
  ?src ex:trust ?t .
}
GROUP BY ?s ?p ?o
ORDER BY DESC(?score)
```

On the document node rather than baked onto the edge, and that is the whole point: **a trust judgement is the thing most likely to change.** One triple on one document node re-scores every claim that document ever made, including claims ingested years apart. A number written onto each derived edge at ingest time cannot be revised at all without rewriting every edge, and once written it is no longer decomposable — nothing can tell whether `0.4` meant "the source is unreliable", "the sentence hedged", or "they barely know each other".

Store the factors and multiply at query time. That way each one can be corrected on its own.

A re-ingest of the document leaves your triples alone. It retracts what the pipeline itself wrote — the `doc:`, `nif:` and `po:` terms, `rdf:type`, the reification triple and `rdfs:label` — and nothing else, on the document node and on every node stamped with it. Anything in your own vocabulary survives. (`rdfs:label` and extra `@type` values on a pipeline node do not: the emitters write those.)

### Trust filtering is a query concern, not a policy one

A policy cannot express "show me only edges above this score". Policy targets bind `?$this` to the **subject**, so the decision is all-or-nothing per subject: a person with one well-supported edge and one thin one is either entirely visible or entirely hidden. Filter in the query, as above.

## Assertion mode

`doc:assertionMode` records **how the source states a relation**: `asserted`, `hedged`, `attributed` or `negated`. It sits on the relation node beside `doc:verdict`.

It exists because corroboration and verdict, between them, cannot tell a hedged claim from a certain one. Two documents stating that Alice knows Bob — one saying *"because she is his wife"*, one saying *"reportedly may know"* — produce relation nodes that are otherwise identical.

**What it is not.** It is not a confidence score and must not be read as one. It says nothing about whether the source is right, nothing about how strong the relationship is, and nothing about how sure the model was. `attributed` is not "less true than" `asserted`; a well-attributed claim from a named authority may be your best evidence. There is deliberately no ordering and no number, for the same reason `doc:verdict` has neither.

**Asking for it.** The shipped prompt does not, because that prompt text goes out verbatim with Fluree AI's hosted extraction. Add it with your own `--system-prompt`, which keeps the `{model}` and `{guidance}` slots:

```text
For each relation, alongside the fields above, add:
  - "assertionMode": how the source states this relation. Exactly one of
      "asserted"   — the text states it plainly
      "hedged"     — "may", "reportedly", "is thought to", "appears to"
      "attributed" — the text reports someone else saying it
      "negated"    — the text denies it
    Classify what the excerpt does, not how sure you are.
```

A value outside those four is refused and counted in the run summary, not stored: an unrecognised mode that silently disappeared would be indistinguishable from a prompt that never asked.

**Why a classification and not a confidence.** "How confident are you?" asks the model to grade itself, which it is bad at, and folds three independent questions — did the text really say this, how hedged was it, how strong is the relation — into one number whose meaning is set by whatever prompt produced it. "Does the source assert, hedge, attribute or negate this?" is a question about text the model is holding, and it is the one axis this pipeline could not already express. If your prompt asks for a `confidence` field anyway, the run will tell you it was ignored.

## Re-runs

The document node records a fingerprint of the ontology, the gazetteer sources, the language model, the guidance and the relation mode. A document is unchanged only when that fingerprint is too, so editing the ontology re-extracts and editing nothing skips.

A re-ingest retracts the document's mentions and relation nodes and re-derives them. Minted entity nodes are shared between documents and are not retracted. An edge the earlier extraction asserted is kept only while some other relation, from any document, still supports it; otherwise it goes with the relation that produced it.

What it retracts is scoped to what the pipeline writes: the `doc:`, `nif:` and `po:` namespaces, plus `rdf:type`, `rdf:subject`/`rdf:predicate`/`rdf:object` and `rdfs:label`. Triples you added yourself on the document node or on a relation node survive, which is what makes [source trust](#source-trust-goes-on-the-document-node) usable.

Each chunk's answer is cached on the exact ask: the model, the ontology, the guidance, the known entities and the text. Re-running over an unchanged corpus with an unchanged setup makes no model calls.

Chunks are asked about several at a time (`--concurrency`, default 4). A chunk whose call fails keeps its gazetteer mentions and contributes nothing else; the document is still written, the run says so, and the document is not stamped as extracted, so the next run asks about that chunk again while the cache answers for the ones that succeeded.

## Guidance and custom prompts

Three levels, all of which can live in the project's `config.toml` so every run over the corpus makes the same ask, or be given as flags for one run:

```toml
[doc.extraction]
guidance = "prompts/guidance.md"          # priorities placed in the standard prompt
# system_prompt = "prompts/system.txt"    # replaces the system prompt; keep {model} and {guidance}
# user_prompt = "prompts/user.txt"        # replaces the user prompt; keep {existing} and {document}
# concurrency = 4
# drop_off_model = false
```

- **Guidance** (`--guidance`) places your own priorities in the standard prompt, between the extraction rules and the ontology: "the drawing's identity is the title-block part number", "express employment only as schema:worksFor from Person to Organization". It may outrank the model's own sense of what is significant; it cannot admit a predicate the ontology does not have. This is the lever for consistency across documents: the same words, every run.
- **A system prompt file** (`--system-prompt`) replaces the whole template. Keep `{model}`, where the ontology is rendered, and `{guidance}` if you still want the guidance slot.
- **A user prompt file** (`--user-prompt`) replaces the per-chunk ask. Keep `{existing}`, the known entities found in the chunk, and `{document}`, the chunk's text.

The prompt text is part of the extraction fingerprint and the cache key: a changed prompt re-extracts, an unchanged one skips. Paths in the config are relative to the project; a flag's path is relative to where the command runs.
