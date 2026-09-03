# Querying the graph

`fluree doc search` is a convenience. Everything it does is a query you can write yourself, and the graph goes further than it shows.

## From a hit to the page

A chunk cites the elements it was built from; a PDF element knows its page and box:

```sparql
PREFIX doc: <https://ns.flur.ee/doc#>
PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>

SELECT ?el ?page ?box ?start ?end WHERE {
  <urn:fluree:doc:msa-2024.pdf/chunk/97> doc:sourceElement ?el .
  ?el doc:pageIndex ?page ; doc:bbox ?box ;
      nif:beginIndex ?start ; nif:endIndex ?end .
}
```

`doc:bbox` is `x0,y0,x1,y1` in PDF units with a top-left origin, and the document node's `doc:pages` gives each page's size, so a box scales onto a rendered page image without a coordinate flip. The offsets index into the document's text projection, the string every `nif:beginIndex` in the document shares.

## Section-scoped search

Containment is `po:contains`, so "chunks under this heading" is a traversal:

```sparql
PREFIX doc:  <https://ns.flur.ee/doc#>
PREFIX doco: <http://purl.org/spar/doco/>
PREFIX po:   <http://www.essepuntato.it/2008/12/pattern#>
PREFIX nif:  <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>

SELECT ?chunk ?text WHERE {
  ?section a doco:Section ; po:contains ?title .
  ?title a doco:SectionTitle ; nif:isString "12. Term and Termination" .
  ?section po:contains+ ?el .
  ?chunk a doc:Chunk ; doc:sourceElement ?el ; doc:text ?text .
}
```

## Across ledgers, on the entities you already have

A mention points at the entity's own IRI from the `--entities` source, so a dataset query joins the source ledger and the documents ledger with no mapping. Everything a memo says about a person, next to what the people ledger knows about them:

```sparql
PREFIX doc: <https://ns.flur.ee/doc#>
PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>
PREFIX schema: <https://schema.org/>

SELECT ?name ?email ?file ?said
FROM <people:main>
FROM <memos:main>
WHERE {
  ?person a schema:Person ; schema:name ?name ; schema:email ?email .
  ?m nif:entity ?person ; nif:anchorOf ?said ; doc:sourceDocument ?d .
  ?d doc:relativePath ?file .
}
```

Relations are reified with their evidence, so a review of what the model claimed and how it was judged is one query:

```sparql
PREFIX doc: <https://ns.flur.ee/doc#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?verdict ?s ?p ?o ?excerpt ?reason WHERE {
  ?r a doc:Relation ; doc:verdict ?verdict ;
     rdf:subject ?s ; rdf:predicate ?p ; rdf:object ?o ; doc:excerpt ?excerpt .
  OPTIONAL { ?r doc:rejectionReason ?reason }
} ORDER BY ?verdict
```

Admitted relations are also plain edges, so `?person schema:worksFor ?org` works as it would over hand-written data.

## Vector and full-text search by hand

The indexes are graph sources. A vector search takes a query vector — the same embedding endpoint the ingest used — and joins the hits to chunk data:

```json
{
  "@context": {"doc": "https://ns.flur.ee/doc#", "f": "https://ns.flur.ee/db#"},
  "from": "contracts:main",
  "where": [
    {"f:graphSource": "contracts-vectors:main", "f:queryVector": [0.012, -0.041, "…"],
     "f:searchLimit": 10, "f:searchResult": {"f:resultId": "?c", "f:resultScore": "?score"}},
    {"@id": "?c", "doc:text": "?text", "doc:headerPath": "?path", "doc:sourceDocument": "?d"}
  ],
  "select": ["?score", "?d", "?path", "?text"],
  "orderBy": [["desc", "?score"]]
}
```

Replace the first pattern with `{"f:graphSource": "contracts-text:main", "f:searchText": "termination notice", …}` for BM25. `fluree doc search --mode hybrid` runs both and fuses them by reciprocal rank; by hand you can do the same and add filters on `doc:sourceDocument`, `doc:headerPath`, the entities a chunk mentions (`?m nif:referenceContext ?c ; nif:entity <…>`) or anything else in the graph, which the command does not do.

## Time travel and branches

Every ingest is a commit. `fluree query contracts --at 3` sees the corpus as it was; `fluree branch create` gives an experiment its own copy to re-ingest with different chunking or a different model, and `fluree branch diff` shows what changed.
