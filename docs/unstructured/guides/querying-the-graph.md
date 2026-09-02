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

Replace the first pattern with `{"f:graphSource": "contracts-text:main", "f:searchText": "termination notice", …}` for BM25. Run both and merge in your application for hybrid retrieval, or add filters on `doc:sourceDocument`, `doc:headerPath` or anything else in the graph.

## Time travel and branches

Every ingest is a commit. `fluree query contracts --at 3` sees the corpus as it was; `fluree branch create` gives an experiment its own copy to re-ingest with different chunking or a different model, and `fluree branch diff` shows what changed.
