# What gets built

One commit per document, holding four things, and a fifth when extraction is on.

## The structure graph

The parsing engine ([fluree-doc-parse](https://github.com/fluree/fluree-doc-parse)) turns every input into one element model, emitted as JSON-LD in the Document Components Ontology (DoCO) and the NLP Interchange Format (NIF), with Fluree-specific placement and evidence under `doc:` (`https://ns.flur.ee/doc#`).

- `doco:Document` → `doco:BodyMatter` → `doco:Section` (with `doco:SectionTitle` and `doc:sectionLevel`) → `doco:Paragraph`, `doco:ListItem`, `doco:Caption`, `doco:Table` → `doc:TableCell`. Containment is `po:contains`, an IRI-valued property, so the hierarchy is traversable.
- Every text-bearing element carries `nif:isString` (its text) and `nif:beginIndex` / `nif:endIndex`, character offsets into the document's plain-text projection.
- PDF elements carry `doc:pageIndex` (0-based) and `doc:bbox` (`"x0,y0,x1,y1"` in PDF units, top-left origin). The document node carries `doc:pages` with each page's size, the denominator for placing a box on a rendered page. Word, PowerPoint, Markdown and HTML declare their structure and carry no geometry.
- Table cells are addressable: `doc:rowIndex`, `doc:columnIndex`, and the row and column headers denormalised onto each cell, so "the Supply voltage row of the LM358B column" is a lookup, not a grid reconstruction.
- `doc:evidence` says which signal classified each element; `doc:provenance` is `vlm` for text a vision model transcribed.

PDF is the geometric path: structure inferred from glyph and rule positions, escalating to a vision model where the inference is weak. The other formats are read directly. All produce the same graph shape, so a mixed corpus lands under one schema.

## Chunks

A `doc:Chunk` is a retrieval unit cut along that structure. The chunker walks the graph and collects text from paragraphs, list items, captions and table cells until a chunk reaches `--min-chars` (default 800), closing early at a heading once it is at least half full so a chunk rarely straddles sections. A single element longer than `--max-chars` (default 2000) is split at sentence boundaries. Table cells are embedded with their headers: `Supply voltage / LM358B: 3 V`.

| Property | Meaning |
|---|---|
| `doc:text` | The chunk's text, whitespace collapsed. |
| `doc:headerPath` | The section titles above it, outermost first, joined with ` / `. Also prefixed to the embedding input. |
| `doc:sourceElement` | The elements it was built from, in order. |
| `doc:sourceDocument` | The document, and the tag a re-ingest retracts by. |
| `doc:chunkIndex` | Its position in the document. |
| `doc:embedding` | Its vector, when an embedding model ran. An `@vector` literal, stored as 32-bit floats. |

## The document node

A `doc:SourceDocument` at the document's IRI records the file: `doc:fileName`, `doc:relativePath`, `doc:sha256`, `doc:mediaType`, `doc:byteSize`, `doc:pageCount`, `doc:escalatedCrops`, `doc:parserRevision`, `doc:chunkCount`, `doc:embeddingModel`, `doc:embeddingDimensions`, `doc:ingestedAt`, and, when extraction ran, `doc:extractionModel`, `doc:extractionFingerprint`, `doc:mentionCount`, `doc:entityCount`, `doc:relationCount`. It is what a later run compares against to decide whether the document is unchanged.

## Entities, mentions and relations

With `--model` and/or `--entities`, the same commit also holds what the document is about. A `doc:Mention` is a span of a chunk naming an entity: `nif:beginIndex` / `nif:endIndex` into the chunk's text, `nif:anchorOf`, `nif:referenceContext` (the chunk), `nif:entity` (the entity, under the IRI it already has in your `--entities` source), and `doc:sourceElement`. A `doc:Relation` is one statement the language model reported, reified with `rdf:subject`, `rdf:predicate`, `rdf:object`, its excerpt and the gate's verdict; admitted relations are also written as plain edges. An entity no source knew is minted as `doc:Entity` with `schema:name`, `skos:altLabel`, `doc:nerLabel` and any attributes the ontology admits. See [Entities and relations](entities-and-relations.md).

## IRIs

Documents are minted as `<base-iri><relative path>`, default `urn:fluree:doc:` plus the path relative to the folder you ingested, percent-encoded but with `/` kept. Elements are `<document>/element/<n>` and `<document>/section/<n>` in emission order; chunks are `<document>/chunk/<n>`, mentions `<chunk>/mention/<n>`, relations `<document>/relation/<n>`, and minted entities `<base-iri>entity/<hash of the name>`. Because the document IRI depends only on where the file sits, it survives re-runs, and `--base-iri` lets you put a corpus under your own namespace.

## The indexes

Over the chunks, two graph sources: a BM25 full-text index named `<ledger>-text`, and, when embeddings were produced, an HNSW vector index named `<ledger>-vectors`. They are Fluree graph sources like any other and can be queried directly with `f:searchText` and `f:queryVector` patterns; `fluree doc search` is a convenience over them.
