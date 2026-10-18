# What gets built

One commit per document, holding four things, and a fifth when extraction is on.

## The structure graph

The parsing engine ([fluree-doc-parse](https://github.com/fluree/fluree-doc-parse)) turns every input into one element model, emitted as JSON-LD in the Document Components Ontology (DoCO) and the NLP Interchange Format (NIF), with Dublin Core for what a file says about itself and Fluree's own terms under `doc:` (`https://ns.flur.ee/doc#`) for what no standard ontology names. `fluree doc parse <file>` prints this graph for one file without writing anything.

- `foaf:Document` → `doco:BodyMatter` → `doco:Section` → `doco:Paragraph`, `doco:List`, `doco:Table` → `doc:TableCell`, and `doco:Figure`. Containment is `po:contains`, an IRI-valued property, so the hierarchy is traversable.
- A section holds its `doco:SectionTitle` by `po:containsAsHeader`, as DoCO specifies, and by `po:contains` too, so everything a section holds is one hop; `doc:sectionLevel` is its depth. A title the file declares for itself (Word's Title style, a deck's title slide) is a `doco:Title`, held the same way by the document root.
- A list's items are the `doco:Paragraph` nodes its `doco:List` contains: DoCO has no list-item class. Reading order is `nif:beginIndex`.
- Every text-bearing element carries `nif:isString` (its text) and `nif:beginIndex` / `nif:endIndex`, character offsets into the document's plain-text projection (`fluree doc parse <file> --format text`).
- PDF elements carry `doc:pageIndex` (0-based) and `doc:bbox` (`"x0,y0,x1,y1"` in PDF units, top-left origin). The root carries `doc:pages` with each page's size, the denominator for placing a box on a rendered page. Word, PowerPoint, Excel, Markdown, HTML, transcripts and email declare their structure and carry no geometry; a slide's or a sheet's index is its `doc:pageIndex`.
- Table cells are addressable: `doc:rowIndex`, `doc:columnIndex`, `doc:cellValue`, and the row and column headers denormalised onto each cell, so "the Supply voltage row of the LM358B column" is a lookup, not a grid reconstruction. A cell's offsets slice the projection to its value.
- The root records what the file declares about itself — `dcterms:title`, `dcterms:creator`, `dcterms:created`, `dcterms:modified`, from a PDF's information dictionary, an Office file's core properties, an HTML page's `<title>` and author, or an email's headers — and what was read: `doc:sha256`, `doc:sourceName`, and, where they apply, `doc:runningText` (a PDF's running header and footer) and `doc:unreadPages` (pages nothing could transcribe).
- A transcript turn is a `doco:Paragraph` with `doc:speaker`, `doc:startMs` and `doc:endMs`. An email is split into `doc:Message` nodes with their senders and recipients as `doc:Mailbox` nodes; its attachments are described in `doc:attachments` but not read — ingest each as a document of its own.
- `doc:evidence` says which signal classified each element. Text a vision model read carries `page-tier` (a whole page), `region-splice` or `region-chart` (a region), or `shape-arbitrated` (a table whose model reading replaced the deterministic one).

PDF is the geometric path: structure inferred from glyph and rule positions, escalating to a vision model where the inference is weak. The other formats are read directly. All produce the same graph shape, so a mixed corpus lands under one schema. The engine's [vocabulary reference](https://github.com/fluree/fluree-doc-parse/blob/main/docs/reference/vocabulary.md) lists every term.

## Chunks

A `doc:Chunk` is a retrieval unit cut along that structure. The chunker walks the graph and collects text from the document's title, paragraphs (a list's items among them) and table cells until a chunk reaches `--min-chars` (default 1500), closing early at a heading once it is at least half full so a chunk rarely straddles sections. A single element longer than `--max-chars` (default 4000) is split at sentence boundaries. Table cells are embedded with their headers: `Supply voltage / LM358B: 3 V`.

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

Documents are minted as `<base-iri><relative path>`, default `urn:fluree:doc:` plus the path relative to the folder you ingested, percent-encoded but with `/` kept. Everything minted for a document extends its IRI with a hyphen, in emission order: structure nodes are `<document>-element-<n>`, `<document>-section-<n>`, `<document>-table-<n>`, `<document>-cell-<n>` and so on; chunks are `<document>-chunk-<n>`, mentions `<chunk>-mention-<n>`, relations `<document>-relation-<n>`. Minted entities are `<base-iri>entity/<hash of the name>`, shared across documents.

No `/`, `#` or `:` is added, on purpose. Fluree stores an IRI as a namespace (everything up to the last of those characters) and a name, and encodes each distinct namespace once; a delimiter after the document IRI would cost a namespace per document and per kind of node. As minted, a corpus costs one namespace per directory its files sit in, and one more for its minted entities. Because the document IRI depends only on where the file sits, it survives re-runs, and `--base-iri` lets you put a corpus under your own namespace.

A re-ingest retracts by `doc:sourceDocument`, which every node above carries except a table cell: a cell is always one `po:contains` below its table, which does, and is retracted through it.

## The indexes

Over the chunks, one graph source: a BM25 full-text index named `<ledger>-text`. It is a Fluree graph source like any other and can be queried directly with an `f:searchText` pattern; `fluree doc search --mode text` is a convenience over it.

There is no vector index. Similarity runs over the `doc:embedding` values themselves — see [vector search](vector-search.md).
