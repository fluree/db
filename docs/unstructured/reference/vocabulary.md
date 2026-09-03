# Vocabulary

Namespace `doc:` is `https://ns.flur.ee/doc#`. Structure elements additionally use DoCO (`doco:`, `http://purl.org/spar/doco/`), the pattern ontology it extends (`po:`, `http://www.essepuntato.it/2008/12/pattern#`) and NIF (`nif:`, `http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#`). The structure terms are those of the parsing engine and are documented in full in [fluree-doc-parse](https://github.com/fluree/fluree-doc-parse); the terms below are what the ingest adds.

## Classes

| Class | Meaning |
|---|---|
| `doc:Chunk` | A retrieval unit cut from a document's structure. |
| `doc:SourceDocument` | The file a graph was extracted from; the `doc:sourceDocument` target. |

## Chunk properties

| Property | Range | Meaning |
|---|---|---|
| `doc:chunkIndex` | integer | Position in the document. |
| `doc:text` | string | The chunk's text, whitespace collapsed. |
| `doc:headerPath` | string | Section titles above it, outermost first, joined with ` / `. Absent when there are none. |
| `doc:sourceElement` | IRI, repeated | The elements it was built from, in order. |
| `doc:sourceDocument` | IRI | The document. Also on every element. |
| `doc:embedding` | `@vector` | Its embedding, when produced. |

## Document properties

| Property | Range | Meaning |
|---|---|---|
| `doc:fileName` | string | The file's name. |
| `doc:relativePath` | string | Path relative to the ingested directory, `/`-separated. |
| `doc:sha256` | string | Hex digest of the bytes. |
| `doc:mediaType` | string | The media type inferred from the extension. |
| `doc:byteSize` | integer | Size in bytes. |
| `doc:pageCount` | integer | Pages, for sources that have them; `0` otherwise. |
| `doc:escalatedCrops` | integer | Crops a vision model read for this extraction. |
| `doc:parserRevision` | string | The fluree-doc-parse revision that produced the structure graph. |
| `doc:chunkCount` | integer | Chunks written. |
| `doc:embeddingModel` | string | The embedding model, when one ran. |
| `doc:embeddingDimensions` | integer | Its vector width. |
| `doc:ingestedAt` | `xsd:dateTime` | When this extraction was written. |

## Structure terms used by the chunker

`doco:Document`, `doco:BodyMatter`, `doco:Section`, `doco:SectionTitle`, `doco:Title`, `doco:Paragraph`, `doco:ListItem`, `doco:Caption`, `doco:Table`, `doc:TableCell` (`doc:cellValue`, `doc:rowHeader`, `doc:columnHeader`, `doc:rowIndex`, `doc:columnIndex`), `po:contains`, `nif:isString`, `nif:beginIndex`, `nif:endIndex`, `doc:sectionLevel`, `doc:pageIndex`, `doc:bbox`, `doc:pages`, `doc:evidence`, `doc:provenance`.
