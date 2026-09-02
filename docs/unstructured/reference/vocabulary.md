# Vocabulary

Namespace `doc:` is `https://ns.flur.ee/doc#`. Structure elements additionally use DoCO (`doco:`, `http://purl.org/spar/doco/`), the pattern ontology it extends (`po:`, `http://www.essepuntato.it/2008/12/pattern#`) and NIF (`nif:`, `http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#`). The structure terms are those of the parsing engine and are documented in full in [fluree-doc-parse](https://github.com/fluree/fluree-doc-parse); the terms below are what the ingest adds.

## Classes

| Class | Meaning |
|---|---|
| `doc:Chunk` | A retrieval unit cut from a document's structure. |
| `doc:SourceDocument` | The file a graph was extracted from; the `doc:sourceDocument` target. |
| `doc:Mention` | A span of a chunk naming an entity. Also typed `nif:RFC5147String`. |
| `doc:Relation` | One relation the language model reported, reified with its evidence and verdict. |
| `doc:Entity` | An entity extraction minted because no `--entities` source knew it. Known entities keep their own IRI and get no node here. |

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
| `doc:chunking` | string | `min/max` characters the chunks were cut with. |
| `doc:embeddingModel` | string | The embedding model, when one ran. |
| `doc:embeddingDimensions` | integer | Its vector width. |
| `doc:ingestedAt` | `xsd:dateTime` | When this extraction was written. |
| `doc:extractionModel` | string | The language model that extracted, when one ran. |
| `doc:extractionFingerprint` | string | Hash of the ontology, entity sources, model, guidance and relation mode. |
| `doc:mentionCount` | integer | Mentions written. |
| `doc:entityCount` | integer | Distinct entities mentioned, known and new. |
| `doc:relationCount` | integer | Relations written, every verdict included. |

## Mention properties

| Property | Range | Meaning |
|---|---|---|
| `nif:beginIndex`, `nif:endIndex` | integer | Character offsets into the chunk's `doc:text`. |
| `nif:anchorOf` | string | The text as written. |
| `nif:referenceContext` | IRI | The chunk. |
| `nif:entity` | IRI | The entity, under the IRI its source gave it. |
| `doc:sourceElement` | IRI | The structure element the span sits in. |
| `doc:extractedBy` | string | `gazetteer` or `llm`. |
| `doc:sourceDocument` | IRI | The document. |

## Relation properties

| Property | Range | Meaning |
|---|---|---|
| `rdf:subject`, `rdf:predicate` | IRI | The statement. |
| `rdf:object` | IRI or literal | The statement's object: an entity, or the literal the model gave when the object was a value or named no entity in the chunk. |
| `rdfs:label` | string | `subject | predicate label | object`. |
| `doc:excerpt` | string | The text supporting it, as the model quoted it. |
| `doc:verdict` | string | `valid`, `repaired` or `rejected`. |
| `doc:asserted` | boolean | Whether a direct edge was written for it. |
| `doc:originalPredicate`, `doc:repairNote` | string | On a repaired relation: what the model wrote, and how it was resolved. |
| `doc:rejectionReason` | string | On a rejected relation. |
| `doc:sourceChunk`, `doc:sourceDocument` | IRI | Where it was found. |

## Minted entity properties

| Property | Range | Meaning |
|---|---|---|
| `schema:name` | string | The canonical name. |
| `skos:altLabel` | string, repeated | Other surface forms seen. |
| `doc:nerLabel` | string | Coarse label: PERSON, ORG, GPE, LOC, FAC, EVENT, PRODUCT, WORK_OF_ART, LANGUAGE, MISC, CONCEPT. |
| `doc:offModel` | boolean | Present and true when the model's class for the entity is not in the ontology; the node carries no class. |
| *ontology datatype properties* | literal | Attributes the model stated and the ontology admits. |

## Structure terms used by the chunker

`doco:Document`, `doco:BodyMatter`, `doco:Section`, `doco:SectionTitle`, `doco:Title`, `doco:Paragraph`, `doco:ListItem`, `doco:Caption`, `doco:Table`, `doc:TableCell` (`doc:cellValue`, `doc:rowHeader`, `doc:columnHeader`, `doc:rowIndex`, `doc:columnIndex`), `po:contains`, `nif:isString`, `nif:beginIndex`, `nif:endIndex`, `doc:sectionLevel`, `doc:pageIndex`, `doc:bbox`, `doc:pages`, `doc:evidence`, `doc:provenance`.
