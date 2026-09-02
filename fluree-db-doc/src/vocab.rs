//! The vocabulary the chunk graph is written in.
//!
//! Structure elements come from the engine already typed with DoCO / NIF
//! terms under `https://ns.flur.ee/doc#` (`doc:`). Chunks extend that same
//! namespace so a ledger built here and one built by Fluree AI's hosted
//! pipeline describe documents with one vocabulary.

pub const DOC_NS: &str = "https://ns.flur.ee/doc#";
pub const DOCO_NS: &str = "http://purl.org/spar/doco/";
pub const NIF_NS: &str = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#";
pub const PO_NS: &str = "http://www.essepuntato.it/2008/12/pattern#";
pub const FLUREE_NS: &str = "https://ns.flur.ee/db#";
pub const RDFS_NS: &str = "http://www.w3.org/2000/01/rdf-schema#";
pub const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema#";
pub const SKOS_NS: &str = "http://www.w3.org/2004/02/skos/core#";
pub const SCHEMA_NS: &str = "https://schema.org/";

/// A retrieval unit cut from a document's structure.
pub const CHUNK: &str = "doc:Chunk";
/// The file a graph was extracted from: the `doc:sourceDocument` target.
pub const SOURCE_DOCUMENT_CLASS: &str = "doc:SourceDocument";

pub const CHUNK_INDEX: &str = "doc:chunkIndex";
pub const TEXT: &str = "doc:text";
pub const HEADER_PATH: &str = "doc:headerPath";
pub const SOURCE_ELEMENT: &str = "doc:sourceElement";
pub const SOURCE_DOCUMENT: &str = "doc:sourceDocument";
pub const EMBEDDING: &str = "doc:embedding";
pub const EMBEDDING_MODEL: &str = "doc:embeddingModel";
pub const EMBEDDING_DIMENSIONS: &str = "doc:embeddingDimensions";
pub const CHUNK_COUNT: &str = "doc:chunkCount";
pub const FILE_NAME: &str = "doc:fileName";
pub const RELATIVE_PATH: &str = "doc:relativePath";
pub const SHA256: &str = "doc:sha256";
pub const MEDIA_TYPE: &str = "doc:mediaType";
pub const BYTE_SIZE: &str = "doc:byteSize";
pub const PAGE_COUNT: &str = "doc:pageCount";
pub const ESCALATED_CROPS: &str = "doc:escalatedCrops";
pub const PARSER_REVISION: &str = "doc:parserRevision";
pub const INGESTED_AT: &str = "doc:ingestedAt";

/// An entity this pipeline minted because no `--entities` source knew it.
/// Entities that were known keep their source IRI and get no node here.
pub const ENTITY: &str = "doc:Entity";
/// One span of text naming an entity: a NIF string anchored in a chunk.
pub const MENTION: &str = "doc:Mention";
/// One relation the language model reported, reified with its evidence
/// and the gate's verdict, whether or not it became an edge.
pub const RELATION: &str = "doc:Relation";

pub const NER_LABEL: &str = "doc:nerLabel";
/// True on a minted entity whose class the ontology does not have. Kept
/// so a reviewer can see it; filter on it to leave it out.
pub const OFF_MODEL: &str = "doc:offModel";
/// `gazetteer` or `llm`: what found a mention or relation.
pub const EXTRACTED_BY: &str = "doc:extractedBy";
/// `valid`, `repaired` or `rejected`: the gate's verdict on a predicate.
pub const VERDICT: &str = "doc:verdict";
/// True when a direct edge was written for the relation, so a re-ingest
/// knows which edges to reconsider.
pub const ASSERTED: &str = "doc:asserted";
pub const EXCERPT: &str = "doc:excerpt";
pub const ORIGINAL_PREDICATE: &str = "doc:originalPredicate";
pub const REPAIR_NOTE: &str = "doc:repairNote";
pub const REJECTION_REASON: &str = "doc:rejectionReason";
pub const SOURCE_CHUNK: &str = "doc:sourceChunk";
pub const EXTRACTION_MODEL: &str = "doc:extractionModel";
/// Hash of everything that shaped extraction, so an unchanged document is
/// re-extracted only when the ontology, gazetteer or model changed.
pub const EXTRACTION_FINGERPRINT: &str = "doc:extractionFingerprint";
pub const MENTION_COUNT: &str = "doc:mentionCount";
pub const ENTITY_COUNT: &str = "doc:entityCount";
pub const RELATION_COUNT: &str = "doc:relationCount";

/// Full IRI of the embedding property, for index configuration that must not
/// depend on a prefix map.
pub fn embedding_iri() -> String {
    format!("{DOC_NS}embedding")
}

pub fn text_iri() -> String {
    format!("{DOC_NS}text")
}

pub fn header_path_iri() -> String {
    format!("{DOC_NS}headerPath")
}

pub fn source_document_iri() -> String {
    format!("{DOC_NS}sourceDocument")
}

/// The `@context` every emitted graph carries. A superset of the engine's
/// DoCO context so the structure and chunk graphs can share one transaction.
pub fn context() -> serde_json::Value {
    serde_json::json!({
        "doc": DOC_NS,
        "doco": DOCO_NS,
        "nif": NIF_NS,
        "po": PO_NS,
        "f": FLUREE_NS,
        "rdf": RDF_NS,
        "rdfs": RDFS_NS,
        "xsd": XSD_NS,
        "skos": SKOS_NS,
        "schema": SCHEMA_NS,
        "po:contains": { "@type": "@id" },
        "doc:sourceElement": { "@type": "@id" },
        "doc:sourceDocument": { "@type": "@id" },
        "doc:sourceChunk": { "@type": "@id" },
        "nif:entity": { "@type": "@id" },
        "nif:referenceContext": { "@type": "@id" },
        "rdf:subject": { "@type": "@id" },
        "rdf:predicate": { "@type": "@id" }
    })
}
