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
pub const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema#";

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
        "rdfs": RDFS_NS,
        "xsd": XSD_NS,
        "po:contains": { "@type": "@id" },
        "doc:sourceElement": { "@type": "@id" },
        "doc:sourceDocument": { "@type": "@id" }
    })
}
