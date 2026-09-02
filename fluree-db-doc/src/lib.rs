//! Document ingestion for Fluree.
//!
//! A folder of documents goes in; what comes out, per document, is ready to
//! transact into a ledger:
//!
//! 1. a DoCO structure graph (sections, paragraphs, table cells, with
//!    character offsets and page/bbox provenance) from the
//!    `fluree-doc-parse` engine, escalating pixel-only regions to a
//!    configured vision model when one is set;
//! 2. retrieval chunks cut along that structure, each citing the elements
//!    it was built from and carrying its section path;
//! 3. an embedding per chunk from a configured OpenAI-compatible endpoint.
//!
//! Every model call is optional and cached. With nothing configured the
//! pipeline is deterministic and makes no network connection.
//!
//! The ledger write, the vector/BM25 index steps and the CLI surface live in
//! `fluree-db-cli`; this crate owns the document side and stays free of the
//! database.

pub mod cache;
pub mod chunk;
pub mod config;
pub mod embed;
pub mod escalate;
pub mod graph;
pub mod ingest;
pub mod parse;
pub mod vocab;

pub use cache::DocCache;
pub use chunk::{Chunk, ChunkConfig};
pub use config::{DocConfig, ModelEndpoint};
pub use embed::EmbeddingClient;
pub use escalate::VlmReader;
pub use ingest::{collect_inputs, prepare, IngestOptions, PreparedDocument, SourceMeta};
pub use parse::{ParsedDocument, SourceKind};

#[derive(Debug, thiserror::Error)]
pub enum DocError {
    #[error("{0}")]
    Io(String),
    #[error("unsupported document type: {0}")]
    Unsupported(String),
    #[error("parse: {0}")]
    Parse(String),
    #[error("configuration: {0}")]
    Config(String),
    #[error("model endpoint: {0}")]
    Model(String),
    /// The document asks the vision model for more crops than the caller
    /// allowed. Raised before any call is made, so nothing has been spent.
    #[error("{crops} crop(s) routed to the vision model, past the cap of {cap}")]
    CropCap { crops: usize, cap: usize },
}

impl From<std::io::Error> for DocError {
    fn from(e: std::io::Error) -> Self {
        DocError::Io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, DocError>;
