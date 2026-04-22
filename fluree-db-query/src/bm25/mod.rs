//! BM25 Full-Text Search Module
//!
//! This module implements BM25 full-text search.
//! Fluree's graph source system.
//!
//! # Components
//!
//! - [`index`]: Core BM25 index data structures (`Bm25Index`, `DocKey`, `PostingList`)
//! - [`scoring`]: BM25 scoring algorithm (`Bm25Scorer`, IDF calculation)
//! - [`analyzer`]: Text analysis pipeline (`Analyzer`, tokenizers, filters)
//! - [`text`]: Text extraction from JSON-LD values (`extract_text`)
//! - [`builder`]: Index building from query results (`Bm25IndexBuilder`)
//!
//! Build an index with `Bm25IndexBuilder`, then query it using `Bm25Scorer` with analyzed search terms.

pub mod analyzer;
pub mod builder;
pub mod index;
pub mod manifest;
pub mod operator;
pub mod scoring;
pub mod serialize;
pub mod text;

// Re-export commonly used types
pub use analyzer::{
    Analyzer, DefaultEnglishTokenizer, SnowballStemmerFilter, StopwordFilter, Token,
};
pub use builder::{
    Bm25IndexBuilder, BuilderError, IncrementalUpdateResult, IncrementalUpdater,
    MultiBm25IndexBuilder,
};
pub use index::{
    Bm25Config, Bm25Index, Bm25Stats, CompiledPropertyDeps, DocKey, DocMeta, GraphSourceWatermark,
    Posting, PostingList, PropertyDeps, TermEntry,
};
pub use manifest::{Bm25Manifest, Bm25SnapshotEntry};
pub use operator::{
    Bm25IndexProvider, Bm25SearchOperator, Bm25SearchProvider, Bm25SearchResult, SearchHit,
};
pub use scoring::{compute_idf, compute_term_score, Bm25Scorer};
pub use serialize::{
    assemble_from_chunked_root, deserialize, deserialize_chunked_root, deserialize_posting_leaflet,
    finalize_chunked_root, is_chunked_format, prepare_chunked, read_snapshot, serialize,
    write_snapshot, ChunkedPrepResult, ChunkedRoot, LeafletInfo, LeafletRef, SerializeError,
};
pub use text::{extract_and_analyze, extract_text};
