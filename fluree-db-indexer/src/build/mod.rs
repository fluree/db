//! Index build pipeline modules.
//!
//! The actual pipeline logic lives in focused sub-modules:
//! - [`rebuild`]: Full index rebuild from genesis (Phase A..F)
//! - [`incremental`]: Incremental index update from existing root (Phase 1..5)
//! - [`root_assembly`]: Common root finalization (encode, CAS write, garbage chain)
//! - [`commit_chain`]: Commit chain walking helpers
//! - [`upload`]: CAS upload primitives + index artifact upload
//! - [`upload_dicts`]: Dictionary flat-file upload (forward packs + reverse trees)
//! - [`dicts`]: Incremental reverse tree update helpers
//! - [`spatial`]: Spatial index building (S2 complex geometries)
//! - [`types`]: Shared types (`UploadedIndexes`, `UploadedDicts`, etc.)

pub(crate) mod commit_chain;
pub(crate) mod dicts;
pub(crate) mod fulltext;
pub(crate) mod incremental;
pub(crate) mod rebuild;
pub(crate) mod root_assembly;
pub(crate) mod spatial;
pub(crate) mod types;
pub(crate) mod upload;
pub(crate) mod upload_dicts;
