//! The derived-only set D of a generation, and the synthetic commit that
//! carries a fold's facts into the indexer.
//!
//! Both are commit-codec blobs: the codec is Sid-direct, so the facts stay in
//! namespace-code space, independent of any dictionary. D is stored behind
//! [`DERIVED_FACTS_MAGIC`]; the fold blob goes to the indexer bare.

use fluree_db_core::commit::codec::{read_commit, write_commit};
use fluree_db_core::reasoned_head::DERIVED_FACTS_MAGIC;
use fluree_db_core::{Commit, Flake};

use crate::{ApiError, Result};

/// Encode facts at `t` as a commit-codec blob.
pub(crate) fn encode_facts(t: i64, flakes: Vec<Flake>) -> Result<Vec<u8>> {
    let commit = Commit::new(t, flakes);
    write_commit(&commit, true, None)
        .map(|written| written.bytes)
        .map_err(|e| ApiError::internal(format!("encode reasoned facts: {e}")))
}

/// Encode a derived-only set: [`DERIVED_FACTS_MAGIC`] then the facts.
pub(crate) fn encode_derived_only(t: i64, flakes: Vec<Flake>) -> Result<Vec<u8>> {
    let mut out = DERIVED_FACTS_MAGIC.to_vec();
    out.extend(encode_facts(t, flakes)?);
    Ok(out)
}

/// Decode a derived-only set written by [`encode_derived_only`].
pub(crate) fn decode_derived_only(bytes: &[u8]) -> Result<Vec<Flake>> {
    let body = bytes
        .strip_prefix(DERIVED_FACTS_MAGIC.as_slice())
        .ok_or_else(|| ApiError::internal("not a derived-only fact set (bad magic)"))?;
    read_commit(body)
        .map(|commit| commit.flakes)
        .map_err(|e| ApiError::internal(format!("decode derived-only set: {e}")))
}
