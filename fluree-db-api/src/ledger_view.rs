//! Read-only, lock-free view of a ledger at a point in time.
//!
//! A [`LedgerView`] bundles the indexed snapshot, novelty overlay, dictionary
//! state, and head metadata needed to query or resolve against a ledger without
//! taking the write lock. It's produced by [`LedgerHandle::snapshot`] and is
//! safe to hold across `.await` points or pass to subtasks.
//!
//! This module also defines [`CommitRef`] — the user-facing forms for
//! identifying a commit — and owns the resolvers that turn one into a
//! canonical [`CommitId`] via [`LedgerView::resolve_commit`].
//!
//! [`LedgerHandle::snapshot`]: crate::LedgerHandle::snapshot

use std::sync::Arc;

use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::db::LedgerSnapshot;
use fluree_db_core::{CommitId, ContentId};
use fluree_db_ledger::{LedgerState, TypeErasedStore};
use fluree_db_nameservice::NsRecord;
use fluree_db_novelty::Novelty;

use crate::error::{ApiError, Result};

/// How a caller identifies a commit.
///
/// Commits have a canonical content-addressed id ([`CommitId`]), but there are
/// several user-facing forms that resolve to the same id.
pub enum CommitRef {
    /// Exact CID (e.g., from API or full CID string like "bagaybqabciq...")
    Exact(CommitId),
    /// Hex digest prefix (e.g., "3dd028" — the SHA-256 hex prefix of the commit)
    Prefix(String),
    /// Transaction number (e.g., t=5)
    T(i64),
}

/// Read-only view of a ledger at a point in time.
///
/// Holds no locks. Safe to clone, pass to subtasks, or keep across `.await`
/// points. Underlying state is Arc-shared, so cloning is cheap.
pub struct LedgerView {
    /// The indexed database snapshot (cheap clone - Arc fields)
    pub snapshot: LedgerSnapshot,
    /// In-memory overlay of uncommitted transactions
    pub novelty: Arc<Novelty>,
    /// Dictionary novelty layer (subjects and strings since last index build)
    pub dict_novelty: Arc<fluree_db_core::DictNovelty>,
    /// Ledger-scoped runtime IDs for predicates and datatypes.
    pub runtime_small_dicts: Arc<fluree_db_core::RuntimeSmallDicts>,
    /// Current transaction t value
    pub t: i64,
    /// Content identifier of the head commit (identity)
    pub head_commit_id: Option<ContentId>,
    /// Content identifier of the current index root (identity)
    pub head_index_id: Option<ContentId>,
    /// Nameservice record (if loaded via nameservice)
    pub ns_record: Option<NsRecord>,
    /// Binary columnar index store (v2 only).
    ///
    /// Present when `snapshot.range_provider` is also set — the two are always
    /// set/cleared together (see coherence `debug_assert` in `snapshot()`).
    pub binary_store: Option<Arc<BinaryIndexStore>>,
    /// Default JSON-LD @context for this ledger.
    pub default_context: Option<serde_json::Value>,
}

impl LedgerView {
    /// Build a view from ledger state.
    ///
    /// Note: `binary_store` is set to `None` here — callers that have a
    /// binary store must set it after construction (see `LedgerHandle::snapshot()`).
    pub(crate) fn from_state(state: &LedgerState) -> Self {
        Self {
            snapshot: state.snapshot.clone(),
            novelty: Arc::clone(&state.novelty),
            dict_novelty: Arc::clone(&state.dict_novelty),
            runtime_small_dicts: Arc::clone(&state.runtime_small_dicts),
            t: state.t(),
            head_commit_id: state.head_commit_id.clone(),
            head_index_id: state.head_index_id.clone(),
            ns_record: state.ns_record.clone(),
            binary_store: None,
            default_context: state.default_context.clone(),
        }
    }

    /// Get the ledger name (without branch suffix)
    ///
    /// Returns the base ledger name (e.g., "mydb"), NOT the canonical form (e.g., "mydb:main").
    /// For the canonical ledger_id, use `ledger_id()` instead.
    ///
    /// Note: This matches `NsRecord.name` semantics where "name" is the base name.
    pub fn name(&self) -> Option<&str> {
        self.ns_record.as_ref().map(|r| r.name.as_str())
    }

    /// Get the canonical ledger ID (with branch suffix)
    ///
    /// Returns the canonical form (e.g., "mydb:main") suitable for cache keys.
    /// This is the primary identifier for ledger lookups.
    pub fn ledger_id(&self) -> Option<&str> {
        self.ns_record.as_ref().map(|r| r.ledger_id.as_str())
    }

    /// Get index_t from the underlying LedgerSnapshot
    pub fn index_t(&self) -> i64 {
        self.snapshot.t
    }

    /// Resolve a [`CommitRef`] to a canonical [`CommitId`] against this
    /// view's indexes and novelty overlay.
    pub async fn resolve_commit(&self, commit_ref: CommitRef) -> Result<CommitId> {
        match commit_ref {
            CommitRef::Exact(id) => Ok(id),
            CommitRef::Prefix(prefix) => {
                resolve_commit_prefix(&self.snapshot, &self.novelty, &prefix, self.t).await
            }
            CommitRef::T(t) => {
                resolve_t_to_commit_id(&self.snapshot, &self.novelty, t, self.t).await
            }
        }
    }

    /// Convert the view to a [`LedgerState`] for backward compatibility.
    ///
    /// This creates a `LedgerState` with the same data as the view. Use this
    /// when you need to pass the state to APIs that expect `LedgerState`.
    pub fn to_ledger_state(self) -> LedgerState {
        let dict_novelty = self.dict_novelty;
        LedgerState {
            snapshot: self.snapshot,
            novelty: self.novelty,
            dict_novelty,
            runtime_small_dicts: self.runtime_small_dicts,
            head_commit_id: self.head_commit_id,
            head_index_id: self.head_index_id,
            ns_record: self.ns_record,
            binary_store: self.binary_store.map(|store| TypeErasedStore(store)),
            default_context: self.default_context,
            spatial_indexes: None,
        }
    }
}

/// Resolve a commit hex-digest prefix to a full [`CommitId`].
///
/// Uses a bounded SPOT index scan on commit subjects (same approach as
/// `time_resolve::commit_to_t`, but returns the CID instead of `t`).
async fn resolve_commit_prefix(
    snapshot: &LedgerSnapshot,
    overlay: &Novelty,
    prefix: &str,
    current_t: i64,
) -> Result<CommitId> {
    use fluree_db_core::{
        range_bounded_with_overlay, Flake, IndexType, RangeOptions, Sid, TXN_META_GRAPH_ID,
    };
    use fluree_vocab::namespaces::FLUREE_COMMIT;

    // Normalize: strip standard prefixes
    let normalized = prefix.strip_prefix("fluree:commit:").unwrap_or(prefix);
    let normalized = normalized.strip_prefix("sha256:").unwrap_or(normalized);

    if normalized.len() < 6 {
        return Err(ApiError::query(format!(
            "Commit prefix must be at least 6 characters, got {}",
            normalized.len()
        )));
    }

    // SHA-256 in hex is 64 characters
    if normalized.len() > 64 {
        return Err(ApiError::query(format!(
            "Commit prefix too long ({} chars). SHA-256 in hex is 64 characters.",
            normalized.len()
        )));
    }

    // Build scan range: [prefix, prefix~) where ~ sorts after all hex chars
    let start_sid = Sid::new(FLUREE_COMMIT, normalized);
    let end_prefix = format!("{normalized}~");
    let end_sid = Sid::new(FLUREE_COMMIT, &end_prefix);

    let start_bound = Flake::min_for_subject(start_sid);
    let end_bound = Flake::min_for_subject(end_sid);

    let opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(32);

    let flakes = range_bounded_with_overlay(
        snapshot,
        TXN_META_GRAPH_ID,
        overlay,
        IndexType::Spot,
        start_bound,
        end_bound,
        opts,
    )
    .await?;

    // Collect unique matching commit subjects
    let mut seen = std::collections::HashSet::new();
    let mut matches: Vec<String> = Vec::new();

    for flake in &flakes {
        if flake.s.namespace_code != FLUREE_COMMIT {
            continue;
        }
        if !flake.s.name.starts_with(normalized) {
            continue;
        }
        if seen.insert(flake.s.name.as_ref()) {
            matches.push(flake.s.name.to_string());
        }
        if matches.len() > 1 {
            break;
        }
    }

    match matches.len() {
        0 => Err(ApiError::NotFound(format!(
            "No commit found with prefix: {normalized}"
        ))),
        1 => {
            let hex = &matches[0];
            let digest: [u8; 32] = hex::decode(hex)
                .map_err(|e| ApiError::internal(format!("Invalid hex digest: {e}")))?
                .try_into()
                .map_err(|_| ApiError::internal("Digest not 32 bytes"))?;
            Ok(ContentId::from_sha256_digest(
                fluree_db_core::CODEC_FLUREE_COMMIT,
                &digest,
            ))
        }
        _ => {
            let ids: Vec<_> = matches
                .iter()
                .take(5)
                .map(|h| &h[..7.min(h.len())])
                .collect();
            Err(ApiError::query(format!(
                "Ambiguous commit prefix '{}': matches {:?}{}",
                normalized,
                ids,
                if matches.len() > 5 { " ..." } else { "" }
            )))
        }
    }
}

/// Resolve a transaction number (`t`) to a full [`CommitId`].
///
/// Queries the POST index for commit flakes where predicate = `fluree:db/t`
/// and object = the target `t` value. The matching commit subject's hex digest
/// is then converted to a [`CommitId`].
async fn resolve_t_to_commit_id(
    snapshot: &LedgerSnapshot,
    overlay: &Novelty,
    target_t: i64,
    current_t: i64,
) -> Result<CommitId> {
    use fluree_db_core::{
        range_with_overlay, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid,
        TXN_META_GRAPH_ID,
    };
    use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB};

    if target_t < 1 {
        return Err(ApiError::query(format!(
            "Transaction number must be >= 1, got {target_t}"
        )));
    }
    if target_t > current_t {
        return Err(ApiError::NotFound(format!(
            "Transaction t={target_t} not found (latest is t={current_t})"
        )));
    }

    let predicate = Sid::new(FLUREE_DB, fluree_vocab::db::T);
    let range_match = RangeMatch::predicate_object(predicate, FlakeValue::Long(target_t));

    let opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(16);

    let flakes = range_with_overlay(
        snapshot,
        TXN_META_GRAPH_ID,
        overlay,
        IndexType::Post,
        RangeTest::Eq,
        range_match,
        opts,
    )
    .await?;

    for flake in &flakes {
        if flake.p.namespace_code != FLUREE_DB || flake.p.name.as_ref() != fluree_vocab::db::T {
            continue;
        }
        if flake.o != FlakeValue::Long(target_t) {
            continue;
        }
        if flake.s.namespace_code != FLUREE_COMMIT {
            continue;
        }
        let hex = flake.s.name.as_ref();
        let digest: [u8; 32] = hex::decode(hex)
            .map_err(|e| ApiError::internal(format!("Invalid hex digest: {e}")))?
            .try_into()
            .map_err(|_| ApiError::internal("Digest not 32 bytes"))?;
        return Ok(ContentId::from_sha256_digest(
            fluree_db_core::CODEC_FLUREE_COMMIT,
            &digest,
        ));
    }

    Err(ApiError::NotFound(format!(
        "No commit found for t={target_t}"
    )))
}
