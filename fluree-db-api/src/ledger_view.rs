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
use fluree_db_ledger::{HeadTemporal, LedgerState, TypeErasedStore};
use fluree_db_nameservice::NsRecord;
use fluree_db_novelty::Novelty;

use crate::error::{ApiError, Result};
use serde::{Deserialize, Serialize};

/// How a caller identifies a commit.
///
/// Commits have a canonical content-addressed id ([`CommitId`]), but there are
/// several user-facing forms that resolve to the same id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitRef {
    /// Fully resolved CID — no lookup needed.
    ///
    /// Produced by [`CommitRef::parse`] when the input is a valid multibase
    /// CID string (e.g., `"bafybei..."`), or constructed directly by callers
    /// that already hold a [`CommitId`].
    Exact(CommitId),
    /// Hex digest prefix (e.g., "3dd028" — the SHA-256 hex prefix of the commit)
    Prefix(String),
    /// Transaction number (e.g., t=5)
    T(i64),
}

impl CommitRef {
    /// Parse a user-supplied commit reference string.
    ///
    /// - `"t:N"` or a bare integer → [`CommitRef::T`] with transaction number `N`
    /// - `"commit:<prefix>"` or a bare hex digest prefix → [`CommitRef::Prefix`]
    ///   (the `fluree:commit:` / `sha256:` prefixed hex forms are handled by the
    ///   prefix resolver)
    /// - a valid multibase CID (e.g., `"bagaybqabciq..."`) → [`CommitRef::Exact`]
    ///
    /// A CID counts only in its canonical spelling — see
    /// [`ContentId::parse_canonical`] for why. A hex digest taken as a CID here
    /// would become [`CommitRef::Exact`] of a commit nobody named, skipping the
    /// prefix scan entirely. A bare integer is checked first, and cannot collide:
    /// a canonical CID always carries its multibase prefix letter.
    ///
    /// The bare integer and `commit:` spellings exist so that every surface
    /// spelling a *commit* accepts the same strings as one spelling a *point in
    /// time* ([`crate::TimeSpec::parse_at`]) wherever the two overlap. Before
    /// #1805 they were exactly inverted: `branch create --at t:2` worked and
    /// rejected `2`, while `query --at 2` worked and rejected `t:2`.
    ///
    /// **A bare integer is a `t`, not a prefix.** `123456` is simultaneously a
    /// valid `t` and a valid 6-character hex prefix. `commit:123456` forces the
    /// prefix reading; `t:123456` forces the other.
    ///
    /// # Why there is no [`COMMIT_PREFIX_MIN_LEN`] check here
    ///
    /// [`TimeSpec::parse_at`](crate::TimeSpec::parse_at) rejects a too-short
    /// prefix at the boundary and this deliberately does not, which looks like
    /// an inconsistency. It is not, for two reasons.
    ///
    /// The floor is already applied to every [`CommitRef::Prefix`]:
    /// [`LedgerView::resolve_commit`] routes it through
    /// [`normalize_commit_ref`], which enforces the same constant with the same
    /// message. A check here would be a second application on that path, not a
    /// missing one.
    ///
    /// More decisively, it would measure the wrong string. `normalize_commit_ref`
    /// strips `fluree:commit:` / `sha256:` and decodes canonical CIDs *before*
    /// measuring; this function does none of that. `sha256:abc` is ten
    /// characters here and three there, so a parse-time floor would pass a
    /// string the resolver correctly rejects — a check that looks like it
    /// happened and did not. The length rule belongs where the stripping does.
    pub fn parse(s: &str) -> Result<Self> {
        if let Some(t_str) = s.strip_prefix("t:") {
            let t: i64 = t_str
                .parse()
                .map_err(|_| ApiError::query(format!("invalid t value in commit ref '{s}'")))?;
            Ok(CommitRef::T(t))
        } else if let Some(prefix) = s.strip_prefix("commit:") {
            if prefix.is_empty() {
                return Err(ApiError::query(format!("empty commit prefix in '{s}'")));
            }
            Ok(CommitRef::Prefix(prefix.to_string()))
        } else if s.is_empty() {
            Err(ApiError::query("empty commit reference"))
        } else if let Ok(t) = s.parse::<i64>() {
            Ok(CommitRef::T(t))
        } else if let Some(cid) = ContentId::parse_canonical(s) {
            Ok(CommitRef::Exact(cid))
        } else {
            Ok(CommitRef::Prefix(s.to_string()))
        }
    }
}

/// Read-only view of a ledger at a point in time.
///
/// Holds no locks. Safe to clone, pass to subtasks, or keep across `.await`
/// points. Underlying state is Arc-shared, so cloning is cheap.
pub struct LedgerView {
    /// The indexed database snapshot. `Arc`-shared so deriving a view from the
    /// cached `LedgerState` (the per-query hot path) is a refcount bump, not a
    /// deep copy of the namespace maps / stats / schema / graph registry.
    pub snapshot: Arc<LedgerSnapshot>,
    /// In-memory overlay of uncommitted transactions
    pub novelty: Arc<Novelty>,
    /// Dictionary novelty layer (subjects and strings since last index build)
    pub dict_novelty: Arc<fluree_db_core::DictNovelty>,
    /// Shared cache of the current RDFS schema hierarchy (see
    /// `LedgerState::schema_hierarchy_cache`).
    pub schema_hierarchy_cache: Arc<fluree_db_core::SchemaHierarchyCache>,
    /// Cross-transaction compiled-SHACL cache slot (see
    /// `LedgerState::shacl_compile_cache`).
    pub shacl_compile_cache: Arc<parking_lot::RwLock<Option<Arc<dyn std::any::Any + Send + Sync>>>>,
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
    /// Temporal metadata of the head commit, carried so a write staged from
    /// this view keeps `HeadTemporal`'s contract — the event-time
    /// monotonicity guard and sticky dual-stamp decision stay in-memory
    /// integer checks. Dropping it here made every optimistic-path commit
    /// re-read and decode the whole head commit blob (`ensure_head_temporal`)
    /// inside the locked commit window.
    pub head_temporal: Option<HeadTemporal>,
}

impl LedgerView {
    /// Build a view from ledger state.
    ///
    /// Note: `binary_store` is set to `None` here — callers that have a
    /// binary store must set it after construction (see `LedgerHandle::snapshot()`).
    pub(crate) fn from_state(state: &LedgerState) -> Self {
        Self {
            // Arc::clone — refcount bump, not a deep snapshot copy. This is the
            // per-query hot path; the old `state.snapshot.clone()` deep-copied
            // the namespace maps / stats / schema under the state lock.
            snapshot: Arc::clone(&state.snapshot),
            novelty: Arc::clone(&state.novelty),
            dict_novelty: Arc::clone(&state.dict_novelty),
            schema_hierarchy_cache: Arc::clone(&state.schema_hierarchy_cache),
            shacl_compile_cache: Arc::clone(&state.shacl_compile_cache),
            runtime_small_dicts: Arc::clone(&state.runtime_small_dicts),
            t: state.t(),
            head_commit_id: state.head_commit_id.clone(),
            head_index_id: state.head_index_id.clone(),
            ns_record: state.ns_record.clone(),
            binary_store: None,
            head_temporal: state.head_temporal,
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
            schema_hierarchy_cache: self.schema_hierarchy_cache,
            shacl_compile_cache: self.shacl_compile_cache,
            runtime_small_dicts: self.runtime_small_dicts,
            head_commit_id: self.head_commit_id,
            head_index_id: self.head_index_id,
            ns_record: self.ns_record,
            binary_store: self.binary_store.map(|store| TypeErasedStore(store)),
            spatial_indexes: None,
            // Carried from the state this view was taken from (same head
            // commit, so the same temporal metadata). Hardcoding `None` here
            // forced `ensure_head_temporal` — a head-commit-blob read plus
            // decode — on every optimistic-path commit, inside the locked
            // window; the lazy resolve now only fires for views built from
            // states that never observed it (e.g. index == head at load).
            head_temporal: self.head_temporal,
        }
    }
}

/// The leading characters shared by every commit CID in its base32 spelling.
///
/// A CIDv1 opens with multibase, version, codec and multihash bytes — seven
/// bytes, 56 bits, before a single bit of digest. Base32 packs five bits per
/// character, so the first twelve characters of every commit CID that has ever
/// existed are this constant and character thirteen carries the digest's first
/// four bits. `normalize_commit_ref` uses it to tell an abbreviated CID from a
/// hex prefix; `commit_cid_header_is_the_documented_constant` pins it against
/// ids the system actually mints, rather than against a written-down string.
///
/// Every character past `bagayb` is outside the hex alphabet, so no genuine
/// hex prefix of six characters or more can collide with it.
pub(crate) const COMMIT_CID_CONSTANT_HEAD: &str = "bagaybqabciq";

// The shortest commit hex prefix either resolver will scan for. Defined in
// `fluree-db-core` so the address grammar (`parse_time_travel_spec`) can apply
// the same floor without depending on this crate; re-exported here, and from
// the crate root, because this is where callers expect to find it.
pub use fluree_db_core::ledger_id::COMMIT_PREFIX_MIN_LEN;

/// The hex digest a commit resolver scans for, from any spelling a user types.
///
/// Both resolvers — this module's [`resolve_commit_prefix`] and
/// `time_resolve::commit_to_t` — key on the *indexed commit subject*, which is
/// minted as `Sid::new(FLUREE_COMMIT, cid.digest_hex())` in
/// `fluree-db-novelty/src/commit_flakes.rs` and mirrored by the indexer. Hex is
/// therefore the only spelling a bounded prefix scan can bound, which is why it
/// is also the spelling `fluree log` prints.
///
/// Accepted:
/// - `fluree:commit:sha256:<hex>` and `sha256:<hex>` — the `#txn-meta` IRI form
/// - a full CID as [`ContentId`] prints it — decoded to its hex digest, so the
///   id copied out of a JSON API response resolves without translation
/// - a bare hex digest or prefix — passed through
///
/// A CID is recognised only in its canonical spelling, via
/// [`ContentId::parse_canonical`] — a bare hex digest would otherwise decode as
/// base16 and resolve a different commit with no diagnostic.
///
/// This base32-against-hex-keyed-lookup split is not new: `resolve_head` in
/// `fluree-db-binary-index/examples/root_graph.rs` hit the same one against the
/// `.fir6` index nodes (9c4957323), where the documented input could never match
/// and a healthy ledger was reported as 100% orphaned. That one resolves it by
/// trying the hex key first, which works when you hold a whole key; a prefix has
/// nothing to look up, so the guard here is the round trip instead.
pub(crate) fn normalize_commit_ref(input: &str) -> Result<String> {
    let stripped = input.strip_prefix("fluree:commit:").unwrap_or(input);
    let stripped = stripped.strip_prefix("sha256:").unwrap_or(stripped);

    if let Some(cid) = ContentId::parse_canonical(stripped) {
        return Ok(cid.digest_hex());
    }

    if stripped.len() < COMMIT_PREFIX_MIN_LEN {
        return Err(ApiError::query(format!(
            "Commit prefix must be at least {COMMIT_PREFIX_MIN_LEN} characters, got {}",
            stripped.len()
        )));
    }

    // An abbreviated CID carries almost no digest — the first twelve characters
    // are a constant — so it cannot be scanned for. Say that, rather than
    // reporting it as a prefix that matched nothing.
    //
    // Typed the same way as the "No commit found with prefix" case it stands
    // beside. That is not a good type — `ApiError::query` builds an
    // `ApiError::Internal`, so every commit-resolution failure reaches the CLI
    // as "Internal error: Query error: …" and the server as a 500, for what is
    // a user typing the wrong thing. Retyping it is a separate change: the
    // obvious candidate, `NotFound`, is swallowed by `build_source_view`, which
    // rewrites any `is_not_found()` from `db_at` into "ledger not found", so
    // switching would make the `from`-clause surface worse while making the
    // others better.
    if stripped.starts_with(COMMIT_CID_CONSTANT_HEAD)
        || COMMIT_CID_CONSTANT_HEAD.starts_with(stripped)
    {
        return Err(ApiError::query(format!(
            "'{input}' is an abbreviated CID, not a commit id this can resolve: \
             its leading characters are a constant shared by every commit. \
             Pass the hex digest that `fluree log` prints, or a full CID."
        )));
    }

    // SHA-256 in hex is 64 characters
    if stripped.len() > 64 {
        return Err(ApiError::query(format!(
            "Commit prefix too long ({} chars). SHA-256 in hex is 64 characters.",
            stripped.len()
        )));
    }

    Ok(stripped.to_string())
}

/// The error for a prefix that matched more than one commit.
///
/// Full hex digests, deliberately. This is the one message whose entire job is
/// to let someone retype something longer, and every candidate here matched the
/// prefix that was queried — so truncating to a fixed width at or below that
/// prefix's length prints the same string once per candidate and tells the
/// reader nothing. `ledger_view` truncated to seven, which meant any query of
/// seven characters or more produced a list of identical stubs.
///
/// Shared with `time_resolve::commit_to_t` so the two resolvers describe the
/// same situation the same way.
///
/// Both callers stop scanning once a second match appears, so this reports the
/// candidates it saw rather than claiming to enumerate them all.
pub(crate) fn ambiguous_commit_prefix<'a>(
    prefix: &str,
    hex_digests: impl IntoIterator<Item = &'a str>,
) -> ApiError {
    let candidates: Vec<&str> = hex_digests.into_iter().collect();
    ApiError::query(format!(
        "Ambiguous commit prefix '{prefix}': it matches at least {candidates:?}. \
         Retype it with enough characters to pick one out."
    ))
}

/// Resolve a commit hex-digest prefix to a full [`CommitId`].
///
/// Uses a bounded SPOT index scan on commit subjects (same approach as
/// `time_resolve::commit_to_t`, but returns the CID instead of `t`). Both share
/// [`normalize_commit_ref`], so the two surfaces accept the same spellings.
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

    let normalized = normalize_commit_ref(prefix)?;
    let normalized = normalized.as_str();

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
        _ => Err(ambiguous_commit_prefix(
            normalized,
            matches.iter().map(String::as_str),
        )),
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

    // A rebased ledger can have multiple commits at the same `t` (old chain
    // + new chain both leave flakes in the POST index). Collect unique
    // matching subjects so we can detect and surface the ambiguity rather
    // than silently returning the first one.
    let mut seen = std::collections::HashSet::new();
    let mut matches: Vec<String> = Vec::new();
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
        if seen.insert(flake.s.name.as_ref()) {
            matches.push(flake.s.name.to_string());
        }
        if matches.len() > 1 {
            break;
        }
    }

    match matches.len() {
        0 => Err(ApiError::NotFound(format!(
            "No commit found for t={target_t}"
        ))),
        1 => {
            let digest: [u8; 32] = hex::decode(&matches[0])
                .map_err(|e| ApiError::internal(format!("Invalid hex digest: {e}")))?
                .try_into()
                .map_err(|_| ApiError::internal("Digest not 32 bytes"))?;
            Ok(ContentId::from_sha256_digest(
                fluree_db_core::CODEC_FLUREE_COMMIT,
                &digest,
            ))
        }
        _ => {
            let ids: Vec<_> = matches.iter().map(|h| &h[..7.min(h.len())]).collect();
            Err(ApiError::query(format!(
                "Ambiguous t={target_t}: multiple commits match {ids:?} (likely a rebased history). Disambiguate by passing the full commit CID."
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    /// `--at` used to mean two contradictory things: `branch create --at t:2`
    /// worked and rejected `2`, while `query --at 2` worked and rejected `t:2`
    /// (#1805). The two grammars denote different things — a commit versus a
    /// point in time — so they stay separate, but every spelling they *share*
    /// must parse the same way on both. These pin the commit half.
    #[test]
    fn parse_accepts_a_bare_transaction_number() {
        assert_eq!(CommitRef::parse("2").unwrap(), CommitRef::T(2));
        assert_eq!(CommitRef::parse("0").unwrap(), CommitRef::T(0));
        assert_eq!(
            CommitRef::parse("2").unwrap(),
            CommitRef::parse("t:2").unwrap(),
            "bare and tagged spellings of t=2 must agree"
        );
    }

    #[test]
    fn parse_accepts_the_commit_tag() {
        assert_eq!(
            CommitRef::parse("commit:abc123").unwrap(),
            CommitRef::Prefix("abc123".to_string())
        );
        assert_eq!(
            CommitRef::parse("commit:abc123").unwrap(),
            CommitRef::parse("abc123").unwrap(),
            "tagged and bare spellings of a hex prefix must agree"
        );
        assert!(CommitRef::parse("commit:").is_err());
    }

    /// An all-digit string of 6+ characters is both a valid `t` and a valid hex
    /// prefix; `t` wins, matching what `--at` has always done. `commit:` is the
    /// escape hatch, and without it the ambiguity would be unresolvable.
    #[test]
    fn parse_resolves_the_digits_ambiguity_toward_t() {
        assert_eq!(CommitRef::parse("123456").unwrap(), CommitRef::T(123_456));
        assert_eq!(
            CommitRef::parse("commit:123456").unwrap(),
            CommitRef::Prefix("123456".to_string())
        );

        // A bare *negative* integer is a `t` too, where it used to fall through
        // to `Prefix("-5")`. Strictly a better error path — a negative `t`
        // fails cleanly downstream and `-5` was never a valid hex prefix, so
        // nothing that used to resolve stops resolving — but it is a behaviour
        // change in the same class as the ambiguity above, and it needs the
        // test more, not less: on the CLI clap consumes `-5` as a flag before
        // this is reached, so the arm is exercised only through the library and
        // `routes/ledger.rs`, which the integration suite never walks.
        assert_eq!(CommitRef::parse("-5").unwrap(), CommitRef::T(-5));
        assert_eq!(
            CommitRef::parse("-5").unwrap(),
            CommitRef::parse("t:-5").unwrap(),
            "bare and tagged spellings must agree on the sign too"
        );
        assert_eq!(
            CommitRef::parse("commit:-5").unwrap(),
            CommitRef::Prefix("-5".to_string()),
            "`commit:` still forces the prefix reading"
        );
    }

    /// Accepting a bare integer must not shadow the CID arm. It cannot: a
    /// canonical CID always carries its multibase prefix letter, so it never
    /// parses as `i64`.
    #[test]
    fn bare_integer_arm_does_not_shadow_a_canonical_cid() {
        let cid = ContentId::new(ContentKind::Commit, b"digits-arm-probe");
        let s = cid.to_string();
        assert!(
            s.parse::<i64>().is_err(),
            "a CID must not parse as an integer: {s}"
        );
        assert!(matches!(CommitRef::parse(&s).unwrap(), CommitRef::Exact(_)));
    }

    /// `parse` must accept the multibase CID string produced by
    /// `ContentId::Display` and return [`CommitRef::Exact`]. If it falls
    /// through to `Prefix` instead, the prefix resolver (which scans by hex
    /// digest) silently fails to match a base32-multibase string.
    #[test]
    fn parse_full_multibase_cid_produces_exact() {
        let cid = ContentId::new(ContentKind::Commit, b"regression-probe");
        let parsed = CommitRef::parse(&cid.to_string()).expect("parse should succeed");
        assert!(
            matches!(&parsed, CommitRef::Exact(c) if c == &cid),
            "expected Exact({cid}), got a different variant"
        );
    }

    /// `parse` must not promote a hex digest to `Exact` either.
    ///
    /// Same hazard as [`normalize_commit_ref`]: these strings are all hex and
    /// all parse as CIDs. Taken as `Exact`, they would be resolved to a commit
    /// that was never asked for instead of scanned for as a prefix.
    #[test]
    fn parse_does_not_promote_a_hex_digest_to_exact() {
        for hex in ["f01550003000102", "f015500080001020304050607"] {
            assert!(
                hex.parse::<ContentId>().is_ok(),
                "fixture must parse: {hex}"
            );
            assert_eq!(
                CommitRef::parse(hex).expect("parse should succeed"),
                CommitRef::Prefix(hex.to_string()),
                "a hex digest must stay a prefix: {hex}"
            );
        }
    }

    /// Every spelling a user can hold in hand reduces to the same hex digest —
    /// which is the only thing the two prefix scans are keyed on.
    #[test]
    fn normalize_accepts_every_spelling_of_one_commit() {
        let cid = ContentId::new(ContentKind::Commit, b"one commit, many spellings");
        let hex = cid.digest_hex();

        for spelling in [
            cid.to_string(),
            hex.clone(),
            format!("sha256:{hex}"),
            format!("fluree:commit:sha256:{hex}"),
            hex[..12].to_string(),
        ] {
            let normalized = normalize_commit_ref(&spelling).expect("should normalize");
            assert!(
                hex.starts_with(&normalized),
                "{spelling} normalized to {normalized}, not a prefix of {hex}"
            );
        }
    }

    /// A hex digest is never re-read as multibase.
    ///
    /// `f` is base16 in the multibase table, so an all-hex string of the right
    /// parity decodes, and if the bytes happen to form a valid CIDv1 it parses.
    /// These are not hypothetical: each one below is accepted by
    /// `ContentId::from_str` and re-displays as the base32 string in the
    /// comment, so without the round-trip check `normalize_commit_ref` would
    /// hand the scan a digest the user never typed.
    ///
    /// They are reachable because the identity multihash makes a valid CID out
    /// of very few bytes — `01` version, `55` raw codec, `00` identity hash,
    /// then a length and that many bytes.
    #[test]
    fn a_hex_digest_is_not_decoded_as_multibase() {
        for hex in [
            "f01550003000102",                                             // bafkqaayaaeba
            "f0155000400010203",                                           // bafkqabaaaebag
            "f015500080001020304050607",                                   // bafkqacaaaebagbafaydq
            "f01cc6bdf780663567b48b39a3b52cd14d15296e0268a9c3701bc5ce138", // bahggxx3yazrvm62iwona
        ] {
            assert!(
                hex.chars().all(|c| c.is_ascii_hexdigit()),
                "fixture must be a plausible hex digest: {hex}"
            );
            assert!(
                hex.parse::<ContentId>().is_ok(),
                "fixture is only interesting if it DOES parse as a CID: {hex}"
            );
            assert_eq!(
                normalize_commit_ref(hex).expect("hex should normalize"),
                hex,
                "a hex digest must pass through unchanged, not be decoded"
            );
        }
    }

    /// An abbreviated CID gets told what it is, not "no commit found".
    #[test]
    fn an_abbreviated_cid_is_named_as_such() {
        let cid = ContentId::new(ContentKind::Commit, b"abbreviate me");
        let full = cid.to_string();

        // Every width from the resolver's six-character floor up to and past
        // the constant header. None of these can be scanned for.
        for len in [6usize, 7, 11, 12, 13, 20] {
            let err = normalize_commit_ref(&full[..len])
                .expect_err("an abbreviated CID cannot be resolved");
            let msg = err.to_string();
            assert!(
                msg.contains("abbreviated CID"),
                "at {len} characters the diagnostic should name the cause, got: {msg}"
            );
        }

        // ...but the whole thing resolves.
        assert_eq!(
            normalize_commit_ref(&full).expect("a full CID resolves"),
            cid.digest_hex()
        );
    }

    /// An ambiguity message has to distinguish the things it is ambiguous
    /// between.
    ///
    /// Every candidate matched the queried prefix, so any fixed-width
    /// truncation at or below that prefix's length renders them identically.
    /// The old form truncated to seven, which made a query of seven or more
    /// characters print the same stub once per candidate — exactly the case the
    /// message exists to resolve. Asserting "the rendered candidates differ" is
    /// what that form cannot satisfy.
    #[test]
    fn an_ambiguity_message_distinguishes_its_candidates() {
        // Two digests sharing a 12-character head, as a real collision would.
        let shared = "0a9ccca1e1bc";
        let a = format!("{shared}aa{}", "0".repeat(50));
        let b = format!("{shared}bb{}", "0".repeat(50));

        let msg = ambiguous_commit_prefix(shared, [a.as_str(), b.as_str()]).to_string();

        assert!(
            msg.contains(&a) && msg.contains(&b),
            "both candidates must appear in full: {msg}"
        );
        // The property the old rendering could not have: what is printed for
        // one candidate is not what is printed for the other.
        let rendered: Vec<&str> = msg.match_indices(shared).map(|(i, _)| &msg[i..]).collect();
        assert!(
            rendered.len() >= 3,
            "expected the prefix plus both candidates: {msg}"
        );
        assert_ne!(a, b);
        assert!(
            !msg.contains(&format!("{:?}", [&shared[..7], &shared[..7]])),
            "must not degenerate into a list of identical stubs: {msg}"
        );
    }

    /// The constant the diagnostic keys on, pinned against a minted id.
    #[test]
    fn commit_cid_header_is_the_documented_constant() {
        for payload in [&b"a"[..], b"b", b"c"] {
            let cid = ContentId::new(ContentKind::Commit, payload).to_string();
            assert!(
                cid.starts_with(COMMIT_CID_CONSTANT_HEAD),
                "{cid} should open with {COMMIT_CID_CONSTANT_HEAD}"
            );
        }
        assert!(
            !COMMIT_CID_CONSTANT_HEAD[1..]
                .chars()
                .all(|c| c.is_ascii_hexdigit()),
            "the header must contain non-hex characters, or a real hex prefix \
             could be mistaken for an abbreviated CID"
        );
    }
}
