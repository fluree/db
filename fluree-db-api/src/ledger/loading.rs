use std::sync::Arc;

use crate::ledger_view::CommitRef;
use crate::{ApiError, Fluree, HistoricalLedgerView, LedgerState, Result, TypeErasedStore};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ContentStore;
use fluree_db_core::DictNovelty;
use fluree_db_core::{collect_dag_cids, load_commit_by_id, CommitId};
use fluree_db_nameservice::{NameServiceError, NsRecord};

impl Fluree {
    /// Load a ledger by address (e.g., "mydb:main")
    ///
    /// This loads the ledger state using the connection-wide cache.
    /// The ledger state combines the indexed database with any uncommitted novelty transactions.
    pub async fn ledger(&self, ledger_id: &str) -> Result<LedgerState> {
        let mut state =
            LedgerState::load(&self.nameservice_mode, ledger_id, self.backend()).await?;

        // If nameservice has an index address, require that the binary index root is
        // readable and loadable. This ensures `fluree.ledger()` always returns a
        // queryable, indexed LedgerSnapshot after (re)indexing.
        //
        // Note: we may already have a `LedgerSnapshot.range_provider` (e.g. attached after `load_ledger_snapshot`),
        // but we still want `binary_store` so query execution can use `BinaryScanOperator`.
        if let Some(index_cid) = state
            .ns_record
            .as_ref()
            .and_then(|r| r.index_head_id.as_ref())
            .cloned()
        {
            if state.snapshot.range_provider.is_none() || state.binary_store.is_none() {
                // Use the NsRecord's ledger_id (canonical namespace) rather than
                // snapshot.ledger_id, which may reflect the source ledger after
                // a pack import/clone into a differently-named destination.
                let ns_ledger_id = state
                    .ns_record
                    .as_ref()
                    .map(|r| r.ledger_id.as_str())
                    .unwrap_or(state.snapshot.ledger_id.as_str());
                let cs = self.content_store(ns_ledger_id);
                let bytes = cs.get(&index_cid).await.map_err(|e| {
                    ApiError::internal(format!(
                        "failed to read binary index root for {index_cid}: {e}"
                    ))
                })?;

                let cache_dir = std::env::temp_dir().join("fluree-cache");

                // Load BinaryIndexStore from FIR6 root.
                let mut binary_index_store = BinaryIndexStore::load_from_root_bytes(
                    Arc::clone(&cs),
                    &bytes,
                    &cache_dir,
                    Some(Arc::clone(&self.leaflet_cache)),
                )
                .await
                .map_err(|e| {
                    ApiError::internal(format!(
                        "failed to load binary index store for {index_cid}: {e}"
                    ))
                })?;

                crate::ns_helpers::sync_store_and_snapshot_ns(
                    &mut binary_index_store,
                    &mut state.snapshot,
                )?;

                // Extract stats from the FIR6 root if present.
                // Decode FIR6 root metadata once and apply:
                // - watermarks (needed for DictNovelty/DictOverlay correctness)
                // - optional stats + schema (for formatting/reasoning)
                let root = fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes)
                    .map_err(|e| ApiError::internal(format!("failed to decode FIR6 root: {e}")))?;

                // Watermarks + dict novelty.
                state.snapshot.subject_watermarks = root.subject_watermarks.clone();
                state.snapshot.string_watermark = root.string_watermark;
                state.dict_novelty = Arc::new(DictNovelty::with_watermarks(
                    state.snapshot.subject_watermarks.clone(),
                    state.snapshot.string_watermark,
                ));
                // Re-populate DictNovelty with any already-loaded novelty flakes so
                // overlay translation (BinaryRangeProvider) can resolve newly-introduced IDs.
                //
                // Important: only allocate novelty IDs for entries *not* present in the
                // persisted dictionaries (canonical IDs must win).
                if !state.novelty.is_empty() {
                    let novelty = state.novelty.as_ref();
                    let dn = Arc::make_mut(&mut state.dict_novelty);
                    fluree_db_binary_index::dict_novelty_safe::populate_dict_novelty_safe(
                        dn,
                        Some(&binary_index_store),
                        novelty
                            .iter_index(fluree_db_core::IndexType::Post)
                            .map(|id| novelty.get_flake(id)),
                    )
                    .map_err(|e| ApiError::internal(format!("populate_dict_novelty_safe: {e}")))?;
                }

                // Stats + schema.
                if root.stats.is_some() && state.snapshot.stats.is_none() {
                    state.snapshot.stats = root.stats;
                    tracing::debug!("loaded stats from FIR6 root");
                }
                if root.schema.is_some() && state.snapshot.schema.is_none() {
                    state.snapshot.schema = root.schema;
                    tracing::debug!("loaded schema from FIR6 root");
                }

                let arc_store = Arc::new(binary_index_store);
                state.binary_store = Some(TypeErasedStore(arc_store.clone()));

                // Reseed runtime small dicts from the binary index store BEFORE
                // creating the BinaryRangeProvider. The dicts built during
                // `load_with_store` are unseeded (IDs start at 0) and would
                // collide with persisted predicate/datatype IDs in the binary
                // index. Reseeding first ensures novelty-only entries get IDs
                // above the persisted range.
                crate::runtime_dicts::reseed_runtime_small_dicts(&mut state, &arc_store);

                // Attach range provider for policy/SHACL/reasoner/property paths.
                if state.snapshot.range_provider.is_none() {
                    let ns_fallback = Some(Arc::new(state.snapshot.namespaces().clone()));
                    let provider = fluree_db_query::BinaryRangeProvider::new(
                        Arc::clone(&arc_store),
                        state.dict_novelty.clone(),
                        state.runtime_small_dicts.clone(),
                        ns_fallback,
                    );
                    state.snapshot.range_provider = Some(Arc::new(provider));
                }
                tracing::info!("loaded binary index store");
            }
        }

        // Load default context from CAS if the nameservice record has one.
        if let Some(ctx_id) = state
            .ns_record
            .as_ref()
            .and_then(|r| r.default_context.as_ref())
        {
            let ns_ledger_id = state
                .ns_record
                .as_ref()
                .map(|r| r.ledger_id.as_str())
                .unwrap_or(state.snapshot.ledger_id.as_str());
            let cs = self.content_store(ns_ledger_id);
            match cs.get(ctx_id).await {
                Ok(bytes) => match serde_json::from_slice(&bytes) {
                    Ok(ctx) => state.default_context = Some(ctx),
                    Err(e) => tracing::warn!(%e, "failed to parse default context JSON"),
                },
                Err(e) => tracing::debug!(%e, cid = %&ctx_id, "could not load default context"),
            }
        }

        Ok(state)
    }

    /// Load a historical view of a ledger at a specific time
    ///
    /// This provides time-travel capability by loading the ledger state
    /// as it existed at `target_t`. The view is read-only and time-bounded.
    pub async fn ledger_view_at(
        &self,
        ledger_id: &str,
        target_t: i64,
    ) -> Result<HistoricalLedgerView> {
        let view = HistoricalLedgerView::load_at(
            &self.nameservice_mode,
            ledger_id,
            self.backend(),
            target_t,
        )
        .await?;

        Ok(view)
    }
}

// =============================================================================
// Ledger Creation
// =============================================================================

impl Fluree {
    /// Create a new empty ledger with genesis state
    ///
    /// This operation:
    /// 1. Normalizes the ledger ID (ensures branch suffix like `:main`)
    /// 2. Registers the ledger in the nameservice (fails if already exists)
    /// 3. Creates a genesis database with t=0 (no transactions yet)
    /// 4. Returns the new LedgerState ready for transactions
    ///
    /// # Arguments
    ///
    /// * `ledger_id` - Ledger ID (e.g., "mydb" or "mydb:main")
    ///
    /// # Errors
    ///
    /// Returns `ApiError::LedgerExists` (HTTP 409) if:
    /// - The ledger already exists
    /// - The ledger was previously dropped (retracted) - must use hard drop to reuse address
    ///
    /// # Example
    ///
    /// ```ignore
    /// let ledger = fluree.create_ledger("mydb").await?;
    /// // Now you can transact: fluree.insert(ledger, &data).await?
    /// ```
    pub async fn create_ledger(&self, ledger_id: &str) -> Result<LedgerState> {
        use fluree_db_core::ledger_id::normalize_ledger_id;
        use fluree_db_novelty::Novelty;
        use tracing::info;

        // 1. Normalize ledger_id (ensure branch suffix)
        let ledger_id = normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string());
        info!(ledger_id = %ledger_id, "Creating ledger");

        // 2. Register in nameservice via Publisher (fails if already exists)
        match self.publisher()?.publish_ledger_init(&ledger_id).await {
            Ok(()) => {}
            Err(NameServiceError::LedgerAlreadyExists(a)) => {
                return Err(ApiError::ledger_exists(a));
            }
            Err(e) => {
                return Err(e.into());
            }
        }

        // 3. Create genesis LedgerSnapshot with empty state at t=0
        let db = fluree_db_core::LedgerSnapshot::genesis(&ledger_id);

        // 4. Create LedgerState with empty Novelty (t=0)
        let ledger = LedgerState::new(db, Novelty::new(0));

        info!(ledger_id = %ledger_id, "Ledger created successfully");
        Ok(ledger)
    }

    /// Create a new branch for a ledger.
    ///
    /// Looks up the source branch to capture its current commit state, then
    /// creates a new [`NsRecord`] for `ledger_name:new_branch`.
    ///
    /// When `source_commit` is `None`, the new branch starts at the source's
    /// current HEAD and inherits its index (copied into the new branch's
    /// storage namespace so it's safe from GC on the source). This is the
    /// default behavior.
    ///
    /// When `source_commit` is `Some(ref)`, the ref is resolved to a canonical
    /// [`CommitId`] against the source branch, verified to be reachable from
    /// the source HEAD, and becomes the new branch's head. The index is not
    /// copied — the index at the source HEAD is typically too fresh for a
    /// historical branch point, so the new branch replays from genesis.
    ///
    /// Commits themselves are **not** copied — the branch's content store
    /// reads historical commits from the source namespace via fallback.
    ///
    /// # Errors
    ///
    /// - [`ApiError::LedgerExists`] if the branch already exists
    /// - [`ApiError::NotFound`] if the source branch does not exist, or if
    ///   `source_commit` resolves to a commit not reachable from source HEAD
    pub async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: Option<&str>,
        source_commit: Option<CommitRef>,
    ) -> Result<NsRecord> {
        use fluree_db_core::ledger_id::{format_ledger_id, validate_branch_name};
        use tracing::info;

        validate_branch_name(new_branch).map_err(|e| ApiError::Http {
            status: 400,
            message: e.to_string(),
        })?;

        let source = source_branch.unwrap_or("main");
        let source_id = format_ledger_id(ledger_name, source);
        let new_id = format_ledger_id(ledger_name, new_branch);

        info!(ledger_name, new_branch, source, "Creating branch");

        // Look up the source branch to capture its commit state
        let source_record = self
            .nameservice()
            .lookup(&source_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(source_id.clone()))?;

        // Verify the source branch has a commit head before creating.
        let source_head = source_record.commit_head_id.clone().ok_or_else(|| {
            ApiError::internal(format!("Source branch {source_id} has no commit head"))
        })?;

        // If the caller specified a historical commit, resolve it and verify
        // it's reachable from source HEAD before we touch the nameservice.
        let at_commit = if let Some(commit_ref) = source_commit {
            let view = self.ledger_cached(&source_id).await?.snapshot().await;
            let resolved = view.resolve_commit(commit_ref).await?;
            let store = self.content_store(&source_id);
            let commit = verify_ancestor_and_load(&*store, &source_head, &resolved).await?;
            Some((resolved, commit.t))
        } else {
            None
        };

        let is_historical = at_commit.is_some();

        self.nameservice()
            .create_branch(ledger_name, new_branch, source, at_commit)
            .await
            .map_err(|e| match e {
                NameServiceError::LedgerAlreadyExists(a) => ApiError::ledger_exists(a),
                other => other.into(),
            })?;

        // Historical branches replay from genesis — skip the index copy.
        // The source's current index reflects HEAD, which is too fresh.
        if !is_historical {
            if let Some(ref index_cid) = source_record.index_head_id {
                // Copy the source's index files into the new branch's
                // namespace so it owns its own copy, safe from GC on source.
                if let Err(e) = self
                    .copy_index_to_branch(&source_id, &new_id, index_cid)
                    .await
                {
                    tracing::warn!(
                        %e, source = %source_id, branch = %new_id,
                        "failed to copy index to branch; branch will replay from genesis"
                    );
                } else {
                    self.publisher()?
                        .publish_index(&new_id, source_record.index_t, index_cid)
                        .await?;
                }
            }
        }

        let record = self.nameservice().lookup(&new_id).await?.ok_or_else(|| {
            ApiError::internal(format!(
                "Branch {new_id} was created but not found in nameservice"
            ))
        })?;

        info!(
            ledger_name,
            new_branch, source, "Branch created successfully"
        );
        Ok(record)
    }

    /// Copy index artifacts (excluding dictionaries) from the source branch
    /// namespace into the target branch namespace.
    ///
    /// Copies the index root, fact leaves, branch manifests, and specialty
    /// arenas (numbig, vector, spatial, fulltext). Dictionary blobs are
    /// **not** copied — they are stored globally per ledger (not per branch),
    /// so all branches already share the same dict artifacts.
    pub(crate) async fn copy_index_to_branch(
        &self,
        source_id: &str,
        target_id: &str,
        index_cid: &fluree_db_core::ContentId,
    ) -> Result<()> {
        use fluree_db_binary_index::format::branch::read_branch_from_bytes;
        use fluree_db_binary_index::format::index_root::IndexRoot;
        use fluree_db_core::content_kind::ContentKind;
        use fluree_db_core::storage::content_address;
        use fluree_db_core::CODEC_FLUREE_DICT_BLOB;

        let storage = self.backend().admin_storage_cloned().ok_or_else(|| {
            ApiError::internal("copy_index_to_branch requires managed storage backend")
        })?;
        let method = storage.storage_method();
        let source_store = self.content_store(source_id);

        // Read and parse the index root
        let root_bytes = source_store.get(index_cid).await.map_err(|e| {
            ApiError::internal(format!("failed to read index root {index_cid}: {e}"))
        })?;
        let root = IndexRoot::decode(&root_bytes).map_err(|e| {
            ApiError::internal(format!("failed to decode index root {index_cid}: {e}"))
        })?;

        // Collect all CIDs referenced by the index root
        let mut all_cids = root.all_cas_ids();

        // Expand named graph branch manifests → leaf CIDs
        // (all_cas_ids includes branch CIDs but not the leaves within)
        for ng in &root.named_graphs {
            for (_, branch_cid) in &ng.orders {
                let branch_addr = content_address(
                    method,
                    ContentKind::IndexBranch,
                    source_id,
                    &branch_cid.digest_hex(),
                );
                if let Ok(branch_bytes) = storage.read_bytes(&branch_addr).await {
                    if let Ok(manifest) = read_branch_from_bytes(&branch_bytes) {
                        for leaf in &manifest.leaves {
                            all_cids.push(leaf.leaf_cid.clone());
                        }
                    }
                }
            }
        }

        // Add the root CID itself
        all_cids.push(index_cid.clone());

        // Skip dictionary blobs — they are stored globally per ledger (not
        // per branch), so all branches already share the same dict artifacts.
        // Also filter out CIDs with no recognized content kind.
        all_cids
            .retain(|cid| cid.codec() != CODEC_FLUREE_DICT_BLOB && cid.content_kind().is_some());

        // Deduplicate
        all_cids.sort();
        all_cids.dedup();

        // Copy artifacts concurrently from source to target namespace
        use futures::stream::{self, StreamExt, TryStreamExt};

        const COPY_CONCURRENCY: usize = 32;

        let source_label = source_id.to_string();
        let artifact_count = all_cids.len();

        stream::iter(all_cids.into_iter().map(|cid| {
            let kind = cid.content_kind().expect("filtered above");
            let hex = cid.digest_hex();
            let src_addr = content_address(method, kind, source_id, &hex);
            let dst_addr = content_address(method, kind, target_id, &hex);
            let storage = storage.clone();
            let cid_display = cid.to_string();
            let source_label = source_label.clone();
            async move {
                let bytes = storage.read_bytes(&src_addr).await.map_err(|e| {
                    ApiError::internal(format!(
                        "failed to read index artifact {cid_display} from {source_label}: {e}"
                    ))
                })?;
                storage
                    .write_bytes(&dst_addr, &bytes)
                    .await
                    .map_err(ApiError::from)
            }
        }))
        .buffer_unordered(COPY_CONCURRENCY)
        .try_for_each(|()| async { Ok(()) })
        .await?;

        tracing::info!(
            source = %source_id, target = %target_id,
            count = artifact_count,
            "copied index artifacts to branch namespace"
        );

        Ok(())
    }
}

impl Fluree {
    /// List all non-retracted branches for a ledger.
    pub async fn list_branches(&self, ledger_name: &str) -> Result<Vec<NsRecord>> {
        Ok(self.nameservice().list_branches(ledger_name).await?)
    }
}

/// Load `target` and verify it's reachable by walking parents from `source_head`.
///
/// Used by `create_branch` when a caller specifies a historical commit — we
/// only allow branching from commits on the source branch's ancestry.
async fn verify_ancestor_and_load<C: ContentStore + ?Sized>(
    store: &C,
    source_head: &CommitId,
    target: &CommitId,
) -> Result<fluree_db_core::Commit> {
    let target_commit = load_commit_by_id(store, target).await.map_err(|_| {
        ApiError::NotFound(format!("commit {target} not found in source namespace"))
    })?;

    if source_head == target {
        return Ok(target_commit);
    }

    // Walk backward from source_head, stopping once we pass below target's t.
    let stop_at = (target_commit.t - 1).max(0);
    let dag = collect_dag_cids(store, source_head, stop_at).await?;

    if dag.iter().any(|(_, cid)| cid == target) {
        Ok(target_commit)
    } else {
        Err(ApiError::NotFound(format!(
            "commit {target} is not an ancestor of source head {source_head}"
        )))
    }
}
