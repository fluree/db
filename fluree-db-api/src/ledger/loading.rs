use std::sync::Arc;

use crate::ledger_view::CommitRef;
use crate::{ApiError, Fluree, HistoricalLedgerView, LedgerState, Result};
use fluree_db_core::ContentStore;
use fluree_db_core::{collect_dag_cids, load_commit_envelope_by_id, CommitId};
use fluree_db_nameservice::{NameServiceError, NsRecord};

impl Fluree {
    /// Attach a binary index store and range provider to an already-loaded
    /// ledger state when its nameservice record points at a binary index root.
    pub(crate) async fn attach_binary_index_store(&self, state: &mut LedgerState) -> Result<()> {
        crate::ledger_manager::load_and_attach_binary_store(
            self.backend(),
            self.nameservice(),
            state,
            &self.binary_store_cache_dir(),
            Some(Arc::clone(self.leaflet_cache())),
        )
        .await?;
        Ok(())
    }

    /// Load a branch-aware ledger state from an already-resolved nameservice
    /// record and content store, then attach the binary range provider needed
    /// for any path that will run indexed range queries.
    pub(crate) async fn load_queryable_state_with_store<C>(
        &self,
        store: C,
        record: NsRecord,
    ) -> Result<LedgerState>
    where
        C: ContentStore + Clone + 'static,
    {
        let mut state = LedgerState::load_with_store(store, record).await?;
        self.attach_binary_index_store(&mut state).await?;
        Ok(state)
    }

    /// Load a ledger by address (e.g., "mydb:main")
    ///
    /// This loads the ledger state using the connection-wide cache.
    /// The ledger state combines the indexed database with any uncommitted novelty transactions.
    pub async fn ledger(&self, ledger_id: &str) -> Result<LedgerState> {
        let mut state =
            LedgerState::load(&self.nameservice_mode, ledger_id, self.backend()).await?;
        self.attach_binary_index_store(&mut state).await?;
        // Default context is not loaded here. Opt-in callers route through
        // `Fluree::db_with_default_context` / `db_at_with_default_context`,
        // which fetch and attach the context onto the returned `GraphDb`.
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
        // A ref that resolves to the source head itself is collapsed to the
        // default (None) path so the user still gets an index copy — otherwise
        // `--at <head-cid>` would silently produce a slower branch than
        // omitting `--at` entirely.
        let at_commit = if let Some(commit_ref) = source_commit {
            let view = self.ledger_cached(&source_id).await?.snapshot().await;
            let resolved = view.resolve_commit(commit_ref).await?;
            if resolved == source_head {
                None
            } else {
                let store = self.content_store(&source_id);
                let resolved_t = verify_ancestor(&*store, &source_head, &resolved).await?;
                Some((resolved, resolved_t))
            }
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
        // Branch-aware source store so the copy can read inherited index
        // artifacts when `source_id` itself is a branch with ancestors.
        let source_store = fluree_db_nameservice::branched_content_store_for_id(
            self.backend(),
            self.nameservice(),
            source_id,
        )
        .await
        .map_err(ApiError::from)?;

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

/// Verify `target` is reachable by walking parents from `source_head`, and
/// return its `t` value.
///
/// Used by `create_branch` when a caller specifies a historical commit — we
/// only allow branching from commits on the source branch's ancestry.
/// Loads only the target commit's envelope (not its flakes) since we just
/// need `t` for the ancestry walk's stop condition.
async fn verify_ancestor<C: ContentStore + ?Sized>(
    store: &C,
    source_head: &CommitId,
    target: &CommitId,
) -> Result<i64> {
    let target_envelope = load_commit_envelope_by_id(store, target)
        .await
        .map_err(|_| {
            ApiError::NotFound(format!("commit {target} not found in source namespace"))
        })?;

    if source_head == target {
        return Ok(target_envelope.t);
    }

    // Walk backward from source_head, stopping once we pass below target's t.
    let stop_at = (target_envelope.t - 1).max(0);
    let dag = collect_dag_cids(store, source_head, stop_at).await?;

    if dag.iter().any(|(_, cid)| cid == target) {
        Ok(target_envelope.t)
    } else {
        Err(ApiError::NotFound(format!(
            "commit {target} is not an ancestor of source head {source_head}"
        )))
    }
}
