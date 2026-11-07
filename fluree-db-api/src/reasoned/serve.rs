//! Serving a reasoning query from a reasoned head.
//!
//! A generation is servable to a query when:
//! - its head is active and the generation belongs to the head's current
//!   revision;
//! - its basis is exactly the query's snapshot: the view is the branch's
//!   state at its commit head, of the same ledger instance. A staged view,
//!   or one assembled from other state, is never served;
//! - the reasoning the query settled to has the generation's settled key,
//!   which covers the program, the reasoned graph, rule sources, the schema
//!   bundle and the evaluation time;
//! - the view is unrestricted. A head is materialized without policy, so a
//!   policy-restricted view always reasons at query time.
//!
//! Anything else falls back to query-time reasoning, as do errors reading a
//! head: serving from a head is an acceleration, never a requirement.

use std::sync::Arc;

use fluree_db_core::reasoned_head::{HeadId, ReasonedManifest, RegistrationState};
use fluree_db_core::{BranchedContentStore, ContentId, ContentStore, ReasonedHeadTally};
use fluree_db_nameservice::NsRecord;
use fluree_db_query::ir::{ReasoningConfig, ReasoningFreshness};

use super::{program, LoadedGeneration};
use crate::view::GraphDb;
use crate::{ApiError, Fluree, LedgerState, Result};

/// A head view to run a query against, with reasoning off.
pub(crate) struct ServedHead {
    pub view: GraphDb,
    pub tally: ReasonedHeadTally,
    /// The generation the view reads, for the other graphs of a dataset.
    pub generation: Arc<LoadedGeneration>,
}

const STRICT_POLL: std::time::Duration = std::time::Duration::from_millis(50);

impl Fluree {
    /// The head generation that can answer a query with settled
    /// `reasoning` against `db`, if any, as the query's freshness mode
    /// allows. Only a `strict` query fails for want of one.
    pub(crate) async fn reasoned_head_for(
        &self,
        db: &GraphDb,
        reasoning: &ReasoningConfig,
    ) -> Result<Option<ServedHead>> {
        let strict = || ApiError::ReasonedHeadNotReady {
            ledger_id: db.ledger_id.to_string(),
            t: db.t,
        };
        let freshness = reasoning.modes.freshness;
        if !program::materializes(reasoning) || !db.is_root() {
            return match freshness {
                ReasoningFreshness::Strict if program::materializes(reasoning) => Err(strict()),
                _ => Ok(None),
            };
        }
        match freshness {
            ReasoningFreshness::Default | ReasoningFreshness::Exact => {
                Ok(self.servable(db, reasoning, false).await)
            }
            ReasoningFreshness::AsOf => match self.servable(db, reasoning, false).await {
                Some(served) => Ok(Some(served)),
                None => Ok(self.servable(db, reasoning, true).await),
            },
            ReasoningFreshness::Strict => {
                let deadline = std::time::Instant::now() + self.reasoned.strict_wait();
                loop {
                    if let Some(served) = self.servable(db, reasoning, false).await {
                        return Ok(Some(served));
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(strict());
                    }
                    tokio::time::sleep(STRICT_POLL).await;
                }
            }
        }
    }

    async fn servable(
        &self,
        db: &GraphDb,
        reasoning: &ReasoningConfig,
        as_of: bool,
    ) -> Option<ServedHead> {
        match self.try_servable(db, reasoning, as_of).await {
            Ok(served) => served,
            Err(e) => {
                tracing::debug!(ledger = %db.ledger_id, error = %e, "reasoned head not served");
                None
            }
        }
    }

    /// A generation at exactly `db`'s snapshot, or with `as_of` the newest
    /// one whose basis is at or before it on the branch's line.
    async fn try_servable(
        &self,
        db: &GraphDb,
        reasoning: &ReasoningConfig,
        as_of: bool,
    ) -> Result<Option<ServedHead>> {
        let ledger_id = db.ledger_id.as_ref();
        if self.reasoned.known_to_have_no_heads(ledger_id) {
            return Ok(None);
        }
        let doc = match self.nameservice().get_reasoned_heads(ledger_id).await? {
            Some(doc) if !doc.is_empty() => doc,
            _ => {
                self.reasoned.note_no_heads(ledger_id);
                return Ok(None);
            }
        };
        let Some(view_commit) = db.commit_id.clone() else {
            return Ok(None);
        };
        let Some(record) = self.nameservice().lookup(ledger_id).await? else {
            return Ok(None);
        };
        let instance = record
            .storage_root
            .as_ref()
            .and_then(fluree_db_core::StorageRoot::instance_bits)
            .unwrap_or(0);
        if instance != db.instance_bits {
            return Ok(None);
        }
        if !as_of
            && (record.commit_head_id.as_ref() != Some(&view_commit) || record.commit_t != db.t)
        {
            return Ok(None);
        }
        let settled = program::settled_key(reasoning, db.graph_id);
        for (head_id, entry) in &doc.heads {
            let Some(published) = &entry.published else {
                continue;
            };
            if !matches!(entry.registration.state, RegistrationState::Active)
                || published.revision != entry.registration.revision
            {
                continue;
            }
            let manifest_id = if !as_of {
                if published.basis.commit != view_commit
                    || published.basis.t != db.t
                    || published.settled != settled
                {
                    continue;
                }
                published.manifest.clone()
            } else {
                match self
                    .as_of_generation(
                        &record,
                        &view_commit,
                        head_id,
                        &published.manifest,
                        db.t,
                        &settled,
                    )
                    .await?
                {
                    Some(id) => id,
                    None => continue,
                }
            };
            let generation = self
                .loaded_generation(&record, head_id, &manifest_id)
                .await?;
            if generation.manifest.settled != settled
                || generation.manifest.revision != entry.registration.revision
            {
                continue;
            }
            return Ok(Some(ServedHead {
                view: head_view(db, &generation.state),
                tally: ReasonedHeadTally {
                    head_id: head_id.to_string(),
                    generation: generation.manifest.generation,
                    basis_t: generation.manifest.basis.t,
                    basis_commit: generation.manifest.basis.commit.to_string(),
                },
                generation,
            }));
        }
        Ok(None)
    }

    /// The newest retained generation of a head whose basis is on the line
    /// of the view's commit at or before `t`, computed under `settled`.
    async fn as_of_generation(
        &self,
        record: &NsRecord,
        view_commit: &ContentId,
        head_id: &HeadId,
        newest: &ContentId,
        t: i64,
        settled: &ContentId,
    ) -> Result<Option<ContentId>> {
        let store = self.head_store(record, head_id).await?;
        let generations = fluree_db_indexer::gc::heads::head_generations(store.as_ref(), newest)
            .await
            .map_err(|e| ApiError::internal(format!("read head chain: {e}")))?;
        let Some(oldest_t) = generations.iter().map(|g| g.manifest.basis.t).min() else {
            return Ok(None);
        };
        let branch_store = fluree_db_nameservice::branched_content_store_for_record(
            self.backend(),
            self.nameservice(),
            record,
        )
        .await?;
        let line = super::build::Line::walk(branch_store.as_ref(), view_commit, oldest_t).await?;
        Ok(generations
            .into_iter()
            .find(|g| {
                g.manifest.basis.t <= t
                    && &g.manifest.settled == settled
                    && line.on_line(&g.manifest.basis)
            })
            .map(|g| g.manifest_id))
    }

    async fn loaded_generation(
        &self,
        record: &NsRecord,
        head_id: &HeadId,
        manifest_id: &ContentId,
    ) -> Result<Arc<LoadedGeneration>> {
        let ledger_id = record.ledger_id.as_str();
        if let Some(loaded) = self.reasoned.loaded(ledger_id, head_id, manifest_id) {
            return Ok(loaded);
        }
        let store = self.head_store(record, head_id).await?;
        let bytes = store
            .get(manifest_id)
            .await
            .map_err(|e| ApiError::internal(format!("read manifest {manifest_id}: {e}")))?;
        let manifest = ReasonedManifest::decode(&bytes).map_err(ApiError::internal)?;
        let state = self.load_head_state(store, record, &manifest).await?;
        let loaded = Arc::new(LoadedGeneration {
            manifest_id: manifest_id.clone(),
            manifest,
            state,
        });
        self.reasoned.keep(ledger_id, head_id, Arc::clone(&loaded));
        Ok(loaded)
    }

    /// A head's store: its own namespace, falling back to the branch's for
    /// commits.
    pub(crate) async fn head_store(
        &self,
        record: &NsRecord,
        head_id: &HeadId,
    ) -> Result<Arc<dyn ContentStore>> {
        let branch_store = fluree_db_nameservice::branched_content_store_for_record(
            self.backend(),
            self.nameservice(),
            record,
        )
        .await?;
        let head_cs = self
            .backend()
            .content_store(&record.storage_namespace().reasoned_head(head_id.as_str()));
        Ok(Arc::new(BranchedContentStore::with_parents(
            head_cs,
            vec![BranchedContentStore::leaf(branch_store)],
        )))
    }

    /// The ledger state a generation's root describes: the branch at the
    /// basis, every graph, merged with the derived facts, and no novelty.
    async fn load_head_state(
        &self,
        store: Arc<dyn ContentStore>,
        record: &NsRecord,
        manifest: &ReasonedManifest,
    ) -> Result<LedgerState> {
        let mut head_record = record.clone();
        head_record.index_head_id = Some(manifest.root.clone());
        head_record.index_t = manifest.basis.t;
        head_record.commit_head_id = Some(manifest.basis.commit.clone());
        head_record.commit_t = manifest.basis.t;
        let (mut state, root) = LedgerState::load_with_store_decoding_root(
            Arc::clone(&store),
            head_record,
            crate::ledger_manager::decode_index_root,
        )
        .await?;
        crate::ledger_manager::attach_binary_store_from(
            store,
            &mut state,
            Some(Arc::clone(self.leaflet_cache())),
            None,
            root,
        )
        .await?;
        Ok(state)
    }
}

/// `base` with the head's state in place of its own: the same ledger,
/// configuration and graph, reading the head's index at its basis.
pub(crate) fn head_view(base: &GraphDb, head: &LedgerState) -> GraphDb {
    let head = GraphDb::from_ledger_state(head);
    let mut view = base.clone();
    view.t = head.t;
    view.commit_id = head.commit_id;
    view.snapshot = head.snapshot;
    view.overlay = head.overlay;
    view.novelty = head.novelty;
    view.binary_store = head.binary_store;
    view.dict_novelty = head.dict_novelty;
    view.runtime_small_dicts = head.runtime_small_dicts;
    view
}
