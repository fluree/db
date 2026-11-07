//! Serving a reasoning query from a reasoned head.
//!
//! A generation is servable to a query when:
//! - its head is active and the generation belongs to the head's current
//!   revision;
//! - its basis is exactly the query's snapshot: the branch's commit head,
//!   at the query's `t`;
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
use fluree_db_query::ir::ReasoningConfig;

use super::{program, LoadedGeneration};
use crate::view::GraphDb;
use crate::{ApiError, Fluree, LedgerState, Result};

/// A head view to run a query against, with reasoning off.
pub(crate) struct ServedHead {
    pub view: GraphDb,
    pub tally: ReasonedHeadTally,
}

impl Fluree {
    /// The head generation that can answer a query with settled
    /// `reasoning` against `db`, if any.
    pub(crate) async fn reasoned_head_for(
        &self,
        db: &GraphDb,
        reasoning: &ReasoningConfig,
    ) -> Option<ServedHead> {
        if !program::materializes(reasoning) || !db.is_root() {
            return None;
        }
        match self.try_reasoned_head_for(db, reasoning).await {
            Ok(served) => served,
            Err(e) => {
                tracing::debug!(ledger = %db.ledger_id, error = %e, "reasoned head not served");
                None
            }
        }
    }

    async fn try_reasoned_head_for(
        &self,
        db: &GraphDb,
        reasoning: &ReasoningConfig,
    ) -> Result<Option<ServedHead>> {
        let ledger_id = db.ledger_id.as_ref();
        let doc = match self.nameservice().get_reasoned_heads(ledger_id).await? {
            Some(doc) if !doc.is_empty() => doc,
            _ => return Ok(None),
        };
        let Some(record) = self.nameservice().lookup(ledger_id).await? else {
            return Ok(None);
        };
        let Some(commit) = record.commit_head_id.as_ref() else {
            return Ok(None);
        };
        if record.commit_t != db.t {
            return Ok(None);
        }
        let settled = program::settled_key(reasoning, db.graph_id);
        for (head_id, entry) in &doc.heads {
            let Some(published) = &entry.published else {
                continue;
            };
            if !matches!(entry.registration.state, RegistrationState::Active)
                || published.revision != entry.registration.revision
                || &published.basis.commit != commit
                || published.basis.t != db.t
            {
                continue;
            }
            let generation = self
                .loaded_generation(&record, head_id, &published.manifest)
                .await?;
            if generation.manifest.settled != settled {
                continue;
            }
            return Ok(Some(ServedHead {
                view: head_view(db, &generation.state),
                tally: ReasonedHeadTally {
                    head_id: head_id.to_string(),
                    generation: generation.manifest.generation,
                    basis_t: generation.manifest.basis.t,
                },
            }));
        }
        Ok(None)
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
/// configuration and `t`, reading the head's index.
fn head_view(base: &GraphDb, head: &LedgerState) -> GraphDb {
    let head = GraphDb::from_ledger_state(head);
    let mut view = base.clone();
    view.snapshot = head.snapshot;
    view.overlay = head.overlay;
    view.novelty = head.novelty;
    view.binary_store = head.binary_store;
    view.dict_novelty = head.dict_novelty;
    view.runtime_small_dicts = head.runtime_small_dicts;
    view
}
