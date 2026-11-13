//! Serving a reasoning query from a reasoning index.
//!
//! A generation is servable to a query when:
//! - its index is active and the generation belongs to the index's current
//!   revision;
//! - its basis is exactly the query's snapshot: the view is the branch's
//!   state at its commit head, of the same ledger instance. A staged view,
//!   or one assembled from other state, is never served;
//! - the reasoning the query settled to has the generation's settled key,
//!   which covers the program, the reasoned graph, rule sources, the schema
//!   bundle, the evaluation time and the engine semantics version. A query
//!   is served only under the version it runs itself, so while instances of
//!   two versions run, each reasons at query time over the other's
//!   generations;
//! - for a request under a view policy, on its view or any graph of its
//!   dataset, the basis is exact even under `asOf`: an older generation would
//!   also take the policy's class lookups and conditions back to that commit.
//!   An index is materialized without policy; the caller's policy, for
//!   reasoning queries, judges what the generation view reads
//!   ([`generation_view`]).
//!
//! Anything else falls back to query-time reasoning, as do errors reading an
//! index: serving from an index is an acceleration, never a requirement.

use std::sync::Arc;

use fluree_db_core::reasoning_index::{
    ReasoningIndexEntry, ReasoningIndexId, ReasoningManifest, RegistrationState,
};
use fluree_db_core::{BranchedContentStore, ContentId, ContentStore, ReasoningIndexTally};
use fluree_db_nameservice::NsRecord;
use fluree_db_query::ir::{ReasoningConfig, ReasoningFreshness};

use super::{program, LoadedGeneration};
use crate::view::GraphDb;
use crate::{ApiError, Fluree, LedgerState, Result};

/// An index view to run a query against, with reasoning off.
pub(crate) struct ServedGeneration {
    pub view: GraphDb,
    pub tally: ReasoningIndexTally,
    /// The generation the view reads, for the other graphs of a dataset.
    pub generation: Arc<LoadedGeneration>,
}

const STRICT_POLL: std::time::Duration = std::time::Duration::from_millis(50);

impl Fluree {
    /// The index generation that can answer a query with settled
    /// `reasoning` against `db`, if any, as the query's freshness mode
    /// allows. Only a `strict` query fails for want of one.
    ///
    /// A `restricted` request (a view policy on `db` or on any other graph
    /// it reads) is served only a generation at exactly `db`'s snapshot: an
    /// older one would also rewind the facts its policy's class lookups and
    /// conditions read, so its `asOf` falls back to query time instead.
    pub(crate) async fn reasoning_index_for(
        &self,
        db: &GraphDb,
        reasoning: &ReasoningConfig,
        restricted: bool,
    ) -> Result<Option<ServedGeneration>> {
        let strict = || ApiError::ReasoningIndexNotReady {
            ledger_id: db.ledger_id.to_string(),
            t: db.t,
        };
        if !program::materializes(reasoning) {
            return Ok(None);
        }
        match reasoning.modes.freshness {
            ReasoningFreshness::Default | ReasoningFreshness::Exact => {
                Ok(self.servable(db, reasoning, false).await)
            }
            ReasoningFreshness::AsOf => match self.servable(db, reasoning, false).await {
                Some(served) => Ok(Some(served)),
                None if restricted => Ok(None),
                None => Ok(self.servable(db, reasoning, true).await),
            },
            ReasoningFreshness::Strict => {
                let deadline = std::time::Instant::now() + self.reasoning_index_state.strict_wait();
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
    ) -> Option<ServedGeneration> {
        match self.try_servable(db, reasoning, as_of).await {
            Ok(served) => served,
            Err(e) => {
                self.reasoning_index_state
                    .warn_serving(db.ledger_id.as_ref(), &e);
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
    ) -> Result<Option<ServedGeneration>> {
        let ledger_id = db.ledger_id.as_ref();
        if self
            .reasoning_index_state
            .known_to_have_no_indexes(ledger_id)
        {
            return Ok(None);
        }
        let doc = match self.nameservice().get_reasoning_indexes(ledger_id).await? {
            Some(doc) if !doc.is_empty() => doc,
            _ => {
                self.reasoning_index_state.note_no_indexes(ledger_id);
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
        let settled = program::settled_key(
            reasoning,
            db.graph_id,
            self.reasoning_index_state.engine_semantics(),
        );
        // Every index's eligible generation, newest basis first: two programs
        // can settle to the same reasoning, and `asOf` takes the newest.
        let mut candidates = Vec::new();
        for (index_id, entry) in &doc.indexes {
            let Some(published) = &entry.head else {
                continue;
            };
            if !matches!(entry.registration.state, RegistrationState::Active)
                || published.revision != entry.registration.revision
                || published.basis.epoch < entry.floor
            {
                continue;
            }
            if !as_of {
                if published.basis.commit == view_commit
                    && published.basis.t == db.t
                    && published.settled == settled
                {
                    candidates.push((
                        published.basis.t,
                        index_id,
                        entry,
                        published.manifest.clone(),
                    ));
                }
                continue;
            }
            let historical = self
                .as_of_generation(&record, &view_commit, index_id, entry, &settled, db)
                .await;
            let historical = match historical {
                Ok(generation) => generation,
                // A commit or an older manifest that did not read: nothing
                // here says the head is broken.
                Err(error) => {
                    self.reasoning_index_state.warn_serving(ledger_id, &error);
                    continue;
                }
            };
            if let Some((manifest_id, basis_t)) = historical {
                candidates.push((basis_t, index_id, entry, manifest_id));
            }
        }
        candidates.sort_by_key(|c| std::cmp::Reverse(c.0));
        for (_, index_id, entry, manifest_id) in candidates {
            let generation = match self
                .loaded_generation(&record, index_id, &manifest_id)
                .await
            {
                Ok(generation) => generation,
                Err(error) => {
                    self.reasoning_index_state.warn_serving(ledger_id, &error);
                    // An older generation can be released between listing
                    // and loading it; only the head is expected to read.
                    if entry.head.as_ref().map(|head| &head.manifest) == Some(&manifest_id) {
                        self.reasoning_index_state.request_repair(
                            ledger_id,
                            index_id,
                            &manifest_id,
                            error.to_string(),
                        );
                    }
                    continue;
                }
            };
            if generation.manifest.settled != settled
                || generation.manifest.revision != entry.registration.revision
                || generation.manifest.basis.epoch < entry.floor
            {
                continue;
            }
            return Ok(Some(ServedGeneration {
                view: generation_view(db, &generation.state),
                tally: ReasoningIndexTally {
                    index_id: index_id.to_string(),
                    generation: generation.manifest.generation,
                    basis_t: generation.manifest.basis.t,
                    basis_commit: generation.manifest.basis.commit.to_string(),
                },
                generation,
            }));
        }
        Ok(None)
    }

    /// The newest retained generation of an index whose basis is on the line
    /// of the view's commit at or before `db`'s `t`, computed under `settled`,
    /// before no drop `db` records, and at no point a removal took an
    /// occurrence out of: a generation holds what a graph held at its basis,
    /// which a later drop takes out as of every `t`, and a removal out of
    /// every read as of a `t` the occurrence was current at.
    #[cfg(not(target_arch = "wasm32"))]
    async fn as_of_generation(
        &self,
        record: &NsRecord,
        view_commit: &ContentId,
        index_id: &ReasoningIndexId,
        entry: &ReasoningIndexEntry,
        settled: &ContentId,
        db: &GraphDb,
    ) -> Result<Option<(ContentId, i64)>> {
        let Some(head) = &entry.head else {
            return Ok(None);
        };
        let store = self.index_store(record, index_id).await?;
        let generations = fluree_db_indexer::gc::reasoning_indexes::index_generations(
            store.as_ref(),
            &head.manifest,
        )
        .await
        .map_err(|e| ApiError::internal(format!("read index chain: {e}")))?;
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
        for g in generations {
            let basis = &g.manifest.basis;
            let eligible = basis.t <= db.t
                && basis.epoch >= entry.floor
                && &g.manifest.settled == settled
                && line.on_line(basis)
                && db.snapshot.graph_registry.dropped_after(basis.t).is_empty();
            if eligible && !self.removed_as_of(&db.ledger_id, basis.t).await? {
                return Ok(Some((g.manifest_id, basis.t)));
            }
        }
        Ok(None)
    }

    /// Whether a read of everything on `ledger_id` as of `t` would need an
    /// occurrence a removal took out. A generation holds every fact of its
    /// basis, so answering from one is such a read.
    #[cfg(not(target_arch = "wasm32"))]
    async fn removed_as_of(&self, ledger_id: &str, t: i64) -> Result<bool> {
        use crate::retention::suppression::{HistoricalRead, Reads};
        let read = HistoricalRead {
            to_t: t,
            from_t: None,
            reads: || Reads::Everything,
        };
        match self.refuse_suppressed_history(ledger_id, read).await {
            Ok(()) => Ok(false),
            Err(ApiError::HistoryUnavailable(_)) => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// Walking an index's generations needs the native indexer: a wasm build
    /// serves `asOf` only from a generation at exactly the queried commit.
    #[cfg(target_arch = "wasm32")]
    async fn as_of_generation(
        &self,
        _record: &NsRecord,
        _view_commit: &ContentId,
        _index_id: &ReasoningIndexId,
        _entry: &ReasoningIndexEntry,
        _settled: &ContentId,
        _db: &GraphDb,
    ) -> Result<Option<(ContentId, i64)>> {
        Ok(None)
    }

    pub(super) async fn loaded_generation(
        &self,
        record: &NsRecord,
        index_id: &ReasoningIndexId,
        manifest_id: &ContentId,
    ) -> Result<Arc<LoadedGeneration>> {
        let ledger_id = record.ledger_id.as_str();
        if let Some(loaded) = self
            .reasoning_index_state
            .loaded(ledger_id, index_id, manifest_id)
        {
            return Ok(loaded);
        }
        let store = self.index_store(record, index_id).await?;
        let bytes = store
            .get(manifest_id)
            .await
            .map_err(|e| ApiError::internal(format!("read manifest {manifest_id}: {e}")))?;
        let manifest = ReasoningManifest::decode(&bytes).map_err(ApiError::internal)?;
        let state = self.load_generation_state(store, record, &manifest).await?;
        let loaded = Arc::new(LoadedGeneration {
            manifest_id: manifest_id.clone(),
            manifest,
            state,
        });
        self.reasoning_index_state
            .keep(ledger_id, index_id, Arc::clone(&loaded));
        Ok(loaded)
    }

    /// An index's store: its own namespace, falling back to the branch's for
    /// commits.
    pub(crate) async fn index_store(
        &self,
        record: &NsRecord,
        index_id: &ReasoningIndexId,
    ) -> Result<Arc<dyn ContentStore>> {
        let branch_store = fluree_db_nameservice::branched_content_store_for_record(
            self.backend(),
            self.nameservice(),
            record,
        )
        .await?;
        let index_cs = self.backend().content_store(
            &record
                .storage_namespace()
                .reasoning_index(index_id.as_str()),
        );
        Ok(Arc::new(BranchedContentStore::with_parents(
            index_cs,
            vec![BranchedContentStore::leaf(branch_store)],
        )))
    }

    /// The ledger state a generation's root describes: the branch at the
    /// basis, every graph, merged with the derived facts, and no novelty.
    async fn load_generation_state(
        &self,
        store: Arc<dyn ContentStore>,
        record: &NsRecord,
        manifest: &ReasoningManifest,
    ) -> Result<LedgerState> {
        let mut at_basis = record.clone();
        at_basis.commit_head_id = Some(manifest.basis.commit.clone());
        at_basis.commit_t = manifest.basis.t;
        self.load_index_state(store, &at_basis, &manifest.root, manifest.basis.t)
            .await
    }

    /// The ledger state `root` (indexed at `index_t`) describes with the
    /// commits after it, up to `record`'s commit head, replayed as novelty.
    pub(crate) async fn load_index_state(
        &self,
        store: Arc<dyn ContentStore>,
        record: &NsRecord,
        root: &ContentId,
        index_t: i64,
    ) -> Result<LedgerState> {
        let mut index_record = record.clone();
        index_record.index_head_id = Some(root.clone());
        index_record.index_t = index_t;
        let (mut state, root) = LedgerState::load_with_store_decoding_root(
            Arc::clone(&store),
            index_record,
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

/// `base` with a generation's state in place of its own: the same ledger,
/// configuration and graph, reading the generation's index at its basis.
/// That index holds derived facts, so `base`'s policy judges them as it does
/// in a reasoning query; `base` itself is unchanged.
pub(crate) fn generation_view(base: &GraphDb, generation: &LedgerState) -> GraphDb {
    let generation = GraphDb::from_ledger_state(generation);
    let mut view = base.clone();
    view.t = generation.t;
    view.commit_id = generation.commit_id;
    view.snapshot = generation.snapshot;
    view.overlay = generation.overlay;
    view.novelty = generation.novelty;
    view.binary_store = generation.binary_store;
    view.dict_novelty = generation.dict_novelty;
    view.runtime_small_dicts = generation.runtime_small_dicts;
    view.with_reasoned_policy()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{IndexType, RangeMatch, RangeTest, Tracker};
    use serde_json::json;

    /// A restricted view's generation judges by reasoned classes: a rule
    /// makes `ex:alice` a `HighEarner`, so a deny on that class hides her
    /// stored salary there. The view's policy and enforcer agree, and the
    /// base view keeps its ordinary policy.
    #[tokio::test]
    async fn a_generation_view_judges_by_reasoned_classes() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "serve/generation-policy:main";
        let ctx = json!({"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"});
        let rule = json!({
            "@context": {"ex": "http://example.org/"},
            "where": [{"@id": "?p", "ex:salary": "?s"}, ["filter", "(> ?s 200000)"]],
            "insert": {"@id": "?p", "@type": "ex:HighEarner"}
        });
        let ledger = fluree.create_ledger(id).await.unwrap();
        fluree
            .insert(
                ledger,
                &json!({"@context": ctx, "@graph": [
                    {"@id": "ex:rule", "f:rule": {"@type": "@json", "@value": rule}},
                    {"@id": "ex:alice", "ex:salary": 250_000}
                ]}),
            )
            .await
            .unwrap();
        let index = fluree
            .register_reasoning_index(id, &json!({"reasoning": "datalog"}))
            .await
            .unwrap();
        fluree.refresh_reasoning_index(id, &index).await.unwrap();

        let ledger = fluree.ledger(id).await.unwrap();
        let opts = crate::GovernanceOptions {
            policy: Some(json!([{
                "@id": "ex:denyHighEarner",
                "f:required": true,
                "f:onClass": [{"@id": "http://example.org/HighEarner"}],
                "f:action": "f:view",
                "f:allow": false
            }])),
            default_allow: Some(true),
            ..Default::default()
        };
        let base = fluree
            .wrap_policy(GraphDb::from_ledger_state(&ledger), &opts)
            .await
            .unwrap();
        let query = json!({
            "@context": ctx,
            "select": "?v",
            "where": {"@id": "ex:alice", "ex:salary": "?v"},
            "reasoning": "datalog"
        });
        let (_, parsed) =
            crate::query::helpers::parse_jsonld_query(&query, &base.snapshot, None, None).unwrap();
        let executable = fluree
            .build_executable_for_view(&base, &parsed, None)
            .await
            .unwrap();
        let served = fluree
            .try_servable(&base, &executable.reasoning, false)
            .await
            .unwrap()
            .expect("a generation at the base's commit");
        let view = &served.view;

        let policy = view.policy().unwrap();
        assert!(std::ptr::eq(
            policy.view_set(),
            policy.wrapper().reasoned_view()
        ));
        assert!(std::ptr::eq(
            view.policy_enforcer().unwrap().policy(),
            policy
        ));
        let ordinary = base.policy().unwrap();
        assert!(std::ptr::eq(ordinary.view_set(), ordinary.wrapper().view()));

        let db = view.as_graph_db_ref();
        let alice = view
            .snapshot
            .encode_iri("http://example.org/alice")
            .unwrap();
        let salary = view
            .snapshot
            .encode_iri("http://example.org/salary")
            .unwrap();
        let flakes = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(alice, salary),
            )
            .await
            .unwrap();
        assert_eq!(flakes.len(), 1);
        let visible = view
            .policy_enforcer()
            .unwrap()
            .filter_flakes_for_graph(
                db.snapshot,
                db.g_id,
                db.overlay,
                db.t,
                &Tracker::disabled(),
                flakes,
            )
            .await
            .unwrap();
        assert!(visible.is_empty(), "{visible:?}");
    }
}
