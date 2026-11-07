//! Registering reasoning indexes, and building and publishing their
//! generations.
//!
//! A generation is the branch at a basis commit S, every graph, merged with
//! the complete output C of the index's program at S:
//!
//! > M_h(S) = A_h(S) ∪ C_h(P, g, S)
//!
//! Each generation also persists D(S) = C(S) \ A(S), the facts present only
//! because they were derived. Moving from generation k (basis S1) to k+1
//! (basis S2) recomputes C(S2) in full — never seeded from the previous
//! closure — and the indexer folds the window's commits plus one set of
//! derived corrections into the previous root:
//!
//! - assert `D(S2) \ D_k`, plus facts of `D(S2) ∩ D_k` a window commit
//!   retracted;
//! - retract facts of `D_k \ D(S2)` that are not asserted at S2 and that no
//!   window commit already retracted.
//!
//! Applied after the window's commits at S2's `t` (where an assertion holds
//! over a retraction), that leaves exactly A(S2) ∪ D(S2). A rebuild copies
//! the base index instead, and asserts all of D(S2).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use fluree_db_core::reasoning_index::{
    admit, Basis, BasisOracle, Candidate, Discard, ErasureEpoch, GenerationKind, GenerationStats,
    ReasoningIndexEntry, ReasoningIndexId, ReasoningIndexes, ReasoningManifest, Registration,
    RegistrationState,
};
use fluree_db_core::{
    BranchedContentStore, ContentId, ContentKind, ContentStore, Flake, GraphId, IndexType,
    LedgerId, OverlayProvider, Sid,
};
use fluree_db_nameservice::{NsRecord, ReasoningIndexesCasResult};
use fluree_db_query::execute::{compute_derived_facts, QueryRequest};
use fluree_db_query::schema_bundle::SchemaBundleOverlay;
use serde_json::Value as JsonValue;

use super::derived::{encode_facts, read_derived_only, write_derived_only};
use super::program;
use crate::query::helpers::parse_jsonld_query;
use crate::view::GraphDb;
use crate::{ApiError, Fluree, Result};

const CAS_ATTEMPTS: usize = 16;

/// Active indexes a branch may register. Each is a complete index of the
/// branch, refreshed after every commit.
pub const MAX_ACTIVE_INDEXES: usize = 8;

/// The materialization budget a worker runs under unless the program sets
/// one: generous, since a capped result is never published.
const WORKER_MAX_FACTS: u64 = 100_000_000;
const WORKER_MAX_SECONDS: u64 = 3_600;

/// What a refresh of one index did.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "outcome", rename_all = "camelCase")]
pub enum RefreshOutcome {
    /// A new generation was published.
    #[serde(rename_all = "camelCase")]
    Published {
        generation: u64,
        kind: GenerationKind,
        basis_t: i64,
        stats: GenerationStats,
    },
    /// The published generation is already at the branch's head.
    #[serde(rename_all = "camelCase")]
    Current { generation: u64 },
    /// The result was not eligible to publish; nothing was published.
    Discarded { reason: String },
    /// The program could not be computed completely; nothing was published.
    Failed { reason: String },
    /// Another refresh of this index is under way.
    InFlight,
    /// The index already holds as many superseded generations inside the
    /// retention window as it may; publication waits for them to age out.
    #[serde(rename_all = "camelCase")]
    Throttled { in_guard: usize },
    /// A storage sweep of the ledger is running; a later refresh retries.
    Deferred { reason: String },
}

fn now_ms() -> i64 {
    use fluree_db_core::clock::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn canonical_id(ledger_id: &str) -> Result<String> {
    Ok(LedgerId::parse(ledger_id)
        .map_err(|e| ApiError::invalid_query(e.to_string()))?
        .to_string())
}

impl Fluree {
    /// The branch's reasoning-indexes document.
    pub async fn reasoning_indexes(&self, ledger_id: &str) -> Result<ReasoningIndexes> {
        let id = canonical_id(ledger_id)?;
        self.nameservice()
            .get_reasoning_indexes(&id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("ledger {id} not found")))
    }

    async fn branch_record(&self, ledger_id: &str) -> Result<NsRecord> {
        self.nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("ledger {ledger_id} not found")))
    }

    /// Apply `change` to the branch's reasoning-indexes document by compare-and-swap,
    /// retrying from the document as it stands on a conflict.
    async fn update_reasoning_indexes<T>(
        &self,
        ledger_id: &str,
        mut change: impl FnMut(&ReasoningIndexes) -> Result<(Option<ReasoningIndexes>, T)>,
    ) -> Result<T> {
        let record = self.branch_record(ledger_id).await?;
        let mut actual = self.reasoning_indexes(ledger_id).await?;
        for _ in 0..CAS_ATTEMPTS {
            let (next, value) = change(&actual)?;
            let Some(mut next) = next else {
                return Ok(value);
            };
            next.v = actual.v + 1;
            match self
                .publisher()?
                .push_reasoning_indexes_fenced(ledger_id, record.fence, actual.v, &next)
                .await?
            {
                ReasoningIndexesCasResult::Updated => return Ok(value),
                ReasoningIndexesCasResult::Conflict { actual: Some(now) } => actual = now,
                ReasoningIndexesCasResult::Conflict { actual: None } => {
                    return Err(ApiError::NotFound(format!("ledger {ledger_id} not found")))
                }
            }
        }
        Err(ApiError::internal(format!(
            "the reasoning-indexes document of {ledger_id} kept changing; gave up after \
             {CAS_ATTEMPTS} attempts"
        )))
    }

    /// Register a reasoning program for persistence on a branch, returning
    /// its index's id. Registering a program the branch already has active is
    /// a no-op that returns the existing index.
    ///
    /// `program` carries the reasoning keys a query would (`reasoning`,
    /// `rules`, `ontology`, `reasoningTime`, `reasoningBudget`, `@context`)
    /// and optionally `graph`, the IRI of the graph to reason against. The
    /// index fills on the next [`Self::refresh_reasoning_indexes`].
    pub async fn register_reasoning_index(
        &self,
        ledger_id: &str,
        program: &JsonValue,
    ) -> Result<ReasoningIndexId> {
        let id = canonical_id(ledger_id)?;
        let canonical = program::canonical_program(program)?;
        let key = program::registration_key(&canonical);
        self.update_reasoning_indexes(&id, |actual| {
            if let Some((index_id, _)) = actual.indexes.iter().find(|(_, e)| {
                matches!(e.registration.state, RegistrationState::Active)
                    && e.registration.semantics == key
            }) {
                return Ok((None, index_id.clone()));
            }
            let active = actual
                .indexes
                .values()
                .filter(|e| matches!(e.registration.state, RegistrationState::Active))
                .count();
            if active >= MAX_ACTIVE_INDEXES {
                return Err(ApiError::invalid_query(format!(
                    "{id} already has {active} active reasoning indexes, the most a branch may \
                     have; retire one first"
                )));
            }
            let index_id = ReasoningIndexId::parse(&ulid::Ulid::new().to_string())
                .map_err(ApiError::internal)?;
            let mut next = actual.clone();
            next.indexes.insert(
                index_id.clone(),
                ReasoningIndexEntry {
                    registration: Registration {
                        revision: 1,
                        semantics: key.clone(),
                        program: canonical.clone(),
                        state: RegistrationState::Active,
                    },
                    floor: ErasureEpoch::default(),
                    head: None,
                },
            );
            Ok((Some(next), index_id))
        })
        .await
        .inspect(|_| self.reasoning_index_state.note_registered(&id))
    }

    /// Retire an index: from now on workers are refused and queries stop being
    /// served from it. Its files stay until a cleanup pass removes them.
    pub async fn retire_reasoning_index(
        &self,
        ledger_id: &str,
        index_id: &ReasoningIndexId,
    ) -> Result<()> {
        let id = canonical_id(ledger_id)?;
        let at_ms = now_ms();
        self.update_reasoning_indexes(&id, |actual| {
            let Some(entry) = actual.indexes.get(index_id) else {
                return Err(ApiError::NotFound(format!(
                    "reasoning index {index_id} is not registered on {id}"
                )));
            };
            if matches!(entry.registration.state, RegistrationState::Retired { .. }) {
                return Ok((None, ()));
            }
            let mut next = actual.clone();
            let entry = next.indexes.get_mut(index_id).expect("present above");
            entry.registration.revision += 1;
            entry.registration.state = RegistrationState::Retired { at_ms };
            Ok((Some(next), ()))
        })
        .await?;
        self.reasoning_index_state.forget_index(&id, index_id);
        Ok(())
    }

    /// Delete the files of indexes retired longer than the retention window,
    /// then their entries. Their shared dictionary blobs are left for the
    /// sweep. Returns the indexes removed.
    pub async fn clean_retired_reasoning_indexes(
        &self,
        ledger_id: &str,
    ) -> Result<Vec<ReasoningIndexId>> {
        let id = canonical_id(ledger_id)?;
        let doc = self.reasoning_indexes(&id).await?;
        let min_age_ms = self.reasoning_index_state.retention().min_age_ms;
        let now = now_ms();
        let due: Vec<ReasoningIndexId> = doc
            .indexes
            .iter()
            .filter(|(_, e)| {
                matches!(e.registration.state, RegistrationState::Retired { at_ms } if now - at_ms >= min_age_ms)
            })
            .map(|(index_id, _)| index_id.clone())
            .collect();
        if due.is_empty() {
            return Ok(Vec::new());
        }
        let Some(storage) = self.backend().admin_storage_cloned() else {
            return Ok(Vec::new());
        };
        let record = self.branch_record(&id).await?;
        let method = storage.storage_method().to_string();
        let mut cleaned = Vec::new();
        for index_id in due {
            let ns = record
                .storage_namespace()
                .reasoning_index(index_id.as_str());
            let prefix = format!("fluree:{method}://{}/", ns.branch_prefix());
            let files = storage
                .list_prefix(&prefix)
                .await
                .map_err(|e| ApiError::internal(format!("list {prefix}: {e}")))?;
            let mut failed = false;
            for file in &files {
                if let Err(e) = storage.delete(file).await {
                    tracing::warn!(file = %file, error = %e, "could not delete a retired index's file");
                    failed = true;
                }
            }
            if !failed {
                cleaned.push(index_id);
            }
        }
        // Remove the entries only once the files are gone: a Retired entry is
        // how a later pass finds a folder an interrupted one left.
        let removed = self
            .update_reasoning_indexes(&id, |actual| {
                let mut next = actual.clone();
                let before = next.indexes.len();
                next.indexes.retain(|index_id, e| {
                    !(cleaned.contains(index_id)
                        && matches!(e.registration.state, RegistrationState::Retired { .. }))
                });
                Ok((
                    (next.indexes.len() != before).then_some(next),
                    cleaned.clone(),
                ))
            })
            .await?;
        for index_id in &removed {
            self.reasoning_index_state.forget_index(&id, index_id);
        }
        Ok(removed)
    }

    /// Bring every active index of the branch up to its current commit.
    pub async fn refresh_reasoning_indexes(
        &self,
        ledger_id: &str,
    ) -> Result<Vec<(ReasoningIndexId, RefreshOutcome)>> {
        let doc = self.reasoning_indexes(ledger_id).await?;
        let mut out = Vec::new();
        for (index_id, entry) in &doc.indexes {
            if matches!(entry.registration.state, RegistrationState::Active) {
                let outcome = self.refresh_reasoning_index(ledger_id, index_id).await?;
                out.push((index_id.clone(), outcome));
            }
        }
        Ok(out)
    }

    /// Bring one index up to the branch's current commit: compute its program,
    /// fold the result into a new generation, and publish it.
    pub async fn refresh_reasoning_index(
        &self,
        ledger_id: &str,
        index_id: &ReasoningIndexId,
    ) -> Result<RefreshOutcome> {
        let id = canonical_id(ledger_id)?;
        let Some(_flight) = self.reasoning_index_state.begin(&id, index_id) else {
            return Ok(RefreshOutcome::InFlight);
        };
        self.refresh_index_exclusive(&id, index_id).await
    }

    async fn refresh_index_exclusive(
        &self,
        ledger_id: &str,
        index_id: &ReasoningIndexId,
    ) -> Result<RefreshOutcome> {
        let (record, db) = self.consistent_base(ledger_id).await?;
        let Some(commit) = record.commit_head_id.clone() else {
            return Ok(RefreshOutcome::Failed {
                reason: "the branch has no commits".into(),
            });
        };
        let basis = Basis {
            commit,
            t: record.commit_t,
            epoch: ErasureEpoch::default(),
        };
        let doc = self.reasoning_indexes(ledger_id).await?;
        let Some(entry) = doc.indexes.get(index_id) else {
            return Err(ApiError::NotFound(format!(
                "reasoning index {index_id} is not registered on {ledger_id}"
            )));
        };
        if !matches!(entry.registration.state, RegistrationState::Active) {
            return Ok(RefreshOutcome::Discarded {
                reason: "the index is retired".into(),
            });
        }
        let revision = entry.registration.revision;

        // Settle the program as a query would, against the reasoned graph.
        // Before the currency check: what the program settles to can change
        // without the branch moving, when a schema or rules source it reads
        // lives elsewhere.
        let program_json = &entry.registration.program;
        let db = match program_json.get("graph").and_then(|g| g.as_str()) {
            Some(iri) => match db.snapshot.graph_registry.existing_graph_id(iri, db.t) {
                Ok(Some(g_id)) => Fluree::select_graph_id(db, g_id)?,
                Ok(None) | Err(_) => {
                    return Ok(RefreshOutcome::Failed {
                        reason: format!("the reasoned graph {iri} does not exist"),
                    })
                }
            },
            None => db,
        };
        let (_, parsed) = parse_jsonld_query(
            &program::settling_query(program_json),
            &db.snapshot,
            db.default_context.as_ref(),
            None,
        )?;
        let mut executable = self.build_executable_for_view(&db, &parsed, None).await?;
        if !program::materializes(&executable.reasoning) {
            return Ok(RefreshOutcome::Failed {
                reason: "the program, as the branch settles it, materializes nothing".into(),
            });
        }
        let settled = program::settled_key(&executable.reasoning, db.graph_id);
        if let Some(published) = &entry.head {
            if published.revision == revision
                && published.basis == basis
                && published.settled == settled
            {
                return Ok(RefreshOutcome::Current {
                    generation: published.generation,
                });
            }
        }

        // Publication waits rather than let the index collector shorten the
        // protection of generations queries may still be reading.
        if let Some(published) = &entry.head {
            let retention = self.reasoning_index_state.retention();
            let index_cs = self.backend().content_store(
                &record
                    .storage_namespace()
                    .reasoning_index(index_id.as_str()),
            );
            let generations = fluree_db_indexer::gc::reasoning_indexes::index_generations(
                index_cs.as_ref(),
                &published.manifest,
            )
            .await
            .map_err(|e| ApiError::internal(format!("read index chain: {e}")))?;
            let plan = fluree_db_indexer::gc::reasoning_indexes::plan_generation_release(
                generations,
                now_ms(),
                retention.min_age_ms,
            );
            let in_guard = fluree_db_indexer::gc::reasoning_indexes::generations_in_guard(&plan);
            if in_guard >= retention.max_generations_in_guard {
                return Ok(RefreshOutcome::Throttled { in_guard });
            }
        }

        let modes = &mut executable.reasoning.modes;
        if program_json.get("reasoningBudget").is_none() {
            modes.max_facts = Some(WORKER_MAX_FACTS);
            modes.max_seconds = Some(WORKER_MAX_SECONDS);
            modes.max_memory_mb = None;
        }

        let derived = match self.complete_closure(&db, &executable.reasoning).await {
            Ok(Ok(derived)) => derived,
            Ok(Err(reason)) => return Ok(RefreshOutcome::Failed { reason }),
            Err(e) => {
                return Ok(RefreshOutcome::Failed {
                    reason: e.to_string(),
                })
            }
        };
        let derived_total = derived.len() as u64;
        let derived_only = asserted_filter(&db, derived, false).await?;
        if program::serves_rewrites(&executable.reasoning) {
            if let Some(p) = derived_hierarchy_edge(&derived_only) {
                return Ok(RefreshOutcome::Failed {
                    reason: format!(
                        "the program derives {p} facts, which query-time rdfs and owl2ql \
                         rewriting does not read; an index would answer differently"
                    ),
                });
            }
        }

        // The previous generation, when this one can build on it.
        let branch_store = fluree_db_nameservice::branched_content_store_for_record(
            self.backend(),
            self.nameservice(),
            &record,
        )
        .await?;
        let index_ns = record
            .storage_namespace()
            .reasoning_index(index_id.as_str());
        let index_cs = self.backend().content_store(&index_ns);
        let index_store: Arc<dyn ContentStore> = Arc::new(BranchedContentStore::with_parents(
            Arc::clone(&index_cs),
            vec![BranchedContentStore::leaf(Arc::clone(&branch_store))],
        ));
        let previous = match &entry.head {
            Some(p) if p.revision == revision && p.settled == settled => {
                match load_previous(index_cs.as_ref(), &p.manifest, &db).await {
                    Ok(prev) => {
                        let line = Line::walk(
                            branch_store.as_ref(),
                            &basis.commit,
                            p.basis.t.min(basis.t),
                        )
                        .await?;
                        line.is_ancestor(&p.basis, &basis).then_some(prev)
                    }
                    Err(e) => {
                        tracing::warn!(index = %index_id, error = %e, "previous generation unreadable; rebuilding");
                        None
                    }
                }
            }
            _ => None,
        };

        let graph_sids = graph_sids(&db);
        let to_flake = |(g_id, mut flake): (GraphId, Flake), op: bool| {
            flake.g = graph_sids.get(&g_id).cloned().flatten();
            flake.t = basis.t;
            flake.op = op;
            flake
        };
        // From here until publication, files are written that no published
        // generation references yet.
        let Some(_write) = self
            .reasoning_index_state
            .begin_write(record.ledger_id.name())
        else {
            return Ok(RefreshOutcome::Deferred {
                reason: "a storage sweep of the ledger is running".into(),
            });
        };
        // A logical fold the indexer cannot apply is redone as a rebuild.
        let mut previous = previous;
        let (kind, built, stats_added, stats_removed) = loop {
            let (kind, fold, stats_added, stats_removed) = match &previous {
                None => {
                    let asserts: Vec<Flake> = derived_only
                        .iter()
                        .cloned()
                        .map(|f| to_flake(f, true))
                        .collect();
                    let n = asserts.len() as u64;
                    (GenerationKind::Rebuild, asserts, n, 0)
                }
                Some(prev) => {
                    let retracted = window_retractions(
                        branch_store.as_ref(),
                        &basis.commit,
                        prev.manifest.basis.t,
                        &db,
                    )
                    .await?;
                    let mut fold = Vec::new();
                    for fact in &derived_only {
                        if !prev.derived.contains(fact) || retracted.contains(fact) {
                            fold.push(to_flake(fact.clone(), true));
                        }
                    }
                    let added = fold.len() as u64;
                    let lost: HashSet<(GraphId, Flake)> = prev
                        .derived
                        .iter()
                        .filter(|f| !derived_only.contains(*f) && !retracted.contains(*f))
                        .cloned()
                        .collect();
                    let still_asserted =
                        asserted_filter(&db, lost.iter().cloned().collect(), true).await?;
                    let mut removed = 0;
                    for fact in lost {
                        if !still_asserted.contains(&fact) {
                            fold.push(to_flake(fact, false));
                            removed += 1;
                        }
                    }
                    (GenerationKind::Logical, fold, added, removed)
                }
            };

            // Build the merged root in the index's namespace.
            let mut index_record = record.clone();
            match &previous {
                None => {
                    if let Some(base_root) = &record.index_head_id {
                        self.copy_index_to_branch(ledger_id, &index_ns, base_root)
                            .await?;
                    }
                }
                Some(prev) => {
                    index_record.index_head_id = Some(prev.manifest.root.clone());
                    index_record.index_t = prev.manifest.basis.t;
                }
            }
            let mut config = crate::build_indexer_config(&self.config);
            config.incremental_retype_max_subjects = usize::MAX;
            config.incremental_max_commits = 1 << 40;
            config.incremental_max_commit_bytes = None;
            config.derived_facts = Some(Arc::new(encode_facts(basis.t, fold)?));
            let built = match &previous {
                None => {
                    fluree_db_indexer::build_index_for_record(
                        Arc::clone(&index_store),
                        &index_record,
                        config,
                    )
                    .await
                }
                Some(_) => {
                    fluree_db_indexer::fold_index_for_record(
                        Arc::clone(&index_store),
                        &index_record,
                        config,
                    )
                    .await
                }
            };
            match built {
                Ok(built) => break (kind, built, stats_added, stats_removed),
                Err(e) if previous.is_some() => {
                    tracing::warn!(
                        index = %index_id,
                        error = %e,
                        "logical fold failed; rebuilding the index from the base"
                    );
                    previous = None;
                }
                Err(e) => return Err(ApiError::internal(format!("reasoning-index build: {e}"))),
            }
        };

        let derived_only_flakes: Vec<Flake> = derived_only
            .iter()
            .cloned()
            .map(|f| to_flake(f, true))
            .collect();
        let derived_count = derived_only_flakes.len() as u64;
        let derived_id = write_derived_only(index_cs.as_ref(), derived_only_flakes).await?;

        let stats = GenerationStats {
            derived_only: derived_count,
            derived_total,
            added: stats_added,
            removed: stats_removed,
        };
        let candidate = Candidate {
            index_id: index_id.clone(),
            kind,
            revision,
            basis: basis.clone(),
        };
        let manifest = ReasoningManifest {
            index_id: index_id.clone(),
            ledger_id: ledger_id.to_string(),
            generation: 0,
            prev_manifest: None,
            published_at_ms: 0,
            kind,
            revision,
            semantics: entry.registration.semantics.clone(),
            settled,
            basis: basis.clone(),
            root: built.root_id,
            derived: derived_id,
            stats: stats.clone(),
        };
        #[cfg(test)]
        {
            let seam = self.reasoning_index_state.before_publish.lock().clone();
            if let Some(seam) = seam {
                seam.reached.notify_one();
                seam.resume.notified().await;
            }
        }
        match self
            .publish_generation(
                ledger_id,
                index_cs.as_ref(),
                branch_store.as_ref(),
                &candidate,
                manifest,
            )
            .await?
        {
            Ok(generation) => {
                self.reasoning_index_state.forget_index(ledger_id, index_id);
                if let Err(e) = self
                    .collect_generations(ledger_id, index_id, index_cs.as_ref())
                    .await
                {
                    tracing::warn!(index = %index_id, error = %e, "reasoning-index collection failed");
                }
                Ok(RefreshOutcome::Published {
                    generation,
                    kind,
                    basis_t: basis.t,
                    stats,
                })
            }
            Err(discard) => Ok(RefreshOutcome::Discarded {
                reason: discard.to_string(),
            }),
        }
    }

    /// Set how long a query with `reasoningFreshness: "strict"` waits for a
    /// reasoning index at its snapshot before it fails. Ten seconds unless set.
    pub fn set_reasoning_index_strict_wait(&self, wait: std::time::Duration) {
        self.reasoning_index_state.set_strict_wait(wait);
    }

    /// Set how long superseded generations are kept, and how many an index
    /// may hold inside that window before publication waits.
    pub fn set_reasoning_index_retention(&self, retention: super::GenerationRetention) {
        self.reasoning_index_state.set_retention(retention);
    }

    /// Release an index's superseded generations once the retention window has
    /// passed since each was superseded. Shared dictionary blobs are left for
    /// the sweep, which sees every participant at once.
    async fn collect_generations(
        &self,
        ledger_id: &str,
        index_id: &ReasoningIndexId,
        index_cs: &dyn ContentStore,
    ) -> Result<()> {
        let doc = self.reasoning_indexes(ledger_id).await?;
        let Some(pointer) = doc.indexes.get(index_id).and_then(|e| e.head.as_ref()) else {
            return Ok(());
        };
        let generations = fluree_db_indexer::gc::reasoning_indexes::index_generations(
            index_cs,
            &pointer.manifest,
        )
        .await
        .map_err(|e| ApiError::internal(format!("read index chain: {e}")))?;
        let plan = fluree_db_indexer::gc::reasoning_indexes::plan_generation_release(
            generations,
            now_ms(),
            self.reasoning_index_state.retention().min_age_ms,
        );
        let released =
            fluree_db_indexer::gc::reasoning_indexes::release_generations(index_cs, &plan, None)
                .await
                .map_err(|e| ApiError::internal(format!("release index generations: {e}")))?;
        if released.generations > 0 {
            tracing::debug!(index = %index_id, ?released, "released superseded index generations");
        }
        Ok(())
    }

    /// The branch record and a view of the branch at exactly its commit
    /// index.
    async fn consistent_base(&self, ledger_id: &str) -> Result<(NsRecord, GraphDb)> {
        for _ in 0..CAS_ATTEMPTS {
            let record = self.branch_record(ledger_id).await?;
            let db = self.db(ledger_id).await?;
            if db.t == record.commit_t {
                return Ok((record, db));
            }
        }
        Err(ApiError::internal(format!(
            "{ledger_id} kept moving while its reasoning indexes were refreshed"
        )))
    }

    /// The program's complete output at `db`, by graph; `Ok(Err(reason))`
    /// when the closure was capped.
    async fn complete_closure(
        &self,
        db: &GraphDb,
        reasoning: &fluree_db_query::ir::ReasoningConfig,
    ) -> Result<std::result::Result<Vec<(GraphId, Flake)>, String>> {
        let db_ref = db.as_graph_db_ref();
        let schema = reasoning
            .schema_bundle
            .as_ref()
            .filter(|bundle| !bundle.is_empty())
            .map(|bundle| SchemaBundleOverlay::new(db_ref.overlay, bundle.clone()));
        let overlay: &dyn OverlayProvider = schema
            .as_ref()
            .map_or(db_ref.overlay, |o| o as &dyn OverlayProvider);
        let base = fluree_db_core::GraphDbRef::new(db_ref.snapshot, db_ref.g_id, overlay, db_ref.t);
        let outcome = compute_derived_facts(
            base,
            &reasoning.modes,
            reasoning.rules_source_g_id,
            db.binary_store.as_ref(),
            QueryRequest {
                tracker: None,
                cancellation: None,
            },
        )
        .await?;
        if let Some(reason) = outcome.failed {
            return Ok(Err(reason));
        }
        if let Some(diag) = outcome.diagnostics.as_ref().filter(|d| d.capped) {
            return Ok(Err(format!(
                "the closure was capped ({}); raise the program's reasoningBudget",
                diag.capped_reason.as_deref().unwrap_or("budget")
            )));
        }
        let mut out = Vec::new();
        if let Some(overlay) = &outcome.overlay {
            let graphs: Vec<GraphId> = overlay.graph_ids().collect();
            for g_id in graphs {
                overlay.for_each_overlay_flake(
                    g_id,
                    IndexType::Spot,
                    None,
                    None,
                    true,
                    i64::MAX,
                    &mut |flake| {
                        if flake.op {
                            out.push((g_id, flake.clone()));
                        }
                    },
                );
            }
        }
        Ok(Ok(out))
    }

    /// Publish `manifest` as `candidate`'s generation: admit it against the
    /// reasoning-indexes document as it stands, write the manifest with its place in the
    /// chain, and compare-and-swap, retrying from the actual document.
    async fn publish_generation(
        &self,
        ledger_id: &str,
        index_cs: &dyn ContentStore,
        branch_store: &dyn ContentStore,
        candidate: &Candidate,
        mut manifest: ReasoningManifest,
    ) -> Result<std::result::Result<u64, Discard>> {
        for _ in 0..CAS_ATTEMPTS {
            let record = self.branch_record(ledger_id).await?;
            let actual = self.reasoning_indexes(ledger_id).await?;
            let published_t = actual
                .indexes
                .get(&candidate.index_id)
                .and_then(|e| e.head.as_ref())
                .map(|p| p.basis.t)
                .unwrap_or(candidate.basis.t);
            let line = match &record.commit_head_id {
                Some(index) => {
                    Line::walk(branch_store, index, published_t.min(candidate.basis.t)).await?
                }
                None => Line::default(),
            };
            let admitted = match admit(&actual, candidate, &line) {
                Ok(admitted) => admitted,
                Err(discard) => return Ok(Err(discard)),
            };
            manifest.generation = admitted.generation;
            manifest.prev_manifest = admitted.prev_manifest;
            manifest.published_at_ms = now_ms();
            let bytes = manifest.encode();
            let manifest_id = index_cs
                .put(ContentKind::ReasoningManifest, &bytes)
                .await
                .map_err(|e| ApiError::internal(format!("write manifest: {e}")))?;
            let mut next = actual.clone();
            next.v = actual.v + 1;
            next.indexes
                .get_mut(&candidate.index_id)
                .expect("admitted, so present")
                .head = Some(manifest.pointer(manifest_id));
            match self
                .publisher()?
                .push_reasoning_indexes_fenced(ledger_id, record.fence, actual.v, &next)
                .await?
            {
                ReasoningIndexesCasResult::Updated => return Ok(Ok(manifest.generation)),
                ReasoningIndexesCasResult::Conflict { .. } => continue,
            }
        }
        Err(ApiError::internal(format!(
            "the reasoning-indexes document of {ledger_id} kept changing; gave up publishing"
        )))
    }
}

/// A previous generation, as the next one builds on it.
struct Previous {
    manifest: ReasoningManifest,
    derived: HashSet<(GraphId, Flake)>,
}

async fn load_previous(
    store: &dyn ContentStore,
    manifest_id: &ContentId,
    db: &GraphDb,
) -> Result<Previous> {
    let bytes = store
        .get(manifest_id)
        .await
        .map_err(|e| ApiError::internal(format!("read manifest {manifest_id}: {e}")))?;
    let manifest = ReasoningManifest::decode(&bytes).map_err(ApiError::internal)?;
    if !store
        .has(&manifest.root)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?
    {
        return Err(ApiError::internal(format!(
            "the root {} of generation {} is gone",
            manifest.root, manifest.generation
        )));
    }
    let derived = read_derived_only(store, &manifest.derived)
        .await?
        .into_iter()
        .map(|flake| (graph_of(db, flake.g.as_ref()), flake))
        .collect();
    Ok(Previous { manifest, derived })
}

/// The id of a graph whose IRI the branch no longer registers: no fact of
/// the current state can be in it.
const UNREGISTERED_GRAPH: GraphId = GraphId::MAX;

/// Each graph's Sid in flake form: `None` for the default graph.
/// The hierarchy predicate a derived-only fact of the default graph has,
/// if one does. Query-time `rdfs` rewriting reads the class and property
/// hierarchy without derived facts; against an index it would read them.
fn derived_hierarchy_edge(facts: &HashSet<(GraphId, Flake)>) -> Option<&'static str> {
    use fluree_vocab::namespaces::RDFS;
    let sub_class = Sid::new(RDFS, "subClassOf");
    let sub_property = Sid::new(RDFS, "subPropertyOf");
    facts
        .iter()
        .filter(|(g_id, _)| *g_id == 0)
        .find_map(|(_, flake)| {
            if flake.p == sub_class {
                Some("rdfs:subClassOf")
            } else if flake.p == sub_property {
                Some("rdfs:subPropertyOf")
            } else {
                None
            }
        })
}

fn graph_sids(db: &GraphDb) -> HashMap<GraphId, Option<Sid>> {
    let mut out = HashMap::new();
    out.insert(0, None);
    for (g_id, iri) in db.snapshot.graph_registry.iter_entries() {
        if g_id != 0 {
            out.insert(g_id, db.snapshot.encode_iri(iri));
        }
    }
    out
}

/// The facts of `facts` the branch asserts at `db`'s `t` (`keep_asserted`),
/// or the ones it does not.
async fn asserted_filter(
    db: &GraphDb,
    facts: Vec<(GraphId, Flake)>,
    keep_asserted: bool,
) -> Result<HashSet<(GraphId, Flake)>> {
    let mut by_predicate: BTreeMap<(GraphId, Sid), Vec<Flake>> = BTreeMap::new();
    for (g_id, flake) in facts {
        by_predicate
            .entry((g_id, flake.p.clone()))
            .or_default()
            .push(flake);
    }
    let mut out = HashSet::new();
    for ((g_id, p), flakes) in by_predicate {
        let asserted: HashSet<Flake> = fluree_db_core::range::range_with_overlay(
            &db.snapshot,
            g_id,
            db.overlay.as_ref(),
            IndexType::Psot,
            fluree_db_core::query_bounds::RangeTest::Eq,
            fluree_db_core::query_bounds::RangeMatch::predicate(p),
            fluree_db_core::query_bounds::RangeOptions {
                to_t: Some(db.t),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| ApiError::internal(format!("asserted-fact scan: {e}")))?
        .into_iter()
        .filter(|f| f.op)
        .collect();
        for flake in flakes {
            if asserted.contains(&flake) == keep_asserted {
                out.insert((g_id, flake));
            }
        }
    }
    Ok(out)
}

/// Facts a commit in `(after_t, head]` retracted, by graph.
async fn window_retractions(
    store: &dyn ContentStore,
    head: &ContentId,
    after_t: i64,
    db: &GraphDb,
) -> Result<HashSet<(GraphId, Flake)>> {
    let commits = fluree_db_core::collect_first_parent_cids(store, head, after_t)
        .await
        .map_err(|e| ApiError::internal(format!("walk commits: {e}")))?;
    let mut out = HashSet::new();
    for (_, cid) in commits {
        let bytes = store
            .get(&cid)
            .await
            .map_err(|e| ApiError::internal(format!("read commit {cid}: {e}")))?;
        let commit = fluree_db_core::commit::codec::read_commit(&bytes)
            .map_err(|e| ApiError::internal(format!("decode commit {cid}: {e}")))?;
        for flake in commit.flakes.into_iter().filter(|f| !f.op) {
            out.insert((graph_of(db, flake.g.as_ref()), flake));
        }
    }
    Ok(out)
}

fn graph_of(db: &GraphDb, g: Option<&Sid>) -> GraphId {
    match g {
        None => 0,
        Some(sid) => db
            .snapshot
            .decode_sid(sid)
            .and_then(|iri| db.snapshot.graph_registry.assigned_graph_id(&iri))
            .unwrap_or(UNREGISTERED_GRAPH),
    }
}

/// Commits on the branch's first-parent line from its head back to a `t`.
#[derive(Default)]
pub(super) struct Line {
    commits: HashMap<ContentId, i64>,
    floor_t: i64,
}

impl Line {
    pub(super) async fn walk(
        store: &dyn ContentStore,
        head: &ContentId,
        down_to_t: i64,
    ) -> Result<Self> {
        let stop = (down_to_t - 1).max(0);
        let commits = fluree_db_core::collect_first_parent_cids(store, head, stop)
            .await
            .map_err(|e| ApiError::internal(format!("walk commits: {e}")))?;
        Ok(Self {
            commits: commits.into_iter().map(|(t, cid)| (cid, t)).collect(),
            floor_t: down_to_t,
        })
    }

    pub(super) fn on_line(&self, basis: &Basis) -> bool {
        basis.t >= self.floor_t && self.commits.get(&basis.commit) == Some(&basis.t)
    }
}

impl BasisOracle for Line {
    fn is_live(&self, basis: &Basis) -> bool {
        self.on_line(basis)
    }

    fn is_ancestor(&self, ancestor: &Basis, descendant: &Basis) -> bool {
        self.on_line(ancestor) && self.on_line(descendant) && ancestor.t <= descendant.t
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;

    use super::RefreshOutcome;
    use crate::reasoning_index::PauseSeam;

    /// A sweep that starts while an index build has written its files and not
    /// yet published them waits for the build, so what it publishes is
    /// there to read.
    #[tokio::test]
    async fn a_sweep_waits_for_an_index_build_to_publish() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/sweep-race:main";
        let ledger = fluree.create_ledger(id).await.unwrap();
        let ctx = json!({
            "ex": "http://example.org/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        });
        let ledger = fluree
            .insert(
                ledger,
                &json!({
                    "@context": ctx,
                    "@graph": [
                        {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                        {"@id": "ex:alice", "@type": "ex:Student"}
                    ]
                }),
            )
            .await
            .unwrap()
            .ledger;
        fluree
            .reindex(id, crate::ReindexOptions::default())
            .await
            .unwrap();
        let index = fluree
            .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
            .await
            .unwrap();
        // The sweep reads an index's files once it has a published generation;
        // the next one's are the files at risk.
        fluree.refresh_reasoning_index(id, &index).await.unwrap();
        fluree
            .insert(
                ledger,
                &json!({"@context": ctx, "@id": "ex:bob", "@type": "ex:Student"}),
            )
            .await
            .unwrap();

        let seam = Arc::new(PauseSeam::default());
        *fluree.reasoning_index_state.before_publish.lock() = Some(Arc::clone(&seam));
        let refresh = fluree.refresh_reasoning_index(id, &index);
        let sweep = async {
            seam.reached.notified().await;
            let sweep = fluree.sweep_index_storage("reasoning-indexes/sweep-race");
            tokio::pin!(sweep);
            let early = tokio::time::timeout(Duration::from_millis(300), &mut sweep).await;
            seam.resume.notify_one();
            match early {
                Ok(done) => done,
                Err(_) => sweep.await,
            }
        };
        let (outcome, swept) = tokio::join!(refresh, sweep);
        assert!(
            matches!(outcome.unwrap(), RefreshOutcome::Published { .. }),
            "the build publishes"
        );
        swept.unwrap();

        let resp = fluree
            .graph(id)
            .query()
            .jsonld(&json!({
                "@context": ctx,
                "select": "?p",
                "where": {"@id": "?p", "@type": "ex:Person"},
                "reasoning": "owl2rl"
            }))
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        assert!(
            resp.reasoning.and_then(|r| r.index).is_some(),
            "the published generation is readable after the sweep"
        );
    }
}
