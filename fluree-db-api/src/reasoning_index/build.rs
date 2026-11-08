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
    admit, Basis, BasisOracle, Candidate, ClosureMode, Discard, ErasureEpoch, GenerationKind,
    GenerationStats, ReasoningIndexEntry, ReasoningIndexId, ReasoningIndexes, ReasoningManifest,
    Registration, RegistrationState,
};
use fluree_db_core::{
    BranchedContentStore, ContentId, ContentKind, ContentStore, Flake, GraphId, IndexType,
    LedgerId, OverlayProvider, Sid,
};
use fluree_db_nameservice::{NsRecord, ReasoningIndexesCasResult};
use fluree_db_query::execute::{
    compute_derived_facts, compute_derived_facts_incremental, QueryRequest,
};
use fluree_db_query::schema_bundle::SchemaBundleOverlay;
use serde_json::Value as JsonValue;

use super::derived::{encode_facts, read_derived_only, update_derived_only, write_derived_only};
use super::program;
use crate::query::helpers::parse_jsonld_query;
use crate::view::GraphDb;
use crate::{ApiError, Fluree, Result};

const CAS_ATTEMPTS: usize = 16;

/// Active indexes a branch may register. Each is a complete index of the
/// branch, refreshed after every commit.
pub const MAX_ACTIVE_INDEXES: usize = 8;

/// What one entry can grow by after its registration: a head published and
/// the entry retired. Registration reserves it, so a store's size limit
/// refuses a registration rather than a later publication or retirement.
pub const ENTRY_GROWTH_BYTES: usize = 1_024;

/// The bytes `doc` takes once every entry has grown by [`ENTRY_GROWTH_BYTES`].
fn grown_bytes(doc: &ReasoningIndexes) -> usize {
    serde_json::to_vec(doc).map_or(usize::MAX, |json| json.len())
        + doc.indexes.len() * ENTRY_GROWTH_BYTES
}

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
    /// The index is published under newer engine semantics than this
    /// instance runs, so only an instance at least as new refreshes it. After
    /// a downgrade, retire the index and register the program again.
    #[serde(rename_all = "camelCase")]
    NewerEngine { published: u32, engine: u32 },
}

/// Wall-clock milliseconds a refresh spends in each phase, for the debug
/// log.
struct Phases {
    last: fluree_db_core::clock::Instant,
    spent: Vec<(&'static str, u64)>,
}

impl Phases {
    fn new() -> Self {
        Self {
            last: fluree_db_core::clock::Instant::now(),
            spent: Vec::new(),
        }
    }

    fn mark(&mut self, phase: &'static str) {
        let now = fluree_db_core::clock::Instant::now();
        self.spent
            .push((phase, now.duration_since(self.last).as_millis() as u64));
        self.last = now;
    }
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
        let max_bytes = self.publisher()?.max_reasoning_indexes_bytes();
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
            if let Some(max) = max_bytes {
                let bytes = grown_bytes(&next);
                if bytes > max {
                    return Err(ApiError::NameService(
                        fluree_db_nameservice::NameServiceError::TooLarge(format!(
                            "{id}'s reasoning indexes would take {bytes} bytes once published, \
                             more than the {max} this nameservice holds; retire and clean up an \
                             index, or register a smaller program"
                        )),
                    ));
                }
            }
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
        // A build already writing finishes first, and none starts writing
        // until the folders are gone.
        let _hold = self
            .reasoning_index_state
            .hold_for_reclaim(record.ledger_id.name())
            .await;
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
        let mut phases = Phases::new();
        let (record, db) = self.consistent_base(ledger_id).await?;
        phases.mark("base");
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
        phases.mark("settle");
        if !program::materializes(&executable.reasoning) {
            return Ok(RefreshOutcome::Failed {
                reason: "the program, as the branch settles it, materializes nothing".into(),
            });
        }
        let engine = self.reasoning_index_state.engine_semantics();
        let settled = program::settled_key(&executable.reasoning, db.graph_id, engine);
        if let Some(published) = &entry.head {
            // Not even retention: an older collector would act on files a
            // newer engine wrote.
            if published.revision == revision && published.engine > engine {
                return Ok(RefreshOutcome::NewerEngine {
                    published: published.engine,
                    engine,
                });
            }
            if published.revision == revision
                && published.basis == basis
                && published.settled == settled
            {
                // Retention runs whether or not anything is published: an
                // idle branch's superseded generations still expire.
                self.collect_quietly(ledger_id, &record, index_id).await;
                return Ok(RefreshOutcome::Current {
                    generation: published.generation,
                });
            }
            if published.engine < engine {
                tracing::info!(
                    ledger = ledger_id,
                    index = %index_id,
                    published = published.engine,
                    engine,
                    "reasoning index computed under other engine semantics; rebuilding"
                );
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
                self.collect_quietly(ledger_id, &record, index_id).await;
                return Ok(RefreshOutcome::Throttled { in_guard });
            }
        }

        let modes = &mut executable.reasoning.modes;
        if program_json.get("reasoningBudget").is_none() {
            modes.max_facts = Some(WORKER_MAX_FACTS);
            modes.max_seconds = Some(WORKER_MAX_SECONDS);
            modes.max_memory_mb = None;
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
                match load_previous(index_cs.as_ref(), &p.manifest).await {
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
        phases.mark("previous");

        // The program's output: maintained from the previous generation when
        // the window since it only inserted facts the program can fold in,
        // otherwise computed in full.
        let window = match &previous {
            Some(prev) => Some(
                window_changes(
                    branch_store.as_ref(),
                    &basis.commit,
                    prev.manifest.basis.t,
                    &db,
                )
                .await?,
            ),
            None => None,
        };
        phases.mark("window");
        let incremental = match (&previous, &window) {
            (Some(prev), Some(window)) => {
                self.incremental_closure(
                    &record,
                    &index_store,
                    prev,
                    window,
                    &db,
                    &executable.reasoning,
                )
                .await?
            }
            _ => None,
        };
        let closure = if incremental.is_some() {
            ClosureMode::Incremental
        } else {
            ClosureMode::Full
        };
        let derived = match incremental {
            Some(gained) => gained.map(|r| r.map(Output::Gained)),
            None => self
                .complete_closure(&db, &executable.reasoning)
                .await
                .map(|r| r.map(|c| Output::Whole(c.into_iter().collect()))),
        };
        let mut output = match derived {
            Ok(Ok(output)) => output,
            Ok(Err(reason)) => return Ok(RefreshOutcome::Failed { reason }),
            Err(e) => {
                return Ok(RefreshOutcome::Failed {
                    reason: e.to_string(),
                })
            }
        };
        phases.mark("closure");
        #[cfg(test)]
        if let Some(extra) = self
            .reasoning_index_state
            .other_engine
            .lock()
            .as_ref()
            .and_then(|other| other.extra.clone())
        {
            match &mut output {
                Output::Whole(c) => {
                    c.insert(extra);
                }
                Output::Gained(gained) => gained.push(extra),
            }
        }
        let derived_total = match &output {
            Output::Whole(c) => c.len() as u64,
            Output::Gained(gained) => gained.len() as u64,
        };
        // A full closure holds facts also asserted; D is what is not.
        if let Output::Whole(c) = output {
            output = Output::Whole(asserted_filter(&db, c.into_iter().collect(), false).await?);
        }
        phases.mark("asserted_filter");
        if let Output::Whole(derived_only) = &output {
            if program::serves_rewrites(&executable.reasoning) {
                if let Some(p) = derived_hierarchy_edge(derived_only) {
                    return Ok(RefreshOutcome::Failed {
                        reason: format!(
                            "the program derives {p} facts, which query-time rdfs and owl2ql \
                             rewriting does not read; an index would answer differently"
                        ),
                    });
                }
            }
        }
        // The facts the window asserted: an incremental generation takes any
        // of them D holds out of it, matching by graph.
        let asserted_in_window: Vec<(GraphId, Flake)> = window
            .as_ref()
            .map(|w| w.inserted.clone())
            .unwrap_or_default();

        let graph_sids = graph_sids(&db);
        let to_flake = |(g_id, mut flake): (GraphId, Flake), op: bool| {
            flake.g = graph_sids.get(&g_id).cloned().flatten();
            flake.t = basis.t;
            flake.op = op;
            flake
        };
        #[cfg(test)]
        {
            let seam = self.reasoning_index_state.before_write.lock().clone();
            if let Some(seam) = seam {
                seam.reached.notify_one();
                seam.resume.notified().await;
            }
        }
        // From here until publication, files are written that no published
        // generation references yet.
        // Sweeps and collector passes hold these off, or wait for them, so
        // nothing those files reference is reclaimed before they are
        // published.
        let Some(_write) = self
            .reasoning_index_state
            .begin_write(record.ledger_id.name())
        else {
            return Ok(RefreshOutcome::Deferred {
                reason: "a sweep or collector pass of the ledger is running".into(),
            });
        };
        // Retired, or cleaned up, while the closure ran: its folder is not
        // written again once a cleanup could have deleted it.
        match self
            .reasoning_indexes(ledger_id)
            .await?
            .indexes
            .get(index_id)
        {
            Some(entry)
                if matches!(entry.registration.state, RegistrationState::Active)
                    && entry.registration.revision == revision => {}
            _ => {
                return Ok(RefreshOutcome::Discarded {
                    reason: "not eligible: the index was retired while it was computed".into(),
                })
            }
        }
        let _build = match &self.indexing_mode {
            crate::IndexingMode::Background(handle) => {
                match handle.begin_build(&record.ledger_id) {
                    Some(ticket) => Some(ticket),
                    None => {
                        return Ok(RefreshOutcome::Deferred {
                            reason: "a collector pass of the ledger is releasing files".into(),
                        })
                    }
                }
            }
            crate::IndexingMode::Disabled => None,
        };
        // A rebuild copies the branch's index. The one read with the basis
        // may have been superseded, and released, before the hold above was
        // taken; the current one serves if it does not go past the basis.
        let base_index = {
            let current = self.branch_record(ledger_id).await?;
            if current.index_head_id == record.index_head_id {
                (record.index_head_id.clone(), record.index_t)
            } else if current.index_t <= basis.t {
                (current.index_head_id, current.index_t)
            } else {
                return Ok(RefreshOutcome::Deferred {
                    reason: "the branch's index moved past the commit computed".into(),
                });
            }
        };
        // A logical fold the indexer cannot apply is redone as a rebuild. A
        // fold over a drop would have to reassert what the program still
        // derives into the dropped graphs; a rebuild needs nothing of it.
        let no_retractions = HashSet::new();
        let mut logical = previous.is_some() && !window.as_ref().is_some_and(|w| w.drops);
        let (kind, built, stats_added, stats_removed) = loop {
            let (kind, fold, stats_added, stats_removed) = match (&previous, logical, &output) {
                (Some(prev), false, Output::Gained(gained)) => {
                    // A rebuild needs D whole.
                    let mut whole = previous_derived(index_cs.as_ref(), prev, &db).await?;
                    for fact in &asserted_in_window {
                        whole.remove(fact);
                    }
                    whole.extend(gained.iter().cloned());
                    output = Output::Whole(whole);
                    continue;
                }
                (_, false, Output::Whole(derived_only)) => {
                    let asserts: Vec<Flake> = derived_only
                        .iter()
                        .cloned()
                        .map(|f| to_flake(f, true))
                        .collect();
                    let n = asserts.len() as u64;
                    (GenerationKind::Rebuild, asserts, n, 0)
                }
                (_, true, Output::Gained(gained)) => {
                    // Insert-only: D gains these, and nothing derived is lost.
                    let fold: Vec<Flake> =
                        gained.iter().cloned().map(|f| to_flake(f, true)).collect();
                    let n = fold.len() as u64;
                    (GenerationKind::Logical, fold, n, 0)
                }
                (Some(prev), true, Output::Whole(derived_only)) => {
                    let prev_derived = previous_derived(index_cs.as_ref(), prev, &db).await?;
                    let retracted = window.as_ref().map_or(&no_retractions, |w| &w.retracted);
                    let mut fold = Vec::new();
                    for fact in derived_only {
                        if !prev_derived.contains(fact) || retracted.contains(fact) {
                            fold.push(to_flake(fact.clone(), true));
                        }
                    }
                    let added = fold.len() as u64;
                    let lost: HashSet<(GraphId, Flake)> = prev_derived
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
                (None, _, Output::Gained(_)) | (None, true, _) => {
                    unreachable!("an incremental closure or a logical fold has a predecessor")
                }
            };

            phases.mark("fold_diff");
            // Build the merged root in the index's namespace.
            let mut index_record = record.clone();
            match (&previous, logical) {
                (Some(prev), true) => {
                    index_record.index_head_id = Some(prev.manifest.root.clone());
                    index_record.index_t = prev.manifest.basis.t;
                }
                _ => {
                    index_record.index_head_id = base_index.0.clone();
                    index_record.index_t = base_index.1;
                    if let Some(base_root) = &base_index.0 {
                        self.copy_index_to_branch(ledger_id, &index_ns, base_root)
                            .await?;
                    }
                }
            }
            let mut config = crate::build_indexer_config(&self.config);
            config.incremental_retype_max_subjects = usize::MAX;
            config.incremental_max_commits = 1 << 40;
            config.incremental_max_commit_bytes = None;
            config.derived_facts = Some(Arc::new(encode_facts(basis.t, fold)?));
            let built = if logical {
                fluree_db_indexer::fold_index_for_record(
                    Arc::clone(&index_store),
                    &index_record,
                    config,
                )
                .await
            } else {
                fluree_db_indexer::build_index_for_record(
                    Arc::clone(&index_store),
                    &index_record,
                    config,
                )
                .await
            };
            match built {
                Ok(built) => break (kind, built, stats_added, stats_removed),
                Err(e) if logical => {
                    tracing::warn!(
                        index = %index_id,
                        error = %e,
                        "logical fold failed; rebuilding the index from the base"
                    );
                    logical = false;
                }
                Err(e) => return Err(ApiError::internal(format!("reasoning-index build: {e}"))),
            }
        };
        phases.mark("index_build");

        let (derived_id, derived_count) = match (&previous, &output) {
            (Some(prev), Output::Gained(gained)) => {
                update_derived_only(
                    index_cs.as_ref(),
                    &prev.manifest.derived,
                    asserted_in_window
                        .iter()
                        .cloned()
                        .map(|f| to_flake(f, true))
                        .collect(),
                    gained.iter().cloned().map(|f| to_flake(f, true)).collect(),
                )
                .await?
            }
            (_, Output::Whole(derived_only)) => {
                let flakes: Vec<Flake> = derived_only
                    .iter()
                    .cloned()
                    .map(|f| to_flake(f, true))
                    .collect();
                let count = flakes.len() as u64;
                (write_derived_only(index_cs.as_ref(), flakes).await?, count)
            }
            (None, Output::Gained(_)) => unreachable!("an incremental closure has a predecessor"),
        };
        phases.mark("derived_write");

        let stats = GenerationStats {
            derived_only: derived_count,
            derived_total: match closure {
                ClosureMode::Full => derived_total,
                ClosureMode::Incremental => derived_count,
            },
            added: stats_added,
            removed: stats_removed,
            closure,
        };
        let candidate = Candidate {
            index_id: index_id.clone(),
            kind,
            revision,
            basis: basis.clone(),
            engine,
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
            engine,
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
                phases.mark("publish");
                self.reasoning_index_state.forget_index(ledger_id, index_id);
                self.collect_quietly(ledger_id, &record, index_id).await;
                phases.mark("collect");
                tracing::debug!(
                    index = %index_id,
                    generation,
                    ?kind,
                    ?closure,
                    phases_ms = ?phases.spent,
                    "reasoning-index refresh phases"
                );
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

    /// [`Self::collect_generations`], logging rather than failing: a refresh
    /// stands whether or not retention could run.
    async fn collect_quietly(
        &self,
        ledger_id: &str,
        record: &NsRecord,
        index_id: &ReasoningIndexId,
    ) {
        let index_cs = self.backend().content_store(
            &record
                .storage_namespace()
                .reasoning_index(index_id.as_str()),
        );
        if let Err(e) = self
            .collect_generations(ledger_id, index_id, index_cs.as_ref())
            .await
        {
            tracing::warn!(index = %index_id, error = %e, "reasoning-index collection failed");
        }
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
        Ok(outcome_facts(outcome))
    }

    /// What the program's output at the branch head gains over `prev`'s from
    /// the window's insertions. `None` when the program or the window rules
    /// that out, and the output is computed in full instead:
    ///
    /// - the program is not OWL 2 RL alone, or uses the identity rules or
    ///   `owl:sameAs`, whose state a delta alone cannot rebuild (P11b);
    /// - a window commit retracted anything (P11b);
    /// - a window commit inserted vocabulary a program reads as definition
    ///   rather than data: RDFS or OWL terms, or RDF lists.
    async fn incremental_closure(
        &self,
        record: &NsRecord,
        index_store: &Arc<dyn ContentStore>,
        prev: &Previous,
        window: &Window,
        db: &GraphDb,
        reasoning: &fluree_db_query::ir::ReasoningConfig,
    ) -> Result<Option<Result<std::result::Result<Vec<(GraphId, Flake)>, String>>>> {
        let modes = &reasoning.modes;
        if !modes.owl2rl || modes.datalog || modes.owl_datalog || modes.has_rules() {
            return Ok(None);
        }
        if window.drops
            || !window.retracted.is_empty()
            || window.inserted.iter().any(|(_, f)| defines_program(f))
        {
            return Ok(None);
        }
        let inserted: Vec<Flake> = window
            .inserted
            .iter()
            .filter(|(g_id, _)| *g_id == db.graph_id)
            .map(|(_, flake)| flake.clone())
            .collect();
        // The closed state: the predecessor's index, with every commit since
        // its basis replayed over it.
        let started = fluree_db_core::clock::Instant::now();
        let state = self
            .load_index_state(
                Arc::clone(index_store),
                record,
                &prev.manifest.root,
                prev.manifest.basis.t,
            )
            .await?;
        let mut closed = GraphDb::from_ledger_state(&state);
        closed.graph_id = db.graph_id;
        let closed_ref = closed.as_graph_db_ref();
        let schema = reasoning
            .schema_bundle
            .as_ref()
            .filter(|bundle| !bundle.is_empty())
            .map(|bundle| SchemaBundleOverlay::new(closed_ref.overlay, bundle.clone()));
        let overlay: &dyn OverlayProvider = schema
            .as_ref()
            .map_or(closed_ref.overlay, |o| o as &dyn OverlayProvider);
        let base = fluree_db_core::GraphDbRef::new(
            closed_ref.snapshot,
            closed_ref.g_id,
            overlay,
            closed_ref.t,
        );
        let loaded_ms = started.elapsed().as_millis() as u64;
        let outcome = match compute_derived_facts_incremental(
            base,
            modes,
            &inserted,
            self.reasoning_index_state.maintenance(),
            QueryRequest {
                tracker: None,
                cancellation: None,
            },
        )
        .await
        {
            Ok(Some(outcome)) => outcome,
            Ok(None) => return Ok(None),
            Err(e) => return Ok(Some(Err(e.into()))),
        };
        let computed_ms = started.elapsed().as_millis() as u64;
        let facts = outcome_facts(outcome);
        tracing::debug!(
            inserted = inserted.len(),
            state_load_ms = loaded_ms,
            fixpoint_ms = computed_ms - loaded_ms,
            collect_ms = started.elapsed().as_millis() as u64 - computed_ms,
            "incremental closure"
        );
        Ok(Some(Ok(facts)))
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
}

/// The program's output at the basis.
enum Output {
    /// The derived-only set D, whole.
    Whole(HashSet<(GraphId, Flake)>),
    /// What D gains over the predecessor's, from an incremental closure over
    /// an insert-only window. D also loses the facts the window asserted:
    /// their assertion keeps them in the index, so they are not retracted.
    Gained(Vec<(GraphId, Flake)>),
}

/// The predecessor's derived-only set, by graph.
async fn previous_derived(
    store: &dyn ContentStore,
    prev: &Previous,
    db: &GraphDb,
) -> Result<HashSet<(GraphId, Flake)>> {
    Ok(read_derived_only(store, &prev.manifest.derived)
        .await?
        .into_iter()
        .map(|flake| (graph_of(db, flake.g.as_ref()), flake))
        .collect())
}

async fn load_previous(store: &dyn ContentStore, manifest_id: &ContentId) -> Result<Previous> {
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
    Ok(Previous { manifest })
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

/// What the commits in `(after_t, head]` asserted and retracted, by graph,
/// and whether any of them dropped a graph.
struct Window {
    inserted: Vec<(GraphId, Flake)>,
    retracted: HashSet<(GraphId, Flake)>,
    /// A drop takes a graph's facts out with no retraction to show for them.
    drops: bool,
}

async fn window_changes(
    store: &dyn ContentStore,
    head: &ContentId,
    after_t: i64,
    db: &GraphDb,
) -> Result<Window> {
    let commits = fluree_db_core::collect_first_parent_cids(store, head, after_t)
        .await
        .map_err(|e| ApiError::internal(format!("walk commits: {e}")))?;
    let mut window = Window {
        inserted: Vec::new(),
        retracted: HashSet::new(),
        drops: false,
    };
    for (_, cid) in commits {
        let bytes = store
            .get(&cid)
            .await
            .map_err(|e| ApiError::internal(format!("read commit {cid}: {e}")))?;
        let commit = fluree_db_core::commit::codec::read_commit(&bytes)
            .map_err(|e| ApiError::internal(format!("decode commit {cid}: {e}")))?;
        window.drops |= !commit.graph_drops.is_empty();
        for flake in commit.flakes {
            let fact = (graph_of(db, flake.g.as_ref()), flake);
            if fact.1.op {
                window.inserted.push(fact);
            } else {
                window.retracted.insert(fact);
            }
        }
    }
    Ok(window)
}

/// Whether an asserted fact can change what an OWL 2 RL program is, rather
/// than only what it reads: an RDFS or OWL term as its predicate or its
/// type, or an RDF list cell, which axioms are built from.
fn defines_program(flake: &Flake) -> bool {
    use fluree_vocab::namespaces::{OWL, RDF, RDFS};
    let p = &flake.p;
    if p.namespace_code == RDFS || p.namespace_code == OWL {
        return true;
    }
    if p.namespace_code == RDF {
        if matches!(p.name.as_ref(), "first" | "rest") {
            return true;
        }
        if p.name.as_ref() == "type" {
            if let fluree_db_core::FlakeValue::Ref(class) = &flake.o {
                return class.namespace_code == RDFS || class.namespace_code == OWL;
            }
        }
    }
    false
}

/// The facts a derived-facts outcome holds, by graph, or why it does not
/// hold the program's whole output.
fn outcome_facts(
    outcome: fluree_db_query::execute::DerivedFactsOutcome,
) -> std::result::Result<Vec<(GraphId, Flake)>, String> {
    if let Some(reason) = outcome.failed {
        return Err(reason);
    }
    if let Some(diag) = outcome.diagnostics.as_ref().filter(|d| d.capped) {
        return Err(format!(
            "the closure was capped ({}); raise the program's reasoningBudget",
            diag.capped_reason.as_deref().unwrap_or("budget")
        ));
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

    /// A head published and the entry retired, at their largest, fit the
    /// reserve a registration keeps for them.
    #[test]
    fn an_entry_grows_within_its_reserve() {
        use fluree_db_core::reasoning_index::{
            Basis, ClockPolicy, ErasureEpoch, HeadPointer, ReasoningIndexEntry, ReasoningIndexId,
            ReasoningIndexes, Registration, RegistrationState, SemanticKey,
        };
        use fluree_db_core::{ContentId, ContentKind};
        let cid = |kind, s: &str| ContentId::new(kind, s.as_bytes());
        let registered = ReasoningIndexEntry {
            registration: Registration {
                revision: 1,
                semantics: SemanticKey {
                    program: cid(ContentKind::LedgerConfig, "program"),
                    reasoned_graph: None,
                    clock: ClockPolicy::SnapshotCommitTime,
                },
                program: json!({"reasoning": "owl2rl"}),
                state: RegistrationState::Active,
            },
            floor: ErasureEpoch(0),
            head: None,
        };
        let mut grown = registered.clone();
        grown.registration.revision = u64::MAX;
        grown.registration.state = RegistrationState::Retired { at_ms: i64::MIN };
        grown.floor = ErasureEpoch(u64::MAX);
        grown.head = Some(HeadPointer {
            manifest: cid(ContentKind::ReasoningManifest, "manifest"),
            generation: u64::MAX,
            revision: u64::MAX,
            basis: Basis {
                commit: cid(ContentKind::Commit, "commit"),
                t: i64::MIN,
                epoch: ErasureEpoch(u64::MAX),
            },
            settled: cid(ContentKind::LedgerConfig, "settled"),
            engine: u32::MAX,
        });
        let id = ReasoningIndexId::parse("01JC0000000000000000000000").unwrap();
        let bytes = |entry: ReasoningIndexEntry, v| {
            serde_json::to_vec(&ReasoningIndexes {
                v,
                indexes: [(id.clone(), entry)].into(),
            })
            .unwrap()
            .len()
        };
        let growth = bytes(grown, i64::MAX) - bytes(registered, 1);
        assert!(growth <= super::ENTRY_GROWTH_BYTES, "{growth} bytes");
    }
    use crate::reasoning_index::{OtherEngine, PauseSeam};
    use crate::Fluree;
    use fluree_db_core::reasoning_index::{
        ClosureMode, GenerationKind, HeadPointer, ReasoningIndexId, ReasoningManifest,
        RegistrationState, ENGINE_SEMANTICS,
    };
    use fluree_db_core::{ContentKind, ContentStore, Flake, FlakeValue, Sid};
    use fluree_db_query::reasoning::MaintenanceBounds;
    use std::collections::HashSet;

    /// The persisted derived-only set of the index's head generation, with
    /// each fact's graph.
    async fn persisted_derived(
        fluree: &Fluree,
        id: &str,
        index: &ReasoningIndexId,
    ) -> HashSet<(Option<Sid>, Flake)> {
        let doc = fluree.reasoning_indexes(id).await.unwrap();
        let head = doc.indexes[index].head.clone().expect("published");
        let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
        let store = fluree.index_store(&record, index).await.unwrap();
        let manifest =
            ReasoningManifest::decode(&store.get(&head.manifest).await.unwrap()).unwrap();
        super::read_derived_only(store.as_ref(), &manifest.derived)
            .await
            .unwrap()
            .into_iter()
            .map(|f| (f.g.clone(), f))
            .collect()
    }

    /// The reasoning `program` settles to on `db`, as the worker settles it.
    async fn settled_reasoning(
        fluree: &Fluree,
        db: &crate::view::GraphDb,
        program: &serde_json::Value,
    ) -> fluree_db_query::ir::ReasoningConfig {
        let (_, parsed) = crate::query::helpers::parse_jsonld_query(
            &super::program::settling_query(&super::program::canonical_program(program).unwrap()),
            &db.snapshot,
            db.default_context.as_ref(),
            None,
        )
        .unwrap();
        fluree
            .build_executable_for_view(db, &parsed, None)
            .await
            .unwrap()
            .reasoning
    }

    /// The derived-only set a full recompute of `program` gives at the
    /// branch head: the oracle an incremental generation must equal.
    async fn full_derived(
        fluree: &Fluree,
        id: &str,
        program: &serde_json::Value,
    ) -> HashSet<(Option<Sid>, Flake)> {
        let db = fluree.db(id).await.unwrap();
        let reasoning = settled_reasoning(fluree, &db, program).await;
        let derived = fluree
            .complete_closure(&db, &reasoning)
            .await
            .unwrap()
            .unwrap();
        let graph_sids = super::graph_sids(&db);
        super::asserted_filter(&db, derived, false)
            .await
            .unwrap()
            .into_iter()
            .map(|(g_id, f)| (graph_sids.get(&g_id).cloned().flatten(), f))
            .collect()
    }

    /// A collector pass that releases while an index build has written files
    /// and not yet published them waits for the build: the dictionary blobs
    /// the generation shares with a superseded base root stay.
    #[tokio::test]
    async fn a_collector_pass_waits_for_an_index_build_to_publish() {
        use fluree_db_core::ContentKind;
        use fluree_db_indexer::gc::{plan_garbage, release_garbage_plan, CleanGarbageConfig};
        let mut fluree = crate::FlureeBuilder::memory().build_memory();
        let (worker, handle) = crate::BackgroundIndexerWorker::new(
            fluree.backend().clone(),
            fluree.nameservice_mode().publisher_arc().unwrap(),
            fluree_db_indexer::IndexerConfig::small(),
        );
        let local = tokio::task::LocalSet::new();
        local.spawn_local(worker.run());
        fluree.set_indexing_mode(crate::tx::IndexingMode::Background(handle.clone()));
        local
            .run_until(async move {
                let id = "reasoning-indexes/collector-race:main";
                let ledger_id = fluree_db_core::LedgerId::parse(id).unwrap();
                let ctx = json!({
                    "ex": "http://example.org/",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
                });
                let ledger = fluree.create_ledger(id).await.unwrap();
                let ledger = fluree
                    .insert(
                        ledger,
                        &json!({"@context": ctx, "@graph": [
                            {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                            {"@id": "ex:alice", "@type": "ex:Student"}
                        ]}),
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

                let seam = Arc::new(PauseSeam::default());
                *fluree.reasoning_index_state.before_publish.lock() = Some(Arc::clone(&seam));
                let refresh = fluree.refresh_reasoning_index(id, &index);
                let collect = async {
                    seam.reached.notified().await;
                    // The base moves on and is rebuilt; its collector retires the
                    // root the generation was copied from.
                    fluree
                        .insert(
                            ledger,
                            &json!({"@context": ctx, "@id": "ex:carol", "@type": "ex:Student"}),
                        )
                        .await
                        .unwrap();
                    let rebuilt = fluree
                        .reindex(id, crate::ReindexOptions::default())
                        .await
                        .unwrap();
                    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
                    let store = fluree.backend().content_store(&record.storage_namespace());
                    let config = CleanGarbageConfig {
                        max_old_indexes: Some(0),
                        min_time_garbage_mins: Some(0),
                        ..Default::default()
                    };
                    let plan = plan_garbage(store.as_ref(), &rebuilt.root_id, &config)
                        .await
                        .unwrap();
                    // A collector pass as the indexer runs one.
                    let gc = handle.hold_gc(&ledger_id.ledger_name()).await;
                    let pass = async {
                        let _window = handle.open_release_window(&gc).await.expect("not held");
                        release_garbage_plan(
                            plan,
                            fluree.backend(),
                            fluree.nameservice(),
                            &ledger_id,
                            Some(&[]),
                        )
                        .await
                    };
                    tokio::pin!(pass);
                    let early = tokio::time::timeout(Duration::from_millis(300), &mut pass).await;
                    seam.resume.notify_one();
                    match early {
                        Ok(done) => done,
                        Err(_) => pass.await,
                    }
                };
                let (outcome, released) = tokio::join!(refresh, collect);
                assert!(matches!(outcome.unwrap(), RefreshOutcome::Published { .. }));
                released.unwrap();

                let doc = fluree.reasoning_indexes(id).await.unwrap();
                let head = doc.indexes[&index].head.clone().unwrap();
                let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
                let store = fluree.index_store(&record, &index).await.unwrap();
                let manifest =
                    ReasoningManifest::decode(&store.get(&head.manifest).await.unwrap()).unwrap();
                let root = fluree_db_binary_index::format::index_root::IndexRoot::decode(
                    &store.get(&manifest.root).await.unwrap(),
                )
                .unwrap();
                let mut missing = Vec::new();
                for cid in root.all_cas_ids() {
                    if matches!(cid.content_kind(), Some(ContentKind::DictBlob { .. }))
                        && !store.has(&cid).await.unwrap()
                    {
                        missing.push(cid);
                    }
                }
                assert!(
                    missing.is_empty(),
                    "released while the build ran: {missing:?}"
                );
            })
            .await;
    }

    /// A small deterministic generator, so a failing sequence replays.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((self.0 >> 33) as usize) % n
        }
    }

    /// Refresh, expecting a closure of kind `closure` when given, and
    /// compare D and the served results with a full recompute. Returns the
    /// kind of closure the refresh computed.
    async fn refresh_and_compare(
        fluree: &Fluree,
        id: &str,
        index: &ReasoningIndexId,
        program: &serde_json::Value,
        closure: Option<ClosureMode>,
        step: usize,
    ) -> ClosureMode {
        let computed = match fluree.refresh_reasoning_index(id, index).await.unwrap() {
            RefreshOutcome::Published { stats, .. } => stats.closure,
            other => panic!("step {step}: {other:?}"),
        };
        if let Some(closure) = closure {
            assert_eq!(computed, closure, "step {step}");
        }
        let persisted = persisted_derived(fluree, id, index).await;
        let full = full_derived(fluree, id, program).await;
        assert_eq!(
            persisted.len(),
            full.len(),
            "step {step}: only incremental {:?}, only full {:?}",
            persisted.difference(&full).collect::<Vec<_>>(),
            full.difference(&persisted).collect::<Vec<_>>()
        );
        assert_eq!(persisted, full, "step {step}");
        assert_eq!(
            whole_graph(fluree, id, true).await,
            whole_graph(fluree, id, false).await,
            "step {step}"
        );
        computed
    }

    /// Every fact of the ledger with owl2rl reasoning, answered from the
    /// index when `served`, else at query time by an equivalent program the
    /// index does not match (datalog with no rules derives nothing).
    async fn whole_graph(fluree: &Fluree, id: &str, served: bool) -> Vec<String> {
        let reasoning = if served {
            json!("owl2rl")
        } else {
            json!(["owl2rl", "datalog"])
        };
        let resp = fluree
            .graph(id)
            .query()
            .jsonld(&json!({
                "select": ["?s", "?p", "?o"],
                "where": {"@id": "?s", "?p": "?o"},
                "reasoning": reasoning
            }))
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        assert_eq!(
            resp.reasoning.and_then(|r| r.index).is_some(),
            served,
            "served from the index: {served}"
        );
        let mut rows: Vec<String> = resp
            .result
            .as_array()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        rows.sort();
        rows
    }

    async fn bucket_count(fluree: &Fluree, id: &str, index: &ReasoningIndexId) -> usize {
        let doc = fluree.reasoning_indexes(id).await.unwrap();
        let head = doc.indexes[index].head.clone().unwrap();
        let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
        let store = fluree.index_store(&record, index).await.unwrap();
        let manifest =
            ReasoningManifest::decode(&store.get(&head.manifest).await.unwrap()).unwrap();
        fluree_db_core::reasoning_index::DerivedIndex::decode(
            &store.get(&manifest.derived).await.unwrap(),
        )
        .unwrap()
        .buckets
        .len()
    }

    /// Incremental OWL 2 RL maintenance equals a full recompute at every
    /// step, in its derived-only set and in what queries answer, over an
    /// ontology that exercises the property, class, restriction and chain
    /// rules, under random insertions that also re-assert asserted facts,
    /// assert derived ones, and grow the set across bucket partitions; and a
    /// second instance opening the same storage answers the same.
    #[tokio::test]
    async fn incremental_generations_equal_a_full_recompute() {
        oracle(None, Some(ClosureMode::Incremental)).await;
    }

    /// With no read kept from one round to the next, every lookup reads the
    /// closed state again; the answers are the same.
    #[tokio::test]
    async fn incremental_generations_equal_a_full_recompute_with_no_read_cache() {
        let bounds = MaintenanceBounds {
            cache_bytes: 0,
            ..Default::default()
        };
        oracle(Some(bounds), Some(ClosureMode::Incremental)).await;
    }

    /// A closure whose reads are larger than its bounds allow is computed in
    /// full, and publishes the same generation.
    #[tokio::test]
    async fn an_incremental_closure_past_its_bounds_is_computed_in_full() {
        let bounds = MaintenanceBounds {
            read_facts: 1,
            ..Default::default()
        };
        let full = oracle(Some(bounds), None).await;
        assert!(full >= 20, "{full} refreshes fell back to a full closure");
    }

    /// The classes of `ex:alice` under owl2rl, and the generation that
    /// answered, if one did.
    async fn alice_classes(fluree: &Fluree, id: &str) -> (Vec<String>, Option<u64>) {
        let resp = fluree
            .graph(id)
            .query()
            .jsonld(&json!({
                "@context": {"ex": "http://example.org/"},
                "select": "?c",
                "where": {"@id": "ex:alice", "@type": "?c"},
                "reasoning": "owl2rl"
            }))
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        let served = resp.reasoning.and_then(|r| r.index).map(|i| i.generation);
        let mut classes: Vec<String> = resp
            .result
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap().to_string())
            .collect();
        classes.sort();
        (classes, served)
    }

    /// A ledger with a student and an organization, and an owl2rl index
    /// registered on it.
    async fn students(fluree: &Fluree, id: &str) -> (ReasoningIndexId, crate::LedgerState) {
        let ctx = json!({
            "ex": "http://example.org/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        });
        let ledger = fluree.create_ledger(id).await.unwrap();
        let ledger = fluree
            .insert(
                ledger,
                &json!({"@context": ctx, "@graph": [
                    {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                    {"@id": "ex:alice", "@type": "ex:Student"},
                    {"@id": "ex:acme", "@type": "ex:Org"}
                ]}),
            )
            .await
            .unwrap()
            .ledger;
        let index = fluree
            .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
            .await
            .unwrap();
        (index, ledger)
    }

    async fn insert_student(
        fluree: &Fluree,
        ledger: crate::LedgerState,
        student: &str,
    ) -> crate::LedgerState {
        fluree
            .insert(
                ledger,
                &json!({"@context": {"ex": "http://example.org/"},
                        "@id": student, "@type": "ex:Student"}),
            )
            .await
            .unwrap()
            .ledger
    }

    /// How a generation of older engine semantics is written.
    #[derive(Clone, Copy)]
    enum Older {
        /// Under engine semantics 0, versioned as this build versions them.
        Versioned,
        /// As a build before versioning wrote it.
        Unversioned,
    }

    /// A ledger whose owl2rl index was computed under engine semantics 0, by
    /// an engine that also derived `ex:alice a ex:Org`; this instance runs
    /// the current semantics again on return.
    async fn indexed_by_an_older_engine(
        fluree: &Fluree,
        id: &str,
        older: Older,
    ) -> (ReasoningIndexId, crate::LedgerState) {
        let (index, ledger) = students(fluree, id).await;
        let db = fluree.db(id).await.unwrap();
        let sid = |iri: &str| db.snapshot.encode_iri(iri).unwrap();
        let wrong = Flake::new(
            sid("http://example.org/alice"),
            sid(fluree_vocab::rdf::TYPE),
            FlakeValue::Ref(sid("http://example.org/Org")),
            Sid::new(
                fluree_vocab::namespaces::JSON_LD,
                fluree_vocab::jsonld_names::ID,
            ),
            0,
            true,
            None,
        );
        *fluree.reasoning_index_state.other_engine.lock() = Some(OtherEngine {
            version: 0,
            extra: Some((0, wrong)),
        });
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 1, .. }
        ));
        let (classes, served) = alice_classes(fluree, id).await;
        assert_eq!(served, Some(1));
        assert!(classes.contains(&"ex:Org".to_string()), "{classes:?}");
        *fluree.reasoning_index_state.other_engine.lock() = None;
        if let Older::Unversioned = older {
            as_written_before_versioning(fluree, id, &index).await;
        }
        (index, ledger)
    }

    /// Rewrite the index's head as a build before engine versioning wrote
    /// it: no `engine` in its manifest or its pointer, and the settled key
    /// digested without one.
    async fn as_written_before_versioning(fluree: &Fluree, id: &str, index: &ReasoningIndexId) {
        let db = fluree.db(id).await.unwrap();
        let reasoning = settled_reasoning(fluree, &db, &json!({"reasoning": "owl2rl"})).await;
        let settled = serde_json::to_value(super::program::unversioned_settled_key(
            &reasoning,
            db.graph_id,
        ))
        .unwrap();
        let head = fluree.reasoning_indexes(id).await.unwrap().indexes[index]
            .head
            .clone()
            .unwrap();
        let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
        let index_cs = fluree
            .backend()
            .content_store(&record.storage_namespace().reasoning_index(index.as_str()));
        let bytes = index_cs.get(&head.manifest).await.unwrap();
        let mut manifest: serde_json::Value = serde_json::from_slice(&bytes[4..]).unwrap();
        manifest.as_object_mut().unwrap().remove("engine").unwrap();
        manifest["settled"] = settled.clone();
        let mut bytes = b"FRM1".to_vec();
        serde_json::to_writer(&mut bytes, &manifest).unwrap();
        assert_eq!(ReasoningManifest::decode(&bytes).unwrap().engine, 0);
        let manifest_id = index_cs
            .put(ContentKind::ReasoningManifest, &bytes)
            .await
            .unwrap();
        let mut pointer = serde_json::to_value(&head).unwrap();
        pointer.as_object_mut().unwrap().remove("engine").unwrap();
        pointer["settled"] = settled;
        pointer["manifest"] = serde_json::to_value(&manifest_id).unwrap();
        let pointer: HeadPointer = serde_json::from_value(pointer).unwrap();
        assert_eq!(pointer.engine, 0);
        fluree
            .update_reasoning_indexes(id, |actual| {
                let mut next = actual.clone();
                next.indexes.get_mut(index).unwrap().head = Some(pointer.clone());
                Ok((Some(next), ()))
            })
            .await
            .unwrap();
    }

    /// A generation computed under other engine semantics is not served
    /// and not current: it is rebuilt at its own basis, with no new commit,
    /// and the rebuild is served.
    #[tokio::test]
    async fn a_generation_of_other_engine_semantics_is_rebuilt_at_its_basis() {
        rebuilt_at_its_basis(Older::Versioned).await;
    }

    #[tokio::test]
    async fn a_generation_written_before_versioning_is_rebuilt_at_its_basis() {
        rebuilt_at_its_basis(Older::Unversioned).await;
    }

    async fn rebuilt_at_its_basis(older: Older) {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/engine-rebuild:main";
        let (index, _) = indexed_by_an_older_engine(&fluree, id, older).await;

        let (classes, served) = alice_classes(&fluree, id).await;
        assert_eq!(served, None, "served across engine semantics");
        assert_eq!(classes, ["ex:Person", "ex:Student"]);

        let head = fluree.reasoning_indexes(id).await.unwrap().indexes[&index]
            .head
            .clone()
            .unwrap();
        assert_eq!(head.engine, 0);
        match fluree.refresh_reasoning_index(id, &index).await.unwrap() {
            RefreshOutcome::Published {
                generation,
                kind,
                basis_t,
                stats,
            } => assert_eq!(
                (generation, kind, basis_t, stats.closure),
                (2, GenerationKind::Rebuild, head.basis.t, ClosureMode::Full)
            ),
            other => panic!("{other:?}"),
        }
        let head = fluree.reasoning_indexes(id).await.unwrap().indexes[&index]
            .head
            .clone()
            .unwrap();
        assert_eq!(head.engine, ENGINE_SEMANTICS);
        let (classes, served) = alice_classes(&fluree, id).await;
        assert_eq!(served, Some(2));
        assert_eq!(classes, ["ex:Person", "ex:Student"]);
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Current { generation: 2 }
        ));
    }

    /// An insertion after a generation of other engine semantics computes
    /// its closure in full rather than maintain that generation's facts;
    /// the next insertion builds on the rebuilt one.
    #[tokio::test]
    async fn an_insertion_does_not_build_on_other_engine_semantics() {
        not_built_on(Older::Versioned).await;
    }

    #[tokio::test]
    async fn an_insertion_does_not_build_on_a_generation_written_before_versioning() {
        not_built_on(Older::Unversioned).await;
    }

    async fn not_built_on(older: Older) {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/engine-seed:main";
        let (index, ledger) = indexed_by_an_older_engine(&fluree, id, older).await;
        let program = json!({"reasoning": "owl2rl"});

        let ledger = insert_student(&fluree, ledger, "ex:bob").await;
        refresh_and_compare(&fluree, id, &index, &program, Some(ClosureMode::Full), 1).await;
        let (classes, _) = alice_classes(&fluree, id).await;
        assert_eq!(classes, ["ex:Person", "ex:Student"]);

        insert_student(&fluree, ledger, "ex:carol").await;
        refresh_and_compare(
            &fluree,
            id,
            &index,
            &program,
            Some(ClosureMode::Incremental),
            2,
        )
        .await;
    }

    /// Two instances over one storage and nameservice: the first runs engine
    /// semantics 0, the second the current ones.
    fn older_and_newer() -> (Fluree, Fluree) {
        let storage = fluree_db_core::MemoryStorage::new();
        let nameservice = Arc::new(fluree_db_nameservice::memory::MemoryNameService::new());
        let instance = || {
            crate::FlureeBuilder::memory().build_with(
                storage.clone(),
                crate::NameServiceMode::ReadWrite(nameservice.clone()),
            )
        };
        let older = instance();
        *older.reasoning_index_state.other_engine.lock() = Some(OtherEngine {
            version: 0,
            extra: None,
        });
        (older, instance())
    }

    /// Instances of an older and a newer engine sharing a branch, whichever
    /// refreshes first: the newer one's generation stays published, and the
    /// older one leaves it, and the commits after it, to the newer one.
    #[tokio::test]
    async fn an_older_engine_leaves_an_index_to_a_newer_one_in_either_order() {
        let declined = RefreshOutcome::NewerEngine {
            published: ENGINE_SEMANTICS,
            engine: 0,
        };
        for older_first in [true, false] {
            let (older, newer) = older_and_newer();
            let id = "reasoning-indexes/engine-rolling:main";
            let (index, ledger) = students(&newer, id).await;
            let mut generation = 1;
            if older_first {
                assert!(matches!(
                    older.refresh_reasoning_index(id, &index).await.unwrap(),
                    RefreshOutcome::Published { generation: 1, .. }
                ));
                generation = 2;
            }
            match newer.refresh_reasoning_index(id, &index).await.unwrap() {
                RefreshOutcome::Published { generation: g, .. } => {
                    assert_eq!(g, generation, "older first: {older_first}");
                }
                other => panic!("older first: {older_first}: {other:?}"),
            }
            assert_eq!(
                older.refresh_reasoning_index(id, &index).await.unwrap(),
                declined,
                "older first: {older_first}"
            );
            assert_eq!(alice_classes(&newer, id).await.1, Some(generation));

            insert_student(&newer, ledger, "ex:bob").await;
            // As a notified peer or a Raft follower learns of the commit.
            older.refresh(id, Default::default()).await.unwrap();
            assert_eq!(
                older.refresh_reasoning_index(id, &index).await.unwrap(),
                declined,
                "older first: {older_first}"
            );
            match newer.refresh_reasoning_index(id, &index).await.unwrap() {
                RefreshOutcome::Published {
                    generation: g,
                    stats,
                    ..
                } => assert_eq!(
                    (g, stats.closure),
                    (generation + 1, ClosureMode::Incremental),
                    "older first: {older_first}"
                ),
                other => panic!("older first: {older_first}: {other:?}"),
            }
            assert_eq!(alice_classes(&newer, id).await.1, Some(generation + 1));
        }
    }

    /// An older engine's generation, computed while a newer engine published
    /// one, is refused at publication.
    #[tokio::test]
    async fn an_older_engine_s_generation_is_refused_once_a_newer_one_publishes() {
        let (older, newer) = older_and_newer();
        let id = "reasoning-indexes/engine-race:main";
        let (index, _) = students(&newer, id).await;
        let seam = Arc::new(PauseSeam::default());
        *older.reasoning_index_state.before_publish.lock() = Some(Arc::clone(&seam));
        let newer_publishes = async {
            seam.reached.notified().await;
            let outcome = newer.refresh_reasoning_index(id, &index).await.unwrap();
            seam.resume.notify_one();
            outcome
        };
        let (older_outcome, newer_outcome) =
            tokio::join!(older.refresh_reasoning_index(id, &index), newer_publishes);
        assert!(matches!(
            newer_outcome,
            RefreshOutcome::Published { generation: 1, .. }
        ));
        match older_outcome.unwrap() {
            RefreshOutcome::Discarded { reason } => {
                assert!(reason.contains("engine semantics"), "{reason}");
            }
            other => panic!("{other:?}"),
        }
        let head = newer.reasoning_indexes(id).await.unwrap().indexes[&index]
            .head
            .clone()
            .unwrap();
        assert_eq!((head.generation, head.engine), (1, ENGINE_SEMANTICS));
        assert_eq!(alice_classes(&newer, id).await.1, Some(1));
    }

    /// After a downgrade, an index a newer engine published is retired and
    /// its program registered again; the older engine maintains the new
    /// index.
    #[tokio::test]
    async fn after_a_downgrade_an_index_is_registered_again() {
        let (older, newer) = older_and_newer();
        let id = "reasoning-indexes/engine-downgrade:main";
        let (index, _) = students(&newer, id).await;
        assert!(matches!(
            newer.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 1, .. }
        ));
        drop(newer);
        assert!(matches!(
            older.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::NewerEngine { .. }
        ));
        assert_eq!(alice_classes(&older, id).await.1, None);

        older.retire_reasoning_index(id, &index).await.unwrap();
        let again = older
            .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
            .await
            .unwrap();
        assert_ne!(again, index);
        assert!(matches!(
            older.refresh_reasoning_index(id, &again).await.unwrap(),
            RefreshOutcome::Published { generation: 1, .. }
        ));
        assert_eq!(alice_classes(&older, id).await.1, Some(1));
    }

    /// The dictionary blobs an index root references.
    async fn dict_blobs(
        store: &dyn ContentStore,
        root: &fluree_db_core::ContentId,
    ) -> HashSet<fluree_db_core::ContentId> {
        fluree_db_binary_index::format::index_root::IndexRoot::decode(
            &store.get(root).await.unwrap(),
        )
        .unwrap()
        .all_cas_ids()
        .into_iter()
        .filter(|cid| matches!(cid.content_kind(), Some(ContentKind::DictBlob { .. })))
        .collect()
    }

    /// The head generation's manifest.
    async fn head_manifest(
        fluree: &Fluree,
        id: &str,
        index: &ReasoningIndexId,
    ) -> (fluree_db_core::ContentId, ReasoningManifest) {
        let head = fluree.reasoning_indexes(id).await.unwrap().indexes[index]
            .head
            .clone()
            .unwrap();
        let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
        let store = fluree.index_store(&record, index).await.unwrap();
        let manifest =
            ReasoningManifest::decode(&store.get(&head.manifest).await.unwrap()).unwrap();
        (head.manifest, manifest)
    }

    /// Dictionary blobs that only a retained, superseded generation reaches
    /// survive the index collector, a branch collector pass that releases the
    /// base root sharing them, and a sweep; once the generation is released,
    /// a sweep reclaims them. Two kinds: blobs the old base root and the
    /// generation share, and blobs only the generation wrote.
    #[tokio::test]
    async fn dictionaries_only_a_retained_generation_reaches_are_kept() {
        use fluree_db_indexer::gc::{plan_garbage, release_garbage_plan, CleanGarbageConfig};
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/retained-dicts:main";
        let (index, ledger) = students(&fluree, id).await;
        let r0 = fluree
            .reindex(id, crate::ReindexOptions::default())
            .await
            .unwrap()
            .root_id;

        // Generation 1 writes a string nothing else holds.
        let db = fluree.db(id).await.unwrap();
        let sid = |iri: &str| db.snapshot.encode_iri(iri).unwrap();
        let minted = Flake::new(
            sid("http://example.org/alice"),
            sid("http://example.org/tag"),
            FlakeValue::String("only-generation-one-holds-this".into()),
            Sid::new(
                fluree_vocab::namespaces::XSD,
                fluree_vocab::xsd_names::STRING,
            ),
            0,
            true,
            None,
        );
        *fluree.reasoning_index_state.other_engine.lock() = Some(OtherEngine {
            version: 0,
            extra: Some((0, minted)),
        });
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 1, .. }
        ));
        let (g1_manifest_id, g1) = head_manifest(&fluree, id, &index).await;
        *fluree.reasoning_index_state.other_engine.lock() = None;

        // The base moves on and is rebuilt; generation 2 is a rebuild from it.
        fluree
            .insert(
                ledger,
                &json!({"@context": {"ex": "http://example.org/"},
                        "@id": "ex:carol", "@type": "ex:Student", "ex:name": "Carol"}),
            )
            .await
            .unwrap();
        let r1 = fluree
            .reindex(id, crate::ReindexOptions::default())
            .await
            .unwrap()
            .root_id;
        match fluree.refresh_reasoning_index(id, &index).await.unwrap() {
            RefreshOutcome::Published {
                generation: 2,
                kind: GenerationKind::Rebuild,
                ..
            } => {}
            other => panic!("{other:?}"),
        }
        let (_, g2) = head_manifest(&fluree, id, &index).await;

        let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
        let store = fluree.index_store(&record, &index).await.unwrap();
        let (d_r0, d_g1, d_r1, d_g2) = (
            dict_blobs(store.as_ref(), &r0).await,
            dict_blobs(store.as_ref(), &g1.root).await,
            dict_blobs(store.as_ref(), &r1).await,
            dict_blobs(store.as_ref(), &g2.root).await,
        );
        let current: HashSet<_> = d_r1.union(&d_g2).cloned().collect();
        let shared: Vec<_> = d_r0
            .intersection(&d_g1)
            .filter(|cid| !current.contains(*cid))
            .cloned()
            .collect();
        let own: Vec<_> = d_g1
            .iter()
            .filter(|cid| !d_r0.contains(*cid) && !current.contains(*cid))
            .cloned()
            .collect();
        assert!(
            !shared.is_empty() && !own.is_empty(),
            "shared with the old base: {}, generation 1's own: {}",
            shared.len(),
            own.len()
        );
        let all_present = |label: &'static str| {
            let store = Arc::clone(&store);
            let blobs: Vec<_> = shared.iter().chain(&own).cloned().collect();
            async move {
                for cid in blobs {
                    assert!(store.has(&cid).await.unwrap(), "{label}: {cid} released");
                }
            }
        };
        all_present("after the index collector").await;

        let ledger_id = fluree_db_core::LedgerId::parse(id).unwrap();
        let branch_store = fluree.backend().content_store(&record.storage_namespace());
        let config = CleanGarbageConfig {
            max_old_indexes: Some(0),
            min_time_garbage_mins: Some(0),
            ..Default::default()
        };
        let plan = plan_garbage(branch_store.as_ref(), &r1, &config)
            .await
            .unwrap();
        release_garbage_plan(
            plan,
            fluree.backend(),
            fluree.nameservice(),
            &ledger_id,
            Some(&[]),
        )
        .await
        .unwrap();
        assert!(
            !branch_store.has(&r0).await.unwrap(),
            "the branch pass released the old base root"
        );
        all_present("after the branch collector").await;

        fluree
            .sweep_index_storage("reasoning-indexes/retained-dicts")
            .await
            .unwrap();
        all_present("after a sweep").await;
        let index_cs = fluree
            .backend()
            .content_store(&record.storage_namespace().reasoning_index(index.as_str()));
        for (label, root) in [("r0", &r0), ("r1", &r1)] {
            assert!(
                !index_cs.has(root).await.unwrap(),
                "the copy of base root {label} a rebuild started from is reclaimed"
            );
        }
        assert_eq!(alice_classes(&fluree, id).await.1, Some(2));

        // Released with generation 1, and nothing else reaches them.
        fluree.set_reasoning_index_retention(super::super::GenerationRetention {
            min_age_ms: 0,
            ..Default::default()
        });
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Current { generation: 2 }
        ));
        assert!(
            !index_cs.has(&g1_manifest_id).await.unwrap(),
            "generation 1 is released"
        );
        fluree
            .sweep_index_storage("reasoning-indexes/retained-dicts")
            .await
            .unwrap();
        for cid in shared.iter().chain(&own) {
            assert!(
                !store.has(cid).await.unwrap(),
                "{cid} outlived the generation that reached it"
            );
        }
        assert_eq!(alice_classes(&fluree, id).await.1, Some(2));
    }

    /// The files under an index's folder.
    async fn index_files(fluree: &Fluree, id: &str, index: &ReasoningIndexId) -> Vec<String> {
        let storage = fluree.backend().admin_storage_cloned().unwrap();
        let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
        let prefix = format!(
            "fluree:{}://{}/",
            storage.storage_method(),
            record
                .storage_namespace()
                .reasoning_index(index.as_str())
                .branch_prefix()
        );
        storage.list_prefix(&prefix).await.unwrap()
    }

    /// Retiring an index while a refresh builds it refuses the build. The
    /// entry stays retired until cleanup removes it with every file the build
    /// wrote, and nothing recreates it.
    #[tokio::test]
    async fn retiring_an_index_while_it_builds_refuses_the_build() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/retire-race:main";
        let (index, _) = students(&fluree, id).await;
        let seam = Arc::new(PauseSeam::default());
        *fluree.reasoning_index_state.before_publish.lock() = Some(Arc::clone(&seam));
        let retire = async {
            seam.reached.notified().await;
            fluree.retire_reasoning_index(id, &index).await.unwrap();
            seam.resume.notify_one();
        };
        let (outcome, ()) = tokio::join!(fluree.refresh_reasoning_index(id, &index), retire);
        *fluree.reasoning_index_state.before_publish.lock() = None;
        match outcome.unwrap() {
            RefreshOutcome::Discarded { reason } => assert!(reason.contains("retired"), "{reason}"),
            other => panic!("{other:?}"),
        }
        let entry = fluree.reasoning_indexes(id).await.unwrap().indexes[&index].clone();
        assert!(matches!(
            entry.registration.state,
            RegistrationState::Retired { .. }
        ));
        assert!(entry.head.is_none());
        assert_eq!(alice_classes(&fluree, id).await.1, None);
        assert!(fluree
            .refresh_reasoning_indexes(id)
            .await
            .unwrap()
            .is_empty());
        assert!(
            !index_files(&fluree, id, &index).await.is_empty(),
            "the refused build wrote files"
        );

        fluree.set_reasoning_index_retention(super::super::GenerationRetention {
            min_age_ms: 0,
            ..Default::default()
        });
        assert_eq!(
            fluree.clean_retired_reasoning_indexes(id).await.unwrap(),
            vec![index.clone()]
        );
        assert!(fluree
            .reasoning_indexes(id)
            .await
            .unwrap()
            .indexes
            .is_empty());
        assert_eq!(index_files(&fluree, id, &index).await, Vec::<String>::new());
        assert!(fluree
            .refresh_reasoning_indexes(id)
            .await
            .unwrap()
            .is_empty());
        assert!(fluree
            .reasoning_indexes(id)
            .await
            .unwrap()
            .indexes
            .is_empty());
    }

    /// Dropping a branch that has children, while its index builds, freezes
    /// the branch; the build's publication is refused, and nothing is
    /// published on the frozen branch.
    #[tokio::test]
    async fn dropping_a_branch_while_its_index_builds_refuses_the_build() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let name = "reasoning-indexes/frozen-race";
        students(&fluree, &format!("{name}:main")).await;
        fluree.create_branch(name, "dev", None, None).await.unwrap();
        fluree
            .create_branch(name, "child", Some("dev"), None)
            .await
            .unwrap();
        let dev = format!("{name}:dev");
        let index = fluree
            .register_reasoning_index(&dev, &json!({"reasoning": "owl2rl"}))
            .await
            .unwrap();
        let seam = Arc::new(PauseSeam::default());
        *fluree.reasoning_index_state.before_publish.lock() = Some(Arc::clone(&seam));
        let drop = async {
            seam.reached.notified().await;
            let drop = fluree.drop_branch(name, "dev");
            tokio::pin!(drop);
            let early = tokio::time::timeout(Duration::from_millis(300), &mut drop).await;
            seam.resume.notify_one();
            match early {
                Ok(done) => done,
                Err(_) => drop.await,
            }
        };
        let (outcome, dropped) = tokio::join!(fluree.refresh_reasoning_index(&dev, &index), drop);
        *fluree.reasoning_index_state.before_publish.lock() = None;
        assert!(dropped.unwrap().deferred, "a branch with children is kept");
        assert!(
            matches!(
                outcome,
                Err(crate::ApiError::NameService(
                    fluree_db_nameservice::NameServiceError::Fenced(_)
                ))
            ),
            "{outcome:?}"
        );
        let doc = fluree
            .nameservice()
            .get_reasoning_indexes(&dev)
            .await
            .unwrap();
        assert!(doc
            .map(|d| d.indexes.values().all(|e| e.head.is_none()))
            .unwrap_or(true));
    }

    /// A build that starts writing after cleanup removed its retired index
    /// leaves nothing in the index's folder.
    #[tokio::test]
    async fn a_build_outlasting_its_index_cleanup_leaves_no_files() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/cleanup-race:main";
        let (index, _) = students(&fluree, id).await;
        fluree.set_reasoning_index_retention(super::super::GenerationRetention {
            min_age_ms: 0,
            ..Default::default()
        });
        let seam = Arc::new(PauseSeam::default());
        *fluree.reasoning_index_state.before_write.lock() = Some(Arc::clone(&seam));
        let cleanup = async {
            seam.reached.notified().await;
            fluree.retire_reasoning_index(id, &index).await.unwrap();
            let cleaned = fluree.clean_retired_reasoning_indexes(id).await.unwrap();
            seam.resume.notify_one();
            cleaned
        };
        let (outcome, cleaned) = tokio::join!(fluree.refresh_reasoning_index(id, &index), cleanup);
        *fluree.reasoning_index_state.before_write.lock() = None;
        assert_eq!(cleaned, vec![index.clone()]);
        assert!(
            !matches!(outcome, Ok(RefreshOutcome::Published { .. })),
            "{outcome:?}"
        );
        assert!(fluree
            .reasoning_indexes(id)
            .await
            .unwrap()
            .indexes
            .is_empty());
        assert_eq!(index_files(&fluree, id, &index).await, Vec::<String>::new());
    }

    /// Cleanup of a retired index waits for a build writing into its folder,
    /// then deletes what the build wrote along with the rest.
    #[tokio::test]
    async fn cleanup_waits_for_a_build_writing_into_the_folder() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/cleanup-hold:main";
        let (index, _) = students(&fluree, id).await;
        fluree.set_reasoning_index_retention(super::super::GenerationRetention {
            min_age_ms: 0,
            ..Default::default()
        });
        let seam = Arc::new(PauseSeam::default());
        *fluree.reasoning_index_state.before_publish.lock() = Some(Arc::clone(&seam));
        let cleanup = async {
            seam.reached.notified().await;
            fluree.retire_reasoning_index(id, &index).await.unwrap();
            let cleanup = fluree.clean_retired_reasoning_indexes(id);
            tokio::pin!(cleanup);
            let early = tokio::time::timeout(Duration::from_millis(300), &mut cleanup).await;
            seam.resume.notify_one();
            assert!(early.is_err(), "cleanup ran while the build was writing");
            cleanup.await.unwrap()
        };
        let (outcome, cleaned) = tokio::join!(fluree.refresh_reasoning_index(id, &index), cleanup);
        *fluree.reasoning_index_state.before_publish.lock() = None;
        assert!(
            matches!(outcome, Ok(RefreshOutcome::Discarded { .. })),
            "{outcome:?}"
        );
        assert_eq!(cleaned, vec![index.clone()]);
        assert!(fluree
            .reasoning_indexes(id)
            .await
            .unwrap()
            .indexes
            .is_empty());
        assert_eq!(index_files(&fluree, id, &index).await, Vec::<String>::new());
    }

    /// A served query reads every graph from the generation, in its one
    /// dictionary scope. The generation minted a string before the base
    /// asserted it, and the base, reindexed at the same commit, gives it
    /// another id; a join on that value between a derived fact and an
    /// asserted one in another graph is still answered from the index, for
    /// one ledger's graphs and for a dataset of them.
    #[tokio::test]
    async fn a_served_join_across_graphs_reads_one_dictionary() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/dict-scope:main";
        let ctx = json!({"ex": "http://example.org/"});
        let ledger = fluree.create_ledger(id).await.unwrap();
        let ledger = fluree
            .insert(
                ledger,
                &json!({"@context": ctx, "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Student"},
                    {"@id": "ex:bob", "@type": "ex:Student"}
                ]}),
            )
            .await
            .unwrap()
            .ledger;
        fluree
            .reindex(id, crate::ReindexOptions::default())
            .await
            .unwrap();
        let rules = json!([{
            "@context": ctx,
            "where": {"@id": "?x", "@type": "ex:Student"},
            "insert": {"@id": "?x", "ex:tag": "minted"}
        }]);
        let program = json!({"reasoning": "datalog", "rules": rules});
        let index = fluree.register_reasoning_index(id, &program).await.unwrap();
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 1, .. }
        ));

        // The base gives "Ally" the id the generation gave "minted".
        let ledger = fluree
            .stage_owned(ledger)
            .upsert_turtle(
                r#"@prefix ex: <http://example.org/> .
                GRAPH ex:roster { ex:carol ex:nick "Ally" . }"#,
            )
            .execute()
            .await
            .unwrap()
            .ledger;
        fluree
            .stage_owned(ledger)
            .upsert_turtle(
                r#"@prefix ex: <http://example.org/> .
                GRAPH ex:roster { ex:carol ex:alias "minted" . }"#,
            )
            .execute()
            .await
            .unwrap();
        fluree
            .reindex(id, crate::ReindexOptions::default())
            .await
            .unwrap();
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 2, .. }
        ));

        let db = fluree.db(id).await.unwrap();
        let reasoning = settled_reasoning(&fluree, &db, &program).await;
        let served = fluree
            .reasoning_index_for(&db, &reasoning, false)
            .await
            .unwrap()
            .expect("servable");
        let string_ids = |view: &crate::view::GraphDb| {
            let store = view.binary_store.clone().unwrap();
            ["minted", "Ally"].map(|v| store.find_string_id(v).unwrap().unwrap())
        };
        let (base, generation) = (string_ids(&db), string_ids(&served.view));
        assert_ne!(base, generation, "the dictionaries assign the same ids");

        let expected = vec![
            json!(["ex:alice", "ex:carol"]),
            json!(["ex:bob", "ex:carol"]),
        ];
        let joined = |graph: &str| {
            json!({
                "@context": ctx,
                "select": ["?x", "?y"],
                "where": [
                    {"@id": "?x", "ex:tag": "?v"},
                    ["graph", graph, {"@id": "?y", "ex:alias": "?v"}]
                ],
                "reasoning": "datalog",
                "rules": rules
            })
        };
        let rows = |result: &serde_json::Value| {
            let mut rows = result.as_array().unwrap().clone();
            rows.sort_by_key(ToString::to_string);
            rows
        };

        let resp = fluree
            .graph(id)
            .query()
            .jsonld(&joined("http://example.org/roster"))
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        assert!(resp.reasoning.and_then(|r| r.index).is_some(), "served");
        assert_eq!(rows(&resp.result), expected, "one ledger's graphs");

        let mut dataset = joined("?g");
        dataset["from"] = json!(id);
        dataset["fromNamed"] = json!([format!("{id}#http://example.org/roster")]);
        let resp = fluree
            .query_from()
            .jsonld(&dataset)
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        assert!(resp.reasoning.and_then(|r| r.index).is_some(), "served");
        assert_eq!(rows(&resp.result), expected, "a dataset of its graphs");
    }

    /// A drop takes a graph's facts out with no retraction to show for them:
    /// a refresh over it computes the closure in full and rebuilds, so what
    /// the dropped facts supported is gone from the index.
    #[tokio::test]
    async fn a_refresh_over_a_graph_drop_computes_in_full() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/drop-window:main";
        let (index, ledger) = students(&fluree, id).await;
        let program = json!({"reasoning": "owl2rl"});
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 1, .. }
        ));
        drop(ledger);
        fluree
            .graph(id)
            .transact()
            .sparql_update("DROP DEFAULT")
            .commit()
            .await
            .unwrap();
        assert_eq!(
            refresh_and_compare(&fluree, id, &index, &program, None, 1).await,
            ClosureMode::Full
        );
        assert_eq!(alice_classes(&fluree, id).await, (vec![], Some(2)));
    }

    /// A drop of a graph the program derives into takes the derived facts
    /// out of the index with the rest. Written again in the same window, the
    /// graph holds what the program derives there after the refresh.
    #[tokio::test]
    async fn a_graph_the_program_derives_into_holds_its_facts_after_a_drop() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/drop-derived:main";
        let ledger = fluree.create_ledger(id).await.unwrap();
        fluree
            .stage_owned(ledger)
            .upsert_turtle(
                r#"@prefix ex: <http://example.org/> .
                ex:a ex:parent ex:b . ex:b ex:parent ex:c .
                GRAPH ex:inferred { ex:seed ex:note "seed" . }"#,
            )
            .execute()
            .await
            .unwrap();
        let ctx = json!({"ex": "http://example.org/"});
        let rules = json!([{
            "@context": ctx,
            "where": [{"@id": "?x", "ex:parent": "?y"}, {"@id": "?y", "ex:parent": "?z"}],
            "insert": {"@id": "?x", "@graph": "ex:inferred", "ex:grandparent": {"@id": "?z"}}
        }]);
        let index = fluree
            .register_reasoning_index(id, &json!({"reasoning": "datalog", "rules": rules}))
            .await
            .unwrap();
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 1, .. }
        ));
        fluree
            .graph(id)
            .transact()
            .sparql_update("DROP GRAPH <http://example.org/inferred>")
            .commit()
            .await
            .unwrap();
        // Written again in the same window: the graph exists, and the
        // program derives into it.
        fluree
            .graph(id)
            .transact()
            .sparql_update(
                "INSERT DATA { GRAPH <http://example.org/inferred> { \
                 <http://example.org/seed> <http://example.org/note> \"again\" } }",
            )
            .commit()
            .await
            .unwrap();
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 2, .. }
        ));
        let q = json!({
            "@context": ctx,
            "select": ["?x", "?z"],
            "where": [["graph", "http://example.org/inferred", {"@id": "?x", "ex:grandparent": "?z"}]],
            "reasoning": "datalog",
            "rules": rules
        });
        let resp = fluree
            .graph(id)
            .query()
            .jsonld(&q)
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        assert_eq!(
            resp.reasoning.and_then(|r| r.index).map(|i| i.generation),
            Some(2)
        );
        assert_eq!(resp.result, json!([["ex:a", "ex:c"]]));
    }

    /// `asOf` does not take a generation from before a graph drop: it holds
    /// what the graph held, which the drop took out as of every `t`.
    #[tokio::test]
    async fn as_of_skips_a_generation_from_before_a_graph_drop() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let id = "reasoning-indexes/drop-as-of:main";
        let (index, _) = students(&fluree, id).await;
        assert!(matches!(
            fluree.refresh_reasoning_index(id, &index).await.unwrap(),
            RefreshOutcome::Published { generation: 1, .. }
        ));
        let as_of = json!({
            "@context": {"ex": "http://example.org/"},
            "select": "?c",
            "where": {"@id": "ex:alice", "@type": "?c"},
            "reasoning": "owl2rl",
            "reasoningFreshness": "asOf"
        });
        let run = |q: serde_json::Value| {
            let fluree = &fluree;
            async move {
                let resp = fluree
                    .graph(id)
                    .query()
                    .jsonld(&q)
                    .track_all()
                    .execute_tracked()
                    .await
                    .unwrap();
                (
                    resp.result,
                    resp.reasoning.and_then(|r| r.index).map(|i| i.generation),
                )
            }
        };
        // A commit that drops nothing: the generation still answers.
        insert_student(&fluree, fluree.ledger(id).await.unwrap(), "ex:bob").await;
        assert_eq!(run(as_of.clone()).await.1, Some(1));

        fluree
            .graph(id)
            .transact()
            .sparql_update("DROP DEFAULT")
            .commit()
            .await
            .unwrap();
        assert_eq!(run(as_of.clone()).await, (json!([]), None));
    }

    /// Insert-only refreshes under `bounds`, each compared with a full
    /// recompute; each is expected to compute `incremental` when given.
    /// Returns how many computed the closure in full.
    async fn oracle(bounds: Option<MaintenanceBounds>, incremental: Option<ClosureMode>) -> usize {
        let storage = fluree_db_core::MemoryStorage::new();
        let nameservice = Arc::new(fluree_db_nameservice::memory::MemoryNameService::new());
        let instance = || {
            crate::FlureeBuilder::memory().build_with(
                storage.clone(),
                crate::NameServiceMode::ReadWrite(nameservice.clone()),
            )
        };
        let fluree = instance();
        *fluree.reasoning_index_state.maintenance.lock() = bounds;
        let mut full_closures = 0;
        let mut count = |closure: ClosureMode| {
            if closure == ClosureMode::Full {
                full_closures += 1;
            }
        };
        let id = "reasoning-indexes/incremental-oracle:main";
        let ctx = json!({
            "ex": "http://example.org/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "owl": "http://www.w3.org/2002/07/owl#"
        });
        let ledger = fluree.create_ledger(id).await.unwrap();
        let mut ledger = fluree
            .insert(
                ledger,
                &json!({"@context": ctx, "@graph": [
                    {"@id": "ex:GradStudent", "rdfs:subClassOf": {"@id": "ex:Student"}},
                    {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                    {"@id": "ex:advisor", "rdfs:subPropertyOf": {"@id": "ex:knows"}},
                    {"@id": "ex:knows", "@type": "owl:SymmetricProperty"},
                    {"@id": "ex:partOf", "@type": "owl:TransitiveProperty"},
                    {"@id": "ex:hasMember", "owl:inverseOf": {"@id": "ex:memberOf"}},
                    {"@id": "ex:teaches", "rdfs:domain": {"@id": "ex:Faculty"},
                     "rdfs:range": {"@id": "ex:Course"}},
                    {"@id": "ex:Employee", "owl:equivalentClass": {"@id": "ex:WorksSomewhere"}},
                    {"@id": "ex:WorksSomewhere", "@type": "owl:Restriction",
                     "owl:onProperty": {"@id": "ex:worksFor"},
                     "owl:someValuesFrom": {"@id": "ex:Org"}},
                    {"@id": "ex:CsMember", "owl:equivalentClass": {"@id": "ex:InCs"}},
                    {"@id": "ex:InCs", "@type": "owl:Restriction",
                     "owl:onProperty": {"@id": "ex:memberOf"},
                     "owl:hasValue": {"@id": "ex:cs"}},
                    {"@id": "ex:Club", "rdfs:subClassOf": {"@id": "ex:OnlyPeople"}},
                    {"@id": "ex:OnlyPeople", "@type": "owl:Restriction",
                     "owl:onProperty": {"@id": "ex:hasMember"},
                     "owl:allValuesFrom": {"@id": "ex:Person"}},
                    {"@id": "ex:Chair", "owl:equivalentClass": {"@id": "ex:PersonHeading"}},
                    {"@id": "ex:PersonHeading",
                     "owl:intersectionOf": {"@list": [{"@id": "ex:Person"}, {"@id": "ex:HeadsDept"}]}},
                    {"@id": "ex:HeadsDept", "@type": "owl:Restriction",
                     "owl:onProperty": {"@id": "ex:headOf"},
                     "owl:someValuesFrom": {"@id": "ex:Dept"}},
                    {"@id": "ex:Agent",
                     "owl:unionOf": {"@list": [{"@id": "ex:Person"}, {"@id": "ex:Org"}]}},
                    {"@id": "ex:grandparent",
                     "owl:propertyChainAxiom": {"@list": [{"@id": "ex:parent"}, {"@id": "ex:parent"}]}},
                    {"@id": "ex:mentor", "rdfs:subPropertyOf": {"@id": "ex:guides"}},
                    {"@id": "ex:e0", "@type": "ex:GradStudent"}
                ]}),
            )
            .await
            .unwrap()
            .ledger;
        let program = json!({"reasoning": "owl2rl"});
        let index = fluree.register_reasoning_index(id, &program).await.unwrap();
        fluree.refresh_reasoning_index(id, &index).await.unwrap();

        let classes = [
            "ex:Student",
            "ex:GradStudent",
            "ex:Person",
            "ex:Org",
            "ex:Dept",
            "ex:Club",
        ];
        let properties = [
            "ex:advisor",
            "ex:partOf",
            "ex:memberOf",
            "ex:hasMember",
            "ex:teaches",
            "ex:worksFor",
            "ex:headOf",
            "ex:parent",
            "ex:knows",
        ];
        let mut rng = Lcg(0x5eed);
        let mut asserted: Vec<serde_json::Value> = Vec::new();
        let mut partitions = vec![bucket_count(&fluree, id, &index).await];
        let mut transitions = 0;
        for step in 0..24 {
            let mut facts = Vec::new();
            // Re-assert a fact already asserted.
            if !asserted.is_empty() {
                facts.push(asserted[rng.next(asserted.len())].clone());
            }
            // Assert a fact the index holds as derived (in a stable order,
            // so a seed always picks the same one).
            let mut derived: Vec<(Option<Sid>, Flake)> = persisted_derived(&fluree, id, &index)
                .await
                .into_iter()
                .collect();
            derived.sort_by_key(|(g, f)| format!("{g:?}{f:?}"));
            if !derived.is_empty() {
                let (_, flake) = &derived[rng.next(derived.len())];
                if let fluree_db_core::FlakeValue::Ref(o) = &flake.o {
                    let db = fluree.db(id).await.unwrap();
                    let decode = |sid: &Sid| db.snapshot.decode_sid(sid).unwrap();
                    facts.push(
                        json!({"@id": decode(&flake.s), decode(&flake.p): {"@id": decode(o)}}),
                    );
                    transitions += 1;
                }
            }
            for k in 0..8 {
                // Mostly recurring subjects; now and then a new term.
                let s = if rng.next(6) == 0 {
                    format!("ex:n{step}-{k}")
                } else {
                    format!("ex:e{}", rng.next(12))
                };
                if rng.next(3) == 0 {
                    facts.push(json!({"@id": s, "@type": classes[rng.next(classes.len())]}));
                } else {
                    let o = if rng.next(5) == 0 {
                        "ex:cs".to_string()
                    } else {
                        format!("ex:e{}", rng.next(12))
                    };
                    facts.push(
                        json!({"@id": s, properties[rng.next(properties.len())]: {"@id": o}}),
                    );
                }
            }
            asserted.extend(facts.iter().cloned());
            // Some windows span two commits.
            let split = if step % 3 == 1 { facts.len() / 2 } else { 0 };
            for part in [&facts[..split], &facts[split..]] {
                if !part.is_empty() {
                    ledger = fluree
                        .insert(ledger, &json!({"@context": ctx, "@graph": part}))
                        .await
                        .unwrap()
                        .ledger;
                }
            }
            // Some windows also insert into a graph the program does not
            // reason over.
            if step % 4 == 2 {
                ledger = fluree
                    .insert_turtle(
                        ledger,
                        &format!(
                            "@prefix ex: <http://example.org/> .\n\
                             GRAPH ex:side {{ ex:side{step} a ex:Student ; ex:partOf ex:e1 . }}"
                        ),
                    )
                    .await
                    .unwrap()
                    .ledger;
            }
            count(refresh_and_compare(&fluree, id, &index, &program, incremental, step).await);
            partitions.push(bucket_count(&fluree, id, &index).await);
        }

        // Conclusions of predicates no rule reads, already held: a derived
        // chain head asserted and then derived again another way, and a
        // super-property fact asserted before its sub-property fact.
        let mut step = 24;
        for facts in [
            json!([{"@id": "ex:q1", "ex:parent": {"@id": "ex:q2"}},
                   {"@id": "ex:q2", "ex:parent": {"@id": "ex:q3"}},
                   {"@id": "ex:q5", "ex:guides": {"@id": "ex:q6"}}]),
            json!([{"@id": "ex:q1", "ex:grandparent": {"@id": "ex:q3"}}]),
            json!([{"@id": "ex:q1", "ex:parent": {"@id": "ex:q4"}},
                   {"@id": "ex:q4", "ex:parent": {"@id": "ex:q3"}},
                   {"@id": "ex:q5", "ex:mentor": {"@id": "ex:q6"}}]),
        ] {
            ledger = fluree
                .insert(ledger, &json!({"@context": ctx, "@graph": facts}))
                .await
                .unwrap()
                .ledger;
            count(refresh_and_compare(&fluree, id, &index, &program, incremental, step).await);
            step += 1;
        }

        // A retraction is maintained by a full closure, and the next
        // insertion is incremental again over it.
        let gone = asserted[rng.next(asserted.len())].clone();
        ledger = fluree
            .update(
                ledger,
                &json!({"@context": ctx, "delete": gone.clone(), "where": gone}),
            )
            .await
            .unwrap()
            .ledger;
        refresh_and_compare(&fluree, id, &index, &program, Some(ClosureMode::Full), step).await;
        let _ledger = fluree
            .insert(
                ledger,
                &json!({"@context": ctx, "@graph": [
                    {"@id": "ex:e3", "ex:partOf": {"@id": "ex:e4"}},
                    {"@id": "ex:n-last", "@type": "ex:GradStudent"}
                ]}),
            )
            .await
            .unwrap()
            .ledger;
        count(refresh_and_compare(&fluree, id, &index, &program, incremental, step + 1).await);
        let db = fluree.db(id).await.unwrap();
        assert!(
            db.snapshot
                .graph_registry
                .assigned_graph_id("http://example.org/side")
                .is_some(),
            "the side graph is a named graph"
        );
        partitions.dedup();
        assert!(
            partitions.len() >= 3,
            "repartitioned as D grew: {partitions:?}"
        );
        assert!(transitions >= 20, "derived facts asserted: {transitions}");

        let reopened = instance();
        assert_eq!(
            whole_graph(&reopened, id, true).await,
            whole_graph(&fluree, id, false).await,
            "a second instance answers from the stored index"
        );
        full_closures
    }

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
