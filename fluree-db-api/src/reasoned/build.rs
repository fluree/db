//! Registering reasoned heads, and building and publishing their
//! generations.
//!
//! A generation is the branch at a basis commit S, every graph, merged with
//! the complete output C of the head's program at S:
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

use fluree_db_core::reasoned_head::{
    admit, Basis, BasisOracle, Candidate, Discard, ErasureEpoch, GenerationKind, GenerationStats,
    HeadEntry, HeadId, ReasonedHeads, ReasonedManifest, Registration, RegistrationState,
};
use fluree_db_core::{
    BranchedContentStore, ContentId, ContentKind, ContentStore, Flake, GraphId, IndexType,
    LedgerId, OverlayProvider, Sid,
};
use fluree_db_nameservice::{NsRecord, ReasonedHeadsCasResult};
use fluree_db_query::execute::{compute_derived_facts, QueryRequest};
use fluree_db_query::schema_bundle::SchemaBundleOverlay;
use serde_json::Value as JsonValue;

use super::derived::{decode_derived_only, encode_derived_only, encode_facts};
use super::program;
use crate::query::helpers::parse_jsonld_query;
use crate::view::GraphDb;
use crate::{ApiError, Fluree, Result};

const CAS_ATTEMPTS: usize = 16;

/// The materialization budget a worker runs under unless the program sets
/// one: generous, since a capped result is never published.
const WORKER_MAX_FACTS: u64 = 100_000_000;
const WORKER_MAX_SECONDS: u64 = 3_600;

/// What a refresh of one head did.
#[derive(Debug, Clone, PartialEq)]
pub enum RefreshOutcome {
    /// A new generation was published.
    Published {
        generation: u64,
        kind: GenerationKind,
        basis_t: i64,
        stats: GenerationStats,
    },
    /// The published generation is already at the branch's head.
    Current { generation: u64 },
    /// The result was not eligible to publish; nothing was published.
    Discarded { reason: String },
    /// The program could not be computed completely; nothing was published.
    Failed { reason: String },
    /// Another refresh of this head is under way.
    InFlight,
    /// The head already holds as many superseded generations inside the
    /// retention window as it may; publication waits for them to age out.
    Throttled { in_guard: usize },
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
    /// The branch's reasoned-heads document.
    pub async fn reasoned_heads(&self, ledger_id: &str) -> Result<ReasonedHeads> {
        let id = canonical_id(ledger_id)?;
        self.nameservice()
            .get_reasoned_heads(&id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("ledger {id} not found")))
    }

    async fn branch_record(&self, ledger_id: &str) -> Result<NsRecord> {
        self.nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("ledger {ledger_id} not found")))
    }

    /// Apply `change` to the branch's heads document by compare-and-swap,
    /// retrying from the document as it stands on a conflict.
    async fn update_reasoned_heads<T>(
        &self,
        ledger_id: &str,
        mut change: impl FnMut(&ReasonedHeads) -> Result<(Option<ReasonedHeads>, T)>,
    ) -> Result<T> {
        let record = self.branch_record(ledger_id).await?;
        let mut actual = self.reasoned_heads(ledger_id).await?;
        for _ in 0..CAS_ATTEMPTS {
            let (next, value) = change(&actual)?;
            let Some(mut next) = next else {
                return Ok(value);
            };
            next.v = actual.v + 1;
            match self
                .publisher()?
                .push_reasoned_heads_fenced(ledger_id, record.fence, actual.v, &next)
                .await?
            {
                ReasonedHeadsCasResult::Updated => return Ok(value),
                ReasonedHeadsCasResult::Conflict { actual: Some(now) } => actual = now,
                ReasonedHeadsCasResult::Conflict { actual: None } => {
                    return Err(ApiError::NotFound(format!("ledger {ledger_id} not found")))
                }
            }
        }
        Err(ApiError::internal(format!(
            "the reasoned-heads document of {ledger_id} kept changing; gave up after \
             {CAS_ATTEMPTS} attempts"
        )))
    }

    /// Register a reasoning program for persistence on a branch, returning
    /// its head's id. Registering a program the branch already has active is
    /// a no-op that returns the existing head.
    ///
    /// `program` carries the reasoning keys a query would (`reasoning`,
    /// `rules`, `ontology`, `reasoningTime`, `reasoningBudget`, `@context`)
    /// and optionally `graph`, the IRI of the graph to reason against. The
    /// head fills on the next [`Self::refresh_reasoned_heads`].
    pub async fn register_reasoned_head(
        &self,
        ledger_id: &str,
        program: &JsonValue,
    ) -> Result<HeadId> {
        let id = canonical_id(ledger_id)?;
        let canonical = program::canonical_program(program)?;
        let key = program::registration_key(&canonical);
        self.update_reasoned_heads(&id, |actual| {
            if let Some((head_id, _)) = actual.heads.iter().find(|(_, e)| {
                matches!(e.registration.state, RegistrationState::Active)
                    && e.registration.semantics == key
            }) {
                return Ok((None, head_id.clone()));
            }
            let head_id =
                HeadId::parse(&ulid::Ulid::new().to_string()).map_err(ApiError::internal)?;
            let mut next = actual.clone();
            next.heads.insert(
                head_id.clone(),
                HeadEntry {
                    registration: Registration {
                        revision: 1,
                        semantics: key.clone(),
                        program: canonical.clone(),
                        state: RegistrationState::Active,
                    },
                    floor: ErasureEpoch::default(),
                    published: None,
                },
            );
            Ok((Some(next), head_id))
        })
        .await
    }

    /// Retire a head: from now on workers are refused and queries stop being
    /// served from it. Its files stay until a cleanup pass removes them.
    pub async fn retire_reasoned_head(&self, ledger_id: &str, head_id: &HeadId) -> Result<()> {
        let id = canonical_id(ledger_id)?;
        let at_ms = now_ms();
        self.update_reasoned_heads(&id, |actual| {
            let Some(entry) = actual.heads.get(head_id) else {
                return Err(ApiError::NotFound(format!(
                    "reasoned head {head_id} is not registered on {id}"
                )));
            };
            if matches!(entry.registration.state, RegistrationState::Retired { .. }) {
                return Ok((None, ()));
            }
            let mut next = actual.clone();
            let entry = next.heads.get_mut(head_id).expect("present above");
            entry.registration.revision += 1;
            entry.registration.state = RegistrationState::Retired { at_ms };
            Ok((Some(next), ()))
        })
        .await?;
        self.reasoned.forget_head(&id, head_id);
        Ok(())
    }

    /// Bring every active head of the branch up to its current commit.
    pub async fn refresh_reasoned_heads(
        &self,
        ledger_id: &str,
    ) -> Result<Vec<(HeadId, RefreshOutcome)>> {
        let doc = self.reasoned_heads(ledger_id).await?;
        let mut out = Vec::new();
        for (head_id, entry) in &doc.heads {
            if matches!(entry.registration.state, RegistrationState::Active) {
                let outcome = self.refresh_reasoned_head(ledger_id, head_id).await?;
                out.push((head_id.clone(), outcome));
            }
        }
        Ok(out)
    }

    /// Bring one head up to the branch's current commit: compute its program,
    /// fold the result into a new generation, and publish it.
    pub async fn refresh_reasoned_head(
        &self,
        ledger_id: &str,
        head_id: &HeadId,
    ) -> Result<RefreshOutcome> {
        let id = canonical_id(ledger_id)?;
        let Some(_flight) = self.reasoned.begin(&id, head_id) else {
            return Ok(RefreshOutcome::InFlight);
        };
        self.refresh_head_exclusive(&id, head_id).await
    }

    async fn refresh_head_exclusive(
        &self,
        ledger_id: &str,
        head_id: &HeadId,
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
        let doc = self.reasoned_heads(ledger_id).await?;
        let Some(entry) = doc.heads.get(head_id) else {
            return Err(ApiError::NotFound(format!(
                "reasoned head {head_id} is not registered on {ledger_id}"
            )));
        };
        if !matches!(entry.registration.state, RegistrationState::Active) {
            return Ok(RefreshOutcome::Discarded {
                reason: "the head is retired".into(),
            });
        }
        let revision = entry.registration.revision;
        if let Some(published) = &entry.published {
            if published.revision == revision && published.basis == basis {
                return Ok(RefreshOutcome::Current {
                    generation: published.generation,
                });
            }
        }

        // Publication waits rather than let the head collector shorten the
        // protection of generations queries may still be reading.
        if let Some(published) = &entry.published {
            let retention = self.reasoned.retention();
            let head_cs = self
                .backend()
                .content_store(&record.storage_namespace().reasoned_head(head_id.as_str()));
            let generations = fluree_db_indexer::gc::heads::head_generations(
                head_cs.as_ref(),
                &published.manifest,
            )
            .await
            .map_err(|e| ApiError::internal(format!("read head chain: {e}")))?;
            let plan = fluree_db_indexer::gc::heads::plan_head_release(
                generations,
                now_ms(),
                retention.min_age_ms,
            );
            let in_guard = fluree_db_indexer::gc::heads::generations_in_guard(&plan);
            if in_guard >= retention.max_generations_in_guard {
                return Ok(RefreshOutcome::Throttled { in_guard });
            }
        }

        // Settle the program as a query would, against the reasoned graph.
        let program_json = &entry.registration.program;
        let db = match program_json.get("graph").and_then(|g| g.as_str()) {
            Some(iri) => match db.snapshot.graph_registry.existing_graph_id(iri, db.t) {
                Ok(Some(g_id)) => db.with_graph_id(g_id),
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

        // The previous generation, when this one can build on it.
        let branch_store = fluree_db_nameservice::branched_content_store_for_record(
            self.backend(),
            self.nameservice(),
            &record,
        )
        .await?;
        let head_ns = record.storage_namespace().reasoned_head(head_id.as_str());
        let head_cs = self.backend().content_store(&head_ns);
        let head_store: Arc<dyn ContentStore> = Arc::new(BranchedContentStore::with_parents(
            Arc::clone(&head_cs),
            vec![BranchedContentStore::leaf(Arc::clone(&branch_store))],
        ));
        let previous = match &entry.published {
            Some(p) if p.revision == revision => {
                match load_previous(head_cs.as_ref(), &p.manifest, &db).await {
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
                        tracing::warn!(head = %head_id, error = %e, "previous generation unreadable; rebuilding");
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

        // Build the merged root in the head's namespace.
        let mut head_record = record.clone();
        match &previous {
            None => {
                if let Some(base_root) = &record.index_head_id {
                    self.copy_index_to_branch(ledger_id, &head_ns, base_root)
                        .await?;
                }
            }
            Some(prev) => {
                head_record.index_head_id = Some(prev.manifest.root.clone());
                head_record.index_t = prev.manifest.basis.t;
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
                    Arc::clone(&head_store),
                    &head_record,
                    config,
                )
                .await
            }
            Some(_) => {
                fluree_db_indexer::fold_index_for_record(
                    Arc::clone(&head_store),
                    &head_record,
                    config,
                )
                .await
            }
        }
        .map_err(|e| ApiError::internal(format!("reasoned-head build: {e}")))?;

        let derived_only_flakes: Vec<Flake> = derived_only
            .iter()
            .cloned()
            .map(|f| to_flake(f, true))
            .collect();
        let derived_count = derived_only_flakes.len() as u64;
        let derived_id = head_cs
            .put(
                ContentKind::DerivedFacts,
                &encode_derived_only(basis.t, derived_only_flakes)?,
            )
            .await
            .map_err(|e| ApiError::internal(format!("write derived-only set: {e}")))?;

        let stats = GenerationStats {
            derived_only: derived_count,
            derived_total,
            added: stats_added,
            removed: stats_removed,
        };
        let candidate = Candidate {
            head_id: head_id.clone(),
            kind,
            revision,
            basis: basis.clone(),
        };
        let manifest = ReasonedManifest {
            head_id: head_id.clone(),
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
        match self
            .publish_generation(
                ledger_id,
                head_cs.as_ref(),
                branch_store.as_ref(),
                &candidate,
                manifest,
            )
            .await?
        {
            Ok(generation) => {
                self.reasoned.forget_head(ledger_id, head_id);
                if let Err(e) = self
                    .collect_head(ledger_id, head_id, head_cs.as_ref())
                    .await
                {
                    tracing::warn!(head = %head_id, error = %e, "reasoned-head collection failed");
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

    /// Set how long superseded generations are kept, and how many a head
    /// may hold inside that window before publication waits.
    pub fn set_reasoned_head_retention(&self, retention: super::HeadRetention) {
        self.reasoned.set_retention(retention);
    }

    /// Release a head's superseded generations once the retention window has
    /// passed since each was superseded. Shared dictionary blobs are left for
    /// the sweep, which sees every participant at once.
    async fn collect_head(
        &self,
        ledger_id: &str,
        head_id: &HeadId,
        head_cs: &dyn ContentStore,
    ) -> Result<()> {
        let doc = self.reasoned_heads(ledger_id).await?;
        let Some(pointer) = doc.heads.get(head_id).and_then(|e| e.published.as_ref()) else {
            return Ok(());
        };
        let generations =
            fluree_db_indexer::gc::heads::head_generations(head_cs, &pointer.manifest)
                .await
                .map_err(|e| ApiError::internal(format!("read head chain: {e}")))?;
        let plan = fluree_db_indexer::gc::heads::plan_head_release(
            generations,
            now_ms(),
            self.reasoned.retention().min_age_ms,
        );
        let released = fluree_db_indexer::gc::heads::release_head_generations(head_cs, &plan, None)
            .await
            .map_err(|e| ApiError::internal(format!("release head generations: {e}")))?;
        if released.generations > 0 {
            tracing::debug!(head = %head_id, ?released, "released superseded head generations");
        }
        Ok(())
    }

    /// The branch record and a view of the branch at exactly its commit
    /// head.
    async fn consistent_base(&self, ledger_id: &str) -> Result<(NsRecord, GraphDb)> {
        for _ in 0..CAS_ATTEMPTS {
            let record = self.branch_record(ledger_id).await?;
            let db = self.db(ledger_id).await?;
            if db.t == record.commit_t {
                return Ok((record, db));
            }
        }
        Err(ApiError::internal(format!(
            "{ledger_id} kept moving while its reasoned heads were refreshed"
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
        if let Some(diag) = &outcome.diagnostics {
            if diag.capped {
                return Ok(Err(format!(
                    "the closure was capped ({}); raise the program's reasoningBudget",
                    diag.capped_reason.as_deref().unwrap_or("budget")
                )));
            }
        } else if reasoning.modes.owl2rl {
            return Ok(Err("OWL 2 RL materialization failed".into()));
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
    /// heads document as it stands, write the manifest with its place in the
    /// chain, and compare-and-swap, retrying from the actual document.
    async fn publish_generation(
        &self,
        ledger_id: &str,
        head_cs: &dyn ContentStore,
        branch_store: &dyn ContentStore,
        candidate: &Candidate,
        mut manifest: ReasonedManifest,
    ) -> Result<std::result::Result<u64, Discard>> {
        for _ in 0..CAS_ATTEMPTS {
            let record = self.branch_record(ledger_id).await?;
            let actual = self.reasoned_heads(ledger_id).await?;
            let published_t = actual
                .heads
                .get(&candidate.head_id)
                .and_then(|e| e.published.as_ref())
                .map(|p| p.basis.t)
                .unwrap_or(candidate.basis.t);
            let line = match &record.commit_head_id {
                Some(head) => {
                    Line::walk(branch_store, head, published_t.min(candidate.basis.t)).await?
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
            let manifest_id = head_cs
                .put(ContentKind::ReasonedManifest, &bytes)
                .await
                .map_err(|e| ApiError::internal(format!("write manifest: {e}")))?;
            let mut next = actual.clone();
            next.v = actual.v + 1;
            next.heads
                .get_mut(&candidate.head_id)
                .expect("admitted, so present")
                .published = Some(manifest.pointer(manifest_id));
            match self
                .publisher()?
                .push_reasoned_heads_fenced(ledger_id, record.fence, actual.v, &next)
                .await?
            {
                ReasonedHeadsCasResult::Updated => return Ok(Ok(manifest.generation)),
                ReasonedHeadsCasResult::Conflict { .. } => continue,
            }
        }
        Err(ApiError::internal(format!(
            "the reasoned-heads document of {ledger_id} kept changing; gave up publishing"
        )))
    }
}

/// A previous generation, as the next one builds on it.
struct Previous {
    manifest: ReasonedManifest,
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
    let manifest = ReasonedManifest::decode(&bytes).map_err(ApiError::internal)?;
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
    let derived_bytes = store
        .get(&manifest.derived)
        .await
        .map_err(|e| ApiError::internal(format!("read derived-only set: {e}")))?;
    let derived = decode_derived_only(&derived_bytes)?
        .into_iter()
        .map(|flake| (graph_of(db, flake.g.as_ref()), flake))
        .collect();
    Ok(Previous { manifest, derived })
}

/// The id of a graph whose IRI the branch no longer registers: no fact of
/// the current state can be in it.
const UNREGISTERED_GRAPH: GraphId = GraphId::MAX;

/// Each graph's Sid in flake form: `None` for the default graph.
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
struct Line {
    commits: HashMap<ContentId, i64>,
    floor_t: i64,
}

impl Line {
    async fn walk(store: &dyn ContentStore, head: &ContentId, down_to_t: i64) -> Result<Self> {
        let stop = (down_to_t - 1).max(0);
        let commits = fluree_db_core::collect_first_parent_cids(store, head, stop)
            .await
            .map_err(|e| ApiError::internal(format!("walk commits: {e}")))?;
        Ok(Self {
            commits: commits.into_iter().map(|(t, cid)| (cid, t)).collect(),
            floor_t: down_to_t,
        })
    }

    fn on_line(&self, basis: &Basis) -> bool {
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
