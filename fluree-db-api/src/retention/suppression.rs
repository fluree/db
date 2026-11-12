//! Refusing reads that need removed history.
//!
//! A read as of the head is answered from current state, which retention
//! never hides beyond what was retracted. A read as of an earlier point, or a
//! history range, may need an occurrence a targeted removal took out; when
//! its patterns could touch one at a covered time, it fails with
//! [`ApiError::HistoryUnavailable`] instead of answering from incomplete
//! history. `f:noHistory` refuses nothing: its property is read from what
//! the index and novelty still hold. The check runs once per request, while
//! the executable is built, and reads only the journal: a ledger without one
//! reuses its known-empty result until a configuration event invalidates it.
//! Backends without a complete local change stream still revalidate.

use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_core::{ContentId, LedgerSnapshot};
use fluree_db_nameservice::NsRecord;
use fluree_db_query::ir::walk::for_each_pattern;
use fluree_db_query::ir::{Pattern, Query, Ref};

use super::journal::{Journal, Removal};
use super::registry;
use super::watch::{Check, Seen};
use crate::{ApiError, Fluree, Result};

/// What a read can touch.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Reads {
    /// Any predicate (a variable predicate, a wildcard path or crawl,
    /// reasoning).
    Everything,
    /// These `(subject, predicate)` IRIs; a variable subject is `None`.
    Facts(Vec<(Option<String>, String)>),
}

/// What `query` reads, in IRIs: every pattern it evaluates, those in its
/// solution modifiers included.
pub(crate) fn reads_of(query: &Query, snapshot: &LedgerSnapshot) -> Reads {
    if query.output.has_hydration() {
        return Reads::Everything;
    }
    let mut facts = Vec::new();
    let mut bounded = true;
    for_each_pattern(query, &mut |pattern| {
        bounded &= read_by(pattern, snapshot, &mut facts);
    });
    if bounded {
        Reads::Facts(facts)
    } else {
        Reads::Everything
    }
}

fn iri(r: &Ref, snapshot: &LedgerSnapshot) -> Option<String> {
    match r {
        Ref::Var(_) => None,
        Ref::Sid(sid) => snapshot.decode_sid(sid),
        Ref::Iri(iri) => Some(iri.to_string()),
    }
}

/// Collect the facts `pattern` itself reads, not those of the patterns
/// nested in it; `false` when it can read anything.
fn read_by(
    pattern: &Pattern,
    snapshot: &LedgerSnapshot,
    out: &mut Vec<(Option<String>, String)>,
) -> bool {
    match pattern {
        Pattern::Triple(tp) => match iri(&tp.p, snapshot) {
            Some(p) => {
                out.push((iri(&tp.s, snapshot), p));
                true
            }
            None => false,
        },
        Pattern::EdgeAnnotation { edge, .. } => match iri(&edge.p, snapshot) {
            Some(p) => {
                out.push((iri(&edge.s, snapshot), p));
                out.push((None, fluree_vocab::rdf::REIFIES.to_string()));
                true
            }
            None => false,
        },
        // Its values may be a list's members, read through the list's cells.
        Pattern::ListView(lv) => match iri(&lv.triple.p, snapshot) {
            Some(p) => {
                out.push((iri(&lv.triple.s, snapshot), p));
                out.push((None, fluree_vocab::rdf::FIRST.to_string()));
                out.push((None, fluree_vocab::rdf::REST.to_string()));
                true
            }
            None => false,
        },
        // A path's steps past the first, and an inverse step, read subjects
        // other than the constant one: its predicates count for every
        // subject.
        Pattern::PropertyPath(path) => {
            if path.wildcard || !path.sequence_steps.is_empty() {
                false
            } else {
                path.predicates
                    .iter()
                    .all(|p| match snapshot.decode_sid(p) {
                        Some(p) => {
                            out.push((None, p));
                            true
                        }
                        None => false,
                    })
            }
        }
        Pattern::Optional(_)
        | Pattern::Minus(_)
        | Pattern::Exists(_)
        | Pattern::NotExists(_)
        | Pattern::DefaultGraphSource { .. }
        | Pattern::Graph { .. }
        | Pattern::Union(_)
        | Pattern::Subquery(_)
        | Pattern::Filter(_)
        | Pattern::Bind { .. }
        | Pattern::Unwind { .. }
        | Pattern::Values { .. }
        | Pattern::TermComponents(_)
        | Pattern::IndexSearch(_)
        | Pattern::VectorSearch(_)
        | Pattern::R2rml(_) => true,
        Pattern::ShortestPath(_)
        | Pattern::GeoSearch(_)
        | Pattern::S2Search(_)
        | Pattern::Service(_) => false,
    }
}

/// The search graph sources `query` consults.
pub(crate) fn search_sources(query: &Query) -> Vec<String> {
    let mut out = Vec::new();
    for_each_pattern(query, &mut |pattern| match pattern {
        Pattern::IndexSearch(p) => out.push(p.graph_source_id.clone()),
        Pattern::VectorSearch(p) => out.push(p.graph_source_id.clone()),
        _ => {}
    });
    out.sort();
    out.dedup();
    out
}

/// A historical read of one ledger.
pub(crate) struct HistoricalRead<R> {
    /// As-of point (the end of a history range).
    pub(crate) to_t: i64,
    /// The start of a history range; `None` for a point-in-time read.
    pub(crate) from_t: Option<i64>,
    /// What it reads, worked out only for a ledger with a removal.
    pub(crate) reads: R,
}

/// A journal with the removals that apply to its branch looked up by what
/// a read names.
pub(crate) struct Suppression {
    pub(crate) journal: Arc<Journal>,
    /// The journal's identity as its record names it.
    identity: String,
    /// The branch as it stood when this was worked out; see [`Key`].
    key: Key,
    processed_until: i64,
    /// Whether the branch holds each removal's commit, worked out at
    /// `head_t`.
    applies: Arc<Vec<bool>>,
    head_t: i64,
    /// The removals whose commit the branch holds, by predicate, as indexes
    /// into the journal's removals. One whose commit the branch does not
    /// hold (merged from another copy of the lineage) is left out.
    removed: Arc<HashMap<String, RemovedOf>>,
}

/// What a cached [`Suppression`] was worked out from: the branch's
/// incarnation, its configuration revision (`None` when not known, which
/// is read again every time) and its history (see
/// [`Seen::Live::history`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Key {
    fence: Option<fluree_db_nameservice::Fence>,
    instance: u128,
    config_version: Option<i64>,
    history: u64,
}

impl Key {
    /// The same branch and history, whatever its configuration revision.
    fn same_history(&self, other: &Key) -> bool {
        (self.fence, self.instance, self.history) == (other.fence, other.instance, other.history)
    }
}

/// The removals of one predicate that apply to a branch.
#[derive(Default)]
struct RemovedOf {
    /// By their subject's fingerprint.
    by_subject: HashMap<String, Vec<usize>>,
    /// Those whose subject is not recorded, which any subject may be.
    any_subject: Vec<usize>,
}

fn removed_by_predicate(journal: &Journal, applies: &[bool]) -> HashMap<String, RemovedOf> {
    let mut removed: HashMap<String, RemovedOf> = HashMap::new();
    for (i, removal) in journal.removals.iter().enumerate() {
        if !applies.get(i).copied().unwrap_or(true) {
            continue;
        }
        let of = removed.entry(removal.predicate.clone()).or_default();
        match &removal.subject {
            Some(subject) => of.by_subject.entry(subject.clone()).or_default().push(i),
            None => of.any_subject.push(i),
        }
    }
    removed
}

impl Suppression {
    #[cfg(test)]
    fn new(journal: Journal, identity: String, applies: &[bool]) -> Self {
        Suppression {
            removed: Arc::new(removed_by_predicate(&journal, applies)),
            journal: Arc::new(journal),
            identity,
            key: Key {
                fence: None,
                instance: 0,
                config_version: None,
                history: 0,
            },
            processed_until: 0,
            applies: Arc::new(applies.to_vec()),
            head_t: 0,
        }
    }

    /// A removal whose commit the branch did not hold when this was worked
    /// out: commits added since may bring it.
    fn awaits_commits(&self) -> bool {
        self.applies.iter().any(|a| !a)
    }

    /// The predicate of a removal that a read of `reads`, as of `to` (or
    /// over `[from, to]` when not a `point`), could need.
    fn removal_needed(&self, reads: &Reads, from: i64, to: i64, point: bool) -> Option<&str> {
        let covered = |i: &usize| {
            let removal = &self.journal.removals[*i];
            if point {
                removal.covers(to)
            } else {
                removal.overlaps(from, to)
            }
        };
        let any = |of: &RemovedOf| {
            of.any_subject.iter().any(covered) || of.by_subject.values().flatten().any(covered)
        };
        match reads {
            Reads::Everything => self
                .removed
                .iter()
                .find(|(_, of)| any(of))
                .map(|(p, _)| p.as_str()),
            Reads::Facts(facts) => facts.iter().find_map(|(subject, predicate)| {
                let (predicate, of) = self.removed.get_key_value(predicate)?;
                let needed = match subject {
                    None => any(of),
                    Some(iri) => {
                        of.any_subject.iter().any(covered)
                            || of
                                .by_subject
                                .get(&super::journal::subject_fingerprint(iri))
                                .is_some_and(|spans| spans.iter().any(covered))
                    }
                };
                needed.then_some(predicate.as_str())
            }),
        }
    }
}

/// How a view stands against its ledger's head.
pub(crate) enum Freshness {
    /// At the head: current state.
    Current,
    /// Behind it. `journal` is what the ledger's journal holds, where
    /// known.
    Behind {
        journal: Option<super::watch::Holds>,
    },
}

/// Learn `ledger_id` again for `watch`: its record, and what its journal
/// holds where the record does not tell.
async fn revalidate(
    watch: &super::watch::BranchWatch,
    ns: &crate::NameServiceMode,
    ledger_id: &str,
) -> Result<()> {
    use fluree_db_nameservice::NameServiceLookup;
    let token = watch.token();
    let found: Result<_> = async {
        let record = ns.lookup(ledger_id).await?;
        // A record that names no revision tells nothing of a change to
        // the journal: read it.
        let holds = match &record {
            Some(r) if r.config_version.is_none() => {
                Some(match registry::read_stored(ns, ledger_id).await?.1 {
                    Some(stored) => registry::journal_holds(&stored),
                    None => super::watch::Holds::Nothing,
                })
            }
            _ => None,
        };
        Ok((record, holds))
    }
    .await;
    watch.revalidated(
        ledger_id,
        found.as_ref().ok().map(|(r, h)| (r.as_ref(), *h)),
        token,
    );
    found.map(|_| ())
}

/// Suppressions held between reads, by branch. One is reused only for a
/// journal of the same identity, which its record names without its chunks
/// being read: a ledger created again under a dropped one's name starts its
/// generations over.
#[derive(Default)]
pub(crate) struct SuppressionCache {
    by_ledger: parking_lot::Mutex<HashMap<String, Arc<Suppression>>>,
}

impl Fluree {
    /// Where `db` stands against what this instance knows of its ledger.
    /// A view of a branch since dropped, or replaced by another incarnation
    /// under its name, is refused: what that branch suppressed is no longer
    /// recorded anywhere. Takes no ledger lock, so a query issued inside a
    /// transaction can ask, and reads the nameservice only for a branch this
    /// instance has not followed yet.
    pub(crate) async fn view_freshness(&self, db: &crate::GraphDb) -> Result<Freshness> {
        if db.graph_source_id.is_some() || db.over_graph_source {
            return Ok(Freshness::Current);
        }
        let watch = &self.retention.watch;
        let gone = || {
            ApiError::NotFound(format!(
                "{}: the ledger this view reads has been dropped",
                db.ledger_id
            ))
        };
        let mut check = watch.check(&db.ledger_id, db.fence);
        // A view at the head this instance knows reads current state, as
        // every read it makes does. One behind it reads an earlier point,
        // which what is kept decides only while it is not stale.
        let behind = matches!(check, Check::Known(Seen::Live { head_t, .. }) if db.historical || db.t < head_t);
        if behind && self.revalidate_if_stale(&db.ledger_id).await? {
            check = watch.check(&db.ledger_id, db.fence);
        } else if matches!(check, Check::Known(_)) {
            self.revalidate_if_due(&db.ledger_id);
        }
        let seen = match check {
            Check::FenceDropped => return Err(gone()),
            Check::Known(seen) => seen,
            Check::Unknown => {
                let Ok(id) = fluree_db_core::LedgerId::parse(&db.ledger_id) else {
                    return Ok(Freshness::Current);
                };
                let token = watch.token();
                let record = self.nameservice().lookup(id.as_str()).await?;
                watch.learn(id.as_str(), record.as_ref(), token);
                record.as_ref().map_or(Seen::Absent, Seen::live)
            }
        };
        match seen {
            Seen::Dropped => Err(gone()),
            // Not a ledger here: one of another instance, or none at all.
            Seen::Absent => Ok(Freshness::Current),
            Seen::Live {
                fence,
                instance,
                head_t,
                journal,
                ..
            } => {
                let other_fence = db.fence.is_some() && fence.is_some() && db.fence != fence;
                let other_instance =
                    db.instance_bits != 0 && instance != 0 && instance != db.instance_bits;
                if other_fence || other_instance {
                    return Err(gone());
                }
                Ok(if db.t >= head_t {
                    Freshness::Current
                } else {
                    Freshness::Behind { journal }
                })
            }
        }
    }

    /// Learn `ledger_id` again in the background when what is kept of it is
    /// due (see [`super::watch::BranchWatch::revalidation_due`]). The read
    /// that asked serves what is kept.
    fn revalidate_if_due(&self, ledger_id: &str) {
        let watch = &self.retention.watch;
        if !watch.revalidation_due(ledger_id) {
            return;
        }
        let watch = Arc::clone(watch);
        let ns = self.nameservice_mode.clone();
        let ledger_id = ledger_id.to_string();
        crate::wasm_compat::spawn_detached(async move {
            let _ = revalidate(&watch, &ns, &ledger_id).await;
        });
    }

    /// Learn `ledger_id` again before the read that asked goes on, when
    /// what is kept of it is stale (see
    /// [`super::watch::BranchWatch::stale`]); in the background when it is
    /// only due. Whether it was learned before returning.
    async fn revalidate_if_stale(&self, ledger_id: &str) -> Result<bool> {
        let watch = &self.retention.watch;
        if !watch.stale(ledger_id) {
            self.revalidate_if_due(ledger_id);
            return Ok(false);
        }
        revalidate(watch, &self.nameservice_mode, ledger_id).await?;
        Ok(true)
    }

    /// The branch's suppression, using this node's replicated journal.
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) async fn suppression_for(
        &self,
        ledger_id: &str,
    ) -> Result<Option<Arc<Suppression>>> {
        self.suppression_holding(ledger_id, false).await
    }

    /// The branch's suppression when its journal holds a removal: what a
    /// read is refused for.
    pub(crate) async fn removals_for(&self, ledger_id: &str) -> Result<Option<Arc<Suppression>>> {
        self.suppression_holding(ledger_id, true).await
    }

    async fn suppression_holding(
        &self,
        ledger_id: &str,
        removals: bool,
    ) -> Result<Option<Arc<Suppression>>> {
        use super::watch::Holds;
        self.ensure_read_ready(ledger_id).await?;
        let ns = &self.nameservice_mode;
        let watch = &self.retention.watch;
        let mut check = watch.check(ledger_id, None);
        if matches!(check, Check::Unknown) {
            let token = watch.token();
            let record = self.nameservice().lookup(ledger_id).await?;
            watch.learn(ledger_id, record.as_ref(), token);
            check = watch.check(ledger_id, None);
        } else if self.revalidate_if_stale(ledger_id).await? {
            check = watch.check(ledger_id, None);
        }
        let token = watch.token();
        let (key, head_t) = match check {
            Check::Known(Seen::Live {
                journal: Some(holds),
                ..
            }) if holds == Holds::Nothing || (removals && !holds.removals()) => return Ok(None),
            Check::Known(Seen::Live {
                fence,
                instance,
                head_t,
                config_version,
                history,
                ..
            }) => (
                Key {
                    fence,
                    instance,
                    config_version,
                    history,
                },
                head_t,
            ),
            // Dropped, or never created: whatever its record holds.
            _ => (
                Key {
                    fence: None,
                    instance: 0,
                    config_version: None,
                    history: 0,
                },
                i64::MAX,
            ),
        };
        let cache = &self.retention.suppressions;
        let cached = cache.by_ledger.lock().get(ledger_id).cloned();
        if let Some(cached) = &cached {
            if key.config_version.is_some() && cached.key == key {
                return self
                    .with_commits_since(ledger_id, cached, head_t)
                    .await
                    .map(Some);
            }
        }
        let (config, stored) = registry::read_stored(ns, ledger_id).await?;
        if let Some(expected) = key.config_version {
            if config.as_ref().map_or(0, |c| c.v) < expected {
                return Err(ApiError::ReplicaNotCurrent(format!(
                    "{ledger_id}'s journal is older than configuration revision {expected}, which this node has already learned; retry"
                )));
            }
        }
        let holds = stored
            .as_ref()
            .map_or(Holds::Nothing, registry::journal_holds);
        watch.learn_journal(ledger_id, holds, token);
        let Some(stored) = stored.filter(|_| holds != Holds::Nothing) else {
            return Ok(None);
        };
        // Revision of the record this suppression is good for.
        let key = Key {
            config_version: key.config_version.filter(|_| {
                config
                    .as_ref()
                    .is_some_and(|c| Some(c.v) == key.config_version)
            }),
            ..key
        };
        let identity = registry::journal_identity(&stored);
        let processed_until = registry::shell(&stored)?.watermark_t + 1;
        let suppression = match cached
            .filter(|c| c.identity == identity && c.key.same_history(&key))
        {
            // A configuration change that left the journal as it was (a
            // retention pass's bookkeeping): what was worked out stands.
            Some(cached) => {
                let reused = Arc::new(Suppression {
                    journal: Arc::clone(&cached.journal),
                    identity,
                    key,
                    processed_until,
                    applies: Arc::clone(&cached.applies),
                    head_t: cached.head_t,
                    removed: Arc::clone(&cached.removed),
                });
                self.with_commits_since(ledger_id, &reused, head_t).await?
            }
            None => {
                let journal = registry::resolve_journal(&stored, self.registry_objects()).await?;
                let applies = self.removals_applying(ledger_id, &journal, None).await;
                Arc::new(Suppression {
                    removed: Arc::new(removed_by_predicate(&journal, &applies)),
                    journal: Arc::new(journal),
                    identity,
                    key,
                    processed_until,
                    applies: Arc::new(applies),
                    head_t,
                })
            }
        };
        cache
            .by_ledger
            .lock()
            .insert(ledger_id.to_string(), Arc::clone(&suppression));
        if removals && suppression.journal.removals.is_empty() {
            return Ok(None);
        }
        Ok(Some(suppression))
    }

    /// `cached`, as of a head at `head_t`: commits added since it was
    /// worked out may hold the commit of a removal it found not to apply,
    /// so those, and only those, are looked at again.
    async fn with_commits_since(
        &self,
        ledger_id: &str,
        cached: &Arc<Suppression>,
        head_t: i64,
    ) -> Result<Arc<Suppression>> {
        if head_t <= cached.head_t || !cached.awaits_commits() {
            return Ok(Arc::clone(cached));
        }
        let applies = self
            .removals_applying(ledger_id, &cached.journal, Some(&cached.applies))
            .await;
        let suppression = Arc::new(Suppression {
            removed: Arc::new(removed_by_predicate(&cached.journal, &applies)),
            journal: Arc::clone(&cached.journal),
            identity: cached.identity.clone(),
            key: cached.key,
            processed_until: cached.processed_until,
            applies: Arc::new(applies),
            head_t,
        });
        self.retention
            .suppressions
            .by_ledger
            .lock()
            .insert(ledger_id.to_string(), Arc::clone(&suppression));
        Ok(suppression)
    }

    /// Whether each removal of `journal` applies to `ledger_id`: whether the
    /// branch holds its commit. Reads only storage and the nameservice,
    /// never the cached ledger, so a query issued inside a transaction can
    /// ask. Whatever cannot be told applies.
    /// `known` holds what was found before: a removal found to apply still
    /// does, and is not looked at again.
    async fn removals_applying(
        &self,
        ledger_id: &str,
        journal: &Journal,
        known: Option<&[bool]>,
    ) -> Vec<bool> {
        let all = || vec![true; journal.removals.len()];
        let Ok(Some(record)) = self.nameservice().lookup(ledger_id).await else {
            return all();
        };
        let Ok(store) = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await
        else {
            return all();
        };
        let mut held: HashMap<&str, bool> = HashMap::new();
        let mut out = Vec::with_capacity(journal.removals.len());
        for (i, removal) in journal.removals.iter().enumerate() {
            if known.and_then(|k| k.get(i)).copied() == Some(true) {
                out.push(true);
                continue;
            }
            let applies = match held.get(removal.commit.as_str()) {
                Some(applies) => *applies,
                None => {
                    let applies = match removal.commit.parse::<ContentId>() {
                        Ok(cid) => self
                            .holds_commit(&record, store.as_ref(), &cid)
                            .await
                            .unwrap_or(true),
                        Err(_) => true,
                    };
                    held.insert(&removal.commit, applies);
                    applies
                }
            };
            out.push(applies);
        }
        out
    }

    /// Whether `ledger_id` holds commit `cid`, as a walk of its history
    /// asks: a read of it may return an erased commit it does not hold
    /// (see [`Self::holds_commit`]).
    pub async fn ledger_holds_commit(&self, ledger_id: &str, cid: &ContentId) -> Result<bool> {
        let record = self
            .nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
        let store = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await?;
        self.holds_commit(&record, store.as_ref(), cid).await
    }

    /// Whether `record`'s storage, or an ancestor branch's, holds `cid`.
    /// A read of an erased commit returns its replacement wherever it is
    /// made, so an erased one is held where its original's file is, or was
    /// until a purge deleted it: erasing a commit does not change which
    /// branches its removals apply to.
    async fn holds_commit(
        &self,
        #[cfg_attr(not(feature = "native"), allow(unused_variables))] record: &NsRecord,
        store: &dyn fluree_db_core::ContentStore,
        cid: &ContentId,
    ) -> Result<bool> {
        #[cfg(feature = "native")]
        if let Some(runtime) = self.retention.erasure.get() {
            if runtime.index.erased(cid).await?.is_some() {
                let mut next = Some(record.clone());
                let mut seen = std::collections::HashSet::new();
                while let Some(branch) = next {
                    // Sources that form a cycle, as only corrupt records do.
                    if !seen.insert(branch.ledger_id.clone()) {
                        break;
                    }
                    if runtime
                        .index
                        .holds_original(&branch.storage_namespace(), cid)
                        .await?
                    {
                        return Ok(true);
                    }
                    next = match &branch.source_branch {
                        Some(source) => {
                            let parent = fluree_db_core::format_ledger_id(&branch.name, source);
                            self.nameservice().lookup(&parent).await?
                        }
                        None => None,
                    };
                }
                return Ok(false);
            }
        }
        Ok(store.has(cid).await?)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// The history an index build of `ledger_id` leaves out: its
    /// `f:noHistory` rules, through the retention worker's persisted
    /// watermark only. A config commit after it may have turned a rule off,
    /// and the journal does not know yet.
    pub(crate) async fn suppressed_history_for_index(
        &self,
        ledger_id: &str,
    ) -> Result<Vec<fluree_db_indexer::SuppressedHistory>> {
        let Some(suppression) = self.suppression_for(ledger_id).await? else {
            return Ok(Vec::new());
        };
        let processed_until = suppression.processed_until;
        Ok(suppression
            .journal
            .history_rules
            .iter()
            .filter(|rule| rule.from_t < processed_until)
            .map(|rule| fluree_db_indexer::SuppressedHistory {
                predicate_iri: rule.predicate.clone(),
                graph_iri: rule.graph.clone(),
                except_graph_iris: rule.except_graphs.clone(),
                from_t: rule.from_t,
                until_t: Some(
                    rule.until_t
                        .map_or(processed_until, |t| t.min(processed_until)),
                ),
            })
            .collect())
    }

    /// Refuse a search of a graph source that may still hold what its
    /// source ledger's removals took out: one built before a removal it has
    /// not yet indexed, or any earlier snapshot of it (`historical`). Only
    /// removals of properties the index may read count.
    pub(crate) async fn refuse_stale_search_sources(
        &self,
        query: &Query,
        historical: bool,
    ) -> Result<()> {
        let watch = &self.retention.watch;
        for id in search_sources(query) {
            // Ledgers known to hold no removal refuse nothing: neither the
            // graph source nor its configuration is read.
            if let Some(dependencies) = watch.source_dependencies(&id) {
                if dependencies
                    .iter()
                    .all(|ledger_id| self.holds_no_removal(ledger_id))
                {
                    continue;
                }
            }
            let token = watch.token();
            let Some(record) = self.nameservice().lookup_graph_source(&id).await? else {
                continue;
            };
            let dependencies: Vec<String> = record
                .dependencies
                .iter()
                .filter_map(|d| fluree_db_core::LedgerId::parse(d).ok())
                .map(|d| d.to_string())
                .collect();
            watch.learn_source(&id, dependencies.clone(), token);
            let indexed = crate::graph_source::helpers::indexed_properties(&record);
            for ledger_id in dependencies {
                // A current search is a current-state read: like one of a
                // ledger, it is checked against the journal this replica
                // holds, without waiting on the cluster.
                let Some(suppression) = self.removals_for(&ledger_id).await? else {
                    continue;
                };
                let journal = &suppression.journal;
                let removals: Vec<&Removal> = journal
                    .removals
                    .iter()
                    .filter(|r| indexed.may_read(&r.predicate))
                    .collect();
                if removals.is_empty() {
                    continue;
                }
                if historical {
                    return Err(ApiError::HistoryUnavailable(format!(
                        "search index {id} is read as of an earlier point, and {ledger_id} \
                         has removed history it may hold; search it as of the current head"
                    )));
                }
                let removed_through = removals
                    .iter()
                    .map(|r| r.until_t.unwrap_or(r.scope_t))
                    .max();
                if removed_through.is_some_and(|t| record.index_t < t) {
                    return Err(ApiError::HistoryUnavailable(format!(
                        "search index {id} has indexed {ledger_id} through t={}, before \
                         a removal it must reflect; retry once it has synced",
                        record.index_t
                    )));
                }
            }
        }
        Ok(())
    }

    /// Whether this instance knows `ledger_id` (canonically spelled) to
    /// hold no removal, without reading anything. What is kept of it once
    /// stale tells nothing.
    fn holds_no_removal(&self, ledger_id: &str) -> bool {
        let watch = &self.retention.watch;
        let known = matches!(
            watch.check(ledger_id, None),
            Check::Known(Seen::Live {
                journal: Some(holds),
                ..
            }) if !holds.removals()
        ) && !watch.stale(ledger_id);
        if known {
            self.revalidate_if_due(ledger_id);
        }
        known
    }

    /// Refuse `read` of `ledger_id` when it needs suppressed history.
    pub(crate) async fn refuse_suppressed_history(
        &self,
        ledger_id: &str,
        read: HistoricalRead<impl FnOnce() -> Reads>,
    ) -> Result<()> {
        let Some(suppression) = self.removals_for(ledger_id).await? else {
            return Ok(());
        };
        let head_t = match self.retention.watch.check(ledger_id, None) {
            Check::Known(Seen::Live { head_t, .. }) => head_t,
            _ => match self.nameservice().lookup(ledger_id).await? {
                Some(record) => record.commit_t,
                None => {
                    return Err(ApiError::NotFound(format!(
                        "{ledger_id}: the ledger this read needs has been dropped"
                    )))
                }
            },
        };
        if read.from_t.is_none() && read.to_t >= head_t {
            return Ok(());
        }
        let (from, to) = (read.from_t.unwrap_or(read.to_t), read.to_t);
        let point = read.from_t.is_none();
        if let Some(predicate) = suppression.removal_needed(&(read.reads)(), from, to, point) {
            return Err(ApiError::HistoryUnavailable(format!(
                "a removed occurrence of <{predicate}> on {ledger_id} falls in the requested range"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::journal::{subject_fingerprint, Removal};

    #[tokio::test]
    async fn a_peer_never_caches_an_empty_journal_older_than_its_record() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        fluree.create_ledger("revision").await.unwrap();
        let mut record = fluree
            .nameservice()
            .lookup("revision:main")
            .await
            .unwrap()
            .unwrap();
        record.config_version = Some(9);
        fluree.observe_nameservice_record(&record);
        for _ in 0..2 {
            assert!(matches!(
                fluree.history_suppressed("revision:main").await,
                Err(ApiError::ReplicaNotCurrent(_))
            ));
        }
        // An explicitly empty response carries its actual revision too.
        let reader = fluree
            .config_for_reader("revision:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reader.v, 0);
        assert!(reader.payload.is_none());
    }

    fn removal(subject: Option<&str>, commit: &str) -> Removal {
        Removal {
            requester: None,
            request_id: "r".into(),
            commit: commit.into(),
            slot: 0,
            predicate: "ex:likes".into(),
            from_t: 10,
            until_t: Some(50),
            scope_t: 49,
            subject: subject.map(subject_fingerprint),
        }
    }

    fn reads(facts: &[(Option<&str>, &str)]) -> Reads {
        Reads::Facts(
            facts
                .iter()
                .map(|(s, p)| (s.map(str::to_string), p.to_string()))
                .collect(),
        )
    }

    /// A removal whose subject is recorded refuses only reads that name it,
    /// or no subject; one whose subject is not recorded refuses every read
    /// of its predicate; one that does not apply refuses none.
    #[test]
    fn a_removals_subject_narrows_the_reads_it_refuses() {
        let journal = |removals| Journal {
            removals,
            ..Default::default()
        };
        let carol = Suppression::new(
            journal(vec![removal(Some("ex:carol"), "c1")]),
            String::new(),
            &[true],
        );
        let refuses = |s: &Suppression, r: Reads, t| s.removal_needed(&r, t, t, true).is_some();
        assert!(refuses(
            &carol,
            reads(&[(Some("ex:carol"), "ex:likes")]),
            30
        ));
        assert!(!refuses(
            &carol,
            reads(&[(Some("ex:erin"), "ex:likes")]),
            30
        ));
        assert!(refuses(&carol, reads(&[(None, "ex:likes")]), 30));
        assert!(!refuses(
            &carol,
            reads(&[(Some("ex:carol"), "ex:name")]),
            30
        ));
        assert!(refuses(&carol, Reads::Everything, 30));
        // The span is half-open for a point read.
        assert!(!refuses(
            &carol,
            reads(&[(Some("ex:carol"), "ex:likes")]),
            50
        ));
        assert!(!refuses(
            &carol,
            reads(&[(Some("ex:carol"), "ex:likes")]),
            9
        ));
        // A history range lists the retraction that ended it.
        let ranged = reads(&[(Some("ex:carol"), "ex:likes")]);
        assert!(carol.removal_needed(&ranged, 50, 60, false).is_some());
        assert!(carol.removal_needed(&ranged, 51, 60, false).is_none());

        let unknown = Suppression::new(journal(vec![removal(None, "c1")]), String::new(), &[true]);
        assert!(refuses(
            &unknown,
            reads(&[(Some("ex:erin"), "ex:likes")]),
            30
        ));

        let elsewhere =
            Suppression::new(journal(vec![removal(None, "c1")]), String::new(), &[false]);
        assert!(!refuses(&elsewhere, Reads::Everything, 30));
    }
}
