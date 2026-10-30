//! Refusing reads that need suppressed history.
//!
//! A read as of the head is answered from current state, which retention
//! never hides beyond what was retracted. A read as of an earlier point, or a
//! history range, may need what the removal journal suppresses; when its
//! patterns could touch a suppressed predicate (or a removed occurrence) at
//! a covered time, it fails with [`ApiError::HistoryUnavailable`] instead of
//! answering from incomplete history. The check runs once per request, while
//! the executable is built, and reads only the journal: a ledger without one
//! pays one nameservice read on a historical query and nothing on a current
//! one.

use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_core::{ContentId, LedgerSnapshot};
use fluree_db_nameservice::NsRecord;
use fluree_db_query::ir::{Pattern, Query, Ref};

use super::journal::{HistoryRule, Journal, Removal};
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

impl Reads {
    /// Whether the read could touch `predicate`, for any subject.
    fn touches(&self, predicate: &str) -> bool {
        match self {
            Reads::Everything => true,
            Reads::Facts(facts) => facts.iter().any(|(_, p)| p == predicate),
        }
    }
}

/// What `query` reads, in IRIs.
pub(crate) fn reads_of(query: &Query, snapshot: &LedgerSnapshot) -> Reads {
    if query.output.has_hydration() {
        return Reads::Everything;
    }
    let mut facts = Vec::new();
    if collect(&query.patterns, snapshot, &mut facts) {
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

/// Collect the facts `patterns` read; `false` when they can read anything.
fn collect(
    patterns: &[Pattern],
    snapshot: &LedgerSnapshot,
    out: &mut Vec<(Option<String>, String)>,
) -> bool {
    for pattern in patterns {
        let bounded = match pattern {
            Pattern::Triple(tp) => match iri(&tp.p, snapshot) {
                Some(p) => {
                    out.push((iri(&tp.s, snapshot), p));
                    true
                }
                None => false,
            },
            Pattern::EdgeAnnotation { edge, body, .. } => match iri(&edge.p, snapshot) {
                Some(p) => {
                    out.push((iri(&edge.s, snapshot), p));
                    out.push((None, fluree_vocab::rdf::REIFIES.to_string()));
                    collect(body, snapshot, out)
                }
                None => false,
            },
            // A path's steps past the first, and an inverse step, read
            // subjects other than the constant one: its predicates count for
            // every subject.
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
            Pattern::Optional(ps)
            | Pattern::Minus(ps)
            | Pattern::Exists(ps)
            | Pattern::NotExists(ps)
            | Pattern::DefaultGraphSource { patterns: ps }
            | Pattern::Graph { patterns: ps, .. } => collect(ps, snapshot, out),
            Pattern::Union(branches) => branches.iter().all(|b| collect(b, snapshot, out)),
            Pattern::Subquery(sub) => collect(&sub.patterns, snapshot, out),
            Pattern::Filter(expr)
            | Pattern::Bind { expr, .. }
            | Pattern::Unwind { list: expr, .. } => {
                let mut ok = true;
                expr.for_each_embedded_patterns(&mut |ps| ok &= collect(ps, snapshot, out));
                ok
            }
            Pattern::Values { .. }
            | Pattern::TermComponents(_)
            | Pattern::IndexSearch(_)
            | Pattern::VectorSearch(_)
            | Pattern::R2rml(_) => true,
            Pattern::ShortestPath(_)
            | Pattern::GeoSearch(_)
            | Pattern::S2Search(_)
            | Pattern::Service(_) => false,
        };
        if !bounded {
            return false;
        }
    }
    true
}

/// The search graph sources (full-text, vector) `patterns` read.
pub(crate) fn search_sources(patterns: &[Pattern], out: &mut Vec<String>) {
    for pattern in patterns {
        match pattern {
            Pattern::IndexSearch(p) => out.push(p.graph_source_id.clone()),
            Pattern::VectorSearch(p) => out.push(p.graph_source_id.clone()),
            Pattern::EdgeAnnotation { body: ps, .. }
            | Pattern::Optional(ps)
            | Pattern::Minus(ps)
            | Pattern::Exists(ps)
            | Pattern::NotExists(ps)
            | Pattern::DefaultGraphSource { patterns: ps }
            | Pattern::Graph { patterns: ps, .. } => search_sources(ps, out),
            Pattern::Union(branches) => branches.iter().for_each(|b| search_sources(b, out)),
            Pattern::Subquery(sub) => search_sources(&sub.patterns, out),
            Pattern::Filter(expr)
            | Pattern::Bind { expr, .. }
            | Pattern::Unwind { list: expr, .. } => {
                expr.for_each_embedded_patterns(&mut |ps| search_sources(ps, out));
            }
            _ => {}
        }
    }
}

/// A historical read of one ledger.
pub(crate) struct HistoricalRead {
    /// As-of point (the end of a history range).
    pub(crate) to_t: i64,
    /// The start of a history range; `None` for a point-in-time read.
    pub(crate) from_t: Option<i64>,
    pub(crate) reads: Reads,
    /// Whether a cluster replica first confirms its journal is current: a
    /// read as of an earlier point does, while a view merely behind a head
    /// this replica has applied is checked against the journal it holds.
    pub(crate) confirm_journal: bool,
}

/// A journal with the removals that apply to its branch looked up by what
/// a read names.
pub(crate) struct Suppression {
    pub(crate) journal: Journal,
    /// The journal's identity as its record names it.
    identity: String,
    /// The removals whose commit the branch holds, by predicate, as indexes
    /// into the journal's removals. One whose commit the branch does not
    /// hold (merged from another copy of the lineage) is left out.
    removed: HashMap<String, RemovedOf>,
}

/// The removals of one predicate that apply to a branch.
#[derive(Default)]
struct RemovedOf {
    /// By their subject's fingerprint.
    by_subject: HashMap<String, Vec<usize>>,
    /// Those whose subject is not recorded, which any subject may be.
    any_subject: Vec<usize>,
}

impl Suppression {
    fn new(journal: Journal, identity: String, applies: &[bool]) -> Self {
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
        Suppression {
            journal,
            identity,
            removed,
        }
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
    /// Behind it. `journal` is whether the ledger's journal holds anything,
    /// where known.
    Behind { journal: Option<bool> },
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
        let seen = match watch.check(&db.ledger_id, db.fence) {
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

    /// The branch's suppression, or `None` when its journal is empty. On a
    /// cluster replica, read once the replica has confirmed it holds the
    /// current journal.
    pub(crate) async fn suppression_for(
        &self,
        ledger_id: &str,
    ) -> Result<Option<Arc<Suppression>>> {
        self.suppression_with(ledger_id, true).await
    }

    /// [`Self::suppression_for`], confirming the journal is current first
    /// only when `confirm`.
    async fn suppression_with(
        &self,
        ledger_id: &str,
        confirm: bool,
    ) -> Result<Option<Arc<Suppression>>> {
        let ns = &self.nameservice_mode;
        // On a cluster replica, first confirm the local copy is current,
        // whether or not this replica has seen the ledger registered yet: one
        // that has not applied its first registration does not know it is
        // enabled. Outside a cluster this is a no-op.
        if confirm {
            self.confirm_journal_current(ledger_id).await?;
        }
        let token = self.retention.watch.token();
        let (_, stored) = registry::read_stored(ns, ledger_id).await?;
        let stored = stored.filter(|s| !registry::journal_empty(s));
        self.retention
            .watch
            .learn_journal(ledger_id, stored.is_some(), token);
        let Some(stored) = stored else {
            return Ok(None);
        };
        let identity = registry::journal_identity(&stored);
        let cache = &self.retention.suppressions;
        if let Some(cached) = cache.by_ledger.lock().get(ledger_id) {
            if cached.identity == identity {
                return Ok(Some(Arc::clone(cached)));
            }
        }
        let journal = registry::resolve_journal(&stored, self.registry_objects()).await?;
        let applies = self.removals_applying(ledger_id, &journal).await;
        let suppression = Arc::new(Suppression::new(journal, identity, &applies));
        cache
            .by_ledger
            .lock()
            .insert(ledger_id.to_string(), Arc::clone(&suppression));
        Ok(Some(suppression))
    }

    /// Whether each removal of `journal` applies to `ledger_id`: whether the
    /// branch holds its commit. Reads only storage and the nameservice,
    /// never the cached ledger, so a query issued inside a transaction can
    /// ask. Whatever cannot be told applies.
    async fn removals_applying(&self, ledger_id: &str, journal: &Journal) -> Vec<bool> {
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
        for removal in &journal.removals {
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
    /// `f:noHistory` rules.
    pub(crate) async fn suppressed_history_for_index(
        &self,
        ledger_id: &str,
    ) -> Result<Vec<fluree_db_indexer::SuppressedHistory>> {
        let Some(suppression) = self.suppression_for(ledger_id).await? else {
            return Ok(Vec::new());
        };
        Ok(suppression
            .journal
            .history_rules
            .iter()
            .map(|rule| fluree_db_indexer::SuppressedHistory {
                predicate_iri: rule.predicate.clone(),
                graph_iri: rule.graph.clone(),
                except_graph_iris: rule.except_graphs.clone(),
                from_t: rule.from_t,
                until_t: rule.until_t,
            })
            .collect())
    }

    /// Refuse a search of a graph source that may still hold what its
    /// source ledger's retention suppressed: one built before a removal it
    /// has not yet indexed, one lagging the head of a ledger with
    /// `f:noHistory` (a value it holds may since have been superseded), or
    /// any earlier snapshot of it (`historical`). Only removals and rules of
    /// properties the index may read count.
    pub(crate) async fn refuse_stale_search_sources(
        &self,
        query: &Query,
        historical: bool,
    ) -> Result<()> {
        let mut sources = Vec::new();
        search_sources(&query.patterns, &mut sources);
        if sources.is_empty() {
            return Ok(());
        }
        sources.sort();
        sources.dedup();
        for id in sources {
            let Some(record) = self.nameservice().lookup_graph_source(&id).await? else {
                continue;
            };
            let indexed = crate::graph_source::helpers::indexed_properties(&record);
            for ledger_id in &record.dependencies {
                let Ok(ledger_id) = fluree_db_core::LedgerId::parse(ledger_id) else {
                    continue;
                };
                let ledger_id = ledger_id.to_string();
                let Some(suppression) = self.suppression_for(&ledger_id).await? else {
                    continue;
                };
                let journal = &suppression.journal;
                let removals: Vec<&Removal> = journal
                    .removals
                    .iter()
                    .filter(|r| indexed.may_read(&r.predicate))
                    .collect();
                let rules: Vec<&HistoryRule> = journal
                    .history_rules
                    .iter()
                    .filter(|r| indexed.may_read(&r.predicate))
                    .collect();
                if removals.is_empty() && rules.is_empty() {
                    continue;
                }
                if historical {
                    return Err(ApiError::HistoryUnavailable(format!(
                        "search index {id} is read as of an earlier point, and {ledger_id} \
                         suppresses history; search it as of the current head"
                    )));
                }
                let removed_through = removals
                    .iter()
                    .map(|r| r.until_t.unwrap_or(r.scope_t))
                    .max();
                let lagging_removal = removed_through.filter(|t| record.index_t < *t);
                let lagging_rules = rules.iter().any(|r| r.until_t.is_none())
                    && match self.nameservice().lookup(&ledger_id).await? {
                        Some(head) => record.index_t < head.commit_t,
                        None => false,
                    };
                if lagging_removal.is_some() || lagging_rules {
                    return Err(ApiError::HistoryUnavailable(format!(
                        "search index {id} has indexed {ledger_id} through t={}, before \
                         retention changes it must reflect; retry once it has synced",
                        record.index_t
                    )));
                }
            }
        }
        Ok(())
    }

    /// Refuse `read` of `ledger_id` when it needs suppressed history.
    pub(crate) async fn refuse_suppressed_history(
        &self,
        ledger_id: &str,
        read: HistoricalRead,
    ) -> Result<()> {
        let Some(suppression) = self
            .suppression_with(ledger_id, read.confirm_journal)
            .await?
        else {
            return Ok(());
        };
        let head_t = match self.nameservice().lookup(ledger_id).await? {
            Some(record) => record.commit_t,
            None => {
                return Err(ApiError::NotFound(format!(
                    "{ledger_id}: the ledger this read needs has been dropped"
                )))
            }
        };
        if read.from_t.is_none() && read.to_t >= head_t {
            return Ok(());
        }
        let (from, to) = (read.from_t.unwrap_or(read.to_t), read.to_t);
        let point = read.from_t.is_none();
        for rule in &suppression.journal.history_rules {
            let covered = if point {
                rule.covers(to)
            } else {
                rule.overlaps(from, to)
            };
            if covered && read.reads.touches(&rule.predicate) {
                return Err(ApiError::HistoryUnavailable(format!(
                    "<{}> keeps no history (f:noHistory) on {ledger_id}; read it as of the \
                     current head",
                    rule.predicate
                )));
            }
        }
        if let Some(predicate) = suppression.removal_needed(&read.reads, from, to, point) {
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

    fn removal(subject: Option<&str>, commit: &str) -> Removal {
        Removal {
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
