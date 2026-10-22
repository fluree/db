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
use fluree_db_query::ir::{Pattern, Query, Ref};

use super::journal::Journal;
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
    fn touches(&self, subject: Option<&str>, predicate: &str) -> bool {
        match self {
            Reads::Everything => true,
            Reads::Facts(facts) => facts.iter().any(|(s, p)| {
                p == predicate
                    && match (s.as_deref(), subject) {
                        (Some(s), Some(subject)) => s == subject,
                        _ => true,
                    }
            }),
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
pub(crate) struct HistoricalRead<'a> {
    /// As-of point (the end of a history range).
    pub(crate) to_t: i64,
    /// The start of a history range; `None` for a point-in-time read.
    pub(crate) from_t: Option<i64>,
    pub(crate) reads: Reads,
    pub(crate) snapshot: &'a LedgerSnapshot,
    /// Whether a cluster replica first confirms its journal is current: a
    /// read as of an earlier point does, while a view merely behind a head
    /// this replica has applied is checked against the journal it holds.
    pub(crate) confirm_journal: bool,
}

/// A journal with the subjects of its removals read from their commits.
pub(crate) struct Suppression {
    pub(crate) journal: Journal,
    /// Subject of each removal, by index, as its commit encodes it. Decoded
    /// against the reading view's snapshot.
    removal_subjects: Vec<RemovalSubject>,
}

/// What a removed occurrence's commit says about its subject.
#[derive(Clone)]
enum RemovalSubject {
    Known(fluree_db_core::Sid),
    /// The commit could not be read: any subject may be the one.
    Unknown,
    /// The commit is not in this ledger, so neither is the occurrence (a
    /// journal merged from another copy of the lineage).
    Absent,
}

/// How a view stands against its ledger's head.
pub(crate) enum Freshness {
    /// At the head: current state.
    Current,
    /// Behind it. `journal` is whether the ledger's journal holds anything,
    /// where known.
    Behind { journal: Option<bool> },
}

/// Suppressions held between reads, by branch and journal generation.
#[derive(Default)]
pub(crate) struct SuppressionCache {
    by_ledger: parking_lot::Mutex<HashMap<String, (u64, Arc<Suppression>)>>,
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
        let (_, registration) = registry::read(ns, ledger_id).await?;
        let journal = registration.map(|r| r.journal).filter(|j| !j.is_empty());
        self.retention
            .watch
            .learn_journal(ledger_id, journal.is_some(), token);
        let Some(journal) = journal else {
            return Ok(None);
        };
        let cache = &self.retention.suppressions;
        if let Some((generation, cached)) = cache.by_ledger.lock().get(ledger_id) {
            if *generation == journal.generation {
                return Ok(Some(Arc::clone(cached)));
            }
        }
        let removal_subjects = self.removal_subjects(ledger_id, &journal).await;
        let suppression = Arc::new(Suppression {
            journal,
            removal_subjects,
        });
        cache.by_ledger.lock().insert(
            ledger_id.to_string(),
            (suppression.journal.generation, Arc::clone(&suppression)),
        );
        Ok(Some(suppression))
    }

    /// The subject of each removed occurrence, read from its commit. Reads
    /// only storage and the nameservice, never the cached ledger, so a query
    /// issued inside a transaction can ask.
    async fn removal_subjects(&self, ledger_id: &str, journal: &Journal) -> Vec<RemovalSubject> {
        use fluree_db_core::ContentStore as _;
        if journal.removals.is_empty() {
            return Vec::new();
        }
        let unknown = || vec![RemovalSubject::Unknown; journal.removals.len()];
        let Ok(Some(record)) = self.nameservice().lookup(ledger_id).await else {
            return unknown();
        };
        let Ok(store) = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await
        else {
            return unknown();
        };
        let mut out = Vec::with_capacity(journal.removals.len());
        let mut commits: HashMap<String, Option<Option<fluree_db_core::Commit>>> = HashMap::new();
        for removal in &journal.removals {
            // `Some(None)`: not in this ledger; `None`: unreadable.
            let commit = match commits.get(&removal.commit) {
                Some(c) => c.clone(),
                None => {
                    let loaded = match removal.commit.parse::<ContentId>() {
                        Ok(cid) => match store.has(&cid).await {
                            Ok(false) => Some(None),
                            Ok(true) => fluree_db_core::load_commit_by_id(store.as_ref(), &cid)
                                .await
                                .ok()
                                .map(Some),
                            Err(_) => None,
                        },
                        Err(_) => None,
                    };
                    commits.insert(removal.commit.clone(), loaded.clone());
                    loaded
                }
            };
            out.push(match commit {
                Some(None) => RemovalSubject::Absent,
                Some(Some(c)) => c
                    .at_slot(removal.slot)
                    .map_or(RemovalSubject::Unknown, |f| {
                        RemovalSubject::Known(f.s.clone())
                    }),
                None => RemovalSubject::Unknown,
            });
        }
        out
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
    /// any earlier snapshot of it (`historical`).
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
            for ledger_id in &record.dependencies {
                let Ok(ledger_id) = fluree_db_core::LedgerId::parse(ledger_id) else {
                    continue;
                };
                let ledger_id = ledger_id.to_string();
                let Some(suppression) = self.suppression_for(&ledger_id).await? else {
                    continue;
                };
                if historical {
                    return Err(ApiError::HistoryUnavailable(format!(
                        "search index {id} is read as of an earlier point, and {ledger_id} \
                         suppresses history; search it as of the current head"
                    )));
                }
                let journal = &suppression.journal;
                let removed_through = journal
                    .removals
                    .iter()
                    .map(|r| r.until_t.unwrap_or(r.scope_t))
                    .max();
                let lagging_removal = removed_through.filter(|t| record.index_t < *t);
                let lagging_rules = journal.history_rules.iter().any(|r| r.until_t.is_none())
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
        read: HistoricalRead<'_>,
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
            if covered && read.reads.touches(None, &rule.predicate) {
                return Err(ApiError::HistoryUnavailable(format!(
                    "<{}> keeps no history (f:noHistory) on {ledger_id}; read it as of the \
                     current head",
                    rule.predicate
                )));
            }
        }
        for (removal, subject) in suppression
            .journal
            .removals
            .iter()
            .zip(&suppression.removal_subjects)
        {
            let covered = if point {
                removal.covers(to)
            } else {
                removal.overlaps(from, to)
            };
            let subject = match subject {
                RemovalSubject::Absent => continue,
                RemovalSubject::Unknown => None,
                RemovalSubject::Known(s) => read.snapshot.decode_sid(s),
            };
            if covered && read.reads.touches(subject.as_deref(), &removal.predicate) {
                return Err(ApiError::HistoryUnavailable(format!(
                    "a removed occurrence of <{}> on {ledger_id} falls in the requested range",
                    removal.predicate
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_constant_subject_narrows_what_a_read_touches() {
        let reads = Reads::Facts(vec![
            (Some("ex:a".into()), "ex:p".into()),
            (None, "ex:q".into()),
        ]);
        assert!(reads.touches(Some("ex:a"), "ex:p"));
        assert!(!reads.touches(Some("ex:b"), "ex:p"));
        assert!(reads.touches(None, "ex:p"));
        assert!(reads.touches(Some("ex:z"), "ex:q"));
        assert!(!reads.touches(None, "ex:r"));
        assert!(Reads::Everything.touches(None, "ex:r"));
    }
}
