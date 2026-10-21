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
            Pattern::PropertyPath(path) => {
                if path.wildcard || !path.sequence_steps.is_empty() {
                    false
                } else {
                    let subject = iri(&path.subject, snapshot);
                    path.predicates
                        .iter()
                        .all(|p| match snapshot.decode_sid(p) {
                            Some(p) => {
                                out.push((subject.clone(), p));
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

/// A historical read of one ledger.
pub(crate) struct HistoricalRead<'a> {
    /// As-of point (the end of a history range).
    pub(crate) to_t: i64,
    /// The start of a history range; `None` for a point-in-time read.
    pub(crate) from_t: Option<i64>,
    pub(crate) reads: Reads,
    pub(crate) snapshot: &'a LedgerSnapshot,
}

/// A journal with the subjects of its removals resolved from their commits.
pub(crate) struct Suppression {
    pub(crate) journal: Journal,
    /// Subject IRI of each removal, by index; `None` when unresolvable.
    removal_subjects: Vec<Option<String>>,
}

/// Suppressions held between reads, by branch and journal generation.
#[derive(Default)]
pub(crate) struct SuppressionCache {
    by_ledger: parking_lot::Mutex<HashMap<String, (u64, Arc<Suppression>)>>,
}

impl Fluree {
    /// The branch's suppression, or `None` when its journal is empty.
    pub(crate) async fn suppression_for(
        &self,
        ledger_id: &str,
    ) -> Result<Option<Arc<Suppression>>> {
        let ns = self.publisher()?;
        let (_, registration) = registry::read(ns, ledger_id).await?;
        let Some(journal) = registration.map(|r| r.journal).filter(|j| !j.is_empty()) else {
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

    /// The subject of each removed occurrence, read from its commit.
    async fn removal_subjects(&self, ledger_id: &str, journal: &Journal) -> Vec<Option<String>> {
        if journal.removals.is_empty() {
            return Vec::new();
        }
        let Ok(state) = self.ledger_cached(ledger_id).await else {
            return vec![None; journal.removals.len()];
        };
        let snapshot = state.snapshot().await;
        let Ok(publisher) = self.publisher() else {
            return vec![None; journal.removals.len()];
        };
        let Ok(Some(record)) = publisher.lookup(ledger_id).await else {
            return vec![None; journal.removals.len()];
        };
        let Ok(store) = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await
        else {
            return vec![None; journal.removals.len()];
        };
        let mut out = Vec::with_capacity(journal.removals.len());
        let mut commits: HashMap<String, Option<fluree_db_core::Commit>> = HashMap::new();
        for removal in &journal.removals {
            let commit = match commits.get(&removal.commit) {
                Some(c) => c.clone(),
                None => {
                    let loaded = match removal.commit.parse::<ContentId>() {
                        Ok(cid) => fluree_db_core::load_commit_by_id(store.as_ref(), &cid)
                            .await
                            .ok(),
                        Err(_) => None,
                    };
                    commits.insert(removal.commit.clone(), loaded.clone());
                    loaded
                }
            };
            out.push(
                commit
                    .and_then(|c| c.flakes.get(removal.slot as usize).cloned())
                    .and_then(|f| snapshot.snapshot.decode_sid(&f.s)),
            );
        }
        out
    }

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

    /// Refuse `read` of `ledger_id` when it needs suppressed history.
    pub(crate) async fn refuse_suppressed_history(
        &self,
        ledger_id: &str,
        read: HistoricalRead<'_>,
    ) -> Result<()> {
        let Some(suppression) = self.suppression_for(ledger_id).await? else {
            return Ok(());
        };
        let head_t = match self.publisher()?.lookup(ledger_id).await? {
            Some(record) => record.commit_t,
            None => return Ok(()),
        };
        if read.from_t.is_none() && read.to_t >= head_t {
            return Ok(());
        }
        let (from, to) = (read.from_t.unwrap_or(read.to_t), read.to_t);
        let point = read.from_t.is_none();
        let _ = read.snapshot;
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
