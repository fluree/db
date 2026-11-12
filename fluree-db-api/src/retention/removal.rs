//! Targeted query removal: selected occurrences become unavailable to
//! current and historical application queries.
//!
//! A request names occurrences by their original commit and operation slot,
//! or selects facts (subject, predicate, and optionally graph and object)
//! whose every occurrence up to the request's head is expanded into such a
//! manifest — assertions and the retractions that ended them. An occurrence
//! still current is retracted through the ordered write path; then each one
//! is recorded in the removal journal, which serving honors from then on.
//! The commit log keeps the original operations for audit access; removal is
//! not erasure.

use std::collections::HashMap;
#[cfg(feature = "native")]
use std::collections::{BTreeMap, HashSet};

#[cfg(feature = "native")]
use fluree_db_core::TripleTermValue;
use fluree_db_core::{
    collect_first_parent_cids, load_commit_by_id, ContentId, Flake, FlakeValue, LedgerId, Sid,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::journal::Removal;
use super::registry;
use super::state::FactKey;
use super::writer::{outcome_of, RetentionBatch, RetentionWriter, SubmitOutcome};
use crate::{ApiError, Fluree, Result};

/// One operation of one commit.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OccurrenceRef {
    /// The original commit's content id.
    pub commit: String,
    /// The operation's position in the commit.
    pub slot: u32,
}

/// Facts whose every occurrence a request removes. A removal cannot be
/// undone, so a field this does not know is refused: a misspelled `object`
/// would otherwise select every object.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FactSelector {
    /// Subject IRI.
    pub subject: String,
    /// Predicate IRI.
    pub predicate: String,
    /// Named graph IRI; absent for the default graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
    /// The object: an IRI as `{"@id": ...}`; a JSON string (an
    /// `xsd:string`), number or boolean; or a value object,
    /// `{"@value": ..., "@type": "<datatype IRI>"}` or
    /// `{"@value": "...", "@language": "<tag>"}`. Absent selects every
    /// object; a `null` is refused, not read as absent.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "named_object"
    )]
    pub object: Option<serde_json::Value>,
}

fn named_object<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<serde_json::Value>, D::Error> {
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Null => Err(serde::de::Error::custom(
            "`object` is null; leave it out to select every object",
        )),
        object => Ok(Some(object)),
    }
}

/// A removal request.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemovalRequest {
    #[serde(default)]
    pub occurrences: Vec<OccurrenceRef>,
    #[serde(default)]
    pub matching: Vec<FactSelector>,
}

/// What a removal did.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemovalReceipt {
    pub request_id: String,
    /// The head the request's scope was fixed at.
    pub scope_t: i64,
    /// Every occurrence removed.
    pub manifest: Vec<OccurrenceRef>,
    /// The commit that retracted occurrences still current, if any.
    pub retracted_t: Option<i64>,
    /// The journal generation that records the removal.
    pub generation: u64,
    /// Cluster replicas that had not applied the removal when the request
    /// returned; until they do, they serve the ledger as of before it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unacknowledged_replicas: Vec<String>,
}

/// An occurrence located in the commit log: its operation and commit `t`.
struct Located {
    occurrence: OccurrenceRef,
    flake: Flake,
    t: i64,
}

/// How a permanent erasure widens a removal.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Widening {
    /// A named occurrence also takes the one bounding its span: an assertion
    /// the retraction that ends it, a retraction the assertion it ends. Both
    /// carry the value, and erasing one alone would change what replaying
    /// the other states.
    pub pair_spans: bool,
    /// Named occurrences outside this branch's history are skipped rather
    /// than refused: the request is another branch's.
    pub skip_foreign: bool,
    /// Each occurrence bounding a named one's span must be named too, or the
    /// removal is refused: an erasure received from elsewhere cannot widen
    /// here, where nothing proves an erasure.
    pub bounded: bool,
}

/// A removal and the `t` of each manifest occurrence's commit.
pub(crate) struct Removed {
    pub receipt: RemovalReceipt,
    // Read by permanent erasure, which needs file storage.
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    pub manifest_t: Vec<i64>,
    /// Each occurrence's property IRI, empty where it did not decode.
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    pub properties: Vec<String>,
}

/// One assertion or retraction of a fact: its commit `t`, whether it
/// asserts, and where it is (commit index, slot).
#[derive(Clone, Copy)]
struct Event {
    t: i64,
    op: bool,
    commit: usize,
    slot: u32,
}

/// How many times a removal recomputes after a concurrent write.
const ATTEMPTS: usize = 5;

impl Fluree {
    /// Remove the selected occurrences from what `ledger_id` serves,
    /// retracting the current ones directly. See [`Self::remove_occurrences_with`].
    pub async fn remove_occurrences(
        &self,
        ledger_id: &str,
        request: RemovalRequest,
    ) -> Result<RemovalReceipt> {
        struct Direct<'a>(&'a Fluree);
        #[async_trait::async_trait]
        impl RetentionWriter for Direct<'_> {
            async fn submit(
                &self,
                ledger_id: &str,
                batch: RetentionBatch,
            ) -> Result<SubmitOutcome> {
                outcome_of(self.0.commit_retention_batch(ledger_id, batch).await)
            }
        }
        self.remove_occurrences_with(&Direct(self), ledger_id, request)
            .await
    }

    /// Remove the selected occurrences, committing retractions of the
    /// current ones through `writer`.
    pub async fn remove_occurrences_with(
        &self,
        writer: &dyn RetentionWriter,
        ledger_id: &str,
        request: RemovalRequest,
    ) -> Result<RemovalReceipt> {
        self.remove_occurrences_by(writer, ledger_id, request, None)
            .await
    }

    /// [`Self::remove_occurrences_with`], recording `requester`, the
    /// identity that asked as the caller authenticated it, with each
    /// removal: audit data, as the removal is.
    pub async fn remove_occurrences_by(
        &self,
        writer: &dyn RetentionWriter,
        ledger_id: &str,
        request: RemovalRequest,
        requester: Option<String>,
    ) -> Result<RemovalReceipt> {
        self.remove_widened(
            writer,
            ledger_id,
            &request,
            Widening::default(),
            requester.as_deref(),
        )
        .await
        .map(|r| r.receipt)
    }

    /// [`Self::remove_occurrences_with`], widened as `widening` says.
    pub(crate) async fn remove_widened(
        &self,
        writer: &dyn RetentionWriter,
        ledger_id: &str,
        request: &RemovalRequest,
        widening: Widening,
        requester: Option<&str>,
    ) -> Result<Removed> {
        if request.occurrences.is_empty() && request.matching.is_empty() {
            return Err(ApiError::http(
                400,
                "a removal names occurrences or facts to remove",
            ));
        }
        let ledger_id = LedgerId::parse(ledger_id)?.to_string();
        for _ in 0..ATTEMPTS {
            let Some(read) = self.read_history(&ledger_id, None).await? else {
                continue;
            };
            let Found {
                mut located,
                mut spans,
                current,
            } = locate(&read, request, widening)?;
            let scope_t = read.scope_t;

            // Retract what is still current, against the head the scope
            // was fixed at.
            let mut retracted_t = None;
            if !current.is_empty() {
                let retractions = current
                    .iter()
                    .map(|f| Flake {
                        op: false,
                        t: scope_t + 1,
                        ..f.clone()
                    })
                    .collect::<Vec<_>>();
                let mut stored = Vec::new();
                for f in &retractions {
                    stored.extend(stored_terms(&read.state, f).await?);
                }
                let batch = RetentionBatch {
                    expected_t: scope_t,
                    retractions: if stored.is_empty() {
                        retractions
                    } else {
                        stored
                    },
                    expired: current.len(),
                    retired: 0,
                    control: Vec::new(),
                };
                match writer.submit(&ledger_id, batch).await? {
                    SubmitOutcome::Committed { t, commit_id } => {
                        retracted_t = Some(t);
                        // The retractions just made are occurrences of the
                        // removed facts too.
                        let commit = load_commit_by_id(read.store.as_ref(), &commit_id).await?;
                        for (slot, flake) in commit.occurrences() {
                            if let Some(from) = current
                                .iter()
                                .find(|c| !flake.op && FactKey::of(c) == FactKey::of(flake))
                                .map(|c| c.t)
                            {
                                located.push(Located {
                                    occurrence: OccurrenceRef {
                                        commit: commit_id.to_string(),
                                        slot,
                                    },
                                    flake: flake.clone(),
                                    t,
                                });
                                spans.push((from, Some(t)));
                            }
                        }
                    }
                    SubmitOutcome::HeadMoved => continue,
                    SubmitOutcome::Deferred => {
                        return Err(ApiError::Transact(
                            fluree_db_transact::TransactError::NoveltyAtMax,
                        ))
                    }
                }
            }

            let request_id = request_id(&ledger_id, scope_t, &located);
            let removals = entries(&read, &request_id, &located, &spans, retracted_t, requester);
            let properties = removals.iter().map(|r| r.predicate.clone()).collect();
            let registration = self
                .record_removals(&ledger_id, read.since(), removals)
                .await?;
            let unacknowledged_replicas = self
                .unacknowledged_replicas(super::replicas::ACK_TIMEOUT)
                .await;
            let manifest_t = located.iter().map(|l| l.t).collect();
            return Ok(Removed {
                receipt: RemovalReceipt {
                    request_id,
                    scope_t,
                    manifest: located.into_iter().map(|l| l.occurrence).collect(),
                    retracted_t,
                    generation: registration.journal.generation,
                    unacknowledged_replicas,
                },
                manifest_t,
                properties,
            });
        }
        Err(ApiError::commit_contention(
            ATTEMPTS,
            ApiError::internal("the head kept moving during the removal"),
        ))
    }

    /// The removal that erasing `occurrences`, received from elsewhere,
    /// makes on `ledger_id`, which holds some of their commits: computed,
    /// not recorded. Nothing is retracted: a retraction would hold the
    /// value, which this storage cannot erase without a prover; what is
    /// current leaves the ledger's index instead. `None` when its history
    /// holds none of them. `held` is the ledger's state under its write
    /// lock, when the caller holds it.
    #[cfg(feature = "native")]
    pub(crate) async fn received_removal(
        &self,
        ledger_id: &str,
        occurrences: Vec<OccurrenceRef>,
        held: Option<fluree_db_ledger::LedgerState>,
    ) -> Result<Option<ReceivedRemoval>> {
        let ledger_id = LedgerId::parse(ledger_id)?.to_string();
        let request = RemovalRequest {
            occurrences,
            matching: Vec::new(),
        };
        let widening = Widening {
            pair_spans: false,
            skip_foreign: true,
            bounded: true,
        };
        for _ in 0..ATTEMPTS {
            let Some(read) = self.read_history(&ledger_id, held.clone()).await? else {
                continue;
            };
            let found = match locate(&read, &request, widening) {
                Ok(found) => found,
                Err(e) if e.status_code() == 404 => return Ok(None),
                Err(e) => return Err(e),
            };
            let request_id = request_id(&ledger_id, read.scope_t, &found.located);
            let removals = entries(&read, &request_id, &found.located, &found.spans, None, None);
            return Ok(Some(ReceivedRemoval {
                ledger_id,
                since: read.since(),
                scope_t: read.scope_t,
                removals,
                flakes: found.located.into_iter().map(|l| l.flake).collect(),
            }));
        }
        Err(ApiError::commit_contention(
            ATTEMPTS,
            ApiError::internal("the head kept moving while reading the history"),
        ))
    }

    /// Record a removal [`Self::received_removal`] computed.
    #[cfg(feature = "native")]
    pub(crate) async fn record_received_removal(&self, removal: ReceivedRemoval) -> Result<()> {
        self.record_removals(&removal.ledger_id, removal.since, removal.removals)
            .await
            .map(|_| ())
    }

    /// `ledger_id`'s history, as of its head; `None` when the head moved
    /// while it was read. `held` is its state, read under its write lock.
    async fn read_history(
        &self,
        ledger_id: &str,
        held: Option<fluree_db_ledger::LedgerState>,
    ) -> Result<Option<History>> {
        let Some(record) = self.publisher()?.lookup(ledger_id).await? else {
            return Err(ApiError::NotFound(ledger_id.to_string()));
        };
        let Some(head) = record.commit_head_id.clone() else {
            return Err(ApiError::http(400, "the ledger has no commits"));
        };
        let scope_t = record.commit_t;
        let store = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await?;
        let state = match held {
            Some(state) if state.t() != scope_t => {
                return Err(ApiError::internal(format!(
                    "{ledger_id}'s cached state is not at its head"
                )))
            }
            Some(state) => state,
            None => self.ledger(ledger_id).await?,
        };
        if state.t() != scope_t {
            return Ok(None);
        }
        // Every commit up to the head, oldest first, with its flakes.
        let mut cids = collect_first_parent_cids(store.as_ref(), &head, 0).await?;
        cids.reverse();
        let mut commits = Vec::with_capacity(cids.len());
        for (t, cid) in cids {
            let commit = load_commit_by_id(store.as_ref(), &cid).await?;
            commits.push((t, cid, commit));
        }
        Ok(Some(History {
            ledger_id: ledger_id.to_string(),
            scope_t,
            store,
            state,
            commits,
        }))
    }

    /// Add `removals` to `ledger_id`'s journal and its lineage's checkpoint.
    async fn record_removals(
        &self,
        ledger_id: &str,
        since: i64,
        removals: Vec<Removal>,
    ) -> Result<registry::Registration> {
        // A ledger this registers keeps the retention input it already
        // holds, as the worker's activation would.
        self.retention.watch.journal_changing(ledger_id);
        let incoming = super::journal::Journal {
            removals,
            ..Default::default()
        };
        let mut conflict = None;
        let registration = registry::update(
            self.publisher()?,
            self.registry_objects(),
            ledger_id,
            |current| {
                let mut next = current.cloned().unwrap_or_else(|| registry::Registration {
                    active_since_t: since,
                    watermark_t: since - 1,
                    ..Default::default()
                });
                match next.journal.merge(&incoming) {
                    Ok(changed) => (changed || current.is_none()).then_some(next),
                    Err(e) => {
                        conflict = Some(e);
                        None
                    }
                }
            },
        )
        .await?
        .unwrap_or_default();
        if let Some(e) = conflict {
            return Err(e.into());
        }
        // A copy restored after this ledger is gone keeps the removal
        // only through the checkpoint; a retry publishes it.
        self.checkpoint_journal(ledger_id).await.map_err(|e| {
            ApiError::internal(format!(
                "the removal is recorded on {ledger_id}, but its lineage checkpoint \
                 could not be published ({e}); retry the request"
            ))
        })?;
        Ok(registration)
    }
}

/// A ledger's history as a removal reads it, as of its head.
struct History {
    ledger_id: String,
    scope_t: i64,
    store: std::sync::Arc<dyn fluree_db_core::ContentStore>,
    state: fluree_db_ledger::LedgerState,
    /// Oldest first.
    commits: Vec<(i64, ContentId, fluree_db_core::Commit)>,
}

impl History {
    /// Where a registration this removal makes starts: at the first commit
    /// carrying retention input, as the worker's activation would.
    fn since(&self) -> i64 {
        self.commits
            .iter()
            .find(|(_, _, c)| super::engine::carries_retention_input(&c.txn_meta))
            .map_or(self.scope_t + 1, |(t, _, _)| *t)
    }
}

/// A removal an erasure received from elsewhere makes on a ledger holding
/// its commits: see [`Fluree::received_removal`].
#[cfg(feature = "native")]
pub(crate) struct ReceivedRemoval {
    pub(crate) ledger_id: String,
    since: i64,
    /// The ledger's head `t` when it was computed.
    pub(crate) scope_t: i64,
    pub(crate) removals: Vec<Removal>,
    /// The removed operations, as the ledger's history holds them.
    pub(crate) flakes: Vec<Flake>,
}

/// What a removal finds in a history: the occurrences it names, widened,
/// each with its span (asserted at, retracted at), and those still current.
struct Found {
    located: Vec<Located>,
    spans: Vec<(i64, Option<i64>)>,
    current: Vec<Flake>,
}

/// Find what `request` names in `read`'s history, widened as `widening`
/// says.
fn locate(read: &History, request: &RemovalRequest, widening: Widening) -> Result<Found> {
    let commits = &read.commits;
    let selectors: Vec<(Option<Sid>, Sid, Sid, Option<ObjectTerm>)> = request
        .matching
        .iter()
        .map(|sel| selector_terms(&read.state.snapshot, sel))
        .collect::<Result<_>>()?;
    let mut located: Vec<Located> = Vec::new();
    for occ in &request.occurrences {
        let cid: ContentId = occ
            .commit
            .parse()
            .map_err(|_| ApiError::http(400, format!("not a commit id: {}", occ.commit)))?;
        let Some((t, _, commit)) = commits.iter().find(|(_, c, _)| *c == cid) else {
            if widening.skip_foreign {
                continue;
            }
            return Err(ApiError::CommitNotFound(format!(
                "commit {} is not in {}'s history",
                occ.commit, read.ledger_id
            )));
        };
        let Some(flake) = commit.at_slot(occ.slot) else {
            return Err(ApiError::http(
                400,
                format!("commit {} has no operation {}", occ.commit, occ.slot),
            ));
        };
        located.push(Located {
            occurrence: occ.clone(),
            flake: flake.clone(),
            t: *t,
        });
    }
    for (t, cid, commit) in commits {
        for (slot, flake) in commit.occurrences() {
            if selectors.iter().any(|(g, s, p, o)| {
                flake.g == *g
                    && flake.s == *s
                    && flake.p == *p
                    && o.as_ref().is_none_or(|o| o.matches(flake))
            }) {
                located.push(Located {
                    occurrence: OccurrenceRef {
                        commit: cid.to_string(),
                        slot,
                    },
                    flake: flake.clone(),
                    t: *t,
                });
            }
        }
    }
    let events = fact_events(commits);
    let drops = graph_drops(read);
    if widening.pair_spans {
        let pairs: Vec<Located> = located
            .iter()
            .filter_map(|loc| span_pair(loc, &events, &drops, commits))
            .collect();
        located.extend(pairs);
    }
    located.sort_by(|a, b| {
        a.occurrence
            .commit
            .cmp(&b.occurrence.commit)
            .then(a.occurrence.slot.cmp(&b.occurrence.slot))
    });
    located.dedup_by(|a, b| a.occurrence == b.occurrence);
    if located.is_empty() {
        return Err(ApiError::http(
            404,
            "nothing in the ledger's history matches the removal",
        ));
    }
    if widening.bounded {
        for loc in &located {
            let Some(pair) = span_pair(loc, &events, &drops, commits) else {
                continue;
            };
            if !located.iter().any(|l| l.occurrence == pair.occurrence) {
                return Err(ApiError::http(
                    409,
                    format!(
                        "commit {} of {} holds a value erased in commit {}, and is not \
                         erased with it; this storage cannot erase it without a prover",
                        pair.occurrence.commit, read.ledger_id, loc.occurrence.commit
                    ),
                ));
            }
        }
    }

    // The span of each occurrence: an assertion runs to the fact's
    // next retraction or its graph's next drop; a retraction ends the
    // assertion before it.
    let mut spans: Vec<(i64, Option<i64>)> = Vec::with_capacity(located.len());
    let mut current: Vec<Flake> = Vec::new();
    for loc in &located {
        let key = FactKey::of(&loc.flake);
        let timeline = events.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        if loc.flake.op {
            let retracted = timeline
                .iter()
                .find(|e| e.t > loc.flake.t && !e.op)
                .map(|e| e.t);
            let until = retracted
                .into_iter()
                .chain(dropped_after(&drops, &loc.flake.g, loc.flake.t))
                .min();
            if until.is_none() {
                current.push(loc.flake.clone());
            }
            spans.push((loc.flake.t, until));
        } else {
            let from = timeline
                .iter()
                .rev()
                .find(|e| e.t < loc.flake.t && e.op)
                .map_or(0, |e| e.t);
            spans.push((from, Some(loc.flake.t)));
        }
    }
    Ok(Found {
        located,
        spans,
        current,
    })
}

/// When each graph was dropped, ascending, keyed as a flake names its graph
/// (`None` for the default graph).
fn graph_drops(read: &History) -> HashMap<Option<Sid>, Vec<i64>> {
    let mut drops: HashMap<Option<Sid>, Vec<i64>> = HashMap::new();
    for (t, _, commit) in &read.commits {
        for iri in &commit.graph_drops {
            let graph = if iri == fluree_db_core::DEFAULT_GRAPH_IRI {
                None
            } else {
                let Some(sid) = read.state.snapshot.encode_iri(iri) else {
                    continue;
                };
                Some(sid)
            };
            drops.entry(graph).or_default().push(*t);
        }
    }
    for ts in drops.values_mut() {
        ts.sort_unstable();
    }
    drops
}

/// The first drop of `graph` after `t`, which ends what it held at `t`. A
/// drop applies before its own commit's writes.
fn dropped_after(
    drops: &HashMap<Option<Sid>, Vec<i64>>,
    graph: &Option<Sid>,
    t: i64,
) -> Option<i64> {
    drops.get(graph)?.iter().copied().find(|d| *d > t)
}

/// The occurrence bounding `loc`'s span in `commits`: an assertion's
/// retraction, unless its graph's drop ended it first; a retraction's
/// assertion.
fn span_pair(
    loc: &Located,
    events: &HashMap<FactKey, Vec<Event>>,
    drops: &HashMap<Option<Sid>, Vec<i64>>,
    commits: &[(i64, ContentId, fluree_db_core::Commit)],
) -> Option<Located> {
    let timeline = events.get(&FactKey::of(&loc.flake))?;
    let pair = if loc.flake.op {
        let dropped = dropped_after(drops, &loc.flake.g, loc.t);
        timeline
            .iter()
            .find(|e| e.t > loc.t && !e.op)
            .filter(|e| dropped.is_none_or(|d| e.t < d))
    } else {
        timeline.iter().rev().find(|e| e.t < loc.t && e.op)
    }?;
    let (t, cid, commit) = &commits[pair.commit];
    Some(Located {
        occurrence: OccurrenceRef {
            commit: cid.to_string(),
            slot: pair.slot,
        },
        flake: commit.at_slot(pair.slot)?.clone(),
        t: *t,
    })
}

/// The journal entries recording `located`, each with its span; a span
/// still open ends at `retracted_t` when the removal retracted it.
fn entries(
    read: &History,
    request_id: &str,
    located: &[Located],
    spans: &[(i64, Option<i64>)],
    retracted_t: Option<i64>,
    requester: Option<&str>,
) -> Vec<Removal> {
    let snapshot = &read.state.snapshot;
    located
        .iter()
        .zip(spans)
        .map(|(loc, (from, until))| Removal {
            request_id: request_id.to_string(),
            commit: loc.occurrence.commit.clone(),
            slot: loc.occurrence.slot,
            predicate: snapshot.decode_sid(&loc.flake.p).unwrap_or_default(),
            from_t: *from,
            until_t: until.or(retracted_t),
            scope_t: read.scope_t,
            subject: snapshot
                .decode_sid(&loc.flake.s)
                .map(|iri| super::journal::subject_fingerprint(&iri)),
            requester: requester.map(str::to_string),
        })
        .collect()
}

#[cfg(feature = "native")]
/// A fact whose object is a triple term spelling a value an erasure takes
/// out: it keeps the value readable, in the index and in its own commit.
pub(crate) struct Spelling {
    pub(crate) commit: ContentId,
    pub(crate) slot: u32,
    pub(crate) t: i64,
    pub(crate) fact: Flake,
}

#[cfg(feature = "native")]
/// The facts of `commit` whose object is a triple term, at any depth,
/// spelling one of `facts`, other than the occurrences `named`.
fn spellings(
    t: i64,
    cid: &ContentId,
    commit: &fluree_db_core::Commit,
    facts: &HashSet<TripleTermValue>,
    named: &BTreeMap<&str, HashSet<u32>>,
) -> Vec<Spelling> {
    fn spells(mut value: &FlakeValue, facts: &HashSet<TripleTermValue>) -> bool {
        while let FlakeValue::TripleTerm(term) = value {
            if facts.contains(term.as_ref()) {
                return true;
            }
            value = &term.o;
        }
        false
    }
    let id = cid.to_string();
    let named = named.get(id.as_str());
    let mut found = Vec::new();
    for (slot, flake) in commit.occurrences() {
        if !named.is_some_and(|slots| slots.contains(&slot)) && spells(&flake.o, facts) {
            found.push(Spelling {
                commit: cid.clone(),
                slot,
                t,
                fact: flake.clone(),
            });
        }
    }
    found
}

#[cfg(feature = "native")]
/// Why an erasure leaving `spelled` on `ledger_id` cannot be made: what must
/// go with it. `snapshot` names its terms, when the ledger can be read.
#[cfg(feature = "native")]
pub(crate) fn spelled_refusal(
    snapshot: Option<&fluree_db_core::LedgerSnapshot>,
    ledger_id: &str,
    spelled: &[Spelling],
) -> String {
    let iri = |sid: &Sid| {
        snapshot
            .and_then(|snapshot| snapshot.decode_sid(sid))
            .map_or_else(|| format!("{sid:?}"), |iri| format!("<{iri}>"))
    };
    let named: Vec<String> = spelled
        .iter()
        .take(5)
        .map(|s| {
            format!(
                "{} {} (commit {}, slot {}, t={})",
                iri(&s.fact.s),
                iri(&s.fact.p),
                s.commit,
                s.slot,
                s.t
            )
        })
        .collect();
    let more = spelled.len().saturating_sub(named.len());
    format!(
        "on {ledger_id}, {} name a triple term spelling a value this erasure removes, \
         which would stay readable through them: {}{}. Name those occurrences in the \
         erasure too",
        match spelled.len() {
            1 => "1 fact".to_string(),
            n => format!("{n} facts"),
        },
        named.join("; "),
        if more > 0 {
            format!(" and {more} more")
        } else {
            String::new()
        }
    )
}

#[cfg(feature = "native")]
impl Fluree {
    /// The occurrences `request` names on `ledger_id`, widened as
    /// `widening` says: what a removal there would take out.
    pub(crate) async fn located_occurrences(
        &self,
        ledger_id: &str,
        request: &RemovalRequest,
        widening: Widening,
    ) -> Result<Vec<OccurrenceRef>> {
        for _ in 0..ATTEMPTS {
            let Some(read) = self.read_history(ledger_id, None).await? else {
                continue;
            };
            let found = locate(&read, request, widening)?;
            return Ok(found.located.into_iter().map(|l| l.occurrence).collect());
        }
        Err(ApiError::commit_contention(
            ATTEMPTS,
            ApiError::internal("the head kept moving while the erasure was checked"),
        ))
    }

    /// Why one of `holders` would keep a value `occurrences` erase readable,
    /// through a fact, now or in its history, naming a triple term that
    /// spells it, if one would. An erasure is checked by this when it is
    /// accepted, received and published, against every ledger holding its
    /// commits.
    pub(crate) async fn spelled_in(
        &self,
        holders: &[(
            fluree_db_nameservice::NsRecord,
            std::sync::Arc<dyn fluree_db_core::ContentStore>,
        )],
        occurrences: &[OccurrenceRef],
    ) -> Result<Option<String>> {
        let mut facts = HashSet::new();
        let mut slots: BTreeMap<&str, HashSet<u32>> = BTreeMap::new();
        for occurrence in occurrences {
            slots
                .entry(occurrence.commit.as_str())
                .or_default()
                .insert(occurrence.slot);
        }
        for (commit, slots) in &slots {
            let cid: ContentId = commit
                .parse()
                .map_err(|_| ApiError::internal(format!("not a commit id: {commit}")))?;
            for (_, store) in holders {
                if store.has(&cid).await? {
                    let original = load_commit_by_id(store.as_ref(), &cid).await?;
                    // TripleTermValue is the graph-independent identity
                    // used by the terms themselves, including datatype and
                    // language. Repeated occurrences need only one key.
                    facts.extend(slots.iter().filter_map(|s| original.at_slot(*s)).map(|f| {
                        TripleTermValue {
                            s: f.s.clone(),
                            p: f.p.clone(),
                            o: f.o.clone(),
                            dt: f.dt.clone(),
                            lang: f.m.as_ref().and_then(|m| m.lang.clone()),
                        }
                    }));
                    break;
                }
            }
        }
        if facts.is_empty() {
            return Ok(None);
        }
        for (holder, store) in holders {
            let Some(head) = holder.commit_head_id.as_ref() else {
                continue;
            };
            let mut found = Vec::new();
            for (t, cid) in collect_first_parent_cids(store.as_ref(), head, 0).await? {
                let commit = load_commit_by_id(store.as_ref(), &cid).await?;
                found.extend(spellings(t, &cid, &commit, &facts, &slots));
            }
            if !found.is_empty() {
                // A dropped holder is not read: its terms go unnamed.
                let state = self.ledger(holder.ledger_id.as_str()).await.ok();
                return Ok(Some(spelled_refusal(
                    state.as_ref().map(|s| &*s.snapshot),
                    holder.ledger_id.as_str(),
                    &found,
                )));
            }
        }
        Ok(None)
    }
}

/// Each fact's assertions and retractions, in commit order.
fn fact_events(
    commits: &[(i64, ContentId, fluree_db_core::Commit)],
) -> HashMap<FactKey, Vec<Event>> {
    let mut events: HashMap<FactKey, Vec<Event>> = HashMap::new();
    for (index, (t, _, commit)) in commits.iter().enumerate() {
        for (slot, flake) in commit.occurrences() {
            events.entry(FactKey::of(flake)).or_default().push(Event {
                t: *t,
                op: flake.op,
                commit: index,
                slot,
            });
        }
    }
    events
}

/// The terms a selector names, in the ledger's encoding.
/// A selector's object: its value, and the datatype and language that
/// must match too where it names them.
struct ObjectTerm {
    value: FlakeValue,
    datatype: Option<Sid>,
    /// A language tag, or `Some(None)` for a value with none.
    language: Option<Option<String>>,
}

impl ObjectTerm {
    fn matches(&self, flake: &Flake) -> bool {
        flake.o == self.value
            && self.datatype.as_ref().is_none_or(|dt| flake.dt == *dt)
            && self.language.as_ref().is_none_or(|lang| {
                let held = flake.m.as_ref().and_then(|m| m.lang.as_deref());
                match (lang.as_deref(), held) {
                    (Some(lang), Some(held)) => lang.eq_ignore_ascii_case(held),
                    (None, None) => true,
                    _ => false,
                }
            })
    }
}

fn selector_terms(
    snapshot: &fluree_db_core::LedgerSnapshot,
    sel: &FactSelector,
) -> Result<(Option<Sid>, Sid, Sid, Option<ObjectTerm>)> {
    let encode = |iri: &str| {
        snapshot
            .encode_iri(iri)
            .ok_or_else(|| ApiError::http(404, format!("<{iri}> does not occur in the ledger")))
    };
    let shape = || {
        ApiError::http(
            400,
            "a selector object is a scalar, {\"@id\": ...}, or {\"@value\": ...} with \
             \"@type\" or \"@language\"",
        )
    };
    let graph = sel.graph.as_deref().map(encode).transpose()?;
    let subject = encode(&sel.subject)?;
    let predicate = encode(&sel.predicate)?;
    let any = |value| ObjectTerm {
        value,
        datatype: None,
        language: None,
    };
    let object = match &sel.object {
        None => None,
        Some(serde_json::Value::Object(map)) => {
            if let Some(iri) = map.get("@id").and_then(|v| v.as_str()) {
                Some(any(FlakeValue::Ref(encode(iri)?)))
            } else {
                let value = map.get("@value").ok_or_else(shape)?;
                match (map.get("@type"), map.get("@language")) {
                    (Some(dt), None) => {
                        let dt = datatype_iri(dt.as_str().ok_or_else(shape)?)?;
                        let dt = dt.as_str();
                        let coerced = match value {
                            serde_json::Value::String(s) => {
                                fluree_db_core::coerce::coerce_string_value(s, dt)
                            }
                            other => fluree_db_core::coerce::coerce_value(scalar(other)?, dt),
                        }
                        .map_err(|e| ApiError::http(400, format!("selector object: {e}")))?;
                        Some(ObjectTerm {
                            value: coerced,
                            datatype: Some(encode(dt)?),
                            language: None,
                        })
                    }
                    (None, Some(lang)) => Some(ObjectTerm {
                        value: FlakeValue::String(value.as_str().ok_or_else(shape)?.to_string()),
                        datatype: None,
                        language: Some(Some(lang.as_str().ok_or_else(shape)?.to_string())),
                    }),
                    _ => return Err(shape()),
                }
            }
        }
        // A plain string is an `xsd:string`: not the same text with a
        // language or another datatype.
        Some(serde_json::Value::String(s)) => Some(ObjectTerm {
            value: FlakeValue::String(s.clone()),
            datatype: Some(encode(fluree_vocab::xsd::STRING)?),
            language: Some(None),
        }),
        Some(other) => Some(any(scalar(other)?)),
    };
    Ok((graph, subject, predicate, object))
}

/// A selector's `@type` as an IRI: an absolute IRI, `@json`, or a compact
/// `xsd:` or `rdf:` IRI. A selector has no context to expand any other.
fn datatype_iri(dt: &str) -> Result<String> {
    if dt == "@json" {
        return Ok(fluree_vocab::rdf::JSON.to_string());
    }
    for (prefix, ns) in [
        ("xsd:", fluree_vocab::xsd::NS),
        ("rdf:", fluree_vocab::rdf::NS),
    ] {
        if let Some(local) = dt.strip_prefix(prefix) {
            return Ok(format!("{ns}{local}"));
        }
    }
    match dt.split_once(':') {
        Some((scheme, _)) if !scheme.is_empty() && !dt.starts_with('@') => Ok(dt.to_string()),
        _ => Err(ApiError::http(
            400,
            format!(
                "selector object @type {dt:?} is not an absolute IRI, @json, or an xsd: or rdf: \
                 compact IRI"
            ),
        )),
    }
}

/// A JSON boolean or number as a value.
fn scalar(value: &serde_json::Value) -> Result<FlakeValue> {
    match value {
        serde_json::Value::Bool(b) => Ok(FlakeValue::Boolean(*b)),
        serde_json::Value::Number(n) => Ok(match n.as_i64() {
            Some(i) => FlakeValue::Long(i),
            None => FlakeValue::Double(n.as_f64().unwrap_or_default()),
        }),
        serde_json::Value::String(s) => Ok(FlakeValue::String(s.clone())),
        _ => Err(ApiError::http(
            400,
            "a selector object is a scalar, {\"@id\": ...}, or {\"@value\": ...} with \
             \"@type\" or \"@language\"",
        )),
    }
}

/// The stored flakes stating `flake`'s fact at the head, as retractions.
pub(super) async fn stored_terms(
    state: &fluree_db_ledger::LedgerState,
    flake: &Flake,
) -> Result<Vec<Flake>> {
    let reverse = state.snapshot.build_reverse_graph()?;
    let g_id = match &flake.g {
        None => 0,
        Some(g) => match reverse.get(g) {
            Some(id) => *id,
            None => return Ok(Vec::new()),
        },
    };
    let key = FactKey::of(flake);
    let found = fluree_db_core::range_with_overlay(
        &state.snapshot,
        g_id,
        state.novelty.as_ref(),
        fluree_db_core::IndexType::Spot,
        fluree_db_core::RangeTest::Eq,
        fluree_db_core::RangeMatch {
            s: Some(flake.s.clone()),
            p: Some(flake.p.clone()),
            o: Some(flake.o.clone()),
            dt: Some(flake.dt.clone()),
            t: None,
        },
        fluree_db_core::RangeOptions {
            to_t: Some(state.t()),
            ..Default::default()
        },
    )
    .await?;
    Ok(found
        .into_iter()
        .map(|f| Flake {
            g: flake.g.clone(),
            op: false,
            ..f
        })
        .filter(|f| key.matches(f))
        .collect())
}

/// A deterministic id for a request: its ledger, scope and manifest.
fn request_id(ledger_id: &str, scope_t: i64, located: &[Located]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(ledger_id.as_bytes());
    hasher.update(scope_t.to_le_bytes());
    for loc in located {
        hasher.update(loc.occurrence.commit.as_bytes());
        hasher.update(loc.occurrence.slot.to_le_bytes());
    }
    format!("removal-{}", hex::encode(&hasher.finalize()[..12]))
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use super::*;

    #[test]
    fn spellings_match_nested_terms_by_identity_and_keep_original_slots() {
        let selected = TripleTermValue {
            s: Sid::new(100, "alice"),
            p: Sid::new(100, "name"),
            o: FlakeValue::String("secret".into()),
            dt: Sid::new(fluree_vocab::namespaces::RDF, "langString"),
            lang: Some("en".into()),
        };
        let term = |value: TripleTermValue| FlakeValue::TripleTerm(Box::new(value));
        let values = vec![
            FlakeValue::String("secret".into()),
            term(selected.clone()),
            term(TripleTermValue {
                o: term(selected.clone()),
                ..selected.clone()
            }),
            term(TripleTermValue {
                s: Sid::new(100, "bob"),
                ..selected.clone()
            }),
            term(TripleTermValue {
                dt: Sid::xsd_string(),
                ..selected.clone()
            }),
            term(TripleTermValue {
                lang: Some("fr".into()),
                ..selected.clone()
            }),
            term(selected.clone()),
        ];
        let mut commit = fluree_db_core::Commit::new(
            9,
            values
                .into_iter()
                .map(|o| {
                    Flake::new_in_graph(
                        Sid::new(100, "another-graph"),
                        Sid::new(100, "claim"),
                        Sid::new(fluree_vocab::namespaces::RDF, "reifies"),
                        o,
                        Sid::xsd_string(),
                        9,
                        true,
                        None,
                    )
                })
                .collect(),
        );
        commit.redaction = Some(fluree_db_core::commit::Redaction {
            slots: vec![2, 4, 6, 8, 10, 12, 14],
            op_count: 15,
            signers: Vec::new(),
        });
        let cid = ContentId::new(fluree_db_core::ContentKind::Commit, b"spelling-test");
        let id = cid.to_string();
        let named = BTreeMap::from([(id.as_str(), HashSet::from([14]))]);
        let found = spellings(9, &cid, &commit, &HashSet::from([selected]), &named);
        assert_eq!(found.iter().map(|s| s.slot).collect::<Vec<_>>(), [4, 6]);
        assert!(found.iter().all(|s| s.t == 9 && s.commit == cid));
    }
}
