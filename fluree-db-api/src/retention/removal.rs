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

/// Facts whose every occurrence a request removes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FactSelector {
    /// Subject IRI.
    pub subject: String,
    /// Predicate IRI.
    pub predicate: String,
    /// Named graph IRI; absent for the default graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
    /// The object: an IRI as `{"@id": ...}`, or a JSON string, number or
    /// boolean. Absent selects every object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<serde_json::Value>,
}

/// A removal request.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
        self.remove_widened(writer, ledger_id, &request, Widening::default())
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
            let removals = entries(&read, &request_id, &located, &spans, retracted_t);
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
            let removals = entries(&read, &request_id, &found.located, &found.spans, None);
            return Ok(Some(ReceivedRemoval {
                ledger_id,
                since: read.since(),
                scope_t: read.scope_t,
                index_t: read.index_t,
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
            index_t: record.index_t,
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
        let registration = registry::update(self.publisher()?, ledger_id, |current| {
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
        })
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
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    index_t: i64,
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
    /// The ledger's head `t`, and its index's, when it was computed.
    pub(crate) scope_t: i64,
    pub(crate) index_t: i64,
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
    let selectors: Vec<(Option<Sid>, Sid, Sid, Option<FlakeValue>)> = request
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
                    && o.as_ref().is_none_or(|o| flake.o == *o)
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
    if widening.pair_spans {
        let pairs: Vec<Located> = located
            .iter()
            .filter_map(|loc| span_pair(loc, &events, commits))
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
            let Some(pair) = span_pair(loc, &events, commits) else {
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
    // next retraction; a retraction ends the assertion before it.
    let mut spans: Vec<(i64, Option<i64>)> = Vec::with_capacity(located.len());
    let mut current: Vec<Flake> = Vec::new();
    for loc in &located {
        let key = FactKey::of(&loc.flake);
        let timeline = events.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        if loc.flake.op {
            let until = timeline
                .iter()
                .find(|e| e.t > loc.flake.t && !e.op)
                .map(|e| e.t);
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

/// The occurrence bounding `loc`'s span in `commits`: an assertion's
/// retraction, a retraction's assertion.
fn span_pair(
    loc: &Located,
    events: &HashMap<FactKey, Vec<Event>>,
    commits: &[(i64, ContentId, fluree_db_core::Commit)],
) -> Option<Located> {
    let timeline = events.get(&FactKey::of(&loc.flake))?;
    let pair = if loc.flake.op {
        timeline.iter().find(|e| e.t > loc.t && !e.op)
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
        })
        .collect()
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
fn selector_terms(
    snapshot: &fluree_db_core::LedgerSnapshot,
    sel: &FactSelector,
) -> Result<(Option<Sid>, Sid, Sid, Option<FlakeValue>)> {
    let encode = |iri: &str| {
        snapshot
            .encode_iri(iri)
            .ok_or_else(|| ApiError::http(404, format!("<{iri}> does not occur in the ledger")))
    };
    let graph = sel.graph.as_deref().map(encode).transpose()?;
    let subject = encode(&sel.subject)?;
    let predicate = encode(&sel.predicate)?;
    let object = match &sel.object {
        None => None,
        Some(serde_json::Value::Object(map)) => match map.get("@id").and_then(|v| v.as_str()) {
            Some(iri) => Some(FlakeValue::Ref(encode(iri)?)),
            None => {
                return Err(ApiError::http(
                    400,
                    "a selector object is {\"@id\": ...} or a scalar",
                ))
            }
        },
        Some(serde_json::Value::String(s)) => Some(FlakeValue::String(s.clone())),
        Some(serde_json::Value::Bool(b)) => Some(FlakeValue::Boolean(*b)),
        Some(serde_json::Value::Number(n)) => match n.as_i64() {
            Some(i) => Some(FlakeValue::Long(i)),
            None => Some(FlakeValue::Double(n.as_f64().unwrap_or_default())),
        },
        Some(_) => {
            return Err(ApiError::http(
                400,
                "a selector object is {\"@id\": ...} or a scalar",
            ))
        }
    };
    Ok((graph, subject, predicate, object))
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
