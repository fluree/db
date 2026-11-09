//! The removal journal: a branch's authoritative record of what its
//! history no longer serves.
//!
//! Two kinds of suppression:
//! - **History rules** (`f:noHistory`): a predicate's obsolete assertions and
//!   retractions over a covered range of transactions.
//! - **Removals**: explicitly selected assertion occurrences, by original
//!   commit and operation slot.
//!
//! The journal lives in the branch's nameservice config record (beside the
//! retention registration), outside ledger content, so restoring or
//! replaying the ledger does not roll it back, and a cluster replicates it
//! with its other nameservice state. It records predicates, transaction
//! ranges, occurrence positions and a fingerprint of each removed
//! occurrence's subject — never the suppressed values.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A removed occurrence's subject as its journal entry records it: the
/// expanded IRI hashed under a versioned domain tag. It matches reads by
/// identity and keeps the IRI itself out of the journal; it does not hide
/// an IRI that can be guessed.
pub(crate) fn subject_fingerprint(iri: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"fluree:removal-subject:v1\0");
    hash.update(iri.as_bytes());
    format!("v1:{}", hex::encode(hash.finalize()))
}

/// Two entries for one occurrence that disagree on its subject: one of
/// them is corrupt.
#[derive(Debug)]
pub(crate) struct SubjectConflict {
    pub(crate) commit: String,
    pub(crate) slot: u32,
}

impl std::fmt::Display for SubjectConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "removal journals disagree on the subject of operation {} of commit {}",
            self.slot, self.commit
        )
    }
}

impl From<SubjectConflict> for crate::ApiError {
    fn from(e: SubjectConflict) -> Self {
        crate::ApiError::internal(e.to_string())
    }
}

/// The removal journal.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Journal {
    /// Bumped on every change; a served view honors the generation current
    /// when it is prepared.
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history_rules: Vec<HistoryRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removals: Vec<Removal>,
}

/// A predicate whose obsolete history is suppressed in `[from_t, until_t)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryRule {
    /// The predicate IRI.
    pub predicate: String,
    /// The graph IRI the rule governs; `None` for every data graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
    /// Graphs a ledger-wide rule does not govern (a graph's explicit
    /// `f:noHistory false`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub except_graphs: Vec<String>,
    /// First transaction whose history is covered. Enabling `f:noHistory`
    /// covers existing history too, so this is `0` unless the rule was
    /// recorded otherwise.
    pub from_t: i64,
    /// The config commit that turned the rule off: history from here on is
    /// retained again. `None` while the rule is in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_t: Option<i64>,
    /// The config commit that turned it on.
    pub enabled_t: i64,
}

impl HistoryRule {
    /// Whether this rule covers a read as of `t`.
    pub fn covers(&self, t: i64) -> bool {
        self.from_t <= t && self.until_t.is_none_or(|until| t < until)
    }

    /// Whether this rule covers any transaction in `[from, to]`.
    pub fn overlaps(&self, from: i64, to: i64) -> bool {
        self.from_t <= to && self.until_t.is_none_or(|until| from < until)
    }
}

/// One removed occurrence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Removal {
    /// The request that removed it.
    pub request_id: String,
    /// Original commit (CID string) and the position of the occurrence's
    /// operation in it.
    pub commit: String,
    pub slot: u32,
    /// The predicate IRI, so a read can tell whether it might need the
    /// occurrence without resolving it.
    pub predicate: String,
    /// The occurrence's span: asserted at `from_t`, retracted at `until_t`
    /// (the removal's own retraction when it was still current).
    pub from_t: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_t: Option<i64>,
    /// The head the request's scope was fixed at.
    pub scope_t: i64,
    /// The [fingerprint](subject_fingerprint) of the occurrence's subject,
    /// recorded with the removal: it stays when the occurrence's commit no
    /// longer says (an erasure redacts the operation). `None` matches any
    /// subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

impl Removal {
    fn key(&self) -> (&str, u32) {
        (&self.commit, self.slot)
    }

    pub fn covers(&self, t: i64) -> bool {
        self.from_t <= t && self.until_t.is_none_or(|until| t < until)
    }

    /// Whether a history range `[from, to]` could list the occurrence: its
    /// assertion, or the retraction at `until_t` that ended it.
    pub fn overlaps(&self, from: i64, to: i64) -> bool {
        self.from_t <= to && self.until_t.is_none_or(|until| from <= until)
    }
}

impl Journal {
    pub fn is_empty(&self) -> bool {
        self.history_rules.is_empty() && self.removals.is_empty()
    }

    /// The rule in force for `predicate` in `graph`, if any.
    pub fn active_rule(&self, predicate: &str, graph: Option<&str>) -> Option<&HistoryRule> {
        self.history_rules.iter().find(|r| {
            r.until_t.is_none() && r.predicate == predicate && r.graph.as_deref() == graph
        })
    }
}

impl Journal {
    /// Add what `other` suppresses that this journal does not: every removal
    /// (by commit and slot) and every history rule (by predicate, graph and
    /// the commit that enabled it), an open rule winning over its closed
    /// copy since coverage only grows. A removal held without its subject
    /// takes it from `other`'s copy. Returns whether anything was added;
    /// the generation then moves past both.
    pub(crate) fn merge(&mut self, other: &Journal) -> Result<bool, SubjectConflict> {
        let mut changed = false;
        // Borrow only incoming keys, then locate their first occurrences in
        // one pass. Stop as soon as all are found; an empty incoming journal
        // never scans ours. Appends below join the same index.
        let mut positions: std::collections::HashMap<
            &str,
            std::collections::HashMap<u32, Option<usize>>,
        > = std::collections::HashMap::new();
        let mut missing = 0;
        for removal in &other.removals {
            let (commit, slot) = removal.key();
            if positions
                .entry(commit)
                .or_default()
                .insert(slot, None)
                .is_none()
            {
                missing += 1;
            }
        }
        if missing > 0 {
            for (i, removal) in self.removals.iter().enumerate() {
                let (commit, slot) = removal.key();
                if let Some(position @ None) = positions
                    .get_mut(commit)
                    .and_then(|slots| slots.get_mut(&slot))
                {
                    *position = Some(i);
                    missing -= 1;
                    if missing == 0 {
                        break;
                    }
                }
            }
        }
        for removal in &other.removals {
            let (commit, slot) = removal.key();
            let position = positions
                .get_mut(commit)
                .and_then(|slots| slots.get_mut(&slot))
                .expect("indexed above");
            match position {
                None => {
                    *position = Some(self.removals.len());
                    self.removals.push(removal.clone());
                    changed = true;
                }
                Some(i) => {
                    let held = &mut self.removals[*i];
                    match (&held.subject, &removal.subject) {
                        (None, Some(subject)) => {
                            held.subject = Some(subject.clone());
                            changed = true;
                        }
                        (Some(a), Some(b)) if a != b => {
                            return Err(SubjectConflict {
                                commit: removal.commit.clone(),
                                slot: removal.slot,
                            })
                        }
                        _ => {}
                    }
                }
            }
        }
        for rule in &other.history_rules {
            let same = |r: &&mut HistoryRule| {
                r.predicate == rule.predicate
                    && r.graph == rule.graph
                    && r.enabled_t == rule.enabled_t
                    && r.from_t == rule.from_t
            };
            match self.history_rules.iter_mut().find(same) {
                Some(existing) => {
                    if existing.until_t.is_some() && rule.until_t.is_none() {
                        existing.until_t = None;
                        changed = true;
                    }
                }
                None => {
                    self.history_rules.push(rule.clone());
                    changed = true;
                }
            }
        }
        if changed {
            self.generation = self.generation.max(other.generation) + 1;
        }
        Ok(changed)
    }

    /// What of this journal `base` lacks: the entries [`Self::merge`] would
    /// add to it, or the subjects it would add to them.
    pub fn missing_from(&self, base: &Journal) -> Journal {
        let held: std::collections::HashMap<(&str, u32), bool> = base
            .removals
            .iter()
            .map(|r| (r.key(), r.subject.is_some()))
            .collect();
        Journal {
            generation: self.generation,
            removals: self
                .removals
                .iter()
                .filter(|r| match held.get(&r.key()) {
                    None => true,
                    Some(known) => !known && r.subject.is_some(),
                })
                .cloned()
                .collect(),
            history_rules: self
                .history_rules
                .iter()
                .filter(|rule| {
                    !base.history_rules.iter().any(|b| {
                        b.predicate == rule.predicate
                            && b.graph == rule.graph
                            && b.enabled_t == rule.enabled_t
                            && b.from_t == rule.from_t
                            && (b.until_t.is_none() || rule.until_t.is_some())
                    })
                })
                .cloned()
                .collect(),
        }
    }

    /// Bring the history rules in force in line with `desired` (as compiled
    /// from the config), recording a change at config commit `t`: a rule no
    /// longer desired is closed at `t` (its history stays suppressed), a new
    /// one opens covering existing history. Returns whether anything changed;
    /// the generation is bumped when it did.
    ///
    /// A policy from before a rule was enabled leaves the rule alone: a
    /// rediscovery replays earlier policies, and a rule merged from another
    /// copy of the lineage was enabled in history this copy may not hold.
    pub fn apply_history_rules(
        &mut self,
        desired: &[(String, Option<String>, Vec<String>)],
        t: i64,
    ) -> bool {
        let mut changed = false;
        for rule in self
            .history_rules
            .iter_mut()
            .filter(|r| r.until_t.is_none() && r.enabled_t <= t)
        {
            let wanted = desired
                .iter()
                .find(|(p, g, _)| *p == rule.predicate && *g == rule.graph);
            match wanted {
                None => {
                    rule.until_t = Some(t);
                    changed = true;
                }
                Some((_, _, except)) if *except != rule.except_graphs => {
                    rule.except_graphs = except.clone();
                    changed = true;
                }
                Some(_) => {}
            }
        }
        for (predicate, graph, except) in desired {
            if self.active_rule(predicate, graph.as_deref()).is_none() {
                self.history_rules.push(HistoryRule {
                    predicate: predicate.clone(),
                    graph: graph.clone(),
                    except_graphs: except.clone(),
                    from_t: 0,
                    until_t: None,
                    enabled_t: t,
                });
                changed = true;
            }
        }
        if changed {
            self.generation += 1;
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(from_t: i64, until_t: Option<i64>) -> HistoryRule {
        HistoryRule {
            predicate: "ex:p".into(),
            graph: None,
            except_graphs: vec![],
            from_t,
            until_t,
            enabled_t: 3,
        }
    }

    #[test]
    fn coverage_is_half_open() {
        let r = rule(0, Some(10));
        assert!(r.covers(0) && r.covers(9) && !r.covers(10));
        assert!(r.overlaps(5, 20) && r.overlaps(9, 9) && !r.overlaps(10, 20));
        let open = rule(0, None);
        assert!(open.covers(1_000_000) && open.overlaps(50, 60));
    }

    #[test]
    fn disabling_closes_a_rule_and_reenabling_opens_another() {
        let mut j = Journal::default();
        let on = vec![("ex:p".to_string(), None, vec![])];
        assert!(j.apply_history_rules(&on, 5));
        assert!(
            !j.apply_history_rules(&on, 6),
            "no change, no new generation"
        );
        assert_eq!(j.generation, 1);
        assert!(j.apply_history_rules(&[], 9));
        assert_eq!(j.history_rules[0].until_t, Some(9));
        assert!(j.apply_history_rules(&on, 12));
        assert_eq!(j.history_rules.len(), 2);
        assert_eq!(j.generation, 3);
        // The closed rule still covers what it suppressed.
        assert!(j.history_rules[0].covers(8));
    }

    #[test]
    fn a_policy_from_before_a_rule_was_enabled_leaves_it_open() {
        let mut j = Journal::default();
        j.apply_history_rules(&[("ex:p".to_string(), None, vec![])], 5);
        assert!(!j.apply_history_rules(&[], 0), "replayed earlier policy");
        assert!(!j.apply_history_rules(&[], 4));
        assert_eq!(j.history_rules[0].until_t, None);
        assert!(j.apply_history_rules(&[], 7));
        assert_eq!(j.history_rules[0].until_t, Some(7));
    }

    #[test]
    fn what_a_base_lacks_is_what_merging_adds() {
        let removal = |slot| Removal {
            request_id: "r".into(),
            commit: "bafy".into(),
            slot,
            predicate: "ex:q".into(),
            from_t: 1,
            until_t: Some(2),
            scope_t: 1,
            subject: None,
        };
        let ours = Journal {
            generation: 4,
            history_rules: vec![
                rule(0, None),
                HistoryRule {
                    enabled_t: 9,
                    ..rule(0, Some(12))
                },
            ],
            removals: vec![removal(1), removal(2)],
        };
        let base = Journal {
            generation: 1,
            history_rules: vec![
                rule(0, Some(7)),
                HistoryRule {
                    enabled_t: 9,
                    ..rule(0, None)
                },
            ],
            removals: vec![removal(2)],
        };
        let missing = ours.missing_from(&base);
        assert_eq!(missing.removals, vec![removal(1)]);
        // The open copy wins over the closed one; a closed copy adds nothing.
        assert_eq!(missing.history_rules, vec![rule(0, None)]);
        let mut merged = base.clone();
        assert!(merged.merge(&missing).unwrap());
        assert!(ours.missing_from(&merged).is_empty());
    }

    /// A removal held without its subject takes it from a copy that has it,
    /// never the other way; two copies naming different subjects for one
    /// occurrence are an error.
    #[test]
    fn a_known_subject_is_merged_in_and_never_lost() {
        let removal = |subject: Option<&str>| Removal {
            request_id: "r".into(),
            commit: "bafy".into(),
            slot: 3,
            predicate: "ex:q".into(),
            from_t: 1,
            until_t: Some(2),
            scope_t: 1,
            subject: subject.map(str::to_string),
        };
        let journal = |subject| Journal {
            generation: 1,
            removals: vec![removal(subject)],
            ..Default::default()
        };
        let (unknown, known) = (journal(None), journal(Some("v1:aa")));

        assert_eq!(known.missing_from(&unknown).removals.len(), 1);
        assert!(unknown.missing_from(&known).is_empty());
        let mut merged = unknown.clone();
        assert!(merged.merge(&known).unwrap());
        assert_eq!(merged.removals[0].subject.as_deref(), Some("v1:aa"));
        assert_eq!(merged.generation, 2);
        assert!(!merged.merge(&unknown).unwrap());
        assert_eq!(merged.removals[0].subject.as_deref(), Some("v1:aa"));
        assert!(merged.merge(&journal(Some("v1:bb"))).is_err());
    }

    #[test]
    fn a_fingerprint_is_versioned_and_matches_only_its_iri() {
        let a = subject_fingerprint("http://example.org/carol");
        assert!(a.starts_with("v1:") && a.len() == 3 + 64);
        assert_eq!(a, subject_fingerprint("http://example.org/carol"));
        assert_ne!(a, subject_fingerprint("http://example.org/erin"));
    }

    #[test]
    fn merging_repeated_incoming_occurrences_preserves_order_and_subject_checks() {
        let removal = |commit: &str, slot, subject: Option<&str>| Removal {
            request_id: "first".into(),
            commit: commit.into(),
            slot,
            predicate: "ex:p".into(),
            from_t: 1,
            until_t: Some(2),
            scope_t: 1,
            subject: subject.map(str::to_string),
        };
        let batch = Journal {
            generation: 7,
            removals: vec![
                removal("a", 1, None),
                removal("b", 1, None),
                removal("a", 2, None),
                Removal {
                    request_id: "later".into(),
                    ..removal("a", 1, Some("subject"))
                },
            ],
            ..Default::default()
        };
        let mut journal = Journal::default();
        assert!(journal.merge(&batch).unwrap());
        assert_eq!(journal.generation, 8);
        assert_eq!(
            journal.removals,
            [
                removal("a", 1, Some("subject")),
                removal("b", 1, None),
                removal("a", 2, None),
            ]
        );
        assert!(!journal.merge(&batch).unwrap());
        let conflicting = Journal {
            removals: vec![removal("c", 1, Some("one")), removal("c", 1, Some("two"))],
            ..Default::default()
        };
        assert!(journal.merge(&conflicting).is_err());
    }

    #[test]
    fn round_trips() {
        let j = Journal {
            generation: 2,
            history_rules: vec![rule(0, None)],
            removals: vec![Removal {
                request_id: "r1".into(),
                commit: "bafy".into(),
                slot: 4,
                predicate: "ex:q".into(),
                from_t: 2,
                until_t: Some(7),
                scope_t: 6,
                subject: Some(subject_fingerprint("http://example.org/carol")),
            }],
        };
        let back: Journal = serde_json::from_value(serde_json::to_value(&j).unwrap()).unwrap();
        assert_eq!(back, j);
    }
}
