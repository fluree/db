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
//! ranges and occurrence positions — never the suppressed values.

use serde::{Deserialize, Serialize};

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
}

impl Removal {
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
    /// Bring the history rules in force in line with `desired` (as compiled
    /// from the config), recording a change at config commit `t`: a rule no
    /// longer desired is closed at `t` (its history stays suppressed), a new
    /// one opens covering existing history. Returns whether anything changed;
    /// the generation is bumped when it did.
    pub fn apply_history_rules(
        &mut self,
        desired: &[(String, Option<String>, Vec<String>)],
        t: i64,
    ) -> bool {
        let mut changed = false;
        for rule in self
            .history_rules
            .iter_mut()
            .filter(|r| r.until_t.is_none())
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
            }],
        };
        let back: Journal = serde_json::from_value(serde_json::to_value(&j).unwrap()).unwrap();
        assert_eq!(back, j);
    }
}
