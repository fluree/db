//! Property TTL policies compiled from one config version.

use std::collections::HashMap;

use fluree_db_core::ledger_config::{LedgerConfig, RetentionDefaults};
use fluree_db_core::{LedgerSnapshot, Sid};
use fluree_vocab::config_iris;

/// A predicate whose history is not served, with the graph it governs (`None`
/// for every data graph) and the graphs a ledger-wide rule excepts.
pub(crate) type NoHistoryRule = (String, Option<String>, Vec<String>);

/// The duration policy of each governed predicate, ledger-wide and for each
/// graph with its own retention entries. Resolved once per config version
/// and looked up by the IRIs of a flake's graph and predicate.
#[derive(Clone, Debug, Default)]
pub(crate) struct CompiledPolicy {
    /// Ledger-wide: predicate IRI → seconds (only positive durations).
    ledger: HashMap<String, u64>,
    /// Graph IRI (`None` = the default graph) → predicate IRI → seconds,
    /// already merged over the ledger-wide entries; `0` (an explicit
    /// override) is kept so it masks the inherited duration.
    graphs: HashMap<Option<String>, HashMap<String, u64>>,
    /// Problems the reader recorded: they block processing.
    pub(crate) problems: Vec<String>,
    /// `f:noHistory` in force: predicate IRI, the graph it is limited to
    /// (`None`: every data graph), and graphs excepted from a ledger-wide
    /// rule by their own explicit `false`.
    pub(crate) no_history: Vec<NoHistoryRule>,
}

impl CompiledPolicy {
    pub(crate) fn compile(config: Option<&LedgerConfig>) -> Self {
        let mut policy = Self::default();
        let Some(config) = config else {
            return policy;
        };
        let durations = |group: &RetentionDefaults| -> HashMap<String, u64> {
            group
                .properties
                .iter()
                .filter_map(|p| p.ttl_seconds.map(|s| (p.target.clone(), s)))
                .collect()
        };
        if let Some(group) = &config.retention {
            policy.problems.extend(group.problems.iter().cloned());
            policy.ledger = durations(group)
                .into_iter()
                .filter(|(_, s)| *s > 0)
                .collect();
        }
        for graph in &config.graph_overrides {
            let Some(group) = &graph.retention else {
                continue;
            };
            policy.problems.extend(group.problems.iter().cloned());
            let merged = match &config.retention {
                Some(base) => group.merge_over(base),
                None => group.clone(),
            };
            let key = if graph.target_graph == config_iris::DEFAULT_GRAPH {
                None
            } else {
                Some(graph.target_graph.clone())
            };
            policy.graphs.insert(key, durations(&merged));
        }
        policy.no_history = no_history_in_force(config);
        policy
    }

    /// Whether any predicate has a positive duration anywhere.
    #[cfg(test)]
    pub(crate) fn is_active(&self) -> bool {
        !self.ledger.is_empty() || self.graphs.values().any(|m| m.values().any(|s| *s > 0))
    }

    /// The duration for a predicate in a graph, if a positive one applies.
    pub(crate) fn duration(&self, graph: Option<&str>, predicate: &str) -> Option<u64> {
        let seconds = match self.graphs.get(&graph.map(str::to_string)) {
            Some(graph) => graph.get(predicate).copied(),
            None => self.ledger.get(predicate).copied(),
        };
        seconds.filter(|s| *s > 0)
    }
}

/// The `f:noHistory` rules a config puts in force, field-merged per graph.
fn no_history_in_force(config: &LedgerConfig) -> Vec<NoHistoryRule> {
    let graph_key = |target: &str| {
        if target == config_iris::DEFAULT_GRAPH {
            fluree_db_core::graph_registry::DEFAULT_GRAPH_IRI.to_string()
        } else {
            target.to_string()
        }
    };
    let mut rules = Vec::new();
    let ledger_wide: Vec<&str> = config
        .retention
        .iter()
        .flat_map(|g| g.properties.iter())
        .filter(|p| p.no_history == Some(true))
        .map(|p| p.target.as_str())
        .collect();
    for predicate in &ledger_wide {
        let except: Vec<String> = config
            .graph_overrides
            .iter()
            .filter(|gc| {
                gc.retention
                    .as_ref()
                    .and_then(|r| r.property(predicate))
                    .is_some_and(|e| e.no_history == Some(false))
            })
            .map(|gc| graph_key(&gc.target_graph))
            .collect();
        rules.push(((*predicate).to_string(), None, except));
    }
    for gc in &config.graph_overrides {
        let Some(group) = &gc.retention else {
            continue;
        };
        for entry in &group.properties {
            if entry.no_history == Some(true) && !ledger_wide.contains(&entry.target.as_str()) {
                rules.push((
                    entry.target.clone(),
                    Some(graph_key(&gc.target_graph)),
                    Vec::new(),
                ));
            }
        }
    }
    rules.sort();
    rules.dedup();
    rules
}

/// Decodes the Sids a policy lookup needs, once each.
#[derive(Default)]
pub(crate) struct IriCache(HashMap<Sid, Option<String>>);

impl IriCache {
    pub(crate) fn iri(&mut self, snapshot: &LedgerSnapshot, sid: &Sid) -> Option<String> {
        self.0
            .entry(sid.clone())
            .or_insert_with(|| snapshot.decode_sid(sid))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ledger_config::{GraphConfig, RetentionProperty};

    fn entry(target: &str, ttl: Option<u64>, no_history: Option<bool>) -> RetentionProperty {
        RetentionProperty {
            target: target.into(),
            no_history,
            ttl_seconds: ttl,
        }
    }

    #[test]
    fn graph_entries_merge_field_by_field_over_ledger_wide_ones() {
        let config = LedgerConfig {
            retention: Some(RetentionDefaults {
                properties: vec![
                    entry("ex:counter", Some(60), None),
                    entry("ex:x", Some(5), None),
                ],
                problems: vec![],
            }),
            graph_overrides: vec![
                GraphConfig {
                    target_graph: "ex:audited".into(),
                    // Overriding noHistory keeps the inherited TTL.
                    retention: Some(RetentionDefaults {
                        properties: vec![entry("ex:counter", None, Some(false))],
                        problems: vec![],
                    }),
                    ..Default::default()
                },
                GraphConfig {
                    target_graph: config_iris::DEFAULT_GRAPH.into(),
                    // An explicit 0 masks the inherited duration.
                    retention: Some(RetentionDefaults {
                        properties: vec![entry("ex:x", Some(0), None)],
                        problems: vec![],
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let policy = CompiledPolicy::compile(Some(&config));
        assert!(policy.is_active());
        assert_eq!(policy.duration(Some("ex:audited"), "ex:counter"), Some(60));
        assert_eq!(policy.duration(Some("ex:other"), "ex:counter"), Some(60));
        assert_eq!(policy.duration(None, "ex:counter"), Some(60));
        assert_eq!(policy.duration(None, "ex:x"), None);
        assert_eq!(policy.duration(Some("ex:other"), "ex:x"), Some(5));
        assert_eq!(policy.duration(None, "ex:y"), None);
    }

    #[test]
    fn an_empty_graph_group_does_not_clear_inherited_rules() {
        let config = LedgerConfig {
            retention: Some(RetentionDefaults {
                properties: vec![entry("ex:c", Some(10), None)],
                problems: vec![],
            }),
            graph_overrides: vec![GraphConfig {
                target_graph: "ex:g".into(),
                retention: Some(RetentionDefaults::default()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let policy = CompiledPolicy::compile(Some(&config));
        assert_eq!(policy.duration(Some("ex:g"), "ex:c"), Some(10));
    }
}
