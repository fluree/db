//! Which graph sources read which ledgers, for the maintenance workers.
//!
//! Both directions are keyed by canonical [`LedgerId`]. Commit events always
//! carry the canonical `name:branch`, while a graph source's persisted
//! `dependencies` are whatever its creator typed; keying by the parsed id is
//! what makes an index created with `ledger: "docs"` wake on `docs:main`
//! commits.

use fluree_db_core::{LedgerId, LedgerIdParseError};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub(crate) struct DependencyIndex {
    ledger_to_sources: HashMap<LedgerId, HashSet<LedgerId>>,
    source_to_ledgers: HashMap<LedgerId, HashSet<LedgerId>>,
}

impl DependencyIndex {
    /// Register (or re-register) `source` as reading `ledgers`.
    ///
    /// Edges from a previous registration are dropped first, so a ledger
    /// removed from the dependency list stops waking this source.
    pub(crate) fn register(&mut self, source: &LedgerId, ledgers: &[LedgerId]) {
        self.unregister(source);
        let ledgers: HashSet<LedgerId> = ledgers.iter().cloned().collect();
        for ledger in &ledgers {
            self.ledger_to_sources
                .entry(ledger.clone())
                .or_default()
                .insert(source.clone());
        }
        self.source_to_ledgers.insert(source.clone(), ledgers);
    }

    /// Remove every edge for `source`. Returns whether it was registered.
    pub(crate) fn unregister(&mut self, source: &LedgerId) -> bool {
        let Some(ledgers) = self.source_to_ledgers.remove(source) else {
            return false;
        };
        for ledger in ledgers {
            if let Some(sources) = self.ledger_to_sources.get_mut(&ledger) {
                sources.remove(source);
                if sources.is_empty() {
                    self.ledger_to_sources.remove(&ledger);
                }
            }
        }
        true
    }

    pub(crate) fn sources_for(&self, ledger: &LedgerId) -> Vec<LedgerId> {
        self.ledger_to_sources
            .get(ledger)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn sources(&self) -> Vec<LedgerId> {
        self.source_to_ledgers.keys().cloned().collect()
    }

    pub(crate) fn ledgers(&self) -> Vec<LedgerId> {
        self.ledger_to_sources.keys().cloned().collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.source_to_ledgers.len()
    }
}

/// Read persisted graph-source dependencies as canonical ids.
pub(crate) fn parse_dependencies(deps: &[String]) -> Result<Vec<LedgerId>, LedgerIdParseError> {
    deps.iter().map(|d| LedgerId::parse(d)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> LedgerId {
        LedgerId::parse(s).unwrap()
    }

    /// A dependency persisted as `docs` wakes on `docs:main` commits.
    #[test]
    fn branchless_dependency_matches_canonical_commit() {
        let mut index = DependencyIndex::default();
        let deps = parse_dependencies(&["docs".to_string()]).unwrap();
        index.register(&id("search"), &deps);
        assert_eq!(index.sources_for(&id("docs:main")), vec![id("search:main")]);
    }

    #[test]
    fn re_register_drops_stale_edges() {
        let mut index = DependencyIndex::default();
        index.register(&id("search"), &[id("a"), id("b")]);
        index.register(&id("search"), &[id("b")]);
        assert!(index.sources_for(&id("a")).is_empty());
        assert_eq!(index.ledgers(), vec![id("b")]);
        assert!(index.unregister(&id("search:main")));
        assert!(!index.unregister(&id("search")));
        assert_eq!(index.len(), 0);
    }
}
