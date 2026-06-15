//! Property-path patterns: extending what users can write against the
//! standard graph (transitive predicate traversal), independent of where
//! the data lives.

use super::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_core::Sid;

/// Property path modifier (transitive operators)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathModifier {
    /// + : one or more (at least one hop)
    OneOrMore,
    /// * : zero or more (includes starting node)
    ZeroOrMore,
}

/// Resolved property path pattern for transitive traversal.
///
/// Produced by `@path` aliases with `+` or `*` modifiers, e.g.:
/// `{"@context": {"knowsPlus": {"@path": "ex:knows+"}}, "where": [{"@id": "ex:alice", "knowsPlus": "?who"}]}`
#[derive(Debug, Clone)]
pub struct PropertyPathPattern {
    /// Subject ref (Var or Sid — literals not allowed)
    pub subject: Ref,
    /// Predicate to traverse (always resolved to Sid)
    pub predicate: Sid,
    /// Path modifier (+ or *)
    pub modifier: PathModifier,
    /// Object ref (Var or Sid — literals not allowed)
    pub object: Ref,
}

impl PropertyPathPattern {
    /// Create a new property path pattern
    pub fn new(subject: Ref, predicate: Sid, modifier: PathModifier, object: Ref) -> Self {
        Self {
            subject,
            predicate,
            modifier,
            object,
        }
    }

    fn positional_vars(&self) -> Vec<VarId> {
        let mut vars = Vec::with_capacity(2);
        if let Ref::Var(v) = &self.subject {
            vars.push(*v);
        }
        if let Ref::Var(v) = &self.object {
            vars.push(*v);
        }
        vars
    }

    /// Variables mentioned in this pattern (subject and object slots).
    pub fn referenced_vars(&self) -> Vec<VarId> {
        self.positional_vars()
    }

    /// Variables this pattern adds to the binding set.
    pub fn produced_vars(&self) -> Vec<VarId> {
        self.positional_vars()
    }
}

/// Edge-direction for a shortest-path search (Cypher arrow direction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathDirection {
    /// `(a)-[:T*]->(b)` — follow edges subject→object only.
    Outgoing,
    /// `(a)<-[:T*]-(b)` — follow edges object→subject only.
    Incoming,
    /// `(a)-[:T*]-(b)` — undirected; follow edges in either direction.
    Either,
}

/// Search mode for [`ShortestPathPattern`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortestPathMode {
    /// `shortestPath(...)` — a single shortest path (or none).
    Single,
    /// `allShortestPaths(...)` — every path of the minimal length.
    All,
}

/// Anchored shortest-path pattern — Cypher `shortestPath` / `allShortestPaths`.
///
/// V1 contract: **both endpoints must be bound** before this operator runs
/// (anchored search). Unweighted (hop-count) bidirectional BFS over a single
/// typed predicate. Binds `path_var` to a [`crate::binding::Binding::Path`]
/// node sequence per discovered path; under `Single` mode at most one row per
/// input, under `All` mode one row per minimal-length path.
#[derive(Debug, Clone)]
pub struct ShortestPathPattern {
    /// Start node ref (Var or Sid — literals not allowed).
    pub start: Ref,
    /// End node ref (Var or Sid — literals not allowed).
    pub end: Ref,
    /// Predicate to traverse (resolved to Sid).
    pub predicate: Sid,
    /// Traversal direction.
    pub direction: PathDirection,
    /// Single vs. all-shortest-paths.
    pub mode: ShortestPathMode,
    /// Variable bound to the resulting path value.
    pub path_var: VarId,
    /// Minimum hop count (`*min..`), `None` = 1.
    pub min_hops: Option<u32>,
    /// Maximum hop count (`*..max`), `None` = unbounded (subject to safety caps).
    pub max_hops: Option<u32>,
}

impl ShortestPathPattern {
    /// Variables that must be bound before this pattern runs (both endpoints).
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = Vec::with_capacity(2);
        if let Ref::Var(v) = &self.start {
            vars.push(*v);
        }
        if let Ref::Var(v) = &self.end {
            vars.push(*v);
        }
        vars
    }

    /// Variables this pattern adds to the binding set (the path value).
    pub fn produced_vars(&self) -> Vec<VarId> {
        vec![self.path_var]
    }
}
