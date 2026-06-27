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
    /// ? : zero or one (the starting node plus its direct neighbors; no closure)
    ZeroOrOne,
}

/// One step of a composite-path repeated unit: a non-empty set of predicate
/// alternatives plus a traversal direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathStep {
    /// Predicate alternatives for this step (the step follows an edge of any).
    /// Never empty.
    pub predicates: Vec<Sid>,
    /// When `true` the step runs against the object→subject direction (`^p`).
    pub inverse: bool,
}

impl PathStep {
    /// A forward step over `predicates`.
    pub fn forward(predicates: Vec<Sid>) -> Self {
        Self {
            predicates,
            inverse: false,
        }
    }

    /// An inverse step over `predicates`.
    pub fn inverse(predicates: Vec<Sid>) -> Self {
        Self {
            predicates,
            inverse: true,
        }
    }
}

/// Resolved property path pattern for transitive traversal.
///
/// Produced by `@path` aliases with `+` or `*` modifiers, e.g.:
/// `{"@context": {"knowsPlus": {"@path": "ex:knows+"}}, "where": [{"@id": "ex:alice", "knowsPlus": "?who"}]}`
#[derive(Debug, Clone)]
pub struct PropertyPathPattern {
    /// Subject ref (Var or Sid — literals not allowed)
    pub subject: Ref,
    /// Predicate(s) for the first step of each hop, all resolved to Sids. A
    /// single entry is the ordinary `p*` / `p+`; multiple entries are an
    /// alternation-transitive path `(a|b|…)*` — the step follows an edge of ANY
    /// listed predicate (SPARQL `(a|b)*`, Cypher `[:A|B*]`). Empty **only** when
    /// `wildcard` is set (an untyped Cypher path).
    pub predicates: Vec<Sid>,
    /// Wildcard predicate (untyped Cypher variable-length path `-[*]->`): follow
    /// **any** node→node edge at each hop instead of a fixed predicate set. The
    /// traversal still only follows `Ref` objects (so data properties are
    /// excluded) and additionally skips the reserved predicates `rdf:type` and
    /// the `f:reifies*` reifier bundle, so it walks genuine relationships only.
    /// When set, `predicates` is empty (and `sequence_steps` is empty).
    pub wildcard: bool,
    /// Direction of the first step (`predicates`). Only ever `true` for a
    /// composite path whose leading step is inverse (`(^a/b)+`); a plain inverse
    /// path (`^p+`) is lowered by swapping subject/object, so it stays `false`.
    pub first_inverse: bool,
    /// Additional steps making each hop a composite sub-path `(p1/p2/…)+`. Empty
    /// for the simple/alternation/wildcard case. When non-empty, one hop follows
    /// `(predicates, first_inverse)` then each step here in order (so `(a/^b)+`
    /// is `predicates=[a]`, `first_inverse=false`, `sequence_steps=[^[b]]`).
    pub sequence_steps: Vec<PathStep>,
    /// Path modifier (+, *, or ?)
    pub modifier: PathModifier,
    /// Minimum hop count (Cypher `*min..`). `None` falls back to the modifier
    /// (`*` = 0, `+` = 1). Used by bounded untyped paths (`-[*1..3]->`).
    pub min_hops: Option<u32>,
    /// Maximum hop count (Cypher `*..max`). `None` = unbounded (subject to the
    /// operator's safety cap).
    pub max_hops: Option<u32>,
    /// Object ref (Var or Sid — literals not allowed)
    pub object: Ref,
}

impl PropertyPathPattern {
    /// Create a single-predicate property path pattern (`p*` / `p+`).
    pub fn new(subject: Ref, predicate: Sid, modifier: PathModifier, object: Ref) -> Self {
        Self {
            subject,
            predicates: vec![predicate],
            wildcard: false,
            first_inverse: false,
            sequence_steps: Vec::new(),
            modifier,
            min_hops: None,
            max_hops: None,
            object,
        }
    }

    /// Create an alternation-transitive path `(a|b|…)*` over `predicates`
    /// (the closure follows an edge of any listed predicate per hop). The
    /// caller must pass a non-empty list.
    pub fn new_alternatives(
        subject: Ref,
        predicates: Vec<Sid>,
        modifier: PathModifier,
        object: Ref,
    ) -> Self {
        debug_assert!(!predicates.is_empty(), "property path needs ≥1 predicate");
        Self {
            subject,
            predicates,
            wildcard: false,
            first_inverse: false,
            sequence_steps: Vec::new(),
            modifier,
            min_hops: None,
            max_hops: None,
            object,
        }
    }

    /// Create a wildcard (untyped) transitive path — follow any node→node edge
    /// per hop, optionally bounded to `[min_hops, max_hops]`. `modifier` carries
    /// the zero-vs-one lower bound when `min_hops` is `None` (`*` includes the
    /// start node, `+` does not).
    pub fn new_wildcard(
        subject: Ref,
        modifier: PathModifier,
        min_hops: Option<u32>,
        max_hops: Option<u32>,
        object: Ref,
    ) -> Self {
        Self {
            subject,
            predicates: Vec::new(),
            wildcard: true,
            first_inverse: false,
            sequence_steps: Vec::new(),
            modifier,
            min_hops,
            max_hops,
            object,
        }
    }

    /// Create a composite-transitive path `(p1/p2/…)+` from per-step
    /// [`PathStep`]s (`steps[0]` is the first step, etc.). Requires ≥2 steps,
    /// each with ≥1 predicate; for a single step use [`Self::new_alternatives`].
    pub fn new_composite(
        subject: Ref,
        mut steps: Vec<PathStep>,
        modifier: PathModifier,
        object: Ref,
    ) -> Self {
        debug_assert!(steps.len() >= 2, "composite path needs ≥2 steps");
        debug_assert!(
            steps.iter().all(|s| !s.predicates.is_empty()),
            "each composite step needs ≥1 predicate"
        );
        let first = steps.remove(0);
        Self {
            subject,
            predicates: first.predicates,
            wildcard: false,
            first_inverse: first.inverse,
            sequence_steps: steps,
            modifier,
            min_hops: None,
            max_hops: None,
            object,
        }
    }

    /// True if each hop traverses a composite sub-path (`(a/b)+`) rather than a
    /// single (possibly alternated) predicate.
    pub fn is_composite(&self) -> bool {
        !self.sequence_steps.is_empty()
    }

    /// The single traversed predicate, if this path has exactly one — used by
    /// count/scan fast paths that only handle the single-predicate shape.
    /// Returns `None` for an alternation, wildcard, or composite path so callers
    /// fall back to the general traversal operator.
    pub fn single_predicate(&self) -> Option<&Sid> {
        if self.wildcard {
            return None;
        }
        match self.predicates.as_slice() {
            [p] if self.sequence_steps.is_empty() => Some(p),
            _ => None,
        }
    }

    /// The effective minimum hop count: explicit `min_hops`, else the modifier
    /// default (`*` = 0, `+` = 1, `?` = 0). `ZeroOrOne` paths take a dedicated
    /// early-return path during traversal, so this default is only a safety net.
    pub fn effective_min_hops(&self) -> u32 {
        self.min_hops.unwrap_or(match self.modifier {
            PathModifier::ZeroOrMore | PathModifier::ZeroOrOne => 0,
            PathModifier::OneOrMore => 1,
        })
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
    /// Whether the emitted path value's per-hop `edges` are consumed (only
    /// Cypher's `relationships(p)` reads them). When `false` the operator skips
    /// building the per-hop edge tuples — a pure allocation/clone savings on the
    /// JSON-LD/FQL surface, which has no `relationships()` function. Edges are
    /// derivable from `nodes` + this pattern's single `predicate`/`direction`.
    pub needs_relationships: bool,
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
