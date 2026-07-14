//! Lowering context — encoder + variable registry + synthetic var
//! generation + context-driven IRI mapping.

use std::collections::HashMap;

use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::{VarId, VarRegistry};
use fluree_vocab::rdf;

use super::{LowerError, Result};

/// Lowering context. Holds the encoder, variable registry, and
/// counters for synthetic non-distinguished variables.
pub struct LoweringContext<'a, E: IriEncoder> {
    pub encoder: &'a E,
    pub vars: &'a mut VarRegistry,
    /// Counter for `?#__cy_<n>` synthetic vars.
    next_synth: u32,
    /// Optional default-vocabulary prefix used to resolve bare
    /// identifiers (e.g. `Person`) into IRIs — the RDF-compat mode,
    /// sourced from the ledger context's `@vocab`.
    ///
    /// `None` (the default) means Cypher identifiers are plain names
    /// with no IRI at all: they live under namespace code 0 (the empty
    /// prefix), so `Person` is just `Person`. This is the native LPG
    /// mode — a Cypher user never sees or enters a namespace unless
    /// they opt into RDF interop by configuring `@vocab`.
    pub vocab: Option<String>,
    /// Per-variable IRI overrides for labels/types/properties, set
    /// either via request envelope or test fixture. Bare identifier →
    /// IRI string.
    pub overrides: HashMap<String, String>,
    /// Lexical scopes for loop-local variables introduced by list
    /// comprehensions / `reduce` / list predicates. A name in an active scope
    /// resolves to its scoped synthetic `VarId` (innermost wins) instead of a
    /// query variable, and property access on it lowers to eval-time member
    /// access rather than an outer-pattern join.
    scopes: Vec<HashMap<String, VarId>>,
    /// Variables used on the relationship *annotation* surface somewhere in
    /// the statement (`e.prop`, `properties(e)`, …). A bound relationship
    /// variable outside this set can bind a synthesized relationship value
    /// from the plain base triple instead of requiring a reifier bundle.
    annotation_dependent: std::collections::HashSet<String>,
    /// Allow a bare `MATCH (n)` (no label, property, or relationship) to
    /// lower to a whole-graph distinct-subject scan. Off by default — a full
    /// scan is rarely what a production query intends; benchmarks and ad-hoc
    /// exploration opt in (server flag `FLUREE_CYPHER_ALLOW_FULL_SCAN`).
    pub allow_full_scan: bool,
    /// Whether any reified edge can exist in the queried view. When the
    /// caller proves it cannot (index stats + overlay both show no
    /// `f:reifies*` facts), value-only bound relationship variables skip
    /// the per-hop OPTIONAL annotation probe entirely. Defaults to `true`
    /// (conservative): reified parallel edges must never be silently
    /// dropped.
    pub reified_edges_possible: bool,
    /// Whether chains of anonymous untyped hops (`-->()-->()-->(n)`) may
    /// lower to a single exact-depth wildcard path (frontier BFS with
    /// per-level dedup) instead of per-hop triple joins. Join chains produce
    /// one row per *walk*; the path operator one row per *endpoint* — the
    /// two agree only when the statement's output is `DISTINCT`, aggregates
    /// nothing, and never references the interior nodes. The statement
    /// lowering sets this after checking those conditions
    /// ([`super::stmt`]); default off (write-path MATCH lowering and CALL
    /// bodies never enable it).
    pub(super) fuse_reachability_chains: bool,
}

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    pub fn new(encoder: &'a E, vars: &'a mut VarRegistry) -> Self {
        Self {
            encoder,
            vars,
            next_synth: 0,
            vocab: None,
            overrides: HashMap::new(),
            scopes: Vec::new(),
            annotation_dependent: std::collections::HashSet::new(),
            allow_full_scan: false,
            reified_edges_possible: true,
            fuse_reachability_chains: false,
        }
    }

    /// Opt in to whole-graph scans for bare `MATCH (n)` patterns.
    pub fn with_allow_full_scan(mut self, allow: bool) -> Self {
        self.allow_full_scan = allow;
        self
    }

    /// Tell the lowering whether the queried view can contain reified
    /// edges (see the field docs; `false` is a caller-proved guarantee).
    pub fn with_reified_edges_possible(mut self, possible: bool) -> Self {
        self.reified_edges_possible = possible;
        self
    }

    /// Record the statement-wide annotation-surface variable set (see
    /// [`super::annotation_use`]).
    pub(super) fn set_annotation_dependent(&mut self, vars: std::collections::HashSet<String>) {
        self.annotation_dependent = vars;
    }

    /// Whether `name` is used on the relationship annotation surface
    /// anywhere in the statement.
    pub(super) fn is_annotation_dependent(&self, name: &str) -> bool {
        self.annotation_dependent.contains(name)
    }

    /// Push a new loop-local scope. Pair with [`Self::exit_scope`].
    pub fn enter_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    /// Pop the innermost loop-local scope.
    pub fn exit_scope(&mut self) {
        self.scopes.pop();
    }

    /// Bind a loop-local variable name to a fresh synthetic `VarId` in the
    /// current (innermost) scope, returning its id. Fresh synthetics avoid
    /// collisions with an outer variable of the same name and make the
    /// `referenced_vars` subtraction meaningful.
    pub fn bind_local(&mut self, name: &str) -> VarId {
        let id = self.fresh_synth();
        if let Some(top) = self.scopes.last_mut() {
            top.insert(name.to_string(), id);
        }
        id
    }

    /// Whether `name` currently resolves to a loop-local (in any active scope).
    pub fn is_local(&self, name: &str) -> bool {
        self.scopes.iter().any(|s| s.contains_key(name))
    }

    pub fn with_vocab(mut self, vocab: impl Into<String>) -> Self {
        self.vocab = Some(vocab.into());
        self
    }

    pub fn with_vocab_opt(mut self, vocab: Option<String>) -> Self {
        self.vocab = vocab;
        self
    }

    pub fn with_overrides(mut self, overrides: HashMap<String, String>) -> Self {
        self.overrides = overrides;
        self
    }

    /// Allocate a fresh non-distinguished variable. The `?#__cy_<n>`
    /// prefix ensures these are hidden from `RETURN *` per the plan.
    pub fn fresh_synth(&mut self) -> VarId {
        let name = format!("?#__cy_{}", self.next_synth);
        self.next_synth += 1;
        self.vars.get_or_insert(&name)
    }

    /// Resolve a Cypher variable identifier to a VarId. A name bound by an
    /// active loop-local scope (innermost first) resolves to its scoped id;
    /// otherwise it interns as a query variable.
    pub fn intern_var(&mut self, name: &str) -> VarId {
        for scope in self.scopes.iter().rev() {
            if let Some(&id) = scope.get(name) {
                return id;
            }
        }
        self.vars.get_or_insert(name)
    }

    /// Resolve a bare Cypher identifier (label, type, property key).
    /// Order: per-request override → `@vocab` + bare identifier (RDF
    /// compat) → the bare name itself (namespace 0, no IRI).
    pub fn resolve_iri(&self, name: &str) -> String {
        if name == "*" {
            return name.to_string();
        }
        if let Some(iri) = self.overrides.get(name) {
            return iri.clone();
        }
        match &self.vocab {
            // RDF compat: concatenate vocab + name (no prefixing rules
            // in Cypher).
            Some(vocab) => format!("{vocab}{name}"),
            None => name.to_string(),
        }
    }

    /// Resolve and reject reserved-system predicates.
    pub fn resolve_predicate(&self, name: &str) -> Result<String> {
        let iri = self.resolve_iri(name);
        if fluree_vocab::reifies_iris::ALL.iter().any(|x| *x == iri) {
            return Err(LowerError::ReservedPredicate(iri));
        }
        Ok(iri)
    }

    /// rdf:type IRI.
    pub fn rdf_type_iri(&self) -> &'static str {
        rdf::TYPE
    }

    /// Lower a resolved IRI to a pattern `Ref`, encoding to `Ref::Sid` when
    /// the namespace is registered (parity with the SPARQL lowering — the
    /// planner's stats lookups and the batched join lanes are Sid-gated and
    /// silently degrade to per-row paths on `Ref::Iri`). Unregistered
    /// namespaces stay `Ref::Iri`, which matches nothing at scan time.
    pub fn iri_ref(&self, iri: String) -> fluree_db_query::ir::Ref {
        match self.encoder.encode_iri_strict(&iri) {
            Some(sid) => fluree_db_query::ir::Ref::Sid(sid),
            // No @vocab: a bare Cypher name (no scheme — identifiers
            // can't contain `:`) lives under namespace 0 (empty
            // prefix) — construct the Sid directly; `encode_iri_strict`
            // rejects namespace 0 by design. Scheme-ful strings (system
            // IRIs like rdf:type, backticked IRIs) keep the strict
            // behavior: unregistered namespaces stay `Ref::Iri`.
            None if self.vocab.is_none() && !iri.contains(':') => fluree_db_query::ir::Ref::Sid(
                fluree_db_core::Sid::new(fluree_vocab::namespaces::EMPTY, iri),
            ),
            None => fluree_db_query::ir::Ref::Iri(iri.into()),
        }
    }

    /// Object-position counterpart of [`Self::iri_ref`].
    pub fn iri_term(&self, iri: String) -> fluree_db_query::ir::Term {
        match self.encoder.encode_iri_strict(&iri) {
            Some(sid) => fluree_db_query::ir::Term::Sid(sid),
            // Bare namespace-0 name — see [`Self::iri_ref`].
            None if self.vocab.is_none() && !iri.contains(':') => fluree_db_query::ir::Term::Sid(
                fluree_db_core::Sid::new(fluree_vocab::namespaces::EMPTY, iri),
            ),
            None => fluree_db_query::ir::Term::Iri(iri.into()),
        }
    }
}
