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
    /// identifiers (e.g. `Person`) into IRIs. Without this, only
    /// fully-qualified IRIs work — and Cypher doesn't have a syntax
    /// for those, so a default is essentially required in practice.
    ///
    /// Default in v1: `http://example.org/`. Real wiring will pull
    /// this from the ledger config / request envelope.
    pub vocab: String,
    /// Per-variable IRI overrides for labels/types/properties, set
    /// either via request envelope or test fixture. Bare identifier →
    /// IRI string.
    pub overrides: HashMap<String, String>,
}

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    pub fn new(encoder: &'a E, vars: &'a mut VarRegistry) -> Self {
        Self {
            encoder,
            vars,
            next_synth: 0,
            vocab: "http://example.org/".to_string(),
            overrides: HashMap::new(),
        }
    }

    pub fn with_vocab(mut self, vocab: impl Into<String>) -> Self {
        self.vocab = vocab.into();
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

    /// Resolve a Cypher variable identifier to a VarId.
    pub fn intern_var(&mut self, name: &str) -> VarId {
        self.vars.get_or_insert(name)
    }

    /// Resolve a bare Cypher identifier (label, type, property key) to
    /// an IRI. Order: per-request override → vocab + bare identifier.
    pub fn resolve_iri(&self, name: &str) -> String {
        if name == "*" {
            return name.to_string();
        }
        if let Some(iri) = self.overrides.get(name) {
            return iri.clone();
        }
        // No prefixing rules in Cypher; concatenate vocab + name.
        format!("{}{}", self.vocab, name)
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
}
