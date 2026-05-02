//! OWL2-QL query rewriting
//!
//! Implements bounded query rewriting for the OWL2-QL profile, focused on:
//! - `owl:inverseOf` pattern expansion
//! - `rdfs:domain`/`rdfs:range` type-query rewriting
//!
//! # OWL2-QL Rewriting Rules
//!
//! ## owl:inverseOf
//!
//! When a property `P` has an inverse `P_inv` (via `owl:inverseOf`), patterns
//! using `P` are expanded to include the inverse:
//!
//! ```text
//! ?s P ?o  →  UNION(?s P ?o, ?o P_inv ?s)
//! ```
//!
//! ## rdfs:domain Type-Query Rewriting
//!
//! When querying `?s rdf:type D`, if a property `P` has `domain(P) ⊑ D`
//! (domain is D or a subclass of D), the existence of `?s P ?_o` implies
//! `?s rdf:type D`:
//!
//! ```text
//! ?s rdf:type D  →  UNION(?s rdf:type D, ?s P ?_o)
//! ```
//!
//! (where `?_o` is a fresh internal variable)
//!
//! ## rdfs:range Type-Query Rewriting
//!
//! Similarly for range: if `range(P) ⊑ R`, being the object of `P` implies the type:
//!
//! ```text
//! ?s rdf:type R  →  UNION(?s rdf:type R, ?_x P ?s)
//! ```

use crate::ir::Pattern;
use crate::rewrite::{rewrite_subpatterns, Diagnostics, PlanContext, RewriteResult};
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::var_registry::VarId;
use fluree_db_core::{
    is_owl_equivalent_property, is_rdf_type, FlakeValue, GraphDbRef, IndexType, RangeMatch,
    RangeTest, SchemaHierarchy, Sid,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

/// Counter for generating unique internal variable IDs
static INTERNAL_VAR_COUNTER: AtomicU16 = AtomicU16::new(0);

/// Generate a fresh internal variable ID
///
/// These are collision-free variable IDs used for domain/range expansion.
/// Format: VarId with high bit set to distinguish from user variables.
fn mint_internal_var() -> VarId {
    // Use a high offset to avoid collision with user variables
    // User VarIds typically start from 0 and grow sequentially
    let counter = INTERNAL_VAR_COUNTER.fetch_add(1, Ordering::Relaxed);
    VarId(0x8000 | counter)
}

/// Ontology snapshot for OWL2-QL reasoning
///
/// Contains precomputed maps for efficient query rewriting:
/// - `inverse_of`: Property → list of inverse properties
/// - `equivalent_props`: Property → list of other equivalent properties
/// - `domain`: Property → domain class
/// - `range`: Property → range class
///
/// Built from database assertions of `owl:inverseOf`, `owl:equivalentProperty`, `rdfs:domain`, `rdfs:range`.
#[derive(Debug, Clone)]
pub struct Ontology {
    inner: Arc<OntologyInner>,
}

#[derive(Debug)]
struct OntologyInner {
    /// P → [P_inv1, P_inv2, ...] (one property may have multiple inverses)
    inverse_of: HashMap<Sid, Vec<Sid>>,
    /// P → [P_eq1, P_eq2, ...] (does NOT include P)
    equivalent_props: HashMap<Sid, Vec<Sid>>,
    /// P → domain class (property can only have one domain in OWL2-QL)
    domain: HashMap<Sid, Sid>,
    /// P → range class (property can only have one range in OWL2-QL)
    range: HashMap<Sid, Sid>,
    /// Schema epoch for cache validation
    epoch: u64,
}

impl Ontology {
    /// Create an empty ontology
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(OntologyInner {
                inverse_of: HashMap::new(),
                equivalent_props: HashMap::new(),
                domain: HashMap::new(),
                range: HashMap::new(),
                epoch: 0,
            }),
        }
    }

    /// Create an ontology from explicit maps
    ///
    /// Used for testing and when building from database queries.
    pub fn new(
        inverse_of: HashMap<Sid, Vec<Sid>>,
        equivalent_props: HashMap<Sid, Vec<Sid>>,
        domain: HashMap<Sid, Sid>,
        range: HashMap<Sid, Sid>,
        epoch: u64,
    ) -> Self {
        Self {
            inner: Arc::new(OntologyInner {
                inverse_of,
                equivalent_props,
                domain,
                range,
                epoch,
            }),
        }
    }

    /// Get inverse properties for a property
    ///
    /// Returns empty slice if property has no inverses.
    pub fn inverses_of(&self, p: &Sid) -> &[Sid] {
        self.inner
            .inverse_of
            .get(p)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Get equivalent properties for a property (does not include the property itself).
    pub fn equivalents_of(&self, p: &Sid) -> &[Sid] {
        self.inner
            .equivalent_props
            .get(p)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Get domain class for a property
    pub fn domain_of(&self, p: &Sid) -> Option<&Sid> {
        self.inner.domain.get(p)
    }

    /// Get range class for a property
    pub fn range_of(&self, p: &Sid) -> Option<&Sid> {
        self.inner.range.get(p)
    }

    /// Schema epoch for cache validation
    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    /// Check if ontology has any OWL assertions
    pub fn is_empty(&self) -> bool {
        self.inner.inverse_of.is_empty()
            && self.inner.equivalent_props.is_empty()
            && self.inner.domain.is_empty()
            && self.inner.range.is_empty()
    }

    /// Get all properties with a given domain class (or subclass of it)
    ///
    /// Returns properties where `domain(P) ⊑ target_class`, meaning domain
    /// is exactly `target_class` or a subclass of it.
    pub fn properties_with_domain_subsumed_by(
        &self,
        target_class: &Sid,
        hierarchy: Option<&SchemaHierarchy>,
    ) -> Vec<&Sid> {
        self.inner
            .domain
            .iter()
            .filter(|(_, domain_class)| {
                // domain_class ⊑ target_class means:
                // domain_class == target_class, or domain_class is a subclass of target_class
                if *domain_class == target_class {
                    return true;
                }
                // Check if domain_class is a descendant of target_class
                if let Some(h) = hierarchy {
                    h.subclasses_of(target_class).contains(domain_class)
                } else {
                    false
                }
            })
            .map(|(prop, _)| prop)
            .collect()
    }

    /// Get all properties with a given range class (or subclass of it)
    pub fn properties_with_range_subsumed_by(
        &self,
        target_class: &Sid,
        hierarchy: Option<&SchemaHierarchy>,
    ) -> Vec<&Sid> {
        self.inner
            .range
            .iter()
            .filter(|(_, range_class)| {
                if *range_class == target_class {
                    return true;
                }
                if let Some(h) = hierarchy {
                    h.subclasses_of(target_class).contains(range_class)
                } else {
                    false
                }
            })
            .map(|(prop, _)| prop)
            .collect()
    }

    /// Build an Ontology from database assertions
    ///
    /// Queries the database for:
    /// - `owl:inverseOf` assertions (property → inverse property)
    /// - `rdfs:domain` assertions (property → domain class)
    /// - `rdfs:range` assertions (property → range class)
    ///
    /// Uses the PSOT index with predicate-bound queries for efficient extraction.
    ///
    /// # Arguments
    ///
    /// * `db` - Bundled database reference (snapshot, graph, overlay, as-of time)
    ///
    pub async fn from_db(db: GraphDbRef<'_>) -> crate::error::Result<Self> {
        use fluree_vocab::namespaces::{OWL, RDFS};

        let epoch = db.snapshot.t as u64;

        let mut inverse_of: HashMap<Sid, Vec<Sid>> = HashMap::new();
        let mut eq_edges: Vec<(Sid, Sid)> = Vec::new();
        let mut domain_map: HashMap<Sid, Sid> = HashMap::new();
        let mut range_map: HashMap<Sid, Sid> = HashMap::new();

        // Query for owl:inverseOf assertions
        // Pattern: ?property owl:inverseOf ?inverse
        let inverse_of_pred = Sid::new(OWL, "inverseOf");
        let inverse_flakes = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::predicate(inverse_of_pred),
            )
            .await?;

        for flake in inverse_flakes {
            if let FlakeValue::Ref(inverse_prop) = flake.o {
                // flake.s is the property, inverse_prop is its inverse
                inverse_of
                    .entry(flake.s.clone())
                    .or_default()
                    .push(inverse_prop.clone());
                // owl:inverseOf is symmetric, so also record the reverse
                inverse_of.entry(inverse_prop).or_default().push(flake.s);
            }
        }

        // Query for owl:equivalentProperty assertions
        // Pattern: ?property owl:equivalentProperty ?equivalent
        let equivalent_prop_pred = Sid::new(OWL, "equivalentProperty");
        let equivalent_flakes = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::predicate(equivalent_prop_pred),
            )
            .await?;

        for flake in equivalent_flakes {
            if let FlakeValue::Ref(equiv_prop) = flake.o {
                eq_edges.push((flake.s, equiv_prop));
            }
        }

        // Query for rdfs:domain assertions
        // Pattern: ?property rdfs:domain ?class
        let domain_pred = Sid::new(RDFS, "domain");
        let domain_flakes = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::predicate(domain_pred),
            )
            .await?;

        for flake in domain_flakes {
            if let FlakeValue::Ref(domain_class) = flake.o {
                // In OWL2-QL, a property can only have one domain
                // If multiple are asserted, we keep the last one
                domain_map.insert(flake.s, domain_class);
            }
        }

        // Query for rdfs:range assertions
        // Pattern: ?property rdfs:range ?class
        let range_pred = Sid::new(RDFS, "range");
        let range_flakes = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::predicate(range_pred),
            )
            .await?;

        for flake in range_flakes {
            if let FlakeValue::Ref(range_class) = flake.o {
                // In OWL2-QL, a property can only have one range
                range_map.insert(flake.s, range_class);
            }
        }

        let equivalent_props = compute_equivalent_closure(eq_edges);
        Ok(Self::new(
            inverse_of,
            equivalent_props,
            domain_map,
            range_map,
            epoch,
        ))
    }

    /// Build an Ontology from database assertions + overlay flakes.
    ///
    /// In memory-backed tests, schema/ontology assertions often exist only in novelty (overlay)
    /// until background indexing runs. This helper merges `owl:equivalentProperty` assertions
    /// from the overlay into the persisted ontology snapshot.
    pub async fn from_db_with_overlay(db: GraphDbRef<'_>) -> crate::error::Result<Self> {
        let base = Self::from_db(db).await?;

        let epoch = db.snapshot.t as u64;

        let mut inverse_of = base.inner.inverse_of.clone();
        let mut eq_edges: Vec<(Sid, Sid)> = Vec::new();
        let domain = base.inner.domain.clone();
        let range = base.inner.range.clone();

        db.overlay.for_each_overlay_flake(
            0,
            IndexType::Psot,
            None,
            None,
            true,
            db.t,
            &mut |flake| {
                if !flake.op {
                    return;
                }
                if !is_owl_equivalent_property(&flake.p) {
                    return;
                }
                if let FlakeValue::Ref(eq_prop) = &flake.o {
                    eq_edges.push((flake.s.clone(), eq_prop.clone()));
                }
            },
        );

        // Normalize: sort/dedup for determinism.
        for invs in inverse_of.values_mut() {
            invs.sort();
            invs.dedup();
        }
        let mut equivalent_props = base.inner.equivalent_props.clone();
        if !eq_edges.is_empty() {
            // Merge by re-closing across base + overlay edges.
            for (a, b) in eq_edges {
                equivalent_props
                    .entry(a.clone())
                    .or_default()
                    .push(b.clone());
                equivalent_props.entry(b).or_default().push(a);
            }
            let edges: Vec<(Sid, Sid)> = equivalent_props
                .iter()
                .flat_map(|(a, bs)| bs.iter().map(move |b| (a.clone(), b.clone())))
                .collect();
            equivalent_props = compute_equivalent_closure(edges);
        }

        Ok(Self::new(
            inverse_of,
            equivalent_props,
            domain,
            range,
            epoch,
        ))
    }
}

fn compute_equivalent_closure(edges: Vec<(Sid, Sid)>) -> HashMap<Sid, Vec<Sid>> {
    use std::collections::{HashMap, HashSet, VecDeque};

    // undirected adjacency
    let mut adj: HashMap<Sid, Vec<Sid>> = HashMap::new();
    for (a, b) in edges {
        adj.entry(a.clone()).or_default().push(b.clone());
        adj.entry(b).or_default().push(a);
    }

    let mut seen: HashSet<Sid> = HashSet::new();
    let mut out: HashMap<Sid, Vec<Sid>> = HashMap::new();

    for start in adj.keys().cloned().collect::<Vec<_>>() {
        if seen.contains(&start) {
            continue;
        }
        let mut comp: Vec<Sid> = Vec::new();
        let mut q: VecDeque<Sid> = VecDeque::new();
        seen.insert(start.clone());
        q.push_back(start.clone());
        while let Some(cur) = q.pop_front() {
            comp.push(cur.clone());
            if let Some(ns) = adj.get(&cur) {
                for n in ns {
                    if seen.insert(n.clone()) {
                        q.push_back(n.clone());
                    }
                }
            }
        }

        for p in &comp {
            let mut others: Vec<Sid> = comp.iter().filter(|x| *x != p).cloned().collect();
            others.sort();
            others.dedup();
            if !others.is_empty() {
                out.insert(p.clone(), others);
            }
        }
    }

    out
}

/// Extended planning context for OWL-QL rewriting
///
/// Extends the base PlanContext with ontology information.
#[derive(Debug, Clone)]
pub struct OwlQlContext {
    /// Base planning context (entailment mode, hierarchy, limits)
    pub base: PlanContext,
    /// Ontology for OWL-specific rewriting
    pub ontology: Option<Ontology>,
}

impl OwlQlContext {
    /// Create context with ontology
    pub fn new(base: PlanContext, ontology: Option<Ontology>) -> Self {
        Self { base, ontology }
    }
}

/// Rewrite patterns for OWL2-QL entailment
///
/// Applies these transformations:
/// 1. owl:inverseOf expansion: `?s P ?o` → `UNION(?s P ?o, ?o P_inv ?s)`
/// 2. rdfs:domain type-query: `?s rdf:type D` → `UNION(?s rdf:type D, ?s P ?_o)`
/// 3. rdfs:range type-query: `?s rdf:type R` → `UNION(?s rdf:type R, ?_x P ?s)`
///
/// The base RDFS expansion (subclass/subproperty) should be applied separately
/// before or after OWL-QL expansion.
pub fn rewrite_owl_ql_patterns(
    patterns: &[Pattern],
    ctx: &OwlQlContext,
) -> (Vec<Pattern>, Diagnostics) {
    let epoch = ctx.ontology.as_ref().map(Ontology::epoch);
    let mut diag = Diagnostics::with_epoch(epoch);

    // No-op if no ontology available
    let ontology = match &ctx.ontology {
        Some(o) if !o.is_empty() => o,
        _ => {
            return (patterns.to_vec(), diag);
        }
    };

    let mut total_expansions = 0;
    let result =
        rewrite_owl_ql_patterns_internal(patterns, ontology, ctx, &mut diag, &mut total_expansions);

    (result, diag)
}

/// Internal rewriting function with shared budget
fn rewrite_owl_ql_patterns_internal(
    patterns: &[Pattern],
    ontology: &Ontology,
    ctx: &OwlQlContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> Vec<Pattern> {
    let mut result = Vec::with_capacity(patterns.len());

    for pattern in patterns {
        let rewritten =
            rewrite_owl_ql_single_pattern(pattern, ontology, ctx, diag, total_expansions);
        match rewritten {
            RewriteResult::Unchanged => {
                result.push(pattern.clone());
            }
            RewriteResult::Expanded(expanded) => {
                diag.patterns_expanded += 1;
                result.extend(expanded);
            }
            RewriteResult::Capped {
                patterns: expanded,
                original_count,
            } => {
                diag.patterns_expanded += 1;
                diag.was_capped = true;
                diag.warn(format!(
                    "OWL-QL expansion capped: {} patterns reduced to {} due to limits",
                    original_count,
                    expanded.len()
                ));
                result.extend(expanded);
            }
        }
    }

    result
}

/// Rewrite a single pattern for OWL-QL
fn rewrite_owl_ql_single_pattern(
    pattern: &Pattern,
    ontology: &Ontology,
    ctx: &OwlQlContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    match pattern {
        Pattern::Triple(tp) => rewrite_owl_ql_triple(tp, ontology, ctx, diag, total_expansions),

        // Recursively process nested patterns. Subquery is treated as a
        // leaf below; OWL2-QL expansion doesn't cross subquery scope
        // boundaries.
        Pattern::Optional(_)
        | Pattern::Union(_)
        | Pattern::Minus(_)
        | Pattern::Exists(_)
        | Pattern::NotExists(_)
        | Pattern::Graph { .. }
        | Pattern::Service(_) => rewrite_subpatterns(pattern.clone(), diag, |xs, diag| {
            rewrite_owl_ql_patterns_internal(&xs, ontology, ctx, diag, total_expansions)
        }),

        // Non-expandable patterns
        Pattern::Filter(_)
        | Pattern::Bind { .. }
        | Pattern::Values { .. }
        | Pattern::PropertyPath(_)
        | Pattern::Subquery(_)
        | Pattern::IndexSearch(_)
        | Pattern::VectorSearch(_)
        | Pattern::R2rml(_)
        | Pattern::GeoSearch(_)
        | Pattern::S2Search(_) => RewriteResult::Unchanged,
    }
}

/// Rewrite a triple pattern for OWL-QL
///
/// NOTE: Currently only handles Term::Sid predicates and objects because the
/// Ontology lookup requires SIDs. Term::Iri predicates (from cross-ledger
/// lowering) won't trigger expansion. This is a known limitation - OWL-QL expansion
/// requires either:
/// - Single-ledger mode with Term::Sid predicates, or
/// - A future enhancement to support IRI-based ontology lookups
fn rewrite_owl_ql_triple(
    tp: &TriplePattern,
    ontology: &Ontology,
    ctx: &OwlQlContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    // Check for type-query rewriting: ?s rdf:type Class
    if let (Ref::Sid(predicate), Term::Sid(class)) = (&tp.p, &tp.o) {
        if is_rdf_type(predicate) {
            return expand_type_query_owl_ql(tp, class, ontology, ctx, diag, total_expansions);
        }
    }

    // Expand owl:equivalentProperty for non-rdf:type predicate patterns.
    if let Ref::Sid(predicate) = &tp.p {
        if !is_rdf_type(predicate) {
            let rewritten =
                expand_equivalent_property(tp, predicate, ontology, ctx, diag, total_expansions);
            if !matches!(rewritten, RewriteResult::Unchanged) {
                return rewritten;
            }
        }
    }

    // Check for inverseOf expansion: ?s P ?o where P has inverses
    if let Ref::Sid(predicate) = &tp.p {
        if !is_rdf_type(predicate) {
            return expand_inverse_of(tp, predicate, ontology, ctx, diag, total_expansions);
        }
    }

    RewriteResult::Unchanged
}

/// Expand owl:equivalentProperty patterns
///
/// `?s P ?o` → `UNION(?s P ?o, ?s P_eq1 ?o, ?s P_eq2 ?o, ...)`
fn expand_equivalent_property(
    tp: &TriplePattern,
    predicate: &Sid,
    ontology: &Ontology,
    ctx: &OwlQlContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    let eqs = ontology.equivalents_of(predicate);
    if eqs.is_empty() {
        return RewriteResult::Unchanged;
    }

    // Build patterns: original + one for each equivalent predicate.
    let mut patterns: Vec<TriplePattern> = Vec::with_capacity(1 + eqs.len());
    patterns.push(tp.clone());
    for eq in eqs {
        patterns.push(TriplePattern::new(
            tp.s.clone(),
            Ref::Sid(eq.clone()),
            tp.o.clone(),
        ));
    }

    // Apply limits (same logic as inverseOf).
    let total_count = patterns.len();
    let available_budget = ctx
        .base
        .limits
        .max_total_expansions
        .saturating_sub(*total_expansions);
    let effective_limit = ctx
        .base
        .limits
        .max_expansions_per_pattern
        .min(available_budget);

    if effective_limit == 0 {
        return RewriteResult::Capped {
            patterns: vec![Pattern::Triple(tp.clone())],
            original_count: total_count,
        };
    }

    if total_count > effective_limit {
        patterns.truncate(effective_limit);
        *total_expansions += patterns.len();

        if patterns.len() == 1 {
            return RewriteResult::Capped {
                patterns: vec![Pattern::Triple(tp.clone())],
                original_count: total_count,
            };
        }

        let branches: Vec<Vec<Pattern>> = patterns
            .into_iter()
            .map(|p| vec![Pattern::Triple(p)])
            .collect();
        diag.patterns_expanded += 1;
        return RewriteResult::Capped {
            patterns: vec![Pattern::Union(branches)],
            original_count: total_count,
        };
    }

    *total_expansions += patterns.len();
    if patterns.len() == 1 {
        return RewriteResult::Unchanged;
    }

    let branches: Vec<Vec<Pattern>> = patterns
        .into_iter()
        .map(|p| vec![Pattern::Triple(p)])
        .collect();
    diag.patterns_expanded += 1;
    RewriteResult::Expanded(vec![Pattern::Union(branches)])
}

/// Expand owl:inverseOf patterns
///
/// `?s P ?o` → `UNION(?s P ?o, ?o P_inv ?s)` for each inverse P_inv
fn expand_inverse_of(
    tp: &TriplePattern,
    predicate: &Sid,
    ontology: &Ontology,
    ctx: &OwlQlContext,
    _diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    let inverses = ontology.inverses_of(predicate);
    if inverses.is_empty() {
        return RewriteResult::Unchanged;
    }

    // Build patterns: original + one for each inverse with s/o swapped
    let mut patterns: Vec<TriplePattern> = Vec::with_capacity(1 + inverses.len());
    patterns.push(tp.clone()); // Original pattern

    for inverse in inverses {
        // Swap subject and object for inverse property
        // tp.o (Term) moves to subject position (Ref) — must not be a Value
        // tp.s (Ref) moves to object position (Term)
        let new_subject =
            Ref::try_from(tp.o.clone()).expect("inverse property object must be Var, Sid, or Iri");
        patterns.push(TriplePattern::new(
            new_subject,
            Ref::Sid(inverse.clone()),
            tp.s.clone().into(), // Ref -> Term
        ));
    }

    // Apply limits
    let total_count = patterns.len();
    let available_budget = ctx
        .base
        .limits
        .max_total_expansions
        .saturating_sub(*total_expansions);
    let effective_limit = ctx
        .base
        .limits
        .max_expansions_per_pattern
        .min(available_budget);

    if effective_limit == 0 {
        return RewriteResult::Capped {
            patterns: vec![Pattern::Triple(tp.clone())],
            original_count: total_count,
        };
    }

    if total_count > effective_limit {
        patterns.truncate(effective_limit);
        *total_expansions += patterns.len();

        if patterns.len() == 1 {
            return RewriteResult::Capped {
                patterns: vec![Pattern::Triple(tp.clone())],
                original_count: total_count,
            };
        }

        let branches: Vec<Vec<Pattern>> = patterns
            .into_iter()
            .map(|p| vec![Pattern::Triple(p)])
            .collect();

        return RewriteResult::Capped {
            patterns: vec![Pattern::Union(branches)],
            original_count: total_count,
        };
    }

    *total_expansions += total_count;

    let branches: Vec<Vec<Pattern>> = patterns
        .into_iter()
        .map(|p| vec![Pattern::Triple(p)])
        .collect();

    RewriteResult::Expanded(vec![Pattern::Union(branches)])
}

/// Expand type-query using domain/range inference
///
/// Given `?s rdf:type Class`:
/// - For each property P where domain(P) ⊑ Class: add `?s P ?_fresh`
/// - For each property P where range(P) ⊑ Class: add `?_fresh P ?s`
fn expand_type_query_owl_ql(
    tp: &TriplePattern,
    class: &Sid,
    ontology: &Ontology,
    ctx: &OwlQlContext,
    _diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    let hierarchy = ctx.base.hierarchy.as_ref();

    // Find properties whose domain implies this type
    let domain_props = ontology.properties_with_domain_subsumed_by(class, hierarchy);
    // Find properties whose range implies this type
    let range_props = ontology.properties_with_range_subsumed_by(class, hierarchy);

    if domain_props.is_empty() && range_props.is_empty() {
        return RewriteResult::Unchanged;
    }

    // Build patterns: original + domain patterns + range patterns
    let mut patterns: Vec<TriplePattern> =
        Vec::with_capacity(1 + domain_props.len() + range_props.len());
    patterns.push(tp.clone()); // Original type pattern

    // Domain expansion: ?s P ?_fresh (subject has type if it's the subject of P)
    for prop in domain_props {
        let fresh_obj = mint_internal_var();
        patterns.push(TriplePattern::new(
            tp.s.clone(),
            Ref::Sid(prop.clone()),
            Term::Var(fresh_obj),
        ));
    }

    // Range expansion: ?_fresh P ?s (subject has type if it's the object of P)
    for prop in range_props {
        let fresh_subj = mint_internal_var();
        patterns.push(TriplePattern::new(
            Ref::Var(fresh_subj),
            Ref::Sid(prop.clone()),
            tp.s.clone().into(), // Ref -> Term for object position
        ));
    }

    // Apply limits
    let total_count = patterns.len();
    let available_budget = ctx
        .base
        .limits
        .max_total_expansions
        .saturating_sub(*total_expansions);
    let effective_limit = ctx
        .base
        .limits
        .max_expansions_per_pattern
        .min(available_budget);

    if effective_limit == 0 {
        return RewriteResult::Capped {
            patterns: vec![Pattern::Triple(tp.clone())],
            original_count: total_count,
        };
    }

    if total_count > effective_limit {
        patterns.truncate(effective_limit);
        *total_expansions += patterns.len();

        if patterns.len() == 1 {
            return RewriteResult::Capped {
                patterns: vec![Pattern::Triple(tp.clone())],
                original_count: total_count,
            };
        }

        let branches: Vec<Vec<Pattern>> = patterns
            .into_iter()
            .map(|p| vec![Pattern::Triple(p)])
            .collect();

        return RewriteResult::Capped {
            patterns: vec![Pattern::Union(branches)],
            original_count: total_count,
        };
    }

    *total_expansions += total_count;

    let branches: Vec<Vec<Pattern>> = patterns
        .into_iter()
        .map(|p| vec![Pattern::Triple(p)])
        .collect();

    RewriteResult::Expanded(vec![Pattern::Union(branches)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rewrite::{EntailmentMode, PlanLimits};
    use crate::ir::triple::Ref;
    use fluree_db_core::SidInterner;
    use fluree_vocab::namespaces::RDF;

    fn make_rdf_type() -> Sid {
        Sid::new(RDF, "type")
    }

    fn make_test_ontology() -> Ontology {
        let interner = SidInterner::new();

        // hasFriend inverseOf hasFriendOf
        let has_friend = interner.intern(100, "hasFriend");
        let has_friend_of = interner.intern(100, "hasFriendOf");

        // worksAt has domain Person
        let works_at = interner.intern(100, "worksAt");
        let person = interner.intern(100, "Person");

        // locatedIn has range Location
        let located_in = interner.intern(100, "locatedIn");
        let location = interner.intern(100, "Location");

        let mut inverse_of = HashMap::new();
        inverse_of.insert(has_friend.clone(), vec![has_friend_of.clone()]);
        inverse_of.insert(has_friend_of, vec![has_friend]);

        let mut domain = HashMap::new();
        domain.insert(works_at, person);

        let mut range = HashMap::new();
        range.insert(located_in, location);

        Ontology::new(inverse_of, HashMap::new(), domain, range, 1)
    }

    fn make_ctx(ontology: Option<Ontology>) -> OwlQlContext {
        OwlQlContext::new(
            PlanContext {
                entailment_mode: EntailmentMode::OwlQl,
                hierarchy: None,
                limits: PlanLimits::default(),
            },
            ontology,
        )
    }

    #[test]
    fn test_empty_ontology_no_expansion() {
        let interner = SidInterner::new();
        let has_friend = interner.intern(100, "hasFriend");

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(has_friend),
            Term::Var(VarId(1)),
        ));

        let ctx = make_ctx(Some(Ontology::empty()));
        let (result, diag) = rewrite_owl_ql_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert_eq!(diag.patterns_expanded, 0);
    }

    #[test]
    fn test_inverse_of_expansion() {
        let interner = SidInterner::new();
        let has_friend = interner.intern(100, "hasFriend");

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(has_friend),
            Term::Var(VarId(1)),
        ));

        let ctx = make_ctx(Some(make_test_ontology()));
        let (result, diag) = rewrite_owl_ql_patterns(&[pattern], &ctx);

        // Should expand to UNION(original, inverse)
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Union(branches) => {
                assert_eq!(branches.len(), 2);
                // First branch: original pattern (?s hasFriend ?o)
                // Second branch: inverse pattern (?o hasFriendOf ?s)
            }
            _ => panic!("Expected Union pattern"),
        }
        assert_eq!(diag.patterns_expanded, 1);
    }

    #[test]
    fn test_no_inverse_expansion_for_unknown_property() {
        let interner = SidInterner::new();
        let unknown_prop = interner.intern(100, "unknownProp");

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(unknown_prop),
            Term::Var(VarId(1)),
        ));

        let ctx = make_ctx(Some(make_test_ontology()));
        let (result, diag) = rewrite_owl_ql_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert_eq!(diag.patterns_expanded, 0);
    }

    #[test]
    fn test_domain_type_query_expansion() {
        let interner = SidInterner::new();
        let person = interner.intern(100, "Person");

        // Query: ?s rdf:type Person
        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(person),
        ));

        let ctx = make_ctx(Some(make_test_ontology()));
        let (result, diag) = rewrite_owl_ql_patterns(&[pattern], &ctx);

        // Should expand to UNION(?s rdf:type Person, ?s worksAt ?_fresh)
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Union(branches) => {
                assert_eq!(branches.len(), 2);
            }
            _ => panic!("Expected Union pattern"),
        }
        assert_eq!(diag.patterns_expanded, 1);
    }

    #[test]
    fn test_range_type_query_expansion() {
        let interner = SidInterner::new();
        let location = interner.intern(100, "Location");

        // Query: ?s rdf:type Location
        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(location),
        ));

        let ctx = make_ctx(Some(make_test_ontology()));
        let (result, diag) = rewrite_owl_ql_patterns(&[pattern], &ctx);

        // Should expand to UNION(?s rdf:type Location, ?_fresh locatedIn ?s)
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Union(branches) => {
                assert_eq!(branches.len(), 2);
            }
            _ => panic!("Expected Union pattern"),
        }
        assert_eq!(diag.patterns_expanded, 1);
    }

    #[test]
    fn test_no_type_expansion_for_unknown_class() {
        let interner = SidInterner::new();
        let unknown_class = interner.intern(100, "UnknownClass");

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(unknown_class),
        ));

        let ctx = make_ctx(Some(make_test_ontology()));
        let (result, diag) = rewrite_owl_ql_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert_eq!(diag.patterns_expanded, 0);
    }

    #[test]
    fn test_no_expansion_without_ontology() {
        let interner = SidInterner::new();
        let has_friend = interner.intern(100, "hasFriend");

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(has_friend),
            Term::Var(VarId(1)),
        ));

        let ctx = make_ctx(None);
        let (result, diag) = rewrite_owl_ql_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert_eq!(diag.patterns_expanded, 0);
    }

    #[test]
    fn test_internal_var_generation() {
        let var1 = mint_internal_var();
        let var2 = mint_internal_var();

        // Internal vars should be unique
        assert_ne!(var1, var2);
        // Internal vars should have high bit set (0x8000 for u16)
        assert!(var1.0 & 0x8000 != 0);
        assert!(var2.0 & 0x8000 != 0);
    }

    #[test]
    fn test_nested_pattern_expansion() {
        let interner = SidInterner::new();
        let has_friend = interner.intern(100, "hasFriend");

        let inner_pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(has_friend),
            Term::Var(VarId(1)),
        ));

        let pattern = Pattern::Optional(vec![inner_pattern]);

        let ctx = make_ctx(Some(make_test_ontology()));
        let (result, diag) = rewrite_owl_ql_patterns(&[pattern], &ctx);

        // Optional should contain expanded UNION
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Optional(inner) => {
                assert_eq!(inner.len(), 1);
                assert!(matches!(inner[0], Pattern::Union(_)));
            }
            _ => panic!("Expected Optional pattern"),
        }
        // The inner pattern expansion counts as 1, the outer Optional wrapper counts as another
        // since patterns_expanded tracks how many patterns were rewritten
        assert_eq!(diag.patterns_expanded, 2);
    }
}
