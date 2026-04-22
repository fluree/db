//! Semi-naive fixpoint iteration for OWL2-RL reasoning
//!
//! This module implements the main reasoning loop that iteratively applies
//! OWL2-RL rules until a fixpoint is reached or budget is exhausted.

use std::time::Instant;

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::range::{RangeMatch, RangeTest};
use fluree_db_core::{GraphDbRef, Sid};
use fluree_vocab::jsonld_names::ID as JSONLD_ID;
use fluree_vocab::namespaces::{JSON_LD, RDF};
use fluree_vocab::predicates::RDF_TYPE;

use crate::cache::ReasoningBudget;
use crate::execute::{
    apply_all_values_from_rule, apply_domain_rule, apply_equivalent_class_rule,
    apply_functional_property_rule, apply_has_key_rule, apply_has_value_backward_rule,
    apply_has_value_forward_rule, apply_intersection_backward_rule,
    apply_intersection_forward_rule, apply_inverse_functional_property_rule, apply_inverse_rule,
    apply_max_cardinality_rule, apply_max_qualified_cardinality_rule, apply_one_of_rule,
    apply_property_chain_rule, apply_range_rule, apply_same_as_rule, apply_some_values_from_rule,
    apply_sub_property_rule, apply_subclass_rule, apply_symmetric_rule, apply_transitive_rule,
    apply_union_rule, DeltaSet, DerivedSet, IdentityRuleContext, RuleContext,
};
use crate::ontology_rl::{load_same_as_assertions, OntologyRL};
use crate::owl;
use crate::restrictions::{extract_restrictions, RestrictionIndex};
use crate::same_as::SameAsTracker;
use crate::{FrozenSameAs, ReasoningDiagnostics, Result};

/// Run OWL2-RL reasoning to fixpoint
///
/// This function:
/// 1. Extracts ontology information (symmetric, transitive, inverse properties)
/// 2. Seeds initial facts from base assertions
/// 3. Iteratively applies rules until fixpoint or budget exhausted
/// 4. Returns derived facts and diagnostics
pub async fn run_fixpoint(
    db: GraphDbRef<'_>,
    budget: &ReasoningBudget,
) -> Result<(Vec<Flake>, FrozenSameAs, ReasoningDiagnostics)> {
    let start = Instant::now();
    let mut diagnostics = ReasoningDiagnostics::default();

    // Extract OWL2-RL ontology (symmetric, transitive, inverse properties)
    let ontology = OntologyRL::from_db_with_overlay(db).await?;

    // Extract OWL restrictions (hasValue, someValuesFrom, etc.)
    let restrictions = extract_restrictions(db).await?;

    // Load sameAs assertions BEFORE checking if we have work to do
    // sameAs can exist even without symmetric/transitive/inverse declarations
    let mut same_as_tracker = SameAsTracker::new();
    let same_as_pairs = load_same_as_assertions(db).await?;
    let has_same_as = !same_as_pairs.is_empty();
    for (x, y) in &same_as_pairs {
        same_as_tracker.union(x, y);
    }

    // If no property rules, no restrictions, AND no sameAs assertions, no reasoning needed
    if ontology.is_empty() && restrictions.is_empty() && !has_same_as {
        return Ok((
            Vec::new(),
            FrozenSameAs::empty(),
            ReasoningDiagnostics::completed(0, 0, start.elapsed()),
        ));
    }

    // If only sameAs (no property rules and no restrictions), generate sameAs flakes and return
    // This ensures owl:sameAs is queryable even without property rules
    if ontology.is_empty() && restrictions.is_empty() {
        let frozen_same_as = same_as_tracker.finalize();
        // Generate explicit sameAs flakes from equivalence classes (eq-sym, eq-trans)
        let same_as_flakes = generate_same_as_flakes(&frozen_same_as, db.t);
        let flake_count = same_as_flakes.len();
        return Ok((
            same_as_flakes,
            frozen_same_as,
            ReasoningDiagnostics::completed(0, flake_count, start.elapsed()),
        ));
    }

    // Seed initial delta with all base facts for relevant predicates
    let mut delta = seed_initial_delta(db, &ontology, &restrictions).await?;

    // Accumulated derived facts
    let mut derived = DerivedSet::new();

    // owl:sameAs SID for rule application
    let owl_same_as_sid = owl::same_as_sid();

    // rdf:type SID for hasKey rule
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Reasoning t value - derived facts must be visible at the query's `to_t`.
    //
    // IMPORTANT:
    // - We compute entailments "as-of" `db.t`, so derived facts must have `t <= db.t`
    //   to be visible through the normal range filters used by `execute_with_overlay_at`.
    // - For non-time-bounded queries, callers pass `to_t = i64::MAX`, which still
    //   keeps derived facts visible.
    let reasoning_t = db.t;

    let mut iterations = 0;

    // Semi-naive fixpoint loop
    while !delta.is_empty() {
        // Check budget
        if start.elapsed() > budget.max_duration {
            diagnostics =
                ReasoningDiagnostics::capped("time", iterations, derived.len(), start.elapsed());
            break;
        }

        if derived.len() > budget.max_facts {
            diagnostics =
                ReasoningDiagnostics::capped("facts", iterations, derived.len(), start.elapsed());
            break;
        }

        iterations += 1;
        let mut new_delta = DeltaSet::new();

        // Apply rules in order:
        // PHASE A: Process existing sameAs in delta
        // This updates the union-find for any owl:sameAs facts in the current delta
        let mut same_as_changed = apply_same_as_rule(
            &delta,
            &mut same_as_tracker,
            &owl_same_as_sid,
            &mut diagnostics,
        );

        // PHASE B: Apply identity-producing rules (produce owl:sameAs)
        // These rules can derive new sameAs facts: prp-fp, prp-ifp
        // They must run BEFORE non-identity rules to ensure proper canonicalization

        // Create IdentityRuleContext for identity-producing rules
        {
            let mut identity_ctx = IdentityRuleContext {
                delta: &delta,
                derived: &derived,
                new_delta: &mut new_delta,
                same_as: &same_as_tracker,
                owl_same_as_sid: &owl_same_as_sid,
                rdf_type_sid: &rdf_type_sid,
                t: reasoning_t,
                same_as_changed,
                diagnostics: &mut diagnostics,
            };

            // B.1. Functional property rule (prp-fp):
            // FunctionalProperty(P), P(x, y1), P(x, y2) → sameAs(y1, y2)
            apply_functional_property_rule(&ontology, &mut identity_ctx);

            // B.2. Inverse functional property rule (prp-ifp):
            // InverseFunctionalProperty(P), P(x1, y), P(x2, y) → sameAs(x1, x2)
            apply_inverse_functional_property_rule(&ontology, &mut identity_ctx);

            // B.3. hasKey rule (prp-key):
            // hasKey(C, [P1..Pn]), type(x,C), P1(x,z1).., type(y,C), P1(y,z1).. → sameAs(x,y)
            apply_has_key_rule(&ontology, &mut identity_ctx);

            // B.4. MaxCardinality=1 rule (cls-maxc2):
            // P(x, y1), P(x, y2), type(x, C) → sameAs(y1, y2) where C is maxCardinality=1 restriction
            apply_max_cardinality_rule(&restrictions, &mut identity_ctx);

            // B.5. MaxQualifiedCardinality=1 rule (cls-maxqc3/4):
            // P(x, y1), P(x, y2), type(x, C), type(y1, D), type(y2, D) → sameAs(y1, y2)
            apply_max_qualified_cardinality_rule(&restrictions, &mut identity_ctx);
        }

        // B.6. Process any new sameAs facts generated by fp/ifp/hasKey/cardinality rules
        // This ensures the union-find is updated before non-identity rules run
        let identity_same_as_changed = apply_same_as_rule(
            &new_delta,
            &mut same_as_tracker,
            &owl_same_as_sid,
            &mut diagnostics,
        );
        same_as_changed = same_as_changed || identity_same_as_changed;

        // PHASE C: Apply non-identity rules
        // These rules produce property/type facts (not sameAs)

        // Create RuleContext for standard rules
        let mut ctx = RuleContext {
            delta: &delta,
            derived: &derived,
            new_delta: &mut new_delta,
            same_as: &same_as_tracker,
            rdf_type_sid: &rdf_type_sid,
            t: reasoning_t,
            diagnostics: &mut diagnostics,
        };

        // C.1. Symmetric property rule (prp-symp)
        apply_symmetric_rule(&ontology, &mut ctx);

        // C.2. Transitive property rule (prp-trp)
        apply_transitive_rule(&ontology, &mut ctx);

        // C.3. Inverse property rule (prp-inv)
        apply_inverse_rule(&ontology, &mut ctx);

        // C.4. Domain rule (prp-dom): P(x,y), domain(P,C) → type(x,C)
        apply_domain_rule(&ontology, &mut ctx);

        // C.5. Range rule (prp-rng): P(x,y), range(P,C) → type(y,C)
        apply_range_rule(&ontology, &mut ctx);

        // C.6. SubPropertyOf rule (prp-spo1): P1(x,y), subPropertyOf(P1,P2) → P2(x,y)
        apply_sub_property_rule(&ontology, &mut ctx);

        // C.7. PropertyChain rule (prp-spo2): P1(u0,u1), P2(u1,u2) → P(u0,u2)
        apply_property_chain_rule(&ontology, &mut ctx);

        // C.8. SubClassOf rule (cax-sco): type(x, C1), subClassOf(C1, C2) → type(x, C2)
        apply_subclass_rule(&ontology, &mut ctx);

        // C.9. EquivalentClass rule (cax-eqc): type(x, C1), equivalentClass(C1, C2) → type(x, C2)
        apply_equivalent_class_rule(&ontology, &mut ctx);

        // C.10. HasValue backward rule (cls-hv1): type(x, C) where C is hasValue restriction → P(x, v)
        apply_has_value_backward_rule(&restrictions, &mut ctx);

        // C.11. HasValue forward rule (cls-hv2): P(x, v) where C is hasValue restriction → type(x, C)
        apply_has_value_forward_rule(&restrictions, &mut ctx);

        // C.12. SomeValuesFrom rule (cls-svf1): P(x, y), type(y, D) → type(x, C)
        apply_some_values_from_rule(&restrictions, &mut ctx);

        // C.13. AllValuesFrom rule (cls-avf): type(x, C), P(x, y) → type(y, D)
        apply_all_values_from_rule(&restrictions, &mut ctx);

        // C.14. IntersectionOf backward rule (cls-int2): type(x, I) → type(x, Ci)
        apply_intersection_backward_rule(&restrictions, &mut ctx);

        // C.15. IntersectionOf forward rule (cls-int1): type(x, C1) ∧ ... → type(x, I)
        apply_intersection_forward_rule(&restrictions, &mut ctx);

        // C.16. UnionOf rule (cls-uni): type(x, Ci) → type(x, U)
        apply_union_rule(&restrictions, &mut ctx);

        // C.17. OneOf rule (cls-oo): i ∈ oneOf list → type(i, C)
        apply_one_of_rule(&restrictions, &mut ctx);

        // PHASE D: If sameAs changed this iteration, canonicalize both delta and new_delta (eq-rep-s/o)
        // This ensures DerivedSet stays consistently canonical for proper deduplication
        let (delta_to_merge, new_delta) = if same_as_changed {
            // Canonicalize delta before merging into derived
            let canonical_delta = delta.recanonicalize(&same_as_tracker);
            // Also canonicalize new_delta
            let canonical_new = if !new_delta.is_empty() {
                new_delta.recanonicalize(&same_as_tracker)
            } else {
                new_delta
            };
            (canonical_delta, canonical_new)
        } else {
            (delta, new_delta)
        };

        // Merge current delta (canonicalized if sameAs changed) into derived
        for flake in delta_to_merge.iter() {
            derived.try_add(flake.clone());
        }

        // Filter new_delta to only include truly new facts
        let mut filtered_delta = DeltaSet::new();
        for flake in new_delta.iter() {
            if derived.try_add(flake.clone()) {
                filtered_delta.push(flake.clone());
            }
        }

        delta = filtered_delta;
    }

    // Finalize sameAs
    let frozen_same_as = same_as_tracker.finalize();

    // Generate explicit sameAs flakes from equivalence classes (eq-sym, eq-trans)
    // This makes owl:sameAs queryable just like any other predicate.
    // For each equivalence class, emit: canonical sameAs x, x sameAs canonical for all x
    let same_as_flakes = generate_same_as_flakes(&frozen_same_as, reasoning_t);
    for flake in same_as_flakes {
        derived.try_add(flake);
    }

    // Update diagnostics if not already capped
    if !diagnostics.capped {
        diagnostics = ReasoningDiagnostics::completed(iterations, derived.len(), start.elapsed());
        diagnostics.rules_fired = std::mem::take(&mut diagnostics.rules_fired);
    }

    Ok((derived.into_flakes(), frozen_same_as, diagnostics))
}

/// Seed the initial delta with base facts for relevant predicates
///
/// This loads all facts that could trigger rules:
/// - Facts with symmetric predicates
/// - Facts with transitive predicates
/// - Facts with predicates that have inverses
/// - Facts with predicates that have domain declarations
/// - Facts with predicates that have range declarations
/// - Facts with predicates that have super-properties (subPropertyOf)
/// - Facts with predicates that are chain components (propertyChainAxiom)
/// - Facts with functional properties (for prp-fp)
/// - Facts with inverse-functional properties (for prp-ifp)
/// - Facts with restricted properties (hasValue, someValuesFrom, etc.)
/// - owl:sameAs facts
async fn seed_initial_delta(
    db: GraphDbRef<'_>,
    ontology: &OntologyRL,
    restrictions: &RestrictionIndex,
) -> Result<DeltaSet> {
    let mut delta = DeltaSet::new();

    // Collect all predicates we need to query
    let mut predicates_to_query: Vec<&Sid> = Vec::new();

    for p in ontology.symmetric_properties() {
        predicates_to_query.push(p);
    }

    for p in ontology.transitive_properties() {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Add predicates with domain declarations
    for p in ontology.properties_with_domain() {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Add predicates with range declarations
    for p in ontology.properties_with_range() {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Add predicates with super-properties (for prp-spo1)
    for p in ontology.properties_with_super_properties() {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Collect chain component predicates (for prp-spo2)
    // Stored separately because chain_component_predicates returns owned Sids
    let chain_components = ontology.chain_component_predicates();
    for p in &chain_components {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Add functional properties (for prp-fp)
    for p in ontology.functional_properties() {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Add inverse-functional properties (for prp-ifp)
    for p in ontology.inverse_functional_properties() {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Add predicates with inverse declarations (for prp-inv)
    for p in ontology.properties_with_inverses() {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Collect key properties from hasKey declarations (for prp-key)
    // Stored separately because all_key_properties returns owned Sids
    let key_properties = ontology.all_key_properties();
    for p in &key_properties {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // Collect restricted properties (for cls-hv, cls-svf, cls-avf, etc.)
    // Stored separately because restricted_properties returns owned Sids
    let restricted_props: Vec<Sid> = restrictions.restricted_properties().cloned().collect();
    for p in &restricted_props {
        if !predicates_to_query.contains(&p) {
            predicates_to_query.push(p);
        }
    }

    // We need rdf:type facts for:
    // - hasKey declarations (to find class instances for key matching)
    // - class hierarchy rules (to derive superclass types)
    // - restriction rules (hasValue, someValuesFrom, etc.)
    let needs_type_facts =
        !key_properties.is_empty() || ontology.has_class_rules() || !restrictions.is_empty();

    // Query facts for each relevant predicate using PSOT index
    for p in predicates_to_query {
        let flakes: Vec<Flake> = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(p.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| &f.p == p && f.op)
            .collect();

        for flake in flakes {
            delta.push(flake);
        }
    }

    // Load rdf:type facts for hasKey and class hierarchy rules
    if needs_type_facts {
        let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
        let type_flakes: Vec<Flake> = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(rdf_type_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.p == rdf_type_sid && f.op)
            .collect();

        for flake in type_flakes {
            delta.push(flake);
        }
    }

    // Also seed owl:sameAs facts
    let owl_same_as_sid = owl::same_as_sid();
    let same_as_flakes: Vec<Flake> = db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch {
                p: Some(owl_same_as_sid.clone()),
                ..Default::default()
            },
        )
        .await?
        .into_iter()
        .filter(|f| f.p == owl_same_as_sid && f.op)
        .collect();

    for flake in same_as_flakes {
        delta.push(flake);
    }

    Ok(delta)
}

/// Generate explicit owl:sameAs flakes from the equivalence classes.
///
/// For each equivalence class with members [a, b, c, ...], generates:
/// - a sameAs b, a sameAs c, ... (from canonical to all others)
/// - b sameAs a, c sameAs a, ... (from all others back to canonical)
///
/// This is O(2n) flakes per class instead of O(n²) for full pairwise.
/// Transitivity allows queries to derive any pair: if a=b and a=c, then b=c.
///
/// Note: This also generates reflexive flakes (x sameAs x) for each element,
/// which matches OWL2-RL semantics.
fn generate_same_as_flakes(frozen: &FrozenSameAs, t: i64) -> Vec<Flake> {
    use fluree_db_core::value::FlakeValue;

    let owl_same_as_sid = owl::same_as_sid();
    let ref_dt = Sid::new(JSON_LD, JSONLD_ID);

    let mut flakes = Vec::new();

    // For each equivalence class
    for (_root, members) in frozen.members_iter() {
        if members.len() <= 1 {
            continue; // Skip singleton classes
        }

        let canonical = &members[0]; // Members are sorted, first is canonical

        // Generate flakes: canonical sameAs x, x sameAs canonical for each x
        for member in members {
            if member != canonical {
                // canonical sameAs member
                flakes.push(Flake::new(
                    canonical.clone(),
                    owl_same_as_sid.clone(),
                    FlakeValue::Ref(member.clone()),
                    ref_dt.clone(),
                    t,
                    true,
                    None,
                ));

                // member sameAs canonical (symmetry)
                flakes.push(Flake::new(
                    member.clone(),
                    owl_same_as_sid.clone(),
                    FlakeValue::Ref(canonical.clone()),
                    ref_dt.clone(),
                    t,
                    true,
                    None,
                ));
            }
        }
    }

    flakes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_budget_defaults() {
        let budget = ReasoningBudget::default();
        assert_eq!(budget.max_duration, Duration::from_secs(30));
        assert_eq!(budget.max_facts, 1_000_000);
    }
}
