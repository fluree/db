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

use crate::compile::{compile, ClassRuleKind, PropertyRuleKind};
use crate::execute::{
    apply_functional_property_rule, apply_has_key_rule, apply_inverse_functional_property_rule,
    apply_max_cardinality_rule, apply_max_qualified_cardinality_rule, apply_one_of_rule,
    apply_same_as_rule, apply_single_property_chain, fire_all_values_from_prop,
    fire_all_values_from_prop_inverse, fire_all_values_from_type, fire_domain,
    fire_equivalent_class, fire_has_value_backward, fire_has_value_forward,
    fire_has_value_forward_inverse, fire_intersection_backward, fire_intersection_member,
    fire_inverse, fire_range, fire_some_values_from, fire_some_values_from_filler,
    fire_some_values_from_inverse, fire_sub_property, fire_subclass, fire_symmetric,
    fire_transitive, fire_union_member, DeltaSet, DerivedSet, IdentityRuleContext,
    IdentityRuleState, RuleContext,
};
use crate::ontology_rl::{load_same_as_assertions, OntologyRL};
use crate::owl;
use crate::restrictions::{extract_restrictions, RestrictionIndex};
use crate::same_as::SameAsTracker;
use crate::{FrozenSameAs, ReasoningDiagnostics, ReasoningOptions, Result};

/// Run OWL2-RL reasoning to fixpoint
///
/// This function:
/// 1. Extracts ontology information (symmetric, transitive, inverse properties)
/// 2. Seeds initial facts from base assertions
/// 3. Iteratively applies rules until fixpoint or budget exhausted
/// 4. Returns derived facts and diagnostics
pub async fn run_fixpoint(
    db: GraphDbRef<'_>,
    opts: &ReasoningOptions,
) -> Result<(Vec<Flake>, FrozenSameAs, ReasoningDiagnostics)> {
    let budget = &opts.budget;
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
        // Generate explicit sameAs flakes from equivalence classes (eq-sym,
        // eq-trans), excluding pairs already asserted in the base data — the
        // database serves those itself.
        let base_pairs: hashbrown::HashSet<&(Sid, Sid)> = same_as_pairs.iter().collect();
        let same_as_flakes: Vec<Flake> = generate_same_as_flakes(&frozen_same_as, db.t)
            .into_iter()
            .filter(|f| match &f.o {
                fluree_db_core::value::FlakeValue::Ref(o) => {
                    !base_pairs.contains(&(f.s.clone(), o.clone()))
                }
                _ => true,
            })
            .collect();
        let flake_count = same_as_flakes.len();
        return Ok((
            same_as_flakes,
            frozen_same_as,
            ReasoningDiagnostics::completed(0, flake_count, start.elapsed()),
        ));
    }

    // Compile the active ontology into trigger-indexed ground rules (A1.2).
    // `enabled_rules` filtering happens here; the per-iteration loop below
    // dispatches each delta fact straight to the rules it can fire.
    let compiled = compile(&ontology, &restrictions, opts);

    // Seed initial delta with all base facts for relevant predicates
    let mut delta = seed_initial_delta(db, &ontology, &restrictions).await?;

    // Accumulated derived facts
    let mut derived = DerivedSet::new();

    // Seed facts flow through the same delta/derived sets as derived facts
    // (rules join against them), but they already exist in the database —
    // register their content keys so they are excluded from the emitted
    // overlay and from derived-fact counts. A seed fact recanonicalized to a
    // different subject/object no longer matches its base key and is emitted
    // (eq-rep-s/o entailments are genuine derivations).
    for flake in delta.iter() {
        derived.register_base(flake);
    }

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

    // Cross-iteration grouping state for prp-fp/prp-ifp (semi-naive: each
    // iteration folds only its delta in; rebuilt when sameAs changes).
    let mut identity_state = IdentityRuleState::default();

    // Semi-naive fixpoint loop
    while !delta.is_empty() {
        // Check budget
        if start.elapsed() > budget.max_duration {
            diagnostics = ReasoningDiagnostics::capped(
                "time",
                iterations,
                derived.derived_len(),
                start.elapsed(),
            );
            break;
        }

        if derived.derived_len() > budget.max_facts {
            diagnostics = ReasoningDiagnostics::capped(
                "facts",
                iterations,
                derived.derived_len(),
                start.elapsed(),
            );
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
                state: &mut identity_state,
            };

            // B.1. Functional property rule (prp-fp):
            // FunctionalProperty(P), P(x, y1), P(x, y2) → sameAs(y1, y2)
            if opts.rule_enabled("prp-fp") {
                apply_functional_property_rule(&ontology, &mut identity_ctx);
            }

            // B.2. Inverse functional property rule (prp-ifp):
            // InverseFunctionalProperty(P), P(x1, y), P(x2, y) → sameAs(x1, x2)
            if opts.rule_enabled("prp-ifp") {
                apply_inverse_functional_property_rule(&ontology, &mut identity_ctx);
            }

            // B.3. hasKey rule (prp-key):
            // hasKey(C, [P1..Pn]), type(x,C), P1(x,z1).., type(y,C), P1(y,z1).. → sameAs(x,y)
            if opts.rule_enabled("prp-key") {
                apply_has_key_rule(&ontology, &mut identity_ctx);
            }

            // B.4. MaxCardinality=1 rule (cls-maxc2):
            // P(x, y1), P(x, y2), type(x, C) → sameAs(y1, y2) where C is maxCardinality=1 restriction
            if opts.rule_enabled("cls-maxc2") {
                apply_max_cardinality_rule(&restrictions, &mut identity_ctx);
            }

            // B.5. MaxQualifiedCardinality=1 rule (cls-maxqc3/4):
            // P(x, y1), P(x, y2), type(x, C), type(y1, D), type(y2, D) → sameAs(y1, y2)
            if opts.rule_enabled_any(&["cls-maxqc", "cls-maxqc3", "cls-maxqc4"]) {
                apply_max_qualified_cardinality_rule(&restrictions, &mut identity_ctx);
            }
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

        // Phase C rules canonicalize every fact through a shared reference
        // (read-only walks, no compression). Compress once here so those
        // lookups are single-hop for the rest of the iteration.
        if same_as_changed {
            same_as_tracker.compress_all();
        }

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

        // Single pass over the delta: dispatch each fact to exactly the
        // compiled rules its predicate/class can fire (rulesFor index).
        for flake in delta.iter() {
            for rule in compiled.property_rules_for(&flake.p) {
                fire_property_rule(rule, &ontology, &restrictions, flake, &mut ctx);
            }
            if flake.p == rdf_type_sid {
                if let fluree_db_core::value::FlakeValue::Ref(class) = &flake.o {
                    for rule in compiled.class_rules_for(class) {
                        fire_class_rule(rule, &ontology, &restrictions, flake, class, &mut ctx);
                    }
                }
            }
        }

        // Property chains (prp-spo2) are n-way joins seeded from every chain
        // position; they run whole, gated on any component predicate being
        // present in the delta.
        let chains = ontology.property_chains();
        for chain_idx in compiled.triggered_chains(delta.predicates()) {
            apply_single_property_chain(&chains[chain_idx], &mut ctx);
        }

        // cls-oo: enumerated individuals are typed unconditionally (no delta
        // trigger); the derived-set check keeps it idempotent per iteration.
        if compiled.one_of_active() {
            apply_one_of_rule(&restrictions, &mut ctx);
        }

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
        diagnostics =
            ReasoningDiagnostics::completed(iterations, derived.derived_len(), start.elapsed());
        diagnostics.rules_fired = std::mem::take(&mut diagnostics.rules_fired);
    }

    Ok((derived.into_derived_flakes(), frozen_same_as, diagnostics))
}

/// Dispatch one delta fact to a compiled property-triggered rule.
fn fire_property_rule(
    rule: &PropertyRuleKind,
    ontology: &OntologyRL,
    restrictions: &RestrictionIndex,
    flake: &Flake,
    ctx: &mut RuleContext<'_>,
) {
    match rule {
        PropertyRuleKind::Symmetric => fire_symmetric(flake, ctx),
        PropertyRuleKind::Transitive => fire_transitive(flake, ctx),
        PropertyRuleKind::Inverse => fire_inverse(ontology, flake, ctx),
        PropertyRuleKind::Domain => fire_domain(ontology, flake, ctx),
        PropertyRuleKind::Range => fire_range(ontology, flake, ctx),
        PropertyRuleKind::SubProperty => fire_sub_property(ontology, flake, ctx),
        PropertyRuleKind::HasValueForward { restriction } => {
            fire_has_value_forward(restrictions, restriction, flake, ctx);
        }
        PropertyRuleKind::HasValueForwardInverse { restriction } => {
            fire_has_value_forward_inverse(restrictions, restriction, flake, ctx);
        }
        PropertyRuleKind::SomeValuesFrom { restriction } => {
            fire_some_values_from(restrictions, restriction, flake, ctx);
        }
        PropertyRuleKind::SomeValuesFromInverse { restriction } => {
            fire_some_values_from_inverse(restrictions, restriction, flake, ctx);
        }
        PropertyRuleKind::AllValuesFromProp { restriction } => {
            fire_all_values_from_prop(restrictions, restriction, flake, ctx);
        }
        PropertyRuleKind::AllValuesFromPropInverse { restriction } => {
            fire_all_values_from_prop_inverse(restrictions, restriction, flake, ctx);
        }
    }
}

/// Dispatch one delta `rdf:type` fact to a compiled class-triggered rule.
fn fire_class_rule(
    rule: &ClassRuleKind,
    ontology: &OntologyRL,
    restrictions: &RestrictionIndex,
    flake: &Flake,
    class: &Sid,
    ctx: &mut RuleContext<'_>,
) {
    match rule {
        ClassRuleKind::SubClass => fire_subclass(ontology, flake, class, ctx),
        ClassRuleKind::EquivalentClass => fire_equivalent_class(ontology, flake, class, ctx),
        ClassRuleKind::HasValueBackward => {
            fire_has_value_backward(restrictions, flake, class, ctx);
        }
        ClassRuleKind::AllValuesFromType => {
            fire_all_values_from_type(restrictions, flake, class, ctx);
        }
        ClassRuleKind::SomeValuesFromFiller {
            restriction,
            inverse,
        } => {
            fire_some_values_from_filler(restrictions, restriction, *inverse, flake, ctx);
        }
        ClassRuleKind::IntersectionMember { intersection } => {
            fire_intersection_member(restrictions, intersection, flake, ctx);
        }
        ClassRuleKind::IntersectionBackward => {
            fire_intersection_backward(restrictions, flake, class, ctx);
        }
        ClassRuleKind::UnionMember { union } => {
            fire_union_member(restrictions, union, flake, ctx);
        }
    }
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
    use crate::cache::ReasoningBudget;
    use std::time::Duration;

    #[test]
    fn test_budget_defaults() {
        let budget = ReasoningBudget::default();
        assert_eq!(budget.max_duration, Duration::from_secs(30));
        assert_eq!(budget.max_facts, 1_000_000);
    }
}
