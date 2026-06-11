//! Compiled OWL2-RL rule set (roadmap A1.2).
//!
//! Compiles the active ontology (`OntologyRL` + `RestrictionIndex`) into
//! ontology-specific ground rules indexed by the facts that can fire them —
//! the `rulesFor(predicate)` / `rulesFor(class)` index from the reasoning
//! roadmap. The fixpoint loop then makes a *single* pass over each
//! iteration's delta, dispatching every fact to exactly the rules it can
//! trigger, instead of running ~20 rule-family passes that each rescan the
//! delta (the `rdf:type` extent in particular was walked once per
//! class-triggered family).
//!
//! `enabled_rules` filtering happens here, at compile time: a disabled rule
//! never enters the index, so the fixpoint no longer consults
//! [`ReasoningOptions`] per iteration.
//!
//! The compiled artifact is also the consumption point for later roadmap
//! phases: A1.3 partitions delta facts across threads with this same
//! dispatch, A2's FBF maintenance re-derives through it, and B1 explanations
//! tag derivations with the compiled rule that fired.
//!
//! Identity-producing rules (`prp-fp`, `prp-ifp`, `prp-key`, `cls-maxc2`,
//! `cls-maxqc`) are *not* compiled into the per-fact index: they run as
//! grouped phase-B passes with incremental cross-iteration state
//! ([`IdentityRuleState`](crate::execute::IdentityRuleState)) and their own
//! delta gating.

use fluree_db_core::Sid;
use hashbrown::{HashMap, HashSet};

use crate::ontology_rl::OntologyRL;
use crate::restrictions::{ClassRef, RestrictionIndex, RestrictionType};
use crate::types::PropertyExpression;
use crate::ReasoningOptions;

/// A compiled rule fired by a delta fact's *predicate*.
///
/// The trigger predicate is the `by_predicate` key; variants carry only what
/// the body can't recover from the fact itself (restriction bindings).
/// Axiom detail (superproperties, domains, …) is read back from the
/// `OntologyRL` / `RestrictionIndex` at fire time — those structures are the
/// single source of truth and the lookups are single hash probes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyRuleKind {
    /// `prp-symp`: P(x,y) → P(y,x)
    Symmetric,
    /// `prp-trp`: P(x,y), P(y,z) → P(x,z)
    Transitive,
    /// `prp-inv`: P(x,y) → P⁻¹(y,x) for each declared inverse
    Inverse,
    /// `prp-dom`: P(x,y) → type(x, C) for each domain class
    Domain,
    /// `prp-rng`: P(x,y) → type(y, C) for each range class
    Range,
    /// `prp-spo1`: P(x,y) → P′(x,y) for each superproperty
    SubProperty,
    /// `cls-hv2` (direct): P(x,v) → type(x, R)
    HasValueForward { restriction: Sid },
    /// `cls-hv2` (inverse expression): P(v,x) → type(x, R)
    HasValueForwardInverse { restriction: Sid },
    /// `cls-svf1` (direct, property side): P(x,y), type(y,D) → type(x, R)
    SomeValuesFrom { restriction: Sid },
    /// `cls-svf1` (inverse expression, property side)
    SomeValuesFromInverse { restriction: Sid },
    /// `cls-avf` (direct, property side): P(x,y), type(x,R) → type(y, D)
    AllValuesFromProp { restriction: Sid },
    /// `cls-avf` (inverse expression, property side)
    AllValuesFromPropInverse { restriction: Sid },
}

/// A compiled rule fired by a delta `rdf:type` fact's *class* (object).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassRuleKind {
    /// `cax-sco`: type(x,C) → type(x,C′) for each superclass
    SubClass,
    /// `cax-eqc`: type(x,C) → type(x,C′) for each equivalent class
    EquivalentClass,
    /// `cls-hv1`: type(x,R) → P(x,v) — the trigger class IS the restriction
    HasValueBackward,
    /// `cls-avf` (type side): type(x,R), P(x,y) → type(y,D)
    AllValuesFromType,
    /// `cls-svf1` (type side): type(y,D) joins existing P(x,y) → type(x,R).
    /// The trigger class is the restriction's filler D; `inverse` mirrors
    /// which property index the restriction was registered under.
    SomeValuesFromFiller { restriction: Sid, inverse: bool },
    /// `cls-int1`: the trigger class can satisfy a member expression of this
    /// intersection — re-evaluate the subject against the full member list.
    IntersectionMember { intersection: Sid },
    /// `cls-int2`: type(x,I) → type(x,Cᵢ) — the trigger class IS the intersection
    IntersectionBackward,
    /// `cls-uni`: the trigger class can satisfy a member expression of this union
    UnionMember { union: Sid },
}

/// Ontology-specific ground rules indexed by their trigger facts.
///
/// Built once per materialization (and, with A2, once per ontology epoch)
/// by [`compile`].
#[derive(Debug, Default)]
pub struct CompiledRules {
    /// rulesFor(predicate): rules fired by a delta fact with this predicate.
    by_predicate: HashMap<Sid, Vec<PropertyRuleKind>>,
    /// rulesFor(class): rules fired by a delta `rdf:type` fact whose object
    /// class matches.
    by_class: HashMap<Sid, Vec<ClassRuleKind>>,
    /// `prp-spo2`: indices into [`OntologyRL::property_chains`] keyed by
    /// component predicate. A chain runs (whole, via the existing
    /// bidirectional-extension join) when any component predicate appears in
    /// the delta.
    chains_by_trigger: HashMap<Sid, Vec<usize>>,
    /// `cls-oo` enabled and at least one `owl:oneOf` restriction exists.
    one_of_active: bool,
}

impl CompiledRules {
    /// Rules triggered by a fact with this predicate.
    pub fn property_rules_for(&self, p: &Sid) -> &[PropertyRuleKind] {
        self.by_predicate.get(p).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Rules triggered by an `rdf:type` fact with this class object.
    pub fn class_rules_for(&self, class: &Sid) -> &[ClassRuleKind] {
        self.by_class.get(class).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Distinct chain indices triggered by any of the delta's predicates.
    ///
    /// Returned sorted so chain application order is deterministic.
    pub fn triggered_chains<'a>(
        &self,
        delta_predicates: impl Iterator<Item = &'a Sid>,
    ) -> Vec<usize> {
        let mut triggered: HashSet<usize> = HashSet::new();
        for p in delta_predicates {
            if let Some(indices) = self.chains_by_trigger.get(p) {
                triggered.extend(indices.iter().copied());
            }
        }
        let mut result: Vec<usize> = triggered.into_iter().collect();
        result.sort_unstable();
        result
    }

    /// Whether `cls-oo` should run (enabled and oneOf restrictions exist).
    pub fn one_of_active(&self) -> bool {
        self.one_of_active
    }

    /// Number of compiled rule bindings (diagnostic).
    pub fn rule_count(&self) -> usize {
        self.by_predicate.values().map(Vec::len).sum::<usize>()
            + self.by_class.values().map(Vec::len).sum::<usize>()
            + self.chains_by_trigger.values().map(Vec::len).sum::<usize>()
    }

    fn add_property_rule(&mut self, p: &Sid, rule: PropertyRuleKind) {
        self.by_predicate.entry(p.clone()).or_default().push(rule);
    }

    fn add_class_rule(&mut self, class: &Sid, rule: ClassRuleKind) {
        let rules = self.by_class.entry(class.clone()).or_default();
        // Trigger-class collection can revisit a class (e.g. a class that is
        // a member of an intersection both directly and through a nested
        // union); the binding must fire once.
        if !rules.contains(&rule) {
            rules.push(rule);
        }
    }
}

/// Compile the active ontology into trigger-indexed ground rules.
///
/// `opts.enabled_rules` is applied here: disabled rule families produce no
/// bindings. Phase-B identity rules are intentionally not compiled — see the
/// module docs.
pub fn compile(
    ontology: &OntologyRL,
    restrictions: &RestrictionIndex,
    opts: &ReasoningOptions,
) -> CompiledRules {
    let mut compiled = CompiledRules::default();

    // ---- property-axiom rules (prp-*) ----

    if opts.rule_enabled("prp-symp") {
        for p in ontology.symmetric_properties() {
            compiled.add_property_rule(p, PropertyRuleKind::Symmetric);
        }
    }
    if opts.rule_enabled("prp-trp") {
        for p in ontology.transitive_properties() {
            compiled.add_property_rule(p, PropertyRuleKind::Transitive);
        }
    }
    if opts.rule_enabled("prp-inv") {
        for p in ontology.properties_with_inverses() {
            compiled.add_property_rule(p, PropertyRuleKind::Inverse);
        }
    }
    if opts.rule_enabled("prp-dom") {
        for p in ontology.properties_with_domain() {
            compiled.add_property_rule(p, PropertyRuleKind::Domain);
        }
    }
    if opts.rule_enabled("prp-rng") {
        for p in ontology.properties_with_range() {
            compiled.add_property_rule(p, PropertyRuleKind::Range);
        }
    }
    if opts.rule_enabled("prp-spo1") {
        for p in ontology.properties_with_super_properties() {
            compiled.add_property_rule(p, PropertyRuleKind::SubProperty);
        }
    }
    if opts.rule_enabled("prp-spo2") {
        for (idx, chain) in ontology.property_chains().iter().enumerate() {
            let mut seen: HashSet<&Sid> = HashSet::new();
            for element in &chain.chain {
                if seen.insert(&element.property) {
                    compiled
                        .chains_by_trigger
                        .entry(element.property.clone())
                        .or_default()
                        .push(idx);
                }
            }
        }
    }

    // ---- class-hierarchy rules (cax-*) ----

    if opts.rule_enabled("cax-sco") {
        for c in ontology.classes_with_super_classes() {
            compiled.add_class_rule(c, ClassRuleKind::SubClass);
        }
    }
    if opts.rule_enabled_any(&["cax-eqc", "cax-eqc1", "cax-eqc2"]) {
        for c in ontology.classes_with_equivalent_classes() {
            compiled.add_class_rule(c, ClassRuleKind::EquivalentClass);
        }
    }

    // ---- restriction rules (cls-*) ----

    let hv1 = opts.rule_enabled("cls-hv1");
    let hv2 = opts.rule_enabled("cls-hv2");
    let svf = opts.rule_enabled("cls-svf1");
    let avf = opts.rule_enabled("cls-avf");
    let int1 = opts.rule_enabled("cls-int1");
    let int2 = opts.rule_enabled("cls-int2");
    let uni = opts.rule_enabled("cls-uni");

    for restriction in restrictions.iter() {
        let id = &restriction.restriction_id;
        match &restriction.restriction_type {
            RestrictionType::HasValue { .. } => {
                if hv1 {
                    compiled.add_class_rule(id, ClassRuleKind::HasValueBackward);
                }
            }
            RestrictionType::SomeValuesFrom {
                property,
                target_class,
            } => {
                // Type-side trigger: a new type(y, D) fact can complete the
                // join for restrictions on a (direct or inverse) named
                // property — matching how `RestrictionIndex::add_restriction`
                // registers them. Chain-expression restrictions have no
                // property-side index and keep their existing
                // (type-side-less) behavior.
                if svf {
                    match property {
                        PropertyExpression::Named(_) => {
                            compiled.add_class_rule(
                                target_class.sid(),
                                ClassRuleKind::SomeValuesFromFiller {
                                    restriction: id.clone(),
                                    inverse: false,
                                },
                            );
                        }
                        PropertyExpression::Inverse(inner)
                            if matches!(inner.as_ref(), PropertyExpression::Named(_)) =>
                        {
                            compiled.add_class_rule(
                                target_class.sid(),
                                ClassRuleKind::SomeValuesFromFiller {
                                    restriction: id.clone(),
                                    inverse: true,
                                },
                            );
                        }
                        _ => {}
                    }
                }
            }
            RestrictionType::AllValuesFrom { .. } => {
                if avf {
                    compiled.add_class_rule(id, ClassRuleKind::AllValuesFromType);
                }
            }
            RestrictionType::IntersectionOf { members } => {
                if int2 {
                    compiled.add_class_rule(id, ClassRuleKind::IntersectionBackward);
                }
                if int1 {
                    let mut triggers = HashSet::new();
                    collect_trigger_classes(members, restrictions, 0, &mut triggers);
                    for trigger in triggers {
                        compiled.add_class_rule(
                            &trigger,
                            ClassRuleKind::IntersectionMember {
                                intersection: id.clone(),
                            },
                        );
                    }
                }
            }
            RestrictionType::UnionOf { members } => {
                if uni {
                    let mut triggers = HashSet::new();
                    collect_trigger_classes(members, restrictions, 0, &mut triggers);
                    for trigger in triggers {
                        compiled.add_class_rule(
                            &trigger,
                            ClassRuleKind::UnionMember { union: id.clone() },
                        );
                    }
                }
            }
            RestrictionType::OneOf { .. } => {
                if opts.rule_enabled("cls-oo") {
                    compiled.one_of_active = true;
                }
            }
            // Identity-producing restrictions run with the other identity
            // rules in the fixpoint loop, not through the per-fact index.
            RestrictionType::MaxCardinality1 { .. }
            | RestrictionType::MaxQualifiedCardinality1 { .. } => {}
        }
    }

    // Property-side bindings for restriction rules, keyed by the named
    // property of the restriction's property expression (matching the
    // RestrictionIndex's per-property registration; chain expressions are
    // evaluated from the type side via `collect_property_values`).
    type RuleCtor = fn(Sid) -> PropertyRuleKind;
    for restriction in restrictions.iter() {
        let id = &restriction.restriction_id;
        let (enabled, property, direct_kind, inverse_kind): (
            bool,
            &PropertyExpression,
            RuleCtor,
            RuleCtor,
        ) = match &restriction.restriction_type {
            RestrictionType::HasValue { property, .. } => (
                hv2,
                property,
                |restriction| PropertyRuleKind::HasValueForward { restriction },
                |restriction| PropertyRuleKind::HasValueForwardInverse { restriction },
            ),
            RestrictionType::SomeValuesFrom { property, .. } => (
                svf,
                property,
                |restriction| PropertyRuleKind::SomeValuesFrom { restriction },
                |restriction| PropertyRuleKind::SomeValuesFromInverse { restriction },
            ),
            RestrictionType::AllValuesFrom { property, .. } => (
                avf,
                property,
                |restriction| PropertyRuleKind::AllValuesFromProp { restriction },
                |restriction| PropertyRuleKind::AllValuesFromPropInverse { restriction },
            ),
            _ => continue,
        };
        if !enabled {
            continue;
        }
        match property {
            PropertyExpression::Named(p) => {
                compiled.add_property_rule(p, direct_kind(id.clone()));
            }
            PropertyExpression::Inverse(inner) => {
                if let PropertyExpression::Named(p) = inner.as_ref() {
                    compiled.add_property_rule(p, inverse_kind(id.clone()));
                }
            }
            PropertyExpression::Chain(_) => {}
        }
    }

    compiled
}

/// Collect every class SID whose arrival as an `rdf:type` object could
/// change whether an entity satisfies one of `members`.
///
/// Mirrors the containment checks of `entity_satisfies_class_ref`: named
/// classes and non-composite anonymous expressions are checked by SID;
/// nested intersections/unions recurse into their members (and are
/// themselves checkable by SID when typed directly).
fn collect_trigger_classes(
    members: &[ClassRef],
    restrictions: &RestrictionIndex,
    depth: usize,
    out: &mut HashSet<Sid>,
) {
    const MAX_DEPTH: usize = 20;
    if depth >= MAX_DEPTH {
        return;
    }

    for member in members {
        match member {
            ClassRef::Named(sid) => {
                out.insert(sid.clone());
            }
            ClassRef::Anonymous(sid) => {
                out.insert(sid.clone());
                if let Some(nested) = restrictions.get(sid) {
                    match &nested.restriction_type {
                        RestrictionType::IntersectionOf { members }
                        | RestrictionType::UnionOf { members } => {
                            collect_trigger_classes(members, restrictions, depth + 1, out);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restrictions::ParsedRestriction;
    use crate::types::{ChainElement, PropertyChain};

    fn sid(n: u16) -> Sid {
        Sid::new(n, format!("test:{n}"))
    }

    fn ontology_with_symmetric_and_chain() -> OntologyRL {
        let mut symmetric = HashSet::new();
        symmetric.insert(sid(1));
        OntologyRL::new_full(
            symmetric,
            HashSet::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            {
                let mut super_classes = HashMap::new();
                super_classes.insert(sid(10), vec![sid(11)]);
                super_classes
            },
            HashMap::new(),
            vec![PropertyChain::new(
                sid(5),
                vec![ChainElement::direct(sid(2)), ChainElement::direct(sid(3))],
            )],
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
            0,
        )
    }

    #[test]
    fn compiles_property_and_class_triggers() {
        let ontology = ontology_with_symmetric_and_chain();
        let restrictions = RestrictionIndex::new();
        let compiled = compile(&ontology, &restrictions, &ReasoningOptions::default());

        assert_eq!(
            compiled.property_rules_for(&sid(1)),
            &[PropertyRuleKind::Symmetric]
        );
        assert!(compiled.property_rules_for(&sid(9)).is_empty());
        assert_eq!(
            compiled.class_rules_for(&sid(10)),
            &[ClassRuleKind::SubClass]
        );
        assert!(compiled.class_rules_for(&sid(11)).is_empty());
    }

    #[test]
    fn chain_triggers_on_any_component() {
        let ontology = ontology_with_symmetric_and_chain();
        let restrictions = RestrictionIndex::new();
        let compiled = compile(&ontology, &restrictions, &ReasoningOptions::default());

        let delta_p = [sid(3)];
        assert_eq!(compiled.triggered_chains(delta_p.iter()), vec![0]);
        let unrelated = [sid(7)];
        assert!(compiled.triggered_chains(unrelated.iter()).is_empty());
        // Both components present → chain still listed once.
        let both = [sid(2), sid(3)];
        assert_eq!(compiled.triggered_chains(both.iter()), vec![0]);
    }

    #[test]
    fn enabled_rules_filter_at_compile_time() {
        let ontology = ontology_with_symmetric_and_chain();
        let restrictions = RestrictionIndex::new();
        let opts = ReasoningOptions {
            enabled_rules: vec!["cax-sco".to_string()],
            ..Default::default()
        };
        let compiled = compile(&ontology, &restrictions, &opts);

        // prp-symp and prp-spo2 disabled → no property/chain bindings.
        assert!(compiled.property_rules_for(&sid(1)).is_empty());
        assert!(compiled.triggered_chains([sid(2)].iter()).is_empty());
        // cax-sco enabled → class binding present.
        assert_eq!(
            compiled.class_rules_for(&sid(10)),
            &[ClassRuleKind::SubClass]
        );
    }

    #[test]
    fn nested_union_members_trigger_intersection() {
        // I = intersectionOf(A, U) where U = unionOf(B, C): a new type fact
        // for any of A, B, C, or U itself must re-evaluate I.
        let ontology = OntologyRL::empty();
        let mut restrictions = RestrictionIndex::new();
        let union_id = sid(20);
        let intersection_id = sid(21);
        restrictions.add_restriction_for_test(ParsedRestriction {
            restriction_id: union_id.clone(),
            restriction_type: RestrictionType::UnionOf {
                members: vec![ClassRef::Named(sid(31)), ClassRef::Named(sid(32))],
            },
        });
        restrictions.add_restriction_for_test(ParsedRestriction {
            restriction_id: intersection_id.clone(),
            restriction_type: RestrictionType::IntersectionOf {
                members: vec![
                    ClassRef::Named(sid(30)),
                    ClassRef::Anonymous(union_id.clone()),
                ],
            },
        });

        let compiled = compile(&ontology, &restrictions, &ReasoningOptions::default());

        let member_rule = ClassRuleKind::IntersectionMember {
            intersection: intersection_id.clone(),
        };
        for trigger in [sid(30), sid(31), sid(32), union_id.clone()] {
            assert!(
                compiled.class_rules_for(&trigger).contains(&member_rule),
                "expected {trigger:?} to trigger the intersection"
            );
        }
        assert_eq!(
            compiled.class_rules_for(&intersection_id),
            &[ClassRuleKind::IntersectionBackward]
        );
        // The union's own member triggers are also compiled for cls-uni.
        assert!(compiled
            .class_rules_for(&sid(31))
            .contains(&ClassRuleKind::UnionMember {
                union: union_id.clone()
            }));
    }
}
