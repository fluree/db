//! Class hierarchy OWL2-RL rules (cax-*).
//!
//! This module implements class hierarchy rules from the OWL2-RL profile:
//! - `cax-sco` - SubClassOf
//! - `cax-eqc` - EquivalentClass

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;

use crate::ontology_rl::OntologyRL;

use super::util::{ref_dt, RuleContext};

/// Apply cax-sco rule (rdfs:subClassOf class hierarchy)
///
/// Rule: type(x, C1), C1 rdfs:subClassOf* C2 → type(x, C2)
///
/// For each rdf:type assertion in delta where the class has superclasses,
/// derive type facts for all superclasses.
pub fn apply_subclass_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    let delta = ctx.delta;
    for flake in delta.get_by_p(ctx.rdf_type_sid) {
        // type(x, C1) - object must be a Ref (class)
        if let FlakeValue::Ref(c1) = &flake.o {
            fire_subclass(ontology, flake, c1, ctx);
        }
    }
}

/// Per-fact body of `cax-sco`: `flake` is `type(x, c1)` and `c1` has superclasses.
pub(crate) fn fire_subclass(
    ontology: &OntologyRL,
    flake: &Flake,
    c1: &Sid,
    ctx: &mut RuleContext<'_>,
) {
    let superclasses = ontology.super_classes_of(c1);
    if superclasses.is_empty() {
        return;
    }

    let ref_dt = ref_dt();

    // Canonicalize subject for consistent derived facts
    let x_canonical = ctx.same_as.canonical(&flake.s);

    // Derive type(x, C2) for each superclass C2
    for c2 in superclasses {
        let derived_flake = Flake::new(
            x_canonical.clone(),
            ctx.rdf_type_sid.clone(),
            FlakeValue::Ref(c2.clone()),
            ref_dt.clone(),
            ctx.t,
            true,
            None,
        );

        // Only add if not already derived
        if !ctx
            .derived
            .contains(&derived_flake.s, &derived_flake.p, &derived_flake.o)
        {
            ctx.new_delta.push(derived_flake);
            ctx.diagnostics.record_rule_fired("cax-sco");
        }
    }
}

/// Apply cax-eqc1/cax-eqc2 (EquivalentClass) rule
///
/// cax-eqc: type(x, C1), equivalentClass(C1, C2) → type(x, C2)
///
/// Since equivalentClass is bidirectional, we store both directions
/// (C1→C2 and C2→C1), so this implementation works for both directions.
///
/// For each rdf:type assertion in delta where the class has equivalent classes,
/// derive type facts for all equivalent classes.
pub fn apply_equivalent_class_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    let delta = ctx.delta;
    for flake in delta.get_by_p(ctx.rdf_type_sid) {
        // type(x, C1) - object must be a Ref (class)
        if let FlakeValue::Ref(c1) = &flake.o {
            fire_equivalent_class(ontology, flake, c1, ctx);
        }
    }
}

/// Per-fact body of `cax-eqc`: `flake` is `type(x, c1)` and `c1` has equivalents.
pub(crate) fn fire_equivalent_class(
    ontology: &OntologyRL,
    flake: &Flake,
    c1: &Sid,
    ctx: &mut RuleContext<'_>,
) {
    let equiv_classes = ontology.equivalent_classes_of(c1);
    if equiv_classes.is_empty() {
        return;
    }

    let ref_dt = ref_dt();

    // Canonicalize subject for consistent derived facts
    let x_canonical = ctx.same_as.canonical(&flake.s);

    // Derive type(x, C2) for each equivalent class C2
    for c2 in equiv_classes {
        let derived_flake = Flake::new(
            x_canonical.clone(),
            ctx.rdf_type_sid.clone(),
            FlakeValue::Ref(c2.clone()),
            ref_dt.clone(),
            ctx.t,
            true,
            None,
        );

        // Only add if not already derived
        if !ctx
            .derived
            .contains(&derived_flake.s, &derived_flake.p, &derived_flake.o)
        {
            ctx.new_delta.push(derived_flake);
            ctx.diagnostics.record_rule_fired("cax-eqc");
        }
    }
}
