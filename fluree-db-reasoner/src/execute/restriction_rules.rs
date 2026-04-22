//! OWL restriction rules (cls-*).
//!
//! This module implements OWL restriction rules from the OWL2-RL profile:
//! - `cls-hv1/hv2` - HasValue
//! - `cls-svf1` - SomeValuesFrom
//! - `cls-avf` - AllValuesFrom
//! - `cls-maxc2` - MaxCardinality = 1
//! - `cls-maxqc3/4` - MaxQualifiedCardinality = 1
//! - `cls-int1/int2` - IntersectionOf
//! - `cls-uni` - UnionOf
//! - `cls-oo` - OneOf

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;
use hashbrown::HashSet;

use crate::restrictions::{ClassRef, RestrictionIndex, RestrictionType, RestrictionValue};
use crate::same_as::SameAsTracker;
use crate::types::{ChainElement, PropertyExpression};

use super::delta::DeltaSet;
use super::derived::DerivedSet;
use super::util::{ref_dt, IdentityRuleContext, RuleContext};

// ============================================================================
// Property value collection helpers
// ============================================================================

/// Convert a property expression into a traversal path.
///
/// This normalizes:
/// - `Named(P)` -> `[P]`
/// - `Inverse(P)` -> `[P^-1]`
/// - `Chain([P1, P2])` -> `[P1, P2]`
/// - `Inverse(Chain([P1, P2]))` -> `[P2^-1, P1^-1]`
fn property_expression_path(
    property: &PropertyExpression,
    invert: bool,
) -> Option<Vec<ChainElement>> {
    match property {
        PropertyExpression::Named(prop_sid) => Some(vec![if invert {
            ChainElement::inverse(prop_sid.clone())
        } else {
            ChainElement::direct(prop_sid.clone())
        }]),
        PropertyExpression::Inverse(inner) => property_expression_path(inner, !invert),
        PropertyExpression::Chain(elements) => {
            let path: Vec<ChainElement> = if invert {
                elements
                    .iter()
                    .rev()
                    .cloned()
                    .map(ChainElement::with_inverse_toggle)
                    .collect()
            } else {
                elements.clone()
            };
            Some(path)
        }
    }
}

/// Get all objects for facts with (predicate, subject) in delta ∪ derived.
fn get_all_objects_for_subject(
    predicate: &Sid,
    subject: &Sid,
    delta: &DeltaSet,
    derived: &DerivedSet,
    same_as: &SameAsTracker,
) -> HashSet<Sid> {
    let mut objects = HashSet::new();

    for flake in delta.get_by_ps(predicate, subject) {
        if let FlakeValue::Ref(obj) = &flake.o {
            objects.insert(same_as.canonical(obj));
        }
    }

    for flake in derived.get_by_ps(predicate, subject) {
        if let FlakeValue::Ref(obj) = &flake.o {
            objects.insert(same_as.canonical(obj));
        }
    }

    objects
}

/// Get all subjects for facts with (predicate, object) in delta ∪ derived.
fn get_all_subjects_for_object(
    predicate: &Sid,
    object: &Sid,
    delta: &DeltaSet,
    derived: &DerivedSet,
    same_as: &SameAsTracker,
) -> HashSet<Sid> {
    let mut subjects = HashSet::new();

    for flake in delta.get_by_po(predicate, object) {
        subjects.insert(same_as.canonical(&flake.s));
    }

    for flake in derived.get_by_po(predicate, object) {
        subjects.insert(same_as.canonical(&flake.s));
    }

    subjects
}

/// Collect all property values for a subject given a PropertyExpression from delta ∪ derived.
///
/// Chain expressions must traverse the union of delta and derived facts; otherwise
/// mixed paths such as `delta(P1) + derived(P2)` would be missed.
fn collect_property_values(
    property: &PropertyExpression,
    subject: &Sid,
    delta: &DeltaSet,
    derived: &DerivedSet,
    same_as: &SameAsTracker,
) -> Vec<Sid> {
    let Some(path) = property_expression_path(property, false) else {
        return Vec::new();
    };

    let mut current_nodes = HashSet::new();
    current_nodes.insert(subject.clone());
    current_nodes.insert(same_as.canonical(subject));

    for element in path {
        let mut next_nodes = HashSet::new();

        for node in &current_nodes {
            if element.is_inverse {
                next_nodes.extend(get_all_subjects_for_object(
                    &element.property,
                    node,
                    delta,
                    derived,
                    same_as,
                ));
            } else {
                next_nodes.extend(get_all_objects_for_subject(
                    &element.property,
                    node,
                    delta,
                    derived,
                    same_as,
                ));
            }
        }

        if next_nodes.is_empty() {
            return Vec::new();
        }

        current_nodes = next_nodes;
    }

    current_nodes.into_iter().collect()
}

// ============================================================================
// HasValue rules (cls-hv1, cls-hv2)
// ============================================================================

/// Apply cls-hv1 (HasValue backward) rule
///
/// cls-hv1: type(x, C) where C is hasValue restriction on P with value v → P(x, v)
///
/// If x is of type C (where C is a hasValue restriction class),
/// then x has the required property value.
///
/// **Restricted form**: Only supports Ref values (IRIs/blank nodes).
/// Literal hasValue restrictions are not yet supported.
///
/// **Property expressions**:
/// - Named property: derive P(x, v)
/// - Inverse property: derive P(v, x) (reversed)
/// - Chain property: not supported for backward entailment (chains need intermediate links)
pub fn apply_has_value_backward_rule(restrictions: &RestrictionIndex, ctx: &mut RuleContext<'_>) {
    let ref_dt = ref_dt();

    // Process rdf:type facts in delta
    for flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
        // type(x, C) where C might be a hasValue restriction
        if let FlakeValue::Ref(restriction_class) = &flake.o {
            if let Some(restriction) = restrictions.get(restriction_class) {
                if let RestrictionType::HasValue { property, value } = &restriction.restriction_type
                {
                    // Restricted form: only Ref values are supported
                    let RestrictionValue::Ref(value_ref) = value;
                    let x_canonical = ctx.same_as.canonical(&flake.s);
                    let v_canonical = ctx.same_as.canonical(value_ref);

                    // Handle property expression
                    let derived_flake = match property {
                        PropertyExpression::Named(prop_sid) => {
                            // Named property: derive P(x, v)
                            Some(Flake::new(
                                x_canonical.clone(),
                                prop_sid.clone(),
                                FlakeValue::Ref(v_canonical.clone()),
                                ref_dt.clone(),
                                ctx.t,
                                true,
                                None,
                            ))
                        }
                        PropertyExpression::Inverse(inner) => {
                            // Inverse property: derive P(v, x)
                            if let PropertyExpression::Named(prop_sid) = inner.as_ref() {
                                Some(Flake::new(
                                    v_canonical.clone(),
                                    prop_sid.clone(),
                                    FlakeValue::Ref(x_canonical.clone()),
                                    ref_dt.clone(),
                                    ctx.t,
                                    true,
                                    None,
                                ))
                            } else {
                                // Complex inverse (e.g., inverse of chain) - not supported
                                None
                            }
                        }
                        PropertyExpression::Chain(_) => {
                            // Chain properties can't be directly derived backward
                            // (they need intermediate links to exist)
                            None
                        }
                    };

                    if let Some(df) = derived_flake {
                        if !ctx.derived.contains(&df.s, &df.p, &df.o) {
                            ctx.new_delta.push(df);
                            ctx.diagnostics.record_rule_fired("cls-hv1");
                        }
                    }
                }
            }
        }
    }
}

/// Apply cls-hv2 (HasValue forward) rule
///
/// cls-hv2: P(x, v) where C is hasValue restriction on P with value v → type(x, C)
///
/// If x has the required property value, then x is of type C.
///
/// **Restricted form**: Only supports Ref values (IRIs/blank nodes).
/// Literal hasValue restrictions are not yet supported.
pub fn apply_has_value_forward_rule(restrictions: &RestrictionIndex, ctx: &mut RuleContext<'_>) {
    let ref_dt = ref_dt();

    // Part 1: Direct properties - P(x, v) -> type(x, C)
    for property in restrictions.restricted_properties() {
        let restriction_ids = restrictions.has_value_restrictions_for(property);
        if restriction_ids.is_empty() {
            continue;
        }

        // Get facts with this property from delta
        for flake in ctx.delta.get_by_p(property) {
            // Restricted form: only Ref values are supported, so skip literals
            let FlakeValue::Ref(actual_ref) = &flake.o else {
                continue;
            };

            let x_canonical = ctx.same_as.canonical(&flake.s);

            // Check each hasValue restriction on this property
            for restriction_id in restriction_ids {
                if let Some(restriction) = restrictions.get(restriction_id) {
                    if let RestrictionType::HasValue { value, .. } = &restriction.restriction_type {
                        // Restricted form: only Ref values
                        let RestrictionValue::Ref(required_ref) = value;

                        // Compare canonical forms
                        let matches = ctx.same_as.canonical(required_ref)
                            == ctx.same_as.canonical(actual_ref);

                        if matches {
                            // Derive type(x, C)
                            let derived_flake = Flake::new(
                                x_canonical.clone(),
                                ctx.rdf_type_sid.clone(),
                                FlakeValue::Ref(restriction.restriction_id.clone()),
                                ref_dt.clone(),
                                ctx.t,
                                true,
                                None,
                            );

                            if !ctx.derived.contains(
                                &derived_flake.s,
                                &derived_flake.p,
                                &derived_flake.o,
                            ) {
                                ctx.new_delta.push(derived_flake);
                                ctx.diagnostics.record_rule_fired("cls-hv2");
                            }
                        }
                    }
                }
            }
        }
    }

    // Part 2: Inverse properties - P(v, x) -> type(x, C) when restriction has inverseOf(P)
    // hasValue(inverseOf(P), v) means: x is of type C if P(v, x) exists
    for property in restrictions.restricted_inverse_properties() {
        let restriction_ids = restrictions.has_value_restrictions_for_inverse(property);
        if restriction_ids.is_empty() {
            continue;
        }

        // For inverse property, we look for facts where entity x is in the OBJECT position
        // P(v, x) means inverseOf(P)(x, v)
        for flake in ctx.delta.get_by_p(property) {
            // x is the object (the entity we want to type)
            let FlakeValue::Ref(x) = &flake.o else {
                continue;
            };
            // v is the subject (the required value)
            let actual_value = &flake.s;

            let x_canonical = ctx.same_as.canonical(x);

            // Check each hasValue restriction with this inverse property
            for restriction_id in restriction_ids {
                if let Some(restriction) = restrictions.get(restriction_id) {
                    if let RestrictionType::HasValue { value, .. } = &restriction.restriction_type {
                        let RestrictionValue::Ref(required_ref) = value;

                        // For inverse: check if actual subject matches required value
                        let matches = ctx.same_as.canonical(required_ref)
                            == ctx.same_as.canonical(actual_value);

                        if matches {
                            // Derive type(x, C)
                            let derived_flake = Flake::new(
                                x_canonical.clone(),
                                ctx.rdf_type_sid.clone(),
                                FlakeValue::Ref(restriction.restriction_id.clone()),
                                ref_dt.clone(),
                                ctx.t,
                                true,
                                None,
                            );

                            if !ctx.derived.contains(
                                &derived_flake.s,
                                &derived_flake.p,
                                &derived_flake.o,
                            ) {
                                ctx.new_delta.push(derived_flake);
                                ctx.diagnostics.record_rule_fired("cls-hv2");
                            }
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// SomeValuesFrom rule (cls-svf1)
// ============================================================================

/// Apply cls-svf1 (SomeValuesFrom) rule
///
/// cls-svf1: P(x, y), type(y, D), someValuesFrom(C, P, D) → type(x, C)
///
/// If x has property P with value y, and y is of type D, and C is a
/// restriction class requiring some value of type D for property P,
/// then x is of type C.
///
/// This rule requires joining:
/// 1. Property facts P(x, y) from delta
/// 2. Type facts type(y, D) from delta or derived
/// 3. SomeValuesFrom restrictions on P requiring type D
///
/// **Property expressions**:
/// - Named property: find P(x, y) facts where x is subject, y is object
/// - Inverse property: find P(y, x) facts where x is object, y is subject
pub fn apply_some_values_from_rule(restrictions: &RestrictionIndex, ctx: &mut RuleContext<'_>) {
    let ref_dt = ref_dt();

    // Helper to collect types for an entity
    let collect_types = |entity: &Sid,
                         y_canonical: &Sid,
                         delta: &DeltaSet,
                         derived: &DerivedSet,
                         rdf_type_sid: &Sid|
     -> HashSet<Sid> {
        let mut types: HashSet<Sid> = HashSet::new();
        // From delta
        for type_flake in delta.get_by_ps(rdf_type_sid, y_canonical) {
            if let FlakeValue::Ref(type_class) = &type_flake.o {
                types.insert(type_class.clone());
            }
        }
        // Also check non-canonical form in delta
        for type_flake in delta.get_by_ps(rdf_type_sid, entity) {
            if let FlakeValue::Ref(type_class) = &type_flake.o {
                types.insert(type_class.clone());
            }
        }
        // From derived
        for type_flake in derived.get_by_ps(rdf_type_sid, y_canonical) {
            if let FlakeValue::Ref(type_class) = &type_flake.o {
                types.insert(type_class.clone());
            }
        }
        types
    };

    // Part 1a: Direct properties - P(x, y) -> type(x, C)
    for property in restrictions.restricted_properties() {
        let restriction_ids = restrictions.some_values_from_restrictions_for(property);
        if restriction_ids.is_empty() {
            continue;
        }

        // Process property facts P(x, y) from delta
        for flake in ctx.delta.get_by_p(property) {
            // y must be a Ref to check its type
            if let FlakeValue::Ref(y) = &flake.o {
                let x_canonical = ctx.same_as.canonical(&flake.s);
                let y_canonical = ctx.same_as.canonical(y);

                let y_types =
                    collect_types(y, &y_canonical, ctx.delta, ctx.derived, ctx.rdf_type_sid);

                for restriction_id in restriction_ids {
                    if let Some(restriction) = restrictions.get(restriction_id) {
                        if let RestrictionType::SomeValuesFrom { target_class, .. } =
                            &restriction.restriction_type
                        {
                            if y_types.contains(target_class.sid()) {
                                let derived_flake = Flake::new(
                                    x_canonical.clone(),
                                    ctx.rdf_type_sid.clone(),
                                    FlakeValue::Ref(restriction.restriction_id.clone()),
                                    ref_dt.clone(),
                                    ctx.t,
                                    true,
                                    None,
                                );

                                if !ctx.derived.contains(
                                    &derived_flake.s,
                                    &derived_flake.p,
                                    &derived_flake.o,
                                ) {
                                    ctx.new_delta.push(derived_flake);
                                    ctx.diagnostics.record_rule_fired("cls-svf1");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Part 1b: Inverse properties - P(y, x) -> type(x, C) when restriction has inverseOf(P)
    // someValuesFrom(inverseOf(P), D) means: x is of type C if P(y, x) and type(y, D)
    for property in restrictions.restricted_inverse_properties() {
        let restriction_ids = restrictions.some_values_from_restrictions_for_inverse(property);
        if restriction_ids.is_empty() {
            continue;
        }

        // For inverse property: P(y, x) where x is the entity to type, y is the value to check
        for flake in ctx.delta.get_by_p(property) {
            // x is in the object position (entity to be typed)
            let FlakeValue::Ref(x) = &flake.o else {
                continue;
            };
            // y is in the subject position (whose type we check)
            let y = &flake.s;

            let x_canonical = ctx.same_as.canonical(x);
            let y_canonical = ctx.same_as.canonical(y);

            let y_types = collect_types(y, &y_canonical, ctx.delta, ctx.derived, ctx.rdf_type_sid);

            for restriction_id in restriction_ids {
                if let Some(restriction) = restrictions.get(restriction_id) {
                    if let RestrictionType::SomeValuesFrom { target_class, .. } =
                        &restriction.restriction_type
                    {
                        if y_types.contains(target_class.sid()) {
                            let derived_flake = Flake::new(
                                x_canonical.clone(),
                                ctx.rdf_type_sid.clone(),
                                FlakeValue::Ref(restriction.restriction_id.clone()),
                                ref_dt.clone(),
                                ctx.t,
                                true,
                                None,
                            );

                            if !ctx.derived.contains(
                                &derived_flake.s,
                                &derived_flake.p,
                                &derived_flake.o,
                            ) {
                                ctx.new_delta.push(derived_flake);
                                ctx.diagnostics.record_rule_fired("cls-svf1");
                            }
                        }
                    }
                }
            }
        }
    }

    // Part 2a: Process when new type facts arrive (type(y, D) in delta)
    // and check if y is already an object of some P(x, y) fact (direct property)
    for flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
        if let FlakeValue::Ref(d_class) = &flake.o {
            let y_canonical = ctx.same_as.canonical(&flake.s);

            // For each direct property with someValuesFrom restrictions targeting D
            for property in restrictions.restricted_properties() {
                let restriction_ids = restrictions.some_values_from_restrictions_for(property);
                if restriction_ids.is_empty() {
                    continue;
                }

                // Check each restriction to see if it targets class D
                for restriction_id in restriction_ids {
                    if let Some(restriction) = restrictions.get(restriction_id) {
                        if let RestrictionType::SomeValuesFrom { target_class, .. } =
                            &restriction.restriction_type
                        {
                            if target_class.sid() == d_class {
                                // Look for P(x, y) facts where y is the object
                                for prop_flake in ctx.derived.get_by_po(property, &y_canonical) {
                                    let x_canonical = ctx.same_as.canonical(&prop_flake.s);

                                    let derived_flake = Flake::new(
                                        x_canonical.clone(),
                                        ctx.rdf_type_sid.clone(),
                                        FlakeValue::Ref(restriction.restriction_id.clone()),
                                        ref_dt.clone(),
                                        ctx.t,
                                        true,
                                        None,
                                    );

                                    if !ctx.derived.contains(
                                        &derived_flake.s,
                                        &derived_flake.p,
                                        &derived_flake.o,
                                    ) {
                                        ctx.new_delta.push(derived_flake);
                                        ctx.diagnostics.record_rule_fired("cls-svf1");
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Part 2b: Also check inverse properties - P(y, x) where y is now typed
            for property in restrictions.restricted_inverse_properties() {
                let restriction_ids =
                    restrictions.some_values_from_restrictions_for_inverse(property);
                if restriction_ids.is_empty() {
                    continue;
                }

                for restriction_id in restriction_ids {
                    if let Some(restriction) = restrictions.get(restriction_id) {
                        if let RestrictionType::SomeValuesFrom { target_class, .. } =
                            &restriction.restriction_type
                        {
                            if target_class.sid() == d_class {
                                // For inverse: look for P(y, x) facts where y is the subject
                                // and x (the object) should be typed
                                for prop_flake in ctx.derived.get_by_ps(property, &y_canonical) {
                                    if let FlakeValue::Ref(x) = &prop_flake.o {
                                        let x_canonical = ctx.same_as.canonical(x);

                                        let derived_flake = Flake::new(
                                            x_canonical.clone(),
                                            ctx.rdf_type_sid.clone(),
                                            FlakeValue::Ref(restriction.restriction_id.clone()),
                                            ref_dt.clone(),
                                            ctx.t,
                                            true,
                                            None,
                                        );

                                        if !ctx.derived.contains(
                                            &derived_flake.s,
                                            &derived_flake.p,
                                            &derived_flake.o,
                                        ) {
                                            ctx.new_delta.push(derived_flake);
                                            ctx.diagnostics.record_rule_fired("cls-svf1");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// AllValuesFrom rule (cls-avf)
// ============================================================================

/// Apply cls-avf (AllValuesFrom) rule
///
/// cls-avf: type(x, C), P(x, y), allValuesFrom(C, P, D) → type(y, D)
///
/// If x is of type C (where C is an allValuesFrom restriction requiring all
/// values of property P to be of type D), and x has property P with value y,
/// then y must be of type D.
///
/// **Property expressions**:
/// - Named property: find P(x, y) facts
/// - Inverse property: find P(y, x) facts (y is in subject position)
/// - Chain property: not yet supported
pub fn apply_all_values_from_rule(restrictions: &RestrictionIndex, ctx: &mut RuleContext<'_>) {
    let ref_dt = ref_dt();

    // Process type facts in delta: type(x, C) where C is an allValuesFrom restriction
    for flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
        if let FlakeValue::Ref(restriction_class) = &flake.o {
            if let Some(restriction) = restrictions.get(restriction_class) {
                if let RestrictionType::AllValuesFrom {
                    property,
                    target_class,
                } = &restriction.restriction_type
                {
                    let target_sid = target_class.sid();

                    // Track what we've derived to avoid duplicates
                    let mut seen: HashSet<(Sid, Sid)> = HashSet::new();

                    // Collect values over the union of delta and derived so chain
                    // expressions can span both sets within the current iteration.
                    let all_values = collect_property_values(
                        property,
                        &flake.s,
                        ctx.delta,
                        ctx.derived,
                        ctx.same_as,
                    );

                    for y_canonical in all_values {
                        // Skip if we've already derived this
                        if !seen.insert((y_canonical.clone(), target_sid.clone())) {
                            continue;
                        }

                        let derived_flake = Flake::new(
                            y_canonical.clone(),
                            ctx.rdf_type_sid.clone(),
                            FlakeValue::Ref(target_sid.clone()),
                            ref_dt.clone(),
                            ctx.t,
                            true,
                            None,
                        );

                        if !ctx.derived.contains(
                            &derived_flake.s,
                            &derived_flake.p,
                            &derived_flake.o,
                        ) {
                            ctx.new_delta.push(derived_flake);
                            ctx.diagnostics.record_rule_fired("cls-avf");
                        }
                    }
                }
            }
        }
    }

    // Also process when new property facts arrive: P(x, y) in delta
    // and x already has type C (allValuesFrom restriction) in DERIVED ONLY
    // (delta-vs-delta case is handled above)

    // Part 2a: Direct properties - P(x, y) arrives, x has type C in derived
    for property in restrictions.restricted_properties() {
        let restriction_ids = restrictions.all_values_from_restrictions_for(property);
        if restriction_ids.is_empty() {
            continue;
        }

        for flake in ctx.delta.get_by_p(property) {
            if let FlakeValue::Ref(y) = &flake.o {
                let x_canonical = ctx.same_as.canonical(&flake.s);
                let y_canonical = ctx.same_as.canonical(y);

                // Check if x has any allValuesFrom restriction type in DERIVED only
                // (delta-vs-delta is already handled by the first code path)
                for restriction_id in restriction_ids {
                    if let Some(restriction) = restrictions.get(restriction_id) {
                        if let RestrictionType::AllValuesFrom { target_class, .. } =
                            &restriction.restriction_type
                        {
                            // Check if x is of type C (the restriction class) in derived ONLY
                            let x_has_type_in_derived = ctx
                                .derived
                                .get_by_ps(ctx.rdf_type_sid, &x_canonical)
                                .any(|f| {
                                    if let FlakeValue::Ref(c) = &f.o {
                                        c == restriction_id
                                    } else {
                                        false
                                    }
                                });

                            if x_has_type_in_derived {
                                let derived_flake = Flake::new(
                                    y_canonical.clone(),
                                    ctx.rdf_type_sid.clone(),
                                    FlakeValue::Ref(target_class.sid().clone()),
                                    ref_dt.clone(),
                                    ctx.t,
                                    true,
                                    None,
                                );

                                if !ctx.derived.contains(
                                    &derived_flake.s,
                                    &derived_flake.p,
                                    &derived_flake.o,
                                ) {
                                    ctx.new_delta.push(derived_flake);
                                    ctx.diagnostics.record_rule_fired("cls-avf");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Part 2b: Inverse properties - P(y, x) arrives, x has type C in derived
    // For allValuesFrom(inverseOf(P), D): when P(y, x) arrives and type(x, C) in derived,
    // derive type(y, D)
    for property in restrictions.restricted_inverse_properties() {
        let restriction_ids = restrictions.all_values_from_restrictions_for_inverse(property);
        if restriction_ids.is_empty() {
            continue;
        }

        // For inverse: P(y, x) means inverseOf(P)(x, y)
        // x is in object position, y is in subject position
        for flake in ctx.delta.get_by_p(property) {
            // x is the object (entity with the restriction type)
            let FlakeValue::Ref(x) = &flake.o else {
                continue;
            };
            // y is the subject (entity to be typed)
            let y = &flake.s;

            let x_canonical = ctx.same_as.canonical(x);
            let y_canonical = ctx.same_as.canonical(y);

            for restriction_id in restriction_ids {
                if let Some(restriction) = restrictions.get(restriction_id) {
                    if let RestrictionType::AllValuesFrom { target_class, .. } =
                        &restriction.restriction_type
                    {
                        // Check if x is of type C (the restriction class) in derived ONLY
                        let x_has_type_in_derived = ctx
                            .derived
                            .get_by_ps(ctx.rdf_type_sid, &x_canonical)
                            .any(|f| {
                                if let FlakeValue::Ref(c) = &f.o {
                                    c == restriction_id
                                } else {
                                    false
                                }
                            });

                        if x_has_type_in_derived {
                            let derived_flake = Flake::new(
                                y_canonical.clone(),
                                ctx.rdf_type_sid.clone(),
                                FlakeValue::Ref(target_class.sid().clone()),
                                ref_dt.clone(),
                                ctx.t,
                                true,
                                None,
                            );

                            if !ctx.derived.contains(
                                &derived_flake.s,
                                &derived_flake.p,
                                &derived_flake.o,
                            ) {
                                ctx.new_delta.push(derived_flake);
                                ctx.diagnostics.record_rule_fired("cls-avf");
                            }
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// Cardinality rules (cls-maxc2, cls-maxqc3/4)
// ============================================================================

/// Apply cls-maxc2 (MaxCardinality = 1) rule
///
/// cls-maxc2: P(x, y1), P(x, y2), type(x, C) → sameAs(y1, y2)
/// where C is a maxCardinality(P, 1) restriction
///
/// If x is of type C (where C restricts property P to have at most one value),
/// and x has P with values y1 and y2, then y1 and y2 must be owl:sameAs.
///
/// This is an identity-producing rule and should be applied in Phase B.
pub fn apply_max_cardinality_rule(
    restrictions: &RestrictionIndex,
    ctx: &mut IdentityRuleContext<'_>,
) {
    let ref_dt = ref_dt();

    // For each property that has maxCardinality=1 restrictions
    for property in restrictions.restricted_properties() {
        let restriction_ids = restrictions.max_cardinality_restrictions_for(property);
        if restriction_ids.is_empty() {
            continue;
        }

        // Skip if no relevant facts
        let delta_has_p = ctx.delta.get_by_p(property).next().is_some();
        let delta_has_type = ctx.delta.get_by_p(ctx.rdf_type_sid).next().is_some();
        let derived_has_p = ctx.derived.get_by_p(property).next().is_some();
        if !(delta_has_p || delta_has_type || ctx.same_as_changed && derived_has_p) {
            continue;
        }

        // Collect subjects that are instances of any maxCardinality=1 restriction class
        let mut restriction_subjects: HashSet<Sid> = HashSet::new();

        for restriction_id in restriction_ids {
            // Find all x such that type(x, C) in delta or derived
            for type_flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
                if let FlakeValue::Ref(cls) = &type_flake.o {
                    if cls == restriction_id {
                        let x_canonical = ctx.same_as.canonical(&type_flake.s);
                        restriction_subjects.insert(x_canonical);
                    }
                }
            }
            for type_flake in ctx.derived.get_by_p(ctx.rdf_type_sid) {
                if let FlakeValue::Ref(cls) = &type_flake.o {
                    if cls == restriction_id {
                        let x_canonical = ctx.same_as.canonical(&type_flake.s);
                        restriction_subjects.insert(x_canonical);
                    }
                }
            }
        }

        if restriction_subjects.is_empty() {
            continue;
        }

        // For each subject x that is a restriction instance, collect P(x, y) values
        for x_canonical in &restriction_subjects {
            let mut objects: Vec<Sid> = Vec::new();

            // From delta
            for prop_flake in ctx.delta.get_by_ps(property, x_canonical) {
                if let FlakeValue::Ref(y) = &prop_flake.o {
                    let y_canonical = ctx.same_as.canonical(y);
                    objects.push(y_canonical);
                }
            }

            // From derived
            for prop_flake in ctx.derived.get_by_ps(property, x_canonical) {
                if let FlakeValue::Ref(y) = &prop_flake.o {
                    let y_canonical = ctx.same_as.canonical(y);
                    objects.push(y_canonical);
                }
            }

            // Deduplicate and check for conflicts
            let unique_objects: HashSet<Sid> = objects.into_iter().collect();
            if unique_objects.len() <= 1 {
                continue;
            }

            // Derive sameAs for conflicting values
            let objects_vec: Vec<Sid> = unique_objects.into_iter().collect();
            let first = &objects_vec[0];
            for other in &objects_vec[1..] {
                let same_as_flake = Flake::new(
                    first.clone(),
                    ctx.owl_same_as_sid.clone(),
                    FlakeValue::Ref(other.clone()),
                    ref_dt.clone(),
                    ctx.t,
                    true,
                    None,
                );

                if !ctx
                    .derived
                    .contains(&same_as_flake.s, &same_as_flake.p, &same_as_flake.o)
                {
                    ctx.new_delta.push(same_as_flake);
                    ctx.diagnostics.record_rule_fired("cls-maxc2");
                }
            }
        }
    }
}

/// Apply cls-maxqc3/4 (MaxQualifiedCardinality = 1) rule
///
/// cls-maxqc3/4: P(x, y1), P(x, y2), type(x, C), type(y1, D), type(y2, D) → sameAs(y1, y2)
/// where C is a maxQualifiedCardinality(P, 1, D) restriction
///
/// Similar to maxCardinality, but only applies when the values are of the qualifying class D.
///
/// This is an identity-producing rule and should be applied in Phase B.
pub fn apply_max_qualified_cardinality_rule(
    restrictions: &RestrictionIndex,
    ctx: &mut IdentityRuleContext<'_>,
) {
    let ref_dt = ref_dt();

    // For each property that has maxQualifiedCardinality=1 restrictions
    for property in restrictions.restricted_properties() {
        let restriction_ids = restrictions.max_qualified_cardinality_restrictions_for(property);
        if restriction_ids.is_empty() {
            continue;
        }

        // Skip if no relevant facts
        let delta_has_p = ctx.delta.get_by_p(property).next().is_some();
        let delta_has_type = ctx.delta.get_by_p(ctx.rdf_type_sid).next().is_some();
        let derived_has_p = ctx.derived.get_by_p(property).next().is_some();
        if !(delta_has_p || delta_has_type || ctx.same_as_changed && derived_has_p) {
            continue;
        }

        for restriction_id in restriction_ids {
            if let Some(restriction) = restrictions.get(restriction_id) {
                if let RestrictionType::MaxQualifiedCardinality1 { on_class, .. } =
                    &restriction.restriction_type
                {
                    // Find all x such that type(x, C) in delta or derived
                    let mut restriction_subjects: HashSet<Sid> = HashSet::new();

                    for type_flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
                        if let FlakeValue::Ref(cls) = &type_flake.o {
                            if cls == restriction_id {
                                let x_canonical = ctx.same_as.canonical(&type_flake.s);
                                restriction_subjects.insert(x_canonical);
                            }
                        }
                    }
                    for type_flake in ctx.derived.get_by_p(ctx.rdf_type_sid) {
                        if let FlakeValue::Ref(cls) = &type_flake.o {
                            if cls == restriction_id {
                                let x_canonical = ctx.same_as.canonical(&type_flake.s);
                                restriction_subjects.insert(x_canonical);
                            }
                        }
                    }

                    if restriction_subjects.is_empty() {
                        continue;
                    }

                    // For each subject x that is a restriction instance
                    for x_canonical in &restriction_subjects {
                        let mut qualified_objects: Vec<Sid> = Vec::new();

                        // From delta
                        for prop_flake in ctx.delta.get_by_ps(property, x_canonical) {
                            if let FlakeValue::Ref(y) = &prop_flake.o {
                                let y_canonical = ctx.same_as.canonical(y);
                                // Only include if y is of type D
                                let y_has_type_d = ctx
                                    .delta
                                    .get_by_ps(ctx.rdf_type_sid, &y_canonical)
                                    .any(|f| {
                                        if let FlakeValue::Ref(cls) = &f.o {
                                            cls == on_class
                                        } else {
                                            false
                                        }
                                    })
                                    || ctx.derived.get_by_ps(ctx.rdf_type_sid, &y_canonical).any(
                                        |f| {
                                            if let FlakeValue::Ref(cls) = &f.o {
                                                cls == on_class
                                            } else {
                                                false
                                            }
                                        },
                                    );
                                if y_has_type_d {
                                    qualified_objects.push(y_canonical);
                                }
                            }
                        }

                        // From derived
                        for prop_flake in ctx.derived.get_by_ps(property, x_canonical) {
                            if let FlakeValue::Ref(y) = &prop_flake.o {
                                let y_canonical = ctx.same_as.canonical(y);
                                let y_has_type_d = ctx
                                    .delta
                                    .get_by_ps(ctx.rdf_type_sid, &y_canonical)
                                    .any(|f| {
                                        if let FlakeValue::Ref(cls) = &f.o {
                                            cls == on_class
                                        } else {
                                            false
                                        }
                                    })
                                    || ctx.derived.get_by_ps(ctx.rdf_type_sid, &y_canonical).any(
                                        |f| {
                                            if let FlakeValue::Ref(cls) = &f.o {
                                                cls == on_class
                                            } else {
                                                false
                                            }
                                        },
                                    );
                                if y_has_type_d {
                                    qualified_objects.push(y_canonical);
                                }
                            }
                        }

                        // Deduplicate and check for conflicts
                        let unique_objects: HashSet<Sid> = qualified_objects.into_iter().collect();
                        if unique_objects.len() <= 1 {
                            continue;
                        }

                        // Derive sameAs for conflicting qualified values
                        let objects_vec: Vec<Sid> = unique_objects.into_iter().collect();
                        let first = &objects_vec[0];
                        for other in &objects_vec[1..] {
                            let same_as_flake = Flake::new(
                                first.clone(),
                                ctx.owl_same_as_sid.clone(),
                                FlakeValue::Ref(other.clone()),
                                ref_dt.clone(),
                                ctx.t,
                                true,
                                None,
                            );

                            if !ctx.derived.contains(
                                &same_as_flake.s,
                                &same_as_flake.p,
                                &same_as_flake.o,
                            ) {
                                ctx.new_delta.push(same_as_flake);
                                ctx.diagnostics.record_rule_fired("cls-maxqc");
                            }
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// Intersection and Union rules (cls-int1, cls-int2, cls-uni)
// ============================================================================

/// Check if an entity satisfies a class expression (ClassRef).
///
/// This handles nested class expressions recursively:
/// - Named class: check if type(x, C) exists in types set
/// - Anonymous class expression: look up in RestrictionIndex and:
///   - IntersectionOf: entity satisfies ALL member class expressions
///   - UnionOf: entity satisfies ANY member class expression
///   - Other restrictions: check if type(x, restrictionId) exists
///
/// # Arguments
/// * `entity_types` - Set of type SIDs that the entity has
/// * `class_ref` - The class expression to check
/// * `restrictions` - The restriction index for looking up anonymous class expressions
/// * `depth` - Current recursion depth (to prevent infinite loops)
///
/// # Returns
/// `true` if the entity satisfies the class expression
fn entity_satisfies_class_ref(
    entity_types: &HashSet<Sid>,
    class_ref: &ClassRef,
    restrictions: &RestrictionIndex,
    depth: usize,
) -> bool {
    const MAX_DEPTH: usize = 20;

    if depth >= MAX_DEPTH {
        // Prevent infinite recursion on malformed ontologies
        return false;
    }

    match class_ref {
        ClassRef::Named(sid) => {
            // Named class: check if entity has this type
            entity_types.contains(sid)
        }
        ClassRef::Anonymous(sid) => {
            // Anonymous class expression: look up and recursively evaluate
            if let Some(restriction) = restrictions.get(sid) {
                match &restriction.restriction_type {
                    RestrictionType::IntersectionOf { members } => {
                        // Entity must satisfy ALL members
                        members.iter().all(|member| {
                            entity_satisfies_class_ref(
                                entity_types,
                                member,
                                restrictions,
                                depth + 1,
                            )
                        })
                    }
                    RestrictionType::UnionOf { members } => {
                        // Entity must satisfy ANY member
                        members.iter().any(|member| {
                            entity_satisfies_class_ref(
                                entity_types,
                                member,
                                restrictions,
                                depth + 1,
                            )
                        })
                    }
                    _ => {
                        // Other restriction types (hasValue, someValuesFrom, etc.)
                        // Check if entity has type(x, restrictionId)
                        entity_types.contains(sid)
                    }
                }
            } else {
                // Not found in index - fall back to checking if entity has this type
                entity_types.contains(sid)
            }
        }
    }
}

/// Apply cls-int1 (IntersectionOf forward) rule
///
/// cls-int1: type(x, C1) ∧ type(x, C2) ∧ ... → type(x, I)
/// where I is owl:intersectionOf [C1, C2, ...]
///
/// If x has all the member types of an intersection class I, then x is of type I.
pub fn apply_intersection_forward_rule(restrictions: &RestrictionIndex, ctx: &mut RuleContext<'_>) {
    let ref_dt = ref_dt();

    // Get all intersection restriction IDs
    let intersection_ids = restrictions.intersection_restrictions();
    if intersection_ids.is_empty() {
        return;
    }

    // Collect subjects that have new type facts in delta
    let mut subjects_with_new_types: HashSet<Sid> = HashSet::new();
    for flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
        subjects_with_new_types.insert(ctx.same_as.canonical(&flake.s));
    }

    if subjects_with_new_types.is_empty() {
        return;
    }

    // For each intersection restriction
    for intersection_id in intersection_ids {
        if let Some(restriction) = restrictions.get(intersection_id) {
            if let RestrictionType::IntersectionOf { members } = &restriction.restriction_type {
                if members.is_empty() {
                    continue;
                }

                // Check each subject with new types
                for x_canonical in &subjects_with_new_types {
                    // Collect types for this subject
                    let mut x_types: HashSet<Sid> = HashSet::new();
                    for type_flake in ctx.delta.get_by_ps(ctx.rdf_type_sid, x_canonical) {
                        if let FlakeValue::Ref(cls) = &type_flake.o {
                            x_types.insert(cls.clone());
                        }
                    }
                    for type_flake in ctx.derived.get_by_ps(ctx.rdf_type_sid, x_canonical) {
                        if let FlakeValue::Ref(cls) = &type_flake.o {
                            x_types.insert(cls.clone());
                        }
                    }

                    // Check if x satisfies ALL member class expressions (handles nested unions/intersections)
                    let has_all_members = members.iter().all(|member| {
                        entity_satisfies_class_ref(&x_types, member, restrictions, 0)
                    });

                    if has_all_members {
                        let derived_flake = Flake::new(
                            x_canonical.clone(),
                            ctx.rdf_type_sid.clone(),
                            FlakeValue::Ref(intersection_id.clone()),
                            ref_dt.clone(),
                            ctx.t,
                            true,
                            None,
                        );

                        if !ctx.derived.contains(
                            &derived_flake.s,
                            &derived_flake.p,
                            &derived_flake.o,
                        ) {
                            ctx.new_delta.push(derived_flake);
                            ctx.diagnostics.record_rule_fired("cls-int1");
                        }
                    }
                }
            }
        }
    }
}

/// Apply cls-int2 (IntersectionOf backward) rule
///
/// cls-int2: type(x, I) → type(x, C1) ∧ type(x, C2) ∧ ...
/// where I is owl:intersectionOf [C1, C2, ...]
///
/// If x is of intersection type I, then x has all the member types.
pub fn apply_intersection_backward_rule(
    restrictions: &RestrictionIndex,
    ctx: &mut RuleContext<'_>,
) {
    let ref_dt = ref_dt();

    // Process type facts in delta where the type is an intersection restriction
    for flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
        if let FlakeValue::Ref(intersection_class) = &flake.o {
            if let Some(restriction) = restrictions.get(intersection_class) {
                if let RestrictionType::IntersectionOf { members } = &restriction.restriction_type {
                    let x_canonical = ctx.same_as.canonical(&flake.s);

                    // Derive type(x, Ci) for each member class Ci
                    for member in members {
                        let derived_flake = Flake::new(
                            x_canonical.clone(),
                            ctx.rdf_type_sid.clone(),
                            FlakeValue::Ref(member.sid().clone()),
                            ref_dt.clone(),
                            ctx.t,
                            true,
                            None,
                        );

                        if !ctx.derived.contains(
                            &derived_flake.s,
                            &derived_flake.p,
                            &derived_flake.o,
                        ) {
                            ctx.new_delta.push(derived_flake);
                            ctx.diagnostics.record_rule_fired("cls-int2");
                        }
                    }
                }
            }
        }
    }
}

/// Apply cls-uni (UnionOf) rule
///
/// cls-uni: type(x, Ci) → type(x, U)
/// where U is owl:unionOf [C1, C2, ...]
///
/// If x is of any member type Ci, then x is of the union type U.
pub fn apply_union_rule(restrictions: &RestrictionIndex, ctx: &mut RuleContext<'_>) {
    let ref_dt = ref_dt();

    // Get all union restriction IDs
    let union_ids = restrictions.union_restrictions();
    if union_ids.is_empty() {
        return;
    }

    // Collect subjects that have new type facts in delta
    let mut subjects_with_new_types: HashSet<Sid> = HashSet::new();
    for flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
        subjects_with_new_types.insert(ctx.same_as.canonical(&flake.s));
    }

    if subjects_with_new_types.is_empty() {
        return;
    }

    // For each union restriction
    for union_id in union_ids {
        if let Some(restriction) = restrictions.get(union_id) {
            if let RestrictionType::UnionOf { members } = &restriction.restriction_type {
                if members.is_empty() {
                    continue;
                }

                // Check each subject with new types
                for x_canonical in &subjects_with_new_types {
                    // Collect types for this subject
                    let mut x_types: HashSet<Sid> = HashSet::new();
                    for type_flake in ctx.delta.get_by_ps(ctx.rdf_type_sid, x_canonical) {
                        if let FlakeValue::Ref(cls) = &type_flake.o {
                            x_types.insert(cls.clone());
                        }
                    }
                    for type_flake in ctx.derived.get_by_ps(ctx.rdf_type_sid, x_canonical) {
                        if let FlakeValue::Ref(cls) = &type_flake.o {
                            x_types.insert(cls.clone());
                        }
                    }

                    // Check if x satisfies ANY member class expression (handles nested unions/intersections)
                    let satisfies_any_member = members.iter().any(|member| {
                        entity_satisfies_class_ref(&x_types, member, restrictions, 0)
                    });

                    if satisfies_any_member {
                        let derived_flake = Flake::new(
                            x_canonical.clone(),
                            ctx.rdf_type_sid.clone(),
                            FlakeValue::Ref(union_id.clone()),
                            ref_dt.clone(),
                            ctx.t,
                            true,
                            None,
                        );

                        if !ctx.derived.contains(
                            &derived_flake.s,
                            &derived_flake.p,
                            &derived_flake.o,
                        ) {
                            ctx.new_delta.push(derived_flake);
                            ctx.diagnostics.record_rule_fired("cls-uni");
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// OneOf rule (cls-oo)
// ============================================================================

/// Apply cls-oo (OneOf) rule
///
/// cls-oo: For each individual i in owl:oneOf list → type(i, C)
///
/// Each individual in the enumeration is of the oneOf class type.
/// This is typically applied once when restrictions are loaded, but we
/// also check delta for completeness.
pub fn apply_one_of_rule(restrictions: &RestrictionIndex, ctx: &mut RuleContext<'_>) {
    let ref_dt = ref_dt();

    // Get all oneOf restriction IDs
    let one_of_ids = restrictions.one_of_restrictions();
    if one_of_ids.is_empty() {
        return;
    }

    // For each oneOf restriction, derive type facts for all listed individuals
    for one_of_id in one_of_ids {
        if let Some(restriction) = restrictions.get(one_of_id) {
            if let RestrictionType::OneOf { individuals } = &restriction.restriction_type {
                for individual in individuals {
                    let i_canonical = ctx.same_as.canonical(individual);

                    let derived_flake = Flake::new(
                        i_canonical.clone(),
                        ctx.rdf_type_sid.clone(),
                        FlakeValue::Ref(one_of_id.clone()),
                        ref_dt.clone(),
                        ctx.t,
                        true,
                        None,
                    );

                    if !ctx
                        .derived
                        .contains(&derived_flake.s, &derived_flake.p, &derived_flake.o)
                        && !ctx
                            .delta
                            .get_by_ps(ctx.rdf_type_sid, &i_canonical)
                            .any(|f| f.o == FlakeValue::Ref(one_of_id.clone()))
                    {
                        ctx.new_delta.push(derived_flake);
                        ctx.diagnostics.record_rule_fired("cls-oo");
                    }
                }
            }
        }
    }
}
