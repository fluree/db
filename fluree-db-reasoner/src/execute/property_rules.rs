//! Property-related OWL2-RL rules (prp-*).
//!
//! This module implements property rules from the OWL2-RL profile:
//! - `prp-symp` - Symmetric property
//! - `prp-trp` - Transitive property
//! - `prp-inv` - Inverse property
//! - `prp-dom` - Domain inference
//! - `prp-rng` - Range inference
//! - `prp-spo1` - SubPropertyOf
//! - `prp-spo2` - PropertyChainAxiom
//! - `prp-fp` - Functional property
//! - `prp-ifp` - Inverse functional property
//! - `prp-key` - HasKey

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;
use hashbrown::{HashMap, HashSet};

use crate::ontology_rl::OntologyRL;
use crate::types::PropertyChain;

use super::util::{ref_dt, IdentityRuleContext, RuleContext};

/// Apply symmetric property rule: P(x,y) → P(y,x)
///
/// For each new fact δP(x,y) where P is symmetric, derive P(y,x).
pub fn apply_symmetric_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    for p in ontology.symmetric_properties() {
        for flake in ctx.delta.get_by_p(p) {
            // P(x, y) → P(y, x)
            if let FlakeValue::Ref(y) = &flake.o {
                let x = &flake.s;

                // Create inverse flake P(y, x)
                let inverse = Flake::new(
                    y.clone(),
                    p.clone(),
                    FlakeValue::Ref(x.clone()),
                    flake.dt.clone(),
                    ctx.t,
                    true,
                    None,
                );

                // Only add if not already derived
                if !ctx.derived.contains(&inverse.s, &inverse.p, &inverse.o) {
                    ctx.new_delta.push(inverse);
                    ctx.diagnostics.record_rule_fired("prp-symp");
                }
            }
        }
    }
}

/// Apply transitive property rule: P(x,y), P(y,z) → P(x,z)
///
/// For each new fact δP(x,y) where P is transitive:
/// - Join with existing P(y,z) to derive P(x,z)
/// - Join with existing P(w,x) to derive P(w,y)
pub fn apply_transitive_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    for p in ontology.transitive_properties() {
        for flake in ctx.delta.get_by_p(p) {
            if let FlakeValue::Ref(y) = &flake.o {
                let x = &flake.s;

                // Forward: δP(x,y) ⋈ P(y,z) → P(x,z)
                // Look up all P(y, ?) in derived
                for existing in ctx.derived.get_by_ps(p, y) {
                    if let FlakeValue::Ref(z) = &existing.o {
                        // Don't create self-loops (x = z)
                        if x != z {
                            let new_flake = Flake::new(
                                x.clone(),
                                p.clone(),
                                FlakeValue::Ref(z.clone()),
                                flake.dt.clone(),
                                ctx.t,
                                true,
                                None,
                            );

                            if !ctx
                                .derived
                                .contains(&new_flake.s, &new_flake.p, &new_flake.o)
                            {
                                ctx.new_delta.push(new_flake);
                                ctx.diagnostics.record_rule_fired("prp-trp");
                            }
                        }
                    }
                }

                // Also check delta for P(y, ?) (new facts joining with new facts)
                for delta_flake in ctx.delta.get_by_ps(p, y) {
                    if let FlakeValue::Ref(z) = &delta_flake.o {
                        if x != z {
                            let new_flake = Flake::new(
                                x.clone(),
                                p.clone(),
                                FlakeValue::Ref(z.clone()),
                                flake.dt.clone(),
                                ctx.t,
                                true,
                                None,
                            );

                            if !ctx
                                .derived
                                .contains(&new_flake.s, &new_flake.p, &new_flake.o)
                            {
                                ctx.new_delta.push(new_flake);
                                ctx.diagnostics.record_rule_fired("prp-trp");
                            }
                        }
                    }
                }

                // Backward: P(w,x) ⋈ δP(x,y) → P(w,y)
                // Look up all P(?, x) in derived (use by_po index)
                for existing in ctx.derived.get_by_po(p, x) {
                    let w = &existing.s;
                    // Don't create self-loops (w = y)
                    if w != y {
                        let new_flake = Flake::new(
                            w.clone(),
                            p.clone(),
                            FlakeValue::Ref(y.clone()),
                            flake.dt.clone(),
                            ctx.t,
                            true,
                            None,
                        );

                        if !ctx
                            .derived
                            .contains(&new_flake.s, &new_flake.p, &new_flake.o)
                        {
                            ctx.new_delta.push(new_flake);
                            ctx.diagnostics.record_rule_fired("prp-trp");
                        }
                    }
                }
            }
        }
    }
}

/// Apply inverse property rule: P(x,y) → P_inv(y,x)
///
/// For each new fact δP(x,y) where P has inverses, derive P_inv(y,x).
pub fn apply_inverse_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    for flake in ctx.delta.iter() {
        let inverses = ontology.inverses_of(&flake.p);
        if inverses.is_empty() {
            continue;
        }

        if let FlakeValue::Ref(y) = &flake.o {
            let x = &flake.s;

            for p_inv in inverses {
                // P(x, y) → P_inv(y, x)
                let inverse = Flake::new(
                    y.clone(),
                    p_inv.clone(),
                    FlakeValue::Ref(x.clone()),
                    flake.dt.clone(),
                    ctx.t,
                    true,
                    None,
                );

                if !ctx.derived.contains(&inverse.s, &inverse.p, &inverse.o) {
                    ctx.new_delta.push(inverse);
                    ctx.diagnostics.record_rule_fired("prp-inv");
                }
            }
        }
    }
}

/// Apply domain rule (prp-dom): P(x, y), domain(P, C) → type(x, C)
///
/// For each new fact δP(x,y) where P has domain C, derive rdf:type(x, C).
/// The subject x is inferred to be of type C because it appears in the
/// subject position of property P which has domain C.
///
/// Note: sameAs canonicalization should be applied before calling this
/// to ensure x is the canonical representative.
pub fn apply_domain_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    for p in ontology.properties_with_domain() {
        let domain_classes = ontology.domain_of(p);
        if domain_classes.is_empty() {
            continue;
        }

        for flake in ctx.delta.get_by_p(p) {
            // Canonicalize subject before deriving type
            let canonical_s = ctx.same_as.canonical(&flake.s);

            for c in domain_classes {
                // Derive rdf:type(x, C)
                super::util::try_derive_type(ctx, &canonical_s, c, "prp-dom");
            }
        }
    }
}

/// Apply range rule (prp-rng): P(x, y), range(P, C) → type(y, C)
///
/// For each new fact δP(x,y) where P has range C and y is a Ref,
/// derive rdf:type(y, C).
/// The object y is inferred to be of type C because it appears in the
/// object position of property P which has range C.
///
/// Note: sameAs canonicalization should be applied before calling this
/// to ensure y is the canonical representative.
/// Only Ref objects are typed (literals don't get rdf:type assertions).
pub fn apply_range_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    for p in ontology.properties_with_range() {
        let range_classes = ontology.range_of(p);
        if range_classes.is_empty() {
            continue;
        }

        for flake in ctx.delta.get_by_p(p) {
            // Only derive type for Ref objects (not literals)
            if let FlakeValue::Ref(y) = &flake.o {
                // Canonicalize object before deriving type
                let canonical_y = ctx.same_as.canonical(y);

                for c in range_classes {
                    super::util::try_derive_type(ctx, &canonical_y, c, "prp-rng");
                }
            }
        }
    }
}

/// Apply subPropertyOf rule (prp-spo1): P1(x, y), subPropertyOf(P1, P2) → P2(x, y)
///
/// For each new fact δP1(x,y) where P1 is a subproperty of P2,
/// derive P2(x, y).
///
/// Note: sameAs canonicalization should be applied before calling this
/// to ensure consistent subject/object values.
pub fn apply_sub_property_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    for p1 in ontology.properties_with_super_properties() {
        let super_props = ontology.super_properties_of(p1);
        if super_props.is_empty() {
            continue;
        }

        for flake in ctx.delta.get_by_p(p1) {
            // Canonicalize subject
            let canonical_s = ctx.same_as.canonical(&flake.s);

            // Canonicalize object if it's a Ref
            let canonical_o = match &flake.o {
                FlakeValue::Ref(o) => FlakeValue::Ref(ctx.same_as.canonical(o)),
                other => other.clone(),
            };

            for p2 in super_props {
                // Derive P2(x, y)
                let derived_flake = Flake::new(
                    canonical_s.clone(),
                    p2.clone(),
                    canonical_o.clone(),
                    flake.dt.clone(),
                    ctx.t,
                    true,
                    None,
                );

                if !ctx
                    .derived
                    .contains(&derived_flake.s, &derived_flake.p, &derived_flake.o)
                {
                    ctx.new_delta.push(derived_flake);
                    ctx.diagnostics.record_rule_fired("prp-spo1");
                }
            }
        }
    }
}

/// Apply property chain rule (prp-spo2) for n-length chains with inverse support.
///
/// For each property chain P = E1 o E2 o ... o En:
/// - Find all matching paths through the delta ∪ derived sets
/// - For each complete path, derive P(start, end)
///
/// Supports:
/// - Chains of arbitrary length (≥2)
/// - Inverse elements: `_:b owl:inverseOf P` means traverse P backwards
///
/// # Algorithm
///
/// For a chain [E1, E2, ..., En], we seed from each element position in delta,
/// then extend bidirectionally using delta ∪ derived.
///
/// For a direct element Ei at position i: Ei(ui, ui+1)
/// For an inverse element Ei^-1: means Ei(ui+1, ui) - traverse backwards
pub fn apply_property_chain_rule(ontology: &OntologyRL, ctx: &mut RuleContext<'_>) {
    for chain in ontology.property_chains() {
        apply_single_property_chain(chain, ctx);
    }
}

/// Apply a single property chain rule.
fn apply_single_property_chain(chain: &PropertyChain, ctx: &mut RuleContext<'_>) {
    let chain_len = chain.len();
    if chain_len < 2 {
        return;
    }

    // For each position in the chain, seed from delta facts matching that element
    for seed_pos in 0..chain_len {
        let element = &chain.chain[seed_pos];

        // Get all facts from delta matching this element's property
        for seed_flake in ctx.delta.get_by_p(&element.property) {
            // Determine the binding based on whether element is inverse
            let (binding_in, binding_out) = if element.is_inverse {
                // Inverse: P(y, x) means x flows in, y flows out
                // For P^-1(u_i, u_{i+1}), we need P(u_{i+1}, u_i)
                match &seed_flake.o {
                    FlakeValue::Ref(obj) => (
                        ctx.same_as.canonical(obj),
                        ctx.same_as.canonical(&seed_flake.s),
                    ),
                    _ => continue, // Non-ref objects can't be part of chain
                }
            } else {
                // Direct: P(x, y) means x flows in, y flows out
                match &seed_flake.o {
                    FlakeValue::Ref(obj) => (
                        ctx.same_as.canonical(&seed_flake.s),
                        ctx.same_as.canonical(obj),
                    ),
                    _ => continue, // Non-ref objects can't be part of chain
                }
            };

            // Now extend backwards to position 0 and forwards to position n-1
            // Collect all possible paths

            // First, extend backwards to find all possible start nodes
            let start_nodes = extend_chain_backwards(chain, seed_pos, &binding_in, ctx);

            if start_nodes.is_empty() {
                continue;
            }

            // Then, extend forwards to find all possible end nodes
            let end_nodes = extend_chain_forwards(chain, seed_pos, &binding_out, ctx);

            if end_nodes.is_empty() {
                continue;
            }

            // For each combination of start/end, derive the chain fact
            for start in &start_nodes {
                for end in &end_nodes {
                    derive_chain_fact(&chain.derived_property, start, end, ctx);
                }
            }
        }
    }
}

/// Extend a chain backwards from position seed_pos to position 0.
///
/// Returns all possible start nodes (u0 values).
fn extend_chain_backwards(
    chain: &PropertyChain,
    seed_pos: usize,
    current_binding: &Sid,
    ctx: &RuleContext<'_>,
) -> Vec<Sid> {
    if seed_pos == 0 {
        // Already at start - current binding IS the start
        return vec![current_binding.clone()];
    }

    // Work backwards from seed_pos - 1 to 0
    let mut current_nodes = vec![current_binding.clone()];

    for pos in (0..seed_pos).rev() {
        let element = &chain.chain[pos];
        let mut next_nodes = Vec::new();

        for node in &current_nodes {
            // Find all facts that could produce this binding
            // For direct element E(u_pos, u_{pos+1}): we have u_{pos+1} (object), need u_pos (subject)
            // For inverse element E^-1(u_pos, u_{pos+1}) ≡ E(u_{pos+1}, u_pos):
            //   we have u_{pos+1} (subject), need u_pos (object)
            let candidates = if element.is_inverse {
                // Need objects for subject = u_{pos+1}
                get_all_objects_for_subject(ctx, &element.property, node)
            } else {
                // Need subjects for object = u_{pos+1}
                get_all_subjects_for_object(ctx, &element.property, node)
            };

            next_nodes.extend(candidates);
        }

        if next_nodes.is_empty() {
            return Vec::new();
        }

        current_nodes = next_nodes;
    }

    current_nodes
}

/// Extend a chain forwards from position seed_pos to position n-1.
///
/// Returns all possible end nodes (u_n values).
fn extend_chain_forwards(
    chain: &PropertyChain,
    seed_pos: usize,
    current_binding: &Sid,
    ctx: &RuleContext<'_>,
) -> Vec<Sid> {
    let chain_len = chain.len();

    if seed_pos == chain_len - 1 {
        // Already at end - current binding IS the end
        return vec![current_binding.clone()];
    }

    // Work forwards from seed_pos + 1 to chain_len - 1
    let mut current_nodes = vec![current_binding.clone()];

    for pos in (seed_pos + 1)..chain_len {
        let element = &chain.chain[pos];
        let mut next_nodes = Vec::new();

        for node in &current_nodes {
            // Find all facts that could produce the next binding
            // For direct element E(u_pos, u_{pos+1}): we have u_pos (subject), need u_{pos+1} (object)
            // For inverse element E^-1(u_pos, u_{pos+1}) ≡ E(u_{pos+1}, u_pos):
            //   we have u_pos (object), need u_{pos+1} (subject)
            let candidates = if element.is_inverse {
                // Need subjects for object = u_pos
                get_all_subjects_for_object(ctx, &element.property, node)
            } else {
                // Direct: E(u_pos, u_{pos+1})
                // We have u_pos (subject of E), need u_{pos+1} (object of E)
                get_all_objects_for_subject(ctx, &element.property, node)
            };

            next_nodes.extend(candidates);
        }

        if next_nodes.is_empty() {
            return Vec::new();
        }

        current_nodes = next_nodes;
    }

    current_nodes
}

/// Get all subjects for facts with (predicate, object) in delta ∪ derived.
fn get_all_subjects_for_object(ctx: &RuleContext<'_>, predicate: &Sid, object: &Sid) -> Vec<Sid> {
    let mut subjects = Vec::new();

    // From delta
    for flake in ctx.delta.get_by_po(predicate, object) {
        subjects.push(ctx.same_as.canonical(&flake.s));
    }

    // From derived
    for flake in ctx.derived.get_by_po(predicate, object) {
        subjects.push(ctx.same_as.canonical(&flake.s));
    }

    subjects
}

/// Get all objects for facts with (predicate, subject) in delta ∪ derived.
fn get_all_objects_for_subject(ctx: &RuleContext<'_>, predicate: &Sid, subject: &Sid) -> Vec<Sid> {
    let mut objects = Vec::new();

    // From delta
    for flake in ctx.delta.get_by_ps(predicate, subject) {
        if let FlakeValue::Ref(obj) = &flake.o {
            objects.push(ctx.same_as.canonical(obj));
        }
    }

    // From derived
    for flake in ctx.derived.get_by_ps(predicate, subject) {
        if let FlakeValue::Ref(obj) = &flake.o {
            objects.push(ctx.same_as.canonical(obj));
        }
    }

    objects
}

/// Helper to derive a fact from property chain rule.
fn derive_chain_fact(derived_prop: &Sid, start: &Sid, end: &Sid, ctx: &mut RuleContext<'_>) {
    let canonical_start = ctx.same_as.canonical(start);
    let canonical_end = FlakeValue::Ref(ctx.same_as.canonical(end));

    // Check if already in derived
    if ctx
        .derived
        .contains(&canonical_start, derived_prop, &canonical_end)
    {
        return;
    }

    // Check if already in new_delta (we might seed from multiple positions)
    for existing in ctx.new_delta.get_by_ps(derived_prop, &canonical_start) {
        if existing.o == canonical_end {
            return;
        }
    }

    // Derive P(start, end)
    let chain_flake = Flake::new(
        canonical_start,
        derived_prop.clone(),
        canonical_end,
        ref_dt(),
        ctx.t,
        true,
        None,
    );

    ctx.new_delta.push(chain_flake);
    ctx.diagnostics.record_rule_fired("prp-spo2");
}

/// Apply functional property rule (prp-fp):
/// FunctionalProperty(P), P(x, y1), P(x, y2) → sameAs(y1, y2)
///
/// For each functional property P, find facts with the same subject but different
/// objects. Those objects must be owl:sameAs.
///
/// This rule produces identity (owl:sameAs) and should be applied early in the
/// fixpoint loop, before rules that depend on canonicalized subjects/objects.
///
/// # Arguments
///
/// * `ctx.same_as_changed` - If true, re-evaluate even without new P facts in delta.
///   This is needed because sameAs merges can create new conflicts:
///   e.g., if sameAs(s1, s2) arrives and derived has P(s1, y1) and P(s2, y2),
///   after canonicalization they have the same subject, triggering fp inference.
pub fn apply_functional_property_rule(ontology: &OntologyRL, ctx: &mut IdentityRuleContext<'_>) {
    let ref_dt = ref_dt();

    for p in ontology.functional_properties() {
        // Process if there are new facts in delta OR if sameAs changed
        // (sameAs changes can create new conflicts by merging subjects)
        let delta_has_p = ctx.delta.get_by_p(p).next().is_some();
        let derived_has_p = ctx.derived.get_by_p(p).next().is_some();
        if !(delta_has_p || ctx.same_as_changed && derived_has_p) {
            continue;
        }

        // Collect all (canonical_subject, canonical_object) pairs for this property
        // Group by subject to find conflicts
        let mut objects_by_subject: HashMap<Sid, Vec<Sid>> = HashMap::new();

        // From delta
        for flake in ctx.delta.get_by_p(p) {
            if let FlakeValue::Ref(y) = &flake.o {
                let x_canonical = ctx.same_as.canonical(&flake.s);
                let y_canonical = ctx.same_as.canonical(y);
                objects_by_subject
                    .entry(x_canonical)
                    .or_default()
                    .push(y_canonical);
            }
        }

        // From derived (need to check for conflicts with delta facts)
        for flake in ctx.derived.get_by_p(p) {
            if let FlakeValue::Ref(y) = &flake.o {
                let x_canonical = ctx.same_as.canonical(&flake.s);
                let y_canonical = ctx.same_as.canonical(y);
                objects_by_subject
                    .entry(x_canonical)
                    .or_default()
                    .push(y_canonical);
            }
        }

        // For each subject with multiple distinct objects, derive sameAs
        for (_x, objects) in objects_by_subject {
            // Deduplicate objects (same canonical form = already known sameAs)
            let unique_objects: HashSet<Sid> = objects.into_iter().collect();
            if unique_objects.len() <= 1 {
                continue;
            }

            // Derive sameAs by chaining to first element: O(k-1) instead of O(k²)
            // Union-find will compute full transitive closure internally
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
                    ctx.diagnostics.record_rule_fired("prp-fp");
                }
            }
        }
    }
}

/// Apply inverse functional property rule (prp-ifp):
/// InverseFunctionalProperty(P), P(x1, y), P(x2, y) → sameAs(x1, x2)
///
/// For each inverse-functional property P, find facts with the same object but
/// different subjects. Those subjects must be owl:sameAs.
///
/// This rule produces identity (owl:sameAs) and should be applied early in the
/// fixpoint loop, before rules that depend on canonicalized subjects/objects.
///
/// # Arguments
///
/// * `ctx.same_as_changed` - If true, re-evaluate even without new P facts in delta.
///   This is needed because sameAs merges can create new conflicts:
///   e.g., if sameAs(y1, y2) arrives and derived has P(x1, y1) and P(x2, y2),
///   after canonicalization they have the same object, triggering ifp inference.
pub fn apply_inverse_functional_property_rule(
    ontology: &OntologyRL,
    ctx: &mut IdentityRuleContext<'_>,
) {
    let ref_dt = ref_dt();

    for p in ontology.inverse_functional_properties() {
        // Process if there are new facts in delta OR if sameAs changed
        // (sameAs changes can create new conflicts by merging objects)
        let delta_has_p = ctx.delta.get_by_p(p).next().is_some();
        let derived_has_p = ctx.derived.get_by_p(p).next().is_some();
        if !(delta_has_p || ctx.same_as_changed && derived_has_p) {
            continue;
        }

        // Collect all (canonical_object, canonical_subject) pairs
        // Group by object to find conflicting subjects
        let mut subjects_by_object: HashMap<Sid, Vec<Sid>> = HashMap::new();

        // From delta
        for flake in ctx.delta.get_by_p(p) {
            if let FlakeValue::Ref(y) = &flake.o {
                let x_canonical = ctx.same_as.canonical(&flake.s);
                let y_canonical = ctx.same_as.canonical(y);
                subjects_by_object
                    .entry(y_canonical)
                    .or_default()
                    .push(x_canonical);
            }
        }

        // From derived
        for flake in ctx.derived.get_by_p(p) {
            if let FlakeValue::Ref(y) = &flake.o {
                let x_canonical = ctx.same_as.canonical(&flake.s);
                let y_canonical = ctx.same_as.canonical(y);
                subjects_by_object
                    .entry(y_canonical)
                    .or_default()
                    .push(x_canonical);
            }
        }

        // For each object with multiple distinct subjects, derive sameAs
        for (_y, subjects) in subjects_by_object {
            // Deduplicate subjects
            let unique_subjects: HashSet<Sid> = subjects.into_iter().collect();
            if unique_subjects.len() <= 1 {
                continue;
            }

            // Derive sameAs by chaining to first element: O(k-1) instead of O(k²)
            // Union-find will compute full transitive closure internally
            let subjects_vec: Vec<Sid> = unique_subjects.into_iter().collect();
            let first = &subjects_vec[0];
            for other in &subjects_vec[1..] {
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
                    ctx.diagnostics.record_rule_fired("prp-ifp");
                }
            }
        }
    }
}

/// Apply hasKey rule (prp-key):
/// hasKey(C, [P1..Pn]), type(x, C), P1(x, z1), ..., Pn(x, zn),
/// type(y, C), P1(y, z1), ..., Pn(y, zn) → sameAs(x, y)
///
/// For each class C with a hasKey declaration, find instances with matching
/// key property values and derive owl:sameAs between them.
///
/// RESTRICTED FORM (per OWL2-RL parity plan):
/// - Only IRI/Ref key values (literals are skipped)
/// - All key properties must be present (instances missing any key property are skipped)
///
/// This rule produces identity (owl:sameAs) and should be applied early in the
/// fixpoint loop, before rules that depend on canonicalized subjects/objects.
///
/// # Arguments
///
/// * `ctx.same_as_changed` - If true, re-evaluate even without new relevant facts in delta.
///   This is needed because sameAs merges can create new key matches after canonicalization.
pub fn apply_has_key_rule(ontology: &OntologyRL, ctx: &mut IdentityRuleContext<'_>) {
    let ref_dt = ref_dt();

    // For each class with hasKey declarations
    for class in ontology.classes_with_keys() {
        for key_properties in ontology.has_keys_of(class) {
            if key_properties.is_empty() {
                continue;
            }

            // Check if we need to process this class/key combination
            // Process if:
            // 1. Delta has new type(?, class) facts, OR
            // 2. Delta has new key property facts (P_i(?, ?)), OR
            // 3. same_as_changed AND we have existing relevant facts
            let delta_has_new_type = ctx
                .delta
                .get_by_p(ctx.rdf_type_sid)
                .any(|f| matches!(&f.o, FlakeValue::Ref(c) if ctx.same_as.canonical(c) == ctx.same_as.canonical(class)));

            let delta_has_key_property = key_properties
                .iter()
                .any(|p| ctx.delta.get_by_p(p).next().is_some());

            let derived_has_relevant = ctx.derived.get_by_p(ctx.rdf_type_sid).next().is_some()
                || key_properties
                    .iter()
                    .any(|p| ctx.derived.get_by_p(p).next().is_some());

            if !(delta_has_new_type
                || delta_has_key_property
                || ctx.same_as_changed && derived_has_relevant)
            {
                continue;
            }

            // Collect all instances of this class from delta and derived
            // An instance is any subject x where type(x, C) exists
            let canonical_class = ctx.same_as.canonical(class);
            let mut instances: HashSet<Sid> = HashSet::new();

            // From delta: type(x, C)
            for flake in ctx.delta.get_by_p(ctx.rdf_type_sid) {
                if let FlakeValue::Ref(c) = &flake.o {
                    if ctx.same_as.canonical(c) == canonical_class {
                        instances.insert(ctx.same_as.canonical(&flake.s));
                    }
                }
            }

            // From derived: type(x, C)
            for flake in ctx.derived.get_by_p(ctx.rdf_type_sid) {
                if let FlakeValue::Ref(c) = &flake.o {
                    if ctx.same_as.canonical(c) == canonical_class {
                        instances.insert(ctx.same_as.canonical(&flake.s));
                    }
                }
            }

            if instances.len() < 2 {
                // Need at least 2 instances to find duplicates
                continue;
            }

            // For each instance, collect key values
            // Key: (canonical key values tuple) -> Vec<canonical instance>
            // We use a sorted Vec<(Sid, Sid)> as the key tuple: [(P1, V1), (P2, V2), ...]
            let mut instances_by_key: HashMap<Vec<(Sid, Sid)>, Vec<Sid>> = HashMap::new();

            for instance in &instances {
                let mut key_values: Vec<(Sid, Sid)> = Vec::with_capacity(key_properties.len());
                let mut valid = true;

                for key_prop in key_properties {
                    // Collect ALL distinct canonical Ref values for this key property
                    // from both delta and derived. If 0 or >1 distinct values, skip instance.
                    let mut canonical_values: HashSet<Sid> = HashSet::new();

                    // From delta
                    for flake in ctx.delta.get_by_ps(key_prop, instance) {
                        if let FlakeValue::Ref(v) = &flake.o {
                            canonical_values.insert(ctx.same_as.canonical(v));
                        }
                        // Skip literals - restricted form only supports Ref values
                    }

                    // From derived
                    for flake in ctx.derived.get_by_ps(key_prop, instance) {
                        if let FlakeValue::Ref(v) = &flake.o {
                            canonical_values.insert(ctx.same_as.canonical(v));
                        }
                    }

                    // RESTRICTED FORM: require exactly 1 distinct canonical value
                    // - 0 values: missing key property, skip instance
                    // - >1 values: multi-valued key property, skip instance (ambiguous)
                    if canonical_values.len() != 1 {
                        valid = false;
                        break;
                    }

                    // Safe: we know there's exactly 1 value
                    let value = canonical_values.into_iter().next().unwrap();
                    key_values.push((key_prop.clone(), value));
                }

                if valid {
                    // Sort key values by property to ensure consistent key tuple
                    key_values.sort_by(|a, b| a.0.cmp(&b.0));
                    instances_by_key
                        .entry(key_values)
                        .or_default()
                        .push(instance.clone());
                }
            }

            // For each key tuple with multiple instances, derive sameAs
            for (_key_tuple, matching_instances) in instances_by_key {
                if matching_instances.len() <= 1 {
                    continue;
                }

                // Derive sameAs by chaining to first element: O(k-1) instead of O(k²)
                let first = &matching_instances[0];
                for other in &matching_instances[1..] {
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
                        ctx.diagnostics.record_rule_fired("prp-key");
                    }
                }
            }
        }
    }
}
