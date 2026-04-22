//! Shared utility functions for rule execution.
//!
//! This module provides helper functions used across multiple rule implementations:
//! - `RuleContext` - Shared context for rule execution
//! - `ref_dt()` - Default datatype SID for Ref values
//! - `rdf_type_sid()` - SID for the rdf:type predicate
//! - `canonicalize_flake()` - Apply sameAs canonicalization to a flake
//! - `try_derive_type()` - Derive a type fact with deduplication

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;
use fluree_vocab::jsonld_names::ID as JSONLD_ID;
use fluree_vocab::namespaces::{JSON_LD, RDF};
use fluree_vocab::predicates::RDF_TYPE;

use super::delta::DeltaSet;
use super::derived::DerivedSet;
use crate::same_as::SameAsTracker;
use crate::ReasoningDiagnostics;

/// Shared context for rule execution.
///
/// Bundles common parameters passed to all reasoning rules, reducing
/// function signatures from 8-10 parameters to 2 (ontology/restrictions + context).
///
/// # Fields
///
/// * `delta` - New facts from the current iteration to process
/// * `derived` - Accumulated derived facts for deduplication checks
/// * `new_delta` - Output buffer for newly derived facts
/// * `same_as` - SameAs equivalence tracker for canonicalization
/// * `rdf_type_sid` - SID for the rdf:type predicate
/// * `t` - Transaction time for derived flakes
/// * `diagnostics` - Diagnostic counters for rule firings
pub struct RuleContext<'a> {
    pub delta: &'a DeltaSet,
    pub derived: &'a DerivedSet,
    pub new_delta: &'a mut DeltaSet,
    pub same_as: &'a SameAsTracker,
    pub rdf_type_sid: &'a Sid,
    pub t: i64,
    pub diagnostics: &'a mut ReasoningDiagnostics,
}

/// Extended context for identity-producing rules (prp-fp, prp-ifp, prp-key, cls-maxc, cls-maxqc).
///
/// These rules derive owl:sameAs facts and need additional parameters:
/// * `owl_same_as_sid` - SID for the owl:sameAs predicate
/// * `same_as_changed` - Whether sameAs equivalences changed this iteration
pub struct IdentityRuleContext<'a> {
    pub delta: &'a DeltaSet,
    pub derived: &'a DerivedSet,
    pub new_delta: &'a mut DeltaSet,
    pub same_as: &'a SameAsTracker,
    pub owl_same_as_sid: &'a Sid,
    pub rdf_type_sid: &'a Sid,
    pub t: i64,
    pub same_as_changed: bool,
    pub diagnostics: &'a mut ReasoningDiagnostics,
}

/// Default datatype SID for derived Ref values.
///
/// When deriving new flakes with Ref objects (e.g., rdf:type assertions),
/// the datatype should be `$id` (JSON_LD namespace, "id" local name).
pub fn ref_dt() -> Sid {
    Sid::new(JSON_LD, JSONLD_ID)
}

/// SID for the rdf:type predicate.
///
/// This is used extensively in reasoning rules to derive type assertions.
pub fn rdf_type_sid() -> Sid {
    Sid::new(RDF, RDF_TYPE)
}

/// Canonicalize a flake's subject and object positions using sameAs equivalence
///
/// This implements eq-rep-s (canonicalize subject) and eq-rep-o (canonicalize object).
/// Returns a new flake with S and O replaced by their canonical representatives.
pub fn canonicalize_flake(flake: &Flake, same_as: &SameAsTracker) -> Flake {
    let canonical_s = same_as.canonical(&flake.s);

    let canonical_o = match &flake.o {
        FlakeValue::Ref(o_sid) => FlakeValue::Ref(same_as.canonical(o_sid)),
        other => other.clone(),
    };

    // Only create a new flake if something changed
    if canonical_s == flake.s && canonical_o == flake.o {
        flake.clone()
    } else {
        Flake::new(
            canonical_s,
            flake.p.clone(),
            canonical_o,
            flake.dt.clone(),
            flake.t,
            flake.op,
            flake.m.clone(),
        )
    }
}

/// Attempt to derive a type fact `rdf:type(subject, type_class)`.
///
/// Creates the flake and adds it to `ctx.new_delta` if not already in `ctx.derived`.
/// Returns `true` if the fact was new and added, `false` if it already existed.
///
/// This helper reduces boilerplate in rule implementations by encapsulating
/// the common pattern of creating a type flake, checking for duplicates,
/// and recording diagnostics.
pub fn try_derive_type(
    ctx: &mut RuleContext<'_>,
    subject: &Sid,
    type_class: &Sid,
    rule_name: &str,
) -> bool {
    let flake = Flake::new(
        subject.clone(),
        ctx.rdf_type_sid.clone(),
        FlakeValue::Ref(type_class.clone()),
        ref_dt(),
        ctx.t,
        true,
        None,
    );

    if !ctx.derived.contains(&flake.s, &flake.p, &flake.o) {
        ctx.new_delta.push(flake);
        ctx.diagnostics.record_rule_fired(rule_name);
        true
    } else {
        false
    }
}
