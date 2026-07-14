//! Policy building from query connection options
//!
//! This module provides functions to build `PolicyContext` from query connection options:
//! - Identity-based policies (via `f:policyClass` on identity subject)
//! - Class-based policies (policies of given types/classes)
//! - Inline policy JSON-LD
//!
//! # Compatibility notes
//!
//! This module preserves the legacy policy-wrapping behavior:
//! - Load policies via an identity's `f:policyClass`
//! - Load policies of given classes
//! - Parse inline policy JSON-LD

use crate::dataset::GovernanceOptions;
use crate::error::{ApiError, Result};
use async_trait::async_trait;
use fluree_db_core::IndexStats;
use fluree_db_core::{FlakeValue, GraphDbRef, IndexType, LedgerSnapshot, Sid};
use fluree_db_core::{RangeMatch, RangeOptions, RangeTest};
use fluree_db_novelty::{Novelty, StatsAssemblyError, StatsLookup};
use fluree_db_policy::{
    build_policy_set, ConditionState, PolicyAction, PolicyContext, PolicyQuery,
    PolicyQueryLanguage, PolicyRestriction, PolicyValue, PolicyWrapper, TargetMode, WriteVerbs,
};
use fluree_db_query::{execute_pattern, Binding, Ref, Term, TriplePattern, VarRegistry};
use fluree_vocab::rdf::TYPE as RDF_TYPE_IRI;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Default policy graph set: only the default graph (g_id = 0).
const DEFAULT_POLICY_GRAPHS: [fluree_db_core::GraphId; 1] = [0];

// ============================================================================
// Constants - Fluree policy vocabulary IRIs (from fluree-vocab)
// ============================================================================

use fluree_vocab::{config_iris, fluree, policy_iris};

// ============================================================================
// Public API
// ============================================================================

/// Resolve a `GraphSourceRef` from config into concrete graph IDs for policy loading.
///
/// Returns `Err` if the source specifies unsupported features (`at_t`, `trust_policy`,
/// `rollback_guard`, cross-ledger `ledger`). Returns the default graph `[0]` when
/// `source` is `None`.
pub fn resolve_policy_source_g_ids(
    source: Option<&fluree_db_core::ledger_config::GraphSourceRef>,
    snapshot: &LedgerSnapshot,
) -> Result<Vec<fluree_db_core::GraphId>> {
    let source = match source {
        None => return Ok(DEFAULT_POLICY_GRAPHS.to_vec()),
        Some(s) => s,
    };

    if source.ledger.is_some() {
        return Err(ApiError::query(
            "f:policySource with a cross-ledger f:ledger reference is not yet supported",
        ));
    }
    if source.at_t.is_some() {
        return Err(ApiError::query(
            "f:policySource with f:atT (temporal pinning) is not yet supported",
        ));
    }
    if source.trust_policy.is_some() {
        return Err(ApiError::query(
            "f:policySource with f:trustPolicy is not yet supported",
        ));
    }
    if source.rollback_guard.is_some() {
        return Err(ApiError::query(
            "f:policySource with f:rollbackGuard is not yet supported",
        ));
    }

    let g_id = match source.graph_selector.as_deref() {
        Some(iri) if iri == config_iris::DEFAULT_GRAPH => Some(0u16),
        Some(iri) => snapshot.graph_registry.graph_id_for_iri(iri),
        None => Some(0u16),
    };

    match g_id {
        Some(id) => Ok(vec![id]),
        None => Err(ApiError::query(format!(
            "f:policySource graph '{}' not found in this ledger's graph registry",
            source.graph_selector.as_deref().unwrap_or("<none>"),
        ))),
    }
}

/// Build a `PolicyContext` from `GovernanceOptions`.
///
/// Handles the three policy modes:
/// 1. **identity**: Query for policies via the identity's `f:policyClass` property
/// 2. **policy_class**: Query for policies of the given class types
/// 3. **policy**: Parse inline policy JSON-LD
///
/// Priority: (identity + policy_class: classes select, identity binds) >
/// identity > policy_class > policy
///
/// # Arguments
///
/// * `snapshot` - The database snapshot to query against
/// * `overlay` - Overlay provider for query execution
/// * `novelty_for_stats` - Optional novelty for computing current stats (needed for f:onClass)
/// * `to_t` - Time bound for queries
/// * `opts` - Query connection options with policy configuration
/// * `policy_graphs` - Which graphs to scan for policy triples (resolved from config)
pub async fn build_policy_context_from_opts(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    novelty_for_stats: Option<&Novelty>,
    to_t: i64,
    opts: &GovernanceOptions,
    policy_graphs: &[fluree_db_core::GraphId],
) -> Result<PolicyContext> {
    build_policy_context_from_opts_inner(
        snapshot,
        overlay,
        novelty_for_stats,
        to_t,
        opts,
        policy_graphs,
        None,
        None,
    )
    .await
}

/// Cross-ledger variant of [`build_policy_context_from_opts`].
///
/// `cross_ledger_restrictions` is a pre-materialized list produced
/// against a model ledger by the cross-ledger resolver and
/// translated into D's term space via
/// `fluree_db_policy::wire_to_restrictions` (with the policy-class
/// filter chain already applied). When supplied, the local
/// same-ledger policy load (`load_policies_by_identity` /
/// `load_policies_by_class` / `parse_inline_policy`) is bypassed
/// for rule selection — those restrictions are used as-is, plus any
/// inline `opts.policy` merge.
///
/// Identity contract: `opts.identity` is **bind-only** under
/// cross-ledger. It resolves against D to populate `?$identity` for
/// f:query rules; it never selects rules (same-ledger identity-mode
/// consults the identity's D-local `f:policyClass` triples — those
/// are intentionally ignored here because a cross-ledger
/// `f:policySource` declares M the policy authority).
///
/// `policy_graphs` is still consulted for the identity binding's
/// subject-existence check because identity binding always resolves
/// against the data ledger; cross-ledger never contributes identity
/// records.
/// [`build_policy_context_from_opts`] plus a pre-resolved cross-ledger
/// ontology bundle (`f:reasoningDefaults` / `f:schemaSource` with
/// `f:ledger`): the model ledger's subclass/subproperty edges merge into the
/// policy entailment hierarchy. Policies themselves remain same-ledger.
#[allow(clippy::too_many_arguments)]
pub async fn build_policy_context_from_opts_with_schema(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    novelty_for_stats: Option<&Novelty>,
    to_t: i64,
    opts: &GovernanceOptions,
    policy_graphs: &[fluree_db_core::GraphId],
    cross_ledger_schema: Option<std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>>,
) -> Result<PolicyContext> {
    build_policy_context_from_opts_inner(
        snapshot,
        overlay,
        novelty_for_stats,
        to_t,
        opts,
        policy_graphs,
        None,
        cross_ledger_schema,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn build_policy_context_from_opts_with_cross_ledger(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    novelty_for_stats: Option<&Novelty>,
    to_t: i64,
    opts: &GovernanceOptions,
    policy_graphs: &[fluree_db_core::GraphId],
    cross_ledger_restrictions: Vec<PolicyRestriction>,
    cross_ledger_schema: Option<std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>>,
) -> Result<PolicyContext> {
    build_policy_context_from_opts_inner(
        snapshot,
        overlay,
        novelty_for_stats,
        to_t,
        opts,
        policy_graphs,
        Some(cross_ledger_restrictions),
        cross_ledger_schema,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn build_policy_context_from_opts_inner(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    novelty_for_stats: Option<&Novelty>,
    to_t: i64,
    opts: &GovernanceOptions,
    policy_graphs: &[fluree_db_core::GraphId],
    cross_ledger_restrictions: Option<Vec<PolicyRestriction>>,
    cross_ledger_schema: Option<std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>>,
) -> Result<PolicyContext> {
    // A cross-ledger `f:policySource` is only ever passed here when configured,
    // so its presence — even with an empty restriction set — means policy
    // governs this request. It must count as an explicit policy input so a
    // policy class that selects zero model-ledger rules cannot collapse to a
    // root (unrestricted) context; `default_allow` governs instead.
    let has_cross_ledger_source = cross_ledger_restrictions.is_some();

    struct PolicyStatsLookup<'a> {
        overlay: &'a dyn fluree_db_core::OverlayProvider,
    }

    #[async_trait]
    impl StatsLookup for PolicyStatsLookup<'_> {
        async fn lookup_subject_classes(
            &self,
            snapshot: &LedgerSnapshot,
            _overlay: &dyn fluree_db_core::OverlayProvider,
            to_t: i64,
            g_id: fluree_db_core::GraphId,
            subjects: &[Sid],
        ) -> std::result::Result<HashMap<Sid, Vec<Sid>>, StatsAssemblyError> {
            fluree_db_policy::lookup_subject_classes(
                subjects,
                GraphDbRef::new(snapshot, g_id, self.overlay, to_t),
            )
            .await
            .map_err(|e| StatsAssemblyError::Message(e.to_string()))
        }
    }

    // Build policy values map first (SID mappings for policy variables)
    let mut policy_values = build_policy_values(snapshot, &opts.policy_values)?;

    // Load policies and resolve identity SID.
    //
    // When opts.identity is set (same-ledger), load_policies_by_identity returns a
    // three-state enum distinguishing identity-not-in-ledger,
    // identity-exists-with-no-policies, and identity-exists-with-policies. The
    // distinction matters for binding `?$identity` in policy_values (only possible
    // when we have a concrete SID), not for gating access — `opts.default_allow`
    // governs in all three cases.
    //
    // Stored-policy selection priority: cross-ledger restrictions >
    // identity + policy_class (classes select, identity binds) > identity >
    // policy_class. Inline `opts.policy` is not part of the chain — it merges
    // additively after selection.
    // `?$identity` binding priority: identity > policy_values["?$identity"].
    let (identity_sid, mut restrictions) = if let Some(merged) = cross_ledger_restrictions {
        // Cross-ledger short-circuit: the resolver already materialized
        // restrictions from the model ledger, filtered by the policy-class
        // chain. Rule selection is complete before this function runs.
        //
        // Identity contract: an identity on the request is BIND-ONLY here.
        // It resolves against the data ledger to populate `?$identity` for
        // f:query rules — it never selects rules the way same-ledger
        // identity-mode does (via the identity's f:policyClass triples in
        // D). Those D-local triples are intentionally not consulted: a
        // cross-ledger f:policySource declares M the policy authority.
        // An identity with no subject node in D yields an unbound
        // `?$identity` (f:query rules referencing it won't match), same as
        // identity-mode's NotFound.
        //
        // opts.policy (inline JSON-LD) still applies — it merges after the
        // selection chain. Moving — not cloning — the owned input keeps
        // model-ledger policy sets (which can be large: each
        // `PolicyRestriction` carries strings + hash sets) from paying a
        // per-request copy.
        let identity_sid = if let Some(identity_iri) = &opts.identity {
            let resolved =
                resolve_identity_binding_sid(snapshot, overlay, to_t, identity_iri, policy_graphs)
                    .await?;
            if let Some(sid) = &resolved {
                policy_values.insert("?$identity".to_string(), sid.clone());
            }
            resolved
        } else if let Some(sid) = policy_values.get("?$identity") {
            Some(sid.clone())
        } else if let Some(pv) = &opts.policy_values {
            if pv.contains_key("?$identity") {
                return Err(ApiError::query(
                    "?$identity provided in policy-values but could not be encoded",
                ));
            }
            None
        } else {
            None
        };

        (identity_sid, merged)
    } else if let (Some(identity_iri), Some(classes)) = (
        &opts.identity,
        opts.policy_class.as_ref().filter(|c| !c.is_empty()),
    ) {
        // Same-ledger identity + explicit `policy-class`: the request's
        // classes select the policy set; the identity is BIND-ONLY — it
        // resolves to populate `?$identity` for f:query rules and never
        // drives rule selection. This mirrors the cross-ledger identity
        // contract above.
        //
        // Without this arm, a request carrying both fields silently ignored
        // `policy-class` and fell through to identity-mode selection below —
        // which yields an empty policy set (deny-all under default-deny)
        // whenever the identity has no `f:policyClass` triples in the
        // ledger, and can never work for identities that are not resolvable
        // IRIs (bare emails / UUID subjects minted by application auth
        // systems). Gateways that resolve grant-derived classes per request
        // and forward them alongside the authenticated identity depend on
        // the classes being honored.
        let identity_sid =
            resolve_identity_binding_sid(snapshot, overlay, to_t, identity_iri, policy_graphs)
                .await?;
        if let Some(sid) = &identity_sid {
            policy_values.insert("?$identity".to_string(), sid.clone());
        }
        let restrictions =
            load_policies_by_class(snapshot, overlay, to_t, classes, policy_graphs).await?;
        (identity_sid, restrictions)
    } else if let Some(identity_iri) = &opts.identity {
        match load_policies_by_identity(snapshot, overlay, to_t, identity_iri, policy_graphs)
            .await?
        {
            IdentityLookupResult::NotFound => {
                // IRI unresolvable or no subject node in this ledger. No SID to bind
                // and no restrictions to apply; default_allow governs as configured.
                (None, vec![])
            }
            IdentityLookupResult::FoundNoPolicies { identity_sid } => {
                policy_values.insert("?$identity".to_string(), identity_sid.clone());
                (Some(identity_sid), vec![])
            }
            IdentityLookupResult::FoundWithPolicies {
                identity_sid,
                restrictions,
            } => {
                policy_values.insert("?$identity".to_string(), identity_sid.clone());
                (Some(identity_sid), restrictions)
            }
        }
    } else {
        // Non-identity paths: resolve ?$identity from policy_values if present,
        // then load restrictions from policy_class / inline policy / none.
        let identity_sid = if let Some(sid) = policy_values.get("?$identity") {
            Some(sid.clone())
        } else if let Some(pv) = &opts.policy_values {
            if pv.contains_key("?$identity") {
                return Err(ApiError::query(
                    "?$identity provided in policy-values but could not be encoded",
                ));
            }
            None
        } else {
            None
        };

        let restrictions = if let Some(classes) = &opts.policy_class {
            load_policies_by_class(snapshot, overlay, to_t, classes, policy_graphs).await?
        } else {
            vec![]
        };

        (identity_sid, restrictions)
    };

    // Inline `opts.policy` merges additively in every selection mode: the
    // modes above choose which STORED policies load; they never gate an
    // explicitly supplied inline policy. Merging once here keeps the arms
    // consistent — selection-specific merging silently dropped inline
    // policies on the identity-only and class-only paths, which under
    // default-deny meant deny-all with no signal.
    if let Some(policy_json) = &opts.policy {
        restrictions.extend(parse_inline_policy(snapshot, policy_json)?);
    }

    // Build policy sets (view and modify)
    //
    // Stats are critical for VIEW-side f:onClass policies - they need
    // class→property relationships to know which properties to index.
    // Without stats, view-set OnClass policies only match @id and rdf:type
    // properties (the implicit ones). Modify sets index OnClass policies by
    // class instead (`PolicySet::by_class`) and don't consult stats.
    //
    // Policies need the full novelty-aware class/property view so `f:onClass`
    // restrictions apply even when novelty adds properties without restating
    // the subject's `@type` in the same transaction.
    let stats: Option<IndexStats> = if let Some(novelty) = novelty_for_stats {
        let indexed = snapshot.stats.clone().unwrap_or_default();
        let lookup = PolicyStatsLookup { overlay };
        Some(
            fluree_db_novelty::assemble_full_stats(
                &indexed, snapshot, overlay, novelty, to_t, &lookup,
            )
            .await
            .map_err(|e| ApiError::internal(format!("policy stats assembly failed: {e}")))?,
        )
    } else {
        snapshot.stats.clone()
    };

    // Current RDFS hierarchy (always-on entailment for enforcement): class
    // policies govern subclass instances, property policies govern
    // subproperties. Only OnClass/OnProperty restrictions consult it —
    // identity-only, default-allow, and OnSubject policies don't. Skip the
    // O(ontology-size) schema clone + sort + scans entirely when nothing can
    // use it, since this builder runs uncached on every governed query.
    let needs_hierarchy = restrictions
        .iter()
        .any(|r| !r.for_classes.is_empty() || matches!(r.target_mode, TargetMode::OnProperty));
    let hierarchy = if !needs_hierarchy {
        None
    } else {
        match &cross_ledger_schema {
            // Cross-ledger ontology: compose the model ledger's schema bundle
            // over the local overlay so its subclass/subproperty edges merge in.
            Some(bundle) => {
                let composed = fluree_db_query::schema_bundle::SchemaBundleOverlay::new(
                    overlay,
                    std::sync::Arc::clone(bundle),
                );
                fluree_db_core::compute_schema_hierarchy_with_overlay(snapshot, &composed, to_t)
                    .await
            }
            None => {
                fluree_db_core::compute_schema_hierarchy_with_overlay(snapshot, overlay, to_t).await
            }
        }
        .map_err(|e| ApiError::internal(format!("policy hierarchy computation failed: {e}")))?
    };

    let view_set = build_policy_set(
        restrictions.clone(),
        stats.as_ref(),
        PolicyAction::View,
        hierarchy.as_ref(),
    );
    let modify_set = build_policy_set(
        restrictions,
        stats.as_ref(),
        PolicyAction::Modify,
        hierarchy.as_ref(),
    );

    // Check if this is a root policy (unrestricted access).
    //
    // is_root = true ONLY when no explicit policy inputs (identity / policy-class / policy)
    // were provided. When an identity IS specified but has no matching policies, is_root must
    // be false so that `default_allow` (not a blanket bypass) governs access.
    let has_explicit_policy_input = opts.identity.is_some()
        || opts.policy_class.as_ref().is_some_and(|v| !v.is_empty())
        || opts.policy.is_some()
        || has_cross_ledger_source;
    let is_root = !has_explicit_policy_input
        && view_set.restrictions.is_empty()
        && modify_set.restrictions.is_empty();

    // `default_allow` is honored as the caller set it, including for unknown identities.
    // An identity IRI that has no subject node in the ledger yields empty restrictions,
    // and a permissive `default_allow: true` is an explicit admin opt-in — typically
    // when an application layer in front of the DB handles authorization and Fluree
    // just records the signed transaction for provenance. Callers who want fail-closed
    // behavior set `default_allow: false`.
    let wrapper = PolicyWrapper::new(
        view_set,
        modify_set,
        is_root,
        opts.default_allow,
        policy_values,
    );

    // Create context with identity
    Ok(PolicyContext::new(wrapper, identity_sid))
}

/// Returns `true` iff `identity_iri` exists as a subject in the ledger but has
/// **no** `f:policyClass` assignments — meaning no policy restrictions apply to
/// that identity.
///
/// This is the predicate used to decide whether a bearer-authenticated identity
/// may impersonate another identity via `opts.identity` for policy testing.
/// The semantics are:
///
/// - `FoundNoPolicies` → `true`: the identity is known and unrestricted, so it
///   may delegate / impersonate.
/// - `FoundWithPolicies` → `false`: the identity is itself policy-constrained
///   and must not be allowed to bypass its own constraints by acting as another
///   identity.
/// - `NotFound` → `false`: an unknown identity must not gain impersonation
///   rights regardless of `default_allow`.
pub async fn identity_has_no_policies(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    identity_iri: &str,
) -> Result<bool> {
    match load_policies_by_identity(
        snapshot,
        overlay,
        to_t,
        identity_iri,
        &DEFAULT_POLICY_GRAPHS,
    )
    .await?
    {
        IdentityLookupResult::FoundNoPolicies { .. } => Ok(true),
        IdentityLookupResult::FoundWithPolicies { .. } | IdentityLookupResult::NotFound => {
            Ok(false)
        }
    }
}

// ============================================================================
// Identity-based policy loading
// ============================================================================

/// Outcome of looking up an identity's policies in the ledger.
///
/// The three-way split lets callers distinguish whether a concrete identity SID is
/// available for binding `?$identity` in `policy_values`, and whether the identity
/// carries restrictions. `default_allow` governs access in all three cases — the
/// "not found" / "found-no-policies" distinction is about SID availability, not gating.
///
/// A separate predicate, [`identity_has_no_policies`], uses this enum to gate
/// impersonation (only `FoundNoPolicies` qualifies); that gate is orthogonal to
/// `default_allow`.
enum IdentityLookupResult {
    /// The identity IRI cannot be resolved (unregistered namespace) or has no subject
    /// node in this ledger. No identity SID is available to bind `?$identity`.
    NotFound,
    /// The identity IRI exists as a subject in the ledger but has no `f:policyClass`
    /// property. No restrictions apply; `default_allow` governs access.
    FoundNoPolicies { identity_sid: Sid },
    /// The identity IRI exists and has associated policy restrictions.
    FoundWithPolicies {
        identity_sid: Sid,
        restrictions: Vec<PolicyRestriction>,
    },
}

/// Resolve an identity IRI to a bindable SID **without loading its policies**.
///
/// Used under cross-ledger `f:policySource`, where rule selection is
/// exclusively the wire's policy-class filter and the identity contributes
/// only the `?$identity` binding. Mirrors identity-mode's three-state
/// contract for the binding decision: `None` when the IRI is unresolvable or
/// has no subject node in the searched graphs (identity-mode's `NotFound` —
/// no binding), `Some(sid)` when the subject exists (with or without
/// D-local policies, which are intentionally not consulted here).
async fn resolve_identity_binding_sid(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    identity_iri: &str,
    graphs: &[fluree_db_core::GraphId],
) -> Result<Option<Sid>> {
    let identity_sid = match resolve_identity_iri_to_sid(snapshot, identity_iri) {
        Ok(sid) => sid,
        Err(_) => return Ok(None),
    };

    if subject_exists_in_graphs(snapshot, overlay, to_t, &identity_sid, graphs).await? {
        Ok(Some(identity_sid))
    } else {
        Ok(None)
    }
}

/// True if `subject` appears as the subject of at least one flake in any of
/// `graphs`. A `SPOT` range capped at one flake — the cheapest existence probe.
///
/// Shared by identity binding under cross-ledger policy
/// ([`resolve_identity_binding_sid`]) and same-ledger identity-mode's
/// found-no-policies check ([`load_policies_by_identity`]) so the two agree on
/// what "the identity exists" means (same index, same graph set).
async fn subject_exists_in_graphs(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    subject: &Sid,
    graphs: &[fluree_db_core::GraphId],
) -> Result<bool> {
    let range_opts = RangeOptions::default().with_flake_limit(1);
    for &g_id in graphs {
        let db = GraphDbRef::new(snapshot, g_id, overlay, to_t);
        let exists = db
            .range_with_opts(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(subject.clone()),
                range_opts.clone(),
            )
            .await
            .map_err(|e| ApiError::internal(format!("identity existence check failed: {e}")))?;
        if !exists.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Look up the policies for `identity_iri` via its `f:policyClass` property.
///
/// Returns an [`IdentityLookupResult`] that distinguishes whether the identity
/// subject exists in the ledger and whether it carries any restrictions.
///
/// Legacy equivalent: `wrap-identity-policy`
///
/// Query pattern:
/// ```sparql
/// SELECT ?policy WHERE {
///   <identity> f:policyClass ?class .
///   ?policy a ?class .
/// }
/// ```
async fn load_policies_by_identity(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    identity_iri: &str,
    policy_graphs: &[fluree_db_core::GraphId],
) -> Result<IdentityLookupResult> {
    // Encode the identity IRI strictly — unregistered namespaces (including CURIEs
    // passed as opts.identity) produce NotFound rather than a silent empty result.
    let identity_sid = match resolve_identity_iri_to_sid(snapshot, identity_iri) {
        Ok(sid) => sid,
        Err(_) => return Ok(IdentityLookupResult::NotFound),
    };

    // `https://ns.flur.ee/db#` is in default_namespace_codes() and is pre-registered in
    // every ledger from genesis, so this encoding cannot fail in practice. Propagate as an
    // internal error rather than silently absorbing an invariant violation.
    let policy_class_sid =
        resolve_system_iri_to_sid(snapshot, policy_iris::POLICY_CLASS, "f:policyClass")?;

    let mut vars = VarRegistry::new();
    let class_var = vars.get_or_insert("?class");

    // Query: <identity> f:policyClass ?class
    let pattern = TriplePattern::new(
        Ref::Sid(identity_sid.clone()),
        Ref::Sid(policy_class_sid),
        Term::Var(class_var),
    );

    // Collect class SIDs from the configured policy graphs.
    // Eager materialization: `as_sid()` needs concrete `Binding::Sid`, not
    // late-materialized `EncodedSid` from binary scans with epoch=0.
    let mut class_sids: Vec<Sid> = Vec::new();
    for &g_id in policy_graphs {
        let db = GraphDbRef::new(snapshot, g_id, overlay, to_t).eager();
        let batches = execute_pattern(db, &vars, pattern.clone()).await?;
        for batch in &batches {
            for row in 0..batch.len() {
                if let Some(binding) = batch.get(row, class_var) {
                    if let Some(sid) = binding.as_sid() {
                        class_sids.push(sid.clone());
                    }
                }
            }
        }
    }

    if class_sids.is_empty() {
        // No f:policyClass found. Determine whether the identity subject itself exists
        // in any of the configured policy graphs. Both the policyClass lookup and this
        // existence check must cover the same set of graphs so that named-graph
        // policy configurations work consistently.
        if subject_exists_in_graphs(snapshot, overlay, to_t, &identity_sid, policy_graphs).await? {
            return Ok(IdentityLookupResult::FoundNoPolicies { identity_sid });
        }
        return Ok(IdentityLookupResult::NotFound);
    }

    // Step 2: Load policies of those classes
    let restrictions =
        load_policies_of_classes(snapshot, overlay, to_t, &class_sids, policy_graphs).await?;
    Ok(IdentityLookupResult::FoundWithPolicies {
        identity_sid,
        restrictions,
    })
}

// ============================================================================
// Class-based policy loading
// ============================================================================

/// Load policies by querying for subjects of the given class types.
///
/// Legacy equivalent: `wrap-class-policy`. `pub(crate)` so the
/// cross-ledger resolver can reuse the same load path against a
/// model ledger's snapshot.
pub(crate) async fn load_policies_by_class(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    class_iris: &[String],
    policy_graphs: &[fluree_db_core::GraphId],
) -> Result<Vec<PolicyRestriction>> {
    // Resolve class IRIs to SIDs
    let mut class_sids = Vec::with_capacity(class_iris.len());
    for iri in class_iris {
        class_sids.push(resolve_policy_class_iri_to_sid(snapshot, iri)?);
    }

    load_policies_of_classes(snapshot, overlay, to_t, &class_sids, policy_graphs).await
}

/// Load policies that are instances of the given classes.
///
/// Query pattern:
/// ```sparql
/// SELECT ?policy WHERE {
///   ?policy a ?class .
/// }
/// ```
/// Then load each policy's properties.
async fn load_policies_of_classes(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    class_sids: &[Sid],
    policy_graphs: &[fluree_db_core::GraphId],
) -> Result<Vec<PolicyRestriction>> {
    let rdf_type_sid = resolve_system_iri_to_sid(snapshot, RDF_TYPE_IRI, "rdf:type")?;

    // Collect all policy subjects
    let mut policy_sids: HashSet<Sid> = HashSet::new();

    for class_sid in class_sids {
        for &g_id in policy_graphs {
            let db = GraphDbRef::new(snapshot, g_id, overlay, to_t);
            let flakes = db
                .range(
                    // POST is the correct index for `rdf:type` lookups by object (class).
                    IndexType::Post,
                    RangeTest::Eq,
                    RangeMatch::predicate_object(
                        rdf_type_sid.clone(),
                        FlakeValue::Ref(class_sid.clone()),
                    ),
                )
                .await
                .map_err(|e| ApiError::internal(format!("policy class lookup failed: {e}")))?;

            for flake in flakes {
                policy_sids.insert(flake.s);
            }
        }
    }

    // Load each policy's restrictions
    let mut restrictions = Vec::new();
    for policy_sid in policy_sids {
        if let Some(restriction) =
            load_policy_restriction(snapshot, overlay, to_t, &policy_sid, policy_graphs).await?
        {
            restrictions.push(restriction);
        }
    }

    Ok(restrictions)
}

/// Load a single policy's restriction from the database.
///
/// NOTE: This function uses explicit predicate queries (not wildcard `?pred`)
/// because the scan layer filters out internal `fluree:ledger` predicates
/// when the predicate is a variable. Since all policy vocabulary predicates
/// are in the `fluree:ledger` namespace, we must query them explicitly.
///
/// `pub(crate)` so the cross-ledger materializer can reuse the same
/// per-policy predicate fan-out against a model ledger's snapshot.
pub(crate) async fn load_policy_restriction(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    policy_sid: &Sid,
    policy_graphs: &[fluree_db_core::GraphId],
) -> Result<Option<PolicyRestriction>> {
    // Collect properties using explicit predicate queries
    // (wildcard ?pred would be filtered by scan layer for fluree:ledger predicates)
    let mut allow: Option<bool> = None;
    let mut on_property: HashSet<Sid> = HashSet::new();
    let mut on_subject: HashSet<Sid> = HashSet::new();
    let mut on_class: HashSet<Sid> = HashSet::new();
    let mut required = false;
    let mut message: Option<String> = None;
    let mut policy_query_source: Option<(String, PolicyQueryLanguage)> = None;

    // Resolve predicate SIDs we need to query (system IRIs must resolve strictly).
    let view_sid = resolve_system_iri_to_sid(snapshot, policy_iris::VIEW, "f:view")?;
    let modify_sid = resolve_system_iri_to_sid(snapshot, policy_iris::MODIFY, "f:modify")?;

    // Query each policy predicate explicitly
    // f:allow
    {
        let allow_sid = resolve_system_iri_to_sid(snapshot, policy_iris::ALLOW, "f:allow")?;
        let bindings = query_predicate(
            snapshot,
            overlay,
            to_t,
            policy_sid,
            &allow_sid,
            policy_graphs,
        )
        .await?;
        for binding in bindings {
            if let Binding::Lit {
                val: FlakeValue::Boolean(b),
                ..
            } = binding
            {
                allow = Some(b);
                break;
            }
        }
    }

    // f:action - collect action values: f:view / f:modify plus the write
    // verbs f:create / f:update / f:delete. Any verb implies the modify side.
    let (action, verbs): (Option<PolicyAction>, Option<WriteVerbs>) = {
        let action_sid = resolve_system_iri_to_sid(snapshot, policy_iris::ACTION, "f:action")?;
        let create_sid = resolve_system_iri_to_sid(snapshot, policy_iris::CREATE, "f:create")?;
        let update_sid = resolve_system_iri_to_sid(snapshot, policy_iris::UPDATE, "f:update")?;
        let delete_sid = resolve_system_iri_to_sid(snapshot, policy_iris::DELETE, "f:delete")?;
        let bindings = query_predicate(
            snapshot,
            overlay,
            to_t,
            policy_sid,
            &action_sid,
            policy_graphs,
        )
        .await?;
        let mut has_view = false;
        let mut has_modify = false;
        let mut v = WriteVerbs::default();
        for binding in bindings {
            if let Some(action_ref) = binding.as_sid() {
                if &view_sid == action_ref {
                    has_view = true;
                } else if &modify_sid == action_ref {
                    has_modify = true;
                } else if &create_sid == action_ref {
                    v.create = true;
                } else if &update_sid == action_ref {
                    v.update = true;
                } else if &delete_sid == action_ref {
                    v.delete = true;
                }
            }
        }
        // Explicit verbs select exact lifecycle semantics. Bare f:modify
        // alongside verbs still means "all writes", so it widens the verb
        // set to ALL (keeping exact semantics); bare f:modify alone stays
        // legacy (verbs: None).
        let verbs = if v.any() {
            if has_modify {
                Some(WriteVerbs::ALL)
            } else {
                Some(v)
            }
        } else {
            None
        };
        let has_modify_side = has_modify || v.any();
        let action = match (has_view, has_modify_side) {
            (true, true) => Some(PolicyAction::Both),
            (true, false) => Some(PolicyAction::View),
            (false, true) => Some(PolicyAction::Modify),
            (false, false) => None,
        };
        (action, verbs)
    };

    // f:onProperty (can have multiple values)
    {
        let pred_sid =
            resolve_system_iri_to_sid(snapshot, policy_iris::ON_PROPERTY, "f:onProperty")?;
        let bindings = query_predicate(
            snapshot,
            overlay,
            to_t,
            policy_sid,
            &pred_sid,
            policy_graphs,
        )
        .await?;
        for binding in bindings {
            if let Some(sid) = binding.as_sid() {
                on_property.insert(sid.clone());
            }
        }
    }

    // f:onSubject (can have multiple values)
    {
        let pred_sid = resolve_system_iri_to_sid(snapshot, policy_iris::ON_SUBJECT, "f:onSubject")?;
        let bindings = query_predicate(
            snapshot,
            overlay,
            to_t,
            policy_sid,
            &pred_sid,
            policy_graphs,
        )
        .await?;
        for binding in bindings {
            if let Some(sid) = binding.as_sid() {
                on_subject.insert(sid.clone());
            }
        }
    }

    // f:onClass (can have multiple values)
    {
        let pred_sid = resolve_system_iri_to_sid(snapshot, policy_iris::ON_CLASS, "f:onClass")?;
        let bindings = query_predicate(
            snapshot,
            overlay,
            to_t,
            policy_sid,
            &pred_sid,
            policy_graphs,
        )
        .await?;
        for binding in bindings {
            if let Some(sid) = binding.as_sid() {
                on_class.insert(sid.clone());
            }
        }
    }

    // f:required
    {
        let pred_sid = resolve_system_iri_to_sid(snapshot, policy_iris::REQUIRED, "f:required")?;
        let bindings = query_predicate(
            snapshot,
            overlay,
            to_t,
            policy_sid,
            &pred_sid,
            policy_graphs,
        )
        .await?;
        for binding in bindings {
            if let Binding::Lit {
                val: FlakeValue::Boolean(b),
                ..
            } = binding
            {
                required = b;
                break;
            }
        }
    }

    // f:exMessage
    {
        let pred_sid = resolve_system_iri_to_sid(snapshot, policy_iris::EX_MESSAGE, "f:exMessage")?;
        let bindings = query_predicate(
            snapshot,
            overlay,
            to_t,
            policy_sid,
            &pred_sid,
            policy_graphs,
        )
        .await?;
        for binding in bindings {
            if let Binding::Lit {
                val: FlakeValue::String(s),
                ..
            } = binding
            {
                message = Some(s.clone());
                break;
            }
        }
    }

    // f:query
    {
        let pred_sid = resolve_system_iri_to_sid(snapshot, policy_iris::QUERY, "f:query")?;
        let bindings = query_predicate(
            snapshot,
            overlay,
            to_t,
            policy_sid,
            &pred_sid,
            policy_graphs,
        )
        .await?;
        for binding in bindings {
            match binding {
                Binding::Lit {
                    val: FlakeValue::Json(s),
                    ..
                } => {
                    policy_query_source = Some((s.clone(), PolicyQueryLanguage::JsonLd));
                    break;
                }
                // Plain-string literals: the datatype selects the language.
                // `f:sparql` → SPARQL; `f:cypher` → Cypher; anything else
                // (bare xsd:string) keeps the legacy JSON-LD interpretation.
                Binding::Lit {
                    val: FlakeValue::String(s),
                    dtc,
                    ..
                } => {
                    let language = if is_language_datatype(&dtc, fluree_vocab::db::SPARQL) {
                        PolicyQueryLanguage::Sparql
                    } else if is_language_datatype(&dtc, fluree_vocab::db::CYPHER) {
                        PolicyQueryLanguage::Cypher
                    } else {
                        PolicyQueryLanguage::JsonLd
                    };
                    policy_query_source = Some((s.clone(), language));
                    break;
                }
                _ => {}
            }
        }
    }

    // f:queryState — which transaction state the f:query condition
    // evaluates against (f:preState default / f:postState).
    let query_state: ConditionState = {
        let qs_sid = resolve_system_iri_to_sid(snapshot, policy_iris::QUERY_STATE, "f:queryState")?;
        let post_sid = resolve_system_iri_to_sid(snapshot, policy_iris::POST_STATE, "f:postState")?;
        let bindings =
            query_predicate(snapshot, overlay, to_t, policy_sid, &qs_sid, policy_graphs).await?;
        let mut state = ConditionState::Pre;
        for binding in bindings {
            if binding.as_sid() == Some(&post_sid) {
                state = ConditionState::Post;
            }
        }
        state
    };

    // Determine target mode and targets
    let (target_mode, targets, for_classes) = if !on_property.is_empty() {
        (TargetMode::OnProperty, on_property, HashSet::new())
    } else if !on_subject.is_empty() {
        (TargetMode::OnSubject, on_subject, HashSet::new())
    } else if !on_class.is_empty() {
        (TargetMode::OnClass, HashSet::new(), on_class)
    } else {
        // Default policy
        (TargetMode::Default, HashSet::new(), HashSet::new())
    };

    // Decode policy SID to IRI for better tracking/debugging
    let policy_id = snapshot
        .decode_sid(policy_sid)
        .unwrap_or_else(|| policy_sid.name.to_string());

    // Determine policy value (allow/deny/query)
    // Priority: f:allow takes precedence over f:query
    let value = match allow {
        Some(true) => PolicyValue::Allow,
        Some(false) => PolicyValue::Deny,
        None => {
            // No explicit allow/deny - check for f:query
            if let Some((source, language)) = policy_query_source {
                // Store the raw policy query source. Parsing/lowering is handled
                // by the query engine. We still validate the source parses to
                // preserve the "deny on parse error" behavior without
                // duplicating query lowering logic.
                make_policy_query_value(&policy_id, source, language, query_state)
            } else {
                // No f:allow and no f:query - this is likely a misconfigured policy
                tracing::warn!(
                    "Policy '{}': missing both f:allow and f:query, defaulting to deny",
                    policy_id
                );
                PolicyValue::Deny
            }
        }
    };

    // Warn if policy has no action specified (will default to Both)
    if action.is_none() {
        tracing::debug!(
            "Policy '{}': no f:action specified, applying to both view and modify",
            policy_id
        );
    }

    // Create restriction
    let restriction = PolicyRestriction {
        id: policy_id,
        target_mode,
        targets,
        action: action.unwrap_or(PolicyAction::Both),
        verbs,
        value,
        required,
        message,
        class_policy: !for_classes.is_empty(),
        for_classes,
        class_check_needed: false, // Will be set by build_policy_set
    };

    Ok(Some(restriction))
}

/// Query for a specific predicate on a subject and return all object bindings.
///
/// Uses an explicit predicate SID (not a variable) to avoid the scan layer's
/// filtering of internal `fluree:ledger` predicates.
async fn query_predicate(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    predicate_sid: &Sid,
    policy_graphs: &[fluree_db_core::GraphId],
) -> Result<Vec<Binding>> {
    // Use range() to avoid late-materialized Encoded* bindings.
    // Policy loading needs concrete SID/literal values for restriction indexing.
    let mut results: Vec<Binding> = Vec::new();
    for &g_id in policy_graphs {
        let db = GraphDbRef::new(snapshot, g_id, overlay, to_t);
        let flakes = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(subject_sid.clone(), predicate_sid.clone()),
            )
            .await
            .map_err(|e| ApiError::internal(format!("policy predicate lookup failed: {e}")))?;

        for flake in flakes {
            match flake.o {
                FlakeValue::Ref(sid) => results.push(Binding::sid(sid)),
                val => {
                    let dtc = match flake
                        .m
                        .as_ref()
                        .and_then(|m| m.lang.as_ref())
                        .map(|s| Arc::<str>::from(s.as_str()))
                    {
                        Some(lang) => fluree_db_core::DatatypeConstraint::LangTag(lang),
                        None => fluree_db_core::DatatypeConstraint::Explicit(flake.dt),
                    };
                    results.push(Binding::Lit {
                        val,
                        dtc,
                        t: Some(flake.t),
                        op: None,
                        p_id: None,
                    });
                }
            }
        }
    }

    Ok(results)
}

// ============================================================================
// Inline policy parsing
// ============================================================================

/// Parse inline policy JSON-LD into restrictions.
///
/// Legacy equivalent: `wrap-policy` with inline policy
fn parse_inline_policy(
    snapshot: &LedgerSnapshot,
    policy_json: &JsonValue,
) -> Result<Vec<PolicyRestriction>> {
    // The inline policy can be a single object or an array of objects
    let policies = match policy_json {
        JsonValue::Array(arr) => arr.clone(),
        JsonValue::Object(_) => vec![policy_json.clone()],
        _ => {
            return Err(ApiError::query(
                "Invalid policy: expected object or array of policy objects",
            ))
        }
    };

    let mut restrictions = Vec::new();

    for (idx, policy) in policies.iter().enumerate() {
        let obj = policy.as_object().ok_or_else(|| {
            ApiError::query(format!("Invalid policy at index {idx}: expected object"))
        })?;

        // Extract policy ID early for use in logging
        let id = obj
            .get("@id")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string)
            .unwrap_or_else(|| format!("inline-policy-{idx}"));

        // Extract f:allow (optional). If absent, policy may be driven by f:query.
        let allow: Option<bool> = obj
            .get("f:allow")
            .or_else(|| obj.get(&format!("{}allow", fluree::DB)))
            .and_then(serde_json::Value::as_bool);

        // Extract f:query (optional). For inline policies we accept:
        // - String: JSON query string
        // - Object: {"@type":"@json","@value":{...}} where @value is serialized to JSON string
        // - Object: {"@type":"f:sparql","@value":"ASK ..."} for SPARQL policies
        // - Object: {"@type":"f:cypher","@value":"MATCH ..."} for Cypher policies
        //
        // `@json` values can use object `@value` (not just string).
        let policy_query_source: Option<(String, PolicyQueryLanguage)> = obj
            .get("f:query")
            .or_else(|| obj.get(&format!("{}query", fluree::DB)))
            .and_then(|v| match v {
                JsonValue::String(s) => Some((s.clone(), PolicyQueryLanguage::JsonLd)),
                JsonValue::Object(o) => {
                    let inner = o.get("@value")?;
                    let type_str = o.get("@type").and_then(JsonValue::as_str);
                    // SPARQL typed value: {"@type": "f:sparql", "@value": "ASK ..."}
                    if type_str
                        .is_some_and(|t| t == "f:sparql" || t == fluree_vocab::fluree::SPARQL)
                    {
                        return inner
                            .as_str()
                            .map(|s| (s.to_string(), PolicyQueryLanguage::Sparql));
                    }
                    // Cypher typed value: {"@type": "f:cypher", "@value": "MATCH ..."}
                    if type_str
                        .is_some_and(|t| t == "f:cypher" || t == fluree_vocab::fluree::CYPHER)
                    {
                        return inner
                            .as_str()
                            .map(|s| (s.to_string(), PolicyQueryLanguage::Cypher));
                    }
                    // @json typed values
                    match inner {
                        // @value is a string (already serialized JSON)
                        JsonValue::String(s) => Some((s.clone(), PolicyQueryLanguage::JsonLd)),
                        // @value is an object (needs serialization)
                        JsonValue::Object(_) | JsonValue::Array(_) => serde_json::to_string(inner)
                            .ok()
                            .map(|s| (s, PolicyQueryLanguage::JsonLd)),
                        _ => None,
                    }
                }
                _ => None,
            });

        // f:queryState — which transaction state the f:query condition
        // evaluates against (f:preState default / f:postState).
        let query_state = obj
            .get("f:queryState")
            .or_else(|| obj.get(&format!("{}queryState", fluree::DB)))
            .map(|v| {
                let iris = extract_iris(v);
                if iris
                    .iter()
                    .any(|i| i == "f:postState" || i == policy_iris::POST_STATE)
                {
                    ConditionState::Post
                } else {
                    ConditionState::Pre
                }
            })
            .unwrap_or_default();

        // Extract f:action - can be string, object with @id, or array of these
        let action_value = obj
            .get("f:action")
            .or_else(|| obj.get(&format!("{}action", fluree::DB)));

        let (action, verbs) = parse_action_value(action_value);

        // Extract targets - track whether targeting was specified for validation
        let mut on_property: HashSet<Sid> = HashSet::new();
        let mut on_subject: HashSet<Sid> = HashSet::new();
        let mut on_class: HashSet<Sid> = HashSet::new();
        let mut had_on_property = false;
        let mut had_on_subject = false;
        let mut had_on_class = false;

        // f:onProperty
        if let Some(props) = obj
            .get("f:onProperty")
            .or_else(|| obj.get(&format!("{}onProperty", fluree::DB)))
        {
            had_on_property = true;
            for iri in extract_iris(props) {
                match resolve_iri_to_sid(snapshot, &iri) {
                    Ok(sid) => {
                        on_property.insert(sid);
                    }
                    Err(_) => {
                        tracing::warn!(
                            policy = %id,
                            iri = %iri,
                            key = "f:onProperty",
                            "IRI could not be resolved - namespace may not be registered"
                        );
                    }
                }
            }
        }

        // f:onSubject
        if let Some(subjs) = obj
            .get("f:onSubject")
            .or_else(|| obj.get(&format!("{}onSubject", fluree::DB)))
        {
            had_on_subject = true;
            for iri in extract_iris(subjs) {
                match resolve_iri_to_sid(snapshot, &iri) {
                    Ok(sid) => {
                        on_subject.insert(sid);
                    }
                    Err(_) => {
                        tracing::warn!(
                            policy = %id,
                            iri = %iri,
                            key = "f:onSubject",
                            "IRI could not be resolved"
                        );
                    }
                }
            }
        }

        // f:onClass
        if let Some(classes) = obj
            .get("f:onClass")
            .or_else(|| obj.get(&format!("{}onClass", fluree::DB)))
        {
            had_on_class = true;
            for iri in extract_iris(classes) {
                match resolve_iri_to_sid(snapshot, &iri) {
                    Ok(sid) => {
                        on_class.insert(sid);
                    }
                    Err(_) => {
                        tracing::warn!(
                            policy = %id,
                            iri = %iri,
                            key = "f:onClass",
                            "IRI could not be resolved"
                        );
                    }
                }
            }
        }

        // Validate: if targeting was specified but all IRIs failed to resolve,
        // this is likely a configuration error. We log a warning but allow the
        // policy to proceed (it will effectively be inactive).
        if had_on_property && on_property.is_empty() {
            tracing::warn!(
                policy = %id,
                "f:onProperty specified but no IRIs could be resolved - policy will not match any property"
            );
        }
        if had_on_subject && on_subject.is_empty() {
            tracing::warn!(
                policy = %id,
                "f:onSubject specified but no IRIs could be resolved - policy will not match any subject"
            );
        }
        if had_on_class && on_class.is_empty() {
            tracing::warn!(
                policy = %id,
                "f:onClass specified but no IRIs could be resolved - policy will not match any class"
            );
        }

        // f:required
        let required = obj
            .get("f:required")
            .or_else(|| obj.get(&format!("{}required", fluree::DB)))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        // f:exMessage
        let message = obj
            .get("f:exMessage")
            .or_else(|| obj.get(&format!("{}exMessage", fluree::DB)))
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        // Determine target mode
        //
        // When f:onProperty is combined with f:onClass, the policy targets those
        // Properties should apply only to instances of those classes.
        // The `for_classes` field carries the class restriction.
        let (target_mode, targets, for_classes) = if !on_property.is_empty() {
            // OnProperty may also have a class restriction
            (TargetMode::OnProperty, on_property, on_class)
        } else if !on_subject.is_empty() {
            (TargetMode::OnSubject, on_subject, HashSet::new())
        } else if !on_class.is_empty() {
            (TargetMode::OnClass, HashSet::new(), on_class)
        } else {
            (TargetMode::Default, HashSet::new(), HashSet::new())
        };

        // Policy value (allow/deny/query)
        // Priority: `f:allow` takes precedence over `f:query`.
        let value = match allow {
            Some(true) => PolicyValue::Allow,
            Some(false) => PolicyValue::Deny,
            None => {
                if let Some((source, language)) = policy_query_source {
                    make_policy_query_value(&id, source, language, query_state)
                } else {
                    PolicyValue::Deny
                }
            }
        };

        restrictions.push(PolicyRestriction {
            id,
            target_mode,
            targets,
            action,
            verbs,
            value,
            required,
            message,
            class_policy: !for_classes.is_empty(),
            for_classes,
            class_check_needed: false,
        });
    }

    Ok(restrictions)
}

// ============================================================================
// Helper functions
// ============================================================================

// NOTE: Policy query parsing is intentionally delegated to the query engine
// (`fluree-db-query`) to avoid duplicating query parsing/lowering and to ensure
// full feature support (e.g., FILTER patterns) in f:query policies.

/// True when a literal's datatype is the given `f:` language datatype
/// (local name under the fluree-db namespace, e.g. `f:sparql` / `f:cypher`).
fn is_language_datatype(dtc: &fluree_db_core::DatatypeConstraint, local_name: &str) -> bool {
    matches!(
        dtc,
        fluree_db_core::DatatypeConstraint::Explicit(sid)
            if *sid == Sid::new(fluree_vocab::namespaces::FLUREE_DB, local_name)
    )
}

/// Build the `PolicyValue` for an `f:query` source, validating per language.
///
/// Validation preserves the historical "deny on parse error" behavior: a
/// source that fails to parse (or is not the language's condition form —
/// ASK/SELECT for SPARQL, a read-only query for Cypher) yields
/// `PolicyValue::Deny` with a warning rather than an error.
///
/// For SPARQL / Cypher sources this also registers the corresponding
/// lowering hooks so the policy executor can evaluate the query later.
fn make_policy_query_value(
    policy_id: &str,
    source: String,
    language: PolicyQueryLanguage,
    state: ConditionState,
) -> PolicyValue {
    let validation = match language {
        PolicyQueryLanguage::JsonLd => serde_json::from_str::<JsonValue>(&source)
            .map(|_| ())
            .map_err(|e| e.to_string()),
        PolicyQueryLanguage::Sparql => {
            crate::sparql_lang::ensure_sparql_support_registered();
            crate::sparql_lang::validate_sparql_policy_source(&source)
        }
        PolicyQueryLanguage::Cypher => {
            crate::cypher_lang::ensure_cypher_support_registered();
            crate::cypher_lang::validate_cypher_policy_source(&source)
        }
        other => Err(format!(
            "unsupported policy query language {}",
            other.as_str()
        )),
    };
    match validation {
        Ok(()) => PolicyValue::Query(PolicyQuery {
            source,
            language,
            state,
        }),
        Err(e) => {
            tracing::warn!(
                "Policy '{}': invalid {} f:query, defaulting to deny: {}",
                policy_id,
                language.as_str(),
                e
            );
            PolicyValue::Deny
        }
    }
}

/// Resolve an IRI string to a SID using the snapshot's namespace table.
///
/// This is intentionally **lenient**: it uses `encode_iri()` (EMPTY-namespace fallback)
/// rather than `encode_iri_strict()`. Policy inputs often contain full IRIs that may
/// not have an explicit namespace-code registration in unindexed / in-memory ledgers.
/// Using the EMPTY fallback keeps policy enforcement consistent with how queries and
/// transactions encode such IRIs.
fn resolve_iri_to_sid(snapshot: &LedgerSnapshot, iri: &str) -> Result<Sid> {
    Ok(snapshot.encode_iri(iri).unwrap_or_else(|| Sid::new(0, iri)))
}

/// Resolve an identity IRI to a SID **strictly**.
///
/// Connection `opts.identity` is used to look up policies via `f:policyClass`.
/// For parity with the existing API behavior and tests, we treat unknown IRIs
/// (no registered namespace prefix) as an error rather than silently encoding
/// them under the EMPTY namespace.
fn resolve_identity_iri_to_sid(snapshot: &LedgerSnapshot, iri: &str) -> Result<Sid> {
    snapshot.encode_iri_strict(iri).ok_or_else(|| {
        ApiError::query(format!("Failed to resolve IRI '{iri}' for identity policy"))
    })
}

/// Resolve a policy class IRI **strictly**.
///
/// Server-level policy defaults (and query `opts.policy-class`) should not silently
/// fall back to EMPTY namespace encoding, because that would make the class lookup
/// a no-op and effectively disable policy enforcement.
fn resolve_policy_class_iri_to_sid(snapshot: &LedgerSnapshot, iri: &str) -> Result<Sid> {
    snapshot
        .encode_iri_strict(iri)
        .ok_or_else(|| ApiError::query(format!("Failed to resolve IRI '{iri}' for policy-class")))
}

/// Resolve a system vocabulary IRI **strictly**.
///
/// Used for policy vocabulary + RDF/RDFS terms where silent fallback encoding would
/// disable enforcement (e.g., `f:onProperty` mismatch → no targeted policies apply).
fn resolve_system_iri_to_sid(snapshot: &LedgerSnapshot, iri: &str, label: &str) -> Result<Sid> {
    snapshot.encode_iri_strict(iri).ok_or_else(|| {
        ApiError::internal(format!(
            "Failed to resolve required system IRI '{iri}' ({label})"
        ))
    })
}

/// Build policy values map from JSON values.
fn build_policy_values(
    snapshot: &LedgerSnapshot,
    values: &Option<HashMap<String, JsonValue>>,
) -> Result<HashMap<String, Sid>> {
    let mut result = HashMap::new();

    if let Some(vals) = values {
        for (key, val) in vals {
            // Try to extract IRI from value
            let iri = match val {
                JsonValue::String(s) => s.clone(),
                JsonValue::Object(obj) => {
                    // Check for {"@id": "..."} or {"@value": "..."}
                    obj.get("@id")
                        .or_else(|| obj.get("@value"))
                        .and_then(|v| v.as_str())
                        .map(std::string::ToString::to_string)
                        .ok_or_else(|| {
                            ApiError::query(format!(
                                "Invalid policy value for '{key}': expected IRI"
                            ))
                        })?
                }
                _ => {
                    return Err(ApiError::query(format!(
                        "Invalid policy value for '{key}': expected string or object with @id"
                    )))
                }
            };

            if let Ok(sid) = resolve_iri_to_sid(snapshot, &iri) {
                result.insert(key.clone(), sid);
            }
        }
    }

    Ok(result)
}

/// Parse f:action value into PolicyAction plus optional write verbs.
///
/// Handles multiple formats:
/// - String: "f:view", "f:modify", or full IRI
/// - Object with @id: {"@id": "f:view"}
/// - Array of the above: [{"@id": "f:view"}, {"@id": "f:modify"}]
///
/// The write verbs `f:create` / `f:update` / `f:delete` imply the modify
/// side and select exact lifecycle semantics; bare `f:modify` alone keeps
/// legacy semantics (`verbs: None`), and `f:modify` alongside a verb widens
/// the verb set to ALL while keeping exact semantics.
///
/// Returns PolicyAction::Both if both view and modify sides are specified
/// or if the value cannot be parsed.
fn parse_action_value(value: Option<&JsonValue>) -> (PolicyAction, Option<WriteVerbs>) {
    let value = match value {
        Some(v) => v,
        None => return (PolicyAction::Both, None),
    };

    // Collect all action IRIs from the value
    let action_strs = extract_action_strings(value);

    let mut has_view = false;
    let mut has_modify = false;
    let mut v = WriteVerbs::default();

    for s in action_strs {
        if s.contains("view") {
            has_view = true;
        }
        if s.contains("modify") {
            has_modify = true;
        }
        if s == "f:create" || s.ends_with("#create") {
            v.create = true;
        }
        if s == "f:update" || s.ends_with("#update") {
            v.update = true;
        }
        if s == "f:delete" || s.ends_with("#delete") {
            v.delete = true;
        }
    }

    let verbs = if v.any() {
        if has_modify {
            Some(WriteVerbs::ALL)
        } else {
            Some(v)
        }
    } else {
        None
    };
    let has_modify_side = has_modify || v.any();

    let action = match (has_view, has_modify_side) {
        (true, true) => PolicyAction::Both,
        (true, false) => PolicyAction::View,
        (false, true) => PolicyAction::Modify,
        (false, false) => PolicyAction::Both, // Default if no recognized action
    };
    (action, verbs)
}

/// Extract action strings from a JSON value (string, object with @id, or array).
fn extract_action_strings(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::String(s) => vec![s.clone()],
        JsonValue::Object(obj) => {
            if let Some(id) = obj.get("@id").and_then(|v| v.as_str()) {
                vec![id.to_string()]
            } else {
                vec![]
            }
        }
        JsonValue::Array(arr) => arr.iter().flat_map(extract_action_strings).collect(),
        _ => vec![],
    }
}

/// Extract IRIs from a JSON value (single string, object with @id, or array).
fn extract_iris(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::String(s) => vec![s.clone()],
        JsonValue::Object(obj) => {
            if let Some(id) = obj.get("@id").and_then(|v| v.as_str()) {
                vec![id.to_string()]
            } else {
                vec![]
            }
        }
        JsonValue::Array(arr) => arr.iter().flat_map(extract_iris).collect(),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // extract_iris tests
    // ========================================================================

    #[test]
    fn test_extract_iris_string() {
        let v = JsonValue::String("http://example.org/foo".to_string());
        assert_eq!(extract_iris(&v), vec!["http://example.org/foo"]);
    }

    #[test]
    fn test_extract_iris_object() {
        let v = serde_json::json!({"@id": "http://example.org/bar"});
        assert_eq!(extract_iris(&v), vec!["http://example.org/bar"]);
    }

    #[test]
    fn test_extract_iris_array() {
        let v = serde_json::json!(["http://example.org/a", {"@id": "http://example.org/b"}]);
        assert_eq!(
            extract_iris(&v),
            vec!["http://example.org/a", "http://example.org/b"]
        );
    }

    // NOTE: expand_iri tests removed - IRI expansion is tested in the json-ld crate.
    // The json_ld::expand_iri function requires a ParsedContext, and IRI expansion
    // happens at the JSON-LD parsing boundary, not in policy_builder.

    // NOTE: f:query policy parsing/lowering is tested in `fluree-db-query` now.

    // ========================================================================
    // parse_action_value tests
    // ========================================================================

    #[test]
    fn test_parse_action_none() {
        assert_eq!(parse_action_value(None).0, PolicyAction::Both);
    }

    #[test]
    fn test_parse_action_string_view() {
        let v = serde_json::json!("f:view");
        assert_eq!(parse_action_value(Some(&v)).0, PolicyAction::View);
    }

    #[test]
    fn test_parse_action_string_modify() {
        let v = serde_json::json!("f:modify");
        assert_eq!(parse_action_value(Some(&v)).0, PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_object_view() {
        let v = serde_json::json!({"@id": "f:view"});
        assert_eq!(parse_action_value(Some(&v)).0, PolicyAction::View);
    }

    #[test]
    fn test_parse_action_object_modify() {
        let v = serde_json::json!({"@id": "https://ns.flur.ee/db#modify"});
        assert_eq!(parse_action_value(Some(&v)).0, PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_array_view_only() {
        let v = serde_json::json!([{"@id": "f:view"}]);
        assert_eq!(parse_action_value(Some(&v)).0, PolicyAction::View);
    }

    #[test]
    fn test_parse_action_array_modify_only() {
        let v = serde_json::json!([{"@id": "f:modify"}]);
        assert_eq!(parse_action_value(Some(&v)).0, PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_array_both() {
        let v = serde_json::json!([{"@id": "f:view"}, {"@id": "f:modify"}]);
        assert_eq!(parse_action_value(Some(&v)).0, PolicyAction::Both);
    }

    #[test]
    fn test_parse_action_array_full_iris() {
        let v = serde_json::json!([
            {"@id": "https://ns.flur.ee/db#view"},
            {"@id": "https://ns.flur.ee/db#modify"}
        ]);
        assert_eq!(parse_action_value(Some(&v)).0, PolicyAction::Both);
    }
}
