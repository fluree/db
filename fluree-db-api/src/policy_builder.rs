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

use crate::dataset::QueryConnectionOptions;
use crate::error::{ApiError, Result};
use async_trait::async_trait;
use fluree_db_core::IndexStats;
use fluree_db_core::{FlakeValue, GraphDbRef, IndexType, LedgerSnapshot, Sid};
use fluree_db_core::{RangeMatch, RangeOptions, RangeTest};
use fluree_db_novelty::{Novelty, StatsAssemblyError, StatsLookup};
use fluree_db_policy::{
    build_policy_set, PolicyAction, PolicyContext, PolicyQuery, PolicyRestriction, PolicyValue,
    PolicyWrapper, TargetMode,
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

/// Build a `PolicyContext` from `QueryConnectionOptions`.
///
/// Handles the three policy modes:
/// 1. **identity**: Query for policies via the identity's `f:policyClass` property
/// 2. **policy_class**: Query for policies of the given class types
/// 3. **policy**: Parse inline policy JSON-LD
///
/// Priority: identity > policy_class > policy
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
    opts: &QueryConnectionOptions,
    policy_graphs: &[fluree_db_core::GraphId],
) -> Result<PolicyContext> {
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
    // When opts.identity is set, load_policies_by_identity returns a three-state enum
    // distinguishing identity-not-in-ledger, identity-exists-with-no-policies, and
    // identity-exists-with-policies. The distinction matters for binding `?$identity`
    // in policy_values (only possible when we have a concrete SID), not for gating
    // access — `opts.default_allow` governs in all three cases.
    //
    // Priority: identity > policy_class > policy > policy_values["?$identity"]
    let (identity_sid, restrictions) = if let Some(identity_iri) = &opts.identity {
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
        } else if let Some(policy_json) = &opts.policy {
            parse_inline_policy(snapshot, policy_json)?
        } else {
            vec![]
        };

        (identity_sid, restrictions)
    };

    // Build policy sets (view and modify)
    //
    // Stats are critical for f:onClass policies - they need class→property relationships
    // to know which properties to index. Without stats, OnClass policies only match
    // @id and rdf:type properties (the implicit ones).
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

    let view_set = build_policy_set(restrictions.clone(), stats.as_ref(), PolicyAction::View);
    let modify_set = build_policy_set(restrictions, stats.as_ref(), PolicyAction::Modify);

    // Check if this is a root policy (unrestricted access).
    //
    // is_root = true ONLY when no explicit policy inputs (identity / policy-class / policy)
    // were provided. When an identity IS specified but has no matching policies, is_root must
    // be false so that `default_allow` (not a blanket bypass) governs access.
    let has_explicit_policy_input = opts.identity.is_some()
        || opts.policy_class.as_ref().is_some_and(|v| !v.is_empty())
        || opts.policy.is_some();
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
        let range_opts = RangeOptions::default().with_flake_limit(1);
        for &g_id in policy_graphs {
            let db = GraphDbRef::new(snapshot, g_id, overlay, to_t);
            let exists = db
                .range_with_opts(
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject(identity_sid.clone()),
                    range_opts.clone(),
                )
                .await
                .map_err(|e| ApiError::internal(format!("identity existence check failed: {e}")))?;
            if !exists.is_empty() {
                return Ok(IdentityLookupResult::FoundNoPolicies { identity_sid });
            }
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
async fn load_policy_restriction(
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
    let mut policy_query_json: Option<String> = None;

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

    // f:action - collect all action values to determine View, Modify, or Both
    let action: Option<PolicyAction> = {
        let action_sid = resolve_system_iri_to_sid(snapshot, policy_iris::ACTION, "f:action")?;
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
        for binding in bindings {
            if let Some(action_ref) = binding.as_sid() {
                if &view_sid == action_ref {
                    has_view = true;
                } else if &modify_sid == action_ref {
                    has_modify = true;
                }
            }
        }
        match (has_view, has_modify) {
            (true, true) => Some(PolicyAction::Both),
            (true, false) => Some(PolicyAction::View),
            (false, true) => Some(PolicyAction::Modify),
            (false, false) => None,
        }
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
                    policy_query_json = Some(s.clone());
                    break;
                }
                Binding::Lit {
                    val: FlakeValue::String(s),
                    ..
                } => {
                    policy_query_json = Some(s.clone());
                    break;
                }
                _ => {}
            }
        }
    }

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
            if let Some(query_json) = policy_query_json {
                // Store raw policy query JSON. Parsing/lowering is handled by the query engine.
                // We still validate that it's valid JSON to preserve previous "deny on parse error"
                // behavior without duplicating query parsing logic.
                match serde_json::from_str::<JsonValue>(&query_json) {
                    Ok(_) => PolicyValue::Query(PolicyQuery { json: query_json }),
                    Err(e) => {
                        tracing::warn!(
                            "Policy '{}': failed to parse f:query JSON, defaulting to deny: {}",
                            policy_id,
                            e
                        );
                        PolicyValue::Deny // Fall back to deny on parse error
                    }
                }
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
        //
        // `@json` values can use object `@value` (not just string).
        let policy_query_json: Option<String> = obj
            .get("f:query")
            .or_else(|| obj.get(&format!("{}query", fluree::DB)))
            .and_then(|v| match v {
                JsonValue::String(s) => Some(s.clone()),
                JsonValue::Object(o) => {
                    // Handle @json typed values
                    let inner = o.get("@value")?;
                    match inner {
                        // @value is a string (already serialized JSON)
                        JsonValue::String(s) => Some(s.clone()),
                        // @value is an object (needs serialization)
                        JsonValue::Object(_) | JsonValue::Array(_) => {
                            serde_json::to_string(inner).ok()
                        }
                        _ => None,
                    }
                }
                _ => None,
            });

        // Extract f:action - can be string, object with @id, or array of these
        let action_value = obj
            .get("f:action")
            .or_else(|| obj.get(&format!("{}action", fluree::DB)));

        let action = parse_action_value(action_value);

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
                if let Some(query_json) = policy_query_json {
                    match serde_json::from_str::<JsonValue>(&query_json) {
                        Ok(_) => PolicyValue::Query(PolicyQuery { json: query_json }),
                        Err(e) => {
                            tracing::warn!("Failed to parse inline policy query JSON: {}", e);
                            PolicyValue::Deny
                        }
                    }
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

/// Parse f:action value into PolicyAction.
///
/// Handles multiple formats:
/// - String: "f:view", "f:modify", or full IRI
/// - Object with @id: {"@id": "f:view"}
/// - Array of the above: [{"@id": "f:view"}, {"@id": "f:modify"}]
///
/// Returns PolicyAction::Both if both view and modify are specified or if
/// the value cannot be parsed.
fn parse_action_value(value: Option<&JsonValue>) -> PolicyAction {
    let value = match value {
        Some(v) => v,
        None => return PolicyAction::Both,
    };

    // Collect all action IRIs from the value
    let action_strs = extract_action_strings(value);

    let mut has_view = false;
    let mut has_modify = false;

    for s in action_strs {
        if s.contains("view") {
            has_view = true;
        }
        if s.contains("modify") {
            has_modify = true;
        }
    }

    match (has_view, has_modify) {
        (true, true) => PolicyAction::Both,
        (true, false) => PolicyAction::View,
        (false, true) => PolicyAction::Modify,
        (false, false) => PolicyAction::Both, // Default if no recognized action
    }
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
        assert_eq!(parse_action_value(None), PolicyAction::Both);
    }

    #[test]
    fn test_parse_action_string_view() {
        let v = serde_json::json!("f:view");
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::View);
    }

    #[test]
    fn test_parse_action_string_modify() {
        let v = serde_json::json!("f:modify");
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_object_view() {
        let v = serde_json::json!({"@id": "f:view"});
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::View);
    }

    #[test]
    fn test_parse_action_object_modify() {
        let v = serde_json::json!({"@id": "https://ns.flur.ee/db#modify"});
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_array_view_only() {
        let v = serde_json::json!([{"@id": "f:view"}]);
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::View);
    }

    #[test]
    fn test_parse_action_array_modify_only() {
        let v = serde_json::json!([{"@id": "f:modify"}]);
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_array_both() {
        let v = serde_json::json!([{"@id": "f:view"}, {"@id": "f:modify"}]);
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Both);
    }

    #[test]
    fn test_parse_action_array_full_iris() {
        let v = serde_json::json!([
            {"@id": "https://ns.flur.ee/db#view"},
            {"@id": "https://ns.flur.ee/db#modify"}
        ]);
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Both);
    }
}
