//! Policy evaluation logic
//!
//! This module implements the core policy evaluation:
//! - **Deny Overrides** combining: if any matching policy denies, access is denied
//! - Required subset filtering AFTER class-applicability filter
//! - Schema flake bypass (schema flakes always allowed)
//! - Identity grounding via random never-match IRI
//! - Targeted `f:query` policies: if query returns false, deny (don't fall through)

use crate::query_eval::PolicyQueryExecutor;
use crate::schema::is_schema_flake;
use crate::types::{
    FlakePolicyEntry, PolicyDecision, PolicyRestriction, PolicySet, PolicyValue, PolicyWrapper,
    TargetMode,
};
use crate::Result;
use fluree_db_core::{FlakeValue, Sid, Tracker};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// Parameters for flake policy evaluation.
///
/// Bundles the flake-related parameters to reduce argument count in evaluation methods.
#[derive(Debug, Clone, Copy)]
struct FlakeEvalParams<'a> {
    /// The flake's subject SID
    subject: &'a Sid,
    /// The flake's property SID
    property: &'a Sid,
    /// The flake's object value
    object: &'a FlakeValue,
    /// Classes the subject belongs to (for class policy checks)
    subject_classes: &'a [Sid],
}

impl<'a> FlakeEvalParams<'a> {
    /// Create new flake evaluation parameters.
    fn new(
        subject: &'a Sid,
        property: &'a Sid,
        object: &'a FlakeValue,
        subject_classes: &'a [Sid],
    ) -> Self {
        Self {
            subject,
            property,
            object,
            subject_classes,
        }
    }
}

/// Policy context for evaluation
///
/// Holds the policy wrapper, grounded identity, and class cache.
/// Designed to be shareable across concurrent query executions.
#[derive(Debug, Clone)]
pub struct PolicyContext {
    /// The policy wrapper containing view and modify policy sets
    pub wrapper: PolicyWrapper,
    /// The grounded identity (always has a value, even if random)
    pub identity: Sid,
    /// Cache of subject -> classes for runtime class membership checks
    class_cache: Arc<RwLock<std::collections::HashMap<Sid, Vec<Sid>>>>,
}

impl PolicyContext {
    /// Create a new policy context with optional identity
    ///
    /// If identity is None, a random never-match IRI is generated.
    pub fn new(wrapper: PolicyWrapper, identity: Option<Sid>) -> Self {
        let grounded_identity = ensure_ground_identity(identity);
        Self {
            wrapper,
            identity: grounded_identity,
            class_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Get the policy wrapper
    pub fn wrapper(&self) -> &PolicyWrapper {
        &self.wrapper
    }

    /// Check if a view flake is allowed by policy
    ///
    /// This method uses the NoOp evaluator for f:query policies, which cannot
    /// execute async queries. For full f:query support, use the async method
    /// `allow_view_flake_async` or `allow_view_flake_with_evaluator`.
    ///
    /// # Arguments
    ///
    /// * `subject` - The flake's subject SID
    /// * `property` - The flake's property SID
    /// * `object` - The flake's object value
    /// * `subject_classes` - Classes the subject belongs to (for class policy checks)
    ///
    /// # Returns
    ///
    /// `Ok(true)` if allowed, `Ok(false)` if denied
    pub fn allow_view_flake(
        &self,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
    ) -> Result<bool> {
        let tracker = Tracker::disabled();
        self.evaluate_flake(
            self.wrapper.view(),
            subject,
            property,
            object,
            subject_classes,
            &tracker,
        )
    }

    /// Check if a view flake is allowed by policy, with execution tracking.
    pub fn allow_view_flake_tracked(
        &self,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
        tracker: &Tracker,
    ) -> Result<bool> {
        self.evaluate_flake(
            self.wrapper.view(),
            subject,
            property,
            object,
            subject_classes,
            tracker,
        )
    }

    // Note: The old allow_view_flake_with_evaluator methods have been removed.
    // Use allow_view_flake_async for full f:query support.
    // The sync methods (allow_view_flake, allow_view_flake_tracked) only support Allow/Deny.

    /// Check if a view flake is allowed by policy (async with full f:query support)
    ///
    /// This is the primary method for policy evaluation in async contexts.
    /// It supports `f:query` policies by using the provided `PolicyQueryExecutor`.
    ///
    /// # Arguments
    ///
    /// * `subject` - The flake's subject SID
    /// * `property` - The flake's property SID
    /// * `object` - The flake's object value
    /// * `subject_classes` - Classes the subject belongs to (for class policy checks)
    /// * `executor` - Async query executor for f:query policies
    /// * `tracker` - Execution tracker
    ///
    /// # Returns
    ///
    /// `Ok(true)` if allowed, `Ok(false)` if denied
    pub async fn allow_view_flake_async(
        &self,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
        executor: &dyn PolicyQueryExecutor,
        tracker: &Tracker,
    ) -> Result<bool> {
        let flake = FlakeEvalParams::new(subject, property, object, subject_classes);
        self.evaluate_flake_async(self.wrapper.view(), flake, executor, tracker)
            .await
    }

    /// Check if a modify flake is allowed by policy (async with full f:query support)
    pub async fn allow_modify_flake_async(
        &self,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
        executor: &dyn PolicyQueryExecutor,
        tracker: &Tracker,
    ) -> Result<bool> {
        let flake = FlakeEvalParams::new(subject, property, object, subject_classes);
        self.evaluate_flake_async(self.wrapper.modify(), flake, executor, tracker)
            .await
    }

    /// Check if a modify flake is allowed by policy with detailed result
    ///
    /// Returns `PolicyDecision` which includes either the winning restriction on allow,
    /// or the candidate restrictions on denial (for error message extraction).
    pub async fn allow_modify_flake_async_detailed<'a>(
        &'a self,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
        executor: &dyn PolicyQueryExecutor,
        tracker: &Tracker,
    ) -> Result<PolicyDecision<'a>> {
        let flake = FlakeEvalParams::new(subject, property, object, subject_classes);
        self.evaluate_flake_async_detailed(self.wrapper.modify(), flake, executor, tracker)
            .await
    }

    /// Check if a view flake is allowed by policy with detailed result
    ///
    /// Returns `PolicyDecision` which includes either the winning restriction on allow,
    /// or the candidate restrictions on denial.
    pub async fn allow_view_flake_async_detailed<'a>(
        &'a self,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
        executor: &dyn PolicyQueryExecutor,
        tracker: &Tracker,
    ) -> Result<PolicyDecision<'a>> {
        let flake = FlakeEvalParams::new(subject, property, object, subject_classes);
        self.evaluate_flake_async_detailed(self.wrapper.view(), flake, executor, tracker)
            .await
    }

    /// Check if a modify flake is allowed by policy
    ///
    /// This method uses the NoOp evaluator for f:query policies, which cannot
    /// execute async queries. For full f:query support, use the async method
    /// `allow_modify_flake_async` or `allow_modify_flake_with_evaluator`.
    ///
    /// # Arguments
    ///
    /// * `subject` - The flake's subject SID
    /// * `property` - The flake's property SID
    /// * `object` - The flake's object value
    /// * `subject_classes` - Classes the subject belongs to
    ///
    /// # Returns
    ///
    /// `Ok(true)` if allowed, `Ok(false)` if denied
    pub fn allow_modify_flake(
        &self,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
    ) -> Result<bool> {
        let tracker = Tracker::disabled();
        self.evaluate_flake(
            self.wrapper.modify(),
            subject,
            property,
            object,
            subject_classes,
            &tracker,
        )
    }

    // Note: allow_modify_flake_with_evaluator has been removed.
    // Use allow_modify_flake_async for full f:query support.

    /// Evaluate a flake against a policy set (sync, no f:query support)
    ///
    /// NOTE: This sync version does NOT support f:query policies - they are
    /// treated as "continue to next policy". Use the async methods
    /// (allow_view_flake_async, allow_modify_flake_async) for full f:query support.
    ///
    /// Implements policy evaluation semantics:
    /// 1. Schema bypass first
    /// 2. Collect candidates in order (property -> subject -> default)
    /// 3. Filter by class applicability
    /// 4. Apply required subset filtering
    /// 5. Evaluate in order - return true on first allow
    /// 6. Return default_allow if no policy allowed
    fn evaluate_flake(
        &self,
        policy_set: &PolicySet,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
        tracker: &Tracker,
    ) -> Result<bool> {
        // Root policy bypasses all checks
        if self.wrapper.is_root() {
            return Ok(true);
        }

        // Schema flakes always allowed (before any policy evaluation)
        if is_schema_flake(property, object) {
            return Ok(true);
        }

        // 1. Collect all candidate policy entries (property -> subject -> default order)
        let candidate_entries = policy_set.policy_entries_for_flake(subject, property);

        // Convert subject_classes to HashSet for efficient lookup
        let subject_class_set: HashSet<&Sid> = subject_classes.iter().collect();

        // 2. Filter by class applicability
        let applicable_entries: Vec<FlakePolicyEntry> = candidate_entries
            .into_iter()
            .filter(|entry| {
                let r = &policy_set.restrictions[entry.idx];
                if r.class_policy && entry.class_check_needed {
                    r.for_classes.iter().any(|c| subject_class_set.contains(c))
                } else {
                    true
                }
            })
            .collect();

        // 3. Apply required subset filtering (AFTER class filtering)
        let has_required = applicable_entries
            .iter()
            .any(|entry| policy_set.restrictions[entry.idx].required);
        let filtered_entries: Vec<FlakePolicyEntry> = if has_required {
            applicable_entries
                .into_iter()
                .filter(|entry| policy_set.restrictions[entry.idx].required)
                .collect()
        } else {
            applicable_entries
        };

        // IMPORTANT parity: default_allow applies only when no policies apply.
        if filtered_entries.is_empty() {
            return Ok(self.wrapper.default_allow());
        }

        // 4. Evaluate with "Deny Overrides" semantics:
        //    If ANY applicable policy explicitly denies (f:allow: false), access is denied.
        //    Otherwise, if any policy allows (f:allow: true), access is granted.
        //
        // First pass: check for explicit Deny
        for entry in &filtered_entries {
            let restriction = &policy_set.restrictions[entry.idx];
            if matches!(restriction.value, PolicyValue::Deny) {
                tracker.policy_executed(&restriction.id);
                // Deny overrides - this policy denies access
                return Ok(false);
            }
        }

        // Second pass: check for Allow (no Deny found)
        for entry in filtered_entries {
            let restriction = &policy_set.restrictions[entry.idx];
            tracker.policy_executed(&restriction.id);
            match &restriction.value {
                PolicyValue::Allow => {
                    tracker.policy_allowed(&restriction.id);
                    return Ok(true);
                }
                PolicyValue::Deny => unreachable!(), // Already handled above
                PolicyValue::Query(_) => {
                    // Sync method cannot execute async f:query - skip to next policy
                    // Use allow_view_flake_async for f:query support
                    continue;
                }
            }
        }

        // 5. Policies applied, but none allowed -> deny
        Ok(false)
    }

    /// Async version of evaluate_flake that properly awaits f:query evaluation
    ///
    /// This is the core async policy evaluation method. It uses the same
    /// evaluation semantics as the sync version but awaits query execution.
    async fn evaluate_flake_async(
        &self,
        policy_set: &PolicySet,
        flake: FlakeEvalParams<'_>,
        executor: &dyn PolicyQueryExecutor,
        tracker: &Tracker,
    ) -> Result<bool> {
        // Root policy bypasses all checks
        if self.wrapper.is_root() {
            return Ok(true);
        }

        // Schema flakes always allowed (needed for query planning/formatting)
        if is_schema_flake(flake.property, flake.object) {
            return Ok(true);
        }

        // 1. Collect all candidate policy entries (property -> subject -> default order)
        let candidate_entries = policy_set.policy_entries_for_flake(flake.subject, flake.property);

        // Convert subject_classes to HashSet for efficient lookup
        let subject_class_set: HashSet<&Sid> = flake.subject_classes.iter().collect();

        // 2. Filter by class applicability
        let applicable_entries: Vec<FlakePolicyEntry> = candidate_entries
            .into_iter()
            .filter(|entry| {
                let r = &policy_set.restrictions[entry.idx];
                if r.class_policy && entry.class_check_needed {
                    r.for_classes.iter().any(|c| subject_class_set.contains(c))
                } else {
                    true
                }
            })
            .collect();

        // 3. Apply required subset filtering (AFTER class filtering)
        let has_required = applicable_entries
            .iter()
            .any(|entry| policy_set.restrictions[entry.idx].required);
        let filtered_entries: Vec<FlakePolicyEntry> = if has_required {
            applicable_entries
                .into_iter()
                .filter(|entry| policy_set.restrictions[entry.idx].required)
                .collect()
        } else {
            applicable_entries
        };

        // IMPORTANT parity: default_allow applies only when no policies apply.
        if filtered_entries.is_empty() {
            return Ok(self.wrapper.default_allow());
        }

        // 4. Evaluate with "Deny Overrides" semantics:
        //    If ANY applicable policy explicitly denies (f:allow: false), access is denied.
        //    Otherwise, if any policy allows (f:allow: true or f:query succeeds), access is granted.
        //
        // First pass: check for explicit Deny
        for entry in &filtered_entries {
            let restriction = &policy_set.restrictions[entry.idx];
            if matches!(restriction.value, PolicyValue::Deny) {
                tracker.policy_executed(&restriction.id);
                // Deny overrides - this policy denies access
                return Ok(false);
            }
        }

        // Second pass: check for Allow/Query (no Deny found)
        for entry in filtered_entries {
            let restriction = &policy_set.restrictions[entry.idx];
            tracker.policy_executed(&restriction.id);
            match &restriction.value {
                PolicyValue::Allow => {
                    tracker.policy_allowed(&restriction.id);
                    return Ok(true);
                }
                PolicyValue::Deny => unreachable!(), // Already handled above
                PolicyValue::Query(q) => {
                    // Build bindings for special variables + wrapper's policy_values
                    let bindings = build_policy_values_clause(
                        flake.subject,
                        &self.identity,
                        self.wrapper.policy_values(),
                    );
                    // Await the async query evaluation
                    if executor.evaluate_policy_query(q, &bindings).await? {
                        tracker.policy_allowed(&restriction.id);
                        return Ok(true);
                    }
                    // Query returned false.
                    // For targeted policies (OnProperty, OnSubject, OnClass), a failing
                    // query means access is denied for that target. For Default policies,
                    // continue to the next policy.
                    if matches!(
                        restriction.target_mode,
                        TargetMode::OnProperty | TargetMode::OnSubject | TargetMode::OnClass
                    ) {
                        return Ok(false);
                    }
                    continue;
                }
            }
        }

        // 5. Policies applied, but none allowed -> deny
        Ok(false)
    }

    /// Async policy evaluation with detailed result (returns PolicyDecision)
    ///
    /// This is the core async policy evaluation method that returns the winning restriction
    /// on allow, or the candidate restrictions on denial.
    async fn evaluate_flake_async_detailed<'a>(
        &'a self,
        policy_set: &'a PolicySet,
        flake: FlakeEvalParams<'_>,
        executor: &dyn PolicyQueryExecutor,
        tracker: &Tracker,
    ) -> Result<PolicyDecision<'a>> {
        // Root policy bypasses all checks
        if self.wrapper.is_root() {
            return Ok(PolicyDecision::Allowed { restriction: None });
        }

        // Schema flakes always allowed (needed for query planning/formatting)
        if is_schema_flake(flake.property, flake.object) {
            return Ok(PolicyDecision::Allowed { restriction: None });
        }

        // 1. Collect all candidate policy entries (property -> subject -> default order)
        let candidate_entries = policy_set.policy_entries_for_flake(flake.subject, flake.property);

        // Convert subject_classes to HashSet for efficient lookup
        let subject_class_set: HashSet<&Sid> = flake.subject_classes.iter().collect();

        // 2. Filter by class applicability
        let applicable_entries: Vec<FlakePolicyEntry> = candidate_entries
            .into_iter()
            .filter(|entry| {
                let r = &policy_set.restrictions[entry.idx];
                if r.class_policy && entry.class_check_needed {
                    r.for_classes.iter().any(|c| subject_class_set.contains(c))
                } else {
                    true
                }
            })
            .collect();

        // 3. Apply required subset filtering (AFTER class filtering)
        let has_required = applicable_entries
            .iter()
            .any(|entry| policy_set.restrictions[entry.idx].required);
        let filtered_entries: Vec<FlakePolicyEntry> = if has_required {
            applicable_entries
                .into_iter()
                .filter(|entry| policy_set.restrictions[entry.idx].required)
                .collect()
        } else {
            applicable_entries
        };

        // IMPORTANT parity: default_allow applies only when no policies apply.
        if filtered_entries.is_empty() {
            return if self.wrapper.default_allow() {
                Ok(PolicyDecision::Allowed { restriction: None })
            } else {
                Ok(PolicyDecision::Denied { candidates: vec![] })
            };
        }

        // Collect candidate restrictions for potential denial message
        let candidate_restrictions: Vec<&'a PolicyRestriction> = filtered_entries
            .iter()
            .map(|entry| &policy_set.restrictions[entry.idx])
            .collect();

        // 4. Evaluate with "Deny Overrides" semantics
        //
        // First pass: check for explicit Deny
        for entry in &filtered_entries {
            let restriction = &policy_set.restrictions[entry.idx];
            if matches!(restriction.value, PolicyValue::Deny) {
                tracker.policy_executed(&restriction.id);
                // Deny overrides - return with the denying restriction as candidate
                return Ok(PolicyDecision::Denied {
                    candidates: vec![restriction],
                });
            }
        }

        // Second pass: check for Allow/Query (no Deny found)
        for entry in &filtered_entries {
            let restriction = &policy_set.restrictions[entry.idx];
            tracker.policy_executed(&restriction.id);
            match &restriction.value {
                PolicyValue::Allow => {
                    tracker.policy_allowed(&restriction.id);
                    return Ok(PolicyDecision::Allowed {
                        restriction: Some(restriction),
                    });
                }
                PolicyValue::Deny => unreachable!(), // Already handled above
                PolicyValue::Query(q) => {
                    // Build bindings for special variables + wrapper's policy_values
                    let bindings = build_policy_values_clause(
                        flake.subject,
                        &self.identity,
                        self.wrapper.policy_values(),
                    );
                    // Await the async query evaluation
                    if executor.evaluate_policy_query(q, &bindings).await? {
                        tracker.policy_allowed(&restriction.id);
                        return Ok(PolicyDecision::Allowed {
                            restriction: Some(restriction),
                        });
                    }
                    // Query returned false.
                    // For targeted policies, a failing query means access denied.
                    if matches!(
                        restriction.target_mode,
                        TargetMode::OnProperty | TargetMode::OnSubject | TargetMode::OnClass
                    ) {
                        return Ok(PolicyDecision::Denied {
                            candidates: vec![restriction],
                        });
                    }
                    continue;
                }
            }
        }

        // 5. Policies applied, but none allowed -> deny with candidates
        Ok(PolicyDecision::Denied {
            candidates: candidate_restrictions,
        })
    }

    /// Evaluate a modify flake and return both the result and evaluated restrictions
    ///
    /// Used for modify enforcement where we need the restrictions for error messages.
    /// Evaluate a modify flake and return both the result and evaluated restrictions (sync, no f:query)
    ///
    /// Used for modify enforcement where we need the restrictions for error messages.
    /// NOTE: f:query policies are skipped - use async version for full support.
    pub fn evaluate_modify_flake_with_candidates<'a>(
        &'a self,
        subject: &Sid,
        property: &Sid,
        object: &FlakeValue,
        subject_classes: &[Sid],
    ) -> Result<(bool, Vec<&'a PolicyRestriction>)> {
        // Root policy bypasses all checks
        if self.wrapper.is_root() {
            return Ok((true, vec![]));
        }

        // Schema flakes always allowed
        if is_schema_flake(property, object) {
            return Ok((true, vec![]));
        }

        let policy_set = self.wrapper.modify();
        let candidate_entries = policy_set.policy_entries_for_flake(subject, property);
        let subject_class_set: HashSet<&Sid> = subject_classes.iter().collect();

        // Filter by class applicability using per-property class_check_needed
        let applicable_entries: Vec<FlakePolicyEntry> = candidate_entries
            .into_iter()
            .filter(|entry| {
                let r = &policy_set.restrictions[entry.idx];
                // Use per-property class_check_needed for the optimization
                if r.class_policy && entry.class_check_needed {
                    r.for_classes.iter().any(|c| subject_class_set.contains(c))
                } else {
                    true
                }
            })
            .collect();

        let has_required = applicable_entries
            .iter()
            .any(|entry| policy_set.restrictions[entry.idx].required);
        let filtered_entries: Vec<FlakePolicyEntry> = if has_required {
            applicable_entries
                .into_iter()
                .filter(|entry| policy_set.restrictions[entry.idx].required)
                .collect()
        } else {
            applicable_entries
        };

        // If no policies apply, fall back to default_allow.
        if filtered_entries.is_empty() {
            return Ok((self.wrapper.default_allow(), vec![]));
        }

        // Evaluate with "Deny Overrides" semantics
        //
        // First pass: check for explicit Deny
        for entry in &filtered_entries {
            let restriction = &policy_set.restrictions[entry.idx];
            if matches!(restriction.value, PolicyValue::Deny) {
                // Deny overrides - return with the denying restriction
                return Ok((false, vec![restriction]));
            }
        }

        // Second pass: check for Allow (no Deny found)
        // NOTE: f:query policies are skipped in this sync version.
        for entry in &filtered_entries {
            let restriction = &policy_set.restrictions[entry.idx];
            match &restriction.value {
                PolicyValue::Allow => return Ok((true, vec![])),
                PolicyValue::Deny => unreachable!(), // Already handled above
                PolicyValue::Query(_) => {
                    // Sync method cannot execute async f:query - skip to next policy
                    continue;
                }
            }
        }

        // Policies applied, but none allowed -> deny. Return evaluated candidates for message.
        let evaluated: Vec<&PolicyRestriction> = filtered_entries
            .into_iter()
            .map(|entry| &policy_set.restrictions[entry.idx])
            .collect();
        Ok((false, evaluated))
    }

    /// Cache subject classes for repeated lookups
    pub fn cache_subject_classes(&self, subject: Sid, classes: Vec<Sid>) {
        if let Ok(mut cache) = self.class_cache.write() {
            cache.insert(subject, classes);
        }
    }

    /// Get cached subject classes
    pub fn get_cached_subject_classes(&self, subject: &Sid) -> Option<Vec<Sid>> {
        self.class_cache
            .read()
            .ok()
            .and_then(|cache| cache.get(subject).cloned())
    }
}

/// Marker prefix for unbound identity SIDs.
///
/// This prefix is used to identify identity SIDs that were auto-generated because
/// no identity was provided. The policy_eval code can check for this prefix
/// to handle unbound identities specially.
pub const UNBOUND_IDENTITY_PREFIX: &str = "urn:fluree:unbound:identity:";

/// Ensure identity is always ground (bound) in policy queries.
///
/// If no identity is provided, generates a random IRI that can never exist in real data.
/// Uses UUID to guarantee uniqueness across queries.
///
/// The generated IRI uses a URN format (`urn:fluree:unbound:identity:UUID`) that:
/// 1. Is a valid IRI (so it can be encoded/decoded properly)
/// 2. Will never match real data in the database
/// 3. Can be detected by the UNBOUND_IDENTITY_PREFIX constant
fn ensure_ground_identity(identity: Option<Sid>) -> Sid {
    identity.unwrap_or_else(|| {
        // Generate a random URN that can never exist in real data
        // Using URN format ensures it's a valid IRI
        let random_iri = format!("{}{}", UNBOUND_IDENTITY_PREFIX, Uuid::new_v4());
        // Use namespace code 0 with the full URN as the local name
        Sid::new(0, random_iri)
    })
}

/// Build values clause for policy query execution.
///
/// ALWAYS includes ?$identity binding to ensure it's ground.
/// Also includes any user-provided policy_values from the wrapper.
pub fn build_policy_values_clause(
    subject: &Sid,
    identity: &Sid,
    wrapper_policy_values: &std::collections::HashMap<String, Sid>,
) -> std::collections::HashMap<String, Sid> {
    // Start with wrapper's policy values (user-provided bindings)
    let mut values = wrapper_policy_values.clone();
    // ?$this and ?$identity always override/supplement wrapper values
    values.insert("?$this".to_string(), subject.clone());
    values.insert("?$identity".to_string(), identity.clone()); // ALWAYS ground
    values
}

/// Filter restrictions by required flag.
///
/// If ANY required candidate exists, ignore ALL non-required candidates.
/// Preserves insertion order of the remaining candidates.
pub fn filter_by_required(candidates: Vec<&PolicyRestriction>) -> Vec<&PolicyRestriction> {
    let has_required = candidates.iter().any(|c| c.required);
    if has_required {
        candidates.into_iter().filter(|c| c.required).collect()
    } else {
        candidates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PolicyAction, PolicySet, PropertyPolicyEntry, TargetMode};
    use fluree_vocab::namespaces::RDFS;

    fn make_sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn make_allow_restriction(id: &str, property: Sid) -> PolicyRestriction {
        PolicyRestriction {
            id: id.to_string(),
            target_mode: TargetMode::OnProperty,
            targets: [property].into_iter().collect(),
            action: PolicyAction::View,
            value: PolicyValue::Allow,
            required: false,
            message: None,
            class_policy: false,
            for_classes: HashSet::new(),
            class_check_needed: false,
        }
    }

    fn make_deny_restriction(id: &str, property: Sid) -> PolicyRestriction {
        let mut r = make_allow_restriction(id, property);
        r.value = PolicyValue::Deny;
        r
    }

    #[test]
    fn test_ensure_ground_identity_with_identity() {
        let identity = make_sid(100, "alice");
        let result = ensure_ground_identity(Some(identity.clone()));
        assert_eq!(result, identity);
    }

    #[test]
    fn test_ensure_ground_identity_without_identity() {
        let result = ensure_ground_identity(None);
        // Should have namespace code 0 and random UUID-based name
        assert_eq!(result.namespace_code, 0);
        assert!(result.name.as_ref().starts_with(UNBOUND_IDENTITY_PREFIX));
    }

    #[test]
    fn test_filter_by_required_no_required() {
        let r1 = make_allow_restriction("r1", make_sid(100, "name"));
        let r2 = make_deny_restriction("r2", make_sid(100, "age"));
        let candidates = vec![&r1, &r2];

        let filtered = filter_by_required(candidates);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_filter_by_required_with_required() {
        let r1 = make_allow_restriction("r1", make_sid(100, "name"));
        let mut r2 = make_deny_restriction("r2", make_sid(100, "age"));
        r2.required = true;

        let candidates = vec![&r1, &r2];
        let filtered = filter_by_required(candidates);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "r2");
    }

    #[test]
    fn test_build_policy_values_clause() {
        let subject = make_sid(100, "alice");
        let identity = make_sid(100, "bob");
        let wrapper_values = std::collections::HashMap::new();

        let values = build_policy_values_clause(&subject, &identity, &wrapper_values);

        assert_eq!(values.get("?$this"), Some(&subject));
        assert_eq!(values.get("?$identity"), Some(&identity));
    }

    #[test]
    fn test_build_policy_values_clause_with_wrapper_values() {
        let subject = make_sid(100, "alice");
        let identity = make_sid(100, "bob");
        let custom_var = make_sid(100, "custom_value");

        let mut wrapper_values = std::collections::HashMap::new();
        wrapper_values.insert("?myVar".to_string(), custom_var.clone());

        let values = build_policy_values_clause(&subject, &identity, &wrapper_values);

        // Should include ?$this and ?$identity
        assert_eq!(values.get("?$this"), Some(&subject));
        assert_eq!(values.get("?$identity"), Some(&identity));
        // Should also include wrapper's policy values
        assert_eq!(values.get("?myVar"), Some(&custom_var));
    }

    #[test]
    fn test_build_policy_values_clause_override() {
        // Test that ?$this and ?$identity override any wrapper values with same names
        let subject = make_sid(100, "alice");
        let identity = make_sid(100, "bob");
        let wrong_identity = make_sid(100, "wrong");

        let mut wrapper_values = std::collections::HashMap::new();
        wrapper_values.insert("?$identity".to_string(), wrong_identity);

        let values = build_policy_values_clause(&subject, &identity, &wrapper_values);

        // ?$identity should be the correct one, not the wrapper's
        assert_eq!(values.get("?$identity"), Some(&identity));
    }

    #[test]
    fn test_policy_context_root_always_allows() {
        let wrapper = PolicyWrapper::root();
        let ctx = PolicyContext::new(wrapper, None);

        let result = ctx
            .allow_view_flake(
                &make_sid(100, "alice"),
                &make_sid(100, "ssn"),
                &FlakeValue::String("123-45-6789".to_string()),
                &[],
            )
            .unwrap();

        assert!(result);
    }

    #[test]
    fn test_policy_context_deny_overrides_allow() {
        // Create policy set with deny first, then allow
        // Tests "Deny Overrides" combining algorithm: if any policy denies, access is denied
        let mut set = PolicySet::new();

        let deny = make_deny_restriction("deny", make_sid(100, "name"));
        let allow = make_allow_restriction("allow", make_sid(100, "name"));

        set.restrictions.push(deny);
        set.by_property
            .entry(make_sid(100, "name"))
            .or_default()
            .push(PropertyPolicyEntry {
                idx: 0,
                class_check_needed: false,
            });

        set.restrictions.push(allow);
        set.by_property
            .entry(make_sid(100, "name"))
            .or_default()
            .push(PropertyPolicyEntry {
                idx: 1,
                class_check_needed: false,
            });

        let wrapper = PolicyWrapper::new(
            set,
            PolicySet::new(),
            false,
            false, // default deny
            std::collections::HashMap::new(),
        );

        let ctx = PolicyContext::new(wrapper, None);

        // With "Deny Overrides" combining: Deny wins even if Allow also exists
        let result = ctx
            .allow_view_flake(
                &make_sid(100, "alice"),
                &make_sid(100, "name"),
                &FlakeValue::String("Alice".to_string()),
                &[],
            )
            .unwrap();

        assert!(!result); // Deny overrides allow
    }

    #[test]
    fn test_policy_context_default_deny() {
        // Create policy set with no matching policies
        let set = PolicySet::new();

        let wrapper = PolicyWrapper::new(
            set,
            PolicySet::new(),
            false,
            false, // default deny
            std::collections::HashMap::new(),
        );

        let ctx = PolicyContext::new(wrapper, None);

        let result = ctx
            .allow_view_flake(
                &make_sid(100, "alice"),
                &make_sid(100, "name"),
                &FlakeValue::String("Alice".to_string()),
                &[],
            )
            .unwrap();

        assert!(!result); // Default is deny
    }

    #[test]
    fn test_schema_flake_always_allowed() {
        // Create deny-all policy
        let mut set = PolicySet::new();
        let mut deny_all = make_deny_restriction("deny-all", make_sid(4, "subClassOf"));
        deny_all.target_mode = TargetMode::Default;
        deny_all.targets.clear();
        set.restrictions.push(deny_all);
        set.defaults.push(0);

        let wrapper = PolicyWrapper::new(
            set,
            PolicySet::new(),
            false,
            false, // default deny
            std::collections::HashMap::new(),
        );

        let ctx = PolicyContext::new(wrapper, None);

        // rdfs:subClassOf should be allowed regardless of policy
        let result = ctx
            .allow_view_flake(
                &make_sid(100, "Person"),
                &make_sid(RDFS, "subClassOf"),
                &FlakeValue::Ref(make_sid(100, "Thing")),
                &[],
            )
            .unwrap();

        assert!(result);
    }
}
