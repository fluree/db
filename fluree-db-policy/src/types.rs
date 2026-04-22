//! Core policy types
//!
//! This module defines the fundamental types for policy enforcement:
//! - `PolicyRestriction`: An individual policy rule
//! - `PolicyValue`: Allow, Deny, or conditional Query
//! - `PolicySet`: Indexed collection of restrictions
//! - `PolicyWrapper`: Container with view and modify policy sets

use fluree_db_core::Sid;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Policy query for conditional evaluation
///
/// Represents a query that determines if a policy allows access.
/// If the query returns any results, access is granted.
///
/// IMPORTANT: This stores the raw JSON query (typically as a string stored in the ledger
/// as an `@json` value). Parsing/lowering is delegated to the query engine to avoid
/// duplicating query parsing logic inside the policy system (and to support FILTER, etc.).
#[derive(Debug, Clone)]
pub struct PolicyQuery {
    /// JSON query payload (string containing a JSON object)
    pub json: String,
}

/// Target mode for a policy restriction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetMode {
    /// Targets specific subjects (f:onSubject)
    OnSubject,
    /// Targets specific properties (f:onProperty)
    OnProperty,
    /// Targets instances of classes (f:onClass)
    OnClass,
    /// Default policy (applies when no specific target matches)
    Default,
}

/// Policy action - which operations this policy applies to
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PolicyAction {
    /// View (query) operations only
    View,
    /// Modify (transaction) operations only
    Modify,
    /// Both view and modify operations
    #[default]
    Both,
}

/// Result of detailed policy evaluation
///
/// This enum provides access to the winning restriction on allow,
/// or the candidate set on denial, avoiding the need for a second
/// evaluation pass to extract error messages.
///
/// # Semantics
///
/// - `Allowed { restriction: Some(_) }`: A specific policy granted access
/// - `Allowed { restriction: None }`: Root/schema bypass (no policy evaluated)
/// - `Denied { candidates: [...] }`: One or more policies were evaluated but denied access
/// - `Denied { candidates: [] }`: **Default deny** - no policies applied to this flake
///   and `default_allow` is `false`. This is distinct from "policies evaluated and denied".
#[derive(Debug)]
pub enum PolicyDecision<'a> {
    /// Access allowed - optionally includes the winning restriction
    Allowed {
        /// The restriction that granted access (None for root/schema bypass)
        restriction: Option<&'a PolicyRestriction>,
    },
    /// Access denied - includes the candidate restrictions that were evaluated
    ///
    /// Note: An empty `candidates` vec means "default deny" (no policies applied),
    /// not "policies were evaluated and denied".
    Denied {
        /// The restrictions that were evaluated but didn't allow access
        /// (useful for extracting error messages)
        candidates: Vec<&'a PolicyRestriction>,
    },
}

impl PolicyDecision<'_> {
    /// Returns true if access was allowed
    pub fn is_allowed(&self) -> bool {
        matches!(self, PolicyDecision::Allowed { .. })
    }

    /// Returns true if access was denied
    pub fn is_denied(&self) -> bool {
        matches!(self, PolicyDecision::Denied { .. })
    }

    /// Get the first `f:exMessage` from the candidate restrictions, if any.
    ///
    /// Returns `None` if:
    /// - The decision is `Allowed`
    /// - The decision is `Denied` but no candidates have a message
    /// - The decision is `Denied` with empty candidates (default deny)
    ///
    /// Callers should provide their own action-specific default message
    /// when this returns `None` (e.g., "Policy enforcement prevents modification.").
    pub fn deny_message(&self) -> Option<&str> {
        match self {
            PolicyDecision::Denied { candidates } => candidates
                .iter()
                .filter_map(|r| r.message.as_deref())
                .next(),
            PolicyDecision::Allowed { .. } => None,
        }
    }
}

/// Policy value - the effect of the policy
#[derive(Debug, Clone)]
pub enum PolicyValue {
    /// Allow access unconditionally
    Allow,
    /// Deny access (but don't short-circuit - try next policy)
    Deny,
    /// Conditional access based on query result
    Query(PolicyQuery),
}

/// An individual policy restriction (rule)
#[derive(Debug, Clone)]
pub struct PolicyRestriction {
    /// Policy identifier (for tracking/debugging)
    pub id: String,
    /// Target mode (OnSubject, OnProperty, OnClass, Default)
    pub target_mode: TargetMode,
    /// Resolved target SIDs (HashSet for O(1) lookup)
    pub targets: HashSet<Sid>,
    /// Action (View, Modify, Both)
    pub action: PolicyAction,
    /// Policy value (Allow, Deny, Query)
    pub value: PolicyValue,
    /// Required flag (for subset filtering)
    pub required: bool,
    /// Custom error message (f:exMessage)
    pub message: Option<String>,

    // For f:onClass (indexed into by_property)
    /// True if this restriction comes from f:onClass
    pub class_policy: bool,
    /// Class SIDs this targets (HashSet for subset checks)
    pub for_classes: HashSet<Sid>,
    /// True if runtime class membership check is needed
    pub class_check_needed: bool,
}

/// Entry in the property index
///
/// For class policies, `class_check_needed` indicates whether runtime class
/// membership checking is required for this specific property. This varies
/// by property because:
/// - Exclusive properties (only used by target classes) don't need checks
/// - Shared properties (used by other classes too) need checks
/// - Implicit properties (@id, rdf:type) always need checks
#[derive(Debug, Clone, Copy)]
pub struct PropertyPolicyEntry {
    /// Index into the restrictions vector
    pub idx: usize,
    /// Whether class membership check is needed for THIS property
    /// (only meaningful for class policies)
    pub class_check_needed: bool,
}

/// Indexed policy set
///
/// Class policies are indexed INTO by_property (not a separate by_class index).
/// This is designed for efficient lookup.
#[derive(Debug, Default)]
pub struct PolicySet {
    /// All restrictions in parse order (insertion order preserved)
    pub restrictions: Vec<PolicyRestriction>,
    /// Index: subject SID -> restriction indices
    pub by_subject: HashMap<Sid, Vec<usize>>,
    /// Index: property SID -> property policy entries (includes class policies!)
    /// Each entry includes whether class check is needed for that specific property
    pub by_property: HashMap<Sid, Vec<PropertyPolicyEntry>>,
    /// Default-bucket policy indices
    pub defaults: Vec<usize>,
}

/// Entry returned when looking up policies for a flake
///
/// Contains the restriction index and per-property class_check_needed flag.
#[derive(Debug, Clone, Copy)]
pub struct FlakePolicyEntry {
    /// Index into the restrictions vector
    pub idx: usize,
    /// Whether class membership check is needed for THIS property
    /// (only meaningful for class policies)
    pub class_check_needed: bool,
}

impl PolicySet {
    /// Create an empty policy set
    pub fn new() -> Self {
        Self::default()
    }

    /// Get candidate restrictions for a flake
    ///
    /// Order: property-specific -> subject-specific -> defaults
    /// Preserves insertion order within each bucket.
    pub fn restrictions_for_flake(&self, subject: &Sid, property: &Sid) -> Vec<&PolicyRestriction> {
        let mut candidates = Vec::new();

        // 1. Property-specific (includes class policies mapped here)
        if let Some(entries) = self.by_property.get(property) {
            for entry in entries {
                candidates.push(&self.restrictions[entry.idx]);
            }
        }

        // 2. Subject-specific (preserve insertion order)
        if let Some(indices) = self.by_subject.get(subject) {
            for &idx in indices {
                candidates.push(&self.restrictions[idx]);
            }
        }

        // 3. Default-bucket policies (preserve insertion order)
        for &idx in &self.defaults {
            candidates.push(&self.restrictions[idx]);
        }

        candidates
    }

    /// Get candidate policy entries for a flake with per-property class_check_needed info.
    ///
    /// Order: property-specific -> subject-specific -> defaults.
    /// Preserves insertion order within each bucket.
    ///
    /// Returns `FlakePolicyEntry` which includes:
    /// - `idx`: restriction index
    /// - `class_check_needed`: whether class membership check is needed for THIS property
    ///
    /// For non-class policies (property, subject, default), `class_check_needed` is always false
    /// since they don't need class membership verification.
    pub fn policy_entries_for_flake(&self, subject: &Sid, property: &Sid) -> Vec<FlakePolicyEntry> {
        let mut candidates = Vec::new();

        // 1. Property-specific (includes class policies mapped here)
        // Uses per-property class_check_needed
        if let Some(entries) = self.by_property.get(property) {
            for entry in entries {
                candidates.push(FlakePolicyEntry {
                    idx: entry.idx,
                    class_check_needed: entry.class_check_needed,
                });
            }
        }

        // 2. Subject-specific (never need class check)
        if let Some(indices) = self.by_subject.get(subject) {
            for &idx in indices {
                candidates.push(FlakePolicyEntry {
                    idx,
                    class_check_needed: false,
                });
            }
        }

        // 3. Default-bucket policies (don't need class check)
        for &idx in &self.defaults {
            candidates.push(FlakePolicyEntry {
                idx,
                class_check_needed: false,
            });
        }

        candidates
    }
}

/// Inner data for PolicyWrapper (Arc-wrapped for cheap cloning)
#[derive(Debug)]
struct PolicyWrapperInner {
    /// View (query) policy set
    view: PolicySet,
    /// Modify (transaction) policy set
    modify: PolicySet,
    /// Root flag - bypasses all policies
    root: bool,
    /// Single default-allow knob
    default_allow: bool,
    /// Policy values map (for identity/context bindings)
    policy_values: HashMap<String, Sid>,
}

/// Policy wrapper container
///
/// Provides cheap cloning via Arc and access to both view and modify policy sets.
#[derive(Debug, Clone)]
pub struct PolicyWrapper {
    inner: Arc<PolicyWrapperInner>,
}

impl PolicyWrapper {
    /// Create a new policy wrapper
    pub fn new(
        view: PolicySet,
        modify: PolicySet,
        root: bool,
        default_allow: bool,
        policy_values: HashMap<String, Sid>,
    ) -> Self {
        Self {
            inner: Arc::new(PolicyWrapperInner {
                view,
                modify,
                root,
                default_allow,
                policy_values,
            }),
        }
    }

    /// Create a root policy wrapper (bypasses all policies)
    pub fn root() -> Self {
        Self {
            inner: Arc::new(PolicyWrapperInner {
                view: PolicySet::default(),
                modify: PolicySet::default(),
                root: true,
                default_allow: true,
                policy_values: HashMap::new(),
            }),
        }
    }

    /// Check if this is a root policy (bypasses all checks)
    pub fn is_root(&self) -> bool {
        self.inner.root
    }

    /// Get the default allow setting
    pub fn default_allow(&self) -> bool {
        self.inner.default_allow
    }

    /// Get the view policy set
    pub fn view(&self) -> &PolicySet {
        &self.inner.view
    }

    /// Get the modify policy set
    pub fn modify(&self) -> &PolicySet {
        &self.inner.modify
    }

    /// Get policy values (identity/context bindings)
    pub fn policy_values(&self) -> &HashMap<String, Sid> {
        &self.inner.policy_values
    }

    /// Check if any view or modify policy needs class membership checks
    ///
    /// Returns true if there are any class policies (f:onClass).
    /// This is used to decide whether to pre-populate the class cache.
    ///
    /// Note: With per-property class_check_needed, ANY class policy may need
    /// class checking for implicit properties (@id, rdf:type) which are always
    /// indexed with class_check_needed=true. So we check for class_policy alone.
    pub fn has_class_policies(&self) -> bool {
        let view_has = self.inner.view.restrictions.iter().any(|r| r.class_policy);

        let modify_has = self
            .inner
            .modify
            .restrictions
            .iter()
            .any(|r| r.class_policy);

        view_has || modify_has
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    #[test]
    fn test_policy_wrapper_root() {
        let wrapper = PolicyWrapper::root();
        assert!(wrapper.is_root());
        assert!(wrapper.default_allow());
    }

    #[test]
    fn test_policy_set_restrictions_for_flake() {
        let mut set = PolicySet::new();

        // Add a property-specific restriction
        let prop_restriction = PolicyRestriction {
            id: "prop-1".to_string(),
            target_mode: TargetMode::OnProperty,
            targets: [make_sid(100, "name")].into_iter().collect(),
            action: PolicyAction::View,
            value: PolicyValue::Allow,
            required: false,
            message: None,
            class_policy: false,
            for_classes: HashSet::new(),
            class_check_needed: false,
        };
        set.restrictions.push(prop_restriction);
        set.by_property
            .entry(make_sid(100, "name"))
            .or_default()
            .push(PropertyPolicyEntry {
                idx: 0,
                class_check_needed: false,
            });

        // Add a default restriction
        let default_restriction = PolicyRestriction {
            id: "default-1".to_string(),
            target_mode: TargetMode::Default,
            targets: HashSet::new(),
            action: PolicyAction::Both,
            value: PolicyValue::Deny,
            required: false,
            message: Some("Access denied".to_string()),
            class_policy: false,
            for_classes: HashSet::new(),
            class_check_needed: false,
        };
        set.restrictions.push(default_restriction);
        set.defaults.push(1);

        // Query for the "name" property
        let candidates =
            set.restrictions_for_flake(&make_sid(100, "alice"), &make_sid(100, "name"));
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].id, "prop-1");
        assert_eq!(candidates[1].id, "default-1");

        // Query for a different property
        let other_candidates =
            set.restrictions_for_flake(&make_sid(100, "alice"), &make_sid(100, "age"));
        assert_eq!(other_candidates.len(), 1);
        assert_eq!(other_candidates[0].id, "default-1");
    }
}
