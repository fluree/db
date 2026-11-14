//! Policy enforcer for batch filtering
//!
//! Provides the `QueryPolicyEnforcer` which filters flakes by policy with caching.

use super::QueryPolicyExecutor;
use crate::error::Result;
use fluree_db_core::{Flake, GraphId, LedgerSnapshot, OverlayProvider, Sid, Tracker};
use fluree_db_policy::{is_schema_flake, ClassState, PolicyContext};
use std::sync::Arc;

/// Plan-time-style verdict for a single statically-known scanned predicate under
/// an enforcer's *view* policy.
///
/// Lets a single-predicate fast path skip the per-flake policy filter when the
/// predicate is provably uncovered by the view policy
/// (see [`PolicySet::covers_predicate`](fluree_db_policy::PolicySet::covers_predicate)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateCoverage {
    /// A view restriction may apply to flakes with this predicate; the per-flake
    /// filtered scan fallback is required.
    Covered,
    /// No restriction can apply and the effective default is allow — every flake
    /// with this predicate is visible, so a fast path may run unfiltered.
    UncoveredAllow,
    /// No restriction can apply and the effective default is deny — every flake
    /// with this predicate is hidden, so the result for it is empty.
    UncoveredDeny,
}

/// Policy enforcer for query execution
///
/// Wraps a `PolicyContext` and provides async batch filtering for flakes.
/// Designed to be used by scan operators for per-leaf filtering.
///
/// # Caching (TODO)
///
/// Future versions will cache f:query results to avoid re-executing
/// the same policy query for every flake.
#[derive(Clone)]
pub struct QueryPolicyEnforcer {
    /// The policy context containing restrictions and identity
    policy: Arc<PolicyContext>,
    // TODO: Add PolicyQueryCache for memoization
    // cache: Arc<PolicyQueryCache>,
}

impl QueryPolicyEnforcer {
    /// Create a new policy enforcer
    pub fn new(policy: Arc<PolicyContext>) -> Self {
        Self { policy }
    }

    /// Get the underlying policy context
    pub fn policy(&self) -> &PolicyContext {
        &self.policy
    }

    /// This enforcer, for a query that reads reasoned facts: class policies
    /// are selected by the subject's reasoned classes on every property
    /// ([`PolicyContext::for_reasoned_query`]).
    pub fn for_reasoned_view(&self) -> Self {
        Self {
            policy: Arc::new(self.policy.for_reasoned_query()),
        }
    }

    /// Check if this is a root policy (bypasses all checks)
    pub fn is_root(&self) -> bool {
        self.policy.wrapper().is_root()
    }

    /// Classify a single statically-known scanned predicate against the *view*
    /// policy, amortizing the wrapper/view/default_allow walk into one call.
    ///
    /// A root enforcer reports [`PredicateCoverage::Covered`]: callers are
    /// expected to short-circuit the no-policy / root case via
    /// [`ExecutionContext::allow_unfiltered`](crate::context::ExecutionContext::allow_unfiltered)
    /// before reaching here, so the root arm is only a defensive fallback (it
    /// forces the filtered path, which is correct — just slower — for root).
    pub fn classify_view_predicate(&self, predicate: &Sid) -> PredicateCoverage {
        let wrapper = self.policy.wrapper();
        if wrapper.is_root() {
            return PredicateCoverage::Covered;
        }
        if self.policy.view_set().covers_predicate(predicate) {
            PredicateCoverage::Covered
        } else if wrapper.default_allow() {
            PredicateCoverage::UncoveredAllow
        } else {
            PredicateCoverage::UncoveredDeny
        }
    }

    /// Filter a batch of flakes by policy using explicit graph parameters.
    ///
    /// This is the **correct** method for dataset mode - it uses the graph's
    /// db/overlay/to_t, ensuring `f:query` policies run against the same
    /// snapshot that produced the flakes.
    ///
    /// # Arguments
    ///
    /// * `snapshot` - The database for this graph
    /// * `g_id` - The graph these flakes belong to, used to look up class
    ///   membership in the graph that actually asserted it
    /// * `overlay` - The overlay provider for this graph
    /// * `to_t` - Target transaction time for this graph
    /// * `tracker` - Fuel tracker for limits
    /// * `flakes` - Flakes to filter
    ///
    /// # Returns
    ///
    /// Filtered flakes that pass policy checks
    pub async fn filter_flakes_for_graph(
        &self,
        snapshot: &LedgerSnapshot,
        g_id: GraphId,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        tracker: &Tracker,
        flakes: Vec<Flake>,
    ) -> Result<Vec<Flake>> {
        // Root policy bypasses all checks
        if self.policy.wrapper().is_root() {
            return Ok(flakes);
        }

        // Create executor using the GRAPH's snapshot/overlay/to_t (not ctx-level!)
        let executor = QueryPolicyExecutor::with_overlay(snapshot, overlay, to_t);
        let state = ClassState::new(g_id, overlay, to_t);
        self.cache_subject_classes(state, snapshot, g_id, overlay, to_t, &flakes)
            .await?;

        let mut result = Vec::with_capacity(flakes.len());

        for flake in flakes {
            // Schema flakes always allowed
            if is_schema_flake(&flake.p, &flake.o) {
                result.push(flake);
                continue;
            }

            // Get subject classes from cache
            let subject_classes = self
                .policy
                .get_cached_subject_classes(state, &flake.s)
                .unwrap_or_default();

            // Async policy check with f:query support
            match self
                .policy
                .allow_view_flake_async(
                    &flake.s,
                    &flake.p,
                    &flake.o,
                    &subject_classes,
                    &executor,
                    tracker,
                )
                .await
            {
                Ok(true) => result.push(flake),
                Ok(false) => {} // Filtered out by policy (a genuine deny is Ok(false))
                // An Err here is an execution failure (malformed f:query, storage
                // IO, cooperative cancellation, or fuel exhaustion mid-scan), not a
                // policy denial. Propagate it: silently dropping the flake would
                // turn "too expensive"/"transient failure" into a successful
                // response with fewer rows than the identity is authorized to see.
                Err(e) => return Err(crate::error::QueryError::Policy(e.to_string())),
            }
        }

        Ok(result)
    }

    /// Check if a single flake is allowed by policy using explicit graph parameters.
    ///
    /// This is the correct method for dataset mode.
    pub async fn allow_flake_for_graph(
        &self,
        snapshot: &LedgerSnapshot,
        g_id: GraphId,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        tracker: &Tracker,
        flake: &Flake,
    ) -> Result<bool> {
        // Root policy bypasses all checks
        if self.policy.wrapper().is_root() {
            return Ok(true);
        }

        // Schema flakes always allowed
        if is_schema_flake(&flake.p, &flake.o) {
            return Ok(true);
        }

        // Create executor using the GRAPH's snapshot/overlay/to_t
        let executor = QueryPolicyExecutor::with_overlay(snapshot, overlay, to_t);
        let state = ClassState::new(g_id, overlay, to_t);
        self.cache_subject_classes(
            state,
            snapshot,
            g_id,
            overlay,
            to_t,
            std::slice::from_ref(flake),
        )
        .await?;

        // Get subject classes from cache
        let subject_classes = self
            .policy
            .get_cached_subject_classes(state, &flake.s)
            .unwrap_or_default();

        // Async policy check
        self.policy
            .allow_view_flake_async(
                &flake.s,
                &flake.p,
                &flake.o,
                &subject_classes,
                &executor,
                tracker,
            )
            .await
            .map_err(|e| crate::error::QueryError::Policy(e.to_string()))
    }

    /// Cache, under `state`, the classes of every subject the checks of
    /// `flakes` read. A caller's prefetch is not enough: a miss reads as no
    /// classes, and a state of an overlay with no content version matches
    /// only itself.
    async fn cache_subject_classes(
        &self,
        state: ClassState,
        snapshot: &LedgerSnapshot,
        g_id: GraphId,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        flakes: &[Flake],
    ) -> Result<()> {
        if !self.policy.wrapper().has_class_policies() {
            return Ok(());
        }
        let mut subjects: Vec<Sid> = flakes.iter().map(|f| f.s.clone()).collect();
        subjects.sort();
        subjects.dedup();
        let db = fluree_db_core::GraphDbRef::new(snapshot, g_id, overlay, to_t);
        fluree_db_policy::populate_class_cache_in(state, &subjects, db, &self.policy)
            .await
            .map_err(|e| crate::error::QueryError::Policy(e.to_string()))
    }

    /// Populate the class cache for subjects using a graph database reference.
    ///
    /// Call this before filtering to ensure class lookups are cached.
    pub async fn populate_class_cache_for_graph(
        &self,
        db: fluree_db_core::GraphDbRef<'_>,
        subjects: &[fluree_db_core::Sid],
    ) -> Result<()> {
        fluree_db_policy::populate_class_cache(subjects, db, &self.policy)
            .await
            .map_err(|e| crate::error::QueryError::Policy(e.to_string()))?;
        Ok(())
    }
}

impl std::fmt::Debug for QueryPolicyEnforcer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPolicyEnforcer")
            .field("is_root", &self.is_root())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, GraphDbRef, IndexType};
    use fluree_db_policy::{
        build_policy_set, PolicyAction, PolicyRestriction, PolicySet, PolicyValue, PolicyWrapper,
        TargetMode,
    };
    use std::cmp::Ordering;
    use std::collections::{HashMap, HashSet};

    /// Facts in each index order, under no content version.
    struct Unversioned(Vec<Vec<Flake>>);

    impl Unversioned {
        fn new(flakes: Vec<Flake>) -> Self {
            Self(
                [
                    IndexType::Spot,
                    IndexType::Psot,
                    IndexType::Post,
                    IndexType::Opst,
                ]
                .into_iter()
                .map(|i| {
                    let mut v = flakes.clone();
                    v.sort_by(|a, b| i.compare(a, b));
                    v
                })
                .collect(),
            )
        }
    }

    impl OverlayProvider for Unversioned {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn epoch(&self) -> u64 {
            1
        }
        fn for_each_overlay_flake(
            &self,
            _g: GraphId,
            index: IndexType,
            first: Option<&Flake>,
            rhs: Option<&Flake>,
            leftmost: bool,
            to_t: i64,
            cb: &mut dyn FnMut(&Flake),
        ) {
            let list = &self.0[match index {
                IndexType::Spot => 0,
                IndexType::Psot => 1,
                IndexType::Post => 2,
                IndexType::Opst => 3,
            }];
            for f in list {
                if f.t > to_t
                    || (!leftmost
                        && first.is_some_and(|lb| index.compare(f, lb) != Ordering::Greater))
                    || rhs.is_some_and(|rb| index.compare(f, rb) == Ordering::Greater)
                {
                    continue;
                }
                cb(f);
            }
        }
    }

    fn ex(name: &str) -> Sid {
        Sid::new(100, name)
    }

    /// A class deny reaches a subject read through an overlay with no content
    /// version, whose classes a caller's prefetch cached under a state of its
    /// own.
    #[tokio::test]
    async fn a_class_policy_applies_over_an_overlay_with_no_content_version() {
        let mut snapshot = LedgerSnapshot::genesis("test:main");
        for (code, iri) in [
            (3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            (100, "http://example.org/"),
        ] {
            let _ = snapshot.insert_namespace_code(code, iri.to_string());
        }
        let name = |s: &str| {
            Flake::new(
                ex(s),
                ex("name"),
                FlakeValue::String(s.into()),
                Sid::new(2, "string"),
                1,
                true,
                None,
            )
        };
        let secret_type = Flake::new(
            ex("alice"),
            Sid::new(3, "type"),
            FlakeValue::Ref(ex("Secret")),
            Sid::new(1, "id"),
            1,
            true,
            None,
        );
        let overlay = Unversioned::new(vec![secret_type, name("alice"), name("bob")]);

        let deny = PolicyRestriction {
            id: "secret".into(),
            target_mode: TargetMode::OnClass,
            targets: HashSet::new(),
            action: PolicyAction::View,
            verbs: None,
            value: PolicyValue::Deny,
            required: false,
            message: None,
            class_policy: true,
            for_classes: [ex("Secret")].into_iter().collect(),
            class_check_needed: true,
        };
        let view = build_policy_set(vec![deny], None, PolicyAction::View, None)
            .selecting_classes_by_subject()
            .expect("a class policy");
        let wrapper = PolicyWrapper::new(view, PolicySet::default(), false, true, HashMap::new());
        let enforcer = QueryPolicyEnforcer::new(Arc::new(PolicyContext::new(wrapper, None)));

        enforcer
            .populate_class_cache_for_graph(
                GraphDbRef::new(&snapshot, 0, &overlay, 1),
                &[ex("alice"), ex("bob")],
            )
            .await
            .unwrap();
        let kept = enforcer
            .filter_flakes_for_graph(
                &snapshot,
                0,
                &overlay,
                1,
                &Tracker::disabled(),
                vec![name("alice"), name("bob")],
            )
            .await
            .unwrap();
        assert_eq!(kept, vec![name("bob")]);
    }
}
