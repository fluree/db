//! Policy enforcer for batch filtering
//!
//! Provides the `QueryPolicyEnforcer` which filters flakes by policy with caching.

use super::QueryPolicyExecutor;
use crate::error::Result;
use fluree_db_core::{
    Flake, FlakeValue, GraphDbRef, GraphId, LedgerSnapshot, OverlayProvider, Sid, Tracker,
};
use fluree_db_policy::{is_schema_flake, is_schema_predicate, ClassState, PolicyContext};
use std::collections::HashMap;
use std::sync::Arc;

/// Plan-time-style verdict for a single statically-known scanned predicate under
/// an enforcer's *view* policy.
///
/// Lets a single-predicate fast path skip the per-flake policy filter when the
/// predicate is provably uncovered by the view policy
/// (see [`PolicySet::covers_predicate`](fluree_db_policy::PolicySet::covers_predicate)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateCoverage {
    /// A view restriction may apply to flakes with this predicate, or a deny
    /// default meets a schema predicate (whose schema flakes stay visible); the
    /// per-flake filtered scan fallback is required.
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
        } else if is_schema_predicate(predicate) {
            // Schema flakes stay visible under a deny default; filter rather
            // than fold to empty.
            PredicateCoverage::Covered
        } else {
            PredicateCoverage::UncoveredDeny
        }
    }

    /// Whether the view policy can hide any flake at all. Gates the lanes'
    /// triple-term check: an uncovered predicate's term rows are hidden only
    /// through the restrictions on the triples they name.
    pub fn view_restricts_anything(&self) -> bool {
        let view = self.policy.view_set();
        !(view.by_property.is_empty()
            && view.by_class.is_empty()
            && view.by_subject.is_empty()
            && view.defaults.is_empty())
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
        let classes = self
            .subject_classes(GraphDbRef::new(snapshot, g_id, overlay, to_t), &flakes)
            .await?;

        let mut result = Vec::with_capacity(flakes.len());

        for flake in flakes {
            // A term names its triple even under a schema predicate, so it is
            // checked before the schema exemption.
            if !self
                .term_visible(&classes, &flake.o, &executor, tracker)
                .await?
            {
                continue;
            }
            // Schema flakes always allowed
            if is_schema_flake(&flake.p, &flake.o) {
                result.push(flake);
                continue;
            }

            let subject_classes = classes.of(&self.policy, &flake.s);

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

        // Create executor using the GRAPH's snapshot/overlay/to_t
        let executor = QueryPolicyExecutor::with_overlay(snapshot, overlay, to_t);
        let classes = self
            .subject_classes(
                GraphDbRef::new(snapshot, g_id, overlay, to_t),
                std::slice::from_ref(flake),
            )
            .await?;
        // Before the schema exemption: a term names its triple under any predicate.
        if !self
            .term_visible(&classes, &flake.o, &executor, tracker)
            .await?
        {
            return Ok(false);
        }

        // Schema flakes always allowed
        if is_schema_flake(&flake.p, &flake.o) {
            return Ok(true);
        }

        let subject_classes = classes.of(&self.policy, &flake.s);

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

    /// A triple-term object shows the triple it names, so a flake holding one
    /// (an `rdf:reifies` link) is visible only when that triple would be, and
    /// so on through a nested term. Other objects pass.
    async fn term_visible(
        &self,
        subject_classes: &SubjectClasses,
        object: &FlakeValue,
        executor: &QueryPolicyExecutor<'_>,
        tracker: &Tracker,
    ) -> Result<bool> {
        let mut object = object;
        while let FlakeValue::TripleTerm(term) = object {
            let classes = subject_classes.of(&self.policy, &term.s);
            let allowed = self
                .policy
                .allow_view_flake_async(&term.s, &term.p, &term.o, &classes, executor, tracker)
                .await
                .map_err(|e| crate::error::QueryError::Policy(e.to_string()))?;
            if !allowed {
                return Ok(false);
            }
            object = &term.o;
        }
        Ok(true)
    }

    /// Resolve, under the state they read with, the classes of every subject
    /// the checks of `flakes` read: each non-schema flake's, and those of the
    /// triple terms among their objects, which `term_visible` checks like any
    /// flake subject. Callers need no prefetch: a miss reads as no classes, so
    /// the check resolves its own.
    ///
    /// A state shared with nothing keeps its lookups in a map of this call's
    /// own: in the context's cache they would be entries no later check
    /// reads, kept for the context's life.
    async fn subject_classes(
        &self,
        db: GraphDbRef<'_>,
        flakes: &[Flake],
    ) -> Result<SubjectClasses> {
        if !self.policy.wrapper().has_class_policies() {
            return Ok(SubjectClasses::Local(HashMap::new()));
        }
        let subjects = flakes.iter().flat_map(|flake| {
            let terms = std::iter::successors(Some(&flake.o), |object| match object {
                FlakeValue::TripleTerm(term) => Some(&term.o),
                _ => None,
            })
            .filter_map(|object| match object {
                FlakeValue::TripleTerm(term) => Some(&term.s),
                _ => None,
            });
            let own = (!is_schema_flake(&flake.p, &flake.o)).then_some(&flake.s);
            own.into_iter().chain(terms)
        });
        let state = ClassState::of(db);
        if state.is_shared() {
            let mut uncached = self.policy.retain_uncached_iter(&state, subjects);
            if !uncached.is_empty() {
                uncached.sort();
                uncached.dedup();
                fluree_db_policy::populate_class_cache_in(&state, &uncached, db, &self.policy)
                    .await
                    .map_err(|e| crate::error::QueryError::Policy(e.to_string()))?;
            }
            Ok(SubjectClasses::Shared(state))
        } else {
            let mut subjects: Vec<Sid> = subjects.cloned().collect();
            subjects.sort();
            subjects.dedup();
            fluree_db_policy::lookup_subject_classes(&subjects, db)
                .await
                .map(SubjectClasses::Local)
                .map_err(|e| crate::error::QueryError::Policy(e.to_string()))
        }
    }

    /// Prefetch the classes of `subjects` for a run of
    /// [`allow_flake_for_graph`](Self::allow_flake_for_graph) checks, which
    /// otherwise look up one subject at a time. A subject with no classes is
    /// cached too, so its check does not read its `rdf:type` again.
    /// [`filter_flakes_for_graph`](Self::filter_flakes_for_graph) needs no
    /// prefetch: it resolves its batch's subjects in one lookup.
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

/// Where a check finds its subjects' classes.
enum SubjectClasses {
    /// The context's cache, under a state other lookups can share.
    Shared(ClassState),
    /// This call's own lookups.
    Local(HashMap<Sid, Vec<Sid>>),
}

impl SubjectClasses {
    fn of(&self, policy: &PolicyContext, subject: &Sid) -> Vec<Sid> {
        match self {
            Self::Shared(state) => policy
                .get_cached_subject_classes(state, subject)
                .unwrap_or_default(),
            Self::Local(classes) => classes.get(subject).cloned().unwrap_or_default(),
        }
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
    use fluree_db_core::{GraphDbRef, IndexType};
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
    /// version, which shares its class lookups with nothing: not a caller's
    /// prefetch, nor the context's cache.
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

    /// `rdf:type` (a schema predicate) is uncovered under an allow default
    /// unless a rule can reach it; under a deny default it is never folded to
    /// empty.
    #[test]
    fn rdf_type_coverage_follows_rules_and_default() {
        let rdf_type = Sid::new(fluree_vocab::namespaces::RDF, "type");
        let restriction = |mode: TargetMode, target: Sid| PolicyRestriction {
            id: "r".into(),
            target_mode: mode,
            targets: [target.clone()].into_iter().collect(),
            action: PolicyAction::View,
            verbs: None,
            value: PolicyValue::Deny,
            required: false,
            message: None,
            class_policy: mode == TargetMode::OnClass,
            for_classes: if mode == TargetMode::OnClass {
                [target].into_iter().collect()
            } else {
                HashSet::new()
            },
            class_check_needed: mode == TargetMode::OnClass,
        };
        let classify = |rules: Vec<PolicyRestriction>, default_allow: bool, p: &Sid| {
            let view = build_policy_set(rules, None, PolicyAction::View, None);
            let wrapper = PolicyWrapper::new(
                view,
                PolicySet::default(),
                false,
                default_allow,
                HashMap::new(),
            );
            QueryPolicyEnforcer::new(Arc::new(PolicyContext::new(wrapper, None)))
                .classify_view_predicate(p)
        };
        let on_ssn = || vec![restriction(TargetMode::OnProperty, ex("ssn"))];

        assert_eq!(
            classify(on_ssn(), true, &rdf_type),
            PredicateCoverage::UncoveredAllow
        );
        assert_eq!(
            classify(on_ssn(), false, &rdf_type),
            PredicateCoverage::Covered
        );
        assert_eq!(
            classify(on_ssn(), false, &ex("name")),
            PredicateCoverage::UncoveredDeny
        );
        assert_eq!(
            classify(
                vec![restriction(TargetMode::OnProperty, rdf_type.clone())],
                true,
                &rdf_type
            ),
            PredicateCoverage::Covered
        );
        assert_eq!(
            classify(
                vec![restriction(TargetMode::OnClass, ex("Admin"))],
                true,
                &rdf_type
            ),
            PredicateCoverage::Covered
        );
    }
}
