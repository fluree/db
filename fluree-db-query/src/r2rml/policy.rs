//! View-policy enforcement for R2RML (Iceberg / SQL) graph-source scans.
//!
//! Native scans filter flakes through `QueryPolicyEnforcer`, which works on
//! SIDs and looks subject classes up in the graph's index. An R2RML scan
//! produces string-IRI bindings straight from table rows with no index behind
//! them, so it evaluates the *same* policy set here instead:
//!
//! * subject classes come from the mapping (`rr:class`, constant `rdf:type`
//!   object maps, and any column-derived `rdf:type` the row materializes);
//! * policy targets are matched by encoding the row's IRIs through the graph's
//!   snapshot, exactly as the targets were encoded when the policy was built;
//! * `f:onClass` view policies are indexed against class→property stats
//!   derived from the mapping (a virtual source has no index stats), so a class
//!   policy governs every predicate the class's triples maps declare;
//! * `f:query` policies evaluate as "returned no rows" — there is no graph to
//!   run them against, so they deny (fail closed) exactly as they would against
//!   an empty ledger.
//!
//! Decisions are static per `(TriplesMap, predicate)` unless the policy set
//! targets specific subjects or the map derives `rdf:type` from a column. In
//! that common case a window's decision is settled once before its rows are
//! walked at all, and a map whose required predicates are all denied is skipped
//! before its table is scanned. Only a bound predicate variable, a
//! subject-targeted policy, or column-derived classes put the gate on the
//! per-row path, where it costs a borrowed hash lookup.

use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::R2rmlPattern;
use crate::var_registry::VarId;
use fluree_db_core::{
    ClassPropertyUsage, ClassStatEntry, FlakeValue, IndexStats, LedgerSnapshot, Sid,
};
use fluree_db_policy::{
    build_policy_set, NoOpQueryExecutor, PolicyAction, PolicyContext, PolicySet, PolicyWrapper,
    TargetMode,
};
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ConstantValue, ObjectMap, TriplesMap};
use fluree_vocab::rdf;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// A `(triples map, predicate)` view verdict decided before any row is read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Verdict {
    Allow,
    Deny,
    /// Decided by the row's column-derived class: each class a view policy
    /// targets whose verdict differs from `otherwise`, which a row of any
    /// other class, or of none, gets.
    ByClass {
        classes: Vec<(String, bool)>,
        otherwise: bool,
    },
    /// Decided by the row's subject: each subject a view policy targets
    /// whose verdict differs from `otherwise`, which any other subject gets.
    BySubject {
        subjects: Vec<(String, bool)>,
        otherwise: bool,
    },
}

/// Per-scan view-policy gate for an R2RML graph source.
pub(crate) struct R2rmlPolicyGate {
    /// Lane-local policy context: the request's view restrictions re-indexed
    /// against mapping-derived class stats.
    policy: PolicyContext,
    /// The view set targets specific subjects, so a decision can depend on the
    /// row's subject IRI (no static memoization).
    per_subject: bool,
    /// Any view restriction is class-targeted, so a map with column-derived
    /// `rdf:type` needs each row's classes.
    has_class_policies: bool,
    /// Placeholder subject for subject-independent evaluation.
    any_subject: Sid,
    /// Static classes per TriplesMap IRI, encoded.
    tm_classes: HashMap<String, Vec<Sid>>,
    /// Every class a view restriction is selected by: the only classes a
    /// row can carry that change a verdict.
    policy_classes: Vec<String>,
    /// Every subject a view restriction targets; `None` when one could not
    /// be named as an IRI, so no verdict is static.
    policy_subjects: Option<Vec<String>>,
    /// TriplesMap IRI → predicate IRI → allowed, for subject-independent maps.
    /// Nested so a lookup borrows both keys and only a miss allocates.
    static_cache: HashMap<String, HashMap<String, bool>>,
    /// The snapshot the policy's targets are encoded against, where it is not
    /// the context's own (a source reached through `GRAPH` in a dataset).
    encoder: Option<LedgerSnapshot>,
    /// What a subject may hold beyond the classes one row of its map shows.
    scope: ClassScope,
    /// The classes class-targeted restrictions select by: those of every
    /// `f:onClass` restriction, which applies to every predicate, and those
    /// of each `f:onProperty` restriction with classes, by property.
    targeted_any: Vec<String>,
    targeted_by_predicate: HashMap<String, Vec<String>>,
    /// The view set holds an `f:required` or an `f:query` restriction, which
    /// make a decision move both ways as a subject's class set grows, so the
    /// two-point check does not settle it (see [`Self::allows`]).
    enumerate: bool,
    /// The source, for messages.
    graph_source_id: String,
    /// Final decisions, by map, triple and the row's classes, for
    /// subject-independent policies.
    decision_cache: HashMap<(String, String, Vec<String>), bool>,
    /// Policy evaluations run (tests read it).
    #[cfg(test)]
    evaluations: std::sync::atomic::AtomicUsize,
}

/// The most classes beyond a row's own that a decision is enumerated over
/// (`2^n` evaluations); a decision needing more refuses the query.
const MAX_EXTRA_CLASSES: usize = 8;

/// What a subject of each map may hold beyond the classes one row of its map
/// shows, from the mapping alone (and the keys of maps deriving classes).
#[derive(Debug, Default)]
struct ClassScope {
    /// Per map: the other maps whose subjects are not provably apart from its
    /// own (`TriplesMap::subjects_provably_disjoint`).
    overlaps: HashMap<String, Vec<String>>,
    /// Per map: the classes every subject of it holds.
    statics: HashMap<String, Vec<String>>,
    /// Per map deriving a class from its row: the prefixes a derived class
    /// starts with (an IRI template's constant text, a reference's parent
    /// subject template's), or `None` where it can be any IRI (an
    /// `rr:column` IRI, a parent minting from a column).
    derives: HashMap<String, Option<Vec<String>>>,
    /// Per map deriving a class: whether its table's key makes each subject
    /// one row's, so a row's derived classes are the subject's.
    unique: HashMap<String, bool>,
}

impl ClassScope {
    fn of(mapping: &CompiledR2rmlMapping) -> Self {
        let maps: Vec<&TriplesMap> = mapping.triples_maps.values().collect();
        let mut scope = ClassScope::default();
        for tm in &maps {
            let others = maps
                .iter()
                .filter(|o| o.iri != tm.iri && !tm.subjects_provably_disjoint(o))
                .map(|o| o.iri.clone())
                .collect();
            scope.overlaps.insert(tm.iri.clone(), others);
            scope.statics.insert(tm.iri.clone(), static_classes(tm));
            if let Some(prefixes) = derived_class_prefixes(mapping, tm) {
                scope.derives.insert(tm.iri.clone(), prefixes);
            }
        }
        scope
    }

    /// Whether `prefixes` (from [`Self::derives`]) can mint `class`.
    fn can_mint(prefixes: &Option<Vec<String>>, class: &str) -> bool {
        prefixes
            .as_ref()
            .is_none_or(|ps| ps.iter().any(|p| class.starts_with(p.as_str())))
    }

    /// The classes among `targeted` a subject of `tm` may hold that its row
    /// does not show (`own`): those of overlapping maps' static classes, and
    /// those deriving maps can mint where a row may not show its subject's
    /// derived classes (the map's own rows when its subjects are not proven
    /// one row's, every row of an overlapping map deriving classes).
    fn extra(&self, tm: &str, own: &[String], targeted: &[String]) -> (Vec<String>, Vec<String>) {
        let (mut statics, mut data) = (Vec::new(), Vec::new());
        if targeted.is_empty() {
            return (statics, data);
        }
        let overlaps = self.overlaps.get(tm).map(Vec::as_slice).unwrap_or_default();
        for other in overlaps {
            for class in self
                .statics
                .get(other)
                .map(Vec::as_slice)
                .unwrap_or_default()
            {
                if targeted.contains(class) && !own.contains(class) && !statics.contains(class) {
                    statics.push(class.clone());
                }
            }
        }
        let mut deriving: Vec<&Option<Vec<String>>> = Vec::new();
        if let Some(prefixes) = self.derives.get(tm) {
            if !self.unique.get(tm).copied().unwrap_or(false) {
                deriving.push(prefixes);
            }
        }
        for other in overlaps {
            if let Some(prefixes) = self.derives.get(other) {
                deriving.push(prefixes);
            }
        }
        for class in targeted {
            if own.contains(class) || statics.contains(class) || data.contains(class) {
                continue;
            }
            if deriving.iter().any(|p| Self::can_mint(p, class)) {
                data.push(class.clone());
            }
        }
        statics.sort();
        data.sort();
        (statics, data)
    }
}

/// Warnings, for a source registered with a model, about subjects whose
/// classes one row cannot show: each pair of triples maps whose subjects are
/// not provably apart and that declare different classes, or where one derives
/// classes from its row. A class-targeted view policy decides such a map's
/// rows over every class set its subjects could hold, hiding rows a decision
/// on one row's classes would show (or, where the classes come from rows the
/// scan does not read, refusing the query).
pub fn class_overlap_warnings(mapping: &CompiledR2rmlMapping) -> Vec<String> {
    let scope = ClassScope::of(mapping);
    let mut maps: Vec<&TriplesMap> = mapping.triples_maps.values().collect();
    maps.sort_by(|a, b| a.iri.cmp(&b.iri));
    let mut warnings = Vec::new();
    for (i, a) in maps.iter().enumerate() {
        for b in &maps[i + 1..] {
            if a.subjects_provably_disjoint(b) {
                continue;
            }
            let differ = scope.statics.get(&a.iri) != scope.statics.get(&b.iri)
                || scope.derives.contains_key(&a.iri)
                || scope.derives.contains_key(&b.iri);
            if differ {
                warnings.push(format!(
                    "triples maps <{}> and <{}> can mint the same subjects with different \
                     classes; a class-targeted view policy decides their rows over every class \
                     set a subject could hold, and may hide rows a native ledger would show \
                     (give them subject templates that keep them apart, or target such \
                     policies by property or subject)",
                    a.iri, b.iri
                ));
            }
        }
    }
    warnings
}

/// The prefixes a class `tm` derives from its row starts with (see/// The prefixes a class `tm` derives from its row starts with (see
/// [`ClassScope::derives`]); `None` (outer) when it derives none.
fn derived_class_prefixes(
    mapping: &CompiledR2rmlMapping,
    tm: &TriplesMap,
) -> Option<Option<Vec<String>>> {
    use fluree_db_r2rml::mapping::TermType;
    let mut prefixes: Vec<String> = Vec::new();
    let mut derives = false;
    for pom in &tm.predicate_object_maps {
        if pom.predicate_map.as_constant() != Some(rdf::TYPE) {
            continue;
        }
        match &pom.object_map {
            ObjectMap::Constant { .. } => continue,
            ObjectMap::Template {
                template,
                term_type,
                ..
            } => {
                derives = true;
                // Only an IRI is a class.
                if *term_type == TermType::Iri {
                    prefixes.push(super::rewrite::constant_prefix(template).to_string());
                }
            }
            ObjectMap::RefObjectMap(rom) => {
                derives = true;
                let parent = mapping.get(&rom.parent_triples_map);
                match parent.map(|p| &p.subject_map) {
                    Some(sm) if sm.constant.is_some() => {
                        prefixes.push(sm.constant.clone().unwrap_or_default());
                    }
                    Some(sm) if sm.column.is_none() && sm.template.is_some() => {
                        let template = sm.template.as_deref().unwrap_or_default();
                        prefixes.push(super::rewrite::constant_prefix(template).to_string());
                    }
                    _ => return Some(None),
                }
            }
            ObjectMap::Column { term_type, .. } => {
                derives = true;
                if *term_type == TermType::Iri {
                    return Some(None);
                }
            }
        }
    }
    derives.then_some(Some(prefixes))
}

impl R2rmlPolicyGate {
    /// Build the gate for this scan of `graph_source_id`, or `None` when no
    /// view policy governs the source and the scan may run unfiltered.
    ///
    /// The policy that governs a source is its own. In a dataset the source is
    /// a graph with its own enforcer (built from the request and the source's
    /// model), and a scan reached through `GRAPH` runs in a context switched
    /// from another graph, whose enforcer is that graph's: the dataset's entry
    /// for the source is taken first. Outside a dataset the context's enforcer
    /// is the source's own. Where filtering is required and no enforcer can be
    /// resolved for the source, the scan is refused rather than run
    /// unfiltered.
    pub(crate) async fn build(
        ctx: &ExecutionContext<'_>,
        mapping: &CompiledR2rmlMapping,
        graph_source_id: &str,
    ) -> Result<Option<Self>> {
        // The source's own graph in a dataset carries the snapshot its policy
        // was built against: targets and classes are encoded there, which a
        // context switched to the source from another graph does not share.
        let (enforcer, encoder) = match ctx.dataset.and_then(|ds| ds.named_graph(graph_source_id)) {
            Some(graph) => (graph.policy_enforcer.clone(), Some(graph.snapshot.clone())),
            None => {
                // Outside a dataset the context's policy is the source's own
                // only where the context was built for it. A source a query
                // over another graph reaches through `GRAPH` (a graph variable
                // bound to its IRI) would be read under that graph's policy,
                // or none: refuse it.
                let in_graph_scope = matches!(
                    &ctx.active_graph,
                    crate::dataset::ActiveGraph::Named(iri) if iri.as_ref() == graph_source_id
                );
                let other_policy = ctx
                    .policy_source
                    .as_deref()
                    .is_some_and(|source| source != graph_source_id);
                if in_graph_scope && other_policy {
                    return Err(QueryError::InvalidQuery(format!(
                        "Graph source '{graph_source_id}' is reached through a graph scope but \
                         is not in the query's dataset, so the view policy in force is not the \
                         source's own. Name it in from-named \
                         (SPARQL: FROM NAMED <{graph_source_id}>) and use GRAPH \
                         <{graph_source_id}> or VALUES ?g {{ <{graph_source_id}> }}."
                    )));
                }
                if ctx.allow_unfiltered() {
                    return Ok(None);
                }
                match ctx.policy_enforcer.clone() {
                    Some(enforcer) => (Some(enforcer), None),
                    None => {
                        return Err(QueryError::Policy(format!(
                            "Graph source '{graph_source_id}': a view policy applies to this \
                             query, but none could be resolved for the source; refusing to \
                             read it unfiltered"
                        )))
                    }
                }
            }
        };
        let Some(enforcer) = enforcer.filter(|e| !e.is_root()) else {
            return Ok(None);
        };
        let base = enforcer.policy();
        let snapshot = encoder.as_ref().unwrap_or(ctx.active_snapshot);

        let stats = mapping_class_stats(mapping, snapshot);
        let mut view = build_policy_set(
            base.wrapper().view().restrictions.clone(),
            Some(&stats),
            PolicyAction::View,
            None,
        );
        // Also select class policies by the subject's classes at evaluation time
        // (the modify-set indexing): a map that derives `rdf:type` from a column
        // has classes no mapping stats can enumerate, so the property index
        // above cannot reach them, and a static map's class policy must cover
        // a templated predicate the stats never listed.
        for idx in 0..view.restrictions.len() {
            let r = &view.restrictions[idx];
            if r.target_mode != TargetMode::OnClass {
                continue;
            }
            let classes: Vec<Sid> = r.for_classes.iter().cloned().collect();
            for c in classes {
                view.by_class.entry(c).or_default().push(idx);
            }
        }
        // A class-targeted restriction applies to a subject holding one of its
        // classes. The property index may skip that check for a property
        // only instances of the restriction's classes are known to use; the
        // mapping's stats credit a property to static classes only, so a map
        // that derives its class from a row, or declares none, would have its
        // rows decided as if classed. Every class-targeted entry checks.
        let class_policies: Vec<bool> = view.restrictions.iter().map(|r| r.class_policy).collect();
        for entries in view.by_property.values_mut() {
            for entry in entries {
                if class_policies[entry.idx] {
                    entry.class_check_needed = true;
                }
            }
        }
        let per_subject = !view.by_subject.is_empty();
        let has_class_policies = view.restrictions.iter().any(|r| r.class_policy);
        // Every class any restriction is selected by, whatever it targets: a
        // restriction on a property of a class (`f:onProperty` with
        // `f:onClass`) is indexed by its property, not its class, yet its
        // verdict turns on the class all the same.
        let mut policy_classes: Vec<String> = view
            .restrictions
            .iter()
            .filter(|r| r.class_policy)
            .flat_map(|r| r.for_classes.iter())
            .filter_map(|c| snapshot.decode_sid(c))
            .collect();
        policy_classes.sort();
        policy_classes.dedup();
        let mut policy_subjects: Option<Vec<String>> = view
            .by_subject
            .keys()
            .map(|s| snapshot.decode_sid(s))
            .collect();
        if let Some(subjects) = policy_subjects.as_mut() {
            subjects.sort();
        }
        report_unevaluable_policies(ctx, graph_source_id, &view);
        let mut targeted_any: Vec<String> = Vec::new();
        let mut targeted_by_predicate: HashMap<String, Vec<String>> = HashMap::new();
        for r in view.restrictions.iter().filter(|r| r.class_policy) {
            let classes: Vec<String> = r
                .for_classes
                .iter()
                .filter_map(|c| snapshot.decode_sid(c))
                .collect();
            if r.target_mode == TargetMode::OnClass {
                targeted_any.extend(classes);
            } else {
                for target in &r.targets {
                    if let Some(p) = snapshot.decode_sid(target) {
                        targeted_by_predicate
                            .entry(p)
                            .or_default()
                            .extend(classes.iter().cloned());
                    }
                }
            }
        }
        targeted_any.sort();
        targeted_any.dedup();
        let enumerate = view
            .restrictions
            .iter()
            .any(|r| r.required || matches!(r.value, fluree_db_policy::PolicyValue::Query(_)));
        // Without a class-targeted restriction no class changes a decision:
        // nothing about other maps' classes is worked out.
        let mut scope = if has_class_policies {
            ClassScope::of(mapping)
        } else {
            ClassScope::default()
        };
        if has_class_policies && !scope.derives.is_empty() {
            // Whether a deriving map's rows each show their subject's derived
            // classes rests on its table's keys, trusted as the deduplication
            // that relies on them is.
            let as_of_t = if ctx.explicit_dataset().is_some() {
                None
            } else {
                Some(ctx.to_t)
            };
            let mut keys = super::set_semantics::TableKeyEvidence::default();
            let deriving: Vec<String> = scope.derives.keys().cloned().collect();
            for iri in deriving {
                let unique = match (ctx.r2rml_table_provider, mapping.get(&iri)) {
                    (Some(provider), Some(tm)) => {
                        keys.subject_unique_fetching(
                            provider,
                            mapping,
                            graph_source_id,
                            as_of_t,
                            tm,
                        )
                        .await?
                    }
                    _ => false,
                };
                scope.unique.insert(iri, unique);
            }
        }
        let wrapper = PolicyWrapper::new(
            view,
            PolicySet::default(),
            false,
            base.wrapper().default_allow(),
            base.wrapper().policy_values().clone(),
        );
        let policy = PolicyContext::new(wrapper, Some(base.identity.clone()));

        let tm_classes = mapping
            .triples_maps
            .values()
            .map(|tm| {
                let sids = static_classes(tm)
                    .iter()
                    .filter_map(|c| snapshot.encode_iri(c))
                    .collect();
                (tm.iri.clone(), sids)
            })
            .collect();

        Ok(Some(Self {
            policy,
            per_subject,
            has_class_policies,
            any_subject: Sid::new(fluree_vocab::namespaces::EMPTY, ""),
            tm_classes,
            policy_classes,
            policy_subjects,
            static_cache: HashMap::new(),
            encoder,
            scope,
            targeted_any,
            targeted_by_predicate,
            enumerate,
            graph_source_id: graph_source_id.to_string(),
            decision_cache: HashMap::new(),
            #[cfg(test)]
            evaluations: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    /// Whether this map's rows carry classes the mapping cannot know
    /// statically (a column/template/ref `rdf:type` object map) that a class
    /// policy would need — the scan must then project those columns and
    /// materialize each row's classes.
    /// Every `(triples map, predicate)` verdict the mapping can need, decided
    /// up front for a lane that plans the whole block before reading a row.
    /// A class policy over a map with one column-derived `rdf:type` is
    /// decided per targeted class ([`Verdict::ByClass`]), which the lane
    /// turns into a predicate on that column; a subject-targeted policy is
    /// decided per targeted subject ([`Verdict::BySubject`]) the same way.
    /// `None` when a verdict depends on the row beyond that (both at once,
    /// or a map deriving classes the lane cannot key), or when the policies
    /// name more than `subject_cap` subjects: each costs an evaluation per
    /// (map, predicate) here and a member of an `IN` list in the statement.
    pub(crate) async fn static_verdicts(
        &mut self,
        ctx: &ExecutionContext<'_>,
        mapping: &CompiledR2rmlMapping,
        subject_cap: usize,
    ) -> Result<Option<HashMap<(String, String), Verdict>>> {
        let subjects = match (&self.policy_subjects, self.per_subject) {
            (Some(s), true) => s.clone(),
            (None, true) => return Ok(None),
            (_, false) => Vec::new(),
        };
        if subjects.len() > subject_cap {
            tracing::debug!(
                subjects = subjects.len(),
                subject_cap,
                "sql pushdown: policy names more subjects than the key-set cap"
            );
            return Ok(None);
        }
        let mut out = HashMap::new();
        for tm in mapping.triples_maps.values() {
            let by_class = self.needs_row_classes(tm);
            if by_class && (derived_type_map(tm).is_none() || self.per_subject) {
                return Ok(None);
            }
            let mut preds: Vec<String> = tm
                .predicate_object_maps
                .iter()
                .filter_map(|pom| match &pom.predicate_map {
                    fluree_db_r2rml::mapping::PredicateMap::Constant(p) => Some(p.clone()),
                    _ => None,
                })
                .collect();
            preds.push(rdf::TYPE.to_string());
            for pred in preds {
                // A type triple is decided on the class it states; a map
                // stating classes whose triples are decided apart is not one
                // verdict, and the lane declines it.
                let mut decided: Option<bool> = None;
                if pred == rdf::TYPE {
                    for class in static_classes(tm) {
                        let allowed = self.allows(ctx, tm, None, &pred, Some(&class), &[]).await?;
                        if decided.is_some_and(|d| d != allowed) {
                            return Ok(None);
                        }
                        decided = Some(allowed);
                    }
                }
                let otherwise = match decided {
                    Some(allowed) => allowed,
                    None => self.allows(ctx, tm, None, &pred, None, &[]).await?,
                };
                let mut classes = Vec::new();
                if by_class {
                    for class in self.policy_classes.clone() {
                        let allowed = self
                            .allows(ctx, tm, None, &pred, None, std::slice::from_ref(&class))
                            .await?;
                        if allowed != otherwise {
                            classes.push((class, allowed));
                        }
                    }
                }
                let mut by_subject = Vec::new();
                for subject in &subjects {
                    let allowed = self
                        .allows(ctx, tm, Some(subject), &pred, None, &[])
                        .await?;
                    if allowed != otherwise {
                        by_subject.push((subject.clone(), allowed));
                    }
                }
                let verdict = match (classes.is_empty(), by_subject.is_empty(), otherwise) {
                    (true, true, true) => Verdict::Allow,
                    (true, true, false) => Verdict::Deny,
                    (false, _, _) => Verdict::ByClass { classes, otherwise },
                    (true, false, _) => Verdict::BySubject {
                        subjects: by_subject,
                        otherwise,
                    },
                };
                out.insert((tm.iri.clone(), pred), verdict);
            }
        }
        Ok(Some(out))
    }

    pub(crate) fn needs_row_classes(&self, tm: &TriplesMap) -> bool {
        self.has_class_policies && !derived_type_columns(tm).is_empty()
    }

    /// Columns to add to the scan projection so row classes can be derived.
    pub(crate) fn row_class_columns(tm: &TriplesMap) -> Vec<String> {
        derived_type_columns(tm)
    }

    /// Whether this map can yield any row for the pattern: every predicate the
    /// pattern *requires* of a row must be viewable for the map's subjects.
    /// Subject-dependent or row-class-dependent maps are always kept (decided
    /// per row).
    pub(crate) async fn tm_can_yield(
        &mut self,
        ctx: &ExecutionContext<'_>,
        pattern: &R2rmlPattern,
        tm: &TriplesMap,
    ) -> Result<bool> {
        if self.per_subject || self.needs_row_classes(tm) {
            return Ok(true);
        }
        for pred in required_predicates(pattern, tm) {
            // A type triple is decided on the class it states: the map can
            // yield when one of the classes the pattern can read is viewable.
            let objects: Vec<Option<String>> = if pred == rdf::TYPE {
                match (&pattern.class_filter, pattern.type_var) {
                    (Some(class), _) => vec![Some(class.clone())],
                    (None, Some(_)) => static_classes(tm).into_iter().map(Some).collect(),
                    (None, None) => vec![None],
                }
            } else {
                vec![None]
            };
            let mut any = false;
            for object in &objects {
                if self
                    .allows(ctx, tm, None, &pred, object.as_deref(), &[])
                    .await?
                {
                    any = true;
                    break;
                }
            }
            if !any {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Keep only the produced rows whose every read triple is viewable.
    ///
    /// `row_classes`, when present, is parallel to `rows` and carries each
    /// row's column-derived classes (see [`Self::needs_row_classes`]).
    pub(crate) async fn filter_rows(
        &mut self,
        ctx: &ExecutionContext<'_>,
        pattern: &R2rmlPattern,
        tm: &TriplesMap,
        rows: Vec<Vec<(VarId, Binding)>>,
        row_classes: Option<Vec<Vec<String>>>,
    ) -> Result<Vec<Vec<(VarId, Binding)>>> {
        let required = required_predicates(pattern, tm);
        let subject_var = pattern.subject_var;
        let predicate_var = pattern.predicate_var;
        let type_var = pattern.type_var;
        let needs_type = type_var.is_some() && !required.iter().any(|p| p == rdf::TYPE);
        let no_classes: Vec<String> = Vec::new();
        // The class a row's type triple states, which decides it: the class
        // the pattern selects, else the row's projected `?type`. A pattern
        // reading neither (a bare subject) is decided on no specific class.
        let type_object = |row: &[(VarId, Binding)]| -> Option<String> {
            match (&pattern.class_filter, type_var) {
                (Some(class), _) => Some(class.clone()),
                (None, Some(tv)) => bound_iri(row, tv).map(str::to_string),
                (None, None) => None,
            }
        };

        // When no view restriction targets subjects, the map derives no row
        // classes, and no type triple's class varies by row, every input to a
        // decision is fixed for the whole window: the subject is ignored
        // (`allows` substitutes `any_subject`) and the class set is the map's
        // own. Settle `required` once here rather than re-asking it for each
        // of up to a window's worth of rows; only a bound predicate variable
        // still varies per row.
        let type_varies = type_var.is_some() && pattern.class_filter.is_none();
        if !self.per_subject && row_classes.is_none() && !type_varies {
            for pred in &required {
                let object = pattern
                    .class_filter
                    .as_deref()
                    .filter(|_| pred == rdf::TYPE);
                if !self
                    .allows(ctx, tm, None, pred, object, &no_classes)
                    .await?
                {
                    return Ok(Vec::new());
                }
            }
            let Some(pv) = predicate_var else {
                return Ok(rows);
            };
            let mut kept = Vec::with_capacity(rows.len());
            for row in rows {
                let ok = match bound_iri(&row, pv) {
                    Some(p) => {
                        let object = wildcard_type_object(pattern, &row, p);
                        self.allows(ctx, tm, None, p, object, &no_classes).await?
                    }
                    None => true,
                };
                if ok {
                    kept.push(row);
                }
            }
            return Ok(kept);
        }

        let mut kept = Vec::with_capacity(rows.len());
        for (i, row) in rows.into_iter().enumerate() {
            let classes: &[String] = match &row_classes {
                Some(rc) => rc.get(i).map_or(&no_classes, |v| v),
                None => &no_classes,
            };
            let subject: Option<&str> = match pattern.subject_constant.as_deref() {
                Some(s) => Some(s),
                None => subject_var.and_then(|sv| bound_iri(&row, sv)),
            };
            let type_class = type_object(&row);
            let mut ok = true;
            for pred in &required {
                let object = type_class.as_deref().filter(|_| pred == rdf::TYPE);
                if !self.allows(ctx, tm, subject, pred, object, classes).await? {
                    ok = false;
                    break;
                }
            }
            if ok {
                if let Some(pv) = predicate_var {
                    if let Some(p) = bound_iri(&row, pv) {
                        let object = wildcard_type_object(pattern, &row, p);
                        ok = self.allows(ctx, tm, subject, p, object, classes).await?;
                    }
                }
            }
            if ok && needs_type {
                ok = self
                    .allows(ctx, tm, subject, rdf::TYPE, type_class.as_deref(), classes)
                    .await?;
            }
            if ok {
                kept.push(row);
            }
        }
        Ok(kept)
    }

    /// Whether a `(subject, predicate, object)` flake produced by `tm` is
    /// viewable. `subject` is only consulted when the view set targets
    /// subjects. `object` is the class a type triple states, which a type
    /// triple is decided on (a schema class is always viewable, as on a
    /// native ledger); `None` for any other triple, or a type triple of no
    /// specific class, which takes a placeholder no schema class matches.
    ///
    /// A row shows its map's static classes and the classes it derives
    /// (`row_classes`), but its subject can hold more: those another map
    /// minting the same subjects declares, and, where a map's subjects are
    /// not proven one row's or another such map derives classes, those other
    /// rows derive ([`ClassScope::extra`]). Only classes a class-targeted
    /// restriction applying to the predicate selects can change the
    /// decision, so the extra classes `E` are those. The flake is viewable
    /// when the policy allows it for every class set `S` with
    /// `own ⊆ S ⊆ own ∪ E`: the subject's true set is one of them.
    ///
    /// Without `f:required` and `f:query` restrictions two evaluations settle
    /// "every `S` allows". A restriction then applies to a set exactly when
    /// it applies to some subset of it, so the applicable restrictions grow
    /// with `S`; a decision is the default when none applies, a denial when a
    /// denying one does, an allow otherwise. Let `A(S)` say an allowing
    /// restriction applies and `D(S)` a denying one; both only grow with `S`.
    /// With default allow the decision is `¬D(S)`, so every `S` allows iff
    /// `¬D(own ∪ E)`; with default deny it is `A(S) ∧ ¬D(S)`, so every `S`
    /// allows iff `A(own) ∧ ¬D(own ∪ E)`. In both cases that is
    /// `decide(own) ∧ decide(own ∪ E)`. An `f:required` restriction drops the
    /// others when it applies (so a denial can vanish as `S` grows) and an
    /// `f:query` one can block the default, so with either every `S` is
    /// evaluated, up to [`MAX_EXTRA_CLASSES`] extra classes.
    ///
    /// Where the decision is not "allow" for every `S`:
    /// - extra classes known from the mapping alone hide the flake, and the
    ///   map is recorded on the tracker and logged once;
    /// - extra classes that depend on rows the scan does not see refuse the
    ///   query when some `S` would allow it: the answer depends on data the
    ///   decision cannot read.
    async fn allows(
        &mut self,
        ctx: &ExecutionContext<'_>,
        tm: &TriplesMap,
        subject: Option<&str>,
        pred: &str,
        object: Option<&str>,
        row_classes: &[String],
    ) -> Result<bool> {
        let object = object.filter(|_| pred == rdf::TYPE);
        let cache_key = match object {
            Some(class) => format!("{pred} {class}"),
            None => pred.to_string(),
        };
        let mut own: Vec<String> = self.scope.statics.get(&tm.iri).cloned().unwrap_or_default();
        for c in row_classes {
            if !own.contains(c) {
                own.push(c.clone());
            }
        }
        let targeted: Vec<String> = match self.targeted_by_predicate.get(pred) {
            Some(by_pred) => {
                let mut t = self.targeted_any.clone();
                t.extend(by_pred.iter().cloned());
                t
            }
            None => self.targeted_any.clone(),
        };
        let (static_extra, data_extra) = self.scope.extra(&tm.iri, &own, &targeted);
        if static_extra.is_empty() && data_extra.is_empty() {
            return self
                .decide_once(ctx, tm, subject, pred, object, row_classes, &cache_key)
                .await;
        }

        let key = if self.per_subject {
            None
        } else {
            let mut classes = row_classes.to_vec();
            classes.sort();
            Some((tm.iri.clone(), cache_key, classes))
        };
        if let Some(allowed) = key.as_ref().and_then(|k| self.decision_cache.get(k)) {
            return Ok(*allowed);
        }
        let mut extra = static_extra.clone();
        extra.extend(data_extra.iter().cloned());
        let all_allow = if self.enumerate {
            self.check_extra_cap(tm, extra.len())?;
            let mut all = true;
            for subset in 0..(1u32 << extra.len()) {
                if !self
                    .decide_with(ctx, subject, pred, object, &own, &extra, subset)
                    .await?
                {
                    all = false;
                    break;
                }
            }
            all
        } else {
            self.decide_with(ctx, subject, pred, object, &own, &extra, 0)
                .await?
                && self
                    .decide_with(
                        ctx,
                        subject,
                        pred,
                        object,
                        &own,
                        &extra,
                        (1u32 << extra.len()) - 1,
                    )
                    .await?
        };
        let allowed = if all_allow {
            true
        } else if data_extra.is_empty() {
            self.report_undecidable(ctx, tm);
            false
        } else {
            self.check_extra_cap(tm, extra.len())?;
            for subset in 0..(1u32 << extra.len()) {
                if self
                    .decide_with(ctx, subject, pred, object, &own, &extra, subset)
                    .await?
                {
                    return Err(QueryError::InvalidQuery(format!(
                        "Graph source '{}': a class-targeted view policy cannot be decided on the \
                         rows of triples map <{}>: their subjects can hold classes rows the scan \
                         does not read derive ({}), and the decision depends on them. If no two \
                         rows of its table mint one subject, declare the table's key (f:key); \
                         for a history table, map its current rows with an rr:sqlQuery \
                         (`SELECT … WHERE <current-row condition>`) and declare the key there; \
                         or target the policy by property or subject.",
                        self.graph_source_id,
                        tm.iri,
                        data_extra.join(", ")
                    )));
                }
            }
            false
        };
        if let Some(k) = key {
            self.decision_cache.insert(k, allowed);
        }
        Ok(allowed)
    }

    /// Refuse a decision needing more than [`MAX_EXTRA_CLASSES`] extra classes.
    fn check_extra_cap(&self, tm: &TriplesMap, extra: usize) -> Result<()> {
        if extra > MAX_EXTRA_CLASSES {
            return Err(QueryError::InvalidQuery(format!(
                "Graph source '{}': a class-targeted view policy over triples map <{}> would \
                 have to be decided over {extra} classes its subjects might also hold (at most \
                 {MAX_EXTRA_CLASSES}); target the policy by property or subject, or give the \
                 maps minting these subjects keys or templates that keep them apart.",
                self.graph_source_id, tm.iri
            )));
        }
        Ok(())
    }

    /// Record a map whose rows were hidden on classes another map declares.
    fn report_undecidable(&self, ctx: &ExecutionContext<'_>, tm: &TriplesMap) {
        use std::collections::HashSet;
        use std::sync::{Mutex, OnceLock};
        static WARNED: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
        ctx.tracker.record_undecidable_class_map(&tm.iri);
        let first = WARNED
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .map(|mut set| set.insert((self.graph_source_id.clone(), tm.iri.clone())))
            .unwrap_or(true);
        if first {
            tracing::warn!(
                graph_source = %self.graph_source_id,
                triples_map = %tm.iri,
                "a class-targeted view policy hid rows of this triples map: another map \
                 mints the same subjects with other classes, and a row is shown only when \
                 every class set its subject could hold allows it"
            );
        }
    }

    /// One evaluation over `own` plus the members of `extra` that `subset`'s
    /// bits select.
    #[allow(clippy::too_many_arguments)]
    async fn decide_with(
        &mut self,
        ctx: &ExecutionContext<'_>,
        subject: Option<&str>,
        pred: &str,
        object: Option<&str>,
        own: &[String],
        extra: &[String],
        subset: u32,
    ) -> Result<bool> {
        let snapshot = self.encoder.as_ref().unwrap_or(ctx.active_snapshot);
        let mut classes: Vec<Sid> = own.iter().map(|c| encode(snapshot, c)).collect();
        for (i, c) in extra.iter().enumerate() {
            if subset & (1 << i) != 0 {
                classes.push(encode(snapshot, c));
            }
        }
        self.evaluate(ctx, subject, pred, object, &classes).await
    }

    /// The decision on the row's classes alone, cached for subject- and
    /// row-independent decisions.
    #[allow(clippy::too_many_arguments)]
    async fn decide_once(
        &mut self,
        ctx: &ExecutionContext<'_>,
        tm: &TriplesMap,
        subject: Option<&str>,
        pred: &str,
        object: Option<&str>,
        row_classes: &[String],
        cache_key: &str,
    ) -> Result<bool> {
        let static_decision = !self.per_subject && row_classes.is_empty();
        if static_decision {
            if let Some(&allowed) = self
                .static_cache
                .get(tm.iri.as_str())
                .and_then(|preds| preds.get(cache_key))
            {
                return Ok(allowed);
            }
        }
        let snapshot = self.encoder.as_ref().unwrap_or(ctx.active_snapshot);
        let mut classes: Vec<Sid> = self.tm_classes.get(&tm.iri).cloned().unwrap_or_default();
        classes.extend(row_classes.iter().map(|c| encode(snapshot, c)));
        let allowed = self.evaluate(ctx, subject, pred, object, &classes).await?;
        if static_decision {
            self.static_cache
                .entry(tm.iri.clone())
                .or_default()
                .insert(cache_key.to_string(), allowed);
        }
        Ok(allowed)
    }

    /// The policy's decision on one flake of a subject holding `classes`.
    async fn evaluate(
        &self,
        ctx: &ExecutionContext<'_>,
        subject: Option<&str>,
        pred: &str,
        object: Option<&str>,
        classes: &[Sid],
    ) -> Result<bool> {
        #[cfg(test)]
        self.evaluations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let snapshot = self.encoder.as_ref().unwrap_or(ctx.active_snapshot);
        let subject_sid = match (self.per_subject, subject) {
            (true, Some(iri)) => encode(snapshot, iri),
            _ => self.any_subject.clone(),
        };
        let pred_sid = encode(snapshot, pred);
        // The object only matters for the schema-flake exception (an rdf:type
        // whose object is itself a schema class); `f:query` never runs here.
        let object = if pred == rdf::TYPE {
            FlakeValue::Ref(
                object.map_or_else(|| self.any_subject.clone(), |c| encode(snapshot, c)),
            )
        } else {
            FlakeValue::Long(0)
        };
        self.policy
            .allow_view_flake_async(
                &subject_sid,
                &pred_sid,
                &object,
                classes,
                &NoOpQueryExecutor,
                &ctx.tracker,
            )
            .await
            .map_err(|e| QueryError::Policy(e.to_string()))
    }
}

/// The class a wildcard row's triple states when its bound predicate is
/// `rdf:type`: the row's object.
fn wildcard_type_object<'r>(
    pattern: &R2rmlPattern,
    row: &'r [(VarId, Binding)],
    predicate: &str,
) -> Option<&'r str> {
    if predicate != rdf::TYPE {
        return None;
    }
    pattern.object_var.and_then(|ov| bound_iri(row, ov))
}

/// Surface every `f:query` policy in the view set: it cannot run against a
/// virtual source and denies its targets, which a caller would otherwise only
/// see as missing rows. Recorded on the tracker (the tracked response's
/// `policy_enforcement.unevaluable_policies`) and logged once per policy per
/// process — a warning per scan would flood a busy server.
fn report_unevaluable_policies(
    ctx: &ExecutionContext<'_>,
    graph_source_id: &str,
    view: &PolicySet,
) {
    use fluree_db_policy::PolicyValue;
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    // Keyed by (graph source, policy) because the message names both: two
    // sources governed by the same model, or sharing an inline policy id, would
    // otherwise log only whichever scanned first.
    static WARNED: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();

    for r in &view.restrictions {
        if !matches!(r.value, PolicyValue::Query(_)) {
            continue;
        }
        ctx.tracker.record_unevaluable_policy(&r.id);
        let first = WARNED
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .map(|mut set| set.insert((graph_source_id.to_string(), r.id.clone())))
            .unwrap_or(true);
        if first {
            tracing::warn!(
                policy = %r.id,
                graph_source = %graph_source_id,
                "policy uses f:query, which cannot be evaluated against a virtual \
                 (Iceberg / SQL) graph source; its targets are denied. Use static \
                 targeting (f:onClass / f:onProperty / f:onSubject with f:allow) for \
                 policies that govern virtual sources."
            );
        }
    }
}

/// The predicates a row must have viewable to satisfy `pattern` at all: the
/// fixed predicate(s) it binds or constrains, and `rdf:type` when the pattern
/// selects or projects the class. A wildcard's variable predicate is checked
/// per row instead. A pattern that reads nothing specific (bare subject) is
/// gated on `rdf:type` so a fully hidden subject does not surface.
fn required_predicates(pattern: &R2rmlPattern, tm: &TriplesMap) -> Vec<String> {
    let mut preds: Vec<String> = Vec::new();
    if pattern.predicate_var.is_none() {
        if let Some(p) = pattern.predicate_filter.as_deref() {
            preds.push(p.to_string());
        }
    }
    for (p, _) in &pattern.star_bindings {
        preds.push(p.clone());
    }
    for (p, _) in &pattern.star_constraints {
        preds.push(p.clone());
    }
    if pattern.class_filter.is_some() || pattern.type_var.is_some() {
        preds.push(rdf::TYPE.to_string());
    }
    // `static_classes`, not `tm.classes()`: a map that mints its type through a
    // constant `rdf:type` predicate-object map asserts a class just as `rr:class`
    // does, and the gate's `tm_classes` already keys on the wider set.
    if preds.is_empty() && pattern.predicate_var.is_none() && !static_classes(tm).is_empty() {
        preds.push(rdf::TYPE.to_string());
    }
    preds.sort();
    preds.dedup();
    preds
}

fn bound_iri(row: &[(VarId, Binding)], var: VarId) -> Option<&str> {
    row.iter()
        .find(|(v, _)| *v == var)
        .and_then(|(_, b)| match b {
            Binding::Iri(iri) => Some(&**iri),
            _ => None,
        })
}

fn encode(snapshot: &LedgerSnapshot, iri: &str) -> Sid {
    snapshot
        .encode_iri(iri)
        .unwrap_or_else(|| Sid::new(fluree_vocab::namespaces::EMPTY, iri))
}

/// Classes every subject of `tm` carries regardless of row: `rr:class` plus
/// constant-IRI `rdf:type` object maps.
pub(crate) fn static_classes(tm: &TriplesMap) -> Vec<String> {
    let mut classes: Vec<String> = tm.classes().to_vec();
    for pom in &tm.predicate_object_maps {
        if pom.predicate_map.as_constant() != Some(rdf::TYPE) {
            continue;
        }
        if let ObjectMap::Constant {
            value: ConstantValue::Iri(iri),
        } = &pom.object_map
        {
            classes.push(iri.clone());
        }
    }
    classes.sort();
    classes.dedup();
    classes
}

/// The one object map deriving `rdf:type` from one column of `tm`'s row,
/// so a row carries at most one class beyond its static ones and a class
/// is a value of that column. `None` for no derived type, or for shapes
/// (several maps, a multi-column template) a lane cannot key.
pub(crate) fn derived_type_map(tm: &TriplesMap) -> Option<&ObjectMap> {
    let mut derived = tm.predicate_object_maps.iter().filter(|pom| {
        pom.predicate_map.as_constant() == Some(rdf::TYPE)
            && !matches!(pom.object_map, ObjectMap::Constant { .. })
    });
    let om = &derived.next()?.object_map;
    if derived.next().is_some() || om.referenced_columns().len() != 1 {
        return None;
    }
    Some(om)
}

/// Columns feeding a non-constant `rdf:type` object map of `tm`.
pub(crate) fn derived_type_columns(tm: &TriplesMap) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    for pom in &tm.predicate_object_maps {
        if pom.predicate_map.as_constant() != Some(rdf::TYPE) {
            continue;
        }
        if matches!(pom.object_map, ObjectMap::Constant { .. }) {
            continue;
        }
        cols.extend(
            pom.object_map
                .referenced_columns()
                .into_iter()
                .map(ToString::to_string),
        );
    }
    cols.sort();
    cols.dedup();
    cols
}

/// Class→property stats derived from the mapping, standing in for index
/// stats when `f:onClass` view policies are indexed by property: each class
/// is credited with every constant predicate of the maps that declare it.
fn mapping_class_stats(mapping: &CompiledR2rmlMapping, snapshot: &LedgerSnapshot) -> IndexStats {
    let mut by_class: BTreeMap<Sid, BTreeSet<Sid>> = BTreeMap::new();
    for tm in mapping.triples_maps.values() {
        let props: Vec<Sid> = tm
            .predicate_object_maps
            .iter()
            .filter_map(|pom| pom.predicate_map.as_constant())
            .map(|p| encode(snapshot, p))
            .collect();
        for class in static_classes(tm) {
            by_class
                .entry(encode(snapshot, &class))
                .or_default()
                .extend(props.iter().cloned());
        }
    }
    IndexStats {
        classes: Some(
            by_class
                .into_iter()
                .map(|(class_sid, props)| ClassStatEntry {
                    class_sid,
                    count: 0,
                    properties: props
                        .into_iter()
                        .map(|property_sid| ClassPropertyUsage {
                            property_sid,
                            datatypes: Vec::new(),
                            langs: Vec::new(),
                            ref_classes: Vec::new(),
                        })
                        .collect(),
                })
                .collect(),
        ),
        ..IndexStats::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_r2rml::mapping::{PredicateMap, PredicateObjectMap};

    fn tm(iri: &str, classes: &[&str], poms: Vec<PredicateObjectMap>) -> TriplesMap {
        let mut tm = TriplesMap::new(iri, "t");
        tm.subject_map.template = Some("http://ex/s/{id}".to_string());
        tm.subject_map.template_columns = vec!["id".to_string()];
        tm.subject_map.classes = classes.iter().map(ToString::to_string).collect();
        tm.predicate_object_maps = poms;
        tm
    }

    fn pom(pred: &str, object_map: ObjectMap) -> PredicateObjectMap {
        PredicateObjectMap {
            predicate_map: PredicateMap::Constant(pred.to_string()),
            object_map,
        }
    }

    fn templated(
        iri: &str,
        template: &str,
        classes: &[&str],
        poms: Vec<PredicateObjectMap>,
    ) -> TriplesMap {
        let mut map = tm(iri, classes, poms);
        map.subject_map.template = Some(template.to_string());
        map
    }

    /// The classes a subject may hold beyond a row's own: overlapping maps'
    /// declared classes, and classes a deriving map's template can mint where
    /// a row may not show them; only targeted classes count, and none without
    /// a targeted class.
    #[test]
    fn extra_classes_come_from_overlap_and_unproven_derivation() {
        let us = templated(
            "http://ex/us",
            "http://ex/c/{id}",
            &["http://ex/Customer"],
            vec![],
        );
        let eu = templated(
            "http://ex/eu",
            "http://ex/c/{id}",
            &["http://ex/Restricted"],
            vec![],
        );
        let apart = templated(
            "http://ex/other",
            "http://ex/o/{id}",
            &["http://ex/Secret"],
            vec![],
        );
        let hist = templated(
            "http://ex/hist",
            "http://ex/h/{id}",
            &[],
            vec![pom(
                rdf::TYPE,
                ObjectMap::Template {
                    template: "http://ex/city/{city}".to_string(),
                    columns: vec!["city".to_string()],
                    term_type: fluree_db_r2rml::mapping::TermType::Iri,
                    datatype: None,
                    language: None,
                },
            )],
        );
        let mapping = CompiledR2rmlMapping::new(vec![us, eu, apart, hist]);
        let mut scope = ClassScope::of(&mapping);
        let own = |c: &[&str]| c.iter().map(ToString::to_string).collect::<Vec<_>>();
        let targeted = own(&[
            "http://ex/Restricted",
            "http://ex/Secret",
            "http://ex/city/Boston",
            "http://ex/Other",
        ]);
        assert_eq!(
            scope.extra("http://ex/us", &own(&["http://ex/Customer"]), &targeted),
            (own(&["http://ex/Restricted"]), vec![]),
            "an overlapping map's class; a provably apart map's is not"
        );
        assert_eq!(
            scope.extra("http://ex/us", &own(&["http://ex/Customer"]), &[]),
            (vec![], vec![]),
            "no targeted class, nothing to consider"
        );
        assert_eq!(
            scope.extra(
                "http://ex/hist",
                &own(&["http://ex/city/Denver"]),
                &targeted
            ),
            (vec![], own(&["http://ex/city/Boston"])),
            "only classes the template can mint"
        );
        scope.unique.insert("http://ex/hist".to_string(), true);
        assert_eq!(
            scope.extra(
                "http://ex/hist",
                &own(&["http://ex/city/Denver"]),
                &targeted
            ),
            (vec![], vec![]),
            "a subject one row's shows its derived classes"
        );
    }

    /// A gate over `mapping` for one view restriction.
    fn gate_for(
        mapping: &CompiledR2rmlMapping,
        restriction: fluree_db_policy::PolicyRestriction,
        default_allow: bool,
    ) -> R2rmlPolicyGate {
        use crate::policy::QueryPolicyEnforcer;
        use std::sync::Arc;
        let snapshot = LedgerSnapshot::genesis("gs:main");
        let vars = crate::var_registry::VarRegistry::new();
        let view = build_policy_set(vec![restriction], None, PolicyAction::View, None);
        let wrapper = PolicyWrapper::new(
            view,
            PolicySet::default(),
            false,
            default_allow,
            HashMap::new(),
        );
        let enforcer = Arc::new(QueryPolicyEnforcer::new(Arc::new(PolicyContext::new(
            wrapper, None,
        ))));
        let ctx = ExecutionContext::new(&snapshot, &vars).with_policy_enforcer(enforcer);
        futures::executor::block_on(R2rmlPolicyGate::build(&ctx, mapping, "gs:main"))
            .expect("build")
            .expect("a gate")
    }

    fn restriction(
        mode: TargetMode,
        targets: &[&str],
        classes: &[&str],
        allow: bool,
    ) -> fluree_db_policy::PolicyRestriction {
        let snapshot = LedgerSnapshot::genesis("gs:main");
        fluree_db_policy::PolicyRestriction {
            id: "ex:p".to_string(),
            target_mode: mode,
            targets: targets.iter().map(|t| encode(&snapshot, t)).collect(),
            action: PolicyAction::View,
            verbs: None,
            value: if allow {
                fluree_db_policy::PolicyValue::Allow
            } else {
                fluree_db_policy::PolicyValue::Deny
            },
            required: false,
            message: None,
            class_policy: !classes.is_empty(),
            for_classes: classes.iter().map(|c| encode(&snapshot, c)).collect(),
            class_check_needed: !classes.is_empty(),
        }
    }

    fn decide(gate: &mut R2rmlPolicyGate, tm: &TriplesMap, pred: &str) -> bool {
        let snapshot = LedgerSnapshot::genesis("gs:main");
        let vars = crate::var_registry::VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        futures::executor::block_on(gate.allows(&ctx, tm, None, pred, None, &[])).expect("decide")
    }

    /// Without a class-targeted restriction nothing about other maps'
    /// classes is worked out and each (map, predicate) costs one evaluation,
    /// as before; with one, a map another differently classed map overlaps is
    /// decided over its class sets.
    #[test]
    fn a_policy_without_classes_costs_one_evaluation_per_decision() {
        use std::sync::atomic::Ordering::Relaxed;
        let us = templated(
            "http://ex/us",
            "http://ex/c/{id}",
            &["http://ex/Customer"],
            vec![],
        );
        let eu = templated(
            "http://ex/eu",
            "http://ex/c/{id}",
            &["http://ex/Restricted"],
            vec![],
        );
        let mapping = CompiledR2rmlMapping::new(vec![us.clone(), eu]);

        let by_property = restriction(TargetMode::OnProperty, &["http://ex/age"], &[], false);
        let mut gate = gate_for(&mapping, by_property, true);
        assert!(
            gate.scope.overlaps.is_empty(),
            "no scope without class policies"
        );
        assert!(decide(&mut gate, &us, "http://ex/name"));
        assert!(decide(&mut gate, &us, "http://ex/name"));
        assert!(!decide(&mut gate, &us, "http://ex/age"));
        assert_eq!(
            gate.evaluations.load(Relaxed),
            2,
            "one per (map, predicate)"
        );

        let by_class = restriction(TargetMode::OnClass, &[], &["http://ex/Restricted"], false);
        let mut gate = gate_for(&mapping, by_class, true);
        assert!(
            !decide(&mut gate, &us, "http://ex/name"),
            "denied for the EU map's class"
        );
        assert_eq!(
            gate.evaluations.load(Relaxed),
            2,
            "own, and own with the extra class"
        );
        assert!(!decide(&mut gate, &us, "http://ex/name"));
        assert_eq!(gate.evaluations.load(Relaxed), 2, "cached");
    }

    /// More extra classes than the enumeration's cap refuse the decision,
    /// naming the map and the count.
    #[test]
    fn a_decision_over_too_many_extra_classes_is_refused() {
        let classes: Vec<String> = (0..=MAX_EXTRA_CLASSES)
            .map(|i| format!("http://ex/K{i}"))
            .collect();
        let mut maps = vec![templated(
            "http://ex/plain",
            "http://ex/c/{id}",
            &[],
            vec![],
        )];
        for (i, c) in classes.iter().enumerate() {
            maps.push(templated(
                &format!("http://ex/m{i}"),
                "http://ex/c/{id}",
                &[c],
                vec![],
            ));
        }
        let mapping = CompiledR2rmlMapping::new(maps.clone());
        let class_refs: Vec<&str> = classes.iter().map(String::as_str).collect();
        let mut required = restriction(TargetMode::OnClass, &[], &class_refs, true);
        required.required = true;
        let mut gate = gate_for(&mapping, required, true);
        let snapshot = LedgerSnapshot::genesis("gs:main");
        let vars = crate::var_registry::VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let err = futures::executor::block_on(gate.allows(
            &ctx,
            &maps[0],
            None,
            "http://ex/name",
            None,
            &[],
        ))
        .expect_err("refused");
        let text = err.to_string();
        assert!(
            text.contains("http://ex/plain")
                && text.contains(&format!("{}", MAX_EXTRA_CLASSES + 1)),
            "{text}"
        );
    }

    #[test]
    fn overlap_warnings_name_differently_classed_overlapping_maps() {
        let mapping = CompiledR2rmlMapping::new(vec![
            templated(
                "http://ex/us",
                "http://ex/c/{id}",
                &["http://ex/Customer"],
                vec![],
            ),
            templated(
                "http://ex/eu",
                "http://ex/c/{id}",
                &["http://ex/Restricted"],
                vec![],
            ),
            templated(
                "http://ex/same",
                "http://ex/c/{id}",
                &["http://ex/Customer"],
                vec![],
            ),
            templated(
                "http://ex/other",
                "http://ex/o/{id}",
                &["http://ex/Secret"],
                vec![],
            ),
        ]);
        let warnings = class_overlap_warnings(&mapping);
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(
            warnings.iter().all(|w| w.contains("http://ex/eu")),
            "{warnings:?}"
        );
        assert!(!warnings.iter().any(|w| w.contains("http://ex/other")));
    }

    #[test]
    fn static_classes_union_rr_class_and_constant_type_maps() {
        let map = tm(
            "http://ex/tm",
            &["http://ex/B"],
            vec![
                pom(rdf::TYPE, ObjectMap::constant_iri("http://ex/A")),
                pom(rdf::TYPE, ObjectMap::column_iri("kind")),
                pom("http://ex/name", ObjectMap::column("name")),
            ],
        );
        assert_eq!(static_classes(&map), vec!["http://ex/A", "http://ex/B"]);
        assert_eq!(derived_type_columns(&map), vec!["kind"]);
    }

    #[test]
    fn required_predicates_cover_every_pattern_shape() {
        let map = tm("http://ex/tm", &["http://ex/C"], vec![]);
        let mut p = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)));
        p.predicate_filter = Some("http://ex/name".to_string());
        p.star_bindings = vec![("http://ex/age".to_string(), VarId(2))];
        p.star_constraints = vec![(
            "http://ex/status".to_string(),
            crate::r2rml::ObjectConstant::Iri("http://ex/live".to_string()),
        )];
        p.class_filter = Some("http://ex/C".to_string());
        let mut got = required_predicates(&p, &map);
        got.sort();
        assert_eq!(
            got,
            vec![
                "http://ex/age",
                "http://ex/name",
                "http://ex/status",
                rdf::TYPE
            ]
        );

        // A wildcard checks its variable predicate per row, not up front.
        let mut w = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)));
        w.predicate_var = Some(VarId(2));
        assert!(required_predicates(&w, &map).is_empty());

        // A bare subject scan over a classed map is gated on rdf:type.
        let bare = R2rmlPattern::new("gs:main", VarId(0), None);
        assert_eq!(required_predicates(&bare, &map), vec![rdf::TYPE]);
    }

    #[test]
    fn mapping_stats_credit_each_class_with_its_maps_predicates() {
        let mapping = CompiledR2rmlMapping::new(vec![
            tm(
                "http://ex/people",
                &["http://ex/Person"],
                vec![pom("http://ex/name", ObjectMap::column("name"))],
            ),
            tm(
                "http://ex/staff",
                &["http://ex/Person", "http://ex/Employee"],
                vec![pom("http://ex/salary", ObjectMap::column("salary"))],
            ),
        ]);
        let snapshot = LedgerSnapshot::genesis("gs:main");
        let stats = mapping_class_stats(&mapping, &snapshot);
        let classes = stats.classes.expect("classes");
        let props = |class: &str| -> Vec<String> {
            let sid = encode(&snapshot, class);
            let mut v: Vec<String> = classes
                .iter()
                .find(|c| c.class_sid == sid)
                .map(|c| {
                    c.properties
                        .iter()
                        .map(|p| snapshot.decode_sid(&p.property_sid).unwrap())
                        .collect()
                })
                .unwrap_or_default();
            v.sort();
            v
        };
        assert_eq!(
            props("http://ex/Person"),
            vec!["http://ex/name", "http://ex/salary"]
        );
        assert_eq!(props("http://ex/Employee"), vec!["http://ex/salary"]);
    }
}
