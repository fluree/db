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
//! targets specific subjects or the map derives `rdf:type` from a column, so
//! the per-row cost is a hash lookup in the common case, and a map whose
//! required predicates are all denied is skipped before its table is scanned.

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
    /// `(TriplesMap IRI, predicate IRI)` → allowed, for subject-independent maps.
    static_cache: HashMap<(String, String), bool>,
}

impl R2rmlPolicyGate {
    /// Build the gate for this scan, or `None` when no view policy is active
    /// (no enforcer, or a root enforcer) and the scan may run unfiltered.
    pub(crate) fn build(
        ctx: &ExecutionContext<'_>,
        mapping: &CompiledR2rmlMapping,
    ) -> Option<Self> {
        if ctx.allow_unfiltered() {
            return None;
        }
        let enforcer = ctx.policy_enforcer.as_ref()?;
        let base = enforcer.policy();
        let snapshot = ctx.active_snapshot;

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
        let per_subject = !view.by_subject.is_empty();
        let has_class_policies = view.restrictions.iter().any(|r| r.class_policy);
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

        Some(Self {
            policy,
            per_subject,
            has_class_policies,
            any_subject: Sid::new(fluree_vocab::namespaces::EMPTY, ""),
            tm_classes,
            static_cache: HashMap::new(),
        })
    }

    /// Whether this map's rows carry classes the mapping cannot know
    /// statically (a column/template/ref `rdf:type` object map) that a class
    /// policy would need — the scan must then project those columns and
    /// materialize each row's classes.
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
            if !self.allows(ctx, tm, None, &pred, &[]).await? {
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
        let no_classes: Vec<String> = Vec::new();

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
            let mut ok = true;
            for pred in &required {
                if !self.allows(ctx, tm, subject, pred, classes).await? {
                    ok = false;
                    break;
                }
            }
            if ok {
                if let Some(pv) = predicate_var {
                    if let Some(p) = bound_iri(&row, pv) {
                        let p = p.to_string();
                        ok = self.allows(ctx, tm, subject, &p, classes).await?;
                    }
                }
            }
            if ok && type_var.is_some() && !required.iter().any(|p| p == rdf::TYPE) {
                ok = self.allows(ctx, tm, subject, rdf::TYPE, classes).await?;
            }
            if ok {
                kept.push(row);
            }
        }
        Ok(kept)
    }

    /// Whether a `(subject, predicate)` flake produced by `tm` is viewable.
    /// `subject` is only consulted when the view set targets subjects.
    async fn allows(
        &mut self,
        ctx: &ExecutionContext<'_>,
        tm: &TriplesMap,
        subject: Option<&str>,
        pred: &str,
        row_classes: &[String],
    ) -> Result<bool> {
        let static_decision = !self.per_subject && row_classes.is_empty();
        let key = (tm.iri.clone(), pred.to_string());
        if static_decision {
            if let Some(&allowed) = self.static_cache.get(&key) {
                return Ok(allowed);
            }
        }

        let snapshot = ctx.active_snapshot;
        let mut classes: Vec<Sid> = self.tm_classes.get(&tm.iri).cloned().unwrap_or_default();
        classes.extend(row_classes.iter().filter_map(|c| snapshot.encode_iri(c)));
        let subject_sid = match (self.per_subject, subject) {
            (true, Some(iri)) => encode(snapshot, iri),
            _ => self.any_subject.clone(),
        };
        let pred_sid = encode(snapshot, pred);
        // The object only matters for the schema-flake bypass (rdf:type whose
        // object is itself a schema class); data rows never qualify, and
        // `f:query` never runs here, so a class ref / placeholder suffices.
        let object = if pred == rdf::TYPE {
            FlakeValue::Ref(
                classes
                    .first()
                    .cloned()
                    .unwrap_or_else(|| self.any_subject.clone()),
            )
        } else {
            FlakeValue::Long(0)
        };

        let allowed = self
            .policy
            .allow_view_flake_async(
                &subject_sid,
                &pred_sid,
                &object,
                &classes,
                &NoOpQueryExecutor,
                &ctx.tracker,
            )
            .await
            .map_err(|e| QueryError::Policy(e.to_string()))?;
        if static_decision {
            self.static_cache.insert(key, allowed);
        }
        Ok(allowed)
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
    if preds.is_empty() && pattern.predicate_var.is_none() && !tm.classes().is_empty() {
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

/// Columns feeding a non-constant `rdf:type` object map of `tm`.
fn derived_type_columns(tm: &TriplesMap) -> Vec<String> {
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
