//! Neo4j `db.*` / `dbms.*` procedure shims for Cypher clients.
//!
//! Graph tooling (Neo4j Browser, LangChain, driver smoke tests) introspects
//! the database through built-in procedures — `CALL db.labels() YIELD label`
//! and friends — before it issues real queries. Fluree answers the common
//! ones from ledger statistics instead of executing a scan: a
//! [`ProcedureCall`](fluree_db_cypher::ast::ProcedureCall) statement is
//! rewritten here into a constant-rows [`Query`](fluree_db_cypher::ast::Query)
//! AST (an `InlineRows` VALUES source plus the caller's YIELD/WHERE/RETURN),
//! which then lowers and executes through the ordinary read pipeline — so
//! projection, filtering, ordering, and result formatting all behave exactly
//! like any other Cypher read.
//!
//! Answers come from the HEAD-index stats merged with novelty
//! ([`assemble_fast_stats`]), so labels and types written since the last
//! index build are visible. Like Neo4j's own catalog procedures, the
//! answers are lenient about tombstones: a label or key whose every fact
//! was later retracted may keep appearing until a reindex.
//!
//! Supported: `db.labels`, `db.relationshipTypes`, `db.propertyKeys`,
//! `db.schema.visualization` (best effort), `dbms.components`,
//! `apoc.meta.data` (the LangChain `Neo4jGraph` schema fetch).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use fluree_db_core::{
    is_rdf_type, FlakeValue, IndexStats, IndexType, LedgerSnapshot, OverlayProvider, Sid,
    ValueTypeTag,
};
use fluree_db_cypher::ast::{
    Expr, Literal, ProcedureCall, ProjectionItem, Query, ReadClause, ReturnClause, Variable,
    WithClause,
};
use fluree_db_novelty::{assemble_fast_stats, Novelty};
use fluree_db_query::policy::QueryPolicyEnforcer;

use crate::error::ApiError;
use crate::Result;

/// The Neo4j version this server impersonates for compatibility probes.
// SYNC: keep in step with `server_agent()` in fluree-db-server/src/bolt.rs —
// drivers and Browser feature-gate on the version from both surfaces.
const COMPAT_NEO4J_VERSION: &str = "5.4.0";

/// Rewrite a standalone `CALL proc(…) [YIELD …] [RETURN …]` statement into a
/// constant-rows read query answering the procedure from ledger stats.
///
/// `vocab` / `overrides` are the ledger-context IRI mappings (the same ones
/// the lowering applies to bare identifiers) — label/type/key names are
/// rendered back through them so `db.labels()` returns the identifiers a
/// user would actually write in a MATCH.
///
/// `policy` is the caller's view-policy enforcer, if any. These procedures
/// answer from aggregate ledger stats rather than a per-flake scan, so an
/// explicit unconditional `Deny` targeting a class/predicate excludes it from
/// the catalog rather than being answered unfiltered (see
/// [`class_denied`]/[`predicate_denied`]).
pub(crate) fn procedure_call_query(
    call: &ProcedureCall,
    snapshot: &LedgerSnapshot,
    overlay: Option<&dyn OverlayProvider>,
    vocab: Option<&str>,
    overrides: &HashMap<String, String>,
    policy: Option<&QueryPolicyEnforcer>,
) -> Result<Query> {
    let span = call.span;
    let lit = |s: &str| Expr::Lit(Literal::String(s.to_string(), span));
    let name = call.name.to_ascii_lowercase();
    // Root (no policy attached, or an explicitly unrestricted policy) sees
    // the full catalog unfiltered — the same as today's behavior.
    let policy = policy.filter(|p| !p.is_root());

    let (columns, rows): (&[&str], Vec<Vec<Expr>>) = match name.as_str() {
        "db.labels" => {
            require_no_args(call)?;
            let names = schema_names(snapshot, overlay, vocab, overrides, policy);
            (
                &["label"],
                names.labels.iter().map(|l| vec![lit(l)]).collect(),
            )
        }
        "db.relationshiptypes" => {
            require_no_args(call)?;
            let names = schema_names(snapshot, overlay, vocab, overrides, policy);
            (
                &["relationshipType"],
                names.rel_types.iter().map(|t| vec![lit(t)]).collect(),
            )
        }
        "db.propertykeys" => {
            require_no_args(call)?;
            let names = schema_names(snapshot, overlay, vocab, overrides, policy);
            (
                &["propertyKey"],
                names.prop_keys.iter().map(|k| vec![lit(k)]).collect(),
            )
        }
        "db.schema.visualization" => {
            require_no_args(call)?;
            let names = schema_names(snapshot, overlay, vocab, overrides, policy);
            let nodes = Expr::List(
                names
                    .labels
                    .iter()
                    .map(|l| {
                        Expr::Map(
                            vec![
                                ("name".to_string(), lit(l)),
                                ("labels".to_string(), Expr::List(vec![lit(l)], span)),
                            ],
                            span,
                        )
                    })
                    .collect(),
                span,
            );
            let relationships = Expr::List(
                names
                    .rel_types
                    .iter()
                    .map(|t| Expr::Map(vec![("name".to_string(), lit(t))], span))
                    .collect(),
                span,
            );
            (
                &["nodes", "relationships"],
                vec![vec![nodes, relationships]],
            )
        }
        "dbms.components" => {
            require_no_args(call)?;
            // Mirrors the Bolt handshake's `Neo4j/<semver> (compatible; …)`
            // convention: a Neo4j-parseable identity with Fluree attribution.
            let edition = format!(
                "community (compatible; Fluree/{})",
                env!("CARGO_PKG_VERSION")
            );
            (
                &["name", "versions", "edition"],
                vec![vec![
                    lit("Neo4j Kernel"),
                    Expr::List(vec![lit(COMPAT_NEO4J_VERSION)], span),
                    Expr::Lit(Literal::String(edition, span)),
                ]],
            )
        }
        "apoc.meta.data" => {
            require_no_args(call)?;
            (
                META_DATA_COLUMNS,
                meta_data_rows(call, snapshot, overlay, vocab, overrides, policy),
            )
        }
        _ => {
            return Err(ApiError::cypher(
                format!(
                    "unknown procedure `{}` — supported: db.labels, db.relationshipTypes, \
                     db.propertyKeys, db.schema.visualization, dbms.components, apoc.meta.data",
                    call.name
                ),
                Vec::new(),
            ))
        }
    };

    build_query(call, columns, rows)
}

/// The full `apoc.meta.data` column set, so any tooling YIELD succeeds.
/// Constraint/index/degree columns answer constant defaults (Fluree has no
/// user-managed constraint catalog and does not track per-pair degrees).
const META_DATA_COLUMNS: &[&str] = &[
    "label",
    "property",
    "count",
    "unique",
    "index",
    "existence",
    "type",
    "array",
    "sample",
    "left",
    "right",
    "leftCount",
    "rightCount",
    "other",
    "otherLabels",
    "elementType",
];

/// Per-(label, property) schema rows in `apoc.meta.data` shape:
///
/// - node property: `elementType: "node"`, `type: "STRING"/"INTEGER"/…`
/// - outgoing relationship: `elementType: "node"`, `type: "RELATIONSHIP"`,
///   `property` = the relationship type, `other` = end-node labels
///
/// Rows attributing edge-annotation properties to relationship types
/// (`elementType: "relationship"`) are not emitted — annotation subjects
/// carry no class, and consumers (LangChain) tolerate their absence.
///
/// Attribution sources: the HEAD index's per-class property usage (exact),
/// plus a two-pass walk of novelty (subject classes from novelty `rdf:type`,
/// then per-class attribution of novelty property flakes). A novel property
/// on a subject whose only `rdf:type` fact is already indexed is attributed
/// only after a reindex — the same staleness leniency as the other shims
/// (and `apoc.meta.data` itself is sampled in Neo4j).
fn meta_data_rows(
    call: &ProcedureCall,
    snapshot: &LedgerSnapshot,
    overlay: Option<&dyn OverlayProvider>,
    vocab: Option<&str>,
    overrides: &HashMap<String, String>,
    policy: Option<&QueryPolicyEnforcer>,
) -> Vec<Vec<Expr>> {
    let span = call.span;

    // (class, property) -> per-meta-type counts, and
    // (class, property) -> (ref count, end classes) — Sid-keyed until render.
    let mut props: BTreeMap<(Sid, Sid), BTreeMap<&'static str, i64>> = BTreeMap::new();
    let mut rels: BTreeMap<(Sid, Sid), (i64, BTreeSet<Sid>)> = BTreeMap::new();
    let ref_tag = ValueTypeTag::JSON_LD_ID.as_u8();

    // Source 1: indexed per-class property usage (exact attribution).
    let indexed = snapshot.stats.clone().unwrap_or_default();
    for class_entry in indexed.classes.iter().flatten() {
        for usage in &class_entry.properties {
            if is_rdf_type(&usage.property_sid) {
                continue;
            }
            let key = (class_entry.class_sid.clone(), usage.property_sid.clone());
            for &(tag, count) in &usage.datatypes {
                if count == 0 {
                    continue;
                }
                if tag == ref_tag {
                    let entry = rels.entry(key.clone()).or_default();
                    entry.0 += count as i64;
                    entry
                        .1
                        .extend(usage.ref_classes.iter().map(|rc| rc.class_sid.clone()));
                } else {
                    *props
                        .entry(key.clone())
                        .or_default()
                        .entry(meta_type_name(tag))
                        .or_insert(0) += count as i64;
                }
            }
        }
    }

    // Source 2: novelty attribution. Pass A collects subject classes from
    // novelty `rdf:type`; pass B attributes novelty property flakes to them.
    if let Some(novelty) = overlay.and_then(|o| o.as_any().downcast_ref::<Novelty>()) {
        let mut subject_classes: HashMap<Sid, BTreeSet<Sid>> = HashMap::new();
        for flake in novelty.iter_flakes(IndexType::Post) {
            if !meta_include(flake) || !is_rdf_type(&flake.p) {
                continue;
            }
            if let FlakeValue::Ref(class) = &flake.o {
                let classes = subject_classes.entry(flake.s.clone()).or_default();
                if flake.op {
                    classes.insert(class.clone());
                } else {
                    classes.remove(class);
                }
            }
        }
        for flake in novelty.iter_flakes(IndexType::Post) {
            if !meta_include(flake) || is_rdf_type(&flake.p) {
                continue;
            }
            let Some(classes) = subject_classes.get(&flake.s) else {
                continue;
            };
            let delta = if flake.op { 1 } else { -1 };
            for class in classes {
                let key = (class.clone(), flake.p.clone());
                if let FlakeValue::Ref(target) = &flake.o {
                    let entry = rels.entry(key).or_default();
                    entry.0 += delta;
                    if let Some(target_classes) = subject_classes.get(target) {
                        entry.1.extend(target_classes.iter().cloned());
                    }
                } else {
                    *props
                        .entry(key)
                        .or_default()
                        .entry(flake_meta_type_name(&flake.o))
                        .or_insert(0) += delta;
                }
            }
        }
    }

    // Render: one row per (label, property, type) for node properties, one
    // per (label, relationship type) for relationships. System vocabulary
    // and label-less entries drop out via `display_name`.
    let lit = |s: &str| Expr::Lit(Literal::String(s.to_string(), span));
    let int = |n: i64| Expr::Lit(Literal::Integer(n, span));
    let boolean = |b: bool| Expr::Lit(Literal::Bool(b, span));
    let display = |sid: &Sid| display_name(sid, snapshot, vocab, overrides);

    let mut rows = Vec::new();
    let mut push_row =
        |label: &str, property: &str, count: i64, type_name: &str, other: Vec<Expr>| {
            rows.push(vec![
                lit(label),
                lit(property),
                int(count),
                boolean(false), // unique
                boolean(false), // index
                boolean(false), // existence
                lit(type_name),
                boolean(false), // array
                Expr::Lit(Literal::Null(span)),
                int(0), // left
                int(0), // right
                int(0), // leftCount
                int(0), // rightCount
                Expr::List(other.clone(), span),
                Expr::List(other, span),
                lit("node"),
            ]);
        };

    let denied = |class: &Sid, property: &Sid| {
        policy.is_some_and(|p| class_denied(p, class) || predicate_denied(p, property))
    };

    for ((class, property), by_type) in &props {
        if denied(class, property) {
            continue;
        }
        let (Some(label), Some(prop)) = (display(class), display(property)) else {
            continue;
        };
        for (type_name, &count) in by_type {
            if count > 0 {
                push_row(&label, &prop, count, type_name, Vec::new());
            }
        }
    }
    for ((class, property), (count, ends)) in &rels {
        if *count <= 0 || denied(class, property) {
            continue;
        }
        let (Some(label), Some(rel_type)) = (display(class), display(property)) else {
            continue;
        };
        // End-node labels are informational (LangChain uses them for schema
        // hints, not access), but drop ones an explicit class deny hides too.
        let other: Vec<Expr> = ends
            .iter()
            .filter(|end| !policy.is_some_and(|p| class_denied(p, end)))
            .filter_map(&display)
            .map(|n| lit(&n))
            .collect();
        push_row(&label, &rel_type, *count, "RELATIONSHIP", other);
    }
    rows
}

/// Mirror of the runtime-stats flake filter: commit metadata and txn-meta
/// graph flakes are not part of the user's property graph.
fn meta_include(flake: &fluree_db_core::Flake) -> bool {
    if flake.s.namespace_code == fluree_vocab::namespaces::FLUREE_COMMIT {
        return false;
    }
    if let Some(g) = &flake.g {
        if g.name.as_ref().contains("txn-meta") {
            return false;
        }
    }
    true
}

/// `ValueTypeTag` → `apoc.meta.data` type name.
fn meta_type_name(tag: u8) -> &'static str {
    use ValueTypeTag as T;
    match ValueTypeTag::from_u8(tag) {
        T::BOOLEAN => "BOOLEAN",
        T::INTEGER
        | T::LONG
        | T::INT
        | T::SHORT
        | T::BYTE
        | T::UNSIGNED_LONG
        | T::UNSIGNED_INT
        | T::UNSIGNED_SHORT
        | T::UNSIGNED_BYTE
        | T::NON_NEGATIVE_INTEGER
        | T::POSITIVE_INTEGER
        | T::NON_POSITIVE_INTEGER
        | T::NEGATIVE_INTEGER => "INTEGER",
        T::DOUBLE | T::FLOAT | T::DECIMAL => "FLOAT",
        T::DATE_TIME => "DATE_TIME",
        T::DATE => "DATE",
        T::TIME => "TIME",
        T::DURATION | T::DAY_TIME_DURATION | T::YEAR_MONTH_DURATION => "DURATION",
        _ => "STRING",
    }
}

/// `FlakeValue` → `apoc.meta.data` type name (novelty flakes carry values,
/// not stat tags).
fn flake_meta_type_name(v: &FlakeValue) -> &'static str {
    match v {
        FlakeValue::Boolean(_) => "BOOLEAN",
        FlakeValue::Long(_) | FlakeValue::BigInt(_) => "INTEGER",
        FlakeValue::Double(_) | FlakeValue::Decimal(_) => "FLOAT",
        FlakeValue::DateTime(_) => "DATE_TIME",
        FlakeValue::Date(_) => "DATE",
        FlakeValue::Time(_) => "TIME",
        FlakeValue::Duration(_)
        | FlakeValue::DayTimeDuration(_)
        | FlakeValue::YearMonthDuration(_) => "DURATION",
        _ => "STRING",
    }
}

fn require_no_args(call: &ProcedureCall) -> Result<()> {
    if call.args.is_empty() {
        Ok(())
    } else {
        Err(ApiError::cypher(
            format!("procedure `{}` takes no arguments", call.name),
            Vec::new(),
        ))
    }
}

/// Sorted display names for the ledger's labels (classes), relationship
/// types (ref-object predicates), and property keys (literal-object
/// predicates), from novelty-merged index stats.
struct SchemaNames {
    labels: BTreeSet<String>,
    rel_types: BTreeSet<String>,
    prop_keys: BTreeSet<String>,
}

/// Render a class/predicate SID back to the identifier a Cypher user would
/// write: term overrides reversed, `@vocab` prefix stripped, otherwise the
/// full IRI. `None` for Fluree system vocabulary (commit metadata,
/// edge-annotation reifiers, …) — not part of the user's property graph.
fn display_name(
    sid: &Sid,
    snapshot: &LedgerSnapshot,
    vocab: Option<&str>,
    overrides: &HashMap<String, String>,
) -> Option<String> {
    let prefix = snapshot.namespaces().get(&sid.namespace_code)?;
    let iri = format!("{}{}", prefix, sid.name);
    if iri.starts_with("https://ns.flur.ee/") {
        return None;
    }
    if let Some(short) = overrides
        .iter()
        .find_map(|(short, target)| (target == &iri).then_some(short))
    {
        return Some(short.clone());
    }
    if let Some(rest) = vocab.and_then(|v| iri.strip_prefix(v)) {
        if !rest.is_empty() {
            return Some(rest.to_string());
        }
    }
    Some(iri)
}

fn schema_names(
    snapshot: &LedgerSnapshot,
    overlay: Option<&dyn OverlayProvider>,
    vocab: Option<&str>,
    overrides: &HashMap<String, String>,
    policy: Option<&QueryPolicyEnforcer>,
) -> SchemaNames {
    let stats = merged_stats(snapshot, overlay);
    let display = |sid: &Sid| display_name(sid, snapshot, vocab, overrides);

    let mut names = SchemaNames {
        labels: BTreeSet::new(),
        rel_types: BTreeSet::new(),
        prop_keys: BTreeSet::new(),
    };

    for entry in stats.classes.iter().flatten() {
        if entry.count == 0 {
            continue;
        }
        if policy.is_some_and(|p| class_denied(p, &entry.class_sid)) {
            continue;
        }
        if let Some(name) = display(&entry.class_sid) {
            names.labels.insert(name);
        }
    }

    let ref_tag = ValueTypeTag::JSON_LD_ID.as_u8();
    for entry in stats.properties.iter().flatten() {
        if entry.count == 0 {
            continue;
        }
        let sid = Sid::new(entry.sid.0, &entry.sid.1);
        if is_rdf_type(&sid) {
            continue;
        }
        if policy.is_some_and(|p| predicate_denied(p, &sid)) {
            continue;
        }
        let Some(name) = display(&sid) else { continue };
        let has_ref = entry
            .datatypes
            .iter()
            .any(|&(dt, c)| dt == ref_tag && c > 0);
        let has_literal = entry
            .datatypes
            .iter()
            .any(|&(dt, c)| dt != ref_tag && c > 0);
        if has_ref {
            names.rel_types.insert(name.clone());
        }
        // No datatype breakdown (older index formats) counts as a property
        // key — the common case, and the safer default for graph viz.
        if has_literal || entry.datatypes.is_empty() {
            names.prop_keys.insert(name);
        }
    }

    names
}

/// True when an unconditional `Deny` restriction targets `class_sid` directly
/// (an `f:onClass` restriction whose `for_classes` includes it, for a View or
/// Both action).
///
/// This is deliberately narrower than "is this class guaranteed visible":
/// these procedures answer from aggregate stats with no per-instance
/// filtered-scan fallback, so precisely reproducing ordinary `MATCH`
/// enforcement (subject-scoped restrictions, default-bucket policies,
/// conditional `f:query` restrictions) isn't possible without evaluating
/// per-subject — which would defeat the point of a stats-only answer. What
/// *is* tractable, and closes the common "hide this whole class/property
/// from schema introspection" case, is catching an explicit, unconditional
/// deny that names the class outright. A blanket `default_allow: true`
/// fallback (the common shape paired with a handful of specific denies)
/// leaves everything else listed, matching what an ordinary read would show
/// for those classes.
fn class_denied(enforcer: &QueryPolicyEnforcer, class_sid: &Sid) -> bool {
    let view = enforcer.policy().wrapper().view();
    view.restrictions.iter().any(|r| {
        r.class_policy
            && r.for_classes.contains(class_sid)
            && matches!(r.value, fluree_db_policy::PolicyValue::Deny)
            && matches!(
                r.action,
                fluree_db_policy::PolicyAction::View | fluree_db_policy::PolicyAction::Both
            )
    })
}

/// True when an unconditional `Deny` restriction targets `p` directly (an
/// `f:onProperty` restriction, or an `f:onClass` restriction where `p` is
/// exclusive to that class so no runtime class check is needed — see
/// [`class_denied`] for why this checks only the unconditional case).
fn predicate_denied(enforcer: &QueryPolicyEnforcer, p: &Sid) -> bool {
    let view = enforcer.policy().wrapper().view();
    view.by_property.get(p).is_some_and(|entries| {
        entries.iter().any(|e| {
            !e.class_check_needed && {
                let r = &view.restrictions[e.idx];
                matches!(r.value, fluree_db_policy::PolicyValue::Deny)
                    && matches!(
                        r.action,
                        fluree_db_policy::PolicyAction::View | fluree_db_policy::PolicyAction::Both
                    )
            }
        })
    })
}

/// HEAD-index stats merged with novelty, so labels/types/keys written since
/// the last index build are visible. Non-`Novelty` overlays (policy views)
/// contribute no new schema, so the indexed stats stand alone there.
fn merged_stats(snapshot: &LedgerSnapshot, overlay: Option<&dyn OverlayProvider>) -> IndexStats {
    let indexed = snapshot.stats.clone().unwrap_or_default();
    match overlay.and_then(|o| o.as_any().downcast_ref::<Novelty>()) {
        Some(novelty) => assemble_fast_stats(&indexed, snapshot, novelty, i64::MAX, None),
        None => indexed,
    }
}

/// Assemble the constant-rows query: `InlineRows` over the procedure's
/// columns, a `WITH` realizing YIELD projection/renames and its WHERE, and
/// the caller's RETURN (or an implicit RETURN of the visible columns).
fn build_query(call: &ProcedureCall, columns: &[&str], rows: Vec<Vec<Expr>>) -> Result<Query> {
    let span = call.span;
    let var = |name: &str| Variable {
        name: name.to_string(),
        span,
    };

    for y in &call.yields {
        if !columns.contains(&y.column.as_str()) {
            return Err(ApiError::cypher(
                format!(
                    "unknown YIELD column `{}` for `{}` (columns: {})",
                    y.column,
                    call.name,
                    columns.join(", ")
                ),
                Vec::new(),
            ));
        }
    }

    let mut clauses = vec![ReadClause::InlineRows {
        vars: columns.iter().map(|c| var(c)).collect(),
        rows,
    }];

    // Names visible after YIELD (aliases applied); all columns when bare.
    let visible: Vec<String> = if call.yields.is_empty() {
        columns.iter().map(|c| (*c).to_string()).collect()
    } else {
        call.yields
            .iter()
            .map(|y| {
                y.alias
                    .as_ref()
                    .map_or_else(|| y.column.clone(), |a| a.name.clone())
            })
            .collect()
    };

    if !call.yields.is_empty() || call.where_clause.is_some() {
        let items = if call.yields.is_empty() {
            columns
                .iter()
                .map(|c| ProjectionItem {
                    expr: Expr::Var(var(c)),
                    alias: None,
                    span,
                })
                .collect()
        } else {
            call.yields
                .iter()
                .map(|y| ProjectionItem {
                    expr: Expr::Var(var(&y.column)),
                    alias: y.alias.clone(),
                    span,
                })
                .collect()
        };
        clauses.push(ReadClause::With(WithClause {
            items,
            distinct: false,
            where_clause: call.where_clause.clone(),
            order_by: Vec::new(),
            skip: None,
            limit: None,
            span,
        }));
    }

    // Read clauses following the YIELD (`… YIELD x UNWIND … RETURN …`)
    // continue the pipeline; the parser guarantees an explicit RETURN
    // accompanies them.
    clauses.extend(call.rest.iter().cloned());

    let return_clause = call.return_clause.clone().unwrap_or_else(|| ReturnClause {
        items: visible
            .iter()
            .map(|name| ProjectionItem {
                expr: Expr::Var(var(name)),
                alias: None,
                span,
            })
            .collect(),
        distinct: false,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        span,
    });

    Ok(Query {
        clauses,
        return_clause,
        union_tail: None,
        span,
    })
}
