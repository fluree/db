//! SPARQL-based constraints (`sh:sparql`, SHACL-SPARQL §5).
//!
//! A `sh:sparql` constraint attaches a `sh:SPARQLConstraint` node to a node
//! or property shape. The constraint's `sh:select` query runs once per focus
//! node with `$this` pre-bound; every solution row is a violation. Bindings
//! of `?value`, `?path`, and `?message` in a solution populate the matching
//! validation-result fields.
//!
//! # Compilation
//!
//! [`build_constraint`] assembles the final query text — the `sh:prefixes`
//! declarations (followed through `owl:imports`, per spec) become a `PREFIX`
//! header — then parses it and enforces the SHACL pre-binding restrictions
//! (Appendix B): no `MINUS`, no federated queries (`SERVICE`), no `VALUES`,
//! no reassignment of `$this`, and every sub-`SELECT` must explicitly
//! project `$this` (`SELECT *` is rejected — it does not bring pre-bound
//! variables into scope; W3C `pre-binding-006`). `$shapesGraph` /
//! `$currentShape` are optional per spec and not supported here. A query
//! that violates any of these compiles to an error that surfaces — as a
//! *failure*, not a violation — when the owning shape fires, mirroring how
//! unresolvable `sh:path` structures are scoped to the shapes that use them.
//!
//! # Execution
//!
//! Parsing happens at shape-compile time (snapshot-independent); lowering to
//! query IR happens per validation against the *data* ledger's namespace
//! registry — the staged registry when the caller provides one (so a
//! constraint over a namespace the in-flight transaction introduced matches
//! its staged data), else the data snapshot's — so cross-ledger
//! `f:shapesSource` shapes encode against the graph being validated. An IRI
//! whose namespace the data ledger has never seen lowers to a never-matching
//! Sid: constraints over absent vocabulary are silently inert, not errors.
//! Pre-binding injects a single-row `VALUES` for `$this`
//! (and `$PATH`, on property shapes with a plain predicate path) into every
//! scope (top level, union branches, optionals, GRAPH bodies, and
//! sub-selects) — the standard implementation of the spec's solution-mapping
//! injection semantics, which the pre-binding restrictions above make
//! equivalent to substitution.

use crate::compile::Severity;
use crate::error::{Result, ShaclError};
use crate::validate::{FocusNode, ValidationResult};
use fluree_db_core::{FlakeValue, GraphDbRef, Sid};
use fluree_db_query::{
    execute, Binding, ContextConfig, ExecutableQuery, ExecutionContext, Pattern, VarId, VarRegistry,
};
use fluree_db_sparql::ast::pattern::{GraphPattern, SubSelect};
use fluree_db_sparql::ast::query::{GroupCondition, SelectVariable, SelectVariables};
use fluree_db_sparql::{parse_sparql, QueryBody, SparqlAst};
use fluree_vocab::shacl as sh_vocab;
use std::sync::Arc;

/// A compiled `sh:sparql` constraint, attached to a node or property shape.
#[derive(Debug, Clone)]
pub struct SparqlConstraint {
    /// The `sh:SPARQLConstraint` node (reported as `sh:sourceConstraint`).
    pub source: Sid,
    /// Final query text: prefix header + `sh:select`.
    pub query_text: String,
    /// `sh:message` values on the constraint node. `{?var}` / `{$var}`
    /// templates are substituted from each solution's bindings.
    pub messages: Vec<String>,
    /// `sh:deactivated true` on the constraint node — skipped entirely.
    pub deactivated: bool,
    /// The query references `$PATH` — valid only on a property shape with a
    /// plain predicate path, bound at validation time like `$this`.
    /// Parse + pre-binding-restriction outcome. `Err` is raised as a
    /// validation *failure* (an engine error) when the owning shape fires.
    pub parsed: std::result::Result<Arc<SparqlAst>, String>,
}

/// Assemble and parse one `sh:sparql` constraint.
///
/// `select` is the raw `sh:select` text; `prefix_header` the PREFIX
/// declarations resolved from `sh:prefixes`.
pub(crate) fn build_constraint(
    source: Sid,
    select: &str,
    prefix_header: &str,
    messages: Vec<String>,
    deactivated: bool,
) -> SparqlConstraint {
    let query_text = if prefix_header.is_empty() {
        select.to_string()
    } else {
        format!("{prefix_header}\n{select}")
    };
    let parsed = analyze_select(&query_text);

    SparqlConstraint {
        source,
        query_text,
        messages,
        deactivated,
        parsed,
    }
}

/// A constraint whose structure is already known to be invalid (e.g. missing
/// `sh:select`) — carries the error to raise when the owning shape fires.
pub(crate) fn invalid_constraint(
    source: Sid,
    messages: Vec<String>,
    deactivated: bool,
    error: String,
) -> SparqlConstraint {
    SparqlConstraint {
        source,
        query_text: String::new(),
        messages,
        deactivated,
        parsed: Err(error),
    }
}

/// Parse `sh:select` text and enforce the SHACL pre-binding restrictions.
fn analyze_select(text: &str) -> std::result::Result<Arc<SparqlAst>, String> {
    // $shapesGraph / $currentShape support is optional (SHACL §5.3.2); an
    // implementation without it MUST treat queries using them as invalid.
    // A text scan is sufficient: variables cannot appear inside comments
    // stripped by the lexer, and a false positive inside a string literal
    // only rejects a query no sane constraint would write.
    for reserved in [
        "$shapesGraph",
        "?shapesGraph",
        "$currentShape",
        "?currentShape",
    ] {
        if text.contains(reserved) {
            return Err(format!(
                "pre-bound variable {reserved} is not supported in sh:sparql constraints"
            ));
        }
    }

    let out = parse_sparql(text);
    if out.has_errors() {
        let msg = out
            .errors()
            .map(|d| d.message.clone())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!("invalid sh:select query: {msg}"));
    }
    let ast = out
        .ast
        .ok_or_else(|| "invalid sh:select query".to_string())?;

    let select = match &ast.body {
        QueryBody::Select(s) => s,
        _ => return Err("sh:select must be a SELECT query".to_string()),
    };

    check_select_clause(&select.select.variables)?;
    if select.values.is_some() {
        return Err("VALUES is not allowed in sh:sparql constraints (pre-binding)".to_string());
    }
    if let Some(group_by) = &select.modifiers.group_by {
        for cond in &group_by.conditions {
            if let GroupCondition::Expr { alias: Some(v), .. } = cond {
                if v.name.as_ref() == "this" {
                    return Err("cannot reassign pre-bound variable $this".to_string());
                }
            }
        }
    }
    check_pattern(&select.where_clause.pattern)?;
    Ok(Arc::new(ast))
}

/// Reject assignment to `$this` in a SELECT clause (`... AS $this`).
fn check_select_clause(vars: &SelectVariables) -> std::result::Result<(), String> {
    if let SelectVariables::Explicit(list) = vars {
        for v in list {
            if let SelectVariable::Expr { alias, .. } = v {
                if alias.name.as_ref() == "this" {
                    return Err("cannot reassign pre-bound variable $this".to_string());
                }
            }
        }
    }
    Ok(())
}

/// Recursive pre-binding restriction walk over a graph pattern.
fn check_pattern(pattern: &GraphPattern) -> std::result::Result<(), String> {
    match pattern {
        GraphPattern::Minus { .. } => {
            Err("MINUS is not allowed in sh:sparql constraints (pre-binding)".to_string())
        }
        GraphPattern::Service { .. } => {
            Err("federated queries (SERVICE) are not allowed in sh:sparql constraints".to_string())
        }
        GraphPattern::Values { .. } => {
            Err("VALUES is not allowed in sh:sparql constraints (pre-binding)".to_string())
        }
        GraphPattern::Bind { var, .. } => {
            if var.name.as_ref() == "this" {
                Err("cannot reassign pre-bound variable $this".to_string())
            } else {
                Ok(())
            }
        }
        GraphPattern::Group { patterns, .. } => patterns.iter().try_for_each(check_pattern),
        GraphPattern::Optional { pattern, .. } | GraphPattern::Graph { pattern, .. } => {
            check_pattern(pattern)
        }
        GraphPattern::Union { left, right, .. } => {
            check_pattern(left)?;
            check_pattern(right)
        }
        GraphPattern::SubSelect { query, .. } => check_subselect(query),
        GraphPattern::Bgp { .. }
        | GraphPattern::Filter { .. }
        | GraphPattern::Path { .. }
        | GraphPattern::AnnotationTarget { .. } => Ok(()),
    }
}

/// A sub-SELECT must explicitly project `$this` (SHACL Appendix B: subqueries
/// must return all potentially pre-bound variables; `SELECT *` does not bring
/// pre-bound variables into scope, so it is rejected — W3C `pre-binding-006`)
/// and obeys the same restrictions as the outer query.
fn check_subselect(sub: &SubSelect) -> std::result::Result<(), String> {
    match &sub.variables {
        SelectVariables::Star => {
            return Err(
                "SELECT * sub-queries are not allowed in sh:sparql constraints (pre-binding)"
                    .to_string(),
            );
        }
        SelectVariables::Explicit(list) => {
            check_select_clause(&sub.variables)?;
            let projects_this = list.iter().any(|v| match v {
                SelectVariable::Var(v) => v.name.as_ref() == "this",
                SelectVariable::Expr { .. } => false,
            });
            if !projects_this {
                return Err(
                    "sub-SELECT in a sh:sparql constraint must project $this (pre-binding)"
                        .to_string(),
                );
            }
        }
    }
    if sub.values.is_some() {
        return Err("VALUES is not allowed in sh:sparql constraints (pre-binding)".to_string());
    }
    check_pattern(&sub.pattern)
}

/// Inject a single-row `VALUES` binding the pre-bound variables (`$this`,
/// and `$PATH` on property shapes) into every evaluation scope of the lowered
/// query, implementing SHACL pre-binding semantics (solution-mapping
/// injection: the pre-bound variables are bound in *every* scope, including
/// otherwise-empty groups whose only content is a FILTER).
fn inject_bindings(patterns: &mut Vec<Pattern>, bound: &[(VarId, Sid)]) {
    for p in patterns.iter_mut() {
        match p {
            Pattern::Union(branches) => {
                for branch in branches.iter_mut() {
                    inject_bindings(branch, bound);
                }
            }
            Pattern::Optional(inner) => inject_bindings(inner, bound),
            Pattern::Graph { patterns, .. } => inject_bindings(patterns, bound),
            Pattern::Subquery(sq) => {
                inject_bindings(&mut sq.patterns, bound);
                // The restriction walk guarantees the sub-SELECT projects
                // $this explicitly; $PATH may not be projected, so extend
                // the projection to keep the injected binding joinable.
                for (var, _) in bound {
                    if !sq.select.contains(var) {
                        sq.select.push(*var);
                    }
                }
            }
            _ => {}
        }
    }
    patterns.insert(
        0,
        Pattern::Values {
            vars: bound.iter().map(|(v, _)| *v).collect(),
            rows: vec![bound.iter().map(|(_, s)| Binding::sid(s.clone())).collect()],
        },
    );
}

/// Variable names referenced by `{?var}` / `{$var}` templates in `messages`.
fn template_var_names(messages: &[String]) -> Vec<String> {
    let mut names = Vec::new();
    for msg in messages {
        let bytes = msg.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'{'
                && i + 1 < bytes.len()
                && (bytes[i + 1] == b'?' || bytes[i + 1] == b'$')
            {
                if let Some(end) = msg[i..].find('}') {
                    let name = &msg[i + 2..i + end];
                    if !name.is_empty() && !names.iter().any(|n| n == name) {
                        names.push(name.to_string());
                    }
                    i += end + 1;
                    continue;
                }
            }
            i += 1;
        }
    }
    names
}

/// Render a materialized binding value for message templates.
fn render_value(value: &FlakeValue, ctx: &ExecutionContext<'_>) -> String {
    match value {
        FlakeValue::Ref(sid) => ctx.decode_sid(sid).unwrap_or_else(|| format!("{sid:?}")),
        FlakeValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A solution binding materialized for report fields.
struct MaterializedValue {
    value: FlakeValue,
    datatype: Option<Sid>,
    lang: Option<String>,
}

/// Materialize one binding to a report-ready value. Encoded (late-
/// materialization) bindings resolve through the execution context;
/// unresolvable bindings drop to `None` rather than failing the report.
fn materialize_binding(
    binding: &Binding,
    ctx: &ExecutionContext<'_>,
    db: GraphDbRef<'_>,
) -> Option<MaterializedValue> {
    match binding {
        Binding::Sid { sid, .. } => Some(MaterializedValue {
            value: FlakeValue::Ref(sid.clone()),
            datatype: None,
            lang: None,
        }),
        Binding::Lit { val, dtc, .. } => Some(MaterializedValue {
            value: val.clone(),
            datatype: Some(dtc.datatype().clone()),
            lang: match dtc {
                fluree_db_core::DatatypeConstraint::LangTag(tag) => Some(tag.to_string()),
                fluree_db_core::DatatypeConstraint::Explicit(_) => None,
            },
        }),
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } => {
            let value = ctx
                .decode_encoded_value(*o_kind, *o_key, *p_id, *dt_id, *lang_id)?
                .ok()?;
            let lang = if *lang_id != 0 {
                ctx.lang_tag_for_id(*lang_id).map(|t| t.to_string())
            } else {
                None
            };
            Some(MaterializedValue {
                value,
                datatype: None,
                lang,
            })
        }
        Binding::EncodedSid { s_id, .. } => {
            let iri = ctx.resolve_subject_iri(*s_id)?.ok()?;
            let sid = db.snapshot.encode_iri(&iri)?;
            Some(MaterializedValue {
                value: FlakeValue::Ref(sid),
                datatype: None,
                lang: None,
            })
        }
        Binding::IriMatch { iri, .. } | Binding::Iri(iri) => {
            let sid = db.snapshot.encode_iri(iri)?;
            Some(MaterializedValue {
                value: FlakeValue::Ref(sid),
                datatype: None,
                lang: None,
            })
        }
        _ => None,
    }
}

/// Evaluate one `sh:sparql` constraint against one focus node.
///
/// Every solution row of the constraint's SELECT (with `$this` pre-bound to
/// `focus`) becomes one violation. `fallback_path` is the owning property
/// shape's predicate path, reported when the solution does not bind `?path`.
///
/// `iri_encoder`, when provided, overrides the lowering term resolver —
/// staging passes the staged namespace registry (snapshot + this
/// transaction's allocations) so a constraint over a namespace the in-flight
/// transaction introduced matches its staged data. Either way, an IRI whose
/// namespace the ledger has never seen lowers to a never-matching Sid: a
/// constraint over vocabulary with no data is silently inert, never an error.
/// The two per-request inputs a constraint query needs beyond the shape
/// itself: how IRIs lower, and what bounds the query.
#[derive(Clone, Copy, Default)]
pub(crate) struct SparqlConstraintCtx<'a> {
    /// Lowering-time IRI resolver override. Staging passes the staged
    /// namespace registry so a constraint over a namespace the in-flight
    /// transaction introduced matches its staged data; `None` falls back to
    /// the data snapshot's registry.
    pub iri_encoder: Option<&'a (dyn fluree_db_query::parse::IriEncoder + Sync)>,
    /// Request deadline and per-query memory ceiling. `None` leaves the
    /// constraint body unbounded — see [`crate::ShaclEngine::with_cancellation`].
    pub cancellation: Option<&'a fluree_db_core::QueryCancellation>,
}

pub(crate) async fn validate_sparql_constraint(
    db: GraphDbRef<'_>,
    focus: &Sid,
    constraint: &SparqlConstraint,
    fallback_path: Option<&Sid>,
    severity: Severity,
    source_shape: &Sid,
    exec: SparqlConstraintCtx<'_>,
) -> Result<Vec<ValidationResult>> {
    if constraint.deactivated {
        return Ok(Vec::new());
    }

    let ast = constraint
        .parsed
        .as_ref()
        .map_err(|e| ShaclError::SparqlConstraint {
            constraint: constraint.source.clone(),
            message: e.clone(),
        })?;

    // Lower against the staged registry when one is provided, else the data
    // snapshot — a fresh `VarRegistry` per call keeps compiled shapes
    // snapshot-independent (the shapes cache can outlive namespace
    // allocations on the data ledger).
    let mut vars = VarRegistry::new();
    let lowered = match exec.iri_encoder {
        Some(encoder) => fluree_db_sparql::lower_sparql(ast, &encoder, &mut vars),
        None => fluree_db_sparql::lower_sparql(ast, db.snapshot, &mut vars),
    };
    let mut query = lowered.map_err(|e| ShaclError::SparqlConstraint {
        constraint: constraint.source.clone(),
        message: format!("failed to lower sh:select query: {e}"),
    })?;

    // Lowered variables register under their `?`-prefixed surface name.
    let this_var = vars.get_or_insert("?this");
    let value_var = vars.get_or_insert("?value");
    let path_var = vars.get_or_insert("?path");
    let message_var = vars.get_or_insert("?message");
    let template_vars: Vec<(String, VarId)> = template_var_names(&constraint.messages)
        .into_iter()
        .map(|name| {
            let id = vars.get_or_insert(&format!("?{name}"));
            (name, id)
        })
        .collect();

    let mut bound = vec![(this_var, focus.clone())];
    // Whether the constraint uses `$PATH` is read off the LOWERED variable
    // set, which is exact. Scanning the query text instead would fire on a
    // `$PATH` inside a string literal and turn a valid node-shape constraint
    // into a hard validation failure.
    if vars.get("?PATH").is_some() {
        // $PATH is only meaningful on a property shape with a plain
        // predicate path; bind it exactly like $this.
        let Some(path) = fallback_path else {
            return Err(ShaclError::SparqlConstraint {
                constraint: constraint.source.clone(),
                message: "$PATH is only supported in sh:sparql constraints on property shapes \
                          with a plain predicate path"
                    .to_string(),
            });
        };
        bound.push((vars.get_or_insert("?PATH"), path.clone()));
    }
    inject_bindings(&mut query.patterns, &bound);

    let batches = execute(
        db,
        &vars,
        &ExecutableQuery::simple(query),
        ContextConfig {
            // Charge the constraint query against the validation's fuel
            // budget when the caller tracks one.
            tracker: db.tracker,
            // The deadline AND the per-query memory ceiling: the ceiling is
            // installed only when a cancellation is present, so a constraint
            // body without one walks as far as it likes. Pre-binding `$this`
            // bounds where the query starts, not how much it reads.
            cancellation: exec.cancellation.cloned(),
            ..Default::default()
        },
    )
    .await?;

    // Decode context for late-materialized (encoded) bindings.
    let ctx = ExecutionContext::from_graph_db_ref(db, &vars);

    let mut results = Vec::new();
    for batch in &batches {
        for row in 0..batch.len() {
            // Per SHACL §5.3.1, sh:value is the ?value binding when the
            // solution has one, otherwise the focus node.
            let value = batch
                .get(row, value_var)
                .and_then(|b| materialize_binding(b, &ctx, db))
                .unwrap_or_else(|| MaterializedValue {
                    value: FlakeValue::Ref(focus.clone()),
                    datatype: None,
                    lang: None,
                });
            let result_path = batch
                .get(row, path_var)
                .and_then(|b| materialize_binding(b, &ctx, db))
                .and_then(|m| match m.value {
                    FlakeValue::Ref(sid) => Some(sid),
                    _ => None,
                })
                .or_else(|| fallback_path.cloned());

            let bound_message = batch.get(row, message_var).and_then(|b| match b {
                Binding::Lit {
                    val: FlakeValue::String(s),
                    ..
                } => Some(s.clone()),
                _ => None,
            });
            let message = bound_message
                .or_else(|| {
                    constraint.messages.first().map(|template| {
                        let mut msg = template.clone();
                        for (name, id) in &template_vars {
                            let rendered = if name == "this" {
                                Some(render_value(&FlakeValue::Ref(focus.clone()), &ctx))
                            } else {
                                batch
                                    .get(row, *id)
                                    .and_then(|b| materialize_binding(b, &ctx, db))
                                    .map(|m| render_value(&m.value, &ctx))
                            };
                            if let Some(rendered) = rendered {
                                msg = msg
                                    .replace(&format!("{{?{name}}}"), &rendered)
                                    .replace(&format!("{{${name}}}"), &rendered);
                            }
                        }
                        msg
                    })
                })
                .unwrap_or_else(|| "SPARQL constraint violation".to_string());

            results.push(ValidationResult {
                focus_node: FocusNode::Node(focus.clone()),
                result_path,
                source_shape: source_shape.clone(),
                source_constraint: Some(constraint.source.clone()),
                constraint_component: sh_vocab::SPARQL_CONSTRAINT_COMPONENT,
                severity,
                message,
                value: Some(value.value.clone()),
                value_datatype: value.datatype.clone(),
                value_lang: value.lang,
                graph_id: None,
            });
        }
    }
    Ok(results)
}
