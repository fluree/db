//! Rule parsing: JSON-LD `{where, insert}` rules and SPARQL `CONSTRUCT … WHERE`
//! rules, from stored `f:rule` values or the query-time `rules` array.
//!
//! The body goes through the same parser and lowering as a query's `WHERE`
//! clause; the heads are parsed here. See docs/design/rules-engine.md.

use super::{rule_iri, validate, DatalogRule, HeadTerm, RuleBody, RuleHead};
use crate::error::{QueryError, Result};
use crate::ir::reasoning::ReasoningConfig;
use crate::ir::{Pattern, Query, QueryOutput, Ref, Term, TriplePattern};
use crate::lang_support::SparqlRuleParts;
use crate::parse::ast::UnresolvedQuery;
use crate::parse::policy::resolve_parse_policy;
use crate::parse::{
    extract_path_aliases, lower_query, normalize_context_value, parse_where_with_counters,
    IriEncoder, JsonLdParseCtx, SelectMode,
};
use crate::var_registry::VarRegistry;
use fluree_db_core::edge::id_datatype_sid;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{DatatypeConstraint, LedgerSnapshot, Sid};
use fluree_graph_json_ld::{parse_context, TypeValue};
use fluree_vocab::namespaces::{RDF, XSD};
use fluree_vocab::{rdf_names, xsd_names};
use serde_json::Value as JsonValue;
use std::sync::Arc;

fn invalid(rule: &str, what: impl std::fmt::Display) -> QueryError {
    QueryError::InvalidQuery(format!("datalog rule {rule}: {what}"))
}

/// Recognize a SPARQL typed-value object
/// `{"@type": "f:sparql", "@value": "CONSTRUCT ..."}` (compact or full
/// `https://ns.flur.ee/db#sparql` type IRI). Returns the SPARQL source.
fn as_sparql_typed_value(json: &JsonValue) -> Option<&str> {
    let obj = json.as_object()?;
    let type_str = obj.get("@type")?.as_str()?;
    if type_str != "f:sparql" && type_str != fluree_vocab::fluree::SPARQL {
        return None;
    }
    obj.get("@value")?.as_str()
}

/// Parse one entry of a query's `rules` array.
///
/// Accepted shapes:
/// 1. A rule body: `{"@context": …, "where": …, "insert": …}`
/// 2. A stored-rule document: `{"@id": "…", "f:rule": {"@value": {…}}}`
///    (the `f:rule` value may also be the SPARQL typed value below)
/// 3. A SPARQL typed value: `{"@type": "f:sparql", "@value": "CONSTRUCT …"}`
pub fn parse_query_time_rule(
    json: &JsonValue,
    snapshot: &LedgerSnapshot,
    index: usize,
) -> Result<DatalogRule> {
    let synthetic_id = || Sid::new(0, format!("_:query_rule_{index}"));

    if let Some(f_rule) = json
        .get("f:rule")
        .or_else(|| json.get(fluree_vocab::fluree::RULE))
    {
        let rule_id = match json.get("@id").and_then(|v| v.as_str()) {
            Some(id_str) => Sid::new(0, id_str),
            None => synthetic_id(),
        };
        if let Some(source) = as_sparql_typed_value(f_rule) {
            return parse_sparql_rule(&rule_id, source, snapshot);
        }
        let rule_value = f_rule.get("@value").unwrap_or(f_rule);
        return parse_jsonld_rule(&rule_id, rule_value, snapshot, &rule_value.to_string());
    }

    let rule_id = synthetic_id();
    if let Some(source) = as_sparql_typed_value(json) {
        return parse_sparql_rule(&rule_id, source, snapshot);
    }
    parse_jsonld_rule(&rule_id, json, snapshot, &json.to_string())
}

/// Parse a JSON-LD rule `{ "@context", "where", "insert" }`.
pub(super) fn parse_jsonld_rule(
    rule_id: &Sid,
    json: &JsonValue,
    snapshot: &LedgerSnapshot,
    source: &str,
) -> Result<DatalogRule> {
    let label = rule_iri(snapshot, rule_id);
    let obj = json.as_object().ok_or_else(|| {
        invalid(
            &label,
            "a rule must be a JSON object with `where` and `insert` clauses",
        )
    })?;

    let context_val = obj
        .get("@context")
        .or_else(|| obj.get("context"))
        .cloned()
        .unwrap_or(JsonValue::Null);
    let context = parse_context(&normalize_context_value(&context_val))
        .map_err(|e| invalid(&label, format!("invalid @context: {e}")))?;
    let policy = resolve_parse_policy(None, obj);
    let path_aliases = extract_path_aliases(&context_val, &context, policy)
        .map_err(|e| invalid(&label, format!("invalid @context: {e}")))?;
    let ctx = JsonLdParseCtx::new(context.clone(), path_aliases, policy);

    let where_val = obj
        .get("where")
        .ok_or_else(|| invalid(&label, "missing `where` clause"))?;
    let insert_val = obj
        .get("insert")
        .ok_or_else(|| invalid(&label, "missing `insert` clause"))?;

    // The body is an ordinary WHERE clause.
    validate::reject_unknown_keyword_keys(where_val, &label)?;
    let mut unresolved = UnresolvedQuery::new(context);
    let (mut subject_counter, mut nested_counter) = (0u32, 0u32);
    parse_where_with_counters(
        where_val,
        &ctx,
        &mut unresolved,
        &mut subject_counter,
        &mut nested_counter,
        true,
    )
    .map_err(|e| invalid(&label, format!("invalid where clause: {e}")))?;
    validate::lint_unresolved_filters(&unresolved.patterns, &label)?;

    let mut vars = VarRegistry::new();
    let query = lower_query(unresolved, snapshot, &mut vars, SelectMode::Many)
        .map_err(|e| invalid(&label, format!("invalid where clause: {e}")))?;

    let heads = parse_heads(insert_val, &ctx, snapshot, &vars, &label)?;
    build_rule(rule_id.clone(), label, query, vars, heads, source, snapshot)
}

/// Parse a SPARQL `CONSTRUCT … WHERE …` rule. The CONSTRUCT template is the
/// head, the WHERE clause is the body.
pub(super) fn parse_sparql_rule(
    rule_id: &Sid,
    source: &str,
    snapshot: &LedgerSnapshot,
) -> Result<DatalogRule> {
    let label = rule_iri(snapshot, rule_id);
    let support = crate::lang_support::sparql_support().ok_or_else(|| {
        QueryError::Internal(
            "SPARQL rule support is not registered in this process; \
             cannot parse f:sparql datalog rule"
                .to_string(),
        )
    })?;
    let SparqlRuleParts { mut query, vars } =
        (support.lower_rule)(source, snapshot).map_err(|e| invalid(&label, e))?;

    let template = match std::mem::replace(&mut query.output, QueryOutput::Ask) {
        QueryOutput::Construct(template) => template,
        _ => {
            return Err(invalid(
                &label,
                "SPARQL rules must be CONSTRUCT ... WHERE ... queries; the CONSTRUCT \
                 template is the rule head (insert) and the WHERE clause is the rule body",
            ))
        }
    };
    if !template.bnode_vars.is_empty() {
        return Err(invalid(
            &label,
            "the CONSTRUCT template contains blank nodes; a rule head cannot mint fresh \
             nodes (every round would mint new ones and the fixpoint would never end)",
        ));
    }
    if template.patterns.is_empty() {
        return Err(invalid(
            &label,
            "the CONSTRUCT template has no triple patterns",
        ));
    }
    let heads = template
        .patterns
        .iter()
        .map(|tp| head_from_triple(tp, snapshot, &label))
        .collect::<Result<Vec<_>>>()?;

    build_rule(rule_id.clone(), label, query, vars, heads, source, snapshot)
}

/// Validate the parsed pieces and assemble the rule.
fn build_rule(
    id: Sid,
    label: String,
    mut query: Query,
    vars: VarRegistry,
    heads: Vec<RuleHead>,
    source: &str,
    snapshot: &LedgerSnapshot,
) -> Result<DatalogRule> {
    if heads.is_empty() {
        return Err(invalid(&label, "the insert clause derives no triples"));
    }
    if query.patterns.is_empty() && query.post_values.is_none() {
        return Err(invalid(&label, "the where clause has no patterns"));
    }
    validate::validate_body(&query, snapshot, &label)?;
    let body_vars = validate::body_vars(&query);
    validate::check_range_restriction(&heads, &body_vars, &vars, &label)?;
    validate::warn_iri_vs_literal(&query, &vars, &label);

    // The body projects every variable it binds; a rule has no modifiers of
    // its own and never triggers nested reasoning.
    query.output = QueryOutput::select_all(body_vars);
    query.reasoning = ReasoningConfig::default();
    query.limit = None;
    query.offset = None;
    query.ordering.clear();
    query.order_binds.clear();
    query.grouping = None;

    let mut depends_on = Vec::new();
    collect_predicates(&query.patterns, &mut depends_on);
    let generates = heads
        .iter()
        .filter_map(|h| match &h.predicate {
            HeadTerm::Sid(sid) => Some(sid.clone()),
            _ => None,
        })
        .collect();

    Ok(DatalogRule {
        id,
        name: label,
        body: RuleBody { query, vars },
        heads,
        depends_on,
        generates,
        source: Arc::from(source),
    })
}

/// Constant predicates a body reads, for the execution-order heuristic.
fn collect_predicates(patterns: &[Pattern], out: &mut Vec<Sid>) {
    for pattern in patterns {
        match pattern {
            Pattern::Triple(tp) => {
                if let Ref::Sid(p) = &tp.p {
                    if !out.contains(p) {
                        out.push(p.clone());
                    }
                }
            }
            Pattern::EdgeAnnotation { edge, body, .. }
            | Pattern::AnnotationTarget { edge, body, .. } => {
                if let Ref::Sid(p) = &edge.p {
                    if !out.contains(p) {
                        out.push(p.clone());
                    }
                }
                collect_predicates(body, out);
            }
            Pattern::Union(branches) => {
                for branch in branches {
                    collect_predicates(branch, out);
                }
            }
            Pattern::Graph { patterns, .. }
            | Pattern::DefaultGraphSource { patterns }
            | Pattern::Exists(patterns) => collect_predicates(patterns, out),
            Pattern::Subquery(sub) => collect_predicates(&sub.patterns, out),
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// JSON-LD heads
// ---------------------------------------------------------------------------

fn parse_heads(
    insert: &JsonValue,
    ctx: &JsonLdParseCtx,
    snapshot: &LedgerSnapshot,
    vars: &VarRegistry,
    label: &str,
) -> Result<Vec<RuleHead>> {
    let mut heads = Vec::new();
    match insert {
        JsonValue::Object(map) => parse_head_node(map, ctx, snapshot, vars, label, &mut heads)?,
        JsonValue::Array(items) => {
            for item in items {
                match item {
                    JsonValue::Object(map) => {
                        parse_head_node(map, ctx, snapshot, vars, label, &mut heads)?;
                    }
                    other => {
                        return Err(invalid(
                            label,
                            format!("insert clause entries must be objects, got {other}"),
                        ))
                    }
                }
            }
        }
        other => {
            return Err(invalid(
                label,
                format!("insert clause must be an object or an array of objects, got {other}"),
            ))
        }
    }
    Ok(heads)
}

fn parse_head_node(
    map: &serde_json::Map<String, JsonValue>,
    ctx: &JsonLdParseCtx,
    snapshot: &LedgerSnapshot,
    vars: &VarRegistry,
    label: &str,
    heads: &mut Vec<RuleHead>,
) -> Result<()> {
    if let Some(JsonValue::Array(nodes)) = map.get("@graph") {
        for node in nodes {
            match node {
                JsonValue::Object(inner) => {
                    parse_head_node(inner, ctx, snapshot, vars, label, heads)?;
                }
                other => {
                    return Err(invalid(
                        label,
                        format!("insert @graph entries must be objects, got {other}"),
                    ))
                }
            }
        }
        return Ok(());
    }

    let id_val = map.get("@id").ok_or_else(|| {
        invalid(
            label,
            "every node in an insert pattern needs an @id (an anonymous node has no \
             subject to derive facts about)",
        )
    })?;
    let subject = head_ref_term(id_val, ctx, snapshot, vars, label, false)?;

    for (key, value) in map {
        match key.as_str() {
            "@id" | "@context" | "@graph" => continue,
            "@annotation" | "@edge" | "@reifies" => {
                return Err(invalid(
                    label,
                    format!(
                        "insert patterns cannot use `{key}` yet: a rule head derives plain \
                         triples only, so it cannot mint an edge annotation (reifier minting \
                         in rule heads is tracked as a follow-up in docs/design/rules-engine.md)"
                    ),
                ));
            }
            "@type" => {
                let predicate = HeadTerm::Sid(Sid::new(RDF, rdf_names::TYPE));
                for item in json_values(value) {
                    let object = head_ref_term(item, ctx, snapshot, vars, label, true)?;
                    heads.push(RuleHead {
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object,
                    });
                }
            }
            k if k.starts_with('@') => {
                return Err(invalid(
                    label,
                    format!("unsupported keyword `{k}` in an insert pattern"),
                ));
            }
            k if k.starts_with('?') => {
                let predicate = HeadTerm::Var(head_var(k, vars, label)?);
                for item in json_values(value) {
                    let object = head_object_term(item, ctx, snapshot, vars, label, false, heads)?;
                    heads.push(RuleHead {
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object,
                    });
                }
            }
            k => {
                let (iri, entry) = ctx
                    .expand_vocab(k)
                    .map_err(|e| invalid(label, format!("invalid predicate `{k}`: {e}")))?;
                let coerce_id = entry
                    .as_ref()
                    .and_then(|e| e.type_.as_ref())
                    .is_some_and(|t| matches!(t, TypeValue::Id));
                let predicate = HeadTerm::Sid(encode_iri(&iri, snapshot, label)?);
                for item in json_values(value) {
                    let object =
                        head_object_term(item, ctx, snapshot, vars, label, coerce_id, heads)?;
                    heads.push(RuleHead {
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object,
                    });
                }
            }
        }
    }
    Ok(())
}

fn json_values(value: &JsonValue) -> Vec<&JsonValue> {
    match value {
        JsonValue::Array(items) => items.iter().collect(),
        other => vec![other],
    }
}

fn head_var(name: &str, vars: &VarRegistry, label: &str) -> Result<crate::var_registry::VarId> {
    vars.get(name).ok_or_else(|| {
        invalid(
            label,
            format!(
                "the insert pattern uses `{name}` which the where clause never binds; every \
                 variable used in `insert` must also appear in `where` (a where/insert typo \
                 is the usual cause)"
            ),
        )
    })
}

fn encode_iri(iri: &str, snapshot: &LedgerSnapshot, label: &str) -> Result<Sid> {
    IriEncoder::encode_iri(snapshot, iri)
        .ok_or_else(|| invalid(label, format!("cannot encode IRI <{iri}>")))
}

/// A subject / `@type` / reference position: a variable or an IRI.
fn head_ref_term(
    value: &JsonValue,
    ctx: &JsonLdParseCtx,
    snapshot: &LedgerSnapshot,
    vars: &VarRegistry,
    label: &str,
    vocab: bool,
) -> Result<HeadTerm> {
    match value {
        JsonValue::String(s) if s.starts_with('?') => Ok(HeadTerm::Var(head_var(s, vars, label)?)),
        JsonValue::String(s) => {
            let iri = if vocab {
                ctx.expand_vocab(s).map(|(iri, _)| iri)
            } else {
                ctx.expand_id(s).map(|(iri, _)| iri)
            }
            .map_err(|e| invalid(label, format!("invalid IRI `{s}`: {e}")))?;
            Ok(HeadTerm::Sid(encode_iri(&iri, snapshot, label)?))
        }
        JsonValue::Object(map) => match map.get("@id") {
            Some(id) => head_ref_term(id, ctx, snapshot, vars, label, false),
            None => Err(invalid(
                label,
                format!("expected a variable or an IRI reference, got {value}"),
            )),
        },
        other => Err(invalid(
            label,
            format!("expected a variable or an IRI, got {other}"),
        )),
    }
}

/// An object position: variable, reference, or literal (scalar or
/// `@value` object).
fn head_object_term(
    value: &JsonValue,
    ctx: &JsonLdParseCtx,
    snapshot: &LedgerSnapshot,
    vars: &VarRegistry,
    label: &str,
    coerce_id: bool,
    heads: &mut Vec<RuleHead>,
) -> Result<HeadTerm> {
    match value {
        JsonValue::String(s) if s.starts_with('?') => Ok(HeadTerm::Var(head_var(s, vars, label)?)),
        JsonValue::String(s) if coerce_id => {
            head_ref_term(value, ctx, snapshot, vars, label, false)
                .map_err(|_| invalid(label, format!("invalid IRI `{s}`")))
        }
        JsonValue::String(s) => Ok(literal(
            FlakeValue::String(s.clone()),
            Sid::new(XSD, xsd_names::STRING),
            None,
        )),
        JsonValue::Bool(b) => Ok(literal(
            FlakeValue::Boolean(*b),
            Sid::new(XSD, xsd_names::BOOLEAN),
            None,
        )),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(literal(
                    FlakeValue::Long(i),
                    Sid::new(XSD, xsd_names::INTEGER),
                    None,
                ))
            } else if let Some(f) = n.as_f64() {
                Ok(literal(
                    FlakeValue::Double(f),
                    Sid::new(XSD, xsd_names::DOUBLE),
                    None,
                ))
            } else {
                Err(invalid(label, format!("unsupported number {n}")))
            }
        }
        JsonValue::Object(map) => {
            if let Some(inner) = map.get("@value") {
                return head_value_object(inner, map, ctx, snapshot, label);
            }
            if map.contains_key("@id") {
                // A referenced node may carry its own properties: derive
                // those too (`{"@id": "?b", "ex:flag": true}`).
                if map.keys().any(|k| k != "@id") {
                    parse_head_node(map, ctx, snapshot, vars, label, heads)?;
                }
                return head_ref_term(value, ctx, snapshot, vars, label, false);
            }
            if map.contains_key("@annotation") || map.contains_key("@edge") {
                return Err(invalid(
                    label,
                    "insert patterns cannot use `@annotation` yet: a rule head derives plain \
                     triples only, so it cannot mint an edge annotation (reifier minting in \
                     rule heads is tracked as a follow-up in docs/design/rules-engine.md)",
                ));
            }
            Err(invalid(
                label,
                "every node in an insert pattern needs an @id (an anonymous node has no \
                 subject to derive facts about)",
            ))
        }
        other => Err(invalid(
            label,
            format!("unsupported value {other} in an insert pattern"),
        )),
    }
}

fn head_value_object(
    inner: &JsonValue,
    map: &serde_json::Map<String, JsonValue>,
    ctx: &JsonLdParseCtx,
    snapshot: &LedgerSnapshot,
    label: &str,
) -> Result<HeadTerm> {
    if let Some(lang) = map.get("@language").and_then(|l| l.as_str()) {
        let JsonValue::String(s) = inner else {
            return Err(invalid(label, "a language-tagged @value must be a string"));
        };
        let tag: Arc<str> = Arc::from(fluree_db_core::normalize_lang_tag(lang).as_ref());
        let datatype = DatatypeConstraint::LangTag(tag.clone()).datatype().clone();
        return Ok(literal(FlakeValue::String(s.clone()), datatype, Some(tag)));
    }
    let explicit_dt = match map.get("@type").and_then(|t| t.as_str()) {
        Some("@id") => {
            let JsonValue::String(s) = inner else {
                return Err(invalid(label, "an @id-typed @value must be a string"));
            };
            let (iri, _) = ctx
                .expand_id(s)
                .map_err(|e| invalid(label, format!("invalid IRI `{s}`: {e}")))?;
            return Ok(HeadTerm::Sid(encode_iri(&iri, snapshot, label)?));
        }
        Some(t) => {
            let (iri, _) = ctx
                .expand_vocab(t)
                .map_err(|e| invalid(label, format!("invalid datatype `{t}`: {e}")))?;
            Some(encode_iri(&iri, snapshot, label)?)
        }
        None => None,
    };
    let (value, default_dt) = match inner {
        JsonValue::String(s) => (
            FlakeValue::String(s.clone()),
            Sid::new(XSD, xsd_names::STRING),
        ),
        JsonValue::Bool(b) => (FlakeValue::Boolean(*b), Sid::new(XSD, xsd_names::BOOLEAN)),
        JsonValue::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => (FlakeValue::Long(i), Sid::new(XSD, xsd_names::INTEGER)),
            (None, Some(f)) => (FlakeValue::Double(f), Sid::new(XSD, xsd_names::DOUBLE)),
            _ => return Err(invalid(label, format!("unsupported number {n}"))),
        },
        other => {
            return Err(invalid(
                label,
                format!("unsupported @value {other} in an insert pattern"),
            ))
        }
    };
    Ok(literal(value, explicit_dt.unwrap_or(default_dt), None))
}

fn literal(value: FlakeValue, datatype: Sid, lang: Option<Arc<str>>) -> HeadTerm {
    HeadTerm::Literal {
        value,
        datatype,
        lang,
    }
}

// ---------------------------------------------------------------------------
// SPARQL heads
// ---------------------------------------------------------------------------

fn head_from_triple(
    tp: &TriplePattern,
    snapshot: &LedgerSnapshot,
    label: &str,
) -> Result<RuleHead> {
    let subject = head_from_ref(&tp.s, snapshot, label)?;
    let predicate = head_from_ref(&tp.p, snapshot, label)?;
    let object = match &tp.o {
        Term::Var(v) => HeadTerm::Var(*v),
        Term::Sid(sid) => HeadTerm::Sid(sid.clone()),
        Term::Iri(iri) => HeadTerm::Sid(encode_iri(iri, snapshot, label)?),
        Term::Value(value) => {
            let datatype = tp
                .dtc
                .as_ref()
                .map(|d| d.datatype().clone())
                .unwrap_or_else(|| default_datatype(value));
            let lang = tp.dtc.as_ref().and_then(|d| d.lang_tag().map(Arc::from));
            literal(value.clone(), datatype, lang)
        }
    };
    Ok(RuleHead {
        subject,
        predicate,
        object,
    })
}

fn head_from_ref(r: &Ref, snapshot: &LedgerSnapshot, label: &str) -> Result<HeadTerm> {
    Ok(match r {
        Ref::Var(v) => HeadTerm::Var(*v),
        Ref::Sid(sid) => HeadTerm::Sid(sid.clone()),
        Ref::Iri(iri) => HeadTerm::Sid(encode_iri(iri, snapshot, label)?),
    })
}

/// Datatype for a template literal that carries no explicit constraint.
fn default_datatype(value: &FlakeValue) -> Sid {
    match value {
        FlakeValue::Ref(_) => id_datatype_sid(),
        FlakeValue::Long(_) | FlakeValue::BigInt(_) => Sid::new(XSD, xsd_names::INTEGER),
        FlakeValue::Double(_) => Sid::new(XSD, xsd_names::DOUBLE),
        FlakeValue::Decimal(_) => Sid::new(XSD, xsd_names::DECIMAL),
        FlakeValue::Boolean(_) => Sid::new(XSD, xsd_names::BOOLEAN),
        _ => Sid::new(XSD, xsd_names::STRING),
    }
}
