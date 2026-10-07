//! Query parameters.
//!
//! A parameter fixes a variable to one RDF term for the whole request:
//! `SELECT ?s WHERE { ?s ex:name $name }` with `name = "Alice"` runs as if
//! `"Alice"` were written in place of `$name` (`?name` and `$name` are the
//! same variable). Substitution rewrites the parsed AST before lowering, so
//! the planner only ever sees constants and a request without parameters
//! never reaches this module.
//!
//! The rules that keep the rewrite equivalent to writing the value inline:
//!
//! - every occurrence is replaced, subqueries and update templates included;
//! - a projected parameter stays a column, as `(value AS ?name)` (or as the
//!   `GROUP BY` key it is grouped on); `SELECT *` no longer lists it;
//! - a parameter the query assigns itself (`BIND`, `VALUES`, `AS`) is an error,
//!   as is one the query never mentions — a misspelt name would otherwise
//!   leave its variable unbound and silently widen the match.
//!
//! Values use the JSON-LD forms: a JSON string, number or boolean;
//! `{"@id": iri}`; `{"@value": v, "@type": iri}` (`"@type": "@id"` reads `v`
//! as an IRI); `{"@value": s, "@language": tag}`. IRIs are full IRIs: no
//! prefix or `@context` applies. A blank node is a stored node's `_:fdb-…`
//! id; any other label would lower to a variable and match every node.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::ast::annotation::{Annotation, AnnotationVerb, ReifierId, TripleTerm};
use crate::ast::pattern::{ServiceEndpoint, SubSelect};
use crate::ast::query::DescribeTarget;
use crate::ast::{
    BlankNode, Expression, GraphName, GraphPattern, GroupCondition, Iri, IriValue, Literal,
    LiteralValue, OrderExpr, PredicateTerm, QuadPatternElement, QueryBody, QuotedTriple,
    SelectVariable, SelectVariables, SolutionModifiers, SparqlAst, SubjectTerm, Term,
    TriplePattern, UpdateOperation, Var, VarOrIri,
};
use crate::span::SourceSpan;
use crate::validate::STABLE_BLANK_NODE_LABEL_PREFIX;

/// SERVICE endpoints under this prefix query a ledger of this instance from
/// the lowered patterns, not from the body text.
const LOCAL_LEDGER_SERVICE: &str = "fluree:ledger:";

/// Parameter name (without `?` or `$`) → JSON-LD value.
pub type ParamMap = serde_json::Map<String, JsonValue>;

/// A parameter that can't be substituted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamError {
    pub name: String,
    pub reason: String,
}

impl ParamError {
    fn new(name: &str, reason: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for ParamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parameter `${}` {}", self.name, self.reason)
    }
}

impl std::error::Error for ParamError {}

type Result<T> = std::result::Result<T, ParamError>;

/// Replace every variable named in `params` with its value.
pub fn substitute_params(ast: &mut SparqlAst, params: &ParamMap) -> Result<()> {
    if params.is_empty() {
        return Ok(());
    }
    let mut subst = Substitution::new(params)?;
    match &mut ast.body {
        QueryBody::Select(q) => subst.select(
            &mut q.select.variables,
            &mut q.where_clause.pattern,
            &mut q.modifiers,
            q.values.as_deref_mut(),
        )?,
        QueryBody::Construct(q) => {
            if let Some(template) = &mut q.template {
                for triple in &mut template.triples {
                    subst.triple(triple)?;
                }
                for graph in template.graphs.iter_mut().flatten() {
                    subst.graph_name(graph)?;
                }
            }
            subst.pattern(&mut q.where_clause.pattern)?;
            subst.modifiers(&mut q.modifiers, &[])?;
        }
        QueryBody::Ask(q) => {
            subst.pattern(&mut q.where_clause.pattern)?;
            subst.modifiers(&mut q.modifiers, &[])?;
        }
        QueryBody::Describe(q) => {
            if let DescribeTarget::Resources(resources) = &mut q.target {
                for resource in resources {
                    if let VarOrIri::Var(v) = resource {
                        if let Some(iri) = subst.iri(v, "is described")? {
                            *resource = VarOrIri::Iri(iri);
                        }
                    }
                }
            }
            if let Some(where_clause) = &mut q.where_clause {
                subst.pattern(&mut where_clause.pattern)?;
            }
            subst.modifiers(&mut q.modifiers, &[])?;
        }
        QueryBody::Update(request) => {
            for op in &mut request.operations {
                match &mut op.operation {
                    UpdateOperation::InsertData(d) => subst.quads(&mut d.data.quads)?,
                    UpdateOperation::DeleteData(d) => subst.quads(&mut d.data.quads)?,
                    UpdateOperation::DeleteWhere(d) => subst.quads(&mut d.pattern.patterns)?,
                    UpdateOperation::Modify(m) => {
                        if let Some(delete) = &mut m.delete_clause {
                            subst.quads(&mut delete.patterns)?;
                        }
                        if let Some(insert) = &mut m.insert_clause {
                            subst.quads(&mut insert.patterns)?;
                        }
                        subst.pattern(&mut m.where_clause)?;
                    }
                    UpdateOperation::Load(_)
                    | UpdateOperation::Clear(_)
                    | UpdateOperation::Drop(_)
                    | UpdateOperation::Create(_)
                    | UpdateOperation::Add(_)
                    | UpdateOperation::Copy(_)
                    | UpdateOperation::Move(_) => {}
                }
            }
        }
    }
    subst.finish()
}

#[derive(Clone)]
enum Value {
    Iri(Arc<str>),
    Blank(Arc<str>),
    Literal(LiteralValue),
}

impl Value {
    fn parse(name: &str, json: &JsonValue) -> Result<Self> {
        let literal = match json {
            JsonValue::Null => {
                return Err(ParamError::new(name, "is null, which is not an RDF term"));
            }
            JsonValue::Bool(b) => LiteralValue::Boolean(*b),
            JsonValue::Number(n) => match n.as_i64() {
                Some(i) => LiteralValue::Integer(i),
                None if n.is_u64() => LiteralValue::BigInteger(Arc::from(n.to_string())),
                None => LiteralValue::Double(n.as_f64().unwrap_or(f64::NAN)),
            },
            JsonValue::String(s) => LiteralValue::Simple(Arc::from(s.as_str())),
            JsonValue::Array(_) => {
                return Err(ParamError::new(
                    name,
                    "is a list; a parameter is a single RDF term",
                ));
            }
            JsonValue::Object(object) => return Self::parse_object(name, object),
        };
        Ok(Value::Literal(literal))
    }

    fn parse_object(name: &str, object: &serde_json::Map<String, JsonValue>) -> Result<Self> {
        let unexpected = |allowed: &[&str]| {
            object
                .keys()
                .find(|k| !allowed.contains(&k.as_str()))
                .map(|k| ParamError::new(name, format!("has an unexpected key `{k}`")))
        };
        if let Some(id) = object.get("@id") {
            if let Some(err) = unexpected(&["@id"]) {
                return Err(err);
            }
            let Some(id) = id.as_str() else {
                return Err(ParamError::new(name, "has an `@id` that is not a string"));
            };
            return Self::node(name, id);
        }
        let Some(value) = object.get("@value") else {
            return Err(ParamError::new(
                name,
                "is an object without `@id` or `@value`",
            ));
        };
        if let Some(err) = unexpected(&["@value", "@type", "@language"]) {
            return Err(err);
        }
        match (object.get("@type"), object.get("@language")) {
            (Some(_), Some(_)) => Err(ParamError::new(name, "has both `@type` and `@language`")),
            (None, Some(lang)) => match (value.as_str(), lang.as_str()) {
                (Some(value), Some(lang)) => Ok(Value::Literal(LiteralValue::LangTagged {
                    value: Arc::from(value),
                    lang: Arc::from(lang),
                })),
                _ => Err(ParamError::new(
                    name,
                    "has a language tag, so `@value` and `@language` must be strings",
                )),
            },
            (Some(datatype), None) => {
                let Some(datatype) = datatype.as_str() else {
                    return Err(ParamError::new(name, "has a `@type` that is not a string"));
                };
                if datatype == "@id" {
                    return match value.as_str() {
                        Some(id) => Self::node(name, id),
                        None => Err(ParamError::new(
                            name,
                            "has `\"@type\": \"@id\"`, so its `@value` must be a string",
                        )),
                    };
                }
                if datatype.starts_with('@') {
                    return Err(ParamError::new(
                        name,
                        format!(
                            "has a `@type` of `{datatype}`; only `@id` or a datatype IRI is a type"
                        ),
                    ));
                }
                let lexical = match value {
                    JsonValue::String(s) => s.clone(),
                    JsonValue::Number(n) => n.to_string(),
                    JsonValue::Bool(b) => b.to_string(),
                    _ => {
                        return Err(ParamError::new(
                            name,
                            "has a `@value` that is not a string, number or boolean",
                        ));
                    }
                };
                Ok(Value::Literal(LiteralValue::Typed {
                    value: Arc::from(lexical),
                    datatype: Box::new(Iri::full(datatype, SourceSpan::new(0, 0))),
                }))
            }
            (None, None) if value.is_object() || value.is_array() => Err(ParamError::new(
                name,
                "has a `@value` that is not a string, number or boolean",
            )),
            (None, None) => Self::parse(name, value),
        }
    }

    /// An IRI, or a stored node's `_:fdb-…` id. Any other blank-node label
    /// lowers to a non-distinguished variable, which fixes nothing.
    fn node(name: &str, id: &str) -> Result<Self> {
        match id.strip_prefix("_:") {
            Some(label) if label.starts_with(STABLE_BLANK_NODE_LABEL_PREFIX) => {
                Ok(Value::Blank(Arc::from(label)))
            }
            Some(_) => Err(ParamError::new(
                name,
                "is a blank node label, which would match any node; \
                 only a stored node's `_:fdb-…` id can be a parameter",
            )),
            None => Ok(Value::Iri(Arc::from(id))),
        }
    }

    fn term(self, span: SourceSpan) -> Term {
        match self {
            Value::Iri(iri) => Term::Iri(Iri::full(iri, span)),
            Value::Blank(label) => Term::BlankNode(BlankNode::labeled(label, span)),
            Value::Literal(value) => Term::Literal(Literal { value, span }),
        }
    }
}

struct Param {
    value: Value,
    used: bool,
}

struct Substitution<'p> {
    params: HashMap<&'p str, Param>,
    /// Inside the body of a SERVICE that is not a local ledger: that body is
    /// sent to the endpoint as written, so a substituted value would not reach it.
    in_remote_service: bool,
}

impl<'p> Substitution<'p> {
    fn new(params: &'p ParamMap) -> Result<Self> {
        let mut by_name = HashMap::with_capacity(params.len());
        for (key, json) in params {
            let name = key.trim_start_matches(['?', '$']);
            let value = Value::parse(name, json)?;
            if by_name.insert(name, Param { value, used: false }).is_some() {
                return Err(ParamError::new(
                    name,
                    format!("is given twice (`{name}`, `?{name}` and `${name}` are one variable)"),
                ));
            }
        }
        Ok(Self {
            params: by_name,
            in_remote_service: false,
        })
    }

    fn finish(self) -> Result<()> {
        let mut unused: Vec<&str> = self
            .params
            .iter()
            .filter(|(_, p)| !p.used)
            .map(|(name, _)| *name)
            .collect();
        unused.sort_unstable();
        match unused.first() {
            Some(name) => Err(ParamError::new(name, "is not a variable in the query")),
            None => Ok(()),
        }
    }

    /// The value for `var`, when it is a parameter.
    fn value(&mut self, var: &Var) -> Result<Option<Value>> {
        let Some(param) = self.params.get_mut(var.name.as_ref()) else {
            return Ok(None);
        };
        param.used = true;
        if self.in_remote_service {
            return Err(ParamError::new(
                &var.name,
                "is inside a remote SERVICE, whose body is sent to the endpoint as written",
            ));
        }
        Ok(Some(param.value.clone()))
    }

    /// Reject a parameter in a position that binds its variable.
    fn assigned(&mut self, var: &Var) -> Result<()> {
        match self.value(var)? {
            Some(_) => Err(ParamError::new(
                &var.name,
                "is assigned in the query (by BIND, VALUES or AS), so it can't be a parameter",
            )),
            None => Ok(()),
        }
    }

    fn iri(&mut self, var: &Var, position: &str) -> Result<Option<Iri>> {
        match self.value(var)? {
            None => Ok(None),
            Some(Value::Iri(iri)) => Ok(Some(Iri::full(iri, var.span))),
            Some(_) => Err(ParamError::new(
                &var.name,
                format!("{position}, so it must be an IRI"),
            )),
        }
    }

    fn subject(&mut self, subject: &mut SubjectTerm) -> Result<()> {
        match subject {
            SubjectTerm::Var(v) => match self.value(v)? {
                None => {}
                Some(Value::Iri(iri)) => *subject = SubjectTerm::Iri(Iri::full(iri, v.span)),
                Some(Value::Blank(label)) => {
                    *subject = SubjectTerm::BlankNode(BlankNode::labeled(label, v.span));
                }
                Some(Value::Literal(_)) => {
                    return Err(ParamError::new(
                        &v.name,
                        "is a subject, so it must be an IRI or a blank node",
                    ));
                }
            },
            SubjectTerm::QuotedTriple(q) => self.quoted(q)?,
            SubjectTerm::TripleTerm(t) => self.triple_term(t)?,
            SubjectTerm::Iri(_) | SubjectTerm::BlankNode(_) => {}
        }
        Ok(())
    }

    fn predicate(&mut self, predicate: &mut PredicateTerm) -> Result<()> {
        if let PredicateTerm::Var(v) = predicate {
            if let Some(iri) = self.iri(v, "is a predicate")? {
                *predicate = PredicateTerm::Iri(iri);
            }
        }
        Ok(())
    }

    fn object(&mut self, object: &mut Term) -> Result<()> {
        match object {
            Term::Var(v) => {
                if let Some(value) = self.value(v)? {
                    *object = value.term(v.span);
                }
            }
            Term::QuotedTriple(q) => self.quoted(q)?,
            Term::TripleTerm(t) => self.triple_term(t)?,
            Term::Iri(_) | Term::Literal(_) | Term::BlankNode(_) => {}
        }
        Ok(())
    }

    fn graph_name(&mut self, graph: &mut GraphName) -> Result<()> {
        if let GraphName::Var(v) = graph {
            if let Some(iri) = self.iri(v, "names a graph")? {
                *graph = GraphName::Iri(iri);
            }
        }
        Ok(())
    }

    fn reifier(&mut self, reifier: &mut ReifierId) -> Result<()> {
        if let ReifierId::Var(v) = reifier {
            match self.value(v)? {
                None => {}
                Some(Value::Iri(iri)) => *reifier = ReifierId::Iri(Iri::full(iri, v.span)),
                Some(Value::Blank(label)) => {
                    *reifier = ReifierId::BlankNode(BlankNode::labeled(label, v.span));
                }
                Some(Value::Literal(_)) => {
                    return Err(ParamError::new(
                        &v.name,
                        "is a reifier, so it must be an IRI or a blank node",
                    ));
                }
            }
        }
        Ok(())
    }

    fn quoted(&mut self, quoted: &mut QuotedTriple) -> Result<()> {
        self.subject(&mut quoted.subject)?;
        self.predicate(&mut quoted.predicate)?;
        self.object(&mut quoted.object)?;
        if let Some(id) = quoted.reifier.as_mut().and_then(|r| r.id.as_mut()) {
            self.reifier(id)?;
        }
        Ok(())
    }

    fn triple_term(&mut self, triple: &mut TripleTerm) -> Result<()> {
        self.subject(&mut triple.subject)?;
        self.predicate(&mut triple.predicate)?;
        self.object(&mut triple.object)
    }

    fn triple(&mut self, triple: &mut TriplePattern) -> Result<()> {
        self.subject(&mut triple.subject)?;
        self.predicate(&mut triple.predicate)?;
        self.object(&mut triple.object)?;
        if let Some(annotation) = &mut triple.annotation {
            self.annotation(annotation)?;
        }
        Ok(())
    }

    fn annotation(&mut self, annotation: &mut Annotation) -> Result<()> {
        for unit in &mut annotation.units {
            if let Some(reifier) = &mut unit.reifier {
                self.reifier(reifier)?;
            }
            for entry in unit.block.iter_mut().flat_map(|b| &mut b.entries) {
                if let AnnotationVerb::Simple(predicate) = &mut entry.verb {
                    self.predicate(predicate)?;
                }
                self.object(&mut entry.object)?;
            }
        }
        Ok(())
    }

    fn quads(&mut self, quads: &mut [QuadPatternElement]) -> Result<()> {
        for quad in quads {
            match quad {
                QuadPatternElement::Triple(triple) => self.triple(triple)?,
                QuadPatternElement::Graph { name, triples, .. } => {
                    self.graph_name(name)?;
                    for triple in triples {
                        self.triple(triple)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn pattern(&mut self, pattern: &mut GraphPattern) -> Result<()> {
        match pattern {
            GraphPattern::Bgp { patterns, .. } => {
                for triple in patterns {
                    self.triple(triple)?;
                }
            }
            GraphPattern::Group { patterns, .. } => {
                for p in patterns {
                    self.pattern(p)?;
                }
            }
            GraphPattern::Optional { pattern, .. } => self.pattern(pattern)?,
            GraphPattern::Union { left, right, .. } | GraphPattern::Minus { left, right, .. } => {
                self.pattern(left)?;
                self.pattern(right)?;
            }
            GraphPattern::Filter { expr, .. } => self.expr(expr)?,
            GraphPattern::Bind { expr, var, .. } => {
                self.assigned(var)?;
                self.expr(expr)?;
            }
            GraphPattern::Values { vars, .. } => {
                for var in vars {
                    self.assigned(var)?;
                }
            }
            GraphPattern::Graph { name, pattern, .. } => {
                self.graph_name(name)?;
                self.pattern(pattern)?;
            }
            GraphPattern::Service {
                endpoint, pattern, ..
            } => {
                if let ServiceEndpoint::Var(v) = endpoint {
                    if let Some(iri) = self.iri(v, "names a SERVICE endpoint")? {
                        *endpoint = ServiceEndpoint::Iri(iri);
                    }
                }
                let local = matches!(
                    endpoint,
                    ServiceEndpoint::Iri(Iri { value: IriValue::Full(iri), .. })
                        if iri.starts_with(LOCAL_LEDGER_SERVICE)
                );
                let outer = std::mem::replace(&mut self.in_remote_service, !local);
                let result = self.pattern(pattern);
                self.in_remote_service = outer;
                result?;
            }
            GraphPattern::SubSelect { query, .. } => {
                let SubSelect {
                    variables,
                    pattern,
                    modifiers,
                    values,
                    ..
                } = query.as_mut();
                self.select(variables, pattern, modifiers, values.as_deref_mut())?;
            }
            GraphPattern::Path {
                subject, object, ..
            } => {
                self.subject(subject)?;
                self.object(object)?;
            }
            GraphPattern::AnnotationTarget {
                reifier,
                predicate,
                triple_term,
                ..
            } => {
                self.subject(reifier)?;
                self.predicate(predicate)?;
                self.triple_term(triple_term)?;
            }
        }
        Ok(())
    }

    fn select(
        &mut self,
        variables: &mut SelectVariables,
        pattern: &mut GraphPattern,
        modifiers: &mut SolutionModifiers,
        values: Option<&mut GraphPattern>,
    ) -> Result<()> {
        // A parameter that is also a GROUP BY key is projected through the key.
        let grouped: Vec<Arc<str>> = modifiers
            .group_by
            .iter()
            .flat_map(|g| &g.conditions)
            .filter_map(|c| match c {
                GroupCondition::Var(v) if self.params.contains_key(v.name.as_ref()) => {
                    Some(v.name.clone())
                }
                _ => None,
            })
            .collect();
        if let SelectVariables::Explicit(items) = variables {
            for item in items {
                match item {
                    SelectVariable::Var(v) if !grouped.contains(&v.name) => {
                        let v = v.clone();
                        if let Some(value) = self.value(&v)? {
                            *item = SelectVariable::Expr {
                                expr: self.constant(&v, value)?,
                                span: v.span,
                                alias: v,
                            };
                        }
                    }
                    SelectVariable::Var(_) => {}
                    SelectVariable::Expr { expr, alias, .. } => {
                        self.assigned(alias)?;
                        self.expr(expr)?;
                    }
                }
            }
        }
        self.pattern(pattern)?;
        self.modifiers(modifiers, &grouped)?;
        if let Some(values) = values {
            self.pattern(values)?;
        }
        Ok(())
    }

    fn modifiers(&mut self, modifiers: &mut SolutionModifiers, grouped: &[Arc<str>]) -> Result<()> {
        if let Some(group_by) = &mut modifiers.group_by {
            for condition in &mut group_by.conditions {
                match condition {
                    GroupCondition::Var(v) if grouped.contains(&v.name) => {
                        let v = v.clone();
                        let value = self.value(&v)?.expect("grouped names are parameters");
                        *condition = GroupCondition::Expr {
                            expr: self.constant(&v, value)?,
                            span: v.span,
                            alias: Some(v),
                        };
                    }
                    GroupCondition::Var(_) => {}
                    GroupCondition::Expr { expr, alias, .. } => {
                        if let Some(alias) = alias {
                            self.assigned(alias)?;
                        }
                        self.expr(expr)?;
                    }
                }
            }
        }
        if let Some(having) = &mut modifiers.having {
            for condition in &mut having.conditions {
                self.expr(condition)?;
            }
        }
        if let Some(order_by) = &mut modifiers.order_by {
            for condition in &mut order_by.conditions {
                match &mut condition.expr {
                    OrderExpr::Var(v) => {
                        if let Some(value) = self.value(v)? {
                            let constant = self.constant(v, value)?;
                            condition.expr = OrderExpr::Expr(constant);
                        }
                    }
                    OrderExpr::Expr(expr) => self.expr(expr)?,
                }
            }
        }
        Ok(())
    }

    fn constant(&self, var: &Var, value: Value) -> Result<Expression> {
        match value.term(var.span) {
            Term::Iri(iri) => Ok(Expression::Iri(iri)),
            Term::Literal(literal) => Ok(Expression::Literal(literal)),
            _ => Err(ParamError::new(
                &var.name,
                "is a blank node, which can't be used in an expression",
            )),
        }
    }

    fn expr(&mut self, expr: &mut Expression) -> Result<()> {
        match expr {
            Expression::Var(v) => {
                if let Some(value) = self.value(v)? {
                    *expr = self.constant(v, value)?;
                }
            }
            Expression::Literal(_) | Expression::Iri(_) => {}
            Expression::Binary { left, right, .. } => {
                self.expr(left)?;
                self.expr(right)?;
            }
            Expression::Unary { operand, .. } => self.expr(operand)?,
            Expression::FunctionCall { args, .. } | Expression::Coalesce { args, .. } => {
                for arg in args {
                    self.expr(arg)?;
                }
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                self.expr(condition)?;
                self.expr(then_expr)?;
                self.expr(else_expr)?;
            }
            Expression::In { expr, list, .. } => {
                self.expr(expr)?;
                for item in list {
                    self.expr(item)?;
                }
            }
            Expression::Exists { pattern, .. } | Expression::NotExists { pattern, .. } => {
                self.pattern(pattern)?;
            }
            Expression::Aggregate { expr, .. } => {
                if let Some(inner) = expr {
                    self.expr(inner)?;
                }
            }
            Expression::Bracketed { inner, .. } => self.expr(inner)?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_sparql;
    use serde_json::json;

    fn substituted(sparql: &str, params: JsonValue) -> Result<SparqlAst> {
        let mut ast = parse_sparql(sparql).ast.expect("parses");
        let JsonValue::Object(params) = params else {
            panic!("params must be an object")
        };
        substitute_params(&mut ast, &params)?;
        Ok(ast)
    }

    fn mentions_var(ast: &SparqlAst, name: &str) -> bool {
        format!("{ast:?}").contains(&format!("Var {{ name: {name:?}"))
    }

    fn select(ast: &SparqlAst) -> &crate::ast::SelectQuery {
        match &ast.body {
            QueryBody::Select(q) => q,
            _ => panic!("not a SELECT"),
        }
    }

    fn first_triple(pattern: &GraphPattern) -> &TriplePattern {
        match pattern {
            GraphPattern::Bgp { patterns, .. } => &patterns[0],
            GraphPattern::Group { patterns, .. } => first_triple(&patterns[0]),
            other => panic!("no triple in {other:?}"),
        }
    }

    #[test]
    fn dollar_and_question_mark_are_the_same_variable() {
        for sparql in [
            "SELECT ?s WHERE { ?s <name> $name }",
            "SELECT ?s WHERE { ?s <name> ?name }",
        ] {
            let ast = substituted(sparql, json!({"name": "Alice"})).unwrap();
            let triple = first_triple(&select(&ast).where_clause.pattern);
            assert!(matches!(
                &triple.object,
                Term::Literal(Literal { value: LiteralValue::Simple(s), .. }) if s.as_ref() == "Alice"
            ));
            assert!(!mentions_var(&ast, "name"));
        }
    }

    #[test]
    fn no_parameters_leave_the_query_untouched() {
        let sparql = "SELECT ?s WHERE { ?s <name> $name }";
        let ast = substituted(sparql, json!({})).unwrap();
        assert_eq!(ast, parse_sparql(sparql).ast.unwrap());
    }

    #[test]
    fn values_follow_the_json_ld_forms() {
        let ast = substituted(
            "SELECT * WHERE { ?s <p> $a, $b, $c, $d, $e, $f, $g, $h }",
            json!({
                "a": 7,
                "b": 1.5,
                "c": true,
                "d": {"@id": "http://example.org/x"},
                "e": {"@id": "_:fdb-1"},
                "f": {"@value": "2024-01-02", "@type": "http://www.w3.org/2001/XMLSchema#date"},
                "g": {"@value": "chat", "@language": "fr"},
                "h": {"@value": 3},
            }),
        )
        .unwrap();
        let GraphPattern::Bgp { patterns, .. } = &select(&ast).where_clause.pattern else {
            panic!("expected a BGP")
        };
        let objects: Vec<&Term> = patterns.iter().map(|t| &t.object).collect();
        assert!(matches!(
            objects[0],
            Term::Literal(Literal {
                value: LiteralValue::Integer(7),
                ..
            })
        ));
        assert!(
            matches!(objects[1], Term::Literal(Literal { value: LiteralValue::Double(d), .. }) if *d == 1.5)
        );
        assert!(matches!(
            objects[2],
            Term::Literal(Literal {
                value: LiteralValue::Boolean(true),
                ..
            })
        ));
        assert!(
            matches!(objects[3], Term::Iri(Iri { value: crate::ast::IriValue::Full(i), .. }) if i.as_ref() == "http://example.org/x")
        );
        assert!(
            matches!(objects[4], Term::BlankNode(BlankNode { value: crate::ast::BlankNodeValue::Labeled(l), .. }) if l.as_ref() == "fdb-1")
        );
        assert!(
            matches!(objects[5], Term::Literal(Literal { value: LiteralValue::Typed { value, .. }, .. }) if value.as_ref() == "2024-01-02")
        );
        assert!(
            matches!(objects[6], Term::Literal(Literal { value: LiteralValue::LangTagged { lang, .. }, .. }) if lang.as_ref() == "fr")
        );
        assert!(matches!(
            objects[7],
            Term::Literal(Literal {
                value: LiteralValue::Integer(3),
                ..
            })
        ));
    }

    #[test]
    fn values_that_are_not_one_term_are_rejected() {
        for (value, reason) in [
            (json!(null), "null"),
            (json!([1, 2]), "list"),
            (json!({"x": 1}), "without `@id` or `@value`"),
            (json!({"@id": "_:"}), "blank node label"),
            (json!({"@value": "_:x", "@type": "@id"}), "blank node label"),
            (json!({"@value": 1, "@type": "@id"}), "must be a string"),
            (
                json!({"@value": "x", "@type": "@vocab"}),
                "only `@id` or a datatype IRI",
            ),
            (json!({"@id": "x", "@type": "y"}), "unexpected key"),
            (
                json!({"@value": "a", "@type": "t", "@language": "en"}),
                "both",
            ),
            (json!({"@value": 1, "@language": "en"}), "must be strings"),
        ] {
            let err =
                substituted("SELECT ?s WHERE { ?s <p> $v }", json!({ "v": value })).unwrap_err();
            assert!(err.reason.contains(reason), "{err}");
        }
    }

    #[test]
    fn only_a_stored_blank_node_is_a_parameter() {
        // Any other label lowers to a variable: under DELETE WHERE it would
        // delete every triple instead of one node's.
        for sparql in ["DELETE WHERE { $s ?p ?o }", "SELECT ?o WHERE { $s ?p ?o }"] {
            for value in [
                json!({"@id": "_:x"}),
                json!({"@value": "_:x", "@type": "@id"}),
            ] {
                let err = substituted(sparql, json!({ "s": value })).unwrap_err();
                assert!(err.reason.contains("blank node label"), "{sparql}: {err}");
            }
        }
    }

    #[test]
    fn an_id_typed_value_is_a_node() {
        let ast = substituted(
            "SELECT * WHERE { ?s <p> $a, $b }",
            json!({
                "a": {"@value": "http://example.org/x", "@type": "@id"},
                "b": {"@value": "_:fdb-1", "@type": "@id"},
            }),
        )
        .unwrap();
        let GraphPattern::Bgp { patterns, .. } = &select(&ast).where_clause.pattern else {
            panic!("expected a BGP")
        };
        assert!(
            matches!(&patterns[0].object, Term::Iri(Iri { value: IriValue::Full(i), .. }) if i.as_ref() == "http://example.org/x")
        );
        assert!(
            matches!(&patterns[1].object, Term::BlankNode(BlankNode { value: crate::ast::BlankNodeValue::Labeled(l), .. }) if l.as_ref() == "fdb-1")
        );
    }

    #[test]
    fn a_name_given_twice_is_an_error() {
        let err = substituted(
            "SELECT ?s WHERE { ?s <name> $name }",
            json!({"name": "Alice", "$name": "Bob"}),
        )
        .unwrap_err();
        assert!(err.reason.contains("given twice"), "{err}");
    }

    #[test]
    fn a_parameter_not_in_the_query_is_an_error() {
        let err = substituted(
            "SELECT ?s WHERE { ?s <name> $name }",
            json!({"nmae": "Alice"}),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "parameter `$nmae` is not a variable in the query"
        );
    }

    #[test]
    fn positions_constrain_the_term() {
        let ast = substituted(
            "SELECT ?o WHERE { GRAPH $g { $s $p ?o } }",
            json!({"s": {"@id": "_:fdb-b"}, "p": {"@id": "http://example.org/p"}, "g": {"@id": "http://example.org/g"}}),
        )
        .unwrap();
        assert!(!mentions_var(&ast, "s") && !mentions_var(&ast, "p") && !mentions_var(&ast, "g"));

        for (sparql, reason) in [
            ("SELECT * WHERE { $v <p> ?o }", "is a subject"),
            ("SELECT * WHERE { ?s $v ?o }", "is a predicate"),
            ("SELECT * WHERE { GRAPH $v { ?s ?p ?o } }", "names a graph"),
            ("SELECT * WHERE { SERVICE $v { ?s ?p ?o } }", "SERVICE"),
        ] {
            let err = substituted(sparql, json!({"v": "literal"})).unwrap_err();
            assert!(err.reason.contains(reason), "{sparql}: {err}");
        }
        let err = substituted(
            "SELECT * WHERE { ?s <p> ?o FILTER(?o = $v) }",
            json!({"v": {"@id": "_:fdb-b"}}),
        )
        .unwrap_err();
        assert!(err.reason.contains("blank node"), "{err}");
    }

    #[test]
    fn only_a_local_service_body_takes_parameters() {
        let ast = substituted(
            "SELECT ?s WHERE { SERVICE <fluree:ledger:other:main> { ?s <p> $v } }",
            json!({"v": 1}),
        )
        .unwrap();
        assert!(!mentions_var(&ast, "v"));
        for sparql in [
            "SELECT ?s WHERE { SERVICE <http://example.org/sparql> { ?s <p> $v } }",
            "SELECT ?s WHERE { SERVICE ?e { ?s <p> $v } }",
        ] {
            let err = substituted(sparql, json!({"v": 1})).unwrap_err();
            assert!(err.reason.contains("remote SERVICE"), "{sparql}: {err}");
        }
        let ast = substituted(
            "SELECT ?s WHERE { SERVICE $e { ?s <p> ?o } }",
            json!({"e": {"@id": "http://example.org/sparql"}}),
        )
        .unwrap();
        assert!(!mentions_var(&ast, "e"));
    }

    #[test]
    fn nested_scopes_see_the_value() {
        let ast = substituted(
            "SELECT ?s WHERE {
               ?s <p> ?o
               OPTIONAL { ?s <age> ?a FILTER(?a > $min) }
               FILTER EXISTS { ?s <q> $min }
               { SELECT ?s WHERE { ?s <r> $min } }
               MINUS { ?s <t> $min }
             }",
            json!({"min": 21}),
        )
        .unwrap();
        assert!(!mentions_var(&ast, "min"));
    }

    #[test]
    fn projected_parameters_stay_columns() {
        let ast = substituted(
            "SELECT ?name ?s WHERE { ?s <name> ?name } ORDER BY ?name",
            json!({"name": "Alice"}),
        )
        .unwrap();
        let q = select(&ast);
        let SelectVariables::Explicit(items) = &q.select.variables else {
            panic!("explicit projection")
        };
        assert!(matches!(
            &items[0],
            SelectVariable::Expr { expr: Expression::Literal(_), alias, .. } if alias.name.as_ref() == "name"
        ));
        let order = &q.modifiers.order_by.as_ref().unwrap().conditions[0];
        assert!(matches!(
            order.expr,
            OrderExpr::Expr(Expression::Literal(_))
        ));
    }

    #[test]
    fn a_grouped_parameter_is_projected_through_its_key() {
        let ast = substituted(
            "SELECT ?name (COUNT(?s) AS ?n) WHERE { ?s <name> ?name } GROUP BY ?name",
            json!({"name": "Alice"}),
        )
        .unwrap();
        let q = select(&ast);
        let SelectVariables::Explicit(items) = &q.select.variables else {
            panic!("explicit projection")
        };
        assert!(matches!(&items[0], SelectVariable::Var(v) if v.name.as_ref() == "name"));
        assert!(matches!(
            &q.modifiers.group_by.as_ref().unwrap().conditions[0],
            GroupCondition::Expr { expr: Expression::Literal(_), alias: Some(v), .. } if v.name.as_ref() == "name"
        ));
    }

    #[test]
    fn a_parameter_the_query_assigns_is_an_error() {
        for sparql in [
            "SELECT ?x WHERE { ?s <p> ?o BIND(?o AS ?x) }",
            "SELECT ?x WHERE { VALUES ?x { 1 2 } ?s <p> ?x }",
            "SELECT (?o AS ?x) WHERE { ?s <p> ?o }",
            "SELECT ?x WHERE { ?s <p> ?o } GROUP BY (?o AS ?x)",
            "SELECT ?s WHERE { ?s <p> ?o } VALUES ?x { 1 }",
        ] {
            let err = substituted(sparql, json!({"x": 1})).unwrap_err();
            assert!(err.reason.contains("assigned"), "{sparql}: {err}");
        }
    }

    #[test]
    fn update_templates_and_where_are_substituted() {
        let ast = substituted(
            "DELETE { ?s <status> ?old } INSERT { ?s <status> $new } WHERE { ?s <id> $id ; <status> ?old }",
            json!({"id": 7, "new": "done"}),
        )
        .unwrap();
        assert!(!mentions_var(&ast, "id") && !mentions_var(&ast, "new"));
        assert!(mentions_var(&ast, "old"));

        let ast = substituted(
            "DELETE WHERE { $s ?p ?o }",
            json!({"s": {"@id": "http://example.org/x"}}),
        )
        .unwrap();
        assert!(!mentions_var(&ast, "s"));
    }

    #[test]
    fn construct_and_describe_are_substituted() {
        let ast = substituted(
            "CONSTRUCT { $s <seen> true } WHERE { $s ?p ?o }",
            json!({"s": {"@id": "http://example.org/x"}}),
        )
        .unwrap();
        assert!(!mentions_var(&ast, "s"));
        let ast =
            substituted("DESCRIBE $s", json!({"s": {"@id": "http://example.org/x"}})).unwrap();
        assert!(!mentions_var(&ast, "s"));
    }
}
