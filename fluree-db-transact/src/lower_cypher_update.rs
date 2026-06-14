//! Cypher write statement → Transaction IR lowering.
//!
//! Maps Cypher's `CREATE / SET / REMOVE / DELETE / DETACH DELETE /
//! MERGE` to the same `Txn` shape that `lower_sparql_update` and the
//! JSON-LD transactor produce. The shared staging pipeline handles
//! flake generation, cascade, policy, and firewalls — Cypher-side
//! work is purely about producing the right `TripleTemplate` bundle.
//!
//! See `GQL_CYPHER_SUPPORT.md` §M5.4 for the per-clause rules.
//!
//! ## v1 scope
//!
//! - **CREATE** (nodes + directed typed relationships, with optional
//!   relationship properties producing an `f:reifies*` bundle).
//! - **CREATE** with leading MATCH bindings is structurally accepted
//!   but the WHERE-driven template substitution is not yet wired —
//!   pattern-only CREATE works today.
//!
//! Other write clauses (SET / REMOVE / DELETE / MERGE) are stubbed
//! out and return a clear deferred-feature error. The follow-on slice
//! lands them.

use std::sync::Arc;

use fluree_db_core::{FlakeValue, Sid};
use fluree_db_query::VarRegistry;
use fluree_vocab::{rdf, reifies_iris};
use thiserror::Error;

use fluree_db_cypher::ast::{
    CreateClause, CypherAst, Direction, Expr, Label, Literal, MapLit, NodePattern, Pattern,
    PatternPart, RelPattern, Statement, Update, WriteClause,
};

use crate::ir::{TemplateTerm, TripleTemplate, Txn, TxnOpts, TxnType};
use crate::namespace::NamespaceRegistry;

/// Errors raised by Cypher → Txn lowering.
#[derive(Debug, Error)]
pub enum LowerCypherError {
    #[error("Cypher AST is a read query, not an update")]
    NotAnUpdate,

    #[error("{0}")]
    Generic(String),

    #[error("unsupported in v1: {0}")]
    Unsupported(String),

    #[error("rejected: {0}")]
    Rejected(String),

    #[error("attempt to use reserved Fluree system predicate: {0}")]
    ReservedPredicate(String),
}

impl LowerCypherError {
    fn unsupported(s: impl Into<String>) -> Self {
        Self::Unsupported(s.into())
    }

    fn rejected(s: impl Into<String>) -> Self {
        Self::Rejected(s.into())
    }
}

/// Options for Cypher update lowering. Mirrors the SPARQL update
/// signature so callers can pass through `opts.lpg_edge_lifecycle`
/// even though Cypher writes default to LPG mode.
#[derive(Debug, Default)]
pub struct CypherLowerOpts {
    /// Default vocab IRI used to resolve bare Cypher identifiers
    /// (labels, types, property keys). Defaults to
    /// `http://example.org/`.
    pub vocab: Option<String>,
    /// Per-term IRI overrides. A bare Cypher identifier (label,
    /// relationship type, property key) that has an entry here resolves
    /// to the override IRI rather than `vocab + name`. Mirrors the
    /// read-path `LoweringContext::overrides`, ensuring write Cypher
    /// honors the same ledger context mappings as read Cypher.
    pub overrides: std::collections::HashMap<String, String>,
}

/// Lower a parsed Cypher AST to a `Txn`. Only valid for write
/// statements; queries must use `fluree_db_cypher::lower_cypher`.
pub fn lower_cypher_update(
    ast: &CypherAst,
    ns: &mut NamespaceRegistry,
    opts: TxnOpts,
    cypher_opts: CypherLowerOpts,
) -> Result<Txn, LowerCypherError> {
    let update = match &ast.statement {
        Statement::Update(u) => u,
        Statement::Query(_) => return Err(LowerCypherError::NotAnUpdate),
    };

    let mut lower = CypherLowering::new(ns, opts, cypher_opts);
    lower.lower_update(update)?;
    let mut txn = lower.finish();
    txn.namespace_delta = ns.delta().clone();
    Ok(txn)
}

struct CypherLowering<'a> {
    ns: &'a mut NamespaceRegistry,
    vocab: String,
    overrides: std::collections::HashMap<String, String>,
    vars: VarRegistry,
    txn_type: TxnType,
    delete_templates: Vec<TripleTemplate>,
    insert_templates: Vec<TripleTemplate>,
    opts: TxnOpts,
    /// Counter for fresh blank-node labels in the CREATE template.
    bnode_counter: u32,
    /// Stable per-pattern-occurrence node labels, keyed by node span.
    /// Used so two appearances of the same node pattern in `CREATE
    /// (a)-[]->(b), (a)-[]->(c)` resolve to the same SID at staging
    /// time — when `a` has no `var`, the second occurrence reuses the
    /// span of the first.
    node_subject_cache: std::collections::HashMap<(usize, usize), TemplateTerm>,
}

impl<'a> CypherLowering<'a> {
    fn new(ns: &'a mut NamespaceRegistry, opts: TxnOpts, cypher_opts: CypherLowerOpts) -> Self {
        let vocab = cypher_opts
            .vocab
            .unwrap_or_else(|| "http://example.org/".to_string());
        Self {
            ns,
            vocab,
            overrides: cypher_opts.overrides,
            vars: VarRegistry::new(),
            txn_type: TxnType::Insert,
            delete_templates: Vec::new(),
            insert_templates: Vec::new(),
            opts,
            bnode_counter: 0,
            node_subject_cache: std::collections::HashMap::new(),
        }
    }

    fn finish(self) -> Txn {
        Txn {
            txn_type: self.txn_type,
            where_patterns: Vec::new(),
            sparql_where: None,
            delete_templates: self.delete_templates,
            insert_templates: self.insert_templates,
            values: None,
            update_where_default_graph_iris: None,
            update_where_named_graphs: None,
            opts: self.opts,
            vars: self.vars,
            txn_meta: Vec::new(),
            graph_delta: Default::default(),
            namespace_delta: std::collections::HashMap::new(),
        }
    }

    fn lower_update(&mut self, update: &Update) -> Result<(), LowerCypherError> {
        if !update.read_clauses.is_empty() {
            return Err(LowerCypherError::unsupported(
                "MATCH before CREATE/SET/DELETE (template-driven writes) is deferred in v1 — submit a pure CREATE",
            ));
        }
        if update.return_clause.is_some() {
            return Err(LowerCypherError::unsupported(
                "RETURN on a write statement is deferred in v1",
            ));
        }

        for clause in &update.write_clauses {
            match clause {
                WriteClause::Create(c) => self.lower_create(c)?,
                WriteClause::Set(_) => {
                    return Err(LowerCypherError::unsupported(
                        "SET is deferred — initial Cypher write slice covers CREATE",
                    ));
                }
                WriteClause::Remove(_) => {
                    return Err(LowerCypherError::unsupported(
                        "REMOVE is deferred — initial Cypher write slice covers CREATE",
                    ));
                }
                WriteClause::Delete(_) => {
                    return Err(LowerCypherError::unsupported(
                        "DELETE is deferred — initial Cypher write slice covers CREATE",
                    ));
                }
                WriteClause::Merge(_) => {
                    return Err(LowerCypherError::unsupported(
                        "MERGE is deferred. Cypher's find-or-create semantics need a \
                         search-first phase that the existing TxnType variants don't \
                         model (Insert is unconditional; Upsert is delete-then-insert; \
                         Update silently skips unbound variables). A v1.1 implementation \
                         can layer it at the API level: snapshot-query for the \
                         identifying pattern, then conditionally stage CREATE-shape \
                         flakes when no match is found, or apply ON MATCH SET when \
                         matches exist.",
                    ));
                }
            }
        }
        Ok(())
    }

    fn lower_create(&mut self, c: &CreateClause) -> Result<(), LowerCypherError> {
        self.lower_create_pattern(&c.pattern)
    }

    fn lower_create_pattern(&mut self, p: &Pattern) -> Result<(), LowerCypherError> {
        for part in &p.parts {
            self.lower_create_part(part)?;
        }
        Ok(())
    }

    fn lower_create_part(&mut self, part: &PatternPart) -> Result<(), LowerCypherError> {
        require_node_anchored(&part.head)?;
        let head_subj = self.node_subject(&part.head);
        self.lower_node_create(&part.head, head_subj.clone())?;

        let mut prev_subj = head_subj;
        let mut prev_node = &part.head;
        for (rel, next) in &part.tail {
            // Both nodes must be anchored if they appear in CREATE.
            require_node_anchored(next)?;
            let next_subj = self.node_subject(next);
            self.lower_node_create(next, next_subj.clone())?;
            self.lower_rel_create(prev_node, &prev_subj, rel, next, &next_subj)?;
            prev_subj = next_subj;
            prev_node = next;
        }
        Ok(())
    }

    fn lower_node_create(
        &mut self,
        n: &NodePattern,
        subj: TemplateTerm,
    ) -> Result<(), LowerCypherError> {
        // Labels — emit (n, rdf:type, label_iri).
        let rdf_type_sid = self.ns.sid_for_iri(rdf::TYPE);
        for Label { name, .. } in &n.labels {
            let iri = self.resolve_iri(name);
            let label_sid = self.ns.sid_for_iri(&iri);
            self.insert_templates.push(TripleTemplate::new(
                subj.clone(),
                TemplateTerm::Sid(rdf_type_sid.clone()),
                TemplateTerm::Sid(label_sid),
            ));
        }
        // Inline properties — emit (n, prop, value).
        if let Some(props) = &n.props {
            self.emit_property_triples(&subj, props)?;
        }
        Ok(())
    }

    fn lower_rel_create(
        &mut self,
        _left_node: &NodePattern,
        left_subj: &TemplateTerm,
        rel: &RelPattern,
        _right_node: &NodePattern,
        right_subj: &TemplateTerm,
    ) -> Result<(), LowerCypherError> {
        if matches!(rel.direction, Direction::Either) {
            return Err(LowerCypherError::rejected(
                "undirected relationship `-[r]-` in CREATE — use `-[r]->` or `<-[r]-`",
            ));
        }
        if rel.length.is_some() {
            return Err(LowerCypherError::rejected(
                "variable-length paths in CREATE are not allowed",
            ));
        }
        if rel.types.len() != 1 {
            return Err(LowerCypherError::rejected(
                "CREATE relationship needs exactly one type — `-[:T]->`",
            ));
        }
        let type_iri = self.resolve_predicate(&rel.types[0].name)?;
        let type_sid = self.ns.sid_for_iri(&type_iri);

        let (s, o) = match rel.direction {
            Direction::Outgoing => (left_subj.clone(), right_subj.clone()),
            Direction::Incoming => (right_subj.clone(), left_subj.clone()),
            Direction::Either => unreachable!(),
        };

        // Base edge triple.
        self.insert_templates.push(TripleTemplate::new(
            s.clone(),
            TemplateTerm::Sid(type_sid.clone()),
            o.clone(),
        ));

        // LPG mode: always mint a reifier bundle. The annotation
        // subject is a fresh blank node. Cypher CREATE always carries
        // relationship identity per the plan's LPG-default rule.
        let ann = self.fresh_bnode();
        self.emit_reifier_bundle(&ann, &s, &type_sid, &o)?;

        // Relationship property body.
        if let Some(props) = &rel.props {
            self.emit_property_triples(&ann, props)?;
        }

        Ok(())
    }

    fn emit_reifier_bundle(
        &mut self,
        ann: &TemplateTerm,
        s: &TemplateTerm,
        p_sid: &Sid,
        o: &TemplateTerm,
    ) -> Result<(), LowerCypherError> {
        let subj_pred = self.ns.sid_for_iri(reifies_iris::SUBJECT);
        let pred_pred = self.ns.sid_for_iri(reifies_iris::PREDICATE);
        let obj_pred = self.ns.sid_for_iri(reifies_iris::OBJECT);

        self.insert_templates.push(TripleTemplate::new(
            ann.clone(),
            TemplateTerm::Sid(subj_pred),
            s.clone(),
        ));
        self.insert_templates.push(TripleTemplate::new(
            ann.clone(),
            TemplateTerm::Sid(pred_pred),
            TemplateTerm::Sid(p_sid.clone()),
        ));
        self.insert_templates.push(TripleTemplate::new(
            ann.clone(),
            TemplateTerm::Sid(obj_pred),
            o.clone(),
        ));
        Ok(())
    }

    fn emit_property_triples(
        &mut self,
        subj: &TemplateTerm,
        props: &MapLit,
    ) -> Result<(), LowerCypherError> {
        for (key, val_expr) in &props.entries {
            let pred_iri = self.resolve_predicate(key)?;
            let pred_sid = self.ns.sid_for_iri(&pred_iri);
            let obj = self.expr_to_object(val_expr)?;
            self.insert_templates.push(TripleTemplate::new(
                subj.clone(),
                TemplateTerm::Sid(pred_sid),
                obj,
            ));
        }
        Ok(())
    }

    fn expr_to_object(&self, e: &Expr) -> Result<TemplateTerm, LowerCypherError> {
        match e {
            Expr::Lit(lit) => Ok(TemplateTerm::Value(lower_literal_value(lit)?)),
            Expr::Var(_) => Err(LowerCypherError::unsupported(
                "variable references in CREATE property maps are deferred (requires WHERE-driven templates)",
            )),
            _ => Err(LowerCypherError::unsupported(
                "CREATE property values must be literals in v1",
            )),
        }
    }

    fn node_subject(&mut self, n: &NodePattern) -> TemplateTerm {
        if let Some(var) = &n.var {
            // Reuse a stable bnode for the same Cypher variable.
            return TemplateTerm::BlankNode(format!("_:cy_{}", var.name));
        }
        // Anonymous node — cache by span.
        let key = (n.span.start, n.span.end);
        if let Some(t) = self.node_subject_cache.get(&key) {
            return t.clone();
        }
        let label = format!("_:cy_anon_{}_{}", n.span.start, n.span.end);
        let t = TemplateTerm::BlankNode(label);
        self.node_subject_cache.insert(key, t.clone());
        t
    }

    fn fresh_bnode(&mut self) -> TemplateTerm {
        let label = format!("_:cy_ann_{}", self.bnode_counter);
        self.bnode_counter += 1;
        TemplateTerm::BlankNode(label)
    }

    fn resolve_iri(&self, name: &str) -> String {
        if let Some(iri) = self.overrides.get(name) {
            return iri.clone();
        }
        format!("{}{}", self.vocab, name)
    }

    fn resolve_predicate(&self, name: &str) -> Result<String, LowerCypherError> {
        let iri = self.resolve_iri(name);
        if reifies_iris::ALL.iter().any(|x| *x == iri) {
            return Err(LowerCypherError::ReservedPredicate(iri));
        }
        Ok(iri)
    }
}

fn require_node_anchored(node: &NodePattern) -> Result<(), LowerCypherError> {
    if node.labels.is_empty() && node.props.is_none() && node.var.is_none() {
        return Err(LowerCypherError::rejected(
            "bare `()` node in CREATE — every node needs a variable, a label, or a property",
        ));
    }
    Ok(())
}

fn lower_literal_value(lit: &Literal) -> Result<FlakeValue, LowerCypherError> {
    Ok(match lit {
        Literal::Integer(n, _) => FlakeValue::Long(*n),
        Literal::Float(f, _) => FlakeValue::Double(*f),
        Literal::String(s, _) => FlakeValue::String(s.clone()),
        Literal::Bool(b, _) => FlakeValue::Boolean(*b),
        Literal::Null(_) => {
            return Err(LowerCypherError::unsupported(
                "NULL literal in CREATE is rejected — omit the property instead",
            ));
        }
    })
}

// Silence unused-import warnings for symbols we'll wire later.
#[allow(dead_code)]
fn _retain_arc(_a: &Arc<str>) {}
