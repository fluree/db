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
//!   relationship properties producing an `f:reifies*` bundle). Pure
//!   pattern CREATE (no leading MATCH) only.
//! - **MATCH … SET / REMOVE** — node-anchored WHERE patterns (labels +
//!   inline property filters + directed single-typed relationships)
//!   lowered to `where_patterns`, with DELETE/INSERT templates that
//!   reference the bound variables (`TxnType::Update`). Covers
//!   `SET n.prop = lit`, `SET n += {…}`, `SET n:Label`,
//!   `REMOVE n.prop`, `REMOVE n:Label`.
//!
//! Still deferred (clear errors): `MATCH … CREATE`, DELETE / DETACH
//! DELETE, MERGE, `SET n = {…}` (bounded replace), WHERE-clause filter
//! expressions, named/untyped/alternation relationships in a write
//! MATCH, and parameter substitution.

use std::sync::Arc;

use fluree_db_core::{FlakeValue, Sid};
use fluree_db_query::parse::{
    LiteralValue, UnresolvedPattern, UnresolvedTerm, UnresolvedTriplePattern,
};
use fluree_db_query::VarRegistry;
use fluree_vocab::{rdf, reifies_iris};
use thiserror::Error;

use fluree_db_cypher::ast::{
    CreateClause, CypherAst, Direction, Expr, Label, Literal, MapLit, MatchClause, NodePattern,
    Pattern, PatternPart, ReadClause, RelPattern, RemoveClause, RemoveItem, SetClause, SetItem,
    Statement, Update, Variable, WriteClause,
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
    /// WHERE patterns lowered from leading MATCH / OPTIONAL MATCH
    /// clauses. Empty for pure CREATE.
    where_patterns: Vec<UnresolvedPattern>,
    delete_templates: Vec<TripleTemplate>,
    insert_templates: Vec<TripleTemplate>,
    opts: TxnOpts,
    /// Counter for fresh blank-node labels in the CREATE template.
    bnode_counter: u32,
    /// Counter for synthetic old-value variables in SET / REMOVE
    /// (`?#__cy_old_N` bound via OPTIONAL so retracts skip cleanly when
    /// the property is absent).
    synth_counter: u32,
    /// Variable names bound by a preceding MATCH, used to reject SET /
    /// REMOVE on an unbound target.
    bound_vars: std::collections::HashSet<String>,
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
            where_patterns: Vec::new(),
            delete_templates: Vec::new(),
            insert_templates: Vec::new(),
            opts,
            bnode_counter: 0,
            synth_counter: 0,
            bound_vars: std::collections::HashSet::new(),
            node_subject_cache: std::collections::HashMap::new(),
        }
    }

    fn finish(self) -> Txn {
        Txn {
            txn_type: self.txn_type,
            where_patterns: self.where_patterns,
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
        if update.return_clause.is_some() {
            return Err(LowerCypherError::unsupported(
                "RETURN on a write statement is deferred in v1",
            ));
        }

        // Lower any leading MATCH / OPTIONAL MATCH into where_patterns.
        // Their presence flips the transaction into Update mode (DELETE /
        // INSERT templates reference the bound variables).
        let has_match = !update.read_clauses.is_empty();
        if has_match {
            self.lower_read_clauses(&update.read_clauses)?;
            self.txn_type = TxnType::Update;
        }

        for clause in &update.write_clauses {
            match clause {
                WriteClause::Create(c) => {
                    if has_match {
                        return Err(LowerCypherError::unsupported(
                            "MATCH … CREATE (template-driven writes) is deferred — submit a pure CREATE",
                        ));
                    }
                    self.lower_create(c)?;
                }
                WriteClause::Set(s) => {
                    self.require_match(has_match, "SET")?;
                    self.lower_set(s)?;
                }
                WriteClause::Remove(r) => {
                    self.require_match(has_match, "REMOVE")?;
                    self.lower_remove(r)?;
                }
                WriteClause::Delete(_) => {
                    return Err(LowerCypherError::unsupported(
                        "DELETE / DETACH DELETE is deferred — cascade lowering lands in the next write slice",
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

    fn require_match(&self, has_match: bool, clause: &str) -> Result<(), LowerCypherError> {
        if !has_match {
            return Err(LowerCypherError::rejected(format!(
                "{clause} requires a preceding MATCH to bind its target variable"
            )));
        }
        Ok(())
    }

    // ---- MATCH → WHERE lowering (shared by SET / REMOVE) ----------------

    fn lower_read_clauses(&mut self, clauses: &[ReadClause]) -> Result<(), LowerCypherError> {
        for clause in clauses {
            match clause {
                ReadClause::Match(m) => {
                    let mut pats = Vec::new();
                    self.lower_match_pattern(m, &mut pats)?;
                    self.where_patterns.append(&mut pats);
                }
                ReadClause::OptionalMatch(m) => {
                    let mut pats = Vec::new();
                    self.lower_match_pattern(m, &mut pats)?;
                    self.where_patterns.push(UnresolvedPattern::Optional(pats));
                }
                ReadClause::With(_) => {
                    return Err(LowerCypherError::unsupported(
                        "WITH before a write clause is deferred",
                    ));
                }
                ReadClause::Unwind(_) => {
                    return Err(LowerCypherError::unsupported(
                        "UNWIND before a write clause is deferred",
                    ));
                }
            }
        }
        Ok(())
    }

    fn lower_match_pattern(
        &mut self,
        m: &MatchClause,
        out: &mut Vec<UnresolvedPattern>,
    ) -> Result<(), LowerCypherError> {
        if m.where_clause.is_some() {
            return Err(LowerCypherError::unsupported(
                "WHERE filter expressions in a write-statement MATCH are deferred — use inline property filters `(n:Label {key: val})`",
            ));
        }
        for part in &m.pattern.parts {
            self.lower_match_part(part, out)?;
        }
        Ok(())
    }

    fn lower_match_part(
        &mut self,
        part: &PatternPart,
        out: &mut Vec<UnresolvedPattern>,
    ) -> Result<(), LowerCypherError> {
        if part.tail.is_empty() {
            require_node_match_anchored(&part.head)?;
        }
        self.lower_node_match(&part.head, out)?;

        let mut prev_node = &part.head;
        for (rel, next) in &part.tail {
            self.lower_node_match(next, out)?;
            self.lower_rel_match(prev_node, rel, next, out)?;
            prev_node = next;
        }
        Ok(())
    }

    fn lower_node_match(
        &mut self,
        n: &NodePattern,
        out: &mut Vec<UnresolvedPattern>,
    ) -> Result<(), LowerCypherError> {
        let subj = self.node_match_term(n);
        if let Some(v) = &n.var {
            self.bound_vars.insert(v.name.clone());
        }

        // Labels — `?n rdf:type <label>`.
        for Label { name, .. } in &n.labels {
            let label_iri = self.resolve_iri(name);
            out.push(UnresolvedPattern::Triple(UnresolvedTriplePattern {
                s: subj.clone(),
                p: UnresolvedTerm::Iri(Arc::from(rdf::TYPE)),
                o: UnresolvedTerm::Iri(Arc::from(label_iri.as_str())),
                dtc: None,
            }));
        }

        // Inline property filters — `?n <prop> value`.
        if let Some(props) = &n.props {
            for (key, val_expr) in &props.entries {
                let pred_iri = self.resolve_predicate(key)?;
                let obj = self.match_object_term(val_expr)?;
                out.push(UnresolvedPattern::Triple(UnresolvedTriplePattern {
                    s: subj.clone(),
                    p: UnresolvedTerm::Iri(Arc::from(pred_iri.as_str())),
                    o: obj,
                    dtc: None,
                }));
            }
        }
        Ok(())
    }

    fn lower_rel_match(
        &mut self,
        left: &NodePattern,
        rel: &RelPattern,
        right: &NodePattern,
        out: &mut Vec<UnresolvedPattern>,
    ) -> Result<(), LowerCypherError> {
        if matches!(rel.direction, Direction::Either) {
            return Err(LowerCypherError::rejected(
                "undirected relationship `-[r]-` in a write MATCH — write `-[:T]->` or `<-[:T]-`",
            ));
        }
        if rel.length.is_some() {
            return Err(LowerCypherError::rejected(
                "variable-length paths in a write MATCH are not allowed",
            ));
        }
        if rel.var.is_some() || rel.props.is_some() {
            return Err(LowerCypherError::unsupported(
                "relationship-bound variables / property filters in a write MATCH are deferred — match the connecting nodes instead",
            ));
        }
        if rel.types.len() != 1 {
            return Err(LowerCypherError::unsupported(
                "write MATCH relationships need exactly one type (`-[:T]->`); untyped and alternation forms are deferred",
            ));
        }
        let type_iri = self.resolve_predicate(&rel.types[0].name)?;
        let left_t = self.node_match_term(left);
        let right_t = self.node_match_term(right);
        let (s, o) = match rel.direction {
            Direction::Outgoing => (left_t, right_t),
            Direction::Incoming => (right_t, left_t),
            Direction::Either => unreachable!(),
        };
        out.push(UnresolvedPattern::Triple(UnresolvedTriplePattern {
            s,
            p: UnresolvedTerm::Iri(Arc::from(type_iri.as_str())),
            o,
            dtc: None,
        }));
        Ok(())
    }

    /// The WHERE-side term for a node: its named variable, or a stable
    /// span-keyed synthetic for an anonymous node.
    fn node_match_term(&self, n: &NodePattern) -> UnresolvedTerm {
        match &n.var {
            Some(v) => UnresolvedTerm::Var(Arc::from(var_name(&v.name).as_str())),
            None => UnresolvedTerm::Var(Arc::from(
                format!("?#__cy_anon_{}_{}", n.span.start, n.span.end).as_str(),
            )),
        }
    }

    fn match_object_term(&self, e: &Expr) -> Result<UnresolvedTerm, LowerCypherError> {
        match e {
            Expr::Lit(lit) => Ok(UnresolvedTerm::Literal(lower_literal_unresolved(lit)?)),
            Expr::Var(v) => Ok(UnresolvedTerm::Var(Arc::from(var_name(&v.name).as_str()))),
            _ => Err(LowerCypherError::unsupported(
                "inline MATCH property values must be literals or variables in v1",
            )),
        }
    }

    // ---- SET / REMOVE ---------------------------------------------------

    fn lower_set(&mut self, s: &SetClause) -> Result<(), LowerCypherError> {
        for item in &s.items {
            match item {
                SetItem::Property {
                    target,
                    property,
                    value,
                } => {
                    self.require_bound(target)?;
                    self.set_property(&target.name, property, value)?;
                }
                SetItem::MapMerge { target, map } => {
                    self.require_bound(target)?;
                    for (key, val_expr) in &map.entries {
                        self.set_property(&target.name, key, val_expr)?;
                    }
                }
                SetItem::MapReplace { .. } => {
                    return Err(LowerCypherError::unsupported(
                        "SET n = {…} (replace all data properties) is deferred — its bounded retract scope needs a predicate-variable scan; use `SET n += {…}` or explicit per-property SET",
                    ));
                }
                SetItem::Labels { target, labels } => {
                    self.require_bound(target)?;
                    let subj = self.var_term(&target.name);
                    let rdf_type_sid = self.ns.sid_for_iri(rdf::TYPE);
                    for label in labels {
                        let iri = self.resolve_iri(label);
                        let label_sid = self.ns.sid_for_iri(&iri);
                        self.insert_templates.push(TripleTemplate::new(
                            subj.clone(),
                            TemplateTerm::Sid(rdf_type_sid.clone()),
                            TemplateTerm::Sid(label_sid),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// `SET n.prop = value` — retract any existing value (bound via an
    /// OPTIONAL so it skips when absent) and assert the new one.
    fn set_property(
        &mut self,
        target: &str,
        property: &str,
        value: &Expr,
    ) -> Result<(), LowerCypherError> {
        let pred_iri = self.resolve_predicate(property)?;
        let pred_sid = self.ns.sid_for_iri(&pred_iri);
        let obj = self.expr_to_object(value)?;
        let subj = self.var_term(target);

        self.push_optional_old_value(target, &pred_iri, &pred_sid);
        self.insert_templates.push(TripleTemplate::new(
            subj,
            TemplateTerm::Sid(pred_sid),
            obj,
        ));
        Ok(())
    }

    fn lower_remove(&mut self, r: &RemoveClause) -> Result<(), LowerCypherError> {
        for item in &r.items {
            match item {
                RemoveItem::Property { target, property } => {
                    self.require_bound(target)?;
                    let pred_iri = self.resolve_predicate(property)?;
                    let pred_sid = self.ns.sid_for_iri(&pred_iri);
                    self.push_optional_old_value(&target.name, &pred_iri, &pred_sid);
                }
                RemoveItem::Labels { target, labels } => {
                    self.require_bound(target)?;
                    let subj = self.var_term(&target.name);
                    let rdf_type_sid = self.ns.sid_for_iri(rdf::TYPE);
                    for label in labels {
                        let iri = self.resolve_iri(label);
                        let label_sid = self.ns.sid_for_iri(&iri);
                        self.delete_templates.push(TripleTemplate::new(
                            subj.clone(),
                            TemplateTerm::Sid(rdf_type_sid.clone()),
                            TemplateTerm::Sid(label_sid),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Emit `OPTIONAL { ?target <pred> ?old }` plus a delete template for
    /// `(?target, <pred>, ?old)`. Shared by SET (replace) and REMOVE.
    fn push_optional_old_value(&mut self, target: &str, pred_iri: &str, pred_sid: &Sid) {
        let old_name = format!("?#__cy_old_{}", self.synth_counter);
        self.synth_counter += 1;
        let old_vid = self.vars.get_or_insert(&old_name);

        self.where_patterns
            .push(UnresolvedPattern::Optional(vec![UnresolvedPattern::Triple(
                UnresolvedTriplePattern {
                    s: UnresolvedTerm::Var(Arc::from(var_name(target).as_str())),
                    p: UnresolvedTerm::Iri(Arc::from(pred_iri)),
                    o: UnresolvedTerm::Var(Arc::from(old_name.as_str())),
                    dtc: None,
                },
            )]));

        let subj = self.var_term(target);
        self.delete_templates.push(TripleTemplate::new(
            subj,
            TemplateTerm::Sid(pred_sid.clone()),
            TemplateTerm::Var(old_vid),
        ));
    }

    fn require_bound(&self, target: &Variable) -> Result<(), LowerCypherError> {
        if !self.bound_vars.contains(&target.name) {
            return Err(LowerCypherError::rejected(format!(
                "variable `{}` is not bound by a preceding MATCH",
                target.name
            )));
        }
        Ok(())
    }

    /// `TemplateTerm::Var` for a named Cypher variable, interned under the
    /// same `?name` key the WHERE side uses.
    fn var_term(&mut self, name: &str) -> TemplateTerm {
        TemplateTerm::Var(self.vars.get_or_insert(&var_name(name)))
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

/// A standalone node in a write-statement MATCH must carry a label or a
/// property filter — a bare `(n)` is a whole-graph scan and is rejected,
/// matching the read-path node-existence model.
fn require_node_match_anchored(node: &NodePattern) -> Result<(), LowerCypherError> {
    if node.labels.is_empty() && node.props.is_none() {
        let name = node.var.as_ref().map(|v| v.name.as_str()).unwrap_or("");
        return Err(LowerCypherError::rejected(format!(
            "bare node `({name})` in MATCH — add a label or property filter (`(n:Label)` or `(n {{key: val}})`)"
        )));
    }
    Ok(())
}

/// Variable name as interned in the shared `VarRegistry` (leading `?`),
/// keeping the WHERE side and the DELETE/INSERT templates on the same id.
fn var_name(name: &str) -> String {
    format!("?{name}")
}

fn lower_literal_unresolved(lit: &Literal) -> Result<LiteralValue, LowerCypherError> {
    Ok(match lit {
        Literal::Integer(n, _) => LiteralValue::Long(*n),
        Literal::Float(f, _) => LiteralValue::Double(*f),
        Literal::String(s, _) => LiteralValue::String(Arc::from(s.as_str())),
        Literal::Bool(b, _) => LiteralValue::Boolean(*b),
        Literal::Null(_) => {
            return Err(LowerCypherError::unsupported(
                "NULL literal in a MATCH property filter is rejected",
            ));
        }
    })
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
