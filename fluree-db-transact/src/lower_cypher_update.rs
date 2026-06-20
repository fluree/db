//! Cypher write statement → Transaction IR lowering.
//!
//! Maps Cypher's `CREATE / SET / REMOVE / DELETE / DETACH DELETE /
//! MERGE` to the same `Txn` shape that `lower_sparql_update` and the
//! JSON-LD transactor produce. The shared staging pipeline handles
//! flake generation, cascade, policy, and firewalls — Cypher-side
//! work is purely about producing the right `TripleTemplate` bundle.
//!
//! See `docs/concepts/cypher.md` for the supported write surface.
//!
//! ## v1 scope
//!
//! - **CREATE** (nodes + directed typed relationships, with optional
//!   relationship properties producing an `f:reifies*` bundle).
//! - **MATCH … CREATE / SET / REMOVE** — node-anchored WHERE patterns
//!   (labels + inline property filters + directed single-typed relationships
//!   plus scalar `WHERE` filters) lowered to `where_patterns`, with DELETE/INSERT
//!   templates that reference the bound variables (`TxnType::Update`).
//!   Covers template-driven CREATE, `SET n.prop = lit`, `SET n.prop = null`
//!   (removes the property), `SET n += {…}`, `SET n = {…}`, `SET n:Label`,
//!   `REMOVE n.prop`, and `REMOVE n:Label`.
//!
//! Parameter (`$param`) substitution happens upstream in
//! `fluree_db_cypher::substitute_params` before this lowering runs, so the
//! AST reaching here carries only concrete literals.
//!
//! Still deferred (clear errors): CASE/EXISTS in write-side WHERE, and
//! named/untyped/alternation relationships in a write MATCH.

use std::sync::Arc;

use fluree_db_core::{FlakeValue, Sid};
use fluree_db_query::parse::{
    LiteralValue, UnresolvedExpression, UnresolvedFilterValue, UnresolvedPattern, UnresolvedTerm,
    UnresolvedTriplePattern, UnresolvedValue,
};
use fluree_db_query::VarRegistry;
use fluree_vocab::{rdf, reifies_iris};
use thiserror::Error;

use fluree_db_cypher::ast::{
    CreateClause, CypherAst, DeleteClause, Direction, Expr, Label, Literal, MapLit, MatchClause,
    MergeClause, NodePattern, Pattern, PatternPart, ReadClause, RelPattern, RemoveClause,
    RemoveItem, SetClause, SetItem, Statement, Update, Variable, WithClause, WriteClause,
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
    /// For each named relationship variable bound in the MATCH, the base edge
    /// it reifies: `(subject var-name, predicate Sid, object var-name)`. Lets
    /// `DELETE r` retract the base edge (the `f:reifies*` cascade then removes
    /// the bundle).
    rel_var_edges: std::collections::HashMap<String, (String, Sid, String)>,
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
            rel_var_edges: std::collections::HashMap::new(),
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

        // A MERGE must be the only write. Each MERGE appends a top-level NOT
        // EXISTS guard evaluated against the pre-transaction snapshot, so
        // combining it with another write (or a second MERGE) would make the
        // guards conjunctive and blind to earlier writes in the same statement.
        // A *leading MATCH* is the one exception: a relationship MERGE whose
        // endpoints are bound by the MATCH (`MATCH (a),(b) MERGE (a)-[:T]->(b)`)
        // is a per-row find-or-create — the NOT EXISTS guard runs once per bound
        // row against the pre-write snapshot, exactly like SPARQL
        // `INSERT … WHERE { … FILTER NOT EXISTS { … } }`.
        let merges: Vec<&MergeClause> = update
            .write_clauses
            .iter()
            .filter_map(|w| match w {
                WriteClause::Merge(m) => Some(m),
                _ => None,
            })
            .collect();
        let is_relationship_merge =
            |m: &MergeClause| m.pattern.parts.len() == 1 && m.pattern.parts[0].tail.len() == 1;
        if !merges.is_empty() {
            if update.write_clauses.len() > 1 {
                return Err(LowerCypherError::unsupported(
                    "MERGE must be the only write clause in v1 — combining MERGE with another \
                     write or a second MERGE needs sequential snapshot evaluation that \
                     single-Txn staging can't provide",
                ));
            }
            if !update.read_clauses.is_empty() && !is_relationship_merge(merges[0]) {
                return Err(LowerCypherError::unsupported(
                    "a leading MATCH is only allowed before a relationship MERGE \
                     (`MATCH (a),(b) MERGE (a)-[:T]->(b)`) in v1 — a node MERGE must stand alone",
                ));
            }
        }

        // OPTIONAL MATCH before a reifier-bundle write (CREATE, or a
        // relationship MERGE) is unsafe: the bundle is multi-triple, and a
        // template referencing an optionally-unbound variable skips per-triple —
        // which could assert only part of the bundle (a malformed reifier).
        // Require mandatory binding for these.
        let has_optional = update
            .read_clauses
            .iter()
            .any(|c| matches!(c, ReadClause::OptionalMatch(_)));
        let has_create = update
            .write_clauses
            .iter()
            .any(|w| matches!(w, WriteClause::Create(_)));
        let has_relationship_merge = merges.iter().any(|m| is_relationship_merge(m));
        if has_optional && (has_create || has_relationship_merge) {
            return Err(LowerCypherError::rejected(
                "OPTIONAL MATCH before CREATE / a relationship MERGE is not supported — a \
                 template referencing an optionally-unbound variable could assert a partial \
                 reifier bundle; bind its variables with a mandatory MATCH",
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
                WriteClause::Create(c) => self.lower_create(c)?,
                WriteClause::Set(s) => {
                    self.require_match(has_match, "SET")?;
                    self.lower_set(s)?;
                }
                WriteClause::Remove(r) => {
                    self.require_match(has_match, "REMOVE")?;
                    self.lower_remove(r)?;
                }
                WriteClause::Delete(d) => {
                    self.require_match(has_match, "DELETE")?;
                    self.lower_delete(d)?;
                }
                // MERGE-standalone is guaranteed by the merge-count guard above.
                WriteClause::Merge(m) => self.lower_merge(m)?,
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
                ReadClause::With(w) => {
                    self.lower_with_clause(w)?;
                }
                ReadClause::Unwind(_) => {
                    return Err(LowerCypherError::unsupported(
                        "UNWIND before a write clause is deferred — supply the rows as a \
                         `$param` list of maps so they desugar to a VALUES join",
                    ));
                }
                // Desugared constant rows (the `UNWIND $listOfMaps` → VALUES
                // rewrite). Bind each column as a VALUES block; the MATCH and
                // CREATE/SET templates then fire once per row.
                ReadClause::InlineRows { vars, rows } => {
                    let value_vars: Vec<Arc<str>> = vars
                        .iter()
                        .map(|v| Arc::from(var_name(&v.name).as_str()))
                        .collect();
                    let mut value_rows = Vec::with_capacity(rows.len());
                    for row in rows {
                        let mut cells = Vec::with_capacity(row.len());
                        for cell in row {
                            cells.push(inline_cell_to_value(cell)?);
                        }
                        value_rows.push(cells);
                    }
                    self.where_patterns.push(UnresolvedPattern::Values {
                        vars: value_vars,
                        rows: value_rows,
                    });
                }
            }
        }
        Ok(())
    }

    /// Lower a `WITH` projection that precedes a write clause. This is the
    /// **horizon subset** that maps cleanly onto the where-pattern stream:
    /// pass-through variables, renames, and computed (non-aggregate) aliases
    /// (each a `Bind`), plus an optional `WHERE` filter. After the projection
    /// the in-scope variables are narrowed to the WITH list — exactly Cypher's
    /// scoping rule — so a later write can only reference projected names (a
    /// dropped node referenced in a write `MATCH`-style position becomes a fresh
    /// node, and a dropped target of SET/REMOVE/DELETE is rejected as unbound).
    ///
    /// Deferred (clear errors): aggregation (rejected via the computed-projection
    /// path, since aggregate calls aren't in the filter-expression surface),
    /// `DISTINCT`, and `ORDER BY` / `SKIP` / `LIMIT` — these need a query-level
    /// grouping or slice the single-Txn write model doesn't carry.
    fn lower_with_clause(&mut self, w: &WithClause) -> Result<(), LowerCypherError> {
        if w.distinct {
            return Err(LowerCypherError::unsupported(
                "WITH DISTINCT before a write clause is deferred",
            ));
        }
        if !w.order_by.is_empty() || w.skip.is_some() || w.limit.is_some() {
            return Err(LowerCypherError::unsupported(
                "ORDER BY / SKIP / LIMIT on a WITH before a write clause is deferred",
            ));
        }

        // Build the new in-scope set (the WITH horizon) and emit a Bind for each
        // rename / computed alias. Pass-through variables must already be bound.
        let mut horizon: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut binds: Vec<UnresolvedPattern> = Vec::new();
        for item in &w.items {
            let alias = item.alias.as_ref().map(|a| a.name.clone());
            match &item.expr {
                Expr::Var(v) => {
                    self.require_bound(v)?;
                    match alias {
                        // Pass-through (`WITH a` / `WITH a AS a`).
                        None => {
                            horizon.insert(v.name.clone());
                        }
                        Some(a) if a == v.name => {
                            horizon.insert(v.name.clone());
                        }
                        // Rename (`WITH a AS b`).
                        Some(a) => {
                            binds.push(UnresolvedPattern::Bind {
                                var: Arc::from(var_name(&a).as_str()),
                                expr: UnresolvedExpression::var(var_name(&v.name)),
                            });
                            horizon.insert(a);
                        }
                    }
                }
                // Computed projection (`WITH a.age + 1 AS next`) — needs an alias
                // and must not contain an aggregate (lower_filter_expr rejects
                // aggregate calls). Property-accessor scans land in `aux`.
                other => {
                    let Some(a) = alias else {
                        return Err(LowerCypherError::rejected(
                            "a non-variable WITH expression before a write needs an alias \
                             (`<expr> AS name`)",
                        ));
                    };
                    let mut aux = Vec::new();
                    let expr = self.lower_filter_expr(other, &mut aux)?;
                    self.where_patterns.append(&mut aux);
                    binds.push(UnresolvedPattern::Bind {
                        var: Arc::from(var_name(&a).as_str()),
                        expr,
                    });
                    horizon.insert(a);
                }
            }
        }
        self.where_patterns.append(&mut binds);

        // Narrow lowering scope to the horizon. The execution stream keeps every
        // matched binding (the Bind/Filter patterns still see them); this only
        // gates which names a later write clause may reference.
        self.bound_vars = horizon;

        // Optional post-projection (HAVING-style) filter over the horizon.
        if let Some(where_expr) = &w.where_clause {
            let mut aux = Vec::new();
            let filter = self.lower_filter_expr(where_expr, &mut aux)?;
            self.where_patterns.append(&mut aux);
            self.where_patterns.push(UnresolvedPattern::Filter(filter));
        }
        Ok(())
    }

    fn lower_match_pattern(
        &mut self,
        m: &MatchClause,
        out: &mut Vec<UnresolvedPattern>,
    ) -> Result<(), LowerCypherError> {
        for part in &m.pattern.parts {
            self.lower_match_part(part, out)?;
        }
        if let Some(where_expr) = &m.where_clause {
            self.lower_match_where(where_expr, out)?;
        }
        Ok(())
    }

    fn lower_match_where(
        &mut self,
        where_expr: &Expr,
        out: &mut Vec<UnresolvedPattern>,
    ) -> Result<(), LowerCypherError> {
        let mut aux = Vec::new();
        let filter = self.lower_filter_expr(where_expr, &mut aux)?;
        out.extend(aux);
        out.push(UnresolvedPattern::Filter(filter));
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
        if rel.props.is_some() {
            return Err(LowerCypherError::unsupported(
                "relationship property filters in a write MATCH are deferred — match the connecting nodes instead",
            ));
        }
        if rel.types.len() != 1 {
            return Err(LowerCypherError::unsupported(
                "write MATCH relationships need exactly one type (`-[:T]->`); untyped and alternation forms are deferred",
            ));
        }
        let type_iri = self.resolve_predicate(&rel.types[0].name)?;
        let (left_name, right_name) = (self.node_var_name(left), self.node_var_name(right));
        let (s_name, o_name) = match rel.direction {
            Direction::Outgoing => (left_name, right_name),
            Direction::Incoming => (right_name, left_name),
            Direction::Either => unreachable!(),
        };
        let edge = UnresolvedTriplePattern {
            s: UnresolvedTerm::Var(Arc::from(s_name.as_str())),
            p: UnresolvedTerm::Iri(Arc::from(type_iri.as_str())),
            o: UnresolvedTerm::Var(Arc::from(o_name.as_str())),
            dtc: None,
        };

        match &rel.var {
            // Anonymous relationship → plain base-edge triple (set semantics).
            None => out.push(UnresolvedPattern::Triple(edge)),
            // Named relationship → bind `r` to the annotation SID via an
            // EdgeAnnotation pattern (only matches reifier-bundled edges, which
            // is every LPG/Cypher-written relationship). This makes `SET r.prop`
            // / `REMOVE r.prop` target the relationship's annotation metadata,
            // and `DELETE r` retract the base edge it reifies.
            Some(v) => {
                // A relationship variable may bind only one edge in a MATCH;
                // reusing it would make the probe (first occurrence) and the
                // delete lowering (last occurrence) disagree.
                if self.rel_var_edges.contains_key(&v.name) {
                    return Err(LowerCypherError::rejected(format!(
                        "relationship variable `{}` is bound more than once in a write MATCH; \
                         use a distinct name for each relationship",
                        v.name
                    )));
                }
                self.bound_vars.insert(v.name.clone());
                let p_sid = self.ns.sid_for_iri(&type_iri);
                self.rel_var_edges
                    .insert(v.name.clone(), (s_name, p_sid, o_name));
                out.push(UnresolvedPattern::EdgeAnnotation {
                    edge,
                    annotation: UnresolvedTerm::Var(Arc::from(var_name(&v.name).as_str())),
                    body: Vec::new(),
                });
            }
        }
        Ok(())
    }

    /// The interned variable name for a node: its named variable (`?name`), or
    /// a stable span-keyed synthetic for an anonymous node.
    fn node_var_name(&self, n: &NodePattern) -> String {
        match &n.var {
            Some(v) => var_name(&v.name),
            None => format!("?#__cy_anon_{}_{}", n.span.start, n.span.end),
        }
    }

    /// The WHERE-side term for a node.
    fn node_match_term(&self, n: &NodePattern) -> UnresolvedTerm {
        UnresolvedTerm::Var(Arc::from(self.node_var_name(n).as_str()))
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

    fn lower_filter_expr(
        &mut self,
        e: &Expr,
        aux: &mut Vec<UnresolvedPattern>,
    ) -> Result<UnresolvedExpression, LowerCypherError> {
        match e {
            Expr::Var(v) => Ok(UnresolvedExpression::var(var_name(&v.name))),
            Expr::Lit(lit) => lower_filter_literal(lit),
            Expr::Param(_) => Err(LowerCypherError::unsupported(
                "internal: Cypher params should be substituted before write MATCH WHERE lowering",
            )),
            Expr::Prop(target, key, _) => {
                let prop_var = self.resolve_filter_property_accessor(target, key, aux)?;
                Ok(UnresolvedExpression::var(prop_var))
            }
            Expr::BinOp(op, l, r, _) => {
                let l = self.lower_filter_expr(l, aux)?;
                let r = self.lower_filter_expr(r, aux)?;
                let func = match op {
                    fluree_db_cypher::ast::BinOp::Eq => "=",
                    fluree_db_cypher::ast::BinOp::Ne => "!=",
                    fluree_db_cypher::ast::BinOp::Lt => "<",
                    fluree_db_cypher::ast::BinOp::Le => "<=",
                    fluree_db_cypher::ast::BinOp::Gt => ">",
                    fluree_db_cypher::ast::BinOp::Ge => ">=",
                    fluree_db_cypher::ast::BinOp::Add => "+",
                    fluree_db_cypher::ast::BinOp::Sub => "-",
                    fluree_db_cypher::ast::BinOp::Mul => "*",
                    fluree_db_cypher::ast::BinOp::Div => "/",
                    fluree_db_cypher::ast::BinOp::Mod => "%",
                    fluree_db_cypher::ast::BinOp::And => {
                        return Ok(UnresolvedExpression::And(vec![l, r]));
                    }
                    fluree_db_cypher::ast::BinOp::Or => {
                        return Ok(UnresolvedExpression::Or(vec![l, r]));
                    }
                };
                Ok(unresolved_call(func, vec![l, r]))
            }
            Expr::UnaryOp(op, inner, _) => {
                let inner = self.lower_filter_expr(inner, aux)?;
                match op {
                    fluree_db_cypher::ast::UnaryOp::Not => {
                        Ok(UnresolvedExpression::Not(Box::new(inner)))
                    }
                    fluree_db_cypher::ast::UnaryOp::Neg => {
                        Ok(unresolved_call("negate", vec![inner]))
                    }
                }
            }
            Expr::In(left, list, _) => {
                let expr = self.lower_filter_expr(left, aux)?;
                let Expr::List(items, _) = list.as_ref() else {
                    return Err(LowerCypherError::unsupported(
                        "`IN` right-hand side in write MATCH WHERE must be an inline list",
                    ));
                };
                let values = items
                    .iter()
                    .map(|item| self.lower_filter_expr(item, aux))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(UnresolvedExpression::In {
                    expr: Box::new(expr),
                    values,
                    negated: false,
                })
            }
            Expr::IsNull(inner, _) => {
                let inner = self.lower_filter_expr(inner, aux)?;
                Ok(UnresolvedExpression::Not(Box::new(unresolved_call(
                    "bound",
                    vec![inner],
                ))))
            }
            Expr::IsNotNull(inner, _) => {
                let inner = self.lower_filter_expr(inner, aux)?;
                Ok(unresolved_call("bound", vec![inner]))
            }
            Expr::StartsWith(l, r, _) => {
                let l = self.lower_filter_expr(l, aux)?;
                let r = self.lower_filter_expr(r, aux)?;
                Ok(unresolved_call("strstarts", vec![l, r]))
            }
            Expr::EndsWith(l, r, _) => {
                let l = self.lower_filter_expr(l, aux)?;
                let r = self.lower_filter_expr(r, aux)?;
                Ok(unresolved_call("strends", vec![l, r]))
            }
            Expr::Contains(l, r, _) => {
                let l = self.lower_filter_expr(l, aux)?;
                let r = self.lower_filter_expr(r, aux)?;
                Ok(unresolved_call("contains", vec![l, r]))
            }
            Expr::List(items, _) => {
                let args = items
                    .iter()
                    .map(|item| self.lower_filter_expr(item, aux))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(unresolved_call("list", args))
            }
            Expr::Index(list, index, _) => {
                let list = self.lower_filter_expr(list, aux)?;
                let index = self.lower_filter_expr(index, aux)?;
                Ok(unresolved_call("nth", vec![list, index]))
            }
            Expr::Call(call) => {
                let args = call
                    .args
                    .iter()
                    .map(|arg| self.lower_filter_expr(arg, aux))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let func = match call.name.to_ascii_lowercase().as_str() {
                    "coalesce" => "coalesce",
                    "abs" => "abs",
                    "size" => "size",
                    "head" => "head",
                    "last" => "last",
                    "tail" => "tail",
                    "reverse" => "reverse",
                    "tostring" => "str",
                    "tointeger" => "xsd:integer",
                    "tofloat" => "xsd:double",
                    "range" => "range",
                    other => {
                        return Err(LowerCypherError::unsupported(format!(
                            "function `{other}` in write MATCH WHERE is not supported"
                        )));
                    }
                };
                Ok(unresolved_call(func, args))
            }
            Expr::Map(_, _) => Err(LowerCypherError::unsupported(
                "map literals in a write MATCH WHERE are not supported",
            )),
            Expr::Case(_) | Expr::Exists(_, _, _) => Err(LowerCypherError::unsupported(
                "CASE and EXISTS in write MATCH WHERE are deferred",
            )),
        }
    }

    fn resolve_filter_property_accessor(
        &mut self,
        target: &Expr,
        key: &str,
        aux: &mut Vec<UnresolvedPattern>,
    ) -> Result<String, LowerCypherError> {
        let target = match target {
            Expr::Var(v) => v,
            Expr::Prop(_, _, _) => {
                return Err(LowerCypherError::unsupported(
                    "chained property accessors in write MATCH WHERE are deferred",
                ));
            }
            _ => {
                return Err(LowerCypherError::unsupported(
                    "property accessors in write MATCH WHERE require a bare variable target",
                ));
            }
        };
        self.require_bound(target)?;

        let pred_iri = self.resolve_predicate(key)?;
        let prop_var = format!("?#__cy_where_prop_{}_{}", target.name, key);
        aux.push(UnresolvedPattern::Optional(vec![
            UnresolvedPattern::Triple(UnresolvedTriplePattern {
                s: self.unresolved_named_var(&target.name),
                p: UnresolvedTerm::Iri(Arc::from(pred_iri.as_str())),
                o: UnresolvedTerm::Var(Arc::from(prop_var.as_str())),
                dtc: None,
            }),
        ]));
        Ok(prop_var)
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
                SetItem::MapReplace { target, map } => {
                    self.require_bound(target)?;
                    self.replace_property_map(&target.name, map)?;
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

        // SET replaces: always retract the existing value(s). A multi-valued
        // predicate produces one OPTIONAL solution per old value, so the delete
        // template clears all of them. `SET n.prop = null` (and `= []`) then
        // asserts nothing — the property is removed. A list value asserts one
        // triple per element (the new multi-valued set); re-asserting a kept
        // value across the per-solution firings is idempotent.
        let objs = self.expr_to_object_terms(value)?;
        let subj = self.var_term(target);
        self.push_optional_old_value(target, &pred_iri, &pred_sid);
        for obj in objs {
            self.insert_templates.push(TripleTemplate::new(
                subj.clone(),
                TemplateTerm::Sid(pred_sid.clone()),
                obj,
            ));
        }
        Ok(())
    }

    /// `SET n = { ... }` — replace all scalar node properties visible to Cypher.
    /// Labels (`rdf:type`), relationship edges (ref-valued objects), and
    /// `f:reifies*` sidecar facts are not node properties and are preserved.
    fn replace_property_map(&mut self, target: &str, map: &MapLit) -> Result<(), LowerCypherError> {
        self.push_optional_old_data_properties(target);
        for (key, val_expr) in &map.entries {
            let pred_iri = self.resolve_predicate(key)?;
            let pred_sid = self.ns.sid_for_iri(&pred_iri);
            let subj = self.var_term(target);
            for obj in self.expr_to_object_terms(val_expr)? {
                self.insert_templates.push(TripleTemplate::new(
                    subj.clone(),
                    TemplateTerm::Sid(pred_sid.clone()),
                    obj,
                ));
            }
        }
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

    // ---- DELETE / DETACH DELETE -----------------------------------------

    fn lower_delete(&mut self, d: &DeleteClause) -> Result<(), LowerCypherError> {
        // Cypher relationship deletes operate on annotation identity, which
        // requires LPG-lifecycle cleanup so a base-edge retract also clears
        // the annotation body metadata (not just the `f:reifies*` bundle).
        self.opts.lpg_edge_lifecycle = Some(true);

        for target in &d.targets {
            // `DELETE r` — retract the base edge the relationship reifies; the
            // cascade then removes the `f:reifies*` bundle. Parallel-edge safety
            // (a shared `(s,p,o)` carrying multiple annotation SIDs) is enforced
            // by the API-level conditional probe before this lowering runs.
            if let Some((s_name, p_sid, o_name)) = self.rel_var_edges.get(&target.name).cloned() {
                let s = TemplateTerm::Var(self.vars.get_or_insert(&s_name));
                let o = TemplateTerm::Var(self.vars.get_or_insert(&o_name));
                self.delete_templates
                    .push(TripleTemplate::new(s, TemplateTerm::Sid(p_sid), o));
                continue;
            }

            self.require_bound(target)?;
            if d.detach {
                self.lower_detach_delete(&target.name);
            } else {
                // Cypher requires bare `DELETE n` to fail when `n` still has
                // relationships. That guard needs a snapshot probe (no Txn can
                // conditionally error), so it is handled by the API-level
                // conditional-write path before this lowering runs.
                return Err(LowerCypherError::unsupported(
                    "bare DELETE n is resolved by the API-level conditional-write path \
                     (it must error when the node still has relationships, which needs a \
                     snapshot probe). Use DETACH DELETE n to remove a node together with \
                     its relationships.",
                ));
            }
        }
        Ok(())
    }

    /// `DETACH DELETE n`: retract every triple touching `n` in both
    /// directions. The reifier-bundle cascade fires automatically as the base
    /// edges are retracted; the var-predicate scans exclude `f:reifies*`
    /// (write-WHERE runs with `include_system_facts = false`), so the inbound
    /// scan never tries to delete a reserved predicate directly.
    fn lower_detach_delete(&mut self, target: &str) {
        let n_unres = self.unresolved_named_var(target);
        let n_term = self.var_term(target);

        // Outbound: OPTIONAL { ?n ?p ?o } → delete (?n, ?p, ?o).
        let (p_unres, p_term) = self.fresh_scan_var();
        let (o_unres, o_term) = self.fresh_scan_var();
        self.where_patterns.push(UnresolvedPattern::Optional(vec![
            UnresolvedPattern::Triple(UnresolvedTriplePattern {
                s: n_unres.clone(),
                p: p_unres,
                o: o_unres,
                dtc: None,
            }),
        ]));
        self.delete_templates
            .push(TripleTemplate::new(n_term.clone(), p_term, o_term));

        // Inbound: OPTIONAL { ?s ?p2 ?n } → delete (?s, ?p2, ?n).
        let (s_unres, s_term) = self.fresh_scan_var();
        let (p2_unres, p2_term) = self.fresh_scan_var();
        self.where_patterns.push(UnresolvedPattern::Optional(vec![
            UnresolvedPattern::Triple(UnresolvedTriplePattern {
                s: s_unres,
                p: p2_unres,
                o: n_unres,
                dtc: None,
            }),
        ]));
        self.delete_templates
            .push(TripleTemplate::new(s_term, p2_term, n_term));
    }

    /// A WHERE-side `UnresolvedTerm::Var` for a named Cypher variable, keyed
    /// the same way the templates intern it.
    fn unresolved_named_var(&self, name: &str) -> UnresolvedTerm {
        UnresolvedTerm::Var(Arc::from(var_name(name).as_str()))
    }

    /// Mint a fresh synthetic scan variable, returning the matching WHERE-side
    /// and template-side terms (same interned id).
    fn fresh_scan_var(&mut self) -> (UnresolvedTerm, TemplateTerm) {
        let name = format!("?#__cy_scan_{}", self.synth_counter);
        self.synth_counter += 1;
        let vid = self.vars.get_or_insert(&name);
        (
            UnresolvedTerm::Var(Arc::from(name.as_str())),
            TemplateTerm::Var(vid),
        )
    }

    // ---- MERGE ----------------------------------------------------------

    /// Single-node MERGE (find-or-create). The identifying pattern (labels +
    /// inline props) becomes a `NOT EXISTS` guard: when no match exists the
    /// guard yields one empty solution and the create templates fire once
    /// against a fresh node (folding in `ON CREATE SET`); when a match exists
    /// the guard yields zero rows and nothing is inserted. The zero-row =
    /// no-op behavior relies on the SPARQL-UPDATE staging fix (a present WHERE
    /// matching nothing no longer fires all-literal inserts).
    fn lower_merge(&mut self, m: &MergeClause) -> Result<(), LowerCypherError> {
        if m.pattern.parts.len() != 1 {
            return Err(LowerCypherError::unsupported(
                "multi-part MERGE (comma-separated patterns) is deferred — v1 supports one pattern",
            ));
        }
        let part = &m.pattern.parts[0];
        if part.path_search.is_some() || part.path_var.is_some() {
            return Err(LowerCypherError::unsupported(
                "shortestPath / path-variable MERGE is not supported",
            ));
        }
        if !m.on_match.is_empty() {
            return Err(LowerCypherError::unsupported(
                "MERGE … ON MATCH SET is deferred — it needs a complementary guarded \
                 operation (create is the NOT EXISTS branch; ON MATCH SET is the EXISTS \
                 branch). v1 supports MERGE [ON CREATE SET …].",
            ));
        }

        self.txn_type = TxnType::Update;

        match part.tail.len() {
            0 => self.lower_merge_node(m, &part.head),
            1 => self.lower_merge_relationship(m, part),
            _ => Err(LowerCypherError::unsupported(
                "multi-hop MERGE pattern is deferred — v1 supports a single relationship \
                 `(a)-[r:T]->(b)`",
            )),
        }
    }

    /// Single-node MERGE (find-or-create). The identifying pattern (labels +
    /// inline props) becomes the `NOT EXISTS` guard; the create branch fires a
    /// fresh node carrying the same labels/props plus any `ON CREATE SET`.
    fn lower_merge_node(
        &mut self,
        m: &MergeClause,
        node: &NodePattern,
    ) -> Result<(), LowerCypherError> {
        if node.labels.is_empty() && node.props.is_none() {
            return Err(LowerCypherError::rejected(
                "bare MERGE `(n)` — a MERGE node needs a label or property to identify it",
            ));
        }

        // NOT EXISTS guard over a fresh probe var: the full identifying pattern.
        let probe = self.fresh_merge_probe();
        let guard = self.build_merge_guard(node, &probe)?;
        self.where_patterns
            .push(UnresolvedPattern::NotExists(guard));

        // Create branch: a fresh node (blank node, keyed on the MERGE var so
        // ON CREATE SET shares the subject) carrying the identifying
        // labels/props, plus ON CREATE SET inserts.
        let new_subj = self.node_subject(node);
        self.lower_node_create(node, new_subj.clone())?;
        let merge_var = node.var.as_ref().map(|v| v.name.clone());
        let identity_keys = node_identity_keys(node);
        for item in &m.on_create {
            self.emit_on_create_set(&new_subj, merge_var.as_deref(), &identity_keys, item)?;
        }
        Ok(())
    }

    /// Relationship MERGE `(a)-[r:T]->(b)`. The whole path becomes one
    /// `NOT EXISTS` guard; when no matching path exists the create branch mints
    /// the missing endpoints and the edge (with its `f:reifies*` reifier bundle)
    /// exactly once. `ON CREATE SET` may target either endpoint node variable.
    ///
    /// Endpoints are bound-aware: a variable bound by a preceding MATCH
    /// (`MATCH (a),(b) MERGE (a)-[:T]->(b)`) references the existing node, so the
    /// guard runs per matched row (per-row find-or-create); an endpoint
    /// introduced here (standalone MERGE) gets a fresh existential probe in the
    /// guard and a fresh blank node in the create branch.
    ///
    /// Deferred (clear errors): relationship properties and `ON CREATE SET` on
    /// the relationship variable — matching a property-bearing edge needs an
    /// annotation-sidecar guard the single-Txn model doesn't build.
    fn lower_merge_relationship(
        &mut self,
        m: &MergeClause,
        part: &PatternPart,
    ) -> Result<(), LowerCypherError> {
        let head_node = &part.head;
        let (rel, tail_node) = &part.tail[0];

        if matches!(rel.direction, Direction::Either) {
            return Err(LowerCypherError::rejected(
                "undirected relationship `-[r]-` in MERGE — use `-[r]->` or `<-[r]-`",
            ));
        }
        if rel.length.is_some() {
            return Err(LowerCypherError::rejected(
                "variable-length relationship in MERGE is not supported",
            ));
        }
        if rel.types.len() != 1 {
            return Err(LowerCypherError::rejected(
                "MERGE relationship needs exactly one type — `-[:T]->`",
            ));
        }
        if rel.props.is_some() {
            return Err(LowerCypherError::unsupported(
                "properties on a MERGE relationship are deferred — matching them needs an \
                 annotation-sidecar guard; MERGE the bare edge, or use CREATE",
            ));
        }

        // NOT EXISTS guard over the whole path: a bound endpoint contributes its
        // MATCH variable (per-row check); an unbound one a fresh existential
        // probe. Both are joined by the directed type triple.
        let head_term = self.merge_endpoint_term(head_node);
        let tail_term = self.merge_endpoint_term(tail_node);
        let mut guard = self.build_merge_guard(head_node, &head_term)?;
        guard.extend(self.build_merge_guard(tail_node, &tail_term)?);
        let type_iri = self.resolve_predicate(&rel.types[0].name)?;
        let (gs, go) = match rel.direction {
            Direction::Outgoing => (head_term, tail_term),
            Direction::Incoming => (tail_term, head_term),
            Direction::Either => unreachable!(),
        };
        guard.push(UnresolvedPattern::Triple(UnresolvedTriplePattern {
            s: gs,
            p: UnresolvedTerm::Iri(Arc::from(type_iri.as_str())),
            o: go,
            dtc: None,
        }));
        self.where_patterns
            .push(UnresolvedPattern::NotExists(guard));

        // Create branch: both endpoints + the directed edge with its bundle.
        self.lower_create_part(part)?;

        // ON CREATE SET — route each item to whichever endpoint var it targets.
        if !m.on_create.is_empty() {
            let head_subj = self.node_subject(head_node);
            let tail_subj = self.node_subject(tail_node);
            let head_var = head_node.var.as_ref().map(|v| v.name.as_str());
            let tail_var = tail_node.var.as_ref().map(|v| v.name.as_str());
            let head_keys = node_identity_keys(head_node);
            let tail_keys = node_identity_keys(tail_node);
            for item in &m.on_create {
                let tgt = set_item_target(item).name.as_str();
                if head_var == Some(tgt) {
                    self.emit_on_create_set(&head_subj, head_var, &head_keys, item)?;
                } else if tail_var == Some(tgt) {
                    self.emit_on_create_set(&tail_subj, tail_var, &tail_keys, item)?;
                } else {
                    return Err(LowerCypherError::unsupported(
                        "ON CREATE SET on a MERGE relationship targets only the endpoint node \
                         variables in v1 (the relationship variable is deferred)",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Mint a fresh existential probe variable for a MERGE `NOT EXISTS` guard.
    fn fresh_merge_probe(&mut self) -> UnresolvedTerm {
        let name = format!("?#__cy_merge_{}", self.synth_counter);
        self.synth_counter += 1;
        UnresolvedTerm::Var(Arc::from(name.as_str()))
    }

    /// The guard term for a MERGE relationship endpoint: a MATCH-bound variable
    /// references the existing node (per-row find-or-create against the
    /// pre-write snapshot); an unbound endpoint gets a fresh existential probe
    /// (whole-pattern standalone MERGE). The create branch makes the matching
    /// choice independently via [`Self::node_subject`].
    fn merge_endpoint_term(&mut self, node: &NodePattern) -> UnresolvedTerm {
        if let Some(var) = &node.var {
            if self.bound_vars.contains(&var.name) {
                return UnresolvedTerm::Var(Arc::from(var_name(&var.name).as_str()));
            }
        }
        self.fresh_merge_probe()
    }

    fn build_merge_guard(
        &mut self,
        node: &NodePattern,
        probe: &UnresolvedTerm,
    ) -> Result<Vec<UnresolvedPattern>, LowerCypherError> {
        let mut guard = Vec::new();
        for Label { name, .. } in &node.labels {
            let label_iri = self.resolve_iri(name);
            guard.push(UnresolvedPattern::Triple(UnresolvedTriplePattern {
                s: probe.clone(),
                p: UnresolvedTerm::Iri(Arc::from(rdf::TYPE)),
                o: UnresolvedTerm::Iri(Arc::from(label_iri.as_str())),
                dtc: None,
            }));
        }
        if let Some(props) = &node.props {
            for (key, val_expr) in &props.entries {
                let pred_iri = self.resolve_predicate(key)?;
                let obj = self.match_object_term(val_expr)?;
                guard.push(UnresolvedPattern::Triple(UnresolvedTriplePattern {
                    s: probe.clone(),
                    p: UnresolvedTerm::Iri(Arc::from(pred_iri.as_str())),
                    o: obj,
                    dtc: None,
                }));
            }
        }
        Ok(guard)
    }

    /// Emit an `ON CREATE SET` item as a plain insert on the freshly created
    /// node (no retract — the node is new). The target must be the MERGE
    /// node's variable.
    fn emit_on_create_set(
        &mut self,
        subj: &TemplateTerm,
        merge_var: Option<&str>,
        identity_keys: &std::collections::HashSet<&str>,
        item: &SetItem,
    ) -> Result<(), LowerCypherError> {
        let target_ok = |t: &Variable| matches!(merge_var, Some(v) if v == t.name);
        match item {
            SetItem::Property {
                target,
                property,
                value,
            } => {
                if !target_ok(target) {
                    return Err(merge_target_err());
                }
                if identity_keys.contains(property.as_str()) {
                    return Err(merge_identity_override_err(property));
                }
                let pred_iri = self.resolve_predicate(property)?;
                let pred_sid = self.ns.sid_for_iri(&pred_iri);
                // null / empty list → nothing on a new node; a list → one triple
                // per element (multi-valued predicate).
                for obj in self.expr_to_object_terms(value)? {
                    self.insert_templates.push(TripleTemplate::new(
                        subj.clone(),
                        TemplateTerm::Sid(pred_sid.clone()),
                        obj,
                    ));
                }
            }
            SetItem::MapMerge { target, map } => {
                if !target_ok(target) {
                    return Err(merge_target_err());
                }
                for (key, val) in &map.entries {
                    if identity_keys.contains(key.as_str()) {
                        return Err(merge_identity_override_err(key));
                    }
                    let pred_iri = self.resolve_predicate(key)?;
                    let pred_sid = self.ns.sid_for_iri(&pred_iri);
                    // null / empty list asserts nothing; a list asserts one
                    // triple per element.
                    for obj in self.expr_to_object_terms(val)? {
                        self.insert_templates.push(TripleTemplate::new(
                            subj.clone(),
                            TemplateTerm::Sid(pred_sid.clone()),
                            obj,
                        ));
                    }
                }
            }
            SetItem::Labels { target, labels } => {
                if !target_ok(target) {
                    return Err(merge_target_err());
                }
                let rdf_type_sid = self.ns.sid_for_iri(rdf::TYPE);
                for label in labels {
                    let iri = self.resolve_iri(label);
                    let sid = self.ns.sid_for_iri(&iri);
                    self.insert_templates.push(TripleTemplate::new(
                        subj.clone(),
                        TemplateTerm::Sid(rdf_type_sid.clone()),
                        TemplateTerm::Sid(sid),
                    ));
                }
            }
            SetItem::MapReplace { .. } => {
                return Err(LowerCypherError::unsupported(
                    "ON CREATE SET n = {…} (replace) is deferred",
                ));
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

        self.where_patterns.push(UnresolvedPattern::Optional(vec![
            UnresolvedPattern::Triple(UnresolvedTriplePattern {
                s: UnresolvedTerm::Var(Arc::from(var_name(target).as_str())),
                p: UnresolvedTerm::Iri(Arc::from(pred_iri)),
                o: UnresolvedTerm::Var(Arc::from(old_name.as_str())),
                dtc: None,
            }),
        ]));

        let subj = self.var_term(target);
        self.delete_templates.push(TripleTemplate::new(
            subj,
            TemplateTerm::Sid(pred_sid.clone()),
            TemplateTerm::Var(old_vid),
        ));
    }

    /// Emit `OPTIONAL { ?target ?p ?old }` plus filters for Cypher node
    /// properties, then delete the matched old property triples.
    fn push_optional_old_data_properties(&mut self, target: &str) {
        let (p_unres, p_term) = self.fresh_scan_var();
        let (old_unres, old_term) = self.fresh_scan_var();
        let p_name = p_unres
            .as_var()
            .expect("fresh scan predicate is a variable")
            .to_string();
        let old_name = old_unres
            .as_var()
            .expect("fresh scan object is a variable")
            .to_string();

        let p_expr = UnresolvedExpression::var(&p_name);
        let p_str = unresolved_call("str", vec![p_expr.clone()]);
        let old_expr = UnresolvedExpression::var(&old_name);
        let mut filters = Vec::with_capacity(2 + reifies_iris::ALL.len());
        filters.push(unresolved_call(
            "!=",
            vec![p_str.clone(), UnresolvedExpression::string(rdf::TYPE)],
        ));
        for iri in reifies_iris::ALL {
            filters.push(unresolved_call(
                "!=",
                vec![p_str.clone(), UnresolvedExpression::string(iri)],
            ));
        }
        filters.push(UnresolvedExpression::Not(Box::new(
            UnresolvedExpression::Or(vec![
                unresolved_call("isiri", vec![old_expr.clone()]),
                unresolved_call("isblank", vec![old_expr]),
            ]),
        )));
        let data_property_filter = UnresolvedExpression::And(filters);

        self.where_patterns.push(UnresolvedPattern::Optional(vec![
            UnresolvedPattern::Triple(UnresolvedTriplePattern {
                s: self.unresolved_named_var(target),
                p: p_unres,
                o: old_unres,
                dtc: None,
            }),
            UnresolvedPattern::Filter(data_property_filter),
        ]));

        let subj = self.var_term(target);
        self.delete_templates
            .push(TripleTemplate::new(subj, p_term, old_term));
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

        // LPG semantics: every Cypher-created relationship gets identity — a
        // fresh `f:reifies*` reifier bundle — so it is visible to named reads
        // (`-[r:T]->`), deletable by `DELETE r`, guarded by bare `DELETE n`,
        // and not collapsed with a parallel edge. The annotation blank node is
        // freshened per WHERE solution (SPARQL §3.1.3), so batched edge inserts
        // mint a distinct annotation per row. The base triple above also makes
        // the edge visible to anonymous (plain-RDF) reads.
        let ann = self.fresh_bnode();
        self.emit_reifier_bundle(&ann, &s, &type_sid, &o)?;
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
            // Null / empty-list values expand to no term ("no property"); a list
            // value expands to one term per element (multi-valued predicate).
            for obj in self.expr_to_object_terms(val_expr)? {
                self.insert_templates.push(TripleTemplate::new(
                    subj.clone(),
                    TemplateTerm::Sid(pred_sid.clone()),
                    obj,
                ));
            }
        }
        Ok(())
    }

    fn expr_to_object(&mut self, e: &Expr) -> Result<TemplateTerm, LowerCypherError> {
        match e {
            Expr::Lit(lit) => Ok(TemplateTerm::Value(lower_literal_value(lit)?)),
            // A WHERE-bound variable (e.g. a desugared `row.field` VALUES
            // column, or a MATCH-bound value) fires the property template per
            // solution. Unbound on a given row → that flake is skipped.
            Expr::Var(v) => Ok(self.var_term(&v.name)),
            _ => Err(LowerCypherError::unsupported(
                "CREATE property values must be literals or bound variables in v1",
            )),
        }
    }

    /// Expand a property value into its object terms — **one per element** for a
    /// list-valued property (a multi-valued RDF predicate: `{email: ['a','b']}`,
    /// IU1's `email[]` / `language[]`), or a single term otherwise. A null
    /// value, a null element, and an empty list all yield no term (Cypher: null
    /// = "no property"). Shared by CREATE, SET (`=` / `+=`), and MERGE ON CREATE
    /// SET so list-valued properties behave identically across all write ops.
    fn expr_to_object_terms(
        &mut self,
        value: &Expr,
    ) -> Result<Vec<TemplateTerm>, LowerCypherError> {
        let items: Vec<&Expr> = match value {
            Expr::List(items, _) => items.iter().collect(),
            other => vec![other],
        };
        let mut terms = Vec::with_capacity(items.len());
        for item in items {
            if matches!(item, Expr::Lit(Literal::Null(_))) {
                continue;
            }
            terms.push(self.expr_to_object(item)?);
        }
        Ok(terms)
    }

    fn node_subject(&mut self, n: &NodePattern) -> TemplateTerm {
        if let Some(var) = &n.var {
            // A var bound by a preceding MATCH references the existing
            // node (per-solution SID); an unbound var is a new node and
            // gets a stable per-variable blank node.
            if self.bound_vars.contains(&var.name) {
                return self.var_term(&var.name);
            }
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

/// Inline-property keys that a MERGE node asserts as its identity. `ON CREATE
/// SET` on one of these would double-assert (the identity insert can't be
/// retracted in the same create branch), so callers reject it.
fn node_identity_keys(node: &NodePattern) -> std::collections::HashSet<&str> {
    node.props
        .as_ref()
        .map(|p| p.entries.iter().map(|(k, _)| k.as_str()).collect())
        .unwrap_or_default()
}

/// The variable a `SET` / `ON CREATE SET` item targets.
fn set_item_target(item: &SetItem) -> &Variable {
    match item {
        SetItem::Property { target, .. }
        | SetItem::MapMerge { target, .. }
        | SetItem::MapReplace { target, .. }
        | SetItem::Labels { target, .. } => target,
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

fn merge_target_err() -> LowerCypherError {
    LowerCypherError::rejected("ON CREATE SET target must be the MERGE node variable")
}

fn merge_identity_override_err(key: &str) -> LowerCypherError {
    LowerCypherError::unsupported(format!(
        "ON CREATE SET on `{key}`, which is already part of the MERGE identity map, is \
         deferred — it would double-assert the property (Cypher SET overwrites, but the \
         identity insert can't be retracted in the same create branch). Drop `{key}` from \
         the MERGE map or from ON CREATE SET."
    ))
}

/// Convert a desugared `InlineRows` cell (a literal expression) into a VALUES
/// cell. `null` becomes `UNDEF` (unbound), so a row missing a field drops only
/// that row's match rather than the whole batch.
fn inline_cell_to_value(e: &Expr) -> Result<UnresolvedValue, LowerCypherError> {
    match e {
        Expr::Lit(Literal::Null(_)) => Ok(UnresolvedValue::Unbound),
        Expr::Lit(lit) => Ok(UnresolvedValue::Literal {
            value: lower_literal_unresolved(lit)?,
            dtc: None,
        }),
        _ => Err(LowerCypherError::unsupported(
            "internal: InlineRows cells must be literal values",
        )),
    }
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

fn lower_filter_literal(lit: &Literal) -> Result<UnresolvedExpression, LowerCypherError> {
    Ok(UnresolvedExpression::Const(match lit {
        Literal::Integer(n, _) => UnresolvedFilterValue::Long(*n),
        Literal::Float(f, _) => UnresolvedFilterValue::Double(*f),
        Literal::String(s, _) => UnresolvedFilterValue::String(Arc::from(s.as_str())),
        Literal::Bool(b, _) => UnresolvedFilterValue::Bool(*b),
        Literal::Null(_) => {
            return Err(LowerCypherError::unsupported(
                "NULL literal in write MATCH WHERE is rejected — use IS NULL / IS NOT NULL",
            ));
        }
    }))
}

fn unresolved_call(func: &str, args: Vec<UnresolvedExpression>) -> UnresolvedExpression {
    UnresolvedExpression::Call {
        func: Arc::from(func),
        args,
    }
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
