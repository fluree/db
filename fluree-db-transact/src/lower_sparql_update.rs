//! SPARQL UPDATE to Transaction IR lowering.
//!
//! This module converts parsed SPARQL UPDATE AST (`UpdateOperation`) into the
//! Transaction IR (`Txn`) that is shared with JSON-LD transactions.
//!
//! # Architecture
//!
//! ```text
//!        SPARQL UPDATE                    JSON-LD Transaction
//!              │                                  │
//! parse_sparql()                      parse_transaction()
//!              │                                  │
//!              ▼                                  ▼
//!        SPARQL AST                        JSON-LD Value
//!    (UpdateOperation)                           │
//!              │                                  │
//! lower_sparql_update()◄────────────────────────►│
//!              │                                  │
//!              └─────────────► Txn IR ◄──────────┘
//!                       │
//!                       │  (shared from here)
//!                       ▼
//!                   stage()
//!                       │
//!                       ▼
//!                Vec<Flake>
//! ```
//!
//! # MVP Restrictions
//!
//! This implementation supports full SPARQL graph patterns in UPDATE WHERE clauses
//! by deferring WHERE lowering to staging time (when a ledger snapshot is available
//! for IRI encoding) and reusing the shared query engine.
//!
//! Additional restrictions:
//! - WITH/USING clauses are rejected

use std::mem;
use std::sync::Arc;

use fluree_db_core::DatatypeConstraint;
use fluree_db_core::FlakeValue;
use fluree_db_query::parse::{
    LiteralValue, UnresolvedDatatypeConstraint, UnresolvedPattern, UnresolvedTerm,
    UnresolvedTriplePattern,
};
use fluree_db_query::VarRegistry;
use fluree_db_sparql::ast::{
    BlankNodeValue, Iri, IriValue, Literal, LiteralValue as SparqlLiteralValue, Modify,
    PredicateTerm, Prologue, QuadData, QuadPattern, QuadPatternElement, QueryBody, SparqlAst,
    SubjectTerm, Term, TriplePattern, UpdateOperation,
};
use fluree_db_sparql::SourceSpan;
use rustc_hash::FxHashMap;
use thiserror::Error;

use crate::ir::{SparqlWhereClause, TemplateTerm, TripleTemplate, Txn, TxnOpts, TxnType};
use crate::namespace::NamespaceRegistry;
use fluree_vocab::{fluree, xsd};

/// Result of converting a SPARQL term to an unresolved term with metadata.
struct UnresolvedTermWithMeta {
    /// The unresolved term
    term: UnresolvedTerm,
    /// Optional datatype or language-tag constraint
    dtc: Option<UnresolvedDatatypeConstraint>,
}

/// Errors that can occur during SPARQL UPDATE lowering.
#[derive(Debug, Error)]
pub enum LowerError {
    /// Expected SPARQL UPDATE, found a query
    #[error("Expected SPARQL UPDATE, found query")]
    NotAnUpdate { span: SourceSpan },

    /// Unsupported feature encountered
    #[error("{feature} is not yet supported in SPARQL UPDATE lowering")]
    UnsupportedFeature {
        feature: &'static str,
        span: SourceSpan,
    },

    /// Undefined prefix in IRI
    #[error("Undefined prefix '{prefix}:'")]
    UndefinedPrefix { prefix: String, span: SourceSpan },
}

/// Counter for generating anonymous blank node labels.
struct BlankNodeCounter {
    next: u32,
}

impl BlankNodeCounter {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> String {
        let label = format!("_:b{}", self.next);
        self.next += 1;
        label
    }
}

/// Assign stable variable names for SPARQL blank nodes when lowering
/// triple-template forms like `DELETE WHERE { ... }`.
///
/// In SPARQL graph patterns, blank node labels behave like locally-scoped
/// existential variables; lowering rewrites them to query variables with
/// special names (e.g., `_:b1`).
struct BlankNodeVarNamer {
    anon_counter: u32,
}

struct TemplateGraphIds {
    next: u16,
    iri_to_local: std::collections::HashMap<String, u16>,
    delta: FxHashMap<u16, String>,
}

impl TemplateGraphIds {
    fn new() -> Self {
        // 0=default, 1=txn-meta, 2=config, 3+=user graphs (txn-local ids)
        Self {
            next: 3,
            iri_to_local: std::collections::HashMap::new(),
            delta: FxHashMap::default(),
        }
    }

    fn get_or_assign(&mut self, iri: String) -> u16 {
        if let Some(id) = self.iri_to_local.get(&iri) {
            return *id;
        }
        let id = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("txn-local graph id overflow");
        self.iri_to_local.insert(iri.clone(), id);
        self.delta.insert(id, iri);
        id
    }

    fn delta(&self) -> FxHashMap<u16, String> {
        self.delta.clone()
    }
}

impl BlankNodeVarNamer {
    fn new() -> Self {
        Self { anon_counter: 0 }
    }

    fn var_name(&mut self, bn: &BlankNodeValue) -> Arc<str> {
        match bn {
            BlankNodeValue::Labeled(label) => Arc::from(format!("_:{label}")),
            BlankNodeValue::Anon => {
                let name: Arc<str> = Arc::from(format!("_:b{}", self.anon_counter));
                self.anon_counter += 1;
                name
            }
        }
    }
}

/// Result of converting a literal to template form.
struct LiteralResult {
    term: TemplateTerm,
    dtc: Option<DatatypeConstraint>,
}

/// Lower a parsed SPARQL AST to the Transaction IR.
///
/// This is a convenience wrapper that extracts the `UpdateOperation` and `Prologue`
/// from a `SparqlAst` and calls `lower_sparql_update`.
///
/// # Arguments
///
/// * `ast` - The parsed SPARQL AST (must contain an UPDATE operation)
/// * `ns` - The namespace registry for IRI-to-Sid encoding
/// * `opts` - Transaction options (branch, context, etc.)
///
/// # Returns
///
/// A `Txn` that can be staged using the shared transaction pipeline.
///
/// # Errors
///
/// Returns `LowerError` if:
/// - The AST body is not an UPDATE operation (is a query)
/// - WITH or USING clauses are present
/// - Blank nodes appear in WHERE patterns
/// - RDF-star quoted triples are used
pub fn lower_sparql_update_ast(
    ast: &SparqlAst,
    ns: &mut NamespaceRegistry,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let update_op = match &ast.body {
        QueryBody::Update(op) => op,
        _ => {
            return Err(LowerError::NotAnUpdate { span: ast.span });
        }
    };
    lower_sparql_update(update_op, &ast.prologue, ns, opts)
}

/// Lower a SPARQL UPDATE operation to the Transaction IR.
///
/// # Arguments
///
/// * `op` - The parsed SPARQL UPDATE operation
/// * `prologue` - The prologue containing PREFIX declarations
/// * `ns` - The namespace registry for IRI-to-Sid encoding
/// * `opts` - Transaction options (branch, context, etc.)
///
/// # Returns
///
/// A `Txn` that can be staged using the shared transaction pipeline.
///
/// # Errors
///
/// Returns `LowerError` if:
/// - WITH or USING clauses are present
/// - Blank nodes appear in WHERE patterns
/// - RDF-star quoted triples are used
pub fn lower_sparql_update(
    op: &UpdateOperation,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let mut vars = VarRegistry::new();
    let mut bnodes = BlankNodeCounter::new();

    let mut txn = match op {
        UpdateOperation::InsertData(insert) => {
            lower_insert_data(&insert.data, prologue, ns, &mut vars, &mut bnodes, opts)?
        }
        UpdateOperation::DeleteData(delete) => {
            lower_delete_data(&delete.data, prologue, ns, &mut vars, &mut bnodes, opts)?
        }
        UpdateOperation::DeleteWhere(delete_where) => {
            lower_delete_where(&delete_where.pattern, prologue, ns, &mut vars, opts)?
        }
        UpdateOperation::Modify(modify) => {
            lower_modify(modify, prologue, ns, &mut vars, &mut bnodes, opts)?
        }
    };
    // Hand off the lowering registry's allocations so `stage_transaction_from_txn`
    // can merge them into its own snapshot-derived registry. Without this, the
    // first SPARQL `INSERT DATA` on a fresh ledger commits flakes referencing
    // namespace codes the staging registry never learned about — the commit's
    // persisted namespace map omits them, and post-commit SELECT can't resolve
    // the predicate IRI back to the same Sid.
    txn.namespace_delta = ns.delta().clone();
    Ok(txn)
}

/// Lower INSERT DATA operation.
///
/// INSERT DATA contains ground triples (no variables) that are directly inserted.
fn lower_insert_data(
    data: &QuadData,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let insert_templates = lower_triples_to_templates(&data.triples, prologue, ns, vars, bnodes)?;

    Ok(Txn {
        txn_type: TxnType::Insert,
        where_patterns: Vec::new(),
        sparql_where: None,
        delete_templates: Vec::new(),
        insert_templates,
        values: None,
        update_where_default_graph_iris: None,
        update_where_named_graphs: None,
        opts,
        vars: mem::take(vars),
        txn_meta: Vec::new(),
        graph_delta: FxHashMap::default(),
        namespace_delta: std::collections::HashMap::new(),
    })
}

/// Lower DELETE DATA operation.
///
/// DELETE DATA contains ground triples (no variables) that are retracted.
/// Uses TxnType::Update because it's a retract-only transaction.
fn lower_delete_data(
    data: &QuadData,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let delete_templates = lower_triples_to_templates(&data.triples, prologue, ns, vars, bnodes)?;

    Ok(Txn {
        txn_type: TxnType::Update,
        where_patterns: Vec::new(),
        sparql_where: None,
        delete_templates,
        insert_templates: Vec::new(),
        values: None,
        update_where_default_graph_iris: None,
        update_where_named_graphs: None,
        opts,
        vars: mem::take(vars),
        txn_meta: Vec::new(),
        graph_delta: FxHashMap::default(),
        namespace_delta: std::collections::HashMap::new(),
    })
}

/// Lower DELETE WHERE operation.
///
/// DELETE WHERE uses the same pattern for matching and deletion.
/// The pattern becomes both the WHERE clause and the DELETE template.
fn lower_delete_where(
    pattern: &QuadPattern,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    // DELETE WHERE in SPARQL Update uses a QuadPattern (i.e., a set of triple templates),
    // not a general GroupGraphPattern. Keeping this on the triple-only path is
    // intentional: there is no FILTER/OPTIONAL/subquery form for DELETE WHERE.
    //
    // (In contrast, Modify operations store a full graph-pattern WHERE for staging-time lowering.)
    // DELETE WHERE is shorthand for `DELETE { pattern } WHERE { pattern }`.
    //
    // In SPARQL, blank nodes in a graph pattern behave like locally-scoped existential
    // variables. To keep DELETE WHERE semantics correct (including for blank nodes),
    // we lower blank nodes into variables consistently across BOTH:
    // - the WHERE patterns (for matching/bindings)
    // - the DELETE templates (for instantiating concrete retractions)
    // Phase 1: GRAPH blocks are not supported in DELETE WHERE lowering yet.
    let triples: Vec<&TriplePattern> = pattern
        .patterns
        .iter()
        .map(|el| match el {
            QuadPatternElement::Triple(t) => Ok(t),
            QuadPatternElement::Graph { span, .. } => Err(LowerError::UnsupportedFeature {
                feature: "GRAPH blocks in DELETE WHERE",
                span: *span,
            }),
        })
        .collect::<Result<_, _>>()?;

    let mut bnode_vars = BlankNodeVarNamer::new();
    let mut where_patterns: Vec<UnresolvedPattern> = Vec::with_capacity(triples.len());
    let mut delete_templates: Vec<TripleTemplate> = Vec::with_capacity(triples.len());

    for tp in triples {
        // WHERE side: lower to UnresolvedPattern::Triple with bnodes rewritten as vars
        let s = subject_to_unresolved_delete_where(&tp.subject, prologue, &mut bnode_vars)?;
        let p = predicate_to_unresolved(&tp.predicate, prologue)?;
        let obj = object_to_unresolved_delete_where(&tp.object, prologue, &mut bnode_vars)?;

        where_patterns.push(UnresolvedPattern::Triple(UnresolvedTriplePattern {
            s,
            p,
            o: obj.term,
            dtc: obj.dtc,
        }));

        // DELETE side: lower to TripleTemplate with the same bnode->var mapping
        delete_templates.push(lower_triple_to_delete_template_delete_where(
            tp,
            prologue,
            ns,
            vars,
            &mut bnode_vars,
        )?);
    }

    Ok(Txn {
        txn_type: TxnType::Update,
        where_patterns,
        sparql_where: None,
        delete_templates,
        insert_templates: Vec::new(),
        values: None,
        update_where_default_graph_iris: None,
        update_where_named_graphs: None,
        opts,
        vars: mem::take(vars),
        txn_meta: Vec::new(),
        graph_delta: FxHashMap::default(),
        namespace_delta: std::collections::HashMap::new(),
    })
}

/// Lower Modify operation (DELETE/INSERT with WHERE).
///
/// The most general update form with optional WITH, DELETE, INSERT, and WHERE clauses.
fn lower_modify(
    modify: &Modify,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    // Store WHERE clause for staging-time SPARQL lowering (full graph-pattern support).
    //
    // Note: `lower_sparql_update` takes `&UpdateOperation`, so we can't move out of the AST here.
    // Cloning keeps the lowering interface simple and ensures the transaction IR owns its WHERE.
    let mut graph_ids = TemplateGraphIds::new();
    let with_graph_iri: Option<String> = if let Some(iri) = modify.with_iri.as_ref() {
        Some(expand_iri(iri, prologue)?)
    } else {
        None
    };

    let using_default_graph_iris: Vec<String> = if let Some(using) = modify.using.as_ref() {
        using
            .default_graphs
            .iter()
            .map(|iri| expand_iri(iri, prologue))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };

    let using_named_graph_iris: Vec<String> = if let Some(using) = modify.using.as_ref() {
        using
            .named_graphs
            .iter()
            .map(|iri| expand_iri(iri, prologue))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };

    let default_template_graph_id: Option<u16> = with_graph_iri
        .as_ref()
        .map(|iri| graph_ids.get_or_assign(iri.clone()));

    let sparql_where = SparqlWhereClause {
        prologue: prologue.clone(),
        with_graph_iri,
        using_default_graph_iris,
        using_named_graph_iris,
        pattern: modify.where_clause.clone(),
    };

    // Lower DELETE templates (if present)
    let delete_templates = if let Some(delete_clause) = &modify.delete_clause {
        lower_quad_pattern_to_templates(
            delete_clause,
            prologue,
            ns,
            vars,
            bnodes,
            &mut graph_ids,
            default_template_graph_id,
        )?
    } else {
        Vec::new()
    };

    // Lower INSERT templates (if present)
    let insert_templates = if let Some(insert_clause) = &modify.insert_clause {
        lower_quad_pattern_to_templates(
            insert_clause,
            prologue,
            ns,
            vars,
            bnodes,
            &mut graph_ids,
            default_template_graph_id,
        )?
    } else {
        Vec::new()
    };

    Ok(Txn {
        txn_type: TxnType::Update,
        where_patterns: Vec::new(),
        sparql_where: Some(sparql_where),
        delete_templates,
        insert_templates,
        values: None,
        update_where_default_graph_iris: None,
        update_where_named_graphs: None,
        opts,
        vars: mem::take(vars),
        txn_meta: Vec::new(),
        graph_delta: graph_ids.delta(),
        namespace_delta: std::collections::HashMap::new(),
    })
}

fn lower_quad_pattern_to_templates(
    pattern: &QuadPattern,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    graph_ids: &mut TemplateGraphIds,
    default_graph_id: Option<u16>,
) -> Result<Vec<TripleTemplate>, LowerError> {
    let mut out: Vec<TripleTemplate> = Vec::new();
    for el in &pattern.patterns {
        match el {
            QuadPatternElement::Triple(tp) => {
                let mut t = lower_triple_to_template(tp, prologue, ns, vars, bnodes)?;
                if let Some(g_id) = default_graph_id {
                    t = t.with_graph_id(g_id);
                }
                out.push(t);
            }
            QuadPatternElement::Graph { name, triples, .. } => {
                let graph_iri = match name {
                    fluree_db_sparql::ast::pattern::GraphName::Iri(iri) => {
                        expand_iri(iri, prologue)?
                    }
                    fluree_db_sparql::ast::pattern::GraphName::Var(v) => {
                        return Err(LowerError::UnsupportedFeature {
                            feature: "GRAPH variables in UPDATE templates",
                            span: v.span,
                        });
                    }
                };
                let txn_local_g_id = graph_ids.get_or_assign(graph_iri);
                for tp in triples {
                    out.push(
                        lower_triple_to_template(tp, prologue, ns, vars, bnodes)?
                            .with_graph_id(txn_local_g_id),
                    );
                }
            }
        }
    }
    Ok(out)
}

/// Lower triple patterns to TripleTemplate for DELETE/INSERT templates.
fn lower_triples_to_templates(
    triples: &[TriplePattern],
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
) -> Result<Vec<TripleTemplate>, LowerError> {
    triples
        .iter()
        .map(|tp| lower_triple_to_template(tp, prologue, ns, vars, bnodes))
        .collect()
}

/// Lower a single triple pattern to TripleTemplate.
fn lower_triple_to_template(
    triple: &TriplePattern,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
) -> Result<TripleTemplate, LowerError> {
    let subject = subject_to_template(&triple.subject, prologue, ns, vars, bnodes)?;
    let predicate = predicate_to_template(&triple.predicate, prologue, ns, vars)?;

    // Object needs special handling for literal metadata
    let (object, dtc) = match &triple.object {
        Term::Literal(lit) => {
            let result = literal_to_template(lit, prologue, ns)?;
            (result.term, result.dtc)
        }
        other => (object_to_template(other, prologue, ns, vars, bnodes)?, None),
    };

    Ok(TripleTemplate {
        subject,
        predicate,
        object,
        dtc,
        list_index: None, // Always None for SPARQL UPDATE
        graph_id: None,   // Default graph
    })
}

// =============================================================================
// Term conversion for WHERE patterns (UnresolvedTerm)
// =============================================================================

fn subject_to_unresolved_delete_where(
    term: &SubjectTerm,
    prologue: &Prologue,
    bnodes: &mut BlankNodeVarNamer,
) -> Result<UnresolvedTerm, LowerError> {
    match term {
        SubjectTerm::Var(v) => Ok(UnresolvedTerm::Var(Arc::from(format!("?{}", v.name)))),
        SubjectTerm::Iri(iri) => Ok(UnresolvedTerm::Iri(Arc::from(expand_iri(iri, prologue)?))),
        SubjectTerm::BlankNode(bn) => Ok(UnresolvedTerm::Var(bnodes.var_name(&bn.value))),
        SubjectTerm::QuotedTriple(qt) => Err(LowerError::UnsupportedFeature {
            feature: "RDF-star quoted triple",
            span: qt.span,
        }),
    }
}

/// Convert SPARQL PredicateTerm to UnresolvedTerm.
fn predicate_to_unresolved(
    term: &PredicateTerm,
    prologue: &Prologue,
) -> Result<UnresolvedTerm, LowerError> {
    match term {
        PredicateTerm::Var(v) => Ok(UnresolvedTerm::Var(Arc::from(format!("?{}", v.name)))),
        PredicateTerm::Iri(iri) => Ok(UnresolvedTerm::Iri(Arc::from(expand_iri(iri, prologue)?))),
    }
}

fn object_to_unresolved_delete_where(
    term: &Term,
    prologue: &Prologue,
    bnodes: &mut BlankNodeVarNamer,
) -> Result<UnresolvedTermWithMeta, LowerError> {
    match term {
        Term::Var(v) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Var(Arc::from(format!("?{}", v.name))),
            dtc: None,
        }),
        Term::Iri(iri) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Iri(Arc::from(expand_iri(iri, prologue)?)),
            dtc: None,
        }),
        Term::Literal(lit) => literal_to_unresolved(lit, prologue),
        Term::BlankNode(bn) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Var(bnodes.var_name(&bn.value)),
            dtc: None,
        }),
    }
}

fn lower_triple_to_delete_template_delete_where(
    triple: &TriplePattern,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeVarNamer,
) -> Result<TripleTemplate, LowerError> {
    // Subject
    let subject = match &triple.subject {
        SubjectTerm::Var(v) => TemplateTerm::Var(vars.get_or_insert(&format!("?{}", v.name))),
        SubjectTerm::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            TemplateTerm::Sid(ns.sid_for_iri(&expanded))
        }
        SubjectTerm::BlankNode(bn) => {
            let name = bnodes.var_name(&bn.value);
            TemplateTerm::Var(vars.get_or_insert(&name))
        }
        SubjectTerm::QuotedTriple(qt) => {
            return Err(LowerError::UnsupportedFeature {
                feature: "RDF-star quoted triple",
                span: qt.span,
            });
        }
    };

    // Predicate
    let predicate = match &triple.predicate {
        PredicateTerm::Var(v) => TemplateTerm::Var(vars.get_or_insert(&format!("?{}", v.name))),
        PredicateTerm::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            TemplateTerm::Sid(ns.sid_for_iri(&expanded))
        }
    };

    // Object + datatype constraint (for literals)
    let (object, dtc) = match &triple.object {
        Term::Var(v) => (
            TemplateTerm::Var(vars.get_or_insert(&format!("?{}", v.name))),
            None,
        ),
        Term::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            (TemplateTerm::Sid(ns.sid_for_iri(&expanded)), None)
        }
        Term::Literal(lit) => {
            let r = literal_to_template(lit, prologue, ns)?;
            (r.term, r.dtc)
        }
        Term::BlankNode(bn) => {
            let name = bnodes.var_name(&bn.value);
            (TemplateTerm::Var(vars.get_or_insert(&name)), None)
        }
    };

    Ok(TripleTemplate {
        subject,
        predicate,
        object,
        dtc,
        list_index: None,
        graph_id: None,
    })
}

/// Convert SPARQL Literal to UnresolvedTerm with metadata.
fn literal_to_unresolved(
    lit: &Literal,
    prologue: &Prologue,
) -> Result<UnresolvedTermWithMeta, LowerError> {
    match &lit.value {
        SparqlLiteralValue::Simple(s) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::String(Arc::from(s.as_ref()))),
            dtc: None,
        }),
        SparqlLiteralValue::LangTagged { value, lang } => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::String(Arc::from(value.as_ref()))),
            dtc: Some(UnresolvedDatatypeConstraint::LangTag(Arc::from(
                lang.as_ref(),
            ))),
        }),
        SparqlLiteralValue::Typed { value, datatype } => {
            let dt_iri = expand_iri(datatype, prologue)?;
            let coerced = coerce_typed_value(value, &dt_iri);
            Ok(UnresolvedTermWithMeta {
                term: coerced,
                dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(dt_iri))),
            })
        }
        SparqlLiteralValue::Integer(i) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::Long(*i)),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                xsd::INTEGER,
            ))),
        }),
        SparqlLiteralValue::Double(d) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::Double(*d)),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                xsd::DOUBLE,
            ))),
        }),
        SparqlLiteralValue::Decimal(s) => {
            // Try to parse as f64; on failure, keep as string with datatype
            let term = match s.parse::<f64>() {
                Ok(d) => UnresolvedTerm::Literal(LiteralValue::Double(d)),
                Err(_) => UnresolvedTerm::Literal(LiteralValue::String(Arc::from(s.as_ref()))),
            };
            Ok(UnresolvedTermWithMeta {
                term,
                dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                    xsd::DECIMAL,
                ))),
            })
        }
        SparqlLiteralValue::Boolean(b) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::Boolean(*b)),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                xsd::BOOLEAN,
            ))),
        }),
    }
}

// =============================================================================
// Term conversion for DELETE/INSERT templates (TemplateTerm)
// =============================================================================

/// Convert SPARQL SubjectTerm to TemplateTerm (for DELETE/INSERT templates).
fn subject_to_template(
    term: &SubjectTerm,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
) -> Result<TemplateTerm, LowerError> {
    match term {
        SubjectTerm::Var(v) => {
            let var_name = format!("?{}", v.name);
            Ok(TemplateTerm::Var(vars.get_or_insert(&var_name)))
        }
        SubjectTerm::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            Ok(TemplateTerm::Sid(ns.sid_for_iri(&expanded)))
        }
        SubjectTerm::BlankNode(bn) => {
            let label = match &bn.value {
                BlankNodeValue::Labeled(l) => format!("_:{l}"),
                BlankNodeValue::Anon => bnodes.next(),
            };
            Ok(TemplateTerm::BlankNode(label))
        }
        SubjectTerm::QuotedTriple(qt) => Err(LowerError::UnsupportedFeature {
            feature: "RDF-star quoted triple",
            span: qt.span,
        }),
    }
}

/// Convert SPARQL PredicateTerm to TemplateTerm.
fn predicate_to_template(
    term: &PredicateTerm,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
) -> Result<TemplateTerm, LowerError> {
    match term {
        PredicateTerm::Var(v) => {
            let var_name = format!("?{}", v.name);
            Ok(TemplateTerm::Var(vars.get_or_insert(&var_name)))
        }
        PredicateTerm::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            Ok(TemplateTerm::Sid(ns.sid_for_iri(&expanded)))
        }
    }
}

/// Convert SPARQL Term (object position) to TemplateTerm.
///
/// Note: For literal terms, use the metadata-aware path in `lower_triple_to_template`
/// which calls `literal_to_template` directly to preserve datatype/language.
fn object_to_template(
    term: &Term,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
) -> Result<TemplateTerm, LowerError> {
    match term {
        Term::Var(v) => {
            let var_name = format!("?{}", v.name);
            Ok(TemplateTerm::Var(vars.get_or_insert(&var_name)))
        }
        Term::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            Ok(TemplateTerm::Sid(ns.sid_for_iri(&expanded)))
        }
        // Literals should go through literal_to_template for metadata; this is a fallback
        Term::Literal(lit) => Ok(literal_to_template(lit, prologue, ns)?.term),
        Term::BlankNode(bn) => {
            let label = match &bn.value {
                BlankNodeValue::Labeled(l) => format!("_:{l}"),
                BlankNodeValue::Anon => bnodes.next(),
            };
            Ok(TemplateTerm::BlankNode(label))
        }
    }
}

/// Convert SPARQL Literal to TemplateTerm with datatype/language metadata.
fn literal_to_template(
    lit: &Literal,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
) -> Result<LiteralResult, LowerError> {
    match &lit.value {
        SparqlLiteralValue::Simple(s) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::String(s.to_string())),
            dtc: None,
        }),
        SparqlLiteralValue::LangTagged { value, lang } => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::String(value.to_string())),
            dtc: Some(DatatypeConstraint::LangTag(Arc::from(lang.as_ref()))),
        }),
        SparqlLiteralValue::Typed { value, datatype } => {
            let dt_iri = expand_iri(datatype, prologue)?;
            let dt_sid = ns.sid_for_iri(&dt_iri);
            let coerced = coerce_typed_flake_value(value, &dt_iri);
            Ok(LiteralResult {
                term: TemplateTerm::Value(coerced),
                dtc: Some(DatatypeConstraint::Explicit(dt_sid)),
            })
        }
        SparqlLiteralValue::Integer(i) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::Long(*i)),
            dtc: Some(DatatypeConstraint::Explicit(ns.sid_for_iri(xsd::INTEGER))),
        }),
        SparqlLiteralValue::Double(d) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::Double(*d)),
            dtc: Some(DatatypeConstraint::Explicit(ns.sid_for_iri(xsd::DOUBLE))),
        }),
        SparqlLiteralValue::Decimal(s) => {
            // Try to parse as f64; on failure, keep as string with datatype
            let term = match s.parse::<f64>() {
                Ok(d) => TemplateTerm::Value(FlakeValue::Double(d)),
                Err(_) => TemplateTerm::Value(FlakeValue::String(s.to_string())),
            };
            Ok(LiteralResult {
                term,
                dtc: Some(DatatypeConstraint::Explicit(ns.sid_for_iri(xsd::DECIMAL))),
            })
        }
        SparqlLiteralValue::Boolean(b) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::Boolean(*b)),
            dtc: Some(DatatypeConstraint::Explicit(ns.sid_for_iri(xsd::BOOLEAN))),
        }),
    }
}

// =============================================================================
// IRI expansion
// =============================================================================

/// Expand an IRI using prologue PREFIX declarations.
fn expand_iri(iri: &Iri, prologue: &Prologue) -> Result<String, LowerError> {
    match &iri.value {
        IriValue::Full(full) => Ok(full.to_string()),
        IriValue::Prefixed { prefix, local } => {
            // Look up prefix in prologue
            for decl in &prologue.prefixes {
                if decl.prefix.as_ref() == prefix.as_ref() {
                    return Ok(format!("{}{}", decl.iri, local));
                }
            }
            // Undefined prefix is an error
            Err(LowerError::UndefinedPrefix {
                prefix: prefix.to_string(),
                span: iri.span,
            })
        }
    }
}

// =============================================================================
// Typed value coercion
// =============================================================================

/// Coerce a typed literal lexical value to UnresolvedTerm.
fn coerce_typed_value(lexical: &str, datatype_iri: &str) -> UnresolvedTerm {
    // MVP: basic coercion for common types
    match datatype_iri {
        xsd::INTEGER => {
            if let Ok(i) = lexical.parse::<i64>() {
                return UnresolvedTerm::Literal(LiteralValue::Long(i));
            }
        }
        xsd::DOUBLE | xsd::DECIMAL => {
            if let Ok(d) = lexical.parse::<f64>() {
                return UnresolvedTerm::Literal(LiteralValue::Double(d));
            }
        }
        xsd::BOOLEAN => {
            if lexical == "true" || lexical == "1" {
                return UnresolvedTerm::Literal(LiteralValue::Boolean(true));
            } else if lexical == "false" || lexical == "0" {
                return UnresolvedTerm::Literal(LiteralValue::Boolean(false));
            }
        }
        // f:embeddingVector — share the core lexical parser with JSON-LD/Turtle
        // so f32 quantization is uniform across ingest paths. The query
        // layer's `LiteralValue::Vector` is lowered to `FlakeValue::Vector`
        // with the correct datatype Sid in `parse/lower.rs`.
        fluree::EMBEDDING_VECTOR => {
            if let Ok(FlakeValue::Vector(v)) =
                fluree_db_core::coerce::coerce_string_value(lexical, datatype_iri)
            {
                return UnresolvedTerm::Literal(LiteralValue::Vector(v));
            }
        }
        _ => {}
    }
    // Fall back to string
    UnresolvedTerm::Literal(LiteralValue::String(Arc::from(lexical)))
}

/// Coerce a typed literal lexical value to FlakeValue.
fn coerce_typed_flake_value(lexical: &str, datatype_iri: &str) -> FlakeValue {
    // MVP: basic coercion for common types
    match datatype_iri {
        xsd::INTEGER => {
            if let Ok(i) = lexical.parse::<i64>() {
                return FlakeValue::Long(i);
            }
        }
        xsd::DOUBLE | xsd::DECIMAL => {
            if let Ok(d) = lexical.parse::<f64>() {
                return FlakeValue::Double(d);
            }
        }
        xsd::BOOLEAN => {
            if lexical == "true" || lexical == "1" {
                return FlakeValue::Boolean(true);
            } else if lexical == "false" || lexical == "0" {
                return FlakeValue::Boolean(false);
            }
        }
        // See `coerce_typed_value` — share core's parser for f:embeddingVector.
        fluree::EMBEDDING_VECTOR => {
            if let Ok(fv @ FlakeValue::Vector(_)) =
                fluree_db_core::coerce::coerce_string_value(lexical, datatype_iri)
            {
                return fv;
            }
        }
        _ => {}
    }
    // Fall back to string
    FlakeValue::String(lexical.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_sparql::ast::PrefixDecl;
    use fluree_db_sparql::SourceSpan;

    fn test_span() -> SourceSpan {
        SourceSpan::new(0, 10)
    }

    fn test_prologue() -> Prologue {
        Prologue {
            base: None,
            prefixes: vec![PrefixDecl {
                prefix: Arc::from("ex"),
                iri: Arc::from("http://example.org/"),
                span: test_span(),
            }],
        }
    }

    #[test]
    fn test_expand_full_iri() {
        let iri = Iri::full("http://example.org/test", test_span());
        let prologue = test_prologue();
        assert_eq!(
            expand_iri(&iri, &prologue).unwrap(),
            "http://example.org/test"
        );
    }

    #[test]
    fn test_expand_prefixed_iri() {
        let iri = Iri::prefixed("ex", "name", test_span());
        let prologue = test_prologue();
        assert_eq!(
            expand_iri(&iri, &prologue).unwrap(),
            "http://example.org/name"
        );
    }

    #[test]
    fn test_expand_undefined_prefix_error() {
        let iri = Iri::prefixed("unknown", "name", test_span());
        let prologue = test_prologue();
        let result = expand_iri(&iri, &prologue);
        assert!(matches!(
            result,
            Err(LowerError::UndefinedPrefix { prefix, .. }) if prefix == "unknown"
        ));
    }

    #[test]
    fn test_variable_normalization() {
        use fluree_db_sparql::ast::Var;

        let var = Var::new("name", test_span());
        let prologue = test_prologue();
        let subject = SubjectTerm::Var(var);
        let mut namer = BlankNodeVarNamer::new();

        let result = subject_to_unresolved_delete_where(&subject, &prologue, &mut namer).unwrap();
        match result {
            UnresolvedTerm::Var(name) => assert_eq!(name.as_ref(), "?name"),
            _ => panic!("Expected variable term"),
        }
    }

    #[test]
    fn test_blank_node_in_delete_where_is_lowered_to_var() {
        use fluree_db_sparql::ast::BlankNode;

        let bn = BlankNode::labeled("b1", test_span());
        let prologue = test_prologue();
        let subject = SubjectTerm::BlankNode(bn);
        let mut namer = BlankNodeVarNamer::new();

        let result = subject_to_unresolved_delete_where(&subject, &prologue, &mut namer).unwrap();
        assert_eq!(result, UnresolvedTerm::Var(Arc::from("_:b1")));
    }

    #[test]
    fn test_coerce_typed_integer() {
        let result = coerce_typed_value("42", xsd::INTEGER);
        assert!(matches!(
            result,
            UnresolvedTerm::Literal(LiteralValue::Long(42))
        ));
    }

    #[test]
    fn test_coerce_typed_boolean() {
        let result = coerce_typed_value("true", xsd::BOOLEAN);
        assert!(matches!(
            result,
            UnresolvedTerm::Literal(LiteralValue::Boolean(true))
        ));
    }

    #[test]
    fn test_blank_node_counter() {
        let mut counter = BlankNodeCounter::new();
        assert_eq!(counter.next(), "_:b0");
        assert_eq!(counter.next(), "_:b1");
        assert_eq!(counter.next(), "_:b2");
    }

    #[test]
    fn test_coerce_typed_flake_value_vector_lexical() {
        // Regression for the vector-corruption bug: a SPARQL typed literal
        // `"[..]"^^f:embeddingVector` must produce FlakeValue::Vector, not
        // FlakeValue::String — otherwise downstream flake gen pairs a String
        // value with the embeddingVector datatype and the index decodes
        // garbage.
        let result = coerce_typed_flake_value("[0.1, 0.2, 0.3, 0.4]", fluree::EMBEDDING_VECTOR);
        match result {
            FlakeValue::Vector(v) => assert_eq!(v.len(), 4),
            other => panic!("expected FlakeValue::Vector, got {other:?}"),
        }
    }

    #[test]
    fn test_coerce_typed_value_vector_lexical_unresolved() {
        // Same coverage on the UnresolvedTerm path used by literal_to_unresolved.
        let result = coerce_typed_value("[0.1, 0.2]", fluree::EMBEDDING_VECTOR);
        match result {
            UnresolvedTerm::Literal(LiteralValue::Vector(v)) => assert_eq!(v.len(), 2),
            other => panic!("expected LiteralValue::Vector, got {other:?}"),
        }
    }
}
