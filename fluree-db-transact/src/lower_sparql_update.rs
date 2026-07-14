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
    AnnotationUnit, AnnotationVerb, BlankNode, BlankNodeValue, GraphPattern, Iri, IriValue,
    Literal, LiteralValue as SparqlLiteralValue, Modify, PredicateTerm, Prologue, PropertyPath,
    QuadData, QuadPattern, QuadPatternElement, QueryBody, ReifierId, SparqlAst, SubjectTerm, Term,
    TriplePattern, UpdateOperation,
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

    /// Blank node in a DELETE context (SPARQL 1.1 Update §19.8 grammar note 8)
    #[error(
        "blank nodes are not allowed in {context}: a blank node denotes a fresh node and can \
         never match existing data (use a variable bound by WHERE, a concrete IRI, or a Fluree \
         stable _:fdb- id)"
    )]
    BlankNodeInDelete {
        context: &'static str,
        span: SourceSpan,
    },
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

/// SPARQL UPDATE context for annotation-tail expansion. Different
/// operations have different blank-node / variable rules per SPARQL
/// 1.1 Update §3.1 and the per-operation table in
/// `docs/concepts/edge-annotations.md` "SPARQL UPDATE rules by
/// operation".
#[derive(Clone, Copy, Debug)]
enum AnnotationExpansionMode {
    /// `INSERT DATA { ... }` — ground triples; blank nodes mint fresh
    /// SIDs; variables forbidden.
    InsertData,
    /// `DELETE DATA { ... }` — ground triples; blank nodes forbidden
    /// (no addressable identity per §3.1.3); variables forbidden.
    DeleteData,
    /// `INSERT { ... } WHERE { ... }` template — per-solution blank
    /// nodes; variables bound by WHERE.
    InsertTemplate,
    /// `DELETE { ... } WHERE { ... }` template — blank nodes
    /// forbidden per §3.1.3; variables bound by WHERE.
    DeleteTemplate,
    /// `DELETE WHERE { ... }` — same triples in WHERE and DELETE
    /// template. Blank nodes act as variables.
    DeleteWhere,
}

impl AnnotationExpansionMode {
    fn rejects_blank_reifier(&self) -> bool {
        matches!(self, Self::DeleteData | Self::DeleteTemplate)
    }
    fn rejects_var_reifier(&self) -> bool {
        matches!(self, Self::InsertData | Self::DeleteData)
    }
    fn rejects_anonymous_block(&self) -> bool {
        // Anonymous `{| |}` (no `~`) mints a blank node, which is
        // forbidden in DELETE DATA / DELETE templates.
        matches!(self, Self::DeleteData | Self::DeleteTemplate)
    }
    fn name(&self) -> &'static str {
        match self {
            Self::InsertData => "INSERT DATA",
            Self::DeleteData => "DELETE DATA",
            Self::InsertTemplate => "INSERT WHERE template",
            Self::DeleteTemplate => "DELETE WHERE template",
            Self::DeleteWhere => "DELETE WHERE",
        }
    }
}

/// Resolve the reifier for one SPARQL annotation unit under a given
/// expansion mode. Returns the `SubjectTerm` representing the
/// reifier — to be used as the subject of the `f:reifies*` and body
/// triples that the expansion emits.
///
/// Mints a fresh blank node when the user wrote either an anonymous
/// block (`{| ... |}` with no `~`) or a bare `~` with no id, in modes
/// where blank nodes are allowed. Rejects the relevant per-mode shapes
/// per the M4.4 contract.
fn resolve_reifier(
    annotation: &AnnotationUnit,
    mode: AnnotationExpansionMode,
    bnodes: &mut BlankNodeCounter,
) -> Result<SubjectTerm, LowerError> {
    match annotation.reifier.as_ref() {
        Some(ReifierId::Iri(iri)) => Ok(SubjectTerm::Iri(iri.clone())),
        Some(ReifierId::BlankNode(bn)) => {
            if mode.rejects_blank_reifier() {
                return Err(LowerError::UnsupportedFeature {
                    feature: blank_in_mode_msg(mode.name()),
                    span: bn.span,
                });
            }
            Ok(SubjectTerm::BlankNode(bn.clone()))
        }
        Some(ReifierId::Var(v)) => {
            if mode.rejects_var_reifier() {
                return Err(LowerError::UnsupportedFeature {
                    feature: var_in_mode_msg(mode.name()),
                    span: v.span,
                });
            }
            Ok(SubjectTerm::Var(v.clone()))
        }
        None => {
            if mode.rejects_anonymous_block() {
                return Err(LowerError::UnsupportedFeature {
                    feature: anon_in_mode_msg(mode.name()),
                    span: annotation.span,
                });
            }
            // Mint a fresh labeled blank node so it round-trips through
            // the existing template lowering exactly like a user-supplied
            // `~ _:foo` would.
            //
            // **Reserved label space.** `BlankNodeCounter::next()`
            // yields plain `_:b{N}` which can collide with
            // user-authored `_:b0` — both would skolemize to the
            // same SID (via `FlakeGenerator::skolemize_blank_node`
            // keying on `(txn_id, label)`), silently fusing two
            // distinct subjects. Prefix the synthesized label with
            // `__fluree_ann_` so it can't conflict with any
            // hand-written blank-node label. The prefix matches the
            // by-selector retract's `?_fluree_del_ann_N` convention
            // and surfaces clearly in any diagnostic output.
            let raw = bnodes.next();
            let stripped = raw.strip_prefix("_:").unwrap_or(&raw);
            let reserved = format!("__fluree_ann_{stripped}");
            Ok(SubjectTerm::BlankNode(BlankNode::labeled(
                reserved,
                annotation.span,
            )))
        }
    }
}

fn blank_in_mode_msg(op: &'static str) -> &'static str {
    // Static slice-leak: thread the operation name in via a small
    // perfect-hash on the variants. We only have five and never grow,
    // so a match suffices.
    match op {
        "DELETE DATA" => "blank-node reifier in DELETE DATA (SPARQL §3.1.3 forbids blanks here)",
        "DELETE WHERE template" => {
            "blank-node reifier in DELETE template (SPARQL §3.1.3 forbids blanks)"
        }
        _ => "blank-node reifier not allowed in this UPDATE context",
    }
}

fn var_in_mode_msg(op: &'static str) -> &'static str {
    match op {
        "INSERT DATA" => "variable reifier in INSERT DATA (SPARQL §3.1.1 forbids variables here)",
        "DELETE DATA" => "variable reifier in DELETE DATA (SPARQL §3.1.1 forbids variables here)",
        _ => "variable reifier not allowed in this UPDATE context",
    }
}

fn anon_in_mode_msg(op: &'static str) -> &'static str {
    match op {
        "DELETE DATA" => {
            "anonymous annotation block ({| |}) in DELETE DATA — no addressable identity to delete"
        }
        "DELETE WHERE template" => {
            "anonymous annotation block ({| |}) in DELETE template — use a named reifier bound by WHERE"
        }
        _ => "anonymous annotation block not allowed in this UPDATE context",
    }
}

/// Expand any annotated triples in a Vec into the equivalent set of
/// unannotated triples: the base triple, the `f:reifies*` bundle
/// (subject/predicate/object only — graph/datatype/lang/listIndex are
/// derived at flake time), and the body's predicate-object pairs.
///
/// Default-graph only in v1; an annotation tail inside a `GRAPH` block
/// is rejected by the caller before this is invoked.
fn expand_annotated_triples(
    triples: &mut Vec<TriplePattern>,
    mode: AnnotationExpansionMode,
    bnodes: &mut BlankNodeCounter,
) -> Result<(), LowerError> {
    use fluree_vocab::reifies_iris;

    let original = std::mem::take(triples);
    let mut out: Vec<TriplePattern> = Vec::with_capacity(original.len());

    for tp in original {
        let Some(annotation) = tp.annotation.clone() else {
            out.push(tp);
            continue;
        };

        // Reject RDF-star quoted-triple subjects explicitly. The
        // legacy `<< s p o >>` quoted-triple form has no compatible
        // representation in the f:reifies* bundle (the base triple
        // would need to embed inside f:reifiesSubject's object slot
        // which violates the bundle shape), and `subject_to_object`
        // below would otherwise hit its `unreachable!()` panic.
        // Surface this as an explicit `UnsupportedFeature` so the
        // user sees a real error rather than a transactor panic.
        if let SubjectTerm::QuotedTriple(qt) = &tp.subject {
            return Err(LowerError::UnsupportedFeature {
                feature: "RDF-star quoted-triple subject combined with an RDF 1.2 \
                          annotation tail (`{| ... |}`) in SPARQL UPDATE",
                span: qt.span,
            });
        }

        // Reify the base edge and emit base + per-unit bundle + body.
        // The base triple stripped of its annotation goes through
        // unchanged; each annotation unit (`~ r? {| … |}?`) contributes
        // its own reifier bundle.
        let span = tp.span;

        // Base triple (without annotation)
        out.push(TriplePattern::new(
            tp.subject.clone(),
            tp.predicate.clone(),
            tp.object.clone(),
            span,
        ));

        for unit in &annotation.units {
            let reifier = resolve_reifier(unit, mode, bnodes)?;

            // f:reifies* bundle: SUBJECT, PREDICATE, OBJECT, and (for a
            // language-tagged object) LANG. f:reifiesGraph is omitted
            // (default graph only) — WITH-scoped templates are rejected
            // upstream by `reject_with_scoped_annotations` so this default
            // identity never gets graph-stamped. f:reifiesDatatype rides on
            // the f:reifiesObject flake's flake-level dt (the decoder derives
            // it), and f:reifiesListIndex is deferred (v1).
            let pred_iri =
                |s: &'static str| -> PredicateTerm { PredicateTerm::Iri(Iri::full(s, span)) };
            out.push(TriplePattern::new(
                reifier.clone(),
                pred_iri(reifies_iris::SUBJECT),
                subject_to_object(&tp.subject),
                span,
            ));
            out.push(TriplePattern::new(
                reifier.clone(),
                pred_iri(reifies_iris::PREDICATE),
                predicate_to_object(&tp.predicate),
                span,
            ));
            out.push(TriplePattern::new(
                reifier.clone(),
                pred_iri(reifies_iris::OBJECT),
                tp.object.clone(),
                span,
            ));

            // f:reifiesLang — required for a language-tagged object.
            // `EdgeKey::from_reifies_facts` reads `lang` from a dedicated
            // f:reifiesLang flake, NOT from the f:reifiesObject flake's
            // `m.lang`. Without this triple the decoded EdgeKey carries
            // `lang = None` while the base edge's EdgeKey carries
            // `lang = Some(tag)`, so the forward-map lookup misses: the
            // annotation silently vanishes from `@annotation` hydration
            // and the bundle is never cascaded on base-edge retract.
            // Mirrors the JSON-LD writer (`build_annotation_sibling`).
            if let Term::Literal(lit) = &tp.object {
                if let SparqlLiteralValue::LangTagged { lang, .. } = &lit.value {
                    out.push(TriplePattern::new(
                        reifier.clone(),
                        pred_iri(reifies_iris::LANG),
                        Term::Literal(Literal::string(lang.as_ref(), span)),
                        span,
                    ));
                }
            }

            // Body entries become (reifier, ann_pred, ann_obj) triples.
            // Property-path verbs (legal in query annotation blocks)
            // have no template meaning — reject with a clear error.
            if let Some(block) = unit.block.as_ref() {
                for entry in &block.entries {
                    let predicate = match &entry.verb {
                        AnnotationVerb::Simple(p) => p.clone(),
                        AnnotationVerb::Path(path) => {
                            return Err(LowerError::UnsupportedFeature {
                                feature: "property path inside an annotation block in \
                                          SPARQL UPDATE (paths cannot be asserted)",
                                span: path.span(),
                            });
                        }
                    };
                    out.push(TriplePattern::new(
                        reifier.clone(),
                        predicate,
                        entry.object.clone(),
                        entry.span,
                    ));
                }
            }
        }
    }

    *triples = out;
    Ok(())
}

/// Convert a SPARQL subject term into the corresponding object term so
/// the `f:reifiesSubject` pointer can carry it. Subjects and objects
/// share the IRI / blank-node / variable cases; literals never appear
/// as subjects so the case is unreachable in practice.
fn subject_to_object(s: &SubjectTerm) -> Term {
    match s {
        SubjectTerm::Var(v) => Term::Var(v.clone()),
        SubjectTerm::Iri(i) => Term::Iri(i.clone()),
        SubjectTerm::BlankNode(b) => Term::BlankNode(b.clone()),
        SubjectTerm::QuotedTriple(_) => {
            unreachable!("RDF-star quoted triples are rejected before annotation expansion")
        }
    }
}

/// Convert a predicate (IRI or var) into the object slot for
/// `f:reifiesPredicate`.
fn predicate_to_object(p: &PredicateTerm) -> Term {
    match p {
        PredicateTerm::Var(v) => Term::Var(v.clone()),
        PredicateTerm::Iri(i) => Term::Iri(i.clone()),
    }
}

/// Walk the QuadPatternElement list and expand every annotated triple
/// in-place. Annotation tails inside a GRAPH block are rejected with a
/// "deferred to a follow-up" message so the v1 default-graph contract
/// stays unambiguous.
fn expand_annotated_triples_in_quad_pattern(
    pattern: &mut QuadPattern,
    mode: AnnotationExpansionMode,
    bnodes: &mut BlankNodeCounter,
) -> Result<(), LowerError> {
    // Two passes so we can replace QuadPatternElement::Triple with
    // multiple expanded Triples without iterator-invalidation gymnastics.
    let mut default_triples: Vec<TriplePattern> = Vec::new();
    let mut graph_blocks: Vec<QuadPatternElement> = Vec::new();
    for el in std::mem::take(&mut pattern.patterns) {
        match el {
            QuadPatternElement::Triple(t) => default_triples.push(*t),
            QuadPatternElement::Graph {
                name,
                triples,
                span,
            } => {
                if triples.iter().any(|t| t.annotation.is_some()) {
                    return Err(LowerError::UnsupportedFeature {
                        feature: "annotation tail inside a GRAPH block in SPARQL UPDATE \
                                  (default-graph only in v1)",
                        span,
                    });
                }
                graph_blocks.push(QuadPatternElement::Graph {
                    name,
                    triples,
                    span,
                });
            }
        }
    }
    expand_annotated_triples(&mut default_triples, mode, bnodes)?;
    let mut out: Vec<QuadPatternElement> = default_triples
        .into_iter()
        .map(|t| QuadPatternElement::Triple(Box::new(t)))
        .collect();
    out.extend(graph_blocks);
    pattern.patterns = out;
    Ok(())
}

/// Reject any user-authored `f:reifies*` predicate in a triple list.
/// Mirrors the JSON-LD path's `run_user_authored_reifies_firewall`. The
/// expansion pass synthesizes legitimate `f:reifies*` triples; this
/// firewall rejects ones the user wrote directly.
///
/// Walks BOTH the top-level triple predicates AND any annotation-block
/// body predicates, so a user can't hide a `f:reifiesSubject` inside
/// `{| f:reifiesSubject ex:evil |}` to bypass the check — expansion
/// would otherwise emit that body predicate as an asserted triple
/// against the reifier.
fn reject_user_authored_reifies(
    triples: &[TriplePattern],
    prologue: &Prologue,
) -> Result<(), LowerError> {
    use fluree_vocab::reifies_iris;

    fn check_predicate(pred: &PredicateTerm, prologue: &Prologue) -> Result<(), LowerError> {
        if let PredicateTerm::Iri(iri) = pred {
            let expanded = expand_iri(iri, prologue)?;
            if reifies_iris::ALL.contains(&expanded.as_str()) {
                return Err(LowerError::UnsupportedFeature {
                    feature: "user-authored f:reifies* predicate in SPARQL UPDATE \
                              (system-controlled — use the `~ {| ... |}` annotation \
                              syntax instead)",
                    span: iri.span,
                });
            }
        }
        Ok(())
    }

    // Path verbs are rejected later by the expansion pass (paths can't
    // be asserted), but the firewall still walks their IRI leaves so a
    // hidden `f:reifies*` leaf is reported as a firewall violation, not
    // as a generic unsupported-path error after partial validation.
    fn check_path(path: &PropertyPath, prologue: &Prologue) -> Result<(), LowerError> {
        match path {
            PropertyPath::Iri(iri) => check_predicate(&PredicateTerm::Iri(iri.clone()), prologue),
            PropertyPath::A { .. } => Ok(()),
            PropertyPath::Inverse { path, .. }
            | PropertyPath::ZeroOrMore { path, .. }
            | PropertyPath::OneOrMore { path, .. }
            | PropertyPath::ZeroOrOne { path, .. }
            | PropertyPath::Group { path, .. } => check_path(path, prologue),
            PropertyPath::Sequence { left, right, .. }
            | PropertyPath::Alternative { left, right, .. } => {
                check_path(left, prologue)?;
                check_path(right, prologue)
            }
            PropertyPath::NegatedSet { iris, .. } => {
                use fluree_db_sparql::ast::NegatedPredicate;
                for p in iris {
                    match p {
                        NegatedPredicate::Forward(iri) | NegatedPredicate::Inverse(iri) => {
                            check_predicate(&PredicateTerm::Iri(iri.clone()), prologue)?;
                        }
                        NegatedPredicate::ForwardA { .. } | NegatedPredicate::InverseA { .. } => {}
                    }
                }
                Ok(())
            }
        }
    }

    for tp in triples {
        check_predicate(&tp.predicate, prologue)?;
        if let Some(ann) = &tp.annotation {
            for unit in &ann.units {
                if let Some(block) = &unit.block {
                    for entry in &block.entries {
                        match &entry.verb {
                            AnnotationVerb::Simple(p) => check_predicate(p, prologue)?,
                            AnnotationVerb::Path(path) => check_path(path, prologue)?,
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Reject non-stable blank nodes in DELETE-side quad patterns (SPARQL 1.1
/// Update §19.8 grammar note 8: no blank nodes in DELETE DATA nor in the
/// DeleteClause template). A blank node here denotes a fresh node, so a
/// retraction built from it skolemizes a brand-new SID and silently matches
/// nothing. Mirrors the validator's `BlankNodeInDelete` rule as
/// defense-in-depth: the fluree-db-api seam now runs `validate()` before
/// lowering, but callers of the public `lower_sparql_update_ast` entry reach
/// lowering directly and still get the same clear error. Matches
/// [`AnnotationExpansionMode::rejects_blank_reifier`]
/// (DeleteData | DeleteTemplate).
///
/// Two deliberate carve-outs:
/// - Fluree stable ids (`_:fdb-...`) pass: they are constants addressing the
///   existing stored node (see `it_stable_blank_nodes.rs`).
/// - DELETE WHERE is NOT routed through this check at lowering: its blank
///   nodes keep Fluree's documented existential-variable semantics
///   ([`BlankNodeVarNamer`]); the strict SPARQL surface rejects them in
///   `validate()`, and the api seam waives exactly that rejection via
///   `Capabilities::with_delete_where_extensions`.
fn reject_blank_nodes_in_delete_quad_pattern(
    pattern: &QuadPattern,
    context: &'static str,
) -> Result<(), LowerError> {
    fn check_blank_node(bn: &BlankNode, context: &'static str) -> Result<(), LowerError> {
        if let BlankNodeValue::Labeled(l) = &bn.value {
            if crate::namespace::stable_blank_node_sid_from_label(l).is_some() {
                return Ok(());
            }
        }
        Err(LowerError::BlankNodeInDelete {
            context,
            span: bn.span,
        })
    }
    fn check_triple(tp: &TriplePattern, context: &'static str) -> Result<(), LowerError> {
        if let SubjectTerm::BlankNode(bn) = &tp.subject {
            check_blank_node(bn, context)?;
        }
        if let Term::BlankNode(bn) = &tp.object {
            check_blank_node(bn, context)?;
        }
        Ok(())
    }
    for el in &pattern.patterns {
        match el {
            QuadPatternElement::Triple(tp) => check_triple(tp, context)?,
            QuadPatternElement::Graph { triples, .. } => {
                for tp in triples {
                    check_triple(tp, context)?;
                }
            }
        }
    }
    Ok(())
}

/// Same firewall, but for QuadPattern (handles Triple + Graph blocks).
fn reject_user_authored_reifies_in_quad_pattern(
    pattern: &QuadPattern,
    prologue: &Prologue,
) -> Result<(), LowerError> {
    for el in &pattern.patterns {
        match el {
            QuadPatternElement::Triple(t) => {
                reject_user_authored_reifies(std::slice::from_ref(t.as_ref()), prologue)?;
            }
            QuadPatternElement::Graph { triples, .. } => {
                reject_user_authored_reifies(triples, prologue)?;
            }
        }
    }
    Ok(())
}

/// Reject RDF 1.2 annotation tails on `WITH <g>`-scoped template triples.
///
/// `WITH <g>` re-homes default-position template triples into `<g>` *after*
/// annotation expansion, but the v1 expansion omits `f:reifiesGraph` — the
/// synthetic bundle encodes a default-graph edge identity. Stamping the WITH
/// graph id over that bundle would mint graph-tagged reifications whose edge
/// identity is still default-graph, so the forward-map lookup misses: the
/// annotation never hydrates and never cascades on base-edge retract. Reject
/// until graph-aware expansion (emitting `f:reifiesGraph`) lands. Annotation
/// tails inside explicit `GRAPH { ... }` blocks are already rejected by
/// [`expand_annotated_triples_in_quad_pattern`]; this covers the top-level
/// (WITH-scoped) triples it would otherwise expand as default-graph.
fn reject_with_scoped_annotations(pattern: &QuadPattern) -> Result<(), LowerError> {
    for el in &pattern.patterns {
        if let QuadPatternElement::Triple(tp) = el {
            if tp.annotation.is_some() {
                return Err(LowerError::UnsupportedFeature {
                    feature: "RDF 1.2 annotation tail (`{| ... |}`) on a WITH-scoped \
                              SPARQL UPDATE template (SPARQL UPDATE annotations are \
                              default-graph only; use the JSON-LD @annotation surface to \
                              annotate an edge in a named graph)",
                    span: tp.span,
                });
            }
        }
    }
    Ok(())
}

/// Assign stable variable names for SPARQL blank nodes when lowering
/// DELETE WHERE forms — the triple-only fast path directly, and the
/// GRAPH-bearing path via [`rewrite_blank_nodes_to_vars`].
///
/// In SPARQL graph patterns, blank node labels behave like locally-scoped
/// existential variables; lowering rewrites them to query variables with
/// reserved names. Two invariants keep the reserved namespace airtight:
///
/// - Every name starts with `_:`, which can never be lexed as a SPARQL
///   variable (`?`/`$` names cannot contain `:`), so no user-written
///   variable can unify with a rewritten existential. The names live only
///   as opaque registry keys; nothing re-lexes them.
/// - The anonymous arm mints `_:[]{N}`: `[` is illegal in a
///   BLANK_NODE_LABEL, so anonymous names are disjoint from the labeled
///   arm's `_:{label}` for every possible label. (Plain `_:b{N}` collided
///   with a user-written `_:b0`, silently unifying two distinct
///   existentials — the same label-space hazard the annotation expansion
///   avoids with its `__fluree_ann_` prefix.)
///
/// Same label ⇒ same name, per SPARQL's blank-node label scoping within a
/// request.
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
                let name: Arc<str> = Arc::from(format!("_:[]{}", self.anon_counter));
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

/// Lower a parsed SPARQL UPDATE request to a sequence of Transaction IRs,
/// one per `;`-separated operation, in request order.
///
/// Each operation is lowered against the prologue in effect for it (the
/// grammar's accumulating `PREFIX`/`BASE` scope). The returned `Txn`s must
/// be staged **sequentially** — SPARQL 1.1 Update §3.1 requires operation
/// N+1 to observe the graph-store state operation N produced — and
/// committed as ONE atomic commit (roadmap decision D-10). An empty vector
/// (empty or prologue-only request) is a valid no-op.
///
/// # Errors
///
/// Returns `LowerError` if:
/// - The AST body is not an UPDATE request (is a query)
/// - WITH or USING clauses are present
/// - Blank nodes appear in WHERE patterns
/// - RDF-star quoted triples are used
pub fn lower_sparql_update_request(
    ast: &SparqlAst,
    ns: &mut NamespaceRegistry,
    opts: TxnOpts,
) -> Result<Vec<Txn>, LowerError> {
    let request = match &ast.body {
        QueryBody::Update(request) => request,
        _ => {
            return Err(LowerError::NotAnUpdate { span: ast.span });
        }
    };
    let mut txns = Vec::with_capacity(request.operations.len());
    for op in &request.operations {
        txns.push(lower_sparql_update(
            &op.operation,
            &op.prologue,
            ns,
            opts.clone(),
        )?);
    }
    Ok(txns)
}

/// Lower a parsed **single-operation** SPARQL AST to the Transaction IR.
///
/// This is a convenience wrapper over [`lower_sparql_update_request`] for
/// the common one-operation request. It fails loudly on a request with
/// zero or multiple operations — callers that accept the full request
/// grammar (the API transaction seam) must use
/// [`lower_sparql_update_request`] and stage the sequence.
///
/// # Errors
///
/// Returns `LowerError` if:
/// - The AST body is not an UPDATE request (is a query)
/// - The request does not contain exactly one operation
/// - WITH or USING clauses are present
/// - Blank nodes appear in WHERE patterns
/// - RDF-star quoted triples are used
pub fn lower_sparql_update_ast(
    ast: &SparqlAst,
    ns: &mut NamespaceRegistry,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let request = match &ast.body {
        QueryBody::Update(request) => request,
        _ => {
            return Err(LowerError::NotAnUpdate { span: ast.span });
        }
    };
    match request.operations.as_slice() {
        [op] => lower_sparql_update(&op.operation, &op.prologue, ns, opts),
        [] => Err(LowerError::UnsupportedFeature {
            feature: "empty update request (no operation) in single-operation lowering; \
                      use lower_sparql_update_request",
            span: ast.span,
        }),
        _ => Err(LowerError::UnsupportedFeature {
            feature: "multi-operation update request in single-operation lowering; \
                      use lower_sparql_update_request",
            span: ast.span,
        }),
    }
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
/// INSERT DATA contains ground quads (no variables) that are directly inserted.
/// `GRAPH <iri> { ... }` blocks route their triples into the named graph,
/// registering it via `graph_delta` (same machinery as DELETE/INSERT ... WHERE).
fn lower_insert_data(
    data: &QuadData,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    // M4.4: reject user-authored f:reifies* IRIs and expand any RDF 1.2
    // annotation tails before lowering. Route through a QuadPattern so the
    // same firewall + expansion used by INSERT ... WHERE applies, while
    // GRAPH blocks still lower into their named graphs.
    let mut pattern = QuadPattern::new(data.quads.clone(), data.span);
    reject_user_authored_reifies_in_quad_pattern(&pattern, prologue)?;
    expand_annotated_triples_in_quad_pattern(
        &mut pattern,
        AnnotationExpansionMode::InsertData,
        bnodes,
    )?;
    let mut graph_ids = TemplateGraphIds::new();
    let insert_templates = lower_quad_pattern_to_templates(
        &pattern.patterns,
        prologue,
        ns,
        vars,
        bnodes,
        &mut graph_ids,
        None,
    )?;

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
        graph_delta: graph_ids.delta(),
        namespace_delta: std::collections::HashMap::new(),
    })
}

/// Lower DELETE DATA operation.
///
/// DELETE DATA contains ground quads (no variables) that are retracted.
/// `GRAPH <iri> { ... }` blocks scope the retraction to the named graph.
/// Uses TxnType::Update because it's a retract-only transaction.
fn lower_delete_data(
    data: &QuadData,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    // M4.4: same firewall + expansion as INSERT DATA, with the
    // DELETE DATA blank-node / variable rejections per SPARQL §3.1.3.
    let mut pattern = QuadPattern::new(data.quads.clone(), data.span);
    reject_blank_nodes_in_delete_quad_pattern(&pattern, "DELETE DATA")?;
    reject_user_authored_reifies_in_quad_pattern(&pattern, prologue)?;
    expand_annotated_triples_in_quad_pattern(
        &mut pattern,
        AnnotationExpansionMode::DeleteData,
        bnodes,
    )?;
    let mut graph_ids = TemplateGraphIds::new();
    let delete_templates = lower_quad_pattern_to_templates(
        &pattern.patterns,
        prologue,
        ns,
        vars,
        bnodes,
        &mut graph_ids,
        None,
    )?;

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
        graph_delta: graph_ids.delta(),
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
    // M4.4: reject user-authored f:reifies* and expand annotation tails.
    reject_user_authored_reifies_in_quad_pattern(pattern, prologue)?;
    let mut expanded_pattern = pattern.clone();
    let mut local_bnodes = BlankNodeCounter::new();
    expand_annotated_triples_in_quad_pattern(
        &mut expanded_pattern,
        AnnotationExpansionMode::DeleteWhere,
        &mut local_bnodes,
    )?;

    // `GRAPH <iri> { ... }` blocks route through the same Modify machinery
    // that DELETE/INSERT ... WHERE uses (staging-time SPARQL WHERE lowering +
    // graph-scoped delete templates). The triple-only fast path below stays
    // byte-identical for patterns without GRAPH blocks.
    if expanded_pattern
        .patterns
        .iter()
        .any(|el| matches!(el, QuadPatternElement::Graph { .. }))
    {
        return lower_delete_where_with_graphs(&expanded_pattern, prologue, ns, vars, opts);
    }

    let triples: Vec<&TriplePattern> = expanded_pattern
        .patterns
        .iter()
        .map(|el| match el {
            QuadPatternElement::Triple(t) => t.as_ref(),
            QuadPatternElement::Graph { .. } => {
                unreachable!("GRAPH-bearing DELETE WHERE handled above")
            }
        })
        .collect();

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

/// Lower a GRAPH-bearing DELETE WHERE through the Modify machinery.
///
/// `DELETE WHERE { P }` is shorthand for `DELETE { P } WHERE { P }`, so a quad
/// pattern with `GRAPH <iri>` blocks lowers exactly like the equivalent Modify:
/// the WHERE side becomes a stored [`SparqlWhereClause`] (lowered at staging
/// time by the shared query engine, which already evaluates GRAPH blocks) and
/// the DELETE side becomes graph-scoped templates via
/// [`lower_quad_pattern_to_templates`].
///
/// Blank nodes keep the same existential-variable semantics as the triple-only
/// path: non-stable blank nodes are rewritten to reserved variables shared by
/// the WHERE pattern and the DELETE templates, while stable `_:fdb-` ids pass
/// through both lowerings as constants addressing the stored node.
fn lower_delete_where_with_graphs(
    pattern: &QuadPattern,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let rewritten = rewrite_blank_nodes_to_vars(pattern);

    let sparql_where = SparqlWhereClause {
        prologue: prologue.clone(),
        with_graph_iri: None,
        using_default_graph_iris: Vec::new(),
        using_named_graph_iris: Vec::new(),
        pattern: quad_pattern_to_graph_pattern(&rewritten),
    };

    let mut graph_ids = TemplateGraphIds::new();
    // Blank nodes were rewritten to variables above, so the counter is only a
    // signature requirement here — the template lowering never mints from it.
    let mut bnodes = BlankNodeCounter::new();
    let delete_templates = lower_quad_pattern_to_templates(
        &rewritten.patterns,
        prologue,
        ns,
        vars,
        &mut bnodes,
        &mut graph_ids,
        None,
    )?;

    Ok(Txn {
        txn_type: TxnType::Update,
        where_patterns: Vec::new(),
        sparql_where: Some(sparql_where),
        delete_templates,
        insert_templates: Vec::new(),
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

/// Rewrite non-stable blank nodes in a quad pattern into reserved variables.
///
/// SPARQL blank nodes in a graph pattern are locally-scoped existential
/// variables. Because a DELETE WHERE pattern is used as both the WHERE clause
/// and the DELETE template, the same variable must appear on both sides, so
/// the rewrite happens once on the shared AST. Naming comes from the shared
/// [`BlankNodeVarNamer`] — the same scheme the triple-only DELETE WHERE path
/// ships — whose `_:`-prefixed names cannot be lexed as user variables (the
/// previous lexable `_fluree_bn_{label}` names let a user-written
/// `?_fluree_bn_x` in the same request unify with the rewritten existential,
/// changing which triples get deleted). Both the template lowering and the
/// staging-time SPARQL WHERE lowering intern the name as an opaque
/// `"?" + name` registry key; nothing re-lexes it. Stable `_:fdb-` ids are
/// left untouched: they denote the stored node as a constant in both the
/// WHERE lowering and the template lowering.
fn rewrite_blank_nodes_to_vars(pattern: &QuadPattern) -> QuadPattern {
    use fluree_db_sparql::ast::Var;

    let mut namer = BlankNodeVarNamer::new();
    let mut rewrite_bnode = |bn: &BlankNode| -> Option<Var> {
        if let BlankNodeValue::Labeled(l) = &bn.value {
            if crate::namespace::stable_blank_node_sid_from_label(l).is_some() {
                return None;
            }
        }
        Some(Var::new(namer.var_name(&bn.value), bn.span))
    };

    let mut rewrite_triple = |tp: &TriplePattern| -> TriplePattern {
        let mut out = tp.clone();
        if let SubjectTerm::BlankNode(bn) = &tp.subject {
            if let Some(v) = rewrite_bnode(bn) {
                out.subject = SubjectTerm::Var(v);
            }
        }
        if let Term::BlankNode(bn) = &tp.object {
            if let Some(v) = rewrite_bnode(bn) {
                out.object = Term::Var(v);
            }
        }
        out
    };

    let patterns = pattern
        .patterns
        .iter()
        .map(|el| match el {
            QuadPatternElement::Triple(tp) => {
                QuadPatternElement::Triple(Box::new(rewrite_triple(tp)))
            }
            QuadPatternElement::Graph {
                name,
                triples,
                span,
            } => QuadPatternElement::Graph {
                name: name.clone(),
                triples: triples.iter().map(&mut rewrite_triple).collect(),
                span: *span,
            },
        })
        .collect();

    QuadPattern::new(patterns, pattern.span)
}

/// Build the `GroupGraphPattern` equivalent of a quad pattern for WHERE use.
///
/// Runs of default-graph triples become one BGP; each `GRAPH <iri>|?g { ... }`
/// block becomes a `GraphPattern::Graph` wrapping its own BGP. Source order is
/// preserved so bindings join exactly as the user wrote them.
fn quad_pattern_to_graph_pattern(pattern: &QuadPattern) -> GraphPattern {
    let span = pattern.span;
    let mut parts: Vec<GraphPattern> = Vec::new();
    let mut bgp: Vec<TriplePattern> = Vec::new();

    for el in &pattern.patterns {
        match el {
            QuadPatternElement::Triple(tp) => bgp.push((**tp).clone()),
            QuadPatternElement::Graph {
                name,
                triples,
                span: g_span,
            } => {
                if !bgp.is_empty() {
                    parts.push(GraphPattern::Bgp {
                        patterns: std::mem::take(&mut bgp),
                        span,
                    });
                }
                parts.push(GraphPattern::Graph {
                    name: name.clone(),
                    pattern: Box::new(GraphPattern::Bgp {
                        patterns: triples.clone(),
                        span: *g_span,
                    }),
                    span: *g_span,
                });
            }
        }
    }
    if !bgp.is_empty() {
        parts.push(GraphPattern::Bgp {
            patterns: bgp,
            span,
        });
    }

    if parts.len() == 1 {
        parts.pop().expect("len checked")
    } else {
        GraphPattern::Group {
            patterns: parts,
            span,
        }
    }
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

    // M4.4: pre-expand annotation tails in DELETE / INSERT templates
    // before they're lowered. The user-authored-reifies firewall runs
    // first so synthetic f:reifies* triples (added by the expansion)
    // aren't mistaken for user input.
    let delete_templates = if let Some(delete_clause) = &modify.delete_clause {
        reject_blank_nodes_in_delete_quad_pattern(delete_clause, "DELETE templates")?;
        reject_user_authored_reifies_in_quad_pattern(delete_clause, prologue)?;
        if default_template_graph_id.is_some() {
            reject_with_scoped_annotations(delete_clause)?;
        }
        let mut expanded = delete_clause.clone();
        expand_annotated_triples_in_quad_pattern(
            &mut expanded,
            AnnotationExpansionMode::DeleteTemplate,
            bnodes,
        )?;
        lower_quad_pattern_to_templates(
            &expanded.patterns,
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

    let insert_templates = if let Some(insert_clause) = &modify.insert_clause {
        reject_user_authored_reifies_in_quad_pattern(insert_clause, prologue)?;
        if default_template_graph_id.is_some() {
            reject_with_scoped_annotations(insert_clause)?;
        }
        let mut expanded = insert_clause.clone();
        expand_annotated_triples_in_quad_pattern(
            &mut expanded,
            AnnotationExpansionMode::InsertTemplate,
            bnodes,
        )?;
        lower_quad_pattern_to_templates(
            &expanded.patterns,
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
    elements: &[QuadPatternElement],
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    graph_ids: &mut TemplateGraphIds,
    default_graph_id: Option<u16>,
) -> Result<Vec<TripleTemplate>, LowerError> {
    let mut out: Vec<TripleTemplate> = Vec::new();
    for el in elements {
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
        SubjectTerm::BlankNode(bn) => {
            // Stable Fluree blank-node ids match the stored node as a
            // constant; other labels act as existential variables (§3.1).
            if let BlankNodeValue::Labeled(l) = &bn.value {
                if crate::namespace::stable_blank_node_sid_from_label(l).is_some() {
                    return Ok(UnresolvedTerm::Iri(Arc::from(format!("_:{l}"))));
                }
            }
            Ok(UnresolvedTerm::Var(bnodes.var_name(&bn.value)))
        }
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
        Term::BlankNode(bn) => {
            // Stable Fluree blank-node ids match as constants
            // (see subject_to_unresolved_delete_where).
            if let BlankNodeValue::Labeled(l) = &bn.value {
                if crate::namespace::stable_blank_node_sid_from_label(l).is_some() {
                    return Ok(UnresolvedTermWithMeta {
                        term: UnresolvedTerm::Iri(Arc::from(format!("_:{l}"))),
                        dtc: None,
                    });
                }
            }
            Ok(UnresolvedTermWithMeta {
                term: UnresolvedTerm::Var(bnodes.var_name(&bn.value)),
                dtc: None,
            })
        }
        Term::QuotedTriple(qt) => Err(LowerError::UnsupportedFeature {
            feature: "RDF 1.2 reified triple (`<< s p o >>`) in SPARQL UPDATE (deferred)",
            span: qt.span,
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
            let stable = match &bn.value {
                BlankNodeValue::Labeled(l) => crate::namespace::stable_blank_node_sid_from_label(l),
                BlankNodeValue::Anon => None,
            };
            match stable {
                // Stable Fluree blank-node ids retract from the stored node.
                Some(sid) => TemplateTerm::Sid(sid),
                None => {
                    let name = bnodes.var_name(&bn.value);
                    TemplateTerm::Var(vars.get_or_insert(&name))
                }
            }
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
            let stable = match &bn.value {
                BlankNodeValue::Labeled(l) => crate::namespace::stable_blank_node_sid_from_label(l),
                BlankNodeValue::Anon => None,
            };
            match stable {
                Some(sid) => (TemplateTerm::Sid(sid), None),
                None => {
                    let name = bnodes.var_name(&bn.value);
                    (TemplateTerm::Var(vars.get_or_insert(&name)), None)
                }
            }
        }
        Term::QuotedTriple(qt) => {
            return Err(LowerError::UnsupportedFeature {
                feature: "RDF 1.2 reified triple (`<< s p o >>`) in SPARQL UPDATE (deferred)",
                span: qt.span,
            });
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
        SparqlLiteralValue::BigInteger(s) => {
            let term = match s.parse::<num_bigint::BigInt>() {
                Ok(n) => UnresolvedTerm::Literal(LiteralValue::BigInt(Box::new(n))),
                Err(_) => UnresolvedTerm::Literal(LiteralValue::String(Arc::from(s.as_ref()))),
            };
            Ok(UnresolvedTermWithMeta {
                term,
                dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                    xsd::INTEGER,
                ))),
            })
        }
        SparqlLiteralValue::Double(d) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::Double(*d)),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                xsd::DOUBLE,
            ))),
        }),
        SparqlLiteralValue::Decimal(s) => {
            // Parse exactly; on failure, keep as string with datatype
            let term = match s.parse::<bigdecimal::BigDecimal>() {
                Ok(d) => UnresolvedTerm::Literal(LiteralValue::Decimal(Box::new(d))),
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
                BlankNodeValue::Labeled(l) => {
                    // Stable Fluree blank-node ids address the existing
                    // stored node instead of skolemizing a fresh one, which
                    // makes DELETE templates on them meaningful.
                    if let Some(sid) = crate::namespace::stable_blank_node_sid_from_label(l) {
                        return Ok(TemplateTerm::Sid(sid));
                    }
                    format!("_:{l}")
                }
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
                BlankNodeValue::Labeled(l) => {
                    // Stable Fluree blank-node ids resolve to the stored node
                    // (see subject_to_template).
                    if let Some(sid) = crate::namespace::stable_blank_node_sid_from_label(l) {
                        return Ok(TemplateTerm::Sid(sid));
                    }
                    format!("_:{l}")
                }
                BlankNodeValue::Anon => bnodes.next(),
            };
            Ok(TemplateTerm::BlankNode(label))
        }
        Term::QuotedTriple(qt) => Err(LowerError::UnsupportedFeature {
            feature: "RDF 1.2 reified triple (`<< s p o >>`) in SPARQL UPDATE (deferred)",
            span: qt.span,
        }),
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
        SparqlLiteralValue::BigInteger(s) => {
            let term = match s.parse::<num_bigint::BigInt>() {
                Ok(n) => TemplateTerm::Value(FlakeValue::BigInt(Box::new(n))),
                Err(_) => TemplateTerm::Value(FlakeValue::String(s.to_string())),
            };
            Ok(LiteralResult {
                term,
                dtc: Some(DatatypeConstraint::Explicit(ns.sid_for_iri(xsd::INTEGER))),
            })
        }
        SparqlLiteralValue::Double(d) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::Double(*d)),
            dtc: Some(DatatypeConstraint::Explicit(ns.sid_for_iri(xsd::DOUBLE))),
        }),
        SparqlLiteralValue::Decimal(s) => {
            // Parse exactly; on failure, keep as string with datatype
            let term = match s.parse::<bigdecimal::BigDecimal>() {
                Ok(d) => TemplateTerm::Value(FlakeValue::Decimal(Box::new(d))),
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

/// Expand a constant IRI using the operation's prologue: prefixed names
/// expand against PREFIX declarations, and relative references resolve
/// against `BASE` — the same `fluree_vocab::iri`-backed semantics as the
/// query path's `expand_iri_with` (RFC 3986 §5; SPARQL 1.1 §4.1.1) — so one
/// document's constant IRIs denote the same absolute IRI on the update and
/// query surfaces. `BASE <http://x/> INSERT DATA { <s1> <p1> "v" }` used to
/// commit literal `s1`/`p1` that a query for `<http://x/s1>` could never
/// find. Without a BASE, relative references stay as written (Fluree
/// accepts them as ledger-local names).
fn expand_iri(iri: &Iri, prologue: &Prologue) -> Result<String, LowerError> {
    let base: Option<&str> = prologue.base.as_ref().map(|b| b.iri.as_ref());
    match &iri.value {
        IriValue::Full(full) => {
            if let Some(base) = base {
                if !fluree_vocab::iri::is_absolute_iri(full) {
                    return Ok(fluree_vocab::iri::resolve_iri(base, full));
                }
            }
            Ok(full.to_string())
        }
        IriValue::Prefixed { prefix, local } => {
            // Look up prefix in prologue (first declaration wins, matching
            // `Prologue::get_prefix`)
            for decl in &prologue.prefixes {
                if decl.prefix.as_ref() == prefix.as_ref() {
                    // A PREFIX namespace may itself be a relative reference
                    // (`PREFIX : <#>`); per SPARQL 1.1 §4.1.1 it resolves
                    // against BASE too.
                    let expanded = match base {
                        Some(base) if !fluree_vocab::iri::is_absolute_iri(&decl.iri) => {
                            format!(
                                "{}{}",
                                fluree_vocab::iri::resolve_iri(base, &decl.iri),
                                local
                            )
                        }
                        _ => format!("{}{}", decl.iri, local),
                    };
                    return Ok(expanded);
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
            // xsd:integer is unbounded: promote past i64 instead of falling
            // back to a string-valued literal.
            if let Ok(i) = lexical.parse::<i64>() {
                return UnresolvedTerm::Literal(LiteralValue::Long(i));
            }
            if let Ok(n) = lexical.parse::<num_bigint::BigInt>() {
                return UnresolvedTerm::Literal(LiteralValue::BigInt(Box::new(n)));
            }
        }
        xsd::DOUBLE => {
            if let Ok(d) = lexical.parse::<f64>() {
                return UnresolvedTerm::Literal(LiteralValue::Double(d));
            }
        }
        xsd::DECIMAL => {
            if let Ok(d) = lexical.parse::<bigdecimal::BigDecimal>() {
                return UnresolvedTerm::Literal(LiteralValue::Decimal(Box::new(d)));
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
                return UnresolvedTerm::Literal(LiteralValue::Vector(v.to_vec()));
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
            // xsd:integer is unbounded: promote past i64 instead of falling
            // back to a string-valued literal.
            if let Ok(i) = lexical.parse::<i64>() {
                return FlakeValue::Long(i);
            }
            if let Ok(n) = lexical.parse::<num_bigint::BigInt>() {
                return FlakeValue::BigInt(Box::new(n));
            }
        }
        xsd::DOUBLE => {
            if let Ok(d) = lexical.parse::<f64>() {
                return FlakeValue::Double(d);
            }
        }
        xsd::DECIMAL => {
            if let Ok(d) = lexical.parse::<bigdecimal::BigDecimal>() {
                return FlakeValue::Decimal(Box::new(d));
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

    fn test_prologue_with_base(base: &str) -> Prologue {
        Prologue {
            base: Some(fluree_db_sparql::ast::BaseDecl::new(base, test_span())),
            prefixes: vec![
                PrefixDecl {
                    prefix: Arc::from("ex"),
                    iri: Arc::from("http://example.org/"),
                    span: test_span(),
                },
                PrefixDecl {
                    prefix: Arc::from("rel"),
                    iri: Arc::from("#"),
                    span: test_span(),
                },
            ],
        }
    }

    #[test]
    fn test_expand_relative_iri_against_base() {
        // PR-1454 review: the update surface must resolve constant IRIs
        // against BASE with the same RFC 3986 §5 semantics as the query
        // surface — `BASE <…> INSERT DATA { <s1> … }` used to store the
        // literal `s1` that a query for the resolved IRI could never find.
        let prologue = test_prologue_with_base("http://x.example/dir/doc");
        for (input, expected) in [
            ("s1", "http://x.example/dir/s1"),
            ("", "http://x.example/dir/doc"),
            ("#frag", "http://x.example/dir/doc#frag"),
            ("/rooted", "http://x.example/rooted"),
            ("../up", "http://x.example/up"),
        ] {
            let iri = Iri::full(input, test_span());
            assert_eq!(
                expand_iri(&iri, &prologue).unwrap(),
                expected,
                "for <{input}>"
            );
        }
    }

    #[test]
    fn test_expand_absolute_iri_ignores_base() {
        // Any valid scheme passes through verbatim — including non-`://`
        // forms like `urn:` / `did:`.
        let prologue = test_prologue_with_base("http://x.example/");
        for absolute in ["http://other.example/a", "urn:uuid:abc", "did:key:xyz"] {
            let iri = Iri::full(absolute, test_span());
            assert_eq!(expand_iri(&iri, &prologue).unwrap(), absolute);
        }
    }

    #[test]
    fn test_expand_relative_prefix_namespace_against_base() {
        // A PREFIX namespace that is itself a relative reference resolves
        // against BASE (SPARQL 1.1 §4.1.1) before local-name concatenation
        // — same as the query path's `prologue_environment`.
        let prologue = test_prologue_with_base("http://x.example/doc");
        let iri = Iri::prefixed("rel", "x", test_span());
        assert_eq!(
            expand_iri(&iri, &prologue).unwrap(),
            "http://x.example/doc#x"
        );
    }

    #[test]
    fn test_expand_relative_iri_without_base_stays_as_written() {
        // Fluree accepts relative references as ledger-local names when no
        // BASE is declared.
        let iri = Iri::full("ledger-local", test_span());
        let prologue = test_prologue();
        assert_eq!(expand_iri(&iri, &prologue).unwrap(), "ledger-local");
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
    fn test_lower_insert_data_graph_block_registers_named_graph() {
        // Issue #1288: INSERT DATA { GRAPH <g> { ... } } must lower into
        // graph-tagged templates plus a graph_delta registering the named graph.
        let parsed = fluree_db_sparql::parse_sparql(
            "INSERT DATA { GRAPH <urn:g1> { <http://example.org/s> <http://example.org/p> \"v\" } }",
        );
        assert!(
            !parsed.has_errors(),
            "parse errors: {:?}",
            parsed.diagnostics
        );
        let ast = parsed.ast.expect("AST");

        let mut ns = NamespaceRegistry::new();
        let txn = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default()).expect("lower");

        assert_eq!(txn.txn_type, TxnType::Insert);
        assert_eq!(txn.insert_templates.len(), 1);
        assert!(
            txn.insert_templates[0].graph_id.is_some(),
            "template should carry a txn-local graph id"
        );
        // The named graph IRI must be registered in graph_delta.
        assert!(
            txn.graph_delta.values().any(|iri| iri == "urn:g1"),
            "graph_delta must register <urn:g1>, got {:?}",
            txn.graph_delta
        );
    }

    #[test]
    fn test_lower_delete_where_graph_block_uses_modify_machinery() {
        // DELETE WHERE { GRAPH <g> { ... } } routes through the same
        // staging-time SPARQL WHERE + graph-scoped template lowering as
        // DELETE/INSERT ... WHERE (W3C dawg-delete-where-02/04/06).
        let parsed = fluree_db_sparql::parse_sparql(
            "DELETE WHERE { GRAPH <urn:g1> { <http://example.org/a> <http://example.org/knows> ?b } }",
        );
        assert!(
            !parsed.has_errors(),
            "parse errors: {:?}",
            parsed.diagnostics
        );
        let ast = parsed.ast.expect("AST");

        let mut ns = NamespaceRegistry::new();
        let txn = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default()).expect("lower");

        assert_eq!(txn.txn_type, TxnType::Update);
        assert!(
            txn.sparql_where.is_some(),
            "GRAPH-bearing DELETE WHERE must store a SPARQL WHERE for staging-time lowering"
        );
        assert!(txn.where_patterns.is_empty());
        assert_eq!(txn.delete_templates.len(), 1);
        assert!(
            txn.delete_templates[0].graph_id.is_some(),
            "delete template should carry a txn-local graph id"
        );
        assert!(
            txn.graph_delta.values().any(|iri| iri == "urn:g1"),
            "graph_delta must register <urn:g1>, got {:?}",
            txn.graph_delta
        );
    }

    #[test]
    fn test_lower_delete_where_triple_only_path_unchanged() {
        // Patterns without GRAPH blocks stay on the triple-only fast path:
        // unresolved WHERE patterns, no stored SPARQL WHERE clause.
        let parsed = fluree_db_sparql::parse_sparql(
            "DELETE WHERE { <http://example.org/a> <http://example.org/knows> ?b }",
        );
        assert!(
            !parsed.has_errors(),
            "parse errors: {:?}",
            parsed.diagnostics
        );
        let ast = parsed.ast.expect("AST");

        let mut ns = NamespaceRegistry::new();
        let txn = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default()).expect("lower");

        assert!(txn.sparql_where.is_none());
        assert_eq!(txn.where_patterns.len(), 1);
        assert_eq!(txn.delete_templates.len(), 1);
        assert!(txn.delete_templates[0].graph_id.is_none());
    }

    #[test]
    fn test_lower_delete_where_graph_block_rewrites_blank_nodes_to_vars() {
        // Blank nodes keep existential-variable semantics on the GRAPH path,
        // with the same variable shared by WHERE and the delete template.
        let parsed = fluree_db_sparql::parse_sparql(
            "DELETE WHERE { GRAPH <urn:g1> { _:x <http://example.org/knows> ?b } }",
        );
        assert!(
            !parsed.has_errors(),
            "parse errors: {:?}",
            parsed.diagnostics
        );
        let ast = parsed.ast.expect("AST");

        let mut ns = NamespaceRegistry::new();
        let txn = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default()).expect("lower");

        let template = &txn.delete_templates[0];
        assert!(
            matches!(template.subject, TemplateTerm::Var(_)),
            "blank-node subject must lower to an existential variable, got {:?}",
            template.subject
        );
        let sparql_where = txn.sparql_where.as_ref().expect("sparql where");
        let rendered = format!("{:?}", sparql_where.pattern);
        assert!(
            rendered.contains("_:x"),
            "WHERE pattern must reference the shared existential var: {rendered}"
        );
    }

    #[test]
    fn test_delete_where_bnode_vars_cannot_collide_with_user_vars() {
        // pr-1453 review: the GRAPH path used to mint LEXABLE reserved names
        // (`_fluree_bn_x`), so a user-written ?_fluree_bn_x in the same
        // request silently unified with the rewritten existential, changing
        // which triples get deleted. The shared `_:`-scheme names cannot be
        // written as SPARQL variables.
        let parsed = fluree_db_sparql::parse_sparql(
            "DELETE WHERE { GRAPH <urn:g1> { _:x <http://example.org/p> ?_fluree_bn_x } }",
        );
        assert!(
            !parsed.has_errors(),
            "parse errors: {:?}",
            parsed.diagnostics
        );
        let ast = parsed.ast.expect("AST");
        let mut ns = NamespaceRegistry::new();
        let txn = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default()).expect("lower");
        let template = &txn.delete_templates[0];
        let (TemplateTerm::Var(subject), TemplateTerm::Var(object)) =
            (&template.subject, &template.object)
        else {
            panic!("both positions must lower to variables: {template:?}");
        };
        assert_ne!(
            subject, object,
            "the rewritten existential must not unify with the user's variable"
        );
    }

    #[test]
    fn test_delete_where_anon_bnode_vars_disjoint_from_labels() {
        // A user-written label `_:b0` and an anonymous `[]` are distinct
        // existentials. The old `_:b{N}` anonymous names lived inside the
        // label namespace, so `[]` unified with a user's `_:b0` — deleting
        // by the JOIN of two patterns that the spec treats independently.
        // The `_:[]{N}` anonymous namespace cannot be produced by any
        // BLANK_NODE_LABEL. Pin both DELETE WHERE paths.
        for input in [
            // Triple-only fast path (BlankNodeVarNamer used directly).
            "DELETE WHERE { _:b0 <http://example.org/p> ?o . [] <http://example.org/q> ?r }",
            // GRAPH path (rewrite_blank_nodes_to_vars, shared namer).
            "DELETE WHERE { GRAPH <urn:g1> { _:b0 <http://example.org/p> ?o . \
             [] <http://example.org/q> ?r } }",
        ] {
            let parsed = fluree_db_sparql::parse_sparql(input);
            assert!(
                !parsed.has_errors(),
                "parse errors for {input}: {:?}",
                parsed.diagnostics
            );
            let ast = parsed.ast.expect("AST");
            let mut ns = NamespaceRegistry::new();
            let txn = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default()).expect("lower");
            let TemplateTerm::Var(labeled) = &txn.delete_templates[0].subject else {
                panic!("labeled bnode must lower to a variable ({input})");
            };
            let TemplateTerm::Var(anon) = &txn.delete_templates[1].subject else {
                panic!("anonymous bnode must lower to a variable ({input})");
            };
            assert_ne!(
                labeled, anon,
                "a user label _:b0 must not unify with an anonymous [] existential ({input})"
            );
        }
    }

    #[test]
    fn test_lower_delete_data_blank_node_rejected() {
        // SPARQL 1.1 Update §19.8 note 8: no blank nodes in DELETE DATA.
        // Enforced at lowering too, as defense-in-depth for direct
        // lower_sparql_update_ast callers (the api seam also rejects this
        // via validate() before lowering).
        let parsed =
            fluree_db_sparql::parse_sparql("DELETE DATA { _:a <http://example.org/p> <urn:o> }");
        let ast = parsed.ast.expect("AST");
        let mut ns = NamespaceRegistry::new();
        let err = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
            .expect_err("bnode in DELETE DATA must be rejected");
        assert!(
            matches!(
                err,
                LowerError::BlankNodeInDelete {
                    context: "DELETE DATA",
                    ..
                }
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_lower_modify_delete_template_blank_node_rejected() {
        // Anonymous `[]` in a Modify DELETE template (W3C delete-insert-03
        // family shape).
        let parsed = fluree_db_sparql::parse_sparql(
            "DELETE { ?a <http://example.org/knows> [] } WHERE { ?a <http://example.org/name> \"Alan\" }",
        );
        let ast = parsed.ast.expect("AST");
        let mut ns = NamespaceRegistry::new();
        let err = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
            .expect_err("bnode in a DELETE template must be rejected");
        assert!(
            matches!(
                err,
                LowerError::BlankNodeInDelete {
                    context: "DELETE templates",
                    ..
                }
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_lower_delete_forms_stable_blank_node_ids_allowed() {
        // Fluree stable ids stay legal: they address the stored node.
        let parsed = fluree_db_sparql::parse_sparql(
            "DELETE DATA { _:fdb-abc123 <http://example.org/p> <urn:o> }",
        );
        let ast = parsed.ast.expect("AST");
        let mut ns = NamespaceRegistry::new();
        lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
            .expect("stable blank-node id in DELETE DATA must lower");
    }

    #[test]
    fn test_stable_id_exemption_predicate_matches_validator() {
        // pr-1453 review follow-up: the validator's stable-id exemption
        // (validate/mod.rs, a `starts_with("fdb-")` on a duplicated const)
        // and this crate's `stable_blank_node_sid_from_label` must classify
        // the SAME labels as stable — today both are prefix-only, so
        // `_:fdb-zzz` is exempt on both sides (a constant addressing a —
        // possibly nonexistent — stored node; deleting it is a no-op, not
        // an error). The lowering-feature sync test in fluree-db-sparql
        // pins only the prefix CONSTANT; this pins the PREDICATE, so if the
        // helper ever grows stricter parsing without the validator
        // following, the drift fails here instead of shipping a
        // passes-validate-then-fails-at-lowering flow.
        for label in ["fdb-zzz", "fdb-1234-0-b0", "fdb-", "fdbx", "b0", "x0"] {
            let lowering_exempts =
                crate::namespace::stable_blank_node_sid_from_label(label).is_some();
            let sparql = format!("DELETE DATA {{ _:{label} <http://example.org/p> <urn:o> }}");
            let parsed = fluree_db_sparql::parse_sparql(&sparql);
            let ast = parsed
                .ast
                .unwrap_or_else(|| panic!("label {label} must parse: {:?}", parsed.diagnostics));
            let diags =
                fluree_db_sparql::validate(&ast, &fluree_db_sparql::Capabilities::default());
            let validator_exempts = !diags
                .iter()
                .any(|d| d.code == fluree_db_sparql::DiagCode::BlankNodeInDelete && d.is_error());
            assert_eq!(
                lowering_exempts, validator_exempts,
                "stable-id classification diverged for label {label:?} \
                 (lowering: {lowering_exempts}, validator: {validator_exempts})"
            );
        }
    }

    #[test]
    fn test_lower_insert_data_blank_node_still_allowed() {
        // INSERT DATA keeps CONSTRUCT-style fresh-mint blank nodes.
        let parsed =
            fluree_db_sparql::parse_sparql("INSERT DATA { _:a <http://example.org/p> <urn:o> }");
        let ast = parsed.ast.expect("AST");
        let mut ns = NamespaceRegistry::new();
        lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
            .expect("blank node in INSERT DATA must lower");
    }

    #[test]
    fn test_lower_insert_data_default_graph_has_no_graph_delta() {
        // A plain INSERT DATA (no GRAPH block) lowers with no named-graph delta.
        let parsed = fluree_db_sparql::parse_sparql(
            "INSERT DATA { <http://example.org/s> <http://example.org/p> \"v\" }",
        );
        assert!(
            !parsed.has_errors(),
            "parse errors: {:?}",
            parsed.diagnostics
        );
        let ast = parsed.ast.expect("AST");

        let mut ns = NamespaceRegistry::new();
        let txn = lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default()).expect("lower");

        assert_eq!(txn.insert_templates.len(), 1);
        assert!(txn.insert_templates[0].graph_id.is_none());
        assert!(txn.graph_delta.is_empty());
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
