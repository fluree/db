//! SHACL property paths (`sh:path`)
//!
//! `sh:path` may be a single predicate IRI or a *property path expression* built
//! from blank nodes: `sh:inversePath`, a sequence (bare RDF list), `sh:alternativePath`,
//! `sh:zeroOrMorePath`, `sh:oneOrMorePath`, and `sh:zeroOrOnePath`.
//!
//! Compilation ([`resolve_sh_path`]) walks the blank-node structure into a
//! [`PropertyPath`] AST. Validation ([`eval_path`]) evaluates that AST against a
//! focus node to produce the set of *value nodes* the path reaches — the same set
//! that a simple predicate would produce via a single `SPOT` scan.
//!
//! Unsupported forms (e.g. the inverse of a composite path, `^(p1/p2)`) compile
//! to [`PropertyPath::Unresolvable`] rather than silently misbehaving; the
//! reason is surfaced as a violation when the owning shape fires on a focus
//! node, keeping the failure scoped to that shape's targets.

use crate::error::{Result, ShaclError};
use crate::predicates;
use fluree_db_core::{
    id_datatype_sid, FlakeValue, GraphDbRef, IndexType, RangeMatch, RangeTest, SchemaHierarchy, Sid,
};
use fluree_vocab::namespaces::{BLANK_NODE, RDF, SHACL};
use fluree_vocab::rdf_names;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

/// A resolved `sh:path` expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyPath {
    /// A single predicate IRI (`ex:knows`).
    Predicate(Sid),
    /// `sh:inversePath` — reversed traversal. Only the inverse of a single
    /// predicate is supported; the inverse of a composite path is rejected.
    Inverse(Sid),
    /// A sequence path (RDF list of sub-paths): `p1 / p2 / …`.
    Sequence(Vec<PropertyPath>),
    /// `sh:alternativePath` (RDF list of sub-paths): `p1 | p2 | …`.
    Alternative(Vec<PropertyPath>),
    /// `sh:zeroOrMorePath`: `p*`.
    ZeroOrMore(Box<PropertyPath>),
    /// `sh:oneOrMorePath`: `p+`.
    OneOrMore(Box<PropertyPath>),
    /// `sh:zeroOrOnePath`: `p?`.
    ZeroOrOne(Box<PropertyPath>),
    /// A path that could not be compiled (unsupported form, or a blank-node
    /// expression whose structure never resolved). Carries the reason. Rather
    /// than failing shape compilation for the whole ledger, this is surfaced as
    /// a violation only when the owning shape actually fires on a focus node.
    Unresolvable(String),
}

/// A value node reached by a path: `(value, datatype, language tag)`,
/// mirroring a flake's object, datatype, and metadata language columns.
pub type PathValue = (FlakeValue, Sid, Option<String>);

/// Split path values into the parallel `(values, datatypes, langs)` columns
/// the constraint validators consume.
pub fn split_path_values(
    values: Vec<PathValue>,
) -> (Vec<FlakeValue>, Vec<Sid>, Vec<Option<String>>) {
    let mut vs = Vec::with_capacity(values.len());
    let mut dts = Vec::with_capacity(values.len());
    let mut langs = Vec::with_capacity(values.len());
    for (v, dt, lang) in values {
        vs.push(v);
        dts.push(dt);
        langs.push(lang);
    }
    (vs, dts, langs)
}

/// Boxed future returned by the recursive async path helpers.
type PathFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

impl PropertyPath {
    /// The single predicate for a simple path, else `None`.
    ///
    /// Used both for the validation fast path (a plain `SPOT` scan) and for
    /// `sh:resultPath` reporting, which can only name a single predicate.
    pub fn as_predicate(&self) -> Option<&Sid> {
        match self {
            PropertyPath::Predicate(p) => Some(p),
            _ => None,
        }
    }

    /// Whether this is a single predicate (the common, fast case).
    pub fn is_simple(&self) -> bool {
        matches!(self, PropertyPath::Predicate(_))
    }

    /// The unresolvable reason, if this path failed to compile.
    pub fn unresolvable_reason(&self) -> Option<&str> {
        match self {
            PropertyPath::Unresolvable(reason) => Some(reason),
            _ => None,
        }
    }

    /// Whether any predicate anywhere in the AST is a blank node — i.e. the
    /// path structure was not fully resolved (its sub-structure lives in a
    /// graph not yet scanned). Used as a belt-and-braces resolution check.
    pub fn references_blank_node(&self) -> bool {
        match self {
            PropertyPath::Predicate(p) | PropertyPath::Inverse(p) => p.namespace_code == BLANK_NODE,
            PropertyPath::Sequence(steps) | PropertyPath::Alternative(steps) => {
                steps.iter().any(PropertyPath::references_blank_node)
            }
            PropertyPath::ZeroOrMore(inner)
            | PropertyPath::OneOrMore(inner)
            | PropertyPath::ZeroOrOne(inner) => inner.references_blank_node(),
            PropertyPath::Unresolvable(_) => false,
        }
    }
}

fn shacl(name: &str) -> Sid {
    Sid::new(SHACL, name)
}

/// Rewrite a path into its inverse: `^(p1/p2)` becomes `^p2/^p1`,
/// `^(p1|p2)` becomes `^p1|^p2`, `^(p*)` becomes `(^p)*`, and `^^p`
/// collapses back to `p`. `Unresolvable` passes through unchanged.
fn invert(path: PropertyPath) -> PropertyPath {
    match path {
        PropertyPath::Predicate(p) => PropertyPath::Inverse(p),
        PropertyPath::Inverse(p) => PropertyPath::Predicate(p),
        PropertyPath::Sequence(steps) => {
            PropertyPath::Sequence(steps.into_iter().rev().map(invert).collect())
        }
        PropertyPath::Alternative(alts) => {
            PropertyPath::Alternative(alts.into_iter().map(invert).collect())
        }
        PropertyPath::ZeroOrMore(inner) => PropertyPath::ZeroOrMore(Box::new(invert(*inner))),
        PropertyPath::OneOrMore(inner) => PropertyPath::OneOrMore(Box::new(invert(*inner))),
        PropertyPath::ZeroOrOne(inner) => PropertyPath::ZeroOrOne(Box::new(invert(*inner))),
        PropertyPath::Unresolvable(reason) => PropertyPath::Unresolvable(reason),
    }
}

/// Resolve the `sh:path` of a property shape subject into a [`PropertyPath`].
///
/// Handles all three encodings of `sh:path`:
/// - a single predicate IRI → [`PropertyPath::Predicate`];
/// - a Turtle blank-node path expression (`sh:inversePath`, a bare RDF list
///   sequence, `sh:alternativePath`, `sh:zeroOrMorePath`, …);
/// - the JSON-LD `@list` sequence encoding, where multiple ordered `sh:path`
///   flakes (indexed via flake metadata) form the sequence.
///
/// Returns `Ok(None)` if `ps_subject` has no usable `sh:path` in this graph
/// (e.g. a blank-node path whose structure lives in a different graph); the
/// caller may retry against another graph and ultimately reject if unresolved.
pub fn resolve_sh_path<'a>(
    db: GraphDbRef<'a>,
    ps_subject: &'a Sid,
) -> PathFuture<'a, Option<PropertyPath>> {
    Box::pin(async move {
        let (members, all_indexed) =
            ordered_objects(db, ps_subject, &shacl(predicates::PATH)).await?;
        match members.len() {
            0 => Ok(None),
            1 => match &members[0] {
                FlakeValue::Ref(obj) => Ok(Some(resolve_path_node(db, obj).await?)),
                // sh:path with a literal object is invalid; skip.
                _ => Ok(None),
            },
            // Multiple sh:path objects are only a sequence under the JSON-LD
            // @list encoding (every flake carries a list index). Un-indexed
            // multiples are distinct sh:path assertions — a malformed shape.
            _ if !all_indexed => Err(unsupported(ps_subject, "multiple sh:path values")),
            _ => {
                // JSON-LD @list sequence: each ordered object is a path step.
                let mut steps = Vec::new();
                for obj in members {
                    match obj {
                        FlakeValue::Ref(sid) => steps.push(resolve_path_node(db, &sid).await?),
                        // A literal in a sequence path is invalid; reject rather
                        // than silently dropping the step.
                        _ => {
                            return Err(unsupported(
                                ps_subject,
                                "sh:path sequence step is a literal",
                            ))
                        }
                    }
                }
                Ok(Some(PropertyPath::Sequence(steps)))
            }
        }
    })
}

/// Resolve a single `sh:path` object node (a predicate IRI or a path-expression
/// blank node) into a [`PropertyPath`].
fn resolve_path_node<'a>(db: GraphDbRef<'a>, node: &'a Sid) -> PathFuture<'a, PropertyPath> {
    Box::pin(async move {
        // An IRI-valued sh:path is always a plain predicate (SHACL spec); only
        // blank nodes carry path-expression structure. Short-circuit the common
        // case to avoid six empty operator probes per property shape — resolve
        // runs per shape per compile, and compiles happen per transaction.
        if node.namespace_code != BLANK_NODE {
            return Ok(PropertyPath::Predicate(node.clone()));
        }

        // Bare RDF list → sequence path. Checked before the operator keys:
        // an (ill-formed) node carrying both a list structure and an
        // operator reads as the sequence, matching the W3C suite's
        // path-strange-001/002 expectations.
        let rdf_first = Sid::new(RDF, rdf_names::FIRST);
        if has_object(db, node, &rdf_first).await? {
            let members = resolve_rdf_list(db, node).await?;
            match members.len() {
                0 => return Err(unsupported(node, "sh:path sequence list is empty")),
                1 => return Ok(members.into_iter().next().unwrap()),
                _ => return Ok(PropertyPath::Sequence(members)),
            }
        }

        // sh:inversePath — inverse of any path, rewritten into the AST
        // (inverse of a sequence = reversed sequence of inverses, etc.).
        if let Some(inner) = operand_path(db, node, &shacl(predicates::INVERSE_PATH)).await? {
            return Ok(invert(inner));
        }

        // sh:alternativePath (RDF list or JSON-LD @list of sub-paths)
        if has_object(db, node, &shacl(predicates::ALTERNATIVE_PATH)).await? {
            let members = resolve_members(db, node, &shacl(predicates::ALTERNATIVE_PATH)).await?;
            if members.is_empty() {
                return Err(unsupported(node, "sh:alternativePath list is empty"));
            }
            return Ok(PropertyPath::Alternative(members));
        }

        // sh:zeroOrMorePath / sh:oneOrMorePath / sh:zeroOrOnePath
        for (pred, wrap) in [
            (
                predicates::ZERO_OR_MORE_PATH,
                PropertyPath::ZeroOrMore as fn(Box<PropertyPath>) -> PropertyPath,
            ),
            (predicates::ONE_OR_MORE_PATH, PropertyPath::OneOrMore),
            (predicates::ZERO_OR_ONE_PATH, PropertyPath::ZeroOrOne),
        ] {
            if let Some(inner) = operand_path(db, node, &shacl(pred)).await? {
                return Ok(wrap(Box::new(inner)));
            }
        }

        // No path-expression structure → a plain predicate IRI.
        Ok(PropertyPath::Predicate(node.clone()))
    })
}

/// Resolve the ordered members of a `(subject, predicate)` list, transparently
/// handling both the Turtle RDF-list encoding (a single object that heads an
/// `rdf:first`/`rdf:rest` list) and the JSON-LD `@list` encoding (multiple
/// ordered objects).
fn resolve_members<'a>(
    db: GraphDbRef<'a>,
    subject: &'a Sid,
    predicate: &'a Sid,
) -> PathFuture<'a, Vec<PropertyPath>> {
    Box::pin(async move {
        let (objects, all_indexed) = ordered_objects(db, subject, predicate).await?;

        // Turtle RDF-list form: a single object that is itself a list head.
        if let [FlakeValue::Ref(head)] = objects.as_slice() {
            let rdf_first = Sid::new(RDF, rdf_names::FIRST);
            if has_object(db, head, &rdf_first).await? {
                return resolve_rdf_list(db, head).await;
            }
        }
        if objects.len() > 1 && !all_indexed {
            return Err(unsupported(
                subject,
                &format!(
                    "multiple values for {} in a path expression",
                    predicate.name
                ),
            ));
        }

        // JSON-LD @list form (or a single direct member).
        let mut out = Vec::new();
        for obj in objects {
            if let FlakeValue::Ref(sid) = obj {
                out.push(resolve_path_node(db, &sid).await?);
            }
        }
        Ok(out)
    })
}

/// Walk an `rdf:first`/`rdf:rest` list, resolving each element as a sub-path.
fn resolve_rdf_list<'a>(
    db: GraphDbRef<'a>,
    list_head: &'a Sid,
) -> PathFuture<'a, Vec<PropertyPath>> {
    Box::pin(async move {
        let rdf_first = Sid::new(RDF, rdf_names::FIRST);
        let rdf_rest = Sid::new(RDF, rdf_names::REST);
        let rdf_nil = Sid::new(RDF, rdf_names::NIL);

        let mut members = Vec::new();
        let mut current = list_head.clone();
        const MAX_LIST_LENGTH: usize = 10_000;

        for _ in 0..MAX_LIST_LENGTH {
            if current == rdf_nil {
                break;
            }
            let Some(first) = sole_ref(db, &current, &rdf_first).await? else {
                break;
            };
            members.push(resolve_path_node(db, &first).await?);

            match sole_ref(db, &current, &rdf_rest).await? {
                Some(next) => current = next,
                None => break,
            }
        }
        Ok(members)
    })
}

/// All objects of `(subject, predicate)`, ordered by the JSON-LD list index in
/// flake metadata (falling back to scan order when unindexed). The flag
/// reports whether every flake carried a list index — multiple objects
/// WITHOUT indexes are separate assertions, not a list encoding.
async fn ordered_objects(
    db: GraphDbRef<'_>,
    subject: &Sid,
    predicate: &Sid,
) -> Result<(Vec<FlakeValue>, bool)> {
    let flakes = db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(subject.clone(), predicate.clone()),
        )
        .await?;
    // Order by the JSON-LD list index when present; unindexed flakes keep their
    // scan order and sort after indexed ones (never interleaved). JSON-LD `@list`
    // always stamps `m.i`, so in practice all-or-none carry an index.
    let mut items: Vec<(Option<i32>, usize, FlakeValue)> = flakes
        .iter()
        .enumerate()
        .map(|(pos, f)| (f.m.as_ref().and_then(|m| m.i), pos, f.o.clone()))
        .collect();
    items.sort_by_key(|(idx, pos, _)| (idx.is_none(), idx.unwrap_or(0), *pos));
    let all_indexed = items.iter().all(|(idx, _, _)| idx.is_some());
    Ok((items.into_iter().map(|(_, _, v)| v).collect(), all_indexed))
}

/// The predicate plus its RDFS subproperties — the always-on entailment
/// expansion for enforcement: a step over `p` also traverses every
/// `q rdfs:subPropertyOf p`.
pub(crate) fn with_subproperties(p: &Sid, hierarchy: Option<&SchemaHierarchy>) -> Vec<Sid> {
    let mut preds = vec![p.clone()];
    if let Some(h) = hierarchy {
        preds.extend(h.subproperties_of(p).iter().cloned());
    }
    preds
}

/// Evaluate a property path from `focus`, returning the reached value nodes as
/// `(value, datatype, language)` tuples — the direct analogue of the objects
/// of a single `SPOT` scan for a simple predicate.
pub fn eval_path<'a>(
    db: GraphDbRef<'a>,
    focus: &'a Sid,
    path: &'a PropertyPath,
    hierarchy: Option<&'a SchemaHierarchy>,
) -> PathFuture<'a, Vec<PathValue>> {
    Box::pin(async move {
        match path {
            PropertyPath::Predicate(p) => forward_step(db, focus, p, hierarchy).await,
            PropertyPath::Inverse(p) => inverse_step(db, focus, p, hierarchy).await,
            PropertyPath::Sequence(steps) => eval_sequence(db, focus, steps, hierarchy).await,
            PropertyPath::Alternative(alts) => {
                let mut out = Vec::new();
                for alt in alts {
                    out.extend(eval_path(db, focus, alt, hierarchy).await?);
                }
                Ok(dedup(out))
            }
            PropertyPath::ZeroOrMore(inner) => {
                let mut out = vec![(FlakeValue::Ref(focus.clone()), id_datatype_sid(), None)];
                out.extend(closure(db, focus, inner, hierarchy).await?);
                Ok(dedup(out))
            }
            PropertyPath::OneOrMore(inner) => {
                Ok(dedup(closure(db, focus, inner, hierarchy).await?))
            }
            PropertyPath::ZeroOrOne(inner) => {
                let mut out = vec![(FlakeValue::Ref(focus.clone()), id_datatype_sid(), None)];
                out.extend(eval_path(db, focus, inner, hierarchy).await?);
                Ok(dedup(out))
            }
            // Never evaluated: validation surfaces a violation for the owning
            // shape before reaching value evaluation. Defensive empty set.
            PropertyPath::Unresolvable(_) => Ok(Vec::new()),
        }
    })
}

/// Forward single-predicate step: objects of `(focus, p, ?)`, unioned over
/// `p` and its RDFS subproperties.
async fn forward_step(
    db: GraphDbRef<'_>,
    focus: &Sid,
    p: &Sid,
    hierarchy: Option<&SchemaHierarchy>,
) -> Result<Vec<PathValue>> {
    let mut out = Vec::new();
    for pred in with_subproperties(p, hierarchy) {
        let flakes = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(focus.clone(), pred),
            )
            .await?;
        out.extend(flakes.iter().map(|f| {
            (
                f.o.clone(),
                f.dt.clone(),
                f.m.as_ref().and_then(|m| m.lang.clone()),
            )
        }));
    }
    Ok(out)
}

/// Inverse single-predicate step: subjects of `(?, p, focus)`, unioned over
/// `p` and its RDFS subproperties.
async fn inverse_step(
    db: GraphDbRef<'_>,
    focus: &Sid,
    p: &Sid,
    hierarchy: Option<&SchemaHierarchy>,
) -> Result<Vec<PathValue>> {
    let mut out = Vec::new();
    for pred in with_subproperties(p, hierarchy) {
        let flakes = db
            .range(
                IndexType::Opst,
                RangeTest::Eq,
                RangeMatch::predicate_object(pred, FlakeValue::Ref(focus.clone())),
            )
            .await?;
        out.extend(
            flakes
                .iter()
                .map(|f| (FlakeValue::Ref(f.s.clone()), id_datatype_sid(), None)),
        );
    }
    Ok(out)
}

/// Evaluate a sequence path: chain each step, carrying `(value, dt)` only for
/// the final step. Intermediate steps must reach reference nodes to continue.
async fn eval_sequence(
    db: GraphDbRef<'_>,
    focus: &Sid,
    steps: &[PropertyPath],
    hierarchy: Option<&SchemaHierarchy>,
) -> Result<Vec<PathValue>> {
    let mut frontier: Vec<Sid> = vec![focus.clone()];

    for (i, step) in steps.iter().enumerate() {
        let is_last = i + 1 == steps.len();
        let mut reached: Vec<PathValue> = Vec::new();
        for node in &frontier {
            reached.extend(eval_path(db, node, step, hierarchy).await?);
        }
        reached = dedup(reached);

        if is_last {
            return Ok(reached);
        }
        frontier = reached
            .into_iter()
            .filter_map(|(v, _, _)| match v {
                FlakeValue::Ref(sid) => Some(sid),
                _ => None,
            })
            .collect();
        frontier.sort();
        frontier.dedup();
        if frontier.is_empty() {
            return Ok(Vec::new());
        }
    }
    Ok(Vec::new())
}

/// Transitive closure of `inner` from `focus` (one or more steps), BFS over the
/// reference nodes reached. Non-reference values are terminal value nodes.
async fn closure(
    db: GraphDbRef<'_>,
    focus: &Sid,
    inner: &PropertyPath,
    hierarchy: Option<&SchemaHierarchy>,
) -> Result<Vec<PathValue>> {
    let mut out: Vec<PathValue> = Vec::new();
    // Seed `visited` with the focus so a cycle back to it isn't re-expanded.
    let mut visited: HashSet<Sid> = HashSet::from([focus.clone()]);
    let mut queue: Vec<Sid> = vec![focus.clone()];

    while let Some(node) = queue.pop() {
        for (value, dt, lang) in eval_path(db, &node, inner, hierarchy).await? {
            if let FlakeValue::Ref(sid) = &value {
                if visited.insert(sid.clone()) {
                    queue.push(sid.clone());
                }
            }
            out.push((value, dt, lang));
        }
    }
    Ok(dedup(out))
}

/// Deduplicate value nodes (SHACL value nodes are a set).
fn dedup(mut values: Vec<PathValue>) -> Vec<PathValue> {
    let mut seen: HashSet<(FlakeValue, Sid, Option<String>)> = HashSet::new();
    values.retain(|(v, dt, lang)| seen.insert((v.clone(), dt.clone(), lang.clone())));
    values
}

/// Resolve a path operator's operand: a single reference resolves as one
/// sub-path; multiple references are a sequence only under the JSON-LD @list
/// encoding (every flake indexed) — un-indexed multiples are a malformed path.
fn operand_path<'a>(
    db: GraphDbRef<'a>,
    subject: &'a Sid,
    predicate: &'a Sid,
) -> PathFuture<'a, Option<PropertyPath>> {
    Box::pin(async move {
        let (objects, all_indexed) = ordered_objects(db, subject, predicate).await?;
        match objects.len() {
            0 => Ok(None),
            1 => match &objects[0] {
                FlakeValue::Ref(obj) => Ok(Some(resolve_path_node(db, obj).await?)),
                _ => Err(unsupported(
                    subject,
                    &format!("{} operand is a literal", predicate.name),
                )),
            },
            _ if !all_indexed => Err(unsupported(
                subject,
                &format!(
                    "multiple values for {} in a path expression",
                    predicate.name
                ),
            )),
            _ => {
                let mut steps = Vec::new();
                for obj in objects {
                    match obj {
                        FlakeValue::Ref(sid) => steps.push(resolve_path_node(db, &sid).await?),
                        _ => {
                            return Err(unsupported(
                                subject,
                                &format!("{} sequence step is a literal", predicate.name),
                            ))
                        }
                    }
                }
                Ok(Some(PropertyPath::Sequence(steps)))
            }
        }
    })
}

/// Fetch the sole reference object of `(subject, predicate, ?)`, if any.
/// Path operators take exactly one value — multiple distinct references are a
/// malformed path and error rather than compiling nondeterministically.
async fn sole_ref(db: GraphDbRef<'_>, subject: &Sid, predicate: &Sid) -> Result<Option<Sid>> {
    let flakes = db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(subject.clone(), predicate.clone()),
        )
        .await?;
    let mut refs = flakes.iter().filter_map(|f| match &f.o {
        FlakeValue::Ref(sid) => Some(sid.clone()),
        _ => None,
    });
    let first = refs.next();
    if let Some(first) = &first {
        if refs.any(|other| &other != first) {
            return Err(unsupported(
                subject,
                &format!(
                    "multiple values for {} in a path expression",
                    predicate.name
                ),
            ));
        }
    }
    Ok(first)
}

/// Whether `(subject, predicate, ?)` has any object (regardless of type).
async fn has_object(db: GraphDbRef<'_>, subject: &Sid, predicate: &Sid) -> Result<bool> {
    let flakes = db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(subject.clone(), predicate.clone()),
        )
        .await?;
    Ok(!flakes.is_empty())
}

fn unsupported(shape_node: &Sid, message: &str) -> ShaclError {
    ShaclError::InvalidConstraint {
        shape_id: shape_node.clone(),
        message: message.to_string(),
    }
}
