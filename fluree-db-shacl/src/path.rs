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
//! Unsupported forms (e.g. the inverse of a composite path, `^(p1/p2)`) are
//! rejected at compile time with a clear error rather than silently misbehaving.

use crate::error::{Result, ShaclError};
use crate::predicates;
use fluree_db_core::{FlakeValue, GraphDbRef, IndexType, RangeMatch, RangeTest, Sid};
use fluree_vocab::namespaces::{JSON_LD, RDF, SHACL};
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
}

/// A value node reached by a path: `(value, datatype)`, mirroring a flake's
/// object + datatype columns.
pub type PathValue = (FlakeValue, Sid);

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
}

/// Datatype SID carried by reference (node) value nodes: `$id`.
fn ref_dt() -> Sid {
    Sid::new(JSON_LD, "id")
}

fn shacl(name: &str) -> Sid {
    Sid::new(SHACL, name)
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
        let members = ordered_objects(db, ps_subject, &shacl(predicates::PATH)).await?;
        match members.len() {
            0 => Ok(None),
            1 => match &members[0] {
                FlakeValue::Ref(obj) => Ok(Some(resolve_path_node(db, obj).await?)),
                // sh:path with a literal object is invalid; skip.
                _ => Ok(None),
            },
            _ => {
                // JSON-LD @list sequence: each ordered object is a path step.
                let mut steps = Vec::new();
                for obj in members {
                    if let FlakeValue::Ref(sid) = obj {
                        steps.push(resolve_path_node(db, &sid).await?);
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
        // sh:inversePath
        if let Some(obj) = single_ref(db, node, &shacl(predicates::INVERSE_PATH)).await? {
            let inner = resolve_path_node(db, &obj).await?;
            return match inner {
                PropertyPath::Predicate(p) => Ok(PropertyPath::Inverse(p)),
                _ => Err(unsupported(
                    node,
                    "sh:inversePath is only supported over a single predicate",
                )),
            };
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
            if let Some(obj) = single_ref(db, node, &shacl(pred)).await? {
                let inner = resolve_path_node(db, &obj).await?;
                return Ok(wrap(Box::new(inner)));
            }
        }

        // Bare RDF list → sequence path.
        let rdf_first = Sid::new(RDF, rdf_names::FIRST);
        if has_object(db, node, &rdf_first).await? {
            let members = resolve_rdf_list(db, node).await?;
            match members.len() {
                0 => return Err(unsupported(node, "sh:path sequence list is empty")),
                1 => return Ok(members.into_iter().next().unwrap()),
                _ => return Ok(PropertyPath::Sequence(members)),
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
        let objects = ordered_objects(db, subject, predicate).await?;

        // Turtle RDF-list form: a single object that is itself a list head.
        if let [FlakeValue::Ref(head)] = objects.as_slice() {
            let rdf_first = Sid::new(RDF, rdf_names::FIRST);
            if has_object(db, head, &rdf_first).await? {
                return resolve_rdf_list(db, head).await;
            }
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
            let Some(first) = single_ref(db, &current, &rdf_first).await? else {
                break;
            };
            members.push(resolve_path_node(db, &first).await?);

            match single_ref(db, &current, &rdf_rest).await? {
                Some(next) => current = next,
                None => break,
            }
        }
        Ok(members)
    })
}

/// All objects of `(subject, predicate)`, ordered by the JSON-LD list index in
/// flake metadata (falling back to scan order when unindexed).
async fn ordered_objects(
    db: GraphDbRef<'_>,
    subject: &Sid,
    predicate: &Sid,
) -> Result<Vec<FlakeValue>> {
    let flakes = db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(subject.clone(), predicate.clone()),
        )
        .await?;
    let mut items: Vec<(i32, FlakeValue)> = flakes
        .iter()
        .enumerate()
        .map(|(pos, f)| {
            let idx = f.m.as_ref().and_then(|m| m.i).unwrap_or(pos as i32);
            (idx, f.o.clone())
        })
        .collect();
    items.sort_by_key(|(i, _)| *i);
    Ok(items.into_iter().map(|(_, v)| v).collect())
}

/// Evaluate a property path from `focus`, returning the reached value nodes as
/// `(value, datatype)` pairs — the direct analogue of the objects of a single
/// `SPOT` scan for a simple predicate.
pub fn eval_path<'a>(
    db: GraphDbRef<'a>,
    focus: &'a Sid,
    path: &'a PropertyPath,
) -> PathFuture<'a, Vec<PathValue>> {
    Box::pin(async move {
        match path {
            PropertyPath::Predicate(p) => forward_step(db, focus, p).await,
            PropertyPath::Inverse(p) => inverse_step(db, focus, p).await,
            PropertyPath::Sequence(steps) => eval_sequence(db, focus, steps).await,
            PropertyPath::Alternative(alts) => {
                let mut out = Vec::new();
                for alt in alts {
                    out.extend(eval_path(db, focus, alt).await?);
                }
                Ok(dedup(out))
            }
            PropertyPath::ZeroOrMore(inner) => {
                let mut out = vec![(FlakeValue::Ref(focus.clone()), ref_dt())];
                out.extend(closure(db, focus, inner).await?);
                Ok(dedup(out))
            }
            PropertyPath::OneOrMore(inner) => Ok(dedup(closure(db, focus, inner).await?)),
            PropertyPath::ZeroOrOne(inner) => {
                let mut out = vec![(FlakeValue::Ref(focus.clone()), ref_dt())];
                out.extend(eval_path(db, focus, inner).await?);
                Ok(dedup(out))
            }
        }
    })
}

/// Forward single-predicate step: objects of `(focus, p, ?)`.
async fn forward_step(db: GraphDbRef<'_>, focus: &Sid, p: &Sid) -> Result<Vec<(FlakeValue, Sid)>> {
    let flakes = db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(focus.clone(), p.clone()),
        )
        .await?;
    Ok(flakes.iter().map(|f| (f.o.clone(), f.dt.clone())).collect())
}

/// Inverse single-predicate step: subjects of `(?, p, focus)`.
async fn inverse_step(db: GraphDbRef<'_>, focus: &Sid, p: &Sid) -> Result<Vec<(FlakeValue, Sid)>> {
    let flakes = db
        .range(
            IndexType::Opst,
            RangeTest::Eq,
            RangeMatch::predicate_object(p.clone(), FlakeValue::Ref(focus.clone())),
        )
        .await?;
    Ok(flakes
        .iter()
        .map(|f| (FlakeValue::Ref(f.s.clone()), ref_dt()))
        .collect())
}

/// Evaluate a sequence path: chain each step, carrying `(value, dt)` only for
/// the final step. Intermediate steps must reach reference nodes to continue.
async fn eval_sequence(
    db: GraphDbRef<'_>,
    focus: &Sid,
    steps: &[PropertyPath],
) -> Result<Vec<(FlakeValue, Sid)>> {
    let mut frontier: Vec<Sid> = vec![focus.clone()];

    for (i, step) in steps.iter().enumerate() {
        let is_last = i + 1 == steps.len();
        let mut reached: Vec<(FlakeValue, Sid)> = Vec::new();
        for node in &frontier {
            reached.extend(eval_path(db, node, step).await?);
        }
        reached = dedup(reached);

        if is_last {
            return Ok(reached);
        }
        frontier = reached
            .into_iter()
            .filter_map(|(v, _)| match v {
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
) -> Result<Vec<(FlakeValue, Sid)>> {
    let mut out: Vec<(FlakeValue, Sid)> = Vec::new();
    let mut visited: HashSet<Sid> = HashSet::new();
    let mut queue: Vec<Sid> = vec![focus.clone()];

    while let Some(node) = queue.pop() {
        for (value, dt) in eval_path(db, &node, inner).await? {
            if let FlakeValue::Ref(sid) = &value {
                if visited.insert(sid.clone()) {
                    queue.push(sid.clone());
                }
            }
            out.push((value, dt));
        }
    }
    Ok(dedup(out))
}

/// Deduplicate value nodes (SHACL value nodes are a set).
fn dedup(mut values: Vec<(FlakeValue, Sid)>) -> Vec<(FlakeValue, Sid)> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    values.retain(|(v, dt)| seen.insert((format!("{v:?}"), format!("{dt:?}"))));
    values
}

/// Fetch the single reference object of `(subject, predicate, ?)`, if any.
async fn single_ref(db: GraphDbRef<'_>, subject: &Sid, predicate: &Sid) -> Result<Option<Sid>> {
    let flakes = db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(subject.clone(), predicate.clone()),
        )
        .await?;
    Ok(flakes.iter().find_map(|f| match &f.o {
        FlakeValue::Ref(sid) => Some(sid.clone()),
        _ => None,
    }))
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
