//! The extraction model's ontology input policy, `ontology-adapter-v1`.
//!
//! Ontology input the model has no place for is read another way, or dropped
//! and counted, rather than refused. In order:
//!
//! - blank nodes outside SHACL (OWL restrictions, `owl:unionOf` lists,
//!   anonymous classes) are dropped, with every triple that points at one;
//! - a class or property without `rdfs:label` takes its `skos:prefLabel`,
//!   and one without `rdfs:comment` its `skos:definition` (English or
//!   untagged first, then by text, so the choice is the same every time);
//! - a term still without a label takes its IRI's local name in words
//!   ([`term_words`]).
//!
//! What the model then shows of a class or property is its declaration,
//! label, comment, one parent class and the SHACL node shapes that target a
//! class. Everything else in the caller's ontology is "not shown", counted by
//! predicate against the ontology as the caller wrote it. Both counts use the
//! keys an extraction response reports them under, so the two can be
//! compared line for line.

use crate::read::Quad;
use crate::vocab::*;
use crate::words::term_words;
use fluree_graph_ir::Term;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// The policy this module mirrors, as an extraction response names it.
pub const POLICY: &str = "ontology-adapter-v1";

/// Report keys for what the policy adapts.
pub const DROPPED_BLANK: &str = "blank-node triples dropped";
pub const LABEL_FROM_PREF_LABEL: &str = "rdfs:label from skos:prefLabel";
pub const COMMENT_FROM_DEFINITION: &str = "rdfs:comment from skos:definition";
pub const LABEL_FROM_IRI: &str = "rdfs:label from the IRI";
/// Report keys for what is not shown, beside the predicates themselves.
pub const NOT_A_SHOWN_TERM: &str = "(not a shown term)";
pub const SECOND_PARENT: &str = "rdfs:subClassOf (a second parent)";
pub const SH_UNSUPPORTED: &str = "sh:(unsupported)";

/// The declarations a term may carry and still be shown as it is.
const SHOWN_TYPES: &[&str] = &[
    RDFS_CLASS,
    RDF_PROPERTY,
    OWL_CLASS,
    OWL_OBJECT_PROPERTY,
    OWL_DATATYPE_PROPERTY,
];
const SHOWN_PREDICATES: &[&str] = &[RDFS_LABEL, RDFS_COMMENT];
const TERM_TYPES: &[&str] = &[RDFS_CLASS, RDF_PROPERTY];

/// What the policy changed: the adapted quads (what the model input is built
/// from), counts and example terms by report key, and the exact SKOS
/// statements a fallback read (those count as shown).
#[derive(Clone, Debug, Default)]
pub struct Adapted {
    pub quads: Vec<Quad>,
    pub counts: BTreeMap<String, usize>,
    pub examples: BTreeMap<String, Vec<String>>,
    pub borrowed: HashSet<(Term, String, Term)>,
}

/// How many example terms a report keeps per key.
pub const EXAMPLES: usize = 3;

fn note(map: &mut BTreeMap<String, Vec<String>>, key: &str, term: &Term) {
    if let Some(iri) = term.as_iri() {
        let list = map.entry(key.to_string()).or_default();
        if list.len() < EXAMPLES && !list.iter().any(|x| x == iri) {
            list.push(iri.to_string());
        }
    }
}

/// OWL declarations normalized to the RDFS ones the model reads, without
/// changing their IRIs: `owl:Class` adds `rdfs:Class`, an OWL property adds
/// `rdf:Property`.
pub fn normalize(quads: &[Quad]) -> Vec<Quad> {
    let mut out: BTreeSet<Quad> = quads.iter().cloned().collect();
    for q in quads {
        if !q.is(RDF_TYPE) {
            continue;
        }
        let to = match q.object.as_iri() {
            Some(OWL_CLASS) => RDFS_CLASS,
            Some(OWL_OBJECT_PROPERTY | OWL_DATATYPE_PROPERTY) => RDF_PROPERTY,
            _ => continue,
        };
        out.insert(Quad {
            object: Term::iri(to),
            ..q.clone()
        });
    }
    out.into_iter().collect()
}

/// An ontology's SHACL nodes: every shape (typed `sh:NodeShape` or
/// `sh:PropertyShape`, or the subject of a SHACL predicate) and every blank
/// node reachable from one: its property shapes and the list nodes of its
/// `sh:in`. IRIs a shape points to are not followed.
pub fn shape_nodes(quads: &[Quad]) -> HashSet<&Term> {
    let roots: Vec<&Term> = quads
        .iter()
        .filter(|q| {
            q.predicate.starts_with(SH)
                || q.is(RDF_TYPE)
                    && matches!(q.object.as_iri(), Some(SH_NODE_SHAPE | SH_PROPERTY_SHAPE))
        })
        .map(|q| &q.subject)
        .collect();
    let mut blank_objects: HashMap<&Term, Vec<&Term>> = HashMap::new();
    for q in quads {
        if q.object.is_blank() {
            blank_objects.entry(&q.subject).or_default().push(&q.object);
        }
    }
    let mut nodes: HashSet<&Term> = roots.iter().copied().collect();
    let mut stack = roots;
    while let Some(node) = stack.pop() {
        for next in blank_objects.get(node).into_iter().flatten() {
            if nodes.insert(next) {
                stack.push(next);
            }
        }
    }
    nodes
}

/// The blank nodes an ontology may carry: SHACL nodes that are not declared
/// classes or properties.
fn shape_support(quads: &[Quad]) -> HashSet<Term> {
    let terms: HashSet<&Term> = quads
        .iter()
        .filter(|q| q.is(RDF_TYPE) && q.object.as_iri().is_some_and(|o| SCHEMA_TYPES.contains(&o)))
        .map(|q| &q.subject)
        .collect();
    shape_nodes(quads)
        .into_iter()
        .filter(|n| n.is_blank() && !terms.contains(n))
        .cloned()
        .collect()
}

/// A literal's text, language tag and datatype.
pub fn literal(term: &Term) -> Option<(String, Option<&str>, &str)> {
    term.as_literal()
        .map(|(value, datatype, lang)| (value.lexical(), lang, datatype.as_iri()))
}

/// One literal of several: English or untagged first, then by its text, so
/// the choice is the same every time.
fn preferred<'a>(objects: &[&'a Term]) -> Option<&'a Term> {
    objects
        .iter()
        .filter_map(|o| literal(o).map(|(text, lang, dt)| (*o, text, lang, dt)))
        .min_by(|a, b| rank(a).cmp(&rank(b)))
        .map(|(o, ..)| o)
}

fn rank<'a>(
    (_, text, lang, datatype): &'a (&Term, String, Option<&str>, &str),
) -> (u8, &'a str, &'a str, &'a str) {
    let primary = lang
        .unwrap_or("en")
        .split('-')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    (
        u8::from(primary != "en"),
        text.as_str(),
        lang.unwrap_or_default(),
        datatype,
    )
}

/// The ontology as the model input can show it. `quads` should already be
/// [`normalize`]d.
pub fn adapt(quads: Vec<Quad>) -> Adapted {
    let mut out = Adapted::default();
    let support = shape_support(&quads);
    let loose: HashSet<Term> = quads
        .iter()
        .flat_map(|q| [&q.subject, &q.object])
        .filter(|t| t.is_blank() && !support.contains(*t))
        .cloned()
        .collect();
    let quads = if loose.is_empty() {
        quads
    } else {
        let (kept, dropped): (Vec<Quad>, Vec<Quad>) = quads
            .into_iter()
            .partition(|q| !loose.contains(&q.subject) && !loose.contains(&q.object));
        out.counts.insert(DROPPED_BLANK.to_string(), dropped.len());
        for q in &dropped {
            note(&mut out.examples, DROPPED_BLANK, &q.subject);
        }
        kept
    };
    let mut terms: BTreeMap<String, (Term, Option<std::sync::Arc<str>>)> = BTreeMap::new();
    for q in &quads {
        if q.is(RDF_TYPE)
            && q.subject.is_iri()
            && q.object.as_iri().is_some_and(|o| TERM_TYPES.contains(&o))
        {
            terms.insert(nt(&q.subject), (q.subject.clone(), q.graph.clone()));
        }
    }
    let mut values: HashMap<(&Term, &str), Vec<&Term>> = HashMap::new();
    for q in &quads {
        values
            .entry((&q.subject, &*q.predicate))
            .or_default()
            .push(&q.object);
    }
    let mut added = Vec::new();
    for (subject, graph) in terms.values() {
        let has = |p: &str| values.get(&(subject, p)).is_some_and(|v| !v.is_empty());
        let get = |p: &str| values.get(&(subject, p)).cloned().unwrap_or_default();
        for (wanted, fallback, key) in [
            (RDFS_LABEL, SKOS_PREF_LABEL, LABEL_FROM_PREF_LABEL),
            (RDFS_COMMENT, SKOS_DEFINITION, COMMENT_FROM_DEFINITION),
        ] {
            if has(wanted) {
                continue;
            }
            if let Some(value) = preferred(&get(fallback)) {
                added.push(Quad {
                    subject: subject.clone(),
                    predicate: wanted.into(),
                    object: value.clone(),
                    graph: graph.clone(),
                });
                out.borrowed
                    .insert((subject.clone(), fallback.to_string(), value.clone()));
                *out.counts.entry(key.to_string()).or_default() += 1;
                note(&mut out.examples, key, subject);
            }
        }
        if !has(RDFS_LABEL) && preferred(&get(SKOS_PREF_LABEL)).is_none() {
            if let Some(words) = subject.as_iri().and_then(term_words) {
                added.push(Quad {
                    subject: subject.clone(),
                    predicate: RDFS_LABEL.into(),
                    object: Term::string(words),
                    graph: graph.clone(),
                });
                *out.counts.entry(LABEL_FROM_IRI.to_string()).or_default() += 1;
                note(&mut out.examples, LABEL_FROM_IRI, subject);
            }
        }
    }
    let mut all: BTreeSet<Quad> = quads.into_iter().collect();
    all.extend(added);
    out.quads = all.into_iter().collect();
    out
}

/// A term as N-Triples writes it, for the orderings the model input uses.
pub fn nt(term: &Term) -> String {
    match term {
        Term::Iri(iri) => format!("<{iri}>"),
        Term::BlankNode(id) => format!("_:{}", id.as_str()),
        Term::Literal { .. } => {
            let (text, lang, datatype) = literal(term).unwrap_or_default();
            match lang {
                Some(lang) => format!("{text:?}@{lang}"),
                None if datatype == "http://www.w3.org/2001/XMLSchema#string" => {
                    format!("{text:?}")
                }
                None => format!("{text:?}^^<{datatype}>"),
            }
        }
    }
}

/// A compiled SHACL property shape: what the model input shows of it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PropertyShape {
    pub path: String,
    /// `sh:class`, else `sh:datatype`: the range the shape restates.
    pub value: Option<String>,
    pub unsupported: Vec<String>,
}

/// A compiled SHACL node shape.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeShape {
    pub id: Option<String>,
    pub targets: Vec<String>,
    pub properties: Vec<PropertyShape>,
    pub unsupported: Vec<String>,
}

const PROPERTY_COMPILED: &[&str] = &[
    "path",
    "datatype",
    "class",
    "nodeKind",
    "in",
    "minCount",
    "maxCount",
    "pattern",
    "name",
    "description",
];
const INFORMATIONAL: &[&str] = &[
    "order",
    "group",
    "defaultValue",
    "severity",
    "message",
    "name",
    "description",
];
const TARGETS: &[&str] = &[
    "targetClass",
    "targetNode",
    "targetSubjectsOf",
    "targetObjectsOf",
    "target",
];
const PATH_KINDS: &[&str] = &[
    "inversePath",
    "alternativePath",
    "zeroOrMorePath",
    "oneOrMorePath",
    "zeroOrOnePath",
];

fn sh_local(predicate: &str) -> Option<&str> {
    predicate.strip_prefix(SH)
}

/// A subject's predicates and their objects, in first-seen order.
type Predicates<'a> = Vec<(&'a str, Vec<&'a Term>)>;

/// Subject → predicate → objects, in first-seen order.
struct Index<'a> {
    po: Vec<(&'a Term, Predicates<'a>)>,
    at: HashMap<&'a Term, usize>,
}

impl<'a> Index<'a> {
    fn new(quads: &'a [Quad]) -> Self {
        let mut index = Index {
            po: Vec::new(),
            at: HashMap::new(),
        };
        for q in quads {
            let i = *index.at.entry(&q.subject).or_insert_with(|| {
                index.po.push((&q.subject, Vec::new()));
                index.po.len() - 1
            });
            let preds = &mut index.po[i].1;
            let objs = match preds.iter().position(|(p, _)| *p == &*q.predicate) {
                Some(j) => &mut preds[j].1,
                None => {
                    preds.push((&q.predicate, Vec::new()));
                    &mut preds.last_mut().unwrap().1
                }
            };
            if !objs.contains(&&q.object) {
                objs.push(&q.object);
            }
        }
        index
    }

    fn objects(&self, subject: &Term, predicate: &str) -> Vec<&'a Term> {
        self.at
            .get(subject)
            .and_then(|i| self.po[*i].1.iter().find(|(p, _)| *p == predicate))
            .map(|(_, o)| o.clone())
            .unwrap_or_default()
    }

    fn predicates(&self, subject: &Term) -> Vec<&'a str> {
        self.at
            .get(subject)
            .map(|i| self.po[*i].1.iter().map(|(p, _)| *p).collect())
            .unwrap_or_default()
    }

    fn types(&self, subject: &Term) -> Vec<&'a str> {
        self.objects(subject, RDF_TYPE)
            .into_iter()
            .filter_map(Term::as_iri)
            .collect()
    }

    /// The members of a well-formed RDF list, or `None`.
    fn list(&self, head: &Term) -> Option<Vec<&'a Term>> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut node = head;
        while node.as_iri() != Some(RDF_NIL) {
            if !node.is_blank() || !seen.insert(node.clone()) {
                return None;
            }
            let first = self.objects(node, RDF_FIRST);
            let rest = self.objects(node, RDF_REST);
            if first.len() != 1 || rest.len() != 1 {
                return None;
            }
            out.push(first[0]);
            node = rest[0];
        }
        Some(out)
    }
}

fn path_problem(index: &Index<'_>, paths: &[&Term]) -> Option<String> {
    let path = match paths {
        [] => return Some(format!("{SH}path (missing)")),
        [one] => *one,
        _ => return Some(format!("{SH}path (several)")),
    };
    if path.is_iri() {
        return None;
    }
    if path.is_literal() {
        return Some(format!("{SH}path (not an IRI)"));
    }
    if let Some(kind) = PATH_KINDS
        .iter()
        .find(|k| !index.objects(path, &format!("{SH}{k}")).is_empty())
    {
        return Some(format!("{SH}{kind}"));
    }
    if !index.objects(path, RDF_FIRST).is_empty() {
        return Some(format!("{SH}path (sequence)"));
    }
    Some(format!("{SH}path (not an IRI)"))
}

fn is_count(term: &Term) -> bool {
    literal(term).is_some_and(|(text, ..)| {
        let t = text.trim();
        let digits = t.strip_prefix('+').unwrap_or(t);
        !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
    })
}

fn property_shape(
    index: &Index<'_>,
    node: &Term,
    passed: &[&str],
) -> Result<PropertyShape, String> {
    let paths = index.objects(node, &format!("{SH}path"));
    if let Some(problem) = path_problem(index, &paths) {
        return Err(problem);
    }
    let mut unsupported: BTreeSet<String> = index
        .predicates(node)
        .into_iter()
        .filter(|p| {
            sh_local(p).is_some_and(|l| {
                !PROPERTY_COMPILED.contains(&l)
                    && !INFORMATIONAL.contains(&l)
                    && !passed.contains(&l)
            })
        })
        .map(str::to_string)
        .collect();
    let mut one = |name: &str, accept: &dyn Fn(&Term) -> bool| -> Option<String> {
        let values = index.objects(node, &format!("{SH}{name}"));
        match values.as_slice() {
            [] => None,
            [v] if accept(v) => Some(v.as_iri().map(str::to_string).unwrap_or_default()),
            _ => {
                unsupported.insert(format!("{SH}{name}"));
                None
            }
        }
    };
    let datatype = one("datatype", &Term::is_iri);
    let class = one("class", &Term::is_iri);
    one("nodeKind", &Term::is_iri);
    one("pattern", &Term::is_literal);
    one("minCount", &is_count);
    one("maxCount", &is_count);
    let heads = index.objects(node, &format!("{SH}in"));
    if !heads.is_empty() {
        let members = if heads.len() == 1 {
            index.list(heads[0])
        } else {
            None
        };
        if members.is_none_or(|m| m.iter().any(|t| t.is_blank())) {
            unsupported.insert(format!("{SH}in"));
        }
    }
    Ok(PropertyShape {
        path: paths[0].as_iri().unwrap_or_default().to_string(),
        value: class.or(datatype),
        unsupported: unsupported.into_iter().collect(),
    })
}

/// The node shapes in an ontology: subjects typed `sh:NodeShape` or carrying
/// a target, and classes that are their own shapes. Sorted by content.
pub fn compile_shapes(quads: &[Quad]) -> Vec<NodeShape> {
    let index = Index::new(quads);
    let mut out = BTreeSet::new();
    for (subject, _) in &index.po {
        let types = index.types(subject);
        let targets: Vec<&str> = index
            .predicates(subject)
            .into_iter()
            .filter(|p| sh_local(p).is_some_and(|l| TARGETS.contains(&l)))
            .collect();
        let is_class = types.iter().any(|t| *t == RDFS_CLASS || *t == OWL_CLASS);
        let is_shape = types
            .iter()
            .any(|t| *t == SH_NODE_SHAPE || *t == SH_PROPERTY_SHAPE);
        let implicit = subject.is_iri() && is_class && is_shape;
        if !types.contains(&SH_NODE_SHAPE) && targets.is_empty() && !implicit {
            continue;
        }
        let mut target_classes: Vec<String> = Vec::new();
        if implicit {
            target_classes.push(subject.as_iri().unwrap_or_default().to_string());
        }
        let declared = index.objects(subject, &format!("{SH}targetClass"));
        target_classes.extend(
            declared
                .iter()
                .filter_map(|t| t.as_iri())
                .map(str::to_string),
        );
        let mut unsupported: BTreeSet<String> = targets
            .iter()
            .filter(|p| !p.ends_with("#targetClass"))
            .map(|p| (*p).to_string())
            .collect();
        if declared.iter().any(|t| !t.is_iri()) {
            unsupported.insert(format!("{SH}targetClass"));
        }
        let mut properties = BTreeSet::new();
        if !index.objects(subject, &format!("{SH}path")).is_empty() {
            match property_shape(&index, subject, TARGETS) {
                Ok(shape) => {
                    properties.insert(shape);
                }
                Err(problem) => {
                    unsupported.insert(problem);
                }
            }
        } else {
            unsupported.extend(
                index
                    .predicates(subject)
                    .into_iter()
                    .filter(|p| {
                        sh_local(p).is_some_and(|l| {
                            !TARGETS.contains(&l) && l != "property" && !INFORMATIONAL.contains(&l)
                        })
                    })
                    .map(str::to_string),
            );
            for node in index.objects(subject, &format!("{SH}property")) {
                if node.is_literal() {
                    unsupported.insert(format!("{SH}property (not a node)"));
                    continue;
                }
                match property_shape(&index, node, &[]) {
                    Ok(shape) => {
                        properties.insert(shape);
                    }
                    Err(problem) => {
                        unsupported.insert(problem);
                    }
                }
            }
        }
        target_classes.sort();
        target_classes.dedup();
        out.insert(NodeShape {
            id: subject.as_iri().map(str::to_string),
            targets: target_classes,
            properties: properties.into_iter().collect(),
            unsupported: unsupported.into_iter().collect(),
        });
    }
    out.into_iter().collect()
}

/// A class or property the model input shows.
#[derive(Clone, Debug, Default)]
pub struct ShownTerm {
    pub iri: String,
    pub label: Option<String>,
    pub comment: Option<String>,
    /// The one parent class shown (classes only).
    pub parent: Option<String>,
}

/// The classes and properties the model reads, from an adapted ontology.
#[derive(Clone, Debug, Default)]
pub struct Shown {
    pub classes: BTreeMap<String, ShownTerm>,
    pub properties: BTreeMap<String, ShownTerm>,
    pub shapes: Vec<NodeShape>,
}

impl Shown {
    pub fn read(adapted: &[Quad]) -> Self {
        let mut by_subject: HashMap<&Term, HashMap<&str, Vec<&Term>>> = HashMap::new();
        for q in adapted {
            by_subject
                .entry(&q.subject)
                .or_default()
                .entry(&q.predicate)
                .or_default()
                .push(&q.object);
        }
        // the first value in the order the prepared ontology is written:
        // sorted as N-Triples
        let first = |s: &Term, p: &str| -> Option<String> {
            let mut objects = by_subject.get(s)?.get(p)?.clone();
            objects.sort_by_key(|o| nt(o));
            objects.first().map(|o| match literal(o) {
                Some((text, ..)) => text,
                None => o.as_iri().unwrap_or_default().to_string(),
            })
        };
        let mut shown = Shown::default();
        for q in adapted {
            let Some(iri) = q.subject.as_iri() else {
                continue;
            };
            if !q.is(RDF_TYPE) {
                continue;
            }
            let into = match q.object.as_iri() {
                Some(RDFS_CLASS) => &mut shown.classes,
                Some(RDF_PROPERTY) => &mut shown.properties,
                _ => continue,
            };
            into.entry(iri.to_string()).or_insert_with(|| ShownTerm {
                iri: iri.to_string(),
                label: first(&q.subject, RDFS_LABEL),
                comment: first(&q.subject, RDFS_COMMENT),
                parent: None,
            });
        }
        for (iri, term) in &mut shown.classes {
            term.parent = first(&Term::iri(iri), RDFS_SUBCLASS_OF);
        }
        shown.shapes = compile_shapes(adapted)
            .into_iter()
            .filter(|s| s.targets.iter().any(|t| shown.classes.contains_key(t)))
            .collect();
        shown
    }
}

/// The caller's ontology triples the model input does not show, counted by
/// predicate, with example subjects. `original` is the ontology as written,
/// before [`normalize`] and [`adapt`].
pub fn not_shown(
    original: &[Quad],
    adapted: &Adapted,
    shown: &Shown,
) -> (BTreeMap<String, usize>, BTreeMap<String, Vec<String>>) {
    let kept: HashSet<(&Term, &str, &Term)> = adapted
        .quads
        .iter()
        .map(|q| (&q.subject, &*q.predicate, &q.object))
        .collect();
    let shape_nodes = shape_nodes(original);
    let mut domains_shown = HashSet::new();
    let mut ranges_shown = HashSet::new();
    for shape in &shown.shapes {
        for target in &shape.targets {
            for ps in &shape.properties {
                domains_shown.insert((ps.path.clone(), target.clone()));
                if let Some(value) = &ps.value {
                    ranges_shown.insert((ps.path.clone(), value.clone()));
                }
            }
        }
    }
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut examples: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut seen = HashSet::new();
    for q in original {
        let (s, p, o) = (&q.subject, &*q.predicate, &q.object);
        if !seen.insert((s, p, o)) {
            continue;
        }
        let type_key = |o: &Term| {
            let value = o
                .as_iri()
                .map(str::to_string)
                .or_else(|| literal(o).map(|(t, ..)| t))
                .unwrap_or_else(|| nt(o));
            format!("rdf:type {}", short(&value))
        };
        let key = if !kept.contains(&(s, p, o)) {
            Some(if p == RDF_TYPE && o.is_iri() {
                type_key(o)
            } else {
                short(p)
            })
        } else if shape_nodes.contains(s) {
            None
        } else {
            let name = s.as_iri();
            let class = name.and_then(|n| shown.classes.get(n));
            let property = name.is_some_and(|n| shown.properties.contains_key(n));
            let pair = |set: &HashSet<(String, String)>| {
                o.as_iri().is_some_and(|v| {
                    set.contains(&(name.unwrap_or_default().to_string(), v.to_string()))
                })
            };
            // in the policy's order: a borrowed SKOS text, a shown declaration,
            // label or comment, a domain or range a shape restates
            let shown_here = adapted
                .borrowed
                .contains(&(s.clone(), p.to_string(), o.clone()))
                || p == RDF_TYPE && o.as_iri().is_some_and(|t| SHOWN_TYPES.contains(&t))
                || SHOWN_PREDICATES.contains(&p)
                || p == RDFS_DOMAIN && pair(&domains_shown)
                || p == RDFS_RANGE && pair(&ranges_shown);
            if class.is_none() && !property {
                Some(NOT_A_SHOWN_TERM.to_string())
            } else if shown_here {
                None
            } else if p == RDF_TYPE {
                Some(type_key(o))
            } else if p == RDFS_SUBCLASS_OF && class.is_some() {
                let parent = class.and_then(|c| c.parent.as_deref());
                if o.as_iri().is_some() && o.as_iri() == parent {
                    None
                } else {
                    Some(SECOND_PARENT.to_string())
                }
            } else {
                Some(short(p))
            }
        };
        if let Some(key) = key {
            *counts.entry(key.clone()).or_default() += 1;
            note(&mut examples, &key, s);
        }
    }
    let unsupported: usize = shown
        .shapes
        .iter()
        .map(|s| {
            s.unsupported.len()
                + s.properties
                    .iter()
                    .map(|p| p.unsupported.len())
                    .sum::<usize>()
        })
        .sum();
    if unsupported > 0 {
        counts.insert(SH_UNSUPPORTED.to_string(), unsupported);
    }
    (counts, examples)
}

/// A count map as a response header writes it: `key=n,key=n`.
pub fn header_value(counts: &BTreeMap<String, usize>) -> String {
    counts
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}
