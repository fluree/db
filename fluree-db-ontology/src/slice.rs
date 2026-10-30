//! Slicing a large vocabulary to the classes reachable from a seed set.
//!
//! Every class and property in a request's ontology is shown to the model in
//! every window, so a whole public vocabulary (schema.org, FIBO) costs tens
//! of thousands of tokens and blurs which of many overlapping terms to use.
//! A slice keeps what a set of seed classes needs:
//!
//! - the seeds and all their ancestors (`rdfs:subClassOf`);
//! - the properties whose domain is one of those classes (`rdfs:domain`, or
//!   schema.org's `domainIncludes`, an `owl:unionOf` domain counting each
//!   member), and the classes in their ranges, with their ancestors;
//! - repeated `depth` times from the classes the last step added.
//!
//! "Reachable" stops at a depth on purpose: following every range of a
//! vocabulary built on one root class reaches nearly all of it. A property
//! with no domain at all is left out and counted.

use crate::read::Quad;
use crate::vocab::*;
use fluree_graph_ir::Term;
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Slice {
    #[serde(skip)]
    pub quads: Vec<Quad>,
    /// The classes kept (with any seed the file doesn't declare).
    #[serde(skip)]
    pub classes: BTreeSet<String>,
    pub seeds: Vec<String>,
    pub depth: usize,
    pub classes_kept: usize,
    pub classes_total: usize,
    pub properties_kept: usize,
    pub properties_total: usize,
    /// Properties left out because they declare no domain.
    pub without_domain: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum UnknownSeed {
    #[error("no class in the ontology matches --seed {0:?} (give an IRI, a prefixed name or a class label)")]
    Unknown(String),
    #[error("--seed {0:?} matches several classes ({}); give an IRI or a prefixed name", .1.join(", "))]
    Ambiguous(String, Vec<String>),
}

const DOMAINS: &[&str] = &[
    RDFS_DOMAIN,
    "https://schema.org/domainIncludes",
    "http://schema.org/domainIncludes",
];
const RANGES: &[&str] = &[
    RDFS_RANGE,
    "https://schema.org/rangeIncludes",
    "http://schema.org/rangeIncludes",
];
const OWL_UNION_OF: &str = "http://www.w3.org/2002/07/owl#unionOf";

struct Vocabulary<'a> {
    objects: HashMap<(&'a Term, &'a str), Vec<&'a Term>>,
    classes: BTreeSet<&'a str>,
    properties: BTreeSet<&'a str>,
}

impl<'a> Vocabulary<'a> {
    fn new(quads: &'a [Quad]) -> Self {
        let mut v = Vocabulary {
            objects: HashMap::new(),
            classes: BTreeSet::new(),
            properties: BTreeSet::new(),
        };
        for q in quads {
            v.objects
                .entry((&q.subject, &*q.predicate))
                .or_default()
                .push(&q.object);
            if let (Some(s), true) = (q.subject.as_iri(), q.is(RDF_TYPE)) {
                match q.object.as_iri() {
                    Some(RDFS_CLASS | OWL_CLASS) => {
                        v.classes.insert(s);
                    }
                    Some(RDF_PROPERTY | OWL_OBJECT_PROPERTY | OWL_DATATYPE_PROPERTY) => {
                        v.properties.insert(s);
                    }
                    _ => {}
                }
            }
        }
        v
    }

    fn get(&self, s: &Term, p: &str) -> Vec<&'a Term> {
        self.objects.get(&(s, p)).cloned().unwrap_or_default()
    }

    /// The class IRIs a domain or range value names: an IRI, or the members
    /// of an `owl:unionOf` list.
    fn class_refs(&self, value: &'a Term, out: &mut Vec<&'a str>) {
        if let Some(iri) = value.as_iri() {
            out.push(iri);
            return;
        }
        for head in self.get(value, OWL_UNION_OF) {
            let mut node = head;
            let mut seen = HashSet::new();
            while node.is_blank() && seen.insert(node) {
                for first in self.get(node, RDF_FIRST) {
                    if let Some(iri) = first.as_iri() {
                        out.push(iri);
                    }
                }
                match self.get(node, RDF_REST).first() {
                    Some(next) => node = next,
                    None => break,
                }
            }
        }
    }

    fn refs(&self, term: &str, predicates: &[&str]) -> Vec<&'a str> {
        let subject = Term::iri(term);
        let mut out = Vec::new();
        for p in predicates {
            for value in self.get(&subject, p) {
                self.class_refs(value, &mut out);
            }
        }
        out
    }

    /// `class` and its ancestors, added to `keep`; the ones newly added.
    fn add_with_ancestors(&self, class: &'a str, keep: &mut BTreeSet<&'a str>) -> Vec<&'a str> {
        let mut added = Vec::new();
        let mut stack = vec![class];
        while let Some(c) = stack.pop() {
            if keep.insert(c) {
                added.push(c);
                for parent in self.refs(c, &[RDFS_SUBCLASS_OF]) {
                    stack.push(parent);
                }
            }
        }
        added
    }
}

/// Resolve a `--seed` name against an ontology: an IRI, a prefixed name
/// (the file's prefixes, then the well-known ones), or a class's label or
/// local name, compared without case.
pub fn resolve_seed(
    name: &str,
    quads: &[Quad],
    prefixes: &[(String, String)],
) -> Result<String, UnknownSeed> {
    let name = name.trim();
    let unknown = || UnknownSeed::Unknown(name.to_string());
    let vocabulary = Vocabulary::new(quads);
    // an IRI or a prefixed name must name a class the file declares, or the
    // slice would be empty
    let declared = |iri: String| -> Result<String, UnknownSeed> {
        if vocabulary.classes.contains(iri.as_str()) {
            Ok(iri)
        } else {
            Err(unknown())
        }
    };
    if let Some(iri) = name.strip_prefix('<').and_then(|n| n.strip_suffix('>')) {
        return declared(iri.to_string());
    }
    if let Some((prefix, local)) = name.split_once(':') {
        if !local.starts_with("//") {
            let ns = prefixes
                .iter()
                .rev()
                .map(|(p, ns)| (p.as_str(), ns.as_str()))
                .chain(WELL_KNOWN_PREFIXES.iter().copied())
                .find(|(p, _)| *p == prefix)
                .map(|(_, ns)| ns);
            if let Some(ns) = ns {
                return declared(format!("{ns}{local}"));
            }
        }
        if vocabulary.classes.contains(name) || name.contains("://") {
            return declared(name.to_string());
        }
    }
    let wanted = name.to_lowercase();
    let by_label: Vec<&str> = vocabulary
        .classes
        .iter()
        .copied()
        .filter(|c| {
            let subject = Term::iri(*c);
            [RDFS_LABEL, SKOS_PREF_LABEL]
                .iter()
                .flat_map(|p| vocabulary.get(&subject, p))
                .filter_map(|t| t.as_literal().map(|(v, ..)| v.lexical().to_lowercase()))
                .any(|label| label == wanted)
        })
        .collect();
    // a label names a class more surely than a local name does
    let matches = if by_label.is_empty() {
        vocabulary
            .classes
            .iter()
            .copied()
            .filter(|c| crate::words::local_name(c).to_lowercase() == wanted)
            .collect()
    } else {
        by_label
    };
    match matches.as_slice() {
        [] => Err(unknown()),
        [one] => Ok((*one).to_string()),
        several => Err(UnknownSeed::Ambiguous(
            name.to_string(),
            several.iter().map(|c| (*c).to_string()).collect(),
        )),
    }
}

/// The part of `ontology` the seed classes reach in `depth` steps.
pub fn slice(ontology: &[Quad], seeds: &[String], depth: usize) -> Slice {
    let v = Vocabulary::new(ontology);
    let mut keep: BTreeSet<&str> = BTreeSet::new();
    let mut frontier = Vec::new();
    for seed in seeds {
        // seeds are resolved against the file's classes (`resolve_seed`)
        let Some(seed) = v.classes.get(seed.as_str()).copied() else {
            continue;
        };
        frontier.extend(v.add_with_ancestors(seed, &mut keep));
    }
    let with_domain: Vec<(&str, Vec<&str>)> = v
        .properties
        .iter()
        .map(|p| (*p, v.refs(p, DOMAINS)))
        .collect();
    let without_domain = with_domain.iter().filter(|(_, d)| d.is_empty()).count();
    let mut kept_properties: BTreeSet<&str> = BTreeSet::new();
    for _ in 0..depth {
        let reached: HashSet<&str> = frontier.iter().copied().collect();
        let mut next = Vec::new();
        for (p, domains) in &with_domain {
            if domains.iter().any(|d| reached.contains(d)) && kept_properties.insert(p) {
                for range in v.refs(p, RANGES) {
                    if v.classes.contains(range) {
                        next.extend(v.add_with_ancestors(range, &mut keep));
                    }
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    // the last step's classes keep their own properties only if a further
    // step would have reached them, so properties are those found above
    let terms: HashSet<&str> = keep.iter().chain(kept_properties.iter()).copied().collect();
    let mut subjects: HashSet<&Term> = ontology
        .iter()
        .filter(|q| q.subject.as_iri().is_some_and(|s| terms.contains(s)))
        .map(|q| &q.subject)
        .collect();
    // shapes that target a kept class
    for q in ontology {
        if q.predicate.starts_with(SH)
            && q.predicate.ends_with("targetClass")
            && q.object.as_iri().is_some_and(|c| keep.contains(c))
        {
            subjects.insert(&q.subject);
        }
    }
    // and every blank node a kept subject points at
    let mut stack: Vec<&Term> = ontology
        .iter()
        .filter(|q| subjects.contains(&q.subject) && q.object.is_blank())
        .map(|q| &q.object)
        .collect();
    while let Some(node) = stack.pop() {
        if subjects.insert(node) {
            stack.extend(
                ontology
                    .iter()
                    .filter(|q| &q.subject == node && q.object.is_blank())
                    .map(|q| &q.object),
            );
        }
    }
    Slice {
        quads: ontology
            .iter()
            .filter(|q| subjects.contains(&q.subject))
            .cloned()
            .collect(),
        classes: keep.iter().map(|c| (*c).to_string()).collect(),
        seeds: seeds.to_vec(),
        depth,
        classes_kept: keep.iter().filter(|c| v.classes.contains(*c)).count(),
        classes_total: v.classes.len(),
        properties_kept: kept_properties.len(),
        properties_total: v.properties.len(),
        without_domain,
    }
}

/// The catalog records a slice still needs: those typed with a kept class,
/// and those with no type at all. A vocabulary file's enumeration members
/// (schema.org's) are records of its classes, and go with them.
pub fn slice_catalog(catalog: &[Quad], slice: &Slice) -> Vec<Quad> {
    let mut typed: HashMap<&Term, bool> = HashMap::new();
    for q in catalog {
        if q.is(RDF_TYPE) {
            let kept = q.object.as_iri().is_some_and(|c| slice.classes.contains(c));
            let entry = typed.entry(&q.subject).or_insert(false);
            *entry = *entry || kept;
        }
    }
    catalog
        .iter()
        .filter(|q| typed.get(&q.subject).is_none_or(|kept| *kept))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::{parse, RdfFormat};

    const VOCAB: &str = r#"
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix s: <https://schema.org/> .
        @prefix ex: <https://ex.com/> .
        ex:Thing a rdfs:Class ; rdfs:label "thing" .
        ex:Person a rdfs:Class ; rdfs:subClassOf ex:Thing ; rdfs:label "person" .
        ex:Organization a rdfs:Class ; rdfs:subClassOf ex:Thing ; rdfs:label "organization" .
        ex:Place a rdfs:Class ; rdfs:subClassOf ex:Thing ; rdfs:label "place" .
        ex:Vehicle a rdfs:Class ; rdfs:subClassOf ex:Thing ; rdfs:label "vehicle" .
        ex:name a rdf:Property ; rdfs:domain ex:Thing .
        ex:worksFor a rdf:Property ; s:domainIncludes ex:Person ; s:rangeIncludes ex:Organization .
        ex:location a rdf:Property ; rdfs:domain ex:Organization ; rdfs:range ex:Place .
        ex:wheels a rdf:Property ; rdfs:domain ex:Vehicle .
        ex:note a rdf:Property .
    "#;

    fn ontology() -> (Vec<Quad>, Vec<(String, String)>) {
        let parsed = parse(VOCAB, RdfFormat::Turtle, None).unwrap();
        (parsed.quads, parsed.prefixes)
    }

    #[test]
    fn one_step_from_a_seed() {
        let (quads, prefixes) = ontology();
        let seed = resolve_seed("ex:Person", &quads, &prefixes).unwrap();
        let s = slice(&quads, &[seed], 1);
        // Person, its ancestor Thing, and Organization through worksFor
        assert_eq!((s.classes_kept, s.classes_total), (3, 5));
        // name (Thing's) and worksFor; location is a second step away
        assert_eq!((s.properties_kept, s.properties_total), (2, 5));
        assert_eq!(s.without_domain, 1);
    }

    #[test]
    fn deeper_reaches_further() {
        let (quads, prefixes) = ontology();
        let seed = resolve_seed("person", &quads, &prefixes).unwrap();
        let s = slice(&quads, &[seed], 2);
        assert_eq!(s.classes_kept, 4);
        assert_eq!(s.properties_kept, 3);
    }

    #[test]
    fn seeds_resolve_by_iri_prefix_label() {
        let (quads, prefixes) = ontology();
        for name in ["<https://ex.com/Place>", "ex:Place", "Place", "place"] {
            assert_eq!(
                resolve_seed(name, &quads, &prefixes).unwrap(),
                "https://ex.com/Place"
            );
        }
        assert!(resolve_seed("Spaceship", &quads, &prefixes).is_err());
        // a prefixed name or IRI that names no declared class is refused too
        assert!(resolve_seed("ex:Persn", &quads, &prefixes).is_err());
        assert!(resolve_seed("<https://ex.com/Persn>", &quads, &prefixes).is_err());
        assert!(resolve_seed("https://ex.com/Persn", &quads, &prefixes).is_err());
        assert!(
            resolve_seed("ex:name", &quads, &prefixes).is_err(),
            "a property is not a class"
        );
    }
}
