//! Rewriting an ontology into the shape the extraction model reads, and
//! writing it out.
//!
//! [`rewrite`] applies the input policy to the file itself instead of at
//! request time: SKOS labels and definitions become `rdfs:label` and
//! `rdfs:comment`, a term without a label gets one from its IRI, OWL
//! declarations gain their RDFS ones, and OWL restrictions and other
//! blank-node constructs are dropped. A label that reads as an identifier
//! (`employer_id`, `AboutPage`) is rewritten in words (`employer id`,
//! `about page`). Domains, ranges and other axioms stay: the model doesn't
//! see them, but a reading is still checked against them. What a reader
//! refuses wherever it appears (`xt:example`, extraction results) is
//! dropped, and a result record goes whole: a blank node that carries a
//! result, or an IRI with nothing left but its types once its results go
//! (either would be refused as a catalog record). An optional slice cuts the
//! vocabulary first.
//!
//! The catalog, if the file has one, is kept, except that a record with
//! `rdfs:label` and no SKOS label (a vocabulary's enumeration members) gets
//! its label as `skos:prefLabel`, and its `rdfs:comment` as
//! `skos:definition`: catalog records are read by their SKOS text.

use crate::policy;
use crate::read::{Parsed, Quad};
use crate::slice::{self, Slice, UnknownSeed};
use crate::vocab::{
    RDFS_COMMENT, RDFS_LABEL, RDF_TYPE, RESULT_PREDICATES, SKOS_ALT_LABEL, SKOS_DEFINITION,
    SKOS_HIDDEN_LABEL, SKOS_PREF_LABEL, WELL_KNOWN_PREFIXES, XT_EXAMPLE,
};
use crate::words;
use fluree_graph_format::{
    format_jsonld, format_turtle, BlankNodePolicy, ContextPolicy, JsonLdFormatConfig, PrefixMap,
    TypeHandling,
};
use fluree_graph_ir::{Graph, Term};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    Turtle,
    JsonLd,
}

/// The rewritten ontology (and catalog), with what was done to it.
#[derive(Clone, Debug, Default)]
pub struct Rewritten {
    pub quads: Vec<Quad>,
    pub adapted: BTreeMap<String, usize>,
    pub slice: Option<Slice>,
    pub classes: usize,
    pub properties: usize,
    pub records: usize,
}

/// The ontology in `parsed`, rewritten (see the module docs), sliced to
/// `seeds` first when there are any.
pub fn rewrite(parsed: &Parsed, seeds: &[String], depth: usize) -> Result<Rewritten, UnknownSeed> {
    let split = parsed.split();
    let (ontology, slice) = if seeds.is_empty() {
        (split.ontology, None)
    } else {
        let seeds = seeds
            .iter()
            .map(|s| slice::resolve_seed(s, &split.ontology, &parsed.prefixes))
            .collect::<Result<Vec<_>, _>>()?;
        let s = slice::slice(&split.ontology, &seeds, depth);
        (s.quads.clone(), Some(s))
    };
    let adapted = policy::adapt(policy::normalize(&ontology));
    let shown = policy::Shown::read(&adapted.quads);
    // a SKOS statement a fallback read is now an RDFS one; the copy goes
    let mut quads: Vec<Quad> = adapted
        .quads
        .into_iter()
        .filter(|q| {
            !adapted.borrowed.contains(&(
                q.subject.clone(),
                q.predicate.to_string(),
                q.object.clone(),
            ))
        })
        .collect();
    let mut counts = adapted.counts;
    let relabelled = label_words(&mut quads, &shown);
    if relabelled > 0 {
        counts.insert(LABELS_IN_WORDS.to_string(), relabelled);
    }
    let mut catalog = match &slice {
        Some(s) => slice::slice_catalog(&split.catalog, s),
        None => split.catalog,
    };
    // OWL axioms and annotation property declarations would be read as
    // records, and refused: they go, counted
    let (machinery, roots) = crate::read::owl_machinery(&catalog);
    if !machinery.is_empty() {
        catalog.retain(|q| !machinery.contains(&q.subject));
        let axioms = roots.iter().filter(|(s, _)| s.is_blank()).count();
        let declarations = roots.len() - axioms;
        if axioms > 0 {
            counts.insert(OWL_AXIOMS_DROPPED.to_string(), axioms);
        }
        if declarations > 0 {
            counts.insert(DECLARATIONS_DROPPED.to_string(), declarations);
        }
    }
    for (key, n) in skos_for_records(&mut catalog) {
        counts.insert(key.to_string(), n);
    }
    // refused wherever they appear: they go, counted
    let (examples, annotations) = (
        count_dropped(&mut quads, &mut catalog, |q| &*q.predicate == XT_EXAMPLE),
        drop_results(&mut quads, &mut catalog),
    );
    let records = crate::check::records(&catalog).len();
    quads.extend(catalog);
    for (key, n) in [
        (EXAMPLES_DROPPED, examples),
        (ANNOTATIONS_DROPPED, annotations.statements),
        (RESULT_RECORDS_DROPPED, annotations.records_dropped),
        (ANNOTATED_RECORDS_KEPT, annotations.records_kept),
    ] {
        if n > 0 {
            counts.insert(key.to_string(), n);
        }
    }
    for q in &mut quads {
        q.graph = None;
    }
    Ok(Rewritten {
        quads,
        adapted: counts,
        slice,
        classes: shown.classes.len(),
        properties: shown.properties.len(),
        records,
    })
}

/// What [`rewrite`] reports beside the input policy's own keys.
pub const LABELS_IN_WORDS: &str = "rdfs:label in words";
pub const OWL_AXIOMS_DROPPED: &str = "OWL axioms dropped";
pub const DECLARATIONS_DROPPED: &str = "annotation property and datatype declarations dropped";
pub const RECORD_LABELS: &str = "skos:prefLabel from rdfs:label (catalog)";
pub const RECORD_DEFINITIONS: &str = "skos:definition from rdfs:comment (catalog)";
pub const EXAMPLES_DROPPED: &str = "xt:example dropped";
pub const ANNOTATIONS_DROPPED: &str = "extraction annotations dropped";
pub const RESULT_RECORDS_DROPPED: &str = "extraction result records dropped whole";
pub const ANNOTATED_RECORDS_KEPT: &str =
    "records kept without their extraction annotations (check they are records)";

/// Drop the statements `refused` matches from both halves; how many.
fn count_dropped(
    quads: &mut Vec<Quad>,
    catalog: &mut Vec<Quad>,
    refused: impl Fn(&Quad) -> bool,
) -> usize {
    let before = quads.len() + catalog.len();
    quads.retain(|q| !refused(q));
    catalog.retain(|q| !refused(q));
    before - quads.len() - catalog.len()
}

/// What dropping extraction results took.
struct ResultsDropped {
    /// Result statements (`xt:entity`, `xt:score`, ...).
    statements: usize,
    /// Subjects dropped whole, as results and nothing else.
    records_dropped: usize,
    /// Subjects with a label or description of their own, kept without
    /// their results: they may be real records.
    records_kept: usize,
}

/// Drop extraction results. A subject that carries one goes whole when it
/// can only be a result: a blank node, or an IRI with nothing but `rdf:type`
/// besides its results (kept, it would be a record without a label). So do
/// statements pointing at a blank node that went. Any other subject keeps
/// everything but its results.
fn drop_results(quads: &mut Vec<Quad>, catalog: &mut Vec<Quad>) -> ResultsDropped {
    let is_result = |q: &Quad| RESULT_PREDICATES.contains(&&*q.predicate);
    let carriers: HashSet<Term> = quads
        .iter()
        .chain(catalog.iter())
        .filter(|q| is_result(q))
        .map(|q| q.subject.clone())
        .collect();
    let mut own: HashSet<Term> = HashSet::new();
    for q in quads.iter().chain(catalog.iter()) {
        if carriers.contains(&q.subject) && !is_result(q) && &*q.predicate != RDF_TYPE {
            own.insert(q.subject.clone());
        }
    }
    let whole: HashSet<Term> = carriers
        .iter()
        .filter(|s| s.is_blank() || !own.contains(*s))
        .cloned()
        .collect();
    let results = quads
        .iter()
        .chain(catalog.iter())
        .filter(|q| is_result(q))
        .count();
    let refused = |q: &Quad| {
        is_result(q)
            || whole.contains(&q.subject)
            || (q.object.is_blank() && whole.contains(&q.object))
    };
    count_dropped(quads, catalog, refused);
    ResultsDropped {
        statements: results,
        records_dropped: whole.len(),
        records_kept: carriers.len() - whole.len(),
    }
}

/// Rewrite shown terms' labels that read as identifiers into words; how
/// many were.
fn label_words(quads: &mut [Quad], shown: &policy::Shown) -> usize {
    let mut n = 0;
    for q in quads.iter_mut() {
        if !q.is(RDFS_LABEL) {
            continue;
        }
        let Some(iri) = q.subject.as_iri() else {
            continue;
        };
        if !shown.classes.contains_key(iri) && !shown.properties.contains_key(iri) {
            continue;
        }
        let Some((text, lang, _)) = policy::literal(&q.object) else {
            continue;
        };
        if text.contains(' ')
            || !words::reads_as_identifier(&text) && !is_camel_word(&text)
            || is_brand_style(&text)
        {
            continue;
        }
        if let Some(in_words) = words::name_words(&text).filter(|w| *w != text) {
            q.object = match lang {
                Some(lang) => Term::lang_string(in_words, lang),
                None => Term::string(in_words),
            };
            n += 1;
        }
    }
    n
}

/// `AboutPage`: one token of capitalized words run together.
/// `iPhone`, `eBay`: one lower-case letter before a capital is a name
/// written that way, not words run together.
fn is_brand_style(text: &str) -> bool {
    let mut c = text.chars();
    matches!((c.next(), c.next()), (Some(a), Some(b)) if a.is_lowercase() && b.is_uppercase())
}

fn is_camel_word(text: &str) -> bool {
    text.chars().skip(1).any(|c| c.is_ascii_uppercase())
        && text.chars().any(|c| c.is_ascii_lowercase())
}

/// Give catalog records with only RDFS text their SKOS label and
/// definition; how many of each.
fn skos_for_records(catalog: &mut Vec<Quad>) -> Vec<(&'static str, usize)> {
    let has = |p: &str| -> HashSet<Term> {
        catalog
            .iter()
            .filter(|q| q.is(p))
            .map(|q| q.subject.clone())
            .collect()
    };
    let mut labelled = has(SKOS_PREF_LABEL);
    labelled.extend(has(SKOS_ALT_LABEL));
    labelled.extend(has(SKOS_HIDDEN_LABEL));
    let defined = has(SKOS_DEFINITION);
    let mut added = Vec::new();
    let (mut labels, mut definitions) = (0, 0);
    let mut relabelled = HashSet::new();
    for q in catalog.iter() {
        if q.is(RDFS_LABEL) && !labelled.contains(&q.subject) {
            added.push(Quad::new(
                q.subject.clone(),
                SKOS_PREF_LABEL,
                q.object.clone(),
            ));
            if relabelled.insert(q.subject.clone()) {
                labels += 1;
            }
        } else if q.is(RDFS_COMMENT) && !defined.contains(&q.subject) {
            added.push(Quad::new(
                q.subject.clone(),
                SKOS_DEFINITION,
                q.object.clone(),
            ));
            definitions += 1;
        }
    }
    catalog.extend(added);
    [(RECORD_LABELS, labels), (RECORD_DEFINITIONS, definitions)]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .collect()
}

/// The prefixes a document's IRIs use, from `declared` (later declarations
/// win) and the well-known ones.
pub fn used_prefixes(quads: &[Quad], declared: &[(String, String)]) -> BTreeMap<String, String> {
    let mut candidates: BTreeMap<String, String> = WELL_KNOWN_PREFIXES
        .iter()
        .map(|(p, ns)| ((*p).to_string(), (*ns).to_string()))
        .collect();
    for (p, ns) in declared {
        if !p.is_empty() {
            candidates.insert(p.clone(), ns.clone());
        }
    }
    let iris: Vec<&str> = quads
        .iter()
        .flat_map(|q| {
            [
                q.subject.as_iri(),
                Some(&*q.predicate),
                q.object.as_iri(),
                q.object.as_literal().map(|(_, dt, _)| dt.as_iri()),
            ]
        })
        .flatten()
        .collect();
    candidates
        .into_iter()
        .filter(|(_, ns)| iris.iter().any(|iri| iri.starts_with(ns.as_str())))
        .collect()
}

/// The quads as one graph, blank nodes relabelled `b0`, `b1`, ... in order
/// of first appearance, sorted by subject.
fn graph(quads: &[Quad]) -> Graph {
    let mut labels: HashMap<String, String> = HashMap::new();
    let mut relabel = |t: &Term| -> Term {
        match t.as_blank() {
            Some(id) => {
                let next = labels.len();
                Term::blank(
                    labels
                        .entry(id.as_str().to_string())
                        .or_insert_with(|| format!("b{next}")),
                )
            }
            None => t.clone(),
        }
    };
    let mut g = Graph::new();
    for q in quads {
        let s = relabel(&q.subject);
        let o = relabel(&q.object);
        g.add_triple(s, Term::iri(&*q.predicate), o);
    }
    g.sort();
    g.dedupe();
    g
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("{0}")]
    Format(String),
}

/// Serialize quads (graph names dropped) as Turtle or JSON-LD.
pub fn serialize(
    quads: &[Quad],
    prefixes: &BTreeMap<String, String>,
    format: OutputFormat,
) -> Result<String, WriteError> {
    let g = graph(quads);
    let map = PrefixMap::from_map(prefixes.clone());
    match format {
        OutputFormat::Turtle => {
            format_turtle(&g, &map).map_err(|e| WriteError::Format(e.to_string()))
        }
        OutputFormat::JsonLd => {
            let context: serde_json::Map<String, serde_json::Value> = prefixes
                .iter()
                .map(|(p, ns)| (p.clone(), serde_json::Value::String(ns.clone())))
                .collect();
            let compact = map.clone();
            let config = JsonLdFormatConfig::new()
                .with_context_policy(ContextPolicy::UseProvided(serde_json::Value::Object(
                    context,
                )))
                .with_type_handling(TypeHandling::AsAtType)
                .with_blank_node_policy(BlankNodePolicy::Deterministic)
                .with_compactor(move |iri| compact.compact(iri).unwrap_or_else(|| iri.to_string()));
            let json = format_jsonld(&g, &config).map_err(|e| WriteError::Format(e.to_string()))?;
            serde_json::to_string_pretty(&json).map_err(|e| WriteError::Format(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::{parse, RdfFormat};

    #[test]
    fn what_a_reader_refuses_is_dropped() {
        let parsed = parse(
            r#"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
               @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
               @prefix xt: <https://ns.flur.ee/extract#> .
               @prefix ex: <https://ex.com/> .
               ex:Commitment a rdfs:Class, xt:EventClass ; rdfs:label "commitment" ;
                 xt:example "I'll call you back", "we will send the quote" .
               ex:callback a ex:Commitment ; skos:prefLabel "callback" ;
                 xt:accepted true ; xt:linkConfidence 0.8 ."#,
            RdfFormat::Turtle,
            None,
        )
        .unwrap();
        let out = rewrite(&parsed, &[], 1).unwrap();
        assert_eq!(out.adapted.get(EXAMPLES_DROPPED), Some(&2));
        assert_eq!(out.adapted.get(ANNOTATIONS_DROPPED), Some(&2));
        // a labelled record keeps its label, and the summary says so
        assert_eq!(out.adapted.get(ANNOTATED_RECORDS_KEPT), Some(&1));
        assert_eq!(out.adapted.get(RESULT_RECORDS_DROPPED), None);
        assert_eq!(out.records, 1);
        // the event class keeps its declaration
        assert!(out
            .quads
            .iter()
            .any(|q| q.object.as_iri() == Some(crate::vocab::XT_EVENT_CLASS)));
        let ttl = serialize(
            &out.quads,
            &used_prefixes(&out.quads, &parsed.prefixes),
            OutputFormat::Turtle,
        )
        .unwrap();
        let again = parse(&ttl, RdfFormat::Turtle, None).unwrap();
        let report = crate::check::check(&again, &Default::default()).unwrap();
        assert!(report.passes(), "{:?}", report.will_fail);
        assert_eq!(report.event_classes, 1);
    }

    /// A subject that is only a result goes whole, blank or not, with what
    /// points at a blank one, so what `from` writes passes `check`.
    #[test]
    fn a_result_record_is_dropped_whole() {
        let parsed = parse(
            r#"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
               @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
               @prefix xt: <https://ns.flur.ee/extract#> .
               @prefix ex: <https://ex.com/> .
               ex:Claim a rdfs:Class ; rdfs:label "claim" .
               ex:Person a rdfs:Class ; rdfs:label "person" .
               ex:alice a ex:Person ; skos:prefLabel "Alice" ; ex:made [ a ex:Claim ; xt:entity ex:alice ; xt:score 0.9 ] .
               ex:c1 a ex:Claim ; xt:entity ex:alice ; xt:accepted true ."#,
            RdfFormat::Turtle,
            None,
        )
        .unwrap();
        let out = rewrite(&parsed, &[], 1).unwrap();
        assert_eq!(out.adapted.get(RESULT_RECORDS_DROPPED), Some(&2));
        assert_eq!(out.adapted.get(ANNOTATIONS_DROPPED), Some(&4));
        assert_eq!(out.adapted.get(ANNOTATED_RECORDS_KEPT), None);
        assert_eq!(out.records, 1);
        let alice = Term::iri("https://ex.com/alice");
        assert!(!out
            .quads
            .iter()
            .any(|q| q.subject.is_blank() || q.object.is_blank()));
        assert!(!out
            .quads
            .iter()
            .any(|q| q.subject == Term::iri("https://ex.com/c1")));
        assert!(out.quads.iter().any(|q| q.subject == alice));
        let ttl = serialize(
            &out.quads,
            &used_prefixes(&out.quads, &parsed.prefixes),
            OutputFormat::Turtle,
        )
        .unwrap();
        let again = parse(&ttl, RdfFormat::Turtle, None).unwrap();
        let report = crate::check::check(&again, &Default::default()).unwrap();
        assert!(report.passes(), "{:?}", report.will_fail);
        assert_eq!(report.records, 1);
    }

    #[test]
    fn skos_and_owl_become_what_the_model_reads() {
        let parsed = parse(
            r#"@prefix owl: <http://www.w3.org/2002/07/owl#> .
               @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
               @prefix ex: <https://ex.com/> .
               ex:Custodian a owl:Class ;
                 skos:prefLabel "custodian"@en, "dépositaire"@fr ;
                 skos:definition "A bank that holds a fund's assets." ;
                 owl:equivalentClass [ a owl:Restriction ; owl:onProperty ex:holds ; owl:someValuesFrom ex:Asset ] .
               ex:holds a owl:ObjectProperty ."#,
            RdfFormat::Turtle,
            None,
        )
        .unwrap();
        let out = rewrite(&parsed, &[], 1).unwrap();
        let ttl = serialize(
            &out.quads,
            &used_prefixes(&out.quads, &parsed.prefixes),
            OutputFormat::Turtle,
        )
        .unwrap();
        assert!(ttl.contains("rdfs:label \"custodian\"@en"), "{ttl}");
        assert!(ttl.contains("rdfs:comment \"A bank"), "{ttl}");
        assert!(
            ttl.contains("\"dépositaire\"@fr"),
            "the other translation stays: {ttl}"
        );
        assert!(!ttl.contains("Restriction"), "{ttl}");
        assert!(ttl.contains("rdfs:label \"holds\""), "{ttl}");
        assert_eq!(out.adapted.get(policy::DROPPED_BLANK), Some(&4));
        // the rewrite reads back with nothing left to adapt
        let again = parse(&ttl, RdfFormat::Turtle, None).unwrap();
        let report = crate::check(&again, &crate::CheckOptions::default()).unwrap();
        assert!(report.adapted.is_empty(), "{:?}", report.adapted);
        let jsonld = serialize(
            &out.quads,
            &used_prefixes(&out.quads, &parsed.prefixes),
            OutputFormat::JsonLd,
        )
        .unwrap();
        let back = parse(&jsonld, RdfFormat::JsonLd, None).unwrap();
        assert_eq!(back.quads.len(), again.quads.len(), "{jsonld}");
    }

    #[test]
    fn labels_in_words_but_not_brand_names() {
        let parsed = parse(
            r#"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
               @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
               <https://ex.com/worksFor> a rdf:Property ; rdfs:label "worksFor" .
               <https://ex.com/Phone> a rdfs:Class ; rdfs:label "iPhone" .
               <https://ex.com/Shop> a rdfs:Class ; rdfs:label "eBay" .
               <https://ex.com/Page> a rdfs:Class ; rdfs:label "AboutPage" ."#,
            RdfFormat::Turtle,
            None,
        )
        .unwrap();
        let out = rewrite(&parsed, &[], 1).unwrap();
        let label = |iri: &str| -> String {
            out.quads
                .iter()
                .find(|q| q.subject.as_iri() == Some(iri) && q.is(RDFS_LABEL))
                .and_then(|q| policy::literal(&q.object))
                .map(|(t, ..)| t)
                .unwrap()
        };
        assert_eq!(label("https://ex.com/worksFor"), "works for");
        assert_eq!(label("https://ex.com/Page"), "about page");
        assert_eq!(label("https://ex.com/Phone"), "iPhone");
        assert_eq!(label("https://ex.com/Shop"), "eBay");
        assert_eq!(out.adapted.get(LABELS_IN_WORDS), Some(&2));
    }

    #[test]
    fn owl_machinery_is_dropped_and_counted() {
        let parsed = parse(
            r#"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
               @prefix owl: <http://www.w3.org/2002/07/owl#> .
               @prefix dc: <http://purl.org/dc/elements/1.1/> .
               <https://ex.com/p> a owl:Ontology ; dc:creator "Someone" .
               dc:creator a owl:AnnotationProperty .
               <https://ex.com/A> a owl:Class ; rdfs:label "a thing" .
               <https://ex.com/B> a owl:Class ; rdfs:label "b thing" .
               [] a owl:AllDisjointClasses ; owl:members ( <https://ex.com/A> <https://ex.com/B> ) .
               <https://ex.com/acme> a <https://ex.com/A> ; rdfs:label "Acme" ."#,
            RdfFormat::Turtle,
            None,
        )
        .unwrap();
        let before = crate::check(&parsed, &crate::CheckOptions::default()).unwrap();
        assert!(
            before.will_fail.iter().any(|f| f.code == "owl-machinery"),
            "{:?}",
            before.will_fail
        );
        let out = rewrite(&parsed, &[], 1).unwrap();
        assert_eq!(out.adapted.get(OWL_AXIOMS_DROPPED), Some(&1));
        assert_eq!(out.adapted.get(DECLARATIONS_DROPPED), Some(&1));
        assert_eq!(out.records, 1, "the real record stays");
        let ttl = serialize(
            &out.quads,
            &used_prefixes(&out.quads, &parsed.prefixes),
            OutputFormat::Turtle,
        )
        .unwrap();
        let again = parse(&ttl, RdfFormat::Turtle, None).unwrap();
        let after = crate::check(&again, &crate::CheckOptions::default()).unwrap();
        assert!(after.passes(), "{:?}\n{ttl}", after.will_fail);
    }
}
