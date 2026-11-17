//! What an HTTP reader will and won't read of an ontology and catalog.
//!
//! A [`Report`] has four parts:
//!
//! - **will fail**: what a request carrying this ontology would be refused
//!   for (no class, a term that can't be given a label, a catalog record
//!   without a label or an IRI, `xt:example` or an extraction result, over a
//!   request limit);
//! - **adapted**: what the model reads another way, by the input policy
//!   ([`crate::policy`]);
//! - **not shown**: what the model never sees, by predicate;
//! - **advice**: what the evidence says improves a reading (descriptions,
//!   labels in words rather than column names, a smaller vocabulary, event
//!   classes whose kinds the reader can find).
//!
//! `adapted` and `not_shown` use the keys an extraction response reports
//! them under, so they compare line for line with a real request's.

use crate::estimate::{self, Estimate, Sizes};
use crate::policy::{self, Adapted, Shown, ShownTerm};
use crate::read::{Parsed, Quad, Split};
use crate::slice::{self, Slice, UnknownSeed};
use crate::vocab::*;
use crate::words::{self, local_name};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// More properties than this in one prompt is more than the model was
/// shaped on.
pub const LARGE_PROPERTY_COUNT: usize = 96;
pub const LARGE_CLASS_COUNT: usize = 64;

#[derive(Clone, Debug, Default)]
pub struct CheckOptions {
    /// Slice the ontology to these classes first (IRIs, prefixed names or
    /// labels).
    pub seeds: Vec<String>,
    /// Steps a slice follows from the seeds.
    pub depth: usize,
    /// The file's size in bytes, checked against a request's limit.
    pub source_bytes: Option<usize>,
}

/// One thing the report says, with the terms it is about.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Finding {
    pub code: &'static str,
    pub message: String,
    /// How many terms or records it applies to.
    pub count: usize,
    /// Up to a few of them.
    pub examples: Vec<String>,
}

impl Finding {
    fn new(code: &'static str, message: impl Into<String>, terms: &[String]) -> Self {
        Finding {
            code,
            message: message.into(),
            count: terms.len(),
            examples: terms.iter().take(policy::EXAMPLES).cloned().collect(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Examples {
    pub adapted: BTreeMap<String, Vec<String>>,
    pub not_shown: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Report {
    /// The input policy mirrored.
    pub policy: &'static str,
    /// Classes and properties shown, and catalog records.
    pub classes: usize,
    pub properties: usize,
    pub records: usize,
    /// Of the classes, those declared `xt:EventClass` and read as event
    /// classes: an event subclass of one is read as one of its kinds instead.
    pub event_classes: usize,
    /// The event kinds the reader reads: one per catalog record of an event
    /// class's kind class, and one per event subclass.
    pub event_kinds: usize,
    /// Whether the file used a request's graph roles (`xt:ontology`, ...).
    pub by_graph: bool,
    pub will_fail: Vec<Finding>,
    pub adapted: BTreeMap<String, usize>,
    pub not_shown: BTreeMap<String, usize>,
    pub examples: Examples,
    pub advice: Vec<Finding>,
    pub estimate: Estimate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slice: Option<Slice>,
}

impl Report {
    /// No request carrying this ontology and catalog would be refused for
    /// them.
    pub fn passes(&self) -> bool {
        self.will_fail.is_empty()
    }
}

/// A catalog record as the model reads it.
#[derive(Clone, Debug, Default)]
pub struct Record {
    pub subject: Option<String>,
    pub labels: Vec<String>,
    pub definition: Option<String>,
    pub classes: Vec<String>,
}

/// The catalog's records: one per subject.
pub fn records(catalog: &[Quad]) -> Vec<Record> {
    let mut by_subject: BTreeMap<String, Record> = BTreeMap::new();
    for q in catalog {
        let key = policy::nt(&q.subject);
        let record = by_subject.entry(key).or_insert_with(|| Record {
            subject: q.subject.as_iri().map(str::to_string),
            ..Record::default()
        });
        let text = || policy::literal(&q.object).map(|(t, ..)| t);
        match &*q.predicate {
            SKOS_PREF_LABEL | SKOS_ALT_LABEL | SKOS_HIDDEN_LABEL => record.labels.extend(text()),
            SKOS_DEFINITION => {
                // the first by text, as the model reads one
                if let Some(t) = text() {
                    if record.definition.as_ref().is_none_or(|d| &t < d) {
                        record.definition = Some(t);
                    }
                }
            }
            RDF_TYPE => record.classes.extend(q.object.as_iri().map(str::to_string)),
            _ => {}
        }
    }
    by_subject.into_values().collect()
}

/// Check a parsed file.
pub fn check(parsed: &Parsed, options: &CheckOptions) -> Result<Report, UnknownSeed> {
    let Split {
        ontology,
        catalog,
        by_graph,
        ..
    } = parsed.split();
    let (ontology, slice) = if options.seeds.is_empty() {
        (ontology, None)
    } else {
        let seeds = options
            .seeds
            .iter()
            .map(|s| slice::resolve_seed(s, &ontology, &parsed.prefixes))
            .collect::<Result<Vec<_>, _>>()?;
        let s = slice::slice(&ontology, &seeds, options.depth);
        (s.quads.clone(), Some(s))
    };
    let catalog = match &slice {
        Some(s) => slice::slice_catalog(&catalog, s),
        None => catalog,
    };
    Ok(check_parts(&ontology, &catalog, by_graph, slice, options))
}

fn check_parts(
    ontology: &[Quad],
    catalog: &[Quad],
    by_graph: bool,
    slice: Option<Slice>,
    options: &CheckOptions,
) -> Report {
    let adapted: Adapted = policy::adapt(policy::normalize(ontology));
    let shown = Shown::read(&adapted.quads);
    let (not_shown, not_shown_examples) = policy::not_shown(ontology, &adapted, &shown);
    let mut will_fail = Vec::new();
    let mut advice = Vec::new();

    if ontology.is_empty() {
        will_fail.push(Finding::new(
            "no-ontology",
            "No ontology: the file declares no classes or properties.",
            &[],
        ));
    } else if shown.classes.is_empty() {
        will_fail.push(Finding::new(
            "no-class",
            "The ontology declares no class (rdfs:Class or owl:Class); a request needs at least one.",
            &[],
        ));
    }
    let unlabelled: Vec<String> = shown
        .classes
        .values()
        .chain(shown.properties.values())
        .filter(|t| t.label.is_none())
        .map(|t| t.iri.clone())
        .collect();
    if !unlabelled.is_empty() {
        will_fail.push(Finding::new(
            "no-label",
            "These terms have no rdfs:label or skos:prefLabel, and their IRIs' local names have no letters to make one from. Give each a label.",
            &unlabelled,
        ));
    }

    // OWL axioms and annotation property declarations are not records, but
    // a reader reads them as records and refuses them; say so in their terms
    let (machinery, machinery_roots) = crate::read::owl_machinery(catalog);
    let (machine_quads, record_quads): (Vec<Quad>, Vec<Quad>) = catalog
        .iter()
        .cloned()
        .partition(|q| machinery.contains(&q.subject));
    let refused = records(&machine_quads)
        .iter()
        .any(|r| r.subject.is_none() || r.labels.is_empty());
    if refused {
        let examples: Vec<String> = machinery_roots
            .iter()
            .map(|(s, t)| {
                s.as_iri()
                    .map_or_else(|| format!("[] a {}", short(t)), str::to_string)
            })
            .collect();
        will_fail.push(Finding::new(
            "owl-machinery",
            "OWL axioms (owl:AllDisjointClasses and the like) and declarations of annotation properties or datatypes are neither classes nor properties, so a reader reads them as catalog records, and refuses them (records need IRIs and SKOS labels). `fluree ontology from` drops them.",
            &examples,
        ));
    }
    let records = records(&record_quads);
    catalog_findings(&records, &shown, &mut will_fail, &mut advice);
    refused_terms(ontology, catalog, &mut will_fail);
    ontology_advice(&shown, &not_shown, &mut advice);
    let event_kinds = event_advice(ontology, catalog, &shown, &records, &mut advice);

    let terms: Vec<_> = shown
        .classes
        .values()
        .chain(shown.properties.values())
        .collect();
    let chars = |s: &Option<String>| s.as_deref().map_or(0, |s| s.chars().count());
    let sizes = Sizes {
        terms: terms.len(),
        ontology_chars: terms
            .iter()
            .map(|t| chars(&t.label) + chars(&t.comment))
            .sum(),
        records: records.len(),
        catalog_chars: records
            .iter()
            .map(|r| {
                r.labels
                    .iter()
                    .map(|l| l.chars().count() + 2)
                    .sum::<usize>()
                    + r.definition
                        .as_deref()
                        .map_or(0, estimate::definition_chars)
            })
            .sum(),
    };
    let estimate = estimate::estimate(sizes);
    if estimate.tokens > estimate.limit {
        will_fail.push(Finding::new(
            "window-full",
            format!(
                "The ontology and catalog come to about {} serialized tokens before any document text, over the {} one window holds. Slice the ontology with --seed, or send a smaller catalog.",
                thousands(estimate.tokens),
                thousands(estimate.limit)
            ),
            &[],
        ));
    } else if estimate.document_chars < estimate::MAX_TEXT_CHARS {
        advice.push(Finding::new(
            "little-room-for-text",
            format!(
                "The ontology and catalog leave room for about {} characters of document text in each window (a request may carry {} in up to {} windows, and the catalog is repeated in every one).",
                thousands(estimate.document_chars),
                thousands(estimate::MAX_TEXT_CHARS),
                estimate::MAX_WINDOWS
            ),
            &[],
        ));
    }
    if let Some(bytes) = options
        .source_bytes
        .filter(|b| *b > estimate::MAX_BODY_BYTES)
    {
        will_fail.push(Finding::new(
            "too-many-bytes",
            format!(
                "As written, this file alone is {} bytes, over the {} a request may carry with its document.",
                thousands(bytes),
                thousands(estimate::MAX_BODY_BYTES)
            ),
            &[],
        ));
    }

    Report {
        policy: policy::POLICY,
        classes: shown.classes.len(),
        properties: shown.properties.len(),
        records: records.len(),
        // an event subclass is read as a kind of its parent, not as an
        // event class of its own
        event_classes: shown.events.len() - shown.subtypes.len(),
        event_kinds,
        by_graph,
        will_fail,
        adapted: adapted.counts,
        not_shown,
        examples: Examples {
            adapted: adapted.examples,
            not_shown: not_shown_examples,
        },
        advice,
        estimate,
        slice,
    }
}

fn catalog_findings(
    records: &[Record],
    shown: &Shown,
    will_fail: &mut Vec<Finding>,
    advice: &mut Vec<Finding>,
) {
    let anonymous = records.iter().filter(|r| r.subject.is_none()).count();
    if anonymous > 0 {
        will_fail.push(Finding {
            code: "catalog-blank-node",
            message: "Catalog records need IRIs; these are blank nodes.".into(),
            count: anonymous,
            examples: Vec::new(),
        });
    }
    let named = |f: &dyn Fn(&Record) -> bool| -> Vec<String> {
        records
            .iter()
            .filter(|r| f(r))
            .filter_map(|r| r.subject.clone())
            .collect()
    };
    let unlabelled = named(&|r| r.labels.is_empty());
    if !unlabelled.is_empty() {
        will_fail.push(Finding::new(
            "catalog-no-label",
            "Catalog records need a SKOS label (skos:prefLabel, altLabel or hiddenLabel); these have none. In a file without graph roles, anything that isn't a declared class, property or shape is read as a catalog record.",
            &unlabelled,
        ));
    }
    let mut unknown: BTreeSet<&str> = BTreeSet::new();
    for r in records {
        for c in &r.classes {
            if !shown.classes.contains_key(c) {
                unknown.insert(c);
            }
        }
    }
    if !unknown.is_empty() {
        let unknown: Vec<String> = unknown.into_iter().map(str::to_string).collect();
        advice.push(Finding::new(
            "catalog-class-not-in-ontology",
            "Catalog records are typed with classes the ontology doesn't declare; the reader won't see those types.",
            &unknown,
        ));
    }
    let long = named(&|r| {
        r.definition
            .as_deref()
            .is_some_and(|d| d.chars().count() > estimate::DEFINITION_CHARS)
    });
    if !long.is_empty() {
        advice.push(Finding::new(
            "catalog-long-definition",
            format!(
                "The reader reads the first {} characters of a record's skos:definition; these are longer, and are cut.",
                estimate::DEFINITION_CHARS
            ),
            &long,
        ));
    }
}

/// Statements a reader refuses wherever they appear: `xt:example` (example
/// phrases are no longer read) and extraction results, which are not
/// evidence.
fn refused_terms(ontology: &[Quad], catalog: &[Quad], will_fail: &mut Vec<Finding>) {
    let subjects = |f: &dyn Fn(&str) -> bool| -> Vec<String> {
        let found: BTreeSet<String> = ontology
            .iter()
            .chain(catalog)
            .filter(|q| f(&q.predicate))
            .map(|q| {
                q.subject
                    .as_iri()
                    .map_or_else(|| policy::nt(&q.subject), str::to_string)
            })
            .collect();
        found.into_iter().collect()
    };
    let examples = subjects(&|p| p == XT_EXAMPLE);
    if !examples.is_empty() {
        will_fail.push(Finding::new(
            "example-refused",
            "A reader refuses xt:example. Say what the examples show in the description instead: rdfs:comment for a class or property, skos:definition for a catalog record. `fluree ontology from` drops them.",
            &examples,
        ));
    }
    let annotated = subjects(&|p| RESULT_PREDICATES.contains(&p));
    if !annotated.is_empty() {
        will_fail.push(Finding::new(
            "extraction-annotations",
            "These carry extraction results (xt:entity, xt:score, xt:accepted, rdf:reifies and the like), which a reader refuses as request input. `fluree ontology from` drops them.",
            &annotated,
        ));
    }
}

/// An event class's kinds, as a reader finds them: through the one class
/// labelled "<the event class's label> kind" and the one property labelled
/// "kind", from the catalog records of that class. A subclass of an event
/// class that is an event class too is read as one of its kinds. Returns how
/// many kinds are read.
fn event_advice(
    ontology: &[Quad],
    catalog: &[Quad],
    shown: &Shown,
    records: &[Record],
    advice: &mut Vec<Finding>,
) -> usize {
    let label = |t: &ShownTerm| t.label.as_deref().map(|l| l.trim().to_lowercase());
    let is_event = |t: &ShownTerm| shown.events.contains(&t.iri);
    // The classes that aren't event classes, by label, built once so a
    // class's kind class is one lookup: linear in the class count.
    let mut by_label: HashMap<String, Vec<&ShownTerm>> = HashMap::new();
    for class in shown.classes.values().filter(|c| !is_event(c)) {
        if let Some(l) = label(class) {
            by_label.entry(l).or_default().push(class);
        }
    }
    let kind_classes = |of: &ShownTerm| -> &[&ShownTerm] {
        label(of)
            .and_then(|l| by_label.get(&format!("{l} kind")))
            .map_or(&[], Vec::as_slice)
    };
    let kind_properties = shown
        .properties
        .values()
        .filter(|p| label(p).as_deref() == Some("kind"))
        .count();

    let mut unmarked = Vec::new();
    let mut subtypes = Vec::new();
    let mut ambiguous = Vec::new();
    let mut no_property = Vec::new();
    let mut no_records = Vec::new();
    let mut kinds_read = shown.subtypes.len();
    for class in shown.classes.values() {
        let kinds = kind_classes(class);
        if !is_event(class) {
            if !kinds.is_empty() {
                unmarked.push(class.iri.clone());
            }
            if class
                .parent
                .as_ref()
                .is_some_and(|p| shown.events.contains(p))
            {
                subtypes.push(class.iri.clone());
            }
            continue;
        }
        match kinds {
            [] => {}
            [_] if kind_properties != 1 => no_property.push(class.iri.clone()),
            [kind] => {
                let n = records
                    .iter()
                    .filter(|r| r.classes.contains(&kind.iri))
                    .count();
                if n == 0 {
                    no_records.push(class.iri.clone());
                }
                kinds_read += n;
            }
            _ => ambiguous.push(class.iri.clone()),
        }
    }
    let undeclared: BTreeSet<String> = ontology
        .iter()
        .chain(catalog)
        .filter(|q| q.is(RDF_TYPE) && q.object.as_iri() == Some(XT_EVENT_CLASS))
        .filter_map(|q| q.subject.as_iri())
        .filter(|iri| !shown.classes.contains_key(*iri))
        .map(str::to_string)
        .collect();
    let undeclared: Vec<String> = undeclared.into_iter().collect();

    let findings: [(&'static str, String, &[String]); 6] = [
        (
            "event-class-not-marked",
            "These classes have a kind class (one labelled \"<label> kind\") but aren't declared xt:EventClass, so the reader reads them as ordinary classes: it finds no events of them and reads no kinds. Declare each `a rdfs:Class, xt:EventClass` (xt: is https://ns.flur.ee/extract#).".into(),
            &unmarked,
        ),
        (
            "event-class-undeclared",
            "These are declared xt:EventClass but not as classes (rdfs:Class or owl:Class), so the reader doesn't read them as event classes. In a file without graph roles they are read as catalog records.".into(),
            &undeclared,
        ),
        (
            "event-subtype-not-marked",
            "These classes are subclasses of an event class but aren't declared xt:EventClass themselves, so the reader reads them as ordinary classes, not as kinds of the event. Declare them xt:EventClass too.".into(),
            &subtypes,
        ),
        (
            "event-kind-ambiguous",
            "These event classes have more than one class labelled \"<label> kind\", so the reader reads none of their kinds. Keep one.".into(),
            &ambiguous,
        ),
        (
            "event-kind-property",
            format!(
                "These event classes have a kind class, but the reader reads kinds through the one property labelled \"kind\", and the ontology has {kind_properties}."
            ),
            &no_property,
        ),
        (
            "event-kind-no-records",
            "These event classes have a kind class, but the catalog holds no records of it, so there are no kinds to read. Add a record for each kind (if they're in another file, check with --catalog).".into(),
            &no_records,
        ),
    ];
    for (code, message, terms) in findings {
        if !terms.is_empty() {
            advice.push(Finding::new(code, message, terms));
        }
    }
    kinds_read
}

fn ontology_advice(shown: &Shown, not_shown: &BTreeMap<String, usize>, advice: &mut Vec<Finding>) {
    let terms: Vec<_> = shown
        .classes
        .values()
        .chain(shown.properties.values())
        .collect();
    if terms.is_empty() {
        return;
    }
    let undescribed: Vec<String> = terms
        .iter()
        .filter(|t| t.comment.is_none())
        .map(|t| t.iri.clone())
        .collect();
    if !undescribed.is_empty() {
        advice.push(Finding::new(
            "no-description",
            format!(
                "{} of {} classes and properties have no description (rdfs:comment or skos:definition). After labels, descriptions help the reader most: say what counts and what doesn't, and name the words people use for it.",
                thousands(undescribed.len()),
                thousands(terms.len())
            ),
            &undescribed,
        ));
    }
    let identifier_labels: Vec<String> = terms
        .iter()
        .filter(|t| t.label.as_deref().is_some_and(words::reads_as_identifier))
        .map(|t| t.iri.clone())
        .collect();
    if !identifier_labels.is_empty() {
        advice.push(Finding::new(
            "label-not-words",
            "These labels read as column names rather than words. Write them as a person would (\"employer\", not \"employer_id\" or \"employerId\").",
            &identifier_labels,
        ));
    }
    let misleading: Vec<String> = terms
        .iter()
        .filter(|t| {
            t.label
                .as_deref()
                .is_some_and(|l| !words::reads_as_identifier(l))
                && words::ends_with_id(local_name(&t.iri))
        })
        .map(|t| t.iri.clone())
        .collect();
    if !misleading.is_empty() {
        advice.push(Finding::new(
            "local-name-not-words",
            "The reader reads an IRI's local name beside its label, and a local name like \"employer_id\" reads as a key column even when the label says \"employer\". Mint these IRIs from their labels.",
            &misleading,
        ));
    }
    let opaque: Vec<String> = terms
        .iter()
        .filter(|t| t.comment.is_none() && is_opaque(local_name(&t.iri)))
        .map(|t| t.iri.clone())
        .collect();
    if !opaque.is_empty() {
        advice.push(Finding::new(
            "opaque-local-name",
            "These terms have codes for local names and no description, so their labels are all the reader has to go on. Add descriptions.",
            &opaque,
        ));
    }
    if shown.properties.len() > LARGE_PROPERTY_COUNT || shown.classes.len() > LARGE_CLASS_COUNT {
        advice.push(Finding::new(
            "large-vocabulary",
            format!(
                "{} classes and {} properties. A large vocabulary costs tokens in every window and blurs which of several overlapping terms applies. Slice it to the classes your documents need with --seed.",
                thousands(shown.classes.len()),
                thousands(shown.properties.len())
            ),
            &[],
        ));
    }
    let hidden_axes: usize = [
        RDFS_DOMAIN,
        RDFS_RANGE,
        "https://schema.org/domainIncludes",
        "https://schema.org/rangeIncludes",
        "http://schema.org/domainIncludes",
        "http://schema.org/rangeIncludes",
    ]
    .iter()
    .filter_map(|p| not_shown.get(&short(p)))
    .sum();
    if hidden_axes > 0 {
        advice.push(Finding {
            code: "domain-range-not-shown",
            message: "The model behind an HTTP reader isn't shown rdfs:domain or rdfs:range (nor schema.org's domainIncludes and rangeIncludes), though the reader still writes its answer by them: a datatype range sets a value's type, and a domain or class range keeps an entity a node. To help the model find what a property connects, say it in the property's description (\"the organization that employs the person\"), or restate it as a SHACL property shape on the class.".into(),
            count: hidden_axes,
            examples: Vec::new(),
        });
    }
    if let Some(n) = not_shown.get(policy::SECOND_PARENT) {
        advice.push(Finding {
            code: "one-parent-shown",
            message: "Only one parent class is shown for each class (the first by IRI); the others are not.".into(),
            count: *n,
            examples: Vec::new(),
        });
    }
}

/// A code rather than a name: one to three letters then digits (`P07`, `C123`).
fn is_opaque(local: &str) -> bool {
    let letters = local.chars().take_while(char::is_ascii_alphabetic).count();
    let rest = &local[letters..];
    (1..=3).contains(&letters) && !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit())
}

/// `12,345`.
pub fn thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::{parse, RdfFormat};

    const PREFIXES: &str = r"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix xt: <https://ns.flur.ee/extract#> .
        @prefix ex: <https://ex.com/> .
        ";

    fn report(ttl: &str) -> Report {
        let parsed = parse(&format!("{PREFIXES}{ttl}"), RdfFormat::Turtle, None).unwrap();
        check(&parsed, &CheckOptions::default()).unwrap()
    }

    fn codes(findings: &[Finding]) -> Vec<&'static str> {
        findings.iter().map(|f| f.code).collect()
    }

    fn finding<'a>(findings: &'a [Finding], code: &str) -> &'a Finding {
        findings.iter().find(|f| f.code == code).unwrap()
    }

    /// An event class, its kind class, the kind property and two kinds.
    const COMMITMENTS: &str = r#"
        ex:Commitment a rdfs:Class, xt:EventClass ; rdfs:label "commitment" ;
          rdfs:comment "Something a person says they will do." .
        ex:CommitmentKind a rdfs:Class ; rdfs:label "commitment kind" ;
          rdfs:comment "What sort of thing was promised." .
        ex:kind a rdf:Property ; rdfs:label "kind" ; rdfs:comment "The kind of a commitment." .
        ex:Callback a ex:CommitmentKind ; skos:prefLabel "callback" .
        ex:SendQuote a ex:CommitmentKind ; skos:prefLabel "send a quote" .
        "#;

    #[test]
    fn an_event_class_is_shown_with_its_kinds() {
        let r = report(COMMITMENTS);
        assert!(r.passes(), "{:?}", r.will_fail);
        assert_eq!(
            (r.classes, r.event_classes, r.event_kinds, r.records),
            (2, 1, 2, 2)
        );
        assert!(
            !r.not_shown.contains_key("rdf:type xt:EventClass"),
            "{:?}",
            r.not_shown
        );
        assert!(
            !codes(&r.advice).iter().any(|c| c.starts_with("event-")),
            "{:?}",
            r.advice
        );
    }

    #[test]
    fn a_class_with_kinds_that_isnt_an_event_class_is_found() {
        let r = report(&COMMITMENTS.replace("a rdfs:Class, xt:EventClass", "a rdfs:Class"));
        assert_eq!(r.event_classes, 0);
        let f = finding(&r.advice, "event-class-not-marked");
        assert_eq!(f.examples, ["https://ex.com/Commitment"]);
    }

    #[test]
    fn kinds_the_reader_cant_find_are_found() {
        // a second "kind" property, and no records of the kind class
        let two_properties = COMMITMENTS.to_string()
            + r#"ex:sort a rdf:Property ; rdfs:label "Kind " ; rdfs:comment "Also a kind." ."#;
        let r = report(&two_properties);
        let f = finding(&r.advice, "event-kind-property");
        assert!(f.message.ends_with("has 2."), "{}", f.message);
        let no_records: String = COMMITMENTS
            .lines()
            .filter(|l| !l.contains("a ex:CommitmentKind"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            finding(&report(&no_records).advice, "event-kind-no-records").examples,
            ["https://ex.com/Commitment"]
        );
        let two_kind_classes = COMMITMENTS.to_string()
            + r#"ex:CommitmentType a rdfs:Class ; rdfs:label "Commitment kind" ."#;
        assert!(codes(&report(&two_kind_classes).advice).contains(&"event-kind-ambiguous"));
    }

    #[test]
    fn event_subtypes_and_undeclared_event_classes_are_found() {
        let r = report(
            r#"
            ex:Meeting a rdfs:Class, xt:EventClass ; rdfs:label "meeting" .
            ex:BoardMeeting a rdfs:Class ; rdfs:label "board meeting" ; rdfs:subClassOf ex:Meeting .
            ex:Offsite a rdfs:Class, xt:EventClass ; rdfs:label "offsite" ; rdfs:subClassOf ex:Meeting .
            ex:Launch a xt:EventClass ; rdfs:label "launch" .
            "#,
        );
        // Offsite is read as a kind of Meeting, not as an event class
        assert_eq!((r.event_classes, r.event_kinds), (1, 1));
        // the subtype is shown as a kind of its parent, and its own
        // statements are counted as a reader counts them
        assert_eq!(r.not_shown.get(policy::NOT_A_SHOWN_TERM), Some(&4));
        assert_eq!(
            finding(&r.advice, "event-subtype-not-marked").examples,
            ["https://ex.com/BoardMeeting"]
        );
        assert_eq!(
            finding(&r.advice, "event-class-undeclared").examples,
            ["https://ex.com/Launch"]
        );
    }

    /// Nested event subclasses are each a kind of their parent: one event
    /// class, read with two kinds.
    #[test]
    fn nested_event_subclasses_are_kinds_of_one_event_class() {
        let r = report(
            r#"
            ex:A a rdfs:Class, xt:EventClass ; rdfs:label "a" .
            ex:B a rdfs:Class, xt:EventClass ; rdfs:label "b" ; rdfs:subClassOf ex:A .
            ex:C a rdfs:Class, xt:EventClass ; rdfs:label "c" ; rdfs:subClassOf ex:B .
            "#,
        );
        assert_eq!((r.event_classes, r.event_kinds), (1, 2));
    }

    #[test]
    fn examples_and_extraction_results_are_refused() {
        let r = report(
            r#"
            ex:Person a rdfs:Class ; rdfs:label "person" ; xt:example "Ada Lovelace" .
            ex:ada a ex:Person ; skos:prefLabel "Ada" ; xt:score 0.9 .
            "#,
        );
        assert!(!r.passes());
        assert_eq!(
            codes(&r.will_fail),
            ["example-refused", "extraction-annotations"]
        );
        assert_eq!(
            finding(&r.will_fail, "example-refused").examples,
            ["https://ex.com/Person"]
        );
        assert_eq!(
            finding(&r.will_fail, "extraction-annotations").examples,
            ["https://ex.com/ada"]
        );
    }
}
