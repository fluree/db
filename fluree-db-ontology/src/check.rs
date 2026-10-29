//! What an extraction model will and won't read of an ontology and catalog.
//!
//! A [`Report`] has four parts:
//!
//! - **will fail**: what a request carrying this ontology would be refused
//!   for (no class, a term that can't be given a label, a catalog record
//!   without a label or an IRI, over a request limit);
//! - **adapted**: what the model reads another way, by the input policy
//!   ([`crate::policy`]);
//! - **not shown**: what the model never sees, by predicate;
//! - **advice**: what the evidence says improves a reading (descriptions,
//!   labels in words rather than column names, a smaller vocabulary).
//!
//! `adapted` and `not_shown` use the keys an extraction response reports
//! them under, so they compare line for line with a real request's.

use crate::estimate::{self, Estimate, Sizes};
use crate::policy::{self, Adapted, Shown};
use crate::read::{Parsed, Quad, Split};
use crate::slice::{self, Slice, UnknownSeed};
use crate::vocab::*;
use crate::words::{self, local_name};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

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

    let records = records(catalog);
    catalog_findings(&records, &shown, &mut will_fail, &mut advice);
    ontology_advice(&shown, &not_shown, &mut advice);

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
            "Catalog records are typed with classes the ontology doesn't declare; the extraction model won't see those types.",
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
                "The extraction model reads the first {} characters of a record's skos:definition; these are longer, and are cut.",
                estimate::DEFINITION_CHARS
            ),
            &long,
        ));
    }
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
                "{} of {} classes and properties have no description (rdfs:comment or skos:definition). After labels, descriptions help the extraction model most: say what counts and what doesn't, and name the words people use for it.",
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
            "The extraction model reads an IRI's local name beside its label, and a local name like \"employer_id\" reads as a key column even when the label says \"employer\". Mint these IRIs from their labels.",
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
            "These terms have codes for local names and no description, so their labels are all the extraction model has to go on. Add descriptions.",
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
            message: "The extraction model doesn't see rdfs:domain or rdfs:range (nor schema.org's domainIncludes and rangeIncludes). Say in each property's description what it connects (\"the organization that employs the person\"), or restate it as a SHACL property shape on the class.".into(),
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
