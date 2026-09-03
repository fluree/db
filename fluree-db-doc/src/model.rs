//! The ontology extraction is grounded on: what `--model` names.
//!
//! One inventory serves two consumers that must never disagree. The prompt
//! renders it as the MODEL block the language model is told to use, and the
//! gate judges what comes back against the same classes and properties. A
//! predicate the model invented is not written as an edge, however
//! plausible it reads; one it spelled differently is repaired to the IRI it
//! meant when that is unambiguous, and rejected otherwise.
//!
//! The inventory is filled from query rows, so a model can come from any
//! ledger — a file is loaded into a scratch ledger by the caller and queried
//! the same way.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

pub const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
pub const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const OWL: &str = "http://www.w3.org/2002/07/owl#";
pub const SCHEMA: &str = "https://schema.org/";
pub const SKOS: &str = "http://www.w3.org/2004/02/skos/core#";
pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
pub const SHACL: &str = "http://www.w3.org/ns/shacl#";

/// Prefixes the model text and the gate understand in both spellings, so
/// `schema:Person` and `https://schema.org/Person` name one class.
pub const PREFIXES: &[(&str, &str)] = &[
    ("rdfs", RDFS),
    ("rdf", RDF),
    ("owl", OWL),
    ("schema", SCHEMA),
    ("skos", SKOS),
    ("xsd", XSD),
    ("sh", SHACL),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassInfo {
    pub iri: String,
    pub label: Option<String>,
    pub comment: Option<String>,
    pub parents: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyKind {
    /// Declared `owl:ObjectProperty`, or ranging over a model class.
    Object,
    /// Declared `owl:DatatypeProperty`, or ranging over a datatype.
    Datatype,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyInfo {
    pub iri: String,
    pub label: Option<String>,
    pub comment: Option<String>,
    pub domains: Vec<String>,
    pub ranges: Vec<String>,
    pub kind: PropertyKind,
}

/// What the gate decided about a predicate the language model used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// A model property, as spelled.
    Valid,
    /// Not as spelled, but recoverable without a coin flip: `property` is
    /// what was meant, `note` says how that was decided.
    Repaired {
        property: String,
        note: &'static str,
    },
    /// Not in the model and not recoverable. Kept as evidence, never
    /// written as an edge.
    Rejected { reason: String },
}

impl Verdict {
    pub fn note(&self) -> &'static str {
        match self {
            Verdict::Valid => "valid",
            Verdict::Repaired { .. } => "repaired",
            Verdict::Rejected { .. } => "rejected",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Model {
    classes: Vec<ClassInfo>,
    properties: Vec<PropertyInfo>,
    class_set: HashSet<String>,
    property_index: HashMap<String, usize>,
    /// Lowercased label → property IRIs carrying it.
    label_to_properties: HashMap<String, Vec<String>>,
}

impl Model {
    pub fn new(classes: Vec<ClassInfo>, mut properties: Vec<PropertyInfo>) -> Self {
        let class_set: HashSet<String> = classes.iter().map(|c| c.iri.clone()).collect();
        for p in &mut properties {
            if p.kind == PropertyKind::Unknown {
                p.kind = infer_kind(&p.ranges, &class_set);
            }
        }
        let property_index = properties
            .iter()
            .enumerate()
            .map(|(i, p)| (p.iri.clone(), i))
            .collect();
        let mut label_to_properties: HashMap<String, Vec<String>> = HashMap::new();
        for p in &properties {
            if let Some(label) = &p.label {
                let key = label.trim().to_lowercase();
                if !key.is_empty() {
                    label_to_properties
                        .entry(key)
                        .or_default()
                        .push(p.iri.clone());
                }
            }
        }
        Self {
            classes,
            properties,
            class_set,
            property_index,
            label_to_properties,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.classes.is_empty() && self.properties.is_empty()
    }

    pub fn classes(&self) -> &[ClassInfo] {
        &self.classes
    }

    pub fn properties(&self) -> &[PropertyInfo] {
        &self.properties
    }

    /// Queries whose rows [`ModelBuilder::add_class_rows`] takes: anything
    /// typed `rdfs:Class` or `owl:Class`, and anything with a superclass.
    pub fn class_queries() -> Vec<Value> {
        let optionals = |s: &str| {
            json!([
                ["optional", { "@id": s, "rdfs:label": "?label" }],
                ["optional", { "@id": s, "rdfs:comment": "?comment" }],
                ["optional", { "@id": s, "rdfs:subClassOf": "?parent" }]
            ])
        };
        let mut queries = Vec::new();
        for anchor in [
            json!({ "@id": "?s", "@type": "rdfs:Class" }),
            json!({ "@id": "?s", "@type": "owl:Class" }),
            json!({ "@id": "?s", "rdfs:subClassOf": "?any" }),
        ] {
            let mut where_ = vec![anchor];
            where_.extend(optionals("?s").as_array().unwrap().iter().cloned());
            queries.push(json!({
                "@context": query_context(),
                "where": where_,
                "select": ["?s", "?label", "?comment", "?parent"]
            }));
        }
        queries
    }

    /// Queries whose rows [`ModelBuilder::add_property_rows`] takes, each
    /// with the kind its anchor implies: anything typed `rdf:Property` or an
    /// OWL property class, anything with a declared domain, and every
    /// `sh:path` of a property shape on a node shape with a target class,
    /// which is how a Fluree model usually declares its properties.
    pub fn property_queries() -> Vec<(Value, PropertyKind)> {
        let mut queries = vec![(
            json!({
                "@context": query_context(),
                "where": [
                    { "@id": "?shape", "sh:targetClass": "?domain", "sh:property": "?ps" },
                    { "@id": "?ps", "sh:path": "?s" },
                    ["optional", { "@id": "?ps", "sh:name": "?label" }],
                    ["optional", { "@id": "?ps", "sh:description": "?comment" }],
                    ["optional", { "@id": "?ps", "sh:class": "?range" }],
                    ["optional", { "@id": "?ps", "sh:datatype": "?range" }]
                ],
                "select": ["?s", "?label", "?comment", "?domain", "?range"]
            }),
            PropertyKind::Unknown,
        )];
        for (anchor, kind) in [
            (
                json!({ "@id": "?s", "@type": "rdf:Property" }),
                PropertyKind::Unknown,
            ),
            (
                json!({ "@id": "?s", "@type": "owl:ObjectProperty" }),
                PropertyKind::Object,
            ),
            (
                json!({ "@id": "?s", "@type": "owl:DatatypeProperty" }),
                PropertyKind::Datatype,
            ),
            (
                json!({ "@id": "?s", "schema:domainIncludes": "?any" }),
                PropertyKind::Unknown,
            ),
            (
                json!({ "@id": "?s", "rdfs:domain": "?any" }),
                PropertyKind::Unknown,
            ),
        ] {
            queries.push((
                json!({
                    "@context": query_context(),
                    "where": [
                        anchor,
                        ["optional", { "@id": "?s", "rdfs:label": "?label" }],
                        ["optional", { "@id": "?s", "rdfs:comment": "?comment" }],
                        ["optional", { "@id": "?s", "schema:domainIncludes": "?domain" }],
                        ["optional", { "@id": "?s", "rdfs:domain": "?domain" }],
                        ["optional", { "@id": "?s", "schema:rangeIncludes": "?range" }],
                        ["optional", { "@id": "?s", "rdfs:range": "?range" }]
                    ],
                    "select": ["?s", "?label", "?comment", "?domain", "?range"]
                }),
                kind,
            ));
        }
        queries
    }

    /// The MODEL block of the system prompt: the class tree, then the
    /// properties grouped by their first domain.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        if !self.classes.is_empty() {
            out.push_str("CLASSES (type hierarchy):\n");
            let mut children: HashMap<&str, Vec<&ClassInfo>> = HashMap::new();
            let mut roots: Vec<&ClassInfo> = Vec::new();
            for c in &self.classes {
                // A parent outside the model is a generic anchor (a class
                // that says `subClassOf schema:Thing` while `schema:Thing`
                // is absent): render as a root.
                match c.parents.iter().find(|p| self.class_set.contains(*p)) {
                    Some(p) => children.entry(p.as_str()).or_default().push(c),
                    None => roots.push(c),
                }
            }
            let mut stack: Vec<(&ClassInfo, usize)> =
                roots.iter().rev().map(|c| (*c, 0usize)).collect();
            let mut seen: HashSet<&str> = HashSet::new();
            while let Some((cls, depth)) = stack.pop() {
                if !seen.insert(&cls.iri) {
                    continue;
                }
                for _ in 0..depth {
                    out.push_str("  ");
                }
                out.push_str(&cls.iri);
                if let Some(label) = &cls.label {
                    out.push_str(" — ");
                    out.push_str(label);
                }
                if let Some(comment) = &cls.comment {
                    out.push_str(": ");
                    out.push_str(&one_line(comment));
                }
                out.push('\n');
                if let Some(kids) = children.get(cls.iri.as_str()) {
                    for child in kids.iter().rev() {
                        stack.push((*child, depth + 1));
                    }
                }
            }
        }
        if !self.properties.is_empty() {
            out.push_str("\nPROPERTIES:\n");
            let mut families: Vec<(&str, Vec<&PropertyInfo>)> = Vec::new();
            for p in &self.properties {
                let fam = p
                    .domains
                    .first()
                    .map(String::as_str)
                    .unwrap_or("(no domain)");
                match families.iter_mut().find(|(k, _)| *k == fam) {
                    Some((_, v)) => v.push(p),
                    None => families.push((fam, vec![p])),
                }
            }
            for (family, props) in families {
                out.push('\n');
                out.push_str(family);
                out.push_str(":\n");
                for p in props {
                    out.push_str("  ");
                    out.push_str(&p.iri);
                    if let Some(label) = &p.label {
                        out.push_str(" — ");
                        out.push_str(label);
                    }
                    out.push_str(" (");
                    out.push_str(&p.domains.join("|"));
                    out.push_str(" → ");
                    out.push_str(&p.ranges.join("|"));
                    out.push(')');
                    if let Some(c) = &p.comment {
                        out.push_str(" [");
                        out.push_str(&one_line(c));
                        out.push(']');
                    }
                    out.push('\n');
                }
            }
        }
        out
    }

    /// Changes when anything the prompt or the gate sees changes.
    pub fn fingerprint(&self) -> String {
        hex::encode(Sha256::digest(self.render_text().as_bytes()))
    }

    /// The model class a type the language model wrote names, in the
    /// model's own spelling. `None` when it named nothing in the model.
    pub fn class_iri(&self, given: &str) -> Option<String> {
        let given = given.trim();
        if self.class_set.contains(given) {
            return Some(given.to_string());
        }
        for alt in spellings(given) {
            if self.class_set.contains(&alt) {
                return Some(alt);
            }
        }
        unique_by_local_name(self.class_set.iter(), given)
    }

    fn resolve_property_iri(&self, predicate: &str) -> Option<String> {
        let predicate = predicate.trim();
        if self.property_index.contains_key(predicate) {
            return Some(predicate.to_string());
        }
        for alt in spellings(predicate) {
            if self.property_index.contains_key(&alt) {
                return Some(alt);
            }
        }
        unique_by_local_name(self.property_index.keys(), predicate)
    }

    fn property(&self, iri: &str) -> Option<&PropertyInfo> {
        self.property_index.get(iri).map(|i| &self.properties[*i])
    }

    fn is_object_property(&self, iri: &str) -> bool {
        self.property(iri)
            .is_some_and(|p| p.kind == PropertyKind::Object)
    }

    /// Judge a relation predicate. `subject_types` narrows the class repair
    /// by domain; empty means no domain filter.
    pub fn judge(&self, predicate: &str, subject_types: &[String]) -> Verdict {
        if let Some(iri) = self.resolve_property_iri(predicate) {
            if iri == predicate.trim() {
                return Verdict::Valid;
            }
            return Verdict::Repaired {
                property: iri,
                note: "compact-form",
            };
        }

        // The model said "depicts" or "has drawing" instead of the IRI.
        // Only an unambiguous label recovers.
        let label_key = predicate.trim().to_lowercase();
        if let Some(iris) = self.label_to_properties.get(&label_key) {
            if iris.len() == 1 {
                return Verdict::Repaired {
                    property: iris[0].clone(),
                    note: "label-match",
                };
            }
            return Verdict::Rejected {
                reason: format!(
                    "predicate label '{predicate}' matches {} model properties, ambiguous",
                    iris.len()
                ),
            };
        }

        // A class used as the predicate ("this Drawing —Part→ that part"):
        // recover the unique property whose range is that class, preferring
        // one whose domain admits the subject.
        if let Some(class) = self.class_iri(predicate) {
            let ranged: Vec<&PropertyInfo> = self
                .properties
                .iter()
                .filter(|p| p.ranges.iter().any(|r| r == &class))
                .collect();
            let domain_ok: Vec<&PropertyInfo> = ranged
                .iter()
                .copied()
                .filter(|p| {
                    subject_types.is_empty()
                        || p.domains.is_empty()
                        || p.domains.iter().any(|d| subject_types.contains(d))
                })
                .collect();
            let pick = if domain_ok.len() == 1 {
                Some(domain_ok[0].iri.clone())
            } else if domain_ok.is_empty() && ranged.len() == 1 {
                Some(ranged[0].iri.clone())
            } else {
                None
            };
            if let Some(property) = pick {
                return Verdict::Repaired {
                    property,
                    note: "class-to-property",
                };
            }
            return Verdict::Rejected {
                reason: format!(
                    "predicate '{predicate}' is a model class; {} propert{} range over it, no unique repair",
                    ranged.len(),
                    if ranged.len() == 1 { "y" } else { "ies" }
                ),
            };
        }

        Verdict::Rejected {
            reason: format!("predicate '{predicate}' is not a model property"),
        }
    }

    /// Judge an attribute: a property carrying a literal value. Same rule
    /// as relations, plus the property must not range over a model class.
    pub fn judge_attribute(&self, property: &str) -> Verdict {
        if let Some(iri) = self.resolve_property_iri(property) {
            if self.is_object_property(&iri) {
                return Verdict::Rejected {
                    reason: format!(
                        "'{property}' is an object property; its value is an entity, not a literal"
                    ),
                };
            }
            if iri == property.trim() {
                return Verdict::Valid;
            }
            return Verdict::Repaired {
                property: iri,
                note: "compact-form",
            };
        }
        let label_key = property.trim().to_lowercase();
        if let Some(iris) = self.label_to_properties.get(&label_key) {
            let literal: Vec<&String> = iris
                .iter()
                .filter(|iri| !self.is_object_property(iri))
                .collect();
            if literal.len() == 1 {
                return Verdict::Repaired {
                    property: literal[0].clone(),
                    note: "label-match",
                };
            }
            return Verdict::Rejected {
                reason: format!(
                    "attribute label '{property}' matches {} literal model properties, ambiguous",
                    literal.len()
                ),
            };
        }
        Verdict::Rejected {
            reason: format!("attribute '{property}' is not a model property"),
        }
    }
}

/// Accumulates query rows from any number of sources into one [`Model`],
/// merging the repeats that optional patterns produce.
#[derive(Debug, Default)]
pub struct ModelBuilder {
    classes: Vec<ClassInfo>,
    class_at: HashMap<String, usize>,
    properties: Vec<PropertyInfo>,
    prop_at: HashMap<String, usize>,
}

impl ModelBuilder {
    /// Rows are `[iri, label, comment, parent]`.
    pub fn add_class_rows(&mut self, rows: &[Value]) {
        for row in rows {
            let Some(iri) = cell(row, 0) else { continue };
            if iri.starts_with("_:") {
                continue;
            }
            let idx = *self.class_at.entry(iri.to_string()).or_insert_with(|| {
                self.classes.push(ClassInfo {
                    iri: iri.to_string(),
                    label: None,
                    comment: None,
                    parents: Vec::new(),
                });
                self.classes.len() - 1
            });
            let c = &mut self.classes[idx];
            fill(&mut c.label, cell(row, 1));
            fill(&mut c.comment, cell(row, 2));
            push_unique(&mut c.parents, cell(row, 3));
        }
    }

    /// Rows are `[iri, label, comment, domain, range]`; `kind` is what the
    /// query's anchor established, `Unknown` when it established nothing.
    pub fn add_property_rows(&mut self, rows: &[Value], kind: PropertyKind) {
        for row in rows {
            let Some(iri) = cell(row, 0) else { continue };
            if iri.starts_with("_:") {
                continue;
            }
            let idx = *self.prop_at.entry(iri.to_string()).or_insert_with(|| {
                self.properties.push(PropertyInfo {
                    iri: iri.to_string(),
                    label: None,
                    comment: None,
                    domains: Vec::new(),
                    ranges: Vec::new(),
                    kind: PropertyKind::Unknown,
                });
                self.properties.len() - 1
            });
            let p = &mut self.properties[idx];
            fill(&mut p.label, cell(row, 1));
            fill(&mut p.comment, cell(row, 2));
            push_unique(&mut p.domains, cell(row, 3));
            push_unique(&mut p.ranges, cell(row, 4));
            if p.kind == PropertyKind::Unknown {
                p.kind = kind;
            }
        }
    }

    pub fn build(self) -> Model {
        Model::new(self.classes, self.properties)
    }
}

pub fn query_context() -> Value {
    let mut ctx = serde_json::Map::new();
    for (prefix, iri) in PREFIXES {
        ctx.insert((*prefix).to_string(), json!(iri));
    }
    ctx.insert("doc".to_string(), json!(crate::vocab::DOC_NS));
    Value::Object(ctx)
}

fn cell(row: &Value, i: usize) -> Option<&str> {
    match row.get(i)? {
        Value::String(s) => Some(s.as_str()).filter(|s| !s.is_empty()),
        Value::Object(o) => o
            .get("@id")
            .or_else(|| o.get("@value"))
            .and_then(Value::as_str),
        _ => None,
    }
}

fn fill(slot: &mut Option<String>, value: Option<&str>) {
    if slot.is_none() {
        *slot = value.map(str::to_string);
    }
}

fn push_unique(list: &mut Vec<String>, value: Option<&str>) {
    if let Some(v) = value {
        if !list.iter().any(|x| x == v) {
            list.push(v.to_string());
        }
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn infer_kind(ranges: &[String], classes: &HashSet<String>) -> PropertyKind {
    if ranges.iter().any(|r| classes.contains(r)) {
        return PropertyKind::Object;
    }
    if ranges.iter().any(|r| is_datatype(r)) {
        return PropertyKind::Datatype;
    }
    PropertyKind::Unknown
}

fn is_datatype(iri: &str) -> bool {
    iri.starts_with("xsd:")
        || iri.starts_with(XSD)
        || matches!(
            iri,
            "rdfs:Literal"
                | "schema:Text"
                | "schema:Number"
                | "schema:Integer"
                | "schema:Float"
                | "schema:Date"
                | "schema:DateTime"
                | "schema:Time"
                | "schema:Boolean"
                | "schema:URL"
        )
        || iri == format!("{RDFS}Literal")
        || [
            "Text", "Number", "Integer", "Float", "Date", "DateTime", "Time", "Boolean", "URL",
        ]
        .iter()
        .any(|t| iri == format!("{SCHEMA}{t}"))
}

/// The other ways of writing an IRI under a known prefix: the compact form
/// of a full IRI, and the full form of a compact one.
fn spellings(iri: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (prefix, ns) in PREFIXES {
        if let Some(rest) = iri.strip_prefix(&format!("{prefix}:")) {
            out.push(format!("{ns}{rest}"));
        }
        if let Some(rest) = iri.strip_prefix(ns) {
            out.push(format!("{prefix}:{rest}"));
        }
    }
    out
}

/// The part of an IRI or CURIE after its last `#`, `/` or `:`.
fn local_name(iri: &str) -> &str {
    iri.trim().rsplit(['#', '/', ':']).next().unwrap_or(iri)
}

/// The one IRI among `candidates` whose local name is `given`'s, exactly
/// or ignoring case. Two candidates is a coin flip, so `None`.
fn unique_by_local_name<'a>(
    candidates: impl Iterator<Item = &'a String>,
    given: &str,
) -> Option<String> {
    let tail = local_name(given);
    if tail.is_empty() || tail.chars().any(char::is_whitespace) {
        return None;
    }
    let folded = tail.to_lowercase();
    let mut exact = Vec::new();
    let mut loose = Vec::new();
    for iri in candidates {
        let name = local_name(iri);
        if name == tail {
            exact.push(iri.clone());
        } else if name.to_lowercase() == folded {
            loose.push(iri.clone());
        }
    }
    if exact.len() == 1 {
        return exact.pop();
    }
    if exact.is_empty() && loose.len() == 1 {
        return loose.pop();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A miniature engineering model: Drawing/Part/ChangeOrder classes,
    /// `depicts` (Drawing→Part), `hasDrawing` (Part→Drawing),
    /// `hasChangeOrder` (Drawing→ChangeOrder) and a datatype property.
    fn model() -> Model {
        let ns = "https://ns.flur.ee/magna#";
        let class_rows = vec![
            json!([format!("{ns}Drawing"), "Drawing", null, null]),
            json!([format!("{ns}Part"), "Part", "A manufactured part", null]),
            json!([
                format!("{ns}ChangeOrder"),
                "Change Order",
                null,
                format!("{ns}Drawing")
            ]),
        ];
        let property_rows = vec![
            json!([
                format!("{ns}depicts"),
                "depicts",
                null,
                format!("{ns}Drawing"),
                format!("{ns}Part")
            ]),
            json!([
                format!("{ns}hasDrawing"),
                "has drawing",
                null,
                format!("{ns}Part"),
                format!("{ns}Drawing")
            ]),
            json!([
                format!("{ns}hasChangeOrder"),
                "has change order",
                null,
                format!("{ns}Drawing"),
                format!("{ns}ChangeOrder")
            ]),
            json!([
                format!("{ns}materialMass"),
                "material mass",
                null,
                format!("{ns}Drawing"),
                "xsd:decimal"
            ]),
        ];
        let mut b = ModelBuilder::default();
        b.add_class_rows(&class_rows);
        b.add_property_rows(&property_rows, PropertyKind::Unknown);
        b.build()
    }

    fn classes_only(rows: &[Value]) -> Model {
        let mut b = ModelBuilder::default();
        b.add_class_rows(rows);
        b.build()
    }

    #[test]
    fn rows_merge_and_kinds_infer() {
        let m = model();
        assert_eq!(m.classes().len(), 3);
        assert_eq!(m.properties().len(), 4);
        assert_eq!(m.properties()[0].kind, PropertyKind::Object);
        assert_eq!(m.properties()[3].kind, PropertyKind::Datatype);
        assert_eq!(
            m.classes()[2].parents,
            vec!["https://ns.flur.ee/magna#Drawing"]
        );
    }

    #[test]
    fn render_nests_subclasses_and_groups_properties() {
        let text = model().render_text();
        assert!(text.contains("CLASSES (type hierarchy):\nhttps://ns.flur.ee/magna#Drawing — Drawing\n  https://ns.flur.ee/magna#ChangeOrder — Change Order\n"));
        assert!(text.contains("https://ns.flur.ee/magna#Part — Part: A manufactured part"));
        assert!(text.contains("\nhttps://ns.flur.ee/magna#Drawing:\n  https://ns.flur.ee/magna#depicts — depicts (https://ns.flur.ee/magna#Drawing → https://ns.flur.ee/magna#Part)"));
    }

    #[test]
    fn exact_property_is_valid() {
        assert_eq!(
            model().judge("https://ns.flur.ee/magna#depicts", &[]),
            Verdict::Valid
        );
    }

    #[test]
    fn class_predicate_repairs_to_unique_ranged_property() {
        let v = model().judge(
            "https://ns.flur.ee/magna#Part",
            &["https://ns.flur.ee/magna#Drawing".to_string()],
        );
        assert_eq!(
            v,
            Verdict::Repaired {
                property: "https://ns.flur.ee/magna#depicts".into(),
                note: "class-to-property",
            }
        );
    }

    #[test]
    fn label_and_local_name_repair() {
        assert_eq!(
            model().judge("Has Change Order", &[]),
            Verdict::Repaired {
                property: "https://ns.flur.ee/magna#hasChangeOrder".into(),
                note: "label-match",
            }
        );
        assert_eq!(
            model().judge("mg:depicts", &[]),
            Verdict::Repaired {
                property: "https://ns.flur.ee/magna#depicts".into(),
                note: "compact-form",
            }
        );
        assert_eq!(
            model().judge_attribute("materialmass"),
            Verdict::Repaired {
                property: "https://ns.flur.ee/magna#materialMass".into(),
                note: "compact-form",
            }
        );
    }

    #[test]
    fn free_text_and_object_attributes_are_rejected() {
        assert!(matches!(
            model().judge("part number", &[]),
            Verdict::Rejected { .. }
        ));
        assert!(matches!(
            model().judge_attribute("https://ns.flur.ee/magna#depicts"),
            Verdict::Rejected { .. }
        ));
    }

    #[test]
    fn schema_prefix_spellings_are_one_class() {
        let m = classes_only(&[json!(["https://schema.org/Person", "Person", null, null])]);
        assert_eq!(
            m.class_iri("schema:Person").as_deref(),
            Some("https://schema.org/Person")
        );
        let m = classes_only(&[json!(["schema:Person", "Person", null, null])]);
        assert_eq!(
            m.class_iri("https://schema.org/Person").as_deref(),
            Some("schema:Person")
        );
        assert_eq!(m.class_iri("person").as_deref(), Some("schema:Person"));
        assert_eq!(m.class_iri("schema:Thing"), None);
    }

    #[test]
    fn shacl_shapes_declare_properties() {
        let q = Model::property_queries();
        assert_eq!(q[0].0["where"][0]["sh:targetClass"], "?domain");
        assert_eq!(q[0].0["where"][1]["sh:path"], "?s");
        // Rows as that query returns them: the domain is the shape's target
        // class, the range is sh:class or sh:datatype.
        let mut b = ModelBuilder::default();
        b.add_class_rows(&[
            json!(["mg:Drawing", "Drawing", null, null]),
            json!(["mg:Part", "Part", null, null]),
        ]);
        b.add_property_rows(
            &[
                json!(["mg:depicts", "depicts", null, "mg:Drawing", "mg:Part"]),
                json!([
                    "mg:drawingNumber",
                    "Drawing number",
                    "The title-block number",
                    "mg:Drawing",
                    "xsd:string"
                ]),
            ],
            PropertyKind::Unknown,
        );
        let m = b.build();
        assert_eq!(m.properties()[0].kind, PropertyKind::Object);
        assert_eq!(m.properties()[1].kind, PropertyKind::Datatype);
        assert_eq!(
            m.judge("depicts", &[]),
            Verdict::Repaired {
                property: "mg:depicts".into(),
                note: "compact-form"
            }
        );
    }

    #[test]
    fn fingerprint_tracks_content() {
        let a = model().fingerprint();
        let b = classes_only(&[json!(["x", null, null, null])]).fingerprint();
        assert_ne!(a, b);
        assert_eq!(a, model().fingerprint());
    }
}
