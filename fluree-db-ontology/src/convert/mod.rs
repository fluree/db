//! Ontologies from what people already have: table and column names, a CSV
//! header, a workbook, SQL DDL, a warehouse's semantic layer (a Snowflake
//! semantic view, a Databricks Genie space, a dbt project's semantic models
//! and model properties, an Apache Ossie semantic model). An Ossie ontology,
//! already conceptual, is written straight to RDF ([`ossie::ontology`]).
//!
//! Every source is read into one [`Model`] of tables and columns, and one
//! emitter turns that into an ontology, so the rules are the same whatever
//! the source. They come from measuring what the extraction model reads:
//!
//! - **labels in words, without `_id`**: a table is a class labelled with its
//!   name in words, singular (`DAMAGE_REPORTS` → "damage report"); a column a
//!   property labelled with its name in words. Labels like `employer_id` find
//!   a fraction of the relations "employer" does;
//! - **IRIs minted from the labels** (`DamageReport`, `employer`), since the
//!   model reads the local name beside the label;
//! - **descriptions copied, with synonyms appended** ("Also called: ...");
//! - **foreign keys become relations** from the table's class to the class
//!   of the table referenced, labelled without the key suffix;
//! - **enumerated columns become a class with catalog records**, one per
//!   value, the shape the model reads best for a closed set of kinds;
//! - primary keys, metrics, filters and SQL are left out: they describe
//!   tables, not things.

pub mod dbt;
pub mod ossie;
pub mod semantic;
pub mod sql;
pub mod tabular;

use crate::read::Quad;
use crate::vocab::*;
use crate::words::name_words;
use fluree_graph_ir::{Datatype, Term};
use std::collections::{BTreeMap, HashMap};

/// A YAML document read into typed fields. Fields typed `String` keep a
/// scalar as written (`NO`, `007`, `.inf`), where an untyped value would
/// apply YAML's implicit typing. Duplicate keys keep the last value, as
/// PyYAML (and so dbt) does.
pub fn from_yaml<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, String> {
    let mut options = serde_saphyr::Options::default();
    options.duplicate_keys = serde_saphyr::DuplicateKeyPolicy::LastWins;
    serde_saphyr::from_str_with_options(text, options).map_err(|e| e.to_string())
}

/// Whether a JSON document is JSON-LD (it has `@context`, `@graph`, `@id` or
/// `@type` at its top), which no converter reads, whatever else it holds.
pub fn is_jsonld(doc: &serde_json::Value) -> bool {
    let has = |v: &serde_json::Value| {
        v.as_object().is_some_and(|m| {
            ["@context", "@graph", "@id", "@type"]
                .iter()
                .any(|k| m.contains_key(*k))
        })
    };
    match doc {
        serde_json::Value::Array(items) => items.iter().any(has),
        other => has(other),
    }
}

/// A YAML or JSON document as untyped values, for telling sources apart by
/// their keys (converters read their fields typed: `from_yaml`).
pub fn parse_document(text: &str) -> Result<serde_json::Value, String> {
    if crate::read::RdfFormat::sniff(text) == crate::read::RdfFormat::JsonLd {
        serde_json::from_str(text).map_err(|e| format!("invalid JSON: {e}"))
    } else {
        from_yaml(text).map_err(|e| format!("invalid YAML: {e}"))
    }
}

/// A table, from any source.
#[derive(Clone, Debug, Default)]
pub struct Table {
    pub name: String,
    pub description: Option<String>,
    pub synonyms: Vec<String>,
    pub columns: Vec<Column>,
    /// Primary key columns, left out of the ontology.
    pub primary_key: Vec<String>,
}

/// A column, from any source.
#[derive(Clone, Debug, Default)]
pub struct Column {
    pub name: String,
    /// An XSD datatype's local name (`string`, `integer`, `date`, ...).
    pub datatype: Option<&'static str>,
    pub description: Option<String>,
    pub synonyms: Vec<String>,
    /// The values of an enumerated column (a `CHECK ... IN`, an enum type,
    /// or a text column with few distinct values).
    pub values: Vec<String>,
    /// The table this column references, as a foreign key.
    pub references: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Model {
    pub tables: Vec<Table>,
    /// What the reader left out or couldn't place, for the summary.
    pub notes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct EmitOptions {
    /// Namespace for minted IRIs.
    pub base: String,
}

/// What the emitter made.
#[derive(Clone, Debug, Default)]
pub struct Converted {
    pub ontology: Vec<Quad>,
    /// Records for enumerated columns' values.
    pub catalog: Vec<Quad>,
    pub classes: usize,
    pub properties: usize,
    /// Classes made for enumerated columns, and their records.
    pub enum_classes: usize,
    pub records: usize,
    /// Primary key columns left out.
    pub keys_left_out: usize,
    /// What the emitter left out, kept apart or qualified, for the summary.
    pub notes: Vec<String>,
}

/// A name in words, singular: a table's class label.
pub fn class_label(name: &str) -> String {
    let words = name_words(name).unwrap_or_else(|| name.to_lowercase());
    let mut parts: Vec<String> = words.split(' ').map(str::to_string).collect();
    if let Some(last) = parts.last_mut() {
        *last = singular(last);
    }
    parts.join(" ")
}

/// A column's property label: its name in words, without a key suffix
/// (`employer_id` and `CustomerID` → "employer", "customer"). `None` when
/// nothing is left.
pub fn property_label(name: &str, foreign_key: bool) -> Option<String> {
    let words = name_words(name)?;
    if !foreign_key {
        return Some(words);
    }
    // a key named for a code or number keeps its words (`origin_airport_code`
    // is "origin airport"); the target's label stands in only when nothing
    // is left
    let trimmed = ["id", "key", "fk", "code", "ref", "no", "num", "number"]
        .iter()
        .find_map(|suffix| strip_word_suffix(&words, suffix))
        .unwrap_or(&words);
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// `words` without a last word equal to `suffix`, ignoring case
/// ("customer ID" without "id" is "customer").
pub fn strip_word_suffix<'a>(words: &'a str, suffix: &str) -> Option<&'a str> {
    let (stem, last) = words.rsplit_once(' ')?;
    last.eq_ignore_ascii_case(suffix).then_some(stem)
}

/// An English plural made singular, by the common rules only:
/// `companies` → `company`, `addresses` → `address`, `orders` → `order`.
pub fn singular(word: &str) -> String {
    let lower = word.to_lowercase();
    if let Some((_, single)) = [
        ("people", "person"),
        ("children", "child"),
        ("men", "man"),
        ("women", "woman"),
    ]
    .iter()
    .find(|(plural, _)| lower == *plural)
    {
        return (*single).to_string();
    }
    let keep = [
        "status", "series", "species", "news", "analysis", "basis", "business", "class", "address",
        "process", "access",
    ];
    if word.len() <= 3 || keep.iter().any(|k| lower.ends_with(k)) {
        return word.to_string();
    }
    for (plural, single) in [
        ("ies", "y"),
        ("sses", "ss"),
        ("xes", "x"),
        ("ches", "ch"),
        ("shes", "sh"),
    ] {
        if let Some(stem) = word.strip_suffix(plural) {
            return format!("{stem}{single}");
        }
    }
    if let Some(stem) = word.strip_suffix('s') {
        if !stem.ends_with('s') && !stem.ends_with('u') && !stem.ends_with('i') {
            return stem.to_string();
        }
    }
    word.to_string()
}

/// `damage report` → `DamageReport`.
pub fn pascal(label: &str) -> String {
    label
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut c = w.chars();
            c.next()
                .map(|f| f.to_uppercase().chain(c).collect::<String>())
                .unwrap_or_default()
        })
        .collect()
}

/// `works for` → `worksFor`.
pub fn camel(label: &str) -> String {
    let p = pascal(label);
    let mut c = p.chars();
    match c.next() {
        Some(f) => f.to_lowercase().chain(c).collect(),
        None => p,
    }
}

/// A value as an IRI path segment: `Net 30 days` → `net-30-days`.
pub fn slug(value: &str) -> String {
    let mut out = String::new();
    for c in value.chars().flat_map(char::to_lowercase) {
        if c.is_alphanumeric() {
            out.push(c);
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        "value".to_string()
    } else {
        out
    }
}

/// A description with its synonyms appended.
pub fn comment(description: Option<&str>, synonyms: &[String]) -> Option<String> {
    let text = description
        .map(|d| d.split_whitespace().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();
    let synonyms: Vec<&str> = synonyms
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let mut text = text;
    if !synonyms.is_empty() {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(&format!("Also called: {}.", synonyms.join(", ")));
    }
    (!text.is_empty()).then_some(text)
}

/// "a" or "an", by the word's first letter.
fn article(word: &str) -> &'static str {
    if word.starts_with(['a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U']) {
        "an"
    } else {
        "a"
    }
}

fn capitalized(word: &str) -> String {
    let mut c = word.chars();
    c.next()
        .map(|f| f.to_uppercase().chain(c).collect())
        .unwrap_or_default()
}

/// A property's label, comment, domains and range. A property is shared only
/// by columns with the same name (and range) in different tables.
struct PropertyParts {
    label: String,
    comment: Option<String>,
    domains: Vec<String>,
    /// The domains' class labels, for a description written for them all.
    domain_labels: Vec<String>,
    range: Option<String>,
    /// For a relation with no description of its own: the class it points
    /// at, so one can be written once every table using it is known.
    target_label: Option<String>,
}

impl PropertyParts {
    /// The description: the source's, else one written for a relation.
    fn description(&self) -> Option<String> {
        if self.comment.is_some() {
            return self.comment.clone();
        }
        let target = self.target_label.as_deref()?;
        let tables = match self.domain_labels.as_slice() {
            [] => return None,
            [one] => one.clone(),
            [rest @ .., last] => format!("{} or {last}", rest.join(", ")),
        };
        let (of, to) = (article(&tables), article(target));
        Some(if self.label == target {
            format!("The {target} of {of} {tables}.")
        } else if self.domain_labels.len() == 1 {
            format!(
                "{} {tables}'s {}: {to} {target}.",
                capitalized(of),
                self.label
            )
        } else {
            format!("The {} of {of} {tables}: {to} {target}.", self.label)
        })
    }
}

struct Emitter<'a> {
    base: &'a str,
    out: Converted,
    /// IRIs already minted, by local name, so two terms never share one.
    used: HashMap<String, String>,
    /// Properties by IRI, merged across tables that share a column name.
    properties: BTreeMap<String, PropertyParts>,
    /// Which property key each property IRI was minted for.
    property_iri: HashMap<String, String>,
    /// Labels already given, by the key of the term that has each, so two
    /// different terms never read the same.
    class_labels: HashMap<String, String>,
    property_labels: HashMap<String, String>,
    qualified: usize,
}

impl Emitter<'_> {
    /// A fresh IRI for `local`, suffixed if another term (`key`) has it.
    fn mint(&mut self, local: &str, key: &str) -> String {
        let mut candidate = local.to_string();
        let mut n = 2;
        while let Some(owner) = self.used.get(&candidate) {
            if owner == key {
                break;
            }
            candidate = format!("{local}{n}");
            n += 1;
        }
        self.used.insert(candidate.clone(), key.to_string());
        format!("{}{candidate}", self.base)
    }

    fn triple(&mut self, s: &str, p: &str, o: Term) {
        self.out.ontology.push(Quad::new(Term::iri(s), p, o));
    }

    fn class(&mut self, iri: &str, label: &str, comment: Option<String>) {
        self.triple(iri, RDF_TYPE, Term::iri(RDFS_CLASS));
        self.triple(iri, RDFS_LABEL, Term::string(label));
        if let Some(c) = comment {
            self.triple(iri, RDFS_COMMENT, Term::string(c));
        }
        self.out.classes += 1;
    }

    /// The first of `candidates` no other term has as a label (or the last,
    /// if all are taken), recorded as `key`'s.
    fn label(&mut self, classes: bool, key: &str, candidates: &[String]) -> String {
        let taken = if classes {
            &self.class_labels
        } else {
            &self.property_labels
        };
        let chosen = candidates
            .iter()
            .find(|c| taken.get(*c).is_none_or(|k| k == key))
            .or(candidates.last())
            .cloned()
            .unwrap_or_default();
        if candidates.first() != Some(&chosen) {
            self.qualified += 1;
        }
        let taken = if classes {
            &mut self.class_labels
        } else {
            &mut self.property_labels
        };
        taken
            .entry(chosen.clone())
            .or_insert_with(|| key.to_string());
        chosen
    }

    /// Add `domain` to the property `key` names, creating it with the first
    /// label in `labels` no other property has.
    fn property(
        &mut self,
        key: &str,
        labels: &[String],
        comment: Option<String>,
        (domain, domain_label): (&str, &str),
        range: Option<String>,
        target_label: Option<&str>,
    ) {
        let iri = match self.property_iri.get(key) {
            Some(iri) => iri.clone(),
            None => {
                let label = self.label(false, key, labels);
                let iri = self.mint(&camel(&label), key);
                self.property_iri.insert(key.to_string(), iri.clone());
                self.properties.insert(
                    iri.clone(),
                    PropertyParts {
                        label,
                        comment: None,
                        domains: Vec::new(),
                        domain_labels: Vec::new(),
                        range,
                        target_label: target_label.map(str::to_string),
                    },
                );
                iri
            }
        };
        let entry = self.properties.get_mut(&iri).expect("just inserted");
        if entry.comment.is_none() {
            entry.comment = comment;
        }
        if !entry.domains.iter().any(|d| d == domain) {
            entry.domains.push(domain.to_string());
            entry.domain_labels.push(domain_label.to_string());
        }
    }
}

/// The ontology (and enumerated values' records) for a model. Two source
/// names never become one term: a clashing IRI is suffixed, a clashing label
/// is qualified (the column's full words, or its table's name in front), and
/// everything left out is counted in `notes`.
pub fn emit(model: &Model, options: &EmitOptions) -> Converted {
    let mut e = Emitter {
        base: &options.base,
        out: Converted::default(),
        used: HashMap::new(),
        properties: BTreeMap::new(),
        property_iri: HashMap::new(),
        class_labels: HashMap::new(),
        property_labels: HashMap::new(),
        qualified: 0,
    };
    e.out.notes.extend(model.notes.iter().cloned());
    // classes first, so a foreign key can name the class it points at
    let mut class_of: HashMap<String, (String, String)> = HashMap::new();
    for table in &model.tables {
        let key = format!("table:{}", table.name.to_lowercase());
        if class_of.contains_key(&table.name.to_lowercase()) {
            e.out.notes.push(format!(
                "two tables are named {:?}; the second was left out",
                table.name
            ));
            continue;
        }
        let label = class_label(&table.name);
        let label = e.label(true, &key, &[label.clone(), table.name.to_lowercase()]);
        let iri = e.mint(&pascal(&label), &key);
        e.class(
            &iri,
            &label,
            comment(table.description.as_deref(), &table.synonyms),
        );
        class_of.insert(table.name.to_lowercase(), (iri, label));
    }
    let mut no_label = Vec::new();
    let mut unresolved = 0;
    let mut seen_tables = std::collections::HashSet::new();
    for table in &model.tables {
        if !seen_tables.insert(table.name.to_lowercase()) {
            continue;
        }
        let (domain, table_label) = class_of[&table.name.to_lowercase()].clone();
        for column in &table.columns {
            if table
                .primary_key
                .iter()
                .any(|k| k.eq_ignore_ascii_case(&column.name))
            {
                e.out.keys_left_out += 1;
                continue;
            }
            let column_key = column.name.to_lowercase();
            let words = property_label(&column.name, false);
            let target = column
                .references
                .as_ref()
                .and_then(|t| class_of.get(&t.to_lowercase()).cloned());
            if let Some((range, target_label)) = target {
                let label =
                    property_label(&column.name, true).unwrap_or_else(|| target_label.clone());
                // with no description of its own, one is written once every
                // table using the relation is known
                let text = comment(column.description.as_deref(), &column.synonyms);
                let mut labels = vec![label.clone()];
                labels.extend(words.clone().filter(|w| *w != label));
                labels.push(format!("{table_label} {label}"));
                e.property(
                    &format!("fk:{column_key}:{range}"),
                    &labels,
                    text,
                    (&domain, &table_label),
                    Some(range),
                    Some(&target_label),
                );
            } else if !column.values.is_empty() {
                let label = words.clone().unwrap_or_else(|| column.name.clone());
                let key = format!("enum:{}.{}", table.name, column.name);
                let class_label = e.label(
                    true,
                    &key,
                    &[label.clone(), format!("{table_label} {label}")],
                );
                let class_iri = e.mint(&pascal(&class_label), &key);
                let text = comment(column.description.as_deref(), &column.synonyms);
                e.class(&class_iri, &class_label, text.clone());
                e.out.enum_classes += 1;
                // one record per distinct value; values that slug alike get
                // their own IRIs
                let mut slugs: HashMap<String, String> = HashMap::new();
                let mut done = std::collections::HashSet::new();
                for value in &column.values {
                    if !done.insert(value.clone()) {
                        continue;
                    }
                    let base_slug = slug(value);
                    let mut s = base_slug.clone();
                    let mut n = 2;
                    while slugs.get(&s).is_some_and(|v| v != value) {
                        s = format!("{base_slug}-{n}");
                        n += 1;
                    }
                    slugs.insert(s.clone(), value.clone());
                    let record = format!("{class_iri}/{s}");
                    e.out.catalog.push(Quad::new(
                        Term::iri(&record),
                        RDF_TYPE,
                        Term::iri(&class_iri),
                    ));
                    e.out.catalog.push(Quad::new(
                        Term::iri(&record),
                        SKOS_PREF_LABEL,
                        Term::string(value),
                    ));
                    e.out.records += 1;
                }
                e.property(
                    &format!("enum-property:{key}"),
                    &[label.clone(), format!("{table_label} {label}")],
                    text,
                    (&domain, &table_label),
                    Some(class_iri),
                    None,
                );
            } else {
                let fk = column.references.is_some();
                let Some(label) = property_label(&column.name, fk) else {
                    no_label.push(format!("{}.{}", table.name, column.name));
                    continue;
                };
                if fk {
                    unresolved += 1;
                }
                let text = comment(column.description.as_deref(), &column.synonyms);
                // a key to a table the input doesn't have: a relation with
                // no class to range over, not an integer-valued attribute
                let range = column
                    .datatype
                    .filter(|_| !fk)
                    .map(|dt| format!("{XSD}{dt}"));
                let mut labels = vec![label.clone()];
                labels.extend(words.clone().filter(|w| *w != label));
                labels.push(format!("{table_label} {label}"));
                let key = format!("value:{column_key}:{}", range.as_deref().unwrap_or("-"));
                e.property(&key, &labels, text, (&domain, &table_label), range, None);
            }
        }
    }
    if !no_label.is_empty() {
        e.out.notes.push(format!(
            "{} column(s) left out: no letters to make a label from ({})",
            no_label.len(),
            no_label.join(", ")
        ));
    }
    if unresolved > 0 {
        e.out.notes.push(format!(
            "{unresolved} foreign key(s) reference tables the input doesn't have, so their relations have no range"
        ));
    }
    if e.qualified > 0 {
        e.out.notes.push(format!(
            "{} term(s) labelled with more words (the column's full name, or the table's in front) because another term had the label",
            e.qualified
        ));
    }
    let properties = std::mem::take(&mut e.properties);
    for (iri, parts) in properties {
        let text = parts.description();
        let PropertyParts {
            label,
            domains,
            range,
            ..
        } = parts;
        e.triple(&iri, RDF_TYPE, Term::iri(RDF_PROPERTY));
        e.triple(&iri, RDFS_LABEL, Term::string(&label));
        if let Some(c) = text {
            e.triple(&iri, RDFS_COMMENT, Term::string(c));
        }
        for d in domains {
            e.triple(&iri, RDFS_DOMAIN, Term::iri(d));
        }
        if let Some(r) = range {
            e.triple(&iri, RDFS_RANGE, Term::iri(r));
        }
        e.out.properties += 1;
    }
    e.out
}

/// A literal typed with an XSD datatype's local name.
pub fn typed(value: &str, datatype: &str) -> Term {
    Term::typed(value, Datatype::from_iri(format!("{XSD}{datatype}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_become_words() {
        assert_eq!(class_label("DAMAGE_REPORTS"), "damage report");
        assert_eq!(class_label("companies"), "company");
        assert_eq!(class_label("order_status"), "order status");
        assert_eq!(class_label("addresses"), "address");
        assert_eq!(
            property_label("employer_id", true).as_deref(),
            Some("employer")
        );
        assert_eq!(
            property_label("employerId", true).as_deref(),
            Some("employer")
        );
        assert_eq!(
            property_label("employer_id", false).as_deref(),
            Some("employer id")
        );
        assert_eq!(pascal("damage report"), "DamageReport");
        assert_eq!(camel("works for"), "worksFor");
        assert_eq!(slug("Net 30 days!"), "net-30-days");
    }

    #[test]
    fn the_rules() {
        let model = Model {
            tables: vec![
                Table {
                    name: "companies".into(),
                    description: Some("Customer accounts.".into()),
                    synonyms: vec!["accounts".into(), "clients".into()],
                    columns: vec![
                        Column {
                            name: "id".into(),
                            datatype: Some("integer"),
                            ..Default::default()
                        },
                        Column {
                            name: "name".into(),
                            datatype: Some("string"),
                            ..Default::default()
                        },
                        Column {
                            name: "tier".into(),
                            values: vec!["Gold".into(), "Silver".into()],
                            ..Default::default()
                        },
                    ],
                    primary_key: vec!["id".into()],
                },
                Table {
                    name: "people".into(),
                    columns: vec![
                        Column {
                            name: "employer_id".into(),
                            references: Some("companies".into()),
                            ..Default::default()
                        },
                        Column {
                            name: "name".into(),
                            datatype: Some("string"),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            notes: Vec::new(),
        };
        let out = emit(
            &model,
            &EmitOptions {
                base: "https://ex.com/".into(),
            },
        );
        // company, person, tier
        assert_eq!(out.classes, 3);
        assert_eq!(out.enum_classes, 1);
        assert_eq!(out.records, 2);
        assert_eq!(out.keys_left_out, 1);
        let has = |s: &str, p: &str, o: &str| {
            out.ontology.iter().any(|q| {
                q.subject.as_iri() == Some(s)
                    && q.is(p)
                    && (q.object.as_iri() == Some(o)
                        || q.object
                            .as_literal()
                            .is_some_and(|(v, ..)| v.lexical() == o))
            })
        };
        assert!(has(
            "https://ex.com/Company",
            RDFS_COMMENT,
            "Customer accounts. Also called: accounts, clients."
        ));
        assert!(has("https://ex.com/employer", RDFS_LABEL, "employer"));
        assert!(has(
            "https://ex.com/employer",
            RDFS_RANGE,
            "https://ex.com/Company"
        ));
        assert!(has(
            "https://ex.com/tier",
            RDFS_RANGE,
            "https://ex.com/Tier"
        ));
        // one `name` property for both tables
        assert!(has(
            "https://ex.com/name",
            RDFS_DOMAIN,
            "https://ex.com/Company"
        ));
        assert!(has(
            "https://ex.com/name",
            RDFS_DOMAIN,
            "https://ex.com/Person"
        ));
        assert!(out
            .catalog
            .iter()
            .any(|q| q.subject.as_iri() == Some("https://ex.com/Tier/gold")));
    }

    fn labels_of(out: &Converted, kind: &str) -> Vec<(String, String)> {
        let typed: std::collections::HashSet<String> = out
            .ontology
            .iter()
            .filter(|q| q.is(RDF_TYPE) && q.object.as_iri() == Some(kind))
            .filter_map(|q| q.subject.as_iri().map(str::to_string))
            .collect();
        let mut v: Vec<(String, String)> = out
            .ontology
            .iter()
            .filter(|q| q.is(RDFS_LABEL))
            .filter_map(|q| {
                let s = q.subject.as_iri()?.to_string();
                let l = q.object.as_literal()?.0.lexical();
                typed.contains(&s).then_some((s, l))
            })
            .collect();
        v.sort();
        v
    }

    #[test]
    fn two_names_never_become_one_term() {
        let col = |name: &str| Column {
            name: name.into(),
            ..Default::default()
        };
        let model = Model {
            tables: vec![
                Table {
                    name: "orders".into(),
                    columns: vec![
                        Column {
                            name: "status".into(),
                            values: vec![
                                "open".into(),
                                "Open!".into(),
                                "open".into(),
                                "???".into(),
                                "!!!".into(),
                            ],
                            ..Default::default()
                        },
                        Column {
                            name: "employer_id".into(),
                            references: Some("companies".into()),
                            ..Default::default()
                        },
                        Column {
                            name: "employer".into(),
                            references: Some("companies".into()),
                            ..Default::default()
                        },
                        col("123"),
                    ],
                    ..Default::default()
                },
                Table {
                    name: "tickets".into(),
                    columns: vec![Column {
                        name: "status".into(),
                        values: vec!["new".into()],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                Table {
                    name: "companies".into(),
                    columns: vec![col("name")],
                    ..Default::default()
                },
            ],
            notes: Vec::new(),
        };
        let out = emit(
            &model,
            &EmitOptions {
                base: "https://ex.com/".into(),
            },
        );
        let classes = labels_of(&out, RDFS_CLASS);
        let class_labels: Vec<&str> = classes.iter().map(|(_, l)| l.as_str()).collect();
        // two enumerations named status: the second reads with its table's name
        assert!(class_labels.contains(&"status"), "{classes:?}");
        assert!(class_labels.contains(&"ticket status"), "{classes:?}");
        let properties = labels_of(&out, RDF_PROPERTY);
        let mut seen = std::collections::HashSet::new();
        assert!(
            properties.iter().all(|(_, l)| seen.insert(l.clone())),
            "no two properties share a label: {properties:?}"
        );
        // employer_id and employer are two relations, not one
        assert_eq!(
            properties
                .iter()
                .filter(|(_, l)| l.contains("employer"))
                .count(),
            2,
            "{properties:?}"
        );
        // distinct values that slug alike keep their own records; repeats are one
        assert_eq!(out.records, 5, "open, Open!, ???, !!! and new");
        let record_iris: std::collections::HashSet<String> = out
            .catalog
            .iter()
            .filter_map(|q| q.subject.as_iri().map(str::to_string))
            .collect();
        assert_eq!(record_iris.len(), 5);
        assert!(
            out.notes.iter().any(|n| n.contains("no letters")),
            "{:?}",
            out.notes
        );
    }

    #[test]
    fn code_named_keys_keep_their_words() {
        let fk = |name: &str, to: &str| Column {
            name: name.into(),
            references: Some(to.into()),
            ..Default::default()
        };
        let model = Model {
            tables: vec![
                Table {
                    name: "flights".into(),
                    columns: vec![
                        fk("origin_airport_code", "airports"),
                        fk("destination_airport_code", "airports"),
                        fk("airport_code", "airports"),
                    ],
                    ..Default::default()
                },
                Table {
                    name: "airports".into(),
                    ..Default::default()
                },
                Table {
                    name: "contacts".into(),
                    columns: vec![fk("account_id", "accounts")],
                    ..Default::default()
                },
                Table {
                    name: "orders".into(),
                    columns: vec![fk("account_id", "accounts")],
                    ..Default::default()
                },
                Table {
                    name: "invoices".into(),
                    columns: vec![fk("account_id", "accounts")],
                    ..Default::default()
                },
                Table {
                    name: "accounts".into(),
                    ..Default::default()
                },
            ],
            notes: Vec::new(),
        };
        let out = emit(
            &model,
            &EmitOptions {
                base: "https://ex.com/".into(),
            },
        );
        let properties = labels_of(&out, RDF_PROPERTY);
        let labels: Vec<&str> = properties.iter().map(|(_, l)| l.as_str()).collect();
        // three relations to airports, three labels; nothing merged
        assert!(labels.contains(&"origin airport"), "{labels:?}");
        assert!(labels.contains(&"destination airport"), "{labels:?}");
        assert!(labels.contains(&"airport"), "{labels:?}");
        let comment = |iri: &str| -> String {
            out.ontology
                .iter()
                .find(|q| q.subject.as_iri() == Some(iri) && q.is(RDFS_COMMENT))
                .and_then(|q| q.object.as_literal().map(|(v, ..)| v.lexical()))
                .unwrap_or_default()
        };
        // one generated description for a relation every table shares, with
        // "an" before a vowel
        assert_eq!(
            comment("https://ex.com/account"),
            "The account of a contact, order or invoice."
        );
        assert_eq!(
            comment("https://ex.com/originAirport"),
            "A flight's origin airport: an airport."
        );
        assert_eq!(
            comment("https://ex.com/airport"),
            "The airport of a flight."
        );
    }
}
