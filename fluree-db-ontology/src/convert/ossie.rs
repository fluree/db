//! Apache Ossie documents (formerly Open Semantic Interchange), version
//! 0.2: a semantic model, or an ontology.
//!
//! A **semantic model** (`datasets`, `relationships`, `metrics`) is a data
//! dictionary, read into the same table model as DDL: datasets are tables,
//! fields are columns (with their `description`, `datatype` and
//! `ai_context` synonyms), relationships are foreign keys from their `from`
//! fields to the `to` dataset, and metrics are left out.
//!
//! An **ontology** (`ontology:` a list of concepts) is already conceptual, so
//! it is written straight to RDF, keeping the IRIs it gives:
//!
//! - an `EntityType` concept is a class (its `extends` its parents);
//! - a `ValueType` concept is a datatype, resolved through `extends` to a
//!   built-in (`Integer`, `Date`, ...); one whose `requires` lists its values
//!   (`Code == 'A' OR Code == 'B'`) is enumerated: a class with a catalog
//!   record per value;
//! - a binary relationship is a property from the concept that declares it
//!   to its role's concept or datatype, described by its `description`, else
//!   by its verbalization ("{Flight} uses {Aircraft}" → "A flight uses an
//!   aircraft.");
//! - a unary relationship (`{Flight} was diverted`) is a boolean property;
//! - a relationship with more roles is a class with a property per role;
//! - `derived_by`, `requires` (other than an enumeration), `identify_by` and
//!   mappings are logic and bindings, not what a document says, and are left
//!   out.

use super::sql::xsd_for;
use super::{camel, pascal, slug, Column, Converted, Model, Table};
use crate::read::Quad;
use crate::vocab::*;
use crate::words::name_words;
use fluree_graph_ir::Term;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

fn text(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
}

fn list<'a>(v: &'a Value, key: &str) -> impl Iterator<Item = &'a Value> {
    v.get(key).and_then(Value::as_array).into_iter().flatten()
}

fn strings(v: &Value, key: &str) -> Vec<String> {
    list(v, key)
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect()
}

/// Whether a document is an Ossie semantic model.
pub fn is_semantic_model(doc: &Value) -> bool {
    doc.get("datasets").and_then(Value::as_array).is_some()
}

/// Whether a document is an Ossie ontology.
pub fn is_ontology(doc: &Value) -> bool {
    doc.get("ontology")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|i| i.get("concept").is_some()))
}

/// `ai_context`'s synonyms, and any instructions it gives as text.
fn ai_context(v: &Value) -> (Vec<String>, Option<String>) {
    match v.get("ai_context") {
        Some(Value::String(s)) => (Vec::new(), Some(s.clone())),
        Some(ctx @ Value::Object(_)) => (strings(ctx, "synonyms"), text(ctx, "instructions")),
        _ => (Vec::new(), None),
    }
}

/// An Ossie data type as an XSD datatype's local name.
fn ossie_datatype(name: &str) -> Option<&'static str> {
    Some(match name {
        "String" => "string",
        "Integer" => "integer",
        "Decimal" | "Float" => "decimal",
        "Boolean" => "boolean",
        "Date" => "date",
        "Time" => "time",
        "DateTime" | "DateTimeTz" => "dateTime",
        _ => return xsd_for(name),
    })
}

/// A semantic model's tables.
pub fn semantic_model(doc: &Value) -> Model {
    let mut tables = Vec::new();
    for d in list(doc, "datasets") {
        let Some(name) = text(d, "name") else {
            continue;
        };
        let (synonyms, instructions) = ai_context(d);
        let mut table = Table {
            name,
            description: text(d, "description").or(instructions),
            synonyms,
            primary_key: strings(d, "primary_key"),
            ..Table::default()
        };
        for f in list(d, "fields") {
            let Some(field) = text(f, "name") else {
                continue;
            };
            let (synonyms, instructions) = ai_context(f);
            table.columns.push(Column {
                name: field,
                datatype: text(f, "datatype").as_deref().and_then(ossie_datatype),
                description: text(f, "description").or(instructions),
                synonyms,
                ..Column::default()
            });
        }
        tables.push(table);
    }
    for r in list(doc, "relationships") {
        let (Some(from), Some(to)) = (text(r, "from"), text(r, "to")) else {
            continue;
        };
        let from_columns = strings(r, "from_columns");
        let to_columns = strings(r, "to_columns");
        if let Some(t) = tables
            .iter_mut()
            .find(|t| t.name.eq_ignore_ascii_case(&from))
        {
            if let Some(first) = from_columns.first() {
                match t
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(first))
                {
                    Some(c) => c.references = Some(to.clone()),
                    None => t.columns.push(Column {
                        name: first.clone(),
                        references: Some(to.clone()),
                        ..Column::default()
                    }),
                }
                for other in from_columns.iter().skip(1) {
                    t.columns.retain(|c| !c.name.eq_ignore_ascii_case(other));
                }
            }
        }
        if let Some(t) = tables.iter_mut().find(|t| t.name.eq_ignore_ascii_case(&to)) {
            for c in &to_columns {
                if !t.primary_key.iter().any(|k| k.eq_ignore_ascii_case(c)) {
                    t.primary_key.push(c.clone());
                }
            }
        }
    }
    Model { tables }
}

const BUILT_IN_VALUES: &[&str] = &[
    "Boolean", "Date", "DateTime", "Decimal", "Float", "Integer", "String",
];

/// The values an enumerated value type's `requires` lists:
/// `Code == 'A' OR Code == 'B'` (or `IN ('A', 'B')`).
fn enumeration(concept: &str, requires: &[String]) -> Vec<String> {
    let mut values = Vec::new();
    for r in requires {
        let quoted = |s: &str| -> Option<String> {
            let s = s.trim();
            s.strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .or_else(|| s.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
                .map(str::to_string)
        };
        if let Some((lhs, rhs)) = r.split_once(" IN ") {
            if lhs.trim() == concept {
                let inner = rhs.trim().trim_start_matches('(').trim_end_matches(')');
                let found: Option<Vec<String>> = inner.split(',').map(quoted).collect();
                if let Some(found) = found {
                    values.extend(found);
                }
            }
            continue;
        }
        let parts: Vec<&str> = r.split(" OR ").collect();
        let found: Option<Vec<String>> = parts
            .iter()
            .map(|p| {
                let (lhs, rhs) = p.split_once("==")?;
                (lhs.trim() == concept).then_some(())?;
                quoted(rhs)
            })
            .collect();
        if let Some(found) = found {
            values.extend(found);
        }
    }
    values
}

fn article(word: &str) -> &'static str {
    if word.starts_with(['a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U']) {
        "an"
    } else {
        "a"
    }
}

/// A verbalization as a sentence: `{Flight} uses {Aircraft}` → "A flight
/// uses an aircraft." (`{Store:destination}` reads "a destination store").
fn verbalize(pattern: &str) -> String {
    let mut out = String::new();
    let mut rest = pattern;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let Some(close) = rest[open..].find('}') else {
            break;
        };
        let inner = &rest[open + 1..open + close];
        let (concept, role) = inner.split_once(':').unwrap_or((inner, ""));
        let mut phrase = name_words(concept).unwrap_or_else(|| concept.to_lowercase());
        if !role.is_empty() {
            phrase = format!("{} {phrase}", name_words(role).unwrap_or_default());
        }
        out.push_str(&format!("{} {phrase}", article(&phrase)));
        rest = &rest[open + close + 1..];
    }
    out.push_str(rest);
    // a hyphen marks a word's attachment for constraint verbalization
    let sentence = out
        .replace("- ", " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let mut c = sentence.chars();
    match c.next() {
        Some(f) => format!("{}{}.", f.to_uppercase(), c.as_str().trim_end_matches('.')),
        None => sentence,
    }
}

/// An ontology, written as RDF.
pub fn ontology(doc: &Value, base: &str) -> Converted {
    let mut prefixes: HashMap<String, String> = HashMap::new();
    if let Some(Value::Object(map)) = doc.get("prefixes") {
        for (k, v) in map {
            if let Some(ns) = v.as_str() {
                prefixes.insert(k.clone(), ns.to_string());
            }
        }
    }
    let expand = |iri: &str| -> String {
        match iri.split_once(':') {
            Some((p, local)) if !local.starts_with("//") && prefixes.contains_key(p) => {
                format!("{}{local}", prefixes[p])
            }
            _ => iri.to_string(),
        }
    };
    let concepts: Vec<&Value> = list(doc, "ontology")
        .filter(|c| c.get("concept").is_some())
        .collect();
    let by_name: HashMap<String, &Value> = concepts
        .iter()
        .filter_map(|c| text(c, "concept").map(|n| (n, *c)))
        .collect();
    let is_value = |name: &str| -> bool {
        BUILT_IN_VALUES.contains(&name)
            || by_name
                .get(name)
                .is_some_and(|c| text(c, "type").as_deref() == Some("ValueType"))
    };
    // a value type's built-in, through its `extends`
    let built_in = |name: &str| -> Option<&'static str> {
        let mut seen = HashSet::new();
        let mut current = name.to_string();
        loop {
            if BUILT_IN_VALUES.contains(&current.as_str()) {
                return ossie_datatype(&current);
            }
            if !seen.insert(current.clone()) {
                return None;
            }
            current = by_name
                .get(&current)
                .and_then(|c| strings(c, "extends").into_iter().next())?;
        }
    };
    let mut out = Converted::default();
    let push = |out: &mut Converted, s: &str, p: &str, o: Term| {
        out.ontology.push(Quad::new(Term::iri(s), p, o));
    };
    let mut used: HashSet<String> = HashSet::new();
    let mut mint = |local: String| -> String {
        let mut candidate = local.clone();
        let mut n = 2;
        while !used.insert(candidate.clone()) {
            candidate = format!("{local}{n}");
            n += 1;
        }
        format!("{base}{candidate}")
    };
    // classes: entity types, and enumerated value types
    let mut class_iri: HashMap<String, String> = HashMap::new();
    let mut enum_values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for c in &concepts {
        let Some(name) = text(c, "concept") else {
            continue;
        };
        let entity = text(c, "type").as_deref() != Some("ValueType");
        let values = if entity {
            Vec::new()
        } else {
            enumeration(&name, &strings(c, "requires"))
        };
        if !entity && values.is_empty() {
            continue;
        }
        let label = name_words(&name).unwrap_or_else(|| name.to_lowercase());
        let iri = text(c, "iri")
            .map(|i| expand(&i))
            .unwrap_or_else(|| mint(pascal(&label)));
        class_iri.insert(name.clone(), iri.clone());
        if !values.is_empty() {
            enum_values.insert(name.clone(), values);
        }
        push(&mut out, &iri, RDF_TYPE, Term::iri(RDFS_CLASS));
        push(&mut out, &iri, RDFS_LABEL, Term::string(&label));
        if let Some(d) = text(c, "description") {
            push(&mut out, &iri, RDFS_COMMENT, Term::string(d));
        }
        out.classes += 1;
    }
    for c in &concepts {
        let Some(iri) = text(c, "concept").and_then(|n| class_iri.get(&n).cloned()) else {
            continue;
        };
        for parent in strings(c, "extends") {
            if let Some(p) = class_iri.get(&parent) {
                push(&mut out, &iri, RDFS_SUBCLASS_OF, Term::iri(p));
            }
        }
    }
    for (name, values) in &enum_values {
        let iri = &class_iri[name];
        for v in values {
            let record = format!("{iri}/{}", slug(v));
            out.catalog
                .push(Quad::new(Term::iri(&record), RDF_TYPE, Term::iri(iri)));
            out.catalog.push(Quad::new(
                Term::iri(&record),
                SKOS_PREF_LABEL,
                Term::string(v),
            ));
            out.records += 1;
        }
        out.enum_classes += 1;
    }
    // relationship names declared by more than one concept read with the concept's name
    let mut name_count: HashMap<String, usize> = HashMap::new();
    for c in &concepts {
        for r in list(c, "relationships") {
            if let Some(n) = text(r, "name") {
                *name_count.entry(n).or_default() += 1;
            }
        }
    }
    for c in &concepts {
        let Some(owner) = text(c, "concept") else {
            continue;
        };
        let Some(domain) = class_iri.get(&owner).cloned() else {
            continue;
        };
        if is_value(&owner) && !enum_values.contains_key(&owner) {
            continue;
        }
        let owner_label = name_words(&owner).unwrap_or_else(|| owner.to_lowercase());
        for r in list(c, "relationships") {
            let Some(rname) = text(r, "name") else {
                continue;
            };
            let words = name_words(&rname).unwrap_or_else(|| rname.to_lowercase());
            let label = if name_count.get(&rname).copied().unwrap_or(0) > 1 {
                format!("{owner_label} {words}")
            } else {
                words
            };
            let roles: Vec<&Value> = list(r, "roles").collect();
            let verbal = strings(r, "verbalizes").first().map(|v| verbalize(v));
            let mut description = text(r, "description").or(verbal);
            let property = |out: &mut Converted,
                            iri: &str,
                            label: &str,
                            description: Option<String>,
                            domain: &str,
                            range: Option<String>| {
                out.ontology
                    .push(Quad::new(Term::iri(iri), RDF_TYPE, Term::iri(RDF_PROPERTY)));
                out.ontology
                    .push(Quad::new(Term::iri(iri), RDFS_LABEL, Term::string(label)));
                if let Some(d) = description {
                    out.ontology
                        .push(Quad::new(Term::iri(iri), RDFS_COMMENT, Term::string(d)));
                }
                out.ontology
                    .push(Quad::new(Term::iri(iri), RDFS_DOMAIN, Term::iri(domain)));
                if let Some(r) = range {
                    out.ontology
                        .push(Quad::new(Term::iri(iri), RDFS_RANGE, Term::iri(r)));
                }
                out.properties += 1;
            };
            let range_of = |concept: &str| -> Option<String> {
                class_iri
                    .get(concept)
                    .cloned()
                    .or_else(|| built_in(concept).map(|dt| format!("{XSD}{dt}")))
            };
            match roles.as_slice() {
                [] => {
                    let iri = text(r, "iri")
                        .map(|i| expand(&i))
                        .unwrap_or_else(|| mint(camel(&label)));
                    property(
                        &mut out,
                        &iri,
                        &label,
                        description,
                        &domain,
                        Some(format!("{XSD}boolean")),
                    );
                }
                [role] => {
                    let Some(player) = text(role, "concept") else {
                        continue;
                    };
                    // what a value type means, when the relationship doesn't say
                    if let Some(vt) = by_name.get(&player).filter(|_| is_value(&player)) {
                        if let Some(d) = text(vt, "description") {
                            description = Some(match description {
                                Some(s) => format!("{s} {d}"),
                                None => d,
                            });
                        }
                    }
                    let iri = text(r, "iri")
                        .map(|i| expand(&i))
                        .unwrap_or_else(|| mint(camel(&label)));
                    let range = range_of(&player);
                    property(&mut out, &iri, &label, description, &domain, range);
                }
                many => {
                    // a link among three or more things: a class with a property per role
                    let class = mint(pascal(&label));
                    push(&mut out, &class, RDF_TYPE, Term::iri(RDFS_CLASS));
                    push(&mut out, &class, RDFS_LABEL, Term::string(&label));
                    if let Some(d) = description {
                        push(&mut out, &class, RDFS_COMMENT, Term::string(d));
                    }
                    out.classes += 1;
                    let first = std::iter::once((owner.clone(), String::new()));
                    let others = many.iter().filter_map(|role| {
                        Some((
                            text(role, "concept")?,
                            text(role, "name").unwrap_or_default(),
                        ))
                    });
                    for (player, role_name) in first.chain(others) {
                        let role_label = if role_name.is_empty() {
                            name_words(&player).unwrap_or_else(|| player.to_lowercase())
                        } else {
                            name_words(&role_name).unwrap_or(role_name.clone())
                        };
                        let iri = mint(camel(&format!("{label} {role_label}")));
                        property(&mut out, &iri, &role_label, None, &class, range_of(&player));
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONTOLOGY: &str = r"
version: 0.2.0.dev0
name: Flights
prefixes:
  schema: https://schema.org/
ontology:
- concept: CancelationCode
  description: The single character code that identifies the reason a flight is canceled
  type: ValueType
  extends: [ String ]
  requires: [ CancelationCode == 'A' OR CancelationCode == 'B' ]
- concept: NrMinutes
  type: ValueType
  extends: [ Decimal ]
- concept: Delay
  description: Minutes late.
  type: ValueType
  extends: [ NrMinutes ]
- concept: Airport
  type: EntityType
  iri: schema:Airport
  description: An airport identified by its IATA code.
- concept: Flight
  type: EntityType
  relationships:
  - name: departs_from
    roles:
    - concept: Airport
    verbalizes: [ '{Flight} departs from {Airport}' ]
    multiplicity: ManyToOne
  - name: delay
    roles:
    - concept: Delay
    verbalizes: [ '{Flight} has {Delay}' ]
  - name: diverted
    verbalizes: [ '{Flight} was diverted' ]
  - name: canceled_for
    roles:
    - concept: CancelationCode
    verbalizes: [ '{Flight} was canceled for {CancelationCode}' ]
  - name: stops_at
    roles:
    - concept: Airport
    - concept: Date
    verbalizes: [ '{Flight} stops at {Airport} on {Date}' ]
- concept: Charter
  type: EntityType
  extends: [ Flight ]
";

    #[test]
    fn an_ontology_as_rdf() {
        let doc: Value = serde_saphyr::from_str(ONTOLOGY).unwrap();
        assert!(is_ontology(&doc));
        let out = ontology(&doc, "https://ex.com/");
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
        assert!(
            has("https://schema.org/Airport", RDFS_LABEL, "airport"),
            "its IRI is kept"
        );
        assert!(has(
            "https://ex.com/departsFrom",
            RDFS_COMMENT,
            "A flight departs from an airport."
        ));
        assert!(has(
            "https://ex.com/departsFrom",
            RDFS_RANGE,
            "https://schema.org/Airport"
        ));
        assert!(has(
            "https://ex.com/delay",
            RDFS_RANGE,
            &format!("{XSD}decimal")
        ));
        assert!(has(
            "https://ex.com/delay",
            RDFS_COMMENT,
            "A flight has a delay. Minutes late."
        ));
        assert!(has(
            "https://ex.com/diverted",
            RDFS_RANGE,
            &format!("{XSD}boolean")
        ));
        assert!(has(
            "https://ex.com/canceledFor",
            RDFS_RANGE,
            "https://ex.com/CancelationCode"
        ));
        assert!(has(
            "https://ex.com/Charter",
            RDFS_SUBCLASS_OF,
            "https://ex.com/Flight"
        ));
        assert!(
            has("https://ex.com/StopsAt", RDF_TYPE, RDFS_CLASS),
            "a ternary link is a class"
        );
        assert_eq!(out.records, 2);
        assert_eq!(out.enum_classes, 1);
    }

    #[test]
    fn a_semantic_model() {
        let doc: Value = serde_saphyr::from_str(
            r"
version: 0.2.0.dev0
name: sales
datasets:
  - name: customers
    source: sales.public.customers
    primary_key: [id]
    description: People who buy.
    ai_context: {synonyms: [buyers]}
    fields:
      - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
      - {name: signup_date, datatype: Date, expression: {dialects: [{dialect: ANSI_SQL, expression: signup_date}]}}
  - name: orders
    source: sales.public.orders
    fields:
      - {name: customer_id, expression: {dialects: [{dialect: ANSI_SQL, expression: customer_id}]}}
relationships:
  - {name: orders_to_customers, from: orders, to: customers, from_columns: [customer_id], to_columns: [id]}
",
        )
        .unwrap();
        assert!(is_semantic_model(&doc));
        let m = semantic_model(&doc);
        assert_eq!(m.tables[0].synonyms, ["buyers"]);
        assert_eq!(m.tables[0].columns[1].datatype, Some("date"));
        assert_eq!(
            m.tables[1].columns[0].references.as_deref(),
            Some("customers")
        );
    }

    #[test]
    fn verbalizations() {
        assert_eq!(
            verbalize("{Store} ships to {Store:destination} in {NrDays}"),
            "A store ships to a destination store in a nr days."
        );
        assert_eq!(
            verbalize("{CancelationReason} has description- {String}"),
            "A cancelation reason has description a string."
        );
    }
}
