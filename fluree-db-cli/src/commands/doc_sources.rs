//! Where `--ontology` and `--entities` come from.
//!
//! A source is a ledger in the local store, a graph source mapped over a
//! table (`fluree iceberg|sql|delta map`), or an RDF file. A file is loaded
//! into a scratch in-memory ledger, so every source is queried the same
//! way with the database's own parsers and no second RDF stack. Names and
//! file paths are told apart by extension: `.ttl`, `.nt`, `.jsonld` and
//! `.json` are files.

use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_doc::gazetteer::{Gazetteer, GazetteerBuilder};
use fluree_db_doc::model::{Model, ModelBuilder};
use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceKind {
    Ledger(String),
    File(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub kind: SourceKind,
    /// `#Class` suffix: only subjects of this type.
    pub class: Option<String>,
}

impl Source {
    /// `ledger`, `ledger#Class`, `path.ttl`, `path.ttl#Class`. Ledger names
    /// and paths carry no `#`, so everything after the first one is the
    /// class, full IRIs with their own fragment included.
    pub fn parse(spec: &str) -> Self {
        let (base, class) = match spec.split_once('#') {
            Some((b, c)) if !b.is_empty() && !c.is_empty() => (b, Some(c.to_string())),
            Some((b, _)) if !b.is_empty() => (b, None),
            _ => (spec, None),
        };
        let kind = if is_rdf_file(base) {
            SourceKind::File(PathBuf::from(base))
        } else {
            SourceKind::Ledger(base.to_string())
        };
        Self { kind, class }
    }

    pub fn label(&self) -> String {
        let base = match &self.kind {
            SourceKind::Ledger(alias) => alias.clone(),
            SourceKind::File(path) => path.display().to_string(),
        };
        match &self.class {
            Some(c) => format!("{base}#{c}"),
            None => base,
        }
    }
}

fn is_rdf_file(spec: &str) -> bool {
    Path::new(spec)
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| {
            matches!(
                e.to_ascii_lowercase().as_str(),
                "ttl" | "nt" | "jsonld" | "json"
            )
        })
}

/// A source made queryable: the store holding it and the alias to query.
pub struct Opened {
    fluree: Fluree,
    alias: String,
    /// Queried through the graph-source path, which maps the table's rows
    /// to triples as it reads them; a ledger or file has flakes to read.
    graph_source: bool,
}

impl Opened {
    pub async fn open(source: &Source, dirs: &FlureeDir) -> CliResult<Self> {
        match &source.kind {
            SourceKind::Ledger(alias) => {
                let fluree = context::build_fluree(dirs).await?;
                let ledger_id = context::to_ledger_id(alias)?;
                let graph_source = if fluree.ledger_exists(&ledger_id).await? {
                    false
                } else if is_graph_source(&fluree, &ledger_id).await? {
                    true
                } else {
                    return Err(CliError::NotFound(format!(
                        "{alias}: no such ledger or graph source (a file source needs a .ttl, .nt, .jsonld or .json extension)"
                    )));
                };
                Ok(Self {
                    fluree,
                    alias: alias.clone(),
                    graph_source,
                })
            }
            SourceKind::File(path) => {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| CliError::Input(format!("{}: {e}", path.display())))?;
                let fluree = FlureeBuilder::memory().build_memory();
                let alias = "scratch:main".to_string();
                fluree
                    .create_ledger(&context::to_ledger_id(&alias)?)
                    .await?;
                let g = fluree.graph(&alias);
                let ext = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(str::to_ascii_lowercase)
                    .unwrap_or_default();
                if ext == "ttl" || ext == "nt" {
                    g.transact().insert_turtle(&content).commit().await?;
                } else {
                    let json: Value = serde_json::from_str(&content).map_err(|e| {
                        CliError::Input(format!("{}: not JSON: {e}", path.display()))
                    })?;
                    g.transact().insert(&json).commit().await?;
                }
                Ok(Self {
                    fluree,
                    alias,
                    graph_source: false,
                })
            }
        }
    }

    pub async fn rows(&self, query: &Value) -> CliResult<Vec<Value>> {
        if self.graph_source {
            let json = self
                .fluree
                .graph(&self.alias)
                .query()
                .jsonld(query)
                .execute_formatted()
                .await?;
            return Ok(json.as_array().cloned().unwrap_or_default());
        }
        let view = self.fluree.db_with_default_context(&self.alias).await?;
        let result = self.fluree.query(&view, query).await?;
        let json = result.to_jsonld(&view.snapshot)?;
        Ok(json.as_array().cloned().unwrap_or_default())
    }
}

/// A graph source this build can read. Without the `iceberg` feature the
/// table readers are not compiled in, so a mapped source reads as missing
/// rather than failing on its first query.
async fn is_graph_source(fluree: &Fluree, id: &str) -> CliResult<bool> {
    if !cfg!(feature = "iceberg") {
        return Ok(false);
    }
    Ok(fluree
        .nameservice()
        .lookup_graph_source(id)
        .await?
        .is_some_and(|r| !r.retracted))
}

pub async fn load_model(source: &Source, dirs: &FlureeDir) -> CliResult<Model> {
    let opened = Opened::open(source, dirs).await?;
    let mut builder = ModelBuilder::default();
    for query in Model::class_queries() {
        builder.add_class_rows(&opened.rows(&query).await?);
    }
    for (query, kind) in Model::property_queries() {
        builder.add_property_rows(&opened.rows(&query).await?, kind);
    }
    let model = builder.build();
    if model.is_empty() {
        return Err(CliError::Input(format!(
            "--ontology {}: no classes or properties found (rdfs:Class, owl:Class, rdf:Property, owl:ObjectProperty, owl:DatatypeProperty, or anything with rdfs:subClassOf / rdfs:domain / schema:domainIncludes)",
            source.label()
        )));
    }
    Ok(model)
}

pub struct LoadedGazetteer {
    pub gazetteer: Gazetteer,
    /// Labels found per source, for the run's announcement.
    pub counts: Vec<(String, usize)>,
    /// Over the sources alone. The target ledger's own entities are not
    /// part of it: they grow with every run, and a document is unchanged
    /// when its inputs are.
    pub fingerprint: String,
}

/// Every label from every source, plus the entities earlier runs minted
/// into the target ledger, so a name already on a node keeps that node.
/// A `#Class` scope must be something the label queries can name: a full
/// IRI, or a compact form under a prefix they know.
fn check_class_scope(source: &Source) -> CliResult<()> {
    let Some(class) = &source.class else {
        return Ok(());
    };
    if class.contains("://") || class.starts_with("urn:") {
        return Ok(());
    }
    let known = fluree_db_doc::model::PREFIXES
        .iter()
        .map(|(p, _)| *p)
        .chain(std::iter::once("doc"));
    let prefix = class.split_once(':').map(|(p, _)| p).unwrap_or("");
    if known.clone().any(|k| k == prefix) {
        return Ok(());
    }
    Err(CliError::Usage(format!(
        "--entities {}: class scope '{class}' uses a prefix the scan does not know; write the full IRI (known prefixes: {})",
        source.label(),
        known.collect::<Vec<_>>().join(", ")
    )))
}

pub async fn load_gazetteer(
    sources: &[Source],
    target: Option<(&Fluree, &str)>,
    lang: &str,
    dirs: &FlureeDir,
) -> CliResult<LoadedGazetteer> {
    let mut builder = GazetteerBuilder::default();
    let mut counts = Vec::new();
    for source in sources {
        check_class_scope(source)?;
        let opened = Opened::open(source, dirs).await?;
        let mut n = 0;
        for (query, predicate) in Gazetteer::queries(source.class.as_deref()) {
            let rows = opened.rows(&query).await?;
            n += rows.len();
            builder.add_rows(&rows, predicate);
        }
        counts.push((source.label(), n));
    }
    let fingerprint = builder.fingerprint();
    if let Some((fluree, alias)) = target {
        let opened = Opened {
            fluree: fluree.clone(),
            alias: alias.to_string(),
            graph_source: false,
        };
        for (query, predicate) in Gazetteer::queries(Some(fluree_db_doc::vocab::ENTITY)) {
            builder.add_rows(&opened.rows(&query).await?, predicate);
        }
        // An entity minted outside the ontology is kept for review, not
        // trusted: "notes" scanned for by name would tag every page.
        let off_model = opened
            .rows(&serde_json::json!({
                "@context": { "doc": fluree_db_doc::vocab::DOC_NS },
                "where": [{ "@id": "?s", "@type": fluree_db_doc::vocab::ENTITY, fluree_db_doc::vocab::OFF_MODEL: true }],
                "select": ["?s"]
            }))
            .await?
            .iter()
            .filter_map(|row| row.as_array()?.first()?.as_str().map(str::to_string))
            .collect();
        builder.remove(&off_model);
    }
    Ok(LoadedGazetteer {
        gazetteer: builder.build(lang),
        counts,
        fingerprint,
    })
}

/// An ontology as RDF, for a reader that is sent the ontology itself: a
/// file read as it is, or a ledger's default graph.
pub async fn load_rdf(source: &Source, dirs: &FlureeDir) -> CliResult<fluree_graph_ir::Graph> {
    use fluree_db_doc::reader::request::{parse_jsonld, parse_turtle};
    let label = source.label();
    let graph = match &source.kind {
        SourceKind::File(path) => {
            let content = std::fs::read_to_string(path)
                .map_err(|e| CliError::Input(format!("{}: {e}", path.display())))?;
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .map(str::to_ascii_lowercase)
                .unwrap_or_default();
            if ext == "ttl" || ext == "nt" {
                parse_turtle(&content)
            } else {
                parse_jsonld(&content)
            }
            .map_err(|e| CliError::Input(format!("{label}: {e}")))?
        }
        SourceKind::Ledger(alias) => {
            let opened = Opened::open(source, dirs).await?;
            if opened.graph_source {
                return Err(CliError::Usage(format!(
                    "--ontology {alias}: a graph source cannot be an ontology; export one to a file, or name a ledger"
                )));
            }
            let mut bytes = Vec::new();
            opened
                .fluree
                .export(&opened.alias)
                .format(fluree_db_api::export::ExportFormat::NTriples)
                .write_to(&mut bytes)
                .await?;
            parse_turtle(&String::from_utf8_lossy(&bytes))
                .map_err(|e| CliError::Input(format!("{label}: {e}")))?
        }
    };
    if graph.is_empty() {
        return Err(CliError::Input(format!("--ontology {label}: no triples")));
    }
    Ok(graph)
}

/// What a catalog record carries: its names, a short description, and its
/// types. Everything else about a subject stays behind, because the whole
/// catalog goes into every window a reader reads.
const RECORD_PREDICATES: &[&str] = &[
    "skos:prefLabel",
    "skos:altLabel",
    "skos:hiddenLabel",
    "skos:definition",
    "rdfs:label",
    "rdfs:comment",
    "schema:name",
    "schema:alternateName",
    "schema:legalName",
    "schema:description",
];

pub struct LoadedCatalog {
    pub graph: fluree_graph_ir::Graph,
    /// Records found per source, for the run's announcement.
    pub counts: Vec<(String, usize)>,
}

/// The catalog a reader is sent: each source's records, scoped to its
/// `#Class` when it has one.
pub async fn load_catalog(sources: &[Source], dirs: &FlureeDir) -> CliResult<LoadedCatalog> {
    use fluree_graph_ir::Term;
    let mut graph = fluree_graph_ir::Graph::new();
    let mut counts = Vec::new();
    let rdf_type = format!("{}type", fluree_db_doc::vocab::RDF_NS);
    for source in sources {
        check_class_scope(source)?;
        let opened = Opened::open(source, dirs).await?;
        let anchor = |extra: (&str, &str)| {
            let mut node = serde_json::json!({ "@id": "?s", extra.0: extra.1 });
            if let Some(c) = &source.class {
                node["@type"] = serde_json::json!(c);
            }
            node
        };
        let mut subjects = std::collections::BTreeSet::new();
        for predicate in RECORD_PREDICATES {
            let query = serde_json::json!({
                "@context": fluree_db_doc::model::query_context(),
                "where": [anchor((predicate, "?v"))],
                "select": ["?s", "?v"]
            });
            for row in opened.rows(&query).await? {
                let (Some(s), Some(v)) = (row_cell(&row, 0), row_cell(&row, 1)) else {
                    continue;
                };
                if s.starts_with("_:") || v.trim().is_empty() {
                    continue;
                }
                let s = expand_compact(&s);
                graph.add_triple(
                    Term::iri(&s),
                    Term::iri(expand_compact(predicate)),
                    Term::string(v.trim()),
                );
                subjects.insert(s);
            }
        }
        let query = serde_json::json!({
            "@context": fluree_db_doc::model::query_context(),
            "where": [anchor(("@type", "?t"))],
            "select": ["?s", "?t"]
        });
        for row in opened.rows(&query).await? {
            let (Some(s), Some(t)) = (row_cell(&row, 0), row_cell(&row, 1)) else {
                continue;
            };
            let s = expand_compact(&s);
            if subjects.contains(&s) {
                graph.add_triple(
                    Term::iri(&s),
                    Term::iri(&rdf_type),
                    Term::iri(expand_compact(&t)),
                );
            }
        }
        counts.push((source.label(), subjects.len()));
    }
    graph.sort();
    graph.dedupe();
    Ok(LoadedCatalog { graph, counts })
}

fn row_cell(row: &Value, i: usize) -> Option<String> {
    match row.get(i)? {
        Value::String(s) => Some(s.clone()),
        Value::Object(o) => o
            .get("@id")
            .or_else(|| o.get("@value"))
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

/// A compact IRI from a query row, with the prefixes the queries bind.
fn expand_compact(iri: &str) -> String {
    for (prefix, ns) in fluree_db_doc::model::PREFIXES {
        if let Some(rest) = iri.strip_prefix(&format!("{prefix}:")) {
            return format!("{ns}{rest}");
        }
    }
    if let Some(rest) = iri.strip_prefix("doc:") {
        return format!("{}{rest}", fluree_db_doc::vocab::DOC_NS);
    }
    iri.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specs_split_into_kind_and_class() {
        assert_eq!(
            Source::parse("people"),
            Source {
                kind: SourceKind::Ledger("people".into()),
                class: None
            }
        );
        assert_eq!(
            Source::parse("people:main#schema:Person"),
            Source {
                kind: SourceKind::Ledger("people:main".into()),
                class: Some("schema:Person".into())
            }
        );
        assert_eq!(
            Source::parse("./ont/model.TTL"),
            Source {
                kind: SourceKind::File("./ont/model.TTL".into()),
                class: None
            }
        );
        assert_eq!(
            Source::parse("gaz.jsonld#https://schema.org/City")
                .class
                .as_deref(),
            Some("https://schema.org/City")
        );
        assert_eq!(
            Source::parse("gaz.jsonld#").kind,
            SourceKind::File("gaz.jsonld".into())
        );
        assert_eq!(
            Source::parse("model.ttl#https://ns.flur.ee/magna#Drawing")
                .class
                .as_deref(),
            Some("https://ns.flur.ee/magna#Drawing")
        );
    }
}
