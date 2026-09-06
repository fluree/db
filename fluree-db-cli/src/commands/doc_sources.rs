//! Where `--model` and `--entities` come from.
//!
//! A source is a ledger in the local store or an RDF file. A file is loaded
//! into a scratch in-memory ledger, so every source is queried the same
//! way with the database's own parsers and no second RDF stack. Ledger
//! names and file paths are told apart by extension: `.ttl`, `.nt`,
//! `.jsonld` and `.json` are files.

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
}

impl Opened {
    pub async fn open(source: &Source, dirs: &FlureeDir) -> CliResult<Self> {
        match &source.kind {
            SourceKind::Ledger(alias) => {
                let fluree = context::build_fluree(dirs)?;
                let ledger_id = context::to_ledger_id(alias);
                if !fluree.ledger_exists(&ledger_id).await? {
                    return Err(CliError::NotFound(format!(
                        "{alias}: no such ledger (a file source needs a .ttl, .nt, .jsonld or .json extension)"
                    )));
                }
                Ok(Self {
                    fluree,
                    alias: alias.clone(),
                })
            }
            SourceKind::File(path) => {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| CliError::Input(format!("{}: {e}", path.display())))?;
                let fluree = FlureeBuilder::memory().build_memory();
                let alias = "scratch:main".to_string();
                fluree.create_ledger(&context::to_ledger_id(&alias)).await?;
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
                Ok(Self { fluree, alias })
            }
        }
    }

    pub async fn rows(&self, query: &Value) -> CliResult<Vec<Value>> {
        let view = self.fluree.db_with_default_context(&self.alias).await?;
        let result = self.fluree.query(&view, query).await?;
        let json = result.to_jsonld(&view.snapshot)?;
        Ok(json.as_array().cloned().unwrap_or_default())
    }
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
            "--model {}: no classes or properties found (rdfs:Class, owl:Class, rdf:Property, owl:ObjectProperty, owl:DatatypeProperty, or anything with rdfs:subClassOf / rdfs:domain / schema:domainIncludes)",
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
