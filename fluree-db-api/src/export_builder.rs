//! Builder for streaming RDF export operations.
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_api::export::{ExportFormat, PrefixMap};
//!
//! let stats = fluree.export("mydb")
//!     .format(ExportFormat::Turtle)
//!     .context(&json!({"ex": "http://example.org/"}))
//!     .write_to(&mut writer)
//!     .await?;
//! ```

use crate::export::{self, ExportConfig, ExportFormat, ExportStats, PrefixMap};
use crate::{time_resolve, ApiError, Fluree, Result, TimeSpec};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphRegistry;
use std::io::{self, BufWriter, Write};
use std::sync::Arc;

/// Builder for configuring and executing a streaming export.
pub struct ExportBuilder<'a> {
    fluree: &'a Fluree,
    ledger_id: String,
    format: ExportFormat,
    all_graphs: bool,
    graph_iri: Option<String>,
    context_override: Option<serde_json::Value>,
    time_spec: Option<TimeSpec>,
}

impl<'a> ExportBuilder<'a> {
    pub(crate) fn new(fluree: &'a Fluree, ledger_id: String) -> Self {
        Self {
            fluree,
            ledger_id,
            format: ExportFormat::Turtle,
            all_graphs: false,
            graph_iri: None,
            context_override: None,
            time_spec: None,
        }
    }

    /// Set the output format (default: `Turtle`).
    pub fn format(mut self, format: ExportFormat) -> Self {
        self.format = format;
        self
    }

    /// Export all named graphs (dataset export), including system graphs.
    ///
    /// Only valid with `TriG` or `NQuads` formats.
    pub fn all_graphs(mut self) -> Self {
        self.all_graphs = true;
        self
    }

    /// Export a specific named graph by IRI.
    ///
    /// The IRI must match a graph registered in the ledger's graph registry.
    /// Mutually exclusive with `all_graphs()`.
    pub fn graph(mut self, iri: &str) -> Self {
        self.graph_iri = Some(iri.to_string());
        self
    }

    /// Override the prefix map with a JSON-LD context object.
    ///
    /// Expects `{"prefix": "iri", ...}`. If not set, the ledger's
    /// default context (from the nameservice) is used for Turtle/TriG.
    pub fn context(mut self, ctx: &serde_json::Value) -> Self {
        self.context_override = Some(ctx.clone());
        self
    }

    /// Export data as of a specific point in time.
    ///
    /// Accepts any [`TimeSpec`]: transaction number, ISO-8601 datetime,
    /// or commit CID prefix. If not set, exports at the latest committed
    /// time (including committed-but-not-yet-indexed data in novelty).
    pub fn as_of(mut self, spec: TimeSpec) -> Self {
        self.time_spec = Some(spec);
        self
    }

    /// Validate the builder configuration.
    fn validate(&self) -> Result<()> {
        if self.all_graphs && self.graph_iri.is_some() {
            return Err(ApiError::Config(
                "cannot use both all_graphs() and graph() — choose one".to_string(),
            ));
        }
        if self.all_graphs {
            match self.format {
                ExportFormat::TriG | ExportFormat::NQuads => {}
                ExportFormat::NTriples | ExportFormat::Turtle => {
                    return Err(ApiError::Config(
                        "cannot export all graphs as Turtle/N-Triples (graph boundaries would be lost); \
                         use TriG or NQuads format"
                            .to_string(),
                    ));
                }
                ExportFormat::JsonLd => {
                    return Err(ApiError::Config(
                        "exporting all graphs as JSON-LD is not yet supported; use TriG or NQuads"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Resolve the prefix map for Turtle/TriG/JSON-LD output.
    async fn resolve_prefixes(&self) -> Result<PrefixMap> {
        if let Some(ctx) = &self.context_override {
            return Ok(PrefixMap::from_context(ctx));
        }

        // Try to load the default context from the nameservice
        match self.fluree.get_default_context(&self.ledger_id).await {
            Ok(Some(ctx)) => Ok(PrefixMap::from_context(&ctx)),
            Ok(None) => Ok(PrefixMap::from_context(&serde_json::Value::Null)),
            Err(_) => Ok(PrefixMap::from_context(&serde_json::Value::Null)),
        }
    }

    /// Resolve a graph IRI to a `(g_id, iri)` pair via the graph registry.
    fn resolve_graph_iri(&self, registry: &GraphRegistry) -> Result<(u16, String)> {
        let iri = self.graph_iri.as_deref().unwrap();
        match registry.graph_id_for_iri(iri) {
            Some(g_id) => Ok((g_id, iri.to_string())),
            None => Err(ApiError::Config(format!(
                "graph '{iri}' not found in ledger graph registry"
            ))),
        }
    }

    /// Execute the export, writing to the provided `Write` sink.
    ///
    /// Returns export statistics (triples written, rows skipped).
    pub async fn write_to<W: Write>(self, writer: &mut W) -> Result<ExportStats> {
        self.validate()?;

        let ledger = self.fluree.ledger(&self.ledger_id).await?;

        // All formats require binary index
        let binary_store: Arc<BinaryIndexStore> = ledger
            .binary_store
            .as_ref()
            .and_then(|te| te.0.clone().downcast::<BinaryIndexStore>().ok())
            .ok_or_else(|| {
                ApiError::Config(
                    "no binary index available for export (is the ledger indexed?)".to_string(),
                )
            })?;

        // Resolve the target graph if a specific graph was requested
        let target_graph = if self.graph_iri.is_some() {
            Some(self.resolve_graph_iri(&ledger.snapshot.graph_registry)?)
        } else {
            None
        };

        // Resolve time-travel bound: explicit TimeSpec, or current ledger time
        let to_t = match &self.time_spec {
            Some(spec) => time_resolve::resolve_time_spec(&ledger, spec).await?,
            None => ledger.t(),
        };

        // Novelty overlay — always include so export sees committed-but-not-yet-indexed data
        let overlay: &dyn fluree_db_core::OverlayProvider = ledger.novelty.as_ref();
        let dict_novelty = &ledger.dict_novelty;

        let mut total_stats = ExportStats::default();

        match self.format {
            ExportFormat::Turtle => {
                let prefixes = self.resolve_prefixes().await?;
                export::write_prefix_declarations(&prefixes, writer).map_err(io_err)?;

                let config = ExportConfig {
                    g_id: target_graph.as_ref().map_or(0, |(g_id, _)| *g_id),
                    graph_iri: None,
                    to_t,
                    overlay: Some(overlay),
                    dict_novelty: Some(dict_novelty),
                };
                let stats = export::export_graph_turtle(&binary_store, &config, &prefixes, writer)
                    .map_err(io_err)?;
                total_stats.triples_written += stats.triples_written;
                total_stats.rows_skipped += stats.rows_skipped;
            }

            ExportFormat::NTriples => {
                let config = ExportConfig {
                    g_id: target_graph.as_ref().map_or(0, |(g_id, _)| *g_id),
                    graph_iri: None,
                    to_t,
                    overlay: Some(overlay),
                    dict_novelty: Some(dict_novelty),
                };
                let stats = export::export_graph_ntriples(&binary_store, &config, writer)
                    .map_err(io_err)?;
                total_stats.triples_written += stats.triples_written;
                total_stats.rows_skipped += stats.rows_skipped;
            }

            ExportFormat::NQuads => {
                if let Some((g_id, iri)) = &target_graph {
                    // Single named graph
                    let config = ExportConfig {
                        g_id: *g_id,
                        graph_iri: Some(iri.clone()),
                        to_t,
                        overlay: Some(overlay),
                        dict_novelty: Some(dict_novelty),
                    };
                    let stats = export::export_graph_ntriples(&binary_store, &config, writer)
                        .map_err(io_err)?;
                    total_stats.triples_written += stats.triples_written;
                    total_stats.rows_skipped += stats.rows_skipped;
                } else {
                    // Default graph (no graph term)
                    let config = ExportConfig {
                        g_id: 0,
                        graph_iri: None,
                        to_t,
                        overlay: Some(overlay),
                        dict_novelty: Some(dict_novelty),
                    };
                    let stats = export::export_graph_ntriples(&binary_store, &config, writer)
                        .map_err(io_err)?;
                    total_stats.triples_written += stats.triples_written;
                    total_stats.rows_skipped += stats.rows_skipped;

                    if self.all_graphs {
                        for (g_id, iri) in ledger.snapshot.graph_registry.iter_entries() {
                            let config = ExportConfig {
                                g_id,
                                graph_iri: Some(iri.to_string()),
                                to_t,
                                overlay: Some(overlay),
                                dict_novelty: Some(dict_novelty),
                            };
                            let stats =
                                export::export_graph_ntriples(&binary_store, &config, writer)
                                    .map_err(io_err)?;
                            total_stats.triples_written += stats.triples_written;
                            total_stats.rows_skipped += stats.rows_skipped;
                        }
                    }
                }
            }

            ExportFormat::TriG => {
                let prefixes = self.resolve_prefixes().await?;
                export::write_prefix_declarations(&prefixes, writer).map_err(io_err)?;

                if let Some((g_id, iri)) = &target_graph {
                    // Single named graph in GRAPH { } block
                    write!(writer, "GRAPH ").map_err(io_err)?;
                    export::write_turtle_iri(writer, iri, &prefixes).map_err(io_err)?;
                    writeln!(writer, " {{").map_err(io_err)?;

                    let config = ExportConfig {
                        g_id: *g_id,
                        graph_iri: None,
                        to_t,
                        overlay: Some(overlay),
                        dict_novelty: Some(dict_novelty),
                    };
                    let stats =
                        export::export_graph_turtle(&binary_store, &config, &prefixes, writer)
                            .map_err(io_err)?;
                    total_stats.triples_written += stats.triples_written;
                    total_stats.rows_skipped += stats.rows_skipped;

                    writeln!(writer, "}}").map_err(io_err)?;
                } else {
                    // Default graph as top-level triples
                    let config = ExportConfig {
                        g_id: 0,
                        graph_iri: None,
                        to_t,
                        overlay: Some(overlay),
                        dict_novelty: Some(dict_novelty),
                    };
                    let stats =
                        export::export_graph_turtle(&binary_store, &config, &prefixes, writer)
                            .map_err(io_err)?;
                    total_stats.triples_written += stats.triples_written;
                    total_stats.rows_skipped += stats.rows_skipped;

                    // Named graphs in GRAPH { } blocks
                    if self.all_graphs {
                        for (g_id, iri) in ledger.snapshot.graph_registry.iter_entries() {
                            write!(writer, "\nGRAPH ").map_err(io_err)?;
                            export::write_turtle_iri(writer, iri, &prefixes).map_err(io_err)?;
                            writeln!(writer, " {{").map_err(io_err)?;

                            let config = ExportConfig {
                                g_id,
                                graph_iri: None,
                                to_t,
                                overlay: Some(overlay),
                                dict_novelty: Some(dict_novelty),
                            };
                            let stats = export::export_graph_turtle(
                                &binary_store,
                                &config,
                                &prefixes,
                                writer,
                            )
                            .map_err(io_err)?;
                            total_stats.triples_written += stats.triples_written;
                            total_stats.rows_skipped += stats.rows_skipped;

                            writeln!(writer, "}}").map_err(io_err)?;
                        }
                    }
                }
            }

            ExportFormat::JsonLd => {
                let prefixes = self.resolve_prefixes().await?;
                export::write_jsonld_header(&prefixes, writer).map_err(io_err)?;

                let config = ExportConfig {
                    g_id: target_graph.as_ref().map_or(0, |(g_id, _)| *g_id),
                    graph_iri: None,
                    to_t,
                    overlay: Some(overlay),
                    dict_novelty: Some(dict_novelty),
                };
                let stats = export::export_graph_jsonld(&binary_store, &config, &prefixes, writer)
                    .map_err(io_err)?;
                total_stats.triples_written += stats.triples_written;
                total_stats.rows_skipped += stats.rows_skipped;

                export::write_jsonld_footer(writer).map_err(io_err)?;
            }
        }

        writer.flush().map_err(io_err)?;
        Ok(total_stats)
    }

    /// Convenience: execute the export writing to stdout.
    pub async fn to_stdout(self) -> Result<ExportStats> {
        let stdout = io::stdout().lock();
        let mut writer = BufWriter::new(stdout);
        self.write_to(&mut writer).await
    }
}

fn io_err(e: io::Error) -> ApiError {
    ApiError::internal(format!("I/O error during export: {e}"))
}
