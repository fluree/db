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
use crate::export_annotations::AnnotationProbe;
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
    system_graphs: bool,
    raw_reifies: bool,
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
            system_graphs: false,
            raw_reifies: false,
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

    /// Export the default graph plus every user-visible named graph.
    ///
    /// Only valid with `TriG` or `NQuads` formats. The ledger's system graphs
    /// (`#txn-meta`, `#config`) are excluded — see [`export::is_system_graph`]
    /// — unless [`Self::system_graphs`] is also set.
    pub fn all_graphs(mut self) -> Self {
        self.all_graphs = true;
        self
    }

    /// Also emit the ledger's system graphs under `all_graphs()`.
    ///
    /// Diagnostic only. The resulting file is not portable: `#txn-meta` and
    /// `#config` are named for the ledger that produced them, so re-importing
    /// it into a ledger of the same name routes those triples into reserved
    /// graph ids (#1846) and into a differently-named one lands a foreign
    /// ledger's commit history in a user graph. Use `--format ledger` to move
    /// a ledger.
    pub fn system_graphs(mut self) -> Self {
        self.system_graphs = true;
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

    /// Emit edge annotations as the raw `f:reifies*` system facts, the output
    /// every release before RDF 1.2 annotation syntax produced.
    ///
    /// Kept as an escape hatch for consumers pinned to those bytes. Note that
    /// Fluree's own JSON-LD and Turtle write surfaces reject hand-written
    /// `f:reifies*` triples, so this output is re-ingestible only through the
    /// bulk-import path.
    pub fn raw_reifies(mut self) -> Self {
        self.raw_reifies = true;
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
        if self.system_graphs && !self.all_graphs {
            return Err(ApiError::Config(
                "system_graphs() selects nothing on its own; combine it with all_graphs()"
                    .to_string(),
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

    /// The named graphs an `all_graphs()` export covers.
    ///
    /// `is_system_graph` was written with the doc comment "System graph IDs
    /// excluded from dataset exports" and then never called from anywhere, so
    /// `--all-graphs` shipped emitting `#txn-meta` and `#config` from the v4
    /// baseline onward. This is the call site it was missing.
    fn selected_named_graphs<'r>(&self, registry: &'r GraphRegistry) -> Vec<(u16, &'r str)> {
        registry
            .iter_entries()
            .filter(|(g_id, _)| self.system_graphs || !export::is_system_graph(*g_id))
            .collect()
    }

    /// User-visible named graphs this export does not cover.
    ///
    /// Zero under `all_graphs()`. Under a single-graph or default-graph-only
    /// export it is every other user graph in the registry — the number the
    /// caller needs to say so out loud.
    fn omitted_named_graph_count(&self, registry: &GraphRegistry, target: Option<u16>) -> u64 {
        // Zero whenever the caller chose which graphs it wanted — either all
        // of them, or one by IRI. The count answers "your request lost data
        // you did not ask to drop", so a *targeted* export has nothing to
        // report: not returning the graphs you did not name is the feature.
        //
        // Counting them made the HTTP surface disagree with the CLI, which
        // gates the same warning on `all_graphs || graph.is_some()`. A client
        // acting on the header saw a false positive on every single-graph
        // request.
        if self.all_graphs || target.is_some() {
            return 0;
        }
        registry
            .iter_entries()
            .filter(|(g_id, _)| !export::is_system_graph(*g_id) && Some(*g_id) != target)
            .count() as u64
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

        // The writers scan through a `BinaryCursor`, so they need a store even
        // when the ledger has never been indexed. On such a ledger every
        // committed row is in the novelty overlay attached below, and the
        // cursor's overlay-only tail emits all of it once its (empty) leaf
        // range is exhausted — so an empty store, seeded with the snapshot's
        // namespace table so ID→IRI resolution can complete, exports the same
        // bytes an indexed ledger does. Building an index here instead would
        // triple the ledger's on-disk footprint as a side effect of a
        // read-shaped command.
        let binary_store: Arc<BinaryIndexStore> = match ledger
            .binary_store
            .as_ref()
            .and_then(|te| te.0.clone().downcast::<BinaryIndexStore>().ok())
        {
            Some(store) => store,
            None => {
                let mut store = BinaryIndexStore::empty(self.fluree.binary_store_cache_dir());
                store
                    .augment_namespace_codes(&ledger.snapshot.shared_namespaces())
                    .map_err(io_err)?;
                store.set_ns_split_mode(ledger.snapshot.ns_split_mode());
                Arc::new(store)
            }
        };

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

        // Forward annotation lookup, chosen once for the whole export. `None`
        // on a ledger that has never carried an annotation — and on
        // `raw_reifies()`, which keeps the pre-RDF-1.2 output byte for byte.
        let annotations = if self.raw_reifies {
            None
        } else {
            AnnotationProbe::for_ledger(&ledger, to_t).await?
        };
        // `EdgeKey.g` for a graph being scanned. Computed per graph rather
        // than per row, and not at all when nothing will probe it.
        let graph_sid_of = |g_id: u16| -> Option<fluree_db_core::Sid> {
            if annotations.is_none() || g_id == 0 {
                return None;
            }
            ledger
                .snapshot
                .graph_registry
                .iri_for_graph_id(g_id)
                .map(|iri| {
                    // Resolve the way `ExportResolver::resolve_subject_sid`
                    // does: the stored Sid first, a fresh encode only as a
                    // fallback. `encode_iri` returns the split form whenever
                    // the namespace prefix is registered, while storage may
                    // hold the full-IRI form for that same IRI — and a Sid
                    // differing in any position lands on the wrong arena span
                    // and reports "no annotations" rather than failing.
                    binary_store
                        .find_subject_sid(iri)
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| binary_store.encode_iri(iri))
                })
        };

        let mut total_stats = ExportStats::default();

        match self.format {
            ExportFormat::Turtle => {
                let prefixes = self.resolve_prefixes().await?;
                prefixes.write_declarations(writer).map_err(io_err)?;

                let config = ExportConfig {
                    g_id: target_graph.as_ref().map_or(0, |(g_id, _)| *g_id),
                    graph_iri: None,
                    to_t,
                    overlay: Some(overlay),
                    dict_novelty: Some(dict_novelty),
                    annotations: annotations.as_ref(),
                    graph_sid: graph_sid_of(target_graph.as_ref().map_or(0, |(g_id, _)| *g_id)),
                };
                let stats = export::export_graph_turtle(&binary_store, &config, &prefixes, writer)
                    .await
                    .map_err(io_err)?;
                accumulate(&mut total_stats, stats);
            }

            ExportFormat::NTriples => {
                let config = ExportConfig {
                    g_id: target_graph.as_ref().map_or(0, |(g_id, _)| *g_id),
                    graph_iri: None,
                    to_t,
                    overlay: Some(overlay),
                    dict_novelty: Some(dict_novelty),
                    annotations: annotations.as_ref(),
                    graph_sid: graph_sid_of(target_graph.as_ref().map_or(0, |(g_id, _)| *g_id)),
                };
                let stats = export::export_graph_ntriples(&binary_store, &config, writer)
                    .await
                    .map_err(io_err)?;
                accumulate(&mut total_stats, stats);
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
                        annotations: annotations.as_ref(),
                        graph_sid: graph_sid_of(*g_id),
                    };
                    let stats = export::export_graph_ntriples(&binary_store, &config, writer)
                        .await
                        .map_err(io_err)?;
                    accumulate(&mut total_stats, stats);
                } else {
                    // Default graph (no graph term)
                    let config = ExportConfig {
                        g_id: 0,
                        graph_iri: None,
                        to_t,
                        overlay: Some(overlay),
                        dict_novelty: Some(dict_novelty),
                        annotations: annotations.as_ref(),
                        graph_sid: graph_sid_of(0),
                    };
                    let stats = export::export_graph_ntriples(&binary_store, &config, writer)
                        .await
                        .map_err(io_err)?;
                    accumulate(&mut total_stats, stats);

                    if self.all_graphs {
                        for (g_id, iri) in
                            self.selected_named_graphs(&ledger.snapshot.graph_registry)
                        {
                            let config = ExportConfig {
                                g_id,
                                graph_iri: Some(iri.to_string()),
                                to_t,
                                overlay: Some(overlay),
                                dict_novelty: Some(dict_novelty),
                                annotations: annotations.as_ref(),
                                graph_sid: graph_sid_of(g_id),
                            };
                            let stats =
                                export::export_graph_ntriples(&binary_store, &config, writer)
                                    .await
                                    .map_err(io_err)?;
                            accumulate(&mut total_stats, stats);
                        }
                    }
                }
            }

            ExportFormat::TriG => {
                let prefixes = self.resolve_prefixes().await?;
                prefixes.write_declarations(writer).map_err(io_err)?;

                if let Some((g_id, iri)) = &target_graph {
                    // Single named graph in GRAPH { } block
                    write!(writer, "GRAPH ").map_err(io_err)?;
                    prefixes.write_iri(writer, iri).map_err(io_err)?;
                    writeln!(writer, " {{").map_err(io_err)?;

                    let config = ExportConfig {
                        g_id: *g_id,
                        graph_iri: None,
                        to_t,
                        overlay: Some(overlay),
                        dict_novelty: Some(dict_novelty),
                        annotations: annotations.as_ref(),
                        graph_sid: graph_sid_of(*g_id),
                    };
                    let stats =
                        export::export_graph_turtle(&binary_store, &config, &prefixes, writer)
                            .await
                            .map_err(io_err)?;
                    accumulate(&mut total_stats, stats);

                    writeln!(writer, "}}").map_err(io_err)?;
                } else {
                    // Default graph as top-level triples
                    let config = ExportConfig {
                        g_id: 0,
                        graph_iri: None,
                        to_t,
                        overlay: Some(overlay),
                        dict_novelty: Some(dict_novelty),
                        annotations: annotations.as_ref(),
                        graph_sid: graph_sid_of(0),
                    };
                    let stats =
                        export::export_graph_turtle(&binary_store, &config, &prefixes, writer)
                            .await
                            .map_err(io_err)?;
                    accumulate(&mut total_stats, stats);

                    // Named graphs in GRAPH { } blocks
                    if self.all_graphs {
                        for (g_id, iri) in
                            self.selected_named_graphs(&ledger.snapshot.graph_registry)
                        {
                            write!(writer, "\nGRAPH ").map_err(io_err)?;
                            prefixes.write_iri(writer, iri).map_err(io_err)?;
                            writeln!(writer, " {{").map_err(io_err)?;

                            let config = ExportConfig {
                                g_id,
                                graph_iri: None,
                                to_t,
                                overlay: Some(overlay),
                                dict_novelty: Some(dict_novelty),
                                annotations: annotations.as_ref(),
                                graph_sid: graph_sid_of(g_id),
                            };
                            let stats = export::export_graph_turtle(
                                &binary_store,
                                &config,
                                &prefixes,
                                writer,
                            )
                            .await
                            .map_err(io_err)?;
                            accumulate(&mut total_stats, stats);

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
                    annotations: annotations.as_ref(),
                    graph_sid: graph_sid_of(target_graph.as_ref().map_or(0, |(g_id, _)| *g_id)),
                };
                let stats = export::export_graph_jsonld(&binary_store, &config, &prefixes, writer)
                    .await
                    .map_err(io_err)?;
                accumulate(&mut total_stats, stats);

                export::write_jsonld_footer(writer).map_err(io_err)?;
            }
        }

        writer.flush().map_err(io_err)?;
        if let Some(probe) = annotations.as_ref() {
            total_stats.annotations_out_of_scope = probe.out_of_scope_count();
            total_stats.annotations_unresolved = probe.unresolved_count();
        }
        total_stats.named_graphs_omitted = self.omitted_named_graph_count(
            &ledger.snapshot.graph_registry,
            target_graph.as_ref().map(|(g_id, _)| *g_id),
        );
        Ok(total_stats)
    }

    /// Convenience: execute the export writing to stdout.
    pub async fn to_stdout(self) -> Result<ExportStats> {
        let stdout = io::stdout().lock();
        let mut writer = BufWriter::new(stdout);
        self.write_to(&mut writer).await
    }
}

/// Fold one graph's scan into the running totals.
///
/// A graph counts toward `graphs_written` only if it produced a triple: an
/// empty `GRAPH { }` block is a graph in the registry, not a graph of data,
/// and reporting it would overstate what the export carries.
fn accumulate(total: &mut ExportStats, stats: ExportStats) {
    if stats.triples_written > 0 {
        total.graphs_written += 1;
    }
    total.triples_written += stats.triples_written;
    total.rows_skipped += stats.rows_skipped;
}

fn io_err(e: io::Error) -> ApiError {
    ApiError::internal(format!("I/O error during export: {e}"))
}
