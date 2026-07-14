//! Query a **provisional** (unpersisted) R2RML mapping against live Iceberg data.
//!
//! This is the LLM agent's empirical validation lane (WP-DB3): the agent edits an
//! R2RML IR, renders it to Turtle, and asks "does `?e a edw:WebEvent LIMIT 5`
//! return rows against the real table?" — WITHOUT creating any persisted
//! graph-source or nameservice record.
//!
//! [`crate::Fluree::query_provisional_r2rml`] compiles the candidate Turtle,
//! wraps `(compiled mapping + connection)` in an [`EphemeralR2rmlProvider`], and
//! runs a normal SPARQL graph query through the same R2RML operator the persisted
//! path uses — the provider is injected in place of the nameservice-backed
//! [`FlureeR2rmlProvider`](super::r2rml::FlureeR2rmlProvider). Nothing is written.
//!
//! # Reuse / duplication
//!
//! The provider's own table scan mirrors the catalog/storage/plan/read drive of
//! the WP-DB2 sampler ([`sample_iceberg_rows`](super::iceberg_sample)): it reuses
//! the shared [`rest_catalog_client`] + [`build_preview_storage`] helpers and the
//! `SendScanPlanner` + `read_task` primitives, but streams **all** of a table's
//! data files (not a bounded first-row-group peek). It deliberately does NOT call
//! [`FlureeR2rmlProvider::scan_table`](super::r2rml) — that method resolves its
//! config from the nameservice and is the parallel session's active surface. The
//! small amount of duplicated scan-drive glue here is the low-collision choice; a
//! future WP-DB1 could consolidate the two config-driven scans behind one helper.
//!
//! # Scope
//!
//! Targeted SPARQL — the agent runs `?s a <Class> LIMIT k`, join tests, and
//! property filters. The wildcard "View Instances" crawl (which regroups a flat
//! scan into per-subject JSON-LD documents) lives in [`super::crawl`] and is left
//! to the persisted path; serving it over an ephemeral mapping would be a future
//! extension. REST catalogs only (Direct mode returns the shared
//! [`rest_catalog_client`] typed error) — mirroring the sampler's own scope.

use std::sync::Arc;

use async_trait::async_trait;

use fluree_db_iceberg::catalog::{parse_table_identifier, RestCatalogClient, SendCatalogClient};
use fluree_db_iceberg::io::{ColumnBatch, SendIcebergStorage, SendParquetReader};
use fluree_db_iceberg::metadata::TableMetadata;
use fluree_db_iceberg::scan::{ScanConfig, SendScanPlanner};
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::{ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter};
use fluree_db_r2rml::loader::R2rmlLoader;
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use futures::StreamExt;

use crate::graph_source::config::IcebergConnectionConfig;
use crate::graph_source::iceberg_catalog::{build_preview_storage, rest_catalog_client};
use crate::view::{GraphDb, QueryInput};
use crate::{QueryExecutionOptions, Result};

/// The synthetic graph-source alias an ephemeral query runs under. Any alias works
/// (the provider ignores the id and always serves the injected mapping/config); a
/// stable value keeps traces readable. `maybe_wrap_for_graph_source` uses it to
/// wrap the query in `GRAPH <provisional:main> { ... }` so patterns route to the
/// injected provider.
const PROVISIONAL_GS_ID: &str = "provisional:main";

/// An in-memory R2RML provider for one provisional query: it serves an injected
/// compiled mapping and scans the underlying Iceberg tables from an injected
/// connection config — no nameservice lookup, no persisted graph source.
///
/// Modeled on the injection precedent `MockCrawlProvider` (see [`super::crawl`]
/// tests): both traits are implemented by one struct that already holds the
/// compiled mapping. Unlike the mock, the scan here reads **live** data driven by
/// [`IcebergConnectionConfig`].
pub(crate) struct EphemeralR2rmlProvider {
    /// Iceberg connection (catalog mode + IO) the scan resolves storage from.
    conn: IcebergConnectionConfig,
    /// REST catalog client, built once for the whole query so a multi-table join
    /// does one OAuth exchange, not one per table.
    catalog: Arc<RestCatalogClient>,
    /// The candidate mapping under test, returned to the operator verbatim.
    mapping: Arc<CompiledR2rmlMapping>,
}

impl std::fmt::Debug for EphemeralR2rmlProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EphemeralR2rmlProvider")
            .field("triples_maps", &self.mapping.triples_maps.len())
            .finish_non_exhaustive()
    }
}

impl EphemeralR2rmlProvider {
    /// Scan one logical table of the provisional mapping, streaming its data-file
    /// batches. Mirrors the sampler's catalog/storage/metadata resolution, then —
    /// unlike the bounded sampler — plans and reads **every** selected file.
    ///
    /// `filters` from the operator are ignored (no pushdown): correctness is
    /// preserved by the in-engine FILTER, and the agent's targeted `LIMIT k`
    /// probes do not need file pruning. Adding pushdown later is a perf-only
    /// extension.
    async fn scan_provisional_table(
        &self,
        table_name: &str,
        projection: &[String],
    ) -> QueryResult<ColumnBatchStream> {
        // The engine passes the mapping's logical table name for every scan.
        let table_id = parse_table_identifier(table_name).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse table identifier '{table_name}': {e}"
            ))
        })?;

        // Load table metadata via the shared REST catalog client.
        let load = SendCatalogClient::load_table(
            self.catalog.as_ref(),
            &table_id,
            self.conn.io.vended_credentials,
        )
        .await
        .map_err(|e| {
            QueryError::Internal(format!(
                "Failed to load table '{table_name}' from catalog: {e}"
            ))
        })?;

        // Build S3 storage exactly as the preview/sample paths do: vended creds
        // when the catalog delegated them, else the ambient AWS chain.
        let storage = build_preview_storage(&self.conn, load.credentials.as_ref())
            .await
            .map_err(|e| {
                QueryError::Internal(format!("Failed to create storage for '{table_name}': {e}"))
            })?;

        // Prefer the catalog's inline metadata (no extra round-trip); fall back to
        // reading the metadata file from object storage when it is omitted.
        let metadata: TableMetadata = match load.metadata {
            Some(m) => m,
            None => {
                let bytes = storage.read(&load.metadata_location).await.map_err(|e| {
                    QueryError::Internal(format!(
                        "Failed to read table metadata for '{table_name}': {e}"
                    ))
                })?;
                TableMetadata::from_json(&bytes).map_err(|e| {
                    QueryError::Internal(format!(
                        "Failed to parse table metadata for '{table_name}': {e}"
                    ))
                })?
            }
        };

        let schema = metadata.current_schema().ok_or_else(|| {
            QueryError::Internal(format!("Table '{table_name}' has no current schema"))
        })?;

        // Resolve projected column names to field IDs (all non-nested columns when
        // the projection is empty, matching the persisted scan default).
        let projected_field_ids: Vec<i32> = if projection.is_empty() {
            schema
                .fields
                .iter()
                .filter(|f| !f.is_nested())
                .map(|f| f.id)
                .collect()
        } else {
            projection
                .iter()
                .filter_map(|col| schema.field_by_name(col).map(|f| f.id))
                .collect()
        };
        if projected_field_ids.is_empty() && !projection.is_empty() {
            return Err(QueryError::InvalidQuery(format!(
                "None of the projected columns {:?} exist in table '{table_name}'. Available: {:?}",
                projection,
                schema.field_names()
            )));
        }

        // Plan a projected scan over the current snapshot. Scope the planner so its
        // borrow of `storage`/`metadata` ends before `storage` is moved into the
        // streaming closure below.
        let tasks = {
            let scan_config = ScanConfig::new().with_projection(projected_field_ids);
            let planner = SendScanPlanner::new(&storage, &metadata, scan_config);
            planner
                .plan_scan()
                .await
                .map_err(|e| {
                    QueryError::Internal(format!("Failed to plan scan for '{table_name}': {e}"))
                })?
                .tasks
        };

        if tasks.is_empty() {
            return Ok(Box::pin(
                futures::stream::empty::<QueryResult<ColumnBatch>>(),
            ));
        }

        // Read data files with bounded parallelism, streaming each file's batches
        // to the operator as it completes (spawned so the stream is `Send + Sync`,
        // matching the persisted scan's shape).
        let storage = Arc::new(storage);
        let concurrency = tasks.len().clamp(1, 8);
        let stream = futures::stream::iter(tasks)
            .map(move |task| {
                let storage = Arc::clone(&storage);
                async move {
                    tokio::spawn(async move {
                        let reader = SendParquetReader::new(storage.as_ref());
                        reader.read_task(&task).await.map_err(|e| {
                            QueryError::Internal(format!(
                                "Failed to read Parquet file '{}': {e}",
                                task.data_file.file_path
                            ))
                        })
                    })
                    .await
                    .map_err(|e| QueryError::Internal(format!("Parquet read worker failed: {e}")))?
                }
            })
            .buffer_unordered(concurrency)
            .flat_map(|res: QueryResult<Vec<ColumnBatch>>| match res {
                Ok(batches) => {
                    futures::stream::iter(batches.into_iter().map(Ok).collect::<Vec<_>>())
                }
                Err(e) => futures::stream::iter(vec![Err(e)]),
            });

        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl R2rmlProvider for EphemeralR2rmlProvider {
    async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
        // The provisional mapping is always present — that is the whole point.
        true
    }

    async fn compiled_mapping(
        &self,
        _graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
        Ok(Arc::clone(&self.mapping))
    }
}

#[async_trait]
impl R2rmlTableProvider for EphemeralR2rmlProvider {
    async fn scan_table(
        &self,
        _graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        _filters: &[ScanFilter],
        _topk: Option<&fluree_db_query::r2rml::ScanTopK>,
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        self.scan_provisional_table(table_name, projection).await
    }
}

impl crate::Fluree {
    /// Run real SPARQL against a **provisional** R2RML mapping — a candidate
    /// defined entirely by `(mapping_ttl + conn)` — creating no persisted
    /// graph-source or nameservice record.
    ///
    /// This is the LLM agent's empirical validation tool: it renders a candidate
    /// mapping to Turtle and tests whether a probe query (e.g.
    /// `SELECT ?e WHERE { ?e a <…/WebEvent> } LIMIT 5`) returns rows against the
    /// live table. Compile and scan errors surface verbatim so the agent can use
    /// failures as signal.
    ///
    /// Returns the same [`QueryResult`](crate::QueryResult) shape the normal query
    /// path yields (raw batches + var registry), so the caller formats it exactly
    /// as it would a persisted graph-source query.
    ///
    /// # Errors
    ///
    /// - the Turtle fails to compile (`R2rmlLoader::from_turtle().compile()`),
    /// - the connection is Direct mode (REST is required — see [module docs](self)),
    /// - the catalog/scan fails, or the SPARQL is invalid.
    pub async fn query_provisional_r2rml(
        &self,
        conn: IcebergConnectionConfig,
        mapping_ttl: String,
        sparql: String,
    ) -> Result<crate::QueryResult> {
        // Compile the candidate mapping inline (same loader the validate/persist
        // paths use). A compile failure is the agent's first signal.
        let compiled = R2rmlLoader::from_turtle(&mapping_ttl)
            .and_then(R2rmlLoader::compile)
            .map_err(|e| {
                crate::ApiError::config(format!("Failed to compile provisional R2RML mapping: {e}"))
            })?;

        // Build the REST catalog client once (Direct mode fails fast with the
        // shared typed error). Reused across every table scan in this query.
        let (catalog, _uri, _warehouse) = rest_catalog_client(&conn, "provisional R2RML query")?;

        let provider = EphemeralR2rmlProvider {
            conn,
            catalog: Arc::new(catalog),
            mapping: Arc::new(compiled),
        };

        // A minimal genesis view tagged with the synthetic graph-source id, so
        // `maybe_wrap_for_graph_source` auto-wraps the query in
        // `GRAPH <provisional:main> { ... }` and the injected provider resolves it.
        // Mirrors `resolve_graph_source`, minus the nameservice lookup (there is no
        // record for a provisional mapping). `state` is kept in scope for the query.
        let snapshot = fluree_db_core::LedgerSnapshot::genesis(PROVISIONAL_GS_ID);
        let state =
            fluree_db_ledger::LedgerState::new(snapshot, fluree_db_novelty::Novelty::new(0));
        let mut view = GraphDb::from_ledger_state(&state);
        view.graph_source_id = Some(PROVISIONAL_GS_ID.into());

        // Run the probe through the same R2RML-aware execution path a persisted
        // graph-source query uses. The genesis view carries no reasoning, so the
        // execution context is built with reasoning inactive (fusion allowed).
        self.query_view_with_r2rml_options(
            &view,
            QueryInput::Sparql(sparql.as_str()),
            &provider,
            &provider,
            QueryExecutionOptions::default(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use fluree_db_iceberg::io::batch::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
    use fluree_db_query::error::Result as QResult;
    use fluree_db_query::r2rml::{
        ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter,
    };
    use fluree_db_r2rml::mapping::{
        CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, TriplesMap,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::view::{GraphDb, QueryInput};
    use crate::{FlureeBuilder, LedgerState, Novelty, QueryExecutionOptions};
    use fluree_db_core::LedgerSnapshot;

    /// A stub standing in for [`EphemeralR2rmlProvider`] with NO live catalog: it
    /// serves the injected compiled mapping and per-table in-memory batches, so a
    /// SPARQL probe exercises the FULL operator pipeline (rewrite → scan → bind)
    /// exactly as it would over a live scan. This is the injection precedent
    /// `MockCrawlProvider` reduced to what a targeted SPARQL query needs.
    #[derive(Debug)]
    struct StubEphemeralProvider {
        mapping: Arc<CompiledR2rmlMapping>,
        tables: HashMap<String, Vec<ColumnBatch>>,
    }

    #[async_trait]
    impl R2rmlProvider for StubEphemeralProvider {
        async fn has_r2rml_mapping(&self, _gs: &str) -> bool {
            true
        }
        async fn compiled_mapping(
            &self,
            _gs: &str,
            _as_of_t: Option<i64>,
        ) -> QResult<Arc<CompiledR2rmlMapping>> {
            Ok(Arc::clone(&self.mapping))
        }
    }

    #[async_trait]
    impl R2rmlTableProvider for StubEphemeralProvider {
        async fn scan_table(
            &self,
            _gs: &str,
            table: &str,
            _projection: &[String],
            _filters: &[ScanFilter],
            _topk: Option<&fluree_db_query::r2rml::ScanTopK>,
            _as_of_t: Option<i64>,
        ) -> QResult<ColumnBatchStream> {
            let batches = self.tables.get(table).cloned().unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(batches).map(Ok)))
        }
    }

    /// A `TriplesMap`: table + subject template + one class + one string column POM.
    fn tm(table: &str, template: &str, class: &str, pred: &str, col: &str) -> TriplesMap {
        TriplesMap::new(format!("#{table}"), table)
            .with_subject_template(template)
            .with_class(class)
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(pred),
                object_map: ObjectMap::column(col),
            })
    }

    /// One batch: an `id` (Int64) column and one nullable String column.
    fn id_str_batch(col: &str, ids: &[i64], vals: &[&str]) -> ColumnBatch {
        let schema = BatchSchema::new(vec![
            FieldInfo {
                name: "id".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 1,
            },
            FieldInfo {
                name: col.to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 2,
            },
        ]);
        ColumnBatch::new(
            Arc::new(schema),
            vec![
                Column::Int64(ids.iter().map(|i| Some(*i)).collect()),
                Column::String(vals.iter().map(|s| Some((*s).to_string())).collect()),
            ],
        )
        .unwrap()
    }

    /// A genesis graph-source view with the `example.org` namespace registered.
    /// Returns the backing ledger too so its snapshot Arc stays alive.
    fn genesis_view() -> (LedgerState, GraphDb) {
        let snapshot = LedgerSnapshot::genesis("provisional-test:main");
        let ledger = LedgerState::new(snapshot, Novelty::new(0));
        let mut view = GraphDb::from_ledger_state(&ledger);
        Arc::make_mut(&mut view.snapshot)
            .insert_namespace_code(9_999, "http://example.org/".to_string())
            .unwrap();
        view.graph_source_id = Some("provisional-test:main".into());
        (ledger, view)
    }

    /// A genesis graph-source view built EXACTLY as the real
    /// [`crate::Fluree::query_provisional_r2rml`] builds it — genesis snapshot +
    /// `graph_source_id`, with NO namespace pre-registration. Proves the real
    /// entry does not depend on the caller having registered the mapping's
    /// namespaces.
    fn genesis_view_unregistered() -> (LedgerState, GraphDb) {
        let snapshot = LedgerSnapshot::genesis("provisional-test:main");
        let ledger = LedgerState::new(snapshot, Novelty::new(0));
        let mut view = GraphDb::from_ledger_state(&ledger);
        view.graph_source_id = Some("provisional-test:main".into());
        (ledger, view)
    }

    /// Total row count across the raw result batches.
    fn row_count(result: &crate::QueryResult) -> usize {
        result.batches.iter().map(fluree_db_query::Batch::len).sum()
    }

    fn people_provider() -> StubEphemeralProvider {
        let mapping = CompiledR2rmlMapping::new(vec![tm(
            "people",
            "http://example.org/person/{id}",
            "http://example.org/Person",
            "http://example.org/name",
            "name",
        )]);
        let mut tables = HashMap::new();
        tables.insert(
            "people".to_string(),
            vec![id_str_batch("name", &[1, 2], &["Alice", "Bob"])],
        );
        StubEphemeralProvider {
            mapping: Arc::new(mapping),
            tables,
        }
    }

    // A matching `?s a <Class>` returns rows; a non-matching class returns empty.
    #[tokio::test]
    async fn provisional_sparql_matches_class_and_empty_for_nonmatch() {
        let provider = people_provider();
        let (_ledger, view) = genesis_view();
        let fluree = FlureeBuilder::memory().build_memory();

        // Matching class → the two Person subjects.
        let matching = "SELECT ?s WHERE { ?s a <http://example.org/Person> } LIMIT 5";
        let result = fluree
            .query_view_with_r2rml_options(
                &view,
                QueryInput::Sparql(matching),
                &provider,
                &provider,
                QueryExecutionOptions::new(),
            )
            .await
            .expect("provisional SPARQL query succeeds");
        assert_eq!(
            row_count(&result),
            2,
            "a matching class must bind both Person subjects"
        );

        // A class absent from the mapping resolves zero TriplesMaps → empty, not an
        // error (operator returns `Ok(None)` when no map matches).
        let nonmatch = "SELECT ?s WHERE { ?s a <http://example.org/Ghost> } LIMIT 5";
        let result = fluree
            .query_view_with_r2rml_options(
                &view,
                QueryInput::Sparql(nonmatch),
                &provider,
                &provider,
                QueryExecutionOptions::new(),
            )
            .await
            .expect("a non-matching class is an empty result, not an error");
        assert_eq!(
            row_count(&result),
            0,
            "a class not in the mapping must return no rows"
        );
    }

    // The real `query_provisional_r2rml` does NOT pre-register the mapping's
    // namespaces. Prove class matching still works without them — otherwise a
    // genuinely-matching class would silently return empty on the live path (a bug
    // only the ignored live regression would otherwise surface).
    //
    // Uses a TWO-class mapping and asserts DISCRIMINATION by distinct counts
    // (Person → 2, Order → 3, absent → 0). A single-table mapping would false-pass
    // if the class constraint were being dropped (scan-all); distinct counts + the
    // empty non-match prove the class SID→string decode (`snapshot.decode_sid` in
    // the rewrite) actually round-trips the unregistered namespace.
    #[tokio::test]
    async fn provisional_sparql_class_discriminates_without_preregistered_namespace() {
        let mapping = CompiledR2rmlMapping::new(vec![
            tm(
                "people",
                "http://example.org/person/{id}",
                "http://example.org/Person",
                "http://example.org/name",
                "name",
            ),
            tm(
                "orders",
                "http://example.org/order/{id}",
                "http://example.org/Order",
                "http://example.org/label",
                "label",
            ),
        ]);
        let mut tables = HashMap::new();
        tables.insert(
            "people".to_string(),
            vec![id_str_batch("name", &[1, 2], &["Alice", "Bob"])],
        );
        tables.insert(
            "orders".to_string(),
            vec![id_str_batch(
                "label",
                &[10, 11, 12],
                &["O-10", "O-11", "O-12"],
            )],
        );
        let provider = StubEphemeralProvider {
            mapping: Arc::new(mapping),
            tables,
        };
        let (_ledger, view) = genesis_view_unregistered();
        let fluree = FlureeBuilder::memory().build_memory();

        let count = |class: &str| {
            let q = format!("SELECT ?s WHERE {{ ?s a <{class}> }} LIMIT 10");
            let provider = &provider;
            let view = &view;
            let fluree = &fluree;
            async move {
                let result = fluree
                    .query_view_with_r2rml_options(
                        view,
                        QueryInput::Sparql(q.as_str()),
                        provider,
                        provider,
                        QueryExecutionOptions::new(),
                    )
                    .await
                    .expect("provisional SPARQL query succeeds");
                row_count(&result)
            }
        };

        assert_eq!(
            count("http://example.org/Person").await,
            2,
            "the Person class must bind ONLY the two people (namespace not pre-registered)"
        );
        assert_eq!(
            count("http://example.org/Order").await,
            3,
            "the Order class must bind ONLY the three orders — distinct count proves \
             the class constraint discriminates, it is not being dropped"
        );
        assert_eq!(
            count("http://example.org/Ghost").await,
            0,
            "a class absent from the mapping must return no rows"
        );
    }

    // A property probe binds the mapped column value for the matched subjects.
    #[tokio::test]
    async fn provisional_sparql_binds_property_column() {
        let provider = people_provider();
        let (_ledger, view) = genesis_view();
        let fluree = FlureeBuilder::memory().build_memory();

        let q = "SELECT ?s ?name WHERE { \
                 ?s a <http://example.org/Person> . \
                 ?s <http://example.org/name> ?name } LIMIT 5";
        let result = fluree
            .query_view_with_r2rml_options(
                &view,
                QueryInput::Sparql(q),
                &provider,
                &provider,
                QueryExecutionOptions::new(),
            )
            .await
            .expect("provisional property probe succeeds");
        assert_eq!(
            row_count(&result),
            2,
            "both Person subjects bind their name column"
        );
    }
}
