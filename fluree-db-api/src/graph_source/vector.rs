//! Embedded vector similarity search index operations.
//!
//! This module provides APIs for creating, loading, syncing, and dropping
//! embedded vector similarity search indexes.

#[cfg(feature = "vector")]
use crate::graph_source::config::VectorCreateConfig;
#[cfg(feature = "vector")]
use crate::graph_source::helpers::{
    expand_ids_in_results, expand_prefixed_iri, expand_properties_in_results, extract_prefix_map,
};
#[cfg(feature = "vector")]
use crate::graph_source::result::{
    VectorCreateResult, VectorDropResult, VectorStalenessCheck, VectorSyncResult,
};
#[cfg(feature = "vector")]
use crate::Result;
#[cfg(feature = "vector")]
use fluree_db_core::{ledger_id::split_ledger_id, ContentId, ContentStore};
#[cfg(feature = "vector")]
use fluree_db_ledger::LedgerState;
#[cfg(feature = "vector")]
use fluree_db_nameservice::GraphSourceType;
#[cfg(feature = "vector")]
use fluree_db_query::parse::parse_query;
#[cfg(feature = "vector")]
use fluree_db_query::vector::usearch::{
    IncrementalVectorUpdater, VectorIndex, VectorIndexBuilder, VectorPropertyDeps,
};
#[cfg(feature = "vector")]
use fluree_db_query::{execute, ContextConfig, ExecutableQuery, QueryOutput, VarRegistry};
#[cfg(feature = "vector")]
use serde_json::Value as JsonValue;
#[cfg(feature = "vector")]
use std::collections::HashSet;
#[cfg(feature = "vector")]
use std::sync::Arc;
#[cfg(feature = "vector")]
use tracing::{info, warn};

// =============================================================================
// Vector Index Creation
// =============================================================================

#[cfg(feature = "vector")]
impl crate::Fluree {
    /// Create a vector similarity search index.
    ///
    /// This operation:
    /// 1. Loads the source ledger
    /// 2. Executes the indexing query to get documents
    /// 3. Extracts embedding vectors from each document
    /// 4. Builds the vector index using usearch HNSW
    /// 5. Persists the index snapshot to storage
    /// 6. Publishes the graph source record to the nameservice
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying the index name, source ledger, query, and embedding property
    ///
    /// # Returns
    ///
    /// Result containing the created index metadata
    ///
    /// # Example
    ///
    /// ```ignore
    /// use fluree_db_api::VectorCreateConfig;
    /// use fluree_db_query::vector::DistanceMetric;
    ///
    /// let config = VectorCreateConfig::new(
    ///     "embeddings",
    ///     "docs:main",
    ///     json!({
    ///         "where": [{"@id": "?x", "@type": "Article"}],
    ///         "select": {"?x": ["@id", "embedding"]}
    ///     }),
    ///     "embedding",
    ///     768,
    /// ).with_metric(DistanceMetric::Cosine);
    ///
    /// let result = fluree.create_vector_index(config).await?;
    /// ```
    pub async fn create_vector_index(
        &self,
        config: VectorCreateConfig,
    ) -> Result<VectorCreateResult> {
        let graph_source_id = config.graph_source_id();
        info!(
            graph_source_id = %graph_source_id,
            ledger = %config.ledger,
            dimensions = config.dimensions,
            "Creating vector similarity search index"
        );

        // Check if graph source already exists (prevent duplicates)
        if let Some(existing) = self
            .nameservice()
            .lookup_graph_source(&graph_source_id)
            .await?
        {
            if !existing.retracted {
                return Err(crate::ApiError::Config(format!(
                    "Graph source '{graph_source_id}' already exists"
                )));
            }
        }

        // 1. Load source ledger
        let ledger = self.ledger(&config.ledger).await?;
        let source_t = ledger.t();

        info!(
            ledger = %config.ledger,
            t = source_t,
            "Loaded source ledger"
        );

        // 2. Execute indexing query
        let results = self
            .execute_vector_indexing_query(&ledger, &config.query)
            .await?;

        info!(result_count = results.len(), "Executed indexing query");

        // 2b. Expand prefixed IRIs in @id fields and property names to full IRIs
        let context = config
            .query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);
        let results = expand_properties_in_results(results, &prefix_map);

        // 2c. Expand embedding property name if prefixed
        let embedding_property = expand_prefixed_iri(&config.embedding_property, &prefix_map)
            .unwrap_or_else(|| config.embedding_property.clone());

        // 3. Build vector index
        let property_deps = VectorPropertyDeps::from_query(&embedding_property, &config.query);
        let metric = config
            .metric
            .unwrap_or(fluree_db_query::vector::DistanceMetric::Cosine);

        let mut builder =
            VectorIndexBuilder::new(config.ledger.as_str(), config.dimensions, metric)?
                .with_embedding_property(&embedding_property)
                .with_property_deps(property_deps)
                .with_watermark(source_t);

        // Reserve capacity if we know the result count
        if !results.is_empty() {
            builder = builder.with_capacity(results.len())?;
        }

        for result in &results {
            builder.add_result(result)?;
        }

        let vector_count = builder.indexed_count();
        let skipped_count = builder.skipped_count();
        let index = builder.build();

        info!(
            vector_count = vector_count,
            skipped_count = skipped_count,
            dimensions = config.dimensions,
            "Built vector index"
        );

        // 4. Persist index snapshot
        let index_id = self
            .write_vector_snapshot_blob(&graph_source_id, &index, source_t)
            .await?;

        info!(
            index_id = %index_id,
            index_t = source_t,
            "Persisted versioned index snapshot"
        );

        // 5. Publish graph source record to nameservice
        let config_json = serde_json::to_string(&serde_json::json!({
            "embedding_property": config.embedding_property,
            "dimensions": config.dimensions,
            "metric": format!("{:?}", metric),
            "query": config.query,
            "usearch": {
                "connectivity": config.connectivity,
                "expansion_add": config.expansion_add,
                "expansion_search": config.expansion_search,
            }
        }))?;

        self.publisher()?
            .publish_graph_source(
                &config.name,
                config.effective_branch(),
                GraphSourceType::Vector,
                &config_json,
                std::slice::from_ref(&config.ledger),
            )
            .await?;

        // Publish index CID and watermark
        self.publisher()?
            .publish_graph_source_index(
                &config.name,
                config.effective_branch(),
                &index_id,
                source_t,
            )
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            vector_count = vector_count,
            index_t = source_t,
            "Created vector similarity search index"
        );

        Ok(VectorCreateResult {
            graph_source_id,
            vector_count,
            skipped_count,
            dimensions: config.dimensions,
            index_t: source_t,
            index_id: Some(index_id),
        })
    }

    /// Execute the indexing query and return JSON-LD results.
    ///
    /// Executes the query and formats results as JSON-LD objects suitable for indexing.
    /// Each result object will have an `@id` field identifying the document.
    pub(crate) async fn execute_vector_indexing_query(
        &self,
        ledger: &LedgerState,
        query_json: &JsonValue,
    ) -> Result<Vec<JsonValue>> {
        // Parse the query
        let mut vars = VarRegistry::new();
        let parsed = parse_query(query_json, &ledger.snapshot, &mut vars, None)?;

        // Execute with a wildcard select so the operator pipeline does not project away
        // bindings we need for indexing (Wildcard naturally drops any hydration).
        let mut parsed_for_exec = parsed.clone();
        parsed_for_exec.output = QueryOutput::wildcard();

        let executable = ExecutableQuery::simple(parsed_for_exec);

        let db = ledger.as_graph_db_ref(0);
        let batches = execute(db, &vars, &executable, ContextConfig::default()).await?;

        // Format using the standard JSON-LD formatter
        let result = crate::query::helpers::build_query_result(
            vars,
            parsed,
            batches,
            Some(ledger.t()),
            Some(ledger.novelty.clone()),
            None,
        );

        let json = result.to_jsonld_async(ledger.as_graph_db_ref(0)).await?;
        match json {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(_) => Ok(vec![json]),
            _ => Ok(Vec::new()),
        }
    }

    /// Write a vector index snapshot blob to storage.
    ///
    /// Creates a snapshot at `graph-sources/{name}/{branch}/vector/t{index_t}/snapshot.bin`.
    /// Returns the storage address. Caller is responsible for publishing
    /// the head pointer via nameservice.
    /// Returns (storage_address, ContentId) tuple.
    pub(crate) async fn write_vector_snapshot_blob(
        &self,
        graph_source_id: &str,
        index: &VectorIndex,
        _index_t: i64,
    ) -> Result<ContentId> {
        use fluree_db_query::vector::usearch::serialize;

        // Serialize the index
        let bytes = serialize(index)?;

        // Write through the content store so it's stored at the CID-mapped address
        let cs = self.content_store(graph_source_id);
        let index_id = cs
            .put(fluree_db_core::ContentKind::IndexRoot, &bytes)
            .await?;

        Ok(index_id)
    }
}

// =============================================================================
// Vector Index Loading (for queries)
// =============================================================================

#[cfg(feature = "vector")]
impl crate::Fluree {
    /// Load a vector index from storage (head snapshot).
    ///
    /// Vector indexes are head-only and do not support time-travel queries.
    pub async fn load_vector_index(&self, graph_source_id: &str) -> Result<Arc<VectorIndex>> {
        use fluree_db_query::vector::usearch::deserialize;

        // Look up graph source record
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        // Get index CID
        let index_cid = record.index_id.ok_or_else(|| {
            crate::ApiError::NotFound(format!("No index for graph source: {graph_source_id}"))
        })?;

        // Load from content store
        let store = self.content_store(graph_source_id);
        let bytes = store.get(&index_cid).await?;

        // Deserialize
        let index = deserialize(&bytes)?;

        Ok(Arc::new(index))
    }

    /// Check if a vector index is stale relative to its source ledger.
    ///
    /// This is a lightweight check that only looks up nameservice records.
    pub async fn check_vector_staleness(
        &self,
        graph_source_id: &str,
    ) -> Result<VectorStalenessCheck> {
        // Look up graph source record
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        // Get source ledger from dependencies
        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // Check minimum head across all dependencies
        let mut ledger_t: Option<i64> = None;
        for dep in &record.dependencies {
            let ledger_record = self.nameservice().lookup(dep).await?.ok_or_else(|| {
                crate::ApiError::NotFound(format!("Source ledger not found: {dep}"))
            })?;
            ledger_t = Some(match ledger_t {
                Some(cur) => cur.min(ledger_record.commit_t),
                None => ledger_record.commit_t,
            });
        }
        let ledger_t = ledger_t.unwrap_or(0);

        let index_t = record.index_t;
        let is_stale = index_t < ledger_t;
        let lag = ledger_t - index_t;

        Ok(VectorStalenessCheck {
            graph_source_id: graph_source_id.to_string(),
            source_ledger,
            index_t,
            ledger_t,
            is_stale,
            lag,
        })
    }
}

// =============================================================================
// Vector Index Sync (Maintenance)
// =============================================================================

#[cfg(feature = "vector")]
impl crate::Fluree {
    /// Sync a vector index to catch up with ledger updates.
    ///
    /// This operation performs incremental updates when possible,
    /// falling back to full resync if needed.
    pub async fn sync_vector_index(&self, graph_source_id: &str) -> Result<VectorSyncResult> {
        use fluree_db_core::trace_commits_by_id;
        use fluree_db_query::bm25::CompiledPropertyDeps;
        use fluree_db_query::vector::usearch::deserialize;
        use futures::StreamExt;

        info!(graph_source_id = %graph_source_id, "Starting vector index sync");

        // 1. Look up graph source record to get config and index address
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        // Check if graph source has been dropped
        if record.retracted {
            return Err(crate::ApiError::Drop(format!(
                "Cannot sync retracted graph source: {graph_source_id}"
            )));
        }

        let index_cid = match &record.index_id {
            Some(cid) => cid.clone(),
            None => {
                // No index yet - need full resync
                return self.resync_vector_index(graph_source_id).await;
            }
        };

        // Parse config to get query
        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));

        // Get source ledger alias from dependencies
        let source_ledger_alias = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // 2. Load source ledger to get current state
        let ledger = self.ledger(&source_ledger_alias).await?;
        let ledger_t = ledger.t();

        // 3. Load existing index by CID
        let cs = self.content_store(graph_source_id);
        let bytes = cs.get(&index_cid).await?;
        let mut index = deserialize(&bytes)?;
        let old_watermark = index.watermark.get(&source_ledger_alias).unwrap_or(0);

        // Already up to date?
        if ledger_t <= old_watermark {
            info!(graph_source_id = %graph_source_id, ledger_t = ledger_t, "Index already up to date");
            return Ok(VectorSyncResult {
                graph_source_id: graph_source_id.to_string(),
                upserted: 0,
                removed: 0,
                skipped: 0,
                old_watermark,
                new_watermark: old_watermark,
                was_full_resync: false,
            });
        }

        // 4. Get head commit CID for tracing
        let head_commit_id = ledger
            .ns_record
            .as_ref()
            .and_then(|r| r.commit_head_id.clone())
            .ok_or_else(|| crate::ApiError::NotFound("No commit head for ledger".to_string()))?;

        // 5. Compile property deps for this ledger's namespace
        // Convert VectorPropertyDeps to PropertyDeps for compilation
        let bm25_property_deps = index.property_deps.query_deps.clone();
        let compiled_deps = CompiledPropertyDeps::compile(&bm25_property_deps, |iri: &str| {
            ledger.snapshot.encode_iri(iri)
        });

        // 6. Trace commits and collect affected subjects. Branch-aware
        //    store so the walk can resolve pre-fork ancestors when the
        //    ledger is a branch.
        let mut affected_sids: HashSet<fluree_db_core::Sid> = HashSet::new();
        let commit_store = self
            .content_store_for_record_or_id(ledger.ns_record.as_ref(), &ledger.snapshot.ledger_id)
            .await?;
        let stream = trace_commits_by_id(commit_store, head_commit_id.clone(), old_watermark);
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result?;
            let subjects = compiled_deps.affected_subjects(&commit.flakes);
            affected_sids.extend(subjects);
        }

        // If no subjects affected, fall back to full resync
        if affected_sids.is_empty() {
            warn!(
                graph_source_id = %graph_source_id,
                old_watermark = old_watermark,
                ledger_t = ledger_t,
                "No affected subjects detected, falling back to full resync"
            );
            return self.resync_vector_index(graph_source_id).await;
        }

        // 7. Convert affected Sids to IRIs
        let affected_iris: HashSet<Arc<str>> = affected_sids
            .into_iter()
            .filter_map(|sid| {
                ledger
                    .snapshot
                    .decode_sid(&sid)
                    .map(|s| Arc::from(s.as_str()))
            })
            .collect();

        info!(
            graph_source_id = %graph_source_id,
            affected_count = affected_iris.len(),
            "Found affected subjects for incremental update"
        );

        // 8. Re-run indexing query and filter to affected subjects
        let results = self.execute_vector_indexing_query(&ledger, &query).await?;

        // Expand prefix map for @id fields and property names
        let context = query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);
        let results = expand_properties_in_results(results, &prefix_map);

        let mut affected_iris_expanded = affected_iris.clone();
        for full_iri in &affected_iris {
            for (prefix, ns) in &prefix_map {
                if full_iri.starts_with(ns.as_str()) {
                    let local = &full_iri[ns.len()..];
                    let prefixed = format!("{prefix}:{local}");
                    affected_iris_expanded.insert(Arc::from(prefixed));
                }
            }
        }

        // 9. Apply incremental update
        let mut updater = IncrementalVectorUpdater::new(source_ledger_alias.as_str(), &mut index);
        let update_result = updater.apply_update(&results, &affected_iris_expanded, ledger_t);

        info!(
            graph_source_id = %graph_source_id,
            upserted = update_result.upserted,
            removed = update_result.removed,
            skipped = update_result.skipped,
            "Applied incremental update"
        );

        // 10. Persist updated index and update head pointer
        let new_index_id = self
            .write_vector_snapshot_blob(graph_source_id, &index, ledger_t)
            .await?;

        let (name, branch) = split_ledger_id(graph_source_id).map_err(|e| {
            crate::ApiError::config(format!("Invalid graph source ID '{graph_source_id}': {e}"))
        })?;

        // 11. Update graph source index record (head pointer)
        self.publisher()?
            .publish_graph_source_index(&name, &branch, &new_index_id, ledger_t)
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            index_id = %new_index_id,
            ledger_t = ledger_t,
            "Persisted synced vector index"
        );

        Ok(VectorSyncResult {
            graph_source_id: graph_source_id.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            skipped: update_result.skipped,
            old_watermark,
            new_watermark: ledger_t,
            was_full_resync: false,
        })
    }

    /// Full resync of a vector index.
    ///
    /// Rebuilds the entire index from scratch by re-running the indexing query.
    pub async fn resync_vector_index(&self, graph_source_id: &str) -> Result<VectorSyncResult> {
        use fluree_db_query::vector::usearch::deserialize;

        info!(graph_source_id = %graph_source_id, "Starting full vector index resync");

        // 1. Look up graph source record
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        if record.retracted {
            return Err(crate::ApiError::Drop(format!(
                "Cannot resync retracted graph source: {graph_source_id}"
            )));
        }

        // Parse config
        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let embedding_property = config
            .get("embedding_property")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                crate::ApiError::Config(
                    "Missing embedding_property in graph source config".to_string(),
                )
            })?;
        let dimensions = config
            .get("dimensions")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                crate::ApiError::Config("Missing dimensions in graph source config".to_string())
            })? as usize;

        // Get source ledger
        let source_ledger_alias = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // 2. Load source ledger
        let ledger = self.ledger(&source_ledger_alias).await?;
        let ledger_t = ledger.t();

        // 3. Load existing index to get old watermark
        let old_watermark = if let Some(cid) = &record.index_id {
            let cs = self.content_store(graph_source_id);
            let bytes = cs.get(cid).await?;
            let old_index = deserialize(&bytes)?;
            old_index.watermark.get(&source_ledger_alias).unwrap_or(0)
        } else {
            0
        };

        // 4. Execute indexing query
        let results = self.execute_vector_indexing_query(&ledger, &query).await?;

        // Expand prefix map for @id fields and property names
        let context = query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);
        let results = expand_properties_in_results(results, &prefix_map);

        // Expand embedding property name if prefixed
        let embedding_property_expanded = expand_prefixed_iri(embedding_property, &prefix_map)
            .unwrap_or_else(|| embedding_property.to_string());

        // 5. Build new index
        let property_deps = VectorPropertyDeps::from_query(&embedding_property_expanded, &query);

        // Parse metric from config
        let metric_str = config
            .get("metric")
            .and_then(|v| v.as_str())
            .unwrap_or("Cosine");
        let metric = match metric_str {
            "Dot" => fluree_db_query::vector::DistanceMetric::Dot,
            "Euclidean" => fluree_db_query::vector::DistanceMetric::Euclidean,
            _ => fluree_db_query::vector::DistanceMetric::Cosine,
        };

        let mut builder =
            VectorIndexBuilder::new(source_ledger_alias.as_str(), dimensions, metric)?
                .with_embedding_property(&embedding_property_expanded)
                .with_property_deps(property_deps)
                .with_watermark(ledger_t);

        if !results.is_empty() {
            builder = builder.with_capacity(results.len())?;
        }

        for result in &results {
            builder.add_result(result)?;
        }

        let upserted = builder.indexed_count();
        let skipped = builder.skipped_count();
        let index = builder.build();

        info!(
            graph_source_id = %graph_source_id,
            upserted = upserted,
            skipped = skipped,
            "Built new vector index"
        );

        // 6. Persist new index and update head pointer
        let new_index_id = self
            .write_vector_snapshot_blob(graph_source_id, &index, ledger_t)
            .await?;

        let (name, branch) = split_ledger_id(graph_source_id).map_err(|e| {
            crate::ApiError::config(format!("Invalid graph source ID '{graph_source_id}': {e}"))
        })?;

        // 7. Update graph source index record (head pointer)
        self.publisher()?
            .publish_graph_source_index(&name, &branch, &new_index_id, ledger_t)
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            index_id = %new_index_id,
            ledger_t = ledger_t,
            "Completed full vector index resync"
        );

        Ok(VectorSyncResult {
            graph_source_id: graph_source_id.to_string(),
            upserted,
            removed: 0, // Full resync doesn't track removals
            skipped,
            old_watermark,
            new_watermark: ledger_t,
            was_full_resync: true,
        })
    }

    /// Drop a vector index.
    ///
    /// This marks the graph source as retracted in the nameservice but does not
    /// immediately delete snapshot files (they may be needed for time-travel).
    pub async fn drop_vector_index(&self, graph_source_id: &str) -> Result<VectorDropResult> {
        info!(graph_source_id = %graph_source_id, "Dropping vector index");

        // Look up graph source record
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        if record.retracted {
            return Ok(VectorDropResult {
                graph_source_id: graph_source_id.to_string(),
                deleted_snapshots: 0,
                was_already_retracted: true,
            });
        }

        // Mark as retracted
        let (name, branch) = split_ledger_id(graph_source_id).map_err(|e| {
            crate::ApiError::config(format!("Invalid graph source ID '{graph_source_id}': {e}"))
        })?;

        self.publisher()?
            .retract_graph_source(&name, &branch)
            .await?;

        info!(graph_source_id = %graph_source_id, "Marked vector index as retracted");

        Ok(VectorDropResult {
            graph_source_id: graph_source_id.to_string(),
            deleted_snapshots: 0,
            was_already_retracted: false,
        })
    }
}
