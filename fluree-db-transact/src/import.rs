//! Import mode: parse TTL/TriG → stream to commit-v2 blob → store.
//!
//! Bypasses the full staging/novelty pipeline for bulk import of clean
//! RDF data. No WHERE evaluation, no cancellation, no policy enforcement,
//! no novelty index merge. Duplicate facts within a chunk are written as-is;
//! dedup happens during indexing or at query time.
//!
//! # Supported Formats
//!
//! - **Turtle**: Default graph triples only
//! - **TriG**: Default graph + named GRAPH blocks with g_id allocation
//! - **JSON-LD**: Expanded JSON-LD documents via GraphSink adapter
//!
//! See the Phase 3 plan for full semantics documentation.

mod inner {
    /// Bytes of ingest-shaped Turtle per distinct term, used to hint the
    /// parser's term-cache size. Empirical and historical: it is the ratio the
    /// parser's own unconditional reservation used before that reservation
    /// became the caller's decision, so passing it here keeps this path's
    /// memory profile — and the budget formula calibrated against it —
    /// exactly where it was.
    const BYTES_PER_DISTINCT_TERM: usize = 60;

    use crate::commit_v2::CodecEnvelope;
    use crate::commit_v2::StreamingCommitWriter;
    use crate::error::{Result, TransactError};
    use crate::generate::{infer_datatype, DT_ID, DT_LANG_STRING};
    use crate::import_sink::ImportSink;
    use crate::namespace::{NamespaceRegistry, NsAllocator, SharedNamespaceAllocator, WorkerCache};
    use crate::parse::trig_meta::{parse_trig_phase1, resolve_trig_meta, RawObject, RawTerm};
    use crate::value_convert::convert_string_literal;
    use fluree_db_core::ns_encoding::NsSplitMode;
    use fluree_db_core::CommitId;
    use fluree_db_core::{
        ContentAddressedWrite, ContentId, ContentKind, Flake, FlakeMeta, FlakeValue, Sid,
        TxnMetaEntry,
    };

    /// Returns `Some(mode)` for the genesis commit (no parent), `None` otherwise.
    /// The split mode is only persisted in the genesis commit envelope.
    fn genesis_split_mode(state: &ImportState, mode: NsSplitMode) -> Option<NsSplitMode> {
        if state.parent.is_none() {
            Some(mode)
        } else {
            None
        }
    }
    use fluree_graph_turtle::splitter::TurtlePrelude;
    use rustc_hash::FxHashSet;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Mutable state carried across chunks during an import session.
    pub struct ImportState {
        /// Current transaction number. Starts at 0; first chunk produces t=1.
        pub t: i64,
        /// Reference to the previous commit (address + id).
        pub parent: Option<CommitId>,
        /// Namespace registry (accumulates across chunks).
        pub ns_registry: NamespaceRegistry,
        /// Cumulative flake count across all commits (for progress reporting).
        pub cumulative_flakes: u64,
        /// Import start time (reused for all commits to avoid per-chunk Utc::now).
        pub import_time: String,
        /// Named graph IRI → g_id mapping (stable across chunks).
        /// g_id 0 = default graph, 1 = txn-meta, 2 = config, 3+ = user-defined.
        pub graph_ids: HashMap<String, u16>,
        /// Next available g_id for user-defined named graphs. Only used by the
        /// non-spooling fallback; the spooling path allocates from the shared
        /// graph allocator instead (see `import_trig_commit`).
        pub next_gid: u16,
        /// Accumulated turtle @prefix short names: IRI → short prefix.
        /// Captured from `on_prefix()` calls across all chunks.
        pub prefix_map: HashMap<String, String>,
    }

    impl ImportState {
        /// Create a new import state for a fresh ledger.
        ///
        /// `NamespaceRegistry::new()` includes all predefined namespace codes
        /// (rdf, xsd, etc). User-defined namespaces are added as chunks are parsed.
        pub fn new() -> Self {
            Self {
                t: 0,
                parent: None,
                ns_registry: NamespaceRegistry::new(),
                cumulative_flakes: 0,
                import_time: chrono::Utc::now().to_rfc3339(),
                graph_ids: HashMap::new(),
                // 0=default, 1=txn-meta, 2=config (reserved); user graphs start
                // at FIRST_USER_GRAPH_ID. Matches the shared graph allocator's
                // dict_id+1 convention so spooled and fallback g_ids agree.
                next_gid: fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID,
                prefix_map: HashMap::new(),
            }
        }
    }

    impl Default for ImportState {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Result of importing a single TTL chunk.
    pub struct ImportCommitResult {
        /// Content identifier (CIDv1).
        pub commit_id: ContentId,
        /// Transaction number.
        pub t: i64,
        /// Number of flakes in this commit.
        pub flake_count: u32,
        /// Size of the committed blob in bytes.
        pub blob_bytes: usize,
        /// The raw commit blob (moved, not cloned — zero extra cost).
        /// Available for downstream consumers (e.g., run generation)
        /// without re-reading from storage.
        pub commit_blob: Vec<u8>,
        /// Buffered spool result from this chunk's parse (if spool was enabled).
        /// Contains buffered RunRecords with chunk-local IDs and chunk-local
        /// dictionaries for the post-parse sort + sorted commit write pipeline.
        pub spool_result: Option<crate::import_sink::BufferedSpoolResult>,
    }

    /// Import a single TTL chunk as a v2 commit blob.
    ///
    /// Parses the Turtle input, streams flakes through the commit-v2 writer,
    /// and stores the resulting blob. Advances `state` for the next chunk.
    ///
    /// # Arguments
    /// * `state` — mutable import state (carried across chunks)
    /// * `ttl` — Turtle input text
    /// * `storage` — storage backend for writing commit blobs
    /// * `ledger_id` — ledger name for storage path construction
    /// * `skolem_base` — document-scoped blank-node key, built by the caller
    ///   via `fluree_db_core::skolem::skolem_base`
    /// * `compress` — whether to zstd-compress the ops stream
    #[allow(clippy::too_many_arguments)]
    pub async fn import_commit<S>(
        state: &mut ImportState,
        ttl: &str,
        storage: &S,
        ledger_id: &str,
        skolem_base: &str,
        compress: bool,
        spool_dir: Option<&std::path::Path>,
        spool_config: Option<&crate::import_sink::SpoolConfig>,
        chunk_idx: usize,
    ) -> Result<ImportCommitResult>
    where
        S: ContentAddressedWrite,
    {
        let new_t = state.t + 1;

        // 1. Create ImportSink + parse TTL
        let ns_codes_before = state.ns_registry.code_count();
        let _parse_span = tracing::debug_span!(
            "import_parse",
            t = new_t,
            ttl_bytes = ttl.len(),
            ns_codes = ns_codes_before,
        )
        .entered();
        // Allocate namespace codes through the spool's shared allocator when
        // spooling — SpoolContext resolves code→prefix at record-push time
        // (mid-parse), so registry-only codes would fall back to an empty
        // prefix and write suffix-only strings into the index dicts. See the
        // matching comment in `import_trig_commit`.
        let shared_ns: Arc<SharedNamespaceAllocator> = match spool_config {
            Some(cfg) => Arc::clone(&cfg.ns_alloc),
            None => Arc::new(SharedNamespaceAllocator::from_registry(&state.ns_registry)),
        };
        let mut worker_cache = WorkerCache::new(Arc::clone(&shared_ns));
        let mut sink = ImportSink::new_cached(
            &mut worker_cache,
            new_t,
            skolem_base.to_string(),
            0,
            compress,
        )
        .map_err(|e| TransactError::Parse(format!("failed to create import sink: {e}")))?;

        if let Some((dir, config)) = spool_dir.zip(spool_config) {
            let spool_path = dir.join(format!("chunk_{chunk_idx}.spool"));
            let spool_ctx = crate::import_sink::SpoolContext::new(spool_path, chunk_idx, 0, config)
                .map_err(|e| TransactError::Parse(format!("spool create: {e}")))?;
            sink.set_spool_context(spool_ctx);
        }

        fluree_graph_turtle::parse(ttl, &mut sink)
            .map_err(|e| TransactError::Parse(e.to_string()))?;
        drop(_parse_span);

        // 2. Retrieve writer, get namespace delta, build envelope
        let (writer, op_count, spool_result, envelope) = {
            let _span = tracing::debug_span!("import_build_envelope", t = new_t).entered();
            let (writer, chunk_prefix_map, spool_ctx) = sink
                .into_parts()
                .map_err(|e| TransactError::Parse(format!("flake encode error: {e}")))?;
            state.prefix_map.extend(chunk_prefix_map);

            let spool_result = spool_ctx.map(crate::import_sink::SpoolContext::finish_buffered);
            let op_count = writer.op_count();

            // Adopt this commit's shared-allocator codes into the registry so
            // take_delta() includes them in the commit's namespace_delta.
            let new_codes = worker_cache.into_new_codes();
            if !new_codes.is_empty() {
                let adopted = shared_ns.lookup_codes(&new_codes);
                state
                    .ns_registry
                    .adopt_delta_for_persistence(&adopted)
                    .map_err(|e| TransactError::Parse(format!("namespace adopt conflict: {e}")))?;
            }
            let ns_delta = state.ns_registry.take_delta();
            let ns_codes_after = state.ns_registry.code_count();

            tracing::debug!(
                op_count,
                ns_delta_size = ns_delta.len(),
                ns_codes = ns_codes_after,
                "import sink finalized"
            );

            // 3. Update cumulative flake count
            state.cumulative_flakes += op_count as u64;

            // Persist split mode in genesis commit (first chunk, no previous ref).
            let ns_split_mode = genesis_split_mode(state, state.ns_registry.split_mode());

            let envelope = CodecEnvelope {
                t: new_t,
                parents: state.parent.clone().into_iter().collect(),
                namespace_delta: ns_delta,
                txn: None,
                time: Some(state.import_time.clone()),

                txn_signature: None,
                txn_meta: Vec::new(),
                graph_delta: HashMap::new(),
                ns_split_mode,
            };

            (writer, op_count, spool_result, envelope)
        };

        // 4. Finalize blob
        let result = {
            let _span = tracing::debug_span!("import_finish_blob", t = new_t, op_count).entered();
            writer.finish(&envelope)?
        };
        let commit_cid = ContentId::new(ContentKind::Commit, &result.bytes);
        let blob_bytes = result.bytes.len();

        // 5. Store
        // `.instrument` (not a held `EnteredSpan`) across the await: an entered
        // span guard is !Send and must never live across a suspension point.
        let write_res = {
            use tracing::Instrument as _;
            storage
                .content_write_bytes(ContentKind::Commit, ledger_id, &result.bytes)
                .instrument(tracing::debug_span!("import_store", t = new_t, blob_bytes))
                .await?
        };

        tracing::debug!(
            t = new_t,
            flakes = op_count,
            blob_bytes,
            address = %write_res.address,
            "import commit stored"
        );

        // 8. Advance state
        state.t = new_t;
        state.parent = Some(commit_cid.clone());

        Ok(ImportCommitResult {
            commit_id: commit_cid,
            t: new_t,
            flake_count: op_count,
            blob_bytes,
            commit_blob: result.bytes,
            spool_result,
        })
    }

    /// Import a single Turtle chunk using a pre-extracted header prelude (prefixes/base).
    ///
    /// This avoids the need to prepend the raw prefix block text onto every chunk,
    /// which would otherwise force an extra full-chunk string copy per parse.
    #[allow(clippy::too_many_arguments)]
    pub async fn import_commit_with_prelude<S>(
        state: &mut ImportState,
        ttl: &str,
        prelude: &TurtlePrelude,
        storage: &S,
        ledger_id: &str,
        skolem_base: &str,
        compress: bool,
        spool_dir: Option<&std::path::Path>,
        spool_config: Option<&crate::import_sink::SpoolConfig>,
        chunk_idx: usize,
    ) -> Result<ImportCommitResult>
    where
        S: ContentAddressedWrite,
    {
        // Apply prelude once (equivalent to parsing the directive header).
        if state.t == 0 {
            for (short, ns_iri) in &prelude.prefixes {
                state.ns_registry.get_or_allocate(ns_iri);
                if !short.is_empty() {
                    state.prefix_map.insert(ns_iri.clone(), short.clone());
                }
            }
        }

        let new_t = state.t + 1;

        let ns_codes_before = state.ns_registry.code_count();
        let _parse_span = tracing::debug_span!(
            "import_parse",
            t = new_t,
            ttl_bytes = ttl.len(),
            ns_codes = ns_codes_before,
        )
        .entered();
        // Shared-allocator-backed allocation for spool prefix visibility;
        // see the comment in `import_commit`.
        let shared_ns: Arc<SharedNamespaceAllocator> = match spool_config {
            Some(cfg) => Arc::clone(&cfg.ns_alloc),
            None => Arc::new(SharedNamespaceAllocator::from_registry(&state.ns_registry)),
        };
        let mut worker_cache = WorkerCache::new(Arc::clone(&shared_ns));
        let mut sink = ImportSink::new_cached(
            &mut worker_cache,
            new_t,
            skolem_base.to_string(),
            0,
            compress,
        )
        .map_err(|e| TransactError::Parse(format!("failed to create import sink: {e}")))?;

        if let Some((dir, config)) = spool_dir.zip(spool_config) {
            let spool_path = dir.join(format!("chunk_{chunk_idx}.spool"));
            let spool_ctx = crate::import_sink::SpoolContext::new(spool_path, chunk_idx, 0, config)
                .map_err(|e| TransactError::Parse(format!("spool create: {e}")))?;
            sink.set_spool_context(spool_ctx);
        }

        // Import is the caller that HAS an estimate, so it passes one. The
        // parser starts small without a hint, which is right for a converter
        // handed an arbitrary document and wrong here: an import chunk is
        // ingest-shaped and routinely holds more than a million distinct terms,
        // where growing the caches by doubling peaks at ~1.5× the final table —
        // roughly +200 MiB per in-flight chunk at 2M distinct, against a memory
        // budget calibrated when this reservation was unconditional.
        //
        // The ratio is the one that reservation used: ~60 bytes of ingest-shaped
        // Turtle per distinct term. It is a hint, clamped by the parser, and
        // wrong only in the direction the old code was already wrong in.
        let terms_hint = ttl.len() / BYTES_PER_DISTINCT_TERM;
        fluree_graph_turtle::parse_with_prefixes_base_options(
            ttl,
            &mut sink,
            &prelude.prefixes,
            prelude.base.as_deref(),
            fluree_graph_turtle::ParserOptions::default().with_distinct_terms_hint(terms_hint),
        )
        .map_err(|e| TransactError::Parse(e.to_string()))?;
        drop(_parse_span);

        let (writer, op_count, spool_result, envelope) = {
            let _span = tracing::debug_span!("import_build_envelope", t = new_t).entered();
            let (writer, _chunk_prefix_map, spool_ctx) = sink
                .into_parts()
                .map_err(|e| TransactError::Parse(format!("flake encode error: {e}")))?;

            let spool_result = spool_ctx.map(crate::import_sink::SpoolContext::finish_buffered);
            let op_count = writer.op_count();

            let new_codes = worker_cache.into_new_codes();
            if !new_codes.is_empty() {
                let adopted = shared_ns.lookup_codes(&new_codes);
                state
                    .ns_registry
                    .adopt_delta_for_persistence(&adopted)
                    .map_err(|e| TransactError::Parse(format!("namespace adopt conflict: {e}")))?;
            }
            let ns_delta = state.ns_registry.take_delta();
            let ns_codes_after = state.ns_registry.code_count();

            tracing::debug!(
                op_count,
                ns_delta_size = ns_delta.len(),
                ns_codes = ns_codes_after,
                "import sink finalized"
            );

            state.cumulative_flakes += op_count as u64;

            // Persist split mode in genesis commit (first chunk, no previous ref).
            let ns_split_mode = genesis_split_mode(state, state.ns_registry.split_mode());

            let envelope = CodecEnvelope {
                t: new_t,
                parents: state.parent.clone().into_iter().collect(),
                namespace_delta: ns_delta,
                txn: None,
                time: Some(state.import_time.clone()),

                txn_signature: None,
                txn_meta: Vec::new(),
                graph_delta: HashMap::new(),
                ns_split_mode,
            };

            (writer, op_count, spool_result, envelope)
        };

        let result = {
            let _span = tracing::debug_span!("import_finish_blob", t = new_t, op_count).entered();
            writer.finish(&envelope)?
        };
        let commit_cid = ContentId::new(ContentKind::Commit, &result.bytes);
        let blob_bytes = result.bytes.len();

        // `.instrument` (not a held `EnteredSpan`) across the await: an entered
        // span guard is !Send and must never live across a suspension point.
        let write_res = {
            use tracing::Instrument as _;
            storage
                .content_write_bytes(ContentKind::Commit, ledger_id, &result.bytes)
                .instrument(tracing::debug_span!("import_store", t = new_t, blob_bytes))
                .await?
        };

        tracing::debug!(
            t = new_t,
            flakes = op_count,
            blob_bytes,
            address = %write_res.address,
            "import commit stored"
        );

        state.t = new_t;
        state.parent = Some(commit_cid.clone());

        Ok(ImportCommitResult {
            commit_id: commit_cid,
            t: new_t,
            flake_count: op_count,
            blob_bytes,
            commit_blob: result.bytes,
            spool_result,
        })
    }

    /// Import a single TriG chunk as a v2 commit blob (with named graph support).
    ///
    /// Like `import_commit`, but supports TriG format with GRAPH blocks for named graphs.
    /// If the input contains GRAPH blocks, they are processed and stored with appropriate
    /// graph IDs, and `graph_delta` is populated in the commit envelope.
    ///
    /// # Named Graph ID Allocation
    ///
    /// - g_id 0: default graph (implicit)
    /// - g_id 1: txn-meta graph (reserved)
    /// - g_id 2+: user-defined named graphs
    ///
    /// # Arguments
    /// * `state` — mutable import state (carried across chunks)
    /// * `trig` — TriG input text (Turtle-compatible if no GRAPH blocks)
    /// * `storage` — storage backend for writing commit blobs
    /// * `ledger_id` — ledger name for storage path construction
    /// * `skolem_base` — document-scoped blank-node key, built by the caller
    ///   via `fluree_db_core::skolem::skolem_base`
    /// * `compress` — whether to zstd-compress the ops stream
    #[allow(clippy::too_many_arguments)]
    pub async fn import_trig_commit<S>(
        state: &mut ImportState,
        trig: &str,
        storage: &S,
        ledger_id: &str,
        skolem_base: &str,
        compress: bool,
        spool_dir: Option<&std::path::Path>,
        spool_config: Option<&crate::import_sink::SpoolConfig>,
        chunk_idx: usize,
    ) -> Result<ImportCommitResult>
    where
        S: ContentAddressedWrite,
    {
        let new_t = state.t + 1;

        // 1. Parse TriG to extract GRAPH blocks
        let phase1 = parse_trig_phase1(trig)?;

        // If no named graphs and no txn-meta, use the faster pure-Turtle path
        if phase1.named_graphs.is_empty() && phase1.raw_meta.is_none() {
            return import_commit(
                state,
                trig,
                storage,
                ledger_id,
                skolem_base,
                compress,
                spool_dir,
                spool_config,
                chunk_idx,
            )
            .await;
        }

        let _parse_span = tracing::debug_span!(
            "import_trig_parse",
            t = new_t,
            trig_bytes = trig.len(),
            named_graph_count = phase1.named_graphs.len(),
        )
        .entered();

        // 2. Create ImportSink and parse default graph Turtle.
        //
        // Namespace codes MUST be allocated through the spool's shared
        // allocator (not `state.ns_registry` directly): SpoolContext resolves
        // code→prefix via `ns_alloc` at record-push time — i.e. DURING the
        // parse, right after an `@prefix` directive allocates the code. Codes
        // that only live in the registry are invisible to it, and its
        // empty-prefix fallback would permanently write suffix-only predicate
        // strings into the index's predicate dict (breaking bound-predicate
        // lookups after import). Without spooling, a throwaway allocator
        // seeded from the registry preserves exact code assignment.
        let shared_ns: Arc<SharedNamespaceAllocator> = match spool_config {
            Some(cfg) => Arc::clone(&cfg.ns_alloc),
            None => Arc::new(SharedNamespaceAllocator::from_registry(&state.ns_registry)),
        };
        let mut worker_cache = WorkerCache::new(Arc::clone(&shared_ns));
        let mut sink = ImportSink::new_cached(
            &mut worker_cache,
            new_t,
            skolem_base.to_string(),
            0,
            compress,
        )
        .map_err(|e| TransactError::Parse(format!("failed to create import sink: {e}")))?;

        if let Some((dir, config)) = spool_dir.zip(spool_config) {
            let spool_path = dir.join(format!("chunk_{chunk_idx}.spool"));
            let spool_ctx = crate::import_sink::SpoolContext::new(spool_path, chunk_idx, 0, config)
                .map_err(|e| TransactError::Parse(format!("spool create: {e}")))?;
            sink.set_spool_context(spool_ctx);
        }

        fluree_graph_turtle::parse(&phase1.turtle, &mut sink)
            .map_err(|e| TransactError::Parse(e.to_string()))?;

        // 3. Retrieve writer + spool context for named graph flakes.
        //
        // Default-graph triples were parsed (and spooled) above. Named-graph
        // flakes are pushed below to BOTH the commit blob (`writer`) and the
        // spool (`spool_ctx`), so they reach the Tier-2 index and the index
        // root's `named_graphs` routing exactly like default-graph triples.
        // The spool is finished only after the named-graph loop completes.
        let (mut writer, chunk_prefix_map, mut spool_ctx) = sink
            .into_parts()
            .map_err(|e| TransactError::Parse(format!("flake encode error: {e}")))?;
        state.prefix_map.extend(chunk_prefix_map);
        let mut op_count = writer.op_count();

        // 4. Process named graphs
        // Use session-level graph ID allocation for stability across chunks.
        // Only new mappings (introduced by this commit) go into graph_delta.
        let mut graph_delta: HashMap<u16, String> = HashMap::new();

        for block in &phase1.named_graphs {
            // Allocate or reuse g_id for this graph IRI.
            //
            // When spooling (index build) is active, allocate from the shared
            // graph allocator so the commit's `graph_delta` and the index's
            // `graphs.dict` / query graph registry agree on the same g_id
            // (allocator convention: dict_id + 1 = g_id). Without spooling
            // (commit-only paths), fall back to the session counter.
            let g_id = if let Some(&existing) = state.graph_ids.get(&block.iri) {
                existing
            } else {
                // New graph IRI in this session — allocate and record in delta.
                let id = match spool_ctx.as_mut() {
                    Some(sc) => sc.graph_g_id_for_iri(&block.iri),
                    None => {
                        let id = state.next_gid;
                        state.next_gid += 1;
                        id
                    }
                };
                state.graph_ids.insert(block.iri.clone(), id);
                graph_delta.insert(id, block.iri.clone());
                id
            };

            // Create a graph Sid (using the graph IRI's namespace + local name)
            let graph_sid = worker_cache.sid_for_iri(&block.iri);

            // Process each triple in this named graph
            for triple in &block.triples {
                let subject = triple.subject.as_ref().ok_or_else(|| {
                    TransactError::Parse("named graph triple missing subject".to_string())
                })?;

                let s = expand_term(subject, &block.prefixes, &mut worker_cache, skolem_base)?;
                let p = expand_term(
                    &triple.predicate,
                    &block.prefixes,
                    &mut worker_cache,
                    skolem_base,
                )?;

                for obj in &triple.objects {
                    let (o, dt, lang) =
                        expand_object(obj, &block.prefixes, &mut worker_cache, skolem_base)?;

                    // Spool the named-graph flake under its g_id (so it enters
                    // the index), then encode it into the commit blob.
                    if let Some(sc) = spool_ctx.as_mut() {
                        sc.push_named_graph_record(
                            g_id,
                            crate::import_sink::FlakeRecord {
                                s: &s,
                                p: &p,
                                o: &o,
                                dt: &dt,
                                lang: lang.as_deref(),
                                list_index: None,
                                t: new_t,
                            },
                        );
                    }

                    let meta = lang.as_deref().map(FlakeMeta::with_lang);
                    let flake = Flake::new_in_graph(
                        graph_sid.clone(),
                        s.clone(),
                        p.clone(),
                        o,
                        dt,
                        new_t,
                        true,
                        meta,
                    );

                    writer.push_flake(&flake).map_err(|e| {
                        TransactError::Parse(format!("failed to encode named graph flake: {e}"))
                    })?;
                    op_count += 1;
                }
            }
        }

        // Named-graph flakes are now in the spool; finish it for the index.
        let spool_result = spool_ctx.map(crate::import_sink::SpoolContext::finish_buffered);
        drop(_parse_span);

        // Adopt codes this commit allocated in the shared allocator into the
        // registry, recording them for persistence so take_delta() below
        // includes them in this commit's namespace_delta.
        let new_codes = worker_cache.into_new_codes();
        if !new_codes.is_empty() {
            let adopted = shared_ns.lookup_codes(&new_codes);
            state
                .ns_registry
                .adopt_delta_for_persistence(&adopted)
                .map_err(|e| TransactError::Parse(format!("namespace adopt conflict: {e}")))?;
        }

        // 5. Resolve txn-meta if present
        let txn_meta = if let Some(ref raw_meta) = phase1.raw_meta {
            resolve_trig_meta(raw_meta, &mut state.ns_registry)?
        } else {
            Vec::new()
        };

        // 6. Build envelope
        let ns_delta = state.ns_registry.take_delta();
        let named_graph_count = graph_delta.len();

        tracing::debug!(
            op_count,
            ns_delta_size = ns_delta.len(),
            graph_delta_size = named_graph_count,
            txn_meta_count = txn_meta.len(),
            "import trig sink finalized"
        );

        state.cumulative_flakes += op_count as u64;

        // Persist split mode in genesis commit (first chunk, no previous ref).
        let ns_split_mode = genesis_split_mode(state, state.ns_registry.split_mode());

        let envelope = CodecEnvelope {
            t: new_t,
            parents: state.parent.clone().into_iter().collect(),
            namespace_delta: ns_delta,
            txn: None,
            time: Some(state.import_time.clone()),

            txn_signature: None,
            txn_meta,
            graph_delta,
            ns_split_mode,
        };

        // 7. Finalize blob
        let result = {
            let _span =
                tracing::debug_span!("import_trig_finish_blob", t = new_t, op_count).entered();
            writer.finish(&envelope)?
        };
        let commit_cid = ContentId::new(ContentKind::Commit, &result.bytes);
        let blob_bytes = result.bytes.len();

        // 8. Store
        // `.instrument` (not a held `EnteredSpan`) across the await — see
        // `import_commit`'s store step.
        let write_res = {
            use tracing::Instrument as _;
            storage
                .content_write_bytes(ContentKind::Commit, ledger_id, &result.bytes)
                .instrument(tracing::debug_span!(
                    "import_trig_store",
                    t = new_t,
                    blob_bytes
                ))
                .await?
        };

        tracing::debug!(
            t = new_t,
            flakes = op_count,
            blob_bytes,
            address = %write_res.address,
            named_graphs = named_graph_count,
            "import trig commit stored"
        );

        // 9. Advance state
        state.t = new_t;
        state.parent = Some(commit_cid.clone());

        Ok(ImportCommitResult {
            commit_id: commit_cid,
            t: new_t,
            flake_count: op_count,
            blob_bytes,
            commit_blob: result.bytes,
            spool_result,
        })
    }

    /// Expand a RawTerm to a Sid using the prefix map and worker cache.
    ///
    /// Allocation goes through the shared-allocator-backed [`WorkerCache`] so
    /// the spool's prefix lookups see codes the moment they're allocated (see
    /// the comment in `import_trig_commit`).
    ///
    /// Blank-node labels are skolemized with the same `{skolem_base}-{label}` key
    /// as `ImportSink::skolemize`, so a label shared between the default graph
    /// and a named-graph block of one TriG document resolves to one node
    /// (TriG scopes labels to the whole document), while the same label in a
    /// different document/commit stays distinct.
    fn expand_term(
        term: &RawTerm,
        prefixes: &rustc_hash::FxHashMap<String, String>,
        ns: &mut WorkerCache,
        skolem_base: &str,
    ) -> Result<Sid> {
        match term {
            RawTerm::Iri(iri) => {
                if let Some(local) = iri.strip_prefix("_:") {
                    Ok(ns.blank_node_sid(&format!("{skolem_base}-{local}")))
                } else {
                    Ok(ns.sid_for_iri(iri))
                }
            }
            RawTerm::PrefixedName { prefix, local } => {
                let namespace = prefixes
                    .get(prefix.as_str())
                    .ok_or_else(|| TransactError::Parse(format!("undefined prefix: {prefix}")))?;
                let iri = format!("{namespace}{local}");
                Ok(ns.sid_for_iri(&iri))
            }
        }
    }

    /// Expand a RawObject to FlakeValue + datatype Sid + optional language.
    fn expand_object(
        obj: &RawObject,
        prefixes: &rustc_hash::FxHashMap<String, String>,
        ns: &mut WorkerCache,
        skolem_base: &str,
    ) -> Result<(FlakeValue, Sid, Option<String>)> {
        match obj {
            RawObject::Iri(iri) => {
                // Blank labels use the ImportSink skolem key; see expand_term.
                let sid = if let Some(local) = iri.strip_prefix("_:") {
                    ns.blank_node_sid(&format!("{skolem_base}-{local}"))
                } else {
                    ns.sid_for_iri(iri)
                };
                Ok((FlakeValue::Ref(sid), DT_ID.clone(), None))
            }
            RawObject::PrefixedName { prefix, local } => {
                let namespace = prefixes
                    .get(prefix.as_str())
                    .ok_or_else(|| TransactError::Parse(format!("undefined prefix: {prefix}")))?;
                let iri = format!("{namespace}{local}");
                let sid = ns.sid_for_iri(&iri);
                Ok((FlakeValue::Ref(sid), DT_ID.clone(), None))
            }
            RawObject::String(s) => Ok((
                FlakeValue::String(s.clone()),
                infer_datatype(&FlakeValue::String(s.clone())),
                None,
            )),
            RawObject::Integer(n) => Ok((
                FlakeValue::Long(*n),
                infer_datatype(&FlakeValue::Long(*n)),
                None,
            )),
            RawObject::Double(n) => Ok((
                FlakeValue::Double(*n),
                infer_datatype(&FlakeValue::Double(*n)),
                None,
            )),
            RawObject::Boolean(b) => Ok((
                FlakeValue::Boolean(*b),
                infer_datatype(&FlakeValue::Boolean(*b)),
                None,
            )),
            RawObject::LangString { value, lang } => Ok((
                FlakeValue::String(value.clone()),
                DT_LANG_STRING.clone(),
                Some(lang.clone()),
            )),
            RawObject::TypedLiteral { value, datatype } => {
                let (fv, dt) =
                    convert_string_literal(value, datatype, &mut NsAllocator::Cached(ns));
                Ok((fv, dt, None))
            }
        }
    }

    // ========================================================================
    // Parallel-friendly split: parse_chunk + finalize_parsed_chunk
    // ========================================================================

    /// Result of parsing a single TTL chunk (parallelizable step).
    ///
    /// Contains the `StreamingCommitWriter` (tempfile-backed, Send) and the
    /// set of namespace codes first observed by this worker after its snapshot.
    /// Can be sent across threads and finalized later on the commit thread.
    pub struct ParsedChunk {
        /// The streaming writer with all encoded ops spooled to a tempfile.
        pub writer: StreamingCommitWriter,
        /// Number of flakes (ops) encoded.
        pub op_count: u32,
        /// Namespace codes first observed after this worker's snapshot
        /// (code >= snapshot_next_code). Includes codes allocated by this
        /// worker AND codes allocated by other workers that this chunk uses.
        /// The serial finalizer uses this to determine which codes need to
        /// be "published" in this commit's namespace_delta.
        pub new_codes: FxHashSet<u16>,
        /// Turtle @prefix short names from this chunk: IRI → short prefix.
        pub prefix_map: HashMap<String, String>,
        /// Buffered spool result from parallel import (if enabled). Contains
        /// buffered RunRecords with chunk-local IDs and chunk-local
        /// dictionaries for the post-parse sort + sorted commit write pipeline.
        pub spool_result: Option<crate::import_sink::BufferedSpoolResult>,
        /// Transaction metadata for this chunk's commit envelope. EMPTY for every
        /// text-import chunk (Turtle/TriG/JSON-LD) — those set `Vec::new()`, so the
        /// commit is byte-identical to before this field existed. The materialize
        /// (twin) builder sets it on ONE chunk — the FINAL commit — to carry the
        /// completion stamp (watermark + mapping hash + builder version), so a
        /// twin is valid iff a head-walk finds the stamp (DEC-003 §17).
        pub txn_meta: Vec<TxnMetaEntry>,
    }

    /// Parse a TTL chunk into a `StreamingCommitWriter`. Thread-safe.
    ///
    /// Uses a [`WorkerCache`] backed by the shared allocator for lock-free
    /// namespace lookups. New prefix allocations are tracked in the worker's
    /// `new_codes` set for commit-order publication by the serial finalizer.
    ///
    /// The `t` value is pre-assigned by the caller (chunk_index + 1);
    /// `doc_scope` identifies the source document this chunk was cut from (see
    /// [`skolem_base`]).
    ///
    /// If `spool_dir` is `Some`, a spool file is written alongside the commit
    /// blob for Phase A validation of the spool format.
    #[allow(clippy::too_many_arguments)]
    pub fn parse_chunk(
        ttl: &str,
        alloc: &Arc<SharedNamespaceAllocator>,
        t: i64,
        skolem_base: &str,
        sub_chunk: u32,
        compress: bool,
        spool_dir: Option<&std::path::Path>,
        spool_config: Option<&crate::import_sink::SpoolConfig>,
        chunk_idx: usize,
    ) -> Result<ParsedChunk> {
        let _parse_span = tracing::debug_span!("parse_chunk", t, ttl_bytes = ttl.len(),).entered();

        let mut worker_cache = WorkerCache::new(Arc::clone(alloc));
        let mut sink = ImportSink::new_cached(
            &mut worker_cache,
            t,
            skolem_base.to_string(),
            sub_chunk,
            compress,
        )
        .map_err(|e| TransactError::Parse(format!("failed to create import sink: {e}")))?;

        if let Some((dir, config)) = spool_dir.zip(spool_config) {
            let spool_path = dir.join(format!("chunk_{chunk_idx}.spool"));
            let spool_ctx = crate::import_sink::SpoolContext::new(spool_path, chunk_idx, 0, config)
                .map_err(|e| TransactError::Parse(format!("spool create: {e}")))?;
            sink.set_spool_context(spool_ctx);
        }

        fluree_graph_turtle::parse(ttl, &mut sink)
            .map_err(|e| TransactError::Parse(e.to_string()))?;
        drop(_parse_span);

        let (writer, prefix_map, spool_ctx) = sink
            .into_parts()
            .map_err(|e| TransactError::Parse(format!("flake encode error: {e}")))?;
        let op_count = writer.op_count();
        let new_codes = worker_cache.into_new_codes();

        let spool_result = spool_ctx.map(crate::import_sink::SpoolContext::finish_buffered);

        Ok(ParsedChunk {
            writer,
            op_count,
            new_codes,
            prefix_map,
            spool_result,
            txn_meta: Vec::new(),
        })
    }

    /// Parse a TTL chunk using a pre-extracted header prelude (prefixes/base).
    ///
    /// Like `parse_chunk`, but does not require the prefix block text to be
    /// prepended onto `ttl`. Uses a [`WorkerCache`] for lock-free lookups.
    ///
    /// `doc_scope` carries the same document-scoping contract as in
    /// [`parse_chunk`] — see [`skolem_base`].
    ///
    /// If `spool_dir` is `Some`, a spool file is written alongside the commit
    /// blob for Phase A validation of the spool format.
    #[allow(clippy::too_many_arguments)]
    pub fn parse_chunk_with_prelude(
        ttl: &str,
        alloc: &Arc<SharedNamespaceAllocator>,
        prelude: &TurtlePrelude,
        t: i64,
        skolem_base: &str,
        sub_chunk: u32,
        compress: bool,
        spool_dir: Option<&std::path::Path>,
        spool_config: Option<&crate::import_sink::SpoolConfig>,
        chunk_idx: usize,
    ) -> Result<ParsedChunk> {
        let _parse_span = tracing::debug_span!("parse_chunk", t, ttl_bytes = ttl.len(),).entered();

        let mut worker_cache = WorkerCache::new(Arc::clone(alloc));

        // Pre-register namespaces from the prelude for stable code assignment.
        for (_short, ns_iri) in &prelude.prefixes {
            worker_cache.get_or_allocate(ns_iri);
        }

        let mut sink = ImportSink::new_cached(
            &mut worker_cache,
            t,
            skolem_base.to_string(),
            sub_chunk,
            compress,
        )
        .map_err(|e| TransactError::Parse(format!("failed to create import sink: {e}")))?;

        if let Some((dir, config)) = spool_dir.zip(spool_config) {
            let spool_path = dir.join(format!("chunk_{chunk_idx}.spool"));
            let spool_ctx = crate::import_sink::SpoolContext::new(spool_path, chunk_idx, 0, config)
                .map_err(|e| TransactError::Parse(format!("spool create: {e}")))?;
            sink.set_spool_context(spool_ctx);
        }

        // Import is the caller that HAS an estimate, so it passes one. The
        // parser starts small without a hint, which is right for a converter
        // handed an arbitrary document and wrong here: an import chunk is
        // ingest-shaped and routinely holds more than a million distinct terms,
        // where growing the caches by doubling peaks at ~1.5× the final table —
        // roughly +200 MiB per in-flight chunk at 2M distinct, against a memory
        // budget calibrated when this reservation was unconditional.
        //
        // The ratio is the one that reservation used: ~60 bytes of ingest-shaped
        // Turtle per distinct term. It is a hint, clamped by the parser, and
        // wrong only in the direction the old code was already wrong in.
        let terms_hint = ttl.len() / BYTES_PER_DISTINCT_TERM;
        fluree_graph_turtle::parse_with_prefixes_base_options(
            ttl,
            &mut sink,
            &prelude.prefixes,
            prelude.base.as_deref(),
            fluree_graph_turtle::ParserOptions::default().with_distinct_terms_hint(terms_hint),
        )
        .map_err(|e| TransactError::Parse(e.to_string()))?;
        drop(_parse_span);

        let (writer, _prefix_map, spool_ctx) = sink
            .into_parts()
            .map_err(|e| TransactError::Parse(format!("flake encode error: {e}")))?;
        let op_count = writer.op_count();
        let new_codes = worker_cache.into_new_codes();

        let spool_result = spool_ctx.map(crate::import_sink::SpoolContext::finish_buffered);

        Ok(ParsedChunk {
            writer,
            op_count,
            new_codes,
            // Prelude prefixes are applied at session start; chunks should not
            // need to contribute additional prefix mappings.
            prefix_map: HashMap::new(),
            spool_result,
            txn_meta: Vec::new(),
        })
    }

    /// Extract prefix→IRI mappings from a JSON-LD `@context` and register them
    /// in the sink's namespace allocator via `on_prefix()`.
    ///
    /// JSON-LD `expand()` resolves `@context` prefixes internally, producing
    /// fully-expanded IRIs that bypass the namespace trie. Pre-registering the
    /// declared prefixes ensures:
    /// - The trie has entries for the declared namespaces (optimal code allocation)
    /// - `sid_for_iri()` hits the trie instead of the split heuristic
    /// - Namespace codes match the intended prefix boundaries
    ///
    /// Handles both inline `@context` objects and arrays of contexts.
    fn register_jsonld_prefixes(
        doc: &serde_json::Value,
        sink: &mut crate::import_sink::ImportSink,
    ) {
        use fluree_graph_ir::GraphSink;

        fn visit_context(ctx: &serde_json::Value, sink: &mut crate::import_sink::ImportSink) {
            match ctx {
                serde_json::Value::Object(obj) => {
                    for (key, val) in obj {
                        // Skip JSON-LD keywords (@base, @language, @vocab, etc.)
                        if key.starts_with('@') {
                            continue;
                        }
                        // Simple string values are prefix → IRI mappings
                        if let Some(iri) = val.as_str() {
                            sink.on_prefix(key, iri);
                        }
                        // Object values with @id are also prefix → IRI mappings
                        else if let Some(obj_val) = val.as_object() {
                            if let Some(id) = obj_val.get("@id").and_then(|v| v.as_str()) {
                                sink.on_prefix(key, id);
                            }
                        }
                    }
                }
                serde_json::Value::Array(arr) => {
                    for item in arr {
                        visit_context(item, sink);
                    }
                }
                _ => {} // String contexts (remote URLs) — can't extract prefixes
            }
        }

        if let Some(ctx) = doc.get("@context") {
            visit_context(ctx, sink);
        }
    }

    /// Parse a JSON-LD document into a `ParsedChunk` using a shared allocator.
    ///
    /// Analogous to [`parse_chunk`] (Turtle) but for JSON-LD input. Uses
    /// `SharedNamespaceAllocator` via `WorkerCache` so that namespace codes are
    /// visible to the spool pipeline during the same parse pass.
    #[allow(clippy::too_many_arguments)]
    pub fn parse_jsonld_chunk(
        jsonld: &str,
        alloc: &Arc<SharedNamespaceAllocator>,
        t: i64,
        skolem_base: &str,
        sub_chunk: u32,
        compress: bool,
        spool_dir: Option<&std::path::Path>,
        spool_config: Option<&crate::import_sink::SpoolConfig>,
        chunk_idx: usize,
    ) -> Result<ParsedChunk> {
        // Chunk-scoped, matching the behavior before document scoping existed.
        // For the `Files` and remote arms that is also document-scoped, because
        // one chunk is one whole `.jsonld` file. It is NOT for the ndjson
        // stream, where `NdjsonReader` packs many lines into one synthetic
        // `@graph` document: labels there merge across lines within a chunk and
        // split across chunks. That predates document scoping and is left alone
        // deliberately — deciding whether an ndjson line is its own RDF
        // document is a JSON-LD question, not a Turtle-chunking one.

        let _parse_span =
            tracing::debug_span!("parse_jsonld_chunk", t, jsonld_bytes = jsonld.len(),).entered();

        let mut worker_cache = WorkerCache::new(Arc::clone(alloc));
        let mut sink = ImportSink::new_cached(
            &mut worker_cache,
            t,
            skolem_base.to_string(),
            sub_chunk,
            compress,
        )
        .map_err(|e| TransactError::Parse(format!("failed to create import sink: {e}")))?;

        if let Some((dir, config)) = spool_dir.zip(spool_config) {
            let spool_path = dir.join(format!("chunk_{chunk_idx}.spool"));
            let spool_ctx = crate::import_sink::SpoolContext::new(spool_path, chunk_idx, 0, config)
                .map_err(|e| TransactError::Parse(format!("spool create: {e}")))?;
            sink.set_spool_context(spool_ctx);
        }

        let mut doc: serde_json::Value = serde_json::from_str(jsonld)
            .map_err(|e| TransactError::Parse(format!("JSON parse error: {e}")))?;

        // Edge annotations: rewrite `@annotation` / `@edge` into the seven-fact
        // `f:reifies*` encoding before expansion, exactly as the transact path
        // does — otherwise the JSON-LD expander silently drops them and a bulk
        // import loses every edge property. RDF mode (lpg_mode=false): a
        // non-empty `@annotation` lowers to a reifier bundle; the CSV/LPG
        // loader never emits an empty `@annotation: {}`.
        //
        // The lowering is gated on a cheap presence check so the common
        // (non-annotated) bulk import pays nothing. The firewall gate is
        // *broader* on purpose: it must also fire on the fully-expanded
        // `…#reifies*` IRI form, which contains none of the `@`-keywords
        // above. Gating it on the same check would let a bulk import inject
        // system facts directly, bypassing the guard the transact path
        // always enforces. The bare `reifies` token covers both forms.
        let needs_lowering = jsonld.contains("@annotation")
            || jsonld.contains("@edge")
            || jsonld.contains("@reifies");
        if needs_lowering || jsonld.contains("reifies") {
            let top_ctx = crate::parse::edge_annotations::top_level_context(&doc)?;
            crate::parse::edge_annotations::run_user_authored_reifies_firewall(&doc, &top_ctx)?;
            if needs_lowering {
                crate::parse::edge_annotations::lower_edge_annotations_after_firewall(
                    &mut doc, &top_ctx, false,
                )?;
            }
        }

        // Register @context prefix→IRI mappings in the namespace trie BEFORE
        // expansion. JSON-LD expand() resolves prefixes internally, but the
        // resulting fully-expanded IRIs bypass the trie. Without pre-registration,
        // every IRI falls through to the split heuristic, potentially allocating
        // more namespace codes than necessary and losing alignment with the
        // declared prefixes.
        register_jsonld_prefixes(&doc, &mut sink);

        let expanded = fluree_graph_json_ld::expand(&doc)
            .map_err(|e| TransactError::Parse(format!("JSON-LD expand error: {e}")))?;
        fluree_graph_json_ld::adapter::to_graph_events(&expanded, &mut sink)
            .map_err(|e| TransactError::Parse(format!("JSON-LD adapter error: {e}")))?;
        drop(_parse_span);

        let (writer, prefix_map, spool_ctx) = sink
            .into_parts()
            .map_err(|e| TransactError::Parse(format!("flake encode error: {e}")))?;
        let op_count = writer.op_count();
        let new_codes = worker_cache.into_new_codes();

        let spool_result = spool_ctx.map(crate::import_sink::SpoolContext::finish_buffered);

        Ok(ParsedChunk {
            writer,
            op_count,
            new_codes,
            prefix_map,
            spool_result,
            txn_meta: Vec::new(),
        })
    }

    /// Finalize a parsed chunk: build envelope, store blob, update state.
    ///
    /// Must be called **serially in chunk order** because each commit
    /// references the previous commit's address (hash chain).
    ///
    /// The `ns_delta` is computed by the caller via commit-order publication:
    /// codes from `parsed.new_codes` that haven't been published by a prior
    /// commit are looked up from the shared allocator and passed here.
    pub async fn finalize_parsed_chunk<S>(
        state: &mut ImportState,
        parsed: ParsedChunk,
        ns_delta: HashMap<u16, String>,
        storage: &S,
        ledger_id: &str,
    ) -> Result<ImportCommitResult>
    where
        S: ContentAddressedWrite,
    {
        let new_t = state.t + 1;

        // `.instrument` (not a held `EnteredSpan`): the body awaits the blob
        // store, and an entered span guard is !Send across suspension points.
        let span = tracing::debug_span!("finalize_parsed_chunk", t = new_t);
        use tracing::Instrument as _;
        async move {
            // Merge published namespaces into serial registry to keep it in sync
            // (needed for TriG serial paths and the final namespace snapshot).
            for (code, prefix) in &ns_delta {
                state.ns_registry.ensure_code(*code, prefix).map_err(|e| {
                    TransactError::FlakeGeneration(format!("namespace code conflict: {e}"))
                })?;
            }
            // Merge turtle prefix short names into session state
            state.prefix_map.extend(parsed.prefix_map);

            state.cumulative_flakes += parsed.op_count as u64;

            // Persist split mode in genesis commit (first chunk, no previous ref).
            let ns_split_mode = genesis_split_mode(state, state.ns_registry.split_mode());

            let envelope = CodecEnvelope {
                t: new_t,
                parents: state.parent.clone().into_iter().collect(),
                namespace_delta: ns_delta,
                txn: None,
                time: Some(state.import_time.clone()),

                txn_signature: None,
                // Empty for every text-import chunk (unchanged behavior); the
                // materialize builder sets it on the final commit to carry the twin's
                // completion stamp. The predicate namespace codes were interned through
                // this chunk's sink, so they are already in `ns_delta` above.
                txn_meta: parsed.txn_meta,
                graph_delta: HashMap::new(),
                ns_split_mode,
            };

            let result = parsed.writer.finish(&envelope)?;
            let commit_cid = ContentId::new(ContentKind::Commit, &result.bytes);
            let blob_bytes = result.bytes.len();

            let write_res = storage
                .content_write_bytes(ContentKind::Commit, ledger_id, &result.bytes)
                .await?;

            tracing::debug!(
                t = new_t,
                flakes = parsed.op_count,
                blob_bytes,
                address = %write_res.address,
                "parsed chunk finalized and stored"
            );

            state.t = new_t;
            state.parent = Some(commit_cid.clone());

            Ok(ImportCommitResult {
                commit_id: commit_cid,
                t: new_t,
                flake_count: parsed.op_count,
                blob_bytes,
                commit_blob: result.bytes,
                spool_result: parsed.spool_result,
            })
        }
        .instrument(span)
        .await
    }
}

pub use inner::{
    finalize_parsed_chunk, import_commit_with_prelude, import_trig_commit, parse_chunk,
    parse_chunk_with_prelude, parse_jsonld_chunk, ImportCommitResult, ImportState, ParsedChunk,
};
