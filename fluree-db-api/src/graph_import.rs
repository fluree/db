//! Bulk import into a new named graph of an existing ledger.
//!
//! The fresh-ledger pipeline ([`crate::import`]), changed in three places:
//!
//! - its commits continue the ledger's chain, every statement in the target
//!   graph, so the commit history alone still reproduces the ledger;
//! - its dictionaries extend the ledger's: a term the ledger already holds
//!   keeps its id, and new ones are numbered above the watermarks;
//! - its index is spliced into the ledger's published root rather than
//!   replacing it. The new graph's four orders are built in bulk; the
//!   import's commit metadata, a handful of rows per commit, goes through
//!   the incremental indexer into the ledger's txn-meta graph.
//!
//! The ledger is held for the whole import: background indexing stops and
//! writes through the same cached ledger handle wait
//! ([`crate::LedgerHandle::exclude_writers`]), while reads go on against the
//! head the import started from. A writer that does not share the handle (a
//! separate connection or process, or a connection without ledger caching)
//! is not held; the commit-head CAS catches it.
//!
//! Both heads are published at the end, under the handle's state lock, and
//! the state at them, loaded beforehand, is installed with them. The commit
//! head goes first and only if it has not moved: losing that CAS publishes
//! nothing. Once it has moved the import is in the ledger, and a failure or a
//! stop before the index publishes leaves its commits read as novelty until
//! an index covers them ([`ImportError::PublishedWithoutIndex`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use fluree_db_binary_index::dict::DictTreeReader;
use fluree_db_binary_index::format::index_root::NamedGraphRouting;
use fluree_db_binary_index::{BinaryIndexStore, IndexRoot};
use fluree_db_core::{ContentId, ContentStore, LedgerId, Sid, Storage};
use fluree_db_indexer::run_index::resolver::SharedResolverState;
use fluree_db_indexer::run_index::{IncrementalNovelty, NewTerms};
use fluree_db_transact::NamespaceRegistry;

use crate::import::{ImportConfig, ImportError, ImportResult, ImportSource};
use crate::{ApiError, Fluree, IndexingMode};

/// What an import into a new named graph continues from: an existing
/// ledger's published head, indexed through it.
pub(crate) struct GraphImportSeed {
    /// The named graph the import fills.
    pub graph_iri: String,
    /// The g_id that graph gets: the next free one.
    pub g_id: u16,
    /// The ledger's head, which the import's first commit chains onto.
    pub head_t: i64,
    pub head_commit: ContentId,
    /// The ledger's namespaces at the head.
    pub ns_registry: NamespaceRegistry,
    /// The published index root, at `head_t`, and its id.
    pub base_root: IndexRoot,
    pub base_root_id: ContentId,
    /// `base_root`'s predicates, datatypes, graphs and language tags.
    pub base_dicts: SharedResolverState,
    /// `base_root`'s reverse dictionaries, which the seeded vocabulary
    /// merges look the import's terms up in.
    pub subject_tree: Arc<DictTreeReader>,
    pub string_tree: Arc<DictTreeReader>,
    /// `base_root`'s index, which names existing subjects (class stats).
    pub base_store: Arc<BinaryIndexStore>,
    /// Reads the ledger's artifacts — those it inherits from a parent
    /// branch included — and writes new ones.
    pub content_store: Arc<dyn ContentStore>,
}

impl GraphImportSeed {
    /// The ledger's graphs, IRI → g_id.
    pub(crate) fn graph_ids(&self) -> HashMap<String, u16> {
        let graphs = &self.base_dicts.graphs;
        (0..graphs.len())
            .filter_map(|id| Some((graphs.resolve(id)?.to_string(), (id + 1) as u16)))
            .collect()
    }
}

/// Where in its run directory an import into an existing ledger writes the
/// subjects and strings the ledger's dictionaries lack.
pub(crate) const NEW_SUBJECTS_FILE: &str = "subjects.new";
pub(crate) const NEW_STRINGS_FILE: &str = "strings.new";

/// How an import into an existing ledger grew its dictionaries.
pub(crate) struct SeededDicts {
    /// New subjects, sid64 → namespace-stripped suffix.
    pub new_subjects: NewTerms,
    pub subject_watermarks: Vec<u64>,
    /// Whether a new local id exceeds `u16::MAX`.
    pub needs_wide: bool,
    /// New strings, string id → value.
    pub new_strings: NewTerms,
    pub string_watermark: u32,
    /// The ledger's predicates, datatypes and graphs after the import, in
    /// id order (graph dict id + 1 = g_id).
    pub predicate_iris: Vec<String>,
    pub datatype_iris: Vec<String>,
    pub graph_iris: Vec<String>,
}

/// The new graph's bulk-built index and what the import learned building it.
pub(crate) struct GraphBuild<'a> {
    pub ledger_id: &'a str,
    pub run_dir: &'a Path,
    pub build: &'a fluree_db_indexer::BuildResult,
    /// The graph's stats, and its class stats when it has rdf:type data.
    pub stats: Option<(
        fluree_db_indexer::stats::IdStatsResult,
        Option<fluree_db_indexer::stats::SpotClassStats>,
    )>,
    pub dicts: SeededDicts,
    pub namespace_codes: &'a HashMap<u16, String>,
    pub language_tags: Vec<String>,
    /// The import's commit metadata: the meta chunk, remapped like any other.
    pub txn_meta: fluree_db_indexer::CommitInput,
    pub final_t: i64,
    pub total_commit_size: u64,
    pub total_asserts: u64,
    pub saw_list_meta: bool,
}

/// The ledger's index after the import, written but not yet published.
pub(crate) struct SplicedIndex {
    pub root_id: ContentId,
    pub index_t: i64,
    /// Whether the graph holds statement annotations, which leaves the
    /// ledger without an annotation arena until a reindex.
    pub has_annotations: bool,
}

/// Splice the new graph into the ledger's index: upload its artifacts, grow
/// the dictionaries, and merge the import's commit metadata into txn-meta,
/// writing the root that holds all three.
pub(crate) async fn splice_graph_into_index(
    seed: &GraphImportSeed,
    graph: GraphBuild<'_>,
) -> Result<SplicedIndex, ImportError> {
    let cs = &seed.content_store;
    let base = &seed.base_root;
    let upload_error = |e: fluree_db_indexer::IndexerError| ImportError::Upload(e.to_string());

    // ---- The graph's own index and arenas ----
    let uploaded = fluree_db_indexer::upload_indexes_to_cas(cs.as_ref(), graph.build)
        .await
        .map_err(upload_error)?;
    let routing: NamedGraphRouting = uploaded
        .named_graphs
        .into_iter()
        .find(|ng| ng.g_id == seed.g_id)
        .ok_or_else(|| {
            ImportError::NoChunks(format!(
                "the source holds no statements to import into <{}>",
                seed.graph_iri
            ))
        })?;
    let (numbig, vectors) = tokio::try_join!(
        fluree_db_indexer::upload_numbig_arenas_from_disk(cs.as_ref(), graph.run_dir),
        fluree_db_indexer::upload_vector_arenas_from_disk(cs.as_ref(), graph.run_dir),
    )
    .map_err(upload_error)?;
    let arenas = crate::import::graph_arena_refs(&numbig, &vectors);

    // ---- Dictionaries ----
    let dicts = graph.dicts;
    let dict_update = fluree_db_indexer::extend_dicts(
        cs.as_ref(),
        &base.dict_refs,
        base.index_t,
        &dicts.new_subjects,
        &dicts.new_strings,
    )
    .await
    .map_err(upload_error)?;
    let mut replaced_cids = dict_update.replaced_cids;
    replaced_cids.extend(fluree_db_indexer::run_index::replaced_dict_cids(
        &base.dict_refs,
        &dict_update.dict_refs,
    ));

    // ---- The base root with the graph in it ----
    let mut namespace_codes = base.namespace_codes.clone();
    for (&code, prefix) in graph.namespace_codes {
        match namespace_codes.get(&code) {
            Some(existing) if existing != prefix => {
                return Err(ImportError::Transact(format!(
                    "namespace code {code} is <{existing}> in the ledger but <{prefix}> \
                     in the import"
                )));
            }
            Some(_) => {}
            None => {
                namespace_codes.insert(code, prefix.clone());
            }
        }
    }
    let predicate_sids = fluree_db_indexer::predicate_sids(
        dicts.predicate_iris.iter().map(String::as_str),
        &namespace_codes,
    );
    let is_reifies = |p_id: u32| {
        predicate_sids.get(p_id as usize).is_some_and(|(ns, name)| {
            fluree_db_core::is_reserved_reifies_predicate(&Sid::new(*ns, name.as_str()))
        })
    };
    // Whether the graph holds statement annotations. Without its stats to
    // say, assume it does.
    let annotated = graph.stats.as_ref().is_none_or(|(id_stats, _)| {
        id_stats
            .graphs
            .iter()
            .filter(|g| g.g_id == seed.g_id)
            .flat_map(|g| &g.properties)
            .any(|p| is_reifies(p.p_id))
    });
    let new_classes = new_class_names(seed, &graph.stats, &dicts.new_subjects)?;
    let stats = graph_stats(
        seed,
        &graph.stats,
        &new_classes,
        &predicate_sids,
        &graph.language_tags,
        &namespace_codes,
    );

    let mut root = base.clone();
    root.named_graphs.push(routing);
    root.named_graphs.sort_by_key(|ng| ng.g_id);
    if let Some(arena) = arenas.into_iter().find(|a| a.g_id == seed.g_id) {
        root.graph_arenas.push(arena);
        root.graph_arenas.sort_by_key(|a| a.g_id);
    }
    root.dict_refs = dict_update.dict_refs;
    root.subject_watermarks = dicts.subject_watermarks;
    root.string_watermark = dicts.string_watermark;
    if dicts.needs_wide {
        root.subject_id_encoding = fluree_db_core::SubjectIdEncoding::Wide;
    }
    root.has_annotations |= (0..predicate_sids.len() as u32).any(is_reifies);
    root.namespace_codes = namespace_codes;
    root.predicate_sids = predicate_sids;
    root.graph_iris = dicts.graph_iris;
    root.datatype_iris = dicts.datatype_iris;
    root.language_tags = graph.language_tags;
    let custom_datatypes: Vec<String> = root
        .datatype_iris
        .iter()
        .skip(fluree_db_core::DatatypeDictId::RESERVED_COUNT as usize)
        .cloned()
        .collect();
    root.o_type_table = IndexRoot::build_o_type_table(&custom_datatypes, &root.language_tags);
    if graph.saw_list_meta {
        root.has_list_meta = Some(true);
    }
    if let Some(stats) = stats {
        let mut merged = root.stats.take().unwrap_or_default();
        merged.flakes += stats.flakes;
        let mut graphs = merged.graphs.take().unwrap_or_default();
        graphs.push(stats);
        graphs.sort_by_key(|g| g.g_id);
        merged.graphs = Some(graphs);
        root.stats = Some(merged);
    }

    // ---- txn-meta, through the incremental indexer ----
    let records = txn_meta_records(&graph.txn_meta).map_err(ImportError::Io)?;
    let shared = SharedResolverState::from_index_root(&root)
        .map_err(|e| ImportError::IndexBuild(format!("spliced root dictionaries: {e}")))?;
    let novelty = IncrementalNovelty {
        ops: vec![1; records.len()],
        records,
        superseded: Vec::new(),
        superseded_ops: Vec::new(),
        updated_watermarks: root.subject_watermarks.clone(),
        updated_string_watermark: root.string_watermark,
        base_root: root,
        shared,
        new_subjects: Vec::new(),
        new_strings: Vec::new(),
        max_t: graph.final_t,
        delta_commit_size: graph.total_commit_size,
        delta_asserts: graph.total_asserts,
        delta_retracts: 0,
        base_vector_counts: HashMap::new(),
        base_numbig_counts: HashMap::new(),
        fulltext_string_bytes: HashMap::new(),
    };
    // The ledger's annotation arena still covers every annotation when the
    // graph adds none; otherwise it is dropped, and annotations read by
    // scan until a reindex seals the arena again.
    let config = fluree_db_indexer::IndexerConfig {
        attachment_events: (!annotated)
            .then(|| fluree_db_indexer::AttachmentEventCoverage::Augment(Vec::new())),
        ..Default::default()
    };
    let indexed = fluree_db_indexer::index_novelty(
        Arc::clone(cs),
        graph.ledger_id,
        seed.base_root_id.clone(),
        novelty,
        config,
        replaced_cids,
    )
    .await
    .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

    Ok(SplicedIndex {
        root_id: indexed.root_id,
        index_t: indexed.index_t,
        has_annotations: annotated,
    })
}

/// The suffixes of the graph's classes, and of the classes its references
/// point at, that are new to the ledger: only the import's own new
/// subjects name them yet.
fn new_class_names(
    seed: &GraphImportSeed,
    stats: &Option<(
        fluree_db_indexer::stats::IdStatsResult,
        Option<fluree_db_indexer::stats::SpotClassStats>,
    )>,
    new_subjects: &NewTerms,
) -> std::io::Result<HashMap<u64, String>> {
    let mut names = HashMap::new();
    let Some((_, Some(class_stats))) = stats else {
        return Ok(names);
    };
    let watermarks = &seed.base_root.subject_watermarks;
    let is_new = |sid64: &u64| {
        let id = fluree_db_core::subject_id::SubjectId::from_u64(*sid64);
        watermarks
            .get(id.ns_code() as usize)
            .is_none_or(|&watermark| id.local_id() > watermark)
    };
    let in_graph = |&(g_id, _): &(u16, u64)| g_id == seed.g_id;
    let classes = class_stats
        .class_counts
        .keys()
        .filter(|key| in_graph(key))
        .map(|&(_, class)| class);
    let targets = class_stats
        .class_prop_refs
        .iter()
        .filter(|(key, _)| in_graph(key))
        .flat_map(|(_, by_property)| by_property.values())
        .flat_map(|by_target| by_target.keys().copied());
    let mut wanted: BTreeSet<u64> = classes.chain(targets).filter(is_new).collect();

    // The file is in sid64 order, so the scan stops past the last wanted.
    let Some(&last) = wanted.last() else {
        return Ok(names);
    };
    let mut reader = new_subjects.reader()?;
    while let Some((sid64, suffix)) = reader.next_term()? {
        if sid64 > last {
            break;
        }
        if wanted.remove(&sid64) {
            names.insert(sid64, String::from_utf8_lossy(suffix).into_owned());
        }
    }
    Ok(names)
}

/// The new graph's stats entry, its classes named through the ledger's
/// dictionary and `new_classes`.
fn graph_stats(
    seed: &GraphImportSeed,
    stats: &Option<(
        fluree_db_indexer::stats::IdStatsResult,
        Option<fluree_db_indexer::stats::SpotClassStats>,
    )>,
    new_classes: &HashMap<u64, String>,
    predicate_sids: &[(u16, String)],
    language_tags: &[String],
    namespace_codes: &BTreeMap<u16, String>,
) -> Option<fluree_db_core::GraphStatsEntry> {
    let (id_stats, class_stats) = stats.as_ref()?;
    let mut entry = id_stats
        .graphs
        .iter()
        .find(|g| g.g_id == seed.g_id)
        .cloned()?;
    if let Some(class_stats) = class_stats {
        let mut resolve = |sid64: u64| -> Option<Sid> {
            let ns_code = fluree_db_core::subject_id::SubjectId::from_u64(sid64).ns_code();
            if let Some(suffix) = new_classes.get(&sid64) {
                return Some(Sid::new(ns_code, suffix.as_str()));
            }
            let iri = seed.base_store.resolve_subject_iri(sid64).ok()?;
            let prefix = namespace_codes.get(&ns_code).map_or("", String::as_str);
            Some(Sid::new(ns_code, iri.strip_prefix(prefix).unwrap_or(&iri)))
        };
        let mut classes = fluree_db_indexer::stats::build_class_stat_entries_with(
            class_stats,
            predicate_sids,
            language_tags,
            None,
            &mut resolve,
        );
        entry.classes = classes.remove(&seed.g_id);
    }
    Some(entry)
}

/// The import's commit metadata as txn-meta novelty: the meta chunk's
/// records in global ids, sorted by `(g_id, SPOT)`.
fn txn_meta_records(
    commit: &fluree_db_indexer::CommitInput,
) -> std::io::Result<Vec<fluree_db_binary_index::format::run_record_v2::RunRecordV2>> {
    use fluree_db_indexer::run_index::spool::{
        remap_v2_record, MmapStringRemap, MmapSubjectRemap, SortedCommitReaderV2,
    };

    let subjects = MmapSubjectRemap::open(&commit.subject_remap_path)?;
    let strings = MmapStringRemap::open(&commit.string_remap_path)?;
    let mut records = Vec::with_capacity(commit.record_count as usize);
    for record in SortedCommitReaderV2::open(&commit.commit_path, commit.record_count)? {
        let mut record = record?;
        remap_v2_record(&mut record, &subjects, &strings, Some(&commit.lang_remap))?;
        if record.g_id != fluree_db_core::graph_registry::TXN_META_GRAPH_ID {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "the import's meta chunk holds a record in graph {}",
                    record.g_id
                ),
            ));
        }
        records.push(record);
    }
    Ok(records)
}

// ============================================================================
// Orchestration
// ============================================================================

/// The named graph an import into `ledger_id` would fill: `graph_iri` as
/// the import names it. Refuses the default graph and the ledger's own
/// graphs; whether the ledger exists and already has the graph is left to
/// the import, which checks both against the head it starts from.
pub fn graph_import_target(ledger_id: &str, graph_iri: &str) -> Result<String, ImportError> {
    let alias = LedgerId::parse(ledger_id)
        .map_err(|e| ImportError::Api(e.into()))?
        .to_string();
    let names_default = || {
        ImportError::Refused(format!(
            "<{graph_iri}> names the default graph of {alias}; an import into an \
             existing ledger fills a new named graph"
        ))
    };
    match fluree_db_transact::import::target_graph_iri(graph_iri, &alias) {
        Ok(Some(graph)) => Ok(graph),
        Ok(None) | Err(fluree_db_transact::TransactError::DefaultGraphNameAsGraph) => {
            Err(names_default())
        }
        Err(e) => Err(ImportError::Invalid(e)),
    }
}

/// Index `ledger_id` through its head in this task, for a connection with
/// no background indexer to ask: incremental over the commits after its
/// index where it can be, a rebuild otherwise.
async fn index_to_head(fluree: &Fluree, ledger_id: &str) -> Result<(), ImportError> {
    let mut config = fluree_db_indexer::IndexerConfig::default()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());
    // An annotated ledger seals its annotation arena in the build, as the
    // background indexer's would; others skip the provider, as
    // `Fluree::reindex` does.
    if let Some(provider) = fluree.attachment_events_provider() {
        let handle = fluree.ledger_cached(ledger_id).await?;
        let view = handle.snapshot().await;
        if view.snapshot.has_annotations || view.novelty.attachments.has_annotations() {
            config.attachment_events = provider.attachment_events(handle.id()).await;
        }
    }
    let content_store = fluree.branched_content_store(ledger_id).await?;
    let indexed = fluree_db_indexer::build_index_for_ledger(
        content_store,
        fluree.nameservice(),
        ledger_id,
        config,
    )
    .await
    .map_err(|e| ImportError::IndexBuild(e.to_string()))?;
    fluree
        .publisher()?
        .publish_index_allow_equal(ledger_id, indexed.index_t, &indexed.root_id)
        .await
        .map_err(|e| ImportError::Api(e.into()))?;
    Ok(())
}

/// Import `source` into the new named graph `graph_iri` of `ledger_id`.
pub(crate) async fn import_into_graph<S>(
    fluree: &Fluree,
    storage: &S,
    ledger_id: &str,
    graph_iri: &str,
    source: ImportSource,
    config: &ImportConfig,
) -> Result<ImportResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if !config.build_index || !config.publish {
        return Err(ImportError::Refused(String::from(
            "an import into a graph of an existing ledger builds and publishes its index; \
             build_index(false) and publish(false) apply only to a new ledger",
        )));
    }
    if config.virtual_source.is_some() {
        return Err(ImportError::Refused(String::from(
            "an R2RML source can be materialized only into a new ledger",
        )));
    }
    let ledger = LedgerId::parse(ledger_id).map_err(|e| ImportError::Api(e.into()))?;
    let alias = ledger.to_string();
    let graph_iri = graph_import_target(&alias, graph_iri)?;

    // Index the ledger up to its head first, so the root the import splices
    // into covers every commit before it.
    let record = lookup(fluree, &ledger).await?;
    if record.index_t < record.commit_t {
        match &fluree.indexing_mode {
            IndexingMode::Background(_) => {
                tracing::info!(
                    ledger_id = %alias,
                    index_t = record.index_t,
                    commit_t = record.commit_t,
                    "indexing the ledger to its head before the graph import"
                );
                fluree
                    .trigger_index(&alias, crate::admin::TriggerIndexOptions::default())
                    .await?;
            }
            IndexingMode::Disabled => {
                tracing::info!(
                    ledger_id = %alias,
                    index_t = record.index_t,
                    commit_t = record.commit_t,
                    "indexing the ledger to its head before the graph import"
                );
                index_to_head(fluree, &alias).await?;
            }
        }
    }

    // Hold the ledger: no background build, no write, until both heads move.
    let _maintenance = match &fluree.indexing_mode {
        IndexingMode::Background(handle) => {
            Some(handle.hold_quiesced(&ledger).await.ok_or_else(|| {
                ImportError::Api(ApiError::BranchConflict(format!(
                    "another maintenance operation holds {alias}; retry when it completes"
                )))
            })?)
        }
        IndexingMode::Disabled => None,
    };
    let handle = match &fluree.ledger_manager {
        Some(_) => Some(fluree.ledger_cached(&alias).await?),
        None => None,
    };
    let writers = match &handle {
        Some(handle) => Some(handle.exclude_writers().await),
        None => None,
    };

    let record = lookup(fluree, &ledger).await?;
    let (Some(head_commit), Some(base_root_id)) =
        (record.commit_head_id.clone(), record.index_head_id.clone())
    else {
        return Err(ImportError::Refused(format!(
            "{alias} has no indexed commits; import into a new ledger with `create` instead"
        )));
    };
    if record.index_t != record.commit_t {
        return Err(ImportError::Api(ApiError::BranchConflict(format!(
            "{alias} committed t={} while its index was brought up to date; retry",
            record.commit_t
        ))));
    }

    let seed = load_seed(fluree, &record, graph_iri, head_commit, base_root_id).await?;
    tracing::info!(
        ledger_id = %alias,
        graph = %seed.graph_iri,
        g_id = seed.g_id,
        base_t = seed.head_t,
        "importing into a new named graph"
    );

    let mut result = crate::import::run_import_pipeline(
        storage,
        fluree.publisher()?,
        &alias,
        source,
        config,
        Some(&seed),
    )
    .await?;
    let root_id = result
        .root_id
        .clone()
        .ok_or_else(|| ImportError::IndexBuild("the graph import wrote no index".into()))?;

    // ---- Publish: the commit head, guarded on the head it started from,
    // then the index that covers it ----
    storage
        .sync()
        .await
        .map_err(|e| ImportError::Storage(format!("flush import artifacts: {e}")))?;

    // The state the cached handle takes, loaded before either head moves:
    // the ledger's record with both heads replaced, read as a load reads it.
    let prepared = match &handle {
        Some(_) => {
            let mut published = lookup(fluree, &ledger).await?;
            published.commit_head_id = Some(result.commit_head_id.clone());
            published.commit_t = result.t;
            published.index_head_id = Some(root_id.clone());
            published.index_t = result.index_t;
            Some(load_state(fluree, published).await?)
        }
        None => None,
    };
    // Publish under the state lock and install with it, as a push does. A
    // snapshot taken meanwhile, this process's cache listener's among them,
    // waits and reads the new head, rather than a commit head moved past the
    // state it holds and the import's commits to catch up on.
    let mut installing = match writers {
        Some(writers) => Some(writers.lock_state().await),
        None => None,
    };

    tracing::info!(
        ledger_id = %alias,
        graph = %seed.graph_iri,
        t = result.t,
        commit_head = %result.commit_head_id,
        index_t = result.index_t,
        index_root = %root_id,
        "publishing graph import"
    );
    let publisher = fluree.publisher()?;
    let expected = fluree_db_nameservice::RefValue {
        id: Some(seed.head_commit.clone()),
        t: seed.head_t,
    };
    let new_head = fluree_db_nameservice::RefValue {
        id: Some(result.commit_head_id.clone()),
        t: result.t,
    };
    match publisher
        .compare_and_set_ref(
            &alias,
            fluree_db_nameservice::RefKind::CommitHead,
            Some(&expected),
            &new_head,
        )
        .await
        .map_err(|e| ImportError::Storage(e.to_string()))?
    {
        fluree_db_nameservice::CasResult::Updated => {}
        fluree_db_nameservice::CasResult::Conflict { actual } => {
            return Err(ImportError::Api(ApiError::BranchConflict(format!(
                "{alias} moved to {} while importing into <{}>; nothing was published",
                actual.map_or_else(|| "no head".to_string(), |a| format!("t={}", a.t)),
                seed.graph_iri
            ))));
        }
    }
    // From here the import is in the ledger. A failure or a stop before the
    // index publishes leaves the commits read as novelty until an index
    // covers them; the cached handle keeps the state it had, which the next
    // write reconciles on losing its commit-head CAS.
    publisher
        .publish_index(&alias, result.index_t, &root_id)
        .await
        .map_err(|e| ImportError::PublishedWithoutIndex {
            t: result.t,
            reason: e.to_string(),
        })?;

    // Writers held off resume on the new head.
    if let (Some(handle), Some(guard), Some(state)) = (&handle, installing.as_mut(), prepared) {
        handle.sync_binary_store_from_state(&state).await;
        guard.replace(state);
    }
    drop(installing);

    result.graph = Some(crate::import::ImportedGraph {
        iri: seed.graph_iri,
        g_id: seed.g_id,
        base_t: seed.head_t,
    });
    tracing::info!(
        ledger_id = %alias,
        t = result.t,
        root_id = %root_id,
        "graph import published"
    );
    Ok(result)
}

async fn lookup(
    fluree: &Fluree,
    ledger: &LedgerId,
) -> Result<fluree_db_nameservice::NsRecord, ImportError> {
    let record = fluree
        .nameservice()
        .lookup(ledger)
        .await
        .map_err(|e| ImportError::Storage(e.to_string()))?
        .filter(|r| !r.retracted)
        .ok_or_else(|| {
            ImportError::Api(ApiError::NotFound(format!("Ledger not found: {ledger}")))
        })?;
    Ok(record)
}

/// Name `root`'s txn-meta and config graphs for `ledger_id`. A branch forked
/// from an indexed ledger starts on its parent's root, which names them for
/// the parent; the root this import publishes is the branch's own, and one
/// whose txn-meta graph names another ledger no longer loads. The graphs keep
/// their ids and data; only their IRIs change.
fn bind_reserved_graphs(root: &mut IndexRoot, ledger_id: &str) {
    root.ledger_id = ledger_id.to_string();
    if let Some(iri) = root.graph_iris.get_mut(0) {
        *iri = fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id);
    }
    if let Some(iri) = root.graph_iris.get_mut(1) {
        *iri = fluree_db_core::config_graph_iri(ledger_id);
    }
}

/// Load what the import continues from: the published root at the head.
async fn load_seed(
    fluree: &Fluree,
    record: &fluree_db_nameservice::NsRecord,
    graph_iri: String,
    head_commit: ContentId,
    base_root_id: ContentId,
) -> Result<GraphImportSeed, ImportError> {
    let storage_error = |e: std::io::Error| ImportError::Storage(e.to_string());
    let content_store: Arc<dyn ContentStore> =
        fluree_db_nameservice::branched_content_store_for_record(
            fluree.backend(),
            fluree.nameservice_mode.reader(),
            record,
        )
        .await
        .map_err(|e| ImportError::Storage(e.to_string()))?;
    let root_bytes = content_store
        .get(&base_root_id)
        .await
        .map_err(|e| ImportError::Storage(format!("read index root {base_root_id}: {e}")))?;
    let mut base_root = IndexRoot::decode(&root_bytes)
        .map_err(|e| ImportError::Storage(format!("decode index root {base_root_id}: {e}")))?;
    bind_reserved_graphs(&mut base_root, &record.ledger_id);
    let base_dicts = SharedResolverState::from_index_root(&base_root)
        .map_err(|e| ImportError::Storage(format!("index root dictionaries: {e}")))?;

    if base_dicts.graphs.get(&graph_iri).is_some() {
        return Err(ImportError::Refused(format!(
            "{} already has the graph <{graph_iri}>; an import fills a new graph only",
            record.ledger_id
        )));
    }
    let g_id = u16::try_from(base_dicts.graphs.len() + 1)
        .map_err(|_| ImportError::Transact("the ledger has no graph id left".into()))?;

    let mut ns_registry = NamespaceRegistry::new();
    for (&code, prefix) in &base_root.namespace_codes {
        ns_registry
            .ensure_code(code, prefix)
            .map_err(|e| ImportError::Storage(format!("ledger namespace {code}: {e}")))?;
    }
    ns_registry.set_split_mode(base_root.ns_split_mode);

    let cache_dir = fluree_db_indexer::IndexerConfig::default().artifact_cache_dir();
    let _ = std::fs::create_dir_all(&cache_dir);
    let subject_tree = DictTreeReader::from_refs(
        &content_store,
        &base_root.dict_refs.subject_reverse,
        None,
        Some(&cache_dir),
    )
    .await
    .map_err(storage_error)?;
    let string_tree = DictTreeReader::from_refs(
        &content_store,
        &base_root.dict_refs.string_reverse,
        None,
        Some(&cache_dir),
    )
    .await
    .map_err(storage_error)?;
    let base_store = BinaryIndexStore::load_from_root_v6(
        Arc::clone(&content_store),
        &base_root,
        &cache_dir,
        None,
    )
    .await
    .map_err(storage_error)?;

    Ok(GraphImportSeed {
        graph_iri,
        g_id,
        head_t: record.commit_t,
        head_commit,
        ns_registry,
        base_root,
        base_root_id,
        base_dicts,
        subject_tree: Arc::new(subject_tree),
        string_tree: Arc::new(string_tree),
        base_store: Arc::new(base_store),
        content_store,
    })
}

/// The ledger's state at the heads `record` names, loaded as the ledger
/// manager loads one.
async fn load_state(
    fluree: &Fluree,
    record: fluree_db_nameservice::NsRecord,
) -> Result<crate::LedgerState, ImportError> {
    let mut state =
        crate::LedgerState::load_record(&fluree.nameservice_mode, record, fluree.backend())
            .await
            .map_err(|e| ImportError::Api(e.into()))?;
    if let Some(manager) = &fluree.ledger_manager {
        crate::ledger_manager::load_and_attach_binary_store(
            fluree.backend(),
            fluree.nameservice_mode.reader(),
            &mut state,
            &manager.config().cache_dir,
            manager.config().leaflet_cache.clone(),
            None,
        )
        .await?;
    }
    Ok(state)
}
