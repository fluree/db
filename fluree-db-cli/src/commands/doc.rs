//! `fluree doc` — a folder of documents into a graph-RAG ledger, and search
//! over what it built.
//!
//! `ingest` runs in-process against local storage: parse (cached), chunk,
//! embed, retract the document's previous extraction, insert structure plus
//! chunks as one commit, then create or sync the vector and full-text graph
//! sources over the ledger. The HNSW index has no HTTP creation endpoint,
//! so there is no server route for this command yet.
//!
//! `search` embeds the query with the same endpoint and joins the index hit
//! back to the chunk's text, section path and source document, so a result
//! is a citation and not just a score.

use crate::cli::{DocAction, DocIngestArgs, DocRelationMode, DocSearchArgs, DocSearchMode};
use crate::commands::doc_sources::{self, Source};
use crate::context::{self, build_fluree};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{Bm25CreateConfig, Fluree};
use fluree_db_doc::extract::{self, ChunkExtraction, ChunkInput, ExtractionGraph};
use fluree_db_doc::graph::{self, DocumentMeta, ExtractionStamp};
use fluree_db_doc::{
    collect_inputs, prepare, vocab, Chunk, ChunkConfig, DocCache, DocConfig, EmbeddingClient,
    ExtractOptions, Extractor, Gazetteer, IngestOptions, LlmClient, Model, RelationMode,
    ResolvePolicy, VlmReader,
};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

pub async fn run(action: DocAction, dirs: &FlureeDir) -> CliResult<()> {
    match action {
        DocAction::Ingest(args) => run_ingest(args, dirs).await,
        DocAction::Search(args) => run_search(args, dirs).await,
    }
}

fn cache_root(dirs: &FlureeDir) -> PathBuf {
    dirs.config_dir().join("cache").join("doc")
}

/// The `[doc]` config with `remote = "<name>"` resolved: the remote's URL
/// becomes the gateway base and its stored login the bearer token for every
/// slot not set explicitly. The remote is looked up in the project config
/// first, then the global one, so a login done once from the home directory
/// serves every project.
async fn resolve_config(dirs: &FlureeDir) -> CliResult<DocConfig> {
    let config = crate::config::read_doc_config(dirs.config_dir())?.with_env();
    let Some(remote_name) = config.remote.clone() else {
        return Ok(config);
    };
    // Project config first; then `~/.fluree`, which is where a `fluree
    // remote add` run from the home directory lands; then the platform
    // config directory.
    let mut candidates = vec![dirs.clone()];
    if let Some(home) = dirs::home_dir() {
        let dot = home.join(fluree_db_api::server_defaults::FLUREE_DIR);
        if dot.is_dir() {
            candidates.push(FlureeDir::unified(dot));
        }
    }
    candidates.extend(FlureeDir::global());
    let mut found = None;
    for candidate in candidates {
        match context::build_remote_client(&remote_name, &candidate).await {
            Ok(client) => {
                found = Some((client, candidate));
                break;
            }
            Err(CliError::NotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }
    let Some((client, remote_dirs)) = found else {
        return Err(CliError::NotFound(format!(
            "doc.remote '{remote_name}': no such remote\n  hint: fluree remote add {remote_name} <url>"
        )));
    };
    // The model slots carry the token themselves, so it must be valid up
    // front. A stored login with time left is used as is; one about to
    // expire is refreshed by a cheap authenticated call, which the client
    // does on its own.
    let fresh = client
        .current_token()
        .is_some_and(|t| token_seconds_left(&t).is_some_and(|left| left > 60));
    if !fresh {
        client.list_ledgers().await.map_err(|e| {
            CliError::Remote(format!(
                "doc.remote '{remote_name}': {e}\n  hint: fluree auth login --remote {remote_name}"
            ))
        })?;
        context::persist_refreshed_tokens(&client, &remote_name, &remote_dirs).await;
    }
    let token = client.current_token().ok_or_else(|| {
        CliError::Config(format!(
            "doc.remote '{remote_name}' has no stored login\n  hint: fluree auth login --remote {remote_name}"
        ))
    })?;
    Ok(config.fill_from_gateway(&gateway_base(client.base_url()), &token))
}

/// Seconds until a JWT's `exp`, read without verifying it: this decides
/// only whether to bother the server for a refresh. `None` for anything
/// that is not a JWT with a numeric `exp`.
fn token_seconds_left(token: &str) -> Option<i64> {
    use base64::Engine;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    let exp = claims.get("exp")?.as_i64()?;
    Some(exp - chrono::Utc::now().timestamp())
}

/// `https://stack/v1/fluree` (the CLI-compat base a remote is registered
/// with) → `https://stack/v1`, where the gateway's model routes live.
fn gateway_base(remote_url: &str) -> String {
    let trimmed = remote_url.trim_end_matches('/');
    let origin_v1 = trimmed.strip_suffix("/fluree").unwrap_or(trimmed);
    if origin_v1.ends_with("/v1") {
        origin_v1.to_string()
    } else {
        format!("{origin_v1}/v1")
    }
}

/// `name:branch` → (`name`, `branch`), defaulting the branch to `main`.
fn split_alias(alias: &str) -> (String, String) {
    match alias.split_once(':') {
        Some((n, b)) => (n.to_string(), b.to_string()),
        None => (alias.to_string(), "main".to_string()),
    }
}

fn vector_index_id(alias: &str) -> (String, String) {
    let (name, branch) = split_alias(alias);
    let index = format!("{name}-vectors");
    (format!("{index}:{branch}"), index)
}

fn text_index_id(alias: &str) -> (String, String) {
    let (name, branch) = split_alias(alias);
    let index = format!("{name}-text");
    (format!("{index}:{branch}"), index)
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn short_rev(rev: &str) -> &str {
    &rev[..rev.len().min(7)]
}

// ---------------------------------------------------------------------------
// ingest
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Totals {
    ingested: usize,
    skipped: usize,
    failed: usize,
    chunks: usize,
    pages: usize,
    escalated_crops: usize,
    cache_hits: usize,
    /// Documents that asked for more crops than the cap and landed
    /// deterministic-only.
    unescalated: usize,
    mentions: usize,
    entities_new: usize,
    relations: usize,
    relations_rejected: usize,
    hallucinated: usize,
    off_model: usize,
    off_model_dropped: usize,
    extraction_cache_hits: usize,
    chunks_failed: usize,
}

const DEFAULT_CONCURRENCY: usize = 4;

/// What `--model` and `--entities` set up, shared across documents.
struct Extraction {
    model: Option<Arc<Model>>,
    gazetteer: Option<Arc<Gazetteer>>,
    extractor: Option<Arc<Extractor>>,
    policy: ResolvePolicy,
    concurrency: usize,
    /// Changes when the ontology, the gazetteer, the model, the guidance,
    /// the relation mode or the language does.
    fingerprint: String,
    gazetteer_counts: Vec<(String, usize)>,
}

async fn setup_extraction(
    args: &DocIngestArgs,
    config: &DocConfig,
    cache: Option<DocCache>,
    target: Option<&Fluree>,
    alias: &str,
    dirs: &FlureeDir,
) -> CliResult<Option<Extraction>> {
    if args.no_extract || (args.model.is_none() && args.entities.is_empty()) {
        return Ok(None);
    }
    let mode = match args.relations {
        DocRelationMode::Direct => RelationMode::Direct,
        DocRelationMode::Reified => RelationMode::Reified,
        DocRelationMode::Off => RelationMode::Off,
    };
    let project = config.extraction.clone().unwrap_or_default();
    // A flag names a file where the command runs; a config value names one
    // in the project, so every run of the project makes the same ask.
    let project_root = dirs.config_dir().parent().map(Path::to_path_buf);
    let prompt_file = |flag: &Option<PathBuf>, configured: &Option<String>, what: &str| {
        let path = match (flag, configured) {
            (Some(p), _) => p.clone(),
            (None, Some(c)) => match &project_root {
                Some(root) if Path::new(c).is_relative() => root.join(c),
                _ => PathBuf::from(c),
            },
            (None, None) => return Ok(None),
        };
        std::fs::read_to_string(&path)
            .map(Some)
            .map_err(|e| CliError::Input(format!("{what} {}: {e}", path.display())))
    };
    let guidance = prompt_file(&args.guidance, &project.guidance, "--guidance")?;
    let system_template = prompt_file(
        &args.system_prompt,
        &project.system_prompt,
        "--system-prompt",
    )?;
    let user_template = prompt_file(&args.user_prompt, &project.user_prompt, "--user-prompt")?;
    let policy = ResolvePolicy {
        relations: mode,
        drop_off_model: args.drop_off_model || project.drop_off_model.unwrap_or(false),
    };
    let concurrency = args
        .concurrency
        .or(project.concurrency)
        .unwrap_or(DEFAULT_CONCURRENCY)
        .max(1);

    let model = match &args.model {
        Some(spec) => Some(Arc::new(
            doc_sources::load_model(&Source::parse(spec), dirs).await?,
        )),
        None => None,
    };
    let extractor = match (&model, &config.llm) {
        (Some(m), Some(endpoint)) => Some(Arc::new(Extractor::new(
            LlmClient::new(endpoint.clone()),
            m,
            cache,
            &ExtractOptions {
                relations: mode,
                guidance,
                system_template,
                user_template,
                drop_off_model: policy.drop_off_model,
            },
        )?)),
        (Some(_), None) => {
            return Err(CliError::Config(
                "--model needs a language model: set [doc.llm] (fluree config set doc.llm.url …) or doc.remote for a Fluree AI account".into(),
            ))
        }
        (None, _) => None,
    };

    let sources: Vec<Source> = args.entities.iter().map(|s| Source::parse(s)).collect();
    let loaded =
        doc_sources::load_gazetteer(&sources, target.map(|f| (f, alias)), &args.lang, dirs).await?;
    let gazetteer = (!loaded.gazetteer.is_empty()).then(|| Arc::new(loaded.gazetteer));
    let gazetteer_counts = loaded.counts;

    let fingerprint = fluree_db_doc::cache::sha256_hex(
        format!(
            "{}|{}|{:?}|{}|{}",
            extractor
                .as_ref()
                .map(|x| x.fingerprint())
                .unwrap_or_default(),
            loaded.fingerprint,
            mode,
            policy.drop_off_model,
            args.lang
        )
        .as_bytes(),
    );
    Ok(Some(Extraction {
        model,
        gazetteer,
        extractor,
        policy,
        concurrency,
        fingerprint,
        gazetteer_counts,
    }))
}

struct Extracted {
    graph: ExtractionGraph,
    cache_hits: usize,
    /// Chunks whose model call failed; they contributed gazetteer mentions
    /// only, and the document is not stamped as extracted.
    chunks_failed: usize,
    first_failure: Option<String>,
}

/// Scan every chunk for known entities, ask the language model about each,
/// and resolve the answers into nodes. Blocking: the model calls are
/// synchronous, like the crop reads, and `concurrency` of them run at once.
/// A chunk whose call fails contributes its gazetteer mentions and nothing
/// else; the run goes on and says so.
async fn run_extraction(
    setup: &Extraction,
    doc_iri: &str,
    entity_prefix: &str,
    chunks: Vec<Chunk>,
) -> CliResult<Extracted> {
    let gazetteer = setup.gazetteer.clone();
    let extractor = setup.extractor.clone();
    let model = setup.model.clone();
    let policy = setup.policy;
    let concurrency = setup.concurrency;
    let doc_iri = doc_iri.to_string();
    let entity_prefix = entity_prefix.to_string();
    tokio::task::spawn_blocking(move || -> CliResult<Extracted> {
        // `concurrency` workers pull chunks off one counter; a chunk whose
        // call fails contributes its gazetteer mentions and nothing else.
        type ChunkWork = (Vec<fluree_db_doc::Mention>, Option<ChunkExtraction>);
        type ChunkSlot = std::sync::Mutex<Option<(ChunkWork, Option<String>)>>;
        let results: Vec<ChunkSlot> = chunks.iter().map(|_| ChunkSlot::default()).collect();
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..concurrency.min(chunks.len().max(1)) {
                scope.spawn(|| loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let Some(chunk) = chunks.get(i) else { break };
                    let mentions = gazetteer
                        .as_ref()
                        .map(|g| g.scan(&chunk.text))
                        .unwrap_or_default();
                    let (extraction, failure) = match &extractor {
                        Some(x) => {
                            let existing = gazetteer
                                .as_ref()
                                .map(|g| g.existing_block(&mentions))
                                .unwrap_or_else(|| {
                                    "(none)
"
                                    .to_string()
                                });
                            match x.extract_chunk(&chunk.text, &existing) {
                                Ok(e) => (Some(e), None),
                                Err(e) => (None, Some(format!("chunk {i}: {e}"))),
                            }
                        }
                        None => (None, None),
                    };
                    *results[i].lock().expect("chunk slot") =
                        Some(((mentions, extraction), failure));
                });
            }
        });
        let mut cache_hits = 0;
        let mut chunks_failed = 0;
        let mut first_failure = None;
        let mut per_chunk: Vec<ChunkWork> = Vec::with_capacity(chunks.len());
        for slot in results {
            let (work, failure) = slot
                .into_inner()
                .expect("chunk slot")
                .expect("every chunk was visited");
            if let Some(f) = failure {
                chunks_failed += 1;
                tracing::warn!("{f}");
                first_failure.get_or_insert(f);
            }
            if work.1.as_ref().is_some_and(|e| e.from_cache) {
                cache_hits += 1;
            }
            per_chunk.push(work);
        }
        let inputs: Vec<ChunkInput<'_>> = chunks
            .iter()
            .zip(per_chunk.iter())
            .enumerate()
            .map(|(i, (chunk, (mentions, extraction)))| ChunkInput {
                chunk,
                chunk_iri: graph::chunk_iri(&doc_iri, i),
                mentions,
                extraction: extraction.as_ref(),
            })
            .collect();
        let graph = extract::resolve(
            &doc_iri,
            &entity_prefix,
            &inputs,
            gazetteer.as_deref(),
            model.as_deref(),
            policy,
        );
        Ok(Extracted {
            graph,
            cache_hits,
            chunks_failed,
            first_failure,
        })
    })
    .await
    .map_err(|e| CliError::Input(format!("extraction task failed: {e}")))?
}

/// A compact IRI from a query row, expanded with the prefixes the ingest
/// context knows, so a retraction names the same term the insert did.
fn expand_iri(iri: &str) -> String {
    for (prefix, ns) in fluree_db_doc::model::PREFIXES {
        if let Some(rest) = iri.strip_prefix(&format!("{prefix}:")) {
            return format!("{ns}{rest}");
        }
    }
    if let Some(rest) = iri.strip_prefix("doc:") {
        return format!("{}{rest}", vocab::DOC_NS);
    }
    iri.to_string()
}

/// The edges a document's previous extraction asserted. Called before the
/// retraction; after it, each is dropped unless another relation still
/// states it.
async fn asserted_edges(
    fluree: &Fluree,
    alias: &str,
    doc_iri: &str,
) -> CliResult<Vec<(String, String, String)>> {
    let rows = query_rows(fluree, alias, &graph::asserted_triples_query(doc_iri)).await?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            let row = row.as_array()?;
            let cell = |i: usize| row.get(i).and_then(Value::as_str).map(expand_iri);
            Some((cell(0)?, cell(1)?, cell(2)?))
        })
        .collect())
}

async fn drop_unsupported_edges(
    fluree: &Fluree,
    alias: &str,
    edges: &[(String, String, String)],
) -> CliResult<usize> {
    let mut orphaned = Vec::new();
    for (s, p, o) in edges {
        let support = query_rows(fluree, alias, &graph::relation_support_query(s, p, o)).await?;
        if support.is_empty() {
            orphaned.push((s.clone(), p.clone(), o.clone()));
        }
    }
    if !orphaned.is_empty() {
        fluree
            .graph(alias)
            .transact()
            .sparql_update(&graph::delete_triples_update(&orphaned))
            .commit()
            .await?;
    }
    Ok(orphaned.len())
}

async fn run_ingest(args: DocIngestArgs, dirs: &FlureeDir) -> CliResult<()> {
    let alias = context::resolve_ledger(args.ledger.as_deref(), dirs)?;
    let ledger_id = context::to_ledger_id(&alias);
    let config = resolve_config(dirs).await?;

    let inputs = collect_inputs(&args.paths)?;
    if inputs.is_empty() {
        return Err(CliError::Usage(
            "no supported documents found (pdf, md, html, docx, pptx, png, jpg)".into(),
        ));
    }

    let cache = (!args.no_cache).then(|| DocCache::new(cache_root(dirs)));
    let vlm = match (args.no_escalate, config.crop_reader()) {
        (false, Some(endpoint)) => Some(Arc::new(VlmReader::new(
            endpoint.clone(),
            cache.clone(),
            args.max_crops,
        )?)),
        _ => None,
    };
    let embedder = match (args.no_embed, &config.embedding) {
        (false, Some(endpoint)) => Some(EmbeddingClient::new(endpoint.clone())?),
        _ => None,
    };

    let fluree = if args.dry_run {
        None
    } else {
        let fluree = build_fluree(dirs)?;
        if !fluree.ledger_exists(&ledger_id).await? {
            fluree.create_ledger(&ledger_id).await?;
            eprintln!("{} created ledger {alias}", "→".dimmed());
        }
        Some(fluree)
    };
    let extraction =
        setup_extraction(&args, &config, cache.clone(), fluree.as_ref(), &alias, dirs).await?;

    announce(
        &alias,
        inputs.len(),
        &config,
        vlm.as_deref(),
        embedder.as_ref(),
        extraction.as_ref(),
        &args,
    );

    let ingest_opts = IngestOptions {
        iri_prefix: args.base_iri.clone(),
        chunk: ChunkConfig {
            min_chars: args.min_chars,
            max_chars: args.max_chars,
        },
        cache,
        vlm,
    };

    if let Some(out) = &args.out_dir {
        std::fs::create_dir_all(out)?;
    }

    let started = Instant::now();
    let mut totals = Totals::default();
    let mut dimensions: Option<usize> = None;
    let parser_revision = fluree_db_doc::parse::DOC_PARSE_REV.to_string();
    let chunking = format!("{}/{}", args.min_chars, args.max_chars);

    for (path, relative) in &inputs {
        let label = relative.as_str();

        // Skip what the ledger already holds from these same inputs, unless told otherwise.
        let sha = fluree_db_doc::cache::sha256_hex(&std::fs::read(path)?);
        let previous = match &fluree {
            Some(f) => {
                previous_ingest(
                    f,
                    &alias,
                    &fluree_db_doc::ingest::document_iri(&args.base_iri, relative),
                )
                .await?
            }
            None => None,
        };
        if !args.force {
            if let Some(prev) = &previous {
                let same_model = prev.embedding_model.as_deref()
                    == embedder.as_ref().map(EmbeddingClient::model);
                let same_extraction = prev.extraction_fingerprint.as_deref()
                    == extraction.as_ref().map(|x| x.fingerprint.as_str());
                // A document written before chunking was recorded is
                // treated as cut with these sizes.
                let same_chunking = prev.chunking.as_deref().is_none_or(|c| c == chunking);
                if prev.sha256 == sha
                    && prev.parser_revision == parser_revision
                    && same_model
                    && same_extraction
                    && same_chunking
                {
                    println!("  {} {label}  unchanged", "=".dimmed());
                    totals.skipped += 1;
                    continue;
                }
            }
        }

        let opts = ingest_opts.clone();
        let (p, r) = (path.clone(), relative.clone());
        let prepared = tokio::task::spawn_blocking(move || prepare(&p, &r, &opts))
            .await
            .map_err(|e| CliError::Input(format!("parse task failed: {e}")))?;
        let doc = match prepared {
            Ok(doc) => doc,
            Err(e) => {
                println!("  {} {label}  {e}", "✗".red());
                totals.failed += 1;
                continue;
            }
        };

        let embeddings = match &embedder {
            Some(client) if !doc.chunks.is_empty() => {
                let texts: Vec<String> = doc.chunks.iter().map(Chunk::embedding_input).collect();
                let vectors = client.embed(&texts).await?;
                if let Some(v) = vectors.first() {
                    dimensions = Some(v.len());
                }
                Some(vectors)
            }
            _ => None,
        };

        let extracted = match &extraction {
            Some(setup) => {
                match run_extraction(setup, &doc.doc_iri, &args.base_iri, doc.chunks.clone()).await
                {
                    Ok(x) => Some(x),
                    Err(e) => {
                        println!("  {} {label}  extraction: {e}", "✗".red());
                        totals.failed += 1;
                        continue;
                    }
                }
            }
            None => None,
        };

        let meta = DocumentMeta {
            doc_iri: doc.doc_iri.clone(),
            file_name: doc.meta.file_name.clone(),
            relative_path: doc.meta.relative_path.clone(),
            sha256: doc.meta.sha256.clone(),
            media_type: doc.meta.kind.media_type().to_string(),
            byte_size: doc.meta.byte_size,
            pages: doc.parsed.pages,
            escalated_crops: doc.parsed.escalated_crops,
            parser_revision: parser_revision.clone(),
            ingested_at: now_rfc3339(),
            chunking: chunking.clone(),
        };
        let embedding_stamp = embedder
            .as_ref()
            .zip(dimensions)
            .map(|(c, d)| (c.model(), d));
        // A document with a failed chunk is not stamped: the next run
        // extracts it again, with the cache answering for the chunks that
        // did succeed.
        let fully_extracted = extracted.as_ref().is_none_or(|x| x.chunks_failed == 0);
        let extraction_stamp = extraction
            .as_ref()
            .filter(|_| fully_extracted)
            .map(|setup| {
                let stats = extracted.as_ref().map(|x| &x.graph.stats);
                ExtractionStamp {
                    fingerprint: setup.fingerprint.clone(),
                    model: setup.extractor.as_ref().map(|x| x.model_name().to_string()),
                    mentions: stats.map_or(0, |s| s.gazetteer_mentions + s.llm_mentions),
                    entities: stats.map_or(0, |s| s.entities_known + s.entities_new),
                    relations: stats.map_or(0, |s| {
                        s.relations_valid + s.relations_repaired + s.relations_rejected
                    }),
                }
            });
        let document = graph::document_node(
            &meta,
            doc.chunks.len(),
            embedding_stamp,
            extraction_stamp.as_ref(),
        );
        let chunks = graph::chunk_nodes(&doc.doc_iri, &doc.chunks, embeddings.as_deref());
        let mut extra = Vec::new();
        if let Some(x) = &extracted {
            extra.extend(x.graph.nodes.iter().cloned());
            extra.extend(x.graph.direct.iter().cloned());
        }
        let tx = graph::transaction(&doc.parsed.doco, document, chunks, extra)?;

        if let Some(out) = &args.out_dir {
            let target = out.join(format!("{}.jsonld", relative.replace('/', "__")));
            std::fs::write(&target, serde_json::to_vec_pretty(&tx)?)?;
        }

        let mut commit_note = String::new();
        if let Some(f) = &fluree {
            let g = f.graph(&alias);
            if previous.is_some() {
                // Edges the earlier extraction wrote outlive its nodes only
                // while some other relation still supports them.
                let edges = asserted_edges(f, &alias, &doc.doc_iri).await?;
                g.transact()
                    .sparql_update(&graph::retract_update(&doc.doc_iri))
                    .commit()
                    .await?;
                drop_unsupported_edges(f, &alias, &edges).await?;
            }
            let result = g.transact().insert(&tx).commit().await?;
            commit_note = format!("  t={}", result.receipt.t);
        }

        let source = if doc.parsed.from_cache {
            totals.cache_hits += 1;
            "cached"
        } else {
            "parsed"
        };
        let pages = if doc.parsed.pages > 0 {
            format!("{}p, ", doc.parsed.pages)
        } else {
            String::new()
        };
        let crops = if doc.parsed.escalated_crops > 0 {
            format!(", {} crop(s) read", doc.parsed.escalated_crops)
        } else {
            String::new()
        };
        let embedded = if embeddings.is_some() {
            ", embedded"
        } else {
            ""
        };
        let extraction_note = match &extracted {
            Some(x) => {
                let s = &x.graph.stats;
                let mentions = s.gazetteer_mentions + s.llm_mentions;
                let mut note = format!(
                    ", {mentions} mention(s) of {} entit{} ({} new{})",
                    s.entities_known + s.entities_new,
                    if s.entities_known + s.entities_new == 1 {
                        "y"
                    } else {
                        "ies"
                    },
                    s.entities_new,
                    if s.off_model > 0 {
                        format!(", {} off-model", s.off_model)
                    } else {
                        String::new()
                    }
                );
                let relations = s.relations_valid + s.relations_repaired + s.relations_rejected;
                if relations > 0 {
                    note.push_str(&format!(", {relations} relation(s)"));
                    if s.relations_rejected > 0 {
                        note.push_str(&format!(" ({} rejected)", s.relations_rejected));
                    }
                }
                totals.mentions += mentions;
                totals.entities_new += s.entities_new;
                totals.relations += relations;
                totals.relations_rejected += s.relations_rejected;
                totals.hallucinated += s.hallucinated;
                if x.chunks_failed > 0 {
                    note.push_str(&format!(", {} chunk(s) not extracted", x.chunks_failed));
                }
                totals.off_model += s.off_model;
                totals.off_model_dropped += s.off_model_dropped;
                totals.extraction_cache_hits += x.cache_hits;
                totals.chunks_failed += x.chunks_failed;
                note
            }
            None => String::new(),
        };
        println!(
            "  {} {label}  {source}: {pages}{} elements, {} chunks{crops}{embedded}{extraction_note}{commit_note}",
            "✓".green(),
            doc.parsed.elements,
            doc.chunks.len()
        );
        if let Some(why) = extracted.as_ref().and_then(|x| x.first_failure.as_ref()) {
            println!(
                "    {} extraction incomplete, will be retried next run: {why}",
                "!".yellow()
            );
        }
        if let Some(why) = &doc.parsed.escalation_skipped {
            println!(
                "    {} deterministic tier only: {why}; raise --max-crops to read them",
                "!".yellow()
            );
            totals.unescalated += 1;
        }
        totals.ingested += 1;
        totals.chunks += doc.chunks.len();
        totals.pages += doc.parsed.pages;
        totals.escalated_crops += doc.parsed.escalated_crops;
    }

    if let (Some(f), false) = (&fluree, args.no_index) {
        if totals.ingested > 0 {
            ensure_indexes(f, &alias, dimensions).await?;
        } else if totals.skipped > 0 {
            sync_indexes_if_present(f, &alias).await?;
        }
    }

    let elapsed = started.elapsed();
    println!();
    println!(
        "{} {} ingested, {} unchanged, {} failed — {} chunks, {} pages, {} crop(s) read, {} parse(s) from cache, {:.1}s{}",
        if totals.failed == 0 { "done:".green() } else { "done with errors:".yellow() },
        totals.ingested,
        totals.skipped,
        totals.failed,
        totals.chunks,
        totals.pages,
        totals.escalated_crops,
        totals.cache_hits,
        elapsed.as_secs_f64(),
        if args.dry_run { " (dry run, nothing written)" } else { "" }
    );
    if extraction.is_some() && totals.ingested > 0 {
        println!(
            "  extraction: {} mention(s), {} new entit{} ({} off-model), {} relation(s) ({} rejected), {} dropped ({} hallucinated, {} off-model), {} chunk(s) from cache{}",
            totals.mentions,
            totals.entities_new,
            if totals.entities_new == 1 { "y" } else { "ies" },
            totals.off_model,
            totals.relations,
            totals.relations_rejected,
            totals.hallucinated + totals.off_model_dropped,
            totals.hallucinated,
            totals.off_model_dropped,
            totals.extraction_cache_hits,
            if totals.chunks_failed > 0 {
                format!(", {} chunk(s) failed", totals.chunks_failed)
            } else {
                String::new()
            }
        );
    }
    if totals.unescalated > 0 {
        eprintln!(
            "note: {} document(s) exceeded the crop cap and were parsed without the vision model",
            totals.unescalated
        );
    }
    if totals.failed > 0 {
        return Err(CliError::ExitCode(1));
    }
    Ok(())
}

fn announce(
    alias: &str,
    count: usize,
    config: &DocConfig,
    vlm: Option<&VlmReader>,
    embedder: Option<&EmbeddingClient>,
    extraction: Option<&Extraction>,
    args: &DocIngestArgs,
) {
    eprintln!("ingest {count} document(s) → {alias}");
    if let Some(remote) = &config.remote {
        eprintln!("  account    {remote} (Fluree AI gateway supplies unset model slots)");
    }
    eprintln!(
        "  parser     fluree-doc-parse {}",
        short_rev(fluree_db_doc::parse::DOC_PARSE_REV)
    );
    match vlm {
        Some(v) => eprintln!("  escalation {} (crops the parser cannot read)", v.model()),
        None if args.no_escalate => eprintln!("  escalation off (--no-escalate)"),
        None => eprintln!(
            "  escalation none — deterministic tier only; set [doc.vlm] to read scanned pages"
        ),
    }
    match embedder {
        Some(e) => eprintln!("  embedding  {}", e.model()),
        None if args.no_embed => eprintln!("  embedding  off (--no-embed)"),
        None if config.embedding.is_none() => {
            eprintln!("  embedding  none — set [doc.embedding] to enable vector search");
        }
        None => {}
    }
    match extraction {
        Some(x) => {
            if let Some(m) = &x.model {
                eprintln!(
                    "  model      {} ({} classes, {} properties)",
                    args.model.as_deref().unwrap_or(""),
                    m.classes().len(),
                    m.properties().len()
                );
            }
            match &x.extractor {
                Some(e) => eprintln!(
                    "  extraction {} (relations: {}; off-model entities: {}; {} chunk(s) at once)",
                    e.model_name(),
                    match x.policy.relations {
                        RelationMode::Direct => "reified + direct edges",
                        RelationMode::Reified => "reified only",
                        RelationMode::Off => "off",
                    },
                    if x.policy.drop_off_model {
                        "dropped"
                    } else {
                        "kept, flagged"
                    },
                    x.concurrency
                ),
                None => eprintln!(
                    "  extraction gazetteer scan only; add --model for the language model"
                ),
            }
            for (label, n) in &x.gazetteer_counts {
                eprintln!("  entities   {label}: {n} label(s)");
            }
            if let Some(g) = &x.gazetteer {
                eprintln!(
                    "  gazetteer  {} entit{}",
                    g.entries().len(),
                    if g.entries().len() == 1 { "y" } else { "ies" }
                );
            }
        }
        None if args.no_extract => eprintln!("  extraction off (--no-extract)"),
        None => {}
    }
    if args.no_cache {
        eprintln!("  cache      off");
    }
}

struct PreviousIngest {
    sha256: String,
    parser_revision: String,
    embedding_model: Option<String>,
    extraction_fingerprint: Option<String>,
    chunking: Option<String>,
}

async fn previous_ingest(
    fluree: &Fluree,
    alias: &str,
    doc_iri: &str,
) -> CliResult<Option<PreviousIngest>> {
    let rows = query_rows(fluree, alias, &graph::exists_query(doc_iri)).await?;
    let Some(row) = rows.first().and_then(Value::as_array) else {
        return Ok(None);
    };
    let text = |i: usize| row.get(i).and_then(Value::as_str).map(str::to_string);
    Ok(Some(PreviousIngest {
        sha256: text(0).unwrap_or_default(),
        parser_revision: text(1).unwrap_or_default(),
        embedding_model: text(2),
        extraction_fingerprint: text(3),
        chunking: text(4),
    }))
}

/// Run a JSON-LD select against the ledger head and return its rows.
async fn query_rows(fluree: &Fluree, alias: &str, query: &Value) -> CliResult<Vec<Value>> {
    let view = fluree.db_with_default_context(alias).await?;
    let result = fluree.query(&view, query).await?;
    let json = result.to_jsonld(&view.snapshot)?;
    Ok(json.as_array().cloned().unwrap_or_default())
}

async fn ensure_indexes(fluree: &Fluree, alias: &str, dimensions: Option<usize>) -> CliResult<()> {
    let (text_id, text_name) = text_index_id(alias);
    if graph_source_present(fluree, &text_id).await? {
        let r = fluree.sync_bm25_index(&text_id).await?;
        println!(
            "  {} full-text index {text_id}: +{} −{} chunk(s)",
            "⟳".dimmed(),
            r.upserted,
            r.removed
        );
    } else {
        let query = json!({
            "@context": { "doc": vocab::DOC_NS },
            "where": [{ "@id": "?c", "@type": vocab::CHUNK }],
            "select": { "?c": ["@id", vocab::TEXT, vocab::HEADER_PATH] }
        });
        let config =
            Bm25CreateConfig::new(&text_name, alias, query).with_branch(split_alias(alias).1);
        let r = fluree.create_full_text_index(config).await?;
        println!(
            "  {} full-text index {}: {} chunk(s), {} terms",
            "+".green(),
            r.graph_source_id,
            r.doc_count,
            r.term_count
        );
    }

    let (vec_id, vec_name) = vector_index_id(alias);
    let mut present = graph_source_present(fluree, &vec_id).await?;
    // An index built for another embedding model has the wrong width:
    // vectors of a new size cannot be synced into it, so it is rebuilt.
    if let (true, Some(dims)) = (present, dimensions) {
        if let Some(existing) = index_dimensions(fluree, &vec_id).await? {
            if existing != dims {
                fluree.drop_vector_index(&vec_id).await?;
                println!(
                    "  {} vector index {vec_id}: rebuilt, embeddings changed from {existing} to {dims} dims",
                    "×".yellow()
                );
                present = false;
            }
        }
    }
    if present {
        let r = fluree.sync_vector_index(&vec_id).await?;
        println!(
            "  {} vector index {vec_id}: +{} −{} vector(s)",
            "⟳".dimmed(),
            r.upserted,
            r.removed
        );
    } else if let Some(dims) = dimensions {
        let query = json!({
            "@context": { "doc": vocab::DOC_NS },
            "where": [{ "@id": "?c", "@type": vocab::CHUNK }],
            "select": { "?c": ["@id", vocab::EMBEDDING] }
        });
        let config = fluree_db_api::VectorCreateConfig::new(
            &vec_name,
            alias,
            query,
            vocab::embedding_iri(),
            dims,
        )
        .with_branch(split_alias(alias).1)
        .with_metric(fluree_db_query::vector::DistanceMetric::Cosine);
        let r = fluree.create_vector_index(config).await?;
        println!(
            "  {} vector index {}: {} vector(s), {} dims",
            "+".green(),
            r.graph_source_id,
            r.vector_count,
            r.dimensions
        );
    }
    Ok(())
}

/// Nothing new was written, but earlier runs may have left indexes behind
/// a ledger that moved on.
async fn sync_indexes_if_present(fluree: &Fluree, alias: &str) -> CliResult<()> {
    let (text_id, _) = text_index_id(alias);
    if graph_source_present(fluree, &text_id).await? {
        fluree.sync_bm25_index(&text_id).await?;
    }
    let (vec_id, _) = vector_index_id(alias);
    if graph_source_present(fluree, &vec_id).await? {
        fluree.sync_vector_index(&vec_id).await?;
    }
    Ok(())
}

/// The width a vector index was built for, from its published config.
async fn index_dimensions(fluree: &Fluree, id: &str) -> CliResult<Option<usize>> {
    let Some(record) = fluree.nameservice().lookup_graph_source(id).await? else {
        return Ok(None);
    };
    let config: Value = serde_json::from_str(&record.config).unwrap_or(Value::Null);
    Ok(config
        .get("dimensions")
        .and_then(Value::as_u64)
        .map(|d| d as usize))
}

async fn graph_source_present(fluree: &Fluree, id: &str) -> CliResult<bool> {
    Ok(fluree
        .nameservice()
        .lookup_graph_source(id)
        .await?
        .is_some_and(|r| !r.retracted))
}

// ---------------------------------------------------------------------------
// search
// ---------------------------------------------------------------------------

async fn run_search(args: DocSearchArgs, dirs: &FlureeDir) -> CliResult<()> {
    let alias = context::resolve_ledger(args.ledger.as_deref(), dirs)?;
    let fluree = build_fluree(dirs)?;

    let (vec_id, _) = vector_index_id(&alias);
    let (text_id, _) = text_index_id(&alias);
    let has_vectors = graph_source_present(&fluree, &vec_id).await?;
    let has_text = graph_source_present(&fluree, &text_id).await?;
    // Whether the query could be embedded is known without the network: a
    // slot, or an account that would fill one. The account is only asked
    // for its token when a vector lane actually runs, so a text search
    // never waits on it.
    let local = crate::config::read_doc_config(dirs.config_dir())?.with_env();
    let can_embed = local.embedding.is_some() || local.remote.is_some();

    // Everything available, unless told otherwise: both indexes fused when
    // the query can be embedded, else whichever one there is.
    let mode = match args.mode {
        DocSearchMode::Auto if can_embed && has_vectors && has_text => DocSearchMode::Hybrid,
        DocSearchMode::Auto if can_embed && has_vectors => DocSearchMode::Vector,
        DocSearchMode::Auto => DocSearchMode::Text,
        m => m,
    };
    let wants_vectors = matches!(mode, DocSearchMode::Vector | DocSearchMode::Hybrid);
    let wants_text = matches!(mode, DocSearchMode::Text | DocSearchMode::Hybrid);
    if wants_vectors && !can_embed {
        return Err(CliError::Config(format!(
            "{} search needs `[doc.embedding]` configured to embed the query (or use --mode text)",
            mode_label(mode)
        )));
    }
    if wants_vectors && !has_vectors {
        return Err(CliError::NotFound(format!(
            "no vector index {vec_id}; run `fluree doc ingest` with an embedding endpoint configured (or use --mode text)"
        )));
    }
    if wants_text && !has_text {
        return Err(CliError::NotFound(format!(
            "no full-text index {text_id}; run `fluree doc ingest` first"
        )));
    }
    let config = if wants_vectors {
        resolve_config(dirs).await?
    } else {
        local
    };
    if wants_vectors && config.embedding.is_none() {
        return Err(CliError::Config(format!(
            "{} search needs `[doc.embedding]` configured to embed the query (or use --mode text)",
            mode_label(mode)
        )));
    }

    let started = Instant::now();
    // Each method is asked for more than the final count, so a hit that
    // one method ranks low and the other high still has a chance to fuse
    // upward.
    let per_method = if mode == DocSearchMode::Hybrid {
        args.limit * 3
    } else {
        args.limit
    };
    // The two lanes are independent, and hybrid should cost the slower
    // one, not their sum.
    let vector_lane = async {
        if !wants_vectors {
            return Ok::<Vec<Hit>, CliError>(Vec::new());
        }
        let endpoint = config.embedding.as_ref().expect("checked above");
        let client = EmbeddingClient::new(endpoint.clone())?;
        let vector = client
            .embed(std::slice::from_ref(&args.query))
            .await?
            .pop()
            .ok_or_else(|| CliError::Input("embedding endpoint returned no vector".into()))?;
        let pattern = json!({
            "f:graphSource": vec_id,
            "f:queryVector": vector,
            "f:searchLimit": per_method,
            "f:searchResult": { "f:resultId": "?c", "f:resultScore": "?score" }
        });
        search_hits(&fluree, &alias, pattern).await
    };
    let text_lane = async {
        if !wants_text {
            return Ok::<Vec<Hit>, CliError>(Vec::new());
        }
        let pattern = json!({
            "f:graphSource": text_id,
            "f:searchText": args.query,
            "f:searchLimit": per_method,
            "f:searchResult": { "f:resultId": "?c", "f:resultScore": "?score" }
        });
        search_hits(&fluree, &alias, pattern).await
    };
    let (vector_hits, text_hits) = tokio::try_join!(vector_lane, text_lane)?;
    let hits = match mode {
        DocSearchMode::Hybrid => fuse_hits(vector_hits, text_hits, args.limit),
        DocSearchMode::Vector => vector_hits,
        _ => text_hits,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&hits)?);
        return Ok(());
    }
    let label = mode_label(mode);
    if hits.is_empty() {
        println!(
            "no matches ({label}, {:.0} ms)",
            started.elapsed().as_secs_f64() * 1000.0
        );
        return Ok(());
    }
    for (rank, hit) in hits.iter().enumerate() {
        println!(
            "{} {}  {}{}{}",
            format!("{:>2}.", rank + 1).bold(),
            format!("{:.3}", hit.score).dimmed(),
            hit.file.cyan(),
            if hit.path.is_empty() {
                String::new()
            } else {
                format!("  {}", hit.path.dimmed())
            },
            if hit.ranks.is_empty() {
                String::new()
            } else {
                format!("  {}", hit.ranks.dimmed())
            }
        );
        println!("    {}", snippet(&hit.text, 240));
        println!("    {}", hit.chunk.dimmed());
    }
    eprintln!(
        "({} result(s), {label}, {:.0} ms)",
        hits.len(),
        started.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}

fn mode_label(mode: DocSearchMode) -> &'static str {
    match mode {
        DocSearchMode::Vector => "vector",
        DocSearchMode::Hybrid => "hybrid",
        _ => "text",
    }
}

/// One search hit, with the chunk it names joined to its file and section.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
struct Hit {
    score: f64,
    chunk: String,
    document: String,
    file: String,
    path: String,
    text: String,
    /// Where each method ranked it, for a fused result: `vector #1 · text #4`.
    #[serde(skip_serializing_if = "String::is_empty")]
    ranks: String,
}

/// Run one index search pattern and join each hit to its chunk, in score
/// order.
async fn search_hits(fluree: &Fluree, alias: &str, pattern: Value) -> CliResult<Vec<Hit>> {
    let query = json!({
        "@context": { "doc": vocab::DOC_NS, "f": vocab::FLUREE_NS },
        "where": [
            pattern,
            { "@id": "?c", vocab::TEXT: "?text", vocab::SOURCE_DOCUMENT: "?d" },
            ["optional", { "@id": "?c", vocab::HEADER_PATH: "?path" }],
            ["optional", { "@id": "?d", vocab::RELATIVE_PATH: "?file" }]
        ],
        "select": ["?score", "?c", "?d", "?file", "?path", "?text"],
        "orderBy": [["desc", "?score"]]
    });
    let rows = query_rows(fluree, alias, &query).await?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            let row = row.as_array()?;
            let text = |i: usize| row.get(i).and_then(Value::as_str).unwrap_or("").to_string();
            let document = text(2);
            Some(Hit {
                score: row.first().and_then(Value::as_f64).unwrap_or(0.0),
                chunk: text(1),
                file: if row.get(3).and_then(Value::as_str).is_some() {
                    text(3)
                } else {
                    document.clone()
                },
                document,
                path: text(4),
                text: text(5),
                ranks: String::new(),
            })
        })
        .collect())
}

/// The two methods score on different scales: a cosine similarity sits in
/// 0..1 and 0.9 is very high, while a BM25 weight is a sum of term weights
/// that grows with the query and the corpus, where 3 is weak and 35 is
/// strong. Fusion by rank alone would credit a weak first place as much as
/// a strong one, so each score is first put on a common 0..1 confidence:
/// cosine as it is, BM25 as `s / (s + 10)`, which reads 3 as 0.23, 10 as
/// 0.5, 24 as 0.71 and 35 as 0.78. A chunk's fused score is the mean of
/// its confidences over both methods, a method that did not find it
/// counting as 0, so agreement wins and a strong single-method hit still
/// surfaces above a weak agreed one.
const TEXT_HALF_CONFIDENCE: f64 = 10.0;

fn text_confidence(bm25: f64) -> f64 {
    let s = bm25.max(0.0);
    s / (s + TEXT_HALF_CONFIDENCE)
}

fn vector_confidence(cosine: f64) -> f64 {
    cosine.clamp(0.0, 1.0)
}

fn fuse_hits(vector: Vec<Hit>, text: Vec<Hit>, limit: usize) -> Vec<Hit> {
    struct Fused {
        hit: Hit,
        vector: Option<(usize, f64)>,
        text: Option<(usize, f64)>,
    }
    let mut fused: Vec<Fused> = Vec::new();
    for (list, from_vector) in [(vector, true), (text, false)] {
        for (i, hit) in list.into_iter().enumerate() {
            let placed = (i + 1, hit.score);
            match fused.iter_mut().find(|f| f.hit.chunk == hit.chunk) {
                Some(f) => {
                    if from_vector {
                        f.vector = Some(placed);
                    } else {
                        f.text = Some(placed);
                    }
                }
                None => fused.push(Fused {
                    hit,
                    vector: from_vector.then_some(placed),
                    text: (!from_vector).then_some(placed),
                }),
            }
        }
    }
    let score = |f: &Fused| {
        let v = f.vector.map_or(0.0, |(_, s)| vector_confidence(s));
        let t = f.text.map_or(0.0, |(_, s)| text_confidence(s));
        f64::midpoint(v, t)
    };
    fused.sort_by(|a, b| {
        score(b)
            .partial_cmp(&score(a))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    fused
        .into_iter()
        .take(limit)
        .map(|f| {
            let fused_score = score(&f);
            let mut hit = f.hit;
            hit.score = fused_score;
            hit.ranks = [
                f.vector.map(|(r, s)| format!("vector {s:.2} #{r}")),
                f.text.map(|(r, s)| format!("text {s:.1} #{r}")),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" · ");
            hit
        })
        .collect()
}

fn snippet(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        return flat;
    }
    let cut: String = flat.chars().take(max).collect();
    format!("{}…", cut.trim_end())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(chunk: &str, score: f64) -> Hit {
        Hit {
            score,
            chunk: chunk.into(),
            document: "d".into(),
            file: "f".into(),
            path: String::new(),
            text: String::new(),
            ranks: String::new(),
        }
    }

    #[test]
    fn fusion_weighs_calibrated_scores_not_ranks() {
        // Vector ranks a (0.9), b (0.6), c (0.5); text ranks c (35), a (3),
        // d (24). `c` is first for text with a strong score and third for
        // vector; `a` is first for vector and second for text but its text
        // score is weak; `d` is a strong text-only hit, `b` a middling
        // vector-only one.
        let vector = vec![hit("a", 0.9), hit("b", 0.6), hit("c", 0.5)];
        let text = vec![hit("c", 35.0), hit("a", 3.0), hit("d", 24.0)];
        let fused = fuse_hits(vector, text, 4);
        let order: Vec<&str> = fused.iter().map(|h| h.chunk.as_str()).collect();
        assert_eq!(order, vec!["c", "a", "d", "b"]);
        assert!((fused[0].score - f64::midpoint(0.5, 35.0 / 45.0)).abs() < 1e-12);
        assert_eq!(fused[0].ranks, "vector 0.50 #3 · text 35.0 #1");
        assert_eq!(fused[2].ranks, "text 24.0 #3");
        // A weak BM25 first place is not worth a strong cosine.
        assert!(text_confidence(3.0) < vector_confidence(0.6));
        assert!(text_confidence(35.0) > 0.75);
    }
}
