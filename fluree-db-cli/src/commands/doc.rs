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

use crate::cli::{DocAction, DocIngestArgs, DocSearchArgs, DocSearchMode};
use crate::context::{self, build_fluree};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{Bm25CreateConfig, Fluree};
use fluree_db_doc::graph::{self, DocumentMeta};
use fluree_db_doc::{
    collect_inputs, prepare, vocab, Chunk, ChunkConfig, DocCache, DocConfig, EmbeddingClient,
    IngestOptions, VlmReader,
};
use serde_json::{json, Value};
use std::path::PathBuf;
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
}

async fn run_ingest(args: DocIngestArgs, dirs: &FlureeDir) -> CliResult<()> {
    let alias = context::resolve_ledger(args.ledger.as_deref(), dirs)?;
    let ledger_id = context::to_ledger_id(&alias);
    let config = crate::config::read_doc_config(dirs.config_dir()).with_env();

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

    announce(
        &alias,
        inputs.len(),
        &config,
        vlm.as_deref(),
        embedder.as_ref(),
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
    if let Some(out) = &args.out_dir {
        std::fs::create_dir_all(out)?;
    }

    let started = Instant::now();
    let mut totals = Totals::default();
    let mut dimensions: Option<usize> = None;
    let parser_revision = fluree_db_doc::parse::DOC_PARSE_REV.to_string();

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
                if prev.sha256 == sha && prev.parser_revision == parser_revision && same_model {
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
        };
        let embedding_stamp = embedder
            .as_ref()
            .zip(dimensions)
            .map(|(c, d)| (c.model(), d));
        let document = graph::document_node(&meta, doc.chunks.len(), embedding_stamp);
        let chunks = graph::chunk_nodes(&doc.doc_iri, &doc.chunks, embeddings.as_deref());
        let tx = graph::transaction(&doc.parsed.doco, document, chunks)?;

        if let Some(out) = &args.out_dir {
            let target = out.join(format!("{}.jsonld", relative.replace('/', "__")));
            std::fs::write(&target, serde_json::to_vec_pretty(&tx)?)?;
        }

        let mut commit_note = String::new();
        if let Some(f) = &fluree {
            let g = f.graph(&alias);
            if previous.is_some() {
                g.transact()
                    .sparql_update(&graph::retract_update(&doc.doc_iri))
                    .commit()
                    .await?;
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
        println!(
            "  {} {label}  {source}: {pages}{} elements, {} chunks{crops}{embedded}{commit_note}",
            "✓".green(),
            doc.parsed.elements,
            doc.chunks.len()
        );
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
    args: &DocIngestArgs,
) {
    eprintln!("ingest {count} document(s) → {alias}");
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
    if args.no_cache {
        eprintln!("  cache      off");
    }
}

struct PreviousIngest {
    sha256: String,
    parser_revision: String,
    embedding_model: Option<String>,
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
    if graph_source_present(fluree, &vec_id).await? {
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
    let config = crate::config::read_doc_config(dirs.config_dir()).with_env();
    let fluree = build_fluree(dirs)?;

    let mode = match args.mode {
        DocSearchMode::Auto if config.embedding.is_some() => DocSearchMode::Vector,
        DocSearchMode::Auto => DocSearchMode::Text,
        m => m,
    };

    let search_pattern = match mode {
        DocSearchMode::Vector => {
            let Some(endpoint) = &config.embedding else {
                return Err(CliError::Config(
                    "vector search needs `[doc.embedding]` configured (or use --mode text)".into(),
                ));
            };
            let (vec_id, _) = vector_index_id(&alias);
            if !graph_source_present(&fluree, &vec_id).await? {
                return Err(CliError::NotFound(format!(
                    "no vector index {vec_id}; run `fluree doc ingest` with an embedding endpoint configured"
                )));
            }
            let client = EmbeddingClient::new(endpoint.clone())?;
            let vector = client
                .embed(std::slice::from_ref(&args.query))
                .await?
                .pop()
                .ok_or_else(|| CliError::Input("embedding endpoint returned no vector".into()))?;
            json!({
                "f:graphSource": vec_id,
                "f:queryVector": vector,
                "f:searchLimit": args.limit,
                "f:searchResult": { "f:resultId": "?c", "f:resultScore": "?score" }
            })
        }
        DocSearchMode::Text | DocSearchMode::Auto => {
            let (text_id, _) = text_index_id(&alias);
            if !graph_source_present(&fluree, &text_id).await? {
                return Err(CliError::NotFound(format!(
                    "no full-text index {text_id}; run `fluree doc ingest` first"
                )));
            }
            json!({
                "f:graphSource": text_id,
                "f:searchText": args.query,
                "f:searchLimit": args.limit,
                "f:searchResult": { "f:resultId": "?c", "f:resultScore": "?score" }
            })
        }
    };

    let query = json!({
        "@context": { "doc": vocab::DOC_NS, "f": vocab::FLUREE_NS },
        "where": [
            search_pattern,
            { "@id": "?c", vocab::TEXT: "?text", vocab::SOURCE_DOCUMENT: "?d" },
            ["optional", { "@id": "?c", vocab::HEADER_PATH: "?path" }],
            ["optional", { "@id": "?d", vocab::RELATIVE_PATH: "?file" }]
        ],
        "select": ["?score", "?c", "?d", "?file", "?path", "?text"],
        "orderBy": [["desc", "?score"]]
    });

    let started = Instant::now();
    let rows = query_rows(&fluree, &alias, &query).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }

    let mode_label = match mode {
        DocSearchMode::Vector => "vector",
        _ => "text",
    };
    if rows.is_empty() {
        println!(
            "no matches ({mode_label}, {:.0} ms)",
            started.elapsed().as_secs_f64() * 1000.0
        );
        return Ok(());
    }
    for (rank, row) in rows.iter().enumerate() {
        let Some(row) = row.as_array() else { continue };
        let score = row.first().and_then(Value::as_f64).unwrap_or(0.0);
        let chunk = row.get(1).and_then(Value::as_str).unwrap_or("");
        let doc = row.get(2).and_then(Value::as_str).unwrap_or("");
        let file = row.get(3).and_then(Value::as_str).unwrap_or(doc);
        let path = row.get(4).and_then(Value::as_str).unwrap_or("");
        let text = row.get(5).and_then(Value::as_str).unwrap_or("");
        println!(
            "{} {}  {}{}",
            format!("{:>2}.", rank + 1).bold(),
            format!("{score:.3}").dimmed(),
            file.cyan(),
            if path.is_empty() {
                String::new()
            } else {
                format!("  {}", path.dimmed())
            }
        );
        println!("    {}", snippet(text, 240));
        println!("    {}", chunk.dimmed());
    }
    eprintln!(
        "({} result(s), {mode_label}, {:.0} ms)",
        rows.len(),
        started.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}

fn snippet(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        return flat;
    }
    let cut: String = flat.chars().take(max).collect();
    format!("{}…", cut.trim_end())
}
