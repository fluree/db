//! From a path on disk to a prepared document: parse (cached), then chunk.
//!
//! Embedding and the ledger write happen in the caller, which owns the async
//! runtime and the database; everything here is synchronous and CPU-bound,
//! suited to `spawn_blocking`.

use crate::cache::{sha256_hex, DocCache};
use crate::chunk::{chunk_doco, Chunk, ChunkConfig};
use crate::escalate::VlmReader;
use crate::parse::{parse_bytes, ParseOptions, ParsedDocument, SourceKind};
use crate::{DocError, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Default IRI prefix for documents when the caller names none.
pub const DEFAULT_IRI_PREFIX: &str = "urn:fluree:doc:";

#[derive(Debug, Clone)]
pub struct IngestOptions {
    /// Prefix the document's relative path is appended to, forming its IRI.
    pub iri_prefix: String,
    pub chunk: ChunkConfig,
    pub cache: Option<DocCache>,
    pub vlm: Option<Arc<VlmReader>>,
}

/// The file behind a document, as the ledger will record it.
#[derive(Debug, Clone)]
pub struct SourceMeta {
    pub path: PathBuf,
    /// Path relative to the ingest root, `/`-separated — the stable part of
    /// the document IRI.
    pub relative_path: String,
    pub file_name: String,
    pub sha256: String,
    pub kind: SourceKind,
    pub byte_size: u64,
}

#[derive(Debug)]
pub struct PreparedDocument {
    pub meta: SourceMeta,
    pub doc_iri: String,
    pub parsed: ParsedDocument,
    pub chunks: Vec<Chunk>,
}

/// One entry per supported file under the given paths. A directory is
/// walked (hidden entries skipped) and its files are relative to it; a file
/// given directly is relative to its parent. Sorted, deduplicated.
pub fn collect_inputs(paths: &[PathBuf]) -> Result<Vec<(PathBuf, String)>> {
    let mut out: Vec<(PathBuf, String)> = Vec::new();
    for path in paths {
        if path.is_dir() {
            for entry in walkdir::WalkDir::new(path)
                .follow_links(true)
                .sort_by_file_name()
                .into_iter()
                .filter_entry(|e| !is_hidden(e.path()))
            {
                let entry = entry.map_err(|e| DocError::Io(e.to_string()))?;
                if !entry.file_type().is_file() || SourceKind::from_path(entry.path()).is_none() {
                    continue;
                }
                let rel = entry
                    .path()
                    .strip_prefix(path)
                    .unwrap_or(entry.path())
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                out.push((entry.path().to_path_buf(), rel));
            }
        } else if path.is_file() {
            if SourceKind::from_path(path).is_none() {
                return Err(DocError::Unsupported(path.display().to_string()));
            }
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            out.push((path.clone(), name));
        } else {
            return Err(DocError::Io(format!(
                "{}: no such file or directory",
                path.display()
            )));
        }
    }
    out.sort_by(|a, b| a.1.cmp(&b.1));
    out.dedup_by(|a, b| a.1 == b.1);
    Ok(out)
}

fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with('.'))
}

/// `{prefix}{relative path}`, with the path percent-encoded so the result is
/// one IRI whatever the file was called. `/` is kept: it is the hierarchy.
pub fn document_iri(prefix: &str, relative_path: &str) -> String {
    let mut out = String::with_capacity(prefix.len() + relative_path.len());
    out.push_str(prefix);
    for b in relative_path.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub fn prepare(path: &Path, relative_path: &str, opts: &IngestOptions) -> Result<PreparedDocument> {
    let kind = SourceKind::from_path(path)
        .ok_or_else(|| DocError::Unsupported(path.display().to_string()))?;
    let data = std::fs::read(path).map_err(|e| DocError::Io(format!("{}: {e}", path.display())))?;
    let meta = SourceMeta {
        path: path.to_path_buf(),
        relative_path: relative_path.to_string(),
        file_name: path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        sha256: sha256_hex(&data),
        kind,
        byte_size: data.len() as u64,
    };
    let doc_iri = document_iri(&opts.iri_prefix, relative_path);
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "document".into());

    let parse_opts = ParseOptions {
        base_iri: doc_iri.clone(),
        doc_iri: doc_iri.clone(),
        vlm: opts.vlm.clone(),
    };
    let fingerprint = parse_opts.fingerprint();
    let parsed = match opts
        .cache
        .as_ref()
        .and_then(|c| c.load_parse(&meta.sha256, &fingerprint))
    {
        Some(hit) => hit,
        None => {
            let parsed = parse_bytes(data, kind, &stem, &parse_opts)?;
            if let Some(cache) = &opts.cache {
                cache.store_parse(&meta.sha256, &fingerprint, &parsed)?;
            }
            parsed
        }
    };
    let chunks = chunk_doco(&parsed.doco, &opts.chunk)?;
    Ok(PreparedDocument {
        meta,
        doc_iri,
        parsed,
        chunks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_iri_encodes_but_keeps_slashes() {
        assert_eq!(
            document_iri("urn:fluree:doc:", "reports/Q1 2026.pdf"),
            "urn:fluree:doc:reports/Q1%202026.pdf"
        );
    }

    #[test]
    fn markdown_file_prepares_end_to_end() {
        let dir = std::env::temp_dir().join(format!(
            "fluree-db-doc-ingest-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("note.md");
        std::fs::write(
            &file,
            "# Title\n\nFirst paragraph.\n\n## Sub\n\nSecond paragraph.\n",
        )
        .unwrap();
        let opts = IngestOptions {
            iri_prefix: "urn:t:".into(),
            chunk: ChunkConfig {
                min_chars: 1,
                max_chars: 500,
            },
            cache: Some(DocCache::new(dir.join("cache"))),
            vlm: None,
        };
        let inputs = collect_inputs(std::slice::from_ref(&dir)).unwrap();
        assert_eq!(inputs.len(), 1);
        let (path, rel) = &inputs[0];
        assert_eq!(rel, "note.md");

        let first = prepare(path, rel, &opts).unwrap();
        assert!(!first.parsed.from_cache);
        assert_eq!(first.doc_iri, "urn:t:note.md");
        assert!(first.parsed.doco.contains("doc:sourceDocument"));
        assert_eq!(first.chunks.len(), 2);
        assert_eq!(first.chunks[1].header_path, vec!["Title", "Sub"]);

        let second = prepare(path, rel, &opts).unwrap();
        assert!(second.parsed.from_cache);
        assert_eq!(second.chunks, first.chunks);
        std::fs::remove_dir_all(dir).ok();
    }
}
