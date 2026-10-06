//! Shared source-conversion helpers for the bulk-import pipeline.
//!
//! CSV (neo4j-admin header convention) and Cypher dump inputs are not
//! pipeline-native: they are first converted to newline-delimited JSON-LD
//! shards in a caller-provided scratch directory, then the shard directory
//! is handed to the ordinary chunked bulk import (`CreateBuilder::import`).
//! The CLI (`fluree create --from data.csv|dump.cypher`) and the server's
//! source-upload import endpoint share these helpers so both surfaces
//! accept exactly the same input formats.

use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::csv_import::{write_csv_ndjson, CsvImportOptions};
use crate::cypher_import::{CypherImportOptions, CypherImportStats, CypherImporter};

/// Source-conversion failure: which input file, and why.
#[derive(Debug, thiserror::Error)]
pub enum SourceConvertError {
    #[error("{path}: {message}")]
    Input { path: String, message: String },
    #[error("no convertible source files found in {0}")]
    NoInputFiles(String),
    #[error("Cypher input produced no data (no CREATE statements found)")]
    EmptyCypher,
}

impl SourceConvertError {
    fn input(path: &Path, message: impl std::fmt::Display) -> Self {
        Self::Input {
            path: path.display().to_string(),
            message: message.to_string(),
        }
    }
}

/// Whether the path is a single `.csv` file or a directory containing one.
pub fn is_csv_input(path: &Path) -> bool {
    if path.is_dir() {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .filter_map(std::result::Result::ok)
            .any(|e| has_csv_extension(&e.path()))
    } else {
        has_csv_extension(path)
    }
}

pub fn has_csv_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("csv"))
}

/// Whether the path is a single `.cypher`/`.cyp`/`.cql` file or a directory
/// containing one.
pub fn is_cypher_input(path: &Path) -> bool {
    if path.is_dir() {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .filter_map(std::result::Result::ok)
            .any(|e| has_cypher_extension(&e.path()))
    } else {
        has_cypher_extension(path)
    }
}

pub fn has_cypher_extension(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()).is_some_and(|e| {
        e.eq_ignore_ascii_case("cypher")
            || e.eq_ignore_ascii_case("cyp")
            || e.eq_ignore_ascii_case("cql")
    })
}

/// Replace non-alphanumeric characters with underscores for safe filenames.
fn sanitize_for_filename(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn input_files(
    path: &Path,
    matches: impl Fn(&Path) -> bool,
) -> Result<Vec<PathBuf>, SourceConvertError> {
    let files: Vec<PathBuf> = if path.is_dir() {
        let mut v: Vec<_> = std::fs::read_dir(path)
            .map_err(|e| SourceConvertError::input(path, format!("failed to read dir: {e}")))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| matches(p))
            .collect();
        v.sort();
        v
    } else {
        vec![path.to_path_buf()]
    };
    if files.is_empty() {
        return Err(SourceConvertError::NoInputFiles(path.display().to_string()));
    }
    Ok(files)
}

/// Stream CSV input (a file or a directory of `.csv`) into one `.jsonl`
/// shard per source file under `out_dir`. One shard per source file lets
/// the bulk pipeline parallelize across them. Returns the object count.
pub fn csv_to_shards(
    input: &Path,
    opts: &CsvImportOptions,
    out_dir: &Path,
) -> Result<usize, SourceConvertError> {
    let files = input_files(input, has_csv_extension)?;
    let mut total_objects = 0usize;
    for (i, csv_path) in files.iter().enumerate() {
        let stem = csv_path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(sanitize_for_filename)
            .unwrap_or_else(|| format!("part{i}"));
        let out_path = out_dir.join(format!("{i:05}_{stem}.jsonl"));
        let reader =
            BufReader::new(std::fs::File::open(csv_path).map_err(|e| {
                SourceConvertError::input(csv_path, format!("failed to open: {e}"))
            })?);
        let mut writer = BufWriter::new(std::fs::File::create(&out_path).map_err(|e| {
            SourceConvertError::input(&out_path, format!("failed to create shard: {e}"))
        })?);
        total_objects += write_csv_ndjson(reader, opts, &mut writer)
            .map_err(|e| SourceConvertError::input(csv_path, e))?;
        writer
            .into_inner()
            .map_err(|e| SourceConvertError::input(&out_path, format!("failed to flush: {e}")))?;
    }
    Ok(total_objects)
}

/// Convert Cypher scripts (`CREATE` / `MATCH … CREATE` dump convention) into
/// `.jsonl` shards under `out_dir`: pass 1 over every file learns each
/// label's match-key property set (so node ids derive identically across
/// files), then one node shard and one edge shard per source file. Returns
/// the importer stats (nodes / edges / skipped).
pub fn cypher_to_shards(
    input: &Path,
    opts: CypherImportOptions,
    out_dir: &Path,
) -> Result<CypherImportStats, SourceConvertError> {
    let files = input_files(input, has_cypher_extension)?;
    let mut importer = CypherImporter::new(opts);

    let open = |p: &Path| {
        std::fs::File::open(p)
            .map(BufReader::new)
            .map_err(|e| SourceConvertError::input(p, format!("failed to open: {e}")))
    };

    for p in &files {
        importer
            .learn_keys(open(p)?)
            .map_err(|e| SourceConvertError::input(p, e))?;
    }

    let mut total_objects = 0usize;
    for pass in 0..2 {
        for (i, src) in files.iter().enumerate() {
            let stem = src
                .file_stem()
                .and_then(|s| s.to_str())
                .map(sanitize_for_filename)
                .unwrap_or_else(|| format!("part{i}"));
            let (kind, shard) = match pass {
                0 => ("nodes", i),
                _ => ("edges", files.len() + i),
            };
            let out_path = out_dir.join(format!("{shard:05}_{stem}_{kind}.jsonl"));
            let mut writer = BufWriter::new(std::fs::File::create(&out_path).map_err(|e| {
                SourceConvertError::input(&out_path, format!("failed to create shard: {e}"))
            })?);
            let written = match pass {
                0 => importer.write_nodes_ndjson(open(src)?, &mut writer),
                _ => importer.write_edges_ndjson(open(src)?, &mut writer),
            }
            .map_err(|e| SourceConvertError::input(src, e))?;
            writer
                .into_inner()
                .map_err(|e| SourceConvertError::input(&out_path, format!("failed to flush: {e}")))
                .and_then(|mut f| {
                    f.flush().map_err(|e| {
                        SourceConvertError::input(&out_path, format!("failed to flush: {e}"))
                    })
                })?;
            if written == 0 {
                // Keep the shard set dense.
                std::fs::remove_file(&out_path).ok();
            }
            total_objects += written;
        }
    }
    if total_objects == 0 {
        return Err(SourceConvertError::EmptyCypher);
    }
    Ok(importer.stats)
}
