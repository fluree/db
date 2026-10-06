//! Peer-cache inspection and cleanup: `fluree cache status|clear`
//!
//! The peer cache holds index artifacts fetched from remotes in peer mode
//! (see [`crate::context::peer_cache_root`]). Everything in it is
//! content-addressed and re-fetchable, so clearing is always safe.

use crate::cli::CacheAction;
use crate::context::{peer_cache_dir, peer_cache_root};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use std::fs;
use std::path::Path;

pub fn run(action: CacheAction) -> CliResult<()> {
    match action {
        CacheAction::Status => run_status(),
        CacheAction::Clear { remote } => run_clear(remote.as_deref()),
    }
}

fn run_status() -> CliResult<()> {
    let root = peer_cache_root();
    if !root.exists() {
        println!("Peer cache is empty ({}).", root.display());
        return Ok(());
    }

    let mut total: u64 = 0;
    let mut rows: Vec<(String, u64)> = Vec::new();
    let entries =
        fs::read_dir(&root).map_err(|e| CliError::Config(format!("read cache dir: {e}")))?;
    for entry in entries.flatten() {
        if entry.path().is_dir() {
            let size = dir_size(&entry.path());
            total += size;
            rows.push((entry.file_name().to_string_lossy().to_string(), size));
        }
    }
    rows.sort_by_key(|(_, size)| std::cmp::Reverse(*size));

    if rows.is_empty() {
        println!("Peer cache is empty ({}).", root.display());
        return Ok(());
    }

    println!("Peer cache: {}", root.display());
    for (remote, size) in &rows {
        println!("  {:<24} {}", remote, human_bytes(*size));
    }
    println!("  {:<24} {}", "total".bold(), human_bytes(total));
    Ok(())
}

fn run_clear(remote: Option<&str>) -> CliResult<()> {
    let target = match remote {
        Some(name) => peer_cache_dir(name),
        None => peer_cache_root(),
    };
    if !target.exists() {
        println!("Nothing to clear ({}).", target.display());
        return Ok(());
    }
    let freed = dir_size(&target);
    fs::remove_dir_all(&target)
        .map_err(|e| CliError::Config(format!("clear cache {}: {e}", target.display())))?;
    println!("Cleared {} ({}).", target.display(), human_bytes(freed));
    Ok(())
}

fn dir_size(path: &Path) -> u64 {
    let mut size = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                size += dir_size(&p);
            } else if let Ok(meta) = entry.metadata() {
                size += meta.len();
            }
        }
    }
    size
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
