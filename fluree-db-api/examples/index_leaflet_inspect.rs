//! Dump the PSOT leaf/leaflet shape of one or more predicates and time a full
//! decode pass — the unit of work the count-plan fast paths perform per
//! leaflet. Diagnostic for import-layout regressions that leave fetch counts
//! unchanged but change per-leaflet CPU.
//!
//!   cargo run --release -p fluree-db-api --example index_leaflet_inspect -- \
//!       <storage-path> <ledger-alias> <predicate-iri> [<predicate-iri> ...]
//!
//! `<storage-path>` is the directory holding `storage/` (e.g. `.fluree`).

use fluree_db_api::FlureeBuilder;
use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
use fluree_db_binary_index::{ColumnProjection, ColumnSet, LeafEntry, RunSortOrder};
use std::collections::BTreeMap;
use std::time::Instant;

fn pct(sorted: &[u32], q: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("usage: index_leaflet_inspect <storage-path> <ledger> <predicate-iri>...");
        std::process::exit(2);
    }
    let storage = args[0].trim_end_matches('/').to_string();
    let storage = if std::path::Path::new(&storage).join("storage").is_dir() {
        format!("{storage}/storage")
    } else {
        storage
    };
    let fluree = FlureeBuilder::file(storage)
        .without_ledger_caching()
        .without_indexing()
        .build()?;
    let db = fluree.db(&args[1]).await?;
    let store = db
        .binary_store()
        .ok_or("ledger has no binary index store")?;

    for iri in &args[2..] {
        let sid = store.encode_iri(iri);
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            println!("== {iri}: not in index");
            continue;
        };
        // Default graph is g_id 0 for single-graph ledgers; fall back to the
        // first graph that has a PSOT branch.
        let (g_id, branch) = (0u16..16)
            .find_map(|g| {
                store
                    .branch_for_order(g, RunSortOrder::Psot)
                    .map(|b| (g, b))
            })
            .ok_or("no PSOT branch")?;
        let leaves: Vec<&LeafEntry> = branch
            .leaves
            .iter()
            .filter(|e| e.first_key.p_id <= p_id && p_id <= e.last_key.p_id)
            .collect();

        let mut leaflets = 0usize;
        let mut zero_row = 0usize;
        let mut rows_total = 0u64;
        let mut rows_per: Vec<u32> = Vec::new();
        let mut payload = 0u64;
        let mut history = 0u64;
        let mut dir_entries_total = 0usize;
        // (col_id, codec, elem_width) -> (count, compressed, uncompressed)
        let mut cols: BTreeMap<(u16, u8, u8), (u64, u64, u64)> = BTreeMap::new();
        let mut handles = Vec::with_capacity(leaves.len());

        let t_open = Instant::now();
        for leaf in &leaves {
            let h = store.open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)?;
            let dir = h.dir();
            dir_entries_total += dir.entries.len();
            for e in &dir.entries {
                if e.p_const != Some(p_id) {
                    continue;
                }
                leaflets += 1;
                if e.row_count == 0 {
                    zero_row += 1;
                }
                rows_total += e.row_count as u64;
                rows_per.push(e.row_count);
                payload += e.payload_len as u64;
                history += e.history_len as u64;
                for c in &e.column_refs {
                    let s = cols.entry((c.col_id, c.codec, c.elem_width)).or_default();
                    s.0 += 1;
                    s.1 += c.compressed_len as u64;
                    s.2 += c.uncompressed_len as u64;
                }
            }
            handles.push(h);
        }
        let open_ms = t_open.elapsed().as_secs_f64() * 1e3;
        rows_per.sort_unstable();

        println!("== {iri}  (p_id {p_id}, g_id {g_id})");
        println!(
            "   leaves {}  dir-entries {}  leaflets {}  zero-row {}  rows {}  open+dir {:.1} ms",
            leaves.len(),
            dir_entries_total,
            leaflets,
            zero_row,
            rows_total,
            open_ms
        );
        println!(
            "   rows/leaflet  min {}  p10 {}  p50 {}  p90 {}  max {}",
            pct(&rows_per, 0.0),
            pct(&rows_per, 0.1),
            pct(&rows_per, 0.5),
            pct(&rows_per, 0.9),
            pct(&rows_per, 1.0)
        );
        println!(
            "   payload {:.1} MB  history {:.1} MB  bytes/row {:.2}",
            payload as f64 / 1e6,
            history as f64 / 1e6,
            payload as f64 / rows_total.max(1) as f64
        );
        for ((col, codec, w), (n, comp, unc)) in &cols {
            println!(
                "   col {:>2} codec {} width {}  blocks {:>7}  comp {:>8.1} MB  unc {:>8.1} MB  ratio {:.2}",
                col,
                codec,
                w,
                n,
                *comp as f64 / 1e6,
                *unc as f64 / 1e6,
                *unc as f64 / (*comp).max(1) as f64
            );
        }

        // Decode pass: every leaflet of the predicate, all columns (what the
        // count lanes request through the leaflet cache), then SId-only.
        let mut last_s = 0u64;
        for (label, projection) in [
            ("all-columns", ColumnProjection::all()),
            (
                "sid-only",
                ColumnProjection {
                    // SId is bit 0 of the column set (see ColumnSet::ALL).
                    output: ColumnSet(1),
                    internal: ColumnSet::EMPTY,
                },
            ),
        ] {
            let t = Instant::now();
            let mut rows = 0u64;
            let mut sum = 0u64;
            for h in &handles {
                let dir = h.dir();
                for (i, e) in dir.entries.iter().enumerate() {
                    if e.p_const != Some(p_id) || e.row_count == 0 {
                        continue;
                    }
                    let last = read_ordered_key_v2(RunSortOrder::Psot, &e.last_key);
                    last_s = last_s.max(last.s_id.as_u64());
                    let b = h.load_columns(i, &projection, RunSortOrder::Psot)?;
                    rows += b.row_count as u64;
                    sum = sum.wrapping_add(b.s_id.get(b.row_count - 1));
                }
            }
            let ms = t.elapsed().as_secs_f64() * 1e3;
            println!(
                "   decode {label:<11} {:>9.1} ms  ({:.1} ns/row, {} rows, chk {})",
                ms,
                ms * 1e6 / rows.max(1) as f64,
                rows,
                sum % 1000
            );
        }
        println!("   max subject id {last_s}");
    }
    Ok(())
}
