//! Time the chunk pre-scan on a real document.
//!
//! The pre-scan is single-threaded and runs to completion before the first
//! worker starts, so it is the parallel path's Amdahl term and its throughput
//! is the ceiling on parallel conversion. This measures it on its own, without
//! a parse or a writer in the way.
//!
//! ```console
//! $ cargo run --release --example scan_probe -- corpus.ttl 5
//! 143.5 MiB, 5 runs: best 0.271s = 529.5 MiB/s, 61 chunks
//! ```
//!
//! Best-of-N, because contention can only inflate a run.

use std::time::Instant;

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: scan_probe <file.ttl> [runs] [target_chunk_bytes]");
        std::process::exit(2);
    };
    let runs: usize = args.next().map_or(5, |n| n.parse().unwrap_or(5));
    let target: u64 = args
        .next()
        .map_or(8 * 1024 * 1024, |n| n.parse().unwrap_or(8 * 1024 * 1024));

    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        eprintln!("cannot read {path}: {e}");
        std::process::exit(1);
    });

    let mut best = f64::MAX;
    let mut chunks = 0;
    for _ in 0..runs {
        let started = Instant::now();
        let (_prefix, ranges) = fluree_graph_turtle::splitter::chunk_in_memory(&text, target)
            .unwrap_or_else(|e| {
                eprintln!("cannot chunk {path}: {e}");
                std::process::exit(1);
            });
        let elapsed = started.elapsed().as_secs_f64();
        // Read the result so the scan cannot be optimized away.
        chunks = ranges.len();
        best = best.min(elapsed);
    }

    let mib = text.len() as f64 / (1024.0 * 1024.0);
    println!(
        "{mib:.1} MiB, {runs} runs: best {best:.3}s = {:.1} MiB/s, {chunks} chunks",
        mib / best
    );
}
