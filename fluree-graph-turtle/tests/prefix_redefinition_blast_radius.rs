//! The blast radius of this fix, reproduced rather than asserted — authored by
//! the review of this fix and adopted as a permanent test.
//!
//! The PR claims that a mid-document prefix redefinition reaching bulk import
//! meets TWO independent defects, and that this fix closes only one of them.
//! Every step of that claim is executed here: the streaming reader accepts such
//! a file (no `PrefixAfterData` guard on the import path), the extracted
//! prelude holds only the head binding, the redefinition arrives as ordinary
//! chunk data with chunks after it, and the guarded pre-scan path would have
//! rejected the same file.
//!
//! # One assertion here describes a LIVE DEFECT on purpose
//!
//! `wrong_after_carrier > 0` asserts that chunks *after* the redefinition are
//! still mis-resolved. That is not a property worth having — it is the §1.4
//! chunker gap (mid-file directive detection → serial fallback), which belongs
//! to the parallel-convert workstream and is not touched here.
//!
//! It is asserted rather than `#[ignore]`d deliberately. An ignored test runs
//! nowhere and rots; this one runs every time and will FAIL the moment §1.4
//! lands — which is the signal wanted, because whoever closes that gap should
//! be told to come here and flip this to `== 0`. Same for
//! `the_guard_misses_a_mid_line_directive`, which pins that `PrefixCheck` only
//! matches a directive keyword at line start.
//!
//! Adapted from the review probe in one respect: temporary files go through
//! `tempfile` rather than a fixed path under the system temp dir, so
//! concurrent runs of this binary cannot interfere with each other.

use fluree_graph_ir::GraphCollectorSink;
use fluree_graph_turtle::parse_with_prefixes_base;
use fluree_graph_turtle::splitter::{
    compute_chunk_boundaries, extract_prefix_block, StreamingTurtleReader,
};
use std::io::Write;

/// 4000 statements with a redefinition of `e:` exactly half way through.
fn write_corpus(path: &std::path::Path, n: usize) {
    let mut f = std::fs::File::create(path).unwrap();
    writeln!(f, "@prefix e: <http://a/> .").unwrap();
    for i in 0..n / 2 {
        writeln!(f, "e:s{i} <http://p/> \"v\" .").unwrap();
    }
    writeln!(f, "@prefix e: <http://b/> .").unwrap();
    for i in n / 2..n {
        writeln!(f, "e:s{i} <http://p/> \"v\" .").unwrap();
    }
}

#[test]
fn streaming_reader_accepts_a_mid_file_redefinition() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let path = dir.join("corpus.ttl");
    write_corpus(&path, 4000);

    // Claim 1: StreamingTurtleReader ACCEPTS the file (no PrefixAfterData).
    let reader = StreamingTurtleReader::new(&path, 16 * 1024, 4, None)
        .expect("StreamingTurtleReader accepts a mid-file redefinition");

    // Claim 2: the extracted prefix block is only the HEAD binding.
    let block = reader.prefix_block().to_string();
    println!("prefix_block = {block:?}");
    assert!(block.contains("http://a/"), "head binding present");
    assert!(
        !block.contains("http://b/"),
        "redefinition is NOT in the extracted prelude"
    );

    let prelude = reader.prelude().clone();
    println!("prelude prefixes = {:?}", prelude.prefixes);

    // Claim 3: the redefinition is delivered as ordinary chunk data.
    let mut chunks: Vec<(usize, String)> = Vec::new();
    while let Some((idx, raw)) = reader.recv_chunk().unwrap() {
        chunks.push((idx, String::from_utf8(raw).unwrap()));
    }
    chunks.sort_by_key(|(i, _)| *i);
    println!("chunk count = {}", chunks.len());
    let carrier: Vec<usize> = chunks
        .iter()
        .filter(|(_, t)| t.contains("@prefix e: <http://b/>"))
        .map(|(i, _)| *i)
        .collect();
    assert_eq!(
        carrier.len(),
        1,
        "redefinition delivered inside exactly one chunk"
    );
    println!("redefinition carried in chunk index {}", carrier[0]);
    assert!(
        chunks.len() > carrier[0] + 1,
        "there ARE chunks after the carrier — cross-chunk exposure is real"
    );

    // Claim 4: parse each chunk the way import does — prelude prefixes seeded,
    // prefix block prepended — and count how many subjects are wrong.
    let seeded: Vec<(String, String)> = prelude.prefixes.to_vec();
    let mut wrong_in_carrier = 0usize;
    let mut wrong_after_carrier = 0usize;
    for (idx, text) in &chunks {
        let doc = format!("{block}{text}");
        let mut sink = GraphCollectorSink::new();
        parse_with_prefixes_base(&doc, &mut sink, &seeded, None).expect("chunk parses");
        for t in sink.finish().iter() {
            let s = t.s.as_iri().unwrap_or("");
            let n: usize = s
                .rsplit("/s")
                .next()
                .and_then(|d| d.parse().ok())
                .unwrap_or(usize::MAX);
            if n >= 2000 && n != usize::MAX && !s.starts_with("http://b/") {
                if *idx == carrier[0] {
                    wrong_in_carrier += 1;
                } else {
                    wrong_after_carrier += 1;
                }
            }
        }
    }
    println!(
        "post-redefinition subjects still on the OLD namespace: \
         in carrier chunk = {wrong_in_carrier}, in later chunks = {wrong_after_carrier}"
    );

    // The fix repairs the carrier chunk. Later chunks remain broken — the PR
    // says so explicitly and does not claim otherwise.
    assert_eq!(
        wrong_in_carrier, 0,
        "within the carrier chunk the fix must hold"
    );
    assert!(
        wrong_after_carrier > 0,
        "later chunks are still mis-resolved — the separate chunker defect"
    );

    // Claim 5: the guarded pre-scan path WOULD have rejected this file.
    let (_, guard_ds) = extract_prefix_block(&path).unwrap();
    let guarded = compute_chunk_boundaries(&path, guard_ds, 16 * 1024);
    println!("compute_chunk_boundaries => {guarded:?}");
    assert!(
        guarded.is_err(),
        "the guarded path rejects what the streaming path accepts"
    );
}

/// The PR also notes, without fixing: `PrefixCheck` only matches a directive
/// keyword at line start, so a mid-line redefinition slips even the guard.
#[test]
fn the_guard_misses_a_mid_line_directive() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    let at_line_start = dir.join("line_start.ttl");
    {
        let mut f = std::fs::File::create(&at_line_start).unwrap();
        writeln!(f, "@prefix e: <http://a/> .").unwrap();
        writeln!(f, "e:s <http://p/> \"v\" .").unwrap();
        writeln!(f, "@prefix e: <http://b/> .").unwrap();
        writeln!(f, "e:t <http://p/> \"v\" .").unwrap();
    }
    // Feed the guard the way its real caller does: scanning starts AFTER the
    // extracted header block. (Passing 0 makes the header's own directive look
    // like a post-data one, because PrefixCheck marks data_started on the
    // keyword's first byte.)
    let (_, ds) = extract_prefix_block(&at_line_start).unwrap();
    let res_line_start = compute_chunk_boundaries(&at_line_start, ds, 1024);
    println!("line-start directive (data_start={ds}) => {res_line_start:?}");
    assert!(
        res_line_start.is_err(),
        "line-start directive after data IS caught"
    );

    let mid_line = dir.join("mid_line.ttl");
    {
        let mut f = std::fs::File::create(&mid_line).unwrap();
        writeln!(f, "@prefix e: <http://a/> .").unwrap();
        writeln!(
            f,
            "e:s <http://p/> \"v\" . @prefix e: <http://b/> . e:t <http://p/> \"v\" ."
        )
        .unwrap();
    }
    let (_, ds2) = extract_prefix_block(&mid_line).unwrap();
    let res = compute_chunk_boundaries(&mid_line, ds2, 1024);
    println!("mid-line directive (data_start={ds2}) => {res:?}");
    assert!(
        res.is_ok(),
        "mid-line directive after data is MISSED by the guard (noted, not fixed)"
    );
}
