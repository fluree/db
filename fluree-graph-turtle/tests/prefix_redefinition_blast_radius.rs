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
//! # The two LIVE-DEFECT assertions here have since been CLOSED
//!
//! This file was written on `main`, where two of its assertions deliberately
//! pinned defects rather than properties: that chunks *after* a mid-file
//! redefinition are still mis-resolved (the §1.4 chunker gap), and that
//! `PrefixCheck` only matches a directive keyword at line start.
//!
//! Both were asserted rather than `#[ignore]`d precisely so they would FAIL
//! the moment §1.4 landed, and tell whoever closed the gap to come here. §1.4
//! has landed — the parallel-convert workstream drives the streaming chunker
//! with the real byte-level `BoundaryScanner` and detects a directive at any
//! token start — so both assertions have been flipped to the closed behavior,
//! which is what this comment is the record of.
//!
//! What replaced them is worth stating, because it is not "the chunker now
//! chunks such a file correctly". It refuses it. A mid-file directive makes a
//! document unchunkable — only the first chunk would carry the redefinition —
//! so the reader thread raises [`SplitError::PrefixAfterData`] and stops.
//!
//! That refusal reaches a consumer through [`StreamingTurtleReader::join`] and
//! NOT through `recv_chunk`, which reports the closed channel as a benign
//! `Ok(None)`. Every live import call site joins (see `import.rs`, which
//! carries a comment saying why). A consumer that drains without joining sees
//! a short read and no error — which is exactly what the original version of
//! this test did, and why it read as truncation when §1.4 landed.
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
///
/// The OBJECT is the load-bearing part, and an earlier version of this corpus
/// did not have it. Subjects are `e:s{i}`, a distinct span every time, so no
/// prefixed name is ever looked up twice and the expansion cache is never
/// consulted for a span it already holds — which is the only way the bug can
/// show. With a literal object, the carrier-chunk assertion below passed
/// against a parser with the fix entirely removed: it proved nothing.
///
/// `e:tag` repeats on every line, so it is cached under the FIRST binding and
/// then requested again after the rebinding. That is the whole defect in one
/// term.
fn write_corpus(path: &std::path::Path, n: usize) {
    let mut f = std::fs::File::create(path).unwrap();
    writeln!(f, "@prefix e: <http://a/> .").unwrap();
    for i in 0..n / 2 {
        writeln!(f, "e:s{i} <http://p/> e:tag .").unwrap();
    }
    writeln!(f, "@prefix e: <http://b/> .").unwrap();
    for i in n / 2..n {
        writeln!(f, "e:s{i} <http://p/> e:tag .").unwrap();
    }
}

#[test]
fn streaming_reader_refuses_a_mid_file_redefinition_and_says_so() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let path = dir.join("corpus.ttl");
    write_corpus(&path, 4000);

    // Claim 1: the reader still CONSTRUCTS. The header is well-formed, and the
    // redefinition is only discoverable by scanning the body, which happens on
    // the reader thread after this returns.
    let mut reader = StreamingTurtleReader::new(&path, 16 * 1024, 4, None)
        .expect("construction reads only the header");

    // Claim 2: the extracted prefix block is only the HEAD binding.
    let block = reader.prefix_block().to_string();
    assert!(block.contains("http://a/"), "head binding present");
    assert!(
        !block.contains("http://b/"),
        "redefinition is NOT in the extracted prelude"
    );

    // Claim 3: no chunk ever carries the redefinition, because the scanner
    // stops at it. Before §1.4 this file streamed to completion and the
    // redefinition arrived as ordinary chunk data with chunks after it.
    let mut chunks: Vec<(usize, String)> = Vec::new();
    while let Some((idx, raw)) = reader.recv_chunk().unwrap() {
        chunks.push((idx, String::from_utf8(raw).unwrap()));
    }
    assert!(
        !chunks
            .iter()
            .any(|(_, t)| t.contains("@prefix e: <http://b/>")),
        "no chunk may carry a mid-file redefinition"
    );

    // Claim 4 — THE claim, and the one the old version of this test could not
    // make. Draining alone looks like a clean end of stream; the error lives
    // on the reader thread and only `join` surfaces it. This is the assertion
    // that distinguishes "refused" from "silently truncated", and it is why
    // every import call site joins.
    let joined = reader.join();
    assert!(
        matches!(
            joined,
            Err(fluree_graph_turtle::splitter::SplitError::PrefixAfterData { .. })
        ),
        "join must report the refusal, not a benign end of stream: {joined:?}"
    );

    // Claim 5: the in-memory and pre-scan paths agree with the streaming one.
    let (_, guard_ds) = extract_prefix_block(&path).unwrap();
    assert!(
        compute_chunk_boundaries(&path, guard_ds, 16 * 1024).is_err(),
        "the pre-scan path refuses the same file"
    );
}

/// The parser fix itself, on the shape that motivated it: a prefixed name
/// reused across a rebinding must follow the rebinding, not the cache.
///
/// Kept separate from the chunker story above because it is a property of the
/// PARSER and holds whether or not the document is ever chunked. Before the
/// fix, `e:tag` after the redefinition was served from the span cache under
/// the old namespace.
#[test]
fn a_name_reused_across_a_rebinding_follows_it_rather_than_the_cache() {
    let mut doc = String::from("@prefix e: <http://a/> .\n");
    for i in 0..10 {
        doc.push_str(&format!("e:s{i} <http://p/> e:tag .\n"));
    }
    doc.push_str("@prefix e: <http://b/> .\n");
    for i in 10..20 {
        doc.push_str(&format!("e:s{i} <http://p/> e:tag .\n"));
    }

    let mut sink = GraphCollectorSink::new();
    parse_with_prefixes_base(&doc, &mut sink, &[], None).expect("parses");

    let mut stale_subjects = 0usize;
    let mut stale_objects = 0usize;
    for t in sink.into_graph().iter() {
        let s = t.s.as_iri().unwrap_or("");
        let n: usize = s
            .rsplit("/s")
            .next()
            .and_then(|d| d.parse().ok())
            .unwrap_or(usize::MAX);
        if n >= 10 && n != usize::MAX {
            if !s.starts_with("http://b/") {
                stale_subjects += 1;
            }
            if t.o.as_iri() == Some("http://a/tag") {
                stale_objects += 1;
            }
        }
    }
    assert_eq!(
        stale_subjects, 0,
        "subjects after the rebinding must follow it"
    );
    assert_eq!(
        stale_objects, 0,
        "a repeated prefixed name must be re-expanded after a rebinding"
    );
}

/// `main` noted, without fixing, that `PrefixCheck` matched a directive
/// keyword only at line start, so a mid-line redefinition slipped the guard.
/// §1.4 closed it: detection now fires at any token start.
#[test]
fn the_guard_catches_a_mid_line_directive() {
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
        res.is_err(),
        "a mid-line directive after data is caught too, not just a line-start one"
    );
}
