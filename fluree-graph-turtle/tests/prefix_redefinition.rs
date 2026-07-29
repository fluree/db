//! Mid-document prefix redeclaration.
//!
//! Turtle allows a prefix to be rebound part-way through a document, and every
//! prefixed name after the rebinding denotes something new. The parser caches
//! expanded prefixed names by their span text (`ex:name`), which identifies an
//! IRI only relative to the bindings in force — so a stale cache turns a
//! rebinding into a silent no-op.
//!
//! Discovered while wiring term validation for H-8 (the burn-down's cause C),
//! when a probe of the prefix path produced two statements on one subject.
//! Nothing about it is specific to validation; it reproduces on plain `parse`.

use fluree_graph_ir::GraphCollectorSink;
use fluree_graph_turtle::{parse, parse_with_prefixes_base};

/// Subjects in document order.
fn subjects(doc: &str) -> Vec<String> {
    let mut sink = GraphCollectorSink::new();
    parse(doc, &mut sink).expect("parses");
    sink.finish()
        .iter()
        .filter_map(|t| t.s.as_iri().map(str::to_string))
        .collect()
}

/// The reproduction, verbatim. Before the fix both subjects came back as
/// `http://a/x`: the second statement was attributed to the first namespace,
/// with no error anywhere.
#[test]
fn a_rebound_prefix_changes_what_later_names_mean() {
    let doc = "@prefix e: <http://a/> .\n\
               e:x <http://p/> \"1\" .\n\
               @prefix e: <http://b/> .\n\
               e:x <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://a/x", "http://b/x"]);
}

/// The same thing in SPARQL spelling, which takes the same code path and would
/// have been just as broken.
#[test]
fn the_sparql_style_directive_rebinds_too() {
    let doc = "PREFIX e: <http://a/>\n\
               e:x <http://p/> \"1\" .\n\
               PREFIX e: <http://b/>\n\
               e:x <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://a/x", "http://b/x"]);
}

/// Rebinding must not disturb prefixes it did not name.
#[test]
fn a_rebinding_leaves_other_prefixes_alone() {
    let doc = "@prefix e: <http://a/> .\n\
               @prefix f: <http://f/> .\n\
               e:x f:p \"1\" .\n\
               @prefix e: <http://b/> .\n\
               e:x f:p \"2\" .\n";
    let mut sink = GraphCollectorSink::new();
    parse(doc, &mut sink).expect("parses");
    let graph = sink.finish();

    let subjects: Vec<&str> = graph.iter().filter_map(|t| t.s.as_iri()).collect();
    assert_eq!(subjects, vec!["http://a/x", "http://b/x"]);
    let predicates: Vec<&str> = graph.iter().filter_map(|t| t.p.as_iri()).collect();
    assert_eq!(
        predicates,
        vec!["http://f/p", "http://f/p"],
        "f: was never rebound"
    );
}

/// Rebinding back to a namespace already used has to work too — the cache must
/// be rebuilt, not merely invalidated once.
#[test]
fn rebinding_back_and_forth_keeps_working() {
    let doc = "@prefix e: <http://a/> .\n\
               e:x <http://p/> \"1\" .\n\
               @prefix e: <http://b/> .\n\
               e:x <http://p/> \"2\" .\n\
               @prefix e: <http://a/> .\n\
               e:x <http://p/> \"3\" .\n";
    assert_eq!(
        subjects(doc),
        vec!["http://a/x", "http://b/x", "http://a/x"]
    );
}

/// The empty prefix is a prefix.
#[test]
fn the_default_prefix_rebinds() {
    let doc = "@prefix : <http://a/> .\n\
               :x <http://p/> \"1\" .\n\
               @prefix : <http://b/> .\n\
               :x <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://a/x", "http://b/x"]);
}

/// A namespace-only prefixed name (`e:`) is cached under the same key space
/// and must be invalidated with the rest.
#[test]
fn a_namespace_only_name_is_invalidated_too() {
    let doc = "@prefix e: <http://a/> .\n\
               e: <http://p/> \"1\" .\n\
               @prefix e: <http://b/> .\n\
               e: <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://a/", "http://b/"]);
}

/// Redeclaring a prefix to the SAME namespace changes nothing, and must not.
///
/// This is the case that decides the invalidation strategy: it is what chunked
/// import does on every chunk — the file's prefix block is prepended to a
/// parser already seeded with those same bindings — so clearing on every
/// declaration rather than on every *rebinding* would throw away cache hits
/// across the whole import path for no correctness gain.
#[test]
fn redeclaring_the_same_binding_is_not_a_rebinding() {
    let doc = "@prefix e: <http://a/> .\n\
               e:x <http://p/> \"1\" .\n\
               @prefix e: <http://a/> .\n\
               e:x <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://a/x", "http://a/x"]);
}

/// The chunked-import shape: a pre-seeded prefix map plus a prelude that
/// declares the same bindings, then a genuine rebinding inside the chunk.
#[test]
fn a_pre_seeded_map_rebinds_the_same_way() {
    let seeded = [("e".to_string(), "http://a/".to_string())];
    let doc = "@prefix e: <http://a/> .\n\
               e:x <http://p/> \"1\" .\n\
               @prefix e: <http://b/> .\n\
               e:x <http://p/> \"2\" .\n";

    let mut sink = GraphCollectorSink::new();
    parse_with_prefixes_base(doc, &mut sink, &seeded, None).expect("parses");
    let subjects: Vec<String> = sink
        .finish()
        .iter()
        .filter_map(|t| t.s.as_iri().map(str::to_string))
        .collect();
    assert_eq!(subjects, vec!["http://a/x", "http://b/x"]);
}

/// `@base` was never affected, and this pins that the scope of the fix is
/// right rather than merely asserted.
///
/// Resolved IRIs are cached under the *resolved* string, so changing the base
/// produces a different key and never collided. Prefixed names do not consult
/// the base at lookup time either — a namespace is resolved once, when its
/// directive is read.
#[test]
fn rebasing_was_never_broken_and_still_is_not() {
    let doc = "@base <http://a/> .\n\
               <x> <http://p/> \"1\" .\n\
               @base <http://b/> .\n\
               <x> <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://a/x", "http://b/x"]);
}

/// A rebinding deep in a document must not lose the entries cached before it,
/// nor keep them. Exercises the cache at a size where a bug would show as a
/// partial rather than total failure.
#[test]
fn a_rebinding_after_many_names_invalidates_all_of_them() {
    let mut doc = String::from("@prefix e: <http://a/> .\n");
    for i in 0..500 {
        doc.push_str(&format!("e:s{i} <http://p/> \"v\" .\n"));
    }
    doc.push_str("@prefix e: <http://b/> .\n");
    for i in 0..500 {
        doc.push_str(&format!("e:s{i} <http://p/> \"w\" .\n"));
    }

    let got = subjects(&doc);
    assert_eq!(got.len(), 1000);
    for (i, subject) in got.iter().enumerate().take(500) {
        assert_eq!(subject, &format!("http://a/s{i}"));
    }
    for (i, subject) in got.iter().skip(500).enumerate() {
        assert_eq!(subject, &format!("http://b/s{i}"));
    }
}
