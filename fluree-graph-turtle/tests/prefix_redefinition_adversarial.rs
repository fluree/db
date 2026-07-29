//! Adversarial probes for prefix-rebind cache invalidation, authored by the
//! review of this fix and adopted as permanent regression tests.
//!
//! `prefix_redefinition.rs` covers the shapes the fix was written against.
//! These attack the two directions the *comparison* can be wrong in, which the
//! first file cannot reach because it never varies the base:
//!
//! - A1, a MISSED clear: one spelling, two meanings.
//! - A2, a SPURIOUS clear: two spellings, one meaning.
//!
//! Both work only because `prefixes` stores the RESOLVED namespace, so the
//! comparison is on meaning rather than text — see `bind_prefix`. Then B..H
//! walk the epochs (A→B→A with overlapping and disjoint names), the coarse
//! whole-cache clear with twenty live prefixes, terms nested inside `[ ]` and
//! `( )`, escaped local names, a pre-seeded relative binding, and the two
//! directive spellings mixed.

use fluree_graph_ir::GraphCollectorSink;
use fluree_graph_turtle::{parse, parse_with_prefixes_base};

fn subjects(doc: &str) -> Vec<String> {
    let mut sink = GraphCollectorSink::new();
    parse(doc, &mut sink).expect("parses");
    sink.finish()
        .iter()
        .filter_map(|t| t.s.as_iri().map(str::to_string))
        .collect()
}

fn objects(doc: &str) -> Vec<String> {
    let mut sink = GraphCollectorSink::new();
    parse(doc, &mut sink).expect("parses");
    sink.finish()
        .iter()
        .filter_map(|t| t.o.as_iri().map(str::to_string))
        .collect()
}

/// A1 — MISSED CLEAR probe. Same lexical namespace text, different in-scope
/// base, so the two declarations mean different IRIs. If `prefixes` stored the
/// raw text the comparison would find them equal and skip the clear.
#[test]
fn a1_same_spelling_different_base_must_clear() {
    let doc = "@base <http://x/> .\n\
               @prefix e: <a/> .\n\
               e:q <http://p/> \"1\" .\n\
               @base <http://y/> .\n\
               @prefix e: <a/> .\n\
               e:q <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://x/a/q", "http://y/a/q"]);
}

/// A2 — SPURIOUS CLEAR probe (correctness half). Two different spellings that
/// resolve to the SAME namespace must keep resolving names identically.
#[test]
fn a2_different_spelling_same_namespace_stays_correct() {
    let doc = "@prefix e: <http://x/a/> .\n\
               e:q <http://p/> \"1\" .\n\
               @base <http://x/> .\n\
               @prefix e: <a/> .\n\
               e:q <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://x/a/q", "http://x/a/q"]);
}

/// B — A→B→A with names cached in every epoch, overlapping and disjoint, so a
/// stale entry surviving from the first A epoch into the second would show.
#[test]
fn b_abba_with_overlapping_and_disjoint_names() {
    let doc = "@prefix e: <http://a/> .\n\
               e:shared <http://p/> \"1\" .\n\
               e:onlyA <http://p/> \"1\" .\n\
               @prefix e: <http://b/> .\n\
               e:shared <http://p/> \"2\" .\n\
               e:onlyB <http://p/> \"2\" .\n\
               @prefix e: <http://a/> .\n\
               e:shared <http://p/> \"3\" .\n\
               e:onlyB <http://p/> \"3\" .\n\
               e:onlyA <http://p/> \"3\" .\n";
    assert_eq!(
        subjects(doc),
        vec![
            "http://a/shared",
            "http://a/onlyA",
            "http://b/shared",
            "http://b/onlyB",
            "http://a/shared",
            "http://a/onlyB",
            "http://a/onlyA",
        ]
    );
}

/// C — many prefixes, one rebinds: the survivors must still resolve, and the
/// rebound one must move. Exercises the coarse (whole-cache) clear.
#[test]
fn c_many_prefixes_one_rebinds() {
    let mut doc = String::new();
    for i in 0..20 {
        doc.push_str(&format!("@prefix p{i}: <http://n{i}/> .\n"));
    }
    for i in 0..20 {
        doc.push_str(&format!("p{i}:s <http://p/> p{i}:o .\n"));
    }
    doc.push_str("@prefix p7: <http://REBOUND/> .\n");
    for i in 0..20 {
        doc.push_str(&format!("p{i}:s <http://p/> p{i}:o .\n"));
    }

    let subs = subjects(&doc);
    let objs = objects(&doc);
    assert_eq!(subs.len(), 40);
    for i in 0..20 {
        assert_eq!(subs[i], format!("http://n{i}/s"));
        assert_eq!(objs[i], format!("http://n{i}/o"));
    }
    for i in 0..20 {
        let want_ns = if i == 7 {
            "http://REBOUND/".to_string()
        } else {
            format!("http://n{i}/")
        };
        assert_eq!(subs[20 + i], format!("{want_ns}s"), "subject prefix p{i}");
        assert_eq!(objs[20 + i], format!("{want_ns}o"), "object prefix p{i}");
    }
}

/// D — a rebind between the two halves of a predicate-object list is not
/// reachable (a directive cannot appear mid-triple), but a rebind straddling
/// blank-node and collection syntax is. Terms inside `[ ]` and `( )` come from
/// the same cache.
#[test]
fn d_rebind_around_blank_nodes_and_collections() {
    let doc = "@prefix e: <http://a/> .\n\
               e:s <http://p/> [ <http://q/> e:inner ] .\n\
               e:s2 <http://p/> ( e:i1 e:i2 ) .\n\
               @prefix e: <http://b/> .\n\
               e:s <http://p/> [ <http://q/> e:inner ] .\n\
               e:s2 <http://p/> ( e:i1 e:i2 ) .\n";
    let objs = objects(doc);
    let inner: Vec<&String> = objs
        .iter()
        .filter(|o| o.contains("inner") || o.contains("/i1") || o.contains("/i2"))
        .collect();
    assert_eq!(
        inner,
        vec![
            "http://a/inner",
            "http://a/i1",
            "http://a/i2",
            "http://b/inner",
            "http://b/i1",
            "http://b/i2",
        ]
    );
}

/// E — an undefined prefix errors before anything is cached, and defining it
/// afterwards must work.
#[test]
fn e_undefined_then_defined() {
    let mut sink = GraphCollectorSink::new();
    let err = parse("e:x <http://p/> \"1\" .\n", &mut sink);
    assert!(err.is_err(), "undefined prefix must error");

    let doc = "@prefix e: <http://a/> .\n\
               e:x <http://p/> \"1\" .\n";
    assert_eq!(subjects(doc), vec!["http://a/x"]);
}

/// F — pre-seeded map whose namespace is a RELATIVE reference while the
/// in-document declaration resolves against a base. The stored binding really
/// does change, so clearing is correct, not spurious.
#[test]
fn f_preseeded_relative_vs_resolved() {
    let seeded = [("e".to_string(), "a/".to_string())];
    let doc = "e:before <http://p/> \"0\" .\n\
               @base <http://x/> .\n\
               @prefix e: <a/> .\n\
               e:after <http://p/> \"1\" .\n";
    let mut sink = GraphCollectorSink::new();
    parse_with_prefixes_base(doc, &mut sink, &seeded, None).expect("parses");
    let subs: Vec<String> = sink
        .finish()
        .iter()
        .filter_map(|t| t.s.as_iri().map(str::to_string))
        .collect();
    assert_eq!(subs, vec!["a/before", "http://x/a/after"]);
}

/// G — local-name escapes take the non-cached branch on a miss but are cached
/// by span like everything else, so they must follow a rebind too.
#[test]
fn g_escaped_local_names_follow_the_rebind() {
    let doc = "@prefix e: <http://a/> .\n\
               e:a\\-b <http://p/> \"1\" .\n\
               @prefix e: <http://b/> .\n\
               e:a\\-b <http://p/> \"2\" .\n";
    assert_eq!(subjects(doc), vec!["http://a/a-b", "http://b/a-b"]);
}

/// H — mixing SPARQL-style and @-style declarations of the same prefix.
#[test]
fn h_mixed_directive_styles_rebind() {
    let doc = "@prefix e: <http://a/> .\n\
               e:x <http://p/> \"1\" .\n\
               PREFIX e: <http://b/>\n\
               e:x <http://p/> \"2\" .\n\
               @prefix e: <http://c/> .\n\
               e:x <http://p/> \"3\" .\n";
    assert_eq!(
        subjects(doc),
        vec!["http://a/x", "http://b/x", "http://c/x"]
    );
}
