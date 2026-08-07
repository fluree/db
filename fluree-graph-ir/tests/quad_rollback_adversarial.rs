//! Adversarial probes for `DatasetCollectorSink`'s rollback and refusal
//! paths, authored by the review of this branch and adopted verbatim.
//!
//! They attack the parts the implementation's own tests had the least
//! distance from: multi-id marking of one graph (A1, A2), whether a recycled
//! literal slot can dangle into a graph name (A3), what a refused quad leaves
//! behind (A4), and which layer actually enforces the graph-name invariant
//! (A10). A9 pins a known hole rather than a guarantee — directives emitted
//! during a statement are NOT rolled back by `abort_statement`, the same as
//! in `GraphCollectorSink`.

use fluree_graph_ir::{
    DatasetCollectorSink, Datatype, GraphSink, LiteralValue, SinkError, Term, TermId,
};

fn iri(sink: &mut DatasetCollectorSink, s: &str) -> TermId {
    sink.term_iri(s)
}

/// A1 — same graph reached through THREE distinct ids, marks recorded at
/// three different lengths. The earliest must win.
#[test]
fn a1_three_ids_one_graph_earliest_mark_wins() {
    let mut sink = DatasetCollectorSink::new();
    let s = iri(&mut sink, "http://ex/s");
    let p = iri(&mut sink, "http://ex/p");
    let o = iri(&mut sink, "http://ex/o");

    let g0 = iri(&mut sink, "http://ex/g");
    sink.emit_quad(s, p, o, g0).unwrap();
    sink.emit_quad(s, p, o, g0).unwrap();
    sink.end_statement();
    assert_eq!(sink.dataset().len(), 2);

    let ga = iri(&mut sink, "http://ex/g");
    let gb = iri(&mut sink, "http://ex/g");
    let gc = iri(&mut sink, "http://ex/g");
    assert!(ga != gb && gb != gc && ga != gc);
    sink.emit_quad(s, p, o, ga).unwrap(); // marks Some(2)
    sink.emit_quad(s, p, o, gb).unwrap(); // marks Some(3)
    sink.emit_quad(s, p, o, gc).unwrap(); // marks Some(4)
    sink.emit_quad(s, p, o, ga).unwrap(); // already marked
    sink.abort_statement();

    assert_eq!(
        sink.dataset().len(),
        2,
        "must rewind to the EARLIEST mark (2), not the last one"
    );
}

/// A2 — three ids where the FIRST is the creating one (mark None).
#[test]
fn a2_three_ids_first_creates_graph_removed_entirely() {
    let mut sink = DatasetCollectorSink::new();
    let s = iri(&mut sink, "http://ex/s");
    let p = iri(&mut sink, "http://ex/p");
    let o = iri(&mut sink, "http://ex/o");
    let ga = iri(&mut sink, "http://ex/g");
    let gb = iri(&mut sink, "http://ex/g");
    let gc = iri(&mut sink, "http://ex/g");

    sink.emit_quad(s, p, o, ga).unwrap(); // None
    sink.emit_quad(s, p, o, gb).unwrap(); // Some(1)
    sink.emit_quad(s, p, o, gc).unwrap(); // Some(2)
    sink.abort_statement();

    let d = sink.into_dataset();
    assert_eq!(
        d.named_graph_count(),
        0,
        "graph must be REMOVED, not emptied"
    );
}

/// A3 — THE recycled-slot question: can a graph TermId ever dangle onto a
/// live literal slot and be accepted?
#[test]
fn a3_graph_id_pointing_at_a_recycled_literal_slot_is_still_refused() {
    let mut sink = DatasetCollectorSink::new();
    let s = iri(&mut sink, "http://ex/s");
    let p = iri(&mut sink, "http://ex/p");
    let o = iri(&mut sink, "http://ex/o");

    let stale = sink.term_literal("stale", Datatype::xsd_string(), None);
    sink.emit_triple(s, p, stale).unwrap();
    sink.end_statement();

    let fresh = sink.term_literal("fresh", Datatype::xsd_string(), None);
    assert_eq!(stale, fresh, "slot must have been recycled for this probe");

    let err = sink
        .emit_quad(s, p, o, stale)
        .expect_err("a recycled literal slot must never be accepted as a graph");
    assert!(matches!(err, SinkError::Rejected(_)), "{err:?}");

    sink.end_statement();
    let native = sink.term_literal_value(LiteralValue::Integer(7), Datatype::xsd_integer());
    let err = sink.emit_quad(s, p, o, native).expect_err("still refused");
    assert!(matches!(err, SinkError::Rejected(_)), "{err:?}");
    sink.abort_statement();
    let d = sink.into_dataset();
    assert_eq!(d.named_graph_count(), 0, "no graph was ever created");
    assert_eq!(d.len(), 1, "only statement 1's committed triple");
}

/// A4 — a refusal mid-statement must not leave a mark behind that a later
/// abort acts on, and must not itself create the graph.
#[test]
fn a4_refused_quad_leaves_no_mark_and_no_graph() {
    let mut sink = DatasetCollectorSink::new();
    let s = iri(&mut sink, "http://ex/s");
    let p = iri(&mut sink, "http://ex/p");
    let o = iri(&mut sink, "http://ex/o");
    let good = iri(&mut sink, "http://ex/g");
    let bad = sink.term_literal("bad", Datatype::xsd_string(), None);

    sink.emit_quad(s, p, o, good).unwrap();
    let _ = sink.emit_quad(s, p, o, bad).expect_err("refused");
    sink.abort_statement();

    let d = sink.into_dataset();
    assert_eq!(
        d.named_graph_count(),
        0,
        "the good graph is rolled back too"
    );
    assert!(d.is_empty());
}

/// A5 — abort twice in a row (outside the documented contract, but must not
/// eat committed data if a driver gets it wrong).
#[test]
fn a5_double_abort_is_idempotent() {
    let mut sink = DatasetCollectorSink::new();
    let s = iri(&mut sink, "http://ex/s");
    let p = iri(&mut sink, "http://ex/p");
    let o = iri(&mut sink, "http://ex/o");
    let g = iri(&mut sink, "http://ex/g");
    sink.emit_quad(s, p, o, g).unwrap();
    sink.end_statement();

    let g2 = iri(&mut sink, "http://ex/g2");
    sink.emit_quad(s, p, o, g2).unwrap();
    sink.abort_statement();
    sink.abort_statement();
    sink.abort_statement();

    let d = sink.into_dataset();
    assert_eq!(
        d.len(),
        1,
        "the committed statement survives repeated aborts"
    );
    assert_eq!(d.named_graph_count(), 1);
}

/// A6 — abort on a virgin sink, and abort with only default-graph writes.
#[test]
fn a6_abort_with_nothing_in_flight() {
    let mut sink = DatasetCollectorSink::new();
    sink.abort_statement();
    assert!(sink.dataset().is_empty());

    let s = iri(&mut sink, "http://ex/s");
    let p = iri(&mut sink, "http://ex/p");
    let o = iri(&mut sink, "http://ex/o");
    sink.emit_triple(s, p, o).unwrap();
    sink.end_statement();
    sink.abort_statement();
    assert_eq!(sink.into_dataset().len(), 1);
}

/// A7 — blank-node graph names: same label across statements is one graph;
/// two anonymous mints are two graphs.
#[test]
fn a7_blank_graph_identity() {
    let mut sink = DatasetCollectorSink::new();
    let s = iri(&mut sink, "http://ex/s");
    let p = iri(&mut sink, "http://ex/p");
    let o = iri(&mut sink, "http://ex/o");

    let g1 = sink.term_blank(Some("g"));
    sink.emit_quad(s, p, o, g1).unwrap();
    sink.end_statement();
    let g2 = sink.term_blank(Some("g"));
    assert_eq!(g1, g2, "labeled blanks are session-scoped");
    sink.emit_quad(s, p, o, g2).unwrap();
    sink.end_statement();
    let g3 = sink.term_blank(None);
    let g4 = sink.term_blank(None);
    sink.emit_quad(s, p, o, g3).unwrap();
    sink.emit_quad(s, p, o, g4).unwrap();
    sink.end_statement();

    let d = sink.into_dataset();
    assert_eq!(d.named_graph_count(), 3, "one labeled + two distinct mints");
    assert_eq!(d.named_graph(&Term::blank("g")).unwrap().len(), 2);
}

/// A8 — cross-graph rollback where the pre-existing graph is touched FIRST
/// and the new one second, and the default graph in the middle.
#[test]
fn a8_mixed_order_rollback() {
    let mut sink = DatasetCollectorSink::new();
    let s = iri(&mut sink, "http://ex/s");
    let p = iri(&mut sink, "http://ex/p");
    let o = iri(&mut sink, "http://ex/o");

    let pre = iri(&mut sink, "http://ex/pre");
    sink.emit_quad(s, p, o, pre).unwrap();
    sink.emit_triple(s, p, o).unwrap();
    sink.end_statement();

    let pre2 = iri(&mut sink, "http://ex/pre");
    let new1 = iri(&mut sink, "http://ex/new1");
    let new2 = iri(&mut sink, "http://ex/new2");
    sink.emit_quad(s, p, o, pre2).unwrap();
    sink.emit_triple(s, p, o).unwrap();
    sink.emit_quad(s, p, o, new1).unwrap();
    sink.emit_quad(s, p, o, new2).unwrap();
    sink.emit_quad(s, p, o, pre2).unwrap();
    sink.abort_statement();

    let d = sink.into_dataset();
    assert_eq!(d.len(), 2, "only the committed statement's two survive");
    assert_eq!(d.named_graph_count(), 1);
    assert_eq!(d.named_graph(&Term::iri("http://ex/pre")).unwrap().len(), 1);
    assert_eq!(d.default_graph().len(), 1);
}

/// A9 — directives emitted during a statement that then ABORTS are NOT
/// rolled back. Documents the behavior; same hole GraphCollectorSink has.
#[test]
fn a9_aborted_directive_leaves_its_prefix_behind() {
    let mut sink = DatasetCollectorSink::new();
    sink.on_prefix("ex", "http://example.org/");
    sink.on_base("http://base.example/");
    sink.abort_statement();

    let d = sink.into_dataset();
    assert_eq!(
        d.prefixes().get("ex"),
        Some(&"http://example.org/".to_string())
    );
    assert_eq!(d.base(), Some("http://base.example/"));
}

/// A10 — the Dataset type itself does not enforce the graph-name invariant;
/// only the sink does. Confirms where the guard actually lives.
#[test]
fn a10_dataset_itself_accepts_a_literal_key() {
    use fluree_graph_ir::{Dataset, Quad, Triple};
    let mut d = Dataset::new();
    let t = Triple::new(
        Term::iri("http://ex/s"),
        Term::iri("http://ex/p"),
        Term::iri("http://ex/o"),
    );
    d.add_quad(Quad::in_named_graph(t, Term::string("literal graph")));
    assert_eq!(
        d.named_graph_count(),
        1,
        "Dataset stores it; only the SINK refuses"
    );
}
