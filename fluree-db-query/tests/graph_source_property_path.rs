//! SPARQL path quantifiers (`+`, `*`, `?`) over an Iceberg/R2RML graph source.
//!
//! Grown out of the triage of fluree/azure-chat#56. The reporter's graph is
//! `n1 -edge-> n2 -edge-> n3`, queried from `n1` over a graph source. On Fluree
//! 4.1.3/4.1.4 they measured `edge+` -> empty, `edge*` -> `{n1}`, `edge?` ->
//! `{n1}` (identity only), while fixed-length `edge` and `edge/edge` were
//! correct — all with HTTP 200.
//!
//! Three arms:
//!
//! * `prefix_*` reproduces the MECHANISM of the reported behavior. A
//!   graph-source view is a `LedgerSnapshot::genesis(gs_id)` — an EMPTY native
//!   snapshot (see `fluree-db-api/src/view/fluree_ext.rs`). `PropertyPathOperator`
//!   reads edges only through `range_with_overlay` over that snapshot's SPOT/POST/PSOT
//!   indexes (`fluree-db-query/src/property_path.rs`), so it sees zero edges. The
//!   arm asserts the exact reported matrix falls out of running the path operator
//!   against a genesis snapshot: `+` empty, `*`/`?` identity-only.
//!
//! * `guard_*` asserts what the engine does on the same route: the R2RML
//!   rewrite flags a residual `Pattern::PropertyPath` as an unsupported sub-scope
//!   (`fluree-db-query/src/r2rml/rewrite.rs`) and `GraphOperator` refuses the
//!   query (`fluree-db-query/src/graph.rs`), instead of returning the silent
//!   wrong answer. A fixed-length triple in the same block still lowers to an
//!   R2RML scan and reaches the provider.
//!
//! * `outside_a_graph_scope_*` covers the residual the triage found: that guard
//!   lives on the `GRAPH`-block path, so a path pattern the auto-wrap left at the
//!   top level never reaches it. `unsupported_outside_graph_scopes` is the scan
//!   the API layer runs before execution to refuse those; here we pin both that
//!   it flags them and that the executor alone does not, which is why the check
//!   has to exist at all. The end-to-end routing assertions live in
//!   `fluree-db-api/src/view/query.rs`, where the graph-source view is built.

use async_trait::async_trait;
use fluree_db_core::{GraphDbRef, LedgerSnapshot, NoOverlay, Sid, Tracker};
use fluree_db_query::ir::path::{PathModifier, PropertyPathPattern};
use fluree_db_query::ir::triple::{Ref, Term, TriplePattern};
use fluree_db_query::ir::{GraphName, Pattern, Query, QueryOutput};
use fluree_db_query::r2rml::{
    unsupported_outside_graph_scopes, unsupported_subscope_error, ColumnBatchStream, R2rmlProvider,
    R2rmlTableProvider, ScanFilter, ScanTopK,
};
use fluree_db_query::{execute, ContextConfig, ExecutableQuery, VarRegistry};
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use fluree_graph_json_ld::ParsedContext;
use std::sync::Arc;

const GS_ID: &str = "fs3repro:main";
const NS_CODE: u16 = 9_999;
const NS_IRI: &str = "http://fs3.example/";
const EDGE_PRED: &str = "http://fs3.example/vocab#edge";
const N1: &str = "http://fs3.example/node/n1";

/// Marker the stub provider errors with, so a test can prove a pattern actually
/// reached the R2RML scan layer rather than being refused during the rewrite.
const SCAN_MARKER: &str = "AUDIT56-SCAN-REACHED";

/// Minimal stub standing in for `FlureeR2rmlProvider`: it reports a mapping for
/// the graph source (which is all the rewrite decision needs) and fails loudly
/// with `SCAN_MARKER` if a pattern gets as far as scanning a table.
#[derive(Debug)]
struct StubProvider {
    mapping: Arc<CompiledR2rmlMapping>,
}

impl StubProvider {
    fn new() -> Self {
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap, TriplesMap};
        let tm = TriplesMap::new("#Edge", "fs3.edge")
            .with_subject_template("http://fs3.example/node/{src}")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(EDGE_PRED),
                object_map: ObjectMap::column("dst"),
            });
        Self {
            mapping: Arc::new(CompiledR2rmlMapping::new(vec![tm])),
        }
    }
}

#[async_trait]
impl R2rmlProvider for StubProvider {
    async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
        true
    }

    async fn compiled_mapping(
        &self,
        _graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> fluree_db_query::Result<Arc<CompiledR2rmlMapping>> {
        Ok(Arc::clone(&self.mapping))
    }
}

#[async_trait]
impl R2rmlTableProvider for StubProvider {
    async fn scan_table(
        &self,
        _graph_source_id: &str,
        _table_name: &str,
        _projection: &[String],
        _filters: &[ScanFilter],
        _topk: Option<&ScanTopK>,
        _as_of_t: Option<i64>,
    ) -> fluree_db_query::Result<ColumnBatchStream> {
        Err(fluree_db_query::QueryError::Internal(
            SCAN_MARKER.to_string(),
        ))
    }
}

/// The snapshot a graph-source-addressed query actually runs against: a genesis
/// (zero-flake) `LedgerSnapshot` named for the graph source, with the fixture's
/// namespace registered so IRIs encode to `Sid`s.
fn graph_source_snapshot() -> LedgerSnapshot {
    let mut snapshot = LedgerSnapshot::genesis(GS_ID);
    snapshot
        .insert_namespace_code(NS_CODE, NS_IRI.to_string())
        .expect("namespace registration");
    snapshot
}

fn encode(snapshot: &LedgerSnapshot, iri: &str) -> Sid {
    snapshot
        .encode_iri(iri)
        .unwrap_or_else(|| panic!("{iri} should encode against the registered namespace"))
}

/// `<n1> edge{modifier} ?o` as a bare top-level pattern — the shape the path
/// operator sees when nothing lowered it to an R2RML scan.
fn path_query(snapshot: &LedgerSnapshot, modifier: PathModifier) -> (VarRegistry, Query) {
    let mut vars = VarRegistry::new();
    let o = vars.get_or_insert("?o");
    let pattern = Pattern::PropertyPath(PropertyPathPattern::new(
        Ref::Sid(encode(snapshot, N1)),
        encode(snapshot, EDGE_PRED),
        modifier,
        Ref::Var(o),
    ));
    let mut query = Query::new(ParsedContext::default());
    query.patterns = vec![pattern];
    query.output = QueryOutput::select_all(vec![o]);
    (vars, query)
}

/// Wrap `inner` in `GRAPH <fs3repro:main> { … }`, exactly as
/// `maybe_wrap_for_graph_source` does for a query addressed to a graph source
/// (`fluree-db-api/src/view/query.rs`).
fn wrap_in_graph(query: &mut Query) {
    let inner = std::mem::take(&mut query.patterns);
    query.patterns = vec![Pattern::Graph {
        name: GraphName::Iri(GS_ID.into()),
        patterns: inner,
    }];
}

async fn run(
    snapshot: &LedgerSnapshot,
    vars: &VarRegistry,
    query: Query,
    provider: Option<&StubProvider>,
) -> fluree_db_query::Result<usize> {
    let tracker = Tracker::disabled();
    let executable = ExecutableQuery::simple(query);
    let config = ContextConfig {
        tracker: Some(&tracker),
        r2rml: provider.map(|p| (p as &dyn R2rmlProvider, p as &dyn R2rmlTableProvider)),
        ..Default::default()
    };
    let batches = execute(
        GraphDbRef::new(snapshot, 0, &NoOverlay, 0),
        vars,
        &executable,
        config,
    )
    .await?;
    Ok(batches.iter().map(fluree_db_query::Batch::len).sum())
}

// ---------------------------------------------------------------------------
// Arm 1 — the reported (pre-fix) behavior falls out of the empty genesis snapshot
// ---------------------------------------------------------------------------

#[tokio::test]
async fn prefix_plus_is_empty_over_a_graph_source_snapshot() {
    let snapshot = graph_source_snapshot();
    let (vars, query) = path_query(&snapshot, PathModifier::OneOrMore);
    let rows = run(&snapshot, &vars, query, None)
        .await
        .expect("path over a genesis snapshot executes without error");
    assert_eq!(
        rows, 0,
        "reported behavior: `edge+` returns NOTHING over a graph source \
         (expected {{n2, n3}} per SPARQL 1.1 §9.1)"
    );
}

#[tokio::test]
async fn prefix_star_is_identity_only_over_a_graph_source_snapshot() {
    let snapshot = graph_source_snapshot();
    let (vars, query) = path_query(&snapshot, PathModifier::ZeroOrMore);
    let rows = run(&snapshot, &vars, query, None)
        .await
        .expect("path over a genesis snapshot executes without error");
    assert_eq!(
        rows, 1,
        "reported behavior: `edge*` returns ONLY the zero-length identity match \
         (expected {{n1, n2, n3}} per SPARQL 1.1 §9.1)"
    );
}

#[tokio::test]
async fn prefix_question_is_identity_only_over_a_graph_source_snapshot() {
    let snapshot = graph_source_snapshot();
    let (vars, query) = path_query(&snapshot, PathModifier::ZeroOrOne);
    let rows = run(&snapshot, &vars, query, None)
        .await
        .expect("path over a genesis snapshot executes without error");
    assert_eq!(
        rows, 1,
        "reported behavior: `edge?` returns ONLY the zero-length identity match \
         (expected {{n1, n2}} per SPARQL 1.1 §9.1)"
    );
}

// ---------------------------------------------------------------------------
// Arm 2 — origin/main refuses the same query instead of answering it wrongly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn guard_refuses_every_quantifier_over_an_r2rml_graph_source() {
    let snapshot = graph_source_snapshot();
    let provider = StubProvider::new();

    for (label, modifier) in [
        ("edge+", PathModifier::OneOrMore),
        ("edge*", PathModifier::ZeroOrMore),
        ("edge?", PathModifier::ZeroOrOne),
    ] {
        let (vars, mut query) = path_query(&snapshot, modifier);
        wrap_in_graph(&mut query);
        let err = run(&snapshot, &vars, query, Some(&provider))
            .await
            .expect_err(&format!(
                "{label} over an R2RML graph source must be REFUSED, not answered"
            ));
        let msg = err.to_string();
        assert!(
            msg.contains("property path") && msg.contains("cannot be evaluated"),
            "{label}: expected the unsupported-sub-scope refusal, got: {msg}"
        );
    }
}

/// The contrast the reporter observed: a fixed-length pattern in the same block
/// is NOT refused — it lowers to an R2RML scan and reaches the provider (here,
/// the stub's `SCAN_MARKER`). So the guard is specific to quantified paths.
#[tokio::test]
async fn guard_does_not_refuse_a_fixed_length_pattern() {
    let snapshot = graph_source_snapshot();
    let provider = StubProvider::new();

    let mut vars = VarRegistry::new();
    let o = vars.get_or_insert("?o");
    let mut query = Query::new(ParsedContext::default());
    query.patterns = vec![Pattern::Triple(TriplePattern::new(
        Ref::Sid(encode(&snapshot, N1)),
        Ref::Sid(encode(&snapshot, EDGE_PRED)),
        Term::Var(o),
    ))];
    query.output = QueryOutput::select_all(vec![o]);
    wrap_in_graph(&mut query);

    let err = run(&snapshot, &vars, query, Some(&provider))
        .await
        .expect_err("the stub provider fails the scan on purpose");
    let msg = err.to_string();
    assert!(
        msg.contains(SCAN_MARKER),
        "a fixed-length pattern must reach the R2RML scan (not the path guard), got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Arm 3 — the same pattern OUTSIDE a GRAPH scope, which that guard cannot see
// ---------------------------------------------------------------------------

/// The mixed shape: a top-level quantifier next to the user's own `GRAPH` block.
/// `maybe_wrap_for_graph_source` skips the auto-wrap for the whole query once any
/// `GRAPH` block is present, so the path stays at the top level — outside every
/// scope the rewrite guard inspects.
fn mixed_shape(snapshot: &LedgerSnapshot, modifier: PathModifier) -> (VarRegistry, Query) {
    let (mut vars, mut query) = path_query(snapshot, modifier);
    let a = vars.get_or_insert("?a");
    let c = vars.get_or_insert("?c");
    query.patterns.push(Pattern::Graph {
        name: GraphName::Iri(GS_ID.into()),
        patterns: vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(a),
            Ref::Sid(encode(snapshot, EDGE_PRED)),
            Term::Var(c),
        ))],
    });
    (vars, query)
}

/// Executing the mixed shape does NOT refuse: the path is outside the `GRAPH`
/// block, so no rewrite ever inspects it and it reads the empty genesis index
/// instead. It succeeds with the reporter's wrong answer — and it never even
/// reaches the R2RML scan, because the empty path result leaves the correlated
/// `GRAPH` block with no parent row to scan for. This is the bypass, and the
/// reason the check has to run before execution rather than as another guard
/// inside `GraphOperator`.
#[tokio::test]
async fn outside_a_graph_scope_the_executor_guard_never_fires() {
    let snapshot = graph_source_snapshot();
    let provider = StubProvider::new();
    let (vars, query) = mixed_shape(&snapshot, PathModifier::OneOrMore);

    let rows = run(&snapshot, &vars, query, Some(&provider))
        .await
        .expect("the mixed shape is answered, not refused — that is the bug");
    assert_eq!(
        rows, 0,
        "`edge+` beside a GRAPH block still returns the silent-empty wrong answer"
    );
}

/// What the API layer runs before execution to close that bypass: every
/// quantifier at the top level is flagged, under the same kind name the rewrite
/// guard uses, whether or not the query also carries a `GRAPH` block.
#[tokio::test]
async fn outside_a_graph_scope_the_pre_execution_scan_flags_every_quantifier() {
    let snapshot = graph_source_snapshot();

    for (label, modifier) in [
        ("edge+", PathModifier::OneOrMore),
        ("edge*", PathModifier::ZeroOrMore),
        ("edge?", PathModifier::ZeroOrOne),
    ] {
        let (_, mixed) = mixed_shape(&snapshot, modifier);
        assert_eq!(
            unsupported_outside_graph_scopes(&mixed.patterns),
            vec!["property path"],
            "{label} beside a GRAPH block must be flagged"
        );

        let (_, bare) = path_query(&snapshot, modifier);
        assert_eq!(
            unsupported_outside_graph_scopes(&bare.patterns),
            vec!["property path"],
            "{label} alone must be flagged the same way"
        );
    }
}

/// The refusal the API layer raises from that scan is the same
/// `QueryError::InvalidQuery` (HTTP 400) the rewrite guard raises, naming the
/// graph source and the pattern kind.
#[test]
fn the_refusal_is_the_shared_unsupported_subscope_error() {
    let err = unsupported_subscope_error(&[GS_ID], &["property path"]);
    assert!(
        matches!(err, fluree_db_query::QueryError::InvalidQuery(_)),
        "must be the 400-mapped variant, got: {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains(GS_ID) && msg.contains("property path"),
        "names the graph source and the kind: {msg}"
    );
}
