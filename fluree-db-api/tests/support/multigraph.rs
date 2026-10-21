//! Fixture A, its scopes and its answers, for the lanes that read several
//! graphs of one ledger (design: round4/MULTIGRAPH-LANES-DESIGN.md §5).
//!
//! A lane test picks a [`Shape`], runs it under every [`Scope`] with
//! [`check`], and gets back what ran: the answer with fast paths on and with
//! them off (both compared with the hand-written [`expected`] answer), the
//! `index scope` stamp, and the fast-path sites that proceeded or declined, for
//! [`assert_routing`].
//!
//! The answers are written by hand from the fixture below, never taken from
//! the general plan: [`expected`] is the oracle both pipelines answer to.
//!
//! Fast paths are switched off process-wide (`set_fast_paths_disabled`), so a
//! test binary using this module must be its own `[[test]]` target, and
//! [`check`] holds a lock across each on/off pair.

// Kept for: the lane tests (C, D, E, B, A) that build on this module; the
// fixture's own validation does not use every helper.
#![allow(dead_code)]

use crate::span_capture::{init_test_tracing, SpanStore};
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, GraphDb, ReindexOptions};
use fluree_db_binary_index::RunSortOrder;
use serde_json::{json, Value as JsonValue};
use std::sync::OnceLock;

pub const EX: &str = "http://example.org/";
/// Fixture A: graphs g0 (the default graph), `urn:g1` and `urn:g2`.
pub const LEDGER: &str = "it/mg-lanes:main";
/// Fixture A again, with `f:unionDefaultGraph` set in its config graph.
pub const UNION_LEDGER: &str = "it/mg-lanes-union:main";
/// A second ledger, for a default graph that spans ledgers.
pub const OTHER_LEDGER: &str = "it/mg-lanes-other:main";
/// [`FIXTURE_A_EMPTY_G0`]: fixture A with an empty default graph.
pub const EMPTY_G0_LEDGER: &str = "it/mg-lanes-empty-g0:main";
/// [`FIXTURE_A_EMPTY_G0`] again, with `f:unionDefaultGraph` set.
pub const EMPTY_G0_UNION_LEDGER: &str = "it/mg-lanes-empty-g0-union:main";
pub const G1: &str = "urn:g1";
pub const G2: &str = "urn:g2";

/// The parts of fixture A that [`FIXTURE_A`] and [`FIXTURE_A_EMPTY_G0`] share:
/// its prefixes and its named graphs (what they hold is under [`FIXTURE_A`]).
macro_rules! fixture_a {
    (prefixes) => {
        "
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"
    };
    (named) => {
        r#"
GRAPH <urn:g1> {
  ex:s1 ex:n 1 .
  ex:s2 ex:n 2 ; ex:label "s2" .
  ex:s3 ex:n 3 .
  ex:k1 ex:kind ex:T1 .
  ex:k2 ex:kind ex:T1 .
  ex:b ex:big 2.5 .
  ex:t1a ex:knows ex:t1b . ex:t1b ex:knows ex:t1c . ex:t1c ex:knows ex:t1a .
  ex:t2a ex:knows ex:t2b . ex:t2b ex:knows ex:t2c .
  ex:t3a ex:knows ex:t3b . ex:t3b ex:knows ex:t3c . ex:t3c ex:knows ex:t3a .
  ex:q1 ex:m 2 . ex:q2 ex:m 2 .
}
GRAPH <urn:g2> {
  ex:s1 ex:n 2 .
  ex:s2 ex:n 2 .
  ex:s3 ex:label "s3" .
  ex:k1 ex:kind ex:T1 .
  ex:k3 ex:kind ex:T1 .
  ex:o ex:only 7 .
  ex:b ex:big 2.5 .
  ex:L ex:items ( "v" "v" ) .
  ex:t2c ex:knows ex:t2a .
  ex:t3a ex:knows ex:t3b . ex:t3b ex:knows ex:t3c . ex:t3c ex:knows ex:t3a .
  ex:q1 ex:m 2.0e0 . ex:q2 ex:m 2 . ex:q3 ex:m 2.0e0 . ex:q4 ex:m "2"^^xsd:short .
}
"#
    };
}

/// Fixture A, written at t 1.
///
/// - an identical triple in g1 and g2: `s2 n 2`, `k1 kind T1`, `b big 2.5`
///   and triangle t3;
/// - subjects shared by g1 and g2 with different objects: `s1 n 1` / `s1 n 2`,
///   and `s3` (`n 3` in g1, its label in g2);
/// - a predicate only g2 has: `ex:only`;
/// - a `@list` value repeated in one member: `L items ("v" "v")` in g2, two
///   rows that differ only in their list index;
/// - one NumBig per graph on one predicate: `b big` is `1.5` in g0 and `2.5`
///   in g1 and g2 (every `xsd:decimal` is a NumBig arena handle, numbered per
///   graph, so all three are handle 0 of their graph's arena);
/// - triangles over `ex:knows`: t1 inside g1, t2 spanning g1 and g2 (its
///   closing edge `t2c → t2a` is in g2), t3 in g1 and again in g2;
/// - a constant that matches facts of three datatypes: `?s ex:m 2` matches
///   `2` (`xsd:integer`), `2.0e0` (`xsd:double`) and `"2"^^xsd:short`. `q1`
///   holds `2` in g1 and `2.0e0` in g2 (two facts), `q2` holds `2` in both
///   (one fact).
pub const FIXTURE_A: &str = concat!(
    fixture_a!(prefixes),
    r#"
ex:a1 ex:n 1 ; ex:label "a1" .
ex:a2 ex:n 0 ; ex:label "a2" .
ex:k0 ex:kind ex:T1 .
ex:b ex:big 1.5 .
"#,
    fixture_a!(named)
);

/// Fixture A's named graphs g1 and g2 alone: a ledger whose default graph
/// holds no facts, so its index has no leaf for it in any order. Every
/// union over it answers as g1 and g2 do; the empty member contributes
/// nothing, and is no reason for a lane to decline.
pub const FIXTURE_A_EMPTY_G0: &str = concat!(fixture_a!(prefixes), fixture_a!(named));

/// The second ledger's default graph.
pub const FIXTURE_OTHER: &str = r#"
@prefix ex: <http://example.org/> .
ex:x1 ex:n 5 ; ex:label "x1" ; ex:kind ex:T1 .
"#;

/// The write at t 2, to g2 only: a new `kind T1` subject, a second `n` for
/// `s3` (which has its label in g2), and t2's first two edges, so all of t2
/// is in g2 too.
pub const WRITE_TO_G2: &str = r"
@prefix ex: <http://example.org/> .
GRAPH <urn:g2> {
  ex:k4 ex:kind ex:T1 .
  ex:s3 ex:n 4 .
  ex:t2a ex:knows ex:t2b . ex:t2b ex:knows ex:t2c .
}
";

fn union_config(ledger: &str) -> String {
    format!(
        "@prefix f: <https://ns.flur.ee/db#> .
         GRAPH <urn:fluree:{ledger}#config> {{
           <urn:config:main> a f:LedgerConfig ; f:queryDefaults <urn:config:query> .
           <urn:config:query> f:unionDefaultGraph true . }}"
    )
}

// ============================================================================
// Fixture states
// ============================================================================

/// The state the fixture's ledgers are in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Fixture A at t 1, indexed, no novelty.
    Indexed,
    /// [`WRITE_TO_G2`] committed at t 2 and not indexed: novelty in g2.
    Novelty,
    /// [`WRITE_TO_G2`] committed and indexed; [`Scope::TimeTravel`] reads t 1,
    /// below the index.
    Indexed2,
}

/// Fixture A (twice: plain and with `f:unionDefaultGraph`), the same pair
/// with an empty default graph, and the second ledger, on one in-memory
/// connection with background indexing off, so only explicit index builds
/// change what is indexed.
pub struct Fixture {
    pub fluree: Fluree,
    pub state: State,
}

impl Fixture {
    pub async fn build(state: State) -> Self {
        let fluree = FlureeBuilder::memory().without_indexing().build_memory();
        let config = union_config(UNION_LEDGER);
        seed(&fluree, LEDGER, FIXTURE_A).await;
        seed(&fluree, UNION_LEDGER, &format!("{FIXTURE_A}\n{config}")).await;
        seed(&fluree, OTHER_LEDGER, FIXTURE_OTHER).await;
        let config = union_config(EMPTY_G0_UNION_LEDGER);
        seed(&fluree, EMPTY_G0_LEDGER, FIXTURE_A_EMPTY_G0).await;
        seed(
            &fluree,
            EMPTY_G0_UNION_LEDGER,
            &format!("{FIXTURE_A_EMPTY_G0}\n{config}"),
        )
        .await;
        if state != State::Indexed {
            for ledger in [LEDGER, UNION_LEDGER, EMPTY_G0_LEDGER, EMPTY_G0_UNION_LEDGER] {
                let handle = fluree.ledger_cached(ledger).await.expect("ledger");
                fluree
                    .stage(&handle)
                    .upsert_turtle(WRITE_TO_G2)
                    .execute()
                    .await
                    .expect("write to g2");
                if state == State::Indexed2 {
                    index(&fluree, ledger).await;
                }
            }
        }
        for ledger in [EMPTY_G0_LEDGER, EMPTY_G0_UNION_LEDGER] {
            assert_no_default_graph_leaf(&fluree, ledger).await;
        }
        Self { fluree, state }
    }

    /// A view of `ledger`'s head, as a plain query on it reads.
    pub async fn view(&self, ledger: &str) -> GraphDb {
        let handle = self.fluree.ledger_cached(ledger).await.expect("ledger");
        GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state())
    }
}

async fn seed(fluree: &Fluree, ledger: &str, trig: &str) {
    fluree.create_ledger(ledger).await.expect("create ledger");
    let handle = fluree.ledger_cached(ledger).await.expect("cache");
    fluree
        .stage(&handle)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect("seed");
    index(fluree, ledger).await;
}

/// The empty default graph is what the scopes over [`EMPTY_G0_LEDGER`] are
/// for: its index must hold no leaf for g0 in any order, and one for g1. The
/// build here leaves g0 out of the root; a root an older bulk import wrote can
/// instead give it a SPOT with no leaves (#2118). A lane must take either as a
/// member with no facts.
async fn assert_no_default_graph_leaf(fluree: &Fluree, ledger: &str) {
    let handle = fluree.ledger_cached(ledger).await.expect("ledger");
    let view = handle.snapshot().await;
    let store = view.binary_store.as_ref().expect("indexed");
    let g1 = store.graph_id_for_iri(G1).expect("g1 is indexed");
    for &order in RunSortOrder::all_build_orders() {
        assert!(
            store
                .branch_for_order(0, order)
                .is_none_or(|branch| branch.leaves.is_empty()),
            "{ledger}: no {order:?} leaf for the default graph"
        );
        assert!(
            store
                .branch_for_order(g1, order)
                .is_some_and(|branch| !branch.leaves.is_empty()),
            "{ledger}: {order:?} leaves for g1"
        );
    }
}

/// Build `ledger`'s index and reload it, so the cached handle carries no
/// novelty.
async fn index(fluree: &Fluree, ledger: &str) {
    fluree
        .reindex(ledger, ReindexOptions::default())
        .await
        .expect("index");
    fluree.disconnect_ledger(ledger).await;
    let handle = fluree.ledger_cached(ledger).await.expect("reload");
    let view = handle.snapshot().await;
    assert!(view.binary_store.is_some(), "{ledger}: indexed");
    assert!(
        view.novelty.is_empty(),
        "{ledger}: no novelty after indexing"
    );
}

// ============================================================================
// Scopes (design §5's table)
// ============================================================================

/// The query surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Surface {
    Sparql,
    JsonLd,
}

/// Every scope of design §5's table, and the union scopes over a default graph
/// the index holds no leaf for. A scope says what the query's default graph
/// is, the `index scope` stamp it must get, and which answer it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// The ledger's default graph, no dataset.
    DefaultGraph,
    /// `FROM <urn:g1>`.
    FromG1,
    /// `FROM <urn:g1> FROM <urn:g2>`, within the ledger (SPARQL on the ledger's
    /// view); JSON-LD `from: [L#urn:g1, L#urn:g2]` through the connection.
    FromG1G2,
    /// `FROM <L#urn:g1> FROM <L#urn:g2>` through the connection, whose sources
    /// load the ledger themselves (`LedgerLoads`).
    FromG1G2Connection,
    /// `FROM <L> FROM <urn:g1> FROM <urn:g2>`.
    FromG0G1G2,
    /// The union default graph by `# PRAGMA union-default-graph` (SPARQL only).
    UnionPragma,
    /// The union default graph by `opts.unionDefaultGraph` (JSON-LD only).
    UnionOpts,
    /// The union default graph by the ledger's `f:unionDefaultGraph`.
    UnionConfig,
    /// `FROM NAMED <urn:g2>` alone: a default graph of none.
    FromNamedOnly,
    /// `GRAPH <urn:g2> { … }`, no dataset.
    GraphG2,
    /// `GRAPH ?g { … }`, no dataset: g1 and g2, each with its `?g`.
    GraphVar,
    /// `FROM <L> FROM <other ledger>`: a default graph that spans ledgers.
    TwoLedgers,
    /// `FROM <L@t:1#urn:g1> FROM <L@t:1#urn:g2>`, for [`State::Indexed2`]:
    /// both members at t 1, below the index.
    TimeTravel,
    /// [`Scope::UnionPragma`] on [`EMPTY_G0_LEDGER`]: a union of three
    /// members, one of them a default graph the index holds no leaf for.
    UnionPragmaEmptyG0,
    /// [`Scope::UnionOpts`] on [`EMPTY_G0_LEDGER`].
    UnionOptsEmptyG0,
    /// [`Scope::UnionConfig`] on [`EMPTY_G0_UNION_LEDGER`].
    UnionConfigEmptyG0,
    /// `FROM <E> FROM <urn:g1>` on [`EMPTY_G0_LEDGER`]: a union of the empty
    /// default graph and g1.
    FromEmptyG0G1,
}

/// The scopes every lane runs: design §5's order, then the empty default
/// graph's.
pub const SCOPES: &[Scope] = &[
    Scope::FromG1G2,
    Scope::FromG1G2Connection,
    Scope::FromG0G1G2,
    Scope::UnionPragma,
    Scope::UnionOpts,
    Scope::UnionConfig,
    Scope::FromNamedOnly,
    Scope::FromG1,
    Scope::DefaultGraph,
    Scope::GraphG2,
    Scope::GraphVar,
    Scope::TwoLedgers,
    Scope::TimeTravel,
    Scope::UnionPragmaEmptyG0,
    Scope::UnionOptsEmptyG0,
    Scope::UnionConfigEmptyG0,
    Scope::FromEmptyG0G1,
];

/// Which graphs a scope's answer reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Graphs {
    G0,
    G1,
    G2,
    G1G2,
    G0G1G2,
    /// The ledger's default graph and the other ledger's.
    G0Other,
    /// A default graph of none.
    Empty,
    /// One row set per named graph (g1, g2), each row led by its graph.
    PerNamedGraph,
    /// g1 and g2 with an empty default graph: g1 and g2's answer.
    G1G2WithEmptyG0,
    /// g1 with an empty default graph: g1's answer.
    G1WithEmptyG0,
}

/// What `ExecutionContext::index_scope` makes of a scope's default graph, for
/// a lane to route on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// One graph's index.
    One,
    /// Two or more graphs of one ledger, on one index.
    Union,
    /// A default graph of none.
    Empty,
    /// No index scope: the default graph spans ledgers.
    None,
}

impl Scope {
    pub fn name(self) -> &'static str {
        match self {
            Scope::DefaultGraph => "the default graph",
            Scope::FromG1 => "FROM g1",
            Scope::FromG1G2 => "FROM g1 FROM g2",
            Scope::FromG1G2Connection => "FROM L#g1 FROM L#g2 (connection)",
            Scope::FromG0G1G2 => "FROM g0 FROM g1 FROM g2",
            Scope::UnionPragma => "union default graph, PRAGMA",
            Scope::UnionOpts => "union default graph, opts",
            Scope::UnionConfig => "union default graph, f:unionDefaultGraph",
            Scope::FromNamedOnly => "FROM NAMED g2 alone",
            Scope::GraphG2 => "GRAPH <g2>",
            Scope::GraphVar => "GRAPH ?g",
            Scope::TwoLedgers => "two ledgers",
            Scope::TimeTravel => "FROM L@t:1#g1 FROM L@t:1#g2",
            Scope::UnionPragmaEmptyG0 => "union default graph, PRAGMA, empty g0",
            Scope::UnionOptsEmptyG0 => "union default graph, opts, empty g0",
            Scope::UnionConfigEmptyG0 => "union default graph, f:unionDefaultGraph, empty g0",
            Scope::FromEmptyG0G1 => "FROM empty g0 FROM g1",
        }
    }

    /// Whether `surface` can spell this scope.
    pub fn supports(self, surface: Surface) -> bool {
        match self {
            Scope::UnionPragma | Scope::UnionPragmaEmptyG0 => surface == Surface::Sparql,
            Scope::UnionOpts | Scope::UnionOptsEmptyG0 => surface == Surface::JsonLd,
            _ => true,
        }
    }

    /// Whether this scope applies in `state` (time travel needs a later index).
    pub fn applies(self, state: State) -> bool {
        self != Scope::TimeTravel || state == State::Indexed2
    }

    pub fn graphs(self) -> Graphs {
        match self {
            Scope::DefaultGraph => Graphs::G0,
            Scope::FromG1 => Graphs::G1,
            Scope::FromG1G2 | Scope::FromG1G2Connection | Scope::TimeTravel => Graphs::G1G2,
            Scope::FromG0G1G2 | Scope::UnionPragma | Scope::UnionOpts | Scope::UnionConfig => {
                Graphs::G0G1G2
            }
            Scope::FromNamedOnly => Graphs::Empty,
            Scope::GraphG2 => Graphs::G2,
            Scope::GraphVar => Graphs::PerNamedGraph,
            Scope::TwoLedgers => Graphs::G0Other,
            Scope::UnionPragmaEmptyG0 | Scope::UnionOptsEmptyG0 | Scope::UnionConfigEmptyG0 => {
                Graphs::G1G2WithEmptyG0
            }
            Scope::FromEmptyG0G1 => Graphs::G1WithEmptyG0,
        }
    }

    pub fn kind(self) -> Kind {
        match self {
            Scope::DefaultGraph | Scope::FromG1 | Scope::GraphG2 | Scope::GraphVar => Kind::One,
            Scope::FromG1G2
            | Scope::FromG1G2Connection
            | Scope::FromG0G1G2
            | Scope::UnionPragma
            | Scope::UnionOpts
            | Scope::UnionConfig
            | Scope::TimeTravel
            | Scope::UnionPragmaEmptyG0
            | Scope::UnionOptsEmptyG0
            | Scope::UnionConfigEmptyG0
            | Scope::FromEmptyG0G1 => Kind::Union,
            Scope::FromNamedOnly => Kind::Empty,
            Scope::TwoLedgers => Kind::None,
        }
    }

    /// The state the answer reads: time travel reads the fixture at t 1.
    fn answer_state(self, state: State) -> State {
        if self == Scope::TimeTravel {
            State::Indexed
        } else {
            state
        }
    }

    /// The `index scope` stamps the query must get, as `site=scope/members/
    /// current` (see `it_index_scope`): the query's, then each GRAPH's.
    pub fn stamps(self, state: State) -> Vec<String> {
        // Novelty is per ledger: any write the index lacks makes every scope
        // of that ledger stale.
        let current = if state == State::Novelty {
            "stale"
        } else {
            "current"
        };
        let q = |s: &str| format!("query={s}/{current}");
        let g = || format!("graph=one/1/{current}");
        match self {
            Scope::DefaultGraph | Scope::FromG1 => vec![q("one/1")],
            Scope::FromG1G2 | Scope::FromG1G2Connection | Scope::FromEmptyG0G1 => {
                vec![q("union/2")]
            }
            Scope::FromG0G1G2
            | Scope::UnionPragma
            | Scope::UnionOpts
            | Scope::UnionConfig
            | Scope::UnionPragmaEmptyG0
            | Scope::UnionOptsEmptyG0
            | Scope::UnionConfigEmptyG0 => vec![q("union/3")],
            // No member to be stale: an empty scope reads no index.
            Scope::FromNamedOnly => vec!["query=empty/0/current".to_string()],
            Scope::GraphG2 | Scope::GraphVar => vec![q("one/1"), g()],
            Scope::TwoLedgers => vec!["query=none/0/stale".to_string()],
            // Both members at t 1 under an index at t 2.
            Scope::TimeTravel => vec!["query=union/2/stale".to_string()],
        }
    }

    /// The ledger whose view or `from` the scope reads.
    fn ledger(self) -> &'static str {
        match self {
            Scope::UnionConfig => UNION_LEDGER,
            Scope::UnionPragmaEmptyG0 | Scope::UnionOptsEmptyG0 | Scope::FromEmptyG0G1 => {
                EMPTY_G0_LEDGER
            }
            Scope::UnionConfigEmptyG0 => EMPTY_G0_UNION_LEDGER,
            _ => LEDGER,
        }
    }

    /// Whether the SPARQL spelling goes through the connection (its `FROM`s
    /// name ledgers) rather than the ledger's view.
    fn sparql_through_connection(self) -> bool {
        matches!(
            self,
            Scope::FromG1G2Connection | Scope::TwoLedgers | Scope::TimeTravel
        )
    }
}

// ============================================================================
// Shapes (design §4) and their spellings
// ============================================================================

/// The five shapes the lanes answer (design §4), named by lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// C: `FILTER(EXISTS { ?x ex:label ?l } && ?v > 0)`, the EXISTS cache.
    ExistsInExpression,
    /// D: `COUNT(?s)` over `?s ex:kind ex:T1`, the bound-object count.
    BoundObjectCount,
    /// D′: `COUNT(?s)` over `?s ex:m 2`, a bound-object count whose constant
    /// matches facts of three numeric datatypes.
    BoundNumericCount,
    /// E: `COUNT(?s)` per predicate over `?s ?p ?o`.
    PerPredicateCount,
    /// B: the `ex:knows` triangle, the cyclic join.
    Triangle,
    /// A: `COUNT(*)` grouped by `?v` over `?s ex:n ?v`, the partitioned GROUP BY.
    GroupByObject,
}

pub const SHAPES: &[Shape] = &[
    Shape::ExistsInExpression,
    Shape::BoundObjectCount,
    Shape::BoundNumericCount,
    Shape::PerPredicateCount,
    Shape::Triangle,
    Shape::GroupByObject,
];

/// A shape's parts, so a scope can put its pattern inside `GRAPH`.
struct Parts {
    /// The projected variables, aggregates last.
    columns: &'static [&'static str],
    sparql_select: &'static str,
    sparql_body: &'static str,
    sparql_group_by: &'static str,
    jsonld_select: &'static [&'static str],
    jsonld_where: fn() -> JsonValue,
    jsonld_group_by: &'static [&'static str],
}

impl Shape {
    pub fn name(self) -> &'static str {
        match self {
            Shape::ExistsInExpression => "C exists in an expression",
            Shape::BoundObjectCount => "D bound-object count",
            Shape::BoundNumericCount => "D′ numeric bound-object count",
            Shape::PerPredicateCount => "E per-predicate count",
            Shape::Triangle => "B triangle",
            Shape::GroupByObject => "A GROUP BY object",
        }
    }

    fn parts(self) -> Parts {
        match self {
            Shape::ExistsInExpression => Parts {
                columns: &["x"],
                sparql_select: "?x",
                sparql_body: "?x ex:n ?v FILTER(EXISTS { ?x ex:label ?l } && ?v > 0)",
                sparql_group_by: "",
                jsonld_select: &["?x"],
                jsonld_where: || {
                    json!([
                        {"@id": "?x", "ex:n": "?v"},
                        ["filter", ["and",
                            ["exists", {"@id": "?x", "ex:label": "?l"}],
                            [">", "?v", 0]]]
                    ])
                },
                jsonld_group_by: &[],
            },
            Shape::BoundObjectCount => Parts {
                columns: &["c"],
                sparql_select: "(COUNT(?s) AS ?c)",
                sparql_body: "?s ex:kind ex:T1",
                sparql_group_by: "",
                jsonld_select: &["(as (count ?s) ?c)"],
                jsonld_where: || json!([{"@id": "?s", "ex:kind": {"@id": "ex:T1"}}]),
                jsonld_group_by: &[],
            },
            Shape::BoundNumericCount => Parts {
                columns: &["c"],
                sparql_select: "(COUNT(?s) AS ?c)",
                sparql_body: "?s ex:m 2",
                sparql_group_by: "",
                jsonld_select: &["(as (count ?s) ?c)"],
                jsonld_where: || json!([{"@id": "?s", "ex:m": 2}]),
                jsonld_group_by: &[],
            },
            Shape::PerPredicateCount => Parts {
                columns: &["p", "c"],
                sparql_select: "?p (COUNT(?s) AS ?c)",
                sparql_body: "?s ?p ?o",
                sparql_group_by: "?p",
                jsonld_select: &["?p", "(as (count ?s) ?c)"],
                jsonld_where: || json!([{"@id": "?s", "?p": "?o"}]),
                jsonld_group_by: &["?p"],
            },
            Shape::Triangle => Parts {
                columns: &["x", "y", "z"],
                sparql_select: "?x ?y ?z",
                sparql_body: "?x ex:knows ?y . ?y ex:knows ?z . ?z ex:knows ?x",
                sparql_group_by: "",
                jsonld_select: &["?x", "?y", "?z"],
                jsonld_where: || {
                    json!([
                        {"@id": "?x", "ex:knows": "?y"},
                        {"@id": "?y", "ex:knows": "?z"},
                        {"@id": "?z", "ex:knows": "?x"}
                    ])
                },
                jsonld_group_by: &[],
            },
            Shape::GroupByObject => Parts {
                columns: &["v", "k"],
                sparql_select: "?v (COUNT(*) AS ?k)",
                sparql_body: "?s ex:n ?v",
                sparql_group_by: "?v",
                jsonld_select: &["?v", "(as (count ?s) ?k)"],
                jsonld_where: || json!([{"@id": "?s", "ex:n": "?v"}]),
                jsonld_group_by: &["?v"],
            },
        }
    }

    /// The shape's SPARQL under `scope`.
    pub fn sparql(self, scope: Scope) -> String {
        let p = self.parts();
        let prefixes = format!("PREFIX ex: <{EX}>\n");
        let at_t1 = |g: &str| format!("<{LEDGER}@t:1#{g}>");
        let (prologue, dataset) = match scope {
            Scope::DefaultGraph
            | Scope::UnionConfig
            | Scope::UnionConfigEmptyG0
            | Scope::GraphG2
            | Scope::GraphVar => (String::new(), String::new()),
            Scope::FromG1 => (String::new(), format!("FROM <{G1}>")),
            Scope::FromG1G2 => (String::new(), format!("FROM <{G1}> FROM <{G2}>")),
            Scope::FromG1G2Connection => (
                String::new(),
                format!("FROM <{LEDGER}#{G1}> FROM <{LEDGER}#{G2}>"),
            ),
            Scope::FromG0G1G2 => (
                String::new(),
                format!("FROM <{LEDGER}> FROM <{G1}> FROM <{G2}>"),
            ),
            Scope::UnionPragma | Scope::UnionPragmaEmptyG0 => (
                "# PRAGMA union-default-graph: true\n".to_string(),
                String::new(),
            ),
            Scope::UnionOpts | Scope::UnionOptsEmptyG0 => {
                panic!("opts.unionDefaultGraph has no SPARQL spelling")
            }
            Scope::FromNamedOnly => (String::new(), format!("FROM NAMED <{G2}>")),
            Scope::TwoLedgers => (
                String::new(),
                format!("FROM <{LEDGER}> FROM <{OTHER_LEDGER}>"),
            ),
            Scope::TimeTravel => (
                String::new(),
                format!("FROM {} FROM {}", at_t1(G1), at_t1(G2)),
            ),
            Scope::FromEmptyG0G1 => (
                String::new(),
                format!("FROM <{EMPTY_G0_LEDGER}> FROM <{G1}>"),
            ),
        };
        let (select, body, group_by) = match scope {
            Scope::GraphG2 => (
                p.sparql_select.to_string(),
                format!("GRAPH <{G2}> {{ {} }}", p.sparql_body),
                p.sparql_group_by.to_string(),
            ),
            // An aggregate over GRAPH ?g groups per graph.
            Scope::GraphVar => (
                format!("?g {}", p.sparql_select),
                format!("GRAPH ?g {{ {} }}", p.sparql_body),
                if p.sparql_select.contains("COUNT") {
                    format!("?g {}", p.sparql_group_by)
                } else {
                    p.sparql_group_by.to_string()
                },
            ),
            _ => (
                p.sparql_select.to_string(),
                p.sparql_body.to_string(),
                p.sparql_group_by.to_string(),
            ),
        };
        let group_by = group_by.trim();
        let group_by = if group_by.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {group_by}")
        };
        format!("{prologue}{prefixes}SELECT {select} {dataset} WHERE {{ {body} }}{group_by}")
    }

    /// The shape's JSON-LD under `scope`, for the connection.
    pub fn jsonld(self, scope: Scope) -> JsonValue {
        let p = self.parts();
        let g = |iri: &str| format!("{LEDGER}#{iri}");
        let mut select: Vec<String> = p.jsonld_select.iter().map(|s| (*s).to_string()).collect();
        let mut group_by: Vec<String> =
            p.jsonld_group_by.iter().map(|s| (*s).to_string()).collect();
        let mut where_ = (p.jsonld_where)();
        let mut q = json!({"@context": {"ex": EX}});
        match scope {
            Scope::DefaultGraph | Scope::GraphG2 | Scope::GraphVar => q["from"] = json!(LEDGER),
            Scope::FromG1 => q["from"] = json!(g(G1)),
            Scope::FromG1G2 | Scope::FromG1G2Connection => q["from"] = json!([g(G1), g(G2)]),
            Scope::FromG0G1G2 => q["from"] = json!([LEDGER, g(G1), g(G2)]),
            Scope::UnionPragma | Scope::UnionPragmaEmptyG0 => {
                panic!("the PRAGMA has no JSON-LD spelling")
            }
            Scope::UnionOpts => {
                q["from"] = json!(LEDGER);
                q["opts"] = json!({"unionDefaultGraph": true});
            }
            Scope::UnionOptsEmptyG0 => {
                q["from"] = json!(EMPTY_G0_LEDGER);
                q["opts"] = json!({"unionDefaultGraph": true});
            }
            Scope::UnionConfig => q["from"] = json!(UNION_LEDGER),
            Scope::UnionConfigEmptyG0 => q["from"] = json!(EMPTY_G0_UNION_LEDGER),
            Scope::FromEmptyG0G1 => {
                q["from"] = json!([EMPTY_G0_LEDGER, format!("{EMPTY_G0_LEDGER}#{G1}")]);
            }
            Scope::FromNamedOnly => q["fromNamed"] = json!([g(G2)]),
            Scope::TwoLedgers => q["from"] = json!([LEDGER, OTHER_LEDGER]),
            Scope::TimeTravel => {
                q["from"] = json!([format!("{LEDGER}@t:1#{G1}"), format!("{LEDGER}@t:1#{G2}")]);
            }
        }
        // `["graph", name, pattern, pattern, …]`: the patterns follow the name.
        let in_graph = |name: &str, patterns: JsonValue| {
            let mut clause = vec![json!("graph"), json!(name)];
            clause.extend(patterns.as_array().cloned().unwrap_or_default());
            json!([clause])
        };
        match scope {
            Scope::GraphG2 => where_ = in_graph(G2, where_),
            Scope::GraphVar => {
                where_ = in_graph("?g", where_);
                select.insert(0, "?g".to_string());
                if select.iter().any(|s| s.contains("count")) {
                    group_by.insert(0, "?g".to_string());
                }
            }
            _ => {}
        }
        q["select"] = json!(select);
        q["where"] = where_;
        if !group_by.is_empty() {
            q["groupBy"] = json!(group_by);
        }
        q
    }

    /// The projected columns under `scope` (`GRAPH ?g` leads with `g`).
    pub fn columns(self, scope: Scope) -> Vec<&'static str> {
        let mut cols = self.parts().columns.to_vec();
        if scope == Scope::GraphVar {
            cols.insert(0, "g");
        }
        cols
    }
}

// ============================================================================
// The hand-written answers
// ============================================================================

/// Rows, each a list of short values (an `ex:` IRI without its prefix),
/// sorted.
pub type Rows = Vec<Vec<String>>;

fn rows(rows: &[&[&str]]) -> Rows {
    let mut out: Rows = rows
        .iter()
        .map(|r| r.iter().map(|s| (*s).to_string()).collect())
        .collect();
    out.sort();
    out
}

/// The three rotations of the triangle `t{n}a → t{n}b → t{n}c → t{n}a`.
fn triangle(n: u8) -> [[String; 3]; 3] {
    let (a, b, c) = (format!("t{n}a"), format!("t{n}b"), format!("t{n}c"));
    [
        [a.clone(), b.clone(), c.clone()],
        [b.clone(), c.clone(), a.clone()],
        [c, a, b],
    ]
}

fn triangles(ns: &[u8]) -> Rows {
    let mut out: Rows = ns
        .iter()
        .flat_map(|n| triangle(*n))
        .map(|r| r.to_vec())
        .collect();
    out.sort();
    out
}

/// The answer to `shape` over `graphs` of the fixture in `state`, written by
/// hand from [`FIXTURE_A`], [`FIXTURE_OTHER`] and [`WRITE_TO_G2`].
///
/// A default graph of two or more graphs is the union of their facts (AJ-66):
/// a fact two members both hold is one solution. A fact is identified with its
/// list position, so a list's repeated value stays two rows in a union, as in
/// one graph.
pub fn expected(shape: Shape, graphs: Graphs, state: State) -> Rows {
    // With the write at t 2 (Novelty, Indexed2), g2 holds k4, `s3 n 4` and all
    // of t2.
    let w = state != State::Indexed;
    match graphs {
        // The empty default graph holds no fact, so it adds no solution.
        Graphs::G1G2WithEmptyG0 => expected(shape, Graphs::G1G2, state),
        Graphs::G1WithEmptyG0 => expected(shape, Graphs::G1, state),
        Graphs::PerNamedGraph => {
            let mut out = Rows::new();
            for (iri, g) in [(G1, Graphs::G1), (G2, Graphs::G2)] {
                for mut row in expected(shape, g, state) {
                    row.insert(0, iri.to_string());
                    out.push(row);
                }
            }
            out.sort();
            out
        }
        _ => match shape {
            // ?x with an `n` above 0 and a label, one row per such `n`.
            Shape::ExistsInExpression => match graphs {
                Graphs::G0 => rows(&[&["a1"]]),
                Graphs::G1 => rows(&[&["s2"]]),
                // s3's label is in g2; its `n 4` arrives with the write.
                Graphs::G2 if w => rows(&[&["s3"]]),
                Graphs::G2 => rows(&[]),
                // s2 (`n 2` in both, one solution), s3 (`n 3` from g1, label
                // from g2), and with the write s3's `n 4` too.
                Graphs::G1G2 if w => rows(&[&["s2"], &["s3"], &["s3"]]),
                Graphs::G1G2 => rows(&[&["s2"], &["s3"]]),
                Graphs::G0G1G2 if w => rows(&[&["a1"], &["s2"], &["s3"], &["s3"]]),
                Graphs::G0G1G2 => rows(&[&["a1"], &["s2"], &["s3"]]),
                Graphs::G0Other => rows(&[&["a1"], &["x1"]]),
                Graphs::Empty => rows(&[]),
                Graphs::PerNamedGraph | Graphs::G1G2WithEmptyG0 | Graphs::G1WithEmptyG0 => {
                    unreachable!()
                }
            },
            // Distinct subjects with `kind T1`.
            Shape::BoundObjectCount => {
                let n = match graphs {
                    Graphs::G0 => 1,        // k0
                    Graphs::G1 => 2,        // k1 k2
                    Graphs::G2 if w => 3,   // k1 k3 k4
                    Graphs::G2 => 2,        // k1 k3
                    Graphs::G1G2 if w => 4, // k1 k2 k3 k4
                    Graphs::G1G2 => 3,      // k1 (both) k2 k3
                    Graphs::G0G1G2 if w => 5,
                    Graphs::G0G1G2 => 4,
                    Graphs::G0Other => 2, // k0, x1
                    Graphs::Empty => 0,
                    Graphs::PerNamedGraph | Graphs::G1G2WithEmptyG0 | Graphs::G1WithEmptyG0 => {
                        unreachable!()
                    }
                };
                rows(&[&[&n.to_string()]])
            }
            // Facts `?s ex:m v` with v numerically 2, any datatype. Over a
            // union a fact two members both hold counts once (AJ-66); q1's
            // integer and double are two facts.
            Shape::BoundNumericCount => {
                let n = match graphs {
                    Graphs::G0 | Graphs::G0Other | Graphs::Empty => 0,
                    Graphs::G1 => 2,                    // q1 int, q2 int
                    Graphs::G2 => 4,                    // q1 dbl, q2 int, q3 dbl, q4 short
                    Graphs::G1G2 | Graphs::G0G1G2 => 5, // q1 int, q1 dbl, q2 int (both), q3 dbl, q4 short
                    Graphs::PerNamedGraph | Graphs::G1G2WithEmptyG0 | Graphs::G1WithEmptyG0 => {
                        unreachable!()
                    }
                };
                rows(&[&[&n.to_string()]])
            }
            // Triples per predicate.
            Shape::PerPredicateCount => {
                let counts: &[(&str, u32)] = match graphs {
                    Graphs::G0 => &[("n", 2), ("label", 2), ("kind", 1), ("big", 1)],
                    Graphs::G1 => &[
                        ("n", 3),
                        ("label", 1),
                        ("kind", 2),
                        ("big", 1),
                        ("knows", 8),
                        ("m", 2),
                    ],
                    // A bag: L's two list items are two rows.
                    Graphs::G2 if w => &[
                        ("n", 3),
                        ("label", 1),
                        ("kind", 3),
                        ("only", 1),
                        ("big", 1),
                        ("items", 2),
                        ("knows", 6),
                        ("m", 4),
                    ],
                    Graphs::G2 => &[
                        ("n", 2),
                        ("label", 1),
                        ("kind", 2),
                        ("only", 1),
                        ("big", 1),
                        ("items", 2),
                        ("knows", 4),
                        ("m", 4),
                    ],
                    // `s2 n 2`, `k1 kind`, `b big 2.5`, t3 and `q2 m 2` once
                    // each (both members hold them); L's two list items stay
                    // two facts, and so do q1's `m 2` and `m 2.0e0` (AJ-66);
                    // the write's t2 edges are g1's.
                    Graphs::G1G2 if w => &[
                        ("n", 5),
                        ("label", 2),
                        ("kind", 4),
                        ("big", 1),
                        ("only", 1),
                        ("items", 2),
                        ("knows", 9),
                        ("m", 5),
                    ],
                    Graphs::G1G2 => &[
                        ("n", 4),
                        ("label", 2),
                        ("kind", 3),
                        ("big", 1),
                        ("only", 1),
                        ("items", 2),
                        ("knows", 9),
                        ("m", 5),
                    ],
                    // g0 adds `n` ×2, `label` ×2, `kind` ×1, and `b big 1.5`,
                    // a value g1 and g2 do not hold (all three are NumBig
                    // handle 0 of their own graph's arena).
                    Graphs::G0G1G2 if w => &[
                        ("n", 7),
                        ("label", 4),
                        ("kind", 5),
                        ("big", 2),
                        ("only", 1),
                        ("items", 2),
                        ("knows", 9),
                        ("m", 5),
                    ],
                    Graphs::G0G1G2 => &[
                        ("n", 6),
                        ("label", 4),
                        ("kind", 4),
                        ("big", 2),
                        ("only", 1),
                        ("items", 2),
                        ("knows", 9),
                        ("m", 5),
                    ],
                    Graphs::G0Other => &[("n", 3), ("label", 3), ("kind", 2), ("big", 1)],
                    Graphs::Empty => &[],
                    Graphs::PerNamedGraph | Graphs::G1G2WithEmptyG0 | Graphs::G1WithEmptyG0 => {
                        unreachable!()
                    }
                };
                let mut out: Rows = counts
                    .iter()
                    .map(|(p, n)| vec![(*p).to_string(), n.to_string()])
                    .collect();
                out.sort();
                out
            }
            // Each triangle's three rotations, once each.
            Shape::Triangle => match graphs {
                Graphs::G0 | Graphs::G0Other | Graphs::Empty => Rows::new(),
                Graphs::G1 => triangles(&[1, 3]),
                Graphs::G2 if w => triangles(&[2, 3]),
                Graphs::G2 => triangles(&[3]),
                Graphs::G1G2 | Graphs::G0G1G2 => triangles(&[1, 2, 3]),
                Graphs::PerNamedGraph | Graphs::G1G2WithEmptyG0 | Graphs::G1WithEmptyG0 => {
                    unreachable!()
                }
            },
            // (v, solutions of `?s ex:n v`).
            Shape::GroupByObject => match graphs {
                Graphs::G0 => rows(&[&["0", "1"], &["1", "1"]]),
                Graphs::G1 => rows(&[&["1", "1"], &["2", "1"], &["3", "1"]]),
                Graphs::G2 if w => rows(&[&["2", "2"], &["4", "1"]]),
                Graphs::G2 => rows(&[&["2", "2"]]),
                Graphs::G1G2 if w => rows(&[&["1", "1"], &["2", "2"], &["3", "1"], &["4", "1"]]),
                Graphs::G1G2 => rows(&[&["1", "1"], &["2", "2"], &["3", "1"]]),
                Graphs::G0G1G2 if w => rows(&[
                    &["0", "1"],
                    &["1", "2"],
                    &["2", "2"],
                    &["3", "1"],
                    &["4", "1"],
                ]),
                Graphs::G0G1G2 => rows(&[&["0", "1"], &["1", "2"], &["2", "2"], &["3", "1"]]),
                Graphs::G0Other => rows(&[&["0", "1"], &["1", "1"], &["5", "1"]]),
                Graphs::Empty => Rows::new(),
                Graphs::PerNamedGraph | Graphs::G1G2WithEmptyG0 | Graphs::G1WithEmptyG0 => {
                    unreachable!()
                }
            },
        },
    }
}

// ============================================================================
// Running a shape under a scope
// ============================================================================

/// Strip `ex:` (either spelling) from a value.
fn short(value: &str) -> String {
    value
        .strip_prefix(EX)
        .or_else(|| value.strip_prefix("ex:"))
        .unwrap_or(value)
        .to_string()
}

fn sparql_rows(result: &JsonValue, vars: &[&str]) -> Rows {
    let bindings = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("SPARQL results: {result}"));
    let mut out: Rows = bindings
        .iter()
        .map(|b| {
            vars.iter()
                .map(|v| match b.get(*v) {
                    Some(term) => short(term["value"].as_str().unwrap_or_default()),
                    None => "-".to_string(),
                })
                .collect()
        })
        .collect();
    out.sort();
    out
}

fn json_value(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "-".to_string(),
        JsonValue::String(s) => short(s),
        JsonValue::Object(o) => match o.get("@id").or_else(|| o.get("@value")) {
            Some(inner) => json_value(inner),
            None => v.to_string(),
        },
        other => other.to_string(),
    }
}

fn jsonld_rows(result: &JsonValue) -> Rows {
    let mut out: Rows = result
        .as_array()
        .unwrap_or_else(|| panic!("JSON-LD rows: {result}"))
        .iter()
        .map(|row| match row {
            JsonValue::Array(cols) => cols.iter().map(json_value).collect(),
            single => vec![json_value(single)],
        })
        .collect();
    out.sort();
    out
}

/// Run `shape` under `scope` on `surface`, without asserting anything.
pub async fn answer(
    fx: &Fixture,
    scope: Scope,
    shape: Shape,
    surface: Surface,
) -> Result<Rows, String> {
    match surface {
        Surface::Sparql if !scope.sparql_through_connection() => {
            let query = shape.sparql(scope);
            let json = fx
                .view(scope.ledger())
                .await
                .query(&fx.fluree)
                .sparql(&query)
                .execute_formatted()
                .await
                .map_err(|e| format!("{query}: {e}"))?;
            Ok(sparql_rows(&json, &shape.columns(scope)))
        }
        Surface::Sparql => {
            let query = shape.sparql(scope);
            let result = fx
                .fluree
                .query_connection_sparql(&query)
                .await
                .map_err(|e| format!("{query}: {e}"))?;
            format_through(fx, scope, result).await
        }
        Surface::JsonLd => {
            let query = shape.jsonld(scope);
            let result = fx
                .fluree
                .query_connection(&query)
                .await
                .map_err(|e| format!("{query}: {e}"))?;
            format_through(fx, scope, result).await
        }
    }
}

async fn format_through(
    fx: &Fixture,
    scope: Scope,
    result: fluree_db_api::QueryResult,
) -> Result<Rows, String> {
    let ledger = fx
        .fluree
        .ledger(scope.ledger())
        .await
        .map_err(|e| e.to_string())?;
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .map_err(|e| e.to_string())?;
    Ok(jsonld_rows(&json))
}

/// What one run of a shape under a scope did.
#[derive(Debug)]
pub struct Run {
    pub label: String,
    /// The answer with fast paths on, and with them off.
    pub fast: Result<Rows, String>,
    pub generic: Result<Rows, String>,
    /// The `index scope` stamps of the fast run, deduplicated in order.
    pub scope_stamps: Vec<String>,
    /// The fast-path sites that proceeded in the fast run.
    pub proceeded: Vec<String>,
    /// The fast-path sites that stamped a fallback, with its outcome.
    pub fell_back: Vec<(String, String)>,
}

/// The lock every on/off pair in this binary holds while it flips the
/// process-wide fast-path switch.
pub fn switch() -> &'static tokio::sync::Mutex<()> {
    static SWITCH: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    SWITCH.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Run `shape` under `scope` on `surface` twice, with fast paths on (under
/// span capture) and with `set_fast_paths_disabled(true)`, holding the switch
/// lock across both.
pub async fn run(fx: &Fixture, scope: Scope, shape: Shape, surface: Surface) -> Run {
    let _switch = switch().lock().await;
    let label = format!(
        "{} / {} / {surface:?} / {:?}",
        shape.name(),
        scope.name(),
        fx.state
    );
    set_fast_paths_disabled(false);
    let (store, guard) = init_test_tracing();
    let fast = answer(fx, scope, shape, surface).await;
    drop(guard);
    set_fast_paths_disabled(true);
    let generic = answer(fx, scope, shape, surface).await;
    set_fast_paths_disabled(false);
    Run {
        label,
        fast,
        generic,
        scope_stamps: query_scope_stamps(&store),
        proceeded: outcomes(&store)
            .into_iter()
            .filter(|(_, o)| o == "proceed")
            .map(|(s, _)| s)
            .collect(),
        fell_back: outcomes(&store)
            .into_iter()
            .filter(|(_, o)| o != "proceed")
            .collect(),
    }
}

pub fn outcomes(store: &SpanStore) -> Vec<(String, String)> {
    store
        .all_events()
        .iter()
        .filter(|e| e.message() == "fast-path outcome")
        .filter_map(|e| {
            Some((
                e.fields.get("site")?.clone(),
                e.fields.get("outcome")?.clone(),
            ))
        })
        .collect()
}

/// The query's own `index scope` stamps: from its last `query` stamp (a
/// ledger's config is read through queries of its own first), deduplicated.
fn query_scope_stamps(store: &SpanStore) -> Vec<String> {
    let stamps: Vec<String> = store
        .all_events()
        .iter()
        .filter(|e| e.message() == "index scope")
        .map(|e| {
            let f = |n: &str| e.fields.get(n).cloned().unwrap_or_default();
            let current = if f("current") == "true" {
                "current"
            } else {
                "stale"
            };
            format!("{}={}/{}/{current}", f("site"), f("scope"), f("members"))
        })
        .collect();
    let start = stamps
        .iter()
        .rposition(|s| s.starts_with("query="))
        .unwrap_or(0);
    let mut seen: Vec<String> = Vec::new();
    for s in &stamps[start..] {
        if !seen.contains(s) {
            seen.push(s.clone());
        }
    }
    seen
}

/// Run `shape` under `scope` on `surface` and check it: both answers equal
/// [`expected`], and the `index scope` stamps equal [`Scope::stamps`]. Every
/// mismatch is pushed to `failures`; the [`Run`] is returned for routing
/// checks.
pub async fn check(
    fx: &Fixture,
    scope: Scope,
    shape: Shape,
    surface: Surface,
    failures: &mut Vec<String>,
) -> Run {
    let run = run(fx, scope, shape, surface).await;
    let want = expected(shape, scope.graphs(), scope.answer_state(fx.state));
    for (lane, got) in [
        ("fast paths on", &run.fast),
        ("fast paths off", &run.generic),
    ] {
        match got {
            Ok(rows) if *rows == want => {}
            Ok(rows) => failures.push(format!("{}: {lane}: {rows:?}, want {want:?}", run.label)),
            Err(e) => failures.push(format!("{}: {lane}: failed: {e}", run.label)),
        }
    }
    let stamps = scope.stamps(fx.state);
    if run.scope_stamps != stamps {
        failures.push(format!(
            "{}: index scope {:?}, want {stamps:?}",
            run.label, run.scope_stamps
        ));
    }
    run
}

/// Check that every site in `must_fire` proceeded in `run` and no site in
/// `must_not_fire` did: a union lane's site with the one-graph site it
/// replaces, or the reverse.
pub fn assert_routing(
    run: &Run,
    must_fire: &[&str],
    must_not_fire: &[&str],
    failures: &mut Vec<String>,
) {
    for site in must_fire {
        if !run.proceeded.iter().any(|s| s == site) {
            failures.push(format!(
                "{}: `{site}` did not proceed (proceeded: {:?}; fell back: {:?})",
                run.label, run.proceeded, run.fell_back
            ));
        }
    }
    for site in must_not_fire {
        if run.proceeded.iter().any(|s| s == site) {
            failures.push(format!("{}: `{site}` proceeded", run.label));
        }
    }
}
