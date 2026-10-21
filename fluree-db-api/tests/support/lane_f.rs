//! Fixture F, for the nested-loop join's batched probes over a union of
//! graphs (lane F): joins whose right side the subject, object and
//! existence probes answer, and the keep/drop membership join, over facts
//! two graphs both hold and facts only one does.
//!
//! The facts are written as data ([`facts`]), so the answer to each shape is
//! computed from them by a plain evaluator ([`expected`]) that applies the
//! union's semantics directly: the union is the set of facts, one fact per
//! subject, predicate, term and list position. That evaluator is the oracle
//! the lane and the general plan both answer to, beside the one-graph floor.
//!
//! The cases a union probe can get wrong are in the data:
//! - a fact both graphs hold (one solution) beside facts one graph holds;
//! - list positions: `y3 v ("a" "a")` in g1 and `("a")` in g2 are two facts,
//!   `y4 v ("a")` in g1 and `("b" "a")` in g2 are three;
//! - a bare constant matching two datatypes: `y5 score 1` in g1 and
//!   `1.0e0` in g2 are two facts, `y6 score 1` in both is one;
//! - NumBig values (every `xsd:decimal`) under one handle number in two
//!   graphs: handles are numbered per `(graph, predicate)`, so g1's `1.5`
//!   and g2's `2.5` on `y0 v` are both the first handle of their graph, and
//!   g1's and g2's `3.5` on `y1 v` need not be the same handle;
//! - a ref list: `x900 r (y9 y9)` in g1 and `(y9)` in g2 are two facts.

use crate::multigraph::{outcomes, switch};
use crate::span_capture::init_test_tracing;
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, GraphDb, ReindexOptions};
use std::collections::BTreeMap;
use std::fmt::Write as _;

pub const EX: &str = "http://example.org/";
pub const G1: &str = "urn:g1";
pub const G2: &str = "urn:g2";
/// Fixture F's g1 and g2.
pub const LEDGER: &str = "it/lane-f:main";
/// Fixture F with a list-position duplicate on the membership join's
/// predicate, which sends that join to its exact per-row path.
pub const LEDGER_LISTS: &str = "it/lane-f-lists:main";
/// Each ledger's union, once, in a default graph.
pub const FLOOR: &str = "it/lane-f-floor:main";
pub const FLOOR_LISTS: &str = "it/lane-f-floor-lists:main";

/// Driving rows for the membership join: at least the planner's minimum
/// (256), so it picks keep/drop over the nested loop.
const N: usize = 300;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Obj {
    Iri(String),
    Str(String),
    Int(i64),
    /// A double, written as its lexical form.
    Double(&'static str),
    /// An `xsd:decimal` (a NumBig), written as its lexical form.
    Dec(&'static str),
}

impl Obj {
    fn turtle(&self) -> String {
        match self {
            Obj::Iri(n) => format!("ex:{n}"),
            Obj::Str(v) => format!("\"{v}\""),
            Obj::Int(v) => v.to_string(),
            Obj::Double(v) => format!("\"{v}\"^^xsd:double"),
            Obj::Dec(v) => (*v).to_string(),
        }
    }

    /// The term as a SPARQL JSON result renders it: `value^^type`.
    fn rendered(&self) -> String {
        match self {
            Obj::Iri(n) => n.clone(),
            // A plain string renders with no datatype.
            Obj::Str(v) => v.clone(),
            Obj::Int(v) => format!("{v}^^integer"),
            Obj::Double(v) => format!("{}^^double", v.replace('e', "E")),
            Obj::Dec(v) => format!("{v}^^decimal"),
        }
    }
}

/// One fact: graph, subject, predicate, object, list position.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fact {
    pub g: &'static str,
    pub s: String,
    pub p: &'static str,
    pub o: Obj,
    pub i: Option<u32>,
}

pub fn fact(g: &'static str, s: impl Into<String>, p: &'static str, o: Obj) -> Fact {
    Fact {
        g,
        s: s.into(),
        p,
        o,
        i: None,
    }
}

fn list(g: &'static str, s: &str, p: &'static str, items: &[Obj]) -> Vec<Fact> {
    items
        .iter()
        .enumerate()
        .map(|(i, o)| Fact {
            g,
            s: s.to_string(),
            p,
            o: o.clone(),
            i: Some(i as u32),
        })
        .collect()
}

fn iri(n: impl Into<String>) -> Obj {
    Obj::Iri(n.into())
}

/// Fixture F's facts; with `lists`, `y20 q` is the list `(x20 x20)` instead
/// of the single fact `y20 q x20`.
pub fn facts(lists: bool) -> Vec<Fact> {
    let mut f = Vec::new();
    for i in 0..N {
        f.push(fact(G1, format!("x{i}"), "p", iri(format!("y{i}"))));
        if !(lists && i == 20) {
            f.push(fact(G1, format!("y{i}"), "q", iri(format!("x{i}"))));
        }
    }
    if lists {
        f.extend(list(G1, "y20", "q", &[iri("x20"), iri("x20")]));
    }
    for i in N / 2..N {
        f.push(fact(G2, format!("x{i}"), "p", iri(format!("y{i}"))));
        f.push(fact(G2, format!("y{i}"), "q", iri(format!("x{i}"))));
    }
    // Subject probe objects.
    f.push(fact(G1, "y0", "v", Obj::Dec("1.5")));
    f.push(fact(G1, "y1", "v", Obj::Dec("3.5")));
    f.push(fact(G2, "y0", "v", Obj::Dec("2.5")));
    f.push(fact(G2, "y10", "v", Obj::Dec("7.5")));
    f.push(fact(G2, "y1", "v", Obj::Dec("3.5")));
    for g in [G1, G2] {
        f.push(fact(g, "y2", "v", Obj::Str("s".into())));
    }
    f.extend(list(
        G1,
        "y3",
        "v",
        &[Obj::Str("a".into()), Obj::Str("a".into())],
    ));
    f.extend(list(G2, "y3", "v", &[Obj::Str("a".into())]));
    f.extend(list(G1, "y4", "v", &[Obj::Str("a".into())]));
    f.extend(list(
        G2,
        "y4",
        "v",
        &[Obj::Str("b".into()), Obj::Str("a".into())],
    ));
    // The probed predicates are the large side of each join, so the planner
    // drives from the selective `sel` / `tag` patterns and probes them.
    for i in 100..N {
        f.push(fact(G1, format!("y{i}"), "v", Obj::Str("pad".into())));
        f.push(fact(G1, format!("y{i}"), "score", Obj::Int(2)));
        f.push(fact(G1, format!("x{i}"), "r", iri(format!("z{i}"))));
    }
    for y in ["y0", "y1", "y2", "y3", "y4", "y5", "y6", "y10"] {
        for g in [G1, G2] {
            f.push(fact(g, format!("s_{y}"), "sel", iri(y)));
        }
    }
    // Existence probe: `?y ex:score 1`.
    f.push(fact(G1, "y5", "score", Obj::Int(1)));
    f.push(fact(G2, "y5", "score", Obj::Double("1.0e0")));
    for g in [G1, G2] {
        f.push(fact(g, "y6", "score", Obj::Int(1)));
    }
    // Object probe: `?y ex:tag ex:T . ?x ex:r ?y`.
    for g in [G1, G2] {
        f.push(fact(g, "y7", "tag", iri("T")));
        f.push(fact(g, "x7b", "r", iri("y7")));
        f.push(fact(g, "y9", "tag", iri("T")));
    }
    f.push(fact(G2, "x7c", "r", iri("y7")));
    f.extend(list(G1, "x900", "r", &[iri("y9"), iri("y9")]));
    f.extend(list(G2, "x900", "r", &[iri("y9")]));
    f
}

/// The union's facts: one per subject, predicate, term and list position.
pub fn union_of(facts: &[Fact]) -> Vec<Fact> {
    let mut out: Vec<Fact> = facts.iter().map(|f| Fact { g: "", ..f.clone() }).collect();
    out.sort();
    out.dedup();
    out
}

/// TriG for `facts`, each fact in its own graph (`""`: the default graph).
pub fn trig(facts: &[Fact]) -> String {
    let mut by_graph: BTreeMap<&str, Vec<&Fact>> = BTreeMap::new();
    for f in facts {
        by_graph.entry(f.g).or_default().push(f);
    }
    let mut out = String::from(
        "@prefix ex: <http://example.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n",
    );
    for (g, facts) in by_graph {
        if !g.is_empty() {
            let _ = writeln!(out, "GRAPH <{g}> {{");
        }
        // Lists by (subject, predicate), in position order; the rest as triples.
        let mut lists: BTreeMap<(&str, &str), Vec<&Fact>> = BTreeMap::new();
        for f in facts {
            match f.i {
                Some(_) => lists.entry((f.s.as_str(), f.p)).or_default().push(f),
                None => {
                    let _ = writeln!(out, "  ex:{} ex:{} {} .", f.s, f.p, f.o.turtle());
                }
            }
        }
        for ((s, p), mut items) in lists {
            items.sort_by_key(|f| f.i);
            let items: Vec<String> = items.iter().map(|f| f.o.turtle()).collect();
            let _ = writeln!(out, "  ex:{s} ex:{p} ( {} ) .", items.join(" "));
        }
        if !g.is_empty() {
            out.push_str("}\n");
        }
    }
    out
}

/// A join the batched probes answer, over fixture F.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// The subject probe: `?x ex:sel ?y . ?y ex:v ?o`.
    Subject,
    /// The object probe: `?y ex:tag ex:T . ?x ex:r ?y`.
    Object,
    /// The existence probe: `?x ex:sel ?y . ?y ex:score 1`.
    Exists,
    /// The keep/drop membership join: `?x ex:p ?y . ?y ex:q ?x`.
    Membership,
}

pub const SHAPES: &[Shape] = &[
    Shape::Subject,
    Shape::Object,
    Shape::Exists,
    Shape::Membership,
];

impl Shape {
    pub fn body(self) -> &'static str {
        match self {
            Shape::Subject => "?x ex:sel ?y . ?y ex:v ?o",
            Shape::Object => "?y ex:tag ex:T . ?x ex:r ?y",
            Shape::Exists => "?x ex:sel ?y . ?y ex:score 1",
            Shape::Membership => "?x ex:p ?y . ?y ex:q ?x",
        }
    }

    pub fn vars(self) -> &'static [&'static str] {
        match self {
            Shape::Subject => &["y", "o"],
            Shape::Exists => &["y"],
            Shape::Object | Shape::Membership => &["x", "y"],
        }
    }

    /// The routing-stamp site that answers the shape's join.
    pub fn site(self) -> &'static str {
        match self {
            Shape::Membership => "membership-join",
            _ => "nested-loop-batched-union",
        }
    }
}

fn numeric_one(o: &Obj) -> bool {
    match o {
        Obj::Int(v) => *v == 1,
        Obj::Double(v) => v.parse::<f64>().is_ok_and(|d| d == 1.0),
        Obj::Dec(v) => v.parse::<f64>().is_ok_and(|d| d == 1.0),
        _ => false,
    }
}

/// The answer to `shape` over the union of `facts`, each row rendered as
/// its variables' values joined by a space, sorted.
pub fn expected(shape: Shape, facts: &[Fact]) -> Vec<String> {
    let u = union_of(facts);
    let with = |p: &'static str| u.iter().filter(move |f| f.p == p);
    let mut rows: Vec<String> = Vec::new();
    match shape {
        Shape::Subject => {
            for a in with("sel") {
                let Obj::Iri(y) = &a.o else { continue };
                for b in with("v").filter(|b| &b.s == y) {
                    rows.push(format!("{y} {}", b.o.rendered()));
                }
            }
        }
        Shape::Object => {
            for t in with("tag").filter(|t| t.o == iri("T")) {
                for a in with("r").filter(|a| a.o == iri(t.s.clone())) {
                    rows.push(format!("{} {}", a.s, t.s));
                }
            }
        }
        Shape::Exists => {
            for a in with("sel") {
                let Obj::Iri(y) = &a.o else { continue };
                for _ in with("score").filter(|b| &b.s == y && numeric_one(&b.o)) {
                    rows.push(y.clone());
                }
            }
        }
        Shape::Membership => {
            for a in with("p") {
                let Obj::Iri(y) = &a.o else { continue };
                for _ in with("q").filter(|b| &b.s == y && b.o == iri(a.s.clone())) {
                    rows.push(format!("{} {y}", a.s));
                }
            }
        }
    }
    rows.sort();
    rows
}

/// Fixture F's ledgers, indexed, on one in-memory connection with background
/// indexing off.
pub struct Fixture {
    pub fluree: Fluree,
}

impl Fixture {
    pub async fn build() -> Self {
        let fluree = FlureeBuilder::memory().without_indexing().build_memory();
        for (ledger, trig) in [
            (LEDGER, trig(&facts(false))),
            (LEDGER_LISTS, trig(&facts(true))),
            (FLOOR, trig(&union_of(&facts(false)))),
            (FLOOR_LISTS, trig(&union_of(&facts(true)))),
        ] {
            fluree.create_ledger(ledger).await.expect("create");
            let handle = fluree.ledger_cached(ledger).await.expect("cache");
            fluree
                .stage(&handle)
                .insert_turtle(&trig)
                .execute()
                .await
                .expect("seed");
            fluree
                .reindex(ledger, ReindexOptions::default())
                .await
                .expect("index");
            fluree.disconnect_ledger(ledger).await;
        }
        Self { fluree }
    }

    /// Commit `facts` to `ledger` without indexing them: novelty in each
    /// fact's graph.
    pub async fn write(&self, ledger: &str, facts: &[Fact]) {
        let handle = self.fluree.ledger_cached(ledger).await.expect("ledger");
        self.fluree
            .stage(&handle)
            .insert_turtle(&trig(facts))
            .execute()
            .await
            .expect("write");
    }

    /// `shape` over `ledger` with `dataset` (`FROM …`, or empty) and an
    /// optional prologue, rows rendered as [`expected`] renders them.
    pub async fn sparql(
        &self,
        ledger: &str,
        prologue: &str,
        dataset: &str,
        shape: Shape,
    ) -> Result<Vec<String>, String> {
        let query = format!(
            "{prologue}PREFIX ex: <{EX}>\nSELECT ?{} {dataset} WHERE {{ {} }}",
            shape.vars().join(" ?"),
            shape.body()
        );
        let handle = self.fluree.ledger_cached(ledger).await.expect("ledger");
        let view = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
        let json = view
            .query(&self.fluree)
            .sparql(&query)
            .execute_formatted()
            .await
            .map_err(|e| format!("{query}: {e}"))?;
        let mut rows: Vec<String> = json["results"]["bindings"]
            .as_array()
            .ok_or_else(|| format!("{query}: {json}"))?
            .iter()
            .map(|b| {
                shape
                    .vars()
                    .iter()
                    .map(|v| {
                        let term = &b[*v];
                        let value = term["value"].as_str().unwrap_or_default();
                        let value = value.strip_prefix(EX).unwrap_or(value);
                        match term["datatype"].as_str() {
                            Some(dt) => format!("{value}^^{}", dt.rsplit('#').next().unwrap()),
                            None => value.to_string(),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        rows.sort();
        Ok(rows)
    }
}

/// What one run of a shape did: the answers with fast paths on and off, and
/// the routing stamps of the fast run.
pub struct Run {
    pub fast: Result<Vec<String>, String>,
    pub generic: Result<Vec<String>, String>,
    pub proceeded: Vec<String>,
    pub fell_back: Vec<(String, String)>,
}

pub async fn run(fx: &Fixture, ledger: &str, prologue: &str, dataset: &str, shape: Shape) -> Run {
    let _switch = switch().lock().await;
    set_fast_paths_disabled(false);
    let (store, guard) = init_test_tracing();
    let fast = fx.sparql(ledger, prologue, dataset, shape).await;
    drop(guard);
    set_fast_paths_disabled(true);
    let generic = fx.sparql(ledger, prologue, dataset, shape).await;
    set_fast_paths_disabled(false);
    let stamps = outcomes(&store);
    Run {
        fast,
        generic,
        proceeded: stamps
            .iter()
            .filter(|(_, o)| o == "proceed")
            .map(|(s, _)| s.clone())
            .collect(),
        fell_back: stamps.into_iter().filter(|(_, o)| o != "proceed").collect(),
    }
}
