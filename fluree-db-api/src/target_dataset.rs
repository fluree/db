//! A query's dataset, resolved in the one ledger a surface targets.
//!
//! A ledger route (`/query/<ledger>`), a view, and the CLI with a ledger all
//! read a dataset reference in the ledger they address: its own address in
//! any spelling names it, a keyword or a registered graph IRI names one of its
//! graphs, and another ledger is either refused or left for the dataset lane
//! to load. This module is the one place that reading happens for JSON-LD, so
//! the server and the CLI agree with each other and with the engine's own
//! dataset parser ([`DatasetSpec::from_json`](crate::DatasetSpec::from_json)),
//! whose key precedence it follows: `opts` before the top level, `from` before
//! `ledger`, `fromNamed` before `from-named`.

use crate::dataset::source_object_id;
use crate::{ApiError, Result};
use fluree_db_core::{
    DatasetRef, GraphId, GraphIri, GraphSel, LedgerId, LedgerRef, MemberRef, TargetError,
    TargetLedger, TimeSpec,
};
use serde_json::{Map, Value as JsonValue};

/// A lookup of the target ledger's registered graph IRIs. Surfaces pass a
/// borrowed read of the ledger's snapshot
/// ([`LedgerView::graph_id_for_iri`](crate::LedgerView::graph_id_for_iri)), so
/// each reference costs one lookup and the registry is never copied.
pub type GraphLookup<'a> = &'a dyn Fn(&str) -> Option<GraphId>;

/// One dataset reference, resolved in the target ledger.
#[derive(Clone, Debug)]
pub struct InTarget {
    /// The graph of the target ledger the reference names.
    pub graph: GraphSel,
    /// The time the reference pins, if it is an address with a pin.
    pub at: Option<TimeSpec>,
    /// Written as the target's own address (any spelling, with or without a
    /// graph and a pin), which the dataset parsers read as is.
    pub own_address: bool,
}

/// Resolve one dataset reference (a SPARQL `FROM` / `FROM NAMED` IRI after
/// prefix and BASE expansion, or a JSON-LD `from` / `fromNamed` string) in
/// `target`. The target's own address in any spelling names its default
/// graph, or the graph the address names; a keyword or a registered IRI names
/// that graph; another ledger's address is a 400. An IRI `graphs` does not
/// hold is left to loading, which reports a graph the ledger does not have
/// (404). With no `graphs` (a surface that cannot read the registry: the CLI
/// against a remote server), text that reads both as a graph IRI and as
/// another ledger's address (`other:main`) is that address, as a surface with
/// no target reads it: nothing says it is a graph of the target.
pub fn resolve_in_target(
    target: &LedgerId,
    graphs: Option<GraphLookup<'_>>,
    written: &str,
) -> Result<InTarget> {
    if let Some(own) = own_id_as_written(target, written) {
        return Ok(own);
    }
    let member = MemberRef::parse(written).map_err(|e| not_a_graph(written, e))?;
    resolve_member_in_target(target, graphs, written, &member)
}

/// The target's id exactly as it stands is its own address with no pin and
/// no graph: its default graph, whatever the registry holds (resolution's
/// step 0). Recognized as is, so the id is not parsed a second time. An id
/// has exactly one `:`; one stored before the current grammar may also hold
/// `@`, `#` or `://`, which a dataset position reads otherwise, so such an id
/// takes the full parse.
fn own_id_as_written(target: &LedgerId, written: &str) -> Option<InTarget> {
    if written != target.as_str() || written.contains(['@', '#']) || written.contains("://") {
        return None;
    }
    debug_assert!(MemberRef::parse(written).is_ok_and(|member| member
        .address()
        .is_some_and(|a| a.id() == target && a.at().is_none() && a.graph().is_default())));
    Some(InTarget {
        graph: GraphSel::Default,
        at: None,
        own_address: true,
    })
}

fn not_a_graph(written: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::invalid_query(format!("<{written}> is not a graph of this ledger: {e}"))
}

/// [`resolve_in_target`] for a reference the caller has already parsed.
fn resolve_member_in_target(
    target: &LedgerId,
    graphs: Option<GraphLookup<'_>>,
    written: &str,
    member: &MemberRef,
) -> Result<InTarget> {
    let at = member.address().and_then(|a| a.at().cloned());
    let own_address = member.address().is_some_and(|a| a.id() == target);
    let none = |_: &str| -> Option<GraphId> { None };
    let lookup: GraphLookup<'_> = graphs.unwrap_or(&none);
    match TargetLedger::new(target, &lookup).resolve(written, member, at.is_some()) {
        Ok(graph) => Ok(InTarget {
            graph: graph.graph,
            at,
            own_address,
        }),
        Err(TargetError::GraphNotFound(iri)) => Ok(InTarget {
            graph: GraphIri::parse(&iri)
                .map(GraphSel::Named)
                .map_err(|e| not_a_graph(written, e))?,
            at,
            own_address,
        }),
        Err(TargetError::CrossLedger {
            named,
            target,
            also_iri,
        }) => Err(ApiError::invalid_query(format!(
            "Ledger mismatch: endpoint ledger is '{target}' but <{written}> {}names ledger \
             '{named}'",
            if also_iri {
                "is not a graph of this ledger, and as an address it "
            } else {
                ""
            }
        ))),
    }
}

/// Where a JSON-LD query runs on a surface that targets one ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonLdLane {
    /// The target's own view: the query names no dataset, or a lone `from`
    /// naming a ledger with nothing but its address (no graph, no time).
    View,
    /// The dataset lane: named graphs, several sources, a graph, a time, or a
    /// history range.
    Dataset,
}

/// What a single JSON-LD `from` (a string or one object) may name besides
/// the target ledger. A `from` array and `fromNamed` may always name other
/// ledgers; they stay as written for the dataset lane to load.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SingleFrom {
    /// Only the target: another ledger is a 400 (a ledger route, whose path
    /// scopes the request).
    TargetOnly,
    /// Any ledger: another one stays as written (a surface that can hand the
    /// query to the connection path instead).
    AnyLedger,
}

/// The outcome of [`resolve_jsonld_dataset_in_target`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JsonLdInTarget {
    /// Where the rewritten query runs.
    pub lane: JsonLdLane,
    /// Whether some source names a ledger other than the target (it was left
    /// as written).
    pub names_other_ledger: bool,
}

/// Where the engine's dataset parser reads one JSON-LD dataset key.
#[derive(Clone, Copy)]
struct KeyAt {
    in_opts: bool,
    key: &'static str,
}

impl KeyAt {
    fn get<'a>(&self, obj: &'a Map<String, JsonValue>) -> Option<&'a JsonValue> {
        if self.in_opts {
            obj.get("opts")?.as_object()?.get(self.key)
        } else {
            obj.get(self.key)
        }
    }

    fn set(&self, obj: &mut Map<String, JsonValue>, value: JsonValue) {
        let map = if self.in_opts {
            match obj.get_mut("opts").and_then(JsonValue::as_object_mut) {
                Some(opts) => opts,
                None => return,
            }
        } else {
            obj
        };
        map.insert(self.key.to_string(), value);
    }
}

/// The dataset keys a JSON-LD query carries, located as
/// [`DatasetSpec::from_json`](crate::DatasetSpec::from_json) reads them.
struct DatasetKeys {
    from: Option<KeyAt>,
    from_named: Option<KeyAt>,
    to: bool,
}

fn dataset_keys(obj: &Map<String, JsonValue>) -> DatasetKeys {
    let opts = obj.get("opts").and_then(JsonValue::as_object);
    let find = |keys: &[&'static str]| -> Option<KeyAt> {
        for (in_opts, map) in [(true, opts), (false, Some(obj))] {
            let Some(map) = map else { continue };
            if let Some(key) = keys.iter().find(|key| map.contains_key(**key)) {
                return Some(KeyAt { in_opts, key });
            }
        }
        None
    };
    DatasetKeys {
        from: find(&["from", "ledger"]),
        from_named: find(&["fromNamed", "from-named"]),
        to: find(&["to"]).is_some(),
    }
}

/// Whether a JSON-LD query names a dataset source: a `from` (or `ledger`) or
/// a `fromNamed` (or `from-named`), in `opts` or at the top level.
pub fn jsonld_names_dataset(query: &JsonValue) -> bool {
    query.as_object().is_some_and(|obj| {
        let keys = dataset_keys(obj);
        keys.from.is_some() || keys.from_named.is_some()
    })
}

/// The lane a JSON-LD query needs, read from its dataset keys as the engine's
/// parser reads them. A lone `from` naming a whole ledger at head (an address
/// with no graph and no time, as a string or as an object with nothing but
/// its id, [`source_object_id`]) runs on that ledger's view; any other
/// dataset (named graphs,
/// several sources, a graph, a time, a history range, a source that is no
/// ledger address) needs the dataset lane.
pub fn jsonld_lane(query: &JsonValue) -> JsonLdLane {
    let Some(obj) = query.as_object() else {
        return JsonLdLane::View;
    };
    let keys = dataset_keys(obj);
    if keys.to || keys.from_named.is_some() {
        return JsonLdLane::Dataset;
    }
    let Some(from) = keys.from.and_then(|at| at.get(obj)) else {
        return JsonLdLane::View;
    };
    let lone_ledger = match from {
        JsonValue::String(s) => names_whole_ledger(s),
        JsonValue::Object(o) => {
            o.keys().all(|k| k == "@id" || k == "id")
                && source_object_id(o).is_some_and(names_whole_ledger)
        }
        _ => false,
    };
    if lone_ledger {
        JsonLdLane::View
    } else {
        JsonLdLane::Dataset
    }
}

/// Whether `written` reads as a whole ledger at head: an address with no
/// graph and no time.
fn names_whole_ledger(written: &str) -> bool {
    !written.contains('@')
        && !written.contains('#')
        && MemberRef::parse(written).is_ok_and(|m| m.address().is_some_and(LedgerRef::is_bare))
}

/// The ledger a JSON-LD query's view reads, when [`jsonld_lane`] is
/// [`JsonLdLane::View`] and the query names one: its lone `from`, as written.
pub fn jsonld_view_ledger(query: &JsonValue) -> Option<String> {
    let obj = query.as_object()?;
    if jsonld_lane(query) != JsonLdLane::View {
        return None;
    }
    match dataset_keys(obj).from?.get(obj)? {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Object(o) => source_object_id(o).map(str::to_string),
        _ => None,
    }
}

/// The ledger a JSON-LD query addresses, as written, read with the engine's
/// dataset-key precedence: the ledger its view reads ([`jsonld_view_ledger`]),
/// else the first source that reads as a ledger address, else its first
/// source. `None` when it names no source. A surface with no ledger of its
/// own uses it to pick the view a query runs on and to label the request; the
/// dataset lane reads every source itself.
pub fn jsonld_dataset_ledger(query: &JsonValue) -> Option<String> {
    if let Some(ledger) = jsonld_view_ledger(query) {
        return Some(ledger);
    }
    let obj = query.as_object()?;
    let keys = dataset_keys(obj);
    let mut written = Vec::new();
    for (at, named) in [(keys.from, false), (keys.from_named, true)] {
        if let Some(value) = at.and_then(|at| at.get(obj)) {
            source_texts(value, named, &mut written);
        }
    }
    written
        .iter()
        .find(|w| MemberRef::parse(w).is_ok_and(|m| m.address().is_some()))
        .or_else(|| written.first())
        .map(|w| (*w).to_string())
}

/// The written text of every source in one `from` / `fromNamed` value.
fn source_texts<'a>(value: &'a JsonValue, named: bool, out: &mut Vec<&'a str>) {
    match value {
        JsonValue::String(s) => out.push(s),
        JsonValue::Array(items) => {
            for item in items {
                source_texts(item, named, out);
            }
        }
        JsonValue::Object(obj) => match source_object_id(obj) {
            Some(id) => out.push(id),
            // `fromNamed` object form: aliases mapped to sources.
            None if named => {
                for source in obj.values() {
                    source_texts(source, named, out);
                }
            }
            None => {}
        },
        _ => {}
    }
}

/// Resolve a JSON-LD query's dataset in `target`, the ledger the surface
/// addresses, rewriting the query in place so the dataset lane reads what
/// each source names:
///
/// - a graph of `target`, named by keyword, registered IRI or `target#g`,
///   becomes `{"@id": <target>, "graph": <graph>}` (a `fromNamed` one keeps
///   its written text as its alias);
/// - the target's own address in any spelling stays as written;
/// - another ledger stays as written, except in a single `from` under
///   [`SingleFrom::TargetOnly`], where it is a 400.
///
/// Returns the lane the rewritten query runs on (the target's view only when
/// the dataset is the target itself) and whether it names another ledger.
pub fn resolve_jsonld_dataset_in_target(
    query: &mut JsonValue,
    target: &LedgerId,
    graphs: Option<GraphLookup<'_>>,
    single_from: SingleFrom,
) -> Result<JsonLdInTarget> {
    let mut names_other_ledger = false;
    if let Some(obj) = query.as_object_mut() {
        let keys = dataset_keys(obj);
        for (at, named) in [(keys.from, false), (keys.from_named, true)] {
            let Some(at) = at else { continue };
            let Some(value) = at.get(obj) else { continue };
            let scope = match (named, value, single_from) {
                (false, JsonValue::String(_) | JsonValue::Object(_), SingleFrom::TargetOnly) => {
                    Scope::TargetOnly
                }
                _ => Scope::AnyLedger,
            };
            let mut sources = Sources {
                target,
                graphs,
                named,
                scope,
                names_other_ledger: false,
            };
            let normalized = sources.normalize(value)?;
            names_other_ledger |= sources.names_other_ledger;
            at.set(obj, normalized);
        }
    }
    // A lone `from` naming some other ledger runs on that ledger, not on the
    // target's view.
    let lane = match jsonld_view_ledger(query) {
        Some(ledger) if !is_own_address(&ledger, target) => JsonLdLane::Dataset,
        _ => jsonld_lane(query),
    };
    Ok(JsonLdInTarget {
        lane,
        names_other_ledger,
    })
}

/// Whether `written` is `target`'s own address (any spelling, no pin, the
/// default graph).
fn is_own_address(written: &str, target: &LedgerId) -> bool {
    MemberRef::parse(written)
        .ok()
        .and_then(|member| member.address().cloned())
        .is_some_and(|address| address.is_own_address(target))
}

/// Whether `member` names a ledger other than `target`: an address that can
/// be nothing else (`other`, `other@t:1`), or, when the target's registry is
/// unknown, text that also reads as a graph IRI (`other:main`), since with no
/// registry to hold it as a graph the address is the reading a surface with
/// no target gives it.
fn names_another_ledger(member: &MemberRef, target: &LedgerId, registry_known: bool) -> bool {
    match member {
        MemberRef::Dataset(DatasetRef::Address(address)) => address.id() != target,
        MemberRef::Dataset(DatasetRef::Ambiguous { address, .. }) => {
            !registry_known && address.id() != target
        }
        MemberRef::Dataset(DatasetRef::GraphIri(_)) | MemberRef::Keyword(_) => false,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    TargetOnly,
    AnyLedger,
}

struct Sources<'a> {
    target: &'a LedgerId,
    graphs: Option<GraphLookup<'a>>,
    named: bool,
    scope: Scope,
    names_other_ledger: bool,
}

impl Sources<'_> {
    /// Resolve one source, or `None` when it names another ledger the
    /// position may name (it then stays as written). The source is parsed
    /// once, here, and every reading below takes the parsed reference.
    fn source(&mut self, written: &str) -> Result<Option<InTarget>> {
        if let Some(own) = own_id_as_written(self.target, written) {
            return Ok(Some(own));
        }
        let member = match MemberRef::parse(written) {
            Ok(member) => member,
            // Text that is no reference stays as written where another ledger
            // may stand, for the dataset lane to report.
            Err(_) if self.scope == Scope::AnyLedger => return Ok(None),
            Err(e) => return Err(not_a_graph(written, e)),
        };
        if self.scope == Scope::AnyLedger {
            let registered = self.graphs.is_some_and(|graphs| graphs(written).is_some());
            let other_ledger = member.address().is_some_and(|a| a.id() != self.target);
            if other_ledger && !registered {
                self.names_other_ledger |=
                    names_another_ledger(&member, self.target, self.graphs.is_some());
                return Ok(None);
            }
        }
        resolve_member_in_target(self.target, self.graphs, written, &member).map(Some)
    }

    fn normalize(&mut self, value: &JsonValue) -> Result<JsonValue> {
        match value {
            JsonValue::String(written) => {
                let Some(scoped) = self.source(written)? else {
                    return Ok(value.clone());
                };
                if scoped.own_address {
                    return Ok(value.clone());
                }
                let mut src = Map::new();
                src.insert(
                    "@id".to_string(),
                    JsonValue::String(self.target.to_string()),
                );
                src.insert(
                    "graph".to_string(),
                    JsonValue::String(scoped.graph.to_string()),
                );
                if self.named {
                    src.insert("alias".to_string(), JsonValue::String(written.clone()));
                }
                Ok(JsonValue::Object(src))
            }
            JsonValue::Array(items) => items
                .iter()
                .map(|item| self.normalize(item))
                .collect::<Result<Vec<_>>>()
                .map(JsonValue::Array),
            // The object names its source as the parser reads it: `@id`, or
            // else `id`.
            JsonValue::Object(obj) => match source_object_id(obj) {
                Some(id) => {
                    let Some(scoped) = self.source(id)? else {
                        return Ok(value.clone());
                    };
                    if scoped.own_address {
                        return Ok(value.clone());
                    }
                    // The object names a graph of this ledger by its id.
                    if obj.contains_key("graph") || obj.contains_key("@graph") {
                        return Err(ApiError::invalid_query(format!(
                            "'{id}' names a graph of this ledger, so it takes no \"graph\" of \
                             its own"
                        )));
                    }
                    let mut src = obj.clone();
                    src.remove("id");
                    src.insert(
                        "@id".to_string(),
                        JsonValue::String(self.target.to_string()),
                    );
                    src.insert(
                        "graph".to_string(),
                        JsonValue::String(scoped.graph.to_string()),
                    );
                    if self.named && !src.contains_key("alias") {
                        src.insert("alias".to_string(), JsonValue::String(id.to_string()));
                    }
                    Ok(JsonValue::Object(src))
                }
                // `fromNamed` object form: aliases mapped to sources.
                None if self.named => obj
                    .iter()
                    .map(|(alias, source)| {
                        self.normalize(source).map(|source| (alias.clone(), source))
                    })
                    .collect::<Result<Map<_, _>>>()
                    .map(JsonValue::Object),
                // A `from` object that names no source is the parser's error,
                // refused here so no execution path reads past it.
                None => Err(ApiError::invalid_query(
                    "a 'from' object must name its source with an '@id' or 'id' string field",
                )),
            },
            other => Ok(other.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn id(s: &str) -> LedgerId {
        LedgerId::parse(s).unwrap()
    }

    fn graphs(names: &'static [&'static str]) -> impl Fn(&str) -> Option<GraphId> {
        move |iri: &str| {
            names
                .iter()
                .position(|n| *n == iri)
                .map(|i| GraphId::try_from(i + 3).unwrap())
        }
    }

    /// The lane is read from the keys the parser reads: `opts.from` wins over a
    /// top-level `from`, and `opts.fromNamed` is a dataset like `fromNamed`.
    #[test]
    fn the_lane_follows_the_parsers_key_precedence() {
        assert_eq!(jsonld_lane(&json!({"select": "?s"})), JsonLdLane::View);
        assert_eq!(
            jsonld_lane(&json!({"from": "books:main"})),
            JsonLdLane::View
        );
        assert_eq!(
            jsonld_lane(&json!({"from": {"@id": "books:main"}})),
            JsonLdLane::View
        );
        for dataset in [
            json!({"from": "books:main@t:1"}),
            json!({"from": "books:main#g"}),
            json!({"from": ["books:main"]}),
            json!({"from": {"@id": "books:main", "t": 1}}),
            json!({"fromNamed": ["books:main"]}),
            json!({"from": "books:main", "to": "books:main@t:latest"}),
            json!({"from": "books:main", "opts": {"from": "books:main@t:1"}}),
            json!({"opts": {"from": "books:main#g"}}),
            json!({"opts": {"fromNamed": "books:main"}}),
            json!({"ledger": "books:main@t:1"}),
        ] {
            assert_eq!(jsonld_lane(&dataset), JsonLdLane::Dataset, "{dataset}");
        }
        // A plain top-level `from` with a pinned `opts.from`: the view ledger
        // is not read from the top level the parser ignores.
        assert_eq!(
            jsonld_view_ledger(&json!({"from": "a:main", "opts": {"from": "b:main"}})).as_deref(),
            Some("b:main")
        );
        assert_eq!(
            jsonld_view_ledger(&json!({"from": "a:main", "opts": {"from": "b:main@t:1"}})),
            None
        );
    }

    /// The target's id as it stands reads exactly as every other spelling of
    /// its address does: its default graph, even where a graph is registered
    /// under that text, with or without a registry.
    #[test]
    fn the_targets_own_id_reads_as_its_other_spellings() {
        let target = id("books:main");
        let lookup = graphs(&["books:main"]);
        let lookup: GraphLookup<'_> = &lookup;
        for graphs in [Some(lookup), None] {
            for written in [
                "books:main",
                "books",
                "urn:fluree:books:main",
                "books:main#default",
            ] {
                let resolved = resolve_in_target(&target, graphs, written).unwrap();
                assert!(matches!(resolved.graph, GraphSel::Default), "{written}");
                assert_eq!(resolved.at, None, "{written}");
                assert!(resolved.own_address, "{written}");
            }
        }
        // A stored id from before the current grammar, with `@` in its name,
        // is not the plain address its text spells: it takes the full parse.
        let legacy = LedgerId::parse_persisted("old@db:main").unwrap();
        let resolved = resolve_in_target(&legacy, None, legacy.as_str());
        assert!(
            !matches!(
                resolved,
                Ok(InTarget {
                    graph: GraphSel::Default,
                    own_address: true,
                    ..
                })
            ),
            "{resolved:?}"
        );
    }

    /// Graphs of the target become object members; the target itself and
    /// other ledgers (where allowed) stay as written.
    #[test]
    fn sources_resolve_in_the_target() {
        let target = id("books:main");
        let lookup = graphs(&["http://ex.org/g"]);
        let lookup: GraphLookup<'_> = &lookup;
        for (written, expected) in [
            ("config", json!({"@id": "books:main", "graph": "config"})),
            (
                "http://ex.org/g",
                json!({"@id": "books:main", "graph": "http://ex.org/g"}),
            ),
            // The target's own address, with or without a graph, stays as
            // written: the dataset parsers read any spelling of it.
            (
                "books:main#http://ex.org/g",
                json!("books:main#http://ex.org/g"),
            ),
            ("books", json!("books")),
            ("urn:fluree:books:main", json!("urn:fluree:books:main")),
        ] {
            let mut q = json!({"from": written});
            let out = resolve_jsonld_dataset_in_target(
                &mut q,
                &target,
                Some(lookup),
                SingleFrom::TargetOnly,
            )
            .unwrap();
            assert_eq!(q["from"], expected, "{written}");
            assert!(!out.names_other_ledger, "{written}");
        }

        // `opts.from` is the one the parser reads, so it is the one resolved.
        let mut q = json!({"from": "books:main", "opts": {"from": "http://ex.org/g"}});
        let out =
            resolve_jsonld_dataset_in_target(&mut q, &target, Some(lookup), SingleFrom::TargetOnly)
                .unwrap();
        assert_eq!(
            q["opts"]["from"],
            json!({"@id": "books:main", "graph": "http://ex.org/g"})
        );
        assert_eq!(out.lane, JsonLdLane::Dataset);

        // A single `from` naming another ledger: refused on a ledger route,
        // kept for a surface that can send it elsewhere.
        let mut q = json!({"from": "other"});
        let err =
            resolve_jsonld_dataset_in_target(&mut q, &target, Some(lookup), SingleFrom::TargetOnly)
                .unwrap_err();
        assert!(err.to_string().contains("Ledger mismatch"), "{err}");
        let out =
            resolve_jsonld_dataset_in_target(&mut q, &target, Some(lookup), SingleFrom::AnyLedger)
                .unwrap();
        assert_eq!(q["from"], json!("other"));
        assert!(out.names_other_ledger);
        assert_eq!(
            out.lane,
            JsonLdLane::Dataset,
            "another ledger is not the target's view"
        );

        // A `fromNamed` graph keeps its written text as its alias; another
        // ledger there stays as written.
        let mut q = json!({"fromNamed": ["http://ex.org/g", "other"]});
        let out =
            resolve_jsonld_dataset_in_target(&mut q, &target, Some(lookup), SingleFrom::TargetOnly)
                .unwrap();
        assert_eq!(
            q["fromNamed"],
            json!([
                {"@id": "books:main", "graph": "http://ex.org/g", "alias": "http://ex.org/g"},
                "other"
            ])
        );
        assert!(out.names_other_ledger);
    }

    /// An `@id` that already names a graph of the target cannot carry a graph
    /// of its own, in either spelling of the key.
    #[test]
    fn a_graph_member_takes_no_second_graph() {
        let target = id("books:main");
        for key in ["graph", "@graph"] {
            let mut q = json!({"from": {"@id": "config", key: "http://ex.org/g"}});
            let err =
                resolve_jsonld_dataset_in_target(&mut q, &target, None, SingleFrom::TargetOnly)
                    .unwrap_err();
            assert!(err.to_string().contains("takes no"), "{key}: {err}");
        }
    }

    /// A source object names its source as the dataset parser reads it:
    /// `@id`, or else `id`. A single `from` naming the target that way reads
    /// it (on the target's view when nothing else is named), one naming
    /// another ledger that way is refused, and an object that names no source
    /// is refused rather than passed on.
    #[test]
    fn a_source_object_is_read_by_the_parsers_id_keys() {
        let target = id("books:main");
        let lookup = graphs(&[]);
        let lookup: GraphLookup<'_> = &lookup;
        for (from, reads_target) in [
            (json!({"id": "books:main"}), true),
            (json!({"id": "books"}), true),
            (json!({"@id": "books:main", "id": "other:main"}), true),
            (json!({"id": "other:main"}), false),
            (json!({"id": "other:main", "t": 1}), false),
            (json!({"id": "books:main", "@id": "other:main"}), false),
            (json!({}), false),
            (json!({"graph": "txn-meta"}), false),
            (json!({"@id": 5, "id": "books:main"}), false),
        ] {
            let mut q = json!({"from": from});
            let got = resolve_jsonld_dataset_in_target(
                &mut q,
                &target,
                Some(lookup),
                SingleFrom::TargetOnly,
            );
            assert_eq!(got.is_ok(), reads_target, "{from}: {got:?}");
            if reads_target {
                assert_eq!(got.unwrap().lane, JsonLdLane::View, "{from}");
            }
        }
        // A `from` array reads the same keys: another ledger named by `id`
        // stays for the dataset lane to load, and is reported.
        let mut q = json!({"from": [{"id": "other"}]});
        let out =
            resolve_jsonld_dataset_in_target(&mut q, &target, Some(lookup), SingleFrom::TargetOnly)
                .expect("an array may name another ledger");
        assert!(out.names_other_ledger);
        assert_eq!(
            jsonld_dataset_ledger(&json!({"from": [{"id": "other"}]})).as_deref(),
            Some("other")
        );
    }
}
