"""Engine features that need no API of their own — reasoning, datalog rules,
edge annotations, named graphs, geospatial values — working through the
package's verbs, in each query language that has them."""

import datetime as dt
from decimal import Decimal

import pytest

import fluree
from fluree import IRI, BlankNode, Literal

EX = "http://example.org/"
F = "https://ns.flur.ee/db#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
GEO = "http://www.opengis.net/ont/geosparql#"
P = f"PREFIX ex: <{EX}> "
CTX = {"ex": EX, "rdfs": RDFS, "owl": OWL, "f": F}


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


# -- reasoning -----------------------------------------------------------------


@pytest.fixture
def family(conn):
    ledger = conn.create("family")
    ledger.insert({
        "@context": CTX,
        "@graph": [
            {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
            {"@id": "ex:alice", "@type": "ex:Student", "ex:name": "Alice"},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"},
            {"@id": "ex:livesWith", "@type": "owl:SymmetricProperty"},
            {"@id": "ex:alice", "ex:livesWith": {"@id": "ex:bob"}},
            {"@id": "ex:hasAncestor", "@type": "owl:TransitiveProperty"},
            {"@id": "ex:carol", "ex:hasAncestor": {"@id": "ex:dave"}, "ex:parent": {"@id": "ex:dave"}},
            {"@id": "ex:dave", "ex:hasAncestor": {"@id": "ex:eve"}, "ex:parent": {"@id": "ex:eve"}},
        ],
    })
    return ledger


PEOPLE = P + "SELECT ?name WHERE { ?s a ex:Person ; ex:name ?name } ORDER BY ?name"
PEOPLE_JSONLD = {
    "@context": CTX,
    "select": "?name",
    "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name"},
    "orderBy": "?name",
}


def test_reasoning_is_opt_in_in_both_languages(family):
    assert family.query(PEOPLE).value("name") == ["Bob"]
    assert family.query("# PRAGMA reasoning: rdfs\n" + PEOPLE).value("name") == ["Alice", "Bob"]
    assert family.select("# PRAGMA reasoning: rdfs\n" + PEOPLE).value("name") == ["Alice", "Bob"]
    assert family.query(PEOPLE_JSONLD) == ["Bob"]
    assert family.query({**PEOPLE_JSONLD, "reasoning": "rdfs"}) == ["Alice", "Bob"]
    assert family.query({**PEOPLE_JSONLD, "reasoning": "none"}) == ["Bob"]


def test_owl2rl_infers_symmetric_and_transitive_facts(family):
    reasoned = "# PRAGMA reasoning: owl2rl\n" + P
    assert family.query(reasoned + "SELECT ?w WHERE { ex:bob ex:livesWith ?w }").value("w") == [IRI(EX + "alice")]
    ancestors = family.query(reasoned + "SELECT ?a WHERE { ex:carol ex:hasAncestor ?a } ORDER BY ?a")
    assert ancestors.value("a") == [IRI(EX + "dave"), IRI(EX + "eve")]


def test_reasoning_on_snapshots_and_in_transactions(family):
    past = family.at(t=1)
    with family.transaction() as txn:
        txn.insert({"@context": CTX, "@id": "ex:fay", "@type": "ex:Student", "ex:name": "Fay"})
        assert txn.query({**PEOPLE_JSONLD, "reasoning": "rdfs"}) == ["Alice", "Bob", "Fay"]
    assert past.query({**PEOPLE_JSONLD, "reasoning": "rdfs"}) == ["Alice", "Bob"]


# -- datalog rules -------------------------------------------------------------

GRANDPARENT_RULE = {
    "@context": {"ex": EX},
    "where": {"@id": "?person", "ex:parent": {"ex:parent": "?gp"}},
    "insert": {"@id": "?person", "ex:grandparent": {"@id": "?gp"}},
}
GRANDPARENTS = {"@context": CTX, "select": "?gp", "where": {"@id": "ex:carol", "ex:grandparent": "?gp"}}


def test_query_time_rules(family):
    assert family.query(GRANDPARENTS) == []
    assert family.query({**GRANDPARENTS, "reasoning": "datalog", "rules": [GRANDPARENT_RULE]}) == ["ex:eve"]


def test_stored_rules_in_json_ld_and_sparql(family):
    family.insert({
        "@context": CTX,
        "@id": "ex:grandparentRule",
        "f:rule": {"@type": "@json", "@value": GRANDPARENT_RULE},
    })
    assert family.query({**GRANDPARENTS, "reasoning": "datalog"}) == ["ex:eve"]
    sparql = "# PRAGMA reasoning: datalog\n" + P + "SELECT ?gp WHERE { ex:carol ex:grandparent ?gp }"
    assert family.query(sparql).value("gp") == [IRI(EX + "eve")]

    family.insert({
        "@context": CTX,
        "@id": "ex:greatRule",
        "f:rule": {
            "@type": "f:sparql",
            "@value": P + "CONSTRUCT { ?p ex:elder ?a } WHERE { ?p ex:grandparent ?a }",
        },
    })
    elders = "# PRAGMA reasoning: datalog\n" + P + "SELECT ?a WHERE { ex:carol ex:elder ?a }"
    assert family.query(elders).value("a") == [IRI(EX + "eve")]


def test_an_invalid_rule_is_an_error_not_a_silent_skip(family):
    bad = {**GRANDPARENT_RULE, "where": [["optional", {"@id": "?person", "ex:parent": "?p"}]]}
    with pytest.raises(fluree.InvalidRequestError):
        family.query({**GRANDPARENTS, "reasoning": "datalog", "rules": [bad]})


# -- edge annotations ----------------------------------------------------------


@pytest.fixture
def staff(conn):
    ledger = conn.create("staff")
    ledger.insert({
        "@context": CTX,
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            # Python values inside an annotation convert as property values do.
            "@annotation": {"ex:role": "Engineer", "ex:since": dt.date(2024, 1, 2), "ex:by": IRI(EX + "hr")},
        },
    })
    return ledger


def test_annotations_read_back_in_sparql_and_json_ld(staff):
    row = staff.query(
        P + "SELECT ?role ?since ?by WHERE { ex:alice ex:worksFor ex:acme {| ex:role ?role ; ex:since ?since ; ex:by ?by |} }"
    ).single()
    assert (row.role, row.since, row.by) == ("Engineer", dt.date(2024, 1, 2), IRI(EX + "hr"))
    jsonld = staff.query({
        "@context": CTX,
        "select": ["?org", "?role"],
        "where": {"@id": "ex:alice", "ex:worksFor": {"@id": "?org", "@annotation": {"ex:role": "?role"}}},
    })
    assert jsonld == [["ex:acme", "Engineer"]]


def test_annotation_rooted_queries_and_parameters(staff):
    by_role = P + "SELECT ?who ?ann WHERE { ?who ex:worksFor ex:acme ~ ?ann {| ex:role $role |} }"
    row = staff.query(by_role, role="Engineer").single()
    assert row.who == IRI(EX + "alice") and isinstance(row.ann, BlankNode)
    rooted = staff.query({
        "@context": CTX,
        "select": "?person",
        "where": {"ex:since": dt.date(2024, 1, 2), "@reifies": {"@id": "?person", "ex:worksFor": {"@id": "ex:acme"}}},
    })
    assert rooted == ["ex:alice"]


def test_annotations_from_turtle_and_cypher(conn):
    ledger = conn.create("graph")
    ledger.insert(f'@prefix ex: <{EX}> . ex:bob ex:knows ex:carol {{| ex:confidence 0.9 |}} .')
    rows = ledger.query(P + "SELECT ?c WHERE { ex:bob ex:knows ex:carol {| ex:confidence ?c |} }")
    assert rows.value("c") == [Decimal("0.9")]  # a Turtle 0.9 is an xsd:decimal
    ledger.update("CREATE (:Person {name: 'Dan'})-[:KNOWS {since: 2020}]->(:Person {name: 'Eve'})")
    since = ledger.query("MATCH (:Person {name: 'Dan'})-[k:KNOWS]->() RETURN k.since AS since").single()
    assert since.since == 2020
    sparql = "SELECT ?since WHERE { ?a <KNOWS> ?b {| <since> ?since |} }"
    assert ledger.query(sparql).value("since") == [2020]


# -- named graphs --------------------------------------------------------------

STAFF_GRAPH = EX + "graphs/staff"


def test_named_graphs_across_write_and_query_forms(conn):
    ledger = conn.create("org")
    ledger.insert(f'@prefix ex: <{EX}> . GRAPH <{STAFF_GRAPH}> {{ ex:alice ex:name "Alice" }}', format="trig")
    ledger.update(P + f'INSERT DATA {{ GRAPH <{STAFF_GRAPH}> {{ ex:bob ex:name "Bob" }} }}')
    ledger.insert({"@context": CTX, "@id": "ex:carol", "@graph": STAFF_GRAPH, "ex:name": "Carol"})
    ledger.insert({"@context": CTX, "@id": "ex:dave", "ex:name": "Dave"})

    in_graph = P + f"SELECT ?n WHERE {{ GRAPH <{STAFF_GRAPH}> {{ ?s ex:name ?n }} }} ORDER BY ?n"
    assert ledger.query(in_graph).value("n") == ["Alice", "Bob", "Carol"]
    assert ledger.query(P + "SELECT ?n WHERE { ?s ex:name ?n }").value("n") == ["Dave"]
    jsonld = ledger.query({
        "@context": CTX,
        "select": "?n",
        "where": [["graph", STAFF_GRAPH, {"@id": "?s", "ex:name": "?n"}]],
        "orderBy": "?n",
    })
    assert jsonld == ["Alice", "Bob", "Carol"]
    from_graph = conn.query({
        "@context": CTX,
        "from": {"@id": "org:main", "graph": STAFF_GRAPH},
        "select": "?n",
        "where": {"@id": "?s", "ex:name": "?n"},
        "orderBy": "?n",
    })
    assert from_graph == ["Alice", "Bob", "Carol"]


def test_graphs_are_listed_and_dropped(conn):
    ledger = conn.create("org")
    ledger.insert(f'@prefix ex: <{EX}> . GRAPH <{STAFF_GRAPH}> {{ ex:alice ex:name "Alice" }}', format="trig")
    ledger.insert({"@context": CTX, "@id": "ex:dave", "ex:name": "Dave"})
    assert ledger.graphs() == [IRI(STAFF_GRAPH)]
    before = ledger.log()[0].t
    commit = ledger.drop_graph(STAFF_GRAPH)
    assert commit.retracts == 1
    assert ledger.query(P + f"ASK {{ GRAPH <{STAFF_GRAPH}> {{ ?s ?p ?o }} }}") is False
    assert ledger.at(t=before).query(P + f"ASK {{ GRAPH <{STAFF_GRAPH}> {{ ?s ?p ?o }} }}") is True
    assert ledger.query(P + "ASK { ex:dave ex:name ?n }") is True


# -- geospatial ----------------------------------------------------------------


def point(lon, lat):
    return Literal(f"POINT({lon} {lat})", GEO + "wktLiteral")


@pytest.fixture
def places(conn):
    ledger = conn.create("places")
    ledger.insert({
        "@context": CTX,
        "@graph": [
            {"@id": "ex:eiffel", "ex:location": point(2.2945, 48.8584)},
            {"@id": "ex:louvre", "ex:location": {"@value": "POINT(2.3376 48.8606)", "@type": GEO + "wktLiteral"}},
            {"@id": "ex:colosseum", "ex:location": point(12.4922, 41.8902)},
        ],
    })
    return ledger


NEAR = (
    P + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
    "SELECT ?place ?d WHERE { ?place ex:location ?loc BIND(geof:distance(?loc, $center) AS ?d) "
    "FILTER(?d < $radius) } ORDER BY ?d"
)


def test_points_round_trip_as_wkt_literals(places):
    assert places.query(P + "SELECT ?l WHERE { ex:eiffel ex:location ?l }").single().l == point(2.2945, 48.8584)


def test_distance_search_in_sparql_and_json_ld(places):
    near = places.query(NEAR, center=point(2.35, 48.85), radius=10_000)
    assert near.value("place") == [IRI(EX + "louvre"), IRI(EX + "eiffel")]
    assert 1_000 < near[0].d < 2_000
    jsonld = places.query({
        "@context": CTX,
        "select": "?place",
        "where": [
            {"@id": "?place", "ex:location": "?loc"},
            ["bind", "?d", '(geof:distance ?loc "POINT(2.35 48.85)")'],
            ["filter", "(< ?d 10000)"],
        ],
        "orderBy": "?d",
    })
    assert jsonld == ["ex:louvre", "ex:eiffel"]
