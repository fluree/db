import pytest

import fluree
from fluree import IRI, InvalidRequestError, PermissionDeniedError, ValidationResult

EX = "http://example.org/"
SH = "http://www.w3.org/ns/shacl#"
CONTEXT = {"ex": EX}
SHAPES = {
    "@context": {"sh": SH, "ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"},
    "@id": "ex:PersonShape",
    "@type": "sh:NodeShape",
    "sh:targetClass": {"@id": "ex:Person"},
    "sh:property": [
        {"sh:path": {"@id": "ex:age"}, "sh:datatype": {"@id": "xsd:integer"}},
        {"sh:path": {"@id": "ex:name"}, "sh:minCount": 1},
    ],
}
SHAPES_TURTLE = f"""
@prefix sh: <{SH}> . @prefix ex: <{EX}> .
ex:NamedShape a sh:NodeShape ; sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"""
PEOPLE = {
    "@context": CONTEXT,
    "@graph": [
        {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 30},
        {"@id": "ex:bob", "@type": "ex:Person", "ex:age": "old"},
    ],
}


@pytest.fixture
def conn(tmp_path):
    with fluree.connect(tmp_path) as conn:
        yield conn


@pytest.fixture
def ledger(conn):
    ledger = conn.create("people")
    ledger.insert(PEOPLE)
    return ledger


def test_validate_against_given_shapes(ledger):
    report = ledger.validate(SHAPES)
    assert not report.conforms
    assert (report.shape_count, report.t) == (1, 1)
    by_path = {r.path: r for r in report.results}
    assert by_path[IRI(EX + "age")] == ValidationResult(
        focus=IRI(EX + "bob"),
        path=IRI(EX + "age"),
        message=by_path[IRI(EX + "age")].message,
        severity="violation",
        value="old",
        shape=IRI(EX + "PersonShape"),
        component=IRI(SH + "DatatypeConstraintComponent"),
    )
    assert by_path[IRI(EX + "name")].focus == IRI(EX + "bob")
    assert by_path[IRI(EX + "name")].value is None

    assert ledger.validate(SHAPES_TURTLE).shape_count == 1


def test_validate_with_no_shapes_conforms(ledger):
    report = ledger.validate()
    assert report.conforms and report.shape_count == 0 and report.results == []


def test_validate_against_a_shapes_graph(ledger):
    graph = EX + "shapes"
    ledger.update(
        f"PREFIX sh: <{SH}> PREFIX ex: <{EX}> INSERT DATA {{ GRAPH <{graph}> {{ "
        "ex:NamedShape a sh:NodeShape ; sh:targetClass ex:Person ; "
        "sh:property [ sh:path ex:name ; sh:minCount 1 ] } }"
    )
    report = ledger.validate(shapes_graph=graph)
    assert [(r.focus, r.path) for r in report.results] == [(IRI(EX + "bob"), IRI(EX + "name"))]
    with pytest.raises(InvalidRequestError):
        ledger.validate(SHAPES, shapes_graph=graph)


def test_validate_is_bounded(ledger):
    with pytest.raises(fluree.ResourceLimitError):
        ledger.validate(SHAPES, max_fuel=0.001)


def test_indexing(ledger):
    status = ledger.index_status()
    assert status.enabled and status.commit_t == 1
    assert ledger.index(timeout=30) == 1
    assert ledger.index_status().index_t == 1
    assert ledger.reindex() == 1
    assert len(ledger.query(f"PREFIX ex: <{EX}> SELECT ?s WHERE {{ ?s a ex:Person }}")) == 2


def test_index_needs_background_indexing():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("people")
        ledger.insert(PEOPLE)
        assert not ledger.index_status().enabled
        with pytest.raises(InvalidRequestError):
            ledger.index()


def test_verify(ledger):
    ledger.insert({"@context": CONTEXT, "@id": "ex:carol", "ex:name": "Carol"})
    report = ledger.verify()
    assert report.healthy and report.problems == []
    assert (report.head_t, report.commits_checked, report.truncated) == (2, 2, False)
    assert ledger.verify(max_commits=1).commits_checked == 1


def test_sweep(ledger):
    for age in range(31, 34):
        ledger.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": age})
        ledger.index(timeout=30)
    planned = ledger.sweep(dry_run=True)
    assert planned.dry_run and planned.reclaimed == 0
    swept = ledger.sweep()
    assert swept.reclaimed == swept.orphans and swept.failures == []
    assert ledger.sweep(dry_run=True).orphans == 0
    assert ledger.verify().healthy
    assert len(ledger.query(f"PREFIX ex: <{EX}> SELECT ?s WHERE {{ ?s a ex:Person }}")) == 2


def test_ops_refused_on_a_governed_ledger(ledger):
    governed = ledger.with_policy(default_allow=True)
    for attempt in (governed.validate, governed.index, governed.reindex, governed.verify, governed.sweep):
        with pytest.raises(PermissionDeniedError):
            attempt()
    assert governed.index_status().commit_t == 1
