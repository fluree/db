import pytest

import fluree
from fluree import InvalidRequestError, PermissionDeniedError

EX = "http://example.org/"
F = "https://ns.flur.ee/db#"
GRAPH = EX + "graphs/staff"
CONTEXT = {"ex": EX}


def staff(*people):
    return {"@context": CONTEXT, "@graph": [{"@id": f"ex:{p.lower()}", "ex:name": p} for p in people]}


def names(ledger, graph=None):
    pattern = "?s ex:name ?name"
    where = f"GRAPH <{graph}> {{ {pattern} }}" if graph else pattern
    return [n for (n,) in ledger.query(f"PREFIX ex: <{EX}> SELECT ?name WHERE {{ {where} }} ORDER BY ?name")]


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        yield conn.create("people")


def test_sync_commits_only_the_difference(ledger):
    first = ledger.sync(staff("Alice", "Bob"), message="initial")
    assert (first.t, first.asserts, first.retracts) == (1, 2, 0)

    second = ledger.sync(staff("Alice", "Carol"))
    assert (second.t, second.asserts, second.retracts) == (2, 1, 1)
    assert second.id == ledger.log()[0].id
    assert names(ledger) == ["Alice", "Carol"]
    assert names(ledger.at(t=1)) == ["Alice", "Bob"]
    assert ledger.log()[-1].message == "initial"


def test_unchanged_data_writes_no_commit(ledger):
    ledger.sync(staff("Alice"))
    again = ledger.sync(staff("Alice"))
    assert (again.id, again.t, again.asserts, again.retracts) == (None, 1, 0, 0)
    assert len(ledger.log()) == 1


def test_dry_run(ledger):
    ledger.sync(staff("Alice", "Bob"))
    preview = ledger.sync(staff("Carol"), dry_run=True)
    assert (preview.id, preview.t, preview.asserts, preview.retracts) == (None, 1, 1, 2)
    assert names(ledger) == ["Alice", "Bob"]


def test_named_graph_and_turtle(ledger):
    ledger.insert(staff("Zed"))
    ledger.sync(f'@prefix ex: <{EX}> . ex:alice ex:name "Alice" .', graph=GRAPH)
    assert names(ledger, GRAPH) == ["Alice"]
    assert names(ledger) == ["Zed"]  # the default graph is untouched

    ledger.sync(f'@prefix ex: <{EX}> . ex:bob ex:name "Bob" .', graph=GRAPH)
    assert names(ledger, GRAPH) == ["Bob"]
    assert names(ledger) == ["Zed"]


def test_empty_data_needs_allow_empty(ledger):
    ledger.sync(staff("Alice"))
    with pytest.raises(InvalidRequestError, match="allowEmpty"):
        ledger.sync({"@graph": []})
    cleared = ledger.sync({"@graph": []}, allow_empty=True)
    assert cleared.retracts == 1
    assert names(ledger) == []


def test_sync_under_policy(ledger):
    ledger.sync(staff("Alice"))
    read_only = ledger.with_policy(
        policy=[{"@id": "view-only", "f:action": {"@id": F + "view"}, "f:allow": True}],
    )
    with pytest.raises(PermissionDeniedError):
        read_only.sync(staff("Bob"))
    assert names(ledger) == ["Alice"]
