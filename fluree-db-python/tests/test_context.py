import pytest

import fluree

EX = "http://example.org/"


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("people")
        ledger.insert({"@context": {"ex": EX}, "@id": "ex:alice", "ex:name": "Alice"})
        yield ledger


def test_no_context_by_default(ledger):
    assert ledger.context is None


def test_default_context_resolves_prefixes(ledger):
    ledger.set_context({"ex": EX})
    assert ledger.context == {"ex": EX}
    # SPARQL without PREFIX, on the ledger and on a snapshot.
    assert ledger.query("ASK { ex:alice ex:name 'Alice' }") is True
    assert ledger.snapshot().query("ASK { ex:alice ex:name 'Alice' }") is True
    # JSON-LD without @context; IRIs come back compacted with it.
    assert ledger.query({"select": "?s", "where": {"@id": "?s", "ex:name": "Alice"}}) == ["ex:alice"]
    # A query's own context still wins.
    rows = ledger.query({"@context": {"x": EX}, "select": "?s", "where": {"@id": "?s", "x:name": "Alice"}})
    assert rows == ["x:alice"]


def test_default_context_applies_to_past_states(ledger):
    t = ledger.snapshot().t
    ledger.set_context({"ex": EX})
    assert ledger.at(t=t).query("ASK { ex:alice ex:name 'Alice' }") is True


def test_context_must_be_an_object(ledger):
    with pytest.raises(ValueError):
        ledger.set_context(["not", "a", "map"])


def test_governed_ledger_cannot_set_context(ledger):
    with pytest.raises(PermissionError):
        ledger.with_policy(identity=EX + "x").set_context({"ex": EX})
