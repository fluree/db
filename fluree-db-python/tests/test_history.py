import pytest

import fluree
from fluree import Change, IRI

EX = "http://example.org/"
CONTEXT = {"ex": EX}
NAME = EX + "name"
AGE = EX + "age"


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def people(conn):
    ledger = conn.create("people")
    ledger.insert({"@context": CONTEXT, "@id": "ex:alice", "ex:name": "Alice", "ex:age": 30})  # t=1
    ledger.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:name": "Alicia"})  # t=2
    ledger.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": 31})  # t=3
    return ledger


def test_history_of_one_property(people):
    alice = IRI(EX + "alice")
    assert people.history(alice, NAME) == [
        Change(1, "assert", alice, IRI(NAME), "Alice"),
        Change(2, "retract", alice, IRI(NAME), "Alice"),
        Change(2, "assert", alice, IRI(NAME), "Alicia"),
    ]


def test_history_of_every_property(people):
    changes = people.history(EX + "alice")
    # Ordered by t, retractions first; predicates within that are unordered.
    assert [(c.t, c.op) for c in changes] == [
        (1, "assert"), (1, "assert"), (2, "retract"), (2, "assert"), (3, "retract"), (3, "assert"),
    ]
    assert sorted((c.t, c.op == "assert", c.predicate, c.value) for c in changes) == [
        (1, True, AGE, 30),
        (1, True, NAME, "Alice"),
        (2, False, NAME, "Alice"),
        (2, True, NAME, "Alicia"),
        (3, False, AGE, 30),
        (3, True, AGE, 31),
    ]


def test_history_from_t(people):
    assert [c.t for c in people.history(EX + "alice", AGE, from_t=3)] == [3, 3]


def test_history_to_t(people):
    assert [c.t for c in people.history(EX + "alice", AGE, to_t=2)] == [1]


def test_history_rejects_non_iris(people):
    with pytest.raises(ValueError):
        people.history("ex:alice> ?x <")


def test_history_respects_policy(people):
    hidden_age = people.with_policy(
        policy=[
            {"@id": "deny-age", "f:action": {"@id": "https://ns.flur.ee/db#view"},
             "f:onProperty": [{"@id": AGE}], "f:allow": False},
            {"@id": "view-rest", "f:action": {"@id": "https://ns.flur.ee/db#view"}, "f:allow": True},
        ],
    )
    assert {c.predicate for c in hidden_age.history(EX + "alice")} == {NAME}


def test_connection_query_spans_ledgers(conn):
    conn.create("a").insert({"@context": CONTEXT, "@id": "ex:x", "ex:n": 1})
    conn.create("b").insert({"@context": CONTEXT, "@id": "ex:y", "ex:n": 2})
    rows = conn.query(
        f"PREFIX ex: <{EX}> SELECT ?s ?n FROM <a:main> FROM <b:main> WHERE {{ ?s ex:n ?n }} ORDER BY ?n"
    )
    assert rows.columns == ["s", "n"]
    assert [(r.s, r.n) for r in rows] == [(EX + "x", 1), (EX + "y", 2)]
    jsonld = conn.query({"@context": CONTEXT, "from": ["a:main", "b:main"], "select": "?n",
                         "where": {"@id": "?s", "ex:n": "?n"}, "orderBy": "?n"})
    assert jsonld == [1, 2]


def test_connection_query_at_a_time(conn):
    ledger = conn.create("a")
    ledger.insert({"@context": CONTEXT, "@id": "ex:x", "ex:n": 1})
    ledger.insert({"@context": CONTEXT, "@id": "ex:y", "ex:n": 2})
    rows = conn.query(f"PREFIX ex: <{EX}> SELECT ?n FROM <a:main@t:1> WHERE {{ ?s ex:n ?n }}")
    assert [r.n for r in rows] == [1]


def test_log_newest_first(people):
    log = people.log()
    assert [c.t for c in log] == [3, 2, 1]
    assert all(c.time is not None and c.time.tzinfo is not None for c in log)
    assert log[0].digest.startswith(log[0].short_id)
    assert [c.t for c in people.log(limit=2)] == [3, 2]


def test_changes_by_t_id_and_digest_prefix(people):
    head = people.log(limit=1)[0]
    expected = [
        Change(3, "retract", IRI(EX + "alice"), IRI(AGE), 30),
        Change(3, "assert", IRI(EX + "alice"), IRI(AGE), 31),
    ]
    by_op = lambda changes: sorted(changes, key=lambda c: c.op != "retract")  # noqa: E731
    assert by_op(people.changes(3)) == expected
    assert by_op(people.changes(head.id)) == expected
    assert by_op(people.changes(head.short_id)) == expected


def test_changes_resolve_refs_and_blank_nodes(conn):
    ledger = conn.create("refs")
    ledger.insert(f"@prefix ex: <{EX}> . ex:a ex:knows ex:b ; ex:owns [ ex:n 1 ] .")
    changes = ledger.changes(1)
    knows = next(c for c in changes if c.predicate == EX + "knows")
    assert knows.value == IRI(EX + "b") and isinstance(knows.value, IRI)
    owned = next(c for c in changes if c.predicate == EX + "owns").value
    assert isinstance(owned, fluree.BlankNode)
    assert any(c.subject == owned and c.value == 1 for c in changes)


def test_changes_respect_policy(people):
    hidden_age = people.with_policy(
        policy=[
            {"@id": "deny-age", "f:action": {"@id": "https://ns.flur.ee/db#view"},
             "f:onProperty": [{"@id": AGE}], "f:allow": False},
            {"@id": "view-rest", "f:action": {"@id": "https://ns.flur.ee/db#view"}, "f:allow": True},
        ],
    )
    assert hidden_age.changes(3) == []
    assert {c.predicate for c in hidden_age.changes(1)} == {NAME}
