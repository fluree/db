import pytest

import fluree

EX = "http://example.org/"
CONTEXT = {"ex": EX}
TURTLE = f"""
@prefix ex: <{EX}> .
ex:alice ex:name "Alice" ; ex:age 42 .
GRAPH ex:g {{ ex:bob ex:name "Bob" }}
"""


@pytest.fixture
def conn(tmp_path):
    with fluree.connect(tmp_path / "db") as conn:
        yield conn


@pytest.fixture
def people(conn):
    ledger = conn.create("people")
    ledger.insert(TURTLE)
    return ledger


def test_export_returns_text_and_round_trips(conn, people):
    text = people.export(format="ntriples")
    assert f'<{EX}alice> <{EX}name> "Alice" .' in text
    assert "Bob" not in text  # default graph only
    copy = conn.create("copy")
    copy.insert(text)
    assert copy.query(f"PREFIX ex: <{EX}> ASK {{ ex:alice ex:age 42 }}") is True


def test_export_named_graphs(people):
    assert "Bob" in people.export(format="nquads", all_graphs=True)
    only_g = people.export(format="ntriples", graph=EX + "g")
    assert "Bob" in only_g and "Alice" not in only_g


def test_export_to_file_infers_format(people, tmp_path):
    out = tmp_path / "people.ttl"
    assert people.export(out, context=CONTEXT) is None
    text = out.read_text()
    assert "@prefix ex:" in text and "ex:alice" in text


def test_export_jsonld(people):
    import json

    doc = json.loads(people.export(format="jsonld", context=CONTEXT))
    assert any(node.get("@id") == "ex:alice" for node in doc["@graph"])


def test_export_past_state(people):
    t = people.snapshot().t
    people.insert({"@context": CONTEXT, "@id": "ex:carol", "ex:name": "Carol"})
    assert "Carol" in people.export(format="ntriples")
    assert "Carol" not in people.at(t=t).export(format="ntriples")


def test_archive_and_restore(conn, people, tmp_path):
    archive = tmp_path / "people.flpack"
    people.archive(archive)
    restored = conn.restore(archive, "restored")
    assert restored.id == "restored:main"
    assert restored.query(f"PREFIX ex: <{EX}> ASK {{ ex:alice ex:age 42 }}") is True
    assert [c.t for c in restored.log()] == [c.t for c in people.log()]


def test_governed_ledger_cannot_export(people, tmp_path):
    governed = people.with_policy(identity=EX + "someone")
    with pytest.raises(PermissionError):
        governed.export()
    with pytest.raises(PermissionError):
        governed.snapshot().export()
    with pytest.raises(PermissionError):
        governed.archive(tmp_path / "x.flpack")
