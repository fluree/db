"""One set of verbs and one result type across SPARQL, Cypher and JSON-LD."""

import pytest

import fluree
from fluree import IRI, BlankNode, InvalidRequestError, Node, Record, Result
from fluree._connection import _text_language

EX = "http://example.org/"
SPARQL_NAMES = f"PREFIX ex: <{EX}> SELECT ?name ?age WHERE {{ ?s ex:name ?name OPTIONAL {{ ?s ex:age ?age }} }} ORDER BY ?name"
CYPHER_NAMES = "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name"


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("people")
        ledger.insert({
            "@context": {"ex": EX},
            "@graph": [{"@id": "ex:alice", "ex:name": "Alice", "ex:age": 30}, {"@id": "ex:bob", "ex:name": "Bob"}],
        })
        ledger.update('CREATE (:Person {name: "Carol", age: 40}), (:Person {name: "Dan"})')
        yield ledger


@pytest.mark.parametrize("query", [SPARQL_NAMES, CYPHER_NAMES], ids=["sparql", "cypher"])
def test_one_result_type(ledger, query):
    result = ledger.query(query)
    assert isinstance(result, Result) and len(result) == 2
    assert result.keys() == result.columns == ["name", "age"]
    first = result[0]
    assert isinstance(first, Record)
    assert first.name == first["name"] == first[0] == first.get("name")
    name, age = first
    assert first.data() == {"name": name, "age": age}
    assert result.value("age")[1] is None
    assert result.values()[0] == [name, age]
    assert [r.name for r in result] == result.value("name")
    assert len(result.data()) == 2


def test_methods_win_over_attribute_access(ledger):
    record = ledger.query(f"PREFIX ex: <{EX}> SELECT (COUNT(?s) AS ?count) WHERE {{ ?s ex:name ?n }}").single()
    assert record["count"] == 2
    assert callable(record.count)  # tuple.count, not the column
    with pytest.raises(AttributeError):
        record.nope


def test_single_and_frames(ledger):
    assert ledger.query(SPARQL_NAMES + " LIMIT 1").single().name == "Alice"
    assert ledger.query(f"PREFIX ex: <{EX}> SELECT ?n WHERE {{ ex:nobody ex:name ?n }}").first() is None
    pandas = pytest.importorskip("pandas")
    for query in (SPARQL_NAMES, CYPHER_NAMES):
        result = ledger.query(query)
        assert isinstance(result.to_df(), pandas.DataFrame)
        assert list(result.to_pandas()["name"]) == result.value("name")


def test_cypher_writes_go_through_update(ledger):
    with pytest.raises(InvalidRequestError, match=r"update\(\)"):
        ledger.query('CREATE (:Person {name: "Eve"})')
    with pytest.raises(InvalidRequestError, match=r"query\(\)"):
        ledger.update("MATCH (p:Person) RETURN p")
    commit = ledger.update("CREATE (p:Person {name: $name}) RETURN p", name="Eve")
    assert isinstance(commit.result.single()["p"], Node)
    assert ledger.query("MATCH (p:Person {name: $n}) RETURN p.name", {"n": "Eve"}).single()[0] == "Eve"


def test_element_ids_match_sparql_terms(ledger):
    node = ledger.query('MATCH (p:Person {name: "Carol"}) RETURN p').single()["p"]
    assert isinstance(node.element_id, (IRI, BlankNode))
    sparql = ledger.query('SELECT ?s WHERE { ?s <name> "Carol" }').single()
    assert sparql["s"] == node.element_id


def test_language_override_and_files(ledger, tmp_path):
    assert len(ledger.query("MATCH (p:Person) RETURN p", language="cypher")) == 2
    with pytest.raises(InvalidRequestError):
        ledger.query("MATCH (p) RETURN p", language="gremlin")
    script = tmp_path / "add.cypher"
    script.write_text('CREATE (:Person {name: "Fay"})')
    ledger.update(script)
    assert ledger.query('MATCH (p {name: "Fay"}) RETURN p.name').single()[0] == "Fay"


def test_parameters_in_every_language(ledger):
    sparql = f"PREFIX ex: <{EX}> SELECT ?age WHERE {{ ?s ex:name $name ; ex:age ?age }}"
    cypher = "MATCH (p:Person {name: $name}) RETURN p.age AS age"
    assert ledger.query(sparql, name="Alice").value("age") == [30]
    assert ledger.query(cypher, name="Carol").value("age") == [40]
    with pytest.raises(InvalidRequestError, match="JSON-LD"):
        ledger.query({"select": ["?s"], "where": {"@id": "?s"}}, name="Alice")


def test_unsupported_cypher_combinations(ledger):
    with pytest.raises(InvalidRequestError):
        ledger.query(CYPHER_NAMES, max_fuel=1000)
    with pytest.raises(InvalidRequestError):
        ledger.profile(CYPHER_NAMES)
    with pytest.raises(InvalidRequestError):
        ledger.stream(CYPHER_NAMES)
    with pytest.raises(InvalidRequestError):
        ledger._connection.query(CYPHER_NAMES)
    with pytest.raises(InvalidRequestError, match=r"query\(\)"):
        with ledger.transaction() as txn:
            txn.update("MATCH (p) RETURN p")
    assert len(ledger.query(CYPHER_NAMES, timeout=30)) == 2


def test_explain_cypher(ledger):
    assert isinstance(ledger.explain(CYPHER_NAMES), dict)
    assert isinstance(ledger.explain("MATCH (p {name: $n}) RETURN p", n="Alice"), dict)
    assert isinstance(ledger.at(t=1).explain(CYPHER_NAMES), dict)


@pytest.mark.parametrize(
    ("text", "write", "language"),
    [
        ("SELECT * WHERE { ?s ?p ?o }", False, "sparql"),
        ("# a comment\nPREFIX ex: <x:> ASK { ?s ?p ?o }", False, "sparql"),
        ("MATCH (n) RETURN n", False, "cypher"),
        ("// note\n/* block */ OPTIONAL MATCH (n) RETURN n", False, "cypher"),
        ("INSERT DATA { <a> <b> <c> }", True, "sparql"),
        ("CREATE GRAPH <g>", True, "sparql"),
        ("create silent graph <g>", True, "sparql"),
        ("CREATE (n:Person)", True, "cypher"),
        ("DELETE DATA { <a> <b> <c> }", True, "sparql"),
        ("DELETE WHERE { ?s ?p ?o }", True, "sparql"),
        ("DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }", True, "sparql"),
        ("DELETE{ ?s ?p ?o } WHERE { ?s ?p ?o }", True, "sparql"),
        ("WITH <g> DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }", True, "sparql"),
        ("MATCH (n) DETACH DELETE n", True, "cypher"),
        ("UNWIND $rows AS r MERGE (n {id: r.id})", True, "cypher"),
        ("WITH 1 AS x CREATE (:N {x: x})", True, "cypher"),
    ],
)
def test_language_detection(text, write, language):
    assert _text_language(text, write=write) == language


@pytest.mark.parametrize("query", [SPARQL_NAMES, CYPHER_NAMES], ids=["sparql", "cypher"])
def test_select_returns_the_table_query_does(ledger, query):
    assert ledger.select(query).values() == ledger.query(query).values()
    assert ledger.snapshot().select(query).keys() == ["name", "age"]
    with ledger.transaction() as txn:
        txn.insert({"@context": {"ex": EX}, "@id": "ex:eve", "ex:name": "Eve"})
        txn.update('CREATE (:Person {name: "Fay"})')
        assert "Eve" in txn.select(SPARQL_NAMES).value("name")
        assert "Fay" in txn.select(CYPHER_NAMES).value("name")
        txn.rollback()


def test_select_takes_parameters_and_limits(ledger):
    by_name = f"PREFIX ex: <{EX}> SELECT ?age WHERE {{ ?s ex:name $name ; ex:age ?age }}"
    assert ledger.select(by_name, name="Alice").single().age == 30
    assert ledger.select("MATCH (p:Person {name: $n}) RETURN p.age AS age", n="Carol").value("age") == [40]
    with pytest.raises(fluree.ResourceLimitError):
        ledger.select(SPARQL_NAMES, max_fuel=1)
    from_people = SPARQL_NAMES.replace(" WHERE", " FROM <people> WHERE")
    assert ledger._connection.select(from_people).value("name") == ["Alice", "Bob"]


def test_select_refuses_what_is_not_a_table_without_running_it(ledger):
    prefix = f"PREFIX ex: <{EX}> "
    for query, message in (
        (prefix + "ASK { ?s ex:name ?n }", "not ASK"),
        (prefix + "CONSTRUCT { ?s ex:name ?n } WHERE { ?s ex:name ?n }", "not CONSTRUCT"),
        (prefix + "DESCRIBE ex:alice", "not DESCRIBE"),
        ({"select": ["?n"], "where": {"@id": "?s", EX + "name": "?n"}}, "JSON-LD"),
        (prefix + 'INSERT DATA { ex:zed ex:name "Zed" }', "update()"),
    ):
        with pytest.raises(InvalidRequestError, match=message):
            ledger.select(query)  # type: ignore[arg-type]
    assert ledger.query(prefix + "ASK { ex:zed ?p ?o }") is False
    # A query that does not parse runs, to report why.
    with pytest.raises(InvalidRequestError, match="SPARQL error"):
        ledger.select("SELECT ?x WHERE { ?x")


def test_select_is_typed_as_a_result():
    import typing

    for handle in (fluree.Connection, fluree.Ledger, fluree.Snapshot, fluree.Transaction):
        assert typing.get_type_hints(handle.select)["return"] is Result
