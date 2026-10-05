"""SPARQL parameters: a value bound to a variable runs as if written in its place."""

import asyncio
import datetime as dt
from decimal import Decimal

import pytest

import fluree
import fluree.aio
from fluree import IRI, InvalidRequestError, LangString, Literal, Node

EX = "http://example.org/"
P = f"PREFIX ex: <{EX}> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def ledger(conn):
    ledger = conn.create("people")
    ledger.update(
        P
        + """INSERT DATA {
            ex:alice ex:name "Alice" ; ex:age 30 ; ex:knows ex:bob ;
                     ex:born "1994-05-01"^^xsd:date ; ex:greeting "bonjour"@fr ;
                     ex:balance 12.50 ; ex:active true .
            ex:bob ex:name "Bob" ; ex:age 17 .
            ex:carol ex:name "Carol" .
        }"""
    )
    return ledger


def test_a_parameter_stands_in_for_its_variable(ledger):
    inline = ledger.query(P + 'SELECT ?s ?age WHERE { ?s ex:name "Alice" ; ex:age ?age FILTER(?age > 21) }')
    for query in (
        P + "SELECT ?s ?age WHERE { ?s ex:name $name ; ex:age ?age FILTER(?age > $min) }",
        P + "SELECT ?s ?age WHERE { ?s ex:name ?name ; ex:age ?age FILTER(?age > ?min) }",
    ):
        by_keyword = ledger.query(query, name="Alice", min=21)
        by_dict = ledger.query(query, {"name": "Alice", "min": 21})
        assert list(by_keyword) == list(by_dict) == list(inline)
        assert by_keyword.single().age == 30


def test_rdf_terms_and_python_values(ledger):
    def who(where, **params):
        return ledger.query(P + f"SELECT ?s WHERE {{ {where} }}", **params).value("s")

    assert who("?s ex:knows $friend", friend=IRI(EX + "bob")) == [IRI(EX + "alice")]
    assert who("?s ex:born $day", day=dt.date(1994, 5, 1)) == [IRI(EX + "alice")]
    assert who("?s ex:greeting $word", word=LangString("bonjour", "fr")) == [IRI(EX + "alice")]
    assert who("?s ex:greeting $word", word=LangString("bonjour", "en")) == []
    assert who("?s ex:balance $amount", amount=Decimal("12.50")) == [IRI(EX + "alice")]
    assert who("?s ex:active $flag", flag=True) == [IRI(EX + "alice")]
    assert who("?s ex:age $age", age=Literal("17", "http://www.w3.org/2001/XMLSchema#integer")) == [
        IRI(EX + "bob")
    ]
    names = ledger.query(P + "SELECT ?n WHERE { $who ex:name ?n }", who=IRI(EX + "carol"))
    assert names.value("n") == ["Carol"]


def test_a_cypher_node_stands_for_its_element_id(ledger):
    ledger.update('CREATE (:Person {name: "Dan"})')
    node = ledger.query("MATCH (p:Person) RETURN p").single()["p"]
    assert isinstance(node, Node)
    assert ledger.query("SELECT ?n WHERE { $p <name> ?n }", p=node).value("n") == ["Dan"]


def test_a_filter_inside_optional_sees_the_value(ledger):
    rows = ledger.query(
        P + "SELECT ?name ?age WHERE { ?s ex:name ?name OPTIONAL { ?s ex:age ?age FILTER(?age >= $min) } } "
        "ORDER BY ?name",
        min=18,
    )
    assert rows.values() == [["Alice", 30], ["Bob", None], ["Carol", None]]


def test_a_projected_parameter_is_a_column(ledger):
    result = ledger.query(P + "SELECT $name ?age WHERE { ?s ex:name $name ; ex:age ?age }", name="Bob")
    assert result.keys() == ["name", "age"]
    assert result.single().data() == {"name": "Bob", "age": 17}


def test_mistakes_are_refused(ledger):
    with pytest.raises(InvalidRequestError, match="not a variable"):
        ledger.query(P + "SELECT ?s WHERE { ?s ex:name $name }", nmae="Alice")
    with pytest.raises(InvalidRequestError, match="assigned"):
        ledger.query(P + "SELECT ?x WHERE { ?s ex:age ?a BIND(?a AS ?x) }", x=1)
    with pytest.raises(InvalidRequestError, match="subject"):
        ledger.query(P + "SELECT ?n WHERE { $who ex:name ?n }", who="Carol")
    with pytest.raises(InvalidRequestError, match="JSON-LD"):
        ledger.query({"select": ["?s"], "where": {"@id": "?s", EX + "name": "?n"}}, n="Alice")
    with pytest.raises(InvalidRequestError, match="not an RDF term"):
        ledger.query(P + "SELECT ?s WHERE { ?s ex:name $name }", name=object())


def test_stream_explain_and_profile(ledger):
    query = P + "SELECT ?s WHERE { ?s ex:name $name }"
    with ledger.stream(query, name="Bob") as rows:
        assert [r.s for r in rows] == [IRI(EX + "bob")]
    inline = ledger.explain(P + 'SELECT ?s WHERE { ?s ex:name "Bob" }')
    assert ledger.explain(query, name="Bob")["plan"] == inline["plan"]
    assert ledger.profile(query, name="Bob").result.value("s") == [IRI(EX + "bob")]
    snapshot = ledger.snapshot()
    assert snapshot.query(query, name="Bob").value("s") == [IRI(EX + "bob")]
    assert [r.s for r in snapshot.stream(query, name="Bob")] == [IRI(EX + "bob")]
    assert snapshot.explain(query, name="Bob")["plan"] == inline["plan"]


def test_updates_take_parameters(ledger):
    birthday = (
        P + "DELETE { ?s ex:age ?old } INSERT { ?s ex:age $age } WHERE { ?s ex:name $name ; ex:age ?old }"
    )
    ledger.update(birthday, name="Bob", age=18)
    with ledger.transaction() as txn:
        txn.update(birthday, {"name": "Alice", "age": 31})
        assert txn.query(P + "SELECT ?age WHERE { ?s ex:name $name ; ex:age ?age }", name="Alice").value(
            "age"
        ) == [31]
    ages = ledger.query(P + "SELECT ?name ?age WHERE { ?s ex:name ?name ; ex:age ?age } ORDER BY ?name")
    assert ages.values() == [["Alice", 31], ["Bob", 18]]
    with pytest.raises(InvalidRequestError, match="not a variable"):
        ledger.update(birthday, nmae="Bob", age=1)
    with pytest.raises(InvalidRequestError, match="JSON-LD"):
        ledger.update({"where": {"@id": "?s"}, "delete": {"@id": "?s"}}, s=1)


def test_a_connection_query_takes_parameters(conn, ledger):
    result = conn.query(P + "SELECT ?age FROM <people> WHERE { ?s ex:name $name ; ex:age ?age }", name="Alice")
    assert result.value("age") == [30]


def test_asyncio(ledger):
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            people = await conn.create("people")
            await people.update(P + 'INSERT DATA { ex:a ex:name "A" . ex:b ex:name "B" }')
            query = P + "SELECT ?s WHERE { ?s ex:name $name }"
            result = await people.query(query, name="B")
            streamed = [r.s async for r in people.stream(query, name="B")]
            return result.value("s"), streamed

    result, streamed = asyncio.run(main())
    assert result == streamed == [IRI(EX + "b")]


def test_time_travel_keeps_parameters(ledger):
    ledger.update(P + 'DELETE DATA { ex:bob ex:age 17 } ; INSERT DATA { ex:bob ex:age 18 }')
    query = P + "SELECT ?age WHERE { ?s ex:name $name ; ex:age ?age }"
    assert ledger.at(t=1).query(query, name="Bob").value("age") == [17]
    assert ledger.query(query, name="Bob").value("age") == [18]
