"""Python values written as property values read back as the same values,
and match the same way in queries and as parameters."""

import datetime as dt
import math
from decimal import Decimal

import pytest

import fluree
from fluree import IRI, BlankNode, InvalidRequestError, LangString, Literal, Vector

EX = "http://example.org/"
CTX = {"ex": EX}
UTC = dt.timezone.utc

VALUES = {
    "iri": IRI(EX + "target"),
    "lang": LangString("bonjour", "fr"),
    "literal": Literal("abc", EX + "code"),
    "decimal": Decimal("12.50"),
    "datetime": dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
    "date": dt.date(2024, 1, 2),
    "time": dt.time(3, 4, 5),
    "big": 2**70,
    "inf": float("inf"),
    "vector": Vector([0.5, 0.25]),
    "text": "plain",
    "int": 7,
    "bool": True,
}


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("values")
        ledger.insert({"@context": CTX, "@id": "ex:s", **{f"ex:{k}": v for k, v in VALUES.items()}})
        yield ledger


def read(source, prop):
    return source.query(f"PREFIX ex: <{EX}> SELECT ?v WHERE {{ ex:s ex:{prop} ?v }}").single().v


@pytest.mark.parametrize("prop", VALUES)
def test_a_value_reads_back_as_written(ledger, prop):
    value = read(ledger, prop)
    assert value == VALUES[prop] and type(value) is type(VALUES[prop])
    if prop == "lang":
        assert value.language == "fr"


@pytest.mark.parametrize("prop", VALUES)
def test_a_value_matches_itself(ledger, prop):
    sparql = f"PREFIX ex: <{EX}> ASK {{ ex:s ex:{prop} $v }}"
    assert ledger.query(sparql, v=VALUES[prop]) is True
    jsonld = {"@context": CTX, "select": "?s", "where": {"@id": "?s", f"ex:{prop}": VALUES[prop]}}
    assert ledger.query(jsonld) == ["ex:s"]


def test_nan_reads_back_as_nan(ledger):
    ledger.insert({"@context": CTX, "@id": "ex:s", "ex:nan": float("nan")})
    assert math.isnan(read(ledger, "nan"))


def test_an_iri_value_is_a_reference(ledger):
    ledger.insert({"@context": CTX, "@id": "ex:target", "ex:name": "Target"})
    path = f"PREFIX ex: <{EX}> SELECT ?n WHERE {{ ex:s ex:iri ?t . ?t ex:name ?n }}"
    assert ledger.query(path).value() == ["Target"]


def test_blank_nodes_and_cypher_nodes_are_references(ledger):
    ledger.insert({"@context": CTX, "@id": "ex:a", "ex:name": "A", "ex:child": {"ex:name": "kid"}})
    kid = ledger.query(f"PREFIX ex: <{EX}> SELECT ?k WHERE {{ ex:a ex:child ?k }}").single().k
    assert isinstance(kid, BlankNode)
    node = ledger.query("MATCH (n {`http://example.org/name`: 'A'}) RETURN n").single()[0]
    ledger.insert({"@context": CTX, "@id": "ex:b", "ex:sibling": kid, "ex:friend": node})
    row = ledger.query(f"PREFIX ex: <{EX}> SELECT ?s ?f WHERE {{ ex:b ex:sibling ?s ; ex:friend ?f }}").single()
    assert (row.s, row.f) == (kid, IRI(EX + "a"))


def test_keywords_stay_plain_data(ledger):
    ledger.insert({
        "@context": {"ex": IRI(EX)},
        "@id": IRI(EX + "k"),
        "@type": IRI(EX + "Thing"),
        "ex:name": {"@value": LangString("chat", "fr"), "@language": "fr"},
    })
    row = ledger.query(f"PREFIX ex: <{EX}> SELECT ?n WHERE {{ ex:k a ex:Thing ; ex:name ?n }}").single()
    assert row.n == LangString("chat", "fr")


def test_values_without_a_json_form_are_refused(ledger):
    with pytest.raises(TypeError, match="not an RDF term"):
        ledger.insert({"@context": CTX, "@id": "ex:x", "ex:v": object()})
    with pytest.raises(TypeError, match="RDF value"):
        ledger.insert({"@context": CTX, "@id": "ex:x", "ex:v": {"@value": Decimal("1")}})
    with pytest.raises(InvalidRequestError, match="xsd:decimal"):
        ledger.insert({"@context": CTX, "@id": "ex:x", "ex:v": Decimal("NaN")})
    with pytest.raises(TypeError):
        ledger.set_context({"ex": Decimal("1")})


def test_language_strings_compare_consistently():
    en, fr = LangString("hi", "en"), LangString("hi", "fr")
    assert en != fr and not en == fr
    assert en == LangString("hi", "EN") and not en != LangString("hi", "EN")
    assert en == "hi" and not en != "hi"
    assert hash(en) == hash(fr) == hash("hi")  # equal values hash alike


def test_records_and_results_pickle(ledger):
    import pickle

    result = ledger.query(f"PREFIX ex: <{EX}> SELECT ?lang ?decimal WHERE {{ ex:s ex:lang ?lang ; ex:decimal ?decimal }}")
    row = result.single()
    copy = pickle.loads(pickle.dumps(row))
    assert copy == row and copy.keys() == ["lang", "decimal"] and copy.lang.language == "fr"
    assert type(copy) is type(row)
    whole = pickle.loads(pickle.dumps(result))
    assert whole.keys() == result.keys() and list(whole) == list(result)
