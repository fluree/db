"""RDF 1.2 triple terms: queries and streams return them as fluree.Triple,
and a Triple is a value to write and a parameter to match."""

import pickle

import pytest

import fluree
from fluree import IRI, BlankNode, LangString, Triple

EX = "http://example.org/"
REIFIES = "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"
PREFIXES = f"PREFIX ex: <{EX}> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "

CLAIMS = f"""
@prefix ex: <{EX}> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
ex:alice ex:knows ex:bob {{| ex:since 2020 |}} .
ex:claim rdf:reifies <<( ex:carol ex:age 30 )>> ; ex:source ex:wiki .
"""

KNOWS = Triple(EX + "alice", EX + "knows", IRI(EX + "bob"))
CAROL = Triple(EX + "carol", EX + "age", 30)


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("claims")
        ledger.insert(CLAIMS)
        yield ledger


def test_rdf_reifies_binds_the_triple_term(ledger):
    rows = ledger.query(PREFIXES + "SELECT ?r ?t WHERE { ?r rdf:reifies ?t }")
    assert {row.t for row in rows} == {KNOWS, CAROL}
    claim = ledger.query(PREFIXES + "SELECT ?t WHERE { ex:claim rdf:reifies ?t }").single().t
    subject, predicate, value = claim
    assert (subject, predicate, value) == (IRI(EX + "carol"), IRI(EX + "age"), 30)
    assert type(subject) is IRI and type(predicate) is IRI and type(value) is int


def test_a_scan_returns_the_reification_links(ledger):
    rows = ledger.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    assert {row.o for row in rows if row.p == REIFIES} == {KNOWS, CAROL}


def test_a_stream_returns_triple_terms(ledger):
    with ledger.stream(PREFIXES + "SELECT ?t WHERE { ex:claim rdf:reifies ?t }") as rows:
        assert [row.t for row in rows] == [CAROL]


def test_a_triple_term_parameter_finds_its_reifiers(ledger):
    sparql = PREFIXES + "SELECT ?r WHERE { ?r rdf:reifies $t }"
    assert ledger.query(sparql, t=CAROL).single().r == IRI(EX + "claim")
    # In an expression it is the same triple term.
    sparql = PREFIXES + "SELECT ?r WHERE { ?r rdf:reifies ?t FILTER(?t = $t) }"
    assert ledger.query(sparql, t=CAROL).single().r == IRI(EX + "claim")
    sparql = PREFIXES + "SELECT ?since WHERE { ?r rdf:reifies $t ; ex:since ?since }"
    assert ledger.query(sparql, t=KNOWS).single().since == 2020


def test_a_claim_is_written_with_its_triple_term(ledger):
    ledger.insert({"@id": EX + "report", "@reifies": KNOWS, EX + "source": IRI(EX + "news")})
    sparql = PREFIXES + "SELECT ?source WHERE { ?r rdf:reifies $t ; ex:source ?source }"
    assert [row.source for row in ledger.query(sparql, t=KNOWS)] == [IRI(EX + "news")]


def test_a_claim_does_not_assert_its_triple(ledger):
    ledger.insert({"@id": EX + "rumour", "@reifies": Triple(EX + "bob", EX + "age", 99)})
    assert ledger.query(PREFIXES + "ASK { ex:bob ex:age 99 }") is False
    sparql = PREFIXES + "SELECT ?t WHERE { ex:rumour rdf:reifies ?t }"
    assert ledger.query(sparql).single().t == Triple(EX + "bob", EX + "age", 99)


def test_a_triple_term_is_not_a_subject_parameter(ledger):
    with pytest.raises(fluree.InvalidRequestError, match="is a subject"):
        ledger.query(PREFIXES + "SELECT ?p WHERE { $t ?p ?o }", t=CAROL)


def test_a_commit_lists_its_triple_terms(ledger):
    links = {change.value for change in ledger.changes(1) if change.predicate == REIFIES}
    assert links == {KNOWS, CAROL}


def test_a_change_keeps_blank_nodes_tags_and_nesting():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("nested")
        commit = ledger.insert(f"""
            @prefix ex: <{EX}> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            ex:claim rdf:reifies <<( _:someone ex:says <<( ex:dave ex:label "hi"@en )>> )>> .
        """)
        (link,) = [c.value for c in ledger.changes(commit.t) if c.predicate == REIFIES]
        assert type(link.subject) is BlankNode
        assert link.object == Triple(EX + "dave", EX + "label", LangString("hi", "en"))
        assert link.object.object.language == "en"


def test_a_triple_takes_plain_strings_as_iris():
    triple = Triple(EX + "a", EX + "p", "text")
    assert type(triple.subject) is IRI and type(triple.predicate) is IRI
    assert triple.object == "text" and type(triple.object) is str
    assert type(Triple(BlankNode("fdb-1"), EX + "p", 1).subject) is BlankNode
    for subject, predicate in [
        (42, EX + "p"),
        (LangString("a", "en"), EX + "p"),
        (EX + "s", BlankNode("b")),
        (EX + "s", 7),
    ]:
        with pytest.raises(TypeError):
            Triple(subject, predicate, 1)


def test_a_triple_hashes_compares_and_pickles():
    nested = Triple(EX + "doc", EX + "says", CAROL)
    assert pickle.loads(pickle.dumps(nested)) == nested
    assert hash(Triple(IRI(EX + "carol"), IRI(EX + "age"), 30)) == hash(CAROL)
    assert CAROL != Triple(EX + "carol", EX + "age", 31)
