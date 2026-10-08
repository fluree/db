"""RDF documents to and from quads: fluree.parse(), fluree.serialize(), and
quads written to a ledger."""

import pickle
from decimal import Decimal

import pytest

import fluree
from fluree import IRI, BlankNode, InvalidRequestError, LangString, Literal, Quad, Triple

EX = "http://example.org/"
REIFIES = "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"
XSD = "http://www.w3.org/2001/XMLSchema#"
PREFIXES = f"PREFIX ex: <{EX}>\nPREFIX xsd: <{XSD}>\n"


def ex(name):
    return IRI(EX + name)


def test_every_format_reads_the_same_quads():
    triples = [Quad(ex("a"), ex("p"), ex("b")), Quad(ex("a"), ex("name"), "A")]
    texts = {
        "turtle": PREFIXES + 'ex:a ex:p ex:b ; ex:name "A" .',
        "ntriples": f'<{EX}a> <{EX}p> <{EX}b> .\n<{EX}a> <{EX}name> "A" .\n',
    }
    for format, text in texts.items():
        assert set(fluree.parse(text, format)) == set(triples), format

    quads = [*triples, Quad(ex("a"), ex("p"), ex("c"), ex("g"))]
    texts = {
        "trig": PREFIXES + 'ex:a ex:p ex:b ; ex:name "A" . ex:g { ex:a ex:p ex:c }',
        "nquads": texts["ntriples"] + f"<{EX}a> <{EX}p> <{EX}c> <{EX}g> .\n",
    }
    for format, text in texts.items():
        assert set(fluree.parse(text, format)) == set(quads), format


def test_rdf_1_2_reads_whole():
    quads = fluree.parse(
        PREFIXES
        + """
        ex:alice ex:knows ex:bob {| ex:since 2020 |} .
        ex:claim <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> <<( ex:carol ex:age 30 )>> .
        ex:doc ex:says <<( ex:dave ex:says <<( ex:eve ex:age 7 )>> )>> .
        """,
        "turtle",
    )
    knows = Triple(ex("alice"), ex("knows"), ex("bob"))
    links = {q.object: q.subject for q in quads if q.predicate == REIFIES}
    assert set(links) == {knows, Triple(ex("carol"), ex("age"), 30)}
    reifier = links[knows]
    assert isinstance(reifier, BlankNode)
    assert Quad(reifier, ex("since"), 2020) in quads
    # The annotation asserts its triple; the claim does not.
    assert Quad(ex("alice"), ex("knows"), ex("bob")) in quads
    assert not any(q.subject == ex("carol") for q in quads)
    nested = next(q.object for q in quads if q.predicate == ex("says"))
    assert nested.object == Triple(ex("eve"), ex("age"), 7)


def test_blank_nodes_keep_their_labels_and_anonymous_ones_get_fresh_ones():
    quads = fluree.parse(PREFIXES + "_:b1 ex:p [ ex:q ex:r ] .", "turtle")
    subjects = {q.subject for q in quads}
    assert BlankNode("b1") in subjects
    (anonymous,) = subjects - {BlankNode("b1")}
    assert isinstance(anonymous, BlankNode) and anonymous != "b1"
    # Fresh labels are ones every format writes as they are.
    assert not anonymous.startswith("-")
    assert set(fluree.parse(fluree.serialize(quads, "ntriples"), "ntriples")) == set(quads)


def test_a_collection_reads_as_its_rdf_list():
    rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    quads = fluree.parse(PREFIXES + "ex:s ex:list ( 1 2 ) .", "turtle")
    by_predicate = {}
    for q in quads:
        by_predicate.setdefault(q.predicate[len(rdf) :] if q.predicate.startswith(rdf) else "list", []).append(q)
    assert sorted(q.object for q in by_predicate["first"]) == [1, 2]
    assert len(by_predicate["rest"]) == 2 and len(by_predicate["list"]) == 1


def test_literals_are_python_values():
    quads = fluree.parse(
        PREFIXES
        + """ex:s ex:int 42 ; ex:dec 1.50 ; ex:dbl 1.5e0 ; ex:bool true ;
              ex:lang "hi"@en--ltr ; ex:other "x"^^ex:custom ; ex:lead "01"^^xsd:integer .""",
        "turtle",
    )
    values = {q.predicate[len(EX) :]: q.object for q in quads}
    assert values["int"] == 42 and type(values["int"]) is int
    assert values["dec"] == Decimal("1.50")
    assert values["dbl"] == 1.5 and type(values["dbl"]) is float
    assert values["bool"] is True
    assert values["lang"] == LangString("hi", "en--ltr")
    assert values["other"] == Literal("x", EX + "custom")
    assert values["lead"] == 1


def test_lexical_literals_keep_their_spelling():
    doc = (
        f'<{EX}s> <{EX}int> "01"^^<{XSD}integer> .\n'
        f'<{EX}s> <{EX}dbl> "1.0E0"^^<{XSD}double> .\n'
        f'<{EX}s> <{EX}bool> "1"^^<{XSD}boolean> .\n'
        f'<{EX}s> <{EX}when> "2020-01-01T00:00:00.000Z"^^<{XSD}dateTime> .\n'
        f'<{EX}s> <{EX}str> "x" .\n'
        f'<{EX}s> <{EX}lang> "y"@en .\n'
        f'<{EX}s> <{EX}says> <<( <{EX}a> <{EX}b> "+2"^^<{XSD}integer> )>> .\n'
    )
    quads = fluree.parse(doc, "ntriples", literals="lexical")
    values = {q.predicate[len(EX) :]: q.object for q in quads}
    assert values["int"] == Literal("01", XSD + "integer")
    assert values["bool"] == Literal("1", XSD + "boolean")
    assert values["str"] == "x" and type(values["str"]) is str
    assert values["lang"] == LangString("y", "en")
    assert values["says"].object == Literal("+2", XSD + "integer")
    assert fluree.serialize(quads, "ntriples") == doc
    turtle = fluree.serialize(quads, "turtle")
    assert fluree.parse(turtle, "turtle", literals="lexical") == quads, turtle

    assert fluree.parse(doc, "ntriples")[0].object == 1
    with pytest.raises(InvalidRequestError, match="literals="):
        fluree.parse(doc, "ntriples", literals="exact")


def test_serialize_round_trips_every_format():
    default = [
        Quad(ex("a"), ex("p"), ex("b")),
        Quad(ex("a"), ex("v"), Decimal("2.5")),
        Quad(BlankNode("r"), IRI(REIFIES), Triple(ex("a"), ex("p"), ex("b"))),
        Quad(BlankNode("r"), ex("since"), 2020),
        Quad(ex("doc"), ex("says"), Triple(ex("c"), ex("p"), LangString("x", "fr"))),
    ]
    named = [*default, Quad(ex("a"), ex("p"), ex("c"), ex("g")), Quad(ex("a"), ex("p"), 1, BlankNode("g2"))]
    for format, quads in [("ntriples", default), ("turtle", default), ("nquads", named), ("trig", named)]:
        text = fluree.serialize(quads, format, prefixes={"ex": EX})
        assert set(fluree.parse(text, format)) == set(quads), f"{format}:\n{text}"


def test_a_reification_of_an_asserted_triple_is_written_as_an_annotation():
    quads = fluree.parse(PREFIXES + "ex:a ex:p ex:b {| ex:since 2020 |} .", "turtle")
    text = fluree.serialize(quads, "turtle", prefixes={"ex": EX})
    assert "ex:a ex:p ex:b ~ " in text, text


def test_a_float_keeps_its_short_form():
    text = f'<{EX}s> <{EX}p> "0.9957"^^<{XSD}double> .\n'
    assert fluree.serialize(fluree.parse(text, "ntriples"), "ntriples") == text


def test_turtle_and_n_triples_hold_the_default_graph_only():
    quads = [Quad(ex("a"), ex("p"), ex("b"), ex("g"))]
    for format in ("turtle", "ntriples"):
        with pytest.raises(InvalidRequestError, match="named graphs"):
            fluree.serialize(quads, format)


def test_tuples_serialize_as_quads():
    text = fluree.serialize([(EX + "a", EX + "p", "x"), (EX + "a", EX + "p", 1, EX + "g")], "nquads")
    assert set(fluree.parse(text, "nquads")) == {
        Quad(ex("a"), ex("p"), "x"),
        Quad(ex("a"), ex("p"), 1, ex("g")),
    }
    with pytest.raises(TypeError):
        fluree.serialize([(EX + "a", EX + "p")], "nquads")


def test_a_path_gives_the_format_and_text_needs_one(tmp_path):
    path = tmp_path / "data.nq"
    path.write_text(f"<{EX}a> <{EX}p> <{EX}b> <{EX}g> .\n")
    assert fluree.parse(path) == [Quad(ex("a"), ex("p"), ex("b"), ex("g"))]
    with pytest.raises(InvalidRequestError, match="format="):
        fluree.parse(f"<{EX}a> <{EX}p> <{EX}b> .")
    with pytest.raises(InvalidRequestError, match="format of"):
        fluree.parse(tmp_path / "data.rdf")


def test_an_error_names_its_line_and_column():
    with pytest.raises(InvalidRequestError, match="line 2, column 1"):
        fluree.parse(f"<{EX}a> <{EX}p> <{EX}b> .\n<a> <{EX}p> <{EX}b> .\n", "ntriples")


def test_base_resolves_relative_iris():
    quads = fluree.parse("<a> <p> <b> .", "turtle", base=EX)
    assert quads == [Quad(ex("a"), ex("p"), ex("b"))]


def test_quads_write_to_a_ledger():
    quads = fluree.parse(
        PREFIXES
        + """ex:alice ex:knows ex:bob {| ex:since 2020 |} .
             ex:claim <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> <<( ex:carol ex:age 30 )>> .
             ex:g { ex:dave ex:likes ex:eve }""",
        "trig",
    )
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("quads")
        ledger.insert(quads)
        assert ledger.query(PREFIXES + "ASK { ex:carol ex:age 30 }") is False
        claimed = ledger.query(PREFIXES + f"SELECT ?t WHERE {{ ex:claim <{REIFIES}> ?t }}")
        assert claimed.single().t == Triple(ex("carol"), ex("age"), 30)
        stored = fluree.parse(ledger.export(format="nquads", all_graphs=True), "nquads")
        assert len(stored) == len(quads)
        assert Quad(ex("dave"), ex("likes"), ex("eve"), ex("g")) in stored
        with pytest.raises(InvalidRequestError, match="quads"):
            ledger.insert(quads, format="jsonld")


RDF_TYPE = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
G = ex("g")


def _reifies(reifier, *triple, graph=None):
    return Quad(reifier, IRI(REIFIES), Triple(*triple), graph)


LEDGER_ROUND_TRIPS = {
    "annotation in a named graph": [
        Quad(ex("a"), ex("knows"), ex("b"), G),
        _reifies(BlankNode("r"), ex("a"), ex("knows"), ex("b"), graph=G),
        Quad(BlankNode("r"), ex("since"), 2020, G),
    ],
    "unasserted reification in a named graph": [
        _reifies(ex("claim"), ex("c"), ex("age"), 30, graph=G),
        Quad(ex("claim"), ex("by"), ex("d"), G),
    ],
    "annotated language-tagged literal": [
        Quad(ex("a"), ex("name"), LangString("Al", "en"), G),
        _reifies(ex("r"), ex("a"), ex("name"), LangString("Al", "en"), graph=G),
    ],
    "two reifiers of one triple": [
        Quad(ex("a"), ex("knows"), ex("b")),
        _reifies(ex("r1"), ex("a"), ex("knows"), ex("b")),
        _reifies(ex("r2"), ex("a"), ex("knows"), ex("b")),
    ],
    "annotated rdf:type": [
        Quad(ex("a"), RDF_TYPE, ex("Person")),
        _reifies(ex("r"), ex("a"), RDF_TYPE, ex("Person")),
    ],
    "nested triple terms in a named graph": [
        Quad(ex("doc"), ex("says"), Triple(ex("c"), ex("p"), Triple(ex("e"), ex("q"), "x")), G),
    ],
    "strings that need escaping": [
        Quad(ex("s"), ex("v"), 'quote " backslash \\ newline \n tab \t return \r', G),
        Quad(ex("s"), ex("w"), "'''single''' and \"\"\"double\"\"\" triples, trailing \\"),
        Quad(ex("s"), ex("u"), "é 𝄞 \u0000 \u007f"),
    ],
    "literals": [
        Quad(ex("s"), ex("dir"), LangString("مرحبا", "ar--rtl")),
        Quad(ex("s"), ex("dec"), Decimal("1.50")),
        Quad(ex("s"), ex("big"), 123456789012345678901234567890),
        Quad(ex("s"), ex("dbl"), 0.1),
        Quad(ex("s"), ex("custom"), Literal("x y", EX + "dt")),
        Quad(ex("s"), ex("bool"), False),
    ],
    "non-ASCII IRIs": [Quad(IRI(EX + "é"), ex("p"), IRI(EX + "𝄞"), IRI(EX + "ü"))],
    "blank nodes in a named graph": [
        Quad(BlankNode("x"), ex("p"), BlankNode("y"), G),
        Quad(BlankNode("y"), ex("q"), 1, G),
    ],
    "one triple in two graphs": [Quad(ex("a"), ex("p"), ex("b")), Quad(ex("a"), ex("p"), ex("b"), G)],
}


def _shape(quads):
    """The quads with every blank node alike, for comparing across the
    relabeling a ledger does."""

    def term(t):
        if isinstance(t, BlankNode):
            return "_:"
        if isinstance(t, Triple):
            return ("<<", term(t.subject), term(t.predicate), term(t.object))
        return (type(t).__name__, t)

    return sorted(repr(tuple(term(t) for t in q)) for q in quads)


@pytest.mark.parametrize("quads", LEDGER_ROUND_TRIPS.values(), ids=LEDGER_ROUND_TRIPS.keys())
def test_quads_come_back_out_of_a_ledger_as_they_went_in(quads):
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("quads")
        ledger.insert(quads)
        stored = fluree.parse(ledger.export(format="nquads", all_graphs=True), "nquads")
        assert _shape(stored) == _shape(quads)


def test_a_ledger_refuses_a_blank_node_graph_name():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("quads")
        with pytest.raises(InvalidRequestError, match="IRIs, not blank nodes"):
            ledger.insert([Quad(ex("a"), ex("p"), ex("b"), BlankNode("g"))])


def test_a_quad_takes_plain_strings_as_iris():
    quad = Quad(EX + "s", EX + "p", "text", EX + "g")
    assert type(quad.subject) is IRI and type(quad.predicate) is IRI and type(quad.graph) is IRI
    assert type(quad.object) is str
    subject, predicate, value, graph = quad
    assert (subject, value) == (ex("s"), "text")
    assert Quad(EX + "s", EX + "p", 1).graph is None
    assert pickle.loads(pickle.dumps(quad)) == quad and hash(quad) == hash(Quad(*quad))
    for args in [(1, EX + "p", 1), (EX + "s", BlankNode("p"), 1), (EX + "s", EX + "p", 1, 5)]:
        with pytest.raises(TypeError):
            Quad(*args)
