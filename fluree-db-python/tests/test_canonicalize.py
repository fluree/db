"""RDF Dataset Canonicalization (RDFC-1.0): fluree.canonicalize()."""

import pytest

import fluree
from fluree import InvalidRequestError

EX = "http://example.org/"
PREFIXES = f"PREFIX ex: <{EX}>\n"


def test_the_same_statements_canonicalize_alike():
    one = fluree.parse(PREFIXES + "_:x ex:p _:y . _:y ex:q 'v' .", "turtle")
    two = fluree.parse(PREFIXES + "_:b ex:q 'v' . _:a ex:p _:b . _:a ex:p _:b .", "turtle")
    assert fluree.canonicalize(one) == fluree.canonicalize(two)
    assert fluree.canonicalize(one) == f'_:c14n0 <{EX}q> "v" .\n_:c14n1 <{EX}p> _:c14n0 .\n'


def test_different_statements_canonicalize_differently():
    one = fluree.parse(PREFIXES + "_:x ex:p _:y . _:y ex:q 'v' .", "turtle")
    other = fluree.parse(PREFIXES + "_:x ex:p _:y . _:y ex:q 'w' .", "turtle")
    assert fluree.canonicalize(one) != fluree.canonicalize(other)


def test_tuples_named_graphs_and_another_hash():
    quads = [(fluree.BlankNode("a"), EX + "p", "v", EX + "g")]
    assert fluree.canonicalize(quads) == f'_:c14n0 <{EX}p> "v" <{EX}g> .\n'
    assert fluree.canonicalize(quads, hash="sha384") == fluree.canonicalize(quads)
    with pytest.raises(InvalidRequestError, match="sha256"):
        fluree.canonicalize(quads, hash="md5")  # type: ignore[arg-type]


def test_a_poison_graph_is_refused():
    # Ten blank nodes, each linked to every other: nothing tells them apart.
    clique = PREFIXES + "".join(f"_:n{i} ex:p _:n{j} . " for i in range(10) for j in range(10) if i != j)
    with pytest.raises(InvalidRequestError, match="too complex"):
        fluree.canonicalize(fluree.parse(clique, "turtle"))


def test_the_limit_can_be_lowered():
    ring = PREFIXES + "".join(f"_:n{i} ex:p _:n{(i + 1) % 6} . " for i in range(6))
    quads = fluree.parse(ring, "turtle")
    assert fluree.canonicalize(quads).count("\n") == 6
    with pytest.raises(InvalidRequestError, match="more than 2 deep comparisons"):
        fluree.canonicalize(quads, max_deep_calls=2)


def test_lexical_literals_keep_their_spelling():
    doc = '<http://example.org/s> <http://example.org/p> "01"^^<http://www.w3.org/2001/XMLSchema#long> .'
    assert '"01"^^<http://www.w3.org/2001/XMLSchema#long>' in fluree.canonicalize(
        fluree.parse(doc, "ntriples", literals="lexical")
    )
