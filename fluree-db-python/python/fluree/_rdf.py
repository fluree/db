"""RDF documents to and from quads, with no ledger involved."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any, TypeGuard
from typing import Literal as _Literal

from fluree import _fluree as native
from fluree._terms import BlankNode, Quad, to_lexical, to_python, trusted_quad, trusted_triple
from fluree.errors import InvalidRequestError

RdfFormat = _Literal["turtle", "trig", "ntriples", "nquads", "jsonld"]

_SUFFIXES: dict[str, RdfFormat] = {
    ".ttl": "turtle",
    ".trig": "trig",
    ".nt": "ntriples",
    ".nq": "nquads",
    ".jsonld": "jsonld",
    ".json": "jsonld",
}


def parse(
    data: str | os.PathLike[str] | dict[str, Any] | list[Any],
    format: RdfFormat | None = None,
    *,
    base: str | None = None,
    literals: _Literal["value", "lexical"] = "value",
) -> list[Quad]:
    """The quads of an RDF document: Turtle, TriG, N-Triples, N-Quads or
    JSON-LD.

    ``data`` is the document's text, with its ``format`` given; a path,
    whose extension (``.ttl``, ``.trig``, ``.nt``, ``.nq``, ``.jsonld``,
    ``.json``) gives the format when ``format`` does not; or a JSON-LD
    document as a dict or list. Turtle, TriG and JSON-LD resolve relative
    IRIs against ``base``.

    RDF 1.2 is read whole: a triple term is a :class:`Triple`, and an
    annotation or reified triple is the quad ``(reifier, rdf:reifies,
    Triple(...))``, with the annotated triple as a quad of its own. In
    JSON-LD these are ``@annotation`` on a value, a node's ``@reifies``, and
    ``{"@id": {"@id": s, p: o}}``; a named graph is a node's ``@graph``.

    Literals are Python values, as query results are, so a number's
    spelling is not kept (``"01"`` reads as ``1``); with
    ``literals="lexical"`` every typed literal but a plain string is a
    :class:`Literal` with its lexical form, and :func:`serialize` writes it
    back as it was. Blank nodes keep the document's labels; an anonymous one
    (``[]``, a collection, an annotation) gets a fresh label. Malformed input
    raises :class:`InvalidRequestError` naming the line and column.
    """
    if isinstance(data, (dict, list)):
        if format not in (None, "jsonld"):
            raise InvalidRequestError(f"a dict or list is JSON-LD, not {format}")
        data, format = json.dumps(data), "jsonld"
    elif isinstance(data, os.PathLike):
        path = Path(data)
        if format is None:
            format = _SUFFIXES.get(path.suffix.lower())
            if format is None:
                raise InvalidRequestError(f"cannot tell the format of {path.name}; pass format=")
        data = path.read_text(encoding="utf-8")
    elif format is None:
        raise InvalidRequestError("pass format= (turtle, trig, ntriples, nquads or jsonld)")
    if literals not in ("value", "lexical"):
        raise InvalidRequestError(f"literals= is 'value' or 'lexical', not {literals!r}")
    literal = to_python if literals == "value" else to_lexical

    def value(term: Any) -> Any:
        if type(term) is not tuple:
            return term
        if term[0] == "triple":
            return trusted_triple(term[1], term[2], value(term[3]))
        return literal(term)

    return [trusted_quad(s, p, value(o), g) for s, p, o, g in native.parse_rdf(data, format, base)]


def serialize(
    quads: Iterable[Quad | tuple[Any, ...]],
    format: RdfFormat,
    *,
    prefixes: dict[str, str] | None = None,
) -> str:
    """Write quads as an RDF document: Turtle, TriG, N-Triples, N-Quads or
    JSON-LD.

    Each item is a :class:`Quad`, or a ``(subject, predicate, object)`` or
    ``(subject, predicate, object, graph)`` tuple. Turtle and N-Triples hold
    the default graph only; a quad in a named graph needs TriG, N-Quads or
    JSON-LD. Turtle and TriG declare ``prefixes``
    (``{"ex": "http://example.org/"}``) and write IRIs with them, and JSON-LD
    makes them its ``@context``. A quad ``(r, rdf:reifies, Triple(...))`` is
    written as an annotation where the format has one.
    """
    return native.serialize_rdf([q if type(q) is Quad else _quad(q) for q in quads], format, prefixes)


def _quad(item: Quad | tuple[Any, ...]) -> Quad:
    if isinstance(item, Quad):
        return item
    if isinstance(item, tuple) and len(item) in (3, 4):
        return Quad(*item)
    raise TypeError(f"a quad is a Quad or a 3- or 4-tuple, not {item!r}")


def ledger_trig(quads: list[Quad]) -> str:
    """``quads`` as the TriG a ledger write takes. A ledger names its graphs
    with IRIs, so a blank-node graph name is refused here, naming the quad."""
    for quad in quads:
        if isinstance(quad.graph, BlankNode):
            raise InvalidRequestError(f"a ledger's named graphs are IRIs, not blank nodes: {quad!r}")
    return serialize(quads, "trig")


def is_quads(data: Any) -> TypeGuard[list[Quad]]:
    """Whether ``data`` is a list of quads rather than JSON-LD."""
    return isinstance(data, list) and bool(data) and all(isinstance(q, Quad) for q in data)
