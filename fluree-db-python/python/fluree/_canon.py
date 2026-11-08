"""RDF Dataset Canonicalization (RDFC-1.0) of quads."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from typing import Literal as _Literal

from fluree import _fluree as native
from fluree._terms import Quad


def canonicalize(
    quads: Iterable[Quad | tuple[Any, ...]],
    *,
    hash: _Literal["sha256", "sha384"] = "sha256",
) -> str:
    """The canonical N-Quads of ``quads`` by RDF Dataset Canonicalization
    (`RDFC-1.0 <https://www.w3.org/TR/rdf-canon/>`_): blank nodes labeled
    ``_:c14n0``, ``_:c14n1``, ... from the statements alone, duplicates
    dropped, and lines in code point order. Any two sets of quads that hold
    the same statements, whatever their blank node labels and order, give the
    same text, so it can be compared, hashed or signed.

    Each item is a :class:`Quad`, or a ``(subject, predicate, object)`` or
    ``(subject, predicate, object, graph)`` tuple. ``hash`` is the algorithm
    the canonicalization hashes with. A dataset whose blank nodes take too
    much work to tell apart (a "poison" graph) raises
    :class:`~fluree.errors.InvalidRequestError`.
    """
    items = [q if type(q) is Quad else Quad(*q) for q in quads]
    return native.canonicalize_rdf(items, hash)
