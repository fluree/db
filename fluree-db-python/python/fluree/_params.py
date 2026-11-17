"""Query parameters: the values bound to ``$name`` in Cypher and to
``?name`` / ``$name`` in SPARQL, as each engine takes them."""

from __future__ import annotations

import datetime as _dt
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from fluree._graph import Node
from fluree._terms import Vector
from fluree.errors import InvalidRequestError


def _params(parameters: Mapping[str, Any] | None, kwparameters: dict[str, Any]) -> dict[str, Any] | None:
    """``parameters`` with the keyword arguments over them; ``None`` when empty."""
    return {**(parameters or {}), **kwparameters} or None


def _cypher_params(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    return {k: _cypher(v) for k, v in params.items()} if params else None


def _cypher(value: Any) -> Any:
    if isinstance(value, Node):
        return value.element_id
    if _is_ndarray(value):
        return value.tolist()
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _cypher(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_cypher(v) for v in value]
    return value


def _sparql_params(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Each value as the native layer converts it: an RDF term keeps what it
    is, and a Python value takes the datatype it reads back as, exactly as a
    property value written with ``insert`` does."""
    return {k: _sparql(k, v) for k, v in params.items()} if params else None


def _sparql(name: str, value: Any) -> Any:
    if isinstance(value, Node):
        return value
    if isinstance(value, (list, set, frozenset)) or (
        isinstance(value, tuple) and not isinstance(value, Vector)
    ):
        raise InvalidRequestError(f"parameter {name!r}: a {type(value).__name__} is not an RDF term")
    if isinstance(value, Mapping):
        # A JSON-LD term, as {"@id": ...} or {"@value": ..., "@type": ...}.
        return dict(value)
    return value


def _is_ndarray(value: Any) -> bool:
    kind = type(value)
    return kind.__name__ == "ndarray" and kind.__module__ == "numpy"
