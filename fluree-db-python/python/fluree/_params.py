"""Query parameters: the values bound to ``$name`` in Cypher and to
``?name`` / ``$name`` in SPARQL, as each engine takes them."""

from __future__ import annotations

import datetime as _dt
import json
import math
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from fluree._graph import Node
from fluree._terms import EMBEDDING_VECTOR, XSD, IRI, BlankNode, LangString, Literal, Vector
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
    """Each value as the JSON-LD term the engine substitutes: an RDF term
    keeps what it is, and a Python value takes the datatype it reads back as."""
    return {k: _sparql(k, v) for k, v in params.items()} if params else None


def _sparql(name: str, value: Any) -> Any:
    if isinstance(value, Node):
        value = value.element_id
    if _is_ndarray(value) and value.ndim != 1:
        raise InvalidRequestError(f"parameter {name!r}: a vector is a one-dimensional array")
    if isinstance(value, Vector) or _is_ndarray(value):
        return {"@value": json.dumps(list(Vector(value))), "@type": EMBEDDING_VECTOR}
    if isinstance(value, BlankNode):
        return {"@id": f"_:{value}"}
    if isinstance(value, IRI):
        return {"@id": str(value)}
    if isinstance(value, LangString):
        return {"@value": str(value), "@language": value.language}
    if isinstance(value, Literal):
        return {"@value": value.value, "@type": value.datatype}
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        # JSON numbers past 64 bits lose precision on the way in.
        if -(2**63) <= value < 2**64:
            return value
        return {"@value": str(value), "@type": XSD + "integer"}
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        lexical = "NaN" if math.isnan(value) else ("INF" if value > 0 else "-INF")
        return {"@value": lexical, "@type": XSD + "double"}
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise InvalidRequestError(f"parameter {name!r}: {value} is not an xsd:decimal")
        return {"@value": format(value, "f"), "@type": XSD + "decimal"}
    if isinstance(value, _dt.datetime):
        return {"@value": value.isoformat(), "@type": XSD + "dateTime"}
    if isinstance(value, _dt.date):
        return {"@value": value.isoformat(), "@type": XSD + "date"}
    if isinstance(value, _dt.time):
        return {"@value": value.isoformat(), "@type": XSD + "time"}
    if isinstance(value, str):
        return str(value)
    if isinstance(value, Mapping):
        # A JSON-LD term, as {"@id": ...} or {"@value": ..., "@type": ...}.
        return dict(value)
    raise InvalidRequestError(f"parameter {name!r}: a {type(value).__name__} is not an RDF term")


def _is_ndarray(value: Any) -> bool:
    kind = type(value)
    return kind.__name__ == "ndarray" and kind.__module__ == "numpy"
