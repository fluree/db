"""Rows — a pandas or polars DataFrame, or dicts — as a JSON-LD document of
one node per row."""

from __future__ import annotations

import math
import string
from collections.abc import Iterable, Iterator, Mapping
from typing import Any
from urllib.parse import quote

from fluree.errors import InvalidRequestError

# The characters a value keeps unescaped in an IRI: RFC 3986 unreserved, as
# R2RML's IRI-safe templates leave them.
_IRI_SAFE = "-._~"


def _records(rows: Any) -> Iterator[Mapping[str, Any]]:
    kind = type(rows)
    module = kind.__module__.split(".")[0]
    if module == "pandas" and kind.__name__ == "DataFrame":
        return iter(rows.to_dict("records"))
    if module == "polars" and kind.__name__ == "DataFrame":
        return rows.iter_rows(named=True)
    if isinstance(rows, Mapping):
        raise TypeError("rows are an iterable of dicts or a DataFrame; wrap a single row in a list")
    if isinstance(rows, Iterable):
        return iter(rows)
    raise TypeError(f"rows are a pandas or polars DataFrame or an iterable of dicts, not {kind.__name__}")


def _missing(value: Any) -> bool:
    """A null in any of the forms DataFrames use: None, NaN, NaT, NA."""
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    return type(value).__name__ in ("NAType", "NaTType")


def _native(value: Any) -> Any:
    """A numpy scalar as the Python value it holds."""
    if type(value).__module__ == "numpy" and hasattr(value, "item"):
        return value.item()
    return value


class _IriTemplate:
    """``"ex:person/{id}"``: a row's columns, IRI-escaped, in an IRI."""

    def __init__(self, template: str) -> None:
        self.template = template
        self.columns = [name for _, name, _, _ in string.Formatter().parse(template) if name]

    def expand(self, row: Mapping[str, Any]) -> str | None:
        """The IRI, or ``None`` when a column it names is missing."""
        values = {}
        for column in self.columns:
            if column not in row:
                raise InvalidRequestError(f"IRI template {self.template!r} names column {column!r}, which rows lack")
            value = _native(row[column])
            if _missing(value):
                return None
            values[column] = quote(str(value), safe=_IRI_SAFE)
        return self.template.format_map(values)


def _rows_document(
    rows: Any,
    *,
    id: str | None,
    type: str | list[str] | None,
    vocab: str | None,
    columns: Mapping[str, str | None] | None,
    refs: Mapping[str, str] | None,
    context: Any,
) -> dict[str, Any]:
    subject = _IriTemplate(id) if id is not None else None
    references = {prop: _IriTemplate(t) for prop, t in (refs or {}).items()}
    renamed = dict(columns or {})
    nodes = []
    for number, row in enumerate(_records(rows)):
        node: dict[str, Any] = {}
        if subject is not None:
            iri = subject.expand(row)
            if iri is None:
                raise InvalidRequestError(f"row {number} has no value for the id template {id!r}")
            node["@id"] = iri
        if type is not None:
            node["@type"] = type
        for column, value in row.items():
            prop = renamed.get(column, column)
            if prop is None or column in references:
                continue
            value = _native(value)
            if _missing(value):
                continue
            node[prop if vocab is None or column in renamed else vocab + prop] = value
        for prop, template in references.items():
            target = template.expand(row)
            if target is not None:
                node[prop if vocab is None else vocab + prop] = {"@id": target}
        nodes.append(node)
    return {"@context": context or {}, "@graph": nodes}
