"""RDF terms as Python values.

Literals become the Python type their datatype names (``int``, ``float``,
``Decimal``, ``datetime``, ...). Anything without a lossless Python type stays
a :class:`Literal` carrying its lexical form and datatype, so no information
is dropped. IRIs, blank nodes and language-tagged strings are ``str``
subclasses: they print and compare like strings but keep what they are.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

XSD = "http://www.w3.org/2001/XMLSchema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
EMBEDDING_VECTOR = "https://ns.flur.ee/db#embeddingVector"


class IRI(str):
    """An IRI. Compares equal to the same IRI as a plain ``str``."""

    __slots__ = ()

    def __repr__(self) -> str:
        return f"IRI({str.__repr__(self)})"


class BlankNode(str):
    """A blank node, by its label (without the ``_:`` prefix)."""

    __slots__ = ()

    def __repr__(self) -> str:
        return f"BlankNode({str.__repr__(self)})"


class LangString(str):
    """A language-tagged string: a ``str`` with a ``language`` tag.

    It compares like a string: equal to a plain ``str`` with the same text,
    and to another ``LangString`` with the same text and the same tag
    (compared case-insensitively, as BCP 47 tags are)."""

    __slots__ = ("language",)
    language: str

    def __new__(cls, value: str, language: str) -> LangString:
        s = super().__new__(cls, value)
        s.language = language
        return s

    def __repr__(self) -> str:
        return f"LangString({str.__repr__(self)}, language={self.language!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LangString):
            return str.__eq__(self, other) and self.language.lower() == other.language.lower()
        return str.__eq__(self, other)

    def __ne__(self, other: object) -> bool:
        equal = self.__eq__(other)
        return equal if equal is NotImplemented else not equal

    def __hash__(self) -> int:
        return str.__hash__(self)

    def __reduce__(self) -> tuple[Any, ...]:
        return (LangString, (str(self), self.language))


class Vector(tuple):  # type: ignore[type-arg]
    """An embedding vector: a tuple of floats, stored as ``f:embeddingVector``.

    Build one from any sequence of numbers or a numpy array. Insert it as a
    property value, pass it as a query parameter, and read it back from
    queries; ``numpy.asarray(vector)`` gives a ``float32`` array. Values are
    stored at ``float32`` precision. A one-dimensional numpy array is taken as
    a vector wherever a ``Vector`` is.
    """

    __slots__ = ()

    def __new__(cls, values: Any = ()) -> Vector:
        return super().__new__(cls, (float(v) for v in values))

    def __repr__(self) -> str:
        if len(self) <= 8:
            return f"Vector({list(self)!r})"
        head = ", ".join(repr(v) for v in self[:4])
        return f"Vector([{head}, ...], dims={len(self)})"

    def __array__(self, dtype: Any = None, copy: Any = None) -> Any:
        import numpy

        return numpy.array(tuple(self), dtype=dtype or numpy.float32)

    def __reduce__(self) -> tuple[Any, ...]:
        return (Vector, (tuple(self),))


def _vector(lexical: str) -> Vector:
    return Vector(json.loads(lexical))


@dataclass(frozen=True, slots=True)
class Literal:
    """A literal with no lossless Python type, as lexical form and datatype."""

    value: str
    datatype: str

    def __str__(self) -> str:
        return self.value


def _boolean(lexical: str) -> bool:
    if lexical in ("true", "1"):
        return True
    if lexical in ("false", "0"):
        return False
    raise ValueError(lexical)


def _datetime(lexical: str) -> _dt.datetime:
    # fromisoformat only accepts a "Z" suffix from Python 3.11.
    if lexical.endswith("Z"):
        lexical = lexical[:-1] + "+00:00"
    return _dt.datetime.fromisoformat(lexical)


def _time(lexical: str) -> _dt.time:
    if lexical.endswith("Z"):
        lexical = lexical[:-1] + "+00:00"
    return _dt.time.fromisoformat(lexical)


def _decimal(lexical: str) -> Decimal:
    try:
        return Decimal(lexical)
    except InvalidOperation:
        raise ValueError(lexical) from None


_INTEGER_TYPES = (
    "integer",
    "long",
    "int",
    "short",
    "byte",
    "nonNegativeInteger",
    "nonPositiveInteger",
    "positiveInteger",
    "negativeInteger",
    "unsignedLong",
    "unsignedInt",
    "unsignedShort",
    "unsignedByte",
)

_CONVERTERS: dict[str, Callable[[str], Any]] = {
    XSD + "string": str,
    XSD + "boolean": _boolean,
    XSD + "decimal": _decimal,
    XSD + "double": float,
    XSD + "float": float,
    XSD + "dateTime": _datetime,
    # A date with a timezone has no lossless `datetime.date` form, so
    # `date.fromisoformat` rejecting one sends it to the Literal fallback.
    XSD + "date": _dt.date.fromisoformat,
    XSD + "time": _time,
    RDF + "JSON": json.loads,
    EMBEDDING_VECTOR: _vector,
    **{XSD + name: int for name in _INTEGER_TYPES},
}


def to_python(cell: tuple[Any, ...] | None) -> Any:
    """Convert a native term tuple to its Python value; ``None`` stays unbound."""
    if cell is None:
        return None
    kind = cell[0]
    if kind == "iri":
        return IRI(cell[1])
    if kind == "bnode":
        return BlankNode(cell[1])
    _, lexical, datatype, language = cell
    if language is not None:
        return LangString(lexical, language)
    convert = _CONVERTERS.get(datatype)
    if convert is not None:
        try:
            return convert(lexical)
        except ValueError:
            pass
    return Literal(lexical, datatype)
