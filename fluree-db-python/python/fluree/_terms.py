"""RDF terms as Python values.

Literals become the Python type their datatype names (``int``, ``float``,
``Decimal``, ``datetime``, ...). Anything without a lossless Python type stays
a :class:`Literal` carrying its lexical form and datatype, so no information
is dropped. IRIs, blank nodes and language-tagged strings are ``str``
subclasses: they print and compare like strings but keep what they are. An
RDF 1.2 triple term is a :class:`Triple`, and a statement in a dataset a
:class:`Quad`.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterator

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


@dataclass(frozen=True, slots=True)
class Triple:
    """An RDF 1.2 triple term: a triple used as a value, as the object of
    ``rdf:reifies`` or of any other property, or inside another triple term.
    It unpacks as ``subject, predicate, object``.

    The subject is an :class:`IRI` or a :class:`BlankNode`, and the predicate
    an :class:`IRI`; a plain ``str`` in either place is taken as an IRI. The
    object is any value a property holds, another ``Triple`` included."""

    subject: IRI | BlankNode
    predicate: IRI
    object: Any

    def __post_init__(self) -> None:
        object.__setattr__(self, "subject", _node(self.subject, "triple term's subject"))
        object.__setattr__(self, "predicate", _iri(self.predicate, "triple term's predicate"))

    def __iter__(self) -> Iterator[Any]:
        return iter((self.subject, self.predicate, self.object))

    def __reduce__(self) -> tuple[Any, ...]:
        return (Triple, (self.subject, self.predicate, self.object))


@dataclass(frozen=True, slots=True)
class Quad:
    """A statement in a dataset: ``subject predicate object`` in ``graph``,
    or in the default graph when ``graph`` is ``None``. It unpacks as
    ``subject, predicate, object, graph``.

    The subject and graph are an :class:`IRI` or a :class:`BlankNode`, and
    the predicate an :class:`IRI`; a plain ``str`` in any of them is taken as
    an IRI. The object is any value a property holds. A claim about a triple
    is the quad ``(claim, rdf:reifies, Triple(...))``."""

    subject: IRI | BlankNode
    predicate: IRI
    object: Any
    graph: IRI | BlankNode | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "subject", _node(self.subject, "quad's subject"))
        object.__setattr__(self, "predicate", _iri(self.predicate, "quad's predicate"))
        if self.graph is not None:
            object.__setattr__(self, "graph", _node(self.graph, "quad's graph"))

    def __iter__(self) -> Iterator[Any]:
        return iter((self.subject, self.predicate, self.object, self.graph))

    def __reduce__(self) -> tuple[Any, ...]:
        return (Quad, (self.subject, self.predicate, self.object, self.graph))


def _node(value: Any, role: str) -> IRI | BlankNode:
    """``value`` as an IRI or blank node; a plain ``str`` is an IRI."""
    if isinstance(value, (IRI, BlankNode)):
        return value
    if type(value) is str:
        return IRI(value)
    raise TypeError(f"a {role} is an IRI or a blank node, not a {type(value).__name__}")


def _iri(value: Any, role: str) -> IRI:
    """``value`` as an IRI; a plain ``str`` is one."""
    if isinstance(value, IRI):
        return value
    if type(value) is str:
        return IRI(value)
    raise TypeError(f"a {role} is an IRI, not a {type(value).__name__}")


def _boolean(lexical: str) -> bool:
    if lexical in ("true", "1"):
        return True
    if lexical in ("false", "0"):
        return False
    raise ValueError(lexical)


_FRACTION = re.compile(r"\.(\d+)")


def _iso(lexical: str) -> str:
    """`lexical` in the form every supported Python's `fromisoformat` accepts.

    Before 3.11 it takes no "Z" suffix and only a 3- or 6-digit fraction; the
    engine writes up to 9 digits, cut here to microseconds as 3.11 does."""
    if lexical.endswith("Z"):
        lexical = lexical[:-1] + "+00:00"
    return _FRACTION.sub(lambda m: "." + m[1][:6].ljust(6, "0"), lexical, count=1)


def _datetime(lexical: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(_iso(lexical))


def _time(lexical: str) -> _dt.time:
    return _dt.time.fromisoformat(_iso(lexical))


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
    if kind == "triple":
        return Triple(to_python(cell[1]), to_python(cell[2]), to_python(cell[3]))
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


def to_lexical(cell: tuple[Any, ...]) -> Any:
    """Like :func:`to_python`, but a typed literal other than ``xsd:string``
    stays a :class:`Literal` with its lexical form, so ``"01"^^xsd:integer``
    is ``Literal("01", xsd:integer)`` rather than ``1``."""
    kind = cell[0]
    if kind == "triple":
        return Triple(to_lexical(cell[1]), to_lexical(cell[2]), to_lexical(cell[3]))
    if kind != "literal":
        return to_python(cell)
    _, lexical, datatype, language = cell
    if language is not None:
        return LangString(lexical, language)
    if datatype == XSD + "string":
        return lexical
    return Literal(lexical, datatype)
