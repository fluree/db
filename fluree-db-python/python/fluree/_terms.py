"""RDF terms as Python values.

Literals become the Python type their datatype names (``int``, ``float``,
``Decimal``, ``datetime``, ...). Anything without a lossless Python type stays
a :class:`Literal` carrying its lexical form and datatype, so no information
is dropped. IRIs, blank nodes and language-tagged strings are ``str``
subclasses: they print, slice and pass as their text, but compare as RDF
terms do, so an IRI equals only the same IRI, never the plain string (a
literal) with its text. An RDF 1.2 triple term is a :class:`Triple`, and a
statement in a dataset a :class:`Quad`.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterator, TypeVar

XSD = "http://www.w3.org/2001/XMLSchema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
EMBEDDING_VECTOR = "https://ns.flur.ee/db#embeddingVector"

_T = TypeVar("_T")


# An absolute IRI: a scheme, then no character an IRI cannot hold.
_ABSOLUTE_IRI = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:[^\x00-\x20<>\"{}|^`\\]*\Z")


def _check_iri(value: str, role: str) -> str:
    if not _ABSOLUTE_IRI.match(value):
        raise ValueError(f"{role} is an absolute IRI, not {value!r}")
    return value


class IRI(str):
    """An absolute IRI. It is equal only to an ``IRI`` with the same text,
    not to a plain ``str``, which is a string literal. Constructing one
    checks it: ``IRI("not an iri")`` raises ``ValueError``."""

    __slots__ = ()

    def __new__(cls, value: str) -> IRI:
        if not isinstance(value, str):
            raise TypeError(f"an IRI is made from a str, not a {type(value).__name__}")
        return super().__new__(cls, _check_iri(value, "an IRI"))

    def __repr__(self) -> str:
        return f"IRI({str.__repr__(self)})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, IRI):
            return str.__eq__(self, other)
        return NotImplemented if not isinstance(other, str) else False

    def __ne__(self, other: object) -> bool:
        equal = self.__eq__(other)
        return equal if equal is NotImplemented else not equal

    def __hash__(self) -> int:
        return hash((IRI, str.__str__(self)))

    def __reduce__(self) -> tuple[Any, ...]:
        return (trusted_iri, (str.__str__(self),))


def trusted_iri(text: str) -> IRI:
    """An IRI the engine produced, made without checking it: a ledger can
    hold IRIs written before checks were made, and reading them back must
    not fail."""
    return str.__new__(IRI, text)


class BlankNode(str):
    """A blank node, by its label (without the ``_:`` prefix). It is equal
    only to a ``BlankNode`` with the same label."""

    __slots__ = ()

    def __repr__(self) -> str:
        return f"BlankNode({str.__repr__(self)})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BlankNode):
            return str.__eq__(self, other)
        return NotImplemented if not isinstance(other, str) else False

    def __ne__(self, other: object) -> bool:
        equal = self.__eq__(other)
        return equal if equal is NotImplemented else not equal

    def __hash__(self) -> int:
        return hash((BlankNode, str.__str__(self)))


class LangString(str):
    """A language-tagged string: a ``str`` with a ``language`` tag and, in
    RDF 1.2, a base ``direction`` (``"ltr"``, ``"rtl"`` or ``None``).

    It is equal only to a ``LangString`` with the same text, the same tag
    (compared case-insensitively, as BCP 47 tags are) and the same
    direction; never to a plain ``str``, a string with no tag. A tag in
    RDF 1.2's form, ``"ar--rtl"``, is taken apart: ``LangString("x",
    "ar--rtl")`` is ``LangString("x", "ar", "rtl")``."""

    __slots__ = ("language", "direction")
    language: str
    direction: str | None

    def __new__(cls, value: str, language: str, direction: str | None = None) -> LangString:
        if direction is None:
            base, _, suffix = language.rpartition("--")
            if base and suffix in ("ltr", "rtl"):
                language, direction = base, suffix
        elif direction not in ("ltr", "rtl"):
            raise ValueError(f"a base direction is 'ltr' or 'rtl', not {direction!r}")
        s = super().__new__(cls, value)
        s.language = language
        s.direction = direction
        return s

    def __repr__(self) -> str:
        direction = "" if self.direction is None else f", direction={self.direction!r}"
        return f"LangString({str.__repr__(self)}, language={self.language!r}{direction})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LangString):
            return (
                str.__eq__(self, other)
                and self.language.lower() == other.language.lower()
                and self.direction == other.direction
            )
        return NotImplemented if not isinstance(other, str) else False

    def __ne__(self, other: object) -> bool:
        equal = self.__eq__(other)
        return equal if equal is NotImplemented else not equal

    def __hash__(self) -> int:
        return hash((LangString, str.__str__(self), self.language.lower(), self.direction))

    def __reduce__(self) -> tuple[Any, ...]:
        return (LangString, (str.__str__(self), self.language, self.direction))


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
    """A literal with no lossless Python type, as lexical form and datatype.
    The datatype is an absolute IRI; constructing one checks it."""

    value: str
    datatype: str

    def __post_init__(self) -> None:
        _check_iri(self.datatype, "a literal's datatype")

    def __str__(self) -> str:
        return self.value


def _object_key(value: Any) -> tuple[Any, ...]:
    """What tells one object position value from another as RDF terms do:
    ``1``, ``True``, ``1.0`` and ``Decimal(1)`` are four literals, though
    Python finds them equal. A JSON value (``rdf:JSON``) is keyed by its
    canonical text, as a ``dict`` or ``list`` cannot be hashed."""
    if isinstance(value, (dict, list)):
        return (type(value), json.dumps(value, sort_keys=True, separators=(",", ":")))
    return (type(value), value)


@dataclass(frozen=True, slots=True, eq=False)
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Triple):
            return NotImplemented
        return (self.subject, self.predicate, _object_key(self.object)) == (
            other.subject,
            other.predicate,
            _object_key(other.object),
        )

    def __hash__(self) -> int:
        return hash((self.subject, self.predicate, _object_key(self.object)))

    def __str__(self) -> str:
        """The triple term in N-Triples: ``<<( s p o )>>``."""
        return ntriples(self)


@dataclass(frozen=True, slots=True, eq=False)
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Quad):
            return NotImplemented
        return (self.subject, self.predicate, _object_key(self.object), self.graph) == (
            other.subject,
            other.predicate,
            _object_key(other.object),
            other.graph,
        )

    def __hash__(self) -> int:
        return hash((self.subject, self.predicate, _object_key(self.object), self.graph))

    def __str__(self) -> str:
        """The statement as an N-Quads line, without its newline."""
        return ntriples(self)


def ntriples(value: Any) -> str:
    """A term, triple or quad in N-Triples syntax: ``<http://e/x>``,
    ``_:b0``, ``"chat"@fr``, ``"1"^^<http://www.w3.org/2001/XMLSchema#integer>``,
    ``<<( s p o )>>`` for a :class:`Triple`, and a whole ``s p o g .``
    N-Quads line for a :class:`Quad`. A plain ``str`` is a string literal.

    The text is what :func:`serialize` writes, so it compares and sorts
    terms as their RDF syntax does."""
    from fluree import _fluree

    if isinstance(value, Quad):
        parts = [value.subject, value.predicate, value.object]
        if value.graph is not None:
            parts.append(value.graph)
        return " ".join(_fluree.term_ntriples(part) for part in parts) + " ."
    return _fluree.term_ntriples(value)


def _unchecked(cls: type[_T], fields: tuple[str, ...]) -> Callable[..., _T]:
    """A constructor for ``cls`` that sets ``fields`` without checking them,
    three times faster than the class: for terms the readers produce, which
    are already of the right kinds."""
    slots = tuple(cls.__dict__[name].__set__ for name in fields)

    def make(*values: Any) -> _T:
        made = cls.__new__(cls)
        for slot, value in zip(slots, values):
            slot(made, value)
        return made

    return make


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
    engine writes at most six digits, without trailing zeros, so the fraction
    is padded to six here."""
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
    # The engine returns a date with no offset (it discards one at parse), so
    # `date.fromisoformat` takes every date it returns. One with an offset
    # would be rejected and go to the Literal fallback.
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
        return trusted_iri(cell[1])
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
    return trusted_literal(lexical, datatype)


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
    return trusted_literal(lexical, datatype)


trusted_quad = _unchecked(Quad, ("subject", "predicate", "object", "graph"))
trusted_triple = _unchecked(Triple, ("subject", "predicate", "object"))
trusted_literal = _unchecked(Literal, ("value", "datatype"))
