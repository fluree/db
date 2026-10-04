"""Cypher results, shaped like the Neo4j Python driver's: records indexable by
key or position, and graph values as nodes, relationships, and paths."""

from __future__ import annotations

import datetime as _dt
import warnings
from collections.abc import Iterator, Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any, overload

from fluree._records import Commit, _jsonld_term
from fluree._terms import _datetime
from fluree.errors import InvalidRequestError

if TYPE_CHECKING:
    import pandas

    from fluree import _fluree


class Node(Mapping[str, Any]):
    """A node: its properties (read like a dict), its ``labels`` — the
    node's classes — and ``element_id``, its IRI."""

    __slots__ = ("_properties", "element_id", "labels")

    def __init__(self, element_id: str, labels: frozenset[str], properties: dict[str, Any]) -> None:
        self.element_id = element_id
        self.labels = labels
        self._properties = properties

    def __getitem__(self, key: str) -> Any:
        return self._properties[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._properties)

    def __len__(self) -> int:
        return len(self._properties)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Node) and other.element_id == self.element_id

    def __hash__(self) -> int:
        return hash(self.element_id)

    def __repr__(self) -> str:
        return f"<Node element_id={self.element_id!r} labels={set(self.labels) or '{}'} properties={self._properties!r}>"


class Relationship(Mapping[str, Any]):
    """A relationship: its properties (read like a dict), its ``type``, and
    the nodes it runs from and to. ``element_id`` is its IRI when it has one
    (every relationship a Cypher ``CREATE`` makes does).

    ``start_node`` and ``end_node`` carry their properties and labels when
    the same result returned those nodes; otherwise only their
    ``element_id``.
    """

    __slots__ = ("_properties", "element_id", "end_node", "start_node", "type")

    def __init__(
        self,
        element_id: str | None,
        type: str,
        start_node: Node,
        end_node: Node,
        properties: dict[str, Any],
    ) -> None:
        self.element_id = element_id
        self.type = type
        self.start_node = start_node
        self.end_node = end_node
        self._properties = properties

    @property
    def nodes(self) -> tuple[Node, Node]:
        return (self.start_node, self.end_node)

    def __getitem__(self, key: str) -> Any:
        return self._properties[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._properties)

    def __len__(self) -> int:
        return len(self._properties)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Relationship):
            return False
        if self.element_id is not None or other.element_id is not None:
            return self.element_id == other.element_id
        return (self.start_node, self.type, self.end_node) == (other.start_node, other.type, other.end_node)

    def __hash__(self) -> int:
        return hash(self.element_id or (self.start_node.element_id, self.type, self.end_node.element_id))

    def __repr__(self) -> str:
        return (
            f"<Relationship element_id={self.element_id!r} "
            f"nodes=({self.start_node.element_id!r}, {self.end_node.element_id!r}) "
            f"type={self.type!r} properties={self._properties!r}>"
        )


class Path:
    """A walk through the graph: ``nodes`` and the ``relationships``
    between them, in order. Iterating a path yields its relationships."""

    __slots__ = ("nodes", "relationships")

    def __init__(self, nodes: tuple[Node, ...], relationships: tuple[Relationship, ...]) -> None:
        self.nodes = nodes
        self.relationships = relationships

    @property
    def start_node(self) -> Node:
        return self.nodes[0]

    @property
    def end_node(self) -> Node:
        return self.nodes[-1]

    def __len__(self) -> int:
        return len(self.relationships)

    def __iter__(self) -> Iterator[Relationship]:
        return iter(self.relationships)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Path) and (self.nodes, self.relationships) == (other.nodes, other.relationships)

    def __hash__(self) -> int:
        return hash((self.nodes, self.relationships))

    def __repr__(self) -> str:
        return f"<Path start={self.start_node.element_id!r} end={self.end_node.element_id!r} size={len(self)}>"


class Record(tuple):  # type: ignore[type-arg]
    """One result row: a tuple of values that can also be read by column
    name — ``record["name"]``, ``record.get("name")``, ``record.data()``."""

    _keys: tuple[str, ...]

    def __new__(cls, values: Sequence[Any], keys: tuple[str, ...]) -> Record:
        record = super().__new__(cls, values)
        record._keys = keys
        return record

    @overload
    def __getitem__(self, key: int | str) -> Any: ...
    @overload
    def __getitem__(self, key: slice) -> tuple[Any, ...]: ...
    def __getitem__(self, key: int | str | slice) -> Any:
        if isinstance(key, str):
            try:
                return super().__getitem__(self._keys.index(key))
            except ValueError:
                raise KeyError(key) from None
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self[key] if key in self._keys else default

    def index(self, key: str | int) -> int:  # type: ignore[override]
        """The position of column ``key``."""
        if isinstance(key, int):
            if not 0 <= key < len(self):
                raise IndexError(key)
            return key
        try:
            return self._keys.index(key)
        except ValueError:
            raise KeyError(key) from None

    def keys(self) -> list[str]:
        return list(self._keys)

    def values(self, *keys: str | int) -> list[Any]:
        return [self[k] for k in keys] if keys else list(self)

    def items(self, *keys: str | int) -> list[tuple[str, Any]]:
        selected = [self.index(k) for k in keys] if keys else range(len(self))
        return [(self._keys[i], self[i]) for i in selected]

    def data(self, *keys: str | int) -> dict[str, Any]:
        """The record as a dict of plain values: nodes become their property
        dicts, relationships ``(start properties, type, end properties)``."""
        return {k: _plain(v) for k, v in self.items(*keys)}

    def __repr__(self) -> str:
        fields = " ".join(f"{k}={v!r}" for k, v in zip(self._keys, self))
        return f"<Record {fields}>"


class CypherResult(Sequence[Record]):
    """The records a Cypher statement returned, in order, and for a write,
    the :class:`Commit` it made (``commit``; ``None`` for a read)."""

    __slots__ = ("_keys", "_records", "commit")

    def __init__(self, keys: list[str], records: list[Record], commit: Commit | None) -> None:
        self._keys = keys
        self._records = records
        self.commit = commit

    @overload
    def __getitem__(self, index: int) -> Record: ...
    @overload
    def __getitem__(self, index: slice) -> list[Record]: ...
    def __getitem__(self, index: int | slice) -> Record | list[Record]:
        return self._records[index]

    def __len__(self) -> int:
        return len(self._records)

    def __repr__(self) -> str:
        return f"<CypherResult keys={self._keys!r} len={len(self)}>"

    def keys(self) -> list[str]:
        return list(self._keys)

    def single(self, strict: bool = False) -> Record | None:
        """The one record. With no records, ``None``; with more than one, the
        first, with a warning. ``strict`` raises instead of either."""
        if len(self._records) == 1:
            return self._records[0]
        if strict:
            raise InvalidRequestError(f"expected exactly one record, got {len(self._records)}")
        if not self._records:
            return None
        warnings.warn("expected a result with a single record, but it contains more", stacklevel=2)
        return self._records[0]

    def value(self, key: str | int = 0, default: Any = None) -> list[Any]:
        """One column's values."""
        return [r[key] if (isinstance(key, int) or key in r._keys) else default for r in self._records]

    def values(self, *keys: str | int) -> list[list[Any]]:
        return [r.values(*keys) for r in self._records]

    def data(self, *keys: str | int) -> list[dict[str, Any]]:
        """Every record as a dict of plain values; see :meth:`Record.data`."""
        return [r.data(*keys) for r in self._records]

    def to_df(self) -> pandas.DataFrame:
        """The records as a pandas DataFrame, one column per key; nodes and
        relationships are kept as objects."""
        import pandas

        return pandas.DataFrame.from_records(list(self._records), columns=self._keys)


class CypherTransaction:
    """Cypher statements committed together, all or nothing.

    Get one from :meth:`Ledger.cypher_transaction`. ``run`` executes a
    statement: writes stage in the transaction, reads see what it has
    staged, and nothing is visible on the ledger until :meth:`commit`. As a
    context manager it commits on a clean exit and rolls back on an
    exception. If another commit lands on the ledger first, :meth:`commit`
    raises :class:`ConflictError`; run the transaction again.
    """

    __slots__ = ("_native", "committed")

    def __init__(self, native: _fluree.CypherTransaction) -> None:
        self._native = native
        self.committed: Commit | None = None

    def __repr__(self) -> str:
        return f"<CypherTransaction {'open' if self._native.is_open else 'closed'}>"

    def __enter__(self) -> CypherTransaction:
        return self

    def __exit__(self, exc_type: object, *exc: object) -> None:
        if not self._native.is_open:
            return
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def run(self, query: str, parameters: Mapping[str, Any] | None = None, **kwparameters: Any) -> CypherResult:
        """Run a Cypher statement (or ``;`` script) in the transaction.
        Parameters (``$name``) come from ``parameters`` and keyword
        arguments."""
        table = self._native.run(query, _params(parameters, kwparameters))
        return _result(None, table)

    def commit(self) -> Commit:
        self.committed = Commit(**self._native.commit())
        return self.committed

    def rollback(self) -> None:
        self._native.rollback()


def _params(parameters: Mapping[str, Any] | None, kwparameters: dict[str, Any]) -> dict[str, Any] | None:
    merged = {**(parameters or {}), **kwparameters}
    return {k: _param(v) for k, v in merged.items()} or None


def _param(value: Any) -> Any:
    if isinstance(value, Node):
        return value.element_id
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _param(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_param(v) for v in value]
    return value


def _result(commit: dict[str, Any] | None, table: tuple[list[str], list[tuple[Any, ...]]]) -> CypherResult:
    columns, rows = table
    keys = tuple(columns)
    records = []
    for row in rows:
        nodes: dict[str, Node] = {}
        values = [_decode(cell, nodes) for cell in row]
        # A relationship's endpoints are the full nodes when the row has them.
        for value in values:
            _link(value, nodes)
        records.append(Record(values, keys))
    return CypherResult(list(columns), records, None if commit is None else Commit(**commit))


def _decode(cell: tuple[Any, ...], nodes: dict[str, Node]) -> Any:
    kind = cell[0]
    if kind == "value":
        return _jsonld_term(cell[1])
    if kind == "decimal":
        return Decimal(cell[1])
    if kind == "bigint":
        return int(cell[1])
    if kind == "temporal":
        _, which, iso = cell
        if which == "date":
            return _dt.date.fromisoformat(iso[:10])
        if which == "datetime":
            return _datetime(iso)
        return _dt.time.fromisoformat(iso.removesuffix("Z"))
    if kind == "list":
        return [_decode(c, nodes) for c in cell[1]]
    if kind == "map":
        return {k: _decode(v, nodes) for k, v in cell[1]}
    if kind == "node":
        _, iri, labels, props = cell
        node = Node(iri, frozenset(labels), {k: _decode(v, nodes) for k, v in props})
        nodes.setdefault(iri, node)
        return node
    if kind == "rel":
        _, element_id, type_, start, end, props = cell
        return Relationship(
            element_id,
            type_,
            Node(start, frozenset(), {}),
            Node(end, frozenset(), {}),
            {k: _decode(v, nodes) for k, v in props},
        )
    if kind == "path":
        _, raw_nodes, raw_rels, indices = cell
        path_nodes = [_decode(n, nodes) for n in raw_nodes]
        path_rels = [_decode(r, nodes) for r in raw_rels]
        walk_nodes = [path_nodes[0]]
        walk_rels = []
        for i in range(0, len(indices), 2):
            walk_rels.append(path_rels[abs(indices[i]) - 1])
            walk_nodes.append(path_nodes[indices[i + 1]])
        return Path(tuple(walk_nodes), tuple(walk_rels))
    raise InvalidRequestError(f"unknown Cypher value {kind!r}")


def _link(value: Any, nodes: dict[str, Node]) -> None:
    if isinstance(value, Relationship):
        value.start_node = nodes.get(value.start_node.element_id, value.start_node)
        value.end_node = nodes.get(value.end_node.element_id, value.end_node)
    elif isinstance(value, Path):
        for rel in value.relationships:
            _link(rel, nodes)
    elif isinstance(value, list):
        for item in value:
            _link(item, nodes)
    elif isinstance(value, dict):
        for item in value.values():
            _link(item, nodes)


def _plain(value: Any) -> Any:
    if isinstance(value, Node):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, Relationship):
        return (_plain(value.start_node), value.type, _plain(value.end_node))
    if isinstance(value, Path):
        out: list[Any] = [_plain(value.start_node)]
        for rel, node in zip(value.relationships, value.nodes[1:]):
            out += [rel.type, _plain(node)]
        return out
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    return value
