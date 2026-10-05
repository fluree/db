"""Cypher: decoding typed result cells into Python values and graph objects,
and explicit Cypher transactions."""

from __future__ import annotations

import datetime as _dt
from collections.abc import Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from fluree._graph import Node, Path, Relationship
from fluree._records import Commit, _jsonld_term, _node
from fluree._results import Result, _record_type
from fluree._terms import _datetime
from fluree.errors import InvalidRequestError

if TYPE_CHECKING:
    from fluree import _fluree


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

    def run(self, query: str, parameters: Mapping[str, Any] | None = None, **kwparameters: Any) -> Result:
        """Run a Cypher statement (or ``;`` script) in the transaction.
        Parameters (``$name``) come from ``parameters`` and keyword
        arguments."""
        return _table(self._native.run(query, _params(parameters, kwparameters)))

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


def _table(table: tuple[list[str], list[tuple[Any, ...]]]) -> Result:
    """A :class:`Result` from native ``(columns, rows)`` of typed cells."""
    columns, rows = table
    record = _record_type(columns)
    records = []
    for row in rows:
        nodes: dict[Any, Node] = {}
        values = [_decode(cell, nodes) for cell in row]
        # A relationship's endpoints are the full nodes when the row has them.
        for value in values:
            _link(value, nodes)
        records.append(record(values))
    return Result(list(columns), records)


def _decode(cell: tuple[Any, ...], nodes: dict[Any, Node]) -> Any:
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
        node = Node(_node(iri), frozenset(labels), {k: _decode(v, nodes) for k, v in props})
        nodes.setdefault(node.element_id, node)
        return node
    if kind == "rel":
        _, element_id, type_, start, end, props = cell
        return Relationship(
            None if element_id is None else _node(element_id),
            type_,
            Node(_node(start), frozenset(), {}),
            Node(_node(end), frozenset(), {}),
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


def _link(value: Any, nodes: dict[Any, Node]) -> None:
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
