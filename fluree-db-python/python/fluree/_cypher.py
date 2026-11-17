"""Cypher: typed result cells decoded into Python values and graph objects."""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from typing import Any

from fluree._graph import Node, Path, Relationship
from fluree._records import _jsonld_term, _node
from fluree._results import Result, _record_type
from fluree._terms import _datetime, _iso
from fluree.errors import InvalidRequestError


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
        return _dt.time.fromisoformat(_iso(iso.removesuffix("Z")))
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
