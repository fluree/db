"""Graph values a Cypher result returns: nodes, relationships, and paths."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from fluree._terms import IRI, BlankNode


class Node(Mapping[str, Any]):
    """A node: its properties (read like a dict), its ``labels`` — the
    node's classes — and ``element_id``, its identity: the same
    :class:`IRI` (or :class:`BlankNode`) a SPARQL query returns for it."""

    __slots__ = ("_properties", "element_id", "labels")

    def __init__(
        self, element_id: IRI | BlankNode, labels: frozenset[str], properties: dict[str, Any]
    ) -> None:
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
    the nodes it runs from and to. ``element_id`` is its identity when it
    has one (every relationship a Cypher ``CREATE`` makes does).

    ``start_node`` and ``end_node`` carry their properties and labels when
    the same result returned those nodes; otherwise only their
    ``element_id``.
    """

    __slots__ = ("_properties", "element_id", "end_node", "start_node", "type")

    def __init__(
        self,
        element_id: IRI | BlankNode | None,
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
