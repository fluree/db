"""Read documents with an extraction model reached over HTTP.

:func:`read` parses a file (PDF, Word, PowerPoint, HTML, Markdown, text,
spreadsheets, transcripts, email) the way ``fluree doc parse`` does, sends
its text and structure with your ontology and catalog to a reader, and
returns what it read: mentions linked to your catalog's records, entities,
and claims, each anchored to the text's character offsets::

    import fluree

    reading = fluree.doc.read("invoice.pdf", ontology="invoices.ttl",
                              catalog="suppliers.ttl", model="reader-small")
    for m in reading.mentions():
        print(m["text"], m["entity"])

    with fluree.connect("./data") as conn:
        conn.create("invoices").insert(reading.to_jsonld())

The read is the ``fluree`` CLI's own, so a document is cut into calls, sent,
resent when the reader has no capacity yet, and kept the same way.

**Signing in.** The first of these that is present is used:

1. ``api_key`` with ``auth_header``: that key, sent as is in that header,
   to ``url``;
2. ``api_key`` alone: a Fluree.ai API key (``flk_…``), traded for an access
   token;
3. this machine's ``fluree login``;
4. the ``FLUREE_AI_API_KEY`` environment variable.

With no ``url``, a Fluree.ai sign-in reads through the account.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from fluree import _fluree

__all__ = ["Reading", "Usage", "read"]

_DOC = "https://ns.flur.ee/doc#"
_NIF = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#"
_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


@dataclass(frozen=True)
class Usage:
    """What a read was billed, as the reader reported it."""

    input_tokens: int
    windows: int
    cache_read_tokens: int
    cache_write_tokens: int
    fuel: float | None
    note: str
    """The same, as one line: ``1,032 tokens, cache 900 read / 0 written``."""


@dataclass(frozen=True)
class Reading:
    """One document's reading, in the shape ``fluree doc ingest`` commits."""

    nodes: list[dict[str, Any]]
    """The reading as expanded JSON-LD nodes."""
    mention_count: int
    entity_count: int
    relation_count: int
    linked: list[str]
    """Catalog records the reading links to."""
    usage: Usage
    calls: int
    """Calls the document took; :attr:`cached` of them were answered from the cache."""
    cached: int
    release: str | None
    ontology_notes: list[str] = field(default_factory=list)
    """What the reader set aside or adapted in the ontology."""

    def to_jsonld(self) -> dict[str, Any]:
        """The reading as a JSON-LD document, ready for ``Ledger.insert``."""
        return {"@context": json.loads(_fluree.reading_context()), "@graph": self.nodes}

    def to_turtle(self) -> str:
        """The reading as Turtle."""
        return _fluree.reading_turtle(json.dumps(self.nodes))

    def mentions(self) -> list[dict[str, Any]]:
        """One row per mention: ``text``, ``begin``, ``end``, ``class``,
        ``entity`` (the record or entity it names), ``score``, ``element``."""
        rows = []
        for node in self.nodes:
            if _DOC + "Mention" not in node.get("@type", []):
                continue
            rows.append({
                "id": node.get("@id"),
                "text": _value(node, _NIF + "anchorOf"),
                "begin": _int(_value(node, _NIF + "beginIndex")),
                "end": _int(_value(node, _NIF + "endIndex")),
                "class": _value(node, _DOC + "mentionClass"),
                "entity": _value(node, _NIF + "entity"),
                "score": _float(_value(node, _DOC + "score")),
                "element": _value(node, _DOC + "sourceElement"),
            })
        return rows

    def claims(self) -> list[dict[str, Any]]:
        """One row per claim: ``subject``, ``predicate``, ``object``,
        ``asserted``, ``score``."""
        rows = []
        for node in self.nodes:
            if _DOC + "Relation" not in node.get("@type", []):
                continue
            rows.append({
                "id": node.get("@id"),
                "subject": _value(node, _RDF + "subject"),
                "predicate": _value(node, _RDF + "predicate"),
                "object": _value(node, _RDF + "object"),
                "asserted": bool(_value(node, _DOC + "asserted")),
                "score": _float(_value(node, _DOC + "score")),
            })
        return rows


def read(
    path: str | os.PathLike[str] | None = None,
    *,
    text: str | None = None,
    ontology: str | os.PathLike[str],
    catalog: str | os.PathLike[str] | Sequence[str | os.PathLike[str] | tuple[str | os.PathLike[str], str]] | None = None,
    model: str,
    cache: str | None = None,
    url: str | None = None,
    api_key: str | None = None,
    auth_header: str | None = None,
    doc_iri: str | None = None,
    base_iri: str = "urn:fluree:doc:",
    cache_dir: str | os.PathLike[str] | None = None,
) -> Reading:
    """Read one document with ``model`` and return its :class:`Reading`.

    Give a file as ``path``, or plain text as ``text``. ``ontology`` is a
    ``.ttl``, ``.nt``, ``.jsonld`` or ``.json`` file; ``catalog`` one such
    file or several, each optionally ``(file, class IRI)`` to take only that
    class's records. ``cache`` (``"ontology"`` or ``"catalog"``) lets the
    reader keep that much for the next request sharing it. With
    ``cache_dir``, parses and answers are kept there, so reading the same
    request again costs nothing.

    Raises :class:`fluree.errors.PermissionDeniedError` when the sign-in is
    refused, and :class:`fluree.errors.FlureeError` for a reader's refusal,
    with its message.
    """
    if (path is None) == (text is None):
        raise TypeError("read() takes a path or text=, not both")
    catalogs: list[tuple[str, str | None]] = []
    for entry in _as_list(catalog):
        if isinstance(entry, tuple):
            catalogs.append((os.fspath(entry[0]), entry[1]))
        else:
            catalogs.append((os.fspath(entry), None))
    kwargs: dict[str, Any] = dict(
        ontology=os.fspath(ontology),
        catalog=catalogs,
        model=model,
        url=url,
        api_key=api_key,
        auth_header=auth_header,
        cache=cache,
        doc_iri=doc_iri,
        base_iri=base_iri,
        cache_dir=os.fspath(cache_dir) if cache_dir is not None else None,
    )
    if text is not None:
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp) / "text.txt"
            file.write_text(text, encoding="utf-8")
            raw = _fluree.read_document(os.fspath(file), **kwargs)
    else:
        raw = _fluree.read_document(os.fspath(path), **kwargs)  # type: ignore[arg-type]
    return _reading(raw)


def _reading(raw: Mapping[str, Any]) -> Reading:
    u = raw["usage"]
    return Reading(
        nodes=json.loads(raw["nodes"]),
        mention_count=raw["mentions"],
        entity_count=raw["entities"],
        relation_count=raw["relations"],
        linked=list(raw["linked"]),
        usage=Usage(
            input_tokens=u["input_tokens"],
            windows=u["windows"],
            cache_read_tokens=u["cache_read_tokens"],
            cache_write_tokens=u["cache_write_tokens"],
            fuel=u["fuel"],
            note=u["note"],
        ),
        calls=raw["calls"],
        cached=raw["cached"],
        release=raw["release"],
        ontology_notes=list(raw["ontology_notes"]),
    )


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike, tuple)):
        return [value]
    return list(value)


def _value(node: Mapping[str, Any], key: str) -> Any:
    values = node.get(key)
    if not values:
        return None
    first = values[0]
    if isinstance(first, Mapping):
        return first.get("@id", first.get("@value"))
    return first


def _int(value: Any) -> int | None:
    return None if value is None else int(value)


def _float(value: Any) -> float | None:
    return None if value is None else float(value)
