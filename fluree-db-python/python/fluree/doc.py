"""Read documents with an extraction model reached over HTTP.

:func:`read` parses a file (PDF, Word, PowerPoint, HTML, Markdown, text,
spreadsheets, transcripts, email) the way ``fluree doc parse`` does, sends
its text and structure with your ontology and catalog to a reader, and
returns what it read: mentions linked to your catalog's records, entities,
and claims, each anchored to character offsets in the parser's text (what
``fluree doc parse --format text`` prints)::

    import fluree

    reading = fluree.doc.read("invoice.pdf", ontology="invoices.ttl",
                              catalog="suppliers.ttl", model="reader-small")
    for m in reading.mentions():
        print(m["text"], m["entity"])

    with fluree.connect("./data") as conn:
        conn.create("invoices").insert(reading.to_jsonld())

The read is the ``fluree`` CLI's own, so a document is cut into calls, sent,
resent when the reader has no capacity yet, and kept the same way. Waits for
capacity (a model still starting can take minutes) are logged as warnings
on the ``fluree.engine`` logger. Python does no vision escalation: scanned
pages and images are read with the parser's deterministic tier only, and
:attr:`Reading.notes` says what it couldn't read.

**Signing in.**

1. ``api_key`` with ``auth_header``: that key, sent as is in that header,
   to ``url``.
2. ``api_key`` alone: a Fluree.ai API key (``flk_…``) is traded for an
   access token (reused for the hour it lasts); any other key is sent as
   ``Authorization: Bearer``, as the CLI's ``[doc.reader] api_key`` is.
3. No ``api_key``: this machine's ``fluree login``, else the
   ``FLUREE_AI_API_KEY`` environment variable (as in 2). Each is sent only
   to its own service; a ``url`` elsewhere is called with no credential.

With no ``url``, a Fluree.ai sign-in reads through the account.

**Rows and terms.** :meth:`Reading.mentions` and :meth:`Reading.claims`
read NIF offsets (``nif:beginIndex``, ``nif:anchorOf``, ``nif:entity``) and
the pipeline's ``doc:`` terms. A reader that writes terms of its own keeps
them in :attr:`Reading.nodes` and :meth:`Reading.to_jsonld` as it wrote
them.
"""

from __future__ import annotations

import hashlib
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

# A node of one of these types, with offsets, is a mention.
_MENTION_TYPES = (_DOC + "Mention", _NIF + "RFC5147String", _NIF + "String")
_RDF_FILE = (".ttl", ".nt", ".jsonld", ".json")


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
    notes: list[str] = field(default_factory=list)
    """What the parse left out: pages it couldn't read (no vision escalation
    from Python), an email's attachments, and the like."""

    def to_jsonld(self) -> dict[str, Any]:
        """The reading as a JSON-LD document, ready for ``Ledger.insert``."""
        return {"@context": json.loads(_fluree.reading_context()), "@graph": self.nodes}

    def to_turtle(self) -> str:
        """The reading as Turtle."""
        return _fluree.reading_turtle(json.dumps(self.nodes))

    def mentions(self) -> list[dict[str, Any]]:
        """One row per mention, a NIF occurrence of the text: ``id``,
        ``text`` (``nif:anchorOf``), ``begin`` and ``end`` (offsets in the
        parser's text), ``entity`` (``nif:entity``, the record or entity it
        names, when the reading links it) and ``element`` (the structure
        element it sits in, ``doc:sourceElement``)."""
        rows = []
        for node in self.nodes:
            types = node.get("@type", [])
            if not any(t in types for t in _MENTION_TYPES):
                continue
            if node.get(_NIF + "beginIndex") is None:
                continue
            rows.append({
                "id": node.get("@id"),
                "text": _value(node, _NIF + "anchorOf"),
                "begin": _int(_value(node, _NIF + "beginIndex")),
                "end": _int(_value(node, _NIF + "endIndex")),
                "entity": _value(node, _NIF + "entity"),
                "element": _value(node, _DOC + "sourceElement"),
            })
        return rows

    def claims(self) -> list[dict[str, Any]]:
        """One row per claim, a reified statement (``doc:Relation``):
        ``id``, ``subject``, ``predicate``, ``object``, and ``asserted``
        (the reading also states the triple)."""
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

    Give a file as ``path``, or plain text as ``text``. A text is read under
    ``doc_iri``, by default ``<base_iri>text/<first 16 hex of its SHA-256>``,
    so two different texts never share a document's nodes. ``ontology`` is
    a ``.ttl``, ``.nt``, ``.jsonld`` or ``.json`` file. ``catalog`` is one
    such file, or a list of them, each optionally a ``(file, class IRI)``
    pair to take only that class's records; a bare 2-tuple is read as such
    a pair unless its second item is itself an RDF file. ``cache``
    (``"ontology"``, ``"catalog"`` or ``"off"``) lets the reader keep that
    much for the next request sharing it. With ``cache_dir``, parses and
    answers are kept there, so reading the same request again costs nothing.

    Raises :class:`fluree.errors.InvalidRequestError` for arguments that
    can't make a read (a bad ``cache``, ``auth_header`` without
    ``api_key``, no way to sign in),
    :class:`fluree.errors.PermissionDeniedError` when a credential is
    refused, and :class:`fluree.errors.FlureeError` for a reader's other
    refusals, with its message.
    """
    if (path is None) == (text is None):
        raise TypeError("read() takes a path or text=, not both")
    catalogs: list[tuple[str, str | None]] = []
    for entry in _as_list(catalog):
        if isinstance(entry, tuple):
            if len(entry) != 2:
                raise TypeError(
                    f"a catalog entry is a file or a (file, class IRI) pair, not {entry!r}"
                )
            catalogs.append((os.fspath(entry[0]), str(entry[1])))
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
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
        if doc_iri is None:
            kwargs["doc_iri"] = f"{base_iri}text/{digest}"
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp) / f"{digest}.txt"
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
        notes=list(raw.get("notes", [])),
    )


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike)):
        return [value]
    if isinstance(value, tuple):
        # A (file, class) pair, unless both items are RDF files.
        if len(value) == 2 and not os.fspath(value[1]).lower().endswith(_RDF_FILE):
            return [value]
        return list(value)
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


