"""Records the API returns: commits, changes, branches, and the outcomes and
previews of merging, rebasing and reverting."""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal as _Literal

from fluree._terms import IRI, BlankNode, _datetime, to_python

if TYPE_CHECKING:
    from fluree._results import Result


@dataclass(frozen=True, slots=True)
class Commit:
    """The outcome of a transaction.

    ``id`` is the commit's content id (a CID). ``digest`` is its hex hash, the
    form ``fluree log`` shows: like a git SHA, a unique prefix of it
    (``short_id``, say) identifies the commit. A prefix of ``id`` does not,
    since every CID starts with the same header characters.

    ``id`` and ``digest`` are ``None`` when the transaction changed nothing, in
    which case no commit was written and ``t`` is the ledger's unchanged ``t``.

    ``time`` and ``message`` are filled in for commits read from
    :meth:`Ledger.log` and the previews. ``result`` holds the records a
    Cypher write's ``RETURN`` produced.
    """

    t: int
    id: str | None
    digest: str | None
    asserts: int
    retracts: int
    time: _dt.datetime | None = None
    message: str | None = None
    #: The records a Cypher write's ``RETURN`` produced, else ``None``.
    result: Result | None = field(default=None, compare=False, repr=False)

    @property
    def short_id(self) -> str | None:
        """The first 12 hex digits of ``digest``, as ``fluree log`` prints them."""
        return None if self.digest is None else self.digest[:12]


@dataclass(frozen=True, slots=True)
class Change:
    """One fact asserted or retracted at transaction ``t``: ``subject``'s
    ``predicate`` gained (``op == "assert"``) or lost (``op == "retract"``)
    ``value``, in the default graph or the named ``graph``.

    ``t`` is ``None`` for the net changes a merge preview reports, which span
    several commits.
    """

    t: int | None
    op: _Literal["assert", "retract"]
    subject: IRI | BlankNode
    predicate: IRI
    value: Any
    graph: IRI | None = None


@dataclass(frozen=True, slots=True)
class Branch:
    """A branch of a ledger. Open it with ``conn.ledger(branch.id)``.

    ``source`` is the branch it was created from, ``None`` for the ledger's
    first branch. ``t`` and ``head`` are its latest commit's ``t`` and id.
    """

    name: str
    id: str
    source: str | None
    t: int
    head: str | None


@dataclass(frozen=True, slots=True)
class Conflict:
    """A property of a subject that both sides changed.

    ``source`` and ``target`` are what each side wrote to it, when the preview
    was asked for ``details``; otherwise ``None``.
    """

    subject: IRI | BlankNode
    predicate: IRI
    graph: IRI | None
    source: list[Change] | None = None
    target: list[Change] | None = None


@dataclass(frozen=True, slots=True)
class MergeResult:
    """The outcome of :meth:`Ledger.merge`.

    A ``fast_forward`` merge moved ``target`` to ``source``'s latest commit
    without writing one; otherwise ``id`` is the new merge commit. ``t`` is
    ``target``'s ``t`` afterwards. ``conflicts`` counts the properties
    ``strategy`` resolved.
    """

    source: str
    target: str
    fast_forward: bool
    t: int
    id: str
    digest: str
    conflicts: int
    strategy: str | None


@dataclass(frozen=True, slots=True)
class RebaseResult:
    """The outcome of :meth:`Ledger.rebase`.

    Of the branch's ``total`` own commits, ``replayed`` were rewritten on top
    of its source's latest commit (``source_t``) and ``skipped`` were dropped
    by the ``"skip"`` strategy. ``conflicts`` lists ``(t, properties,
    resolution)`` for each commit that conflicted, ``t`` being the commit's
    original ``t``; ``failures`` lists ``(t, error)`` for commits that failed
    validation once replayed. A ``fast_forward`` rebase had no commits of its
    own to replay.
    """

    fast_forward: bool
    replayed: int
    skipped: int
    total: int
    source_t: int
    conflicts: list[tuple[int, int, str]]
    failures: list[tuple[int, str]]


@dataclass(frozen=True, slots=True)
class RevertResult:
    """The outcome of :meth:`Ledger.revert`.

    ``reverted`` holds the ids of the undone commits, newest first. When
    ``committed`` is false the commits had nothing left to undo, no commit was
    written, and ``t``/``id`` describe the unchanged head. ``conflicts`` counts
    the properties changed since that ``strategy`` resolved.
    """

    committed: bool
    t: int
    id: str
    digest: str
    reverted: list[str]
    conflicts: int
    strategy: str


@dataclass(frozen=True, slots=True)
class MergePreview:
    """What merging ``source`` into ``target`` would do, without doing it.

    - ``ahead``: commits on ``source`` that ``target`` lacks, newest first;
      ``behind``: the reverse. Each list may be capped; ``ahead_count`` and
      ``behind_count`` are the full counts.
    - ``ancestor_t``: the ``t`` of the last commit the two share.
    - ``fast_forward``: ``target`` has no commits of its own since, so the
      merge just moves it forward.
    - ``conflicts``: properties both sides changed (may be capped;
      ``conflict_count`` is the full count).
    - ``mergeable``: the merge would succeed under the previewed strategy,
      including the ledger's SHACL shapes; when it would fail validation,
      ``violations`` holds the report.
    - ``changes``: the net facts the merge would bring in, when asked for. Pass
      ``changes_after`` back as ``changes_after=`` to read the next page.
    """

    source: str
    target: str
    fast_forward: bool
    mergeable: bool
    ancestor_t: int | None
    ahead: list[Commit]
    ahead_count: int
    behind: list[Commit]
    behind_count: int
    conflicts: list[Conflict]
    conflict_count: int
    violations: str | None
    changes: list[Change] | None
    changes_after: str | None


@dataclass(frozen=True, slots=True)
class RevertPreview:
    """What reverting would do, without doing it.

    ``commits`` are the commits that would be undone, newest first (may be
    capped; ``commit_count`` is the full count). ``conflicts`` are properties
    changed again since those commits. ``revertable`` says whether the revert
    would succeed under the previewed strategy and the ledger's SHACL shapes;
    when validation would fail, ``violations`` holds the report.
    """

    revertable: bool
    commits: list[Commit]
    commit_count: int
    conflicts: list[Conflict]
    conflict_count: int
    violations: str | None


def _node(iri: str) -> IRI | BlankNode:
    return BlankNode(iri[2:]) if iri.startswith("_:") else IRI(iri)


def _commit(summary: dict[str, Any]) -> Commit:
    time = summary["time"]
    return Commit(**{**summary, "time": None if time is None else _datetime(time)})


def _change(t: int | None, flake: tuple[Any, ...]) -> Change:
    """A :class:`Change` from the native ``(s, p, object, assert, graph)``."""
    s, p, o, op, g = flake
    return Change(
        t=t,
        op="assert" if op else "retract",
        subject=_node(s),
        predicate=IRI(p),
        value=_node(o[1]) if o[0] == "iri" else to_python(o),
        graph=None if g is None else IRI(g),
    )


def _conflict(raw: dict[str, Any]) -> Conflict:
    side = raw["source"], raw["target"]
    source, target = ([_change(None, f) for f in flakes] if flakes is not None else None for flakes in side)
    return Conflict(
        subject=_node(raw["subject"]),
        predicate=IRI(raw["predicate"]),
        graph=None if raw["graph"] is None else IRI(raw["graph"]),
        source=source,
        target=target,
    )


def _merge_preview(raw: dict[str, Any]) -> MergePreview:
    changes = raw["changes"]
    return MergePreview(
        **{
            **raw,
            "ahead": [_commit(c) for c in raw["ahead"]],
            "behind": [_commit(c) for c in raw["behind"]],
            "conflicts": [_conflict(c) for c in raw["conflicts"]],
            "changes": None if changes is None else [_change(None, f) for f in changes],
        }
    )


def _revert_preview(raw: dict[str, Any]) -> RevertPreview:
    return RevertPreview(
        **{
            **raw,
            "commits": [_commit(c) for c in raw["commits"]],
            "conflicts": [_conflict(c) for c in raw["conflicts"]],
        }
    )


_SHACL = "http://www.w3.org/ns/shacl#"
_RDF_LANG_STRING = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """One way the data fails a SHACL shape.

    ``focus`` is the node that failed and ``path`` the property, when the
    constraint is on one; ``value`` is the offending value, when there is
    one. ``severity`` is ``"violation"``, ``"warning"`` or ``"info"``.
    ``shape`` is the node shape and ``component`` the SHACL constraint
    component (``sh:MinCountConstraintComponent``, ...) that produced it.
    """

    focus: Any
    path: IRI | None
    message: str
    severity: str
    value: Any
    shape: IRI | BlankNode
    component: IRI


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """The outcome of :meth:`Ledger.validate`. ``conforms`` is false when any
    result is a violation. ``shape_count`` is how many shapes were checked —
    ``0`` means no shapes were found, so the data trivially conforms. ``t`` is
    the state that was validated."""

    conforms: bool
    results: list[ValidationResult]
    shape_count: int
    t: int


@dataclass(frozen=True, slots=True)
class IndexStatus:
    """Where indexing of a ledger stands. Commits after ``index_t`` (up to
    ``commit_t``) are queryable but not yet indexed. ``phase`` is ``"idle"``,
    ``"pending"`` or ``"in_progress"``; ``enabled`` is false on a connection
    without background indexing (``":memory:"``, or ``indexing=False``)."""

    index_t: int
    commit_t: int
    enabled: bool
    phase: str
    error: str | None


@dataclass(frozen=True, slots=True)
class VerifyReport:
    """The outcome of :meth:`Ledger.verify`.

    ``severity`` is ``"healthy"``, ``"provenance"`` (a raw transaction is
    missing: the data is intact) or ``"chain"`` (a commit or the index root is
    missing or unreadable). ``problems`` describes each one, as a dict with a
    ``kind``. ``truncated`` means ``max_commits`` stopped the check early.
    """

    severity: str
    head_t: int
    index_t: int
    commits_checked: int
    truncated: bool
    problems: list[dict[str, Any]]

    @property
    def healthy(self) -> bool:
        return self.severity == "healthy"


@dataclass(frozen=True, slots=True)
class SweepResult:
    """The outcome of :meth:`Ledger.sweep`: ``orphans`` index files no longer
    referenced, of which ``reclaimed`` were deleted (none on a dry run);
    ``failures`` lists ``(file, error)`` for those that could not be."""

    dry_run: bool
    orphans: int
    reclaimed: int
    failures: list[tuple[str, str]]


def _jsonld_term(value: Any) -> Any:
    """A Python value for a JSON-LD node reference, value object, or scalar."""
    if isinstance(value, dict):
        if "@id" in value:
            return _node(value["@id"])
        if "@language" in value:
            return to_python(("literal", str(value["@value"]), _RDF_LANG_STRING, value["@language"]))
        if "@type" in value:
            return to_python(("literal", str(value["@value"]), value["@type"], None))
        return value.get("@value")
    return value


def _validation_report(raw: dict[str, Any]) -> ValidationReport:
    results = [
        ValidationResult(
            focus=_node(r["focus_node"]) if isinstance(r["focus_node"], str) else _jsonld_term(r["focus_node"]),
            path=None if r.get("result_path") is None else IRI(r["result_path"]),
            message=r["message"],
            severity=r["severity"].removeprefix(_SHACL).lower(),
            value=_jsonld_term(r.get("value")),
            shape=_node(r["source_shape"]),
            component=IRI(r["constraint_component"]),
        )
        for r in raw["results"]
    ]
    return ValidationReport(raw["conforms"], results, raw["shape_count"], raw["t"])
