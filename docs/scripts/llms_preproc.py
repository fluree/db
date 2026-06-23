#!/usr/bin/env python3
"""mdBook preprocessor that emits llms.txt artifacts for the docs site.

This is a *pass-through* preprocessor: it never modifies the book. It reuses
mdBook's own parser (so ordering, hierarchy, and any future ``{{#include}}``
expansion come straight from mdBook) and, as a side effect, writes three
artifacts that make the docs site agent-navigable via the llms.txt convention
(https://llmstxt.org):

  * ``llms.txt``       -- a curated, ordered markdown index of the whole book.
  * ``llms-full.txt``  -- every page concatenated, in SUMMARY order.
  * a mirrored ``.md`` tree so agents can fetch clean markdown per page.

Because preprocessors run *before* renderers and the html renderer cleans its
output directory, the artifacts are written to a gitignored staging dir
(``docs/.llms-staging``). The docs.yml workflow copies that dir into
``docs/book`` after the build, so the artifacts ship at the site root.

The script is *fail-safe*: on any error it logs to stderr and still echoes the
original book JSON to stdout, so a bug here can never break ``mdbook build`` or
``mdbook serve``.

Protocol (https://rust-lang.github.io/mdBook/for_developers/preprocessors.html):
  * ``llms_preproc.py supports <renderer>``  -> exit 0 to participate.
  * otherwise: read ``[context, book]`` JSON from stdin, write ``book`` to stdout.

Pure Python 3 standard library -- no third-party dependencies.
"""

import json
import os
import re
import shutil
import sys
from pathlib import Path

# Public origin where the Pages build is served. Overridable for the
# labs.flur.ee mirror (which uses a different /docs/db/ prefix).
BASE_URL = os.environ.get("LLMS_BASE_URL", "https://fluree.github.io/db").rstrip("/")

# One-line per-link note length budget (truncated on a word boundary).
NOTE_MAX = 140

# Pages whose source path we skip when mirroring / linking (TOC source, etc.).
SKIP_SOURCE_PATHS = {"SUMMARY.md"}


# --------------------------------------------------------------------------- #
# Book traversal
# --------------------------------------------------------------------------- #

def iter_chapters(items, depth=0, section=None):
    """Yield (chapter_dict, depth, section_title) in SUMMARY order.

    ``items`` is a list of serialized BookItems. A BookItem is one of:
      * ``{"Chapter": {...}}``  -- a real chapter
      * ``"Separator"``         -- horizontal rule in the TOC
      * ``{"PartTitle": "..."}``-- a part heading
    Separators and part titles are skipped. ``section`` carries the title of the
    top-level (depth 0) chapter that opens the current ## group.
    """
    for item in items:
        if not isinstance(item, dict) or "Chapter" not in item:
            continue  # Separator / PartTitle
        chapter = item["Chapter"]
        title = chapter.get("name", "")
        # A depth-0 chapter opens a new section; deeper chapters inherit it.
        this_section = title if depth == 0 else section
        yield chapter, depth, this_section
        sub_items = chapter.get("sub_items") or []
        yield from iter_chapters(sub_items, depth + 1, this_section)


def chapter_source_path(chapter):
    """Return the chapter's source path (relative to docs/), or None."""
    src = chapter.get("source_path") or chapter.get("path")
    if not src:
        return None
    # Normalize to forward slashes for URLs / staging paths.
    return str(src).replace("\\", "/")


# --------------------------------------------------------------------------- #
# Text helpers
# --------------------------------------------------------------------------- #

_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]*\)")
_IMG_RE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_EMPH_RE = re.compile(r"[*_`]+")
_LIST_PREFIX_RE = re.compile(r"^\s*(?:[-*+]|\d+\.)\s+")
_WS_RE = re.compile(r"\s+")


def strip_markdown(text):
    """Reduce a line of markdown to clean one-line plain text."""
    text = _IMG_RE.sub("", text)            # drop images entirely
    text = _LINK_RE.sub(r"\1", text)        # [text](url) -> text
    text = _LIST_PREFIX_RE.sub("", text)    # leading bullet / number marker
    text = _EMPH_RE.sub("", text)           # * _ ` emphasis / code markers
    text = _WS_RE.sub(" ", text).strip()
    return text.rstrip(":").strip()


def truncate(text, limit=NOTE_MAX):
    """Truncate on a word boundary, appending an ellipsis if cut."""
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(" ", 1)[0].rstrip()
    return (cut or text[:limit].rstrip()) + "…"


def first_h1(content):
    """Return the text of the first level-1 heading, or ''."""
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return strip_markdown(stripped[2:])
    return ""


def lede(content):
    """First non-empty, non-heading line after the H1 -> a one-line note."""
    seen_h1 = False
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            seen_h1 = seen_h1 or stripped.startswith("# ")
            continue
        if not seen_h1:
            # Some pages may start without an H1; still take the first prose line.
            pass
        note = strip_markdown(stripped)
        if note:
            return truncate(note)
    # Fallback: the H1 text itself.
    return truncate(first_h1(content))


# --------------------------------------------------------------------------- #
# Artifact rendering
# --------------------------------------------------------------------------- #

def to_url(source_path):
    return f"{BASE_URL}/{source_path}"


def render_index(chapters, title, summary):
    """Build llms.txt from the ordered (chapter, depth, section) list."""
    lines = [f"# {title}", ""]
    if summary:
        lines += [f"> {summary}", ""]

    current_section = None
    for chapter, _depth, section in chapters:
        src = chapter_source_path(chapter)
        if src is None or src in SKIP_SOURCE_PATHS:
            continue
        if section != current_section:
            current_section = section
            lines += ["", f"## {section}", ""]  # blank line before each header
        name = chapter.get("name", "") or first_h1(chapter.get("content", ""))
        note = lede(chapter.get("content", ""))
        link = f"- [{name}]({to_url(src)})"
        lines.append(f"{link}: {note}" if note else link)
    lines.append("")  # trailing newline
    return "\n".join(lines)


def render_full(chapters, out_path):
    """Stream-write llms-full.txt: every page, in order, with path headers."""
    with out_path.open("w", encoding="utf-8") as fh:
        for chapter, _depth, _section in chapters:
            src = chapter_source_path(chapter)
            if src is None or src in SKIP_SOURCE_PATHS:
                continue
            fh.write(f"\n\n---\n\n# {src}\n\n")
            fh.write(chapter.get("content", ""))


def mirror_markdown(chapters, staging):
    """Write each chapter's resolved markdown to STAGING/<source_path>."""
    for chapter, _depth, _section in chapters:
        src = chapter_source_path(chapter)
        if src is None or src in SKIP_SOURCE_PATHS:
            continue
        dest = staging / src
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(chapter.get("content", ""), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def resolve_staging(context):
    root = context.get("root")
    base = Path(root) if root else Path(__file__).resolve().parent.parent
    # ``root`` is the book root (where book.toml lives) == docs/.
    return base / ".llms-staging"


def book_title(context):
    try:
        return context["config"]["book"]["title"] or "Documentation"
    except (KeyError, TypeError):
        return "Documentation"


def find_summary(chapters):
    """Lede of the README/index page -> the blockquote project summary."""
    for chapter, _depth, _section in chapters:
        src = chapter_source_path(chapter)
        if src == "README.md":
            return lede(chapter.get("content", ""))
    return ""


def generate(context, book):
    staging = resolve_staging(context)
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    # mdBook 0.5.x serializes the book's items under "items"; older versions
    # used "sections". Accept either.
    chapters = list(iter_chapters(book.get("items") or book.get("sections") or []))
    linkable = [c for c in chapters
                if chapter_source_path(c[0]) not in (None, *SKIP_SOURCE_PATHS)]

    title = book_title(context)
    summary = find_summary(chapters)

    (staging / "llms.txt").write_text(
        render_index(chapters, title, summary), encoding="utf-8")
    render_full(chapters, staging / "llms-full.txt")
    mirror_markdown(chapters, staging)

    print(f"[llms_preproc] wrote {len(linkable)} pages to {staging}",
          file=sys.stderr)


def main():
    # Support check: `llms_preproc.py supports <renderer>` -> exit 0 to run.
    if len(sys.argv) > 1 and sys.argv[1] == "supports":
        sys.exit(0)

    raw = sys.stdin.read()
    context, book = json.loads(raw)

    try:
        generate(context, book)
    except Exception as exc:  # noqa: BLE001 -- fail-safe: never break the build
        print(f"[llms_preproc] WARNING: artifact generation failed: {exc}",
              file=sys.stderr)

    # Always echo the book back unchanged (pass-through preprocessor).
    json.dump(book, sys.stdout)


if __name__ == "__main__":
    main()
