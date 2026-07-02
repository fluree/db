# fluree validate

Validate data against SHACL shapes and print a **validation report** —
instead of rejecting a transaction the way staging-time enforcement does,
`validate` inspects existing state (or a standalone file) and reports every
result it finds.

```bash
fluree validate [<ledger[:branch]> | <file.ttl|file.jsonld>] [options]
```

Requires the `shacl` build feature (enabled by default).

## Two modes

**Ledger mode** validates the current state of a local ledger — indexed data
plus any commits not yet indexed — against its attached shapes (or ad-hoc
shapes you supply):

```bash
fluree validate mydb                          # attached shapes
fluree validate mydb --shacl proposed.ttl     # trial new shapes (replaces attached)
fluree validate mydb --shacl-graph http://example.org/graphs/shapes
```

**File mode** validates an RDF file with no ledger at all: the data loads
into an ephemeral in-memory ledger, the report prints, and nothing persists.
This is the recommended pre-flight for [bulk import](import.md), which
deliberately never runs SHACL:

```bash
fluree validate data.ttl --shacl shapes.ttl
fluree validate dataset.jsonld --shacl shapes.jsonld --format jsonld
fluree validate data-with-embedded-shapes.ttl
```

A file that embeds its own shapes validates against them (staging-time
enforcement is disabled during the ephemeral load, so violating data can't
be rejected before the report is produced).

## Options

| Option | Description |
|--------|-------------|
| `--graph <iri>` | Validate a named data graph instead of the default graph |
| `--shacl <file>` | Shapes file (Turtle or JSON-LD). **Replaces** the ledger's attached shapes by default |
| `--shacl-graph <iri>` | Named graph in the target ledger holding the shapes (conflicts with `--shacl`) |
| `--include-attached` | Union ad-hoc shapes with the attached shapes instead of replacing them |
| `--format <fmt>` | `table` (default, human), `jsonld`, or `turtle` (W3C `sh:ValidationReport`) |
| `--fail-on <sev>` | Exit non-zero when results at or above this severity exist: `violation` (default), `warning`, or `info` |

## Replace vs union semantics

`--shacl` answers "does this data conform to *these* rules?" — the ledger's
attached shapes are not evaluated. This makes trialing a stricter shape
predictable before you transact it. Pass `--include-attached` to evaluate
both sets together.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Report conforms under the `--fail-on` threshold |
| 1 | Validation results at or above the threshold exist (or an operational error) |
| 2 | Usage error (bad flags/arguments) |

CI example:

```bash
fluree validate export.ttl --shacl contracts.ttl --fail-on warning \
  || { echo "source data failed SHACL pre-flight"; exit 1; }
```

## Output

`table` prints one block per result plus a summary line:

```
Violation: http://example.org/ns/bob
    path:      http://schema.org/name
    component: MinCountConstraintComponent
    message:   Expected at least 1 value(s) but found 0

Conforms: false — 1 violation(s), 0 warning(s), 0 info (1 shape(s) checked)
```

`jsonld` and `turtle` emit a W3C-shaped `sh:ValidationReport` with
`sh:focusNode`, `sh:resultPath` (single-predicate paths only — complex paths
are omitted rather than misrepresented), `sh:resultSeverity`,
`sh:sourceShape`, `sh:sourceConstraintComponent`, `sh:resultMessage`, and
`sh:value`.

If the shapes source produces no shapes, the report is vacuously conforming
and a warning is printed to stderr.

## Current limitations

- Local ledgers only; validating a remote (tracked) ledger over HTTP is not
  yet supported.
- Shapes from a *different* ledger must be exported to a file first.

## Related

- [Cookbook: SHACL validation](../guides/cookbook-shacl.md) — shape authoring
  and transaction-time enforcement
- [import](import.md) — bulk import (SHACL-exempt by design; validate first)
