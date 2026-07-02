# W3C SHACL Compliance Testing

The `testsuite-shacl/` crate runs the official [W3C data-shapes test
suite](https://github.com/w3c/data-shapes) (core section) against the SHACL
engine, through the same `fluree_db_api::validate` core that powers
`fluree validate` and the `/validate` HTTP endpoint.

The crate is **excluded from the workspace** — `cd testsuite-shacl/` before
running any cargo or make commands. The W3C tests are vendored as a git
submodule; on a fresh clone run:

```bash
git submodule update --init testsuite-shacl/data-shapes
```

## Quick commands (from testsuite-shacl/)

| Command | Purpose |
|---------|---------|
| `make count` | Overall pass/fail numbers |
| `make summary` | Per-category breakdown table |
| `make test-cat CAT=node` | Run one category |
| `make test-one TEST=minLength-001` | Run matching tests with full output |
| `make failures` | List failing tests with mismatch reasons |
| `make report-json` | Machine-readable report → `report.json` |
| `make show TEST=node/minLength-001` | Print a W3C test file |

To debug a single case interactively, the CLI runs the same pipeline:

```bash
fluree validate <test>.ttl --format turtle   # most tests embed data+shapes
```

## How a test runs

Each `sht:Validate` entry names a data graph and a shapes graph (usually the
test file itself). The runner loads the data into an ephemeral in-memory
ledger (staging-time SHACL disabled via the config graph, so violating data
can load), validates with `ShapesSource::InlineTurtle`, and compares the
produced report against the `sh:ValidationReport` embedded in the manifest.

## Comparison semantics and deliberate leniency

Results are compared as a multiset on `sh:focusNode`, `sh:resultPath`,
`sh:resultSeverity`, `sh:sourceConstraintComponent`, and `sh:value`
(`sh:resultMessage` is never compared, per the suite's own rules). Where
exact comparison needs machinery we don't have yet, the harness is lenient:

- **Blank nodes** (focus nodes, values, complex `sh:resultPath` structures)
  match as wildcards instead of by graph isomorphism, and `sh:sourceShape`
  is not compared at all.
- **Fields absent from an expected result** accept any actual value.

Tightening these is future work; a pass under leniency is still meaningful
because conformance flag, result count, component IRIs, and severities must
match exactly.

## Known engine gaps the suite surfaces

Failures are expected in these areas (honest gaps, not harness bugs):

- Complex `sh:resultPath` serialization (sequence/inverse path structures)
  is omitted from reports.
- `sh:equals` reports one aggregate violation instead of one per
  missing/extra value.
- `xsd:dateTime` range comparison does not implement the spec's
  timezone partial order (`minInclusive-002/003`).
- `sh:sparql` (the whole `sparql/` section of the suite is not wired in).

A few expectations are unachievable by design — Fluree's value-centric
store differs from RDF term identity:

- `4` and `4.0` are the same value in flake space, so they collapse into
  one `sh:targetNode` target (`minExclusive-001` / `maxExclusive-001`).
- Ill-formed typed literals (`"aldi"^^xsd:integer`) are rejected at ingest
  and can never be present to validate (`datatype-001`).
- Duplicate values in an RDF list collapse (`xone-duplicate`).

Track the current pass rate with `make count` before and after engine
changes; regressions in previously-passing categories are the signal to
watch.
