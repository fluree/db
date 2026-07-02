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
- `sh:equals` / `sh:lessThan` report one aggregate violation instead of one
  per missing/extra value (`equals-001`, `lessThan-002`).
- `xsd:dateTime` range comparison does not implement the spec's
  timezone partial order (`minInclusive-002/003`).
- `sh:property` nested on a *property* shape (validating each value against
  child property shapes) is not implemented (`property-001`,
  `validation-reports/shared`).
- Custom severity IRIs (`sh:severity ex:MySeverity`) collapse to
  `sh:Violation` (`severity-002`).
- `sh:sparql` (the whole `sparql/` section of the suite is not wired in).
- `complex/shacl-shacl` (validating shapes against the SHACL-SHACL meta
  shapes) depends on several of the above.

A few expectations are unachievable by design — Fluree's value-centric
store differs from RDF term identity:

- `4` and `4.0` are the same value in flake space, so they collapse into
  one `sh:targetNode` target (`minExclusive-001` / `maxExclusive-001`);
  likewise `"1"^^xsd:boolean` canonicalizes to `true`, so a spec-pedantic
  `sh:uniqueLang "1"^^xsd:boolean` (which must be ignored — only the term
  `true` activates the component) is indistinguishable from `true` and
  fires (`uniqueLang-002`).
- Ill-formed typed literals (`"aldi"^^xsd:integer`, `"none"^^xsd:boolean`)
  are rejected at ingest and can never be present to validate
  (`datatype-001`, `datatype-ill-formed`, `or-datatypes-001`).
- Duplicate values in an RDF list collapse (`xone-duplicate`,
  `path-complex-002`'s `( _:pinv _:pinv )` sequence).

Track the current pass rate with `make count` before and after engine
changes; regressions in previously-passing categories are the signal to
watch.
