# testsuite-rdf — W3C RDF syntax conformance

Gates the **readers**: `fluree-graph-turtle` and the `fluree-graph-ir` terms it
emits. Its sibling `testsuite-sparql/` gates the query engine; this crate
depends on neither `fluree-db-api` nor a runtime.

Like `testsuite-sparql`, this is an **excluded workspace** (listed in the root
`Cargo.toml`'s `exclude`, with its own `[workspace]` marker so cargo does not
walk up out of a nested git worktree). It builds and runs on its own:

```sh
cd testsuite-rdf
cargo test                     # every suite; the gated ones must be green
cargo run --bin rdf-conformance -- conformance.json
```

## The rdf-tests submodule is SHARED, not duplicated

There is exactly one `w3c/rdf-tests` checkout in this repo, registered in
`.gitmodules` at `testsuite-sparql/rdf-tests`. This harness reads it through
the relative path `../testsuite-sparql/rdf-tests`.

That is deliberate. A second submodule would be a second commit pointer, the
two could drift, and "conformance at rdf-tests SHA X" would stop having a
single answer — which the benchmark lane's freshness contract
(`riot-analog-bench-strategy.md` §6b) depends on.

The cost: **this crate does not run without `testsuite-sparql`'s submodule
initialized.** A fresh worktree needs

```sh
git submodule update --init testsuite-sparql/rdf-tests
```

`files::ensure_rdf_tests_checkout` fails with exactly that command rather than
letting an uninitialized submodule surface as an empty (and therefore
vacuously green) suite.

## What runs, and what gates

| Suite | Mode | Gates? |
|---|---|---|
| RDF 1.1 Turtle | conformant | **yes** |
| RDF 1.1 N-Triples | conformant | **yes** |
| RDF 1.1 Turtle | ingest-default | no — measures the ingest shape's cost |
| RDF 1.2 Turtle / N-Triples | conformant | no — asserting subset only |

**Conformant mode** is `ParserOptions::conformant()`: `CollectionStyle::Spine`
plus `NumericStyle::PreserveLexical`. That is the shape W3C defines and the
shape `fluree convert` must produce, so it is the shape that gates.

**Ingest-default mode** is `ParserOptions::default()` — indexed list items and
canonicalized numeric literals. It is deliberately lossy as RDF, so it is
reported, never gated. The delta between the two modes is the point: it is
what `ParserOptions` bought, expressed in tests.

**RDF 1.2** is informational because the parser implements the *asserting*
subset of RDF-star by decision; non-asserting triple terms are a documented
non-goal. Do not quote an RDF 1.2 number as a conformance figure.

Note that the RDF 1.2 suites run the RDF 1.2-only sub-manifests (`eval/`,
`syntax/`, `c14n/`), **not** the published root manifest. That root
`mf:include`s `../../rdf11/…`, so running it would fold the RDF 1.1 results
into the RDF 1.2 number and report a figure describing neither — 409 tests at
79.0% rather than the 96 RDF 1.2 tests at 27.1% that are actually being
measured.

## Denominator policy

`total = passed + failed + ignored`, and `pass_rate = passed / total`.

Registered (`ignored`) tests **stay in the denominator and out of the
numerator**. Registering a known failure therefore *lowers* the reported rate —
the register is a burn-down ledger, not a way to improve the score. Only rows
with `gating: true` (RDF 1.1, conformant) may be quoted as conformance; the
rule travels inside `conformance.json` as `denominator_policy` so a reader of
the artifact alone cannot misread it.

## The register

`tests/registers/mod.rs` lists known failures with their root cause.
`check_testsuite` polices it in both directions — an unregistered failure and a
registered pass both fail the suite — so the register can only shrink, and an
entry must be deleted in the same change that fixes its cause.

Two limits worth knowing:

- The register cannot catch a test that passes **for the wrong reason**.
- A **negative**-syntax test passes when the parser rejects the document for
  *any* reason. The positive suites are the counterweight; read a high
  negative-suite rate next to a low positive-suite one as over-rejection, not
  conformance. (`registers::NEGATIVE_SYNTAX_BLIND_SPOT`.)

## N-Triples runs through the Turtle parser

There is no N-Triples reader yet. N-Triples is a syntactic subset of Turtle, so
the suites run through the Turtle parser: positive-syntax tests are meaningful,
and negative-syntax tests are only meaningful where the construct is invalid in
Turtle too. `@prefix`, `,` object lists, relative IRIs, `'''`/`"""` strings and
bare numeric literals are all valid Turtle, so those tests cannot fail and are
registered under that cause. The N-Triples pass rate must be read with that
caveat attached.

## Manifests are parsed with our own parser

The manifests are Turtle, and `manifest.rs` reads them with
`fluree-graph-turtle` in conformant mode — including walking `mf:entries` as a
real `rdf:first`/`rdf:rest` spine. A harness that read the suite's own index
with a third-party parser could report green while the parser under test could
not read the suite at all. Loading a manifest is consequently the first
exercise of the conformant collection path on every run.

## conformance.json

`cargo run --bin rdf-conformance` writes the artifact the benchmark lane gates
on: `git_sha`, `captured_at`, `rdf_tests_submodule_sha`, per-format summaries
(format × spec × mode, each flagged `gating`), and the denominator policy. It
runs with an EMPTY register, so it records what the parser does rather than
what CI tolerates.
