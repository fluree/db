# W3C JSON-LD Compliance Testing

The `testsuite-jsonld/` crate runs the W3C [json-ld-api test
suite](https://github.com/w3c/json-ld-api/tree/main/tests): its expand and
toRdf manifests, as the test fns `jsonld_expand` and `jsonld_to_rdf`. It tests
`fluree-graph-json-ld`, the expander every JSON-LD reader shares
(transactions, bulk import, `rdf::parse`, inline SHACL shapes), and the
adapter that turns expanded JSON-LD into RDF statements.

The crate is **excluded from the workspace**, so run cargo from inside
`testsuite-jsonld/`. The suite is a git submodule; on a fresh clone run:

```bash
git submodule update --init testsuite-jsonld/json-ld-api
```

## Commands (from testsuite-jsonld/)

| Command | Purpose |
|---------|---------|
| `cargo test` | Both suites, as CI runs them |
| `cargo test jsonld_expand` | One suite |
| `JSONLD_TEST=tc014 cargo test -- --nocapture` | Only the tests whose IRI contains `tc014`, each outcome printed in full |

## How a test runs

- **Expand:** the input is expanded with the test's document base (its own
  URL, or `option.base`) and `option.expandContext`. The result is compared
  with `expect` by JSON-LD object comparison: arrays without regard to order
  except a `@list`'s, language tags without regard to case. The expander is the
  Expansion Algorithm; the harness applies the three steps
  `JsonLdProcessor.expand()` adds (§9.1, 8.1–8.3), which Fluree's readers apply
  themselves.
- **toRdf:** the expanded input goes through the JSON-LD adapter, and the
  statements are compared with `expect` (N-Quads) by RDF dataset isomorphism.
  The graph IR holds a `@list` as indexed values of its property; the harness
  writes each list out as its `rdf:first`/`rdf:rest` chain before comparing.
  Literals compare by lexical form, with a number the IR holds by value in its
  canonical form.

The permissive expander runs, as the spec defines expansion. A transaction's
default strict mode also refuses a compact-looking IRI whose prefix is not
defined (`opts.strictCompactIri`).

What a pass does not check:

- Tests for JSON-LD 1.0 processors only (`specVersion: json-ld-1.0`) are not
  run; the summary counts them.
- A negative test passes when the input is refused. Fluree's errors carry no
  JSON-LD error codes, so which error is not compared.
- There is no document loader, so a context named by IRI is never fetched.

## The skip register

`tests/registers/mod.rs` lists every test Fluree fails, grouped by cause.
Each group is a documented deviation (see [JSON-LD
1.1](../reference/compatibility.md#json-ld-11)) or an open issue. The check
works both ways: an unregistered failure fails the suite, and so does a
registered test that passes, or a registered id the manifest no longer has.
When a change makes a registered test pass, remove its entry in the same
change. A registered test that panics fails the suite too.

A new failure belongs in an existing group only if it has the same cause. A
new cause needs an issue, or a deviation documented in the compatibility page,
before its entries go in.

## Updating the submodule

```bash
cd testsuite-jsonld/json-ld-api && git fetch && git checkout <commit> && cd ../..
cd testsuite-jsonld && cargo test
```

New tests that fail need entries; tests that were removed or renamed are
reported as registered but not in the manifest. Update the baseline commit in
the register's header.
