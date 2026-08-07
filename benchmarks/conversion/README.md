# Tier-2 conversion benchmark harness

The competitor A/B instrument from `riot-analog-bench-strategy.md` §3 + §6b.
Scripts, not criterion; **not in CI**; run on demand at milestones.

**This is the instrument, not the claim.** Nothing measured here is publishable.
Numbers become publishable only from the official locus (H-10, pending AJ), and
the tooling is built to make an accidental publication hard rather than to trust
that nobody will.

## Quick start

```sh
cd benchmarks/conversion
(cd harness && cargo build --release)   # the tested helpers
./harness/target/release/bench-harness calibrate   # verify RSS units on this host
./gen-synthetic.sh smoke                # deterministic local corpus, no network
./fetch-tools.sh                        # what is installed vs what is pinned
./run-matrix.sh synthetic-smoke         # the cells
./check-correctness.sh                  # agreement + independent isomorphism
./render-matrix.sh                      # human view
```

## What each piece refuses to do

| file | refuses |
|---|---|
| `tools.lock.json` | comparing against an unrecorded build — a `--version` mismatch removes the tool from the matrix |
| `corpora.lock.json` | vendoring corpora; dbpedia-live is CC-BY-SA and DBLP republishes in place |
| `fetch-corpora.sh` | mixing a re-downloaded corpus with numbers taken against the old bytes |
| `lib/common.sh` | running without GNU time; stamping a translated shell as a real host |
| `run-matrix.sh` | user-time clocks; unwarmed samples; a cell whose statement count changed |
| `check-correctness.sh` | calling a fast-but-wrong cell fast; skipping rdflib silently |
| `harness/` | guessing an RSS unit; averaging away an outlier |

## Any number in an older report is void

Every figure produced by this harness before the EPOCHREALTIME clock landed is
**clock-contaminated and must not be quoted**, even as dev-signal. The previous
implementation took its timestamps with `python3 -c`, putting two interpreter
startups — measured here at 26–40 ms the pair — *inside* every measured
interval. On a corpus where serdi does ~10 ms of real work that is not a
perturbation, it is most of the number, and because it was a near-constant it
compressed every ratio toward whichever tool was slowest. Re-measure; do not
rescale.

## Three things that are easy to get wrong, and how they are handled

**Peak RSS units.** Darwin's `getrusage(2)` returns `ru_maxrss` in *bytes* where
Linux returns *kibibytes*, so the obvious portable fix is a
`#[cfg(target_os = "macos")]` divide-by-1024. That is wrong for this input: GNU
time normalizes before printing, and its `(kbytes)` label is truthful on both
platforms. Applying the syscall correction makes every macOS number 1024× too
small, and "1.5 MB peak RSS for a parser" is plausible enough that nobody would
question it. The harness reads the label and ships `calibrate`, which allocates
a known 200 MiB and checks the reading, so the next host answers the question by
measurement instead of by trusting this paragraph.

**Binary translation, and what it does *not* imply.** On an Apple-silicon Mac
running a translated shell, `uname -m` reports `x86_64`, so every result would
be stamped with hardware that was not involved. Detected via
`sysctl.proc_translated` and stamped `…-translated`.

But the interesting hazard is not the shell. A translated parent still runs an
**arm64-only child natively**, so our own binaries are fine. What bites is a
*competitor* shipping x86_64-only — an Intel-prefix Homebrew build, say —
because then one column is emulated, another is not, and the matrix reports the
emulator as a performance difference. Every cell therefore records the
architecture class its executable actually runs as, and the renderer marks a run
whose cells disagree as non-comparable.

Two classes are deliberate admissions rather than answers:
`translated-x86_64` (definitely emulated here) and `universal-indeterminate`
(several slices; which one the kernel chose is not observable from outside the
process, and a universal binary launched from a translated parent does not
reliably pick the native one). **riot is a script over a universal JVM, so it
falls in the second class** — which matters, since riot is the named target.

**Machine load.** A benchmark harness that cannot distinguish "this tool is
slow" from "this machine was busy" is not measuring the tool. Load inflates
every sample in the same direction, so the median moves with it and MAD does not
correct for it — repetition does not help. The runner records load per core,
warns when it exceeds 0.5, and marks the run non-publishable. This was found the
same way as the others: a clean re-run reported serdi at 21 ms against a
directly-measured ground truth of 10 ms, and the residual turned out to be
sibling build jobs holding the load average at 10 on a 16-core box.

**Checking parity.** riot validates Turtle by default and N-Triples not at all.
Our parser does no IRI validation. A default-vs-default matrix would therefore
race a validating Turtle run against a non-validating one and call the
difference performance. riot is run at both `--check=false` and `--check=true`,
both published, and our column is labelled `validation_level: none` until W-h8
lands — at which point `our_tool.modes.validate` (already in the lock) becomes
the primary cell opposite `check_true`.

## Normalization rules for cross-tool diffing

Two conformant tools spell the same RDF differently. The normalizer folds only
differences that are provably not differences:

- **Language-tag case.** `@EN-gb` and `@en-GB` are the same tag. We preserve the
  source spelling; riot canonicalizes. A byte-diff would flag every
  language-tagged triple in the corpus.
- **`"s"^^xsd:string` ≡ `"s"`.** Identical in RDF 1.1. We and riot emit the short
  form; rdflib and rapper keep the explicit datatype.
- **Statement order.** Not RDF.

It deliberately does **not** touch IRIs. riot may warn on an IRI containing a
space — that warning is about the IRI *value* (invalid per RFC 3987) and is a
property of the input, not of anyone's escaping. Both tools name the same
resource, so it is not a triple-level difference and is not counted as one.

The normalizer has **no fluree dependencies at all**. Routing both sides of a
differential through our own parser would let a bug in our reader cancel itself
out and disappear.

## The writer differential, and why `--profile` cannot do it

`--profile`'s sink lane has a hard ~99 ns/call resolution floor — three
clock-pair reads per statement, size-independent. Real writer sinks cost
50–200 ns/call, so the instrument is the same order as the thing measured.
`differential.sh` instead runs the same parse twice, once with the writer
attached (`convert`) and once without (`count`), both completely
un-instrumented, and subtracts wall clocks. It reports the combined MAD next to
the delta and says outright when the delta does not exceed it, because a
difference of two medians carries the noise of both.

## Open items

- **Conformance gating is not wired.** §6b F8 requires Tier-2 to run the W3C
  suites first and refuse to emit a matrix on any `git_sha` / rdf-tests mismatch,
  with pass-rate printed per format next to the matching speed cell. The manifest
  carries a `conformance` field recording that this is absent rather than
  implying it passed.
- **Cold-start cells** (small file × N invocations) are specified in §3 and not
  yet implemented. This is where riot's JVM startup shows up, and it is one of
  the genuinely unopposed measurements.
- **Thread-scaling curve** is gate #1 in §6b and belongs with the parallel
  pipeline (W-parallel), not here.
- **Our own cells** need `fluree rdf convert`, which lands with the writers. The
  matrix already carries the column and the runner reports the gap explicitly
  rather than showing an empty row.
