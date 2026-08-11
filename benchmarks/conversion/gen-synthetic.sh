#!/usr/bin/env bash
# Generate the deterministic synthetic corpus (g1a shape) in Turtle and N-Triples.
#
# Byte-identical on every machine for a given (size, seed), needs no network,
# and is what makes the harness runnable in a sandbox. Both syntaxes are emitted
# NATIVELY here — neither is produced by converting the other, because a corpus
# made with our own writer would benchmark every tool on input shaped by the
# tool under test.
#
# This corpus is a SMOKE FIXTURE, not a benchmark corpus. It is dense, uniform
# and prefix-friendly, which flatters a parser; corpora.lock.json records that
# caveat next to it. Numbers from it are never competitor results.

set -euo pipefail
# shellcheck source=lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

require_bash 5

ensure_dirs

size="${1:-smoke}"
case "$size" in
smoke) triples=10000 ;;
small) triples=250000 ;;
medium) triples=5000000 ;;
*[!0-9]*) die "usage: gen-synthetic.sh [smoke|small|medium|<triple-count>]" ;;
*) triples="$size" ;;
esac

ttl="$CORPORA_DIR/synthetic-$size.ttl"
nt="$CORPORA_DIR/synthetic-$size.nt"

info "generating $triples triples -> $(basename "$ttl"), $(basename "$nt")"

# awk rather than a shell loop: a bash loop emitting 5M lines takes minutes,
# and the generator must not be the slow part of a benchmark harness.
awk -v n="$triples" -v TTL="$ttl" -v NT="$nt" '
BEGIN {
  print "@prefix ex: <http://example.org/g1a/> ." > TTL
  print "" > TTL
  # Subject-major with a small predicate fan-out, which is the shape the
  # parallel splitter is restricted to (one prefix header, no multiline
  # literals). ~28.5 bytes/triple, matching the g1a scaling corpus.
  for (i = 0; i < n; i++) {
    s = int(i / 5)
    p = i % 5
    printf "ex:s%d ex:p%d \"v%d\" .\n", s, p, i > TTL
    printf "<http://example.org/g1a/s%d> <http://example.org/g1a/p%d> \"v%d\" .\n", s, p, i > NT
  }
}' </dev/null

ttl_bytes=$(wc -c <"$ttl" | tr -d ' ')
nt_bytes=$(wc -c <"$nt" | tr -d ' ')

cat >"$CORPORA_DIR/synthetic-$size.json" <<EOF
{
  "corpus": "synthetic-$size",
  "role": "smoke-fixture-not-a-benchmark-corpus",
  "triples": $triples,
  "files": {
    "ttl": {"path": "$ttl", "bytes": $ttl_bytes, "sha256": "$(sha256_of "$ttl")"},
    "nt":  {"path": "$nt",  "bytes": $nt_bytes,  "sha256": "$(sha256_of "$nt")"}
  },
  "shape_caveat": "Dense, uniform, single prefix header, no multiline literals. Flatters a parser and matches the splitter's restriction; never quote as a competitor result."
}
EOF

info "ttl $(printf '%s' "$ttl_bytes") bytes, nt $(printf '%s' "$nt_bytes") bytes"
info "manifest: $CORPORA_DIR/synthetic-$size.json"
