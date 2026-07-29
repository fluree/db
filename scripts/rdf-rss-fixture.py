#!/usr/bin/env python3
"""Generate the two corpora behind the peak-RSS table in docs/cli/rdf/README.md.

Peak RSS for `fluree rdf` is not a fixed multiple of input size, because most
of the excess is the Turtle parser's IRI cache and that grows with the number
of *distinct* IRIs rather than with bytes. These two fixtures isolate exactly
that variable: same statement count, near-identical byte size, and nothing
different between them but how many subjects there are to cache.

    python3 scripts/rdf-rss-fixture.py /tmp
    fluree rdf count -q --profile=json --no-hash /tmp/distinct.ttl 2>&1 \
      | jq '.corpus.bytes_decoded, .host.peak_rss_bytes'
    fluree rdf count -q --profile=json --no-hash /tmp/reused.ttl 2>&1 \
      | jq '.corpus.bytes_decoded, .host.peak_rss_bytes'

Measure with a release build: a debug binary allocates differently enough to
move the ratio. The figures documented in README.md were taken this way.
"""

import sys

STATEMENTS = 480_000
REUSED_SUBJECTS = 100


def write(path, subject_of):
    with open(path, "w") as f:
        f.write("@prefix ex: <http://example.org/> .\n")
        for i in range(STATEMENTS):
            f.write(f'ex:s{subject_of(i)} ex:name "person {i}" ; ex:age {i % 90} .\n')


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "."
    # Every subject a fresh IRI: worst case for the cache.
    write(f"{out}/distinct.ttl", lambda i: i)
    # A hundred subjects, reused: closer to a real dump.
    write(f"{out}/reused.ttl", lambda i: i % REUSED_SUBJECTS)
    print(f"wrote {out}/distinct.ttl and {out}/reused.ttl ({STATEMENTS} statements each)")


if __name__ == "__main__":
    main()
