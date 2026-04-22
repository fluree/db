## Turtle utilities

This directory contains ad-hoc scripts for working with large Turtle (`.ttl`) files.

### Count declared prefixes in a Turtle source file

Counts `@prefix`/`PREFIX` and `@base`/`BASE` directives (header-only by default).

```bash
python3 scripts/ttl_prefix_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl
```

If you suspect directives appear later in the file, use:

```bash
python3 scripts/ttl_prefix_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl --scan-all
```

### Count namespaces actually used (likely closer to “NS codes”)

Scans the Turtle source and counts **distinct namespace IRIs used** in:
- full IRI refs like `<http://example.com/a/b>` (namespace determined by a heuristic split)
- prefixed names like `ex:Thing` (namespace resolved via `@prefix`)

```bash
python3 scripts/ttl_namespace_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl
```

By default the IRI namespace split matches Fluree’s `sid_for_iri` heuristic (split on the last `#` or `/`).
If you want the older behavior that can also split on `:`, use:

```bash
python3 scripts/ttl_namespace_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl --namespace-split hash-slash-colon
```

To write a machine-readable report:

```bash
python3 scripts/ttl_namespace_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl \
  --json-out /tmp/dblp_ns_report.json
```

### Interpreting results

- If **declared prefixes** are small (e.g. tens/hundreds) but **distinct namespaces used**
  are very large (e.g. ~30,000), that usually indicates the dataset contains many IRIs
  whose “namespace” is being derived by splitting the IRI (often at the last `/` or `#`).
- If you see many **unresolved prefixes**, it suggests the Turtle contains prefixed names
  where the corresponding `@prefix` declaration is missing, late, or non-standard.

Note: `ttl_namespace_report.py`’s default namespace split now matches Fluree’s `sid_for_iri` fallback:
split only on the last `#` or `/`, and if neither exists the namespace prefix is treated as empty (`""`).

### Simulate namespace allocation strategies on a file slice

If you want a fast estimate of how many distinct namespace prefixes a large TTL will
produce, without running a full import:

```bash
python3 scripts/ttl_ns_sim.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl --max-gb 1.0
```

This reports distinct prefix counts for:
- legacy `last-slash-or-hash`
- current import-side `coarse-heuristic` split (approximation of the Rust logic)

