#!/usr/bin/env bash
# Correctness gate for a matrix run — speed cells are meaningless without it.
#
# Two levels, deliberately separate:
#
#   1. CROSS-TOOL AGREEMENT. Every eligible tool's output for the same corpus is
#      normalized (langtag case, explicit xsd:string, statement order) and
#      diffed against the others. Normalization is done by the Rust helper,
#      which has no fluree dependencies at all — routing both sides through our
#      own parser would let a bug in our reader cancel itself out and vanish.
#
#   2. INDEPENDENT ISOMORPHISM. rdflib, a wholly separate implementation, reads
#      each output and compares graphs. This catches what a normalized line-diff
#      cannot: blank-node structure. Skipped BY NAME if rdflib is absent, never
#      silently.
#
# A tool that is fast and wrong is a failed cell, not a fast one.

set -euo pipefail
# shellcheck source=lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

require_cmd jq
HARNESS="$(require_harness_bin)"

# A correctness gate that dies silently is worse than no gate: the caller sees
# no complaint and concludes the run was clean. `set -e` plus `pipefail` makes
# that failure mode easy to write by accident (it happened here — `diff` inside
# a command substitution). Anything that kills this script says so.
REPORTED=0
# shellcheck disable=SC2154  # `rc` is assigned in the trap body itself
trap 'rc=$?; if [ "$rc" -ne 0 ] && [ "$REPORTED" -eq 0 ]; then printf "\ncorrectness gate ABORTED before reaching a verdict (exit %s) — this is NOT a pass.\n" "$rc" >&2; fi' EXIT

RUN_DIR="${1:-}"
if [ -z "$RUN_DIR" ]; then
	RUN_DIR="$(find "$RESULTS_DIR" -maxdepth 1 -type d -name '*-*' 2>/dev/null | sort | tail -1)"
fi
[ -n "$RUN_DIR" ] && [ -d "$RUN_DIR" ] || die "no run directory (usage: check-correctness.sh [.results/<run-id>])"

printf '\ncorrectness gate: %s\n\n' "$(basename "$RUN_DIR")" >&2

# --- 1. cross-tool agreement on normalized output ---------------------------
declare -a outputs=()
while IFS= read -r f; do outputs+=("$f"); done < <(find "$RUN_DIR" -name '*.out.nt' | sort)

[ ${#outputs[@]} -gt 0 ] || die "no cell outputs in $RUN_DIR"

normalized_dir="$RUN_DIR/normalized"
mkdir -p "$normalized_dir"

declare -A by_syntax=()
for out in "${outputs[@]}"; do
	base="$(basename "$out" .out.nt)"   # tool.mode.syntax
	syntax="${base##*.}"
	norm="$normalized_dir/$base.norm.nt"
	"$HARNESS" normalize "$out" >"$norm"
	by_syntax["$syntax"]="${by_syntax[$syntax]:-} $norm"
done

failures=0
for syntax in "${!by_syntax[@]}"; do
	# shellcheck disable=SC2206  # deliberate word-splitting of the accumulated list
	files=(${by_syntax[$syntax]})
	[ ${#files[@]} -gt 1 ] || {
		info "$syntax: only one tool produced output — nothing to cross-check"
		continue
	}
	reference="${files[0]}"
	ref_name="$(basename "$reference" .norm.nt)"
	for candidate in "${files[@]:1}"; do
		cand_name="$(basename "$candidate" .norm.nt)"
		if diff -q "$reference" "$candidate" >/dev/null 2>&1; then
			printf '  %-9s %-28s == %-28s  agree\n' "$syntax" "$ref_name" "$cand_name" >&2
		else
			# `|| true` is load-bearing: diff exits 1 when files differ, and
			# under `set -o pipefail` that becomes the substitution's status,
			# which `set -e` then treats as a script error. The gate would die
			# SILENTLY at the exact moment it found a defect — indistinguishable
			# from passing, if the caller only reads stdout.
			first_diff="$(diff "$reference" "$candidate" | head -6 || true)"
			printf '  %-9s %-28s != %-28s  DIFFER\n' "$syntax" "$ref_name" "$cand_name" >&2
			printf '%s\n' "$first_diff" | sed 's/^/        /' >&2
			failures=$((failures + 1))
		fi
	done
done

# --- 2. independent isomorphism via rdflib ----------------------------------
printf '\n' >&2
if ! python3 -c 'import rdflib' >/dev/null 2>&1; then
	printf '  SKIPPED (named): rdflib not installed — the independent isomorphism\n' >&2
	printf '  check did not run. Install with: python3 -m pip install rdflib\n' >&2
else
	python3 - "$RUN_DIR" <<'PY' >&2 || failures=$((failures + 1))
import sys, pathlib, itertools
import rdflib
from rdflib.compare import to_isomorphic

run = pathlib.Path(sys.argv[1])
outs = sorted(run.glob("*.out.nt"))
by_syntax = {}
for p in outs:
    syntax = p.name.rsplit(".out.nt", 1)[0].split(".")[-1]
    by_syntax.setdefault(syntax, []).append(p)

bad = 0
for syntax, paths in sorted(by_syntax.items()):
    graphs = {}
    for p in paths:
        g = rdflib.Graph()
        try:
            g.parse(str(p), format="nt")
        except Exception as e:  # a cell that rdflib cannot read is a failed cell
            print(f"  {syntax:9} {p.name}: rdflib REFUSED to parse: {e}")
            bad += 1
            continue
        graphs[p.name] = to_isomorphic(g)
    for (an, ag), (bn, bg) in itertools.combinations(graphs.items(), 2):
        if ag == bg:
            print(f"  {syntax:9} {an} ~= {bn}  isomorphic ({len(ag)} triples)")
        else:
            print(f"  {syntax:9} {an} !~ {bn}  NOT ISOMORPHIC ({len(ag)} vs {len(bg)})")
            bad += 1
sys.exit(1 if bad else 0)
PY
fi

printf '\n' >&2
REPORTED=1
if [ "$failures" -gt 0 ]; then
	die "$failures correctness failure(s) — the speed numbers in this run are not comparable."
fi
printf 'correctness gate passed.\n' >&2
