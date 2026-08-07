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

require_bash 5
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

# --- 0. ground truth: every cell must have emitted the right COUNT ----------
#
# A cell that produced nothing is not a fast cell, and an empty file trivially
# agrees with another empty file — so without this the cross-tool diff below
# blesses two tools that both emitted zero statements.
failures=0
checked_cells=0
for cell in "$RUN_DIR"/*.json; do
	[ "$(basename "$cell")" = "manifest.json" ] && continue
	status="$(jq -r '.status' "$cell")"
	name="$(basename "$cell" .json)"
	if [ "$status" != "ok" ]; then
		printf '  %-30s cell status %s — EXCLUDED from comparison\n' "$name" "$status" >&2
		failures=$((failures + 1))
		continue
	fi
	# Recount the output FILE rather than trusting the runner's own bookkeeping:
	# validate bytes, not a number the producer wrote about itself.
	out_file="$RUN_DIR/$name.out.nt"
	if [ ! -f "$out_file" ]; then
		printf '  %-30s output file MISSING\n' "$name" >&2
		failures=$((failures + 1))
		continue
	fi
	got="$(grep -cve '^[[:space:]]*$' -e '^[[:space:]]*#' "$out_file" || true)"
	claimed="$(jq -r '.out_statements' "$cell")"
	if [ "$got" != "$claimed" ]; then
		printf '  %-30s BOOKKEEPING MISMATCH: record says %s, file has %s\n' \
			"$name" "$claimed" "$got" >&2
		failures=$((failures + 1))
	fi
	want="$(jq -r '.expected_statements // "null"' "$cell")"
	if [ "$want" = "null" ]; then
		printf '  %-30s %s statements (no ground truth for this corpus)\n' "$name" "$got" >&2
	elif [ "$got" != "$want" ]; then
		printf '  %-30s WRONG COUNT: expected %s, got %s\n' "$name" "$want" "$got" >&2
		failures=$((failures + 1))
	else
		printf '  %-30s %s statements, matches ground truth\n' "$name" "$got" >&2
	fi
	checked_cells=$((checked_cells + 1))
done
printf '\n' >&2

# --- 1. cross-tool agreement on normalized output ---------------------------
#
# Only cells that passed the count check above take part: comparing against a
# known-bad cell tells you nothing about the good one.
declare -a outputs=()
while IFS= read -r f; do
	cell_json="${f%.out.nt}.json"
	[ -f "$cell_json" ] || continue
	[ "$(jq -r '.status' "$cell_json")" = "ok" ] || continue
	outputs+=("$f")
done < <(find "$RUN_DIR" -name '*.out.nt' | sort)

[ ${#outputs[@]} -gt 0 ] || die "no usable cell outputs in $RUN_DIR"

normalized_dir="$RUN_DIR/normalized"
mkdir -p "$normalized_dir"

declare -A by_syntax=()
declare -A exempt_of=()
for out in "${outputs[@]}"; do
	base="$(basename "$out" .out.nt)"   # tool.mode.syntax
	syntax="${base##*.}"
	norm="$normalized_dir/$base.norm.nt"
	# The normalizer reports on stderr how many blank-node statements it left
	# out of the level-1 comparison; level 2 owns those.
	# An unreadable or malformed cell is an I/O failure, not a content
	# disagreement. Swallowing normalize's status here would have produced an
	# empty normalized file and reported it downstream as a DIFFER — a wrong
	# diagnosis of a real problem.
	if ! "$HARNESS" normalize "$out" 2>"$normalized_dir/$base.exempt" >"$norm"; then
		printf '  %-30s normalize FAILED: %s\n' "$base" \
			"$(head -1 "$normalized_dir/$base.exempt")" >&2
		failures=$((failures + 1))
		continue
	fi
	exempt_of["$base"]="$(cat "$normalized_dir/$base.exempt")"
	by_syntax["$syntax"]="${by_syntax[$syntax]:-} $norm"
done

compared_pairs=0
for syntax in "${!by_syntax[@]}"; do
	# shellcheck disable=SC2206  # deliberate word-splitting of the accumulated list
	files=(${by_syntax[$syntax]})
	if [ ${#files[@]} -le 1 ]; then
		# NOT a pass. One tool agreeing with itself verifies nothing, and
		# reporting success here is how a matrix with a single eligible tool
		# looks fully validated.
		printf '  %-9s only ONE tool produced output — cross-tool agreement UNVERIFIED\n' "$syntax" >&2
		failures=$((failures + 1))
		continue
	fi
	reference="${files[0]}"
	ref_name="$(basename "$reference" .norm.nt)"
	for candidate in "${files[@]:1}"; do
		cand_name="$(basename "$candidate" .norm.nt)"
		if diff -q "$reference" "$candidate" >/dev/null 2>&1; then
			compared_pairs=$((compared_pairs + 1))
			printf '  %-9s %-28s == %-28s  agree (%s blank-node stmts exempt)\n' \
				"$syntax" "$ref_name" "$cand_name" "${exempt_of[$ref_name]:-0}" >&2
		else
			# `|| true` is load-bearing: diff exits 1 when files differ, and
			# under `set -o pipefail` that becomes the substitution's status,
			# which `set -e` then treats as a script error. The gate would die
			# SILENTLY at the exact moment it found a defect — indistinguishable
			# from passing, if the caller only reads stdout.
			first_diff="$(diff "$reference" "$candidate" | head -6 || true)"
			compared_pairs=$((compared_pairs + 1))
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
if [ "$checked_cells" -eq 0 ]; then
	die "no cell reached the correctness gate — nothing was verified."
fi
if [ "$failures" -gt 0 ]; then
	die "$failures correctness failure(s) — the speed numbers in this run are not comparable."
fi
printf 'correctness gate passed.\n' >&2
