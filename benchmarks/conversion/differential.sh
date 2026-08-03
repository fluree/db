#!/usr/bin/env bash
# Writer cost by DIFFERENTIAL: convert (parse + write) minus count (parse only).
#
# Why this exists rather than reading it off `--profile`
# -----------------------------------------------------
# The profile's sink lane has a hard resolution floor of roughly 99ns per call:
# three clock-pair reads per emitted statement, size-independent. Real writer
# sinks sit at 50-200ns/call. So for a cheap sink the instrument costs more than
# the thing it measures, and for an expensive one it still contributes the same
# ~99ns of its own overhead to every sample. Neither is a writer measurement.
#
# The differential sidesteps it: run the SAME parse twice, once with the writer
# attached and once without, both completely un-instrumented, and subtract the
# wall clocks. Nothing is timed from inside the pipeline, so the floor does not
# apply. What is left is the writer's real cost plus output I/O, which is what a
# user actually pays.
#
# It is a difference of two medians, so its noise is the sum of two noises. The
# MAD of both sides is reported next to it; a differential smaller than the
# combined MAD is not a measurement, and the script says so rather than printing
# a confident small number.

set -euo pipefail
# shellcheck source=lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

require_bash 5 "EPOCHREALTIME (the measurement clock) is a bash 5 builtin"
require_dot_decimal_clock
bench_lock_acquire "differential"
require_cmd jq
ensure_dirs

RUNS="${FLUREE_BENCH_RUNS:-10}"
CORPUS="${1:-synthetic-smoke}"
HARNESS="$(require_harness_bin)"
FLUREE_BIN="$REPO_ROOT/target/release/fluree"
input="$CORPORA_DIR/$CORPUS.ttl"

[ -x "$FLUREE_BIN" ] || die "target/release/fluree not built.
    cargo build --release -p fluree-db-cli"
[ -f "$input" ] || die "corpus not found: $input (run ./gen-synthetic.sh)"

# `convert` is the whole point of the differential, so its absence is a real
# blocker rather than something to work around.
if ! "$FLUREE_BIN" convert "$input" --to ntriples >/dev/null 2>&1; then
	die "fluree convert did not run on this build.
  The differential measures convert-minus-count; without convert there is
  nothing to subtract from. Note the verb is top-level — \`fluree convert\`,
  not \`fluree rdf convert\`, which was the spelling before the group was
  dissolved. Re-run against a build that has it."
fi

samples() {
	local i out=()
	for ((i = 0; i <= RUNS; i++)); do
		local s e
		# Builtin clock, no fork. The previous `python3 -c` pair put two
		# interpreter startups inside every interval — fatal for a
		# DIFFERENTIAL, which subtracts two medians: the constant cancels but
		# its jitter does not, so it inflated both MADs and made real deltas
		# fail their own significance test.
		s=${EPOCHREALTIME/./}
		"$@" >/dev/null 2>&1
		e=${EPOCHREALTIME/./}
		[ "$i" -eq 0 ] && continue # warmup
		out+=("$(awk -v s="$s" -v e="$e" 'BEGIN{printf "%.6f",(e-s)/1e6}')")
	done
	printf '%s\n' "${out[@]}" | "$HARNESS" stats
}

info "differential on $CORPUS ($RUNS runs each, first discarded)"

convert_stats="$(samples "$FLUREE_BIN" convert "$input" --to ntriples)"
count_stats="$(samples "$FLUREE_BIN" count "$input")"

convert_med=$(printf '%s' "$convert_stats" | jq -r '.median')
count_med=$(printf '%s' "$count_stats" | jq -r '.median')
convert_mad=$(printf '%s' "$convert_stats" | jq -r '.mad')
count_mad=$(printf '%s' "$count_stats" | jq -r '.mad')

delta=$(awk -v a="$convert_med" -v b="$count_med" 'BEGIN{printf "%.6f", a-b}')
combined_mad=$(awk -v a="$convert_mad" -v b="$count_mad" 'BEGIN{printf "%.6f", a+b}')
trustworthy=$(awk -v d="$delta" -v m="$combined_mad" 'BEGIN{print (d > m) ? "true" : "false"}')

statements=$("$FLUREE_BIN" count --quiet "$input" 2>/dev/null | tr -dc '0-9')
per_stmt_ns=0
if [ -n "$statements" ] && [ "$statements" -gt 0 ]; then
	per_stmt_ns=$(awk -v d="$delta" -v n="$statements" 'BEGIN{printf "%.1f", (d*1e9)/n}')
fi

outfile="$RESULTS_DIR/differential-$CORPUS.json"
jq -n \
	--arg corpus "$CORPUS" --arg host "$(host_class)" \
	--argjson convert "$convert_stats" --argjson count "$count_stats" \
	--argjson delta "$delta" --argjson combined_mad "$combined_mad" \
	--argjson trustworthy "$trustworthy" \
	--argjson statements "${statements:-0}" --argjson per_stmt_ns "$per_stmt_ns" \
	'{corpus:$corpus, host_class:$host, method:"convert-minus-count, both un-instrumented",
	  convert_seconds:$convert, count_seconds:$count,
	  writer_delta_seconds:$delta, combined_mad_seconds:$combined_mad,
	  delta_exceeds_combined_mad:$trustworthy,
	  statements:$statements, writer_ns_per_statement:$per_stmt_ns,
	  caveat:"A difference of two medians carries the noise of both. When delta_exceeds_combined_mad is false the number is not a measurement."}' \
	>"$outfile"

printf '\n  convert  %ss (mad %s)\n' "$convert_med" "$convert_mad" >&2
printf '  count    %ss (mad %s)\n' "$count_med" "$count_mad" >&2
printf '  ---------------------------------\n' >&2
printf '  writer   %ss  (~%s ns/statement over %s statements)\n' "$delta" "$per_stmt_ns" "$statements" >&2
if [ "$trustworthy" = "true" ]; then
	printf '  delta exceeds combined MAD (%s) — usable\n' "$combined_mad" >&2
else
	printf '  delta does NOT exceed combined MAD (%s) — NOT a measurement.\n' "$combined_mad" >&2
	printf '  Use a larger corpus or more runs before quoting it.\n' >&2
fi
printf '\n  wrote %s\n' "$outfile" >&2
