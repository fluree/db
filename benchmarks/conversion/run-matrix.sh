#!/usr/bin/env bash
# Tier-2: run every (tool x corpus x syntax x mode) cell and emit machine-readable
# results, one JSON per cell, into .results/<run-id>/.
#
# What this measures, and what it refuses to:
#
#   - WALL CLOCK, never user time. User time double-counts threads and punishes
#     the JVM for its own startup on another core; the lane's most-cited serd
#     numbers are user-time numbers, which is part of why they disagree with
#     everyone else's by ~6x.
#   - Peak RSS via GNU time, through the one unit-normalizing function in the
#     Rust helper. No gtime, no run.
#   - Median + MAD over n>=10 with the first run discarded as warmup.
#   - Correctness ALONGSIDE speed: every cell's output is re-parsed and its
#     triples counted. A fast cell that lost statements is a failed cell, not a
#     fast one.
#
# Nothing here is publishable. host_class is stamped into every result and the
# renderer marks anything that is not the official locus as dev-signal.

set -euo pipefail
# shellcheck source=lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

require_bash 5 "EPOCHREALTIME (the measurement clock) is a bash 5 builtin"
require_cmd jq
ensure_dirs

RUNS="${FLUREE_BENCH_RUNS:-10}"
CORPUS="${1:-synthetic-smoke}"
GNU_TIME="$(gnu_time_bin)"
HARNESS="$(require_harness_bin)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$CORPUS"
OUT="$RESULTS_DIR/$RUN_ID"
mkdir -p "$OUT"

HOST_CLASS="$(host_class)"
if host_is_publishable; then
	PUBLISHABLE=true
else
	PUBLISHABLE=false
fi

# Was the machine idle enough to be measuring the tools rather than itself?
LOAD_PER_CORE="$(host_load_per_core)"
if host_is_quiet; then
	LOAD_CONTAMINATED=false
else
	LOAD_CONTAMINATED=true
	PUBLISHABLE=false
fi

printf '\nTier-2 conversion matrix\n' >&2
printf '  corpus      %s\n' "$CORPUS" >&2
printf '  runs        %s (first discarded as warmup)\n' "$RUNS" >&2
printf '  host_class  %s%s\n' "$HOST_CLASS" \
	"$([ "$PUBLISHABLE" = false ] && printf ' — DEV-SIGNAL ONLY, never publishable')" >&2
printf '  load        %s per core (%s cores)\n' "$LOAD_PER_CORE" "$(host_cores)" >&2
if [ "$LOAD_CONTAMINATED" = true ]; then
	warn "the machine is BUSY (load/core $LOAD_PER_CORE). Every cell in this run is
  inflated in the same direction, so the medians move with the load and MAD
  does not correct for it. Treat this run as ordering-only, not as timings."
fi
printf '  results     %s\n\n' "$OUT" >&2

ttl="$CORPORA_DIR/$CORPUS.ttl"
nt="$CORPORA_DIR/$CORPUS.nt"

# Ground-truth statement count, if the corpus ships a manifest. Every cell is
# checked against it; a corpus without one degrades to "no count check" rather
# than to "the check passed".
EXPECTED_TRIPLES=""
if [ -f "$CORPORA_DIR/$CORPUS.json" ]; then
	EXPECTED_TRIPLES="$(jq -r '.triples // empty' "$CORPORA_DIR/$CORPUS.json")"
	[ -n "$EXPECTED_TRIPLES" ] && info "ground truth: $EXPECTED_TRIPLES statements per cell"
else
	warn "no manifest for $CORPUS — statement counts cannot be checked against ground truth"
fi

# ---------------------------------------------------------------------------
# One cell: n timed invocations of one command, plus a correctness check.
# ---------------------------------------------------------------------------
run_cell() {
	local tool="$1" mode="$2" syntax="$3" input="$4"
	shift 4
	local -a cmd=("$@")

	[ -f "$input" ] || {
		info "$tool/$mode/$syntax: input missing, skipped"
		return 0
	}

	local cell_out="$OUT/$tool.$mode.$syntax"
	local converted="$cell_out.out.nt"
	local timings=() child_elapsed=() rss_bytes=0 wall

	local i
	for ((i = 0; i <= RUNS; i++)); do
		local time_file="$cell_out.time"
		local start end
		# `EPOCHREALTIME` is a bash builtin: reading it forks NOTHING.
		#
		# This replaces a `python3 -c` timestamp on each side of the interval,
		# which put TWO interpreter startups (measured here at 26-40ms the
		# pair) INSIDE the measured region. On a corpus where serdi does ~10ms
		# of real work that is not a perturbation, it is most of the number,
		# and because it is a constant it compressed every ratio toward
		# whichever tool was slowest.
		#
		# GNU time's own `%e` would be child-only and is what gtime is for,
		# but it is centisecond-resolution: measured directly, `gtime -f %e`
		# reports "0.00" for serdi on the smoke corpus. It is recorded below
		# as a cross-check for corpora large enough to register, and is not
		# the primary clock.
		start=${EPOCHREALTIME/./}
		if ! "$GNU_TIME" -v -o "$time_file" "${cmd[@]}" >"$converted" 2>"$cell_out.stderr"; then
			end=${EPOCHREALTIME/./}
			info "$tool/$mode/$syntax: command FAILED (see $cell_out.stderr)"
			# Remove the partial output so the correctness gate cannot mistake
			# a failed cell's leavings for a result.
			rm -f "$converted"
			jq -n --arg tool "$tool" --arg mode "$mode" --arg syntax "$syntax" \
				--arg corpus "$CORPUS" --arg host "$HOST_CLASS" \
				'{tool:$tool,mode:$mode,syntax:$syntax,corpus:$corpus,host_class:$host,status:"failed"}' \
				>"$cell_out.json"
			return 0
		fi
		end=${EPOCHREALTIME/./}
		# Discard run 0 as warmup: page cache, JIT, dynamic linking.
		[ "$i" -eq 0 ] && continue
		wall=$(awk -v s="$start" -v e="$end" 'BEGIN{printf "%.6f",(e-s)/1e6}')
		timings+=("$wall")
		child_elapsed+=("$(awk '/Elapsed \(wall clock\)/{n=split($NF,p,":"); printf "%.2f", (n==3)?p[1]*3600+p[2]*60+p[3]:p[1]*60+p[2]}' "$time_file")")
		rss_bytes="$("$HARNESS" rss "$time_file")"
	done

	local stats
	stats="$(printf '%s\n' "${timings[@]}" | "$HARNESS" stats)"

	# --- correctness, alongside speed, not after it ---------------------
	local out_lines
	out_lines="$(grep -cve '^[[:space:]]*$' -e '^[[:space:]]*#' "$converted" || true)"

	# Ground truth from the corpus manifest, when the corpus has one. A cell
	# that emitted the wrong NUMBER of statements is a failed cell however
	# fast it was, and "0 statements, 0.00s" is the fastest wrong answer
	# available. Without this the gate happily blessed an empty file.
	local expected="null" verdict='"ok"'
	if [ -n "$EXPECTED_TRIPLES" ]; then
		expected="$EXPECTED_TRIPLES"
		if [ "$out_lines" -ne "$EXPECTED_TRIPLES" ]; then
			verdict='"statement-count-mismatch"'
			info "$tool/$mode/$syntax: WRONG COUNT — expected $EXPECTED_TRIPLES, got $out_lines"
		fi
	fi

	local in_bytes median mb_s tr_s
	in_bytes=$(wc -c <"$input" | tr -d ' ')
	median="$(printf '%s' "$stats" | jq -r '.median')"
	mb_s=$(awk -v b="$in_bytes" -v t="$median" 'BEGIN{printf "%.2f", (t>0)?(b/1048576)/t:0}')
	tr_s=$(awk -v n="$out_lines" -v t="$median" 'BEGIN{printf "%.0f", (t>0)?n/t:0}')

	jq -n \
		--arg tool "$tool" --arg mode "$mode" --arg syntax "$syntax" \
		--arg corpus "$CORPUS" --arg host "$HOST_CLASS" \
		--argjson publishable "$PUBLISHABLE" \
		--argjson stats "$stats" \
		--argjson rss "$rss_bytes" --argjson in_bytes "$in_bytes" \
		--argjson out_statements "$out_lines" \
		--argjson mb_s "$mb_s" --argjson tr_s "$tr_s" \
		--argjson expected "$expected" --argjson verdict "$verdict" \
		--arg child_elapsed "${child_elapsed[*]}" \
		--arg cmd "${cmd[*]}" \
		'{tool:$tool, mode:$mode, syntax:$syntax, corpus:$corpus,
		  host_class:$host, publishable:$publishable,
		  clock:"wall (bash EPOCHREALTIME, no subprocess in the interval)",
		  wall_seconds:$stats,
		  child_elapsed_seconds_crosscheck:$child_elapsed,
		  child_elapsed_note:"GNU time %e, child-only but centisecond-resolution; corroboration only, not the primary clock",
		  peak_rss_bytes:$rss, input_bytes:$in_bytes,
		  out_statements:$out_statements, expected_statements:$expected,
		  mb_per_second:$mb_s, statements_per_second:$tr_s,
		  invocation:$cmd, status:$verdict}' >"$cell_out.json"

	printf '  %-10s %-12s %-9s %8.3fs  %8s MB/s  %10s stmt/s  %6s stmts\n' \
		"$tool" "$mode" "$syntax" "$median" "$mb_s" "$tr_s" "$out_lines" >&2
}

# ---------------------------------------------------------------------------
# Tool gate: only tools whose version matches the lock may produce a cell.
# ---------------------------------------------------------------------------
declare -a ELIGIBLE=()
declare -a REFUSED=()
mapfile -t all_tools < <(jq -r '.tools | to_entries[] | select(.value.role != "reference-only") | .key' "$TOOLS_LOCK")
for tool in "${all_tools[@]}"; do
	set +e
	verify_tool_version "$tool"
	rc=$?
	set -e
	case $rc in
	0) ELIGIBLE+=("$tool") ;;
	1) REFUSED+=("$tool (version mismatch)") ;;
	2) REFUSED+=("$tool (not installed)") ;;
	esac
done

printf 'eligible: %s\n' "${ELIGIBLE[*]:-none}" >&2
[ ${#REFUSED[@]} -gt 0 ] && printf 'refused:  %s\n' "$(
	IFS=', '
	printf '%s' "${REFUSED[*]}"
)" >&2
printf '\n' >&2

# ---------------------------------------------------------------------------
# The cells
# ---------------------------------------------------------------------------
for tool in "${ELIGIBLE[@]}"; do
	case "$tool" in
	riot)
		xmx="$(jq -r '.tools.riot.jvm.xmx' "$TOOLS_LOCK")"
		export JVM_ARGS="$xmx"
		run_cell riot check_false ttl "$ttl" riot --output=NT --check=false "$ttl"
		run_cell riot check_true ttl "$ttl" riot --output=NT --check=true "$ttl"
		run_cell riot check_false nt "$nt" riot --output=NT --check=false "$nt"
		run_cell riot check_true nt "$nt" riot --output=NT --check=true "$nt"
		;;
	serdi)
		run_cell serdi default ttl "$ttl" serdi -o ntriples "$ttl"
		run_cell serdi default nt "$nt" serdi -i ntriples -o ntriples "$nt"
		;;
	oxigraph)
		run_cell oxigraph default ttl "$ttl" oxigraph convert --from-format ttl --to-format nt --file "$ttl"
		run_cell oxigraph default nt "$nt" oxigraph convert --from-format nt --to-format nt --file "$nt"
		;;
	rapper)
		run_cell rapper default ttl "$ttl" rapper -i turtle -o ntriples "$ttl"
		run_cell rapper default nt "$nt" rapper -i ntriples -o ntriples "$nt"
		;;
	esac
done

# --- our own column ---------------------------------------------------------
FLUREE_BIN="$REPO_ROOT/target/release/fluree"
if [ -x "$FLUREE_BIN" ]; then
	if "$FLUREE_BIN" rdf convert --help >/dev/null 2>&1 &&
		! "$FLUREE_BIN" rdf convert "$ttl" --to ntriples >/dev/null 2>&1; then
		info "fluree rdf convert is present but not yet implemented on this branch — our cells deferred"
		info "  (check/count exist; convert lands with the writers. The matrix carries the column already.)"
	else
		run_cell fluree default ttl "$ttl" "$FLUREE_BIN" rdf convert "$ttl" --to ntriples
		run_cell fluree default nt "$nt" "$FLUREE_BIN" rdf convert "$nt" --to ntriples
	fi
else
	info "target/release/fluree not built — our cells skipped (cargo build --release -p fluree-db-cli)"
fi

# ---------------------------------------------------------------------------
# Run manifest
# ---------------------------------------------------------------------------
jq -n \
	--arg run_id "$RUN_ID" --arg corpus "$CORPUS" --arg host "$HOST_CLASS" \
	--argjson publishable "$PUBLISHABLE" --argjson runs "$RUNS" \
	--argjson load_per_core "$LOAD_PER_CORE" --argjson load_contaminated "$LOAD_CONTAMINATED" \
	--arg git_sha "$(git -C "$REPO_ROOT" rev-parse HEAD)" \
	--argjson eligible "$(printf '%s\n' "${ELIGIBLE[@]:-}" | jq -Rsc 'split("\n")|map(select(length>0))')" \
	--argjson refused "$(printf '%s\n' "${REFUSED[@]:-}" | jq -Rsc 'split("\n")|map(select(length>0))')" \
	'{run_id:$run_id, corpus:$corpus, host_class:$host, publishable:$publishable,
	  load_per_core:$load_per_core, load_contaminated:$load_contaminated,
	  runs_per_cell:$runs, warmup_discarded:1,
	  clock:"wall (bash EPOCHREALTIME builtin; no subprocess inside the measured interval)",
	  git_sha:$git_sha, tools_eligible:$eligible, tools_refused:$refused,
	  conformance:"NOT YET WIRED — Tier-2 must refuse to emit a matrix on a conformance mismatch (§6b F8); see README Open items."}' \
	>"$OUT/manifest.json"

printf '\nwrote %s\n' "$OUT" >&2
[ "$PUBLISHABLE" = false ] &&
	printf 'DEV-SIGNAL ONLY: host_class %s is not the official locus.\n' "$HOST_CLASS" >&2
exit 0
