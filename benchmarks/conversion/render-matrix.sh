#!/usr/bin/env bash
# Render a human-readable matrix from a run's JSON cells.
#
# The JSON is the artifact; this is a view of it. Every number carries its
# provenance in the header — host_class, git sha, run count, clock — because a
# matrix screenshotted out of a terminal and pasted into a document is exactly
# how this lane's unreproducible numbers propagate.

set -euo pipefail
# shellcheck source=lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

require_cmd jq

RUN_DIR="${1:-}"
if [ -z "$RUN_DIR" ]; then
	RUN_DIR="$(find "$RESULTS_DIR" -maxdepth 1 -type d -name '*-*' 2>/dev/null | sort | tail -1)"
fi
[ -n "$RUN_DIR" ] && [ -f "$RUN_DIR/manifest.json" ] ||
	die "no run to render (usage: render-matrix.sh [.results/<run-id>])"

m="$RUN_DIR/manifest.json"
publishable=$(jq -r '.publishable' "$m")
host=$(jq -r '.host_class' "$m")

echo
echo "  Tier-2 conversion matrix — $(jq -r '.corpus' "$m")"
echo "  ==============================================================================="
printf '  host_class  %s\n' "$host"
printf '  git         %s\n' "$(jq -r '.git_sha' "$m" | cut -c1-12)"
printf '  runs        %s per cell, %s warmup discarded\n' \
	"$(jq -r '.runs_per_cell' "$m")" "$(jq -r '.warmup_discarded' "$m")"
printf '  clock       %s\n' "$(jq -r '.clock' "$m")"
printf '  load        %s per core%s\n' "$(jq -r '.load_per_core // "unknown"' "$m")" \
	"$([ "$(jq -r '.load_contaminated // false' "$m")" = true ] && printf ' — BUSY, see below')"
printf '  eligible    %s\n' "$(jq -r '.tools_eligible | join(", ")' "$m")"
refused=$(jq -r '.tools_refused | join(", ")' "$m")
[ -n "$refused" ] && printf '  REFUSED     %s\n' "$refused"

if [ "$publishable" != "true" ]; then
	echo
	echo "  *** DEV-SIGNAL ONLY — NOT PUBLISHABLE ***"
	if [ "$(jq -r '.load_contaminated // false' "$m")" = true ]; then
		echo "  The machine was BUSY during this run (load/core above 0.5). Load"
		echo "  inflates every cell in the same direction, so the medians move with"
		echo "  it and MAD does not correct for it. Read the ORDERING, not the"
		echo "  timings, and re-run on an idle host before believing a magnitude."
	fi
	case "$host" in
	*-translated)
		echo "  This SHELL runs under binary translation, so the harness's own"
		echo "  per-sample overhead is emulated. It does NOT follow that the tools"
		echo "  are: a translated parent still runs an arm64-only child natively,"
		echo "  which is what the ARCH column reports per cell. Read that column"
		echo "  rather than inferring emulation from this line."
		;;
	*) echo "  host_class is not the official publication locus (H-10)." ;;
	esac
fi

echo
# Arch heterogeneity check: comparing a native cell against an emulated one
# reports the emulator as a performance difference.
declare -A arch_seen=()
for cell in "$RUN_DIR"/*.json; do
	[ "$(basename "$cell")" = "manifest.json" ] && continue
	[ "$(jq -r '.status' "$cell")" = "ok" ] || continue
	a="$(jq -r '.tool_arch_class // "unknown"' "$cell")"
	arch_seen["$a"]=1
done
if [ "${#arch_seen[@]}" -gt 1 ]; then
	echo
	echo "  *** NON-COMPARABLE: tool architectures differ ***"
	echo "  Cells in this run ran as: ${!arch_seen[*]}"
	echo "  A native cell against an emulated one measures the emulator, not the"
	echo "  tool. Same disqualification tier as host translation."
fi
for a in "${!arch_seen[@]}"; do
	case "$a" in
	native-*) ;;
	*)
		echo
		echo "  *** ARCH NOT CONFIRMED NATIVE: $a ***"
		echo "  A universal binary's chosen slice is not observable from outside the"
		echo "  process, and one launched from a translated parent does not reliably"
		echo "  pick the native slice. Treat these cells as unverified."
		;;
	esac
done

echo
printf '  %-10s %-12s %-7s %10s %10s %12s %10s %9s %-28s\n' \
	TOOL MODE SYNTAX MEDIAN_S REL_MAD% MB/S STMT/S PEAK_RSS ARCH
printf '  %s\n' "-------------------------------------------------------------------------------------"

for cell in "$RUN_DIR"/*.json; do
	[ "$(basename "$cell")" = "manifest.json" ] && continue
	status=$(jq -r '.status' "$cell")
	tool=$(jq -r '.tool' "$cell")
	mode=$(jq -r '.mode' "$cell")
	syntax=$(jq -r '.syntax' "$cell")
	if [ "$status" != "ok" ]; then
		printf '  %-10s %-12s %-7s %s\n' "$tool" "$mode" "$syntax" "FAILED"
		continue
	fi
	printf '  %-10s %-12s %-7s %10.4f %9.1f%% %12.2f %10d %8.1fM %-28s\n' \
		"$tool" "$mode" "$syntax" \
		"$(jq -r '.wall_seconds.median' "$cell")" \
		"$(jq -r '.wall_seconds.rel_mad_pct' "$cell")" \
		"$(jq -r '.mb_per_second' "$cell")" \
		"$(jq -r '.statements_per_second' "$cell")" \
		"$(jq -r '.peak_rss_bytes / 1048576' "$cell")" \
		"$(jq -r '.tool_arch_class // "unknown"' "$cell")"
done

echo
echo "  Reading notes"
echo "  -------------"
echo "  * Turtle and N-Triples rows are SEPARATE measurements and must not be"
echo "    averaged or quoted interchangeably. Conflating them is the error that"
echo "    makes the lane's two most-cited benchmarks disagree about raptor by ~6x."
echo "  * riot check_false / check_true are the same tool with validation off and"
echo "    on. Our column currently performs NO IRI validation, so it is comparable"
echo "    with check_false only. That asymmetry is disclosed, not averaged away."
echo "  * OUR VALIDATE CELL becomes fillable at WAVE-3 INTEGRATION, when the"
echo "    harness runs the integrated binary. At that point the PRIMARY comparison"
echo "    flips to riot check_true vs our validate (per H-8), and today's"
echo "    check_false pairing becomes the disclosed secondary. The lock already"
echo "    carries the column so the matrix is never quietly asymmetric."
echo "  * rapper is dormant-since-2023 and carried for historical continuity."
echo "  * ARCH is the class the tool's executable actually runs as. Only"
echo "    native-<host arch> cells are comparable to each other; a translated or"
echo "    universal-indeterminate cell measures partly the emulator. riot is a"
echo "    script over a universal JVM, so its slice is not observable from"
echo "    outside — stated rather than assumed."
echo "  * REL_MAD% is the spread of the bulk of the samples. Above ~5%, treat the"
echo "    cell as noise and re-run on a quieter host."
echo
