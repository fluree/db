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
printf '  runs        %s per cell, %s warmup discarded, %s clock\n' \
	"$(jq -r '.runs_per_cell' "$m")" "$(jq -r '.warmup_discarded' "$m")" "$(jq -r '.clock' "$m")"
printf '  eligible    %s\n' "$(jq -r '.tools_eligible | join(", ")' "$m")"
refused=$(jq -r '.tools_refused | join(", ")' "$m")
[ -n "$refused" ] && printf '  REFUSED     %s\n' "$refused"

if [ "$publishable" != "true" ]; then
	echo
	echo "  *** DEV-SIGNAL ONLY — NOT PUBLISHABLE ***"
	case "$host" in
	*-translated)
		echo "  This shell runs under binary translation. uname reports the emulated"
		echo "  architecture and the harness itself is emulated, so these are not"
		echo "  measurements of the tools alone."
		;;
	*) echo "  host_class is not the official publication locus (H-10)." ;;
	esac
fi

echo
printf '  %-10s %-12s %-7s %10s %10s %12s %10s %9s\n' \
	TOOL MODE SYNTAX MEDIAN_S REL_MAD% MB/S STMT/S PEAK_RSS
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
	printf '  %-10s %-12s %-7s %10.4f %9.1f%% %12.2f %10d %8.1fM\n' \
		"$tool" "$mode" "$syntax" \
		"$(jq -r '.wall_seconds.median' "$cell")" \
		"$(jq -r '.wall_seconds.rel_mad_pct' "$cell")" \
		"$(jq -r '.mb_per_second' "$cell")" \
		"$(jq -r '.statements_per_second' "$cell")" \
		"$(jq -r '.peak_rss_bytes / 1048576' "$cell")"
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
echo "  * rapper is dormant-since-2023 and carried for historical continuity."
echo "  * REL_MAD% is the spread of the bulk of the samples. Above ~5%, treat the"
echo "    cell as noise and re-run on a quieter host."
echo
