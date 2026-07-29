#!/usr/bin/env bash
# Verify (and report on) the pinned competitor tools.
#
# This does NOT install anything by itself. Building Jena, serd, raptor and
# oxigraph from source has four different build systems and three different
# dependency sets, and a script that half-installs them produces a matrix with
# silently missing columns. Instead it reports, per tool, exactly one of:
#
#   ok        installed and the version matches the lock
#   MISMATCH  installed but a DIFFERENT version — the matrix will refuse it
#   absent    not installed — here is the pinned artifact and how to build it
#
# `--verify-artifacts` additionally downloads each pinned source artifact and
# checks its sha256, which is the part that has to be reproducible for a
# publishable run.

set -euo pipefail
# shellcheck source=lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

require_cmd jq "Install jq to read the tool lock."
ensure_dirs

verify_artifacts=0
[ "${1:-}" = "--verify-artifacts" ] && verify_artifacts=1

printf '\ntool status against %s\n\n' "$(basename "$TOOLS_LOCK")" >&2

exit_code=0
mapfile -t tools < <(jq -r '.tools | keys[]' "$TOOLS_LOCK")

for tool in "${tools[@]}"; do
	pinned="$(jq -r --arg t "$tool" '.tools[$t].version' "$TOOLS_LOCK")"
	role="$(jq -r --arg t "$tool" '.tools[$t].role // "competitor"' "$TOOLS_LOCK")"

	set +e
	verify_tool_version "$tool"
	rc=$?
	set -e

	case $rc in
	0) printf '  %-10s %-10s ok        (%s)\n' "$tool" "$pinned" "$role" >&2 ;;
	1)
		printf '  %-10s %-10s MISMATCH  matrix will REFUSE this tool\n' "$tool" "$pinned" >&2
		exit_code=1
		;;
	2)
		printf '  %-10s %-10s absent\n' "$tool" "$pinned" >&2
		jq -r --arg t "$tool" '"                 src: " + (.tools[$t].source_url // "n/a")' "$TOOLS_LOCK" >&2
		jq -r --arg t "$tool" '"                 via: " + (.tools[$t].install_cmd // "n/a")' "$TOOLS_LOCK" >&2
		req="$(jq -r --arg t "$tool" '.tools[$t].runtime_requirement // empty' "$TOOLS_LOCK")"
		[ -n "$req" ] && printf '                 req: %s\n' "$req" >&2
		;;
	esac
done

if [ "$verify_artifacts" = 1 ]; then
	printf '\nverifying pinned artifacts (sha256)\n\n' >&2
	require_cmd curl
	for tool in "${tools[@]}"; do
		url="$(jq -r --arg t "$tool" '.tools[$t].source_url // empty' "$TOOLS_LOCK")"
		want="$(jq -r --arg t "$tool" '.tools[$t].sha256 // empty' "$TOOLS_LOCK")"
		if [ -z "$want" ] || [ "$want" = "null" ]; then
			printf '  %-10s no pinned hash (reference row) — skipped\n' "$tool" >&2
			continue
		fi
		local_file="$TOOLS_DIR/$(basename "$url")"
		if [ ! -f "$local_file" ]; then
			info "downloading $tool …"
			curl -sSfL --max-time 900 -o "$local_file" "$url" ||
				die "download failed for $tool: $url"
		fi
		got="$(sha256_of "$local_file")"
		if [ "$got" = "$want" ]; then
			printf '  %-10s sha256 ok\n' "$tool" >&2
		else
			printf '  %-10s sha256 MISMATCH\n    want %s\n    got  %s\n' \
				"$tool" "$want" "$got" >&2
			exit_code=1
		fi
	done
fi

printf '\n' >&2
if [ "$exit_code" != 0 ]; then
	printf 'one or more tools disagree with the lock — see above.\n' >&2
	printf 'The matrix refuses mismatched tools rather than quietly comparing\n' >&2
	printf 'against an unrecorded build; that is the whole point of the lock.\n' >&2
fi
exit "$exit_code"
