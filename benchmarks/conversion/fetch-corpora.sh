#!/usr/bin/env bash
# Fetch and hash-verify the real corpora into .corpora/ (gitignored).
#
# Nothing is vendored. Two reasons, and only one of them is size:
#
#   - dbpedia-live is CC-BY-SA. Committing it, or a normalized copy, or a
#     trimmed sample of it, puts a share-alike obligation on this repo.
#   - DBLP republishes in place, so a committed copy would silently become a
#     different corpus from the one the URL serves.
#
# Instead: fetch, record the hash into corpora.local.json on first download,
# and hard-fail on any later mismatch. That gives one internally consistent,
# re-runnable matrix — which is what a comparison needs — without pretending
# upstream froze for us. Self-archive the recorded pair before a published run.

set -euo pipefail
# shellcheck source=lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

require_cmd jq
require_cmd curl
ensure_dirs

LOCAL_LOCK="$BENCH_ROOT/corpora.local.json"
[ -f "$LOCAL_LOCK" ] || echo '{}' >"$LOCAL_LOCK"

usage() {
	cat >&2 <<'EOF'
usage: fetch-corpora.sh <corpus> [corpus ...]
       fetch-corpora.sh --list

Corpora are defined in corpora.lock.json. The synthetic corpus does not come
from here — it is generated locally by gen-synthetic.sh.
EOF
	exit 2
}

[ $# -gt 0 ] || usage

if [ "$1" = "--list" ]; then
	jq -r '.corpora | to_entries[] |
	       "  \(.key)\n      role: \(.value.role)\n      why:  \(.value.why)"' "$CORPORA_LOCK" >&2
	exit 0
fi

# Record-or-verify a hash. First sighting records; every later run compares.
record_or_verify() {
	local key="$1" file="$2" got want
	got="$(sha256_of "$file")"
	want="$(jq -r --arg k "$key" '.[$k] // empty' "$LOCAL_LOCK")"

	if [ -z "$want" ]; then
		local tmp
		tmp="$(mktemp)"
		jq --arg k "$key" --arg v "$got" '.[$k] = $v' "$LOCAL_LOCK" >"$tmp"
		mv "$tmp" "$LOCAL_LOCK"
		info "recorded $key sha256 $got"
		return 0
	fi

	if [ "$got" != "$want" ]; then
		die "corpus $key CHANGED since it was recorded.
    recorded $want
    now      $got
  Upstream republished. Every number measured against the old bytes is from a
  different corpus than every number measured against the new ones, and the
  matrix must not mix them. Either restore the archived copy, or delete the
  entry from corpora.local.json and re-measure EVERY cell."
	fi
	info "$key sha256 verified"
}

fetch_one() {
	local corpus="$1"
	jq -e --arg c "$corpus" '.corpora[$c]' "$CORPORA_LOCK" >/dev/null 2>&1 ||
		die "unknown corpus '$corpus' (try --list)"

	local license constraint
	license="$(jq -r --arg c "$corpus" '.corpora[$c].license' "$CORPORA_LOCK")"
	constraint="$(jq -r --arg c "$corpus" '.corpora[$c].license_constraint // empty' "$CORPORA_LOCK")"
	printf '\n%s  (%s)\n' "$corpus" "$license" >&2
	[ -n "$constraint" ] && printf '  !! %s\n' "$constraint" >&2

	local turtle_na
	turtle_na="$(jq -r --arg c "$corpus" '.corpora[$c].turtle_column // empty' "$CORPORA_LOCK")"
	if [ "$turtle_na" = "N/A" ]; then
		jq -r --arg c "$corpus" '"  turtle column: N/A — " + .corpora[$c].turtle_column_reason' \
			"$CORPORA_LOCK" >&2
	fi

	local syntaxes
	mapfile -t syntaxes < <(jq -r --arg c "$corpus" '.corpora[$c].files // {} | keys[]' "$CORPORA_LOCK")
	[ ${#syntaxes[@]} -gt 0 ] || {
		info "no fetchable files (generator-based corpus) — see corpora.lock.json"
		return 0
	}

	# The same-day rule: both halves of a pair are fetched in one invocation,
	# or the pair is not a pair.
	local same_day
	same_day="$(jq -r --arg c "$corpus" '.corpora[$c].same_day_requirement // empty' "$CORPORA_LOCK")"
	[ -n "$same_day" ] && printf '  note: %s\n' "$same_day" >&2

	for syn in "${syntaxes[@]}"; do
		local url dest
		url="$(jq -r --arg c "$corpus" --arg s "$syn" '.corpora[$c].files[$s].url' "$CORPORA_LOCK")"
		dest="$CORPORA_DIR/$(basename "$url")"

		if [ -f "$dest" ]; then
			info "$(basename "$dest") already present"
		else
			info "downloading $(basename "$dest") …"
			curl -sSfL --max-time 3600 -o "$dest.part" "$url" ||
				die "download failed: $url"
			mv "$dest.part" "$dest"
		fi
		record_or_verify "$corpus.$syn" "$dest"
	done
}

for corpus in "$@"; do
	fetch_one "$corpus"
done

printf '\nrecorded hashes: %s\n' "$LOCAL_LOCK" >&2
