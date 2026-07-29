#!/usr/bin/env bash
# Shared plumbing for the Tier-2 conversion harness.
#
# Sourced, never executed. Every function here either succeeds or kills the run:
# a benchmark that silently degrades is worse than one that refuses to start,
# because the number still gets written down.

# The paths and helpers below are this file's INTERFACE — every one is consumed
# by a script that sources it. shellcheck cannot see across the `.` boundary, so
# without this it reports each as unused. File-level directive, which must
# precede the first command.
# shellcheck disable=SC2034
set -euo pipefail

BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$BENCH_ROOT/../.." && pwd)"
TOOLS_DIR="$BENCH_ROOT/.tools"
CORPORA_DIR="$BENCH_ROOT/.corpora"
RESULTS_DIR="$BENCH_ROOT/.results"
TOOLS_LOCK="$BENCH_ROOT/tools.lock.json"
CORPORA_LOCK="$BENCH_ROOT/corpora.lock.json"

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

warn() {
	printf 'warning: %s\n' "$*" >&2
}

info() {
	printf '  %s\n' "$*" >&2
}

# Refuse to run on a bash too old for the features the harness depends on.
#
# macOS still ships bash 3.2 as /bin/bash, and the failure mode without this is
# not a clean error: `declare -A` and `mapfile` fail in ways that leave the
# harness half-working, and EPOCHREALTIME simply expands to nothing, which
# would produce timings of zero rather than a complaint.
require_bash() {
	local want="$1" why="${2:-}"
	if [ "${BASH_VERSINFO[0]:-0}" -lt "$want" ]; then
		die "bash $want+ required, found ${BASH_VERSION:-unknown}.${why:+ $why.}
  macOS ships bash 3.2 as /bin/bash; install a current one (brew install bash)
  and run the harness under it."
	fi
}

require_cmd() {
	local cmd="$1" hint="${2:-}"
	command -v "$cmd" >/dev/null 2>&1 && return 0
	if [ -n "$hint" ]; then
		die "$cmd not found. $hint"
	fi
	die "$cmd not found."
}

# GNU time, which is the only portable source of peak RSS.
#
# BSD /usr/bin/time cannot report it, so there is no silent fallback: a missing
# gtime fails the run with the install line rather than quietly dropping the
# memory column, which is one of the few genuinely differentiating measurements
# we have (§6b F7).
gnu_time_bin() {
	if command -v gtime >/dev/null 2>&1; then
		printf 'gtime'
		return 0
	fi
	# Some Linux distributions ship it as /usr/bin/time with -v support.
	if /usr/bin/time -v true >/dev/null 2>&1; then
		printf '/usr/bin/time'
		return 0
	fi
	die "GNU time not found — peak RSS cannot be measured without it.
    macOS:         brew install gnu-time      (installs as 'gtime')
    Debian/Ubuntu: apt-get install time
    Fedora/RHEL:   dnf install time
  BSD /usr/bin/time is NOT a substitute: it does not report maximum resident set size."
}

# Whether this shell is running under binary translation (Rosetta 2).
#
# This is not a curiosity. `uname -m` in a translated shell reports the
# EMULATED architecture, so an Apple-silicon machine stamps itself
# `darwin-x86_64` and every result file claims to have been measured on
# hardware that was not involved. Worse, anything the harness itself times —
# the shell, the clock calls — is running emulated, so the numbers are not
# measurements of the tools alone.
#
# Detected rather than assumed, and recorded rather than corrected: a
# translated run is disqualified, not adjusted.
host_is_translated() {
	[ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = "1" ]
}

# The true machine architecture, seeing through translation.
host_arch() {
	if host_is_translated; then
		# `arch -arm64 uname -m` asks the kernel for the native answer.
		arch -arm64 uname -m 2>/dev/null || printf 'arm64'
	else
		uname -m
	fi
}

# host_class stamps every result file. Publication tooling refuses anything that
# is not the official locus, so a dev-machine number can be produced freely and
# still cannot be published by accident (§6b F7).
host_class() {
	local os
	os="$(uname -s | tr '[:upper:]' '[:lower:]')"
	if host_is_translated; then
		printf '%s-%s-translated' "$os" "$(host_arch)"
	else
		printf '%s-%s' "$os" "$(host_arch)"
	fi
}

host_is_publishable() {
	# A translated shell is never a measurement environment, whatever the
	# hardware underneath it.
	host_is_translated && return 1
	# The official locus is a single Linux instance class, pending AJ (H-10).
	# Everything else — every developer machine, this one included — is
	# dev-signal only.
	case "$(host_class)" in
	linux-x86_64) [ "${FLUREE_BENCH_OFFICIAL_LOCUS:-0}" = "1" ] ;;
	*) return 1 ;;
	esac
}

# 1-minute load average, and cores, so a run can say whether the box was busy.
#
# Added after a re-run measured serdi at 21ms against a directly-measured
# ground truth of 10ms. The clock fix was correct; the residual was that
# sibling build jobs had the load average at 10 on a 16-core box. A benchmark
# harness that cannot tell "this tool is slow" from "this machine was busy" is
# not measuring the tool, and no amount of median-and-MAD repairs it — load
# inflates every sample in the same direction, so the median moves with it.
host_load1() {
	# `uptime` output differs across platforms; the load triple is always the
	# last three fields.
	uptime | awk -F'load average[s]*:' '{gsub(/^[ \t]+/,"",$2); split($2,l,","); gsub(/ /,"",l[1]); print l[1]}'
}

host_cores() {
	if command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu >/dev/null 2>&1; then
		sysctl -n hw.ncpu
	elif command -v nproc >/dev/null 2>&1; then
		nproc
	else
		echo 1
	fi
}

# Load per core, to 3dp. Above ~0.5 the box is doing something else and the
# numbers are about the box, not the tools.
host_load_per_core() {
	awk -v l="$(host_load1)" -v c="$(host_cores)" 'BEGIN{printf "%.3f", (c>0)?l/c:l}'
}

host_is_quiet() {
	awk -v r="$(host_load_per_core)" 'BEGIN{exit (r < 0.5) ? 0 : 1}'
}

sha256_of() {
	local file="$1"
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$file" | cut -d' ' -f1
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$file" | cut -d' ' -f1
	else
		die "no sha256sum or shasum available"
	fi
}

# Verify an installed tool against tools.lock.json.
#
# Hard-fails on mismatch. The whole reason this lane's published numbers rot is
# that nobody records which build produced them; accepting "close enough" here
# would reproduce the exact failure the lock exists to prevent.
verify_tool_version() {
	local tool="$1"
	local probe pattern actual

	pattern="$(jq -r --arg t "$tool" '.tools[$t].version_pattern // empty' "$TOOLS_LOCK")"
	[ -n "$pattern" ] || die "tool '$tool' is not in $TOOLS_LOCK"

	mapfile -t probe < <(jq -r --arg t "$tool" '.tools[$t].version_probe[]' "$TOOLS_LOCK")
	command -v "${probe[0]}" >/dev/null 2>&1 || return 2 # interpreter/binary absent

	# A failing probe means "not installed", not "wrong version". The two are
	# reported differently and only one of them is a problem: `node` exists but
	# the n3 module is not there, which is absence, not a mismatch.
	local probe_rc=0
	actual="$("${probe[@]}" 2>&1 | head -5 | tr -d '\r')" || probe_rc=$?
	[ "$probe_rc" -eq 0 ] || return 2

	if printf '%s' "$actual" | grep -qE "$pattern"; then
		return 0
	fi

	local want
	want="$(jq -r --arg t "$tool" '.tools[$t].version' "$TOOLS_LOCK")"
	warn "$tool version mismatch: lock pins $want, found:"
	printf '%s\n' "$actual" | sed 's/^/      /' >&2
	# Hand the caller what was actually found, so a report can name the build
	# that was refused rather than re-print the pin it failed to match.
	VERIFY_TOOL_ACTUAL="$actual"
	return 1
}

# Median of numbers on stdin. Delegates to the Rust helper so that median and
# MAD come from one tested implementation rather than two shell approximations.
require_harness_bin() {
	local bin="$BENCH_ROOT/harness/target/release/bench-harness"
	if [ ! -x "$bin" ]; then
		die "harness helper not built. Run:
    (cd $BENCH_ROOT/harness && cargo build --release)"
	fi
	printf '%s' "$bin"
}

ensure_dirs() {
	mkdir -p "$TOOLS_DIR/bin" "$CORPORA_DIR" "$RESULTS_DIR"
}
