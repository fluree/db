# Sourced (off camera) by tour.tape. Not meant to be run on its own.
#
# - Works in a throwaway /tmp/fluree-tour so your own .fluree project is untouched.
# - Writes movies.rq, the query file the tour shows with `cat`.
# - Wraps `fluree` to drop the "novelty is NKB ... run `fluree index`" notice from
#   stderr. It's index-maintenance advice, not an error, and is noise on a
#   two-commit demo ledger. Exit codes and every other line pass through unchanged.
# - Sets a brand-colored prompt.

rm -rf /tmp/fluree-tour && mkdir -p /tmp/fluree-tour && cd /tmp/fluree-tour || return

cat > movies.rq <<'RQ'
PREFIX s: <http://schema.org/>
SELECT ?title ?genre
WHERE { ?m s:name ?title . OPTIONAL { ?m s:genre ?genre } }
RQ

fluree() {
  local rc
  { command fluree "$@" 2>&1 1>&3 3>&- | { grep -v 'novelty is' || true; } >&2
    rc=${PIPESTATUS[0]}; } 3>&1
  return "$rc"
}

PS1=$'\\[\e[38;2;19;198;255m\\]'"❯"$'\\[\e[0m\\] '
PS2=$'\\[\e[38;2;106;120;147m\\]'"·"$'\\[\e[0m\\] '
clear
