#!/bin/sh
# Archiver shim paired with scripts/wasm-clang.sh (see rationale there).
for cand in /opt/homebrew/opt/llvm/bin/llvm-ar /usr/local/opt/llvm/bin/llvm-ar; do
    if [ -x "$cand" ]; then
        exec "$cand" "$@"
    fi
done
if command -v llvm-ar >/dev/null 2>&1; then
    exec llvm-ar "$@"
fi
exec ar "$@"
