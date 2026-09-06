#!/bin/sh
# C compiler shim for wasm32-unknown-unknown C dependencies (zstd-sys).
# Apple clang has no WebAssembly backend, so on macOS prefer Homebrew LLVM;
# everywhere else (Linux dev boxes, CI) the system clang targets wasm32 fine.
for cand in /opt/homebrew/opt/llvm/bin/clang /usr/local/opt/llvm/bin/clang; do
    if [ -x "$cand" ]; then
        exec "$cand" "$@"
    fi
done
exec clang "$@"
