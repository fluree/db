#!/usr/bin/env node
// Node smoke harness for the wasm probe.
//
// Builds the probe with wasm-pack, instantiates it under Node, drives
// probe_start + probe_poll to completion, and exits 0 ONLY when the probe's
// own future resolved Ok (poll code 1) — the positive ran-marker. "Didn't
// crash" is never a pass: Pending exhaustion, Err, and traps all exit 1.
//
//   node run-node.mjs             # dev profile
//   node run-node.mjs --release   # release profile (wasm-opt skipped; see Cargo.toml)
//
// Requires: wasm-pack (which fetches the matching wasm-bindgen CLI), the
// wasm32-unknown-unknown target, and the repo's .cargo/config.toml wasm
// toolchain (clang for zstd, getrandom backend rustflags).

import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const profile = process.argv[2] ?? '--dev';
if (!['--dev', '--release'].includes(profile)) {
  console.error('usage: node run-node.mjs [--dev|--release]');
  process.exit(2);
}
const outDir = join(here, profile === '--release' ? 'pkg-node-release' : 'pkg-node-dev');

console.log(`[harness] wasm-pack build ${profile} --target nodejs --out-dir ${outDir}`);
execFileSync('wasm-pack', ['build', profile, '--target', 'nodejs', '--out-dir', outDir], {
  cwd: here,
  stdio: 'inherit',
});

// wasm-pack's nodejs glue re-exports only #[wasm_bindgen] items; the probe's
// entry points are deliberately #[no_mangle] extern "C" (so plain cargo
// builds stay measurable with zero JS glue). Append a raw-exports handle to
// the generated module (idempotent) to reach them.
const glue = join(outDir, 'fluree_wasm_probe.js');
if (!readFileSync(glue, 'utf8').includes('module.exports.__raw')) {
  writeFileSync(glue, readFileSync(glue, 'utf8') + '\nmodule.exports.__raw = wasm;\n');
}

const require = createRequire(import.meta.url);
const probe = require(glue);
console.log('[harness] module instantiated');

probe.__raw.probe_start();
console.log('[harness] probe_start returned (no trap at builder/init)');

// The probe future is polled manually with a noop waker; the memory-ledger
// path performs no external I/O, so a bounded poll budget is plenty.
const POLL_BUDGET = 1_000_000;
let rc = 0;
let polls = 0;
for (; polls < POLL_BUDGET; polls++) {
  rc = probe.__raw.probe_poll();
  if (rc !== 0) break;
}
console.log(`[harness] probe_poll -> ${rc} after ${polls} poll(s)`);

if (rc === 1) {
  console.log(
    '[harness] PASS: end-to-end Ok (build_memory -> create_ledger -> insert -> SPARQL query)'
  );
  process.exit(0);
}
console.error(
  rc === -1
    ? '[harness] FAIL: probe future returned Err'
    : rc === -2
      ? '[harness] FAIL: probe_poll called before probe_start'
      : rc === 0
        ? `[harness] FAIL: still Pending after ${POLL_BUDGET} polls`
        : `[harness] FAIL: unexpected poll code ${rc}`
);
process.exit(1);
