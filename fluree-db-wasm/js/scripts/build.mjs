#!/usr/bin/env node
// Build the npm package: engine (.wasm) + wasm-bindgen glue + TypeScript.
//
//   node scripts/build.mjs                 # wasm-release profile + wasm-opt
//   node scripts/build.mjs --dev           # cargo dev profile, no wasm-opt (fast iteration)
//   node scripts/build.mjs --profile NAME  # any cargo profile
//   WASM_OPT=0 node scripts/build.mjs      # skip wasm-opt
//
// Why not `wasm-pack build`: wasm-pack can only select cargo's `dev`/`release`
// profiles, and the workspace `release` profile is tuned for native binaries
// (opt-level 3) — which we must not change. The browser artifact builds with
// the root `[profile.wasm-release]` (inert for native builds), so this script
// drives cargo + the `wasm-bindgen` CLI directly. wasm-pack is still the
// runner for the browser tests (`wasm-pack test`).
//
// Steps: cargo build → wasm-bindgen (--target web, emits pkg/) → wasm-opt →
// tsc → size report (raw / gzip / brotli, via node's zlib so the numbers are
// reproducible without external tools).

import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { brotliCompressSync, constants as zc, gzipSync } from "node:zlib";

const here = dirname(fileURLToPath(import.meta.url));
const pkgDir = resolve(here, "..");           // fluree-db-wasm/js
const crateDir = resolve(pkgDir, "..");       // fluree-db-wasm
const repoRoot = resolve(crateDir, "..");     // workspace root
const outDir = join(pkgDir, "pkg");
const CRATE = "fluree-db-wasm";
const ARTIFACT = "fluree_db_wasm";

const args = process.argv.slice(2);
let profile = "wasm-release";
let dev = false;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--dev") { dev = true; profile = "dev"; }
  else if (args[i] === "--profile") { profile = args[++i]; }
  else { console.error(`unknown arg ${args[i]}`); process.exit(2); }
}
const wantOpt = !dev && process.env.WASM_OPT !== "0";

function run(cmd, cmdArgs, opts = {}) {
  console.log(`\n$ ${cmd} ${cmdArgs.join(" ")}`);
  const r = spawnSync(cmd, cmdArgs, { stdio: "inherit", ...opts });
  if (r.error) throw r.error;
  if (r.status !== 0) process.exit(r.status ?? 1);
}

function which(bin) {
  const r = spawnSync(bin, ["--version"], { encoding: "utf8" });
  return r.error ? null : (r.stdout || r.stderr || "").trim();
}

// 1. cargo build. The repo's .cargo/config.toml supplies the wasm32 C
//    toolchain shims and the getrandom cfg; nothing here overrides RUSTFLAGS.
const profileArgs = profile === "dev" ? [] : ["--profile", profile];
run("cargo", ["build", "--target", "wasm32-unknown-unknown", "-p", CRATE, ...profileArgs], {
  cwd: repoRoot,
});
const profileDir = profile === "dev" ? "debug" : profile;
const wasmIn = join(repoRoot, "target", "wasm32-unknown-unknown", profileDir, `${ARTIFACT}.wasm`);
if (!existsSync(wasmIn)) {
  console.error(`expected cargo artifact at ${wasmIn}`);
  process.exit(1);
}

// 2. wasm-bindgen CLI. Must match the wasm-bindgen crate version in
//    Cargo.lock exactly (the ABI between glue and module is version-locked).
const lock = readFileSync(join(repoRoot, "Cargo.lock"), "utf8");
const locked = /name = "wasm-bindgen"\nversion = "([^"]+)"/.exec(lock)?.[1];
const cliVersion = which("wasm-bindgen")?.replace(/^wasm-bindgen\s+/, "");
if (!cliVersion) {
  console.error(`wasm-bindgen CLI not found. Install the locked version:\n` +
    `  cargo install wasm-bindgen-cli --version ${locked} --locked`);
  process.exit(1);
}
if (locked && cliVersion !== locked) {
  console.error(`wasm-bindgen CLI ${cliVersion} != Cargo.lock wasm-bindgen ${locked}.\n` +
    `  cargo install wasm-bindgen-cli --version ${locked} --locked --force`);
  process.exit(1);
}
mkdirSync(outDir, { recursive: true });
run("wasm-bindgen", [
  "--target", "web",
  "--typescript",
  "--out-dir", outDir,
  "--out-name", ARTIFACT,
  wasmIn,
]);
const wasmOut = join(outDir, `${ARTIFACT}_bg.wasm`);

// 3. wasm-opt. Rust ≥1.82 targets wasm32 with bulk-memory, sign-ext,
//    nontrapping-fptoint, mutable-globals and reference-types enabled, and
//    wasm-bindgen emits multivalue/reference-types glue; every feature the
//    module already uses must be enabled or wasm-opt rejects the input.
if (wantOpt) {
  const level = process.env.WASM_OPT_LEVEL ?? "-Os";
  if (which("wasm-opt")) {
    run("wasm-opt", [
      level,
      "--enable-bulk-memory",
      "--enable-sign-ext",
      "--enable-nontrapping-float-to-int",
      "--enable-mutable-globals",
      "--enable-reference-types",
      "--enable-multivalue",
      wasmOut,
      "-o", wasmOut,
    ]);
  } else {
    console.warn("wasm-opt not found on PATH — shipping the un-optimized module (install binaryen)");
  }
}

// 4. TypeScript → dist/. Prefer the package-local install; fall back to a
//    global tsc so a checkout without `npm install` still builds.
const localTsc = join(pkgDir, "node_modules", ".bin", "tsc");
run(existsSync(localTsc) ? localTsc : "tsc", ["-p", join(pkgDir, "tsconfig.json")]);

// 5. Size report.
const raw = readFileSync(wasmOut);
const gz = gzipSync(raw, { level: 9 });
const br = brotliCompressSync(raw, {
  params: { [zc.BROTLI_PARAM_QUALITY]: 11, [zc.BROTLI_PARAM_SIZE_HINT]: raw.length },
});
const mb = (n) => (n / 1_048_576).toFixed(2);
const glue = statSync(join(outDir, `${ARTIFACT}.js`)).size;
const report = {
  profile,
  wasmOpt: wantOpt ? (process.env.WASM_OPT_LEVEL ?? "-Os") : "skipped",
  wasmBytes: raw.length,
  wasmGzip: gz.length,
  wasmBrotli: br.length,
  glueBytes: glue,
};
writeFileSync(join(outDir, "size-report.json"), JSON.stringify(report, null, 2) + "\n");
console.log(`
${ARTIFACT}_bg.wasm (${profile}${wantOpt ? ", wasm-opt " + report.wasmOpt : ""}):
  raw     ${mb(raw.length)} MB  (${raw.length} bytes)
  gzip -9 ${mb(gz.length)} MB
  brotli  ${mb(br.length)} MB
  glue    ${(glue / 1024).toFixed(1)} KB (${ARTIFACT}.js)
package: ${pkgDir}
`);
