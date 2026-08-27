#!/usr/bin/env node
// Browser smoke test of the SHIPPED package: main-thread proxy → worker →
// wasm, in a real Chrome, over plain HTTP with no special headers.
//
//   node scripts/smoke-browser.mjs            # after `node scripts/build.mjs`
//   node scripts/smoke-browser.mjs --packed   # smoke the `npm pack` tarball
//   CHROME=/path/to/chrome node scripts/smoke-browser.mjs
//
// Drives headless Chrome over the DevTools protocol (no puppeteer/webdriver
// dependency — node ≥22 has a global WebSocket). Loads demo/smoke.html, which
// sets `window.__smoke` to a promise; the test awaits it via
// Runtime.evaluate and exits 0 ONLY when the page reports `status: "pass"`
// with the expected bound rows AND the crash/recycle phase's typed outcomes —
// positive ran-markers. Timeouts, thrown errors, and a missing marker all
// exit 1.
//
// `--packed` smokes what an install actually receives: `npm pack`, extract
// the tarball, overlay demo/ (not shipped), and serve THAT — so a `files`
// allowlist regression (e.g. the .wasm dropped from the tarball) fails here
// instead of in consumers' installs.

import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { awaitPageMarker, cdpConnect, findChrome, launchChrome, openPage } from "./chrome.mjs";
import { packageRoot, startServer } from "./serve.mjs";

const TIMEOUT_MS = Number(process.env.SMOKE_TIMEOUT_MS ?? 180_000);
const packed = process.argv.includes("--packed");

for (const f of ["dist/index.js", "dist/worker.js", "pkg/fluree_db_wasm_bg.wasm"]) {
  if (!existsSync(join(packageRoot, f))) {
    console.error(`missing ${f} — run \`node scripts/build.mjs\` first`);
    process.exit(2);
  }
}

let serveRoot = packageRoot;
let packDir = null;
if (packed) {
  packDir = mkdtempSync(join(tmpdir(), "fluree-wasm-pack-"));
  const packOut = spawnSync("npm", ["pack", "--pack-destination", packDir], {
    cwd: packageRoot,
    encoding: "utf8",
  });
  if (packOut.status !== 0) {
    console.error(`npm pack failed: ${packOut.stderr}`);
    process.exit(2);
  }
  const tarball = packOut.stdout.trim().split("\n").pop();
  const untar = spawnSync("tar", ["-xzf", join(packDir, tarball), "-C", packDir]);
  if (untar.status !== 0) {
    console.error("tarball extraction failed");
    process.exit(2);
  }
  serveRoot = join(packDir, "package");
  // The demo pages are deliberately not shipped; overlay them on the
  // extracted tarball so the smoke drives exactly the published files.
  cpSync(join(packageRoot, "demo"), join(serveRoot, "demo"), { recursive: true });
  console.log(`smoking packed tarball: ${tarball}`);
}

const { server, url } = await startServer(serveRoot, 0);
const chromeBin = findChrome();
if (!chromeBin) {
  console.error("no Chrome/Chromium found; set CHROME=/path/to/binary");
  process.exit(2);
}
const chrome = launchChrome(chromeBin);

function cleanup() {
  chrome.kill();
  server.close();
  if (packDir) {
    try {
      rmSync(packDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    } catch { /* leftover temp dir is harmless */ }
  }
}

const wsUrl = await chrome.wsUrl.catch((e) => {
  console.error(e.message);
  cleanup();
  process.exit(1);
});
const cdp = await cdpConnect(wsUrl);

let exit = 1;
const deadline = setTimeout(() => {
  console.error(`FAIL: no result within ${TIMEOUT_MS} ms`);
  cleanup();
  process.exit(1);
}, TIMEOUT_MS);

try {
  const session = await openPage(cdp, `${url}/demo/smoke.html`);
  const r = await awaitPageMarker(cdp, session, "__smoke");
  console.log("page report:", JSON.stringify(r, null, 2));
  const names = Array.isArray(r?.names) ? r.names.join(",") : "";
  const phase1 = r?.status === "pass" && r.rows === 2 && names === "Alice,Bob" && r.ask === true;
  // Phase 2 (H-4/H-5): the deliberate crash was typed and fatal, the fresh
  // engine came back, the stale pre-crash snapshot handle was refused typed
  // (never answered from the fresh engine), and its release() left the fresh
  // engine's snapshot intact.
  const phase2 =
    r?.crashCode === "engine_crashed" &&
    r?.crashFatal === true &&
    r?.staleCode === "not_found" &&
    r?.postAlive === true;
  // Phase 3 (A1 peer boundary, serverless): connect() resolved with the
  // token supplied over the EVENT channel (getToken called exactly once,
  // reason "connect" — init itself carries no credential), and an
  // unreachable remote failed typed and non-fatal.
  // Phase 1b (A4): the live subscription primed with the initial 2 rows and
  // delivered exactly one further update (3 rows) after the commit.
  const phaseLive =
    Array.isArray(r?.live?.rows) && r.live.rows.join(",") === "2,3";
  const phase3 =
    r?.peer?.version === r?.version &&
    Array.isArray(r?.peer?.tokenReasons) &&
    r.peer.tokenReasons.join(",") === "connect" &&
    typeof r?.peer?.ledgerCode === "string" &&
    r.peer.ledgerCode.length > 0 &&
    r.peer.ledgerCode !== "engine_crashed" &&
    r.peer.ledgerFatal !== true;
  if (phase1 && phaseLive && phase2 && phase3) {
    console.log(`PASS: create → insert → query returned ${r.rows} rows via ${r.transport} transport ` +
      `(engine ${r.version}; init ${r.timings.init} ms, insert ${r.timings.insert} ms, query ${r.timings.query} ms); ` +
      `crash/recycle: typed ${r.crashCode}, stale snapshot → ${r.staleCode}, ` +
      `engine back after ${r.recycleAttempts} attempt(s); ` +
      `live: primed→updated rows ${r.live.rows.join("→")}; ` +
      `peer boundary: token via event channel (${r.peer.tokenReasons.join(",")}), ` +
      `unreachable remote → typed ${r.peer.ledgerCode}`);
    exit = 0;
  } else if (!phase1) {
    console.error("FAIL: page did not report the expected rows");
  } else if (!phaseLive) {
    console.error("FAIL: live subscription did not deliver primed→updated results");
  } else if (!phase2) {
    console.error("FAIL: crash/recycle phase did not report the expected typed outcomes");
  } else {
    console.error("FAIL: peer-boundary phase did not report the expected typed outcomes");
  }
} catch (err) {
  console.error("FAIL:", err.message ?? err);
} finally {
  clearTimeout(deadline);
  cdp.close();
  cleanup();
}
process.exit(exit);
