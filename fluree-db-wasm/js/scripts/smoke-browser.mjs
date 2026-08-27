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

import { spawn, spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { packageRoot, startServer } from "./serve.mjs";

const TIMEOUT_MS = Number(process.env.SMOKE_TIMEOUT_MS ?? 180_000);
const packed = process.argv.includes("--packed");

function findChrome() {
  const candidates = [
    process.env.CHROME,
    process.env.CHROME_BIN,
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
  ].filter(Boolean);
  for (const c of candidates) {
    if (c.includes("/")) {
      if (existsSync(c)) return c;
    } else if (spawnSync("which", [c], { stdio: "ignore" }).status === 0) {
      // `which`, not a --version launch: on macOS, launching the Chrome
      // binary can hand off to a running instance and never exit.
      return c;
    }
  }
  return null;
}

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
const profileDir = mkdtempSync(join(tmpdir(), "fluree-wasm-smoke-"));
const chrome = spawn(
  chromeBin,
  [
    "--headless=new",
    "--disable-gpu",
    "--no-sandbox",
    "--no-first-run",
    "--disable-extensions",
    `--user-data-dir=${profileDir}`,
    "--remote-debugging-port=0",
    "about:blank",
  ],
  { stdio: ["ignore", "ignore", "pipe"] },
);

function cleanup() {
  try { chrome.kill("SIGKILL"); } catch { /* already gone */ }
  server.close();
  try {
    // The killed Chrome may still be flushing profile files; retry, and never
    // let cleanup decide the exit code.
    rmSync(profileDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  } catch { /* leftover temp dir is harmless */ }
  if (packDir) {
    try {
      rmSync(packDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    } catch { /* leftover temp dir is harmless */ }
  }
}

const wsUrl = await new Promise((resolve, reject) => {
  let buf = "";
  const t = setTimeout(() => reject(new Error("Chrome did not announce a DevTools endpoint")), 30_000);
  chrome.stderr.on("data", (d) => {
    buf += d.toString();
    const m = /DevTools listening on (ws:\/\/\S+)/.exec(buf);
    if (m) { clearTimeout(t); resolve(m[1]); }
  });
  chrome.on("error", (e) => { clearTimeout(t); reject(e); });
  chrome.on("exit", (code) => { clearTimeout(t); reject(new Error(`Chrome exited early (${code}): ${buf}`)); });
}).catch((e) => { console.error(e.message); cleanup(); process.exit(1); });

// Tiny CDP client.
const ws = new WebSocket(wsUrl);
await new Promise((resolve, reject) => { ws.onopen = resolve; ws.onerror = reject; });
let nextId = 1;
const waiting = new Map();
ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);
  if (msg.id && waiting.has(msg.id)) {
    const { resolve, reject } = waiting.get(msg.id);
    waiting.delete(msg.id);
    msg.error ? reject(new Error(JSON.stringify(msg.error))) : resolve(msg.result);
  }
};
function cdp(method, params = {}, sessionId) {
  const id = nextId++;
  ws.send(JSON.stringify({ id, method, params, sessionId }));
  return new Promise((resolve, reject) => waiting.set(id, { resolve, reject }));
}

let exit = 1;
const deadline = setTimeout(() => {
  console.error(`FAIL: no result within ${TIMEOUT_MS} ms`);
  cleanup();
  process.exit(1);
}, TIMEOUT_MS);

try {
  const page = `${url}/demo/smoke.html`;
  const { targetId } = await cdp("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp("Target.attachToTarget", { targetId, flatten: true });
  await cdp("Runtime.enable", {}, sessionId);
  await cdp("Page.enable", {}, sessionId);
  await cdp("Page.navigate", { url: page }, sessionId);
  const { result, exceptionDetails } = await cdp(
    "Runtime.evaluate",
    {
      // Poll until the page has installed its marker, then await it.
      expression: `(async () => {
        for (let i = 0; i < 600 && !window.__smoke; i++) await new Promise(r => setTimeout(r, 100));
        if (!window.__smoke) return { status: "fail", error: "page never installed window.__smoke" };
        return await window.__smoke;
      })()`,
      awaitPromise: true,
      returnByValue: true,
    },
    sessionId,
  );
  if (exceptionDetails) throw new Error(exceptionDetails.text ?? "evaluate threw");
  const r = result.value;
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
  const phase3 =
    r?.peer?.version === r?.version &&
    Array.isArray(r?.peer?.tokenReasons) &&
    r.peer.tokenReasons.join(",") === "connect" &&
    typeof r?.peer?.ledgerCode === "string" &&
    r.peer.ledgerCode.length > 0 &&
    r.peer.ledgerCode !== "engine_crashed" &&
    r.peer.ledgerFatal !== true;
  if (phase1 && phase2 && phase3) {
    console.log(`PASS: create → insert → query returned ${r.rows} rows via ${r.transport} transport ` +
      `(engine ${r.version}; init ${r.timings.init} ms, insert ${r.timings.insert} ms, query ${r.timings.query} ms); ` +
      `crash/recycle: typed ${r.crashCode}, stale snapshot → ${r.staleCode}, ` +
      `engine back after ${r.recycleAttempts} attempt(s); ` +
      `peer boundary: token via event channel (${r.peer.tokenReasons.join(",")}), ` +
      `unreachable remote → typed ${r.peer.ledgerCode}`);
    exit = 0;
  } else if (!phase1) {
    console.error("FAIL: page did not report the expected rows");
  } else if (!phase2) {
    console.error("FAIL: crash/recycle phase did not report the expected typed outcomes");
  } else {
    console.error("FAIL: peer-boundary phase did not report the expected typed outcomes");
  }
} catch (err) {
  console.error("FAIL:", err.message ?? err);
} finally {
  clearTimeout(deadline);
  ws.close();
  cleanup();
}
process.exit(exit);
